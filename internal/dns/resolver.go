// Package dns provides an isolated, application-level DNS resolver that
// resolves domain names exclusively through admin-configured upstream DNS
// roots, completely bypassing the host OS resolver.
//
// This is the client-side Local Traffic Classification and Resolution Point.
// Every Bale signaling/SFU WebSocket connection and every proxy split-routing
// decision resolves its target host through this resolver so that:
//
//   1. DNS queries cannot leak to the ISP/default resolver (stealth).
//   2. Split-routing classification uses deterministic IP translation rather
//      than passing unresolved domain strings down different interfaces.
//
// The resolver is hot-swappable: SetServers() atomically replaces the
// upstream DNS targets so the admin dashboard can change them at runtime
// without restarting the tunnel or dropping in-flight connections.
//
// DNS CACHE: An in-memory cache with TTL eliminates repeated DNS lookups
// through the tunnel. Stale-while-revalidate serves cached results instantly
// while refreshing in the background, and negative caching prevents retrying
// dead domains for 30 seconds.
package dns

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ── Cache Constants ────────────────────────────────────────────────────

const (
	// cacheTTL is the default time-to-live for positive cache entries.
	cacheTTL = 120 * time.Second

	// negativeCacheTTL is the TTL for NXDOMAIN / lookup-failure entries.
	// Prevents hammering the DNS for domains that don't exist.
	negativeCacheTTL = 30 * time.Second

	// staleTolerance is how long past TTL a stale entry can be served
	// while a background refresh is in flight (stale-while-revalidate).
	staleTolerance = 60 * time.Second

	// maxCacheSize caps the cache to prevent unbounded memory growth.
	maxCacheSize = 4096
)

// DialContextFunc is the signature of a context-aware network dialer that
// resolves the host portion of addr through the application DNS before dialing.
// It is compatible with websocket.Dialer.NetDialContext and
// http.Transport.DialContext.
type DialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// resolverServers holds the current upstream DNS configuration. Stored behind
// an atomic pointer so lookups are lock-free and hot-swap is instant.
type resolverServers struct {
	primary   string
	secondary string
}

// cacheEntry stores a resolved DNS result with expiration metadata.
type cacheEntry struct {
	ips        []net.IP  // Resolved addresses (nil for negative cache)
	err        error     // Non-nil for negative cache entries
	expiresAt  time.Time // When the entry becomes stale
	createdAt  time.Time // When the entry was created
	refreshing int32     // 1 if a background refresh is in-flight (atomic)
}

// isExpired returns true if the entry is past its TTL.
func (e *cacheEntry) isExpired() bool {
	return time.Now().After(e.expiresAt)
}

// isServable returns true if the entry can still be served (within stale tolerance).
func (e *cacheEntry) isServable() bool {
	return time.Now().Before(e.expiresAt.Add(staleTolerance))
}

// isNegative returns true if this is a negative (NXDOMAIN) cache entry.
func (e *cacheEntry) isNegative() bool {
	return e.err != nil
}

// AppResolver is an isolated DNS resolver that resolves domains exclusively
// through configured upstream DNS servers, bypassing the OS resolver.
// Includes an in-memory cache with TTL and stale-while-revalidate.
type AppResolver struct {
	servers atomic.Pointer[resolverServers]

	// DNS cache: domain → cacheEntry
	cacheMu sync.RWMutex
	cache   map[string]*cacheEntry
}

// NewAppResolver configures an isolated network dialer mapping exclusively to
// custom DNS backhauls.
func NewAppResolver(primary, secondary string) *AppResolver {
	r := &AppResolver{
		cache: make(map[string]*cacheEntry),
	}
	r.SetServers(primary, secondary)
	return r
}

// SetServers atomically replaces the primary and secondary upstream DNS
// targets. Safe to call concurrently with LookupIP / DialContext.
func (r *AppResolver) SetServers(primary, secondary string) {
	r.servers.Store(&resolverServers{
		primary:   primary,
		secondary: secondary,
	})
}

// Servers returns the currently configured primary and secondary DNS targets.
func (r *AppResolver) Servers() (primary, secondary string) {
	s := r.servers.Load()
	if s == nil {
		return "", ""
	}
	return s.primary, s.secondary
}

// LookupIP resolves domain mappings through the isolated application DNS
// engine with an in-memory cache layer.
//
// Cache behaviour:
//   - Cache HIT (fresh): return immediately (0ms latency).
//   - Cache HIT (stale): return immediately + trigger background refresh.
//   - Cache HIT (negative, fresh): return cached error immediately.
//   - Cache MISS: resolve synchronously, cache the result, return.
func (r *AppResolver) LookupIP(ctx context.Context, host string) ([]net.IP, error) {
	// ── Step 1: Check cache ────────────────────────────────────────────
	r.cacheMu.RLock()
	entry, cached := r.cache[host]
	r.cacheMu.RUnlock()

	if cached {
		if !entry.isExpired() {
			// Fresh cache hit — return instantly.
			if entry.isNegative() {
				return nil, entry.err
			}
			return entry.ips, nil
		}

		if entry.isServable() && !entry.isNegative() {
			// Stale but servable — return stale data and refresh in background.
			if atomic.CompareAndSwapInt32(&entry.refreshing, 0, 1) {
				go func() {
					defer atomic.StoreInt32(&entry.refreshing, 0)
					bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					r.resolveAndCache(bgCtx, host)
				}()
			}
			return entry.ips, nil
		}

		// Expired beyond stale tolerance — fall through to synchronous resolve.
	}

	// ── Step 2: Cache miss — resolve synchronously ─────────────────────
	return r.resolveAndCache(ctx, host)
}

// resolveAndCache performs the actual DNS lookup and caches the result.
func (r *AppResolver) resolveAndCache(ctx context.Context, host string) ([]net.IP, error) {
	ips, err := r.rawLookup(ctx, host)

	now := time.Now()
	r.cacheMu.Lock()

	// Evict if cache is too large (simple LRU-like: just clear half).
	if len(r.cache) >= maxCacheSize {
		count := 0
		for k := range r.cache {
			delete(r.cache, k)
			count++
			if count >= maxCacheSize/2 {
				break
			}
		}
	}

	if err != nil {
		// Negative cache: remember the failure for a short time.
		r.cache[host] = &cacheEntry{
			err:       err,
			expiresAt: now.Add(negativeCacheTTL),
			createdAt: now,
		}
		r.cacheMu.Unlock()
		return nil, err
	}

	// Positive cache.
	r.cache[host] = &cacheEntry{
		ips:       ips,
		expiresAt: now.Add(cacheTTL),
		createdAt: now,
	}
	r.cacheMu.Unlock()

	return ips, nil
}

// rawLookup performs the actual DNS resolution without caching.
func (r *AppResolver) rawLookup(ctx context.Context, host string) ([]net.IP, error) {
	s := r.servers.Load()
	if s == nil || (s.primary == "" && s.secondary == "") {
		return net.DefaultResolver.LookupIP(ctx, "ip4", host)
	}

	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: 3 * time.Second}
			if s.primary != "" {
				conn, err := d.DialContext(ctx, "udp", net.JoinHostPort(s.primary, "53"))
				if err == nil {
					return conn, nil
				}
				if s.secondary == "" {
					return nil, err
				}
			}
			return d.DialContext(ctx, "udp", net.JoinHostPort(s.secondary, "53"))
		},
	}
	return resolver.LookupIP(ctx, "ip4", host)
}

// CacheStats returns the current cache size for monitoring.
func (r *AppResolver) CacheStats() (size int) {
	r.cacheMu.RLock()
	size = len(r.cache)
	r.cacheMu.RUnlock()
	return
}

// DialContext resolves the host portion of addr through the application DNS
// engine, then dials the resolved IP directly. This preserves TLS SNI (the
// TLS layer uses the original hostname) while ensuring the underlying TCP
// connection targets the DNS-returned IP.
//
// Used as websocket.Dialer.NetDialContext and http.Transport.DialContext so
// that all Bale signaling, SFU, and scraping HTTP requests route through the
// admin-configured DNS roots.
func (r *AppResolver) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		d := net.Dialer{Timeout: 10 * time.Second}
		return d.DialContext(ctx, network, addr)
	}

	// If the host is already a literal IP, dial directly (no resolution needed).
	if ip := net.ParseIP(host); ip != nil {
		d := net.Dialer{Timeout: 10 * time.Second}
		return d.DialContext(ctx, network, addr)
	}

	ips, err := r.LookupIP(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("app DNS resolve %s: %w", host, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("app DNS: no addresses for %s", host)
	}

	d := net.Dialer{Timeout: 10 * time.Second}
	return d.DialContext(ctx, network, net.JoinHostPort(ips[0].String(), port))
}
