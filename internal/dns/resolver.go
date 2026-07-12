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
package dns

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"
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

// AppResolver is an isolated DNS resolver that resolves domains exclusively
// through configured upstream DNS servers, bypassing the OS resolver.
type AppResolver struct {
	servers atomic.Pointer[resolverServers]
}

// NewAppResolver configures an isolated network dialer mapping exclusively to
// custom DNS backhauls.
func NewAppResolver(primary, secondary string) *AppResolver {
	r := &AppResolver{}
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
// engine. It performs a UDP DNS query against the primary upstream; on
// failure it transparently falls over to the secondary.
func (r *AppResolver) LookupIP(ctx context.Context, host string) ([]net.IP, error) {
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
