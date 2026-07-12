package main

// routing.go — Client-side Local Traffic Classification and Resolution Point.
//
// The RoutingEngine wraps the application-level DNS resolver and the
// split-tunneling bypass engine.  Every incoming proxy transaction (SOCKS5
// or HTTP CONNECT/plain-HTTP) is intercepted here, the target host is
// resolved strictly through the admin-configured DNS roots, and the
// BypassEngine classifies the resolved target before a single byte is
// written to either the network card or the QUIC pool.
//
// Branch A (bypass=true)  → direct TCP dial over the local interface.
// Branch B (bypass=false) → WebRTC QUIC artery pool (existing path).
//
// The engine is hot-swappable: Reload() atomically rebuilds the resolver
// and bypass trie so the admin dashboard can change DNS/bypass settings at
// runtime without restarting the tunnel.

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/salman/ble-webrtc-tun/internal/bale"
	"github.com/salman/ble-webrtc-tun/internal/dns"
	lk "github.com/salman/ble-webrtc-tun/internal/livekit"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"github.com/salman/ble-webrtc-tun/internal/pool"
	"github.com/salman/ble-webrtc-tun/internal/router"
)

var routingLog = logger.New("routing")

// RoutingEngine is the client-side traffic classification and resolution
// point. It is safe for concurrent use.
type RoutingEngine struct {
	mu       sync.RWMutex
	resolver *dns.AppResolver
	bypass   *router.BypassEngine
}

// NewRoutingEngine builds a routing engine from the given DNS and bypass
// configuration.  The Iranian CIDR list is loaded automatically.
func NewRoutingEngine(primary, secondary, bypassDomains string) *RoutingEngine {
	return &RoutingEngine{
		resolver: dns.NewAppResolver(primary, secondary),
		bypass:   router.NewBypassEngine(bypassDomains, router.IranianCIDRs()),
	}
}

// Reload atomically rebuilds the resolver DNS targets and the bypass domain
// trie from new settings.  In-flight classification calls continue with the
// old state until the swap completes.
func (re *RoutingEngine) Reload(primary, secondary, bypassDomains string) {
	re.mu.Lock()
	defer re.mu.Unlock()
	if re.resolver != nil {
		re.resolver.SetServers(primary, secondary)
	} else {
		re.resolver = dns.NewAppResolver(primary, secondary)
	}
	if re.bypass != nil {
		re.bypass.SetCustomDomains(bypassDomains)
	} else {
		re.bypass = router.NewBypassEngine(bypassDomains, router.IranianCIDRs())
	}
	routingLog.Info("Routing engine reloaded: DNS=%s/%s bypass-domains=%d",
		primary, secondary, len(strings.Split(bypassDomains, ",")))
}

// Snapshot returns the current DNS primary/secondary and the admin-defined
// bypass domains as a comma-separated string.  Used by the settings API.
func (re *RoutingEngine) Snapshot() (primary, secondary, bypassDomains string) {
	re.mu.RLock()
	defer re.mu.RUnlock()
	if re.resolver != nil {
		primary, secondary = re.resolver.Servers()
	}
	if re.bypass != nil {
		bypassDomains = strings.Join(re.bypass.CustomDomains(), ", ")
	}
	return
}

// resolverDial returns the application DNS dial function for Bale/LiveKit
// package-level installation, or nil if the engine is unconfigured.
func (re *RoutingEngine) resolverDial() dns.DialContextFunc {
	re.mu.RLock()
	defer re.mu.RUnlock()
	if re.resolver == nil {
		return nil
	}
	return re.resolver.DialContext
}

// InstallAppDNS configures the Bale and LiveKit packages to resolve all
// outbound WebSocket/HTTP connections (signaling, SFU, scraping, auth)
// through this engine's application DNS roots.  This ensures every Bale
// infrastructure connection bypasses the host OS resolver and routes
// exclusively through the admin-configured upstream DNS.  Called at startup
// and on every settings reload.
func (re *RoutingEngine) InstallAppDNS() {
	re.mu.RLock()
	resolver := re.resolver
	re.mu.RUnlock()
	if resolver == nil {
		bale.SetAppDialContext(nil)
		lk.SetAppDialContext(nil)
		return
	}
	// The method value resolver.DialContext has an unnamed function type that
	// is directly assignable to the named DialContextFunc types in both
	// packages, ensuring all Bale connections resolve through the app DNS.
	bale.SetAppDialContext(resolver.DialContext)
	lk.SetAppDialContext(resolver.DialContext)
	primary, secondary := resolver.Servers()
	routingLog.Info("Application DNS installed: %s / %s", primary, secondary)
}

// classifyAndRelay intercepts a proxy transaction (SOCKS5 or HTTP CONNECT),
// resolves the target host through the application DNS engine, classifies it
// via the BypassEngine, and routes accordingly:
//
//   - Branch A (bypass): direct TCP dial over the local interface.
//   - Branch B (tunnel): forwarded through the WebRTC QUIC artery pool.
//
// targetAddr is "host:port".  For the tunnel branch the ORIGINAL address
// (domain or IP) is sent to the server so the server resolves it on its end;
// resolution on the client is only used for the bypass classification
// decision.
func (re *RoutingEngine) classifyAndRelay(targetAddr string, localConn net.Conn, p *pool.TunnelPool) {
	host, port, err := net.SplitHostPort(targetAddr)
	if err != nil || host == "" {
		// Can't parse — fall through to the tunnel path (server handles it).
		dialAndRelay(p, targetAddr, localConn)
		return
	}

	var targetIP net.IP
	resolvedAddr := targetAddr

	// If the host is a domain (not a literal IP), resolve it strictly through
	// the application DNS roots for classification.
	if ip := net.ParseIP(host); ip == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		re.mu.RLock()
		resolver := re.resolver
		re.mu.RUnlock()
		if resolver != nil {
			ips, lerr := resolver.LookupIP(ctx, host)
			if lerr == nil && len(ips) > 0 {
				targetIP = ips[0]
				resolvedAddr = net.JoinHostPort(targetIP.String(), port)
			}
		}
		cancel()
	} else {
		targetIP = ip
	}

	// Evaluate the routing path classification.
	re.mu.RLock()
	bypass := re.bypass
	re.mu.RUnlock()

	if bypass != nil && bypass.EvaluateRoutingPath(host, targetIP) {
		// Branch A: Bypass the WebRTC tunnel pool — route directly over the
		// local network interface using the DNS-resolved IP.
		routingLog.Info("[Bypass] %s → direct local (resolved=%s)", targetAddr, resolvedAddr)
		directDialAndRelay(resolvedAddr, localConn)
		return
	}

	// Branch B: Default route path — forward packets securely down the
	// WebRTC QUIC artery pool.  The original address is sent so the server
	// resolves it on its end.
	dialAndRelay(p, targetAddr, localConn)
}

// directDialAndRelay opens a direct TCP connection over the local network
// interface and relays data bidirectionally.  Used by the bypass branch.
func directDialAndRelay(addr string, localConn net.Conn) {
	remoteConn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		routingLog.Info("[Bypass] direct dial failed for %s: %v", addr, err)
		return
	}
	defer remoteConn.Close()
	relayBidir(localConn, remoteConn)
}

// relayBidir pumps data in both directions between two connections using
// 32KB buffers until one side closes.
func relayBidir(a, b net.Conn) {
	done := make(chan struct{}, 2)
	go func() {
		buf := make([]byte, 32*1024)
		io.CopyBuffer(a, b, buf)
		done <- struct{}{}
	}()
	go func() {
		buf := make([]byte, 32*1024)
		io.CopyBuffer(b, a, buf)
		done <- struct{}{}
	}()
	<-done
}

// classifyHTTPPlain handles a plain (non-CONNECT) HTTP proxy request with
// split-routing.  For the tunnel branch it opens a QUIC stream and sends the
// target address header + request (existing behaviour).  For the bypass
// branch it dials the target directly over the local interface and forwards
// the raw HTTP request.
func (re *RoutingEngine) classifyHTTPPlain(host, reqLine string, localConn net.Conn, p *pool.TunnelPool) {
	// Extract the bare hostname (strip port) for classification.
	classifyHost := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		classifyHost = h
	}

	var targetIP net.IP
	resolvedHost := host

	if ip := net.ParseIP(classifyHost); ip == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		re.mu.RLock()
		resolver := re.resolver
		re.mu.RUnlock()
		if resolver != nil {
			ips, lerr := resolver.LookupIP(ctx, classifyHost)
			if lerr == nil && len(ips) > 0 {
				targetIP = ips[0]
				// Rebuild host:port with resolved IP.
				if _, port, err := net.SplitHostPort(host); err == nil {
					resolvedHost = net.JoinHostPort(targetIP.String(), port)
				}
			}
		}
		cancel()
	} else {
		targetIP = ip
	}

	re.mu.RLock()
	bypass := re.bypass
	re.mu.RUnlock()

	if bypass != nil && bypass.EvaluateRoutingPath(classifyHost, targetIP) {
		// Branch A: direct local routing for plain HTTP.
		routingLog.Info("[Bypass-HTTP] %s → direct local (resolved=%s)", classifyHost, resolvedHost)
		remoteConn, err := net.DialTimeout("tcp", resolvedHost, 10*time.Second)
		if err != nil {
			routingLog.Info("[Bypass-HTTP] direct dial failed for %s: %v", resolvedHost, err)
			return
		}
		defer remoteConn.Close()
		// Forward the raw HTTP request line to the direct connection.
		if _, err := io.WriteString(remoteConn, reqLine); err != nil {
			return
		}
		relayBidir(localConn, remoteConn)
		return
	}

	// Branch B: tunnel via QUIC pool.
	httpTunnelRelay(p, host, reqLine, localConn)
}

// httpTunnelRelay forwards a plain HTTP request through the WebRTC QUIC pool.
// It sends the target address as a length-prefixed header followed by the raw
// HTTP request, then relays bidirectionally.
func httpTunnelRelay(p *pool.TunnelPool, host, reqLine string, localConn net.Conn) {
	stream, err := p.OpenStream()
	if err != nil {
		io.WriteString(localConn, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
		return
	}
	defer stream.Close()

	addrBytes := []byte(host)
	hdr := make([]byte, 2+len(addrBytes))
	binaryPutUint16(hdr[:2], uint16(len(addrBytes)))
	copy(hdr[2:], addrBytes)
	if _, err := stream.Write(hdr); err != nil {
		io.WriteString(localConn, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
		return
	}
	stream.Write([]byte(reqLine))
	relayBidir(localConn, stream)
}

// binaryPutUint16 writes a big-endian uint16 into b[:2].
func binaryPutUint16(b []byte, v uint16) {
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}
