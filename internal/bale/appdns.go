package bale

// appdns.go — Application-level DNS injection for the Bale package.
//
// The Bale client makes outbound connections (WebSocket signaling, gRPC-Web
// auth, and live-bundle scraping) to Bale's own infrastructure.  All of these
// must resolve their target hosts through the admin-configured application
// DNS roots rather than the host OS resolver, so that DNS queries cannot leak
// to the ISP/default resolver.
//
// SetAppDialContext installs a context-aware dial function that the package
// uses inside websocket.Dialer.NetDialContext and http.Transport.DialContext.
// When nil (the default), the OS resolver is used — preserving backwards
// compatibility.

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync/atomic"
)

// DialContextFunc resolves the host portion of addr through the application
// DNS engine before establishing the TCP connection.
type DialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

var appDialContext atomic.Pointer[DialContextFunc]

// SetAppDialContext installs (or clears, with nil) the application-level DNS
// dial function used by all Bale outbound connections.  Safe to call
// concurrently; the update is atomic.
func SetAppDialContext(fn DialContextFunc) {
	if fn == nil {
		appDialContext.Store(nil)
		return
	}
	appDialContext.Store(&fn)
}

// appDial returns the currently-installed application dial function, or nil
// if none is configured.
func appDial() DialContextFunc {
	fnp := appDialContext.Load()
	if fnp == nil {
		return nil
	}
	return *fnp
}

// newHTTPTransport builds an *http.Transport that resolves hosts through the
// application DNS engine when configured.  Used by the extractor and auth
// clients so that scraping and gRPC-Web requests to web.bale.ai route through
// the admin-configured DNS roots.
func newHTTPTransport() *http.Transport {
	t := &http.Transport{
		TLSClientConfig: &tls.Config{},
		ForceAttemptHTTP2: true,
	}
	if dc := appDial(); dc != nil {
		t.DialContext = dc
	}
	return t
}
