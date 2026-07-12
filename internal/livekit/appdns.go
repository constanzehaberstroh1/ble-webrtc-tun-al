package livekit

// appdns.go — Application-level DNS injection for the LiveKit SFU package.
//
// The LiveKit signaling and SFU transports make outbound WebSocket connections
// to Bale's LiveKit infrastructure (e.g. meet-turn.ble.ir).  These connections
// must resolve their target hosts through the admin-configured application
// DNS roots rather than the host OS resolver.
//
// SetAppDialContext installs a context-aware dial function that the package
// uses inside websocket.Dialer.NetDialContext.  When nil (the default), the OS
// resolver is used.

import (
	"context"
	"net"
	"sync/atomic"
)

// DialContextFunc resolves the host portion of addr through the application
// DNS engine before establishing the TCP connection.
type DialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

var appDialContext atomic.Pointer[DialContextFunc]

// SetAppDialContext installs (or clears, with nil) the application-level DNS
// dial function used by all LiveKit SFU outbound WebSocket connections.
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
