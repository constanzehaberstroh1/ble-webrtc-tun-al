// Package webui embeds the React admin panel build output and provides
// an HTTP handler to serve it. The build must exist at web/dist/ before
// compiling the Go binary.
//
// Usage in your server:
//
//	import "github.com/salman/ble-webrtc-tun/internal/webui"
//	mux.Handle("/admin/", webui.Handler("/admin"))
package webui

import (
	"embed"
	"io/fs"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"net/http"
	"strings"
)

var webuiLog = logger.New("webui")

//go:embed all:dist
var distFS embed.FS

// Handler returns an HTTP handler that serves the embedded React app.
// prefix is the URL path prefix (e.g. "/admin").
// All unknown routes are served index.html for client-side routing.
func Handler(prefix string) http.Handler {
	subFS, err := fs.Sub(distFS, "dist")
	if err != nil {
		webuiLog.Fatal("[WebUI] Failed to create sub-filesystem: %v", err)
	}

	fileServer := http.FileServer(http.FS(subFS))
	prefix = strings.TrimSuffix(prefix, "/")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Strip prefix
		path := strings.TrimPrefix(r.URL.Path, prefix)
		if path == "" {
			path = "/"
		}

		// Try to serve the file directly
		if path != "/" {
			trimmed := strings.TrimPrefix(path, "/")
			if _, err := fs.Stat(subFS, trimmed); err == nil {
				r.URL.Path = path
				fileServer.ServeHTTP(w, r)
				return
			}
		}

		// Fallback: serve index.html for SPA routing
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}
