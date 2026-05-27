package admin

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var staticFS embed.FS

// StaticHandler returns an HTTP handler that serves the embedded admin static
// assets (xterm JS/CSS) from /admin/static/*.
func StaticHandler() http.Handler {
	subFS, _ := fs.Sub(staticFS, "static")
	return http.StripPrefix("/admin/static/", http.FileServer(http.FS(subFS)))
}
