package api

import (
	"net/http"
	"os"
)

func (s *Server) handleGuide(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	content, err := os.ReadFile("GUIDE.md")
	if err != nil {
		// Fallback if not run from root directory
		content, err = os.ReadFile("../../GUIDE.md")
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Failed to load GUIDE.md")
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"content": string(content),
	})
}
