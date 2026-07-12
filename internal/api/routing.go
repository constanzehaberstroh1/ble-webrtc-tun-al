package api

// routing.go — REST API for the application-level DNS and split-tunneling
// bypass configuration.
//
//   GET  /api/routing/settings  → returns the current DNS + bypass settings.
//   POST /api/routing/settings  → updates DNS + bypass settings and hot-swaps
//                                 the routing engine (no tunnel restart needed).
//
// On the server role these endpoints are available but the reload callback is
// nil (the server does not run proxy listeners), so a POST returns 501.

import (
	"encoding/json"
	"net/http"
	"strings"
)

// routingSettingsRequest is the body of POST /api/routing/settings.
type routingSettingsRequest struct {
	DNSPrimary    string `json:"dns_primary"`
	DNSSecondary  string `json:"dns_secondary"`
	BypassDomains string `json:"bypass_domains"`
}

// handleRoutingSettings manages the application-level DNS and bypass-domain
// configuration.
func (s *Server) handleRoutingSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if s.GetRoutingSettings != nil {
			settings, err := s.GetRoutingSettings()
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, settings)
			return
		}
		// Server role or not configured — return empty defaults.
		writeJSON(w, http.StatusOK, map[string]string{
			"dns_primary":    "",
			"dns_secondary":  "",
			"bypass_domains": "",
		})

	case http.MethodPost:
		var req routingSettingsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}

		// Basic validation: at least the primary DNS should be a plausible IP
		// or empty (to clear).  We trim whitespace and skip empty values.
		req.DNSPrimary = strings.TrimSpace(req.DNSPrimary)
		req.DNSSecondary = strings.TrimSpace(req.DNSSecondary)
		req.BypassDomains = strings.TrimSpace(req.BypassDomains)

		if s.OnReloadRouting == nil {
			writeError(w, http.StatusNotImplemented, "routing reload not available in this mode")
			return
		}
		if err := s.OnReloadRouting(req.DNSPrimary, req.DNSSecondary, req.BypassDomains); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeOK(w, "routing settings updated")

	default:
		writeError(w, http.StatusMethodNotAllowed, "GET or POST only")
	}
}
