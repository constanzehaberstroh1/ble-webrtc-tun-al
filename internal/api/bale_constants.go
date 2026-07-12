package api

// bale_constants.go — REST API for the Dynamic Upstream Parameter Extraction
// Engine.
//
//   GET  /api/bale/constants       — return the current client-emulation constants
//   POST /api/bale/constants/sync — scrape the live Bale bundle, update the
//                                    store + DB, return the new snapshot
//
// Both endpoints live behind the standard Basic-auth middleware.

import (
	"context"
	"net/http"
	"time"

	"github.com/salman/ble-webrtc-tun/internal/bale"
)

// handleBaleConstants handles GET /api/bale/constants — returns the current
// in-memory client-emulation constants (app_version, LiveKit SDK/protocol
// versions, browser version, infrastructure URLs, last-sync timestamp).
func (s *Server) handleBaleConstants(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	snap := bale.Snapshot()
	writeJSON(w, http.StatusOK, snap)
}

// handleBaleConstantsSync handles POST /api/bale/constants/sync — triggers a
// live scrape of the Bale web bundle, applies extracted constants to the
// in-memory store, persists them to the database, and returns the updated
// snapshot.  The scrape is bounded by a 45-second context timeout so a slow
// or unresponsive upstream can't block the worker.
func (s *Server) handleBaleConstantsSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()

	snap, err := bale.SyncConstants(ctx, s.database)
	if err != nil {
		apiLog.Warn("Upstream constant sync failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"status":  "error",
			"message": "Failed to extract constants from Bale upstream",
			"details": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":                   "success",
		"message":                  "Constants synchronized with upstream Bale bundle",
		"app_version":              snap.AppVersion,
		"web_api_key":              snap.WebAPIKey,
		"livekit_sdk_version":      snap.SDKVersion,
		"livekit_protocol_version": snap.ProtocolVersion,
		"browser_version":          snap.BrowserVersion,
		"bale_ws_url":              snap.BaleWSURL,
		"bale_grpc_base":           snap.BaleGRPCBase,
		"livekit_origin":           snap.LiveKitOrigin,
		"bale_web_origin":          snap.BaleWebOrigin,
		"last_synced_at":           snap.LastSyncedAt,
	})
}
