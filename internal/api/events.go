package api

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/salman/ble-webrtc-tun/internal/logger"
)

// handleEvents handles GET /api/events?since=N&limit=M.
// Used by the sync protocol and admin panel for real-time event streaming.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	sinceID := uint(0)
	if s := r.URL.Query().Get("since"); s != "" {
		if parsed, err := strconv.ParseUint(s, 10, 32); err == nil {
			sinceID = uint(parsed)
		}
	}

	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	events, err := s.database.GetEventsSince(sinceID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	latestID, _ := s.database.GetLatestEventID()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"events":    events,
		"latest_id": latestID,
		"count":     len(events),
	})
}

// handleLogs handles GET /api/logs?limit=N&level=WARN&component=BALE.
// Returns recent log entries from the in-memory ring buffer.
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	limit := 500
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	levelFilter := r.URL.Query().Get("level")
	componentFilter := r.URL.Query().Get("component")

	var logs []logger.LogEntry
	if levelFilter != "" || componentFilter != "" {
		logs = logger.GetFilteredLogs(limit, levelFilter, componentFilter)
	} else {
		logs = logger.GetLogs(limit)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"logs": logs,
	})
}

// handleLogsWS handles WebSocket connections for live log streaming.
// GET /api/logs/ws?level=WARN&component=BALE
//
// The client receives JSON log entries in real-time, pushed as they occur.
// This replaces the polling-based approach and provides zero-latency log viewing.
func (s *Server) handleLogsWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		apiLog.Error("logs WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	// Parse filters from query string
	var level logger.Level = logger.DEBUG // default: all levels
	if lv := r.URL.Query().Get("level"); lv != "" {
		switch strings.ToUpper(lv) {
		case "INFO":
			level = logger.INFO
		case "WARN":
			level = logger.WARN
		case "ERROR":
			level = logger.ERROR
		case "FATAL":
			level = logger.FATAL
		}
	}
	component := r.URL.Query().Get("component")

	// Subscribe to live log stream
	sub := logger.Subscribe(level, component)
	defer logger.Unsubscribe(sub)

	// Send recent history first (last 200 entries matching filter)
	history := logger.GetFilteredLogs(200, r.URL.Query().Get("level"), component)
	for _, entry := range history {
		if err := conn.WriteJSON(entry); err != nil {
			return
		}
	}

	// Read pump — detect disconnection
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// Write pump — stream live entries
	for {
		select {
		case entry, ok := <-sub.Ch:
			if !ok {
				return
			}
			if err := conn.WriteJSON(entry); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}

// LogFileInfo describes an available log file for the UI.
type LogFileInfo struct {
	Name      string `json:"name"`
	Component string `json:"component"`
	Size      int64  `json:"size"`
	SizeHuman string `json:"size_human"`
	Path      string `json:"path"`
}

// handleLogFiles handles GET /api/logs/files.
// Lists all available log files with their sizes.
func (s *Server) handleLogFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	dir := logger.GetLogDir()
	if dir == "" {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"files": []LogFileInfo{},
			"role":  logger.GetRole(),
		})
		return
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("read log dir: %v", err))
		return
	}

	var files []LogFileInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		component := strings.TrimSuffix(e.Name(), ".log")
		files = append(files, LogFileInfo{
			Name:      e.Name(),
			Component: component,
			Size:      info.Size(),
			SizeHuman: formatBytes(info.Size()),
			Path:      filepath.Join(dir, e.Name()),
		})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"files": files,
		"role":  logger.GetRole(),
		"dir":   dir,
	})
}

// handleLogDownload handles GET /api/logs/download?file=combined.
// Streams the requested log file as a downloadable attachment.
func (s *Server) handleLogDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	fileName := r.URL.Query().Get("file")
	if fileName == "" {
		writeError(w, http.StatusBadRequest, "missing 'file' parameter")
		return
	}

	// Sanitize: only allow alphanumeric, dash, underscore
	for _, c := range fileName {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
			writeError(w, http.StatusBadRequest, "invalid file name")
			return
		}
	}

	dir := logger.GetLogDir()
	if dir == "" {
		writeError(w, http.StatusNotFound, "log directory not initialized")
		return
	}

	filePath := filepath.Join(dir, fileName+".log")
	f, err := os.Open(filePath)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("log file not found: %s", fileName))
		return
	}
	defer f.Close()

	info, _ := f.Stat()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s_%s.log"`, logger.GetRole(), fileName))
	if info != nil {
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	}

	http.ServeContent(w, r, fileName+".log", info.ModTime(), f)
}

// formatBytes converts bytes to a human-readable string.
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
