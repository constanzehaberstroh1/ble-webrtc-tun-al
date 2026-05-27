// Package api — terminal.go provides the web terminal endpoints.
// Two modes:
//   - Server role: Direct PTY over WebSocket (local shell)
//   - Client role: Proxy WebSocket to remote server shell via VPN tunnel
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"

	pty "github.com/creack/pty"
	"github.com/gorilla/websocket"
)

// ============ WebSocket Terminal Handler ============

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// handleTerminalWS provides a WebSocket terminal.
// For server admin: direct local shell via PTY.
// For client admin: proxy WebSocket to remote server's shell API via VPN.
func (s *Server) handleTerminalWS(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	role := s.database.Role()

	if role == "server" {
		// Direct PTY mode — local shell
		s.runDirectPTY(conn)
	} else {
		// Client mode — proxy to remote server shell via VPN
		s.runVPNProxy(conn)
	}
}

// runDirectPTY spawns a local bash shell with PTY and bridges it to WebSocket.
func (s *Server) runDirectPTY(conn *websocket.Conn) {
	shell := os.Getenv("SHELL")
	if shell == "" {
		// Try common shells
		for _, sh := range []string{"/bin/bash", "/bin/sh"} {
			if _, err := os.Stat(sh); err == nil {
				shell = sh
				break
			}
		}
		if shell == "" {
			shell = "/bin/sh"
		}
	}

	apiLog.Info("Terminal: starting shell %s", shell)

	cmd := exec.Command(shell, "-l")
	cmd.Env = append(os.Environ(),
		"TERM=xterm-256color",
		"COLORTERM=truecolor",
		"LANG=en_US.UTF-8",
		"HOME=/root",
	)
	cmd.Dir = "/app"

	ptmx, err := pty.Start(cmd)
	if err != nil {
		apiLog.Error("Terminal: failed to start PTY: %v", err)
		conn.WriteMessage(websocket.TextMessage,
			[]byte(fmt.Sprintf("\r\n\x1b[31mError starting shell: %v\x1b[0m\r\n", err)))
		return
	}
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
		ptmx.Close()
		apiLog.Info("Terminal: PTY session ended")
	}()

	pty.Setsize(ptmx, &pty.Winsize{Rows: 24, Cols: 80})

	var wsMu sync.Mutex
	done := make(chan struct{})

	// PTY → WebSocket
	go func() {
		buf := make([]byte, 8192)
		for {
			n, err := ptmx.Read(buf)
			if err != nil {
				break
			}
			wsMu.Lock()
			conn.WriteMessage(websocket.BinaryMessage, buf[:n])
			wsMu.Unlock()
		}
		close(done)
	}()

	// WebSocket → PTY
	go func() {
		for {
			msgType, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			if msgType == websocket.TextMessage {
				var resizeCmd struct {
					Type string `json:"type"`
					Cols int    `json:"cols"`
					Rows int    `json:"rows"`
				}
				if json.Unmarshal(msg, &resizeCmd) == nil && resizeCmd.Type == "resize" {
					if resizeCmd.Cols > 0 && resizeCmd.Rows > 0 {
						pty.Setsize(ptmx, &pty.Winsize{
							Rows: uint16(resizeCmd.Rows),
							Cols: uint16(resizeCmd.Cols),
						})
					}
					continue
				}
				ptmx.Write(msg)
			} else if msgType == websocket.BinaryMessage {
				ptmx.Write(msg)
			}
		}
	}()

	<-done
}

// runVPNProxy connects to the remote server's /api/shell/ws endpoint
// through the VPN tunnel and bridges the two WebSocket connections.
func (s *Server) runVPNProxy(clientConn *websocket.Conn) {
	if s.RemoteServerURL == "" {
		clientConn.WriteMessage(websocket.TextMessage,
			[]byte("\r\n\x1b[31mError: Remote server URL not configured.\x1b[0m\r\n"+
				"\x1b[33mPlease configure the remote server URL in settings.\x1b[0m\r\n"))
		return
	}

	// Build WebSocket URL from the HTTP server URL
	wsURL := s.RemoteServerURL
	if len(wsURL) > 0 {
		if wsURL[len(wsURL)-1] == '/' {
			wsURL = wsURL[:len(wsURL)-1]
		}
	}
	// Convert http(s) to ws(s)
	if len(wsURL) >= 5 && wsURL[:5] == "https" {
		wsURL = "wss" + wsURL[5:]
	} else if len(wsURL) >= 4 && wsURL[:4] == "http" {
		wsURL = "ws" + wsURL[4:]
	}
	wsURL += "/api/shell/ws"

	apiLog.Info("Terminal VPN proxy: connecting to %s", wsURL)

	// Connect to the remote server's shell WebSocket
	// Use the same auth as the remote proxy
	dialer := websocket.DefaultDialer
	header := http.Header{}

	// Use the remote server auth credentials
	if s.RemoteServerURL != "" {
		header.Set("Authorization", s.remoteAuthHeader())
	}

	serverConn, _, err := dialer.Dial(wsURL, header)
	if err != nil {
		apiLog.Error("Terminal VPN proxy: failed to connect to server: %v", err)
		clientConn.WriteMessage(websocket.TextMessage,
			[]byte(fmt.Sprintf("\r\n\x1b[31mFailed to connect to server shell: %v\x1b[0m\r\n"+
				"\x1b[33mMake sure the VPN tunnel is active and the server is reachable.\x1b[0m\r\n", err)))
		return
	}
	defer serverConn.Close()

	clientConn.WriteMessage(websocket.TextMessage,
		[]byte("\x1b[32m✓ Connected to server shell via VPN\x1b[0m\r\n"+
			"\x1b[90m  Transport: VPN Tunnel → Server PTY\x1b[0m\r\n\r\n"))

	done := make(chan struct{})

	// Server → Client
	go func() {
		defer close(done)
		for {
			msgType, msg, err := serverConn.ReadMessage()
			if err != nil {
				return
			}
			if err := clientConn.WriteMessage(msgType, msg); err != nil {
				return
			}
		}
	}()

	// Client → Server
	go func() {
		for {
			msgType, msg, err := clientConn.ReadMessage()
			if err != nil {
				serverConn.Close()
				return
			}
			if err := serverConn.WriteMessage(msgType, msg); err != nil {
				return
			}
		}
	}()

	<-done
	apiLog.Info("Terminal VPN proxy: session ended")
}

// remoteAuthHeader returns the Basic auth header for the remote server.
func (s *Server) remoteAuthHeader() string {
	// Use the same credentials as proxyToRemote in remote.go
	return "Basic c2FsbWFuOlNhbG1hbjEzNjUxNw==" // salman:Salman136517
}

// handleTerminalInfo returns info about the terminal mode (direct vs vpn_proxy).
func (s *Server) handleTerminalInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	role := s.database.Role()
	info := map[string]interface{}{
		"role":   role,
		"mode":   "direct",
		"status": "ready",
	}

	if role == "client" {
		info["mode"] = "vpn_proxy"
		if s.RemoteServerURL == "" {
			info["status"] = "no_server_url"
		}
	}

	writeJSON(w, http.StatusOK, info)
}
