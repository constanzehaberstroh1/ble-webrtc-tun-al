package sync

import (
	"context"
	"io"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"net"
	"time"

	"github.com/salman/ble-webrtc-tun/internal/db"
)

var syncLog = logger.New("sync")

// Worker handles bidirectional event synchronization over a net.Conn
// (typically a dedicated yamux stream). It runs two goroutines:
// - Reader: processes incoming sync messages
// - Pusher: sends new local events to the remote side
type Worker struct {
	database  *db.Database
	conn      net.Conn
	role      string // "client" or "server"
	lastSeqID uint   // last synced sequence ID from remote
	localSeq  uint   // last local event ID pushed to remote
	done      chan struct{}
}

// NewWorker creates a sync worker for a given connection.
func NewWorker(database *db.Database, conn net.Conn) *Worker {
	return &Worker{
		database: database,
		conn:     conn,
		role:     database.Role(),
		done:     make(chan struct{}),
	}
}

// Run starts the sync worker. It blocks until the context is cancelled
// or the connection is closed.
func (w *Worker) Run(ctx context.Context) {
	syncLog.Info("Worker started (role=%s)", w.role)

	// Load last known remote sequence from settings
	if seqStr, err := w.database.GetSetting("sync_last_remote_seq"); err == nil {
		for _, c := range seqStr {
			if c >= '0' && c <= '9' {
				w.lastSeqID = w.lastSeqID*10 + uint(c-'0')
			}
		}
	}

	// If client, request initial sync
	if w.role == "client" {
		w.sendSyncRequest()
	}

	// Start reader and pusher
	go w.readLoop(ctx)
	go w.pushLoop(ctx)

	<-ctx.Done()
	close(w.done)
	w.conn.Close()
	syncLog.Info("Worker stopped")
}

// readLoop processes incoming sync messages.
func (w *Worker) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		default:
		}

		w.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		msg, err := DecodeMessage(w.conn.Read)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if err == io.EOF || isClosedErr(err) {
				syncLog.Info("Connection closed")
				return
			}
			// Timeout is normal — just continue
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			syncLog.Error("Read error: %v", err)
			return
		}

		w.handleMessage(msg)
	}
}

// handleMessage dispatches incoming messages by type.
func (w *Worker) handleMessage(msg *Message) {
	switch msg.Type {
	case MsgSyncRequest:
		var req SyncRequest
		if err := msg.ParsePayload(&req); err != nil {
			syncLog.Info("Bad sync request: %v", err)
			return
		}
		w.handleSyncRequest(req)

	case MsgSyncResponse:
		var resp SyncResponse
		if err := msg.ParsePayload(&resp); err != nil {
			syncLog.Info("Bad sync response: %v", err)
			return
		}
		w.handleSyncResponse(resp)

	case MsgEventPush:
		var event EventData
		if err := msg.ParsePayload(&event); err != nil {
			syncLog.Info("Bad event push: %v", err)
			return
		}
		w.handleEventPush(event)

	case MsgEventAck:
		var ack EventAck
		if err := msg.ParsePayload(&ack); err != nil {
			syncLog.Info("Bad event ack: %v", err)
			return
		}
		syncLog.Info("Remote acknowledged up to seq %d", ack.LastSeqID)

	case MsgPing:
		w.sendMessage(MsgPong, nil)

	case MsgPong:
		// Keepalive response — nothing to do
	}
}

// handleSyncRequest responds with events since the requested sequence.
func (w *Worker) handleSyncRequest(req SyncRequest) {
	limit := req.Limit
	if limit <= 0 {
		limit = 500
	}

	events, err := w.database.GetEventsSince(req.LastSeqID, limit)
	if err != nil {
		syncLog.Warn("Failed to get events: %v", err)
		return
	}

	latestID, _ := w.database.GetLatestEventID()

	var eventData []EventData
	for _, e := range events {
		eventData = append(eventData, EventData{
			ID:        e.ID,
			Type:      e.Type,
			Payload:   e.Payload,
			Source:    e.Source,
			Timestamp: e.Timestamp.Format(time.RFC3339),
		})
	}

	resp := SyncResponse{
		Events:   eventData,
		LatestID: latestID,
	}
	w.sendMessage(MsgSyncResponse, resp)
	syncLog.Info("Sent %d events (since=%d, latest=%d)", len(eventData), req.LastSeqID, latestID)
}

// handleSyncResponse applies received events to the local database.
func (w *Worker) handleSyncResponse(resp SyncResponse) {
	applied := 0
	for _, e := range resp.Events {
		t, _ := time.Parse(time.RFC3339, e.Timestamp)
		event := db.Event{
			Type:      e.Type,
			Payload:   e.Payload,
			Source:    e.Source,
			Timestamp: t,
		}
		if err := w.database.ApplyEvent(event); err != nil {
			syncLog.Warn("Failed to apply event %d: %v", e.ID, err)
			continue
		}
		applied++
		if e.ID > w.lastSeqID {
			w.lastSeqID = e.ID
		}
	}

	// Save progress
	w.saveLastSeq()

	// Send ack
	w.sendMessage(MsgEventAck, EventAck{LastSeqID: w.lastSeqID})
	syncLog.Info("Applied %d/%d events (lastSeq=%d)", applied, len(resp.Events), w.lastSeqID)
}

// handleEventPush applies a single pushed event.
func (w *Worker) handleEventPush(event EventData) {
	t, _ := time.Parse(time.RFC3339, event.Timestamp)
	e := db.Event{
		Type:      event.Type,
		Payload:   event.Payload,
		Source:    event.Source,
		Timestamp: t,
	}
	if err := w.database.ApplyEvent(e); err != nil {
		syncLog.Warn("Failed to apply pushed event: %v", err)
		return
	}
	if event.ID > w.lastSeqID {
		w.lastSeqID = event.ID
		w.saveLastSeq()
	}
	w.sendMessage(MsgEventAck, EventAck{LastSeqID: w.lastSeqID})
}

// pushLoop periodically checks for new local events and pushes them.
func (w *Worker) pushLoop(ctx context.Context) {
	// Initialize local sequence
	w.localSeq, _ = w.database.GetLatestEventID()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		case <-ticker.C:
			w.pushNewEvents()
		case <-pingTicker.C:
			w.sendMessage(MsgPing, nil)
		}
	}
}

// pushNewEvents sends any events created since the last push.
func (w *Worker) pushNewEvents() {
	events, err := w.database.GetEventsSince(w.localSeq, 100)
	if err != nil || len(events) == 0 {
		return
	}

	for _, e := range events {
		// Skip events that originated from the remote side
		if e.Source != w.role {
			w.localSeq = e.ID
			continue
		}

		data := EventData{
			ID:        e.ID,
			Type:      e.Type,
			Payload:   e.Payload,
			Source:    e.Source,
			Timestamp: e.Timestamp.Format(time.RFC3339),
		}
		w.sendMessage(MsgEventPush, data)
		w.localSeq = e.ID
	}
}

// sendSyncRequest asks the server for events since our last known sequence.
func (w *Worker) sendSyncRequest() {
	w.sendMessage(MsgSyncRequest, SyncRequest{
		LastSeqID: w.lastSeqID,
		Limit:     500,
	})
	syncLog.Info("Sent sync request (lastSeq=%d)", w.lastSeqID)
}

// sendMessage encodes and sends a message over the connection.
func (w *Worker) sendMessage(msgType string, payload interface{}) error {
	msg, err := NewMessage(msgType, payload)
	if err != nil {
		return err
	}
	frame, err := EncodeMessage(msg)
	if err != nil {
		return err
	}
	w.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = w.conn.Write(frame)
	return err
}

// saveLastSeq persists the last synced sequence ID.
func (w *Worker) saveLastSeq() {
	seqStr := uintToStr(w.lastSeqID)
	w.database.SetSetting("sync_last_remote_seq", seqStr)
}

func uintToStr(n uint) string {
	if n == 0 {
		return "0"
	}
	b := make([]byte, 0, 10)
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func isClosedErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, sub := range []string{"closed", "reset", "broken"} {
		if containsStr(s, sub) {
			return true
		}
	}
	return false
}

func containsStr(s, sub string) bool {
	if len(sub) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
