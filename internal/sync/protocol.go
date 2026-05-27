// Package sync provides event-sourced synchronization between client and
// server databases over the existing Yamux data channel. It ensures both
// sides have a consistent view of account states, pairings, and connection
// history even after network interruptions.
//
// Protocol messages are length-prefixed JSON frames sent over a dedicated
// yamux stream. The server is the source of truth for sequence IDs.
package sync

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// Message types for the sync protocol.
const (
	MsgSyncRequest  = "SYNC_REQ"   // Client → Server: give me events since lastSeqID
	MsgSyncResponse = "SYNC_RESP"  // Server → Client: here are the events
	MsgEventPush    = "EVENT_PUSH" // Bidirectional: new event occurred
	MsgEventAck     = "EVENT_ACK"  // Acknowledgment of received events
	MsgPing         = "SYNC_PING"  // Keepalive
	MsgPong         = "SYNC_PONG"  // Keepalive response
)

// Message is the wire format for sync protocol messages.
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// SyncRequest asks the remote side for events since a given sequence ID.
type SyncRequest struct {
	LastSeqID uint `json:"last_seq_id"`
	Limit     int  `json:"limit,omitempty"`
}

// SyncResponse contains events in response to a SyncRequest.
type SyncResponse struct {
	Events   []EventData `json:"events"`
	LatestID uint        `json:"latest_id"`
}

// EventData is a serializable event for transmission.
type EventData struct {
	ID        uint   `json:"id"`
	Type      string `json:"type"`
	Payload   string `json:"payload"`
	Source    string `json:"source"`
	Timestamp string `json:"timestamp"`
}

// EventAck acknowledges receipt of events up to a sequence ID.
type EventAck struct {
	LastSeqID uint `json:"last_seq_id"`
}

// EncodeMessage serializes a sync message to a length-prefixed frame.
// Format: [4 bytes big-endian length][JSON payload]
func EncodeMessage(msg *Message) ([]byte, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(data)))
	copy(frame[4:], data)
	return frame, nil
}

// DecodeMessage reads a length-prefixed frame and deserializes it.
func DecodeMessage(readFn func([]byte) (int, error)) (*Message, error) {
	// Read length prefix
	lenBuf := make([]byte, 4)
	if _, err := readFull(readFn, lenBuf); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)
	if msgLen == 0 || msgLen > 10*1024*1024 { // 10MB max
		return nil, fmt.Errorf("invalid message length: %d", msgLen)
	}

	// Read message body
	body := make([]byte, msgLen)
	if _, err := readFull(readFn, body); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	var msg Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal message: %w", err)
	}
	return &msg, nil
}

// NewMessage creates a Message with a typed payload.
func NewMessage(msgType string, payload interface{}) (*Message, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Message{
		Type:    msgType,
		Payload: json.RawMessage(raw),
	}, nil
}

// ParsePayload deserializes the payload into the target type.
func (m *Message) ParsePayload(target interface{}) error {
	return json.Unmarshal(m.Payload, target)
}

// readFull reads exactly len(buf) bytes using the provided read function.
func readFull(readFn func([]byte) (int, error), buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := readFn(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
