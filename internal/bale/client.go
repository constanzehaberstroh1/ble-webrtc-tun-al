package bale

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var baleLog = logger.New("bale")

// Client represents a connection to Bale's WebSocket.
type Client struct {
	token   string
	conn    *websocket.Conn
	mu      sync.Mutex
	seqNum  uint32
	seqMu   sync.Mutex

	// RPC response channel for synchronous calls
	rpcRespCh chan []byte

	// Channel for blocking waits
	callCh   chan *IncomingCall
	acceptCh chan *CallAcceptResult

	// Text message channel for SDP exchange
	TextMsgCh chan string

	// Cached access hashes
	accessHashCache map[int64]int64
	accessHashMu    sync.RWMutex

	// Call dedup: prevent emitting the same callID to callCh multiple times
	// Bale sends multiple push events (incoming, ring, update) for the same call.
	emittedCallIDs map[int64]bool
	emittedCallMu  sync.Mutex

	// Message tracking for cleanup (delete all fingerprints)
	sentMsgIDs []trackedMsg
	recvMsgIDs []trackedMsg
	msgMu      sync.Mutex

	closed bool
}

// trackedMsg stores the info needed to delete a message.
type trackedMsg struct {
	PeerID   int64
	RandomID int64 // randomId used in SendMessage or received mid
	DateMs   int64 // message timestamp in milliseconds
}

// IncomingCall represents a received call.
type IncomingCall struct {
	CallID   int64
	CallerID int64
}

// CallAcceptResult contains the LiveKit connection info.
type CallAcceptResult struct {
	CallID       int64
	LivekitToken string
	RoomID       string
	WssURL       string
}

// UserInfo holds account details extracted from the GetFullUser RPC response.
type UserInfo struct {
	UserID      int64
	AccessHash  int64
	DisplayName string
	Phone       string
}

// GetFullUserInfo connects, calls GetFullUser, and extracts structured account info.
// Unlike LoadUserAccessHash, this also extracts the display name and phone number
// by parsing readable strings from the protobuf response.
func (c *Client) GetFullUserInfo(userID int64) (*UserInfo, error) {
	seq := c.nextSeq()
	inner := appendVarintField(nil, 1, uint64(userID))
	inner = appendVarintField(inner, 2, 1)
	req := appendField(nil, 1, inner)
	msg := buildRPCMessage("bale.users.v1.Users", "GetFullUser", req, seq)
	baleLog.Info("GetFullUserInfo %d (seq=%d)", userID, seq)

	// Drain pending responses
	for {
		select {
		case <-c.rpcRespCh:
		default:
			goto send
		}
	}
send:
	if err := c.send(msg); err != nil {
		return nil, err
	}

	select {
	case resp := <-c.rpcRespCh:
		info := &UserInfo{UserID: userID}

		// Extract access_hash (field 2, varint > 100M, not equal to userID)
		for i := 0; i < len(resp)-2; i++ {
			if resp[i] == 0x10 { // field 2, varint
				val, n := binary.Uvarint(resp[i+1:])
				if n > 0 && val > 100000000 && int64(val) != userID {
					info.AccessHash = int64(val)
					break
				}
			}
		}

		// Cache access hash
		if info.AccessHash != 0 {
			c.accessHashMu.Lock()
			c.accessHashCache[userID] = info.AccessHash
			c.accessHashMu.Unlock()
		}

		// Extract readable strings from the response.
		// The protobuf response contains length-delimited string fields
		// for name parts and phone. We extract all strings and heuristically
		// identify them by content pattern.
		strings := extractAllStrings(resp)
		for _, s := range strings {
			if len(s) == 0 {
				continue
			}
			// Phone numbers start with digits or +
			if len(s) >= 10 && len(s) <= 15 && isPhoneNumber(s) {
				if info.Phone == "" {
					info.Phone = s
				}
				continue
			}
			// Skip known protocol strings and JWT tokens
			if s == "bale.users.v1.Users" || s == "GetFullUser" ||
				len(s) > 100 || s == "web_lite" {
				continue
			}
			// Display name: first non-phone, non-protocol readable string > 1 char
			if info.DisplayName == "" && len(s) > 1 && !isNumericOnly(s) {
				info.DisplayName = s
			}
		}

		// Check varints for phone numbers (e.g. 989...)
		phones := extractAllPhoneVarints(resp)
		if len(phones) > 0 && info.Phone == "" {
			info.Phone = phones[0]
		}

		baleLog.Info("UserInfo: id=%d hash=%d name=%q phone=%q",
			info.UserID, info.AccessHash, info.DisplayName, info.Phone)
		return info, nil

	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for GetFullUserInfo response")
	}
}

// extractAllStrings extracts all length-delimited string fields from protobuf data.
func extractAllStrings(data []byte) []string {
	var result []string
	var parse func(b []byte)
	parse = func(b []byte) {
		offset := 0
		for offset < len(b)-2 {
			tag, newOff := decodeVarintAt(b, offset)
			if newOff == offset {
				offset++
				continue
			}
			wireType := tag & 0x07
			switch wireType {
			case 0: // varint
				_, nextOff := decodeVarintAt(b, newOff)
				if nextOff == newOff {
					offset++
					continue
				}
				offset = nextOff
			case 2: // length-delimited
				length, nextOff := decodeVarintAt(b, newOff)
				if nextOff == newOff || nextOff+int(length) > len(b) {
					offset++
					continue
				}
				content := b[nextOff : nextOff+int(length)]
				// Check if content is printable UTF-8 string
				if isPrintableString(content) && len(content) > 0 {
					result = append(result, string(content))
				} else if len(content) > 0 {
					// It might be a nested message, parse it recursively
					parse(content)
				}
				offset = nextOff + int(length)
			case 1: // fixed64
				offset = newOff + 8
			case 5: // fixed32
				offset = newOff + 4
			default:
				offset++
			}
		}
	}
	parse(data)
	return result
}

// extractAllPhoneVarints recursively extracts varints that match Iran phone numbers.
func extractAllPhoneVarints(data []byte) []string {
	var result []string
	var parse func(b []byte)
	parse = func(b []byte) {
		offset := 0
		for offset < len(b)-2 {
			tag, newOff := decodeVarintAt(b, offset)
			if newOff == offset {
				offset++
				continue
			}
			wireType := tag & 0x07
			switch wireType {
			case 0: // varint
				val, nextOff := decodeVarintAt(b, newOff)
				if nextOff == newOff {
					offset++
					continue
				}
				// Check if it's a phone number: e.g. 989xxxxxxxxx
				if val >= 989000000000 && val <= 989999999999 {
					result = append(result, fmt.Sprintf("%d", val))
				}
				offset = nextOff
			case 2: // length-delimited
				length, nextOff := decodeVarintAt(b, newOff)
				if nextOff == newOff || nextOff+int(length) > len(b) {
					offset++
					continue
				}
				content := b[nextOff : nextOff+int(length)]
				if len(content) > 0 {
					parse(content)
				}
				offset = nextOff + int(length)
			case 1:
				offset = newOff + 8
			case 5:
				offset = newOff + 4
			default:
				offset++
			}
		}
	}
	parse(data)
	return result
}

func isPrintableString(data []byte) bool {
	for _, b := range data {
		if b < 32 && b != '\t' && b != '\n' && b != '\r' {
			return false
		}
	}
	return true
}

func isPhoneNumber(s string) bool {
	for i, c := range s {
		if c == '+' && i == 0 {
			continue
		}
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func isNumericOnly(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// NewClient creates a new Bale WebSocket client.
func NewClient(accessToken string) *Client {
	return &Client{
		token:           accessToken,
		seqNum:          1,
		rpcRespCh:       make(chan []byte, 10),
		callCh:          make(chan *IncomingCall, 10),
		acceptCh:        make(chan *CallAcceptResult, 10),
		TextMsgCh:       make(chan string, 10),
		accessHashCache: make(map[int64]int64),
		emittedCallIDs:  make(map[int64]bool),
	}
}

// DrainChannels flushes all pending messages from channels.
// Call between sessions to prevent stale data from interfering.
func (c *Client) DrainChannels() {
	drained := 0
	for {
		select {
		case <-c.callCh:
			drained++
		case <-c.acceptCh:
			drained++
		case <-c.TextMsgCh:
			drained++
		case <-c.rpcRespCh:
			drained++
		default:
			if drained > 0 {
				baleLog.Debug("Drained %d stale messages from channels", drained)
			}
			return
		}
	}
}

// DrainTextChannels flushes text/accept/rpc channels but NOT callCh.
// Use this between sessions to avoid losing new call notifications.
func (c *Client) DrainTextChannels() {
	drained := 0
	for {
		select {
		case <-c.acceptCh:
			drained++
		case <-c.TextMsgCh:
			drained++
		case <-c.rpcRespCh:
			drained++
		default:
			if drained > 0 {
				baleLog.Debug("Drained %d stale text messages", drained)
			}
			return
		}
	}
}

// Connect establishes the WebSocket connection.
func (c *Client) Connect() error {
	headers := http.Header{}
	headers.Set("Origin", "https://web.bale.ai")
	headers.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
	headers.Set("Cookie", "access_token="+c.token)

	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
	}

	conn, _, err := dialer.Dial("wss://next-ws.bale.ai/ws/", headers)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	c.conn = conn

	// Send init message
	initMsg, _ := base64.StdEncoding.DecodeString("GgQIARAB")
	if err := c.send(initMsg); err != nil {
		return fmt.Errorf("send init: %w", err)
	}

	// Read init response
	_, msg, err := c.conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read init response: %w", err)
	}
	b64 := base64.StdEncoding.EncodeToString(msg)
	baleLog.Info("Connected, init response: %s", b64)

	// Start reader
	go c.readLoop()

	return nil
}

func (c *Client) send(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (c *Client) nextSeq() uint32 {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	seq := c.seqNum
	c.seqNum++
	return seq
}

// readLoop processes incoming messages.
func (c *Client) readLoop() {
	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			if !c.closed {
				baleLog.Error("Read error: %v", err)
			}
			return
		}

		c.handleMessage(msg)
	}
}

func (c *Client) handleMessage(data []byte) {
	if len(data) < 2 {
		return
	}

	// Check ALL messages for our tunnel marker (text messages can arrive in any wrapper)
	if bytes.Contains(data, []byte("BLETUN:")) {
		text := c.extractTunnelMessage(data)
		if text != "" {
			baleLog.Info("📨 Tunnel message received (%d bytes)", len(text))
			// Track incoming message for cleanup
			c.trackIncomingMessageFromPush(data)
			select {
			case c.TextMsgCh <- text:
			default:
				baleLog.Warn("Warning: TextMsgCh full, dropping message")
			}
			return
		}
	}

	// Check for terminal relay messages (BLECMD, BLERES, BLERSZ, BLEEND)
	for _, marker := range []string{"BLECMD:", "BLERES:", "BLERSZ:", "BLEEND:"} {
		if bytes.Contains(data, []byte(marker)) {
			text := c.extractTerminalMessage(data, marker)
			if text != "" {
				baleLog.Info("📨 Terminal message received: %s (%d bytes)", marker[:6], len(text))
				c.trackIncomingMessageFromPush(data)
				select {
				case c.TextMsgCh <- text:
				default:
					baleLog.Warn("Warning: TextMsgCh full, dropping terminal message")
				}
				return
			}
		}
	}

	// Detect message type by first byte tag
	firstTag := data[0]

	// Debug: log all incoming messages to diagnose token detection
	baleLog.Info("MSG tag=0x%02x len=%d hasToken=%v", firstTag, len(data),
		containsBytes(data, []byte("eyJhbGci")))

	switch {
	case firstTag == 0x0a:
		// field 1, length-delimited = RPC response
		// Check for LiveKit JWT token in AcceptCall response
		if containsBytes(data, []byte("eyJhbGci")) {
			baleLog.Info("RPC response contains LiveKit token!")
			token, roomID, wssURL := extractCallAcceptInfo(data)
			if token != "" {
				c.acceptCh <- &CallAcceptResult{
					LivekitToken: token,
					RoomID:       roomID,
					WssURL:       wssURL,
				}
			}
		} else {
			// Route to rpcRespCh for synchronous callers
			select {
			case c.rpcRespCh <- data:
			default:
			}
		}
		text := extractReadable(data)
		if len(text) > 10 {
			baleLog.Debug("RPC (%d bytes): %s", len(data), truncStr(text, 300))
		}
		// Always log hex for short responses to debug error codes
		if len(data) < 100 {
			baleLog.Info("RPC hex (%d): %x", len(data), data)
		}

	case firstTag == 0x12:
		// field 2, length-delimited = Server push event
		c.handlePushEvent(data)

	case firstTag == 0x22:
		// field 4 = pong
		// ignore

	case firstTag == 0x2a:
		// field 5 = init response
		// ignore
	}
}

func (c *Client) handlePushEvent(data []byte) {
	if len(data) < 4 {
		return
	}

	// Detect LiveKit connection info (room ID + WSS URL)
	if containsBytes(data, []byte("wss://meet-")) {
		token, roomID, wssURL := extractCallAcceptInfo(data)
		callID := extractCallIDFromPush(data)
		callerID := extractCallerIDFromPush(data)

		baleLog.Debug("\U0001f4de Call push: callID=%d callerID=%d room=%s wss=%s token=%d chars (hex=%x)",
			callID, callerID, roomID, wssURL, len(token), data)

		if wssURL != "" {
			// Dedup: only emit each callID to callCh ONCE.
			// Bale sends multiple push events (ring, update, etc.) for the same call.
			c.emittedCallMu.Lock()
			alreadyEmitted := c.emittedCallIDs[callID]
			if !alreadyEmitted {
				c.emittedCallIDs[callID] = true
			}
			c.emittedCallMu.Unlock()

			if !alreadyEmitted {
				c.callCh <- &IncomingCall{CallID: callID, CallerID: callerID}
			} else {
				baleLog.Info("Skipping duplicate push for callID=%d", callID)
			}

			if token != "" {
				c.acceptCh <- &CallAcceptResult{
					CallID:       callID,
					LivekitToken: token,
					RoomID:       roomID,
					WssURL:       wssURL,
				}
			}
		}
		return
	}

	// Log push events for debugging
	if len(data) > 10 {
		readable := extractReadable(data)
		if len(readable) > 5 {
			baleLog.Debug("Push event (%d bytes): %s", len(data), truncStr(readable, 200))
		} else {
			baleLog.Debug("Push event (%d bytes): hex=%x", len(data), data[:min(len(data), 100)])
		}
	}
}

// extractCallIDFromPush extracts a call ID from a push event message.
// The call info is deeply nested: field 2 → field 1 → field 1 → (extension) → field 1.
// Inside that: field 1 = callID, field 3 = roomID (UUID), field 8 = callerUserID.
func extractCallIDFromPush(data []byte) int64 {
	// Find room UUID as anchor — "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
	roomIdx := -1
	for i := 0; i < len(data)-36; i++ {
		if data[i+8] == '-' && data[i+13] == '-' && data[i+18] == '-' && data[i+23] == '-' {
			roomIdx = i
			break
		}
	}

	// The call ID is field 1 (tag 0x08) BEFORE the room UUID,
	// encoded as a large varint (typically 8-10 bytes)
	if roomIdx > 5 {
		for i := roomIdx - 2; i >= 0; i-- {
			if data[i] == 0x08 { // field 1, varint
				val, n := binary.Uvarint(data[i+1:])
				if n > 4 && val > 1000000 {
					return int64(val)
				}
			}
		}
	}

	// Fallback: scan all 0x08 tags for any large varint
	for i := 0; i < len(data)-6; i++ {
		if data[i] == 0x08 {
			val, n := binary.Uvarint(data[i+1:])
			if n > 4 && val > 1000000 {
				return int64(val)
			}
		}
	}
	return 0
}

// extractCallerIDFromPush extracts the caller user ID from a call push event.
// The caller ID is in field 8 of the call info message.
func extractCallerIDFromPush(data []byte) int64 {
	// Field 8 varint tag = (8 << 3) | 0 = 0x40
	for i := 0; i < len(data)-2; i++ {
		if data[i] == 0x40 { // field 8, varint
			val, n := binary.Uvarint(data[i+1:])
			if n > 0 && val > 100000 && val < 10000000000 {
				return int64(val)
			}
		}
	}
	return 0
}

// LoadUserAccessHash fetches the access_hash for a user by querying GetFullUser.
func (c *Client) LoadUserAccessHash(userID int64) (int64, error) {
	seq := c.nextSeq()
	// Build GetFullUser request: { field1: { field1: userID, field2: 1 } }
	inner := appendVarintField(nil, 1, uint64(userID))
	inner = appendVarintField(inner, 2, 1)
	req := appendField(nil, 1, inner)
	msg := buildRPCMessage("bale.users.v1.Users", "GetFullUser", req, seq)
	baleLog.Info("GetFullUser %d (seq=%d)", userID, seq)

	// Drain any pending responses
	for {
		select {
		case <-c.rpcRespCh:
		default:
			goto send
		}
	}
send:
	if err := c.send(msg); err != nil {
		return 0, err
	}

	// Wait for RPC response containing access_hash
	select {
	case resp := <-c.rpcRespCh:
		// The response structure is deeply nested:
		// F1 (len-delim) → F2 (len-delim) → F1 (len-delim) → user details
		// Inside user details: F1=userID (varint), F2=access_hash (varint)
		// Access hashes are full 64-bit values, NOT limited to 32-bit range.

		// Strategy: find the target userID in the response, then the next field 2
		// varint after it is the access_hash.
		var bestHash int64
		for i := 0; i < len(resp)-2; i++ {
			if resp[i] == 0x10 { // field 2, varint wire type
				val, n := binary.Uvarint(resp[i+1:])
				if n > 0 && val > 100000000 {
					// Accept any large value as access_hash candidate.
					// Skip values that match the userID itself.
					if int64(val) != userID {
						bestHash = int64(val)
						baleLog.Info("Candidate access_hash: %d (0x%x)", val, val)
						break // Take the first valid candidate
					}
				}
			}
		}
		if bestHash != 0 {
			baleLog.Info("Using access_hash for user %d: %d", userID, bestHash)
			c.accessHashMu.Lock()
			c.accessHashCache[userID] = bestHash
			c.accessHashMu.Unlock()
			return bestHash, nil
		}
		baleLog.Debug("GetFullUser response (%d bytes): hex=%x", len(resp), resp[:min(len(resp), 300)])
		return 0, fmt.Errorf("access_hash not found in response")
	case <-time.After(10 * time.Second):
		return 0, fmt.Errorf("timeout waiting for GetFullUser response")
	}
}

// StartCall initiates a call to the target user.
func (c *Client) StartCall(targetUserID int64, isVideo bool) error {
	// First try to get access_hash
	accessHash, err := c.LoadUserAccessHash(targetUserID)
	if err != nil {
		baleLog.Info("Could not get access_hash: %v, trying without it", err)
		accessHash = 0
	}

	// Cache it
	c.accessHashMu.Lock()
	c.accessHashCache[targetUserID] = accessHash
	c.accessHashMu.Unlock()

	seq := c.nextSeq()
	msg := buildStartCallMsg(targetUserID, accessHash, isVideo, seq)
	baleLog.Info("StartCall to user %d (accessHash=%d, seq=%d)", targetUserID, accessHash, seq)
	return c.send(msg)
}

// SendTextMessage sends a text message to a user via Bale.
func (c *Client) SendTextMessage(userID int64, text string) error {
	// Get cached access_hash or fetch it
	c.accessHashMu.RLock()
	accessHash := c.accessHashCache[userID]
	c.accessHashMu.RUnlock()

	if accessHash == 0 {
		var err error
		accessHash, err = c.LoadUserAccessHash(userID)
		if err != nil {
			baleLog.Info("SendTextMessage: no access_hash, trying without: %v", err)
		}
		c.accessHashMu.Lock()
		c.accessHashCache[userID] = accessHash
		c.accessHashMu.Unlock()
	}

	seq := c.nextSeq()
	randomID := time.Now().UnixNano()

	// Build peer: {field1: type=USER(1), field2: userId}
	// Note: access_hash is NOT in the peer for messaging (confirmed via browser sniff)
	peer := appendVarintField(nil, 1, 1)
	peer = appendVarintField(peer, 2, uint64(userID))

	// Build message body: field3 = { field15: { field1: text, field2: "" } }
	textMessage := appendField(nil, 1, []byte(text))
	textMessage = appendField(textMessage, 2, []byte{})
	messageBody := appendField(nil, 15, textMessage)

	// Build sender peer (same format as peer but with our own info - just repeat peer format)
	senderPeer := appendVarintField(nil, 1, 1)
	senderPeer = appendVarintField(senderPeer, 2, uint64(userID))

	// RequestSendMessage: peer=1, randomId=2, message=3, senderPeer=6
	req := appendField(nil, 1, peer)
	req = appendVarintField(req, 2, uint64(randomID))
	req = appendField(req, 3, messageBody)
	req = appendField(req, 6, senderPeer)

	msg := buildRPCMessage("bale.messaging.v2.Messaging", "SendMessage", req, seq)
	baleLog.Info("SendMessage to %d (seq=%d, %d bytes text)", userID, seq, len(text))

	// Track for cleanup
	c.msgMu.Lock()
	c.sentMsgIDs = append(c.sentMsgIDs, trackedMsg{
		PeerID:   userID,
		RandomID: randomID,
		DateMs:   time.Now().UnixMilli(),
	})
	c.msgMu.Unlock()

	return c.send(msg)
}

// TrackReceivedMessage records a received message's ID for later cleanup.
func (c *Client) TrackReceivedMessage(peerID int64, randomID int64, dateMs int64) {
	c.msgMu.Lock()
	c.recvMsgIDs = append(c.recvMsgIDs, trackedMsg{
		PeerID:   peerID,
		RandomID: randomID,
		DateMs:   dateMs,
	})
	c.msgMu.Unlock()
}

// DeleteMessage deletes a single message by its randomId.
// Protocol: service=bale.messaging.v2.Messaging, method=DeleteMessage
// Request: F1=peer, F2=messageId(varint), F3={F1=date_ms(varint)}, F4=""
func (c *Client) DeleteMessage(peerID int64, messageID int64, dateMs int64) error {
	accessHash := c.GetCachedAccessHash(peerID)
	if accessHash == 0 {
		var err error
		accessHash, err = c.LoadUserAccessHash(peerID)
		if err != nil {
			baleLog.Info("DeleteMessage: no access_hash: %v", err)
		}
	}

	seq := c.nextSeq()

	// Build peer: {F1: 1 (USER), F2: userID}
	peer := appendVarintField(nil, 1, 1)
	peer = appendVarintField(peer, 2, uint64(peerID))

	// Build date field: {F1: date_ms}
	dateField := appendVarintField(nil, 1, uint64(dateMs))

	// DeleteMessage request: F1=peer, F2=messageId, F3=date, F4=""
	req := appendField(nil, 1, peer)
	req = appendVarintField(req, 2, uint64(messageID))
	req = appendField(req, 3, dateField)
	req = appendField(req, 4, []byte{})

	msg := buildRPCMessage("bale.messaging.v2.Messaging", "DeleteMessage", req, seq)
	baleLog.Info("DeleteMessage peer=%d msgId=%d (seq=%d)", peerID, messageID, seq)
	return c.send(msg)
}
// ClearChat sends the ClearChat RPC to delete ALL messages in a chat with a peer.
// This wipes the entire chat history, not just tracked messages.
func (c *Client) ClearChat(peerID int64) error {
	accessHash := c.GetCachedAccessHash(peerID)
	if accessHash == 0 {
		var err error
		accessHash, err = c.LoadUserAccessHash(peerID)
		if err != nil {
			baleLog.Info("ClearChat: no access_hash for %d: %v", peerID, err)
		}
	}

	seq := c.nextSeq()

	// Build peer: {F1: 1 (USER), F2: userID, F3: accessHash}
	peer := appendVarintField(nil, 1, 1)
	peer = appendVarintField(peer, 2, uint64(peerID))
	if accessHash != 0 {
		peer = appendVarintField(peer, 3, uint64(accessHash))
	}

	// ClearChat request: F1=peer
	req := appendField(nil, 1, peer)

	msg := buildRPCMessage("bale.messaging.v2.Messaging", "ClearChat", req, seq)
	baleLog.Info("ClearChat peer=%d (seq=%d)", peerID, seq)
	return c.send(msg)
}

// DeleteChatHistory sends the DeleteChatHistory RPC to wipe ALL chat history
// including call records (تماس موفق, تماس ناموفق, تماس از دست رفته).
// The F2 flag requests deletion for both sides.
func (c *Client) DeleteChatHistory(peerID int64) error {
	accessHash := c.GetCachedAccessHash(peerID)
	if accessHash == 0 {
		var err error
		accessHash, err = c.LoadUserAccessHash(peerID)
		if err != nil {
			baleLog.Info("DeleteChatHistory: no access_hash for %d: %v", peerID, err)
		}
	}

	seq := c.nextSeq()

	// Build peer
	peer := appendVarintField(nil, 1, 1)
	peer = appendVarintField(peer, 2, uint64(peerID))
	if accessHash != 0 {
		peer = appendVarintField(peer, 3, uint64(accessHash))
	}

	// DeleteChatHistory: F1=peer, F2=1 (delete for both sides)
	req := appendField(nil, 1, peer)
	req = appendVarintField(req, 2, 1) // revoke = true (delete for both)

	msg := buildRPCMessage("bale.messaging.v2.Messaging", "DeleteChatHistory", req, seq)
	baleLog.Info("DeleteChatHistory peer=%d (seq=%d)", peerID, seq)
	return c.send(msg)
}

// ArchiveChat archives the chat (hides it from chat list).
func (c *Client) ArchiveChat(peerID int64) error {
	accessHash := c.GetCachedAccessHash(peerID)

	seq := c.nextSeq()

	peer := appendVarintField(nil, 1, 1)
	peer = appendVarintField(peer, 2, uint64(peerID))
	if accessHash != 0 {
		peer = appendVarintField(peer, 3, uint64(accessHash))
	}

	req := appendField(nil, 1, peer)

	msg := buildRPCMessage("bale.messaging.v2.Messaging", "ArchiveChat", req, seq)
	baleLog.Info("ArchiveChat peer=%d (seq=%d)", peerID, seq)
	return c.send(msg)
}

// CleanupMessages deletes ALL messages and call records in chats with all known peers.
// Uses multiple strategies to ensure zero fingerprints remain:
// 1. DeleteChatHistory (removes call records + messages for both sides)
// 2. ClearChat (fallback for regular messages)
// 3. Individual message deletion (final fallback)
// This runs without blocking the connection — safe to call during disconnect.
func (c *Client) CleanupMessages() {
	// Collect all known peer IDs from access hash cache
	c.accessHashMu.RLock()
	peerIDs := make([]int64, 0, len(c.accessHashCache))
	for peerID := range c.accessHashCache {
		peerIDs = append(peerIDs, peerID)
	}
	c.accessHashMu.RUnlock()

	if len(peerIDs) == 0 {
		baleLog.Info("CleanupMessages: no known peers to clear")
		return
	}

	baleLog.Info("CleanupMessages: clearing chats with %d peers", len(peerIDs))

	for _, peerID := range peerIDs {
		// Strategy 1: DeleteChatHistory — removes call logs + messages for both sides
		if err := c.DeleteChatHistory(peerID); err != nil {
			baleLog.Warn("DeleteChatHistory failed for %d: %v", peerID, err)
		}
		time.Sleep(300 * time.Millisecond)

		// Strategy 2: ClearChat — backup for regular messages
		if err := c.ClearChat(peerID); err != nil {
			baleLog.Warn("ClearChat failed for %d: %v", peerID, err)
		}
		time.Sleep(300 * time.Millisecond)

		// Strategy 3: Archive the chat to hide from chat list
		if err := c.ArchiveChat(peerID); err != nil {
			baleLog.Warn("ArchiveChat failed for %d: %v", peerID, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Also delete individually tracked messages as final fallback
	c.msgMu.Lock()
	sent := c.sentMsgIDs
	recv := c.recvMsgIDs
	c.sentMsgIDs = nil
	c.recvMsgIDs = nil
	c.msgMu.Unlock()

	for _, m := range sent {
		c.DeleteMessage(m.PeerID, m.RandomID, m.DateMs)
		time.Sleep(100 * time.Millisecond)
	}
	for _, m := range recv {
		c.DeleteMessage(m.PeerID, m.RandomID, m.DateMs)
		time.Sleep(100 * time.Millisecond)
	}

	baleLog.Info("CleanupMessages: done (cleared %d chats + %d individual msgs)",
		len(peerIDs), len(sent)+len(recv))
}

// trackIncomingMessageFromPush extracts message metadata from a push event
// and records it for later cleanup. Push message structure for text messages:
// The message contains mid (message ID) and date fields we need for DeleteMessage.
func (c *Client) trackIncomingMessageFromPush(data []byte) {
	// Walk protobuf fields to extract mid and date
	// Push messages have the message data deeply nested.
	// We look for patterns: a varint that looks like a mid (large number)
	// and a varint that looks like a timestamp in ms (around current time).
	
	// Extract sender user ID from push data
	senderID := extractCallerIDFromPush(data)
	if senderID == 0 {
		// Try to find any user ID in the data
		senderID = extractVarint(data)
	}

	// Extract mid and date by scanning for likely values
	mid, date := extractMidAndDate(data)
	if mid != 0 && date != 0 {
		baleLog.Debug("Tracked incoming msg: peer=%d mid=%d date=%d", senderID, mid, date)
		c.TrackReceivedMessage(senderID, mid, date)
	} else if mid != 0 {
		// Use current time as fallback
		baleLog.Debug("Tracked incoming msg (approx date): peer=%d mid=%d", senderID, mid)
		c.TrackReceivedMessage(senderID, mid, time.Now().UnixMilli())
	}
}

// extractMidAndDate scans protobuf data for message mid and date fields.
// In Bale push messages, mid is typically a large int64 and date is a timestamp in ms.
func extractMidAndDate(data []byte) (mid int64, date int64) {
	offset := 0
	var varints []int64
	
	for offset < len(data) {
		tag, newOff := decodeVarintAt(data, offset)
		if newOff == offset {
			offset++
			continue
		}
		
		fieldNum := tag >> 3
		wireType := tag & 0x07
		
		switch wireType {
		case 0: // varint
			val, nextOff := decodeVarintAt(data, newOff)
			if nextOff == newOff {
				offset++
				continue
			}
			
			if fieldNum >= 1 && fieldNum <= 20 && val > 0 {
				varints = append(varints, int64(val))
			}
			offset = nextOff
			
		case 2: // length-delimited
			length, nextOff := decodeVarintAt(data, newOff)
			if nextOff == newOff || nextOff+int(length) > len(data) {
				offset++
				continue
			}
			offset = nextOff + int(length)
			
		case 1: // fixed64
			offset = newOff + 8
		case 5: // fixed32
			offset = newOff + 4
		default:
			offset++
		}
	}
	
	// Find mid: largest varint that's not a user ID (> 1 billion, looks like randomId/mid)
	// Find date: varint close to current time in ms
	nowMs := time.Now().UnixMilli()
	
	for _, v := range varints {
		// Check if it looks like a timestamp (within 1 day of now)
		if v > nowMs-86400000 && v < nowMs+86400000 {
			if date == 0 || v > date {
				date = v
			}
		}
		// Check if it looks like a message ID (large number, not a timestamp)
		if v > 100000000 && (v < nowMs-86400000 || v > nowMs+86400000) {
			if mid == 0 || v > mid {
				mid = v
			}
		}
	}
	return
}

func decodeVarintAt(data []byte, offset int) (uint64, int) {
	var result uint64
	var shift uint
	for offset < len(data) {
		b := data[offset]
		result |= uint64(b&0x7F) << shift
		offset++
		if b&0x80 == 0 {
			return result, offset
		}
		shift += 7
		if shift >= 64 {
			break
		}
	}
	return 0, offset
}


// extractTunnelMessage finds and extracts BLETUN: prefixed text from push data.
func (c *Client) extractTunnelMessage(data []byte) string {
	idx := bytes.Index(data, []byte("BLETUN:"))
	if idx < 0 {
		return ""
	}

	// The text is inside a protobuf length-prefixed string field.
	// Walk backwards from "BLETUN:" to find the length varint.
	// The byte(s) before our text is the string length.
	if idx >= 2 {
		// Try to read varint length from 1-2 bytes before the start
		strLen := 0
		lenStart := idx - 1
		if idx >= 2 && data[idx-2]&0x80 != 0 {
			// 2-byte varint
			strLen = int(data[idx-2]&0x7F) | (int(data[idx-1]) << 7)
			lenStart = idx - 2
		} else {
			strLen = int(data[idx-1])
		}
		_ = lenStart
		if strLen > 0 && idx+strLen <= len(data) {
			result := string(data[idx : idx+strLen])
			if strings.HasPrefix(result, "BLETUN:") {
				return result
			}
		}
	}

	// Fallback: scan for valid base64/protocol chars (A-Za-z0-9+/=:_)
	start := idx
	end := start
	for end < len(data) {
		b := data[end]
		if (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') ||
			b == '+' || b == '/' || b == '=' || b == ':' || b == '_' {
			end++
		} else {
			break
		}
	}
	return string(data[start:end])
}

// extractTerminalMessage finds and extracts terminal relay messages (BLECMD:, BLERES:, BLERSZ:, BLEEND:) from push data.
// Uses the same protobuf string extraction approach as extractTunnelMessage.
func (c *Client) extractTerminalMessage(data []byte, marker string) string {
	idx := bytes.Index(data, []byte(marker))
	if idx < 0 {
		return ""
	}

	// Try protobuf length-prefix extraction first
	if idx >= 2 {
		strLen := 0
		if idx >= 2 && data[idx-2]&0x80 != 0 {
			strLen = int(data[idx-2]&0x7F) | (int(data[idx-1]) << 7)
		} else {
			strLen = int(data[idx-1])
		}
		if strLen > 0 && idx+strLen <= len(data) {
			result := string(data[idx : idx+strLen])
			if strings.HasPrefix(result, marker) {
				return result
			}
		}
	}

	// Fallback: scan for valid base64/protocol chars (A-Za-z0-9+/=:-_)
	start := idx
	end := start
	for end < len(data) {
		b := data[end]
		if (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') ||
			b == '+' || b == '/' || b == '=' || b == ':' || b == '-' || b == '_' || b == 'x' {
			end++
		} else {
			break
		}
	}
	return string(data[start:end])
}

// GetCachedAccessHash returns a cached access hash for a user.
func (c *Client) GetCachedAccessHash(userID int64) int64 {
	c.accessHashMu.RLock()
	defer c.accessHashMu.RUnlock()
	return c.accessHashCache[userID]
}

// ReceiveCall acknowledges receiving a call.
func (c *Client) ReceiveCall(callID int64) error {
	seq := c.nextSeq()
	msg := buildReceiveCallMsg(callID, seq)
	baleLog.Info("ReceiveCall %d (seq=%d)", callID, seq)
	return c.send(msg)
}

// GetWssURL requests the LiveKit WebSocket URL.
func (c *Client) GetWssURL(callID int64) error {
	seq := c.nextSeq()
	msg := buildGetWssURLMsg(callID, seq)
	baleLog.Info("GetWssURL %d (seq=%d)", callID, seq)
	return c.send(msg)
}

// AcceptCall accepts an incoming call.
func (c *Client) AcceptCall(callID int64, isVideo bool) error {
	seq := c.nextSeq()
	msg := buildAcceptCallMsg(callID, isVideo, seq)
	baleLog.Info("AcceptCall %d (seq=%d)", callID, seq)
	return c.send(msg)
}

// DiscardCall ends/rejects a call.
func (c *Client) DiscardCall(callID int64) error {
	seq := c.nextSeq()
	msg := buildDiscardCallMsg(callID, seq)
	baleLog.Info("DiscardCall %d (seq=%d)", callID, seq)
	return c.send(msg)
}

// SendPing sends a keepalive ping.
func (c *Client) SendPing() error {
	ping, _ := base64.StdEncoding.DecodeString("EgIIBg==")
	return c.send(ping)
}

// WaitForCall blocks until an incoming call is received.
// timeout=0 means wait forever.
func (c *Client) WaitForCall(timeout time.Duration) (*IncomingCall, error) {
	if timeout == 0 {
		call := <-c.callCh
		if call == nil {
			return nil, fmt.Errorf("channel closed")
		}
		return call, nil
	}
	select {
	case call := <-c.callCh:
		return call, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for call")
	}
}

// WaitForAccept blocks until call accept result with LiveKit token is received.
func (c *Client) WaitForAccept(timeout time.Duration) (*CallAcceptResult, error) {
	select {
	case result := <-c.acceptCh:
		return result, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for accept result")
	}
}

// GetCallCh returns the incoming call channel.
func (c *Client) GetCallCh() <-chan *IncomingCall {
	return c.callCh
}

// Close closes the WebSocket connection.
func (c *Client) Close() {
	c.closed = true
	if c.conn != nil {
		c.conn.Close()
	}
}

// StartPingLoop sends periodic pings to keep the connection alive.
func (c *Client) StartPingLoop() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			if c.closed {
				return
			}
			<-ticker.C
			if err := c.SendPing(); err != nil {
				return
			}
		}
	}()
}

// ---- Protobuf message builders ----

// buildRPCMessage builds a Bale RPC message.
// Format: field1 { field1:service, field2:method, field3:request, field4:metadata, field5:seqNum }
func buildRPCMessage(service, method string, request []byte, seq uint32) []byte {
	// Build metadata
	metadata := buildMetadata()

	// Build inner envelope with all fields
	inner := appendField(nil, 1, []byte(service))
	inner = appendField(inner, 2, []byte(method))
	if len(request) > 0 {
		inner = appendField(inner, 3, request)
	}
	inner = appendField(inner, 4, metadata)
	inner = appendVarintField(inner, 5, uint64(seq))

	// Wrap in field 1
	msg := appendField(nil, 1, inner)
	return msg
}

func buildMetadata() []byte {
	pairs := [][2]string{
		{"app_version", "154014"},
		{"browser_type", "1"},
		{"browser_version", "138.0.0.0"},
		{"os_type", "4"},
		{"session_id", fmt.Sprintf("%d", time.Now().UnixMilli())},
	}

	var meta []byte
	for _, p := range pairs {
		// Each entry: field1 { field1: key_string }  field2 { field1: value_string }
		keyWrapped := appendField(nil, 1, []byte(p[0]))
		valInner := appendField(nil, 1, []byte(p[1]))
		valWrapped := appendField(nil, 2, valInner)
		entry := append(keyWrapped, valWrapped...)
		meta = appendField(meta, 1, entry)
	}
	return meta
}

func buildStartCallMsg(userID int64, accessHash int64, isVideo bool, seq uint32) []byte {
	// Confirmed via browser WebSocket capture:
	// F6: { F1: peer{F1:1, F2:userId}, F2: accessHash, F3: callType, F4: {F1:1} }
	// Peer does NOT contain access_hash; it's a separate field in the call wrapper.
	peer := appendVarintField(nil, 1, 1)
	peer = appendVarintField(peer, 2, uint64(userID))

	inner := appendField(nil, 1, peer)
	if accessHash != 0 {
		inner = appendVarintField(inner, 2, uint64(accessHash))
	}
	inner = appendVarintField(inner, 3, 1) // call type
	if isVideo {
		videoFlag := appendVarintField(nil, 1, 1)
		inner = appendField(inner, 4, videoFlag)
	}

	request := appendField(nil, 6, inner)
	return buildRPCMessage("bale.meet.v1.Meet", "StartCall", request, seq)
}

func buildReceiveCallMsg(callID int64, seq uint32) []byte {
	request := encodeVarint(uint64(callID))
	// field 1: callID as varint
	req := appendVarintField(nil, 1, uint64(callID))
	_ = request
	return buildRPCMessage("bale.meet.v1.Meet", "ReceiveCall", req, seq)
}

func buildGetWssURLMsg(callID int64, seq uint32) []byte {
	req := appendVarintField(nil, 1, uint64(callID))
	return buildRPCMessage("bale.meet.v1.Meet", "GetWssURL", req, seq)
}

func buildAcceptCallMsg(callID int64, isVideo bool, seq uint32) []byte {
	req := appendVarintField(nil, 1, uint64(callID))
	if isVideo {
		videoFlag := appendVarintField(nil, 1, 1)
		req = appendField(req, 2, videoFlag)
	}
	return buildRPCMessage("bale.meet.v1.Meet", "AcceptCall", req, seq)
}

func buildDiscardCallMsg(callID int64, seq uint32) []byte {
	req := appendVarintField(nil, 1, uint64(callID))
	req = appendVarintField(req, 3, 3) // reason: hangup
	return buildRPCMessage("bale.meet.v1.Meet", "DiscardCall", req, seq)
}

// ---- Protobuf encoding helpers ----

func encodeVarint(v uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	return buf[:n]
}

func encodeString(s string) []byte {
	return append(encodeVarint(uint64(len(s))), []byte(s)...)
}

// appendField appends a length-delimited protobuf field.
func appendField(dst []byte, fieldNum int, data []byte) []byte {
	tag := uint64(fieldNum<<3) | 2 // wire type 2 = length-delimited
	dst = append(dst, encodeVarint(tag)...)
	dst = append(dst, encodeVarint(uint64(len(data)))...)
	dst = append(dst, data...)
	return dst
}

// appendVarintField appends a varint protobuf field.
func appendVarintField(dst []byte, fieldNum int, value uint64) []byte {
	tag := uint64(fieldNum<<3) | 0 // wire type 0 = varint
	dst = append(dst, encodeVarint(tag)...)
	dst = append(dst, encodeVarint(value)...)
	return dst
}

// ---- Parsing helpers ----

func extractReadable(data []byte) string {
	var text string
	for _, b := range data {
		if b >= 32 && b < 127 {
			text += string(rune(b))
		}
	}
	return text
}

func containsBytes(data, pattern []byte) bool {
	for i := 0; i <= len(data)-len(pattern); i++ {
		match := true
		for j := 0; j < len(pattern); j++ {
			if data[i+j] != pattern[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func extractVarint(data []byte) int64 {
	// Try to find a varint after field tag 0x08 (field 1, varint)
	for i := 0; i < len(data)-1; i++ {
		if data[i] == 0x08 {
			val, _ := binary.Uvarint(data[i+1:])
			if val > 1000 {
				return int64(val)
			}
		}
	}
	return 0
}

func extractCallAcceptInfo(data []byte) (token, roomID, wssURL string) {
	text := string(data)
	// Find JWT token
	for i := 0; i < len(text)-10; i++ {
		if text[i:i+10] == "eyJhbGciOi" {
			// Find end of JWT
			end := i + 10
			for end < len(text) {
				c := text[end]
				if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
					end++
				} else {
					break
				}
			}
			token = text[i:end]
			break
		}
	}

	// Find room ID (UUID format)
	for i := 0; i < len(text)-36; i++ {
		if isUUIDChar(text[i]) && i+36 <= len(text) {
			candidate := text[i : i+36]
			if isUUID(candidate) {
				roomID = candidate
				break
			}
		}
	}

	// Find wss:// URL
	for i := 0; i < len(text)-6; i++ {
		if text[i:i+6] == "wss://" {
			end := i + 6
			for end < len(text) && text[end] != 0 && text[end] > 32 && text[end] < 127 {
				end++
			}
			wssURL = text[i:end]
			break
		}
	}

	return
}

func isUUIDChar(c byte) bool {
	return (c >= 'a' && c <= 'f') || (c >= '0' && c <= '9')
}

func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !((c >= 'a' && c <= 'f') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}

func truncStr(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}
