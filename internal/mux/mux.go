// Package mux provides a simple stream multiplexer over a reliable byte channel (KCP).
// Each message: [4 bytes streamID][1 byte cmd][data...]
package mux

import (
	"encoding/binary"
	"fmt"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"sync"
	"sync/atomic"
)

var muxLog = logger.New("mux")

// Command types
const (
	CmdConnect    byte = 1 // client→server: data = "host:port"
	CmdConnected  byte = 2 // server→client: connection OK
	CmdData       byte = 3 // bidirectional: raw TCP data
	CmdClose      byte = 4 // bidirectional: stream closed
	CmdConnectErr byte = 5 // server→client: data = error string
)

// Stream represents one multiplexed TCP connection.
type Stream struct {
	ID      uint32
	DataCh  chan []byte
	CloseCh chan struct{}
	closed  atomic.Bool
}

func (s *Stream) Close() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.CloseCh)
	}
}

// Mux multiplexes multiple streams over a single KCP channel.
type Mux struct {
	sendFn  func([]byte) int // KCP Send
	streams sync.Map         // map[uint32]*Stream
	nextID  atomic.Uint32
	onNew   func(id uint32, addr string) // server-side: new connect request
}

// NewMux creates a client-side mux (allocates stream IDs).
func NewMux(sendFn func([]byte) int) *Mux {
	return &Mux{sendFn: sendFn}
}

// NewServerMux creates a server-side mux (handles incoming connects).
func NewServerMux(sendFn func([]byte) int, onNew func(id uint32, addr string)) *Mux {
	return &Mux{sendFn: sendFn, onNew: onNew}
}

// NewStream creates a new client-side stream.
func (m *Mux) NewStream() *Stream {
	id := m.nextID.Add(1)
	s := &Stream{
		ID:      id,
		DataCh:  make(chan []byte, 4096),
		CloseCh: make(chan struct{}),
	}
	m.streams.Store(id, s)
	return s
}

// GetOrCreateStream gets or creates a server-side stream.
func (m *Mux) GetOrCreateStream(id uint32) *Stream {
	if v, ok := m.streams.Load(id); ok {
		return v.(*Stream)
	}
	s := &Stream{
		ID:      id,
		DataCh:  make(chan []byte, 4096),
		CloseCh: make(chan struct{}),
	}
	m.streams.Store(id, s)
	return s
}

// RemoveStream removes a stream.
func (m *Mux) RemoveStream(id uint32) {
	if v, ok := m.streams.LoadAndDelete(id); ok {
		v.(*Stream).Close()
	}
}

// SendConnect sends a CONNECT request (client→server).
func (m *Mux) SendConnect(s *Stream, addr string) {
	m.send(s.ID, CmdConnect, []byte(addr))
}

// SendConnected sends a CONNECTED response (server→client).
func (m *Mux) SendConnected(id uint32) {
	m.send(id, CmdConnected, nil)
}

// SendConnectErr sends a connection error (server→client).
func (m *Mux) SendConnectErr(id uint32, errMsg string) {
	m.send(id, CmdConnectErr, []byte(errMsg))
}

// SendData sends data on a stream.
func (m *Mux) SendData(id uint32, data []byte) {
	m.send(id, CmdData, data)
}

// SendClose sends a close notification.
func (m *Mux) SendClose(id uint32) {
	m.send(id, CmdClose, nil)
}

func (m *Mux) send(id uint32, cmd byte, data []byte) {
	frame := make([]byte, 5+len(data))
	binary.BigEndian.PutUint32(frame[0:4], id)
	frame[4] = cmd
	if len(data) > 0 {
		copy(frame[5:], data)
	}
	m.sendFn(frame)
}

// HandleFrame processes a received KCP message. Called from KCP recv callback.
func (m *Mux) HandleFrame(frame []byte) {
	if len(frame) < 5 {
		return
	}
	id := binary.BigEndian.Uint32(frame[0:4])
	cmd := frame[4]
	data := frame[5:]

	switch cmd {
	case CmdConnect:
		// Server side: new connection request
		if m.onNew != nil {
			m.onNew(id, string(data))
		}

	case CmdConnected:
		if s := m.getStream(id); s != nil {
			select {
			case s.DataCh <- nil: // signal connected (nil = connected OK)
			default:
			}
		}

	case CmdConnectErr:
		if s := m.getStream(id); s != nil {
			muxLog.Error("Stream %d connect error: %s", id, string(data))
			s.Close()
		}

	case CmdData:
		if s := m.getStream(id); s != nil {
			select {
			case s.DataCh <- append([]byte{}, data...):
			case <-s.CloseCh:
			}
		}

	case CmdClose:
		m.RemoveStream(id)

	default:
		muxLog.Info("Unknown cmd %d on stream %d", cmd, id)
	}
}

func (m *Mux) getStream(id uint32) *Stream {
	v, ok := m.streams.Load(id)
	if !ok {
		return nil
	}
	return v.(*Stream)
}

// DialAndRelay connects to addr, sends CONNECT, waits for CONNECTED, then relays.
// Used by the client SOCKS5/HTTP proxy handlers.
func (m *Mux) DialAndRelay(addr string, localConn interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
}) error {
	s := m.NewStream()
	defer func() {
		m.SendClose(s.ID)
		m.RemoveStream(s.ID)
	}()

	// Send CONNECT
	m.SendConnect(s, addr)

	// Wait for CONNECTED or error
	select {
	case msg := <-s.DataCh:
		if msg != nil {
			// Unexpected data before connected
			return fmt.Errorf("unexpected data before connected")
		}
		// nil = connected OK
	case <-s.CloseCh:
		return fmt.Errorf("connection rejected")
	}

	// Relay: local → tunnel
	done := make(chan struct{}, 2)
	go func() {
		buf := make([]byte, 16384)
		for {
			n, err := localConn.Read(buf)
			if n > 0 {
				m.SendData(s.ID, buf[:n])
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}()

	// Relay: tunnel → local
	go func() {
		for {
			select {
			case data, ok := <-s.DataCh:
				if !ok || data == nil {
					done <- struct{}{}
					return
				}
				if _, err := localConn.Write(data); err != nil {
					done <- struct{}{}
					return
				}
			case <-s.CloseCh:
				done <- struct{}{}
				return
			}
		}
	}()

	<-done
	localConn.Close()
	<-done
	return nil
}
