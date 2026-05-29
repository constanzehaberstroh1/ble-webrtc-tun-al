package kcptun

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

// Conn implements io.ReadWriteCloser and wraps an underlying io.ReadWriteCloser
// with the KCP ARQ protocol to provide reliable, ordered packet delivery.
type Conn struct {
	underlying io.ReadWriteCloser
	kcp        *kcp.KCP
	mu         sync.Mutex
	readCh     chan []byte
	buf        []byte
	done       chan struct{}
	once       sync.Once
	closed     atomic.Bool
}

// DeriveConvID derives a KCP conversation ID from a shared secret.
// Both client and server must use the same secret to get the same ID.
// If secret is empty, falls back to a default (less secure but compatible).
func DeriveConvID(secret string) uint32 {
	if secret == "" {
		return 0x11223344 // backwards compatible default
	}
	// Use SHA-256 of "kcp-conv:" + secret, take first 4 bytes as uint32
	h := sha256.Sum256([]byte("kcp-conv:" + secret))
	return binary.BigEndian.Uint32(h[:4])
}

// Wrap returns a new KCP-wrapped connection.
// underlying is typically the rtpconn.Conn representing the lossy audio track.
// Uses the default conversation ID. For secret-derived IDs, use WrapWithSecret.
func Wrap(underlying io.ReadWriteCloser) *Conn {
	return WrapWithSecret(underlying, "")
}

// WrapWithSecret returns a new KCP-wrapped connection with a conversation ID
// derived from the shared obfuscation secret. This prevents the KCP conv ID
// from being a recognizable fingerprint.
func WrapWithSecret(underlying io.ReadWriteCloser, secret string) *Conn {
	convID := DeriveConvID(secret)

	c := &Conn{
		underlying: underlying,
		readCh:     make(chan []byte, 4096),
		done:       make(chan struct{}),
	}

	c.kcp = kcp.NewKCP(convID, func(buf []byte, size int) {
		// Output callback: write KCP frames to the underlying lossy transport
		if !c.closed.Load() {
			c.underlying.Write(buf[:size])
		}
	})

	// Tune KCP for ultra-low-latency and robust delivery over a lossy SFU:
	// - NoDelay=1: disable delayed ACK
	// - Interval=10ms: fast update interval
	// - Resend=2: fast resend on 2 duplicate ACKs
	// - NC=1: disable congestion control (we manage bandwidth externally)
	c.kcp.NoDelay(1, 10, 2, 1)

	// Set window sizes (in packets) — generous to handle latency spikes
	c.kcp.WndSize(256, 256)

	// Max segment size: MTU of Opus payloads is typically ~1200
	c.kcp.SetMtu(1000)

	// Start threads
	go c.updateLoop()
	go c.readLoop()

	return c
}

// Read implements io.Reader. Blocks until ordered data is available.
func (c *Conn) Read(p []byte) (int, error) {
	if len(c.buf) > 0 {
		n := copy(p, c.buf)
		c.buf = c.buf[n:]
		return n, nil
	}

	select {
	case data, ok := <-c.readCh:
		if !ok {
			return 0, io.EOF
		}
		n := copy(p, data)
		if n < len(data) {
			c.buf = data[n:]
		}
		return n, nil
	case <-c.done:
		return 0, io.EOF
	}
}

// Write implements io.Writer. Queues data for KCP delivery.
func (c *Conn) Write(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	errCode := c.kcp.Send(p)
	if errCode < 0 {
		return 0, fmt.Errorf("kcp send failed: %d", errCode)
	}

	// Trigger immediate update to flush outbound packets
	c.kcp.Update()

	return len(p), nil
}

// Close implements io.Closer.
func (c *Conn) Close() error {
	c.once.Do(func() {
		c.closed.Store(true)
		close(c.done)
		c.underlying.Close()
	})
	return nil
}

// updateLoop periodically ticks KCP update for packet pacing & retransmissions.
func (c *Conn) updateLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			c.mu.Lock()
			c.kcp.Update()
			c.mu.Unlock()
		}
	}
}

// readLoop pulls lossy packets from the underlying connection, feeds them to KCP,
// and extracts complete, ordered payloads.
func (c *Conn) readLoop() {
	buf := make([]byte, 2048)
	for {
		select {
		case <-c.done:
			return
		default:
		}

		n, err := c.underlying.Read(buf)
		if err != nil {
			c.Close()
			return
		}

		if n > 0 {
			c.mu.Lock()
			c.kcp.Input(buf[:n], kcp.IKCP_PACKET_REGULAR, true)
			c.kcp.Update()

			// Check for fully reassembled, ordered packets
			for {
				size := c.kcp.PeekSize()
				if size <= 0 {
					break
				}

				packet := make([]byte, size)
				nRecv := c.kcp.Recv(packet)
				if nRecv > 0 {
					select {
					case c.readCh <- packet[:nRecv]:
					case <-c.done:
						c.mu.Unlock()
						return
					}
				}
			}
			c.mu.Unlock()
		}
	}
}
