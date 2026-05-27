package proxy

import (
	"fmt"
	"io"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var proxyLog = logger.New("proxy")

// TCPProxy handles TCP connections received via DataChannel and forwards
// them to the internet. Used on PaaS environments where TUN is unavailable.
type TCPProxy struct {
	activeConns sync.Map
	connCount   atomic.Int64
}

// NewTCPProxy creates a new TCP proxy.
func NewTCPProxy() *TCPProxy {
	return &TCPProxy{}
}

// ConnID uniquely identifies a proxied connection.
type ConnID struct {
	SrcPort uint16
	DstIP   string
	DstPort uint16
	Proto   string // "tcp" or "udp"
}

func (c ConnID) String() string {
	return fmt.Sprintf("%s:%s:%d<-%d", c.Proto, c.DstIP, c.DstPort, c.SrcPort)
}

// ProxyRequest represents a request to proxy a connection.
type ProxyRequest struct {
	ID      ConnID
	DstAddr string // "host:port"
	Data    []byte // Initial data
}

// ProxyResponse is sent back to the client.
type ProxyResponse struct {
	ID   ConnID
	Data []byte
	EOF  bool // Connection closed
}

// HandleTCPConnect opens a TCP connection and proxies data bidirectionally.
func (p *TCPProxy) HandleTCPConnect(dstAddr string, initialData []byte, sendBack func([]byte)) error {
	conn, err := net.DialTimeout("tcp", dstAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", dstAddr, err)
	}

	p.connCount.Add(1)

	// Send initial data
	if len(initialData) > 0 {
		if _, err := conn.Write(initialData); err != nil {
			conn.Close()
			return fmt.Errorf("writing initial data: %w", err)
		}
	}

	// Read responses and send back
	go func() {
		defer func() {
			conn.Close()
			p.connCount.Add(-1)
		}()

		buf := make([]byte, 4096)
		for {
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			n, err := conn.Read(buf)
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])
				sendBack(data)
			}
			if err != nil {
				if err != io.EOF {
					proxyLog.Info("Read error from %s: %v", dstAddr, err)
				}
				return
			}
		}
	}()

	return nil
}

// ActiveConnections returns the number of active connections.
func (p *TCPProxy) ActiveConnections() int64 {
	return p.connCount.Load()
}
