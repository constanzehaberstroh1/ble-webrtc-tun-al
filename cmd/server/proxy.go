package main

import (
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/hashicorp/yamux"
)

// handleYamuxSession accepts yamux streams from the client and proxies
// them to real internet destinations. Each stream carries a 2-byte
// length-prefixed target address header, followed by raw TCP data.
func handleYamuxSession(session *yamux.Session) {
	for {
		stream, err := session.Accept()
		if err != nil {
			if err != io.EOF {
				mainLog.Error("Proxy: yamux accept: %v", err)
			}
			return
		}
		go handleStream(stream)
	}
}

func handleStream(stream net.Conn) {
	defer stream.Close()

	// Read target address: [2 bytes len][addr]
	// Set a deadline for reading the header to avoid stuck streams
	stream.SetReadDeadline(time.Now().Add(10 * time.Second))
	lenBuf := make([]byte, 2)
	if _, err := io.ReadFull(stream, lenBuf); err != nil {
		return
	}
	addrLen := binary.BigEndian.Uint16(lenBuf)
	if addrLen == 0 || addrLen > 512 {
		return
	}
	addrBuf := make([]byte, addrLen)
	if _, err := io.ReadFull(stream, addrBuf); err != nil {
		return
	}
	addr := string(addrBuf)
	// Clear the header deadline
	stream.SetReadDeadline(time.Time{})

	mainLog.Info("Proxy: CONNECT %s", addr)

	// Connect to real destination with reasonable timeout
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		mainLog.Error("Proxy: dial %s: %v", addr, err)
		return
	}
	defer conn.Close()

	// Enable TCP keepalive on outbound connections to detect dead peers
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	// Bidirectional relay using 256KB buffers (reduces syscall overhead over TURN relay)
	// Use proper half-close: when one direction finishes, signal the other side
	done := make(chan struct{}, 2)
	go func() {
		buf := make([]byte, 256*1024)
		io.CopyBuffer(conn, stream, buf)
		// Half-close: signal to the remote server that we're done sending
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
		done <- struct{}{}
	}()
	go func() {
		buf := make([]byte, 256*1024)
		io.CopyBuffer(stream, conn, buf)
		done <- struct{}{}
	}()
	<-done
}

