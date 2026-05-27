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

	mainLog.Info("Proxy: CONNECT %s", addr)

	// Connect to real destination
	conn, err := net.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		mainLog.Error("Proxy: dial %s: %v", addr, err)
		return
	}
	defer conn.Close()

	// Bidirectional relay using 256KB buffers (reduces syscall overhead over TURN relay)
	done := make(chan struct{}, 2)
	go func() {
		buf := make([]byte, 256*1024)
		io.CopyBuffer(conn, stream, buf)
		done <- struct{}{}
	}()
	go func() {
		buf := make([]byte, 256*1024)
		io.CopyBuffer(stream, conn, buf)
		done <- struct{}{}
	}()
	<-done
}
