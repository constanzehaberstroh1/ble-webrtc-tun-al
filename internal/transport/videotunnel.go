package transport

// videotunnel.go — Legacy VP8 video tunnel code has been removed.
// All tunnel data now flows through Opus audio RTP via rtpconn.
// This file is kept only for the webrtcLog declaration.

import (
	"github.com/salman/ble-webrtc-tun/internal/logger"
)

var webrtcLog = logger.New("webrtc")
