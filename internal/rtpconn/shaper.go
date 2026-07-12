// Package rtpconn — shaper.go implements steganographic camouflage for tunnel
// data transported inside Opus audio frames.
//
// Phase 1 — Authentic Opus TOC Container:
//
//	Every data frame is prefixed with a valid Opus Table-of-Contents byte so
//	that any DPI or media gateway attempting to parse the RTP payload as Opus
//	audio sees a structurally valid frame instead of raw encrypted bytes.
//
//	0x78 = config 15 (SILK narrow-band, 20 ms frame), mono, 1 frame/packet.
//	This is a common low-bitrate VoIP configuration that blends in with normal
//	Bale voice traffic.
//
// Phase 2 — VBR Padding (length camouflage):
//
//	Real Opus audio uses Variable Bit Rate encoding — packet sizes fluctuate
//	naturally.  Tunnel data packets tend to cluster around MTU boundaries
//	(e.g. ~1100 bytes), which creates a telltale length-frequency spike.
//	The shaper appends 0–31 bytes of deterministic padding to each frame so the
//	size distribution spreads out and no single size dominates.
//
//	The padding length is derived from a hash of the frame's first bytes
//	(visible to both sides), so the receiver can strip it without any extra
//	metadata on the wire.
//
// Wire format:
//
//	[TOC byte 0x78] [payload (plaintext or nonce+ciphertext)] [0-31 padding bytes]
//
// The 3-byte silence keepalive frame (0xF8 0xFF 0xFE) is NOT wrapped — it's
// already a valid Opus DTX frame and is recognised and skipped on receive.
package rtpconn

// opusTOCByte is the Opus Table-of-Contents byte prepended to every data
// frame so the payload parses as valid Opus audio.  Config 15 = SILK NB,
// 20 ms, mono, 1 frame per packet.
const opusTOCByte byte = 0x78

// maxPadLen is the maximum number of padding bytes appended for VBR
// length camouflage.  At most ~2.8 % overhead for a 1100-byte frame.
const maxPadLen = 32

// seedBytes is how many leading bytes of the payload are hashed to
// derive the padding length.  Both sides see these bytes.
const seedBytes = 16

// fnvHash computes a 64-bit FNV-1a hash of the first n bytes of data.
func fnvHash(data []byte) uint64 {
	var h uint64 = 14695981039346656037 // FNV-1a offset basis
	n := len(data)
	if n > seedBytes {
		n = seedBytes
	}
	for i := 0; i < n; i++ {
		h ^= uint64(data[i])
		h *= 1099511628211 // FNV-1a prime
	}
	return h
}

// derivePadLen returns the padding length (0–maxPadLen-1) for a frame
// whose payload starts with the given data.
func derivePadLen(data []byte) int {
	if len(data) == 0 {
		return 0
	}
	return int(fnvHash(data) % maxPadLen)
}

// fillPadding writes deterministic pseudo-random bytes into buf using an
// LCG seeded with hash.  The bytes look like compressed-audio payload
// to a DPI histogram analyser.
func fillPadding(buf []byte, hash uint64) {
	var s uint64 = hash
	if s == 0 {
		s = 1
	}
	for i := range buf {
		s = s*6364136223846793005 + 1442695040888963407 // Numerical Recipes LCG
		buf[i] = byte(s >> 33)
	}
}

// WrapFrame wraps a payload in an Opus TOC container with VBR padding.
//
// payload is the data to send on the wire — either plaintext (obfuscation
// disabled) or nonce+ciphertext (obfuscation enabled).
//
// Returns a new byte slice: [opusTOCByte][payload][padding].
func WrapFrame(payload []byte) []byte {
	padLen := derivePadLen(payload)
	framed := make([]byte, 1+len(payload)+padLen)
	framed[0] = opusTOCByte
	copy(framed[1:], payload)
	if padLen > 0 {
		fillPadding(framed[1+len(payload):], fnvHash(payload))
	}
	return framed
}

// UnwrapFrame strips the Opus TOC byte and trailing VBR padding from a
// received frame, returning the original payload.
//
// If the frame doesn't start with opusTOCByte it is returned as-is (backward
// compatibility with peers that don't wrap).
func UnwrapFrame(frame []byte) []byte {
	if len(frame) == 0 {
		return frame
	}
	// Not our TOC byte — pass through unwrapped (backward compat).
	if frame[0] != opusTOCByte {
		return frame
	}
	data := frame[1:]
	if len(data) == 0 {
		return data
	}
	padLen := derivePadLen(data)
	if padLen > 0 && padLen < len(data) {
		data = data[:len(data)-padLen]
	}
	return data
}

// hasOpusTOC returns true if the frame starts with our Opus TOC byte.
func hasOpusTOC(frame []byte) bool {
	return len(frame) > 0 && frame[0] == opusTOCByte
}
