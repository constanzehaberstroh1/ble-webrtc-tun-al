package main

// bond_registry.go — server-side bond group coordinator.
//
// When the server binary manages multiple Bale accounts (one per WebRTC
// channel), each account's handleSFUProxy goroutine gets a separate rtpconn.Conn.
// For packet-level bonding these N rtpconn instances must be aggregated into
// one BondedPacketConn before the QUIC listener is created.
//
// The BondRegistry solves this coordination problem:
//   1. Each handleSFUProxy call reads a 4-byte bond group ID sent by the client.
//   2. It registers its rtpconn.Conn with the registry under that group ID.
//   3. The registry assembles a BondedPacketConn per group and starts a
//      shared QUIC listener when the first lane arrives.
//   4. All subsequent lanes are hot-added to the running BondedPacketConn.

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/salman/ble-webrtc-tun/internal/logger"
	"github.com/salman/ble-webrtc-tun/internal/quicconn"
	"github.com/salman/ble-webrtc-tun/internal/rtpconn"
)

var bondRegLog = logger.New("bondreg")

// bondGroup holds the aggregated state for one logical client bond.
type bondGroup struct {
	mu        sync.Mutex
	id        uint32
	bondConn  *quicconn.BondedPacketConn
	listener  *quic.Listener
	qconn     quic.Connection // set once Accept() succeeds
	ready     chan struct{}    // closed when qconn is set
	laneCount int
	createdAt time.Time
}

// BondRegistry maps bond group IDs to their aggregated state.
// A single global instance is used by all handleSFUProxy goroutines.
type BondRegistry struct {
	mu     sync.Mutex
	groups map[uint32]*bondGroup
}

func newBondRegistry() *BondRegistry {
	br := &BondRegistry{
		groups: make(map[uint32]*bondGroup),
	}
	go br.cleanupLoop()
	return br
}

// RegisterLane adds an rtpconn.Conn to the bond group identified by groupID.
// If this is the first lane for this group, a new BondedPacketConn and QUIC
// listener are created. Returns the bond group's QUIC connection (may block
// up to 90s waiting for the QUIC handshake to complete).
func (br *BondRegistry) RegisterLane(
	ctx context.Context,
	groupID uint32,
	conn *rtpconn.Conn,
	label string,
	tlsCfg *tls.Config,
	quicCfg *quic.Config,
) (quic.Connection, *quicconn.BondedPacketConn, error) {
	br.mu.Lock()
	grp, exists := br.groups[groupID]
	if !exists {
		bc, err := quicconn.NewBondedPacketConn()
		if err != nil {
			br.mu.Unlock()
			return nil, nil, fmt.Errorf("new bonded conn: %w", err)
		}
		grp = &bondGroup{
			id:        groupID,
			bondConn:  bc,
			ready:     make(chan struct{}),
			createdAt: time.Now(),
		}
		br.groups[groupID] = grp
		bondRegLog.Info("[BondReg] New bond group 0x%08x created", groupID)
	}
	br.mu.Unlock()

	// Add this lane to the bond.
	grp.mu.Lock()
	grp.bondConn.AddLane(conn, label)
	grp.laneCount++
	laneNum := grp.laneCount
	grp.mu.Unlock()

	bondRegLog.Info("[BondReg] Group 0x%08x: lane %d registered (%s)", groupID, laneNum, label)

	// The first lane to register starts the QUIC listener.
	if laneNum == 1 {
		go br.startListener(ctx, grp, tlsCfg, quicCfg)
	}

	// All lanes wait for the QUIC connection to be accepted.
	const acceptTimeout = 90 * time.Second
	select {
	case <-grp.ready:
		grp.mu.Lock()
		qconn := grp.qconn
		bc := grp.bondConn
		grp.mu.Unlock()
		return qconn, bc, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-time.After(acceptTimeout):
		return nil, nil, fmt.Errorf("bond group 0x%08x: QUIC accept timeout after %v", groupID, acceptTimeout)
	}
}

// startListener creates the QUIC listener on the BondedPacketConn and
// accepts the client's connection. Called in a goroutine by the first lane.
func (br *BondRegistry) startListener(
	ctx context.Context,
	grp *bondGroup,
	tlsCfg *tls.Config,
	quicCfg *quic.Config,
) {
	grp.mu.Lock()
	bc := grp.bondConn
	grp.mu.Unlock()

	listener, err := quic.Listen(bc, tlsCfg, quicCfg)
	if err != nil {
		bondRegLog.Error("[BondReg] QUIC listen failed for group 0x%08x: %v", grp.id, err)
		return
	}

	grp.mu.Lock()
	grp.listener = listener
	grp.mu.Unlock()

	bondRegLog.Info("[BondReg] Group 0x%08x: QUIC listener ready — waiting for client", grp.id)

	const acceptTimeout = 90 * time.Second
	accCtx, accCancel := context.WithTimeout(ctx, acceptTimeout)
	defer accCancel()

	qconn, err := listener.Accept(accCtx)
	if err != nil {
		bondRegLog.Error("[BondReg] Group 0x%08x: QUIC accept failed: %v", grp.id, err)
		return
	}

	grp.mu.Lock()
	grp.qconn = qconn
	grp.mu.Unlock()

	bondRegLog.Info("[BondReg] Group 0x%08x: QUIC client connected!", grp.id)
	close(grp.ready) // unblock all waiting lanes
}

// RemoveLane removes a lane from the bond group.
func (br *BondRegistry) RemoveLane(groupID uint32, conn *rtpconn.Conn) {
	br.mu.Lock()
	grp, exists := br.groups[groupID]
	br.mu.Unlock()

	if !exists {
		return
	}

	grp.mu.Lock()
	grp.bondConn.RemoveLane(conn)
	grp.laneCount--
	remaining := grp.laneCount
	grp.mu.Unlock()

	bondRegLog.Info("[BondReg] Group 0x%08x: lane removed (%d remaining)", groupID, remaining)

	if remaining <= 0 {
		br.mu.Lock()
		delete(br.groups, groupID)
		br.mu.Unlock()
		bondRegLog.Info("[BondReg] Group 0x%08x: deleted (no lanes)", groupID)
	}
}

// GetQConn returns the QUIC connection for a bond group if already established.
func (br *BondRegistry) GetQConn(groupID uint32) quic.Connection {
	br.mu.Lock()
	grp, exists := br.groups[groupID]
	br.mu.Unlock()
	if !exists {
		return nil
	}
	grp.mu.Lock()
	defer grp.mu.Unlock()
	return grp.qconn
}

// cleanupLoop evicts stale bond groups (>5 minutes with no QUIC connection).
func (br *BondRegistry) cleanupLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		br.mu.Lock()
		for id, grp := range br.groups {
			grp.mu.Lock()
			if grp.qconn == nil && now.Sub(grp.createdAt) > 5*time.Minute {
				delete(br.groups, id)
				bondRegLog.Warn("[BondReg] Evicted stale group 0x%08x (no QUIC conn after 5m)", id)
			}
			grp.mu.Unlock()
		}
		br.mu.Unlock()
	}
}
