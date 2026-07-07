package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/salman/ble-webrtc-tun/internal/bale"
	"github.com/salman/ble-webrtc-tun/internal/config"
	"github.com/salman/ble-webrtc-tun/internal/pool"
)

// =============================================================================
// Layer 3: Staggered Dynamic Session Refresh Engine
//
// A centralized coordinator that grants "refresh tickets" to individual
// channels. Two strict invariants must hold before a channel may refresh:
//   1. Mutual-Exclusion: no other channel is currently refreshing (limit = 1).
//   2. Quorum-Safety: at least 2 channels are active so that removing the
//      refreshing one still leaves >= 1 channel carrying traffic.
//
// This prevents "load-shedding blackouts" where multiple lines refresh at the
// same time and overload the Bale signaling plane.
// =============================================================================

type refreshSupervisor struct {
	mu         sync.Mutex
	refreshing bool
}

// tryAcquire grants a refresh ticket if no other channel is refreshing AND the
// quorum-safety constraint is satisfied (activeCount >= 2 so one survives).
func (rs *refreshSupervisor) tryAcquire(activeCount int) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.refreshing {
		return false
	}
	if activeCount < 2 {
		// Refreshing would drop the pool to 0 active — never allow that.
		return false
	}
	rs.refreshing = true
	return true
}

func (rs *refreshSupervisor) release() {
	rs.mu.Lock()
	rs.refreshing = false
	rs.mu.Unlock()
}

// nextRefreshInterval returns a randomized per-channel refresh window:
//
//	60 minutes + RandomJitter(0..60 minutes)
//
// This naturally spreads expirations across a 60-minute spectrum even if all
// channels initialized at the same instant.
func (tm *TunnelManager) nextRefreshInterval() time.Duration {
	return 60*time.Minute + time.Duration(rand.Intn(60))*time.Minute
}

// =============================================================================
// Layer 1: Intelligent Error Classification & Resolution Matrix
// =============================================================================

// errorClass categorizes a handshake failure so the orchestrator can apply the
// correct recovery workflow instead of a generic retry.
type errorClass int

const (
	classBaleConnect errorClass = iota // Bale WS handshake / sign-in failure
	classCallPhase                     // StartCall / WaitForAccept — server busy or stuck
	classSFUPhase                      // SFU connect / track negotiation / QUIC dial
	classUnknown                       // Unclassified
)

// classifyError maps the phase at which initChannelTracked failed to an
// errorClass. This drives Layer 1 targeted recovery.
func (tm *TunnelManager) classifyError(failPhase ChannelPhase, errMsg string) errorClass {
	switch failPhase {
	case PhaseBaleConnect:
		return classBaleConnect
	case PhaseCalling, PhaseWaitAccept:
		return classCallPhase
	case PhaseSFUConnect, PhaseWaitTrack, PhaseTunnelSetup:
		return classSFUPhase
	default:
		return classUnknown
	}
}

// isTokenError heuristically detects token revocation/expiry so the channel can
// stop retrying instead of looping forever on a dead credential.
func isTokenError(msg string) bool {
	m := strings.ToLower(msg)
	return strings.Contains(m, "401") ||
		strings.Contains(m, "unauthorized") ||
		strings.Contains(m, "expired") ||
		strings.Contains(m, "forbidden") ||
		strings.Contains(m, "access denied") ||
		strings.Contains(m, "invalid token")
}

// =============================================================================
// Layer 2: Controlled Disruption Retries & Clean Hangup Protocol
// =============================================================================

// endCallForPair sends BLETUN:ENDCALL to a single paired server account via a
// transient Bale WebSocket, waits for BLETUN:ENDCALL_ACK, then erases message
// traces. This forces the paired server out of any stuck RESERVED/IN_CALL
// state back to IDLE so a fresh handshake can succeed.
//
// This is the out-of-band pre-flight signaling loop: it operates independently
// of the channel's own (possibly dead) Bale client and reuses the server's
// existing ENDCALL handling path.
func (tm *TunnelManager) endCallForPair(ctx context.Context, tp config.TokenPair, label string) bool {
	if tp.ClientToken == "" || tp.TargetUserID == 0 {
		return false
	}

	client := bale.NewClient(tp.ClientToken)
	if err := client.Connect(); err != nil {
		mainLog.Warn("[%s] ENDCALL: Bale connect failed: %v", label, err)
		return false
	}
	defer client.Close()
	client.StartPingLoop()
	time.Sleep(1 * time.Second)

	mainLog.Info("[%s] ENDCALL: sending to %d", label, tp.TargetUserID)
	if err := client.SendTextMessage(tp.TargetUserID, "BLETUN:ENDCALL"); err != nil {
		mainLog.Warn("[%s] ENDCALL: send failed: %v", label, err)
		return false
	}

	// Wait for ACK (up to 15s), respecting cancellation.
	timeout := time.NewTimer(15 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case msg := <-client.TextMsgCh:
			if msg == "BLETUN:ENDCALL_ACK" {
				mainLog.Info("[%s] ENDCALL: ACK received — server is IDLE", label)
				time.Sleep(500 * time.Millisecond)
				client.CleanupMessages()
				return true
			}
		case <-timeout.C:
			mainLog.Warn("[%s] ENDCALL: timeout (no ACK) — proceeding anyway", label)
			client.CleanupMessages()
			return false
		}
	}
}

// initChannelWithRetry wraps initChannelTracked with the three-layer recovery:
//
//  1. Layer 1: classify the failure phase and decide a targeted response.
//  2. Layer 2: for call/SFU-phase failures, run the Clean Hangup Protocol
//     (endCallForPair) to unstick the paired server before retrying.
//  3. Exponential backoff with a cap; token/credential failures stop retrying.
//
// It returns nil only on a fatal (token) failure or context cancellation,
// otherwise it keeps retrying until the channel connects successfully — exactly
// the "repeat until this account connects successfully" requirement.
func (tm *TunnelManager) initChannelWithRetry(ctx context.Context, idx int, tp config.TokenPair, label string) (*channelState, quic.Connection) {
	backoff := 3 * time.Second
	const maxBackoff = 30 * time.Second
	baleFails := 0
	const baleFailLimit = 8

	for {
		select {
		case <-ctx.Done():
			return nil, nil
		default:
		}

		ch, qconn, failPhase, failErr := tm.initChannelTracked(ctx, idx, tp, label)
		if ch != nil && qconn != nil {
			return ch, qconn
		}

		errStr := ""
		if failErr != nil {
			errStr = failErr.Error()
		}
		cls := tm.classifyError(failPhase, errStr)
		mainLog.Warn("[%s] init failed at %s (%s) class=%d — applying recovery", label, failPhase, errStr, cls)

		switch cls {
		case classBaleConnect:
			baleFails++
			if isTokenError(errStr) {
				tm.setChannelPhase(idx, PhaseError, "token revoked/expired: "+errStr)
				mainLog.Error("[%s] Stopping retries — token appears invalid", label)
				return nil, nil
			}
			if baleFails >= baleFailLimit {
				tm.setChannelPhase(idx, PhaseError, fmt.Sprintf("Bale connect failed %d times: %s", baleFails, errStr))
				mainLog.Error("[%s] Stopping retries after %d Bale connect failures", label, baleFails)
				return nil, nil
			}
			// Network block / throttle — backoff only (no call was started).

		case classCallPhase, classSFUPhase, classUnknown:
			// Layer 2: the paired server may be stranded in RESERVED/IN_CALL.
			// Force it back to IDLE before re-dialing so the retried call is
			// accepted instead of rejected as "busy".
			tm.setChannelPhase(idx, PhaseTeardown, "clean hangup of paired server")
			tm.endCallForPair(ctx, tp, label)
			baleFails = 0
		}

		select {
		case <-ctx.Done():
			return nil, nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// refreshChannel performs a clean teardown of the current channel resources and
// re-establishes a fresh connection. Used by both:
//   - Layer 2 death-reconnect (channel died unexpectedly)
//   - Layer 3 scheduled refresh (healthy rotation)
//
// It removes the QUIC connection from the routing pool (so traffic flows over
// surviving channels), tears down Bale/SFU/QUIC, runs the Clean Hangup Protocol
// against the paired server, then re-dials via initChannelWithRetry.
//
// Results are written back through the pointer receivers so the caller's
// monitor loop tracks the new connection.
func (tm *TunnelManager) refreshChannel(
	ctx context.Context,
	tunnelPool *pool.TunnelPool,
	curCh **channelState,
	curQ *quic.Connection,
	idx int,
	tp config.TokenPair,
	label string,
	mu *sync.Mutex,
	channels *[]*channelState,
) {
	// 1. Drop the QUIC connection from the routing pool FIRST so the
	//    round-robin balancer immediately steers traffic to other channels.
	if *curQ != nil {
		tunnelPool.Remove(*curQ)
		(*curQ).CloseWithError(0, "refresh/reconnect")
		*curQ = nil
	}

	// 2. Tear down the old Bale/SFU/QUIC stack.
	if *curCh != nil {
		old := *curCh
		go old.client.CleanupMessages()
		time.Sleep(500 * time.Millisecond)
		old.sfu.Close()
		old.client.Close()

		mu.Lock()
		for i, c := range *channels {
			if c == old {
				*channels = append((*channels)[:i], (*channels)[i+1:]...)
				break
			}
		}
		mu.Unlock()
		*curCh = nil
	}

	// 3. Layer 2: Clean Hangup Protocol — force the paired server to IDLE.
	tm.setChannelPhase(idx, PhaseTeardown, "clean hangup of paired server")
	tm.endCallForPair(ctx, tp, label)

	// 4. Re-dial with Layer 1+2 retry (ENDCALL on any further call-phase fail).
	tm.setChannelPhase(idx, PhaseBaleConnect, "")
	newCh, newQConn := tm.initChannelWithRetry(ctx, idx, tp, label)
	if newCh == nil || newQConn == nil {
		return
	}

	tm.setChannelPhase(idx, PhaseTunnelActive, "")
	tunnelPool.Add(newQConn, label)
	mu.Lock()
	*channels = append(*channels, newCh)
	mu.Unlock()

	*curCh = newCh
	*curQ = newQConn
	mainLog.Info("[%s] ✅ Channel refreshed/reconnected", label)
}
