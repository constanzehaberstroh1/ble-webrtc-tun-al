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
//      Exception: if the total configured pair count is 1, quorum is relaxed
//      to allow single-channel deployments to still refresh (accepting a
//      brief downtime window) — otherwise they never refresh at all.
//
// This prevents "load-shedding blackouts" where multiple lines refresh at the
// same time and overload the Bale signaling plane.
// =============================================================================

type refreshSupervisor struct {
	mu         sync.Mutex
	refreshing bool
}

// tryAcquire grants a refresh ticket if no other channel is refreshing AND the
// quorum-safety constraint is satisfied.
//
// FIX (Hole 3 — Single-Pair Quorum Starvation): totalPairs is now required.
// When totalPairs == 1 the minimum-active-count guard is bypassed so that
// single-channel deployments can still schedule periodic refreshes instead of
// being permanently locked out and silently degrading WebRTC quality.
func (rs *refreshSupervisor) tryAcquire(activeCount, totalPairs int) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.refreshing {
		return false
	}
	// Multi-channel: must keep at least one survivor during refresh.
	// Single-channel: relax the quorum constraint — a momentary gap is acceptable.
	if totalPairs > 1 && activeCount < 2 {
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

// endCallForPair sends BLETUN:ENDCALL to a single paired server account, waits
// for BLETUN:ENDCALL_ACK, then erases message traces. This forces the paired
// server out of any stuck RESERVED/IN_CALL state back to IDLE.
//
// FIX (Hole 2 — Dual-Session Token Collision): existingClient is now accepted.
// If the caller holds a healthy, already-connected *bale.Client for this token
// pair (e.g. during a Layer 3 scheduled refresh where the channel is still up),
// it is reused directly — no second login is performed. A second simultaneous
// connection with the same JWT would trigger a duplicate-session event on the
// platform, causing the server to drop the older (active) socket prematurely.
//
// existingClient == nil means the channel is dead and a fresh transient client
// must be created (the Layer 2 death-reconnect path).
func (tm *TunnelManager) endCallForPair(ctx context.Context, tp config.TokenPair, label string, existingClient *bale.Client) bool {
	if tp.ClientToken == "" || tp.TargetUserID == 0 {
		return false
	}

	var client *bale.Client
	isTransient := false

	if existingClient != nil {
		// Reuse the active session — avoids token collision.
		client = existingClient
		mainLog.Info("[%s] ENDCALL: reusing active signaling client (no duplicate login)", label)
	} else {
		// Channel is dead — spin up a short-lived transient client.
		client = bale.NewClient(tp.ClientToken)
		if err := client.Connect(); err != nil {
			mainLog.Warn("[%s] ENDCALL: transient Bale connect failed: %v", label, err)
			return false
		}
		isTransient = true
		client.StartPingLoop()
		time.Sleep(1 * time.Second)
	}

	// Drain stale messages so ACK detection starts from a clean state.
	for {
		select {
		case <-client.TextMsgCh:
		default:
			goto drained
		}
	}
drained:

	mainLog.Info("[%s] ENDCALL: sending to %d", label, tp.TargetUserID)
	if err := client.SendTextMessage(tp.TargetUserID, "BLETUN:ENDCALL"); err != nil {
		mainLog.Warn("[%s] ENDCALL: send failed: %v", label, err)
		if isTransient {
			client.Close()
		}
		return false
	}

	// Wait for ACK (up to 15s), respecting cancellation.
	timeout := time.NewTimer(15 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case <-ctx.Done():
			if isTransient {
				client.Close()
			}
			return false
		case msg := <-client.TextMsgCh:
			if msg == "BLETUN:ENDCALL_ACK" {
				mainLog.Info("[%s] ENDCALL: ACK received — server is IDLE", label)
				time.Sleep(500 * time.Millisecond)
				client.CleanupMessages()
				if isTransient {
					client.Close()
				}
				return true
			}
		case <-timeout.C:
			mainLog.Warn("[%s] ENDCALL: timeout (no ACK) — proceeding anyway", label)
			client.CleanupMessages()
			if isTransient {
				client.Close()
			}
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
		// In bonded mode initChannelTracked returns a non-nil ch with qconn==nil
		// (the master QUIC conn is shared and dialed separately). Only ch is
		// required there; requiring qconn!=nil would treat every bonded success
		// as a failure and loop forever (re-dialing the SFU each time).
		if ch != nil && (qconn != nil || tm.bonded) {
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
			// The channel is dead at this point, so existingClient = nil:
			// a transient client must be created (no collision risk here).
			tm.setChannelPhase(idx, PhaseTeardown, "clean hangup of paired server")
			tm.endCallForPair(ctx, tp, label, nil)
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
	//
	// FIX (Hole 1 — Graceful Traffic Drain):
	// Remove from pool first to block new stream assignments, then wait 3s
	// for in-flight bytes on active multiplexed streams to finish flushing
	// before issuing the hard CloseWithError. Without the drain window,
	// active downloads or long-lived API requests are killed mid-transfer.
	if *curQ != nil {
		tunnelPool.Remove(*curQ)
		time.Sleep(3 * time.Second) // allow active streams to drain
		(*curQ).CloseWithError(0, "refresh/reconnect")
		*curQ = nil
	}

	// 2. Tear down the old Bale/SFU/QUIC stack.
	//
	// FIX (Hole 2 — Token Collision): capture the active Bale client BEFORE
	// closing the channel so we can pass it to endCallForPair below, avoiding
	// a duplicate login with the same token while the main connection is still
	// technically alive on the server side.
	//
	// FIX (Hole 4 — Slice Mutation): build a new slice instead of in-place
	// truncation to avoid backing-array aliasing when other goroutines hold
	// a snapshot of the old slice header.
	var activeBaleClient *bale.Client
	if *curCh != nil {
		old := *curCh
		activeBaleClient = old.client // capture before teardown

		go old.client.CleanupMessages()
		time.Sleep(200 * time.Millisecond)
		old.sfu.Close()
		// Note: old.client is closed below, AFTER endCallForPair has finished
		// using it. Do NOT call old.client.Close() here.

		mu.Lock()
		newSlice := make([]*channelState, 0, len(*channels))
		for _, c := range *channels {
			if c != old {
				newSlice = append(newSlice, c)
			}
		}
		*channels = newSlice
		mu.Unlock()
		*curCh = nil
	}

	// 3. Layer 2: Clean Hangup Protocol — force the paired server to IDLE.
	//    Pass activeBaleClient so the existing session is reused if healthy
	//    (scheduled refresh path), or nil to create a transient one (dead path).
	tm.setChannelPhase(idx, PhaseTeardown, "clean hangup of paired server")
	tm.endCallForPair(ctx, tp, label, activeBaleClient)

	// Now safe to close the old Bale client — endCallForPair is done with it.
	if activeBaleClient != nil {
		activeBaleClient.Close()
		activeBaleClient = nil
	}

	// 4. Re-dial with Layer 1+2 retry (ENDCALL on any further call-phase fail).
	tm.setChannelPhase(idx, PhaseBaleConnect, "")
	newCh, newQConn := tm.initChannelWithRetry(ctx, idx, tp, label)
	if newCh == nil {
		// In bonded mode newQConn is intentionally nil (master QUIC is shared),
		// so only newCh is required. A nil newCh is a fatal reconnect failure.
		tm.setChannelPhase(idx, PhaseError, "reconnect yielded no channel — giving up")
		return
	}

	tm.setChannelPhase(idx, PhaseTunnelActive, "")
	if tm.bonded {
		// Re-register the refreshed lane's rtpconn with the shared bond.
		if newCh.sfu != nil {
			if rtpC := newCh.sfu.GetRTPConn(); rtpC != nil {
				tunnelPool.AddLane(rtpC, label)
				mainLog.Info("[%s] Bonded lane re-registered after refresh", label)
			}
		}
	} else {
		tunnelPool.Add(newQConn, label)
	}
	mu.Lock()
	*channels = append(*channels, newCh)
	mu.Unlock()

	*curCh = newCh
	*curQ = newQConn
	mainLog.Info("[%s] ✅ Channel refreshed/reconnected", label)
}
