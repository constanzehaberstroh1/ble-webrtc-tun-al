package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/salman/ble-webrtc-tun/internal/artery"
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
//   - Artery orchestrator autonomous revival
//
// It removes the QUIC connection from the routing pool (so traffic flows over
// surviving connections), tears down Bale/SFU/QUIC, runs the Clean Hangup Protocol
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
	// 1. Unregister the QUIC connection from the pool FIRST so the
	//    ECF scheduler immediately steers traffic to other arteries.
	if *curQ != nil {
		tunnelPool.Unregister(label)
		time.Sleep(3 * time.Second) // allow active streams to drain
		(*curQ).CloseWithError(0, "refresh/reconnect")
		*curQ = nil
	}

	// 2. Tear down the old Bale/SFU/QUIC stack.
	var activeBaleClient *bale.Client
	if *curCh != nil {
		old := *curCh
		activeBaleClient = old.client // capture before teardown

		go old.client.CleanupMessages()
		time.Sleep(200 * time.Millisecond)
		old.sfu.Close()

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
	tm.setChannelPhase(idx, PhaseTeardown, "clean hangup of paired server")
	tm.endCallForPair(ctx, tp, label, activeBaleClient)

	// Now safe to close the old Bale client.
	if activeBaleClient != nil {
		activeBaleClient.Close()
		activeBaleClient = nil
	}

	// 4. Re-dial with Layer 1+2 retry.
	tm.setChannelPhase(idx, PhaseBaleConnect, "")
	newCh, newQConn := tm.initChannelWithRetry(ctx, idx, tp, label)
	if newCh == nil {
		tm.setChannelPhase(idx, PhaseError, "reconnect yielded no channel — giving up")
		return
	}

	tm.setChannelPhase(idx, PhaseTunnelActive, "")
	// Register the new independent QUIC connection into the pool.
	// Use RegisterWithIndex to associate the artery with its pair index
	// for the orchestrator's telemetry tracking.
	if newQConn != nil {
		tunnelPool.RegisterWithIndex(label, newQConn, idx)
	}
	mu.Lock()
	*channels = append(*channels, newCh)
	mu.Unlock()

	*curCh = newCh
	*curQ = newQConn
	mainLog.Info("[%s] ✅ Channel refreshed/reconnected", label)
}

// =============================================================================
// Artery Orchestrator Revival Bridge
//
// Bridges the artery.Orchestrator's autonomous revival pipeline to the
// TunnelManager's refreshChannel() method.  When the orchestrator detects
// a dead/quarantined artery, it calls this function which performs the
// full Bale WS teardown, re-auth, SFU reconnect, and QUIC re-dial.
// =============================================================================

// startArteryOrchestrator creates and starts the artery orchestrator as a
// background goroutine.  The revival callback is wired to refreshChannel.
func (tm *TunnelManager) startArteryOrchestrator(
	ctx context.Context,
	tunnelPool *pool.TunnelPool,
	channels *[]*channelState,
	mu *sync.Mutex,
	pairs []config.TokenPair,
) *artery.Orchestrator {
	revivalFn := func(revCtx context.Context, label string, pairIndex int) {
		mainLog.Info("[Orchestrator] Revival triggered for %s (pair=%d)", label, pairIndex)

		// Find the channel state and token pair for this artery
		mu.Lock()
		var curCh *channelState
		for _, ch := range *channels {
			if ch.label == label {
				curCh = ch
				break
			}
		}
		mu.Unlock()

		// Find the token pair
		var tp config.TokenPair
		if pairIndex >= 0 && pairIndex < len(pairs) {
			tp = pairs[pairIndex]
		} else {
			mainLog.Error("[Orchestrator] Invalid pair index %d for %s", pairIndex, label)
			return
		}

		var curQ quic.Connection
		if curCh != nil {
			curQ = curCh.qconn
		}

		// Perform the full refresh (teardown + re-auth + reconnect)
		tm.refreshChannel(revCtx, tunnelPool, &curCh, &curQ, pairIndex, tp, label, mu, channels)

		// After successful revival, transition the artery to SHADOW state
		// for stabilization.  The orchestrator's hysteresis engine will
		// promote it to ACTIVE after 15s if metrics are good.
		if curCh != nil && curQ != nil {
			a := tunnelPool.GetArtery(label)
			if a != nil {
				// The artery was re-registered by refreshChannel as ACTIVE.
				// Transition to SHADOW for stabilization period.
				_ = a.TransitionTo(artery.StateShadow)
				mainLog.Info("[Orchestrator] %s revived — in SHADOW for stabilization", label)
			}
		} else {
			mainLog.Warn("[Orchestrator] Revival of %s failed — artery stays in REVIVING/DEAD", label)
		}
	}

	orch := artery.NewOrchestrator(tunnelPool.ArteryPool(), revivalFn)
	go orch.Start(ctx)
	return orch
}
