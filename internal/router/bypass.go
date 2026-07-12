// Package router — bypass.go: High-performance request splitting engine.
//
// This layer parses resolved target matrices and determines if a connection
// request must be routed over the local network interface or through the
// WebRTC QUIC tunnel pool.
//
// It handles two primary evaluation pipelines:
//
//  1. The Domain Matching Matrix — matches target requests against
//     administrative exception entries (custom bypass domains).
//  2. The High-Velocity CIDR Lookup Trie — evaluates resolved IPv4 addresses
//     against domestic Iranian IP ranges (APNIC IR network data) to determine
//     localization context.
//
// CRITICAL INVARIANT: Bale application endpoints must NEVER bypass the
// tunnel, regardless of where their data centers are physically located.
// This is enforced as the very first check in EvaluateRoutingPath so that
// Bale traffic is always carried through the secure WebRTC artery pool.
package router

import (
	"net"
	"strings"
	"sync/atomic"
)

// baleProtectedSuffixes are domain suffixes that belong to the Bale
// application itself.  Traffic to these endpoints must always traverse the
// tunnel — they are never eligible for direct local routing even if their
// resolved IPs fall inside Iranian CIDR blocks.
var baleProtectedSuffixes = []string{
	".bale.ai",
	".ble.ir",
}

func isBaleDomain(host string) bool {
	lower := strings.ToLower(strings.TrimSpace(host))
	if lower == "bale.ai" || lower == "ble.ir" {
		return true
	}
	for _, suffix := range baleProtectedSuffixes {
		if strings.HasSuffix(lower, suffix) {
			return true
		}
	}
	return false
}

// BypassEngine classifies proxy connection targets and decides whether they
// should bypass the WebRTC tunnel (direct local routing) or be forwarded
// through the QUIC artery pool.
//
// The engine is built from an admin-configured custom domain list and a
// curated set of Iranian IPv4 CIDR blocks.  It is hot-swappable: the
// TunnelManager rebuilds it when the admin updates the bypass-domains
// setting, so traffic classification changes take effect instantly without
// a process restart.
type BypassEngine struct {
	// customDomains is the compiled set of admin-defined bypass domains.
	// Stored behind an atomic pointer so concurrent EvaluateRoutingPath
	// callers are lock-free and hot-rebuild is instant.
	customDomains atomic.Pointer[map[string]bool]

	// iranSubnets is the compiled set of Iranian IPv4 CIDRs.  This is a
	// read-only slice shared across all rebuilds (the CIDR list itself does
	// not change at runtime), so it is set once at construction time.
	iranSubnets []*net.IPNet
}

// NewBypassEngine builds a BypassEngine from a comma-separated custom domain
// list and a slice of Iranian CIDR strings.  Invalid CIDRs are silently
// skipped.  If irCIDRs is nil, the default curated Iranian list is used.
func NewBypassEngine(customList string, irCIDRs []string) *BypassEngine {
	be := &BypassEngine{}

	be.SetCustomDomains(customList)

	if irCIDRs == nil {
		irCIDRs = IranianCIDRs()
	}

	be.iranSubnets = make([]*net.IPNet, 0, len(irCIDRs))
	for _, cidr := range irCIDRs {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err == nil {
			be.iranSubnets = append(be.iranSubnets, ipNet)
		}
	}
	return be
}

// SetCustomDomains atomically replaces the admin-defined bypass domain set
// from a comma-separated string.  This enables hot-swapping from the admin
// dashboard without dropping in-flight classification calls.
func (be *BypassEngine) SetCustomDomains(customList string) {
	domains := make(map[string]bool)
	for _, domain := range strings.Split(customList, ",") {
		trimmed := strings.TrimSpace(strings.ToLower(domain))
		if trimmed != "" {
			domains[trimmed] = true
		}
	}
	be.customDomains.Store(&domains)
}

// CustomDomains returns the current list of admin-defined bypass domains as
// a comma-separated string (sorted-independent).  Used by the API to report
// the current configuration.
func (be *BypassEngine) CustomDomains() []string {
	dp := be.customDomains.Load()
	if dp == nil {
		return nil
	}
	out := make([]string, 0, len(*dp))
	for d := range *dp {
		out = append(out, d)
	}
	return out
}

// EvaluateRoutingPath runs classification checks to enforce strict tunnel
// exceptions.  Returns true if the target should bypass the tunnel and route
// directly over the local network interface, false if it must traverse the
// WebRTC QUIC artery pool.
//
// host is the original target hostname (domain or IP literal, case-insensitive).
// ip is the resolved IPv4 address (may be nil if resolution failed or the
// target was already an IP that couldn't be classified).
func (be *BypassEngine) EvaluateRoutingPath(host string, ip net.IP) bool {
	lowerHost := strings.ToLower(strings.TrimSpace(host))

	// CRITICAL INVARIANT: Bale application endpoints must NEVER bypass the
	// tunnel, regardless of where their data centers are physically located.
	if isBaleDomain(lowerHost) {
		return false
	}

	// Check 1: User-defined custom domain matching (exact + subdomain).
	dp := be.customDomains.Load()
	if dp != nil {
		if (*dp)[lowerHost] {
			return true
		}
		for cd := range *dp {
			if strings.HasSuffix(lowerHost, "."+cd) {
				return true
			}
		}
	}

	// Check 2: High-velocity CIDR verification against Iranian IP
	// allocations.  If the resolved IP falls inside a domestic Iranian
	// range, authorize direct local routing.
	if ip != nil {
		ip4 := ip.To4()
		if ip4 != nil {
			for _, subnet := range be.iranSubnets {
				if subnet.Contains(ip4) {
					return true
				}
			}
		}
	}

	// Default: Route through the WebRTC tunnel pool.
	return false
}
