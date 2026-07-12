package bale

// extractor.go — Dynamic Upstream Parameter Extraction Engine.
//
// Scrapes the Bale web app's live JavaScript bundles (web.bale.ai) and
// extracts the client-emulation constants that Bale changes with each release:
//
//   - app_version          (e.g. "154014")
//   - web API key           (64-hex)
//   - LiveKit SDK version   (e.g. "2.13.6")
//   - LiveKit protocol       (e.g. "15"  → subprotocol "lk-protocol-15")
//   - browser version       (e.g. "138.0.0.0")
//   - infrastructure URLs    (Bale WS, gRPC base, web origins)
//
// Design goals (from the protocol-design review):
//
//  1. Polymorphic chunk names — never look for a static file like
//     index.<hash>.js.  The landing-page HTML is parsed first to discover
//     all <script src> paths dynamically, so chunk-hash changes can't break the
//     extractor.
//  2. Semantic regex — match structural code patterns (lk-protocol-<N>,
//     adaptive_stream, apiKey) rather than minified variable names.
//  3. Graceful fallback — anything that can't be found keeps its current
//     (persisted or default) value, so a failed scrape never breaks the
//     connection.
//  4. Fingerprint mimicry — requests use a real Chrome User-Agent so the
//     upstream WAF doesn't drop the scraper.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// ExtractedConstants holds everything the scraper managed to find upstream.
// Empty fields mean "not found" — the caller keeps the existing value.
type ExtractedConstants struct {
	AppVersion      string
	WebAPIKey       string
	SDKVersion      string
	ProtocolVersion string
	BrowserVersion  string
	BaleWSURL       string
	BaleGRPCBase    string
	LiveKitOrigin   string
	BaleWebOrigin   string
}

// BaleWebBaseURL is the landing page we scrape for script discovery.
const BaleWebBaseURL = "https://web.bale.ai/"

// maxScriptBytes limits how much of a single JS chunk we download into memory.
const maxScriptBytes = 8 * 1024 * 1024 // 8 MiB

// maxScriptChunks limits how many JS files we audit per scrape.
const maxScriptChunks = 30

// Pre-compiled regex patterns (structural / semantic — immune to minification).

// Script source discovery in the landing HTML.
var reScriptSrc = regexp.MustCompile(`src=["']([^"']*\.js[^"']*)["']`)

// app_version — 5-7 digit integer in patterns like appVersion:"154014".
var reAppVersion = []*regexp.Regexp{
	regexp.MustCompile(`[Aa]pp[Vv]ersion[:\s=]+["']?(\d{5,7})["']?`),
	regexp.MustCompile(`APP_VERSION[:\s=]+["']?(\d{5,7})["']?`),
	regexp.MustCompile(`"app_version"\s*,\s*"(\d{5,7})"`),
	regexp.MustCompile(`"app_version":\s*(\d{5,7})`),
	regexp.MustCompile(`buildNumber[:\s=]+["']?(\d{5,7})["']?`),
}

// API key — 64-char uppercase hex near id:4 (Web app_id).
var reAPIKeyPrimary = regexp.MustCompile(`id:\s*4\s*,\s*apiKey:\s*["']([A-F0-9]{64})["']`)
var reAPIKeyFallback = regexp.MustCompile(`apiKey:\s*["']([A-F0-9]{64})["']`)

// LiveKit protocol version — the subprotocol string is the most reliable
// signal: lk-protocol-15.
var reLKProtocol = regexp.MustCompile(`lk-protocol-(\d+)`)

// LiveKit SDK version — a semver near the livekit URL-building code.  The
// livekit-client sets query params version / protocol / adaptive_stream
// together, so we look for the version string adjacent to those markers.
var reLKSDKVersion = regexp.MustCompile(`version["'?:\s,]+["'](\d+\.\d+\.\d+)["']`)

// LiveKit chunk signatures — if a chunk contains any of these, it's the
// livekit-client bundle.
var lkSignatures = []string{
	"adaptive_stream",
	"auto_subscribe",
	"lk-protocol-",
	"SignalRequest",
	"JoinResponse",
	"addTrack",
}

// Browser version — Chrome major version.  We construct "<N>.0.0.0" from it.
var reChromeVersion = regexp.MustCompile(`Chrome/(\d+)`)
var reChromiumSecCHUA = regexp.MustCompile(`Chromium";v="(\d+)"`)

// Infrastructure URLs.
var reBaleWSURL = regexp.MustCompile(`(wss?://[a-zA-Z0-9.-]+\.bale\.ai[a-zA-Z0-9./_:-]*)`)
var reBaleHTTPURL = regexp.MustCompile(`(https?://[a-zA-Z0-9.-]+\.bale\.ai[a-zA-Z0-9./_:-]*)`)
var reBleIrURL = regexp.MustCompile(`(https?://[a-zA-Z0-9.-]+\.ble\.ir[a-zA-Z0-9./_:-]*)`)

// scrapeClient builds an HTTP client that mimics a real Chrome browser.
func scrapeClient() *http.Client {
	return &http.Client{Timeout: 20 * time.Second}
}

// scrapeUserAgent returns a Chrome User-Agent using the current browser version.
func scrapeUserAgent() string {
	return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/" + BrowserVersion() + " Safari/537.36"
}

// httpGet fetches a URL with a Chrome-like UA and returns the body as a string.
func httpGet(ctx context.Context, client *http.Client, url string) (string, int, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("User-Agent", scrapeUserAgent())
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", resp.StatusCode, fmt.Errorf("HTTP %d for %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxScriptBytes))
	if err != nil {
		return "", resp.StatusCode, err
	}
	return string(body), resp.StatusCode, nil
}

// resolveScriptURL turns a script src attribute into an absolute URL.
func resolveScriptURL(src string) string {
	switch {
	case strings.HasPrefix(src, "http://") || strings.HasPrefix(src, "https://"):
		return src
	case strings.HasPrefix(src, "//"):
		return "https:" + src
	case strings.HasPrefix(src, "/"):
		return "https://web.bale.ai" + src
	default:
		return "https://web.bale.ai/" + src
	}
}

// findScriptURLs parses the landing-page HTML and returns all unique .js URLs.
func findScriptURLs(htmlStr string) []string {
	matches := reScriptSrc.FindAllStringSubmatch(htmlStr, -1)
	seen := make(map[string]bool)
	var urls []string
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		u := resolveScriptURL(m[1])
		if !seen[u] {
			seen[u] = true
			urls = append(urls, u)
		}
	}
	return urls
}

// isLiveKitChunk returns true if the JS content looks like the livekit bundle.
func isLiveKitChunk(content string) bool {
	for _, sig := range lkSignatures {
		if strings.Contains(content, sig) {
			return true
		}
	}
	return false
}

// extractAppVersion searches content for the Bale app_version.
func extractAppVersion(content string) string {
	for _, re := range reAppVersion {
		if m := re.FindStringSubmatch(content); len(m) >= 2 {
			return m[1]
		}
	}
	return ""
}

// extractAPIKey searches content for the 64-hex API key.
func extractAPIKey(content string) string {
	if m := reAPIKeyPrimary.FindStringSubmatch(content); len(m) >= 2 {
		return m[1]
	}
	if m := reAPIKeyFallback.FindStringSubmatch(content); len(m) >= 2 {
		return m[1]
	}
	return ""
}

// extractProtocolVersion searches content for the lk-protocol-<N> subprotocol.
func extractProtocolVersion(content string) string {
	if m := reLKProtocol.FindStringSubmatch(content); len(m) >= 2 {
		return m[1]
	}
	return ""
}

// extractSDKVersion searches livekit-chunk content for the SDK semver.
func extractSDKVersion(content string) string {
	matches := reLKSDKVersion.FindAllStringSubmatch(content, -1)
	// Prefer the match that appears closest to "protocol" / "adaptive_stream".
	bestIdx := -1
	bestDist := -1
	protoIdx := strings.Index(content, "protocol")
	if protoIdx < 0 {
		protoIdx = strings.Index(content, "adaptive_stream")
	}
	for i, m := range matches {
		if len(m) < 2 {
			continue
		}
		matchIdx := strings.Index(content, m[0])
		if matchIdx < 0 {
			continue
		}
		if protoIdx >= 0 {
			dist := matchIdx - protoIdx
			if dist < 0 {
				dist = -dist
			}
			if bestIdx < 0 || dist < bestDist {
				bestIdx = i
				bestDist = dist
			}
		} else {
			// No anchor — take the first match.
			return m[1]
		}
	}
	if bestIdx >= 0 {
		return matches[bestIdx][1]
	}
	return ""
}

// extractBrowserVersion searches content for a Chrome major version.
func extractBrowserVersion(content string) string {
	if m := reChromiumSecCHUA.FindStringSubmatch(content); len(m) >= 2 {
		return m[1] + ".0.0.0"
	}
	if m := reChromeVersion.FindStringSubmatch(content); len(m) >= 2 {
		return m[1] + ".0.0.0"
	}
	return ""
}

// extractInfraURLs searches content for Bale/ble infrastructure URLs.
func extractInfraURLs(content string) (wsURL, grpcBase, livekitOrigin, baleWebOrigin string) {
	if m := reBaleWSURL.FindStringSubmatch(content); len(m) >= 2 {
		wsURL = strings.TrimRight(m[1], "/")
		// Normalize: ensure it ends with /ws/ like the Bale signaling endpoint.
		if strings.Contains(wsURL, "/ws") {
			if !strings.HasSuffix(wsURL, "/ws/") {
				if strings.HasSuffix(wsURL, "/ws") {
					wsURL += "/"
				}
			}
		} else {
			wsURL = wsURL + "/ws/"
		}
	}
	if m := reBaleHTTPURL.FindStringSubmatch(content); len(m) >= 2 {
		u := strings.TrimRight(m[1], "/")
		// The gRPC-Web base is the https URL without a trailing path.
		if strings.HasPrefix(u, "https://") {
			grpcBase = u
			baleWebOrigin = "https://web.bale.ai"
		}
	}
	if m := reBleIrURL.FindStringSubmatch(content); len(m) >= 2 {
		u := strings.TrimRight(m[1], "/")
		if strings.HasPrefix(u, "https://") {
			livekitOrigin = u
		}
	}
	return
}

// ExtractConstants scrapes the Bale web bundle and returns all discovered
// constants.  Fields that could not be found are left empty; the caller keeps
// existing values for those.
func ExtractConstants(ctx context.Context) (*ExtractedConstants, error) {
	client := scrapeClient()

	// 1. Fetch the landing page to discover script URLs dynamically.
	htmlStr, _, err := httpGet(ctx, client, BaleWebBaseURL)
	if err != nil {
		return nil, fmt.Errorf("fetch landing page: %w", err)
	}

	scriptURLs := findScriptURLs(htmlStr)
	if len(scriptURLs) == 0 {
		return nil, errors.New("no JavaScript bundles found in landing page")
	}

	// 2. Start from current values so we only override what we actually find.
	result := &ExtractedConstants{
		AppVersion:      AppVersion(),
		WebAPIKey:       WebAPIKey(),
		SDKVersion:      SDKVersion(),
		ProtocolVersion: ProtocolVersion(),
		BrowserVersion:  BrowserVersion(),
		BaleWSURL:       BaleWSURL(),
		BaleGRPCBase:    BaleGRPCBase(),
		LiveKitOrigin:   LiveKitOrigin(),
		BaleWebOrigin:   BaleWebOrigin(),
	}

	// Track which constants we still need to find.
	needAppVer := true
	needAPIKey := true
	needProtocol := true
	needSDK := true
	needBrowser := true
	needInfra := true

	audited := 0
	for _, u := range scriptURLs {
		if audited >= maxScriptChunks {
			break
		}
		if !needAppVer && !needAPIKey && !needProtocol && !needSDK && !needBrowser && !needInfra {
			break
		}

		content, _, err := httpGet(ctx, client, u)
		if err != nil {
			continue
		}
		audited++

		// app_version + API key (usually in the main app bundle).
		if needAppVer {
			if v := extractAppVersion(content); v != "" {
				result.AppVersion = v
				needAppVer = false
			}
		}
		if needAPIKey {
			if v := extractAPIKey(content); v != "" {
				result.WebAPIKey = v
				needAPIKey = false
			}
		}

		// LiveKit constants — only search chunks that look like the livekit bundle.
		lkChunk := isLiveKitChunk(content)
		if lkChunk {
			if needProtocol {
				if v := extractProtocolVersion(content); v != "" {
					result.ProtocolVersion = v
					needProtocol = false
				}
			}
			if needSDK {
				if v := extractSDKVersion(content); v != "" {
					result.SDKVersion = v
					needSDK = false
				}
			}
		}

		// Browser version (anywhere in the bundle).
		if needBrowser {
			if v := extractBrowserVersion(content); v != "" {
				result.BrowserVersion = v
				needBrowser = false
			}
		}

		// Infrastructure URLs (anywhere).
		if needInfra {
			ws, grpc, lko, bwo := extractInfraURLs(content)
			if ws != "" {
				result.BaleWSURL = ws
			}
			if grpc != "" {
				result.BaleGRPCBase = grpc
			}
			if lko != "" {
				result.LiveKitOrigin = lko
			}
			if bwo != "" {
				result.BaleWebOrigin = bwo
			}
			if ws != "" || grpc != "" || lko != "" || bwo != "" {
				// Keep searching — we might find more in other chunks.
			}
		}
	}

	baleLog.Info("Extraction complete: app_version=%s api_key=%s sdk=%s protocol=%s browser=%s",
		result.AppVersion, maskKey(result.WebAPIKey), result.SDKVersion, result.ProtocolVersion, result.BrowserVersion)

	return result, nil
}

// maskKey returns a masked version of the API key for logging.
func maskKey(key string) string {
	if len(key) <= 12 {
		return strings.Repeat("*", len(key))
	}
	return key[:6] + "..." + key[len(key)-6:]
}

// SyncConstants runs a full extraction, applies the results to the in-memory
// store, persists them to the database, and returns the updated snapshot.
// This is the function called by the REST API "Sync from Bale" button.
func SyncConstants(ctx context.Context, db SettingsStore) (*ConstantSnapshot, error) {
	extracted, err := ExtractConstants(ctx)
	if err != nil {
		return nil, err
	}

	// Apply all extracted values to the in-memory store.
	ApplySnapshot(ConstantSnapshot{
		AppVersion:      extracted.AppVersion,
		WebAPIKey:       extracted.WebAPIKey,
		SDKVersion:      extracted.SDKVersion,
		ProtocolVersion: extracted.ProtocolVersion,
		BrowserVersion:  extracted.BrowserVersion,
		BaleWSURL:       extracted.BaleWSURL,
		BaleGRPCBase:    extracted.BaleGRPCBase,
		LiveKitOrigin:   extracted.LiveKitOrigin,
		BaleWebOrigin:   extracted.BaleWebOrigin,
	})

	// Persist to database so values survive restarts.
	if db != nil {
		if err := PersistToSettings(db); err != nil {
			baleLog.Warn("Extraction applied but DB persist failed: %v", err)
		}
	}

	MarkSynced(db)

	snap := Snapshot()
	return &snap, nil
}
