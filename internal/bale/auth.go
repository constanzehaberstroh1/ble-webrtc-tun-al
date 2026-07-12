package bale

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// Bale client constants (app_version, API key, browser version, gRPC base,
// etc.) now live in the centralized constants store (constants.go) and are
// accessed via the thread-safe getters (AppVersion(), WebAPIKey(),
// BaleGRPCBase(), BrowserVersion(), …).  They are loaded from the database at
// startup and can be re-extracted from the live Bale bundle via the REST API.

// AuthClient handles the Bale OTP login flow via gRPC-Web.
type AuthClient struct {
	httpClient  *http.Client
	sessionID   string
	deviceUUID  string
	appVersionS string
	accessToken string // existing JWT token used as cookie for Envoy auth
}

// AuthResult holds the data returned after a successful ValidateCode call.
type AuthResult struct {
	Token       string // access_token cookie value
	UserID      int64
	AccessHash  int64
	DisplayName string
	Phone       string
}

// NewAuthClient creates a new Bale auth client.
func NewAuthClient() *AuthClient {
	return newAuthClient("")
}

// NewAuthClientWithToken creates an auth client that uses an existing access_token
// as a Cookie header. The Bale Envoy proxy requires this for the StartPhoneAuth call.
func NewAuthClientWithToken(token string) *AuthClient {
	return newAuthClient(token)
}

func newAuthClient(token string) *AuthClient {
	transport := &http.Transport{
		TLSClientConfig:   &tls.Config{},
		ForceAttemptHTTP2: true,
	}
	return &AuthClient{
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
		sessionID:   fmt.Sprintf("%d", time.Now().UnixMilli()),
		deviceUUID:  generateUUID(),
		appVersionS: AppVersion(),
		accessToken: token,
	}
}

// SetAccessToken sets an existing JWT to be sent as cookie.
func (a *AuthClient) SetAccessToken(token string) {
	a.accessToken = token
}

// fetchLatestAPIKey dynamically fetches the latest API key from the Bale web app's JS bundle.
func fetchLatestAPIKey(httpClient *http.Client) (string, error) {
	// 1. Fetch main page
	resp, err := httpClient.Get("https://web.bale.ai/")
	if err != nil {
		return "", fmt.Errorf("failed to fetch main page: %w", err)
	}
	defer resp.Body.Close()

	htmlBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	htmlStr := string(htmlBytes)

	// 2. Find the main index.js script
	re := regexp.MustCompile(`src="(/static/js/index\.[a-f0-9]+\.js)"`)
	matches := re.FindStringSubmatch(htmlStr)
	if len(matches) < 2 {
		return "", fmt.Errorf("could not find index JS file in HTML")
	}
	jsPath := matches[1]

	// 3. Fetch the JS file
	jsURL := "https://web.bale.ai" + jsPath
	jsResp, err := httpClient.Get(jsURL)
	if err != nil {
		return "", fmt.Errorf("failed to fetch JS bundle: %w", err)
	}
	defer jsResp.Body.Close()

	jsBytes, err := io.ReadAll(jsResp.Body)
	if err != nil {
		return "", err
	}
	jsStr := string(jsBytes)

	// 4. Extract the apiKey
	// Look for {id:4,apiKey:"C28D..."} (app_id=4 is Web)
	keyRe := regexp.MustCompile(`id:\s*4\s*,\s*apiKey:\s*["']([A-F0-9]{64})["']`)
	keyMatches := keyRe.FindStringSubmatch(jsStr)
	if len(keyMatches) < 2 {
		// Fallback: just find any 64-character hex string starting with C (like the original)
		// Or if not found, just grab all and return the last one (Web usually comes after iOS)
		fallbackRe := regexp.MustCompile(`apiKey:\s*["']([A-F0-9]{64})["']`)
		allMatches := fallbackRe.FindAllStringSubmatch(jsStr, -1)
		if len(allMatches) == 0 {
			return "", fmt.Errorf("could not find apiKey in JS bundle")
		}
		return allMatches[len(allMatches)-1][1], nil
	}

	return keyMatches[1], nil
}

// fetchLatestAppVersion fetches the current app_version from Bale's JS bundle.
// Bale uses a 6-digit integer (e.g. "154014") that they increment on each
// release. Clients with a stale version still connect and receive pongs, but
// Bale's gateway silently stops delivering push events (messages, calls).
//
// The version appears in the bundle as: appVersion:"154014" or APP_VERSION="154014"
func fetchLatestAppVersion(httpClient *http.Client) (string, error) {
	// 1. Fetch main page to find current JS bundle path.
	resp, err := httpClient.Get("https://web.bale.ai/")
	if err != nil {
		return "", fmt.Errorf("fetch main page: %w", err)
	}
	defer resp.Body.Close()
	htmlBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	htmlStr := string(htmlBytes)

	// 2. Find the main JS bundle.
	re := regexp.MustCompile(`src="(/static/js/index\.[a-f0-9]+\.js)"`)
	matches := re.FindStringSubmatch(htmlStr)
	if len(matches) < 2 {
		return "", fmt.Errorf("JS bundle not found in HTML")
	}

	// 3. Fetch the JS bundle.
	jsURL := "https://web.bale.ai" + matches[1]
	jsResp, err := httpClient.Get(jsURL)
	if err != nil {
		return "", fmt.Errorf("fetch JS bundle: %w", err)
	}
	defer jsResp.Body.Close()
	jsBytes, err := io.ReadAll(jsResp.Body)
	if err != nil {
		return "", err
	}
	jsStr := string(jsBytes)

	// 4. Extract app_version. The bundle contains patterns like:
	//    appVersion:"154014"   APP_VERSION:"154014"   "app_version","154014"
	//    version:154014        appVersion=154014
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`[Aa]pp[Vv]ersion[:\s=]+["']?(\d{5,7})["']?`),
		regexp.MustCompile(`APP_VERSION[:\s=]+["']?(\d{5,7})["']?`),
		regexp.MustCompile(`"app_version"\s*,\s*"(\d{5,7})"`),
		regexp.MustCompile(`"app_version":(\d{5,7})`),
		regexp.MustCompile(`buildNumber[:\s=]+["']?(\d{5,7})["']?`),
	}
	for _, pat := range patterns {
		if m := pat.FindStringSubmatch(jsStr); len(m) >= 2 {
			return m[1], nil
		}
	}
	return "", fmt.Errorf("app_version not found in JS bundle (url=%s)", jsURL)
}

// FetchAndUpdateClientMeta fetches the latest client-emulation constants
// (app_version, API key, LiveKit SDK/protocol versions, browser version, and
// infrastructure URLs) from Bale's live JS bundle and updates the centralized
// constants store.
//
// Call this once at server/client startup before creating any Client instances.
// On failure the persisted/default fallback values remain in use.
func FetchAndUpdateClientMeta() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// Try the comprehensive extractor first (parses all JS chunks dynamically).
	extracted, err := ExtractConstants(ctx)
	if err != nil {
		baleLog.Warn("Comprehensive extraction failed (%v) — falling back to index-only scrape", err)
		httpClient := &http.Client{Timeout: 20 * time.Second}
		if ver, ferr := fetchLatestAppVersion(httpClient); ferr == nil {
			SetAppVersion(ver)
			baleLog.Info("app_version updated (fallback): %s", ver)
		} else {
			baleLog.Warn("Could not fetch live app_version (%v) — using fallback %s", ferr, AppVersion())
		}
		if key, ferr := fetchLatestAPIKey(httpClient); ferr == nil {
			SetWebAPIKey(key)
		}
		return
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

	baleLog.Info("Client meta synced: app_version=%s sdk=%s protocol=%s browser=%s",
		extracted.AppVersion, extracted.SDKVersion, extracted.ProtocolVersion, extracted.BrowserVersion)
}

// StartPhoneAuth sends an OTP SMS to the given phone number.
// phoneNumber should include country code, e.g. 989151016774.
func (a *AuthClient) StartPhoneAuth(phoneNumber int64) (string, error) {
	// Build protobuf: StartPhoneAuthRequest
	// field 1 (int64):  phone_number
	// field 2 (int32):  app_id = 4 (WEB)
	// field 3 (string): api_key — hardcoded constant from Bale web app JS
	// field 4 (string): device_hash (device UUID)
	// field 5 (string): device_title
	// field 9 (varint): preferred_verification = 1 (SMS)
	// field 10 (bytes): flags [0x00, 0x01]

	var resp []byte
	var err error

	for attempt := 1; attempt <= 2; attempt++ {
		currentKey := WebAPIKey()

		var msg []byte
		msg = appendVarintField(msg, 1, uint64(phoneNumber))
		msg = appendVarintField(msg, 2, 4) // app_id = WEB
		msg = appendField(msg, 3, []byte(currentKey))
		msg = appendField(msg, 4, []byte(a.deviceUUID))
		msg = appendField(msg, 5, []byte(DeviceTitle()))
		msg = appendVarintField(msg, 9, 1) // SMS
		msg = appendField(msg, 10, []byte{0x00, 0x01})

		body := grpcWebEncode(msg)

		resp, _, err = a.doGRPCRequestFull("/bale.auth.v1.Auth/StartPhoneAuth", body)
		if err != nil {
			if strings.Contains(err.Error(), "invalid api_key") && attempt == 1 {
				baleLog.Warn("API key invalid. Attempting to fetch new key dynamically...")
				newKey, fetchErr := fetchLatestAPIKey(a.httpClient)
				if fetchErr != nil {
					baleLog.Error("Failed to fetch new API key: %v", fetchErr)
					return "", fmt.Errorf("StartPhoneAuth failed (invalid api_key) and fetch failed: %w", fetchErr)
				}
				baleLog.Info("Found new API key: %s", newKey)
				SetWebAPIKey(newKey)
				continue // Retry with new key
			}
			return "", fmt.Errorf("StartPhoneAuth request failed: %w", err)
		}

		// Success! Break retry loop
		break
	}

	baleLog.Info("StartPhoneAuth response: %d bytes, hex=%s", len(resp), hex.EncodeToString(resp))

	// Parse the response — may contain multiple gRPC frames (data + trailers)
	payload, err := grpcWebDecode(resp)
	if err != nil {
		return "", fmt.Errorf("decoding StartPhoneAuth response: %w", err)
	}

	// Extract the transaction hash (field 1, length-delimited string, usually 40 hex chars)
	txHash := extractStringField(payload, 1)
	if txHash == "" {
		// Try to find any 40-char hex string in the response
		txHash = findHexString(payload, 40)
	}
	if txHash == "" {
		baleLog.Warn("StartPhoneAuth payload hex: %s", hex.EncodeToString(payload))
		return "", fmt.Errorf("no transaction hash in StartPhoneAuth response")
	}

	baleLog.Info("StartPhoneAuth: SMS sent, txHash=%s", txHash)
	return txHash, nil
}

// ValidateCode validates the OTP code and returns the auth result.
func (a *AuthClient) ValidateCode(transactionHash, code string) (*AuthResult, error) {
	// Build protobuf: ValidateCodeRequest
	// field 1 (string): transaction_hash
	// field 2 (string): code
	// field 3 (message): { field 1 (varint): 1 }
	inner := appendVarintField(nil, 1, 1)

	var msg []byte
	msg = appendField(msg, 1, []byte(transactionHash))
	msg = appendField(msg, 2, []byte(code))
	msg = appendField(msg, 3, inner)

	body := grpcWebEncode(msg)

	resp, respHeaders, err := a.doGRPCRequestFull("/bale.auth.v1.Auth/ValidateCode", body)
	if err != nil {
		return nil, fmt.Errorf("ValidateCode request failed: %w", err)
	}

	// Extract access token from Set-Cookie header
	token := ""
	for _, cookie := range respHeaders["Set-Cookie"] {
		if strings.Contains(cookie, "access_token=") {
			parts := strings.SplitN(cookie, "access_token=", 2)
			if len(parts) == 2 {
				tokenPart := parts[1]
				if idx := strings.IndexAny(tokenPart, ";, "); idx > 0 {
					token = tokenPart[:idx]
				} else {
					token = tokenPart
				}
			}
		}
	}

	// Parse the response protobuf for user info
	payload, err := grpcWebDecode(resp)
	if err != nil {
		return nil, fmt.Errorf("decoding ValidateCode response: %w", err)
	}

	result := &AuthResult{Token: token}

	// If no token from cookie, look for it in the response body
	if result.Token == "" {
		// The token might be in the protobuf response as a string field
		strs := extractAllStrings(payload)
		for _, s := range strs {
			if len(s) > 50 && !strings.Contains(s, " ") {
				result.Token = s
				break
			}
		}
	}

	// Extract user ID from the response (field 1, varint in a nested message)
	result.UserID = extractFirstLargeVarint(payload)

	// Extract readable strings for display name and phone
	strs := extractAllStrings(payload)
	for _, s := range strs {
		if len(s) >= 10 && len(s) <= 15 && isPhoneNumber(s) {
			result.Phone = s
		} else if len(s) > 1 && len(s) < 50 && !isNumericOnly(s) && result.DisplayName == "" {
			// Skip known protocol strings
			if s != "bale.auth.v1.Auth" && s != "ValidateCode" {
				result.DisplayName = s
			}
		}
	}

	baleLog.Info("ValidateCode: token=%s userID=%d name=%q phone=%q",
		truncStr(result.Token, 20), result.UserID, result.DisplayName, result.Phone)

	if result.Token == "" {
		return nil, fmt.Errorf("no access token received from ValidateCode")
	}

	return result, nil
}

// doGRPCRequestFull sends a gRPC-Web POST and returns body + headers.
func (a *AuthClient) doGRPCRequestFull(path string, body []byte) ([]byte, http.Header, error) {
	url := BaleGRPCBase() + path
	ua := LinuxUserAgent()
	origin := BaleWebOrigin()
	chromeMajor := ChromeMajorVersion()

	// Optional: Send OPTIONS preflight just like the browser
	optionsReq, _ := http.NewRequest("OPTIONS", url, nil)
	optionsReq.Header.Set("Origin", origin)
	optionsReq.Header.Set("Access-Control-Request-Method", "POST")
	optionsReq.Header.Set("Access-Control-Request-Headers", "app_version,browser_type,browser_version,content-type,mt_app_version,mt_browser_type,mt_browser_version,mt_os_type,mt_session_id,os_type,session_id,x-grpc-web")
	optionsReq.Header.Set("User-Agent", ua)
	a.httpClient.Do(optionsReq)

	baleLog.Info("Sending POST to %s with payload hex: %x", url, body)

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}

	appVer := AppVersion()
	bv := BrowserVersion()

	req.Header.Set("Content-Type", "application/grpc-web+proto")
	req.Header.Set("x-grpc-web", "1")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Origin", origin)
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-site")
	req.Header.Set("sec-ch-ua", `"Not)A;Brand";v="8", "Chromium";v="`+chromeMajor+`"`)
	req.Header.Set("sec-ch-ua-mobile", "?0")
	req.Header.Set("sec-ch-ua-platform", `"Linux"`)
	req.Header.Set("session_id", a.sessionID)
	req.Header.Set("mt_session_id", a.sessionID)
	req.Header.Set("os_type", "4")
	req.Header.Set("mt_os_type", "4")
	req.Header.Set("browser_type", "1")
	req.Header.Set("mt_browser_type", "1")
	req.Header.Set("browser_version", bv)
	req.Header.Set("mt_browser_version", bv)
	req.Header.Set("app_version", appVer)
	req.Header.Set("mt_app_version", appVer)
	req.Header.Set("DNT", "1")
	req.Header.Set("Sec-GPC", "1")

	// Send access_token cookie if available (required for authenticated endpoints)
	if a.accessToken != "" {
		req.Header.Set("Cookie", "access_token="+a.accessToken)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	baleLog.Info("gRPC response: status=%d, content-type=%s, content-length=%d",
		resp.StatusCode, resp.Header.Get("Content-Type"), resp.ContentLength)

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("reading response body: %w", err)
	}

	baleLog.Info("gRPC response body: %d bytes, Headers: %v, Trailers: %v", len(respBody), resp.Header, resp.Trailer)

	if resp.StatusCode != 200 {
		return nil, resp.Header, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	grpcStatus := resp.Header.Get("grpc-status")
	if grpcStatus == "" {
		grpcStatus = resp.Trailer.Get("grpc-status")
	}
	if grpcStatus != "" && grpcStatus != "0" {
		grpcMsg := resp.Header.Get("grpc-message")
		if grpcMsg == "" {
			grpcMsg = resp.Trailer.Get("grpc-message")
		}
		return nil, resp.Header, fmt.Errorf("gRPC error: status=%s message=%s", grpcStatus, grpcMsg)
	}

	return respBody, resp.Header, nil
}

// ---- gRPC-Web frame encoding/decoding ----

// grpcWebEncode wraps a protobuf message in a gRPC-Web frame.
// Format: [1 byte flag=0x00] [4 bytes big-endian length] [message]
func grpcWebEncode(msg []byte) []byte {
	frame := make([]byte, 5+len(msg))
	frame[0] = 0x00 // not compressed
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(msg)))
	copy(frame[5:], msg)
	return frame
}

// grpcWebDecode extracts the protobuf message from a gRPC-Web frame.
func grpcWebDecode(data []byte) ([]byte, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("gRPC-Web frame too short (%d bytes)", len(data))
	}
	// flag := data[0]
	length := binary.BigEndian.Uint32(data[1:5])
	if int(length) > len(data)-5 {
		// Return whatever we have
		return data[5:], nil
	}
	return data[5 : 5+length], nil
}

// ---- Protobuf field extraction helpers ----

// extractStringField extracts a string from a specific protobuf field number.
func extractStringField(data []byte, fieldNum int) string {
	expectedTag := byte((fieldNum << 3) | 2) // wire type 2 = length-delimited
	for i := 0; i < len(data)-2; i++ {
		if data[i] == expectedTag {
			length, n := binary.Uvarint(data[i+1:])
			if n > 0 && int(length) > 0 && i+1+n+int(length) <= len(data) {
				content := data[i+1+n : i+1+n+int(length)]
				if isPrintableString(content) {
					return string(content)
				}
			}
		}
	}
	return ""
}

// findHexString looks for a hex string of the given length in protobuf data.
func findHexString(data []byte, hexLen int) string {
	text := string(data)
	for i := 0; i <= len(text)-hexLen; i++ {
		candidate := text[i : i+hexLen]
		if isHexString(candidate) {
			return candidate
		}
	}
	return ""
}

func isHexString(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return len(s) > 0
}

// extractFirstLargeVarint finds the first varint > 1000000 in the data.
func extractFirstLargeVarint(data []byte) int64 {
	for i := 0; i < len(data)-1; i++ {
		tag := data[i]
		wireType := tag & 0x07
		if wireType == 0 { // varint
			val, n := binary.Uvarint(data[i+1:])
			if n > 0 && val > 1000000 {
				return int64(val)
			}
		}
	}
	return 0
}

// ---- Crypto helpers ----

func generateDeviceHash() string {
	b := make([]byte, 32)
	rand.Read(b)
	h := sha256.Sum256(b)
	return strings.ToUpper(hex.EncodeToString(h[:]))
}

func generateUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 2
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
