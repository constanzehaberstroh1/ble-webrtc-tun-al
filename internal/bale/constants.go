package bale

// constants.go — Centralized, thread-safe store for the Bale/LiveKit client
// emulation constants.
//
// These values (app_version, LiveKit SDK/protocol versions, browser version,
// API key, infrastructure URLs) are extracted directly from the Bale web app
// (web.bale.ai) and hardcoded statically throughout the codebase.  Bale
// changes them with every release, so this store makes them hot-swappable:
//
//   - At startup LoadFromSettings() pulls persisted values from the database.
//   - FetchAndUpdateClientMeta() / ExtractConstants() scrape the live bundle.
//   - The REST API exposes GET/POST endpoints to view and trigger a sync.
//   - All connection code (bale client, LiveKit signaling/SFU) reads from the
//     getters here instead of string literals, so an update takes effect for
//     new connections immediately without a restart.
//
// The connection method itself is unchanged — only the constant VALUES become
// dynamic.  Query-parameter structure, subprotocols, and header layout are
// preserved exactly.

import (
	"strings"
	"sync"
	"time"
)

// SettingsStore is the minimal persistence interface the store needs.
// *db.Database satisfies it via GetSetting/SetSetting, so the bale package
// never imports the db package directly (no import cycle).
type SettingsStore interface {
	GetSetting(key string) (string, error)
	SetSetting(key, value string) error
}

// Setting key prefix used for all persisted constants.
const settingPrefix = "bale."

const (
	setKeyAppVersion      = "app_version"
	setKeyWebAPIKey       = "web_api_key"
	setKeySDKVersion      = "livekit_sdk_version"
	setKeyProtocolVersion = "livekit_protocol_version"
	setKeyBrowserVersion  = "browser_version"
	setKeyBaleWSURL       = "bale_ws_url"
	setKeyBaleGRPCBase    = "bale_grpc_base"
	setKeyLiveKitOrigin   = "livekit_origin"
	setKeyBaleWebOrigin   = "bale_web_origin"
	setKeyLastSyncedAt    = "last_synced_at"
)

// Default constant values — match the previously hardcoded literals so
// behaviour is identical when nothing has been persisted or extracted yet.
const (
	defAppVersion      = "154014"
	defWebAPIKey       = "C28D46DC4C3A7A26564BFCC48B929086A95C93C98E789A19847BEE8627DE4E7D"
	defSDKVersion      = "2.13.6"
	defProtocolVersion = "15"
	defBrowserVersion  = "138.0.0.0"
	defBaleWSURL       = "wss://next-ws.bale.ai/ws/"
	defBaleGRPCBase    = "https://next-ws.bale.ai"
	defLiveKitOrigin   = "https://web.ble.ir"
	defBaleWebOrigin   = "https://web.bale.ai"
)

// constants holds all Bale/LiveKit client emulation parameters in memory.
// All fields are protected by mu.
type constants struct {
	mu              sync.RWMutex
	appVersion      string
	webAPIKey       string
	sdkVersion      string
	protocolVersion string
	browserVersion  string
	baleWSURL       string
	baleGRPCBase    string
	liveKitOrigin   string
	baleWebOrigin   string
	lastSyncedAt    string
}

// store is the package-level singleton.
var store = &constants{
	appVersion:      defAppVersion,
	webAPIKey:       defWebAPIKey,
	sdkVersion:      defSDKVersion,
	protocolVersion: defProtocolVersion,
	browserVersion:  defBrowserVersion,
	baleWSURL:       defBaleWSURL,
	baleGRPCBase:    defBaleGRPCBase,
	liveKitOrigin:   defLiveKitOrigin,
	baleWebOrigin:   defBaleWebOrigin,
	lastSyncedAt:    "",
}

// ---- Getters (thread-safe) ----

// AppVersion returns the Bale app_version sent in RPC metadata/headers.
func AppVersion() string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.appVersion
}

// WebAPIKey returns the Bale web API key used for authentication.
func WebAPIKey() string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.webAPIKey
}

// SDKVersion returns the LiveKit client SDK version (query param "version").
func SDKVersion() string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.sdkVersion
}

// ProtocolVersion returns the LiveKit wire protocol version (query param
// "protocol" and subprotocol "lk-protocol-<N>").
func ProtocolVersion() string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.protocolVersion
}

// ProtocolSubprotocol returns the WebSocket subprotocol string, e.g.
// "lk-protocol-15".
func ProtocolSubprotocol() string {
	return "lk-protocol-" + ProtocolVersion()
}

// BrowserVersion returns the emulated Chrome browser version.
func BrowserVersion() string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.browserVersion
}

// BaleWSURL returns the Bale signaling WebSocket URL.
func BaleWSURL() string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.baleWSURL
}

// BaleGRPCBase returns the Bale gRPC-Web base URL.
func BaleGRPCBase() string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.baleGRPCBase
}

// LiveKitOrigin returns the Origin header used for LiveKit SFU connections.
func LiveKitOrigin() string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.liveKitOrigin
}

// BaleWebOrigin returns the Origin header used for Bale signaling/auth.
func BaleWebOrigin() string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.baleWebOrigin
}

// LastSyncedAt returns the timestamp of the last upstream sync, or "".
func LastSyncedAt() string {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.lastSyncedAt
}

// ---- Setters (thread-safe, used by the extractor) ----

// SetAppVersion updates the app_version constant.
func SetAppVersion(v string) {
	if v == "" {
		return
	}
	store.mu.Lock()
	store.appVersion = v
	store.mu.Unlock()
}

// SetWebAPIKey updates the web API key constant.
func SetWebAPIKey(v string) {
	if v == "" {
		return
	}
	store.mu.Lock()
	store.webAPIKey = v
	store.mu.Unlock()
}

// SetSDKVersion updates the LiveKit SDK version constant.
func SetSDKVersion(v string) {
	if v == "" {
		return
	}
	store.mu.Lock()
	store.sdkVersion = v
	store.mu.Unlock()
}

// SetProtocolVersion updates the LiveKit protocol version constant.
func SetProtocolVersion(v string) {
	if v == "" {
		return
	}
	store.mu.Lock()
	store.protocolVersion = v
	store.mu.Unlock()
}

// SetBrowserVersion updates the browser version constant.
func SetBrowserVersion(v string) {
	if v == "" {
		return
	}
	store.mu.Lock()
	store.browserVersion = v
	store.mu.Unlock()
}

// SetBaleWSURL updates the Bale WebSocket URL constant.
func SetBaleWSURL(v string) {
	if v == "" {
		return
	}
	store.mu.Lock()
	store.baleWSURL = v
	store.mu.Unlock()
}

// SetBaleGRPCBase updates the Bale gRPC base URL constant.
func SetBaleGRPCBase(v string) {
	if v == "" {
		return
	}
	store.mu.Lock()
	store.baleGRPCBase = v
	store.mu.Unlock()
}

// SetLiveKitOrigin updates the LiveKit Origin constant.
func SetLiveKitOrigin(v string) {
	if v == "" {
		return
	}
	store.mu.Lock()
	store.liveKitOrigin = v
	store.mu.Unlock()
}

// SetBaleWebOrigin updates the Bale web Origin constant.
func SetBaleWebOrigin(v string) {
	if v == "" {
		return
	}
	store.mu.Lock()
	store.baleWebOrigin = v
	store.mu.Unlock()
}

func setLastSyncedAt(v string) {
	store.mu.Lock()
	store.lastSyncedAt = v
	store.mu.Unlock()
}

// ConstantSnapshot is a JSON-serializable view of all constants, returned by
// the REST API and displayed in the settings panel.
type ConstantSnapshot struct {
	AppVersion      string `json:"app_version"`
	WebAPIKey       string `json:"web_api_key"`
	SDKVersion      string `json:"livekit_sdk_version"`
	ProtocolVersion string `json:"livekit_protocol_version"`
	BrowserVersion  string `json:"browser_version"`
	BaleWSURL       string `json:"bale_ws_url"`
	BaleGRPCBase    string `json:"bale_grpc_base"`
	LiveKitOrigin   string `json:"livekit_origin"`
	BaleWebOrigin   string `json:"bale_web_origin"`
	LastSyncedAt    string `json:"last_synced_at"`
}

// Snapshot returns a point-in-time copy of all constants for the API/UI.
func Snapshot() ConstantSnapshot {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return ConstantSnapshot{
		AppVersion:      store.appVersion,
		WebAPIKey:       store.webAPIKey,
		SDKVersion:      store.sdkVersion,
		ProtocolVersion: store.protocolVersion,
		BrowserVersion:  store.browserVersion,
		BaleWSURL:       store.baleWSURL,
		BaleGRPCBase:    store.baleGRPCBase,
		LiveKitOrigin:   store.liveKitOrigin,
		BaleWebOrigin:   store.baleWebOrigin,
		LastSyncedAt:    store.lastSyncedAt,
	}
}

// ApplySnapshot updates the in-memory store from a snapshot, ignoring empty
// fields (so partial updates don't wipe values).
func ApplySnapshot(s ConstantSnapshot) {
	SetAppVersion(s.AppVersion)
	SetWebAPIKey(s.WebAPIKey)
	SetSDKVersion(s.SDKVersion)
	SetProtocolVersion(s.ProtocolVersion)
	SetBrowserVersion(s.BrowserVersion)
	SetBaleWSURL(s.BaleWSURL)
	SetBaleGRPCBase(s.BaleGRPCBase)
	SetLiveKitOrigin(s.LiveKitOrigin)
	SetBaleWebOrigin(s.BaleWebOrigin)
	if s.LastSyncedAt != "" {
		setLastSyncedAt(s.LastSyncedAt)
	}
}

// LoadFromSettings reads persisted constants from the database (if available)
// and applies them to the in-memory store.  Missing keys keep their defaults.
// This is called at startup so constants survive process restarts.
func LoadFromSettings(db SettingsStore) {
	if db == nil {
		return
	}
	keys := map[string]func(string){
		setKeyAppVersion:      SetAppVersion,
		setKeyWebAPIKey:       SetWebAPIKey,
		setKeySDKVersion:      SetSDKVersion,
		setKeyProtocolVersion: SetProtocolVersion,
		setKeyBrowserVersion:  SetBrowserVersion,
		setKeyBaleWSURL:       SetBaleWSURL,
		setKeyBaleGRPCBase:    SetBaleGRPCBase,
		setKeyLiveKitOrigin:   SetLiveKitOrigin,
		setKeyBaleWebOrigin:   SetBaleWebOrigin,
		setKeyLastSyncedAt:    setLastSyncedAt,
	}
	for key, setter := range keys {
		val, err := db.GetSetting(settingPrefix + key)
		if err != nil || val == "" {
			continue
		}
		setter(val)
	}
}

// PersistToSettings writes all current constants to the database.
func PersistToSettings(db SettingsStore) error {
	if db == nil {
		return nil
	}
	s := Snapshot()
	pairs := map[string]string{
		setKeyAppVersion:      s.AppVersion,
		setKeyWebAPIKey:       s.WebAPIKey,
		setKeySDKVersion:      s.SDKVersion,
		setKeyProtocolVersion: s.ProtocolVersion,
		setKeyBrowserVersion:  s.BrowserVersion,
		setKeyBaleWSURL:       s.BaleWSURL,
		setKeyBaleGRPCBase:    s.BaleGRPCBase,
		setKeyLiveKitOrigin:   s.LiveKitOrigin,
		setKeyBaleWebOrigin:   s.BaleWebOrigin,
		setKeyLastSyncedAt:    s.LastSyncedAt,
	}
	for key, val := range pairs {
		if err := db.SetSetting(settingPrefix+key, val); err != nil {
			return err
		}
	}
	return nil
}

// LinuxUserAgent returns the Chrome User-Agent string (Linux variant) used
// for Bale signaling and gRPC-Web requests, embedding the current browser
// version so the fingerprint stays consistent with the extracted constants.
func LinuxUserAgent() string {
	return "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/" + BrowserVersion() + " Safari/537.36"
}

// WindowsUserAgent returns the Chrome User-Agent string (Windows variant) used
// for LiveKit SFU signaling connections.
func WindowsUserAgent() string {
	return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/" + BrowserVersion() + " Safari/537.36"
}

// DeviceTitle returns the device_title string sent in StartPhoneAuth,
// embedding the current browser version.
func DeviceTitle() string {
	return "Chrome_" + BrowserVersion() + ", Linux"
}

// ChromeMajorVersion returns the major version component of the browser
// version (e.g. "138" from "138.0.0.0"), used for sec-ch-ua headers.
func ChromeMajorVersion() string {
	bv := BrowserVersion()
	if idx := strings.IndexByte(bv, '.'); idx > 0 {
		return bv[:idx]
	}
	return bv
}

// MarkSynced records the current time as the last-sync timestamp in both the
// in-memory store and (optionally) the database.
func MarkSynced(db SettingsStore) {
	ts := time.Now().Format("2006-01-02 15:04:05")
	setLastSyncedAt(ts)
	if db != nil {
		_ = db.SetSetting(settingPrefix+setKeyLastSyncedAt, ts)
	}
}
