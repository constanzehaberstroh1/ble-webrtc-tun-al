// Package logger provides a centralized, structured logging system for the
// BLE WebRTC Tunnel. Each component gets its own log file under the logs/
// directory, while all logs are also written to stdout and a combined log.
//
// The logger is fully asynchronous — log calls never block the caller.
// A background goroutine handles file I/O, ring-buffer storage, and
// WebSocket broadcast to connected UI clients. This ensures zero impact
// on tunnel throughput and connection stability.
//
// Usage:
//
//	l := logger.New("bale")       // creates logs/<role>/bale.log
//	l.Info("connected to %s", url)
//	l.Warn("token expiring in %d hours", hours)
//	l.Error("connect failed: %v", err)
//
// The package auto-detects role from the ROLE environment variable or the
// database path. Log files are organized as:
//
//	logs/
//	├── server/
//	│   ├── combined.log     # All server logs
//	│   ├── main.log         # Server startup, shutdown
//	│   ├── bale.log         # Bale WebSocket protocol
//	│   ├── sfu.log          # LiveKit SFU / WebRTC
//	│   ├── api.log          # REST API requests
//	│   ├── router.log       # Connection routing + state
//	│   ├── accounts.log     # Account lifecycle
//	│   ├── db.log           # Database operations
//	│   ├── sync.log         # Event sync engine
//	│   ├── tunnel.log       # Data tunnel / yamux
//	│   ├── webrtc.log       # WebRTC PeerConnection
//	│   └── admin.log        # Legacy admin panel
//	└── client/
//	    ├── combined.log
//	    ├── main.log
//	    ├── bale.log
//	    ├── sfu.log
//	    └── ...
package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Level represents log severity.
type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
	FATAL
)

var levelNames = [...]string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
var levelColors = [...]string{"\033[36m", "\033[32m", "\033[33m", "\033[31m", "\033[35m"}

func (l Level) String() string {
	if l >= DEBUG && l <= FATAL {
		return levelNames[l]
	}
	return "UNKNOWN"
}

// Logger is a component-specific logger that writes to both stdout and
// a dedicated log file.
type Logger struct {
	component string
	fileLog   *log.Logger
	stdLog    *log.Logger
	combLog   *log.Logger
	file      *os.File
	minLevel  Level
}

// LogEntry represents a single structured log line for the web UI.
type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Component string `json:"component"`
	Message   string `json:"message"`
}

// Subscriber receives log entries via a buffered channel.
// Used by WebSocket handlers to stream logs to the UI in real-time.
type Subscriber struct {
	Ch        chan LogEntry
	Level     Level  // minimum level filter (0 = all)
	Component string // component filter ("" = all)
}

// Global state
var (
	mu       sync.Mutex
	role     string
	logDir   string
	combFile *os.File
	combLog  *log.Logger
	loggers  = make(map[string]*Logger)
	minLevel = INFO
	colorOut = true

	// Async pipeline: all log entries are sent here without blocking.
	// The background goroutine drains this channel.
	asyncCh chan asyncEntry

	// Ring buffer for recent log history (read by REST API).
	ringBuf   []LogEntry
	ringMu    sync.RWMutex
	ringSize  = 5000 // keep last 5000 entries in memory

	// WebSocket subscribers — background goroutine fans out to them.
	subsMu sync.RWMutex
	subs   = make(map[*Subscriber]struct{})
)

// asyncEntry is the internal message type for the async pipeline.
type asyncEntry struct {
	entry    LogEntry
	level    Level
	fileLog  *log.Logger
	fileLine string // pre-formatted line for component log file
}

// GetLogs returns up to the last n log entries from the ring buffer.
// Optionally filters by level and component.
func GetLogs(n int) []LogEntry {
	ringMu.RLock()
	defer ringMu.RUnlock()

	l := len(ringBuf)
	if l == 0 {
		return nil
	}
	if n <= 0 || n > l {
		n = l
	}

	res := make([]LogEntry, n)
	copy(res, ringBuf[l-n:])
	return res
}

// GetFilteredLogs returns recent log entries filtered by level and/or component.
func GetFilteredLogs(n int, levelFilter string, componentFilter string) []LogEntry {
	ringMu.RLock()
	defer ringMu.RUnlock()

	levelFilter = strings.ToUpper(levelFilter)
	componentFilter = strings.ToUpper(componentFilter)

	var result []LogEntry
	for i := len(ringBuf) - 1; i >= 0 && len(result) < n; i-- {
		e := ringBuf[i]
		if levelFilter != "" && e.Level != levelFilter {
			continue
		}
		if componentFilter != "" && e.Component != componentFilter {
			continue
		}
		result = append(result, e)
	}

	// Reverse to chronological order
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

// GetLogDir returns the current log directory path.
func GetLogDir() string {
	mu.Lock()
	defer mu.Unlock()
	return logDir
}

// GetRole returns the current role (server/client).
func GetRole() string {
	mu.Lock()
	defer mu.Unlock()
	return role
}

// Subscribe registers a new WebSocket subscriber for live log streaming.
// The caller must call Unsubscribe when done (e.g. on WebSocket close).
func Subscribe(level Level, component string) *Subscriber {
	s := &Subscriber{
		Ch:        make(chan LogEntry, 256),
		Level:     level,
		Component: strings.ToUpper(component),
	}
	subsMu.Lock()
	subs[s] = struct{}{}
	subsMu.Unlock()
	return s
}

// Unsubscribe removes a subscriber and closes its channel.
func Unsubscribe(s *Subscriber) {
	subsMu.Lock()
	delete(subs, s)
	subsMu.Unlock()
	// Don't close the channel — the sender (background goroutine) uses select/default.
}

// Init initializes the logging system for the given role.
// Must be called once at startup. Starts the background async writer goroutine.
// Loggers created before Init() will retroactively get their log files opened.
func Init(r string) error {
	mu.Lock()
	defer mu.Unlock()

	role = strings.ToLower(r)
	if role == "" {
		role = strings.ToLower(os.Getenv("ROLE"))
	}
	if role == "" {
		role = "app"
	}

	logDir = filepath.Join("logs", role)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("create log dir %s: %w", logDir, err)
	}

	// Open combined log file
	var err error
	combPath := filepath.Join(logDir, "combined.log")
	combFile, err = os.OpenFile(combPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open combined log: %w", err)
	}

	combLog = log.New(combFile, "", log.LstdFlags|log.Lmicroseconds)

	// Check if stdout is a terminal (for color output)
	if fi, err := os.Stdout.Stat(); err == nil {
		colorOut = (fi.Mode() & os.ModeCharDevice) != 0
	}

	// Retroactively open log files for loggers created before Init()
	for name, l := range loggers {
		if l.file == nil {
			filePath := filepath.Join(logDir, name+".log")
			f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err == nil {
				l.file = f
				l.fileLog = log.New(f, "", log.LstdFlags|log.Lmicroseconds)
			}
		}
		l.combLog = combLog
	}

	// Initialize the async pipeline
	asyncCh = make(chan asyncEntry, 8192)
	ringBuf = make([]LogEntry, 0, ringSize)

	// Start background writer goroutine
	go asyncWriter()

	return nil
}

// SetLevel sets the minimum log level globally.
func SetLevel(l Level) {
	mu.Lock()
	defer mu.Unlock()
	minLevel = l
}

// SetLevelFromString parses a level name and sets it.
func SetLevelFromString(s string) {
	switch strings.ToUpper(s) {
	case "DEBUG":
		SetLevel(DEBUG)
	case "INFO":
		SetLevel(INFO)
	case "WARN", "WARNING":
		SetLevel(WARN)
	case "ERROR":
		SetLevel(ERROR)
	case "FATAL":
		SetLevel(FATAL)
	}
}

// New creates or retrieves a logger for the named component.
// Each component gets a separate log file: logs/<role>/<component>.log
func New(component string) *Logger {
	mu.Lock()
	defer mu.Unlock()

	if l, ok := loggers[component]; ok {
		return l
	}

	l := &Logger{
		component: component,
		minLevel:  minLevel,
	}

	if logDir != "" {
		filePath := filepath.Join(logDir, component+".log")
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			l.file = f
			l.fileLog = log.New(f, "", log.LstdFlags|log.Lmicroseconds)
		}
	}

	// Stdout logger (no prefix — we format manually)
	l.stdLog = log.New(os.Stdout, "", 0)
	l.combLog = combLog

	loggers[component] = l
	return l
}

// Close drains the async pipeline and closes all open log files. Call at shutdown.
func Close() {
	// Signal the async writer to stop by closing the channel
	mu.Lock()
	ch := asyncCh
	asyncCh = nil
	mu.Unlock()

	if ch != nil {
		close(ch)
		// Give the writer a moment to drain
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	for _, l := range loggers {
		if l.file != nil {
			l.file.Close()
		}
	}
	if combFile != nil {
		combFile.Close()
	}
}

// Writer returns an io.Writer for this logger at INFO level.
// Useful for passing to http.Server.ErrorLog or similar.
func (l *Logger) Writer() io.Writer {
	return &logWriter{logger: l, level: INFO}
}

// ErrorWriter returns an io.Writer for this logger at ERROR level.
func (l *Logger) ErrorWriter() io.Writer {
	return &logWriter{logger: l, level: ERROR}
}

// StdLogger returns a standard *log.Logger that writes to this logger.
func (l *Logger) StdLogger() *log.Logger {
	return log.New(l.Writer(), "", 0)
}

// --- Log methods ---

func (l *Logger) Debug(format string, args ...interface{}) { l.log(DEBUG, format, args...) }
func (l *Logger) Info(format string, args ...interface{})  { l.log(INFO, format, args...) }
func (l *Logger) Warn(format string, args ...interface{})  { l.log(WARN, format, args...) }
func (l *Logger) Error(format string, args ...interface{}) { l.log(ERROR, format, args...) }

func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log(FATAL, format, args...)
	// For Fatal, we need to ensure the message is written before exit.
	// Give the async writer a moment to process.
	time.Sleep(50 * time.Millisecond)
	os.Exit(1)
}

func (l *Logger) log(level Level, format string, args ...interface{}) {
	if level < l.minLevel && level < minLevel {
		return
	}

	msg := fmt.Sprintf(format, args...)
	ts := time.Now().Format("2006/01/02 15:04:05.000")
	tag := strings.ToUpper(l.component)

	// Stdout is written synchronously (it's fast and expected to be immediate)
	fileLine := fmt.Sprintf("%s [%-5s] [%s] %s", ts, level.String(), tag, msg)
	if colorOut {
		color := levelColors[level]
		reset := "\033[0m"
		dim := "\033[2m"
		l.stdLog.Printf("%s%s%s %s%-5s%s %s[%s]%s %s",
			dim, ts, reset,
			color, level.String(), reset,
			"\033[1m", tag, reset,
			msg)
	} else {
		l.stdLog.Print(fileLine)
	}

	// Everything else (file I/O, ring buffer, WebSocket broadcast) is async.
	entry := asyncEntry{
		entry: LogEntry{
			Timestamp: ts,
			Level:     level.String(),
			Component: tag,
			Message:   msg,
		},
		level:    level,
		fileLog:  l.fileLog,
		fileLine: fmt.Sprintf("[%-5s] [%s] %s", level.String(), tag, msg),
	}

	// Non-blocking send to async pipeline. If the channel is full,
	// we drop the entry rather than blocking the caller — connection
	// stability and speed are the top priority.
	ch := asyncCh
	if ch != nil {
		select {
		case ch <- entry:
		default:
			// Channel full — drop this log entry to avoid blocking.
			// This should only happen under extreme load.
		}
	}
}

// asyncWriter is the background goroutine that processes all log entries.
// It handles file I/O, ring buffer management, and WebSocket fan-out.
// This runs in a single goroutine to avoid contention.
func asyncWriter() {
	for entry := range asyncCh {
		// Write to component-specific log file
		if entry.fileLog != nil {
			entry.fileLog.Output(2, entry.fileLine)
		}

		// Write to combined log
		mu.Lock()
		cl := combLog
		mu.Unlock()
		if cl != nil {
			cl.Output(2, entry.fileLine)
		}

		// Append to ring buffer
		ringMu.Lock()
		ringBuf = append(ringBuf, entry.entry)
		if len(ringBuf) > ringSize {
			// Trim 20% when full to avoid trimming on every entry
			trim := ringSize / 5
			ringBuf = append(ringBuf[:0:0], ringBuf[trim:]...)
		}
		ringMu.Unlock()

		// Fan out to WebSocket subscribers (non-blocking)
		subsMu.RLock()
		for s := range subs {
			// Apply subscriber filters
			if entry.level < s.Level {
				continue
			}
			if s.Component != "" && entry.entry.Component != s.Component {
				continue
			}
			select {
			case s.Ch <- entry.entry:
			default:
				// Subscriber is slow — skip this entry for them
			}
		}
		subsMu.RUnlock()
	}
}

// logWriter adapts the Logger to io.Writer for compatibility.
type logWriter struct {
	logger *Logger
	level  Level
}

func (w *logWriter) Write(p []byte) (n int, err error) {
	msg := strings.TrimRight(string(p), "\n")
	if msg != "" {
		w.logger.log(w.level, "%s", msg)
	}
	return len(p), nil
}
