// Package logger provides a centralized, structured logging system for the
// BLE WebRTC Tunnel. Each component gets its own log file under the logs/
// directory, while all logs are also written to stdout and a combined log.
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

	// Memory buffer for UI streaming
	logBuffer []LogEntry
	logBufferMu sync.RWMutex
	maxLogBuffer = 1000
)

// GetLogs returns up to the last n log entries.
func GetLogs(n int) []LogEntry {
	logBufferMu.RLock()
	defer logBufferMu.RUnlock()

	l := len(logBuffer)
	if l == 0 {
		return nil
	}
	if n <= 0 || n > l {
		n = l
	}

	res := make([]LogEntry, n)
	copy(res, logBuffer[l-n:])
	return res
}

// Init initializes the logging system for the given role.
// Must be called once at startup. Loggers created before Init() will
// retroactively get their log files opened.
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

// Close closes all open log files. Call at shutdown.
func Close() {
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
	os.Exit(1)
}

func (l *Logger) log(level Level, format string, args ...interface{}) {
	if level < l.minLevel && level < minLevel {
		return
	}

	msg := fmt.Sprintf(format, args...)
	ts := time.Now().Format("2006/01/02 15:04:05.000")
	tag := strings.ToUpper(l.component)

	// File format: timestamp [LEVEL] [COMPONENT] message
	fileLine := fmt.Sprintf("%s [%-5s] [%s] %s", ts, level.String(), tag, msg)

	// Write to component-specific log file
	if l.fileLog != nil {
		l.fileLog.Output(2, fmt.Sprintf("[%-5s] [%s] %s", level.String(), tag, msg))
	}

	// Write to combined log
	if l.combLog != nil {
		l.combLog.Output(2, fmt.Sprintf("[%-5s] [%s] %s", level.String(), tag, msg))
	}

	// Write to stdout (with optional color)
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

	// Add to memory buffer for UI
	logBufferMu.Lock()
	logBuffer = append(logBuffer, LogEntry{
		Timestamp: ts,
		Level:     level.String(),
		Component: tag,
		Message:   msg,
	})
	if len(logBuffer) > maxLogBuffer {
		logBuffer = logBuffer[1:]
	}
	logBufferMu.Unlock()
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
