// Package model holds the one record type every part of signalbox shares.
package model

import "time"

// Severity follows the OpenTelemetry severity numbers (1 trace .. 21 fatal).
type Severity uint8

const (
	SevUnset Severity = 0
	SevTrace Severity = 1
	SevDebug Severity = 5
	SevInfo  Severity = 9
	SevWarn  Severity = 13
	SevError Severity = 17
	SevFatal Severity = 21
)

// ParseSeverity maps common severity words to OpenTelemetry numbers.
func ParseSeverity(s string) (Severity, bool) {
	switch s {
	case "trace", "TRACE":
		return SevTrace, true
	case "debug", "DEBUG":
		return SevDebug, true
	case "info", "INFO", "information", "notice":
		return SevInfo, true
	case "warn", "WARN", "warning", "WARNING":
		return SevWarn, true
	case "error", "ERROR", "err", "ERR":
		return SevError, true
	case "fatal", "FATAL", "critical", "CRITICAL", "panic", "PANIC":
		return SevFatal, true
	}
	return SevUnset, false
}

// String returns the lower-case severity word for the band s falls in.
func (s Severity) String() string {
	switch {
	case s == SevUnset:
		return "unset"
	case s < SevDebug:
		return "trace"
	case s < SevInfo:
		return "debug"
	case s < SevWarn:
		return "info"
	case s < SevError:
		return "warn"
	case s < SevFatal:
		return "error"
	default:
		return "fatal"
	}
}

// HTTP is set when a record describes one served request.
type HTTP struct {
	Method     string
	Path       string
	Status     int
	DurationMs float64 // negative when the source did not report a duration
	RequestID  string
}

// Record is one log line, or one request log, after ingestion.
type Record struct {
	Time        time.Time
	Service     string
	Environment string
	Severity    Severity
	Body        string
	Attrs       map[string]string
	HTTP        *HTTP
}
