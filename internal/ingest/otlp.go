// Package ingest turns OTLP and NDJSON payloads into model.Records.
package ingest

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"

	"github.com/shanurwan/signalbox/internal/config"
	"github.com/shanurwan/signalbox/internal/model"
)

// Converter maps source attributes to record fields.
type Converter struct {
	m config.Mapping
}

func NewConverter(m config.Mapping) *Converter { return &Converter{m: m} }

// FromOTLP flattens an OTLP logs request. Resource attributes and record
// attributes are merged, record attributes winning.
func (c *Converter) FromOTLP(req *collogs.ExportLogsServiceRequest, now time.Time) []model.Record {
	n := 0
	for _, rl := range req.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			n += len(sl.GetLogRecords())
		}
	}
	out := make([]model.Record, 0, n)
	for _, rl := range req.GetResourceLogs() {
		res := map[string]string{}
		for _, kv := range rl.GetResource().GetAttributes() {
			flatten(kv.GetKey(), kv.GetValue(), res)
		}
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				attrs := make(map[string]string, len(res)+len(lr.GetAttributes()))
				for k, v := range res {
					attrs[k] = v
				}
				for _, kv := range lr.GetAttributes() {
					flatten(kv.GetKey(), kv.GetValue(), attrs)
				}
				ts := lr.GetTimeUnixNano()
				if ts == 0 {
					ts = lr.GetObservedTimeUnixNano()
				}
				t := now
				if ts != 0 {
					t = time.Unix(0, int64(ts))
				}
				sev := model.Severity(lr.GetSeverityNumber())
				if sev == model.SevUnset {
					sev, _ = model.ParseSeverity(lr.GetSeverityText())
				}
				out = append(out, c.finish(t, sev, anyString(lr.GetBody()), attrs))
			}
		}
	}
	return out
}

// FromJSON maps one flat JSON object (one NDJSON line) to a record. Known
// keys: time/timestamp, level/severity, message/msg/body; everything else
// becomes an attribute, nested objects flattened with dots.
func (c *Converter) FromJSON(obj map[string]any, now time.Time) model.Record {
	attrs := map[string]string{}
	t := now
	var sev model.Severity
	var body string
	for k, v := range obj {
		switch k {
		case "time", "timestamp", "ts", "@timestamp":
			if pt, ok := parseTime(v); ok {
				t = pt
				continue
			}
		case "level", "severity", "lvl":
			if s, ok := v.(string); ok {
				if p, ok := model.ParseSeverity(s); ok {
					sev = p
					continue
				}
			}
		case "message", "msg", "body":
			if s, ok := v.(string); ok {
				body = s
				continue
			}
		}
		flattenJSON(k, v, attrs)
	}
	return c.finish(t, sev, body, attrs)
}

func (c *Converter) finish(t time.Time, sev model.Severity, body string, attrs map[string]string) model.Record {
	r := model.Record{
		Time:        t,
		Service:     first(attrs, c.m.Service),
		Environment: first(attrs, c.m.Environment),
		Severity:    sev,
		Body:        body,
		Attrs:       attrs,
	}
	if s := first(attrs, c.m.HTTPStatus); s != "" {
		if code, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && code >= 100 && code < 600 {
			h := &model.HTTP{
				Status:     code,
				Method:     first(attrs, c.m.HTTPMethod),
				Path:       first(attrs, c.m.HTTPPath),
				RequestID:  first(attrs, c.m.RequestID),
				DurationMs: -1,
			}
			if d := first(attrs, c.m.HTTPDurationMs); d != "" {
				if f, err := strconv.ParseFloat(d, 64); err == nil && f >= 0 {
					h.DurationMs = f
				}
			} else if d := first(attrs, c.m.HTTPDurationS); d != "" {
				if f, err := strconv.ParseFloat(d, 64); err == nil && f >= 0 {
					h.DurationMs = f * 1000
				}
			}
			if h.Path == "" && strings.HasPrefix(body, "/") {
				h.Path = body
			}
			r.HTTP = h
			if r.Severity == model.SevUnset {
				switch {
				case code >= 500:
					r.Severity = model.SevError
				case code >= 400:
					r.Severity = model.SevWarn
				default:
					r.Severity = model.SevInfo
				}
			}
		}
	}
	return r
}

func first(attrs map[string]string, keys []string) string {
	for _, k := range keys {
		if v, ok := attrs[k]; ok && v != "" {
			return v
		}
	}
	return ""
}

func anyString(v *common.AnyValue) string {
	if v == nil {
		return ""
	}
	switch x := v.Value.(type) {
	case *common.AnyValue_StringValue:
		return x.StringValue
	case *common.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *common.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	case *common.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	case *common.AnyValue_BytesValue:
		return string(x.BytesValue)
	case *common.AnyValue_KvlistValue, *common.AnyValue_ArrayValue:
		m := map[string]string{}
		flatten("", v, m)
		b, _ := json.Marshal(m)
		return string(b)
	}
	return ""
}

func flatten(prefix string, v *common.AnyValue, out map[string]string) {
	join := func(k string) string {
		if prefix == "" {
			return k
		}
		return prefix + "." + k
	}
	switch x := v.GetValue().(type) {
	case *common.AnyValue_KvlistValue:
		for _, kv := range x.KvlistValue.GetValues() {
			flatten(join(kv.GetKey()), kv.GetValue(), out)
		}
	case *common.AnyValue_ArrayValue:
		for i, e := range x.ArrayValue.GetValues() {
			flatten(join(strconv.Itoa(i)), e, out)
		}
	case nil:
	default:
		if prefix != "" {
			out[prefix] = anyString(v)
		}
	}
}

func flattenJSON(prefix string, v any, out map[string]string) {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			flattenJSON(prefix+"."+k, e, out)
		}
	case []any:
		for i, e := range x {
			flattenJSON(prefix+"."+strconv.Itoa(i), e, out)
		}
	case string:
		out[prefix] = x
	case float64:
		out[prefix] = strconv.FormatFloat(x, 'f', -1, 64)
	case json.Number:
		out[prefix] = x.String()
	case bool:
		out[prefix] = strconv.FormatBool(x)
	case nil:
	}
}

func parseTime(v any) (time.Time, bool) {
	switch x := v.(type) {
	case string:
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999"} {
			if t, err := time.Parse(layout, x); err == nil {
				return t, true
			}
		}
		if n, err := strconv.ParseFloat(x, 64); err == nil {
			return epoch(n), true
		}
	case float64:
		return epoch(x), true
	case json.Number:
		if n, err := x.Float64(); err == nil {
			return epoch(n), true
		}
	}
	return time.Time{}, false
}

// epoch accepts seconds, milliseconds, microseconds or nanoseconds.
func epoch(n float64) time.Time {
	switch {
	case n > 1e17:
		return time.Unix(0, int64(n))
	case n > 1e14:
		return time.UnixMicro(int64(n))
	case n > 1e11:
		return time.UnixMilli(int64(n))
	default:
		sec := int64(n)
		return time.Unix(sec, int64((n-float64(sec))*1e9))
	}
}
