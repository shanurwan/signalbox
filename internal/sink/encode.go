package sink

import (
	"bytes"
	"compress/gzip"
	"strconv"
	"unicode/utf8"

	"github.com/shanurwan/signalbox/internal/model"
)

// encode writes the batch as gzip-compressed JSONEachRow. It is written by
// hand rather than through encoding/json: reflection and sorting each row's
// attribute keys cost more CPU than the rest of the write path.
func encode(recs []model.Record) ([]byte, error) {
	var buf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	line := make([]byte, 0, 1024)
	for i := range recs {
		r := &recs[i]
		line = append(line[:0], `{"tenant":`...)
		tenant := r.Tenant
		if tenant == "" {
			tenant = model.DefaultTenant
		}
		line = appendString(line, tenant)
		line = append(line, `,"ts":"`...)
		line = r.Time.UTC().AppendFormat(line, "2006-01-02 15:04:05.000000000")
		line = append(line, `","service":`...)
		line = appendString(line, r.Service)
		line = append(line, `,"environment":`...)
		line = appendString(line, r.Environment)
		line = append(line, `,"severity":`...)
		line = strconv.AppendUint(line, uint64(r.Severity), 10)
		line = append(line, `,"body":`...)
		line = appendString(line, r.Body)
		line = append(line, `,"attrs":{`...)
		first := true
		for k, v := range r.Attrs {
			if !first {
				line = append(line, ',')
			}
			first = false
			line = appendString(line, k)
			line = append(line, ':')
			line = appendString(line, v)
		}
		line = append(line, '}')
		if h := r.HTTP; h != nil {
			line = append(line, `,"http_method":`...)
			line = appendString(line, h.Method)
			line = append(line, `,"http_path":`...)
			line = appendString(line, h.Path)
			line = append(line, `,"http_status":`...)
			line = strconv.AppendInt(line, int64(h.Status), 10)
			line = append(line, `,"request_id":`...)
			line = appendString(line, h.RequestID)
			line = append(line, `,"duration_ms":`...)
			if h.DurationMs >= 0 {
				line = strconv.AppendFloat(line, h.DurationMs, 'f', -1, 32)
			} else {
				line = append(line, `-1`...) // unknown, so quantiles can leave it out
			}
		}
		line = append(line, '}', '\n')
		if _, err := zw.Write(line); err != nil {
			return nil, err
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

const hexDigits = "0123456789abcdef"

// appendString appends s as a JSON string. Invalid UTF-8 becomes U+FFFD, as
// encoding/json does.
func appendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch c {
			case '"', '\\':
				dst = append(dst, '\\', c)
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, "�"...)
			i += size
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}
