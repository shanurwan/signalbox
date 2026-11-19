package sink

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/shanurwan/signalbox/internal/model"
)

func TestEncodeIsValidJSONEachRow(t *testing.T) {
	recs := []model.Record{
		{Time: time.Date(2026, 11, 19, 1, 2, 3, 456, time.UTC), Service: "api", Severity: model.SevError,
			Body:  "quote \" backslash \\ newline \n tab \t ctrl \x01 bad utf8 \xff emoji 🚂",
			Attrs: map[string]string{"k\"ey": "v\nal", "plain": "x"}},
		{Time: time.Unix(0, 0), Service: "web", HTTP: &model.HTTP{Method: "GET", Path: "/a?b=\"c\"", Status: 503, DurationMs: 12.5, RequestID: "r1"}},
		{Time: time.Unix(0, 0), Service: "web", HTTP: &model.HTTP{Status: 200, DurationMs: -1}},
	}
	gz, err := encode(recs)
	if err != nil {
		t.Fatal(err)
	}
	zr, _ := gzip.NewReader(bytes.NewReader(gz))
	raw, _ := io.ReadAll(zr)
	lines := bytes.Split(bytes.TrimSpace(raw), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("%d lines", len(lines))
	}
	var row map[string]any
	if err := json.Unmarshal(lines[0], &row); err != nil {
		t.Fatalf("line 1 invalid: %v\n%s", err, lines[0])
	}
	want := "quote \" backslash \\ newline \n tab \t ctrl \x01 bad utf8 � emoji 🚂"
	if row["body"] != want || row["ts"] != "2026-11-19 01:02:03.000000456" || row["severity"] != float64(17) {
		t.Fatalf("row 1: %#v", row)
	}
	if row["attrs"].(map[string]any)["k\"ey"] != "v\nal" {
		t.Fatalf("attrs: %#v", row["attrs"])
	}
	if err := json.Unmarshal(lines[1], &row); err != nil || row["http_status"] != float64(503) || row["duration_ms"] != 12.5 || row["http_path"] != "/a?b=\"c\"" {
		t.Fatalf("row 2: %v %#v", err, row)
	}
	row = nil
	if err := json.Unmarshal(lines[2], &row); err != nil {
		t.Fatal(err)
	}
	// Written as -1 rather than omitted: a table created by an older version
	// defaults the column to 0, which latency quantiles would count.
	if row["duration_ms"] != float64(-1) || row["tenant"] != "default" {
		t.Fatalf("row 3: %#v", row)
	}
}
