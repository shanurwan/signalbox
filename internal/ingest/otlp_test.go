package ingest

import (
	"testing"
	"time"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/shanurwan/signalbox/internal/config"
	"github.com/shanurwan/signalbox/internal/model"
)

func conv(t *testing.T) *Converter {
	c, err := config.Parse([]byte("rules: []\n"))
	if err != nil {
		t.Fatal(err)
	}
	return NewConverter(c.Mapping)
}

// The shape a common log forwarder sends in OTLP/JSON mode for one request
// log: status as an int attribute (a string, per OTLP/JSON), the rest of the
// request flattened into string attributes, the path as the body.
const requestLogJSON = `{"resourceLogs":[{"resource":{"attributes":[
  {"key":"service.name","value":{"stringValue":"api"}},
  {"key":"deployment.environment.name","value":{"stringValue":"production"}},
  {"key":"service.namespace","value":{"stringValue":"shop"}}]},
 "scopeLogs":[{"scope":{"name":"forwarder"},"logRecords":[
  {"timeUnixNano":"1763553600000000000","observedTimeUnixNano":"1763553600100000000",
   "severityNumber":17,"severityText":"ERROR","body":{"stringValue":"/v1/checkout"},
   "attributes":[
    {"key":"http.response.status_code","value":{"intValue":"502"}},
    {"key":"method","value":{"stringValue":"POST"}},
    {"key":"path","value":{"stringValue":"/v1/checkout"}},
    {"key":"httpStatus","value":{"stringValue":"502"}},
    {"key":"totalDuration","value":{"stringValue":"1834"}},
    {"key":"requestId","value":{"stringValue":"abc-123"}}]},
  {"timeUnixNano":"1763553601000000000","severityNumber":9,"severityText":"info",
   "body":{"stringValue":"listening on :3000"}}]}]}]}`

func TestRequestLogFromOTLPJSON(t *testing.T) {
	var req collogs.ExportLogsServiceRequest
	if err := protojson.Unmarshal([]byte(requestLogJSON), &req); err != nil {
		t.Fatal(err)
	}
	recs := conv(t).FromOTLP(&req, time.Now())
	if len(recs) != 2 {
		t.Fatalf("got %d records", len(recs))
	}
	r := recs[0]
	if r.Service != "api" || r.Environment != "production" || r.Severity != model.SevError {
		t.Fatalf("record: %+v", r)
	}
	if r.HTTP == nil || r.HTTP.Status != 502 || r.HTTP.Method != "POST" || r.HTTP.Path != "/v1/checkout" ||
		r.HTTP.DurationMs != 1834 || r.HTTP.RequestID != "abc-123" {
		t.Fatalf("http: %+v", r.HTTP)
	}
	if !r.Time.Equal(time.Unix(0, 1763553600000000000)) {
		t.Fatalf("time %v", r.Time)
	}
	if recs[1].HTTP != nil || recs[1].Body != "listening on :3000" || recs[1].Service != "api" {
		t.Fatalf("plain log: %+v", recs[1])
	}
}

func TestOTelSemconvDurationInSeconds(t *testing.T) {
	r := conv(t).FromJSON(map[string]any{
		"service.name": "web", "http.response.status_code": float64(200), "http.server.request.duration": 0.25,
	}, time.Now())
	if r.HTTP == nil || r.HTTP.DurationMs != 250 || r.Severity != model.SevInfo {
		t.Fatalf("%+v %+v", r, r.HTTP)
	}
}

func TestNDJSONObject(t *testing.T) {
	r := conv(t).FromJSON(map[string]any{
		"timestamp": "2026-11-19T10:00:00.5Z", "level": "warning", "msg": "slow query",
		"service": "worker", "ctx": map[string]any{"table": "orders", "ms": float64(812)},
	}, time.Now())
	if r.Service != "worker" || r.Severity != model.SevWarn || r.Body != "slow query" || r.HTTP != nil {
		t.Fatalf("%+v", r)
	}
	if r.Attrs["ctx.table"] != "orders" || r.Attrs["ctx.ms"] != "812" {
		t.Fatalf("attrs %v", r.Attrs)
	}
	if want := time.Date(2026, 11, 19, 10, 0, 0, 5e8, time.UTC); !r.Time.Equal(want) {
		t.Fatalf("time %v", r.Time)
	}
}

func TestEpochUnits(t *testing.T) {
	want := time.Unix(1763553600, 0)
	for _, v := range []float64{1763553600, 1763553600000, 1763553600000000, 1763553600000000000} {
		if got := epoch(v); !got.Equal(want) {
			t.Errorf("epoch(%g) = %v", v, got)
		}
	}
}

func TestBadStatusIsNotARequest(t *testing.T) {
	r := conv(t).FromJSON(map[string]any{"status": "ok", "msg": "health"}, time.Now())
	if r.HTTP != nil {
		t.Fatal("a non-numeric status must not make a request log")
	}
}
