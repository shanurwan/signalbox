package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Query limits: one tenant's dashboard must not be able to occupy ClickHouse.
const (
	MaxRange   = 31 * 24 * time.Hour
	MaxPoints  = 1500
	MaxLogRows = 1000
)

// selectJSON runs a read-only query with bound parameters ({name:Type} in
// the SQL, values passed as param_name) and returns its "data" rows.
// Values never become part of the SQL text, so they cannot change it.
func (c *ClickHouse) selectJSON(ctx context.Context, sql string, params map[string]string) ([]map[string]any, error) {
	// Settings and parameters go in the URL; ClickHouse reads the POST body
	// as the query itself.
	v := url.Values{}
	v.Set("readonly", "2") // reads only; settings below may still be set
	v.Set("max_execution_time", "10")
	v.Set("max_result_rows", strconv.Itoa(MaxPoints+MaxLogRows))
	v.Set("output_format_json_quote_64bit_integers", "0")
	for k, p := range params {
		v.Set("param_"+k, p)
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.cfg.URL, "/")+"/?"+v.Encode(),
		strings.NewReader(sql+" FORMAT JSON"))
	if err != nil {
		return nil, err
	}
	if c.cfg.User != "" {
		req.Header.Set("X-ClickHouse-User", c.cfg.User)
		req.Header.Set("X-ClickHouse-Key", c.cfg.Password)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("clickhouse %s: %s", resp.Status, strings.TrimSpace(string(msg)))
	}
	var out struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.Data == nil {
		out.Data = []map[string]any{}
	}
	return out.Data, nil
}

// RequestQuery asks for request rate, errors and latency of one tenant over time.
type RequestQuery struct {
	Tenant, Service, Environment, Path string
	Since, Step                        time.Duration
}

// Requests returns one row per step: requests, 5xx errors, error ratio and
// p50/p90/p99 latency, for request logs that reported a duration.
func (c *ClickHouse) Requests(ctx context.Context, q RequestQuery) ([]map[string]any, error) {
	sql := `SELECT
    toStartOfInterval(ts, toIntervalSecond({step:UInt32})) AS time,
    count() AS requests,
    countIf(http_status >= 500) AS errors,
    round(errors / requests, 6) AS error_ratio,
    arrayMap(x -> round(x, 1), quantilesIf(0.5, 0.9, 0.99)(duration_ms, duration_ms >= 0)) AS q,
    q[1] AS p50_ms, q[2] AS p90_ms, q[3] AS p99_ms
FROM ` + c.table() + `
WHERE tenant = {tenant:String}
  AND ts >= now64(9) - toIntervalSecond({since:UInt32})
  AND http_status > 0
  AND ({service:String} = '' OR service = {service:String})
  AND ({environment:String} = '' OR environment = {environment:String})
  AND ({path:String} = '' OR http_path = {path:String})
GROUP BY time
ORDER BY time`
	rows, err := c.selectJSON(ctx, sql, map[string]string{
		"tenant": q.Tenant, "service": q.Service, "environment": q.Environment, "path": q.Path,
		"since": strconv.Itoa(int(q.Since.Seconds())), "step": strconv.Itoa(int(q.Step.Seconds())),
	})
	for _, r := range rows {
		delete(r, "q")
	}
	return rows, err
}

// LogQuery searches one tenant's stored records, newest first.
type LogQuery struct {
	Tenant, Service, Environment, Text string
	MinSeverity                        uint8
	Since                              time.Duration
	Limit                              int
}

func (c *ClickHouse) Logs(ctx context.Context, q LogQuery) ([]map[string]any, error) {
	sql := `SELECT ts AS time, service, environment, severity, body, attrs,
    http_method, http_path, http_status, duration_ms, request_id
FROM ` + c.table() + `
WHERE tenant = {tenant:String}
  AND ts >= now64(9) - toIntervalSecond({since:UInt32})
  AND ({service:String} = '' OR service = {service:String})
  AND ({environment:String} = '' OR environment = {environment:String})
  AND severity >= {severity:UInt8}
  AND ({text:String} = '' OR positionCaseInsensitive(body, {text:String}) > 0)
ORDER BY ts DESC
LIMIT {limit:UInt32}`
	return c.selectJSON(ctx, sql, map[string]string{
		"tenant": q.Tenant, "service": q.Service, "environment": q.Environment, "text": q.Text,
		"severity": strconv.Itoa(int(q.MinSeverity)), "since": strconv.Itoa(int(q.Since.Seconds())),
		"limit": strconv.Itoa(q.Limit),
	})
}
