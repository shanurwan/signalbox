// Package sink stores records in ClickHouse in large batches.
package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shanurwan/signalbox/internal/config"
	"github.com/shanurwan/signalbox/internal/model"
)

type ClickHouse struct {
	cfg    config.ClickHouse
	client *http.Client
	log    *slog.Logger
	in     chan []model.Record
	queued atomic.Int64

	Written      atomic.Uint64
	Dropped      atomic.Uint64
	FailedWrites atomic.Uint64
	LastFlushMs  atomic.Int64
}

func NewClickHouse(cfg config.ClickHouse, log *slog.Logger) *ClickHouse {
	return &ClickHouse{
		cfg:    cfg,
		client: &http.Client{Timeout: 60 * time.Second},
		log:    log,
		in:     make(chan []model.Record, 4096),
	}
}

func (c *ClickHouse) Queued() int64 { return c.queued.Load() }

// Offer queues a batch for storage. When the queue is full the batch is
// dropped and counted: alerting and live tail have already seen it, and
// they must not stall because storage is slow or down.
func (c *ClickHouse) Offer(recs []model.Record) {
	n := int64(len(recs))
	if c.queued.Add(n) > int64(c.cfg.QueueRows) {
		c.queued.Add(-n)
		c.Dropped.Add(uint64(n))
		return
	}
	select {
	case c.in <- recs:
	default:
		c.queued.Add(-n)
		c.Dropped.Add(uint64(n))
	}
}

func (c *ClickHouse) query(ctx context.Context, q string, body io.Reader, gz bool) error {
	u := strings.TrimRight(c.cfg.URL, "/") + "/?query=" + url.QueryEscape(q)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, body)
	if err != nil {
		return err
	}
	if c.cfg.User != "" {
		req.Header.Set("X-ClickHouse-User", c.cfg.User)
		req.Header.Set("X-ClickHouse-Key", c.cfg.Password)
	}
	if gz {
		req.Header.Set("Content-Encoding", "gzip")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("clickhouse %s: %s", resp.Status, strings.TrimSpace(string(msg)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (c *ClickHouse) table() string { return c.cfg.Database + "." + c.cfg.Table }

// DDL is the table signalbox writes to. Sorting by service then time makes
// "this service, last hour" reads touch a narrow range; whole days expire at
// once through partition drops.
func (c *ClickHouse) DDL() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    ts           DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    service      LowCardinality(String),
    environment  LowCardinality(String),
    severity     UInt8,
    body         String CODEC(ZSTD(3)),
    attrs        Map(LowCardinality(String), String) CODEC(ZSTD(3)),
    http_method  LowCardinality(String),
    http_path    String CODEC(ZSTD(3)),
    http_status  UInt16,
    duration_ms  Float32,
    request_id   String CODEC(ZSTD(3)),
    INDEX body_tokens body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (service, environment, ts)
TTL toDateTime(ts) + INTERVAL %d DAY DELETE
SETTINGS ttl_only_drop_parts = 1`, c.table(), c.cfg.RetentionDays)
}

// Init creates the table, retrying until ClickHouse answers or ctx ends.
func (c *ClickHouse) Init(ctx context.Context) error {
	back := time.Second
	for {
		err := c.query(ctx, c.DDL(), nil, false)
		if err == nil {
			return nil
		}
		c.log.Warn("clickhouse not ready", "err", err, "retry_in", back)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(back):
		}
		if back < 30*time.Second {
			back *= 2
		}
	}
}

// Run starts the configured number of writers, each batching from the
// shared queue, and returns when all have flushed after ctx ends.
func (c *ClickHouse) Run(ctx context.Context) {
	n := c.cfg.Writers
	if n < 1 {
		n = 1
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.writer(ctx) }()
	}
	wg.Wait()
}

func (c *ClickHouse) writer(ctx context.Context) {
	tick := time.NewTicker(c.cfg.FlushInterval)
	defer tick.Stop()
	var batch []model.Record
	flush := func(fctx context.Context) {
		if len(batch) == 0 {
			return
		}
		c.write(fctx, batch)
		c.queued.Add(-int64(len(batch)))
		batch = nil
	}
	for {
		select {
		case recs := <-c.in:
			batch = append(batch, recs...)
			if len(batch) >= c.cfg.BatchRows {
				flush(ctx)
			}
		case <-tick.C:
			flush(ctx)
		case <-ctx.Done():
		drain:
			for {
				select {
				case recs := <-c.in:
					batch = append(batch, recs...)
				default:
					break drain
				}
			}
			fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			flush(fctx)
			cancel()
			return
		}
	}
}

func (c *ClickHouse) write(ctx context.Context, batch []model.Record) {
	body, err := encode(batch)
	if err != nil {
		c.log.Error("encode batch", "err", err)
		c.Dropped.Add(uint64(len(batch)))
		return
	}
	q := "INSERT INTO " + c.table() + " SETTINGS input_format_skip_unknown_fields = 1 FORMAT JSONEachRow"
	// Retry until it lands or the process stops. While a writer waits, the
	// queue behind it fills; once full, Offer drops new rows (counted). So an
	// outage shorter than queue_rows / ingest rate loses nothing.
	back := 500 * time.Millisecond
	for attempt := 1; ; attempt++ {
		start := time.Now()
		err = c.query(ctx, q, bytes.NewReader(body), true)
		if err == nil {
			c.Written.Add(uint64(len(batch)))
			c.LastFlushMs.Store(time.Since(start).Milliseconds())
			if attempt > 1 {
				c.log.Info("clickhouse write succeeded after retries", "rows", len(batch), "attempts", attempt)
			}
			return
		}
		c.FailedWrites.Add(1)
		if ctx.Err() != nil {
			c.Dropped.Add(uint64(len(batch)))
			c.log.Error("clickhouse write abandoned at shutdown", "rows", len(batch), "err", err)
			return
		}
		if attempt == 1 || attempt%10 == 0 {
			c.log.Warn("clickhouse write failed, retrying", "rows", len(batch), "attempt", attempt, "err", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(back):
		}
		if back < 5*time.Second {
			back *= 2
		}
	}
}
