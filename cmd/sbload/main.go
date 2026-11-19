// Command sbload sends synthetic request logs to signalbox over OTLP/gRPC
// and reports the throughput it sustained and how long exports took.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:4317", "signalbox gRPC address")
	token := flag.String("token", "", "ingest bearer token")
	conns := flag.Int("conns", 8, "parallel senders")
	batch := flag.Int("batch", 1000, "records per export")
	services := flag.Int("services", 20, "distinct services")
	errPct := flag.Float64("error-pct", 1, "percent of requests that return 5xx")
	rate := flag.Int("rate", 0, "target records per second across all senders (0 = as fast as possible)")
	dur := flag.Duration("duration", 30*time.Second, "how long to send")
	compress := flag.Bool("gzip", false, "gzip-compress exports")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()
	if *token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+*token)
	}
	var sent, failed atomic.Uint64
	var mu sync.Mutex
	var lat []time.Duration
	var seq atomic.Uint64
	runID := strconv.FormatInt(time.Now().UnixNano(), 36) + "-"
	var firstErr atomic.Value

	var wg sync.WaitGroup
	start := time.Now()
	for c := 0; c < *conns; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			cc, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			defer cc.Close()
			client := collogs.NewLogsServiceClient(cc)
			var opts []grpc.CallOption
			if *compress {
				opts = append(opts, grpc.UseCompressor(gzip.Name))
			}
			rng := rand.New(rand.NewPCG(uint64(c), 7))
			reqs := make([]*collogs.ExportLogsServiceRequest, *services)
			for s := range reqs {
				reqs[s] = build(s, *batch, rng, *errPct)
			}
			var perConn time.Duration
			if *rate > 0 {
				perConn = time.Duration(float64(time.Second) * float64(*batch) * float64(*conns) / float64(*rate))
			}
			next := time.Now()
			local := make([]time.Duration, 0, 4096)
			for i := 0; ctx.Err() == nil; i++ {
				if perConn > 0 {
					if d := time.Until(next); d > 0 {
						time.Sleep(d)
					}
					next = next.Add(perConn)
				}
				req := reqs[i%len(reqs)]
				stamp(req, &seq, runID)
				t := time.Now()
				_, err := client.Export(ctx, req, opts...)
				if ctx.Err() != nil || status.Code(err) == codes.DeadlineExceeded {
					break // the run ended mid-export
				}
				if err != nil {
					failed.Add(uint64(*batch))
					firstErr.CompareAndSwap(nil, err.Error())
					continue
				}
				local = append(local, time.Since(t))
				sent.Add(uint64(*batch))
			}
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	el := time.Since(start).Seconds()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	q := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(p*float64(len(lat)-1))]
	}
	fmt.Printf("sent=%d failed=%d seconds=%.1f records_per_sec=%.0f exports=%d export_p50=%v export_p99=%v export_max=%v\n",
		sent.Load(), failed.Load(), el, float64(sent.Load())/el, len(lat), q(0.5), q(0.99), q(1))
	if e := firstErr.Load(); e != nil {
		fmt.Println("first error:", e)
	}
}

func str(k, v string) *common.KeyValue {
	return &common.KeyValue{Key: k, Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: v}}}
}

func build(svc, n int, rng *rand.Rand, errPct float64) *collogs.ExportLogsServiceRequest {
	paths := []string{"/", "/api/orders", "/api/cart", "/api/login", "/healthz"}
	methods := []string{"GET", "GET", "GET", "POST", "PUT"}
	recs := make([]*logs.LogRecord, n)
	for i := range recs {
		code := 200
		if rng.Float64()*100 < errPct {
			code = 500 + rng.IntN(4)
		} else if rng.Float64() < 0.05 {
			code = 404
		}
		ms := rng.ExpFloat64() * 40
		recs[i] = &logs.LogRecord{
			SeverityNumber: logs.SeverityNumber_SEVERITY_NUMBER_INFO,
			Body:           &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: paths[i%len(paths)]}},
			Attributes: []*common.KeyValue{
				{Key: "http.response.status_code", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64(code)}}},
				str("method", methods[i%len(methods)]),
				str("path", paths[i%len(paths)]),
				str("totalDuration", strconv.FormatFloat(ms, 'f', 0, 64)),
				str("requestId", ""),
			},
		}
	}
	return &collogs.ExportLogsServiceRequest{ResourceLogs: []*logs.ResourceLogs{{
		Resource: &resource.Resource{Attributes: []*common.KeyValue{
			str("service.name", "svc-"+strconv.Itoa(svc)),
			str("deployment.environment.name", "production"),
		}},
		ScopeLogs: []*logs.ScopeLogs{{LogRecords: recs}},
	}}}
}

// stamp gives every record the current time and a unique request ID, so
// the server's window and dedupe logic do real work.
func stamp(req *collogs.ExportLogsServiceRequest, seq *atomic.Uint64, runID string) {
	now := uint64(time.Now().UnixNano())
	recs := req.ResourceLogs[0].ScopeLogs[0].LogRecords
	base := seq.Add(uint64(len(recs))) - uint64(len(recs))
	for i, r := range recs {
		r.TimeUnixNano = now
		r.Attributes[4].Value.Value = &common.AnyValue_StringValue{StringValue: runID + strconv.FormatUint(base+uint64(i), 36)}
	}
}
