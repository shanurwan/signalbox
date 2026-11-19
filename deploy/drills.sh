#!/usr/bin/env bash
# Failure drills: run signalbox + ClickHouse + a webhook catcher in Docker,
# drive traffic with sbload, break things, and print what the catcher received.
# Needs only Docker. Run from the repository root: bash deploy/drills.sh
set -u
cd "$(dirname "$0")/.."

ts() { date -u +%H:%M:%S; }
say() { echo "$(ts) >>> $*"; }

CFG='listen: ":8080"
grpc_listen: ":4317"
ingest_token: t
api_token: a
state_file: /data/state.json
clickhouse: {url: "http://sb-ch:8123", user: signalbox, password: signalbox}
receivers: [{name: hook, type: webhook, url: "http://sb-catch:9000"}]
rules:
  - name: http-5xx
    when: error_ratio > 0.05
    min_requests: 100
    window: 30s
    for: 10s
    resolve_after: 10s
    notify: [hook]'

CATCHER='import http.server, json, time
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        e = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        print(time.strftime("%H:%M:%S"), e.get("status"), e.get("rule"), e.get("labels"), round(e.get("value", 0), 4), flush=True)
        self.send_response(200); self.end_headers()
    def log_message(self, *a): pass
http.server.HTTPServer(("", 9000), H).serve_forever()'

DATA=$(mktemp -d)
chmod 777 "$DATA"
docker build -q -t signalbox:drill . >/dev/null
docker run --rm -v "$PWD":/src -v "$DATA":/out -w /src -e CGO_ENABLED=0 golang:1.26-alpine go build -o /out/sbload ./cmd/sbload
docker rm -f sb sb-ch sb-catch >/dev/null 2>&1
docker network create sbnet >/dev/null 2>&1
docker run -d --name sb-ch --network sbnet --ulimit nofile=262144:262144 -e CLICKHOUSE_USER=signalbox \
  -e CLICKHOUSE_PASSWORD=signalbox -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 clickhouse/clickhouse-server:25.8 >/dev/null
docker run -d --name sb-catch --network sbnet python:3.12-slim python -u -c "$CATCHER" >/dev/null
sleep 5

start_sb() {
  docker rm -f sb >/dev/null 2>&1
  docker run -d --name sb --network sbnet -p 18080:8080 -p 14317:4317 -v "$DATA":/data \
    -e SIGNALBOX_CONFIG_YAML="$CFG" signalbox:drill >/dev/null
  sleep 2
}
load() { "$DATA/sbload" -addr 127.0.0.1:14317 -token t -conns 2 -batch 500 -services 3 -rate 20000 -duration "$1" -error-pct "$2" | sed 's/^/        load: /'; }
metric() { curl -s -H "Authorization: Bearer a" localhost:18080/metrics | grep -E "$1" | grep -v '^#'; }

start_sb
say "A1 healthy traffic, 1% errors: expect nothing"
load 25s 1
say "A2 errors rise to 10%: expect FIRING per service"
load 30s 10

say "B1 kill -9 while firing, restart on the same volume, errors continue: expect no second FIRING"
docker kill -s KILL sb >/dev/null
start_sb
load 40s 10
say "B2 errors stop: expect one RESOLVED per service, after warm-up and resolve_after"
load 60s 1

say "C1 ClickHouse stopped for 30s: alerting keeps working, rows queue"
docker stop sb-ch >/dev/null
load 30s 10
metric 'signalbox_storage_rows_total|signalbox_alert_series\{state="firing"'
say "C2 ClickHouse back: queued rows written"
docker start sb-ch >/dev/null
sleep 5
load 20s 1
metric 'signalbox_storage_rows_total'

say "D1 receiver down when alerts resolve: expect retries, then delivery"
docker kill sb-catch >/dev/null
load 50s 1
metric 'signalbox_notifications_total|signalbox_outbox_pending'
docker start sb-catch >/dev/null
sleep 70
metric 'signalbox_notifications_total|signalbox_outbox_pending'

echo "===== notifications received (UTC) ====="
docker logs sb-catch 2>&1
docker rm -f sb sb-ch sb-catch >/dev/null
