# signalbox

Real-time alerts on what your logs and requests say.

Send logs and request logs over OpenTelemetry (OTLP) or plain JSON. signalbox watches them as they arrive and notifies Slack, Discord, PagerDuty or any webhook when the 5xx ratio, p99 latency, error-log rate or a crash signature crosses a line you set. It fires seconds after the evidence lands, keeps working when storage is down, and does not page twice after a restart.

Most hosting dashboards alert on CPU, memory and disk. The failures your users actually feel usually show up first somewhere else: in a rising share of 502s, a p99 that has doubled, or a `panic:` line in the logs. signalbox is for those.

## What it does

- **Alerts on request logs:** error ratio, p50/p90/p95/p99 latency and request rate, per service or per path.
- **Alerts on log lines:** counts and rates of lines matching a severity and/or a regular expression, plus "this service has gone quiet".
- **Sustained, not twitchy:** each rule has a sliding window, a `for` duration it must hold before firing, and a `resolve_after` hysteresis before it resolves.
- **Delivery you can rely on:** an outbox retries with backoff, keeps order per receiver (a resolve never overtakes its firing), and survives restarts. PagerDuty gets a stable `dedup_key`, so a trigger and its resolve close the same incident.
- **Live tail:** `GET /tail` streams matching records as server-sent events, filtered on the server.
- **Optional storage:** batches into ClickHouse with a TTL, for search and longer history.
- **Metrics:** request rate, errors and duration per service in Prometheus format, ready for VictoriaMetrics or Prometheus to scrape, plus signalbox's own health.
- **Tenants:** one instance can serve many teams or projects. Each has its own tokens, rules, receivers, alerts, live tail and stored logs, with limits on rate, groups, rules and receivers. One tenant can't see another's data or crowd it out.
- **Query API:** request rate, error ratio and latency quantiles over time, and log search, from ClickHouse, scoped to the caller's tenant.

## Quick start

```sh
git clone https://github.com/shanurwan/signalbox && cd signalbox
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/... docker compose -f deploy/compose.yaml up -d --build
```

This starts signalbox (HTTP on `:8080`, OTLP/gRPC on `:4317`), ClickHouse and VictoriaMetrics. Check that notifications reach you:

```sh
curl -X POST -H 'Authorization: Bearer dev-api' 'localhost:8080/api/test-notify?receiver=slack'
```

Send a few request logs that fail:

```sh
for i in $(seq 1 60); do echo '{"service":"api","http.response.status_code":503,"duration_ms":40}'; done |
  curl -s -X POST -H 'Authorization: Bearer dev-ingest' --data-binary @- localhost:8080/ingest
curl -s -H 'Authorization: Bearer dev-api' localhost:8080/api/alerts
```

The example `http-5xx` rule shows as `pending`. It fires if failures continue for 2 minutes (its `for:`).

Watch everything at error level or above as it arrives:

```sh
curl -N -H 'Authorization: Bearer dev-api' 'localhost:8080/tail?level=error'
```

To run the binary on its own: `go install github.com/shanurwan/signalbox/cmd/signalbox@latest`, then `signalbox -config examples/signalbox.yaml`.

## Sending data

| Protocol | Endpoint | Notes |
|---|---|---|
| OTLP/HTTP | `POST /v1/logs` | protobuf or JSON, optionally gzip |
| OTLP/gRPC | `:4317`, `LogsService/Export` | optionally gzip |
| JSON | `POST /ingest` | one object per line, or a JSON array |

Senders authenticate with `Authorization: Bearer <ingest_token>`.

A record counts as a **request log** when it carries a numeric status code. The attribute names it looks for come from the OpenTelemetry semantic conventions plus the names common log forwarders use (`httpStatus`, `totalDuration`, `requestId` and so on). They are configurable under `mapping:`. The defaults are in [`internal/config/config.go`](internal/config/config.go).

Request logs with the same request ID are deduplicated for two minutes, because forwarders replay recent lines after a reconnect.

[docs/forwarders.md](docs/forwarders.md) shows setups for the OpenTelemetry Collector, Vector, Fluent Bit and sidecar log forwarders.

## Rules

```yaml
rules:
  - name: checkout-5xx
    match: {service: [checkout], path: "^/api/"}   # optional filters
    when: error_ratio > 0.05                       # metric, operator, threshold
    min_requests: 50                               # below this, no judgement
    window: 5m                                     # sliding window
    for: 2m                                        # must hold this long to fire
    resolve_after: 1m                              # must be false this long to resolve (default 1m)
    repeat: 4h                                     # reminder while firing; 0s turns it off (default 4h)
    group_by: [service, environment]               # one alert per group (the default)
    severity: critical
    summary: Checkout is failing.
    notify: [slack, pagerduty]
```

| Metric | Counts |
|---|---|
| `count`, `rate` | matching records in the window, or per second |
| `error_ratio` | request logs with status ≥ 500 ÷ all request logs |
| `p50_ms` … `p99_ms` | request duration quantile, at most 10% high |

Operators are `>`, `>=`, `<` and `<=`. A `<` rule (for example `count < 1`) detects a service that has gone quiet.

`match` takes `service`, `environment`, `min_severity`, `body` (a regex, for log lines) and `path` (a regex, for request logs). `group_by` takes `service`, `environment`, `path`, `method` or `attr.<key>`.

The whole config can also come from the `SIGNALBOX_CONFIG_YAML` environment variable. `${VAR}` references are expanded from the environment, and a receiver whose URL or key is empty is switched off with a warning instead of failing startup. A complete example is [examples/signalbox.yaml](examples/signalbox.yaml).

## Tenants

With no tenants created, everything belongs to the `default` tenant, using the tokens in the config file. To serve several teams or projects, the operator creates tenants with the `api_token`:

```sh
curl -X PUT https://SIGNALBOX/api/tenants/shop -H 'Authorization: Bearer API_TOKEN' \
  -d '{"limits": {"records_per_second": 5000, "max_series": 2000, "max_rules": 50}}'
# {"id": "shop", "ingest_token": "sb_in_...", "api_token": "sb_api_...", ...}   tokens are shown once
```

The tenant sends with its ingest token and manages its own receivers and rules with its API token, in the same form as the config file:

```sh
curl -X PUT https://SIGNALBOX/api/receivers/team -H 'Authorization: Bearer sb_api_...' \
  -d '{"type": "slack", "url": "https://hooks.slack.com/services/..."}'
curl -X PUT https://SIGNALBOX/api/rules/checkout-5xx -H 'Authorization: Bearer sb_api_...' --data-binary @- <<'EOF'
when: error_ratio > 0.05
min_requests: 50
match: {service: [checkout]}
window: 5m
for: 2m
notify: [team]
EOF
```

Rules in the config file are platform rules. They apply to every tenant separately, and `notify` names are looked up among each tenant's own receivers. Defaults for new tenants' limits go under `tenant_defaults:`. [docs/design.md](docs/design.md#tenants) covers the isolation guarantees and what was rejected.

## How it behaves when things fail

Each scenario below was run against a live stack: signalbox and ClickHouse at 20,000 request logs/s, with an alert rule on the 5xx ratio and a webhook receiver recording what arrived.

| What failed | What happened |
|---|---|
| signalbox killed with `kill -9` while alerts were firing | Restarted from its state file. Firing alerts were **not** sent again. Two alerts that were pending at the kill kept their progress and fired 5 s after the restart. Each resolved exactly once when errors stopped. |
| ClickHouse stopped for 30 s | Alerts kept firing. **0 rows lost**: writers retried and the queue held 600,000 rows until ClickHouse returned. |
| The notification receiver was down when alerts resolved | Retried 6 times with backoff. All 3 resolves were delivered, in order, when it came back. |
| The forwarder replayed records after reconnecting | Duplicates were dropped by request ID and counted in `signalbox_duplicate_records_total`. |

The first run of these drills found two bugs: a duplicate page after `kill -9`, and rows dropped during the ClickHouse outage. Both are fixed. [docs/design.md](docs/design.md) explains why, and what is still lost: rows still queued in memory when the process is killed, up to about one second's worth.

## Performance

All numbers are from one laptop-class machine (Intel i7-1265U, 12 threads) running signalbox, ClickHouse and the load generator **together**. The method is in [docs/benchmarks.md](docs/benchmarks.md).

| | Result |
|---|---|
| Alert engine alone, 3 rules, 20 services | 12.3 M records/s on 1 core, 33 M records/s on 12 |
| OTLP/gRPC end to end, alerting only | 359,000 records/s, 15.5 CPU-seconds per million records, 434 MiB RSS |
| OTLP/gRPC end to end, with ClickHouse storage | 150,000 records/s with 0 rows dropped |
| Export latency at 150,000 records/s | p50 33 ms, p99 100 ms (1,000 records per export) |

## Endpoints

Ingest takes an ingest token, which picks the tenant. Everything else takes an API token and covers that tenant only. The config file's `api_token` is the admin token: it sees every tenant, can act for one with `?tenant=`, and manages tenants.

| | |
|---|---|
| `POST /v1/logs`, `POST /ingest`, gRPC `:4317` | ingest |
| `GET /tail?service=&environment=&level=&q=&http=1` | live tail as server-sent events |
| `GET /api/alerts` | pending and firing alerts |
| `GET`, `PUT`, `DELETE /api/rules[/{name}]` | platform rules (read only) and the tenant's own rules |
| `GET`, `PUT`, `DELETE /api/receivers[/{name}]` | the tenant's receivers (URLs are not shown back) |
| `POST /api/test-notify?receiver=<name>` | send a test notification |
| `GET /api/requests?service=&environment=&path=&since=1h&step=1m` | request rate, 5xx ratio, p50/p90/p99 over time (needs ClickHouse) |
| `GET /api/logs?service=&environment=&level=&q=&since=15m&limit=100` | recent records, newest first (needs ClickHouse) |
| `GET /api/usage` | the tenant's limits and usage |
| `GET /api/tenants`, `PUT`, `DELETE /api/tenants/{id}` | manage tenants (admin) |
| `GET /metrics` | Prometheus metrics (admin) |
| `GET /healthz` | liveness |

## Design

[docs/design.md](docs/design.md) is the engineering requirements document. It covers where signalbox fits beside a metrics pipeline for stateless and stateful workloads, the alert state machine, tenant isolation, why alerting comes before storage, delivery guarantees, restart warm-up, late and duplicate data, and how to scale past one node.

## License

Apache-2.0
