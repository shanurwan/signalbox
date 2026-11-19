# Sending logs to signalbox

signalbox accepts OTLP over HTTP (`/v1/logs`) and gRPC (`:4317`), and newline-delimited JSON (`/ingest`). Any of these forwarders works. In every example, replace `SIGNALBOX` with your signalbox address and `TOKEN` with its `ingest_token`.

For request logs, signalbox needs a numeric status code. Duration and request ID are optional but recommended. These attributes are recognised by default:

| Field | Attribute names (first one present wins) |
|---|---|
| service | `service.name`, `service`, `_metadata.service_name` |
| environment | `deployment.environment.name`, `deployment.environment`, `environment`, `_metadata.environment_name` |
| status | `http.response.status_code`, `http.status_code`, `httpStatus`, `status` |
| duration (ms) | `totalDuration`, `duration_ms`, `latency_ms` |
| duration (s) | `http.server.request.duration` |
| method | `http.request.method`, `http.method`, `method` |
| path | `url.path`, `http.target`, `path` |
| request ID | `http.request.id`, `requestId`, `request_id` |

Override them under `mapping:` in the config.

## OpenTelemetry Collector

```yaml
exporters:
  otlphttp/signalbox:
    endpoint: https://SIGNALBOX
    headers: {Authorization: "Bearer TOKEN"}
service:
  pipelines:
    logs:
      receivers: [otlp, filelog]
      exporters: [otlphttp/signalbox]
```

## Vector

```toml
[sinks.signalbox]
type = "http"
inputs = ["app_logs"]
uri = "https://SIGNALBOX/ingest"
encoding.codec = "json"
framing.method = "newline_delimited"
request.headers.Authorization = "Bearer TOKEN"
```

## Fluent Bit

```ini
[OUTPUT]
    Name    http
    Match   *
    Host    SIGNALBOX
    Port    443
    tls     On
    URI     /ingest
    Format  json_lines
    json_date_key timestamp
    json_date_format iso8601
    Header  Authorization Bearer TOKEN
```

## Locomotive

[Locomotive](https://github.com/brody192/locomotive) is a sidecar that streams a Railway environment's deployment logs and HTTP logs to a webhook. Its `otel_http` mode sends OTLP/JSON that signalbox reads as-is: service and environment names come from its resource attributes, request logs from `http.response.status_code`, `totalDuration` and `requestId`.

```
LOCOMOTIVE_WEBHOOK_MODE=otel_http
LOCOMOTIVE_WEBHOOK_URL=https://SIGNALBOX/v1/logs
LOCOMOTIVE_ADDITIONAL_HEADERS=Authorization=Bearer TOKEN
LOCOMOTIVE_ENABLE_HTTP_LOGS=true
```

Tested by sending payloads built with Locomotive's own `otel_http` encoder to a running signalbox. 40 HTTP logs (10 of them 502) and 3 `panic:` lines produced the expected alerts: error ratio 0.25, p99 within 10% of the true maximum, and the crash rule. Request IDs make its reconnect replays harmless: signalbox drops the duplicates.

signalbox alerts on what logs and requests say. It does not replace a platform's own monitors for CPU, memory or disk. Keep using those for resource limits.

## Your own code

Any OpenTelemetry SDK with a logs exporter pointed at `https://SIGNALBOX/v1/logs`. Or post JSON directly:

```sh
curl -X POST https://SIGNALBOX/ingest -H 'Authorization: Bearer TOKEN' --data-binary @- <<'EOF'
{"service":"api","level":"error","msg":"payment provider timeout","order":"A-1042"}
{"service":"api","http.response.status_code":504,"duration_ms":30012,"path":"/api/pay","requestId":"r-77"}
EOF
```
