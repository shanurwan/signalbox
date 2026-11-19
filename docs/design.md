# signalbox: engineering requirements

## Problem

A platform that runs many people's services, both stateless (web servers, workers) and stateful (databases, queues), can easily tell its users that a container is using 90% CPU. What users need to know is different: requests to their checkout are failing, the API got slow, or a worker printed `panic:` and is restart-looping. That evidence lives in two streams the platform already has, **application logs** and **request logs from the edge proxy**. Usually nobody watches those streams in real time, so people ship them to an external vendor to get alerts.

signalbox is the engine that watches those streams and turns them into alerts, live tails and history, for every tenant (project or team) on the platform, each isolated from the others.

## Goals

1. Alert within seconds of the evidence on: request error ratio, request latency quantiles, log-line counts and rates by severity and pattern, and the absence of logs.
2. Never page twice for one incident because of a restart. Never lose a page because a receiver was briefly down.
3. Keep alerting when storage is slow or down.
4. Accept standard input (OTLP over HTTP and gRPC) plus plain JSON, so any forwarder works.
5. Stay cheap enough that one small instance covers a typical project, with a clear way to scale out.
6. Report its own health: every drop, retry and backlog is a metric.
7. Isolate tenants. One tenant cannot read, change, slow down or crowd out another's alerts, rules, receivers, live tail or stored logs, and cannot use signalbox to reach the network it runs in.

## Non-goals

- **Infrastructure metrics** (CPU, memory, disk). Platforms already alert on these well. See "Where signalbox fits" for how the two sides meet.
- **Full-text search UI.** ClickHouse holds the data, and any SQL client or Grafana reads it.
- **Exactly-once delivery.** Delivery is at-least-once, with stable fingerprints so receivers can deduplicate (see Delivery).
- **Tracing.**

## Requirements and how they were verified

| Requirement | Target | Verified by |
|---|---|---|
| Fire on a sustained 5xx ratio | within one window plus `for` | drill A: errors stepped from 1% to 10%; alerts fired 23–28 s later (30 s window, `for: 10s`) |
| No duplicate page after a crash | 0 duplicates | drill B: `kill -9` while firing, 0 duplicates |
| A restart does not lose progress | pending `for` timers survive | drill B: 2 pending alerts fired 5 s after restart |
| A restart does not resolve early | no resolve during warm-up | unit test `TestRestartDoesNotRepageOrResolveEarly`, drill B |
| Storage outage | alerting unaffected; 0 loss within the queue's capacity | drill C: 30 s outage, 0 rows lost |
| Receiver outage | retried, delivered in order | drill D |
| Throughput per core | ≥ 50,000 records/s through OTLP/gRPC | 64,000 records/s per core (15.5 CPU-s per million) |
| Quantile accuracy | ≤ 10% high | unit test `TestP99WithinTenPercent` |
| Memory under a flood | bounded | dedupe capped; RSS 434 MiB at 359,000 records/s |
| Tenant isolation | no cross-tenant reads, alerts or deduplication | `TestTenantLifecycleAndIsolation`, `TestDedupeIsPerTenant`; live run: two tenants with the same service names, 200,000 records each, alerts and stored rows only under the sender's tenant |
| A tenant over its rate | back-pressure for it, nothing for others | live run: a tenant limited to 2,000 records/s sent 10,000/s; 78,000 records refused with `RESOURCE_EXHAUSTED`, 0 refused for the others |
| A tenant with too many groups | its series capped, others unaffected | `TestSeriesLimitPerTenant` |
| Tenant webhooks stay outside | private, loopback, link-local and metadata addresses refused at connect time | `TestTenantReceiverCannotReachPrivateAddresses`, `TestPublicAddresses` |
| Tokens at rest | only SHA-256 hashes stored | `TestTenantStateSurvivesRestartWithoutTokensOnDisk` |
| Changing a rule | open incidents close; a new rule raises no false "too few" | `TestChangingAFiringRuleResolvesIt`, `TestNewTooFewRuleWarmsFromCreation` |
| Tenants cost nothing measurable | within noise of the single-tenant build | A/B on one machine: engine 12.5 M vs 12.9 M records/s (median of 10); end to end about 400,000 records/s for both |

## Where signalbox fits

A platform's observability has three streams. signalbox owns the two that say what users feel; a metrics pipeline owns the third.

| Signal | Stateless workloads (web, workers) | Stateful workloads (databases, queues, volumes) | Owner |
|---|---|---|---|
| Request logs from the edge | error ratio and latency per service and path | rare: most traffic is TCP through a proxy | signalbox |
| Application logs | crash lines, error bursts, a worker gone quiet | the database's own words: "too many connections", "could not write to file", replication errors, out of memory | signalbox (severity and pattern rules, absence rules) |
| Resource metrics | CPU and memory against limits, restarts | the same, plus volume fill and time-to-full, replication lag, backup age | metrics pipeline (VictoriaMetrics, vmalert) |

The split follows how the failures look. A stateless service fails loudly and at once: 502s, a crash loop. Request and log rules catch that within seconds. A stateful service often fails slowly: a volume fills over days, replication falls behind. That needs trends over metrics, `predict_linear` on volume usage for example, which is what a metrics store and its rule engine are for. Some failures show in both: a database restart-looping is a crash line, then silence, in its logs, and a restart counter in its metrics.

Where they meet:

- **The tenant is the same key everywhere.** signalbox stamps it on every record from the ingest token, puts it on every metric it exports (`tenant` label), and sorts storage by it. The metrics pipeline uses the same id, for example as the VictoriaMetrics account ID.
- **One place for receivers.** Designed, not built: an intake endpoint so vmalert's notifications go through signalbox's outbox and the tenant's own receivers. A user then sets up Slack once, and metric alerts get the same ordering, retries, deduplication and restart safety as log alerts.

## Architecture

```
 forwarders ──OTLP/HTTP, OTLP/gRPC, JSON──▶ ingest ──▶ convert to Record
                                                          │
                                                          ▼
                                          tenant from the token; rate limit
                                                          │
                                                          ▼
                                              dedupe (tenant + request ID)
                                                          │
                 ┌─────────────────────┬──────────────────┼───────────────────┐
                 ▼                     ▼                  ▼                   ▼
          alert engine            live tail          RED metrics       storage queue
     (sharded sliding windows,   (fan-out, drops    (Prometheus)     (bounded, N writers)
      state machine per group)    for slow readers)                         │
                 │                                                          ▼
                 ▼                                                     ClickHouse
               outbox ──retry, in order per receiver──▶ Slack / Discord / PagerDuty / webhook
                 │
                 ▼
            state file  (alert states + outbox, written atomically)
```

Every protocol produces the same `Record`: tenant (from the token, never from the payload), time, service, environment, severity, body, attributes, and, for request logs, method, path, status, duration and request ID. All later stages see only `Record`.

## Alert engine

**Windows.** Each (rule, group) pair owns a ring of up to 60 time buckets covering the rule's window. Records go into buckets by **event time**, not arrival time, so out-of-order records count where they belong. Records older than the window, or than `max_lateness`, are counted as late and ignored. Each bucket holds a count, a request count, an error count and, for latency rules, a 170-bucket log histogram (growth 1.1, from 0 ms to about 3.8 hours), so a quantile is never more than 10% high.

**State machine,** run every `eval_interval` (default 5 s):

```
          condition true                held for `for`
inactive ───────────────▶ pending ─────────────────────────▶ firing ──every `repeat`──▶ reminder
   ▲                        │ condition false                  │
   └────────────────────────┘                                  │ condition false for `resolve_after`
   ▲                                                           ▼
   └──────────────────────────────────────────────────────── resolved
```

`min_requests` means "not enough evidence": below it, the condition counts as false. A 100% error ratio over 3 requests doesn't page anyone.

**Concurrency.** Series live in 64 shards, each with a mutex. A batch from one forwarder usually comes from one service, so the engine takes a shard lock once per *run* of records in the same group, not once per record, and updates shared counters once per batch. Measured: 12.3 M records/s on one core and 33 M on 12 (3 rules, 20 services). The worst case, where every record in a batch switches group, is 2 M records/s. The engine is never the bottleneck. Protobuf decoding is: 38% of CPU in profiles, against 1.1% for the engine.

## Decisions

### Alerting comes before storage

Every record reaches the alert engine and live tail first. Storage gets a bounded queue (`queue_rows`, default 1 M). When ClickHouse fails, writers retry with backoff until it returns, and the queue absorbs the gap. Only when the queue is full does `Offer` drop new rows, counted in `signalbox_storage_rows_total{result="dropped"}`.

*Rejected:* pushing back on senders (HTTP 503, gRPC `RESOURCE_EXHAUSTED`) when storage is slow. That makes alerting blind exactly when something is wrong, which is usually when storage is slow too.

*First version and why it changed:* writers originally gave up after 5 attempts (about 7.5 s). The ClickHouse-outage drill lost 281,000 rows from a 30 s outage even though the queue had room. Retrying until the queue fills cut that to 0.

### Delivery is at-least-once, saved immediately

Notifications go through an outbox. Each receiver gets its oldest item first, and nothing newer goes to it until that item is delivered or dropped, so a resolve never overtakes its firing. Retryable failures (network errors, 408, 429, 5xx) back off exponentially from 1 s to 60 s, for up to 24 h. Other 4xx errors are configuration mistakes: the item is dropped and logged.

Alert states and the outbox are saved to the state file (temporary file, fsync, rename) **immediately** after any transition or delivery, and every 5 s regardless.

*First version and why it changed:* state was saved only every 5 s. In the `kill -9` drill, two notifications were delivered 0.5 s after the last save, so the restarted process sent them again. Saving at once shrinks that window from up to 5 s to the length of one write. A crash between a send and the next save can still repeat one notification. That is at-least-once, and why every event carries a `fingerprint`, which PagerDuty uses as `dedup_key`.

### Restart warm-up

After a restart the windows are empty, which looks like "no errors" and "no logs". So for one window after start, signalbox will not resolve a firing alert, drop a pending one, or start a `<` (absence) alert. Without this, every restart would resolve every incident and then re-fire it, and flag every service as silent.

### Duplicates

Forwarders replay recent records after a reconnect. Request logs carry a request ID, so signalbox remembers 64-bit hashes of IDs in two generations that rotate every `dedupe_window` (default 2 m). Each generation is capped at `dedupe_max_entries` (default 2 M) and rotates early when full. Under a flood the effective window shortens instead of memory growing. Before the cap, memory reached 1.1 GiB at 270,000 records/s. After it, 434 MiB at 359,000. Plain log lines have no stable ID, so they are not deduplicated.

### Request logs and body patterns

A request log's body is its path. Body regexes apply only to log lines. The first profile showed a case-insensitive crash regex taking 11% of CPU, all of it spent matching URLs that could never match. Paths have their own `path` filter.

### Cardinality

Per-service metrics are capped at 2,000 (tenant, service, environment) combinations. Beyond that, labels collapse to `_other`, so a misconfigured forwarder can't create unbounded series.

## Tenants

A tenant is the unit of isolation: on a platform, one project or one team. With no tenants created, everything belongs to the `default` tenant and signalbox is a single-team service configured by its file, exactly as before tenants existed.

**Identity comes from the token, never from the data.** The operator creates a tenant with `PUT /api/tenants/{id}` (admin token) and gets back an ingest token and an API token, shown once. signalbox stores only their SHA-256 hashes, and looks tokens up by hash, so the lookup doesn't leak through timing how much of a guess was right. Every record is stamped with the tenant whose token delivered it; a `tenant` attribute in the payload is just an attribute. An ingest token can't call the API and an API token can't ingest. `?rotate=1` issues new tokens, and the old ones stop working at once.

**Platform rules and tenant rules.** Rules in the config file are platform rules: they apply to every tenant, evaluated separately for each. A tenant adds its own rules with `PUT /api/rules/{name}` (the same YAML as the file, or JSON), and they see only its records. A tenant rule can't take a platform rule's name. The tenant is part of every series key and of every fingerprint except the default tenant's, which stays what it was so existing PagerDuty incidents still resolve.

**Receivers are per tenant.** `PUT /api/receivers/{name}` creates one in the tenant's own namespace. A platform rule that says `notify: [slack]` sends each tenant's alerts to that tenant's receiver called `slack`, if it has one. A tenant without one still sees the alert in the API. A receiver that a rule uses can't be deleted. Receiver URLs carry secrets, so the API shows only their host.

**Tenant webhooks can't reach inside.** A tenant chooses the URL, so without a check any tenant could make signalbox send requests to the network it runs in, cloud metadata services included. Requests to a tenant's receivers check the address at connect time, after DNS resolution, so a name that resolves, or later re-resolves, to a private address is refused too. Loopback, private, link-local, carrier-grade NAT and NAT64 ranges are blocked, proxies are ignored and redirects are not followed. A refused delivery is dropped, not retried. Receivers in the config file are the operator's and are not restricted. `tenant_receivers_allow_private: true` lifts the check for development.

**Limits keep tenants from crowding each other out.**

| Limit | Over it |
|---|---|
| `records_per_second`, `burst` | the batch is refused: HTTP 429 with `Retry-After`, or gRPC `RESOURCE_EXHAUSTED` with `RetryInfo`. OTLP exporters and log forwarders retry later. |
| `max_series` | records that would start a new group are counted, not evaluated; existing groups keep working |
| `max_rules`, `max_receivers` | the API refuses the new one |

The rate limit is a balance in records, refilled at the rate and capped at the burst. A batch passes while the balance is positive and may take it below zero. So an export larger than the burst still gets through, and the debt is repaid before the next one: the average holds without forcing senders to split exports. This is the one place signalbox pushes back on senders. Slow storage never does (see "Alerting comes before storage"); a tenant exceeding what it was given does.

**Changing rules at runtime.** Rules live in a copy-on-write set that `Observe` reads without a lock. Replacing or deleting a rule marks the old one retired. At the next evaluation its series are removed, and any that were firing send a resolve that says the rule changed. Nothing stays open in PagerDuty with no rule left to resolve it. A new rule warms from its creation, not from process start. It has seen only what arrived since, so a "fewer than 50 in 5 minutes" rule would otherwise page for a healthy service one minute after it was created. The test for this failed when the fix was removed.

**Deleting a tenant** revokes its tokens at once, removes its rules, receivers and queued notifications, and drops its series at the next evaluation without notifying anyone. Its stored rows expire with the table's TTL.

**State.** Tenants (token hashes, limits, rules and receivers as sent) are saved in the state file with alert states and the outbox, and validated again on load. Receiver URLs are secrets, so the state file needs the same protection as the config file.

**Reading stored data.** `GET /api/requests` returns request rate, 5xx ratio and p50/p90/p99 over time. `GET /api/logs` returns recent records, filtered by service, environment, level and text. Both always filter by the caller's tenant. Every value goes to ClickHouse as a bound query parameter, never pasted into SQL. Queries run with `readonly=2` and a 10 s time limit, and ranges, steps and row counts are capped, so one tenant's dashboard can't take ClickHouse away from the others.

*Rejected:* one process per tenant. Isolation would be stronger, but at hundreds of thousands of small tenants, a process each wastes memory on idle ones and multiplies connections to storage. Shared processes with per-tenant limits, and routing large tenants to their own nodes (see "Scaling past one node"), cover both ends.

## Data model (ClickHouse)

```sql
CREATE TABLE signalbox_logs (
    tenant LowCardinality(String) DEFAULT 'default',
    ts DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    service LowCardinality(String), environment LowCardinality(String),
    severity UInt8, body String CODEC(ZSTD(3)),
    attrs Map(LowCardinality(String), String) CODEC(ZSTD(3)),
    http_method LowCardinality(String), http_path String CODEC(ZSTD(3)),
    http_status UInt16, duration_ms Float32 DEFAULT -1, request_id String CODEC(ZSTD(3)),
    INDEX body_tokens body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4
) ENGINE = MergeTree
PARTITION BY toDate(ts) ORDER BY (tenant, service, environment, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY DELETE SETTINGS ttl_only_drop_parts = 1
```

The sort key makes "this tenant's service, this hour" read a narrow range, and keeps one tenant's rows out of another's granules. A table created before tenants gets the column on start (`ADD COLUMN IF NOT EXISTS`), and its rows belong to `default`; that was checked against a real table from the previous release. An unknown duration is stored as -1 so latency quantiles can leave it out. Daily partitions make expiry a cheap part drop. In the drills, 13.5 M synthetic request logs took 146 MiB compressed (1.56 GiB uncompressed, 11×).

## Scaling past one node

The measured cost is 15.5 CPU-seconds per million records on the alerting path. At 1 M records/s that is about 16 cores of ingest and evaluation, so one large node, or several small ones, can do it.

Several nodes need one invariant: **all records for one alert group go to the same node**. Then windows and states never have to be merged. The plan:

1. The forwarder, or an L7 balancer, routes by consistent hash of (tenant, service): the default group key, inside its tenant. Each node owns its groups' windows, states and outbox. Adding a node moves only 1/N of the groups.
2. Each node has its own state file on its own volume. A node that moves restarts with warm-up, so it can't produce false resolves.
3. Storage scales separately: ClickHouse `Distributed` over shards keyed by `service`. Inserts stay batched from every node.
4. Rules grouped by something finer than service (`path`, `attr.*`) still work, because their groups nest inside a service.
5. Tenant definitions are small and change rarely, so every node can hold all of them; a write goes to every node. Rate limits are per node: with N nodes, each enforces 1/N of the tenant's rate.
6. A very large tenant can be routed to nodes of its own, so its load never shares a process with anyone.

Not built yet, and stated plainly: multi-node routing, a persistent storage queue, and reloading the config file without a restart (tenant rules and receivers already change at runtime; state survives a restart either way).

## Limits

- Rows still in the in-memory storage queue are lost if the process is killed: up to about one flush interval (1 s) in the `kill -9` drill. Alerts and notifications are not lost, because they are in the state file.
- Quantiles are at most 10% high.
- One node holds every series in memory: about 41 KB per (latency rule × group), less for count rules.
- Live-tail readers that fall behind lose records, and are told how many (`event: dropped`).
- Limits are per tenant, not per service inside a tenant: one noisy service can use up its own tenant's rate.
- Tenants share one process: limits bound how much of it a tenant takes, but a crash of the process affects every tenant on that node.

## Monitoring signalbox

Alert on these:

| Metric | Meaning |
|---|---|
| `rate(signalbox_storage_rows_total{result="dropped"}[5m]) > 0` | storage is behind or down long enough to lose data |
| `signalbox_outbox_pending > 0` for 10 m | a receiver is failing |
| `rate(signalbox_notifications_total{result="dropped"}[1h]) > 0` | a receiver is misconfigured |
| `rate(signalbox_late_records_total[5m])` rising | a forwarder is lagging |
| `rate(signalbox_ingested_records_total[5m]) == 0` | nothing is arriving |
| `rate(signalbox_tenant_rejected_records_total[5m]) > 0` | a tenant is over its rate: raise the limit or tell them |
| `rate(signalbox_tenant_series_limited_total[5m]) > 0` | a tenant has more groups than its limit, often a rule grouped by something unbounded |
