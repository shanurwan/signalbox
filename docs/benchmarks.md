# Benchmarks

Measured on 19 November 2026 on one machine: Intel i7-1265U (10 cores, 12 threads), 30 GiB RAM, Docker 29.8. **signalbox, ClickHouse 25.8 and the load generator shared the machine** and its CPUs, with no limits. A dedicated host would do better. Read the absolute numbers as a floor, and the CPU cost per record as the useful figure.

## Alert engine alone

```sh
go test -run x -bench Observe -benchtime 3s -cpu 1,4,12 ./internal/engine/
```

Three rules (error-log count, 5xx ratio, p99 latency), 20 services, 1,000-record batches.

| Batch shape | 1 core | 4 cores | 12 cores |
|---|---|---|---|
| one service per batch (as OTLP exports group by resource) | 12.3 M records/s | 24.4 M | 33.2 M |
| every record a different group (worst case) | 1.6 M | 2.1 M | 2.0 M |

### With tenants

Tenants added a tenant lookup per batch and the tenant in every series key. On 20 November the previous build and the tenant build ran back to back on the same machine, ten runs each. `ObserveTenant` is the path the pipeline takes, since a batch always comes from one token.

| Batch shape | before tenants (median) | with tenants (median) |
|---|---|---|
| one service per batch, 1 core | 12.9 M records/s | 12.5 M |
| one service per batch, 12 cores | 32.6 M | 31.7 M |
| every record a different group, 1 core | 1.5 M | 1.6 M |
| every record a different group, 12 cores | 1.8 M | 1.8 M |

The first tenant build was about 8% slower on one core. A profile showed 10% of CPU in a loop that split each batch into runs of the same tenant, touching every record once more to find what the pipeline already knew. `ObserveTenant` skips it.

End to end, alerting only, with four rules (16 connections, 1,000 records per export, 20 s): the previous build did 400,050 and 391,482 records/s, and the tenant build 402,676 and 370,619. The runs vary as much as the builds.

## End to end over OTLP/gRPC

`cmd/sbload` sends synthetic request logs: 20 services, about 5% 404s, 1% 5xx, exponential latency with a 40 ms mean, and a unique request ID per record (so dedupe does real work). Each export carries 1,000 records.

```sh
go build -o sbload ./cmd/sbload
./sbload -addr 127.0.0.1:4317 -token TOKEN -conns 16 -batch 1000 -duration 25s
./sbload -addr 127.0.0.1:4317 -token TOKEN -conns 8 -batch 1000 -duration 25s -rate 150000
```

| Run | Records/s | Export p50 / p99 | signalbox CPU per million records | RSS | Rows dropped |
|---|---|---|---|---|---|
| alerting only, as fast as possible | 359,380 | 40 / 104 ms | 15.5 s | 434 MiB | n/a |
| with ClickHouse, 100,000/s | 99,614 | 21 / 65 ms | 19.0 s | 488 MiB | 0 |
| with ClickHouse, 150,000/s | 149,576 | 33 / 100 ms | 22.7 s | 675 MiB | 0 |

Without a rate limit and with storage on, the shared machine took about 190,000 records/s, and storage dropped about a quarter of them because ClickHouse on the same CPUs could not keep up. That is the designed behaviour (alerting first), and it showed up in `signalbox_storage_rows_total{result="dropped"}`.

## Where the CPU goes

A 12 s CPU profile at about 190,000 records/s with storage on (cumulative shares, so they overlap; taken before the dedupe change below):

| Share | |
|---|---|
| 37.8% | protobuf decoding of OTLP, including UTF-8 validation |
| 19.0% | allocation (`mallocgc`), plus 15.9% in background GC marking |
| 14.8% | encoding rows for ClickHouse: JSON plus gzip |
| 11.1% | converting OTLP to records (attribute flattening) |
| 6.2% | request-ID dedupe |
| 4.2% | per-service request metrics |
| 1.1% | the alert engine |

Changes the profiles led to, each measured:

- Body regexes no longer run on request logs. They were 11% of CPU spent matching URLs.
- A hand-written JSON row encoder replaced reflection and per-row map-key sorting. Together with the regex change, throughput with storage went from 167,000 to 191,000 records/s.
- Request-ID dedupe stores 64-bit hashes and caps each generation. Memory went from 1.1 GiB to 434 MiB at a higher rate.
- The engine takes its lock once per run of same-group records and updates shared counters once per batch. On the same worst-case batch shape, 12-core throughput went from 0.79 M to 2.0 M records/s; before the change, adding cores made it slower.

## Failure drills

The script is `deploy/drills.sh`, and the results are in the README. Each drill ran at 20,000 records/s with ClickHouse storage and a webhook receiver.
