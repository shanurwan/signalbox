# Contributing to signalbox

Thanks for helping. Bug reports, fixes, docs and new receivers are all welcome.

## Before you start

- **Bugs:** open an issue with the bug form. The startup lines and the rule involved usually make it quick to reproduce.
- **Small fixes** (typos, docs, an obvious bug with a test): send a pull request straight away.
- **New features or behaviour changes:** open an issue first so we can agree on the shape before you write code. It saves you rework.
- **Security problems:** don't open a public issue. See [SECURITY.md](SECURITY.md).

Issues labelled `good first issue` are small and self-contained.

## Setting up

You need Go 1.26 or newer. Docker is only needed for the image and the optional ClickHouse sink.

```sh
git clone https://github.com/shanurwan/signalbox
cd signalbox
go test -race ./...
go run ./cmd/signalbox -config examples/signalbox.yaml
```

To check a config without starting the server:

```sh
go run ./cmd/signalbox -config examples/signalbox.yaml -check
```

`cmd/sbload` sends synthetic request logs over OTLP/gRPC and reports the throughput, for load testing a change.

## Where things live

| Path | What it does |
|---|---|
| `internal/ingest` | OTLP (HTTP and gRPC) and JSON intake, turning everything into one record type |
| `internal/engine` | windows, rule evaluation and the alert state machine |
| `internal/notify` | the outbox and receivers (Slack, Discord, PagerDuty, webhook) |
| `internal/sink` | the optional ClickHouse writer |
| `internal/source/railway` | reading a Railway environment's logs through its API |
| `internal/config` | config loading, `${VAR:-default}` expansion and validation |
| `internal/server` | HTTP API, tenants and auth |

[docs/design.md](docs/design.md) explains why it's built this way. Please read the part that touches your change.

## What a good pull request looks like

- **One change per pull request.** Two small PRs get reviewed faster than one big one.
- **A test that fails without your change.** For alerting behaviour, test the case that matters on call: late data, duplicates, a restart, a receiver that's down.
- **The same checks CI runs** pass locally:
  ```sh
  gofmt -l .        # prints nothing
  go vet ./...
  go test -race -count=1 ./...
  ```
- **Say what changes for users** in the description: a new config field, a different default, a changed alert message. If it changes behaviour, update the README or the example config in the same PR.
- **Commit messages** describe what the change does, in plain words: "Keep logs that arrive out of timestamp order", not "fix bug".

A few things we keep true, so please don't trade them away:

- Alerting never waits on storage. A slow or broken ClickHouse must not delay or drop an alert.
- Windows use the time the event happened, not the time it arrived.
- A notification that couldn't be sent is retried, not lost.
- One tenant can never see or affect another tenant's data, rules or alerts.

## Using AI tools

Fine to use. You're responsible for every line: understand it, test it, and be able to explain it in review.

## Review

I aim to reply to issues and pull requests within a few days. If something has been quiet for a week, a polite ping on the thread is welcome.

By contributing, you agree that your contribution is licensed under the [Apache License 2.0](LICENSE), the same as the project.
