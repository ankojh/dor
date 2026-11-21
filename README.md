# Dor

A real-time agentic news globe. Go agents on NATS JetStream ingest, place, classify,
cluster and link news stories; a SolidJS + MapLibre web app shows them on a 3D globe.

## Quickstart

```bash
make dev                 # Postgres/pgvector :5433 + TEI :8090, waits for healthy
make build               # bin/dor, bin/probe
./bin/dor run --agents=all
curl -i localhost:8091/healthz
curl -s localhost:8091/metrics | grep dor_agent
```

Publish a test message to the sample echo agent (`echo.in` → `echo.out`):

```bash
./bin/probe publish --subject echo.in --count 5 --prefix hello
./bin/probe collect --subject echo.out --expected 5
```

Split processes (one owns and provisions the bus, another runs one agent):

```bash
./bin/dor run --agents=none
./bin/dor run --agents=echo --nats-url nats://127.0.0.1:4222 --admin-addr 127.0.0.1:8092
```

For an external NATS server, make exactly one process the provisioning owner
with `--provision` / `DOR_PROVISION=true`; ordinary clients only bind and
validate the streams and KV.

Web shell (shows the data worker's `ready` line):

```bash
make web-dev             # http://127.0.0.1:5173
```

Checks:

```bash
make test-race           # Go tests with the race detector
make test-echo-restart   # kill -9 the echo agent mid-stream; no duplicates
make ci                  # exactly what .github/workflows/ci.yml runs
```

Read [docs/conventions.md](docs/conventions.md) before adding an agent, subject,
proto field or make target. Open technical choices are in
[docs/decisions.md](docs/decisions.md).
