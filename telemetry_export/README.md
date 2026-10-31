# Telemetry Export Service (MVP)

External data-plane service for telemetry export.

This service queries InfluxDB telemetry and provides a guarded, authenticated API
for `signals` exports. It intentionally does not add export routes to
`anolis-runtime`.

## Endpoints

1. `GET /v1/health`
2. `POST /v1/exports/signals:query`
3. `POST /v1/exports/runs:export`
4. `GET /v1/exports/manifests/{export_id}`

## Auth

Use bearer token auth on every export request:

- Header: `Authorization: Bearer <token>`

This applies to both query and manifest routes.

Token comes from service config (`server.auth_token`).
Token precedence:

1. `ANOLIS_EXPORT_AUTH_TOKEN` (env, if set)
2. `server.auth_token` (config)

Influx token precedence:

1. `ANOLIS_EXPORT_INFLUX_TOKEN` (env, if set)
2. `influxdb.token` (config)

## Request Tracing

Optional headers for traceability:

1. `X-Request-Id`
2. `X-Requester-Id`

If missing, the service generates `X-Request-Id` and defaults requester to
`anonymous`. Successful responses include `X-Request-Id`.

`POST /v1/exports/signals:query` and `POST /v1/exports/runs:export` responses
also include:

1. `X-Export-Id`
2. `X-Export-Manifest-Hash` (`sha256:...`)

The full manifest is always available in JSON bodies and via
`GET /v1/exports/manifests/{export_id}`.

## Optional Scope Enforcement

Config section `authorization` can enforce selector allowlists.

- `authorization.enforce_selector_scope: true`
- Non-empty `allowed_runtime_names` / `allowed_provider_ids` /
  `allowed_device_ids` / `allowed_signal_ids` become hard allowlists.
- Violations return `403 permission_denied`.

## Runtime Disambiguation (v1)

Telemetry points include `runtime_name` tag, and exports support:

- `selector.runtime_names` (optional list filter)
- `runtime_name` output column (included by default)

If `selector.runtime_names` is omitted, results include all matching runtimes in
the bucket.

## Downsample Aggregation Matrix (v1)

For `resolution.mode=downsampled`:

1. Numeric fields (`value_double`, `value_int`, `value_uint`) use requested aggregation (`last|mean|min|max|count`).
2. Non-numeric fields (`value_bool`, `value_string`, `quality`) always use `last`.
3. Requests that combine non-numeric typed output columns (`value_bool`, `value_string`) with non-`last` aggregation are rejected with `400 invalid_argument`.

## Timezone Behavior

`timezone` request input is not supported in v1.
All timestamps are exported in UTC (`RFC3339 Z`).
Supplying `timezone` to `POST /v1/exports/signals:query` or
`POST /v1/exports/runs:export` returns `400 invalid_argument`.

## Response Formats

`format` supports:

1. `json`
2. `csv`
3. `ndjson`

Both routes stream the Influx response and spool the export to a file before
sending it, so a full export is not assembled in RAM. `max_rows` is enforced
while rows are read. `runs:export` holds its seed rows (at most one per scoped
series) in memory and spools the window's rows to disk until it writes them.

Byte limits, on both routes:

1. `limits.max_response_bytes` applies to `format=json`.
2. `limits.max_stream_bytes` applies to `csv` and `ndjson`. On `runs:export`
   it counts the whole JSON envelope that carries the `csv_body` or
   `ndjson_body`.

## Run-based export (`POST /v1/exports/runs:export`)

Export the telemetry of an anolis **run** (the runtime's experiment primitive,
epic anolishq/anolis#111) as a self-contained unit, plus Grafana annotations.

The request carries a **portable `RunManifest`** — the export service never calls
the runtime, so an export stays reproducible after the originating runtime is
offline. Materialize a manifest once (while the runtime is reachable) with
`scripts/fetch-run-manifest.py`, then export it any number of times. The
script scopes the manifest to the runtime named by the `run_id`'s prefix
(`<runtime.name>-<ULID>`). For a `run_id` without that shape, pass
`--runtime-name`, or the script refuses rather than write an unscoped manifest.

```jsonc
{
  "run": {
    "schema_version": 1,
    "run_id": "bioreactor-telemetry-01J...",
    "started_at_epoch_ms": 1711929600000,
    "ended_at_epoch_ms": 1711933200000,   // null => open run, exports up to "now"
    "polling_interval_ms": 2000,
    "runtime_names": ["bioreactor-telemetry"],
    "tag_scope": { "provider_ids": ["bread0"], "device_ids": ["rlht0"], "signal_ids": [] },
    "markers": [ { "sequence": 4, "category": "annotation", "type": "sample",
                   "occurred_at_epoch_ms": 1711931400000, "payload": { "volume_ml": 5 } } ]
  },
  "resolution": { "mode": "raw_event" },   // optional; same shapes as signals:query
  "format": "json",                          // optional: json | csv | ndjson
  "seed_stable_signals": true                // optional, default true
}
```

`run`, `resolution`, `format`, `columns` and `seed_stable_signals` are the
only top-level keys. Any other key, such as `time_range`, `selector` or
`timezone`, returns `400 invalid_argument` before anything is queried: the
window and scope come from the run.

The response is a JSON envelope with the windowed rows (`data` for json,
`ndjson_body` for ndjson, `csv_body` for csv), an augmented `manifest`
(adds `dataset: "run"`, a `run` provenance block, a `seed` block, and
`annotations`), and a top-level `annotations` array.

Semantics (anolishq/anolis-telemetry-export#31):

* **Window** is half-open `[started_at, ended_at)` — Flux `range`'s
  start-inclusive / stop-exclusive bounds. The boundary is fuzzy by
  ~`polling_interval_ms` + provider latency + scheduler jitter (telemetry is
  timestamped when the runtime *observes* a change and emitted only on change);
  `polling_interval_ms` is carried in `manifest.run` so a consumer can reason
  about it. `run_id` is **never** a telemetry tag — correlation is purely this
  time-window join over the frozen 4-tag schema.
* **Every scoped series is seeded** with its last known value before
  `started_at`, carried forward to `started_at`. A flat signal still has a point,
  and one that changes later in the run has its value from the start. A series
  with a real point exactly at `started_at` is not seeded: that point is its
  value there. `manifest.seed.seeded_rows` counts the seed rows emitted. Set
  `seed_stable_signals: false` for change-events-only. Seeding requires a
  non-empty `tag_scope` to bound the pre-window lookback (`max_span_seconds`).
* **Markers** render as **Grafana annotation regions** (the run window) + point
  annotations (markers / lifecycle events). See `grafana/README.md`.
* **`run.schema_version`** must be a version this exporter reads: `1` today.
  Omitted, it is `1`; any other value returns `400 invalid_argument` naming the
  supported versions. `manifest.run.schema_version` records the version used.

## Run

```bash
cd /path/to/anolis-telemetry-export
python -m telemetry_export.export_service --config config/bioreactor/telemetry-export.bioreactor.yaml
```

## Quick Local Export (Existing Run)

Use this when you already ran an experiment and just want data out of Influx.

```bash
BASE_URL="http://127.0.0.1:8091"
EXPORT_TOKEN="export-dev-token"
START="2026-04-13T00:00:00Z"
END="2026-04-13T06:00:00Z"
mkdir -p artifacts/exports
```

Discover available `runtime_name` values first (recommended):

```bash
curl -sS --request POST "http://127.0.0.1:8086/api/v2/query?org=anolis" \
  --header "Authorization: Token dev-token" \
  --header "Accept: application/csv" \
  --header "Content-type: application/vnd.flux" \
  --data 'from(bucket:"anolis") |> range(start: -30d) |> filter(fn:(r) => r._measurement == "anolis_signal") |> keep(columns:["runtime_name"]) |> group() |> distinct(column:"runtime_name")'
```

Bioreactor profile runtime names:

1. `bioreactor-manual`
2. `bioreactor-telemetry`
3. `bioreactor-automation`
4. `bioreactor-full`

Export downsampled CSV for the bioreactor device set:

```bash
curl -sS -D artifacts/exports/run.headers \
  -o artifacts/exports/run.csv \
  -X POST "${BASE_URL}/v1/exports/signals:query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${EXPORT_TOKEN}" \
  -H "X-Requester-Id: lab-validation" \
  -d "{
    \"time_range\": {\"start\": \"${START}\", \"end\": \"${END}\"},
    \"selector\": {
      \"provider_ids\": [\"bread0\", \"ezo0\"],
      \"device_ids\": [\"rlht0\", \"dcmt0\", \"dcmt1\", \"ph0\", \"do0\"]
    },
    \"resolution\": {\"mode\": \"downsampled\", \"interval\": \"10s\", \"aggregation\": \"last\"},
    \"format\": \"csv\"
  }"
```

Fetch manifest:

```bash
EXPORT_ID=$(awk '/^X-Export-Id:/ {print $2}' artifacts/exports/run.headers | tr -d '\r')
curl -sS "${BASE_URL}/v1/exports/manifests/${EXPORT_ID}" \
  -H "Authorization: Bearer ${EXPORT_TOKEN}" \
  > artifacts/exports/run.manifest.json
```

## Example Query (JSON Response)

```bash
curl -sS -X POST "http://127.0.0.1:8091/v1/exports/signals:query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer export-dev-token" \
  -H "X-Requester-Id: operator-ui" \
  -d '{
    "time_range": {
      "start": "2026-04-01T00:00:00Z",
      "end": "2026-04-01T00:30:00Z"
    },
    "selector": {
      "runtime_names": ["bioreactor-telemetry"],
      "provider_ids": ["bread0", "ezo0"],
      "device_ids": ["rlht0", "dcmt0", "dcmt1", "ph0", "do0"]
    },
    "resolution": {
      "mode": "raw_event"
    },
    "format": "json"
  }'
```

## Example Query (CSV Response)

`format=csv` returns a `text/csv` body. The service writes CSV to a temporary
spool file first (bounded memory path), enforces row limits, then streams the
file to the response.

```bash
curl -sS -D /tmp/export.headers \
  -o /tmp/export.csv \
  -X POST "http://127.0.0.1:8091/v1/exports/signals:query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer export-dev-token" \
  -d '{
    "time_range": {
      "start": "2026-04-01T00:00:00Z",
      "end": "2026-04-01T00:30:00Z"
    },
    "resolution": {
      "mode": "downsampled",
      "interval": "10s",
      "aggregation": "last"
    },
    "format": "csv"
  }'
```

Fetch manifest metadata for the CSV response:

```bash
export EXPORT_ID=$(grep -i '^X-Export-Id:' /tmp/export.headers | awk '{print $2}' | tr -d '\r')
curl -sS "http://127.0.0.1:8091/v1/exports/manifests/${EXPORT_ID}" \
  -H "Authorization: Bearer export-dev-token"
```

## Example Query (NDJSON Response)

```bash
curl -sS -X POST "http://127.0.0.1:8091/v1/exports/signals:query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer export-dev-token" \
  -d '{
    "time_range": {
      "start": "2026-04-01T00:00:00Z",
      "end": "2026-04-01T00:30:00Z"
    },
    "resolution": {
      "mode": "raw_event"
    },
    "format": "ndjson"
  }'
```

## Programmatic Example

```bash
python telemetry_export/examples/query_signals.py \
  --start 2026-04-01T00:00:00Z \
  --end 2026-04-01T00:30:00Z \
  --provider bread0 \
  --provider ezo0 \
  --format json
```

## Notes

1. MVP scope is `signals` only.
2. Completeness is best-effort under current telemetry overflow behavior.
3. `bytes` vs `string` fidelity remains a documented MVP limitation.
4. Config validates field ranges and semantics (for example: `server.port`
   bounds and positive limit values).
5. `authorization.enforce_selector_scope=true` requires at least one non-empty
   `allowed_*` allowlist.
6. On both export routes, `limits.max_response_bytes` is enforced for
   `format=json`; `csv` and `ndjson` are governed by `limits.max_stream_bytes`.
   `max_rows` applies to every format.
7. Manifest metadata retention is bounded by `limits.max_manifest_entries` and
   `limits.manifest_ttl_seconds`.
