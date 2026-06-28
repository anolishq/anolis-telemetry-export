# Grafana run annotations

Render an anolis **run** as a shaded region with operator-marker pins on a
Grafana dashboard.

## How it fits together

1. **Materialize a portable run manifest** from a live runtime (once, while it is
   reachable):

   ```bash
   python scripts/fetch-run-manifest.py \
     --runtime-url http://127.0.0.1:8080 --run-id <run_id> --out run.manifest.json
   ```

2. **Export the run** — the response carries the windowed telemetry plus a
   Grafana-shaped `annotations` array (a region for the run window + a point per
   marker / lifecycle event):

   ```bash
   jq -n --slurpfile run run.manifest.json '{run: $run[0], format: "json"}' \
     | curl -s -H "Authorization: Bearer $ANOLIS_EXPORT_AUTH_TOKEN" \
         -XPOST http://127.0.0.1:8091/v1/exports/runs:export -d @- > run.export.json
   ```

3. **Push the annotations** into Grafana (Editor-role API token):

   ```bash
   python scripts/push-grafana-annotations.py \
     --grafana-url http://localhost:3000 --token "$GRAFANA_TOKEN" --input run.export.json
   ```

The provisioned dashboard (`dashboards/anolis-run.json`) has two annotation
queries — `anolis-run` (regions) and `anolis-marker` (points) — so the pushed
annotations appear automatically.

## Provisioning

Mount these into a Grafana container/instance:

| This repo | Grafana path |
|-----------|--------------|
| `provisioning/datasources/` | `/etc/grafana/provisioning/datasources` |
| `provisioning/dashboards/` | `/etc/grafana/provisioning/dashboards` |
| `dashboards/` | `/var/lib/grafana/dashboards/anolis` |

The InfluxDB datasource reads `GF_INFLUX_URL`, `GF_INFLUX_ORG`, and
`GF_INFLUX_TOKEN` from the environment — secrets are never committed.

## Boundary semantics

The run window is half-open `[run_start, run_end)`. The exact boundary is fuzzy
by approximately `polling_interval_ms` + provider latency + scheduler jitter
(telemetry points are timestamped when the runtime *observes* a change, and are
emitted only on change). `polling_interval_ms` is carried in the export manifest
(`manifest.run.polling_interval_ms`) so a consumer can reason about it. `run_id`
is **never** a telemetry tag — runs correlate to telemetry purely by this time
window over the frozen 4-tag schema.
