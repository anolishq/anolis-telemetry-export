# anolis-telemetry-export

Telemetry export service for `anolis_signal` timeseries data.

This repository is the authoritative home for the export service that was
extracted from `anolishq/anolis`.

## What Lives Here

1. Export service implementation: `telemetry_export/`
2. Service tests: `tests/integration/`
3. Example service config: `config/bioreactor/telemetry-export.bioreactor.yaml`
4. Upstream telemetry schema mirror + lock:
   - `contracts/upstream/anolis/telemetry-timeseries.schema.v1.json`
   - `contracts/upstream/anolis/telemetry-timeseries.lock.json`
5. Run-based export helpers: `scripts/fetch-run-manifest.py` (materialize a
   portable run manifest from a live runtime) and
   `scripts/push-grafana-annotations.py`.
6. Grafana run-annotation provisioning + dashboard: `grafana/`.

## Run Locally

```bash
python -m telemetry_export.export_service --config config/bioreactor/telemetry-export.bioreactor.yaml
```

## Local Validation

```bash
python scripts/verify-upstream-schema.py
python -m pytest tests/integration/test_telemetry_export_service_unit.py -vv
```

For full API usage and curl examples, see `telemetry_export/README.md`.

## Upstream Schema Sync

Canonical schema source is `anolishq/anolis`.

## Release-Pinned Sync (Recommended)

After a tagged `anolis` release publishes telemetry schema artifacts:

```bash
python scripts/sync-upstream-schema-from-release.py --tag vX.Y.Z
python scripts/verify-upstream-schema.py --require-release-artifact
```

This updates vendored schema and rewrites lock metadata in `release-artifact`
mode with pinned checksums.

Current repository lock mode is recorded in
`contracts/upstream/anolis/telemetry-timeseries.lock.json` and may remain
`manual-copy` until release-artifact cutover is completed.

## Manual Lock Verification

Always verify current lock and schema parity before commit:

```bash
python scripts/verify-upstream-schema.py
```
