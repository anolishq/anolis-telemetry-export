# anolis-telemetry-export

Telemetry export service repository (extraction target from `anolis`).

## Upstream telemetry schema sync (short-term model)

Canonical schema source is `anolishq/anolis`.

Vendored artifacts in this repo:

1. `contracts/upstream/anolis/telemetry-timeseries.schema.v1.json`
2. `contracts/upstream/anolis/telemetry-timeseries.lock.json`

Verify lock/checksum:

```bash
python3 scripts/verify-upstream-schema.py
```

The CI workflow runs this check on every PR and push.
