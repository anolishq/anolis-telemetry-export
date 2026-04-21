# TODO

- [ ] Harden config parsing to reject YAML `null` values for required strings/secrets (do not coerce to `"None"`).
- [ ] Harden numeric config parsing to reject booleans for integer fields (for example `server.port: true`).
- [ ] Add bounded retention/eviction for in-memory export manifest storage to avoid unbounded growth.
- [ ] Decide and implement explicit response-byte controls for streamed `csv`/`ndjson` exports (or document accepted risk).
