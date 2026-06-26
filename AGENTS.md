# AGENTS.md — anolis-telemetry-export

> Per-repo conventions for coding agents. The canonical cross-repo rules
> (Conventional Commits, minimal-first/YAGNI, no secrets, run checks before
> claiming success) live in the user's **global** `AGENTS.md` and are not
> repeated here. This file records only what is specific to this repo.

## Build / test

- `uv sync --locked --extra dev` to set up, then `uv run ruff check .`,
  `uv run ruff format --check .`, `uv run mypy telemetry_export tests/integration`,
  and `uv run pytest` (tests live under `tests/integration`).
- A separate **e2e lane** runs against a real InfluxDB container.
- The required CI status check is the **`ok`** job; never merge red.

## Tooling

- `uv` + `uv.lock` (Python 3.12+). ruff = lint **and** format; mypy for types.

## Repo-specific gotchas

- **Lean dependency surface by design** — runtime deps are only `requests` +
  `pyyaml` (no heavy frameworks/clients). Justify any new dependency; prefer the
  stdlib. (Note: not literally stdlib-only — it does ship those two deps.)
- Exports the `anolis_signal` timeseries to **InfluxDB via Flux** queries.
- The package is **`telemetry_export`** — run
  `python -m telemetry_export.export_service` (or the `anolis-telemetry-export`
  console script). There is **no `tools.` prefix**.
