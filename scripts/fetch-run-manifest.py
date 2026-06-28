#!/usr/bin/env python3
"""Materialize a portable RunManifest from a live anolis runtime.

This is the one place that talks to the runtime HTTP API. Run it once while the
runtime is reachable; the resulting manifest is self-contained, so subsequent
`POST /v1/exports/runs:export` calls reproduce the run's export even after the
runtime is offline.

It reads:
  * `GET /v0/runs/{run_id}`         — window, tag_scope, polling interval, provenance
  * `GET /v0/runtime/status`        — runtime name (the runtime_name scope dim)
  * `GET /v0/runs/{run_id}/events`  — operator markers + lifecycle events
                                      (anolishq/anolis#116; skipped gracefully if the
                                      runtime predates it)

Usage:
  python scripts/fetch-run-manifest.py --runtime-url http://127.0.0.1:8080 \
      --run-id default-01J... --out run.manifest.json
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

MANIFEST_SCHEMA_VERSION = 1


def _get_json(base_url: str, path: str, token: str | None, *, timeout: float = 15.0) -> Any:
    url = base_url.rstrip("/") + path
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with urlopen(Request(url, headers=headers), timeout=timeout) as response:  # noqa: S310 (operator-supplied URL)
        return json.loads(response.read().decode("utf-8"))


def _fetch_markers(base_url: str, run_id: str, token: str | None) -> list[dict[str, Any]]:
    """Fetch run events as markers; tolerate a runtime without the events endpoint."""
    try:
        body = _get_json(base_url, f"/v0/runs/{run_id}/events", token)
    except HTTPError as exc:
        if exc.code == 404:
            print(f"note: /v0/runs/{run_id}/events not available (HTTP 404) — markers omitted", file=sys.stderr)
            return []
        raise
    events = body.get("events", []) if isinstance(body, dict) else []
    markers: list[dict[str, Any]] = []
    for event in events:
        markers.append(
            {
                "sequence": event.get("sequence", 0),
                "category": event.get("category", "annotation"),
                "type": event.get("type", ""),
                "occurred_at_epoch_ms": event.get("occurred_at_epoch_ms", 0),
                "payload": event.get("payload", {}),
            }
        )
    return markers


def build_manifest(base_url: str, run_id: str, token: str | None) -> dict[str, Any]:
    run_body = _get_json(base_url, f"/v0/runs/{run_id}", token)
    run = run_body.get("run") if isinstance(run_body, dict) else None
    if not isinstance(run, dict):
        raise RuntimeError(f"unexpected /v0/runs/{run_id} response shape")

    runtime_names: list[str] = []
    try:
        status = _get_json(base_url, "/v0/runtime/status", token)
        name = status.get("name") if isinstance(status, dict) else None
        if isinstance(name, str) and name:
            runtime_names = [name]
    except (HTTPError, URLError) as exc:
        print(f"note: could not read /v0/runtime/status ({exc}) — runtime_names omitted", file=sys.stderr)

    tag_scope = run.get("tag_scope") or {}
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "run_id": run.get("run_id"),
        "started_at_epoch_ms": run.get("started_at_epoch_ms"),
        "ended_at_epoch_ms": run.get("ended_at_epoch_ms"),
        "polling_interval_ms": run.get("polling_interval_ms", 0),
        "runtime_names": runtime_names,
        "runtime_version": run.get("runtime_version"),
        "experiment_label": run.get("experiment_label"),
        "automation_version": run.get("automation_version"),
        "tag_scope": {
            "provider_ids": tag_scope.get("provider_ids", []),
            "device_ids": tag_scope.get("device_ids", []),
            "signal_ids": tag_scope.get("signal_ids", []),
        },
        "markers": _fetch_markers(base_url, run_id, token),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize a portable RunManifest from a live anolis runtime")
    parser.add_argument(
        "--runtime-url", default="http://127.0.0.1:8080", help="Base URL of the anolis runtime HTTP API"
    )
    parser.add_argument("--run-id", required=True, help="Run id to materialize")
    parser.add_argument("--token", default=None, help="Bearer token if the runtime requires auth")
    parser.add_argument("--out", default="-", help="Output path for the manifest JSON (default: stdout)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        manifest = build_manifest(args.runtime_url, args.run_id, args.token)
    except (HTTPError, URLError) as exc:
        print(f"ERROR: failed to reach runtime at {args.runtime_url}: {exc}", file=sys.stderr)
        return 1

    rendered = json.dumps(manifest, indent=2) + "\n"
    if args.out == "-":
        sys.stdout.write(rendered)
    else:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(rendered)
        print(f"Wrote RunManifest for {manifest['run_id']} to {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
