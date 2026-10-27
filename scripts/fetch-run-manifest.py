#!/usr/bin/env python3
"""Materialize a portable RunManifest from a live anolis runtime.

This is the one place that talks to the runtime HTTP API. Run it once while the
runtime is reachable; the resulting manifest is self-contained, so subsequent
`POST /v1/exports/runs:export` calls reproduce the run's export even after the
runtime is offline.

It reads:
  * `GET /v0/runs/{run_id}`         — window, tag_scope, polling interval, provenance
  * `GET /v0/runs/{run_id}/events`  — operator markers + lifecycle events
                                      (anolishq/anolis#116; skipped gracefully if the
                                      runtime predates it)

The runtime name (the runtime_name scope dim) is the run_id's prefix: the runtime
mints run ids as `<runtime.name>-<26-char ULID>`, and no endpoint returns the name
itself. `--runtime-name` sets it for a run_id without that shape.

Usage:
  python scripts/fetch-run-manifest.py --runtime-url http://127.0.0.1:8080 \
      --run-id default-01J... --out run.manifest.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

MANIFEST_SCHEMA_VERSION = 1

# `<runtime.name>-<ULID>`: 26 Crockford base32 characters after the last hyphen.
_RUN_ID_RE = re.compile(r"^(?P<runtime_name>.+)-[0-9A-HJKMNP-TV-Z]{26}$")


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


def build_manifest(base_url: str, run_id: str, token: str | None, runtime_name: str | None = None) -> dict[str, Any]:
    run_body = _get_json(base_url, f"/v0/runs/{run_id}", token)
    run = run_body.get("run") if isinstance(run_body, dict) else None
    if not isinstance(run, dict):
        raise RuntimeError(f"unexpected /v0/runs/{run_id} response shape")

    # An empty runtime_names would export every runtime's series in the bucket
    # that match the tag scope, so a run with no name is refused (#101).
    if not runtime_name:
        match = _RUN_ID_RE.match(run_id)
        if match is None:
            raise RuntimeError(
                f"cannot tell the runtime name from run_id {run_id!r} (expected <runtime-name>-<ULID>); "
                "pass --runtime-name"
            )
        runtime_name = match.group("runtime_name")

    tag_scope = run.get("tag_scope") or {}
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "run_id": run.get("run_id"),
        "started_at_epoch_ms": run.get("started_at_epoch_ms"),
        "ended_at_epoch_ms": run.get("ended_at_epoch_ms"),
        "polling_interval_ms": run.get("polling_interval_ms", 0),
        "runtime_names": [runtime_name],
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
    parser.add_argument(
        "--runtime-name",
        default=None,
        help="The runtime's runtime.name (default: the run_id's prefix, <runtime-name>-<ULID>)",
    )
    parser.add_argument("--out", default="-", help="Output path for the manifest JSON (default: stdout)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        manifest = build_manifest(args.runtime_url, args.run_id, args.token, args.runtime_name)
    except (HTTPError, URLError) as exc:
        print(f"ERROR: failed to reach runtime at {args.runtime_url}: {exc}", file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
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
