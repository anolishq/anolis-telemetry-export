#!/usr/bin/env python3
"""Push a run export's Grafana annotations to a Grafana instance.

The run export (`POST /v1/exports/runs:export`) returns an `annotations` array in
Grafana's native annotation shape: one **region** annotation spanning the run
window plus a **point** annotation per operator marker / lifecycle event. This
helper POSTs each to Grafana's `POST /api/annotations`, so a run renders as a
shaded region with marker pins on any dashboard.

Input is the run-export response JSON (or any object with an `annotations` array,
e.g. a stored manifest). Read from a file or stdin.

Usage:
  curl -s -XPOST .../v1/exports/runs:export -d @req.json | \
    python scripts/push-grafana-annotations.py --grafana-url http://localhost:3000 --token "$GRAFANA_TOKEN"
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _post_annotation(grafana_url: str, token: str, annotation: dict[str, Any], *, timeout: float = 15.0) -> None:
    url = grafana_url.rstrip("/") + "/api/annotations"
    body = json.dumps(annotation).encode("utf-8")
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    with urlopen(Request(url, data=body, headers=headers, method="POST"), timeout=timeout) as response:  # noqa: S310
        response.read()


def _to_grafana_payload(annotation: dict[str, Any], extra_tags: list[str]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "time": int(annotation["time"]),
        "tags": list(annotation.get("tags", [])) + extra_tags,
        "text": annotation.get("text", ""),
    }
    if annotation.get("isRegion") and annotation.get("timeEnd") is not None:
        payload["timeEnd"] = int(annotation["timeEnd"])
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Push run-export annotations to Grafana")
    parser.add_argument("--grafana-url", required=True, help="Base URL of the Grafana instance")
    parser.add_argument("--token", required=True, help="Grafana API token (Editor role)")
    parser.add_argument("--input", default="-", help="Run-export response JSON file (default: stdin)")
    parser.add_argument(
        "--tag", action="append", default=[], help="Extra tag to attach to every annotation (repeatable)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the Grafana payloads without POSTing")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    raw = sys.stdin.read() if args.input == "-" else open(args.input, encoding="utf-8").read()
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"ERROR: input is not valid JSON: {exc}", file=sys.stderr)
        return 1

    annotations = document.get("annotations", []) if isinstance(document, dict) else []
    if not annotations:
        print("ERROR: no `annotations` array found in input", file=sys.stderr)
        return 1

    pushed = 0
    for annotation in annotations:
        payload = _to_grafana_payload(annotation, args.tag)
        if args.dry_run:
            print(json.dumps(payload))
            continue
        try:
            _post_annotation(args.grafana_url, args.token, payload)
            pushed += 1
        except (HTTPError, URLError) as exc:
            print(f"ERROR: failed to POST annotation {payload.get('text')!r}: {exc}", file=sys.stderr)
            return 1

    if not args.dry_run:
        print(f"Pushed {pushed} annotation(s) to {args.grafana_url}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
