#!/usr/bin/env python3
"""Verify vendored upstream telemetry schema against lock metadata."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(8192)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    schema_path = repo_root / "contracts/upstream/anolis/telemetry-timeseries.schema.v1.json"
    lock_path = repo_root / "contracts/upstream/anolis/telemetry-timeseries.lock.json"

    if not schema_path.is_file():
        print(f"ERROR: schema file not found: {schema_path}", file=sys.stderr)
        return 1
    if not lock_path.is_file():
        print(f"ERROR: lock file not found: {lock_path}", file=sys.stderr)
        return 1

    try:
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"ERROR: failed to parse lock file: {exc}", file=sys.stderr)
        return 1

    expected = lock.get("distribution", {}).get("sha256") if isinstance(lock, dict) else None
    if not isinstance(expected, str) or len(expected) != 64:
        print("ERROR: lock file missing valid distribution.sha256", file=sys.stderr)
        return 1

    actual = sha256_file(schema_path)
    print("upstream schema verification summary")
    print(f"  schema:   {schema_path}")
    print(f"  lock:     {lock_path}")
    print(f"  expected: {expected}")
    print(f"  actual:   {actual}")

    if actual != expected:
        print("\nERROR: schema checksum mismatch", file=sys.stderr)
        return 1

    print("\nSchema checksum verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
