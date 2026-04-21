#!/usr/bin/env python3
"""Sync vendored telemetry schema from an anolis release artifact.

This script downloads the telemetry schema release bundle from:
  https://github.com/<repo>/releases/download/<tag>/<asset>

Then it:

1. extracts the schema file,
2. updates vendored schema copy,
3. rewrites lock file in release-artifact mode with pinned checksums.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fetch_url_bytes(url: str, timeout_seconds: int = 45) -> bytes:
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            return bytes(response.read())
    except HTTPError as exc:
        raise RuntimeError(f"HTTP error while fetching {url}: {exc.code} {exc.reason}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error while fetching {url}: {exc.reason}") from exc


def extract_tar_member(archive_bytes: bytes, member_path: str) -> bytes:
    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as archive:
        # tar -C <dir> . produces ./path members; try both forms.
        try:
            member = archive.getmember(member_path)
        except KeyError:
            try:
                member = archive.getmember(f"./{member_path}")
            except KeyError as exc:
                raise RuntimeError(f"schema member not found in artifact: {member_path}") from exc

        if not member.isfile():
            raise RuntimeError(f"schema member is not a regular file: {member_path}")

        handle = archive.extractfile(member)
        if handle is None:
            raise RuntimeError(f"failed to extract schema member: {member_path}")
        return handle.read()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync vendored telemetry schema from an anolis release artifact")
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Repository root path (default: auto-detected)",
    )
    parser.add_argument(
        "--upstream-repo",
        default="anolishq/anolis",
        help="Upstream GitHub repo in owner/name form",
    )
    parser.add_argument(
        "--tag",
        required=True,
        help="Upstream release tag (for example: v0.2.0)",
    )
    parser.add_argument(
        "--asset",
        default="",
        help="Release asset name (default: inferred as anolis-<version>-telemetry-schema.tar.gz)",
    )
    parser.add_argument(
        "--manifest-asset",
        default="telemetry-schema-manifest.json",
        help="Schema manifest asset name",
    )
    parser.add_argument(
        "--schema-member",
        default="schemas/telemetry/telemetry-timeseries.schema.v1.json",
        help="Schema path inside release tarball",
    )
    parser.add_argument(
        "--vendored-schema-path",
        default="contracts/upstream/anolis/telemetry-timeseries.schema.v1.json",
        help="Vendored schema path relative to repo root",
    )
    parser.add_argument(
        "--lock-path",
        default="contracts/upstream/anolis/telemetry-timeseries.lock.json",
        help="Lock file path relative to repo root",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    repo_root = Path(args.repo_root).resolve()
    schema_out = (repo_root / args.vendored_schema_path).resolve()
    lock_out = (repo_root / args.lock_path).resolve()

    version = args.tag[1:] if args.tag.startswith("v") else args.tag
    asset = args.asset or f"anolis-{version}-telemetry-schema.tar.gz"

    asset_url = f"https://github.com/{args.upstream_repo}/releases/download/{args.tag}/{asset}"
    manifest_url = f"https://github.com/{args.upstream_repo}/releases/download/{args.tag}/{args.manifest_asset}"

    asset_bytes = fetch_url_bytes(asset_url)
    asset_sha = sha256_bytes(asset_bytes)

    schema_bytes = extract_tar_member(asset_bytes, args.schema_member)
    schema_sha = sha256_bytes(schema_bytes)

    manifest_bytes = fetch_url_bytes(manifest_url)
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"failed to parse schema manifest asset as JSON: {exc}") from exc

    if not isinstance(manifest, dict):
        raise RuntimeError("schema manifest asset must be a JSON object")

    manifest_asset = manifest.get("asset")
    manifest_sha = manifest.get("sha256")
    if manifest_asset != asset:
        raise RuntimeError(f"schema manifest asset mismatch: expected '{asset}', found '{manifest_asset}'")
    if manifest_sha != asset_sha:
        raise RuntimeError(f"schema manifest sha mismatch: expected '{asset_sha}', found '{manifest_sha}'")

    schema_out.parent.mkdir(parents=True, exist_ok=True)
    lock_out.parent.mkdir(parents=True, exist_ok=True)

    schema_out.write_bytes(schema_bytes)

    lock_payload = {
        "schema_version": 2,
        "source": {
            "repo": args.upstream_repo,
            "path": args.schema_member,
            "tag": args.tag,
        },
        "distribution": {
            "mode": "release-artifact",
            "release": {
                "repo": args.upstream_repo,
                "tag": args.tag,
                "asset": asset,
                "manifest_asset": args.manifest_asset,
            },
            "schema_sha256": schema_sha,
            "asset_sha256": asset_sha,
        },
        "synced_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }

    lock_out.write_text(json.dumps(lock_payload, indent=2) + "\n", encoding="utf-8")

    print("upstream schema sync summary")
    print(f"  repo:            {args.upstream_repo}")
    print(f"  tag:             {args.tag}")
    print(f"  asset:           {asset}")
    print(f"  asset_sha256:    {asset_sha}")
    print(f"  schema_member:   {args.schema_member}")
    print(f"  schema_sha256:   {schema_sha}")
    print(f"  schema_out:      {schema_out}")
    print(f"  lock_out:        {lock_out}")
    print("\nSync complete. Commit updated schema + lock.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
