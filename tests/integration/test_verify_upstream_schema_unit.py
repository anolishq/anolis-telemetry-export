"""Unit tests for scripts/verify-upstream-schema.py (anolishq/anolis-telemetry-export#104)."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sys
import tarfile
from pathlib import Path

import pytest

SCHEMA = b'{"title": "telemetry"}\n'
MEMBER = "schemas/telemetry/telemetry-timeseries.schema.v1.json"


def _load_script():
    path = Path(__file__).resolve().parents[2] / "scripts" / "verify-upstream-schema.py"
    spec = importlib.util.spec_from_file_location("verify_upstream_schema", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _artifact() -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        info = tarfile.TarInfo(MEMBER)
        info.size = len(SCHEMA)
        archive.addfile(info, io.BytesIO(SCHEMA))
    return buffer.getvalue()


def _release_lock(asset_bytes: bytes) -> dict:
    return {
        "source": {"repo": "anolishq/anolis", "path": MEMBER, "tag": "v0.0.1"},
        "distribution": {
            "mode": "release-artifact",
            "release": {
                "repo": "anolishq/anolis",
                "tag": "v0.0.1",
                "asset": "telemetry-schema.tar.gz",
                "manifest_asset": "telemetry-schema-manifest.json",
            },
            "schema_sha256": hashlib.sha256(SCHEMA).hexdigest(),
            "asset_sha256": hashlib.sha256(asset_bytes).hexdigest(),
        },
    }


def _validate_with_manifest(monkeypatch: pytest.MonkeyPatch, manifest_body: bytes) -> int:
    module = _load_script()
    asset_bytes = _artifact()
    monkeypatch.setattr(
        module,
        "fetch_url_bytes",
        lambda url, timeout_seconds=30: manifest_body if url.endswith("manifest.json") else asset_bytes,
    )
    lock = _release_lock(asset_bytes)
    return int(module.validate_release_mode(SCHEMA, Path("schema.json"), Path("lock.json"), lock, offline=False))


@pytest.mark.parametrize("body", [b"[]", b"null", b'"not a manifest"', b"7"])
def test_release_mode_refuses_a_manifest_that_is_not_an_object(monkeypatch: pytest.MonkeyPatch, capsys, body):
    assert _validate_with_manifest(monkeypatch, body) == 1
    assert "must be a JSON object" in capsys.readouterr().err


def test_release_mode_still_checks_an_object_manifest(monkeypatch: pytest.MonkeyPatch):
    asset_sha = hashlib.sha256(_artifact()).hexdigest()
    good = json.dumps({"asset": "telemetry-schema.tar.gz", "sha256": asset_sha}).encode()
    wrong = json.dumps({"asset": "WRONG", "sha256": asset_sha}).encode()
    assert _validate_with_manifest(monkeypatch, good) == 0
    assert _validate_with_manifest(monkeypatch, wrong) == 1


def _run_main(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, distribution: dict) -> int:
    module = _load_script()
    (tmp_path / "schema.json").write_bytes(SCHEMA)
    (tmp_path / "lock.json").write_text(json.dumps({"distribution": distribution}), encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        ["verify", "--repo-root", str(tmp_path), "--schema-path", "schema.json", "--lock-path", "lock.json"],
    )
    return int(module.main())


@pytest.mark.parametrize("with_sha", [False, True])
@pytest.mark.parametrize("mode", ["release_artifact", "bogus"])
def test_an_unknown_lock_mode_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys, mode, with_sha):
    distribution = {"mode": mode}
    if with_sha:
        distribution["sha256"] = hashlib.sha256(SCHEMA).hexdigest()
    assert _run_main(monkeypatch, tmp_path, distribution) == 1
    err = capsys.readouterr().err
    assert f"unknown lock mode '{mode}'" in err
    assert "manual-copy" in err and "release-artifact" in err


@pytest.mark.parametrize("distribution", [{"mode": "manual-copy"}, {}])
def test_manual_copy_and_a_legacy_lock_without_a_mode_still_verify(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, distribution
):
    distribution = {**distribution, "sha256": hashlib.sha256(SCHEMA).hexdigest()}
    assert _run_main(monkeypatch, tmp_path, distribution) == 0
