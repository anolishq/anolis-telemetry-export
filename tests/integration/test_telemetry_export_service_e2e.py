"""End-to-end export service integration against a live InfluxDB fixture."""
# ruff: noqa: I001

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
import os
import subprocess
import sys
import tempfile
import time
from typing import Any

import pytest
import requests
import yaml


pytestmark = pytest.mark.skipif(
    os.getenv("ANOLIS_EXPORT_E2E", "0") != "1",
    reason="set ANOLIS_EXPORT_E2E=1 to run export service e2e integration tests",
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _write_points(influx_url: str, token: str, org: str, bucket: str, lines: list[str]) -> None:
    payload = "\n".join(lines) + "\n"
    response = requests.post(
        f"{influx_url}/api/v2/write",
        params={"org": org, "bucket": bucket, "precision": "ms"},
        headers={"Authorization": f"Token {token}", "Content-Type": "text/plain; charset=utf-8"},
        data=payload.encode("utf-8"),
        timeout=15,
    )
    response.raise_for_status()


def _wait_for_http_ok(url: str, timeout_seconds: int = 30) -> None:
    start = time.time()
    while time.time() - start < timeout_seconds:
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(0.3)
    raise RuntimeError(f"Timed out waiting for healthy endpoint: {url}")


def _collect_service_output(process: subprocess.Popen[str]) -> str:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)

    if process.stdout is None:
        return "<service stdout unavailable>"

    try:
        output = process.stdout.read().strip()
    except Exception as exc:
        return f"<unable to read service stdout: {exc}>"
    return output or "<service stdout empty>"


def _assert_status(
    response: requests.Response,
    expected_status: int,
    *,
    process: subprocess.Popen[str],
    label: str,
) -> None:
    if response.status_code == expected_status:
        return
    request_id = response.headers.get("X-Request-Id", "<missing>")
    service_output = _collect_service_output(process)
    raise AssertionError(
        f"{label} expected HTTP {expected_status}, got {response.status_code}. "
        f"request_id={request_id}. response_body={response.text}. service_output={service_output}"
    )


def _make_service_config(*, port: int, max_response_bytes: int, max_stream_bytes: int) -> dict[str, Any]:
    influx_cfg = {
        "url": os.getenv("ANOLIS_EXPORT_E2E_INFLUX_URL", "http://127.0.0.1:8086"),
        "org": os.getenv("ANOLIS_EXPORT_E2E_INFLUX_ORG", "anolis"),
        "bucket": os.getenv("ANOLIS_EXPORT_E2E_INFLUX_BUCKET", "anolis"),
        "token": os.getenv("ANOLIS_EXPORT_E2E_INFLUX_TOKEN", "dev-token"),
    }
    return {
        "server": {"host": "127.0.0.1", "port": port, "auth_token": "export-e2e-token"},
        "influxdb": influx_cfg,
        "authorization": {
            "enforce_selector_scope": True,
            "allowed_runtime_names": ["e2e-runtime"],
            "allowed_provider_ids": ["bread0"],
            "allowed_device_ids": [],
            "allowed_signal_ids": [],
        },
        "limits": {
            "max_span_seconds": 86400,
            "max_rows": 5000,
            "max_response_bytes": max_response_bytes,
            "max_stream_bytes": max_stream_bytes,
            "max_selector_items": 128,
            "request_timeout_seconds": 15,
            "max_request_bytes": 200000,
            "max_manifest_entries": 1000,
            "manifest_ttl_seconds": 3600,
        },
    }


@contextmanager
def _serve(service_cfg: dict[str, Any]) -> Generator[tuple[subprocess.Popen[str], dict[str, Any]], None, None]:
    repo_root = _repo_root()
    port = service_cfg["server"]["port"]
    with tempfile.TemporaryDirectory(prefix="anolis_export_e2e_") as tmp_dir:
        cfg_path = Path(tmp_dir) / "telemetry-export.e2e.yaml"
        cfg_path.write_text(yaml.safe_dump(service_cfg, sort_keys=False), encoding="utf-8")

        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "telemetry_export.export_service",
                "--config",
                str(cfg_path),
            ],
            cwd=str(repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            _wait_for_http_ok(f"http://127.0.0.1:{port}/v1/health", timeout_seconds=30)
            yield process, service_cfg
        except Exception as exc:
            exit_code = process.poll()
            startup_output = ""
            if exit_code is not None and process.stdout is not None:
                try:
                    startup_output = process.stdout.read().strip()
                except Exception:
                    startup_output = "<unable to read process output>"
            detail = (
                f"export service failed to start (exit_code={exit_code}). startup_output={startup_output or '<none>'}"
            )
            raise RuntimeError(detail) from exc
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()


@pytest.fixture()
def export_service_process() -> Generator[tuple[subprocess.Popen[str], dict[str, Any]], None, None]:
    # A deliberately tiny response budget so the large-query path trips the limit.
    cfg = _make_service_config(port=18091, max_response_bytes=5000, max_stream_bytes=5000)
    with _serve(cfg) as served:
        yield served


@pytest.fixture()
def run_export_service_process() -> Generator[tuple[subprocess.Popen[str], dict[str, Any]], None, None]:
    # Run exports carry data + run provenance + annotations, so they need a
    # realistic response budget (matching the default config).
    cfg = _make_service_config(port=18092, max_response_bytes=200000, max_stream_bytes=200000)
    with _serve(cfg) as served:
        yield served


def test_export_service_e2e_paths(
    export_service_process: tuple[subprocess.Popen[str], dict[str, Any]],
) -> None:
    _process, service_cfg = export_service_process
    influx_cfg = service_cfg["influxdb"]
    base_url = "http://127.0.0.1:18091"
    headers = {
        "Authorization": "Bearer export-e2e-token",
        "Content-Type": "application/json",
        "X-Requester-Id": "pytest-e2e",
    }

    base_ts = 1711929600000
    points = [
        (
            "anolis_signal,runtime_name=e2e-runtime,provider_id=bread0,device_id=dcmt0,signal_id=motor.rpm "
            f'value_double=120.5,quality="OK" {base_ts}'
        ),
        (
            "anolis_signal,runtime_name=e2e-runtime,provider_id=bread0,device_id=rlht0,signal_id=note "
            f'value_string="line1\\\\nline2",quality="OK" {base_ts + 10}'
        ),
        (
            "anolis_signal,runtime_name=e2e-runtime,provider_id=ezo0,device_id=ph0,signal_id=ph.value "
            f'value_double=7.12,quality="OK" {base_ts + 20}'
        ),
    ]
    for idx in range(350):
        points.append(
            "anolis_signal,runtime_name=e2e-runtime,provider_id=bread0,device_id=dcmt0,signal_id=motor.rpm "
            f'value_double={100 + idx / 10.0},quality="OK" {base_ts + 100 + idx}'
        )
    _write_points(influx_cfg["url"], influx_cfg["token"], influx_cfg["org"], influx_cfg["bucket"], points)

    raw_request = {
        "time_range": {"start": "2024-04-01T00:00:00Z", "end": "2024-04-01T01:00:00Z"},
        "selector": {
            "runtime_names": ["e2e-runtime"],
            "provider_ids": ["bread0"],
            "device_ids": ["rlht0"],
        },
        "resolution": {"mode": "raw_event"},
        "format": "json",
    }
    raw_response = requests.post(
        f"{base_url}/v1/exports/signals:query",
        headers=headers,
        json=raw_request,
        timeout=30,
    )
    _assert_status(raw_response, 200, process=_process, label="raw json query")
    raw_payload = raw_response.json()
    assert raw_payload["status"] == "ok"
    assert raw_payload["manifest"]["row_count"] > 0
    assert raw_payload["manifest"]["export_id"]

    csv_request = {
        "time_range": {"start": "2024-04-01T00:00:00Z", "end": "2024-04-01T01:00:00Z"},
        "selector": {
            "runtime_names": ["e2e-runtime"],
            "provider_ids": ["bread0"],
        },
        "resolution": {"mode": "downsampled", "interval": "10s", "aggregation": "last"},
        "format": "csv",
    }
    csv_response = requests.post(
        f"{base_url}/v1/exports/signals:query",
        headers=headers,
        json=csv_request,
        timeout=30,
    )
    _assert_status(csv_response, 200, process=_process, label="downsample csv query")
    assert csv_response.headers["Content-Type"].startswith("text/csv")
    assert "X-Export-Manifest" not in csv_response.headers
    assert csv_response.headers.get("X-Export-Id")
    assert csv_response.headers.get("X-Export-Manifest-Hash", "").startswith("sha256:")

    export_id = csv_response.headers["X-Export-Id"]
    manifest_response = requests.get(
        f"{base_url}/v1/exports/manifests/{export_id}",
        headers={"Authorization": "Bearer export-e2e-token"},
        timeout=10,
    )
    _assert_status(manifest_response, 200, process=_process, label="manifest fetch")
    manifest_payload = manifest_response.json()
    assert manifest_payload["status"] == "ok"
    assert manifest_payload["manifest"]["export_id"] == export_id

    ndjson_request = {
        "time_range": {"start": "2024-04-01T00:00:00Z", "end": "2024-04-01T01:00:00Z"},
        "selector": {"runtime_names": ["e2e-runtime"], "provider_ids": ["bread0"], "device_ids": ["rlht0"]},
        "resolution": {"mode": "raw_event"},
        "format": "ndjson",
    }
    ndjson_response = requests.post(
        f"{base_url}/v1/exports/signals:query",
        headers=headers,
        json=ndjson_request,
        timeout=30,
    )
    _assert_status(ndjson_response, 200, process=_process, label="raw ndjson query")
    assert ndjson_response.headers["Content-Type"].startswith("application/x-ndjson")
    assert len([line for line in ndjson_response.text.splitlines() if line.strip()]) > 0

    large_request = {
        "time_range": {"start": "2024-04-01T00:00:00Z", "end": "2024-04-01T01:00:00Z"},
        "selector": {
            "runtime_names": ["e2e-runtime"],
            "provider_ids": ["bread0"],
        },
        "resolution": {"mode": "raw_event"},
        "format": "json",
    }
    large_response = requests.post(
        f"{base_url}/v1/exports/signals:query",
        headers=headers,
        json=large_request,
        timeout=30,
    )
    _assert_status(large_response, 413, process=_process, label="large json limit query")

    denied_request = {
        "time_range": {"start": "2024-04-01T00:00:00Z", "end": "2024-04-01T01:00:00Z"},
        "selector": {"runtime_names": ["e2e-runtime"], "provider_ids": ["ezo0"]},
        "resolution": {"mode": "raw_event"},
        "format": "json",
    }
    denied_response = requests.post(
        f"{base_url}/v1/exports/signals:query",
        headers=headers,
        json=denied_request,
        timeout=30,
    )
    _assert_status(denied_response, 403, process=_process, label="scope denied query")


def test_run_export_e2e(
    run_export_service_process: tuple[subprocess.Popen[str], dict[str, Any]],
) -> None:
    _process, service_cfg = run_export_service_process
    influx_cfg = service_cfg["influxdb"]
    base_url = "http://127.0.0.1:18092"
    headers = {
        "Authorization": "Bearer export-e2e-token",
        "Content-Type": "application/json",
        "X-Requester-Id": "pytest-e2e-run",
    }

    run_start = 1711929600000  # 2024-04-01T00:00:00Z
    run_end = run_start + 60_000  # +1 minute
    points = [
        # A signal that changes inside the run window.
        (
            "anolis_signal,runtime_name=e2e-runtime,provider_id=bread0,device_id=dcmt0,signal_id=motor.rpm "
            f'value_double=120.0,quality="OK" {run_start + 1000}'
        ),
        (
            "anolis_signal,runtime_name=e2e-runtime,provider_id=bread0,device_id=dcmt0,signal_id=motor.rpm "
            f'value_double=180.0,quality="OK" {run_start + 2000}'
        ),
        # A stable signal whose last change is BEFORE the window — it must be
        # seeded (carried forward to run_start) so it appears in the export.
        (
            "anolis_signal,runtime_name=e2e-runtime,provider_id=bread0,device_id=rlht0,signal_id=setpoint.c "
            f'value_double=25.0,quality="OK" {run_start - 5000}'
        ),
    ]
    _write_points(influx_cfg["url"], influx_cfg["token"], influx_cfg["org"], influx_cfg["bucket"], points)

    run_manifest = {
        "schema_version": 1,
        "run_id": "e2e-runtime-01J0RUN",
        "started_at_epoch_ms": run_start,
        "ended_at_epoch_ms": run_end,
        "polling_interval_ms": 2000,
        "runtime_names": ["e2e-runtime"],
        "runtime_version": "0.1.24",
        "experiment_label": "e2e-campaign",
        "tag_scope": {"provider_ids": ["bread0"], "device_ids": [], "signal_ids": []},
        "markers": [
            {"sequence": 1, "category": "run_opened", "type": "", "occurred_at_epoch_ms": run_start},
            {
                "sequence": 2,
                "category": "annotation",
                "type": "sample",
                "occurred_at_epoch_ms": run_start + 30_000,
                "payload": {"volume_ml": 5},
            },
        ],
    }

    response = requests.post(
        f"{base_url}/v1/exports/runs:export",
        headers=headers,
        json={"run": run_manifest, "format": "json", "seed_stable_signals": True},
        timeout=30,
    )
    _assert_status(response, 200, process=_process, label="run export")
    payload = response.json()

    assert payload["status"] == "ok"
    assert payload["dataset"] == "run"
    assert payload["run_id"] == "e2e-runtime-01J0RUN"
    assert payload["manifest"]["run"]["polling_interval_ms"] == 2000
    assert payload["manifest"]["seed"]["enabled"] is True
    assert payload["manifest"]["seed"]["seeded_rows"] >= 1

    rows = payload["data"]
    by_signal = {row["signal_id"]: row for row in rows}
    # The in-window signal is present.
    assert "motor.rpm" in by_signal
    # The stable signal was seeded at the run-start boundary, carrying its
    # pre-window value forward.
    assert "setpoint.c" in by_signal
    seeded = next(r for r in rows if r["signal_id"] == "setpoint.c")
    assert seeded["timestamp"].startswith("2024-04-01T00:00:00")
    assert float(seeded["value"]) == 25.0

    # Annotations: a run-window region + a point per marker.
    annotations = payload["annotations"]
    region = annotations[0]
    assert region["isRegion"] is True
    assert region["time"] == run_start and region["timeEnd"] == run_end
    assert any("sample" in a.get("tags", []) for a in annotations)

    # Opting out of seeding drops the stable signal.
    no_seed = requests.post(
        f"{base_url}/v1/exports/runs:export",
        headers=headers,
        json={"run": run_manifest, "format": "json", "seed_stable_signals": False},
        timeout=30,
    )
    _assert_status(no_seed, 200, process=_process, label="run export no-seed")
    no_seed_signals = {row["signal_id"] for row in no_seed.json()["data"]}
    assert "setpoint.c" not in no_seed_signals
    assert "motor.rpm" in no_seed_signals
