"""Unit tests for run-based export: RunManifest parsing, window/scope derivation,
stable-signal seed query, and Grafana run annotations."""

from __future__ import annotations

import pytest

from telemetry_export.export_core.flux_builder import build_seed_flux_query_from_plan
from telemetry_export.export_core.models import ApiError, Resolution, SignalsQuery
from telemetry_export.export_core.query_plan import build_query_plan
from telemetry_export.export_core.run_export import (
    build_run_annotations,
    build_signals_request_from_run,
    parse_run_manifest,
    run_provenance,
)


def _manifest_body(**overrides):
    body = {
        "schema_version": 1,
        "run_id": "bioreactor-telemetry-01ABC",
        "started_at_epoch_ms": 1_711_929_600_000,  # 2024-04-01T00:00:00Z
        "ended_at_epoch_ms": 1_711_933_200_000,  # 2024-04-01T01:00:00Z
        "polling_interval_ms": 2000,
        "runtime_names": ["bioreactor-telemetry"],
        "runtime_version": "0.1.24",
        "experiment_label": "campaign-A",
        "tag_scope": {"provider_ids": ["bread0"], "device_ids": ["rlht0"], "signal_ids": []},
        "markers": [
            {"sequence": 1, "category": "run_opened", "type": "", "occurred_at_epoch_ms": 1_711_929_600_000},
            {
                "sequence": 4,
                "category": "annotation",
                "type": "sample",
                "occurred_at_epoch_ms": 1_711_931_400_000,
                "payload": {"volume_ml": 5},
            },
        ],
    }
    body.update(overrides)
    return body


def test_parse_run_manifest_round_trip():
    run = parse_run_manifest(_manifest_body())
    assert run.run_id == "bioreactor-telemetry-01ABC"
    assert run.started_at_epoch_ms == 1_711_929_600_000
    assert run.ended_at_epoch_ms == 1_711_933_200_000
    assert run.polling_interval_ms == 2000
    assert run.provider_ids == ["bread0"]
    assert run.has_scope is True
    assert len(run.markers) == 2
    assert run.markers[1].type == "sample"
    assert run.markers[1].payload == {"volume_ml": 5}


def test_parse_run_manifest_rejects_missing_run_id_and_bad_window():
    with pytest.raises(ApiError):
        parse_run_manifest(_manifest_body(run_id=""))
    with pytest.raises(ApiError):
        parse_run_manifest(_manifest_body(ended_at_epoch_ms=1_711_929_500_000))  # end < start
    with pytest.raises(ApiError):
        parse_run_manifest("not-an-object")


def test_open_run_has_no_scope_flag_and_null_end():
    run = parse_run_manifest(
        _manifest_body(ended_at_epoch_ms=None, tag_scope={"provider_ids": [], "device_ids": [], "signal_ids": []})
    )
    assert run.ended_at_epoch_ms is None
    assert run.has_scope is False


def test_build_signals_request_derives_half_open_window_and_scope():
    run = parse_run_manifest(_manifest_body())
    request = build_signals_request_from_run(
        run, resolution={"mode": "raw_event"}, fmt="json", columns=None, now_epoch_ms=9_999_999_999_999
    )
    # Window comes straight from the run; end is exclusive (Flux range semantics).
    assert request["time_range"]["start"] == "2024-04-01T00:00:00Z"
    assert request["time_range"]["end"] == "2024-04-01T01:00:00Z"
    assert request["selector"]["provider_ids"] == ["bread0"]
    assert request["selector"]["runtime_names"] == ["bioreactor-telemetry"]


def test_open_run_export_window_ends_at_now():
    run = parse_run_manifest(_manifest_body(ended_at_epoch_ms=None))
    request = build_signals_request_from_run(
        run, resolution={"mode": "raw_event"}, fmt="json", columns=None, now_epoch_ms=1_711_940_400_000
    )
    assert request["time_range"]["end"] == "2024-04-01T03:00:00Z"  # now, since the run is open


def test_seed_flux_query_is_last_before_window_over_scope():
    run = parse_run_manifest(_manifest_body())
    # Seed window: a lookback ending exactly at run_start.
    seed_query = SignalsQuery(
        start=__import__("datetime").datetime(2024, 3, 31, 23, 0, tzinfo=__import__("datetime").timezone.utc),
        end=__import__("datetime").datetime(2024, 4, 1, 0, 0, tzinfo=__import__("datetime").timezone.utc),
        resolution=Resolution(mode="raw_event"),
        fmt="json",
        columns=["timestamp", "value"],
        runtime_names=run.runtime_names,
        provider_ids=run.provider_ids,
        device_ids=run.device_ids,
        signal_ids=run.signal_ids,
        original_request={},
    )
    flux = build_seed_flux_query_from_plan(build_query_plan(seed_query, "anolis"))
    assert "|> last()" in flux
    assert 'r._measurement == "anolis_signal"' in flux
    assert 'r.provider_id == "bread0"' in flux
    assert 'stop: time(v: "2024-04-01T00:00:00Z")' in flux  # seed window stops at run_start


def test_build_run_annotations_region_plus_markers():
    run = parse_run_manifest(_manifest_body())
    assert run.ended_at_epoch_ms is not None
    annotations = build_run_annotations(run, resolved_end_epoch_ms=run.ended_at_epoch_ms)
    region = annotations[0]
    assert region["isRegion"] is True
    assert region["time"] == 1_711_929_600_000
    assert region["timeEnd"] == 1_711_933_200_000
    assert "anolis-run" in region["tags"] and run.run_id in region["tags"]
    assert region["text"] == "campaign-A"
    # One annotation per marker, point-shaped.
    sample = next(a for a in annotations if "sample" in a["tags"])
    assert sample["time"] == 1_711_931_400_000
    assert "isRegion" not in sample


def test_run_provenance_carries_polling_interval_for_boundary_fuzz():
    run = parse_run_manifest(_manifest_body())
    assert run.ended_at_epoch_ms is not None
    prov = run_provenance(run, resolved_end_epoch_ms=run.ended_at_epoch_ms)
    assert prov["polling_interval_ms"] == 2000
    assert prov["is_open"] is False
    assert prov["tag_scope"]["provider_ids"] == ["bread0"]
