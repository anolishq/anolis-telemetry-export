"""Run-based export: a portable RunManifest, run-window derivation, and Grafana
run-annotation regions.

A *run* is the anolis runtime's explicit experiment primitive (epic
anolishq/anolis#111). This module makes a run's telemetry exportable as a
self-contained unit without coupling the export service to a live runtime: the
caller supplies a portable ``RunManifest`` (fetched once from the runtime's
``/v0/runs/{id}`` + ``/v0/runs/{id}/events`` via ``scripts/fetch-run-manifest.py``,
or hand-authored), and the exporter derives the query window + scope from it. So
an export stays reproducible after the originating runtime is offline.

Decisions (anolishq/anolis#31, matching the RFC):

* **Window** is half-open ``[run_start, run_end)`` — exactly Flux ``range``'s
  start-inclusive / stop-exclusive semantics. An open run (no ``end``) exports
  up to "now". The boundary is fuzzy by ~ ``polling_interval_ms`` + provider
  latency + scheduler jitter; ``polling_interval_ms`` is carried in the manifest
  so a consumer can reason about it. ``run_id`` is **never** a telemetry tag —
  correlation is the time-window join over the frozen 4-tag schema.
* **Stable signals** (no value change inside the window) are **seeded** with
  their last known value carried forward to ``run_start`` so a flat signal still
  has a point in the export (opt-out via ``seed_stable_signals: false``).
* **Markers** render as **Grafana annotation regions** (the run window) +
  point annotations (operator markers / lifecycle events).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Any

from .models import ApiError

RUN_MANIFEST_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class RunMarker:
    """A single run event/marker (anolis `RunEvent`, anolishq/anolis#116)."""

    sequence: int
    category: str
    type: str
    occurred_at_epoch_ms: int
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunManifest:
    """Portable, self-contained description of a run for offline export."""

    run_id: str
    started_at_epoch_ms: int
    ended_at_epoch_ms: int | None
    polling_interval_ms: int
    runtime_names: list[str]
    provider_ids: list[str]
    device_ids: list[str]
    signal_ids: list[str]
    markers: list[RunMarker]
    runtime_version: str | None = None
    experiment_label: str | None = None
    automation_version: dict[str, Any] | None = None
    schema_version: int = RUN_MANIFEST_SCHEMA_VERSION

    @property
    def has_scope(self) -> bool:
        return bool(self.provider_ids or self.device_ids or self.signal_ids)


def _epoch_ms_to_iso(epoch_ms: int) -> str:
    dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _require_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", f"{field_name} must be an integer")
    return value


def _optional_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    return _require_int(value, field_name)


def _string_list(value: Any, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", f"{field_name} must be an array of strings")
    out: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ApiError(
                HTTPStatus.BAD_REQUEST, "invalid_argument", f"{field_name} entries must be non-empty strings"
            )
        out.append(item.strip())
    return out


def _parse_markers(value: Any) -> list[RunMarker]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", "run.markers must be an array")
    markers: list[RunMarker] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, dict):
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", f"run.markers[{index}] must be an object")
        payload = raw.get("payload", {})
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            raise ApiError(
                HTTPStatus.BAD_REQUEST, "invalid_argument", f"run.markers[{index}].payload must be an object"
            )
        markers.append(
            RunMarker(
                sequence=_require_int(raw.get("sequence", index), f"run.markers[{index}].sequence"),
                category=str(raw.get("category", "annotation")),
                type=str(raw.get("type", "")),
                occurred_at_epoch_ms=_require_int(
                    raw.get("occurred_at_epoch_ms"), f"run.markers[{index}].occurred_at_epoch_ms"
                ),
                payload=payload,
            )
        )
    return markers


def parse_run_manifest(value: Any) -> RunManifest:
    """Validate and parse a portable RunManifest (the `run` block of a request)."""
    if not isinstance(value, dict):
        raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", "run object is required")

    run_id = value.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", "run.run_id is required")

    started = _require_int(value.get("started_at_epoch_ms"), "run.started_at_epoch_ms")
    ended = _optional_int(value.get("ended_at_epoch_ms"), "run.ended_at_epoch_ms")
    if ended is not None and ended < started:
        raise ApiError(
            HTTPStatus.BAD_REQUEST, "invalid_argument", "run.ended_at_epoch_ms must be >= started_at_epoch_ms"
        )

    tag_scope = value.get("tag_scope", {})
    if tag_scope is None:
        tag_scope = {}
    if not isinstance(tag_scope, dict):
        raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", "run.tag_scope must be an object")

    automation_version = value.get("automation_version")
    if automation_version is not None and not isinstance(automation_version, dict):
        raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", "run.automation_version must be an object or null")

    return RunManifest(
        run_id=run_id.strip(),
        started_at_epoch_ms=started,
        ended_at_epoch_ms=ended,
        polling_interval_ms=_require_int(value.get("polling_interval_ms", 0), "run.polling_interval_ms"),
        runtime_names=_string_list(value.get("runtime_names"), "run.runtime_names"),
        provider_ids=_string_list(tag_scope.get("provider_ids"), "run.tag_scope.provider_ids"),
        device_ids=_string_list(tag_scope.get("device_ids"), "run.tag_scope.device_ids"),
        signal_ids=_string_list(tag_scope.get("signal_ids"), "run.tag_scope.signal_ids"),
        markers=_parse_markers(value.get("markers")),
        runtime_version=value.get("runtime_version") if isinstance(value.get("runtime_version"), str) else None,
        experiment_label=value.get("experiment_label") if isinstance(value.get("experiment_label"), str) else None,
        automation_version=automation_version,
        schema_version=_require_int(value.get("schema_version", RUN_MANIFEST_SCHEMA_VERSION), "run.schema_version"),
    )


def build_signals_request_from_run(
    run: RunManifest,
    *,
    resolution: dict[str, Any],
    fmt: str,
    columns: list[str] | None,
    now_epoch_ms: int,
) -> dict[str, Any]:
    """Lower a RunManifest to a signals query request body (reuses the standard
    validation + guardrails). The window is half-open [start, end); an open run
    exports up to `now`."""
    end_ms = run.ended_at_epoch_ms if run.ended_at_epoch_ms is not None else now_epoch_ms
    request: dict[str, Any] = {
        "time_range": {"start": _epoch_ms_to_iso(run.started_at_epoch_ms), "end": _epoch_ms_to_iso(end_ms)},
        "selector": {
            "runtime_names": list(run.runtime_names),
            "provider_ids": list(run.provider_ids),
            "device_ids": list(run.device_ids),
            "signal_ids": list(run.signal_ids),
        },
        "resolution": resolution,
        "format": fmt,
    }
    if columns is not None:
        request["columns"] = columns
    return request


def run_provenance(run: RunManifest, *, resolved_end_epoch_ms: int) -> dict[str, Any]:
    """The run provenance block embedded in a run-export manifest."""
    return {
        "run_id": run.run_id,
        "experiment_label": run.experiment_label,
        "started_at_epoch_ms": run.started_at_epoch_ms,
        "ended_at_epoch_ms": run.ended_at_epoch_ms,
        "resolved_end_epoch_ms": resolved_end_epoch_ms,
        "is_open": run.ended_at_epoch_ms is None,
        "polling_interval_ms": run.polling_interval_ms,
        "runtime_version": run.runtime_version,
        "automation_version": run.automation_version,
        "runtime_names": list(run.runtime_names),
        "tag_scope": {
            "provider_ids": list(run.provider_ids),
            "device_ids": list(run.device_ids),
            "signal_ids": list(run.signal_ids),
        },
    }


def _marker_text(marker: RunMarker) -> str:
    if marker.category == "annotation":
        label = marker.type or "marker"
    else:
        label = f"{marker.category}{(': ' + marker.type) if marker.type else ''}"
    return label


def build_run_annotations(run: RunManifest, *, resolved_end_epoch_ms: int) -> list[dict[str, Any]]:
    """Grafana-shaped annotations: the run window as a region + each marker as a
    point annotation. Timestamps are epoch ms (Grafana's native unit). Post these
    to Grafana's `POST /api/annotations` (see scripts/push-grafana-annotations.py)."""
    region_text = run.experiment_label or run.run_id
    annotations: list[dict[str, Any]] = [
        {
            "time": run.started_at_epoch_ms,
            "timeEnd": resolved_end_epoch_ms,
            "isRegion": True,
            "tags": ["anolis-run", run.run_id],
            "text": region_text,
        }
    ]
    for marker in run.markers:
        annotations.append(
            {
                "time": marker.occurred_at_epoch_ms,
                "tags": ["anolis-marker", marker.category, *([marker.type] if marker.type else [])],
                "text": _marker_text(marker),
            }
        )
    return annotations
