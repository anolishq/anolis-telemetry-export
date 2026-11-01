#!/usr/bin/env python3
"""Telemetry export MVP service.

External data-plane service for querying InfluxDB telemetry (`anolis_signal`) with
explicit guardrails and auth. This service intentionally does not modify
`anolis-runtime` HTTP APIs.
"""

from __future__ import annotations

import argparse
import csv
import hmac
import io
import itertools
import json
import logging
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from telemetry_export.export_core.config import load_config
from telemetry_export.export_core.flux_builder import build_flux_query, build_seed_flux_query_from_plan
from telemetry_export.export_core.influx_client import influx_query_csv, influx_query_csv_stream
from telemetry_export.export_core.models import (
    ApiError,
    AppConfig,
    AuthorizationConfig,
    InfluxConfig,
    LimitConfig,
    Resolution,
    ServerConfig,
    SignalsQuery,
    SpoolResult,
)
from telemetry_export.export_core.query_plan import build_query_plan
from telemetry_export.export_core.run_export import (
    build_run_annotations,
    build_signals_request_from_run,
    parse_run_manifest,
    run_provenance,
)
from telemetry_export.export_core.serialization import (
    build_manifest,
    coerce_request_id,
    coerce_requester_id,
    compute_manifest_hash,
    iter_influx_csv_rows,
    json_error_payload,
    normalize_row,
    normalize_rows,
    parse_influx_csv_rows,
    render_csv,
)
from telemetry_export.export_core.validation import parse_rfc3339_utc, reject_timezone, validate_query_request

LOGGER = logging.getLogger("telemetry_export")

# Re-export selected helpers for compatibility with existing tests/tools.
__all__ = [
    "ApiError",
    "AppConfig",
    "AuthorizationConfig",
    "ExportService",
    "InfluxConfig",
    "LimitConfig",
    "Resolution",
    "ServerConfig",
    "SignalsQuery",
    "build_flux_query",
    "influx_query_csv",
    "influx_query_csv_stream",
    "load_config",
    "normalize_row",
    "normalize_rows",
    "parse_influx_csv_rows",
    "render_csv",
    "validate_query_request",
]

CONTENT_TYPE_BY_FORMAT = {
    "csv": "text/csv; charset=utf-8",
    "json": "application/json",
    "ndjson": "application/x-ndjson; charset=utf-8",
}

SUFFIX_BY_FORMAT = {
    "csv": ".csv",
    "json": ".json",
    "ndjson": ".ndjson",
}

# The top-level keys runs:export reads; any other key is refused (#97).
RUN_EXPORT_KEYS = frozenset({"run", "resolution", "format", "columns", "seed_stable_signals"})


def _series_key(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (row.get("runtime_name", ""), row.get("provider_id", ""), row.get("device_id", ""), row.get("signal_id", ""))


def _is_at(timestamp: str, instant: datetime) -> bool:
    """Whether an InfluxDB `_time` (RFC3339, trailing zeros trimmed) is `instant`."""
    try:
        return parse_rfc3339_utc(timestamp, "_time") == instant
    except ApiError:
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Anolis telemetry export MVP service")
    parser.add_argument(
        "--config",
        default="config/bioreactor/telemetry-export.bioreactor.yaml",
        help="Path to export service YAML config",
    )
    parser.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error"])
    return parser.parse_args()


class _BoundedTextWriter:
    """UTF-8 text writer with deterministic max-byte enforcement."""

    def __init__(self, handle: Any, max_bytes: int, limit_name: str):
        self._handle = handle
        self._max_bytes = max_bytes
        self._limit_name = limit_name  # the config key behind max_bytes, for the 413 message
        self._bytes_written = 0

    def write(self, text: str) -> int:
        encoded = text.encode("utf-8")
        self._bytes_written += len(encoded)
        if self._bytes_written > self._max_bytes:
            raise ApiError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "limit_exceeded",
                f"response exceeds {self._limit_name}={self._max_bytes}",
            )
        written = self._handle.write(text)
        return int(written)

    def flush(self) -> None:
        self._handle.flush()

    @property
    def bytes_written(self) -> int:
        return self._bytes_written


class ExportService:
    """In-memory request handler facade for the HTTP layer."""

    def __init__(self, config: AppConfig):
        self.config = config
        self._manifest_lock = threading.Lock()
        self._manifest_by_export_id: dict[str, dict[str, Any]] = {}

    def authorize(self, authorization_header: str | None) -> None:
        # The scheme is case-insensitive (RFC 7235 §2.1); one or more spaces
        # separate it from the token, and the token is compared exactly.
        scheme, separator, token = (authorization_header or "").partition(" ")
        if not separator or scheme.lower() != "bearer":
            raise ApiError(HTTPStatus.UNAUTHORIZED, "unauthenticated", "Authorization: Bearer <token> is required")

        supplied = token.strip()
        # As bytes: compare_digest raises TypeError on a non-ASCII str, which
        # would answer 500 for a malformed token.
        if not supplied or not hmac.compare_digest(supplied.encode(), self.config.server.auth_token.encode()):
            raise ApiError(HTTPStatus.UNAUTHORIZED, "unauthenticated", "invalid bearer token")

    def enforce_scope(self, query: SignalsQuery) -> None:
        auth = self.config.authorization
        if not auth.enforce_selector_scope:
            return

        self.enforce_scope_dimension(query.runtime_names, auth.allowed_runtime_names, "selector.runtime_names")
        self.enforce_scope_dimension(query.provider_ids, auth.allowed_provider_ids, "selector.provider_ids")
        self.enforce_scope_dimension(query.device_ids, auth.allowed_device_ids, "selector.device_ids")
        self.enforce_scope_dimension(query.signal_ids, auth.allowed_signal_ids, "selector.signal_ids")

    @staticmethod
    def enforce_scope_dimension(requested: list[str], allowed: tuple[str, ...], field_name: str) -> None:
        if not allowed:
            return

        if not requested:
            raise ApiError(
                HTTPStatus.FORBIDDEN,
                "permission_denied",
                f"{field_name} must be explicitly set when selector scope enforcement is enabled",
            )

        allowed_set = set(allowed)
        denied = [value for value in requested if value not in allowed_set]
        if denied:
            raise ApiError(
                HTTPStatus.FORBIDDEN,
                "permission_denied",
                f"{field_name} contains unauthorized values: {', '.join(denied[:5])}",
            )

    def get_manifest(self, export_id: str) -> dict[str, Any] | None:
        with self._manifest_lock:
            self._prune_manifests_locked()
            record = self._manifest_by_export_id.get(export_id)
            if record is None:
                return None
            return dict(record["manifest"])

    def _prune_manifests_locked(self) -> None:
        now = time.time()
        ttl_seconds = self.config.limits.manifest_ttl_seconds

        expired_ids = [
            export_id
            for export_id, record in self._manifest_by_export_id.items()
            if now - float(record.get("created_at_epoch", now)) > ttl_seconds
        ]
        for export_id in expired_ids:
            self._manifest_by_export_id.pop(export_id, None)

        overflow = len(self._manifest_by_export_id) - self.config.limits.max_manifest_entries
        if overflow > 0:
            oldest = sorted(
                self._manifest_by_export_id.items(),
                key=lambda item: float(item[1].get("created_at_epoch", now)),
            )[:overflow]
            for export_id, _ in oldest:
                self._manifest_by_export_id.pop(export_id, None)

    def _store_manifest(self, export_id: str, manifest: dict[str, Any], manifest_hash: str) -> None:
        with self._manifest_lock:
            self._prune_manifests_locked()
            self._manifest_by_export_id[export_id] = {
                "manifest_hash": manifest_hash,
                "manifest": manifest,
                "created_at_epoch": time.time(),
            }
            self._prune_manifests_locked()

    def execute_query(
        self,
        request_body: Any,
        *,
        request_id: str = "unknown",
        requester_id: str = "anonymous",
    ) -> tuple[int, dict[str, Any]]:
        query = validate_query_request(request_body, self.config.limits)
        return self.execute_query_from_query(query, request_id=request_id, requester_id=requester_id)

    def execute_query_from_query(
        self,
        query: SignalsQuery,
        *,
        request_id: str = "unknown",
        requester_id: str = "anonymous",
    ) -> tuple[int, dict[str, Any]]:
        # Backward-compatible in-memory helper used by unit tests and direct callers.
        self.enforce_scope(query)
        flux_query = build_flux_query(query, self.config.influx.bucket)
        csv_text = influx_query_csv(self.config, flux_query)
        raw_rows = parse_influx_csv_rows(csv_text)

        if len(raw_rows) > self.config.limits.max_rows:
            raise ApiError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "limit_exceeded",
                f"row count exceeds max_rows={self.config.limits.max_rows}",
            )

        normalized_rows = normalize_rows(raw_rows, query.columns)
        export_id = str(uuid.uuid4())
        manifest = build_manifest(
            query,
            self.config,
            row_count=len(normalized_rows),
            export_id=export_id,
            request_id=request_id,
            requester_id=requester_id,
        )
        manifest_hash = compute_manifest_hash(manifest)
        self._store_manifest(export_id, manifest, manifest_hash)

        if query.fmt == "json":
            payload = {
                "status": "ok",
                "dataset": "signals",
                "format": "json",
                "manifest": manifest,
                "data": normalized_rows,
            }
            encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            if len(encoded) > self.config.limits.max_response_bytes:
                raise ApiError(
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                    "limit_exceeded",
                    f"response exceeds max_response_bytes={self.config.limits.max_response_bytes}",
                )
            return HTTPStatus.OK, payload

        if query.fmt == "ndjson":
            ndjson_body = "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in normalized_rows)
            if len(ndjson_body.encode("utf-8")) > self.config.limits.max_response_bytes:
                raise ApiError(
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                    "limit_exceeded",
                    f"response exceeds max_response_bytes={self.config.limits.max_response_bytes}",
                )
            return HTTPStatus.OK, {
                "status": "ok",
                "dataset": "signals",
                "format": "ndjson",
                "manifest": manifest,
                "ndjson_body": ndjson_body,
            }

        csv_body = render_csv(normalized_rows, query.columns)
        if len(csv_body.encode("utf-8")) > self.config.limits.max_response_bytes:
            raise ApiError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "limit_exceeded",
                f"response exceeds max_response_bytes={self.config.limits.max_response_bytes}",
            )

        return HTTPStatus.OK, {
            "status": "ok",
            "dataset": "signals",
            "format": "csv",
            "manifest": manifest,
            "csv_body": csv_body,
        }

    def _stream_influx_rows(self, flux_query: str) -> Any:
        """Rows of a streamed Influx query, read one at a time."""
        response = influx_query_csv_stream(self.config, flux_query)
        try:
            yield from iter_influx_csv_rows(response)
        except UnicodeDecodeError as exc:
            raise ApiError(
                HTTPStatus.BAD_GATEWAY, "upstream_error", "failed to decode Influx CSV stream as UTF-8"
            ) from exc
        finally:
            response.close()

    def _check_row_count(self, row_count: int) -> None:
        if row_count > self.config.limits.max_rows:
            raise ApiError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "limit_exceeded",
                f"row count exceeds max_rows={self.config.limits.max_rows}",
            )

    def execute_run_export(
        self,
        request_body: Any,
        *,
        request_id: str = "unknown",
        requester_id: str = "anonymous",
    ) -> SpoolResult:
        """Export a run's telemetry from a portable RunManifest: derive the
        half-open [run_start, run_end) window + scope, seed each scoped series
        with its last value carried forward to run_start, and attach Grafana run
        annotations.

        Both Influx queries are streamed and the JSON envelope is spooled to a
        temp file through the same byte limits as signals:query (#99)."""
        if not isinstance(request_body, dict):
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", "request body must be a JSON object")
        # The window and scope come from the run, so a key such as time_range
        # or selector would be dropped without a word (#97).
        reject_timezone(request_body)
        unknown = sorted(set(request_body) - RUN_EXPORT_KEYS)
        if unknown:
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_argument",
                f"runs:export does not accept {', '.join(unknown)}; "
                f"accepted keys: {', '.join(sorted(RUN_EXPORT_KEYS))}",
            )

        run = parse_run_manifest(request_body.get("run"))
        resolution = request_body.get("resolution") or {"mode": "raw_event"}
        fmt = request_body.get("format", "json")
        columns = request_body.get("columns")
        seed_stable_signals = request_body.get("seed_stable_signals", True)
        if not isinstance(seed_stable_signals, bool):
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", "seed_stable_signals must be a boolean")

        now_epoch_ms = int(time.time() * 1000)
        signals_request = build_signals_request_from_run(
            run, resolution=resolution, fmt=fmt, columns=columns, now_epoch_ms=now_epoch_ms
        )
        query = validate_query_request(signals_request, self.config.limits)
        self.enforce_scope(query)

        limits = self.config.limits
        # The window's normalized rows, one JSON object per line, until the
        # seed rows that go before them are known.
        window_file = tempfile.TemporaryFile(mode="w+", encoding="utf-8", newline="")
        out_file = tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", delete=False, prefix="anolis_run_export_", suffix=".json"
        )
        out_path = Path(out_file.name)
        try:
            window_rows = 0
            measured_at_start: set[tuple[str, str, str, str]] = set()
            for raw in self._stream_influx_rows(build_flux_query(query, self.config.influx.bucket)):
                window_rows += 1
                self._check_row_count(window_rows)
                if _is_at(raw.get("_time", ""), query.start):
                    measured_at_start.add(_series_key(raw))
                window_file.write(json.dumps(normalize_row(raw, query.columns), separators=(",", ":")) + "\n")

            # Seed: carry each scoped series' last value before run_start forward to
            # the window boundary, so a signal that never changes during the run still
            # has a point and one that changes later has its value from the start.
            # Bounded by max_span_seconds and only when the run is scoped (an unscoped
            # seed would scan every series in the bucket).
            seed_rows: list[dict[str, Any]] = []
            seed_info: dict[str, Any] = {"enabled": bool(seed_stable_signals)}
            if seed_stable_signals and run.has_scope:
                lookback_seconds = limits.max_span_seconds
                seed_query = SignalsQuery(
                    start=query.start - timedelta(seconds=lookback_seconds),
                    end=query.start,
                    resolution=Resolution(mode="raw_event"),
                    fmt="json",
                    columns=query.columns,
                    runtime_names=query.runtime_names,
                    provider_ids=query.provider_ids,
                    device_ids=query.device_ids,
                    signal_ids=query.signal_ids,
                    original_request={},
                )
                seed_plan = build_query_plan(seed_query, self.config.influx.bucket)
                run_start_iso = query.start.isoformat().replace("+00:00", "Z")
                for raw in self._stream_influx_rows(build_seed_flux_query_from_plan(seed_plan)):
                    # A series with a real point at run_start needs no seed there: two
                    # rows for one series at one timestamp would conflict (#95).
                    if _series_key(raw) in measured_at_start:
                        continue
                    raw["_time"] = run_start_iso  # carry forward to the window boundary
                    seed_rows.append(normalize_row(raw, query.columns))
                    self._check_row_count(window_rows + len(seed_rows))
                seed_info.update({"seeded_rows": len(seed_rows), "lookback_seconds": lookback_seconds})
            elif seed_stable_signals and not run.has_scope:
                seed_info.update({"skipped": "run has no tag_scope; seeding requires a scope to bound the lookback"})

            row_count = len(seed_rows) + window_rows
            export_id = str(uuid.uuid4())
            manifest = build_manifest(
                query,
                self.config,
                row_count=row_count,
                export_id=export_id,
                request_id=request_id,
                requester_id=requester_id,
            )
            resolved_end = run.ended_at_epoch_ms if run.ended_at_epoch_ms is not None else now_epoch_ms
            annotations = build_run_annotations(run, resolved_end_epoch_ms=resolved_end)
            manifest["dataset"] = "run"
            manifest["run"] = run_provenance(run, resolved_end_epoch_ms=resolved_end)
            manifest["seed"] = seed_info
            manifest["annotations"] = annotations
            manifest_hash = compute_manifest_hash(manifest)

            if query.fmt == "json":
                writer = _BoundedTextWriter(out_file, limits.max_response_bytes, "max_response_bytes")
            else:
                writer = _BoundedTextWriter(out_file, limits.max_stream_bytes, "max_stream_bytes")
            writer.write(
                f'{{"status":"ok","dataset":"run","run_id":{json.dumps(run.run_id)},"format":{json.dumps(query.fmt)},'
            )
            window_file.seek(0)
            lines = [json.dumps(row, separators=(",", ":")) + "\n" for row in seed_rows]
            all_lines = itertools.chain(lines, window_file)
            if query.fmt == "json":
                writer.write('"data":[')
                for index, line in enumerate(all_lines):
                    writer.write(("," if index else "") + line.rstrip("\n"))
                writer.write("]")
            elif query.fmt == "ndjson":
                # The body is one JSON string; each line goes in escaped, without quotes.
                writer.write('"ndjson_body":"')
                for line in all_lines:
                    writer.write(json.dumps(line)[1:-1])
                writer.write('"')
            else:
                writer.write('"csv_body":"')
                buffer = io.StringIO()
                csv_writer = csv.DictWriter(buffer, fieldnames=query.columns)
                csv_writer.writeheader()
                for line in all_lines:
                    csv_writer.writerow(json.loads(line))
                    writer.write(json.dumps(buffer.getvalue())[1:-1])
                    buffer.seek(0)
                    buffer.truncate()
                writer.write(json.dumps(buffer.getvalue())[1:-1])
                writer.write('"')
            writer.write(
                f',"manifest":{json.dumps(manifest, separators=(",", ":"))},'
                f'"annotations":{json.dumps(annotations, separators=(",", ":"))}}}'
            )
            writer.flush()
            out_file.close()
            self._store_manifest(export_id, manifest, manifest_hash)
            return SpoolResult(
                path=out_path,
                fmt=query.fmt,
                content_type="application/json",
                row_count=row_count,
                content_length=writer.bytes_written,
                export_id=export_id,
                manifest_hash=manifest_hash,
                manifest=manifest,
            )
        except Exception:
            out_file.close()
            out_path.unlink(missing_ok=True)
            raise
        finally:
            window_file.close()

    def execute_spooled_query(
        self,
        request_body: Any,
        *,
        request_id: str = "unknown",
        requester_id: str = "anonymous",
    ) -> SpoolResult:
        query = validate_query_request(request_body, self.config.limits)
        return self.execute_spooled_query_from_query(
            query,
            request_id=request_id,
            requester_id=requester_id,
        )

    def execute_spooled_query_from_query(
        self,
        query: SignalsQuery,
        *,
        request_id: str = "unknown",
        requester_id: str = "anonymous",
    ) -> SpoolResult:
        self.enforce_scope(query)

        flux_query = build_flux_query(query, self.config.influx.bucket)
        response = influx_query_csv_stream(self.config, flux_query)

        suffix = SUFFIX_BY_FORMAT.get(query.fmt)
        content_type = CONTENT_TYPE_BY_FORMAT.get(query.fmt)
        if suffix is None or content_type is None:
            response.close()
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", f"unsupported format: {query.fmt}")

        tmp_file = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            delete=False,
            prefix="anolis_export_",
            suffix=suffix,
        )
        tmp_path = Path(tmp_file.name)
        limits = self.config.limits
        if query.fmt == "json":
            bounded_writer = _BoundedTextWriter(tmp_file, limits.max_response_bytes, "max_response_bytes")
        else:
            bounded_writer = _BoundedTextWriter(tmp_file, limits.max_stream_bytes, "max_stream_bytes")
        export_id = str(uuid.uuid4())
        row_count = 0
        content_length = 0

        try:
            if query.fmt == "csv":
                csv_writer = csv.DictWriter(bounded_writer, fieldnames=query.columns)
                csv_writer.writeheader()
            elif query.fmt == "json":
                bounded_writer.write('{"status":"ok","dataset":"signals","format":"json","data":[')
                json_first = True
            else:
                json_first = False

            try:
                for raw_row in iter_influx_csv_rows(response):
                    row_count += 1
                    if row_count > self.config.limits.max_rows:
                        raise ApiError(
                            HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                            "limit_exceeded",
                            f"row count exceeds max_rows={self.config.limits.max_rows}",
                        )

                    normalized = normalize_row(raw_row, query.columns)

                    if query.fmt == "csv":
                        csv_writer.writerow(normalized)
                    elif query.fmt == "json":
                        if not json_first:
                            bounded_writer.write(",")
                        bounded_writer.write(json.dumps(normalized, separators=(",", ":")))
                        json_first = False
                    else:
                        bounded_writer.write(json.dumps(normalized, separators=(",", ":")))
                        bounded_writer.write("\n")
            except UnicodeDecodeError as exc:
                content_encoding = ""
                headers = getattr(response, "headers", None)
                if headers is not None:
                    try:
                        content_encoding = str(headers.get("Content-Encoding", "")).strip()
                    except Exception:
                        content_encoding = ""
                LOGGER.exception(
                    "request_id=%s stream decode failure format=%s content_encoding=%s",
                    request_id,
                    query.fmt,
                    content_encoding or "<none>",
                )
                raise ApiError(
                    HTTPStatus.BAD_GATEWAY,
                    "upstream_error",
                    f"failed to decode Influx CSV stream as UTF-8 (content_encoding={content_encoding or 'unknown'})",
                ) from exc

            manifest = build_manifest(
                query,
                self.config,
                row_count=row_count,
                export_id=export_id,
                request_id=request_id,
                requester_id=requester_id,
            )
            manifest_hash = compute_manifest_hash(manifest)
            self._store_manifest(export_id, manifest, manifest_hash)

            if query.fmt == "json":
                bounded_writer.write('],"manifest":')
                bounded_writer.write(json.dumps(manifest, separators=(",", ":")))
                bounded_writer.write("}")
            bounded_writer.flush()
            content_length = bounded_writer.bytes_written

            tmp_file.close()
            response.close()

            return SpoolResult(
                path=tmp_path,
                fmt=query.fmt,
                content_type=content_type,
                row_count=row_count,
                content_length=content_length,
                export_id=export_id,
                manifest_hash=manifest_hash,
                manifest=manifest,
            )
        except Exception:
            try:
                tmp_file.close()
            except Exception:
                pass
            try:
                response.close()
            except Exception:
                pass
            try:
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            raise

    # Backward-compatible aliases kept for existing tests/callers.
    def execute_csv_spooled_query(
        self,
        request_body: Any,
        *,
        request_id: str = "unknown",
        requester_id: str = "anonymous",
    ) -> SpoolResult:
        result = self.execute_spooled_query(
            request_body,
            request_id=request_id,
            requester_id=requester_id,
        )
        if result.fmt != "csv":
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", "format must be csv")
        return result

    def execute_csv_spooled_query_from_query(
        self,
        query: SignalsQuery,
        *,
        request_id: str = "unknown",
        requester_id: str = "anonymous",
    ) -> SpoolResult:
        result = self.execute_spooled_query_from_query(
            query,
            request_id=request_id,
            requester_id=requester_id,
        )
        if result.fmt != "csv":
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", "format must be csv")
        return result


class ExportRequestHandler(BaseHTTPRequestHandler):
    """HTTP request adapter for ExportService."""

    service: ExportService

    def log_message(self, format_str: str, *args: Any) -> None:
        LOGGER.info("%s - %s", self.address_string(), format_str % args)

    def do_GET(self) -> None:
        request_id = coerce_request_id(self.headers.get("X-Request-Id"))

        if self.path == "/v1/health":
            self.send_json(HTTPStatus.OK, {"status": "ok"}, request_id=request_id)
            return

        prefix = "/v1/exports/manifests/"
        if self.path.startswith(prefix):
            # As do_POST does: every error is a JSON answer, never a closed socket.
            try:
                self.service.authorize(self.headers.get("Authorization"))
                export_id = self.path[len(prefix) :].strip()
                if not export_id:
                    raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", "export_id is required")
                manifest = self.service.get_manifest(export_id)
                if manifest is None:
                    raise ApiError(HTTPStatus.NOT_FOUND, "not_found", f"manifest not found for export_id={export_id}")
            except ApiError as exc:
                self.send_json(exc.status, json_error_payload(exc.code, exc.message), request_id=request_id)
                return
            except Exception as exc:
                LOGGER.exception("request_id=%s unhandled export service error", request_id)
                self.send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    json_error_payload("internal", f"unexpected error: {exc}"),
                    request_id=request_id,
                )
                return
            self.send_json(
                HTTPStatus.OK,
                {"status": "ok", "export_id": export_id, "manifest": manifest},
                request_id=request_id,
            )
            return

        self.send_json(
            HTTPStatus.NOT_FOUND,
            json_error_payload("not_found", "route not found"),
            request_id=request_id,
        )

    def do_POST(self) -> None:
        request_id = coerce_request_id(self.headers.get("X-Request-Id"))
        requester_id = coerce_requester_id(self.headers.get("X-Requester-Id"))

        LOGGER.info("request_id=%s method=POST path=%s requester=%s", request_id, self.path, requester_id)

        if self.path not in ("/v1/exports/signals:query", "/v1/exports/runs:export"):
            self.send_json(
                HTTPStatus.NOT_FOUND,
                json_error_payload("not_found", "route not found"),
                request_id=request_id,
            )
            return

        try:
            self.service.authorize(self.headers.get("Authorization"))
            body = self.read_json_body(self.service.config.limits.max_request_bytes)

            if self.path == "/v1/exports/runs:export":
                result = self.service.execute_run_export(
                    body,
                    request_id=request_id,
                    requester_id=requester_id,
                )
            else:
                query = validate_query_request(body, self.service.config.limits)
                result = self.service.execute_spooled_query_from_query(
                    query,
                    request_id=request_id,
                    requester_id=requester_id,
                )
            try:
                self.send_file_response(
                    HTTPStatus.OK,
                    export_path=result.path,
                    content_length=result.content_length,
                    content_type=result.content_type,
                    export_id=result.export_id,
                    manifest_hash=result.manifest_hash,
                    request_id=request_id,
                    requester_id=requester_id,
                )
            finally:
                result.path.unlink(missing_ok=True)
        except ApiError as exc:
            LOGGER.warning(
                "request_id=%s api_error status=%s code=%s message=%s",
                request_id,
                int(exc.status),
                exc.code,
                exc.message,
            )
            self.send_json(exc.status, json_error_payload(exc.code, exc.message), request_id=request_id)
        except Exception as exc:  # pragma: no cover - defensive fallback
            LOGGER.exception("request_id=%s unhandled export service error", request_id)
            self.send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                json_error_payload("internal", f"unexpected error: {exc}"),
                request_id=request_id,
            )

    def read_json_body(self, max_request_bytes: int) -> Any:
        content_length_raw = self.headers.get("Content-Length", "")
        if not content_length_raw.isdigit():
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", "Content-Length header is required")

        content_length = int(content_length_raw)
        if content_length <= 0:
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", "request body is required")
        if content_length > max_request_bytes:
            raise ApiError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "limit_exceeded",
                f"request body exceeds max_request_bytes={max_request_bytes}",
            )

        raw = self.rfile.read(content_length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_argument", f"invalid JSON body: {exc}") from exc

    def send_json(self, status: int | HTTPStatus, payload: dict[str, Any], *, request_id: str) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Request-Id", request_id)
        self.end_headers()
        self.wfile.write(body)

    def send_file_response(
        self,
        status: int | HTTPStatus,
        *,
        export_path: Path,
        content_length: int,
        content_type: str,
        export_id: str,
        manifest_hash: str,
        request_id: str,
        requester_id: str,
    ) -> None:
        self.send_response(int(status))
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(content_length))
        self.send_header("X-Request-Id", request_id)
        self.send_header("X-Requester-Id", requester_id)
        self.send_header("X-Export-Id", export_id)
        self.send_header("X-Export-Manifest-Hash", manifest_hash)
        self.end_headers()
        with export_path.open("rb") as handle:
            while True:
                chunk = handle.read(64 * 1024)
                if not chunk:
                    break
                self.wfile.write(chunk)


def run_server(config: AppConfig) -> None:
    handler_cls = ExportRequestHandler
    handler_cls.service = ExportService(config)

    server = ThreadingHTTPServer((config.server.host, config.server.port), handler_cls)
    LOGGER.info("Telemetry export service listening on %s:%d", config.server.host, config.server.port)

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        LOGGER.info("Shutdown requested")
    finally:
        server.server_close()
        LOGGER.info("Telemetry export service stopped")


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="[%(asctime)s] [%(levelname)s] %(message)s",
    )

    config = load_config(Path(args.config))
    run_server(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
