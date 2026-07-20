from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import time
import uuid
from datetime import date, datetime, time as datetime_time
from decimal import Decimal
from pathlib import Path

from cdw_extract.analytics_compiler import AnalyticsCompiler, AnalyticsDetailCompiler
from cdw_extract.analytics_models import (
    AnalyticsDetailRequest,
    AnalyticsDetailResponse,
    AnalyticsQueryRequest,
    AnalyticsQueryResponse,
    ReferenceLineType,
    ResolvedReferenceLine,
)
from cdw_extract.duck import connect
from cdw_extract.query import SourceResolver


DEFAULT_MAX_SOURCE_BYTES = 3 * 1024 * 1024 * 1024


def _max_source_bytes() -> int:
    raw = os.environ.get("ANALYTICS_MAX_SOURCE_BYTES")
    if not raw:
        return DEFAULT_MAX_SOURCE_BYTES
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("ANALYTICS_MAX_SOURCE_BYTES must be an integer") from exc
    if value < 1:
        raise ValueError("ANALYTICS_MAX_SOURCE_BYTES must be positive")
    return value


def _source_version(manifest: dict, source_path: Path, request: AnalyticsQueryRequest | AnalyticsDetailRequest) -> str:
    material = {
        "sourceKind": request.source.source_kind,
        "userId": request.source.user_id,
        "userDatasetId": request.source.user_dataset_id,
        "userDatasetFileId": request.source.user_dataset_file_id,
        "metadataId": request.source.metadata_id,
        "metadataTableId": request.source.metadata_table_id,
        "sha256Checksum": manifest.get("sha256Checksum"),
        "jobId": manifest.get("jobId"),
        "requestId": manifest.get("requestId"),
        "createdAt": manifest.get("createdAt"),
        "completedAt": manifest.get("completedAt"),
        "path": manifest.get("path"),
    }
    if not material["sha256Checksum"]:
        stat = source_path.stat()
        material["sizeBytes"] = stat.st_size
        material["modifiedNs"] = stat.st_mtime_ns
    encoded = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _resolve_source(
    request: AnalyticsQueryRequest | AnalyticsDetailRequest,
    data_root: str | Path,
) -> tuple[Path, dict, str, int]:
    source = request.source
    resolver = SourceResolver(source.metadata_id or "", data_root)
    source_reference = source.model_dump(by_alias=True)
    if source.source_kind == "MTDT_TBL":
        source_reference["tableId"] = source.metadata_table_id
    source_path = resolver.table_path(source_reference).resolve()
    if any(character in source_path.as_posix() for character in "*?[]{}"):
        raise ValueError("resolved analytics source path must not contain glob pattern characters")
    resolved_root = Path(data_root).resolve()
    if not source_path.is_relative_to(resolved_root):
        raise ValueError("resolved analytics source must remain beneath DATA_ROOT")
    manifest = (resolver.connection_manifest() if source.source_kind == "MTDT_TBL" else
                resolver.user_dataset_file_manifest(source.user_id, source.user_dataset_id, source.user_dataset_file_id))
    size = source_path.stat().st_size
    return source_path, manifest, _source_version(manifest, source_path, request), size


def _source_slice(connection, source_path: Path, source_bytes: int) -> tuple[int | None, str | None]:
    maximum = _max_source_bytes()
    if source_bytes <= maximum:
        return None, None
    row = connection.execute(
        "SELECT coalesce(sum(row_group_num_rows), 0) FROM parquet_metadata(?)",
        [source_path.as_posix()],
    ).fetchone()
    total_rows = int(row[0] or 0)
    if total_rows < 1:
        raise ValueError("source Parquet contains no rows")
    selected_rows = max(1, min(total_rows, int(total_rows * maximum / source_bytes)))
    warning = (
        f"Source Parquet is larger than the {maximum}-byte analytics cap; "
        f"the first {selected_rows:,} of {total_rows:,} rows were analyzed."
    )
    return selected_rows, warning


def _json_value(value: object, warnings: list[str]) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        warning = "Non-finite numeric values were converted to null for JSON compatibility."
        if warning not in warnings:
            warnings.append(warning)
        return None
    if isinstance(value, Decimal):
        if not value.is_finite():
            warning = "Non-finite numeric values were converted to null for JSON compatibility."
            if warning not in warnings:
                warnings.append(warning)
            return None
        if value == value.to_integral_value():
            return int(value)
        converted = float(value)
        if not math.isfinite(converted):
            warning = "Numeric values outside JSON's finite range were converted to null."
            if warning not in warnings:
                warnings.append(warning)
            return None
        return converted
    if isinstance(value, (datetime, date, datetime_time)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, bytes):
        warning = "Binary chart values were converted to hexadecimal strings."
        if warning not in warnings:
            warnings.append(warning)
        return value.hex()
    warning = f"Values of type {type(value).__name__} were converted to strings."
    if warning not in warnings:
        warnings.append(warning)
    return str(value)


def _fetch_with_timeout(
    connection,
    sql: str,
    parameters: list[object],
    timeout_ms: int,
) -> tuple[list[str], list[tuple]]:
    timed_out = threading.Event()

    def interrupt() -> None:
        timed_out.set()
        connection.interrupt()

    timer = threading.Timer(timeout_ms / 1000, interrupt)
    timer.daemon = True
    timer.start()
    try:
        cursor = connection.execute(sql, parameters)
        names = [str(item[0]) for item in cursor.description]
        rows = cursor.fetchall()
        if timed_out.is_set():
            raise TimeoutError(f"analytics query exceeded timeoutMs={timeout_ms}")
        return names, rows
    except TimeoutError:
        raise
    except Exception as exc:
        if timed_out.is_set():
            raise TimeoutError(f"analytics query exceeded timeoutMs={timeout_ms}") from exc
        raise
    finally:
        timer.cancel()


def run_analytics_query(
    request: AnalyticsQueryRequest,
    data_root: str | Path,
) -> AnalyticsQueryResponse:
    started = time.perf_counter()
    source_path, _manifest, source_version, source_bytes = _resolve_source(request, data_root)
    connection = connect(data_root, "analytics", request.request_id)
    try:
        options = request.options
        connection.execute(f"SET memory_limit='{options.memory_limit_mb}MB'")
        connection.execute(f"SET threads={options.threads}")
        connection.execute("SET preserve_insertion_order=false")
        source_row_limit, source_warning = _source_slice(connection, source_path, source_bytes)
        schema_rows = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [source_path.as_posix()],
        ).fetchall()
        schema = [(str(row[0]), str(row[1])) for row in schema_rows]
        compiled = AnalyticsCompiler(request, source_path, schema, source_row_limit).compile()
        names, raw_rows = _fetch_with_timeout(
            connection,
            compiled.sql,
            compiled.parameters,
            options.timeout_ms,
        )
    finally:
        connection.close()

    if compiled.others_label and "__label_collision" in names:
        collision_index = names.index("__label_collision")
        if any(bool(row[collision_index]) for row in raw_rows):
            raise ValueError(
                f"othersLabel conflicts with an existing category value: {compiled.others_label}"
            )

    visible_indices = [index for index, name in enumerate(names) if name not in compiled.hidden_keys]
    if len(visible_indices) != len(names):
        names = [names[index] for index in visible_indices]
        raw_rows = [tuple(row[index] for index in visible_indices) for row in raw_rows]

    warnings = list(compiled.warnings)
    if source_warning:
        warnings.append(source_warning)
    result_truncated = compiled.detect_truncation and len(raw_rows) > compiled.row_limit
    if len(raw_rows) > compiled.row_limit:
        raw_rows = raw_rows[: compiled.row_limit]
        if result_truncated:
            warnings.append(
                f"Result exceeded the {compiled.row_limit}-row cap; additional rows were omitted."
            )
    rows = [
        {name: _json_value(value, warnings) for name, value in zip(names, raw_row)}
        for raw_row in raw_rows
    ]
    elapsed_ms = max(0, int(round((time.perf_counter() - started) * 1000)))
    reference_lines: list[ResolvedReferenceLine] = []
    numeric_values = [
        float(row["value"])
        for row in rows
        if isinstance(row.get("value"), (int, float)) and not isinstance(row.get("value"), bool)
    ]
    for item in request.reference_lines:
        if item.type == ReferenceLineType.AVERAGE:
            if not numeric_values:
                warnings.append(f"Reference line {item.id} was omitted because the result has no numeric values.")
                continue
            value = sum(numeric_values) / len(numeric_values)
        else:
            value = float(item.value)
        reference_lines.append(
            ResolvedReferenceLine(
                id=item.id,
                type=item.type,
                value=value,
                label=item.label,
                color=item.color,
            )
        )

    metadata: dict[str, object] = {
        "appliedFilterCount": len(request.all_filters),
        "valueTransform": request.options.value_transform.value,
    }
    if source_row_limit is not None:
        metadata["sourceTruncated"] = True
        metadata["analyzedSourceRows"] = source_row_limit
    if request.top_n and request.top_n.enabled:
        metadata["topN"] = request.top_n.model_dump(by_alias=True, mode="json")
    if request.drilldown:
        metadata["drilldown"] = {
            "level": request.drilldown.level,
            "fields": [field.model_dump(by_alias=True, mode="json") for field in request.drilldown.fields],
            "canDrillDown": request.drilldown.level + 1 < len(request.drilldown.fields),
        }
    if request.comparison and request.comparison.enabled:
        metadata["comparison"] = request.comparison.model_dump(by_alias=True, mode="json")

    return AnalyticsQueryResponse(
        requestId=request.request_id,
        chartType=request.chart_type,
        sourceVersion=source_version,
        elapsedMs=elapsed_ms,
        rowCount=len(rows),
        truncated=result_truncated or source_row_limit is not None,
        warnings=warnings,
        columns=compiled.columns,
        rows=rows,
        referenceLines=reference_lines,
        metadata=metadata,
    )


def run_analytics_detail(
    request: AnalyticsDetailRequest,
    data_root: str | Path,
) -> AnalyticsDetailResponse:
    started = time.perf_counter()
    source_path, _manifest, source_version, source_bytes = _resolve_source(request, data_root)
    connection = connect(data_root, "analytics-detail", request.request_id)
    try:
        options = request.options
        connection.execute(f"SET memory_limit='{options.memory_limit_mb}MB'")
        connection.execute(f"SET threads={options.threads}")
        connection.execute("SET preserve_insertion_order=false")
        source_row_limit, source_warning = _source_slice(connection, source_path, source_bytes)
        schema_rows = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [source_path.as_posix()]
        ).fetchall()
        schema = [(str(row[0]), str(row[1])) for row in schema_rows]
        compiled = AnalyticsDetailCompiler(request, source_path, schema, source_row_limit).compile()
        names, raw_rows = _fetch_with_timeout(
            connection,
            compiled.sql,
            compiled.parameters,
            options.timeout_ms,
        )
    finally:
        connection.close()

    has_more = len(raw_rows) > compiled.row_limit
    raw_rows = raw_rows[: compiled.row_limit]
    warnings: list[str] = []
    if source_warning:
        warnings.append(source_warning)
    rows = [
        {name: _json_value(value, warnings) for name, value in zip(names, raw_row)}
        for raw_row in raw_rows
    ]
    elapsed_ms = max(0, int(round((time.perf_counter() - started) * 1000)))
    return AnalyticsDetailResponse(
        requestId=request.request_id,
        sourceVersion=source_version,
        elapsedMs=elapsed_ms,
        offset=request.offset,
        limit=request.limit,
        rowCount=len(rows),
        hasMore=has_more,
        columns=compiled.columns,
        rows=rows,
        warnings=warnings,
    )
