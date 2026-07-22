"""Callback-free handlers over the current DATA_ROOT implementation.

These adapters provide a migration bridge.  They call operation-level
functions only; filesystem job state and callback delivery remain owned by the
existing FastAPI adapter and are intentionally not used here.
"""

from __future__ import annotations

import hashlib
import mimetypes
from pathlib import Path
from typing import Any, Mapping

from ..contracts import ArtifactDescriptor, JobEnvelope, JobType
from ..errors import ResourceLimitExceeded
from ..runtime import ExecutionContext, RuntimeServices
from .local import LocalArtifactStore


def _root(services: RuntimeServices) -> Path:
    value = services.extensions.get("legacyDataRoot")
    if value is None:
        raise ValueError("legacyDataRoot is required by the local compatibility handler")
    return Path(value).expanduser().resolve()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _descriptor(
    root: Path,
    path: str | Path,
    *,
    row_count: int | None = None,
    format_name: str | None = None,
) -> ArtifactDescriptor:
    resolved = Path(path).expanduser().resolve()
    try:
        key = resolved.relative_to(root).as_posix()
    except ValueError as exc:
        raise ValueError("legacy operation returned an artifact outside its data root") from exc
    content_type = mimetypes.guess_type(resolved.name)[0] or "application/octet-stream"
    return ArtifactDescriptor(
        store="LOCAL",
        key=key,
        version=str(resolved.stat().st_mtime_ns),
        sha256=_sha256(resolved),
        sizeBytes=resolved.stat().st_size,
        rowCount=row_count,
        contentType=content_type,
        format=(format_name or resolved.suffix.lstrip(".")).upper() or "OTHER",
    )


def _public_metrics(result: Mapping[str, Any]) -> dict[str, Any]:
    path_fields = {"filePath", "filePaths", "manifestPath", "resultManifestPath"}
    return {
        key: value
        for key, value in result.items()
        if key not in path_fields and not key.startswith("_") and key != "artifact"
    }


def _extract_handler(
    envelope: JobEnvelope,
    context: ExecutionContext,
    services: RuntimeServices,
) -> dict[str, Any]:
    # Lazy imports keep the stable package import independent from legacy job
    # state and callback modules.
    from ..extract import execute_extract

    command = envelope.command
    connection_id = str(command.get("connectionId") or "").strip()
    request = command.get("request")
    if not isinstance(request, dict):
        raise ValueError("EXTRACT command.request must be an object")
    request = {**request, "jobId": str(envelope.job_id)}
    context.cancellation.raise_if_cancelled()
    result = execute_extract(
        connection_id,
        request,
        _root(services),
        str(envelope.job_id),
        budget=context.budget or envelope.resource_budget,
    )
    context.cancellation.raise_if_cancelled()
    output_path = result.get("filePath")
    artifacts = () if not output_path else (
        _descriptor(
            _root(services),
            output_path,
            row_count=result.get("rowCount"),
            format_name=result.get("outputFormat"),
        ),
    )
    return {"artifacts": artifacts, "metrics": _public_metrics(result)}


def _metadata_refresh_handler(
    envelope: JobEnvelope,
    context: ExecutionContext,
    services: RuntimeServices,
) -> dict[str, Any]:
    from ..refresh import refresh_tables_impl

    command = envelope.command
    connection_id = str(command.get("connectionId") or "").strip()
    request = command.get("request")
    if not isinstance(request, dict):
        raise ValueError("METADATA_REFRESH command.request must be an object")
    source_connection = request.get("sourceConnection")
    if not isinstance(source_connection, Mapping):
        raise ValueError("METADATA_REFRESH request.sourceConnection must be an object")
    if source_connection.get("username") or source_connection.get("password"):
        raise ValueError("METADATA_REFRESH queue commands must not contain inline credentials")
    secret_ref = str(command.get("secretRef") or "").strip()
    if not secret_ref:
        raise ValueError("METADATA_REFRESH command.secretRef is required")
    if services.secret_provider is None:
        raise ValueError("METADATA_REFRESH requires a secret provider")
    context.cancellation.raise_if_cancelled()
    username = ""
    password = ""
    try:
        with services.secret_provider.resolve(secret_ref, purpose="METADATA_REFRESH") as secret:
            username = str(secret.get("username") or "").strip()
            password = str(secret.get("password") or "")
            if not username or not password:
                raise ValueError("METADATA_REFRESH secret must provide username and password")
            execution_request = {
                **request,
                "sourceConnection": {
                    **source_connection,
                    "username": username,
                    "password": password,
                },
            }
            result = refresh_tables_impl(
                connection_id,
                execution_request,
                _root(services),
                str(envelope.job_id),
                context.budget or envelope.resource_budget,
            )
    except Exception as exc:
        from ..refresh import REDACTED, sanitize_refresh_text

        safe_message = sanitize_refresh_text(exc)
        for sensitive_value in (username, password):
            if sensitive_value:
                safe_message = safe_message.replace(sensitive_value, REDACTED)
        raise RuntimeError(safe_message) from None
    context.cancellation.raise_if_cancelled()
    artifacts = []
    for item in result.get("tables") or []:
        relative = item.get("path")
        if relative:
            artifacts.append(
                _descriptor(
                    _root(services),
                    _root(services) / "connections" / connection_id / relative,
                    row_count=item.get("rowCount"),
                    format_name="PARQUET",
                )
            )
    metrics = _public_metrics(result)
    metrics["tables"] = [
        {
            key: item.get(key)
            for key in ("tableId", "schemaName", "tableName", "path", "rowCount")
            if item.get(key) is not None
        }
        for item in result.get("tables") or []
    ]
    return {"artifacts": artifacts, "metrics": metrics}


def _dataset_convert_handler(
    envelope: JobEnvelope,
    context: ExecutionContext,
    services: RuntimeServices,
) -> dict[str, Any]:
    from ..user_dataset import convert_user_dataset_file_from_path, dataset_file_parquet_path

    command = envelope.command
    request = command.get("request")
    if not isinstance(request, dict):
        raise ValueError("DATASET_CONVERT command.request must be an object")
    request = {**request, "jobId": str(envelope.job_id)}
    raw_input = command.get("input")
    if not isinstance(raw_input, Mapping):
        raise ValueError("DATASET_CONVERT command.input must be an ArtifactDescriptor object")
    if services.artifact_store is None:
        raise ValueError("DATASET_CONVERT requires an artifact store to materialize command.input")
    from ..execution_scope import current_execution_resources

    resources = current_execution_resources()
    if resources is None:
        raise RuntimeError("DATASET_CONVERT handler requires an active execution workspace")
    descriptor = ArtifactDescriptor.model_validate(raw_input)
    budget = context.budget or envelope.resource_budget
    if budget.input_bytes is not None and descriptor.size_bytes > budget.input_bytes:
        raise ResourceLimitExceeded(
            "DATASET_CONVERT input artifact exceeds resourceBudget.inputBytes"
        )
    context.cancellation.raise_if_cancelled()
    with services.artifact_store.materialize(
        descriptor,
        resources.temp_root / "dataset-convert-input",
    ) as input_path:
        context.cancellation.raise_if_cancelled()
        result = convert_user_dataset_file_from_path(
            Path(input_path),
            _root(services),
            request,
            budget=budget,
            workspace=resources.temp_root / "dataset-convert",
        )
    context.cancellation.raise_if_cancelled()
    output = dataset_file_parquet_path(
        _root(services),
        result["userId"],
        result["userDatasetId"],
        result["userDatasetFileId"],
    )
    return {
        "artifacts": (
            _descriptor(
                _root(services),
                output,
                row_count=result.get("rowCount"),
                format_name="PARQUET",
            ),
        ),
        "metrics": _public_metrics(result),
    }


def _analysis_artifact_handler(
    envelope: JobEnvelope,
    context: ExecutionContext,
    services: RuntimeServices,
) -> dict[str, Any]:
    from ..analytics_artifacts import (
        artifact_relative_path,
        render_analysis_artifact_operation,
    )
    from ..analytics_models import AnalyticsArtifactRequest
    from ..execution_scope import current_execution_resources

    command = envelope.command
    raw_request = command.get("request")
    if not isinstance(raw_request, dict):
        raise ValueError("ANALYSIS_ARTIFACT command.request must be an object")
    supplied_job_id = raw_request.get("jobId")
    if supplied_job_id is not None and str(supplied_job_id) != str(envelope.job_id):
        raise ValueError("ANALYSIS_ARTIFACT request.jobId must match JobEnvelope jobId")
    # Queue execution never inherits an HTTP callback from an embedded legacy
    # request.  The queue host owns all result delivery around CdwEngine.
    request = AnalyticsArtifactRequest.model_validate(
        {
            **raw_request,
            "jobId": str(envelope.job_id),
            "callback": None,
        }
    )
    resources = current_execution_resources()
    if resources is None:
        raise RuntimeError("ANALYSIS_ARTIFACT handler requires an active execution workspace")
    context.cancellation.raise_if_cancelled()
    rendered = render_analysis_artifact_operation(
        request,
        _root(services),
        resources.temp_root / "analysis-artifact",
        cancellation=context.cancellation,
        check_tombstone=False,
    )
    context.cancellation.raise_if_cancelled()

    target = command.get("target")
    if target is not None and not isinstance(target, Mapping):
        raise ValueError("ANALYSIS_ARTIFACT command.target must be an object")
    if target:
        requested_store = str(target.get("store") or "LOCAL").strip().upper()
        if requested_store != "LOCAL":
            raise ValueError("legacy ANALYSIS_ARTIFACT adapter supports only target.store=LOCAL")
        key = str(target.get("key") or "").strip()
        if not key:
            raise ValueError("ANALYSIS_ARTIFACT command.target.key is required")
    else:
        key = artifact_relative_path(
            request.user_id,
            request.analysis_artifact_id,
            str(rendered["fileName"]),
        )
    if services.artifact_store is None:
        raise ValueError("ANALYSIS_ARTIFACT requires an artifact store")
    rendered_path = Path(rendered["filePath"])
    budget = context.budget or envelope.resource_budget
    if budget.output_bytes is not None and rendered_path.stat().st_size > budget.output_bytes:
        raise ResourceLimitExceeded(
            "ANALYSIS_ARTIFACT output exceeds resourceBudget.outputBytes"
        )
    descriptor = services.artifact_store.publish(
        rendered_path,
        key,
        idempotency_key=envelope.idempotency_key,
    )
    context.cancellation.raise_if_cancelled()
    return {
        "artifacts": (descriptor,),
        "metrics": {
            "analysisArtifactId": request.analysis_artifact_id,
            "analysisId": request.analysis_id,
            "userId": request.user_id,
            **_public_metrics(rendered),
        },
    }


def legacy_runtime_services(
    data_root: str | Path,
    *,
    workspace_root: str | Path | None = None,
) -> RuntimeServices:
    root = Path(data_root).expanduser().resolve()
    return RuntimeServices(
        handlers={
            JobType.EXTRACT: _extract_handler,
            JobType.METADATA_REFRESH: _metadata_refresh_handler,
            JobType.DATASET_CONVERT: _dataset_convert_handler,
            JobType.ANALYSIS_ARTIFACT: _analysis_artifact_handler,
        },
        artifact_store=LocalArtifactStore(root),
        workspace_root=Path(workspace_root).expanduser().resolve() if workspace_root else root / "_tmp" / "engine",
        extensions={"legacyDataRoot": root},
    )
