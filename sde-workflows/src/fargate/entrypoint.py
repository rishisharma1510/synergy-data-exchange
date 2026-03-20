"""
entrypoint.py — Fargate/Batch task main entry point.

Receives input from Step Functions via environment variables or SSM parameter.
Orchestrates: init DB → load tables → validate → generate artifact → upload → report.

Credential Handling:
    ECS FARGATE: TaskRoleArn assigned by Step Functions. Task runs with tenant credentials.
                 S3Uploader uses default boto3 credentials (no STS AssumeRole needed).
                 
    AWS BATCH:   Job role is fixed. Step Functions passes TENANT_ROLE_ARN env var.
                 S3Uploader uses STS AssumeRole with that role ARN.
"""

from ees_ap_otel import init_telemetry
init_telemetry()

import json
import logging
import os
import re as _re
import uuid
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)


def _parse_tables_input(tables_raw: str) -> List[str]:
    """
    Parse TABLES environment variable.
    
    Valid inputs:
    - "all" or empty string or not set -> [] (meaning process all tables)
    - JSON array: '["TContract", "TExposure"]' -> ["TContract", "TExposure"]
    
    Returns:
        List of table names, or empty list for "all tables".
    """
    if not tables_raw or tables_raw.lower().strip() in ('all', '"all"', "'all'"):
        return []
    try:
        parsed = json.loads(tables_raw)
        if isinstance(parsed, list):
            return parsed
        elif isinstance(parsed, str) and parsed.lower() == 'all':
            return []
        else:
            logger.warning("TABLES must be 'all' or a JSON array, got: %s. Using all tables.", type(parsed))
            return []
    except json.JSONDecodeError:
        # Treat as comma-separated list
        return [t.strip() for t in tables_raw.split(',') if t.strip()]


def _parse_bool(value: str, default: bool = True) -> bool:
    """Parse a boolean from string (handles 'true', 'false', '1', '0', etc.)."""
    if not value:
        return default
    return value.lower().strip() in ('true', '1', 'yes', 'on')


def is_batch_mode() -> bool:
    """Detect if running in AWS Batch (needs STS AssumeRole for tenant credentials)."""
    # In Batch mode, TENANT_ROLE_ARN is set and we need to assume it.
    # In Fargate mode, TaskRoleArn is assigned by Step Functions (no env var).
    return bool(os.environ.get("TENANT_ROLE_ARN"))


def get_tenant_role_arn() -> Optional[str]:
    """Get tenant role ARN for AWS Batch STS AssumeRole, or None for Fargate."""
    return os.environ.get("TENANT_ROLE_ARN") if is_batch_mode() else None


# ---------------------------------------------------------------------------
# Structured JSON logging
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    """
    Emits each log record as a single-line JSON object.
    Fields: timestamp, level, logger, tenant_id, activity_id, message.
    The legacy '[tenant=... activity=...]' prefix is stripped from the
    message field so the data only lives in the structured fields.
    """
    _CTX_RE = _re.compile(r"^\[tenant=[^\]]*\]\s*")

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "logger": record.name,
            "tenant_id": getattr(record, "tenant_id", ""),
            "activity_id": getattr(record, "activity_id", ""),
            "message": self._CTX_RE.sub("", record.getMessage()).strip(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


class _ContextFilter(logging.Filter):
    """Injects tenant_id / activity_id into every log record produced by any logger."""

    def __init__(self) -> None:
        super().__init__()
        self.tenant_id: str = ""
        self.activity_id: str = ""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        record.tenant_id = self.tenant_id
        record.activity_id = self.activity_id
        return True


@dataclass
class FargateInput:
    """Input payload from Step Functions."""
    tenant_id: str
    exposure_ids: list[int]
    artifact_type: str  # "bak" | "mdf" | "both"
    activity_id: str
    tenant_bucket: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])  # Auto-generate if not provided
    tables: List[str] = None  # Tables to extract (empty/None = all)
    config_override_path: Optional[str] = None
    s3_output_path: Optional[str] = None  # S3 key prefix for final MDF/BAK upload (from S3outputpath input)
    callback_url: Optional[str] = None  # HTTP progress callback URL
    
    def __post_init__(self):
        if self.tables is None:
            self.tables = []


def parse_input() -> FargateInput:
    """Parse input from environment variables or file injected by Step Functions."""
    # Check for input file first (Step Functions container overrides)
    input_file = os.getenv("SFN_INPUT_FILE", "/tmp/sfn_input.json")
    
    if os.path.exists(input_file):
        with open(input_file, "r") as f:
            payload = json.load(f)
    else:
        # Fall back to environment variables
        payload = {
            "tenant_id": os.environ["TENANT_ID"],
            "run_id": os.environ.get("RUN_ID"),  # Optional, auto-generated if missing
            "exposure_ids": json.loads(os.environ.get("EXPOSURE_IDS", "[]")),
            "artifact_type": os.environ.get("ARTIFACT_TYPE", "bak"),
            "activity_id": os.environ["ACTIVITY_ID"],
            "tenant_bucket": os.environ.get("TENANT_BUCKET", ""),
            "callback_url": os.environ.get("CALLBACK_URL"),
            "tables": _parse_tables_input(os.getenv("TABLES", "")),
            "config_override_path": os.environ.get("CONFIG_OVERRIDE_PATH"),
            "s3_output_path": os.environ.get("S3_OUTPUT_PATH"),
        }
    
    # S3outputpath is the canonical field name from the upstream system for final MDF/BAK output.
    s3_output_path = payload.get("S3outputpath") or payload.get("s3_output_path")

    # run_id auto-generates if not provided
    run_id = payload.get("run_id") or str(uuid.uuid4())[:8]

    return FargateInput(
        tenant_id=payload["tenant_id"],
        exposure_ids=payload.get("exposure_ids", []),
        artifact_type=payload.get("artifact_type", "bak"),
        activity_id=payload["activity_id"],
        tenant_bucket=payload.get("tenant_bucket", ""),
        run_id=run_id,
        tables=payload.get("tables") if isinstance(payload.get("tables"), list) else _parse_tables_input(str(payload.get("tables", ""))),
        config_override_path=payload.get("config_override_path"),
        s3_output_path=s3_output_path,
        callback_url=payload.get("callback_url") or None,
    )


def main() -> None:
    """Main Fargate task entry point."""
    import time as _time

    task_start = _time.perf_counter()

    # Wire up JSON structured logging (replaces basicConfig plain-text format).
    # The filter is attached to the HANDLER (not the logger) so that it fires
    # for every record that reaches the handler — including records propagated
    # from child loggers in other modules (sql_writer, iceberg_reader, etc.).
    # Logger-level filters are NOT called for propagated records, but handler-
    # level filters always are.
    _ctx_filter = _ContextFilter()
    _handler = logging.StreamHandler()
    _handler.setFormatter(_JsonFormatter())
    _handler.addFilter(_ctx_filter)          # <-- on handler, covers all modules
    _root_logger = logging.getLogger()
    _root_logger.setLevel(logging.INFO)
    # Preserve any OTel LoggingHandlers added by init_telemetry() before clearing.
    # Without this, _root_logger.handlers.clear() would silently discard the OTel
    # handler and no logs would ever reach Dynatrace.
    from opentelemetry.sdk._logs import LoggingHandler as _OtelLogHandler
    _otel_handlers = [h for h in _root_logger.handlers if isinstance(h, _OtelLogHandler)]
    _root_logger.handlers.clear()
    _root_logger.addHandler(_handler)
    for _h in _otel_handlers:
        _root_logger.addHandler(_h)

    logger.info("Starting Fargate extraction task...")

    # 1. Parse input
    input_payload = parse_input()

    # Wrap the entire pipeline in a root OTel span so the service appears in
    # Dynatrace Distributed Traces / Service view even before botocore spans
    # propagate up. The span is ended in the finally block below.
    from opentelemetry import trace as _otel_trace, context as _otel_context
    _tracer = _otel_trace.get_tracer("sde-express")
    _root_span = _tracer.start_span(
        "extraction-pipeline",
        attributes={
            "run.id": input_payload.run_id,
            "tenant.id": input_payload.tenant_id,
            "artifact.type": input_payload.artifact_type,
            "activity.id": input_payload.activity_id,
        },
    )
    _root_ctx_token = _otel_context.attach(_otel_trace.set_span_in_context(_root_span))

    # Inject full tenant_id + activity_id into every subsequent log record
    _ctx_filter.tenant_id = input_payload.tenant_id
    _ctx_filter.activity_id = input_payload.activity_id

    ctx = f"[tenant={input_payload.tenant_id} activity={input_payload.activity_id}]"
    logger.info(
        "%s Input parsed: run=%s exposures=%s artifact=%s bucket=%s",
        ctx,
        input_payload.run_id,
        input_payload.exposure_ids,
        input_payload.artifact_type,
        input_payload.tenant_bucket,
    )

    # Import modules here to allow for easier testing
    from src.fargate.progress_reporter import ProgressReporter
    from src.fargate.database_manager import DatabaseManager
    from src.fargate.artifact_generator import ArtifactGenerator
    from src.fargate.s3_uploader import S3Uploader
    from src.fargate.validation import Validator
    from src.core.config import load_config
    from src.core.iceberg_reader import IcebergReader
    from src.core.ingest import parallel_load_tables
    from src.models.manifest import build_manifest

    # Writes sde/runs/{run_id}/progress.json to S3 on every report() call.
    reporter = ProgressReporter(
        activity_id=input_payload.activity_id,
        run_id=input_payload.run_id,
        s3_bucket=input_payload.tenant_bucket,
        callback_url=input_payload.callback_url,
    )

    try:
        # 2. Report: STARTED
        reporter.report(10, "PROVISIONING")
        t0 = _time.perf_counter()
        cfg = load_config()
        logger.info("%s Config loaded in %.2fs", ctx, _time.perf_counter() - t0)

        # 3. Create database
        logger.info("%s [STEP 1/7] Creating SQL Server database...", ctx)
        t0 = _time.perf_counter()
        db_mgr = DatabaseManager(cfg.sqlserver)
        db_name = db_mgr.create_database(input_payload.tenant_id, input_payload.run_id)
        logger.info("%s [STEP 1/7] Database '%s' created in %.2fs", ctx, db_name, _time.perf_counter() - t0)

        # 4. Load mapping overrides (optional — from S3)
        overrides = {}
        if input_payload.config_override_path:
            pass

        # 5. Discover tables + build execution plan
        from src.models.execution_plan import build_execution_plan as _build_execution_plan
        logger.info("%s [STEP 2/7] Initialising Iceberg catalog and listing tables...", ctx)
        t0 = _time.perf_counter()
        reader = IcebergReader(cfg.iceberg)
        all_tables = reader.list_tables()
        logger.info(
            "%s [STEP 2/7] Found %d Iceberg tables in %.2fs: %s",
            ctx, len(all_tables), _time.perf_counter() - t0, all_tables,
        )

        if input_payload.tables:
            tables_to_use = [
                t for t in all_tables
                if t.split(".")[-1] in input_payload.tables or t in input_payload.tables
            ]
            logger.info("%s Filtered to %d tables (requested: %s)", ctx, len(tables_to_use), input_payload.tables)
        else:
            tables_to_use = all_tables
            logger.info("%s Using all %d tables", ctx, len(tables_to_use))

        execution_plan = _build_execution_plan(
            tables=tables_to_use,
            overrides=overrides,
            exposure_ids=input_payload.exposure_ids,
            tenant_id=input_payload.tenant_id,
            run_id=input_payload.run_id,
        )
        plan = execution_plan.tables
        logger.info(
            "%s [STEP 2/7] Execution plan ready: %d tables, exposure_ids=%s",
            ctx, len(plan), input_payload.exposure_ids,
        )

        reporter.report(20, "LOADING")

        # 6. Parallel table load
        # MAX_WORKERS is injected as env var by CDK/deploy_updates.py (derived from vCPU or MAX_PARALLEL_TABLES)
        _max_workers = int(os.environ.get('MAX_WORKERS', '4'))
        logger.info("%s [STEP 3/7] Loading tables into SQL Server (workers=%d)...", ctx, _max_workers)
        t0 = _time.perf_counter()
        results = parallel_load_tables(
            plan=plan,
            reader=reader,
            sql_config=cfg.sqlserver,
            db_name=db_name,
            exposure_ids=input_payload.exposure_ids,
            max_workers=_max_workers,
            progress_callback=reporter.report_table_progress,
        )
        total_rows = sum(r.get("rows", 0) for r in results.values())
        logger.info(
            "%s [STEP 3/7] Table load complete in %.2fs — %d tables, %d total rows",
            ctx, _time.perf_counter() - t0, len(results), total_rows,
        )
        for tbl, res in results.items():
            logger.info("%s   table=%-40s rows=%-8d status=%s elapsed=%.2fs",
                ctx, tbl, res.get("rows", 0), res.get("status"), res.get("elapsed_seconds", 0))

        reporter.report(70, "VALIDATING")

        # 7. Validate
        logger.info("%s [STEP 4/7] Validating database...", ctx)
        t0 = _time.perf_counter()
        validator = Validator(cfg.sqlserver)
        validation_report = validator.validate_database(db_name, results)
        logger.info("%s [STEP 4/7] Validation complete in %.2fs — passed=%s errors=%s",
            ctx, _time.perf_counter() - t0, validation_report.passed, validation_report.errors)

        if not validation_report.passed:
            raise ValueError(f"Validation failed: {validation_report.errors}")

        reporter.report(80, "GENERATING_ARTIFACT")

        # 8. Generate artifact
        logger.info("%s [STEP 5/7] Generating artifact (type=%s)...", ctx, input_payload.artifact_type)
        t0 = _time.perf_counter()
        artifact_gen = ArtifactGenerator(cfg.sqlserver)
        # Use /data/backup which is owned by the mssql service account (set up in
        # Dockerfile via 'chown -R mssql:root /data').  /tmp/backup is root-owned
        # (mode 755) so the mssql process cannot write BAK files to it.
        artifacts = artifact_gen.generate(
            db_name=db_name,
            artifact_type=input_payload.artifact_type,
            output_dir="/data/backup",
        )
        total_bytes = sum(a.size_bytes for a in artifacts)
        logger.info(
            "%s [STEP 5/7] Artifact generation complete in %.2fs — %d files, %.2f MB total",
            ctx, _time.perf_counter() - t0, len(artifacts), total_bytes / 1_048_576,
        )
        for a in artifacts:
            logger.info("%s   file=%-60s type=%-4s size=%.2f MB",
                ctx, a.file_path, a.file_type, a.size_bytes / 1_048_576)

        reporter.report(90, "UPLOADING")

        # 9. Upload final MDF/BAK to tenant S3 at S3outputpath/{activity_id}/
        role_arn = get_tenant_role_arn()
        credential_mode = f"Batch/STS role={role_arn}" if role_arn else "Fargate/TaskRoleArn"
        # S3outputpath from input is the base destination; activity_id is appended as a
        # sub-directory so each activity run lands in its own isolated folder.
        if not input_payload.s3_output_path:
            raise ValueError("S3outputpath is required for final output upload")
        output_prefix = (
            input_payload.s3_output_path.lstrip("/").rstrip("/")
            + "/"
            + input_payload.activity_id
            + "/"
        )
        logger.info("%s [STEP 6/7] Uploading %d artifact(s) to s3://%s/%s (creds=%s)...",
            ctx, len(artifacts), input_payload.tenant_bucket, output_prefix, credential_mode)
        t0 = _time.perf_counter()
        uploader = S3Uploader(input_payload.tenant_id, role_arn=role_arn)
        upload_result = uploader.upload_artifacts(
            artifacts=artifacts,
            tenant_bucket=input_payload.tenant_bucket,
            prefix=output_prefix,
        )
        logger.info("%s [STEP 6/7] Upload complete in %.2fs", ctx, _time.perf_counter() - t0)

        # 10. Write manifest alongside the artifacts
        manifest_key = f"{output_prefix}manifest.json"
        logger.info("%s Writing manifest to s3://%s/%s...", ctx, input_payload.tenant_bucket, manifest_key)
        manifest = build_manifest(results, artifacts, upload_result, validation_report)
        uploader.upload_manifest(manifest, input_payload.tenant_bucket, key=manifest_key)

        # 11. Report: COMPLETED  (model_dump(mode="json") ensures datetime → ISO string)
        try:
            reporter.report(100, "COMPLETED", manifest=manifest.model_dump(mode="json"))
        except Exception as report_exc:
            logger.warning("%s Progress report (100%% COMPLETED) failed — non-fatal: %s", ctx, report_exc)

        # Flush background S3 writes before cleanup so the COMPLETED status lands
        # in S3 before the container exits.
        reporter.flush()

        # 12. Cleanup
        logger.info("%s [STEP 7/7] Dropping ephemeral database '%s'...", ctx, db_name)
        t0 = _time.perf_counter()
        db_mgr.drop_database(db_name)
        logger.info("%s [STEP 7/7] Database dropped in %.2fs", ctx, _time.perf_counter() - t0)

        elapsed_total = _time.perf_counter() - task_start
        logger.info("%s *** Extraction task completed successfully in %.2fs (%.1f min) ***",
            ctx, elapsed_total, elapsed_total / 60)

    except Exception as e:
        elapsed_total = _time.perf_counter() - task_start
        logger.exception("%s Extraction task FAILED after %.2fs: %s", ctx, elapsed_total, e)
        reporter.report(0, "FAILED", error=str(e))
        reporter.flush()
        _root_span.set_status(_otel_trace.StatusCode.ERROR, str(e))
        raise
    finally:
        _root_span.end()
        _otel_context.detach(_root_ctx_token)


if __name__ == "__main__":
    main()
