"""
ingestion_entrypoint.py — Fargate/Batch task entrypoint for INGESTION activity.

Triggered by: Activity Management → Step Functions → Fargate/Batch

Flow: Tenant S3 → Download BAK → RESTORE → Read SQL → Write to OUR Iceberg

Credential Handling:
    ECS FARGATE: TaskRoleArn assigned by Step Functions. Task runs with tenant credentials.
                 S3Uploader uses default boto3 credentials (no STS AssumeRole needed).
                 
    AWS BATCH:   Job role is fixed. Step Functions passes TENANT_ROLE_ARN env var.
                 S3Uploader uses STS AssumeRole with that role ARN.

This is the reverse of entrypoint.py (extraction) — it receives data FROM
a tenant's S3 bucket and ingests it INTO our Iceberg data lake.
"""

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class IngestionInput:
    """Input payload for ingestion activity (from Activity Management)."""
    
    # Tenant & activity identification
    tenant_id: str
    activity_id: str  # Activity Management activity ID
    run_id: str
    
    # Source: Tenant's S3 bucket with their BAK/MDF file
    source_s3_bucket: str  # Tenant's S3 bucket
    source_s3_key: str     # Path to BAK file in tenant bucket
    artifact_type: str = "bak"  # "bak" | "mdf"
    checksum: Optional[str] = None  # Expected SHA256 for verification
    
    # Cross-account access
    source_role_arn: Optional[str] = None  # IAM role to assume for tenant bucket
    
    # Target: Our Iceberg data lake
    target_namespace: str = "default"  # Glue namespace to write to
    tables: List[str] = field(default_factory=list)  # Tables to ingest (empty = all)
    create_if_not_exists: bool = False  # Deprecated: destination tables are NEVER created during ingestion; tables absent from destination are skipped
    
    # Write mode
    write_mode: str = "append"  # "append" | "overwrite"
    
    # Activity Management callback
    callback_url: str = ""


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


def parse_input() -> IngestionInput:
    """Parse ingestion input from environment or file injected by Step Functions."""
    
    # Check for input file first (Step Functions container overrides)
    input_file = os.getenv("INGESTION_INPUT_FILE", "/tmp/ingestion_input.json")
    
    if os.path.exists(input_file):
        logger.info("Reading input from file: %s", input_file)
        with open(input_file, "r") as f:
            payload = json.load(f)
    else:
        # Fall back to environment variable
        input_json = os.getenv("INGESTION_INPUT")
        if input_json:
            payload = json.loads(input_json)
        else:
            # Fall back to individual environment variables
            payload = {
                "tenant_id": os.environ["TENANT_ID"],
                "activity_id": os.environ["ACTIVITY_ID"],
                "run_id": os.environ.get("RUN_ID"),  # Optional, auto-generated if missing
                "source_s3_bucket": os.environ.get("SOURCE_S3_BUCKET", os.environ.get("TENANT_BUCKET", "")),
                "source_s3_key": os.environ.get("SOURCE_S3_KEY", os.environ.get("S3_INPUT_PATH", "")),
                "artifact_type": os.getenv("ARTIFACT_TYPE", "bak"),
                "checksum": os.getenv("CHECKSUM"),
                # TENANT_ROLE_ARN from Step Functions (AWS Batch needs STS AssumeRole)
                "source_role_arn": os.getenv("TENANT_ROLE_ARN") or os.getenv("SOURCE_ROLE_ARN"),
                "target_namespace": os.getenv("TARGET_NAMESPACE", "default"),
                "tables": _parse_tables_input(os.getenv("TABLES", "")),
                "create_if_not_exists": _parse_bool(os.getenv("CREATE_IF_NOT_EXISTS", "true")),
                "write_mode": os.getenv("WRITE_MODE", "append"),
                "callback_url": os.environ.get("CALLBACK_URL", ""),
            }
    
    # run_id auto-generates if not provided
    run_id = payload.get("run_id") or str(uuid.uuid4())[:8]
    
    # Normalize source_s3_key — strip any accidental full S3 URI prefix.
    # e.g. if someone passes "s3://bucket/path/file.mdf" as the key, extract
    # just "path/file.mdf" so download_artifact doesn't double-prefix it.
    raw_bucket = payload.get("source_s3_bucket", "")
    raw_key = payload.get("source_s3_key", "")
    if raw_key.startswith("s3://"):
        # Strip scheme + bucket prefix if present
        stripped = raw_key[len("s3://"):]
        if stripped.startswith(raw_bucket + "/"):
            raw_key = stripped[len(raw_bucket) + 1:]
        elif "/" in stripped:
            raw_key = stripped.split("/", 1)[1]
        else:
            raw_key = stripped

    return IngestionInput(
        tenant_id=payload["tenant_id"],
        activity_id=payload["activity_id"],
        run_id=run_id,
        source_s3_bucket=raw_bucket,
        source_s3_key=raw_key,
        artifact_type=payload.get("artifact_type", "bak"),
        checksum=payload.get("checksum"),
        source_role_arn=payload.get("source_role_arn"),
        target_namespace=payload.get("target_namespace", "default"),
        tables=payload.get("tables") if isinstance(payload.get("tables"), list) else _parse_tables_input(str(payload.get("tables", ""))),
        create_if_not_exists=payload.get("create_if_not_exists", True) if isinstance(payload.get("create_if_not_exists"), bool) else _parse_bool(str(payload.get("create_if_not_exists", "true"))),
        write_mode=payload.get("write_mode", "append"),
        callback_url=payload.get("callback_url", ""),
    )


def main() -> None:
    """Main Fargate task entry point for ingestion."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    logger.info("Starting Fargate INGESTION task...")
    
    # 1. Parse input
    input_payload = parse_input()
    logger.info(
        "Received input: tenant=%s, run=%s, source=%s/%s",
        input_payload.tenant_id,
        input_payload.run_id,
        input_payload.source_s3_bucket,
        input_payload.source_s3_key,
    )
    
    # Import modules here to allow for easier testing
    from src.fargate.progress_reporter import ProgressReporter
    from src.fargate.artifact_restorer import ArtifactRestorer
    from src.fargate.s3_uploader import S3Uploader
    from src.core.config import load_config
    from src.core.tenant_context import init_sde_context
    from src.core.sql_reader import SqlServerReader
    from src.core.iceberg_writer import IcebergWriter
    from src.core.column_mapper import ColumnMapper
    from src.sid.transformer import SIDTransformer, create_transformer
    from src.sid.config import SID_CONFIG, get_processing_order
    
    # Initialize progress reporter
    # Writes sde/runs/{run_id}/progress.json to S3 on every report() call.
    # Your API reads that key and returns it to the UI on each poll.
    reporter = ProgressReporter(
        activity_id=input_payload.activity_id,
        callback_url=input_payload.callback_url if input_payload.callback_url else None,
        run_id=input_payload.run_id,
        s3_bucket=input_payload.source_s3_bucket,
    )

    # Initialize cleanup variables — must be defined before try so the except
    # block can safely reference them even if the error occurs before assignment.
    sql_reader = None
    restorer = None
    db_name = None
    local_artifact_path = None

    try:
        import time as _time
        task_start_ts = _time.time()

        # 2. Report: STARTED
        reporter.report(5, "STARTING")
        cfg = load_config()

        # Resolve Iceberg namespace from Tenant Context (same source as extraction path)
        tenant_context = init_sde_context(input_payload.tenant_id)
        tenant_namespace = str(tenant_context.get("glue_database", "")).strip()
        if not tenant_namespace:
            raise ValueError(
                f"Tenant context missing glue_database for tenant {input_payload.tenant_id}"
            )
        cfg.iceberg.namespace = tenant_namespace
        input_payload.target_namespace = tenant_namespace
        logger.info("Using Iceberg namespace from Tenant Context: %s", tenant_namespace)
        
        # 3. Download artifact from tenant's S3 bucket
        reporter.report(10, "DOWNLOADING")
        logger.info(
            "Downloading artifact from s3://%s/%s",
            input_payload.source_s3_bucket,
            input_payload.source_s3_key,
        )
        
        # Fargate: Uses TaskRoleArn (default credentials)
        # Batch: Uses STS AssumeRole with TENANT_ROLE_ARN
        role_arn = input_payload.source_role_arn
        if role_arn:
            logger.info("AWS Batch mode: Using STS AssumeRole with %s", role_arn)
        else:
            logger.info("ECS Fargate mode: Using TaskRoleArn credentials")
        
        uploader = S3Uploader(
            tenant_id=input_payload.tenant_id,
            role_arn=role_arn,
        )
        
        local_artifact_path = uploader.download_artifact(
            bucket=input_payload.source_s3_bucket,
            key=input_payload.source_s3_key,
            local_dir="/data/backup",
        )
        
        logger.info("Downloaded artifact to: %s", local_artifact_path)
        
        # 4. Restore database from artifact
        reporter.report(20, "RESTORING")
        
        restorer = ArtifactRestorer(cfg.sqlserver)
        restorer.connect()
        
        # Generate unique database name
        run_id_short = input_payload.run_id[:8]
        db_name = f"ingest_{input_payload.tenant_id}_{run_id_short}"
        
        if input_payload.artifact_type == "bak":
            db_name = restorer.restore_from_bak(
                bak_file=local_artifact_path,
                database_name=db_name,
                expected_checksum=input_payload.checksum,
            )
        else:
            db_name = restorer.attach_from_mdf(
                mdf_file=local_artifact_path,
                database_name=db_name,
            )
        
        # Validate restore
        validation = restorer.validate_restore(db_name)
        if not validation.get("valid"):
            raise ValueError(f"Restore validation failed: {validation.get('error')}")
        
        logger.info(
            "Database restored: %s (%d tables)",
            db_name,
            validation.get("table_count", 0),
        )
        
        # 5. Read SQL tables and write to Iceberg
        reporter.report(30, "READING")
        
        sql_reader = SqlServerReader(cfg.sqlserver, db_name)
        sql_reader.connect()
        
        ice_writer = IcebergWriter(cfg.iceberg)
        mapper = ColumnMapper(naming_convention=cfg.ingestion.naming_convention)
        
        # Determine tables to process
        if input_payload.tables:
            tables = input_payload.tables
            logger.info("Processing specific tables: %s", tables)
        else:
            tables = sql_reader.list_tables()
            logger.info("Processing all %d tables from restored database", len(tables))
        
        # Log create_if_not_exists setting
        logger.info("create_if_not_exists=%s", input_payload.create_if_not_exists)
        
        total_tables = len(tables)
        if total_tables == 0:
            logger.warning("No tables found to ingest")
        
        logger.info("Processing %d tables", total_tables)
        
        ingestion_results = []
        exposure_sets_info = []

        logger.info("SID transformation enabled - using dependency-ordered processing")
        ingestion_results, exposure_sets_info = _process_tables_with_sid(
            tables=tables,
            sql_reader=sql_reader,
            ice_writer=ice_writer,
            mapper=mapper,
            cfg=cfg,
            input_payload=input_payload,
            reporter=reporter,
        )
        
        # 6. Cleanup
        reporter.report(90, "CLEANUP")
        
        sql_reader.close()
        sql_reader = None
        
        # Drop the restored database
        if db_name:
            restorer.cleanup_database(db_name)
        
        restorer.close()
        restorer = None
        
        # Clean up downloaded artifact
        try:
            os.remove(local_artifact_path)
            logger.info("Removed downloaded artifact: %s", local_artifact_path)
        except Exception as e:
            logger.warning("Failed to remove artifact: %s", e)
        
        # 7. Build summary
        task_end_ts = _time.time()
        duration_s = round(task_end_ts - task_start_ts, 1)

        total_rows = sum(r.get("rows", 0) for r in ingestion_results)
        success_count = sum(1 for r in ingestion_results if r.get("status") == "success")
        failed_count  = sum(1 for r in ingestion_results if r.get("status") == "failed")
        skipped_count = sum(1 for r in ingestion_results if r.get("status") == "skipped")
        rows_per_sec  = round(total_rows / max(duration_s, 1), 1)

        summary = {
            "tenant_id": input_payload.tenant_id,
            "run_id": input_payload.run_id,
            "target_namespace": input_payload.target_namespace,
            "artifact_type": input_payload.artifact_type,
            "source": f"s3://{input_payload.source_s3_bucket}/{input_payload.source_s3_key}",
            # Performance
            "duration_s": duration_s,
            "rows_per_sec": rows_per_sec,
            # Table-level counts
            "tables_total": len(ingestion_results),
            "tables_success": success_count,
            "tables_failed": failed_count,
            "tables_skipped": skipped_count,
            "total_rows": total_rows,
            # ExposureSet details (populated when SID path is used)
            "exposure_sets": exposure_sets_info,
            # Per-table breakdown
            "tables": [
                {
                    "name": r["table"],
                    "status": r["status"],
                    "rows": r.get("rows", 0),
                    "elapsed_s": r.get("elapsed_s"),
                    "sid_transformed": r.get("sid_transformed", False),
                    "sid_range": r.get("sid_range"),
                    "error": r.get("error"),
                }
                for r in ingestion_results
                if r.get("status") in ("success", "failed")
            ],
        }

        # 8. Write summary JSON to S3 so Step Functions can read it back as execution output
        summary_s3_key = f"sde/runs/{input_payload.run_id}/ingestion_summary.json"
        try:
            import boto3 as _boto3
            _s3 = _boto3.client("s3")
            _s3.put_object(
                Bucket=input_payload.source_s3_bucket,
                Key=summary_s3_key,
                Body=json.dumps(summary, default=str).encode("utf-8"),
                ContentType="application/json",
            )
            summary["summary_s3_path"] = f"s3://{input_payload.source_s3_bucket}/{summary_s3_key}"
            logger.info("Summary written to s3://%s/%s", input_payload.source_s3_bucket, summary_s3_key)
        except Exception as _e:
            logger.warning("Failed to write summary to S3: %s", _e)

        # 9. Report: COMPLETED
        reporter.report(100, "COMPLETED", manifest=summary)
        reporter.flush()
        # ── Pretty summary log ──────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("INGESTION COMPLETE  run=%s  %.1fs", input_payload.run_id, duration_s)
        logger.info("  Source     : s3://%s/%s", input_payload.source_s3_bucket, input_payload.source_s3_key)
        logger.info("  Namespace  : %s", input_payload.target_namespace)
        logger.info("  Tables     : %d success / %d failed / %d skipped / %d total",
                    success_count, failed_count, skipped_count, len(ingestion_results))
        logger.info("  Rows total : %d  (%.1f rows/s)", total_rows, rows_per_sec)
        if exposure_sets_info:
            logger.info("  ExposureSets (%d):", len(exposure_sets_info))
            for es in exposure_sets_info:
                logger.info("    [SID=%s] %-40s  rows_in_source=%s  iceberg=%s.tExposureSet",
                            es.get("sid", "?"), es.get("name", "?"),
                            es.get("source_rows", "?"), input_payload.target_namespace)
        logger.info("  Per-table breakdown (success only):")
        for r in ingestion_results:
            if r.get("status") == "success":
                sid_tag = " [SID]"+("|range=%s" % r["sid_range"] if r.get("sid_range") else "") if r.get("sid_transformed") else ""
                logger.info("    %-40s  %8d rows  %5.2fs%s",
                            r["table"], r.get("rows", 0), r.get("elapsed_s") or 0, sid_tag)
        if failed_count:
            logger.info("  Failed tables:")
            for r in ingestion_results:
                if r.get("status") == "failed":
                    logger.info("    %-40s  ERROR: %s", r["table"], r.get("error", ""))
        logger.info("=" * 60)

        # Fail the task (and therefore the SFN) if any table-level errors occurred.
        # Table failures (e.g. SID 401, schema mismatch) must not be silently swallowed —
        # a non-zero exit code causes ECS to report task failure → SFN RunTask state fails.
        if failed_count > 0:
            failed_names = [r["table"] for r in ingestion_results if r.get("status") == "failed"]
            raise RuntimeError(
                f"{failed_count} table(s) failed to ingest: {', '.join(failed_names)}"
            )

    except Exception as e:
        logger.exception("Ingestion task failed.")
        reporter.report(0, "FAILED", error=str(e))
        reporter.flush()
        
        # Cleanup on failure
        if sql_reader:
            try:
                sql_reader.close()
            except Exception:
                pass
        
        if restorer:
            try:
                if db_name:
                    restorer.cleanup_database(db_name)
                restorer.close()
            except Exception:
                pass
        
        raise


def _transform_batch_columns(batch, mapper):
    """Transform batch column names using the mapper.
    
    Returns:
        pa.RecordBatch: Batch with transformed column names.
    """
    import pyarrow as pa
    
    new_names = mapper.transform_names(batch.schema)
    new_fields = [
        pa.field(new_name, f.type, nullable=f.nullable)
        for new_name, f in zip(new_names, batch.schema)
    ]
    new_schema = pa.schema(new_fields)
    
    return pa.RecordBatch.from_arrays(
        [batch.column(i) for i in range(batch.num_columns)],
        schema=new_schema,
    )


def _process_tables_with_sid(
    tables,
    sql_reader,
    ice_writer,
    mapper,
    cfg,
    input_payload,
    reporter,
):
    """Process tables WITH SID transformation.
    
    Uses dependency-ordered processing:
    1. Optionally call Exposure API to create new ExposureSet/View
    2. Read tables into PyArrow
    3. Transform SIDs in dependency order (parent tables first)
    4. Write transformed tables to Iceberg
    
    This ensures FK references are correctly updated.
    """
    import pyarrow as pa
    import time as _time
    from src.sid.transformer import create_transformer
    from src.sid.config import get_processing_order, SID_CONFIG
    
    ingestion_results = []
    exposure_sets_info = []  # [{name, sid, source_rows}]
    tables_set = set(tables)  # For fast lookup
    
    # Create SID transformer based on config
    if cfg.sid.use_mock:
        logger.info("Using MOCK SID client (starting_sid=%d)", cfg.sid.mock_starting_sid)
        sid_transformer = create_transformer(
            use_mock=True,
            starting_sid=cfg.sid.mock_starting_sid,
        )
    else:
        logger.info("Using PRODUCTION SID client (api_url=%s)", cfg.sid.api_url)
        sid_transformer = create_transformer(
            use_mock=False,
            sid_api_url=cfg.sid.api_url,
            tenant_context_lambda_name=cfg.sid.tenant_context_lambda_name,
            app_token_secret_name=cfg.sid.app_token_secret_name,
            max_retries=cfg.sid.max_retries,
            retry_backoff_base=cfg.sid.retry_backoff_base,
        )
    
    # ----------------------------------------------------------------
    # Exposure API Integration
    # EXPOSURE_API_URL is baked into the container via the task definition env var
    # (set at CDK deploy time). It is never passed through SFN input.
    # ----------------------------------------------------------------
    exposure_api_url = os.getenv("EXPOSURE_API_URL")
    if exposure_api_url:
        reporter.report(32, "EXPOSURE_API")
        logger.info("Exposure API enabled: %s", exposure_api_url)
        
        from src.sid.exposure_client import (
            create_exposure_client,
            extract_exposure_metadata,
        )
        
        # Pre-read exposure tables to extract metadata
        exposure_tables_data = {}
        for exposure_table in ["tExposureSet", "tExposureView"]:
            if exposure_table in tables_set:
                try:
                    batches = list(sql_reader.stream_table(
                        exposure_table,
                        batch_size=cfg.ingestion.batch_size,
                    ))
                    if batches:
                        exposure_tables_data[exposure_table] = pa.Table.from_batches(batches)
                        logger.info(
                            "Pre-read %s: %d rows",
                            exposure_table,
                            exposure_tables_data[exposure_table].num_rows,
                        )
                except Exception as e:
                    logger.warning("Failed to pre-read %s: %s", exposure_table, e)
        
        if exposure_tables_data:
            # Extract ExposureSet metadata from tExposureSet table in the source artifact.
            metadata = extract_exposure_metadata(
                tables_data=exposure_tables_data,
                tenant_id=input_payload.tenant_id,
                run_id=input_payload.run_id,
                source_artifact=input_payload.source_s3_key,
            )
            
            # Create Exposure API client — URL comes from task-definition env var
            use_mock_exposure = cfg.sid.use_mock if hasattr(cfg.sid, 'use_mock') else False
            exposure_client = create_exposure_client(
                api_url=exposure_api_url,
                use_mock=use_mock_exposure,
                tenant_id=input_payload.tenant_id,
                tenant_context_lambda_name=cfg.sid.tenant_context_lambda_name,
                app_token_secret_name=cfg.sid.app_token_secret_name,
            )
            
            # Call API to create new exposure entities
            exposure_response = exposure_client.create_exposure(metadata)

            # Set override SIDs on transformer — each created ExposureSet SID
            # replaces all references to old ExposureSetSIDs from the source.
            # With multiple sets the primary (first) SID is used as the blanket override;
            # extend set_override_sid mapping logic here if per-row mapping is needed.
            # How many rows are in tExposureSet source (for stats)
            exp_set_src_rows = (
                exposure_tables_data["tExposureSet"].num_rows
                if "tExposureSet" in exposure_tables_data else None
            )
            for es in exposure_response.exposure_sets:
                logger.info(
                    "Exposure API created: name=%s new_sid=%d",
                    es.name, es.sid,
                )
                exposure_sets_info.append({
                    "name": es.name,
                    "sid": es.sid,
                    "source_rows": exp_set_src_rows,
                    "iceberg_namespace": input_payload.target_namespace,
                })

            if exposure_response.exposure_set_sid:
                sid_transformer.set_override_sid(
                    "tExposureSet",
                    exposure_response.exposure_set_sid,
                )

            logger.info(
                "Exposure API complete: created %d ExposureSet(s), primary SID=%s",
                len(exposure_response.exposure_sets),
                exposure_response.exposure_set_sid,
            )
        else:
            logger.warning(
                "Exposure API URL provided but no exposure tables found in dataset"
            )
    
    # Get processing order (sorted by dependency level)
    processing_order = get_processing_order()
    processing_order = get_processing_order()
    
    # Filter to only tables we're processing
    ordered_tables = []
    for level, table_name in processing_order:
        if table_name in tables_set:
            ordered_tables.append((level, table_name))
    
    # Add any tables not in SID_CONFIG at the end (no SID transformation)
    known_tables = {t for _, t in ordered_tables}
    extra_tables = [t for t in tables if t not in known_tables]
    for table_name in extra_tables:
        ordered_tables.append((-1, table_name))  # -1 = no SID config
    
    total_tables = len(ordered_tables)
    logger.info("Processing %d tables in dependency order", total_tables)
    
    for idx, (level, table_name) in enumerate(ordered_tables):
        progress = 30 + int((idx / max(total_tables, 1)) * 50)
        
        if level >= 0:
            reporter.report(progress, f"SID TRANSFORM L{level}: {table_name}")
            logger.info("Processing table %d/%d (Level %d): %s", 
                       idx + 1, total_tables, level, table_name)
        else:
            reporter.report(progress, f"INGESTING: {table_name}")
            logger.info("Processing table %d/%d (no SID config): %s", 
                       idx + 1, total_tables, table_name)
        
        _t0 = _time.time()

        try:
            # Read from tenant's restored SQL database
            batches = list(sql_reader.stream_table(
                table_name, 
                batch_size=cfg.ingestion.batch_size,
            ))
            
            if not batches:
                logger.warning("Table %s is empty, skipping", table_name)
                ingestion_results.append({
                    "table": table_name,
                    "status": "skipped",
                    "reason": "empty",
                    "rows": 0,
                    "elapsed_s": round(_time.time() - _t0, 3),
                })
                continue
            
            # Combine batches into a single PyArrow table for SID transformation
            arrow_table = pa.Table.from_batches(batches)
            
            # Apply SID transformation if table is in config
            if level >= 0 and table_name in SID_CONFIG:
                logger.info("Transforming SIDs for %s (%d rows)", table_name, arrow_table.num_rows)
                
                transform_result = sid_transformer.transform_table(table_name, arrow_table)
                
                if transform_result.error:
                    raise ValueError(f"SID transformation failed: {transform_result.error}")
                
                # Get transformed table
                arrow_table = sid_transformer.get_transformed_table(table_name)
                
                sid_range_str = f"{transform_result.sid_range_start}-{transform_result.sid_range_end}"
                logger.info(
                    "SID transform complete: %s -> SID range [%d, %d], FKs updated: %s",
                    table_name,
                    transform_result.sid_range_start,
                    transform_result.sid_range_end,
                    transform_result.fk_columns_updated,
                )
            
            # Transform column names if needed
            target_schema = mapper.transform_schema(arrow_table.schema)
            
            # Destination table must already exist — never create tables during ingestion
            if not ice_writer.table_exists(input_payload.target_namespace, table_name):
                logger.warning(
                    "[ingestion] Table '%s.%s' does not exist in destination — skipping table",
                    input_payload.target_namespace, table_name,
                )
                ingestion_results.append({
                    "table": table_name,
                    "status": "skipped",
                    "reason": "table_not_found_in_destination",
                    "rows": 0,
                    "elapsed_s": round(_time.time() - _t0, 3),
                })
                continue
            
            # Transform column names and convert back to batches
            transformed_batches = []
            for batch in arrow_table.to_batches():
                transformed = _transform_batch_columns(batch, mapper)
                transformed_batches.append(transformed)
            
            # Write to our Iceberg lake
            if input_payload.write_mode == "overwrite":
                rows_written = ice_writer.overwrite(
                    input_payload.target_namespace,
                    table_name,
                    transformed_batches,
                )
            else:
                rows_written = ice_writer.append(
                    input_payload.target_namespace,
                    table_name,
                    transformed_batches,
                )
            
            elapsed = round(_time.time() - _t0, 3)
            logger.info("Ingested %d rows to %s.%s (%.3fs)",
                       rows_written, input_payload.target_namespace, table_name, elapsed)
            
            ingestion_results.append({
                "table": table_name,
                "status": "success",
                "rows": rows_written,
                "elapsed_s": elapsed,
                "sid_transformed": level >= 0,
                "sid_range": sid_range_str if (level >= 0 and table_name in SID_CONFIG) else None,
            })
            
        except Exception as e:
            logger.error("Failed to process table %s: %s", table_name, e)
            ingestion_results.append({
                "table": table_name,
                "status": "failed",
                "error": str(e),
                "rows": 0,
                "elapsed_s": round(_time.time() - _t0, 3),
            })
    
    return ingestion_results, exposure_sets_info


if __name__ == "__main__":
    main()
