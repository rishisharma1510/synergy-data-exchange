"""
spla_tracker.py — SPLA usage tracker for Synergy Data Exchange pipelines.

Triggered by two EventBridge rules:
  1. aws.states ExecutionStatusChange → records ALL runs to DynamoDB (with spla_liable flag)
  2. aws-events scheduled cron (every 5 min) → publishes ConcurrentSPLACores CW metric

DynamoDB table: sde-spla-usage
  PK: tenant_id  SK: run_id
  TTL: ttl_epoch (3 years from started_at)
  spla_liable: True  → STANDARD / BATCH paths (SQL Server Standard — incurs SPLA cost)
               False → EXPRESS path (SQL Server Express Edition — free, no SPLA obligation)

S3 report: ss-cdk{stage}-metrics-shared/{stage}/usagebyservice/SQL_SPLA/

Paths:
  - EXPRESS  → SQL Server Express (free) — tracked for usage visibility, spla_liable=False
  - STANDARD → Fargate Standard (16 vCPU = 16 SPLA cores) — spla_liable=True
  - BATCH    → AWS Batch EC2  (16 vCPU = 16 SPLA cores) — spla_liable=True
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr, Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── AWS clients ──────────────────────────────────────────────────────────────
_dynamo = boto3.resource("dynamodb")
_sfn = boto3.client("stepfunctions")
_cw = boto3.client("cloudwatch")

# ── Constants ────────────────────────────────────────────────────────────────
TABLE_NAME: str = os.environ["SPLA_TABLE_NAME"]
EXTRACTION_SM_ARN: str = os.environ["EXTRACTION_SM_ARN"]
INGESTION_SM_ARN: str = os.environ["INGESTION_SM_ARN"]
# Execution ARNs use 'execution:' instead of 'stateMachine:' so match on the SM name only
EXTRACTION_SM_NAME: str = EXTRACTION_SM_ARN.split(":")[-1]
INGESTION_SM_NAME: str = INGESTION_SM_ARN.split(":")[-1]
ENV: str = os.environ.get("ENV", "prod")
# When SqlEdition=Developer (non-prod) the CDK sets this to "false" so no SPLA records are written.
# All non-prod environments save SPLA cost by using SQL Server Developer edition (free, dev/test only).
SPLA_TRACKING_ENABLED: bool = os.environ.get("SPLA_TRACKING_ENABLED", "true").lower() == "true"
CW_NAMESPACE = "sde/SPLA"

# 3 years in seconds (TTL)
TTL_SECONDS = 3 * 365 * 24 * 3600  # 94,608,000

# SPLA-liable paths mapped to core counts (from appsettings.json)
# PATH 1 (EXPRESS) is NOT SPLA-liable — excluded from tracking
SPLA_PATH_CORES: dict[str, int] = {
    "STANDARD": 16,  # StandardCpu: 16384 → 16 vCPU = 16 SPLA cores (Fargate container VM rule)
    "BATCH": 16,     # JobVcpus: 16 → 16 SPLA cores
}

# Task state names that indicate which path was actually executed
# These are the CDK state IDs assigned in SDEStepFunctionsStack.cs
STANDARD_TASK_NAMES = {
    "RunStandardExtractionTask",
    "RunStandardIngestionTask",
}
BATCH_TASK_NAMES = {
    "RunBatchExtractionJob",
    "RunBatchIngestionJob",
}


# ── Entrypoint ───────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context: object) -> dict:
    """Route to the correct handler based on event source."""
    if not SPLA_TRACKING_ENABLED:
        logger.info(
            "SPLA tracking is disabled (SQL Server Developer edition deployed). "
            "No SPLA obligation for this environment. Skipping."
        )
        return {"status": "skipped", "reason": "spla_tracking_disabled_developer_edition"}

    source = event.get("source", "")

    if source == "aws.states":
        return _handle_sfn_state_change(event)
    elif source == "aws.events":
        # Scheduled EventBridge cron — publish concurrent cores metric
        return _publish_concurrent_cores_metric()
    else:
        logger.warning("Unknown event source: %s", source)
        return {"status": "skipped", "reason": f"unknown source: {source}"}


# ── SFN Execution State Change Handler ───────────────────────────────────────

def _handle_sfn_state_change(event: dict) -> dict:
    """Write a DynamoDB record for ALL runs that complete (any terminal state).
    
    Every run is recorded regardless of path so total SQL Server usage is visible.
    The 'spla_liable' field indicates whether the run incurs a SPLA licensing cost:
      - True  → STANDARD or BATCH path (SQL Server Standard Edition)
      - False → EXPRESS path (SQL Server Express Edition, free)
    """
    detail = event.get("detail", {})
    status = detail.get("status", "")

    # Only process terminal states — at this point GetExecutionHistory has full data
    terminal_statuses = {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}
    if status not in terminal_statuses:
        logger.debug("Skipping non-terminal status: %s", status)
        return {"status": "skipped", "reason": f"non-terminal status {status}"}

    execution_arn: str = detail["executionArn"]
    execution_name: str = detail.get("name", "")

    # ── Determine pipeline direction ──────────────────────────────────────────
    # Execution ARN format: arn:aws:states:<r>:<a>:execution:<sm-name>:<exec-name>
    # SM ARN format:        arn:aws:states:<r>:<a>:stateMachine:<sm-name>
    # Match on SM name only (last segment) — the resource-type word differs.
    if EXTRACTION_SM_NAME in execution_arn:
        direction = "extraction"
    elif INGESTION_SM_NAME in execution_arn:
        direction = "ingestion"
    else:
        logger.warning("Execution ARN does not match known state machines: %s", execution_arn)
        return {"status": "skipped", "reason": "unknown state machine"}

    # ── Retrieve execution timing ─────────────────────────────────────────────
    exec_details = _sfn.describe_execution(executionArn=execution_arn)
    started_at: datetime = exec_details["startDate"]
    stopped_at: datetime = exec_details.get("stopDate") or datetime.now(timezone.utc)
    duration_seconds = int((stopped_at - started_at).total_seconds())

    # Extract original input for tenant_id / activity_id (always present)
    try:
        input_data = json.loads(exec_details.get("input", "{}"))
    except (json.JSONDecodeError, TypeError):
        input_data = {}

    # Extract final execution output for sde_context (populated by size_estimator step)
    # ResultPath="$" on EstimateExtractionSize means the output replaces the state,
    # so resource_id ends up in the execution output for any terminal state.
    try:
        output_data = json.loads(exec_details.get("output", "{}"))
    except (json.JSONDecodeError, TypeError):
        output_data = {}

    tenant_id: str = input_data.get("tenant_id", "unknown")
    run_id: str = execution_name or input_data.get("run_id", execution_arn.split(":")[-1])
    activity_id: str = input_data.get("activity_id", "")

    # resource_id is fetched by the size estimator step and stored in sde_context.
    # For extraction: it survives to the final execution output.
    # For ingestion: ReadIngestionSummary replaces the output, so we read it from
    # the TaskStateExited event of the size estimator step in the execution history.
    # We resolve it during the history scan in _get_selected_path_from_history.
    tenant_resource_id: str = output_data.get("sde_context", {}).get("resource_id", "")

    # ── Determine path from execution history ─────────────────────────────────
    selected_path, resource_id_from_history = _get_selected_path_from_history(execution_arn)

    # Use resource_id from history (reliable for both extraction + ingestion).
    # Ingestion's ReadIngestionSummary step overwrites execution output with summary JSON,
    # so output_data won't have sde_context — history is the fallback.
    if not tenant_resource_id and resource_id_from_history:
        tenant_resource_id = resource_id_from_history

    spla_liable: bool = selected_path in SPLA_PATH_CORES
    spla_cores: int = SPLA_PATH_CORES.get(selected_path, 0)
    billing_month = started_at.strftime("%Y-%m")
    ttl_epoch = int(started_at.timestamp()) + TTL_SECONDS

    # Derive path label and compute type
    path_label = (
        "PATH2" if selected_path == "STANDARD" else
        "PATH3" if selected_path == "BATCH" else
        "PATH1"  # EXPRESS
    )
    compute = (
        "fargate" if selected_path == "STANDARD" else
        "batch"   if selected_path == "BATCH" else
        "fargate"  # Express also runs on Fargate (smaller task)
    )

    # ── Write DynamoDB record ─────────────────────────────────────────────────
    table = _dynamo.Table(TABLE_NAME)
    item = {
        "tenant_id": tenant_id,
        "run_id": run_id,
        "activity_id": activity_id,
        "tenant_resource_id": tenant_resource_id,
        "direction": direction,
        "path": path_label,
        "selected_path": selected_path,
        "compute": compute,
        "spla_liable": spla_liable,
        "spla_cores": spla_cores,
        "started_at": started_at.isoformat(),
        "stopped_at": stopped_at.isoformat(),
        "duration_seconds": duration_seconds,
        "billing_month": billing_month,
        "execution_status": status,
        "execution_arn": execution_arn,
        "env": ENV,
        "ttl_epoch": ttl_epoch,
    }

    table.put_item(Item=item)
    logger.info(
        "Recorded run: tenant=%s resource=%s activity=%s run=%s path=%s spla_liable=%s cores=%d duration=%ds",
        tenant_id, tenant_resource_id or "n/a", activity_id or "n/a", run_id, selected_path, spla_liable, spla_cores, duration_seconds
    )

    return {
        "status": "recorded",
        "tenant_id": tenant_id,
        "run_id": run_id,
        "path": selected_path,
        "spla_liable": spla_liable,
        "spla_cores": spla_cores,
        "duration_seconds": duration_seconds,
    }


def _get_selected_path_from_history(execution_arn: str) -> tuple[str, str]:
    """
    Scan execution history for task-state entries that identify the path,
    and extract resource_id from the size estimator step's output.

    Returns (path, resource_id) where path is 'STANDARD', 'BATCH', or 'EXPRESS'
    and resource_id is from sde_context (empty string if not found).

    Size estimator step names:
      Extraction: 'EstimateExtractionSize'
      Ingestion:  'GetIngestionFileSize'

    Uses includeExecutionData=True so we can read the size estimator output.
    Stops paginating as soon as both path and resource_id are resolved.
    """
    SIZE_ESTIMATOR_STATES = {"EstimateExtractionSize", "GetIngestionFileSize"}

    paginator = _sfn.get_paginator("get_execution_history")
    pages = paginator.paginate(
        executionArn=execution_arn,
        includeExecutionData=True,  # needed to read size estimator output for resource_id
    )

    selected_path = "EXPRESS"  # default if no STANDARD/BATCH task found
    resource_id = ""

    for page in pages:
        for event in page.get("events", []):
            event_type = event.get("type", "")

            # Detect which compute path was taken
            if event_type == "TaskStateEntered":
                state_name = event.get("stateEnteredEventDetails", {}).get("name", "")
                if state_name in STANDARD_TASK_NAMES:
                    selected_path = "STANDARD"
                elif state_name in BATCH_TASK_NAMES:
                    selected_path = "BATCH"

            # Extract resource_id from size estimator output
            elif event_type == "TaskStateExited":
                state_name = event.get("stateExitedEventDetails", {}).get("name", "")
                if state_name in SIZE_ESTIMATOR_STATES and not resource_id:
                    try:
                        output = json.loads(event.get("stateExitedEventDetails", {}).get("output", "{}"))
                        resource_id = output.get("sde_context", {}).get("resource_id", "")
                    except (json.JSONDecodeError, TypeError, AttributeError):
                        pass

        # Early exit once we have path AND resource_id
        if selected_path != "EXPRESS" and resource_id:
            break

    return selected_path, resource_id


# ── Concurrent Cores Metric Publisher ────────────────────────────────────────

def _publish_concurrent_cores_metric() -> dict:
    """
    Query running SFN executions, inspect their histories to determine which are
    SPLA-liable (STANDARD or BATCH path), sum 16 cores per job, and publish as
    a CloudWatch gauge metric sde/SPLA/ConcurrentSPLACores.

    This runs on a 5-minute EventBridge cron for near-real-time cost visibility.
    """
    total_concurrent_cores = 0
    spla_run_count = 0

    for sm_arn in [EXTRACTION_SM_ARN, INGESTION_SM_ARN]:
        paginator = _sfn.get_paginator("list_executions")
        pages = paginator.paginate(
            stateMachineArn=sm_arn,
            statusFilter="RUNNING",
        )

        for page in pages:
            for execution in page.get("executions", []):
                exec_arn = execution["executionArn"]
                path_str, _ = _get_selected_path_from_history(exec_arn)
                if path_str in SPLA_PATH_CORES:
                    total_concurrent_cores += SPLA_PATH_CORES[path_str]
                    spla_run_count += 1

    _cw.put_metric_data(
        Namespace=CW_NAMESPACE,
        MetricData=[
            {
                "MetricName": "ConcurrentSPLACores",
                "Value": float(total_concurrent_cores),
                "Unit": "Count",
                "Dimensions": [{"Name": "Environment", "Value": ENV}],
            },
            {
                "MetricName": "ConcurrentSPLARuns",
                "Value": float(spla_run_count),
                "Unit": "Count",
                "Dimensions": [{"Name": "Environment", "Value": ENV}],
            },
        ],
    )

    logger.info(
        "Published concurrent SPLA metrics: %d cores across %d runs",
        total_concurrent_cores, spla_run_count
    )
    return {
        "status": "published",
        "concurrent_spla_cores": total_concurrent_cores,
        "concurrent_spla_runs": spla_run_count,
    }
