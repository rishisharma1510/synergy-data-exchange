"""
spla_monthly_reporter.py — Monthly SPLA usage reporter for Synergy Data Exchange pipelines.

Triggered by an EventBridge cron rule at 01:00 UTC on the 1st of every month.
Computes peak concurrent SPLA cores for the PREVIOUS month using a sweep-line
algorithm over the DynamoDB sde-spla-usage table.

Report written to:
  s3://{REPORT_BUCKET}/{STAGE}/usagebyservice/SQL_SPLA/{YYYY-MM}.json

Microsoft SPLA Reporting Flow:
  1. This Lambda runs on the 1st — generates JSON report in S3.
  2. You (or an ops engineer) read cores_to_declare_to_lsp from the report.
  3. Log in to your LSP portal (e.g. SHI, CDW, Insight) by the 5th of the month.
  4. Declare: Product = "SQL Server Standard Core License"
              Quantity = cores_to_declare_to_lsp
              Period   = billing_month (previous month)
  5. LSP invoices Microsoft on your behalf.

SPLA minimum: 4 cores even in a month with zero usage.
Only SPLA-liable runs (spla_liable=True, path=STANDARD or BATCH) count toward peak.
Express runs are tracked for visibility but excluded from SPLA cost.
"""
from __future__ import annotations

import decimal
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── AWS clients ──────────────────────────────────────────────────────────────
_dynamo = boto3.resource("dynamodb")
_s3 = boto3.client("s3")

# ── Environment ──────────────────────────────────────────────────────────────
TABLE_NAME: str = os.environ["SPLA_TABLE_NAME"]
REPORT_BUCKET: str = os.environ["REPORT_BUCKET"]            # ss-cdk{stage}-metrics-shared
REPORT_PREFIX: str = os.environ["REPORT_PREFIX"]            # {stage}/usagebyservice/SQL_SPLA
ENV: str = os.environ.get("ENV", "prod")

# Microsoft SPLA minimum billable cores — declare at least this even with zero usage
SPLA_MINIMUM_CORES = 4


# ── Entrypoint ───────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context: object) -> dict:
    """Generate SPLA report for the previous calendar month and upload to S3.
    
    For testing, pass {"_override_billing_month": "YYYY-MM"} in the event
    to generate a report for a specific month instead of the previous month.
    """
    now = datetime.now(timezone.utc)

    # Allow test override of billing month
    if "_override_billing_month" in event:
        billing_month = event["_override_billing_month"]
        logger.warning("Using override billing month: %s (test only)", billing_month)
    else:
        # Calculate previous month
        if now.month == 1:
            year, month = now.year - 1, 12
        else:
            year, month = now.year, now.month - 1
        billing_month = f"{year:04d}-{month:02d}"
    logger.info("Generating SPLA report for billing month: %s", billing_month)

    # ── Fetch ALL runs for the billing month (all paths) ────────────────────
    table = _dynamo.Table(TABLE_NAME)
    all_runs = _query_billing_month(table, billing_month)

    # Split into SPLA-liable (STANDARD/BATCH) and non-liable (EXPRESS)
    runs = [r for r in all_runs if r.get("spla_liable") is True]
    express_runs = [r for r in all_runs if r.get("spla_liable") is not True]

    logger.info(
        "Found %d total runs for %s: %d SPLA-liable (STANDARD/BATCH), %d Express (free)",
        len(all_runs), billing_month, len(runs), len(express_runs)
    )

    # ── Compute peak concurrent cores (sweep-line algorithm) ─────────────────
    peak_total, peak_events = _compute_peak_concurrent_cores(runs)

    # Enforce SPLA minimum
    cores_to_report = max(SPLA_MINIMUM_CORES, peak_total)

    # ── Per-tenant summary ───────────────────────────────────────────────────
    per_tenant = _compute_per_tenant_summary(runs)
    per_tenant_express = _compute_per_tenant_summary(express_runs)

    # ── Build report ─────────────────────────────────────────────────────────
    report: dict[str, Any] = {
        "schema_version": "1.1",
        "report_period": billing_month,
        "generated_at": now.isoformat(),
        "environment": ENV,

        # ── Microsoft SPLA declaration (what you submit to your LSP) ──────────
        "microsoft_spla_declaration": {
            "product": "SQL Server Standard Core License",
            "quantity_cores": cores_to_report,
            "billing_period": billing_month,
            "due_date": f"{now.year:04d}-{now.month:02d}-05",
            "instructions": (
                f"Log in to your LSP portal (e.g. SHI, CDW, Insight) and declare "
                f"{cores_to_report} core(s) of 'SQL Server Standard Core License' "
                f"for period {billing_month}. Submission due by the 5th of this month."
            ),
        },

        # ── SPLA-liable detail (STANDARD + BATCH paths only) ──────────────────
        "spla_liable_summary": {
            "peak_concurrent_spla_cores": peak_total,
            "peak_timestamp": peak_events[0]["timestamp"] if peak_events else None,
            "minimum_required_cores": SPLA_MINIMUM_CORES,
            "cores_to_declare_to_lsp": cores_to_report,
            "total_spla_runs": len(runs),
            "total_spla_duration_hours": round(sum(r["duration_seconds"] for r in runs) / 3600, 2),
            "peak_concurrent_events": peak_events[:5],
            "per_tenant": per_tenant,
        },

        # ── Express runs (free, no SPLA cost — for visibility only) ──────────
        "express_summary": {
            "total_express_runs": len(express_runs),
            "total_express_duration_hours": round(sum(r["duration_seconds"] for r in express_runs) / 3600, 2),
            "note": "SQL Server Express Edition — free, no SPLA obligation.",
            "per_tenant": per_tenant_express,
        },

        # ── Full run log (all paths, for audit) ───────────────────────────────
        "all_runs": all_runs,
    }

    # ── Upload to S3 ──────────────────────────────────────────────────────────
    s3_key = f"{REPORT_PREFIX.rstrip('/')}/{billing_month}.json"
    _upload_report(report, s3_key)

    logger.info(
        "SPLA report for %s: peak=%d cores, reporting=%d cores, s3=s3://%s/%s",
        billing_month, peak_total, cores_to_report, REPORT_BUCKET, s3_key
    )

    return {
        "status": "complete",
        "billing_month": billing_month,
        "peak_concurrent_spla_cores": peak_total,
        "cores_to_declare_to_lsp": cores_to_report,
        "total_spla_runs": len(runs),
        "total_express_runs": len(express_runs),
        "s3_key": s3_key,
    }


# ── DynamoDB Query ───────────────────────────────────────────────────────────

def _query_billing_month(table: Any, billing_month: str) -> list[dict]:
    """Query all SPLA records for a billing month using the GSI."""
    items: list[dict] = []
    kwargs: dict = {
        "IndexName": "billing_month-index",
        "KeyConditionExpression": Key("billing_month").eq(billing_month),
    }

    while True:
        response = table.query(**kwargs)
        for item in response.get("Items", []):
            # Normalise Decimal → int/float for JSON serialisation
            items.append(_normalise_item(item))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key

    return items


def _normalise_item(item: dict) -> dict:
    """Convert DynamoDB Decimal types to native Python numbers."""
    result = {}
    for k, v in item.items():
        if isinstance(v, decimal.Decimal):
            result[k] = int(v) if v == v.to_integral_value() else float(v)
        else:
            result[k] = v
    return result


# ── Sweep-line Peak Concurrent Core Calculator ───────────────────────────────

def _compute_peak_concurrent_cores(runs: list[dict]) -> tuple[int, list[dict]]:
    """
    Computes the maximum number of concurrent SPLA cores at any single point in time
    using a sweep-line algorithm over the run intervals.

    Returns (peak_core_count, list_of_events_at_peak).
    """
    events: list[tuple[str, int, str, str]] = []  # (timestamp_iso, delta, run_id, tenant_id)

    for run in runs:
        started = run.get("started_at")
        stopped = run.get("stopped_at")
        cores = int(run.get("spla_cores", 16))
        run_id = run.get("run_id", "?")
        tenant_id = run.get("tenant_id", "?")

        if not started:
            continue

        events.append((started, +cores, run_id, tenant_id))
        if stopped:
            events.append((stopped, -cores, run_id, tenant_id))

    # Sort by timestamp, with END events (+delta<0) before START events at same time
    events.sort(key=lambda e: (e[0], e[1]))

    current = 0
    peak = 0
    peak_events: list[dict] = []

    for ts, delta, run_id, tenant_id in events:
        current += delta
        if current > peak:
            peak = current
            peak_events = [{"timestamp": ts, "run_id": run_id, "tenant_id": tenant_id, "cores": current}]
        elif current == peak and peak > 0:
            peak_events.append({"timestamp": ts, "run_id": run_id, "tenant_id": tenant_id, "cores": current})

    return peak, peak_events


# ── Per-tenant Summary ────────────────────────────────────────────────────────

def _compute_per_tenant_summary(runs: list[dict]) -> list[dict]:
    """Aggregate core-hours and run counts per tenant, including resource ID."""
    by_tenant: dict[str, dict] = defaultdict(lambda: {
        "tenant_id": "",
        "tenant_resource_id": "",
        "total_runs": 0,
        "path1_runs": 0,
        "path2_runs": 0,
        "path3_runs": 0,
        "total_core_hours": 0.0,
        "total_duration_hours": 0.0,
    })

    for run in runs:
        t = run.get("tenant_id", "unknown")
        cores = int(run.get("spla_cores", 0))
        duration_hours = run.get("duration_seconds", 0) / 3600

        entry = by_tenant[t]
        entry["tenant_id"] = t
        entry["tenant_resource_id"] = entry["tenant_resource_id"] or run.get("tenant_resource_id", "")
        entry["total_runs"] += 1
        entry["total_duration_hours"] = round(entry["total_duration_hours"] + duration_hours, 4)
        entry["total_core_hours"] = round(entry["total_core_hours"] + cores * duration_hours, 4)

        path = run.get("path", "")
        if path == "PATH1":
            entry["path1_runs"] += 1
        elif path == "PATH2":
            entry["path2_runs"] += 1
        elif path == "PATH3":
            entry["path3_runs"] += 1

    return sorted(by_tenant.values(), key=lambda x: x["total_core_hours"], reverse=True)


# ── S3 Upload ─────────────────────────────────────────────────────────────────

def _upload_report(report: dict, s3_key: str) -> None:
    """Upload the JSON report to S3 with content-type and metadata."""
    body = json.dumps(report, indent=2, default=str)
    spla = report.get("spla_liable_summary", {})
    _s3.put_object(
        Bucket=REPORT_BUCKET,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "billing_month": report["report_period"],
            "peak_cores": str(spla.get("peak_concurrent_spla_cores", 0)),
            "cores_to_declare": str(spla.get("cores_to_declare_to_lsp", 0)),
            "generated_by": "sde-spla-monthly-reporter",
        },
        ServerSideEncryption="AES256",
    )
    logger.info("Uploaded SPLA report to s3://%s/%s (%d bytes)", REPORT_BUCKET, s3_key, len(body))
