"""
Repair Iceberg table manifest statistics for the INT→LONG column evolution issue.

PROBLEM:
  When a column is evolved from Iceberg IntegerType → LongType (e.g. via
  Athena ALTER TABLE ... CHANGE COLUMN ... BIGINT), the *existing* manifest
  files still store that column's lower_bounds / upper_bounds as 4-byte
  little-endian integers.  PyIceberg v0.11 tries to unpack those as 8-byte
  values and raises:
      struct.error: unpack requires a buffer of 8 bytes
  This forces a full-table-scan fallback in iceberg_reader.py, which for a
  table like tlocation (23 GB Parquet) causes OOM on the Express container.

FIX:
  Run Athena Engine v3 `OPTIMIZE ... REWRITE DATA USING BIN_PACK` on each
  affected table.  Athena reads the table according to its CURRENT Iceberg
  schema (LONG), rewrites the Parquet files with correct 8-byte statistics,
  and creates new manifest files that reference those files.  After the
  OPTIMIZE completes, PyIceberg's manifest-bounds evaluation works correctly
  and partition-pruning is restored to O(files_in_partition) rather than a
  full scan.

SCOPE:
  Affected tables in rs_cdkdev_ssautoma01_exposure_db (confirmed by failures):
    - tcontract   (4.7 GB Parquet, 277M rows)   — struct.error confirmed
    - tlocation   (23 GB Parquet, 829M rows)    — struct.error + OOM confirmed
  Likely affected (slow scans, 0 rows observed):
    - tlocfeature (1.9 GB Parquet, 819M rows)
    - tlocterm    (5.8 GB Parquet, 1.4B rows)

USAGE:
    python scripts/optimize_iceberg_tables.py [--dry-run] [--tables t1,t2]

RUNTIME ESTIMATE:
  tcontract  ~  5-10 min  ($0.12 Athena scan cost)
  tlocation  ~ 30-45 min  ($1.15 Athena scan cost)
  tlocterm   ~ 10-15 min  ($0.29 Athena scan cost)
  tlocfeature~  5-10 min  ($0.10 Athena scan cost)

PERMISSIONS REQUIRED:
  The caller must be able to:
    - athena:StartQueryExecution / GetQueryExecution / GetQueryResults
    - s3:GetObject, s3:PutObject on the tenant S3 bucket
    - glue:GetTable, glue:UpdateTable on the Glue database

NOTE:
  OPTIMIZE creates new data files AND keeps old ones alive in the current
  snapshot.  Old files are cleaned up only when you run
      VACUUM "db"."table"
  which expires the old snapshot.  Run vacuum after verifying the fix works.
"""

import argparse
import sys
import time
import logging
import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
REGION        = "us-east-1"
GLUE_DATABASE = "rs_cdkdev_ssautoma01_exposure_db"
WORKGROUP     = "primary"
# Athena query results are written here (caller must have S3 write access)
OUTPUT_BUCKET = "s3://rs-cdkdev-ruby000001-s3/AthenaQueryResults/iceberg-optimize/"

# Tables to optimize — add/remove as needed
DEFAULT_TABLES = [
    "tcontract",    # 4.7 GB Parquet — struct.error confirmed
    "tlocation",    # 23  GB Parquet — struct.error + OOM confirmed
    "tlocterm",     # 5.8 GB Parquet — slow scan (0 rows) suspected
    "tlocfeature",  # 1.9 GB Parquet — slow scan (0 rows) suspected
]

# How often to poll Athena for query completion (seconds)
POLL_INTERVAL_SECS = 30
# Max time to wait per table before giving up (seconds)
QUERY_TIMEOUT_SECS = 7200  # 2 hours


def start_optimize(athena, database: str, table: str, dry_run: bool) -> str | None:
    """Submit an OPTIMIZE query and return the QueryExecutionId."""
    sql = f"OPTIMIZE {database}.{table} REWRITE DATA USING BIN_PACK"
    if dry_run:
        log.info("[DRY-RUN] Would run: %s", sql)
        return None

    log.info("Submitting: %s", sql)
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database, "Catalog": "AwsDataCatalog"},
        WorkGroup=WORKGROUP,
        ResultConfiguration={"OutputLocation": OUTPUT_BUCKET},
    )
    qid = resp["QueryExecutionId"]
    log.info("  QueryExecutionId: %s", qid)
    return qid


def wait_for_query(athena, qid: str, table: str) -> bool:
    """Poll until the query finishes.  Returns True on success."""
    t0 = time.time()
    while True:
        resp = athena.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED",):
            elapsed = time.time() - t0
            stats = resp["QueryExecution"].get("Statistics", {})
            scanned_mb = stats.get("DataScannedInBytes", 0) / 1_048_576
            log.info(
                "  ✓ %s SUCCEEDED in %.0fs (scanned %.0f MB)",
                table, elapsed, scanned_mb,
            )
            return True
        if state in ("FAILED", "CANCELLED"):
            reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "")
            log.error("  ✗ %s %s: %s", table, state, reason)
            return False
        if time.time() - t0 > QUERY_TIMEOUT_SECS:
            log.error("  ✗ %s timed out after %.0fs (state=%s)", table, time.time() - t0, state)
            return False
        elapsed = time.time() - t0
        log.info("  … %s still running (%.0fs elapsed, state=%s)", table, elapsed, state)
        time.sleep(POLL_INTERVAL_SECS)


def verify_scan(table: str) -> bool:
    """
    Try a filtered PyIceberg scan on the table.  If no struct.error is raised
    the manifest statistics are now correct.
    """
    try:
        from pyiceberg.catalog import load_catalog
        from pyiceberg.expressions import EqualTo

        import os
        catalog = load_catalog(
            "glue",
            **{
                "type": "glue",
                "glue.region": REGION,
            },
        )
        tbl = catalog.load_table(f"{GLUE_DATABASE}.{table}")
        # Use a dummy exposuresetsid value — we just want to exercise the
        # manifest-bounds evaluation path without actually reading data.
        scan = tbl.scan(row_filter=EqualTo("exposuresetsid", 99999999))
        # plan_files() triggers manifest-bounds evaluation
        file_count = sum(1 for _ in scan.plan_files())
        log.info(
            "  ✓ %s verify OK — scan found %d candidate file(s) for dummy filter",
            table, file_count,
        )
        return True
    except Exception as exc:
        log.warning("  ✗ %s verify FAILED: %s", table, exc)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    parser.add_argument("--tables", default=",".join(DEFAULT_TABLES),
                        help="Comma-separated table names to optimize")
    parser.add_argument("--verify", action="store_true", default=True,
                        help="Run a PyIceberg verify scan after each OPTIMIZE (default: True)")
    parser.add_argument("--no-verify", dest="verify", action="store_false")
    parser.add_argument("--vacuum", action="store_true",
                        help="Also run VACUUM after OPTIMIZE to clean up old files")
    args = parser.parse_args()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    log.info("Tables to optimize: %s", tables)
    log.info("Database:           %s", GLUE_DATABASE)
    log.info("Workgroup:          %s", WORKGROUP)
    log.info("Output bucket:      %s", OUTPUT_BUCKET)
    if args.dry_run:
        log.info("DRY-RUN mode — no queries will be executed")

    athena = boto3.client("athena", region_name=REGION)

    results = {}
    for table in tables:
        log.info("=" * 60)
        log.info("Processing table: %s", table)

        qid = start_optimize(athena, GLUE_DATABASE, table, args.dry_run)
        if qid is None:
            results[table] = "dry-run"
            continue

        ok = wait_for_query(athena, qid, table)
        results[table] = "ok" if ok else "failed"

        if ok and args.verify:
            verify_ok = verify_scan(table)
            if not verify_ok:
                log.warning(
                    "  Verify failed for %s — the struct.error may still occur. "
                    "Check that the Iceberg schema has the column as LongType.",
                    table,
                )
                results[table] = "optimize-ok-but-verify-failed"

        if ok and args.vacuum:
            log.info("Running VACUUM on %s …", table)
            vacuum_sql = f"VACUUM {GLUE_DATABASE}.{table}"
            if not args.dry_run:
                resp = athena.start_query_execution(
                    QueryString=vacuum_sql,
                    QueryExecutionContext={"Database": GLUE_DATABASE, "Catalog": "AwsDataCatalog"},
                    WorkGroup=WORKGROUP,
                    ResultConfiguration={"OutputLocation": OUTPUT_BUCKET},
                )
                vacuum_ok = wait_for_query(athena, resp["QueryExecutionId"], f"{table} VACUUM")
                if not vacuum_ok:
                    log.warning("VACUUM failed for %s; old files remain but are not visible.", table)

    log.info("=" * 60)
    log.info("Summary:")
    ok_count = sum(1 for v in results.values() if v == "ok")
    fail_count = sum(1 for v in results.values() if v == "failed")
    for tbl, status in results.items():
        icon = "✓" if status == "ok" else ("~" if status == "dry-run" else "✗")
        log.info("  %s %s → %s", icon, tbl, status)

    if fail_count > 0:
        log.error("%d table(s) failed to optimize.", fail_count)
        return 1
    if ok_count > 0:
        log.info(
            "\nAll %d table(s) optimized successfully.\n"
            "The next extraction run for ssautoma01 will use partition-pruned "
            "scans (no full-scan fallback). Re-run with --vacuum to clean up old "
            "data files once you verify the fix works.",
            ok_count,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
