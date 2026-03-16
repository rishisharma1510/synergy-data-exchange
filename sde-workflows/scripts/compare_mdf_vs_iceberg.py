"""
compare_mdf_vs_iceberg.py  --  MDF vs Iceberg data comparison

Usage:
    python scripts/compare_mdf_vs_iceberg.py [run_id]

Default run_id: ingest-1772921942  (last successful Caribbean_v7_CEDE run)
Filters all SID-transformed tables by exposuresetsid = ExposureSetSID from run.
"""
import sys
import json
import boto3
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog

REGION        = "us-east-1"
BUCKET        = "rs-cdkdev-bridgete01-s3"
NAMESPACE     = "rs_cdkdev_bridgete01_exposure_db"
DEFAULT_RUN_ID = "ingest-1772921942"


def get_summary(run_id):
    s3 = boto3.client("s3", region_name=REGION)
    key = f"sde/runs/{run_id}/ingestion_summary.json"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[WARN] S3 unavailable: {e}")
        return None


def scan_table(catalog, table):
    """Load full Iceberg table as Arrow. Returns (arrow_table, error_str)."""
    try:
        tbl = catalog.load_table(f"{NAMESPACE}.{table}")
        return tbl.scan().to_arrow(), None
    except Exception as e:
        return None, str(e)


def count_by_sid(arrow, sid_col, sid_val):
    """Count rows where sid_col == sid_val."""
    if sid_col not in arrow.schema.names:
        return None, f"column '{sid_col}' not found"
    col = arrow[sid_col]
    mask = pc.fill_null(pc.equal(col, sid_val), False)
    result = pc.sum(mask.cast("int64")).as_py()
    return (int(result) if result is not None else 0), None


def count_by_range(arrow, sid_col, sid_lo, sid_hi):
    """Count rows where sid_lo <= sid_col <= sid_hi."""
    if sid_col not in arrow.schema.names:
        return None, f"column '{sid_col}' not found"
    col = arrow[sid_col]
    mask = pc.fill_null(
        pc.and_(pc.greater_equal(col, sid_lo), pc.less_equal(col, sid_hi)), False
    )
    result = pc.sum(mask.cast("int64")).as_py()
    return (int(result) if result is not None else 0), None


def derive_own_sid_col(table_name_lc):
    """Infer the table's own PK SID column name from lowercase table name.
    Pattern: strip leading 't', append 'sid'  e.g. tlocterm -> loctermsid"""
    stem = table_name_lc.lstrip("t")
    return stem + "sid"


def main():
    run_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_RUN_ID
    print(f"Run: {run_id}\n")

    summary = get_summary(run_id)
    if not summary:
        print("[INFO] Using hardcoded log data (S3 unavailable)\n")
        # Hardcoded from ECS log: ingest-1772921942
        summary = {
            "source": "s3://rs-cdkdev-bridgete01-s3/Synergy-Drive/IngestionInput/Caribbean_v7_CEDE.mdf",
            "tables_total": 53, "tables_success": 4, "tables_failed": 0,
            "tables_skipped": 49, "total_rows": 4824,
            # ExposureSetSID for this tenant in this run = 22 (from tExposureSet SID range=22-22)
            "exposure_set_sid": 22,
            "tables": [
                {"name": "tExposureSet", "status": "success", "rows": 1,    "sid_range": "22-22"},
                {"name": "tContract",    "status": "success", "rows": 7,    "sid_range": "148-154"},
                {"name": "tLocation",   "status": "success", "rows": 2408, "sid_range": "50579-52986"},
                {"name": "tLocTerm",    "status": "success", "rows": 2408, "sid_range": "50569-52976"},
            ],
        }

    # Derive exposureSetSID: the SID assigned to tExposureSet (range start = end = SID key)
    exp_sid = summary.get("exposure_set_sid")
    if not exp_sid:
        for t in summary.get("tables", []):
            if t["name"].lower() == "texposureset" and t.get("sid_range"):
                parts = t["sid_range"].split("-")
                if parts[0].isdigit():
                    exp_sid = int(parts[0])
                    break
    print(f"Source       : {summary['source']}")
    print(f"ExposureSetSID (this run): {exp_sid}")
    print(f"Run summary  : {summary['tables_success']} success / {summary['tables_failed']} failed "
          f"/ {summary['tables_skipped']} skipped / {summary['tables_total']} total  ({summary['total_rows']} rows)\n")

    catalog = load_catalog("glue", **{"type": "glue", "region_name": REGION})
    try:
        iceberg_all = {t[1].lower() for t in catalog.list_tables(NAMESPACE)}
        print(f"Iceberg tables in namespace: {len(iceberg_all)}\n")
    except Exception as e:
        print(f"ERROR listing tables: {e}")
        iceberg_all = set()

    #  Per-table comparison 
    print("=" * 110)
    print(f"{'Table':<30} {'MDF rows':>9} {'Iceberg total':>14} {'This tenant':>13}  SID range         Notes")
    print("-" * 110)

    total_mdf      = 0
    total_tenant   = 0
    all_ok         = True
    success_lc     = set()

    for t in summary["tables"]:
        name    = t["name"]
        name_lc = name.lower()
        mdf_rows = t["rows"]
        sid_range = t.get("sid_range", "")
        success_lc.add(name_lc)

        arrow, err = scan_table(catalog, name_lc)
        if err:
            print(f"{name:<30} {mdf_rows:>9}  {'ERR':>14}  {'ERR':>13}  {sid_range:<18}  {err}")
            all_ok = False
            continue

        total_ice = arrow.num_rows

        # Primary filter: exposuresetsid = exp_sid (FK set for this tenant)
        tenant_rows, ferr = count_by_sid(arrow, "exposuresetsid", exp_sid) if exp_sid else (None, "no SID")
        filter_method = "exposuresetsid"

        # Fallback: if exposuresetsid is all null, use the table's own SID range
        if tenant_rows == 0 and total_ice > 0 and sid_range and not ferr:
            sid_parts = sid_range.split("-")
            if len(sid_parts) == 2 and sid_parts[0].isdigit() and sid_parts[1].isdigit():
                sid_lo, sid_hi = int(sid_parts[0]), int(sid_parts[1])
                own_sid_col = derive_own_sid_col(name_lc)
                fb_rows, fb_err = count_by_range(arrow, own_sid_col, sid_lo, sid_hi)
                if not fb_err and fb_rows is not None and fb_rows > 0:
                    tenant_rows = fb_rows
                    ferr = None
                    filter_method = f"{own_sid_col} range (exposuresetsid=null)"

        # Build notes
        notes = []
        if "range" in filter_method:
            notes.append(f"[!] exposuresetsid=null, filtered by {filter_method}")
        if tenant_rows is not None and tenant_rows != mdf_rows:
            notes.append(f"DIFF: expected {mdf_rows}, found {tenant_rows}")
            all_ok = False
        elif tenant_rows == mdf_rows:
            notes.append("OK")
        if total_ice > mdf_rows and tenant_rows == mdf_rows:
            extra = total_ice - (tenant_rows or 0)
            notes.append(f"{extra} extra rows from other runs/tenants in Iceberg")
        if ferr:
            notes.append(f"filter err: {ferr}")

        tenant_str = str(tenant_rows) if tenant_rows is not None else "N/A"
        print(f"{name:<30} {mdf_rows:>9} {total_ice:>14} {tenant_str:>13}  {sid_range:<18}  {', '.join(notes)}")

        total_mdf    += mdf_rows
        if tenant_rows is not None:
            total_tenant += tenant_rows

    print("-" * 110)
    print(f"{'TOTAL':<30} {total_mdf:>9} {'':>14} {total_tenant:>13}")
    print()

    #  Other tables in Iceberg namespace 
    others = sorted(iceberg_all - success_lc)
    print("=" * 110)
    print(f"Other tables in Iceberg namespace (not written in this run):")
    print(f"  {'Table':<42} {'Total rows':>12}  {'This tenant (exposuresetsid=%s)' % exp_sid:>36}")
    print("  " + "-" * 95)
    for tname in others:
        arrow, err = scan_table(catalog, tname)
        if err:
            print(f"  {tname:<42} {'ERR':>12}  {err}")
            continue
        total = arrow.num_rows
        tenant, _ = count_by_sid(arrow, "exposuresetsid", exp_sid) if exp_sid else (None, None)
        tenant_str = str(tenant) if tenant is not None else "N/A (no exposuresetsid)"
        note = ""
        if isinstance(tenant, int) and tenant > 0:
            note = "<-- tenant has data here"
        print(f"  {tname:<42} {total:>12}  {tenant_str:>36}  {note}")
    print()

    #  Verdict 
    print("=" * 110)
    if all_ok and total_mdf == total_tenant:
        print(f"RESULT: PASS  all {total_mdf} rows for this tenant confirmed in Iceberg.")
    else:
        print(f"RESULT: Review needed  wrote {total_mdf} rows, found {total_tenant} for this tenant.")
    print(f"\nNote: {summary['tables_skipped']} tables were SKIPPED (empty in MDF  nothing to ingest).")


if __name__ == "__main__":
    main()
