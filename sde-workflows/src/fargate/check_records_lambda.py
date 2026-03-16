"""
check_records_lambda.py — Lambda function invoked at the start of the extraction
Step Function to determine whether there are any Iceberg records matching the
requested exposure IDs.

If no records exist, the Step Function exits early (Succeed) without spinning up
an expensive ECS/Fargate or Batch task.

Expected input (passed directly from Step Functions):
{
    "tenant_id": "...",
    "activity_id": "...",
    "run_id": "...",
    "exposure_ids": [1, 2, 3],
    "tables": ["TContract", "TExposure"],   // or []/"all" for all tables
    "target_namespace": "default"           // optional, defaults to "default"
}

Returns (merged back into Step Functions state):
{
    ...original input...,
    "record_check": {
        "has_records": true | false,
        "record_count": <int>,
        "tables_checked": <int>
    }
}
"""

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _parse_exposure_ids(raw) -> list[int]:
    """Accept int list, JSON string, or comma-separated string."""
    if isinstance(raw, list):
        return [int(x) for x in raw if x is not None]
    if isinstance(raw, str):
        raw = raw.strip()
        if raw.startswith("["):
            return [int(x) for x in json.loads(raw)]
        return [int(x.strip()) for x in raw.split(",") if x.strip()]
    return []


def _parse_tables(raw) -> list[str]:
    """Parse tables field — returns [] meaning 'all tables'."""
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    raw = str(raw).strip()
    if raw.lower() in ("all", '"all"', "'all'"):
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
    except (json.JSONDecodeError, ValueError):
        pass
    return [t.strip() for t in raw.split(",") if t.strip()]


def _count_records(namespace: str, table_name: str, exposure_ids: list[int]) -> int:
    """
    Do a lightweight Iceberg scan and return the row count.

    Uses pyiceberg + PyArrow so only the minimal number of row-groups are read.
    If exposure_ids is empty (reference table) we just return 1 on any data
    present — we don't need the exact count for the early-exit check.
    """
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError

    catalog = load_catalog(
        name=os.environ.get("ICEBERG_CATALOG_NAME", "glue"),
        **_catalog_properties(),
    )

    try:
        iceberg_tbl = catalog.load_table(f"{namespace}.{table_name}")
    except (NoSuchTableError, NoSuchNamespaceError):
        logger.warning("Table %s.%s not found — counting as 0", namespace, table_name)
        return 0

    if exposure_ids:
        from pyiceberg.expressions import In as _IcebergIn

        # Case-insensitive lookup for exposure linking column
        _schema_lower = {f.name.lower(): f.name for f in iceberg_tbl.schema().fields}
        _candidates = ["exposuresetsid", "exposuresetid", "exposure_id", "exposureid"]
        filter_expr = None
        for candidate in _candidates:
            actual = _schema_lower.get(candidate)
            if actual:
                filter_expr = _IcebergIn(actual, set(exposure_ids))  # type: ignore[call-arg]
                break

        if filter_expr is not None:
            scan = iceberg_tbl.scan(row_filter=filter_expr, limit=1)
        else:
            # No filter column — reference table, include if it has any rows
            scan = iceberg_tbl.scan(limit=1)
    else:
        scan = iceberg_tbl.scan(limit=1)

    # Arrow conversion to get actual row count (limit=1 keeps it cheap)
    arrow_table = scan.to_arrow()
    return arrow_table.num_rows


def _catalog_properties() -> dict:
    """Build catalog properties from environment variables."""
    props: dict[str, str] = {
        "type": os.environ.get("ICEBERG_CATALOG_TYPE", "glue"),
    }
    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
    if region:
        props["region_name"] = region
    warehouse = os.environ.get("ICEBERG_WAREHOUSE")
    if warehouse:
        props["warehouse"] = warehouse
    return props


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Lambda handler.

    Queries the Iceberg catalog for records matching the supplied exposure_ids.
    Returns the original event enriched with a `record_check` block so the
    downstream Choice state can branch without an extra Pass state.
    """
    tenant_id = event.get("tenant_id", "unknown")
    activity_id = event.get("activity_id", "unknown")
    namespace = event.get("target_namespace") or os.environ.get("DEFAULT_NAMESPACE", "default")
    exposure_ids = _parse_exposure_ids(event.get("exposure_ids", []))
    requested_tables = _parse_tables(event.get("tables", []))

    logger.info(
        "[check_records] tenant=%s activity=%s namespace=%s exposure_ids=%s tables=%s",
        tenant_id, activity_id, namespace, exposure_ids, requested_tables or "ALL",
    )

    try:
        from pyiceberg.catalog import load_catalog
        from pyiceberg.exceptions import NoSuchNamespaceError

        catalog = load_catalog(
            name=os.environ.get("ICEBERG_CATALOG_NAME", "glue"),
            **_catalog_properties(),
        )

        # Enumerate available tables
        try:
            all_table_ids = catalog.list_tables(namespace)
            all_table_names = [t[-1] for t in all_table_ids]  # tuples of (namespace, name)
        except NoSuchNamespaceError:
            logger.warning("[check_records] Namespace '%s' not found — 0 records", namespace)
            return {
                **event,
                "record_check": {"has_records": False, "record_count": 0, "tables_checked": 0},
            }

        if requested_tables:
            tables_to_check = [t for t in all_table_names if t in requested_tables]
        else:
            tables_to_check = all_table_names

        if not tables_to_check:
            logger.info("[check_records] No matching tables found — 0 records")
            return {
                **event,
                "record_check": {"has_records": False, "record_count": 0, "tables_checked": 0},
            }

        total_rows = 0
        for table_name in tables_to_check:
            rows = _count_records(namespace, table_name, exposure_ids)
            logger.info("[check_records]   table=%-40s rows=%d", table_name, rows)
            total_rows += rows
            # Short-circuit: once we know there IS data, no need to scan every table
            if total_rows > 0:
                break

        has_records = total_rows > 0
        logger.info(
            "[check_records] Result: has_records=%s total_rows=%d tables_checked=%d",
            has_records, total_rows, len(tables_to_check),
        )

        return {
            **event,
            "record_check": {
                "has_records": has_records,
                "record_count": total_rows,
                "tables_checked": len(tables_to_check),
            },
        }

    except Exception as exc:
        # On any unexpected error, default to HAS records so we don't silently
        # skip a real extraction run.
        logger.error("[check_records] Unexpected error — defaulting to has_records=True: %s", exc)
        return {
            **event,
            "record_check": {
                "has_records": True,
                "record_count": -1,
                "tables_checked": 0,
                "error": str(exc),
            },
        }
