#!/usr/bin/env python3
"""
ingest.py — Main entry point for Iceberg → SQL Server data ingestion.

Two-step workflow:
    Step 1 (generate mapping):
        python -m src.core.ingest --table default.orders
        python -m src.core.ingest --all-tables

    Step 2 (run ingestion with reviewed mapping):
        python -m src.core.ingest --run
        python -m src.core.ingest --run --table default.orders

    One-shot (generate + ingest immediately):
        python -m src.core.ingest --run --auto
        python -m src.core.ingest --run --auto --table default.orders
"""

import argparse
import gc
import json
import logging
import queue
import sys
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterator, Callable, Optional

from src.core.config import load_config, AppConfig
from src.core.iceberg_reader import IcebergReader
from src.core.sql_writer import SqlServerWriter
from src.core.column_mapper import ColumnMapper, NAMING_TRANSFORMS
from src.core.mapping_config import (
    generate_mapping_for_table,
    load_mapping_file,
    save_mapping_file,
    mapping_entry_to_result,
    print_mapping_summary,
    _table_key,
    DEFAULT_MAPPING_FILE,
)

logger = logging.getLogger("ingest")


# =====================================================================
# Double-buffer prefetch iterator
# =====================================================================

_SENTINEL = object()


def _prefetch_iter(source_iter, *, maxsize: int = 2):
    """Wrap *source_iter* in a threaded prefetch queue.

    A background thread reads from *source_iter* and pushes items into a
    bounded ``queue.Queue`` of *maxsize*.  The main thread (consumer) can
    therefore be writing one batch to SQL Server while the producer thread
    is already fetching the next batch from S3/Iceberg.

    If the producer raises, the exception is re-raised in the consumer.
    """
    buf: queue.Queue = queue.Queue(maxsize=maxsize)
    error_box: list[BaseException] = []

    def _producer():
        try:
            for item in source_iter:
                buf.put(item)
        except BaseException as exc:
            error_box.append(exc)
        finally:
            buf.put(_SENTINEL)

    t = threading.Thread(target=_producer, daemon=True)
    t.start()

    while True:
        item = buf.get()
        if item is _SENTINEL:
            break
        yield item

    if error_box:
        raise error_box[0]


# =====================================================================
# State management (for incremental loads)
# =====================================================================


def _load_state(path: str) -> dict:
    p = Path(path)
    if p.exists():
        return json.loads(p.read_text())
    return {}


def _save_state(path: str, state: dict) -> None:
    Path(path).write_text(json.dumps(state, indent=2))


# =====================================================================
# Parallel table loading (for Fargate use)
# =====================================================================


def parallel_load_tables(
    plan: list[dict],
    reader: IcebergReader,
    sql_config,
    db_name: str,
    exposure_ids: list[int],
    max_workers: int = 4,
    progress_callback: Optional[Callable] = None,
) -> dict[str, dict]:
    """Load multiple tables in parallel using ThreadPoolExecutor.
    
    Parameters
    ----------
    plan : list[dict]
        List of table plan dictionaries with table configuration.
    reader : IcebergReader
        Configured Iceberg reader instance.
    sql_config : SqlServerConfig
        SQL Server configuration.
    db_name : str
        Target database name.
    exposure_ids : list[int]
        List of exposure IDs to filter by.
    max_workers : int
        Maximum number of parallel workers.
    progress_callback : Callable, optional
        Callback function for progress reporting.
    
    Returns
    -------
    dict[str, dict]
        Results keyed by table name with row counts and timing.
    """
    import copy as _copy_pool
    import pyodbc as _pyodbc

    results = {}
    total_tables = len(plan)
    tables_done = 0
    effective_workers = min(max_workers, total_tables) if total_tables > 0 else 1

    # Pre-create a pool of SQL Server connections before the thread-pool starts.
    # This eliminates per-thread connection handshake latency and prevents
    # thundering-herd reconnect spikes when all workers launch simultaneously.
    _conn_pool: Optional[queue.Queue] = None
    _pre_created_conns: list = []
    try:
        _pool_cfg = _copy_pool.copy(sql_config)
        _pool_cfg.database = db_name
        _conn_pool = queue.Queue()
        for _ in range(effective_workers):
            _c = _pyodbc.connect(_pool_cfg.connection_string(), autocommit=False)
            _conn_pool.put(_c)
            _pre_created_conns.append(_c)
        logger.info(
            "[ingest] Connection pool ready: %d pre-created connections for %d tables",
            effective_workers, total_tables,
        )
    except Exception as _pool_exc:
        logger.warning(
            "[ingest] Connection pool setup failed (%s) — workers will connect on demand.",
            _pool_exc,
        )
        for _c in _pre_created_conns:
            try:
                _c.close()
            except Exception:
                pass
        _pre_created_conns = []
        _conn_pool = None

    def _load_single_table(table_plan) -> dict:
        """Load a single Iceberg table into SQL Server and return result."""
        import copy as _copy
        from pyiceberg.expressions import In as _IcebergIn
        from src.core.sql_writer import SqlServerWriter

        # Support both TablePlan (Pydantic) objects and plain dicts
        if hasattr(table_plan, "source_namespace"):
            table_name   = table_plan.name
            namespace    = table_plan.source_namespace
            source_table = table_plan.source_table
            target_table = table_plan.target_table
            target_schema = table_plan.target_schema
        else:
            table_name   = table_plan.get("name", "unknown")
            namespace    = table_plan.get("source_namespace", "default")
            source_table = table_plan.get("source_table", table_name)
            target_table = table_plan.get("target_table", table_name)
            target_schema = table_plan.get("target_schema", "dbo")

        t0 = time.perf_counter()
        logger.info(
            "[ingest] Loading %s.%s → [%s].[%s] in db=%s",
            namespace, source_table, target_schema, target_table, db_name,
        )

        try:
            # 1. Load Iceberg table object (catalog already initialised in reader)
            iceberg_tbl = reader._get_table(namespace, source_table)
            arrow_schema = iceberg_tbl.schema().as_arrow()

            # 2. Detect filter column — check schema for exposure linking columns
            row_filter = None
            if exposure_ids:
                # Case-insensitive lookup: build lowercase → actual-name map from Iceberg schema
                _schema_lower_map = {f.name.lower(): f.name for f in iceberg_tbl.schema().fields}
                # Priority order: most specific first (compared case-insensitively)
                _filter_candidates = [
                    "exposuresetsid", "exposuresetid", "exposure_id", "exposureid",
                ]
                for _candidate in _filter_candidates:
                    _actual_col = _schema_lower_map.get(_candidate.lower())
                    if _actual_col is not None:
                        row_filter = _IcebergIn(_actual_col, set(exposure_ids))
                        logger.info(
                            "[ingest] %s: filter %s IN %s", source_table, _actual_col, exposure_ids
                        )
                        break
                else:
                    logger.info(
                        "[ingest] %s: no filter column found — full scan (reference table)",
                        source_table,
                    )

            # 3. Per-table SqlServerWriter (clone config, override db/table/schema)
            tbl_cfg = _copy.copy(sql_config)
            tbl_cfg.database = db_name
            tbl_cfg.table = target_table
            tbl_cfg.schema = target_schema
            writer = SqlServerWriter(tbl_cfg, naming_convention="as_is")

            # Acquire a pre-created pooled connection to skip reconnect latency.
            # Falls back to writer's own connect() if the pool is unavailable.
            _pooled_conn = _conn_pool.get() if _conn_pool is not None else None
            if _pooled_conn is not None:
                writer._conn = _pooled_conn

            _committed = False
            try:
                # 4. Create/resolve target table; get the resulting column mapping
                mapping = writer.ensure_table(arrow_schema)
                if mapping is None:
                    # ensure_table returns None; grab from internal state
                    mapping = writer._mapping_result

                # 5. Stream Iceberg → SQL Server
                batch_iter = reader.stream_table_batches(
                    namespace, source_table, row_filter=row_filter
                )
                rows_loaded = writer.write_batches_streaming(batch_iter, mapping)
                if writer._conn:
                    writer._conn.commit()
                    _committed = True
            finally:
                if _pooled_conn is not None:
                    # Roll back any uncommitted state before returning to pool
                    if not _committed:
                        try:
                            _pooled_conn.rollback()
                        except Exception:
                            pass
                    # Detach so writer.close() does not destroy the pooled connection
                    writer._conn = None
                    _conn_pool.put(_pooled_conn)
                writer.close()
                # Force a GC sweep after each large table.
                # Millions of Python tuple objects from _arrow_to_rows() accumulate
                # during executemany and may not be swept until the next GC cycle,
                # holding memory while the next parallel table thread starts up.
                gc.collect()

        except Exception as exc:
            logger.exception(
                "[ingest] %s: load FAILED after %.2fs: %s",
                source_table, time.perf_counter() - t0, exc,
            )
            return {
                "table": table_name,
                "rows": 0,
                "elapsed_seconds": time.perf_counter() - t0,
                "status": "failed",
                "error": str(exc),
            }

        elapsed = time.perf_counter() - t0
        logger.info("[ingest] %s: %d rows in %.2fs", source_table, rows_loaded, elapsed)
        return {
            "table": table_name,
            "rows": rows_loaded,
            "elapsed_seconds": elapsed,
            "status": "success",
        }
    
    try:
        with ThreadPoolExecutor(max_workers=effective_workers) as pool:
            futures = {}
            for table_plan in plan:
                future = pool.submit(_load_single_table, table_plan)
                futures[future] = table_plan.name if hasattr(table_plan, "source_namespace") else table_plan.get("name", "unknown")
            
            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    result = future.result()
                    results[table_name] = result
                    tables_done += 1
                    
                    if progress_callback:
                        progress_callback(
                            table_name=table_name,
                            rows_loaded=result["rows"],
                            total_tables=total_tables,
                            tables_done=tables_done,
                        )
                except Exception as exc:
                    logger.error("Failed to load table %s: %s", table_name, exc)
                    results[table_name] = {
                        "table": table_name,
                        "rows": 0,
                        "elapsed_seconds": 0,
                        "status": "failed",
                        "error": str(exc),
                    }
    finally:
        # Teardown: close all pre-created pooled connections
        for _c in _pre_created_conns:
            try:
                _c.close()
            except Exception:
                pass

    return results


# =====================================================================
# Step 1: Generate mapping config
# =====================================================================


def generate_mappings(
    cfg: AppConfig,
    tables: list[str],
) -> dict:
    """
    Connect to Iceberg + SQL Server, auto-generate mapping config
    for the given tables, save to disk, and print summary.

    Parameters
    ----------
    tables : list[str]
        List of ``namespace.table`` strings.

    Returns
    -------
    dict
        The generated mapping config (all tables).
    """
    reader = IcebergReader(cfg.iceberg)
    mapper = ColumnMapper(naming_convention=cfg.ingestion.naming_convention)

    # We need a SQL Server connection to check if target tables exist
    writer = SqlServerWriter(cfg.sqlserver, naming_convention=cfg.ingestion.naming_convention)

    mapping_data: dict = {}

    try:
        for table_ref in tables:
            parts = table_ref.split(".", 1)
            if len(parts) == 2:
                namespace, table = parts
            else:
                namespace = cfg.iceberg.namespace
                table = parts[0]

            key = _table_key(namespace, table)
            target_table = cfg.sqlserver.table or table

            logger.info("Generating mapping for %s.%s ...", namespace, table)

            # Get Iceberg schema
            arrow_schema = reader.get_arrow_schema(namespace, table)
            logger.info(
                "  Iceberg schema: %d columns",
                len(arrow_schema),
            )

            # Check if SQL Server target table exists
            writer.cfg.table = target_table
            target_columns = None
            if writer.table_exists():
                target_columns = writer.get_target_columns()
                logger.info(
                    "  SQL Server table [%s].[%s] exists: %d columns",
                    cfg.sqlserver.schema, target_table, len(target_columns),
                )
            else:
                logger.info(
                    "  SQL Server table [%s].[%s] does not exist (will be created).",
                    cfg.sqlserver.schema, target_table,
                )

            entry = generate_mapping_for_table(
                namespace=namespace,
                table=table,
                target_schema=cfg.sqlserver.schema,
                target_table=target_table,
                arrow_schema=arrow_schema,
                target_columns=target_columns,
                mapper=mapper,
                naming_convention=cfg.ingestion.naming_convention,
                load_mode=cfg.ingestion.load_mode,
            )

            mapping_data[key] = entry
            print_mapping_summary(entry, key)

    finally:
        writer.close()

    # Build dynamic mapping filename:
    #   single table  → <tablename>_mapping.json
    #   multiple      → AllTables_mapping.json
    if len(tables) == 1:
        # Extract just the table name (after the namespace dot)
        _tbl_name = tables[0].split(".", 1)[-1]
        mapping_file = f"{_tbl_name}_mapping.json"
    else:
        mapping_file = "AllTables_mapping.json"

    # Allow CLI --mapping to override
    if cfg.ingestion.mapping_file != DEFAULT_MAPPING_FILE:
        mapping_file = cfg.ingestion.mapping_file

    saved_path = save_mapping_file(mapping_data, mapping_file)

    logger.info("=" * 70)
    logger.info("  Mapping config saved to: %s", saved_path)
    logger.info("  Tables mapped: %d", len(mapping_data))
    logger.info("  Next steps:")
    logger.info("    1. Review/edit %s", mapping_file)
    logger.info("    2. Run:  python -m src.core.ingest --run --mapping %s", mapping_file)
    logger.info("       Or for specific table:  python -m src.core.ingest --run --table %s", tables[0])
    logger.info("=" * 70)

    # Store the resolved filename so callers (one-shot) can find it
    cfg.ingestion.mapping_file = mapping_file

    return mapping_data


# =====================================================================
# Step 2: Run ingestion from mapping config
# =====================================================================


def run_ingestion(
    cfg: AppConfig,
    mapping_data: dict,
    table_keys: list[str] | None = None,
) -> None:
    """
    Run data ingestion using the mapping config.

    Parameters
    ----------
    mapping_data : dict
        The full mapping config (possibly user-edited).
    table_keys : list[str] | None
        Specific table keys to ingest.  None = all tables in the config.
    """
    if table_keys:
        entries = {k: mapping_data[k] for k in table_keys if k in mapping_data}
        missing = [k for k in table_keys if k not in mapping_data]
        if missing:
            logger.error(
                "These table keys are not in the mapping config: %s", missing
            )
            logger.info("Available keys: %s", list(mapping_data.keys()))
            sys.exit(1)
    else:
        entries = mapping_data

    if not entries:
        logger.error("No tables to ingest.")
        sys.exit(1)

    state_file = cfg.ingestion.state_file
    state = _load_state(state_file)

    reader = IcebergReader(cfg.iceberg)

    total_tables = len(entries)
    for idx, (key, entry) in enumerate(entries.items(), 1):
        src = entry["source"]
        tgt = entry["target"]
        namespace = src["namespace"]
        table = src["table"]
        target_schema = tgt["schema"]
        target_table = tgt["table"]
        load_mode = entry.get("load_mode", cfg.ingestion.load_mode)

        logger.info("=" * 60)
        logger.info(
            "[%d/%d] Ingesting %s.%s → [%s].[%s]  (mode: %s)",
            idx, total_tables, namespace, table, target_schema, target_table, load_mode,
        )
        logger.info("=" * 60)

        # Build MappingResult from config
        mapping_result = mapping_entry_to_result(entry)
        mapping_result.log_summary()

        if not mapping_result.matched:
            logger.error("No included columns — skipping table %s.", key)
            continue

        # --- Read from Iceberg ---
        table_state_key = f"{namespace}.{table}"

        # Set reader to this table
        cfg.iceberg.namespace = namespace
        cfg.iceberg.table = table
        reader._table = None  # Reset cached table

        from_snapshot: int | None = None
        if load_mode == "incremental":
            from_snapshot = state.get(table_state_key, {}).get("last_snapshot_id")
            if from_snapshot is None:
                logger.info("No previous snapshot — falling back to full load.")
                load_mode = "full"
            else:
                logger.info("Last ingested snapshot: %s", from_snapshot)

        # --- Write to SQL Server (streaming pipeline) ---
        writer_cfg = cfg.sqlserver
        writer_cfg.schema = target_schema
        writer_cfg.table = target_table

        writer = SqlServerWriter(writer_cfg, naming_convention=cfg.ingestion.naming_convention)
        try:
            # Create table if needed
            if entry.get("create_table_if_missing", False) and not writer.table_exists():
                writer.create_table_from_mapping(entry.get("columns", []))

            row_limit = cfg.ingestion.row_limit or None

            t0 = time.perf_counter()

            # Determine which source columns are actually needed
            # so we can push projection down to the Iceberg/Parquet scan.
            # If every source column is matched we skip projection (read all).
            needed_src_cols = [
                m.source_name for m in mapping_result.matched
            ]
            iceberg_schema_names = [f["name"] for f in reader.schema_fields()]
            if set(needed_src_cols) == set(iceberg_schema_names):
                # All columns needed — no projection advantage; pass None so
                # the scan reads everything without an explicit field list.
                projection_cols = None
            else:
                projection_cols = needed_src_cols or None

            # Stream: read batch → write batch → discard → next batch
            # Memory usage is bounded to ~2 batches (double-buffer).
            raw_batch_iter = reader.stream_batches(
                mode=load_mode,
                from_snapshot_id=from_snapshot,
                limit=row_limit,
                selected_columns=projection_cols,
            )
            # Wrap in a prefetch iterator so S3 reads overlap SQL writes.
            # maxsize=4 keeps up to 4 batches pre-fetched from S3 so the
            # writer never stalls waiting for the next batch.
            batch_iter = _prefetch_iter(raw_batch_iter, maxsize=4)

            rows_written = writer.write_batches_streaming(
                batch_iter,
                mapping_result,
                batch_size=cfg.ingestion.batch_size,
                truncate_first=(load_mode == "full"),
                write_backend=cfg.ingestion.write_backend,
                bcp_path=cfg.ingestion.bcp_path,
                bcp_batch_size=cfg.ingestion.bcp_batch_size,
                bcp_temp_dir=cfg.ingestion.bcp_temp_dir,
                bcp_field_terminator=cfg.ingestion.bcp_field_terminator,
                bcp_row_terminator=cfg.ingestion.bcp_row_terminator,
            )

            elapsed = time.perf_counter() - t0
            logger.info(
                "Ingestion completed in %.2f s (%d rows).", elapsed, rows_written,
            )
        finally:
            writer.close()

        # --- Save state ---
        current_snap = reader.current_snapshot_id()
        if current_snap is not None:
            state[table_state_key] = {"last_snapshot_id": current_snap}
            _save_state(state_file, state)
            logger.info("State saved — snapshot %s.", current_snap)

        logger.info("Done ✓  %s  (%.2f s)", key, elapsed)

    logger.info("=" * 60)
    logger.info("All tables processed (%d).", total_tables)
    logger.info("=" * 60)


# =====================================================================
# CLI
# =====================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Iceberg → SQL Server data ingestion (2-step workflow).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Step 1: Generate mapping config
  python -m src.core.ingest --table default.orders
  python -m src.core.ingest --all-tables

  # Step 2: Review mapping_config.json, then run
  python -m src.core.ingest --run
  python -m src.core.ingest --run --table default.orders

  # One-shot: generate + ingest immediately
  python -m src.core.ingest --run --auto
  python -m src.core.ingest --run --auto --table default.orders
""",
    )

    # Workflow flags
    parser.add_argument(
        "--run",
        action="store_true",
        default=False,
        help="Run ingestion using the mapping config (Step 2).",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        default=False,
        help="With --run: generate mapping + ingest in one shot (skip review).",
    )

    # Table selection
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        default=None,
        help="Source table as namespace.table (can be repeated). "
             "E.g. --table default.orders --table default.customers",
    )
    parser.add_argument(
        "--all-tables",
        action="store_true",
        default=False,
        help="Generate mappings for all tables in the Iceberg namespace.",
    )

    # Overrides
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default=None,
        help="Override LOAD_MODE.",
    )
    parser.add_argument(
        "--target-table",
        default=None,
        help="Override SQLSERVER_TABLE (only for single-table runs).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Override BATCH_SIZE.",
    )
    parser.add_argument(
        "--naming-convention",
        choices=list(NAMING_TRANSFORMS.keys()),
        default=None,
        help="Naming convention for auto-generated target columns.",
    )
    parser.add_argument(
        "--mapping",
        default=None,
        help="Path to mapping config file (default: mapping_config.json).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max rows to read per table (0 = no limit).",
    )
    parser.add_argument(
        "--backend",
        choices=["pyodbc", "bcp", "arrow_odbc"],
        default=None,
        help="Override WRITE_BACKEND (pyodbc | bcp | arrow_odbc).",
    )
    args = parser.parse_args()

    # Load config from .env
    cfg = load_config()

    # Apply CLI overrides
    if args.mode:
        cfg.ingestion.load_mode = args.mode
    if args.target_table:
        cfg.sqlserver.table = args.target_table
    if args.batch_size:
        cfg.ingestion.batch_size = args.batch_size
    if args.naming_convention:
        cfg.ingestion.naming_convention = args.naming_convention
    if args.mapping:
        cfg.ingestion.mapping_file = args.mapping
    if args.limit is not None:
        cfg.ingestion.row_limit = args.limit
    if args.backend:
        cfg.ingestion.write_backend = args.backend

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, cfg.ingestion.log_level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    # Validate
    if not cfg.sqlserver.database:
        logger.error("SQLSERVER_DATABASE is not set. Set it in .env.")
        sys.exit(1)

    # ---- Resolve tables ----
    tables: list[str] = []

    if args.all_tables:
        # List all tables from Iceberg namespace
        reader = IcebergReader(cfg.iceberg)
        tables = reader.list_tables()
        if not tables:
            logger.error("No tables found in namespace '%s'.", cfg.iceberg.namespace)
            sys.exit(1)
        logger.info("Found %d tables: %s", len(tables), tables)

    elif args.tables:
        tables = args.tables

    elif not args.run:
        # Default: use table from .env
        if not cfg.iceberg.table:
            logger.error(
                "No table specified. Use --table, --all-tables, or set ICEBERG_TABLE in .env."
            )
            sys.exit(1)
        tables = [f"{cfg.iceberg.namespace}.{cfg.iceberg.table}"]

    # ---- Workflow routing ----

    if args.run and args.auto:
        # ONE-SHOT: Generate mapping → save to disk → ingest immediately
        if not tables:
            if not cfg.iceberg.table:
                logger.error("No table specified for --auto mode.")
                sys.exit(1)
            tables = [f"{cfg.iceberg.namespace}.{cfg.iceberg.table}"]

        logger.info("One-shot mode: generating mapping and ingesting ...")
        mapping_data = generate_mappings(cfg, tables)
        run_ingestion(cfg, mapping_data)

    elif args.run:
        # STEP 2: Read mapping config from disk and ingest
        # If no explicit --mapping given, try to resolve the dynamic name
        mapping_file = cfg.ingestion.mapping_file
        if mapping_file == DEFAULT_MAPPING_FILE and args.tables and len(args.tables) == 1:
            _tbl_name = args.tables[0].split(".", 1)[-1]
            candidate = f"{_tbl_name}_mapping.json"
            if Path(candidate).exists():
                mapping_file = candidate
        elif mapping_file == DEFAULT_MAPPING_FILE and args.all_tables:
            candidate = "AllTables_mapping.json"
            if Path(candidate).exists():
                mapping_file = candidate
        mapping_data = load_mapping_file(mapping_file)

        # Resolve table keys
        table_keys: list[str] | None = None
        if args.tables:
            table_keys = []
            for t in args.tables:
                parts = t.split(".", 1)
                if len(parts) == 2:
                    table_keys.append(_table_key(parts[0], parts[1]))
                else:
                    table_keys.append(_table_key(cfg.iceberg.namespace, parts[0]))

        run_ingestion(cfg, mapping_data, table_keys)

    else:
        # STEP 1: Generate mapping only (no ingestion)
        generate_mappings(cfg, tables)


if __name__ == "__main__":
    main()
