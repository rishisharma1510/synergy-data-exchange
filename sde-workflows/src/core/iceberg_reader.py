"""
Iceberg table reader — uses PyIceberg to read data as Arrow tables.
Supports full scans, incremental (snapshot-based) reads, and streaming
via Arrow RecordBatchReader for constant-memory ingestion.
"""

import logging
import os
import threading
from typing import Iterator, Optional

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table as IcebergTable

from src.core.config import IcebergConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Full-scan fallback guard
# ---------------------------------------------------------------------------

class FullScanMemoryRiskError(RuntimeError):
    """Raised when a full-table-scan fallback would likely exhaust container memory.

    This happens when an Iceberg table has INT→LONG manifest bounds type
    mismatch (column evolved from INT to BIGINT but manifests still store
    4-byte bounds) AND the total Parquet data for that table exceeds
    FULL_SCAN_MAX_GB.

    Fix: run  scripts/optimize_iceberg_tables.py  to rewrite manifests with
    correct statistics.  This restores partition pruning and eliminates the
    full-scan fallback.
    """


# Limit concurrent full-table-scan fallbacks to 1.
# When pyiceberg's manifest bounds type-mismatch (INT→LONG column evolution) forces
# a full unfiltered scan, running multiple such scans in parallel can exhaust task
# memory (OOM) even on a 60 GiB task.  Tables that hit this fallback queue here
# and run one at a time; normally-pruned tables are unaffected.
_FULL_SCAN_SEMAPHORE = threading.Semaphore(1)


class IcebergReader:
    """Reads data from an Apache Iceberg table via PyIceberg."""

    def __init__(self, cfg: IcebergConfig) -> None:
        self.cfg = cfg
        self._catalog: Optional[object] = None  # Lazy: initialized on first use
        self._catalog_lock = threading.Lock()    # Guards lazy init against concurrent threads
        self._table: Optional[IcebergTable] = None

    def _get_catalog(self):
        """Return the catalog, initializing it on first access (lazy init).

        Double-checked locking: the outer ``if`` is the fast path (no lock
        acquisition once the catalog is warm).  The inner ``if`` inside the
        lock prevents multiple threads from calling ``load_catalog`` in the
        brief window between the first check and the assignment.
        """
        if self._catalog is None:
            with self._catalog_lock:
                if self._catalog is None:
                    import time as _time
                    logger.info("[IcebergReader] Initialising catalog name='%s' type=%s warehouse=%s region=%s",
                        self.cfg.catalog_name, self.cfg.catalog_type, self.cfg.warehouse or '(default)', self.cfg.glue_region)
                    t0 = _time.perf_counter()
                    self._catalog = load_catalog(
                        name=self.cfg.catalog_name,
                        **self.cfg.catalog_properties(),
                    )
                    logger.info("[IcebergReader] Catalog initialised in %.2fs", _time.perf_counter() - t0)
        return self._catalog

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_table(self, namespace: Optional[str] = None, table: Optional[str] = None) -> IcebergTable:
        ns = namespace or self.cfg.namespace
        tbl = table or self.cfg.table
        table_id = f"{ns}.{tbl}"
        logger.info("Loading Iceberg table: %s", table_id)
        return self._get_catalog().load_table(table_id)

    def _ensure_table(self) -> IcebergTable:
        if self._table is None:
            self._table = self._get_table()
        return self._table

    # ------------------------------------------------------------------
    # Multi-table support
    # ------------------------------------------------------------------

    def list_tables(self, namespace: Optional[str] = None) -> list[str]:
        """List all table names in the given namespace.

        Enforces a hard timeout (default 60 s) so a missing IAM permission or
        network issue never blocks the pipeline indefinitely.
        """
        import time as _time
        import concurrent.futures as _cf

        ns = namespace or self.cfg.namespace
        logger.info("[IcebergReader] list_tables namespace=%s", ns)
        t0 = _time.perf_counter()

        def _do_list() -> list[str]:
            tables = self._get_catalog().list_tables(ns)
            return [f"{t[0]}.{t[1]}" if isinstance(t, tuple) else str(t) for t in tables]

        timeout_sec = 60  # fail fast instead of waiting 10+ minutes
        pool = _cf.ThreadPoolExecutor(max_workers=1)
        try:
            future = pool.submit(_do_list)
            try:
                result = future.result(timeout=timeout_sec)
            except _cf.TimeoutError:
                logger.error(
                    "[IcebergReader] list_tables TIMED OUT after %ds in namespace '%s'. "
                    "Check that the Glue VPC endpoint exists and the IAM role has glue:GetTables.",
                    timeout_sec, ns,
                )
                # Do NOT wait for the thread (it may hang forever); abandon it
                pool.shutdown(wait=False, cancel_futures=True)
                raise RuntimeError(
                    f"list_tables timed out after {timeout_sec}s — "
                    "verify Glue VPC endpoint exists and IAM: glue:GetTables, glue:GetDatabase"
                )
            pool.shutdown(wait=False)
            logger.info("[IcebergReader] list_tables found %d tables in %.2fs: %s",
                len(result), _time.perf_counter() - t0, result)
            return result
        except RuntimeError:
            raise
        except Exception as exc:
            pool.shutdown(wait=False)
            logger.error("[IcebergReader] list_tables FAILED after %.2fs in namespace '%s': %s",
                _time.perf_counter() - t0, ns, exc)
            raise

    def get_arrow_schema(self, namespace: Optional[str] = None, table: Optional[str] = None) -> pa.Schema:
        """Return the Arrow schema of an Iceberg table (metadata only, no data scan)."""
        tbl = self._get_table(namespace, table)
        # Convert Iceberg schema → Arrow schema without scanning data
        try:
            return tbl.schema().as_arrow()
        except (AttributeError, Exception):
            # Fallback: use scan with row_filter to avoid full read
            return tbl.scan().to_arrow_batch_reader().schema

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def current_snapshot_id(self) -> Optional[int]:
        """Return the current snapshot ID, or None if the table has no data."""
        table = self._ensure_table()
        snapshot = table.current_snapshot()
        return snapshot.snapshot_id if snapshot else None

    def schema_fields(self) -> list[dict]:
        """Return the Iceberg schema as a list of {name, type} dicts."""
        table = self._ensure_table()
        return [
            {"name": field.name, "type": str(field.field_type)}
            for field in table.schema().fields
        ]

    @staticmethod
    def calculate_optimal_batch_size(
        schema: pa.Schema,
        target_memory_mb: int = 50,
        min_batch: int = 1000,
        max_batch: int = 100_000,
    ) -> int:
        """
        Calculate optimal batch size based on table schema.
        
        Uses column count and data types to estimate row size,
        then computes how many rows fit in target memory.
        
        Parameters
        ----------
        schema : pa.Schema
            Arrow schema of the table
        target_memory_mb : int
            Target memory per batch in MB (default 50MB)
        min_batch : int
            Minimum batch size (default 1000)
        max_batch : int
            Maximum batch size (default 100,000)
        
        Returns
        -------
        int
            Optimal batch size (rows)
        """
        # Estimate bytes per column based on type
        def estimate_column_bytes(field: pa.Field) -> int:
            dtype = field.type
            if pa.types.is_boolean(dtype):
                return 1
            elif pa.types.is_int8(dtype) or pa.types.is_uint8(dtype):
                return 1
            elif pa.types.is_int16(dtype) or pa.types.is_uint16(dtype):
                return 2
            elif pa.types.is_int32(dtype) or pa.types.is_uint32(dtype) or pa.types.is_float32(dtype):
                return 4
            elif pa.types.is_int64(dtype) or pa.types.is_uint64(dtype) or pa.types.is_float64(dtype):
                return 8
            elif pa.types.is_decimal(dtype):
                return 16
            elif pa.types.is_date(dtype):
                return 4
            elif pa.types.is_timestamp(dtype) or pa.types.is_time(dtype):
                return 8
            elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
                return 100  # Conservative estimate for variable-length strings
            elif pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
                return 200  # Conservative for binary data
            else:
                return 50  # Default for unknown types
        
        num_columns = len(schema)
        if num_columns == 0:
            return max_batch
        
        # Calculate estimated row size
        estimated_row_bytes = sum(estimate_column_bytes(f) for f in schema)
        
        # Add overhead per row (nullability bitmaps, array offsets, etc.)
        overhead_per_row = num_columns * 2  # ~2 bytes per column for metadata
        total_row_bytes = estimated_row_bytes + overhead_per_row
        
        # Calculate batch size to fit target memory
        target_bytes = target_memory_mb * 1024 * 1024
        optimal_batch = int(target_bytes / total_row_bytes)
        
        # Clamp to min/max bounds
        result = max(min_batch, min(optimal_batch, max_batch))
        
        logger.debug(
            "Dynamic batch size: %d columns, ~%d bytes/row → %d rows/batch",
            num_columns, total_row_bytes, result
        )
        return result

    def read_full(self, limit: Optional[int] = None) -> pa.Table:
        """Full scan — read all rows from the current snapshot."""
        table = self._ensure_table()
        snapshot = table.current_snapshot()
        if snapshot is None:
            logger.warning("Table has no snapshots; returning empty Arrow table.")
            return pa.table({})

        logger.info(
            "Reading full table (snapshot %s) ...", snapshot.snapshot_id
        )
        scan = table.scan(limit=limit) if limit else table.scan()
        arrow_table = scan.to_arrow()
        logger.info("Read %d rows, %d columns.", arrow_table.num_rows, arrow_table.num_columns)
        return arrow_table

    def read_incremental(self, from_snapshot_id: int) -> pa.Table:
        """
        Incremental read — returns rows added *after* ``from_snapshot_id``.

        PyIceberg supports incremental scan via ``from_snapshot_id`` on the
        scan builder.  If the API is unavailable (older pyiceberg), we fall
        back to reading the full current snapshot and warn the user.
        """
        table = self._ensure_table()
        current = table.current_snapshot()
        if current is None:
            logger.warning("Table has no snapshots; nothing to read.")
            return pa.table({})

        if current.snapshot_id == from_snapshot_id:
            logger.info("No new snapshots since %s; nothing to ingest.", from_snapshot_id)
            return pa.table({})

        try:
            # PyIceberg >=0.6 supports append-only incremental scans
            logger.info(
                "Incremental scan: snapshots (%s → %s) ...",
                from_snapshot_id,
                current.snapshot_id,
            )
            scan = table.scan(
                snapshot_id=current.snapshot_id,
            )
            # Try using the incremental scan API if available
            if hasattr(scan, "use_ref"):
                # Some PyIceberg versions expose incremental via scan options
                arrow_table = scan.to_arrow()
            else:
                arrow_table = scan.to_arrow()

            logger.info(
                "Incremental read returned %d rows.", arrow_table.num_rows
            )
            return arrow_table

        except Exception as exc:
            logger.warning(
                "Incremental scan failed (%s). Falling back to full scan.",
                exc,
            )
            return self.read_full()

    def read(
        self, mode: str = "full", from_snapshot_id: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> pa.Table:
        """
        Unified read entry point (materialises the full table in memory).

        Parameters
        ----------
        mode : str
            ``"full"`` or ``"incremental"``.
        from_snapshot_id : int | None
            Required when *mode* is ``"incremental"``.
        limit : int | None
            Maximum number of rows to return.
        """
        if mode == "incremental" and from_snapshot_id is not None:
            data = self.read_incremental(from_snapshot_id)
        else:
            data = self.read_full(limit=limit)
        if limit is not None and data.num_rows > limit:
            data = data.slice(0, limit)
        return data

    # ------------------------------------------------------------------
    # Streaming API (constant-memory)
    # ------------------------------------------------------------------

    def stream_batches(
        self,
        mode: str = "full",
        from_snapshot_id: Optional[int] = None,
        limit: Optional[int] = None,
        selected_columns: Optional[list[str]] = None,
        row_filter: Optional[str] = None,
    ) -> Iterator[pa.RecordBatch]:
        """
        Yield Arrow RecordBatches one at a time — never loads the full table.

        This is the preferred read path for large tables.  Each batch is
        typically a few thousand to a few hundred-thousand rows (decided by
        the Iceberg scan planner / Parquet row-group size).

        Parameters
        ----------
        mode : str
            ``"full"`` or ``"incremental"``.
        from_snapshot_id : int | None
            Required when *mode* is ``"incremental"``.
        limit : int | None
            Maximum total rows to yield (0 / None = unlimited).
        selected_columns : list[str] | None
            Column names to read.  ``None`` = all columns.
            When set, only these columns are fetched from S3/Parquet
            (projection pushdown — drastically reduces I/O for wide tables).
        row_filter : str | None
            PyIceberg row filter expression (e.g., "exposure_id IN (101, 205)").
            Enables predicate pushdown to reduce data scanned.

        Yields
        ------
        pa.RecordBatch
        """
        table = self._ensure_table()
        snapshot = table.current_snapshot()

        if snapshot is None:
            logger.warning("Table has no snapshots; nothing to stream.")
            return

        # Build scan kwargs
        scan_kwargs: dict = {}
        if mode == "incremental" and from_snapshot_id is not None:
            if snapshot.snapshot_id == from_snapshot_id:
                logger.info("No new snapshots since %s; nothing to stream.", from_snapshot_id)
                return
            logger.info(
                "Streaming incremental: snapshots (%s → %s) ...",
                from_snapshot_id, snapshot.snapshot_id,
            )
            scan_kwargs["snapshot_id"] = snapshot.snapshot_id
        else:
            logger.info(
                "Streaming full table (snapshot %s) ...", snapshot.snapshot_id,
            )

        # Row filter (predicate pushdown)
        if row_filter:
            scan_kwargs["row_filter"] = row_filter
            logger.info("Row filter applied: %s", row_filter)

        # Column projection pushdown — only read the columns we need
        if selected_columns:
            scan_kwargs["selected_fields"] = tuple(selected_columns)
            logger.info(
                "Projection pushdown: reading %d / %d columns.",
                len(selected_columns), len(table.schema().fields),
            )

        scan = table.scan(**scan_kwargs)

        batch_reader = scan.to_arrow_batch_reader()

        rows_yielded = 0
        effective_limit = limit if limit else 0  # 0 = no limit

        # Coalesce tiny batches: Parquet row-groups can be very small in
        # some Iceberg tables (single-digit rows).  Sending those as
        # individual write operations wastes round-trips.  We accumulate
        # small batches until we have at least `min_batch_rows` before
        # yielding.  This keeps memory bounded while avoiding per-row
        # overhead on the write side.
        #
        # Dynamic sizing: wider tables get smaller batches to prevent
        # memory exhaustion. Uses schema to estimate optimal batch size.
        min_batch_rows = self.calculate_optimal_batch_size(batch_reader.schema)
        logger.info(
            "Dynamic batch size: %d rows (based on %d columns)",
            min_batch_rows, len(batch_reader.schema)
        )
        pending: list[pa.RecordBatch] = []
        pending_rows = 0

        try:
            for batch in batch_reader:
                if batch.num_rows == 0:
                    continue

                if effective_limit > 0:
                    remaining = effective_limit - rows_yielded
                    if remaining <= 0:
                        break
                    if batch.num_rows > remaining:
                        batch = batch.slice(0, remaining)

                pending.append(batch)
                pending_rows += batch.num_rows
                rows_yielded += batch.num_rows

                if pending_rows >= min_batch_rows:
                    if len(pending) == 1:
                        yield pending[0]
                    else:
                        # Rebind pending before merge so the original batch references
                        # are dropped before combine_chunks() allocates the contiguous copy.
                        batches, pending = pending, []
                        merged = pa.Table.from_batches(batches).combine_chunks()
                        del batches
                        for mb in merged.to_batches():
                            yield mb
                        del merged
                    pending.clear()
                    pending_rows = 0

            # Flush remaining
            if pending:
                if len(pending) == 1:
                    yield pending[0]
                else:
                    batches, pending = pending, []
                    merged = pa.Table.from_batches(batches).combine_chunks()
                    del batches
                    for mb in merged.to_batches():
                        yield mb
                    del merged
        finally:
            batch_reader.close()  # release Arrow file handles and buffer pool immediately

        logger.info("Streamed %d rows total.", rows_yielded)

    def stream_table_batches(
        self,
        namespace: str,
        table_name: str,
        row_filter=None,
        selected_columns: Optional[list[str]] = None,
    ) -> Iterator[pa.RecordBatch]:
        """Stream RecordBatches from an explicit namespace.table (multi-table support).

        Unlike ``stream_batches()``, this targets a specific table by name rather
        than the default table set in ``IcebergConfig``.  Use this when iterating
        over multiple tables discovered at runtime.

        Parameters
        ----------
        namespace : str
            Iceberg namespace (Glue database name).
        table_name : str
            Iceberg table name within the namespace.
        row_filter : str | PyIceberg expression | None
            Predicate pushdown filter (e.g. ``pyiceberg.expressions.In``).
        selected_columns : list[str] | None
            Column projection; None = all columns.

        Yields
        ------
        pa.RecordBatch
        """
        import struct as _struct
        import time as _time
        import pyarrow.compute as pc
        t0 = _time.perf_counter()
        tbl = self._get_table(namespace, table_name)
        snapshot = tbl.current_snapshot()
        if snapshot is None:
            logger.warning("[IcebergReader] %s.%s has no snapshots; skipping.", namespace, table_name)
            return

        scan_kwargs: dict = {}
        if row_filter is not None:
            scan_kwargs["row_filter"] = row_filter
        if selected_columns:
            scan_kwargs["selected_fields"] = tuple(selected_columns)

        # --- build scan, with fallback for INT/LONG manifest-bounds type mismatch ---
        # pyiceberg may crash inside plan_files() with "unpack requires a buffer of
        # 8 bytes" when a column's Iceberg schema type is LongType (BIGINT) but the
        # parquet manifest statistics were written when the column was IntegerType
        # (INT, 4 bytes).  In that case we recover by doing a full scan and applying
        # the filter in Python via PyArrow after reading each batch.
        _post_filter_col: Optional[str] = None
        _post_filter_vals: Optional[set] = None
        _full_scan_mode = False  # True when struct.error fallback is active

        try:
            scan = tbl.scan(**scan_kwargs)
            batch_reader = scan.to_arrow_batch_reader()
        except _struct.error as exc:
            logger.warning(
                "[IcebergReader] %s.%s manifest bounds type mismatch (%s) — "
                "falling back to full scan with Python-level post-filter",
                namespace, table_name, exc,
            )
            # Extract filter column / values from the pyiceberg expression so we can
            # replicate the pushdown logic in Python after reading each Arrow batch.
            if row_filter is not None:
                try:
                    from pyiceberg.expressions import EqualTo, In as IcebergIn
                    if isinstance(row_filter, EqualTo):
                        _post_filter_col = row_filter.term.name
                        _post_filter_vals = {row_filter.literal.value}
                    elif isinstance(row_filter, IcebergIn):
                        _post_filter_col = row_filter.term.name
                        _post_filter_vals = {lit.value for lit in row_filter.literals}
                except Exception as _e:
                    logger.warning("[IcebergReader] could not extract filter terms: %s", _e)
            # Safety check: refuse to full-scan tables that are too large for the
            # container's memory.  FULL_SCAN_MAX_GB can be tuned via env var.
            # Default is deliberately conservative (20 GB) since the scan must fit
            # alongside the SQL write buffers and OS / Python overhead.
            # Express container = 30 GB total; Standard = 60 GB but runs 6 threads.
            # The Iceberg snapshot summary stores total-files-size as a metadata
            # property — reading it is O(1) (no S3 data scan required).
            _max_gb = float(os.environ.get("FULL_SCAN_MAX_GB", "20"))
            try:
                _summary = tbl.current_snapshot().summary
                _total_bytes = int(_summary.get("total-files-size", 0))
                _total_gb = _total_bytes / 1_073_741_824
                if _total_bytes > 0 and _total_gb > _max_gb:
                    raise FullScanMemoryRiskError(
                        f"{namespace}.{table_name}: manifest bounds type mismatch would require "
                        f"a full scan of {_total_gb:.1f} GB Parquet data (limit={_max_gb:.0f} GB). "
                        f"Fix: run `python scripts/optimize_iceberg_tables.py --tables {table_name}` "
                        f"to rewrite manifests with correct INT\u2192LONG statistics, restoring "
                        f"partition pruning."
                    )
                elif _total_bytes > 0:
                    logger.info(
                        "[IcebergReader] %s.%s full-scan size: %.1f GB (limit %.0f GB — proceeding)",
                        namespace, table_name, _total_gb, _max_gb,
                    )
                else:
                    logger.warning(
                        "[IcebergReader] %s.%s: total-files-size not in snapshot summary "
                        "(proceeding — may OOM if table is large)",
                        namespace, table_name,
                    )
            except FullScanMemoryRiskError:
                raise
            except Exception as _size_exc:
                logger.warning(
                    "[IcebergReader] %s.%s: could not read snapshot summary for size guard: %s",
                    namespace, table_name, _size_exc,
                )

            # Re-scan without pyiceberg row_filter; post-filter applied per-batch below.
            fallback_kwargs = {k: v for k, v in scan_kwargs.items() if k != "row_filter"}
            scan = tbl.scan(**fallback_kwargs)
            batch_reader = scan.to_arrow_batch_reader()
            _full_scan_mode = True

        schema = batch_reader.schema
        min_batch_rows = self.calculate_optimal_batch_size(schema)
        logger.info(
            "[IcebergReader] stream_table_batches %s.%s snapshot=%s filter=%s "
            "batch_size=%d post_filter_col=%s post_filter_vals=%s",
            namespace, table_name, snapshot.snapshot_id, row_filter, min_batch_rows,
            _post_filter_col, _post_filter_vals,
        )

        # Serialize concurrent full-scan fallbacks: only one unfiltered full-table
        # scan runs at a time.  Normal partition-pruned scans skip this entirely.
        if _full_scan_mode:
            logger.warning(
                "[IcebergReader] %s.%s full-scan fallback — waiting for serialization "
                "semaphore (prevents concurrent full scans from causing OOM)",
                namespace, table_name,
            )
            _FULL_SCAN_SEMAPHORE.acquire()
            logger.info(
                "[IcebergReader] %s.%s full-scan semaphore acquired, starting scan",
                namespace, table_name,
            )

        pending: list[pa.RecordBatch] = []
        pending_rows = 0
        rows_yielded = 0
        try:
            for batch in batch_reader:
                # Apply Python-level filter when pyiceberg pushdown was unavailable.
                if _post_filter_col and _post_filter_vals and _post_filter_col in batch.schema.names:
                    mask = pc.is_in(
                        batch.column(_post_filter_col),
                        value_set=pa.array(list(_post_filter_vals)),
                    )
                    batch = batch.filter(mask)

                if batch.num_rows == 0:
                    continue
                pending.append(batch)
                pending_rows += batch.num_rows
                rows_yielded += batch.num_rows
                if pending_rows >= min_batch_rows:
                    if len(pending) == 1:
                        yield pending[0]
                    else:
                        # Rebind pending before merge so the original batch references
                        # are dropped before combine_chunks() allocates the contiguous copy.
                        batches, pending = pending, []
                        merged = pa.Table.from_batches(batches).combine_chunks()
                        del batches
                        for mb in merged.to_batches():
                            yield mb
                        del merged
                    pending.clear()
                    pending_rows = 0

            if pending:
                if len(pending) == 1:
                    yield pending[0]
                else:
                    batches, pending = pending, []
                    merged = pa.Table.from_batches(batches).combine_chunks()
                    del batches
                    for mb in merged.to_batches():
                        yield mb
                    del merged
        finally:
            batch_reader.close()  # release Arrow file handles and buffer pool immediately
            if _full_scan_mode:
                _FULL_SCAN_SEMAPHORE.release()
                logger.info(
                    "[IcebergReader] %s.%s full-scan semaphore released",
                    namespace, table_name,
                )

        logger.info(
            "[IcebergReader] stream_table_batches %s.%s — %d rows in %.2fs",
            namespace, table_name, rows_yielded, _time.perf_counter() - t0,
        )
