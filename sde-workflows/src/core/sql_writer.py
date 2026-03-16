"""
SQL Server writer — creates/validates the target table and bulk-inserts
data from PyArrow tables.

Supports three write backends (configurable via WRITE_BACKEND):
  - pyodbc   — fast_executemany with TABLOCK hint (default, no extra deps)
  - arrow_odbc — zero-copy columnar bulk insert via arrow-odbc (fastest)
  - bcp      — BCP bulk-copy utility via temp file (uses pyarrow.csv)

All backends accept streaming RecordBatch iterators so the full dataset
never needs to be materialised in memory at once.
"""

import logging
import os
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import date, datetime, time
from decimal import Decimal
from typing import Iterator, Optional

import pyarrow as pa
import pyarrow.csv as pcsv
import pyodbc

from src.core.config import SqlServerConfig
from src.core.column_mapper import ColumnMapper, MappingResult, ColumnMatch

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# Arrow → SQL Server type mapping
# -------------------------------------------------------------------------

_ARROW_TO_SQL: dict[str, str] = {
    "bool": "BIT",
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INT",
    "int64": "BIGINT",
    "uint8": "SMALLINT",
    "uint16": "INT",
    "uint32": "BIGINT",
    "uint64": "BIGINT",
    "float16": "REAL",
    "float32": "REAL",
    "float": "FLOAT",
    "float64": "FLOAT",
    "double": "FLOAT",
    "string": "NVARCHAR(MAX)",
    "large_string": "NVARCHAR(MAX)",
    "utf8": "NVARCHAR(MAX)",
    "large_utf8": "NVARCHAR(MAX)",
    "binary": "VARBINARY(MAX)",
    "large_binary": "VARBINARY(MAX)",
    "date32": "DATE",
    "date32[day]": "DATE",
    "date64": "DATE",
    "timestamp[us]": "DATETIME2",
    "timestamp[ns]": "DATETIME2",
    "timestamp[ms]": "DATETIME2",
    "timestamp[s]": "DATETIME2",
    "timestamp[us, tz=UTC]": "DATETIMEOFFSET",
    "timestamp[ns, tz=UTC]": "DATETIMEOFFSET",
    "time32[ms]": "TIME",
    "time64[us]": "TIME",
    "decimal128": "DECIMAL(38, 18)",
}


def _map_arrow_type(arrow_type: pa.DataType) -> str:
    """Convert an Arrow data type to the closest SQL Server type."""
    type_str = str(arrow_type)

    # Direct match
    if type_str in _ARROW_TO_SQL:
        return _ARROW_TO_SQL[type_str]

    # Decimal with precision/scale
    if isinstance(arrow_type, pa.Decimal128Type):
        return f"DECIMAL({arrow_type.precision}, {arrow_type.scale})"

    # Timestamp with timezone
    if "timestamp" in type_str:
        if "tz=" in type_str:
            return "DATETIMEOFFSET"
        return "DATETIME2"

    # Fixed-size binary
    if isinstance(arrow_type, pa.FixedSizeBinaryType):
        return f"BINARY({arrow_type.byte_width})"

    # List / struct / map → store as JSON string
    if isinstance(arrow_type, (pa.ListType, pa.StructType, pa.MapType)):
        return "NVARCHAR(MAX)"

    logger.warning("Unmapped Arrow type '%s'; defaulting to NVARCHAR(MAX).", type_str)
    return "NVARCHAR(MAX)"


def calculate_insert_batch_size(
    num_columns: int,
    target_batch_mb: int = 10,
    min_batch: int = 500,
    max_batch: int = 50_000,
) -> int:
    """
    Calculate optimal INSERT batch size based on column count.
    
    Wider tables need smaller batches to avoid:
    - Memory exhaustion during parameter binding
    - SQL Server query plan compilation overhead
    - Network packet size limits
    
    Parameters
    ----------
    num_columns : int
        Number of columns in target table
    target_batch_mb : int
        Target memory per batch in MB (default 10MB for INSERTs)
    min_batch : int
        Minimum batch size (default 500)
    max_batch : int
        Maximum batch size (default 50,000)
    
    Returns
    -------
    int
        Optimal batch size (rows per INSERT)
    """
    if num_columns <= 0:
        return max_batch
    
    # Estimate ~100 bytes per column per row (conservative for pyodbc binding)
    estimated_bytes_per_row = num_columns * 100
    target_bytes = target_batch_mb * 1024 * 1024
    optimal = int(target_bytes / estimated_bytes_per_row)
    
    result = max(min_batch, min(optimal, max_batch))
    logger.debug(
        "INSERT batch size: %d columns → %d rows/batch",
        num_columns, result
    )
    return result


class SqlServerWriter:
    """Writes Arrow tables to a SQL Server table."""

    def __init__(self, cfg: SqlServerConfig, naming_convention: str = "PascalCase") -> None:
        self.cfg = cfg
        self._conn: Optional[pyodbc.Connection] = None
        self._mapper = ColumnMapper(naming_convention=naming_convention)
        self._mapping_result: Optional[MappingResult] = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> pyodbc.Connection:
        if self._conn is None:
            logger.info("Connecting to SQL Server %s/%s ...", self.cfg.host, self.cfg.database)
            self._conn = pyodbc.connect(self.cfg.connection_string(), autocommit=False)
        return self._conn

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # DDL helpers
    # ------------------------------------------------------------------

    def _full_table_name(self) -> str:
        return f"[{self.cfg.schema}].[{self.cfg.table}]"

    def table_exists(self) -> bool:
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """,
            self.cfg.schema,
            self.cfg.table,
        )
        return cursor.fetchone() is not None

    def get_target_columns(self) -> list[dict]:
        """Query INFORMATION_SCHEMA for existing target table columns."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                   CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """,
            self.cfg.schema,
            self.cfg.table,
        )
        return [
            {
                "name": row.COLUMN_NAME,
                "type": row.DATA_TYPE.upper(),
                "nullable": row.IS_NULLABLE == "YES",
                "max_length": row.CHARACTER_MAXIMUM_LENGTH,
                "precision": row.NUMERIC_PRECISION,
                "scale": row.NUMERIC_SCALE,
            }
            for row in cursor.fetchall()
        ]

    def create_table(self, arrow_schema: pa.Schema) -> None:
        """Create the target SQL Server table using mapped column names."""
        # Apply naming convention to generate target column names
        mapped_schema = self._mapper.transform_schema(arrow_schema)
        cols = []
        for field in mapped_schema:
            sql_type = _map_arrow_type(field.type)
            nullable = "NULL" if field.nullable else "NOT NULL"
            cols.append(f"    [{field.name}] {sql_type} {nullable}")

        ddl = (
            f"CREATE TABLE {self._full_table_name()} (\n"
            + ",\n".join(cols)
            + "\n);"
        )
        logger.info("Creating table:\n%s", ddl)
        conn = self.connect()
        conn.cursor().execute(ddl)
        conn.commit()

        # Build mapping result (trivial — all transformed names)
        self._mapping_result = MappingResult(
            matched=[
                ColumnMatch(
                    source_name=src.name,
                    target_name=tgt.name,
                    strategy="naming-convention",
                    confidence=1.0,
                    source_type=str(src.type),
                    target_type=_map_arrow_type(src.type),
                )
                for src, tgt in zip(arrow_schema, mapped_schema)
            ]
        )
        self._mapping_result.log_summary()

    def resolve_mapping(self, arrow_schema: pa.Schema) -> MappingResult:
        """
        If the target table exists, auto-map columns.
        If not, create it and return the naming-convention mapping.
        """
        if not self.table_exists():
            logger.info("Target table does not exist — creating it.")
            self.create_table(arrow_schema)
        else:
            logger.info("Target table exists — auto-mapping columns ...")
            target_cols = self.get_target_columns()
            self._mapping_result = self._mapper.auto_map(arrow_schema, target_cols)
            self._mapping_result.log_summary()

            if self._mapping_result.unmatched_source:
                logger.warning(
                    "These source columns have no match and will be SKIPPED: %s",
                    self._mapping_result.unmatched_source,
                )
            if self._mapping_result.unmatched_target:
                logger.info(
                    "These target columns have no source (will be NULL): %s",
                    self._mapping_result.unmatched_target,
                )

        assert self._mapping_result is not None
        return self._mapping_result

    def ensure_table(self, arrow_schema: pa.Schema) -> None:
        """Create the table if it does not exist; resolve column mapping."""
        self.resolve_mapping(arrow_schema)

    def create_table_from_mapping(self, mapping_columns: list[dict]) -> None:
        """Create the target SQL Server table from a mapping config entry."""
        cols = []
        for col in mapping_columns:
            if not col.get("include", True) or not col.get("target"):
                continue
            sql_type = col.get("target_type", "NVARCHAR(MAX)")
            cols.append(f"    [{col['target']}] {sql_type} NULL")

        if not cols:
            raise ValueError("No included columns in mapping — cannot create table.")

        ddl = (
            f"CREATE TABLE {self._full_table_name()} (\n"
            + ",\n".join(cols)
            + "\n);"
        )
        logger.info("Creating table from mapping config:\n%s", ddl)
        conn = self.connect()
        conn.cursor().execute(ddl)
        conn.commit()

    @staticmethod
    def _arrow_to_rows(arrow_data: pa.Table | pa.RecordBatch) -> list[tuple]:
        """Convert an Arrow Table/RecordBatch to a list of tuples (fast path).

        Uses ``to_pylist()`` which delegates to Arrow's C++ layer and is
        10-50x faster than cell-by-cell ``.as_py()`` calls.
        """
        return [tuple(row.values()) for row in arrow_data.to_pylist()]

    @staticmethod
    def _bcp_schema_compatible(schema: pa.Schema) -> bool:
        """Quick schema-level check — reject types BCP can't handle."""
        for field in schema:
            arrow_type = field.type
            if isinstance(arrow_type, (pa.ListType, pa.StructType, pa.MapType, pa.FixedSizeBinaryType)):
                logger.warning(
                    "BCP backend does not support nested/fixed-binary column '%s' (%s).",
                    field.name, arrow_type,
                )
                return False
            if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
                logger.warning(
                    "BCP backend does not support binary column '%s' (%s).",
                    field.name, arrow_type,
                )
                return False
        return True

    @staticmethod
    def _bcp_batch_strings_safe(
        batch: pa.Table | pa.RecordBatch,
        field_terminator: str,
        row_terminator: str,
    ) -> bool:
        """Check string columns in a *single batch* for delimiter collisions."""
        row_marker = row_terminator.replace("\r", "").replace("\n", "")
        for field in batch.schema:
            if not (pa.types.is_string(field.type) or pa.types.is_large_string(field.type)):
                continue
            col = batch.column(field.name)
            values = col.to_pylist()
            if any(v is not None and field_terminator in v for v in values):
                logger.warning(
                    "BCP field terminator found in column '%s'; falling back to pyodbc.",
                    field.name,
                )
                return False
            if row_marker and any(v is not None and row_marker in v for v in values):
                logger.warning(
                    "BCP row terminator marker found in column '%s'; falling back to pyodbc.",
                    field.name,
                )
                return False
        return True

    @staticmethod
    def _bcp_serialize_value(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, (datetime, date, time)):
            return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        return str(value)

    def _write_with_bcp(
        self,
        arrow_subset: pa.Table,
        *,
        bcp_path: str,
        bcp_batch_size: int,
        bcp_temp_dir: str,
        bcp_field_terminator: str,
        bcp_row_terminator: str,
    ) -> int:
        if self.cfg.auth_type == "azure_ad":
            raise RuntimeError("BCP backend does not support SQLSERVER_AUTH_TYPE=azure_ad in this script.")

        bcp_exe = shutil.which(bcp_path) if bcp_path == "bcp" else bcp_path
        if not bcp_exe:
            raise RuntimeError(
                "BCP executable not found. Install SQL Server command line tools or set BCP_PATH."
            )

        if not self._bcp_schema_compatible(arrow_subset.schema):
            raise RuntimeError("Schema contains types incompatible with BCP.")

        if not self._bcp_batch_strings_safe(arrow_subset, bcp_field_terminator, bcp_row_terminator):
            raise RuntimeError("Data is not compatible with the configured BCP delimiters.")

        temp_dir = bcp_temp_dir or None

        # --- Fast temp-file generation via pyarrow.csv ---
        fd, temp_file = tempfile.mkstemp(suffix=".bcp.txt", dir=temp_dir)
        os.close(fd)
        try:
            # pyarrow.csv only supports single-char delimiters. If the
            # configured field_terminator is a single char, use it directly.
            # Otherwise fall back to the (still fast) row-based writer.
            if len(bcp_field_terminator) == 1 and bcp_row_terminator == "\n":
                write_opts = pcsv.WriteOptions(
                    include_header=False,
                    delimiter=bcp_field_terminator,
                )
                pcsv.write_csv(self._cast_to_string_table(arrow_subset), temp_file, write_options=write_opts)
            else:
                # Multi-char delimiters: use bulk to_pylist (still far faster
                # than the old cell-by-cell loop).
                rows = arrow_subset.to_pylist()
                with open(temp_file, "w", encoding="utf-8", newline="") as fh:
                    for row in rows:
                        fh.write(bcp_field_terminator.join(
                            self._bcp_serialize_value(v) for v in row.values()
                        ))
                        fh.write(bcp_row_terminator)
        except Exception:
            os.remove(temp_file)
            raise

        table_name = f"{self.cfg.database}.{self.cfg.schema}.{self.cfg.table}"
        server = f"{self.cfg.host},{self.cfg.port}"

        # Use 3-part name (db.schema.table) — do NOT also pass -d, as
        # BCP rejects the combination.
        cmd = [
            bcp_exe,
            table_name,
            "in",
            temp_file,
            "-S",
            server,
            "-q",
            "-c",
            "-C",
            "65001",
            "-t",
            bcp_field_terminator,
            "-r",
            bcp_row_terminator,
            "-b",
            str(max(1, bcp_batch_size)),
            "-h",
            "TABLOCK",
            "-k",
        ]

        if self.cfg.auth_type == "windows":
            cmd.append("-T")
        else:
            cmd.extend(["-U", self.cfg.username, "-P", self.cfg.password])

        logger.info("Loading via BCP: %s", " ".join(cmd[:6]) + " ...")
        try:
            result = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                details = (result.stdout or "") + "\n" + (result.stderr or "")
                raise RuntimeError(f"BCP load failed (exit={result.returncode}). {details.strip()}")
        finally:
            try:
                os.remove(temp_file)
            except Exception:
                logger.warning("Could not delete temp BCP file: %s", temp_file)

        return arrow_subset.num_rows

    @staticmethod
    def _cast_to_string_table(arrow_data: pa.Table) -> pa.Table:
        """Cast all columns to string for CSV/BCP serialisation."""
        cast_fields = []
        needs_cast = False
        for field in arrow_data.schema:
            if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                cast_fields.append(field)
            else:
                cast_fields.append(field.with_type(pa.string()))
                needs_cast = True
        if needs_cast:
            return arrow_data.cast(pa.schema(cast_fields))
        return arrow_data

    def write_with_mapping(
        self,
        arrow_table: pa.Table,
        mapping_result: MappingResult,
        *,
        batch_size: int = 5000,
        truncate_first: bool = False,
        write_backend: str = "pyodbc",
        bcp_path: str = "bcp",
        bcp_batch_size: int = 50000,
        bcp_temp_dir: str = "",
        bcp_field_terminator: str = "|~|",
        bcp_row_terminator: str = "~|~\n",
    ) -> int:
        """
        Insert rows using an explicit MappingResult (from mapping config).

        Unlike ``write()``, this does NOT auto-resolve mappings —
        it uses exactly the MappingResult provided.
        """
        if arrow_table.num_rows == 0:
            logger.info("No rows to write.")
            return 0

        if truncate_first and self.table_exists():
            self.truncate_table()

        col_map = mapping_result.source_to_target()
        matched_source_cols = [m.source_name for m in mapping_result.matched]
        matched_target_cols = [col_map[s] for s in matched_source_cols]

        # Filter to only source columns that actually exist in the arrow table
        available_source = set(arrow_table.column_names)
        valid_pairs = [
            (src, tgt)
            for src, tgt in zip(matched_source_cols, matched_target_cols)
            if src in available_source
        ]

        if not valid_pairs:
            logger.error("No columns matched between source data and mapping config — aborting.")
            return 0

        src_cols = [p[0] for p in valid_pairs]
        tgt_cols = [p[1] for p in valid_pairs]

        arrow_subset = arrow_table.select(src_cols)
        total_rows = arrow_subset.num_rows

        # --- BCP path ---
        if write_backend.lower() == "bcp":
            logger.info("WRITE_BACKEND=bcp selected.")
            try:
                inserted = self._write_with_bcp(
                    arrow_subset,
                    bcp_path=bcp_path,
                    bcp_batch_size=bcp_batch_size,
                    bcp_temp_dir=bcp_temp_dir,
                    bcp_field_terminator=bcp_field_terminator,
                    bcp_row_terminator=bcp_row_terminator,
                )
                logger.info("Finished writing %d rows via BCP to %s.", inserted, self._full_table_name())
                return inserted
            except Exception as exc:
                logger.warning("BCP path failed (%s). Falling back to pyodbc executemany.", exc)

        # --- pyodbc fast_executemany path ---
        conn = self.connect()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.execute("SET NOCOUNT ON")

        target_col_names = [f"[{name}]" for name in tgt_cols]
        placeholders = ", ".join(["?"] * len(target_col_names))
        insert_sql = (
            f"INSERT INTO {self._full_table_name()} "
            f"({', '.join(target_col_names)}) VALUES ({placeholders})"
        )
        logger.info("INSERT SQL: %s", insert_sql)

        inserted = 0
        for start in range(0, total_rows, batch_size):
            batch_len = min(batch_size, total_rows - start)
            batch_table = arrow_subset.slice(start, batch_len)
            batch = self._arrow_to_rows(batch_table)
            try:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                inserted += len(batch)
                logger.info(
                    "Inserted batch %d-%d  (%d / %d rows)",
                    start, start + len(batch) - 1, inserted, total_rows,
                )
            except Exception:
                conn.rollback()
                logger.exception("Failed to insert batch starting at row %d.", start)
                raise

        logger.info("Finished writing %d rows to %s.", inserted, self._full_table_name())
        return inserted

    # ------------------------------------------------------------------
    # arrow-odbc backend helpers
    # ------------------------------------------------------------------

    def _write_batch_arrow_odbc(
        self,
        record_batch: pa.RecordBatch,
        target_cols: list[str],
    ) -> int:
        """Write a single RecordBatch via the arrow-odbc package (zero-copy).

        arrow-odbc sends Arrow columnar buffers directly over ODBC without
        converting to Python objects first.  For wide / large-row tables this
        is typically 5-20x faster than ``fast_executemany``.
        """
        try:
            from arrow_odbc import insert_into_table  # type: ignore[import-untyped]
        except ImportError:
            raise ImportError(
                "The 'arrow-odbc' package is required for WRITE_BACKEND=arrow_odbc. "
                "Install it with: pip install arrow-odbc"
            )

        # arrow-odbc expects a RecordBatch whose column names match the
        # *target* table exactly — rename source cols → target cols.
        renamed = record_batch
        if record_batch.schema.names != target_cols:
            arrays = [record_batch.column(i) for i in range(record_batch.num_columns)]
            renamed = pa.RecordBatch.from_arrays(arrays, names=target_cols)

        # Build the connection string (arrow-odbc uses raw ODBC strings)
        conn_str = self.cfg.connection_string()

        # Use a reasonable chunk_size (not the full batch!) to avoid
        # overwhelming the ODBC driver and spiking memory.
        chunk_size = min(5_000, renamed.num_rows)

        # Schema-qualified table name so arrow-odbc targets the right table.
        table_name = f"{self.cfg.schema}.{self.cfg.table}"

        reader = pa.RecordBatchReader.from_batches(renamed.schema, [renamed])
        insert_into_table(
            reader=reader,
            chunk_size=chunk_size,
            table=table_name,
            connection_string=conn_str,
        )
        return renamed.num_rows

    # ------------------------------------------------------------------
    # Streaming write (constant-memory)
    # ------------------------------------------------------------------

    def write_batches_streaming(
        self,
        batch_iter: Iterator[pa.RecordBatch],
        mapping_result: MappingResult,
        *,
        batch_size: int = 50_000,
        truncate_first: bool = False,
        write_backend: str = "pyodbc",
        bcp_path: str = "bcp",
        bcp_batch_size: int = 50000,
        bcp_temp_dir: str = "",
        bcp_field_terminator: str = "|~|",
        bcp_row_terminator: str = "~|~\n",
    ) -> int:
        """
        Stream-write RecordBatches directly — never materialises the full
        dataset in memory.

        Each batch from *batch_iter* is selected/mapped and written
        immediately, then discarded.

        Parameters
        ----------
        batch_iter : Iterator[pa.RecordBatch]
            Yields RecordBatch objects (typically from ``IcebergReader.stream_batches``).
        mapping_result : MappingResult
            Column mapping (from mapping config).
        batch_size : int
            Max rows per INSERT call (sub-batching within each RecordBatch).
            Default raised to 50 000 for better throughput.
        truncate_first : bool
            Truncate the target table before the first write.
        write_backend : str
            ``"pyodbc"``, ``"arrow_odbc"``, or ``"bcp"``.

        Returns
        -------
        int
            Total rows inserted.
        """
        if truncate_first and self.table_exists():
            self.truncate_table()

        col_map = mapping_result.source_to_target()
        matched_source_cols = [m.source_name for m in mapping_result.matched]
        matched_target_cols = [col_map[s] for s in matched_source_cols]

        backend = write_backend.lower()
        use_bcp = backend == "bcp"
        use_arrow_odbc = backend == "arrow_odbc"

        # State initialised lazily on first batch
        src_cols: list[str] | None = None
        tgt_cols: list[str] | None = None
        insert_sql: str | None = None
        cursor = None
        conn = None

        total_inserted = 0

        for record_batch in batch_iter:
            if record_batch.num_rows == 0:
                continue

            # Resolve columns on first batch
            if src_cols is None:
                available = set(record_batch.schema.names)
                valid_pairs = [
                    (src, tgt)
                    for src, tgt in zip(matched_source_cols, matched_target_cols)
                    if src in available
                ]
                if not valid_pairs:
                    logger.error("No columns matched between streamed data and mapping config — aborting.")
                    return 0
                src_cols = [p[0] for p in valid_pairs]
                tgt_cols = [p[1] for p in valid_pairs]

                if not use_bcp and not use_arrow_odbc:
                    # Prepare pyodbc cursor with TABLOCK hint + fast_executemany
                    target_col_names = [f"[{name}]" for name in tgt_cols]
                    placeholders = ", ".join(["?"] * len(target_col_names))
                    insert_sql = (
                        f"INSERT INTO {self._full_table_name()} WITH (TABLOCK) "
                        f"({', '.join(target_col_names)}) VALUES ({placeholders})"
                    )
                    logger.info("INSERT SQL (TABLOCK): %s", insert_sql)

                    conn = self.connect()
                    cursor = conn.cursor()
                    cursor.fast_executemany = True
                    # Suppress per-batch row-count messages — reduces
                    # network chatter between client and SQL Server.
                    cursor.execute("SET NOCOUNT ON")

            # Select mapped columns from this batch
            selected_batch = record_batch.select(src_cols)
            batch_rows = selected_batch.num_rows

            # ---- arrow-odbc path (fastest) ----
            if use_arrow_odbc:
                assert tgt_cols is not None
                try:
                    written = self._write_batch_arrow_odbc(selected_batch, tgt_cols)
                    total_inserted += written
                except ImportError:
                    raise
                except Exception as exc:
                    logger.warning("arrow-odbc failed for batch (%s). Falling back to pyodbc.", exc)
                    use_arrow_odbc = False
                    # Prepare pyodbc cursor (fall-through below)
                    target_col_names = [f"[{name}]" for name in tgt_cols]
                    placeholders = ", ".join(["?"] * len(target_col_names))
                    insert_sql = (
                        f"INSERT INTO {self._full_table_name()} WITH (TABLOCK) "
                        f"({', '.join(target_col_names)}) VALUES ({placeholders})"
                    )
                    conn = self.connect()
                    cursor = conn.cursor()
                    cursor.fast_executemany = True
                    cursor.execute("SET NOCOUNT ON")

            # ---- BCP path ----
            if use_bcp:
                batch_table = pa.Table.from_batches([selected_batch])
                try:
                    written = self._write_with_bcp(
                        batch_table,
                        bcp_path=bcp_path,
                        bcp_batch_size=bcp_batch_size,
                        bcp_temp_dir=bcp_temp_dir,
                        bcp_field_terminator=bcp_field_terminator,
                        bcp_row_terminator=bcp_row_terminator,
                    )
                    total_inserted += written
                except Exception as exc:
                    logger.warning("BCP failed for batch (%s). Falling back to pyodbc.", exc)
                    use_bcp = False
                    assert tgt_cols is not None
                    target_col_names = [f"[{name}]" for name in tgt_cols]
                    placeholders = ", ".join(["?"] * len(target_col_names))
                    insert_sql = (
                        f"INSERT INTO {self._full_table_name()} WITH (TABLOCK) "
                        f"({', '.join(target_col_names)}) VALUES ({placeholders})"
                    )
                    conn = self.connect()
                    cursor = conn.cursor()
                    cursor.fast_executemany = True
                    cursor.execute("SET NOCOUNT ON")
                    # Fall through to pyodbc path below

            # ---- pyodbc fast_executemany path (with TABLOCK) ----
            if not use_bcp and not use_arrow_odbc:
                assert cursor is not None and insert_sql is not None and conn is not None
                # Sub-batch within each RecordBatch; commit once per
                # RecordBatch (not per sub-batch) to reduce log flushes.
                # Use RecordBatch.slice() directly — no need to wrap in a Table first.
                sub_inserted = 0
                try:
                    for start in range(0, batch_rows, batch_size):
                        sub_len = min(batch_size, batch_rows - start)
                        sub_batch = selected_batch.slice(start, sub_len)
                        rows = self._arrow_to_rows(sub_batch)
                        cursor.executemany(insert_sql, rows)
                        sub_inserted += len(rows)
                        del rows      # free Python tuple list immediately — already sent to SQL Server
                        del sub_batch # free Arrow slice
                    # Single commit for the whole RecordBatch
                    conn.commit()
                    total_inserted += sub_inserted
                except Exception:
                    conn.rollback()
                    logger.exception(
                        "Failed to insert streaming RecordBatch (%d rows).", batch_rows,
                    )
                    raise

            # Free Arrow batch for this loop iteration — already written or yielded to all paths.
            del selected_batch

            logger.info(
                "Streamed batch: %d rows  (total: %d)", batch_rows, total_inserted,
            )

        logger.info(
            "Streaming write complete: %d total rows to %s.",
            total_inserted, self._full_table_name(),
        )
        return total_inserted

    def truncate_table(self) -> None:
        """Truncate the target table (used before a full load)."""
        logger.info("Truncating %s ...", self._full_table_name())
        conn = self.connect()
        conn.cursor().execute(f"TRUNCATE TABLE {self._full_table_name()};")
        conn.commit()

    # ------------------------------------------------------------------
    # Data writing
    # ------------------------------------------------------------------

    def write(
        self,
        arrow_table: pa.Table,
        *,
        batch_size: int = 5000,
        truncate_first: bool = False,
    ) -> int:
        """
        Insert rows from an Arrow table into SQL Server.

        Parameters
        ----------
        arrow_table : pa.Table
            Data to write.
        batch_size : int
            Number of rows per INSERT batch.
        truncate_first : bool
            If True, truncate the target table before inserting.

        Returns
        -------
        int
            Total rows inserted.
        """
        if arrow_table.num_rows == 0:
            logger.info("No rows to write.")
            return 0

        # Ensure table exists and resolve column mapping
        self.ensure_table(arrow_table.schema)
        assert self._mapping_result is not None

        if truncate_first:
            self.truncate_table()

        # Build source→target column mapping
        col_map = self._mapping_result.source_to_target()
        # Only use matched source columns (in mapping order)
        matched_source_cols = [m.source_name for m in self._mapping_result.matched]
        matched_target_cols = [col_map[s] for s in matched_source_cols]

        if not matched_source_cols:
            logger.error("No columns matched between source and target — aborting.")
            return 0

        conn = self.connect()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.execute("SET NOCOUNT ON")

        target_col_names = [f"[{name}]" for name in matched_target_cols]
        placeholders = ", ".join(["?"] * len(target_col_names))
        insert_sql = (
            f"INSERT INTO {self._full_table_name()} "
            f"({', '.join(target_col_names)}) VALUES ({placeholders})"
        )

        logger.info("INSERT SQL: %s", insert_sql)

        # Select only matched source columns, convert via Arrow (no pandas)
        arrow_subset = arrow_table.select(matched_source_cols)

        total_rows = arrow_subset.num_rows

        inserted = 0

        for start in range(0, total_rows, batch_size):
            batch_len = min(batch_size, total_rows - start)
            batch_table = arrow_subset.slice(start, batch_len)
            batch = self._arrow_to_rows(batch_table)
            try:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                inserted += len(batch)
                logger.info(
                    "Inserted batch %d-%d  (%d / %d rows)",
                    start,
                    start + len(batch) - 1,
                    inserted,
                    total_rows,
                )
            except Exception:
                conn.rollback()
                logger.exception(
                    "Failed to insert batch starting at row %d.", start
                )
                raise

        logger.info("Finished writing %d rows to %s.", inserted, self._full_table_name())
        return inserted
