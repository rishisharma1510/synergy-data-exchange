"""
sql_reader.py — Read SQL Server tables and stream as PyArrow batches.

Reverse of sql_writer.py — used for ingestion flow (SQL Server → Iceberg).
Supports streaming reads with memory-efficient batching.
"""

import logging
from typing import Iterator, List, Optional

import pyarrow as pa
import pyodbc

from src.core.config import SqlServerConfig

logger = logging.getLogger(__name__)


# SQL Server types that cannot be read via ODBC and must be excluded
# geography / geometry  → ODBC type -151 (no standard mapping)
# hierarchyid / sql_variant → driver-dependent raw bytes with no sensible cast
UNSUPPORTED_SQL_TYPES = frozenset([
    "GEOGRAPHY",
    "GEOMETRY",
    "HIERARCHYID",
    "SQL_VARIANT",
])

# Type mapping: SQL Server → Arrow
SQL_TO_ARROW_TYPES = {
    "bigint": pa.int64(),
    "int": pa.int32(),
    "smallint": pa.int16(),
    "tinyint": pa.int8(),
    "bit": pa.bool_(),
    "decimal": pa.decimal128,  # Requires precision/scale
    "numeric": pa.decimal128,
    "money": pa.decimal128(19, 4),
    "smallmoney": pa.decimal128(10, 4),
    "float": pa.float64(),
    "real": pa.float32(),
    "date": pa.date32(),
    "datetime": pa.timestamp("us"),
    "datetime2": pa.timestamp("us"),
    "smalldatetime": pa.timestamp("s"),
    "datetimeoffset": pa.timestamp("us", tz="UTC"),
    "time": pa.time64("us"),
    "char": pa.string(),
    "varchar": pa.string(),
    "nchar": pa.string(),
    "nvarchar": pa.string(),
    "text": pa.large_string(),
    "ntext": pa.large_string(),
    "binary": pa.binary(),
    "varbinary": pa.binary(),
    "image": pa.large_binary(),
    "uniqueidentifier": pa.string(),
    "xml": pa.large_string(),
}


class SqlServerReader:
    """Read SQL Server tables as Arrow record batches."""
    
    def __init__(self, config: SqlServerConfig, database: str) -> None:
        """
        Initialize the SQL Server reader.
        
        Parameters
        ----------
        config : SqlServerConfig
            SQL Server connection configuration.
        database : str
            Database name to connect to.
        """
        self.config = config
        self.database = database
        self._connection: Optional[pyodbc.Connection] = None
    
    def connect(self) -> None:
        """Establish connection to SQL Server."""
        # Create a copy of config with the specific database
        conn_str = (
            f"DRIVER={{{self.config.driver}}};"
            f"SERVER={self.config.host},{self.config.port};"
            f"DATABASE={self.database};"
            f"UID={self.config.username};"
            f"PWD={self.config.password};"
            f"Connection Timeout={self.config.connection_timeout};"
        )
        
        if self.config.trust_cert:
            conn_str += "TrustServerCertificate=yes;"
        
        logger.info("Connecting to SQL Server database: %s", self.database)
        self._connection = pyodbc.connect(conn_str)
        logger.info("Connected successfully to %s", self.database)
    
    def list_tables(self, schema: str = "dbo") -> List[str]:
        """
        List all user tables in the database.
        
        Parameters
        ----------
        schema : str
            Schema to list tables from (default: dbo).
        
        Returns
        -------
        list[str]
            List of table names.
        """
        self._ensure_connected()
        cursor = self._connection.cursor()
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
              AND TABLE_SCHEMA = ?
            ORDER BY TABLE_NAME
        """, schema)
        return [row[0] for row in cursor.fetchall()]
    
    def get_table_schema(self, table_name: str, schema: str = "dbo") -> List[dict]:
        """
        Get column information for a table.
        
        Parameters
        ----------
        table_name : str
            Name of the table.
        schema : str
            Schema name (default: dbo).
        
        Returns
        -------
        list[dict]
            List of column definitions with name, type, nullable, etc.
        """
        self._ensure_connected()
        cursor = self._connection.cursor()
        cursor.execute("""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, schema, table_name)
        
        columns = []
        for row in cursor.fetchall():
            columns.append({
                "name": row[0],
                "type": row[1].upper(),
                "max_length": row[2],
                "precision": row[3],
                "scale": row[4],
                "nullable": row[5] == "YES",
                "ordinal": row[6],
            })
        
        return columns
    
    def get_arrow_schema(self, table_name: str, schema: str = "dbo") -> pa.Schema:
        """
        Get an Arrow schema for a SQL Server table.
        
        Parameters
        ----------
        table_name : str
            Name of the table.
        schema : str
            Schema name (default: dbo).
        
        Returns
        -------
        pa.Schema
            PyArrow schema matching the SQL Server table.
        """
        columns = self.get_table_schema(table_name, schema)
        
        fields = []
        for col in columns:
            arrow_type = self._sql_to_arrow_type(
                col["type"],
                col.get("precision"),
                col.get("scale"),
            )
            fields.append(pa.field(col["name"], arrow_type, nullable=col["nullable"]))
        
        return pa.schema(fields)
    
    def stream_table(
        self, 
        table_name: str, 
        schema: str = "dbo",
        batch_size: int = 10000,
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
    ) -> Iterator[pa.RecordBatch]:
        """
        Stream table data as Arrow record batches.
        
        Uses server-side cursor for memory efficiency.
        
        Parameters
        ----------
        table_name : str
            Name of the table to read.
        schema : str
            Schema name (default: dbo).
        batch_size : int
            Number of rows per batch (default: 10000).
        columns : list[str], optional
            Specific columns to read. None reads all columns.
        where_clause : str, optional
            Optional WHERE clause (without the WHERE keyword).
        
        Yields
        ------
        pa.RecordBatch
            Arrow record batches containing table data.
        """
        self._ensure_connected()
        cursor = self._connection.cursor()

        # ── Determine readable columns ─────────────────────────────────
        # If caller passed explicit columns, use them as-is.
        # Otherwise query INFORMATION_SCHEMA to find and skip unsupported types
        # (geography, geometry, hierarchyid, sql_variant) which cause ODBC errors.
        skipped_cols: list[str] = []
        if columns:
            readable_cols = list(columns)
        else:
            all_cols = self.get_table_schema(table_name, schema)
            readable_cols = []
            for col in all_cols:
                if col["type"] in UNSUPPORTED_SQL_TYPES:
                    skipped_cols.append(col["name"])
                else:
                    readable_cols.append(col["name"])
            if skipped_cols:
                logger.warning(
                    "Table %s: skipping %d unsupported column(s): %s",
                    table_name, len(skipped_cols), skipped_cols,
                )

        col_list = ", ".join(f"[{c}]" for c in readable_cols)
        sql = f"SELECT {col_list} FROM [{schema}].[{table_name}]"

        if where_clause:
            sql += f" WHERE {where_clause}"

        logger.info("Streaming table: %s (batch_size=%d)", table_name, batch_size)
        cursor.execute(sql)

        # Get column info from cursor description
        col_names = [desc[0] for desc in cursor.description]

        # Infer Arrow schema from cursor metadata
        arrow_schema = self._infer_schema_from_cursor(cursor.description)

        total_rows = 0
        batch_count = 0

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            # Convert rows to Arrow arrays, safely handling bytes→string coercion
            arrays = []
            for col_idx in range(len(col_names)):
                col_data = [row[col_idx] for row in rows]
                field = arrow_schema.field(col_idx)
                arrays.append(self._safe_array(col_data, field))

            batch = pa.RecordBatch.from_arrays(arrays, schema=arrow_schema)
            total_rows += len(rows)
            batch_count += 1

            yield batch

        logger.info(
            "Finished streaming %s: %d rows in %d batches",
            table_name, total_rows, batch_count,
        )
    
    def read_table(
        self, 
        table_name: str, 
        schema: str = "dbo",
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
    ) -> pa.Table:
        """
        Read entire table into memory as an Arrow Table.
        
        Use stream_table() for large tables to avoid memory issues.
        
        Parameters
        ----------
        table_name : str
            Name of the table to read.
        schema : str
            Schema name (default: dbo).
        columns : list[str], optional
            Specific columns to read.
        where_clause : str, optional
            Optional WHERE clause.
        
        Returns
        -------
        pa.Table
            Arrow table with all data.
        """
        batches = list(self.stream_table(
            table_name, schema, 
            columns=columns, 
            where_clause=where_clause,
        ))
        
        if not batches:
            # Return empty table with schema
            return pa.table({})
        
        return pa.Table.from_batches(batches)
    
    def get_row_count(self, table_name: str, schema: str = "dbo") -> int:
        """
        Get total row count for a table.
        
        Parameters
        ----------
        table_name : str
            Name of the table.
        schema : str
            Schema name (default: dbo).
        
        Returns
        -------
        int
            Total row count.
        """
        self._ensure_connected()
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table_name}]")
        return cursor.fetchone()[0]
    
    def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
    
    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    
    def _ensure_connected(self) -> None:
        """Ensure we have an active connection."""
        if self._connection is None:
            raise RuntimeError("Not connected. Call connect() first.")
    
    def _sql_to_arrow_type(
        self, 
        sql_type: str, 
        precision: Optional[int] = None,
        scale: Optional[int] = None,
    ) -> pa.DataType:
        """Convert SQL Server type to Arrow type."""
        sql_type_lower = sql_type.lower()
        
        # Handle parameterized types
        if sql_type_lower in ("decimal", "numeric"):
            p = precision or 38
            s = scale or 0
            return pa.decimal128(p, s)
        
        # Look up in type mapping
        if sql_type_lower in SQL_TO_ARROW_TYPES:
            arrow_type = SQL_TO_ARROW_TYPES[sql_type_lower]
            # Handle callable types (like decimal128)
            if callable(arrow_type):
                return arrow_type(precision or 38, scale or 0)
            return arrow_type
        
        # Default to string for unknown types
        logger.warning("Unknown SQL type '%s', defaulting to string", sql_type)
        return pa.string()
    
    def _infer_schema_from_cursor(self, description) -> pa.Schema:
        """Infer Arrow schema from cursor description."""
        fields = []
        
        for col in description:
            name = col[0]
            type_code = col[1]
            precision = col[4] if len(col) > 4 else None
            scale = col[5] if len(col) > 5 else None
            nullable = col[6] if len(col) > 6 else True
            
            # Map pyodbc type codes to Arrow types
            arrow_type = self._typecode_to_arrow(type_code, precision, scale)
            fields.append(pa.field(name, arrow_type, nullable=nullable))
        
        return pa.schema(fields)
    
    def _typecode_to_arrow(
        self, 
        type_code, 
        precision: Optional[int] = None,
        scale: Optional[int] = None,
    ) -> pa.DataType:
        """Map pyodbc type code to Arrow type."""
        import datetime
        from decimal import Decimal
        
        # pyodbc type codes are Python types
        if type_code == str:
            return pa.string()
        elif type_code == int:
            return pa.int64()
        elif type_code == float:
            return pa.float64()
        elif type_code == bool:
            return pa.bool_()
        elif type_code == Decimal:
            p = precision or 38
            s = scale or 0
            return pa.decimal128(p, s)
        elif type_code == datetime.datetime:
            return pa.timestamp("us")
        elif type_code == datetime.date:
            return pa.date32()
        elif type_code == datetime.time:
            return pa.time64("us")
        elif type_code == bytes:
            return pa.binary()
        else:
            return pa.string()

    def _safe_array(self, col_data: list, field: pa.Field) -> pa.Array:
        """
        Build a PyArrow array, handling common ODBC → Arrow coercion pitfalls.

        Problems addressed:
        * uniqueidentifier / some binary columns: pyodbc type_code is ``str``
          so field.type becomes ``pa.string()``, but the driver returns raw
          ``bytes`` values (e.g. 16-byte UUIDs).  We hex-encode those bytes so
          they are valid UTF-8 strings instead of crashing.
        * Any other unexpected type mismatch: fall back to casting via string
          rather than raising.
        """
        ftype = field.type

        # Fast path: no special handling needed
        if not (pa.types.is_string(ftype) or pa.types.is_large_string(ftype)):
            try:
                return pa.array(col_data, type=ftype)
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                # Last resort: stringify everything
                logger.debug(
                    "Column %s: Arrow type %s failed, falling back to string", field.name, ftype
                )
                return pa.array([str(v) if v is not None else None for v in col_data], type=pa.string())

        # For string/large_string fields: hex-encode bytes values so ODBC's
        # raw-byte representation of uniqueidentifier etc. does not break Arrow.
        has_bytes = any(isinstance(v, (bytes, bytearray)) for v in col_data if v is not None)
        if has_bytes:
            logger.debug(
                "Column %s: encoding %d bytes values as hex strings",
                field.name, sum(1 for v in col_data if isinstance(v, (bytes, bytearray))),
            )
            coerced = [
                v.hex() if isinstance(v, (bytes, bytearray)) else v
                for v in col_data
            ]
            return pa.array(coerced, type=ftype)

        return pa.array(col_data, type=ftype)
