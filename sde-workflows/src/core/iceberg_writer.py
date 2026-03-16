"""
iceberg_writer.py — Write Arrow record batches to Iceberg tables.

Reverse of iceberg_reader.py — used for ingestion flow (SQL Server → Iceberg).
Supports creating tables, appending, and overwriting data.
"""

import logging
from typing import List, Optional, Union

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
    DateType,
    TimestampType,
    TimestamptzType,
    TimeType,
    DecimalType,
    BinaryType,
    UUIDType,
    NestedField,
)

from src.core.config import IcebergConfig

logger = logging.getLogger(__name__)


class IcebergWriter:
    """Write Arrow data to Iceberg tables via Glue Catalog."""
    
    def __init__(self, config: IcebergConfig) -> None:
        """
        Initialize the Iceberg writer.
        
        Parameters
        ----------
        config : IcebergConfig
            Iceberg catalog and connection configuration.
        """
        self.config = config
        self._catalog = None
    
    @property
    def catalog(self):
        """Lazy-load the Iceberg catalog."""
        if self._catalog is None:
            logger.info(
                "Loading Iceberg catalog: %s (type=%s)",
                self.config.catalog_name,
                self.config.catalog_type,
            )
            self._catalog = load_catalog(
                name=self.config.catalog_name,
                **self.config.catalog_properties(),
            )
        return self._catalog
    
    def namespace_exists(self, namespace: str) -> bool:
        """
        Check if a namespace exists in the catalog.
        
        Parameters
        ----------
        namespace : str
            Namespace to check.
        
        Returns
        -------
        bool
            True if namespace exists.
        """
        try:
            namespaces = self.catalog.list_namespaces()
            return any(ns[0] == namespace for ns in namespaces)
        except Exception:
            return False
    
    def create_namespace(self, namespace: str, properties: Optional[dict] = None) -> None:
        """
        Create a namespace if it doesn't exist.
        
        Parameters
        ----------
        namespace : str
            Namespace to create.
        properties : dict, optional
            Namespace properties.
        """
        try:
            logger.info("Creating namespace: %s", namespace)
            self.catalog.create_namespace(namespace, properties or {})
        except Exception as e:
            err_str = str(e).lower()
            # Glue raises "already exists" / PyIceberg raises NamespaceAlreadyExistsError
            if "already exists" in err_str or "namespaceexists" in type(e).__name__.lower():
                logger.debug("Namespace %s already exists, skipping creation", namespace)
            else:
                raise
    
    def table_exists(self, namespace: str, table_name: str) -> bool:
        """
        Check if a table exists in the catalog.
        
        Parameters
        ----------
        namespace : str
            Iceberg namespace.
        table_name : str
            Table name.
        
        Returns
        -------
        bool
            True if table exists.
        """
        try:
            self.catalog.load_table(f"{namespace}.{table_name}")
            return True
        except NoSuchTableError:
            return False
        except NoSuchNamespaceError:
            return False
    
    def load_table(self, namespace: str, table_name: str) -> Table:
        """
        Load an existing Iceberg table.
        
        Parameters
        ----------
        namespace : str
            Iceberg namespace.
        table_name : str
            Table name.
        
        Returns
        -------
        Table
            PyIceberg Table object.
        """
        table_id = f"{namespace}.{table_name}"
        logger.debug("Loading table: %s", table_id)
        return self.catalog.load_table(table_id)
    
    def create_table(
        self, 
        namespace: str, 
        table_name: str, 
        schema: pa.Schema,
        location: Optional[str] = None,
        partition_by: Optional[List[str]] = None,
        properties: Optional[dict] = None,
    ) -> Table:
        """
        Create a new Iceberg table with the given Arrow schema.
        
        Parameters
        ----------
        namespace : str
            Iceberg namespace.
        table_name : str
            Table name.
        schema : pa.Schema
            Arrow schema defining the table structure.
        location : str, optional
            S3 location for table data. Defaults to warehouse path.
        partition_by : list[str], optional
            Column names to partition by.
        properties : dict, optional
            Table properties.
        
        Returns
        -------
        Table
            The created PyIceberg Table object.
        """
        table_id = f"{namespace}.{table_name}"
        logger.info("Creating Iceberg table: %s", table_id)
        
        # Ensure namespace exists
        self.create_namespace(namespace)
        
        # Convert Arrow schema to Iceberg schema
        iceberg_schema = self._arrow_to_iceberg_schema(schema)
        
        # Build table location
        if location is None and self.config.warehouse:
            location = f"{self.config.warehouse}/{namespace}/{table_name}"
        
        # Create the table
        table = self.catalog.create_table(
            identifier=table_id,
            schema=iceberg_schema,
            location=location,
            properties=properties or {},
        )
        
        logger.info("Created table: %s", table_id)
        return table
    
    def create_table_if_not_exists(
        self,
        namespace: str,
        table_name: str,
        schema: pa.Schema,
        location: Optional[str] = None,
    ) -> Table:
        """
        Create table if it doesn't exist, otherwise return existing table.
        
        Parameters
        ----------
        namespace : str
            Iceberg namespace.
        table_name : str
            Table name.
        schema : pa.Schema
            Arrow schema for table creation.
        location : str, optional
            S3 location for table data.
        
        Returns
        -------
        Table
            The existing or newly created table.
        """
        if self.table_exists(namespace, table_name):
            logger.info("Table %s.%s already exists", namespace, table_name)
            return self.load_table(namespace, table_name)
        
        return self.create_table(namespace, table_name, schema, location)
    
    def append(
        self, 
        namespace: str, 
        table_name: str, 
        data: Union[pa.Table, List[pa.RecordBatch]],
    ) -> int:
        """
        Append data to an existing Iceberg table.
        
        Parameters
        ----------
        namespace : str
            Iceberg namespace.
        table_name : str
            Table name.
        data : pa.Table or list[pa.RecordBatch]
            Data to append.
        
        Returns
        -------
        int
            Total rows written.
        """
        table = self.load_table(namespace, table_name)
        
        # Convert batches to table if needed
        if isinstance(data, list):
            if not data:
                logger.warning("No data to append to %s.%s", namespace, table_name)
                return 0
            arrow_table = pa.Table.from_batches(data)
        else:
            arrow_table = data
        
        row_count = arrow_table.num_rows
        logger.info(
            "Appending %d rows to %s.%s",
            row_count, namespace, table_name,
        )
        
        # Coerce Arrow schema to match destination: renames casing, adds missing
        # null columns, casts numeric/timestamp types, drops unknown columns.
        arrow_table = self._coerce_arrow_schema(table, arrow_table)

        # Append to Iceberg
        table.append(arrow_table)
        
        logger.info("Append complete: %d rows", row_count)
        return row_count
    
    def overwrite(
        self, 
        namespace: str, 
        table_name: str, 
        data: Union[pa.Table, List[pa.RecordBatch]],
    ) -> int:
        """
        Overwrite an Iceberg table with new data.
        
        This replaces ALL existing data in the table.
        
        Parameters
        ----------
        namespace : str
            Iceberg namespace.
        table_name : str
            Table name.
        data : pa.Table or list[pa.RecordBatch]
            Data to write.
        
        Returns
        -------
        int
            Total rows written.
        """
        table = self.load_table(namespace, table_name)
        
        # Convert batches to table if needed
        if isinstance(data, list):
            if not data:
                logger.warning("No data to write to %s.%s", namespace, table_name)
                return 0
            arrow_table = pa.Table.from_batches(data)
        else:
            arrow_table = data
        
        row_count = arrow_table.num_rows
        logger.info(
            "Overwriting %s.%s with %d rows",
            namespace, table_name, row_count,
        )
        
        # Coerce Arrow schema to match destination: renames casing, adds missing
        # null columns, casts numeric/timestamp types, drops unknown columns.
        arrow_table = self._coerce_arrow_schema(table, arrow_table)

        # Overwrite table
        table.overwrite(arrow_table)
        
        logger.info("Overwrite complete: %d rows", row_count)
        return row_count
    
    def upsert(
        self,
        namespace: str,
        table_name: str,
        data: Union[pa.Table, List[pa.RecordBatch]],
        key_columns: List[str],
    ) -> int:
        """
        Upsert data into an Iceberg table (update existing, insert new).
        
        Parameters
        ----------
        namespace : str
            Iceberg namespace.
        table_name : str
            Table name.
        data : pa.Table or list[pa.RecordBatch]
            Data to upsert.
        key_columns : list[str]
            Columns to use for matching existing rows.
        
        Returns
        -------
        int
            Total rows affected.
        """
        # Note: PyIceberg doesn't have native upsert support yet.
        # This would require Iceberg v2 merge-on-read or custom logic.
        # For now, we implement a simple overwrite.
        logger.warning(
            "Upsert not fully implemented - falling back to overwrite for %s.%s",
            namespace, table_name,
        )
        return self.overwrite(namespace, table_name, data)
    
    def delete_table(self, namespace: str, table_name: str) -> None:
        """
        Delete an Iceberg table.
        
        Parameters
        ----------
        namespace : str
            Iceberg namespace.
        table_name : str
            Table name.
        """
        table_id = f"{namespace}.{table_name}"
        logger.info("Deleting table: %s", table_id)
        self.catalog.drop_table(table_id)
    
    # ------------------------------------------------------------------
    # Schema conversion helpers
    # ------------------------------------------------------------------
    
    def _arrow_to_iceberg_schema(self, arrow_schema: pa.Schema) -> Schema:
        """
        Convert an Arrow schema to an Iceberg schema.
        
        Parameters
        ----------
        arrow_schema : pa.Schema
            PyArrow schema.
        
        Returns
        -------
        Schema
            PyIceberg schema.
        """
        fields = []
        
        for field_id, field in enumerate(arrow_schema, start=1):
            iceberg_type = self._arrow_to_iceberg_type(field.type)
            nested_field = NestedField(
                field_id=field_id,
                name=field.name,
                field_type=iceberg_type,
                required=not field.nullable,
            )
            fields.append(nested_field)
        
        return Schema(*fields)

    def _drop_unknown_columns(self, iceberg_table, arrow_table: pa.Table) -> pa.Table:
        """
        Drop columns from *arrow_table* that do not exist in the destination
        Iceberg schema, logging each one.  The destination is NEVER modified.

        This is the ingestion contract: the destination schema is the source of
        truth.  If the incoming data has extra columns, they are silently skipped
        after a warning so operators can investigate the mismatch.
        """
        destination_names = {f.name.lower() for f in iceberg_table.schema().fields}
        incoming_names = arrow_table.schema.names

        unknown = [n for n in incoming_names if n.lower() not in destination_names]
        if not unknown:
            return arrow_table

        for col in unknown:
            logger.warning(
                "[ingestion] Column '%s' does not exist in destination table '%s' — skipping column",
                col,
                iceberg_table.name(),
            )

        keep = [n for n in incoming_names if n.lower() in destination_names]
        logger.info(
            "[ingestion] Dropping %d unknown column(s) from incoming data for '%s': %s",
            len(unknown),
            iceberg_table.name(),
            unknown,
        )
        return arrow_table.select(keep)

    def _align_columns_to_schema(self, iceberg_table, arrow_table: pa.Table) -> pa.Table:
        """
        Rename columns in *arrow_table* so they match the casing already stored
        in the Iceberg schema.  This is a source-side adaptation — the
        destination table is never touched.

        Example: Iceberg has 'contractsid' but Arrow brings 'Contractsid'.
        We rename the Arrow column to 'contractsid' before writing.

        Columns that don't yet exist in the Iceberg schema are left unchanged
        (schema evolution will add them afterwards if needed).
        """
        # Map lowercase → actual Iceberg column name
        iceberg_name_map = {f.name.lower(): f.name for f in iceberg_table.schema().fields}

        new_names = []
        renamed = False
        for col_name in arrow_table.schema.names:
            canonical = iceberg_name_map.get(col_name.lower())
            if canonical is not None and canonical != col_name:
                new_names.append(canonical)
                renamed = True
            else:
                new_names.append(col_name)

        if not renamed:
            return arrow_table

        logger.info(
            "Column case alignment for %s: normalising %d column name(s) to match stored schema",
            iceberg_table.name(),
            sum(1 for a, b in zip(arrow_table.schema.names, new_names) if a != b),
        )
        return arrow_table.rename_columns(new_names)

    def _evolve_schema_if_needed(self, iceberg_table, arrow_table: pa.Table) -> None:
        """
        Evolve the Iceberg table schema so it includes every column present in
        *arrow_table*.  Columns that already exist in the Iceberg schema are
        left untouched (name-based comparison, case-insensitive).

        This guards against the "PyArrow table contains more columns" error that
        PyIceberg raises when you try to append data whose schema is a strict
        superset of the stored schema.
        """
        existing_names = {f.name.lower() for f in iceberg_table.schema().fields}
        incoming_names = [n for n in arrow_table.schema.names if n.lower() not in existing_names]

        if not incoming_names:
            return  # schemas already compatible

        logger.info(
            "Schema evolution: adding %d new column(s) to %s: %s",
            len(incoming_names), iceberg_table.name(), incoming_names,
        )

        # Convert just the new columns from Arrow → PyIceberg types and add them
        with iceberg_table.update_schema(allow_incompatible_changes=False) as update:
            for col_name in incoming_names:
                arrow_field = arrow_table.schema.field(col_name)
                iceberg_type = self._arrow_to_iceberg_type(arrow_field.type)
                update.add_column(col_name, iceberg_type)

    def _iceberg_type_to_arrow_type(self, iceberg_type) -> pa.DataType:
        """
        Map a PyIceberg field type to its corresponding PyArrow type.
        Used for coercing incoming Arrow data to match the destination schema.
        """
        if isinstance(iceberg_type, BooleanType):
            return pa.bool_()
        if isinstance(iceberg_type, IntegerType):
            return pa.int32()
        if isinstance(iceberg_type, LongType):
            return pa.int64()
        if isinstance(iceberg_type, FloatType):
            return pa.float32()
        if isinstance(iceberg_type, DoubleType):
            return pa.float64()
        if isinstance(iceberg_type, StringType):
            return pa.string()
        if isinstance(iceberg_type, DateType):
            return pa.date32()
        if isinstance(iceberg_type, TimestampType):
            return pa.timestamp("us")
        if isinstance(iceberg_type, TimestamptzType):
            return pa.timestamp("us", tz="UTC")
        if isinstance(iceberg_type, TimeType):
            return pa.time64("us")
        if isinstance(iceberg_type, BinaryType):
            return pa.large_binary()
        if isinstance(iceberg_type, DecimalType):
            return pa.decimal128(iceberg_type.precision, iceberg_type.scale)
        # fallback
        return pa.string()

    def _coerce_arrow_schema(self, iceberg_table, arrow_table: pa.Table) -> pa.Table:
        """
        Coerce *arrow_table* so it is byte-for-byte compatible with the Iceberg
        destination schema, using PyIceberg's own schema_to_pyarrow conversion
        so that field IDs are embedded in the metadata and PyIceberg does NOT
        attempt schema evolution (which would require glue:UpdateTable).

        Operations performed (all source-side — destination is never modified):
        1. Derive the *exact* expected Arrow schema via PyIceberg's schema_to_pyarrow.
        2. Reorder / rename columns to Iceberg schema order (case-insensitive).
        3. Add null arrays for columns present in Iceberg but absent in source.
        4. Cast pa.null() columns to the target type.
        5. Cast safe numeric up/down-casts (int64↔int32, float64↔float32).
        6. Convert timestamp → string when the Iceberg schema says string.
        7. Drop columns that exist in source but not in the Iceberg schema.
        """
        # Get the exact Arrow schema PyIceberg expects (includes field-id metadata)
        try:
            from pyiceberg.io.pyarrow import schema_to_pyarrow
            expected_arrow_schema: pa.Schema = schema_to_pyarrow(iceberg_table.schema())
        except Exception:
            # Fallback: build from our own mapper (loses field-id metadata but still
            # structurally correct)
            expected_arrow_schema = None

        iceberg_fields = iceberg_table.schema().fields
        # Build lookup: lowercase name → actual Arrow column name in source
        arrow_name_map: dict[str, str] = {
            n.lower(): n for n in arrow_table.schema.names
        }

        new_columns: list[pa.Array] = []
        new_fields: list[pa.Field] = []
        n_rows = arrow_table.num_rows

        for idx, iceberg_field in enumerate(iceberg_fields):
            col_key = iceberg_field.name.lower()

            # Preferred target type comes from PyIceberg's own conversion
            if expected_arrow_schema is not None:
                try:
                    target_pa_field = expected_arrow_schema.field(iceberg_field.name)
                    target_pa_type = target_pa_field.type
                    target_pa_nullable = target_pa_field.nullable
                except KeyError:
                    target_pa_type = self._iceberg_type_to_arrow_type(iceberg_field.field_type)
                    target_pa_nullable = not iceberg_field.required
            else:
                target_pa_type = self._iceberg_type_to_arrow_type(iceberg_field.field_type)
                target_pa_nullable = not iceberg_field.required

            if col_key in arrow_name_map:
                actual_col_name = arrow_name_map[col_key]
                col = arrow_table.column(actual_col_name)
                current_type = col.type

                if pa.types.is_null(current_type):
                    # All-null column — create typed null array
                    logger.debug(
                        "Coerce: column '%s' pa.null() → %s", iceberg_field.name, target_pa_type
                    )
                    col = pa.array([None] * n_rows, type=target_pa_type)
                elif current_type != target_pa_type:
                    if pa.types.is_timestamp(current_type) and pa.types.is_string(target_pa_type):
                        # timestamp → string (Iceberg stores as string in this table)
                        import datetime as _dt
                        py_vals = col.to_pylist()
                        str_vals = [
                            v.isoformat() if isinstance(v, (_dt.datetime, _dt.date)) else
                            (str(v) if v is not None else None)
                            for v in py_vals
                        ]
                        logger.debug(
                            "Coerce: column '%s' %s → string", iceberg_field.name, current_type
                        )
                        col = pa.array(str_vals, type=pa.string())
                    else:
                        try:
                            col = col.cast(target_pa_type, safe=False)
                            logger.debug(
                                "Coerce: column '%s' %s → %s",
                                iceberg_field.name, current_type, target_pa_type,
                            )
                        except Exception as cast_err:
                            logger.warning(
                                "Coerce: could not cast column '%s' from %s to %s: %s — keeping original",
                                iceberg_field.name, current_type, target_pa_type, cast_err,
                            )
            else:
                # Missing column — insert null array with the correct type
                logger.warning(
                    "[ingestion] Column '%s' missing from source — inserting null column",
                    iceberg_field.name,
                )
                col = pa.array([None] * n_rows, type=target_pa_type)

            # Use the exact pa.Field from the expected schema (preserves field-id metadata)
            if expected_arrow_schema is not None:
                try:
                    out_field = expected_arrow_schema.field(iceberg_field.name)
                except KeyError:
                    out_field = pa.field(iceberg_field.name, target_pa_type, nullable=target_pa_nullable)
            else:
                out_field = pa.field(iceberg_field.name, target_pa_type, nullable=target_pa_nullable)

            new_columns.append(col)
            new_fields.append(out_field)

        # Reconstruct using the exact expected schema (field-IDs in metadata preserved)
        if expected_arrow_schema is not None:
            final_schema = pa.schema(new_fields, metadata=expected_arrow_schema.metadata)
        else:
            final_schema = pa.schema(new_fields)

        coerced = pa.table(
            {f.name: col for f, col in zip(new_fields, new_columns)},
            schema=final_schema,
        )
        return coerced

    def _arrow_to_iceberg_type(self, arrow_type: pa.DataType):
        """
        Convert Arrow type to Iceberg type.
        
        Parameters
        ----------
        arrow_type : pa.DataType
            PyArrow data type.
        
        Returns
        -------
            PyIceberg type.
        """
        # Boolean
        if pa.types.is_boolean(arrow_type):
            return BooleanType()
        
        # Integers
        if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type) or pa.types.is_int32(arrow_type):
            return IntegerType()
        if pa.types.is_int64(arrow_type):
            return LongType()
        if pa.types.is_uint8(arrow_type) or pa.types.is_uint16(arrow_type) or pa.types.is_uint32(arrow_type):
            return IntegerType()
        if pa.types.is_uint64(arrow_type):
            return LongType()
        
        # Floats
        if pa.types.is_float32(arrow_type):
            return FloatType()
        if pa.types.is_float64(arrow_type):
            return DoubleType()
        
        # Decimal
        if pa.types.is_decimal(arrow_type):
            return DecimalType(arrow_type.precision, arrow_type.scale)
        
        # Strings
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return StringType()
        
        # Binary
        if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return BinaryType()
        
        # Date
        if pa.types.is_date(arrow_type) or pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
            return DateType()
        
        # Time
        if pa.types.is_time(arrow_type) or pa.types.is_time32(arrow_type) or pa.types.is_time64(arrow_type):
            return TimeType()
        
        # Timestamp
        if pa.types.is_timestamp(arrow_type):
            if arrow_type.tz:
                return TimestamptzType()
            return TimestampType()
        
        # UUID (fixed-size binary with size 16)
        if pa.types.is_fixed_size_binary(arrow_type) and arrow_type.byte_width == 16:
            return UUIDType()
        
        # Default to string for unknown types
        logger.warning("Unknown Arrow type '%s', defaulting to StringType", arrow_type)
        return StringType()
