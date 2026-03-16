"""
SID Transformer

Core in-memory PyArrow transformation logic for SID remapping.
Handles PK updates and FK cascades based on parent→child relationships.
"""

import logging
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field

import pyarrow as pa

from .config import (
    SID_CONFIG,
    get_processing_order,
    get_fk_relationships,
    get_self_reference_fk,
    get_pk_column,
    get_tables_with_self_reference,
)
from .sid_client import SIDClientBase, SIDAllocationResponse

logger = logging.getLogger(__name__)


@dataclass
class TransformResult:
    """Result of transforming a single table."""
    table_name: str
    rows_processed: int
    pk_column: str
    sid_range_start: int
    sid_range_end: int
    fk_columns_updated: List[str] = field(default_factory=list)
    self_reference_updated: bool = False
    error: Optional[str] = None


@dataclass
class TransformJobResult:
    """Result of transforming all tables in a job."""
    tables_processed: int
    total_rows: int
    results: List[TransformResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    @property
    def success(self) -> bool:
        return len(self.errors) == 0


class SIDTransformer:
    """
    Transforms SID columns in PyArrow tables.
    
    Flow:
        1. Process tables in dependency order (Level 0 → 5)
        2. For each table:
           a. Allocate SID range from API
           b. Build old→new mapping
           c. Update PK column
           d. Update FK columns using parent mappings
        3. Handle self-referencing tables (two-pass)
    """
    
    def __init__(self, sid_client: SIDClientBase):
        """
        Initialize transformer.
        
        Args:
            sid_client: Client for SID API (production or mock).
        """
        self._sid_client = sid_client
        
        # Mappings: { table_name: { old_sid: new_sid } }
        self._sid_mappings: Dict[str, Dict[int, int]] = {}
        
        # Transformed tables: { table_name: PyArrow Table }
        self._transformed_tables: Dict[str, pa.Table] = {}
        
        # Override SIDs: { table_name: new_sid }
        # When set, ALL old SIDs for this table map to the single new_sid.
        # Used for Exposure API integration where one new ExposureSet is created.
        self._override_sids: Dict[str, int] = {}
    
    def reset(self) -> None:
        """Reset all state between jobs."""
        self._sid_mappings.clear()
        self._transformed_tables.clear()
        self._override_sids.clear()
    
    def set_override_sid(self, table_name: str, new_sid: int) -> None:
        """
        Set an override SID for a table.
        
        When set, ALL old SIDs in this table's PK (and FK references to it)
        will map to this single new_sid instead of allocating new SIDs.
        
        This is used for Exposure API integration where a single new
        ExposureSet/View is created to represent all imported data.
        
        Args:
            table_name: Name of the table.
            new_sid: The override SID value to use.
        """
        self._override_sids[table_name] = new_sid
        logger.info(
            "Override SID set: %s -> %d (all old SIDs will map here)",
            table_name,
            new_sid,
        )
    
    def get_mapping(self, table_name: str) -> Dict[int, int]:
        """
        Get SID mapping for a table.
        
        Args:
            table_name: Name of the table.
            
        Returns:
            Dict mapping old SID → new SID.
        """
        return self._sid_mappings.get(table_name, {})
    
    def transform_table(
        self,
        table_name: str,
        arrow_table: pa.Table,
        skip_fk_update: bool = False,
    ) -> TransformResult:
        """
        Transform SID columns in a single PyArrow table.
        
        Args:
            table_name: Name of the table.
            arrow_table: PyArrow Table to transform.
            skip_fk_update: If True, skip FK updates (for testing).
            
        Returns:
            TransformResult with details of transformation.
        """
        if table_name not in SID_CONFIG:
            return TransformResult(
                table_name=table_name,
                rows_processed=0,
                pk_column="",
                sid_range_start=0,
                sid_range_end=0,
                error=f"Table {table_name} not in SID_CONFIG",
            )
        
        config = SID_CONFIG[table_name]
        pk_column = config.pk_column
        row_count = arrow_table.num_rows
        
        if row_count == 0:
            logger.info(f"Table {table_name} is empty, skipping")
            return TransformResult(
                table_name=table_name,
                rows_processed=0,
                pk_column=pk_column,
                sid_range_start=0,
                sid_range_end=0,
            )
        
        # Check if PK column exists
        if pk_column not in arrow_table.column_names:
            return TransformResult(
                table_name=table_name,
                rows_processed=0,
                pk_column=pk_column,
                sid_range_start=0,
                sid_range_end=0,
                error=f"PK column {pk_column} not found in table",
            )
        
        try:
            # Get old SIDs from PK column
            old_sids = arrow_table.column(pk_column).to_pylist()
            
            # Check for override SID (e.g., from Exposure API)
            if table_name in self._override_sids:
                override_sid = self._override_sids[table_name]
                logger.info(
                    "Using override SID for %s: all %d rows -> SID %d",
                    table_name,
                    row_count,
                    override_sid,
                )
                
                # All old SIDs map to the single override SID
                mapping = {
                    old_sid: override_sid
                    for old_sid in old_sids
                    if old_sid is not None
                }
                self._sid_mappings[table_name] = mapping
                
                # Use override_sid for range tracking
                sid_range_start = override_sid
                sid_range_end = override_sid
            else:
                # 1. Allocate SID range from API
                allocation = self._sid_client.allocate(table_name, row_count)
                
                # 2. Build old→new mapping (each old SID gets unique new SID)
                mapping = {
                    old_sid: allocation.start + idx
                    for idx, old_sid in enumerate(old_sids)
                    if old_sid is not None
                }
                self._sid_mappings[table_name] = mapping
                
                sid_range_start = allocation.start
                sid_range_end = allocation.end
            
            # 3. Update PK column
            new_pk_values = pa.array([
                mapping.get(old, old) if old is not None else None
                for old in old_sids
            ])
            arrow_table = self._replace_column(arrow_table, pk_column, new_pk_values)
            
            fk_columns_updated = []
            
            if not skip_fk_update:
                # 4. Update FK columns using parent mappings
                fk_relationships = get_fk_relationships(table_name)
                for fk_column, parent_table in fk_relationships.items():
                    if fk_column not in arrow_table.column_names:
                        logger.warning(f"FK column {fk_column} not found in {table_name}")
                        continue
                    
                    if parent_table not in self._sid_mappings:
                        logger.warning(f"No mapping for parent table {parent_table}")
                        continue
                    
                    parent_mapping = self._sid_mappings[parent_table]
                    arrow_table = self._update_fk_column(
                        arrow_table, fk_column, parent_mapping
                    )
                    fk_columns_updated.append(fk_column)
            
            # 5. Handle self-reference FK (same table mapping)
            self_ref_updated = False
            self_ref_fk = get_self_reference_fk(table_name)
            if self_ref_fk and self_ref_fk in arrow_table.column_names:
                arrow_table = self._update_fk_column(
                    arrow_table, self_ref_fk, mapping
                )
                self_ref_updated = True
                fk_columns_updated.append(self_ref_fk)
            
            # Store transformed table
            self._transformed_tables[table_name] = arrow_table
            
            return TransformResult(
                table_name=table_name,
                rows_processed=row_count,
                pk_column=pk_column,
                sid_range_start=sid_range_start,
                sid_range_end=sid_range_end,
                fk_columns_updated=fk_columns_updated,
                self_reference_updated=self_ref_updated,
            )
            
        except Exception as e:
            logger.error(f"Error transforming {table_name}: {e}")
            return TransformResult(
                table_name=table_name,
                rows_processed=0,
                pk_column=pk_column,
                sid_range_start=0,
                sid_range_end=0,
                error=str(e),
            )
    
    def transform_all(
        self,
        tables: Dict[str, pa.Table],
        on_table_complete: Optional[Callable[[TransformResult], None]] = None,
    ) -> TransformJobResult:
        """
        Transform all tables in dependency order.
        
        Args:
            tables: Dict mapping table names to PyArrow Tables.
            on_table_complete: Optional callback after each table is processed.
            
        Returns:
            TransformJobResult with overall results.
        """
        self.reset()
        
        results: List[TransformResult] = []
        errors: List[str] = []
        total_rows = 0
        
        # Process in dependency order
        processing_order = get_processing_order()
        
        for level, table_name in processing_order:
            if table_name not in tables:
                logger.debug(f"Table {table_name} not in input, skipping")
                continue
            
            logger.info(f"Processing Level {level}: {table_name}")
            
            result = self.transform_table(table_name, tables[table_name])
            results.append(result)
            
            if result.error:
                errors.append(f"{table_name}: {result.error}")
            else:
                total_rows += result.rows_processed
            
            if on_table_complete:
                on_table_complete(result)
        
        return TransformJobResult(
            tables_processed=len(results),
            total_rows=total_rows,
            results=results,
            errors=errors,
        )
    
    def get_transformed_table(self, table_name: str) -> Optional[pa.Table]:
        """
        Get transformed PyArrow table.
        
        Args:
            table_name: Name of the table.
            
        Returns:
            Transformed PyArrow Table or None if not transformed.
        """
        return self._transformed_tables.get(table_name)
    
    def get_all_transformed_tables(self) -> Dict[str, pa.Table]:
        """
        Get all transformed tables.
        
        Returns:
            Dict mapping table names to transformed PyArrow Tables.
        """
        return self._transformed_tables.copy()
    
    def _replace_column(
        self,
        table: pa.Table,
        column_name: str,
        new_values: pa.Array,
    ) -> pa.Table:
        """
        Replace a column in a PyArrow table.
        
        Args:
            table: Source table.
            column_name: Column to replace.
            new_values: New values for the column.
            
        Returns:
            New table with replaced column.
        """
        col_idx = table.column_names.index(column_name)
        return table.set_column(col_idx, column_name, new_values)
    
    def _update_fk_column(
        self,
        table: pa.Table,
        fk_column: str,
        mapping: Dict[int, int],
    ) -> pa.Table:
        """
        Update a FK column using a SID mapping.
        
        Args:
            table: Source table.
            fk_column: FK column to update.
            mapping: Dict mapping old SID → new SID.
            
        Returns:
            Table with updated FK column.
        """
        fk_values = table.column(fk_column).to_pylist()
        new_fk_values = pa.array([
            mapping.get(old, old) if old is not None else None
            for old in fk_values
        ])
        return self._replace_column(table, fk_column, new_fk_values)


def create_transformer(use_mock: bool = True, **kwargs) -> SIDTransformer:
    """
    Factory function to create a SID transformer.
    
    Args:
        use_mock: If True, use MockSIDClient for testing.
        **kwargs: Additional arguments for the client.
        
    Returns:
        Configured SIDTransformer.
    """
    from .sid_client import MockSIDClient, SIDClient
    
    if use_mock:
        client = MockSIDClient(**kwargs)
    else:
        client = SIDClient(**kwargs)
    
    return SIDTransformer(client)
