"""
Parallel Table Processing for SID Transformation

Processes tables at the same dependency level concurrently.
Uses ThreadPoolExecutor for I/O-bound operations (SID API calls).

Thread safety:
- Each table gets its own SID allocation (no contention)
- Mappings are written to thread-safe dict after completion
- FK updates require parent level to complete first (enforced by level ordering)
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any, Tuple
from threading import Lock
import time

import pyarrow as pa

from .config import SID_CONFIG, PROCESSING_LEVELS, get_fk_relationships, get_pk_column
from .transformer import SIDTransformer, TransformResult, TransformJobResult

logger = logging.getLogger(__name__)


@dataclass
class LevelResult:
    """Result of processing all tables at a single dependency level."""
    level: int
    tables_processed: int
    total_rows: int
    duration_ms: float
    results: List[TransformResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    @property
    def success(self) -> bool:
        return len(self.errors) == 0


@dataclass
class ParallelJobResult:
    """Result of a parallel SID transformation job."""
    levels_processed: int
    tables_processed: int
    total_rows: int
    duration_ms: float
    level_results: List[LevelResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    @property
    def success(self) -> bool:
        return len(self.errors) == 0


class ParallelTransformer:
    """
    Parallel SID transformer using ThreadPoolExecutor.
    
    Strategy:
    1. Group tables by dependency level
    2. Process each level sequentially (FK deps)
    3. Within each level, process tables in parallel
    
    This maximizes parallelism while respecting FK constraints.
    """
    
    def __init__(
        self,
        transformer: SIDTransformer,
        max_workers: int = 4,
    ):
        """
        Initialize parallel transformer.
        
        Args:
            transformer: Base SIDTransformer instance.
            max_workers: Maximum concurrent table transformations.
        """
        self._transformer = transformer
        self._max_workers = max_workers
        
        # Thread-safe mapping storage
        self._mappings_lock = Lock()
        self._tables_lock = Lock()
    
    def get_tables_by_level(
        self,
        tables: Dict[str, pa.Table],
    ) -> Dict[int, List[str]]:
        """
        Group input tables by their processing level.
        
        Args:
            tables: Dict of table_name → PyArrow Table.
            
        Returns:
            Dict of level → list of table names.
        """
        by_level: Dict[int, List[str]] = {}
        
        for level, level_tables in PROCESSING_LEVELS.items():
            matching = [t for t in level_tables if t in tables]
            if matching:
                by_level[level] = matching
        
        return by_level
    
    def _transform_single_table(
        self,
        table_name: str,
        arrow_table: pa.Table,
    ) -> TransformResult:
        """
        Transform a single table (called in worker thread).
        
        Args:
            table_name: Name of the table.
            arrow_table: PyArrow Table to transform.
            
        Returns:
            TransformResult.
        """
        try:
            result = self._transformer.transform_table(table_name, arrow_table)
            return result
        except Exception as e:
            logger.error(f"Error in parallel transform of {table_name}: {e}")
            pk_column = get_pk_column(table_name) or ""
            return TransformResult(
                table_name=table_name,
                rows_processed=0,
                pk_column=pk_column,
                sid_range_start=0,
                sid_range_end=0,
                error=str(e),
            )
    
    def transform_level(
        self,
        level: int,
        tables: Dict[str, pa.Table],
        table_names: List[str],
        on_table_complete: Optional[Callable[[TransformResult], None]] = None,
    ) -> LevelResult:
        """
        Transform all tables at a given level in parallel.
        
        Args:
            level: Processing level.
            tables: Dict of all tables.
            table_names: Tables to process at this level.
            on_table_complete: Callback after each table.
            
        Returns:
            LevelResult with aggregate metrics.
        """
        start_time = time.time()
        results: List[TransformResult] = []
        errors: List[str] = []
        total_rows = 0
        
        logger.info(
            "Processing Level %d: %d tables in parallel (max_workers=%d)",
            level,
            len(table_names),
            min(self._max_workers, len(table_names)),
        )
        
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            # Submit all tables at this level
            future_to_table: Dict[Future, str] = {}
            
            for table_name in table_names:
                if table_name not in tables:
                    continue
                
                future = executor.submit(
                    self._transform_single_table,
                    table_name,
                    tables[table_name],
                )
                future_to_table[future] = table_name
            
            # Collect results as they complete
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result.error:
                        errors.append(f"{table_name}: {result.error}")
                    else:
                        total_rows += result.rows_processed
                    
                    if on_table_complete:
                        on_table_complete(result)
                        
                except Exception as e:
                    logger.error(f"Unexpected error processing {table_name}: {e}")
                    errors.append(f"{table_name}: {str(e)}")
        
        duration_ms = (time.time() - start_time) * 1000
        
        logger.info(
            "Level %d complete: tables=%d, rows=%d, duration=%.1fms",
            level,
            len(results),
            total_rows,
            duration_ms,
        )
        
        return LevelResult(
            level=level,
            tables_processed=len(results),
            total_rows=total_rows,
            duration_ms=duration_ms,
            results=results,
            errors=errors,
        )
    
    def transform_all_parallel(
        self,
        tables: Dict[str, pa.Table],
        on_table_complete: Optional[Callable[[TransformResult], None]] = None,
        on_level_complete: Optional[Callable[[LevelResult], None]] = None,
    ) -> ParallelJobResult:
        """
        Transform all tables using parallel processing per level.
        
        Args:
            tables: Dict of table_name → PyArrow Table.
            on_table_complete: Callback after each table.
            on_level_complete: Callback after each level.
            
        Returns:
            ParallelJobResult with comprehensive metrics.
        """
        start_time = time.time()
        self._transformer.reset()
        
        level_results: List[LevelResult] = []
        all_errors: List[str] = []
        total_rows = 0
        tables_processed = 0
        
        # Group tables by level
        by_level = self.get_tables_by_level(tables)
        
        # Process levels sequentially (FK constraint)
        for level in sorted(by_level.keys()):
            table_names = by_level[level]
            
            level_result = self.transform_level(
                level=level,
                tables=tables,
                table_names=table_names,
                on_table_complete=on_table_complete,
            )
            
            level_results.append(level_result)
            all_errors.extend(level_result.errors)
            total_rows += level_result.total_rows
            tables_processed += level_result.tables_processed
            
            if on_level_complete:
                on_level_complete(level_result)
        
        duration_ms = (time.time() - start_time) * 1000
        
        logger.info(
            "Parallel job complete: levels=%d, tables=%d, rows=%d, duration=%.1fms",
            len(level_results),
            tables_processed,
            total_rows,
            duration_ms,
        )
        
        return ParallelJobResult(
            levels_processed=len(level_results),
            tables_processed=tables_processed,
            total_rows=total_rows,
            duration_ms=duration_ms,
            level_results=level_results,
            errors=all_errors,
        )
    
    def get_speedup_estimate(
        self,
        tables: Dict[str, pa.Table],
    ) -> Tuple[float, str]:
        """
        Estimate speedup from parallel processing.
        
        Args:
            tables: Input tables.
            
        Returns:
            Tuple of (estimated_speedup, explanation).
        """
        by_level = self.get_tables_by_level(tables)
        
        # Sequential: sum of all tables
        sequential_units = sum(len(tables) for tables in by_level.values())
        
        # Parallel: max tables per level divided by workers
        parallel_units = sum(
            max(1, len(level_tables) / self._max_workers)
            for level_tables in by_level.values()
        )
        
        if parallel_units == 0:
            return 1.0, "No tables to process"
        
        speedup = sequential_units / parallel_units
        
        explanation = (
            f"Levels: {len(by_level)}, "
            f"Tables: {sequential_units}, "
            f"Max Workers: {self._max_workers}, "
            f"Estimated {speedup:.1f}x speedup"
        )
        
        return speedup, explanation


def create_parallel_transformer(
    use_mock: bool = True,
    max_workers: int = 4,
    **kwargs,
) -> ParallelTransformer:
    """
    Factory function to create a parallel transformer.
    
    Args:
        use_mock: If True, use MockSIDClient.
        max_workers: Maximum concurrent workers.
        **kwargs: Additional arguments for SID client.
        
    Returns:
        Configured ParallelTransformer.
    """
    from .transformer import create_transformer
    
    base_transformer = create_transformer(use_mock=use_mock, **kwargs)
    return ParallelTransformer(
        transformer=base_transformer,
        max_workers=max_workers,
    )
