"""
Enhanced SID Transformer

Combines all SID enhancements into a single high-level interface:
- Checkpoint/Resume for fault tolerance
- Parallel table processing for performance
- Audit trail for compliance and debugging

Usage:
    transformer = EnhancedTransformer.create(
        job_id="job-123",
        tenant_id="tenant-456",
        use_checkpoint=True,
        use_parallel=True,
        use_audit_trail=True,
    )
    
    result = transformer.transform_all(tables)
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any

import pyarrow as pa

from .transformer import SIDTransformer, TransformResult, TransformJobResult, create_transformer
from .parallel import ParallelTransformer, ParallelJobResult, create_parallel_transformer
from .checkpoint import (
    CheckpointState,
    CheckpointManager,
    create_checkpoint_manager,
)
from .audit_trail import AuditTrailCollector, create_audit_trail
from .config import get_pk_column

logger = logging.getLogger(__name__)


@dataclass
class EnhancedJobResult:
    """Result of an enhanced SID transformation job."""
    job_id: str
    tenant_id: str
    
    # Core metrics
    tables_processed: int = 0
    tables_skipped: int = 0  # From checkpoint
    total_rows: int = 0
    duration_ms: float = 0
    
    # Status
    success: bool = True
    resumed_from_checkpoint: bool = False
    
    # Results
    results: List[TransformResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    # Audit
    audit_records_written: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "tenant_id": self.tenant_id,
            "tables_processed": self.tables_processed,
            "tables_skipped": self.tables_skipped,
            "total_rows": self.total_rows,
            "duration_ms": self.duration_ms,
            "success": self.success,
            "resumed_from_checkpoint": self.resumed_from_checkpoint,
            "errors": self.errors,
            "audit_records_written": self.audit_records_written,
        }


class EnhancedTransformer:
    """
    Enhanced SID transformer with checkpoint, parallel, and audit support.
    
    Features:
    - Automatic checkpoint on each table completion
    - Resume from last checkpoint on restart
    - Parallel processing at each dependency level
    - Audit trail of all SID allocations
    """
    
    def __init__(
        self,
        job_id: str,
        tenant_id: str,
        base_transformer: SIDTransformer,
        parallel_transformer: Optional[ParallelTransformer] = None,
        checkpoint_manager: Optional[CheckpointManager] = None,
        audit_collector: Optional[AuditTrailCollector] = None,
        use_parallel: bool = True,
    ):
        """
        Initialize enhanced transformer.
        
        Args:
            job_id: Unique job identifier.
            tenant_id: Tenant identifier.
            base_transformer: Base SIDTransformer instance.
            parallel_transformer: Optional parallel transformer.
            checkpoint_manager: Optional checkpoint manager.
            audit_collector: Optional audit trail collector.
            use_parallel: Whether to use parallel processing.
        """
        self._job_id = job_id
        self._tenant_id = tenant_id
        self._transformer = base_transformer
        self._parallel = parallel_transformer
        self._checkpoint_mgr = checkpoint_manager
        self._audit = audit_collector
        self._use_parallel = use_parallel and parallel_transformer is not None
        
        # Current checkpoint state
        self._checkpoint_state: Optional[CheckpointState] = None
    
    @classmethod
    def create(
        cls,
        job_id: str,
        tenant_id: str,
        use_checkpoint: bool = True,
        use_parallel: bool = True,
        use_audit_trail: bool = True,
        use_mock_sid: bool = True,
        max_workers: int = 4,
        checkpoint_bucket: str = "",
        audit_bucket: str = "",
        use_s3: bool = False,
        use_dynamodb: bool = False,
        **kwargs,
    ) -> "EnhancedTransformer":
        """
        Factory method to create enhanced transformer.
        
        Args:
            job_id: Unique job identifier.
            tenant_id: Tenant identifier.
            use_checkpoint: Enable checkpoint/resume.
            use_parallel: Enable parallel processing.
            use_audit_trail: Enable audit trail.
            use_mock_sid: Use mock SID client.
            max_workers: Max parallel workers.
            checkpoint_bucket: S3 bucket for checkpoints.
            audit_bucket: S3 bucket for audit trail.
            use_s3: Use S3 for storage.
            use_dynamodb: Use DynamoDB for audit.
            **kwargs: Additional arguments for SID client.
            
        Returns:
            Configured EnhancedTransformer.
        """
        # Create base transformer
        base_transformer = create_transformer(use_mock=use_mock_sid, **kwargs)
        
        # Create parallel transformer
        parallel_transformer = None
        if use_parallel:
            parallel_transformer = ParallelTransformer(
                transformer=base_transformer,
                max_workers=max_workers,
            )
        
        # Create checkpoint manager
        checkpoint_manager = None
        if use_checkpoint:
            checkpoint_manager = create_checkpoint_manager(
                use_s3=use_s3,
                bucket=checkpoint_bucket,
            )
        
        # Create audit collector
        audit_collector = None
        if use_audit_trail:
            audit_collector = create_audit_trail(
                job_id=job_id,
                tenant_id=tenant_id,
                use_s3=use_s3,
                use_dynamodb=use_dynamodb,
                s3_bucket=audit_bucket,
            )
        
        return cls(
            job_id=job_id,
            tenant_id=tenant_id,
            base_transformer=base_transformer,
            parallel_transformer=parallel_transformer,
            checkpoint_manager=checkpoint_manager,
            audit_collector=audit_collector,
            use_parallel=use_parallel,
        )
    
    def _load_checkpoint(self) -> bool:
        """
        Load existing checkpoint if available.
        
        Returns:
            True if checkpoint was loaded.
        """
        if not self._checkpoint_mgr:
            return False
        
        state = self._checkpoint_mgr.load(self._job_id, self._tenant_id)
        if state and state.status == "in_progress":
            self._checkpoint_state = state
            
            # Restore SID mappings to transformer
            for table_name, mapping in state.get_all_mappings().items():
                self._transformer._sid_mappings[table_name] = mapping
            
            logger.info(
                "Resuming from checkpoint: %d tables completed",
                len(state.completed_tables),
            )
            return True
        
        # Create new checkpoint state
        self._checkpoint_state = CheckpointState(
            job_id=self._job_id,
            tenant_id=self._tenant_id,
        )
        return False
    
    def _save_checkpoint(self) -> None:
        """Save current checkpoint state."""
        if self._checkpoint_mgr and self._checkpoint_state:
            self._checkpoint_mgr.save(self._checkpoint_state)
    
    def _on_table_complete(self, result: TransformResult) -> None:
        """Callback after each table is processed."""
        if result.error:
            return
        
        # Update checkpoint
        if self._checkpoint_state:
            mapping = self._transformer.get_mapping(result.table_name)
            self._checkpoint_state.mark_table_complete(result.table_name, mapping)
            self._save_checkpoint()
        
        # Write audit trail
        if self._audit:
            mapping = self._transformer.get_mapping(result.table_name)
            self._audit.record_allocation(
                table_name=result.table_name,
                column_name=result.pk_column,
                mapping=mapping,
            )
    
    def transform_all(
        self,
        tables: Dict[str, pa.Table],
        on_progress: Optional[Callable[[TransformResult], None]] = None,
    ) -> EnhancedJobResult:
        """
        Transform all tables with enhanced features.
        
        Args:
            tables: Dict of table_name → PyArrow Table.
            on_progress: Optional progress callback.
            
        Returns:
            EnhancedJobResult with comprehensive metrics.
        """
        start_time = time.time()
        
        # Initialize result
        result = EnhancedJobResult(
            job_id=self._job_id,
            tenant_id=self._tenant_id,
        )
        
        # Load or create checkpoint
        result.resumed_from_checkpoint = self._load_checkpoint()
        
        # Filter out already-completed tables
        tables_to_process = tables
        if result.resumed_from_checkpoint and self._checkpoint_state:
            completed = set(self._checkpoint_state.completed_tables)
            tables_to_process = {
                name: table
                for name, table in tables.items()
                if name not in completed
            }
            result.tables_skipped = len(completed)
            
            logger.info(
                "Skipping %d already-processed tables",
                result.tables_skipped,
            )
        
        # Combined callback
        def combined_callback(tr: TransformResult) -> None:
            self._on_table_complete(tr)
            if on_progress:
                on_progress(tr)
        
        try:
            # Process tables
            if self._use_parallel and self._parallel:
                parallel_result = self._parallel.transform_all_parallel(
                    tables=tables_to_process,
                    on_table_complete=combined_callback,
                )
                
                result.tables_processed = parallel_result.tables_processed
                result.total_rows = parallel_result.total_rows
                result.errors = parallel_result.errors
                
                # Flatten results from levels
                for level_result in parallel_result.level_results:
                    result.results.extend(level_result.results)
            else:
                job_result = self._transformer.transform_all(
                    tables=tables_to_process,
                    on_table_complete=combined_callback,
                )
                
                result.tables_processed = job_result.tables_processed
                result.total_rows = job_result.total_rows
                result.results = job_result.results
                result.errors = job_result.errors
            
            # Mark checkpoint complete
            if self._checkpoint_state:
                self._checkpoint_state.mark_completed()
                self._save_checkpoint()
            
            # Cleanup checkpoint on success
            if not result.errors and self._checkpoint_mgr:
                self._checkpoint_mgr.delete(self._job_id, self._tenant_id)
            
            result.success = len(result.errors) == 0
            
        except Exception as e:
            logger.error(f"Enhanced transform failed: {e}")
            result.errors.append(str(e))
            result.success = False
            
            # Mark checkpoint as failed
            if self._checkpoint_state:
                self._checkpoint_state.mark_failed(str(e))
                self._save_checkpoint()
        
        # Record audit metrics
        if self._audit:
            result.audit_records_written = self._audit.records_written
        
        result.duration_ms = (time.time() - start_time) * 1000
        
        logger.info(
            "Enhanced transform complete: success=%s, tables=%d, skipped=%d, "
            "rows=%d, duration=%.1fms",
            result.success,
            result.tables_processed,
            result.tables_skipped,
            result.total_rows,
            result.duration_ms,
        )
        
        return result
    
    def get_transformed_tables(self) -> Dict[str, pa.Table]:
        """Get all transformed tables."""
        return self._transformer.get_all_transformed_tables()
    
    def get_mapping(self, table_name: str) -> Dict[int, int]:
        """Get SID mapping for a table."""
        return self._transformer.get_mapping(table_name)
