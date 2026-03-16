"""
SID Metrics and Telemetry

Tracks SID allocation statistics for monitoring and debugging.
Can emit metrics to CloudWatch or other monitoring systems.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@dataclass
class AllocationMetric:
    """Metrics for a single table allocation."""
    table_name: str
    sid_start: int
    sid_end: int
    row_count: int
    duration_ms: float
    timestamp: float = field(default_factory=time.time)
    
    @property
    def sid_count(self) -> int:
        return self.sid_end - self.sid_start + 1


@dataclass
class TransformMetric:
    """Metrics for a single table transformation."""
    table_name: str
    rows_processed: int
    pk_column: str
    fk_columns_updated: int
    duration_ms: float
    self_reference_updated: bool = False
    timestamp: float = field(default_factory=time.time)


@dataclass
class JobMetrics:
    """Aggregated metrics for an entire SID transformation job."""
    job_id: str
    tenant_id: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    
    allocations: List[AllocationMetric] = field(default_factory=list)
    transforms: List[TransformMetric] = field(default_factory=list)
    
    @property
    def duration_ms(self) -> float:
        if self.end_time is None:
            return (time.time() - self.start_time) * 1000
        return (self.end_time - self.start_time) * 1000
    
    @property
    def total_rows(self) -> int:
        return sum(t.rows_processed for t in self.transforms)
    
    @property
    def total_sids_allocated(self) -> int:
        return sum(a.sid_count for a in self.allocations)
    
    @property
    def tables_processed(self) -> int:
        return len(self.transforms)
    
    def add_allocation(
        self,
        table_name: str,
        sid_start: int,
        sid_end: int,
        row_count: int,
        duration_ms: float,
    ) -> None:
        """Record a SID allocation."""
        metric = AllocationMetric(
            table_name=table_name,
            sid_start=sid_start,
            sid_end=sid_end,
            row_count=row_count,
            duration_ms=duration_ms,
        )
        self.allocations.append(metric)
        
        logger.info(
            "SID allocation: table=%s, range=[%d-%d], rows=%d, duration=%.1fms",
            table_name, sid_start, sid_end, row_count, duration_ms,
        )
    
    def add_transform(
        self,
        table_name: str,
        rows_processed: int,
        pk_column: str,
        fk_columns_updated: int,
        duration_ms: float,
        self_reference_updated: bool = False,
    ) -> None:
        """Record a table transformation."""
        metric = TransformMetric(
            table_name=table_name,
            rows_processed=rows_processed,
            pk_column=pk_column,
            fk_columns_updated=fk_columns_updated,
            duration_ms=duration_ms,
            self_reference_updated=self_reference_updated,
        )
        self.transforms.append(metric)
        
        logger.info(
            "SID transform: table=%s, rows=%d, pk=%s, fks=%d, duration=%.1fms",
            table_name, rows_processed, pk_column, fk_columns_updated, duration_ms,
        )
    
    def complete(self) -> None:
        """Mark job as complete."""
        self.end_time = time.time()
        
        logger.info(
            "SID job complete: job_id=%s, tenant=%s, tables=%d, rows=%d, sids=%d, duration=%.1fms",
            self.job_id,
            self.tenant_id,
            self.tables_processed,
            self.total_rows,
            self.total_sids_allocated,
            self.duration_ms,
        )
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "job_id": self.job_id,
            "tenant_id": self.tenant_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
            "tables_processed": self.tables_processed,
            "total_rows": self.total_rows,
            "total_sids_allocated": self.total_sids_allocated,
            "allocations": [
                {
                    "table_name": a.table_name,
                    "sid_start": a.sid_start,
                    "sid_end": a.sid_end,
                    "row_count": a.row_count,
                    "duration_ms": a.duration_ms,
                }
                for a in self.allocations
            ],
            "transforms": [
                {
                    "table_name": t.table_name,
                    "rows_processed": t.rows_processed,
                    "pk_column": t.pk_column,
                    "fk_columns_updated": t.fk_columns_updated,
                    "duration_ms": t.duration_ms,
                    "self_reference_updated": t.self_reference_updated,
                }
                for t in self.transforms
            ],
        }


class MetricsCollector:
    """
    Collects and emits SID transformation metrics.
    
    Can optionally emit to CloudWatch for production monitoring.
    """
    
    def __init__(
        self,
        namespace: str = "SDE/SID",
        emit_to_cloudwatch: bool = False,
    ):
        """
        Initialize metrics collector.
        
        Args:
            namespace: CloudWatch namespace for metrics.
            emit_to_cloudwatch: Whether to emit metrics to CloudWatch.
        """
        self._namespace = namespace
        self._emit_to_cloudwatch = emit_to_cloudwatch
        self._current_job: Optional[JobMetrics] = None
        self._completed_jobs: List[JobMetrics] = []
    
    def start_job(self, job_id: str, tenant_id: str) -> JobMetrics:
        """
        Start tracking a new SID transformation job.
        
        Args:
            job_id: Unique identifier for this job.
            tenant_id: Tenant being processed.
            
        Returns:
            JobMetrics instance for tracking.
        """
        self._current_job = JobMetrics(job_id=job_id, tenant_id=tenant_id)
        
        logger.info(
            "SID job started: job_id=%s, tenant=%s",
            job_id, tenant_id,
        )
        
        return self._current_job
    
    def complete_job(self) -> Optional[JobMetrics]:
        """
        Complete the current job and emit metrics.
        
        Returns:
            Completed JobMetrics or None if no active job.
        """
        if self._current_job is None:
            return None
        
        self._current_job.complete()
        
        if self._emit_to_cloudwatch:
            self._emit_cloudwatch_metrics(self._current_job)
        
        self._completed_jobs.append(self._current_job)
        job = self._current_job
        self._current_job = None
        
        return job
    
    @property
    def current_job(self) -> Optional[JobMetrics]:
        """Get the current active job."""
        return self._current_job
    
    def _emit_cloudwatch_metrics(self, job: JobMetrics) -> None:
        """Emit metrics to CloudWatch."""
        try:
            import boto3
            
            cloudwatch = boto3.client("cloudwatch")
            
            metrics = [
                {
                    "MetricName": "TablesProcessed",
                    "Value": job.tables_processed,
                    "Unit": "Count",
                    "Dimensions": [
                        {"Name": "TenantId", "Value": job.tenant_id},
                    ],
                },
                {
                    "MetricName": "RowsTransformed",
                    "Value": job.total_rows,
                    "Unit": "Count",
                    "Dimensions": [
                        {"Name": "TenantId", "Value": job.tenant_id},
                    ],
                },
                {
                    "MetricName": "SIDsAllocated",
                    "Value": job.total_sids_allocated,
                    "Unit": "Count",
                    "Dimensions": [
                        {"Name": "TenantId", "Value": job.tenant_id},
                    ],
                },
                {
                    "MetricName": "JobDuration",
                    "Value": job.duration_ms,
                    "Unit": "Milliseconds",
                    "Dimensions": [
                        {"Name": "TenantId", "Value": job.tenant_id},
                    ],
                },
            ]
            
            cloudwatch.put_metric_data(
                Namespace=self._namespace,
                MetricData=metrics,
            )
            
            logger.info("Emitted %d metrics to CloudWatch", len(metrics))
            
        except Exception as e:
            logger.warning("Failed to emit CloudWatch metrics: %s", e)


@contextmanager
def timed_operation(operation_name: str):
    """
    Context manager for timing operations.
    
    Usage:
        with timed_operation("transform") as timer:
            do_work()
        print(f"Took {timer.duration_ms}ms")
    """
    class Timer:
        def __init__(self):
            self.start_time = time.time()
            self.end_time = None
        
        @property
        def duration_ms(self) -> float:
            end = self.end_time or time.time()
            return (end - self.start_time) * 1000
    
    timer = Timer()
    try:
        yield timer
    finally:
        timer.end_time = time.time()
        logger.debug(
            "Operation '%s' completed in %.1fms",
            operation_name, timer.duration_ms,
        )
