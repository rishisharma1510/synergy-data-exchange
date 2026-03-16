"""
SID Allocation Audit Trail

Logs every SID allocation for compliance, debugging, and anomaly detection.

Records:
- Original ID → New SID mapping per record
- Timestamp, table, column context
- Job and tenant metadata

Storage backends:
- S3 (Parquet) - optimized for analytics
- DynamoDB - for real-time lookups
- Local JSON - for development/testing
"""

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


@dataclass
class AllocationRecord:
    """Single SID allocation record for audit trail."""
    table_name: str
    column_name: str
    original_id: int
    new_sid: int
    job_id: str
    tenant_id: str
    timestamp: float = field(default_factory=time.time)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "column_name": self.column_name,
            "original_id": self.original_id,
            "new_sid": self.new_sid,
            "job_id": self.job_id,
            "tenant_id": self.tenant_id,
            "timestamp": self.timestamp,
            "timestamp_iso": datetime.fromtimestamp(
                self.timestamp, tz=timezone.utc
            ).isoformat(),
        }


@dataclass
class TableAllocationBatch:
    """Batch of allocations for a single table."""
    table_name: str
    column_name: str
    job_id: str
    tenant_id: str
    records: List[AllocationRecord] = field(default_factory=list)
    sid_range_start: int = 0
    sid_range_end: int = 0
    timestamp: float = field(default_factory=time.time)
    
    @classmethod
    def from_mapping(
        cls,
        table_name: str,
        column_name: str,
        mapping: Dict[int, int],
        job_id: str,
        tenant_id: str,
    ) -> "TableAllocationBatch":
        """Create batch from a SID mapping."""
        records = [
            AllocationRecord(
                table_name=table_name,
                column_name=column_name,
                original_id=original,
                new_sid=new_sid,
                job_id=job_id,
                tenant_id=tenant_id,
            )
            for original, new_sid in mapping.items()
        ]
        
        sids = list(mapping.values())
        return cls(
            table_name=table_name,
            column_name=column_name,
            job_id=job_id,
            tenant_id=tenant_id,
            records=records,
            sid_range_start=min(sids) if sids else 0,
            sid_range_end=max(sids) if sids else 0,
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "column_name": self.column_name,
            "job_id": self.job_id,
            "tenant_id": self.tenant_id,
            "record_count": len(self.records),
            "sid_range_start": self.sid_range_start,
            "sid_range_end": self.sid_range_end,
            "timestamp": self.timestamp,
            "timestamp_iso": datetime.fromtimestamp(
                self.timestamp, tz=timezone.utc
            ).isoformat(),
            "records": [r.to_dict() for r in self.records],
        }


class AuditTrailBackend:
    """Abstract base for audit trail storage backends."""
    
    def write_batch(self, batch: TableAllocationBatch) -> None:
        """Write a batch of allocation records."""
        raise NotImplementedError
    
    def query_by_job(self, job_id: str) -> List[TableAllocationBatch]:
        """Query all allocations for a job."""
        raise NotImplementedError
    
    def query_by_original_id(
        self,
        table_name: str,
        original_id: int,
    ) -> List[AllocationRecord]:
        """Query allocation records by original ID."""
        raise NotImplementedError
    
    def query_by_sid(
        self,
        table_name: str,
        new_sid: int,
    ) -> Optional[AllocationRecord]:
        """Query allocation record by new SID (should be unique)."""
        raise NotImplementedError


class LocalAuditTrail(AuditTrailBackend):
    """
    Local filesystem audit trail for development/testing.
    
    Stores as JSON files organized by tenant/job.
    """
    
    def __init__(self, audit_dir: str = "/tmp/sid_audit"):
        self._audit_dir = Path(audit_dir)
        self._audit_dir.mkdir(parents=True, exist_ok=True)
        
        # In-memory index for queries
        self._records: Dict[str, List[AllocationRecord]] = {}
    
    def _get_path(self, batch: TableAllocationBatch) -> Path:
        """Get audit file path for a batch."""
        job_dir = self._audit_dir / batch.tenant_id / batch.job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        return job_dir / f"{batch.table_name}.json"
    
    def write_batch(self, batch: TableAllocationBatch) -> None:
        """Write batch to local JSON file."""
        path = self._get_path(batch)
        
        try:
            with open(path, "w") as f:
                json.dump(batch.to_dict(), f, indent=2)
            
            # Update in-memory index
            key = f"{batch.tenant_id}:{batch.job_id}:{batch.table_name}"
            self._records[key] = batch.records
            
            logger.info(
                "Audit trail written: table=%s, records=%d, path=%s",
                batch.table_name,
                len(batch.records),
                path,
            )
        except Exception as e:
            logger.error(f"Failed to write audit trail: {e}")
            raise
    
    def query_by_job(self, job_id: str) -> List[TableAllocationBatch]:
        """Query all batches for a job."""
        batches = []
        for key, records in self._records.items():
            if job_id in key:
                parts = key.split(":")
                if len(parts) == 3 and parts[1] == job_id:
                    batch = TableAllocationBatch(
                        table_name=parts[2],
                        column_name=records[0].column_name if records else "",
                        job_id=job_id,
                        tenant_id=parts[0],
                        records=records,
                    )
                    batches.append(batch)
        return batches
    
    def query_by_original_id(
        self,
        table_name: str,
        original_id: int,
    ) -> List[AllocationRecord]:
        """Query by original ID across all jobs."""
        matches = []
        for key, records in self._records.items():
            if table_name in key:
                for record in records:
                    if record.original_id == original_id:
                        matches.append(record)
        return matches
    
    def query_by_sid(
        self,
        table_name: str,
        new_sid: int,
    ) -> Optional[AllocationRecord]:
        """Query by new SID (unique)."""
        for key, records in self._records.items():
            if table_name in key:
                for record in records:
                    if record.new_sid == new_sid:
                        return record
        return None


class S3ParquetAuditTrail(AuditTrailBackend):
    """
    S3 Parquet audit trail for production analytics.
    
    Optimized for:
    - Large-scale storage
    - Athena/Spark queries
    - Cost-effective long-term retention
    """
    
    def __init__(
        self,
        bucket: str,
        prefix: str = "sid-audit",
        s3_client=None,
    ):
        self._bucket = bucket
        self._prefix = prefix
        self._s3_client = s3_client
    
    @property
    def s3_client(self):
        """Lazy initialization of S3 client."""
        if self._s3_client is None:
            import boto3
            self._s3_client = boto3.client("s3")
        return self._s3_client
    
    def _get_key(self, batch: TableAllocationBatch) -> str:
        """Get S3 object key for batch."""
        date_str = datetime.fromtimestamp(
            batch.timestamp, tz=timezone.utc
        ).strftime("%Y/%m/%d")
        
        return (
            f"{self._prefix}/"
            f"tenant={batch.tenant_id}/"
            f"date={date_str}/"
            f"job={batch.job_id}/"
            f"{batch.table_name}.parquet"
        )
    
    def write_batch(self, batch: TableAllocationBatch) -> None:
        """Write batch to S3 as Parquet."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            from io import BytesIO
            
            # Convert to PyArrow Table
            data = {
                "table_name": [r.table_name for r in batch.records],
                "column_name": [r.column_name for r in batch.records],
                "original_id": [r.original_id for r in batch.records],
                "new_sid": [r.new_sid for r in batch.records],
                "job_id": [r.job_id for r in batch.records],
                "tenant_id": [r.tenant_id for r in batch.records],
                "timestamp": [r.timestamp for r in batch.records],
            }
            table = pa.table(data)
            
            # Write to buffer
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Upload to S3
            key = self._get_key(batch)
            self.s3_client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=buffer.getvalue(),
                ContentType="application/octet-stream",
            )
            
            logger.info(
                "Audit trail written to S3: bucket=%s, key=%s, records=%d",
                self._bucket,
                key,
                len(batch.records),
            )
            
        except Exception as e:
            logger.error(f"Failed to write audit trail to S3: {e}")
            raise
    
    def query_by_job(self, job_id: str) -> List[TableAllocationBatch]:
        """Query by job - requires listing S3 objects."""
        logger.warning(
            "S3 query_by_job not optimized - consider using Athena"
        )
        return []
    
    def query_by_original_id(
        self,
        table_name: str,
        original_id: int,
    ) -> List[AllocationRecord]:
        """Query by original ID - consider using Athena."""
        logger.warning(
            "S3 query_by_original_id not optimized - consider using Athena"
        )
        return []
    
    def query_by_sid(
        self,
        table_name: str,
        new_sid: int,
    ) -> Optional[AllocationRecord]:
        """Query by SID - consider using Athena."""
        logger.warning(
            "S3 query_by_sid not optimized - consider using Athena"
        )
        return None


class DynamoDBAuditTrail(AuditTrailBackend):
    """
    DynamoDB audit trail for real-time lookups.
    
    Table schema:
    - PK: {table_name}#{new_sid}
    - SK: {tenant_id}#{job_id}
    - GSI1: original_id for reverse lookups
    """
    
    def __init__(
        self,
        table_name: str = "sid-audit-trail",
        dynamodb_client=None,
    ):
        self._table_name = table_name
        self._dynamodb_client = dynamodb_client
    
    @property
    def dynamodb_client(self):
        """Lazy initialization of DynamoDB client."""
        if self._dynamodb_client is None:
            import boto3
            self._dynamodb_client = boto3.client("dynamodb")
        return self._dynamodb_client
    
    def write_batch(self, batch: TableAllocationBatch) -> None:
        """Write batch to DynamoDB using batch_write."""
        try:
            from boto3.dynamodb.types import TypeSerializer
            serializer = TypeSerializer()
            
            # DynamoDB batch_write_item limit is 25
            items = []
            for record in batch.records:
                item = {
                    "pk": {"S": f"{record.table_name}#{record.new_sid}"},
                    "sk": {"S": f"{record.tenant_id}#{record.job_id}"},
                    "table_name": {"S": record.table_name},
                    "column_name": {"S": record.column_name},
                    "original_id": {"N": str(record.original_id)},
                    "new_sid": {"N": str(record.new_sid)},
                    "job_id": {"S": record.job_id},
                    "tenant_id": {"S": record.tenant_id},
                    "timestamp": {"N": str(record.timestamp)},
                }
                items.append({"PutRequest": {"Item": item}})
            
            # Batch write in chunks of 25
            for i in range(0, len(items), 25):
                chunk = items[i:i + 25]
                self.dynamodb_client.batch_write_item(
                    RequestItems={self._table_name: chunk}
                )
            
            logger.info(
                "Audit trail written to DynamoDB: table=%s, records=%d",
                batch.table_name,
                len(batch.records),
            )
            
        except Exception as e:
            logger.error(f"Failed to write audit trail to DynamoDB: {e}")
            raise
    
    def query_by_sid(
        self,
        table_name: str,
        new_sid: int,
    ) -> Optional[AllocationRecord]:
        """Query by new SID using primary key."""
        try:
            response = self.dynamodb_client.get_item(
                TableName=self._table_name,
                Key={"pk": {"S": f"{table_name}#{new_sid}"}},
            )
            
            if "Item" not in response:
                return None
            
            item = response["Item"]
            return AllocationRecord(
                table_name=item["table_name"]["S"],
                column_name=item["column_name"]["S"],
                original_id=int(item["original_id"]["N"]),
                new_sid=int(item["new_sid"]["N"]),
                job_id=item["job_id"]["S"],
                tenant_id=item["tenant_id"]["S"],
                timestamp=float(item["timestamp"]["N"]),
            )
            
        except Exception as e:
            logger.error(f"Failed to query audit trail: {e}")
            return None
    
    def query_by_job(self, job_id: str) -> List[TableAllocationBatch]:
        """Query by job - requires scan or GSI."""
        logger.warning("DynamoDB query_by_job requires GSI - not implemented")
        return []
    
    def query_by_original_id(
        self,
        table_name: str,
        original_id: int,
    ) -> List[AllocationRecord]:
        """Query by original ID - requires GSI."""
        logger.warning("DynamoDB query_by_original_id requires GSI")
        return []


class AuditTrailCollector:
    """
    High-level audit trail collector.
    
    Buffers records and writes to backend(s) in batches.
    Can write to multiple backends simultaneously (e.g., S3 + DynamoDB).
    """
    
    def __init__(
        self,
        backends: List[AuditTrailBackend],
        job_id: str,
        tenant_id: str,
    ):
        self._backends = backends
        self._job_id = job_id
        self._tenant_id = tenant_id
        self._batches_written = 0
        self._records_written = 0
    
    def record_allocation(
        self,
        table_name: str,
        column_name: str,
        mapping: Dict[int, int],
    ) -> None:
        """
        Record a table's SID allocations.
        
        Args:
            table_name: Name of the table.
            column_name: PK column name.
            mapping: Dict of old_sid → new_sid.
        """
        if not mapping:
            return
        
        batch = TableAllocationBatch.from_mapping(
            table_name=table_name,
            column_name=column_name,
            mapping=mapping,
            job_id=self._job_id,
            tenant_id=self._tenant_id,
        )
        
        for backend in self._backends:
            try:
                backend.write_batch(batch)
            except Exception as e:
                logger.error(
                    "Failed to write to backend %s: %s",
                    type(backend).__name__,
                    e,
                )
        
        self._batches_written += 1
        self._records_written += len(mapping)
    
    @property
    def batches_written(self) -> int:
        return self._batches_written
    
    @property
    def records_written(self) -> int:
        return self._records_written


def create_audit_trail(
    job_id: str,
    tenant_id: str,
    use_s3: bool = False,
    use_dynamodb: bool = False,
    s3_bucket: str = "",
    s3_prefix: str = "sid-audit",
    dynamodb_table: str = "sid-audit-trail",
    local_dir: str = "/tmp/sid_audit",
) -> AuditTrailCollector:
    """
    Factory function to create an audit trail collector.
    
    Args:
        job_id: Job identifier.
        tenant_id: Tenant identifier.
        use_s3: Enable S3 Parquet backend.
        use_dynamodb: Enable DynamoDB backend.
        s3_bucket: S3 bucket for Parquet files.
        s3_prefix: S3 prefix for audit objects.
        dynamodb_table: DynamoDB table name.
        local_dir: Local directory for JSON fallback.
        
    Returns:
        Configured AuditTrailCollector.
    """
    backends: List[AuditTrailBackend] = []
    
    # Always add local backend for dev/debugging
    backends.append(LocalAuditTrail(audit_dir=local_dir))
    
    if use_s3:
        if not s3_bucket:
            raise ValueError("S3 bucket required when use_s3=True")
        backends.append(S3ParquetAuditTrail(bucket=s3_bucket, prefix=s3_prefix))
    
    if use_dynamodb:
        backends.append(DynamoDBAuditTrail(table_name=dynamodb_table))
    
    return AuditTrailCollector(
        backends=backends,
        job_id=job_id,
        tenant_id=tenant_id,
    )
