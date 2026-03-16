"""
Checkpoint/Resume Module for SID Transformation Jobs

Enables recovery from interrupted jobs by:
- Saving progress after each table is processed
- Persisting SID mappings for FK consistency
- Resuming from last successful checkpoint

Storage backends: S3 (production) or local JSON (dev/testing)
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class CheckpointState:
    """Serializable checkpoint state for a SID transformation job."""
    job_id: str
    tenant_id: str
    
    # Tables processed successfully
    completed_tables: List[str] = field(default_factory=list)
    
    # SID mappings: { table_name: { old_sid: new_sid } }
    sid_mappings: Dict[str, Dict[str, int]] = field(default_factory=dict)
    
    # Last table being processed (for debugging)
    current_table: Optional[str] = None
    
    # Timestamps
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    
    # Job status
    status: str = "in_progress"  # in_progress, completed, failed
    error: Optional[str] = None
    
    def mark_table_complete(
        self,
        table_name: str,
        mapping: Dict[int, int],
    ) -> None:
        """Record successful completion of a table."""
        if table_name not in self.completed_tables:
            self.completed_tables.append(table_name)
        
        # Store mapping as string keys for JSON serialization
        self.sid_mappings[table_name] = {
            str(k): v for k, v in mapping.items()
        }
        self.current_table = None
        self.updated_at = time.time()
    
    def set_current_table(self, table_name: str) -> None:
        """Set the table currently being processed."""
        self.current_table = table_name
        self.updated_at = time.time()
    
    def is_table_completed(self, table_name: str) -> bool:
        """Check if a table has already been processed."""
        return table_name in self.completed_tables
    
    def mark_completed(self) -> None:
        """Mark job as completed."""
        self.status = "completed"
        self.current_table = None
        self.updated_at = time.time()
    
    def mark_failed(self, error: str) -> None:
        """Mark job as failed."""
        self.status = "failed"
        self.error = error
        self.updated_at = time.time()
    
    def get_mapping(self, table_name: str) -> Dict[int, int]:
        """
        Get SID mapping for a table (convert string keys back to int).
        
        Returns:
            Dict mapping old SID → new SID.
        """
        str_mapping = self.sid_mappings.get(table_name, {})
        return {int(k): v for k, v in str_mapping.items()}
    
    def get_all_mappings(self) -> Dict[str, Dict[int, int]]:
        """Get all SID mappings with int keys."""
        return {
            table: self.get_mapping(table)
            for table in self.sid_mappings
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CheckpointState":
        """Create from dictionary."""
        return cls(**data)


class CheckpointManager:
    """
    Abstract base for checkpoint storage backends.
    
    Provides save/load operations for CheckpointState.
    """
    
    def save(self, state: CheckpointState) -> None:
        """Save checkpoint state."""
        raise NotImplementedError
    
    def load(self, job_id: str, tenant_id: str) -> Optional[CheckpointState]:
        """Load checkpoint state if exists."""
        raise NotImplementedError
    
    def delete(self, job_id: str, tenant_id: str) -> None:
        """Delete checkpoint after successful completion."""
        raise NotImplementedError
    
    def exists(self, job_id: str, tenant_id: str) -> bool:
        """Check if a checkpoint exists."""
        raise NotImplementedError


class LocalCheckpointManager(CheckpointManager):
    """
    Local filesystem checkpoint storage.
    
    Useful for development and testing.
    """
    
    def __init__(self, checkpoint_dir: str = "/tmp/sid_checkpoints"):
        self._checkpoint_dir = Path(checkpoint_dir)
        self._checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_path(self, job_id: str, tenant_id: str) -> Path:
        """Get checkpoint file path."""
        return self._checkpoint_dir / f"{tenant_id}_{job_id}.json"
    
    def save(self, state: CheckpointState) -> None:
        """Save checkpoint state to local file."""
        path = self._get_path(state.job_id, state.tenant_id)
        
        try:
            with open(path, "w") as f:
                json.dump(state.to_dict(), f, indent=2)
            
            logger.info(
                "Checkpoint saved: job=%s, tables=%d",
                state.job_id,
                len(state.completed_tables),
            )
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
            raise
    
    def load(self, job_id: str, tenant_id: str) -> Optional[CheckpointState]:
        """Load checkpoint state from local file."""
        path = self._get_path(job_id, tenant_id)
        
        if not path.exists():
            return None
        
        try:
            with open(path, "r") as f:
                data = json.load(f)
            
            state = CheckpointState.from_dict(data)
            logger.info(
                "Checkpoint loaded: job=%s, tables=%d",
                job_id,
                len(state.completed_tables),
            )
            return state
            
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return None
    
    def delete(self, job_id: str, tenant_id: str) -> None:
        """Delete checkpoint file."""
        path = self._get_path(job_id, tenant_id)
        
        try:
            if path.exists():
                path.unlink()
                logger.info("Checkpoint deleted: job=%s", job_id)
        except Exception as e:
            logger.error(f"Failed to delete checkpoint: {e}")
    
    def exists(self, job_id: str, tenant_id: str) -> bool:
        """Check if checkpoint file exists."""
        return self._get_path(job_id, tenant_id).exists()


class S3CheckpointManager(CheckpointManager):
    """
    S3 checkpoint storage for production use.
    
    Stores checkpoints in a dedicated bucket/prefix.
    """
    
    def __init__(
        self,
        bucket: str,
        prefix: str = "sid-checkpoints",
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
    
    def _get_key(self, job_id: str, tenant_id: str) -> str:
        """Get S3 object key for checkpoint."""
        return f"{self._prefix}/{tenant_id}/{job_id}.json"
    
    def save(self, state: CheckpointState) -> None:
        """Save checkpoint state to S3."""
        key = self._get_key(state.job_id, state.tenant_id)
        
        try:
            body = json.dumps(state.to_dict(), indent=2)
            self.s3_client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
            )
            
            logger.info(
                "Checkpoint saved to S3: bucket=%s, key=%s, tables=%d",
                self._bucket,
                key,
                len(state.completed_tables),
            )
        except Exception as e:
            logger.error(f"Failed to save checkpoint to S3: {e}")
            raise
    
    def load(self, job_id: str, tenant_id: str) -> Optional[CheckpointState]:
        """Load checkpoint state from S3."""
        key = self._get_key(job_id, tenant_id)
        
        try:
            response = self.s3_client.get_object(
                Bucket=self._bucket,
                Key=key,
            )
            body = response["Body"].read().decode("utf-8")
            data = json.loads(body)
            
            state = CheckpointState.from_dict(data)
            logger.info(
                "Checkpoint loaded from S3: key=%s, tables=%d",
                key,
                len(state.completed_tables),
            )
            return state
            
        except self.s3_client.exceptions.NoSuchKey:
            return None
        except Exception as e:
            logger.error(f"Failed to load checkpoint from S3: {e}")
            return None
    
    def delete(self, job_id: str, tenant_id: str) -> None:
        """Delete checkpoint from S3."""
        key = self._get_key(job_id, tenant_id)
        
        try:
            self.s3_client.delete_object(
                Bucket=self._bucket,
                Key=key,
            )
            logger.info("Checkpoint deleted from S3: key=%s", key)
        except Exception as e:
            logger.error(f"Failed to delete checkpoint from S3: {e}")
    
    def exists(self, job_id: str, tenant_id: str) -> bool:
        """Check if checkpoint exists in S3."""
        key = self._get_key(job_id, tenant_id)
        
        try:
            self.s3_client.head_object(
                Bucket=self._bucket,
                Key=key,
            )
            return True
        except:
            return False


def create_checkpoint_manager(
    use_s3: bool = False,
    bucket: str = "",
    prefix: str = "sid-checkpoints",
    local_dir: str = "/tmp/sid_checkpoints",
) -> CheckpointManager:
    """
    Factory function to create a checkpoint manager.
    
    Args:
        use_s3: If True, use S3 backend.
        bucket: S3 bucket (required if use_s3=True).
        prefix: S3 prefix for checkpoint objects.
        local_dir: Local directory for filesystem backend.
        
    Returns:
        Configured CheckpointManager.
    """
    if use_s3:
        if not bucket:
            raise ValueError("S3 bucket required when use_s3=True")
        return S3CheckpointManager(bucket=bucket, prefix=prefix)
    
    return LocalCheckpointManager(checkpoint_dir=local_dir)
