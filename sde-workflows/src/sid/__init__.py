"""
SID Transformation Module

Handles Source Identifier (SID) remapping for data migration:
- Range allocation from SID API
- In-memory PyArrow transformation
- FK cascade updates based on parent→child relationships
- Metrics collection and CloudWatch integration
- Checkpoint/resume for interrupted jobs
- SID allocation audit trail
- Parallel table processing
"""

from .transformer import SIDTransformer, TransformResult, TransformJobResult, create_transformer
from .sid_client import SIDClient, MockSIDClient
from .config import SID_CONFIG, get_processing_order, get_fk_relationships
from .metrics import JobMetrics, MetricsCollector, timed_operation
from .checkpoint import (
    CheckpointState,
    CheckpointManager,
    LocalCheckpointManager,
    S3CheckpointManager,
    create_checkpoint_manager,
)
from .audit_trail import (
    AllocationRecord,
    TableAllocationBatch,
    AuditTrailBackend,
    LocalAuditTrail,
    S3ParquetAuditTrail,
    DynamoDBAuditTrail,
    AuditTrailCollector,
    create_audit_trail,
)
from .parallel import (
    ParallelTransformer,
    LevelResult,
    ParallelJobResult,
    create_parallel_transformer,
)
from .enhanced import EnhancedTransformer, EnhancedJobResult
from .exposure_client import (
    ExposureMetadata,
    ExposureSetResult,
    ExposureAllocationResponse,
    ExposureClientBase,
    MockExposureClient,
    ExposureClient,
    ExposureAPIError,
    extract_exposure_metadata,
    create_exposure_client,
)

__all__ = [
    # Core transformer
    "SIDTransformer",
    "TransformResult",
    "TransformJobResult",
    "create_transformer",
    # SID client
    "SIDClient",
    "MockSIDClient",
    # Configuration
    "SID_CONFIG",
    "get_processing_order",
    "get_fk_relationships",
    # Metrics
    "JobMetrics",
    "MetricsCollector",
    "timed_operation",
    # Checkpoint/Resume
    "CheckpointState",
    "CheckpointManager",
    "LocalCheckpointManager",
    "S3CheckpointManager",
    "create_checkpoint_manager",
    # Audit Trail
    "AllocationRecord",
    "TableAllocationBatch",
    "AuditTrailBackend",
    "LocalAuditTrail",
    "S3ParquetAuditTrail",
    "DynamoDBAuditTrail",
    "AuditTrailCollector",
    "create_audit_trail",
    # Parallel Processing
    "ParallelTransformer",
    "LevelResult",
    "ParallelJobResult",
    "create_parallel_transformer",
    # Enhanced Transformer
    "EnhancedTransformer",
    "EnhancedJobResult",
    # Exposure API
    "ExposureMetadata",
    "ExposureSetResult",
    "ExposureAllocationResponse",
    "ExposureClientBase",
    "MockExposureClient",
    "ExposureClient",
    "ExposureAPIError",
    "extract_exposure_metadata",
    "create_exposure_client",
]
