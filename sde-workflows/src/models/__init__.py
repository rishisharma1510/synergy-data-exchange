"""
Pydantic models for execution plans, configurations, and manifests.

Modules:
  - execution_plan : Models for extraction plan, ingestion plan, and table configuration
  - manifest       : Artifact manifest schema for S3 delivery
"""

from src.models.execution_plan import (
    # Enums
    ArtifactType,
    LoadMode,
    WriteMode,
    ActivityType,
    # Extraction
    ColumnPlan,
    ExecutionPlan,
    ExtractionConfig,
    TablePlan,
    build_execution_plan,
    # Ingestion
    IngestionColumnPlan,
    IngestionTablePlan,
    IngestionPlan,
    IngestionResult,
    IngestionSummary,
    build_ingestion_plan,
)
from src.models.manifest import (
    ArtifactFile,
    Manifest,
    TableResult,
    ValidationReport,
    build_manifest,
)

__all__ = [
    # Enums
    "ArtifactType",
    "LoadMode",
    "WriteMode",
    "ActivityType",
    # Extraction plan
    "ColumnPlan",
    "ExecutionPlan",
    "ExtractionConfig",
    "TablePlan",
    "build_execution_plan",
    # Ingestion plan
    "IngestionColumnPlan",
    "IngestionTablePlan",
    "IngestionPlan",
    "IngestionResult",
    "IngestionSummary",
    "build_ingestion_plan",
    # Manifest
    "ArtifactFile",
    "Manifest",
    "TableResult",
    "ValidationReport",
    "build_manifest",
]
