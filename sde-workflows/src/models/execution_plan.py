"""
execution_plan.py — Pydantic models for extraction and ingestion plans.

Defines the structure of execution plans that drive both the extraction
(Iceberg → SQL Server → BAK) and ingestion (BAK → SQL Server → Iceberg) pipelines.
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ArtifactType(str, Enum):
    """Supported artifact types."""
    BAK = "bak"
    MDF = "mdf"
    BOTH = "both"


class LoadMode(str, Enum):
    """Data load modes."""
    FULL = "full"
    INCREMENTAL = "incremental"


class WriteMode(str, Enum):
    """Data write modes for ingestion."""
    APPEND = "append"
    OVERWRITE = "overwrite"
    UPSERT = "upsert"


class ActivityType(str, Enum):
    """Activity types for the bidirectional pipeline."""
    EXTRACTION = "extraction"
    INGESTION = "ingestion"


class ColumnPlan(BaseModel):
    """Definition for a single column in the extraction plan."""
    source_name: str = Field(..., description="Source column name from Iceberg")
    target_name: str = Field(..., description="Target column name in SQL Server")
    source_type: str = Field(..., description="Arrow/Iceberg data type")
    target_type: str = Field(..., description="SQL Server data type")
    include: bool = Field(default=True, description="Whether to include this column")
    nullable: bool = Field(default=True, description="Whether the column allows NULLs")


class TablePlan(BaseModel):
    """Extraction plan for a single table."""
    name: str = Field(..., description="Table name")
    source_namespace: str = Field(..., description="Iceberg namespace")
    source_table: str = Field(..., description="Iceberg table name")
    target_schema: str = Field(default="dbo", description="SQL Server schema")
    target_table: str = Field(..., description="SQL Server table name")
    columns: list[ColumnPlan] = Field(default_factory=list, description="Column definitions")
    row_filter: Optional[str] = Field(None, description="PyIceberg filter expression")
    create_if_missing: bool = Field(default=True, description="Create table if it doesn't exist")
    load_mode: LoadMode = Field(default=LoadMode.FULL, description="Load mode")
    estimated_rows: Optional[int] = Field(None, description="Estimated row count")


class ExecutionPlan(BaseModel):
    """Complete execution plan for an extraction run."""
    tenant_id: str = Field(..., description="Tenant identifier")
    run_id: str = Field(..., description="Unique run identifier")
    exposure_ids: list[int] = Field(default_factory=list, description="Exposure IDs to filter")
    tables: list[TablePlan] = Field(default_factory=list, description="Tables to extract")
    artifact_type: ArtifactType = Field(default=ArtifactType.BAK, description="Output artifact type")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Plan creation time")
    
    # Computed properties
    @property
    def total_tables(self) -> int:
        """Total number of tables in the plan."""
        return len(self.tables)
    
    @property
    def total_columns(self) -> int:
        """Total number of columns across all tables."""
        return sum(len(t.columns) for t in self.tables)


class ExtractionConfig(BaseModel):
    """Configuration for the extraction service."""
    # Iceberg settings
    iceberg_catalog_type: str = Field(default="glue", description="Catalog type (glue, rest)")
    iceberg_catalog_name: str = Field(default="glue_catalog", description="Catalog name")
    iceberg_region: str = Field(default="us-east-1", description="AWS region for Glue catalog")
    iceberg_namespace: str = Field(default="default", description="Default Iceberg namespace")
    
    # SQL Server settings
    sql_host: str = Field(default="localhost", description="SQL Server host")
    sql_port: int = Field(default=1433, description="SQL Server port")
    sql_auth_type: str = Field(default="sql_login", description="Authentication type")
    sql_driver: str = Field(default="ODBC Driver 18 for SQL Server", description="ODBC driver")
    
    # Pipeline settings
    batch_size: int = Field(default=50000, description="Rows per write batch")
    max_workers: int = Field(default=4, description="Parallel table loading workers")
    write_backend: str = Field(default="pyodbc", description="Write backend (pyodbc, arrow_odbc, bcp)")
    
    # Activity Management
    activity_callback_url: Optional[str] = Field(None, description="Activity Management callback URL")
    
    # S3 settings
    config_bucket: Optional[str] = Field(None, description="S3 bucket for configuration overrides")
    config_prefix: str = Field(default="config/", description="S3 prefix for config files")


def build_execution_plan(
    tables: list[str],
    overrides: dict,
    exposure_ids: list[int],
    tenant_id: str,
    run_id: str,
) -> ExecutionPlan:
    """
    Build an execution plan from discovered tables and overrides.
    
    Parameters
    ----------
    tables : list[str]
        List of discovered Iceberg tables.
    overrides : dict
        Per-table configuration overrides.
    exposure_ids : list[int]
        Exposure IDs to filter by.
    tenant_id : str
        Tenant identifier.
    run_id : str
        Run identifier.
    
    Returns
    -------
    ExecutionPlan
        Complete execution plan.
    """
    table_plans = []
    
    for table_ref in tables:
        parts = table_ref.split(".", 1)
        namespace = parts[0] if len(parts) == 2 else "default"
        table_name = parts[1] if len(parts) == 2 else parts[0]
        
        # Check for overrides
        table_key = f"{namespace}_{table_name}"
        table_override = overrides.get(table_key, {})
        
        # Build row filter for exposure IDs if applicable
        row_filter = None
        if exposure_ids:
            ids_str = ", ".join(str(eid) for eid in exposure_ids)
            row_filter = f"exposure_id IN ({ids_str})"
        
        table_plan = TablePlan(
            name=table_name,
            source_namespace=namespace,
            source_table=table_name,
            target_table=table_override.get("target_table", table_name),
            row_filter=table_override.get("row_filter", row_filter),
            columns=[],  # Will be populated from schema
        )
        
        table_plans.append(table_plan)
    
    return ExecutionPlan(
        tenant_id=tenant_id,
        run_id=run_id,
        exposure_ids=exposure_ids,
        tables=table_plans,
    )


# =============================================================================
# Ingestion Plan Models (BAK → SQL Server → Iceberg)
# =============================================================================


class IngestionColumnPlan(BaseModel):
    """Definition for a single column in the ingestion plan."""
    source_name: str = Field(..., description="Source column name from SQL Server")
    target_name: str = Field(..., description="Target column name in Iceberg")
    source_type: str = Field(..., description="SQL Server data type")
    target_type: str = Field(..., description="Arrow/Iceberg data type")
    include: bool = Field(default=True, description="Whether to include this column")
    transform: Optional[str] = Field(None, description="Optional transformation expression")


class IngestionTablePlan(BaseModel):
    """Ingestion plan for a single table."""
    name: str = Field(..., description="Table name")
    source_schema: str = Field(default="dbo", description="SQL Server schema")
    source_table: str = Field(..., description="SQL Server table name")
    target_namespace: str = Field(..., description="Iceberg namespace")
    target_table: str = Field(..., description="Iceberg table name")
    columns: list[IngestionColumnPlan] = Field(default_factory=list, description="Column definitions")
    where_clause: Optional[str] = Field(None, description="SQL WHERE clause for filtering")
    write_mode: WriteMode = Field(default=WriteMode.APPEND, description="Write mode")
    create_if_missing: bool = Field(default=True, description="Create table if it doesn't exist")
    estimated_rows: Optional[int] = Field(None, description="Estimated row count")


class IngestionPlan(BaseModel):
    """Complete execution plan for an ingestion run."""
    tenant_id: str = Field(..., description="Tenant identifier")
    run_id: str = Field(..., description="Unique run identifier")
    activity_id: str = Field(..., description="Activity Management activity ID")
    
    # Source artifact
    source_bucket: str = Field(..., description="S3 bucket containing the artifact")
    source_key: str = Field(..., description="S3 key of the artifact")
    artifact_type: ArtifactType = Field(default=ArtifactType.BAK, description="Artifact type")
    checksum: Optional[str] = Field(None, description="Expected SHA256 checksum")
    
    # Cross-account access
    source_role_arn: Optional[str] = Field(None, description="IAM role for cross-account access")
    
    # Target configuration
    target_namespace: str = Field(default="default", description="Target Iceberg namespace")
    tables: list[IngestionTablePlan] = Field(default_factory=list, description="Tables to ingest")
    
    # Write settings
    write_mode: WriteMode = Field(default=WriteMode.APPEND, description="Default write mode")
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Plan creation time")
    
    @property
    def total_tables(self) -> int:
        """Total number of tables in the plan."""
        return len(self.tables)


class IngestionResult(BaseModel):
    """Result of an ingestion operation for a single table."""
    table_name: str = Field(..., description="Table name")
    status: str = Field(..., description="Status: success, failed, skipped")
    rows_ingested: int = Field(default=0, description="Number of rows ingested")
    error: Optional[str] = Field(None, description="Error message if failed")
    duration_seconds: Optional[float] = Field(None, description="Time taken in seconds")


class IngestionSummary(BaseModel):
    """Summary of a complete ingestion run."""
    tenant_id: str = Field(..., description="Tenant identifier")
    run_id: str = Field(..., description="Run identifier")
    activity_id: str = Field(..., description="Activity Management activity ID")
    
    # Results
    status: str = Field(..., description="Overall status: completed, failed, partial")
    tables_processed: int = Field(default=0, description="Total tables processed")
    tables_success: int = Field(default=0, description="Tables successfully ingested")
    tables_failed: int = Field(default=0, description="Tables that failed")
    total_rows: int = Field(default=0, description="Total rows ingested")
    
    # Timing
    started_at: datetime = Field(default_factory=datetime.utcnow, description="Start time")
    completed_at: Optional[datetime] = Field(None, description="Completion time")
    duration_seconds: Optional[float] = Field(None, description="Total duration")
    
    # Details
    results: list[IngestionResult] = Field(default_factory=list, description="Per-table results")
    error: Optional[str] = Field(None, description="Overall error message if failed")


def build_ingestion_plan(
    tenant_id: str,
    run_id: str,
    activity_id: str,
    source_bucket: str,
    source_key: str,
    target_namespace: str,
    tables: Optional[list[str]] = None,
    write_mode: WriteMode = WriteMode.APPEND,
    checksum: Optional[str] = None,
    source_role_arn: Optional[str] = None,
) -> IngestionPlan:
    """
    Build an ingestion plan for processing a tenant artifact.
    
    Parameters
    ----------
    tenant_id : str
        Tenant identifier.
    run_id : str
        Unique run identifier.
    activity_id : str
        Activity Management activity ID.
    source_bucket : str
        S3 bucket containing the artifact.
    source_key : str
        S3 key of the artifact.
    target_namespace : str
        Target Iceberg namespace.
    tables : list[str], optional
        Specific tables to ingest. None = all tables.
    write_mode : WriteMode
        Default write mode for tables.
    checksum : str, optional
        Expected SHA256 checksum.
    source_role_arn : str, optional
        IAM role for cross-account access.
    
    Returns
    -------
    IngestionPlan
        Complete ingestion plan.
    """
    # Determine artifact type from key
    artifact_type = ArtifactType.BAK
    if source_key.lower().endswith(".mdf"):
        artifact_type = ArtifactType.MDF
    
    # Build table plans if specific tables requested
    table_plans = []
    if tables:
        for table_name in tables:
            table_plan = IngestionTablePlan(
                name=table_name,
                source_table=table_name,
                target_namespace=target_namespace,
                target_table=table_name,
                write_mode=write_mode,
            )
            table_plans.append(table_plan)
    
    return IngestionPlan(
        tenant_id=tenant_id,
        run_id=run_id,
        activity_id=activity_id,
        source_bucket=source_bucket,
        source_key=source_key,
        artifact_type=artifact_type,
        checksum=checksum,
        source_role_arn=source_role_arn,
        target_namespace=target_namespace,
        tables=table_plans,
        write_mode=write_mode,
    )
