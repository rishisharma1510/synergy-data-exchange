"""
manifest.py — Artifact manifest schema for S3 delivery.

Defines the structure of the manifest JSON that accompanies extracted artifacts.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class ArtifactFile(BaseModel):
    """Metadata for a single artifact file."""
    file_name: str = Field(..., description="File name")
    file_type: str = Field(..., description="File type (bak, mdf, ldf)")
    size_bytes: int = Field(..., description="File size in bytes")
    checksum_sha256: str = Field(..., description="SHA256 checksum")
    s3_key: str = Field(..., description="S3 object key")
    s3_uri: str = Field(..., description="Full S3 URI")


class TableResult(BaseModel):
    """Results for a single table extraction."""
    table_name: str = Field(..., description="Table name")
    source_namespace: str = Field(..., description="Iceberg namespace")
    source_table: str = Field(..., description="Iceberg table name")
    rows_extracted: int = Field(..., description="Number of rows extracted")
    elapsed_seconds: float = Field(..., description="Time to extract in seconds")
    status: str = Field(..., description="Extraction status (success, failed)")
    error: Optional[str] = Field(None, description="Error message if failed")


class ValidationReport(BaseModel):
    """Database validation results."""
    passed: bool = Field(..., description="Whether all validations passed")
    errors: list[str] = Field(default_factory=list, description="Validation errors")
    warnings: list[str] = Field(default_factory=list, description="Validation warnings")
    row_counts: dict[str, int] = Field(default_factory=dict, description="Row counts per table")
    dbcc_result: Optional[str] = Field(None, description="DBCC CHECKDB result")
    checksums: dict[str, str] = Field(default_factory=dict, description="Sample checksums")


class Manifest(BaseModel):
    """Complete extraction manifest."""
    # Identifiers
    tenant_id: str = Field(..., description="Tenant identifier")
    run_id: str = Field(..., description="Run identifier")
    activity_id: str = Field(..., description="Activity Management ID")
    
    # Timestamps
    started_at: datetime = Field(..., description="Extraction start time")
    completed_at: datetime = Field(..., description="Extraction completion time")
    
    # Extraction parameters
    exposure_ids: list[int] = Field(default_factory=list, description="Extracted exposure IDs")
    artifact_type: str = Field(..., description="Artifact type (bak, mdf, both)")
    
    # Results
    tables: list[TableResult] = Field(default_factory=list, description="Per-table results")
    artifacts: list[ArtifactFile] = Field(default_factory=list, description="Generated artifacts")
    validation: Optional[ValidationReport] = Field(None, description="Validation report")
    
    # Summary
    total_rows: int = Field(default=0, description="Total rows extracted")
    total_tables: int = Field(default=0, description="Total tables extracted")
    total_size_bytes: int = Field(default=0, description="Total artifact size")
    
    # Status
    status: str = Field(..., description="Overall status (COMPLETED, FAILED)")
    error: Optional[str] = Field(None, description="Error message if failed")


def build_manifest(
    load_results: dict[str, dict],
    artifacts: list,
    upload_results: list,
    validation_report,
    tenant_id: str = "",
    run_id: str = "",
    activity_id: str = "",
    exposure_ids: list[int] = None,
    artifact_type: str = "bak",
    started_at: datetime = None,
) -> Manifest:
    """
    Build a manifest from extraction results.
    
    Parameters
    ----------
    load_results : dict
        Per-table loading results.
    artifacts : list
        Generated artifact files.
    upload_results : list
        S3 upload results.
    validation_report : ValidationReport
        Validation results.
    tenant_id : str
        Tenant identifier.
    run_id : str
        Run identifier.
    activity_id : str
        Activity ID.
    exposure_ids : list[int]
        Extracted exposure IDs.
    artifact_type : str
        Artifact type.
    started_at : datetime
        Start time.
    
    Returns
    -------
    Manifest
        Complete manifest.
    """
    # Build table results
    table_results = []
    total_rows = 0
    
    for table_name, result in load_results.items():
        rows = result.get("rows", 0)
        total_rows += rows
        
        table_results.append(TableResult(
            table_name=table_name,
            source_namespace=result.get("namespace", "default"),
            source_table=result.get("table", table_name),
            rows_extracted=rows,
            elapsed_seconds=result.get("elapsed_seconds", 0),
            status=result.get("status", "success"),
            error=result.get("error"),
        ))
    
    # Build artifact files
    artifact_files = []
    total_size = 0
    
    for artifact, upload in zip(artifacts, upload_results):
        total_size += artifact.size_bytes
        
        artifact_files.append(ArtifactFile(
            file_name=artifact.file_path.split("/")[-1] if "/" in artifact.file_path else artifact.file_path,
            file_type=artifact.file_type,
            size_bytes=artifact.size_bytes,
            checksum_sha256=artifact.checksum_sha256,
            s3_key=upload.s3_key,
            s3_uri=upload.s3_uri,
        ))
    
    # Convert validation report if needed
    validation = None
    if validation_report:
        validation = ValidationReport(
            passed=validation_report.passed,
            errors=validation_report.errors,
            warnings=validation_report.warnings,
            row_counts=validation_report.row_counts,
            dbcc_result=validation_report.dbcc_result,
            checksums=validation_report.checksums,
        )
    
    return Manifest(
        tenant_id=tenant_id,
        run_id=run_id,
        activity_id=activity_id,
        started_at=started_at or datetime.utcnow(),
        completed_at=datetime.utcnow(),
        exposure_ids=exposure_ids or [],
        artifact_type=artifact_type,
        tables=table_results,
        artifacts=artifact_files,
        validation=validation,
        total_rows=total_rows,
        total_tables=len(table_results),
        total_size_bytes=total_size,
        status="COMPLETED" if (not validation or validation.passed) else "FAILED",
        error=None if (not validation or validation.passed) else "; ".join(validation.errors),
    )
