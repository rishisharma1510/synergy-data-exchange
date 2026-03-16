"""
Fargate task modules — AWS Fargate orchestration for bidirectional data pipeline.

Extraction modules (Iceberg → SQL Server → BAK → Tenant S3):
  - entrypoint          : Main Fargate extraction entry point
  - database_manager    : CREATE/DROP DATABASE lifecycle management
  - artifact_generator  : BACKUP DATABASE / sp_detach_db for .bak/.mdf generation
  - s3_uploader         : Multipart S3 upload with STS AssumeRole
  - progress_reporter   : Activity Management API callbacks
  - validation          : Row count, DBCC, checksum validation

Ingestion modules (Tenant S3 → BAK → SQL Server → Iceberg):
  - ingestion_entrypoint : Main Fargate ingestion entry point
  - artifact_restorer    : RESTORE DATABASE from BAK/ATTACH from MDF
"""

from src.fargate.entrypoint import main as fargate_main
from src.fargate.ingestion_entrypoint import main as ingestion_main
from src.fargate.database_manager import DatabaseManager
from src.fargate.artifact_generator import ArtifactGenerator
from src.fargate.artifact_restorer import ArtifactRestorer
from src.fargate.s3_uploader import S3Uploader, UploadResult, DownloadResult
from src.fargate.progress_reporter import ProgressReporter
from src.fargate.validation import Validator

__all__ = [
    # Entrypoints
    "fargate_main",
    "ingestion_main",
    # Database management
    "DatabaseManager",
    # Artifact handling
    "ArtifactGenerator",
    "ArtifactRestorer",
    # S3 operations
    "S3Uploader",
    "UploadResult",
    "DownloadResult",
    # Reporting & validation
    "ProgressReporter",
    "Validator",
]
