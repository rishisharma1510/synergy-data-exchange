"""
Data Extraction Service — Iceberg to SQL Server pipeline.

This package contains the complete data extraction pipeline:
  - core/    : Core ingestion modules (Iceberg reader, SQL writer, column mapper)
  - fargate/ : AWS Fargate task orchestration and artifact generation
  - models/  : Pydantic models for execution plans and manifests
"""

__version__ = "0.1.0"
