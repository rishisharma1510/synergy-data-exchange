"""
Core modules — proven POC code for bidirectional Iceberg ↔ SQL Server data movement.

Modules:
  - config          : Configuration dataclasses (IcebergConfig, SqlServerConfig, etc.)
  - iceberg_reader  : PyIceberg-based table reader with streaming support
  - iceberg_writer  : PyIceberg-based table writer for ingestion
  - sql_writer      : SQL Server writer with 3 backends (pyodbc, arrow_odbc, bcp)
  - sql_reader      : SQL Server reader for ingestion (SQL → Arrow)
  - column_mapper   : 6-layer automatic column mapping engine
  - mapping_config  : JSON-based mapping configuration management
  - ingest          : Main ingestion orchestration logic
"""

from src.core.config import (
    IcebergConfig, 
    SqlServerConfig, 
    AppConfig, 
    load_config,
    IngestionConfig,
    IngestionActivityConfig,
    S3Config,
    load_ingestion_activity_config,
)
from src.core.iceberg_reader import IcebergReader
from src.core.iceberg_writer import IcebergWriter
from src.core.sql_writer import SqlServerWriter
from src.core.sql_reader import SqlServerReader
from src.core.column_mapper import ColumnMapper, MappingResult, ColumnMatch
from src.core.mapping_config import (
    generate_mapping_for_table,
    load_mapping_file,
    save_mapping_file,
)

__all__ = [
    # Config
    "IcebergConfig",
    "SqlServerConfig", 
    "AppConfig",
    "load_config",
    "IngestionConfig",
    "IngestionActivityConfig",
    "S3Config",
    "load_ingestion_activity_config",
    # Readers/Writers
    "IcebergReader",
    "IcebergWriter",
    "SqlServerWriter",
    "SqlServerReader",
    # Mapping
    "ColumnMapper",
    "MappingResult",
    "ColumnMatch",
    "generate_mapping_for_table",
    "load_mapping_file",
    "save_mapping_file",
]
