"""
Configuration loader — reads connection details from environment variables.
Supports .env file for local development, but not required in containerized environments.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv


def _load_env() -> None:
    """Load .env from the project root (parent of src/) if it exists.
    
    In Fargate/ECS environments, environment variables are injected directly 
    via task definition overrides, so .env file is not required.
    """
    # Skip .env loading if running in a container environment
    if os.environ.get("PIPELINE_MODE"):
        # Environment variables are already set by container orchestration
        return
    
    # Try multiple locations for the .env file (for local development)
    possible_paths = [
        Path(__file__).resolve().parent.parent.parent / ".env",  # DataIngestion/.env
        Path(__file__).resolve().parent / ".env",  # src/core/.env (fallback)
    ]
    
    for env_path in possible_paths:
        if env_path.exists():
            load_dotenv(env_path, override=True)
            return
    
    # No .env found - this is fine if env vars are set directly
    # Only raise if critical vars are missing (checked elsewhere)


@dataclass
class IcebergConfig:
    catalog_type: str = "glue"
    catalog_name: str = "glue_catalog"
    catalog_uri: str = ""           # REST catalog URI (for type=rest)
    s3_endpoint: str = ""           # Override for non-AWS S3 (e.g. MinIO)
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_region: str = "us-east-1"
    warehouse: str = ""
    namespace: str = "default"
    table: str = ""
    # Glue-specific
    glue_region: str = "us-east-1"  # AWS region for Glue catalog
    glue_profile: str = ""          # Named AWS profile (blank = default chain)
    glue_catalog_id: str = ""       # Glue catalog ID (blank = account default)

    def catalog_properties(self) -> dict:
        """Return a dict suitable for pyiceberg catalog construction."""
        props: dict = {
            "type": self.catalog_type,
        }

        # --- Glue catalog ---
        if self.catalog_type == "glue":
            if self.glue_region:
                props["glue.region"] = self.glue_region
                props["region"] = self.glue_region  # Also set generic region
            if self.glue_profile:
                props["profile_name"] = self.glue_profile
            if self.glue_catalog_id:
                props["glue.catalog-id"] = self.glue_catalog_id
            # Limit retries so a missing IAM permission / transient error fails
            # fast instead of hanging for 10+ minutes (default MAX_RETRIES=10).
            props["glue.max-retries"] = 2

        # --- REST catalog ---
        if self.catalog_uri:
            props["uri"] = self.catalog_uri
        if self.warehouse:
            props["warehouse"] = self.warehouse

        # --- S3 storage overrides (optional — defaults to AWS SDK chain) ---
        if self.s3_endpoint:
            props["s3.endpoint"] = self.s3_endpoint
        if self.s3_access_key:
            props["s3.access-key-id"] = self.s3_access_key
        if self.s3_secret_key:
            props["s3.secret-access-key"] = self.s3_secret_key
        if self.s3_region:
            props["s3.region"] = self.s3_region
        return props


@dataclass
class SqlServerConfig:
    host: str = "localhost"
    port: int = 1433
    database: str = ""
    schema: str = "dbo"
    table: str = ""
    username: str = ""
    password: str = ""
    auth_type: str = "sql_login"  # sql_login | windows | azure_ad
    driver: str = "ODBC Driver 18 for SQL Server"
    trust_cert: bool = True
    connection_timeout: int = 600  # 10 minutes
    data_share_path: str = ""  # UNC path for remote file access, e.g. \\server\sqldata

    def connection_string(self) -> str:
        """Build a pyodbc-compatible connection string."""
        parts = [
            f"DRIVER={{{self.driver}}}",
            f"SERVER={self.host},{self.port}",
            f"DATABASE={self.database}",
            f"Connection Timeout={self.connection_timeout}",
        ]
        if self.auth_type == "windows":
            parts.append("Trusted_Connection=yes")
        elif self.auth_type == "azure_ad":
            parts.append("Authentication=ActiveDirectoryInteractive")
            if self.username:
                parts.append(f"UID={self.username}")
        else:  # sql_login
            parts.append(f"UID={self.username}")
            parts.append(f"PWD={self.password}")

        if self.trust_cert:
            parts.append("TrustServerCertificate=yes")

        return ";".join(parts)

    def sqlalchemy_url(self) -> str:
        """Build a SQLAlchemy connection URL using pyodbc."""
        from urllib.parse import quote_plus
        conn_str = quote_plus(self.connection_string())
        return f"mssql+pyodbc:///?odbc_connect={conn_str}"


@dataclass
class IngestionConfig:
    """Configuration for data loading/ingestion behavior (shared by extraction and ingestion)."""
    load_mode: str = "full"  # full | incremental
    batch_size: int = 10_000  # Reduced from 50K to prevent memory exhaustion on wide tables
    row_limit: int = 0  # 0 = no limit; >0 = max rows per table
    write_backend: str = "pyodbc"  # pyodbc | arrow_odbc | bcp
    bcp_path: str = "bcp"
    bcp_batch_size: int = 10000
    bcp_temp_dir: str = ""
    bcp_field_terminator: str = "|~|"
    bcp_row_terminator: str = "~|~\n"
    state_file: str = ".ingestion_state.json"
    log_level: str = "INFO"
    naming_convention: str = "PascalCase"  # PascalCase | camelCase | UPPER_SNAKE | lower_snake | as_is
    mapping_file: str = "mapping_config.json"  # path to mapping config file


@dataclass
class S3Config:
    """S3 configuration for artifact storage."""
    region: str = "us-east-1"
    endpoint_url: str = ""  # Override for non-AWS S3 (e.g., MinIO)
    upload_chunk_size: int = 100 * 1024 * 1024  # 100 MB
    download_chunk_size: int = 100 * 1024 * 1024  # 100 MB


@dataclass
class SIDConfig:
    """Configuration for SID transformation pipeline."""
    api_url: str = ""  # SID API base URL (e.g., https://sid-api.verisk.com)
    tenant_context_lambda_name: str = "rs-cdkdev-ap-tenant-context-lambda"
    app_token_secret_name: str = "rs-cdkdev-app-token-secret"
    identity_provider_token_endpoint: str = ""  # Full Okta token endpoint URL, e.g. https://sso-dev.../v1/token
    max_retries: int = 3
    retry_backoff_base: float = 1.0
    use_mock: bool = False  # Use mock SID client for testing
    mock_starting_sid: int = 1_000_000  # Starting SID for mock client


@dataclass
class IngestionActivityConfig:
    """Configuration specifically for the ingestion activity (Tenant S3 → Iceberg).
    
    This is distinct from IngestionConfig which controls data loading behavior.
    IngestionActivityConfig controls the ingestion activity flow parameters.
    """
    sql_config: Optional[SqlServerConfig] = None
    iceberg_config: Optional[IcebergConfig] = None
    s3_config: Optional[S3Config] = None
    sid_config: Optional[SIDConfig] = None
    activity_type: str = "ingestion"
    batch_size: int = 10000
    write_mode: str = "append"  # append | overwrite
    validate_checksum: bool = True
    data_dir: str = "/data"  # Base directory for temp files
    cleanup_on_success: bool = True
    cleanup_on_failure: bool = True
    
    def __post_init__(self):
        if self.sql_config is None:
            self.sql_config = SqlServerConfig()
        if self.iceberg_config is None:
            self.iceberg_config = IcebergConfig()
        if self.s3_config is None:
            self.s3_config = S3Config()
        if self.sid_config is None:
            self.sid_config = SIDConfig()


# =============================================================================
# EXPOSURE DATA JOIN CHAINS - FK Hierarchy Reference
# =============================================================================
# This configuration documents the FK relationships for exposure data extraction.
# Use this as a reference for building join queries against Iceberg/SQL tables.
#
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │                     EXPOSURE DATA JOIN CHAINS                               │
# │                                                                             │
# │  tExposureSet (root - filter by ExposureSetSID)                             │
# │  │                                                                          │
# │  ├── tContract (direct filter: ExposureSetSID)                              │
# │  │   ├── tLayer (join via ContractSID)                                      │
# │  │   │   └── tLayerCondition (join via LayerSID)                            │
# │  │   │       └── tLayerConditionLocationXref                                │
# │  │   │                                                                      │
# │  │   ├── tStepFunction (join via ContractSID)                               │
# │  │   │   └── tStepFunctionDetail                                            │
# │  │   │                                                                      │
# │  │   └── tProgramBusinessCategoryFactor                                     │
# │  │                                                                          │
# │  ├── tLocation (direct filter: ExposureSetSID)                              │
# │  │   ├── tLocTerm                                                           │
# │  │   ├── tLocFeature                                                        │
# │  │   ├── tLocOffshore                                                       │
# │  │   ├── tLocParsedAddress                                                  │
# │  │   ├── tLocWC                                                             │
# │  │   ├── tEngineLocPhysicalProperty                                         │
# │  │   └── tLocStepFunctionXref                                               │
# │  │                                                                          │
# │  ├── tAggregateExposure (direct filter)                                     │
# │  ├── tReinsAppliestoExp (direct filter)                                     │
# │  ├── tReinsuranceExposureAdjustmentFactor (direct filter)                   │
# │  └── tReinsuranceProgramTargetXref (direct filter)                          │
# │                                                                             │
# │  Reference Tables (no exposure filter - count all):                         │
# │  tCompany, tPortfolio, tPortfolioFilter, tReinsuranceProgram,               │
# │  tReinsuranceTreaty, tReinsuranceEPCurve, tReinsuranceEPCurveSet,           │
# │  tAppliesToArea, tSIDControl                                                │
# └─────────────────────────────────────────────────────────────────────────────┘
#
# Legend:
#   - "filter": Column that can be directly filtered by exposure_ids (ExposureSetSID)
#   - "join": Tables require joining through parent tables to reach exposure filter
#   - "ref": Reference/lookup tables (count all rows, no exposure filter)
#
# Join Path Examples:
#   - tLayer: JOIN tContract ON tLayer.ContractSID = tContract.ContractSID
#             WHERE tContract.ExposureSetSID IN (...)
#   - tLayerCondition: JOIN tLayer → JOIN tContract → WHERE ExposureSetSID IN (...)
# =============================================================================

EXPOSURE_JOIN_CHAINS: dict[str, dict] = {
    # =========================================================================
    # EXPOSURE SET (Root - Direct Filter)
    # =========================================================================
    "tExposureSet": {
        "type": "root",
        "filter_column": "ExposureSetSID",
        "children": ["tContract", "tLocation", "tAggregateExposure", 
                     "tReinsAppliestoExp", "tReinsuranceExposureAdjustmentFactor",
                     "tReinsuranceProgramTargetXref"],
    },
    
    # =========================================================================
    # CONTRACT CHAIN
    # =========================================================================
    "tContract": {
        "type": "direct_filter",
        "filter_column": "ExposureSetSID",
        "pk": "ContractSID",
        "children": ["tLayer", "tStepFunction", "tProgramBusinessCategoryFactor"],
    },
    "tLayer": {
        "type": "join",
        "pk": "LayerSID",
        "join_path": [
            {"table": "tContract", "fk": "ContractSID", "pk": "ContractSID"}
        ],
        "children": ["tLayerCondition"],
    },
    "tLayerCondition": {
        "type": "join",
        "pk": "LayerConditionSID",
        "join_path": [
            {"table": "tLayer", "fk": "LayerSID", "pk": "LayerSID"},
            {"table": "tContract", "fk": "ContractSID", "pk": "ContractSID"}
        ],
        "children": ["tLayerConditionLocationXref"],
    },
    "tLayerConditionLocationXref": {
        "type": "join",
        "pk": "LayerConditionSID",
        "join_path": [
            {"table": "tLayerCondition", "fk": "LayerConditionSID", "pk": "LayerConditionSID"},
            {"table": "tLayer", "fk": "LayerSID", "pk": "LayerSID"},
            {"table": "tContract", "fk": "ContractSID", "pk": "ContractSID"}
        ],
    },
    
    # =========================================================================
    # STEP FUNCTION CHAIN (Contract → StepFunction → StepFunctionDetail)
    # =========================================================================
    "tStepFunction": {
        "type": "join",
        "pk": "StepFunctionSID",
        "join_path": [
            {"table": "tContract", "fk": "ContractSID", "pk": "ContractSID"}
        ],
        "children": ["tStepFunctionDetail"],
    },
    "tStepFunctionDetail": {
        "type": "join",
        "pk": "StepFunctionSID",
        "join_path": [
            {"table": "tStepFunction", "fk": "StepFunctionSID", "pk": "StepFunctionSID"},
            {"table": "tContract", "fk": "ContractSID", "pk": "ContractSID"}
        ],
    },
    
    # =========================================================================
    # LOCATION CHAIN (Direct filter via ExposureSetSID)
    # =========================================================================
    "tLocation": {
        "type": "direct_filter",
        "filter_column": "ExposureSetSID",
        "pk": "LocationSID",
        "children": ["tLocTerm", "tLocFeature", "tLocOffshore", "tLocParsedAddress",
                     "tLocWC", "tEngineLocPhysicalProperty", "tLocStepFunctionXref"],
    },
    "tLocTerm": {
        "type": "join",
        "pk": "LocTermSID",
        "join_path": [
            {"table": "tLocation", "fk": "LocationSID", "pk": "LocationSID"}
        ],
    },
    "tLocFeature": {
        "type": "join",
        "pk": "LocationSID",  # Composite key
        "join_path": [
            {"table": "tLocation", "fk": "LocationSID", "pk": "LocationSID"}
        ],
    },
    "tLocOffshore": {
        "type": "join",
        "pk": "LocationSID",
        "join_path": [
            {"table": "tLocation", "fk": "LocationSID", "pk": "LocationSID"}
        ],
    },
    "tLocParsedAddress": {
        "type": "join",
        "pk": "LocationSID",
        "join_path": [
            {"table": "tLocation", "fk": "LocationSID", "pk": "LocationSID"}
        ],
    },
    "tLocWC": {
        "type": "join",
        "pk": "LocationSID",
        "join_path": [
            {"table": "tLocation", "fk": "LocationSID", "pk": "LocationSID"}
        ],
    },
    "tEngineLocPhysicalProperty": {
        "type": "join",
        "pk": "LocationSID",
        "join_path": [
            {"table": "tLocation", "fk": "LocationSID", "pk": "LocationSID"}
        ],
    },
    "tLocStepFunctionXref": {
        "type": "join",
        "pk": "LocationSID",
        "join_path": [
            {"table": "tLocation", "fk": "LocationSID", "pk": "LocationSID"}
        ],
    },
    
    # =========================================================================
    # AGGREGATE EXPOSURE (Direct filter)
    # =========================================================================
    "tAggregateExposure": {
        "type": "direct_filter",
        "filter_column": "ExposureSetSID",
        "pk": "AggregateExposureSID",
    },
    
    # =========================================================================
    # REINSURANCE TABLES (Various join strategies)
    # =========================================================================
    "tReinsAppliestoExp": {
        "type": "direct_filter",
        "filter_column": "ExposureSetSID",
        "pk": "ReinsAppliesToExpSID",
    },
    "tReinsuranceExposureAdjustmentFactor": {
        "type": "direct_filter",
        "filter_column": "ExposureSetSID",
        "pk": "ReinsuranceExposureAdjustmentFactorSID",
    },
    "tReinsuranceProgramTargetXref": {
        "type": "direct_filter",
        "filter_column": "ExposureSetSID",
        "pk": "ReinsuranceProgramSID",
    },
    "tProgramBusinessCategoryFactor": {
        "type": "join",
        "pk": "ContractSID",
        "join_path": [
            {"table": "tContract", "fk": "ContractSID", "pk": "ContractSID"}
        ],
    },
    
    # =========================================================================
    # REFERENCE TABLES (No exposure filter - count all)
    # =========================================================================
    "tCompany": {"type": "reference", "pk": "CompanySID"},
    "tPortfolio": {"type": "reference", "pk": "PortfolioSID"},
    "tPortfolioFilter": {"type": "reference", "pk": "PortfolioFilterSID"},
    "tReinsuranceProgram": {"type": "reference", "pk": "ReinsuranceProgramSID"},
    "tReinsuranceTreaty": {"type": "reference", "pk": "ReinsuranceTreatySID"},
    "tReinsuranceEPCurve": {"type": "reference", "pk": "ReinsuranceEPCurveSID"},
    "tReinsuranceEPCurveSet": {"type": "reference", "pk": "ReinsuranceEPCurveSetSID"},
    "tAppliesToArea": {"type": "reference", "pk": "AppliesToAreaSID"},
    "tSIDControl": {"type": "reference", "pk": "TableSID"},
}


def get_join_sql(table_name: str, exposure_ids: list[int]) -> str:
    """
    Generate a SQL WHERE clause or JOIN statement for filtering by exposure_ids.
    
    Args:
        table_name: Name of the table (e.g., "tLayer", "tContract")
        exposure_ids: List of ExposureSetSID values to filter by
        
    Returns:
        SQL fragment for filtering/joining
    """
    config = EXPOSURE_JOIN_CHAINS.get(table_name)
    if not config:
        return ""
    
    exposure_ids_str = ", ".join(str(id) for id in exposure_ids)
    table_type = config.get("type", "")
    
    if table_type == "root" or table_type == "direct_filter":
        filter_col = config.get("filter_column", "ExposureSetSID")
        return f"WHERE {filter_col} IN ({exposure_ids_str})"
    
    elif table_type == "join":
        join_path = config.get("join_path", [])
        if not join_path:
            return ""
        
        # Build JOIN chain
        joins = []
        prev_alias = "t"
        for i, step in enumerate(join_path):
            alias = f"j{i}"
            joins.append(
                f"JOIN {step['table']} {alias} ON {prev_alias}.{step['fk']} = {alias}.{step['pk']}"
            )
            prev_alias = alias
        
        # Final filter on the last joined table
        last_table = join_path[-1]["table"]
        last_config = EXPOSURE_JOIN_CHAINS.get(last_table, {})
        filter_col = last_config.get("filter_column", "ExposureSetSID")
        
        join_clause = " ".join(joins)
        return f"{join_clause} WHERE {prev_alias}.{filter_col} IN ({exposure_ids_str})"
    
    elif table_type == "reference":
        # Reference tables - no filter, return empty (count all)
        return ""
    
    return ""


@dataclass
class AppConfig:
    iceberg: IcebergConfig = field(default_factory=IcebergConfig)
    sqlserver: SqlServerConfig = field(default_factory=SqlServerConfig)
    ingestion: IngestionConfig = field(default_factory=IngestionConfig)
    sid: SIDConfig = field(default_factory=SIDConfig)


def load_config() -> AppConfig:
    """Load configuration from .env and return an AppConfig instance."""
    _load_env()

    iceberg = IcebergConfig(
        catalog_type=os.getenv("ICEBERG_CATALOG_TYPE", "glue"),
        catalog_name=os.getenv("ICEBERG_CATALOG_NAME", "glue_catalog"),
        catalog_uri=os.getenv("ICEBERG_CATALOG_URI", ""),
        s3_endpoint=os.getenv("ICEBERG_S3_ENDPOINT", ""),
        s3_access_key=os.getenv("ICEBERG_S3_ACCESS_KEY", ""),
        s3_secret_key=os.getenv("ICEBERG_S3_SECRET_KEY", ""),
        s3_region=os.getenv("ICEBERG_S3_REGION", "us-east-1"),
        warehouse=os.getenv("ICEBERG_WAREHOUSE", ""),
        namespace=os.getenv("ICEBERG_NAMESPACE", os.getenv("GLUE_DATABASE", "default")),
        table=os.getenv("ICEBERG_TABLE", ""),
        glue_region=os.getenv("ICEBERG_GLUE_REGION", os.getenv("ICEBERG_S3_REGION", "us-east-1")),
        glue_profile=os.getenv("ICEBERG_GLUE_PROFILE", ""),
        glue_catalog_id=os.getenv("ICEBERG_GLUE_CATALOG_ID", ""),
    )

    sqlserver = SqlServerConfig(
        host=os.getenv("SQLSERVER_HOST", "localhost"),
        port=int(os.getenv("SQLSERVER_PORT", "1433")),
        database=os.getenv("SQLSERVER_DATABASE", ""),
        schema=os.getenv("SQLSERVER_SCHEMA", "dbo"),
        table=os.getenv("SQLSERVER_TABLE", ""),
        username=os.getenv("SQLSERVER_USERNAME", ""),
        password=os.getenv("SQLSERVER_PASSWORD", ""),
        auth_type=os.getenv("SQLSERVER_AUTH_TYPE", "sql_login"),
        driver=os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server"),
        trust_cert=os.getenv("SQLSERVER_TRUST_CERT", "yes").lower() in ("yes", "true", "1"),
        data_share_path=os.getenv("SQLSERVER_DATA_SHARE_PATH", ""),
    )

    ingestion = IngestionConfig(
        load_mode=os.getenv("LOAD_MODE", "full"),
        batch_size=int(os.getenv("BATCH_SIZE", "10000")),
        row_limit=int(os.getenv("ROW_LIMIT", "0")),
        write_backend=os.getenv("WRITE_BACKEND", "pyodbc"),
        bcp_path=os.getenv("BCP_PATH", "bcp"),
        bcp_batch_size=int(os.getenv("BCP_BATCH_SIZE", "10000")),
        bcp_temp_dir=os.getenv("BCP_TEMP_DIR", ""),
        bcp_field_terminator=os.getenv("BCP_FIELD_TERMINATOR", "|~|"),
        bcp_row_terminator=os.getenv("BCP_ROW_TERMINATOR", "~|~\n"),
        state_file=os.getenv("STATE_FILE", ".ingestion_state.json"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        naming_convention=os.getenv("NAMING_CONVENTION", "PascalCase"),
        mapping_file=os.getenv("MAPPING_FILE", "mapping_config.json"),
    )

    sid = SIDConfig(
        api_url=os.getenv("SID_API_URL", ""),
        tenant_context_lambda_name=os.getenv("TENANT_CONTEXT_LAMBDA", "rs-cdkdev-ap-tenant-context-lambda"),
        app_token_secret_name=os.getenv("APP_TOKEN_SECRET", "rs-cdkdev-app-token-secret"),
        identity_provider_token_endpoint=os.getenv("IDENTITY_PROVIDER_TOKEN_ENDPOINT", ""),
        max_retries=int(os.getenv("SID_MAX_RETRIES", "3")),
        retry_backoff_base=float(os.getenv("SID_RETRY_BACKOFF", "1.0")),
        use_mock=os.getenv("SID_USE_MOCK", "false").lower() in ("yes", "true", "1"),
        mock_starting_sid=int(os.getenv("SID_MOCK_STARTING_SID", "1000000")),
    )

    return AppConfig(iceberg=iceberg, sqlserver=sqlserver, ingestion=ingestion, sid=sid)


def load_ingestion_activity_config() -> IngestionActivityConfig:
    """Load configuration for the ingestion activity (Tenant S3 → Iceberg).
    
    This loads configuration specific to the ingestion activity flow,
    including SQL Server, Iceberg, and S3 settings.
    """
    _load_env()
    
    sql_config = SqlServerConfig(
        host=os.getenv("SQLSERVER_HOST", "localhost"),
        port=int(os.getenv("SQLSERVER_PORT", "1433")),
        database=os.getenv("SQLSERVER_DATABASE", ""),
        schema=os.getenv("SQLSERVER_SCHEMA", "dbo"),
        username=os.getenv("SQLSERVER_USERNAME", ""),
        password=os.getenv("SQLSERVER_PASSWORD", ""),
        auth_type=os.getenv("SQLSERVER_AUTH_TYPE", "sql_login"),
        driver=os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server"),
        trust_cert=os.getenv("SQLSERVER_TRUST_CERT", "yes").lower() in ("yes", "true", "1"),
        connection_timeout=int(os.getenv("SQLSERVER_TIMEOUT", "600")),
    )
    
    iceberg_config = IcebergConfig(
        catalog_type=os.getenv("ICEBERG_CATALOG_TYPE", "glue"),
        catalog_name=os.getenv("ICEBERG_CATALOG_NAME", "glue_catalog"),
        s3_region=os.getenv("ICEBERG_S3_REGION", "us-east-1"),
        warehouse=os.getenv("ICEBERG_WAREHOUSE", ""),
        namespace=os.getenv("ICEBERG_NAMESPACE", os.getenv("GLUE_DATABASE", "default")),
        glue_region=os.getenv("ICEBERG_GLUE_REGION", os.getenv("AWS_REGION", "us-east-1")),
    )
    
    s3_config = S3Config(
        region=os.getenv("AWS_REGION", "us-east-1"),
    )
    
    sid_config = SIDConfig(
        api_url=os.getenv("SID_API_URL", ""),
        tenant_context_lambda_name=os.getenv("TENANT_CONTEXT_LAMBDA", "rs-cdkdev-ap-tenant-context-lambda"),
        app_token_secret_name=os.getenv("APP_TOKEN_SECRET", "rs-cdkdev-app-token-secret"),
        identity_provider_token_endpoint=os.getenv("IDENTITY_PROVIDER_TOKEN_ENDPOINT", ""),
        max_retries=int(os.getenv("SID_MAX_RETRIES", "3")),
        retry_backoff_base=float(os.getenv("SID_RETRY_BACKOFF", "1.0")),
        use_mock=os.getenv("SID_USE_MOCK", "false").lower() in ("yes", "true", "1"),
        mock_starting_sid=int(os.getenv("SID_MOCK_STARTING_SID", "1000000")),
    )
    
    return IngestionActivityConfig(
        sql_config=sql_config,
        iceberg_config=iceberg_config,
        s3_config=s3_config,
        sid_config=sid_config,
        batch_size=int(os.getenv("BATCH_SIZE", "10000")),
        write_mode=os.getenv("INGESTION_WRITE_MODE", "append"),
        validate_checksum=os.getenv("VALIDATE_CHECKSUM", "true").lower() in ("yes", "true", "1"),
        data_dir=os.getenv("DATA_DIR", "/data"),
    )
