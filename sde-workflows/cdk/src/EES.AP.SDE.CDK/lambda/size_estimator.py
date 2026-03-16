"""
Size Estimator Lambda for Synergy Data Exchange Pipeline

This Lambda:
1. Invokes Tenant Context Lambda to get tenant-specific configuration
2. Estimates output size by querying Iceberg tables via Athena
3. Routes to EXPRESS/STANDARD/BATCH based on size
4. Returns sde_context for downstream Fargate/Batch tasks

Multi-Tenant Flow:
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Input: {tenant_id, exposure_ids, run_id, activity_id, ...}                    │
│                          │                                                      │
│                          ▼                                                      │
│  1. Invoke Tenant Context Lambda (using app_token from Secrets Manager)        │
│     - Returns: bucketName, tenantRole, icebergDbConfiguration.exposure, etc.   │
│                          │                                                      │
│                          ▼                                                      │
│  2. Extract Synergy Data Exchange config (glue_database, bucket_name, tenant_role, ...)   │
│                          │                                                      │
│                          ▼                                                      │
│  3. Query Athena: COUNT records across all tables for exposure_ids             │
│     - Uses glue_database from tenant context                                    │
│                          │                                                      │
│                          ▼                                                      │
│  4. Calculate size → select_path (EXPRESS/STANDARD/BATCH)                      │
│                          │                                                      │
│                          ▼                                                      │
│  Output: {...input, sde_context: {...}, selected_path, ...}             │
│                                                                                 │
│  Downstream tasks receive sde_context as container env vars             │
└─────────────────────────────────────────────────────────────────────────────────┘

Environment Variables:
- TENANT_CONTEXT_LAMBDA_NAME: Lambda to invoke for tenant context
- APP_TOKEN_SECRET_NAME: Secrets Manager secret with app token
- AVG_BYTES_PER_RECORD: Average bytes per record (default: 500)
- THRESHOLD_SMALL_GB: Small threshold in GB (default: 9)
- THRESHOLD_LARGE_GB: Large threshold in GB (default: 80)
"""

import json
import logging
import os
import time
from typing import Any

import boto3

class _JsonFormatter(logging.Formatter):
    """Compact single-line JSON log formatter for CloudWatch structured logging."""
    _SKIP = frozenset({
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message", "asctime", "tenant_id", "activity_id",
    })

    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "tenant_id": getattr(record, "tenant_id", ""),
            "activity_id": getattr(record, "activity_id", ""),
            "message": record.getMessage(),
        }
        for key, val in record.__dict__.items():
            if key not in self._SKIP and not key.startswith("_"):
                entry[key] = val
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str, separators=(",", ":"))


class _JsonContextFilter(logging.Filter):
    """Injects tenant_id and activity_id into every log record."""
    tenant_id: str = ""
    activity_id: str = ""

    def filter(self, record: logging.LogRecord) -> bool:
        record.tenant_id = self.tenant_id
        record.activity_id = self.activity_id
        return True


_log_context = _JsonContextFilter()

# Set up logging — replace the default Lambda handler with a single-line JSON handler
logger = logging.getLogger()
logger.setLevel(logging.INFO)
for _h in logger.handlers[:]:
    logger.removeHandler(_h)
_json_handler = logging.StreamHandler()
_json_handler.setFormatter(_JsonFormatter())
_json_handler.addFilter(_log_context)
logger.addHandler(_json_handler)


# =============================================================================
# DYNAMIC TABLE DISCOVERY FROM ICEBERG/GLUE
# =============================================================================

# Known PK naming conventions: table 't{name}' has PK '{name}sid'
# e.g., tcontract -> contractsid, tlocation -> locationsid
PK_NAMING_PATTERN = lambda table_name: table_name[1:] + "sid" if table_name.startswith("t") else table_name + "sid"

# Tables that have direct exposuresetsid column (can filter directly)
KNOWN_DIRECT_FILTER_TABLES = {"texposureset", "tcontract", "tlocation", "taggregateexposure", 
                              "treinsappliesttoexp", "treinsuranceexposureadjustmentfactor"}


def discover_glue_tables(database: str, tenant_credentials: dict | None = None, region: str = "us-east-1") -> list[str]:
    """
    Discover all tables in a Glue database (Iceberg catalog).
    
    Args:
        database: Glue database name
        tenant_credentials: AWS credentials for tenant access
        region: AWS region
    
    Returns:
        List of table names in the database
    """
    if tenant_credentials:
        glue = boto3.client(
            'glue',
            aws_access_key_id=tenant_credentials.get('access_key'),
            aws_secret_access_key=tenant_credentials.get('secret_key'),
            aws_session_token=tenant_credentials.get('session_token'),
            region_name=region
        )
    else:
        glue = boto3.client('glue', region_name=region)
    
    tables = []
    paginator = glue.get_paginator('get_tables')
    
    try:
        for page in paginator.paginate(DatabaseName=database):
            for table in page.get('TableList', []):
                tables.append(table['Name'].lower())
        logger.info(f"Discovered {len(tables)} tables in Glue database '{database}'")
    except Exception as e:
        logger.error(f"Failed to discover tables from Glue: {e}")
        return []
    
    return tables


def get_table_columns(database: str, table_name: str, tenant_credentials: dict | None = None, region: str = "us-east-1") -> list[str]:
    """
    Get column names for a table from Glue catalog.
    
    Args:
        database: Glue database name
        table_name: Table name
        tenant_credentials: AWS credentials for tenant access
        region: AWS region
    
    Returns:
        List of column names (lowercase)
    """
    if tenant_credentials:
        glue = boto3.client(
            'glue',
            aws_access_key_id=tenant_credentials.get('access_key'),
            aws_secret_access_key=tenant_credentials.get('secret_key'),
            aws_session_token=tenant_credentials.get('session_token'),
            region_name=region
        )
    else:
        glue = boto3.client('glue', region_name=region)
    
    try:
        response = glue.get_table(DatabaseName=database, Name=table_name)
        columns = [col['Name'].lower() for col in response['Table']['StorageDescriptor']['Columns']]
        return columns
    except Exception as e:
        logger.warning(f"Failed to get columns for {table_name}: {e}")
        return []


def get_table_glue_stats(database: str, table_name: str, tenant_credentials: dict | None = None, region: str = "us-east-1") -> dict:
    """
    Fetch table statistics from Glue metadata (Iceberg stores these automatically).
    
    Iceberg/Glue tracks:
      - totalSize: total compressed Parquet bytes on S3
      - numRows: total row count across all files
      - numFiles: number of Parquet files
    
    Args:
        database: Glue database name
        table_name: Table name
        tenant_credentials: AWS credentials for tenant access
        region: AWS region
    
    Returns:
        Dict with 'total_size_bytes' and 'total_rows' (0 if unknown)
    """
    if tenant_credentials:
        glue = boto3.client(
            'glue',
            aws_access_key_id=tenant_credentials.get('access_key'),
            aws_secret_access_key=tenant_credentials.get('secret_key'),
            aws_session_token=tenant_credentials.get('session_token'),
            region_name=region
        )
    else:
        glue = boto3.client('glue', region_name=region)
    
    try:
        response = glue.get_table(DatabaseName=database, Name=table_name)
        params = response['Table'].get('Parameters', {})
        columns = response['Table']['StorageDescriptor']['Columns']
        
        # Iceberg / Glue table statistics
        total_size = int(params.get('totalSize', 0) or params.get('total_size', 0))
        total_rows = int(params.get('numRows', 0) or params.get('num_rows', 0))
        num_files = int(params.get('numFiles', 0))
        
        # Iceberg also stores stats in 'metadata_location' or manifest summaries
        # but 'numRows' in Parameters is most accessible
        return {
            'total_size_bytes': total_size,
            'total_rows': total_rows,
            'num_files': num_files,
            'num_columns': len(columns),
        }
    except Exception as e:
        logger.warning(f"Failed to get Glue stats for {table_name}: {e}")
        return {'total_size_bytes': 0, 'total_rows': 0, 'num_files': 0, 'num_columns': 0}


def fetch_iceberg_table_stats(
    database: str,
    table_name: str,
    output_bucket: str,
    tenant_credentials: dict | None = None,
    workgroup: str | None = None,
    region: str = "us-east-1",
    activity_id: str | None = None,
) -> dict:
    """
    Fetch accurate row count and storage size directly from Iceberg metadata.

    Queries the hidden `$files` metadata table which Iceberg always maintains
    in its manifest files — no ANALYZE TABLE required.

    Query:
        SELECT SUM(record_count) AS total_rows,
               SUM(file_size_in_bytes) AS total_size_bytes
        FROM "database"."table$files"

    Returns dict with 'total_rows' and 'total_size_bytes' (0 if unavailable).
    """
    import time

    query = (
        f'SELECT SUM(record_count) AS total_rows, '
        f'SUM(file_size_in_bytes) AS total_size_bytes '
        f'FROM "{database}"."{table_name}$files"'
    )

    # Build Athena client with tenant credentials
    if tenant_credentials:
        athena = boto3.client(
            'athena',
            aws_access_key_id=tenant_credentials.get('access_key', ''),
            aws_secret_access_key=tenant_credentials.get('secret_key', ''),
            aws_session_token=tenant_credentials.get('session_token', ''),
            region_name=region
        )
    else:
        athena = boto3.client('athena', region_name=region)

    if not output_bucket:
        raise ValueError("No tenant bucket available for Athena output. Tenant context must include 'bucket_name'.")
    bucket = output_bucket
    athena_prefix = f"sde/Extraction/{activity_id}/" if activity_id else "sde/Extraction/"
    query_params: dict = {
        'QueryString': query,
        'QueryExecutionContext': {'Database': database},
        'ResultConfiguration': {'OutputLocation': f's3://{bucket}/{athena_prefix}'},
    }
    if workgroup:
        query_params['WorkGroup'] = workgroup

    try:
        response = athena.start_query_execution(**query_params)
        qid = response['QueryExecutionId']

        # Poll until done (max 30 s)
        state = 'RUNNING'
        status_resp: dict = {}
        for _ in range(60):
            time.sleep(0.5)
            status_resp = athena.get_query_execution(QueryExecutionId=qid)
            state = status_resp['QueryExecution']['Status']['State']
            if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
                break

        if state != 'SUCCEEDED':
            reason = status_resp['QueryExecution']['Status'].get('StateChangeReason', '')
            logger.warning(f"Iceberg $files query {state} for {table_name}: {reason}")
            return {'total_rows': 0, 'total_size_bytes': 0}

        results = athena.get_query_results(QueryExecutionId=qid)
        rows = results.get('ResultSet', {}).get('Rows', [])
        if len(rows) >= 2:
            data_row = rows[1].get('Data', [])
            total_rows = int(data_row[0].get('VarCharValue', '0') or '0')
            total_size = int(data_row[1].get('VarCharValue', '0') or '0')
            return {'total_rows': total_rows, 'total_size_bytes': total_size}

    except Exception as e:
        logger.warning(f"Failed to fetch Iceberg $files stats for {table_name}: {e}")

    return {'total_rows': 0, 'total_size_bytes': 0}


def build_table_join_chain(
    database: str,
    available_tables: list[str],
    requested_tables: list[str] | None,
    tenant_credentials: dict | None = None,
    region: str = "us-east-1"
) -> dict[str, dict]:
    """
    Build an in-memory PK/FK join chain by analyzing table schemas.
    
    Discovers relationships based on column naming conventions:
    - Primary key: table 't{name}' has PK column '{name}sid'
    - Foreign key: column '{name}sid' references table 't{name}'
    
    Args:
        database: Glue database name
        available_tables: All tables discovered in Glue
        requested_tables: Specific tables to include (None = all)
        tenant_credentials: AWS credentials
        region: AWS region
    
    Returns:
        Dict mapping table_name -> {
            "pk": primary key column,
            "columns": list of all columns,
            "filter_column": "exposuresetsid" if table has it (direct filter),
            "parent_join": {fk_column: parent_table} for FK relationships
        }
    """
    # Determine which tables to process
    if requested_tables is None or requested_tables == "all":
        tables_to_process = available_tables
    else:
        tables_to_process = [t.lower() for t in requested_tables if t.lower() in available_tables]
        not_found = [t for t in requested_tables if t.lower() not in available_tables]
        if not_found:
            logger.warning(f"Requested tables not found in Glue: {not_found}")
    
    logger.info(f"Building join chain for {len(tables_to_process)} tables")
    
    # Cache for table columns and stats (single Glue call per table)
    table_meta_cache: dict[str, dict] = {}
    
    for table_name in tables_to_process:
        if tenant_credentials:
            glue = boto3.client(
                'glue',
                aws_access_key_id=tenant_credentials.get('access_key'),
                aws_secret_access_key=tenant_credentials.get('secret_key'),
                aws_session_token=tenant_credentials.get('session_token'),
                region_name=region
            )
        else:
            glue = boto3.client('glue', region_name=region)
        
        try:
            response = glue.get_table(DatabaseName=database, Name=table_name)
            table_def = response['Table']
            params = table_def.get('Parameters', {})
            columns = [col['Name'].lower() for col in table_def['StorageDescriptor']['Columns']]
            table_meta_cache[table_name] = {
                'columns': columns,
                'total_size_bytes': int(params.get('totalSize', 0) or 0),
                'total_rows': int(params.get('numRows', 0) or 0),
                'num_files': int(params.get('numFiles', 0) or 0),
            }
        except Exception as e:
            logger.warning(f"Failed to get metadata for {table_name}: {e}")
            table_meta_cache[table_name] = {'columns': [], 'total_size_bytes': 0, 'total_rows': 0, 'num_files': 0}
    
    # Build the join chain
    join_chain: dict[str, dict] = {}
    
    for table_name in tables_to_process:
        meta = table_meta_cache.get(table_name, {})
        columns = meta.get('columns', [])
        
        if not columns:
            continue
        
        # Infer primary key from naming convention
        pk_candidate = PK_NAMING_PATTERN(table_name)
        pk_column = pk_candidate if pk_candidate in columns else None
        
        # Check for direct exposuresetsid filter
        filter_column = "exposuresetsid" if "exposuresetsid" in columns else None
        
        # Find FK relationships (columns ending in 'sid' that reference other tables)
        parent_joins = {}
        for col in columns:
            if col.endswith("sid") and col != pk_column and col != "exposuresetsid":
                parent_name = "t" + col[:-3]  # e.g., 'contractsid' -> 'tcontract'
                if parent_name in available_tables:
                    parent_joins[col] = parent_name
        
        join_chain[table_name] = {
            "pk": pk_column,
            "columns": columns,
            "filter_column": filter_column,
            "parent_join": parent_joins if parent_joins else None,
            # Glue/Iceberg statistics for accurate size estimation
            "total_size_bytes": meta.get('total_size_bytes', 0),
            "total_rows": meta.get('total_rows', 0),
        }
    
    # Log the discovered relationships
    direct_filter_count = sum(1 for t in join_chain.values() if t.get("filter_column"))
    join_count = sum(1 for t in join_chain.values() if t.get("parent_join"))
    logger.info(f"Join chain built: {direct_filter_count} tables with direct filter, {join_count} tables with FK joins")
    
    return join_chain


def _fetch_tenant_context(tenant_id: str) -> dict:
    """Fetch tenant context by invoking the tenant context Lambda."""
    lambda_name = os.environ.get(
        "TENANT_CONTEXT_LAMBDA_NAME", 
        "rs-cdkdev-ap-tenant-context-lambda"
    )
    secret_name = os.environ.get(
        "APP_TOKEN_SECRET_NAME",
        "rs-cdkdev-app-token-secret"
    )
    region = os.environ.get("AWS_REGION", "us-east-1")
    
    # Get app token from Secrets Manager
    secrets_client = boto3.client("secretsmanager", region_name=region)
    secret_response = secrets_client.get_secret_value(SecretId=secret_name)
    secret_value = secret_response.get("SecretString", "")
    try:
        secret_json = json.loads(secret_value)
        app_token = secret_json.get("token") or secret_json.get("appToken") or secret_json.get("app_token") or secret_value
    except json.JSONDecodeError:
        app_token = secret_value
    
    # Invoke tenant context lambda
    lambda_client = boto3.client("lambda", region_name=region)
    response = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        Payload=json.dumps({"tenantId": tenant_id, "appToken": app_token})
    )
    
    payload = json.loads(response["Payload"].read().decode("utf-8"))
    
    if payload.get("error") or payload.get("statusCode") != 200:
        raise RuntimeError(f"Tenant context error: {payload.get('message', 'Unknown')}")
    
    return payload


def _get_sde_config(tenant_context: dict) -> dict:
    """Extract sde-specific fields from tenant context response."""
    iceberg_config = tenant_context.get("icebergDbConfiguration", {})
    aws_keys = tenant_context.get("awsSessionKeys", {})
    tenant_profile = tenant_context.get("tenantProfile", {})
    
    tenant_role = tenant_context.get("tenantRole", "")
    bucket_name = tenant_context.get("bucketName", "")
    glue_database = iceberg_config.get("exposure", "")
    
    # Log tenant context details
    logger.info(f"Tenant Context - tenantRole: {tenant_role}")
    logger.info(f"Tenant Context - bucketName: {bucket_name}")
    logger.info(f"Tenant Context - glue_database (exposure): {glue_database}")
    
    # Log AWS credentials status
    if aws_keys.get("accessKey"):
        access_key = aws_keys.get("accessKey", "")
        masked_key = f"{access_key[:4]}...{access_key[-4:]}" if len(access_key) > 8 else "***"
        logger.info(f"Tenant Context - awsSessionKeys present: Yes, AccessKeyId: {masked_key}")
    else:
        logger.warning("Tenant Context - awsSessionKeys present: No (credentials not provided)")
    
    return {
        "tenant_id": tenant_profile.get("tenantId") or tenant_context.get("tenantId", ""),
        "resource_id": tenant_context.get("resourceId", ""),
        "region": tenant_context.get("region", "us-east-1"),
        "bucket_name": bucket_name,
        "tenant_role": tenant_role,  # Tasks assume this role
        "glue_database": glue_database,
        "aws_credentials": {
            "access_key": aws_keys.get("accessKey", ""),
            "secret_key": aws_keys.get("secretKey", ""),
            "session_token": aws_keys.get("sessionToken", ""),
        } if aws_keys.get("accessKey") else None,
        "generated_on": tenant_context.get("generatedOn", 0),
    }


# AWS clients (lazy initialization)
_s3_client = None
_athena_client = None

def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client('s3')
    return _s3_client

def get_athena_client():
    global _athena_client
    if _athena_client is None:
        _athena_client = boto3.client('athena')
    return _athena_client

# Configuration from environment variables
# Athena results are written to the tenant bucket under sde/Extraction/{activity_id}/
# No separate Athena bucket is needed — tenant bucket is always used.
AVG_BYTES_PER_RECORD = int(os.environ.get('AVG_BYTES_PER_RECORD', 500))  # 500 bytes per record (last-resort fallback)
AVG_PARQUET_BYTES_PER_COLUMN = 2  # Conservative estimate: ~2 bytes/column in parquet (after compression)
                                   # Applied as: num_columns × 2 × expansion_factor → MDF bytes/row
                                   # Derived from observed data: tlocation=0.34, tcontract=0.28, tlocterm=0.27 bytes/col/row

# Thresholds in bytes
THRESHOLD_SMALL_GB = int(os.environ.get('THRESHOLD_SMALL_GB', 9))
THRESHOLD_LARGE_GB = int(os.environ.get('THRESHOLD_LARGE_GB', 80))
THRESHOLD_SMALL_BYTES = THRESHOLD_SMALL_GB * 1024 * 1024 * 1024
THRESHOLD_LARGE_BYTES = THRESHOLD_LARGE_GB * 1024 * 1024 * 1024

# Tenant Context Lambda configuration
TENANT_CONTEXT_LAMBDA_NAME = os.environ.get(
    'TENANT_CONTEXT_LAMBDA_NAME',
    'rs-cdkdev-ap-tenant-context-lambda'
)
APP_TOKEN_SECRET_NAME = os.environ.get(
    'APP_TOKEN_SECRET_NAME',
    'rs-cdkdev-app-token-secret'
)

# Parquet (Iceberg on S3) is typically 10-20x smaller than uncompressed SQL Server MDF.
# Wide tables with many NULLable INT/FLOAT columns compress extremely well in parquet
# (e.g. tlocfeature: 2.3 bytes/row parquet vs ~400 bytes/row SQL Server = 174x).
# Calibrated from act-003 run: estimated 4.65 GB vs actual 6.4 GB MDF => exact ratio=27.5x.
# Using 28x for a slight safety overestimate (~2% over actual).
PARQUET_TO_MDF_EXPANSION_FACTOR = int(os.environ.get('PARQUET_TO_MDF_EXPANSION_FACTOR', 28))

# Table hierarchy for extraction - defines FK relationships
# Derived from SID_CONFIG in src/sid/config.py
# Format: {table_name: {"pk": pk_column, "filter_column": direct_filter_col, "parent_join": {fk: parent}}}

# Tables that can be directly filtered by exposuresetsid
DIRECT_FILTER_TABLES = ["texposureset", "tcontract", "tlocation", "taggregateexposure", 
                        "treinsappliesttoexp", "treinsuranceexposureadjustmentfactor"]

# Full table hierarchy from SID_CONFIG
# Processing levels based on FK dependencies
TABLE_HIERARCHY = {
    # ========== Level 0: Root tables — no FK dependencies ==========
    "tcompany": {
        "pk": "companysid",
        "filter_column": None,  # No direct exposure filter
        "strategy": "count_all",  # Just count all (typically small)
    },
    "tportfoliofilter": {
        "pk": "portfoliofiltersid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "tappliestoarea": {
        "pk": "appliestoareasid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "treinsuranceepcurveset": {
        "pk": "reinsuranceepcurvesetsid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "tsidcontrol": {
        "pk": "tablesid",
        "filter_column": None,
        "strategy": "count_all",
    },
    
    # ========== Level 1: Depend on Level 0 ==========
    "texposureset": {
        "pk": "exposuresetsid",
        "filter_column": "exposuresetsid",  # Direct filter
    },
    "treinsuranceprogram": {
        "pk": "reinsuranceprogramsid",
        "filter_column": None,  # FK to tCompany, not ExposureSet
        "strategy": "count_all",
    },
    "tportfolio": {
        "pk": "portfoliosid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "treinsuranceepcurve": {
        "pk": "reinsuranceepcurvesid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "tcompanycatbondimporthistory": {
        "pk": "companycatbondimporthistorysid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "tcompanyudcclassification_xref": {
        "pk": "companysid",
        "filter_column": None,
        "strategy": "count_all",
    },
    
    # ========== Level 2: Depend on Level 1 ==========
    "tcontract": {
        "pk": "contractsid",
        "filter_column": "exposuresetsid",  # Direct filter by exposure_ids
    },
    "treinsurancetreaty": {
        "pk": "reinsurancetreatysid",
        "filter_column": None,  # FK to ReinsuranceProgram
        "strategy": "count_all",
    },
    "tcompanylossassociation": {
        "pk": "companylossassociationsid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "treinsuranceprogramcoverage": {
        "pk": "reinsuranceprogramsid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "treinsuranceprogramepadjustment": {
        "pk": "reinsuranceprogramsid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "treinsuranceprogramepadjustmentpoints": {
        "pk": "reinsuranceprogramsid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "treinsuranceprogramjapancflratio": {
        "pk": "reinsuranceprogramsid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "treinsuranceprogramsourceinure": {
        "pk": "reinsuranceprogramsubjectsid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "treinsuranceprogramtargetxref": {
        "pk": "reinsuranceprogramsid",
        "filter_column": "exposuresetsid",  # Has FK to ExposureSet
    },
    "tcatbondattributevalue": {
        "pk": "reinsuranceprogramsid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "treinsappliesttoexp": {
        "pk": "reinsappliesttoexpsid",
        "filter_column": "exposuresetsid",  # Direct FK to ExposureSet
    },
    "treinsuranceexposureadjustmentfactor": {
        "pk": "reinsuranceexposureadjustmentfactorsid",
        "filter_column": "exposuresetsid",  # Direct FK to ExposureSet
    },
    "taggregateexposure": {
        "pk": "aggregateexposuresid",
        "filter_column": "exposuresetsid",  # Direct FK to ExposureSet
    },
    "treinsuranceepcurveadjustmentpoint": {
        "pk": "reinsuranceepcurvesid",
        "filter_column": None,
        "strategy": "count_all",
    },
    
    # ========== Level 3: Depend on Level 2 ==========
    "tlocation": {
        "pk": "locationsid",
        "filter_column": "exposuresetsid",  # Has direct FK to ExposureSet
    },
    "tlayer": {
        "pk": "layersid",
        "filter_column": None,  # Need to join via Contract
        "parent_join": {"contractsid": "tcontract"},
    },
    "tstepfunction": {
        "pk": "stepfunctionsid",
        "filter_column": None,
        "parent_join": {"contractsid": "tcontract"},
    },
    "tappliestoeventfilter": {
        "pk": "appliestoeventfiltersid",
        "filter_column": None,  # FK to ReinsuranceTreaty
        "strategy": "count_all",
    },
    "treinsurancetreatyreinstatement": {
        "pk": "reinsurancetreatyreinstatem",
        "filter_column": None,
        "strategy": "count_all",
    },
    "treinsurancetreatysavedresults": {
        "pk": "reinsurancetreatysavedresultsid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "treinsurancetreatyterrorismoption": {
        "pk": "reinsurancetreatysid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "treinsurancetreatyudcclassification_xref": {
        "pk": "reinsurancetreatysid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "tportfolioreinsurancetreaty": {
        "pk": "portfoliosid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "tclfcatalog": {
        "pk": "clfcatalogsid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "tcompanylossmarketshare": {
        "pk": "companylossmaketsharesid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "tcompanyclasizes": {
        "pk": "companysid",
        "filter_column": None,
        "strategy": "count_all",
    },
    
    # ========== Level 4: Depend on Level 3 ==========
    "tlayercondition": {
        "pk": "layerconditionsid",
        "filter_column": None,
        "parent_join": {"layersid": "tlayer"},  # Join via Layer → Contract
    },
    "tlocterm": {
        "pk": "loctermsid",
        "filter_column": None,
        "parent_join": {"locationsid": "tlocation"},  # Join via Location
    },
    "tlocfeature": {
        "pk": "locationsid",
        "filter_column": None,
        "parent_join": {"locationsid": "tlocation"},
    },
    "tlocoffshore": {
        "pk": "locationsid",
        "filter_column": None,
        "parent_join": {"locationsid": "tlocation"},
    },
    "tlocparsedaddress": {
        "pk": "locationsid",
        "filter_column": None,
        "parent_join": {"locationsid": "tlocation"},
    },
    "tlocwc": {
        "pk": "locationsid",
        "filter_column": None,
        "parent_join": {"locationsid": "tlocation"},
    },
    "tenginelocphysicalproperty": {
        "pk": "locationsid",
        "filter_column": None,
        "parent_join": {"locationsid": "tlocation"},
    },
    "tstepfunctiondetail": {
        "pk": "stepfunctionsid",
        "filter_column": None,
        "parent_join": {"stepfunctionsid": "tstepfunction"},
    },
    "tappliestoeventfiltergeoxref": {
        "pk": "appliestoeventfiltersid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "tappliestoeventfilterlatlong": {
        "pk": "appliestoeventfiltersid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "tappliestoeventfilterrule": {
        "pk": "appliestoeventfilterrulesid",
        "filter_column": None,
        "strategy": "count_all",
    },
    "tprogrambusinesscategoryfactor": {
        "pk": "contractsid",
        "filter_column": None,
        "parent_join": {"contractsid": "tcontract"},
    },
    
    # ========== Level 5: Junction/child tables ==========
    "tlayerconditionlocationxref": {
        "pk": "layerconditionsid",
        "filter_column": None,
        "parent_join": {"layerconditionsid": "tlayercondition"},
    },
    "tlocstepfunctionxref": {
        "pk": "locationsid",
        "filter_column": None,
        "parent_join": {"locationsid": "tlocation"},
    },
    "tappliestoareageoxref": {
        "pk": "appliestoareasid",
        "filter_column": None,
        "strategy": "count_all",
    },
}


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Main Lambda handler for size estimation.
    
    Input for Extraction:
    {
        "mode": "extraction",
        "tenant_id": "uuid",
        "exposure_ids": [8858, 8847, 8843],
        "run_id": "uuid",
        "activity_id": "uuid",
        "callback_url": "https://...",
        "artifact_type": "mdf"
    }
    
    Input for Ingestion:
    {
        "mode": "ingestion",
        "tenant_id": "uuid",
        "s3_input_path": "s3://bucket/path/to/file.mdf"
    }
    
    Output:
    {
        ...original input,
        "sde_context": {
            "tenant_id": "uuid",
            "resource_id": "dclegend01",
            "glue_database": "rs_cdkdev_dclegend01_exposure_db",
            "bucket_name": "rs-cdkdev-dclegend01-s3",
            "tenant_role": "arn:aws:iam::451952076009:role/rs-cdkdev-dclegend01-role"
        },
        "estimated_size_bytes": 157286400,
        "total_record_count": 314572,
        "selected_path": "EXPRESS" | "STANDARD" | "BATCH"
    }
    
    Downstream Fargate/Batch tasks receive sde_context as env vars:
    - TENANT_ROLE_ARN: Tasks assume this role for cross-account S3/Glue access
    - GLUE_DATABASE, TENANT_BUCKET, etc.
    """
    tenant_id = event.get('tenant_id', '')
    activity_id = event.get('activity_id', '')
    mode = event.get('mode', 'extraction')

    # Inject tenant_id / activity_id into every subsequent log record
    _log_context.tenant_id = tenant_id
    _log_context.activity_id = activity_id

    logger.info("Received event", extra={"event": event})
    
    # ==========================================================================
    # Step 1: Fetch Tenant Context
    # ==========================================================================
    if not tenant_id:
        raise ValueError("tenant_id is required")
    
    # Fetch tenant context - MUST succeed, no fallback
    raw_context = _fetch_tenant_context(tenant_id)
    sde_context = _get_sde_config(raw_context)
    logger.info("Tenant context retrieved", extra={
        "resource_id": sde_context.get('resource_id'),
        "glue_database": sde_context.get('glue_database'),
    })
    
    # ==========================================================================
    # Step 2: Estimate Size
    # ==========================================================================
    if mode == 'extraction':
        size_bytes, record_count = estimate_extraction_size(event, sde_context)
    elif mode == 'ingestion':
        size_bytes = get_ingestion_file_size(event, sde_context)
        record_count = 0  # Unknown for ingestion
    else:
        raise ValueError(f"Unknown mode: {mode}")
    
    # ==========================================================================
    # Step 3: Determine Routing Path
    # ==========================================================================
    if size_bytes <= THRESHOLD_SMALL_BYTES:
        selected_path = "EXPRESS"
    elif size_bytes <= THRESHOLD_LARGE_BYTES:
        selected_path = "STANDARD"
    else:
        selected_path = "BATCH"
    
    # ==========================================================================
    # Step 4: Build Result
    # Include sde_context for downstream tasks (Fargate/Batch)
    # ==========================================================================
    result = {
        **event,
        "estimated_size_bytes": size_bytes,
        "total_record_count": record_count,
        "selected_path": selected_path
    }
    
    # Add sde_context if we have it
    if sde_context:
        result["sde_context"] = sde_context
    
    # Log final Lambda response
    logger.info("Lambda response", extra={
        "estimated_size_bytes": size_bytes,
        "total_record_count": record_count,
        "selected_path": selected_path,
    })

    # ==========================================================================
    # Step 5: Cleanup — delete Athena result files from tenant bucket
    # Path: {tenant_bucket}/sde/Extraction/{activity_id}/
    # Done after estimation so transient query artefacts don't accumulate.
    # ==========================================================================
    if mode == 'extraction' and sde_context:
        _cleanup_bucket = sde_context.get('bucket_name')
        _cleanup_activity_id = event.get('activity_id')
        if _cleanup_bucket and _cleanup_activity_id:
            _cleanup_athena_results(
                bucket=_cleanup_bucket,
                activity_id=_cleanup_activity_id,
                tenant_credentials=sde_context.get('aws_credentials'),
            )

    return result


def _cleanup_athena_results(
    bucket: str,
    activity_id: str,
    tenant_credentials: dict | None = None,
) -> None:
    """
    Delete all Athena result files written to the tenant bucket for this activity.

    Path deleted: s3://{bucket}/sde/Extraction/{activity_id}/

    Uses tenant credentials when available (Athena wrote to tenant bucket via
    tenant workgroup/IAM), falling back to Lambda execution role.
    Always swallows exceptions — cleanup failure must not affect the Lambda response.
    """
    prefix = f"sde/Extraction/{activity_id}/"
    try:
        if tenant_credentials:
            s3 = boto3.client(
                's3',
                aws_access_key_id=tenant_credentials.get('access_key', ''),
                aws_secret_access_key=tenant_credentials.get('secret_key', ''),
                aws_session_token=tenant_credentials.get('session_token', ''),
            )
        else:
            s3 = boto3.client('s3')

        paginator = s3.get_paginator('list_objects_v2')
        deleted = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects = page.get('Contents', [])
            if not objects:
                continue
            delete_keys = [{'Key': obj['Key']} for obj in objects]
            s3.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys, 'Quiet': True})
            deleted += len(delete_keys)

        if deleted:
            logger.info(f"Cleanup: deleted {deleted} Athena result file(s) from s3://{bucket}/{prefix}")
        else:
            logger.info(f"Cleanup: no Athena result files found under s3://{bucket}/{prefix}")

    except Exception as exc:  # noqa: BLE001
        logger.warning(f"Cleanup: failed to delete s3://{bucket}/{prefix} — {exc} (non-fatal)")


def estimate_extraction_size(
    event: dict[str, Any],
    sde_context: dict[str, Any] | None = None
) -> tuple[int, int]:
    """
    Estimate the expected MDF size for extraction by querying Iceberg tables.
    
    Dynamically discovers tables from Glue/Iceberg catalog and builds PK/FK join
    chain in-memory for extraction queries.
    
    Args:
        event: Input event with:
            - exposure_ids: List of exposure set IDs to filter
            - TABLES: "all" or list of specific table names (default: "all")
        sde_context: Tenant context from tenant context lambda (contains glue_database)
    
    Table selection:
        - TABLES="all" or not provided: Discover and query all tables in Glue database
        - TABLES=["tcontract", "tlocation", ...]: Query only specified tables
        - TABLES="tcontract": Query single table
    
    Dynamic Discovery:
        1. Query Glue catalog to find all tables in tenant's database
        2. Get schema for each table to identify columns
        3. Build PK/FK relationships based on naming conventions:
           - PK: table 't{name}' has PK '{name}sid'
           - FK: column '{name}sid' references table 't{name}'
        4. Generate join queries for tables without direct exposuresetsid filter
    
    Returns:
        Tuple of (estimated_bytes, total_record_count)
    """
    exposure_ids = event.get('exposure_ids', [])
    activity_id = event.get('activity_id')
    region = os.environ.get('AWS_REGION', 'us-east-1')
    
    if not exposure_ids:
        logger.warning("No exposure_ids provided, assuming full dataset extraction - using maximum estimate for BATCH routing")
        max_estimate_bytes = THRESHOLD_LARGE_BYTES + (10 * 1024 * 1024 * 1024)  # 70GB
        return (max_estimate_bytes, 0)
    
    # Get Glue database name
    glue_database = None
    if sde_context:
        glue_database = sde_context.get('glue_database')
        logger.info(f"Using glue_database from tenant context: {glue_database}")
    
    if not glue_database:
        glue_database = event.get('glue_database')
        if glue_database:
            logger.info(f"Using glue_database from event input: {glue_database}")
    
    if not glue_database:
        logger.warning("No glue_database available, using fallback estimation")
        return fallback_estimate(exposure_ids)
    
    # Get tenant credentials
    tenant_credentials = None
    if sde_context and 'aws_credentials' in sde_context:
        tenant_credentials = sde_context['aws_credentials']
        logger.info("Using tenant AWS credentials for Glue/Athena queries")
    
    # Get tenant bucket for Athena output
    output_bucket = sde_context.get('bucket_name') if sde_context else None
    
    # Derive workgroup name from bucket name
    workgroup = None
    if output_bucket and output_bucket.endswith('-s3'):
        workgroup = output_bucket[:-3] + '-workgroup'
        logger.info(f"Using Athena workgroup: {workgroup}")
    
    # ==========================================================================
    # STEP 1: DISCOVER TABLES FROM GLUE/ICEBERG
    # ==========================================================================
    logger.info(f"Discovering tables from Glue database: {glue_database}")
    available_tables = discover_glue_tables(glue_database, tenant_credentials, region)
    
    if not available_tables:
        logger.warning("No tables found in Glue database, using fallback estimation")
        return fallback_estimate(exposure_ids)
    
    # ==========================================================================
    # STEP 2: DETERMINE WHICH TABLES TO EXTRACT
    # ==========================================================================
    tables_input = event.get('TABLES', 'all')
    
    if tables_input == 'all' or not tables_input:
        requested_tables = None  # Will query all discovered tables
        logger.info(f"TABLES='all': Will query all {len(available_tables)} discovered tables")
    elif isinstance(tables_input, list):
        requested_tables = [t.lower() for t in tables_input]
        logger.info(f"TABLES list: Will query specific tables: {requested_tables}")
    elif isinstance(tables_input, str):
        requested_tables = [tables_input.lower()]
        logger.info(f"TABLES string: Will query single table: {tables_input}")
    else:
        logger.warning(f"Invalid TABLES format: {type(tables_input)}, defaulting to all")
        requested_tables = None
    
    # ==========================================================================
    # STEP 3: BUILD IN-MEMORY PK/FK JOIN CHAIN
    # ==========================================================================
    logger.info("Building dynamic PK/FK join chain from table schemas")
    join_chain = build_table_join_chain(
        database=glue_database,
        available_tables=available_tables,
        requested_tables=requested_tables,
        tenant_credentials=tenant_credentials,
        region=region
    )
    
    if not join_chain:
        logger.warning("Failed to build join chain, using fallback estimation")
        return fallback_estimate(exposure_ids)

    # ==========================================================================
    # STEP 3.5: ENRICH JOIN CHAIN WITH ICEBERG $files STATS
    #
    # Query the hidden "$files" metadata table for each discovered table.
    # This returns the ACTIVE live data file sizes only — critical for accuracy.
    #
    # NOTE: Glue's totalSize parameter includes ALL S3 objects (old snapshots,
    # manifests, vacuumed files not yet deleted) and is NOT reliable for size
    # estimation. The $files query reflects only manifest-tracked live files.
    # ==========================================================================
    if output_bucket:
        logger.info("Fetching Iceberg $files stats for size estimation…")
        for tname in list(join_chain.keys()):
            # Only fetch if stats not already populated
            if join_chain[tname].get('total_rows', 0) == 0:
                stats = fetch_iceberg_table_stats(
                    database=glue_database,
                    table_name=tname,
                    output_bucket=output_bucket,
                    tenant_credentials=tenant_credentials,
                    workgroup=workgroup,
                    region=region,
                    activity_id=activity_id,
                )
                if stats['total_rows'] > 0:
                    join_chain[tname]['total_rows'] = stats['total_rows']
                    join_chain[tname]['total_size_bytes'] = stats['total_size_bytes']
                    logger.info(
                        f"  Iceberg $files [{tname}]: "
                        f"{stats['total_rows']:,} rows, "
                        f"{stats['total_size_bytes'] / (1024*1024):.2f} MB parquet"
                    )
                else:
                    logger.warning(f"  Iceberg $files [{tname}]: no stats available — will use fallback")
    else:
        logger.warning("No output_bucket — skipping Iceberg $files stats (no Athena output location)")

    # ==========================================================================
    # STEP 4: QUERY ATHENA TO COUNT RECORDS (PARALLEL)
    #
    # All COUNT queries are fired simultaneously (Phase 1), then polled together
    # in a single shared loop (Phase 2). Worst-case wall time = slowest single
    # query (~300s) regardless of N tables, vs. the old serial N×300s.
    # ==========================================================================
    logger.info("Extraction size estimation — firing all Athena count queries in parallel")

    table_counts: dict[str, int] = {}
    athena_client = _make_athena_client(tenant_credentials, region)

    # Phase 1: build SQL and fire all queries simultaneously (non-blocking)
    pending_queries: dict[str, str] = {}  # {table_name: query_execution_id}
    for table_name, config in join_chain.items():
        try:
            query = _build_count_query(glue_database, table_name, config, exposure_ids)
            if query:
                qid = _start_athena_query_raw(
                    query, glue_database, output_bucket,
                    athena_client, workgroup, activity_id
                )
                pending_queries[table_name] = qid
                strategy = "direct_filter" if config.get("filter_column") else ("fk_join" if config.get("parent_join") else "count_all")
                logger.info(f"  {table_name}: query submitted ({strategy}) → {qid}")
            else:
                table_counts[table_name] = 0
        except Exception as e:
            logger.warning(f"  {table_name}: ERROR submitting query - {e}")
            table_counts[table_name] = 0

    # Phase 2: poll all in-flight queries together until each finishes
    if pending_queries:
        logger.info(f"Polling {len(pending_queries)} Athena queries in parallel (max_wait=300s)…")
        polled = _poll_athena_queries(pending_queries, athena_client, max_wait=300)
        for table_name, count in polled.items():
            table_counts[table_name] = count
            strategy = "direct_filter" if join_chain[table_name].get("filter_column") else ("fk_join" if join_chain[table_name].get("parent_join") else "count_all")
            logger.info(f"  {table_name}: {count:,} records ({strategy})")

    total_records = sum(table_counts.values())
    logger.info("Table count query complete", extra={"total_records": total_records, "table_count": len(table_counts)})

    # ==========================================================================
    # STEP 4b: FALLBACK ESTIMATE FOR TIMED-OUT DIRECT-FILTER TABLES
    #
    # Large tables like tlocation have no partition on exposuresetsid, so
    # Athena COUNT(*) queries time out and return 0.  If the table returned 0
    # AND it uses a direct filter_column (i.e. it was a counting candidate, not
    # a "count_all" table), look for child tables that join to it and succeeded.
    # Estimate parent count as:
    #   fraction = best_child_matched / best_child_total_rows  (from Glue stats)
    #   estimated = fraction × parent_total_rows
    # ==========================================================================
    fallback_applied = False
    for table_name in list(table_counts.keys()):
        if table_counts[table_name] != 0:
            continue
        config = join_chain.get(table_name, {})
        # Only attempt fallback for direct-filter tables (not count_all tables)
        if not config.get("filter_column"):
            continue
        parent_total_rows = config.get("total_rows", 0)
        if parent_total_rows == 0:
            continue  # No Glue stats available — cannot estimate

        # Find the best child: highest selection fraction with known total_rows
        best_fraction = 0.0
        best_child = None
        for child_name, child_config in join_chain.items():
            if child_name == table_name:
                continue
            parent_join = child_config.get("parent_join") or {}
            if table_name not in parent_join.values():
                continue
            child_count = table_counts.get(child_name, 0)
            child_total = child_config.get("total_rows", 0)
            if child_count > 0 and child_total > 0:
                fraction = child_count / child_total
                if fraction > best_fraction:
                    best_fraction = fraction
                    best_child = child_name

        if best_child and best_fraction > 0:
            estimated_count = int(best_fraction * parent_total_rows)
            logger.warning(
                f"{table_name}: returned 0 (likely timeout). "
                f"Fallback via child '{best_child}': "
                f"{best_fraction:.4%} × {parent_total_rows:,} total rows = {estimated_count:,} estimated rows"
            )
            table_counts[table_name] = estimated_count
            total_records += estimated_count
            fallback_applied = True

    if fallback_applied:
        logger.info("Adjusted total records after timeout fallbacks", extra={"adjusted_total_records": total_records})

    # ==========================================================================
    # STEP 5: CALCULATE ESTIMATED MDF SIZE USING GLUE STATISTICS
    #
    # Formula per table:
    #   mdf_bytes = (matched_rows / total_rows) × parquet_total_size × PARQUET_TO_MDF_EXPANSION_FACTOR
    #
    # If Glue stats are missing (numRows=0), fall back to AVG_BYTES_PER_RECORD.
    # Parquet→MDF expansion factor (~8×) accounts for:
    #   - Columnar→row-based format change
    #   - Parquet compression removed
    #   - SQL Server 8KB page alignment + fill factor
    #   - Index pages (clustered + non-clustered)
    # ==========================================================================
    estimated_bytes = 0
    fallback_records = 0  # records where Glue stats were unavailable
    
    logger.info("Extraction size estimation — calculating per-table MDF size", extra={"parquet_to_mdf_expansion_factor": PARQUET_TO_MDF_EXPANSION_FACTOR})
    
    for table_name, matched_rows in table_counts.items():
        if matched_rows == 0:
            continue
        
        config = join_chain.get(table_name, {})
        total_rows = config.get('total_rows', 0)
        total_size_bytes = config.get('total_size_bytes', 0)
        
        if total_rows > 0 and total_size_bytes > 0:
            # Use Glue statistics for accurate estimate
            fraction = matched_rows / total_rows
            table_mdf_bytes = int(fraction * total_size_bytes * PARQUET_TO_MDF_EXPANSION_FACTOR)
            bytes_per_row = total_size_bytes / total_rows
            logger.info(
                f"  {table_name}: {matched_rows:,}/{total_rows:,} rows "
                f"({fraction*100:.2f}%) × {total_size_bytes/(1024*1024):.2f}MB parquet "
                f"× {PARQUET_TO_MDF_EXPANSION_FACTOR}× = {table_mdf_bytes/(1024*1024):.3f} MB "
                f"({bytes_per_row:.1f} bytes/row parquet)"
            )
        else:
            # Fallback: Glue stats not available — estimate from column count × expansion
            num_columns = len(config.get('columns', []))
            if num_columns > 0:
                estimated_parquet_bytes_per_row = num_columns * AVG_PARQUET_BYTES_PER_COLUMN
                table_mdf_bytes = int(matched_rows * estimated_parquet_bytes_per_row * PARQUET_TO_MDF_EXPANSION_FACTOR)
                fallback_records += matched_rows
                logger.info(
                    f"  {table_name}: {matched_rows:,} rows × {num_columns} cols "
                    f"× {AVG_PARQUET_BYTES_PER_COLUMN} bytes/col × {PARQUET_TO_MDF_EXPANSION_FACTOR}× "
                    f"[FALLBACK col-based] = {table_mdf_bytes/(1024*1024):.3f} MB"
                )
            else:
                # Last resort: no column info at all
                table_mdf_bytes = matched_rows * AVG_BYTES_PER_RECORD
                fallback_records += matched_rows
                logger.info(
                    f"  {table_name}: {matched_rows:,} rows × {AVG_BYTES_PER_RECORD} bytes "
                    f"[FALLBACK no-schema] = {table_mdf_bytes/(1024*1024):.3f} MB"
                )
        
        estimated_bytes += table_mdf_bytes
    
    if fallback_records > 0:
        logger.warning(
            f"  {fallback_records:,} records used column-based fallback "
            f"(num_cols × {AVG_PARQUET_BYTES_PER_COLUMN} bytes × {PARQUET_TO_MDF_EXPANSION_FACTOR}× expansion) — Iceberg stats unavailable"
        )
    
    # Minimum 1MB
    estimated_bytes = max(estimated_bytes, 1024 * 1024)
    
    # Convert to human-readable size
    def _human_size(b: int) -> str:
        if b >= 1024 ** 3:
            return f"{b / (1024 ** 3):.2f} GB"
        elif b >= 1024 ** 2:
            return f"{b / (1024 ** 2):.2f} MB"
        return f"{b / 1024:.2f} KB"
    
    # Determine routing path
    if estimated_bytes <= THRESHOLD_SMALL_BYTES:
        routing = "EXPRESS (≤7GB)"
    elif estimated_bytes <= THRESHOLD_LARGE_BYTES:
        routing = "STANDARD (7-60GB)"
    else:
        routing = "BATCH (>60GB)"
    
    logger.info("Extraction size estimation complete", extra={
        "total_matched_records": total_records,
        "estimated_mdf_bytes": estimated_bytes,
        "estimated_mdf_human": _human_size(estimated_bytes),
        "routing_path": routing,
    })
    
    return (estimated_bytes, total_records)


def query_table_count_dynamic(
    database: str,
    table_name: str,
    exposure_ids: list[int],
    config: dict,
    output_bucket: str | None = None,
    tenant_credentials: dict | None = None,
    workgroup: str | None = None,
    activity_id: str | None = None,
) -> int:
    """
    Query Athena to count records using dynamically discovered join chain.
    
    Uses the in-memory join chain built from Glue table schemas to construct
    the appropriate query for each table.
    
    Strategies based on discovered schema:
    1. Direct filter: Table has 'exposuresetsid' column -> filter directly
    2. FK join: Follow parent_join chain to reach table with exposuresetsid
    3. Count all: Table has no path to exposuresetsid (small reference tables)
    
    Args:
        database: Glue database name
        table_name: Table to query
        exposure_ids: List of exposure set IDs to filter
        config: Table configuration from dynamically built join_chain:
            - pk: Primary key column
            - columns: List of all columns
            - filter_column: 'exposuresetsid' if present
            - parent_join: {fk_column: parent_table} FK relationships
        output_bucket: S3 bucket for Athena results
        tenant_credentials: AWS credentials for tenant access
        workgroup: Athena workgroup name
    
    Returns:
        Record count or 0 on error
    """
    filter_column = config.get("filter_column")
    parent_join = config.get("parent_join")
    exposure_ids_str = ", ".join(str(id) for id in exposure_ids)
    
    # Strategy 1: Direct filter - table has exposuresetsid column
    if filter_column == "exposuresetsid":
        query = f"""
            SELECT COUNT(*) as cnt 
            FROM "{database}"."{table_name}"
            WHERE exposuresetsid IN ({exposure_ids_str})
        """
        return execute_athena_count_query(query, database, output_bucket, tenant_credentials, workgroup)
    
    # Strategy 2: FK join - follow parent_join chain to reach exposuresetsid
    if parent_join:
        # Try to build a join path to a table with exposuresetsid
        query = build_join_query_to_exposure(
            database=database,
            table_name=table_name,
            config=config,
            exposure_ids_str=exposure_ids_str
        )
        if query:
            return execute_athena_count_query(query, database, output_bucket, tenant_credentials, workgroup)
    
    # Strategy 3: No path to exposuresetsid - count all (reference tables)
    # Only do this for small reference tables to avoid huge counts
    if table_name in ["tcompany", "tportfoliofilter", "tappliestoarea", "tsidcontrol"]:
        query = f"""
            SELECT COUNT(*) as cnt 
            FROM "{database}"."{table_name}"
        """
        return execute_athena_count_query(query, database, output_bucket, tenant_credentials, workgroup)
    
    # Unknown table relationship - skip
    logger.debug(f"No extraction strategy for table {table_name}, skipping")
    return 0


def build_join_query_to_exposure(
    database: str,
    table_name: str,
    config: dict,
    exposure_ids_str: str,
    visited: set | None = None,
    depth: int = 0,
    max_depth: int = 5
) -> str | None:
    """
    Build a JOIN query to connect table to exposuresetsid filter.
    
    Recursively follows FK relationships until we reach a table with
    exposuresetsid column. Builds the necessary JOINs along the path.
    
    Args:
        database: Glue database name
        table_name: Starting table
        config: Table config from join_chain
        exposure_ids_str: Comma-separated exposure IDs
        visited: Set of visited tables (cycle detection)
        depth: Current recursion depth
        max_depth: Maximum join depth to prevent infinite loops
    
    Returns:
        SQL query string or None if no path found
    """
    if depth >= max_depth:
        return None
    
    if visited is None:
        visited = set()
    
    if table_name in visited:
        return None  # Cycle detected
    
    visited.add(table_name)
    
    parent_join = config.get("parent_join")
    if not parent_join:
        return None
    
    # Try each FK relationship
    for fk_column, parent_table in parent_join.items():
        # Check if parent has direct exposuresetsid
        # For known direct-filter tables
        if parent_table in KNOWN_DIRECT_FILTER_TABLES:
            # Get parent's PK column
            parent_pk = PK_NAMING_PATTERN(parent_table)
            
            if parent_table == "texposureset":
                # Special case: exposuresetsid IS the PK
                query = f"""
                    SELECT COUNT(*) as cnt
                    FROM "{database}"."{table_name}" t
                    JOIN "{database}"."{parent_table}" p ON t.{fk_column} = p.exposuresetsid
                    WHERE p.exposuresetsid IN ({exposure_ids_str})
                """
            else:
                query = f"""
                    SELECT COUNT(*) as cnt
                    FROM "{database}"."{table_name}" t
                    JOIN "{database}"."{parent_table}" p ON t.{fk_column} = p.{parent_pk}
                    WHERE p.exposuresetsid IN ({exposure_ids_str})
                """
            return query
    
    # If direct parent doesn't have exposuresetsid, we'd need deeper recursion
    # For simplicity, stop here - deeper chains are handled by TABLE_HIERARCHY fallback
    return None


def query_table_count(
    database: str,
    table_name: str,
    exposure_ids: list[int],
    config: dict,
    output_bucket: str | None = None,
    tenant_credentials: dict | None = None,
    workgroup: str | None = None
) -> int:
    """
    Query Athena to count records in a table for the given exposure_ids.
    
    Strategies:
    1. filter_column: Direct filter - table has exposuresetsid column
    2. parent_join: Join with parent table following FK hierarchy
    3. count_all: Count all rows (for small reference tables)
    
    Args:
        database: Glue database name
        table_name: Table to query
        exposure_ids: List of exposure set IDs to filter
        config: Table configuration from TABLE_HIERARCHY
        output_bucket: S3 bucket for Athena results (tenant bucket)
        tenant_credentials: AWS credentials for tenant resource access
    """
    filter_column = config.get("filter_column")
    strategy = config.get("strategy")
    
    # Strategy 1: Direct filter - table has exposuresetsid column
    if filter_column:
        exposure_ids_str = ", ".join(str(id) for id in exposure_ids)
        query = f"""
            SELECT COUNT(*) as cnt 
            FROM "{database}"."{table_name}"
            WHERE {filter_column} IN ({exposure_ids_str})
        """
        return execute_athena_count_query(query, database, output_bucket, tenant_credentials, workgroup)
    
    # Strategy 2: Count all (for small reference tables without exposure FK)
    if strategy == "count_all":
        query = f"""
            SELECT COUNT(*) as cnt 
            FROM "{database}"."{table_name}"
        """
        return execute_athena_count_query(query, database, output_bucket, tenant_credentials, workgroup)
    
    # Strategy 3: Join with parent table
    if "parent_join" in config:
        parent_join = config["parent_join"]
        join_column = list(parent_join.keys())[0]
        parent_table = parent_join[join_column]
        parent_config = TABLE_HIERARCHY.get(parent_table, {})
        
        exposure_ids_str = ", ".join(str(id) for id in exposure_ids)
        
        # Build join query based on parent table type
        if parent_table == "tcontract":
            # Join child → contract → filter by exposuresetsid
            query = f"""
                SELECT COUNT(*) as cnt
                FROM "{database}"."{table_name}" t
                JOIN "{database}"."{parent_table}" p ON t.{join_column} = p.contractsid
                WHERE p.exposuresetsid IN ({exposure_ids_str})
            """
        elif parent_table == "tlayer":
            # Join child → layer → contract → filter by exposuresetsid
            query = f"""
                SELECT COUNT(*) as cnt
                FROM "{database}"."{table_name}" t
                JOIN "{database}"."tlayer" l ON t.{join_column} = l.layersid
                JOIN "{database}"."tcontract" c ON l.contractsid = c.contractsid
                WHERE c.exposuresetsid IN ({exposure_ids_str})
            """
        elif parent_table == "tlocation":
            # Location has direct exposuresetsid
            query = f"""
                SELECT COUNT(*) as cnt
                FROM "{database}"."{table_name}" t
                JOIN "{database}"."tlocation" loc ON t.{join_column} = loc.locationsid
                WHERE loc.exposuresetsid IN ({exposure_ids_str})
            """
        elif parent_table == "tstepfunction":
            # StepFunction → Contract
            query = f"""
                SELECT COUNT(*) as cnt
                FROM "{database}"."{table_name}" t
                JOIN "{database}"."tstepfunction" sf ON t.{join_column} = sf.stepfunctionsid
                JOIN "{database}"."tcontract" c ON sf.contractsid = c.contractsid
                WHERE c.exposuresetsid IN ({exposure_ids_str})
            """
        elif parent_table == "tlayercondition":
            # LayerCondition → Layer → Contract
            query = f"""
                SELECT COUNT(*) as cnt
                FROM "{database}"."{table_name}" t
                JOIN "{database}"."tlayercondition" lc ON t.{join_column} = lc.layerconditionsid
                JOIN "{database}"."tlayer" l ON lc.layersid = l.layersid
                JOIN "{database}"."tcontract" c ON l.contractsid = c.contractsid
                WHERE c.exposuresetsid IN ({exposure_ids_str})
            """
        else:
            # Generic: try parent's filter_column or skip
            parent_filter = parent_config.get("filter_column")
            if parent_filter:
                query = f"""
                    SELECT COUNT(*) as cnt
                    FROM "{database}"."{table_name}" t
                    JOIN "{database}"."{parent_table}" p ON t.{join_column} = p.{parent_config.get('pk', join_column)}
                    WHERE p.{parent_filter} IN ({exposure_ids_str})
                """
            else:
                logger.debug(f"No join strategy for {table_name} -> {parent_table}, skipping")
                return 0
        
        return execute_athena_count_query(query, database, output_bucket, tenant_credentials, workgroup, activity_id=activity_id)
    
    # No filter strategy available - skip
    logger.debug(f"No filter strategy for {table_name}, skipping")
    return 0


def _make_athena_client(tenant_credentials: dict | None, region: str):
    """
    Build a boto3 Athena client once, using tenant credentials when provided.
    Logs masked credential info and verifies identity via STS.
    Called once per Step 4 invocation; the returned client is shared across
    all parallel _start_athena_query_raw / _poll_athena_queries calls.
    """
    if tenant_credentials:
        access_key = tenant_credentials.get('access_key', '')
        secret_key = tenant_credentials.get('secret_key', '')
        session_token = tenant_credentials.get('session_token', '')
        masked_access_key = f"{access_key[:4]}...{access_key[-4:]}" if len(access_key) > 8 else "***"
        has_session_token = "Yes" if session_token else "No"
        logger.info(f"Using TENANT credentials for Athena - AccessKeyId: {masked_access_key}, HasSessionToken: {has_session_token}")
        try:
            sts_client = boto3.client(
                'sts',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                region_name=region,
            )
            caller_identity = sts_client.get_caller_identity()
            logger.info(f"Tenant credentials - Account: {caller_identity.get('Account')}, ARN: {caller_identity.get('Arn')}, UserId: {caller_identity.get('UserId')}")
        except Exception as e:
            logger.warning(f"Could not get caller identity for tenant credentials: {e}")
        return boto3.client(
            'athena',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region,
        )
    else:
        logger.info("Using LAMBDA execution role credentials for Athena (no tenant credentials provided)")
        return get_athena_client()


def _build_count_query(
    database: str,
    table_name: str,
    config: dict,
    exposure_ids: list[int],
) -> str | None:
    """
    Build the Athena COUNT SQL for the given table without executing it.
    Returns None when no strategy is applicable (table will be skipped).
    Mirrors the logic of query_table_count_dynamic but is side-effect-free.
    """
    filter_column = config.get("filter_column")
    parent_join = config.get("parent_join")
    exposure_ids_str = ", ".join(str(i) for i in exposure_ids)

    # Strategy 1: Direct filter
    if filter_column == "exposuresetsid":
        return f"""
            SELECT COUNT(*) as cnt
            FROM "{database}"."{table_name}"
            WHERE exposuresetsid IN ({exposure_ids_str})
        """

    # Strategy 2: FK join — follow parent_join chain to reach exposuresetsid
    if parent_join:
        return build_join_query_to_exposure(
            database=database,
            table_name=table_name,
            config=config,
            exposure_ids_str=exposure_ids_str,
        )

    # Strategy 3: Count all (small reference tables with no exposure FK)
    if table_name in ["tcompany", "tportfoliofilter", "tappliestoarea", "tsidcontrol"]:
        return f"""
            SELECT COUNT(*) as cnt
            FROM "{database}"."{table_name}"
        """

    return None  # No strategy — table will be skipped


def _start_athena_query_raw(
    query: str,
    database: str,
    output_bucket: str | None,
    athena_client,
    workgroup: str | None = None,
    activity_id: str | None = None,
) -> str:
    """
    Submit a single Athena query and return its QueryExecutionId immediately
    (non-blocking — does NOT poll for completion).
    """
    if not output_bucket:
        raise ValueError(
            "No tenant bucket available for Athena output. "
            "Tenant context must include 'bucket_name'."
        )
    athena_prefix = f"sde/Extraction/{activity_id}/" if activity_id else "sde/Extraction/"
    query_params = {
        'QueryString': query,
        'QueryExecutionContext': {'Database': database},
        'ResultConfiguration': {
            'OutputLocation': f's3://{output_bucket}/{athena_prefix}'
        }
    }
    if workgroup:
        query_params['WorkGroup'] = workgroup
    logger.debug(f"Firing Athena query - Database: {database}, Workgroup: {workgroup or 'primary'}")
    logger.debug(f"Query: {query[:200]}..." if len(query) > 200 else f"Query: {query}")
    response = athena_client.start_query_execution(**query_params)
    return response['QueryExecutionId']


def _poll_athena_queries(
    pending: dict[str, str],
    athena_client,
    max_wait: int = 300,
) -> dict[str, int]:
    """
    Poll a batch of already-submitted Athena queries in parallel until all
    finish or max_wait seconds have elapsed.

    Args:
        pending:       {table_name: query_execution_id}
        athena_client: shared boto3 Athena client (already holds tenant creds)
        max_wait:      wall-clock seconds before declaring timeout

    Returns:
        {table_name: count}  — 0 on failure or timeout
    """
    results: dict[str, int] = {}
    remaining = dict(pending)  # copy; entries are removed as they complete
    wait_interval = 0.5
    elapsed = 0.0

    while remaining and elapsed < max_wait:
        finished = []
        for table_name, qid in remaining.items():
            try:
                resp = athena_client.get_query_execution(QueryExecutionId=qid)
                state = resp['QueryExecution']['Status']['State']
            except Exception as e:
                logger.warning(f"  {table_name}: error polling query {qid}: {e}")
                results[table_name] = 0
                finished.append(table_name)
                continue

            if state == 'SUCCEEDED':
                try:
                    qr = athena_client.get_query_results(QueryExecutionId=qid)
                    rows = qr.get('ResultSet', {}).get('Rows', [])
                    count_str = rows[1]['Data'][0].get('VarCharValue', '0') if len(rows) >= 2 else '0'
                    results[table_name] = int(count_str)
                except Exception as e:
                    logger.warning(f"  {table_name}: error reading results for {qid}: {e}")
                    results[table_name] = 0
                finished.append(table_name)
            elif state in ('FAILED', 'CANCELLED'):
                reason = resp['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                logger.warning(f"  {table_name}: query {state} — {reason}")
                results[table_name] = 0
                finished.append(table_name)

        for t in finished:
            remaining.pop(t)

        if remaining:
            time.sleep(wait_interval)
            elapsed += wait_interval

    # Any still-pending queries exceeded max_wait
    for table_name in remaining:
        logger.warning(f"  {table_name}: poll timeout after {max_wait}s — treating as 0")
        results[table_name] = 0

    return results


def execute_athena_count_query(query: str, database: str, output_bucket: str | None = None, tenant_credentials: dict | None = None, workgroup: str | None = None, activity_id: str | None = None) -> int:
    """
    Execute a COUNT query in Athena and return the result.
    
    Args:
        query: SQL query to execute
        database: Glue database name
        output_bucket: S3 bucket for Athena results (uses tenant bucket or fallback)
        tenant_credentials: AWS credentials for tenant resource access
        workgroup: Athena workgroup name (for tenant-specific workgroups)
    """
    region = os.environ.get('AWS_REGION', 'us-east-1')
    
    # Create Athena client with tenant credentials if provided
    if tenant_credentials:
        access_key = tenant_credentials.get('access_key', '')
        secret_key = tenant_credentials.get('secret_key', '')
        session_token = tenant_credentials.get('session_token', '')
        
        # Log credential info (masked for security)
        masked_access_key = f"{access_key[:4]}...{access_key[-4:]}" if len(access_key) > 8 else "***"
        has_session_token = "Yes" if session_token else "No"
        logger.info(f"Using TENANT credentials for Athena - AccessKeyId: {masked_access_key}, HasSessionToken: {has_session_token}")
        
        # Get caller identity to verify the role being used
        try:
            sts_client = boto3.client(
                'sts',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                region_name=region
            )
            caller_identity = sts_client.get_caller_identity()
            logger.info(f"Tenant credentials - Account: {caller_identity.get('Account')}, ARN: {caller_identity.get('Arn')}, UserId: {caller_identity.get('UserId')}")
        except Exception as e:
            logger.warning(f"Could not get caller identity for tenant credentials: {e}")
        
        athena = boto3.client(
            'athena',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region
        )
    else:
        logger.info("Using LAMBDA execution role credentials for Athena (no tenant credentials provided)")
        athena = get_athena_client()
    
    # Always use the tenant bucket — no separate Athena results bucket.
    # Path: {tenant_bucket}/sde/Extraction/{activity_id}/
    if not output_bucket:
        raise ValueError(
            "No tenant bucket available for Athena output. "
            "Tenant context must include 'bucket_name'."
        )
    bucket = output_bucket
    logger.info(f"Athena query - Database: {database}, OutputBucket: {bucket}, Workgroup: {workgroup or 'primary'}")
    logger.info(f"Query: {query[:200]}..." if len(query) > 200 else f"Query: {query}")
    
    try:
        # Build start_query_execution parameters
        athena_prefix = f"sde/Extraction/{activity_id}/" if activity_id else "sde/Extraction/"
        query_params = {
            'QueryString': query,
            'QueryExecutionContext': {'Database': database},
            'ResultConfiguration': {
                'OutputLocation': f's3://{bucket}/{athena_prefix}'
            }
        }
        
        # Add workgroup if specified (required for tenant-specific IAM policies)
        if workgroup:
            query_params['WorkGroup'] = workgroup
        
        # Start query execution
        response = athena.start_query_execution(**query_params)
        
        query_execution_id = response['QueryExecutionId']
        
        # Wait for query to complete (with timeout)
        max_wait = 300  # seconds
        wait_interval = 0.5
        elapsed = 0
        
        while elapsed < max_wait:
            result = athena.get_query_execution(QueryExecutionId=query_execution_id)
            state = result['QueryExecution']['Status']['State']
            
            if state == 'SUCCEEDED':
                break
            elif state in ('FAILED', 'CANCELLED'):
                reason = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                logger.warning(f"Query failed: {reason}")
                return 0
            
            time.sleep(wait_interval)
            elapsed += wait_interval
        
        if elapsed >= max_wait:
            logger.warning(f"Query timeout after {max_wait}s")
            return 0
        
        # Get results
        results = athena.get_query_results(QueryExecutionId=query_execution_id)
        
        # Parse count from results
        rows = results.get('ResultSet', {}).get('Rows', [])
        if len(rows) >= 2:  # Header + data row
            count_str = rows[1]['Data'][0].get('VarCharValue', '0')
            return int(count_str)
        
        return 0
        
    except Exception as e:
        logger.error(f"Athena query error: {e}")
        return 0


def fallback_estimate(exposure_ids: list[int]) -> tuple[int, int]:
    """
    Fallback estimation when Athena is not available.
    Uses a heuristic based on exposure count.
    
    Assumes average of 1000 records per exposure across all tables.
    """
    num_exposures = len(exposure_ids)
    estimated_records = num_exposures * 1000  # 1000 records per exposure
    estimated_bytes = estimated_records * AVG_BYTES_PER_RECORD
    estimated_bytes = int(estimated_bytes * 1.25)  # MDF overhead
    
    logger.info(f"Fallback estimate: {num_exposures} exposures × 1000 records = {estimated_records} records")
    return (max(estimated_bytes, 1024 * 1024), estimated_records)


def get_ingestion_file_size(
    event: dict[str, Any],
    sde_context: dict[str, Any] | None = None
) -> int:
    """
    Get actual file size from S3 using HeadObject.
    
    Args:
        event: Input event with s3_input_path
        sde_context: Optional tenant context (can provide bucket_name)
    
    Returns:
        File size in bytes
    """
    s3 = get_s3_client()
    
    # Try to get S3 path from event or sde_context
    s3_input_path = event.get('s3_input_path')
    
    if not s3_input_path and sde_context:
        bucket = sde_context.get('bucket_name', '')
        if bucket:
            # Construct path from bucket and expected artifact location
            s3_input_path = f"s3://{bucket}/sde/artifacts/"
    
    if not s3_input_path:
        s3_input_path = event.get('tenant_bucket')
    
    if not s3_input_path:
        raise ValueError("s3_input_path is required for ingestion mode")
    
    # Parse S3 path: s3://bucket/key
    if s3_input_path.startswith('s3://'):
        path = s3_input_path[5:]  # Remove 's3://'
    else:
        path = s3_input_path
    
    parts = path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    
    # If path ends with /, it's a prefix - need to sum all objects
    if key.endswith('/') or not key:
        return get_prefix_total_size(bucket, key)
    
    # Single file - get its size
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        size_bytes = response['ContentLength']
        logger.info(f"Ingestion file size: s3://{bucket}/{key} = {size_bytes} bytes")
        return size_bytes
    except Exception as e:
        logger.error(f"Error getting file size: {e}")
        # Default to medium size if we can't determine
        return 5 * 1024 * 1024 * 1024  # 5GB default


def get_prefix_total_size(bucket: str, prefix: str) -> int:
    """
    Sum the sizes of all objects under an S3 prefix.
    """
    s3 = get_s3_client()
    total_size = 0
    paginator = s3.get_paginator('list_objects_v2')
    
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                total_size += obj['Size']
        
        logger.info(f"Ingestion prefix total size: s3://{bucket}/{prefix} = {total_size} bytes")
        return total_size if total_size > 0 else 1024 * 1024  # 1MB minimum
    except Exception as e:
        logger.error(f"Error listing prefix: {e}")
        return 5 * 1024 * 1024 * 1024  # 5GB default
