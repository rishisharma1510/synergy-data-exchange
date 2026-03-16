# Data Extraction Service — Full Implementation Plan

## Architecture Summary

**Stack:** PyIceberg + PyArrow + pyodbc | SQL Server Express on Fargate | No Spark | No Glue Jobs  
**Orchestration:** Step Functions (1 execution per tenant) + Activity Management  
**Cost per run:** ~$0.05 (Fargate only, zero license fees)

---

## Scope: What This Plan Covers

- **On-demand extraction of Iceberg tables filtered by exposure ID(s)** — a tenant requests 1–10 exposure IDs, and the service extracts the relevant rows across ~20 tables
- **Automated SQL Server artifact generation** — data is loaded into an ephemeral SQL Server Express instance and packaged as `.bak` and/or `.mdf` files
- **Secure cross-account delivery** — artifacts are uploaded to the tenant's S3 bucket via STS AssumeRole with KMS encryption
- **Activity Management integration** — the service is triggered through the existing Activity Management system, with progress reporting and status callbacks throughout the pipeline
- **Fully serverless, pay-per-use infrastructure** — Fargate scales to zero between runs; no idle compute costs
- **Single-tenant isolation** — each extraction runs in its own ephemeral Fargate task with dedicated SQL Server instance; no shared state between tenants
- **Auto-discovery of tables and columns** — tables are discovered from the Glue Catalog, columns are auto-mapped via a 6-layer matching engine, with optional S3-based config overrides per tenant

## Future Roadmap (Out of Scope for This Plan)

- **SQL Server Standard edition support** — if data volumes exceed the Express 10 GB limit, upgrade to Standard ($0.235/hr license) with no architecture changes required
- **Pattern 2 fan-out scaling** — if sub-15-minute SLAs are mandated for large extractions, the architecture can be extended to parallel loader tasks (see Section 14)
- **Incremental / delta extractions** — currently full extraction per exposure; future support for extracting only changed data since last run using Iceberg snapshot diffing
- **Additional artifact formats** — beyond `.bak`/`.mdf`, potential support for `.bacpac` (Azure SQL), Parquet bundles, or CSV archives depending on tenant requirements
- **Multi-region deployment** — current plan targets a single AWS region; future expansion to additional regions for data residency compliance
- **Self-service tenant onboarding** — automated provisioning of tenant IAM roles, S3 buckets, and config via a management API or portal
- **Real-time progress UI** — exposing Activity Management progress callbacks to a tenant-facing dashboard for extraction status visibility
- **Table-level scheduling** — allowing tenants to configure which tables to include/exclude per extraction, or schedule recurring extractions on a cron basis

---

## 1. Repository Structure (Target State)

```
DataIngestion/
├── docs/
│   ├── sequence_diagram.md          # Mermaid sequence diagrams
│   ├── architecture_detailed.drawio # Full architecture (draw.io)
│   └── architecture_executive.drawio# Executive view (draw.io)
│
├── src/                              # Application code
│   ├── __init__.py
│   │
│   ├── core/                         # ← EXISTING CODE (move here)
│   │   ├── __init__.py
│   │   ├── config.py                 # IcebergConfig, SqlServerConfig (exists)
│   │   ├── iceberg_reader.py         # IcebergReader (exists — add row_filter)
│   │   ├── sql_writer.py             # SqlServerWriter (exists — 3 backends)
│   │   ├── column_mapper.py          # ColumnMapper (exists — 6-layer)
│   │   ├── mapping_config.py         # MappingConfig (exists — JSON overrides)
│   │   └── ingest.py                 # Core ingest logic (exists — add ThreadPool)
│   │
│   ├── fargate/                      # ← NEW CODE
│   │   ├── __init__.py
│   │   ├── entrypoint.py            # Fargate task main — receives SFN input
│   │   ├── database_manager.py      # CREATE/DROP DATABASE lifecycle
│   │   ├── artifact_generator.py    # BACKUP DATABASE / sp_detach_db
│   │   ├── s3_uploader.py           # Multipart upload + checksum verify
│   │   ├── progress_reporter.py     # Activity Management API callbacks
│   │   └── validation.py            # Row counts, DBCC, checksums
│   │
│   └── models/                       # ← NEW
│       ├── __init__.py
│       ├── execution_plan.py         # Pydantic models for plan/config
│       └── manifest.py              # Artifact manifest schema
│
├── infra/                            # ← NEW: Infrastructure as Code
│   ├── cdk/                          # or terraform/
│   │   ├── app.py
│   │   ├── stacks/
│   │   │   ├── ecs_stack.py         # ECS cluster, task def, security groups
│   │   │   ├── sfn_stack.py         # Step Function state machine
│   │   │   ├── iam_stack.py         # Roles: task role, execution role, tenant roles
│   │   │   ├── networking_stack.py  # VPC, private subnets, VPC endpoints
│   │   │   └── shared_stack.py      # Secrets Manager, S3 config bucket, SNS
│   │   └── config.py                # Environment-specific config
│   └── step_function/
│       └── state_machine.asl.json   # Step Function definition (ASL)
│
├── docker/
│   ├── Dockerfile.pipeline          # Python pipeline sidecar image
│   ├── docker-compose.local.yml     # Local dev: SQL Server + pipeline
│   └── .dockerignore
│
├── scripts/
│   ├── register_activity_def.py     # Register Activity Definition at deploy
│   ├── upload_mapping_overrides.py  # Upload tenant overrides to S3
│   └── local_test.py               # End-to-end local test
│
├── tests/
│   ├── unit/
│   │   ├── test_column_mapper.py     # (exists)
│   │   ├── test_artifact_generator.py
│   │   ├── test_s3_uploader.py
│   │   └── test_validation.py
│   ├── integration/
│   │   ├── test_full_ingestion.py    # (exists)
│   │   ├── test_fargate_entrypoint.py
│   │   └── test_end_to_end.py
│   └── conftest.py
│
├── .github/
│   └── workflows/
│       ├── ci.yml                    # Lint, test, type-check
│       └── cd.yml                    # Build, push ECR, deploy infra, register activity
│
├── .env                              # Local dev config (exists)
├── .env.example
├── pyproject.toml                    # or requirements.txt
├── Makefile                          # dev shortcuts
└── README.md
```

---

## 2. POC Foundations and New Modules

### POC Code (Proven Patterns — Productionize for Fargate)

The following modules were built during the proof-of-concept phase. The core logic is validated and working, but each needs hardening, refactoring, or extension to be production-ready in a Fargate environment.

| File | POC Status | Production Changes Needed | Effort |
|------|-----------|---------------------------|--------|
| `config.py` | Working locally | Add `ExtractionConfig` dataclass for Fargate-specific settings (activity callback URL, tenant S3 bucket, artifact type) | 30 min |
| `iceberg_reader.py` | Reads full tables from Iceberg | Add `row_filter` parameter to `stream_batches()` and `read_full()` for `exposure_id IN (...)` filtering | 1-2 hours |
| `sql_writer.py` | 3 write backends working | Wrap with error handling and retry logic for production use | 2 hours |
| `column_mapper.py` | 6-layer matching validated | Add logging and metrics for production observability | 1 hour |
| `mapping_config.py` | Loads from local JSON file | Add S3 config loading for runtime override support | 1 hour |
| `ingest.py` | Sequential CLI tool | Refactor into callable library; add `ThreadPoolExecutor` for parallel table loading | 4 hours |

### New Modules (Build from Scratch)

| File | Purpose | Effort |
|------|---------|--------|
| `entrypoint.py` | Main Fargate task entry — parses SFN input, orchestrates all phases | 1 day |
| `database_manager.py` | CREATE DATABASE, DROP DATABASE, health check | 2 hours |
| `artifact_generator.py` | BACKUP DATABASE → .bak, sp_detach_db → .mdf/.ldf, checksum generation | 1 day |
| `s3_uploader.py` | boto3 multipart upload with STS AssumeRole, checksum verification | 1 day |
| `progress_reporter.py` | HTTP client to call Activity Management API with progress updates | 3 hours |
| `validation.py` | Row count comparison, DBCC CHECKDB, PK check, HASHBYTES checksum | 4 hours |
| `execution_plan.py` | Pydantic models for execution plan, table config | 2 hours |
| `manifest.py` | Pydantic model for artifact manifest (S3 keys, checksums, row counts) | 1 hour |
| `Dockerfile.pipeline` | Multi-stage Docker build for Python pipeline sidecar | 3 hours |
| `state_machine.asl.json` | Step Function ASL definition | 1 day |
| CDK/Terraform stacks | ECS, SFN, IAM, VPC, Secrets Manager | 2-3 days |
| CI/CD pipeline | GitHub Actions: lint → test → build → push → deploy → register | 1 day |

---

## 3. Detailed Code Plan: Each New Module

### 3.1 `entrypoint.py` — Fargate Task Main

```python
"""
Receives input from Step Functions via environment variables or SSM parameter.
Orchestrates: init DB → load tables → validate → generate artifact → upload → report.
"""

def main():
    # 1. Parse input (from env var or file injected by SFN)
    input_payload = parse_input()  # {tenantId, exposureIds[], artifactType, activityId, callbackUrl}
    
    reporter = ProgressReporter(input_payload.callback_url, input_payload.activity_id)
    
    try:
        # 2. Report: STARTED
        reporter.report(10, "PROVISIONING")
        
        # 3. Create database
        db_mgr = DatabaseManager(sql_config)
        db_name = db_mgr.create_database(input_payload.tenant_id, input_payload.run_id)
        
        # 4. Load mapping overrides (optional — from S3)
        overrides = load_overrides_from_s3(input_payload.tenant_id)
        
        # 5. Discover tables + build plan
        reader = IcebergReader(iceberg_config)
        tables = reader.list_tables(tenant_glue_namespace)
        plan = build_execution_plan(tables, overrides, input_payload.exposure_ids)
        
        reporter.report(20, "LOADING")
        
        # 6. Parallel table load (ThreadPoolExecutor)
        results = parallel_load_tables(
            plan=plan,
            reader=reader,
            sql_config=sql_config,
            db_name=db_name,
            exposure_ids=input_payload.exposure_ids,
            max_workers=4,
            progress_callback=reporter.report_table_progress,
        )
        
        reporter.report(70, "VALIDATING")
        
        # 7. Validate
        validation_report = validate_database(sql_config, db_name, results)
        
        if not validation_report.passed:
            raise ValidationError(validation_report)
        
        reporter.report(80, "GENERATING_ARTIFACT")
        
        # 8. Generate artifact
        artifact_gen = ArtifactGenerator(sql_config)
        artifacts = artifact_gen.generate(
            db_name=db_name,
            artifact_type=input_payload.artifact_type,
            output_dir="/data/backup",
        )
        
        reporter.report(90, "UPLOADING")
        
        # 9. Upload to tenant S3
        uploader = S3Uploader(input_payload.tenant_id)
        upload_result = uploader.upload_artifacts(
            artifacts=artifacts,
            tenant_bucket=input_payload.tenant_bucket,
            prefix=f"artifacts/{input_payload.tenant_id}/{input_payload.activity_id}/",
        )
        
        # 10. Write manifest
        manifest = build_manifest(results, artifacts, upload_result, validation_report)
        uploader.upload_manifest(manifest)
        
        # 11. Report: COMPLETED
        reporter.report(100, "COMPLETED", manifest=manifest)
        
        # 12. Cleanup
        db_mgr.drop_database(db_name)
        
    except Exception as e:
        reporter.report(0, "FAILED", error=str(e))
        raise
```

### 3.2 `database_manager.py`

```python
class DatabaseManager:
    def create_database(self, tenant_id: str, run_id: str) -> str:
        """CREATE DATABASE tenant_{tenant_id}_{run_id_short}"""
    
    def drop_database(self, db_name: str) -> None:
        """DROP DATABASE (cleanup after artifact generation)"""
    
    def wait_for_ready(self, timeout_seconds: int = 120) -> bool:
        """Poll SELECT 1 until SQL Server is responsive"""
    
    def create_tables_from_plan(self, db_name: str, plan: ExecutionPlan) -> None:
        """CREATE TABLE for each table in the plan (using Arrow → SQL type mapping)"""
```

### 3.3 `artifact_generator.py`

```python
class ArtifactGenerator:
    def generate_bak(self, db_name: str, output_path: str) -> ArtifactFile:
        """
        BACKUP DATABASE [{db_name}]
        TO DISK = '{output_path}/{db_name}.bak'
        -- Note: WITH COMPRESSION not available on Express
        """
    
    def generate_mdf(self, db_name: str, output_dir: str) -> list[ArtifactFile]:
        """
        ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
        EXEC sp_detach_db '{db_name}'
        -- Returns: [.mdf path, .ldf path]
        """
    
    def generate(self, db_name: str, artifact_type: str, output_dir: str) -> list[ArtifactFile]:
        """Route to bak, mdf, or both based on artifact_type"""
    
    @staticmethod
    def compute_checksum(file_path: str, algorithm: str = "sha256") -> str:
        """Stream-hash the file without loading into memory"""
```

### 3.4 `s3_uploader.py`

```python
class S3Uploader:
    def __init__(self, tenant_id: str):
        """STS AssumeRole into tenant-specific S3 write role"""
    
    def upload_artifacts(self, artifacts: list[ArtifactFile], ...) -> UploadResult:
        """
        For each artifact:
          - CreateMultipartUpload
          - UploadPart (100 MB chunks)
          - CompleteMultipartUpload
          - Verify ETag vs local checksum
        """
    
    def upload_manifest(self, manifest: Manifest) -> str:
        """PUT manifest.json to tenant S3"""
```

### 3.5 `progress_reporter.py`

```python
class ProgressReporter:
    def __init__(self, callback_url: str, activity_id: str):
        """HTTP client to Activity Management API"""
    
    def report(self, progress_pct: int, status: str, **kwargs) -> None:
        """
        POST {callback_url}/activities/{activity_id}/progress
        {progress: 70, status: "LOADING", details: {...}}
        """
    
    def report_table_progress(self, table_name: str, rows_loaded: int, total_tables: int, tables_done: int) -> None:
        """Fine-grained progress during parallel table load"""
```

### 3.6 `validation.py`

```python
class Validator:
    def validate_database(self, db_name: str, load_results: dict) -> ValidationReport:
        """
        1. Row count: SELECT COUNT(*) from each table, compare to source
        2. DBCC CHECKDB('{db_name}') — integrity check
        3. PK/UNIQUE constraint checks
        4. HASHBYTES('SHA2_256', ...) for sample rows
        """
```

---

## 4. Changes to Existing Code

### 4.1 `iceberg_reader.py` — Add Exposure ID Filter

```python
# ADD to stream_batches():
def stream_batches(
    self,
    mode: str = "full",
    from_snapshot_id: Optional[int] = None,
    limit: Optional[int] = None,
    selected_columns: Optional[list[str]] = None,
    row_filter: Optional[str] = None,         # ← NEW PARAMETER
) -> Iterator[pa.RecordBatch]:
    
    # In the scan builder:
    scan_kwargs: dict = {}
    if row_filter:
        scan_kwargs["row_filter"] = row_filter  # ← PyIceberg predicate pushdown
        logger.info("Row filter applied: %s", row_filter)
    # ... rest unchanged
```

Usage:
```python
# Filter by exposure IDs:
batches = reader.stream_batches(
    row_filter="exposure_id IN (101, 205, 309)",
    selected_columns=["exposure_id", "contract_name", "premium"],
)
```

### 4.2 `ingest.py` — Add ThreadPoolExecutor

```python
# ADD parallel table loading function:
from concurrent.futures import ThreadPoolExecutor, as_completed

def parallel_load_tables(
    plan: ExecutionPlan,
    reader: IcebergReader,
    sql_config: SqlServerConfig,
    db_name: str,
    exposure_ids: list[int],
    max_workers: int = 4,
    progress_callback=None,
) -> dict[str, TableResult]:
    """Load N tables in parallel using ThreadPoolExecutor."""
    
    results = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for table_plan in plan.tables:
            future = pool.submit(
                _load_single_table,
                table_plan=table_plan,
                reader=reader,
                sql_config=sql_config,
                db_name=db_name,
                exposure_ids=exposure_ids,
            )
            futures[future] = table_plan.name
        
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                result = future.result()
                results[table_name] = result
                if progress_callback:
                    progress_callback(table_name, result.rows, len(plan.tables), len(results))
            except Exception as e:
                results[table_name] = TableResult(name=table_name, status="FAILED", error=str(e))
    
    return results
```

### 4.3 `mapping_config.py` — Add S3 Loading

```python
# ADD: load overrides from S3 (in addition to existing local file loading)
import boto3

def load_overrides_from_s3(tenant_id: str, config_bucket: str) -> dict:
    """Load mapping_overrides.json from S3 config bucket. Returns {} if not found."""
    s3 = boto3.client("s3")
    key = f"tenants/{tenant_id}/mapping_overrides.json"
    try:
        obj = s3.get_object(Bucket=config_bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        logger.info("No mapping overrides for tenant %s (using auto-discovery)", tenant_id)
        return {}
```

---

## 5. Docker Setup

### `Dockerfile.pipeline`

```dockerfile
# Stage 1: Build
FROM python:3.12-slim AS builder
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Stage 2: Runtime
FROM python:3.12-slim
WORKDIR /app

# SQL Server ODBC driver
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg2 unixodbc-dev && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
COPY src/ ./src/

ENV PYTHONPATH=/app
ENTRYPOINT ["python", "-m", "src.fargate.entrypoint"]
```

### `docker-compose.local.yml` (for local dev)

```yaml
version: '3.8'
services:
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_PID: "Express"
      SA_PASSWORD: "YourStrong!Passw0rd"
    ports:
      - "1433:1433"
    volumes:
      - sql-data:/var/opt/mssql
  
  pipeline:
    build:
      context: .
      dockerfile: docker/Dockerfile.pipeline
    depends_on:
      - sqlserver
    environment:
      SQLSERVER_HOST: sqlserver
      SQLSERVER_PORT: 1433
      SQLSERVER_USERNAME: sa
      SQLSERVER_PASSWORD: "YourStrong!Passw0rd"
      # ... iceberg config ...
    volumes:
      - ./src:/app/src  # hot reload during dev

volumes:
  sql-data:
```

---

## 6. ECS Fargate Task Definition

```json
{
  "family": "iceberg-extraction",
  "cpu": "4096",
  "memory": "8192",
  "ephemeralStorage": { "sizeInGiB": 200 },
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "executionRoleArn": "arn:aws:iam::...:role/extraction-execution-role",
  "taskRoleArn": "arn:aws:iam::...:role/extraction-task-role",
  "containerDefinitions": [
    {
      "name": "sqlserver",
      "image": "mcr.microsoft.com/mssql/server:2022-latest",
      "essential": true,
      "cpu": 2048,
      "memory": 4096,
      "environment": [
        { "name": "ACCEPT_EULA", "value": "Y" },
        { "name": "MSSQL_PID", "value": "Express" }
      ],
      "secrets": [
        { "name": "SA_PASSWORD", "valueFrom": "arn:aws:secretsmanager:...:sql-sa-password" }
      ],
      "portMappings": [{ "containerPort": 1433, "protocol": "tcp" }],
      "mountPoints": [
        { "sourceVolume": "shared-data", "containerPath": "/shared" }
      ],
      "healthCheck": {
        "command": ["CMD-SHELL", "/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $SA_PASSWORD -C -Q 'SELECT 1' || exit 1"],
        "interval": 10,
        "timeout": 5,
        "retries": 12,
        "startPeriod": 60
      },
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/extraction/sqlserver",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "sql"
        }
      }
    },
    {
      "name": "pipeline",
      "image": "{account}.dkr.ecr.{region}.amazonaws.com/extraction-pipeline:latest",
      "essential": true,
      "cpu": 2048,
      "memory": 4096,
      "dependsOn": [
        { "containerName": "sqlserver", "condition": "HEALTHY" }
      ],
      "environment": [
        { "name": "SQLSERVER_HOST", "value": "localhost" },
        { "name": "SQLSERVER_PORT", "value": "1433" },
        { "name": "SQLSERVER_AUTH_TYPE", "value": "sql_login" },
        { "name": "SQLSERVER_USERNAME", "value": "sa" },
        { "name": "SQLSERVER_DRIVER", "value": "ODBC Driver 18 for SQL Server" },
        { "name": "WRITE_BACKEND", "value": "pyodbc" },
        { "name": "BATCH_SIZE", "value": "50000" }
      ],
      "secrets": [
        { "name": "SQLSERVER_PASSWORD", "valueFrom": "arn:aws:secretsmanager:...:sql-sa-password" },
        { "name": "EXTRACTION_INPUT", "valueFrom": "INJECTED_BY_SFN" }
      ],
      "mountPoints": [
        { "sourceVolume": "shared-data", "containerPath": "/shared" }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/extraction/pipeline",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "py"
        }
      }
    }
  ],
  "volumes": [
    {
      "name": "shared-data"
    }
  ]
}
```

---

## 7. Step Function State Machine (ASL)

```json
{
  "Comment": "Iceberg Extraction Pipeline — 1 execution per tenant",
  "StartAt": "RunFargateTask",
  "TimeoutSeconds": 7200,
  "States": {
    "RunFargateTask": {
      "Type": "Task",
      "Resource": "arn:aws:states:::ecs:runTask.sync",
      "Parameters": {
        "LaunchType": "FARGATE",
        "Cluster": "${EcsClusterArn}",
        "TaskDefinition": "${TaskDefinitionArn}",
        "NetworkConfiguration": {
          "AwsvpcConfiguration": {
            "Subnets": ["${PrivateSubnet1}", "${PrivateSubnet2}"],
            "SecurityGroups": ["${SecurityGroupId}"],
            "AssignPublicIp": "DISABLED"
          }
        },
        "Overrides": {
          "ContainerOverrides": [
            {
              "Name": "pipeline",
              "Environment": [
                { "Name": "TENANT_ID", "Value.$": "$.tenantId" },
                { "Name": "EXPOSURE_IDS", "Value.$": "States.JsonToString($.exposureIds)" },
                { "Name": "ARTIFACT_TYPE", "Value.$": "$.artifactType" },
                { "Name": "ACTIVITY_ID", "Value.$": "$.activityId" },
                { "Name": "CALLBACK_URL", "Value.$": "$.callbackUrl" },
                { "Name": "TENANT_BUCKET", "Value.$": "$.tenantBucket" },
                { "Name": "TENANT_GLUE_DB", "Value.$": "$.tenantGlueDb" },
                { "Name": "CONFIG_BUCKET", "Value": "${ConfigBucket}" }
              ]
            }
          ]
        }
      },
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "NotifyFailure"
        }
      ],
      "Next": "NotifySuccess"
    },
    "NotifySuccess": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${SnsTopicArn}",
        "Subject": "Extraction Complete",
        "Message": {
          "tenantId.$": "$.tenantId",
          "activityId.$": "$.activityId",
          "status": "COMPLETED"
        }
      },
      "End": true
    },
    "NotifyFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${SnsTopicArn}",
        "Subject": "Extraction Failed",
        "Message": {
          "tenantId.$": "$.tenantId",
          "activityId.$": "$.activityId",
          "status": "FAILED",
          "error.$": "$.Error",
          "cause.$": "$.Cause"
        }
      },
      "End": true
    }
  }
}
```

Note: The Step Function is intentionally simple — it just runs the Fargate task and handles success/failure notification. All the complex orchestration (load, validate, artifact, upload) happens **inside** the Fargate task's Python entrypoint, which reports progress back to Activity Management via HTTP callbacks.

---

## 8. IAM Roles

### Task Execution Role (ECS pulls image, writes logs)
```yaml
Policies:
  - ecr:GetDownloadUrlForLayer, ecr:BatchGetImage
  - logs:CreateLogStream, logs:PutLogEvents
  - secretsmanager:GetSecretValue (sql-sa-password)
```

### Task Role (what the containers can do at runtime)
```yaml
Policies:
  # Read Iceberg data
  - glue:GetDatabase, glue:GetTable, glue:GetTables
  - s3:GetObject, s3:ListBucket (on tenant Iceberg data bucket)
  
  # Read config
  - s3:GetObject (on config bucket: mapping_overrides.json)
  
  # Assume tenant role for S3 upload
  - sts:AssumeRole (on arn:aws:iam::*:role/extraction-tenant-upload-*)
  
  # CloudWatch metrics
  - cloudwatch:PutMetricData
```

### Tenant S3 Upload Role (assumed via STS)
```yaml
# One per tenant (or parameterized)
Trust: extraction-task-role
Policies:
  - s3:PutObject, s3:CreateMultipartUpload, s3:UploadPart,
    s3:CompleteMultipartUpload, s3:AbortMultipartUpload
    Resource: arn:aws:s3:::{tenantBucket}/artifacts/*
  - kms:GenerateDataKey (for SSE-KMS)
```

### Step Function Role
```yaml
Policies:
  - ecs:RunTask, ecs:StopTask, ecs:DescribeTasks
  - iam:PassRole (task role + execution role)
  - sns:Publish
```

---

## 9. CI/CD Pipeline

### GitHub Actions: `ci.yml`

```yaml
name: CI
on: [push, pull_request]

jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      
      - name: Install dependencies
        run: pip install -e ".[dev]"
      
      - name: Lint (ruff)
        run: ruff check src/ tests/
      
      - name: Type check (mypy)
        run: mypy src/
      
      - name: Unit tests
        run: pytest tests/unit/ -v --cov=src
      
      - name: Integration tests (with local SQL Server)
        services:
          sqlserver:
            image: mcr.microsoft.com/mssql/server:2022-latest
            env:
              ACCEPT_EULA: Y
              MSSQL_PID: Express
              SA_PASSWORD: TestPass!123
            ports:
              - 1433:1433
        run: pytest tests/integration/ -v
```

### GitHub Actions: `cd.yml`

```yaml
name: CD
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    permissions:
      id-token: write   # OIDC for AWS
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::...:role/github-deploy
          aws-region: us-east-1
      
      - name: Login to ECR
        uses: aws-actions/amazon-ecr-login@v2
      
      - name: Build and push Docker image
        run: |
          docker build -f docker/Dockerfile.pipeline -t extraction-pipeline:${{ github.sha }} .
          docker tag extraction-pipeline:${{ github.sha }} $ECR_REGISTRY/extraction-pipeline:latest
          docker tag extraction-pipeline:${{ github.sha }} $ECR_REGISTRY/extraction-pipeline:${{ github.sha }}
          docker push $ECR_REGISTRY/extraction-pipeline:latest
          docker push $ECR_REGISTRY/extraction-pipeline:${{ github.sha }}
      
      - name: Deploy infrastructure (CDK)
        run: |
          cd infra/cdk
          pip install -r requirements.txt
          cdk deploy --all --require-approval never
      
      - name: Register Activity Definition
        run: |
          python scripts/register_activity_def.py \
            --name IcebergExtraction \
            --sfn-arn ${{ steps.cdk.outputs.StepFunctionArn }} \
            --version ${{ github.sha }}
      
      - name: Upload global mapping overrides
        run: |
          aws s3 sync ./config/overrides/ s3://$CONFIG_BUCKET/global/ --delete
```

---

## 10. Implementation Plan (3 Weeks)

**Team:** 3 Engineers + Architect/Manager (oversight, code reviews, design decisions)

### Week 1: Core Modules + Infrastructure Foundation

| Day | Engineer 1 (Backend) | Engineer 2 (Infra) | Engineer 3 (Pipeline) | Architect/Manager |
|-----|---------------------|--------------------|-----------------------|-------------------|
| **Mon** | Restructure repo (move to src/) | VPC + subnets + VPC endpoints (CDK) | `iceberg_reader.py`: add `row_filter` | Kick-off, assign tasks, review repo structure |
| **Tue** | `database_manager.py` | ECS cluster + task definition (CDK) | `ingest.py`: add ThreadPoolExecutor | Review PRs, unblock design questions |
| **Wed** | `artifact_generator.py` | IAM roles (task, exec, tenant) | `mapping_config.py`: S3 loading | Review IAM policies, security sign-off |
| **Thu** | `validation.py` | Step Function ASL + CDK stack | Dockerfile.pipeline + local compose | Review Step Function flow, test plan |
| **Fri** | `s3_uploader.py` | Secrets Manager + S3 config bucket | Unit tests for all new modules | Week 1 retrospective, adjust plan |

### Week 2: Integration + Wiring + CI/CD

| Day | Engineer 1 (Backend) | Engineer 2 (Infra) | Engineer 3 (Pipeline) | Architect/Manager |
|-----|---------------------|--------------------|-----------------------|-------------------|
| **Mon** | `progress_reporter.py` | CI pipeline (GitHub Actions) | `entrypoint.py` (wire all modules) | Review entrypoint design, integration strategy |
| **Tue** | `execution_plan.py` + `manifest.py` | CD pipeline (ECR push + CDK deploy) | docker-compose end-to-end test | Review CI/CD pipeline, approve deployment gates |
| **Wed** | Integration test: full pipeline locally | Deploy to dev environment | `register_activity_def.py` script | Validate dev deployment, smoke test |
| **Thu** | Fix bugs from integration testing | Cross-account IAM + tenant S3 access | End-to-end test with real Iceberg data | Review integration test results |
| **Fri** | Performance profiling (identify bottlenecks) | Monitoring: CloudWatch dashboards + alarms | Activity Management integration test | Week 2 retrospective, prioritise Week 3 |

### Week 3: Hardening + Staging + Production Readiness

| Day | Engineer 1 (Backend) | Engineer 2 (Infra) | Engineer 3 (Pipeline) | Architect/Manager |
|-----|---------------------|--------------------|-----------------------|-------------------|
| **Mon** | Performance tuning (batch sizes, thread count) | Deploy to staging | Error handling + retry edge cases | Review staging deployment plan |
| **Tue** | Load test: 8 GB / 20 tables / 10 exposures | Staging environment validation | Logging + structured log format | Validate load test results against SLAs |
| **Wed** | Fix issues from load test | Production CDK stack + IAM review | Documentation + runbook | Security review, production readiness checklist |
| **Thu** | Final integration test on staging | Production deployment (with feature flag) | End-to-end production smoke test | Go/no-go decision, stakeholder demo |
| **Fri** | Buffer / overflow tasks | Production monitoring + alerting tuning | Knowledge transfer + handoff docs | Final sign-off, close out |

---

## 11. Testing Strategy

### Unit Tests (run in CI, no external deps)

| Test | What |
|------|------|
| `test_column_mapper.py` | (exists) — all 6 mapping layers |
| `test_artifact_generator.py` | Mock pyodbc — test BACKUP/DETACH SQL generation |
| `test_s3_uploader.py` | Mock boto3 — test multipart upload logic, checksum verify |
| `test_validation.py` | Mock pyodbc — test row count comparison, DBCC parsing |
| `test_execution_plan.py` | Test plan building with/without overrides |
| `test_progress_reporter.py` | Mock HTTP — test callback formatting |

### Integration Tests (run in CI with SQL Server container)

| Test | What |
|------|------|
| `test_full_extraction.py` | (exists) — end-to-end with local Iceberg + SQL Server |
| `test_database_manager.py` | CREATE/DROP database lifecycle |
| `test_artifact_real.py` | Generate real .bak, verify it's valid |
| `test_parallel_load.py` | Load 5 tables in parallel, verify all data |

### End-to-End Test (staging environment)

| Test | What |
|------|------|
| `test_e2e_staging.py` | Trigger via Activity Management → SFN → Fargate → verify .bak in S3 |

---

## 12. Configuration / Environment Variables

### Fargate Task (injected by Step Function)

| Variable | Source | Example |
|----------|--------|---------|
| `TENANT_ID` | SFN input | `acme-corp` |
| `EXPOSURE_IDS` | SFN input | `[101, 205, 309]` |
| `ARTIFACT_TYPE` | SFN input | `bak` / `mdf` / `both` |
| `ACTIVITY_ID` | SFN input | `uuid` |
| `CALLBACK_URL` | SFN input | `https://activity-mgmt.internal/api` |
| `TENANT_BUCKET` | SFN input | `acme-corp-artifacts` |
| `TENANT_GLUE_DB` | SFN input | `acme_iceberg_db` |
| `CONFIG_BUCKET` | Task def env | `extraction-config-bucket` |
| `SQLSERVER_HOST` | Task def env | `localhost` |
| `SQLSERVER_PORT` | Task def env | `1433` |
| `SQLSERVER_USERNAME` | Task def env | `sa` |
| `SQLSERVER_PASSWORD` | Secrets Manager | `****` |
| `WRITE_BACKEND` | Task def env | `pyodbc` |
| `BATCH_SIZE` | Task def env | `50000` |
| `NAMING_CONVENTION` | Task def env | `PascalCase` |
| `LOG_LEVEL` | Task def env | `INFO` |

---

## 13. Monitoring + Observability

| What | Where | Alert |
|------|-------|-------|
| Step Function execution status | CloudWatch Metrics | SNS on FAILED |
| Fargate task CPU/memory | CloudWatch Container Insights | >90% memory |
| Python pipeline logs | CloudWatch Logs `/ecs/extraction/pipeline` | ERROR patterns |
| SQL Server logs | CloudWatch Logs `/ecs/extraction/sqlserver` | Error severity |
| S3 upload progress | Custom CloudWatch metrics | Upload failure |
| Activity progress | Activity Management | Stuck >30 min |
| Cost per execution | AWS Cost Explorer (ECS tag) | Monthly budget |

---

## 14. Scaling Patterns & Cost Analysis

### Parameters (Our Use Case)

- **~20 tables** per extraction
- **1–10 exposure IDs** per run
- **Up to 8 GB** max data size (2 GB headroom under Express 10 GB limit)
- SQL Server Express on Fargate: 4 vCPU / 8 GB RAM / 200 GB ephemeral

### Pattern Definitions

#### Pattern 1 — Single Task, Multi-Thread (CHOSEN)

```
┌───────────────────────────────────────────┐
│            Fargate Task                    │
│  ┌────────────┐  ┌──────────────────────┐ │
│  │ SQL Server  │← │ Python (4 threads)   │ │
│  │  Express    │  │  Thread 1 → Table A  │ │
│  │             │  │  Thread 2 → Table B  │ │
│  │  localhost  │  │  Thread 3 → Table C  │ │
│  │    :1433    │  │  Thread 4 → Table D  │ │
│  └────────────┘  └──────────────────────┘ │
└───────────────────────────────────────────┘
```

One Fargate task runs both SQL Server and Python. Python uses `ThreadPoolExecutor(max_workers=4)` to load tables in parallel. Reads from Iceberg and writes to SQL Server over localhost. Zero network overhead.

#### Pattern 2 — Fan-Out Loaders → Central SQL Server

```
Step Function
  ├─→ Start SQL Server Task (stays alive, 10.0.1.47:1433)
  ├─→ Map State (fan-out)
  │     ├─ Loader Task 1 → Tables A–E  → INSERT into 10.0.1.47
  │     ├─ Loader Task 2 → Tables F–J  → INSERT into 10.0.1.47
  │     ├─ Loader Task 3 → Tables K–O  → INSERT into 10.0.1.47
  │     └─ Loader Task 4 → Tables P–T  → INSERT into 10.0.1.47
  ├─→ All loaders done
  └─→ SQL Server Task → generate .bak → upload → stop
```

A dedicated SQL Server task (4 vCPU / 8 GB) runs separately. N lightweight Python-only loader tasks (2 vCPU / 4 GB each) read from Iceberg and write to the SQL Server over VPC private IP. More read parallelism but adds network latency on writes.

### Fargate Pricing (us-east-1)

| Resource | Rate |
|----------|------|
| vCPU | $0.04048 / vCPU / hour |
| Memory | $0.004445 / GB / hour |
| Ephemeral Storage (above 20 GB free) | $0.000111 / GB / hour |

### Task Hourly Rates

| Task Type | vCPU | RAM | Ephemeral | Cost/Hour |
|-----------|------|-----|-----------|-----------|
| Pattern 1 — Combined (SQL + Python) | 4 | 8 GB | 200 GB | **$0.217/hr** |
| Pattern 2 — SQL Server task | 4 | 8 GB | 200 GB | $0.217/hr |
| Pattern 2 — Each loader task | 2 | 4 GB | 20 GB (default) | $0.099/hr |

### Scenario A: 1 Exposure / ~2 GB / 20 Tables (Small Run)

| Phase | Pattern 1 | Pattern 2 |
|-------|-----------|-----------|
| Startup + init | 2 min | 3 min (SQL task + wait for IP) |
| Read + Write (20 tables) | 6 min (4 threads, localhost) | 4 min (4 loaders, network write) |
| Validate + artifact + upload | 4 min | 4 min |
| **Total time** | **~12 min** | **~11 min** |

| Cost Component | Pattern 1 | Pattern 2 |
|----------------|-----------|-----------|
| SQL/Combined task | 12 min × $0.217/hr = $0.043 | 11 min × $0.217/hr = $0.040 |
| Loader tasks | — | 4 × 4 min × $0.099/hr = $0.026 |
| **Total per run** | **$0.043** | **$0.066** |

---

### Scenario B: 5 Exposures / ~4 GB / 20 Tables (Typical Run)

| Phase | Pattern 1 | Pattern 2 |
|-------|-----------|-----------|
| Startup + init | 2 min | 3 min |
| Read + Write (20 tables) | 12 min (4 threads, localhost) | 7 min (4 loaders, network write) |
| Validate + artifact + upload | 5 min | 5 min |
| **Total time** | **~19 min** | **~15 min** |

| Cost Component | Pattern 1 | Pattern 2 |
|----------------|-----------|-----------|
| SQL/Combined task | 19 min × $0.217/hr = $0.069 | 15 min × $0.217/hr = $0.054 |
| Loader tasks | — | 4 × 7 min × $0.099/hr = $0.046 |
| **Total per run** | **$0.069** | **$0.100** |

---

### Scenario C: 10 Exposures / ~8 GB / 20 Tables (Max Run)

| Phase | Pattern 1 | Pattern 2 |
|-------|-----------|-----------|
| Startup + init | 2 min | 3 min |
| Read + Write (20 tables) | 22 min (4 threads, localhost) | 12 min (4 loaders, network write) |
| Validate + artifact + upload | 8 min | 8 min |
| **Total time** | **~32 min** | **~23 min** |

| Cost Component | Pattern 1 | Pattern 2 |
|----------------|-----------|-----------|
| SQL/Combined task | 32 min × $0.217/hr = $0.116 | 23 min × $0.217/hr = $0.083 |
| Loader tasks | — | 4 × 12 min × $0.099/hr = $0.079 |
| **Total per run** | **$0.116** | **$0.162** |

---

### Summary: All Scenarios

| Scenario | P1 Time | P1 Cost | P2 Time | P2 Cost | P2 Premium |
|----------|---------|---------|---------|---------|------------|
| **A** — 1 exp / 2 GB | 12 min | **$0.043** | 11 min | $0.066 | +53% cost for 1 min saved |
| **B** — 5 exp / 4 GB | 19 min | **$0.069** | 15 min | $0.100 | +45% cost for 4 min saved |
| **C** — 10 exp / 8 GB | 32 min | **$0.116** | 23 min | $0.162 | +40% cost for 9 min saved |

### Monthly Projections

| Volume | Pattern 1 | Pattern 2 |
|--------|-----------|-----------|
| 100 runs/month (avg ~4 GB) | **$6.90** | $10.00 |
| 500 runs/month (avg ~4 GB) | **$34.50** | $50.00 |

### Why Pattern 2 Costs More but Doesn't Help Much

The bottleneck in this pipeline is **SQL Server Express writes**, not Iceberg reads. Express is capped at 4 cores regardless. Fan-out (Pattern 2) parallelises the read phase but:

1. **Writes still hit the same 4-core SQL Server** — no faster
2. **Network latency added** — loaders write over VPC instead of localhost
3. **Extra Fargate tasks cost money** — 4 loaders @ $0.099/hr each
4. **More infrastructure complexity** — separate task definitions, Map state in SFN, Security Groups for SQL port

### Decision: Pattern 1

Pattern 1 handles the full range (1–10 exposures, 20 tables, up to 8 GB) with 2 GB headroom under the Express limit. Worst case is ~32 min at 8 GB, which is acceptable for a batch extraction job. Pattern 2 is only worth considering if requirements change to >10 GB (requiring SQL Standard) or if sub-15-min SLA is mandated.

**Upgrade path to Pattern 2 is clean** — extract loader logic into standalone module, change `SQLSERVER_HOST` from `localhost` to a passed-in IP, add a Map state to Step Function.

---

## 15. Key Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Compute for data pipeline | **PyIceberg + PyArrow (not Spark)** | POC-proven, faster cold start, cheaper, localhost writes |
| Compute for SQL Server | **Fargate (not EC2)** | Serverless, scale to zero, no patching |
| SQL Server edition | **Express** | Free, sufficient for <10 GB per run |
| SQL Server storage | **Ephemeral (not EFS)** | Transient workload, auto-cleanup, better I/O |
| Table parallelism | **ThreadPoolExecutor(4)** | Matches Express's 4 cores, no extra service |
| Scaling pattern | **Pattern 1 — Single Task** | Cheapest (40% less than fan-out), handles up to 8 GB, ~32 min worst case |
| State management | **Activity Management (not DynamoDB)** | Already exists in your ecosystem |
| Table discovery | **Auto from Glue Catalog (not manual config)** | Zero config by default |
| Column mapping | **Auto from Arrow schema (with optional S3 overrides)** | POC-proven, 6-layer matching |
| Orchestration | **Step Functions → single Fargate task** | Simple, one service does everything |
| Write backend | **pyodbc + TABLOCK (default)** | Best balance of speed and reliability |
| CI/CD | **GitHub Actions → ECR → CDK** | Standard, auditable |
