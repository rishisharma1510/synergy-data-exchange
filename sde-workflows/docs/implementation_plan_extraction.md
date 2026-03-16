# Extraction Pipeline — Implementation Plan

> **Flow:** Iceberg Data Lake → SQL Server (ephemeral) → MDF/BAK artifact → Tenant S3 bucket

## Overview

The extraction pipeline reads exposure data from our Iceberg/Glue data lake, loads it into an ephemeral SQL Server instance inside a container, generates a `.mdf` or `.bak` artifact, and uploads it to the requesting tenant's S3 bucket.

| Activity Type | Flow | Trigger |
|---------------|------|---------|
| **Extraction** | Iceberg → SQL Server → MDF → Tenant S3 | Tenant submits extraction request via Activity Management |

Both a **Step Functions state machine** and a **Fargate/Batch task** are dedicated to extraction. The ingestion pipeline runs on a completely separate state machine and task — see [implementation_plan_ingestion.md](implementation_plan_ingestion.md).

---

## Implementation Status (Updated 2026-03-10)

| Component | Status | Tests | Notes |
|-----------|--------|-------|-------|
| **Extraction Entrypoint** | ✅ 100% | — | BAK/MDF generation + upload + Tenant Context |
| **Iceberg Reader** | ✅ 100% | — | Row-filter pushdown, exposure_id predicate |
| **SQL Writer** | ✅ 100% | — | 3 backends (pyodbc/sqlalchemy/bulk) |
| **Artifact Generator** | ✅ 100% | — | BACKUP DATABASE / sp_detach_db + checksum |
| **S3 Uploader** | ✅ 100% | — | Multipart upload + ETag verify + STS assume |
| **Progress Reporter** | ✅ 100% | — | S3 writes (always) + HTTP callback (optional) |
| **Check Records Lambda** | ✅ 100% | — | Early-exit, skips ECS if no matching rows |
| **Tenant Context Resolution** | ✅ 100% | — | Lambda lookup for glue_database, bucket, creds |
| **Structured JSON Logging** | ✅ 100% | — | _JsonFormatter + _ContextFilter |
| **Validation Framework** | ⚠️ 25% | — | Basic row-count + DBCC; Lambda-based check missing |
| **Schema Version Detection** | ❌ 0% | — | Blocked on Client schema markers |
| **Infrastructure (CDK)** | ✅ 100% | — | 4 stacks: ECR, Fargate, Batch, Step Functions |

**Overall:** ~90% complete

See [SOW_deliverables_tracker.md](SOW_deliverables_tracker.md) for full detail.

---

## System Architecture

### Three-Path Design

The system uses **size-based routing** with configurable thresholds. The caller (Activity Management) computes `estimated_size_gb` from Iceberg metadata and supplies it in the Step Functions input — the SFN then routes automatically:

| Path | Data Size | Infrastructure | SQL Edition | Cost/Run | License |
|------|-----------|----------------|-------------|----------|---------|
| **PATH 1** | ≤9 GB | ECS Fargate | Express | ~$0.30-0.40 | FREE |
| **PATH 2** | 9-80 GB | ECS Fargate | Standard | ~$1.20-1.50 | SPLA |
| **PATH 3** | >80 GB | AWS Batch EC2 | Standard | ~$1.40-1.80 | SPLA |

Default thresholds: `DEFAULT_THRESHOLD_SMALL_GB = 9`, `DEFAULT_THRESHOLD_LARGE_GB = 80` (in `infra/step_functions.py`). Configurable via CDK: `ThresholdSmallGb` / `ThresholdLargeGb`.

---

### Size Estimation — Caller-Supplied

For extraction, **the caller must include `estimated_size_gb`** in the SFN `StartExecution` input. This is computed from Iceberg metadata before the SFN is started:

```
Activity Management:
  1. Query Iceberg metadata (Glue Data Catalog):
       SELECT sum(file_size_in_bytes) FROM <table>.files
       WHERE partition matches exposure_ids
  2. estimated_size_gb = sum(parquet_file_sizes) / 1_000_000_000
  3. Include in Step Functions StartExecution input:
       { "estimated_size_gb": 12.4, "exposure_ids": [101, 205], ... }
```

**The Step Functions `RouteBySize` Choice state** evaluates `$.estimated_size_gb` as a numeric comparison:

```
Extraction SFN flow:
  CheckRecords (Lambda)
      → EvaluateRecordCount (Choice)
          → [no records]  → NoRecordsFound (Succeed, no task launched)
          → [has records] → RouteBySize (Choice on $.estimated_size_gb numeric)
              → ≤ 9 GB   → RunFargateExpress  (MSSQL_PID=Express)
              → ≤ 80 GB  → RunFargateStandard (MSSQL_PID=Standard)
              → > 80 GB  → RunBatchEC2        (MSSQL_PID=Standard)
```

Step Function routing pseudocode:
```python
# Choice state evaluates $.estimated_size_gb (numeric)
if estimated_size_gb <= threshold_small_gb:      # Default: 9 GB
    route = "PATH 1: Fargate + Express (FREE)"
    MSSQL_PID = "Express"
elif estimated_size_gb <= threshold_large_gb:    # Default: 80 GB
    route = "PATH 2: Fargate + Standard (SPLA Licensed)"
    MSSQL_PID = "Standard"
else:
    route = "PATH 3: Batch EC2 + Standard (SPLA Licensed)"
    MSSQL_PID = "Standard"
```

---

### Architecture Diagram

```
┌─────────────────┐
│  Tenant UI/API  │
└────────┬────────┘
         │ POST /activities  (activity_type: "extraction")
         ▼
┌─────────────────────────────────────────────────────┐
│               ACTIVITY MANAGEMENT                    │
│  1. Compute estimated_size_gb from Iceberg metadata  │
│  2. StartExecution on Extraction State Machine       │
└───────────────────────┬─────────────────────────────┘
                        │ StartExecution { estimated_size_gb, exposure_ids, ... }
                        ▼
┌─────────────────────────────────────────────────────┐
│           EXTRACTION STEP FUNCTIONS                  │
│  CheckRecords (Lambda) → EvaluateRecordCount         │
│  → RouteBySize (Choice on $.estimated_size_gb)       │
│       ≤9GB       9-80GB        >80GB                 │
│        ▼           ▼             ▼                   │
│  Fargate/Express  Fargate/Std  Batch EC2/Std         │
└─────────────────────────────────────────────────────┘
         ▼                ▼               ▼
  ┌─────────────────────────────────────────────────┐
  │  EXTRACTION TASK (all paths use same code)       │
  │  1. Read from Iceberg (network → Glue/S3)        │
  │  2. Write to SQL Server (localhost)               │
  │  3. DETACH → MDF (local filesystem)              │
  │  4. Upload MDF to Tenant S3 (STS AssumeRole)     │
  └─────────────────────────────────────────────────┘
```

### Data Flow

```
Fargate/Batch Extraction Task:
    ├── IcebergReader.stream_batches(row_filter="ExposureSetSID IN (...)")
    │       → Parquet files read from S3 via Glue Catalog
    ├── SqlServerWriter.write_table(batches) × N tables (ThreadPoolExecutor)
    │       → Rows loaded into SQL Server on localhost
    ├── ArtifactGenerator.generate_mdf(db_name)
    │       → sp_detach_db → AIRExposure_{run_id}.mdf + .ldf on /data/
    ├── S3Uploader.upload_artifacts(artifacts, tenant_role_arn)
    │       → STS AssumeRole → multipart PUT to tenant S3 bucket
    └── ProgressReporter.report(100, "COMPLETED", manifest=...)
            → HTTP POST to Activity Management callback URL
```

---

## Repository Structure (Extraction-Relevant)

```
DataIngestion/
├── src/
│   ├── core/
│   │   ├── iceberg_reader.py         # Read from Iceberg with exposure_id filter
│   │   ├── sql_writer.py             # Write Arrow batches to SQL Server
│   │   ├── column_mapper.py          # Iceberg → SQL column name mapping
│   │   ├── mapping_config.py         # JSON override loading (S3 + local)
│   │   ├── tenant_context.py         # Tenant Context Lambda wrapper
│   │   └── config.py                 # IcebergConfig, SqlServerConfig, AppConfig
│   │
│   └── fargate/
│       ├── entrypoint.py             # ← EXTRACTION TASK MAIN
│       ├── database_manager.py       # CREATE/DROP DATABASE lifecycle
│       ├── artifact_generator.py     # BACKUP / sp_detach_db → MDF/BAK
│       ├── s3_uploader.py            # Multipart upload + STS AssumeRole
│       ├── progress_reporter.py      # Activity Management callbacks
│       ├── validation.py             # Row counts, DBCC, checksums
│       └── check_records_lambda.py   # Lambda: early-exit if no matching rows
│
├── infra/
│   ├── step_functions.py             # get_three_path_extraction_definition()
│   ├── ecs.py                        # Fargate task definitions
│   └── iam.py                        # IAM roles and policies
│
├── docker/
│   ├── Dockerfile.express            # PATH 1: MSSQL_PID=Express (≤9GB)
│   ├── Dockerfile.standard           # PATH 2: MSSQL_PID=Standard (9-80GB)
│   └── Dockerfile.batch              # PATH 3: MSSQL_PID=Standard + EBS (>80GB)
│
├── scripts/
│   ├── local_test.py                 # Local extraction test
│   └── test_extraction_flow.py       # End-to-end extraction test
│
└── cdk/src/EES.AP.SDE.CDK/
    └── Stacks/
        ├── SDEEcrStack.cs
        ├── SDEFargateStack.cs
        ├── SDEBatchStack.cs
        └── SDEStepFunctionsStack.cs
```

---

## Docker Images

| Image | Dockerfile | `MSSQL_PID` | Target | Use Case |
|-------|------------|-------------|--------|----------|
| `data-pipeline-express` | Dockerfile.express | `Express` | Fargate | ≤9GB (free) |
| `data-pipeline-standard` | Dockerfile.standard | `Standard` | Fargate | 9-80GB (SPLA) |
| `data-pipeline-batch` | Dockerfile.batch | `Standard` | AWS Batch | >80GB (SPLA) |

```powershell
# Build all images
.\docker\build.ps1

# Build specific
.\docker\build.ps1 -Path express
.\docker\build.ps1 -Path standard
.\docker\build.ps1 -Path batch

# Build and push to ECR
.\docker\build.ps1 -Push -Tag v1.0.0

# Test PATH 1 locally
$env:SA_PASSWORD="YourStrong@Passw0rd"
$env:PIPELINE_MODE="extraction"
docker-compose -f docker/docker-compose.yml --profile express up --build
```

---

## Detailed Module Specifications

### `entrypoint.py` — Extraction Task Main

**File:** `src/fargate/entrypoint.py`  
**Entry:** `python -m src.fargate.entrypoint`

```python
def main() -> None:
    """Main extraction flow — reads from Iceberg, writes MDF artifact to tenant S3."""

    input_payload = parse_input()
    reporter = ProgressReporter(
        activity_id=input_payload.activity_id,
        callback_url=input_payload.callback_url or None,
        run_id=input_payload.run_id,
    )

    try:
        # 1. Report STARTING + resolve Tenant Context
        reporter.report(5, "STARTING")
        cfg = load_config()
        tenant_context = init_sde_context(input_payload.tenant_id)
        cfg.iceberg.namespace = tenant_context["glue_database"]
        bucket_name = tenant_context["bucket_name"]

        # 2. Create ephemeral SQL Server database
        reporter.report(10, "PROVISIONING")
        db_mgr = DatabaseManager(cfg.sqlserver)
        db_name = db_mgr.create_database(input_payload.tenant_id, input_payload.run_id)

        # 3. Load mapping overrides from S3 (optional)
        overrides = load_overrides_from_s3(input_payload.tenant_id)

        # 4. Discover tables + build execution plan
        reader = IcebergReader(cfg.iceberg)
        tables = reader.list_tables(cfg.iceberg.namespace)
        plan = build_execution_plan(tables, overrides, input_payload.exposure_ids)

        reporter.report(20, "LOADING")

        # 5. Parallel table load (ThreadPoolExecutor, max_workers=4)
        results = parallel_load_tables(
            plan=plan,
            reader=reader,
            sql_config=cfg.sqlserver,
            db_name=db_name,
            exposure_ids=input_payload.exposure_ids,
            max_workers=int(os.getenv("MAX_WORKERS", "4")),
            progress_callback=reporter.report_table_progress,
        )

        reporter.report(70, "VALIDATING")

        # 6. Validate (row counts + DBCC CHECKDB)
        validation = Validator(cfg.sqlserver).validate_database(db_name, results)
        if not validation.passed:
            raise ValidationError(validation)

        reporter.report(80, "GENERATING_ARTIFACT")

        # 7. Generate MDF/BAK artifact
        artifact_gen = ArtifactGenerator(cfg.sqlserver)
        artifacts = artifact_gen.generate(
            db_name=db_name,
            artifact_type=input_payload.artifact_type,  # "mdf" | "bak" | "both"
            output_dir="/data/backup",
        )

        reporter.report(90, "UPLOADING")

        # 8. Upload to tenant S3 bucket
        uploader = S3Uploader(
            tenant_id=input_payload.tenant_id,
            role_arn=os.getenv("TENANT_ROLE_ARN"),  # injected by SFN for Batch path
        )
        upload_result = uploader.upload_artifacts(
            artifacts=artifacts,
            tenant_bucket=bucket_name,
            prefix=f"artifacts/{input_payload.tenant_id}/{input_payload.activity_id}/",
        )

        # 9. Write manifest + report COMPLETED
        manifest = build_manifest(results, artifacts, upload_result, validation)
        uploader.upload_manifest(manifest)
        reporter.report(100, "COMPLETED", manifest=manifest)
        reporter.flush()

        # 10. Cleanup
        db_mgr.drop_database(db_name)

    except Exception as e:
        reporter.report(0, "FAILED", error=str(e))
        reporter.flush()
        raise
```

### `artifact_generator.py` — MDF/BAK Generation

**File:** `src/fargate/artifact_generator.py`

```python
class ArtifactGenerator:
    def generate_bak(self, db_name: str, output_path: str) -> ArtifactFile:
        """
        BACKUP DATABASE [{db_name}]
        TO DISK = '{output_path}/{db_name}.bak'
        -- WITH CHECKSUM (available on Standard; Express omits COMPRESSION)
        """

    def generate_mdf(self, db_name: str, output_dir: str) -> list[ArtifactFile]:
        """
        ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
        EXEC sp_detach_db '{db_name}', 'true'
        -- Returns: [.mdf path, .ldf path]
        """

    def generate(self, db_name: str, artifact_type: str, output_dir: str) -> list[ArtifactFile]:
        """Route to bak, mdf, or both based on artifact_type."""

    @staticmethod
    def compute_checksum(file_path: str, algorithm: str = "sha256") -> str:
        """Stream-hash file without loading entire file into memory."""
```

### `s3_uploader.py` — Multipart Upload

**File:** `src/fargate/s3_uploader.py`

```python
class S3Uploader:
    def __init__(self, tenant_id: str, role_arn: Optional[str] = None):
        """
        Fargate: role_arn=None → uses TaskRoleArn injected by Step Functions
        Batch:   role_arn=TENANT_ROLE_ARN → STS AssumeRole
        """

    def upload_artifacts(self, artifacts, tenant_bucket: str, prefix: str) -> UploadResult:
        """
        CreateMultipartUpload → UploadPart (100MB chunks) → CompleteMultipartUpload
        Verify ETag against local SHA256 checksum.
        """

    def upload_manifest(self, manifest: Manifest) -> str:
        """PUT manifest.json alongside artifacts."""
```

### `database_manager.py` — Database Lifecycle

**File:** `src/fargate/database_manager.py`

```python
class DatabaseManager:
    def create_database(self, tenant_id: str, run_id: str) -> str:
        """CREATE DATABASE extract_{tenant_id}_{run_id_short}"""

    def drop_database(self, db_name: str) -> None:
        """DROP DATABASE — cleanup after upload."""

    def wait_for_ready(self, timeout_seconds: int = 120) -> bool:
        """Poll SELECT 1 until SQL Server is responsive (cold start wait)."""

    def create_tables_from_plan(self, db_name: str, plan: ExecutionPlan) -> None:
        """CREATE TABLE for each table using Arrow → SQL type mapping."""
```

### `iceberg_reader.py` — Read with Exposure ID Filter

**File:** `src/core/iceberg_reader.py`

```python
def stream_batches(
    self,
    mode: str = "full",
    limit: Optional[int] = None,
    selected_columns: Optional[list[str]] = None,
    row_filter: Optional[str] = None,   # ← exposure_id predicate pushdown
) -> Iterator[pa.RecordBatch]:
    """
    row_filter example: "ExposureSetSID IN (101, 205, 309)"
    PyIceberg pushes this filter down to Parquet file scan — only reads
    matching row groups from S3, never loads non-matching rows into memory.
    """
```

---

## Check Records Lambda (Early Exit)

**File:** `src/fargate/check_records_lambda.py`  
**Invoked by:** Step Functions Lambda Task state BEFORE launching any ECS/Batch task.

Prevents spinning up an expensive task when no records exist for the requested `exposure_ids`.

```python
# SFN Input:
{
    "tenant_id": "...",
    "exposure_ids": [101, 205, 309],
    "tables": [],            # [] = all tables
    "estimated_size_gb": 12.4
}

# SFN Output (merged back into state):
{
    ...original input...,
    "record_check": {
        "has_records": True,
        "record_count": 15000,
        "tables_checked": 12
    }
}
```

Routing in the Step Function:
```
record_check.has_records == false  →  NoRecordsFound (Succeed, zero cost)
record_check.has_records == true   →  RouteBySize (PATH 1/2/3)
```

---

## Tenant Context Resolution

**File:** `src/core/tenant_context.py`  
**Called by:** `entrypoint.py` at startup.

Resolves per-tenant config from the shared Tenant Context Lambda:

| Field | Description |
|-------|-------------|
| `glue_database` | Iceberg namespace (e.g. `rs_cdkdev_ssautoma03_exposure_db`) |
| `bucket_name` | Tenant's destination S3 bucket |
| `tenant_role` | IAM role for cross-account S3 access |
| `client_id` | OAuth client ID |
| `client_secret` | OAuth client secret |

**Credential flow:**
```
ECS Fargate: Step Functions sets TaskRoleArn = tenant_role → no STS needed
AWS Batch:   TENANT_ROLE_ARN env var injected by SFN → S3Uploader does STS AssumeRole
```

Cached 50 minutes within the container (TTL = `CACHE_TTL_SECONDS = 3000`).

---

## Configuration Reference

All values read from environment variables by `load_config()`:

| Env Var | Purpose | Example |
|---------|---------|---------|
| `ICEBERG_NAMESPACE` | Source Iceberg/Glue namespace | `rs_cdkdev_ssautoma03_exposure_db` |
| `GLUE_CATALOG_REGION` | AWS region for Glue | `us-east-1` |
| `SQLSERVER_HOST` | SQL Server hostname | `localhost` |
| `SQLSERVER_PORT` | SQL Server port | `1433` |
| `SQLSERVER_USERNAME` | SA username | `sa` |
| `SQLSERVER_PASSWORD` | SA password (from Secrets Manager) | — |
| `MSSQL_PID` | SQL edition (injected by SFN) | `Express` or `Standard` |
| `TENANT_ROLE_ARN` | Tenant IAM role (Batch path only) | `arn:aws:iam::...` |
| `TENANT_CONTEXT_LAMBDA` | Lambda name for tenant lookup | `ss-cdkdev-tenant-context` |
| `MAX_WORKERS` | Parallel table load threads | `4` |

CDK thresholds:
```csharp
// cdk/src/EES.AP.SDE.CDK/Stacks/SDEStepFunctionsStack.cs
private const int ThresholdSmallGb = 9;   // ≤9GB → PATH 1 (Express)
private const int ThresholdLargeGb = 80;  // >80GB → PATH 3 (Batch)
```

---

## Column Mapping Configuration

### Three-Tier Mapping System

| Layer | Location | Purpose | Priority |
|-------|----------|---------|----------|
| **Auto-mapping** | `column_mapper.py` | 6-strategy automatic matching (Iceberg → SQL column names) | Lowest |
| **Generated config** | `mapping_config.json` | Auto-generated, user-reviewable | Medium |
| **Runtime overrides** | S3 (`config/{tenant}/mapping_overrides.json`) | Per-tenant overrides | Highest |

### Load Order

```
1. Auto-discover Iceberg columns
2. Load mapping_config.json (if exists)
3. Merge S3 overrides → overrides win on conflict
4. Apply final mapping to all table loads
```

```python
# Upload per-tenant overrides to S3
python scripts/upload_mapping_overrides.py \
  --bucket my-config-bucket \
  --tenant-id tenant-123 \
  --overrides-file my_overrides.json
# Stored at: s3://my-config-bucket/config/tenant-123/mapping_overrides.json
```

---

## Structured JSON Logging

All log output is structured JSON for CloudWatch Logs Insights + Dynatrace:

```json
{
  "timestamp": "2026-03-10T14:23:01Z",
  "level": "INFO",
  "logger": "src.fargate.entrypoint",
  "tenant_id": "ssautoma03",
  "activity_id": "act-abc123",
  "message": "[STEP 5/9] Table load complete — 15 tables, 2,450,000 rows in 42.3s"
}
```

`_ContextFilter` injects `tenant_id` and `activity_id` into every log record globally (attached to root handler). All child modules (iceberg_reader, sql_writer, etc.) automatically inherit this context.

---

## Cost Analysis (Extraction)

### Per-Run Costs

#### PATH 1: Fargate + Express (≤9GB)

| Component | Cost |
|-----------|------|
| Fargate (4 vCPU, 8GB, ~30min) | $0.25 |
| Ephemeral Storage (200GB) | $0.04 |
| S3 Transfer (upload to tenant) | $0.01-0.10 |
| SQL Express License | $0 (FREE) |
| SPLA | $0 |
| **Total per extraction run** | **~$0.30-0.40** |

#### PATH 2: Fargate + Standard (9-80GB)

| Component | Cost |
|-----------|------|
| Fargate (8 vCPU, 16GB, ~45min) | $0.75 |
| Ephemeral Storage (200GB) | $0.04 |
| S3 Transfer | $0.05-0.25 |
| SPLA (4 cores × ~$0.09/hr) | ~$0.36 |
| **Total per extraction run** | **~$1.20-1.40** |

#### PATH 3: Batch EC2 + Standard (>80GB)

| Component | Cost |
|-----------|------|
| EC2 (r5.2xlarge, ~90min) | $0.50 |
| EBS Storage (500GB gp3) | $0.08 |
| S3 Transfer | $0.10-0.50 |
| SPLA (4 cores × ~$0.09/hr) | ~$0.72 |
| **Total per extraction run** | **~$1.40-1.80** |

### Monthly (300 tenants, 1 extraction/week each)

Assuming 70% PATH 1, 20% PATH 2, 10% PATH 3:

| Component | Monthly Cost |
|-----------|-------------|
| PATH 1 — Express Fargate (840/mo) | $250-340 |
| PATH 2 — Standard Fargate (240/mo) | $290-340 |
| PATH 3 — Batch EC2 (120/mo) | $170-220 |
| S3 Storage | $25-50 |
| SPLA (PATH 2+3) | $400-800 |
| **Total monthly** | **~$1,135-1,750** |

**Cost per tenant/month: ~$3.80-5.85**

---

## SQL Server Standard Licensing — SPLA

PATH 2 and PATH 3 use SQL Server Standard (`MSSQL_PID=Standard`). Commercial use in containers requires a **SPLA (Services Provider License Agreement)**.

**PATH 1 (Express) is completely free — no SPLA needed.**

Full procurement guide: see [implementation_plan_ingestion.md → Section 10](implementation_plan_ingestion.md) or the inline section in [implementation_plan_bidirectional.md](implementation_plan_bidirectional.md).

### Quick Reference

| Path | `MSSQL_PID` | License | Action |
|------|-------------|---------|--------|
| PATH 1 ≤9GB | `Express` | None — FREE | No action needed |
| PATH 2 9-80GB | `Standard` | SPLA | Sign SPLA via Microsoft LSP; report 4 cores/task/month |
| PATH 3 >80GB | `Standard` | SPLA | Sign SPLA; report EC2 physical core count/month |

SPLA rate: **~$50–$80 per physical core per month**.

---

## Testing

### Prerequisites

```powershell
cd c:\Github\Rnd\DataIngestion
.\.venv\Scripts\Activate.ps1

# Ensure .env configured:
# ICEBERG_NAMESPACE=rs_cdkdev_ssautoma03_exposure_db
# SQLSERVER_HOST, PORT, USERNAME, PASSWORD
```

### Local Extraction Tests

```powershell
# All tables, 100 row limit per table
python scripts/local_test.py --target-bucket rks-sd-virusscan --target-prefix extraction-test/ --limit 100

# Specific tables only
python scripts/local_test.py --target-bucket rks-sd-virusscan --target-prefix extraction-test/ --tables tcontract tlocation --limit 100

# Full data, no limit
python scripts/local_test.py --target-bucket rks-sd-virusscan --target-prefix extraction-test/
```

**Expected Output:**
```
EXTRACTION TEST
================
Source Namespace: rs_cdkdev_ssautoma03_exposure_db
Target S3: s3://rks-sd-virusscan/extraction-test/
---
Processing: tcontract (100 rows)
Processing: tlocation (100 rows)
...
EXTRACTION COMPLETE
  Tables: 12
  Rows: 800
  MDF uploaded: s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf
```

### End-to-End Extraction Test

```powershell
python scripts/test_extraction_flow.py \
  --exposure-ids 101 205 309 \
  --target-namespace rs_cdkdev_ssautoma03_exposure_db \
  --artifact-type mdf
```

### Key Test Scenarios

| Scenario | Command |
|----------|---------|
| **Small extraction (≤9GB)** | `python scripts/local_test.py --limit 100 ...` |
| **Large extraction (>9GB)** | `python scripts/local_test.py ...` (no limit) |
| **Specific tables** | `--tables tcontract tlocation` |
| **BAK artifact** | `--artifact-type bak` |
| **Debug logging** | Set `LOG_LEVEL=DEBUG` in .env |

### Unit Tests

```
tests/unit/
  test_artifact_generator.py     # BACKUP / sp_detach_db SQL generation
  test_s3_uploader.py            # Multipart upload logic, ETag verify
  test_iceberg_reader.py         # Row filter pushdown, batch streaming
  test_sql_writer.py             # Arrow → SQL type mapping + bulk insert
  test_progress_reporter.py      # HTTP callback + S3 write
  test_validation.py             # Row count, DBCC, checksum verification
```

### Performance Benchmarks

| Dataset | Rows | Expected Time |
|---------|------|--------------|
| Small | 1M | < 2 min |
| Medium | 10M | < 10 min |
| Large | 40M | < 30 min |

### Troubleshooting

| Issue | Solution |
|-------|----------|
| **S3 access denied** | Check `TENANT_ROLE_ARN` and `~/.aws/credentials` |
| **SQL Server not ready** | Increase `CONNECTION_TIMEOUT` or check MSSQL container logs |
| **Iceberg table not found** | Verify `ICEBERG_NAMESPACE` matches Glue catalog |
| **Checksum mismatch** | Check S3 ETag — may indicate multipart upload issue |
| **No records found** | `CheckRecords` Lambda early-exit — check `exposure_ids` exist in namespace |

---

## Implementation Timeline

**Team:** 3 Engineers + Architect/Manager (oversight, code reviews, design decisions)  
**Scope:** Extraction API, Angular UI, Okta SSO, Multi-Tenancy, CDK stacks, GitHub Actions pipelines, Validation, End-to-End hardening

---

### Week 1: Extraction API + Validation + CDK Foundation

| Day | Engineer 1 (Backend API) | Engineer 2 (Infra / CDK) | Engineer 3 (Pipeline / Angular) | Architect/Manager |
|-----|--------------------------|--------------------------|----------------------------------|-------------------|
| Mon | Complete `validation.py` — Lambda-based record-count post-extract verification | CDK stack for API Gateway (REST) + ECS Fargate service for extraction API | Bootstrap Angular project (nx workspace), extraction module scaffold, routing setup | Kick-off, assign tracks, review API contract design and OpenAPI spec |
| Tue | FastAPI extraction service — `POST /extractions` trigger endpoint, `GET /extractions/{id}` status endpoint | Okta developer app registration (OIDC), JWT authorizer Lambda (RS256 token validation) | Okta Angular SDK integration — `OktaAuthModule`, login/logout/callback routes, auth guard | Review Okta integration approach, token scopes, tenant claim mapping |
| Wed | Multi-tenant middleware — extract `tenantId` from Okta JWT claim, scope all DB queries to tenant | ECS task definition CDK — environment injection (`TENANT_ID`, `SFN_ARN`), Secrets Manager for SQL credentials | Extraction trigger form (Angular reactive form) — tenant selector, table list, exposure IDs, estimated size input | Review multi-tenant isolation model, confirm IAM session policy scoping |
| Thu | SFN execution launcher — API calls `StartExecution` with validated payload, returns `executionArn` | IAM CDK — API ECS task role (SFN:StartExecution, SFN:DescribeExecution scoped per env), VPC private endpoints for SFN + Secrets | Extraction status polling component — progress bar, three-path label (Express / Standard / Batch), live log tail via CloudWatch Logs Insights API | Review Step Function input/output contract, validate tenant-scoped SFN ARN strategy |
| Fri | Artifact download URL endpoint — `GET /extractions/{id}/download` returns pre-signed S3 URL scoped to tenant prefix | Unit tests for CDK stacks (`aws-cdk-lib/assertions`), CDK synth validation in CI | Angular download component — pre-signed URL fetch, browser download trigger, checksum display | Week 1 retrospective, review API surface, adjust Week 2 plan |

---

### Week 2: Angular UI Completion + Multi-Tenancy + CI/CD Pipelines

| Day | Engineer 1 (Backend API) | Engineer 2 (Infra / CDK) | Engineer 3 (Pipeline / Angular) | Architect/Manager |
|-----|--------------------------|--------------------------|----------------------------------|-------------------|
| Mon | Extraction history list API — `GET /extractions?tenantId=&status=&from=&to=` with pagination | GitHub Actions workflow — `extraction-api-ci.yml`: lint → pytest → Docker build → ECR push (on PR merge to main) | Extraction history dashboard (Angular) — filterable/sortable table, status badges, tenant-scoped view | Review CI/CD pipeline structure, approve deployment gate strategy (manual approval for prod) |
| Tue | Schema version detection endpoint — `GET /extractions/schema-version` reads Iceberg metadata markers | GitHub Actions workflow — `extraction-infra-cd.yml`: CDK diff → manual approval → CDK deploy (dev → staging → prod) | Multi-tenant tenant-switcher component (header dropdown) — Okta `groups` claim → available tenants list | Review CD promotion gates (staging smoke test must pass before prod), IaC drift alerts |
| Wed | Integration test suite (`tests/integration/test_extraction_api.py`) — full flow with localstack + SQL Server container | CloudFront + S3 CDK stack for Angular SPA hosting — OAC policy, custom domain, ACM certificate, HTTPS-only | Angular extraction E2E test setup (Playwright) — happy path: login → trigger → poll → download | Validate dev deployment, run smoke test checklist, sign off on integration test coverage |
| Thu | Error catalog — structured error codes (`EXT-001` through `EXT-050`) mapped to HTTP status, surfaced in API response and UI | Cross-account IAM CDK — extraction ECS task assumes tenant-specific role via STS, `s3:PutObject` scoped to `/tenants/{tenantId}/extractions/*` | Angular error state handling — API error codes → user-friendly messages, retry button, support reference code display | Review cross-account IAM trust policies, tenant S3 prefix isolation sign-off |
| Fri | Performance profiling — extraction API latency under 10 concurrent requests, SFN polling optimisation | Monitoring CDK — CloudWatch dashboard (extraction SFN executions, Fargate CPU/mem, API 4xx/5xx), SNS alarm → email/Slack | Angular responsive layout polish, loading skeletons, accessibility (WCAG 2.1 AA) audit | Week 2 retrospective, staging deployment review, identify Week 3 hardening items |

---

### Week 3: Integration Testing + Staging + Production Readiness

| Day | Engineer 1 (Backend API) | Engineer 2 (Infra / CDK) | Engineer 3 (Pipeline / Angular) | Architect/Manager |
|-----|--------------------------|--------------------------|----------------------------------|-------------------|
| Mon | Load test setup (Locust) — 20 concurrent extraction triggers across 5 tenants, validate tenant isolation under load | Deploy full extraction stack to staging (CDK deploy `--context env=staging`), validate Okta callback URLs, CloudFront domain | Angular staging smoke test — Okta login with staging IdP, trigger extraction against staging SFN, verify download | Review staging deployment checklist, confirm Okta app config matches staging URLs |
| Tue | Load test execution — 8 GB / 20 tables / 10 exposures per tenant, measure SFN execution times, API p99 latency | Staging validation — CloudWatch alarms fire correctly, X-Ray traces capture full extraction flow, ECS autoscaling triggers | Playwright E2E suite execution on staging — all critical paths (trigger, poll, download, error states, tenant switch) | Validate load test results, confirm SLAs: API < 200ms p99, extraction < 5 min for ≤ 9 GB |
| Wed | Fix issues surfaced from load test and E2E runs | Production CDK stack review — separate AWS account/region, tighter IAM, WAF on API Gateway, Shield Standard | Fix Angular issues from E2E run, add Datadog / Dynatrace RUM script injection | Security review — OWASP top 10 check on API, JWT expiry handling, CSP headers on CloudFront |
| Thu | Final integration test on staging — full extraction pipeline (Iceberg → SQL → MDF → Tenant S3 → API download) | Production deployment (`CDK deploy --context env=prod`) with feature flag (`EXTRACTION_API_ENABLED=true`) | End-to-end production smoke test — one real tenant, one real extraction, verify S3 artifact and download URL | Go/no-go decision, stakeholder demo, sign-off |
| Fri | Buffer — address any post-go-live issues, hot-fix pipeline ready | Production monitoring tuning — CloudWatch alarm thresholds, Runbook published to Confluence | Knowledge transfer session — API usage guide, Angular component docs, local dev setup guide | Final sign-off, close sprint, retrospective |

---

### Week 4 (Buffer — Only If Required)

| Area | Tasks |
|------|-------|
| Backend API | Schema version detection implementation if client markers are ready; additional extraction configuration endpoints |
| CDK / Infra | Multi-region disaster recovery setup; advanced WAF rules; VPC Flow Logs analysis |
| Angular UI | Advanced filtering and export (CSV) on history dashboard; notification email configuration UI |
| Testing | Chaos engineering — Fargate task kill mid-extraction, verify SFN retry and idempotency |
| Documentation | ADR (Architecture Decision Records) for three-path routing, Okta integration, multi-tenancy model |

**Total: 3 weeks core delivery + 1 week buffer**

---

## Security Considerations

### Multi-Tenant Isolation

```
SHARED: ECS Cluster, ECR repos, SFN state machine, Batch CE/Queue
RUNTIME ISOLATION (per job):
  • TaskRoleArn scoped to tenant's S3 prefix only
  • IAM session policy: s3:PutObject restricted to /tenants/{tenantId}/*
  • Fargate PATH 1/2: Firecracker micro-VM (dedicated kernel per task)
  • Batch PATH 3: Docker namespace isolation
  • Ephemeral container destroyed after job
  • No shared SQL Server state between tenants
```

### SOC2 Controls

| Control | Implementation |
|---------|---------------|
| CC6.1 Logical Access | IAM session policy per job, tenant-scoped S3 paths |
| CC6.3 Least Privilege | TaskRole can only write to tenant's S3 prefix |
| CC6.5 Data Disposal | Ephemeral Fargate/Batch containers destroyed post-job |
| CC6.7 Encryption at Rest | S3 SSE-KMS, EBS encryption |
| CC6.7 Encryption in Transit | TLS everywhere (S3, Glue, Secrets Manager) |
| CC7.2 Audit Trail | CloudWatch logs tagged with tenantId + activityId |

### Extraction-Specific Security
- `IcebergReader` uses session policy to restrict S3 data access to tenant namespace only
- Parquet files are read transiently — never persisted to disk in the container
- MDF artifacts written to ephemeral `/data/` volume, deleted after upload
- Tenant S3 bucket receives artifacts only via STS-scoped session

---

## Related Documents

| Document | Description |
|----------|-------------|
| [implementation_plan_ingestion.md](implementation_plan_ingestion.md) | Ingestion pipeline plan (Tenant S3 → Iceberg) |
| [implementation_plan_bidirectional.md](implementation_plan_bidirectional.md) | Shared infrastructure overview |
| [sequence_diagram.md](sequence_diagram.md) | Mermaid sequence diagrams for both flows |
| [SOW_deliverables_tracker.md](SOW_deliverables_tracker.md) | Full deliverable status tracking |
