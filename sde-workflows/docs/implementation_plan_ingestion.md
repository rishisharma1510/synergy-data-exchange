# Ingestion Pipeline — Implementation Plan

> **Flow:** Tenant S3 bucket → Download MDF/BAK → SQL Server (ephemeral) → SID Transform → Iceberg Data Lake

## Overview

The ingestion pipeline downloads an MDF or BAK artifact from a tenant's S3 bucket, restores it into an ephemeral SQL Server instance, applies SID transformations, and writes the data into our Iceberg/Glue data lake.

| Activity Type | Flow | Trigger |
|---------------|------|---------|
| **Ingestion** | Tenant S3 → MDF/BAK → SQL Server → Iceberg | Tenant submits ingestion request via Activity Management |

Both a **Step Functions state machine** and a **Fargate/Batch task** are dedicated to ingestion. The extraction pipeline runs on a completely separate state machine and task — see [implementation_plan_extraction.md](implementation_plan_extraction.md).

---

## Implementation Status (Updated 2026-03-10)

| Component | Status | Tests | Notes |
|-----------|--------|-------|-------|
| **Ingestion Entrypoint** | ✅ 100% | — | Full pipeline with SID + Exposure API + Tenant Context |
| **Artifact Restorer** | ✅ 100% | — | RESTORE DATABASE + ATTACH from MDF + checksum verify |
| **SQL Reader** | ✅ 100% | — | Stream SQL Server tables as Arrow batches |
| **Iceberg Writer** | ✅ 100% | — | Append/overwrite, auto-create table if not exists |
| **S3 Uploader (download)** | ✅ 100% | — | Cross-account download with STS AssumeRole |
| **SID Transformation Pipeline** | ✅ 100% | 73 | transformer, sid_client, checkpoint, audit, parallel |
| **Exposure API Integration** | ✅ 100% | 20 | exposure_client with override SID mapping |
| **Size Estimator Lambda** | ✅ 100% | — | s3:HeadObject → returns selected_path (EXPRESS/STANDARD/BATCH) |
| **Check Records Lambda** | ✅ 100% | — | Early-exit optimization (shared with extraction) |
| **Tenant Context Resolution** | ✅ 100% | — | Lambda lookup for glue_database, bucket, creds |
| **Structured JSON Logging** | ✅ 100% | — | _JsonFormatter + _ContextFilter |
| **Progress Reporter** | ✅ 100% | — | S3 writes (always) + HTTP callback (optional) |
| **Infrastructure (CDK)** | ✅ 100% | — | 4 stacks: ECR, Fargate, Batch, Step Functions |
| **Validation Framework** | ⚠️ 25% | — | Row-count + DBCC; Lambda-based check missing |

**Overall:** ~90% complete

See [SOW_deliverables_tracker.md](SOW_deliverables_tracker.md) for full detail.

---

## System Architecture

### Three-Path Design

The system uses **size-based routing** with configurable thresholds. For ingestion, the **Step Functions pipeline automatically measures the artifact size** via a Lambda before routing — the caller does not need to supply a size value.

| Path | Data Size | Infrastructure | SQL Edition | Cost/Run | License |
|------|-----------|----------------|-------------|----------|---------|
| **PATH 1** | ≤9 GB | ECS Fargate | Express | ~$0.30-0.40 | FREE |
| **PATH 2** | 9-80 GB | ECS Fargate | Standard | ~$1.20-1.50 | SPLA |
| **PATH 3** | >80 GB | AWS Batch EC2 | Standard | ~$1.40-1.80 | SPLA |

Default thresholds: `DEFAULT_THRESHOLD_SMALL_GB = 9`, `DEFAULT_THRESHOLD_LARGE_GB = 80` (in `infra/step_functions.py`). Configurable via CDK: `ThresholdSmallGb` / `ThresholdLargeGb`.

---

### Size Estimation — Automated via Lambda

For ingestion, the Step Function **automatically measures the artifact size** before routing. The caller only needs to supply the S3 location of the artifact.

```
Ingestion SFN flow:
  PrepareIngestionInput (Pass - injects ingestion mode flags)
      → MeasureArtifactSize (Lambda Task)
            Input:  s3_input_path = States.Format('s3://{}/{}',
                        $.source_s3_bucket, $.source_s3_key)
            Action: s3:HeadObject → reads Content-Length of the .mdf/.bak artifact
            Output: {
                       "selected_path": "EXPRESS" | "STANDARD" | "BATCH",
                       "sde_context": { "tenant_role": "arn:aws:iam::..." }
                    }
      → RouteBySize (Choice on $.selected_path STRING — not numeric)
          → "EXPRESS"  → RunFargateExpress  (MSSQL_PID=Express)
          → "STANDARD" → RunFargateStandard (MSSQL_PID=Standard)
          → "BATCH"    → RunBatchEC2        (MSSQL_PID=Standard)
```

> **Key difference from Extraction:** Extraction routes on a *numeric* `$.estimated_size_gb` field supplied by the caller. Ingestion routes on a *string* `$.selected_path` field returned by the Size Estimator Lambda. The caller never needs to know the file size.

The Size Estimator Lambda applies the same thresholds and returns a string label:
```python
# Inside MeasureArtifactSize Lambda:
size_bytes = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
size_gb = size_bytes / 1_000_000_000

if size_gb <= threshold_small_gb:    # 9 GB
    selected_path = "EXPRESS"
elif size_gb <= threshold_large_gb:  # 80 GB
    selected_path = "STANDARD"
else:
    selected_path = "BATCH"

return { "selected_path": selected_path, "sde_context": {...} }
```

### Routing Pseudocode

```python
# Step Function Choice state routes on $.selected_path (string)
if selected_path == "EXPRESS":
    route = "PATH 1: Fargate + Express (FREE)"
    MSSQL_PID = "Express"
elif selected_path == "STANDARD":
    route = "PATH 2: Fargate + Standard (SPLA Licensed)"
    MSSQL_PID = "Standard"
elif selected_path == "BATCH":
    route = "PATH 3: Batch EC2 + Standard (SPLA Licensed)"
    MSSQL_PID = "Standard"
```

---

### Architecture Diagram

```
┌─────────────────┐
│  Tenant UI/API  │
└────────┬────────┘
         │ POST /activities  (activity_type: "ingestion")
         ▼
┌─────────────────────────────────────────────────────────┐
│               ACTIVITY MANAGEMENT                        │
│  StartExecution on Ingestion State Machine              │
│  Input: { source_s3_bucket, source_s3_key, tenant_id } │
│  No size estimation needed — SFN handles it             │
└───────────────────────┬─────────────────────────────────┘
                        │ StartExecution
                        ▼
┌─────────────────────────────────────────────────────────┐
│           INGESTION STEP FUNCTIONS                       │
│  PrepareIngestionInput (Pass)                           │
│  → MeasureArtifactSize (Lambda: s3:HeadObject)          │
│       Returns selected_path: EXPRESS/STANDARD/BATCH     │
│  → RouteBySize (Choice on $.selected_path)              │
│       EXPRESS    STANDARD      BATCH                    │
│          ▼          ▼            ▼                      │
│  Fargate/Express Fargate/Std  Batch EC2/Std             │
└─────────────────────────────────────────────────────────┘
         ▼                ▼               ▼
  ┌─────────────────────────────────────────────────────┐
  │  INGESTION TASK (all paths use same code)            │
  │  1. Download .mdf from Tenant S3                     │
  │  2. ATTACH (or RESTORE from .bak) to SQL Server      │
  │  3. Read tables → SID Transform → Write to Iceberg   │
  │  4. Cleanup: DROP DATABASE, remove local files       │
  └─────────────────────────────────────────────────────┘
```

### Data Flow

```
Fargate/Batch Ingestion Task:
    ├── S3Uploader.download_artifact(bucket, key, role_arn)
    │       → STS AssumeRole → GET from tenant S3 bucket
    │       → artifact saved to /data/backup/
    ├── ArtifactRestorer.attach_from_mdf(mdf_path, db_name)   ← for MDF
    │       OR ArtifactRestorer.restore_from_bak(bak_path, db_name)  ← for BAK
    │       → SQL Server CREATE DATABASE ... FOR ATTACH (or RESTORE)
    ├── SqlServerReader.stream_table(table_name) × N tables
    │       → Arrow record batches streamed from SQL Server (localhost)
    ├── SIDTransformer.transform_table(table_name, arrow_table)
    │       → SID API: allocate new SID range for primary keys
    │       → FK cascade: update foreign key refs to new SIDs
    ├── IcebergWriter.append(namespace, table_name, batches)
    │       → Parquet files written to S3 via Glue Catalog
    └── ProgressReporter.report(100, "COMPLETED", manifest=...)
            → HTTP POST to Activity Management callback URL
```

---

## Repository Structure (Ingestion-Relevant)

```
DataIngestion/
├── src/
│   ├── core/
│   │   ├── sql_reader.py             # ← INGESTION: Read SQL Server as Arrow batches
│   │   ├── iceberg_writer.py         # ← INGESTION: Write Arrow batches to Iceberg
│   │   ├── column_mapper.py          # Reverse mapping: SQL column → Iceberg column
│   │   ├── mapping_config.py         # JSON override loading (S3 + local)
│   │   ├── tenant_context.py         # Tenant Context Lambda wrapper
│   │   └── config.py                 # IcebergConfig, SqlServerConfig, AppConfig
│   │
│   ├── fargate/
│   │   ├── ingestion_entrypoint.py   # ← INGESTION TASK MAIN
│   │   ├── artifact_restorer.py      # ← RESTORE from BAK / ATTACH from MDF
│   │   ├── s3_uploader.py            # Download from tenant S3 (+ existing upload)
│   │   ├── progress_reporter.py      # Activity Management callbacks
│   │   ├── validation.py             # Row counts, DBCC
│   │   └── check_records_lambda.py   # Lambda: early-exit (shared with extraction)
│   │
│   └── sid/
│       ├── transformer.py            # SID allocation + FK cascade
│       ├── sid_client.py             # SID API client
│       ├── config.py                 # SID_CONFIG: table hierarchy, FK map
│       └── exposure_client.py        # Exposure API registration
│
├── infra/
│   ├── step_functions.py             # get_three_path_ingestion_definition()
│   ├── ecs.py                        # Fargate task definitions
│   └── iam.py                        # IAM roles and policies
│
├── docker/
│   ├── Dockerfile.express            # PATH 1: MSSQL_PID=Express (≤9GB)
│   ├── Dockerfile.standard           # PATH 2: MSSQL_PID=Standard (9-80GB)
│   └── Dockerfile.batch              # PATH 3: MSSQL_PID=Standard + EBS (>80GB)
│
├── scripts/
│   ├── test_ingestion_flow.py        # Local ingestion test
│   └── local_test.py                 # Shared local test (extraction + ingestion)
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

# Test ingestion PATH 1 locally
$env:SA_PASSWORD="YourStrong@Passw0rd"
$env:PIPELINE_MODE="ingestion"
docker-compose -f docker/docker-compose.yml --profile express up --build

# Test PATH 2 (Standard)
docker-compose -f docker/docker-compose.yml --profile standard up --build

# Test PATH 3 (Batch)
docker-compose -f docker/docker-compose.yml --profile batch up --build
```

---

## Detailed Module Specifications

### `ingestion_entrypoint.py` — Ingestion Task Main

**File:** `src/fargate/ingestion_entrypoint.py`  
**Entry:** `python -m src.fargate.ingestion_entrypoint`

```python
@dataclass
class IngestionInput:
    tenant_id: str
    activity_id: str
    run_id: str
    source_s3_bucket: str
    source_s3_key: str                   # Path to MDF/BAK in tenant bucket
    artifact_type: str = "bak"           # "bak" | "mdf"
    checksum: Optional[str] = None       # Expected SHA256 for verification
    source_role_arn: Optional[str] = None  # STS role (Batch mode only)
    target_namespace: str = "default"    # Resolved from Tenant Context
    tables: List[str] = field(default_factory=list)  # [] = all tables
    write_mode: str = "append"           # "append" | "overwrite"
    callback_url: str = ""


def main() -> None:
    """Main ingestion flow — receives data FROM tenant INTO our Iceberg lake."""

    input_payload = parse_input()
    reporter = ProgressReporter(
        activity_id=input_payload.activity_id,
        callback_url=input_payload.callback_url or None,
        run_id=input_payload.run_id,
        s3_bucket=input_payload.source_s3_bucket,
    )

    try:
        # 1. Report STARTING + resolve Tenant Context
        reporter.report(5, "STARTING")
        cfg = load_config()
        tenant_context = init_sde_context(input_payload.tenant_id)
        cfg.iceberg.namespace = tenant_context["glue_database"]
        input_payload.target_namespace = tenant_context["glue_database"]

        # 2. Download artifact from tenant S3
        reporter.report(10, "DOWNLOADING")
        role_arn = input_payload.source_role_arn  # None for Fargate; set by SFN for Batch
        uploader = S3Uploader(tenant_id=input_payload.tenant_id, role_arn=role_arn)
        local_artifact_path = uploader.download_artifact(
            bucket=input_payload.source_s3_bucket,
            key=input_payload.source_s3_key,
            local_dir="/data/backup",
        )

        # 3. Restore/Attach database
        reporter.report(20, "RESTORING")
        restorer = ArtifactRestorer(cfg.sqlserver)
        restorer.connect()
        db_name = f"ingest_{input_payload.tenant_id}_{input_payload.run_id[:8]}"

        if input_payload.artifact_type == "bak":
            restorer.restore_from_bak(
                bak_file=local_artifact_path,
                database_name=db_name,
                expected_checksum=input_payload.checksum,
            )
        else:
            restorer.attach_from_mdf(
                mdf_file=local_artifact_path,
                database_name=db_name,
            )

        # Validate restore succeeded
        validation = restorer.validate_restore(db_name)
        if not validation.get("valid"):
            raise ValueError(f"Restore validation failed: {validation.get('error')}")

        # 4. Read SQL + SID transform + write to Iceberg
        reporter.report(30, "READING")
        sql_reader = SqlServerReader(cfg.sqlserver, db_name)
        sql_reader.connect()
        ice_writer = IcebergWriter(cfg.iceberg)
        mapper = ColumnMapper(naming_convention=cfg.ingestion.naming_convention)

        tables = input_payload.tables or sql_reader.list_tables()

        ingestion_results, exposure_sets_info = _process_tables_with_sid(
            tables=tables,
            sql_reader=sql_reader,
            ice_writer=ice_writer,
            mapper=mapper,
            cfg=cfg,
            input_payload=input_payload,
            reporter=reporter,
        )

        # 5. Cleanup
        reporter.report(90, "CLEANUP")
        sql_reader.close()
        restorer.cleanup_database(db_name)
        restorer.close()
        os.remove(local_artifact_path)

        # 6. Write summary JSON to S3
        summary = {
            "tenant_id": input_payload.tenant_id,
            "run_id": input_payload.run_id,
            "target_namespace": input_payload.target_namespace,
            "tables_success": sum(1 for r in ingestion_results if r["status"] == "success"),
            "tables_failed": sum(1 for r in ingestion_results if r["status"] == "failed"),
            "tables_skipped": sum(1 for r in ingestion_results if r["status"] == "skipped"),
            "total_rows": sum(r.get("rows", 0) for r in ingestion_results),
            "exposure_sets": exposure_sets_info,
            "tables": ingestion_results,
        }
        s3.put_object(
            Bucket=input_payload.source_s3_bucket,
            Key=f"sde/runs/{input_payload.run_id}/ingestion_summary.json",
            Body=json.dumps(summary).encode(),
        )

        reporter.report(100, "COMPLETED", manifest=summary)
        reporter.flush()

        failed = [r["table"] for r in ingestion_results if r["status"] == "failed"]
        if failed:
            raise RuntimeError(f"{len(failed)} table(s) failed: {', '.join(failed)}")

    except Exception as e:
        reporter.report(0, "FAILED", error=str(e))
        reporter.flush()
        raise
```

---

### `artifact_restorer.py` — Restore BAK / Attach MDF

**File:** `src/fargate/artifact_restorer.py`

```python
class ArtifactRestorer:
    def restore_from_bak(
        self,
        bak_file: str,
        database_name: str,
        expected_checksum: Optional[str] = None
    ) -> str:
        """
        RESTORE FILELISTONLY FROM DISK = '{bak_path}'
        → discover logical file names
        RESTORE DATABASE [{database_name}]
        FROM DISK = '{bak_path}'
        WITH MOVE '{logical_data}' TO '{mdf_path}',
             MOVE '{logical_log}'  TO '{ldf_path}',
             REPLACE, RECOVERY
        """

    def attach_from_mdf(
        self,
        mdf_file: str,
        database_name: str,
        ldf_file: Optional[str] = None
    ) -> str:
        """
        CREATE DATABASE [{database_name}]
        ON (FILENAME = '{mdf_path}')
        FOR ATTACH_REBUILD_LOG     -- rebuilds log if .ldf not present
        """

    def validate_restore(self, database_name: str) -> dict:
        """
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES → verify tables present
        Returns { "valid": True, "table_count": N }
        """

    def cleanup_database(self, database_name: str) -> None:
        """
        ALTER DATABASE [{database_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
        DROP DATABASE [{database_name}]
        """

    def verify_checksum(self, file_path: Path, expected: str) -> bool:
        """SHA256 stream-hash verify against expected checksum."""
```

---

### `sql_reader.py` — Read SQL Server as Arrow

**File:** `src/core/sql_reader.py`

```python
class SqlServerReader:
    """Read SQL Server tables and stream as PyArrow record batches."""

    def list_tables(self) -> List[str]:
        """
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        """

    def stream_table(
        self,
        table_name: str,
        batch_size: int = 10000
    ) -> Iterator[pa.RecordBatch]:
        """
        Stream table data as Arrow record batches.
        Uses server-side cursor (cursor.fetchmany) for memory efficiency.
        Never loads entire table into memory.
        """
        cursor.execute(f"SELECT * FROM [{table_name}]")
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield pa.RecordBatch.from_arrays(
                [pa.array([r[i] for r in rows]) for i in range(len(columns))],
                names=columns
            )

    def get_row_count(self, table_name: str) -> int:
        """SELECT COUNT(*) FROM [{table_name}]"""
```

---

### `iceberg_writer.py` — Write to Iceberg Tables

**File:** `src/core/iceberg_writer.py`

```python
class IcebergWriter:
    """Write Arrow data to Iceberg tables via Glue Catalog."""

    def table_exists(self, namespace: str, table_name: str) -> bool:
        """Try catalog.load_table(); catch NoSuchTableError."""

    def create_table_if_not_exists(
        self,
        namespace: str,
        table_name: str,
        schema: pa.Schema,
        location: Optional[str] = None
    ) -> Table:
        """
        Convert Arrow schema → Iceberg schema → catalog.create_table()
        Honors type map: pa.int32 → IntegerType, pa.string → StringType, etc.
        """

    def append(
        self,
        namespace: str,
        table_name: str,
        batches: List[pa.RecordBatch]
    ) -> int:
        """table.append(pa.Table.from_batches(batches)). Returns rows written."""

    def overwrite(
        self,
        namespace: str,
        table_name: str,
        batches: List[pa.RecordBatch]
    ) -> int:
        """table.overwrite(pa.Table.from_batches(batches)). Returns rows written."""
```

---

### `s3_uploader.py` — Download from Tenant S3

**File:** `src/fargate/s3_uploader.py` (extended for ingestion)

```python
class S3Uploader:
    def download_artifact(
        self,
        bucket: str,
        key: str,
        local_dir: str,
    ) -> str:
        """
        GET s3://{bucket}/{key} → /data/backup/{filename}
        Uses STS AssumeRole (Batch) or TaskRoleArn (Fargate) for cross-account access.
        Returns local file path.
        """
```

---

### SID Transformation Pipeline

**Files:** `src/sid/transformer.py`, `src/sid/config.py`, `src/sid/exposure_client.py`

The SID transformer runs as part of the ingestion table processing loop:

```python
def _process_tables_with_sid(tables, sql_reader, ice_writer, mapper, cfg, input_payload, reporter):
    """
    Process tables in FK-dependency order (from SID_CONFIG hierarchy).

    Per table:
    1. (Optional) Exposure API: register ExposureSet in central catalog
    2. Read full table from restored SQL database into Arrow
    3. SID transform: allocate new SID range from SID API for PK column
    4. FK cascade: update foreign key refs using parent table's SID mapping
    5. Column rename via ColumnMapper (SQL column → Iceberg column)
    6. Write to Iceberg (append or overwrite)
    """
    # Tables processed in dependency order:
    # Level 0: tExposureSet (root — no FKs)
    # Level 1: tContract, tExposure (FK → tExposureSet)
    # Level 2: tLocation, tInsuredValue (FK → tContract / tExposure)
    # ...
    processing_order = get_processing_order()  # [(level, table_name), ...]
```

**SID Config** at `src/sid/config.py` defines the dependency hierarchy. Tables NOT in `SID_CONFIG` are ingested as-is (no SID transform, just column rename).

---

### Size Estimator Lambda

**Invoked by:** `MeasureArtifactSize` Step Functions Lambda Task state (ingestion SFN only).  
**Not needed for extraction** — extraction caller supplies `estimated_size_gb` directly.

```python
# Lambda handler (deployed separately from the Fargate task)
def handler(event, context):
    bucket = event["source_s3_bucket"]
    key = event["source_s3_key"]

    # s3:HeadObject — does NOT download the file, just reads metadata
    response = s3.head_object(Bucket=bucket, Key=key)
    size_bytes = response["ContentLength"]
    size_gb = size_bytes / 1_000_000_000

    threshold_small = float(os.getenv("THRESHOLD_SMALL_GB", "9"))
    threshold_large = float(os.getenv("THRESHOLD_LARGE_GB", "80"))

    if size_gb <= threshold_small:
        selected_path = "EXPRESS"
    elif size_gb <= threshold_large:
        selected_path = "STANDARD"
    else:
        selected_path = "BATCH"

    return {
        "selected_path": selected_path,
        "size_gb": round(size_gb, 3),
        "sde_context": event.get("sde_context", {}),
    }
```

The Step Functions `RouteBySize` Choice state then routes on `$.selected_path` (string).

---

## Check Records Lambda (Early Exit)

**File:** `src/fargate/check_records_lambda.py`  
**Shared with extraction pipeline.**

For ingestion, this Lambda runs before `MeasureArtifactSize`. It verifies that the artifact contains tables that exist in the target Iceberg namespace. If the MDF/BAK was created from a namespace with no matching tables, the pipeline exits early at zero cost.

```
SFN Ingestion flow with early exit:
  PrepareIngestionInput (Pass)
      → MeasureArtifactSize (Lambda — measures .mdf size)
      → RouteBySize (Choice — EXPRESS/STANDARD/BATCH)
          → RunFargateExpress / RunFargateStandard / RunBatchEC2
```

*Note: The Check Records Lambda for ingestion validates against destination namespace, not source Iceberg (unlike extraction where it validates source).*

---

## Tenant Context Resolution

**File:** `src/core/tenant_context.py`  
**Called by:** `ingestion_entrypoint.py` at startup.

Resolves per-tenant config from the shared Tenant Context Lambda:

| Field | Description |
|-------|-------------|
| `glue_database` | Target Iceberg namespace (e.g. `rs_cdkdev_dclegend01_exposure_db`) |
| `bucket_name` | Tenant's source S3 bucket |
| `tenant_role` | IAM role for cross-account S3 download |
| `client_id` | OAuth client ID for SID / Exposure API calls |
| `client_secret` | OAuth client secret |

**Credential flow:**
```
ECS Fargate: Step Functions sets TaskRoleArn = tenant_role → no STS needed
AWS Batch:   TENANT_ROLE_ARN env var injected by SFN → S3Uploader.download_artifact does STS AssumeRole
```

Cached 50 minutes within the container (TTL = `CACHE_TTL_SECONDS = 3000`).

---

## Configuration Reference

All values read from environment variables by `load_config()`:

| Env Var | Purpose | Example |
|---------|---------|---------|
| `ICEBERG_NAMESPACE` | Target Iceberg/Glue namespace | `rs_cdkdev_dclegend01_exposure_db` |
| `GLUE_CATALOG_REGION` | AWS region for Glue | `us-east-1` |
| `SQLSERVER_HOST` | SQL Server hostname | `localhost` |
| `SQLSERVER_PORT` | SQL Server port | `1433` |
| `SQLSERVER_USERNAME` | SA username | `sa` |
| `SQLSERVER_PASSWORD` | SA password (from Secrets Manager) | — |
| `MSSQL_PID` | SQL edition (injected by SFN) | `Express` or `Standard` |
| `TENANT_ROLE_ARN` | Tenant IAM role (Batch path only) | `arn:aws:iam::...` |
| `TENANT_CONTEXT_LAMBDA` | Lambda name for tenant lookup | `ss-cdkdev-tenant-context` |
| `SID_API_URL` | SID allocation API base URL | `https://sid-api.internal/...` |
| `EXPOSURE_API_URL` | Exposure registration API URL | `https://exposure-api.internal/...` |
| `BATCH_SIZE` | SQL Server cursor fetch size | `10000` |
| `THRESHOLD_SMALL_GB` | Small path threshold (Size Estimator Lambda) | `9` |
| `THRESHOLD_LARGE_GB` | Large path threshold (Size Estimator Lambda) | `80` |

CDK thresholds:
```csharp
// cdk/src/EES.AP.SDE.CDK/Stacks/SDEStepFunctionsStack.cs
private const int ThresholdSmallGb = 9;   // ≤9GB → PATH 1 (Express)
private const int ThresholdLargeGb = 80;  // >80GB → PATH 3 (Batch)
// Passed as env vars to the Size Estimator Lambda at deploy time.
```

---

## Column Mapping Configuration

### Three-Tier Mapping System

| Layer | Location | Purpose | Priority |
|-------|----------|---------|----------|
| **Auto-mapping** | `column_mapper.py` | 6-strategy automatic matching (SQL → Iceberg) | Lowest |
| **Generated config** | `mapping_config.json` | Auto-generated, user-reviewable | Medium |
| **Runtime overrides** | S3 (`config/{tenant}/mapping_overrides.json`) | Per-tenant overrides | Highest |

For ingestion the mapping direction is **SQL column → Iceberg column** (reverse of extraction).

### Auto-Mapping Strategies

```
Strategy 1: EXACT            "contractid"   → "contractid"     confidence 1.0
Strategy 2: CASE-INSENSITIVE "ContractID"   → "contractid"     confidence 0.95
Strategy 3: NAMING-CONV      "contract_id"  → "ContractId"     confidence 0.9
Strategy 4: PREFIX-STRIP     "tbl_contract" → "contract"       confidence 0.8
Strategy 5: ABBREVIATION     "cntrt_id"     → "contract_id"    confidence 0.7
Strategy 6: FUZZY            "contrat_id"   → "contract_id"    confidence 0.5-0.7
```

### Schema Evolution

If a source table in the restored database has columns not present in the destination Iceberg table, the writer automatically evolves the schema:

```python
# IcebergWriter detects new columns:
existing_schema = iceberg_table.schema()
incoming_schema = arrow_batch.schema
new_columns = [f for f in incoming_schema if f.name not in existing_schema.names]

if new_columns:
    with iceberg_table.update_schema() as update:
        for col in new_columns:
            update.add_column(col.name, iceberg_type_for(col.type))
    logger.info("Evolved schema: added %d columns", len(new_columns))
```

---

## Structured JSON Logging

All log output is structured JSON for CloudWatch Logs Insights + Dynatrace:

```json
{
  "timestamp": "2026-03-10T14:23:01Z",
  "level": "INFO",
  "logger": "src.fargate.ingestion_entrypoint",
  "tenant_id": "dclegend01",
  "activity_id": "act-abc123",
  "message": "SID L1: tContract — 12,450 rows transformed, SID range 1000000-1012449"
}
```

`_ContextFilter` injects `tenant_id` and `activity_id` into every log record globally. All child modules (sql_reader, iceberg_writer, sid/transformer, etc.) automatically inherit this context.

---

## SQL Server Standard Licensing — SPLA

PATH 2 and PATH 3 use SQL Server Standard (`MSSQL_PID=Standard`). Commercial use in containers requires a **SPLA (Services Provider License Agreement)** from Microsoft.

**PATH 1 (Express) is completely free — no SPLA needed.**

### Why SPLA is Required

- `MSSQL_PID=Standard` activates SQL Server Standard edition inside the container
- Microsoft requires SPLA for any hosted/SaaS service using SQL Server Standard
- Using Standard without a valid SPLA agreement is a license violation

### What is SPLA?

| Attribute | Details |
|-----------|---------|
| **Full name** | Services Provider License Agreement |
| **Publisher** | Microsoft |
| **Model** | Monthly subscription — report usage each month, pay accordingly |
| **Unit of measure** | Per physical processor core |
| **Scope** | Containerized SQL Server on ECS Fargate and AWS Batch EC2 |
| **Minimum term** | Monthly (no upfront commitment) |

### How to Procure SPLA

#### Step 1 — Find a Microsoft Licensing Solution Provider (LSP)

You must sign SPLA through an authorized Microsoft LSP (also called SPLA Reseller or LAR):

- **Crayon** — https://www.crayon.com
- **SHI International** — https://www.shi.com
- **CDW** — https://www.cdw.com
- **SoftwareONE** — https://www.softwareone.com

Microsoft SPLA program: https://www.microsoft.com/en-us/licensing/licensing-programs/spla-program

#### Step 2 — Sign the SPLA Agreement

1. Provide company details and intended use (hosted data services).
2. Sign the SPLA enrollment (3-year with monthly reporting).
3. You receive a SPLA agreement number — no product key needed; compliance is usage-based.

> The `MSSQL_PID=Standard` env var activates Standard. Compliance audits verify you hold a valid SPLA for the cores used.

#### Step 3 — Monthly Usage Reporting

Report the **maximum physical core count** on which SQL Server Standard ran each month:

| Path | Container Config | Core Count to Report |
|------|-----------------|---------------------|
| PATH 2 (Fargate) | 8 vCPU | **4 physical cores** (8 vCPU ÷ 2 for HyperThreading) |
| PATH 3 (Batch EC2) | Varies by instance | **EC2 physical cores** (check spec) |

> `r5.2xlarge` = 8 vCPU = 4 physical cores. `r5.4xlarge` = 16 vCPU = 8 physical cores.

Report **peak concurrent cores per month**, not hours billed.

#### Step 4 — Pay Monthly Invoice

| Edition | Approximate SPLA Rate |
|---------|----------------------|
| SQL Server Standard | **~$50–$80 per core per month** |

### License by Path

| Path | Size | `MSSQL_PID` | License | Action Required |
|------|------|-------------|---------|----------------|
| PATH 1 | ≤9 GB | `Express` | None — FREE | No action |
| PATH 2 | 9-80 GB | `Standard` | SPLA required | Sign SPLA; report 4 cores/task/month |
| PATH 3 | >80 GB | `Standard` | SPLA required | Sign SPLA; report EC2 physical cores/month |

### Alternative Options

| Option | Notes |
|--------|-------|
| **SQL Server Developer** (`MSSQL_PID=Developer`) | FREE — but **only for dev/test, not production** |
| **BYOL perpetal license** | Requires Software Assurance (SA) for container use |
| **License Included EC2 AMI** | Only for EC2 bare-metal/VM — not applicable to containers |

---

## Cost Analysis (Ingestion)

### Per-Run Costs

#### PATH 1: Fargate + Express (≤9GB)

| Component | Cost |
|-----------|------|
| Fargate (4 vCPU, 8GB, ~30min) | $0.25 |
| Ephemeral Storage (200GB) | $0.04 |
| S3 Transfer (download from tenant) | $0.01-0.10 |
| SQL Express License | $0 (FREE) |
| SPLA | $0 |
| **Total per ingestion run** | **~$0.30-0.40** |

#### PATH 2: Fargate + Standard (9-80GB)

| Component | Cost |
|-----------|------|
| Fargate (8 vCPU, 16GB, ~45min) | $0.75 |
| Ephemeral Storage (200GB) | $0.04 |
| S3 Transfer + SID API calls | $0.05-0.30 |
| SPLA (4 cores × ~$0.09/hr) | ~$0.36 |
| **Total per ingestion run** | **~$1.20-1.45** |

#### PATH 3: Batch EC2 + Standard (>80GB)

| Component | Cost |
|-----------|------|
| EC2 (r5.2xlarge, ~90min) | $0.50 |
| EBS Storage (500GB gp3) | $0.08 |
| S3 Transfer + SID API calls | $0.10-0.50 |
| SPLA (4 cores × ~$0.09/hr) | ~$0.72 |
| **Total per ingestion run** | **~$1.40-1.80** |

### Monthly (300 tenants, 1 ingestion/week each)

Assuming 70% PATH 1, 20% PATH 2, 10% PATH 3:

| Component | Monthly Cost |
|-----------|-------------|
| PATH 1 — Express Fargate (840/mo) | $250-340 |
| PATH 2 — Standard Fargate (240/mo) | $290-350 |
| PATH 3 — Batch EC2 (120/mo) | $170-220 |
| S3 Storage | $25-50 |
| SPLA (PATH 2+3) | $400-800 |
| **Total monthly** | **~$1,135-1,760** |

**Cost per tenant/month: ~$3.80-5.90**

---

## Testing

### Prerequisites

```powershell
cd c:\Github\Rnd\DataIngestion
.\.venv\Scripts\Activate.ps1

# Ensure .env configured:
# ICEBERG_NAMESPACE=rs_cdkdev_dclegend01_exposure_db
# SQLSERVER_HOST, PORT, USERNAME, PASSWORD
```

### Local Ingestion Tests

```powershell
# Ingest from S3 MDF (all tables, 100 row limit)
python scripts/test_ingestion_flow.py \
  --s3-uri s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf \
  --target-namespace rs_cdkdev_dclegend01_exposure_db \
  --limit 100

# Specific tables only
python scripts/test_ingestion_flow.py \
  --s3-uri s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf \
  --target-namespace rs_cdkdev_dclegend01_exposure_db \
  --tables tcontract tlocation \
  --limit 100

# Full data, no limit
python scripts/test_ingestion_flow.py \
  --s3-uri s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf \
  --target-namespace rs_cdkdev_dclegend01_exposure_db

# Test append mode (vs overwrite — default is append)
python scripts/test_ingestion_flow.py \
  --s3-uri s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf \
  --target-namespace rs_cdkdev_dclegend01_exposure_db \
  --write-mode append

# Keep temp database after test (for inspection)
python scripts/test_ingestion_flow.py \
  --s3-uri s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf \
  --target-namespace rs_cdkdev_dclegend01_exposure_db \
  --no-cleanup

# Ingest from already-attached SQL Server database
python scripts/test_ingestion_flow.py \
  --source-db AIRExposure_test \
  --target-namespace rs_cdkdev_dclegend01_exposure_db \
  --limit 100
```

**Expected Output:**
```
INGESTION TEST (from S3 MDF)
============================
S3 URI: s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf
Target Namespace: rs_cdkdev_dclegend01_exposure_db
Database name: ingest_test_1772286164017
---
Downloaded: 8.00 MB
Database attached: ingest_test_1772286164017
---
SID L0: tExposureSet (1,200 rows) — SID range 1000000-1001199
SID L1: tContract (4,500 rows) — SID range 1001200-1005699
Processing: tLocation (1,800 rows)
  Evolving schema: adding 2 columns
  ✓ Written 1,800 rows
...
INGESTION COMPLETE
  Tables: 7 / 12
  Rows: 12,400
---
Database dropped: ingest_test_1772286164017
```

### Round-Trip Test (Extract → Ingest → Verify)

```powershell
# Step 1: Extract from source namespace
python scripts/local_test.py \
  --target-bucket rks-sd-virusscan \
  --target-prefix roundtrip-test/ \
  --tables tcontract tlocation \
  --limit 1000

# Step 2: Ingest to target namespace
python scripts/test_ingestion_flow.py \
  --s3-uri s3://rks-sd-virusscan/roundtrip-test/AIRExposure_test.mdf \
  --target-namespace rs_cdkdev_dclegend01_exposure_db \
  --tables tcontract tlocation

# Step 3: Verify rows written to Iceberg
python -c "
from src.core.iceberg_reader import IcebergReader
from src.core.config import load_config
cfg = load_config()
reader = IcebergReader(cfg.iceberg)
reader.namespace = 'rs_cdkdev_dclegend01_exposure_db'
for row in reader.stream_table('tcontract', limit=5):
    print(row.to_pydict())
"
```

### Key Test Scenarios

| Scenario | Command |
|----------|---------|
| **Small artifact (≤9GB)** | `--limit 100` (small dataset) |
| **MDF artifact type** | `--s3-uri ...mdf` |
| **BAK artifact type** | `--s3-uri ...bak` |
| **Append mode** | `--write-mode append` |
| **Schema evolution** | `--tables tlocation` (tests column addition) |
| **Debug mode** | Set `LOG_LEVEL=DEBUG` in .env |
| **Keep DB for inspection** | `--no-cleanup` |

### Unit Tests

```
tests/unit/
  test_sql_reader.py             # Arrow batch streaming, type mapping
  test_iceberg_writer.py         # Append/overwrite, schema evolution
  test_artifact_restorer.py      # RESTORE SQL generation, attach logic
  test_ingestion_entrypoint.py   # Full orchestration, SID transform integration
  test_column_mapper.py          # SQL→Iceberg mapping strategies
```

### Integration Tests

```
tests/integration/
  test_ingestion_flow.py         # Full ingestion with real SQL Server container
  test_round_trip.py             # Extract → ingest → verify data integrity
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
| **MDF access denied** | Check `TENANT_ROLE_ARN` and STS credentials |
| **RESTORE failed** | Verify artifact not corrupted — check Expected vs actual SHA256 |
| **SID API timeout** | Check `SID_API_URL` and network connectivity from container |
| **Table not found in destination** | Table skipped as "table_not_found_in_destination" — expected if namespace is new |
| **Schema mismatch** | Writer auto-evolves schema — check logs for "Evolved schema" |
| **Database not attached** | Check `SQLSERVER_DATA_SHARE_PATH` in .env for local testing |

---

## Implementation Timeline

**Team:** 3 Engineers + Architect/Manager (oversight, code reviews, design decisions)  
**Scope:** Ingestion pipeline modules, Validation, Ingestion API, Okta SSO, Multi-Tenancy, SID override flow, Angular UI, CDK stacks, GitHub Actions pipelines, Staging, Production hardening  
**Total: 5 weeks**

---

### Week 1: Core Pipeline Modules + CDK Foundation

| Day | Engineer 1 (Backend) | Engineer 2 (Infra / CDK) | Engineer 3 (Pipeline / Testing) | Architect/Manager |
|-----|----------------------|--------------------------|----------------------------------|-------------------|
| Mon | Complete `validation.py` — Lambda-based row-count post-ingest verification, Iceberg vs SQL Server count diff alert, configurable tolerance threshold | VPC CDK construct — private subnets, NAT Gateway, VPC endpoints for SFN, S3, Secrets Manager, ECR, Glue, LakeFormation | Review and harden `artifact_restorer.py` — edge cases: corrupt BAK, partial MDF attach, file-not-found vs checksum failure distinction | Kick-off, assign tracks, review pipeline contract (SFN input schema), confirm Iceberg namespace per-tenant strategy, ADR-001 |
| Tue | `ingestion_entrypoint.py` hardening — structured error handling per stage (download → restore → read → SID → write), idempotent re-entry on SFN retry | ECR CDK — `data-pipeline-express/standard/batch` repositories, lifecycle policies, cross-account pull permissions for staging/prod | `sql_reader.py` edge cases — NULL handling, IDENTITY columns, computed columns, BIT → boolean, DATETIME2 precision, schema drift detection | Review `ingestion_entrypoint.py` error model, confirm retry-safe stages, sign-off idempotency design |
| Wed | `iceberg_writer.py` hardening — schema evolution (add columns, widen types), MERGE vs OVERWRITE mode, partition spec alignment with existing table | ECS task definition CDK — Fargate task for `data-pipeline-express` (PATH 1) + `data-pipeline-standard` (PATH 2), resource allocations (CPU/mem per path) | AWS Batch CDK — `data-pipeline-batch` job definition (PATH 3), Compute Environment (r5.4xlarge, Spot with On-Demand fallback), Job Queue, retry policy | Review Batch CE configuration, Spot interruption handling, SFN `.sync:2` Batch integration |
| Thu | `MeasureArtifactSize` Lambda hardening — handle multi-file artifacts (sum of all BAK parts), return size in GB with 2 decimal precision, thresholds cross-check | Step Functions CDK — `get_three_path_ingestion_definition()`: Choice state on `$.selected_path`, `MSSQL_PID` env override per path, Batch `.sync:2` integration, SFN execution role | Unit tests — `test_artifact_restorer.py`, `test_sql_reader.py`, `test_iceberg_writer.py`, `test_measure_artifact_size.py` with mocked AWS clients | Review Step Functions ASL definition, validate Choice conditions, confirm SFN execution role least-privilege |
| Fri | `s3_uploader.py` — extend `download_artifact()` with multipart download for large BAK files (> 500 MB), retry on `ConnectionReset`, checksum verify post-download | Secrets Manager CDK — SQL SA password per tenant (`/sde/{tenantId}/sqlserver/password`), SID API token (`/sde/{stage}/sid/app-token`), rotation policy | Integration test scaffold — `tests/integration/test_ingestion_pipeline.py` using docker-compose (SQL Server + localstack S3 + Iceberg REST catalog) | Week 1 retrospective, review unit test coverage report (target ≥ 80%), adjust Week 2 plan |

---

### Week 2: Ingestion API + Okta SSO + Multi-Tenancy

| Day | Engineer 1 (Backend API) | Engineer 2 (Infra / CDK) | Engineer 3 (Pipeline / Angular) | Architect/Manager |
|-----|--------------------------|--------------------------|----------------------------------|-------------------|
| Mon | FastAPI ingestion service — `POST /ingestions/presign` (S3 pre-signed URL scoped to tenant prefix, 15-min expiry), `POST /ingestions` SFN launcher, `GET /ingestions/{id}` status | CDK stack for API Gateway (REST, regional) + ECS Fargate service `ingestion-api`, task definition with environment injection (`TENANT_ID`, `SFN_ARN`, `STAGE`) | Bootstrap Angular ingestion module (nx workspace lazy-loaded feature module), routing, `IngestionsModule`, placeholder components for upload, history, SID panels | Kick-off Week 2, review API surface, approve OpenAPI spec, confirm Okta app registration naming convention |
| Tue | Okta JWT authorizer Lambda — RS256 token validation, extract `tenantId` from Okta `groups` claim, propagate via `x-tenant-id` header, cache JWKS (10 min TTL) | Okta app registration CDK module — ingestion OIDC client config stored in SSM Parameter Store (`/sde/{stage}/okta/ingestion-client-id`), authorizer Lambda attached to API Gateway | Okta Angular SDK integration — `OktaAuthModule`, `AuthGuard`, `TenantGuard`, `authInterceptor` (attaches Bearer token + `x-tenant-id` to all API calls), silent token renew | Review Okta token scopes (`ingestion:read`, `ingestion:write`, `ingestion:admin`), confirm `groups` claim mapping to tenants |
| Wed | Multi-tenant ingestion middleware — validate `artifact_s3_key` prefix matches `tenants/{tenantId}/`, reject cross-tenant path traversal, tenant registry DynamoDB lookup | IAM CDK — ingestion ECS task role: `s3:GetObject` scoped to `/tenants/{tenantId}/ingestions/*`, `athena:*` on tenant namespace, `glue:*` scoped to tenant DB, `SFN:StartExecution` | Artifact upload form (Angular) — file picker (`.bak`, `.mdf` only), pre-signed URL upload with progress bar (`XHR + onprogress`), SHA-256 checksum computed client-side via Web Crypto API | Review multi-tenant data isolation model, confirm IAM session policy prevents cross-tenant S3 access |
| Thu | SID override API — `POST /ingestions/{id}/sid-overrides`: accepts per-table SID map, validates FK cascade order (topological sort), stores in DynamoDB for pipeline pickup | Cross-account IAM CDK — ingestion task assumes tenant-specific Iceberg write role via STS, `lakeformation:GrantPermissions` scoped to tenant database, trust policy per tenant AWS account | Ingestion status polling component — live state machine step indicator (`MeasureArtifactSize` → three-path label → restore → read → SID → write), estimated completion time display | Review cross-account IAM trust, LakeFormation permission model per tenant, sign-off on STS AssumeRole chain |
| Fri | Exposure registration API wrapper — `POST /ingestions/{id}/exposures` calls downstream Exposure API, persists registered exposure IDs, returns `exposure_ids[]` | CloudFront + S3 SPA hosting CDK — ingestion Angular app, OAC policy, custom domain (`ingestion.Synergy Data Exchange.internal`), ACM certificate, HTTPS-only, HSTS header | Angular SID conflict resolution UI — diff view (source SID vs target SID per table), accept / override / reject per row, batch approve all, FK dependency warning banners | Week 2 retrospective, review API + UI integration progress, unblock SID UX decisions |

---

### Week 3: Angular UI Completion + SID Pipeline + CI/CD Pipelines

| Day | Engineer 1 (Backend API) | Engineer 2 (Infra / CDK) | Engineer 3 (Pipeline / Angular) | Architect/Manager |
|-----|--------------------------|--------------------------|----------------------------------|-------------------|
| Mon | Schema version detection endpoint — `GET /ingestions/schema-version`: reads artifact metadata header/sidecar JSON, returns compatibility verdict (COMPATIBLE / BREAKING / UNKNOWN) | GitHub Actions reusable workflow `_build-and-push.yml` — inputs: `service-name`, `ecr-repo`; steps: Docker build, ECR login, tag (`${SHA}-${env}`), ECR push, output image URI | Ingestion history dashboard — filterable/sortable table (tenant, date, path label, status badge, SID conflict flag, row count delta), pagination, CSV export | Review GitHub Actions workflow structure, approve secrets management via OIDC (no long-lived AWS keys), confirm ECR lifecycle policies |
| Tue | `ingestion_entrypoint.py` — SID transformer integration: load override map from DynamoDB, run `SIDTransformer.transform()`, cascade FK-safe ordering, log per-table SID allocation counts | GitHub Actions `ingestion-api-ci.yml` — uses `_build-and-push.yml`, triggers on PR to `main`: lint → mypy → pytest (unit + integration) → Docker build → ECR push; coverage gate ≥ 80% | Angular Iceberg row-count diff viewer — post-ingestion comparison table: source SQL Server row count vs Iceberg row count per table, ± delta badge, drill-down to validation Lambda output | Review SID transformer integration in entrypoint, confirm idempotency (re-run with same override map produces same SIDs) |
| Wed | Error catalog — full `ING-001` through `ING-060`: checksum mismatch, SID conflict unresolved, schema BREAKING, Iceberg write OOM, Batch timeout, LakeFormation permission denied; each maps to HTTP status + remediation hint | GitHub Actions `ingestion-infra-cd.yml` — uses `_deploy-cdk.yml`: CDK diff posted as PR comment, manual approval gate for staging + prod, sequential deploy (dev → staging → prod) | Angular E2E tests (Playwright) — upload BAK → trigger → SID override → approve → poll → Iceberg diff viewer, error paths (checksum fail, SID conflict, schema mismatch) | Review CD promotion gates, confirm manual approval owners (architect required for prod), approve dev environment deployment |
| Thu | Integration test suite (`tests/integration/test_ingestion_api.py`) — full flow: presign → upload → `StartExecution` → poll → SID override → expose → validate Iceberg row count matches SQL source | GitHub Actions `angular-ci.yml` — nx affected lint → test → build on PR; `angular-cd.yml` — nx affected build (`--prod`) → S3 sync → CloudFront invalidation (scoped to ingestion distribution) | `tests/integration/test_ingestion_pipeline.py` — real SQL Server container + localstack: restore BAK, SID transform, write Iceberg, count rows, verify FK consistency in Iceberg output | Validate dev deployment, smoke test all three paths (PATH 1 with 500 MB BAK, PATH 2 with 15 GB BAK stub, PATH 3 stub), sign-off on dev sign-off checklist |
| Fri | Performance profiling — ingestion API: 10 concurrent presign requests (< 100ms p99), SFN launch latency (< 500ms), `MeasureArtifactSize` Lambda cold start characterisation | Monitoring CDK — CloudWatch dashboard (SFN per-path executions, Fargate CPU/mem, Batch GPU/mem, S3 upload PutObject rate, `MeasureArtifactSize` duration), SNS alarms → Slack/email | Angular responsive layout polish, loading skeleton screens, WCAG 2.1 AA accessibility audit (keyboard nav, ARIA labels, colour contrast), mobile breakpoints | Week 3 retrospective, review CI/CD pipeline health, unit + integration test coverage summary, identify Week 4 focus areas |

---

### Week 4: Integration Testing + Staging Environment

| Day | Engineer 1 (Backend API) | Engineer 2 (Infra / CDK) | Engineer 3 (Pipeline / Angular) | Architect/Manager |
|-----|--------------------------|--------------------------|----------------------------------|-------------------|
| Mon | Load test setup (Locust) — 20 concurrent ingestion triggers across 5 tenants, artifact sizes: 1 GB (PATH 1), 20 GB (PATH 2), 100 GB (PATH 3), tenant isolation probe (attempt cross-tenant presign URL) | Deploy full ingestion stack to staging — `CDK deploy --context env=staging`: VPC, ECR, ECS, Batch, API GW, Lambda, SFN, CloudFront, DynamoDB; validate Okta callback URLs, ACM cert validation | Angular staging smoke test — Okta login with staging IdP, upload real 200 MB BAK to staging S3 via presign, trigger ingestion, poll status, verify Iceberg row count in staging Glue catalog | Review staging deployment checklist, confirm `MeasureArtifactSize` thresholds, validate three-path routing decision on staging SFN console |
| Tue | Load test execution — PATH 1: 1 GB × 10 concurrent (target < 8 min), PATH 2: 20 GB × 5 concurrent (target < 40 min), PATH 3: 100 GB × 2 concurrent (target < 3 hours); profile SFN execution times + API p99 | Staging environment validation — X-Ray service map confirms full trace (API GW → authorizer → ECS → SFN → Fargate/Batch), CloudWatch alarms tested (trigger manual alarm, confirm SNS fires), Batch CE autoscaling validated | Playwright E2E suite on staging — all critical user journeys: login → upload → trigger → SID override → approve → poll → diff viewer, plus error scenarios (corrupt BAK, SID conflict, schema mismatch) | Validate load test results against SLAs, review X-Ray service map with team, confirm alarm coverage |
| Wed | Bug-fix sprint — address issues from load test: DynamoDB hot partitions (add DAX or caching), STS session caching in Tenant Context, SFN execution history API throttle on high-frequency polling | Staging hardening — VPC Flow Logs enabled, CloudTrail data events for ingestion S3 prefix, WAF rules (rate limit 100 req/min per IP, SQL injection, XSS) attached to API Gateway in staging | Bug-fix sprint — address Angular issues from E2E run: pre-signed upload progress bar on large files, SID diff view performance on 100+ tables, Okta session expiry during long Batch ingestion (silent renew) | Triage load test + E2E findings, prioritise fixes, escalate any SLA-breaking issues |
| Thu | Round-trip integration test — full pipeline: Extraction (Iceberg → MDF) → upload to ingestion S3 → Ingestion (MDF → Iceberg) → row count match + SID integrity check + FK consistency verification | Staging sign-off infrastructure checklist — CloudWatch dashboard reviewed, alarms calibrated to staging baseline, Runbook v1 drafted, DR failover procedure documented | Playwright regression suite re-run post bug-fixes — all E2E scenarios green, performance: UI time-to-interactive < 3s, upload form to trigger confirmed < 2s | Review round-trip test results, staging sign-off decision, prepare production deployment plan |
| Fri | Finalise API documentation — OpenAPI spec reviewed, Postman collection exported, error catalog complete, `README.md` for ingestion API updated | Staging monitoring tuning — CloudWatch alarm thresholds adjusted from load test baseline data, log retention policies set (90 days staging, 365 days prod), cost optimisation review | Angular build optimisation — bundle size audit (`ng build --stats-json` + webpack-bundle-analyzer), lazy-loading verified, CDN cache headers validated on CloudFront | Week 4 retrospective, production readiness checklist review, Week 5 go/no-go planning |

---

### Week 5: Production Hardening + Go-Live

| Day | Engineer 1 (Backend API) | Engineer 2 (Infra / CDK) | Engineer 3 (Pipeline / Angular) | Architect/Manager |
|-----|--------------------------|--------------------------|----------------------------------|-------------------|
| Mon | Schema version detection — full implementation if client schema markers are ready; ingestion dry-run mode — `POST /ingestions?dryRun=true`: validate checksum + schema + SID feasibility without writing to Iceberg | Production CDK stack review — separate AWS prod account, stricter SCPs, WAF with IP allowlist for known tenant CIDR ranges, Shield Standard enabled, CloudTrail organisation trail | Dynatrace / Datadog RUM injection into Angular app — session replay (PII masking for tenant data), Core Web Vitals tracking, error boundary components with correlation ID display | Security review — OWASP Top 10 against ingestion API (JWT expiry, injection in filename parameter, pre-signed URL scope validation), pen test findings review |
| Tue | Final security hardening — rate limiting per tenant (API Gateway usage plans tied to tenant registry), `Content-Type` validation on presign endpoint (reject non `.bak`/`.mdf`), MDF malware scan backlog item documented | Production deployment — `CDK deploy --context env=prod` with feature flag `INGESTION_API_ENABLED=false` (dark launch), verify all stacks healthy, Okta prod app registration activated | End-to-end production smoke test with feature flag off — confirm no public traffic reaches ingestion API, Okta prod login tested with admin account in isolated browser profile | Security sign-off, production architecture review, go/no-go criteria checklist review |
| Wed | Production smoke test with feature flag on (`INGESTION_API_ENABLED=true`) — one real tenant, one real BAK artifact (≤ 1 GB PATH 1), Iceberg row count match confirmed, SID allocation verified in prod Glue catalog | Production monitoring live — CloudWatch dashboard confirmed showing prod metrics, SNS alarms pointing to prod on-call SNS topic, PagerDuty schedule activated, X-Ray sampling set to 5% prod rate | Playwright production smoke test — login with prod Okta (invited test user), upload artifact, trigger ingestion PATH 1, poll to completion, verify diff viewer shows matching row count | Go/no-go final decision, stakeholder demo (PM, client success, engineering lead) |
| Thu | Bug-fix / stabilisation buffer — address any production issues surfaced in smoke test | Hot-fix pipeline ready — `ingestion-api-hotfix.yml` workflow: branch from `hotfix/*`, fast-track CI (unit tests only, skip E2E), deploy to prod on architect approval | Knowledge transfer session 1 — Angular ingestion UI walkthrough, upload component internals, SID conflict resolution UX guide, accessibility testing guide | Stakeholder sign-off, close sprint |
| Fri | Knowledge transfer session 2 — ingestion API internals, SID transformer design, `MeasureArtifactSize` Lambda, Tenant Context, environmental configuration guide | On-call runbook published — alert playbooks for each `ING-001` to `ING-060` error code, escalation path, rollback procedure (`INGESTION_API_ENABLED=false` feature flag) | Final documentation — SID allocation runbook, Iceberg schema evolution guide, multi-tenant namespace management guide, Confluence space structure updated | Final sign-off, full retrospective across all 5 weeks, lessons learned documented |

---

### Summary

| Week | Focus | Key Deliverables |
|------|-------|-----------------|
| **1** | Core pipeline modules + CDK foundation | `validation.py`, hardened `ingestion_entrypoint.py`, `iceberg_writer.py` schema evolution, Batch CDK, SFN three-path CDK, unit tests ≥ 80% |
| **2** | Ingestion API + Okta + Multi-Tenancy | FastAPI service, JWT authorizer Lambda, multi-tenant middleware, SID override API, Angular bootstrap + Okta, cross-account IAM |
| **3** | Angular UI + SID pipeline + CI/CD | SID transformer integration, history dashboard, diff viewer, Playwright E2E, `ingestion-api-ci.yml` + `ingestion-infra-cd.yml`, dev deployment |
| **4** | Integration testing + Staging | Load test (all 3 paths), round-trip extraction→ingestion test, E2E on staging, staging sign-off, Runbook v1 |
| **5** | Production hardening + Go-Live | Dry-run mode, RUM, production CDK deploy, feature-flag dark launch, stakeholder demo, on-call runbook, knowledge transfer |

**Total: 5 weeks**

---

## Security Considerations

### Multi-Tenant Isolation

```
SHARED: ECS Cluster, ECR repos, SFN state machine, Batch CE/Queue
RUNTIME ISOLATION (per job):
  • TaskRoleArn scoped to tenant's S3 prefix only
  • IAM session policy: s3:GetObject restricted to /tenants/{tenantId}/*
  • Fargate PATH 1/2: Firecracker micro-VM (dedicated kernel per task)
  • Batch PATH 3: Docker namespace isolation
  • Ephemeral container destroyed after job
  • No shared SQL Server state between tenants
```

### SOC2 Controls

| Control | Implementation |
|---------|---------------|
| CC6.1 Logical Access | IAM session policy per job, tenant-scoped S3 paths |
| CC6.3 Least Privilege | TaskRole can only read from tenant's source S3 prefix |
| CC6.5 Data Disposal | Ephemeral Fargate/Batch containers destroyed post-job; DB dropped |
| CC6.7 Encryption at Rest | S3 SSE-KMS, EBS encryption |
| CC6.7 Encryption in Transit | TLS everywhere (S3, Glue, SID API, Secrets Manager) |
| CC7.2 Audit Trail | CloudWatch logs tagged with tenantId + activityId |

### Ingestion-Specific Security
- Verify artifact checksum (SHA256) before restoring — prevents processing tampered files
- Sanitize `database_name` to prevent SQL injection in `CREATE DATABASE` / `RESTORE` statements
- Iceberg writer only writes to tenant's own namespace (enforced by `tenant_context.glue_database`)
- Table ingestion can be whitelisted via `tables` parameter in the SFN input — prevents ingesting unexpected tables
- Uploaded MDF files SHOULD be scanned for malware before restore (not yet implemented — backlog item)

### Secrets Management
- SQL Server SA password from Secrets Manager (`/sde/{tenantId}/sqlserver/password`)
- SID API tokens from Secrets Manager (`/sde/{stage}/sid/app-token`)
- Tenant OAuth credentials from Tenant Context Lambda (cached 50 minutes)

---

## Related Documents

| Document | Description |
|----------|-------------|
| [implementation_plan_extraction.md](implementation_plan_extraction.md) | Extraction pipeline plan (Iceberg → Tenant S3) |
| [implementation_plan_bidirectional.md](implementation_plan_bidirectional.md) | Shared infrastructure overview |
| [sequence_diagram.md](sequence_diagram.md) | Mermaid sequence diagrams for both flows |
| [SOW_deliverables_tracker.md](SOW_deliverables_tracker.md) | Full deliverable status tracking |
