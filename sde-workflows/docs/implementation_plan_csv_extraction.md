# Implementation Plan: CSV Extraction Format

## Overview

This plan extends the existing extraction pipeline to support **CSV** as a third artifact type alongside `bak` and `mdf`. The implementation reuses the existing Express Fargate container and adds CSV-specific dispatch logic — zero new AWS infrastructure components are required.

**Caller input change**: `artifact_type` field now accepts `"csv"` in addition to `"bak"`, `"mdf"`, and `"both"`.

---

## Design Decisions

### Why Reuse Express Container (Not a New Dockerfile)

| Factor | Reuse Express | New Dockerfile.csv |
|---|---|---|
| SQL Server installed | Yes (never started for CSV) | No |
| SPLA obligation | None — Express is always free | None |
| Resources (appsettings.json) | 8 vCPU / 60 GiB / 200 GiB EBS | Would be same or smaller |
| New ECR repository | Not needed | Required |
| New ECS task definition | Not needed | Required |
| New CDK config | Not needed | Required |
| `entrypoint.sh` changes | Guards added for SQL Server blocks | Not needed |

Express is already configured at 8 vCPU / 60 GiB (`ExpressCpu: 8192`, `ExpressMemory: 61440` in `appsettings.json`), which is more than sufficient for streaming Arrow batches. The only tradeoff is the larger SQL Server image pull time (~60s vs ~6s for a slim image) — acceptable given the infrastructure savings.

### Why Keep EstimateExtractionSize for CSV

The `EstimateExtractionSize` Lambda does two things:
1. Estimates database size to pick PATH 1/2/3
2. Fetches `sde_context` (tenant role, Glue database, bucket name) needed by the container

For CSV, step 1 is irrelevant (no SQL Server, no size cap) but step 2 is still required. Short-circuiting the Lambda would require a separate tenant-context resolution step — more complexity than the ~5–10s Athena query cost.

### No SQL Server Size Limitation for CSV

Current size tiers exist solely because of SQL Server:
- PATH 1 Express: 10 GB SQL Server Express hard limit
- PATH 2 Standard: ~80 GB practical limit (Fargate ephemeral storage for .mdf/.ldf)
- PATH 3 Batch: Unlimited (large EBS volume)

For CSV, data flows **Iceberg (S3) → Arrow batches (memory) → CSV → S3 multipart**. There is no local database. Peak memory is bounded by `BATCH_SIZE` rows, not total dataset size. A 500 GB extraction and a 5 GB extraction use the same memory footprint. **The 3-tier routing is bypassed for CSV** — it always runs on Express regardless of estimated size.

---

## Architecture

### Current Extraction Flow (BAK/MDF)

```
Input → ValidateInput → EstimateExtractionSize → DetermineExtractionPath
                                                      ├── EXPRESS  → RunExpressTask
                                                      ├── STANDARD → RunStandardTask
                                                      └── BATCH    → RunBatchJob
```

### New Extraction Flow (CSV Added)

```
Input → ValidateInput → EstimateExtractionSize → DetermineExtractionPath
                                                      ├── artifact_type=="csv" → RunExpressTask  ← NEW (first condition)
                                                      ├── EXPRESS  → RunExpressTask              (unchanged)
                                                      ├── STANDARD → RunStandardTask             (unchanged)
                                                      └── BATCH    → RunBatchJob                 (unchanged)
```

Inside the Express container, `entrypoint.sh` detects `ARTIFACT_TYPE=csv` and skips SQL Server startup — routing to `csv_entrypoint.py` instead of `entrypoint.py`.

### CSV Container Execution Flow

```
10%  PROVISIONING      → Parse input, load config, init IcebergReader
20%  LOADING           → Stream per-table: Iceberg → Arrow batches → CSV → S3 multipart
                          (same report_table_progress callback used — per-table progress updates)
70%  VALIDATING        → Row-count cross-check: rows written to CSV == rows read from Iceberg
80%  GENERATING_ARTIFACT → Finalise S3 multipart uploads, compute SHA256 checksums
90%  UPLOADING         → Upload manifest.json to S3
100% COMPLETED         → manifest written, reporter.flush()
  0% FAILED            → on any exception (same error path as BAK/MDF)
```

### S3 Output Structure (Consistent with BAK/MDF Pattern)

```
s3://{tenant_bucket}/{s3_output_path}/{activity_id}/
    TContract.csv
    TLocation.csv
    TExposureSet.csv
    TLocFeature.csv
    ... (one CSV file per table)
    manifest.json
```

---

## File Change Matrix

| File | Change | Description |
|---|---|---|
| `docker/entrypoint.sh` | Modify | Guard SQL Server startup/shutdown blocks; add CSV dispatch branch |
| `src/fargate/csv_entrypoint.py` | **NEW** | Full CSV extraction flow without any SQL Server imports |
| `src/fargate/csv_generator.py` | **NEW** | Iceberg → Arrow → CSV → S3 multipart upload per table |
| `src/models/execution_plan.py` | Modify | Add `CSV = "csv"` to `ArtifactType` enum |
| `cdk/src/EES.AP.SDE.CDK/Stacks/SDEStepFunctionsStack.cs` | Modify | Add one `.When()` condition for CSV in `DetermineExtractionPath` |
| `cdk/src/EES.AP.SDE.CDK/lambda/spla_tracker.py` | Modify | Add `artifact_type` field to DynamoDB SPLA record |
| `tests/unit/test_execution_plan.py` | Modify | Add `ArtifactType.CSV` test case |
| `tests/unit/test_csv_generator.py` | **NEW** | Unit tests for Iceberg→CSV conversion with mock Arrow data |

**Not changed**: `SDEEcrStack.cs`, `SDEFargateStack.cs`, `SDEConfiguration.cs`, `appsettings.json`, `Dockerfile.express`, `entrypoint.py`, `artifact_generator.py`, `check_records_lambda.py`, `progress_reporter.py`, `manifest.py`, `size_estimator.py`, `spla_monthly_reporter.py`, `SDEMonitoringStack.cs`, `SDEBatchStack.cs`

---

## Detailed Changes

### 1. `docker/entrypoint.sh`

Wrap the two SQL Server sections in `ARTIFACT_TYPE != "csv"` guards and add a CSV dispatch branch in the pipeline mode switch.

**Block 1 — SQL Server startup (currently lines 31–62):**
```bash
if [ "$ARTIFACT_TYPE" != "csv" ]; then
    echo "Starting SQL Server (${MSSQL_PID:-Standard})..."
    /opt/mssql/bin/sqlservr &
    SQL_PID=$!
    # ... existing wait loop and timeout check unchanged ...
fi
```

**Block 2 — Path-specific SQL configuration (currently lines 64–80):**
```bash
if [ "$ARTIFACT_TYPE" != "csv" ]; then
    case "$PIPELINE_PATH" in
        PATH1_EXPRESS) ... ;;
        PATH2_STANDARD) ... ;;
        PATH3_BATCH) ... ;;
    esac
fi
```

**Block 3 — Pipeline dispatch (add csv branch before existing extraction):**
```bash
case "$PIPELINE_MODE" in
    extraction)
        if [ "$ARTIFACT_TYPE" = "csv" ]; then
            echo "Starting CSV EXTRACTION pipeline (Iceberg -> CSV -> S3)..."
            python3 -m src.fargate.csv_entrypoint "$@"
        else
            echo "Starting EXTRACTION pipeline (Iceberg -> SQL Server -> MDF/BAK)..."
            python3 -m src.fargate.entrypoint "$@"
        fi
        EXIT_CODE=$?
        ;;
    ingestion)
        # unchanged
    ;;
esac
```

**Block 4 — SQL Server shutdown (bottom of file):**
```bash
if [ "$ARTIFACT_TYPE" != "csv" ]; then
    echo "Stopping SQL Server..."
    $SQLCMD_PATH -S localhost -U sa -P "$SA_PASSWORD" -C -Q "SHUTDOWN WITH NOWAIT" 2>/dev/null || true
fi
```

---

### 2. `src/fargate/csv_entrypoint.py` (NEW)

Mirrors the structure of `entrypoint.py` but with no SQL Server imports. Reuses all shared modules unchanged.

**Key reused modules:**
- `src.fargate.progress_reporter.ProgressReporter` — same progress callbacks, same S3 key
- `src.fargate.s3_uploader.S3Uploader` — same upload logic
- `src.core.config.load_config` — same config
- `src.core.iceberg_reader.IcebergReader` — same reader, unchanged
- `src.models.manifest.build_manifest` / `Manifest` — same manifest model

**Not imported (CSV has no SQL Server):**
- `DatabaseManager`, `ArtifactGenerator`, `Validator`, `pyodbc`

**Progress stages match BAK/MDF exactly** so the Activity Management API and UI see the same status strings and S3 `progress.json` structure regardless of format.

---

### 3. `src/fargate/csv_generator.py` (NEW)

Handles per-table Iceberg → CSV conversion.

- Calls `IcebergReader.stream_table_batches()` — existing method, **unchanged**
- Converts Arrow `RecordBatch` objects to CSV using `pyarrow.csv.write_csv()` (PyArrow already in `requirements.txt`)
- Streams to S3 using boto3 multipart upload — no local disk accumulation (ephemeral storage unused)
- Returns `ArtifactFile` objects (same dataclass as BAK/MDF artifacts, `file_type="csv"`)
- No `pyodbc`, no `DatabaseManager`, no SQL Server dependencies

---

### 4. `src/models/execution_plan.py`

```python
class ArtifactType(str, Enum):
    BAK = "bak"
    MDF = "mdf"
    BOTH = "both"
    CSV = "csv"    # ← add
```

---

### 5. `cdk/src/EES.AP.SDE.CDK/Stacks/SDEStepFunctionsStack.cs`

One new condition added **first** in `CreateExtractionDefinition()`:

```csharp
// CSV bypasses size-based routing — always uses Express (no SQL Server size cap)
determinePath
    .When(Condition.StringEquals("$.artifact_type", "csv"), path1Express)   // ← NEW
    .When(Condition.StringEquals("$.selected_path", "EXPRESS"), path1Express)
    .When(Condition.StringEquals("$.selected_path", "STANDARD"), path2Standard)
    .Otherwise(path3Batch);
```

CSV reuses `path1Express → runExpressTask` — no new SFN states, no new helper methods, no new task definition ARN references.

---

### 6. `cdk/src/EES.AP.SDE.CDK/lambda/spla_tracker.py`

Add `artifact_type` to the DynamoDB record for visibility in SPLA reports:

```python
item = {
    "tenant_id": tenant_id,
    "run_id": run_id,
    # ... existing fields ...
    "artifact_type": input_data.get("artifact_type", "bak"),   # ← add
}
```

**No changes to SPLA core logic.** CSV runs on Express → `spla_liable=False`, `spla_cores=0` — already correct. The `artifact_type` field is purely informational for the monthly report.

---

## Monitoring Behaviour for CSV

### Progress Reporting (Unchanged)

`ProgressReporter` is format-agnostic. Same S3 key (`sde/runs/{run_id}/progress.json`), same HTTP callback, same payload structure. The only visible difference to the Activity Management API / UI is `artifact_type: "csv"` in the completed manifest.

| % | Status | BAK/MDF meaning | CSV meaning |
|---|---|---|---|
| 10 | `PROVISIONING` | Config load | Config load |
| 20–70 | `LOADING` | Load tables into SQL Server | Read tables from Iceberg, write CSV to S3 |
| 70 | `VALIDATING` | DBCC CHECKDB + SQL row counts | Row-count cross-check (Iceberg vs CSV written) |
| 80 | `GENERATING_ARTIFACT` | Compress .bak / detach .mdf | Finalise S3 multipart, compute checksums |
| 90 | `UPLOADING` | Upload .bak/.mdf to S3outputpath | Upload manifest.json |
| 100 | `COMPLETED` | Manifest with checksums | Manifest with per-table row counts + S3 keys |
| 0 | `FAILED` | Exception message | Exception message |

### Manifest (No Schema Changes)

The `Manifest` model in `src/models/manifest.py` requires no changes:
- `artifact_type: str` — already generic, will hold `"csv"`
- `artifacts: list[ArtifactFile]` — `file_type` will hold `"csv"` instead of `"bak"`/`"mdf"`
- `validation: Optional[ValidationReport]` — `dbcc_result` will be `None` (already Optional); `row_counts` still populated from the row-count cross-check
- `status`, `error`, `total_rows`, `total_tables`, `total_size_bytes` — all unchanged

### SPLA / DynamoDB (No Obligation for CSV)

CSV runs on the Express task definition which is always SQL Server Express (free). The SPLA tracker records CSV runs with:

```json
{
  "path": "PATH1",
  "selected_path": "EXPRESS",
  "artifact_type": "csv",
  "spla_liable": false,
  "spla_cores": 0,
  "compute": "fargate"
}
```

`ConcurrentSPLACores` CloudWatch metric and alarms are unaffected — CSV contributes zero cores.

---

## What Is NOT Affected

- All existing BAK / MDF / BOTH extraction paths — no changes, no risk
- Ingestion pipeline — completely untouched
- `check_records_lambda.py` — format-agnostic, no changes
- All CDK stacks except `SDEStepFunctionsStack.cs` (one line added)
- `appsettings.json` — no changes
- IAM policies — CSV reuses the Express task definition, already covered by existing `EcsRunTask` policy
- SPLA alarms and CloudWatch dashboard — unchanged

---

## Testing

### Unit Tests

| File | Tests |
|---|---|
| `tests/unit/test_execution_plan.py` | Add: `test_artifact_type_csv()` — verify `ArtifactType.CSV == "csv"` and plan creation |
| `tests/unit/test_csv_generator.py` | New: mock `IcebergReader.stream_table_batches()` with sample Arrow data; verify CSV output matches expected rows; verify S3 multipart upload calls |

### Integration / Manual Smoke Test

1. Trigger the extraction Step Function with `artifact_type: "csv"` and a known tenant
2. Verify SPLA tracker writes `spla_liable: false`, `artifact_type: "csv"` to DynamoDB
3. Verify S3 output contains one `.csv` per table plus `manifest.json`
4. Verify row counts in manifest match Iceberg source counts
5. Trigger an existing BAK extraction on the same tenant — verify it is unaffected

---

## Summary

| Metric | Value |
|---|---|
| New AWS infrastructure components | 0 |
| New ECR repositories | 0 |
| New ECS task definitions | 0 |
| New CDK stacks | 0 |
| Files modified | 4 |
| Files created | 3 |
| Existing BAK/MDF paths affected | None |
| SQL Server database size cap for CSV | None (not applicable) |
| SPLA obligation for CSV | None (Express is always free) |
