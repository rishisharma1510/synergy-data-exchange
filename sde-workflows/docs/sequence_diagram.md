# Synergy Data Exchange Pipeline — Sequence Diagrams

## 1. Extraction: Iceberg → SQL Server → MDF/BAK → Tenant S3

### Full Sequence: End-to-End

```mermaid
sequenceDiagram
    autonumber
    participant ES as Extraction Service
    participant AM as Activity Management
    participant SFN as Step Function
    participant LMB as Check Records Lambda
    participant TCL as Tenant Context Lambda
    participant ECS as ECS Fargate / Batch
    participant SQL as SQL Server Express/Standard
    participant PY as Python Pipeline
    participant ICE as Iceberg / Glue+S3
    participant S3C as S3 Config Bucket
    participant S3T as Tenant S3 Bucket
    participant SNS as SNS

    Note over ES,SNS: PHASE 1 — TRIGGER

    ES->>AM: POST /activities<br/>tenantId, exposureIds, artifactType, tables
    AM->>AM: Create activity record<br/>status = SUBMITTED
    AM->>SFN: StartExecution<br/>tenantId, exposureIds, artifactType,<br/>activityId, S3outputpath, callbackUrl
    AM-->>ES: 202 Accepted (activityId)

    Note over ES,SNS: PHASE 2 — PRE-CHECK (Early Exit)

    SFN->>LMB: Invoke CheckRecordsLambda<br/>tenantId, exposureIds, tables
    LMB->>ICE: Lightweight Iceberg scan<br/>count rows matching exposureIds
    ICE-->>LMB: row counts per table
    LMB-->>SFN: record_check{has_records, record_count, tables_checked}

    alt No matching records
        SFN->>AM: Callback COMPLETED (early exit)<br/>no records found for exposureIds
        Note over SFN: Skip ECS task — save cost
    end

    Note over ES,SNS: PHASE 3 — PROVISION TASK

    SFN->>ECS: RunTask — Fargate (PATH 1/2) or Batch EC2 (PATH 3)<br/>SQL Server + Python pipeline (same container)<br/>PATH 1: 4 vCPU 8 GB | PATH 2: 8 vCPU 16 GB | PATH 3: 16+ vCPU 64 GB+<br/>TENANT_ROLE_ARN injected as env var if Batch mode
    ECS-->>SFN: taskArn

    Note over ES,SNS: PHASE 4 — RESOLVE TENANT CONTEXT

    PY->>TCL: Invoke Tenant Context Lambda<br/>tenantId
    TCL-->>PY: {glue_database, bucket_name,<br/>tenant_role, client_id, client_secret}
    PY->>PY: Set cfg.iceberg.namespace = glue_database

    Note over ES,SNS: PHASE 5 — INITIALIZE DATABASE

    PY->>PY: Report 10% PROVISIONING<br/>writes sde/runs/{runId}/progress.json to S3<br/>+ optional HTTP POST to callbackUrl
    PY->>SQL: CREATE DATABASE extract_{tenantId}_{runId}
    PY->>S3C: GET mapping_overrides.json (optional)<br/>CONFIG_OVERRIDE_PATH env var
    S3C-->>PY: overrides or 404
    PY->>ICE: Load Glue catalog — list tables in glue_database
    ICE-->>PY: table1, table2, ... tableN

    PY->>PY: Filter tables if TABLES input provided<br/>Build execution plan (DDL per table)

    loop For each table in plan
        PY->>ICE: Get Arrow schema for table_i
        ICE-->>PY: Arrow schema (columns, types)
        PY->>PY: Apply ColumnMapper (6-layer auto-map + overrides)
        PY->>SQL: CREATE TABLE dbo.TargetName<br/>DDL from Arrow schema
    end

    PY->>PY: Report 20% LOADING

    Note over ES,SNS: PHASE 6 — PARALLEL TABLE LOAD

    PY->>PY: ThreadPoolExecutor (MAX_WORKERS env var, default 4)

    par Table 1 — Thread 1
        PY->>ICE: scan table1<br/>row_filter = ExposureSetSID IN (101, 205, ...)<br/>selected_fields = projected cols
        ICE-->>PY: Arrow RecordBatch stream
        loop Each batch (10K rows default)
            PY->>SQL: INSERT INTO dbo.Table1<br/>WITH TABLOCK via pyodbc fast_executemany
        end
    and Table 2 — Thread 2
        PY->>ICE: scan table2 row_filter=...
        ICE-->>PY: Arrow RecordBatch stream
        loop Each batch
            PY->>SQL: INSERT INTO dbo.Table2 WITH TABLOCK
        end
    and Table N — Thread N (up to MAX_WORKERS)
        PY->>ICE: scan tableN row_filter=...
        ICE-->>PY: Arrow RecordBatch stream
        loop Each batch
            PY->>SQL: INSERT INTO dbo.TableN WITH TABLOCK
        end
    end

    Note right of PY: Tables beyond MAX_WORKERS<br/>queued and processed as<br/>threads become available

    PY->>PY: All tables loaded — collect rowCounts per table
    PY->>PY: Report 70% VALIDATING

    Note over ES,SNS: PHASE 7 — VALIDATION

    PY->>SQL: SELECT COUNT(*) FROM each table
    SQL-->>PY: row counts
    PY->>PY: Compare source (Iceberg) vs target (SQL) counts

    PY->>SQL: DBCC CHECKDB on extract_db
    SQL-->>PY: integrity OK

    PY->>SQL: Check PK / UNIQUE constraints
    SQL-->>PY: constraints OK

    PY->>PY: Generate checksums via HASHBYTES

    alt Validation PASSED
        PY->>PY: validationReport = pass
    else Validation FAILED
        PY->>PY: Raise ValidationError
        PY->>PY: Report 0% FAILED + error details
        Note over ECS: Task exits non-zero<br/>Step Function catches failure
    end

    PY->>PY: Report 80% GENERATING_ARTIFACT

    Note over ES,SNS: PHASE 8 — GENERATE ARTIFACT

    Note right of SQL: Artifacts written to /data/backup<br/>(mssql-owned volume, not /tmp)

    alt artifactType = bak
        PY->>SQL: BACKUP DATABASE extract_db<br/>TO DISK = /data/backup/extract_db.bak
        SQL-->>PY: .bak file on shared volume
    else artifactType = mdf
        PY->>SQL: ALTER DATABASE extract_db<br/>SET SINGLE_USER WITH ROLLBACK IMMEDIATE
        PY->>SQL: EXEC sp_detach_db extract_db
        SQL-->>PY: .mdf + .ldf on shared volume
    else artifactType = both
        PY->>SQL: BACKUP DATABASE (create .bak first)
        PY->>SQL: sp_detach_db (detach for .mdf + .ldf)
    end

    PY->>PY: Compute SHA256 checksums per file
    PY->>PY: Report 90% UPLOADING

    Note over ES,SNS: PHASE 9 — UPLOAD TO TENANT S3

    Note right of PY: Fargate: uses TaskRoleArn (no STS)<br/>Batch: STS AssumeRole(TENANT_ROLE_ARN)

    loop For each artifact file
        PY->>S3T: CreateMultipartUpload<br/>{S3outputpath}/{activityId}/filename
        loop Each 100 MB part
            PY->>S3T: UploadPart
        end
        PY->>S3T: CompleteMultipartUpload
        PY->>PY: Verify S3 ETag vs local SHA256
    end

    PY->>S3T: PUT manifest.json alongside artifacts<br/>{S3outputpath}/{activityId}/manifest.json

    Note over ES,SNS: PHASE 10 — CLEANUP + NOTIFY

    PY->>SQL: DROP DATABASE extract_db
    PY->>PY: Report 100% COMPLETED<br/>writes progress.json to S3<br/>+ HTTP callback if callbackUrl set

    Note right of ECS: Container exits 0<br/>Ephemeral storage auto-destroyed

    SFN->>SNS: Publish completion event<br/>tenantId, activityId, artifactType,<br/>s3Location, checksum

    AM-->>ES: Webhook/Poll activity COMPLETED
    ES->>ES: Process result
```

## 2. Ingestion: Tenant S3 → BAK/MDF → SQL Server → SID Transform → Iceberg

### Full Sequence: End-to-End

```mermaid
sequenceDiagram
    autonumber
    participant ES as Extraction Service
    participant AM as Activity Management
    participant SFN as Step Function
    participant TCL as Tenant Context Lambda
    participant ECS as ECS Fargate / Batch
    participant SQL as SQL Server Express/Standard
    participant PY as Python Pipeline
    participant SID as SID API
    participant EXPAPI as Exposure API
    participant S3T as Tenant S3 Bucket
    participant ICE as Iceberg / Glue+S3

    Note over ES,ICE: PHASE 1 — TRIGGER

    ES->>AM: POST /activities (ingestion)<br/>tenantId, source_s3_bucket, source_s3_key,<br/>artifact_type, checksum, tables, write_mode
    AM->>AM: Create activity record<br/>status = SUBMITTED
    AM->>SFN: StartExecution<br/>tenantId, activityId, source_s3_bucket,<br/>source_s3_key, artifact_type, callbackUrl
    AM-->>ES: 202 Accepted (activityId)

    Note over ES,ICE: PHASE 2 — PROVISION TASK

    SFN->>ECS: RunTask — Fargate (PATH 1/2) or Batch EC2 (PATH 3)<br/>SQL Server + Python pipeline (same container)<br/>TENANT_ROLE_ARN injected if Batch mode
    ECS-->>SFN: taskArn

    Note over ES,ICE: PHASE 3 — RESOLVE TENANT CONTEXT

    PY->>PY: Report 5% STARTING<br/>writes sde/runs/{runId}/progress.json to S3
    PY->>TCL: Invoke Tenant Context Lambda<br/>tenantId
    TCL-->>PY: {glue_database, bucket_name,<br/>tenant_role, client_id, client_secret}
    PY->>PY: Set target_namespace = glue_database

    Note over ES,ICE: PHASE 4 — DOWNLOAD ARTIFACT

    PY->>PY: Report 10% DOWNLOADING
    Note right of PY: Fargate: TaskRoleArn credentials<br/>Batch: STS AssumeRole(TENANT_ROLE_ARN)

    PY->>S3T: GetObject streaming download<br/>source_s3_bucket / source_s3_key
    S3T-->>PY: BAK or MDF file stream (100 MB chunks)
    PY->>PY: Save to /data/backup/filename<br/>Verify SHA256 checksum if provided

    Note over ES,ICE: PHASE 5 — RESTORE DATABASE

    PY->>PY: Report 20% RESTORING
    PY->>SQL: Connect to master database

    alt artifact_type = bak
        PY->>SQL: RESTORE FILELISTONLY<br/>FROM DISK = /data/backup/db.bak
        SQL-->>PY: logical file names (data + log)
        PY->>SQL: RESTORE DATABASE ingest_{tenantId}_{runId}<br/>FROM DISK = /data/backup/db.bak<br/>WITH MOVE data → /data/mdf/db.mdf,<br/>MOVE log → /data/mdf/db_log.ldf,<br/>REPLACE, RECOVERY
    else artifact_type = mdf
        PY->>SQL: CREATE DATABASE ingest_{tenantId}_{runId}<br/>ON (FILENAME = /data/mdf/db.mdf)<br/>FOR ATTACH_REBUILD_LOG
    end

    SQL-->>PY: Database restored / attached
    PY->>SQL: Validate restore — SELECT COUNT(*) per table
    SQL-->>PY: table_count, validates OK

    Note over ES,ICE: PHASE 6 — OPTIONAL: EXPOSURE API CALL

    PY->>PY: Check EXPOSURE_API_URL env var<br/>(baked into task definition — never from SFN input)

    alt EXPOSURE_API_URL is set
        PY->>SQL: SELECT * FROM tExposureSet
        PY->>SQL: SELECT * FROM tExposureView
        SQL-->>PY: ExposureSet rows (name, SID, metadata)
        PY->>PY: extract_exposure_metadata(tenant_id, run_id, source_artifact)

        PY->>EXPAPI: POST /exposures<br/>ExposureSet name, tenant_id, source_artifact
        EXPAPI-->>PY: {exposure_set_sid: 5001234,<br/>exposure_sets: [{name, sid}]}

        PY->>PY: sid_transformer.set_override_sid("tExposureSet", 5001234)
        Note right of PY: All tExposureSet SID values<br/>will be remapped to the new<br/>centrally-issued SID
    end

    Note over ES,ICE: PHASE 7 — SID TRANSFORMATION + WRITE TO ICEBERG

    PY->>PY: Report 30% READING<br/>Build dependency-ordered table list<br/>(SID_CONFIG FK hierarchy — parent tables first)

    loop For each table in dependency order (Level 0 → N)
        PY->>PY: Report progress 30-80%<br/>"SID TRANSFORM L{level}: {table}" or "INGESTING: {table}"

        PY->>SQL: SELECT * FROM [{table_name}]<br/>batch_size=10K rows at a time
        SQL-->>PY: Arrow RecordBatch stream
        PY->>PY: Combine batches → Arrow Table

        alt table is in SID_CONFIG (has dependency level)
            PY->>SID: Allocate SID range<br/>POST {sid_api_url}/{table}?count={row_count}<br/>Authorization: Bearer OktaToken
            SID-->>PY: {start_sid, count}

            PY->>PY: Remap primary key column<br/>old_SID → new_SID (allocated range)
            PY->>PY: Cascade FK columns<br/>(child FKs updated using parent mapping built above)
            Note right of PY: Self-referencing tables<br/>(e.g. tLocation.ParentLocationSID)<br/>handled in second pass
        end

        PY->>PY: Apply ColumnMapper<br/>transform column names to target schema

        PY->>ICE: table_exists(target_namespace, table_name)?
        ICE-->>PY: exists / not-found

        alt Table NOT in destination Iceberg
            PY->>PY: Mark as skipped<br/>reason: table_not_found_in_destination<br/>(destination tables never auto-created)
        else Table exists
            alt write_mode = overwrite
                PY->>ICE: table.overwrite(arrow_table)
            else write_mode = append (default)
                PY->>ICE: table.append(arrow_table)
            end
            ICE-->>PY: rows_written
        end
    end

    Note over ES,ICE: PHASE 8 — CLEANUP

    PY->>PY: Report 90% CLEANUP
    PY->>SQL: DROP DATABASE ingest_{tenantId}_{runId}
    PY->>PY: Delete /data/backup/filename

    Note over ES,ICE: PHASE 9 — SUMMARY + NOTIFY

    PY->>S3T: PUT sde/runs/{runId}/ingestion_summary.json<br/>{tables_success, tables_failed, total_rows,<br/>duration_s, rows_per_sec, exposure_sets}

    PY->>PY: Report 100% COMPLETED<br/>writes progress.json to S3<br/>+ HTTP callback if callbackUrl set

    Note right of ECS: Container exits 0<br/>Ephemeral storage auto-destroyed

    AM-->>ES: Webhook/Poll activity COMPLETED
    ES->>ES: Process result
```

---

## 3. Error Handling Sequence

```mermaid
sequenceDiagram
    autonumber
    participant SFN as Step Function
    participant PY as Python Pipeline
    participant ECS as Fargate / Batch Task
    participant AM as Activity Management

    Note over SFN,AM: ERROR — Extraction table load fails (partial)

    PY->>PY: Table 5 load throws exception
    PY->>PY: Log error, mark table5 = FAILED
    PY->>PY: Continue loading tables 6..N (non-critical path)
    PY-->>SFN: Load complete — 14/15 tables OK, 1 FAILED

    SFN->>SFN: Evaluate result — critical table?

    alt Non-critical table failed
        SFN->>SFN: Proceed to validation with warning
    else Critical table failed
        SFN->>AM: Callback FAILED — critical table load failed
        Note over ECS: Task exits non-zero
    end

    Note over SFN,AM: ERROR — Ingestion table not in destination

    PY->>PY: table_exists() returns False
    PY->>PY: Mark as skipped (not failed)<br/>reason: table_not_found_in_destination
    PY->>PY: Continue with remaining tables

    Note over SFN,AM: ERROR — SID allocation fails (HTTP error)

    PY->>PY: SID API returns 4xx / 5xx
    PY->>PY: Retry up to max_retries=3<br/>exponential backoff (retry_backoff_base=1.0s)
    PY->>PY: All retries fail → mark table as FAILED
    PY->>PY: Non-zero exit if any tables failed

    Note over SFN,AM: ERROR — Fargate task dies unexpectedly

    ECS-->>SFN: Task STOPPED (exit code != 0)
    SFN->>SFN: Catch TaskFailed
    SFN->>AM: Callback FAILED — Fargate task died
    Note right of SFN: No explicit cleanup needed<br/>ephemeral storage auto-destroyed

    Note over SFN,AM: ERROR — Step Function timeout

    SFN->>SFN: Execution timeout reached
    SFN->>AM: Callback FAILED — execution timeout
```

---

## 4. Activity Management Integration

```mermaid
sequenceDiagram
    autonumber
    participant DEV as DevOps / CI-CD
    participant AM as Activity Management
    participant SFN as Step Functions

    Note over DEV,SFN: DEPLOYMENT TIME (one-time setup)

    DEV->>AM: POST /activity-definitions<br/>name = Synergy Data ExchangeExtraction<br/>stepFunctionArn = extraction-sfn-arn<br/>timeout 7200, retryPolicy max 2
    AM-->>DEV: 201 Created (definitionId)

    DEV->>AM: POST /activity-definitions<br/>name = Synergy Data ExchangeIngestion<br/>stepFunctionArn = ingestion-sfn-arn<br/>timeout 7200, retryPolicy max 2
    AM-->>DEV: 201 Created (definitionId)

    Note over DEV,SFN: RUNTIME

    Note right of AM: Activity Management holds two<br/>registered SFN ARNs (one per activity type).<br/>Progress comes from Python pipeline via:<br/>1. S3 writes to sde/runs/{runId}/progress.json<br/>2. Optional HTTP POST to callbackUrl
```

---

## Credential Flow Summary

| Mode | S3 / Tenant Bucket Access | SID / Exposure API Auth |
|------|---------------------------|------------------------|
| **ECS Fargate** | TaskRoleArn assigned at runtime by Step Functions — no STS needed | OAuth: Tenant Context Lambda → `client_id/secret` → Okta token endpoint → Bearer token |
| **AWS Batch** | STS AssumeRole with `TENANT_ROLE_ARN` env var injected by Step Functions | OAuth: same Tenant Context Lambda flow |

## Progress Reporting Summary

Progress is reported via **two channels** on every `reporter.report()` call:

| Channel | Key / Target | Description |
|---------|--------------|-------------|
| **S3 (always)** | `sde/runs/{runId}/progress.json` | Polled by Activity Management API for UI updates |
| **HTTP (optional)** | `callbackUrl` from Step Functions input | Direct push notification to Activity Management |

Writes are fire-and-forget via a background daemon thread — never blocks the pipeline critical path.

