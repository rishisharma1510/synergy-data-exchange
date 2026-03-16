# SOW Deliverables Tracker

**Project:** SQL to Iceberg (ees-ap-sde)  
**Client:** Insurance Services Office, Inc., dba Verisk  
**Original SOW Date:** 10/17/2025  
**Change Order Date:** 2/13/2026  
**Last Updated:** 2026-02-28

---

## Technology Stack (per Change Order)

| Component | Original SOW | Change Order | Current Implementation |
|-----------|--------------|--------------|------------------------|
| IaC | Python CDK | .NET CDK | ✅ .NET 8.0 CDK |
| Orchestration | MWAA/Airflow | Step Functions | ✅ Step Functions |
| ETL Engine | Apache Spark | PyIceberg + PyArrow | ✅ PyIceberg + PyArrow |
| Extraction | On-prem scripts | AWS SQL Server Spin-Up | ✅ AWS-based (Fargate/Batch) |

---

## Deliverables Status

### Legend
- ✅ **Complete** — Implemented and tested
- ⚠️ **Partial** — Started but incomplete
- ❌ **Missing** — Not yet implemented
- 🚫 **Removed** — Superseded by Change Order

---

## 1. AWS SQL Server Spin-Up Service — EC2 Path (≥7GB)

> *Change Order: "Provision and manage EC2-based SQL Server Standard instances for processing large databases."*

| # | Deliverable | Status | File(s) | Notes |
|---|-------------|--------|---------|-------|
| 1a | EC2 infrastructure (.NET CDK) with memory-optimized instances, EBS volumes | ⚠️ Partial | `SDEBatchStack.cs` | Using AWS Batch instead of raw EC2. Batch provides managed EC2 with Launch Template + EBS. |
| 1b | Security groups with SQL Server port restrictions, VPC-only access | ✅ Done | `SDEBatchStack.cs` | Security group created |
| 1c | IAM roles (S3, DynamoDB, Secrets Manager, SSM) | ⚠️ Partial | `SDEBatchStack.cs` | S3 + Glue done. DynamoDB + Secrets Manager missing. |
| 1d | Pre-configured SQL Server Standard AMI | ❌ Missing | — | Need AMI with SQL Server Standard + AWS CLI + SSM agent |
| 1e | Step Functions for EC2 lifecycle (launch → attach → detect → upgrade → backup → terminate) | ⚠️ Partial | `SDEStepFunctionsStack.cs` | Routing logic done. Actual lifecycle states are placeholders. |
| 1f | MDF attachment and schema version discovery | ⚠️ Partial | `artifact_restorer.py` | MDF attach done. Schema version detection missing. |
| 1g | Sequential schema upgrade execution | ❌ Missing | — | Need to execute Client-provided SQL scripts |
| 1h | MDF size evaluation service (route ≥7GB to EC2, <7GB to ECS) | ✅ Done | Step Functions | Choice state routes by `fileSizeBytes` |
| 1i | Upgrade plan resolution service | ❌ Missing | — | Determine correct sequence of schema upgrade scripts |
| 1j | Automated EC2 cleanup/termination | ✅ Done | AWS Batch | Batch handles instance lifecycle |
| 1k | .bak file restore support | ✅ Done | `artifact_restorer.py` | RESTORE DATABASE implemented |
| 1l | Component testing (unit, integration, failure, performance) | ❌ Missing | — | Timeboxed 1 week per SOW |

### EC2 Path Summary: 5/12 complete (42%)

---

## 2. AWS SQL Server Spin-Up Service — ECS Path (<7GB)

> *Change Order: "Provision and manage ECS Fargate-based SQL Server Express containers for processing small databases."*

| # | Deliverable | Status | File(s) | Notes |
|---|-------------|--------|---------|-------|
| 2a | ECS cluster + Fargate task definition (8GB mem, 4 vCPU, 200GB ephemeral) | ✅ Done | `SDEFargateStack.cs` | Cluster + tasks defined |
| 2b | ECS task IAM roles (least-privilege) | ✅ Done | `SDEFargateStack.cs` | Task + execution roles |
| 2c | MDF attachment logic (S3 download → attach → verify) | ✅ Done | `artifact_restorer.py` | `attach_mdf()` implemented |
| 2d | Schema version detection from database metadata | ❌ Missing | — | Query DB metadata to identify v1-v4 |
| 2e | Sequential schema upgrade execution | ❌ Missing | — | Execute Client SQL scripts in order |
| 2f | Backup, compression, S3 upload with tenant/job prefix | ✅ Done | `artifact_generator.py`, `s3_uploader.py` | BAK generation + upload |
| 2g | ECS task entrypoint (attach → detect → upgrade → backup) | ⚠️ Partial | `entrypoint.py` | Missing detect + upgrade steps |
| 2h | Step Functions integration for ECS lifecycle | ✅ Done | `SDEStepFunctionsStack.cs` | Pass states for Fargate tasks |
| 2i | .bak file restore support | ✅ Done | `artifact_restorer.py` | RESTORE DATABASE |
| 2j | Component testing (Docker, integration, resources, failure) | ❌ Missing | — | Timeboxed 1 week per SOW |

### ECS Path Summary: 6/10 complete (60%)

---

## 3. SID Transformation Pipeline

> *Change Order: "Deliver an ECS-based processing pipeline for SID (Source Identifier) remapping."*

### SID API Model — Range Allocation (NOT Per-Record)

The SID API uses a **range allocation model**:
- **One API call per table** (not per record)
- Request: `{ tableName, count }`
- Response: `{ start, count }` — contiguous SID range
- Example: For 10,000 Location records → API returns `{ start: 5000000, count: 10000 }`
- Transformation: `old_sid → new_sid = start + row_index`

### Authentication Flow

```
Secrets Manager (App Token)
    ↓
Tenant Context Lambda (aws-sd-lambda-tenant-context)
    ↓ returns: { clientID, clientSecret }
Okta Token API
    ↓ returns: Bearer token
SID API
```

Reference: `EES.AP.SynergyDrive/src/EES.AP.SynergyDrive.CDK/VirusScannerContainer/tenant_service.py`

### Processing Order (Parent → Child)

SID transformation MUST process tables in dependency order:
1. **Parent tables first** — allocate new SID ranges, create mapping
2. **Child tables second** — update FK references using parent mappings
3. Self-referencing tables (e.g., `tLocation.ParentLocationSID`) require two-pass processing

### Processing Sequence (6 Dependency Levels)

| Level | Tables | Why This Order |
|-------|--------|----------------|
| **0** | tCompany, tPortfolioFilter, tAppliesToArea, tReinsuranceEPCurveSet | Root tables — no FK dependencies |
| **1** | tExposureSet, tReinsuranceProgram, tPortfolio, tReinsuranceEPCurve | Depend only on Level 0 |
| **2** | tContract, tReinsuranceTreaty, tCompanyLossAssociation | Depend on Level 1 |
| **3** | tLocation*, tLayer, tStepFunction, tAppliesToEventFilter | Depend on Level 2 |
| **4** | tLayerCondition*, tLocTerm, tLocFeature, tLocOffshore, tLocParsedAddress, tLocWC | Depend on Level 3 |
| **5** | Junction tables (tLayerConditionLocationXref, tLocStepFunctionXref, etc.) | Depend on multiple parents |

*\* = Self-referencing, requires two-pass processing*

### Key SID Columns (Major Parent Tables)

| Table | PK Column | Referenced By (Child Tables) |
|-------|-----------|------------------------------|
| tCompany | CompanySID | tExposureSet, tReinsuranceProgram, tCompanyLossAssociation, tCompanyCatBondImportHistory, tCompanyUDCClassification_Xref |
| tExposureSet | ExposureSetSID | tContract, tLocation, tAggregateExposure, tReinsAppliesToExp, tReinsuranceProgramTargetXRef |
| tContract | ContractSID | tLayer, tLayerCondition, tLocation, tLocTerm, tStepFunction |
| tLocation | LocationSID | tLocFeature, tLocOffshore, tLocParsedAddress, tLocStepFunctionXref, tLocTerm, tLocWC, tEngineLocPhysicalProperty, tLayerConditionLocationXref + **self-reference** |
| tReinsuranceProgram | ReinsuranceProgramSID | tReinsuranceTreaty, tReinsuranceProgramTargetXRef, tReinsuranceProgramSourceInure, tCatBondAttributeValue |

### Self-Referencing Tables (Two-Pass Required)

| Table | PK Column | Self-Reference FK | Processing Notes |
|-------|-----------|-------------------|------------------|
| tLocation | LocationSID | ParentLocationSID | Hierarchical locations — process parents before children |
| tLayerCondition | LayerConditionSID | ParentLayerConditionSID | Nested conditions |
| tCompanyLossAssociation | CompanyLossAssociationSID | ParentCompanyLossAssociationSID | Hierarchical loss sets |

### SID Transformation Algorithm

**IMPORTANT: In-Memory Transformation — Never Modify Source SQL Database**

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   SQL Server    │────▶│  PyArrow Tables  │────▶│  SID Transform  │────▶│  Iceberg Write  │
│   (read-only)   │     │   (in memory)    │     │   (in memory)   │     │ (existing only) │
└─────────────────┘     └──────────────────┘     └─────────────────┘     └─────────────────┘
```

**Key Constraints:**
1. **Source SQL database is NEVER modified** — read-only extraction
2. **SID transformation happens in PyArrow memory** — no intermediate writes
3. **Create Iceberg table if not exists** — auto-create with inferred schema from PyArrow
4. **Row-group level processing** — memory-efficient for large tables

```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema

# Initialize
catalog = load_catalog("glue")
sid_mappings = {}  # { table_name: { old_sid: new_sid } }

def process_table(table_name: str, sql_connection, iceberg_table_path: str):
    """
    Read from SQL → Transform SIDs in memory → Write to Iceberg table (create if not exists)
    """
    # 1. Read from SQL Server into PyArrow (in memory)
    query = f"SELECT * FROM {table_name}"
    arrow_table = read_sql_to_arrow(sql_connection, query)  # PyArrow Table in memory
    
    if arrow_table.num_rows == 0:
        return
    
    # 2. Get or create Iceberg table
    try:
        iceberg_table = catalog.load_table(iceberg_table_path)
    except NoSuchTableError:
        # Create table from PyArrow schema
        iceberg_schema = arrow_schema_to_iceberg(arrow_table.schema)
        iceberg_table = catalog.create_table(
            identifier=iceberg_table_path,
            schema=iceberg_schema,
            location=f"s3://sde-bucket/{table_name.lower()}/"
        )
        print(f"Created Iceberg table: {iceberg_table_path}")
    
    # 3. Allocate SID range from API
    row_count = arrow_table.num_rows
    sid_response = sid_api.allocate(table_name=table_name, count=row_count)
    # sid_response = { "start": 5000000, "count": 10000 }
    
    # 4. Build old→new mapping from PK column
    pk_column = get_pk_column(table_name)  # e.g., "LocationSID"
    old_sids = arrow_table.column(pk_column).to_pylist()
    
    sid_mappings[table_name] = {
        old_sid: sid_response["start"] + idx 
        for idx, old_sid in enumerate(old_sids)
    }
    
    # 5. Transform PK column in memory (PyArrow)
    new_pk_values = pa.array([sid_mappings[table_name][old] for old in old_sids])
    arrow_table = replace_column(arrow_table, pk_column, new_pk_values)
    
    # 6. Transform FK columns using parent mappings (in memory)
    for fk_column, parent_table in get_fk_columns(table_name):
        if parent_table in sid_mappings:
            fk_values = arrow_table.column(fk_column).to_pylist()
            new_fk_values = pa.array([
                sid_mappings[parent_table].get(old, old)  # Keep original if not in mapping
                for old in fk_values
            ])
            arrow_table = replace_column(arrow_table, fk_column, new_fk_values)
    
    # 7. Handle self-reference (same table FK)
    self_ref_fk = get_self_reference_fk(table_name)  # e.g., "ParentLocationSID"
    if self_ref_fk:
        fk_values = arrow_table.column(self_ref_fk).to_pylist()
        new_fk_values = pa.array([
            sid_mappings[table_name].get(old, old) if old else None
            for old in fk_values
        ])
        arrow_table = replace_column(arrow_table, self_ref_fk, new_fk_values)
    
    # 8. Write to EXISTING Iceberg table (append)
    iceberg_table.append(arrow_table)

# Process tables in dependency order
for level in range(0, 6):
    for table in TABLES_BY_LEVEL[level]:
        iceberg_path = f"glue.Synergy Data Exchange.{table.lower()}"
        process_table(table, sql_conn, iceberg_path)
```

| # | Deliverable | Status | File(s) | Notes |
|---|-------------|--------|---------|-------|
| 3a | ECS task infrastructure (.NET CDK) | ✅ Done | `cdk/src/EES.AP.SDE.CDK/Stacks/` | 4 CDK stacks: ECR, Fargate, Batch, Step Functions |
| 3b | IAM roles (S3 read/write, external API, DynamoDB tracking) | ✅ Done | `infra/iam.py` | Lambda invoke, CloudWatch metrics, S3/DynamoDB |
| 3c | SID API client integration (retry + backoff) | ✅ Done | `src/sid/sid_client.py` | SIDClient + MockSIDClient with exponential backoff |
| 3d | SID column transformation (row-group level PyArrow) | ✅ Done | `src/sid/transformer.py` | In-memory transform, FK cascade, self-ref handling |
| 3e | Step Functions integration | ✅ Done | `infra/step_functions.py` | SID env vars passed to Fargate tasks |
| 3f | Error handling + retry (per-table and per-batch) | ✅ Done | `src/sid/transformer.py`, `checkpoint.py` | Per-table errors + checkpoint/resume |
| 3g | Tenant Context Lambda integration | ✅ Done | `src/sid/sid_client.py` | `rs-cdk{stage}-ap-tenant-context-lambda` pattern |
| 3h | SID mapping persistence (old→new for FK updates) | ✅ Done | `src/sid/transformer.py`, `checkpoint.py` | In-memory dict + checkpoint persistence |
| 3i | Iceberg table auto-creation | ✅ Done | `src/core/iceberg_writer.py` | `create_table_if_not_exists()` implemented |
| 3j | Comprehensive unit + integration testing | ✅ Done | `tests/unit/test_*.py` | 88 tests passing |
| 3k | Exposure API integration (override SIDs) | ✅ Done | `src/sid/exposure_client.py` | ExposureClient + override SID mappings |

### SID Pipeline Summary: 11/11 complete (100%)

---

## 4. Expanded Data Validation Framework

> *Change Order: "Deliver an expanded, multi-stage data validation pipeline."*

| # | Deliverable | Status | File(s) | Notes |
|---|-------------|--------|---------|-------|
| 4a | Source metrics collection (row counts, column stats from SQL Server) | ✅ Done | `validation.py` | Via EC2/ECS components |
| 4b | Target metrics collection (from Iceberg via Glue) | ⚠️ Partial | `validation.py` | Basic validation exists |
| 4c | Metrics comparison Lambda (configurable tolerance) | ❌ Missing | — | SOW specifies Lambda |
| 4d | Structured validation report (pass/fail + discrepancies) | ✅ Done | `validation.py` | Report generator exists |
| 4e | SNS notification integration | ❌ Missing | — | Alerts on completion/failure |
| 4f | Step Functions for validation orchestration | ❌ Missing | — | Multi-stage pipeline |
| 4g | Row-by-row sample validation (Fargate + EC2 paths) | ❌ Missing | — | Detailed reconciliation |
| 4h | Comprehensive testing of all validation components | ❌ Missing | — | |

### Validation Framework Summary: 2/8 complete (25%)

---

## 5. Modularization of Migration Processes

> *Change Order: "Refactor into modular, independently invocable services."*

| # | Deliverable | Status | File(s) | Notes |
|---|-------------|--------|---------|-------|
| 5a | Process A: SQL Server → Parquet on S3 | ⚠️ Partial | `sql_reader.py` | Logic exists, not packaged as independent service |
| 5b | Process B: Parquet on S3 → Iceberg tables | ⚠️ Partial | `iceberg_writer.py` | Logic exists, not packaged as independent service |
| 5c | Independent invocation (separate from end-to-end flow) | ❌ Missing | — | Need Step Functions or CLI entry points |

### Modularization Summary: 0/3 complete (0% — needs packaging)

---

## 6. Original SOW Items (Still Valid)

| # | Deliverable | Status | File(s) | Notes |
|---|-------------|--------|---------|-------|
| 6a | Schema mapping for 4 SQL Server versions (v1-v4) | ❌ Missing | — | Config-driven mapping per version |
| 6b | Exposure Management API integration (create exposureset + view) | ✅ Done | `src/sid/exposure_client.py` | ExposureClient + override SID + wired into ingestion_entrypoint.py |
| 6c | Glue catalog registration | ✅ Done | `iceberg_writer.py` | PyIceberg writes metadata |
| 6d | JDBC partitioned reader (split keys, ranges) | ✅ Done | `sql_reader.py` | Partition predicates |
| 6e | Configuration-driven extraction | ✅ Done | `config.py`, `mapping_config.py` | JSON config files |
| 6f | Retry/backoff logic | ✅ Done | Various | Error handling in place |
| 6g | Post-load Exposure artifacts per logical DB | ✅ Done | `ingestion_entrypoint.py` | Exposure API called before SID transform |

### Original SOW Summary: 6/7 complete (86%)

---

## 7. Infrastructure & CI/CD

| # | Deliverable | Status | File(s) | Notes |
|---|-------------|--------|---------|-------|
| 7a | Docker images (Express, Standard, Batch) | ✅ Done | `docker/Dockerfile.*` | 3 images |
| 7b | .NET CDK stacks (ECR, Fargate, Batch, Step Functions) | ✅ Done | `cdk/src/EES.AP.SDE.CDK/Stacks/` | 4 stacks |
| 7c | GitHub workflows | ✅ Done | `.github/workflows/` | 11 workflow files |
| 7d | CI pipeline to build + push to ECR | ✅ Done | `common.deploy.yml` | Docker build + push |
| 7e | Centralized logging (CloudWatch) | ✅ Done | CDK stacks | Log groups per service |

### Infrastructure Summary: 5/5 complete (100%)

---

## 8. Documentation

| # | Deliverable | Status | File(s) | Notes |
|---|-------------|--------|---------|-------|
| 8a | Architecture diagram | ⚠️ Partial | `docs/architecture_*.drawio` | Need update for SID + validation |
| 8b | Schema mapping matrix (4 versions) | ❌ Missing | — | Column/type conversions |
| 8c | Orchestration workflow spec | ✅ Done | `implementation_plan_bidirectional.md` | Step Functions documented |
| 8d | Security/IAM model | ✅ Done | `implementation_plan_bidirectional.md` | SOC2 section |
| 8e | Validation metric catalog | ⚠️ Partial | `implementation_plan_bidirectional.md` | Need tolerance thresholds |
| 8f | Quick-start guide with sample configs | ⚠️ Partial | `README.md` | Basic usage, needs update |
| 8g | SID Transformation Pipeline design | ✅ Done | `src/sid/README.md` | Full module documentation |
| 8h | Exposure API integration spec | ✅ Done | `src/sid/exposure_client.py` | ExposureClient with mock/production modes |

### Documentation Summary: 5/8 complete (63%)

---

## Overall Progress Summary

| Category | Complete | Partial | Missing | Total | % Complete |
|----------|----------|---------|---------|-------|------------|
| EC2 Path (≥7GB) | 5 | 2 | 5 | 12 | 42% |
| ECS Path (<7GB) | 6 | 1 | 3 | 10 | 60% |
| SID Pipeline | 11 | 0 | 0 | 11 | **100%** |
| Validation Framework | 2 | 1 | 5 | 8 | 25% |
| Modularization | 0 | 2 | 1 | 3 | 0% |
| Original SOW | 6 | 0 | 1 | 7 | **86%** |
| Infrastructure | 5 | 0 | 0 | 5 | **100%** |
| Documentation | 4 | 3 | 1 | 8 | 50% |
| **TOTAL** | **39** | **9** | **16** | **64** | **61%** |

---

## Priority Backlog

### P0 — Critical Gaps (SOW Deliverables)

| # | Item | Effort | Dependency | Status |
|---|------|--------|------------|--------|
| 1 | **SID Transformation Pipeline** — In-memory PyArrow transform + Range allocation API + FK cascade | 2-3 weeks | Client SID API endpoint | ✅ **DONE** (11/11 items) |
| 2 | **Exposure API Integration** — Create ExposureSet/View, override SID mappings | 1 week | Client API endpoint | ✅ **DONE** |
| 3 | **Iceberg table auto-creation** — Create if not exists with inferred schema | 1 day | — | ✅ **DONE** |
| 4 | **Schema version detection** — Query DB metadata for v1-v4 | 2 days | Client schema markers | ❌ Missing |
| 5 | **Schema upgrade execution** — Execute Client SQL scripts | 3 days | Client scripts | ❌ Missing |

### P1 — High Priority

| # | Item | Effort | Dependency |
|---|------|--------|------------|
| 5 | Validation metrics comparison Lambda | 2 days | — |
| 6 | SNS notification integration | 1 day | — |
| 7 | Row-by-row sample validation | 3 days | — |
| 8 | Modularization (Process A + B as services) | 2 days | — |

### P2 — Medium Priority

| # | Item | Effort | Dependency |
|---|------|--------|------------|
| 9 | Pre-configured SQL Server Standard AMI | 2 days | AWS account |
| 10 | DynamoDB + Secrets Manager IAM roles | 1 day | — |
| 11 | Step Functions actual task states (not Pass) | 3 days | — |
| 12 | 4-version schema mapping matrix doc | 1 day | Client schema docs |

### P3 — Documentation

| # | Item | Effort | Dependency | Status |
|---|------|--------|------------|--------|
| 13 | SID Pipeline design doc | 1 day | — | ✅ **DONE** — `src/sid/README.md` |
| 14 | Exposure API integration spec | 1 day | Client API spec | ✅ **DONE** — `src/sid/exposure_client.py` |
| 15 | Update architecture diagrams | 1 day | — | ❌ In progress |
| 16 | Validation metric catalog with thresholds | 0.5 day | Client agreement | ❌ Missing |

---

## Client Dependencies

| Item | Required From Client | Status |
|------|---------------------|--------|
| SID API documentation + credentials | Client to provide | ❓ Unknown |
| SID API non-prod endpoint | Client to provide | ❓ Unknown |
| Schema upgrade SQL scripts (v1→v2, v2→v3, v3→v4) | Client to provide + validate | ❓ Unknown |
| Exposure Management API spec (endpoints, auth, fields) | Client to provide | ❓ Unknown |
| Validation tolerance thresholds | Client to agree | ❓ Unknown |
| Sample customer datasets (10-15 DBs, up to 200GB) | Client to provide | ❓ Unknown |

---

## Change Log

| Date | Author | Changes |
|------|--------|---------|
| 2026-02-28 | AI | **Exposure API Integration**: Added `src/sid/exposure_client.py` with ExposureClient, MockExposureClient, override SID support. Wired into `ingestion_entrypoint.py`. 20 new tests. |
| 2026-02-28 | AI | **Iceberg auto-create**: Verified `iceberg_writer.py` has `create_table_if_not_exists()` |
| 2026-02-28 | AI | Updated SOW tracker: SID Pipeline 100%, Original SOW 86%, Overall 61% |
| 2026-02-28 | AI | **SID Pipeline implemented**: transformer.py, sid_client.py, config.py, metrics.py, checkpoint.py, audit_trail.py, parallel.py, enhanced.py — 11/11 deliverables complete (100%) |
| 2026-02-28 | AI | Added optional enhancements: checkpoint/resume, parallel table processing, SID allocation audit trail |
| 2026-02-28 | AI | Updated infra/ecs.py, iam.py, step_functions.py with SID integration and stage-aware naming |
| 2026-02-28 | AI | Added 88 unit/integration tests for SID pipeline + Exposure API (all passing) |
| 2026-02-28 | AI | Added MDF schema analysis appendix with SID columns and FK relationships |
| 2026-02-28 | AI | Updated SID Pipeline section with range allocation model (corrected from per-record) |
| 2026-02-28 | AI | Added Tenant Context Lambda auth flow documentation |
| 2026-02-28 | AI | Initial creation from SOW analysis |

---

## Appendix A: MDF Schema Analysis (AIRExposure.mdf)

**Source:** `s3://rks-sd-virusscan/extraction-test/AIRExposure.mdf` (1032 MB)  
**Analysis Date:** 2026-02-28

### A.1 All Tables (55 total)

| Table Name | Notes |
|------------|-------|
| tAggregateExposure | Has ExposureSetSID, GeographySID FKs |
| tAppliesToArea | Parent table |
| tAppliesToAreaGeoXRef | Junction table |
| tAppliesToEventFilter | |
| tAppliesToEventFilterGeoXRef | |
| tAppliesToEventFilterLatLong | |
| tAppliesToEventFilterRule | |
| tCatBondAttributeValue | |
| tCLFCatalog | |
| tCompany | **Root parent table** — CompanySID referenced by 5+ child tables |
| tCompanyCatBondImportHistory | |
| tCompanyCLASizes | |
| tCompanyLossAssociation | Parent for CLFCatalog, LossMarketShare |
| tCompanyLossMarketShare | |
| tCompanyUDCClassification_Xref | |
| tContract | **Major parent** — ContractSID referenced by Layer, Location, LocTerm, StepFunction |
| tDbHistory | |
| tDBVersion | |
| tEngineLocPhysicalProperty | |
| tExposureSet | **Major parent** — ExposureSetSID referenced by 5+ child tables |
| tLayer | Parent for LayerCondition |
| tLayerCondition | Has self-reference: ParentLayerConditionSID |
| tLayerConditionLocationXref | Junction table |
| tLocation | **Major parent** — LocationSID referenced by 10+ child tables; has self-reference: ParentLocationSID |
| tLocFeature | |
| tLocOffshore | |
| tLocParsedAddress | |
| tLocStepFunctionXref | Junction table |
| tLocTerm | |
| tLocWC | |
| tPortfolio | |
| tPortfolioFilter | |
| tPortfolioReinsuranceTreaty | Junction table |
| tProgramBusinessCategoryFactor | |
| tReinsAppliesToExp | |
| tReinsuraceLOBCountryOverride | |
| tReinsuranceEPCurve | |
| tReinsuranceEPCurveAdjustmentPoint | |
| tReinsuranceEPCurveSet | |
| tReinsuranceExposureAdjustmentFactor | |
| tReinsuranceProgram | **Major parent** — ReinsuranceProgramSID referenced by many child tables |
| tReinsuranceProgramCoverage | |
| tReinsuranceProgramEPAdjustment | |
| tReinsuranceProgramEPAdjustmentPoints | |
| tReinsuranceProgramJapanCFLRatio | |
| tReinsuranceProgramSourceInure | |
| tReinsuranceProgramTargetXRef | |
| tReinsuranceTreaty | Parent for AppliesToEventFilter, Reinstatement, etc. |
| tReinsuranceTreatyReinstatement | |
| tReinsuranceTreatySavedResults | |
| tReinsuranceTreatyTerrorismOption | |
| tReinsuranceTreatyUDCClassification_Xref | |
| tSIDControl | **System table** — tracks LastSID per table |
| tStepFunction | Parent for StepFunctionDetail |
| tStepFunctionDetail | |
| tUpgradeLog | |
| tUpgradeOption | |

### A.2 Primary Key SID Columns (35 tables)

| Table | PK Column(s) | Type |
|-------|-------------|------|
| tAggregateExposure | AggregateExposureSID | int |
| tAppliesToArea | AppliesToAreaSID | int |
| tAppliesToAreaGeoXRef | AppliesToAreaSID, GeographySID | int, int |
| tAppliesToEventFilter | AppliesToEventFilterSID | int |
| tAppliesToEventFilterGeoXRef | AppliesToEventFilterSID, GeographySID | int, int |
| tAppliesToEventFilterLatLong | AppliesToEventFilterSID | int |
| tAppliesToEventFilterRule | AppliesToEventFilterRuleSID | int |
| tCatBondAttributeValue | ReinsuranceProgramSID, CatBondAttributeSID | int, int |
| tCLFCatalog | CLFCatalogSID | int |
| tCompany | CompanySID | int |
| tCompanyCatBondImportHistory | CompanyCatBondImportHistorySID | int |
| tCompanyLossAssociation | CompanyLossAssociationSID | int |
| tCompanyLossMarketShare | CompanyLossMarketShareSID | int |
| tCompanyUDCClassification_Xref | UDCClassificationSID, CompanySID | int, int |
| tContract | ContractSID | int |
| tEngineLocPhysicalProperty | LocationSID | int |
| tExposureSet | ExposureSetSID | int |
| tLayer | LayerSID | int |
| tLayerCondition | LayerConditionSID | int |
| tLayerConditionLocationXref | LayerConditionSID, LocationSID | int, int |
| tLocation | LocationSID | int |
| tLocFeature | LocationSID | int |
| tLocOffshore | LocationSID | int |
| tLocParsedAddress | LocationSID | int |
| tLocStepFunctionXref | LocationSID, StepFunctionSID | int, int |
| tLocTerm | LocTermSID | int |
| tLocWC | LocationSID | int |
| tPortfolio | PortfolioSID | int |
| tPortfolioFilter | PortfolioFilterSID | int |
| tPortfolioReinsuranceTreaty | ReinsuranceTreatySID, PortfolioSID | int, int |
| tProgramBusinessCategoryFactor | ContractSID | int |
| tReinsAppliesToExp | ReinsAppliesToExpSID | int |
| tReinsuranceEPCurve | ReinsuranceEPCurveSID | int |
| tReinsuranceEPCurveAdjustmentPoint | ReinsuranceEPCurveSID | int |
| tReinsuranceEPCurveSet | ReinsuranceEPCurveSetSID | int |
| tReinsuranceExposureAdjustmentFactor | ReinsuranceExposureAdjustmentFactorSID | int |
| tReinsuranceProgram | ReinsuranceProgramSID | int |
| tReinsuranceProgramCoverage | ReinsuranceProgramSID | int |
| tReinsuranceProgramEPAdjustment | ReinsuranceProgramSID | int |
| tReinsuranceProgramEPAdjustmentPoints | ReinsuranceProgramSID | int |
| tReinsuranceProgramJapanCFLRatio | ReinsuranceProgramSID, GeographySID | int, int |
| tReinsuranceProgramSourceInure | ReinsuranceProgramSubjectSID, ReinsuranceProgramSourceInureSID | int, int |
| tReinsuranceProgramTargetXRef | ReinsuranceProgramSID | int |
| tReinsuranceTreaty | ReinsuranceTreatySID | int |
| tReinsuranceTreatyReinstatement | ReinsuranceTreatyReinstatementSID | int |
| tReinsuranceTreatySavedResults | ReinsuranceTreatySavedResultsID | bigint |
| tReinsuranceTreatyTerrorismOption | ReinsuranceTreatySID | int |
| tReinsuranceTreatyUDCClassification_Xref | UDCClassificationSID, ReinsuranceTreatySID | int, int |
| tSIDControl | TableSID | int |
| tStepFunction | StepFunctionSID | int |
| tStepFunctionDetail | StepFunctionSID | int |

### A.3 Foreign Key Relationships (SID Columns)

**Processing order must respect these dependencies — parent tables first!**

| Parent Table | Parent Column | Child Table | Child Column |
|--------------|---------------|-------------|--------------|
| tAppliesToArea | AppliesToAreaSID | tAppliesToAreaGeoXRef | AppliesToAreaSID |
| tAppliesToArea | AppliesToAreaSID | tReinsuranceTreaty | AppliesToAreaSID |
| tAppliesToEventFilter | AppliesToEventFilterSID | tAppliesToEventFilterGeoXRef | AppliesToEventFilterSID |
| tAppliesToEventFilter | AppliesToEventFilterSID | tAppliesToEventFilterLatLong | AppliesToEventFilterSID |
| tAppliesToEventFilter | AppliesToEventFilterSID | tAppliesToEventFilterRule | AppliesToEventFilterSID |
| tCompany | CompanySID | tCompanyCatBondImportHistory | CompanySID |
| tCompany | CompanySID | tCompanyLossAssociation | CompanySID |
| tCompany | CompanySID | tCompanyUDCClassification_Xref | CompanySID |
| tCompany | CompanySID | tExposureSet | CompanySID |
| tCompany | CompanySID | tReinsuranceProgram | CompanySID |
| tCompanyLossAssociation | CompanyLossAssociationSID | tCLFCatalog | CompanyLossAssociationSID |
| tCompanyLossAssociation | CompanyLossAssociationSID | tCompanyLossMarketShare | CompanyLossAssociationSID |
| tCompanyLossAssociation | CompanyLossAssociationSID | tReinsuranceProgramTargetXRef | CompanyLossAssociationSID |
| tContract | ContractSID | tLayer | ContractSID |
| tContract | ContractSID | tLayerCondition | ContractSID |
| tContract | ContractSID | tLocation | ContractSID |
| tContract | ContractSID | tLocTerm | ContractSID |
| tContract | ContractSID | tStepFunction | ContractSID |
| tExposureSet | ExposureSetSID | tAggregateExposure | ExposureSetSID |
| tExposureSet | ExposureSetSID | tContract | ExposureSetSID |
| tExposureSet | ExposureSetSID | tLocation | ExposureSetSID |
| tExposureSet | ExposureSetSID | tReinsAppliesToExp | ExposureSetSID |
| tExposureSet | ExposureSetSID | tReinsuranceProgramTargetXRef | ExposureSetSID |
| tLayer | LayerSID | tLayerCondition | LayerSID |
| tLayerCondition | LayerConditionSID | tLayerCondition | ParentLayerConditionSID |
| tLayerCondition | LayerConditionSID | tLayerConditionLocationXref | LayerConditionSID |
| tLocation | LocationSID | tEngineLocPhysicalProperty | LocationSID |
| tLocation | LocationSID | tLayerConditionLocationXref | LocationSID |
| tLocation | LocationSID | tLocation | ParentLocationSID |
| tLocation | LocationSID | tLocFeature | LocationSID |
| tLocation | LocationSID | tLocOffshore | LocationSID |
| tLocation | LocationSID | tLocParsedAddress | LocationSID |
| tLocation | LocationSID | tLocStepFunctionXref | LocationSID |
| tLocation | LocationSID | tLocTerm | LocationSID |
| tLocation | LocationSID | tLocWC | LocationSID |
| tPortfolio | PortfolioSID | tPortfolioReinsuranceTreaty | PortfolioSID |
| tPortfolioFilter | PortfolioFilterSID | tPortfolio | PortfolioFilterSID |
| tReinsuranceEPCurve | ReinsuranceEPCurveSID | tReinsuranceEPCurveAdjustmentPoint | ReinsuranceEPCurveSID |
| tReinsuranceEPCurveSet | ReinsuranceEPCurveSetSID | tReinsuranceEPCurve | ReinsuranceEPCurveSetSID |
| tReinsuranceProgram | ReinsuranceProgramSID | tCatBondAttributeValue | ReinsuranceProgramSID |
| tReinsuranceProgram | ReinsuranceProgramSID | tReinsuranceProgramSourceInure | ReinsuranceProgramSourceInureSID |
| tReinsuranceProgram | ReinsuranceProgramSID | tReinsuranceProgramSourceInure | ReinsuranceProgramSubjectSID |
| tReinsuranceProgram | ReinsuranceProgramSID | tReinsuranceProgramTargetXRef | ReinsuranceProgramSID |
| tReinsuranceProgram | ReinsuranceProgramSID | tReinsuranceTreaty | ReinsuranceProgramSID |
| tReinsuranceTreaty | ReinsuranceTreatySID | tAppliesToEventFilter | ReinsuranceTreatySID |
| tReinsuranceTreaty | ReinsuranceTreatySID | tReinsuranceTreatyReinstatement | ReinsuranceTreatySID |
| tReinsuranceTreaty | ReinsuranceTreatySID | tReinsuranceTreatyTerrorismOption | ReinsuranceTreatySID |
| tReinsuranceTreaty | ReinsuranceTreatySID | tReinsuranceTreatyUDCClassification_Xref | ReinsuranceTreatySID |
| tStepFunction | StepFunctionSID | tLocStepFunctionXref | StepFunctionSID |
| tStepFunction | StepFunctionSID | tStepFunctionDetail | StepFunctionSID |

### A.4 Self-Referencing Tables (Two-Pass Processing Required)

| Table | PK Column | Self-Reference FK | Notes |
|-------|-----------|-------------------|-------|
| tLocation | LocationSID | ParentLocationSID | Hierarchical locations |
| tLayerCondition | LayerConditionSID | ParentLayerConditionSID | Nested conditions |
| tCompanyLossAssociation | CompanyLossAssociationSID | ParentCompanyLossAssociationSID | Hierarchical loss sets |

### A.5 Recommended Processing Order

Based on FK dependencies, process tables in this order for SID transformation:

**Level 0 (Root tables — no FK dependencies):**
- tCompany
- tPortfolioFilter
- tAppliesToArea
- tReinsuranceEPCurveSet

**Level 1 (Depends on Level 0):**
- tExposureSet (→ tCompany)
- tReinsuranceProgram (→ tCompany)
- tPortfolio (→ tPortfolioFilter)
- tReinsuranceEPCurve (→ tReinsuranceEPCurveSet, tAppliesToArea)

**Level 2 (Depends on Level 1):**
- tContract (→ tExposureSet)
- tReinsuranceTreaty (→ tReinsuranceProgram, tAppliesToArea)
- tCompanyLossAssociation (→ tCompany)

**Level 3 (Depends on Level 2):**
- tLocation (→ tContract, tExposureSet) — **two-pass for self-reference**
- tLayer (→ tContract)
- tStepFunction (→ tContract)
- tAppliesToEventFilter (→ tReinsuranceTreaty)

**Level 4 (Depends on Level 3):**
- tLayerCondition (→ tContract, tLayer) — **two-pass for self-reference**
- tLocTerm, tLocFeature, tLocOffshore, tLocParsedAddress, tLocWC (→ tLocation)
- tStepFunctionDetail (→ tStepFunction)

**Level 5 (Junction/child tables):**
- tLayerConditionLocationXref (→ tLayerCondition, tLocation)
- tLocStepFunctionXref (→ tLocation, tStepFunction)
- All remaining child tables