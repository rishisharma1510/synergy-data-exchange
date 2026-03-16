# Synergy Data Exchange Pipeline — Shared Infrastructure Reference

> **This document covers shared infrastructure, architecture, sizing, SPLA licensing, and cost analysis.**
>
> For pipeline-specific implementation details, see the dedicated plans:
>
> | Pipeline | Document | Flow |
> |----------|----------|------|
> | **Extraction** | [implementation_plan_extraction.md](implementation_plan_extraction.md) | Iceberg → SQL Server → MDF → Tenant S3 |
> | **Ingestion** | [implementation_plan_ingestion.md](implementation_plan_ingestion.md) | Tenant S3 → MDF → SQL Server → Iceberg |

---

## Overview

Both pipelines share the same **three-path infrastructure** (CDK stacks, Docker images, ECS cluster, AWS Batch, Step Functions). They differ in their data direction, entrypoint module, size-estimation mechanism, and Step Function state machine.

| Activity | Flow | Size Estimation | SFN State Machine |
|----------|------|-----------------|-------------------|
| **Extraction** | Iceberg → Tenant S3 | Caller supplies `estimated_size_gb` (from Iceberg metadata) | `get_three_path_extraction_definition()` |
| **Ingestion** | Tenant S3 → Iceberg | Automated via `MeasureArtifactSize` Lambda (`s3:HeadObject`) | `get_three_path_ingestion_definition()` |

Both are submitted via **Client UI/API → Activity Management → Step Functions → Fargate/AWS Batch**.

---

## Implementation Status (Updated 2026-03-10)

| Component | Status | Tests | Notes |
|-----------|--------|-------|-------|
| **SID Transformation Pipeline** | ✅ 100% | 73 | transformer, sid_client, checkpoint, audit, parallel |
| **Exposure API Integration** | ✅ 100% | 20 | exposure_client with override SID mapping |
| **Infrastructure (CDK)** | ✅ 100% | — | 4 stacks: ECR, Fargate, Batch, Step Functions + Monitoring |
| **Iceberg Writer (auto-create)** | ✅ 100% | — | create_table_if_not_exists() |
| **Ingestion Entrypoint** | ✅ 100% | — | Full pipeline with SID + Exposure API + Tenant Context |
| **Extraction Entrypoint** | ✅ 100% | — | BAK/MDF generation + upload + Tenant Context |
| **Check Records Lambda** | ✅ 100% | — | Early-exit optimization — skips ECS if no matching records |
| **Tenant Context Resolution** | ✅ 100% | — | Lambda lookup for glue_database, bucket_name, OAuth creds |
| **Structured JSON Logging** | ✅ 100% | — | _JsonFormatter + _ContextFilter, injects tenant_id + activity_id |
| **Progress Reporter (dual-channel)** | ✅ 100% | — | S3 writes (always) + HTTP callback (optional), background thread |
| **SPLA Monitoring Stack** | ✅ 100% | — | Tracker Lambda + Monthly Reporter Lambda + DynamoDB + CloudWatch |
| **Validation Framework** | ⚠️ 25% | — | Basic row-count + DBCC validation; Lambda-based check missing |
| **Schema Version Detection** | ❌ 0% | — | Blocked on Client schema markers |

**Overall Progress:** ~90% (estimated — see SOW_deliverables_tracker.md for full detail)

**Total Tests:** 93+ passing

See [SOW_deliverables_tracker.md](SOW_deliverables_tracker.md) for detailed breakdown.

See [ARCHITECTURE.md](ARCHITECTURE.md) for current architecture documentation.

---

## System Architecture

### Three-Path Design

The system uses a **size-based routing** approach with **configurable thresholds** for optimal cost and performance:

| Path | Data Size | Infrastructure | SQL Edition | Cost/Run | License |
|------|-----------|----------------|-------------|----------|---------|
| **PATH 1** | ≤9GB | ECS Fargate | Express | ~$0.55-0.65 | FREE |
| **PATH 2** | 9-80GB | ECS Fargate | Standard | ~$0.90-1.20 | SPLA Licensed |
| **PATH 3** | >80GB | AWS Batch EC2 | Standard | ~$1.50-2.50 | SPLA Licensed |

**Note:** Default thresholds are **9 GB** (small) and **80 GB** (large). They are configurable via `ThresholdSmallGb` and `ThresholdLargeGb` in the Step Functions CDK stack (`cdk/src/EES.AP.SDE.CDK/Stacks/SDEStepFunctionsStack.cs`). PATH 2 and PATH 3 require an **SPLA** (Service Provider License Agreement) for SQL Server Standard.

**Key Design Principle:** SQL Server and Python extraction run in the **same container/task** to ensure local filesystem access for MDF files. This eliminates network share dependencies.

### Why This Design

| Decision | Rationale |
|----------|-----------|
| **Same task for SQL + Python** | Local MDF access, no network shares |
| **Linux containers** | Lower cost, faster boot, smaller images |
| **Fargate + Express for small data** | FREE SQL license, fast cold start (~30-60s) |
| **Fargate + Standard for medium data** | Licensed SQL, still serverless, no storage limits |
| **AWS Batch for large data** | Scalable EBS storage, instance right-sizing, 16TB+ |
| **On-Demand EC2 (not provisioned)** | Cost-effective, auto-scaled by Batch |

### Scalability & Performance

| Factor | PATH 1 (Fargate+Express) | PATH 2 (Fargate+Standard) | PATH 3 (Batch EC2) |
|--------|--------------------------|---------------------------|-------------------- |
| **CPU/Memory** | 4 vCPU, 8GB | 8 vCPU, 16GB | 16+ vCPU, 64GB+ |
| **Max Storage** | 200GB ephemeral | 200GB ephemeral | 16TB+ EBS |
| **Cold Start** | ~30-60s | ~30-60s | ~2-5 min |
| **SQL Limit** | 10GB database | No limit | No limit |
| **I/O Performance** | Good | Good | Excellent (provisioned IOPS) |

---

### Architecture Diagram

```
┌─────────────────┐
│  Tenant UI/API  │
└────────┬────────┘
         │ Submit Activity Request
         ▼
┌─────────────────────────────────────────────────────────┐
│               ACTIVITY MANAGEMENT                        │
│  ┌──────────────────┐    ┌──────────────────┐           │
│  │ Extraction       │    │ Ingestion        │           │
│  │ Activity Type    │    │ Activity Type    │           │
│  └────────┬─────────┘    └────────┬─────────┘           │
└───────────┼───────────────────────┼──────────────────────┘
            │ StartExecution        │ StartExecution
            ▼                       ▼
┌─────────────────────────────────────────────────────────┐
│                    STEP FUNCTIONS                        │
│  ┌──────────────────────────────────────────────┐       │
│  │ Size Router (Choice State) - Configurable    │       │
│  └───────┬──────────────┬──────────────┬────────┘       │
│          │ ≤9GB         │ 9-80GB       │ >80GB          │
│          ▼              ▼              ▼                │
│  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐  │
│  │ PATH 1        │ │ PATH 2        │ │ PATH 3        │  │
│  │ Fargate       │ │ Fargate       │ │ AWS Batch     │  │
│  │ Express FREE  │ │ Standard      │ │ EC2 Standard  │  │
│  │ ~$0.55-0.65   │ │ ~$0.90-1.20   │ │ ~$1.50-2.50   │  │
│  └───────────────┘ └───────────────┘ └───────────────┘  │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│         PATH 1: FARGATE + EXPRESS (≤9GB)                │
│  ┌──────────────────────────────────────────────┐       │
│  │ Container 1: SQL Server Express (FREE)       │       │
│  │ Container 2: Python Pipeline (same code)     │       │
│  │ Shared Volume: /sqldata (ephemeral)          │       │
│  │ Resources: 4 vCPU, 8 GB RAM                  │       │
│  └──────────────────────────────────────────────┘       │
│  • FREE SQL license | 10GB database limit               │
│  • Fast cold start (~30-60s)                            │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│         PATH 2: FARGATE + STANDARD (9-80GB)             │
│  ┌──────────────────────────────────────────────┐       │
│  │ Container 1: SQL Server Standard (Licensed)  │       │
│  │ Container 2: Python Pipeline (SAME code)     │       │
│  │ Shared Volume: /sqldata (ephemeral)          │       │
│  │ Resources: 8 vCPU, 16 GB RAM                 │       │
│  └──────────────────────────────────────────────┘       │
│  • Licensed SQL | No size limit | Serverless            │
│  • Fast cold start (~30-60s)                            │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│         PATH 3: AWS BATCH EC2 (>80GB)                   │
│  ┌──────────────────────────────────────────────┐       │
│  │ Compute: On-Demand EC2 (auto-sized)          │       │
│  │ Container 1: SQL Server Standard (Licensed)  │       │
│  │ Container 2: Python Pipeline (SAME code)     │       │
│  │ Storage: EBS Volume (500+ GB, up to 16TB)    │       │
│  │ Resources: 16+ vCPU, 64+ GB RAM              │       │
│  └──────────────────────────────────────────────┘       │
│  • Licensed SQL | No size limit | Provisioned IOPS      │
│  • Instance type selected by Batch                      │
└─────────────────────────────────────────────────────────┘
```

### Data Flow

**Extraction (Iceberg → Tenant S3):**
```
Fargate/Batch Task:
    ├── Read from Iceberg (network → AWS Glue/S3)
    ├── Write to SQL Server (localhost)
    ├── Detach → MDF (local filesystem)
    └── Upload MDF to S3 (network → tenant bucket)
```

**Ingestion (Tenant S3 → Iceberg):**
```
Fargate/Batch Task:
    ├── Download MDF from S3 (network → tenant bucket)
    ├── Attach MDF to SQL Server (local filesystem)
    ├── Read from SQL Server (localhost)
    └── Write to Iceberg (network → AWS Glue/S3)
```

---

## Architecture Changes Summary

### Updated Stack

**Previous:** SQL Server Express (10GB limit), Fargate only  
**New:** Three-path design with size-based routing (configurable thresholds)
- PATH 1: ≤9GB → Fargate + SQL Express (FREE)
- PATH 2: 9-80GB → Fargate + SQL Standard (SPLA Licensed)
- PATH 3: >80GB → AWS Batch EC2 + SQL Standard (SPLA Licensed)

**Previous:** Extraction-only flow  
**New:** Bidirectional - configurable via `mode` parameter

### Infrastructure Components

| Component | PATH 1 (Fargate+Express) | PATH 2 (Fargate+Standard) | PATH 3 (Batch EC2) |
|-----------|--------------------------|---------------------------|--------------------|
| **Compute** | ECS Fargate Task | ECS Fargate Task | AWS Batch (On-Demand EC2) |
| **SQL Server** | Express (FREE) | Standard (Licensed) | Standard (Licensed) |
| **Storage** | Ephemeral 200GB | Ephemeral 200GB | EBS 500GB+ |
| **Cost/Run** | ~$0.55-0.65 | ~$0.90-1.20 | ~$1.50-2.50 |
| **Container Image** | `data-pipeline-express` | `data-pipeline-standard` | `data-pipeline-batch` |

### New Modules for Ingestion

| File | Purpose | Effort |
|------|---------|--------|
| `src/core/sql_reader.py` | Read SQL Server tables as Arrow batches | 4 hours |
| `src/core/iceberg_writer.py` | Write Arrow batches to Iceberg tables | 4 hours |
| `src/fargate/artifact_restorer.py` | RESTORE DATABASE from BAK/ATTACH from MDF | 4 hours |
| `src/fargate/ingestion_entrypoint.py` | Orchestrate ingestion flow | 4 hours |

### Extended Modules

| File | Changes | Effort |
|------|---------|--------|
| `src/fargate/s3_uploader.py` | Add `download_artifact()` method | 2 hours |
| `src/core/column_mapper.py` | Add reverse mapping (SQL → Iceberg) | 2 hours |
| `src/fargate/database_manager.py` | Add `restore_database()` method | 2 hours |
| `docker/Dockerfile.pipeline` | Change `MSSQL_PID=Standard` | 30 min |

**Total Ingestion Implementation:** ~22 hours (~3 days)

---

## Repository Structure (Bidirectional)

```
DataIngestion/
├── docs/
│   ├── architecture_detailed.drawio           # Original extraction-only
│   ├── architecture_executive.drawio          # Original executive view
│   ├── architecture_bidirectional.drawio      # Full bidirectional architecture
│   ├── architecture_executive_bidirectional.drawio  # Executive view
│   ├── implementation_plan.md                 # Original extraction plan
│   ├── implementation_plan_bidirectional.md   # THIS FILE
│   └── sequence_diagram.md
│
├── infra/                             # (Legacy Python - deprecated, use CDK instead)
│   └── ...                            # See cdk/ for infrastructure
│
├── src/
│   ├── core/
│   │   ├── config.py                 # Extraction + Ingestion configs
│   │   ├── iceberg_reader.py         # Read from Iceberg
│   │   ├── iceberg_writer.py         # Write to Iceberg
│   │   ├── sql_writer.py             # Write to SQL Server
│   │   ├── sql_reader.py             # Read from SQL Server
│   │   ├── column_mapper.py          # Bidirectional column mapping
│   │   └── mapping_config.py         # Mapping configuration
│   │
│   ├── fargate/
│   │   ├── entrypoint.py             # Extraction Activity entry (+ JSON logging)
│   │   ├── ingestion_entrypoint.py   # Ingestion orchestration (SID + Exposure API)
│   │   ├── check_records_lambda.py   # Lambda: early-exit if no matching records
│   │   ├── database_manager.py       # CREATE/DROP/RESTORE database
│   │   ├── artifact_generator.py     # Generate MDF/BAK artifacts
│   │   ├── artifact_restorer.py      # RESTORE/ATTACH MDF
│   │   ├── s3_uploader.py            # Upload/download S3 artifacts
│   │   ├── progress_reporter.py      # S3 + HTTP progress callbacks (background thread)
│   │   └── validation.py             # Row-count + DBCC validation
│   │
│   └── models/
│       ├── execution_plan.py         # Extraction + Ingestion plans
│       └── manifest.py               # Artifact manifest
│
├── docker/
│   ├── Dockerfile.express            # PATH 1: SQL Express (≤9GB) - Fargate
│   ├── Dockerfile.standard           # PATH 2: SQL Standard (9-80GB) - Fargate
│   ├── Dockerfile.batch              # PATH 3: SQL Standard (>80GB) - AWS Batch
│   ├── Dockerfile.pipeline           # Legacy (deprecated)
│   ├── docker-compose.yml            # Local testing for all paths
│   ├── docker-compose.local.yml      # Legacy local development
│   ├── entrypoint.sh                 # Unified entrypoint for all paths
│   ├── batch_init.sql                # SQL optimization for PATH 3
│   ├── build.sh                      # Linux/CI build script
│   └── build.ps1                     # Windows build script
│
├── scripts/
│   ├── local_test.py
│   ├── test_extraction_flow.py       # End-to-end extraction test
│   └── test_ingestion_flow.py        # End-to-end ingestion test
│
├── cdk/                               # CDK Infrastructure (C# .NET 8.0)
│   ├── EES.AP.SDE.CDK.sln
│   └── src/
│       ├── EES.AP.SDE.CDK/
│       │   ├── Program.cs             # CDK entry point
│       │   ├── appsettings.json       # Stack configuration
│       │   ├── Services/
│       │   │   └── StackGenerator.cs  # Stack generation logic
│       │   ├── Stacks/
│       │   │   ├── SDEEcrStack.cs         # ECR repositories
│       │   │   ├── SDEFargateStack.cs     # ECS Fargate cluster + tasks
│       │   │   ├── SDEBatchStack.cs       # AWS Batch CE + Queue + Job Def
│       │   │   └── SDEStepFunctionsStack.cs  # State machines
│       │   └── Models/
│       │       └── SDEConfiguration.cs    # Configuration model
│       └── EES.AWSCDK.Common/
│           ├── StackPropertiesFactory.cs         # CDK context → properties
│           └── StackProperties.cs                # Stack props with tags
│
├── .github/
│   └── workflows/
│       ├── events.pull-request.yml    # PR trigger
│       ├── events.merge-group.yml     # Merge trigger → deploy to dev
│       ├── common.find.changes.yml    # Change detection (dorny/paths-filter)
│       ├── common.build.yml           # Build wrapper
│       ├── common.build.Synergy Data Exchange.yml  # Build CDK + Docker images
│       ├── common.deploy.yml          # Deploy CDK stacks + push images
│       ├── common.getRequiredVariables.yml  # Environment variables
│       ├── common.release.generate.yml      # Create GitHub release
│       ├── common.release.manifests.yml     # Generate artifact manifests
│       ├── common.promote.prerelease.yml    # Promote prerelease
│       └── manual.deploy.yml          # Manual deployment trigger
```

### CDK Stack Naming Convention

All stacks and AWS resources follow the `ss-cdk-{name}-{environment}` pattern:

| Stack | Example Name | Description |
|-------|--------------|-------------|
| ECR Stack | `ss-cdk-sde-ecr-dev` | ECR repositories |
| Fargate Stack | `ss-cdk-sde-fargate-dev` | ECS cluster + task definitions |
| Batch Stack | `ss-cdk-sde-batch-dev` | Batch CE + queue + job def |
| Step Functions Stack | `ss-cdk-sde-sfn-dev` | State machines |

### AWS Resource Naming Convention

| Resource Type | Pattern | Example |
|---------------|---------|---------|
| ECR Repository | `ss-cdk{stage}-sde-{image}` | `ss-cdkdev-sde-express` |
| ECS Cluster | `ss-cdk{stage}-sde-cluster` | `ss-cdkdev-sde-cluster` |
| Batch Queue | `ss-cdk{stage}-sde-batch-queue` | `ss-cdkdev-sde-batch-queue` |
| Step Function | `ss-cdk{stage}-sde-{extraction|ingestion}` | `ss-cdkdev-sde-extraction` |
| Log Group | `/aws/{service}/ss-cdk{stage}-sde-*` | `/aws/batch/ss-cdkdev-sde-batch` |

---

## Docker Images

### Three Images for Three Paths

| Image | Dockerfile | SQL Edition | Target | Use Case |
|-------|------------|-------------|--------|----------|
| `data-pipeline-express` | Dockerfile.express | Express (FREE) | Fargate | ≤9GB datasets |
| `data-pipeline-standard` | Dockerfile.standard | Standard | Fargate | 9-80GB datasets |
| `data-pipeline-batch` | Dockerfile.batch | Standard | AWS Batch | >80GB datasets |

### Building Images

```powershell
# Build all images locally
.\docker\build.ps1

# Build specific image
.\docker\build.ps1 -Path express
.\docker\build.ps1 -Path standard
.\docker\build.ps1 -Path batch

# Build and push to ECR
.\docker\build.ps1 -Push -Tag v1.0.0

# Linux/CI
./docker/build.sh --push --tag v1.0.0
```

### Local Testing with Docker Compose

```powershell
# Test PATH 1: Express (≤9GB)
$env:SA_PASSWORD="YourStrong@Passw0rd"
$env:PIPELINE_MODE="extraction"
docker-compose -f docker/docker-compose.yml --profile express up --build

# Test PATH 2: Standard (9-80GB)
docker-compose -f docker/docker-compose.yml --profile standard up --build

# Test PATH 3: Batch (>80GB)
docker-compose -f docker/docker-compose.yml --profile batch up --build

# Test ingestion mode
$env:PIPELINE_MODE="ingestion"
docker-compose -f docker/docker-compose.yml --profile express up --build
```

### ECR Repository Setup

```bash
# Create ECR repositories for each image
aws ecr create-repository --repository-name data-pipeline-express
aws ecr create-repository --repository-name data-pipeline-standard
aws ecr create-repository --repository-name data-pipeline-batch
```

---

## Size-Based Routing

The system uses **size-based routing** to select the optimal execution path. Extraction and Ingestion pipelines measure size in fundamentally different ways — extraction relies on a pre-computed estimate supplied by the caller, while ingestion uses an automated Lambda measurement step inside the Step Function.

---

### Extraction: Caller-Supplied Size Estimate

For the **Extraction pipeline**, the size is **not measured by the Step Function itself**. The caller (Activity Management) must include `estimated_size_gb` in the Step Function input payload. This value is derived by querying Iceberg table metadata before launching the SFN execution:

```
Activity Management:
  1. Query Iceberg metadata (Glue Data Catalog) to sum parquet file sizes
     for all exposure_ids in the request
  2. Compute estimated_size_gb  =  sum(parquet_file_sizes) / 1e9
  3. Include in SFN StartExecution input:
     { "estimated_size_gb": 12.4, "exposure_ids": [...], ... }
```

Inside the Step Function, the SFN **Choice state** (`RouteBySize`) evaluates `$.estimated_size_gb` directly as a numeric comparison:

```
Extraction SFN flow:
  CheckRecords (Lambda)
      → EvaluateRecordCount (Choice)
          → [no records] → NoRecordsFound (Succeed)
          → [has records] → RouteBySize (Choice on $.estimated_size_gb)
              → ≤ ThresholdSmallGb (9 GB)  → RunFargateExpress  (MSSQL_PID=Express)
              → ≤ ThresholdLargeGb (80 GB) → RunFargateStandard (MSSQL_PID=Standard)
              → > ThresholdLargeGb (80 GB) → RunBatchEC2        (MSSQL_PID=Standard)
```

---

### Ingestion: Automated Size Estimator Lambda

For the **Ingestion pipeline**, the Step Function **automatically measures** the artifact size using a dedicated **Size Estimator Lambda** before routing. The caller does **not** need to supply a size value — the SFN handles it internally.

```
Ingestion SFN flow:
  PrepareIngestionInput (Pass - injects ingestion mode flags)
      → MeasureArtifactSize (Lambda Task)
            Input:  s3_input_path = "s3://{source_s3_bucket}/{source_s3_key}"
            Action: s3:HeadObject → reads Content-Length of the .mdf / .bak artifact
            Output: {
                       "selected_path": "EXPRESS" | "STANDARD" | "BATCH",
                       "sde_context": { "tenant_role": "...", ... }
                    }
      → RouteBySize (Choice on $.selected_path STRING — not numeric)
          → "EXPRESS"  → RunFargateExpress  (MSSQL_PID=Express)
          → "STANDARD" → RunFargateStandard (MSSQL_PID=Standard)
          → "BATCH"    → RunBatchEC2        (MSSQL_PID=Standard)
```

The Size Estimator Lambda applies the same thresholds (`ThresholdSmallGb`, `ThresholdLargeGb`) and returns a string label rather than a numeric value. This decouples the routing decision from the Step Function Choice state conditions.

> **Key difference:** Extraction routes on a *numeric* `$.estimated_size_gb` field; Ingestion routes on a *string* `$.selected_path` field returned by the Size Estimator Lambda.

---

### SQL Edition per Path

The `MSSQL_PID` environment variable is injected into the container at runtime by the Step Function:

| Path | `MSSQL_PID` | SQL Edition | License Required |
|------|-------------|-------------|-----------------|
| PATH 1 (≤9 GB) | `Express` | SQL Server Express | **None — FREE** |
| PATH 2 (9-80 GB) | `Standard` | SQL Server Standard | **SPLA** |
| PATH 3 (>80 GB) | `Standard` | SQL Server Standard | **SPLA** |

---

### Configurable Thresholds

Thresholds are set in the CDK stack and passed into the Step Function as environment configuration:

```csharp
// cdk/src/EES.AP.SDE.CDK/Stacks/SDEStepFunctionsStack.cs
private const int ThresholdSmallGb = 9;   // ≤9GB → PATH 1 (Fargate + Express)
private const int ThresholdLargeGb = 80;  // >80GB → PATH 3 (Batch EC2)

// Thresholds are used in Step Function Choice state conditions
// Also defined as Python defaults in infra/step_functions.py:
// DEFAULT_THRESHOLD_SMALL_GB = 9
// DEFAULT_THRESHOLD_LARGE_GB = 80
```

### Step Function Routing Logic (Extraction)

```python
# Extraction: Step Function Choice state routes on numeric $.estimated_size_gb
if estimated_size_gb <= threshold_small_gb:      # Default: 9 GB
    route = "PATH 1: Fargate + Express (FREE)"
    MSSQL_PID = "Express"
elif estimated_size_gb <= threshold_large_gb:    # Default: 80 GB
    route = "PATH 2: Fargate + Standard (SPLA Licensed)"
    MSSQL_PID = "Standard"
else:  # > threshold_large_gb                   # > 80 GB
    route = "PATH 3: Batch EC2 + Standard (SPLA Licensed)"
    MSSQL_PID = "Standard"
```

### Step Function Routing Logic (Ingestion)

```python
# Ingestion: MeasureArtifactSize Lambda returns selected_path STRING
# Step Function Choice state routes on $.selected_path
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

### AWS Batch Configuration

```csharp
// cdk/src/EES.AP.SDE.CDK/Stacks/SDEBatchStack.cs
var computeEnvironment = new CfnComputeEnvironment(this, "ComputeEnv", new CfnComputeEnvironmentProps
{
    ComputeEnvironmentName = $"ss-cdk{stage}-sde-batch-ce",
    Type = "MANAGED",
    ComputeResources = new CfnComputeEnvironment.ComputeResourcesProperty
    {
        Type = "EC2",
        AllocationStrategy = "BEST_FIT_PROGRESSIVE",
        MinvCpus = 0,
        MaxvCpus = 256,
        InstanceTypes = new[] { "optimal" },
        Subnets = vpc.PrivateSubnets.Select(s => s.SubnetId).ToArray(),
        SecurityGroupIds = new[] { securityGroup.SecurityGroupId },
        // Launch Template for EBS volumes attached at runtime
        LaunchTemplate = new CfnComputeEnvironment.LaunchTemplateSpecificationProperty
        {
            LaunchTemplateId = launchTemplate.Ref,
        },
    },
});

// Job Definition with ECS properties for container configuration
var jobDefinition = new CfnJobDefinition(this, "JobDef", new CfnJobDefinitionProps
{
    JobDefinitionName = $"ss-cdk{stage}-sde-batch-jobdef",
    Type = "container",
    PlatformCapabilities = new[] { "EC2" },
    ContainerProperties = new CfnJobDefinition.ContainerPropertiesProperty
    {
        Image = ecrRepository.RepositoryUri,
        Vcpus = 4,
        Memory = 16384,
        MountPoints = new[] {
            new CfnJobDefinition.MountPointsProperty {
                ContainerPath = "/data",
                SourceVolume = "sqldata",
            }
        },
        Volumes = new[] {
            new CfnJobDefinition.VolumesProperty {
                Name = "sqldata",
                Host = new CfnJobDefinition.VolumesHostProperty {
                    SourcePath = "/data",  // Mounted from EBS via Launch Template
                }
            }
        },
    },
});
```

---

## Detailed Module Specifications

### 3.1 `sql_reader.py` — Read SQL Server as Arrow

```python
"""
Read SQL Server tables and stream as PyArrow batches.
Reverse of sql_writer.py - used for ingestion flow.
"""

from typing import Iterator, List, Optional
import pyarrow as pa
import pyodbc

class SqlServerReader:
    """Read SQL Server tables as Arrow record batches."""
    
    def __init__(self, config: SqlServerConfig, database: str):
        self.config = config
        self.database = database
        self._connection: Optional[pyodbc.Connection] = None
    
    def connect(self) -> None:
        """Establish connection to SQL Server."""
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.config.host},{self.config.port};"
            f"DATABASE={self.database};"
            f"UID={self.config.user};"
            f"PWD={self.config.password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout={self.config.connection_timeout};"
        )
        self._connection = pyodbc.connect(conn_str)
    
    def list_tables(self) -> List[str]:
        """List all user tables in the database."""
        cursor = self._connection.cursor()
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
        """)
        return [row[0] for row in cursor.fetchall()]
    
    def stream_table(
        self, 
        table_name: str, 
        batch_size: int = 10000
    ) -> Iterator[pa.RecordBatch]:
        """
        Stream table data as Arrow record batches.
        Uses server-side cursor for memory efficiency.
        """
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT * FROM [{table_name}]")
        
        # Get column info
        columns = [desc[0] for desc in cursor.description]
        
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            
            # Convert to Arrow
            arrays = []
            for col_idx in range(len(columns)):
                col_data = [row[col_idx] for row in rows]
                arrays.append(pa.array(col_data))
            
            yield pa.RecordBatch.from_arrays(arrays, names=columns)
    
    def get_row_count(self, table_name: str) -> int:
        """Get total row count for a table."""
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
        return cursor.fetchone()[0]
    
    def close(self) -> None:
        """Close connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
```

### 3.2 `iceberg_writer.py` — Write to Iceberg Tables

```python
"""
Write Arrow record batches to Iceberg tables.
Reverse of iceberg_reader.py - used for ingestion flow.
"""

from typing import List, Optional
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from pyiceberg.exceptions import NoSuchTableError

class IcebergWriter:
    """Write Arrow data to Iceberg tables via Glue Catalog."""
    
    def __init__(self, config: IcebergConfig):
        self.config = config
        self._catalog = None
    
    @property
    def catalog(self):
        if self._catalog is None:
            self._catalog = load_catalog(
                "glue",
                **{
                    "type": "glue",
                    "glue.region": self.config.region,
                }
            )
        return self._catalog
    
    def table_exists(self, namespace: str, table_name: str) -> bool:
        """Check if table exists in catalog."""
        try:
            self.catalog.load_table(f"{namespace}.{table_name}")
            return True
        except NoSuchTableError:
            return False
    
    def create_table(
        self, 
        namespace: str, 
        table_name: str, 
        schema: pa.Schema,
        location: Optional[str] = None
    ) -> Table:
        """
        Create a new Iceberg table with the given Arrow schema.
        """
        from pyiceberg.schema import Schema
        from pyiceberg.types import (
            BooleanType, IntegerType, LongType, FloatType, 
            DoubleType, StringType, DateType, TimestampType, 
            DecimalType, BinaryType
        )
        
        # Convert Arrow schema to Iceberg schema
        iceberg_fields = []
        for i, field in enumerate(schema):
            iceberg_type = self._arrow_to_iceberg_type(field.type)
            iceberg_fields.append(
                (i + 1, field.name, iceberg_type, not field.nullable)
            )
        
        iceberg_schema = Schema(*iceberg_fields)
        
        # Create table
        table = self.catalog.create_table(
            identifier=f"{namespace}.{table_name}",
            schema=iceberg_schema,
            location=location or f"s3://{self.config.s3_bucket}/{namespace}/{table_name}",
        )
        return table
    
    def append(
        self, 
        namespace: str, 
        table_name: str, 
        batches: List[pa.RecordBatch]
    ) -> int:
        """
        Append Arrow batches to an existing Iceberg table.
        Returns total rows written.
        """
        table = self.catalog.load_table(f"{namespace}.{table_name}")
        
        # Combine batches into a single table for writing
        arrow_table = pa.Table.from_batches(batches)
        
        # Append to Iceberg
        table.append(arrow_table)
        
        return arrow_table.num_rows
    
    def overwrite(
        self, 
        namespace: str, 
        table_name: str, 
        batches: List[pa.RecordBatch]
    ) -> int:
        """
        Overwrite an Iceberg table with new data.
        Returns total rows written.
        """
        table = self.catalog.load_table(f"{namespace}.{table_name}")
        
        arrow_table = pa.Table.from_batches(batches)
        
        # Overwrite (replaces all data)
        table.overwrite(arrow_table)
        
        return arrow_table.num_rows
    
    def _arrow_to_iceberg_type(self, arrow_type: pa.DataType):
        """Convert Arrow type to Iceberg type."""
        from pyiceberg.types import (
            BooleanType, IntegerType, LongType, FloatType,
            DoubleType, StringType, DateType, TimestampType,
            DecimalType, BinaryType
        )
        
        type_map = {
            pa.bool_(): BooleanType(),
            pa.int8(): IntegerType(),
            pa.int16(): IntegerType(),
            pa.int32(): IntegerType(),
            pa.int64(): LongType(),
            pa.float32(): FloatType(),
            pa.float64(): DoubleType(),
            pa.string(): StringType(),
            pa.large_string(): StringType(),
            pa.date32(): DateType(),
            pa.date64(): DateType(),
            pa.binary(): BinaryType(),
            pa.large_binary(): BinaryType(),
        }
        
        if arrow_type in type_map:
            return type_map[arrow_type]
        
        if isinstance(arrow_type, pa.TimestampType):
            return TimestampType()
        
        if isinstance(arrow_type, pa.Decimal128Type):
            return DecimalType(arrow_type.precision, arrow_type.scale)
        
        # Default to string for unknown types
        return StringType()
```

### 3.3 `artifact_restorer.py` — Restore BAK/MDF

```python
"""
Restore SQL Server database from BAK file or attach from MDF.
Reverse of artifact_generator.py - used for ingestion flow.
"""

import os
import hashlib
from pathlib import Path
from typing import Optional
import pyodbc

class ArtifactRestorer:
    """Restore SQL Server databases from backup artifacts."""
    
    def __init__(self, config: SqlServerConfig, data_dir: str = "/data"):
        self.config = config
        self.data_dir = Path(data_dir)
        self.backup_dir = self.data_dir / "backup"
        self.mdf_dir = self.data_dir / "mdf"
        self._connection: Optional[pyodbc.Connection] = None
    
    def connect(self) -> None:
        """Connect to master database for restore operations."""
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.config.host},{self.config.port};"
            f"DATABASE=master;"
            f"UID={self.config.user};"
            f"PWD={self.config.password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout={self.config.connection_timeout};"
        )
        self._connection = pyodbc.connect(conn_str, autocommit=True)
    
    def verify_checksum(self, file_path: Path, expected_checksum: str) -> bool:
        """Verify file integrity using SHA256."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest() == expected_checksum
    
    def restore_from_bak(
        self, 
        bak_file: str, 
        database_name: str,
        expected_checksum: Optional[str] = None
    ) -> str:
        """
        Restore database from .bak file.
        
        Args:
            bak_file: Path to .bak file (relative to backup_dir or absolute)
            database_name: Name for the restored database
            expected_checksum: Optional SHA256 checksum to verify
            
        Returns:
            Restored database name
        """
        bak_path = Path(bak_file)
        if not bak_path.is_absolute():
            bak_path = self.backup_dir / bak_file
        
        if not bak_path.exists():
            raise FileNotFoundError(f"BAK file not found: {bak_path}")
        
        # Verify checksum if provided
        if expected_checksum:
            if not self.verify_checksum(bak_path, expected_checksum):
                raise ValueError(f"Checksum mismatch for {bak_path}")
        
        cursor = self._connection.cursor()
        
        # Get logical file names from backup
        cursor.execute(f"""
            RESTORE FILELISTONLY FROM DISK = N'{bak_path}'
        """)
        files = cursor.fetchall()
        
        # Build RESTORE command with MOVE clauses
        mdf_path = self.mdf_dir / f"{database_name}.mdf"
        ldf_path = self.mdf_dir / f"{database_name}_log.ldf"
        
        move_clauses = []
        for file_info in files:
            logical_name = file_info[0]
            file_type = file_info[2]  # 'D' for data, 'L' for log
            if file_type == 'D':
                move_clauses.append(f"MOVE N'{logical_name}' TO N'{mdf_path}'")
            else:
                move_clauses.append(f"MOVE N'{logical_name}' TO N'{ldf_path}'")
        
        restore_sql = f"""
            RESTORE DATABASE [{database_name}]
            FROM DISK = N'{bak_path}'
            WITH 
                {', '.join(move_clauses)},
                REPLACE,
                RECOVERY
        """
        
        cursor.execute(restore_sql)
        
        return database_name
    
    def attach_from_mdf(
        self, 
        mdf_file: str, 
        database_name: str,
        ldf_file: Optional[str] = None
    ) -> str:
        """
        Attach database from .mdf file.
        
        Args:
            mdf_file: Path to .mdf file
            database_name: Name for the attached database
            ldf_file: Optional path to .ldf file
            
        Returns:
            Attached database name
        """
        mdf_path = Path(mdf_file)
        if not mdf_path.is_absolute():
            mdf_path = self.mdf_dir / mdf_file
        
        if not mdf_path.exists():
            raise FileNotFoundError(f"MDF file not found: {mdf_path}")
        
        cursor = self._connection.cursor()
        
        if ldf_file:
            ldf_path = Path(ldf_file)
            if not ldf_path.is_absolute():
                ldf_path = self.mdf_dir / ldf_file
            
            attach_sql = f"""
                CREATE DATABASE [{database_name}]
                ON (FILENAME = N'{mdf_path}'),
                   (FILENAME = N'{ldf_path}')
                FOR ATTACH
            """
        else:
            # Attach with rebuild log
            attach_sql = f"""
                CREATE DATABASE [{database_name}]
                ON (FILENAME = N'{mdf_path}')
                FOR ATTACH_REBUILD_LOG
            """
        
        cursor.execute(attach_sql)
        
        return database_name
    
    def close(self) -> None:
        """Close connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
```

### 3.4 `ingestion_entrypoint.py` — Ingestion Activity Flow

```python
"""
Fargate/Batch task entrypoint for INGESTION activity.
Triggered by: Activity Management → Step Functions → Fargate/Batch

Flow: Tenant S3 → Download BAK → RESTORE → SID Transform → Write to Iceberg

Credential Handling:
    ECS FARGATE: TaskRoleArn assigned by Step Functions. No STS AssumeRole needed.
    AWS BATCH:   Job role is fixed. TENANT_ROLE_ARN env var injected by Step Functions.
                 S3Uploader uses STS AssumeRole with that role ARN.
"""


@dataclass
class IngestionInput:
    """Input payload for ingestion activity (from Activity Management)."""
    tenant_id: str
    activity_id: str
    run_id: str                          # Auto-generated if not provided
    source_s3_bucket: str
    source_s3_key: str                   # S3 URI prefix stripped automatically
    artifact_type: str = "bak"           # "bak" | "mdf"
    checksum: Optional[str] = None       # Expected SHA256 (for verification)
    source_role_arn: Optional[str] = None  # STS role for cross-account (Batch mode)
    target_namespace: str = "default"    # Resolved from Tenant Context Lambda
    tables: List[str] = field(default_factory=list)  # Empty = all tables
    create_if_not_exists: bool = False   # DEPRECATED — destination tables never created
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
        role_arn = input_payload.source_role_arn  # None for Fargate, set for Batch
        uploader = S3Uploader(tenant_id=input_payload.tenant_id, role_arn=role_arn)
        local_artifact_path = uploader.download_artifact(
            bucket=input_payload.source_s3_bucket,
            key=input_payload.source_s3_key,
            local_dir="/data/backup",
        )

        # 3. Restore database
        reporter.report(20, "RESTORING")
        restorer = ArtifactRestorer(cfg.sqlserver)
        restorer.connect()
        db_name = f"ingest_{input_payload.tenant_id}_{input_payload.run_id[:8]}"

        if input_payload.artifact_type == "bak":
            db_name = restorer.restore_from_bak(
                bak_file=local_artifact_path,
                database_name=db_name,
                expected_checksum=input_payload.checksum,
            )
        else:
            db_name = restorer.attach_from_mdf(
                mdf_file=local_artifact_path,
                database_name=db_name,
            )

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

        # Always uses SID-aware processing path (dependency-ordered)
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

        # 6. Write summary JSON to S3 (read back by Step Functions / UI polls)
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

        # 7. Report COMPLETED
        reporter.report(100, "COMPLETED", manifest=summary)
        reporter.flush()

        # Raise if any tables failed (non-zero exit → Step Functions task failure)
        failed = [r["table"] for r in ingestion_results if r["status"] == "failed"]
        if failed:
            raise RuntimeError(f"{len(failed)} table(s) failed: {', '.join(failed)}")

    except Exception as e:
        reporter.report(0, "FAILED", error=str(e))
        reporter.flush()
        # Cleanup on failure (best-effort)
        if sql_reader: sql_reader.close()
        if restorer and db_name: restorer.cleanup_database(db_name)
        raise


def _process_tables_with_sid(tables, sql_reader, ice_writer, mapper, cfg, input_payload, reporter):
    """
    Process tables in FK-dependency order with SID transformation.

    Steps per table:
    1. (Optional) Call Exposure API to register new ExposureSet/View in central system
    2. Read full table from restored SQL Server database into PyArrow
    3. Allocate SID range from SID API for tables in SID_CONFIG
    4. Remap primary key column (old SID → new allocated SID)
    5. Cascade FK updates using already-transformed parent table mappings
    6. Transform column names via ColumnMapper
    7. Write transformed Arrow table to Iceberg (append or overwrite)
    """
    from src.sid.transformer import create_transformer
    from src.sid.config import get_processing_order, SID_CONFIG
    from src.sid.exposure_client import create_exposure_client, extract_exposure_metadata

    # Create SID transformer (mock or production based on SIDConfig.use_mock)
    sid_transformer = create_transformer(
        use_mock=cfg.sid.use_mock,
        starting_sid=cfg.sid.mock_starting_sid,
        sid_api_url=cfg.sid.api_url,
        tenant_context_lambda_name=cfg.sid.tenant_context_lambda_name,
        app_token_secret_name=cfg.sid.app_token_secret_name,
        max_retries=cfg.sid.max_retries,
    )

    exposure_sets_info = []

    # Optional Exposure API call (EXPOSURE_API_URL baked into task definition)
    exposure_api_url = os.getenv("EXPOSURE_API_URL")
    if exposure_api_url:
        reporter.report(32, "EXPOSURE_API")
        # Pre-read tExposureSet / tExposureView from restored database
        exposure_tables_data = {}
        for tbl in ["tExposureSet", "tExposureView"]:
            if tbl in set(tables):
                batches = list(sql_reader.stream_table(tbl, batch_size=cfg.ingestion.batch_size))
                if batches:
                    exposure_tables_data[tbl] = pa.Table.from_batches(batches)

        if exposure_tables_data:
            metadata = extract_exposure_metadata(
                tables_data=exposure_tables_data,
                tenant_id=input_payload.tenant_id,
                run_id=input_payload.run_id,
                source_artifact=input_payload.source_s3_key,
            )
            exposure_client = create_exposure_client(
                api_url=exposure_api_url,
                use_mock=cfg.sid.use_mock,
                tenant_id=input_payload.tenant_id,
                tenant_context_lambda_name=cfg.sid.tenant_context_lambda_name,
                app_token_secret_name=cfg.sid.app_token_secret_name,
            )
            exposure_response = exposure_client.create_exposure(metadata)
            # Override SID so all tExposureSet references use the central SID
            sid_transformer.set_override_sid("tExposureSet", exposure_response.exposure_set_sid)
            for es in exposure_response.exposure_sets:
                exposure_sets_info.append({"name": es.name, "sid": es.sid})

    # Build dependency-ordered processing list from SID_CONFIG FK hierarchy
    processing_order = get_processing_order()  # [(level, table_name), ...]
    tables_set = set(tables)
    ordered_tables = [(lvl, t) for lvl, t in processing_order if t in tables_set]
    # Append tables not in SID_CONFIG at the end (level=-1, no SID transform)
    known = {t for _, t in ordered_tables}
    ordered_tables += [(-1, t) for t in tables if t not in known]

    ingestion_results = []

    for idx, (level, table_name) in enumerate(ordered_tables):
        progress = 30 + int((idx / max(len(ordered_tables), 1)) * 50)
        reporter.report(progress, f"SID L{level}: {table_name}" if level >= 0 else f"INGESTING: {table_name}")

        try:
            batches = list(sql_reader.stream_table(table_name, batch_size=cfg.ingestion.batch_size))
            if not batches:
                ingestion_results.append({"table": table_name, "status": "skipped", "reason": "empty", "rows": 0})
                continue

            arrow_table = pa.Table.from_batches(batches)

            # Apply SID transformation if table is in config
            sid_range_str = None
            if level >= 0 and table_name in SID_CONFIG:
                result = sid_transformer.transform_table(table_name, arrow_table)
                if result.error:
                    raise ValueError(f"SID transform failed: {result.error}")
                arrow_table = sid_transformer.get_transformed_table(table_name)
                sid_range_str = f"{result.sid_range_start}-{result.sid_range_end}"

            # Skip table if not present in destination Iceberg namespace
            if not ice_writer.table_exists(input_payload.target_namespace, table_name):
                ingestion_results.append({
                    "table": table_name, "status": "skipped",
                    "reason": "table_not_found_in_destination", "rows": 0,
                })
                continue

            # Transform column names and write to Iceberg
            transformed_batches = [_transform_batch_columns(b, mapper) for b in arrow_table.to_batches()]
            if input_payload.write_mode == "overwrite":
                rows_written = ice_writer.overwrite(input_payload.target_namespace, table_name, transformed_batches)
            else:
                rows_written = ice_writer.append(input_payload.target_namespace, table_name, transformed_batches)

            ingestion_results.append({
                "table": table_name, "status": "success", "rows": rows_written,
                "sid_transformed": level >= 0, "sid_range": sid_range_str,
            })

        except Exception as e:
            ingestion_results.append({"table": table_name, "status": "failed", "error": str(e), "rows": 0})

    return ingestion_results, exposure_sets_info
```


---

## 4. Configuration Reference

### `config.py` — Key Config Dataclasses

The `AppConfig` returned by `load_config()` contains:

```python
@dataclass
class SIDConfig:
    """SID transformation API configuration."""
    api_url: str = ""                          # SID API base URL
    tenant_context_lambda_name: str = "..."    # Lambda for OAuth credential lookup
    app_token_secret_name: str = "..."         # Secrets Manager key for app token
    identity_provider_token_endpoint: str = "" # Okta token endpoint URL
    max_retries: int = 3
    retry_backoff_base: float = 1.0
    use_mock: bool = False         # True in local/unit tests
    mock_starting_sid: int = 1_000_000


@dataclass
class AppConfig:
    iceberg: IcebergConfig
    sqlserver: SqlServerConfig
    ingestion: IngestionConfig    # batch_size, write_backend, naming_convention, etc.
    sid: SIDConfig
```

All values are read from environment variables by `load_config()`. Key env vars:

| Env Var | Config Field | Default |
|---------|-------------|---------|
| `SID_API_URL` | `sid.api_url` | `""` |
| `TENANT_CONTEXT_LAMBDA` | `sid.tenant_context_lambda_name` | stage-specific |
| `APP_TOKEN_SECRET` | `sid.app_token_secret_name` | stage-specific |
| `IDENTITY_PROVIDER_TOKEN_ENDPOINT` | `sid.identity_provider_token_endpoint` | `""` |
| `SID_USE_MOCK` | `sid.use_mock` | `false` |
| `EXPOSURE_API_URL` | _(read directly from env)_ | `""` (baked into task def) |
| `MAX_WORKERS` | _(read directly from env)_ | `4` |
| `BATCH_SIZE` | `ingestion.batch_size` | `10000` |

---

## 5. Check Records Lambda (Early Exit Optimization)

**File:** `src/fargate/check_records_lambda.py`  
**Invoked by:** Step Functions (Lambda Task state) **before** launching any ECS/Batch task.

### Purpose

Avoids spinning up an expensive Fargate or Batch EC2 task when the requested `exposure_ids` have no matching records in Iceberg. This is a pure cost-saving measure.

### Input / Output

```python
# Input (passed directly from Step Functions):
{
    "tenant_id": "...",
    "activity_id": "...",
    "run_id": "...",
    "exposure_ids": [1, 2, 3],
    "tables": ["TContract", "TExposure"],  # [] = all tables
    "target_namespace": "default"
}

# Output (merged back into SFN state):
{
    ...original input...,
    "record_check": {
        "has_records": True | False,
        "record_count": 1500,
        "tables_checked": 5
    }
}
```

### Routing Logic

```
Step Functions Choice state:
  record_check.has_records == false  →  Succeed  (no-op, no ECS task launched)
  record_check.has_records == true   →  RunTask  (size-router → PATH 1/2/3)
```

The scan is lightweight — it uses PyIceberg's row-filter pushdown with `ExposureSetSID IN (...)` predicate and stops as soon as any matching row is found.

---

## 6. Tenant Context Resolution

**File:** `src/core/tenant_context.py`  
**Called by:** Both `entrypoint.py` (extraction) and `ingestion_entrypoint.py` (ingestion) at startup.

### Purpose

Resolves per-tenant configuration from the shared **Tenant Context Lambda** (an existing AWS service separate from Synergy Data Exchange). This provides:

| Field | Description |
|-------|-------------|
| `glue_database` | Iceberg/Glue namespace for this tenant (e.g. `rs_cdkdev_dclegend01_exposure_db`) |
| `bucket_name` | Tenant's S3 bucket |
| `tenant_role` | IAM role ARN for cross-account access |
| `client_id` | OAuth client ID for SID / Exposure API calls |
| `client_secret` | OAuth client secret |

### Credential Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│  ECS FARGATE (TaskRoleArn assigned at runtime by Step Functions):         │
│  1. Step Functions assigns TaskRoleArn = tenant_role to the task          │
│  2. Task runs directly with tenant credentials (no STS AssumeRole)        │
│  3. OAuth: task calls get_okta_credentials() → Okta token endpoint        │
│                                                                            │
│  AWS BATCH (job role is fixed — cannot be overridden at job-level):       │
│  1. Step Functions passes TENANT_ROLE_ARN as an env var                   │
│  2. Task calls STS AssumeRole with TENANT_ROLE_ARN                        │
│  3. OAuth: same Tenant Context Lambda → Okta flow                         │
└──────────────────────────────────────────────────────────────────────────┘
```

### Context Caching

Results are cached in-process with a 50-minute TTL (`CACHE_TTL_SECONDS = 3000`) to avoid repeated Lambda invocations within the same container lifecycle.

```python
# Usage in entrypoint
from src.core.tenant_context import init_sde_context

tenant_context = init_sde_context(tenant_id)
cfg.iceberg.namespace = tenant_context["glue_database"]  # e.g. "rs_cdkdev_dclegend01_exposure_db"
bucket_name = tenant_context["bucket_name"]
```

---

## 7. Structured JSON Logging

**Implemented in:** `src/fargate/entrypoint.py` (`_JsonFormatter`, `_ContextFilter`)

### Purpose

All log lines from the extraction task are emitted as single-line JSON objects for structured log ingestion (CloudWatch Logs Insights, Dynatrace, etc.).

```json
{
  "timestamp": "2026-03-10T14:23:01Z",
  "level": "INFO",
  "logger": "src.fargate.entrypoint",
  "tenant_id": "dclegend01",
  "activity_id": "act-abc123",
  "message": "[STEP 3/7] Table load complete in 42.3s — 15 tables, 2,450,000 total rows"
}
```

### Key Design Points

- `_ContextFilter` injects `tenant_id` and `activity_id` into **every** log record from every module (attached to the root handler, not per-logger)
- `_JsonFormatter` strips the old `[tenant=... activity=...]` prefix from message text (data moved to structured fields)
- Child loggers (`sql_writer`, `iceberg_reader`, etc.) automatically inherit the tenant/activity context via log propagation

---

## 8. Activity Management → Step Functions Integration

### How Activities Are Triggered

```
Tenant UI/API
     │
     │ POST /activities (activity_type: "extraction" or "ingestion")
     ▼
┌──────────────────────────────────┐
│   ACTIVITY MANAGEMENT API     │
│                                │
│  1. Validate request           │
│  2. Create activity record     │
│  3. StartExecution on SFN      │
│  4. Return activity_id         │
└────────────────┬─────────────────┘
                 │
                 │ Step Functions StartExecution
                 ▼
┌──────────────────────────────────┐
│   STEP FUNCTIONS              │
│   (per activity type)         │
└────────────────┬─────────────────┘
                 │
                 │ ECS RunTask
                 ▼
┌──────────────────────────────────┐
│   FARGATE TASK                 │
│   (entrypoint or ingestion_   │
│    entrypoint based on type)  │
└────────────────┬─────────────────┘
                 │
                 │ Progress callbacks (HTTP POST)
                 ▼
┌──────────────────────────────────┐
│   ACTIVITY MANAGEMENT         │
│   (receives progress/state)   │
└──────────────────────────────────┘
```

### 5.1 Two Separate State Machines (or Activity Types)

```json
{
  "StartAt": "DetermineMode",
  "States": {
    "DetermineMode": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.activity_type",
          "StringEquals": "extraction",
          "Next": "RunExtractionTask"
        },
        {
          "Variable": "$.activity_type",
          "StringEquals": "ingestion",
          "Next": "RunIngestionTask"
        }
      ],
      "Default": "RunExtractionTask"
    },
    "RunExtractionTask": {
      "Type": "Task",
      "Resource": "arn:aws:states:::ecs:runTask.sync",
      "Parameters": {
        "Cluster": "${EcsCluster}",
        "TaskDefinition": "${TaskDefinition}",
        "Overrides": {
          "ContainerOverrides": [
            {
              "Name": "pipeline",
              "Command": ["python", "-m", "src.fargate.entrypoint"]
            }
          ]
        }
      },
      "End": true
    },
    "RunIngestionTask": {
      "Type": "Task",
      "Resource": "arn:aws:states:::ecs:runTask.sync",
      "Parameters": {
        "Cluster": "${EcsCluster}",
        "TaskDefinition": "${TaskDefinition}",
        "Overrides": {
          "ContainerOverrides": [
            {
              "Name": "pipeline",
              "Command": ["python", "-m", "src.fargate.ingestion_entrypoint"]
            }
          ]
        }
      },
      "End": true
    }
  }
}
```

---

## 9. Cost Analysis (Three-Path Design)

### Per-Run Costs

#### PATH 1: Fargate + Express (≤9GB)

| Component | Extraction | Ingestion |
|-----------|------------|-----------|
| Fargate (4 vCPU, 8GB, 30min) | $0.25 | $0.25 |
| Ephemeral Storage (200GB) | $0.04 | $0.04 |
| S3 Transfer | $0.02-0.10 | $0.02-0.10 |
| SQL Express License | $0 (free) | $0 (free) |
| SPLA Cost | $0 | $0 |
| **Total per run** | **~$0.30-0.40** | **~$0.30-0.40** |

#### PATH 2: Fargate + Standard (9-80GB)

| Component | Extraction | Ingestion |
|-----------|------------|-----------|
| Fargate (8 vCPU, 16GB, 45min) | $0.75 | $0.75 |
| Ephemeral Storage (200GB) | $0.04 | $0.04 |
| S3 Transfer | $0.05-0.30 | $0.05-0.30 |
| SQL Standard License | via SPLA | via SPLA |
| SPLA (4 cores × ~$0.09/hr) | ~$0.36 | ~$0.36 |
| **Total per run** | **~$1.20-1.50** | **~$1.20-1.50** |

> Note: SPLA cost is billed monthly, estimated here as per-run equivalent for 4 physical cores.

#### PATH 3: AWS Batch + Standard EC2 (>80GB)

| Component | Extraction | Ingestion |
|-----------|------------|-----------|
| EC2 (r5.2xlarge, 90min) | $0.50 | $0.50 |
| EBS Storage (500GB gp3) | $0.08 | $0.08 |
| S3 Transfer | $0.10-0.50 | $0.10-0.50 |
| SQL Standard License | via SPLA | via SPLA |
| SPLA (8 cores × ~$0.09/hr) | ~$0.72 | ~$0.72 |
| **Total per run** | **~$1.40-1.80** | **~$1.40-1.80** |

> Note: SPLA cost for EC2 is based on the number of physical vCPU cores of the EC2 instance. r5.2xlarge = 8 vCPU (4 physical cores with HT). Always confirm core count with your SPLA provider.

### Monthly Costs (300 tenants, 1 run/week each)

Assuming 70% use PATH 1 (Express), 20% use PATH 2 (Standard Fargate), 10% use PATH 3 (Batch):

| Component | Cost |
|-----------|------|
| PATH 1 — Express Fargate runs (840/mo) | $250-340 |
| PATH 2 — Standard Fargate runs (240/mo) | $290-360 |
| PATH 3 — Batch EC2 runs (120/mo) | $170-220 |
| S3 Storage | $50-100 |
| SPLA subscription (PATH 2+3 combined cores) | $400-800/mo |
| **Total monthly** | **~$1,160-1,820** |

Cost per tenant: **~$3.90-6.10/month**

### Cost Savings vs Always-Standard

| Approach | Monthly Cost |
|----------|-------------|
| **Three-Path Design** | ~$1,160-1,820 |
| SQL Standard on all runs (no Express path) | ~$2,000-2,800 |
| **Savings** | **~35-45%** |

---

## 10. SQL Server Standard Licensing — SPLA

### Why SPLA is Required

**PATH 2** (Fargate + Standard) and **PATH 3** (Batch EC2 + Standard) run SQL Server Standard edition inside Docker containers. Microsoft requires a **SPLA (Services Provider License Agreement)** when SQL Server is used to deliver hosted or cloud services — which covers this architecture.

- **PATH 1 (Express)** — No license required. SQL Server Express is free and its 10 GB database size limit is acceptable for small payloads.
- **PATH 2 & PATH 3 (Standard)** — Commercial use in containers **requires SPLA**. Using `MSSQL_PID=Standard` without a valid SPLA subscription is a license violation.

---

### What is SPLA?

| Attribute | Details |
|-----------|---------|
| **Full name** | Services Provider License Agreement |
| **Publisher** | Microsoft |
| **Model** | Monthly subscription — you report usage each month and pay accordingly |
| **Who needs it** | Any company hosting/providing software services that include SQL Server |
| **Unit of measure** | **Per processor core** — you pay per physical core on which SQL Server runs |
| **Scope** | Covers containerized SQL Server including Docker on ECS Fargate and AWS Batch EC2 |
| **Minimum term** | Monthly (no upfront commitment) |

> SPLA is the correct Microsoft licensing path for **SaaS/PaaS providers** running SQL Server on behalf of customers. It is **not** a one-time perpetual license.

---

### How to Procure SPLA

#### Step 1 — Identify a Microsoft Licensing Solution Provider (LSP)

You cannot sign SPLA directly with Microsoft in most regions. You need to go through an authorized **Microsoft SPLA Licensing Solution Provider (LSP)**, also called a SPLA Reseller or LAR (Large Account Reseller).

Examples of Microsoft LSPs:
- **Crayon** (https://www.crayon.com)
- **SHI International** (https://www.shi.com)
- **CDW** (https://www.cdw.com)
- **Insight Direct** (https://www.insight.com)
- **SoftwareONE** (https://www.softwareone.com)

> Contact Microsoft directly at https://www.microsoft.com/en-us/licensing/licensing-programs/spla-program to find an LSP in your region.

#### Step 2 — Sign the SPLA Agreement

The LSP will guide you through signing the SPLA enrollment:
1. Provide company details, intended use (hosted services), and estimated core count.
2. Sign the Microsoft SPLA agreement (typically a 3-year enrollment with monthly reporting).
3. You receive a SPLA agreement number — no product key is required; the activation is compliance-based.

> The `MSSQL_PID=Standard` Docker environment variable activates Standard edition inside the container. Microsoft's compliance audit will verify you hold a valid SPLA for the cores used.

#### Step 3 — Monthly Usage Reporting

Each month, report the maximum number of **physical processor cores** on which SQL Server Standard was running:

| Path | Container Resource | Core Count to Report |
|------|--------------------|---------------------|
| PATH 2 (Fargate) | 8 vCPU | **4 physical cores** (8 vCPU ÷ 2 for HyperThreading) |
| PATH 3 (Batch EC2) | Varies by instance | **EC2 physical cores** (check instance spec) |

> Example: `r5.2xlarge` has 8 vCPU = 4 physical cores. `r5.4xlarge` = 8 physical cores.

You report the **peak core usage per month** — not hours billed. If you run 5 concurrent PATH 2 tasks, you report 5 × 4 = 20 cores.

#### Step 4 — Pay Monthly Invoice

Your LSP issues a monthly invoice based on your reported usage:

| SQL Edition | Approximate SPLA Rate |
|-------------|----------------------|
| SQL Server Standard | **~$50–$80 per core per month** |
| SQL Server Enterprise | ~$350–$500 per core per month |

> Rates vary by region and LSP. SQL Server Standard is the correct edition for this architecture — Enterprise is not needed.

---

### Core Counting per Path

```
PATH 2 — ECS Fargate (8 vCPU task):
  Fargate 8 vCPU → reports as 4 physical cores under SPLA
  Each concurrent PATH 2 task = 4 cores
  10 concurrent PATH 2 tasks = 40 cores × $65/core = $650/month

PATH 3 — AWS Batch EC2 (r5.2xlarge = 8 vCPU):
  r5.2xlarge → 4 physical cores
  Each concurrent PATH 3 task = 4 cores (or more for larger instances)
  5 concurrent PATH 3 tasks = 20 cores × $65/core = $1,300/month
```

---

### Alternative Licensing Options

| Option | Applies To | Notes |
|--------|------------|-------|
| **SPLA** | Containers, hosted services | ✅ Correct for this architecture |
| **License Included EC2 AMI** | EC2 bare-metal/VM | Not applicable to containers |
| **BYOL (Bring Your Own License)** | If you have perpetual licenses | Requires Software Assurance (SA) for container use |
| **SQL Server Developer Edition** | `MSSQL_PID=Developer` | **FREE but not for production** — use only in dev/test |
| **SQL Server Express** | PATH 1 only | Free, 10 GB database limit |

> **Never use Developer Edition in production.** Developer Edition is licensed for development and testing only. The `MSSQL_PID` environment variable must match the edition for which you are licensed.

---

### Summary: License by Path

| Path | Size | SQL Edition | `MSSQL_PID` | License | Action Required |
|------|------|-------------|-------------|---------|----------------|
| PATH 1 | ≤9 GB | Express | `Express` | None — FREE | No action |
| PATH 2 | 9-80 GB | Standard | `Standard` | SPLA required | Sign SPLA, report monthly |
| PATH 3 | >80 GB | Standard | `Standard` | SPLA required | Sign SPLA, report monthly |

---

## 10.1 SPLA Usage Tracking & Monitoring  Implemented

> **This section describes the production implementation deployed as of 2026-03-10.**
> All Lambda code lives in `cdk/src/EES.AP.SDE.CDK/lambda/`.
> CDK infrastructure is in `SDEMonitoringStack.cs`.

---

### Actual Resource Allocation (from `appsettings.json`)

| Path | Config Key | vCPU | Memory | SPLA Core Count |
|------|-----------|------|--------|-----------------|
| PATH 1 (Fargate Express) | `ExpressCpu: 8192` | 8 vCPU | 60 GB | **0**  Express is free, `spla_liable=False` |
| PATH 2 (Fargate Standard) | `StandardCpu: 16384` | 16 vCPU | 120 GB | **16**  vCPU = licensable core in containers, `spla_liable=True` |
| PATH 3 (Batch EC2) | `JobVcpus: 16` | 16 vCPU | 64 GB | **16**  vCPU on shared EC2 instances, `spla_liable=True` |

> **Container licensing rule:** Microsoft counts each vCPU assigned to a container as one licensable core when running on shared infrastructure (Fargate, shared EC2). The physical core  2 HyperThreading discount only applies on **dedicated** bare-metal hosts. Minimum billed unit is 4 cores regardless.

---

### SQL Edition per Environment

The SQL edition  and therefore whether a run is SPLA-liable  is controlled at CDK synth time via a single environment variable:

| Variable | Set By | Dev/Staging | Production |
|----------|--------|-------------|------------|
| `SQL_EDITION` | GitHub Actions env var | `Developer` | `Standard` |
| CDK setting | `SDE_CDK_Settings__Fargate__SqlEdition` | `Developer` | `Standard` |
| Container env var | `MSSQL_PID` | `Developer` (free) | `Standard` (SPLA) |
| Local deploy | `deploy-local.ps1` sets `$env:SQL_EDITION = "Developer"` | `Developer` |  |

Both the Fargate Stack (`SDEFargateStack.cs`) and Batch Stack (`SDEBatchStack.cs`) read `fargateSettings.SqlEdition` / `batchSettings.SqlEdition` and inject it as `MSSQL_PID` into every container task definition. PATH 1 (Express) is **always** hardcoded to `MSSQL_PID=Express` regardless of this setting.

This applies identically to **both extraction and ingestion**  there is no separate ingestion task definition. Both SFMs share the same `ss-cdk{stage}-sde-express`, `ss-cdk{stage}-sde-standard`, and Batch job definition.

---

### Per-Run Cost Breakdown (Actual Config)

#### PATH 2  Fargate Standard (30 min run)

| Component | Calculation | Cost |
|-----------|-------------|------|
| 16 vCPU | 16  $0.04048/vCPU-hr  0.5 hr | $0.32 |
| 120 GB memory | 120  $0.004445/GB-hr  0.5 hr | $0.27 |
| Ephemeral 180 GiB (above free 20 GiB) | 180  $0.000111/GB-hr  0.5 hr | $0.01 |
| Misc (S3, SFN state transitions, CW Logs) |  | $0.05 |
| **AWS per run** | | **$0.65** |
| SPLA amortised (16 cores  $0.09/hr  0.5 hr) | Internal allocation only | $0.72 |
| **Total per run (inc. SPLA)** | | **~$1.37** |

#### PATH 3  Batch EC2 `r5.4xlarge` (30 min run)

| Component | Calculation | Cost |
|-----------|-------------|------|
| `r5.4xlarge` On-Demand | $1.008/hr  0.5 hr | $0.50 |
| EBS gp3 scratch 200 GiB | 200  $0.08/730 hr  0.5 hr | $0.01 |
| Misc (S3, SFN, CW Logs) |  | $0.05 |
| **AWS per run** | | **$0.56** |
| SPLA amortised (16 cores  $0.09/hr  0.5 hr) | Internal allocation only | $0.72 |
| **Total per run (inc. SPLA)** | | **~$1.28** |

---

### Monthly Scenario: 100 Runs  30 Min Average

#### AWS Compute Costs (fixed regardless of concurrency)

| | PATH 2 | PATH 3 |
|-|--------|--------|
| AWS compute  100 runs | 100  $0.65 = **$65** | 100  $0.56 = **$56** |
| Misc (S3, SFN, logs) | $5 | $5 |
| **AWS monthly total** | **$70** | **$61** |

#### SPLA Costs (driven entirely by **peak concurrent cores**)

SPLA is billed monthly on peak  not per-run hours:

| Concurrency Pattern | Peak Cores | SPLA monthly* | PATH 2 Total | PATH 3 Total |
|--------------------|------------|--------------|--------------|--------------|
| Sequential  1 at a time | 16 cores | $480 | **$550** | **$541** |
| 2 concurrent jobs peak | 32 cores | $960 | **$1,030** | **$1,021** |
| 5 concurrent jobs peak | 80 cores | $2,400 | **$2,470** | **$2,461** |
| 10 concurrent jobs peak | 160 cores | $4,800 | **$4,870** | **$4,861** |

> *SPLA rate estimated at **$30/core/month** for SQL Server Standard. Confirm exact rate with your LSP (typical range $25$80/core/month by region and volume).

**Key insight:** AWS compute for 100 runs costs ~$6070. SPLA is $480$4,800+ depending purely on how many jobs overlap. **Controlling concurrency is the primary cost lever.**

---

### Implemented: SPLA Monitoring Stack (`SDEMonitoringStack`)

The monitoring stack is a fifth CDK stack (`ss-cdk{stage}-sde-monitoring`) deployed alongside the four core stacks. It contains two Lambda functions and one DynamoDB table.

#### CDK Stack

File: `cdk/src/EES.AP.SDE.CDK/Stacks/SDEMonitoringStack.cs`

- **EventBridge rule**  matches `aws.states` source + `Step Functions Execution Status Change` for both `extractionSmArn` **and** `ingestionSmArn`
- **Tracker Lambda** (`ss-cdk{stage}-sde-spla-tracker`)  triggered on every execution SUCCEEDED/FAILED/TIMED_OUT/ABORTED for either state machine
- **Reporter Lambda** (`ss-cdk{stage}-sde-spla-monthly-reporter`)  triggered by EventBridge cron on the 1st of each month at 06:00 UTC
- **DynamoDB table** (`ss-cdk{stage}-sde-spla-usage`)  TTL-enabled, GSI on `billing_month`
- **CloudWatch alarm**  `ConcurrentSPLACores > 80`  SNS alert

#### DynamoDB Table Schema (Actual)

```
Table: ss-cdk{stage}-sde-spla-usage
PK: tenant_id   (STRING)
SK: run_id      (STRING)   execution_arn last segment

GSI: billing_month-index
  PK: billing_month  (STRING)   "2026-03"

All attributes written per execution:
  activity_id         STRING   from execution input JSON
  tenant_resource_id  STRING   from size-estimator step output in execution history
  direction           STRING   extraction | ingestion
  path                STRING   PATH1 | PATH2 | PATH3
  selected_path       STRING   EXPRESS | STANDARD | BATCH
  compute             STRING   fargate | batch
  spla_liable         BOOL     True for STANDARD/BATCH, False for EXPRESS
  spla_cores          NUMBER   16 for PATH2/PATH3, 0 for PATH1
  started_at          STRING   ISO8601
  stopped_at          STRING   ISO8601
  duration_seconds    NUMBER
  billing_month       STRING   "YYYY-MM"
  execution_status    STRING   SUCCEEDED | FAILED | TIMED_OUT | ABORTED
  execution_arn       STRING
  env                 STRING   dev | staging | prod
  ttl_epoch           NUMBER   90-day TTL for automatic cleanup
```

> Every execution is recorded  PATH 1 (Express) included  with `spla_liable=False, spla_cores=0`. This gives full audit visibility while the monthly reporter filters on `spla_liable=True` for the Microsoft declaration.

---

### Implemented: Tracker Lambda (`spla_tracker.py`)

File: `cdk/src/EES.AP.SDE.CDK/lambda/spla_tracker.py`

#### Path Detection from Execution History

Because neither SFM passes the selected path through to the final execution output reliably (the ingestion SFM's `ReadIngestionSummary` step uses `OutputPath: "$.summary"` which overwrites the entire execution output, discarding `sde_context`), the tracker reads the **execution history** directly via `sfn:GetExecutionHistory`.

It scans `TaskStateEntered` events for known task names to detect path:

```python
STANDARD_TASK_NAMES = {"RunStandardExtractionTask", "RunStandardIngestionTask"}   # PATH 2
BATCH_TASK_NAMES    = {"RunBatchExtractionJob",     "RunBatchIngestionJob"}         # PATH 3
# No task name detected  PATH 1 (Express)
```

The helper `_get_selected_path_from_history(execution_arn) -> tuple[str, str]` returns `(selected_path, tenant_resource_id)`.

#### `tenant_resource_id` from Execution History

The `tenant_resource_id` (e.g. `ruby000001`) is populated by the **size-estimator step** in both pipelines:

| Pipeline | Size Estimator Step | Mechanism |
|----------|---------------------|-----------|
| Extraction | `EstimateExtractionSize` | Lambda reads Iceberg metadata, writes `sde_context.resource_id` |
| Ingestion | `GetIngestionFileSize` | Lambda calls `s3:HeadObject`, writes `sde_context.resource_id` |

Both steps emit their output (including `sde_context.resource_id`) as `TaskStateExited` events in the SFN execution history. The tracker scans for these events:

```python
SIZE_ESTIMATOR_STATES = {"EstimateExtractionSize", "GetIngestionFileSize"}

# In _get_selected_path_from_history():
for event in history["events"]:
    if event["type"] == "TaskStateExited":
        state_name = event.get("stateExitedEventDetails", {}).get("name", "")
        if state_name in SIZE_ESTIMATOR_STATES:
            output = json.loads(event["stateExitedEventDetails"].get("output", "{}"))
            resource_id = output.get("sde_context", {}).get("resource_id", "")
```

This approach is robust because execution history is never overwritten regardless of `OutputPath` settings on later states.

#### Direction Detection

The tracker identifies which pipeline fired via the execution's state machine ARN name (last `:` segment):

```python
EXTRACTION_SM_NAME = EXTRACTION_SM_ARN.split(":")[-1]   # ss-cdkdev-sde-extraction
INGESTION_SM_NAME  = INGESTION_SM_ARN.split(":")[-1]    # ss-cdkdev-sde-ingestion

direction = "extraction" if EXTRACTION_SM_NAME in execution_arn else "ingestion"
```

> The ARN match is done on the **execution ARN** using the **state machine name** (not the full state machine ARN), because execution ARNs use `execution:` prefix while the environment variable holds `stateMachine:`  a substring match on the full ARN would always fail.

#### CloudWatch Concurrent Cores Metric

On every execution status change, the tracker recalculates live concurrent cores and publishes to CloudWatch:

```python
# _publish_concurrent_cores_metric() in spla_tracker.py
path_str, _ = _get_selected_path_from_history(exec_arn)
if path_str in SPLA_PATH_CORES:                          # STANDARD or BATCH only
    active = _get_active_executions(sfn_arn)             # sfn:ListExecutions RUNNING
    concurrent_cores = sum(
        SPLA_PATH_CORES[_get_path_for_execution(arn)]
        for arn in active
    )
    cloudwatch.put_metric_data(
        Namespace="sde/SPLA",
        MetricData=[{
            "MetricName": "ConcurrentSPLACores",
            "Value": concurrent_cores,
            "Unit": "Count",
            "Dimensions": [{"Name": "Environment", "Value": env}]
        }]
    )
```

CloudWatch alarm fires when `ConcurrentSPLACores > 80` (5+ concurrent STANDARD/BATCH runs).

---

### Implemented: Monthly Reporter Lambda (`spla_monthly_reporter.py`)

File: `cdk/src/EES.AP.SDE.CDK/lambda/spla_monthly_reporter.py`

Triggered automatically on the 1st of each month (EventBridge cron). Can be manually triggered for testing with `{"_override_billing_month": "2026-03"}` in the payload.

#### Report Flow

1. Query DynamoDB GSI `billing_month-index` for `billing_month = YYYY-MM`
2. Split records: `spla_liable=True` (PATH2/PATH3) vs `spla_liable=False` (EXPRESS)
3. Run sweep-line algorithm over `started_at`/`stopped_at` intervals on the SPLA-liable subset to calculate **peak concurrent cores**
4. Build `microsoft_spla_declaration` block
5. Calculate per-tenant `core_hours`, SPLA cost share, and list associated `tenant_resource_id` values
6. Write JSON report to S3: `s3://{metrics_bucket}/{stage}/usagebyservice/SQL_SPLA/{YYYY-MM}.json`

#### Peak Concurrent Cores Algorithm (Sweep-Line)

```python
events = []
for record in spla_runs:                             # only spla_liable=True records
    events.append((record["started_at"], +record["spla_cores"]))
    events.append((record["stopped_at"], -record["spla_cores"]))

events.sort(key=lambda x: x[0])                      # chronological order
current_cores = peak_cores = 0
for _, delta in events:
    current_cores += delta
    peak_cores = max(peak_cores, current_cores)

declared_cores = max(4, peak_cores)                  # Microsoft minimum = 4
```

> PATH 1 (Express) runs are **excluded from this calculation** because `spla_liable=False`. Their `spla_cores=0` would otherwise contribute zero to the peak  filtering them keeps the intent explicit.

#### Report JSON Schema (`schema_version: "1.1"`)

```json
{
  "schema_version": "1.1",
  "generated_at": "2026-03-01T06:00:00Z",
  "billing_month": "2026-02",
  "env": "prod",

  "microsoft_spla_declaration": {
    "product": "SQL Server Standard",
    "quantity_cores": 16,
    "due_date": "2026-03-05",
    "instructions": "Report 16 cores of SQL Server Standard to your LSP by 2026-03-05"
  },

  "spla_liable_summary": {
    "total_runs": 42,
    "peak_concurrent_spla_cores": 16,
    "declared_cores": 16,
    "total_core_hours": 5.6,
    "per_tenant": [
      {
        "tenant_id": "ruby",
        "tenant_resource_id": "ruby000001",
        "runs": 12,
        "path1_runs": 0,
        "core_hours": 1.6,
        "share_pct": 28.6,
        "estimated_cost_usd": 4.57
      }
    ]
  },

  "express_summary": {
    "total_runs": 88,
    "note": "PATH1 Express runs  no SPLA cost, included for visibility only"
  },

  "all_runs": [
    {
      "tenant_id": "ruby",
      "run_id": "...",
      "activity_id": "...",
      "tenant_resource_id": "ruby000001",
      "direction": "extraction",
      "selected_path": "STANDARD",
      "spla_liable": true,
      "spla_cores": 16,
      "duration_seconds": 1842,
      "started_at": "2026-02-14T10:22:00Z",
      "stopped_at": "2026-02-14T10:52:42Z"
    }
  ]
}
```

#### S3 Output Location

```
s3://{ss-cdk{stage}-metrics-shared}/{stage}/usagebyservice/SQL_SPLA/{YYYY-MM}.json
```

Example (dev): `s3://ss-cdkdev-metrics-shared/dev/usagebyservice/SQL_SPLA/2026-02.json`

---

### How SPLA Cores Are Counted and Reported

The following summarises the end-to-end flow from a run completing to the monthly Microsoft declaration:

```
 Step Functions execution completes (SUCCEEDED / FAILED / TIMED_OUT)
        
         EventBridge rule (both extraction + ingestion state machines)
 
          spla_tracker Lambda              
                                           
   1. Detect direction (extraction vs      
      ingestion) from execution ARN name   
                                           
   2. Scan execution history for:          
       Task name  path (EXPRESS /        
        STANDARD / BATCH)                  
       EstimateExtractionSize or          
        GetIngestionFileSize output       
        tenant_resource_id                 
                                           
   3. Write DynamoDB record (ALL paths)    
      spla_liable = True/False             
      spla_cores  = 16 / 0                 
                                           
   4. Publish ConcurrentSPLACores metric   
      (STANDARD/BATCH executions only)     
 
        
         1st of month at 06:00 UTC
 
      spla_monthly_reporter Lambda         
                                           
   1. Query DynamoDB by billing_month GSI  
   2. Filter: spla_liable=True for peak    
   3. Sweep-line: peak concurrent cores    
   4. declared_cores = max(4, peak)        
   5. Per-tenant core_hours + cost share   
   6. Write JSON  S3 metrics bucket       
 
        
         Manually by finance team
 Report declared_cores to LSP by 5th of month
```

---

### Monthly SPLA Reporting Process

1. **1st of each month**  scheduled Lambda (`EventBridge cron 0 6 1 * ? *`) auto-triggers
2. Declares **peak concurrent SPLA cores** (minimum 4) based on actual overlap in the previous month
3. Generates full JSON report in S3 with Microsoft SPLA declaration block
4. Finance team reads `microsoft_spla_declaration.quantity_cores` from the report and submits to LSP by the 5th
5. Per-tenant breakdown enables internal chargeback proportional to core-hours consumed

### Key Rules Summary

| Rule | Detail |
|------|--------|
| PATH 1 always `spla_liable=False` | `MSSQL_PID=Express`  written to DynamoDB but excluded from peak calculation |
| Extraction + Ingestion both tracked | Single EventBridge rule covers both SFMs via two ARN conditions |
| `tenant_resource_id` from history | Both pipelines: extracted from size-estimator step `TaskStateExited` event output |
| Shared task definitions | Extraction and ingestion share the same Fargate/Batch task definitions  SQL edition applies to both |
| Dev/staging use Developer edition | `SQL_EDITION=Developer` at deploy time  `MSSQL_PID=Developer`  free, not SPLA-liable |
| No overlapping runs = minimum invoice | Sequential jobs  16 cores declared  lowest possible SPLA cost |
| Every additional concurrent SPLA job = +16 cores | 5 concurrent PATH 2+3 jobs = 80 cores declared = 5 the SPLA invoice |
| Minimum declaration is 4 cores | Microsoft rule  even a single qualifying job must declare at least 4 cores |
| No PATH 2/3 in a month = zero SPLA | If only Express ran, `declared_cores=0` and declaration is skipped |
## 11. Implementation Timeline

**Team:** 3 Engineers + Architect/Manager (oversight, code reviews, design decisions)  
**Scope:** Shared CDK constructs, Okta enterprise integration, Multi-tenancy infrastructure, Shared Angular library, GitHub Actions reusable workflows, Monitoring & Observability, Cross-pipeline orchestration

---

### Week 1: Shared Infrastructure CDK + Okta + Angular Shell

| Day | Engineer 1 (Backend / API Shared) | Engineer 2 (Infra / CDK) | Engineer 3 (Shared Angular / Pipeline) | Architect/Manager |
|-----|-----------------------------------|--------------------------|----------------------------------------|-------------------|
| Mon | Design shared OpenAPI spec (covers both extraction and ingestion API surfaces), shared API error catalog (`EXT-*` and `ING-*` codes), health check endpoint `/health` + `/ready` | VPC CDK construct — private subnets, NAT Gateway, VPC endpoints for SFN, S3, Secrets Manager, ECR, Glue, LakeFormation | Bootstrap nx monorepo workspace — `libs/shared` (auth, tenant, layout), `apps/extraction-ui`, `apps/ingestion-ui`; configure `@nx/angular`, Okta Angular SDK | Kick-off, architecture ADR for shared-vs-separate infrastructure, API versioning strategy (`/v1/`), multi-tenant data model design |
| Tue | JWT authorizer Lambda — shared Okta RS256 token validator, extracts `tenantId` from `groups` claim, propagates via `x-tenant-id` header to downstream services | Okta CDK integration — API Gateway Lambda authorizer attachment, Okta app registrations (extraction client, ingestion client), well-known OIDC config documented in SSM Parameter Store | `libs/shared/auth` Angular library — `OktaAuthModule` wrapper, `AuthGuard`, `TenantGuard`, `authInterceptor` (attaches Bearer token to all API calls) | Review Okta OIDC flow, confirm token scopes (`extraction:read`, `extraction:write`, `ingestion:read`, `ingestion:write`), sign-off tenant claim naming convention |
| Wed | Tenant registry service — DynamoDB table (`tenants`) storing `tenantId`, allowed S3 prefixes, Iceberg namespaces, SFN ARNs; CRUD API `/tenants` (admin-only scope) | ECR CDK — shared repositories (`data-pipeline-express`, `data-pipeline-standard`, `data-pipeline-batch`), lifecycle policies (keep last 20 tagged, expire untagged after 7 days), cross-account pull permissions for staging/prod | `libs/shared/tenant` Angular library — `TenantService` (reads tenant list from `/tenants`), `TenantSelectorComponent` (header dropdown), route resolver that sets active tenant | Review tenant registry design, confirm it covers both extraction and ingestion isolation requirements, RBAC roles per tenant |
| Thu | Shared execution status API — `GET /executions/{executionArn}` wraps `DescribeExecution` for both extraction and ingestion SFN ARNs, normalises status to common schema | IAM CDK — shared execution roles: ECS task role template (parameterised by pipeline direction + tenant), cross-account trust policies, SCP guardrails for prod account | `libs/shared/layout` Angular library — `AppShellComponent` (header + sidebar + breadcrumb), `PipelineStatusBadge`, `ExecutionHistoryTable` (reusable for both pipelines) | Review IAM least-privilege matrix, cross-account trust policy security sign-off |
| Fri | Unit and integration test scaffolding — shared `pytest` fixtures (`mock_tenant_context`, `mock_sfn_client`, `mock_okta_token`), added to both extraction and ingestion test suites | CDK unit tests (`aws-cdk-lib/assertions`) for shared constructs, CDK synth smoke test in CI on every PR | Storybook setup for Angular shared lib — document `TenantSelectorComponent`, `PipelineStatusBadge`, `ExecutionHistoryTable` with mock data | Week 1 retrospective, review shared construct coverage, adjust Week 2 scope |

---

### Week 2: GitHub Actions CI/CD Pipelines + Multi-Tenancy Wiring

| Day | Engineer 1 (Backend / API Shared) | Engineer 2 (Infra / CDK) | Engineer 3 (Shared Angular / Pipeline) | Architect/Manager |
|-----|-----------------------------------|--------------------------|----------------------------------------|-------------------|
| Mon | Tenant onboarding service — `POST /admin/tenants` creates DynamoDB record, provisions tenant S3 prefix, registers tenant Iceberg namespace in Glue, creates tenant-scoped IAM session policy | GitHub Actions reusable workflow `_build-and-push.yml` — inputs: `service-name`, `ecr-repo`; steps: Docker build, ECR login, image tag (`${SHA}-${env}`), ECR push, output image URI | GitHub Actions reusable workflow `_deploy-cdk.yml` — inputs: `stack-name`, `env`, `require-approval`; steps: CDK diff, post diff as PR comment, manual approval gate (staging/prod), CDK deploy | Review CI/CD workflow reuse patterns, confirm deployment approval owners list, environment promotion sequence (dev → staging → prod) |
| Tue | Cross-account S3 access service — shared utility to generate STS-based pre-signed URLs scoped to `tenants/{tenantId}/{direction}/*`, 15-minute expiry, audit log to CloudTrail | GitHub Actions `extraction-api-ci.yml` — uses `_build-and-push.yml`, runs on PR to `main`; `extraction-infra-cd.yml` — uses `_deploy-cdk.yml` for extraction CDK stacks | GitHub Actions `angular-ci.yml` — nx affected lint → test → build on PR; `angular-cd.yml` — nx affected build (`--prod`) → S3 sync → CloudFront invalidation | Review GitHub Actions secrets management (OIDC roles for AWS, Okta client secret in GitHub Secrets), approve OIDC trust policy |
| Wed | Rate limiting + throttling — API Gateway usage plans per tenant (configurable in tenant registry), Lambda concurrency reserved for extraction vs ingestion SFN launchers | GitHub Actions `ingestion-api-ci.yml` + `ingestion-infra-cd.yml` — mirror of extraction workflows for ingestion service and CDK stacks | Integration tests in CI — `nx run-many --target=test --all` including E2E (Playwright headless) against localstack + SQL Server container (docker-compose in GitHub Actions) | Review throttling configuration, confirm per-tenant rate limits don't impact other tenants, approve test container strategy in CI |
| Thu | Audit logging service — all API calls (`tenantId`, `userId`, `action`, `resourceArn`, `timestamp`) → CloudWatch Logs `/audit/{env}` + S3 archive; queryable via `/admin/audit?tenantId=&from=&to=` | CDK pipeline stack (`infra/pipeline.py`) — CodePipeline or GitHub Actions self-hosted runner; separate pipeline per environment; drift detection via scheduled `CDK diff` CloudWatch event | Multi-tenant Angular routing — `TenantGuard` blocks cross-tenant navigation, `authInterceptor` injects `x-tenant-id`, integration with `apps/extraction-ui` and `apps/ingestion-ui` | Review audit log retention policy (7 years for financial data), confirm CloudTrail covers S3 data events per tenant prefix |
| Fri | Deploy to dev environment — both extraction and ingestion APIs, Angular apps, shared infrastructure | Validate dev deployment — Okta login works, extraction and ingestion SFN triggers succeed, CloudFront serves Angular apps with HTTPS | dev environment smoke test — login as test tenant, trigger extraction, trigger ingestion, verify audit log entries created | Week 2 retrospective, dev deployment review, identify hardening tasks for Week 3 |

---

### Week 3: Monitoring + Observability + Production Readiness

| Day | Engineer 1 (Backend / API Shared) | Engineer 2 (Infra / CDK) | Engineer 3 (Shared Angular / Pipeline) | Architect/Manager |
|-----|-----------------------------------|--------------------------|----------------------------------------|-------------------|
| Mon | Load test setup (Locust) — mixed extraction + ingestion load across 5 tenants simultaneously; validate tenant isolation, no cross-tenant data leakage, rate limit enforcement | Deploy to staging — full shared infra, both pipeline APIs, Angular apps; validate Okta staging IdP config, CloudFront custom domains, ACM certificates | Angular staging smoke tests (Playwright) — login, tenant switch, trigger extraction, trigger ingestion, verify both execution history dashboards, logout | Review staging checklist, confirm Okta staging app config, validate all GitHub Actions CD gates fired correctly |
| Tue | Load test execution — 50 concurrent users across 5 tenants; API p99 < 200ms; SFN launch < 1 second; cross-tenant data leak probe (attempt to access another tenant's presigned URL) | Monitoring CDK — unified CloudWatch dashboard: extraction SFN executions (per path per tenant), ingestion SFN executions (per path per tenant), API 4xx/5xx rates, Fargate + Batch resource utilisation | Shared pipeline execution monitor UI — real-time view of all active extractions + ingestions for the logged-in tenant, auto-refresh every 30s, cancel execution button | Validate load test results: no cross-tenant leakage confirmed; review CloudWatch dashboard coverage with operations team |
| Wed | Fix issues from load test — primarily around DynamoDB tenant registry hot partitions (add caching layer), STS session token caching in tenant context | X-Ray tracing CDK — `TracingConfig: Active` on all Lambdas, ECS task Fargate X-Ray sidecar, service map showing extraction and ingestion full flow end-to-end | Angular Datadog / Dynatrace RUM injection, session replay config (PII masking for tenant data), error boundary components with correlation ID display | Security review: OWASP Top 10 against both APIs, pen test findings review, WAF rule tuning (SQL injection, XSS), CSP header audit |
| Thu | Final integration test on staging — extraction + ingestion round-trip (Iceberg → MDF → Iceberg), row count match, SID integrity, audit log completeness | Production CDK deploy — `CDK deploy --context env=prod`; WAF enabled with IP allowlist for known tenant CIDRs; Shield Standard; CloudTrail data events | End-to-end production smoke test — real tenant, full extraction and ingestion cycle, all monitoring dashboards showing green | Go/no-go decision: all SLAs met, security sign-off obtained, monitoring coverage confirmed; stakeholder demo |
| Fri | Buffer — post-go-live triage, hot-fix pipeline ready (`shared-infra-hotfix.yml`) | Production monitoring fine-tuning — alarm threshold calibration based on real traffic, on-call runbook published | Knowledge transfer — shared library usage guide, CDK construct catalogue, GitHub Actions workflow guide, local dev setup | Final sign-off, close sprint, full retrospective across all 3 workstreams |

---

### Week 4 (Buffer — Only If Required)

| Area | Tasks |
|------|-------|
| Shared Infra | Multi-region active-passive DR setup; Route 53 health checks + failover; cross-region CDK pipeline |
| Okta / Auth | SAML 2.0 federation for enterprise tenants who cannot use OIDC; MFA policy enforcement per tenant group |
| Angular Shared Lib | Dark mode support; internationalisation scaffolding (i18n); configurable notification preferences UI |
| Observability | Dynatrace SLO definitions for extraction and ingestion API; PagerDuty on-call schedule integration |
| Documentation | Architecture Decision Records (ADRs) 1-10 written and peer-reviewed; Confluence space structure for Synergy Data Exchange |

**Total: 3 weeks core delivery + 1 week buffer**

---

## 12. Testing Strategy

### Unit Tests
- `test_sql_reader.py` — Mock SQL Server, verify Arrow output
- `test_iceberg_writer.py` — Mock catalog, verify write calls
- `test_artifact_restorer.py` — Verify RESTORE SQL generation

### Integration Tests
- `test_ingestion_flow.py` — Full ingestion with real SQL Server container
- `test_round_trip.py` — Extract → BAK → Ingest → Verify data integrity

### Performance Tests
- 1M rows: < 2 minutes
- 10M rows: < 10 minutes
- 40M rows: < 30 minutes

### Local Testing Commands

These commands test both **Extraction** and **Ingestion** flows locally, simulating production behavior.

#### Prerequisites

```powershell
# Activate virtual environment
cd c:\Github\Rnd\DataIngestion
.\.venv\Scripts\Activate.ps1

# Ensure .env is configured with:
# - ICEBERG_NAMESPACE (source namespace)
# - SQLSERVER_HOST, PORT, USERNAME, PASSWORD
# - SQLSERVER_DATA_SHARE_PATH (format: local_path=unc_path)
```

#### 1. EXTRACTION: Iceberg → MDF → S3

Extract data from Iceberg to MDF file and upload to S3.

```powershell
# Test extraction (all tables, 100 row limit per table)
python scripts/local_test.py --target-bucket rks-sd-virusscan --target-prefix extraction-test/ --limit 100

# Test extraction (specific tables only)
python scripts/local_test.py --target-bucket rks-sd-virusscan --target-prefix extraction-test/ --tables tcontract tlocation --limit 100

# Test extraction (full data, no limit)
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

#### 2. INGESTION: S3 → MDF → SQL Server → Iceberg

Ingest data from S3 MDF file into Iceberg tables.

```powershell
# Test ingestion from S3 MDF (all tables, 100 row limit)
python scripts/test_ingestion_flow.py \
  --s3-uri s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf \
  --target-namespace rs_cdkdev_dclegend01_exposure_db \
  --limit 100

# Test ingestion (specific tables only)
python scripts/test_ingestion_flow.py \
  --s3-uri s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf \
  --target-namespace rs_cdkdev_dclegend01_exposure_db \
  --tables tcontract tlocation \
  --limit 100

# Test ingestion (full data, no limit)
python scripts/test_ingestion_flow.py \
  --s3-uri s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf \
  --target-namespace rs_cdkdev_dclegend01_exposure_db

# Test ingestion from existing SQL Server database
python scripts/test_ingestion_flow.py \
  --source-db AIRExposure_test \
  --target-namespace rs_cdkdev_dclegend01_exposure_db \
  --limit 100

# Test ingestion with append mode (vs overwrite)
python scripts/test_ingestion_flow.py \
  --s3-uri s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf \
  --target-namespace rs_cdkdev_dclegend01_exposure_db \
  --write-mode append

# Keep database after test (don't drop)
python scripts/test_ingestion_flow.py \
  --s3-uri s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf \
  --target-namespace rs_cdkdev_dclegend01_exposure_db \
  --no-cleanup
```

**Expected Output:**
```
INGESTION TEST (from S3 MDF)
============================
S3 URI: s3://rks-sd-virusscan/extraction-test/AIRExposure_test.mdf
Target Namespace: rs_cdkdev_dclegend01_exposure_db
Database name: ingest_test_1772286164017  (unique timestamp)
---
Downloaded: 8.00 MB
Copying MDF to share: \\10.193.57.161\DATA\ingest_test_1772286164017.mdf
Database attached: ingest_test_1772286164017
---
Processing: tcontract (100 rows)
  Evolving schema: adding 2 columns  (if needed)
  Casting exposuresetsid: int64 → int32
  ✓ Written 100 rows
Processing: tlocation (100 rows)
  ✓ Written 100 rows
...
INGESTION COMPLETE
  Tables: 7 / 12
  Rows: 700
---
Database dropped: ingest_test_1772286164017
```

#### 3. ROUND-TRIP TEST: Extract → Ingest → Verify

Test full bidirectional flow: Extract from source, ingest to different namespace.

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

# Step 3: Verify data (manual query via Athena or pyiceberg)
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

#### Key Test Scenarios

| Scenario | Command |
|----------|---------|
| **Extract small (≤9GB)** | `python scripts/local_test.py --limit 100 ...` |
| **Extract large (>9GB)** | `python scripts/local_test.py ...` (no limit) |
| **Ingest with schema evolution** | `--tables tlocation` (adds missing columns) |
| **Ingest append mode** | `--write-mode append` |
| **Debug mode** | Set `LOG_LEVEL=DEBUG` in .env |
| **Keep temp DB** | `--no-cleanup` |

#### Troubleshooting

| Issue | Solution |
|-------|----------|
| **UNC share access denied** | Run `net use \\host\share /user:username password` |
| **Database not found** | Check `SQLSERVER_DATABASE` in .env |
| **Schema mismatch** | Script auto-evolves schema; check logs for "Evolving schema" |
| **pyiceberg_core missing** | `pip install "pyiceberg[pyiceberg-core]"` |
| **S3 access denied** | Check AWS credentials in `~/.aws/credentials` |

---

## 13. Column Mapping Configuration

### Three-Tier Mapping System

The system provides flexible column mapping at three levels:

| Layer | Location | Purpose | Priority |
|-------|----------|---------|----------|
| **Auto-mapping** | `src/core/column_mapper.py` | 6-strategy automatic matching | Lowest |
| **Generated config** | `mapping_config.json` | Auto-generated, user-reviewable | Medium |
| **Runtime overrides** | S3 or Step Functions input | Per-tenant/per-job overrides | Highest |

### Auto-Mapping Strategies

The `ColumnMapper` uses a 6-strategy cascade (highest to lowest confidence):

```
┌──────────────────────────────────────────────────────────────────┐
│  Strategy 1: EXACT           │ "contractid" → "contractid"      │ 1.0
├──────────────────────────────────────────────────────────────────┤
│  Strategy 2: CASE-INSENSITIVE│ "ContractID" → "contractid"      │ 0.95
├──────────────────────────────────────────────────────────────────┤
│  Strategy 3: NAMING-CONV     │ "contract_id" → "ContractId"     │ 0.9
├──────────────────────────────────────────────────────────────────┤
│  Strategy 4: PREFIX-STRIP    │ "tbl_contract" → "contract"      │ 0.8
├──────────────────────────────────────────────────────────────────┤
│  Strategy 5: ABBREVIATION    │ "cntrt_id" → "contract_id"       │ 0.7
├──────────────────────────────────────────────────────────────────┤
│  Strategy 6: FUZZY           │ "contrat_id" → "contract_id"     │ 0.5-0.7
└──────────────────────────────────────────────────────────────────┘
```

### Generated Mapping Config Schema

Auto-generated `mapping_config.json` structure:

```json
{
  "{namespace}_{table}": {
    "source": {
      "namespace": "rs_cdkdev_tenant_db",
      "table": "tcontract"
    },
    "target": {
      "schema": "dbo",
      "table": "tcontract"
    },
    "generated_at": "2026-02-20T14:57:39.038767+00:00",
    "naming_convention": "as_is",
    "create_table_if_missing": true,
    "load_mode": "full",
    "columns": [
      {
        "source": "contractsid",
        "target": "contractsid",
        "source_type": "int64",
        "target_type": "BIGINT",
        "strategy": "exact",
        "confidence": 1.0,
        "include": true
      },
      {
        "source": "old_column",
        "target": null,
        "source_type": "string",
        "target_type": null,
        "strategy": "unmatched",
        "confidence": 0.0,
        "include": false
      }
    ]
  }
}
```

### User Override Workflow

**Option 1: Upload Per-Tenant Overrides to S3**

```bash
# Create overrides file
cat > my_overrides.json << 'EOF'
{
  "tables": {
    "tcontract": {
      "columns": [
        { "source": "cntrt_id", "target": "contract_id", "include": true },
        { "source": "legacy_field", "include": false }
      ]
    }
  }
}
EOF

# Upload to S3
python scripts/upload_mapping_overrides.py \
  --bucket my-config-bucket \
  --tenant-id tenant-123 \
  --overrides-file my_overrides.json

# Stored at: s3://my-config-bucket/config/tenant-123/mapping_overrides.json
```

**Option 2: Pass Overrides in Step Functions Input**

```json
{
  "tenantId": "tenant-123",
  "s3InputPath": "s3://bucket/input/database.mdf",
  "s3OutputPath": "s3://bucket/output/",
  "databaseName": "MyDatabase",
  "fileSizeBytes": 5368709120,
  "mappingOverrides": {
    "tables": {
      "tcontract": {
        "columns": [
          { "source": "old_name", "target": "new_name", "include": true },
          { "source": "deprecated_col", "include": false }
        ]
      }
    }
  }
}
```

### Override Merge Logic

Overrides are merged with auto-generated mappings (overrides win):

```python
# In entrypoint.py
def merge_mappings(auto_mapping: dict, overrides: dict) -> dict:
    """
    Merge auto-generated mappings with user overrides.
    Overrides take precedence for matching source columns.
    """
    merged = copy.deepcopy(auto_mapping)
    
    for table_key, table_overrides in overrides.get("tables", {}).items():
        if table_key not in merged:
            continue
            
        for col_override in table_overrides.get("columns", []):
            source_name = col_override["source"]
            # Find and update matching column in auto-mapping
            for col in merged[table_key]["columns"]:
                if col["source"] == source_name:
                    col.update({k: v for k, v in col_override.items() if v is not None})
                    break
    
    return merged
```

### CLI Override (Local Testing)

```bash
# Generate mapping config first
python -m src.core.ingest \
  --step generate \
  --mapping mapping_config.json

# Edit mapping_config.json manually, then run with it
python -m src.core.ingest \
  --step run \
  --mapping mapping_config.json
```

---

## 14. Security Considerations & SOC2 Compliance

### Multi-Tenant Architecture (Shared Infrastructure)

The system uses **shared infrastructure** with **runtime tenant isolation** for SOC2 compliance:

```
┌─────────────────────────────────────────────────────────────────┐
│  SHARED INFRASTRUCTURE (Deployed Once)                          │
├─────────────────────────────────────────────────────────────────┤
│  • ECS Cluster (Fargate)                                        │
│  • ECR Repositories (Express, Standard, Batch images)           │
│  • Step Functions (Extraction + Ingestion state machines)       │
│  • AWS Batch Queue + Compute Environment (EC2-backed)           │
│  • Batch Job Definition                                         │
│  • IAM Roles (with session policy support)                      │
│  • CloudWatch Log Groups                                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ Runtime Tenant Scoping
┌─────────────────────────────────────────────────────────────────┐
│  PER-JOB ISOLATION (Runtime)                                    │
├─────────────────────────────────────────────────────────────────┤
│  • IAM Session Policy (restricts S3 to tenant's prefix only)    │
│  • Tenant-specific S3 paths (s3://bucket/tenants/{tenantId}/)   │
│  • Tenant-specific Secrets (Secrets Manager)                    │
│  • CloudWatch logs tagged with tenantId                         │
│  • Ephemeral container (destroyed after job)                    │
└─────────────────────────────────────────────────────────────────┘
```

### SOC2 Control Mapping

| SOC2 Control | Implementation | Evidence |
|--------------|----------------|----------|
| **CC6.1 - Logical Access** | IAM session policies scope each job to tenant's S3 prefix only | Session policy attached per job |
| **CC6.1 - Compute Isolation** | Fargate micro-VM (PATH 1/2) or Docker container isolation (PATH 3) | Firecracker/Docker namespaces |
| **CC6.3 - Least Privilege** | Job role can only access specific tenant resources at runtime | IAM policy documents |
| **CC6.5 - Data Disposal** | Ephemeral containers destroyed after job; no persistent database | Container lifecycle logs |
| **CC6.7 - Encryption at Rest** | S3 SSE-KMS, EBS encryption enabled | KMS key policies |
| **CC6.7 - Encryption in Transit** | TLS everywhere (S3, Secrets Manager, Iceberg) | VPC flow logs |
| **CC7.2 - Audit Trail** | CloudWatch logs + CloudTrail tagged with tenantId | Log group retention policies |

### Compute Isolation by Path

| Path | Compute Type | Isolation Level | Kernel Sharing |
|------|--------------|-----------------|----------------|
| **PATH 1** | Fargate | **Micro-VM (Firecracker)** | NO - dedicated kernel per task |
| **PATH 2** | Fargate | **Micro-VM (Firecracker)** | NO - dedicated kernel per task |
| **PATH 3** | Batch EC2 | **Docker Container** | YES - shared kernel, but namespace isolated |

**Note:** PATH 3 uses Docker container isolation on shared EC2. This is acceptable for SOC2 as:
- Containers have isolated process, network, and filesystem namespaces
- IAM session policies prevent cross-tenant S3 access even if container escape occurred
- SQL Server runs INSIDE the container, not on the EC2 host

### IAM Session Scoping (Runtime Tenant Isolation)

Each job runs with a session policy that restricts S3 access:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
    "Resource": [
      "arn:aws:s3:::data-lake/tenants/${tenantId}/*",
      "arn:aws:s3:::data-lake"
    ],
    "Condition": {
      "StringLike": {
        "s3:prefix": ["tenants/${tenantId}/*"]
      }
    }
  }]
}
```

### Step Functions Input Schema

```json
{
  "tenantId": "tenant-123",
  "operationType": "extraction|ingestion",
  "icebergTablePath": "s3://iceberg-catalog/databases/tenant-123/tables/contracts",
  "s3OutputPath": "s3://tenant-artifacts/tenant-123/extractions/",
  "s3InputPath": "s3://tenant-artifacts/tenant-123/uploads/",
  "estimatedSizeGb": 25,
  "callbackUrl": "https://api.example.com/callback",
  "taskToken": "..."
}
```

### Ingestion-Specific Security
- Validate artifact source (must be from known tenant bucket)
- Verify checksum before restore
- Sanitize database names to prevent SQL injection
- Limit tables that can be ingested (whitelist approach)
- Scan uploaded MDF files for malware before processing

### Secrets Management
- Tenant credentials stored in Secrets Manager with path: `/sde/{tenantId}/credentials`
- Job retrieves only its tenant's secrets via scoped IAM policy
- Secrets rotated automatically via Secrets Manager rotation

### Network Security
- Fargate tasks run in private subnets (no public IP)
- Batch EC2 instances in private subnets
- VPC endpoints for S3, ECR, Secrets Manager, CloudWatch
- Security groups allow only outbound traffic (no inbound)

---

## 15. Final Architecture Summary

### Three-Path Design Decision

Based on scalability, performance, and operational requirements:

| Criteria | Decision |
|----------|----------|
| **Small threshold** | 9 GB (SQL Express 10 GB limit with margin) |
| **Large threshold** | 80 GB (Fargate ephemeral storage limit) |
| **Small data (≤9GB)** | ECS Fargate + SQL Express (FREE) |
| **Medium data (9-80GB)** | ECS Fargate + SQL Standard (SPLA) |
| **Large data (>80GB)** | AWS Batch + SQL Standard (SPLA) |
| **Operating System** | Linux (all paths) |
| **Artifact format** | MDF (detached database files) |

### Why This Architecture

| Decision | Rationale |
|----------|-----------|
| **Same container for SQL + Python** | Local filesystem access to MDF, no network shares |
| **Linux containers** | 50% cheaper, 3x faster boot, 10x smaller images than Windows |
| **Fargate for small** | Fast cold start (~30-60s), serverless, cost-effective |
| **AWS Batch for large** | Unlimited EBS storage, optimal instance sizing, provisioned IOPS |
| **On-Demand EC2 (not provisioned)** | Auto-scaled by Batch, no idle costs |

### Infrastructure Paths

```
Tenant Request → Step Functions
    │
    ├── ≤9GB → ECS Fargate Task
    │              ├── SQL Server Express (Linux, FREE)
    │              ├── Python Extraction/Ingestion
    │              └── Ephemeral Volume (up to 200GB)
    │
    ├── 9-80GB → ECS Fargate Task
    │              ├── SQL Server Standard (Linux, SPLA)
    │              ├── Python Extraction/Ingestion
    │              └── Ephemeral Volume (up to 200GB)
    │
    └── >80GB → AWS Batch Job (On-Demand EC2)
                   ├── SQL Server Standard (Linux, SPLA)
                   ├── Python Extraction/Ingestion  
                   └── EBS Volume (sized per job, up to 16TB)
```

### Code Reuse

The Python extraction/ingestion code is **identical** for both paths:
- `SQLSERVER_HOST=localhost` (always local)
- `artifact_generator.py` reads local MDF files
- `s3_uploader.py` uploads to tenant bucket

Only infrastructure differs — the application is path-agnostic.

### Scalability

| Scenario | Concurrent Tenants | Infrastructure |
|----------|-------------------|----------------|
| **Normal load** | 10-50 | Fargate auto-scales |
| **Peak load** | 100+ | Batch auto-provisions EC2 |
| **Large data (100M rows)** | Per-tenant | Batch with r5.4xlarge+, provisioned IOPS EBS |

### Cost Optimization

| Path | Cost per Run | Best For |
|------|--------------|----------|
| **Fargate (Express)** | ~$0.30-0.40 | Majority of runs (≤9GB) |
| **Fargate (Standard)** | ~$1.20-1.50 | Medium datasets (9-80GB) |
| **Batch (Standard)** | ~$1.40-1.80 | Large datasets only (>80GB) |

SQL Server Standard license (via SPLA) is only incurred on PATH 2 and PATH 3. PATH 1 (Express) is completely free.


---

## Related Documents

| Document | Description |
|----------|-------------|
| [implementation_plan_extraction.md](implementation_plan_extraction.md) | Extraction pipeline: Iceberg → Tenant S3 (modules, routing, testing, timeline) |
| [implementation_plan_ingestion.md](implementation_plan_ingestion.md) | Ingestion pipeline: Tenant S3 → Iceberg (modules, routing, SID transform, SPLA, testing, timeline) |
| [sequence_diagram.md](sequence_diagram.md) | Mermaid sequence diagrams for both flows |
| [SOW_deliverables_tracker.md](SOW_deliverables_tracker.md) | Full deliverable status tracking |
