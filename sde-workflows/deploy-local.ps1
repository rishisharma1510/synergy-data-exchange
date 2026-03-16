# deploy-local.ps1 - Manual deployment script for Synergy Data Exchange
#
# Usage:
#   .\deploy-local.ps1                          # Full deploy (bootstrap + CDK + images + Lambda/SFN)
#   .\deploy-local.ps1 -SkipBootstrap           # Skip CDK bootstrap
#   .\deploy-local.ps1 -SkipImages              # Skip image build/push (Lambda/SFN still runs)
#   .\deploy-local.ps1 -PatchImages             # Patch images in-place (no full rebuild; fast, works behind SSL proxy)
#   .\deploy-local.ps1 -SkipCdk                 # Skip all CDK stacks; only push images + update Lambda/SFN
#
# NOTE: Auto-detects podman (local) or docker (CI/GitHub Actions). Behind a corporate TLS
#       proxy, use -PatchImages to avoid apt-get SSL failures during Docker layer builds.
param(
    [string]$Environment = "dev",
    [string]$Region = "us-east-1",
    [switch]$SkipImages,
    [switch]$SkipEcr,
    [switch]$SkipBootstrap,
    [switch]$SkipCdk,        # Skip all CDK stack deployments
    [switch]$PatchImages,    # Patch-only: pull image, copy changed src files, commit, push (no OS-level rebuild)
    [string]$Qualifier = "ss-sde"
)

$ErrorActionPreference = "Stop"
$WORKSPACE = "c:\Github\Rnd\DataIngestion"
$CDK_DIR = "$WORKSPACE\cdk"  # Where cdk.json lives
$CDK_PROJECT_DIR = "$WORKSPACE\cdk\src\EES.AP.SDE.CDK"

function Assert-ExitCode {
    param([string]$StepName)
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: $StepName failed with exit code $LASTEXITCODE" -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

# Get AWS account
$AWS_ACCOUNT_ID = aws sts get-caller-identity --query Account --output text
Assert-ExitCode "AWS credentials check"
Write-Host "Deploying to AWS account: $AWS_ACCOUNT_ID" -ForegroundColor Green
Write-Host "Environment: $Environment" -ForegroundColor Green
Write-Host "Region: $Region" -ForegroundColor Green
Write-Host "Qualifier: $Qualifier" -ForegroundColor Green

# Auto-detect container runtime (podman locally, docker in CI)
if (Get-Command podman -ErrorAction SilentlyContinue) {
    $ContainerTool = "podman"
    $TlsFlag = @("--tls-verify=false")
} else {
    $ContainerTool = "docker"
    $TlsFlag = @()
}
Write-Host "Container tool: $ContainerTool" -ForegroundColor Gray

# Set environment
$env:ENVIRONMENT = $Environment
$env:AWS_REGION = $Region
$env:AWS_ACCOUNT_ID = $AWS_ACCOUNT_ID

# Default environment values (can be overridden by GitHub environment variables)
$defaults = @{
    VpcId = "vpc-024efe736c9fe2a15"
    SubnetIds = "subnet-00d8ca339521c44c0,subnet-07ac9cca6864c1c8c,subnet-07c569a1f9e4ea131,subnet-08b0b5d09497b90f1,subnet-0a7f93123774da400"
    SecurityGroupIds = "sg-0747b695cf75d47db"
    BaseUrl = "https://dev-na.synergystudio.verisk.com"
    Audience = "api://synergy-studio-dev"
    Issuer = "https://sso-dev-na.synergystudio.verisk.com/oauth2/aus281t8zz5gF8z3u0h8"
    EnvironmentType = "dev"
}

# ===== Resource / routing configuration =====
# Override before running to change routing thresholds, Fargate resources, or parallelism.
# These match GitHub environment variables of the same name (set in ees-vss-deployment).
# Only sets defaults — if the var is already set in the shell, the existing value is preserved.
if (-not $env:THRESHOLD_SMALL_GB)            { $env:THRESHOLD_SMALL_GB             = '9'     }  # ≤N GB → Fargate Express
if (-not $env:THRESHOLD_LARGE_GB)            { $env:THRESHOLD_LARGE_GB             = '80'    }  # ≤N GB → Fargate Standard; >N → Batch
if (-not $env:EXPRESS_CPU)                   { $env:EXPRESS_CPU                    = '8192'  }  # 8 vCPU (1024 = 1 vCPU)
if (-not $env:EXPRESS_MEMORY)                { $env:EXPRESS_MEMORY                 = '61440' }  # 60 GiB
if (-not $env:EXPRESS_MAX_PARALLEL_TABLES)   { $env:EXPRESS_MAX_PARALLEL_TABLES    = '5'     }  # parallel workers
if (-not $env:STANDARD_CPU)                  { $env:STANDARD_CPU                   = '16384' }  # 16 vCPU
if (-not $env:STANDARD_MEMORY)               { $env:STANDARD_MEMORY                = '122880' }  # 120 GiB
if (-not $env:STANDARD_MAX_PARALLEL_TABLES)  { $env:STANDARD_MAX_PARALLEL_TABLES   = '10'    }  # parallel workers
if (-not $env:BATCH_CPU)                     { $env:BATCH_CPU                      = '16'    }  # vCPU (r5.4xlarge)
if (-not $env:BATCH_MEMORY)                  { $env:BATCH_MEMORY                   = '65536' }  # 64 GiB
if (-not $env:BATCH_MAX_PARALLEL_TABLES)     { $env:BATCH_MAX_PARALLEL_TABLES      = '16'    }  # parallel workers
if (-not $env:BATCH_MAX_VCPUS)               { $env:BATCH_MAX_VCPUS                = '1024'  }  # compute env ceiling

Write-Host "Thresholds:        Express≤$($env:THRESHOLD_SMALL_GB)GB / Standard≤$($env:THRESHOLD_LARGE_GB)GB / Batch>$($env:THRESHOLD_LARGE_GB)GB" -ForegroundColor Gray
Write-Host "Fargate Express:   $($env:EXPRESS_CPU) CPU / $($env:EXPRESS_MEMORY) MiB / $($env:EXPRESS_MAX_PARALLEL_TABLES) workers" -ForegroundColor Gray
Write-Host "Fargate Standard:  $($env:STANDARD_CPU) CPU / $($env:STANDARD_MEMORY) MiB / $($env:STANDARD_MAX_PARALLEL_TABLES) workers" -ForegroundColor Gray
Write-Host "Batch:             $($env:BATCH_CPU) vCPU / $($env:BATCH_MEMORY) MiB / $($env:BATCH_MAX_PARALLEL_TABLES) workers" -ForegroundColor Gray

# Configure CDK settings
$env:SDE_CDK_Settings__InstanceStage = $Environment
$env:SDE_CDK_Settings__EnvironmentType = $defaults.EnvironmentType
$env:SDE_CDK_Settings__CDK_DEFAULT_ACCOUNT = $AWS_ACCOUNT_ID
$env:SDE_CDK_Settings__CDK_DEFAULT_REGION = $Region

# Forward resource/routing variables to CDK via settings env var binding
$env:SDE_CDK_Settings__Fargate__ExpressCpu                 = $env:EXPRESS_CPU
$env:SDE_CDK_Settings__Fargate__ExpressMemory               = $env:EXPRESS_MEMORY
$env:SDE_CDK_Settings__Fargate__ExpressMaxParallelTables    = $env:EXPRESS_MAX_PARALLEL_TABLES
$env:SDE_CDK_Settings__Fargate__StandardCpu                 = $env:STANDARD_CPU
$env:SDE_CDK_Settings__Fargate__StandardMemory              = $env:STANDARD_MEMORY
$env:SDE_CDK_Settings__Fargate__StandardMaxParallelTables   = $env:STANDARD_MAX_PARALLEL_TABLES
$env:SDE_CDK_Settings__Batch__JobVcpus                      = $env:BATCH_CPU
$env:SDE_CDK_Settings__Batch__JobMemory                     = $env:BATCH_MEMORY
$env:SDE_CDK_Settings__Batch__JobMaxParallelTables          = $env:BATCH_MAX_PARALLEL_TABLES
$env:SDE_CDK_Settings__Batch__MaxVcpus                      = $env:BATCH_MAX_VCPUS
# SQL edition: 'Developer' for non-prod (no SPLA cost), 'Standard' for prod.
# Override via: $env:SQL_EDITION = "Developer" before running this script.
$sqlEdition = if ($env:SQL_EDITION) { $env:SQL_EDITION } else { "Standard" }
$env:SDE_CDK_Settings__Fargate__SqlEdition                   = $sqlEdition
$env:SDE_CDK_Settings__Batch__SqlEdition                     = $sqlEdition
Write-Host "SQL Edition:        $sqlEdition" -ForegroundColor Gray
$env:SDE_CDK_Settings__StepFunctions__ThresholdSmallGb      = $env:THRESHOLD_SMALL_GB
$env:SDE_CDK_Settings__StepFunctions__ThresholdLargeGb      = $env:THRESHOLD_LARGE_GB

# Configure VPC settings
$env:SDE_CDK_ApplicationVariables__VpcId = $defaults.VpcId
$env:SDE_CDK_ApplicationVariables__SubnetIds = $defaults.SubnetIds
$env:SDE_CDK_ApplicationVariables__SecurityGroupIds = $defaults.SecurityGroupIds
# Full API URLs constructed here — update the path version in one place if it ever changes
$env:SDE_CDK_ApplicationVariables__ExposureApiUrl = "$($defaults.BaseUrl)/api/exposure/v1"
$env:SDE_CDK_ApplicationVariables__SidApiUrl = "$($defaults.BaseUrl)/api/sid/v1"

# Configure API settings (Iceberg/Glue info comes from TenantContext at runtime)
$env:SDE_CDK_ApplicationVariables__TenantContextLambda = "rs-cdk$Environment-ap-tenant-context-lambda"
$env:SDE_CDK_ApplicationVariables__AppTokenSecret = "rs-cdk$Environment-app-token-secret"
$env:SDE_CDK_ApplicationVariables__IdentityProviderTokenEndpoint = "$($defaults.Issuer)/v1/token"

$REPO_PREFIX = "$AWS_ACCOUNT_ID.dkr.ecr.$Region.amazonaws.com/ss-cdk$Environment"
$IMAGE_TAG = "latest"

# Ensure appsettings file exists for the environment
$appsettingsSource = "$CDK_PROJECT_DIR\appsettings.$Environment.json"
if (-not (Test-Path $appsettingsSource)) {
    Write-Host "WARNING: No appsettings.$Environment.json found. Using base appsettings.json" -ForegroundColor Yellow
    Write-Host "Create $appsettingsSource for proper environment configuration" -ForegroundColor Yellow
}

# Step 0: CDK Bootstrap (uses cdk-bootstrap like in CI/CD)
# This creates the required IAM roles, S3 bucket, and ECR repo for CDK deployments
if (-not $SkipBootstrap) {
    Write-Host "`n=== Deploying CDK Bootstrap Stack ===" -ForegroundColor Cyan
    Write-Host "This creates the CDK deployment infrastructure (IAM roles, S3 bucket, ECR repo, SSM param)" -ForegroundColor Yellow
    Write-Host "Stack name: CDKToolkit-sde  |  Qualifier: sde" -ForegroundColor Yellow
    
    # Required in Verisk: VA-PB-Standard permissions boundary must be applied to all new IAM roles
    $env:CDK_PERMISSIONS_BOUNDARY = "VA-PB-Standard"
    
    Push-Location $CDK_DIR
    
    # Deploy the custom SDEBootstrapStack (LegacyStackSynthesizer - no pre-existing bootstrap needed)
    # Creates: ss-cdk-sde-{deploy,cfn-exec,file-publishing,image-publishing,lookup}-role-*
    #          ss-cdk-sde-assets-{account}-{region} S3 bucket
    #          ss-cdk-sde-container-assets-{account}-{region} ECR repo
    #          /cdk-bootstrap/sde/version SSM parameter
    cdk deploy "CDKToolkit-sde" `
        -c TargetEnvironment=$Environment `
        -c DeployBootstrapStack=true `
        --require-approval never
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Bootstrap deploy failed. Check the output above for errors." -ForegroundColor Red
        exit $LASTEXITCODE
    }
    
    Pop-Location
    Write-Host "Bootstrap complete." -ForegroundColor Green
}

# Step 1: Deploy ECR
if (-not $SkipEcr) {
    Write-Host "`n=== Deploying ECR Stack ===" -ForegroundColor Cyan
    Push-Location $CDK_DIR
    cdk deploy "ss-cdk-sde-ecr-$Environment" `
        -c TargetEnvironment=$Environment `
        -c DeployEcrStack=true `
        --require-approval never
    Assert-ExitCode "ECR Stack"
    Pop-Location
}

# Step 2: Build / patch and push images
if (-not $SkipImages) {
    Write-Host "`n=== ECR Login ===" -ForegroundColor Cyan
    Push-Location $WORKSPACE

    # ECR login
    $ecrPassword = aws ecr get-login-password --region $Region
    $ecrPassword | & $ContainerTool login --username AWS --password-stdin @TlsFlag "$AWS_ACCOUNT_ID.dkr.ecr.$Region.amazonaws.com"
    Assert-ExitCode "ECR Login"

    if ($PatchImages) {
        # ------------------------------------------------------------------
        # PATCH MODE: pull existing image, overwrite changed source files,
        # commit a new layer, push. Avoids apt-get which fails behind SSL proxy.
        # ------------------------------------------------------------------
        Write-Host "`n=== Patching Images (no full rebuild) ===" -ForegroundColor Cyan

        $ChangedFiles = @(
            @{ Local = "src/fargate/ingestion_entrypoint.py"; Container = "/app/src/fargate/ingestion_entrypoint.py" },
            @{ Local = "src/fargate/entrypoint.py";           Container = "/app/src/fargate/entrypoint.py" },
            @{ Local = "src/fargate/artifact_generator.py";   Container = "/app/src/fargate/artifact_generator.py" },
            @{ Local = "src/fargate/database_manager.py";     Container = "/app/src/fargate/database_manager.py" },
            @{ Local = "src/core/config.py";                  Container = "/app/src/core/config.py" },
            @{ Local = "src/core/iceberg_reader.py";          Container = "/app/src/core/iceberg_reader.py" },
            @{ Local = "src/core/ingest.py";                  Container = "/app/src/core/ingest.py" },
            @{ Local = "src/core/mapping_config.py";          Container = "/app/src/core/mapping_config.py" },
            @{ Local = "src/models/execution_plan.py";        Container = "/app/src/models/execution_plan.py" },
            @{ Local = "src/core/tenant_context.py";          Container = "/app/src/core/tenant_context.py" },
            @{ Local = "src/sid/sid_client.py";               Container = "/app/src/sid/sid_client.py" },
            @{ Local = "src/sid/exposure_client.py";          Container = "/app/src/sid/exposure_client.py" }
        )

        foreach ($imageSuffix in @("express", "standard")) {
            $imageUri = "${REPO_PREFIX}-sde-${imageSuffix}:${IMAGE_TAG}"
            Write-Host "  Pulling $imageUri ..." -ForegroundColor Yellow
            & $ContainerTool pull @TlsFlag $imageUri
            Assert-ExitCode "Pull $imageSuffix"

            $containerName = "tmp-patch-$imageSuffix"
            & $ContainerTool create --name $containerName $imageUri | Out-Null

            foreach ($f in $ChangedFiles) {
                & $ContainerTool cp $f.Local "${containerName}:$($f.Container)"
                Assert-ExitCode "Copy $($f.Local) -> $imageSuffix"
                Write-Host "  Copied $($f.Local)" -ForegroundColor Gray
            }

            & $ContainerTool commit $containerName $imageUri
            Assert-ExitCode "Commit $imageSuffix"
            & $ContainerTool rm $containerName | Out-Null

            Write-Host "  Pushing $imageUri ..." -ForegroundColor Yellow
            & $ContainerTool push @TlsFlag $imageUri
            Assert-ExitCode "Push $imageSuffix"
            Write-Host "  OK: $imageUri patched and pushed" -ForegroundColor Green
        }
    } else {
        # ------------------------------------------------------------------
        # FULL BUILD MODE
        # Note: may fail behind corporate SSL proxy during apt-get.
        # Use -PatchImages if that happens.
        # ------------------------------------------------------------------
        Write-Host "`n=== Building and Pushing Images (full rebuild) ===" -ForegroundColor Cyan

        foreach ($target in @(
            @{ Suffix = "express";  Dockerfile = "Dockerfile.express"  },
            @{ Suffix = "standard"; Dockerfile = "Dockerfile.standard" },
            @{ Suffix = "batch";    Dockerfile = "Dockerfile.batch"    }
        )) {
            $imageUri = "${REPO_PREFIX}-sde-$($target.Suffix):${IMAGE_TAG}"
            & $ContainerTool build -f "docker/$($target.Dockerfile)" -t $imageUri .
            Assert-ExitCode "Build $($target.Suffix) image"
            & $ContainerTool push @TlsFlag $imageUri
            Assert-ExitCode "Push $($target.Suffix) image"
            Write-Host "  OK: $imageUri" -ForegroundColor Green
        }
    }

    Pop-Location
}

if (-not $SkipCdk) {
    # Step 2b: Deploy Monitoring Stack (SPLA cost tracking, CloudWatch dashboard)
    Write-Host "`n=== Deploying Monitoring Stack ===" -ForegroundColor Cyan
    Push-Location $CDK_DIR
    cdk deploy "ss-cdk-sde-monitoring-$Environment" `
        -c TargetEnvironment=$Environment `
        -c DeployMonitoringStack=true `
        --require-approval never
    Assert-ExitCode "Monitoring Stack"
    Pop-Location

    # Step 3: Deploy Fargate, Batch, then Step Functions in dependency order.
    # StepFunctions imports CloudFormation exports from Fargate (ClusterArn, SecurityGroupId)
    # and Batch (JobQueueArn), so Fargate and Batch MUST be fully deployed first.
    Write-Host "`n=== Deploying Fargate Stack ===" -ForegroundColor Cyan
    Push-Location $CDK_DIR
    cdk deploy "ss-cdk-sde-fargate-$Environment" `
        -c TargetEnvironment=$Environment `
        -c DeployFargateStack=true `
        -c ImageTag=$IMAGE_TAG `
        --require-approval never
    Assert-ExitCode "Fargate Stack"
    Pop-Location

    Write-Host "`n=== Deploying Batch Stack ===" -ForegroundColor Cyan
    Push-Location $CDK_DIR
    cdk deploy "ss-cdk-sde-batch-$Environment" `
        -c TargetEnvironment=$Environment `
        -c DeployBatchStack=true `
        -c ImageTag=$IMAGE_TAG `
        --require-approval never
    Assert-ExitCode "Batch Stack"
    Pop-Location

    Write-Host "`n=== Deploying Step Functions Stack ===" -ForegroundColor Cyan
    Push-Location $CDK_DIR
    cdk deploy "ss-cdk-sde-stepfunctions-$Environment" `
        -c TargetEnvironment=$Environment `
        -c DeployStepFunctionsStack=true `
        -c ImageTag=$IMAGE_TAG `
        --require-approval never
    Assert-ExitCode "Step Functions Stack"
    Pop-Location
} else {
    Write-Host "`n=== Skipping CDK stacks (-SkipCdk) ===" -ForegroundColor Yellow
}

# Step 6: Update Lambda + register new task-def revisions + patch state machine
# This step is ALWAYS run (even with -SkipCdk / -PatchImages) because it is required
# to point the state machine at the newest task-definition revision and freshest Lambda code.
# CDK-managed deployments keep CloudFormation in sync automatically; when deploying manually
# (or patch-only) this Python script does the equivalent without touching CloudFormation.
Write-Host "`n=== Updating Lambda / Task Definitions / State Machine ===" -ForegroundColor Cyan
python "$WORKSPACE\scripts\deploy_updates.py"
Assert-ExitCode "Lambda + TaskDef + SFN update"

Write-Host "`n=== Deployment Complete ===" -ForegroundColor Green
Write-Host "Environment:       $Environment" -ForegroundColor Green
Write-Host "Bootstrap Qualifier: $Qualifier" -ForegroundColor Green
Write-Host "PatchImages:       $PatchImages" -ForegroundColor Green
Write-Host "SkipCdk:           $SkipCdk" -ForegroundColor Green