# =============================================================================
# Build and Push Docker Images for All Paths (PowerShell)
# =============================================================================
# Usage:
#   .\docker\build.ps1                           # Build all images locally
#   .\docker\build.ps1 -Push                     # Build and push to ECR
#   .\docker\build.ps1 -Patch                    # Pull existing, patch src files, push (no OS rebuild)
#   .\docker\build.ps1 -Path express             # Build only express image
#   .\docker\build.ps1 -Tag v1.2.3               # Use specific tag
#
# NOTE: Auto-detects podman (local) or docker (CI). Full rebuilds may fail behind a
#       corporate TLS-intercepting proxy (apt-get SSL error). Use -Patch in that case.
# =============================================================================

param(
    [switch]$Push,
    [switch]$Patch,   # Patch-in-place: pull -> copy changed files -> commit -> push
    [ValidateSet("all", "express", "standard", "batch", "path1", "path2", "path3")]
    [string]$Path = "all",
    [string]$Tag = "latest",
    [string]$Region = "us-east-1",
    [string]$AccountId = "",
    [string]$Stage = "dev"   # CDK stage suffix (dev, prod, etc.)
)

$ErrorActionPreference = "Stop"

# ECR repo names follow the CDK naming convention: ss-cdk{stage}-sde-{type}
# e.g. ss-cdkdev-sde-express
$ECR_REPO_PREFIX = "ss-cdk${Stage}-Synergy Data Exchange"

# Files to patch when using -Patch mode (local path -> container path)
$PATCH_FILES = @(
    @{ Local = "src/fargate/entrypoint.py";           Container = "/app/src/fargate/entrypoint.py" },
    @{ Local = "src/fargate/ingestion_entrypoint.py"; Container = "/app/src/fargate/ingestion_entrypoint.py" },
    @{ Local = "src/fargate/progress_reporter.py";    Container = "/app/src/fargate/progress_reporter.py" },
    @{ Local = "src/fargate/artifact_generator.py";   Container = "/app/src/fargate/artifact_generator.py" },
    @{ Local = "src/fargate/validation.py";           Container = "/app/src/fargate/validation.py" },
    @{ Local = "src/core/config.py";                  Container = "/app/src/core/config.py" },
    @{ Local = "src/core/ingest.py";                  Container = "/app/src/core/ingest.py" },
    @{ Local = "src/core/iceberg_reader.py";          Container = "/app/src/core/iceberg_reader.py" },
    @{ Local = "src/core/iceberg_writer.py";          Container = "/app/src/core/iceberg_writer.py" },
    @{ Local = "src/core/column_mapper.py";           Container = "/app/src/core/column_mapper.py" },
    @{ Local = "src/core/sql_writer.py";              Container = "/app/src/core/sql_writer.py" },
    @{ Local = "src/core/sql_reader.py";              Container = "/app/src/core/sql_reader.py" },
    @{ Local = "src/core/mapping_config.py";          Container = "/app/src/core/mapping_config.py" },
    @{ Local = "src/models/execution_plan.py";        Container = "/app/src/models/execution_plan.py" },
    @{ Local = "src/models/manifest.py";              Container = "/app/src/models/manifest.py" }
)

# Auto-detect container runtime (podman locally, docker in CI)
if (Get-Command podman -ErrorAction SilentlyContinue) {
    $ContainerTool = "podman"
    $TlsFlag = @("--tls-verify=false")
} else {
    $ContainerTool = "docker"
    $TlsFlag = @()
}

# Get AWS Account ID if not provided
if (-not $AccountId -and ($Push -or $Patch)) {
    try {
        $AccountId = (aws sts get-caller-identity --query Account --output text)
    } catch {
        Write-Error "Could not determine AWS Account ID. Please provide -AccountId parameter."
        exit 1
    }
}

$ECR_URL = "${AccountId}.dkr.ecr.${Region}.amazonaws.com"

Write-Host "=============================================="
Write-Host "Docker Image Build Script"
Write-Host "=============================================="
Write-Host "Build Path: $Path"
Write-Host "Tag: $Tag"
Write-Host "Push to ECR: $Push"
Write-Host "Patch mode: $Patch"
if ($Push -or $Patch) {
    Write-Host "ECR URL: $ECR_URL"
    Write-Host "ECR prefix: $ECR_REPO_PREFIX"
}
Write-Host "=============================================="

# Change to project root
Push-Location (Split-Path -Parent $PSScriptRoot)

function Build-Image {
    param(
        [string]$Name,
        [string]$Dockerfile
    )
    
    $FullUri = "${ECR_URL}/${ECR_REPO_PREFIX}-${Name}:${Tag}"
    
    Write-Host ""
    Write-Host "Building: $FullUri"
    Write-Host "Dockerfile: $Dockerfile"
    Write-Host "----------------------------------------------"
    
    # Build locally
    & $ContainerTool build `
        -f "docker/$Dockerfile" `
        -t $FullUri `
        -t "${ECR_URL}/${ECR_REPO_PREFIX}-${Name}:latest" `
        .
    
    if ($LASTEXITCODE -ne 0) {
        throw "Build failed for $FullUri"
    }
    
    if ($Push) {
        Write-Host "Pushing $FullUri"
        & $ContainerTool push @TlsFlag $FullUri
        if ($LASTEXITCODE -ne 0) {
            throw "Push failed for $FullUri"
        }
        if ($Tag -ne "latest") {
            & $ContainerTool push @TlsFlag "${ECR_URL}/${ECR_REPO_PREFIX}-${Name}:latest"
        }
    }
    
    Write-Host "OK: $FullUri built successfully" -ForegroundColor Green
}

function Patch-Image {
    param([string]$Name)
    
    $imageUri = "${ECR_URL}/${ECR_REPO_PREFIX}-${Name}:${Tag}"
    $containerName = "tmp-patch-$Name"

    Write-Host ""
    Write-Host "Patching: $imageUri" -ForegroundColor Cyan

    & $ContainerTool pull @TlsFlag $imageUri
    if ($LASTEXITCODE -ne 0) { throw "Pull failed for $imageUri" }

    & $ContainerTool create --name $containerName $imageUri | Out-Null

    foreach ($f in $PATCH_FILES) {
        & $ContainerTool cp $f.Local "${containerName}:$($f.Container)"
        if ($LASTEXITCODE -ne 0) { & $ContainerTool rm $containerName | Out-Null; throw "Copy failed: $($f.Local)" }
        Write-Host "  Copied $($f.Local)" -ForegroundColor Gray
    }

    & $ContainerTool commit $containerName $imageUri
    if ($LASTEXITCODE -ne 0) { & $ContainerTool rm $containerName | Out-Null; throw "Commit failed for $containerName" }
    & $ContainerTool rm $containerName | Out-Null

    if ($Push) {
        & $ContainerTool push @TlsFlag $imageUri
        if ($LASTEXITCODE -ne 0) { throw "Push failed for $imageUri" }
    }

    Write-Host "OK: $imageUri patched" -ForegroundColor Green
}

try {
    # Login to ECR when pushing/patching
    if ($Push -or $Patch) {
        Write-Host "Logging in to ECR..."
        $ecrPassword = aws ecr get-login-password --region $Region
        $ecrPassword | & $ContainerTool login --username AWS --password-stdin @TlsFlag $ECR_URL
        if ($LASTEXITCODE -ne 0) { throw "ECR login failed" }
    }

    # Run on selected images
    switch ($Path) {
        { $_ -in "express", "path1" } {
            if ($Patch) { Patch-Image "express" } else { Build-Image -Name "express" -Dockerfile "Dockerfile.express" }
        }
        { $_ -in "standard", "path2" } {
            if ($Patch) { Patch-Image "standard" } else { Build-Image -Name "standard" -Dockerfile "Dockerfile.standard" }
        }
        { $_ -in "batch", "path3" } {
            if ($Patch) { Patch-Image "batch" } else { Build-Image -Name "batch" -Dockerfile "Dockerfile.batch" }
        }
        "all" {
            if ($Patch) {
                Patch-Image "express"
                Patch-Image "standard"
                Patch-Image "batch"
            } else {
                Build-Image -Name "express"  -Dockerfile "Dockerfile.express"
                Build-Image -Name "standard" -Dockerfile "Dockerfile.standard"
                Build-Image -Name "batch"    -Dockerfile "Dockerfile.batch"
            }
        }
    }

    $op = if ($Patch) { "Patch" } else { "Build" }
    Write-Host ""
    Write-Host "=============================================="
    Write-Host "$op Complete!" -ForegroundColor Green
    Write-Host "=============================================="
    Write-Host ""
    Write-Host "${op}d images:"
    & $ContainerTool images | Select-String $ECR_REPO_PREFIX | Select-Object -First 10

} finally {
    Pop-Location
}
