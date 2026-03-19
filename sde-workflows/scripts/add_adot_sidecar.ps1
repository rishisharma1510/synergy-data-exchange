param(
    [string]$Family = "ss-cdkdev-sde-express",
    [string]$Region = "us-east-1"
)

$ErrorActionPreference = "Stop"
Write-Host "Fetching task definition for: $Family"

# Get current task definition
$td = (aws ecs describe-task-definition --task-definition $Family --query "taskDefinition" 2>&1 | Out-String) | ConvertFrom-Json

Write-Host "Current revision: $($td.revision)"
Write-Host "Current containers: $($td.containerDefinitions.Count)"

# Remove read-only fields
$readOnly = @("taskDefinitionArn","revision","status","requiresAttributes","placementConstraints","compatibilities","registeredAt","registeredBy")
foreach ($field in $readOnly) {
    if ($td.PSObject.Properties[$field]) {
        $td.PSObject.Properties.Remove($field)
    }
}

# Check if adot-sidecar already present
$existing = $td.containerDefinitions | Where-Object { $_.name -eq "adot-sidecar" }
if ($existing) {
    Write-Host "adot-sidecar already present in task definition. Skipping."
    exit 0
}

# ADOT sidecar definition
$adotJson = @"
{
    "name": "adot-sidecar",
    "image": "public.ecr.aws/aws-observability/aws-otel-collector:latest",
    "essential": false,
    "cpu": 256,
    "memory": 512,
    "command": ["--config", "/etc/ecs/ecs-default-config.yaml"],
    "environment": [
        { "name": "OTEL_SERVICE_NAME", "value": "$Family" }
    ],
    "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
            "awslogs-group": "/ecs/ss-cdkdev-sde/adot",
            "awslogs-region": "$Region",
            "awslogs-stream-prefix": "$Family",
            "awslogs-create-group": "true"
        }
    }
}
"@

$adot = $adotJson | ConvertFrom-Json

# Append sidecar
$td.containerDefinitions = [array]$td.containerDefinitions + @($adot)
Write-Host "Containers after adding sidecar: $($td.containerDefinitions.Count)"

# Write to temp file
$tmpFile = "$env:TEMP\adot-td-$Family.json"
$td | ConvertTo-Json -Depth 25 | Out-File $tmpFile -Encoding utf8
Write-Host "Written to: $tmpFile"

# Register new revision
Write-Host "Registering new task definition revision..."
$out = aws ecs register-task-definition --cli-input-json "file://$tmpFile" 2>&1 | Out-String
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: $out" -ForegroundColor Red
    exit 1
}
$newTd = $out | ConvertFrom-Json
Write-Host "SUCCESS: $($newTd.taskDefinition.family) revision $($newTd.taskDefinition.revision) registered" -ForegroundColor Green
Write-Host "Containers: $($newTd.taskDefinition.containerDefinitions.name -join ', ')"
