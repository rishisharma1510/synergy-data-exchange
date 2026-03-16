# Synergy Data Exchange CDK Bootstrap

This project includes a custom bootstrap stack that replaces the standard `cdk bootstrap` command.

## Why a Custom Bootstrap Stack?

In enterprise environments with strict IAM policies (like Verisk's VA-PB-Standard permissions boundary), the standard `cdk bootstrap` command may fail because it tries to create IAM roles without proper permissions boundaries.

The `SDEBootstrapStack` creates all required CDK infrastructure:
- **S3 Bucket**: For storing file assets during deployment
- **ECR Repository**: For storing container images during deployment
- **IAM Roles**:
  - Deploy Role: Assumed by CDK CLI to deploy stacks
  - File Publishing Role: Publishes file assets to S3
  - Image Publishing Role: Publishes container images to ECR
  - CloudFormation Execution Role: Used by CloudFormation to create resources
  - Lookup Role: Used for VPC and resource lookups

## Bootstrap Role Naming Convention

Roles follow the pattern: `ss-cdk-{qualifier}-{role-type}-{account}-{region}`

Where `{qualifier}` is `sde` by default, resulting in roles like:
- `ss-cdk-sde-deploy-role-451952076009-us-east-1`
- `ss-cdk-sde-cfn-exec-role-451952076009-us-east-1`
- etc.

## Deploying the Bootstrap Stack

### Option 1: Using deploy-local.ps1 (Recommended for local development)

```powershell
.\deploy-local.ps1 -Environment dev
```

This will automatically deploy the `CDKToolkit-sde` bootstrap stack first, then all SDE stacks.

### Option 2: Deploy bootstrap stack only

```powershell
cd cdk
cdk deploy "CDKToolkit-sde" `
    -c TargetEnvironment=dev `
    -c DeployBootstrapStack=true `
    --require-approval never
```

This deploys `SDEBootstrapStack` as `CDKToolkit-sde` CloudFormation stack. It uses `LegacyStackSynthesizer`
so no prior bootstrap state is needed — it bootstraps itself.

### Option 3: Skip bootstrap if already deployed

```powershell
.\deploy-local.ps1 -Environment dev -SkipBootstrap
```

## Permissions Boundary

If your organization requires a permissions boundary on all IAM roles, set the environment variable:

```powershell
$env:CDK_PERMISSIONS_BOUNDARY = "VA-PB-Standard"
```

This will be applied to all roles created by the bootstrap stack.

## Execution Policies

The bootstrap stack uses execution policies defined in:
- `BootstrapsExecutionPolicies/exec-policy-1.json` - Core permissions (CloudFormation, IAM, Lambda, S3, ECR, ECS, etc.)
- `BootstrapsExecutionPolicies/exec-policy-2.json` - Additional permissions (Batch, Step Functions, EC2 networking, etc.)

These policies define what the CloudFormation Execution Role is allowed to do when creating resources.

## Troubleshooting

### Error: "User is not authorized to perform: iam:CreateRole"

This means your current role cannot create IAM roles. Options:
1. Request elevated permissions from your cloud team
2. Have your cloud team pre-create the bootstrap roles manually
3. Use an existing CDK bootstrap if one is available in your account

### Error: "Bucket already exists"

The bootstrap S3 bucket already exists. This is usually fine - the stack will use the existing bucket.

### Error: "Stack already exists"

The CDKToolkit stack already exists from a previous bootstrap. You can:
1. Skip bootstrap: `.\deploy-local.ps1 -SkipBootstrap`
2. Delete the existing stack: `aws cloudformation delete-stack --stack-name CDKToolkit`

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    CDKToolkit Stack                          │
├─────────────────────────────────────────────────────────────┤
│  S3 Bucket: ss-cdk-sde-assets-{account}-{region}            │
│  ECR Repo:  ss-cdk-sde-container-assets-{account}-{region}  │
│                                                              │
│  IAM Roles (with permissions boundary if configured):        │
│    - ss-cdk-sde-deploy-role                                 │
│    - ss-cdk-sde-file-publishing-role                        │
│    - ss-cdk-sde-image-publishing-role                       │
│    - ss-cdk-sde-cfn-exec-role                               │
│    - ss-cdk-sde-lookup-role                                 │
│                                                              │
│  SSM Parameter: /cdk-bootstrap/sde/version                   │
└─────────────────────────────────────────────────────────────┘
```
