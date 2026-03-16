"""deploy_updates.py — Code-only deployment for Synergy Data Exchange.

Updates deployed resources WITHOUT a CDK/CloudFormation deploy.
Use this when CDK can't run (e.g. CloudFormation export conflicts) or
for quick code-only hotfixes.

What it does
------------
1. Patches Fargate images (express + standard) in ECR:
   pull -> overwrite changed src files -> commit -> push
   (avoids full Docker rebuild which fails behind TLS-intercepting proxies)
2. Updates the Lambda function code (size_estimator)
3. Registers a new ECS task-definition revision (cloning the last known-good one)
4. Updates the Step Functions state machine definition to point at the new revisions

Changed files patched into images
----------------------------------
- src/fargate/entrypoint.py  (s3_output_path support for final MDF/BAK output)
- src/core/config.py         (ICEBERG_NAMESPACE fallback to GLUE_DATABASE)

Lambda changes
--------------
- cdk/src/lambda/size_estimator.py  (Athena output -> athenaoutput/{activity_id}/)

Usage
-----
    python scripts/deploy_updates.py
"""
import io
import os
import zipfile
import boto3

REGION = 'us-east-1'
SFN_ARN           = 'arn:aws:states:us-east-1:451952076009:stateMachine:ss-cdkdev-sde-extraction'
INGESTION_SFN_ARN = 'arn:aws:states:us-east-1:451952076009:stateMachine:ss-cdkdev-sde-ingestion'
LAMBDA_NAME = 'ss-cdkdev-sde-size-estimator'
ECR_ACCOUNT = '451952076009'
ECR_BASE = f'{ECR_ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com'

# Files to patch into Fargate images (local workspace path -> container path)
PATCH_FILES = [
    ('src/fargate/ingestion_entrypoint.py', '/app/src/fargate/ingestion_entrypoint.py'),
    ('src/fargate/entrypoint.py',           '/app/src/fargate/entrypoint.py'),
    ('src/fargate/artifact_generator.py',   '/app/src/fargate/artifact_generator.py'),
    ('src/fargate/database_manager.py',     '/app/src/fargate/database_manager.py'),
    ('src/fargate/progress_reporter.py',    '/app/src/fargate/progress_reporter.py'),
    ('src/core/config.py',                  '/app/src/core/config.py'),
    ('src/core/iceberg_reader.py',          '/app/src/core/iceberg_reader.py'),
    ('src/core/iceberg_writer.py',          '/app/src/core/iceberg_writer.py'),
    ('src/core/ingest.py',                  '/app/src/core/ingest.py'),
    ('src/core/mapping_config.py',          '/app/src/core/mapping_config.py'),
    ('src/models/execution_plan.py',        '/app/src/models/execution_plan.py'),
    ('src/core/tenant_context.py',          '/app/src/core/tenant_context.py'),
    ('src/sid/sid_client.py',               '/app/src/sid/sid_client.py'),
    ('src/sid/exposure_client.py',          '/app/src/sid/exposure_client.py'),
]

# ===========================================================================
# Resource / routing configuration
# ===========================================================================
# Read from environment variables — same names used as GitHub environment variables.
# Override locally:  $env:EXPRESS_CPU = "2048"  (PowerShell)  or  export EXPRESS_CPU=2048  (bash)
# Defaults match CDK SDEConfiguration model defaults.
EXPRESS_CPU        = int(os.environ.get('EXPRESS_CPU',        8192))   # 8 vCPU  (1024 = 1 vCPU)
EXPRESS_MEMORY     = int(os.environ.get('EXPRESS_MEMORY',     61440))  # 60 GiB
STANDARD_CPU       = int(os.environ.get('STANDARD_CPU',       16384))  # 16 vCPU
STANDARD_MEMORY    = int(os.environ.get('STANDARD_MEMORY',    122880)) # 120 GiB
THRESHOLD_SMALL_GB = int(os.environ.get('THRESHOLD_SMALL_GB', 9))      # ≤N GB → Fargate Express
THRESHOLD_LARGE_GB = int(os.environ.get('THRESHOLD_LARGE_GB', 80))     # ≤N GB → Fargate Standard; >N → Batch

EXPRESS_MAX_WORKERS  = int(os.environ.get('EXPRESS_MAX_PARALLEL_TABLES', 4))
STANDARD_MAX_WORKERS = int(os.environ.get('STANDARD_MAX_PARALLEL_TABLES', 8))
BATCH_CPU            = int(os.environ.get('BATCH_CPU',                   16))
BATCH_MEMORY         = int(os.environ.get('BATCH_MEMORY',              65536))  # 64 GiB
BATCH_MAX_WORKERS    = int(os.environ.get('BATCH_MAX_PARALLEL_TABLES',   16))

# Task definitions to update (must match SFN definition references)
TASK_DEFS = [
    'ss-cdkdev-sde-express',
    'ss-cdkdev-sde-standard',
]

# Env var renames to apply when cloning task definitions.
# Format: (old_name, new_name)  — value is carried over from the old entry.
# Add (old_name, None) to delete an env var without replacement.
ENV_VAR_RENAMES = [
    ('OKTA_URL', 'IDENTITY_PROVIDER_TOKEN_ENDPOINT'),
]

# CPU/Memory overrides — driven by env vars above (Fargate valid combos: 4096→30720 MiB, 8192→61440 MiB).
TASK_DEF_RESOURCE_OVERRIDES = {
    'ss-cdkdev-sde-express':  {'cpu': str(EXPRESS_CPU),  'memory': str(EXPRESS_MEMORY)},
    'ss-cdkdev-sde-standard': {'cpu': str(STANDARD_CPU), 'memory': str(STANDARD_MEMORY)},
}

# Per-task-def env var overrides (applied on top of ENV_VAR_OVERRIDES).
# Used for values that differ between express and standard containers.
TASK_DEF_ENV_OVERRIDES = {
    'ss-cdkdev-sde-express':  {'MAX_WORKERS': str(EXPRESS_MAX_WORKERS)},
    'ss-cdkdev-sde-standard': {'MAX_WORKERS': str(STANDARD_MAX_WORKERS)},
}

# Env var values to explicitly set (overrides whatever the cloned task def had).
# Use this when the correct value is known at deploy time (e.g. CDK var replacements).
ENV_VAR_OVERRIDES = {
    # Full token URL — OKTA_URL only stored the base domain, not the full path
    'IDENTITY_PROVIDER_TOKEN_ENDPOINT': 'https://sso-dev-na.synergystudio.verisk.com/oauth2/aus281t8zz5gF8z3u0h8/v1/token',
    # SID API base URL
    'SID_API_URL': 'https://dev-na.synergystudio.verisk.com/api/sid/v1',
}


def _migrate_env_vars(container_defs: list) -> list:
    """
    Apply ENV_VAR_RENAMES and ENV_VAR_OVERRIDES to every container definition's
    environment list.
    - ENV_VAR_RENAMES: remove old key, insert new key with old value
    - ENV_VAR_OVERRIDES: set the key to an explicit value
    """
    for container in container_defs:
        env = {e['name']: e['value'] for e in container.get('environment', [])}
        for old_name, new_name in ENV_VAR_RENAMES:
            if old_name in env:
                value = env.pop(old_name)
                if new_name:
                    env[new_name] = value
                print(f'    Env rename: {old_name} -> {new_name}')
        for var_name, var_value in ENV_VAR_OVERRIDES.items():
            env[var_name] = var_value
            print(f'    Env override: {var_name} = {var_value[:60]}...' if len(var_value) > 60 else f'    Env override: {var_name} = {var_value}')
        container['environment'] = [{'name': k, 'value': v} for k, v in env.items()]
    return container_defs


# Fields that must be stripped before re-registering a task definition
STRIP_FIELDS = [
    'taskDefinitionArn', 'revision', 'status', 'registeredAt',
    'registeredBy', 'compatibilities', 'requiresAttributes',
]


def _get_latest_revision(ecs, td_name: str) -> int:
    """Return the latest ACTIVE revision number for a task definition family."""
    resp = ecs.describe_task_definition(taskDefinition=td_name)
    return resp['taskDefinition']['revision']


def patch_ecr_image(image_name: str) -> None:
    """
    Patch an existing ECR image in-place:
      1. Pull latest image from ECR
      2. Start a container (sleep entrypoint so it stays alive)
      3. Copy requirements.txt → run pip install (installs new packages)
      4. Copy all patched source files
      5. Commit + push
    This avoids a full Dockerfile rebuild (which requires deadsnakes PPA / external
    network access that may be blocked by corporate TLS proxy).
    """
    import subprocess
    image_uri = f'{ECR_BASE}/{image_name}:latest'
    container = f'tmp-patch-{image_name.replace("/", "-")}'

    print(f'  Patching {image_uri} ...')

    def run(cmd):
        subprocess.run(cmd, shell=True, check=True)

    # ECR login
    run(
        f'aws ecr get-login-password --region {REGION} | '
        f'podman login --tls-verify=false --username AWS --password-stdin {ECR_BASE}'
    )

    run(f'podman pull --tls-verify=false {image_uri}')
    # Use sleep as entrypoint so the container stays alive for exec commands
    run(f'podman run -d --name {container} --entrypoint sleep {image_uri} infinity')
    try:
        # Step 1: copy requirements.txt and run pip install to pick up new packages
        run(f'podman cp requirements.txt {container}:/app/requirements.txt')
        run(
            f'podman exec {container} python3.12 -m pip install '
            f'--no-cache-dir -q -r /app/requirements.txt'
        )
        print('    pip install complete')

        # Step 2: copy patched source files
        for local, remote in PATCH_FILES:
            run(f'podman cp {local} {container}:{remote}')
            print(f'    Copied {local}')

        # Restore original entrypoint (overridden by --entrypoint sleep during patching)
        # Use a list to avoid Windows shell quoting issues with --change flags
        subprocess.run(
            ['podman', 'commit',
             '--change', 'ENTRYPOINT ["/entrypoint.sh"]',
             '--change', 'CMD []',
             container, image_uri],
            check=True
        )
        run(f'podman push --tls-verify=false {image_uri}')
    finally:
        subprocess.run(f'podman rm -f {container}', shell=True)

    print(f'  OK: {image_uri} patched and pushed')


def update_lambda() -> None:
    """Repackage and upload size_estimator Lambda code."""
    lmb = boto3.client('lambda', region_name=REGION)
    print(f'Updating Lambda {LAMBDA_NAME} ...')
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write('cdk/src/EES.AP.SDE.CDK/lambda/size_estimator.py', 'size_estimator.py')
    buf.seek(0)
    resp = lmb.update_function_code(
        FunctionName=LAMBDA_NAME,
        ZipFile=buf.read(),
        Publish=True,
    )
    print(f'  Lambda version {resp["Version"]} state={resp["State"]}')
    # Wait for the code update to finish before changing configuration
    print('  Waiting for Lambda to stabilize...')
    lmb.get_waiter('function_updated').wait(FunctionName=LAMBDA_NAME)
    # Update function configuration: timeout + calibrated expansion factor env var
    current_env = lmb.get_function_configuration(FunctionName=LAMBDA_NAME).get('Environment', {}).get('Variables', {})
    current_env['PARQUET_TO_MDF_EXPANSION_FACTOR'] = '28'  # Calibrated from act-003: exact=27.5x, 28x for slight overestimate
    current_env['THRESHOLD_SMALL_GB'] = str(THRESHOLD_SMALL_GB)
    current_env['THRESHOLD_LARGE_GB'] = str(THRESHOLD_LARGE_GB)
    lmb.update_function_configuration(
        FunctionName=LAMBDA_NAME,
        Timeout=900,  # 15 min: allows up to 300s per Athena query across multiple slow tables
        Environment={'Variables': current_env},
    )
    print(f'  Lambda timeout=900s, EXPANSION=28, THRESHOLD_SMALL_GB={THRESHOLD_SMALL_GB}, THRESHOLD_LARGE_GB={THRESHOLD_LARGE_GB}')


def print_config_summary():
    """Print effective configuration so it is visible in deploy logs."""
    print('\n=== Effective Configuration ===')
    print(f'  Thresholds:  Express<={THRESHOLD_SMALL_GB}GB / Standard<={THRESHOLD_LARGE_GB}GB / Batch>{THRESHOLD_LARGE_GB}GB')
    print(f'  Express:     {EXPRESS_CPU} CPU / {EXPRESS_MEMORY} MiB / {EXPRESS_MAX_WORKERS} workers')
    print(f'  Standard:    {STANDARD_CPU} CPU / {STANDARD_MEMORY} MiB / {STANDARD_MAX_WORKERS} workers')
    print(f'  Batch:       {BATCH_CPU} vCPU / {BATCH_MEMORY} MiB / {BATCH_MAX_WORKERS} workers  (CDK-managed, shown for reference)')


def register_task_defs(ecs) -> dict:
    """Clone latest task-def revisions -> return {name: new_revision}."""
    new_revisions = {}
    for td_name in TASK_DEFS:
        latest_rev = _get_latest_revision(ecs, td_name)
        src = ecs.describe_task_definition(taskDefinition=f'{td_name}:{latest_rev}')['taskDefinition']
        for field in STRIP_FIELDS:
            src.pop(field, None)
        # Migrate any renamed env vars in all containers
        src['containerDefinitions'] = _migrate_env_vars(src.get('containerDefinitions', []))
        # Apply CPU/memory overrides if specified
        if td_name in TASK_DEF_RESOURCE_OVERRIDES:
            overrides = TASK_DEF_RESOURCE_OVERRIDES[td_name]
            if 'cpu' in overrides:
                src['cpu'] = overrides['cpu']
                print(f'    CPU override: {overrides["cpu"]} ({int(overrides["cpu"]) // 1024} vCPU)')
            if 'memory' in overrides:
                src['memory'] = overrides['memory']
                print(f'    Memory override: {overrides["memory"]} MiB ({int(overrides["memory"]) // 1024} GiB)')
        # Apply per-task-def env var overrides (e.g. MAX_WORKERS derived from CPU)
        if td_name in TASK_DEF_ENV_OVERRIDES:
            for container in src.get('containerDefinitions', []):
                env = {e['name']: e['value'] for e in container.get('environment', [])}
                for var_name, var_value in TASK_DEF_ENV_OVERRIDES[td_name].items():
                    env[var_name] = var_value
                    print(f'    Env set: {var_name} = {var_value}')
                container['environment'] = [{'name': k, 'value': v} for k, v in env.items()]
        new_td = ecs.register_task_definition(**src)['taskDefinition']
        rev = new_td['revision']
        new_revisions[td_name] = rev
        print(f'  Registered {td_name}:{rev} (from :{latest_rev})')
    return new_revisions


def update_state_machine(sfn, new_revisions: dict) -> None:
    """Patch SFN definition:
    1. Point task-def references at new revisions.
    2. Inject CheckRecordCount early-exit after EstimateExtractionSize if not already present.
    """
    import re, json as _json
    resp = sfn.describe_state_machine(stateMachineArn=SFN_ARN)
    definition = resp['definition']

    # --- 1. Update task-definition revision numbers ---
    for td_name, new_rev in new_revisions.items():
        definition = re.sub(
            rf'{re.escape(td_name)}:\d+',
            f'{td_name}:{new_rev}',
            definition
        )
        print(f'  SFN: {td_name} -> :{new_rev}')

    # --- 2. Inject early-exit Choice state after the size estimator ---
    defn = _json.loads(definition)
    states = defn.get('States', {})

    if 'CheckRecordCount' not in states:
        print('  SFN: injecting CheckRecordCount early-exit state')

        # Wire EstimateExtractionSize -> CheckRecordCount (was -> DetermineExtractionPath)
        if 'EstimateExtractionSize' in states:
            states['EstimateExtractionSize']['Next'] = 'CheckRecordCount'

        # Add the Choice state
        states['CheckRecordCount'] = {
            'Type': 'Choice',
            'Comment': 'Exit immediately when the size estimator found zero records — no ECS/Batch task needed.',
            'Choices': [
                {
                    'Variable': '$.total_record_count',
                    'NumericEquals': 0,
                    'Next': 'NoRecordsFound',
                }
            ],
            'Default': 'DetermineExtractionPath',
        }

        # Add the Succeed state
        states['NoRecordsFound'] = {
            'Type': 'Succeed',
            'Comment': 'No Iceberg records matched the requested exposure IDs — nothing to extract.',
        }

        defn['States'] = states
        definition = _json.dumps(defn)
    else:
        print('  SFN: CheckRecordCount already present, skipping injection')

    sfn.update_state_machine(
        stateMachineArn=SFN_ARN,
        definition=definition,
        roleArn=resp['roleArn'],
    )


def update_ingestion_state_machine(sfn, new_revisions: dict) -> None:
    """Patch ingestion SFN definition: update task-definition revision numbers."""
    import re, json as _json
    resp = sfn.describe_state_machine(stateMachineArn=INGESTION_SFN_ARN)
    definition = resp['definition']

    for td_name, new_rev in new_revisions.items():
        definition = re.sub(
            rf'{re.escape(td_name)}:\d+',
            f'{td_name}:{new_rev}',
            definition
        )
        print(f'  Ingestion SFN: {td_name} -> :{new_rev}')

    sfn.update_state_machine(
        stateMachineArn=INGESTION_SFN_ARN,
        definition=definition,
        roleArn=resp['roleArn'],
    )


def main():
    ecs = boto3.client('ecs', region_name=REGION)
    sfn = boto3.client('stepfunctions', region_name=REGION)

    print_config_summary()

    # ---------------------------------------------------------------
    # 1. Patch ECR images (pip install new packages + copy source files)
    # ---------------------------------------------------------------
    print('\n=== Patching ECR images ===')
    for image_name in TASK_DEFS:
        patch_ecr_image(image_name)

    # ---------------------------------------------------------------
    # 2. Update Lambda
    # ---------------------------------------------------------------
    print('\n=== Updating Lambda ===')
    update_lambda()

    # ---------------------------------------------------------------
    # 3. Register new task-def revisions
    # ---------------------------------------------------------------
    print('\n=== Registering task definitions ===')
    new_revisions = register_task_defs(ecs)

    # ---------------------------------------------------------------
    # 4. Update state machines (extraction + ingestion)
    # ---------------------------------------------------------------
    print('\n=== Updating State Machines ===')
    update_state_machine(sfn, new_revisions)
    update_ingestion_state_machine(sfn, new_revisions)

    print('\nDeploy complete.')


if __name__ == '__main__':
    main()

