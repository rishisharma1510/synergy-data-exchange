"""
One-time script: patch the extraction SFN to support S3outputpath.

Changes:
- ValidateExtractionInput: remove tenant_bucket, pass S3outputpath through
- RunExpressExtractionTask / RunStandardExtractionTask: add S3_OUTPUT_PATH env var for final MDF/BAK output
"""
import boto3
import json

SFN_ARN = 'arn:aws:states:us-east-1:451952076009:stateMachine:ss-cdkdev-sde-extraction'

sfn = boto3.client('stepfunctions', region_name='us-east-1')
resp = sfn.describe_state_machine(stateMachineArn=SFN_ARN)
defn = json.loads(resp['definition'])

# -----------------------------------------------------------------------
# 1. ValidateExtractionInput: remove tenant_bucket, pass S3outputpath through
# -----------------------------------------------------------------------
params = defn['States']['ValidateExtractionInput']['Parameters']
params.pop('tenant_bucket.$', None)
params['s3_output_path.$'] = '$.S3outputpath'
print('ValidateExtractionInput params:', json.dumps(params, indent=2))

# -----------------------------------------------------------------------
# 2. Add S3_OUTPUT_PATH to ECS container overrides (Express + Standard)
#    This is the final destination for MDF/BAK files
# -----------------------------------------------------------------------
for state_name in ['RunExpressExtractionTask', 'RunStandardExtractionTask']:
    envs = defn['States'][state_name]['Parameters']['Overrides']['ContainerOverrides'][0]['Environment']
    # Remove any stale entry first
    envs = [e for e in envs if e.get('Name') not in ('ARTIFACT_OUTPUT_PATH', 'S3_OUTPUT_PATH')]
    envs.append({'Name': 'S3_OUTPUT_PATH', 'Value.$': '$.s3_output_path'})
    defn['States'][state_name]['Parameters']['Overrides']['ContainerOverrides'][0]['Environment'] = envs
    print(f'{state_name}: added S3_OUTPUT_PATH')

# -----------------------------------------------------------------------
# 3. Apply
# -----------------------------------------------------------------------
sfn.update_state_machine(
    stateMachineArn=SFN_ARN,
    definition=json.dumps(defn),
    roleArn=resp['roleArn'],
)
print('\nSFN updated OK')

# Verify
v = json.loads(sfn.describe_state_machine(stateMachineArn=SFN_ARN)['definition'])
print('\nValidateExtractionInput final params:')
print(json.dumps(v['States']['ValidateExtractionInput']['Parameters'], indent=2))
express_envs = v['States']['RunExpressExtractionTask']['Parameters']['Overrides']['ContainerOverrides'][0]['Environment']
print('\nExpress S3_OUTPUT_PATH entry:', [e for e in express_envs if 'S3_OUTPUT' in e.get('Name', '')])
