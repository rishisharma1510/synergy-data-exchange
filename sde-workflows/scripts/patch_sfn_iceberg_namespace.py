"""
Patch the state machine to add ICEBERG_NAMESPACE env var to container overrides.
This ensures the Fargate container knows which Glue database to use for PyIceberg.
"""
import boto3
import json

sfn = boto3.client('stepfunctions', region_name='us-east-1')
SM_ARN = 'arn:aws:states:us-east-1:451952076009:stateMachine:ss-cdkdev-sde-extraction'

sm = sfn.describe_state_machine(stateMachineArn=SM_ARN)
defn = json.loads(sm['definition'])


def patch_container_overrides(obj):
    """Recursively find ECS RunTask states and add ICEBERG_NAMESPACE to env."""
    if isinstance(obj, dict):
        if obj.get('Type') == 'Task' and 'Parameters' in obj:
            params = obj['Parameters']
            overrides = params.get('Overrides', {})
            for co in overrides.get('ContainerOverrides', []):
                env = co.get('Environment', [])
                names = [e['Name'] for e in env]
                if 'GLUE_DATABASE' in names and 'ICEBERG_NAMESPACE' not in names:
                    env.append({
                        'Name': 'ICEBERG_NAMESPACE',
                        'Value.$': '$.sde_context.glue_database'
                    })
                    print(f"  Added ICEBERG_NAMESPACE to container: {co.get('Name')}")
        for v in obj.values():
            patch_container_overrides(v)
    elif isinstance(obj, list):
        for item in obj:
            patch_container_overrides(item)


patch_container_overrides(defn)

new_defn = json.dumps(defn)

# Verify
count = new_defn.count('ICEBERG_NAMESPACE')
print(f'ICEBERG_NAMESPACE appears {count} time(s) in updated definition')

resp = sfn.update_state_machine(
    stateMachineArn=SM_ARN,
    definition=new_defn
)
print('HTTP status:', resp['ResponseMetadata']['HTTPStatusCode'])
print('Updated at:', resp['updateDate'])
