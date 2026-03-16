import boto3, time, sys

sfn = boto3.client('stepfunctions', region_name='us-east-1')
arn = sys.argv[1] if len(sys.argv) > 1 else 'arn:aws:states:us-east-1:451952076009:execution:ss-cdkdev-sde-ingestion:8e1c8dac-f400-4710-b42a-4de85919c0e2'

for i in range(80):
    r = sfn.describe_execution(executionArn=arn)
    s = r['status']
    print(f'[{i*15}s] {s}', flush=True)
    if s != 'RUNNING':
        print('stop:', r.get('stopDate', ''))
        print('error:', r.get('error', ''))
        print('cause:', r.get('cause', '')[:2000])
        break
    time.sleep(15)
