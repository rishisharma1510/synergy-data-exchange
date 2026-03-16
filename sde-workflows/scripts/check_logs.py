import boto3
import datetime

sfn = boto3.client('stepfunctions', region_name='us-east-1')
logs = boto3.client('logs', region_name='us-east-1')

exec_arn = 'arn:aws:states:us-east-1:451952076009:execution:ss-cdkdev-sde-extraction:test-fixed-1772405379'
resp = sfn.describe_execution(executionArn=exec_arn)
print('SFN Status:', resp['status'])
if resp['status'] in ('FAILED', 'TIMED_OUT', 'ABORTED'):
    print('Error:', resp.get('error', ''))
    print('Cause:', resp.get('cause', ''))

start_ts = int(datetime.datetime(2026, 3, 1, 22, 56, 0, tzinfo=datetime.timezone.utc).timestamp() * 1000)
log_stream = 'express/ss-cdkdev-sde-express/161b3b2e9677482299b4c164fa60c066'
r = logs.get_log_events(
    logGroupName='/ecs/ss-cdkdev-sde/express',
    logStreamName=log_stream,
    startFromHead=True,
    startTime=start_ts,
    limit=100,
)
events = r['events']
print(f'Events from 22:56+: {len(events)}')
print()
for e in events:
    ts = datetime.datetime.fromtimestamp(e['timestamp'] / 1000, tz=datetime.timezone.utc).strftime('%H:%M:%S')
    print(f'[{ts}]', e['message'])
