"""
check_execution.py — Check the latest test-user-data SFN execution,
ECS task state, container logs, and Container Insights metrics.
"""
import boto3
import json
import datetime
import sys
_DEFAULT_EXEC = 'arn:aws:states:us-east-1:451952076009:execution:ss-cdkdev-sde-extraction:test-user-data-1772406877'
EXEC_ARN = sys.argv[1] if len(sys.argv) > 1 else _DEFAULT_EXEC
CLUSTER  = 'ss-cdkdev-sde-cluster'
LOG_GROUP = '/ecs/ss-cdkdev-sde/express'
CW_NAMESPACE = 'ECS/ContainerInsights'

sfn  = boto3.client('stepfunctions', region_name='us-east-1')
ecs  = boto3.client('ecs',           region_name='us-east-1')
logs = boto3.client('logs',          region_name='us-east-1')
cw   = boto3.client('cloudwatch',    region_name='us-east-1')

# ── SFN ──────────────────────────────────────────────────────────────
print('=' * 60)
print('SFN EXECUTION')
print('=' * 60)
resp = sfn.describe_execution(executionArn=EXEC_ARN)
status = resp['status']
print('Status  :', status)
print('Started :', resp['startDate'])
if 'stopDate' in resp:
    elapsed = (resp['stopDate'] - resp['startDate']).total_seconds()
    print('Stopped :', resp['stopDate'], f'  (elapsed {elapsed:.0f}s)')
if status in ('FAILED', 'TIMED_OUT', 'ABORTED'):
    print('Error   :', resp.get('error', ''))
    print('Cause   :', resp.get('cause', '')[:500])

# ── Get ECS task ID from SFN history ─────────────────────────────────
history = sfn.get_execution_history(executionArn=EXEC_ARN, maxResults=50, reverseOrder=False)
task_id = None
task_arn_full = None
for ev in history['events']:
    if ev['type'] == 'TaskSubmitted':
        out = json.loads(ev.get('taskSubmittedEventDetails', {}).get('output', '{}'))
        tasks = out.get('Tasks', [])
        if tasks:
            task_arn_full = tasks[0].get('TaskArn', '')
            task_id = task_arn_full.split('/')[-1]
            break

if not task_id:
    print()
    print('No ECS task submitted yet. SFN history:')
    for ev in history['events']:
        ts = ev['timestamp'].strftime('%H:%M:%S')
        print(f'  {ts}  {ev["type"]}')
    raise SystemExit(0)

# ── ECS Task ──────────────────────────────────────────────────────────
print()
print('=' * 60)
print('ECS TASK:', task_id)
print('=' * 60)
t_resp = ecs.describe_tasks(cluster=CLUSTER, tasks=[task_id])
if t_resp['tasks']:
    task = t_resp['tasks'][0]
    print('lastStatus   :', task['lastStatus'])
    print('desiredStatus:', task['desiredStatus'])
    print('taskDef      :', task['taskDefinitionArn'].split('/')[-1])
    print('startedAt    :', task.get('startedAt', 'not yet'))
    print('stoppedAt    :', task.get('stoppedAt', 'still running'))
    print('stoppedReason:', task.get('stoppedReason', ''))
    ov = task.get('overrides', {})
    print('taskRoleArn  :', ov.get('taskRoleArn', 'NOT SET (using task def default)'))
    print('cpu          :', task.get('cpu', ''))
    print('memory       :', task.get('memory', ''))
    for c in task.get('containers', []):
        cname = c['name']
        cstatus = c.get('lastStatus', '')
        cexit = c.get('exitCode', 'N/A')
        creason = c.get('reason', '')
        print(f'  container={cname}  status={cstatus}  exitCode={cexit}  reason={creason}')
else:
    print('Task not found in ECS API (already stopped/expired)')

# ── Container Insights ───────────────────────────────────────────────
print()
print('=' * 60)
print('CONTAINER INSIGHTS (last 30 min)')
print('=' * 60)
now = datetime.datetime.utcnow()
start = now - datetime.timedelta(minutes=30)

for metric in ['CpuUtilized', 'MemoryUtilized', 'NetworkRxBytes', 'NetworkTxBytes']:
    try:
        r = cw.get_metric_statistics(
            Namespace=CW_NAMESPACE,
            MetricName=metric,
            Dimensions=[
                {'Name': 'ClusterName', 'Value': CLUSTER},
                {'Name': 'TaskId',      'Value': task_id},
            ],
            StartTime=start,
            EndTime=now,
            Period=60,
            Statistics=['Average', 'Maximum'],
        )
        pts = sorted(r['Datapoints'], key=lambda x: x['Timestamp'])
        if pts:
            latest = pts[-1]
            print(f'  {metric:25s} avg={latest.get("Average",0):.1f}  max={latest.get("Maximum",0):.1f}  unit={latest.get("Unit","")}')
        else:
            print(f'  {metric:25s} no data points')
    except Exception as ex:
        print(f'  {metric:25s} error: {ex}')

# ── Container Logs ───────────────────────────────────────────────────
print()
print('=' * 60)
print('CONTAINER LOGS (all lines)')
print('=' * 60)
log_stream = f'express/ss-cdkdev-sde-express/{task_id}'
try:
    token = None
    total = 0
    while True:
        kwargs = dict(
            logGroupName=LOG_GROUP,
            logStreamName=log_stream,
            startFromHead=True,
            limit=100,
        )
        if token:
            kwargs['nextToken'] = token
        r = logs.get_log_events(**kwargs)
        for e in r['events']:
            print(e['message'])
            total += 1
        new_token = r.get('nextForwardToken')
        if new_token == token or not r['events']:
            break
        token = new_token
    print()
    print(f'--- Total log lines: {total} ---')
except Exception as ex:
    print('Log stream error:', ex)
    # Try listing available streams
    try:
        streams = logs.describe_log_streams(
            logGroupName=LOG_GROUP,
            orderBy='LastEventTime',
            descending=True,
            limit=5,
        )
        print('Available streams:')
        for s in streams['logStreams']:
            print(' ', s['logStreamName'], '| lastEvent:', s.get('lastEventTimestamp', ''))
    except Exception as ex2:
        print('Cannot list streams:', ex2)
