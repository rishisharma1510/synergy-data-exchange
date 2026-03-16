"""Check Glue table count and latest execution logs."""
import boto3
import time

EXEC_ARN = 'arn:aws:states:us-east-1:451952076009:execution:ss-cdkdev-sde-extraction:test-user-data-1772406877'
CLUSTER = 'ss-cdkdev-sde-cluster'
LOG_GROUP = '/ecs/ss-cdkdev-sde/express'
TASK_ID = '316f3cb975414b7bab4e324d5654136d'

sfn  = boto3.client('stepfunctions', region_name='us-east-1')
glue = boto3.client('glue',          region_name='us-east-1')
logs = boto3.client('logs',          region_name='us-east-1')
ecs  = boto3.client('ecs',           region_name='us-east-1')

# ── SFN + ECS current state ───────────────────────────────────────────
print('=== CURRENT STATE ===')
resp = sfn.describe_execution(executionArn=EXEC_ARN)
started = resp['startDate']
print('SFN Status:', resp['status'])
print('Started   :', started)
if 'stopDate' in resp:
    elapsed = (resp['stopDate'] - started).total_seconds()
    print('Stopped   :', resp['stopDate'], f'  elapsed={elapsed:.0f}s')
else:
    import datetime
    now = datetime.datetime.now(datetime.timezone.utc)
    elapsed = (now - started).total_seconds()
    print(f'Running for: {elapsed:.0f}s ({elapsed/60:.1f} min)')

t = ecs.describe_tasks(cluster=CLUSTER, tasks=[TASK_ID])
if t['tasks']:
    task = t['tasks'][0]
    print('ECS Status:', task['lastStatus'], '| stoppedReason:', task.get('stoppedReason',''))
    for c in task.get('containers', []):
        print(f'  container={c["name"]}  exitCode={c.get("exitCode","N/A")}  reason={c.get("reason","")}')

# ── Latest logs (last 30 lines since the catalog-init line) ──────────
print()
print('=== LATEST 30 LOG LINES ===')
log_stream = f'express/ss-cdkdev-sde-express/{TASK_ID}'
try:
    r = logs.get_log_events(
        logGroupName=LOG_GROUP,
        logStreamName=log_stream,
        startFromHead=False,
        limit=30,
    )
    for e in r['events']:
        print(e['message'])
except Exception as ex:
    print('Log error:', ex)

# ── Glue table count in the namespace (what list_tables actually does) ─
print()
print('=== GLUE NAMESPACE SCAN ===')
ns = 'rs_cdkdev_ssautoma03_exposure_db'
print(f'Counting tables in namespace: {ns}')
t0 = time.perf_counter()
paginator = glue.get_paginator('get_tables')
total = 0
pages = 0
table_names = []
for page in paginator.paginate(DatabaseName=ns):
    pages += 1
    count = len(page['TableList'])
    total += count
    for tbl in page['TableList']:
        table_names.append(tbl['Name'])
    print(f'  Page {pages}: {count} tables  (running total: {total})')
elapsed_glue = time.perf_counter() - t0
print(f'Done: {total} tables in {pages} page(s) — took {elapsed_glue:.2f}s')
print('Tables:')
for n in sorted(table_names):
    print(f'  {n}')
