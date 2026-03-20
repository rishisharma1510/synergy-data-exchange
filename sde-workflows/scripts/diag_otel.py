import subprocess, json, time, sys

REGION = "us-east-1"
EXEC_ARN = "arn:aws:states:us-east-1:451952076009:execution:ss-cdkdev-sde-extraction:diag-otel-001"

def aws(*args):
    return json.loads(subprocess.check_output(["aws"] + list(args) + ["--region", REGION, "--output", "json"], text=True))

# 1. Poll execution status
d = aws("stepfunctions", "describe-execution", "--execution-arn", EXEC_ARN)
print(f"Execution status: {d['status']}")

# 2. Find ECS cluster
clusters = [c for c in aws("ecs", "list-clusters")["clusterArns"] if "sde" in c]
print(f"Clusters: {clusters}")

# 3. Find running tasks
for cluster in clusters:
    task_arns = aws("ecs", "list-tasks", "--cluster", cluster)["taskArns"]
    stopped_arns = aws("ecs", "list-tasks", "--cluster", cluster, "--desired-status", "STOPPED")["taskArns"]
    all_arns = task_arns + stopped_arns[:5]  # recent stopped too
    if not all_arns:
        print(f"  No tasks in {cluster}")
        continue
    desc = aws("ecs", "describe-tasks", "--cluster", cluster, "--tasks", *all_arns)
    for t in desc["tasks"]:
        td_arn = t.get("taskDefinitionArn", "")
        td_short = td_arn.split("/")[-1]
        containers = [(c["name"], c.get("lastStatus","?")) for c in t.get("containers", [])]
        print(f"\n  Task: {t['taskArn'].split('/')[-1]}")
        print(f"  TaskDef: {td_short}")
        print(f"  Status: {t['lastStatus']}")
        print(f"  Containers: {containers}")
        stopped_reason = t.get("stoppedReason", "")
        if stopped_reason:
            print(f"  StoppedReason: {stopped_reason}")

# 4. Check ADOT log group for recent logs
print("\n=== ADOT sidecar logs (/ecs/ss-cdkdev-sde/adot) ===")
try:
    streams = aws("logs", "describe-log-streams",
                  "--log-group-name", "/ecs/ss-cdkdev-sde/adot",
                  "--order-by", "LastEventTime", "--descending", "--max-items", "3")
    for s in streams.get("logStreams", []):
        stream_name = s["logStreamName"]
        events = aws("logs", "get-log-events",
                     "--log-group-name", "/ecs/ss-cdkdev-sde/adot",
                     "--log-stream-name", stream_name,
                     "--limit", "20", "--start-from-head")
        print(f"\n  Stream: {stream_name}")
        for e in events.get("events", []):
            print(f"    {e['message'][:200]}")
except Exception as ex:
    print(f"  Error reading ADOT logs: {ex}")

# 5. Check express app logs for OTEL init
print("\n=== Express app logs (last 30 lines) ===")
try:
    streams = aws("logs", "describe-log-streams",
                  "--log-group-name", "/ecs/ss-cdkdev-sde/express",
                  "--order-by", "LastEventTime", "--descending", "--max-items", "1")
    for s in streams.get("logStreams", []):
        events = aws("logs", "get-log-events",
                     "--log-group-name", "/ecs/ss-cdkdev-sde/express",
                     "--log-stream-name", s["logStreamName"],
                     "--limit", "30")
        print(f"  Stream: {s['logStreamName']}")
        for e in events.get("events", []):
            print(f"    {e['message'][:200]}")
except Exception as ex:
    print(f"  Error reading express logs: {ex}")
