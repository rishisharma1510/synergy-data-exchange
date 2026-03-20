"""
Registers a new ECS task definition revision with the ADOT sidecar container added.
Usage: python scripts/add_adot_sidecar.py [family]
Default family: ss-cdkdev-sde-express
"""
import json
import subprocess
import sys

REGION = "us-east-1"
STAGE = "dev"
FAMILY = sys.argv[1] if len(sys.argv) > 1 else "ss-cdkdev-sde-express"

READ_ONLY_FIELDS = [
    "taskDefinitionArn", "revision", "status", "requiresAttributes",
    "placementConstraints", "compatibilities", "registeredAt", "registeredBy",
]

# Dynatrace ADOT collector config — forwarded to Dynatrace via OTLP.
# ${env:DT_ENDPOINT}, ${env:DT_API_TOKEN}, ${env:ENVIRONMENT} are substituted by ADOT at startup.
ADOT_COLLECTOR_CONFIG = """
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 256
    spike_limit_mib: 64
  batch:
    timeout: 5s
    send_batch_size: 1000
  resourcedetection:
    detectors: [env, ecs, ec2]
    timeout: 5s
    override: false
  resource:
    attributes:
      - key: deployment.environment
        value: "${env:ENVIRONMENT}"
        action: upsert
      - key: organization
        value: "ees"
        action: upsert

exporters:
  otlphttp/dynatrace:
    endpoint: "${env:DT_ENDPOINT}"
    traces_endpoint: "${env:DT_ENDPOINT}/v1/traces"
    metrics_endpoint: "${env:DT_ENDPOINT}/v1/metrics"
    logs_endpoint: "${env:DT_ENDPOINT}/v1/logs"
    headers:
      Authorization: "Api-Token ${env:DT_API_TOKEN}"
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 300s
    sending_queue:
      enabled: true
      num_consumers: 4
      queue_size: 100

service:
  telemetry:
    logs:
      level: info
  pipelines:
    traces:
      receivers:  [otlp]
      processors: [memory_limiter, resourcedetection, resource, batch]
      exporters:  [otlphttp/dynatrace]
    metrics:
      receivers:  [otlp]
      processors: [memory_limiter, resourcedetection, resource, batch]
      exporters:  [otlphttp/dynatrace]
    logs:
      receivers:  [otlp]
      processors: [memory_limiter, resourcedetection, resource, batch]
      exporters:  [otlphttp/dynatrace]
""".strip()

DT_SECRET_ARN = f"arn:aws:secretsmanager:{REGION}:451952076009:secret:/ees/observability/{STAGE}/dynatrace/api_token-Gl9fex"

def get_dt_endpoint():
    out = subprocess.check_output(
        ["aws", "ssm", "get-parameter", "--name", f"/ees/observability/{STAGE}/dynatrace/endpoint",
         "--region", REGION, "--query", "Parameter.Value", "--output", "text"],
        text=True
    ).strip()
    return out


def build_adot_sidecar(dt_endpoint: str) -> dict:
    return {
        "name": "adot-sidecar",
        "image": "public.ecr.aws/aws-observability/aws-otel-collector:latest",
        "essential": False,
        "cpu": 64,
        "memoryReservation": 128,
        "command": ["--config", "env:AOT_CONFIG_CONTENT"],
        "environment": [
            {"name": "AOT_CONFIG_CONTENT", "value": ADOT_COLLECTOR_CONFIG},
            {"name": "DT_ENDPOINT", "value": dt_endpoint},
            {"name": "ENVIRONMENT", "value": STAGE},
        ],
        "secrets": [
            {"name": "DT_API_TOKEN", "valueFrom": DT_SECRET_ARN},
        ],
        "logConfiguration": {
            "logDriver": "awslogs",
            "options": {
                "awslogs-group": "/ecs/ss-cdkdev-sde/adot",
                "awslogs-region": REGION,
                "awslogs-stream-prefix": FAMILY,
            },
        },
    }


def run(args, capture=True):
    result = subprocess.run(args, capture_output=capture, text=True)
    if result.returncode != 0:
        print(f"ERROR running {' '.join(args[:4])}: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout if capture else None


def main():
    dt_endpoint = get_dt_endpoint()
    print(f"DT endpoint: {dt_endpoint}")
    adot_sidecar = build_adot_sidecar(dt_endpoint)

    print(f"Fetching task definition: {FAMILY}")
    raw = run(["aws", "ecs", "describe-task-definition",
               "--task-definition", FAMILY,
               "--region", REGION,
               "--output", "json"])
    td = json.loads(raw)["taskDefinition"]
    print(f"Current revision: {td['revision']}")
    print(f"Current containers: {[c['name'] for c in td['containerDefinitions']]}")

    if any(c["name"] == "adot-sidecar" for c in td["containerDefinitions"]):
        print("adot-sidecar already present — removing old definition to replace it.")
        td["containerDefinitions"] = [c for c in td["containerDefinitions"] if c["name"] != "adot-sidecar"]

    # ── Inject DT_API_TOKEN + DT_ENDPOINT into the app container ─────────────
    # This lets the Python SDK send directly to Dynatrace, bypassing the ADOT
    # sidecar race condition (sidecar takes ~8s to start, task may finish in ~10s).
    for container in td["containerDefinitions"]:
        if container.get("essential") is True:
            env_list = container.setdefault("environment", [])
            env_names = {e["name"] for e in env_list}
            if "DT_ENDPOINT" not in env_names:
                env_list.append({"name": "DT_ENDPOINT", "value": dt_endpoint})
                print(f"  Added DT_ENDPOINT to {container['name']}")
            secrets_list = container.setdefault("secrets", [])
            secret_names = {s["name"] for s in secrets_list}
            if "DT_API_TOKEN" not in secret_names:
                secrets_list.append({"name": "DT_API_TOKEN", "valueFrom": DT_SECRET_ARN})
                print(f"  Added DT_API_TOKEN secret to {container['name']}")

    for field in READ_ONLY_FIELDS:
        td.pop(field, None)

    td["containerDefinitions"].append(adot_sidecar)

    tmp_file = f"/tmp/adot-td-{FAMILY}.json"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(td, f, indent=2)
    print(f"Clean JSON written to: {tmp_file}")

    print("Registering new revision...")
    out = run(["aws", "ecs", "register-task-definition",
               "--cli-input-json", f"file://{tmp_file}",
               "--region", REGION,
               "--output", "json"])
    result = json.loads(out)
    new_td = result["taskDefinition"]
    names = [c["name"] for c in new_td["containerDefinitions"]]
    print(f"SUCCESS: {new_td['family']} revision {new_td['revision']} registered")
    print(f"Containers: {', '.join(names)}")


if __name__ == "__main__":
    main()
