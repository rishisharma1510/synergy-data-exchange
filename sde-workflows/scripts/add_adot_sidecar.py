"""
Registers a new ECS task definition revision with the ADOT sidecar container added.
Usage: python scripts/add_adot_sidecar.py [family]
Default family: ss-cdkdev-sde-express
"""
import json
import subprocess
import sys

REGION = "us-east-1"
FAMILY = sys.argv[1] if len(sys.argv) > 1 else "ss-cdkdev-sde-express"

READ_ONLY_FIELDS = [
    "taskDefinitionArn", "revision", "status", "requiresAttributes",
    "placementConstraints", "compatibilities", "registeredAt", "registeredBy",
]

ADOT_SIDECAR = {
    "name": "adot-sidecar",
    "image": "public.ecr.aws/aws-observability/aws-otel-collector:latest",
    "essential": False,
    "cpu": 256,
    "memory": 512,
    "command": ["--config", "/etc/ecs/ecs-default-config.yaml"],
    "environment": [
        {"name": "OTEL_SERVICE_NAME", "value": FAMILY}
    ],
    "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
            "awslogs-group": "/ecs/ss-cdkdev-sde/adot",
            "awslogs-region": REGION,
            "awslogs-stream-prefix": FAMILY,
            "awslogs-create-group": "true",
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
    print(f"Fetching task definition: {FAMILY}")
    raw = run(["aws", "ecs", "describe-task-definition",
               "--task-definition", FAMILY,
               "--region", REGION,
               "--output", "json"])
    td = json.loads(raw)["taskDefinition"]
    print(f"Current revision: {td['revision']}")
    print(f"Current containers: {[c['name'] for c in td['containerDefinitions']]}")

    if any(c["name"] == "adot-sidecar" for c in td["containerDefinitions"]):
        print("adot-sidecar already present. Nothing to do.")
        return

    for field in READ_ONLY_FIELDS:
        td.pop(field, None)

    td["containerDefinitions"].append(ADOT_SIDECAR)

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
