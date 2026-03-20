"""
Patches existing ECR images to install ees-ap-otel into the Python environment.
Uses podman/docker to pull, exec pip install inside, commit, and push.
Usage: python scripts/patch_images_otel.py [--dry-run]
"""
import glob
import subprocess
import sys
import os
import tempfile

REGION = "us-east-1"
ACCOUNT_ID = subprocess.check_output(
    ["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"],
    text=True
).strip()
ENV = "dev"
PREFIX = f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/ss-cdk{ENV}"
SUFFIXES = ["express", "standard"]
TAG = "latest"

PIP_FLAGS = [
    "--trusted-host", "pypi.org",
    "--trusted-host", "files.pythonhosted.org",
    "--trusted-host", "pypi.python.org",
]

DRY_RUN = "--dry-run" in sys.argv

# Find the locally-built wheel (avoids needing git inside the container)
WHEEL_DIR = os.path.join(tempfile.gettempdir(), "otel-wheels-v2")
_wheels = glob.glob(os.path.join(WHEEL_DIR, "ees_ap_otel-*.whl"))
if not _wheels:
    # Fall back to original dir
    WHEEL_DIR = os.path.join(tempfile.gettempdir(), "otel-wheels")
    _wheels = glob.glob(os.path.join(WHEEL_DIR, "ees_ap_otel-*.whl"))
if not _wheels:
    print(f"ERROR: No ees_ap_otel wheel found in {WHEEL_DIR}")
    print("Run: pip wheel 'ees-ap-otel @ git+...' --wheel-dir $env:TEMP\\otel-wheels --no-deps")
    sys.exit(1)
WHEEL_FILE = _wheels[0]
print(f"Using wheel: {WHEEL_FILE}")


def run(args, check=True):
    print(f"  $ {' '.join(args)}")
    if DRY_RUN:
        return
    result = subprocess.run(args, text=True, capture_output=False)
    if check and result.returncode != 0:
        print(f"FAILED (exit {result.returncode})", file=sys.stderr)
        sys.exit(result.returncode)


def main():
    print(f"Account: {ACCOUNT_ID}, Region: {REGION}, Env: {ENV}")
    if DRY_RUN:
        print("DRY RUN — no actual changes will be made\n")

    # ECR login
    print("\n=== ECR Login ===")
    ecr_pass = subprocess.check_output(
        ["aws", "ecr", "get-login-password", "--region", REGION], text=True
    ).strip()
    proc = subprocess.Popen(
        ["podman", "login", "--username", "AWS", "--password-stdin",
         "--tls-verify=false", f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com"],
        stdin=subprocess.PIPE, text=True
    )
    proc.communicate(input=ecr_pass)
    if proc.returncode != 0:
        print("ECR login failed", file=sys.stderr)
        sys.exit(1)
    print("Login OK")

    for suffix in SUFFIXES:
        image_uri = f"{PREFIX}-sde-{suffix}:{TAG}"
        container_name = f"tmp-otel-patch-{suffix}"
        print(f"\n=== Patching {suffix} ({image_uri}) ===")

        # Pull latest
        run(["podman", "pull", "--tls-verify=false", image_uri])

        # Always re-install to pick up latest wheel (force upgrade)
        wheel_name = os.path.basename(WHEEL_FILE)

        # Create container, copy wheel in, run pip install, commit
        run(["podman", "create", "--name", container_name, image_uri])
        run(["podman", "cp", WHEEL_FILE, f"{container_name}:/tmp/{wheel_name}"])

        # Start container and run pip install
        run(["podman", "start", container_name])
        run(["podman", "exec", container_name, "python3.12", "-m", "pip", "install",
             "--no-cache-dir", "--force-reinstall"] + PIP_FLAGS + [f"/tmp/{wheel_name}"])
        # Install auto-instrumentation extras (not bundled in the wheel itself)
        run(["podman", "exec", container_name, "python3.12", "-m", "pip", "install",
             "--no-cache-dir"] + PIP_FLAGS + [
             "opentelemetry-instrumentation-botocore",
             "opentelemetry-instrumentation-requests",
        ])
        run(["podman", "stop", container_name])

        # Commit as new image
        run(["podman", "commit", container_name, image_uri])

        # Cleanup container
        run(["podman", "rm", container_name])

        # Push updated image
        run(["podman", "push", "--tls-verify=false", image_uri])

        print(f"  OK: {image_uri} updated with ees-ap-otel")

    print("\nAll images patched.")


if __name__ == "__main__":
    main()
