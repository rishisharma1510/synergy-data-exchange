#!/bin/bash
# =============================================================================
# Build and Push Docker Images for All Paths
# =============================================================================
# Usage:
#   ./docker/build.sh                    # Build all images locally
#   ./docker/build.sh --push             # Build and push to ECR
#   ./docker/build.sh --patch --push     # Patch src files in-place, push (no OS rebuild)
#   ./docker/build.sh --path express     # Build only express image
#   ./docker/build.sh --tag v1.2.3       # Use specific tag
#
# NOTE: Auto-detects podman (local) or docker (CI). Full builds may fail behind a
#       TLS-intercepting proxy (apt-get SSL error). Use --patch in that environment.
# =============================================================================

set -e

# Configuration
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo '')}"
STAGE="${STAGE:-dev}"
# ECR repo names: ss-cdk{stage}-sde-{type}
ECR_REPO_PREFIX="ss-cdk${STAGE}-Synergy Data Exchange"
TAG="${TAG:-latest}"
PUSH=false
PATCH=false
BUILD_PATH="all"

# Auto-detect container runtime (podman locally, docker in CI)
if command -v podman &>/dev/null; then
    CONTAINER_TOOL="podman"
    TLS_FLAG="--tls-verify=false"
else
    CONTAINER_TOOL="docker"
    TLS_FLAG=""
fi

# Source files to inject in patch mode (local:container)
PATCH_FILES=(
    "src/fargate/entrypoint.py:/app/src/fargate/entrypoint.py"
    "src/core/config.py:/app/src/core/config.py"
)

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --push)    PUSH=true; shift ;;
        --patch)   PATCH=true; shift ;;
        --path)    BUILD_PATH="$2"; shift 2 ;;
        --tag)     TAG="$2"; shift 2 ;;
        --region)  AWS_REGION="$2"; shift 2 ;;
        --account) AWS_ACCOUNT_ID="$2"; shift 2 ;;
        --stage)   STAGE="$2"; ECR_REPO_PREFIX="ss-cdk${STAGE}-Synergy Data Exchange"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ECR base URL
ECR_URL="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

echo "=============================================="
echo "Docker Image Build Script"
echo "=============================================="
echo "Build Path: $BUILD_PATH"
echo "Tag: $TAG"
echo "Push to ECR: $PUSH"
echo "Patch mode: $PATCH"
if [ "$PUSH" = true ]; then
    echo "ECR URL: $ECR_URL"
    echo "ECR prefix: $ECR_REPO_PREFIX"
fi
echo "=============================================="

# Change to project root
cd "$(dirname "$0")/.."

# ---------------------------------------------------------------
# Full build function
# ---------------------------------------------------------------
build_image() {
    local name=$1
    local dockerfile=$2
    local image_uri="${ECR_URL}/${ECR_REPO_PREFIX}-${name}:${TAG}"

    echo ""
    echo "Building: $image_uri"
    echo "Dockerfile: $dockerfile"
    echo "----------------------------------------------"

    $CONTAINER_TOOL build \
        -f "docker/$dockerfile" \
        -t "$image_uri" \
        -t "${ECR_URL}/${ECR_REPO_PREFIX}-${name}:latest" \
        .

    if [ "$PUSH" = true ]; then
        echo "Pushing $image_uri"
        $CONTAINER_TOOL push ${TLS_FLAG:+"$TLS_FLAG"} "$image_uri"
        [ "$TAG" != "latest" ] && $CONTAINER_TOOL push ${TLS_FLAG:+"$TLS_FLAG"} "${ECR_URL}/${ECR_REPO_PREFIX}-${name}:latest"
    fi

    echo "OK: $image_uri built"
}

# ---------------------------------------------------------------
# Patch function: pull -> overwrite src files -> commit -> push
# ---------------------------------------------------------------
patch_image() {
    local name=$1
    local image_uri="${ECR_URL}/${ECR_REPO_PREFIX}-${name}:${TAG}"
    local container="tmp-patch-${name}"

    echo ""
    echo "Patching: $image_uri"

    $CONTAINER_TOOL pull ${TLS_FLAG:+"$TLS_FLAG"} "$image_uri"
    $CONTAINER_TOOL create --name "$container" "$image_uri" > /dev/null

    for entry in "${PATCH_FILES[@]}"; do
        local local_path="${entry%%:*}"
        local container_path="${entry##*:}"
        $CONTAINER_TOOL cp "$local_path" "${container}:${container_path}"
        echo "  Copied $local_path"
    done

    $CONTAINER_TOOL commit "$container" "$image_uri"
    $CONTAINER_TOOL rm "$container" > /dev/null

    if [ "$PUSH" = true ]; then
        $CONTAINER_TOOL push ${TLS_FLAG:+"$TLS_FLAG"} "$image_uri"
    fi

    echo "OK: $image_uri patched"
}

# Login to ECR if pushing or patching
if [ "$PUSH" = true ] || [ "$PATCH" = true ]; then
    if [ -z "$AWS_ACCOUNT_ID" ]; then
        echo "ERROR: AWS_ACCOUNT_ID not set and could not be determined"
        exit 1
    fi
    echo "Logging in to ECR..."
    aws ecr get-login-password --region "$AWS_REGION" | \
        $CONTAINER_TOOL login --username AWS --password-stdin ${TLS_FLAG:+"$TLS_FLAG"} "$ECR_URL"
fi

# Build/patch images based on path selection
case "$BUILD_PATH" in
    express|path1)
        if [ "$PATCH" = true ]; then patch_image "express"; else build_image "express" "Dockerfile.express"; fi
        ;;
    standard|path2)
        if [ "$PATCH" = true ]; then patch_image "standard"; else build_image "standard" "Dockerfile.standard"; fi
        ;;
    batch|path3)
        if [ "$PATCH" = true ]; then patch_image "batch"; else build_image "batch" "Dockerfile.batch"; fi
        ;;
    all)
        if [ "$PATCH" = true ]; then
            patch_image "express"
            patch_image "standard"
            patch_image "batch"
        else
            build_image "express"  "Dockerfile.express"
            build_image "standard" "Dockerfile.standard"
            build_image "batch"    "Dockerfile.batch"
        fi
        ;;
    *)
        echo "Unknown path: $BUILD_PATH"
        echo "Valid paths: express, standard, batch, all"
        exit 1
        ;;
esac

OPERATION=$([ "$PATCH" = true ] && echo "Patch" || echo "Build")
echo ""
echo "=============================================="
echo "$OPERATION Complete!"
echo "=============================================="
echo ""
echo "${OPERATION}d images:"
$CONTAINER_TOOL images | grep "${ECR_REPO_PREFIX}" | head -10
