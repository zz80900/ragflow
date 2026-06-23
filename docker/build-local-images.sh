#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

IMAGE_NAMESPACE="xgd"
NEED_MIRROR="${NEED_MIRROR:-0}"

if [[ "$#" -gt 1 ]]; then
  echo "Usage: $0 [tag]" >&2
  exit 1
fi

if [[ "$#" -eq 1 ]]; then
  TAG="$1"
else
  if [[ ! -t 0 ]]; then
    echo "Docker tag argument is required when stdin is not interactive." >&2
    echo "Usage: $0 [tag]" >&2
    exit 1
  fi
  read -r -p "Enter Docker image tag: " TAG
fi

if [[ ! "${TAG}" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
  echo "Invalid Docker tag: ${TAG}" >&2
  echo "Use letters, numbers, underscore, dot, or dash. Max length: 128." >&2
  exit 1
fi

REVISION="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
RAGFLOW_IMAGE="${IMAGE_NAMESPACE}/ragflow:${TAG}"
WECOM_AIBOT_IMAGE="${IMAGE_NAMESPACE}/ragflow-wecom-aibot:${TAG}"

export DOCKER_BUILDKIT="${DOCKER_BUILDKIT:-1}"

cat <<EOF
Building local images:
  ${RAGFLOW_IMAGE}
  ${WECOM_AIBOT_IMAGE}

Image namespace is fixed:
  ${IMAGE_NAMESPACE}
EOF

echo "Building RAGFlow image:"
echo "  ${RAGFLOW_IMAGE}"

docker build \
  --build-arg "NEED_MIRROR=${NEED_MIRROR}" \
  --label "org.opencontainers.image.source=local" \
  --label "org.opencontainers.image.revision=${REVISION}" \
  -t "${RAGFLOW_IMAGE}" \
  -f "${REPO_ROOT}/Dockerfile" \
  "${REPO_ROOT}"

echo "Building WeCom AIBot image:"
echo "  ${WECOM_AIBOT_IMAGE}"

docker build \
  --build-arg "RAGFLOW_BASE_IMAGE=${RAGFLOW_IMAGE}" \
  --build-arg "NEED_MIRROR=${NEED_MIRROR}" \
  --label "org.opencontainers.image.source=local" \
  --label "org.opencontainers.image.revision=${REVISION}" \
  -t "${WECOM_AIBOT_IMAGE}" \
  -f "${REPO_ROOT}/docker/wecom-aibot/Dockerfile" \
  "${REPO_ROOT}"

cat <<EOF

Local images are ready:
  ${RAGFLOW_IMAGE}
  ${WECOM_AIBOT_IMAGE}

Use this with the normal compose override:
  export RAGFLOW_CUSTOM_IMAGE=${WECOM_AIBOT_IMAGE}
EOF
