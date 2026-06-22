#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -f "$SCRIPT_DIR/.env" ]; then
  set -o allexport
  source "$SCRIPT_DIR/.env"
  set +o allexport
fi

if [ -z "${DOCKER_REGISTRY:-}" ]; then
  echo "Error: DOCKER_REGISTRY is not set. Create docker/ci-cd/.env or export the variable." >&2
  exit 1
fi
IMAGE="${DOCKER_REGISTRY}/generic-images/jobrunr-builder:gradle-9.5.0-jdk26-eclipse-temurin"

echo "Building $IMAGE..."
docker build --build-arg DOCKER_REGISTRY="$DOCKER_REGISTRY" -t "$IMAGE" "$SCRIPT_DIR"

echo "Pushing $IMAGE..."
docker push "$IMAGE"

echo "Done: $IMAGE"
