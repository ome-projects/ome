#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "${script_dir}/../.." && pwd)"
: "${STATE_DIR:?Set STATE_DIR to the absolute private test-state directory}"
[[ "${STATE_DIR}" == /* && -d "${STATE_DIR}" ]] || { echo "STATE_DIR must be an existing absolute directory" >&2; exit 1; }
export DOCKER_CONTEXT="${ALFRED_DOCKER_CONTEXT:-colima-alfred-e2e}"
cluster_name="${CLUSTER_NAME:-alfred-e2e}"
case "${cluster_name}" in alfred-e2e|alfred-e2e-*) ;; *) echo "Cluster name must start with alfred-e2e" >&2; exit 1 ;; esac
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
arch="$(docker info --format '{{.Architecture}}')"
case "${arch}" in
  aarch64|arm64) arch=arm64 ;;
  x86_64|amd64) arch=amd64 ;;
  *) echo "Unsupported Docker architecture: ${arch}" >&2; exit 1 ;;
esac
export CGO_ENABLED=0 GOOS=linux GOARCH="${arch}"
export GOMAXPROCS="${ALFRED_BUILD_JOBS:-6}"

cd "${project_dir}"
# Keep the private kubeconfig and collected evidence out of the build context.
cp "${script_dir}/dockerignore" "${STATE_DIR}/.dockerignore"
go build -p "${GOMAXPROCS}" -o "${STATE_DIR}/manager" ./cmd/manager
go build -p "${GOMAXPROCS}" -o "${STATE_DIR}/alfred" ./cmd/alfred
(cd pkg/alfred/simulator && go build -p "${GOMAXPROCS}" -o "${STATE_DIR}/alfred-simulator" ./cmd/alfred-simulator)
(cd scheduler && go build -p "${GOMAXPROCS}" -o "${STATE_DIR}/ome-scheduler" ./cmd/ome-scheduler)

# Homebrew's buildx may be installed without global Docker plugin settings.
if docker buildx version >/dev/null 2>&1; then
  builder=(docker buildx)
elif [[ -x /opt/homebrew/lib/docker/cli-plugins/docker-buildx ]]; then
  builder=(/opt/homebrew/lib/docker/cli-plugins/docker-buildx)
else
  echo "Install Docker buildx before building the test images" >&2
  exit 1
fi
for component in manager alfred scheduler; do
  "${builder[@]}" build --load --target "${component}" \
    -f "${script_dir}/Dockerfile" -t "alfred-e2e/${component}:local" "${STATE_DIR}"
done
kind load docker-image --name "${cluster_name}" \
  alfred-e2e/manager:local alfred-e2e/alfred:local alfred-e2e/scheduler:local

# CRI reports the platform image config digest, not Docker's OCI index digest.
# Check the loaded index, then record CRI IDs to reject stale containers.
image_digests='{}'
for component in manager alfred scheduler; do
  image_ref="docker.io/alfred-e2e/${component}:local"
  docker_id="$(docker image inspect "${image_ref}" --format '{{.Id}}')"
  loaded_id="$(docker exec "${cluster_name}-control-plane" \
    ctr -n k8s.io images ls | awk -v image="${image_ref}" '$1 == image { print $3 }')"
  [[ "${docker_id}" == "${loaded_id}" ]] || {
    echo "kind did not load the freshly built ${component} image" >&2; exit 1;
  }
  cri_id="$(docker exec "${cluster_name}-control-plane" crictl inspecti "${image_ref}" | jq -er '.status.id')"
  image_digests="$(jq -cn --argjson existing "${image_digests}" \
    --arg component "${component}" --arg id "${cri_id}" \
    '$existing + {($component): $id}')"
done
printf '%s\n' "${image_digests}" >"${STATE_DIR}/image-digests.json"
