#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
helper="${script_dir}/verify-running-image.sh"
mock_kubectl="${script_dir}/testdata/mock-bin/mock-kubectl"
mock_docker="${script_dir}/testdata/mock-bin/mock-docker"
expected=sha256:3aa6200000000000000000000000000000000000000000000000000000000000

KUBECTL_BIN="${mock_kubectl}" DOCKER_BIN="${mock_docker}" "${helper}" \
  /dev/null kind-alfred-e2e ome ome-controller-manager manager \
  alfred-e2e-control-plane "${expected}" colima-alfred-e2e

if KUBECTL_BIN="${mock_kubectl}" DOCKER_BIN="${mock_docker}" \
  MOCK_CONFIG_ID=sha256:dead000000000000000000000000000000000000000000000000000000000000 \
  "${helper}" \
    /dev/null kind-alfred-e2e ome ome-controller-manager manager \
    alfred-e2e-control-plane "${expected}" colima-alfred-e2e >/dev/null 2>&1; then
  echo "verify running image test: stale resolved config ID was accepted" >&2
  exit 1
fi

if KUBECTL_BIN="${mock_kubectl}" DOCKER_BIN="${mock_docker}" MOCK_AMBIGUOUS_IMAGE=true \
  "${helper}" \
    /dev/null kind-alfred-e2e ome ome-controller-manager manager \
    alfred-e2e-control-plane "${expected}" colima-alfred-e2e >/dev/null 2>&1; then
  echo "verify running image test: ambiguous import image descriptor was accepted" >&2
  exit 1
fi

echo "verify running image test: passed"
