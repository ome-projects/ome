#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${STATE_DIR:?Set STATE_DIR to an existing absolute private test-state directory}"
[[ "${STATE_DIR}" == /* && -d "${STATE_DIR}" ]] || { echo "STATE_DIR must be an existing absolute directory" >&2; exit 1; }
export DOCKER_CONTEXT="${ALFRED_DOCKER_CONTEXT:-colima-alfred-e2e}"
cluster_name="${CLUSTER_NAME:-alfred-e2e}"
case "${cluster_name}" in alfred-e2e|alfred-e2e-*) ;; *) echo "Cluster name must start with alfred-e2e" >&2; exit 1 ;; esac
node_image="kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0"
k=(kubectl --kubeconfig "${STATE_DIR}/kubeconfig" --context "kind-${cluster_name}")

if [[ -e "${STATE_DIR}/kubeconfig" ]]; then
  # Reuse only our explicitly identified test control plane, never current-context.
  [[ "$("${k[@]}" get node "${cluster_name}-control-plane" -o jsonpath='{.metadata.labels.alfred-e2e/infrastructure}')" == true ]] || {
    echo "Existing kubeconfig does not identify an Alfred test cluster" >&2; exit 1;
  }
else
  while IFS= read -r existing; do
    if [[ "${existing}" == "${cluster_name}" ]]; then
      echo "Cluster ${cluster_name} already exists without this private kubeconfig; refusing to adopt it" >&2
      exit 1
    fi
  done < <(kind get clusters)
  kind create cluster --name "${cluster_name}" --image "${node_image}" \
    --config "${script_dir}/manifests/kind.yaml" \
    --kubeconfig "${STATE_DIR}/kubeconfig" --wait 60s
fi

# Infrastructure DaemonSets must not create pretend agents on KWOK nodes.
for daemonset in kindnet kube-proxy; do
  "${k[@]}" -n kube-system patch daemonset "${daemonset}" --type=merge \
    -p '{"spec":{"template":{"spec":{"nodeSelector":{"alfred-e2e/infrastructure":"true"}}}}}'
done

# OME's unchanged chart uses cert-manager for its admission-webhook certificate.
helm --kubeconfig "${STATE_DIR}/kubeconfig" --kube-context "kind-${cluster_name}" \
  upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager \
  --version v1.21.2 --namespace cert-manager --create-namespace \
  --set crds.enabled=true --wait --timeout 180s
"${k[@]}" get nodes -o wide
