#!/usr/bin/env bash

set -euo pipefail

readonly KWOK_VERSION="v0.8.0"
readonly KWOK_IMAGE="registry.k8s.io/kwok/kwok:${KWOK_VERSION}"
readonly KWOK_MANIFEST_URL="https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}/kwok.yaml"

die() {
  echo "kwok.sh: $*" >&2
  exit 1
}

readonly CLUSTER_NAME="${CLUSTER_NAME:-alfred-e2e}"
case "${CLUSTER_NAME}" in
  alfred-e2e | alfred-e2e-*) ;;
  *) die "Cluster name must start with alfred-e2e" ;;
esac
readonly CLUSTER_CONTEXT="kind-${CLUSTER_NAME}"

[[ -n "${STATE_DIR:-}" ]] || die "STATE_DIR is required"
[[ "${STATE_DIR}" = /* ]] || die "STATE_DIR must be an absolute path"
readonly KUBECONFIG_PATH="${STATE_DIR}/kubeconfig"
[[ -f "${KUBECONFIG_PATH}" ]] || die "missing kubeconfig: ${KUBECONFIG_PATH}"

command -v kubectl >/dev/null 2>&1 || die "kubectl is required"

readonly SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly MANIFEST_DIR="${SCRIPT_DIR}/manifests"
readonly KUBECTL=(kubectl --kubeconfig "${KUBECONFIG_PATH}" --context "${CLUSTER_CONTEXT}")
SMOKE_NAMESPACE=""

cleanup_smoke() {
  local namespace="${SMOKE_NAMESPACE}"
  local wait_for_delete="${1:-false}"
  [[ -n "${namespace}" ]] || return 0

  # The finalizer is deliberately retained during the assertion below. Remove
  # only this test-owned finalizer so namespace cleanup can complete.
  "${KUBECTL[@]}" --namespace "${namespace}" patch pod grace-finalizer \
    --type=merge --patch '{"metadata":{"finalizers":null}}' >/dev/null 2>&1 || true
  if [[ "${wait_for_delete}" == true ]]; then
    "${KUBECTL[@]}" delete namespace "${namespace}" \
      --wait=true --timeout=45s >/dev/null
  else
    "${KUBECTL[@]}" delete namespace "${namespace}" --wait=false >/dev/null 2>&1 || true
  fi
  SMOKE_NAMESPACE=""
}

wait_for_pod_condition() {
  local namespace="$1" pod="$2" condition_type="$3" condition_status="$4" timeout_seconds="$5"
  local deadline=$((SECONDS + timeout_seconds))
  until "${KUBECTL[@]}" --namespace "${namespace}" get pod "${pod}" --output=json 2>/dev/null | \
    jq -e --arg type "${condition_type}" --arg status "${condition_status}" \
      'any(.status.conditions[]?; .type == $type and .status == $status)' >/dev/null; do
    (( SECONDS < deadline )) || die "timed out waiting for ${namespace}/${pod} ${condition_type}=${condition_status}"
    sleep 1
  done
}

wait_for_endpoint_ready() {
  local namespace="$1" expected="$2" timeout_seconds="$3"
  local deadline=$((SECONDS + timeout_seconds))
  until "${KUBECTL[@]}" --namespace "${namespace}" get endpointslices \
    --selector=kubernetes.io/service-name=immediate --output=json 2>/dev/null | \
    jq -e --argjson expected "${expected}" \
      'any(.items[].endpoints[]?; .targetRef.name == "immediate" and .conditions.ready == $expected)' >/dev/null; do
    (( SECONDS < deadline )) || die "timed out waiting for EndpointSlice ready=${expected}"
    sleep 1
  done
}

configure_node_ips() {
  local infra_ip
  infra_ip="$("${KUBECTL[@]}" get nodes \
    --selector=alfred-e2e/infrastructure=true \
    --output=jsonpath='{range .items[*]}{range .status.addresses[?(@.type=="InternalIP")]}{.address}{"\n"}{end}{end}')"
  [[ "${infra_ip}" != *$'\n'* ]] || die "expected exactly one infrastructure node InternalIP"

  local octet1 octet2 octet3 octet4
  IFS=. read -r octet1 octet2 octet3 octet4 <<<"${infra_ip}"
  [[ -n "${octet1}" && -n "${octet2}" && -n "${octet3}" && -n "${octet4}" ]] || \
    die "infrastructure node InternalIP must be IPv4, got ${infra_ip}"

  local i node suffix virtual_ip
  local -a nodes=(
    alfred-kwok-gpu-a
    alfred-kwok-gpu-b
    alfred-kwok-gpu-c
    alfred-kwok-gpu-d
  )
  local -a suffixes=(201 202 203 204)
  for i in "${!nodes[@]}"; do
    node="${nodes[$i]}"
    suffix="${suffixes[$i]}"
    virtual_ip="${octet1}.${octet2}.${octet3}.${suffix}"
    [[ "${virtual_ip}" != "${infra_ip}" ]] || die "virtual node IP collides with infrastructure node: ${virtual_ip}"

    "${KUBECTL[@]}" annotate node "${node}" \
      "alfred-e2e.ome.io/internal-ip=${virtual_ip}" --overwrite
    # Keep reruns convergent if a previous KWOK configuration populated the
    # controller Pod IP. Fresh Nodes still receive the rest of status from the
    # initialization Stage after this annotation makes them eligible.
    "${KUBECTL[@]}" patch node "${node}" --subresource=status --type=merge \
      --patch "{\"status\":{\"addresses\":[{\"address\":\"${virtual_ip}\",\"type\":\"InternalIP\"},{\"address\":\"${node}\",\"type\":\"Hostname\"}]}}"
  done
}

install_kwok() {
  "${KUBECTL[@]}" apply --server-side --force-conflicts -f "${KWOK_MANIFEST_URL}"

  "${KUBECTL[@]}" wait --for=condition=Established \
    customresourcedefinition/stages.kwok.x-k8s.io --timeout=120s

  # The upstream release manifest is generic. A single-node kind cluster needs
  # control-plane tolerations, and KWOK documents host networking for kind.
  "${KUBECTL[@]}" --namespace kube-system patch deployment kwok-controller \
    --type=merge \
    --patch '{"spec":{"template":{"spec":{"dnsPolicy":"ClusterFirstWithHostNet","hostNetwork":true,"nodeSelector":{"alfred-e2e/infrastructure":"true"},"tolerations":[{"effect":"NoSchedule","key":"node-role.kubernetes.io/control-plane","operator":"Exists"},{"effect":"NoSchedule","key":"node-role.kubernetes.io/master","operator":"Exists"}]}}}}'
  "${KUBECTL[@]}" --namespace kube-system rollout status deployment/kwok-controller \
    --timeout=180s

  "${KUBECTL[@]}" apply -f "${MANIFEST_DIR}/kwok-stages.yaml"
  "${KUBECTL[@]}" apply -f "${MANIFEST_DIR}/kwok-nodes.yaml"
  configure_node_ips
  "${KUBECTL[@]}" wait --for=condition=Ready \
    node/alfred-kwok-gpu-a \
    node/alfred-kwok-gpu-b \
    node/alfred-kwok-gpu-c \
    node/alfred-kwok-gpu-d \
    --timeout=120s

  verify_kwok
}

verify_kwok() {
  local actual_image
  actual_image="$("${KUBECTL[@]}" --namespace kube-system get deployment kwok-controller \
    --output=jsonpath='{.spec.template.spec.containers[?(@.name=="kwok-controller")].image}')"
  [[ "${actual_image}" == "${KWOK_IMAGE}" ]] || \
    die "unexpected KWOK image ${actual_image}; want ${KWOK_IMAGE}"

  local node_count
  node_count="$("${KUBECTL[@]}" get nodes \
    --selector=alfred-e2e/virtual=true \
    --output=jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | wc -l | tr -d ' ')"
  [[ "${node_count}" == "4" ]] || die "expected 4 virtual nodes, found ${node_count}"

  "${KUBECTL[@]}" get nodes --selector=alfred-e2e/virtual=true \
    --output='custom-columns=NAME:.metadata.name,IP:.status.addresses[?(@.type=="InternalIP")].address,ZONE:.metadata.labels.topology\.kubernetes\.io/zone,GPU:.status.allocatable.nvidia\.com/gpu,READY:.status.conditions[?(@.type=="Ready")].status'
}

smoke_kwok() {
  command -v jq >/dev/null 2>&1 || die "jq is required for smoke verification"

  SMOKE_NAMESPACE="alfred-e2e-kwok-smoke-$$"
  trap cleanup_smoke EXIT
  "${KUBECTL[@]}" create namespace "${SMOKE_NAMESPACE}" >/dev/null
  "${KUBECTL[@]}" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Service
metadata:
  name: immediate
  namespace: ${SMOKE_NAMESPACE}
spec:
  selector:
    alfred-e2e.ome.io/smoke: immediate
  ports:
  - name: http
    port: 80
    targetPort: 8000
---
apiVersion: v1
kind: Pod
metadata:
  name: immediate
  namespace: ${SMOKE_NAMESPACE}
  labels:
    alfred-e2e.ome.io/smoke: immediate
spec:
  nodeSelector:
    alfred-e2e/virtual: "true"
  tolerations:
  - effect: NoSchedule
    key: alfred-e2e/virtual
    operator: Equal
    value: "true"
  readinessGates:
  - conditionType: ome.io/serving
  containers:
  - image: registry.k8s.io/pause:3.10
    name: runtime
    ports:
    - containerPort: 8000
---
apiVersion: v1
kind: Pod
metadata:
  name: held-source
  namespace: ${SMOKE_NAMESPACE}
  annotations:
    alfred-e2e.ome.io/readiness: hold-nonzero
  labels:
    ome.io/instance-index: "0"
spec:
  nodeSelector:
    alfred-e2e/virtual: "true"
  tolerations:
  - effect: NoSchedule
    key: alfred-e2e/virtual
    operator: Equal
    value: "true"
  containers:
  - image: registry.k8s.io/pause:3.10
    name: runtime
---
apiVersion: v1
kind: Pod
metadata:
  name: held-replacement
  namespace: ${SMOKE_NAMESPACE}
  annotations:
    alfred-e2e.ome.io/readiness: hold-nonzero
  labels:
    ome.io/instance-index: "1"
spec:
  nodeSelector:
    alfred-e2e/virtual: "true"
  tolerations:
  - effect: NoSchedule
    key: alfred-e2e/virtual
    operator: Equal
    value: "true"
  containers:
  - image: registry.k8s.io/pause:3.10
    name: runtime
---
apiVersion: v1
kind: Pod
metadata:
  name: delayed
  namespace: ${SMOKE_NAMESPACE}
  annotations:
    alfred-e2e.ome.io/readiness: delayed
    alfred-e2e.ome.io/readiness-delay: 2s
spec:
  nodeSelector:
    alfred-e2e/virtual: "true"
  tolerations:
  - effect: NoSchedule
    key: alfred-e2e/virtual
    operator: Equal
    value: "true"
  containers:
  - image: registry.k8s.io/pause:3.10
    name: runtime
---
apiVersion: v1
kind: Pod
metadata:
  name: grace-delete
  namespace: ${SMOKE_NAMESPACE}
spec:
  nodeSelector:
    alfred-e2e/virtual: "true"
  tolerations:
  - effect: NoSchedule
    key: alfred-e2e/virtual
    operator: Equal
    value: "true"
  terminationGracePeriodSeconds: 3
  containers:
  - image: registry.k8s.io/pause:3.10
    name: runtime
---
apiVersion: v1
kind: Pod
metadata:
  name: grace-finalizer
  namespace: ${SMOKE_NAMESPACE}
  finalizers:
  - smoke.alfred-e2e/finalizer
spec:
  nodeSelector:
    alfred-e2e/virtual: "true"
  tolerations:
  - effect: NoSchedule
    key: alfred-e2e/virtual
    operator: Equal
    value: "true"
  terminationGracePeriodSeconds: 1
  containers:
  - image: registry.k8s.io/pause:3.10
    name: runtime
EOF

  wait_for_pod_condition "${SMOKE_NAMESPACE}" immediate ContainersReady True 20
  wait_for_pod_condition "${SMOKE_NAMESPACE}" immediate Ready False 20
  wait_for_pod_condition "${SMOKE_NAMESPACE}" held-source Ready True 20
  wait_for_pod_condition "${SMOKE_NAMESPACE}" held-replacement ContainersReady False 20
  wait_for_pod_condition "${SMOKE_NAMESPACE}" held-replacement Ready False 20
  wait_for_pod_condition "${SMOKE_NAMESPACE}" delayed Ready True 20
  wait_for_endpoint_ready "${SMOKE_NAMESPACE}" false 20

  "${KUBECTL[@]}" --namespace "${SMOKE_NAMESPACE}" patch pod immediate \
    --subresource=status --type=strategic \
    --patch '{"status":{"conditions":[{"type":"ome.io/serving","status":"True","reason":"SmokeServing","message":"controller-owned sentinel"}]}}' >/dev/null
  wait_for_pod_condition "${SMOKE_NAMESPACE}" immediate Ready True 20
  wait_for_endpoint_ready "${SMOKE_NAMESPACE}" true 20
  "${KUBECTL[@]}" --namespace "${SMOKE_NAMESPACE}" get pod immediate --output=json | \
    jq -e 'any(.status.conditions[]?; .type == "ome.io/serving" and .status == "True" and .reason == "SmokeServing" and .message == "controller-owned sentinel")' >/dev/null

  "${KUBECTL[@]}" --namespace "${SMOKE_NAMESPACE}" patch pod immediate \
    --subresource=status --type=strategic \
    --patch '{"status":{"conditions":[{"type":"ome.io/serving","status":"False","reason":"SmokeDraining","message":"controller-owned drain sentinel"}]}}' >/dev/null
  wait_for_pod_condition "${SMOKE_NAMESPACE}" immediate Ready False 20
  wait_for_endpoint_ready "${SMOKE_NAMESPACE}" false 20
  "${KUBECTL[@]}" --namespace "${SMOKE_NAMESPACE}" get pod immediate --output=json | \
    jq -e 'any(.status.conditions[]?; .type == "ome.io/serving" and .status == "False" and .reason == "SmokeDraining" and .message == "controller-owned drain sentinel")' >/dev/null

  "${KUBECTL[@]}" --namespace "${SMOKE_NAMESPACE}" annotate pod held-replacement \
    alfred-e2e.ome.io/readiness=immediate --overwrite >/dev/null
  wait_for_pod_condition "${SMOKE_NAMESPACE}" held-replacement Ready True 20

  wait_for_pod_condition "${SMOKE_NAMESPACE}" grace-delete Ready True 20
  "${KUBECTL[@]}" --namespace "${SMOKE_NAMESPACE}" delete pod grace-delete --wait=false >/dev/null
  "${KUBECTL[@]}" --namespace "${SMOKE_NAMESPACE}" get pod grace-delete --output=json | \
    jq -e '.metadata.deletionTimestamp != null and .metadata.deletionGracePeriodSeconds == 3' >/dev/null
  sleep 4
  if "${KUBECTL[@]}" --namespace "${SMOKE_NAMESPACE}" get pod grace-delete >/dev/null 2>&1; then
    die "grace-delete still exists after its grace period"
  fi

  wait_for_pod_condition "${SMOKE_NAMESPACE}" grace-finalizer Ready True 20
  "${KUBECTL[@]}" --namespace "${SMOKE_NAMESPACE}" delete pod grace-finalizer --wait=false >/dev/null
  sleep 2
  "${KUBECTL[@]}" --namespace "${SMOKE_NAMESPACE}" get pod grace-finalizer --output=json | \
    jq -e '.metadata.deletionTimestamp != null and .metadata.finalizers == ["smoke.alfred-e2e/finalizer"]' >/dev/null

  local transition_before heartbeat_before transition_after heartbeat_after
  transition_before="$("${KUBECTL[@]}" get node alfred-kwok-gpu-a --output=jsonpath='{.status.conditions[?(@.type=="Ready")].lastTransitionTime}')"
  heartbeat_before="$("${KUBECTL[@]}" get node alfred-kwok-gpu-a --output=jsonpath='{.status.conditions[?(@.type=="Ready")].lastHeartbeatTime}')"
  sleep 11
  transition_after="$("${KUBECTL[@]}" get node alfred-kwok-gpu-a --output=jsonpath='{.status.conditions[?(@.type=="Ready")].lastTransitionTime}')"
  heartbeat_after="$("${KUBECTL[@]}" get node alfred-kwok-gpu-a --output=jsonpath='{.status.conditions[?(@.type=="Ready")].lastHeartbeatTime}')"
  [[ "${transition_before}" == "${transition_after}" ]] || die "node heartbeat reset Ready lastTransitionTime"
  [[ "${heartbeat_before}" != "${heartbeat_after}" ]] || die "node Ready heartbeat did not advance"

  cleanup_smoke true
  trap - EXIT
  echo "KWOK lifecycle smoke passed"
}

case "${1:-install}" in
  install)
    install_kwok
    ;;
  verify)
    verify_kwok
    ;;
  smoke)
    smoke_kwok
    ;;
  *)
    die "usage: $0 [install|verify|smoke]"
    ;;
esac
