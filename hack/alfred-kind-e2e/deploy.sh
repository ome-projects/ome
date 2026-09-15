#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd "${script_dir}/../.." && pwd)"
: "${STATE_DIR:?Set STATE_DIR to an existing absolute private test-state directory}"
[[ "${STATE_DIR}" == /* && -d "${STATE_DIR}" ]] || {
  echo "STATE_DIR must be an existing absolute directory" >&2
  exit 1
}

for command in docker go helm jq kubectl shasum; do
  command -v "${command}" >/dev/null || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done

cluster_name="${CLUSTER_NAME:-alfred-e2e}"
case "${cluster_name}" in
  alfred-e2e | alfred-e2e-*) ;;
  *)
    echo "Cluster name must start with alfred-e2e" >&2
    exit 1
    ;;
esac

kubeconfig="${STATE_DIR}/kubeconfig"
context="kind-${cluster_name}"
[[ -f "${kubeconfig}" ]] || {
  echo "Private kubeconfig not found: ${kubeconfig}" >&2
  exit 1
}
k=(kubectl --kubeconfig "${kubeconfig}" --context "${context}")
h=(helm --kubeconfig "${kubeconfig}" --kube-context "${context}")
manifests="${script_dir}/manifests"
image_verifier="${script_dir}/verify-running-image.sh"
work_dir="${STATE_DIR}/deploy"
docker_context="${ALFRED_DOCKER_CONTEXT:-colima-alfred-e2e}"
mkdir -p "${work_dir}"

# Helm 4's client-side update mode is required here. The first manager install
# lets cert-manager take ownership of injected webhook caBundle fields; forcing
# server-side ownership on a rerun would conflict with the CA injector.
helm_version="$("${h[@]}" version --template '{{.Version}}')"
helm_major="${helm_version#v}"
helm_major="${helm_major%%.*}"
[[ "${helm_major}" == 4 ]] || {
  echo "Alfred kind acceptance requires Helm 4 (--server-side=false); found ${helm_version}" >&2
  exit 1
}

image_digests="${STATE_DIR}/image-digests.json"
[[ -f "${image_digests}" ]] || {
  echo "Build image digest manifest not found: ${image_digests}; run build.sh first" >&2
  exit 1
}
for component in manager alfred scheduler; do
  expected="$(jq -er --arg component "${component}" '.[$component]' "${image_digests}")" || {
    echo "Missing ${component} digest in ${image_digests}" >&2
    exit 1
  }
  [[ "${expected}" =~ ^sha256:[0-9a-f]{64}$ ]] || {
    echo "Invalid ${component} digest in ${image_digests}: ${expected}" >&2
    exit 1
  }
done

verify_running_image() {
  local deployment="$1"
  local container="$2"
  local component="$3"
  local expected

  expected="$(jq -er --arg component "${component}" '.[$component]' "${image_digests}")"
  "${image_verifier}" \
    "${kubeconfig}" "${context}" ome "${deployment}" "${container}" \
    "${cluster_name}-control-plane" "${expected}" "${docker_context}"
}

wait_manager_webhook() {
  local deadline=$((SECONDS + 180))
  local probe remaining request_timeout

  # Client dry-run extraction does not require the fixture namespace to exist.
  # A fresh generated name makes every server dry run invoke CREATE admission,
  # including when default-runtime was not created by the initial Helm pass.
  # Disabled runtimes still read the webhook cache, without priority conflicts
  # against fixture runtimes left by a previous acceptance run.
  if ! probe="$("${k[@]}" --request-timeout=10s create --dry-run=client --validate=false \
    -f "${manifests}/workload-single.yaml" -o json 2>/dev/null \
    | jq -sce '[.[] | (.items // [.])[] | select(.kind == "ClusterServingRuntime")] |
      if length == 1 then .[0] | .metadata = {generateName: "alfred-e2e-webhook-probe-"} | .spec.disabled = true
      else error("expected one cluster runtime") end' 2>/dev/null)"; then
    echo "Could not prepare the manager webhook admission probe" >&2
    return 1
  fi

  echo "Waiting for manager webhook admission on ${context}" >&2
  while ((SECONDS < deadline)); do
    remaining=$((deadline - SECONDS))
    request_timeout="${remaining}"
    if ((request_timeout > 10)); then request_timeout=10; fi
    if "${k[@]}" --request-timeout="${request_timeout}s" create --dry-run=server -f - \
      <<<"${probe}" >/dev/null 2>&1; then
      return 0
    fi
    remaining=$((deadline - SECONDS))
    if ((remaining <= 0)); then break; fi
    if ((remaining > 2)); then remaining=2; fi
    sleep "${remaining}"
  done
  echo "Manager webhook admission did not become ready within 180 seconds" >&2
  return 1
}

[[ "$("${k[@]}" get node "${cluster_name}-control-plane" -o jsonpath='{.metadata.labels.alfred-e2e/infrastructure}')" == true ]] || {
  echo "Kubeconfig does not identify the expected isolated Alfred cluster" >&2
  exit 1
}
server_minor="$("${k[@]}" version -o json | jq -r '.serverVersion.minor' | tr -d '+')"
[[ "${server_minor}" == 35 ]] || {
  echo "Alfred acceptance requires Kubernetes 1.35.x; server minor is ${server_minor}" >&2
  exit 1
}

"${k[@]}" -n cert-manager rollout status deployment/cert-manager --timeout=180s
"${k[@]}" -n cert-manager rollout status deployment/cert-manager-webhook --timeout=180s
"${k[@]}" -n cert-manager rollout status deployment/cert-manager-cainjector --timeout=180s

# The manager discovers PodGroup support once at startup, so establish the CRD
# before installing the controller deployment.
podgroup_crd="${PODGROUP_CRD_PATH:-}"
if [[ -z "${podgroup_crd}" ]]; then
  scheduler_plugins_dir="$(cd "${project_dir}" && go list -m -mod=readonly -f '{{.Dir}}' sigs.k8s.io/scheduler-plugins)"
  podgroup_crd="${scheduler_plugins_dir}/config/crd/bases/scheduling.x-k8s.io_podgroups.yaml"
fi
[[ -f "${podgroup_crd}" ]] || {
  echo "PodGroup CRD not found: ${podgroup_crd}" >&2
  exit 1
}
"${k[@]}" apply -f "${podgroup_crd}"
"${k[@]}" wait --for=condition=Established crd/podgroups.scheduling.x-k8s.io --timeout=60s

"${h[@]}" upgrade --install ome-crd "${project_dir}/charts/ome-crd" \
  --namespace ome --create-namespace --wait --timeout 3m

# On the first install the chart creates its Certificate, webhook and default
# runtime in one release. Helm 4 can reach the default runtime before CA
# injection and webhook readiness. Keep that first pass, wait for the manager,
# then converge with client-side updates so cert-manager retains caBundle field
# ownership.
if ! "${h[@]}" upgrade --install ome "${project_dir}/charts/ome-resources" \
  --namespace ome --values "${manifests}/manager-values.yaml" \
  --server-side=false --wait --timeout 5m; then
  echo "Initial manager pass is waiting for its generated webhook certificate; converging after readiness" >&2
fi
"${k[@]}" -n ome wait --for=condition=Ready certificate/serving-cert --timeout=180s
"${k[@]}" -n ome rollout status deployment/ome-controller-manager --timeout=180s
wait_manager_webhook
"${h[@]}" upgrade --install ome "${project_dir}/charts/ome-resources" \
  --namespace ome --values "${manifests}/manager-values.yaml" \
  --server-side=false --wait --timeout 5m
"${k[@]}" -n ome rollout restart deployment/ome-controller-manager
"${k[@]}" -n ome rollout status deployment/ome-controller-manager --timeout=180s
wait_manager_webhook
verify_running_image ome-controller-manager manager manager

"${h[@]}" upgrade --install ome-scheduler "${project_dir}/charts/ome-scheduler" \
  --namespace ome --values "${manifests}/scheduler-values.yaml" \
  --wait --timeout 3m

"${k[@]}" -n ome create configmap alfred-default-scheduler-config \
  --from-file=config.yaml="${manifests}/default-scheduler.yaml" \
  --dry-run=client -o yaml | "${k[@]}" apply -f -
"${k[@]}" apply -f "${manifests}/default-scheduler-deployment.yaml"
"${k[@]}" -n ome rollout restart deployment/ome-scheduler
"${k[@]}" -n ome rollout restart deployment/alfred-default-scheduler
"${k[@]}" -n ome rollout status deployment/ome-scheduler --timeout=180s
"${k[@]}" -n ome rollout status deployment/alfred-default-scheduler --timeout=180s
verify_running_image ome-scheduler scheduler scheduler
verify_running_image alfred-default-scheduler scheduler scheduler

# Probe the exact worker artifact against the exact ConfigMaps mounted by the
# two live 1.35.4 schedulers. The returned identities are the only identities
# admitted into Alfred's startup registry and policy map.
probe_profile() {
  local pod_name="$1"
  local backend="$2"
  local config_map="$3"
  local want_scheduler="$4"
  local want_gang="$5"

  "${k[@]}" -n ome delete pod "${pod_name}" --ignore-not-found --wait=true >/dev/null
  jq -n \
    --arg name "${pod_name}" \
    --arg backend "${backend}" \
    --arg configMap "${config_map}" \
    '{
      apiVersion: "v1",
      kind: "Pod",
      metadata: {name: $name, namespace: "ome", labels: {"alfred-e2e/probe": "true"}},
      spec: {
        restartPolicy: "Never",
        automountServiceAccountToken: false,
        enableServiceLinks: false,
        nodeSelector: {"alfred-e2e/infrastructure": "true"},
        tolerations: [{key: "node-role.kubernetes.io/control-plane", operator: "Exists", effect: "NoSchedule"}],
        securityContext: {runAsNonRoot: true, seccompProfile: {type: "RuntimeDefault"}},
        containers: [{
          name: "probe",
          image: "alfred-e2e/alfred:local",
          imagePullPolicy: "IfNotPresent",
          command: ["/alfred-simulator"],
          args: ["--backend", $backend, "--scheduler-config", "/profile/config.yaml", "--print-profile"],
          securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: ["ALL"]}},
          volumeMounts: [{name: "profile", mountPath: "/profile", readOnly: true}]
        }],
        volumes: [{name: "profile", configMap: {name: $configMap}}]
      }
    }' | "${k[@]}" apply -f - >&2

  if ! "${k[@]}" -n ome wait pod/"${pod_name}" \
    --for=jsonpath='{.status.phase}'=Succeeded --timeout=90s >&2; then
    "${k[@]}" -n ome logs "${pod_name}" >&2 || true
    "${k[@]}" -n ome describe pod "${pod_name}" >&2 || true
    return 1
  fi

  local output
  output="$("${k[@]}" -n ome logs "${pod_name}")"
  jq -e \
    --arg scheduler "${want_scheduler}" \
    --arg backend "${backend}" \
    --argjson gang "${want_gang}" \
    '.identity.schedulerName == $scheduler and
     .identity.backend == $backend and
     .identity.schedulerVersion == "v1.35.4" and
     (.identity.configurationID | startswith("sha256:")) and
     .gangScheduling == $gang' <<<"${output}" >/dev/null
  printf '%s\n' "${output}"
}

default_profile="$(probe_profile alfred-profile-default kind-default-v135 alfred-default-scheduler-config alfred-default-scheduler false)"
ome_profile="$(probe_profile alfred-profile-ome kind-ome-v135 ome-scheduler-config ome-scheduler true)"

"${k[@]}" -n ome get configmap ome-scheduler-config -o json \
  | jq -r '.data["config.yaml"]' >"${work_dir}/ome-scheduler.yaml"
jq -n \
  --argjson defaultProfile "${default_profile}" \
  --argjson omeProfile "${ome_profile}" \
  '{workers: [
    {
      binaryPath: "/alfred-simulator",
      schedulerConfigPath: "/etc/alfred-simulation/default-scheduler.yaml",
      identity: $defaultProfile.identity,
      gangScheduling: $defaultProfile.gangScheduling
    },
    {
      binaryPath: "/alfred-simulator",
      schedulerConfigPath: "/etc/alfred-simulation/ome-scheduler.yaml",
      identity: $omeProfile.identity,
      gangScheduling: $omeProfile.gangScheduling
    }
  ]}' >"${work_dir}/workers.json"

registry_hash="$(shasum -a 256 "${work_dir}/workers.json" | awk '{print substr($1, 1, 12)}')"
simulation_name="alfred-simulation-${registry_hash}"
if ! "${k[@]}" -n ome get configmap "${simulation_name}" >/dev/null 2>&1; then
  "${k[@]}" -n ome create configmap "${simulation_name}" \
    --from-file=workers.json="${work_dir}/workers.json" \
    --from-file=default-scheduler.yaml="${manifests}/default-scheduler.yaml" \
    --from-file=ome-scheduler.yaml="${work_dir}/ome-scheduler.yaml" \
    --dry-run=client -o json \
    | jq '.immutable = true' \
    | "${k[@]}" apply -f -
fi

default_config_id="$(jq -r '.identity.configurationID' <<<"${default_profile}")"
ome_config_id="$(jq -r '.identity.configurationID' <<<"${ome_profile}")"
"${h[@]}" upgrade --install ome-alfred "${project_dir}/charts/ome-alfred" \
  --namespace ome --values "${manifests}/alfred-values.yaml" \
  --set-string "simulation.configMapName=${simulation_name}" \
  --set-string 'alfredConfig.scheduling.profiles.alfred-default-scheduler.backend=kind-default-v135' \
  --set-string 'alfredConfig.scheduling.profiles.alfred-default-scheduler.schedulerVersion=v1.35.4' \
  --set-string "alfredConfig.scheduling.profiles.alfred-default-scheduler.configurationID=${default_config_id}" \
  --set 'alfredConfig.scheduling.profiles.alfred-default-scheduler.gangScheduling=false' \
  --set-string 'alfredConfig.scheduling.profiles.ome-scheduler.backend=kind-ome-v135' \
  --set-string 'alfredConfig.scheduling.profiles.ome-scheduler.schedulerVersion=v1.35.4' \
  --set-string "alfredConfig.scheduling.profiles.ome-scheduler.configurationID=${ome_config_id}" \
  --set 'alfredConfig.scheduling.profiles.ome-scheduler.gangScheduling=true' \
  --wait --timeout 3m
"${k[@]}" -n ome rollout restart deployment/ome-alfred
"${k[@]}" -n ome rollout status deployment/ome-alfred --timeout=180s
verify_running_image ome-alfred alfred alfred

echo "Deployed real OME controllers and Alfred on ${context}:"
"${k[@]}" -n ome get deployment ome-controller-manager ome-scheduler alfred-default-scheduler ome-alfred
echo "Simulation registry: ome/${simulation_name}"
