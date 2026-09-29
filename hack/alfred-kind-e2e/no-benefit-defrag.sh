#!/usr/bin/env bash
# Real default-scheduler preflight fits, but its actual placement has zero gain.
set -euo pipefail
: "${STATE_DIR:?Set STATE_DIR to the dedicated absolute test-state directory}"
[[ "${STATE_DIR}" == /* && -f "${STATE_DIR}/kubeconfig" ]] || exit 2
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${dir}/namespace-churn-lib.sh"
source "${dir}/no-benefit-defrag-lib.sh"
for command in kubectl jq yq shasum; do command -v "${command}" >/dev/null; done
k=(kubectl --kubeconfig "${STATE_DIR}/kubeconfig" --context kind-alfred-e2e --request-timeout=15s)
run_id="$(date -u +%Y%m%d%H%M%S)-$$"
art="${STATE_DIR}/artifacts/no-benefit-defrag-${run_id}"
mkdir -p "${art}"
registry_name="alfred-no-benefit-${run_id}"
policy_name="${registry_name}-policy"
runtime="alfred-no-benefit-${run_id}"
configured=false; registry_installed=false; cordoned=false; passed=false
request_pid=''; pod_pid=''; enabled=''; service=''; alfred_pod=''; alfred_uid=''

capture() {
  local file="$1" prefix="${1%.json}"
  "${k[@]}" get pods -A -o json >"${prefix}-pods.json"
  "${k[@]}" get nodes -l alfred-e2e/virtual=true -o json >"${prefix}-nodes.json"
  "${k[@]}" -n alfred-e2e get endpointslices -l "kubernetes.io/service-name=${service}" -o json >"${prefix}-endpoints.json"
  "${k[@]}" -n alfred-e2e get inferenceservice single -o json >"${prefix}-isvc.json"
  "${k[@]}" -n alfred-e2e get inferencereplica single-engine -o json >"${prefix}-ir.json"
  "${k[@]}" -n ome get cm alfred-recommendations alfred-dispatch-state -o json >"${prefix}-alfred.json"
  jq -n --slurpfile pods "${prefix}-pods.json" --slurpfile nodes "${prefix}-nodes.json" \
    --slurpfile endpoints "${prefix}-endpoints.json" --slurpfile isvc "${prefix}-isvc.json" \
    --slurpfile ir "${prefix}-ir.json" --slurpfile alfred "${prefix}-alfred.json" '
    {allPods:$pods[0],nodes:$nodes[0],endpoints:$endpoints[0],isvc:$isvc[0],ir:$ir[0],
     pods:{items:[$pods[0].items[]|select(.metadata.namespace=="alfred-e2e" and .metadata.labels["ome.io/inferenceservice"]=="single")]},
     dispatch:([$alfred[0].items[]|select(.metadata.name=="alfred-dispatch-state")][0]),
     recommendations:([$alfred[0].items[]|select(.metadata.name=="alfred-recommendations")][0])}' >"${file}"
}
idle() {
  jq -e -L "${dir}" --slurpfile e "${art}/context.json" 'include "no-benefit-defrag-sample"; nd_idle($e[0])' "$1" >/dev/null
}
metrics() {
  local prefix="$1"
  "${k[@]}" -n ome get pod "${alfred_pod}" -o json >"${prefix}-pod.json" || return 1
  jq -e --arg uid "${alfred_uid}" '.metadata.uid==$uid and .metadata.deletionTimestamp==null and
    all(.status.containerStatuses[];.restartCount==0)' "${prefix}-pod.json" >/dev/null || return 1
  "${k[@]}" get --raw "/api/v1/namespaces/ome/pods/${alfred_pod}:8080/proxy/metrics" >"${prefix}.txt" || return 1
  jq -Rn '[inputs|select(startswith("alfred_policy_reload_total{"))|
    capture("^alfred_policy_reload_total\\{outcome=\"(?<outcome>[^\"]+)\"\\} (?<value>[0-9.eE+-]+)$")|
    {key:.outcome,value:(.value|tonumber)}]|from_entries' <"${prefix}.txt" >"${prefix}.json"
}
restore_config() {
  local current patch deadline cycle
  metrics "${art}/reload-before" || return 1
  current="$("${k[@]}" -n ome get cm "${policy_name}" -o json)" || return 1
  patch="$(nd_config_patch "$(cat "${art}/config-before.json")" "${current}" "${enabled}" restore)" || return 1
  # An uncertain enable may not have applied. With no override present, leave
  # the fail-closed barrier in place rather than invent reload evidence.
  [[ "${patch}" != '[]' ]] || { echo 'No policy transition to verify; retaining barrier' >&2; return 1; }
  cycle="$("${k[@]}" -n ome get cm alfred-recommendations -o json | jq -er '.data["last-cycle.json"]|fromjson|.timestamp')" || return 1
  "${k[@]}" -n ome patch cm "${policy_name}" --type=json -p "${patch}" -o json >"${art}/config-restored.json" || return 1
  deadline=$((SECONDS+60))
  while ((SECONDS<deadline)); do
    metrics "${art}/reload-after" || return 1
    "${k[@]}" -n ome get cm "${policy_name}" -o json >"${art}/config-verified.json" || return 1
    jq -e --slurpfile old "${art}/config-before.json" '.metadata.uid==$old[0].metadata.uid and .data["config.yaml"]==$old[0].data["config.yaml"]' "${art}/config-verified.json" >/dev/null || return 1
    if jq -e --slurpfile before "${art}/reload-before.json" '.success>($before[0].success // 0) and (.failure // 0)==($before[0].failure // 0)' "${art}/reload-after.json" >/dev/null; then
      capture "${art}/disabled-observed.json" || return 1
      idle "${art}/disabled-observed.json" || return 1
      if jq -e -L "${dir}" --arg cycle "${cycle}" 'include "placement-pause-sample";
        pp_cycle|(.timestamp|pp_time)>($cycle|pp_time) and all(.recommendations[]?;.policy!="defragmentation")' "${art}/disabled-observed.json" >/dev/null; then return 0; fi
    fi
    sleep 0.5
  done
  echo 'Disabled policy reload was not verified; retaining barrier' >&2
  return 1
}
cleanup() {
  local rc=$? current patch object uid name resource namespace key
  for pid in "${request_pid}" "${pod_pid}"; do
    if [[ -n "${pid}" ]]; then kill "${pid}" 2>/dev/null || true; wait "${pid}" 2>/dev/null || true; fi
  done
  if [[ "${configured}" == true ]]; then
    restore_config || { echo "Config cleanup incomplete; retained barrier and evidence: ${art}" >&2; exit 1; }
  fi
  if [[ "${registry_installed}" == true ]]; then
    "${k[@]}" -n ome get cm alfred-config --show-managed-fields -o json >"${art}/original-config-after.json" || exit 1
    jq -e --slurpfile old "${art}/original-config.json" '.metadata.uid==$old[0].metadata.uid and
      .data==$old[0].data and .metadata.managedFields==$old[0].metadata.managedFields' "${art}/original-config-after.json" >/dev/null || exit 1
    current="$("${k[@]}" -n ome get deployment ome-alfred -o json)" || exit 1
    patch="$(nd_deployment_patch "$(cat "${art}/deployment-before.json")" "${current}" "${registry_name}" "${policy_name}" restore)" || exit 1
    if [[ "${patch}" != '[]' ]]; then
      "${k[@]}" -n ome patch deployment ome-alfred --type=json -p "${patch}" -o json >"${art}/deployment-restored.json" || exit 1
      "${k[@]}" -n ome rollout status deployment/ome-alfred --timeout=180s >>"${art}/cleanup.log" 2>&1 || exit 1
    fi
  fi
  if [[ "${cordoned}" == true ]]; then "${k[@]}" uncordon alfred-kwok-gpu-c >/dev/null || rc=1; fi
  if [[ "${passed}" == true && "${rc}" == 0 ]]; then
    nd_created_objects "${art}/fixture-created.json" "${art}/registry-created.json" "${art}/config-before.json" >"${art}/cleanup-objects.jsonl" || exit 1
    # Only these exact created identities are disposable; keep namespace/journal.
    while IFS= read -r object; do
      uid="$(jq -er '.metadata.uid' <<<"${object}")"; name="$(jq -er '.metadata.name' <<<"${object}")"
      key="$(nd_cleanup_key <<<"${object}")" || exit 1
      namespace="$(jq -r '.metadata.namespace // ""' <<<"${object}")"
      case "$(jq -r '.kind' <<<"${object}")" in
        Pod) resource="/api/v1/namespaces/${namespace}/pods/${name}" ;;
        InferenceService) resource="/apis/ome.io/v1beta1/namespaces/${namespace}/inferenceservices/${name}" ;;
        ClusterServingRuntime) resource="/apis/ome.io/v1beta1/clusterservingruntimes/${name}" ;;
        ConfigMap) resource="/api/v1/namespaces/ome/configmaps/${name}" ;;
        *) echo 'Unexpected cleanup kind' >&2; exit 1 ;;
      esac
      jq -n --arg uid "${uid}" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}' >"${art}/delete-${key}.json"
      "${k[@]}" delete --raw "${resource}" -f "${art}/delete-${key}.json" >"${art}/deleted-${key}.json" || rc=1
    done <"${art}/cleanup-objects.jsonl"
    "${k[@]}" -n alfred-e2e wait --for=delete inferenceservice/single --timeout=90s >>"${art}/cleanup.log" 2>&1 || rc=1
    "${k[@]}" -n alfred-e2e wait --for=delete pods --all --timeout=90s >>"${art}/cleanup.log" 2>&1 || rc=1
  else
    ((rc!=0)) || rc=1
    echo 'Retaining failed no-benefit fixture and all raw evidence' >&2
  fi
  echo "no-benefit-defrag exit=${rc}; evidence: ${art}"
  exit "${rc}"
}
trap cleanup EXIT
"${k[@]}" get node alfred-e2e-control-plane -o json | jq -e '.metadata.labels["alfred-e2e/infrastructure"]=="true"' >/dev/null
"${k[@]}" get inferenceservices,inferencereplicas -A -o json | jq -e '.items|length==0' >/dev/null
"${k[@]}" -n alfred-e2e get pods -o json | jq -e '.items|length==0' >/dev/null
"${k[@]}" get nodes -l alfred-e2e/virtual=true -o json | jq -e '(.items|length)==4 and all(.items[];.spec.unschedulable!=true and .metadata.labels["maintenance.example.com/state"]==null)' >/dev/null
"${k[@]}" -n ome get cm alfred-config --show-managed-fields -o json >"${art}/original-config.json"
jq --arg name "${policy_name}" '{apiVersion:"v1",kind:"ConfigMap",metadata:{name:$name,namespace:"ome"},data}' "${art}/original-config.json" |
  "${k[@]}" create -f - -o json >"${art}/config-before.json"
jq -r '.data["config.yaml"]' "${art}/config-before.json" | yq -o=json '.' >"${art}/config-before-parsed.json"
jq -e '.mode=="execute" and .policies.defragmentation.enabled==false' "${art}/config-before-parsed.json" >/dev/null
jq '.policies.defragmentation|=(.enabled=true|.fragmentationThreshold=0.1|.scoring.sizeLadder=[8]|.scoring.sizePrior={"8":1}|.scoring.demandBlendLambda=1)' "${art}/config-before-parsed.json" >"${art}/enabled-config.json"
enabled="$(cat "${art}/enabled-config.json")"
"${k[@]}" -n ome get deployment ome-alfred -o json >"${art}/deployment-before.json"
normal_registry="$(jq -er '.spec.template.spec.volumes[]|select(.name=="simulation")|.configMap.name' "${art}/deployment-before.json")"
"${k[@]}" -n ome get cm "${normal_registry}" -o json >"${art}/registry-before.json"
profile="$(jq -cer '.data["workers.json"]|fromjson|[.workers[]|select(.identity.backend=="kind-default-v135")]|if length==1 then .[0].identity else error("missing profile") end' "${art}/registry-before.json")"
jq -n --slurpfile original "${art}/registry-before.json" --arg name "${registry_name}" --argjson profile "${profile}" '
  {apiVersion:"v1",kind:"ConfigMap",metadata:{name:$name,namespace:"ome"},immutable:true,data:$original[0].data}|
  .data["workers.json"]|=(fromjson|.workers[].binaryPath="/alfred-simulator-barrier"|tojson)|
  .data["barrier.json"]=({profile:$profile,namespace:"alfred-e2e",workload:"single"}|tojson)' |
  "${k[@]}" create -f - -o json >"${art}/registry-created.json"
patch="$(nd_deployment_patch "$(cat "${art}/deployment-before.json")" "$(cat "${art}/deployment-before.json")" "${registry_name}" "${policy_name}" install)"
registry_installed=true
"${k[@]}" -n ome patch deployment ome-alfred --type=json -p "${patch}" -o json >"${art}/deployment-barrier.json"
"${k[@]}" -n ome rollout status deployment/ome-alfred --timeout=180s >"${art}/barrier-rollout.log" 2>&1
deadline=$((SECONDS+60))
while true; do
  "${k[@]}" -n ome get pods -l control-plane=ome-alfred -o json >"${art}/alfred-pods.json"
  if jq -e '.items|length==1 and (.[0]|.metadata.deletionTimestamp==null and
    any(.status.conditions[]?;.type=="Ready" and .status=="True"))' "${art}/alfred-pods.json" >/dev/null; then break; fi
  ((SECONDS<deadline)) || { echo 'Alfred rollout did not settle to one ready Pod' >&2; exit 1; }
  sleep 0.5
done
alfred_pod="$(jq -er '.items|if length==1 then .[0].metadata.name else error("expected one Alfred Pod") end' "${art}/alfred-pods.json")"
alfred_uid="$(jq -er '.items[0].metadata.uid' "${art}/alfred-pods.json")"
cordoned=true
"${k[@]}" cordon alfred-kwok-gpu-c >/dev/null
"${k[@]}" create --dry-run=client -f "${dir}/manifests/workload-single.yaml" -o json |
  jq -s --arg runtime "${runtime}" '
  map(select(.kind!="Namespace")|if .kind=="ClusterServingRuntime" then .metadata.name=$runtime else
    .spec.runtime.name=$runtime|.spec.engine.affinity={nodeAffinity:{requiredDuringSchedulingIgnoredDuringExecution:{nodeSelectorTerms:[{matchExpressions:[
      {key:"kubernetes.io/hostname",operator:"In",values:["alfred-kwok-gpu-a","alfred-kwok-gpu-c"]}]}]}}} end) |
  .+[ ["b",7],["d",8] | . as $block |
    {apiVersion:"v1",kind:"Pod",metadata:{name:("no-benefit-block-"+$block[0]),namespace:"alfred-e2e",labels:{"alfred-e2e/blocker":"true"}},
     spec:{schedulerName:"alfred-default-scheduler",terminationGracePeriodSeconds:1,
       nodeSelector:{"alfred-e2e/virtual":"true","kubernetes.io/hostname":("alfred-kwok-gpu-"+$block[0])},
       tolerations:[{key:"alfred-e2e/virtual",operator:"Equal",value:"true",effect:"NoSchedule"}],
       containers:[{name:"pause",image:"registry.k8s.io/pause:3.10",resources:{requests:{"nvidia.com/gpu":($block[1]|tostring)},limits:{"nvidia.com/gpu":($block[1]|tostring)}}}]}}]|
  {apiVersion:"v1",kind:"List",items:.}' >"${art}/fixture.json"
"${k[@]}" create -f "${art}/fixture.json" -o json >"${art}/fixture-created.json"
deadline=$((SECONDS+90))
until "${k[@]}" -n alfred-e2e get pod single-engine-0-default-0 >/dev/null 2>&1; do ((SECONDS<deadline)); sleep 1; done
"${k[@]}" -n alfred-e2e wait pod --all --for=condition=Ready --timeout=90s >/dev/null
"${k[@]}" -n alfred-e2e get pod single-engine-0-default-0 -o json >"${art}/source.json"
jq -e '.spec.nodeName=="alfred-kwok-gpu-a"' "${art}/source.json" >/dev/null
service="single-engine-rev-$(jq -er '.metadata.labels["ome.io/revision-hash"]' "${art}/source.json")"
"${k[@]}" uncordon alfred-kwok-gpu-c >/dev/null
cordoned=false
echo 'Waiting for the configured recent-placement cooldown'
deadline=$((SECONDS+65)); while ((SECONDS<deadline)); do sleep 1; done
capture "${art}/baseline.json"
jq -n --argjson profile "${profile}" --arg service "${service}" --arg policy "${policy_name}" --slurpfile baseline "${art}/baseline.json" \
  --slurpfile source "${art}/source.json" --slurpfile config "${art}/enabled-config.json" '
  {scenario:"no-benefit-defrag",profile:$profile,policyName:$policy,routingService:$service,baseline:$baseline[0],source:$source[0],config:$config[0]}' >"${art}/context.json"
idle "${art}/baseline.json"
owner_uid="$(jq -er '.isvc.metadata.uid' "${art}/baseline.json")"
"${k[@]}" -n alfred-e2e get inferenceservice single --watch --request-timeout="$(churn_watch_timeout_seconds 360)s" -o json >"${art}/requests.jsonl" 2>"${art}/requests.stderr" &
request_pid=$!
"${k[@]}" -n alfred-e2e get pods --watch --output-watch-events --request-timeout="$(churn_watch_timeout_seconds 360)s" -o json >"${art}/pods.jsonl" 2>"${art}/pods.stderr" &
pod_pid=$!
deadline=$((SECONDS+15))
until [[ -s "${art}/requests.jsonl" && -s "${art}/pods.jsonl" ]]; do kill -0 "${request_pid}"; kill -0 "${pod_pid}"; ((SECONDS<deadline)); sleep 0.1; done
churn_watch_requests "${art}/requests.jsonl" "${owner_uid}" | jq -e 'length==0' >/dev/null
current="$("${k[@]}" -n ome get cm "${policy_name}" -o json)"
patch="$(nd_config_patch "$(cat "${art}/config-before.json")" "${current}" "${enabled}" install)"
configured=true
"${k[@]}" -n ome patch cm "${policy_name}" --type=json -p "${patch}" -o json >"${art}/config-enabled.json"
for index in 1 2 3; do
  echo "Waiting for real no-benefit preflight ${index}"
  prefix="${art}/barrier-${index}"
  "${k[@]}" --request-timeout=190s -n ome exec deployment/ome-alfred -c alfred -- /alfred-simulator-barrier --wait-held >"${prefix}-held.json"
  churn_decode_held "${prefix}-held.json" "${prefix}"
  capture "${prefix}-before-release.json"
  idle "${prefix}-before-release.json"
  jq -c '{nonce}' "${prefix}-held.json" >"${prefix}-release-request.json"
  "${k[@]}" -n ome exec -i deployment/ome-alfred -c alfred -- /alfred-simulator-barrier --release-held <"${prefix}-release-request.json" >"${prefix}-receipt.json"
  deadline=$((SECONDS+30)); observed=false; sample_index=0
  while ((SECONDS<deadline)); do
    kill -0 "${request_pid}"; kill -0 "${pod_pid}"
    churn_watch_requests "${art}/requests.jsonl" "${owner_uid}" | jq -e 'length==0' >/dev/null
    sample_index=$((sample_index+1)); sample="${prefix}-sample-${sample_index}.json"
    capture "${sample}"
    jq -c '.' "${sample}" >>"${prefix}-api.jsonl"
    idle "${sample}"
    if jq -e -L "${dir}" --slurpfile before "${prefix}-before-release.json" 'include "placement-pause-sample"; include "no-benefit-defrag-sample";
      nd_rejected and (pp_cycle.timestamp|pp_time)>($before[0]|pp_cycle.timestamp|pp_time)' "${sample}" >/dev/null; then
      jq '.' "${sample}" >"${prefix}-observed.json"; observed=true; break
    fi
    sleep 0.1
  done
  [[ "${observed}" == true ]] || { echo 'Missing exact no-benefit withholding outcome' >&2; exit 1; }
  jq -n --slurpfile held "${prefix}-held.json" --slurpfile q "${prefix}-request.json" --slurpfile r "${prefix}-result.json" \
    --slurpfile receipt "${prefix}-receipt.json" --slurpfile before "${prefix}-before-release.json" \
    --slurpfile observed "${prefix}-observed.json" --slurpfile samples "${prefix}-api.jsonl" '
    {held:$held[0],request:$q[0],result:$r[0],receipt:$receipt[0],beforeRelease:$before[0],observed:$observed[0],samples:$samples}' >"${prefix}-attempt.json"
  jq -e -L "${dir}" --slurpfile e "${art}/context.json" 'include "no-benefit-defrag-sample"; nd_attempt($e[0])' "${prefix}-attempt.json" >/dev/null
done
kill -0 "${request_pid}"; kill -0 "${pod_pid}"
kill "${request_pid}" "${pod_pid}"; wait "${request_pid}" 2>/dev/null || true; wait "${pod_pid}" 2>/dev/null || true
request_pid=''; pod_pid=''
jq -n --slurpfile context "${art}/context.json" --slurpfile a "${art}/barrier-1-attempt.json" \
  --slurpfile b "${art}/barrier-2-attempt.json" --slurpfile c "${art}/barrier-3-attempt.json" \
  --slurpfile requests "${art}/requests.jsonl" --slurpfile pods "${art}/pods.jsonl" '
  $context[0]+{attempts:[$a[0],$b[0],$c[0]],requestWatch:$requests,podWatch:$pods}' >"${art}/evidence.json"
bash "${dir}/verify-no-benefit-defrag.sh" "${art}"
passed=true
