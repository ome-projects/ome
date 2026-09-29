#!/usr/bin/env bash
# Positive control: a real pending 8-GPU Pod binds only after a real Alfred
# migration consolidates a 1-GPU OMENative source onto a 1-GPU destination hole.
set -euo pipefail
: "${STATE_DIR:?STATE_DIR is required}"
[[ "${STATE_DIR}" == /* && -f "${STATE_DIR}/kubeconfig" ]] || exit 2
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${dir}/namespace-churn-lib.sh"
repo="$(cd "${dir}/../.." && pwd)"
k=(kubectl --kubeconfig "${STATE_DIR}/kubeconfig" --context kind-alfred-e2e --request-timeout=15s)
h=(helm --kubeconfig "${STATE_DIR}/kubeconfig" --kube-context kind-alfred-e2e)
ns=alfred-e2e-defrag
runtime=alfred-e2e-defrag-runtime
art="${STATE_DIR}/artifacts/useful-defrag-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "${art}"
namespace_uid=''
runtime_uid=''
passed=false
cordoned=false
configured=false
watch_pid=''
cleanup() {
  local rc=$?
  if [[ -n "${watch_pid}" ]]; then kill "${watch_pid}" 2>/dev/null || true; wait "${watch_pid}" 2>/dev/null || true; fi
  "${k[@]}" -n "${ns}" get pods,inferencereplicas.ome.io,endpointslices -o json >"${art}/final-objects.json" 2>/dev/null || true
  "${k[@]}" -n ome get cm alfred-recommendations alfred-dispatch-state -o json >"${art}/alfred-final.json" 2>/dev/null || true
  if [[ "${configured}" == true ]]; then
    "${h[@]}" upgrade ome-alfred "${repo}/charts/ome-alfred" -n ome --reset-values -f "${art}/original-values.json" --wait --timeout=120s >"${art}/restore-config.log" 2>&1 || { echo 'restore failed; retaining fixtures' >&2; exit 1; }
  fi
  if [[ "${cordoned}" == true ]]; then "${k[@]}" uncordon alfred-kwok-gpu-b >/dev/null || rc=1; fi
  if [[ "${passed}" == true && "${rc}" == 0 ]]; then
    jq -n --arg uid "${namespace_uid}" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}' >"${art}/namespace-delete.json"
    "${k[@]}" delete --raw "/api/v1/namespaces/${ns}" -f "${art}/namespace-delete.json" >"${art}/namespace-deleted.json" || rc=1
    "${k[@]}" wait --for=delete namespace/"${ns}" --timeout=90s >>"${art}/cleanup.log" 2>&1 || rc=1
    jq -n --arg uid "${runtime_uid}" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}' >"${art}/runtime-delete.json"
    "${k[@]}" delete --raw "/apis/ome.io/v1beta1/clusterservingruntimes/${runtime}" -f "${art}/runtime-delete.json" >"${art}/runtime-deleted.json" || rc=1
    "${k[@]}" wait --for=delete clusterservingruntime/"${runtime}" --timeout=30s >>"${art}/cleanup.log" 2>&1 || rc=1
  else
    ((rc != 0)) || rc=1
    echo 'Retaining failed defrag fixtures and raw evidence' >&2
  fi
  echo "useful-defrag exit=${rc}; evidence: ${art}"
  exit "${rc}"
}
trap cleanup EXIT
[[ "$("${k[@]}" get node alfred-e2e-control-plane -o jsonpath='{.metadata.labels.alfred-e2e/infrastructure}')" == true ]]
if "${k[@]}" get namespace "${ns}" >/dev/null 2>&1 || "${k[@]}" get clusterservingruntime "${runtime}" >/dev/null 2>&1; then
  echo 'defrag fixture already exists' >&2; exit 1
fi
"${k[@]}" get inferenceservices.ome.io -A -o json | jq -e '.items|length == 0' >/dev/null
"${k[@]}" get nodes -l alfred-e2e/virtual=true -o json | jq -e '(.items|length)==4 and all(.items[]; .spec.unschedulable != true and .metadata.labels["maintenance.example.com/state"] == null)' >/dev/null
"${h[@]}" get values ome-alfred -n ome -o json >"${art}/original-values.json"
jq -e '.alfredConfig.policies.defragmentation.enabled == false' "${art}/original-values.json" >/dev/null
"${k[@]}" create namespace "${ns}" -o json >"${art}/namespace-created.json"
namespace_uid="$(jq -er '.metadata.uid' "${art}/namespace-created.json")"
blocker() {
  local suffix="$1" gpus="$2"
  jq -n --arg ns "${ns}" --arg node "alfred-kwok-gpu-${suffix}" --arg gpu "${gpus}" \
    '{apiVersion:"v1",kind:"Pod",metadata:{name:("block-"+$node),namespace:$ns,labels:{"alfred-e2e/blocker":"true"}},spec:{schedulerName:"alfred-default-scheduler",terminationGracePeriodSeconds:1,nodeSelector:{"kubernetes.io/hostname":$node,"alfred-e2e/virtual":"true"},tolerations:[{key:"alfred-e2e/virtual",operator:"Equal",value:"true",effect:"NoSchedule"}],containers:[{name:"pause",image:"registry.k8s.io/pause:3.10",resources:{requests:{"nvidia.com/gpu":$gpu},limits:{"nvidia.com/gpu":$gpu}}}]}}' | "${k[@]}" apply -f - >/dev/null
}
blocker b 7
blocker c 8
blocker d 8
"${k[@]}" -n "${ns}" wait pod -l alfred-e2e/blocker=true --for=condition=Ready --timeout=60s >/dev/null
cordoned=true
"${k[@]}" cordon alfred-kwok-gpu-b >/dev/null
"${k[@]}" create --dry-run=client -f "${dir}/manifests/workload-single.yaml" -o json | jq -s --arg ns "${ns}" --arg runtime "${runtime}" '
  map(select(.kind != "Namespace") | if .kind == "ClusterServingRuntime" then .metadata.name=$runtime
    else .metadata.namespace=$ns | .spec.runtime.name=$runtime end) | {apiVersion:"v1",kind:"List",items:.}' >"${art}/fixture.json"
"${k[@]}" apply -f "${art}/fixture.json" >/dev/null
"${k[@]}" get clusterservingruntime "${runtime}" -o json >"${art}/runtime-created.json"
runtime_uid="$(jq -er '.metadata.uid' "${art}/runtime-created.json")"
"${k[@]}" -n "${ns}" wait pod/single-engine-0-default-0 --for=condition=Ready --timeout=90s >/dev/null 2>&1 || {
  # The controller may not have created the Pod before the initial GET.
  deadline=$((SECONDS+90)); until "${k[@]}" -n "${ns}" get pod single-engine-0-default-0 >/dev/null 2>&1; do ((SECONDS<deadline)); sleep 1; done
  "${k[@]}" -n "${ns}" wait pod/single-engine-0-default-0 --for=condition=Ready --timeout=90s >/dev/null
}
"${k[@]}" -n "${ns}" get pod single-engine-0-default-0 -o json >"${art}/source.json"
source_uid="$(jq -er '.metadata.uid' "${art}/source.json")"
jq -e '.spec.nodeName == "alfred-kwok-gpu-a"' "${art}/source.json" >/dev/null
service="single-engine-rev-$(jq -r '.metadata.labels["ome.io/revision-hash"]' "${art}/source.json")"
"${k[@]}" uncordon alfred-kwok-gpu-b >/dev/null
cordoned=false
echo 'Holding source age and collecting a definitely blocked beneficiary baseline'
# This wait is the configured migration cooldown; no readiness is fabricated.
age_deadline=$((SECONDS+65))
while ((SECONDS<age_deadline)); do sleep 1; done
jq -n --arg ns "${ns}" '{apiVersion:"v1",kind:"Pod",metadata:{name:"beneficiary",namespace:$ns},spec:{schedulerName:"alfred-default-scheduler",terminationGracePeriodSeconds:1,nodeSelector:{"alfred-e2e/virtual":"true"},tolerations:[{key:"alfred-e2e/virtual",operator:"Equal",value:"true",effect:"NoSchedule"}],containers:[{name:"pause",image:"registry.k8s.io/pause:3.10",resources:{requests:{"nvidia.com/gpu":"8"},limits:{"nvidia.com/gpu":"8"}}}]}}' | "${k[@]}" apply -f - >/dev/null
deadline=$((SECONDS+60))
until "${k[@]}" -n "${ns}" get pod beneficiary -o json | jq -e '.spec.nodeName == null and any(.status.conditions[]?; .type=="PodScheduled" and .status=="False" and .reason=="Unschedulable" and (.message|contains("Insufficient nvidia.com/gpu")))' >/dev/null; do ((SECONDS<deadline)); sleep 1; done
capture() {
  local file="$1" prefix="${1%.json}"
  # Read beneficiary before routing so a newly bound beneficiary is checked
  # against subsequent replacement observations, not stale pre-handoff data.
  "${k[@]}" -n "${ns}" get pod beneficiary -o json >"${prefix}-beneficiary.json"
  "${k[@]}" get pods -A -o json >"${prefix}-all-pods.json"
  "${k[@]}" get nodes -l alfred-e2e/virtual=true -o json >"${prefix}-nodes.json"
  "${k[@]}" -n "${ns}" get endpointslices -l "kubernetes.io/service-name=${service}" -o json >"${prefix}-endpoints.json"
  "${k[@]}" -n "${ns}" get inferenceservice single -o json >"${prefix}-isvc.json"
  "${k[@]}" -n "${ns}" get inferencereplica single-engine -o json >"${prefix}-ir.json"
  "${k[@]}" -n ome get cm alfred-recommendations alfred-dispatch-state -o json >"${prefix}-alfred.json"
  jq -n --arg ns "${ns}" --slurpfile beneficiary "${prefix}-beneficiary.json" --slurpfile pods "${prefix}-all-pods.json" \
    --slurpfile nodes "${prefix}-nodes.json" --slurpfile endpoints "${prefix}-endpoints.json" --slurpfile owner "${prefix}-isvc.json" \
    --slurpfile ir "${prefix}-ir.json" --slurpfile alfred "${prefix}-alfred.json" '
    {beneficiary:$beneficiary[0],allPods:$pods[0],nodes:$nodes[0],endpoints:$endpoints[0],isvc:$owner[0],ir:$ir[0],
     pods:{items:[$pods[0].items[]|select(.metadata.namespace==$ns and .metadata.labels["ome.io/inferenceservice"]=="single")]},
     dispatch:([$alfred[0].items[]|select(.metadata.name=="alfred-dispatch-state")][0]),
     recommendations:([$alfred[0].items[]|select(.metadata.name=="alfred-recommendations")][0])}' >"${file}"
}
capture "${art}/baseline.json"
owner_uid="$(jq -er '.isvc.metadata.uid' "${art}/baseline.json")"
jq '.beneficiary' "${art}/baseline.json" >"${art}/beneficiary-before.json"
jq -L "${dir}" 'include "useful-defrag-sample"; df_capacity(.nodes;.allPods)' "${art}/baseline.json" >"${art}/capacity-before.json"
jq -e '(sort_by(.name)|map(.free))==[7,1,0,0]' "${art}/capacity-before.json" >/dev/null
"${k[@]}" -n "${ns}" get inferenceservice single --watch --request-timeout="$(churn_watch_timeout_seconds 420)s" -o json >"${art}/requests.jsonl" 2>"${art}/watch.stderr" &
watch_pid=$!
deadline=$((SECONDS+30))
until [[ -s "${art}/requests.jsonl" ]]; do kill -0 "${watch_pid}"; ((SECONDS<deadline)); sleep 0.1; done
churn_watch_requests "${art}/requests.jsonl" "${owner_uid}" | jq -e 'length==0' >/dev/null
configured=true
"${h[@]}" upgrade ome-alfred "${repo}/charts/ome-alfred" -n ome --reuse-values --set alfredConfig.policies.defragmentation.enabled=true --set-json alfredConfig.policies.defragmentation.fragmentationThreshold=0.1 --wait --timeout=120s >"${art}/enable-defrag.log" 2>&1
"${h[@]}" get values ome-alfred -n ome -o json >"${art}/enabled-values.json"
jq -e '.alfredConfig.policies.defragmentation.enabled == true and .alfredConfig.policies.defragmentation.fragmentationThreshold == 0.1' "${art}/enabled-values.json" >/dev/null
echo 'Waiting for Alfred defragmentation request and real replacement placement'
deadline=$((SECONDS+240)); replacement_uid=''; sample_index=0; completed=0; cycle=''
while ((SECONDS<deadline)); do
  if ! kill -0 "${watch_pid}" 2>/dev/null; then
    echo 'InferenceService watch exited before defragmentation observation completed' >&2
    exit 1
  fi
  churn_watch_requests "${art}/requests.jsonl" "${owner_uid}" >"${art}/current-requests.json"
  jq -e 'length<=1' "${art}/current-requests.json" >/dev/null
  sample_index=$((sample_index+1))
  sample="${art}/sample-${sample_index}.json"
  capture "${sample}"
  if [[ -z "${replacement_uid}" ]]; then
    jq -e -L "${dir}" --arg uid "${source_uid}" --arg service "${service}" 'include "placement-pause-sample";
      pp_pod($uid;true) and pp_endpoint($uid;$service)' "${sample}" >/dev/null
    jq -e --slurpfile base "${art}/baseline.json" '
      .beneficiary as $pod | $base[0].beneficiary as $before |
      $pod.metadata.uid==$before.metadata.uid and $pod.metadata.deletionTimestamp==null and
      $pod.spec==$before.spec and $pod.spec.nodeName==null' "${sample}" >/dev/null
    if jq -e 'length==1' "${art}/current-requests.json" >/dev/null &&
      jq -e 'any(.pods.items[];.metadata.labels["ome.io/instance-index"]=="1" and .spec.nodeName=="alfred-kwok-gpu-b")' "${sample}" >/dev/null; then
      jq '.[0]|{uuid:(.key|sub("^ome.io/migration-request-v1-";"")),payload:(.value|fromjson)}' "${art}/current-requests.json" >"${art}/request.json"
      jq -e '.payload.requested_by=="alfred" and .payload.reason=="Fragmentation" and .payload.from_node=="alfred-kwok-gpu-a"' "${art}/request.json" >/dev/null
      jq -n --arg service "${service}" --slurpfile source "${art}/source.json" --slurpfile baseline "${art}/baseline.json" \
        --slurpfile held "${sample}" --slurpfile request "${art}/request.json" '
        {source:$source[0],replacement:([$held[0].pods.items[]|select(.metadata.labels["ome.io/instance-index"]=="1")][0]),
         baseline:$baseline[0],podsBefore:$baseline[0].allPods,before:$baseline[0].beneficiary,routingService:$service,request:$request[0]}' >"${art}/context.json"
      # Binding may precede KWOK Ready=False or the IR's SurgePending update.
      # Keep every safe raw sample and wait for the complete witness, unchanged.
      if jq -e -L "${dir}" --slurpfile e "${art}/context.json" 'include "useful-defrag-sample"; df_held($e[0])' "${sample}" >/dev/null; then
        jq '.' "${sample}" >"${art}/held.json"
        replacement_uid="$(jq -er '.replacement.metadata.uid' "${art}/context.json")"
        "${k[@]}" -n "${ns}" annotate pod single-engine-1-default-0 alfred-e2e.ome.io/readiness=immediate --overwrite >/dev/null
      fi
    fi
  else
    jq -c '.' "${sample}" >>"${art}/handoff.jsonl"
    jq -e -L "${dir}" --slurpfile e "${art}/context.json" 'include "useful-defrag-sample"; df_handoff($e[0])' "${sample}" >/dev/null
    if jq -e -L "${dir}" --slurpfile e "${art}/context.json" 'include "useful-defrag-sample";
      df_completed($e[0]+{migration:.ir.status.migrations[0]})' "${sample}" >/dev/null; then
      if [[ -z "${cycle}" ]]; then
        jq '.' "${sample}" >"${art}/completion-baseline.json"
        cycle="$(jq -er '.recommendations.data["last-cycle.json"]|fromjson|.timestamp' "${sample}")"
      else
        jq -c '.' "${sample}" >>"${art}/completion-samples.jsonl"
        if jq -e -L "${dir}" --arg previous "${cycle}" --slurpfile e "${art}/context.json" 'include "useful-defrag-sample";
          df_new_cycle($e[0];$previous)' "${sample}" >/dev/null; then
          jq -c '.' "${sample}" >>"${art}/completed.jsonl"
          cycle="$(jq -er '.recommendations.data["last-cycle.json"]|fromjson|.timestamp' "${sample}")"
          completed=$((completed+1))
        fi
      fi
    elif [[ -n "${cycle}" ]]; then
      echo 'Completed migration lost its verified safety or journal state' >&2; exit 1
    fi
    if ((completed==3)); then
      kill -0 "${watch_pid}"
      kill "${watch_pid}"; wait "${watch_pid}" 2>/dev/null || true; watch_pid=''
      [[ ! -s "${art}/watch.stderr" ]] || { echo 'Watch reported an error; retaining evidence' >&2; exit 1; }
      churn_watch_requests "${art}/requests.jsonl" "${owner_uid}" | jq -e 'length==1' >/dev/null
      jq -n --slurpfile context "${art}/context.json" --slurpfile held "${art}/held.json" --slurpfile handoff "${art}/handoff.jsonl" \
        --slurpfile baseline "${art}/completion-baseline.json" --slurpfile samples "${art}/completion-samples.jsonl" \
        --slurpfile completed "${art}/completed.jsonl" --slurpfile watch "${art}/requests.jsonl" --slurpfile capacity "${art}/capacity-before.json" '
        $context[0]+{scenario:"useful-defrag",held:$held[0],handoff:$handoff,completionBaseline:$baseline[0],completionSamples:$samples,
          completed:$completed,requestWatch:$watch,capacityBefore:$capacity[0],nodesBefore:$context[0].baseline.nodes,
          nodesAfter:$completed[-1].nodes,podsAfter:$completed[-1].allPods,after:$completed[-1].beneficiary,
          migration:$completed[-1].ir.status.migrations[0]}' >"${art}/evidence.json"
      jq -e -L "${dir}" -f "${dir}/verify-useful-defrag.jq" "${art}/evidence.json" >/dev/null
      passed=true
      echo 'PASS useful-defrag: unchanged beneficiary bound after safe migration; three completed cycles verified'
      exit 0
    fi
  fi
  sleep 0.5
done
echo 'No verified useful defragmentation completed before deadline' >&2
exit 1
