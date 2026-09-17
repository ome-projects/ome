#!/usr/bin/env bash
# Positive control: a real pending 8-GPU Pod binds only after a real Alfred
# migration consolidates a 1-GPU OMENative source onto a 1-GPU destination hole.
set -euo pipefail
: "${STATE_DIR:?STATE_DIR is required}"
[[ "${STATE_DIR}" == /* && -f "${STATE_DIR}/kubeconfig" ]] || exit 2
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${dir}/../.." && pwd)"
k=(kubectl --kubeconfig "${STATE_DIR}/kubeconfig" --context kind-alfred-e2e --request-timeout=15s)
h=(helm --kubeconfig "${STATE_DIR}/kubeconfig" --kube-context kind-alfred-e2e)
ns=alfred-e2e-defrag
runtime=alfred-e2e-defrag-runtime
art="${STATE_DIR}/artifacts/useful-defrag-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "${art}"
created=false
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
  if [[ "${created}" == true ]]; then
    "${k[@]}" delete namespace "${ns}" --wait=true --timeout=90s >"${art}/cleanup.log" 2>&1 || rc=1
    "${k[@]}" delete clusterservingruntime "${runtime}" --wait=true --timeout=30s >>"${art}/cleanup.log" 2>&1 || rc=1
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
"${k[@]}" create namespace "${ns}" >/dev/null
created=true
blocker() {
  local suffix="$1" gpus="$2"
  jq -n --arg ns "${ns}" --arg node "alfred-kwok-gpu-${suffix}" --arg gpu "${gpus}" \
    '{apiVersion:"v1",kind:"Pod",metadata:{name:("block-"+$node),namespace:$ns,labels:{"alfred-e2e/blocker":"true"}},spec:{schedulerName:"alfred-default-scheduler",terminationGracePeriodSeconds:1,nodeSelector:{"kubernetes.io/hostname":$node,"alfred-e2e/virtual":"true"},tolerations:[{key:"alfred-e2e/virtual",operator:"Equal",value:"true",effect:"NoSchedule"}],containers:[{name:"pause",image:"registry.k8s.io/pause:3.10",resources:{requests:{"nvidia.com/gpu":$gpu},limits:{"nvidia.com/gpu":$gpu}}}]}}' | "${k[@]}" apply -f - >/dev/null
}
blocker b 7
blocker c 8
blocker d 8
"${k[@]}" -n "${ns}" wait pod -l alfred-e2e/blocker=true --for=condition=Ready --timeout=60s >/dev/null
"${k[@]}" cordon alfred-kwok-gpu-b >/dev/null
cordoned=true
"${k[@]}" create --dry-run=client -f "${dir}/manifests/workload-single.yaml" -o json | jq -s --arg ns "${ns}" --arg runtime "${runtime}" '
  map(select(.kind != "Namespace") | if .kind == "ClusterServingRuntime" then .metadata.name=$runtime
    else .metadata.namespace=$ns | .spec.runtime.name=$runtime end) | {apiVersion:"v1",kind:"List",items:.}' >"${art}/fixture.json"
"${k[@]}" apply -f "${art}/fixture.json" >/dev/null
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
"${k[@]}" -n "${ns}" get pod beneficiary -o json >"${art}/beneficiary-before.json"
"${k[@]}" get nodes -l alfred-e2e/virtual=true -o json >"${art}/nodes-before.json"
"${k[@]}" get pods -A -o json >"${art}/pods-before.json"
jq -n --slurpfile nodes "${art}/nodes-before.json" --slurpfile pods "${art}/pods-before.json" '
  [$nodes[0].items[] | . as $node | {name:.metadata.name,free:((.status.allocatable["nvidia.com/gpu"]|tonumber)-
    ([$pods[0].items[] | select(.spec.nodeName==$node.metadata.name and .status.phase!="Succeeded" and .status.phase!="Failed") | .spec.containers[] | (.resources.requests["nvidia.com/gpu"]//"0"|tonumber)]|add//0))}]' >"${art}/capacity-before.json"
jq -e '(map(.free)|sort)==[0,0,1,7]' "${art}/capacity-before.json" >/dev/null
"${k[@]}" -n "${ns}" get inferenceservice single --watch --request-timeout=420s -o json >"${art}/requests.jsonl" 2>"${art}/watch.stderr" &
watch_pid=$!
configured=true
"${h[@]}" upgrade ome-alfred "${repo}/charts/ome-alfred" -n ome --reuse-values --set alfredConfig.policies.defragmentation.enabled=true --set-json alfredConfig.policies.defragmentation.fragmentationThreshold=0.1 --wait --timeout=120s >"${art}/enable-defrag.log" 2>&1
"${h[@]}" get values ome-alfred -n ome -o json >"${art}/enabled-values.json"
jq -e '.alfredConfig.policies.defragmentation.enabled == true and .alfredConfig.policies.defragmentation.fragmentationThreshold == 0.1' "${art}/enabled-values.json" >/dev/null
echo 'Waiting for Alfred defragmentation request and real replacement placement'
deadline=$((SECONDS+180)); request=''; replacement_uid=''
while ((SECONDS<deadline)); do
  if ! kill -0 "${watch_pid}" 2>/dev/null; then
    echo 'InferenceService watch exited before defragmentation observation completed' >&2
    exit 1
  fi
  request="$(jq -cs '[.[]|.metadata.annotations//{}|to_entries[]|select(.key|startswith("ome.io/migration-request-v1-"))|{uuid:(.key|sub("^ome.io/migration-request-v1-";"")),payload:(.value|fromjson)}]|unique_by(.uuid)|if length==1 then .[0] else empty end' "${art}/requests.jsonl" 2>/dev/null || true)"
  "${k[@]}" -n "${ns}" get pods -l ome.io/inferenceservice=single -o json >"${art}/current-pods.json"
  "${k[@]}" -n "${ns}" get endpointslices -l "kubernetes.io/service-name=${service}" -o json >"${art}/current-endpoints.json"
  if [[ -z "${replacement_uid}" ]]; then
    jq -e --arg uid "${source_uid}" 'any(.items[]; .metadata.uid==$uid and .metadata.deletionTimestamp==null and any(.status.conditions[]?;.type=="Ready" and .status=="True") and any(.status.conditions[]?;.type=="ome.io/serving" and .status=="True"))' "${art}/current-pods.json" >/dev/null
    jq -e --arg uid "${source_uid}" 'any(.items[].endpoints[]?; .targetRef.uid==$uid and .conditions.ready==true and .conditions.terminating!=true)' "${art}/current-endpoints.json" >/dev/null
    if [[ -n "${request}" ]] && jq -e 'any(.items[];.metadata.labels["ome.io/instance-index"]=="1" and .spec.nodeName=="alfred-kwok-gpu-b")' "${art}/current-pods.json" >/dev/null; then
      jq -e '(.items|length)==2 and any(.items[]; .metadata.labels["ome.io/instance-index"]=="1" and .metadata.deletionTimestamp==null and all(.status.conditions[]?; (.type!="Ready" and .type!="ome.io/serving") or .status!="True"))' "${art}/current-pods.json" >/dev/null
      "${k[@]}" -n "${ns}" get pod beneficiary -o json >"${art}/beneficiary-held.json"
      jq -e --arg uid "$(jq -r '.metadata.uid' "${art}/beneficiary-before.json")" '.metadata.uid==$uid and .spec.nodeName==null and .metadata.deletionTimestamp==null' "${art}/beneficiary-held.json" >/dev/null
      jq -e '.payload.requested_by=="alfred" and .payload.reason=="Fragmentation" and .payload.from_node=="alfred-kwok-gpu-a"' <<<"${request}" >/dev/null
      replacement_uid="$(jq -r '.items[]|select(.metadata.labels["ome.io/instance-index"]=="1")|.metadata.uid' "${art}/current-pods.json")"
      printf '%s\n' "${request}" >"${art}/request.json"
      "${k[@]}" -n "${ns}" annotate pod single-engine-1-default-0 alfred-e2e.ome.io/readiness=immediate --overwrite >/dev/null
    fi
  else
    jq -n --arg source "${source_uid}" --arg replacement "${replacement_uid}" --arg service "${service}" --slurpfile pods "${art}/current-pods.json" --slurpfile endpoints "${art}/current-endpoints.json" \
      '{pods:$pods[0],endpoints:$endpoints[0],routingService:$service,sourceUIDs:[$source],replacementUIDs:[$replacement],sourceRoutingUID:$source,replacementRoutingUID:$replacement}' | jq -c -f "${dir}/handoff-snapshot.jq" >>"${art}/handoff.jsonl"
    tail -n 1 "${art}/handoff.jsonl" | jq -e '.sourceSafe or .replacementSafe' >/dev/null
    "${k[@]}" -n "${ns}" get inferencereplica single-engine -o json >"${art}/current-ir.json"
    "${k[@]}" -n "${ns}" get pod beneficiary -o json >"${art}/beneficiary-after.json"
    if jq -e --arg uuid "$(jq -r '.uuid' <<<"${request}")" '(.status.migrations|length)==1 and .status.migrations[0].requestUUID==$uuid and .status.migrations[0].phase=="Completed"' "${art}/current-ir.json" >/dev/null &&
      jq -e '.spec.nodeName=="alfred-kwok-gpu-a" and any(.status.conditions[]?;.type=="Ready" and .status=="True")' "${art}/beneficiary-after.json" >/dev/null; then
      "${k[@]}" -n "${ns}" get pods -l ome.io/inferenceservice=single -o json >"${art}/current-pods.json"
      jq -e --arg uid "${source_uid}" 'all(.items[];.metadata.uid!=$uid)' "${art}/current-pods.json" >/dev/null
      jq -n --slurpfile before "${art}/beneficiary-before.json" --slurpfile after "${art}/beneficiary-after.json" --slurpfile capacity "${art}/capacity-before.json" --slurpfile request "${art}/request.json" --slurpfile ir "${art}/current-ir.json" --slurpfile handoff "${art}/handoff.jsonl" \
        '{scenario:"useful-defrag",before:$before[0],after:$after[0],capacityBefore:$capacity[0],request:$request[0],migration:$ir[0].status.migrations[0],handoff:$handoff}' >"${art}/evidence.json"
      jq -e -f "${dir}/verify-useful-defrag.jq" "${art}/evidence.json" >/dev/null
      echo 'PASS useful-defrag: same pending Pod now scheduled on vacated source node'
      exit 0
    fi
  fi
  sleep 0.5
done
echo 'No verified useful defragmentation completed before deadline' >&2
exit 1
