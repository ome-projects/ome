#!/usr/bin/env bash
# Fixture and observation helpers for tpu-defrag.sh. Functions that read the
# cluster use the caller's k, ns, art and fence_key.

TPU_NODE_LABEL=alfred-e2e/virtual-tpu
TPU_POD=alfred-tpu-pod-a
TPU_NODES=(alfred-kwok-tpu-0 alfred-kwok-tpu-1 alfred-kwok-tpu-2 alfred-kwok-tpu-3
  alfred-kwok-tpu-4 alfred-kwok-tpu-5 alfred-kwok-tpu-6 alfred-kwok-tpu-7)

# tpu_wait SECONDS WHAT COMMAND... polls COMMAND once a second.
tpu_wait() {
  local timeout="$1" what="$2"
  shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || { echo "timed out after ${timeout}s waiting for ${what}" >&2; return 1; }
    sleep 1
  done
}

# tpu_stages KWOK_STAGES prints the harness's node and pod lifecycle stages
# re-scoped to the TPU hosts, with four TPU chips in place of eight GPUs. The
# GPU health stages are not copied.
tpu_stages() {
  NODES="$(printf '%s\n' "${TPU_NODES[@]}" | jq -R . | jq -sc .)" LABEL="${TPU_NODE_LABEL}" yq '
    select(.kind == "Stage" and (.spec.resourceRef.kind == "Pod" or
      .metadata.name == "alfred-e2e-node-initialize" or .metadata.name == "alfred-e2e-node-heartbeat"))
    | .metadata.name += "-tpu"
    | .metadata.labels[strenv(LABEL)] = "true"
    | (.spec.selector.matchExpressions[] | select(.key == ".metadata.name" or .key == ".spec.nodeName")).values = env(NODES)
    | with(select(.spec.next.statusTemplate != null);
        .spec.next.statusTemplate |= sub("nvidia.com/gpu: \"8\""; "google.com/tpu: \"4\""))
  ' "$1"
}

# tpu_nodes INFRA_IP prints the TPU hosts as a List: one TPU pod of eight
# hosts holding two 2x2x4 partitions (hosts 0-3 and 4-7), four 2x2x2 and
# eight 2x2x1 partitions, all healthy and provision-only.
tpu_nodes() {
  local infra_ip="$1" i items=()
  for i in "${!TPU_NODES[@]}"; do
    items+=("$(jq -cn --arg name "${TPU_NODES[$i]}" --arg ip "${infra_ip%.*}.$((211 + i))" --arg pod "${TPU_POD}" \
      --arg p "alfred-tpu-p${i}" --arg d "alfred-tpu-d$((i / 2))" --arg q "alfred-tpu-q$((i / 4))" --arg virtual "${TPU_NODE_LABEL}" '
      {apiVersion: "v1", kind: "Node",
       metadata: {name: $name,
         annotations: {"kwok.x-k8s.io/node": "fake", "alfred-e2e.ome.io/internal-ip": $ip},
         labels: {($virtual): "true", "kubernetes.io/arch": "arm64", "kubernetes.io/hostname": $name,
           "kubernetes.io/os": "linux",
           "cloud.google.com/gke-tpu-accelerator": "tpu7x",
           "cloud.google.com/gke-accelerator-topology-mode": "PROVISION_ONLY",
           "cloud.google.com/gce-topology-block": $pod,
           "cloud.google.com/gke-tpu-partition-2x2x1-id": $p,
           "cloud.google.com/gke-tpu-partition-2x2x1-state": "HEALTHY",
           "cloud.google.com/gke-tpu-partition-2x2x2-id": $d,
           "cloud.google.com/gke-tpu-partition-2x2x2-state": "HEALTHY",
           "cloud.google.com/gke-tpu-partition-2x2x4-id": $q,
           "cloud.google.com/gke-tpu-partition-2x2x4-state": "HEALTHY"}},
       spec: {taints: [{key: $virtual, value: "true", effect: "NoSchedule"}]}}')")
  done
  printf '%s\n' "${items[@]}" | jq -s '{apiVersion: "v1", kind: "List", items: .}'
}

# ready_pod ISVC prints "name node uid" of the InferenceService's only ready,
# undeleted pod, and fails otherwise.
ready_pod() {
  "${k[@]}" -n "${ns}" get pods -l "ome.io/inferenceservice=$1" -o json | jq -er '
    [.items[] | select(.metadata.deletionTimestamp == null and
      any(.status.conditions[]?; .type == "Ready" and .status == "True"))] |
    if length == 1 then .[0] | "\(.metadata.name) \(.spec.nodeName) \(.metadata.uid)" else error("not one ready pod") end'
}

# replaced ISVC OLD_UID: the InferenceService runs exactly one pod, ready, and
# it is not OLD_UID.
replaced() {
  "${k[@]}" -n "${ns}" get pods -l "ome.io/inferenceservice=$1" -o json | jq -e --arg old "$2" '
    (.items | length) == 1 and (.items[0] | .metadata.uid != $old and .metadata.deletionTimestamp == null and
      any(.status.conditions[]?; .type == "Ready" and .status == "True"))' >/dev/null
}

capture() {
  local tmp="${art}/.capture"
  "${k[@]}" get nodes -l "${TPU_NODE_LABEL}=true" -o json >"${tmp}-nodes"
  "${k[@]}" get slices.accelerator.gke.io -o json >"${tmp}-slices"
  "${k[@]}" -n "${ns}" get pods -o json >"${tmp}-pods"
  jq -n --slurpfile n "${tmp}-nodes" --slurpfile s "${tmp}-slices" --slurpfile p "${tmp}-pods" \
    '{nodes: $n[0], slices: $s[0], pods: $p[0]}' >"${art}/$1.json"
  rm -f "${tmp}"-*
}

# fence_list prints the sorted names of the TPU hosts carrying Alfred's fence.
fence_list() {
  "${k[@]}" get nodes -l "${TPU_NODE_LABEL}=true" -o json |
    jq -ce --arg key "${fence_key}" '[.items[] | select(.metadata.annotations[$key] != null) | .metadata.name] | sort'
}

# fence_is JSON: exactly these hosts carry the fence, and all are cordoned.
fence_is() {
  "${k[@]}" get nodes -l "${TPU_NODE_LABEL}=true" -o json | jq -e --arg key "${fence_key}" --argjson want "$1" '
    ([.items[] | select(.metadata.annotations[$key] != null) | .metadata.name] | sort) == ($want | sort) and
    all(.items[] | select(.metadata.annotations[$key] != null); .spec.unschedulable == true)' >/dev/null
}

no_fences() {
  "${k[@]}" get nodes -l "${TPU_NODE_LABEL}=true" -o json | jq -e --arg key "${fence_key}" '
    all(.items[]; .metadata.annotations[$key] == null and .spec.unschedulable != true)' >/dev/null
}

no_slices() {
  "${k[@]}" get slices.accelerator.gke.io -o json | jq -e '.items | length == 0' >/dev/null
}

no_slice_labels() {
  "${k[@]}" get nodes -l "${TPU_NODE_LABEL}=true" -o json |
    jq -e 'all(.items[]; .metadata.labels["cloud.google.com/gke-tpu-slice"] == null)' >/dev/null
}

# request_uuid ISVC prints the one migration request UUID the watch observed
# on the InferenceService. OME may consume the annotation before a later GET,
# so only the watch is trusted.
request_uuid() {
  jq -ers --arg name "$1" '
    [.[] | select(.metadata.name == $name) | .metadata.annotations // {} | keys[] |
      select(startswith("ome.io/migration-request-v1-")) | sub("^ome.io/migration-request-v1-"; "")] |
    unique | if length == 1 then .[0] else error("not one request") end' "${art}/isvc-watch.jsonl"
}

# request_after_settle ISVC UUID FROM_NODE SETTLE_SECONDS: the request names
# FROM_NODE with reason TPUFragmentation and was made at least SETTLE_SECONDS
# after every host of the drain was fenced.
request_after_settle() {
  jq -ne --arg name "$1" --arg uuid "$2" --arg from "$3" --argjson settle "$4" --arg key "${fence_key}" \
    --slurpfile fenced "${art}/fenced.json" '
    def ts: capture("^(?<s>[^.Z]+)(?<f>[.][0-9]+)?Z$") | (.s + "Z" | fromdateiso8601) + ("0" + (.f // ".0") | tonumber);
    ([inputs | select(.metadata.name == $name) | .metadata.annotations["ome.io/migration-request-v1-" + $uuid] // empty] |
      first | fromjson) as $req |
    ([$fenced[0].nodes.items[] | .metadata.annotations[$key] // empty | fromjson | .fencedAt | ts] | max) as $fencedAt |
    $req.reason == "TPUFragmentation" and $req.from_node == $from and ($req.requested_at | ts) >= $fencedAt + $settle' \
    "${art}/isvc-watch.jsonl" >/dev/null
}

# request_count prints how many distinct migration requests the watch saw.
request_count() {
  jq -s '[.[] | .metadata.annotations // {} | keys[] | select(startswith("ome.io/migration-request-v1-"))] |
    unique | length' "${art}/isvc-watch.jsonl"
}

# migration_completed ISVC UUID: the InferenceReplica reports the request
# Completed. A Failed request ends the run.
migration_completed() {
  local phase
  phase="$("${k[@]}" -n "${ns}" get inferencereplica "$1-engine" -o json |
    jq -r --arg uuid "$2" '[.status.migrations[]? | select(.requestUUID == $uuid)][0].phase // ""')"
  if [[ "${phase}" == Failed ]]; then
    echo "OME reported migration $2 Failed" >&2
    exit 1
  fi
  [[ "${phase}" == Completed ]]
}

alfred_pod() {
  "${k[@]}" -n ome get pods -l control-plane=ome-alfred -o json | jq -er '
    [.items[] | select(.metadata.deletionTimestamp == null and any(.status.conditions[]?; .type == "Ready" and .status == "True"))] |
    if length == 1 then .[0].metadata.name else error("not one ready Alfred pod") end'
}

metrics() {
  "${k[@]}" get --raw "/api/v1/namespaces/ome/pods/$(alfred_pod):8080/proxy/metrics"
}

# metric_is SERIES VALUE: the exposition has SERIES with exactly VALUE.
metric_is() {
  metrics | awk -v series="$1" -v want="$2" '$1 == series { found = 1; ok = ($2 + 0 == want + 0) } END { exit !(found && ok) }'
}

# drain_counts prints every alfred_tpu_drains_total series, sorted.
drain_counts() {
  metrics | grep '^alfred_tpu_drains_total' | sort || true
}

last_cycle() {
  "${k[@]}" -n ome get configmap alfred-recommendations -o jsonpath='{.data.last-cycle\.json}' | jq -r '.timestamp'
}

# cycles_after TIMESTAMP N waits for N decision cycles completed after
# TIMESTAMP.
cycles_after() {
  local previous="$1" want="$2" seen=0 current deadline=$((SECONDS + 120))
  while ((seen < want)); do
    ((SECONDS < deadline)) || { echo "timed out waiting for ${want} decision cycles" >&2; return 1; }
    current="$(last_cycle 2>/dev/null || true)"
    if [[ -n "${current}" && "${current}" != "${previous}" ]]; then
      seen=$((seen + 1))
      previous="${current}"
    fi
    sleep 0.5
  done
}
