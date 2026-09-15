#!/usr/bin/env bash
# Sourced by gang.sh only for the partial-restart variant. All observations
# come from its explicit private API context and controller-owned objects.
partial_gang_restart() {
  local leader worker old_pod old_holder current_pod holder current_request
  local partial_deadline recovery=''
  leader="$(jq -r '.pods[] | select(.runner == "leader") | .name' <<<"${replacement}")"
  worker="$(jq -r '.pods[] | select(.runner == "worker") | .name' <<<"${replacement}")"
  "${kube[@]}" -n "${namespace}" annotate pod "${leader}" alfred-e2e.ome.io/readiness=immediate --overwrite >/dev/null
  "${kube[@]}" -n "${namespace}" wait "pod/${leader}" --for=condition=ContainersReady --timeout=30s >/dev/null
  old_pod="$("${kube[@]}" -n ome get pods -l control-plane=ome-controller-manager -o json |
    jq -er '[.items[] | select(.metadata.deletionTimestamp == null)] | if length == 1 then .[0].metadata.uid else error("expected one manager") end')"
  old_holder="$("${kube[@]}" -n ome get lease ome-controller-manager-leader-lock -o jsonpath='{.spec.holderIdentity}')"
  echo 'Restarting real OME manager with only replacement leader containers ready'
  "${kube[@]}" -n ome rollout restart deployment/ome-controller-manager >"${artifact_dir}/manager-restart.txt"
  # The configured 60s manager lease outlives the old process; include
  # graceful shutdown and retry jitter without exceeding the 2m surge budget.
  partial_deadline=$((SECONDS + 105))
  while (( SECONDS < partial_deadline )); do
    observe
    source_safe || { echo 'source gang draining or unrouted during partial replacement/restart' >&2; return 1; }
    current_request="$(jq -ce --arg uuid "${uuid}" '[.status.migrations[]?] |
      if length == 1 and .[0].requestUUID == $uuid and .[0].phase == "SurgePending" then .[0]
      else error("partial gang must retain exactly one SurgePending request") end' <<<"${ir}")"
    jq -e --arg leader "${leader}" --arg worker "${worker}" --argjson uids "${replacement_uids}" '
      [.items[] | select(.metadata.labels["ome.io/instance-index"] == "1")] as $surge |
      ($surge | map(.metadata.uid) | sort) == ($uids | sort) and
      all($surge[]; .metadata.deletionTimestamp == null) and
      any($surge[]; .metadata.name == $leader and any(.status.conditions[]?; .type == "ContainersReady" and .status == "True")) and
      any($surge[]; .metadata.name == $worker and any(.status.conditions[]?; .type == "ContainersReady" and .status == "False"))' <<<"${pods}" >/dev/null || {
      echo 'partial readiness or exact replacement identities changed during restart' >&2; return 1;
    }
    jq -cn --argjson pods "${pods}" --argjson ir "${ir}" --argjson endpoints "${endpoints}" \
      '{pods:$pods,ir:$ir,endpoints:$endpoints}' >>"${artifact_dir}/partial-api-samples.jsonl"
    jq -cn --arg uuid "${uuid}" --argjson uids "${replacement_uids}" --arg phase "$(jq -r '.phase' <<<"${current_request}")" \
      '{sourceSafe:true,leaderContainersReady:true,workerContainersReady:false,migrationCount:1,
        requestUUID:$uuid,replacementUIDs:$uids,migrationPhase:$phase}' >>"${artifact_dir}/partial-samples.jsonl"
    current_pod="$("${kube[@]}" -n ome get pods -l control-plane=ome-controller-manager -o json |
      jq -r --arg old "${old_pod}" '[.items[] | select(.metadata.uid != $old and .metadata.deletionTimestamp == null and
        any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | if length == 1 then .[0].metadata.uid else empty end')"
    holder="$("${kube[@]}" -n ome get lease ome-controller-manager-leader-lock -o jsonpath='{.spec.holderIdentity}')"
    if [[ -n "${current_pod}" && -n "${holder}" && "${holder}" != "${old_holder}" ]]; then
      # Retention alone is not execution proof: gang.sh must subsequently
      # complete this same UUID under the replacement manager after release.
      recovery="$(jq -cn --arg old "${old_pod}" --arg new "${current_pod}" \
        '{oldPodUID:$old,newPodUID:$new,leadershipRecovered:true,sameRequestObserved:true}')"
      break
    fi
    sleep 0.5
  done
  [[ -n "${recovery}" ]] || { echo 'manager did not resume the same partial migration' >&2; return 1; }
  jq -n --arg uuid "${uuid}" --argjson uids "${replacement_uids}" --argjson recovery "${recovery}" \
    --slurpfile samples "${artifact_dir}/partial-samples.jsonl" \
    '{requestUUID:$uuid,replacementUIDs:$uids,recovery:$recovery,samples:$samples}' >"${artifact_dir}/partial-evidence.json"
  jq -e -f "${dir}/verify-partial-gang.jq" "${artifact_dir}/partial-evidence.json" >/dev/null
}
