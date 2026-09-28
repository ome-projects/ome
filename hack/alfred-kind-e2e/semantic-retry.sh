#!/usr/bin/env bash
# Sourced only by scenario.sh semantic-retry-single (and offline tests).
sr_started=false
sr_policy_uid=''
sr_binding_uid=''
sr_binding_removed=false
sr_entry=null
sr_baseline=null

sr_error() { echo "semantic-retry: $*" >&2; return 1; }

sr_parse_metrics() {
  jq -Rsc --arg name "${sr_name}" '
    split("\n") as $lines |
    [$lines[] | select(startswith("apiserver_validating_admission_policy_check_total{")) |
      capture("^apiserver_validating_admission_policy_check_total\\{(?<labels>[^}]*)\\} (?<value>[^ ]+)$") |
      select(.labels | contains("policy=\""+$name+"\"") and contains("policy_binding=\""+$name+"\"") and
        contains("error_type=\"no_error\"") and contains("enforcement_action=\"deny\"")) |
      .value | tonumber] as $counts |
    [$lines[] | select(startswith("process_start_time_seconds ")) | split(" ")[1] | tonumber] as $starts |
    if ($counts|length) != 1 or ($starts|length) != 1 or
       ($counts[0] | . < 0 or . != floor) or $starts[0] <= 0
    then error("missing or ambiguous policy rejection/process metric")
    else {denyCount:$counts[0],processStart:$starts[0]} end'
}

sr_request_patch() {
  jq -cen --argjson owner "$1" --argjson entry "$2" --arg uid "${sr_owner_uid}" '
    if $owner.metadata.uid != $uid or ($owner.metadata.resourceVersion|type) != "string" or
      ($owner.metadata.resourceVersion|length) == 0 or ($owner.metadata.annotations|type) != "object"
    then error("probe owner identity/annotations unavailable") else
    [{op:"test",path:"/metadata/uid",value:$uid},
     {op:"test",path:"/metadata/resourceVersion",value:$owner.metadata.resourceVersion},
     {op:"add",path:("/metadata/annotations/ome.io~1migration-request-v1-"+$entry.uuid),value:$entry.payload}] end'
}

sr_set_policy() {
  local revision="$1" paused="$2" owner patch
  owner="$("${kube[@]}" -n "${namespace}" get inferenceservice "${isvc_name}" -o json)" || return 1
  patch="$(jq -cen --argjson owner "${owner}" --arg uid "${sr_owner_uid}" \
    --argjson revision "${revision}" --argjson paused "${paused}" '
    if $owner.metadata.uid != $uid or ($owner.metadata.resourceVersion|type) != "string" or
      ($owner.metadata.resourceVersion|length) == 0 or ($owner.metadata.annotations|type) != "object"
    then error("authority owner identity/annotations unavailable") else
    [{op:"test",path:"/metadata/uid",value:$uid},
     {op:"test",path:"/metadata/resourceVersion",value:$owner.metadata.resourceVersion},
     {op:"add",path:"/metadata/annotations/ome.io~1placement-execution",value:
       ({planID:"alfred-e2e-pause",revision:$revision,sourceUID:"alfred-e2e-source",
         clusterUID:"alfred-e2e-member",pauseSurge:$paused}|tojson)}] end')" || return 1
  "${kube[@]}" -n "${namespace}" patch inferenceservice "${isvc_name}" --type=json -p "${patch}" >/dev/null
}

sr_delete_uid() {
  local kind="$1" uid="$2" object plural options
  case "${kind}" in
    validatingadmissionpolicy) plural=validatingadmissionpolicies ;;
    validatingadmissionpolicybinding) plural=validatingadmissionpolicybindings ;;
    *) sr_error 'unsupported cleanup resource'; return 1 ;;
  esac
  [[ "${sr_name}" == alfred-e2e-retry-* && -n "${uid}" ]] || return 1
  object="$("${kube[@]}" get "${kind}" "${sr_name}" --ignore-not-found -o json)" || return 1
  [[ -n "${object}" ]] || return 0
  jq -e --arg uid "${uid}" '.metadata.uid == $uid' <<<"${object}" >/dev/null || {
    sr_error "refusing to delete successor ${kind}/${sr_name}"; return 1;
  }
  options="$(jq -cn --arg uid "${uid}" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}')" || return 1
  "${kube[@]}" delete --raw="/apis/admissionregistration.k8s.io/v1/${plural}/${sr_name}" -f - <<<"${options}" >/dev/null
}

sr_scope() {
  local i context='' config=''
  [[ "${namespace}" == alfred-e2e && "${isvc_name}" == single && "${ir_name}" == single-engine &&
    "${alfred_namespace}" == ome && "${kube[0]}" == kubectl ]] || return 1
  for ((i=1; i<${#kube[@]}; i++)); do
    case "${kube[$i]}" in
      --context) context="${kube[$((i+1))]:-}" ;;
      --kubeconfig) config="${kube[$((i+1))]:-}" ;;
    esac
  done
  [[ "${context}" == kind-alfred-e2e && "${config}" == /* && "${config}" == "${state_dir}/kubeconfig" && -f "${config}" ]]
}

sr_apply_fixture() {
  # Client dry-run converts the existing pause fixture; only the initial public
  # envelope differs. This never authors an IR or controller-owned status.
  "${kube[@]}" create --dry-run=client -f "${workload_manifest}" -o json |
    jq -sce '[.[] | (.items // [.])[]] |
      if ([.[]|select(.kind == "InferenceService")]|length) != 1 then error("one fixture owner required") else
      {apiVersion:"v1",kind:"List",items:map(if .kind == "InferenceService" then
        .metadata.annotations["ome.io/placement-execution"] |= (fromjson | .pauseSurge=false | tojson) else . end)} end' |
    "${kube[@]}" apply -f -
}

sr_snapshot() {
  local sample requests
  sample="$(placement_snapshot)" || return 1
  # Keep conflicting values for one UUID visible instead of deduplicating them.
  requests="$(jq -cs '[.[] | .metadata.annotations // {} | to_entries[] |
    select(.key | startswith("ome.io/migration-request-v1-"))] | unique_by([.key,.value])' "${request_watch_file}")" || return 1
  jq -c --argjson requests "${requests}" '.requests=$requests' <<<"${sample}"
}

sr_check() {
  jq -e -L "${script_dir}" --arg stage "$1" --argjson base "${sr_baseline}" \
    --argjson entry "${sr_entry}" --argjson source "${source_json}" --argjson replacement "${replacement_json:-null}" \
    'include "semantic-retry-sample"; sr_sample($stage;$base;$entry;$source;$replacement)' <<<"$2" >/dev/null
}

sr_guard_snapshot() {
  local policy binding
  policy="$("${kube[@]}" get validatingadmissionpolicy ome-alfred-migration-writes -o json)" || return 1
  binding="$("${kube[@]}" get validatingadmissionpolicybinding ome-alfred-migration-writes -o json)" || return 1
  jq -cn --argjson policy "${policy}" --argjson binding "${binding}" '{policy:$policy,binding:$binding}'
}

sr_metrics() {
  local label="$1" metrics parsed api
  metrics="$("${kube[@]}" get --raw=/metrics)" || return 1
  printf '%s\n' "${metrics}" >"${artifact_dir}/retry-metrics-${label}.txt"
  parsed="$(sr_parse_metrics <<<"${metrics}")" || return 1
  api="$("${kube[@]}" -n kube-system get pods -l component=kube-apiserver -o json)" || return 1
  printf '%s\n' "${api}" >"${artifact_dir}/retry-apiserver-${label}.json"
  jq -ce --argjson api "${api}" --arg captured "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    'if ($api.items|length) != 1 or ($api.items[0].metadata.uid|type) != "string"
    then error("one identifiable API server required") else . + {apiServerUID:$api.items[0].metadata.uid,capturedAt:$captured} end' <<<"${parsed}"
}

sr_probe() {
  local expected="$1" entry="$2" owner patch output errors code
  owner="$("${kube[@]}" -n "${namespace}" get inferenceservice "${isvc_name}" -o json)" || return 1
  patch="$(sr_request_patch "${owner}" "${entry}")" || return 1
  printf '%s\n' "${patch}" >"${artifact_dir}/retry-probe-${expected}.patch.json"
  if output="$("${kube[@]}" --as="${sr_username}" -n "${namespace}" patch inferenceservice "${isvc_name}" \
    --type=json -p "${patch}" --dry-run=server -o json 2>"${artifact_dir}/retry-probe-${expected}.stderr")"; then code=0; else code=$?; fi
  printf '%s\n' "${output}" >"${artifact_dir}/retry-probe-${expected}.stdout"
  errors="$(<"${artifact_dir}/retry-probe-${expected}.stderr")"
  jq -cn --argjson code "${code}" --arg output "${output}${errors}" '{exitCode:$code,output:$output}' >"${artifact_dir}/retry-probe-${expected}.json"
  jq -c . "${artifact_dir}/retry-probe-${expected}.json" >>"${artifact_dir}/retry-probe-${expected}-attempts.jsonl" || return 1
  if [[ "${expected}" == denied ]]; then
    # Policy/binding informer caches need not observe CREATE synchronously.
    [[ "${code}" != 0 ]] || return 2
    [[ "${code}" != 0 && "${errors}${output}" == *"${sr_name}"* && "${errors}${output}" == *'semantic-retry publication held'* ]] || {
      sr_error 'dry-run did not reach the fixture admission rejection'; return 1;
    }
  else
    # Binding DELETE is likewise asynchronous at the admission cache boundary.
    if [[ "${code}" != 0 && "${errors}${output}" == *"${sr_name}"* && "${errors}${output}" == *'semantic-retry publication held'* ]]; then return 2; fi
    [[ "${code}" == 0 ]] || { sr_error 'unblocked dry-run still failed'; return 1; }
    jq -e --argjson entry "${entry}" --arg uid "${sr_owner_uid}" \
      '.metadata.uid == $uid and .metadata.annotations["ome.io/migration-request-v1-"+$entry.uuid] == $entry.payload' <<<"${output}" >/dev/null
  fi
}

sr_wait_probe() {
  local expected="$1" entry="$2" deadline=$((SECONDS + 20)) code
  if (( ${sr_deadline:-0} > 0 && sr_deadline < deadline )); then deadline="${sr_deadline}"; fi
  while ((SECONDS < deadline)); do
    if sr_probe "${expected}" "${entry}"; then return 0; else code=$?; fi
    [[ "${code}" == 2 ]] || return 1
    sleep 0.2
  done
  sr_error "admission ${expected} probe did not converge"
}

sr_prepare() {
  local existing policy ready=false deadline probe_payload probe_entry kind deployment
  sr_scope || { sr_error 'dedicated kind fixture scope required'; return 1; }
  sr_name="alfred-e2e-retry-$(tr '[:upper:]' '[:lower:]' <<<"${run_id}")"
  sr_username="system:serviceaccount:${alfred_namespace}:ome-alfred"
  deployment="$("${kube[@]}" -n "${alfred_namespace}" get deployment ome-alfred -o json)" || return 1
  printf '%s\n' "${deployment}" >"${artifact_dir}/retry-alfred-deployment.json"
  jq -e '.spec.template.spec.serviceAccountName == "ome-alfred" and
    any(.spec.template.spec.containers[]; any(.args[]?; . == "--migration-ack-timeout=3m"))' <<<"${deployment}" >/dev/null || {
    sr_error 'expected harness Alfred identity and three-minute acknowledgement timeout'; return 1;
  }
  sr_baseline="$(sr_snapshot)" || return 1
  sr_owner_uid="$(jq -r '.isvc.metadata.uid' <<<"${sr_baseline}")"
  sr_check baseline "${sr_baseline}" || { sr_error 'released source baseline invalid'; return 1; }
  printf '%s\n' "${sr_baseline}" >"${artifact_dir}/retry-baseline.json"
  sr_guard_snapshot >"${artifact_dir}/retry-guard-before.json" || return 1
  for kind in validatingadmissionpolicy validatingadmissionpolicybinding; do
    existing="$("${kube[@]}" get "${kind}" "${sr_name}" --ignore-not-found -o json)" || return 1
    [[ -z "${existing}" ]] || { sr_error 'refusing to overwrite an admission object'; return 1; }
  done
  jq -cn -L "${script_dir}" --arg name "${sr_name}" --arg uid "${sr_owner_uid}" --arg user "${sr_username}" \
    'include "semantic-retry-admission"; retry_policy($name;$uid;$user)' >"${artifact_dir}/retry-policy-desired.json" || return 1
  jq -cn -L "${script_dir}" --arg name "${sr_name}" \
    'include "semantic-retry-admission"; retry_binding($name)' >"${artifact_dir}/retry-binding-desired.json" || return 1
  sr_started=true
  "${kube[@]}" create -f "${artifact_dir}/retry-policy-desired.json" -o json >"${artifact_dir}/retry-policy-created.json" || return 1
  sr_policy_uid="$(jq -er '.metadata.uid' "${artifact_dir}/retry-policy-created.json")" || return 1
  "${kube[@]}" create -f "${artifact_dir}/retry-binding-desired.json" -o json >"${artifact_dir}/retry-binding-created.json" || return 1
  sr_binding_uid="$(jq -er '.metadata.uid' "${artifact_dir}/retry-binding-created.json")" || return 1
  deadline=$((SECONDS + 45))
  while ((SECONDS < deadline)); do
    policy="$("${kube[@]}" get validatingadmissionpolicy "${sr_name}" -o json)" || return 1
    if jq -e --arg uid "${sr_policy_uid}" '.metadata.uid == $uid and .metadata.generation >= 1 and
      .status.observedGeneration == .metadata.generation and (.status.typeChecking|type) == "object" and
      ((.status.typeChecking.expressionWarnings // [])|length) == 0' <<<"${policy}" >/dev/null; then ready=true; break; fi
    sleep 0.2
  done
  [[ "${ready}" == true ]] || { sr_error 'fixture admission policy failed type checking'; return 1; }
  printf '%s\n' "${policy}" >"${artifact_dir}/retry-policy-ready.json"
  probe_payload="$(jq -cn --arg node "${source_node}" \
    '{schemaVersion:"v1",component:"engine",instance:0,from_node:$node,requested_by:"alfred",reason:"NodeMaintenance"}')" || return 1
  probe_entry="$(jq -cn --arg payload "${probe_payload}" '{uuid:"00000000-0000-4000-8000-000000000001",payload:$payload}')" || return 1
  sr_wait_probe denied "${probe_entry}" || return 1
  # Do not run another impersonated request probe until the runtime-denial
  # witness is sealed. Dry-run requests count in these API-server metrics too.
  sr_metrics before >"${artifact_dir}/retry-metrics-before.json" || return 1
}

sr_wait_projection() {
  local revision="$1" paused="$2" stage="$3" sample
  while ((SECONDS < sr_deadline)); do
    sample="$(sr_snapshot)" || return 1
    printf '%s\n' "${sample}" >>"${artifact_dir}/retry-${stage}-projection.jsonl"
    sr_check source-safe "${sample}" || { sr_error 'source changed during placement projection'; return 1; }
    if jq -e --argjson revision "${revision}" --argjson paused "${paused}" '
      .ir.spec.placementExecution.revision == $revision and .ir.spec.placementExecution.pauseSurge == $paused and
      .ir.status.placementObservedGeneration == .ir.metadata.generation' <<<"${sample}" >/dev/null; then
      sr_check "${stage}" "${sample}" || return 1
      printf '%s\n' "${sample}" >"${artifact_dir}/retry-${stage}.json"
      return 0
    fi
    sleep 0.2
  done
  sr_error "timed out waiting for ${stage} projection"
}

sr_collect_cycles() {
  local stage="$1" count="$2" label="$3" sample seen='' stamp n=0 completion_stamp=''
  if [[ "${stage}" == completed ]]; then
    sample="$(sr_snapshot)" || return 1
    sr_check completed "${sample}" || return 1
    printf '%s\n' "${sample}" >"${artifact_dir}/retry-completion-baseline.json"
    completion_stamp="$(jq -er '.recommendations.data["last-cycle.json"]|fromjson|.timestamp' <<<"${sample}")" || return 1
    seen="${completion_stamp}|"
  fi
  while ((SECONDS < sr_deadline && n < count)); do
    sample="$(sr_snapshot)" || return 1
    printf '%s\n' "${sample}" >>"${artifact_dir}/retry-${label}-api.jsonl"
    if [[ "${stage}" == paused ]]; then sr_check paused-safe "${sample}" || return 1
    else sr_check completed "${sample}" || return 1; fi
    stamp="$(jq -r '.recommendations.data["last-cycle.json"]|fromjson|.timestamp' <<<"${sample}")"
    if [[ "${stage}" == completed ]] && ! jq -e -L "${script_dir}" \
      --arg stamp "${completion_stamp}" --argjson base "${sr_baseline}" '
      include "semantic-retry-sample"; include "placement-pause-sample";
      (pp_cycle.timestamp|pp_time) > ($stamp|pp_time) and
      (pp_cycle.timestamp|pp_time) > (sr_entry($base)[0].completedAt|pp_time)' <<<"${sample}" >/dev/null; then
      sleep 0.2
      continue
    fi
    if [[ "|${seen}" != *"|${stamp}|"* ]] && sr_check "${stage}" "${sample}"; then
      printf '%s\n' "${sample}" >>"${artifact_dir}/retry-${label}-cycles.jsonl"
      seen="${seen}${stamp}|"
      n=$((n + 1))
    fi
    sleep 0.2
  done
  ((n == count)) || { sr_error "missing distinct ${stage} decision cycles"; return 1; }
}

sr_after_trigger() {
  local sample metrics entry count previous='' bad_cycles=0 stamp absent
  # Bound the rejection/pause/release proof below the configured three-minute
  # acknowledgement timeout. A stalled intent is not a recovered retry.
  sr_deadline=$((SECONDS + 120))
  while ((SECONDS < sr_deadline)); do
    sample="$(sr_snapshot)" || return 1
    printf '%s\n' "${sample}" >>"${artifact_dir}/retry-prepared-api.jsonl"
    sr_check no-effects "${sample}" || return 1
    entry="$(jq -c --arg uid "${sr_owner_uid}" '[.dispatch.data["state.json"]|fromjson|.entries[]|
      select(.workloadUID == $uid)] | if length == 1 and .[0].phase == "prepared" and .[0].reason == "SubmissionUncertain"
      and .[0].lastAttempt != null then .[0] else empty end' <<<"${sample}")" || return 1
    if [[ -n "${entry}" ]]; then
      sr_entry="${entry}"
      sr_check prepared "${sample}" || return 1
      metrics="$(sr_metrics rejected)" || return 1
      if jq -e --slurpfile before "${artifact_dir}/retry-metrics-before.json" '
        .denyCount > $before[0].denyCount and .processStart == $before[0].processStart and .apiServerUID == $before[0].apiServerUID' <<<"${metrics}" >/dev/null; then
        printf '%s\n' "${metrics}" >"${artifact_dir}/retry-metrics-rejected.json"
        printf '%s\n' "${sample}" >"${artifact_dir}/retry-prepared.json"
        printf '%s\n' "${sr_entry}" >"${artifact_dir}/retry-entry.json"
        break
      fi
    fi
    sleep 0.2
  done
  [[ -s "${artifact_dir}/retry-prepared.json" ]] || { sr_error 'no real API-denied prepared intent'; return 1; }
  sr_set_policy 2 true || return 1
  sr_wait_projection 2 true paused-safe || return 1
  sr_collect_cycles paused 1 paused-before-unblock || return 1
  sr_delete_uid validatingadmissionpolicybinding "${sr_binding_uid}" || return 1
  absent="$("${kube[@]}" get validatingadmissionpolicybinding "${sr_name}" --ignore-not-found -o json)" || return 1
  [[ -z "${absent}" ]] || { sr_error 'fixture binding deletion not observed'; return 1; }
  sr_binding_removed=true
  sr_wait_probe allowed "${sr_entry}" || return 1
  sr_collect_cycles paused 3 paused || return 1
  sr_set_policy 3 false || return 1
  sr_wait_projection 3 false released || return 1
  while ((SECONDS < sr_deadline)); do
    sample="$(sr_snapshot)" || return 1
    printf '%s\n' "${sample}" >>"${artifact_dir}/retry-publication-api.jsonl"
    sr_check source-safe "${sample}" || return 1
    count="$(jq '.requests|length' <<<"${sample}")"
    if [[ "${count}" != 0 ]]; then
      jq -e --argjson entry "${sr_entry}" '.requests == [{key:("ome.io/migration-request-v1-"+$entry.uuid),value:$entry.payload}]' <<<"${sample}" >/dev/null || return 1
      printf '%s\n' "${sample}" >"${artifact_dir}/retry-published.json"
      return 0
    fi
    if sr_check old-source-changed "${sample}"; then
      stamp="$(jq -r '.recommendations.data["last-cycle.json"]|fromjson|.timestamp' <<<"${sample}")"
      if [[ "${stamp}" != "${previous}" ]]; then
        previous="${stamp}"; bad_cycles=$((bad_cycles + 1))
        printf '%s\n' "${sample}" >>"${artifact_dir}/retry-source-changed-cycles.jsonl"
      fi
      if ((bad_cycles >= 3)); then sr_error 'SourceChanged persisted after harmless release (pre-fix failure witness)'; return 1; fi
    fi
    sleep 0.2
  done
  sr_error 'same prepared intent was not published within retry deadline'
}

sr_complete() {
  sr_deadline=$((SECONDS + 45))
  sr_collect_cycles completed 3 completed || return 1
  sr_guard_snapshot >"${artifact_dir}/retry-guard-after.json" || return 1
  jq -n --arg name "${sr_name}" --arg user "${sr_username}" --argjson removed "${sr_binding_removed}" \
    --slurpfile baseline "${artifact_dir}/retry-baseline.json" --slurpfile entry "${artifact_dir}/retry-entry.json" \
    --slurpfile prepared "${artifact_dir}/retry-prepared.json" --slurpfile paused "${artifact_dir}/retry-paused-cycles.jsonl" \
    --slurpfile released "${artifact_dir}/retry-released.json" --slurpfile completed "${artifact_dir}/retry-completed-cycles.jsonl" \
    --slurpfile completionBaseline "${artifact_dir}/retry-completion-baseline.json" \
    --slurpfile before "${artifact_dir}/retry-metrics-before.json" --slurpfile rejected "${artifact_dir}/retry-metrics-rejected.json" \
    --slurpfile deniedProbe "${artifact_dir}/retry-probe-denied.json" --slurpfile allowedProbe "${artifact_dir}/retry-probe-allowed.json" \
    --slurpfile guardBefore "${artifact_dir}/retry-guard-before.json" --slurpfile guardAfter "${artifact_dir}/retry-guard-after.json" \
    --slurpfile policy "${artifact_dir}/retry-policy-ready.json" --slurpfile binding "${artifact_dir}/retry-binding-created.json" \
    '{baseline:$baseline[0],entry:$entry[0],prepared:$prepared[0],paused:$paused,released:$released[0],completed:$completed,
      completionBaseline:$completionBaseline[0],
      admission:{name:$name,username:$user,bindingRemoved:$removed,metricsBefore:$before[0],metricsRejected:$rejected[0],
        deniedProbe:$deniedProbe[0],allowedProbe:$allowedProbe[0],guardBefore:$guardBefore[0],guardAfter:$guardAfter[0],
        policy:$policy[0],binding:$binding[0]}}' >"${artifact_dir}/semantic-retry-evidence.json"
}

sr_cleanup() {
  local kind object desired uid owner replica safe=false deadline resource
  [[ "${sr_started}" == true ]] || return 0
  # Recover only a creation with our unique run marker and exact requested
  # spec if create returned an uncertain response before its UID was saved.
  for kind in policy binding; do
    if [[ "${kind}" == policy ]]; then uid="${sr_policy_uid}"; else uid="${sr_binding_uid}"; fi
    if [[ -z "${uid}" ]]; then
      if [[ "${kind}" == policy ]]; then resource=validatingadmissionpolicy; else resource=validatingadmissionpolicybinding; fi
      object="$("${kube[@]}" get "${resource}" "${sr_name}" --ignore-not-found -o json)" || return 1
      if [[ -n "${object}" ]]; then
        desired="$(<"${artifact_dir}/retry-${kind}-desired.json")"
        jq -e --arg name "${sr_name}" --argjson desired "${desired}" '.metadata.labels["alfred-e2e.ome.io/run"] == $name and .spec == $desired.spec' <<<"${object}" >/dev/null || return 1
        uid="$(jq -er '.metadata.uid' <<<"${object}")" || return 1
        if [[ "${kind}" == policy ]]; then sr_policy_uid="${uid}"; else sr_binding_uid="${uid}"; fi
      fi
    fi
  done
  if [[ -n "${sr_binding_uid}" && "${sr_binding_removed}" != true ]]; then
    # The parent first clears maintenance. Also fence any already-preflighted
    # publication by changing owner RV and confirming projected placement pause
    # before removing a still-active deny binding. Never use a dead watch here.
    owner="$("${kube[@]}" -n "${namespace}" get inferenceservice "${isvc_name}" --ignore-not-found -o json)" || return 1
    if [[ -z "${owner}" ]] || ! jq -e --arg uid "${sr_owner_uid}" '.metadata.uid == $uid' <<<"${owner}" >/dev/null; then safe=true
    else
      sr_set_policy 4 true || return 1
      deadline=$((SECONDS + 20))
      while ((SECONDS < deadline)); do
        replica="$("${kube[@]}" -n "${namespace}" get inferencereplica "${ir_name}" -o json)" || return 1
        if jq -e --arg uid "${sr_owner_uid}" 'any(.metadata.ownerReferences[]?;.uid == $uid and .controller == true) and
          .spec.placementExecution.revision == 4 and .spec.placementExecution.pauseSurge == true and
          .status.placementObservedGeneration == .metadata.generation' <<<"${replica}" >/dev/null; then safe=true; break; fi
        sleep 0.2
      done
    fi
    [[ "${safe}" == true ]] || { sr_error "cleanup could not establish pause; retaining narrow deny ${sr_name}"; return 1; }
    sr_delete_uid validatingadmissionpolicybinding "${sr_binding_uid}" || return 1
  fi
  [[ -z "${sr_policy_uid}" ]] || sr_delete_uid validatingadmissionpolicy "${sr_policy_uid}" || return 1
}
