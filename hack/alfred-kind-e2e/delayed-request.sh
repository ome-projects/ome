#!/usr/bin/env bash
# Sourced by scenario.sh. Root owns invoking this against the isolated cluster.
dr_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
dr_initialized=false
dr_manager_changed=false
dr_fallback_changed=false
dr_health_nodes=()
dr_blocker_names=()
dr_blocker_uids=()
delayed_request_evidence_json=null

_delayed_request_error() { echo "delayed-request: $*" >&2; return 1; }

_delayed_request_scope() {
  local i context='' config=''
  [[ "${namespace:-}" == alfred-e2e ]] || { _delayed_request_error 'unexpected workload namespace'; return 1; }
  [[ "${kube[0]:-}" == kubectl ]] || { _delayed_request_error 'explicit kubectl array required'; return 1; }
  for ((i=1; i<${#kube[@]}; i++)); do
    case "${kube[$i]}" in
      --context) context="${kube[$((i+1))]:-}" ;;
      --kubeconfig) config="${kube[$((i+1))]:-}" ;;
    esac
  done
  [[ "$context" == kind-alfred-e2e && "$config" == /* && -f "$config" ]] || {
    _delayed_request_error 'private absolute kubeconfig and kind-alfred-e2e context required'; return 1;
  }
}

# Independently account live bound Pod requests, not KWOK status/capacity alone.
# GPU init/Pod-level resources are outside this tiny fixture contract: reject
# them instead of silently using an incorrect effective-request formula.
_delayed_request_allocated_gpu() {
  jq -er --arg node "$2" '
    [.items[] | select(.spec.nodeName == $node and (.status.phase | IN("Succeeded","Failed") | not))] |
    if any(.[]; .spec.resources.requests["nvidia.com/gpu"] != null or
      any(.spec.initContainers[]?; (.resources.requests["nvidia.com/gpu"] // "0" | tonumber) != 0))
    then error("unsupported GPU init/Pod-level request") else
      [ .[] | ([.spec.containers[]?.resources.requests["nvidia.com/gpu"] // "0" | tonumber] | add // 0) +
        (.spec.overhead["nvidia.com/gpu"] // "0" | tonumber)] | add // 0 end' <<<"$1"
}

_delayed_request_blocker_identity() {
  jq -e --arg uid "$2" '.metadata.uid == $uid and ($uid | length) > 0' <<<"$1" >/dev/null
}

_delayed_request_delete_blocker() {
  local name="$1" uid="$2" options object deadline=$((SECONDS + 45))
  [[ "$namespace" == alfred-e2e && "$name" =~ ^[a-z0-9][a-z0-9.-]*$ && -n "$uid" ]] || return 1
  options=$(jq -cn --arg uid "$uid" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}') || return 1
  # kubectl raw DELETE sends stdin as the body (pkg/rawhttp/raw.go). Do not
  # replace with name-only deletion: the GET-to-DELETE race needs an API fence.
  "${kube[@]}" --request-timeout=10s delete \
    --raw="/api/v1/namespaces/${namespace}/pods/${name}" -f - <<<"$options" >/dev/null || return 1
  # Raw mode does not run kubectl's wait path. Keep ordinary Pod grace and
  # independently wait for this UID to disappear; never delete a successor.
  while ((SECONDS < deadline)); do
    object=$("${kube[@]}" --request-timeout=10s -n "$namespace" get pod "$name" --ignore-not-found -o json) || return 1
    [[ -n "$object" ]] || return 0
    _delayed_request_blocker_identity "$object" "$uid" || return 0
    sleep 0.5
  done
  _delayed_request_error "UID-fenced blocker delete timed out: $name/$uid"
}

_delayed_request_manager_pods() {
  "${kube[@]}" --request-timeout=10s -n ome get pods -l control-plane=ome-controller-manager -o json
}

_delayed_request_patch_manager_args() {
  local patch
  patch=$(jq -cn --argjson i "$dr_manager_index" --argjson args "$1" \
    '[{op:"replace",path:("/spec/template/spec/containers/"+($i|tostring)+"/args"),value:$args}]') || return 1
  "${kube[@]}" --request-timeout=10s -n ome patch deployment ome-controller-manager --type=json -p "$patch" >/dev/null
}

_delayed_request_wait_manager() {
  local args="$1" old_uids="$2" label="$3" deadline=$((SECONDS + 180))
  local pods lease renewed probe holder name first_renew
  # Same dry-run CREATE admission used by deploy.sh; no persisted probe object.
  probe=$("${kube[@]}" --request-timeout=10s create --dry-run=client --validate=false \
    -f "$dr_dir/manifests/workload-single.yaml" -o json |
    jq -sce '[.[] | (.items // [.])[] | select(.kind == "ClusterServingRuntime")] |
      if length == 1 then .[0] | .metadata={generateName:"alfred-e2e-delayed-probe-"} | .spec.disabled=true
      else error("one runtime required") end') || return 1
  while ((SECONDS < deadline)); do
    assert_source_continuity || { _delayed_request_error 'source lost routing during manager transition'; return 1; }
    pods=$(_delayed_request_manager_pods) || return 1
    if ! jq -e --argjson args "$args" --argjson old "$old_uids" '
      (.items | length) == 1 and all(.items[]; .metadata.deletionTimestamp == null and
        (.metadata.uid as $uid | $old | index($uid) == null) and
        any(.status.conditions[]?; .type == "Ready" and .status == "True") and
        any(.spec.containers[]; .name == "manager" and .args == $args))' <<<"$pods" >/dev/null; then
      sleep 1; continue
    fi
    lease=$("${kube[@]}" --request-timeout=10s -n ome get lease ome-controller-manager-leader-lock -o json) || return 1
    holder=$(jq -r '.spec.holderIdentity // ""' <<<"$lease")
    name=$(jq -r '.items[0].metadata.name' <<<"$pods")
    [[ "$holder" == "${name}_"* ]] || { sleep 1; continue; }
    first_renew=$(jq -r '.spec.renewTime // ""' <<<"$lease")
    [[ -n "$first_renew" ]] || { sleep 1; continue; }
    if ! "${kube[@]}" --request-timeout=10s create --dry-run=server -f - \
      <<<"$probe" >"$artifact_dir/delayed-${label}-webhook.json" 2>"$artifact_dir/delayed-${label}-webhook.stderr"; then
      sleep 1; continue
    fi
    sleep 1
    renewed=$("${kube[@]}" --request-timeout=10s -n ome get lease ome-controller-manager-leader-lock -o json) || return 1
    if jq -e --arg holder "$holder" --arg time "$first_renew" \
      '.spec.holderIdentity == $holder and .spec.renewTime != $time' <<<"$renewed" >/dev/null; then
      dr_manager_barrier=$(jq -cn --argjson original "$dr_original_args" --argjson paused "$dr_paused_args" \
        --argjson old "$old_uids" --argjson pods "$pods" --argjson lease "$lease" --argjson renewed "$renewed" \
        '{originalArgs:$original,pausedArgs:$paused,oldPodUIDs:$old,pods:$pods,lease:$lease,renewedLease:$renewed,webhookReady:true}')
      printf '%s\n' "$dr_manager_barrier" >"$artifact_dir/delayed-${label}-manager.json"
      "${kube[@]}" --request-timeout=10s -n ome logs "$name" -c manager \
        >"$artifact_dir/delayed-${label}-manager.log" || return 1
      if [[ "$label" == paused ]]; then
        rg -q 'InferenceReplica controller disabled via --enable-inferencereplica-controller=false' \
          "$artifact_dir/delayed-${label}-manager.log" || { _delayed_request_error 'missing disabled-controller log'; return 1; }
      else
        rg -q 'Starting Controller.*InferenceReplica|Starting Controller.*inferencereplica|Starting workers.*InferenceReplica|Starting workers.*inferencereplica' \
          "$artifact_dir/delayed-${label}-manager.log" || { sleep 1; continue; }
      fi
      return 0
    fi
    sleep 1
  done
  _delayed_request_error "manager ${label} leadership/webhook barrier timed out"
}

_delayed_request_restore_manager() {
  [[ "$dr_manager_changed" == true ]] || return 0
  local old
  old=$(_delayed_request_manager_pods | jq -c '[.items[].metadata.uid]') || return 1
  _delayed_request_patch_manager_args "$dr_original_args" || return 1
  _delayed_request_wait_manager "$dr_original_args" "$old" resumed || return 1
  dr_manager_changed=false
}

delayed_request_prepare() {
  _delayed_request_scope || return 1
  case "${scenario:-}" in hint-exhaustion-single|target-health-race-single) ;; *)
    _delayed_request_error 'unsupported delayed-request variant'; return 1;; esac
  [[ "$dr_initialized" == false ]] || { _delayed_request_error 'prepare called twice'; return 1; }
  [[ "${artifact_dir:-}" == /* && -d "$artifact_dir" ]] || return 1
  local nodes pods deploy candidate count
  nodes=$("${kube[@]}" --request-timeout=10s get nodes -l alfred-e2e/virtual=true -o json) || return 1
  jq -e '([.items[].metadata.name] | sort) ==
    ["alfred-kwok-gpu-a","alfred-kwok-gpu-b","alfred-kwok-gpu-c","alfred-kwok-gpu-d"] and
    all(.items[]; (.spec.unschedulable // false) == false and
      .metadata.labels["nvidia.com/gpu.product"] == "NVIDIA-H100-80GB-HBM3" and
      .metadata.labels["maintenance.example.com/state"] != "patching" and
      (.status.allocatable["nvidia.com/gpu"] | tonumber) == 8 and
      any(.status.conditions[]; .type == "Ready" and .status == "True") and
      all(.status.conditions[] | select(.type == "GpuUnhealthy"); .status == "False" and
        (.lastTransitionTime | fromdateiso8601) <= (now - 60)))' <<<"$nodes" >/dev/null || {
    _delayed_request_error 'exact four healthy uncordoned test nodes required'; return 1;
  }
  jq -e --arg source "$source_node" 'any(.items[]; .metadata.name == $source)' <<<"$nodes" >/dev/null || return 1
  pods=$("${kube[@]}" --request-timeout=10s get pods -A -o json) || return 1
  # All three alternatives must really be empty; no capacity fabrication.
  for candidate in $(jq -r --arg source "$source_node" '.items[] | select(.metadata.name != $source) | .metadata.name' <<<"$nodes"); do
    count=$(_delayed_request_allocated_gpu "$pods" "$candidate") || return 1
    [[ "$count" == 0 ]] || { _delayed_request_error "alternative $candidate is not empty"; return 1; }
  done
  dr_fallback=$(jq -r --arg source "$source_node" '[.items[] | select(.metadata.name != $source)] | sort_by(.metadata.name) | last.metadata.name' <<<"$nodes")
  deploy=$("${kube[@]}" --request-timeout=10s -n ome get deployment ome-controller-manager -o json) || return 1
  [[ "$(jq -r '.spec.replicas' <<<"$deploy")" == 1 ]] || { _delayed_request_error 'one dedicated manager replica required'; return 1; }
  dr_manager_index=$(jq -er '.spec.template.spec.containers | map(.name) | index("manager")' <<<"$deploy") || return 1
  dr_original_args=$(jq -ec --argjson i "$dr_manager_index" '.spec.template.spec.containers[$i].args' <<<"$deploy") || return 1
  jq -e 'all(.[]; . != "--enable-inferencereplica-controller=false")' <<<"$dr_original_args" >/dev/null || return 1
  dr_paused_args=$(jq -c '[.[] | select(startswith("--enable-inferencereplica-controller=") | not)] +
    ["--enable-inferencereplica-controller=false"]' <<<"$dr_original_args")
  dr_original_nodes="$nodes"
  dr_run_id="delayed-$(date -u +%Y%m%d%H%M%S)-$$-${RANDOM}"
  dr_initialized=true
  printf '%s\n' "$deploy" >"$artifact_dir/delayed-original-manager.json"
  printf '%s\n' "$nodes" >"$artifact_dir/delayed-original-nodes.json"
  local old
  old=$(_delayed_request_manager_pods | jq -c '[.items[].metadata.uid]') || return 1
  dr_manager_changed=true
  _delayed_request_patch_manager_args "$dr_paused_args" || return 1
  _delayed_request_wait_manager "$dr_paused_args" "$old" paused || return 1
  dr_pause_barrier="$dr_manager_barrier"
  dr_fallback_changed=true
  "${kube[@]}" --request-timeout=10s cordon "$dr_fallback" >/dev/null || return 1
  dr_fallback_before=$("${kube[@]}" --request-timeout=10s get node "$dr_fallback" -o json) || return 1
  assert_source_continuity || return 1
  echo "Paused real IR consumer/executor; preexcluded fallback ${dr_fallback}" >&2
}

delayed_request_resume() {
  _delayed_request_scope || return 1
  [[ "$dr_initialized" == true && "$dr_manager_changed" == true ]] || return 1
  local owner ir journal hints node pod manifest name uid all_pods allocated deadline nodes_after fallback_after mutation_ir source_pod elapsed
  owner=$("${kube[@]}" --request-timeout=10s -n "$namespace" get isvc "$isvc_name" -o json) || return 1
  ir=$("${kube[@]}" --request-timeout=10s -n "$namespace" get inferencereplica "$ir_name" -o json) || return 1
  jq -e --argjson request "$request_json" '
    ([.metadata.annotations // {} | keys[] | select(startswith("ome.io/migration-request-v1-"))] == [$request.annotationKey]) and
    (.metadata.annotations[$request.annotationKey] | fromjson) == $request.payload' <<<"$owner" >/dev/null || return 1
  jq -e '((.status.migrations // []) | length) == 0' <<<"$ir" >/dev/null || return 1
  [[ "$(pods_json | jq '.items | length')" == 1 ]] || { _delayed_request_error 'replacement existed during paused mailbox'; return 1; }
  hints=$(jq -ec --arg fallback "$dr_fallback" --arg source "$source_node" '
    .payload.hint_target_nodes | select(length == 2 and (unique | length) == 2 and
      index($fallback) == null and index($source) == null) |
    select(all(.[]; IN("alfred-kwok-gpu-a","alfred-kwok-gpu-b","alfred-kwok-gpu-c","alfred-kwok-gpu-d")))' <<<"$request_json") || {
    _delayed_request_error 'real request must hint the two non-source non-fallback nodes'; return 1;
  }
  journal=$("${kube[@]}" --request-timeout=10s -n ome get cm alfred-dispatch-state -o json |
    jq -ec --arg uuid "$request_uuid" '.data["state.json"] | fromjson | [.entries[] | select(.uuid == $uuid)] |
      if length == 1 then .[0] else error("one UUID journal entry required") end') || return 1
  deadline=$((SECONDS + 90))
  for node in $(jq -r '.[]' <<<"$hints"); do
    assert_source_continuity || return 1
    if [[ "$scenario" == hint-exhaustion-single ]]; then
      name="${dr_run_id}-${node##*-}"
      manifest=$(jq -cn --arg name "$name" --arg ns "$namespace" --arg node "$node" --arg run "$dr_run_id" '
        {apiVersion:"v1",kind:"Pod",metadata:{name:$name,namespace:$ns,labels:{"alfred-e2e.ome.io/delayed-request":$run}},
         spec:{schedulerName:"alfred-default-scheduler",terminationGracePeriodSeconds:3,
           nodeSelector:{"alfred-e2e/virtual":"true","kubernetes.io/hostname":$node},
           tolerations:[{key:"alfred-e2e/virtual",operator:"Equal",value:"true",effect:"NoSchedule"}],
           containers:[{name:"occupant",image:"registry.k8s.io/pause:3.10",resources:{requests:{cpu:"10m",memory:"16Mi","nvidia.com/gpu":"8"},limits:{"nvidia.com/gpu":"8"}}}]}}') || return 1
      # Save name before CREATE: uncertain API response still needs scoped cleanup.
      dr_blocker_names+=("$name")
      "${kube[@]}" --request-timeout=10s create -f - -o json <<<"$manifest" \
        >"$artifact_dir/delayed-blocker-${node}.json" || return 1
      uid=$(jq -er '.metadata.uid | select(type == "string" and length > 0)' \
        "$artifact_dir/delayed-blocker-${node}.json") || return 1
      dr_blocker_uids+=("$uid")
      while ((SECONDS < deadline)); do
        assert_source_continuity || return 1
        pod=$("${kube[@]}" --request-timeout=10s -n "$namespace" get pod "$name" -o json) || return 1
        _delayed_request_blocker_identity "$pod" "$uid" || {
          _delayed_request_error "blocker $name recreated after CREATE"; return 1;
        }
        if jq -e --arg node "$node" '.spec.nodeName == $node and .metadata.deletionTimestamp == null and
          any(.status.conditions[]?; .type == "PodScheduled" and .status == "True")' <<<"$pod" >/dev/null; then break; fi
        sleep 0.5
      done
      ((SECONDS < deadline)) || { _delayed_request_error 'actual blocker scheduling timeout'; return 1; }
      all_pods=$("${kube[@]}" --request-timeout=10s get pods -A -o json) || return 1
      allocated=$(_delayed_request_allocated_gpu "$all_pods" "$node") || return 1
      [[ "$allocated" == 8 ]] || { _delayed_request_error "hinted node $node not exactly full"; return 1; }
    else
      dr_health_nodes+=("$node")
      "${kube[@]}" --request-timeout=10s annotate node "$node" alfred-e2e.ome.io/gpu-health=unhealthy --overwrite >/dev/null || return 1
      while ((SECONDS < deadline)); do
        assert_source_continuity || return 1
        pod=$("${kube[@]}" --request-timeout=10s get node "$node" -o json) || return 1
        if jq -e 'any(.status.conditions[]?; .type == "GpuUnhealthy" and .status == "True")' <<<"$pod" >/dev/null; then break; fi
        sleep 0.5
      done
      ((SECONDS < deadline)) || { _delayed_request_error 'actual target-health condition timeout'; return 1; }
    fi
  done
  if [[ "$scenario" == hint-exhaustion-single ]]; then
    "${kube[@]}" --request-timeout=10s uncordon "$dr_fallback" >/dev/null || return 1
  fi
  nodes_after=$("${kube[@]}" --request-timeout=10s get nodes -l alfred-e2e/virtual=true -o json |
    jq -c --argjson hints "$hints" '[.items[] | select(.metadata.name as $n | $hints | index($n) != null)]') || return 1
  fallback_after=$("${kube[@]}" --request-timeout=10s get node "$dr_fallback" -o json) || return 1
  all_pods=$("${kube[@]}" --request-timeout=10s get pods -A -o json) || return 1
  [[ "$(_delayed_request_allocated_gpu "$all_pods" "$dr_fallback")" == 0 ]] || return 1
  mutation_ir=$("${kube[@]}" --request-timeout=10s -n "$namespace" get inferencereplica "$ir_name" -o json) || return 1
  source_pod=$(pods_json | jq -ec --arg uid "$source_uid" '.items[] | select(.metadata.uid == $uid)') || return 1
  elapsed=$(jq -er '.payload.requested_at | sub("\\.[0-9]+Z$";"Z") | fromdateiso8601 | now - . | floor' <<<"$request_json") || return 1
  local blocker_uids='[]'
  if ((${#dr_blocker_uids[@]})); then blocker_uids=$(printf '%s\n' "${dr_blocker_uids[@]}" | jq -Rsc 'split("\n")[:-1]'); fi
  delayed_request_evidence_json=$(jq -cn --arg scenario "$scenario" --arg source "$source_node" --arg uid "$source_uid" \
    --arg fallback "$dr_fallback" --arg run "$dr_run_id" --argjson elapsed "$elapsed" --argjson request "$request_json" \
    --argjson owner "$owner" --argjson ir "$ir" --argjson mutation "$mutation_ir" --argjson journal "$journal" \
    --argjson manager "$dr_pause_barrier" --argjson before "$dr_fallback_before" --argjson after "$fallback_after" \
    --argjson nodes "$nodes_after" --argjson pods "$all_pods" --argjson blockers "$blocker_uids" --argjson sourcePod "$source_pod" \
    --argjson originals "$dr_original_nodes" \
    '{scenario:$scenario,sourceNode:$source,sourceUID:$uid,fallback:$fallback,runID:$run,elapsedSinceRequestSeconds:$elapsed,
      request:$request,pausedISVC:$owner,pausedIR:$ir,mutationIR:$mutation,journal:$journal,managerPause:$manager,
      fallbackBefore:$before,fallbackAfter:$after,sourcePod:$sourcePod,originalNodes:$originals,
      targets:{nodes:$nodes,pods:$pods,blockerUIDs:$blockers}}') || return 1
  printf '%s\n' "$delayed_request_evidence_json" >"$artifact_dir/delayed-request-evidence.json"
  jq -e -f "$dr_dir/verify-delayed-request.jq" <<<"$delayed_request_evidence_json" >/dev/null || {
    _delayed_request_error 'delayed request evidence failed verification'; return 1;
  }
  _delayed_request_restore_manager || return 1
  elapsed=$(jq -er '.payload.requested_at | sub("\\.[0-9]+Z$";"Z") | fromdateiso8601 | now - . | floor' <<<"$request_json") || return 1
  delayed_request_evidence_json=$(jq -c --argjson elapsed "$elapsed" --argjson manager "$dr_manager_barrier" \
    '.resumedElapsedSeconds=$elapsed | .managerResume=$manager' <<<"$delayed_request_evidence_json") || return 1
  printf '%s\n' "$delayed_request_evidence_json" >"$artifact_dir/delayed-request-evidence.json"
  ((elapsed < 180)) || { _delayed_request_error 'resume exceeded three-minute acknowledgement budget'; return 1; }
  assert_source_continuity || return 1
}

delayed_request_cleanup() {
  [[ "$dr_initialized" == true ]] || return 0
  local failed=0 node name object original uid idx health_deadline health_cleared
  # Caller must clear maintenance before this function frees target capacity.
  object=$("${kube[@]}" --request-timeout=10s get node "$source_node" -o json) || return 1
  if jq -e '.metadata.labels["maintenance.example.com/state"] == "patching"' <<<"$object" >/dev/null; then
    _delayed_request_error 'clear source maintenance before delayed-request cleanup'; return 1
  fi
  _delayed_request_restore_manager || failed=1
  for name in ${dr_blocker_names[@]+"${dr_blocker_names[@]}"}; do
    object=$("${kube[@]}" --request-timeout=10s -n "$namespace" get pod "$name" --ignore-not-found -o json) || { failed=1; continue; }
    [[ -n "$object" ]] || continue
    if ! jq -e --arg run "$dr_run_id" '.metadata.labels["alfred-e2e.ome.io/delayed-request"] == $run and
      (.metadata.ownerReferences // [] | length) == 0' <<<"$object" >/dev/null; then
      _delayed_request_error "refusing cleanup of unowned blocker $name"; failed=1; continue
    fi
    uid=$(jq -r '.metadata.uid' <<<"$object")
    # A successful CREATE response is always the identity anchor. With an
    # uncertain CREATE, ownership labels identify only this unique test name;
    # use the observed UID as an API fence without claiming CREATE identity.
    idx=0
    for original in "${dr_blocker_names[@]}"; do
      if [[ "$original" == "$name" ]]; then break; fi
      idx=$((idx+1))
    done
    if [[ -n "${dr_blocker_uids[$idx]:-}" && "$uid" != "${dr_blocker_uids[$idx]}" ]]; then
      _delayed_request_error "blocker $name UID changed"; failed=1; continue
    fi
    _delayed_request_delete_blocker "$name" "$uid" || failed=1
  done
  for node in ${dr_health_nodes[@]+"${dr_health_nodes[@]}"}; do
    original=$(jq -r --arg node "$node" '.items[] | select(.metadata.name == $node) |
      .metadata.annotations["alfred-e2e.ome.io/gpu-health"] // ""' <<<"$dr_original_nodes")
    "${kube[@]}" --request-timeout=10s annotate node "$node" alfred-e2e.ome.io/gpu-health=healthy --overwrite >/dev/null || { failed=1; continue; }
    # KWOK may process several ten-second heartbeat cycles before the health
    # stage runs. A live transition exceeded the original 30-second cleanup
    # budget; keep cleanup bounded without confusing it with migration failure.
    health_deadline=$((SECONDS + 90))
    health_cleared=false
    while ((SECONDS < health_deadline)); do
      object=$("${kube[@]}" --request-timeout=10s get node "$node" -o json) || break
      if jq -e 'any(.status.conditions[]?; .type == "GpuUnhealthy" and .status == "False")' <<<"$object" >/dev/null; then health_cleared=true; break; fi
      sleep 0.5
    done
    if [[ "$health_cleared" != true ]]; then
      _delayed_request_error "health cleanup did not observe GpuUnhealthy=False on $node within 90 seconds" || true
      failed=1; continue
    fi
    if [[ -z "$original" ]]; then
      "${kube[@]}" --request-timeout=10s annotate node "$node" alfred-e2e.ome.io/gpu-health- >/dev/null || failed=1
    else
      "${kube[@]}" --request-timeout=10s annotate node "$node" "alfred-e2e.ome.io/gpu-health=$original" --overwrite >/dev/null || failed=1
    fi
  done
  if [[ "$dr_fallback_changed" == true ]]; then
    # Prepare requires original uncordoned state and changes only this node.
    "${kube[@]}" --request-timeout=10s uncordon "$dr_fallback" >/dev/null || failed=1
  fi
  if ((failed)); then _delayed_request_error 'cleanup failed; saved state/artifacts retained'; return 1; fi
  dr_initialized=false
}
