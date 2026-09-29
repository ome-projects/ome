#!/usr/bin/env bash

nc_error() {
  echo "no-capacity ownership: $*" >&2
}

nc_require_namespace() {
  local current
  current="$("${kube[@]}" get namespace "${namespace}" -o json)" || return 1
  jq -e --arg name "${namespace}" --arg uid "${namespace_uid}" '
    .kind == "Namespace" and .metadata.name == $name and
    (.metadata.uid | type == "string" and . == $uid)
  ' <<<"${current}" >/dev/null || {
    nc_error "Namespace ${namespace} identity changed"
    return 1
  }
}

nc_create_owned() {
  local desired="$1" response="$2" expected_kind="$3" expected_name="$4" expected_namespace="$5"
  [[ -z "${expected_namespace}" ]] || nc_require_namespace || return 1
  if ! "${kube[@]}" create -f "${desired}" -o json >"${response}"; then
    nc_error "CREATE ${expected_kind}/${expected_name} failed or its response was lost"
    return 1
  fi
  if ! jq -e --arg kind "${expected_kind}" --arg name "${expected_name}" --arg ns "${expected_namespace}" '
    .kind == $kind and .metadata.name == $name and
    (.metadata.uid | type == "string" and length > 0) and
    (if $ns == "" then (.metadata.namespace // "") == "" else .metadata.namespace == $ns end)
  ' "${response}" >/dev/null; then
    nc_error "malformed or wrong-identity CREATE response for ${expected_kind}/${expected_name}"
    return 1
  fi
  jq -c '{apiVersion,kind,name:.metadata.name,namespace:(.metadata.namespace // ""),uid:.metadata.uid}' \
    "${response}" >>"${artifact_dir}/ownership.jsonl"
  [[ -z "${expected_namespace}" ]] || nc_require_namespace || return 1
}

nc_set_maintenance() {
  local node="$1" uid="$2" key="$3" value="$4"
  local current labels version escaped patch response
  current="$("${kube[@]}" get node "${node}" -o json)" || return 1
  if ! jq -e --arg name "${node}" --arg uid "${uid}" --arg key "${key}" '
    .kind == "Node" and .metadata.name == $name and .metadata.uid == $uid and
    (.metadata.resourceVersion | type == "string" and length > 0) and
    (.metadata.labels | type == "object") and (.metadata.labels | has($key) | not)
  ' <<<"${current}" >/dev/null; then
    nc_error "source Node identity or maintenance label changed before trigger"
    return 1
  fi
  labels="$(jq -c '.metadata.labels' <<<"${current}")"
  version="$(jq -r '.metadata.resourceVersion' <<<"${current}")"
  escaped="$(jq -rn --arg key "${key}" '$key | gsub("~";"~0") | gsub("/";"~1")')"
  patch="$(jq -cn --arg uid "${uid}" --arg rv "${version}" --argjson labels "${labels}" \
    --arg path "/metadata/labels/${escaped}" --arg value "${value}" '
    [{op:"test",path:"/metadata/uid",value:$uid},
     {op:"test",path:"/metadata/resourceVersion",value:$rv},
     {op:"test",path:"/metadata/labels",value:$labels},
     {op:"add",path:$path,value:$value}]
  ')" || return 1
  printf '%s\n' "${patch}" >"${artifact_dir}/maintenance-set-patch.json"
  response="${artifact_dir}/maintenance-set-response.json"
  if ! "${kube[@]}" patch node "${node}" --type=json -p "${patch}" -o json >"${response}"; then
    trigger_uncertain=true
    nc_error "maintenance marker write failed or its response was lost"
    return 1
  fi
  if ! jq -e --arg name "${node}" --arg uid "${uid}" --arg key "${key}" --arg value "${value}" '
    .kind == "Node" and .metadata.name == $name and .metadata.uid == $uid and
    (.metadata.resourceVersion | type == "string" and length > 0) and
    .metadata.labels[$key] == $value
  ' "${response}" >/dev/null; then
    trigger_uncertain=true
    nc_error "malformed maintenance marker response"
    return 1
  fi
  triggered_node="${node}"
  triggered_node_uid="${uid}"
  trigger_active=true
  trigger_uncertain=false
}

nc_clear_maintenance() {
  local current labels version escaped patch response
  [[ "${trigger_uncertain:-false}" != true ]] || return 1
  [[ "${trigger_active:-false}" == true ]] || return 0
  current="$("${kube[@]}" get node "${triggered_node}" -o json)" || {
    trigger_uncertain=true
    return 1
  }
  if ! jq -e --arg name "${triggered_node}" --arg uid "${triggered_node_uid}" \
    --arg key "${maintenance_key}" --arg value "${maintenance_value}" '
    .kind == "Node" and .metadata.name == $name and .metadata.uid == $uid and
    (.metadata.resourceVersion | type == "string" and length > 0) and
    (.metadata.labels | type == "object") and .metadata.labels[$key] == $value
  ' <<<"${current}" >/dev/null; then
    trigger_uncertain=true
    nc_error "cannot prove ownership of maintenance marker during clearance"
    return 1
  fi
  labels="$(jq -c '.metadata.labels' <<<"${current}")"
  version="$(jq -r '.metadata.resourceVersion' <<<"${current}")"
  escaped="$(jq -rn --arg key "${maintenance_key}" '$key | gsub("~";"~0") | gsub("/";"~1")')"
  patch="$(jq -cn --arg uid "${triggered_node_uid}" --arg rv "${version}" --argjson labels "${labels}" \
    --arg path "/metadata/labels/${escaped}" '
    [{op:"test",path:"/metadata/uid",value:$uid},
     {op:"test",path:"/metadata/resourceVersion",value:$rv},
     {op:"test",path:"/metadata/labels",value:$labels},
     {op:"remove",path:$path}]
  ')" || return 1
  printf '%s\n' "${patch}" >"${artifact_dir}/maintenance-clear-patch.json"
  response="${artifact_dir}/maintenance-clear-response.json"
  if ! "${kube[@]}" patch node "${triggered_node}" --type=json -p "${patch}" -o json >"${response}"; then
    trigger_uncertain=true
    nc_error "maintenance marker clearance failed or its response was lost"
    return 1
  fi
  if ! jq -e --arg name "${triggered_node}" --arg uid "${triggered_node_uid}" --arg key "${maintenance_key}" '
    .kind == "Node" and .metadata.name == $name and .metadata.uid == $uid and
    (.metadata.resourceVersion | type == "string" and length > 0) and
    (.metadata.labels | type == "object") and (.metadata.labels | has($key) | not)
  ' "${response}" >/dev/null; then
    trigger_uncertain=true
    nc_error "malformed maintenance clearance response"
    return 1
  fi
  trigger_active=false
  triggered_node=''
  triggered_node_uid=''
}

nc_source_identity() {
  local isvc_uid="$1" isvc="$2" ir="$3" pods="$4"
  jq -cen --arg isvc_uid "${isvc_uid}" --argjson isvc "${isvc}" --argjson ir "${ir}" --argjson pods "${pods}" '
    if $isvc.kind == "InferenceService" and $isvc.metadata.uid == $isvc_uid and
      $ir.kind == "InferenceReplica" and
      ($ir.metadata.uid | type == "string" and length > 0) and
      any($ir.metadata.ownerReferences[]?;
        .kind == "InferenceService" and .uid == $isvc_uid and .controller == true) and
      ($pods.items | length) == 1 and $pods.items[0].kind == "Pod" and
      ($pods.items[0].metadata.uid | type == "string" and length > 0) and
      any($pods.items[0].metadata.ownerReferences[]?;
        .kind == "InferenceReplica" and .uid == $ir.metadata.uid and .controller == true)
    then {irUID:$ir.metadata.uid,sourceUID:$pods.items[0].metadata.uid}
    else error("source owner chain does not match recorded ISVC") end
  '
}

nc_source_owned() {
  local isvc_uid="$1" ir_uid="$2" source_uid="$3" isvc="$4" ir="$5" pods="$6"
  jq -en --arg isvc_uid "${isvc_uid}" --arg ir_uid "${ir_uid}" --arg source_uid "${source_uid}" \
    --argjson isvc "${isvc}" --argjson ir "${ir}" --argjson pods "${pods}" '
    $isvc.kind == "InferenceService" and $isvc.metadata.uid == $isvc_uid and
    $ir.kind == "InferenceReplica" and $ir.metadata.uid == $ir_uid and
    any($ir.metadata.ownerReferences[]?;
      .kind == "InferenceService" and .uid == $isvc_uid and .controller == true) and
    ($pods.items | length) == 1 and $pods.items[0].kind == "Pod" and
    $pods.items[0].metadata.uid == $source_uid and
    any($pods.items[0].metadata.ownerReferences[]?;
      .kind == "InferenceReplica" and .uid == $ir_uid and .controller == true)
  ' >/dev/null
}

nc_watch_owned() {
  local kind="$1" isvc_uid="$2" ir_uid="$3" source_uid="$4" file="$5"
  jq -es --arg kind "${kind}" --arg isvc_uid "${isvc_uid}" --arg ir_uid "${ir_uid}" \
    --arg source_uid "${source_uid}" '
    def objects: (.object // .) | (.items // [.])[];
    length > 0 and all(.[]; all(objects;
      if $kind == "isvc" then
        .kind == "InferenceService" and .metadata.uid == $isvc_uid
      elif $kind == "ir" then
        .kind == "InferenceReplica" and .metadata.uid == $ir_uid and
        any(.metadata.ownerReferences[]?;
          .kind == "InferenceService" and .uid == $isvc_uid and .controller == true)
      elif $kind == "pods" then
        .kind == "Pod" and .metadata.uid == $source_uid and
        any(.metadata.ownerReferences[]?;
          .kind == "InferenceReplica" and .uid == $ir_uid and .controller == true)
      else false end))
  ' "${file}" >/dev/null
}

nc_delete_uid() {
  local resource="$1" name="$2" uid="$3" path="$4" timeout="$5" prefix="$6"
  local current current_uid deadline options
  [[ -n "${uid}" ]] || { nc_error "missing recorded UID for ${resource}/${name}"; return 1; }
  current="$("${kube[@]}" get "${resource}" "${name}" --ignore-not-found -o json)" || return 1
  if [[ -z "${current}" ]]; then return 0; fi
  current_uid="$(jq -er '.metadata.uid | select(type == "string" and length > 0)' <<<"${current}")" || return 1
  jq -e --arg name "${name}" '.metadata.name == $name' <<<"${current}" >/dev/null || return 1
  [[ "${current_uid}" == "${uid}" ]] || {
    nc_error "refusing to delete successor ${resource}/${name} UID ${current_uid}"
    return 1
  }
  options="${artifact_dir}/${prefix}-delete-options.json"
  jq -n --arg uid "${uid}" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}' >"${options}"
  "${kube[@]}" delete --raw "${path}" -f "${options}" >"${artifact_dir}/${prefix}-delete-response.json" || return 1
  deadline=$((SECONDS + timeout))
  while :; do
    current="$("${kube[@]}" get "${resource}" "${name}" --ignore-not-found -o json)" || return 1
    if [[ -z "${current}" ]]; then return 0; fi
    current_uid="$(jq -er '.metadata.uid | select(type == "string" and length > 0)' <<<"${current}")" || return 1
    jq -e --arg name "${name}" '.metadata.name == $name' <<<"${current}" >/dev/null || return 1
    [[ "${current_uid}" == "${uid}" ]] || {
      nc_error "successor ${resource}/${name} appeared during deletion"
      return 1
    }
    ((SECONDS < deadline)) || { nc_error "timed out deleting ${resource}/${name}"; return 1; }
    sleep 1
  done
}

nc_cleanup() {
  local rc="$1"
  if [[ "${trigger_uncertain:-false}" == true ]]; then
    nc_error 'maintenance state is uncertain; retaining capacity blockers'
    return 1
  fi
  if [[ "${trigger_active:-false}" == true ]] && ! nc_clear_maintenance; then
    nc_error 'maintenance cleanup failed; retaining fixture and capacity blockers'
    return 1
  fi
  if [[ "${passed:-false}" != true ]]; then
    ((rc != 0)) || rc=1
    return "${rc}"
  fi
  ((rc == 0)) || return "${rc}"
  nc_delete_uid namespace "${namespace}" "${namespace_uid:-}" \
    "/api/v1/namespaces/${namespace}" "${nc_namespace_delete_timeout_seconds:-90}" namespace || return 1
  nc_delete_uid clusterservingruntimes.ome.io "${runtime}" "${runtime_uid:-}" \
    "/apis/ome.io/v1beta1/clusterservingruntimes/${runtime}" "${nc_runtime_delete_timeout_seconds:-30}" runtime || return 1
}
