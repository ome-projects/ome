#!/usr/bin/env bash
# Pure boundary checks shared by the live runner and its offline tests.
churn_request_published() {
  jq -e --arg owner "$1" --arg ir "$2" '
    . as $s | .requests as $requests |
    [.journal.data["state.json"]|fromjson|.entries[]|select(.workloadUID==$owner or .irUID==$ir)] as $entries |
    .isvc.metadata.uid==$owner and .ir.metadata.uid==$ir and
    ($requests|length)==1 and ($entries|length)==1 and
    $entries[0].workloadUID==$owner and $entries[0].irUID==$ir and
    (["submitted","acknowledged","completed"]|index($entries[0].phase))!=null and
    $requests[0].key==("ome.io/migration-request-v1-"+$entries[0].uuid) and
    $requests[0].value==$entries[0].payload and
    all(.isvc.metadata.annotations // {}|to_entries[]|select(.key|startswith("ome.io/migration-request-v1-"));
      .==$requests[0])' >/dev/null
}

churn_within_window() {
  jq -en --argjson start "$1" --argjson finish "$2" '
    ($start|type=="number") and ($finish|type=="number") and
    $start>=0 and $finish>=$start and ($finish-$start)<30' >/dev/null
}

churn_monotonic_time() {
  perl -MTime::HiRes=clock_gettime,CLOCK_MONOTONIC -e 'printf "%.9f\n", clock_gettime(CLOCK_MONOTONIC)'
}

churn_watch_requests() {
  local attempt
  # kubectl can be between writes of one JSON watch object. Parse a complete
  # stream only; never turn malformed/truncated evidence into an empty list.
  for attempt in 1 2 3 4 5; do
    if jq -cs --arg owner "$2" '
      if length==0 or any(.[]; .metadata.uid!=$owner) then error("watch identity changed") else
      [.[]|.metadata.annotations // {}|to_entries[]|select(.key|startswith("ome.io/migration-request-v1-"))]|unique end' "$1" 2>/dev/null; then
      return 0
    fi
    sleep 0.05
  done
  return 1
}

churn_check_mutation() {
  jq -en --argjson before "$1" --argjson after "$2" --arg field "$3" --arg key "$4" '
    def stable: del(.metadata.resourceVersion,.metadata.managedFields,.metadata[$field][$key]);
    ($field == "annotations" or $field == "labels") and
    $before.apiVersion == "v1" and $before.kind == "Namespace" and
    ($before.metadata.uid | type == "string" and length > 0) and
    ($before.metadata.resourceVersion | type == "string" and length > 0) and
    ($after.metadata.resourceVersion | type == "string" and length > 0) and
    $before.metadata.resourceVersion != $after.metadata.resourceVersion and
    ($after.metadata[$field][$key] | type == "string") and
    $before.metadata[$field][$key] != $after.metadata[$field][$key] and
    ($before | stable) == ($after | stable)' >/dev/null
}

churn_decode_held() {
  local held="$1" prefix="$2" request_hash result_hash
  jq -erj '.requestBytes | @base64d' "${held}" >"${prefix}-request.json" || return 1
  jq -erj '.resultBytes | @base64d' "${held}" >"${prefix}-result.json" || return 1
  request_hash="$(shasum -a 256 "${prefix}-request.json" | awk '{print $1}')"
  result_hash="$(shasum -a 256 "${prefix}-result.json" | awk '{print $1}')"
  jq -e --arg request "${request_hash}" --arg result "${result_hash}" '
    .requestSHA256 == $request and .resultSHA256 == $result and
    (.nonce | type == "string" and length >= 32) and
    (.heldAt | type == "string") and (.deadline | type == "string")' "${held}" >/dev/null
}

# A failed/uncertain install may already have left the deployment unchanged.
# Never undo a different writer's changes to either owned array.
churn_restore_patch() {
  jq -cn --argjson old "$1" --argjson current "$2" --arg registry "$3" '
    ($old.spec.template.spec.volumes) as $volumes |
    ($old.spec.template.spec.containers[0].args) as $args |
    ($volumes|map(if .name=="simulation" then .configMap.name=$registry else . end)) as $installedVolumes |
    ($args|map(if startswith("--simulation-timeout=") then "--simulation-timeout=20s" else . end)) as $installedArgs |
    if $current.metadata.uid!=$old.metadata.uid or
       ($current.metadata.resourceVersion|type!="string" or length==0) or
       ($old.spec.template.spec.containers|length)!=1 or
       ($current.spec.template.spec.containers|length)!=1 or
       $old.spec.template.spec.containers[0].name!="alfred" or
       $current.spec.template.spec.containers[0].name!="alfred"
    then error("deployment identity changed")
    elif $current.spec.template.spec.volumes==$volumes and $current.spec.template.spec.containers[0].args==$args then []
    elif $current.spec.template.spec.volumes!=$installedVolumes or $current.spec.template.spec.containers[0].args!=$installedArgs
    then error("deployment arguments or volumes changed during barrier run")
    else [{op:"test",path:"/metadata/uid",value:$old.metadata.uid},
          {op:"test",path:"/metadata/resourceVersion",value:$current.metadata.resourceVersion},
          {op:"replace",path:"/spec/template/spec/volumes",value:$volumes},
          {op:"replace",path:"/spec/template/spec/containers/0/args",value:$args}] end'
}
