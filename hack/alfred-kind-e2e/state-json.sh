#!/usr/bin/env bash
# Read optional public-API JSON without altering a nonempty response.
alfred_node_state_is() {
  local configmap_json="$1" node="$2" state="$3"
  jq -e --arg key "node.${node}" --arg state "${state}" \
    '(.data[$key] // "null" | fromjson) | .state == $state' \
    <<<"${configmap_json:-null}" >/dev/null 2>&1
}

alfred_migration_phase() {
  jq -r '.phase // ""' <<<"${1:-null}"
}
