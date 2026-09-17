#!/usr/bin/env bash

# The source at index zero is already released by KWOK. Never release a surge
# merely because it appeared during startup: only the initial index range.
initial_release_names() {
  jq -r --argjson count "$1" '.items[] |
    select((.metadata.labels["ome.io/instance-index"] | tonumber) as $index |
      $index > 0 and $index < $count) |
    select(.metadata.annotations["alfred-e2e.ome.io/readiness"] != "immediate") |
    .metadata.name'
}

replacement_pod() {
  jq -c --argjson baseline "$1" '[.items[] |
    select(.metadata.uid as $uid | ($baseline | index($uid)) == null)] |
    sort_by(.metadata.creationTimestamp) | if length == 1 then .[0] else empty end'
}

assert_initial_spread() {
  jq -e --argjson count "$1" '(.items | length) == $count and
    ([.items[].metadata.labels["ome.io/instance-index"] | tonumber] | sort) == [range(0;$count)] and
    all(.items[]; .spec.nodeName != null and .spec.nodeName != "") and
    ([.items[].spec.nodeName] | unique | length) == $count' >/dev/null
}
