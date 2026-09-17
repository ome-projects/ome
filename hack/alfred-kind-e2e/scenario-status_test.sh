#!/usr/bin/env bash
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/scenario-status.sh"

pods='{"items":[
 {"metadata":{"name":"initial-0","uid":"u0","labels":{"ome.io/instance-index":"0"}},"spec":{"nodeName":"a"}},
 {"metadata":{"name":"initial-1","uid":"u1","labels":{"ome.io/instance-index":"1"},"annotations":{"alfred-e2e.ome.io/readiness":"immediate"}},"spec":{"nodeName":"b"}},
 {"metadata":{"name":"initial-2","uid":"u2","labels":{"ome.io/instance-index":"2"}},"spec":{"nodeName":"c"}},
 {"metadata":{"name":"initial-3","uid":"u3","labels":{"ome.io/instance-index":"3"}},"spec":{"nodeName":"d"}},
 {"metadata":{"name":"surge","uid":"u4","labels":{"ome.io/instance-index":"4"}},"spec":{"nodeName":"b"}}
]}'
released="$(initial_release_names 4 <<<"${pods}")"
[[ "${released}" == $'initial-2\ninitial-3' ]] || { echo "released non-initial Pod or skipped held initial Pod: ${released}" >&2; exit 1; }
replacement="$(replacement_pod '["u0","u1","u2","u3"]' <<<"${pods}")"
[[ "$(jq -r '.metadata.uid' <<<"${replacement}")" == u4 ]] || { echo 'selected a baseline Pod as replacement' >&2; exit 1; }
initial="$(jq '.items |= .[:4]' <<<"${pods}")"
assert_initial_spread 4 <<<"${initial}"
for mutation in '.items[1].spec.nodeName = "a"' '.items[1].spec.nodeName = ""' \
  '.items[1].metadata.labels["ome.io/instance-index"] = "0"' '.items |= .[:3]'; do
  if jq "${mutation}" <<<"${initial}" | assert_initial_spread 4; then
    echo "accepted unsafe initial spread: ${mutation}" >&2; exit 1
  fi
done
echo 'scenario-status tests passed'
