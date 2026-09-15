#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Two real-shaped gang pod sets: only the source is initially ready/routed.
baseline="$(jq -n '
  def pod($uid; $ready): {metadata:{uid:$uid},status:{conditions:[
    {type:"Ready",status:$ready},{type:"ome.io/serving",status:$ready}]}};
  {routingService:"gang-engine-rev-hash",sourceUIDs:["source-leader","source-worker"],
   replacementUIDs:["replacement-leader","replacement-worker"],
   sourceRoutingUID:"source-leader",replacementRoutingUID:"replacement-leader",
   pods:{items:[pod("source-leader";"True"),pod("source-worker";"True"),
     pod("replacement-leader";"False"),pod("replacement-worker";"False")]},
   endpoints:{items:[{metadata:{labels:{"kubernetes.io/service-name":"gang-engine-rev-hash"}},
     endpoints:[{targetRef:{uid:"source-leader"},conditions:{ready:true}}]}]}}')"
check() {
  local name="$1" mutation="$2" want="$3" sample
  sample="$(jq "${mutation}" <<<"${baseline}" | jq -c -f "${dir}/handoff-snapshot.jq")"
  if ! jq -e --argjson want "${want}" '[.sourceSafe,.replacementSafe] == $want' <<<"${sample}" >/dev/null; then
    echo "${name}: expected ${want}, got ${sample}" >&2; exit 1
  fi
}

check held-source '.' '[true,false]'
check source-deleted-before-ready '.pods.items |= map(select(.metadata.uid != "source-worker"))' '[false,false]'
check source-terminating-before-ready '.pods.items[0].metadata.deletionTimestamp="2026-09-15T00:00:00Z"' '[false,false]'
check routing-gap '.endpoints.items[0].endpoints=[]' '[false,false]'
check headless-is-not-routing '.endpoints.items[0].metadata.labels["kubernetes.io/service-name"]="gang-engine"' '[false,false]'

replacement_ready='.pods.items |= map(select(.metadata.uid | startswith("replacement")) | .status.conditions |= map(.status="True"))'
replacement_routed='.endpoints.items[0].endpoints=[{targetRef:{uid:"replacement-leader"},conditions:{ready:true}}]'
check ready-without-route "${replacement_ready}" '[false,false]'
check routed-partial-gang "${replacement_ready} | ${replacement_routed} | .pods.items[1].status.conditions[0].status=\"False\"" '[false,false]'
check routed-wrong-pod "${replacement_ready} | ${replacement_routed} | .pods.items[1].metadata.uid=\"unrelated-worker\"" '[false,false]'
check terminating-endpoint "${replacement_ready} | ${replacement_routed} | .endpoints.items[0].endpoints[0].conditions.terminating=true" '[false,false]'
check completed-handoff "${replacement_ready} | ${replacement_routed}" '[false,true]'
check single-handoff "${replacement_ready} | ${replacement_routed} | .sourceUIDs=[\"source-leader\"] | .replacementUIDs=[\"replacement-leader\"] | .pods.items |= map(select(.metadata.uid==\"replacement-leader\"))" '[false,true]'
echo 'handoff API snapshot tests passed'
