#!/usr/bin/env bash
set -euo pipefail
# Synthetic JSON below is verifier input only; it never writes API status or
# reaches a cluster. Positive fixtures use the repository's handoff witnesses.

draft_dir="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
verifier="${draft_dir}/verify-namespace-churn.sh"
harness="${HARNESS_DIR:-$draft_dir}"
[[ -f "${verifier}" ]] || { echo 'FAIL: namespace churn verifier is not implemented' >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
uuid=01234567-89ab-4cde-8fab-0123456789ab
noise_key=alfred-e2e.ome.io/churn

sha256() { shasum -a 256 "$1" | awk '{print $1}'; }
b64() { base64 <"$1" | tr -d '\n'; }

write_sample() {
  local out=$1 workload=$2 handoff=$3 timestamp=$4 phase=$5 reason=${6:-}
  jq -cn --arg workload "$workload" --arg timestamp "$timestamp" --arg phase "$phase" \
    --arg reason "$reason" --arg owner owner-uid --arg iruid ir-uid --arg uuid "$uuid" \
    --slurpfile h "$handoff" '
    $h[0] as $h |
    (if $workload=="single" then [$h.source]
     else $h.source.pods end) as $source |
    (if $workload=="single" then [$h.surge.replacement]
     else $h.surge.pods end) as $replacement |
    (if $workload=="single" then [$h.source.uid]
     else [$h.source.pods[].uid] end) as $sourceUIDs |
    (if $workload=="single" then [$h.surge.replacement.uid]
     else [$h.surge.pods[].uid] end) as $replacementUIDs |
    (if $workload=="single" then $h.source.routingService
     else $h.source.routingService end) as $service |
    def pod($p;$ready):
      {metadata:{uid:$p.uid,name:($p.name // ($workload+"-"+$p.uid)),namespace:"alfred-e2e",
       labels:{"ome.io/inferenceservice":$workload}},
       spec:{nodeName:$p.node},status:{conditions:[
        {type:"Ready",status:(if $ready then "True" else "False" end)},
        {type:"ome.io/serving",status:(if $ready then "True" else "False" end)}]}};
    def cycle($recommendations):
      {data:{"last-cycle.json":({timestamp:$timestamp,mode:"execute",recommendations:$recommendations}|tojson)}};
    def endpoints($uids):
      {items:[{metadata:{labels:{"kubernetes.io/service-name":$service}},endpoints:
        [$uids[]|{targetRef:{uid:.},conditions:{ready:true,terminating:false}}]}]};
    {isvc:{metadata:{uid:$owner,name:$workload,namespace:"alfred-e2e",annotations:{}}},
     ir:{metadata:{uid:$iruid,name:($workload+"-engine"),namespace:"alfred-e2e",
       ownerReferences:[{uid:$owner,kind:"InferenceService",controller:true}]},
       status:{readyReplicas:1,servingReplicas:1,availableReplicas:1,migrations:[]}},
     pods:{items:[$source[]|pod(.;true)]},endpoints:endpoints(
       if $workload=="single" then $sourceUIDs else [$h.source.pods[]|select(.runner=="leader")|.uid] end),
     recommendations:cycle(if $reason=="" then [] else [{workload:("alfred-e2e/"+$workload),component:"engine",
       instance:0,fromNode:$h.request.payload.from_node,policy:"nodehealth",reason:"NodeMaintenance",
       outcome:"withheld",dispatchStatus:"withheld",dispatchReason:$reason}] end),
     journal:{data:{"state.json":({version:"v1",entries:[]}|tojson)}},requests:[]}
    | if $phase=="requested" then
        .requests=[{key:$h.request.annotationKey,value:($h.request.payload|tojson)}] |
        .journal.data["state.json"] = ({version:"v1",entries:[{uuid:$uuid,workloadUID:$owner,
          irUID:$iruid,phase:"submitted",payload:($h.request.payload|tojson)}]}|tojson)
      elif $phase=="completed" then
        .requests=[{key:$h.request.annotationKey,value:($h.request.payload|tojson)}] |
        .ir.status.migrations=[$h.completed.migration] |
        .pods.items=[$replacement[]|pod(.;true)] |
        .endpoints=endpoints(if $workload=="single" then $replacementUIDs
          else [$h.surge.pods[]|select(.runner=="leader")|.uid] end) |
        .journal.data["state.json"] = ({version:"v1",entries:[{uuid:$uuid,workloadUID:$owner,
          irUID:$iruid,phase:"completed",completedAt:$h.completed.migration.completedAt,
          payload:($h.request.payload|tojson)}]}|tojson)
      else . end' >"$out"
}

build_fixture() {
  local workload=$1 out=$2 handoff profile gang count source_node routing before after mutation request result req_hash result_hash
  mkdir -p "$out"
  if [[ "$workload" == single ]]; then
    handoff="${harness}/testdata/evidence-valid.json"; gang=false; count=1; source_node=alfred-gpu-a
  else
    handoff="${harness}/testdata/evidence-gang-valid.json"; gang=true; count=2; source_node=alfred-kwok-gpu-a
  fi
  cp "$handoff" "$out/handoff-evidence.json"
  jq '.request.payload.requested_at="2026-09-14T12:00:09.000000001Z"' \
    "$out/handoff-evidence.json" >"$out/handoff.tmp"
  mv "$out/handoff.tmp" "$out/handoff-evidence.json"
  handoff="$out/handoff-evidence.json"
  profile=$(jq -cn --arg scheduler "$(if [[ $workload == single ]]; then echo alfred-default-scheduler; else echo ome-scheduler; fi)" \
    --arg backend "kind-${workload}-v135" '{schedulerName:$scheduler,backend:$backend,schedulerVersion:"v1.35.4",configurationID:"sha256:test-config"}')
  before=$(jq -cn --arg key "$noise_key" '{apiVersion:"v1",kind:"Namespace",metadata:{name:"noise",uid:"noise-uid",
    resourceVersion:"1",labels:{($key):"0"},annotations:{($key):"0"}},status:{phase:"Active"}}')
  for index in 1 2 3 4; do
    local prefix="$out/barrier-$index" mode=labels
    [[ $index == 4 ]] && mode=annotations
    printf '%s\n' "$before" >"${prefix}-namespace-before.json"
    request=$(jq -cn --argjson profile "$profile" --arg workload "$workload" --argjson gang "$gang" \
      --arg node "$source_node" --argjson ns "$before" --argjson count "$count" '
      def source($i): {apiVersion:"v1",kind:"Pod",metadata:{namespace:"alfred-e2e",
        name:($workload+"-source-"+($i|tostring)),uid:(if $workload=="single" then "source-uid"
          elif $i==0 then "source-leader" else "source-worker" end),labels:{"ome.io/inferenceservice":$workload}},
        spec:{nodeName:(if $workload=="gang" and $i==1 then "alfred-kwok-gpu-c" else $node end),
          schedulerName:$profile.schedulerName}};
      def replacement($i): {apiVersion:"v1",kind:"Pod",metadata:{namespace:"alfred-e2e",
        name:($workload+"-replacement-"+($i|tostring))},spec:{schedulerName:$profile.schedulerName}};
      {schemaVersion:"v1",requestID:"preflight",profile:$profile,snapshotID:"snapshot-1",
       snapshotTime:"2026-09-14T12:00:00Z",excludedNodes:[$node],migrationFromNode:$node,
       sourcePods:[range(0;$count)|source(.)],replacementPods:[range(0;$count)|replacement(.)],
       clusterObjects:([$ns]+[{apiVersion:"v1",kind:"Node",metadata:{name:"target-node"}}])}
      | if $gang then .requireGang=true else . end')
    printf '%s\n' "$request" >"${prefix}-request.json"
    result=$(jq -cn --argjson r "$request" '
      {schemaVersion:$r.schemaVersion,requestID:$r.requestID,snapshotID:$r.snapshotID,
       snapshotTime:$r.snapshotTime,profile:$r.profile,decision:"Feasible",reason:"PlacementFound",
       placements:[$r.replacementPods[]|{pod:{namespace:.metadata.namespace,name:.metadata.name},nodeName:"target-node"}]}')
    printf '%s\n' "$result" >"${prefix}-result.json"
    req_hash=$(sha256 "${prefix}-request.json"); result_hash=$(sha256 "${prefix}-result.json")
    held_second=$((index * 2 - 1))
    deadline_second=$((held_second + 2))
    jq -cn --arg requestBytes "$(b64 "${prefix}-request.json")" --arg resultBytes "$(b64 "${prefix}-result.json")" \
      --arg requestSHA256 "$req_hash" --arg resultSHA256 "$result_hash" --arg nonce "nonce-000000000000000000000000000${index}" \
      --arg heldAt "2026-09-14T12:00:0${held_second}.000000001Z" --arg deadline "2026-09-14T12:00:0${deadline_second}.000000001Z" \
      '{requestBytes:$requestBytes,resultBytes:$resultBytes,requestSHA256:$requestSHA256,resultSHA256:$resultSHA256,
        nonce:$nonce,heldAt:$heldAt,deadline:$deadline}' >"${prefix}-held.json"
    : >"${prefix}-namespace-mutations.jsonl"
    for mutation in 1 2 3; do
      after=$(jq --arg mode "$mode" --arg key "$noise_key" --arg value "${index}-${mutation}" \
        '.metadata.resourceVersion=((.metadata.resourceVersion|tonumber)+1|tostring) | .metadata[$mode][$key]=$value' <<<"$before")
      printf '%s\n' "$after" >>"${prefix}-namespace-mutations.jsonl"
      before=$after
    done
    printf '%s\n' "$after" >"${prefix}-namespace-after.json"
    jq -cn --arg nonce "nonce-000000000000000000000000000${index}" '{nonce:$nonce}' >"${prefix}-release-request.json"
    jq -cn --arg requestSHA256 "$req_hash" --arg resultSHA256 "$result_hash" \
      --arg releasedAt "2026-09-14T12:00:0${held_second}.500000001Z" \
      '{releasedAt:$releasedAt,requestSHA256:$requestSHA256,resultSHA256:$resultSHA256}' >"${prefix}-receipt.json"
    write_sample "${prefix}-before-release.json" "$workload" "$handoff" "2026-09-14T12:00:0${index}.100000001Z" idle
    if ((index < 4)); then
      write_sample "${prefix}-observed.json" "$workload" "$handoff" "2026-09-14T12:00:0${held_second}.600000001Z" idle SchedulingStateChanged
      cat "${prefix}-before-release.json" "${prefix}-observed.json" >"${prefix}-api.jsonl"
    else
      write_sample "${prefix}-observed.json" "$workload" "$handoff" "2026-09-14T12:00:09.600000001Z" requested
    fi
  done
  jq -cn '{metadata:{uid:"owner-uid",annotations:{}}}' >"$out/request-annotation-watch.jsonl"
  jq -cn --arg uuid "$uuid" --slurpfile h "$handoff" \
    '{metadata:{uid:"owner-uid",annotations:{("ome.io/migration-request-v1-"+$uuid):($h[0].request.payload|tojson)}}}' \
    >>"$out/request-annotation-watch.jsonl"
  jq -cn --arg requestSHA256 "$(jq -r '.requestSHA256' "$out/barrier-4-receipt.json")" \
    '{releaseStarted:10,observationFinished:15,budgetSeconds:30,requestSHA256:$requestSHA256}' \
    >"$out/barrier-4-timing.json"
  write_sample "$out/completion-baseline.json" "$workload" "$handoff" "2026-09-14T12:00:16.000000001Z" completed
  : >"$out/completion-cycles.jsonl"
  for sec in 17 18 19; do
    write_sample "$out/cycle.json" "$workload" "$handoff" "2026-09-14T12:00:${sec}.000000001Z" completed
    cat "$out/cycle.json" >>"$out/completion-cycles.jsonl"
  done
  cp "$out/completion-cycles.jsonl" "$out/completion-api.jsonl"
  rm "$out/cycle.json"
  jq -n --arg workload "$workload" --argjson profile "$profile" --slurpfile h "$out/handoff-evidence.json" \
    --slurpfile b "$out/completion-baseline.json" --slurpfile c "$out/completion-cycles.jsonl" \
    '{workload:$workload,noiseUID:"noise-uid",ownerUID:"owner-uid",irUID:"ir-uid",profile:$profile,
      handoff:$h[0],completionBaseline:$b[0],completed:$c}' >"$out/evidence.json"
}

verify() { HARNESS_DIR="$harness" bash "$verifier" "$1" >/dev/null; }
reseal_barrier() {
  local dir=$1 index=$2 prefix="$1/barrier-$2" request_hash result_hash
  request_hash=$(sha256 "${prefix}-request.json"); result_hash=$(sha256 "${prefix}-result.json")
  jq --arg requestBytes "$(b64 "${prefix}-request.json")" --arg resultBytes "$(b64 "${prefix}-result.json")" \
    --arg requestSHA256 "$request_hash" --arg resultSHA256 "$result_hash" \
    '.requestBytes=$requestBytes | .resultBytes=$resultBytes |
     .requestSHA256=$requestSHA256 | .resultSHA256=$resultSHA256' \
    "${prefix}-held.json" >"$tmp/resealed-held"
  mv "$tmp/resealed-held" "${prefix}-held.json"
  jq --arg requestSHA256 "$request_hash" --arg resultSHA256 "$result_hash" \
    '.requestSHA256=$requestSHA256 | .resultSHA256=$resultSHA256' \
    "${prefix}-receipt.json" >"$tmp/resealed-receipt"
  mv "$tmp/resealed-receipt" "${prefix}-receipt.json"
}
reseal_summary() {
  local dir=$1
  jq --slurpfile baseline "$dir/completion-baseline.json" \
    --slurpfile cycles "$dir/completion-cycles.jsonl" \
    '.completionBaseline=$baseline[0] | .completed=$cycles' \
    "$dir/evidence.json" >"$tmp/resealed-evidence"
  mv "$tmp/resealed-evidence" "$dir/evidence.json"
}
reject_mutation() {
  local label=$1 mutation=$2
  rm -rf "$tmp/bad"; cp -R "$tmp/single" "$tmp/bad"
  eval "$mutation"
  if verify "$tmp/bad" 2>/dev/null; then echo "FAIL: accepted ${label}" >&2; exit 1; fi
}

build_fixture single "$tmp/single"
build_fixture gang "$tmp/gang"
verify "$tmp/single" || { echo 'FAIL: rejected valid single fixture' >&2; exit 1; }
verify "$tmp/gang" || { echo 'FAIL: rejected valid gang fixture' >&2; exit 1; }
cp -R "$tmp/single" "$tmp/present"
request_key=$(jq -r '.request.annotationKey' "$tmp/present/handoff-evidence.json")
request_value=$(jq -c '.request.payload' "$tmp/present/handoff-evidence.json")
for file in "$tmp/present/barrier-4-observed.json" "$tmp/present/completion-baseline.json"; do
  jq --arg key "$request_key" --argjson value "$request_value" \
    '.isvc.metadata.annotations[$key]=($value|tojson)' "$file" >"$tmp/x"
  mv "$tmp/x" "$file"
done
reseal_summary "$tmp/present"
verify "$tmp/present" || { echo 'FAIL: rejected matching present annotation' >&2; exit 1; }

reject_mutation 'held/request byte mismatch' "jq '.requestID=\"other\"' '$tmp/bad/barrier-1-request.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-1-request.json'"
reject_mutation 'release nonce mismatch' "jq '.nonce=\"wrong\"' '$tmp/bad/barrier-1-release-request.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-1-release-request.json'"
reject_mutation 'receipt digest mismatch' "jq '.resultSHA256=\"00\"' '$tmp/bad/barrier-1-receipt.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-1-receipt.json'"
reject_mutation 'response envelope mismatch after valid reseal' "jq '.snapshotID=\"other\"' '$tmp/bad/barrier-2-result.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-2-result.json'; reseal_barrier '$tmp/bad' 2"
reject_mutation 'profile changed for one held worker call' "jq '.profile.backend=\"other\"' '$tmp/bad/barrier-2-request.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-2-request.json'; jq '.profile.backend=\"other\"' '$tmp/bad/barrier-2-result.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-2-result.json'; reseal_barrier '$tmp/bad' 2"
reject_mutation 'source node changed for one held worker call' "jq '.sourcePods[0].spec.nodeName=\"other-node\"' '$tmp/bad/barrier-3-request.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-3-request.json'; reseal_barrier '$tmp/bad' 3"
reject_mutation 'namespace mutation chain gap' "jq '.metadata.resourceVersion=\"99\"' '$tmp/bad/barrier-2-namespace-after.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-2-namespace-after.json'"
reject_mutation 'label hold changed annotation' "jq '.metadata.annotations.extra=\"bad\"' '$tmp/bad/barrier-3-namespace-after.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-3-namespace-after.json'"
reject_mutation 'annotation hold changed label' "jq '.metadata.labels.extra=\"bad\"' '$tmp/bad/barrier-4-namespace-after.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-4-namespace-after.json'"
reject_mutation 'wrong withheld reason' "jq '.recommendations.data[\"last-cycle.json\"]|=(fromjson|.recommendations[0].dispatchReason=\"SourceChanged\"|tojson)' '$tmp/bad/barrier-1-observed.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-1-observed.json'"
reject_mutation 'label hold published request' "jq --arg uuid '$uuid' '.isvc.metadata.annotations[\"ome.io/migration-request-v1-\"+\$uuid]=\"{}\"' '$tmp/bad/barrier-2-observed.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-2-observed.json'"
reject_mutation 'source loss under withheld retry' "jq '.pods.items=[]' '$tmp/bad/barrier-3-observed.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-3-observed.json'"
reject_mutation 'owner changed between holds' "jq '.isvc.metadata.uid=\"new-owner\"' '$tmp/bad/barrier-4-before-release.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-4-before-release.json'"
reject_mutation 'IR changed between holds' "jq '.ir.metadata.uid=\"new-ir\"' '$tmp/bad/barrier-3-before-release.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-3-before-release.json'"
reject_mutation 'source lost from routing' "jq '.endpoints.items=[]' '$tmp/bad/barrier-1-observed.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-1-observed.json'"
reject_mutation 'unsafe intermediate label-hold API snapshot' "jq '.pods.items=[]' '$tmp/bad/barrier-1-before-release.json' >'$tmp/x'; { cat '$tmp/x'; cat '$tmp/bad/barrier-1-observed.json'; } >'$tmp/bad/barrier-1-api.jsonl'"
reject_mutation 'observed snapshot is not final API sample' "cat '$tmp/bad/barrier-1-before-release.json' >>'$tmp/bad/barrier-1-api.jsonl'"
reject_mutation 'request existed before release' "jq --arg uuid '$uuid' '.isvc.metadata.annotations[\"ome.io/migration-request-v1-\"+\$uuid]=\"{}\"' '$tmp/bad/barrier-4-before-release.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-4-before-release.json'"
reject_mutation 'second UUID in full watch' "jq -cn '{metadata:{uid:\"owner-uid\",annotations:{\"ome.io/migration-request-v1-ffffffff-ffff-4fff-8fff-ffffffffffff\":\"{}\"}}}' >>'$tmp/bad/request-annotation-watch.jsonl'"
reject_mutation 'same UUID changed payload in full watch' "jq -cn --arg uuid '$uuid' '{metadata:{uid:\"owner-uid\",annotations:{(\"ome.io/migration-request-v1-\"+\$uuid):\"{}\"}}}' >>'$tmp/bad/request-annotation-watch.jsonl'"
reject_mutation 'positive sample request payload mismatch' "jq '.requests[0].value=\"{}\"' '$tmp/bad/barrier-4-observed.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-4-observed.json'"
reject_mutation 'positive sample stale request UUID' "jq '.requests[0].key=\"ome.io/migration-request-v1-ffffffff-ffff-4fff-8fff-ffffffffffff\"' '$tmp/bad/barrier-4-observed.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-4-observed.json'"
reject_mutation 'positive sample duplicate watched request' "jq '.requests += [.requests[0]]' '$tmp/bad/barrier-4-observed.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-4-observed.json'"
reject_mutation 'present consumed annotation disagrees with watch' "jq --arg uuid '$uuid' '.isvc.metadata.annotations[\"ome.io/migration-request-v1-\"+\$uuid]=\"{}\"' '$tmp/bad/barrier-4-observed.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-4-observed.json'"
reject_mutation 'positive journal remained prepared' "jq '.journal.data[\"state.json\"]|=(fromjson|.entries[0].phase=\"prepared\"|tojson)' '$tmp/bad/barrier-4-observed.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-4-observed.json'"
reject_mutation 'handoff verifier regression' "jq '.completed.migration.phase=\"Failed\"' '$tmp/bad/handoff-evidence.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/handoff-evidence.json'"
reject_mutation 'completion cycle not strictly newer' "first=\$(sed -n '1p' '$tmp/bad/completion-cycles.jsonl'); { printf '%s\n' \"\$first\"; printf '%s\n' \"\$first\"; sed -n '3p' '$tmp/bad/completion-cycles.jsonl'; } >'$tmp/x'; mv '$tmp/x' '$tmp/bad/completion-cycles.jsonl'; reseal_summary '$tmp/bad'"
reject_mutation 'duplicate completed migration' "jq '.ir.status.migrations += [.ir.status.migrations[0]]' '$tmp/bad/completion-baseline.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/completion-baseline.json'; reseal_summary '$tmp/bad'"
reject_mutation 'duplicate completed journal' "jq '.journal.data[\"state.json\"]|=(fromjson|.entries += [.entries[0]]|tojson)' '$tmp/bad/completion-baseline.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/completion-baseline.json'; reseal_summary '$tmp/bad'"
reject_mutation 'source pod remains after completion' "jq '.pods.items[0].metadata.uid=\"source-uid\"' '$tmp/bad/completion-baseline.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/completion-baseline.json'; reseal_summary '$tmp/bad'"
reject_mutation 'selected cycle predates journal completion' "for f in '$tmp/bad/completion-baseline.json' '$tmp/bad/completion-cycles.jsonl' '$tmp/bad/completion-api.jsonl'; do jq '.journal.data[\"state.json\"]|=(fromjson|.entries[0].completedAt=\"2026-09-14T12:00:17.500000001Z\"|tojson)' \"\$f\" >'$tmp/x'; mv '$tmp/x' \"\$f\"; done; reseal_summary '$tmp/bad'"
reject_mutation 'journal matches only IR identity' "jq '.journal.data[\"state.json\"]|=(fromjson|.entries[0].workloadUID=\"other-owner\"|tojson)' '$tmp/bad/completion-baseline.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/completion-baseline.json'; reseal_summary '$tmp/bad'"
reject_mutation 'completion lost watched request evidence' "jq '.requests=[]' '$tmp/bad/completion-baseline.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/completion-baseline.json'; reseal_summary '$tmp/bad'"
reject_mutation 'unsafe unselected completion API sample' "jq '.pods.items[0].metadata.uid=\"source-uid\"' '$tmp/bad/completion-baseline.json' >'$tmp/x'; { cat '$tmp/x'; cat '$tmp/bad/completion-api.jsonl'; } >'$tmp/bad/api.new'; mv '$tmp/bad/api.new' '$tmp/bad/completion-api.jsonl'"
reject_mutation 'barrier hold exceeds ten seconds' "jq '.deadline=\"2026-09-14T12:00:12.000000002Z\"' '$tmp/bad/barrier-1-held.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-1-held.json'"
reject_mutation 'held worker call replays before prior observed cycle' "jq '.heldAt=\"2026-09-14T12:00:01.550000001Z\"' '$tmp/bad/barrier-2-held.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-2-held.json'"
reject_mutation 'positive observation reached thirty-second bound' "jq '.observationFinished=40' '$tmp/bad/barrier-4-timing.json' >'$tmp/x'; mv '$tmp/x' '$tmp/bad/barrier-4-timing.json'"

echo 'namespace churn verifier: valid consumed-annotation single/gang and 37 adversarial cases passed'
