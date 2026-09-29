#!/usr/bin/env bash
# Independently verify the raw evidence emitted by namespace-churn.sh.
set -euo pipefail

artifact_dir=${1:-}
[[ -n "$artifact_dir" && -d "$artifact_dir" ]] || { echo 'usage: verify-namespace-churn.sh ARTIFACT_DIR' >&2; exit 2; }
artifact_dir="$(CDPATH= cd -- "$artifact_dir" && pwd)"
draft_dir="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
harness=${HARNESS_DIR:-$draft_dir}
[[ -f "$harness/namespace-churn-lib.sh" && -f "$harness/placement-pause-sample.jq" ]] || {
  echo 'missing namespace churn verification helpers' >&2; exit 2;
}
source "$harness/namespace-churn-lib.sh"
for command in jq shasum cmp mktemp; do command -v "$command" >/dev/null || exit 2; done

required() { [[ -f "$1" && ! -L "$1" ]] || { echo "missing raw evidence: $1" >&2; exit 1; }; }
required "$artifact_dir/evidence.json"
required "$artifact_dir/handoff-evidence.json"
required "$artifact_dir/request-annotation-watch.jsonl"
required "$artifact_dir/completion-baseline.json"
required "$artifact_dir/completion-cycles.jsonl"
required "$artifact_dir/completion-api.jsonl"
required "$artifact_dir/barrier-4-timing.json"

workload=$(jq -er '.workload | select(.=="single" or .=="gang")' "$artifact_dir/evidence.json")
if [[ $workload == single ]]; then original=verify-evidence.jq; else original=verify-gang-evidence.jq; fi
required "$harness/$original"
jq -e -f "$harness/$original" "$artifact_dir/handoff-evidence.json" >/dev/null
jq -e --slurpfile handoff "$artifact_dir/handoff-evidence.json" \
  '.handoff==$handoff[0] and (.noiseUID|type=="string" and length>0) and
   (.ownerUID|type=="string" and length>0) and (.irUID|type=="string" and length>0) and
   (.profile|type=="object") and
   all(.profile.schedulerName,.profile.backend,.profile.schedulerVersion,.profile.configurationID;
     type=="string" and length>0)' "$artifact_dir/evidence.json" >/dev/null

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

time_after() {
  jq -en -L "$harness" --arg newer "$1" --arg older "$2" \
    'include "placement-pause-sample"; ($newer|pp_time)>($older|pp_time)' >/dev/null
}

verify_idle_snapshot() {
  local file=$1
  jq -e --slurpfile e "$artifact_dir/evidence.json" --slurpfile h "$artifact_dir/handoff-evidence.json" '
    $e[0] as $e | $h[0] as $h |
    (if $e.workload=="single" then [$h.source|{uid,node}] else [$h.source.pods[]|{uid,node}] end) as $source |
    ([$source[].uid]) as $sourceUIDs |
    (if $e.workload=="single" then [$h.source.uid]
     else [$h.source.pods[]|select(.runner=="leader")|.uid] end) as $routed |
    $h.source.routingService as $service |
    . as $s |
    def request_annotations:
      [$s.isvc.metadata.annotations // {} | to_entries[] |
        select(.key|startswith("ome.io/migration-request-v1-"))];
    def journal_entries:
      [$s.journal.data["state.json"]|fromjson|.entries[]? |
        select(.workloadUID==$e.ownerUID or .irUID==$e.irUID)];
    def ready_serving($pod): any($s.pods.items[]; .metadata.uid==$pod.uid and .spec.nodeName==$pod.node and
      .metadata.deletionTimestamp==null and
      any(.status.conditions[]?;.type=="Ready" and .status=="True") and
      any(.status.conditions[]?;.type=="ome.io/serving" and .status=="True"));
    $s.isvc.metadata.uid==$e.ownerUID and $s.isvc.metadata.name==$e.workload and
    $s.isvc.metadata.namespace=="alfred-e2e" and $s.isvc.metadata.deletionTimestamp==null and
    $s.ir.metadata.uid==$e.irUID and $s.ir.metadata.name==($e.workload+"-engine") and
    any($s.ir.metadata.ownerReferences[]?;.uid==$e.ownerUID and .kind=="InferenceService" and .controller==true) and
    $s.ir.status.readyReplicas==1 and $s.ir.status.servingReplicas==1 and $s.ir.status.availableReplicas==1 and
    ([$s.pods.items[].metadata.uid]|sort)==($sourceUIDs|sort) and all($source[]; ready_serving(.)) and
    ([$routed[] as $uid | any($s.endpoints.items[];
      .metadata.labels["kubernetes.io/service-name"]==$service and any(.endpoints[]?;
        .targetRef.uid==$uid and .conditions.ready==true and .conditions.terminating!=true))] | all) and
    (request_annotations|length)==0 and (($s.ir.status.migrations // [])|length)==0 and
    (journal_entries|length)==0 and ($s.requests|type=="array" and length==0)' "$file" >/dev/null
}

profile=$(jq -c '.profile' "$artifact_dir/evidence.json")
noise_uid=$(jq -r '.noiseUID' "$artifact_dir/evidence.json")
prior_release=''
prior_observed=''
for index in 1 2 3 4; do
  prefix="$artifact_dir/barrier-$index"
  for suffix in held request result namespace-before namespace-mutations namespace-after \
    release-request receipt before-release observed; do
    extension=json; [[ $suffix == namespace-mutations ]] && extension=jsonl
    required "${prefix}-${suffix}.${extension}"
  done
  if ((index < 4)); then required "${prefix}-api.jsonl"; fi

  request_file="${prefix}-request.json"; result_file="${prefix}-result.json"
  request_hash=$(shasum -a 256 "$request_file" | awk '{print $1}')
  result_hash=$(shasum -a 256 "$result_file" | awk '{print $1}')
  jq -erj '.requestBytes|@base64d' "${prefix}-held.json" >"$tmp/request-$index"
  jq -erj '.resultBytes|@base64d' "${prefix}-held.json" >"$tmp/result-$index"
  cmp -s "$tmp/request-$index" "$request_file"
  cmp -s "$tmp/result-$index" "$result_file"
  jq -e --arg request "$request_hash" --arg result "$result_hash" '
    .requestSHA256==$request and .resultSHA256==$result and
    (.nonce|type=="string" and length>=32) and (.heldAt|type=="string") and (.deadline|type=="string")' \
    "${prefix}-held.json" >/dev/null
  jq -e -L "$harness" 'include "placement-pause-sample";
    (.heldAt|pp_time) as $held | (.deadline|pp_time) as $deadline |
    $deadline>$held and
    (($deadline[0]-$held[0])<10 or
      (($deadline[0]-$held[0])==10 and $deadline[1]<=$held[1]))' \
    "${prefix}-held.json" >/dev/null
  held_at=$(jq -er '.heldAt' "${prefix}-held.json")
  if ((index > 1)); then
    time_after "$held_at" "$prior_release"
    time_after "$held_at" "$prior_observed"
  fi
  jq -e --slurpfile held "${prefix}-held.json" '.nonce==$held[0].nonce' \
    "${prefix}-release-request.json" >/dev/null
  jq -e -L "$harness" --slurpfile held "${prefix}-held.json" \
    'include "placement-pause-sample";
     .requestSHA256==$held[0].requestSHA256 and .resultSHA256==$held[0].resultSHA256 and
     (.releasedAt|pp_time)>=($held[0].heldAt|pp_time) and
     (.releasedAt|pp_time)<($held[0].deadline|pp_time)' "${prefix}-receipt.json" >/dev/null

  jq -e --argjson profile "$profile" --arg workload "$workload" --arg uid "$noise_uid" \
    --slurpfile before "${prefix}-namespace-before.json" --slurpfile h "$artifact_dir/handoff-evidence.json" '
    $h[0] as $h | . as $q |
    (if $workload=="single" then [$h.source|{uid,node}] else [$h.source.pods[]|{uid,node}] end) as $source |
    $q.schemaVersion=="v1" and $q.requestID=="preflight" and $q.profile==$profile and
    ($q.snapshotID|type=="string" and length>0) and ($q.snapshotTime|type=="string" and length>0) and
    $q.migrationFromNode==$h.request.payload.from_node and $q.excludedNodes==[$q.migrationFromNode] and
    ($q.requireGang // false)==($workload=="gang") and
    ($q.sourcePods|length)==(if $workload=="gang" then 2 else 1 end) and
    ($q.replacementPods|length)==($q.sourcePods|length) and
    ([$q.sourcePods[].metadata.uid]|sort)==([$source[].uid]|sort) and
    ([$source[] as $pod | any($q.sourcePods[];.metadata.uid==$pod.uid and .spec.nodeName==$pod.node)] | all) and
    all($q.sourcePods[];.metadata.namespace=="alfred-e2e" and
      .metadata.labels["ome.io/inferenceservice"]==$workload) and
    ([$q.clusterObjects[]|select(.apiVersion=="v1" and .kind=="Namespace" and .metadata.uid==$uid)]|length)==1 and
    ([$q.clusterObjects[]|select(.apiVersion=="v1" and .kind=="Namespace" and .metadata.uid==$uid)][0] |
      del(.metadata.resourceVersion,.metadata.managedFields)) ==
      ($before[0]|del(.metadata.resourceVersion,.metadata.managedFields))' "$request_file" >/dev/null
  jq -e --slurpfile request "$request_file" '
    $request[0] as $r |
    .schemaVersion==$r.schemaVersion and .requestID==$r.requestID and .snapshotID==$r.snapshotID and
    .snapshotTime==$r.snapshotTime and .profile==$r.profile and
    .decision=="Feasible" and .reason=="PlacementFound" and
    ([.placements[].pod] | sort_by(.namespace,.name,.uid // "")) ==
      ([$r.replacementPods[].metadata|{namespace,name,uid:(.uid // null)} |
        if .uid==null then del(.uid) else . end] | sort_by(.namespace,.name,.uid // "")) and
    all(.placements[]; .nodeName as $node | ($node|type=="string" and length>0) and
      ($r.excludedNodes|index($node)|not) and
      any($r.clusterObjects[];.kind=="Node" and .metadata.name==$node))' "$result_file" >/dev/null

  [[ $(jq -cs 'length' "${prefix}-namespace-mutations.jsonl") == 3 ]]
  n0=$(jq -c . "${prefix}-namespace-before.json")
  n1=$(jq -cs '.[0]' "${prefix}-namespace-mutations.jsonl")
  n2=$(jq -cs '.[1]' "${prefix}-namespace-mutations.jsonl")
  n3=$(jq -cs '.[2]' "${prefix}-namespace-mutations.jsonl")
  if ((index > 1)); then
    jq -e --argjson previous "$(jq -c . "$artifact_dir/barrier-$((index-1))-namespace-after.json")" \
      '.==$previous' "${prefix}-namespace-before.json" >/dev/null
  fi
  mode=labels; [[ $index == 4 ]] && mode=annotations
  churn_check_mutation "$n0" "$n1" "$mode" alfred-e2e.ome.io/churn
  churn_check_mutation "$n1" "$n2" "$mode" alfred-e2e.ome.io/churn
  churn_check_mutation "$n2" "$n3" "$mode" alfred-e2e.ome.io/churn
  jq -e --argjson last "$n3" '.==$last' "${prefix}-namespace-after.json" >/dev/null
  jq -e --arg field "$mode" --arg key alfred-e2e.ome.io/churn --arg v1 "$index-1" \
    --arg v2 "$index-2" --arg v3 "$index-3" \
    '.[0].metadata[$field][$key]==$v1 and .[1].metadata[$field][$key]==$v2 and
     .[2].metadata[$field][$key]==$v3' <(jq -cs . "${prefix}-namespace-mutations.jsonl") >/dev/null

  verify_idle_snapshot "${prefix}-before-release.json"
  if ((index < 4)); then
    verify_idle_snapshot "${prefix}-observed.json"
    released=$(jq -er '.releasedAt' "${prefix}-receipt.json")
    observed=$(jq -er '.recommendations.data["last-cycle.json"]|fromjson|.timestamp' "${prefix}-observed.json")
    time_after "$observed" "$released"
    jq -e --arg workload "alfred-e2e/$workload" '
      [.recommendations.data["last-cycle.json"]|fromjson|.recommendations[]? |
       select(.workload==$workload)] as $r |
      ($r|length)==1 and $r[0].dispatchStatus=="withheld" and
      $r[0].dispatchReason=="SchedulingStateChanged" and (($r[0].requestUUID // "")=="")' \
      "${prefix}-observed.json" >/dev/null
    api_count=$(jq -cs 'length' "${prefix}-api.jsonl")
    ((api_count > 0))
    for ((api_index=0; api_index<api_count; api_index++)); do
      jq -cs ".[$api_index]" "${prefix}-api.jsonl" >"$tmp/barrier-$index-api-$api_index.json"
      verify_idle_snapshot "$tmp/barrier-$index-api-$api_index.json"
    done
    jq -e --slurpfile observed "${prefix}-observed.json" \
      'length>0 and .[-1]==$observed[0]' <(jq -cs . "${prefix}-api.jsonl") >/dev/null
    prior_observed=$observed
  else
    jq -e --slurpfile e "$artifact_dir/evidence.json" --slurpfile h "$artifact_dir/handoff-evidence.json" '
      $e[0] as $e | $h[0] as $h |
      (if $e.workload=="single" then [$h.source|{uid,node}] else [$h.source.pods[]|{uid,node}] end) as $source |
      (if $e.workload=="single" then [$h.surge.replacement|{uid,node}]
       else [$h.surge.pods[]|{uid,node}] end) as $replacement |
      (if $e.workload=="single" then [$h.source.uid]
       else [$h.source.pods[]|select(.runner=="leader")|.uid] end) as $routed |
      (if $e.workload=="single" then [$h.surge.replacement.uid]
       else [$h.surge.pods[]|select(.runner=="leader")|.uid] end) as $replacementRouted |
      $h.source.routingService as $service |
      . as $s |
      [$s.isvc.metadata.annotations // {}|to_entries[]|
        select(.key|startswith("ome.io/migration-request-v1-"))] as $a |
      [$s.journal.data["state.json"]|fromjson|.entries[]? |
        select(.workloadUID==$e.ownerUID or .irUID==$e.irUID)] as $j |
      ($s.ir.status.migrations // []) as $m |
      def healthy($pods): ([$pods[] as $pod | any($s.pods.items[];
        .metadata.uid==$pod.uid and .spec.nodeName==$pod.node and .metadata.deletionTimestamp==null and
        any(.status.conditions[]?;.type=="Ready" and .status=="True") and
        any(.status.conditions[]?;.type=="ome.io/serving" and .status=="True"))] | all);
      def routed($uids): ([$uids[] as $uid | any($s.endpoints.items[];
        .metadata.labels["kubernetes.io/service-name"]==$service and any(.endpoints[]?;
          .targetRef.uid==$uid and .conditions.ready==true and .conditions.terminating!=true))] | all);
      def annotation_matches:
        ($a|length)<=1 and all($a[];.key==$h.request.annotationKey and (.value|fromjson)==$h.request.payload);
      def watched_request:
        ($s.requests|type)=="array" and ($s.requests|length)==1 and
        $s.requests[0].key==$h.request.annotationKey and
        ($s.requests[0].value|fromjson)==$h.request.payload;
      def journal_published:
        ($j|length)==1 and $j[0].workloadUID==$e.ownerUID and $j[0].irUID==$e.irUID and
        $j[0].uuid==$h.request.uuid and ($j[0].payload|fromjson)==$h.request.payload and
        ($j[0].phase=="submitted" or $j[0].phase=="acknowledged" or $j[0].phase=="completed");
      def real_migration:
        ($m|length)<=1 and (all($m[];
          .requestUUID==$h.request.uuid and .trigger=="Manual" and
          .sourceInstance==$h.completed.migration.sourceInstance and
          .surgeInstance==$h.completed.migration.surgeInstance and
          .fromNode==$h.request.payload.from_node and .phase!="Failed"));
      $s.isvc.metadata.uid==$e.ownerUID and $s.ir.metadata.uid==$e.irUID and
      $s.ir.status.readyReplicas==1 and $s.ir.status.servingReplicas==1 and $s.ir.status.availableReplicas==1 and
      annotation_matches and watched_request and journal_published and real_migration and
      ((healthy($source) and routed($routed)) or
       (healthy($replacement) and routed($replacementRouted) and
        ([$source[] as $pod | all($s.endpoints.items[].endpoints[]?;.targetRef.uid!=$pod.uid)] | all)))' \
      "${prefix}-observed.json" >/dev/null
  fi
  prior_release=$(jq -er '.releasedAt' "${prefix}-receipt.json")
done

# The watch proves uniqueness and payload identity. Ordering is separately
# fenced by the hold-4 pre-release snapshot and the payload's creation time.
jq -e --slurpfile e "$artifact_dir/evidence.json" --slurpfile h "$artifact_dir/handoff-evidence.json" '
  $e[0] as $e | $h[0] as $h |
  all(.[];.metadata.uid==$e.ownerUID) and
  [.[]|.metadata.annotations // {}|to_entries[]|
    select(.key|startswith("ome.io/migration-request-v1-"))] as $a |
  ($a|length)>0 and ($a|unique_by([.key,.value])|length)==1 and
  ([$a[].key]|unique)==[$h.request.annotationKey] and
  all($a[]; (.value|fromjson)==$h.request.payload)' \
  <(jq -cs . "$artifact_dir/request-annotation-watch.jsonl") >/dev/null
released=$(jq -er '.releasedAt' "$artifact_dir/barrier-4-receipt.json")
requested=$(jq -er '.request.payload.requested_at' "$artifact_dir/handoff-evidence.json")
jq -en -L "$harness" --arg requested "$requested" --arg released "$released" \
  'include "placement-pause-sample"; ($requested|pp_time)>=($released|pp_time)' >/dev/null
jq -e --slurpfile receipt "$artifact_dir/barrier-4-receipt.json" '
  def finite_nonnegative:
    type=="number" and (isnan|not) and (isinfinite|not) and .>=0;
  (.releaseStarted|finite_nonnegative) and (.observationFinished|finite_nonnegative) and
  .observationFinished>=.releaseStarted and
  (.budgetSeconds==30) and ((.observationFinished-.releaseStarted)<30) and
  .requestSHA256==$receipt[0].requestSHA256' "$artifact_dir/barrier-4-timing.json" >/dev/null

[[ $(jq -cs 'length' "$artifact_dir/completion-cycles.jsonl") == 3 ]]
jq -e --slurpfile baseline "$artifact_dir/completion-baseline.json" \
  --slurpfile cycles "$artifact_dir/completion-cycles.jsonl" \
  '.completionBaseline==$baseline[0] and .completed==$cycles' "$artifact_dir/evidence.json" >/dev/null

verify_completed_snapshot() {
  jq -e --slurpfile e "$artifact_dir/evidence.json" --slurpfile h "$artifact_dir/handoff-evidence.json" '
    $e[0] as $e | $h[0] as $h |
    (if $e.workload=="single" then [$h.source.uid] else [$h.source.pods[].uid] end) as $source |
    (if $e.workload=="single" then [$h.surge.replacement|{uid,node}]
     else [$h.surge.pods[]|{uid,node}] end) as $replacement |
    ([$replacement[].uid]) as $replacementUIDs |
    (if $e.workload=="single" then [$h.surge.replacement.uid]
     else [$h.surge.pods[]|select(.runner=="leader")|.uid] end) as $routed |
    $h.source.routingService as $service |
    . as $s |
    [$s.isvc.metadata.annotations // {}|to_entries[]|
      select(.key|startswith("ome.io/migration-request-v1-"))] as $a |
    [$s.journal.data["state.json"]|fromjson|.entries[]? |
      select(.workloadUID==$e.ownerUID or .irUID==$e.irUID)] as $j |
    $s.isvc.metadata.uid==$e.ownerUID and $s.ir.metadata.uid==$e.irUID and
    any($s.ir.metadata.ownerReferences[]?;.uid==$e.ownerUID and .kind=="InferenceService" and .controller==true) and
    ($s.requests|type)=="array" and ($s.requests|length)==1 and
    $s.requests[0].key==$h.request.annotationKey and ($s.requests[0].value|fromjson)==$h.request.payload and
    ($a|length)<=1 and all($a[];.key==$h.request.annotationKey and (.value|fromjson)==$h.request.payload) and
    ($s.ir.status.migrations|length)==1 and $s.ir.status.migrations[0]==$h.completed.migration and
    ($j|length)==1 and $j[0].workloadUID==$e.ownerUID and $j[0].irUID==$e.irUID and
    $j[0].uuid==$h.request.uuid and $j[0].phase=="completed" and
    ($j[0].completedAt|type=="string" and length>0) and
    ($j[0].payload|fromjson)==$h.request.payload and
    ([$s.pods.items[].metadata.uid]|sort)==($replacementUIDs|sort) and
    $s.ir.status.readyReplicas==1 and $s.ir.status.servingReplicas==1 and $s.ir.status.availableReplicas==1 and
    ([$replacement[] as $pod | any($s.pods.items[];.metadata.uid==$pod.uid and .spec.nodeName==$pod.node)] | all) and
    all($s.pods.items[];.metadata.deletionTimestamp==null and
      any(.status.conditions[]?;.type=="Ready" and .status=="True") and
      any(.status.conditions[]?;.type=="ome.io/serving" and .status=="True")) and
    ([$routed[] as $uid | any($s.endpoints.items[];
      .metadata.labels["kubernetes.io/service-name"]==$service and any(.endpoints[]?;
        .targetRef.uid==$uid and .conditions.ready==true and .conditions.terminating!=true))] | all) and
    ([$source[] as $uid | all($s.endpoints.items[].endpoints[]?;.targetRef.uid!=$uid)] | all)' "$1" >/dev/null
}

completed_at=$(jq -er '.completed.migration.completedAt' "$artifact_dir/handoff-evidence.json")
baseline_time=$(jq -er '.recommendations.data["last-cycle.json"]|fromjson|.timestamp' "$artifact_dir/completion-baseline.json")
time_after "$baseline_time" "$completed_at"
verify_completed_snapshot "$artifact_dir/completion-baseline.json"
completion_api_count=$(jq -cs 'length' "$artifact_dir/completion-api.jsonl")
((completion_api_count > 0))
for ((api_index=0; api_index<completion_api_count; api_index++)); do
  jq -cs ".[$api_index]" "$artifact_dir/completion-api.jsonl" >"$tmp/completion-api-$api_index.json"
  verify_completed_snapshot "$tmp/completion-api-$api_index.json"
done
jq -e --slurpfile cycles "$artifact_dir/completion-cycles.jsonl" '
  . as $api | ([$cycles[] as $cycle | any($api[]; .==$cycle)] | all)' \
  <(jq -cs . "$artifact_dir/completion-api.jsonl") >/dev/null
previous=$baseline_time
for index in 0 1 2; do
  jq -cs ".[$index]" "$artifact_dir/completion-cycles.jsonl" >"$tmp/completion-$index.json"
  current=$(jq -er '.recommendations.data["last-cycle.json"]|fromjson|.timestamp' "$tmp/completion-$index.json")
  time_after "$current" "$previous"
  time_after "$current" "$completed_at"
  verify_completed_snapshot "$tmp/completion-$index.json"
  journal_completed=$(jq -er --arg owner "$(jq -r '.ownerUID' "$artifact_dir/evidence.json")" \
    --arg ir "$(jq -r '.irUID' "$artifact_dir/evidence.json")" '
    [.journal.data["state.json"]|fromjson|.entries[]? |
      select(.workloadUID==$owner and .irUID==$ir)] |
    if length==1 then .[0].completedAt else error("completed journal identity is not unique") end' \
    "$tmp/completion-$index.json")
  time_after "$current" "$journal_completed"
  previous=$current
done

echo "namespace churn evidence verified: $artifact_dir/evidence.json"
