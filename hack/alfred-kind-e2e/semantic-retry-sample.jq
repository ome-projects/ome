include "placement-pause-sample";
def sr_intent:
  {uuid,payload,createdAt,workload,workloadUID,irName,irUID,component,instance,fromNode,sourceFingerprint,targets};
def sr_entry($base): pp_entries($base.isvc.metadata.uid;$base.ir.metadata.uid);
# Public OME projection copies these owner protocol annotations into runner
# metadata. Normalize only the placement envelope and this exact intent's key;
# retain every other spec field, annotation and label. Raw samples are unchanged.
def sr_metadata($entry):
  del(.annotations["ome.io/placement-execution"]) |
  if ($entry.uuid | type) == "string" then del(.annotations["ome.io/migration-request-v1-"+$entry.uuid]) else . end;
def sr_spec($entry):
  del(.placementExecution,.placementReplicaLimit) |
  .runners |= map(.template.metadata |= sr_metadata($entry));
def sr_identity($base;$entry):
  pp_identity($base.isvc.metadata.uid;$base.ir.metadata.uid) and
  (.isvc.metadata.generation | type == "number" and . >= 1) and
  (.ir.status.currentRevision | pp_nonempty) and
  .isvc.metadata.generation == $base.isvc.metadata.generation and
  .ir.status.currentRevision == $base.ir.status.currentRevision and
  (.ir.spec.placementExecution as $policy | .ir.spec.runners | length > 0 and
    all(.[]; (.template.metadata.annotations["ome.io/placement-execution"] | fromjson) == $policy)) and
  (if ($entry.uuid | type) == "string" then
    all(.ir.spec.runners[]; .template.metadata.annotations["ome.io/migration-request-v1-"+$entry.uuid] as $value |
      $value == null or $value == $entry.payload) else true end) and
  (.ir.spec | sr_spec($entry)) == ($base.ir.spec | sr_spec($entry));
def sr_source($base;$source;$entry):
  pp_source($source) and
  any(.pods.items[]; .metadata.uid == $source.uid and
    (.metadata.labels["ome.io/instance-incarnation"]|tonumber) == $source.incarnation) and
  ([.pods.items[] | select(.metadata.uid == $source.uid) |
    {spec,metadata:(.metadata | {labels,annotations})}]) ==
    ([$base.pods.items[] | select(.metadata.uid == $source.uid) |
      {spec,metadata:(.metadata | {labels,annotations})}]);
def sr_no_effects($base;$source;$entry):
  sr_source($base;$source;$entry) and ([.pods.items[].metadata.uid] == [$source.uid]) and
  (pp_annotations | length == 0) and (.requests | length == 0) and
  ((.ir.status.migrations // []) | length == 0) and
  .ir.status.readyReplicas == 1 and .ir.status.servingReplicas == 1 and .ir.status.availableReplicas == 1;
def sr_same_intent($base;$entry):
  sr_entry($base) | length == 1 and (.[0] | sr_intent) == ($entry | sr_intent);
def sr_prepared($base;$entry):
  sr_same_intent($base;$entry) and (sr_entry($base) | .[0] |
    .phase == "prepared" and .reason == "SubmissionUncertain" and
    (.lastAttempt | pp_nonempty) and (.lastAttempt | pp_time) >= (.createdAt | pp_time) and
    .acknowledgedAt == null and .completedAt == null);
def sr_report($entry;$reason):
  pp_cycle | any(.recommendations[]?; .requestUUID == $entry.uuid and .workload == "alfred-e2e/single" and
    .component == "engine" and .instance == 0 and .outcome == "withheld" and .dispatchReason == $reason);
def sr_sample($stage;$base;$entry;$source;$replacement):
  sr_identity($base;$entry) and
  if $stage == "baseline" then
    pp_projection(1;false) and sr_no_effects($base;$source;$entry) and (sr_entry($base) | length == 0)
  elif $stage == "source-safe" then sr_source($base;$source;$entry)
  elif $stage == "no-effects" then sr_no_effects($base;$source;$entry) and (sr_entry($base) | length <= 1)
  elif $stage == "prepared" then
    pp_projection(1;false) and sr_no_effects($base;$source;$entry) and sr_prepared($base;$entry)
  elif $stage == "paused-safe" or $stage == "paused" then
    pp_projection(2;true) and .ir.metadata.generation > $base.ir.metadata.generation and
    sr_no_effects($base;$source;$entry) and sr_prepared($base;$entry) and
    ($stage == "paused-safe" or sr_report($entry;"PolicyNoLongerEligible"))
  elif $stage == "released" then
    pp_projection(3;false) and .ir.metadata.generation > $base.ir.metadata.generation and
    sr_source($base;$source;$entry) and sr_same_intent($base;$entry)
  elif $stage == "old-source-changed" then
    pp_projection(3;false) and sr_no_effects($base;$source;$entry) and sr_prepared($base;$entry) and sr_report($entry;"SourceChanged")
  elif $stage == "completed" then
    pp_projection(3;false) and sr_same_intent($base;$entry) and
    pp_one_request($source;$entry.uuid;$base.isvc.metadata.uid;$base.ir.metadata.uid) and
    .requests[0].value == $entry.payload and all(pp_annotations[]; .value == $entry.payload) and
    ([.pods.items[].metadata.uid] == [$replacement.uid]) and pp_pod($replacement.uid;true) and
    pp_endpoint($replacement.uid;$source.routingService) and
    (any(.endpoints.items[].endpoints[]?; .targetRef.uid == $source.uid) | not) and
    .ir.status.readyReplicas == 1 and .ir.status.servingReplicas == 1 and .ir.status.availableReplicas == 1 and
    (.ir.status.migrations | length == 1 and .[0].requestUUID == $entry.uuid and .[0].phase == "Completed" and
      .[0].trigger == "Manual" and .[0].fromNode == $source.node and .[0].sourceInstance == $source.instance and .[0].surgeInstance == $replacement.instance) and
    (sr_entry($base) | .[0].phase == "completed" and (.[0].completedAt | pp_nonempty))
  else false end;
