include "placement-pause-sample";

def df_entries($e): pp_entries($e.baseline.isvc.metadata.uid;$e.baseline.ir.metadata.uid);
def df_new_cycle($e;$previous):
  (pp_cycle.timestamp|pp_time) as $time |
  $time > ($previous|pp_time) and $time > (.ir.status.migrations[0].completedAt|pp_time) and
  $time > (df_entries($e)[0].completedAt|pp_time);
def df_blockers: [.items[]|select(.metadata.labels["alfred-e2e/blocker"] == "true")|{uid:.metadata.uid,spec}]|sort_by(.uid);
def df_pod_identity($pod;$e):
  .metadata.uid == $pod.metadata.uid and .metadata.name == $pod.metadata.name and
  .metadata.namespace == "alfred-e2e-defrag" and .spec == $pod.spec and
  .metadata.labels["ome.io/inferenceservice"] == "single" and .metadata.labels.component == "engine" and
  .metadata.labels["ome.io/instance-index"] == $pod.metadata.labels["ome.io/instance-index"] and
  any(.metadata.ownerReferences[]?; .kind == "InferenceReplica" and .name == "single-engine" and
    .uid == $e.baseline.ir.metadata.uid and .controller == true);
def df_safe($pod;$e):
  any(.pods.items[]; df_pod_identity($pod;$e) and .metadata.deletionTimestamp == null and
    any(.status.conditions[]?; .type == "Ready" and .status == "True") and
    any(.status.conditions[]?; .type == "ome.io/serving" and .status == "True")) and
  pp_endpoint($pod.metadata.uid;$e.routingService);
def df_identity($e):
  .isvc.metadata.uid == $e.baseline.isvc.metadata.uid and
  .isvc.metadata.name == "single" and .isvc.metadata.namespace == "alfred-e2e-defrag" and
  .isvc.metadata.deletionTimestamp == null and .isvc.metadata.generation == $e.baseline.isvc.metadata.generation and
  .isvc.spec == $e.baseline.isvc.spec and
  .ir.metadata.uid == $e.baseline.ir.metadata.uid and .ir.metadata.name == "single-engine" and
  .ir.metadata.namespace == "alfred-e2e-defrag" and .ir.metadata.deletionTimestamp == null and
  any(.ir.metadata.ownerReferences[]?; .kind == "InferenceService" and .name == "single" and
    .uid == $e.baseline.isvc.metadata.uid and .controller == true) and
  (.pods.items|type == "array" and length > 0) and (.endpoints.items|type == "array") and
  (.allPods|df_blockers) == ($e.podsBefore|df_blockers) and
  all(.allPods.items[]|select(.metadata.labels["alfred-e2e/blocker"] == "true");
    .metadata.deletionTimestamp == null and .status.phase != "Succeeded" and .status.phase != "Failed") and
  (.pods.items|sort_by(.metadata.uid)) ==
    ([.allPods.items[]|select(.metadata.namespace == "alfred-e2e-defrag" and .metadata.labels["ome.io/inferenceservice"] == "single")]|sort_by(.metadata.uid)) and
  ([.pods.items[].metadata.uid]|length == (unique|length)) and
  all(.pods.items[]; df_pod_identity($e.source;$e) or df_pod_identity($e.replacement;$e));
def df_beneficiary($e):
  . as $sample |
  [.allPods.items[]|select(.metadata.namespace == $e.before.metadata.namespace and .metadata.name == $e.before.metadata.name)] as $matches |
  ($matches|length == 1) and $matches[0].metadata.uid == $e.before.metadata.uid and
  $matches[0].metadata.deletionTimestamp == null and
  ($matches[0].spec|del(.nodeName)) == ($e.before.spec|del(.nodeName)) and
  (if .beneficiary.spec.nodeName != null then $matches[0].spec.nodeName == .beneficiary.spec.nodeName
   elif $matches[0].spec.nodeName == null then true
   else $matches[0].spec.nodeName == "alfred-kwok-gpu-a" and ($sample|df_safe($e.replacement;$e)) end) and
  .beneficiary.metadata.uid == $e.before.metadata.uid and
  .beneficiary.metadata.name == $e.before.metadata.name and
  .beneficiary.metadata.namespace == $e.before.metadata.namespace and
  .beneficiary.metadata.deletionTimestamp == null and
  (.beneficiary.spec|del(.nodeName)) == ($e.before.spec|del(.nodeName)) and
  (if .beneficiary.spec.nodeName == null then true
   else .beneficiary.spec.nodeName == "alfred-kwok-gpu-a" and df_safe($e.replacement;$e) end);
def df_handoff($e):
  df_identity($e) and df_beneficiary($e) and (df_safe($e.source;$e) or df_safe($e.replacement;$e));
def df_held($e):
  df_handoff($e) and df_safe($e.source;$e) and
  ([.pods.items[].metadata.uid]|sort) == ([$e.source.metadata.uid,$e.replacement.metadata.uid]|sort) and
  any(.pods.items[]; .metadata.uid == $e.replacement.metadata.uid and .metadata.deletionTimestamp == null and
    any(.status.conditions[]?; .type == "Ready" and .status == "False") and
    all(.status.conditions[]?; .type != "ome.io/serving" or .status != "True")) and
  (pp_endpoint($e.replacement.metadata.uid;$e.routingService)|not) and
  .beneficiary.spec.nodeName == null and
  (.ir.status.migrations|length == 1 and .[0].requestUUID == $e.request.uuid and
    .[0].phase == "SurgePending" and .[0].sourceInstance == 0 and .[0].surgeInstance == 1);
def df_completed($e):
  df_handoff($e) and ([.pods.items[].metadata.uid] == [$e.replacement.metadata.uid]) and
  df_safe($e.replacement;$e) and
  (any(.endpoints.items[].endpoints[]?; .targetRef.uid == $e.source.metadata.uid)|not) and
  .beneficiary.spec.nodeName == "alfred-kwok-gpu-a" and
  any(.beneficiary.status.conditions[]?; .type == "Ready" and .status == "True") and
  .ir.status.readyReplicas == 1 and .ir.status.servingReplicas == 1 and .ir.status.availableReplicas == 1 and
  .ir.status.migrations == [$e.migration] and
  (pp_journal|.version == "v1" and (.entries|type == "array")) and
  (df_entries($e)|length == 1 and (.[0]|
    .uuid == $e.request.uuid and .workloadUID == $e.baseline.isvc.metadata.uid and .irUID == $e.baseline.ir.metadata.uid and
    .workload == {Namespace:"alfred-e2e-defrag",Name:"single"} and .irName == "single-engine" and
    .component == "engine" and .instance == 0 and .fromNode == "alfred-kwok-gpu-a" and
    (.payload|fromjson) == $e.request.payload and .phase == "completed" and (.completedAt|pp_nonempty))) and
  all(.isvc.metadata.annotations // {}|to_entries[]|select(.key|startswith("ome.io/migration-request-v1-"));
    .key == ("ome.io/migration-request-v1-"+$e.request.uuid) and (.value|fromjson) == $e.request.payload) and
  (pp_cycle|.mode == "execute" and (.timestamp|pp_time|length == 2));

# This fixture deliberately uses ordinary integer GPU requests: reject other
# accounting shapes instead of claiming this small verifier models all Pods.
def df_capacity($nodes;$pods):
  [$nodes.items[]|. as $node|
    [$pods.items[]|select(.spec.nodeName == $node.metadata.name and .status.phase != "Succeeded" and .status.phase != "Failed")|
      if (.metadata.deletionTimestamp != null or (.spec.initContainers // []|length) != 0 or
          .spec.overhead["nvidia.com/gpu"] != null or .spec.resources != null)
      then error("unsupported or terminating capacity occupant") else . end|
      .spec.containers[]|(.resources.requests["nvidia.com/gpu"] // "0"|tonumber)] as $requests|
    {name:$node.metadata.name,free:(($node.status.allocatable["nvidia.com/gpu"]|tonumber)-($requests|add // 0))}]|sort_by(.name);
