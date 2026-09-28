include "semantic-retry-sample";
include "placement-pause-sample";
include "semantic-retry-admission";
. as $e | .semanticRetry as $r | $r.baseline as $base | $r.entry as $entry | $r.admission as $a |
.scenario == "semantic-retry-single" and
($entry.uuid | pp_nonempty) and ($entry.sourceFingerprint | pp_nonempty) and
$entry.workload == {Namespace:"alfred-e2e",Name:"single"} and $entry.workloadUID == $base.isvc.metadata.uid and
$entry.irUID == $base.ir.metadata.uid and $entry.component == "engine" and $entry.instance == $e.source.instance and
$entry.fromNode == $e.source.node and $entry.irName == "single-engine" and $entry.uuid == $e.request.uuid and
($entry.payload | fromjson) == $e.request.payload and
($base | sr_sample("baseline";$base;$entry;$e.source;$e.surge.replacement)) and
($r.prepared | sr_sample("prepared";$base;$entry;$e.source;$e.surge.replacement)) and
all($r.paused,$r.completed; type == "array" and length >= 3 and
  ([.[] | pp_cycle.timestamp | pp_time] | . == sort and length == (unique|length))) and
all($r.paused[]; sr_sample("paused";$base;$entry;$e.source;$e.surge.replacement)) and
([$r.paused[] | sr_entry($base)[0].lastAttempt] | unique | length == 1) and
($r.released | sr_sample("released";$base;$entry;$e.source;$e.surge.replacement)) and
$r.released.ir.metadata.generation > $r.paused[-1].ir.metadata.generation and
($r.completionBaseline | sr_sample("completed";$base;$entry;$e.source;$e.surge.replacement)) and
all($r.completed[]; sr_sample("completed";$base;$entry;$e.source;$e.surge.replacement)) and
all($r.completed[];
  (pp_cycle.timestamp|pp_time) > ($r.completionBaseline|pp_cycle.timestamp|pp_time) and
  (pp_cycle.timestamp|pp_time) > (sr_entry($base)[0].completedAt|pp_time)) and
($r.paused[-1] | pp_cycle.timestamp | pp_time) < ($r.completed[0] | pp_cycle.timestamp | pp_time) and
($a.name | type == "string" and startswith("alfred-e2e-retry-")) and
$a.username == "system:serviceaccount:ome:ome-alfred" and
($a.policy.metadata.uid|pp_nonempty) and ($a.binding.metadata.uid|pp_nonempty) and
$a.policy.metadata.name == $a.name and $a.binding.metadata.name == $a.name and
$a.policy.spec == retry_policy($a.name;$base.isvc.metadata.uid;$a.username).spec and $a.binding.spec == retry_binding($a.name).spec and
$a.policy.metadata.generation >= 1 and $a.policy.status.observedGeneration == $a.policy.metadata.generation and
($a.policy.status.typeChecking|type) == "object" and (($a.policy.status.typeChecking.expressionWarnings // [])|length) == 0 and
$a.deniedProbe.exitCode != 0 and
($a.deniedProbe.output | contains($a.name) and contains("semantic-retry publication held")) and
$a.allowedProbe.exitCode == 0 and $a.bindingRemoved == true and
($a.allowedProbe.output | fromjson | .metadata.uid == $entry.workloadUID and
  .metadata.annotations["ome.io/migration-request-v1-"+$entry.uuid] == $entry.payload) and
all($a.metricsBefore,$a.metricsRejected;
  (.denyCount|type == "number" and . >= 1 and floor == .) and (.processStart|type == "number" and . > 0) and
  (.apiServerUID|pp_nonempty)) and
$a.metricsRejected.denyCount > $a.metricsBefore.denyCount and
$a.metricsRejected.processStart == $a.metricsBefore.processStart and
$a.metricsRejected.apiServerUID == $a.metricsBefore.apiServerUID and
# Both metric timestamps use the host clock; do not compare them to the VM's
# Alfred clock. The runner seals baseline after probes and before maintenance.
($a.metricsBefore.capturedAt | pp_time) <= ($a.metricsRejected.capturedAt | pp_time) and
all("policy","binding"; . as $k |
  ($a.guardBefore[$k].metadata.uid | pp_nonempty) and
  $a.guardBefore[$k].metadata.uid == $a.guardAfter[$k].metadata.uid and
  $a.guardBefore[$k].metadata.generation == $a.guardAfter[$k].metadata.generation and
  $a.guardBefore[$k].spec == $a.guardAfter[$k].spec)
