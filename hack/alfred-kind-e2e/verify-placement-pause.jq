include "placement-pause-sample";
. as $e | .placement as $p |
.scenario == "placement-pause-single" and
($p.isvcUID | pp_nonempty) and ($p.irUID | pp_nonempty) and
$p.baseline.metadata.uid == $p.irUID and $p.baseline.spec.placementReplicaLimit == 1 and
$p.baseline.spec.placementExecution == pp_policy(1;true) and
all($p.paused, $p.completed; type == "array" and length >= 3 and
  ([.[] | pp_cycle.timestamp | pp_time] | . == sort and length == (unique | length))) and
($p.paused[-1] | pp_cycle.timestamp | pp_time) < ($p.completed[0] | pp_cycle.timestamp | pp_time) and
all($p.paused[]; placement_sample("paused";$e.source;$p.isvcUID;$p.irUID;$e.request.uuid;$e.surge.replacement)) and
($p.released | placement_sample("released";$e.source;$p.isvcUID;$p.irUID;$e.request.uuid;$e.surge.replacement)) and
($p.allocated | placement_sample("allocated";$e.source;$p.isvcUID;$p.irUID;$e.request.uuid;$e.surge.replacement)) and
($p.repaused | placement_sample("repaused";$e.source;$p.isvcUID;$p.irUID;$e.request.uuid;$e.surge.replacement)) and
all($p.completed[]; placement_sample("completed";$e.source;$p.isvcUID;$p.irUID;$e.request.uuid;$e.surge.replacement))
