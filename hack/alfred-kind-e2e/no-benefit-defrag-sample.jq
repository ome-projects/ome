include "placement-pause-sample";
include "useful-defrag-sample";

def nd_object: {uid:.metadata.uid,name:.metadata.name,namespace:.metadata.namespace,
  labels:.metadata.labels,owners:.metadata.ownerReferences,spec};
def nd_occupants: [.items[]|select(.spec.nodeName // ""|startswith("alfred-kwok-gpu-"))|nd_object]|sort_by(.uid);
def nd_nodes: [.items[]|{uid:.metadata.uid,name:.metadata.name,labels:.metadata.labels,spec}]|sort_by(.name);
def nd_source($e):
  (nd_object)==($e.source|nd_object) and .metadata.deletionTimestamp==null and
  .status.phase=="Running" and
  any(.status.conditions[]?;.type=="Ready" and .status=="True") and
  any(.status.conditions[]?;.type=="ome.io/serving" and .status=="True");
def nd_idle($e):
  (.isvc|nd_object)==($e.baseline.isvc|nd_object) and
  .isvc.metadata.generation==$e.baseline.isvc.metadata.generation and .isvc.metadata.deletionTimestamp==null and
  (.ir|nd_object)==($e.baseline.ir|nd_object) and .ir.metadata.generation==$e.baseline.ir.metadata.generation and
  .ir.metadata.deletionTimestamp==null and
  .ir.status.readyReplicas==1 and .ir.status.servingReplicas==1 and .ir.status.availableReplicas==1 and
  (.ir.status.migrations // []|length)==0 and
  ([.isvc.metadata.annotations // {}|keys[]|select(startswith("ome.io/migration-request-v1-"))]|length)==0 and
  (pp_journal|.version=="v1" and (.entries|type=="array")) and
  (pp_entries($e.baseline.isvc.metadata.uid;$e.baseline.ir.metadata.uid)|length)==0 and
  (.pods.items|length==1 and (.[0]|nd_source($e))) and
  (.pods.items|sort_by(.metadata.uid))==([.allPods.items[]|select(.metadata.namespace=="alfred-e2e" and .metadata.labels["ome.io/inferenceservice"]=="single")]|sort_by(.metadata.uid)) and
  (.allPods|nd_occupants)==($e.baseline.allPods|nd_occupants) and
  all(.allPods.items[]|select(.spec.nodeName // ""|startswith("alfred-kwok-gpu-"));
    .metadata.deletionTimestamp==null and .status.phase!="Succeeded" and .status.phase!="Failed") and
  (.nodes|nd_nodes)==($e.baseline.nodes|nd_nodes) and
  all(.nodes.items[];.spec.unschedulable!=true and any(.status.conditions[]?;.type=="Ready" and .status=="True")) and
  (df_capacity(.nodes;.allPods)|map(.free))==[7,1,8,0] and
  any(.endpoints.items[];.metadata.namespace=="alfred-e2e" and
    .metadata.labels["kubernetes.io/service-name"]==$e.routingService and
    any(.endpoints[]?;.targetRef.uid==$e.source.metadata.uid and .conditions.ready==true and .conditions.terminating!=true)) and
  .recommendations.metadata.uid==$e.baseline.recommendations.metadata.uid and
  (pp_cycle|.mode=="execute" and (.timestamp|pp_time|length==2));
def nd_rejected:
  [pp_cycle.recommendations[]?|select(.workload=="alfred-e2e/single")] as $r |
  ($r|length)==1 and ($r[0]|.component=="engine" and .instance==0 and .policy=="defragmentation" and
    .reason=="Fragmentation" and .fromNode=="alfred-kwok-gpu-a" and .score>0 and
    .outcome=="withheld" and .dispatchStatus=="withheld" and .dispatchReason=="PolicyNoLongerEligible" and
    (.requestUUID // "")=="" and (.advisoryReason // "")=="" and (.rejectReason // "")=="");
def nd_affinity:
  .affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms as $terms |
  ($terms|length)==1 and any($terms[0].matchExpressions[]?;
    .key=="kubernetes.io/hostname" and .operator=="In" and (.values|sort)==["alfred-kwok-gpu-a","alfred-kwok-gpu-c"]);
def nd_attempt($e):
  . as $a | .request as $q | .result as $r |
  ($a.held.heldAt|pp_time) as $held | ($a.held.deadline|pp_time) as $deadline |
  ($a.beforeRelease|nd_idle($e)) and ($a.observed|nd_idle($e) and nd_rejected) and
  ($a.samples|type=="array" and length>0) and all($a.samples[];nd_idle($e)) and
  any($a.samples[];.==$a.observed) and
  ($a.observed|pp_cycle.timestamp|pp_time)>($a.beforeRelease|pp_cycle.timestamp|pp_time) and
  ($a.held.nonce|type=="string" and length>=32) and
  all($a.held.requestSHA256,$a.held.resultSHA256;type=="string" and length>0) and
  $a.receipt.requestSHA256==$a.held.requestSHA256 and $a.receipt.resultSHA256==$a.held.resultSHA256 and
  ($a.receipt.releasedAt|pp_time)>=$held and ($a.receipt.releasedAt|pp_time)<$deadline and
  $deadline>$held and ($deadline[0]-$held[0]<10 or ($deadline[0]-$held[0]==10 and $deadline[1]<=$held[1])) and
  $q.schemaVersion=="v1" and $q.requestID=="preflight" and $q.profile==$e.profile and
  ($q.snapshotID|type=="string" and length>0) and ($q.snapshotTime|pp_time|length==2) and
  $q.migrationFromNode=="alfred-kwok-gpu-a" and $q.excludedNodes==["alfred-kwok-gpu-a"] and
  ($q.requireGang // false)==false and ($q.sourcePods|length)==1 and
  ($q.sourcePods[0]|nd_object)==($e.source|nd_object) and
  ($q.replacementPods|length)==1 and $q.replacementPods[0].metadata.namespace=="alfred-e2e" and
  $q.replacementPods[0].spec.containers==$e.source.spec.containers and
  ($q.replacementPods[0].spec|nd_affinity) and
  ({items:[$q.clusterObjects[]|select(.kind=="Node" and .metadata.labels["alfred-e2e/virtual"]=="true")]}|nd_nodes)==($e.baseline.nodes|nd_nodes) and
  ({items:[$q.clusterObjects[]|select(.kind=="Pod")]}|nd_occupants)==($e.baseline.allPods|nd_occupants) and
  (df_capacity({items:[$q.clusterObjects[]|select(.kind=="Node" and .metadata.labels["alfred-e2e/virtual"]=="true")]};
    {items:[$q.clusterObjects[]|select(.kind=="Pod")]} )|map(.free))==[7,1,8,0] and
  $r.schemaVersion==$q.schemaVersion and $r.requestID==$q.requestID and $r.snapshotID==$q.snapshotID and
  $r.snapshotTime==$q.snapshotTime and $r.profile==$q.profile and $r.decision=="Feasible" and $r.reason=="PlacementFound" and
  ($r.placements|length)==1 and $r.placements[0].pod==($q.replacementPods[0].metadata|{namespace,name,uid}) and
  $r.placements[0].nodeName=="alfred-kwok-gpu-c";
