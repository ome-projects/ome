include "placement-pause-sample";
include "no-benefit-defrag-sample";
. as $e |
.scenario=="no-benefit-defrag" and
(.policyName|type=="string" and startswith("alfred-no-benefit-") and endswith("-policy")) and
.profile.schedulerName=="alfred-default-scheduler" and .profile.backend=="kind-default-v135" and
all(.profile.schedulerVersion,.profile.configurationID,.baseline.isvc.metadata.uid,.baseline.ir.metadata.uid,
  .source.metadata.uid,.baseline.recommendations.metadata.uid;type=="string" and length>0) and
.source.metadata.namespace=="alfred-e2e" and .source.spec.nodeName=="alfred-kwok-gpu-a" and
(.source.spec|nd_affinity) and
([.source.spec.containers[].resources.requests["nvidia.com/gpu"]|tonumber]|add)==1 and
([.baseline.nodes.items[].metadata.name]|sort)==["alfred-kwok-gpu-a","alfred-kwok-gpu-b","alfred-kwok-gpu-c","alfred-kwok-gpu-d"] and
all(.baseline.nodes.items[];(.status.allocatable["nvidia.com/gpu"]|tonumber)==8) and
([.baseline.nodes.items[].metadata.labels["nvidia.com/gpu.product"]]|unique|length==1 and (.[0]|type=="string" and length>0)) and
.baseline.isvc.metadata.name=="single" and .baseline.ir.metadata.name=="single-engine" and
any(.baseline.ir.metadata.ownerReferences[]?;.uid==$e.baseline.isvc.metadata.uid and .kind=="InferenceService" and .controller==true) and
any(.source.metadata.ownerReferences[]?;.uid==$e.baseline.ir.metadata.uid and .kind=="InferenceReplica" and .controller==true) and
.config.mode=="execute" and (.config.policies.defragmentation|
  .enabled==true and .fragmentationThreshold==0.1 and
  .scoring.sizeLadder==[8] and .scoring.sizePrior=={"8":1} and .scoring.demandBlendLambda==1) and
(.baseline|nd_idle($e)) and (.attempts|length)==3 and all(.attempts[];nd_attempt($e)) and
([.attempts[].held.requestSHA256]|length==(unique|length)) and
([.attempts[].held.nonce]|length==(unique|length)) and
([.attempts[].observed|pp_cycle.timestamp|pp_time] as $cycles|$cycles==($cycles|sort|unique)) and
all(range(1;3);. as $i|
  ($e.attempts[$i].beforeRelease|pp_cycle.timestamp|pp_time)>=($e.attempts[$i-1].observed|pp_cycle.timestamp|pp_time)) and
(.requestWatch|type=="array" and length>0) and all(.requestWatch[];
  (nd_object)==($e.baseline.isvc|nd_object) and .metadata.generation==$e.baseline.isvc.metadata.generation and
  .metadata.deletionTimestamp==null and
  ([.metadata.annotations // {}|keys[]|select(startswith("ome.io/migration-request-v1-"))]|length)==0) and
(.podWatch|type=="array" and length>0) and any(.podWatch[];.object.metadata.uid==$e.source.metadata.uid) and
all(.podWatch[];. as $event|(.type=="ADDED" or .type=="MODIFIED") and
  any($e.baseline.allPods.items[];.metadata.namespace=="alfred-e2e" and (nd_object)==($event.object|nd_object)) and
  .object.metadata.deletionTimestamp==null and
  (if .object.metadata.uid==$e.source.metadata.uid then (.object|nd_source($e)) else true end))
