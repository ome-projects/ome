# Offline verifier fixture only, transforming useful-defrag's offline fixture.
# Never apply this evidence or its fabricated objects to a live cluster.
walk(if type=="string" and .=="alfred-e2e-defrag" then "alfred-e2e" else . end) |
.baseline.nodes.items[].metadata.labels["nvidia.com/gpu.product"]="fixture-gpu" |
.source.spec.affinity={nodeAffinity:{requiredDuringSchedulingIgnoredDuringExecution:{nodeSelectorTerms:[{matchExpressions:[
  {key:"kubernetes.io/hostname",operator:"In",values:["alfred-kwok-gpu-a","alfred-kwok-gpu-c"]}]}]}}} |
. as $positive |
{schedulerName:"alfred-default-scheduler",backend:"kind-default-v135",schedulerVersion:"v1.35.4",configurationID:"sha256:fixture"} as $profile |
(.baseline | .isvc.spec.engine.affinity=$positive.source.spec.affinity |
 .recommendations.metadata={uid:"reports"} |
 .pods.items=[$positive.source] |
 .allPods.items=[$positive.source,($positive.podsBefore.items[]|select(.metadata.labels["alfred-e2e/blocker"]=="true" and .spec.nodeName!="alfred-kwok-gpu-c"))] |
 .endpoints.items[0].metadata.namespace="alfred-e2e" | del(.beneficiary)) as $baseline |
def report($index): $baseline | .recommendations.data["last-cycle.json"] =
  ({timestamp:("2026-09-29T04:00:0"+($index|tostring)+"Z"),mode:"execute",recommendations:[
    {workload:"alfred-e2e/single",component:"engine",instance:0,policy:"defragmentation",reason:"Fragmentation",
     fromNode:"alfred-kwok-gpu-a",score:0.35,outcome:"withheld",dispatchStatus:"withheld",dispatchReason:"PolicyNoLongerEligible"}]}|tojson);
{scenario:"no-benefit-defrag",profile:$profile,source:.source,routingService:.routingService,baseline:$baseline,
 config:{mode:"execute",policies:{defragmentation:{enabled:true,fragmentationThreshold:0.1,scoring:{sizeLadder:[8],sizePrior:{"8":1},demandBlendLambda:1}}}},
 requestWatch:[$baseline.isvc],podWatch:[$baseline.allPods.items[]|{type:"ADDED",object:.}],
 attempts:[range(1;4)|. as $index|
   ($positive.source|.metadata.name="synthetic"|.metadata.uid=("synthetic-"+($index|tostring))|del(.spec.nodeName)) as $replacement |
   {schemaVersion:"v1",requestID:"preflight",snapshotID:("snapshot-"+($index|tostring)),snapshotTime:"2026-09-29T04:00:00Z",profile:$profile,
    migrationFromNode:"alfred-kwok-gpu-a",excludedNodes:["alfred-kwok-gpu-a"],sourcePods:[$positive.source],replacementPods:[$replacement],
    clusterObjects:(($baseline.nodes.items|map(.+{kind:"Node"}))+($baseline.allPods.items|map(.+{kind:"Pod"})))} as $q |
   {schemaVersion:$q.schemaVersion,requestID:$q.requestID,snapshotID:$q.snapshotID,snapshotTime:$q.snapshotTime,profile:$profile,
    decision:"Feasible",reason:"PlacementFound",placements:[{pod:($replacement.metadata|{namespace,name,uid}),nodeName:"alfred-kwok-gpu-c"}]} as $r |
   {request:$q,result:$r,held:{nonce:("0123456789012345678901234567890-"+($index|tostring)),requestSHA256:("q"+($index|tostring)),resultSHA256:("r"+($index|tostring)),
      heldAt:("2026-09-29T04:00:0"+($index|tostring)+".1Z"),deadline:("2026-09-29T04:00:1"+($index|tostring)+".1Z")},
    beforeRelease:report($index-1),observed:report($index),samples:[report($index)]} |
   .receipt={requestSHA256:.held.requestSHA256,resultSHA256:.held.resultSHA256,releasedAt:("2026-09-29T04:00:0"+($index|tostring)+".2Z")}]}
