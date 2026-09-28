# Offline verifier fixture only. Live scenarios never write synthetic status.
include "placement-pause-sample";
. as $base |
def pod($uid;$node;$index;$ready):
  {metadata:{uid:$uid,labels:{"ome.io/instance-index":($index|tostring)}},spec:{nodeName:$node},
   status:{conditions:[{type:"Ready",status:$ready},{type:"ome.io/serving",status:$ready}]}};
def recommendations($time):
  {data:{"last-cycle.json":({timestamp:$time,mode:"execute",recommendations:[
    {workload:"alfred-e2e/single",component:"engine",instance:0,fromNode:$base.source.node,
     policy:"nodehealth",reason:"NodeMaintenance",outcome:"advisory",advisoryReason:"OMENativeStateIneligible"}]}|tojson),
    ("node."+$base.source.node):({maintenance:{requested:true},maintenanceDrainedAt:null,omeGpuOccupantsPresent:true}|tojson)}};
def sample($revision;$paused;$time):
  {isvc:{metadata:{uid:"isvc-new",name:"single",namespace:"alfred-e2e",
    labels:{"ome.io/placement-origin":"alfred-e2e-source"},
    annotations:{"ome.io/placement-origin-uid":"alfred-e2e-source",
      "ome.io/placement-execution":(pp_policy($revision;$paused)|tojson)}}},
   ir:{metadata:{uid:"ir-new",name:"single-engine",namespace:"alfred-e2e",generation:$revision,
     ownerReferences:[{kind:"InferenceService",uid:"isvc-new",controller:true}]},
     spec:{placementExecution:pp_policy($revision;$paused),placementReplicaLimit:(if $paused then 1 else null end)},
     status:{placementObservedGeneration:$revision,readyReplicas:1,servingReplicas:1,availableReplicas:1,migrations:[]}},
   pods:{items:[pod($base.source.uid;$base.source.node;0;"True")]},
   endpoints:{items:[{metadata:{labels:{"kubernetes.io/service-name":$base.source.routingService}},
     endpoints:[{targetRef:{uid:$base.source.uid},conditions:{ready:true}}]}]},
   recommendations:recommendations($time),dispatch:{data:{"state.json":({version:"v1",entries:[]}|tojson)}},requests:[]};
def allocated($revision;$paused):
  sample($revision;$paused;"2026-09-14T12:00:08Z") |
  .requests=[{key:$base.request.annotationKey,value:($base.request.payload|tojson)}] |
  .isvc.metadata.annotations[$base.request.annotationKey]=($base.request.payload|tojson) |
  .ir.status.migrations=[($base.completed.migration | .phase="SurgePending" | del(.completedAt))] |
  .pods.items += [pod($base.surge.replacement.uid;$base.surge.replacement.node;1;"False")] |
  .dispatch.data["state.json"]=({version:"v1",entries:[{uuid:$base.request.uuid,
    workloadUID:"isvc-new",irUID:"ir-new",phase:"acknowledged",payload:($base.request.payload|tojson)}]}|tojson);
def completed($time):
  allocated(3;true) |
  .recommendations=recommendations($time) |
  .recommendations.data["last-cycle.json"] |= (fromjson | .recommendations=[] | tojson) |
  .ir.status.migrations=[$base.completed.migration] |
  .pods.items=[pod($base.surge.replacement.uid;$base.surge.replacement.node;1;"True")] |
  .endpoints.items[0].endpoints=[{targetRef:{uid:$base.surge.replacement.uid},conditions:{ready:true}}] |
  .dispatch.data["state.json"] |= (fromjson | .entries[0].phase="completed" |
    .entries[0].completedAt="2026-09-14T12:00:15Z" | tojson);
.scenario="placement-pause-single" |
.placement={isvcUID:"isvc-new",irUID:"ir-new",baseline:sample(1;true;"2026-09-14T12:00:00Z").ir,
  paused:[sample(1;true;"2026-09-14T12:00:02Z"),sample(1;true;"2026-09-14T12:00:04Z"),sample(1;true;"2026-09-14T12:00:06Z")],
  released:sample(2;false;"2026-09-14T12:00:07Z"),allocated:allocated(2;false),repaused:allocated(3;true),
  completed:[completed("2026-09-14T12:00:18Z"),completed("2026-09-14T12:00:20Z"),completed("2026-09-14T12:00:22Z")]}
