# Offline evidence fixture, never used by the live runner. Admission rendering
# is also checked against literal scope expectations in semantic-retry_test.sh.
include "semantic-retry-admission";
. as $e |
def policy($revision;$paused): {planID:"alfred-e2e-pause",revision:$revision,sourceUID:"alfred-e2e-source",clusterUID:"alfred-e2e-member",pauseSurge:$paused};
def pod($uid;$node;$index;$ready):
  {metadata:{uid:$uid,labels:{"ome.io/instance-index":($index|tostring),"ome.io/instance-incarnation":"1"},annotations:{}},
   spec:{nodeName:$node,containers:[{name:"runner",image:"pause:3.10"}]},
   status:{conditions:[{type:"Ready",status:$ready},{type:"ome.io/serving",status:$ready}]}};
def entry: {uuid:$e.request.uuid,payload:($e.request.payload|tojson),createdAt:"2026-09-14T12:00:00Z",
  lastAttempt:"2026-09-14T12:00:01Z",phase:"prepared",reason:"SubmissionUncertain",sourceFingerprint:"fingerprint-a",
  workload:{Namespace:"alfred-e2e",Name:"single"},workloadUID:"owner-uid",irUID:"ir-uid",irName:"single-engine",component:"engine",instance:0,fromNode:$e.source.node};
def sample($revision;$paused;$time):
  {isvc:{metadata:{name:"single",namespace:"alfred-e2e",uid:"owner-uid",generation:1,resourceVersion:($revision|tostring),
    labels:{"ome.io/placement-origin":"alfred-e2e-source"},annotations:{"ome.io/placement-origin-uid":"alfred-e2e-source",
    "ome.io/placement-execution":(policy($revision;$paused)|tojson)}}},
   ir:{metadata:{name:"single-engine",namespace:"alfred-e2e",uid:"ir-uid",generation:$revision,
     ownerReferences:[{kind:"InferenceService",name:"single",uid:"owner-uid",controller:true}]},
     spec:{replicas:1,placementExecution:policy($revision;$paused),placementReplicaLimit:(if $paused then 1 else null end),
       runners:[{name:"runner",template:{metadata:{labels:{app:"single"},annotations:{
         "ome.io/placement-execution":(policy($revision;$paused)|tojson),"fixture.example/stable":"unchanged"}},
         spec:{containers:[{name:"runner",image:"pause:3.10"}]}}}]},
     status:{currentRevision:"testhash",placementObservedGeneration:$revision,readyReplicas:1,servingReplicas:1,availableReplicas:1,migrations:[]}},
   pods:{items:[pod($e.source.uid;$e.source.node;0;"True")]},
   endpoints:{items:[{metadata:{labels:{"kubernetes.io/service-name":$e.source.routingService}},
     endpoints:[{targetRef:{uid:$e.source.uid},conditions:{ready:true}}]}]},
   requests:[],dispatch:{data:{"state.json":({version:"v1",entries:[]}|tojson)}},
   recommendations:{data:{"last-cycle.json":({timestamp:$time,mode:"execute",recommendations:[]}|tojson)}}};
def prepared($revision;$paused;$time;$reason): sample($revision;$paused;$time) |
  .dispatch.data["state.json"]=({version:"v1",entries:[entry]}|tojson) |
  .recommendations.data["last-cycle.json"] |= (fromjson | .recommendations=[{workload:"alfred-e2e/single",component:"engine",instance:0,
    policy:"nodehealth",reason:"NodeMaintenance",fromNode:$e.source.node,requestUUID:$e.request.uuid,outcome:"withheld",dispatchReason:$reason}] | tojson);
def completed($time): prepared(3;false;$time;"TerminalStatusObserved") |
  .requests=[{key:$e.request.annotationKey,value:(entry.payload)}] |
  .ir.spec.runners[0].template.metadata.annotations[$e.request.annotationKey]=entry.payload |
  .ir.status.migrations=[$e.completed.migration] |
  .pods.items=[pod($e.surge.replacement.uid;$e.surge.replacement.node;1;"True")] |
  .endpoints.items[0].endpoints=[{targetRef:{uid:$e.surge.replacement.uid},conditions:{ready:true}}] |
  .dispatch.data["state.json"] |= (fromjson | .entries[0] |= (.phase="completed" | .completedAt="2026-09-14T12:00:15Z") | tojson);
.scenario="semantic-retry-single" |
.semanticRetry={baseline:sample(1;false;"2026-09-14T11:59:59Z"),entry:entry,
  prepared:prepared(1;false;"2026-09-14T12:00:02Z";"SubmissionUncertain"),
  paused:[prepared(2;true;"2026-09-14T12:00:04Z";"PolicyNoLongerEligible"),
    prepared(2;true;"2026-09-14T12:00:06Z";"PolicyNoLongerEligible"),prepared(2;true;"2026-09-14T12:00:08Z";"PolicyNoLongerEligible")],
  released:prepared(3;false;"2026-09-14T12:00:09Z";"PolicyNoLongerEligible"),
  completionBaseline:completed("2026-09-14T12:00:16Z"),
  completed:[completed("2026-09-14T12:00:18Z"),completed("2026-09-14T12:00:20Z"),completed("2026-09-14T12:00:22Z")],
  admission:{name:"alfred-e2e-retry-test",username:"system:serviceaccount:ome:ome-alfred",
    metricsBefore:{denyCount:1,processStart:1234,apiServerUID:"apiserver-uid",capturedAt:"2026-09-14T11:59:59Z"},
    metricsRejected:{denyCount:2,processStart:1234,apiServerUID:"apiserver-uid",capturedAt:"2026-09-14T12:00:03Z"},
    deniedProbe:{exitCode:1,output:"denied by alfred-e2e-retry-test: semantic-retry publication held"},
    allowedProbe:{exitCode:0,output:({metadata:{uid:"owner-uid",annotations:{($e.request.annotationKey):(entry.payload)}}}|tojson)},bindingRemoved:true,
    policy:(retry_policy("alfred-e2e-retry-test";"owner-uid";"system:serviceaccount:ome:ome-alfred") |
      .metadata.uid="temporary-policy" | .metadata.generation=1 | .status={observedGeneration:1,typeChecking:{expressionWarnings:[]}}),
    binding:(retry_binding("alfred-e2e-retry-test") | .metadata.uid="temporary-binding" | .metadata.generation=1),
    guardBefore:{policy:{metadata:{uid:"guard-policy",generation:1},spec:{}},binding:{metadata:{uid:"guard-binding",generation:1},spec:{}}},
    guardAfter:{policy:{metadata:{uid:"guard-policy",generation:1},spec:{}},binding:{metadata:{uid:"guard-binding",generation:1},spec:{}}}}}
