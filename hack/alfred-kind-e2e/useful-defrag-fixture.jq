# Offline verifier fixture only. Never apply these fabricated API objects to a cluster.
def pod($name;$uid;$node;$gpu;$instance;$ready):
  {apiVersion:"v1",kind:"Pod",metadata:{name:$name,namespace:"alfred-e2e-defrag",uid:$uid,
    labels:{"ome.io/inferenceservice":"single","component":"engine","ome.io/instance-index":$instance},
    ownerReferences:[{kind:"InferenceReplica",name:"single-engine",uid:"replica",controller:true}]},
   spec:{schedulerName:"alfred-default-scheduler",nodeName:$node,containers:[{name:"model",
     resources:{requests:{"nvidia.com/gpu":$gpu},limits:{"nvidia.com/gpu":$gpu}}}]},
   status:{phase:"Running",conditions:[{type:"Ready",status:$ready},{type:"ome.io/serving",status:$ready}]}};
def endpoint($uid):
  {items:[{metadata:{labels:{"kubernetes.io/service-name":"single-engine-rev-test"}},
    endpoints:[{targetRef:{uid:$uid},conditions:{ready:true,terminating:false}}]}]};
"11111111-2222-4333-8444-555555555555" as $uuid |
{schemaVersion:"v1",requested_by:"alfred",reason:"Fragmentation",from_node:"alfred-kwok-gpu-a",instance:0,component:"engine"} as $payload |
{key:("ome.io/migration-request-v1-"+$uuid),value:($payload|tojson)} as $request |
pod("single-engine-0-default-0";"source";"alfred-kwok-gpu-a";"1";"0";"True") as $source |
pod("single-engine-1-default-0";"replacement";"alfred-kwok-gpu-b";"1";"1";"True") as $replacement |
{apiVersion:"v1",kind:"InferenceService",metadata:{name:"single",namespace:"alfred-e2e-defrag",uid:"owner",generation:1,annotations:{}},spec:{runtime:{name:"alfred-e2e-defrag-runtime"}}} as $owner |
{apiVersion:"ome.io/v1beta1",kind:"InferenceReplica",metadata:{name:"single-engine",namespace:"alfred-e2e-defrag",uid:"replica",generation:1,
  ownerReferences:[{kind:"InferenceService",name:"single",uid:"owner",controller:true}]},
 spec:{component:"engine",parentRef:{name:"single"}},status:{readyReplicas:1,servingReplicas:1,availableReplicas:1,migrations:[]}} as $ir |
{requestUUID:$uuid,phase:"Completed",sourceInstance:0,surgeInstance:1,fromNode:"alfred-kwok-gpu-a",trigger:"Manual",completedAt:"2026-09-29T04:00:00Z"} as $migration |
{uuid:$uuid,workload:{Namespace:"alfred-e2e-defrag",Name:"single"},workloadUID:"owner",irName:"single-engine",irUID:"replica",component:"engine",instance:0,
 fromNode:"alfred-kwok-gpu-a",payload:$request.value,phase:"completed",completedAt:"2026-09-29T04:00:01Z"} as $entry |
{metadata:{name:"beneficiary",namespace:"alfred-e2e-defrag",uid:"beneficiary"},spec:{schedulerName:"alfred-default-scheduler",containers:[{name:"pause",resources:{requests:{"nvidia.com/gpu":"8"},limits:{"nvidia.com/gpu":"8"}}}]},
 status:{phase:"Pending",conditions:[{type:"PodScheduled",status:"False",reason:"Unschedulable",message:"Insufficient nvidia.com/gpu"}]}} as $before |
($before|.spec.nodeName="alfred-kwok-gpu-a"|.status={phase:"Running",conditions:[{type:"Ready",status:"True"}]}) as $after |
def sample($phase;$second):
  {isvc:($owner|if $phase=="baseline" then . else .metadata.annotations={($request.key):$request.value} end),
   ir:($ir|if $phase=="baseline" then . else .status.migrations=[$migration|if $phase=="held" then .phase="SurgePending"|del(.completedAt) else . end] end),
   pods:{items:(if $phase=="baseline" then [$source] elif $phase=="held" then [$source,($replacement|.status.conditions|=map(.status="False"))] else [$replacement] end)},
   endpoints:endpoint(if $phase=="completed" then "replacement" else "source" end),
   beneficiary:(if $phase=="completed" then $after else $before end),
   dispatch:{data:{"state.json":({version:"v1",entries:(if $phase=="baseline" then [] else [$entry|if $phase=="held" then .phase="acknowledged"|del(.completedAt) else . end] end)}|tojson)}},
   recommendations:{data:{"last-cycle.json":({timestamp:("2026-09-29T04:00:0"+($second|tostring)+"Z"),mode:"execute",recommendations:[]}|tojson)}}};
{items:[range(0;4)|["a","b","c","d"][.] as $suffix|{metadata:{name:("alfred-kwok-gpu-"+$suffix),uid:($suffix+"-node"),labels:{"alfred-e2e/virtual":"true"}},
 spec:{},status:{allocatable:{"nvidia.com/gpu":"8"},conditions:[{type:"Ready",status:"True"}]}}]} as $nodes |
[pod("block-b";"block-b";"alfred-kwok-gpu-b";"7";"";"True"),pod("block-c";"block-c";"alfred-kwok-gpu-c";"8";"";"True"),pod("block-d";"block-d";"alfred-kwok-gpu-d";"8";"";"True")|
  .metadata.labels={"alfred-e2e/blocker":"true"}|del(.metadata.ownerReferences)] as $blockers |
{scenario:"useful-defrag",before:$before,after:$after,source:$source,replacement:$replacement,routingService:"single-engine-rev-test",
 nodesBefore:$nodes,nodesAfter:$nodes,podsBefore:{items:([$source,$before]+$blockers)},podsAfter:{items:([$replacement,$after]+$blockers)},
 capacityBefore:[{name:"alfred-kwok-gpu-a",free:7},{name:"alfred-kwok-gpu-b",free:1},{name:"alfred-kwok-gpu-c",free:0},{name:"alfred-kwok-gpu-d",free:0}],
 request:{uuid:$uuid,payload:$payload},migration:$migration,
 baseline:sample("baseline";0),held:sample("held";0),
 handoff:[(sample("held";0)+{sourceSafe:true,replacementSafe:false}),(sample("completed";2)+{sourceSafe:false,replacementSafe:true})],
 completionBaseline:sample("completed";2),completed:[range(3;6)|sample("completed";.)],
 requestWatch:[$owner,($owner|.metadata.annotations={($request.key):$request.value})]} |
(.baseline,.held,.handoff[],.completionBaseline,.completed[]) |=
  (.allPods={items:(.pods.items+[.beneficiary]+$blockers)}|.nodes=$nodes) |
.completionSamples=.completed
