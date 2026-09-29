#!/usr/bin/env bash
# Names are only unique within a resource kind and namespace. UIDs identify
# the actual created object, including a successor reusing the same name.
nd_cleanup_key() {
  jq -er '(.kind+"-"+.metadata.uid)|select(test("^[A-Za-z0-9-]+$"))'
}

# kubectl emits individual created objects even when the input was a List.
# Accept either response shape, but only return explicitly recorded identities.
nd_created_objects() {
  jq -cs '[.[]|if .kind=="List" then .items[] else . end]|
    if length==0 or any(.[];
      (.metadata.uid|type!="string" or length==0) or
      (.metadata.name|type!="string" or length==0) or
      (.kind as $kind|["Pod","InferenceService","ClusterServingRuntime","ConfigMap"]|index($kind)|not))
    then error("invalid created-object identities") else .[] end' "$@"
}

# Keep test policy ownership off the Helm-managed ConfigMap. Both the policy
# argument and simulator barrier are installed/restored as one fenced change.
nd_deployment_patch() {
  jq -cn --argjson old "$1" --argjson current "$2" --arg registry "$3" --arg policy "$4" --arg mode "$5" '
    $old.spec.template.spec.volumes as $volumes | $old.spec.template.spec.containers[0].args as $args |
    if $old.metadata.uid!=$current.metadata.uid or ($old.metadata.uid|type!="string" or length==0) or
      ($current.metadata.resourceVersion|type!="string" or length==0) or
      ($old.spec.template.spec.containers|length)!=1 or ($current.spec.template.spec.containers|length)!=1 or
      $old.spec.template.spec.containers[0].name!="alfred" or $current.spec.template.spec.containers[0].name!="alfred" or
      ([$volumes[]|select(.name=="simulation")]|length)!=1 or
      ([$args[]|select(startswith("--simulation-timeout="))]|length)!=1 or
      ([$args[]|select(startswith("--config-name="))]|length)!=1 or
      ([$args[]|select(.=="--config-name=alfred-config")]|length)!=1
    then error("unexpected Alfred deployment") else
    ($volumes|map(if .name=="simulation" then .configMap.name=$registry else . end)) as $installedVolumes |
    ($args|map(if startswith("--simulation-timeout=") then "--simulation-timeout=20s"
      elif startswith("--config-name=") then "--config-name="+$policy else . end)) as $installedArgs |
    (if $mode=="install" then {expectedV:$volumes,expectedA:$args,targetV:$installedVolumes,targetA:$installedArgs}
     elif $mode=="restore" then {expectedV:$installedVolumes,expectedA:$installedArgs,targetV:$volumes,targetA:$args}
     else error("invalid deployment operation") end) as $change |
    if $current.spec.template.spec.volumes==$change.targetV and $current.spec.template.spec.containers[0].args==$change.targetA then []
    elif $current.spec.template.spec.volumes!=$change.expectedV or $current.spec.template.spec.containers[0].args!=$change.expectedA
    then error("deployment arguments or volumes changed during test") else
    [{op:"test",path:"/metadata/uid",value:$old.metadata.uid},
     {op:"test",path:"/metadata/resourceVersion",value:$current.metadata.resourceVersion},
     {op:"replace",path:"/spec/template/spec/volumes",value:$change.targetV},
     {op:"replace",path:"/spec/template/spec/containers/0/args",value:$change.targetA}] end end'
}

# Compare-and-swap only the policy key; never overwrite another writer's edit.
nd_config_patch() {
  jq -cn --argjson before "$1" --argjson current "$2" --arg installed "$3" --arg mode "$4" '
    if $before.metadata.uid!=$current.metadata.uid or ($before.metadata.uid|type!="string" or length==0)
    then error("config identity changed") else
    $before.data["config.yaml"] as $old | $current.data["config.yaml"] as $now |
    (if $mode=="install" then $old elif $mode=="restore" then $installed else error("invalid config operation") end) as $expected |
    (if $mode=="install" then $installed else $old end) as $target |
    if $now==$target then [] elif $now!=$expected then error("policy changed by another writer") else
    [{op:"test",path:"/metadata/uid",value:$before.metadata.uid},
     {op:"test",path:"/data/config.yaml",value:$expected},
     {op:"replace",path:"/data/config.yaml",value:$target}] end end'
}
