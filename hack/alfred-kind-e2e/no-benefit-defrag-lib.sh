#!/usr/bin/env bash
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
