#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${dir}/no-benefit-defrag-lib.sh"
runtime='{"kind":"ClusterServingRuntime","metadata":{"uid":"runtime-uid","name":"same-name"}}'
registry='{"kind":"ConfigMap","metadata":{"uid":"config-uid","name":"same-name"}}'
[[ "$(nd_cleanup_key <<<"${runtime}")" == ClusterServingRuntime-runtime-uid ]]
[[ "$(nd_cleanup_key <<<"${registry}")" == ConfigMap-config-uid ]]
[[ "$(jq '.metadata.uid="successor"' <<<"${runtime}" | nd_cleanup_key)" != "$(nd_cleanup_key <<<"${runtime}")" ]]
if jq '.metadata.uid="../unsafe"' <<<"${runtime}" | nd_cleanup_key >/dev/null; then echo 'unsafe cleanup artifact path accepted' >&2; exit 1; fi
objects='{"kind":"InferenceService","metadata":{"uid":"owner","name":"single"}}
{"kind":"Pod","metadata":{"uid":"blocker","name":"block-b"}}
{"kind":"ConfigMap","metadata":{"uid":"registry","name":"registry"}}'
nd_created_objects <<<"${objects}" | jq -se '[.[].metadata.uid]==["owner","blocker","registry"]' >/dev/null
jq -s '{kind:"List",items:.}' <<<"${objects}" | nd_created_objects | jq -se '[.[].metadata.uid]==["owner","blocker","registry"]' >/dev/null
for malformed in '{"kind":"Pod","metadata":{"name":"no-uid"}}' '{"kind":"Namespace","metadata":{"name":"do-not-delete","uid":"namespace"}}'; do
  if nd_created_objects <<<"${malformed}" >/dev/null 2>&1; then echo 'unsafe cleanup identity accepted' >&2; exit 1; fi
done
before='{"metadata":{"uid":"config","resourceVersion":"1"},"data":{"config.yaml":"disabled","other":"keep"}}'
current='{"metadata":{"uid":"config","resourceVersion":"2","labels":{"unrelated":"keep"}},"data":{"config.yaml":"enabled","other":"updated"}}'
nd_config_patch "${before}" "${before}" enabled install | jq -e '.[1].value=="disabled" and .[2].value=="enabled" and length==3' >/dev/null
nd_config_patch "${before}" "${current}" enabled restore | jq -e '.[1].value=="enabled" and .[2].value=="disabled" and length==3' >/dev/null
[[ "$(nd_config_patch "${before}" "${before}" enabled restore)" == '[]' ]]
for mutation in '.metadata.uid="recreated"' '.data["config.yaml"]="other-policy"'; do
  if nd_config_patch "${before}" "$(jq "${mutation}" <<<"${current}")" enabled restore 2>/dev/null; then
    echo "unsafe config restore accepted: ${mutation}" >&2; exit 1
  fi
done
echo 'no-benefit config CAS tests passed'

deployment='{"metadata":{"uid":"alfred","resourceVersion":"1"},"spec":{"template":{"spec":{"volumes":[{"name":"simulation","configMap":{"name":"normal"}}],"containers":[{"name":"alfred","args":["--config-name=alfred-config","--simulation-timeout=10s","--other=true"]}]}}}}'
installed='{"metadata":{"uid":"alfred","resourceVersion":"2"},"spec":{"template":{"spec":{"volumes":[{"name":"simulation","configMap":{"name":"barrier"}}],"containers":[{"name":"alfred","args":["--config-name=temp-policy","--simulation-timeout=20s","--other=true"]}]}}}}'
nd_deployment_patch "${deployment}" "${deployment}" barrier temp-policy install | jq -e '.[2].value==[{name:"simulation",configMap:{name:"barrier"}}] and .[3].value==["--config-name=temp-policy","--simulation-timeout=20s","--other=true"]' >/dev/null
nd_deployment_patch "${deployment}" "${installed}" barrier temp-policy restore | jq -e '.[2].value==[{name:"simulation",configMap:{name:"normal"}}] and .[3].value==["--config-name=alfred-config","--simulation-timeout=10s","--other=true"]' >/dev/null
[[ "$(nd_deployment_patch "${deployment}" "${deployment}" barrier temp-policy restore)" == '[]' ]]
[[ "$(nd_deployment_patch "${deployment}" "${installed}" barrier temp-policy install)" == '[]' ]]
for mutation in '.metadata.uid="other"' '.spec.template.spec.containers[0].name="other"' \
  '.spec.template.spec.containers[0].args[0]="--config-name=third-party"' \
  '.spec.template.spec.containers[0].args[1]="--simulation-timeout=30s"' \
  '.spec.template.spec.volumes[0].configMap.name="third-party"'; do
  if nd_deployment_patch "${deployment}" "$(jq "${mutation}" <<<"${installed}")" barrier temp-policy restore >/dev/null 2>&1; then
    echo "unsafe deployment restore accepted: ${mutation}" >&2; exit 1
  fi
done
for mutation in '.spec.template.spec.containers[0].args|=.[1:]' \
  '.spec.template.spec.containers[0].args+=["--config-name=duplicate"]'; do
  malformed="$(jq "${mutation}" <<<"${deployment}")"
  if nd_deployment_patch "${malformed}" "${malformed}" barrier temp-policy install >/dev/null 2>&1; then
    echo 'ambiguous policy flag accepted' >&2; exit 1
  fi
done
echo 'no-benefit isolated-policy deployment tests passed'
