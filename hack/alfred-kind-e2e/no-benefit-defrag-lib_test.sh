#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${dir}/no-benefit-defrag-lib.sh"
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
