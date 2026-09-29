#!/usr/bin/env bash
# Synthetic offline files exercise byte-integrity checks, never live assertions.
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
jq -n -f "${dir}/useful-defrag-fixture.jq" | jq -f "${dir}/no-benefit-defrag-fixture.jq" >"${tmp}/fixture.json"
jq '.baseline' "${tmp}/fixture.json" >"${tmp}/baseline.json"
jq '.config' "${tmp}/fixture.json" >"${tmp}/enabled-config.json"
jq '{metadata:{name:.policyName,namespace:"ome",uid:"policy"},data:{"config.yaml":(.config|.policies.defragmentation.enabled=false|tojson)}}' "${tmp}/fixture.json" >"${tmp}/config-before.json"
jq '.metadata.name="alfred-config"|.metadata.uid="helm-policy"' "${tmp}/config-before.json" >"${tmp}/original-config.json"
jq '{spec:{template:{spec:{containers:[{name:"alfred",args:["--config-name="+.policyName]}]}}}}' "${tmp}/fixture.json" >"${tmp}/deployment-barrier.json"
jq '{metadata:{name:.policyName,namespace:"ome",uid:"policy"},data:{"config.yaml":(.config|tojson)}}' "${tmp}/fixture.json" >"${tmp}/config-enabled.json"
jq '.' "${tmp}/config-enabled.json" >"${tmp}/config-original.json"
jq -c '.requestWatch[]' "${tmp}/fixture.json" >"${tmp}/requests.jsonl"
jq -c '.podWatch[]' "${tmp}/fixture.json" >"${tmp}/pods.jsonl"
touch "${tmp}/requests.stderr" "${tmp}/pods.stderr"
for index in 1 2 3; do
  prefix="${tmp}/barrier-${index}"
  jq --argjson index "$((index-1))" '.attempts[$index].request' "${tmp}/fixture.json" >"${prefix}-request.json"
  jq --argjson index "$((index-1))" '.attempts[$index].result' "${tmp}/fixture.json" >"${prefix}-result.json"
  jq --argjson index "$((index-1))" --rawfile q "${prefix}-request.json" --rawfile r "${prefix}-result.json" \
    --arg qhash "$(shasum -a 256 "${prefix}-request.json"|awk '{print $1}')" \
    --arg rhash "$(shasum -a 256 "${prefix}-result.json"|awk '{print $1}')" '
    .attempts[$index]|.held|=(.requestBytes=($q|@base64)|.resultBytes=($r|@base64)|.requestSHA256=$qhash|.resultSHA256=$rhash)|
    .receipt.requestSHA256=$qhash|.receipt.resultSHA256=$rhash' "${tmp}/fixture.json" >"${prefix}-attempt.json"
  for mapping in held:held receipt:receipt beforeRelease:before-release observed:observed; do
    jq ".${mapping%%:*}" "${prefix}-attempt.json" >"${prefix}-${mapping#*:}.json"
  done
  jq '.held|{nonce}' "${prefix}-attempt.json" >"${prefix}-release-request.json"
  jq -c '.samples[]' "${prefix}-attempt.json" >"${prefix}-api.jsonl"
done
jq --slurpfile a "${tmp}/barrier-1-attempt.json" --slurpfile b "${tmp}/barrier-2-attempt.json" \
  --slurpfile c "${tmp}/barrier-3-attempt.json" '.attempts=[$a[0],$b[0],$c[0]]' "${tmp}/fixture.json" >"${tmp}/evidence.json"
bash "${dir}/verify-no-benefit-defrag.sh" "${tmp}" >/dev/null
for mutation in '.metadata.uid="other"' '.data["config.yaml"]|=(fromjson|.policies.defragmentation.enabled=false|tojson)'; do
  jq "${mutation}" "${tmp}/config-original.json" >"${tmp}/config-enabled.json"
  if bash "${dir}/verify-no-benefit-defrag.sh" "${tmp}" >/dev/null 2>&1; then
    echo "raw verifier accepted uninstalled policy: ${mutation}" >&2; exit 1
  fi
done
jq '.' "${tmp}/config-original.json" >"${tmp}/config-enabled.json"
for file in original-config deployment-barrier; do
  cp "${tmp}/${file}.json" "${tmp}/${file}-saved.json"
done
for mutation in '.metadata.uid="policy"' '.data["config.yaml"]="different copied policy"'; do
  jq "${mutation}" "${tmp}/original-config-saved.json" >"${tmp}/original-config.json"
  if bash "${dir}/verify-no-benefit-defrag.sh" "${tmp}" >/dev/null 2>&1; then
    echo "raw verifier accepted invalid policy isolation: ${mutation}" >&2; exit 1
  fi
done
cp "${tmp}/original-config-saved.json" "${tmp}/original-config.json"
for mutation in '.spec.template.spec.containers[0].args=["--config-name=alfred-config"]' \
  '.spec.template.spec.containers[0].args+=.spec.template.spec.containers[0].args'; do
  jq "${mutation}" "${tmp}/deployment-barrier-saved.json" >"${tmp}/deployment-barrier.json"
  if bash "${dir}/verify-no-benefit-defrag.sh" "${tmp}" >/dev/null 2>&1; then
    echo "raw verifier accepted ambiguous policy selection: ${mutation}" >&2; exit 1
  fi
done
cp "${tmp}/deployment-barrier-saved.json" "${tmp}/deployment-barrier.json"
for file in original-config deployment-barrier; do
  mv "${tmp}/${file}.json" "${tmp}/${file}-held.json"
  if bash "${dir}/verify-no-benefit-defrag.sh" "${tmp}" >/dev/null 2>&1; then
    echo "raw verifier accepted missing isolation artifact: ${file}" >&2; exit 1
  fi
  mv "${tmp}/${file}-held.json" "${tmp}/${file}.json"
done
bash "${dir}/verify-no-benefit-defrag.sh" "${tmp}" >/dev/null
for mutation in '.requestSHA256="changed"' '.requestBytes=("{}"|@base64)' '.resultBytes=("{}"|@base64)' '.nonce="changed"'; do
  jq ".held|${mutation}" "${tmp}/barrier-1-attempt.json" >"${tmp}/barrier-1-held.json"
  if bash "${dir}/verify-no-benefit-defrag.sh" "${tmp}" >/dev/null 2>&1; then
    echo "raw verifier accepted changed held bytes: ${mutation}" >&2; exit 1
  fi
done
echo 'no-benefit raw evidence integrity tests passed'
