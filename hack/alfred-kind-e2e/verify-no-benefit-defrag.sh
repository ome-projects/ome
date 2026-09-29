#!/usr/bin/env bash
# Authenticate raw real-worker bytes and then verify their observed API effects.
set -euo pipefail
art="${1:-}"
[[ -d "${art}" ]] || { echo 'usage: verify-no-benefit-defrag.sh ARTIFACT_DIR' >&2; exit 2; }
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for index in 1 2 3; do
  prefix="${art}/barrier-${index}"
  cmp -s <(jq -erj '.requestBytes|@base64d' "${prefix}-held.json") "${prefix}-request.json"
  cmp -s <(jq -erj '.resultBytes|@base64d' "${prefix}-held.json") "${prefix}-result.json"
  jq -e --arg q "$(shasum -a 256 "${prefix}-request.json"|awk '{print $1}')" \
    --arg r "$(shasum -a 256 "${prefix}-result.json"|awk '{print $1}')" \
    '.requestSHA256==$q and .resultSHA256==$r' "${prefix}-held.json" >/dev/null
  jq -e --slurpfile held "${prefix}-held.json" '.nonce==$held[0].nonce' "${prefix}-release-request.json" >/dev/null
  jq -e --argjson index "$((index-1))" --slurpfile held "${prefix}-held.json" --slurpfile q "${prefix}-request.json" \
    --slurpfile r "${prefix}-result.json" --slurpfile receipt "${prefix}-receipt.json" \
    --slurpfile before "${prefix}-before-release.json" --slurpfile observed "${prefix}-observed.json" \
    --slurpfile samples "${prefix}-api.jsonl" '
    .attempts[$index]=={held:$held[0],request:$q[0],result:$r[0],receipt:$receipt[0],beforeRelease:$before[0],observed:$observed[0],samples:$samples}' \
    "${art}/evidence.json" >/dev/null
done
[[ ! -s "${art}/requests.stderr" && ! -s "${art}/pods.stderr" ]]
jq -e --slurpfile before "${art}/config-before.json" --slurpfile enabled "${art}/config-enabled.json" '
  ($before[0].metadata.uid|type=="string" and length>0) and
  $enabled[0].metadata.uid==$before[0].metadata.uid and
  $enabled[0].metadata.name=="alfred-config" and $enabled[0].metadata.namespace=="ome" and
  .config==($enabled[0].data["config.yaml"]|fromjson)' "${art}/evidence.json" >/dev/null
original="$(jq -r '.data["config.yaml"]' "${art}/config-before.json" | yq -o=json '.')"
jq -e --argjson original "${original}" '
  $original.policies.defragmentation.enabled==false and
  .config==($original|.policies.defragmentation|=(.enabled=true|.fragmentationThreshold=0.1|
    .scoring.sizeLadder=[8]|.scoring.sizePrior={"8":1}|.scoring.demandBlendLambda=1))' "${art}/evidence.json" >/dev/null
jq -e --slurpfile requests "${art}/requests.jsonl" --slurpfile pods "${art}/pods.jsonl" \
  --slurpfile baseline "${art}/baseline.json" --slurpfile config "${art}/enabled-config.json" \
  '.requestWatch==$requests and .podWatch==$pods and .baseline==$baseline[0] and .config==$config[0]' "${art}/evidence.json" >/dev/null
jq -e -L "${dir}" -f "${dir}/verify-no-benefit-defrag.jq" "${art}/evidence.json" >/dev/null
echo 'PASS no-benefit-defrag: three real feasible placements withheld without migration'
