#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${dir}/namespace-churn-lib.sh"

# The observer must cover the nested deadline plus post-deadline diagnostics,
# cleanup, initial API capture and completion sampling (at least 375s total).
# A fixed 600s watch truncates both a slow default run and an extended run.
for deadline in 360 1200 3600; do
  watch_timeout="$(churn_watch_timeout_seconds "${deadline}")"
  if ((watch_timeout <= deadline + 375 || watch_timeout > deadline + 1800)); then
    echo "watch ends before allowed completion work: deadline=${deadline}, watch=${watch_timeout}" >&2
    exit 1
  fi
done

# OME consumes a published annotation quickly. The watch, bound to the exact
# journal identity/payload, must prove publication even after consumption.
payload='{"schemaVersion":"v1","component":"engine","instance":0,"from_node":"source","requested_by":"alfred"}'
published="$(jq -cn --arg payload "${payload}" '{
  isvc:{metadata:{uid:"owner",annotations:{}}},ir:{metadata:{uid:"ir"}},
  requests:[{key:"ome.io/migration-request-v1-id",value:$payload}],
  journal:{data:{"state.json":({entries:[{uuid:"id",workloadUID:"owner",irUID:"ir",phase:"submitted",payload:$payload}]}|tojson)}}}')"
churn_request_published owner ir <<<"${published}" || { echo 'consumed but witnessed publication rejected' >&2; exit 1; }
for mutation in '.requests=[]' '.requests += [.requests[0]|.value="{}"]' '.isvc.metadata.uid="other"' \
  '.journal.data["state.json"]|=(fromjson|.entries[0].phase="prepared"|tojson)' \
  '.journal.data["state.json"]|=(fromjson|.entries[0].payload="{}"|tojson)' \
  '.journal.data["state.json"]|=(fromjson|.entries[0].irUID="other"|tojson)' \
  '.isvc.metadata.annotations["ome.io/migration-request-v1-other"]="{}"'; do
  if jq "${mutation}" <<<"${published}" | churn_request_published owner ir; then
    echo "unsafe publication accepted: ${mutation}" >&2; exit 1
  fi
done
churn_within_window 100.25 129.249 || { echo 'timely observation rejected' >&2; exit 1; }
for times in '100 130' '100 135' '100 99' '-1 0'; do
  read -r start finish <<<"${times}"
  if churn_within_window "${start}" "${finish}"; then echo "invalid observation window accepted: ${times}" >&2; exit 1; fi
done

before='{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"noise","uid":"noise-uid","resourceVersion":"1","labels":{"tenant":"other"},"annotations":{"observer":"1"}},"status":{"phase":"Active"}}'
after="$(jq '.metadata.resourceVersion="2" | .metadata.annotations.observer="2"' <<<"${before}")"
churn_check_mutation "${before}" "${after}" annotations observer || { echo 'annotation-only API mutation was rejected' >&2; exit 1; }
after="$(jq '.metadata.resourceVersion="2" | .metadata.labels.tenant="prod"' <<<"${before}")"
churn_check_mutation "${before}" "${after}" labels tenant || { echo 'label-only API mutation was rejected' >&2; exit 1; }
reject() {
  if churn_check_mutation "${before}" "$1" annotations observer; then
    echo "unsafe Namespace mutation accepted: $2" >&2; exit 1
  fi
}
reject "${before}" unchanged
reject "$(jq '.metadata.annotations.observer="2"' <<<"${before}")" stale-rv
reject "$(jq '.metadata.resourceVersion="2"' <<<"${before}")" no-churn
reject "$(jq '.metadata.resourceVersion="2" | .metadata.annotations.observer="2" | .metadata.uid="successor"' <<<"${before}")" successor
reject "$(jq '.metadata.resourceVersion="2" | .metadata.annotations.observer="2" | .metadata.labels.tenant="prod"' <<<"${before}")" labels
reject "$(jq '.metadata.resourceVersion="2" | .metadata.annotations.observer="2" | .status.phase="Terminating"' <<<"${before}")" lifecycle
reject "$(jq '.metadata.resourceVersion="2" | .metadata.annotations.observer="2" | .metadata.annotations.unrelated="change"' <<<"${before}")" other-annotation
reject "$(jq '.metadata.resourceVersion="2" | .metadata.annotations.observer="2" | .spec.futureField=true' <<<"${before}")" unknown-field
echo 'Namespace churn mutation-boundary tests passed'

# Raw worker bytes, including whitespace, must be authenticated before use.
scratch="$(mktemp -d)"
trap 'rm -r "${scratch}"' EXIT
jq -cn --arg payload "${payload}" '
  {metadata:{uid:"owner",annotations:{}}},
  {metadata:{uid:"owner",annotations:{"ome.io/migration-request-v1-id":$payload}}},
  {metadata:{uid:"owner",annotations:{}}}' >"${scratch}/watch"
requests="$(churn_watch_requests "${scratch}/watch" owner)" || { echo 'consumed request lost from watch' >&2; exit 1; }
jq -en --argjson requests "${requests}" --argjson published "${published}" '$requests==$published.requests' >/dev/null
printf '{"metadata":{"uid":"successor"}}\n' >>"${scratch}/watch"
if churn_watch_requests "${scratch}/watch" owner >/dev/null; then echo 'successor watch accepted' >&2; exit 1; fi
printf '{"metadata":' >"${scratch}/watch"
if churn_watch_requests "${scratch}/watch" owner >/dev/null; then echo 'partial watch treated as empty' >&2; exit 1; fi
jq -cn --arg payload "${payload}" '
  {metadata:{uid:"owner",annotations:{"ome.io/migration-request-v1-id":$payload}}},
  {metadata:{uid:"owner",annotations:{"ome.io/migration-request-v1-id":"{}"}}}' >"${scratch}/watch"
[[ "$(churn_watch_requests "${scratch}/watch" owner | jq length)" == 2 ]] || { echo 'watch hid changed payload under same UUID' >&2; exit 1; }
printf ' {"requestID":"preflight"}\n\n' >"${scratch}/request"
printf '\n{"decision":"Feasible"}  \n' >"${scratch}/result"
jq -n --rawfile request "${scratch}/request" --rawfile result "${scratch}/result" \
  --arg requestHash "$(shasum -a 256 "${scratch}/request" | awk '{print $1}')" \
  --arg resultHash "$(shasum -a 256 "${scratch}/result" | awk '{print $1}')" \
  '{requestBytes:($request|@base64),resultBytes:($result|@base64),requestSHA256:$requestHash,resultSHA256:$resultHash,
    nonce:"abcdefghijklmnopqrstuvwxyz0123456789",heldAt:"2026-09-29T00:00:00Z",deadline:"2026-09-29T00:00:03Z"}' >"${scratch}/held.json"
churn_decode_held "${scratch}/held.json" "${scratch}/decoded" || { echo 'valid held bytes rejected' >&2; exit 1; }
cmp "${scratch}/request" "${scratch}/decoded-request.json"
cmp "${scratch}/result" "${scratch}/decoded-result.json"
for corruption in '.requestSHA256="wrong"' '.resultBytes=("changed"|@base64)' '.nonce=""' '.requestBytes=null' '.deadline=null'; do
  jq "${corruption}" "${scratch}/held.json" >"${scratch}/bad.json"
  if churn_decode_held "${scratch}/bad.json" "${scratch}/bad" >/dev/null 2>&1; then
    echo "invalid held evidence accepted: ${corruption}" >&2; exit 1
  fi
done
echo 'Namespace churn exact-byte tests passed'

original='{"metadata":{"uid":"deployment-uid","resourceVersion":"1"},"spec":{"template":{"spec":{"volumes":[{"name":"simulation","configMap":{"name":"normal"}},{"name":"other","emptyDir":{}}],"containers":[{"name":"alfred","args":["--simulation-timeout=10s","--other=true"]}]}}}}'
installed="$(jq '.metadata.resourceVersion="2" | .spec.template.spec.volumes[0].configMap.name="barrier" | .spec.template.spec.containers[0].args[0]="--simulation-timeout=20s"' <<<"${original}")"
patch="$(churn_restore_patch "${original}" "${installed}" barrier)" || { echo 'owned deployment restore was rejected' >&2; exit 1; }
jq -e --argjson patch "${patch}" --argjson old "${original}" '
  $patch[0]=={op:"test",path:"/metadata/uid",value:"deployment-uid"} and
  $patch[1]=={op:"test",path:"/metadata/resourceVersion",value:"2"} and
  $patch[2].value==$old.spec.template.spec.volumes and
  $patch[3].value==$old.spec.template.spec.containers[0].args' <<<null >/dev/null
[[ "$(churn_restore_patch "${original}" "${original}" barrier)" == '[]' ]] || { echo 'unchanged deployment not recognized' >&2; exit 1; }
for mutation in '.metadata.uid="successor"' '.spec.template.spec.containers[0].name="other"' \
  '.spec.template.spec.containers[0].args += ["--new-option=true"]' \
  '.spec.template.spec.volumes += [{"name":"new","emptyDir":{}}]' \
  '.spec.template.spec.volumes[0].configMap.name="third-party"'; do
  current="$(jq "${mutation}" <<<"${installed}")"
  if churn_restore_patch "${original}" "${current}" barrier >/dev/null 2>&1; then
    echo "concurrent deployment change would be overwritten: ${mutation}" >&2; exit 1
  fi
done
echo 'Namespace churn restore-fence tests passed'
