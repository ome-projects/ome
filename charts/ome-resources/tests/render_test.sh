#!/usr/bin/env bash
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${HELM_BIN:-helm}"

fail() {
  echo "ome-resources chart test: $*" >&2
  exit 1
}

assert_default_trafficmap_publisher() {
  local rendered_config="$1"
  local scenario="$2"
  local multicluster_json
  local compact_multicluster_json

  multicluster_json="$(awk '
    /^  multicluster: \|-$/ { capture = 1; next }
    capture && /^  [[:alnum:]_-]+: \|-$/ { exit }
    capture { sub(/^    /, ""); print }
  ' <<<"${rendered_config}")"
  compact_multicluster_json="$(tr -d '[:space:]' <<<"${multicluster_json}")"
  grep -Fq '"publisher":{"name":"","resyncInterval":"1m","options":{}}' \
    <<<"${compact_multicluster_json}" ||
    fail "${scenario} did not normalize to the default TrafficMap publisher"
}

render_legacy_publisher_default() {
  local legacy_value="$1"
  local fixture_name="$2"
  local fixture_dir="${legacy_chart_fixtures}/${fixture_name}"

  cp -R "${chart_dir}" "${fixture_dir}"
  awk -v legacy_value="${legacy_value}" '
    /^        publisher:$/ {
      print "        publisher: " legacy_value
      replacing = 1
      replacements++
      next
    }
    replacing && /^        probe:/ { replacing = 0 }
    !replacing { print }
    END { if (replacements != 1) exit 1 }
  ' "${fixture_dir}/values.yaml" >"${fixture_dir}/values.yaml.next" ||
    fail "could not construct ${fixture_name} publisher values fixture"
  mv "${fixture_dir}/values.yaml.next" "${fixture_dir}/values.yaml"

  "${helm_bin}" template ome-resources "${fixture_dir}" \
    --namespace ome \
    --show-only templates/ome-controller/configmap.yaml
}

rendered="$("${helm_bin}" template ome-resources "${chart_dir}" --namespace ome)"
controller="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --show-only templates/ome-controller/deployment.yaml)"
controller_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq 'helm.sh/chart: ome-resources-0.1.0' <<<"${controller}" ||
  fail "controller chart version label was not rendered"
grep -Fq 'app.kubernetes.io/version: "1.16.0"' <<<"${controller}" ||
  fail "controller app version label was not rendered"
grep -Fq '"scaleUpPodBatchSize":100' <<<"${controller_config}" ||
  fail "default OMENative scale-up Pod batch size was not rendered"
grep -Fq '"scaleDownPodBatchSize":100' <<<"${controller_config}" ||
  fail "default OMENative scale-down Pod batch size was not rendered"
grep -Fq '"scaleDownRequeueInterval":"5s"' <<<"${controller_config}" ||
  fail "default OMENative scale-down requeue interval was not rendered"
grep -Fq '"rawDeployment":{"maxUnavailable":1}' <<<"${controller_config}" ||
  fail "default RawDeployment disruption budget was not rendered"
grep -Fq '"omeNative":{"maxUnavailable":1}' <<<"${controller_config}" ||
  fail "default OMENative disruption budget was not rendered"
if grep -Fq 'tpuSliceProvisioning' <<<"${controller_config}"; then
  fail "tpuSliceProvisioning was rendered when unset"
fi

tpu_slice_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-json 'ome.controller.tpuSliceProvisioning={"chipResource":"example.com/tpu","accelerators":{"acc-a":{"sliceType":"type-a","chipsPerHost":4,"topologies":["2x2x1"]}},"slice":{"annotations":{},"readyStates":["READY"]}}' \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"accelerators":{"acc-a":{"chipsPerHost":4,"sliceType":"type-a","topologies":["2x2x1"]}}' <<<"${tpu_slice_config}" ||
  fail "tpuSliceProvisioning accelerators were not rendered as JSON"
grep -Fq '"slice":{"annotations":{},"readyStates":["READY"]}' <<<"${tpu_slice_config}" ||
  fail "tpuSliceProvisioning did not keep explicitly empty slice annotations"
if grep -Fq 'minReadySeconds' <<<"${controller_config}"; then
  fail "deploy.minReadySeconds was rendered when unset"
fi

# The InferenceReplica admission webhook admits spec writes on projected
# replicas only from the controller identity, so the chart always lists the
# ServiceAccount the controller Deployment runs as and adds configured
# identities to it.
inference_replica_identity() {
  awk '
    /^  inferenceReplica: \|-$/ { capture = 1; next }
    capture && /^    / { print; next }
    capture { exit }
  ' <<<"$1" | tr -d '[:space:]'
}
controller_username() {
  awk '
    /^  namespace: / && namespace == "" { namespace = $2 }
    $1 == "serviceAccountName:" && account == "" { account = $2 }
    END { if (namespace != "" && account != "") print "system:serviceaccount:" namespace ":" account }
  ' <<<"$1"
}
default_identity="$(inference_replica_identity "${controller_config}")"
[[ "${default_identity}" == '{"controllerIdentity":{"usernames":["system:serviceaccount:ome:ome-controller-manager"],"groups":[]}}' ]] ||
  fail "default controller identity is not the controller ServiceAccount alone: ${default_identity}"
default_controller_username="$(controller_username "${controller}")"
[[ -n "${default_controller_username}" ]] ||
  fail "controller Deployment namespace or serviceAccountName was not rendered"
grep -Fq "\"${default_controller_username}\"" <<<"${default_identity}" ||
  fail "controller identity does not list ${default_controller_username}, the account the controller Deployment runs as"

team_a_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace team-a \
  --show-only templates/ome-controller/configmap.yaml)"
team_a_controller="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace team-a \
  --show-only templates/ome-controller/deployment.yaml)"
team_a_identity="$(inference_replica_identity "${team_a_config}")"
[[ "${team_a_identity}" == '{"controllerIdentity":{"usernames":["system:serviceaccount:team-a:ome-controller-manager"],"groups":[]}}' ]] ||
  fail "controller identity did not follow the release namespace: ${team_a_identity}"
team_a_controller_username="$(controller_username "${team_a_controller}")"
[[ -n "${team_a_controller_username}" ]] ||
  fail "team-a controller Deployment namespace or serviceAccountName was not rendered"
grep -Fq "\"${team_a_controller_username}\"" <<<"${team_a_identity}" ||
  fail "team-a controller identity does not list ${team_a_controller_username}, the account the controller Deployment runs as"

extra_username_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-json 'ome.controller.inferenceReplica.controllerIdentity.usernames=["system:serviceaccount:ome:custom"]' \
  --show-only templates/ome-controller/configmap.yaml)"
extra_username_identity="$(inference_replica_identity "${extra_username_config}")"
[[ "${extra_username_identity}" == '{"controllerIdentity":{"usernames":["system:serviceaccount:ome:ome-controller-manager","system:serviceaccount:ome:custom"],"groups":[]}}' ]] ||
  fail "an extra username did not add to the controller ServiceAccount: ${extra_username_identity}"

group_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-json 'ome.controller.inferenceReplica.controllerIdentity.groups=["group-a"]' \
  --show-only templates/ome-controller/configmap.yaml)"
group_identity="$(inference_replica_identity "${group_config}")"
[[ "${group_identity}" == '{"controllerIdentity":{"usernames":["system:serviceaccount:ome:ome-controller-manager"],"groups":["group-a"]}}' ]] ||
  fail "a configured group was not rendered beside the controller ServiceAccount: ${group_identity}"

for configured_usernames in \
  '["system:serviceaccount:ome:ome-controller-manager","system:serviceaccount:ome:custom"]' \
  '["system:serviceaccount:ome:custom","system:serviceaccount:ome:ome-controller-manager"]'; do
  dedup_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
    --namespace ome \
    --set-json "ome.controller.inferenceReplica.controllerIdentity.usernames=${configured_usernames}" \
    --show-only templates/ome-controller/configmap.yaml)"
  dedup_identity="$(inference_replica_identity "${dedup_config}")"
  [[ "${dedup_identity}" == '{"controllerIdentity":{"usernames":["system:serviceaccount:ome:ome-controller-manager","system:serviceaccount:ome:custom"],"groups":[]}}' ]] ||
    fail "configured usernames ${configured_usernames} did not list the controller ServiceAccount once and first: ${dedup_identity}"
done

dedup_group_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-json 'ome.controller.inferenceReplica.controllerIdentity.groups=["group-a","group-a"]' \
  --show-only templates/ome-controller/configmap.yaml)"
dedup_group_identity="$(inference_replica_identity "${dedup_group_config}")"
[[ "${dedup_group_identity}" == '{"controllerIdentity":{"usernames":["system:serviceaccount:ome:ome-controller-manager"],"groups":["group-a"]}}' ]] ||
  fail "a repeated group was not listed once: ${dedup_group_identity}"

# The manager rejects the whole block when a key holds a string instead of a
# list, or when a name is not a string, is blank or has surrounding whitespace.
# So a single name set as a scalar (--set key=name) renders as a list, an empty
# scalar renders as an empty list, and any other kind or a bad name fails the
# render and names the key.
scalar_username_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.inferenceReplica.controllerIdentity.usernames=system:serviceaccount:ome:custom \
  --show-only templates/ome-controller/configmap.yaml)"
scalar_username_identity="$(inference_replica_identity "${scalar_username_config}")"
[[ "${scalar_username_identity}" == '{"controllerIdentity":{"usernames":["system:serviceaccount:ome:ome-controller-manager","system:serviceaccount:ome:custom"],"groups":[]}}' ]] ||
  fail "a scalar username did not render as a list beside the controller ServiceAccount: ${scalar_username_identity}"

scalar_group_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.inferenceReplica.controllerIdentity.groups=group-a \
  --show-only templates/ome-controller/configmap.yaml)"
scalar_group_identity="$(inference_replica_identity "${scalar_group_config}")"
[[ "${scalar_group_identity}" == '{"controllerIdentity":{"usernames":["system:serviceaccount:ome:ome-controller-manager"],"groups":["group-a"]}}' ]] ||
  fail "a scalar group did not render as a one-element list: ${scalar_group_identity}"

# render_fails_with <message> <helm arguments...>: rendering the controller
# ConfigMap with the arguments fails and reports <message>.
render_fails_with() {
  local message="$1" output
  shift
  if output="$("${helm_bin}" template ome-resources "${chart_dir}" \
    --namespace ome \
    --show-only templates/ome-controller/configmap.yaml "$@" 2>&1)"; then
    fail "rendering with $* succeeded, expected: ${message}"
  fi
  grep -Fq -- "${message}" <<<"${output}" ||
    fail "rendering with $* did not report \"${message}\": ${output}"
}
for key in usernames groups; do
  identity_key="ome.controller.inferenceReplica.controllerIdentity.${key}"
  empty_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
    --namespace ome \
    --set "${identity_key}=" \
    --show-only templates/ome-controller/configmap.yaml)"
  [[ "$(inference_replica_identity "${empty_config}")" == "${default_identity}" ]] ||
    fail "an empty scalar controllerIdentity.${key} did not render as an empty list"

  render_fails_with "${identity_key} must be a list of names or a single name" \
    --set "${identity_key}=123"
  render_fails_with "${identity_key} must be a non-blank name without surrounding whitespace" \
    --set-json "${identity_key}=\" name-a\""
  render_fails_with "${identity_key}[1] must be a name, got " \
    --set-json "${identity_key}=[\"name-a\",123]"
  render_fails_with "${identity_key}[1] must be a non-blank name without surrounding whitespace" \
    --set-json "${identity_key}=[\"name-a\",\" name-b\"]"
  render_fails_with "${identity_key}[1] must be a non-blank name without surrounding whitespace" \
    --set-json "${identity_key}=[\"name-a\",\"\"]"
done

# A null override removes the key; a template that reads values through the
# removed key fails to render, and one that renders must still list the
# controller ServiceAccount.
for cleared in ome.controller.inferenceReplica ome.controller.inferenceReplica.controllerIdentity; do
  cleared_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
    --namespace ome \
    --set "${cleared}=null" \
    --show-only templates/ome-controller/configmap.yaml)"
  [[ "$(inference_replica_identity "${cleared_config}")" == "${default_identity}" ]] ||
    fail "clearing ${cleared} did not keep the controller ServiceAccount"
done

# validating_webhook <rendered> <name>: the ValidatingWebhookConfiguration
# called <name>, empty when it is not rendered.
validating_webhook() {
  awk -v name="$2" '
    function flush() {
      if (kind && named) printf "%s", doc
      doc = ""
      kind = named = 0
    }
    /^---/ { flush(); next }
    { doc = doc $0 "\n" }
    /^kind: ValidatingWebhookConfiguration$/ { kind = 1 }
    $0 == "  name: " name { named = 1 }
    END { flush() }
  ' <<<"$1"
}
webhook_paths() {
  awk '$1 == "path:" { print $2 }' <<<"$1"
}
webhook_ca_source() {
  awk '$1 == "cert-manager.io/inject-ca-from:" { print $2 }' <<<"$1"
}
# The manager serves InferenceReplica admission on every multicluster role, so
# the webhook configuration renders on every role too.
inference_replica_webhook_path='/validate-ome-io-v1beta1-inferencereplica'
[[ "$(webhook_paths "$(validating_webhook "${rendered}" inferencereplica.ome.io)")" == "${inference_replica_webhook_path}" ]] ||
  fail "ValidatingWebhookConfiguration inferencereplica.ome.io was not rendered with path ${inference_replica_webhook_path}"
control_plane_rendered="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.multicluster.role=control-plane)"
[[ "$(webhook_paths "$(validating_webhook "${control_plane_rendered}" inferencereplica.ome.io)")" == "${inference_replica_webhook_path}" ]] ||
  fail "ValidatingWebhookConfiguration inferencereplica.ome.io was not rendered under the control-plane role"
# The InferenceService validator renders by default and is skipped under the
# control-plane role, which shows the role override reached the render.
[[ -n "$(webhook_paths "$(validating_webhook "${rendered}" inferenceservice.ome.io)")" ]] ||
  fail "ValidatingWebhookConfiguration inferenceservice.ome.io was not rendered by default"
[[ -z "$(webhook_paths "$(validating_webhook "${control_plane_rendered}" inferenceservice.ome.io)")" ]] ||
  fail "ValidatingWebhookConfiguration inferenceservice.ome.io was rendered under the control-plane role"

# cert-manager injects the CA bundle from the Certificate in the release
# namespace. Naming any other namespace leaves the bundle empty, and the
# fail-closed webhook then rejects every write.
for namespace in ome team-a; do
  namespace_webhook="$(validating_webhook "$("${helm_bin}" template ome-resources "${chart_dir}" \
    --namespace "${namespace}" \
    --show-only templates/ome-controller/webhooks/inferencereplicavalidator.yaml)" inferencereplica.ome.io)"
  [[ "$(webhook_ca_source "${namespace_webhook}")" == "${namespace}/serving-cert" ]] ||
    fail "ValidatingWebhookConfiguration inferencereplica.ome.io in release namespace ${namespace} does not inject the CA from ${namespace}/serving-cert"
done

min_ready_zero="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.minReadySeconds=0 \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"minReadySeconds": 0' <<<"${min_ready_zero}" ||
  fail "explicit zero deploy.minReadySeconds was not rendered"

min_ready_overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.minReadySeconds=30 \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"minReadySeconds": 30' <<<"${min_ready_overridden}" ||
  fail "deploy.minReadySeconds override was not rendered"

scale_up_batch_overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleUpPodBatchSize=37 \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"scaleUpPodBatchSize":37' <<<"${scale_up_batch_overridden}" ||
  fail "OMENative scale-up Pod batch size override was not rendered"
grep -Fq '"scaleDownPodBatchSize":100' <<<"${scale_up_batch_overridden}" ||
  fail "scale-up override unexpectedly changed the scale-down Pod batch size"

scale_up_batch_zero="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleUpPodBatchSize=0 \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"scaleUpPodBatchSize":0' <<<"${scale_up_batch_zero}" ||
  fail "explicit zero OMENative scale-up Pod batch size was not rendered"

scale_down_batch_overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleDownPodBatchSize=41 \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"scaleDownPodBatchSize":41' <<<"${scale_down_batch_overridden}" ||
  fail "OMENative scale-down Pod batch size override was not rendered"
grep -Fq '"scaleUpPodBatchSize":100' <<<"${scale_down_batch_overridden}" ||
  fail "scale-down override unexpectedly changed the scale-up Pod batch size"

scale_down_batch_zero="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleDownPodBatchSize=0 \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"scaleDownPodBatchSize":0' <<<"${scale_down_batch_zero}" ||
  fail "explicit zero OMENative scale-down Pod batch size was not rendered"

scale_down_percentage="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-string ome.controller.lifecycle.scaleDownPodBatchSize=10% \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"scaleDownPodBatchSize":"10%"' <<<"${scale_down_percentage}" ||
  fail "OMENative scale-down percentage was not rendered as a JSON string"
grep -Fq '"scaleUpPodBatchSize":100' <<<"${scale_down_percentage}" ||
  fail "scale-down percentage unexpectedly changed the scale-up Pod batch size"

scale_down_batch_omitted="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleDownPodBatchSize=null \
  --show-only templates/ome-controller/configmap.yaml)"
if grep -Fq 'scaleDownPodBatchSize' <<<"${scale_down_batch_omitted}"; then
  fail "OMENative scale-down Pod batch size was rendered when omitted"
fi
grep -Fq '"scaleUpPodBatchSize":100' <<<"${scale_down_batch_omitted}" ||
  fail "omitting scale-down unexpectedly removed the scale-up Pod batch size"

scale_down_interval_overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-string ome.controller.lifecycle.scaleDownRequeueInterval=37s \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"scaleDownRequeueInterval":"37s"' <<<"${scale_down_interval_overridden}" ||
  fail "OMENative scale-down requeue interval override was not rendered"
grep -Fq '"scaleDownPodBatchSize":100' <<<"${scale_down_interval_overridden}" ||
  fail "requeue interval override unexpectedly changed the scale-down Pod batch size"

scale_down_interval_zero="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-string ome.controller.lifecycle.scaleDownRequeueInterval=0s \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"scaleDownRequeueInterval":"0s"' <<<"${scale_down_interval_zero}" ||
  fail "explicit zero OMENative scale-down requeue interval was not rendered"

scale_down_interval_omitted="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleDownRequeueInterval=null \
  --show-only templates/ome-controller/configmap.yaml)"
if grep -Fq 'scaleDownRequeueInterval' <<<"${scale_down_interval_omitted}"; then
  fail "OMENative scale-down requeue interval was rendered when omitted"
fi
grep -Fq '"scaleDownPodBatchSize":100' <<<"${scale_down_interval_omitted}" ||
  fail "omitting the requeue interval unexpectedly removed the scale-down Pod batch size"

repair_batch_overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.repairBatchSize=7 \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"repairBatchSize":7' <<<"${repair_batch_overridden}" ||
  fail "OMENative repair batch size override was not rendered"
grep -Fq '"scaleDownPodBatchSize":100' <<<"${repair_batch_overridden}" ||
  fail "repair batch size override unexpectedly changed the scale-down Pod batch size"
grep -Fq '"repairBatchSize":10' <<<"${scale_down_batch_overridden}" ||
  fail "default OMENative repair batch size was not rendered"

repair_batch_omitted="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.repairBatchSize=null \
  --show-only templates/ome-controller/configmap.yaml)"
if grep -Fq 'repairBatchSize' <<<"${repair_batch_omitted}"; then
  fail "OMENative repair batch size was rendered when omitted"
fi
grep -Fq '"scaleDownPodBatchSize":100' <<<"${repair_batch_omitted}" ||
  fail "omitting the repair batch size unexpectedly removed the scale-down Pod batch size"

default_controller_checksum="$(grep -m1 'checksum/config:' <<<"${controller}" | awk '{print $2}')"
scale_down_controller="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.lifecycle.scaleDownPodBatchSize=41 \
  --show-only templates/ome-controller/deployment.yaml)"
scale_down_controller_checksum="$(grep -m1 'checksum/config:' <<<"${scale_down_controller}" | awk '{print $2}')"
[[ -n "${default_controller_checksum}" && -n "${scale_down_controller_checksum}" ]] ||
  fail "controller ConfigMap checksum was not rendered"
[[ "${default_controller_checksum}" != "${scale_down_controller_checksum}" ]] ||
  fail "changing scale-down Pod batch size did not roll the controller checksum"

scale_down_interval_controller="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-string ome.controller.lifecycle.scaleDownRequeueInterval=37s \
  --show-only templates/ome-controller/deployment.yaml)"
scale_down_interval_checksum="$(grep -m1 'checksum/config:' <<<"${scale_down_interval_controller}" | awk '{print $2}')"
[[ -n "${scale_down_interval_checksum}" ]] ||
  fail "scale-down requeue interval controller checksum was not rendered"
[[ "${default_controller_checksum}" != "${scale_down_interval_checksum}" ]] ||
  fail "changing scale-down requeue interval did not roll the controller checksum"

grep -Fq '"burstSize":400' <<<"${controller_config}" ||
  fail "event recorder burst size default was not rendered"
grep -Fq '"refillInterval":"5s"' <<<"${controller_config}" ||
  fail "event recorder refill interval default was not rendered"

event_burst_overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.eventRecorder.burstSize=37 \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"burstSize":37' <<<"${event_burst_overridden}" ||
  fail "event recorder burst size override was not rendered"
grep -Fq '"refillInterval":"5s"' <<<"${event_burst_overridden}" ||
  fail "burst size override unexpectedly changed the refill interval"

event_refill_overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-string ome.controller.eventRecorder.refillInterval=37s \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"refillInterval":"37s"' <<<"${event_refill_overridden}" ||
  fail "event recorder refill interval override was not rendered"
grep -Fq '"burstSize":400' <<<"${event_refill_overridden}" ||
  fail "refill interval override unexpectedly changed the burst size"

event_burst_omitted="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.eventRecorder.burstSize=null \
  --show-only templates/ome-controller/configmap.yaml)"
if grep -Fq 'burstSize' <<<"${event_burst_omitted}"; then
  fail "event recorder burst size was rendered when omitted"
fi
grep -Fq '"refillInterval":"5s"' <<<"${event_burst_omitted}" ||
  fail "omitting the burst size unexpectedly removed the refill interval"

event_recorder_omitted="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.eventRecorder=null \
  --show-only templates/ome-controller/configmap.yaml)"
if grep -Fq 'eventRecorder' <<<"${event_recorder_omitted}"; then
  fail "event recorder block was rendered when omitted"
fi

event_burst_controller="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.eventRecorder.burstSize=37 \
  --show-only templates/ome-controller/deployment.yaml)"
event_burst_checksum="$(grep -m1 'checksum/config:' <<<"${event_burst_controller}" | awk '{print $2}')"
[[ -n "${event_burst_checksum}" ]] ||
  fail "event recorder controller checksum was not rendered"
[[ "${default_controller_checksum}" != "${event_burst_checksum}" ]] ||
  fail "changing the event recorder burst size did not roll the controller checksum"

model_agent="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set modelAgent.enabled=true \
  --show-only templates/model-agent-daemonset/daemonset.yaml)"
grep -Fq 'helm.sh/chart: ome-resources-0.1.0' <<<"${model_agent}" ||
  fail "model agent chart version label was not rendered"

prometheus_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --show-only templates/prometheus/configmap.yaml)"
prometheus_deployment="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --show-only templates/prometheus/deployment.yaml)"

grep -Fq 'scrape_timeout: 10s' <<<"${prometheus_config}" ||
  fail "default Prometheus scrape timeout was not rendered"
grep -Fq 'cluster: ome' <<<"${prometheus_config}" ||
  fail "default Prometheus external label was not rendered"
if grep -Fq 'sample_limit:' <<<"${prometheus_config}" ||
  grep -Fq 'target_limit:' <<<"${prometheus_config}" ||
  grep -Fq 'body_size_limit:' <<<"${prometheus_config}"; then
  fail "optional Prometheus scrape limits were rendered by default"
fi
grep -Fq 'regex: "false"' <<<"${prometheus_config}" ||
  fail "default Prometheus scrape annotation compatibility mode was not rendered"
if grep -Fq 'regex: "true;.*|.*;true"' <<<"${prometheus_config}"; then
  fail "Prometheus scrape opt-in was rendered by default"
fi
grep -Fq 'job_name: ome-prometheus' <<<"${prometheus_config}" ||
  fail "Prometheus self-scrape job was not rendered"
grep -Fq '127.0.0.1:9090' <<<"${prometheus_config}" ||
  fail "Prometheus self-scrape target was not rendered"
if grep -Fq -- '--query.' <<<"${prometheus_deployment}"; then
  fail "optional Prometheus query limits were rendered by default"
fi
if grep -Fq 'name: GOMEMLIMIT' <<<"${prometheus_deployment}"; then
  fail "Prometheus GOMEMLIMIT was rendered when unset"
fi
if grep -Fq 'templates/prometheus/pvc.yaml' <<<"${rendered}"; then
  fail "Prometheus PVC was rendered when persistence is disabled"
fi

prometheus_without_self_scrape="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set prometheus.selfScrape.enabled=false \
  --show-only templates/prometheus/configmap.yaml)"
if grep -Fq -- '- job_name: ome-prometheus' <<<"${prometheus_without_self_scrape}"; then
  fail "Prometheus self-scrape job was rendered when disabled"
fi

opt_in_scrape_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set prometheus.requireScrapeAnnotation=true \
  --show-only templates/prometheus/configmap.yaml)"
grep -Fq 'regex: "true;.*|.*;true"' <<<"${opt_in_scrape_config}" ||
  fail "Prometheus scrape annotation opt-in was not rendered"

grep -Fq 'target_label: revision_hash' <<<"${prometheus_config}" ||
  fail "Prometheus revision_hash relabel was not rendered"

# Pod addresses are IPv4 or bracketed IPv6 literals. A host pattern that
# excludes colons never rewrites an IPv6 address to the annotated port, so
# every declared container port stays a scrape target. Both pod jobs carry
# the rewrite.
[[ "$(grep -Fc 'regex: (.+?)(?::\d+)?;(\d+)' <<<"${prometheus_config}")" -eq 2 ]] ||
  fail "Prometheus annotated-port relabel does not accept IPv6 addresses in both pod jobs"
if grep -Fq 'regex: ([^:]+)(?::\d+)?;(\d+)' <<<"${prometheus_config}"; then
  fail "Prometheus annotated-port relabel excludes IPv6 addresses"
fi

if grep -Fq -- '- job_name: extra-probe' <<<"${prometheus_config}"; then
  fail "extra scrape configs were rendered when unset"
fi

extra_scrape_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set 'prometheus.extraScrapeConfigs[0].job_name=extra-probe' \
  --set 'prometheus.extraScrapeConfigs[0].static_configs[0].targets[0]=127.0.0.1:1234' \
  --show-only templates/prometheus/configmap.yaml)"
grep -Fq -- '- job_name: extra-probe' <<<"${extra_scrape_config}" ||
  fail "extra scrape config job was not rendered"
grep -Fq 'job_name: ome-inferenceservice-pods' <<<"${extra_scrape_config}" ||
  fail "generated scrape jobs were lost when extraScrapeConfigs was set"
grep -Fq '__meta_kubernetes_pod_annotation_ome_io_enable_prometheus_scraping' <<<"${opt_in_scrape_config}" ||
  fail "OME Prometheus scrape opt-in annotation was not rendered"

metric_relabel_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set 'prometheus.metricRelabelConfigs[0].source_labels[0]=__name__' \
  --set-string 'prometheus.metricRelabelConfigs[0].regex=up|cd_probe_up' \
  --set 'prometheus.metricRelabelConfigs[0].action=keep' \
  --show-only templates/prometheus/configmap.yaml)"
grep -Fq 'metric_relabel_configs:' <<<"${metric_relabel_config}" ||
  fail "Prometheus metric relabel configuration was not rendered"
grep -Fq 'regex: up|cd_probe_up' <<<"${metric_relabel_config}" ||
  fail "Prometheus metric relabel regex override was not rendered"
default_prometheus_checksum="$(grep -m1 'checksum/config:' <<<"${prometheus_deployment}" | awk '{print $2}')"
metric_relabel_deployment="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set 'prometheus.metricRelabelConfigs[0].source_labels[0]=__name__' \
  --set-string 'prometheus.metricRelabelConfigs[0].regex=up|cd_probe_up' \
  --set 'prometheus.metricRelabelConfigs[0].action=keep' \
  --show-only templates/prometheus/deployment.yaml)"
metric_relabel_checksum="$(grep -m1 'checksum/config:' <<<"${metric_relabel_deployment}" | awk '{print $2}')"
[[ -n "${default_prometheus_checksum}" && -n "${metric_relabel_checksum}" ]] ||
  fail "Prometheus ConfigMap checksum was not rendered"
[[ "${default_prometheus_checksum}" != "${metric_relabel_checksum}" ]] ||
  fail "changing Prometheus metric relabeling did not roll the config checksum"

prometheus_runtime_overrides="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set prometheus.listenPort=9191 \
  --set prometheus.service.port=80 \
  --set-string prometheus.goMemLimit=9GiB \
  --show-only templates/prometheus/deployment.yaml)"
prometheus_port_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set prometheus.listenPort=9191 \
  --set prometheus.service.port=80 \
  --show-only templates/prometheus/configmap.yaml)"
prometheus_port_service="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set prometheus.listenPort=9191 \
  --set prometheus.service.port=80 \
  --show-only templates/prometheus/service.yaml)"
prometheus_port_controller_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set prometheus.listenPort=9191 \
  --set prometheus.service.port=80 \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq -- '--web.listen-address=:9191' <<<"${prometheus_runtime_overrides}" ||
  fail "Prometheus listen port override was not rendered"
grep -Fq 'containerPort: 9191' <<<"${prometheus_runtime_overrides}" ||
  fail "Prometheus container port override was not rendered"
grep -Fq '127.0.0.1:9191' <<<"${prometheus_port_config}" ||
  fail "Prometheus self-scrape did not use the listen port"
grep -Fq 'port: 80' <<<"${prometheus_port_service}" ||
  fail "Prometheus Service port override was not rendered"
grep -Fq 'http://ome-prometheus.ome.svc:80' <<<"${prometheus_port_controller_config}" ||
  fail "canary Prometheus address did not use the Service port"
grep -Fq 'name: GOMEMLIMIT' <<<"${prometheus_runtime_overrides}" ||
  fail "Prometheus GOMEMLIMIT name was not rendered"
grep -Fq 'value: "9GiB"' <<<"${prometheus_runtime_overrides}" ||
  fail "Prometheus GOMEMLIMIT value was not rendered"

prometheus_persistent="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set prometheus.persistence.enabled=true \
  --set prometheus.persistence.size=32Gi \
  --set prometheus.persistence.storageClassName=fast-rwo)"
prometheus_persistent_deployment="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set prometheus.persistence.enabled=true \
  --show-only templates/prometheus/deployment.yaml)"
grep -Fq 'templates/prometheus/pvc.yaml' <<<"${prometheus_persistent}" ||
  fail "Prometheus PVC was not rendered when persistence is enabled"
grep -Fq 'claimName: "ome-prometheus"' <<<"${prometheus_persistent_deployment}" ||
  fail "Prometheus chart-managed PVC was not mounted"
grep -Fq 'storage: 32Gi' <<<"${prometheus_persistent}" ||
  fail "Prometheus chart-managed PVC size was not rendered"
grep -Fq 'storageClassName: "fast-rwo"' <<<"${prometheus_persistent}" ||
  fail "Prometheus chart-managed PVC storage class was not rendered"
grep -Fq -- '- ReadWriteOnce' <<<"${prometheus_persistent}" ||
  fail "Prometheus chart-managed PVC access mode was not rendered"
if grep -Fq 'emptyDir:' <<<"${prometheus_persistent_deployment}"; then
  fail "Prometheus emptyDir was rendered with persistence enabled"
fi

prometheus_existing_claim="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set prometheus.persistence.enabled=true \
  --set prometheus.persistence.existingClaim=shared-prometheus)"
prometheus_existing_claim_deployment="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set prometheus.persistence.enabled=true \
  --set prometheus.persistence.existingClaim=shared-prometheus \
  --show-only templates/prometheus/deployment.yaml)"
grep -Fq 'claimName: "shared-prometheus"' <<<"${prometheus_existing_claim_deployment}" ||
  fail "Prometheus existing PVC was not mounted"
if grep -Fq 'templates/prometheus/pvc.yaml' <<<"${prometheus_existing_claim}"; then
  fail "Prometheus PVC was created when an existing claim was configured"
fi

overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set prometheus.storage.sizeLimit=16Gi)"
grep -Fq 'sizeLimit: 16Gi' <<<"${overridden}" ||
  fail "Prometheus storage sizeLimit override was not rendered"

without_size_retention="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-string prometheus.retentionSize=)"
if grep -Fq -- '--storage.tsdb.retention.size=' <<<"${without_size_retention}"; then
  fail "Prometheus size retention was rendered when disabled"
fi

# The fleet-quota controller lives under pkg/controller/v1beta1/acceleratorquota,
# inside the tree controller-gen scans, but it runs in ome-quota-manager with its
# own ServiceAccount. A +kubebuilder:rbac marker added there would regenerate
# role.yaml, pass the manifests-drift gate once committed, and silently hand
# ome-manager the quota plane's permissions — eventually cluster-wide write on
# Kueue's ClusterQueue and Cohort, on the ServiceAccount that also runs the
# fail-closed pod mutating webhook.
if grep -Fq 'acceleratorquotas' <<<"${rendered}"; then
  fail "quota RBAC leaked into the ome-manager role; it belongs to charts/ome-quota-manager"
fi

# The control plane selects a placement winner from the per-component
# InferenceReplica status it reads on each workload cluster, so the
# multicluster-access ClusterRole must grant it. Without the grant every read is
# forbidden, fan-out still succeeds, and placement never leaves Admitting — a
# failure mode no lane in the test matrix reproduces, because they all
# authenticate as cluster-admin rather than exercising this role.
multicluster_access="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.multiclusterAccess.enabled=true \
  --show-only templates/ome-controller/rbac/multicluster_access.yaml)"
grep -Fqx '  - inferencereplicas' <<<"${multicluster_access}" ||
  fail "multicluster-access ClusterRole does not grant inferencereplicas"
# Direct gateway publication follows the generated route to its parent Gateway;
# the workload-cluster credential must be able to read both objects.
grep -Fqx '  - gateways' <<<"${multicluster_access}" ||
  fail "multicluster-access ClusterRole does not grant gateways"
grep -Fqx '  - httproutes' <<<"${multicluster_access}" ||
  fail "multicluster-access ClusterRole does not grant httproutes"

# Placement input reads cannot grant writes to member-owned configuration.
for resource in clusterservingruntimes servingruntimes clusterbasemodels basemodels controllerrevisions configmaps acceleratorquotas resourceflavors runtimeclasses; do
  input_rule="$(awk -v resource="${resource}" '
    /^- apiGroups:/ { if (matches) { printf "%s", rule; matches=0; exit }; rule=""; matches=0 }
    { rule=rule $0 "\n" }
    $0 == "  - " resource { matches=1 }
    END { if (matches) printf "%s", rule }
  ' <<<"${multicluster_access}")"
  test -n "${input_rule}" || fail "multicluster-access lacks ${resource} input reads"
  if grep -Eq '^  - (create|update|patch|delete|deletecollection|\*)$' <<<"${input_rule}"; then
    fail "multicluster-access permits mutation of ${resource}"
  fi
  input_verb=get
  if [[ "${resource}" == resourceflavors ]]; then input_verb=list; fi
  grep -Fqx "  - ${input_verb}" <<<"${input_rule}" || fail "multicluster-access lacks ${resource} ${input_verb}"
done
grep -Fqx '  - inferenceservice-config' <<<"${multicluster_access}" ||
  fail "member configuration reads must name the operator ConfigMap"

capacity_reader="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace placement-system \
  --set ome.multicluster.enabled=true \
  --set ome.multicluster.role=control-plane \
  --set ome.multicluster.config.placement.capacity.rootName=capacity-root \
  --show-only templates/ome-controller/rbac/placement_capacity.yaml)"
for verb in get list watch; do
  grep -Fqx "  - ${verb}" <<<"${capacity_reader}" || fail "capacity reader lacks ${verb}"
done
if grep -Eq '^  - (create|update|patch|delete|deletecollection|\*)$' <<<"${capacity_reader}"; then
  fail "capacity reader must not write quota objects"
fi
grep -Fqx '  - acceleratorquotas' <<<"${capacity_reader}" || fail "capacity reader lacks quota reports"
grep -Fqx '  namespace: placement-system' <<<"${capacity_reader}" || fail "capacity reader binds the wrong namespace"
for role in control-plane workload; do
  without_capacity="$("${helm_bin}" template ome-resources "${chart_dir}" \
    --namespace ome --set ome.multicluster.enabled=true --set "ome.multicluster.role=${role}")"
  if grep -Fq 'ome-placement-capacity-reader' <<<"${without_capacity}"; then
    fail "${role} acquired capacity reads without capacity configuration"
  fi
done

# The manager reads both from multicluster.placement, so a value that is not
# rendered there leaves capacity placement pending however the chart is set.
placement_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.multicluster.config.placement.memberOperatorNamespace=operator-system \
  --set ome.multicluster.config.placement.capacity.rootName=capacity-root \
  --set ome.multicluster.config.placement.capacity.maxAge=5m \
  --set ome.multicluster.config.placement.capacity.stabilityWindow=1m \
  --set ome.multicluster.config.placement.capacity.refreshInterval=10s \
  --show-only templates/ome-controller/configmap.yaml | tr -d '[:space:]')"
grep -Fq '"memberOperatorNamespace":"operator-system"' <<<"${placement_config}" ||
  fail "placement.memberOperatorNamespace is not rendered into the manager configuration"
grep -Fq '"capacity":{"rootName":"capacity-root","maxAge":"5m","stabilityWindow":"1m","refreshInterval":"10s"}' <<<"${placement_config}" ||
  fail "placement.capacity is not rendered into the manager configuration"
if tr -d '[:space:]' <<<"${controller_config}" | grep -Eq '"memberOperatorNamespace"|"capacity":\{"rootName"'; then
  fail "default placement configuration names a member namespace or a capacity block"
fi

routing_topology_error='ome.multicluster.config.routing.enabled=true requires ome.multicluster.enabled=true and ome.multicluster.role=control-plane'

if routing_without_multicluster="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.multicluster.config.routing.enabled=true \
  --set-string ome.multicluster.role=control-plane 2>&1)"; then
  fail "routing was rendered without multi-cluster enabled"
fi
grep -Fq "${routing_topology_error}" <<<"${routing_without_multicluster}" ||
  fail "routing without multi-cluster enabled did not report the topology requirement"

if routing_without_control_plane_role="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.multicluster.config.routing.enabled=true \
  --set ome.multicluster.enabled=true 2>&1)"; then
  fail "routing was rendered without the control-plane role"
fi
grep -Fq "${routing_topology_error}" <<<"${routing_without_control_plane_role}" ||
  fail "routing without the control-plane role did not report the topology requirement"

if routing_with_wrong_role="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.multicluster.config.routing.enabled=true \
  --set ome.multicluster.enabled=true \
  --set-string ome.multicluster.role=worker 2>&1)"; then
  fail "routing was rendered with a non-control-plane role"
fi
grep -Fq "${routing_topology_error}" <<<"${routing_with_wrong_role}" ||
  fail "routing with a non-control-plane role did not report the topology requirement"

routing_control_plane="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.multicluster.config.routing.enabled=true \
  --set ome.multicluster.enabled=true \
  --set-string ome.multicluster.role=control-plane)"
grep -Fq $'"routing": {\n        "enabled": true' <<<"${routing_control_plane}" ||
  fail "control-plane routing was not enabled in the controller ConfigMap"
grep -Fq -- '--enable-multicluster' <<<"${routing_control_plane}" ||
  fail "control-plane routing did not enable multi-cluster mode"
grep -Fq -- '--multicluster-role=control-plane' <<<"${routing_control_plane}" ||
  fail "control-plane routing did not set the control-plane role"

gateway_backend_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.multicluster.config.endpoint.gatewayBackend.rewriteHostname=true \
  --set ome.multicluster.config.endpoint.gatewayBackend.tls.enabled=true \
  --set-string ome.multicluster.config.endpoint.gatewayBackend.tls.wellKnownCACertificates=System \
  --set ome.multicluster.config.endpoint.gatewayBackend.endpointSlices.enabled=true \
  --set-string ome.multicluster.config.endpoint.gatewayBackend.endpointSlices.addressRefreshInterval=1m \
  --show-only templates/ome-controller/configmap.yaml)"
for expected in \
  '"rewriteHostname": true' \
  '"wellKnownCACertificates": "System"' \
  '"addressRefreshInterval": "1m"'; do
  grep -Fq "${expected}" <<<"${gateway_backend_config}" ||
    fail "gateway backend setting was not rendered: ${expected}"
done

for expected in \
  '"maxConcurrentReconciles": 8' \
  '"maxConcurrentRequests": 16' \
  '"maxResponseBytes": 65536' \
  '"minPeriod": "1s"' \
  '"maxSamples": 100'; do
  grep -Fq "${expected}" <<<"${controller_config}" ||
    fail "default routing observer setting was not rendered: ${expected}"
done

routing_observer_args=(
  --set ome.multicluster.config.routing.observer.maxConcurrentReconciles=4
  --set ome.multicluster.config.routing.observer.maxConcurrentRequests=12
  --set ome.multicluster.config.routing.observer.maxResponseBytes=131072
  --set-string ome.multicluster.config.routing.observer.minPeriod=2s
  --set ome.multicluster.config.routing.observer.maxSamples=64
)
routing_observer_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  "${routing_observer_args[@]}" \
  --show-only templates/ome-controller/configmap.yaml)"
for expected in \
  '"maxConcurrentReconciles": 4' \
  '"maxConcurrentRequests": 12' \
  '"maxResponseBytes": 131072' \
  '"minPeriod": "2s"' \
  '"maxSamples": 64'; do
  grep -Fq "${expected}" <<<"${routing_observer_config}" ||
    fail "routing observer override was not rendered: ${expected}"
done
routing_observer_controller="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  "${routing_observer_args[@]}" \
  --show-only templates/ome-controller/deployment.yaml)"
routing_observer_checksum="$(grep -m1 'checksum/config:' <<<"${routing_observer_controller}" | awk '{print $2}')"
[[ -n "${routing_observer_checksum}" ]] ||
  fail "routing observer controller checksum was not rendered"
[[ "${default_controller_checksum}" != "${routing_observer_checksum}" ]] ||
  fail "changing routing observer limits did not roll the controller checksum"

if grep -Fq '"probe"' <<<"${controller_config}"; then
  fail "routing probe was rendered when disabled"
fi
if grep -Fq '"capacity": {' <<<"${controller_config}"; then
  fail "routing capacity was rendered when disabled"
fi

routing_probe_args=(
  --set-string ome.multicluster.config.routing.probe.path=/v1/models
  --set-string ome.multicluster.config.routing.probe.method=GET
  --set 'ome.multicluster.config.routing.probe.acceptStatuses={200}'
  --set 'ome.multicluster.config.routing.probe.gateStatuses={404,500,502,503,504}'
  --set-string ome.multicluster.config.routing.probe.period=10s
  --set-string ome.multicluster.config.routing.probe.timeout=3s
  --set ome.multicluster.config.routing.probe.failureThreshold=3
  --set ome.multicluster.config.routing.probe.successThreshold=2
  --set-string ome.multicluster.config.routing.probe.allFailedPolicy=Drain
)
routing_probe_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  "${routing_probe_args[@]}" \
  --show-only templates/ome-controller/configmap.yaml)"
for expected in \
  '"path": "/v1/models"' \
  '"method": "GET"' \
  '"acceptStatuses": [200]' \
  '"gateStatuses": [404,500,502,503,504]' \
  '"period": "10s"' \
  '"timeout": "3s"' \
  '"failureThreshold": 3' \
  '"successThreshold": 2' \
  '"allFailedPolicy": "Drain"'; do
  grep -Fq "${expected}" <<<"${routing_probe_config}" ||
    fail "routing probe setting was not rendered: ${expected}"
done

routing_probe_controller="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  "${routing_probe_args[@]}" \
  --show-only templates/ome-controller/deployment.yaml)"
routing_probe_checksum="$(grep -m1 'checksum/config:' <<<"${routing_probe_controller}" | awk '{print $2}')"
[[ -n "${routing_probe_checksum}" ]] ||
  fail "routing probe controller checksum was not rendered"
[[ "${default_controller_checksum}" != "${routing_probe_checksum}" ]] ||
  fail "enabling the routing probe did not roll the controller checksum"

routing_capacity_args=(
  --set-string ome.multicluster.config.routing.capacity.path=/capacity
  --set-string ome.multicluster.config.routing.capacity.method=GET
  --set-string ome.multicluster.config.routing.capacity.format=Report
  --set ome.multicluster.config.routing.capacity.samples=20
  --set ome.multicluster.config.routing.capacity.quorum=3
  --set-string ome.multicluster.config.routing.capacity.period=5s
  --set-string ome.multicluster.config.routing.capacity.timeout=2s
  --set-string ome.multicluster.config.routing.capacity.maxAge=30s
)
routing_capacity_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  "${routing_capacity_args[@]}" \
  --show-only templates/ome-controller/configmap.yaml)"
for expected in \
  '"path": "/capacity"' \
  '"method": "GET"' \
  '"format": "Report"' \
  '"options": {}' \
  '"samples": 20' \
  '"quorum": 3' \
  '"period": "5s"' \
  '"timeout": "2s"' \
  '"maxAge": "30s"'; do
  grep -Fq "${expected}" <<<"${routing_capacity_config}" ||
    fail "routing capacity setting was not rendered: ${expected}"
done

routing_capacity_controller="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  "${routing_capacity_args[@]}" \
  --show-only templates/ome-controller/deployment.yaml)"
routing_capacity_checksum="$(grep -m1 'checksum/config:' <<<"${routing_capacity_controller}" | awk '{print $2}')"
[[ -n "${routing_capacity_checksum}" ]] ||
  fail "routing capacity controller checksum was not rendered"
[[ "${default_controller_checksum}" != "${routing_capacity_checksum}" ]] ||
  fail "enabling routing capacity did not roll the controller checksum"

for expected in \
  '"publisher": {' \
  '"name": ""' \
  '"resyncInterval": "1m"' \
  '"options": {}'; do
  grep -Fq "${expected}" <<<"${controller_config}" ||
    fail "default TrafficMap publisher setting was not rendered: ${expected}"
done
assert_default_trafficmap_publisher "${controller_config}" "default values"

legacy_chart_fixtures="$(mktemp -d "${TMPDIR:-/tmp}/ome-resources-legacy-publisher.XXXXXX")"
trap 'rm -rf -- "${legacy_chart_fixtures}"' EXIT

legacy_publisher_list_config="$(render_legacy_publisher_default '[]' list)"
assert_default_trafficmap_publisher "${legacy_publisher_list_config}" "legacy publisher list"

legacy_publisher_null_config="$(render_legacy_publisher_default 'null' null)"
assert_default_trafficmap_publisher "${legacy_publisher_null_config}" "null publisher"

routing_publisher_args=(
  --set-string ome.multicluster.config.routing.publisher.name=test-publisher
  --set-string ome.multicluster.config.routing.publisher.resyncInterval=30s
  --set-json 'ome.multicluster.config.routing.publisher.options={"key":"value"}'
)
routing_publisher_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  "${routing_publisher_args[@]}" \
  --show-only templates/ome-controller/configmap.yaml)"
for expected in \
  '"name": "test-publisher"' \
  '"resyncInterval": "30s"' \
  '"key":"value"'; do
  grep -Fq "${expected}" <<<"${routing_publisher_config}" ||
    fail "TrafficMap publisher setting was not rendered: ${expected}"
done
routing_publisher_controller="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  "${routing_publisher_args[@]}" \
  --show-only templates/ome-controller/deployment.yaml)"
routing_publisher_checksum="$(grep -m1 'checksum/config:' <<<"${routing_publisher_controller}" | awk '{print $2}')"
[[ -n "${routing_publisher_checksum}" ]] ||
  fail "TrafficMap publisher controller checksum was not rendered"
[[ "${default_controller_checksum}" != "${routing_publisher_checksum}" ]] ||
  fail "configuring a TrafficMap publisher did not roll the controller checksum"

# acceleratorResources has no in-code default: the chart default is the only
# source of any non-nvidia recognition. It stays nvidia-only so upgrading the
# chart cannot change which pods get a PARALLELISM_SIZE env var — recognizing
# a resource an existing multi-node Component already requests would change
# that Component's hashed OMENative revision payload and roll it (see
# controllerconfig.InferenceServicesConfig.AcceleratorResourceNames).
grep -Fq '["nvidia.com/gpu"]' <<<"${controller_config}" ||
  fail "default accelerator resources list was not rendered"
if grep -Fq -e 'amd.com/gpu' -e 'google.com/tpu' <<<"${controller_config}"; then
  fail "chart default recognized a non-nvidia accelerator without an explicit override"
fi

accelerator_resources_overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-json 'ome.controller.acceleratorResources=["nvidia.com/gpu","amd.com/gpu","google.com/tpu"]' \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '["nvidia.com/gpu","amd.com/gpu","google.com/tpu"]' <<<"${accelerator_resources_overridden}" ||
  fail "accelerator resources override was not rendered"

accelerator_resources_cleared="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-json 'ome.controller.acceleratorResources=[]' \
  --show-only templates/ome-controller/configmap.yaml)"
if grep -Fq 'acceleratorResources' <<<"${accelerator_resources_cleared}"; then
  fail "acceleratorResources key was rendered when the list was cleared"
fi

# quotaAcceleratorResources decides which workloads carry the Kueue queue-name
# label. Deliberately a different value from acceleratorResources above and a
# flag rather than a ConfigMap key: this one must be settable without touching a
# pod template, which is exactly what widening the ConfigMap list would do.
grep -Fq -- '--accelerator-resources=nvidia.com/gpu,google.com/tpu' <<<"${controller}" ||
  fail "default quota accelerator resources were not rendered"

quota_accel_overridden="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-json 'ome.controller.quotaAcceleratorResources=["nvidia.com/gpu","google.com/tpu","amd.com/gpu"]' \
  --show-only templates/ome-controller/deployment.yaml)"
grep -Fq -- '--accelerator-resources=nvidia.com/gpu,google.com/tpu,amd.com/gpu' <<<"${quota_accel_overridden}" ||
  fail "quota accelerator resources override was not rendered"

# Cleared means "govern every Component": the flag must be absent, not empty,
# because --accelerator-resources= would read as a list naming nothing.
quota_accel_cleared="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-json 'ome.controller.quotaAcceleratorResources=[]' \
  --show-only templates/ome-controller/deployment.yaml)"
if grep -Fq -- '--accelerator-resources' <<<"${quota_accel_cleared}"; then
  fail "quota accelerator resources flag was rendered when the list was cleared"
fi

# The two lists are independent: widening the admission list must not change the
# ConfigMap that sizes PARALLELISM_SIZE, which is the coupling this split exists
# to avoid.
grep -Fq '["nvidia.com/gpu"]' <<<"$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-json 'ome.controller.quotaAcceleratorResources=["nvidia.com/gpu","google.com/tpu"]' \
  --show-only templates/ome-controller/configmap.yaml)" ||
  fail "quota accelerator resources leaked into the acceleratorResources config key"

# ome.autoscalerPolicy.enabled gates the policy validating webhook and the
# config block. Manager RBAC stays ungated in the generated role (the
# optional-CRD precedent: grants on absent CRDs are inert), so the gate-off
# render checks the webhook, not RBAC rule names.
if grep -Fq 'path: /validate-ome-io-v1beta1-autoscalerpolicy' <<<"${rendered}"; then
  fail "AutoscalerPolicy webhook was rendered with the feature gate off"
fi
if grep -Fq 'autoscalerPolicy:' <<<"${controller_config}"; then
  fail "autoscalerPolicy config block was rendered with the feature gate off"
fi

autoscaler_enabled="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.autoscalerPolicy.enabled=true)"
# The autoscalerpolicies / triggerauthentications RBAC lives ungated in the
# generated manager role (the optional-CRD precedent: grants on absent CRDs
# are inert), so it must render regardless of the feature gate.
grep -Fq -- '- triggerauthentications' <<<"${autoscaler_enabled}" ||
  fail "KEDA TriggerAuthentication RBAC was not rendered"
grep -Fq -- '- autoscalerpolicies/status' <<<"${autoscaler_enabled}" ||
  fail "AutoscalerPolicy status RBAC was not rendered"
grep -Fq 'name: autoscalerpolicy.ome.io' <<<"${autoscaler_enabled}" ||
  fail "AutoscalerPolicy validating webhook was not rendered with the gate on"
grep -Fq 'path: /validate-ome-io-v1beta1-autoscalerpolicy' <<<"${autoscaler_enabled}" ||
  fail "AutoscalerPolicy webhook path was not rendered with the gate on"
# In-use deletion denial happens at admission, so the webhook must both
# register the DELETE operation and pass DELETE through its skip-deletion
# matchCondition (a DELETE review has no `object` to guard on).
grep -Fq -- '- DELETE' <<<"${autoscaler_enabled}" ||
  fail "AutoscalerPolicy webhook does not register the DELETE operation"
grep -Fq "request.operation == 'DELETE'" <<<"${autoscaler_enabled}" ||
  fail "AutoscalerPolicy skip-deletion matchCondition does not pass DELETE through"

autoscaler_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.autoscalerPolicy.enabled=true \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"metricProviders": {}' <<<"${autoscaler_config}" ||
  fail "empty autoscalerPolicy provider map was not rendered with the gate on"
grep -Fq '"memberGetTimeoutSeconds": 0' <<<"${autoscaler_config}" ||
  fail "autoscalerPolicy preflight member GET timeout default was not rendered"
grep -Fq '"skewDeadlineSeconds": 0' <<<"${autoscaler_config}" ||
  fail "autoscalerPolicy preflight skew deadline default was not rendered"

autoscaler_provider_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.autoscalerPolicy.enabled=true \
  --set-string 'ome.autoscalerPolicy.metricProviders.cluster-prometheus.serverAddress=http://ome-prometheus.ome.svc:9090' \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Fq '"cluster-prometheus":{"serverAddress":"http://ome-prometheus.ome.svc:9090"}' <<<"${autoscaler_provider_config}" ||
  fail "autoscalerPolicy metric provider binding was not rendered"
# The nested map is the deprecated alias: it must never also render the
# authoritative top-level key, which would mask the alias in the loader.
if grep -Eq '^  metricProviders:' <<<"${autoscaler_provider_config}"; then
  fail "nested autoscalerPolicy providers leaked into the top-level metricProviders key"
fi

# ome.rolloutPolicy.enabled gates the policy validating webhook. Manager RBAC
# stays ungated in the generated role (the optional-CRD precedent: grants on
# absent CRDs are inert), and the rollout config block stays ungated too —
# inline rollout plans exist on every cluster, policy CRD or not.
if grep -Fq 'path: /validate-ome-io-v1beta1-rolloutpolicy' <<<"${rendered}"; then
  fail "RolloutPolicy webhook was rendered with the feature gate off"
fi

rollout_enabled="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.rolloutPolicy.enabled=true)"
grep -Fq -- '- rolloutpolicies/status' <<<"${rollout_enabled}" ||
  fail "RolloutPolicy status RBAC was not rendered"
grep -Fq 'name: rolloutpolicy.ome.io' <<<"${rollout_enabled}" ||
  fail "RolloutPolicy validating webhook was not rendered with the gate on"
grep -Fq 'path: /validate-ome-io-v1beta1-rolloutpolicy' <<<"${rollout_enabled}" ||
  fail "RolloutPolicy webhook path was not rendered with the gate on"
# In-use deletion denial happens at admission, so the webhook must both
# register the DELETE operation and pass DELETE through its skip-deletion
# matchCondition (a DELETE review has no `object` to guard on).
rollout_webhook="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.rolloutPolicy.enabled=true \
  --show-only templates/ome-controller/webhooks/rolloutpolicyvalidator.yaml)"
grep -Fq -- '- DELETE' <<<"${rollout_webhook}" ||
  fail "RolloutPolicy webhook does not register the DELETE operation"
grep -Fq "request.operation == 'DELETE'" <<<"${rollout_webhook}" ||
  fail "RolloutPolicy skip-deletion matchCondition does not pass DELETE through"

# The rollout block and the canaryAnalysis default provider render ungated,
# and the chart values are the only source of their defaults (the OME binary
# deliberately has none).
grep -Fq '"maxPinnedPlanBytes": 16384' <<<"${controller_config}" ||
  fail "rollout pinned-plan size cap default was not rendered"
grep -Fq '"defaultReadyTimeout": "15m"' <<<"${controller_config}" ||
  fail "rollout default ready timeout was not rendered"
grep -Fq '"defaultProvider": ""' <<<"${controller_config}" ||
  fail "canaryAnalysis default provider was not rendered"
# The loader treats a present top-level metricProviders key — even {} — as
# authoritative, so the chart must omit it entirely when no bindings are set.
if grep -Eq '^  metricProviders:' <<<"${controller_config}"; then
  fail "top-level metricProviders key was rendered with no bindings configured"
fi

metric_providers_config="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set-string 'ome.metricProviders.cluster-prometheus.serverAddress=http://ome-prometheus.ome.svc:9090' \
  --set-string 'ome.metricProviders.cluster-prometheus.headers.X-Scope-OrgID=tenant-a' \
  --show-only templates/ome-controller/configmap.yaml)"
grep -Eq '^  metricProviders:' <<<"${metric_providers_config}" ||
  fail "top-level metricProviders key was not rendered when bindings are set"
grep -Fq '"cluster-prometheus":{"headers":{"X-Scope-OrgID":"tenant-a"},"serverAddress":"http://ome-prometheus.ome.svc:9090"}' <<<"${metric_providers_config}" ||
  fail "top-level metric provider binding was not rendered"

# The traffic reconciler picks its translator at startup by probing CRDs
# (reconcilers/traffic/factory) and then watches the chosen backend policy
# kind. Every kind a translator can watch must be listable and watchable by
# the manager, otherwise the informer never syncs and the manager exits on
# cache-sync timeout on any cluster where that CRD happens to exist.
manager_role="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --show-only templates/ome-controller/rbac/role.yaml)"
# role_grants <apiGroup> <resource>: true when some rule of the rendered
# ClusterRole names both, and its verbs include list and watch.
role_grants() {
  awk -v group="$1" -v resource="$2" '
    /^- apiGroups:/ { if (g && r && l && w) ok = 1; g = r = l = w = 0; section = "apiGroups"; next }
    /^  resources:/ { section = "resources"; next }
    /^  verbs:/ { section = "verbs"; next }
    /^  - / {
      item = substr($0, 5)
      if (section == "apiGroups" && item == group) g = 1
      if (section == "resources" && item == resource) r = 1
      if (section == "verbs" && item == "list") l = 1
      if (section == "verbs" && item == "watch") w = 1
    }
    END { if (g && r && l && w) ok = 1; exit !ok }
  ' <<<"${manager_role}"
}
for translator_resource in networking.istio.io/destinationrules gateway.envoyproxy.io/backendtrafficpolicies; do
  role_grants "${translator_resource%/*}" "${translator_resource#*/}" ||
    fail "manager ClusterRole does not grant list+watch on ${translator_resource}, which a traffic translator watches"
done

# The preset mutator only changes runtimes that set ome.io/engine and passes
# every other runtime through untouched, so the webhook must only be called for
# those. Otherwise, while the service is still backed by a manager that does not
# serve the preset path (an upgrade from v1.2.x), failurePolicy: Fail rejects
# every ServingRuntime/ClusterServingRuntime write, including this chart's own
# default-runtime.
preset_webhook="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --show-only templates/ome-controller/webhooks/runtimepreset.yaml)"
engine_preset_condition="expression: \"has(object.metadata.annotations) && 'ome.io/engine' in object.metadata.annotations && object.metadata.annotations['ome.io/engine'] != ''\""
[ "$(grep -Fc -- "${engine_preset_condition}" <<<"${preset_webhook}")" -eq 2 ] ||
  fail "preset webhooks are not scoped to runtimes that set ome.io/engine"
default_runtime="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --show-only templates/ome-controller/default-runtime.yaml)"
if grep -Fq 'ome.io/engine' <<<"${default_runtime}"; then
  fail "default-runtime sets ome.io/engine, so installing it would depend on the preset webhook being served"
fi

# Remote placement access is an optional member-side grant, with platform-owned authentication.
if grep -Fq 'ome.io/component: multicluster-access' <<<"$rendered"; then
  fail "remote placement access rendered by default"
fi
placement_access() {
  "${helm_bin}" template ome-resources "${chart_dir}" --namespace ome \
    --set ome.multiclusterAccess.enabled=true \
    --show-only templates/ome-controller/rbac/multicluster_access.yaml "$@"
}
access="$(placement_access)"
grep -Fq 'kind: ClusterRole' <<<"$access" || fail "placement role missing"
for forbidden in 'kind: ServiceAccount' 'kind: Secret' 'kind: ClusterRoleBinding'; do
  if grep -Fq "$forbidden" <<<"$access"; then
    fail "role-only placement access rendered $forbidden"
  fi
done
bound_access="$(placement_access \
  --set 'ome.multiclusterAccess.subjects[0].kind=Group' \
  --set 'ome.multiclusterAccess.subjects[0].name=placement-controllers')"
grep -Fq 'kind: ClusterRoleBinding' <<<"$bound_access" || fail "placement binding missing"
grep -Fq 'name: "placement-controllers"' <<<"$bound_access" || fail "placement group missing"
if grep -Eq '^kind: (ServiceAccount|Secret)$' <<<"$bound_access"; then
  fail "placement group access provisioned credentials"
fi
for invalid in \
  'ome.multiclusterAccess.subjects[0].kind=Robot' \
  'ome.multiclusterAccess.subjects[0].kind=User' \
  'ome.multiclusterAccess.subjects[0].kind=ServiceAccount,ome.multiclusterAccess.subjects[0].name=existing'; do
  if placement_access --set "$invalid" >/dev/null 2>&1; then
    fail "invalid placement access subject rendered: $invalid"
  fi
done
existing_access="$(placement_access \
  --set 'ome.multiclusterAccess.subjects[0].kind=ServiceAccount' \
  --set 'ome.multiclusterAccess.subjects[0].name=existing' \
  --set 'ome.multiclusterAccess.subjects[0].namespace=identity')"
grep -Fq 'namespace: "identity"' <<<"$existing_access" || fail "existing account namespace missing"
if grep -Eq '^kind: (ServiceAccount|Secret)$' <<<"$existing_access"; then
  fail "placement binding provisioned credentials"
fi

# The preset mutator only changes runtimes that set ome.io/engine and passes
# every other runtime through untouched, so the webhook must only be called for
# those. Otherwise, while the service is still backed by a manager that does not
# serve the preset path, failurePolicy: Fail rejects every
# ServingRuntime/ClusterServingRuntime write, including this chart's own
# default-runtime.
preset_webhook="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --show-only templates/ome-controller/webhooks/runtimepreset.yaml)"
engine_preset_condition="expression: \"has(object.metadata.annotations) && 'ome.io/engine' in object.metadata.annotations && object.metadata.annotations['ome.io/engine'] != ''\""
[ "$(grep -Fc -- "${engine_preset_condition}" <<<"${preset_webhook}")" -eq 2 ] ||
  fail "preset webhooks are not scoped to runtimes that set ome.io/engine"
default_runtime="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --show-only templates/ome-controller/default-runtime.yaml)"
if grep -Fq 'ome.io/engine' <<<"${default_runtime}"; then
  fail "default-runtime sets ome.io/engine, so installing it would depend on the preset webhook being served"
fi
