#!/usr/bin/env bash
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${HELM_BIN:-helm}"

fail() {
  echo "ome-resources chart test: $*" >&2
  exit 1
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
if grep -Fq 'minReadySeconds' <<<"${controller_config}"; then
  fail "deploy.minReadySeconds was rendered when unset"
fi

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

model_agent="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set modelAgent.enabled=true \
  --show-only templates/model-agent-daemonset/daemonset.yaml)"
grep -Fq 'helm.sh/chart: ome-resources-0.1.0' <<<"${model_agent}" ||
  fail "model agent chart version label was not rendered"
grep -Fq -- '--same-path-reuse-wait-timeout' <<<"${model_agent}" ||
  fail "model agent did not render the canonical same-path reuse timeout flag"
if grep -Fq -- '--demand-priority-enabled' <<<"${model_agent}"; then
  fail "model agent still rendered the removed endpoint-demand watcher flag"
fi
if grep -Fq 'priorityClassName:' <<<"${model_agent}"; then
  fail "model agent rendered a PriorityClass when none was configured"
fi

prioritized_model_agent="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set modelAgent.enabled=true \
  --set-string modelAgent.priorityClassName=workload-high \
  --show-only templates/model-agent-daemonset/daemonset.yaml)"
grep -Fq 'priorityClassName: "workload-high"' <<<"${prioritized_model_agent}" ||
  fail "model agent did not render an operator-supplied PriorityClass"

demand_controller="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --set ome.controller.modelDownloadScheduling.servingDemandPriorityEnabled=true \
  --show-only templates/ome-controller/deployment.yaml)"
grep -Fq -- '--serving-demand-download-priority' <<<"${demand_controller}" ||
  fail "controller did not render serving-demand projection when enabled"

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
# forbidden, fan-out still succeeds, and placement never leaves Racing — a
# failure mode no lane in the test matrix reproduces, because they all
# authenticate as cluster-admin rather than as this ServiceAccount.
multicluster_access="$("${helm_bin}" template ome-resources "${chart_dir}" \
  --namespace ome \
  --show-only templates/ome-controller/rbac/multicluster_access.yaml)"
grep -Fqx '  - inferencereplicas' <<<"${multicluster_access}" ||
  fail "multicluster-access ClusterRole does not grant inferencereplicas"

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
