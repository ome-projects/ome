#!/usr/bin/env bash
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
helm_bin="${HELM_BIN:-helm}"
kube_version="1.35.0"

fail() {
  echo "ome-scheduler chart test: $*" >&2
  exit 1
}

assert_contains() {
  local needle="$1"
  grep -Fq -- "${needle}" <<<"${rendered}" || fail "render missing: ${needle}"
}

assert_absent() {
  local needle="$1"
  if grep -Fq -- "${needle}" <<<"${rendered}"; then
    fail "render unexpectedly contains: ${needle}"
  fi
}

"${helm_bin}" lint --strict --kube-version "${kube_version}" "${chart_dir}" >/dev/null
rendered="$("${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --namespace ome-system)"

assert_contains 'helm.sh/chart: ome-scheduler-0.1.0'
assert_contains 'app.kubernetes.io/version: "0.1.0"'

# Lease timing renders as a complete set, and only then.
assert_contains '      leaseDuration: 60s'
assert_contains '      renewDeadline: 30s'
assert_contains '      retryPeriod: 6s'
partial_lease="$("${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --namespace ome-system \
  --set scheduler.leaderElection.retryPeriod=)"
if grep -Fq 'leaseDuration:' <<<"${partial_lease}"; then
  fail "partial leader-election timing was rendered"
fi

# Embedded KubeSchedulerConfiguration contract. Helm cannot schema-check the
# opaque ConfigMap string, so keep indentation-sensitive assertions for its
# extension-point and plugin-argument structure.
assert_contains '          multiPoint:'
assert_contains '              - name: OMEGangPack'
assert_contains '          score:'
assert_contains '              - name: NodeResourcesFit'
assert_contains '                type: MostAllocated'
assert_contains '              - name: PodTopologySpread'
assert_contains '              defaultPermitTimeoutSeconds: 600'
assert_contains '              podGroupTopologyKeyAnnotation: "ome.io/topology-key"'
assert_contains '              unsupportedPlacementGroupLabel: "ome.io/placement-group"'
assert_contains '              podGroupSyncTimeoutSeconds: 30'
assert_contains '              gcIntervalSeconds: 60'
assert_contains '              standaloneDomainPacking: true'

# Gang preemption is off unless enabled, and needs the built-in preemptor off.
assert_absent '              preemption:'
preempting="$("${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.plugin.preemption.enabled=true \
  --set scheduler.plugin.preemption.maxInFlight=2)"
grep -Fq $'              standaloneDomainPacking: true\n              preemption:\n                maxInFlight: 2' <<<"${preempting}" \
  || fail "enabled gang preemption was not rendered into the plugin arguments"
grep -Fq '              - name: DefaultPreemption' <<<"${preempting}" \
  || fail "built-in DefaultPreemption was not disabled beside gang preemption"
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.plugin.preemption.enabled=true \
  --set scheduler.disablePreemption=false >/dev/null 2>&1; then
  fail "gang preemption rendered beside the built-in preemptor"
fi
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.plugin.preemption.maxInFlight=0 >/dev/null 2>&1; then
  fail "zero preemptions in flight unexpectedly passed values schema validation"
fi

# Node sampling is profile-scoped and opt-in: absent by default (upstream's
# adaptive band), rendered as a sibling of schedulerName when configured.
assert_absent 'percentageOfNodesToScore'
exhaustive="$("${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.percentageOfNodesToScore=100)"
grep -Fq '        percentageOfNodesToScore: 100' <<<"${exhaustive}" \
  || fail "configured percentageOfNodesToScore was not rendered into the profile"
for invalid in 0 101; do
  if "${helm_bin}" template ome-scheduler "${chart_dir}" \
    --kube-version "${kube_version}" \
    --set "scheduler.percentageOfNodesToScore=${invalid}" >/dev/null 2>&1; then
    fail "out-of-range percentageOfNodesToScore ${invalid} unexpectedly passed values schema validation"
  fi
done

# Production deployment surface.
assert_contains 'kind: PodDisruptionBudget'
assert_contains 'name: ome-scheduler-metrics'
assert_contains 'path: /healthz'
assert_contains 'path: /readyz'
assert_contains 'requiredDuringSchedulingIgnoredDuringExecution:'
assert_contains 'topologyKey: kubernetes.io/hostname'
assert_contains 'whenUnsatisfiable: ScheduleAnyway'
assert_contains 'maxUnavailable: 1'
assert_contains 'maxSurge: 0'
assert_contains $'        resources:\n          limits:\n            cpu: "2"\n            memory: 12Gi\n          requests:\n            cpu: 200m\n            memory: 4Gi'

# Resource sizing remains configurable for constrained clusters.
custom_resources="$("${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.resources.requests.memory=1Gi \
  --set scheduler.resources.limits.memory=2Gi)"
grep -Fq $'        resources:\n          limits:\n            cpu: "2"\n            memory: 2Gi\n          requests:\n            cpu: 200m\n            memory: 1Gi' \
  <<<"${custom_resources}" || fail "custom scheduler memory resources were not rendered"

# Core and volume permissions come from the cluster's version-managed roles;
# the chart-owned role is restricted to the distinct Lease/PodGroup contract.
assert_contains 'name: system:kube-scheduler'
assert_contains 'name: system:volume-scheduler'
assert_contains 'name: ome-scheduler-extras'
assert_contains 'resources: ["podgroups"]'
assert_absent 'resources: ["nodes"]'
assert_absent 'resources: ["pods"]'
assert_absent 'resources: ["persistentvolumeclaims", "persistentvolumes"]'

# Cross-field safety: replicated schedulers must use leader election.
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.replicaCount=2 \
  --set scheduler.leaderElect=false >/dev/null 2>&1; then
  fail "unsafe HA configuration unexpectedly rendered"
fi

# The values schema must reject invalid behavioral timeouts.
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.plugin.gcIntervalSeconds=0 >/dev/null 2>&1; then
  fail "zero GC interval unexpectedly passed values schema validation"
fi
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.name=Invalid_Name >/dev/null 2>&1; then
  fail "invalid scheduler resource/profile name unexpectedly passed values schema validation"
fi
too_long_name="$(printf '%056d' 0 | tr '0' 'a')"
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set "scheduler.name=${too_long_name}" >/dev/null 2>&1; then
  fail "overlong scheduler resource/profile name unexpectedly passed values schema validation"
fi

# The image contains kube-scheduler 1.35 APIs and is not a cross-minor binary.
for unsupported_version in 1.34.99 1.36.0; do
  if "${helm_bin}" template ome-scheduler "${chart_dir}" \
    --kube-version "${unsupported_version}" >/dev/null 2>&1; then
    fail "unsupported Kubernetes ${unsupported_version} unexpectedly rendered"
  fi
done

# OME's controller publishes a fixed topology annotation. Generic producers
# must explicitly disable that integration guard before selecting another key.
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.plugin.podGroupTopologyKeyAnnotation=example.com/topology-key \
  >/dev/null 2>&1; then
  fail "OME integration accepted a mismatched PodGroup topology annotation"
fi
generic="$("${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.omeControllerIntegration.enabled=false \
  --set scheduler.plugin.podGroupTopologyKeyAnnotation=example.com/topology-key)"
if ! grep -Fq 'podGroupTopologyKeyAnnotation: "example.com/topology-key"' <<<"${generic}"; then
  fail "generic producer annotation was not rendered after disabling OME integration"
fi

# A PDB cannot require every replica without blocking voluntary maintenance.
if "${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.replicaCount=1 \
  --set scheduler.podDisruptionBudget.minAvailable=1 >/dev/null 2>&1; then
  fail "blocking singleton PodDisruptionBudget unexpectedly rendered"
fi
"${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --set scheduler.replicaCount=1 \
  --set scheduler.podDisruptionBudget.minAvailable=0 >/dev/null

# Optional resources must be removable without leaving malformed YAML.
minimal="$("${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --namespace ome-system \
  --set scheduler.replicaCount=1 \
  --set scheduler.metrics.service.enabled=false \
  --set scheduler.podDisruptionBudget.enabled=false)"
if grep -Fq 'kind: PodDisruptionBudget' <<<"${minimal}"; then
  fail "disabled PodDisruptionBudget was rendered"
fi
# Anchored: the Service is named exactly `ome-scheduler-metrics`, and other
# metrics resources share that prefix (e.g. the `-metrics-reader` ClusterRole),
# so a substring match would report them as the disabled Service.
if grep -Eq '^  name: ome-scheduler-metrics$' <<<"${minimal}"; then
  fail "disabled metrics Service was rendered"
fi

# Scraping the secure metrics port needs delegated authn/authz on the scheduler
# side and a /metrics grant on the scraper side. Both ship by default; both must
# be removable.
assert_contains "name: system:auth-delegator"
assert_contains "name: ome-scheduler-metrics-reader"
assert_contains 'nonResourceURLs: ["/metrics"]'

no_scrape_rbac="$("${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --namespace ome-system \
  --set scheduler.metrics.authDelegation.enabled=false \
  --set scheduler.metrics.reader.create=false)"
if grep -Fq 'name: system:auth-delegator' <<<"${no_scrape_rbac}"; then
  fail "disabled auth-delegator binding was rendered"
fi
if grep -Fq 'name: ome-scheduler-metrics-reader' <<<"${no_scrape_rbac}"; then
  fail "disabled metrics-reader ClusterRole was rendered"
fi

# The reader role binds only to explicitly configured scrapers; the chart never
# guesses an identity. With none configured exactly one object carries the name
# (the ClusterRole); a binding would add a second.
reader_objects="$(grep -cE '^  name: ome-scheduler-metrics-reader$' <<<"${rendered}" || true)"
if [[ "${reader_objects}" != "1" ]]; then
  fail "expected only the metrics-reader ClusterRole with no scraper configured, found ${reader_objects} objects"
fi
bound="$("${helm_bin}" template ome-scheduler "${chart_dir}" \
  --kube-version "${kube_version}" \
  --namespace ome-system \
  --set-json 'scheduler.metrics.reader.serviceAccounts=[{"name":"collector","namespace":"telemetry"}]')"
grep -Fq 'name: collector' <<<"${bound}" || fail "configured scraper was not bound"
grep -Fq 'namespace: telemetry' <<<"${bound}" || fail "configured scraper namespace missing"
