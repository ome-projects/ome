#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
trap stop_nested_tunnel EXIT

require_command kubectl jq

scenario_namespace="alfred-e2e"
deadline=$((SECONDS + 120))
last_score="unavailable"
last_observed="unavailable"
last_count="0"
last_advisory="0"
last_migration_annotations="unavailable"
last_pending="unavailable"

start_nested_tunnel

while (( SECONDS < deadline )); do
  alfred_pod="$("${host_kubectl[@]}" -n "${host_namespace}" get pods \
    -l app=alfred-under-test -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "${alfred_pod}" ]]; then
    sleep 2
    continue
  fi

  metrics="$("${host_kubectl[@]}" get --raw \
    "/api/v1/namespaces/${host_namespace}/pods/${alfred_pod}:8080/proxy/metrics" \
    2>/dev/null || true)"
  last_score="$(awk '$1 == "alfred_cluster_fragmentation_score" {print $2}' <<<"${metrics}")"
  last_observed="$(awk '$1 ~ /^alfred_fragmentation_observed\{/ && $1 ~ /pool="ALFRED-E2E-H100"/ && $1 ~ /size="8"/ {print $2}' <<<"${metrics}")"
  record="$(nested_kubectl -n ome get configmap alfred-recommendations \
    -o jsonpath='{.data.last-cycle\.json}' 2>/dev/null || true)"
  if [[ -n "${record}" ]]; then
    last_count="$(jq -r '.recommendations // [] | length' <<<"${record}")"
    last_advisory="$(jq -r '[.recommendations // [] | .[] | select(.outcome == "advisory" and .advisoryReason == "RawDeploymentMigrationUnsupported")] | length' <<<"${record}")"
  fi
  isvc="$(nested_kubectl -n "${scenario_namespace}" get inferenceservice fragmented -o json 2>/dev/null || true)"
  if [[ -n "${isvc}" ]]; then
    last_migration_annotations="$(jq -r '[.metadata.annotations // {} | keys[] | select(startswith("ome.io/migration-request-v1-"))] | length' <<<"${isvc}")"
  fi

  # An empty nodeName alone would also hold if nothing ever tried to schedule
  # the pod, so require the scheduler's own verdict: it looked and found no
  # node with 8 free GPUs, which is the condition the fixture exists to create.
  pending_node="$(nested_kubectl -n "${scenario_namespace}" get pod pending-8gpu \
    -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
  pending_reason="$(nested_kubectl -n "${scenario_namespace}" get pod pending-8gpu \
    -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].reason}' 2>/dev/null || true)"
  last_pending="node=${pending_node:-none} reason=${pending_reason:-none}"
  if [[ -n "${last_score}" && -n "${last_observed}" ]] &&
     awk -v score="${last_score}" 'BEGIN { exit !(score == 0) }' &&
     awk -v observed="${last_observed}" 'BEGIN { exit !(observed > 0.25) }' &&
     (( last_count > 0 )) && (( last_advisory > 0 )) &&
     [[ "${last_migration_annotations}" == "0" ]] &&
     [[ -z "${pending_node}" && "${pending_reason}" == "Unschedulable" ]]; then
    echo "Alfred fragmentation E2E passed"
    echo "  executable score: ${last_score}"
    echo "  observed size-8 fragmentation: ${last_observed}"
    echo "  recommendations: ${last_count}"
    echo "  RawDeployment advisory: ${last_advisory}"
    echo "  migration annotations: ${last_migration_annotations}"
    echo "  pending 8-GPU demand: Unschedulable"
    exit 0
  fi
  sleep 2
done

echo "Alfred fragmentation E2E timed out" >&2
echo "  executable score: ${last_score}" >&2
echo "  observed size-8 fragmentation: ${last_observed}" >&2
echo "  recommendations: ${last_count}" >&2
echo "  RawDeployment advisory: ${last_advisory}" >&2
echo "  migration annotations: ${last_migration_annotations}" >&2
echo "  pending 8-GPU demand: ${last_pending}" >&2
"${host_kubectl[@]}" -n "${host_namespace}" logs deployment/alfred-under-test \
  --tail=80 >&2 || true
exit 1
