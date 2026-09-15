#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
deploy="${script_dir}/deploy.sh"

fail() {
  echo "deploy test: $*" >&2
  exit 1
}

line_of() {
  local pattern="$1"
  local line
  line="$(grep -nF -- "${pattern}" "${deploy}" | head -1 | cut -d: -f1)"
  [[ -n "${line}" ]] || fail "missing ${pattern}"
  printf '%s\n' "${line}"
}

test -x "${deploy}" || fail "deploy.sh must be executable"
bash -n "${deploy}"

# Helm 4 introduced the client-side update switch this harness needs to avoid
# taking cert-manager's injected caBundle field ownership on reruns.
grep -Fq 'Helm 4' "${deploy}" || fail "Helm 4 requirement is not documented"
grep -Fq 'helm_major' "${deploy}" || fail "Helm major version is not checked"
[[ "$(grep -Fc -- '--server-side=false' "${deploy}")" -ge 2 ]] ||
  fail "both manager convergence passes must preserve cert-manager field ownership"

# Same-tag local images and mounted scheduler configuration do not alter a pod
# template. Every real process therefore needs an explicit controlled restart.
manager_restart="$(line_of 'rollout restart deployment/ome-controller-manager')"
ome_restart="$(line_of 'rollout restart deployment/ome-scheduler')"
default_restart="$(line_of 'rollout restart deployment/alfred-default-scheduler')"
first_probe="$(line_of 'default_profile="$(probe_profile')"
alfred_restart="$(line_of 'rollout restart deployment/ome-alfred')"

((manager_restart < first_probe)) || fail "manager restart must precede profile probes"
((ome_restart < first_probe)) || fail "OME scheduler restart must precede profile probes"
((default_restart < first_probe)) || fail "plain scheduler restart must precede profile probes"
((alfred_restart > first_probe)) || fail "Alfred restart must follow the new profile registry"

# Pod readiness can precede CA injection and manager cache synchronization.
# Both manager phases must prove actual admission before deployment continues.
readiness_calls="$(grep -n '^wait_manager_webhook$' "${deploy}" | cut -d: -f1 || true)"
[[ "$(wc -w <<<"${readiness_calls}" | tr -d ' ')" == 2 ]] ||
  fail "manager admission must be checked before convergence and after restart"
read -r before_convergence after_restart <<<"$(tr '\n' ' ' <<<"${readiness_calls}")"
second_manager_helm="$(grep -nF '"${h[@]}" upgrade --install ome "${project_dir}/charts/ome-resources"' "${deploy}" | tail -1 | cut -d: -f1)"
manager_image_check="$(line_of 'verify_running_image ome-controller-manager manager manager')"
((before_convergence < second_manager_helm && second_manager_helm < manager_restart)) ||
  fail "admission readiness must precede the second manager Helm pass"
((manager_restart < after_restart && after_restart < manager_image_check)) ||
  fail "admission readiness must follow the explicit manager restart"

# Exercise the actual wait function without contacting a cluster. Only kubectl
# and sleeping are replaced; JSON filtering, retries and deadlines remain real.
eval "$(sed -n '/^wait_manager_webhook() {$/,/^}$/p' "${deploy}")"
declare -F wait_manager_webhook >/dev/null || fail "missing manager admission wait"
test_dir="$(mktemp -d "${TMPDIR:-/tmp}/alfred-deploy-test.XXXXXX")"
trap 'rm -rf -- "${test_dir}"' EXIT
manifests="${script_dir}/manifests"
context=kind-alfred-e2e-test
k=(mock_kubectl --kubeconfig /dev/null --context kind-alfred-e2e-test)

mock_kubectl() {
  [[ "$1 $2 $3 $4" == '--kubeconfig /dev/null --context kind-alfred-e2e-test' ]] ||
    fail "admission probe omitted the explicit test cluster"
  shift 4
  [[ "$1" =~ ^--request-timeout=([1-9]|10)s$ ]] ||
    fail "admission probe did not bound each API request"
  case "$*" in
    *'create --dry-run=client --validate=false -f '*'workload-single.yaml -o json')
      # kubectl emits one JSON object per YAML document, not a JSON List.
      printf '%s\n' \
        '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"alfred-e2e"}}' \
        '{"apiVersion":"ome.io/v1beta1","kind":"ClusterServingRuntime","metadata":{"name":"alfred-e2e-runtime"},"spec":{"engineConfig":{"runner":{"name":"ome-container","image":"registry.k8s.io/pause:3.10"}}}}' \
        '{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"single","namespace":"alfred-e2e"},"spec":{"runtime":{"name":"alfred-e2e-runtime"}}}'
      ;;
    *'create --dry-run=server -f -')
      jq -e '.kind == "ClusterServingRuntime" and
        .metadata.generateName == "alfred-e2e-webhook-probe-" and
        (.metadata | has("name") or has("namespace") | not) and
        .spec.disabled == true and
        .spec.engineConfig.runner.name == "ome-container"' >/dev/null ||
        fail "admission probe did not isolate a fresh cluster runtime"
      printf '%s\n' admission >>"${test_dir}/calls"
      if [[ "${probe_mode}" == never ]] || (( $(wc -l <"${test_dir}/calls") < 3 )); then
        echo 'private admission error detail' >&2
        return 1
      fi
      ;;
    *) fail "unexpected or persistent kubectl operation: $*" ;;
  esac
}

sleep() {
  [[ "$1" == 2 || "$1" == 1 ]] || fail "readiness retries must wait at most two seconds"
  SECONDS=$((SECONDS + $1))
  ((SECONDS <= 182)) || fail "admission wait exceeded its deadline"
}

probe_mode=transient
: >"${test_dir}/calls"
output="$(SECONDS=0; wait_manager_webhook 2>&1)" || fail "transient admission failures were not retried"
[[ "$(wc -l <"${test_dir}/calls" | tr -d ' ')" == 3 ]] ||
  fail "admission wait did not stop at the first successful server dry run"
[[ "${output}" != *'private admission error detail'* ]] || fail "admission wait leaked command output"

probe_mode=never
: >"${test_dir}/calls"
if output="$(SECONDS=0; wait_manager_webhook 2>&1)"; then
  fail "unavailable admission was accepted"
fi
attempts="$(wc -l <"${test_dir}/calls" | tr -d ' ')"
((attempts > 1 && attempts <= 90)) || fail "admission wait was not bounded to 180 seconds"
[[ "${output}" == *'180 seconds'* ]] || fail "admission timeout did not identify its deadline"
[[ "${output}" != *'private admission error detail'* ]] || fail "admission timeout leaked command output"

# Readiness alone is insufficient: prove each new pod uses the image built by
# build.sh instead of an older cached :local image.
grep -Fq 'image-digests.json' "${deploy}" || fail "build image digest manifest is not required"
for pair in \
  'ome-controller-manager manager manager' \
  'ome-scheduler scheduler scheduler' \
  'alfred-default-scheduler scheduler scheduler' \
  'ome-alfred alfred alfred'; do
  read -r deployment container component <<<"${pair}"
  grep -Fq "verify_running_image ${deployment} ${container} ${component}" "${deploy}" ||
    fail "missing image verification for ${deployment}/${container}"
done

echo "deploy test: passed"
