#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
yq_bin="${YQ_BIN:-yq}"
helm_bin="${HELM_BIN:-helm}"
chart_version="0.0.0-dev.20260914010101+abcdef1"
image_tag="dev-abcdef1"
image_hub="ghcr.io/ome-projects"
temp_dir="$(mktemp -d)"
trap 'rm -rf -- "${temp_dir}"' EXIT

charts_hash() {
  find "${repo_root}/charts" -type f -print0 \
    | sort -z \
    | xargs -0 shasum \
    | shasum \
    | awk '{print $1}'
}

before_hash="$(charts_hash)"
output_dir="${temp_dir}/charts"

"${repo_root}/hack/prepare-dev-charts.sh" \
  "${repo_root}/charts" \
  "${output_dir}" \
  "${chart_version}" \
  "${image_tag}" \
  "${image_hub}"

after_hash="$(charts_hash)"
if [[ "${before_hash}" != "${after_hash}" ]]; then
  echo "source charts changed during dev-chart preparation" >&2
  exit 1
fi

output_count="$(find "${output_dir}" -mindepth 2 -maxdepth 2 -name Chart.yaml | wc -l | tr -d ' ')"
if [[ "${output_count}" != "4" ]]; then
  echo "expected four deployable dev charts, found ${output_count}" >&2
  exit 1
fi
for chart in ome-alfred ome-crd ome-resources ome-scheduler; do
  test -f "${output_dir}/${chart}/Chart.yaml"
done
for excluded_chart in ome-quota-manager ome-serving; do
  if [[ -e "${output_dir}/${excluded_chart}" ]]; then
    echo "unsupported dev chart was prepared: ${excluded_chart}" >&2
    exit 1
  fi
done

while IFS= read -r chart_file; do
  "${yq_bin}" eval --exit-status \
    ".version == \"${chart_version}\" and .appVersion == \"${image_tag}\"" \
    "${chart_file}" >/dev/null
done < <(find "${output_dir}" -mindepth 2 -maxdepth 2 -name Chart.yaml | sort)

"${yq_bin}" eval --exit-status \
  ".ome.version == \"${image_tag}\" and
   .ome.controller.image == \"${image_hub}/ome-manager\" and
   .ome.omeAgent.image == \"${image_hub}/ome-agent\" and
   .modelAgent.image.repository == \"${image_hub}/model-agent\" and
   .ome.benchmarkJob.image == \"genai-bench\" and
   .ome.benchmarkJob.tag == \"0.1.113\" and
   .global.hub == \"ghcr.io/moirai-internal\"" \
  "${output_dir}/ome-resources/values.yaml" >/dev/null

"${yq_bin}" eval --exit-status \
  ".scheduler.image.repository == \"${image_hub}/ome-scheduler\" and
   .scheduler.image.tag == \"${image_tag}\"" \
  "${output_dir}/ome-scheduler/values.yaml" >/dev/null

"${yq_bin}" eval --exit-status \
  ".image.repository == \"${image_hub}/alfred\" and
   .image.tag == \"${image_tag}\"" \
  "${output_dir}/ome-alfred/values.yaml" >/dev/null

resources_render="$(${helm_bin} template dev "${output_dir}/ome-resources" --kube-version 1.35.0 --set modelAgent.enabled=true)"
scheduler_render="$(${helm_bin} template dev "${output_dir}/ome-scheduler" --kube-version 1.35.0)"
alfred_render="$(${helm_bin} template dev "${output_dir}/ome-alfred" --kube-version 1.35.0)"

grep -Fq "${image_hub}/ome-manager:${image_tag}" <<<"${resources_render}"
grep -Fq "${image_hub}/model-agent:${image_tag}" <<<"${resources_render}"
grep -Fq "${image_hub}/ome-agent:${image_tag}" <<<"${resources_render}"
grep -Fq "ghcr.io/moirai-internal/genai-bench:0.1.113" <<<"${resources_render}"
grep -Fq "${image_hub}/ome-scheduler:${image_tag}" <<<"${scheduler_render}"
grep -Fq "${image_hub}/alfred:${image_tag}" <<<"${alfred_render}"

blocked_dir="${temp_dir}/blocked"
mkdir -p "${blocked_dir}"
touch "${blocked_dir}/sentinel"
if "${repo_root}/hack/prepare-dev-charts.sh" \
  "${repo_root}/charts" \
  "${blocked_dir}" \
  "${chart_version}" \
  "${image_tag}" \
  "${image_hub}" >"${temp_dir}/blocked.log" 2>&1; then
  echo "preparation unexpectedly accepted a non-empty destination" >&2
  exit 1
fi
grep -Fq "destination must be absent or empty" "${temp_dir}/blocked.log"
test -f "${blocked_dir}/sentinel"
