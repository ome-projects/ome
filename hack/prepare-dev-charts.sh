#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -ne 5 ]]; then
  echo "usage: $0 SOURCE_DIR DESTINATION_DIR CHART_VERSION IMAGE_TAG IMAGE_HUB" >&2
  exit 2
fi

source_dir="${1%/}"
destination_dir="${2%/}"
chart_version="$3"
image_tag="$4"
image_hub="${5%/}"
yq_bin="${YQ_BIN:-yq}"

if [[ ! -d "${source_dir}" ]]; then
  echo "source chart directory does not exist: ${source_dir}" >&2
  exit 2
fi
if [[ -z "${destination_dir}" || -z "${chart_version}" || -z "${image_tag}" || -z "${image_hub}" ]]; then
  echo "destination, chart version, image tag, and image hub must be non-empty" >&2
  exit 2
fi
if ! command -v "${yq_bin}" >/dev/null 2>&1; then
  echo "yq v4 is required: ${yq_bin}" >&2
  exit 2
fi
if [[ -e "${destination_dir}" && ! -d "${destination_dir}" ]]; then
  echo "destination exists and is not a directory: ${destination_dir}" >&2
  exit 2
fi
if [[ -d "${destination_dir}" ]] && [[ -n "$(find "${destination_dir}" -mindepth 1 -print -quit)" ]]; then
  echo "destination must be absent or empty: ${destination_dir}" >&2
  exit 2
fi

mkdir -p "${destination_dir}"
source_abs="$(cd "${source_dir}" && pwd -P)"
destination_abs="$(cd "${destination_dir}" && pwd -P)"
if [[ "${destination_abs}" == "${source_abs}" || "${destination_abs}" == "${source_abs}/"* ]]; then
  echo "destination must be outside the source chart directory" >&2
  exit 2
fi

dev_charts=(ome-crd ome-resources ome-scheduler ome-alfred)
for chart in "${dev_charts[@]}"; do
  chart_dir="${source_abs}/${chart}"
  if [[ ! -f "${chart_dir}/Chart.yaml" ]]; then
    echo "required dev chart is missing: ${chart_dir}/Chart.yaml" >&2
    exit 2
  fi
  cp -R "${chart_dir}" "${destination_abs}/"
done

export DEV_CHART_VERSION="${chart_version}"
export DEV_IMAGE_TAG="${image_tag}"
export DEV_IMAGE_HUB="${image_hub}"

while IFS= read -r chart_file; do
  "${yq_bin}" eval --inplace \
    '.version = strenv(DEV_CHART_VERSION) | .appVersion = strenv(DEV_IMAGE_TAG)' \
    "${chart_file}"
done < <(find "${destination_abs}" -mindepth 2 -maxdepth 2 -name Chart.yaml | sort)

resources_values="${destination_abs}/ome-resources/values.yaml"
scheduler_values="${destination_abs}/ome-scheduler/values.yaml"
alfred_values="${destination_abs}/ome-alfred/values.yaml"
for values_file in "${resources_values}" "${scheduler_values}" "${alfred_values}"; do
  if [[ ! -f "${values_file}" ]]; then
    echo "required chart values file is missing: ${values_file}" >&2
    exit 2
  fi
done

"${yq_bin}" eval --inplace '
  .ome.version = strenv(DEV_IMAGE_TAG) |
  .ome.controller.image = (strenv(DEV_IMAGE_HUB) + "/ome-manager") |
  .ome.controller.tag = strenv(DEV_IMAGE_TAG) |
  .ome.omeAgent.image = (strenv(DEV_IMAGE_HUB) + "/ome-agent") |
  .ome.omeAgent.tag = strenv(DEV_IMAGE_TAG) |
  .modelAgent.image.repository = (strenv(DEV_IMAGE_HUB) + "/model-agent") |
  .modelAgent.image.tag = strenv(DEV_IMAGE_TAG)
' "${resources_values}"

"${yq_bin}" eval --inplace '
  .scheduler.image.repository = (strenv(DEV_IMAGE_HUB) + "/ome-scheduler") |
  .scheduler.image.tag = strenv(DEV_IMAGE_TAG)
' "${scheduler_values}"

"${yq_bin}" eval --inplace '
  .image.repository = (strenv(DEV_IMAGE_HUB) + "/alfred") |
  .image.tag = strenv(DEV_IMAGE_TAG)
' "${alfred_values}"
