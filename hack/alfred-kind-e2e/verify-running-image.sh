#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 8 ]]; then
  echo "usage: $0 KUBECONFIG CONTEXT NAMESPACE DEPLOYMENT CONTAINER EXPECTED_NODE EXPECTED_CONFIG_ID DOCKER_CONTEXT" >&2
  exit 2
fi

kubeconfig="$1"
context="$2"
namespace="$3"
deployment="$4"
container="$5"
expected_node="$6"
expected_config_id="$7"
docker_context="$8"
kubectl_bin="${KUBECTL_BIN:-kubectl}"
docker_bin="${DOCKER_BIN:-docker}"
k=("${kubectl_bin}" --kubeconfig "${kubeconfig}" --context "${context}")

content_digest() {
  local pod_node="$1"
  local digest="$2"
  "${docker_bin}" --context "${docker_context}" exec "${pod_node}" \
    ctr -n k8s.io content get "${digest}"
}

resolve_imported_config_id() {
  local pod_name="$1"
  local pod_node="$2"
  local configured_image="$3"
  local image_id="$4"
  local node_json operating_system architecture import_digest import_index
  local named_digest platform_index platform_digest image_manifest config_digest

  [[ "${image_id}" =~ (sha256:[0-9a-f]{64})$ ]] || {
    echo "verify running image: invalid Pod image ID for ${pod_name}: ${image_id}" >&2
    return 1
  }
  import_digest="${BASH_REMATCH[1]}"

  node_json="$("${k[@]}" get node "${pod_node}" -o json)" || return 1
  operating_system="$(jq -er '.status.nodeInfo.operatingSystem' <<<"${node_json}")" || return 1
  architecture="$(jq -er '.status.nodeInfo.architecture' <<<"${node_json}")" || return 1

  import_index="$(content_digest "${pod_node}" "${import_digest}")" || return 1
  named_digest="$(jq -er --arg image "${configured_image}" \
    '[.manifests[]? | select(.annotations["io.containerd.image.name"] == $image)] |
     if length == 1 then .[0].digest
     else error("expected exactly one descriptor for configured image") end' \
    <<<"${import_index}")" || {
      echo "verify running image: import index for ${pod_name} does not contain exactly one descriptor for ${configured_image}" >&2
      return 1
    }
  [[ "${named_digest}" =~ ^sha256:[0-9a-f]{64}$ ]] || {
    echo "verify running image: invalid configured-image digest for ${pod_name}: ${named_digest}" >&2
    return 1
  }

  platform_index="$(content_digest "${pod_node}" "${named_digest}")" || return 1
  platform_digest="$(jq -er --arg os "${operating_system}" --arg arch "${architecture}" \
    '[.manifests[]? | select(.platform.os == $os and .platform.architecture == $arch)] |
     if length == 1 then .[0].digest
     else error("expected exactly one descriptor for node platform") end' \
    <<<"${platform_index}")" || {
      echo "verify running image: image index for ${pod_name} does not contain exactly one ${operating_system}/${architecture} descriptor" >&2
      return 1
    }
  [[ "${platform_digest}" =~ ^sha256:[0-9a-f]{64}$ ]] || {
    echo "verify running image: invalid platform-manifest digest for ${pod_name}: ${platform_digest}" >&2
    return 1
  }

  image_manifest="$(content_digest "${pod_node}" "${platform_digest}")" || return 1
  config_digest="$(jq -er '.config.digest' <<<"${image_manifest}")" || return 1
  [[ "${config_digest}" =~ ^sha256:[0-9a-f]{64}$ ]] || {
    echo "verify running image: invalid config digest for ${pod_name}: ${config_digest}" >&2
    return 1
  }
  printf '%s\n' "${config_digest}"
}

for command in "${docker_bin}" jq "${kubectl_bin}"; do
  command -v "${command}" >/dev/null || {
    echo "verify running image: required command not found: ${command}" >&2
    exit 1
  }
done
[[ "${expected_config_id}" =~ ^sha256:[0-9a-f]{64}$ ]] || {
  echo "verify running image: invalid expected config ID: ${expected_config_id}" >&2
  exit 1
}

selector="$("${k[@]}" -n "${namespace}" get deployment "${deployment}" -o json \
  | jq -r '.spec.selector.matchLabels | to_entries | map("\(.key)=\(.value)") | join(",")')"
pods="$("${k[@]}" -n "${namespace}" get pods -l "${selector}" -o json)"
jq -e \
  --arg container "${container}" \
  '[.items[] | select(.metadata.deletionTimestamp == null) |
    .status.containerStatuses[]? | select(.name == $container)] as $statuses |
   ($statuses | length) > 0 and all($statuses[]; .ready == true)' \
  <<<"${pods}" >/dev/null || {
    echo "verify running image: no ready ${deployment}/${container} container found" >&2
    exit 1
  }

records="$(jq -r \
  --arg container "${container}" \
  '.items[] | select(.metadata.deletionTimestamp == null) as $pod |
   $pod.status.containerStatuses[]? | select(.name == $container) |
   [$pod.metadata.name, $pod.spec.nodeName, .image, .imageID] | @tsv' \
  <<<"${pods}")"
count=0
while IFS=$'\t' read -r pod_name pod_node configured_image image_id; do
  [[ -n "${pod_name}" ]] || continue
  [[ "${pod_node}" == "${expected_node}" ]] || {
    echo "verify running image: ${pod_name} runs on ${pod_node}, expected ${expected_node}" >&2
    exit 1
  }

  # First resolve through CRI while its imported alias is still present. A
  # subsequent kind load can remove that alias even though the immutable
  # content backing a running Pod remains. In that case, walk only the exact
  # configured-image and node-platform descriptors in containerd's OCI
  # content store. Ambiguous descriptor matches are rejected.
  inspect_json=""
  resolved=""
  if inspect_json="$("${docker_bin}" --context "${docker_context}" exec "${pod_node}" \
    crictl inspecti "${image_id}" 2>/dev/null)" && \
    resolved="$(jq -er '.status.id' <<<"${inspect_json}" 2>/dev/null)"; then
    :
  else
    resolved="$(resolve_imported_config_id \
      "${pod_name}" "${pod_node}" "${configured_image}" "${image_id}")" || {
        echo "verify running image: cannot resolve immutable image content for ${pod_name}" >&2
        exit 1
      }
  fi
  [[ "${resolved}" =~ (sha256:[0-9a-f]{64})$ ]] || {
    echo "verify running image: CRI returned invalid config ID for ${pod_name}: ${resolved}" >&2
    exit 1
  }
  actual_config_id="${BASH_REMATCH[1]}"
  [[ "${actual_config_id}" == "${expected_config_id}" ]] || {
    echo "verify running image: stale image for ${deployment}/${container}: running ${actual_config_id}, built ${expected_config_id}" >&2
    exit 1
  }
  count=$((count + 1))
done <<<"${records}"

((count > 0)) || {
  echo "verify running image: no runtime image ID found for ${deployment}/${container}" >&2
  exit 1
}
