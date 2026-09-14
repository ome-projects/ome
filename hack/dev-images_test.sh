#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
yq_bin="${YQ_BIN:-yq}"
dev_workflow="${repo_root}/.github/workflows/dev-images.yaml"
pr_workflow="${repo_root}/.github/workflows/pr-validation.yml"

"${repo_root}/hack/prepare-dev-charts_test.sh"

assert_yq() {
  local expression="$1"
  local file="$2"
  local message="$3"
  if ! "${yq_bin}" eval --exit-status "${expression}" "${file}" >/dev/null; then
    echo "${message}" >&2
    exit 1
  fi
}

expected_names="manager,model-agent,ome-agent,ome-scheduler,alfred"
expected_images="ome-manager,model-agent,ome-agent,ome-scheduler,alfred"
expected_targets="ome-image,model-agent-image,ome-agent-image,ome-scheduler-image,alfred-image"

actual_names="$(${yq_bin} eval '.jobs.build-and-push-images.strategy.matrix.component | map(.name) | join(",")' "${dev_workflow}")"
actual_images="$(${yq_bin} eval '.jobs.build-and-push-images.strategy.matrix.component | map(.image) | join(",")' "${dev_workflow}")"
actual_targets="$(${yq_bin} eval '.jobs.docker-validation.strategy.matrix.image | join(",")' "${pr_workflow}")"

if [[ "${actual_names}" != "${expected_names}" ]]; then
  echo "dev image component names differ: ${actual_names}" >&2
  exit 1
fi
if [[ "${actual_images}" != "${expected_images}" ]]; then
  echo "dev image repositories differ: ${actual_images}" >&2
  exit 1
fi
if [[ "${actual_targets}" != "${expected_targets}" ]]; then
  echo "PR Docker targets differ: ${actual_targets}" >&2
  exit 1
fi

assert_yq '.env.IMAGE_ORG == "ome-projects"' "${dev_workflow}" \
  "dev images must publish under the public repository owner"
assert_yq '(.permissions | type) == "!!map" and (.permissions | length) == 0' "${dev_workflow}" \
  "the workflow must deny token permissions by default"
assert_yq '.jobs.build-and-push-images.permissions.contents == "read" and .jobs.build-and-push-images.permissions.packages == "write" and .jobs.build-and-push-images.permissions."id-token" == "write"' "${dev_workflow}" \
  "only the image publisher may receive package and OIDC permissions"
assert_yq '.jobs.publish-dev-charts.permissions.contents == "read" and .jobs.publish-dev-charts.permissions.packages == "write" and (.jobs.publish-dev-charts.permissions."id-token" == null)' "${dev_workflow}" \
  "the chart publisher must receive package permission without OIDC"
for unprivileged_job in verify-public-images verify-dev-charts notify; do
  assert_yq "(.jobs.\"${unprivileged_job}\".permissions | type) == \"!!map\" and (.jobs.\"${unprivileged_job}\".permissions | length) == 0" "${dev_workflow}" \
    "${unprivileged_job} must run without token permissions"
  assert_yq "([.jobs.\"${unprivileged_job}\".steps[].name] | contains([\"Log in to GitHub Container Registry\"])) == false" "${dev_workflow}" \
    "${unprivileged_job} must not authenticate to the registry"
done
assert_yq '.jobs.build-and-push-images.steps[] | select(.name == "Log in to GitHub Container Registry") | .with.password == "${{ secrets.GITHUB_TOKEN }}"' "${dev_workflow}" \
  "dev image publishing must use the repository-scoped GITHUB_TOKEN"
for required_step in "Sign image index" "Install Syft" "Generate signed per-platform SBOM attestations"; do
  assert_yq ".jobs.build-and-push-images.steps[] | select(.name == \"${required_step}\") | .name == \"${required_step}\"" "${dev_workflow}" \
    "dev image publisher is missing required step: ${required_step}"
done
if grep -Fq 'cosign attach sbom' "${dev_workflow}"; then
  echo "deprecated unsigned SBOM attachments must not be published" >&2
  exit 1
fi
sbom_attestation="$(${yq_bin} eval '.jobs.build-and-push-images.steps[] | select(.name == "Generate signed per-platform SBOM attestations") | .run' "${dev_workflow}")"
for required_fragment in \
  'linux/amd64' \
  'linux/arm64' \
  'registry:${IMAGE}@${digest}' \
  'cosign attest' \
  'https://spdx.dev/Document'; do
  if ! grep -Fq "${required_fragment}" <<<"${sbom_attestation}"; then
    echo "per-platform signed SBOM generation omits: ${required_fragment}" >&2
    exit 1
  fi
done
build_labels="$(${yq_bin} eval '.jobs.build-and-push-images.steps[] | select(.id == "build") | .with.labels' "${dev_workflow}")"
if ! grep -Fq 'org.opencontainers.image.source=https://github.com/${{ github.repository }}' <<<"${build_labels}" ||
  ! grep -Fq 'org.opencontainers.image.revision=${{ github.sha }}' <<<"${build_labels}"; then
  echo "published images must carry repository and revision provenance labels" >&2
  exit 1
fi
assert_yq '.jobs.verify-public-images.needs == "build-and-push-images"' "${dev_workflow}" \
  "dev images must be anonymously verified after publication"
assert_yq '.jobs.publish-dev-charts.needs == "build-and-push-images"' "${dev_workflow}" \
  "the first dispatch must create every image and chart package before visibility verification"
assert_yq '.jobs.verify-dev-charts.needs == "publish-dev-charts"' "${dev_workflow}" \
  "published dev charts must be anonymously verified"
assert_yq '.jobs.notify.needs | contains(["verify-public-images", "publish-dev-charts", "verify-dev-charts"])' "${dev_workflow}" \
  "the final summary must observe every publication verification gate"

chart_step="$(${yq_bin} eval '.jobs.publish-dev-charts.steps[] | select(.name == "Prepare SHA-pinned dev charts") | .run' "${dev_workflow}")"
if ! grep -Fq 'hack/prepare-dev-charts.sh' <<<"${chart_step}" ||
  ! grep -Fq 'dev-${SHORT_SHA}' <<<"${chart_step}"; then
  echo "dev chart publication does not invoke SHA-pinned preparation" >&2
  exit 1
fi

image_verification="$(${yq_bin} eval '.jobs.verify-public-images.steps[] | select(.name == "Verify anonymous image pulls") | .run' "${dev_workflow}")"
chart_verification="$(${yq_bin} eval '.jobs.verify-dev-charts.steps[] | select(.name == "Verify anonymous chart pulls") | .run' "${dev_workflow}")"
assert_yq '.jobs.verify-public-images.steps[] | select(.name == "Install cosign") | .uses == "sigstore/cosign-installer@v3"' "${dev_workflow}" \
  "anonymous verification must install cosign"
for required_fragment in \
  'cosign verify' \
  'cosign verify-attestation' \
  'https://spdx.dev/Document' \
  'GITHUB_WORKFLOW_REF' \
  'linux/amd64' \
  'linux/arm64'; do
  if ! grep -Fq "${required_fragment}" <<<"${image_verification}"; then
    echo "anonymous supply-chain verification omits: ${required_fragment}" >&2
    exit 1
  fi
done
if ! grep -Fq 'packages/container/${image}/settings' <<<"${image_verification}" ||
  ! grep -Fq 'package visibility is Public' <<<"${image_verification}"; then
  echo "anonymous image failures must explain the one-time GHCR visibility prerequisite" >&2
  exit 1
fi
if ! grep -Fq 'packages/container/charts%2F${chart}/settings' <<<"${chart_verification}" ||
  ! grep -Fq 'package visibility is Public' <<<"${chart_verification}"; then
  echo "anonymous chart failures must explain the one-time GHCR visibility prerequisite" >&2
  exit 1
fi

chart_summary="$(${yq_bin} eval '.jobs.publish-dev-charts.steps[] | select(.name == "Record chart provenance") | .run' "${dev_workflow}")"
final_summary="$(${yq_bin} eval '.jobs.notify.steps[] | select(.name == "Summary") | .run' "${dev_workflow}")"
version_step="$(${yq_bin} eval '.jobs.build-and-push-images.steps[] | select(.id == "version") | .run' "${dev_workflow}")"
for tag_consumer in "${version_step}" "${image_verification}" "${chart_step}" "${final_summary}"; do
  if ! grep -Fq 'dev-${SHORT_SHA}-amd64' <<<"${tag_consumer}"; then
    echo "express builds must use an amd64-specific immutable image tag" >&2
    exit 1
  fi
done
for chart in ome-alfred ome-crd ome-resources ome-scheduler; do
  chart_reference="oci://ghcr.io/ome-projects/charts/${chart}"
  if ! grep -Fq "${chart_reference}" <<<"${chart_summary}" ||
    ! grep -Fq "${chart_reference}" <<<"${final_summary}"; then
    echo "workflow summaries omit exact OCI reference for ${chart}" >&2
    exit 1
  fi
done
for release in ome-crd ome ome-scheduler ome-alfred; do
  if ! grep -Fq "helm upgrade --install ${release}" <<<"${final_summary}"; then
    echo "final summary omits install command for ${release}" >&2
    exit 1
  fi
done
if ! grep -Fq "echo '\`\`\`bash'" <<<"${final_summary}" ||
  ! grep -Fq "echo '\`\`\`'" <<<"${final_summary}"; then
  echo "final summary must render install commands in a Markdown code block" >&2
  exit 1
fi
if ! grep -Fq 'artifacts_verified=true' <<<"${final_summary}" ||
  ! grep -Fq 'if [[ "${artifacts_verified}" == "true"' <<<"${final_summary}"; then
  echo "install commands must be gated on successful image and chart verification" >&2
  exit 1
fi

docker_filter="$(${yq_bin} eval '.jobs.docker-changes.steps[] | select(.name == "Check changed paths") | .run' "${pr_workflow}")"
if ! grep -Fq 'dev-images\.yaml' <<<"${docker_filter}"; then
  echo "PR Docker validation does not trigger for the dev-images workflow" >&2
  exit 1
fi

while IFS= read -r dockerfile; do
  if [[ ! -f "${repo_root}/${dockerfile}" ]]; then
    echo "matrix Dockerfile does not exist: ${dockerfile}" >&2
    exit 1
  fi
done < <("${yq_bin}" eval '.jobs.build-and-push-images.strategy.matrix.component[].dockerfile' "${dev_workflow}")

echo "Verified five public dev images and four SHA-pinned Helm charts."
