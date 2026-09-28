#!/usr/bin/env bash
set -euo pipefail

# Consume git diff --name-only -z --no-renames output. This is an opt-in gate
# for Alfred-only changes, not a restriction on other contributors' PRs.
if (($# != 0)); then
  echo 'Usage: git diff --name-only -z --no-renames BASE...HEAD | bash check-change-scope.sh' >&2
  exit 1
fi

result=0
path=''
while IFS= read -r -d '' path; do
  case "${path}" in
    cmd/alfred/* | pkg/alfred/* | config/alfred/* | charts/ome-alfred/* | \
      hack/alfred-kind-e2e/* | oeps/0008-alfred-gpu-cluster-caretaker/* | \
      .github/workflows/alfred-simulator.yml | dockerfiles/alfred.Dockerfile)
      ;;
    *)
      printf 'Outside Alfred ownership; separate approval required: %q\n' "${path}" >&2
      result=1
      ;;
  esac
done
if [[ -n "${path}" ]]; then
  echo 'Incomplete diff: expected NUL-terminated paths' >&2
  result=1
fi
exit "${result}"
