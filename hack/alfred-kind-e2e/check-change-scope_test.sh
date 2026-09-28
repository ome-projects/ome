#!/usr/bin/env bash
set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
checker="${dir}/check-change-scope.sh"

allow() {
  if ! printf '%s\0' "$@" | bash "${checker}"; then
    echo 'Alfred-owned changes were rejected' >&2
    exit 1
  fi
}

reject() {
  if printf '%s\0' "$@" | bash "${checker}"; then
    echo 'Out-of-scope changes were accepted' >&2
    exit 1
  fi
}

allow cmd/alfred/README.md pkg/alfred/engine/dispatcher.go \
  pkg/alfred/simulator/go.mod charts/ome-alfred/values.yaml \
  config/alfred/role.yaml hack/alfred-kind-e2e/scenario.sh \
  .github/workflows/alfred-simulator.yml dockerfiles/alfred.Dockerfile \
  oeps/0008-alfred-gpu-cluster-caretaker/README.md
allow 'cmd/alfred/a file.md' $'pkg/alfred/a\nfile_test.go'

for path in \
  pkg/controller/v1beta1/workload/ops/migrate.go \
  pkg/controller/v1beta1/irstatus/codec.go \
  pkg/apis/ome/v1beta1/inferencereplica_types.go \
  scheduler/pkg/scheduler.go charts/ome-resources/values.yaml \
  go.mod go.sum .github/workflows/pr-validation.yml \
  pkg/alfred-other/file.go cmd/alfred.go \
  docs/superpowers/plan.md; do
  reject "${path}"
done

# A rename from another owner's package remains out of scope: callers supply
# both paths with git diff --no-renames. An allowed path cannot hide a refusal.
reject pkg/alfred/new.go pkg/controller/v1beta1/workload/old.go
reject pkg/controller/v1beta1/workload/old.go pkg/alfred/new.go
reject $'pkg/controller/v1beta1/workload/bad\ncmd/alfred/good.go'

if printf '%s' 'cmd/alfred/README.md' | bash "${checker}"; then
  echo 'Unterminated input was accepted as a complete diff' >&2
  exit 1
fi
if printf '\0' | bash "${checker}"; then
  echo 'Empty path was accepted' >&2
  exit 1
fi
if bash "${checker}" unexpected </dev/null; then
  echo 'Unexpected arguments were ignored' >&2
  exit 1
fi
bash "${checker}" </dev/null
echo 'Alfred change-scope checks passed'
