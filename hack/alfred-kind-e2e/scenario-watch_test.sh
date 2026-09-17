#!/usr/bin/env bash
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp_dir="$(mktemp -d)"
watch_pid=""
cleanup() {
  if [[ -n "${watch_pid}" ]]; then kill "${watch_pid}" 2>/dev/null || true; wait "${watch_pid}" 2>/dev/null || true; fi
  rm -rf -- "${tmp_dir}"
}
trap cleanup EXIT

# Load only the runner's pure preflight helper definitions. No cluster command
# or scenario setup is evaluated by this focused lifecycle regression.
eval "$(sed '/^scenario=/,$d' "${script_dir}/scenario.sh")"
declare -F assert_annotation_watch_alive >/dev/null || {
  echo 'runner cannot reject an annotation watch that exits after its first request' >&2; exit 1;
}

# A real live child is accepted; the same child after a clean early exit must
# be rejected even though its capture already contains one request UUID.
mkfifo "${tmp_dir}/release"
bash -c 'printf "%s\n" '\''{"metadata":{"annotations":{"ome.io/migration-request-v1-01234567-89ab-4cde-8fab-0123456789ab":"{}"}}}'\'' >"$1"; IFS= read -r release <"$2"' \
  _ "${tmp_dir}/watch.jsonl" "${tmp_dir}/release" &
watch_pid=$!
attempt=0
while [[ ! -s "${tmp_dir}/watch.jsonl" ]] && ((attempt < 100)); do
  sleep 0.02
  attempt=$((attempt + 1))
done
[[ -s "${tmp_dir}/watch.jsonl" ]] || { echo 'test watcher failed to produce its first request' >&2; exit 1; }
assert_annotation_watch_alive "${watch_pid}" "${tmp_dir}/watch.stderr"
printf 'stop\n' >"${tmp_dir}/release"
dead_pid="${watch_pid}"
wait "${watch_pid}"
watch_pid=""
[[ "$(jq -s '[.[].metadata.annotations | keys[]] | unique | length' "${tmp_dir}/watch.jsonl")" == 1 ]]
if assert_annotation_watch_alive "${dead_pid}" "${tmp_dir}/watch.stderr" >"${tmp_dir}/output" 2>&1; then
  echo 'truncated one-request capture survived early watcher exit' >&2; exit 1
fi
grep -Fq 'request-count evidence is incomplete' "${tmp_dir}/output" || {
  echo 'early watch exit lacked a diagnostic explaining incomplete evidence' >&2; exit 1;
}
for deadline in 30 360; do
  timeout="$(annotation_watch_timeout_seconds "${deadline}")"
  ((timeout > deadline + 30 + 60 + 5 + 1)) || {
    echo 'annotation watch timeout does not cover the full observation budget' >&2; exit 1;
  }
done

# Bounded script contract: the real request, migration, and drain loops must
# invoke the tested liveness guard before reading snapshots. Also require a
# guard immediately before evidence construction. Live runs cover integration.
awk '
  /^while \(\( SECONDS < request_deadline \)\); do/ {
    loops++; getline
    if ($0 !~ /assert_annotation_watch_alive/) exit 1
  }
  /^evidence=/ {
    getline
    if ($0 !~ /assert_annotation_watch_alive/) exit 1
    sealed=1
  }
  END { if (loops != 3 || !sealed) exit 1 }
' "${script_dir}/scenario.sh" || {
  echo 'annotation watch liveness is not guarded through evidence sealing' >&2; exit 1;
}
echo 'scenario annotation-watch regression tests passed'
