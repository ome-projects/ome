#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
checker="${1:-${repo_root}/hack/check-coverage.sh}"
test_dir="$(mktemp -d)"
trap 'rm -rf -- "${test_dir}"' EXIT
mkdir -p "${test_dir}/bin"

# Supply go tool cover output without requiring a build or changing real profiles.
cat >"${test_dir}/bin/go" <<'GO'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  'tool cover -func=coverage-cmd.out'|'tool cover -func=coverage-pkg.out'|'tool cover -func=coverage-internal.out')
    profile="${3#-func=}"
    cat "${profile}"
    if [[ -e "${profile}.fail" ]]; then
      echo 'go tool cover failed' >&2
      exit 2
    fi
    ;;
  *) echo "unexpected go invocation: $*" >&2; exit 2 ;;
esac
GO
chmod +x "${test_dir}/bin/go"

profile() {
  printf 'example/file.go:1:\tfunction\t0.0%%\ntotal:\t(statements)\t%s%%\n' "$2" >"${test_dir}/coverage-$1.out"
}

reset_profiles() {
  rm -f "${test_dir}"/*.fail
  profile cmd 34.4
  profile pkg 83.3
  profile internal 71.3
}

check() {
  local name="$1" expected="$2" message="$3" threshold="$4" output status
  if output="$(cd "${test_dir}" && PATH="${test_dir}/bin:${PATH}" bash "${checker}" "${threshold}" 2>&1)"; then
    status=0
  else
    status=$?
  fi
  if [[ "${expected}" == pass && "${status}" != 0 ]] ||
     [[ "${expected}" == fail && "${status}" == 0 ]] ||
     [[ "${output}" != *"${message}"* ]]; then
    printf 'FAIL: %s (exit %s, expected %s with %s)\n%s\n' "${name}" "${status}" "${expected}" "${message}" "${output}" >&2
    exit 1
  fi
  printf 'PASS: %s\n' "${name}"
}

reset_profiles
printf 'example/pairing.go:281:\ttotalServing\t100.0%%\n' >>"${test_dir}/coverage-pkg.out"
check 'function containing total does not corrupt the average' pass 'Average Coverage: 63.00%' 50
check 'coverage exactly at the threshold passes' pass 'Average Coverage: 63.00%' 63
check 'coverage below the threshold fails' fail 'below threshold' 63.01
profile cmd 34.3
profile pkg 95.1
profile internal 20.6
check 'average that rounds to the threshold passes' pass 'Average Coverage: 50.00%' 50

for part in cmd pkg internal; do profile "${part}" 100.0; done
check 'fully covered profiles pass' pass 'Average Coverage: 100.00%' 100
for part in cmd pkg internal; do profile "${part}" 0.0; done
check 'zero coverage is valid with a zero threshold' pass 'Average Coverage: 0.00%' 0

for part in cmd pkg internal; do
  reset_profiles
  rm "${test_dir}/coverage-${part}.out"
  check "missing ${part} profile fails" fail "coverage-${part}.out" 50
done

reset_profiles
touch "${test_dir}/coverage-pkg.out.fail"
check 'go tool failure fails even with a valid summary on stdout' fail 'go tool cover failed' 50

reset_profiles
printf 'example/file.go:1:\ttotalServing\t100.0%%\n' >"${test_dir}/coverage-pkg.out"
check 'missing summary fails' fail 'Invalid coverage summary' 50
profile pkg 83.3
printf 'total:\t(statements)\t83.3%%\n' >>"${test_dir}/coverage-pkg.out"
check 'duplicate summaries fail' fail 'Invalid coverage summary' 50
for value in NaN -1 101 83.3junk; do
  profile pkg "${value}"
  check "invalid coverage ${value} fails" fail 'Invalid coverage summary' 50
done
printf 'total:\t(statements)\t83.3\n' >"${test_dir}/coverage-pkg.out"
check 'summary without percent sign fails' fail 'Invalid coverage summary' 50
printf 'total:\tother\t83.3%%\n' >"${test_dir}/coverage-pkg.out"
check 'malformed summary fails' fail 'Invalid coverage summary' 50

reset_profiles
for threshold in '' invalid -1 101; do
  check "invalid threshold '${threshold}' fails" fail 'Invalid coverage threshold' "${threshold}"
done
