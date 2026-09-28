#!/usr/bin/env bash
set -euo pipefail

threshold="${1-50}"
if [[ ! "${threshold}" =~ ^[0-9]+([.][0-9]+)?$ ]] ||
   ! awk -v minimum="${threshold}" 'BEGIN { exit (minimum > 100) }'; then
  printf 'Invalid coverage threshold: %s (expected 0 to 100)\n' "${threshold}" >&2
  exit 1
fi

printf '\n---------- Coverage Summary ----------\n'
coverage=()
for part in cmd pkg internal; do
  profile="coverage-${part}.out"
  if ! report="$(go tool cover -func="${profile}")"; then
    printf 'Failed to read coverage profile: %s\n' "${profile}" >&2
    exit 1
  fi

  # Only the summary row counts; function names may also contain "total".
  if ! value="$(printf '%s\n' "${report}" | awk '
    $1 == "total:" {
      count++
      if (NF != 3 || $2 != "(statements)" || $3 !~ /^[0-9]+([.][0-9]+)?%$/) {
        invalid = 1
      } else {
        value = substr($3, 1, length($3) - 1)
        if (value + 0 > 100) invalid = 1
      }
    }
    END {
      if (count != 1 || invalid) exit 1
      print value
    }
  ')"; then
    printf 'Invalid coverage summary in %s: expected one total percentage from 0 to 100\n' "${profile}" >&2
    exit 1
  fi
  coverage+=("${value}")

  printf '\n%s Coverage:\n' "${part}"
  printf '%s\n' "${report}" | awk '$1 == "total:" || $NF != "100.0%"'
done

printf '\nTotal Coverage:\nCMD: %s%%\nPKG: %s%%\nInternal: %s%%\n' "${coverage[@]}"
# Compare the average as printed, to two decimals: the unrounded float can
# land just below a threshold the printed value meets (50.00 vs 49.99999...).
awk -v cmd="${coverage[0]}" -v pkg="${coverage[1]}" \
  -v internal="${coverage[2]}" -v minimum="${threshold}" '
  BEGIN {
    average = sprintf("%.2f", (cmd + pkg + internal) / 3) + 0
    printf "\nAverage Coverage: %.2f%%\n", average
    if (average < minimum) {
      printf "Average coverage %.2f%% is below threshold of %s%%\n", average, minimum
      exit 1
    }
  }
'
