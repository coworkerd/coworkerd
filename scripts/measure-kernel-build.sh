#!/usr/bin/env bash
# Sample the runner filesystem containing the Bazel output, including other uses of that filesystem.
set -euo pipefail

started=$SECONDS
disk=$(df -B1 --output=size,used,avail,target "$RUNNER_TEMP" | tail -n 1)
read -r capacity start_used start_free mount <<< "$disk"
printf 'Filesystem %s: capacity %s B, used %s B, available %s B\n' "$mount" "$capacity" "$start_used" "$start_free"
df -h "$PWD" "$RUNNER_TEMP"
uname -sm
nproc
free -m

samples=$(mktemp /dev/shm/coworkerd-kernel-disk.XXXXXXXX)
printf '%s %s\n' "$start_used" "$start_free" > "$samples"
(
  while sleep 5; do
    df -B1 --output=used,avail "$RUNNER_TEMP" | tail -n 1 >> "$samples"
  done
) &
sampler=$!

report() {
  local status=$?
  kill "$sampler" 2>/dev/null || true
  wait "$sampler" 2>/dev/null || true
  df -B1 --output=used,avail "$RUNNER_TEMP" | tail -n 1 >> "$samples"
  awk -v capacity="$capacity" -v start="$start_used" -v free="$start_free" -v elapsed="$((SECONDS - started))" \
    -v status="$status" '
      NR == 1 || $1 > peak { peak = $1 }
      NR == 1 || $2 < minimum { minimum = $2 }
      END {
        print "### Runner disk and elapsed time"
        printf "\n| Capacity | Initially available | Peak used | Increase at peak | Minimum available | Elapsed | Exit |\n"
        print "| --- | --- | --- | --- | --- | --- | --- |"
        printf "| %.2f GiB | %.2f GiB | %.2f GiB | %.2f GiB | %.2f GiB | %d s | %d |\n\n", \
          capacity/2^30, free/2^30, peak/2^30, (peak-start)/2^30, minimum/2^30, elapsed, status
      }
    ' "$samples" | tee -a "$GITHUB_STEP_SUMMARY"
  unlink "$samples"
}
trap report EXIT

"$@"
