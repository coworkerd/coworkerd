#!/usr/bin/env bash
# Run all four cells on one isolated Linux Docker host.
set -euo pipefail

if [[ $# != 4 ]]; then
  echo 'usage: run.sh BASELINE_BIN CANDIDATE_BIN PUBLISHED_KERNEL OUTPUT_DIR' >&2
  exit 2
fi
baseline=$(realpath "$1")
candidate=$(realpath "$2")
kernel=$(realpath "$3")
mkdir -p "$4"
reports=$(realpath "$4")
runner=$(mktemp -d)
trace_source=$(cd -- "$(dirname -- "$0")" && pwd)
mountpoint="$runner/slow"
name="coworkerd-log-io-$(basename "$runner" | tr -cd '[:alnum:]')"
loop=
cleanup() {
  if mountpoint -q "$mountpoint"; then sudo umount "$mountpoint"; fi
  if sudo dmsetup info "$name" >/dev/null 2>&1; then sudo dmsetup remove "$name"; fi
  if [[ -n "$loop" ]]; then sudo losetup -d "$loop"; fi
  rm -rf "$runner"
}
trap cleanup EXIT
mkdir "$mountpoint"
sudo modprobe dm-delay || sudo dmsetup targets | awk '$1 == "delay" { found=1 } END { exit !found }'

trunc_file="$runner/log-volume.img"
truncate -s 768M "$trunc_file"
loop=$(sudo losetup --find --show "$trunc_file")
sudo mkfs.ext4 -q -E lazy_itable_init=0,lazy_journal_init=0 "$loop"
sectors=$(sudo blockdev --getsz "$loop")
# dm-delay forwards both data and FLUSH requests to the backing block device.
# Only the FLUSH is delayed. The SQLite WAL, probe and other services are on
# the usual volumes.
sudo dmsetup create "$name" --table "0 $sectors delay $loop 0 0 $loop 0 0 $loop 0 20"
sudo mount -o noatime "/dev/mapper/$name" "$mountpoint"
cc -Wall -Wextra -Werror -O2 -shared -fPIC "$trace_source/trace.c" -ldl -o "$runner/trace.so"
{
  uname -a
  lscpu
  printf 'Runner image %s %s\n' "${ImageOS:-local}" "${ImageVersion:-local}"
  docker version --format 'Docker Engine {{.Server.Version}}'
  findmnt -T "$mountpoint"
  lsblk -o NAME,TYPE,SIZE,MOUNTPOINTS "$loop"
  sudo dmsetup table "$name"
  cat "/sys/class/block/${loop##*/}/queue/write_cache"
  sha256sum "$baseline/coworkerd" "$candidate/coworkerd" "$kernel/workerd" "$kernel/coworkerd-threaded"
} >"$reports/runner.txt"

cell() {
  local side=$1 tier=$2 binaries=$3 root=$4
  local path="$root/$side"
  sudo install -d -o 10001 -g 10001 -m 0755 "$path"
  COWORKERD_CLUSTER_BIN_DIR="$binaries" node "$trace_source/measure.mjs" "$kernel" \
    "$reports/$side-$tier.jsonl" "$path" "$runner/trace.so"
}

cell baseline normal "$baseline" "$runner"
cell candidate normal "$candidate" "$runner"
cell candidate slow "$candidate" "$mountpoint"
cell baseline slow "$baseline" "$mountpoint"
node "$trace_source/verify.mjs" "$reports"
