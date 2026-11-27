#!/usr/bin/env bash
# Run both builds on one isolated Linux Docker host.
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
backing="$runner/backing"
name="coworkerd-log-io-$(basename "$runner" | tr -cd '[:alnum:]')"
loop=
backing_loop=
cleanup() {
  if mountpoint -q "$mountpoint"; then sudo umount "$mountpoint"; fi
  if sudo dmsetup info "$name" >/dev/null 2>&1; then sudo dmsetup remove "$name"; fi
  if [[ -n "$loop" ]]; then sudo losetup -d "$loop"; fi
  if mountpoint -q "$backing"; then sudo umount "$backing"; fi
  if [[ -n "$backing_loop" ]]; then sudo losetup -d "$backing_loop"; fi
  sudo rm -rf -- "$runner"
}
trap cleanup EXIT
mkdir "$mountpoint" "$backing"
sudo modprobe dm-delay || sudo dmsetup targets | awk '$1 == "delay" { found=1 } END { exit !found }'

# Give the delayed loop image a private ext4 filesystem and journal. Other
# services keep writing to the runner filesystem. Direct I/O and fully written
# image extents keep the loop devices from sharing the runner's page cache.
dd if=/dev/zero of="$runner/backing.img" bs=8M count=160 oflag=direct conv=fdatasync status=none
backing_loop=$(sudo losetup --find --show --direct-io=on "$runner/backing.img")
sudo mkfs.ext4 -q -E nodiscard,lazy_itable_init=0,lazy_journal_init=0 "$backing_loop"
sudo mount -o noatime "$backing_loop" "$backing"
sudo dd if=/dev/zero of="$backing/delayed.img" bs=8M count=96 oflag=direct conv=fdatasync status=none
loop=$(sudo losetup --find --show --direct-io=on "$backing/delayed.img")
sudo mkfs.ext4 -q -E nodiscard,lazy_itable_init=0,lazy_journal_init=0 "$loop"
sectors=$(sudo blockdev --getsz "$loop")
# Preserve the normal read and write paths. The block layer delays the real
# FLUSH requests, including those sent by ext4 for fdatasync.
sudo dmsetup create "$name" --table "0 $sectors delay $loop 0 0 $loop 0 0 $loop 0 20"
sudo mount -o noatime "/dev/mapper/$name" "$mountpoint"
cc -Wall -Wextra -Werror -O2 -shared -fPIC "$trace_source/trace.c" -ldl -o "$runner/trace.so"
{
  uname -a
  lscpu
  printf 'Runner image %s %s\n' "${ImageOS:-local}" "${ImageVersion:-local}"
  docker version --format 'Docker Engine {{.Server.Version}}'
  findmnt -T "$runner"
  findmnt -T "$backing/delayed.img"
  findmnt -T "$mountpoint"
  lsblk -o NAME,TYPE,SIZE,MOUNTPOINTS "$backing_loop" "$loop"
  sudo losetup --list --output NAME,DIO,BACK-FILE "$backing_loop"
  sudo losetup --list --output NAME,DIO,BACK-FILE "$loop"
  sudo dmsetup table "$name"
  cat "/sys/class/block/${loop##*/}/queue/write_cache"
  sha256sum "$baseline/coworkerd" "$candidate/coworkerd" "$kernel/workerd" "$kernel/coworkerd-threaded"
} >"$reports/runner.txt"

pair() {
  local tier=$1 log_root=$2
  local baseline_objects="$runner/baseline-$tier-objects" candidate_objects="$runner/candidate-$tier-objects"
  for side in baseline candidate; do
    local objects="$runner/$side-$tier-objects"
    sudo install -d -o 10001 -g 10001 -m 0755 "$log_root/$side" "$objects" "$objects/log"
    # A nested bind makes the otherwise empty object root appear initialized.
    printf 'coworkerd-do-root-1\n' | sudo tee "$objects/format" >/dev/null
  done
  node "$trace_source/measure.mjs" "$kernel" "$reports" "$tier" "$runner/trace.so" "$baseline" "$candidate" \
    "$log_root/baseline" "$log_root/candidate" "$baseline_objects" "$candidate_objects"
}

pair normal "$runner"
pair slow "$mountpoint"
node "$trace_source/verify.mjs" "$reports"
