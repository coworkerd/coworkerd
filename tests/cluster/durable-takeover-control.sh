#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"
kernel=${1:?Usage: tests/cluster/durable-takeover-control.sh KERNEL_DIR EVIDENCE_DIR}
out=${2:?Usage: tests/cluster/durable-takeover-control.sh KERNEL_DIR EVIDENCE_DIR}
mkdir "$out"
test -z "$(docker ps -q)"
scratch=$(mktemp -d)
source=node/durable-log.c++
cp "$source" "$scratch/durable-log.c++"
mkdir "$scratch/release"
cp build/coworkerd build/coworkerd-apiserver build/coworkerd-launcher "$scratch/release"
restore() {
  cp "$scratch/durable-log.c++" "$source"
  cp "$scratch/release/coworkerd" build/coworkerd
  rm -rf "$scratch"
}
trap restore EXIT

# Rebuild only the node in this disposable checkout. Keep the release daemon
# separately and run it with the same kernel and cluster fixture afterwards.
python3 - <<'PY'
from pathlib import Path

path = Path('node/durable-log.c++')
source = path.read_text()
before_queue = '  pendingLogAppends.push_back({std::move(payload), record.nodeSeq, flight.peers.empty()});'
before_append = '    if (fd < 0) return fail(EBADF, 0, false);\n    for (const auto& append : batch) {'
assert source.count(before_queue) == source.count(before_append) == 1
source = source.replace(before_queue, '''  if (request && record.kind == LogRecordKind::FRAMES && !flight.peers.empty()) {
    answer(channel, request, ACK_OK);
    flight.request = 0;
  }
''' + before_queue)
source = source.replace(before_append, '''    if (fd < 0) return fail(EBADF, 0, false);
    if (!batch.empty()) std::this_thread::sleep_for(std::chrono::milliseconds(250));
    for (const auto& append : batch) {''')
path.write_text(source)
PY

git rev-parse HEAD > "$out/source.txt"
git diff -- "$source" > "$out/early-answer.patch"
cmake --build build --target coworkerd
sha256sum build/coworkerd "$scratch/release/coworkerd" "$kernel/workerd" "$kernel/coworkerd-threaded" > "$out/binaries.txt"

if COWORKERD_CLUSTER_BIN_DIR="$root/build" node tests/cluster/run.mjs "$kernel" durable-threaded-takeover \
    2>&1 | tee "$out/early-answer.log"; then
  echo 'The early-answer control unexpectedly passed' >&2
  exit 1
fi
grep -Eq 'takeover lost acknowledged writes: [0-9]' "$out/early-answer.log"

cp "$scratch/durable-log.c++" "$source"
test -z "$(docker ps -q)"
COWORKERD_CLUSTER_BIN_DIR="$scratch/release" node tests/cluster/run.mjs "$kernel" durable-threaded-takeover \
  2>&1 | tee "$out/release.log"
