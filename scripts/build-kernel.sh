#!/usr/bin/env bash
# Build and test the kernel from this checkout on an Ubuntu runner.
set -euo pipefail

sudo apt-get update
sudo apt-get install -y --no-install-recommends libxml2
tools="$RUNNER_TEMP/kernel-tools"
mkdir -p "$tools"
bazel="$tools/bazel"
curl --fail --silent --show-error --location --retry 3 \
  "https://github.com/bazelbuild/bazelisk/releases/download/v1.30.0/bazelisk-linux-$BAZELISK_ARCH" -o "$bazel"
printf '%s  %s\n' "$BAZELISK_SHA256" "$bazel" | sha256sum --check --strict
chmod 0755 "$bazel"

tag=$(sed -n 's/^tag=//p' patches/workerd/UPSTREAM)
git clone --depth 1 --single-branch --branch "$tag" https://github.com/cloudflare/workerd.git workerd
python3 scripts/workerd-patches.py import
python3 scripts/workerd-patches.py check

cd workerd
export PATH="$tools:$PATH"
bazel_startup=(--nosystem_rc --nohome_rc --output_user_root="$RUNNER_TEMP/kernel-bazel")
build_options=(--config=release --jobs=3 --nostamp --disk_cache="$RUNNER_TEMP/kernel-action-cache" \
  --remote_verify_downloads)

python3 - "$bazel" <<'PY'
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

inputs = (".bazelversion", ".bazelrc", "MODULE.bazel", "MODULE.bazel.lock",
          "build/deps/llvm.MODULE.bazel", "build/deps/rust.MODULE.bazel")
record = {
    "bazeliskSha256": hashlib.file_digest(Path(sys.argv[1]).open("rb"), "sha256").hexdigest(),
    "bazelVersion": subprocess.check_output([sys.argv[1], "--version"], text=True).strip(),
    "config": "release",
    "stamped": False,
    "inputSha256": {name: hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in inputs},
}
Path(os.environ["RUNNER_TEMP"], "kernel-toolchain.json").write_text(json.dumps(record, sort_keys=True) + "\n")
PY

time bazel "${bazel_startup[@]}" build "${build_options[@]}" \
  //src/workerd/server:workerd //src/workerd/threaded:coworkerd-threaded \
  //src/workerd/threaded:coworkerd-threaded-fatal-test

# Reuse the release build's outputs. Run one test at a time because the
# threaded-server tests start children that allocate several GiB of heap.
# Each explicit upstream label corresponds to a test source modified by the series.
test_started=$SECONDS
test_status=0
bazel "${bazel_startup[@]}" test "${build_options[@]}" \
  --build_tests_only --local_test_jobs=1 --cache_test_results=no --test_output=errors \
  //src/workerd/server/factory/... //src/workerd/threaded/... \
  //src/workerd/io:actor-sqlite-test@ //src/workerd/io:actor-sqlite-test@all-autogates \
  //src/workerd/util:sqlite-test@ \
  //src/workerd/server:channel-token-test@ //src/workerd/server:channel-token-test@all-autogates \
  || test_status=$?
printf '### Bazel kernel tests\n\nElapsed after the executable build: %d s. Bazel exit: %d.\n\n' \
  "$((SECONDS - test_started))" "$test_status" | tee -a "$GITHUB_STEP_SUMMARY"
du -sh "$RUNNER_TEMP/kernel-action-cache"
exit "$test_status"
