#!/usr/bin/env bash
# Build the kernel and its test binaries from this checkout on an Ubuntu runner.
set -euo pipefail

sudo apt-get update
sudo apt-get install -y --no-install-recommends libxml2
tools="$RUNNER_TEMP/kernel-tools"
mkdir -p "$tools"
bazelisk="$tools/bazel"
curl --fail --silent --show-error --location --retry 3 \
  "https://github.com/bazelbuild/bazelisk/releases/download/v1.30.0/bazelisk-linux-$BAZELISK_ARCH" -o "$bazelisk"
printf '%s  %s\n' "$BAZELISK_SHA256" "$bazelisk" | sha256sum --check --strict
chmod 0755 "$bazelisk"

tag=$(sed -n 's/^tag=//p' patches/workerd/UPSTREAM)
git clone --depth 1 --single-branch --branch "$tag" https://github.com/cloudflare/workerd.git workerd
python3 scripts/workerd-patches.py import
python3 scripts/workerd-patches.py check

cd workerd
function bazel() {
  # Keep runner and Actions credentials out of repository rules and actions.
  env -i HOME="$HOME" PATH="$tools:/usr/bin:/bin" LANG=C.UTF-8 "$bazelisk" "$@"
}
bazel_startup=(--nosystem_rc --nohome_rc --output_user_root="$RUNNER_TEMP/kernel-bazel")
build_options=(--config=release --jobs=3 --nostamp --disk_cache="$RUNNER_TEMP/kernel-action-cache" \
  --remote_verify_downloads)

python3 - "$bazelisk" "$(bazel --version)" <<'PY'
import hashlib
import json
import os
from pathlib import Path
import sys

inputs = (".bazelversion", ".bazelrc", "MODULE.bazel", "MODULE.bazel.lock",
          "build/deps/llvm.MODULE.bazel", "build/deps/rust.MODULE.bazel")
record = {
    "bazeliskSha256": hashlib.file_digest(Path(sys.argv[1]).open("rb"), "sha256").hexdigest(),
    "bazelVersion": sys.argv[2],
    "config": "release",
    "stamped": False,
    "inputSha256": {name: hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in inputs},
}
Path(os.environ["RUNNER_TEMP"], "kernel-toolchain.json").write_text(json.dumps(record, sort_keys=True) + "\n")
PY

time bazel "${bazel_startup[@]}" build "${build_options[@]}" \
  //src/workerd/server:workerd //src/workerd/threaded:coworkerd-threaded \
  //src/workerd/threaded:coworkerd-threaded-fatal-test

# Each explicit upstream label corresponds to a test source modified by the series.
test_targets=(//src/workerd/server/factory/... //src/workerd/threaded/... \
  //src/workerd/io:actor-sqlite-test@ //src/workerd/io:actor-sqlite-test@all-autogates \
  //src/workerd/util:sqlite-test@ \
  //src/workerd/server:channel-token-test@ //src/workerd/server:channel-token-test@all-autogates)
test_status=0
if [[ ${KERNEL_COMPILE_TESTS_ONLY:-false} == true ]]; then
  time bazel "${bazel_startup[@]}" build "${build_options[@]}" --build_tests_only "${test_targets[@]}"
else
  # Run one test at a time because the threaded-server tests allocate several GiB of heap.
  test_started=$SECONDS
  bazel "${bazel_startup[@]}" test "${build_options[@]}" \
    --build_tests_only --local_test_jobs=1 --cache_test_results=no --test_output=errors \
    "${test_targets[@]}" || test_status=$?
  printf '### Bazel kernel tests\n\nElapsed after the executable build: %d s. Bazel exit: %d.\n\n' \
    "$((SECONDS - test_started))" "$test_status" | tee -a "$GITHUB_STEP_SUMMARY"
fi

# Flush the cache and free resources before the packaging and profiling steps.
bazel "${bazel_startup[@]}" shutdown
du -sh "$RUNNER_TEMP/kernel-action-cache"
exit "$test_status"
