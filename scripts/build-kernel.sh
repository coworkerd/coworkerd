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
time bazel --output_user_root="$RUNNER_TEMP/kernel-bazel" build --config=release --jobs=3 \
  //src/workerd/server:workerd //src/workerd/threaded:coworkerd-threaded

# Reuse the release build's outputs. Run one test at a time because the
# threaded-server tests start children that allocate several GiB of heap.
# Each explicit upstream label corresponds to a test source modified by the series.
test_started=$SECONDS
test_status=0
bazel --output_user_root="$RUNNER_TEMP/kernel-bazel" test --config=release --jobs=3 \
  --build_tests_only --local_test_jobs=1 --cache_test_results=no --test_output=errors \
  //src/workerd/server/factory/... //src/workerd/threaded/... \
  //src/workerd/io:actor-sqlite-test@ //src/workerd/io:actor-sqlite-test@all-autogates \
  //src/workerd/server:channel-token-test@ //src/workerd/server:channel-token-test@all-autogates \
  || test_status=$?
printf '### Bazel kernel tests\n\nElapsed after the executable build: %d s. Bazel exit: %d.\n\n' \
  "$((SECONDS - test_started))" "$test_status" | tee -a "$GITHUB_STEP_SUMMARY"
exit "$test_status"
