#!/usr/bin/env bash
# Build both kernel executables from this checkout on an Ubuntu runner.
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
