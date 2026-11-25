#!/usr/bin/env bash
set -euo pipefail

control=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export CC=clang-19 CXX=clang++-19 CMAKE_BUILD_PARALLEL_LEVEL=3 WRANGLER_SEND_METRICS=false
sudo apt-get update
sudo apt-get install -y --no-install-recommends clang-19 libstdc++-14-dev cmake ninja-build libssl-dev
npm ci --prefix cli --registry=https://registry.npmjs.org
stage=build/kernel-ci
mkdir -p "$stage"
kernel=$(python3 scripts/fetch-kernel.py --arch "$KERNEL_ARCH")
bash scripts/build.sh
cmake --build build --target coworkerd-launcher

node() {
  if [[ $1 == tests/cluster/run.mjs && $# == 2 ]]; then
    command node "$@" deployment
  else
    command node "$@"
  fi
}

source <(sed -n '/^node tests\/cluster\/run.mjs /,$p' "$control/test-hosted-kernel.sh")
