#!/usr/bin/env bash
# Build, profile and test a Coworkerd checkout on its native Linux runner.
set -euo pipefail

control=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export CC=clang-19 CXX=clang++-19 CMAKE_BUILD_PARALLEL_LEVEL=3 WRANGLER_SEND_METRICS=false

bash "$control/build-kernel.sh"

stage=build/kernel-ci
raw="$stage/raw"
fatal_test="$PWD/workerd/bazel-bin/src/workerd/threaded/coworkerd-threaded-fatal-test"
test -x "$fatal_test"
mkdir -p "$raw/allowlist"
install -m 0755 workerd/bazel-bin/src/workerd/server/workerd "$raw/workerd"
install -m 0755 workerd/bazel-bin/src/workerd/threaded/coworkerd-threaded "$raw/coworkerd-threaded"
touch "$raw/allowlist/coworkerd-syscalls-"{startup,steady}.txt
sudo apt-get install -y --no-install-recommends clang-19 libstdc++-14-dev cmake ninja-build libssl-dev
npm ci --prefix cli --registry=https://registry.npmjs.org
bash scripts/build.sh -DCOWORKERD_TEST_WORKERD=

node "$control/profile-kernel.mjs" "$raw" "$stage/traces"
python3 "$control/summarise-kernel-profile.py" "$stage/traces" "$raw" "$stage/profiles"
profiles=()
for profile in "$stage/profiles/"*.json; do
  profiles+=(--profile "$profile")
done
python3 "$control/syscall-allowlist.py" \
  --workerd "$raw/coworkerd-threaded" --out "$stage/allowlist" "${profiles[@]}"

python3 "$control/package-kernel.py" --github-run --toolchain "$RUNNER_TEMP/kernel-toolchain.json" \
  --arch "$KERNEL_ARCH" --source "$PWD" --checkout workerd \
  --workerd "$raw/workerd" --threaded "$raw/coworkerd-threaded" --allowlist "$stage/allowlist" --out "$stage/archive"
(cd "$stage/archive" && sha256sum --check --strict SHA256SUMS)
kernel="$PWD/$stage/unpacked"
python3 "$control/kernel-release.py" unpack "$stage/archive" --commit "$KERNEL_COMMIT" \
  --arch "$KERNEL_ARCH" --destination "$kernel"
bash scripts/build.sh -DCOWORKERD_TEST_WORKERD="$kernel/workerd" -DCOWORKERD_TEST_JUNIT="$PWD/build/test-results.xml"
cmake --build build --target check
python3 - <<'PY'
import os
import xml.etree.ElementTree as ET

cases = ET.parse("build/test-results.xml").getroot().findall(".//testcase")
failed = sum(case.find("failure") is not None or case.find("error") is not None for case in cases)
skipped = sum(case.find("skipped") is not None for case in cases)
missing = {"node-log-redaction", "workerd-configuration", "node-child-memory"} - {case.get("name") for case in cases}
summary = f"CTest: {len(cases)} tests, {failed} failed, {skipped} skipped."
print(summary)
with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
    print(summary + "\n", file=output)
if missing or failed or skipped:
    raise SystemExit(f"Expected all unit tests to pass with zero skips. Missing tests: {sorted(missing)}")
PY
if [[ ${FULL_CLUSTER:-false} == true ]]; then
  node tests/cluster/run.mjs "$kernel" 2>&1 | tee "$stage/cluster.log"
fi
COWORKERD_CLUSTER_FATAL_TEST_BINARY="$fatal_test" \
  node tests/cluster/run.mjs "$kernel" fatal-attribution 2>&1 | tee -a "$stage/cluster.log"

{
  printf '### Cluster workloads\n\n'
  awk '/^PASS / || / executed, / || /^Cluster run including / { print "- " $0 }' "$stage/cluster.log"
  printf '\n'
} >> "$GITHUB_STEP_SUMMARY"
