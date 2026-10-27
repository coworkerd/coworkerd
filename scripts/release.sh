#!/usr/bin/env bash
# Build a Coworkerd release for this machine's Linux architecture.
#
#   scripts/release.sh <version> [--arch amd64|arm64] [--out DIR] [--no-image]
#
# Builds coworkerd (the node daemon), coworkerd-apiserver (the control plane, with the deployment tool) and
# coworkerd-launcher (the workerd processes' least-privilege environment) in a
# Linux toolchain container with the version written into the binaries (`coworkerd --version`), takes
# the workerd of the current patch series (not built here), and produces in OUT (default
# build/release/<version>):
#   coworkerd-<version>-linux-<arch>.tar.gz        bin/ (the node daemon, the control plane, the launcher, workerd,
#                                                  cowctl, and the system call allowlist of this
#                                                  workerd), lib/ (cowctl's Node.js code with
#                                                  the Wrangler adapter and its dependencies), runtime/,
#                                                  docker/ (compose, entry point, Dockerfile, the hello
#                                                  example), examples/, docs/ (install, quickstart, compat, errors,
#                                                  databases, durable-objects), INSTALL.md, VERSION, SHA256SUMS
#   coworkerd-<version>-linux-<arch>-image.tar.gz  the image coworkerd:<version> (`docker load -i`)
#   coworkerd-<version>-linux-<arch>.json          what was built from what, with every SHA-256
# then (re)writes OUT/SHA256SUMS over every asset present and prints the upload command. The other
# architecture is built the same way on a machine of that architecture (an amd64 Linux machine); its three assets (the
# package, the image and the record) are copied into OUT, and SHA256SUMS written again over every asset there, before
# the upload (SHA256SUMS lists only the assets present when it was written).
#
# Environment:
#   COWORKERD_BUILD_CONTAINER  the toolchain container (default coworkerd-capacity-linux), running, with this
#                              checkout at /repo and the CMake tree below
#   COWORKERD_BUILD_TREE       the CMake build tree inside it (default /work/coworkerd-build; scripts/toolchain/README.md)
#   COWORKERD_RELEASE_WORKERD  the workerd binary to ship (default per architecture below)
#   COWORKERD_IMAGE_BASE       the base image (Dockerfile BASE), e.g. a local tag of the pinned Debian image
#   COWORKERD_RELEASE_ALLOWLIST the directory scripts/syscall-allowlist.py wrote for this workerd:
#                              allowlist.json and the two coworkerd-syscalls-*.txt. Default
#                              build/syscall-allowlist/<the workerd's first 12 hex digits>. The allowlist
#                              names the workerd it was generated for, and this script refuses to package a
#                              pair that does not match.
#   COWORKERD_CLI_DEPENDENCIES a directory of cowctl's dependencies (node_modules) prepared before, else
#                              npm installs them (the registry must be reachable)
#   COWORKERD_BUILD_JOBS       parallel compile jobs (default 6)
# It builds: run one at a time on a machine (the toolchain container's tree is shared by every run).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION=${1:?usage: scripts/release.sh <version> [--arch amd64|arm64] [--out DIR] [--no-image]}
shift
[[ $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]] || { echo "invalid version: $VERSION" >&2; exit 2; }
case "$(uname -m)" in x86_64|amd64) ARCH=amd64 ;; arm64|aarch64) ARCH=arm64 ;; *) ARCH=unknown ;; esac
OUT="$ROOT/build/release/$VERSION"
IMAGE=true
while (($#)); do
  case $1 in
    --arch) ARCH=$2; shift 2 ;;
    --out) OUT=$2; shift 2 ;;
    --no-image) IMAGE=false; shift ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
[[ $ARCH == amd64 || $ARCH == arm64 ]] || { echo "unsupported architecture: $ARCH" >&2; exit 2; }
CONTAINER=${COWORKERD_BUILD_CONTAINER:-coworkerd-capacity-linux}
TREE=${COWORKERD_BUILD_TREE:-/work/coworkerd-build}
JOBS=${COWORKERD_BUILD_JOBS:-6}

# The workerd builds of the current patch series (upstream v1.20261012.1 + patches/workerd 0001-0008, 0010-0014), by
# architecture, each pinned by its SHA-256: built from the fork's branch (patches/README.md) on a Linux machine of that
# architecture, and put at the default path below or named by COWORKERD_RELEASE_WORKERD. A new series needs its own
# builds here.
case $ARCH in
  arm64) WORKERD_DEFAULT="$ROOT/build/workerd-linux-arm64/workerd"; WORKERD_SHA256=a0a8779adb5356893aaf37816793a8f10750745970bd98159163dc94a098349a ;;
  amd64) WORKERD_DEFAULT="$ROOT/build/workerd-linux-amd64/workerd"; WORKERD_SHA256=92d3648362d66a7ed4b2ab756518f09cb0b6d77fc1a60951c52cdbef61653456 ;;
esac
WORKERD=${COWORKERD_RELEASE_WORKERD:-$WORKERD_DEFAULT}
SERIES_DIR="$ROOT/patches/workerd"

sha256() { if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }
log() { printf '[release] %s\n' "$*" >&2; }

[[ -x $WORKERD ]] || { echo "workerd not found: $WORKERD (set COWORKERD_RELEASE_WORKERD)" >&2; exit 1; }
actual=$(sha256 "$WORKERD")
[[ $actual == "$WORKERD_SHA256" ]] || { echo "workerd $WORKERD is $actual, not the $ARCH build of the patch series ($WORKERD_SHA256)" >&2; exit 1; }
# The system call allowlist of the workerd this package ships: generated by the build
# (scripts/syscall-allowlist.py over scripts/syscall-profile.mjs's profiles of that same binary), and it says
# which workerd it belongs to -- a pair that does not match is refused here rather than at the first launch.
ALLOWLIST=${COWORKERD_RELEASE_ALLOWLIST:-$ROOT/build/syscall-allowlist/${WORKERD_SHA256:0:12}}
[[ -f $ALLOWLIST/allowlist.json ]] || { echo "no system call allowlist at $ALLOWLIST (scripts/syscall-allowlist.py; set COWORKERD_RELEASE_ALLOWLIST)" >&2; exit 1; }
for name in coworkerd-syscalls-startup.txt coworkerd-syscalls-steady.txt; do
  [[ -f $ALLOWLIST/$name ]] || { echo "$ALLOWLIST/$name is missing" >&2; exit 1; }
done
# The record names the workerd it was generated for, and the files the launcher reads are the ones it
# describes (their digests) and name that same workerd.
ALLOWLIST_SUMMARY=$(python3 "$ROOT/scripts/syscall-allowlist-check.py" "$ALLOWLIST" "$WORKERD_SHA256") || exit 1
log "system call allowlist: $ALLOWLIST_SUMMARY"

for page in install quickstart compat errors databases durable-objects; do
  [[ -f $ROOT/docs/guide/$page.md ]] || { echo "docs/guide/$page.md is missing" >&2; exit 1; }
done
command -v node >/dev/null && command -v npm >/dev/null || { echo "node and npm are needed to package cowctl" >&2; exit 1; }

# ---- Release metadata: the version, the source, the workerd the binary expects --------------------------
UPSTREAM_TAG=$(sed -n 's/^tag=//p' "$SERIES_DIR/UPSTREAM")
UPSTREAM_COMMIT=$(sed -n 's/^commit=//p' "$SERIES_DIR/UPSTREAM")
SERIES=$(grep -v '^#' "$SERIES_DIR/series" | sed '/^$/d')
SERIES_SHA256=$(cd "$SERIES_DIR" && cat $SERIES | { if command -v sha256sum >/dev/null; then sha256sum; else shasum -a 256; fi; } | cut -d' ' -f1)
SERIES_NAMES=$(printf '%s\n' "$SERIES" | sed 's/\.patch$//' | paste -sd, -)
COMMIT=$(git -C "$ROOT" rev-parse --short=12 HEAD)
# Uncommitted changes to what the binary is built from: the hash of their diff, untracked files included.
DIRTY=$(cd "$ROOT" && { git diff --full-index HEAD -- common node apiserver launcher CMakeLists.txt patches/kj-http-abort.diff; git ls-files --others --exclude-standard -- common node apiserver launcher | LC_ALL=C sort | while read -r f; do printf 'untracked %s\n' "$f"; cat "$f"; done; } |
  { if command -v sha256sum >/dev/null; then sha256sum; else shasum -a 256; fi; } | cut -c1-12)
EMPTY=$(printf '' | { if command -v sha256sum >/dev/null; then sha256sum; else shasum -a 256; fi; } | cut -c1-12)
SOURCE=$COMMIT
[[ $DIRTY == "$EMPTY" ]] || SOURCE="$COMMIT+$DIRTY"
HEADER="// Written by scripts/release.sh; read by common/support.h (the daemons' --version).
#define COWORKERD_VERSION \"$VERSION\"
#define COWORKERD_SOURCE \"$SOURCE\"
#define COWORKERD_WORKERD_UPSTREAM \"$UPSTREAM_TAG ($UPSTREAM_COMMIT)\"
#define COWORKERD_PATCH_SERIES \"$SERIES_NAMES\"
#define COWORKERD_PATCH_SERIES_SHA256 \"$SERIES_SHA256\"
"

# ---- Build in the toolchain container -------------------------------------------------------------------
log "building linux/$ARCH in $CONTAINER:$TREE (source $SOURCE)"
docker start "$CONTAINER" >/dev/null
[[ $(docker exec "$CONTAINER" uname -m) == "$( [[ $ARCH == amd64 ]] && echo x86_64 || echo aarch64)" ]] ||
  { echo "$CONTAINER does not run linux/$ARCH" >&2; exit 1; }
current=$(docker exec "$CONTAINER" cat "$TREE/generated/coworkerd-version.h" 2>/dev/null || true)
if [[ $current != "${HEADER%$'\n'}" ]]; then
  printf '%s' "$HEADER" | docker exec -i "$CONTAINER" bash -c "mkdir -p '$TREE/generated' && cat > '$TREE/generated/coworkerd-version.h'"
fi
started=$SECONDS
docker exec "$CONTAINER" bash -c "export PATH=/usr/lib/llvm-19/bin:\$PATH; cmake --build '$TREE' --target coworkerd coworkerd-apiserver coworkerd-launcher -j $JOBS" >&2
BUILD_SECONDS=$((SECONDS - started))

NAME="coworkerd-$VERSION-linux-$ARCH"
STAGE="$OUT/$NAME"
rm -rf "$STAGE"
mkdir -p "$STAGE/bin" "$STAGE/runtime" "$STAGE/docker/examples" "$STAGE/docs" "$STAGE/lib"
for binary in coworkerd coworkerd-apiserver coworkerd-launcher; do
  docker cp "$CONTAINER:$TREE/$binary" "$STAGE/bin/$binary" >/dev/null
  chmod 0755 "$STAGE/bin/$binary"
done
cp "$WORKERD" "$STAGE/bin/workerd"
chmod 0755 "$STAGE/bin/workerd"
# The allowlist beside the node daemon: that is where it looks for it (sandbox.syscalls.allowlistPath's default),
# and the record of how it was generated travels with it.
cp "$ALLOWLIST/coworkerd-syscalls-startup.txt" "$ALLOWLIST/coworkerd-syscalls-steady.txt" "$STAGE/bin/"
cp "$ALLOWLIST/allowlist.json" "$STAGE/bin/coworkerd-syscalls.json"
chmod 0644 "$STAGE/bin/coworkerd-syscalls-startup.txt" "$STAGE/bin/coworkerd-syscalls-steady.txt" "$STAGE/bin/coworkerd-syscalls.json"
VERSION_TEXT=$(docker exec "$CONTAINER" "$TREE/coworkerd" --version)
grep -q "^coworkerd $VERSION$" <<<"$VERSION_TEXT" || { echo "the built binary reports another version: $VERSION_TEXT" >&2; exit 1; }
APISERVER_VERSION_TEXT=$(docker exec "$CONTAINER" "$TREE/coworkerd-apiserver" --version)
grep -q "^coworkerd-apiserver $VERSION$" <<<"$APISERVER_VERSION_TEXT" || { echo "the built control plane reports another version: $APISERVER_VERSION_TEXT" >&2; exit 1; }

# ---- The package --------------------------------------------------------------------------------------------
cp "$ROOT/runtime/runtime.js" "$STAGE/runtime/runtime.js"
cp "$ROOT/docker/Dockerfile" "$ROOT/docker/docker-compose.yml" "$ROOT/docker/docker-compose.sandbox.yml" "$ROOT/docker/.env.example" "$STAGE/docker/"
cp "$ROOT/docker/entrypoint.sh" "$STAGE/docker/entrypoint.sh"
chmod 0755 "$STAGE/docker/entrypoint.sh"
cp -R "$ROOT/docker/examples/hello" "$STAGE/docker/examples/hello"
cp "$ROOT/docs/guide/install.md" "$STAGE/INSTALL.md"
for page in install quickstart compat errors databases durable-objects; do cp "$ROOT/docs/guide/$page.md" "$STAGE/docs/$page.md"; done
# The examples: hello-world and two official Cloudflare templates as they are (MIT, LICENSE beside them).
cp -R "$ROOT/examples" "$STAGE/examples"
find "$STAGE/examples" \( -name node_modules -o -name .wrangler -o -name dist \) -prune -exec rm -rf {} +

# ---- cowctl ------------------------------------------------------------------------------------------------
# A directory with its node_modules (not one file: the Wrangler adapter reads its pinned Wrangler from beside
# it): lib/cowctl (the package cli/ -- the command, the adapter, the lock and the dependencies the lock pins), run
# by bin/cowctl with the machine's Node.js (22.5 or newer). The dependencies are installed for Linux and for macOS of
# this architecture (esbuild runs the build when the project has no Wrangler of its own). The command never starts
# workerd: the workerd package's platform binaries are placeholders that say so, and sharp's natives (Wrangler's
# image service) are left out.
case $ARCH in amd64) CPU=x64 ;; arm64) CPU=arm64 ;; esac
mkdir -p "$STAGE/lib/cowctl/bin"
cp "$ROOT/cli/package.json" "$ROOT/cli/package-lock.json" "$ROOT/cli/adapter.mjs" "$STAGE/lib/cowctl/"
cp "$ROOT/cli/bin/cowctl.mjs" "$STAGE/lib/cowctl/bin/cowctl.mjs"
CLI_VERSION=$(node -p 'require(process.argv[1]).version' "$ROOT/cli/package.json")
[[ $CLI_VERSION == "$VERSION" ]] || { echo "cli/package.json says $CLI_VERSION, not $VERSION" >&2; exit 1; }
LOCK_SHA=$(sha256 "$ROOT/cli/package-lock.json")
DEPENDENCIES=${COWORKERD_CLI_DEPENDENCIES:-$OUT/.cli-dependencies-$ARCH-${LOCK_SHA:0:12}}
if [[ ! -f $DEPENDENCIES/.complete ]]; then
  log "installing cowctl's dependencies for linux and darwin ($CPU)"
  rm -rf "$DEPENDENCIES"
  for os in linux darwin; do
    mkdir -p "$DEPENDENCIES/$os"
    cp "$ROOT/cli/package.json" "$ROOT/cli/package-lock.json" "$DEPENDENCIES/$os/"
    (cd "$DEPENDENCIES/$os" && npm ci --omit=dev --ignore-scripts --no-audit --no-fund --os=$os --cpu=$CPU --cache "$DEPENDENCIES/npm-cache" >&2)
  done
  touch "$DEPENDENCIES/.complete"
fi
cp -R "$DEPENDENCIES/linux/node_modules" "$STAGE/lib/cowctl/node_modules"
# macOS's esbuild, and the workerd package of macOS (its binary becomes a placeholder too).
for package in "$DEPENDENCIES/darwin/node_modules/@esbuild/"* "$DEPENDENCIES/darwin/node_modules/@cloudflare/workerd-darwin-"*; do
  cp -R "$package" "$STAGE/lib/cowctl/node_modules/$(basename "$(dirname "$package")")/"
done
rm -rf "$STAGE/lib/cowctl/node_modules/@img/sharp-"*
for binary in "$STAGE/lib/cowctl/node_modules/@cloudflare/workerd-"*/bin/workerd; do
  printf '#!/bin/sh\n# Placeholder: cowctl never runs workerd (Wrangler only builds here).\necho "workerd is not part of cowctl; install Wrangler in your project to run wrangler dev" >&2\nexit 1\n' >"$binary"
  chmod 0755 "$binary"
done
cat >"$STAGE/bin/cowctl" <<'SH'
#!/bin/sh
# cowctl: lib/cowctl/bin/cowctl.mjs of this package, run with Node.js 22.5 or newer.
self=$0
while [ -L "$self" ]; do
  target=$(readlink "$self")
  case $target in /*) self=$target ;; *) self=$(dirname "$self")/$target ;; esac
done
here=$(cd "$(dirname "$self")" && pwd -P)
command -v node >/dev/null 2>&1 || { echo "cowctl: Node.js 22.5 or newer is needed (node is not on PATH)" >&2; exit 1; }
exec node "$here/../lib/cowctl/bin/cowctl.mjs" "$@"
SH
chmod 0755 "$STAGE/bin/cowctl" "$STAGE/lib/cowctl/bin/cowctl.mjs"
# The image tag the compose file starts by default.
sed -i.orig "s/COWORKERD_VERSION:-[0-9][0-9A-Za-z.-]*}/COWORKERD_VERSION:-$VERSION}/" "$STAGE/docker/docker-compose.yml"
sed -i.orig "s/^COWORKERD_VERSION=.*/COWORKERD_VERSION=$VERSION/" "$STAGE/docker/.env.example"
rm -f "$STAGE/docker/docker-compose.yml.orig" "$STAGE/docker/.env.example.orig"
printf '%s\n' "$VERSION_TEXT" >"$STAGE/VERSION"
(cd "$STAGE" && find . -type f ! -name SHA256SUMS | sed 's|^\./||' | LC_ALL=C sort | while read -r f; do printf '%s  %s\n' "$(sha256 "$f")" "$f"; done >SHA256SUMS)

# A reproducible tar file: sorted entries, the source commit's time, no owner names.
EPOCH=$(git -C "$ROOT" log -1 --format=%ct HEAD)
python3 - "$OUT" "$NAME" "$EPOCH" <<'PY'
import gzip, io, os, sys, tarfile
out, name, epoch = sys.argv[1], sys.argv[2], int(sys.argv[3])
root = os.path.join(out, name)
paths = []
for directory, dirs, files in os.walk(root):
    dirs.sort()
    for entry in sorted(dirs + files):
        paths.append(os.path.join(directory, entry))
paths.sort(key=lambda p: os.path.relpath(p, out))
buffer = io.BytesIO()
with tarfile.open(fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT) as tar:
    for path in [root] + paths:
        info = tar.gettarinfo(path, arcname=os.path.relpath(path, out))
        info.uid = info.gid = 0
        info.uname = info.gname = ""
        info.mtime = epoch
        info.mode = 0o755 if info.isdir() or (info.mode & 0o111) else 0o644
        if info.isfile():
            with open(path, "rb") as stream:
                tar.addfile(info, stream)
        else:
            tar.addfile(info)
with open(os.path.join(out, name + ".tar.gz"), "wb") as target:
    with gzip.GzipFile(filename="", mode="wb", fileobj=target, mtime=0, compresslevel=9) as stream:
        stream.write(buffer.getvalue())
PY

# ---- The image ----------------------------------------------------------------------------------------------
IMAGE_TAR=""
IMAGE_ID=""
if $IMAGE; then
  log "building the image coworkerd:$VERSION for linux/$ARCH"
  base=()
  [[ -n ${COWORKERD_IMAGE_BASE:-} ]] && base=(--build-arg "BASE=$COWORKERD_IMAGE_BASE")
  docker build --platform "linux/$ARCH" ${base[@]+"${base[@]}"} --build-arg "COWORKERD_VERSION=$VERSION" \
    -t "coworkerd:$VERSION" -f "$STAGE/docker/Dockerfile" "$STAGE" >&2
  IMAGE_ID=$(docker image inspect "coworkerd:$VERSION" --format '{{.Id}}')
  IMAGE_TAR="$OUT/$NAME-image.tar.gz"
  docker save "coworkerd:$VERSION" | gzip -n -6 >"$IMAGE_TAR.next"
  mv "$IMAGE_TAR.next" "$IMAGE_TAR"
  docker run --rm --entrypoint coworkerd "coworkerd:$VERSION" --version | grep -q "^coworkerd $VERSION$"
  docker run --rm --entrypoint coworkerd-apiserver "coworkerd:$VERSION" --version | grep -q "^coworkerd-apiserver $VERSION$"
fi

# ---- The record and the sums --------------------------------------------------------------------------------
python3 - "$OUT" "$NAME" "$ARCH" "$VERSION" "$SOURCE" "$SERIES_SHA256" "$SERIES_NAMES" "$UPSTREAM_TAG" "$UPSTREAM_COMMIT" \
    "$WORKERD" "$BUILD_SECONDS" "$IMAGE_ID" "$IMAGE_TAR" "${COWORKERD_IMAGE_BASE:-}" <<'PY'
import hashlib, json, os, sys
(out, name, arch, version, source, series_sha, series, tag, commit, workerd, seconds, image_id, image_tar, image_base) = sys.argv[1:]
def sha(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()
stage = os.path.join(out, name)
record = {
    "version": version, "architecture": arch, "source": source,
    "workerd": {"upstream": tag, "upstreamCommit": commit, "patchSeries": series.split(","), "patchSeriesSha256": series_sha,
                "from": workerd, "sha256": sha(os.path.join(stage, "bin/workerd"))},
    "binaries": {p: sha(os.path.join(stage, p)) for p in ("bin/coworkerd", "bin/coworkerd-apiserver", "bin/coworkerd-launcher", "bin/workerd", "runtime/runtime.js",
                                                         "bin/cowctl", "lib/cowctl/bin/cowctl.mjs", "lib/cowctl/adapter.mjs",
                                                         "lib/cowctl/package-lock.json")},
    # The system call allowlist of the workerd above: its files' digests and the workerd it names.
    "syscalls": (lambda record: {
        "workerdSha256": record["workerd"]["sha256"],
        "architecture": record["architecture"],
        "generatedAt": record["generatedAt"],
        "profiles": [{"run": q["run"], "sha256": q["sha256"]} for q in record["profiles"]],
        "startup": {"file": "bin/coworkerd-syscalls-startup.txt", "calls": record["startup"]["count"],
                    "sha256": sha(os.path.join(stage, "bin/coworkerd-syscalls-startup.txt"))},
        "steady": {"file": "bin/coworkerd-syscalls-steady.txt", "calls": record["steady"]["count"],
                   "sha256": sha(os.path.join(stage, "bin/coworkerd-syscalls-steady.txt"))},
        "record": {"file": "bin/coworkerd-syscalls.json",
                   "sha256": sha(os.path.join(stage, "bin/coworkerd-syscalls.json"))},
    })(json.load(open(os.path.join(stage, "bin/coworkerd-syscalls.json")))),
    "package": {"file": name + ".tar.gz", "bytes": os.path.getsize(os.path.join(out, name + ".tar.gz")), "sha256": sha(os.path.join(out, name + ".tar.gz"))},
    "image": None if not image_tar else {"tag": "coworkerd:" + version, "id": image_id, "base": image_base or "Dockerfile default",
                                         "file": os.path.basename(image_tar), "bytes": os.path.getsize(image_tar), "sha256": sha(image_tar)},
    "buildSeconds": int(seconds),
}
with open(os.path.join(out, name + ".json"), "w") as stream:
    json.dump(record, stream, indent=2)
    stream.write("\n")
assets = sorted(f for f in os.listdir(out) if f.startswith("coworkerd-" + version + "-linux-") and (f.endswith(".tar.gz") or f.endswith(".json")))
with open(os.path.join(out, "SHA256SUMS"), "w") as stream:
    for asset in assets:
        stream.write(f"{sha(os.path.join(out, asset))}  {asset}\n")
print(json.dumps(record))
PY
log "assets in $OUT:"
(cd "$OUT" && ls -l coworkerd-"$VERSION"-* SHA256SUMS | sed 's/^/[release]   /' >&2)
log "upload (once both architectures are in $OUT):"
log "  gh release create v$VERSION --repo coworkerd/coworkerd --title 'Coworkerd $VERSION' --notes-file <notes> $OUT/coworkerd-$VERSION-linux-*.tar.gz $OUT/coworkerd-$VERSION-linux-*.json $OUT/SHA256SUMS"
