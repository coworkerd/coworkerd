# Toolchain image

`Dockerfile` is the Linux toolchain image (Debian trixie with Node 22, clang 19, libc++ 19, CMake and Bazelisk) that
`scripts/release.sh` builds the Linux binaries in; workerd can be built with Bazel in it too. Its base
(`node:22-trixie`) and the Debian packages are not pinned, so building it today gives newer versions of them.

`scripts/release.sh` runs its build in a container of this image (`COWORKERD_BUILD_CONTAINER`, default
`coworkerd-capacity-linux`) with this checkout at `/repo` and a CMake tree configured with Ninja and clang 19
(`COWORKERD_BUILD_TREE`, default `/work/coworkerd-build`). Setting both up from the repository root:

```bash
docker build -t coworkerd-toolchain scripts/toolchain
docker run -d --init --name coworkerd-capacity-linux -v "$PWD:/repo:ro" -v coworkerd-toolchain-work:/work coworkerd-toolchain tail -f /dev/null
docker exec coworkerd-capacity-linux bash -c 'apt-get update -qq && apt-get install -y -qq --no-install-recommends ninja-build'
docker exec coworkerd-capacity-linux bash -c 'export PATH=/usr/lib/llvm-19/bin:$PATH; cmake -S /repo -B /work/coworkerd-build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/lib/llvm-19/bin/clang -DCMAKE_CXX_COMPILER=/usr/lib/llvm-19/bin/clang++'
```

The container holds nothing but its build tree and can be removed and set up again at any time. The paths of the build
tree and of the Cap'n Proto sources end up in the daemons (KJ's assertions name their source files), so two builds
are comparable byte for byte only when both use the same paths.
