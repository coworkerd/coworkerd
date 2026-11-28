#!/usr/bin/env python3
"""Package both Bazel kernel executables and their generated syscall allowlist."""

import argparse
import fcntl
import gzip
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

from kernel_artifacts import (
    ALLOWLIST, ARCHITECTURES, BINARIES, MODES, PROFILED, RECORD, ROOT, archive_name,
    architecture, check_allowlist, check_binary, json_text, release_tag, run_identity, series_identity, sha256,
)


def package(args):
    arch = architecture(args.arch)
    patches = args.source / "patches/workerd"
    identity, series = series_identity(patches)
    tag = release_tag(identity)
    source = subprocess.check_output(["git", "-C", str(args.source), "rev-parse", "HEAD"], text=True).strip()
    checked = subprocess.check_output(
        [sys.executable, str(args.source / "scripts/workerd-patches.py"), "check", "--checkout", str(args.checkout)],
        text=True,
    )
    tree = json.loads(checked)["actualTree"]
    provenance = {}
    if args.github_run:
        provenance["githubActions"] = run_identity(
            os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_RUN_ID"],
            os.environ["GITHUB_RUN_ATTEMPT"], os.environ["GITHUB_WORKFLOW_SHA"],
        )
    if args.toolchain is not None:
        provenance["toolchain"] = json.loads(args.toolchain.read_text())
    paths = {name: args.checkout / path for name, path in BINARIES.items()}
    if args.workerd is not None:
        paths["workerd"] = args.workerd
    if args.threaded is not None:
        paths["coworkerd-threaded"] = args.threaded
    for binary in paths.values():
        check_binary(binary, arch)
    paths.update({f"allowlist/{name}": args.allowlist / name for name in ALLOWLIST})
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    archive_path = out / archive_name(arch)
    with tempfile.TemporaryDirectory(dir=out) as scratch:
        stage = Path(scratch)
        (stage / "allowlist").mkdir()
        for name, path in paths.items():
            if not path.is_file():
                raise ValueError(f"{path} is not a regular file")
            shutil.copyfile(path, stage / name)
        hashes = {name: sha256(stage / name) for name in paths}
        check_allowlist(stage / "allowlist", hashes[PROFILED[0]], arch)
        record = json_text({**identity, **provenance, "patchSeries": series, "releaseTag": tag, "architecture": arch,
                            "coworkerdCommit": source, "workerdTree": tree, "files": hashes}).encode()

        pending = stage / archive_path.name
        with pending.open("wb") as target, gzip.GzipFile(
            filename="", mode="wb", fileobj=target, mtime=0, compresslevel=9
        ) as compressed, tarfile.open(fileobj=compressed, mode="w|") as archive:
            for name in (RECORD, *paths):
                info = tarfile.TarInfo(name)
                info.mode = MODES[name]
                if name == RECORD:
                    info.size = len(record)
                    archive.addfile(info, io.BytesIO(record))
                else:
                    with (stage / name).open("rb") as stream:
                        info.size = (stage / name).stat().st_size
                        archive.addfile(info, stream)
        digest = sha256(pending)
        with (out / ".package.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            sums = stage / "SHA256SUMS"
            assets = [out / archive_name(published_arch) for published_arch in sorted(ARCHITECTURES)]
            sums.write_text("".join(f"{digest if asset == archive_path else sha256(asset)}  {asset.name}\n"
                                    for asset in assets if asset == archive_path or asset.is_file()))
            pending.replace(archive_path)
            sums.replace(out / sums.name)
    print(f"{digest}  {archive_path}")
    print(f"Release tag: {tag}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arch", choices=ARCHITECTURES, help="Linux target (default: this machine's architecture)")
    parser.add_argument("--source", type=Path, default=ROOT, help="Coworkerd checkout containing the patch series")
    parser.add_argument("--checkout", type=Path, default=ROOT / "workerd", help="patched workerd checkout with Bazel outputs")
    parser.add_argument("--github-run", action="store_true", help="record provenance from the GitHub Actions environment")
    parser.add_argument("--toolchain", type=Path, help="JSON record of the kernel build toolchain")
    parser.add_argument("--workerd", type=Path, help="path to the built workerd (default: the Bazel output in --checkout)")
    parser.add_argument("--threaded", type=Path, help="path to the built coworkerd-threaded (default: the Bazel output in --checkout)")
    parser.add_argument("--allowlist", required=True, type=Path, help="directory written by syscall-allowlist.py")
    parser.add_argument("--out", required=True, type=Path, help="local release directory for the archive and SHA256SUMS")
    try:
        package(parser.parse_args())
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"package-kernel: {error}\n")


if __name__ == "__main__":
    main()
