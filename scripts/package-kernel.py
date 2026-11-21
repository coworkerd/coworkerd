#!/usr/bin/env python3
"""Package both Bazel kernel executables and their generated syscall allowlist."""

import argparse
import gzip
import io
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

from kernel_artifacts import (
    ALLOWLIST, ARCHITECTURES, BINARIES, MODES, RECORD, ROOT, archive_name,
    architecture, check_allowlist, check_binary, json_text, release_tag, series_identity, sha256,
)


def package(args):
    arch = architecture(args.arch)
    identity, series = series_identity()
    tag = release_tag(identity)
    source = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
    checked = subprocess.check_output(
        [sys.executable, str(ROOT / "scripts/workerd-patches.py"), "check", "--checkout", str(args.checkout)],
        text=True,
    )
    import json
    tree = json.loads(checked)["actualTree"]
    paths = {name: args.checkout / path for name, path in BINARIES.items()}
    for binary in paths.values():
        check_binary(binary, arch)
    paths.update({f"allowlist/{name}": args.allowlist / name for name in ALLOWLIST})
    hashes = {name: sha256(path) for name, path in paths.items()}
    check_allowlist(args.allowlist, hashes["workerd"], arch)
    record = json_text({**identity, "patchSeries": series, "releaseTag": tag, "architecture": arch,
                        "coworkerdCommit": source, "workerdTree": tree, "files": hashes}).encode()

    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    archive_path = out / archive_name(arch)
    with tempfile.TemporaryDirectory(dir=out) as scratch:
        pending = Path(scratch) / archive_path.name
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
                    with paths[name].open("rb") as stream:
                        info.size = paths[name].stat().st_size
                        archive.addfile(info, stream)
        pending.replace(archive_path)
        sums = Path(scratch) / "SHA256SUMS"
        assets = [out / archive_name(published_arch) for published_arch in sorted(ARCHITECTURES)]
        sums.write_text("".join(f"{sha256(asset)}  {asset.name}\n" for asset in assets if asset.is_file()))
        sums.replace(out / sums.name)
    print(f"{sha256(archive_path)}  {archive_path}")
    print(f"Release tag: {tag}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arch", choices=ARCHITECTURES, help="Linux target (default: this machine's architecture)")
    parser.add_argument("--checkout", type=Path, default=ROOT / "workerd", help="patched workerd checkout with Bazel outputs")
    parser.add_argument("--allowlist", required=True, type=Path, help="directory written by syscall-allowlist.py")
    parser.add_argument("--out", required=True, type=Path, help="local release directory for the archive and SHA256SUMS")
    try:
        package(parser.parse_args())
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"package-kernel: {error}\n")


if __name__ == "__main__":
    main()
