#!/usr/bin/env python3
"""Install the checkout's pinned Linux kernel from a public release or a local directory."""

import argparse
import fcntl
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

from kernel_artifacts import (
    ARCHITECTURES, RELEASES, ROOT, archive_name, architecture, check_install, metadata,
    pins, release_tag, series_identity, unpack, verify_archive,
)

CACHE = ROOT / ".cache" / "kernel"
INSTALL = ROOT / "build" / "kernel"


def install(args, arch, expected, digest, asset, download_tag):
    destination = INSTALL / download_tag / arch / digest
    cached = CACHE / digest / asset
    if args.installed and (not destination.is_dir() or not cached.is_file()):
        return

    INSTALL.mkdir(parents=True, exist_ok=True)
    with (INSTALL / ".fetch.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with tempfile.TemporaryDirectory(dir=INSTALL) as scratch:
            stage = Path(scratch)
            download = stage / asset
            if args.local_dir is not None:
                shutil.copyfile(args.local_dir / asset, download)
            elif not cached.is_file():
                url = f"{RELEASES}/download/{download_tag}/{asset}"
                subprocess.run(["curl", "--disable", "--fail", "--location", "--silent", "--show-error",
                                "--proto", "=https", "--proto-redir", "=https", "--retry", "3",
                                "--connect-timeout", "15", "--output", str(download), url], check=True)
            source = download if download.is_file() else cached
            verify_archive(source, digest)
            with tarfile.open(source, "r:gz") as archive:
                record, files = metadata(archive, expected)
                if destination.exists():
                    check_install(destination, record, files, arch)
                else:
                    extracted = stage / "extracted"
                    unpack(archive, extracted, record)
                    check_install(extracted, record, files, arch)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    extracted.rename(destination)
            if source == download:
                cached.parent.mkdir(parents=True, exist_ok=True)
                # The cache may be on a different filesystem from the build tree.
                with tempfile.TemporaryDirectory(dir=cached.parent) as pending:
                    copy = shutil.copyfile(download, Path(pending) / asset)
                    copy.replace(cached)
    if args.json:
        print(json.dumps({"path": str(destination), "archive": {
            "releaseTag": download_tag, "archiveAsset": asset, "archiveSha256": digest,
        }}))
    else:
        print(destination)


def fetch(args):
    arch = architecture(args.arch)
    identity, series = series_identity()
    tag = release_tag(identity)
    asset = archive_name(arch)
    if args.release_tag:
        # The series tag can be computed before a changed series has any matching pins.
        print(tag)
        return
    if args.installed and not any(INSTALL.glob(f"{tag}*/{arch}")):
        return
    archives, download_tag = pins(identity)
    digest = archives[arch]
    if digest is None:
        if args.installed:
            return
        raise ValueError(f"no published kernel archive is pinned for {arch} ({download_tag}); "
                         "see 'Building the kernel from source' in CONTRIBUTING.md")
    if args.sha256:
        print(digest)
        return
    expected = {**identity, "patchSeries": series, "releaseTag": tag, "architecture": arch}
    install(args, arch, expected, digest, asset, download_tag)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arch", choices=ARCHITECTURES, help="Linux target (default: this machine's architecture)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--local-dir", type=Path, help="read the archive from a local release directory")
    mode.add_argument("--installed", action="store_true", help="print an existing verified install path without downloading")
    mode.add_argument("--sha256", action="store_true", help="print the pinned archive SHA-256")
    mode.add_argument("--release-tag", action="store_true", help="print the release tag for the current series")
    parser.add_argument("--json", action="store_true", help="report the verified installation path and archive provenance as JSON")
    args = parser.parse_args()
    if args.json and (args.sha256 or args.release_tag):
        parser.error("--json requires a verified installation")
    try:
        fetch(args)
    except (OSError, EOFError, ValueError, KeyError, tarfile.TarError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"fetch-kernel: {error}\n")


if __name__ == "__main__":
    main()
