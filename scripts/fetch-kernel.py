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
    ARCHITECTURES, BINARIES, FILES, MODES, RECORD, RELEASES, ROOT, archive_name,
    architecture, check_allowlist, check_binary, json_text, pins, release_tag, series_identity, sha256,
)

CACHE = ROOT / ".cache" / "kernel"
INSTALL = ROOT / "build" / "kernel"


def verify_archive(path, expected):
    actual = sha256(path)
    if actual != expected:
        raise ValueError(f"{path.name}: SHA-256 mismatch (expected {expected}, got {actual})")


def metadata(archive, expected):
    member = archive.next()
    if member is None or member.name != RECORD or not member.isfile() or member.size > 65536:
        raise ValueError(f"archive must start with its {RECORD} build record")
    with archive.extractfile(member) as stream:
        data = stream.read()
    record = json.loads(data)
    if any(record[key] != value for key, value in expected.items()) or set(record["files"]) != set(FILES):
        raise ValueError("kernel build record does not match this checkout, architecture or layout")
    return data, record["files"]


def check_install(directory, record, files, arch):
    if (directory / RECORD).read_bytes() != record:
        raise ValueError(f"{directory / RECORD} does not match the verified archive")
    for name, digest in files.items():
        if sha256(directory / name) != digest:
            raise ValueError(f"{directory / name} does not match the verified archive")
    for name in BINARIES:
        check_binary(directory / name, arch)
    check_allowlist(directory / "allowlist", files["workerd"], arch)


def unpack(archive, directory, record):
    (directory / "allowlist").mkdir(parents=True)
    (directory / RECORD).write_bytes(record)
    remaining = set(FILES)
    while (member := archive.next()) is not None:
        if member.name not in remaining or not member.isfile():
            raise ValueError(f"unexpected archive entry {member.name!r}")
        remaining.remove(member.name)
        path = directory / member.name
        with archive.extractfile(member) as source, path.open("xb") as target:
            shutil.copyfileobj(source, target)
        path.chmod(MODES[member.name])
    if remaining:
        raise ValueError(f"missing archive entries: {', '.join(sorted(remaining))}")


def install(args, arch, expected, digest, asset, url):
    destination = INSTALL / expected["releaseTag"] / arch / digest
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
    print(destination)


def fetch(args):
    arch = architecture(args.arch)
    identity, series = series_identity()
    tag = release_tag(identity)
    asset = archive_name(arch)
    url = f"{RELEASES}/download/{tag}/{asset}"
    if args.release_tag:
        print(tag)
        return
    if args.installed and not (INSTALL / tag / arch).is_dir():
        return
    digest = pins(identity)[arch]
    if digest is None:
        if args.installed:
            return
        raise ValueError(f"no published kernel archive is pinned for {arch} ({tag}); "
                         "see 'Building the kernel from source' in CONTRIBUTING.md")
    if args.sha256:
        print(digest)
        return
    expected = {**identity, "patchSeries": series, "releaseTag": tag, "architecture": arch}
    install(args, arch, expected, digest, asset, url)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arch", choices=ARCHITECTURES, help="Linux target (default: this machine's architecture)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--local-dir", type=Path, help="read the archive from a local release directory")
    mode.add_argument("--installed", action="store_true", help="print an existing verified install path without downloading")
    mode.add_argument("--sha256", action="store_true", help="print the pinned archive SHA-256")
    mode.add_argument("--release-tag", action="store_true", help="print the release tag for the current series")
    try:
        fetch(parser.parse_args())
    except (OSError, EOFError, ValueError, KeyError, tarfile.TarError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"fetch-kernel: {error}\n")


if __name__ == "__main__":
    main()
