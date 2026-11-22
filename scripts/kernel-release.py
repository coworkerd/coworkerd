#!/usr/bin/env python3
"""Verify hosted kernel outputs, unpacking only the known archive members."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile

from kernel_artifacts import (
    ALLOWLIST, ARCHITECTURES, BINARIES, archive_name, check_allowlist, check_install,
    hosted_release_tag, json_text, metadata, release_tag, run_identity, series_identity, sha256, unpack,
)


def verify(path, directory, arch, expected):
    with tarfile.open(path, mode="r|gz") as archive:
        raw, files = metadata(archive, {**expected, "architecture": arch})
        unpack(archive, directory, raw)
    record = json.loads(raw)
    check_install(directory, raw, files, arch)
    identities = (record["upstreamCommit"], record["patchSeriesSha256"], record["workerdTree"])
    if (not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", record["upstream"])
            or not all(re.fullmatch(r"[0-9a-f]+", value) for value in identities)
            or tuple(map(len, identities)) != (40, 64, 40)
            or record["releaseTag"] != release_tag(record)):
        raise ValueError(f"{path.name}: invalid kernel source identity")

    allowlist = directory / "allowlist"
    summary = json.loads((allowlist / ALLOWLIST[0]).read_text())
    digests = {files[name] for name in BINARIES}
    observed = {profile["workerdSha256"] for profile in summary["profiles"]}
    if observed != digests or set(summary["workerd"]["sha256s"]) != digests:
        raise ValueError(f"{path.name}: syscall profiles must cover these exact two binaries")
    check_allowlist(allowlist, files["coworkerd-threaded"], arch)
    return record


def bundle(args, expected, run):
    checksums = []
    records = []
    with tempfile.TemporaryDirectory(prefix="kernel-release-") as scratch:
        for arch in sorted(ARCHITECTURES):
            name = archive_name(arch)
            archive = args.directory / name
            records.append(verify(archive, Path(scratch) / arch, arch, expected))
            checksums.append(f"{sha256(archive)}  {name}\n")
    first, second = records
    shared = ("upstream", "upstreamCommit", "patchSeriesSha256", "patchSeries", "workerdTree")
    if any(first[key] != second[key] for key in shared):
        raise ValueError("kernel archives were built from different source trees or patch series")

    sums = args.directory / "SHA256SUMS"
    content = "".join(checksums)
    if args.mode == "assemble":
        sums.write_text(content)
    elif sums.read_text() != content:
        raise ValueError("SHA256SUMS does not match both kernel archives")

    tag = hosted_release_tag(first, run)
    lines = ["### Verified Linux kernel archives", "",
             f"Source commit `{args.commit}` · workerd tree `{first['workerdTree']}`", "",
             f"Upstream `{first['upstream']}` at `{first['upstreamCommit']}`", "",
             f"Patch series SHA-256 `{first['patchSeriesSha256']}`", "",
             f"[Build run]({run['url']}) · Pre-release tag `{tag}`", "", "```text", content.rstrip(), "```", ""]
    report = "\n".join(lines)
    print(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(report + "\n")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            print(f"tag={tag}", file=output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("unpack", "assemble", "verify"))
    parser.add_argument("directory", type=Path, help="directory containing kernel archives")
    parser.add_argument("--commit", required=True)
    parser.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID"))
    parser.add_argument("--attempt", default=os.environ.get("GITHUB_RUN_ATTEMPT"))
    parser.add_argument("--workflow-commit", default=os.environ.get("GITHUB_WORKFLOW_SHA"))
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--arch", choices=ARCHITECTURES)
    parser.add_argument("--destination", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.commit):
        parser.error("--commit must be a full lowercase Git SHA")
    try:
        run = run_identity(args.repository or "", args.run_id or "", args.attempt or "", args.workflow_commit or "")
        expected = {"githubActions": run, "coworkerdCommit": args.commit}
        if args.mode == "unpack":
            if args.arch is None or args.destination is None:
                parser.error("unpack requires --arch and --destination")
            identity, names = series_identity(Path("patches/workerd"))
            record = verify(args.directory / archive_name(args.arch), args.destination, args.arch,
                            {**expected, **identity, "patchSeries": names})
            print(json_text(record), end="")
        else:
            bundle(args, expected, run)
    except (KeyError, TypeError, ValueError, EOFError, OSError, tarfile.TarError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"kernel-release: {error}\n")


if __name__ == "__main__":
    main()
