"""Identity and layout shared by the kernel packaging and fetch commands."""

import hashlib
import json
from pathlib import Path, PurePosixPath
import platform
import re
import struct
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
PATCHES = ROOT / "patches" / "workerd"
RELEASES = "https://github.com/coworkerd/coworkerd/releases"
ARCHITECTURES = {"linux-x86_64": ("x86_64", 62), "linux-arm64": ("aarch64", 183)}
ALIASES = {"x86_64": "linux-x86_64", "amd64": "linux-x86_64",
           "aarch64": "linux-arm64", "arm64": "linux-arm64"}
BINARIES = {
    "workerd": "bazel-bin/src/workerd/server/workerd",
    "coworkerd-threaded": "bazel-bin/src/workerd/threaded/coworkerd-threaded",
}
ALLOWLIST = ("allowlist.json", "coworkerd-syscalls-startup.txt", "coworkerd-syscalls-steady.txt")
RECORD = "kernel.json"
FILES = (*BINARIES, *(f"allowlist/{name}" for name in ALLOWLIST))
MODES = {path: 0o755 if path in BINARIES else 0o644 for path in (*FILES, RECORD)}


def sha256(path, digest=None):
    if digest is None:
        digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def json_text(value):
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def architecture(value):
    result = value or ALIASES.get(platform.machine())
    if result not in ARCHITECTURES:
        raise ValueError("unsupported machine; specify --arch linux-x86_64 or linux-arm64")
    return result


def series_identity():
    upstream = dict(line.split("=", 1) for line in (PATCHES / "UPSTREAM").read_text().splitlines() if line)
    tag, commit = upstream["tag"], upstream["commit"]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", tag) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("invalid upstream tag or commit in patches/workerd/UPSTREAM")
    names = [line for line in (PATCHES / "series").read_text().splitlines() if line and not line.startswith("#")]
    if not names or len(names) != len(set(names)) or any(
        PurePosixPath(name).name != name or not name.endswith(".patch") for name in names
    ):
        raise ValueError("invalid patch names in patches/workerd/series")
    # Preserve the release.sh algorithm: hash the concatenated patch bytes in series order.
    digest = hashlib.sha256()
    for name in names:
        sha256(PATCHES / name, digest)
    return {"upstream": tag, "upstreamCommit": commit, "patchSeriesSha256": digest.hexdigest()}, names


def release_tag(identity):
    return f"kernel-{identity['upstream']}-{identity['patchSeriesSha256'][:12]}"


def archive_name(arch):
    return f"coworkerd-kernel-{arch}.tar.gz"


def pins(identity):
    path = PATCHES / "artifacts.json"
    record = json.loads(path.read_text())
    if any(record[key] != value for key, value in identity.items()):
        raise ValueError(f"kernel pins in {path.name} do not match this upstream and patch series")
    archives = record["archives"]
    if archives.keys() != ARCHITECTURES.keys() or any(
        value is not None and (not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value))
        for value in archives.values()
    ):
        raise ValueError(f"invalid archive pins in {path.name}")
    return archives


def check_binary(path, arch):
    expected = ARCHITECTURES[arch][1]
    if not path.is_file() or not path.stat().st_mode & 0o111:
        raise ValueError(f"{path} is not an executable file")
    with path.open("rb") as stream:
        header = stream.read(64)
    if (len(header) != 64 or header[:7] != b"\x7fELF\x02\x01\x01"
            or struct.unpack_from("<HH", header, 16) not in ((2, expected), (3, expected))):
        raise ValueError(f"{path} is not an executable ELF64 for {arch}")


def check_allowlist(directory, workerd_sha256, arch):
    record = json.loads((directory / ALLOWLIST[0]).read_text())
    if record["architecture"] != ARCHITECTURES[arch][0]:
        raise ValueError(f"{directory}: allowlist architecture does not match {arch}")
    for kind, name in zip(("startup", "steady"), ALLOWLIST[1:]):
        if record[kind]["file"] != name:
            raise ValueError(f"{directory}: expected {kind} allowlist file {name}")
    return subprocess.check_output(
        [sys.executable, str(ROOT / "scripts/syscall-allowlist-check.py"), str(directory), workerd_sha256],
        text=True,
    ).strip()
