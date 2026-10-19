#!/usr/bin/env python3
"""Checks that a generated system call allowlist belongs to a given workerd.

usage: syscall-allowlist-check.py <allowlist-directory> <workerd-sha256>

The allowlist of a release is generated from profiles of the very workerd the package ships
(scripts/syscall-allowlist.py), so three things have to agree: the workerd files the record names, the digest
given here (which must be among them), and the files the launcher actually reads -- which carry those digests
in their own header and whose digests the record states. A list may name more than one workerd: that is how
an upgrade is prepared, and a package may ship such a list as long as it covers its own binary. scripts/release.sh runs this before it packages anything; a pair that does not
match is refused there rather than at a launch (the launcher refuses it too, as a launch failure).

Prints one line of JSON for the build log, and exits non-zero with the reason when something does not agree.
"""
from __future__ import annotations
import hashlib
import json
import sys
from pathlib import Path


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            value.update(chunk)
    return value.hexdigest()


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit(__doc__.strip().splitlines()[2])
    directory, workerd = Path(sys.argv[1]), sys.argv[2]
    record = json.loads((directory / "allowlist.json").read_text())
    named = record["workerd"].get("sha256s") or [record["workerd"]["sha256"]]
    if workerd not in named:
        raise SystemExit(f"the allowlist in {directory} names workerd {[d[:12] for d in named]}, "
                         f"and this package ships {workerd[:12]}")
    for kind in ("startup", "steady"):
        name = record[kind]["file"]
        path = directory / name
        found = digest(path)
        if found != record[kind]["sha256"]:
            raise SystemExit(f"{name} is {found[:12]}, and allowlist.json says {record[kind]['sha256'][:12]}")
        header = f"# workerd-sha256 {workerd}"
        if header not in path.read_text():
            raise SystemExit(f"{name} does not name the workerd this package ships")
        if record[kind]["count"] < 1:
            raise SystemExit(f"{name} holds no call")
    print(json.dumps({"workerdSha256": workerd[:12], "workerdFiles": len(named), "architecture": record["architecture"],
                      "startup": record["startup"]["count"], "steady": record["steady"]["count"],
                      "profiles": [p["run"] for p in record["profiles"]], "generatedAt": record["generatedAt"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
