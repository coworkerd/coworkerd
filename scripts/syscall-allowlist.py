#!/usr/bin/env python3
"""Generates the system call allowlist of a workerd build from profiles of that build.

This is a step of the build (docs/guide/install.md, "System call allowlist"): a release ships the allowlist of the
workerd in the same package, never a list written into the code, because the set belongs to those bytes and
that toolchain -- following upstream, changing the compiler or turning on another build option can change it.
scripts/syscall-profile.mjs produces the profiles; scripts/release.sh
checks that the allowlist names the workerd it ships and records both digests.

usage: syscall-allowlist.py --workerd FILE [--workerd FILE ...] --out DIR --profile P.json [--profile Q.json ...]
                            [--cc cc] [--relax CALL[,CALL...]] [--drop CALL[,CALL...]]

Writes into DIR:
  coworkerd-syscalls-startup.txt  what the launcher reads (--allow-file): this architecture's numbers, one
                                  per line; everything else is a comment, among them the digest of every
                                  workerd the list belongs to (one `# workerd-sha256` line each) and the
                                  number-to-name table the Server uses when it reports a call outside the
                                  list. Giving --workerd more than once is how an upgrade is prepared: a
                                  list that covers the workerd in place and the one about to replace it can
                                  be installed before the binary is replaced (docs/guide/install.md), and a
                                  launch is refused unless the list covers the bytes it runs. The profiles
                                  must belong to one of the named files
  coworkerd-syscalls-steady.txt   the same for the steady state (workerd's in-process tightening); not installed
                                  by the launcher, since a filter that survives the execv must allow execve
  allowlist.json                  both sets with every call's evidence (which run's which window, how often),
                                  the always-allowed entries with their reason, and the two files' digests

The sets come from the measured profiles only -- every call seen in any run, so one quiet repetition cannot
narrow the list -- plus the calls below, which no profile reliably shows and which the kernel or the launcher
can raise at any time. `--relax` adds a call with the reason "added by the operator" (for a call attributed
from the observe mode's reports, docs/guide/install.md); `--drop` leaves one out, which is how an artificial
denial is made to check the enforce mode.
"""
from __future__ import annotations
import argparse
import glob
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Calls no window reliably samples, with the reason recorded in allowlist.json. The first six were found while the
# least-privilege environment was designed (two of them by a process the filter ended). The last three are only
# ever used before workerd is running, so they are in the startup set alone.
ALWAYS = {
    "restart_syscall": "the kernel restarts an interrupted call through it",
    "rt_sigreturn": "the return path of every signal handler",
    "exit": "the exit path of a thread",
    "exit_group": "the exit path of the process",
    "accept4": "the Server opens a new connection on the inherited socket per generation and reconnect",
    "sched_yield": "V8's platform worker threads call it under contention; no profile window sampled one",
    # The steady-state filter found this one. V8 starts a platform worker thread when it first needs it,
    # which is often after the process is serving (a cold load), and glibc creates a thread with clone3 where
    # the kernel has it. The two numbers are the same operation, so a set that holds one and not the other only
    # depends on when the window happened to fall; both are always allowed, and a serving process may make a
    # thread either way (what the filter is there to stop -- execve above all -- stays out of the steady set).
    "clone": "a thread is created with it; V8 starts a platform worker when it first needs one, after the process serves",
    "clone3": "glibc's thread creation on a kernel that has it, the same operation as clone (v49)",
    "getrandom": "a tenant asks for entropy while the process serves (crypto.getRandomValues, randomUUID, a Map's "
                 "seed); whether a profile window holds one depends on what the profile's Workers do (v49)",
}
STARTUP_ONLY_ALWAYS = {
    "execve": "the launcher installs the filter, then execve's the payload",
    "seccomp": "the in-process tightening (v47) installs the steady filter with it",
    "sendmsg": "the launcher sends its record, and in the observe mode the notification listener, to the "
               "Server after the filter is in force",
}
# Where the names come from: every call name these headers mention is offered to the compiler below, which
# resolves the ones this architecture has (including a name the header defines through another macro,
# __NR3264_*, and the multiarch location Debian puts the architecture's own header in). A name of another
# architecture simply does not resolve and is left out.
HEADER_PATTERNS = ("/usr/include/asm/unistd*.h", "/usr/include/*-linux-gnu*/asm/unistd*.h",
                   "/usr/include/asm-generic/unistd.h")


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            value.update(chunk)
    return value.hexdigest()


def headers() -> list[str]:
    found = []
    for pattern in HEADER_PATTERNS:
        found.extend(sorted(glob.glob(pattern)))
    return found


def candidate_names() -> list[str]:
    """Every name the headers mention, as candidates for the compiler to resolve."""
    names: set[str] = set()
    for header in headers():
        text = Path(header).read_text(errors="replace")
        names.update(re.findall(r"__NR_(\w+)", text))
        names.update(re.findall(r"#define\s+__NR3264_(\w+)", text))
    if not names:
        raise SystemExit("no system call header found: " + ", ".join(HEADER_PATTERNS))
    return sorted(n for n in names if not n.startswith("3264_") and n not in ("syscalls", "arch_specific_syscall"))


def numbers(compiler: str) -> dict[str, int]:
    """name -> number for this architecture, resolved by the compiler (never by reading the macros)."""
    names = candidate_names()
    source = ["#include <sys/syscall.h>", "#include <stdio.h>", "int main(void) {"]
    for name in names:
        source.append(f'#ifdef __NR_{name}\n  printf("%s %ld\\n", "{name}", (long)__NR_{name});\n#endif')
    source.append("  return 0;\n}")
    with tempfile.TemporaryDirectory(prefix="coworkerd-syscalls-") as directory:
        program = Path(directory) / "numbers.c"
        binary = Path(directory) / "numbers"
        program.write_text("\n".join(source))
        build = subprocess.run([compiler, "-O0", "-o", str(binary), str(program)], capture_output=True, text=True)
        if build.returncode != 0:
            raise SystemExit(f"cannot compile the number table: {build.stderr.strip()[:2000]}")
        listing = subprocess.run([str(binary)], capture_output=True, text=True, check=True).stdout
    table = {}
    for line in listing.splitlines():
        name, value = line.split()
        table[name] = int(value)
    return table


def write_set(path: Path, kind: str, calls: dict[str, int], table: dict[str, int], workerd: list[str],
              profiles: list[str]) -> None:
    """The launcher's form: comments, the number-to-name table for reporting, then one number per line. The
    bytes depend only on the profiles, the architecture and the workerd -- never on the time of the run -- so
    the same inputs give the same file, and allowlist.json records when it was generated."""
    with open(path, "w") as stream:
        stream.write(f"# coworkerd system call allowlist ({kind}), {len(calls)} calls\n")
        stream.write("# generated-by scripts/syscall-allowlist.py\n")
        for digest_text in workerd:
            stream.write(f"# workerd-sha256 {digest_text}\n")
        stream.write(f"# architecture {platform.machine()}\n")
        stream.write(f"# profiles {len(profiles)}\n")
        for name in profiles:
            stream.write(f"# profile {name}\n")
        stream.write("# Every line below that does not start with a digit is a comment. The `name` lines are\n")
        stream.write("# this architecture's number-to-name table, which the Server reports a call outside the\n")
        stream.write("# allowlist by; the launcher reads the numbers alone.\n")
        for name, number in sorted(table.items(), key=lambda row: (row[1], row[0])):
            stream.write(f"# name {number} {name}\n")
        for name, number in sorted(calls.items(), key=lambda row: row[1]):
            stream.write(f"{number}\t# {name}\n")


def main() -> int:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--workerd", action="append", required=True,
                        help="a workerd file this allowlist belongs to (repeatable: an upgrade's old and new)")
    parser.add_argument("--out", required=True, help="the output directory")
    parser.add_argument("--profile", action="append", required=True, help="a profile summary (scripts/syscall-profile.mjs)")
    parser.add_argument("--cc", default=os.environ.get("CC", "cc"))
    parser.add_argument("--relax", default="", help="calls to add, with the reason recorded as the operator's")
    parser.add_argument("--drop", default="", help="calls to leave out (v45's artificial denial)")
    options = parser.parse_args()
    if not shutil.which(options.cc):
        raise SystemExit(f"{options.cc} is needed to resolve this architecture's call numbers")
    workerd_files = [Path(path) for path in options.workerd]
    # Sorted, so the same set of binaries always gives the same bytes.
    workerd_digests = sorted({digest(path) for path in workerd_files})
    # The profiles were measured on one of them (the one being generated for); the others are the chain an
    # upgrade needs.
    workerd_sha256 = workerd_digests[0]
    table = numbers(options.cc)

    startup: set[str] = set()
    steady: set[str] = set()
    evidence: dict[str, dict[str, object]] = {}
    profiles = []
    for path in options.profile:
        data = json.loads(Path(path).read_text())
        if data.get("workerdSha256") and data["workerdSha256"] not in workerd_digests:
            raise SystemExit(f"{path} profiled workerd {data['workerdSha256'][:12]}, which is none of "
                             f"{[d[:12] for d in workerd_digests]}: an allowlist belongs to the bytes it was measured on")
        run = data.get("run") or Path(path).stem
        profiles.append({"path": os.path.relpath(path, ROOT) if str(path).startswith(str(ROOT)) else str(path),
                         "run": run, "sha256": digest(Path(path)),
                         "windows": [w.get("name") for w in data.get("windows", [])],
                         "processes": data.get("processes"), "workerdSha256": data.get("workerdSha256")})
        for phase, counts in data["totals"].items():
            for call, count in counts.items():
                (startup if phase == "startup" else steady).add(call)
                evidence.setdefault(call, {})
                key = f"{run}:{phase}"
                evidence[call][key] = int(evidence[call].get(key, 0)) + int(count)
    if not startup and not steady:
        raise SystemExit("the profiles hold no call")
    for call, reason in ALWAYS.items():
        startup.add(call)
        steady.add(call)
        evidence.setdefault(call, {})["always"] = reason
    for call in filter(None, options.relax.split(",")):
        startup.add(call)
        steady.add(call)
        evidence.setdefault(call, {})["relaxed"] = "added by the operator (--relax)"
    # The startup filter stays in force after the execv, so it has to cover the steady state as well.
    startup |= steady
    for call, reason in STARTUP_ONLY_ALWAYS.items():
        startup.add(call)
        evidence.setdefault(call, {})["startup-always"] = reason
    dropped = [call for call in filter(None, options.drop.split(","))]
    for call in dropped:
        startup.discard(call)
        steady.discard(call)
        evidence.setdefault(call, {})["dropped"] = "left out by --drop"
    unknown = sorted(call for call in startup | steady if call not in table)
    if unknown:
        raise SystemExit(f"these calls are unknown on {platform.machine()}: {unknown}")

    out = Path(options.out)
    out.mkdir(parents=True, exist_ok=True)
    generated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    names = [row["run"] for row in profiles]
    files = {}
    for kind, calls in (("startup", startup), ("steady", steady)):
        path = out / f"coworkerd-syscalls-{kind}.txt"
        write_set(path, kind, {call: table[call] for call in calls}, table, workerd_digests, names)
        files[kind] = {"file": path.name, "sha256": digest(path), "count": len(calls)}
    record = {
        "schemaVersion": 1,
        "generatedBy": "scripts/syscall-allowlist.py",
        "generatedAtUnixMs": int(time.time() * 1000),
        "generatedAt": generated_at,
        "architecture": platform.machine(),
        "headers": headers(),
        # Every workerd this list belongs to; the first is the one the profiles were measured on.
        "workerd": {"sha256": workerd_sha256, "sha256s": workerd_digests,
                    "files": [{"path": str(path), "sha256": digest(path)} for path in workerd_files]},
        "profiles": profiles,
        "startup": {"count": len(startup), "file": files["startup"]["file"], "sha256": files["startup"]["sha256"],
                    "calls": {call: table[call] for call in sorted(startup, key=lambda c: table[c])}},
        "steady": {"count": len(steady), "file": files["steady"]["file"], "sha256": files["steady"]["sha256"],
                   "calls": {call: table[call] for call in sorted(steady, key=lambda c: table[c])}},
        "startupOnly": sorted(startup - steady, key=lambda c: table[c]),
        "always": {**ALWAYS, **STARTUP_ONLY_ALWAYS},
        "dropped": dropped,
        "relaxed": [call for call in filter(None, options.relax.split(","))],
        "evidence": evidence,
        "names": {str(number): name for name, number in table.items()},
    }
    (out / "allowlist.json").write_text(json.dumps(record, indent=1, sort_keys=True) + "\n")
    print(json.dumps({"startup": len(startup), "steady": len(steady), "startupOnly": record["startupOnly"],
                      "workerdSha256": workerd_sha256, "workerdSha256s": workerd_digests,
                      "architecture": record["architecture"], "files": files, "out": str(out)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
