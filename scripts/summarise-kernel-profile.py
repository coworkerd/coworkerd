#!/usr/bin/env python3
"""Associate cluster strace files with node lifecycle events and the observed executables."""

import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile

from kernel_artifacts import BINARIES, ROOT, sha256


def summarise(profile_root, kernel, output):
    windows = json.loads((profile_root / "windows.json").read_text())
    if not windows:
        raise ValueError("no cluster profiling workloads ran")
    output.mkdir(parents=True)
    for program in BINARIES:
        digest = sha256(kernel / program)
        summaries = 0
        for node in sorted(profile_root.glob("node-*")):
            events = [dict(tag="process", **json.loads(line.removeprefix("[process] ")))
                      for line in (node / "node.log").read_text().splitlines() if line.startswith("[process] ")]
            ready = {event["pid"] for event in events if event["event"] == "ready"}
            name = f"{node.name}-{program}"
            with tempfile.TemporaryDirectory(dir=profile_root) as scratch:
                directory = Path(scratch)
                traces = directory / "traces"
                traces.mkdir()
                for trace in sorted(node.glob(f"{program}-*.trace")):
                    match = re.fullmatch(re.escape(program) + r"-([0-9]+)", trace.stem)
                    if match and int(match[1]) in ready:
                        (traces / f"workerd-{match[1]}.trace").symlink_to(trace.resolve())
                if not any(traces.iterdir()):
                    continue
                record = directory / "profile.json"
                record.write_text(json.dumps({"run": name, "windows": windows, "processLines": events,
                                              "binaries": {"workerd": digest},
                                              "traceProgram": f"/opt/coworkerd/bin/profile-target/{program}"}))
                summary = output / f"{name}.json"
                subprocess.run([sys.executable, str(ROOT / "scripts/syscall-summarise.py"),
                                str(directory), str(record), str(summary)], check=True)
            totals = json.loads(summary.read_text())["totals"]
            if not totals.get("startup") or not any(counts for phase, counts in totals.items() if phase != "startup"):
                raise ValueError(f"{name} has no startup or steady-state syscall evidence")
            summaries += 1
        if not summaries:
            raise ValueError(f"no startup and steady-state syscall evidence for {program}")
        print(f"{program}: {summaries} profile summaries for SHA-256 {digest}")


if __name__ == "__main__":
    summarise(*map(Path, sys.argv[1:]))
