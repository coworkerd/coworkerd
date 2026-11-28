#!/usr/bin/env python3
"""Render report tables from a complete Linux main-thread measurement."""

import argparse
import json
import statistics
from pathlib import Path


OPERATIONS = {
    "worker-only": "Worker response",
    "object-noop": "No-op Durable Object",
    "read": "One-row read",
    "insert": "Confirmed one-row insert",
}
STAGES = {
    "receiveRoute": "HTTP receive, route and reply",
    "leaseAdmission": "Lease and admission",
    "nodeToRuntime": "Node to runtime dispatch and HTTP bridge",
    "runtimeToNode": "Runtime to node binding and placement",
    "sharedProtocol": "Shared Cap’n Proto protocol",
    "loggingMetering": "Logging, execution records and metering",
    "durableLog": "Durable log",
    "allocation": "Allocation without an assigned caller",
    "other": "Event loop, system and other resolved stacks",
    "unresolved": "Unresolved stacks",
}


def cpu_ns(row, process="node"):
    cpu = row["cpu"]
    return cpu["processes"][process]["main"]["ticks"] * 1_000_000_000 / cpu["hz"]


def cpu_ms_per_reply(results, process="node"):
    replies = sum(row["n"] for row in results)
    return sum(cpu_ns(row, process) for row in results) / replies / 1_000_000


def median(rows, key):
    return statistics.median(row[key] for row in rows)


def bounds(values, digits=0):
    low, high = min(values), max(values)
    if round(low, digits) == round(high, digits):
        return f"{low:.{digits}f}"
    return f"{low:.{digits}f}–{high:.{digits}f}"


def table(headers, rows):
    print("| " + " | ".join(headers) + " |")
    print("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        print("| " + " | ".join(map(str, row)) + " |")
    print()


def tables(records):
    windows = [row for row in records if row["metric"] == "throughput-window"]
    ceilings = {row["operation"]: row for row in records if row["metric"] == "profile-ceiling"}
    assert records[-1]["metric"] == "complete"
    assert all(row["n"] > 0 and row["warmup"]["failures"] == row["measurement"]["failures"] == 0 for row in windows)
    assert len([row for row in records if row["metric"] == "throughput-confirmation"]) == sum(
        row["operation"] == "insert" for row in windows
    )

    def cells(operation, mode):
        rows = [row for row in windows if row["operation"] == operation and row["profileMode"] == mode]
        assert len(rows) == 3
        assert all(row["clients"] == ceilings[operation]["clients"] for row in rows)
        return rows

    offered = ceilings["worker-only"]["offered"]
    table(["Workload", *map(str, offered), "Clients at knee"], [
        [label, *(f"{rate:,.0f}" for rate in ceilings[op]["medians"]), ceilings[op]["clients"]]
        for op, label in OPERATIONS.items()
    ])

    table(["Workload", "Clients", "Direct replies/s", "Direct node CPU ms/reply",
           "Forwarded replies/s", "Forwarded node CPU ms/reply", "Forwarder CPU ms/reply",
           "Direct p50 / p99 ms", "Forwarded p50 / p99 ms"], [
        [label, ceilings[op]["clients"], f"{median(direct, 'perSecond'):,.0f}", f"{cpu_ms_per_reply(direct):.3f}",
         f"{median(forwarded, 'perSecond'):,.0f}", f"{cpu_ms_per_reply(forwarded):.3f}",
         f"{cpu_ms_per_reply(forwarded, 'forwarder'):.3f}",
         f"{median(direct, 'p50Ms'):.2f} / {median(direct, 'p99Ms'):.2f}",
         f"{median(forwarded, 'p50Ms'):.2f} / {median(forwarded, 'p99Ms'):.2f}"]
        for op, label in OPERATIONS.items()
        for direct, forwarded in [(cells(op, "direct"), cells(op, "forwarded"))]
    ])

    attributed = {}
    comparisons = []
    for op, label in OPERATIONS.items():
        profiled = cells(op, "profiled")
        direct = cells(op, "direct")
        parts = dict.fromkeys(STAGES, 0.0)
        perf_ns = 0
        samples = 0
        for row in profiled:
            profile = row["perf"]
            periods = {stage: bucket["periodNs"] for stage, bucket in profile["histogram"].items()}
            total = sum(periods.values())
            perf_ns += total
            samples += profile["samples"]
            for stage, period in periods.items():
                parts[stage] += cpu_ns(row) * period / total
        replies = sum(row["n"] for row in profiled)
        attributed[op] = {stage: ns / replies / 1_000_000 for stage, ns in parts.items()}
        comparisons.append([label, f"{samples:,}", f"{perf_ns / replies / 1_000_000:.3f}",
                            f"{cpu_ms_per_reply(profiled):.3f}", f"{cpu_ms_per_reply(direct):.3f}",
                            f"{median(profiled, 'perSecond'):,.0f}"])
    table(["Stage (CPU ms/reply)", *OPERATIONS.values(), "No-op minus Worker"], [
        [label, *(f"{attributed[op][stage]:.3f}" for op in OPERATIONS),
         f"{attributed['object-noop'][stage] - attributed['worker-only'][stage]:+.3f}"]
        for stage, label in STAGES.items()
    ] + [["Total", *(f"{sum(parts.values()):.3f}" for parts in attributed.values()),
          f"{sum(attributed['object-noop'].values()) - sum(attributed['worker-only'].values()):+.3f}"]])
    table(["Workload", "CPU samples", "Perf period ms/reply", "Profiled /proc CPU ms/reply",
           "Direct /proc CPU ms/reply", "Profiled replies/s"], comparisons)

    occupancy = []
    for op, label in OPERATIONS.items():
        for client_count in dict.fromkeys([ceilings[op]["clients"], offered[-1]]):
            rows = (cells(op, "direct") if client_count == ceilings[op]["clients"] else
                    [row for row in windows if row["operation"] == op and row["profileMode"] == "sweep"
                     and row["clients"] == client_count])
            assert len(rows) == 3
            stats = [row["occupancy"] for row in rows]
            peak = max((item["peak"] for item in stats), key=lambda event: event["total"])
            occupancy.append([label, client_count,
                              *(bounds([item[kind]["mean"] for item in stats], 1) for kind in ("roots", "nested")),
                              bounds([item["total"]["p99"] for item in stats]),
                              f"{peak['roots']} + {peak['nested']} = {peak['total']}",
                              max(item["atRootAdmission"]["max"] for item in stats)])
    table(["Workload", "Clients", "Mean roots (range)", "Mean nested (range)", "Total p99 (range)",
           "Largest simultaneous total", "Most slots before root arrival"], occupancy)

    inserts = cells("insert", "direct")
    calls = sum(row["flush"]["calls"] for row in inserts)
    elapsed = sum(row["flush"]["elapsedNs"] for row in inserts) / 1_000_000
    cpu = sum(row["flush"]["cpuNs"] for row in inserts) / 1_000_000
    replies = sum(row["n"] for row in inserts)
    max_elapsed = max(row["flush"]["maxElapsedNs"] for row in inserts) / 1_000_000
    table(["Confirmed replies", "Flush calls", "Elapsed ms/flush", "CPU ms/flush",
           "Off-CPU ms/reply", "Largest elapsed ms"], [
        [f"{replies:,}", f"{calls:,}", f"{elapsed / calls:.3f}", f"{cpu / calls:.3f}",
         f"{(elapsed - cpu) / replies:.3f}", f"{max_elapsed:.3f}"]
    ])


def load_windows(path):
    with path.open() as source:
        records = [json.loads(line) for line in source]
    assert records[-1]["metric"] == "complete", path
    windows = [row for row in records if row["metric"] == "throughput-window"]
    assert all(row["clients"] == 16 and row["n"] > 0
               and row["warmup"]["failures"] == row["measurement"]["failures"] == 0 for row in windows)
    return records[0], windows


def controls(directory):
    before, plain_before = load_windows(directory / "plain-before.jsonl")
    after, plain_after = load_windows(directory / "plain-after.jsonl")
    traced, trace_windows = load_windows(directory / "traced.jsonl")
    assert before["runner"] == after["runner"] == traced["runner"]
    assert before["archiveSha256"] == after["archiveSha256"] == traced["archiveSha256"]
    assert before["hashes"]["coworkerd"] == after["hashes"]["coworkerd"] != traced["hashes"]["coworkerd"]

    dso_labels = ["glibc", "Linux kernel and modules", "Node executable",
                  "libstdc++", "Other userspace objects", "Unknown mapping"]

    def dso_label(name):
        if name == "[unknown]":
            return "Unknown mapping"
        if name.startswith("[") and name != "[vdso]":
            return "Linux kernel and modules"
        basename = Path(name).name
        if basename.startswith("libc.so."):
            return "glibc"
        if basename == "coworkerd":
            return "Node executable"
        if basename.startswith("libstdc++.so."):
            return "libstdc++"
        return "Other userspace objects"

    build_rows = []
    costs = {}
    for op, label in OPERATIONS.items():
        plain = [row for row in plain_before + plain_after if row["operation"] == op]
        traced_operation = [row for row in trace_windows if row["operation"] == op]
        direct = [row for row in traced_operation if row["profileMode"] == "direct"]
        sampled = [row for row in traced_operation if row["profileMode"] == "profiled"]
        assert len(plain) == 2 and len(direct) == len(sampled) == 3
        plain_cost = cpu_ms_per_reply(plain)
        trace_cost = cpu_ms_per_reply(direct)
        build_rows.append([label, " / ".join(f"{cpu_ms_per_reply([row]):.3f}" for row in plain),
                           f"{plain_cost:.3f}", f"{trace_cost:.3f}", f"{trace_cost - plain_cost:+.3f}",
                           f"{median(plain, 'perSecond'):,.0f} / {median(direct, 'perSecond'):,.0f}"])
        parts = dict.fromkeys(dso_labels, 0.0)
        for row in sampled:
            profile = row["perf"]
            histogram = profile["histogram"]
            dsos = profile["residualByDso"]
            total = sum(bucket["periodNs"] for bucket in histogram.values())
            assert sum(dsos.values()) == sum(histogram[stage]["periodNs"] for stage in
                    ("allocation", "other", "unresolved"))
            for dso, period in dsos.items():
                parts[dso_label(dso)] += cpu_ns(row) * period / total
        replies = sum(row["n"] for row in sampled)
        costs[op] = {dso: amount / replies / 1_000_000 for dso, amount in parts.items()}

    table(["Workload", "Plain before / after CPU ms/reply", "Plain pooled CPU ms/reply",
           "Traced CPU ms/reply", "Traced minus plain ms/reply", "Plain / traced replies/s"], build_rows)
    table(["Residual DSO (CPU ms/reply)", *OPERATIONS.values()], [
        [dso, *(f"{costs[op][dso]:.3f}" for op in OPERATIONS)] for dso in dso_labels
    ] + [["Total residual", *(f"{sum(costs[op].values()):.3f}" for op in OPERATIONS)]])


def paired_release(directory):
    builds = ("baseline", "candidate")
    modes = ("direct", "forwarded")
    cells = {(build, op, mode): [] for build in builds for op in OPERATIONS for mode in modes}
    provenance = {build: [] for build in builds}
    forwarders = set()
    confirmed = 0
    for pair in range(1, 4):
        for build in builds:
            path = directory / f"{pair}-{build}.jsonl"
            source, windows = load_windows(path)
            provenance[build].append(source)
            assert source["metric"] == "provenance" and source["profileControl"] == "paired-release", path
            assert {(row["operation"], row["profileMode"]) for row in windows} == {
                (op, mode) for op in OPERATIONS for mode in modes
            } and len(windows) == 8, path
            for row in windows:
                cells[build, row["operation"], row["profileMode"]].append(row)
            for row in map(json.loads, path.read_text().splitlines()):
                if row["metric"] == "profile-tools":
                    assert row["perf"] is None
                    forwarders.add(row["forwarderSha256"])
                elif row["metric"] == "throughput-confirmation":
                    assert row["confirmations"] >= row["acknowledged"]
                    confirmed += row["acknowledged"]
    assert len(forwarders) == 1 and confirmed > 0
    reference = provenance["baseline"][0]
    for build, sources in provenance.items():
        for source in sources:
            assert all(source[key] == reference[key] for key in ("runner", "clientHost", "archiveSha256",
                       "productHashes", "settings", "clientImage"))
            assert all(source["hashes"][key] == reference["hashes"][key]
                       for key in ("workerd", "coworkerd-threaded"))
            assert source["commit"] == sources[0]["commit"]
            assert source["hashes"] == sources[0]["hashes"]
    assert reference["hashes"]["coworkerd"] != provenance["candidate"][0]["hashes"]["coworkerd"]

    budgets = {"worker-only": "0.100", "object-noop": "0.200"}
    totals = []
    pairs = []
    for op, label in OPERATIONS.items():
        base = cells["baseline", op, "direct"]
        candidate = cells["candidate", op, "direct"]
        before = cpu_ms_per_reply(base)
        after = cpu_ms_per_reply(candidate)
        forwarded = cells["baseline", op, "forwarded"] + cells["candidate", op, "forwarded"]
        totals.append([label, f"{before:.3f}", f"{after:.3f}", f"{(after / before - 1) * 100:+.1f}%",
                       budgets.get(op, "—"), f"{cpu_ms_per_reply(forwarded, 'forwarder'):.3f}",
                       f"{median(base, 'perSecond'):,.0f} / {median(candidate, 'perSecond'):,.0f}"])
        deltas = [cpu_ms_per_reply([result]) - cpu_ms_per_reply([control])
                  for control, result in zip(base, candidate)]
        pairs.append([label, " / ".join(f"{delta:+.3f}" for delta in deltas),
                      f"{median(base, 'p99Ms'):.2f} / {median(candidate, 'p99Ms'):.2f}"])
    table(["Workload", "Baseline CPU ms/reply", "Candidate CPU ms/reply", "Change",
           "Budget ms/reply", "Bare forwarder CPU ms/reply", "Baseline / candidate replies/s"], totals)
    table(["Workload", "Candidate minus baseline CPU ms/reply by pair", "Baseline / candidate p99 ms"], pairs)
    print(f"{sum(len(rows) for rows in cells.values())} windows, {confirmed:,} verified inserts with warm-up.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jsonl", type=Path, nargs="?")
    parser.add_argument("--controls", type=Path, help="directory of Release and traced control recordings")
    parser.add_argument("--paired-release", type=Path, help="directory of paired dispatch measurements")
    args = parser.parse_args()
    if args.jsonl is None and args.controls is None and args.paired_release is None:
        parser.error("choose a recording, --controls, or --paired-release")
    if args.jsonl:
        with args.jsonl.open() as source:
            tables([json.loads(line) for line in source])
    if args.controls:
        controls(args.controls)
    if args.paired_release:
        paired_release(args.paired_release)


if __name__ == "__main__":
    main()
