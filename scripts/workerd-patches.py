"""Deterministic bridge between the workerd patch sequence and local commits."""
from pathlib import Path
import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
FORMAT = ["--zero-commit", "--no-signature", "--no-stat", "--full-index", "--no-numbered", "--no-renames", "--no-notes", "--no-cover-letter",
    "--src-prefix=a/", "--dst-prefix=b/", "--unified=3", "--diff-algorithm=myers", "--no-color", "--no-ext-diff"]
IDENTITY_PATCH = "0015-rpc-invocation-identity.patch"
IDENTITY_PATHS = frozenset({
    "src/workerd/api/worker-loader.h", "src/workerd/api/worker-loader.c++",
    "src/workerd/io/io-channels.h", "src/workerd/io/io-channels.c++",
})
RPC_CODE_PATCH = "0016-rpc-code-parent-death.patch"
RPC_CODE_PATHS = frozenset({
    "src/workerd/jsg/util.c++", "src/workerd/jsg/BUILD.bazel",
    "src/workerd/jsg/coworkerd-rpc-code.h", "src/workerd/jsg/coworkerd-rpc-code.c++",
})
STARTUP_IDENTITY_PATCH = "0005-memory-limit.patch"
STARTUP_IDENTITY_PATHS = frozenset({
    "src/workerd/api/worker-loader.c++", "src/workerd/io/io-channels.h",
})
# The V8 sandbox needs V8 itself (two V8 patches and the build parameters) and four jsg hooks: the
# memory protection key scope only Cloudflare's V8 supports, the platform wrapper's zero segment,
# the Blob byte view's sandbox base with V8's guard regions, and the default allocator's group.
SANDBOX_PATCH = "0043-v8-sandbox-per-isolate.patch"
SANDBOX_PATHS = frozenset({
    ".bazelrc", "build/deps/v8.MODULE.bazel",
    "patches/v8/0055-Make-the-sandbox-size-and-the-additional-trailing-guard-build-parameters.patch",
    "patches/v8/0057-Add-IsolateGroup-TryCreate-which-can-fail-instead-of-aborting.patch",
    "src/workerd/jsg/jsg.h", "src/workerd/jsg/jsg.c++", "src/workerd/jsg/v8-platform-wrapper.h",
    "src/workerd/jsg/sandbox.h", "src/workerd/jsg/sandbox.c++",
    # jsg's default ArrayBuffer allocator holds its isolate group (newGroupArrayBufferAllocator).
    "src/workerd/jsg/setup.c++", "src/workerd/jsg/BUILD.bazel",
    # The threaded server (0042): an isolate group per Worker that can fail, its sandbox checks.
    "src/workerd/threaded/BUILD.bazel", "src/workerd/threaded/threaded-server.c++",
    "src/workerd/threaded/threaded-server-test.c++",
})
CREATION_IDENTITY_PATCH = "0017-creation-identity.patch"
CREATION_IDENTITY_PATHS = frozenset({
    "src/workerd/api/worker-loader.h", "src/workerd/api/worker-loader.c++",
    "src/workerd/io/io-channels.h",
})

def git(checkout, *args, env=None, input=None):
    return subprocess.check_output(["git", "-c", "core.autocrlf=false", "-c", "diff.noprefix=false", "-c", "core.quotePath=true",
        "-c", "diff.suppressBlankEmpty=false", "-c", "format.subjectPrefix=PATCH", "-c", "format.useAutoBase=false",
        "-C", str(checkout), *args], env=env, input=input)

# C++ implementation hooks may live in the legacy server package or its factory.
# Rust server/CLI changes require a patch-specific exact path, even though they
# live below the server package. Startup identity only forwards one owned value.
SERVER_CPP_ROOTS = ("src/workerd/server/factory/",)
RUST_PATHS = {
    "0140-invocation-budget-identity.patch": frozenset({
        "src/workerd/server/server/bridge.rs",
        "src/workerd/server/server/root_loader.rs",
    }),
    "0041-ordinary-lease-deadline.patch": frozenset({"src/workerd/server/server/root_loader.rs"}),
    "0038-preparation-queue-and-failure-cache.patch": frozenset({"src/workerd/server/server/root_loader.rs"}),
    "0035-preparation-failure-recovery.patch": frozenset({"src/workerd/server/server/root_loader.rs"}),
    "0033-owned-preparation-and-demand-drain.patch": frozenset({"src/workerd/server/server/root_loader.rs", "src/workerd/server/server/bridge.rs"}),
    "0032-ordinary-preparation-and-drain.patch": frozenset({"src/workerd/server/server/root_loader.rs"}),
    "0030-ordinary-compile-errors.patch": frozenset({"src/workerd/server/server/root_loader.rs"}),
    "0029-ordinary-trial-view.patch": frozenset({"src/workerd/server/server/root_loader.rs"}),
    "0027-stateless-dispatch.patch": frozenset({
        "src/workerd/server/server/root_loader.rs",
        "src/workerd/server/server/bridge.rs",
        "src/workerd/server/server/worker.rs",
    }),
    "0026-plain-entry-review.patch": frozenset({
        "src/workerd/server/server/root_loader.rs",
    }),
    "0025-plain-startup-accounting.patch": frozenset({
        "src/workerd/server/server/root_loader.rs",
    }),
    "0024-plain-direct-channel-backoff.patch": frozenset({
        "src/workerd/server/server/root_loader.rs",
    }),
    "0023-plain-worker-entry.patch": frozenset({
        "src/workerd/server/server/root_loader.rs",
        "src/workerd/server/server/config.rs",
        "src/workerd/server/server/worker.rs",
        "src/workerd/server/server/bridge.rs",
    }),
    "0022-root-registry-scale.patch": frozenset({
        "src/workerd/server/server/root_loader.rs",
    }),
    "0020-root-loader.patch": frozenset({
        "src/workerd/server/server/actor.rs",
        "src/workerd/server/server/bindings.rs",
        "src/workerd/server/server/bridge.rs",
        "src/workerd/server/server/channels.rs",
        "src/workerd/server/server/config.rs",
        "src/workerd/server/server/lib.rs",
        "src/workerd/server/server/loader.rs",
        "src/workerd/server/server/root_loader.rs",
        "src/workerd/server/server/worker.rs",
    }),
    # Rust owns root-tree residency and wake accounting; the bridge carries the
    # C++-read limit and residency reports without changing the node protocol.
    "0014-durable-object-hibernation-alarms.patch": frozenset({
        "src/workerd/server/server/actor.rs",
        "src/workerd/server/server/bridge.rs",
    }),
    "0013-durable-object-log.patch": frozenset({
        "src/workerd/server/server/actor.rs",
        "src/workerd/server/server/bridge.rs",
    }),
    "0003-idle-eviction.patch": frozenset({
        "src/workerd/server/server/bridge.rs",
        "src/workerd/server/server/loader.rs",
        "src/workerd/server/server/worker.rs",
    }),
    "0001-native-binding.patch": frozenset({
        "src/workerd/server/server/services/mod.rs",
    }),
    IDENTITY_PATCH: frozenset({
        "src/workerd/server/server/bridge.rs",
        "src/workerd/server/server/loader.rs",
    }),
    CREATION_IDENTITY_PATCH: frozenset({
        "src/workerd/server/server/bridge.rs",
        "src/workerd/server/server/loader.rs",
        "src/workerd/server/server/worker.rs",
    }),
    STARTUP_IDENTITY_PATCH: frozenset({
        "src/workerd/server/server/bridge.rs",
        "src/workerd/server/server/loader.rs",
        "src/workerd/server/server/worker.rs",
    }),
}


def allowed_path(name, path):
    # Do not normalize a suspicious spelling into an authorized path.
    canonical = path and not path.startswith("/") and "\\" not in path and all(
        part not in ("", ".", "..") for part in path.split("/"))
    server_path = (path.startswith(SERVER_CPP_ROOTS) or
                   path.startswith("src/workerd/server/") and
                   "/" not in path[len("src/workerd/server/"):])
    rust_path = (path.startswith(("src/workerd/server/server/", "src/workerd/server/cli/")) and
                 path in RUST_PATHS.get(name, frozenset()))
    return canonical and ((server_path and not path.endswith(".rs")) or rust_path or
                          name == '0042-threaded-server-s1a.patch' and path in {
                              'src/workerd/threaded/BUILD.bazel',
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/threaded/threaded-server-test.c++',
                              'src/workerd/threaded/protocol.h',
                              'src/workerd/threaded/protocol.capnp'} or
                          name == '0044-s1e-node-cost-and-long-operations.patch' and path in {
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/threaded/threaded-server-test.c++',
                              'build/deps/v8.MODULE.bazel',
                              'patches/v8/0056-Let-JSON-parse-and-split-take-interrupts.patch'} or
                          name == '0045-threaded-durable-admission.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0046-durable-test-routing.patch' and path in {
                              'src/workerd/threaded/BUILD.bazel',
                              'src/workerd/threaded/threaded-server.c++'} or
                          name == '0047-durable-test-events.patch' and path in {
                              'src/workerd/threaded/BUILD.bazel',
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/util/sqlite.h',
                              'src/workerd/util/sqlite.c++'} or
                          name == '0048-durable-event-cpu.patch' and path in {
                              'src/workerd/api/global-scope.c++',
                              'src/workerd/api/node/async-hooks.c++',
                              'src/workerd/api/worker-rpc.c++',
                              'src/workerd/io/io-context.c++',
                              'src/workerd/io/io-context.h',
                              'src/workerd/io/limit-enforcer.h',
                              'src/workerd/io/worker-entrypoint.c++',
                              'src/workerd/io/worker.c++',
                              'src/workerd/io/worker.h',
                              'src/workerd/jsg/async-context.c++',
                              'src/workerd/jsg/async-context.h',
                              'src/workerd/threaded/BUILD.bazel',
                              'src/workerd/threaded/threaded-server.c++'} or
                          name == '0049-durable-test-facets.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0050-durable-format.patch' and path in {
                              'src/workerd/api/worker-rpc.c++',
                              'src/workerd/io/worker-entrypoint.c++',
                              'src/workerd/io/worker.c++',
                              'src/workerd/io/worker.h',
                              'src/workerd/jsg/async-context.c++',
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/util/sqlite.c++'} or
                          name == '0051-durable-cold-load-lifetime.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0052-durable-context-lifetime.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0053-durable-context-format.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0054-durable-test-confirmation.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0055-durable-memory-observation.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0056-durable-idle-memory-release.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0057-durable-idle-clock-tests.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0058-durable-group-refusal-test.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0059-durable-default-home-count.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0060-durable-http-only-boundary.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name in {'0061-durable-home-before-ack.patch',
                                   '0062-durable-warm-copies.patch',
                                   '0064-durable-load-waiters.patch',
                                   '0065-durable-session-scope.patch',
                                   '0066-durable-reset-progress.patch',
                                   '0067-durable-facet-reset-lifetime.patch',
                                   '0068-durable-process-memory-progress.patch',
                                   '0069-durable-process-memory-recovery.patch',
                                   '0070-durable-rpc-binding.patch'} and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0081-threaded-durable-storage.patch' and path in {
                              'src/workerd/threaded/BUILD.bazel',
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/util/sqlite.h',
                              'src/workerd/util/sqlite.c++'} or
                          name == '0082-threaded-durable-fixture-state.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0083-threaded-wal-open-proof.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0084-prepare-threaded-wal-off-home.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name in {'0085-threaded-facet-mutations.patch',
                                   '0086-refuse-persistent-facet-cloning.patch',
                                   '0087-allow-threaded-wal-registration.patch',
                                   '0088-check-live-facets-on-reset.patch',
                                   '0089-use-durable-home-memory-reclaim.patch'} and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0100-threaded-durable-alarms.patch' and path in {
                              'src/workerd/io/actor-sqlite.c++',
                              'src/workerd/io/actor-sqlite.h',
                              'src/workerd/threaded/threaded-server.c++'} or
                          name == '0105-bound-threaded-version-drain.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0101-validate-threaded-alarm-entry.patch' and
                              path == 'src/workerd/io/actor-sqlite.c++' or
                          name == '0103-park-threaded-durable-websockets.patch' and path in {
                              'src/workerd/api/http.c++',
                              'src/workerd/api/web-socket.c++',
                              'src/workerd/api/web-socket.h',
                              'src/workerd/io/legacy-hibernation-manager.c++',
                              'src/workerd/io/legacy-hibernation-manager.h',
                              'src/workerd/threaded/protocol.capnp',
                              'src/workerd/threaded/threaded-server.c++'} or
                          name == '0102-threaded-durable-idle-sockets.patch' and path in {
                              'src/workerd/api/web-socket.c++',
                              'src/workerd/io/io-context.c++',
                              'src/workerd/io/io-context.h',
                              'src/workerd/io/worker.c++',
                              'src/workerd/io/worker.h',
                              'src/workerd/threaded/threaded-server.c++'} or
                          name == '0104-settle-threaded-alarm-outcomes.patch' and path in {
                              'src/workerd/io/actor-sqlite.h',
                              'src/workerd/io/actor-sqlite-test.c++',
                              'src/workerd/threaded/threaded-server.c++'} or
                          name == '0106-complete-threaded-lifecycle-transitions.patch' and path in {
                              'src/workerd/api/web-socket.c++',
                              'src/workerd/api/web-socket.h',
                              'src/workerd/io/io-context.c++',
                              'src/workerd/io/io-context.h',
                              'src/workerd/io/legacy-hibernation-manager.c++',
                              'src/workerd/io/legacy-hibernation-manager.h',
                              'src/workerd/io/worker.c++',
                              'src/workerd/io/worker.h',
                              'src/workerd/threaded/protocol.capnp',
                              'src/workerd/threaded/threaded-server.c++'} or
                          name == '0107-settle-threaded-socket-handoffs.patch' and path in {
                              'src/workerd/api/web-socket.c++',
                              'src/workerd/api/web-socket.h',
                              'src/workerd/io/legacy-hibernation-manager.c++',
                              'src/workerd/io/legacy-hibernation-manager.h',
                              'src/workerd/io/worker.h',
                              'src/workerd/threaded/protocol.capnp',
                              'src/workerd/threaded/threaded-server.c++'} or
                          name == '0108-park-threaded-facet-websockets.patch' and path in {
                              'src/workerd/api/hibernatable-web-socket.c++',
                              'src/workerd/api/http.c++',
                              'src/workerd/api/web-socket.c++',
                              'src/workerd/api/web-socket.h',
                              'src/workerd/io/legacy-hibernation-manager.c++',
                              'src/workerd/io/legacy-hibernation-manager.h',
                              'src/workerd/threaded/protocol.capnp',
                              'src/workerd/threaded/threaded-server-test.c++',
                              'src/workerd/threaded/threaded-server.c++'} or
                          name == '0109-deliver-threaded-websocket-bytes.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name in {'0140-invocation-budget-identity.patch', '0141-ordinary-binding-caller.patch'} and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0142-hold-invocation-budget-through-drain.patch' and path in {
                              'src/workerd/io/io-channels.h',
                              'src/workerd/io/worker-entrypoint.c++',
                              'src/workerd/io/worker-interface.capnp',
                              'src/workerd/threaded/protocol.capnp',
                              'src/workerd/threaded/threaded-server.c++'} or
                          name == '0143-release-durable-budgets-over-parent-socket.patch' and path in {
                              'src/workerd/io/io-channels.h',
                              'src/workerd/io/worker-entrypoint.c++',
                              'src/workerd/io/worker-interface.capnp',
                              'src/workerd/threaded/protocol.capnp',
                              'src/workerd/threaded/threaded-server.c++'} or
                          name == '0144-retry-durable-budget-release-errors.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0122-order-threaded-wal-home-teardown.patch' and path in {
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/threaded/threaded-server-test.c++'} or
                          name == '0123-confirm-threaded-storage-close-before-reopen.patch' and path in {
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/util/sqlite.c++',
                              'src/workerd/util/sqlite.h'} or
                          name == '0124-bound-threaded-wal-home-notifications.patch' and path in {
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/threaded/threaded-server-test.c++'} or
                          name == '0185-measure-isolate-busy-time.patch' and path in {
                              'src/workerd/jsg/jsg.c++',
                              'src/workerd/jsg/observer.h',
                              'src/workerd/threaded/BUILD.bazel',
                              'src/workerd/threaded/isolate-busy-test.c++',
                              'src/workerd/threaded/isolate-busy.h',
                              'src/workerd/threaded/threaded-server.c++'} or
                          name == '0186-start-isolate-busy-time-after-load.patch' and path in {
                              'src/workerd/threaded/isolate-busy.h',
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/threaded/threaded-server-test.c++'} or
                          name == '0195-reclaim-retired-durable-memory.patch' and path in {
                              'src/workerd/threaded/BUILD.bazel',
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/threaded/threaded-server-test.c++'} or
                          name == '0130-ordinary-fatal-attribution.patch' and path in {
                              'src/workerd/threaded/BUILD.bazel',
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/threaded/threaded-server-test.c++'} or
                          name == '0131-fatal-attribution-requires-invocation.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0132-fatal-record-test-constant.patch' and
                              path == 'src/workerd/threaded/threaded-server-test.c++' or
                          name == '0133-scope-fatal-attribution.patch' and path in {
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/threaded/threaded-server-test.c++'} or
                          name == '0134-keep-fatal-diagnostics-and-load-scopes.patch' and path in {
                              'src/workerd/threaded/BUILD.bazel',
                              'src/workerd/threaded/fatal-attribution.h',
                              'src/workerd/threaded/fatal-attribution-test.c++',
                              'src/workerd/threaded/fatal-attribution-scope-test.c++',
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/threaded/threaded-server-test.c++'} or
                          name in {'0150-platform-durable-definition.patch',
                                   '0151-reserve-platform-execution-identity.patch',
                                   '0170-place-platform-databases-in-exclusive-copies.patch',
                                   '0176-attribute-platform-databases-to-database-worker.patch'} and path in {
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/threaded/threaded-server-test.c++'} or
                          name == '0171-bound-threaded-sql-wall-time.patch' and path in {
                              'src/workerd/api/sql.c++', 'src/workerd/api/sql.h',
                              'src/workerd/api/actor-state.c++', 'src/workerd/io/actor-sqlite.c++',
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/util/sqlite.c++', 'src/workerd/util/sqlite.h',
                              'src/workerd/util/sqlite-test.c++'} or
                          name == '0172-time-wal-confirmation-after-threaded-sql.patch' and path in {
                              'src/workerd/io/worker.c++', 'src/workerd/io/worker.h',
                              'src/workerd/threaded/threaded-server.c++', 'src/workerd/util/sqlite-test.c++'} or
                          name == '0173-bound-platform-sql-transactions.patch' and path in {
                              'src/workerd/api/actor-state.c++', 'src/workerd/io/actor-sqlite.c++',
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/util/sqlite.c++', 'src/workerd/util/sqlite.h'} or
                          name == '0175-harden-platform-sql-transaction-settlement.patch' and path in {
                              'src/workerd/api/actor-state.c++',
                              'src/workerd/util/sqlite.c++', 'src/workerd/util/sqlite.h'} or
                          name == '0174-release-idle-platform-database-copies.patch' and path in {
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/threaded/threaded-server-test.c++'} or
                          name in {'0177-retire-idle-databases-at-admission.patch',
                                   '0180-fix-database-admission-accounting.patch'} and path in {
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/threaded/threaded-server-test.c++'} or
                          name == '0178-keep-platform-databases-open-after-sql-interruption.patch' and path in {
                              'src/workerd/api/actor-state.c++',
                              'src/workerd/util/sqlite.c++', 'src/workerd/util/sqlite.h',
                              'src/workerd/util/sqlite-test.c++'} or
                          name == '0179-size-default-object-mailbox.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0181-report-durable-copy-reservation-refusal.patch' and
                              path == 'src/workerd/threaded/threaded-server.c++' or
                          name == '0182-bound-platform-sqlite-memory.patch' and path in {
                              'src/workerd/api/actor-state.c++', 'src/workerd/io/actor-sqlite.c++',
                              'src/workerd/threaded/BUILD.bazel', 'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/util/sqlite-metering.c++', 'src/workerd/util/sqlite-metering.h',
                              'src/workerd/util/sqlite-test.c++', 'src/workerd/util/sqlite.c++',
                              'src/workerd/util/sqlite.h'} or
                          name == '0183-bound-platform-sqlite-spill.patch' and path in {
                              'src/workerd/api/actor-state.c++', 'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/util/sqlite-test.c++', 'src/workerd/util/sqlite.c++',
                              'src/workerd/util/sqlite.h'} or
                          name == '0184-charge-database-sqlite-heap-to-process.patch' and path in {
                              'src/workerd/api/actor-state.c++',
                              'src/workerd/threaded/threaded-server.c++',
                              'src/workerd/threaded/threaded-server-test.c++',
                              'src/workerd/util/sqlite-metering.c++',
                              'src/workerd/util/sqlite-metering.h',
                              'src/workerd/util/sqlite.c++', 'src/workerd/util/sqlite.h'} or
                          name == IDENTITY_PATCH and path in IDENTITY_PATHS or
                          name == STARTUP_IDENTITY_PATCH and path in STARTUP_IDENTITY_PATHS or
                          name == CREATION_IDENTITY_PATCH and path in CREATION_IDENTITY_PATHS or
                          name == RPC_CODE_PATCH and path in RPC_CODE_PATHS or
                          name == SANDBOX_PATCH and path in SANDBOX_PATHS or
                          name == '0021-sql-execution-limit.patch' and path in {
                              'src/workerd/util/sqlite.h', 'src/workerd/util/sqlite.c++',
                              'src/workerd/api/sql.h', 'src/workerd/api/sql.c++',
                              'src/workerd/io/limit-enforcer.h',
                              'src/workerd/server/factory/coworkerd-cpu-limit.c++'})

def validate_patch(checkout, name, data):
    """Validate Git's parsed paths and literal headers before any checkout mutation."""
    headers, current, in_hunk = set(), None, False
    rpc_conversion_hunks = 0
    for line in data.splitlines():
        if line.startswith(b"diff --git "):
            match = re.fullmatch(rb"diff --git a/([^\s]+) b/([^\s]+)", line)
            assert match, "patch scope: noncanonical diff header"
            old, new = (part.decode("utf-8") for part in match.groups())
            assert old == new and allowed_path(name, new), "patch scope: " + name + ": " + old + " -> " + new
            headers.add(new)
            current, in_hunk = new, False
        elif line.startswith(b"@@"):
            in_hunk = True
            if name == RPC_CODE_PATCH and current == "src/workerd/jsg/util.c++":
                rpc_conversion_hunks += 1
        elif not in_hunk:
            if line.startswith((b"rename from ", b"rename to ", b"copy from ", b"copy to ")):
                raise AssertionError("patch scope: rename/copy headers are not allowed")
            if line.startswith((b"--- ", b"+++ ")):
                expected = (b"a/" if line.startswith(b"--- ") else b"b/") + (current or "").encode()
                assert current and line[4:] in (expected, b"/dev/null"), "patch scope: inconsistent file header"
            if line.startswith((b"new file mode ", b"old mode ", b"new mode ", b"deleted file mode ")):
                assert line.split()[-1] in (b"100644", b"100755"), "patch scope: non-regular file mode"
    parsed = set()
    for row in git(checkout, "apply", "--numstat", "-z", "-", input=data).split(b"\0"):
        if not row:
            continue
        fields = row.split(b"\t", 2)
        assert len(fields) == 3 and fields[2], "patch scope: ambiguous Git path"
        path = fields[2].decode("utf-8")
        assert allowed_path(name, path), "patch scope: " + name + ": " + path
        parsed.add(path)
    assert headers and headers == parsed, "patch scope: literal and Git paths differ"
    if name == RPC_CODE_PATCH:
        assert rpc_conversion_hunks == 1, "RPC conversion requires exactly one upstream hook"
    return sorted(parsed)

def settings(directory):
    upstream = dict(line.split("=", 1) for line in (directory / "UPSTREAM").read_text().splitlines() if line)
    series = [line for line in (directory / "series").read_text().splitlines() if line and not line.startswith("#")]
    assert len(series) == len(set(series)) and series
    assert all(Path(p).name == p and p.endswith(".patch") for p in series)
    assert len(upstream["commit"]) == 40
    return upstream, series

def clean(checkout):
    assert not git(checkout, "status", "--porcelain"), "checkout must be clean"

def format_commit(checkout, commit):
    return git(checkout, "format-patch", *FORMAT, "-1", "--stdout", commit)

def operate(action, checkout, directory, output=None):
    checkout, directory = checkout.resolve(), directory.resolve()
    upstream, series = settings(directory)
    base = upstream["commit"]
    assert git(checkout, "rev-parse", upstream["tag"]).decode().strip() == base
    patches = [directory / p for p in series]
    # All actions preflight the entire series. A bad final patch cannot leave an apply/import
    # half-finished or overwrite part of an export.
    scopes = {patch.name: validate_patch(checkout, patch.name, patch.read_bytes()) for patch in patches}
    if action in ("apply", "import"):
        clean(checkout)
        assert git(checkout, "rev-parse", "HEAD").decode().strip() == base
        if action == "apply":
            for patch in patches:
                git(checkout, "apply", "--index", str(patch))
        else:
            git(checkout, "-c", "user.name=Codex", "-c", "user.email=codex@openai.com", "am", "--committer-date-is-author-date", *map(str, patches))
    elif action == "export":
        clean(checkout)
        commits = git(checkout, "rev-list", "--reverse", "--first-parent", base + "..HEAD").decode().splitlines()
        assert len(commits) == len(series), "one commit per patch is required"
        target = output.resolve() if output else directory
        target.mkdir(parents=True, exist_ok=True)
        exported = []
        for name, commit in zip(series, commits):
            data = format_commit(checkout, commit)
            validate_patch(checkout, name, data)
            exported.append((name, data))
        for name in ("UPSTREAM", "series"):
            if target != directory:
                (target / name).write_bytes((directory / name).read_bytes())
        for name, data in exported:
            (target / name).write_bytes(data)
    else:
        with tempfile.TemporaryDirectory(prefix="coworkerd-patches-") as temporary:
            environment = {**os.environ, "GIT_INDEX_FILE": str(Path(temporary) / "expected")}
            git(checkout, "read-tree", base, env=environment)
            for patch in patches:
                git(checkout, "apply", "--cached", str(patch), env=environment)
            expected_tree = git(checkout, "write-tree", env=environment).decode().strip()
            expected = git(checkout, "diff", "--cached", "--binary", "--full-index", "--no-ext-diff", "--no-renames", base, env=environment)
            environment["GIT_INDEX_FILE"] = str(Path(temporary) / "actual")
            git(checkout, "read-tree", "HEAD", env=environment)
            git(checkout, "add", "-A", env=environment)
            actual_tree = git(checkout, "write-tree", env=environment).decode().strip()
            actual = git(checkout, "diff", "--cached", "--binary", "--full-index", "--no-ext-diff", "--no-renames", base, env=environment)
            assert actual == expected, "complete checkout difference does not match patch sequence byte for byte"
            paths = git(checkout, "diff", "--cached", "--name-only", base, env=environment).decode().splitlines()
            assert paths and all(any(allowed_path(name, p) for name in series) for p in paths), paths
            assert actual_tree == expected_tree, "complete Git trees differ"
        return {"passed": True, "upstream": upstream, "diffSha256": hashlib.sha256(actual).hexdigest(),
                "expectedTree": expected_tree, "actualTree": actual_tree, "modifiedFiles": paths, "patchScopes": scopes}
    return {"passed": True, "operation": action, "upstream": upstream, "patches": series, "patchScopes": scopes}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("apply", "import", "export", "check"))
    parser.add_argument("--checkout", type=Path, default=ROOT / "workerd")
    parser.add_argument("--patch-dir", type=Path, default=ROOT / "patches/workerd")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    print(json.dumps(operate(args.operation, args.checkout, args.patch_dir, args.output_dir), indent=2))
