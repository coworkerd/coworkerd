cmake_minimum_required(VERSION 3.24)
project(coworkerd_v1 LANGUAGES CXX)

include(FetchContent)
find_package(OpenSSL REQUIRED)

set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(WITH_OPENSSL OFF CACHE STRING "" FORCE)
set(WITH_ZLIB OFF CACHE STRING "" FORCE)
set(WITH_FIBERS OFF CACHE BOOL "" FORCE)

# Keep Cap'n Proto aligned with workerd v1.20260827.1.
FetchContent_Declare(
  capnproto
  GIT_REPOSITORY https://github.com/capnproto/capnproto.git
  GIT_TAG 59cc025fa0c04d083aafb57604a28e72eb506037
  SOURCE_SUBDIR c++
)
FetchContent_MakeAvailable(capnproto)

# Patch KJ's HTTP disconnect handling and the Cap'n Proto HTTP adapter's
# ownership of borrowed request data through upload acknowledgment.
find_package(Git REQUIRED)
foreach(patch IN ITEMS kj-http-abort.diff capnp-http-upload.diff kj-peer-half-close.diff)
  set(patch_file "${CMAKE_CURRENT_SOURCE_DIR}/patches/${patch}")
  execute_process(COMMAND "${GIT_EXECUTABLE}" apply --reverse --check "${patch_file}"
    WORKING_DIRECTORY "${capnproto_SOURCE_DIR}" RESULT_VARIABLE patch_check
    OUTPUT_QUIET ERROR_QUIET)
  if(NOT patch_check EQUAL 0)
    execute_process(COMMAND "${GIT_EXECUTABLE}" apply --check "${patch_file}"
      WORKING_DIRECTORY "${capnproto_SOURCE_DIR}" COMMAND_ERROR_IS_FATAL ANY)
    execute_process(COMMAND "${GIT_EXECUTABLE}" apply "${patch_file}"
      WORKING_DIRECTORY "${capnproto_SOURCE_DIR}" COMMAND_ERROR_IS_FATAL ANY)
  endif()
endforeach()

set(GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${GENERATED_DIR}")

set(CAPNPC_OUTPUT_DIR "${GENERATED_DIR}")
set(CAPNPC_SRC_PREFIX "${capnproto_SOURCE_DIR}/c++/src")
capnp_generate_cpp(
  HTTP_CAPNP_SOURCES
  HTTP_CAPNP_HEADERS
  "${capnproto_SOURCE_DIR}/c++/src/capnp/compat/byte-stream.capnp"
  "${capnproto_SOURCE_DIR}/c++/src/capnp/compat/http-over-capnp.capnp"
)

add_library(capnp-http-over
  ${HTTP_CAPNP_SOURCES}
  ${HTTP_CAPNP_HEADERS}
  "${capnproto_SOURCE_DIR}/c++/src/capnp/compat/byte-stream.c++"
  "${capnproto_SOURCE_DIR}/c++/src/capnp/compat/http-over-capnp.c++"
)
target_include_directories(capnp-http-over PUBLIC
  "${GENERATED_DIR}"
  "${capnproto_SOURCE_DIR}/c++/src"
)
target_link_libraries(capnp-http-over PUBLIC capnp-rpc capnp-json kj-http)

set(CAPNPC_OUTPUT_DIR "${GENERATED_DIR}")
set(CAPNPC_SRC_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/common/schema")
set(CAPNPC_IMPORT_DIRS "${capnproto_SOURCE_DIR}/c++/src")
capnp_generate_cpp(
  COWORKERD_SOURCES
  COWORKERD_HEADERS
  "${CMAKE_CURRENT_SOURCE_DIR}/common/schema/coworkerd.capnp"
)

# The node's outbound TLS (its egress service connects to tenants' destinations itself): KJ's TLS layer, compiled
# from the same Cap'n Proto sources against the system's OpenSSL; Cap'n Proto itself stays built without OpenSSL.
add_library(kj-tls-local STATIC
  "${capnproto_SOURCE_DIR}/c++/src/kj/compat/tls.c++"
  "${capnproto_SOURCE_DIR}/c++/src/kj/compat/readiness-io.c++"
)
target_compile_definitions(kj-tls-local PRIVATE KJ_HAS_OPENSSL=1)
target_include_directories(kj-tls-local PUBLIC "${capnproto_SOURCE_DIR}/c++/src")
target_link_libraries(kj-tls-local PUBLIC kj-async OpenSSL::SSL OpenSSL::Crypto)

# One binary per role (docs/components.md). `common` is what both daemons share -- the object store client and
# the state on it, the JSON helpers, the deployment state model, the configuration, the cleaning of reasons and the
# log sink, the schema. The node daemon `coworkerd` sees common/ and node/, the control plane `coworkerd-apiserver` common/
# and apiserver/: neither has the other's directory on its include path or links its sources.
add_library(common STATIC
  common/json.c++
  common/object-store.c++
  common/object-state.c++
  common/tenant-manifest.c++
  common/code-package.c++
  common/deployment.c++
  common/tenancy.c++
  common/config.c++
  common/cpu-capacity.c++
  common/support.c++
  common/log-cleaning.c++
  common/log-sink.c++
  ${COWORKERD_SOURCES}
  ${COWORKERD_HEADERS}
)
target_compile_features(common PUBLIC cxx_std_23)
target_include_directories(common PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/common" "${GENERATED_DIR}")
target_link_libraries(common PUBLIC capnp-http-over OpenSSL::Crypto)

set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "runtime/platform.js" "runtime/sql-database.js")
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/runtime/platform.js" COWORKERD_PLATFORM_SOURCE)
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/runtime/sql-database.js" COWORKERD_SQL_DATABASE_SOURCE)
configure_file(node/platform-source.h.in "${GENERATED_DIR}/platform-source.h" @ONLY)

add_library(coworkerd-node OBJECT
  node/main.c++
  node/process-pool.c++
  node/isolate-saturation.c++
  node/secret-dictionary.c++
  node/async-log.c++
  node/memory-watermark.c++
  node/host-check.c++
  node/suspects.c++
  node/membership.c++
  node/durable-shards.c++
  node/durable-log.c++
  node/alarm-publication.c++
  node/alarm-discovery.c++
  node/recovery-history.c++
  node/database-seed.c++
  node/root-objects.c++
  node/platform-definition.c++
  node/bundle-cache.c++
  node/desired-state.c++
  node/desired-state-store.c++
  node/tenants.c++
  node/usage.c++
  node/storage-probe.c++
  node/egress.c++
  node/peer-auth.c++
  node/blob-store.c++
  node/v8-sandbox.c++
)
target_include_directories(coworkerd-node PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/node")
target_link_libraries(coworkerd-node PUBLIC common kj-tls-local)

add_executable(coworkerd node/platform-fixture.c++)
target_link_libraries(coworkerd PRIVATE coworkerd-node)

add_executable(coworkerd-platform-test EXCLUDE_FROM_ALL node/platform-fixture.c++)
target_compile_definitions(coworkerd-platform-test PRIVATE COWORKERD_PLATFORM_FIXTURE)
target_link_libraries(coworkerd-platform-test PRIVATE coworkerd-node)

add_executable(coworkerd-platform-definition-test EXCLUDE_FROM_ALL
  node/platform-definition-test.c++ node/platform-definition.c++ node/platform-fixture.c++)
target_link_libraries(coworkerd-platform-definition-test PRIVATE common)
add_test(NAME node-platform-definition COMMAND coworkerd-platform-definition-test)

add_executable(coworkerd-apiserver
  apiserver/main.c++
  apiserver/database-seed-verification.c++
  apiserver/database-time-travel.c++
  apiserver/deployment-tool.c++
  apiserver/manifests.c++
  apiserver/tenants.c++
  apiserver/rotation-tool.c++
)
target_include_directories(coworkerd-apiserver PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/apiserver")
target_link_libraries(coworkerd-apiserver PRIVATE common)

# The workerd processes' launcher: Linux only; the node starts workerd through it when the sandbox
# is on (launcher/PROTOCOL.md). It reads the release metadata from the same generated header.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  enable_language(C)
  add_executable(coworkerd-launcher launcher/launcher.c)
  target_include_directories(coworkerd-launcher PRIVATE "${GENERATED_DIR}")
  target_compile_options(coworkerd-launcher PRIVATE -O2 -Wall -Wextra)
  add_dependencies(coworkerd coworkerd-launcher)
endif()

# A component test against the actual durable log and reader, without running a cluster.
capnp_generate_cpp(DURABLE_TAIL_TEST_SOURCES DURABLE_TAIL_TEST_HEADERS
  "${CMAKE_CURRENT_SOURCE_DIR}/common/schema/durable-tail-legacy-test.capnp")
add_executable(coworkerd-durable-channel-test EXCLUDE_FROM_ALL
  node/durable-channel-test.c++
  node/durable-tail-test.c++
  node/durable-segment-test.c++
  ${DURABLE_TAIL_TEST_SOURCES}
  ${DURABLE_TAIL_TEST_HEADERS}
  node/memory-watermark.c++
  node/durable-log.c++
  node/alarm-publication.c++
  node/alarm-discovery.c++
  node/recovery-history.c++
  node/root-objects.c++
  node/durable-shards.c++
  node/membership.c++
  node/blob-store.c++
  node/peer-auth.c++
)
target_include_directories(coworkerd-durable-channel-test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/node")
target_link_libraries(coworkerd-durable-channel-test PRIVATE common kj-tls-local)

# This gate runs the already built unit/component tests without starting a cluster.
enable_testing()
add_executable(coworkerd-process-culprit-test EXCLUDE_FROM_ALL node/process-culprit-test.c++)
target_link_libraries(coworkerd-process-culprit-test PRIVATE common)
add_test(NAME node-threaded-cpu-culprit COMMAND coworkerd-process-culprit-test cpu)
add_test(NAME node-threaded-memory-culprit COMMAND coworkerd-process-culprit-test memory)
add_test(NAME node-threaded-fatal-culprit COMMAND coworkerd-process-culprit-test fatal)
add_executable(coworkerd-log-cleaning-test EXCLUDE_FROM_ALL node/log-cleaning-test.c++ node/async-log.c++
  node/secret-dictionary.c++)
target_link_libraries(coworkerd-log-cleaning-test PRIVATE common)
set(COWORKERD_TEST_WORKERD "auto" CACHE STRING
  "Patched workerd for kernel component tests: auto detects a fetched Linux or in-tree binary; empty omits kernel tests")
# Resolve the cached choice on every configure: a kernel built later must enable
# its tests in this same build tree. Keep an explicitly empty choice empty.
set(COWORKERD_TEST_WORKERD_PATH "${COWORKERD_TEST_WORKERD}")
if(COWORKERD_TEST_WORKERD STREQUAL "auto")
  set(COWORKERD_TEST_WORKERD_PATH "")
  if(CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux" AND NOT CMAKE_CROSSCOMPILING AND
      CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|aarch64|arm64)$" AND
      IS_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/kernel")
    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    execute_process(COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/scripts/fetch-kernel.py" --installed
      OUTPUT_VARIABLE COWORKERD_INSTALLED_KERNEL OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND_ERROR_IS_FATAL ANY)
    if(COWORKERD_INSTALLED_KERNEL)
      set(COWORKERD_TEST_WORKERD_PATH "${COWORKERD_INSTALLED_KERNEL}/workerd")
    endif()
  endif()
  if(COWORKERD_TEST_WORKERD_PATH STREQUAL "")
    set(COWORKERD_TEST_WORKERD_PATH "${CMAKE_CURRENT_SOURCE_DIR}/workerd/bazel-bin/src/workerd/server/workerd")
  endif()
  if(NOT EXISTS "${COWORKERD_TEST_WORKERD_PATH}")
    set(COWORKERD_TEST_WORKERD_PATH "")
  endif()
endif()
add_test(NAME node-log-stream COMMAND coworkerd-log-cleaning-test streams)
add_test(NAME node-log-many-values COMMAND coworkerd-log-cleaning-test many-values)
add_test(NAME node-log-many-values-equivalence COMMAND coworkerd-log-cleaning-test many-values-equivalence)
add_test(NAME node-log-multi-line-values COMMAND coworkerd-log-cleaning-test multi-line-values)
add_test(NAME node-log-generations COMMAND coworkerd-log-cleaning-test generations)
add_test(NAME node-secret-rotations COMMAND coworkerd-log-cleaning-test secret-rotations)
add_test(NAME node-secret-limits COMMAND coworkerd-log-cleaning-test secret-limits)
add_test(NAME node-secret-share COMMAND coworkerd-log-cleaning-test secret-share)
add_test(NAME node-secret-pinned-rotation COMMAND coworkerd-log-cleaning-test secret-pinned-rotation)
add_test(NAME node-secret-rotation-cooldown COMMAND coworkerd-log-cleaning-test secret-rotation-cooldown)
add_test(NAME node-log-stale COMMAND coworkerd-log-cleaning-test stale-lines)
set_tests_properties(node-log-stream node-log-many-values PROPERTIES TIMEOUT 30)
set_tests_properties(node-log-generations node-secret-rotations node-secret-limits node-secret-share
  node-secret-pinned-rotation node-secret-rotation-cooldown node-log-stale PROPERTIES TIMEOUT 60)
add_executable(coworkerd-node-contract-test EXCLUDE_FROM_ALL
  node/node-contract-test.c++ node/bundle-cache.c++ node/isolate-saturation.c++ apiserver/manifests.c++)
target_include_directories(coworkerd-node-contract-test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/apiserver")
target_link_libraries(coworkerd-node-contract-test PRIVATE common)
add_test(NAME durable-channel COMMAND coworkerd-durable-channel-test)
add_test(NAME durable-upstream-channel-eof COMMAND coworkerd-durable-channel-test upstream-channel-eof)
add_test(NAME durable-threaded-channel COMMAND coworkerd-durable-channel-test threaded-channel)
add_test(NAME durable-parked-websocket COMMAND coworkerd-durable-channel-test parked-websocket)
add_test(NAME durable-websocket-frame-budget COMMAND coworkerd-durable-channel-test websocket-frame-budget)
add_test(NAME durable-websocket-upload-budget COMMAND coworkerd-durable-channel-test websocket-upload-budget)
add_test(NAME durable-threaded-alarms COMMAND coworkerd-durable-channel-test threaded-alarms)
add_test(NAME durable-index COMMAND coworkerd-durable-channel-test index)
add_test(NAME durable-tail-equivalence COMMAND coworkerd-durable-channel-test tail)
add_test(NAME durable-tail-negative-controls COMMAND coworkerd-durable-channel-test tail-negative)
foreach(case admission requeue final-round final-per-object live-owner local-restart)
  add_test(NAME durable-recovery-${case} COMMAND coworkerd-durable-channel-test recovery-${case})
endforeach()
add_test(NAME durable-segment-equivalence COMMAND coworkerd-durable-channel-test segment)
add_test(NAME durable-segment-negative-controls COMMAND coworkerd-durable-channel-test segment-negative)
add_test(NAME durable-segment-cache COMMAND coworkerd-durable-channel-test segment-cache)
add_test(NAME durable-earlier-segment COMMAND coworkerd-durable-channel-test earlier-segment)
add_test(NAME durable-earlier-publish-gate COMMAND coworkerd-durable-channel-test publish-gate)
add_test(NAME durable-restored-tail COMMAND coworkerd-durable-channel-test restored-tail)
add_test(NAME durable-restored-tail-negative-controls COMMAND coworkerd-durable-channel-test restored-tail-negative)
# Freed memory is overwritten, so that a load touching its destroyed cache fails this test instead of passing by luck.
set_tests_properties(durable-segment-cache PROPERTIES ENVIRONMENT "MALLOC_PERTURB_=165")
add_test(NAME durable-lease-tracker COMMAND coworkerd-durable-channel-test lease-tracker)
foreach(case held expired stale-timer slow-flush rotate completion-error wait wait-expired old-epoch not-held)
  add_test(NAME durable-lease-${case} COMMAND coworkerd-durable-channel-test lease-confirm ${case})
endforeach()
foreach(case raise unverified old-ask coalesce outage)
  add_test(NAME durable-peer-fence-${case} COMMAND coworkerd-durable-channel-test peer-fence ${case})
endforeach()
foreach(case fenced full full-resend resend-held)
  add_test(NAME durable-refused-peer-${case} COMMAND coworkerd-durable-channel-test refused-peer ${case})
endforeach()
foreach(case fenced full cut-short limit corrupt fenced-reconnect full-reconnect cut-short-reconnect limit-reconnect
    corrupt-reconnect chunk pair-full pair-fenced fenced-drop fenced-unshown full-keeps fenced-drop-resend fenced-known
    reacquired unreachable)
  add_test(NAME durable-refused-owner-${case} COMMAND coworkerd-durable-channel-test refused-owner ${case})
endforeach()
foreach(case native-config tenant-execution deadlines event-loop shutdown-signals once-per-turn memory-events durable-not-sent tenant-line-truncation trial-verdicts leases tree-lease budget identity admission placement cpu-saturation isolate-saturation package module-types cache
    cache-unavailable cache-deadline cache-capacity cache-single-flight cpu-culprit-current-behavior memory-culprit rejected-manifest)
  add_test(NAME node-${case} COMMAND coworkerd-node-contract-test ${case})
endforeach()
add_test(NAME node-ingress-descriptor-plan COMMAND coworkerd-node-contract-test ingress-descriptor-plan)
add_test(NAME node-peer-half-close COMMAND coworkerd-node-contract-test peer-half-close)
add_test(NAME node-peer-session-liveness COMMAND coworkerd-node-contract-test peer-session-liveness)
add_test(NAME node-live-request-lease COMMAND coworkerd-node-contract-test live-request-lease)
# The object store client's kept-alive connections, against a loopback store of the test's own.
add_executable(coworkerd-object-store-test EXCLUDE_FROM_ALL common/object-store-test.c++)
target_link_libraries(coworkerd-object-store-test PRIVATE common)
foreach(case reuse broken retry-rules timeout limits heartbeat extra-answer)
  add_test(NAME object-store-${case} COMMAND coworkerd-object-store-test ${case})
endforeach()
set_tests_properties(object-store-reuse object-store-broken object-store-retry-rules object-store-timeout
  object-store-limits object-store-heartbeat object-store-extra-answer PROPERTIES TIMEOUT 60)
add_executable(coworkerd-config-test EXCLUDE_FROM_ALL common/config-test.c++)
target_link_libraries(coworkerd-config-test PRIVATE common)
add_test(NAME node-config-idle COMMAND coworkerd-config-test idle)
add_test(NAME node-config-memory COMMAND coworkerd-config-test memory)
add_test(NAME node-config-tenant-borrow COMMAND coworkerd-config-test tenant-borrow)
add_test(NAME node-config-websocket COMMAND coworkerd-config-test websocket)
add_test(NAME node-config-liveness-grace COMMAND coworkerd-config-test liveness-grace)
add_executable(coworkerd-v8-sandbox-test EXCLUDE_FROM_ALL node/v8-sandbox-test.c++ node/v8-sandbox.c++)
target_include_directories(coworkerd-v8-sandbox-test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/node")
target_link_libraries(coworkerd-v8-sandbox-test PRIVATE common)
foreach(case guard-regions scale lowered off-build insecure-build required unknown machine-gaps map-count-first parse probe)
  add_test(NAME node-v8-sandbox-${case} COMMAND coworkerd-v8-sandbox-test ${case})
endforeach()
set(COWORKERD_TEST_JUNIT "" CACHE FILEPATH "Optional JUnit result path for the check target")
set(COWORKERD_TEST_REPORT_ARGS)
if(COWORKERD_TEST_JUNIT)
  list(APPEND COWORKERD_TEST_REPORT_ARGS --output-junit "${COWORKERD_TEST_JUNIT}")
endif()
add_custom_target(check
  COMMAND "${CMAKE_CTEST_COMMAND}" --output-on-failure --no-tests=error -C $<CONFIG> ${COWORKERD_TEST_REPORT_ARGS}
  DEPENDS coworkerd-durable-channel-test coworkerd-node-contract-test coworkerd-process-culprit-test coworkerd-log-cleaning-test coworkerd-config-test
    coworkerd-object-store-test coworkerd-v8-sandbox-test coworkerd-platform-definition-test coworkerd-platform-test
  USES_TERMINAL)

# The CLI tests use the Wrangler and Miniflare versions pinned in cli/package-lock.json.
find_program(COWORKERD_TEST_NODE NAMES node)
if(NOT COWORKERD_TEST_NODE)
  set(COWORKERD_TEST_NODE node)
endif()
# A configured patched Workerd is external to the CMake targets. This component
# starts a single process on a loopback port, with no node, control plane, or storage.
if(NOT COWORKERD_TEST_WORKERD_PATH STREQUAL "")
  if(NOT EXISTS "${COWORKERD_TEST_WORKERD_PATH}")
    message(FATAL_ERROR "COWORKERD_TEST_WORKERD does not exist: ${COWORKERD_TEST_WORKERD_PATH}")
  endif()
  add_test(NAME node-log-redaction COMMAND coworkerd-log-cleaning-test "${COWORKERD_TEST_WORKERD_PATH}")
  set_tests_properties(node-log-redaction PROPERTIES TIMEOUT 30)
  add_test(NAME workerd-configuration COMMAND "${COWORKERD_TEST_NODE}"
    "${CMAKE_CURRENT_SOURCE_DIR}/node/workerd-configuration.test.mjs" "${COWORKERD_TEST_WORKERD_PATH}")
  set_tests_properties(workerd-configuration PROPERTIES TIMEOUT 15)
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_test(NAME node-child-memory COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/node-child-memory.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js")
    set_tests_properties(node-child-memory PROPERTIES TIMEOUT 15)
    add_test(NAME node-threaded-durable-storage-lifecycle COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-storage-lifecycle.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js")
    set_tests_properties(node-threaded-durable-storage-lifecycle PROPERTIES TIMEOUT 35)
    add_test(NAME node-threaded-durable-process-memory COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-process-memory.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js")
    set_tests_properties(node-threaded-durable-process-memory PROPERTIES TIMEOUT 30)
    add_test(NAME node-threaded-durable-process-heap COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-process-memory.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" heap)
    set_tests_properties(node-threaded-durable-process-heap PROPERTIES TIMEOUT 30)
    add_test(NAME node-threaded-durable-reset-progress COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-reset-progress.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js")
    set_tests_properties(node-threaded-durable-reset-progress PROPERTIES TIMEOUT 25)
    add_test(NAME node-threaded-durable-cpu-events COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" cpu-events)
    set_tests_properties(node-threaded-durable-cpu-events PROPERTIES TIMEOUT 30)
    add_test(NAME node-threaded-durable-launch COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js")
    set_tests_properties(node-threaded-durable-launch PROPERTIES TIMEOUT 20)
    add_test(NAME node-threaded-durable-home-reservation COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" home-reservation)
    set_tests_properties(node-threaded-durable-home-reservation PROPERTIES TIMEOUT 20)
    add_test(NAME node-threaded-durable-warm-copies COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" warm-copies)
    set_tests_properties(node-threaded-durable-warm-copies PROPERTIES TIMEOUT 30)
    add_test(NAME node-threaded-durable-load-cancel COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" load-cancel)
    set_tests_properties(node-threaded-durable-load-cancel PROPERTIES TIMEOUT 30)
    add_test(NAME node-threaded-durable-rpc-events COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" events)
    set_tests_properties(node-threaded-durable-rpc-events PROPERTIES TIMEOUT 40)
    add_test(NAME node-threaded-durable-rpc-output-gate COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" rpc-output-gate)
    set_tests_properties(node-threaded-durable-rpc-output-gate PROPERTIES TIMEOUT 20)
    add_test(NAME node-threaded-durable-rpc-cpu COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" rpc-cpu-events)
    set_tests_properties(node-threaded-durable-rpc-cpu PROPERTIES TIMEOUT 30)
    add_test(NAME node-threaded-durable-http-isolation COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" http-isolation)
    set_tests_properties(node-threaded-durable-http-isolation PROPERTIES TIMEOUT 45)
    add_test(NAME node-threaded-durable-http-output-gate COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" http-output-gate)
    set_tests_properties(node-threaded-durable-http-output-gate PROPERTIES TIMEOUT 20)
    add_test(NAME node-threaded-durable-http-overlap COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" http-overlap)
    set_tests_properties(node-threaded-durable-http-overlap PROPERTIES TIMEOUT 20)
    add_test(NAME node-threaded-durable-group-refusal COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" group-refusal)
    set_tests_properties(node-threaded-durable-group-refusal PROPERTIES TIMEOUT 20)
    add_test(NAME node-threaded-durable-http-memory COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" http-memory 4 64 64)
    set_tests_properties(node-threaded-durable-http-memory PROPERTIES TIMEOUT 30)
    add_test(NAME node-threaded-durable-http COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" http 4 64)
    set_tests_properties(node-threaded-durable-http PROPERTIES TIMEOUT 75)
    add_test(NAME node-threaded-durable-placement COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" placement 4 64 3000)
    set_tests_properties(node-threaded-durable-placement PROPERTIES TIMEOUT 30)
    add_test(NAME node-threaded-durable-queues COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" queues 4 64)
    add_test(NAME node-threaded-durable-fd-limit COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" queue-limit 4 64)
    set_tests_properties(node-threaded-durable-queues node-threaded-durable-fd-limit PROPERTIES TIMEOUT 30)
    add_test(NAME node-threaded-durable-memory-reservation COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/threaded-durable-launch.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js" queue-memory 4 256)
    set_tests_properties(node-threaded-durable-memory-reservation PROPERTIES TIMEOUT 30)
    add_dependencies(check coworkerd)
  endif()
else()
  message(STATUS "Kernel tests omitted (no COWORKERD_TEST_WORKERD): node-log-redaction, workerd-configuration, node-child-memory, node-threaded-durable-launch, node-threaded-durable-rpc-events, node-threaded-durable-rpc-output-gate, node-threaded-durable-rpc-cpu, node-threaded-durable-http-isolation, node-threaded-durable-http-output-gate, node-threaded-durable-http-memory, node-threaded-durable-http-overlap, node-threaded-durable-group-refusal, node-threaded-durable-http, node-threaded-durable-placement, node-threaded-durable-queues, node-threaded-durable-fd-limit and node-threaded-durable-memory-reservation (Linux only)")
endif()
foreach(case export import migrations project-config)
  add_test(NAME cli-${case} COMMAND "${COWORKERD_TEST_NODE}" --test "${CMAKE_CURRENT_SOURCE_DIR}/cli/test/${case}.test.mjs")
endforeach()
add_test(NAME node-error-contract COMMAND "${COWORKERD_TEST_NODE}" --test "${CMAKE_CURRENT_SOURCE_DIR}/node/error-contract.test.mjs")

set(COWORKERD_NATIVE_TEST_LOG "" CACHE FILEPATH "Output of the native KJ contract test")
set(COWORKERD_NATIVE_TEST_RECEIPT "" CACHE FILEPATH "Native test source and result receipt")
set(COWORKERD_NATIVE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/workerd" CACHE PATH "Native source tested by the receipt")
if(COWORKERD_NATIVE_TEST_LOG AND COWORKERD_NATIVE_TEST_RECEIPT)
  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  add_test(NAME native-error-contract COMMAND "${Python3_EXECUTABLE}"
    "${CMAKE_CURRENT_SOURCE_DIR}/scripts/test-native-error-contract.py"
    --native-source "${COWORKERD_NATIVE_SOURCE_DIR}" --receipt "${COWORKERD_NATIVE_TEST_RECEIPT}"
    --log "${COWORKERD_NATIVE_TEST_LOG}" --errors "${CMAKE_CURRENT_SOURCE_DIR}/docs/guide/errors.md")
else()
  message(STATUS "Native kernel test omitted (no log/receipt): native-error-contract")
endif()
