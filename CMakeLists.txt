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

# KJ's HTTP entity writers must honor AsyncOutputStream::abortWrite(). Without
# this, destroying a chunked writer after an upstream read failure sends EOF.
find_package(Git REQUIRED)
set(KJ_HTTP_ABORT_PATCH "${CMAKE_CURRENT_SOURCE_DIR}/patches/kj-http-abort.diff")
execute_process(COMMAND "${GIT_EXECUTABLE}" apply --reverse --check "${KJ_HTTP_ABORT_PATCH}"
  WORKING_DIRECTORY "${capnproto_SOURCE_DIR}" RESULT_VARIABLE patch_check
  OUTPUT_QUIET ERROR_QUIET)
if(NOT patch_check EQUAL 0)
  execute_process(COMMAND "${GIT_EXECUTABLE}" apply --check "${KJ_HTTP_ABORT_PATCH}"
    WORKING_DIRECTORY "${capnproto_SOURCE_DIR}" COMMAND_ERROR_IS_FATAL ANY)
  execute_process(COMMAND "${GIT_EXECUTABLE}" apply "${KJ_HTTP_ABORT_PATCH}"
    WORKING_DIRECTORY "${capnproto_SOURCE_DIR}" COMMAND_ERROR_IS_FATAL ANY)
endif()

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

add_executable(coworkerd
  node/main.c++
  node/process-pool.c++
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
  node/bundle-cache.c++
  node/desired-state.c++
  node/desired-state-store.c++
  node/tenants.c++
  node/usage.c++
  node/storage-probe.c++
  node/egress.c++
  node/peer-auth.c++
  node/blob-store.c++
)
target_include_directories(coworkerd PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/node")
target_link_libraries(coworkerd PRIVATE common kj-tls-local)

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
add_executable(coworkerd-log-cleaning-test EXCLUDE_FROM_ALL node/log-cleaning-test.c++ node/async-log.c++
  node/secret-dictionary.c++)
target_link_libraries(coworkerd-log-cleaning-test PRIVATE common)
set(COWORKERD_TEST_WORKERD "${CMAKE_CURRENT_SOURCE_DIR}/workerd/bazel-bin/src/workerd/server/workerd"
  CACHE FILEPATH "Built workerd for the real Worker diagnostic redaction test")
add_test(NAME node-log-stream COMMAND coworkerd-log-cleaning-test streams)
add_test(NAME node-log-many-values COMMAND coworkerd-log-cleaning-test many-values)
add_test(NAME node-log-generations COMMAND coworkerd-log-cleaning-test generations)
add_test(NAME node-secret-rotations COMMAND coworkerd-log-cleaning-test secret-rotations)
add_test(NAME node-secret-limits COMMAND coworkerd-log-cleaning-test secret-limits)
add_test(NAME node-secret-share COMMAND coworkerd-log-cleaning-test secret-share)
add_test(NAME node-secret-pinned-rotation COMMAND coworkerd-log-cleaning-test secret-pinned-rotation)
add_test(NAME node-secret-rotation-cooldown COMMAND coworkerd-log-cleaning-test secret-rotation-cooldown)
add_test(NAME node-log-stale COMMAND coworkerd-log-cleaning-test stale-lines)
add_test(NAME node-log-redaction COMMAND coworkerd-log-cleaning-test "${COWORKERD_TEST_WORKERD}")
set_tests_properties(node-log-stream node-log-many-values node-log-redaction PROPERTIES TIMEOUT 30)
set_tests_properties(node-log-generations node-secret-rotations node-secret-limits node-secret-share
  node-secret-pinned-rotation node-secret-rotation-cooldown node-log-stale PROPERTIES TIMEOUT 60)
add_executable(coworkerd-node-contract-test EXCLUDE_FROM_ALL
  node/node-contract-test.c++ node/bundle-cache.c++ apiserver/manifests.c++)
target_include_directories(coworkerd-node-contract-test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/apiserver")
target_link_libraries(coworkerd-node-contract-test PRIVATE common)
add_test(NAME durable-channel COMMAND coworkerd-durable-channel-test)
add_test(NAME durable-index COMMAND coworkerd-durable-channel-test index)
add_test(NAME durable-tail-equivalence COMMAND coworkerd-durable-channel-test tail)
add_test(NAME durable-tail-negative-controls COMMAND coworkerd-durable-channel-test tail-negative)
add_test(NAME durable-lease-tracker COMMAND coworkerd-durable-channel-test lease-tracker)
foreach(case held expired stale-timer slow-flush wait wait-expired old-epoch not-held)
  add_test(NAME durable-lease-${case} COMMAND coworkerd-durable-channel-test lease-confirm ${case})
endforeach()
foreach(case native-config tenant-execution deadlines event-loop trial-verdicts leases tree-lease budget identity admission placement package module-types cache
    cache-unavailable cache-deadline cache-capacity cache-single-flight cpu-culprit-current-behavior memory-culprit rejected-manifest)
  add_test(NAME node-${case} COMMAND coworkerd-node-contract-test ${case})
endforeach()
add_executable(coworkerd-config-test EXCLUDE_FROM_ALL common/config-test.c++)
target_link_libraries(coworkerd-config-test PRIVATE common)
add_test(NAME node-config-idle COMMAND coworkerd-config-test idle)
add_test(NAME node-config-memory COMMAND coworkerd-config-test memory)
add_test(NAME node-config-tenant-borrow COMMAND coworkerd-config-test tenant-borrow)
add_test(NAME node-config-liveness-grace COMMAND coworkerd-config-test liveness-grace)
set(COWORKERD_TEST_JUNIT "" CACHE FILEPATH "Optional JUnit result path for the check target")
set(COWORKERD_TEST_REPORT_ARGS)
if(COWORKERD_TEST_JUNIT)
  list(APPEND COWORKERD_TEST_REPORT_ARGS --output-junit "${COWORKERD_TEST_JUNIT}")
endif()
add_custom_target(check
  COMMAND "${CMAKE_CTEST_COMMAND}" --output-on-failure --no-tests=error -C $<CONFIG> ${COWORKERD_TEST_REPORT_ARGS}
  DEPENDS coworkerd-durable-channel-test coworkerd-node-contract-test coworkerd-process-culprit-test coworkerd-log-cleaning-test coworkerd-config-test
  USES_TERMINAL)

# The CLI tests use the Wrangler and Miniflare versions pinned in cli/package-lock.json.
find_program(COWORKERD_TEST_NODE NAMES node)
if(NOT COWORKERD_TEST_NODE)
  set(COWORKERD_TEST_NODE node)
endif()
# A configured patched Workerd is external to the CMake targets. This component
# starts a single process on a loopback port, with no node, control plane, or storage.
if(COWORKERD_TEST_WORKERD)
  if(NOT EXISTS "${COWORKERD_TEST_WORKERD}")
    message(FATAL_ERROR "COWORKERD_TEST_WORKERD does not exist: ${COWORKERD_TEST_WORKERD}")
  endif()
  add_test(NAME workerd-configuration COMMAND "${COWORKERD_TEST_NODE}"
    "${CMAKE_CURRENT_SOURCE_DIR}/node/workerd-configuration.test.mjs" "${COWORKERD_TEST_WORKERD}")
  set_tests_properties(workerd-configuration PROPERTIES TIMEOUT 15)
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_test(NAME node-child-memory COMMAND "${COWORKERD_TEST_NODE}"
      "${CMAKE_CURRENT_SOURCE_DIR}/node/node-child-memory.test.mjs" "$<TARGET_FILE:coworkerd>"
      "${COWORKERD_TEST_WORKERD}" "${CMAKE_CURRENT_SOURCE_DIR}/runtime/runtime.js")
    set_tests_properties(node-child-memory PROPERTIES TIMEOUT 15)
    add_dependencies(check coworkerd)
  endif()
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
endif()
