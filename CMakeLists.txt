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
  node/async-log.c++
  node/memory-watermark.c++
  node/suspects.c++
  node/membership.c++
  node/bundle-cache.c++
  node/desired-state.c++
  node/desired-state-store.c++
  node/tenants.c++
  node/usage.c++
  node/storage-probe.c++
  node/egress.c++
  node/peer-auth.c++
)
target_include_directories(coworkerd PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/node")
target_link_libraries(coworkerd PRIVATE common kj-tls-local)

add_executable(coworkerd-apiserver
  apiserver/main.c++
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
