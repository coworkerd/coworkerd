#pragma once

#include <kj/compat/http.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

// Release metadata: scripts/release.sh writes coworkerd-version.h into the build's generated
// directory before it builds; a development build has none.
#if __has_include("coworkerd-version.h")
#include "coworkerd-version.h"
#endif
#ifndef COWORKERD_VERSION
#define COWORKERD_VERSION "0.0.0-dev"
#endif
#ifndef COWORKERD_SOURCE
#define COWORKERD_SOURCE "unknown"
#endif
#ifndef COWORKERD_WORKERD_UPSTREAM
#define COWORKERD_WORKERD_UPSTREAM "unknown"
#endif
#ifndef COWORKERD_PATCH_SERIES
#define COWORKERD_PATCH_SERIES "unknown"
#endif
#ifndef COWORKERD_PATCH_SERIES_SHA256
#define COWORKERD_PATCH_SERIES_SHA256 "unknown"
#endif

// Helpers both daemons use.
namespace coworkerd {

// The test clock offset of `clockOffsetMsForTest` (the node sets it from its configuration); 0 elsewhere.
extern int64_t clockOffsetMsForTest;
// argv[0] of this process (each main sets it), for executableDirectory() where there is no /proc.
extern const char* executableArgument;

std::string readFile(const std::filesystem::path& path);
bool safeName(const std::string& value);
// A node's id: a safe name that is also one segment of an object key (members/<id>/, nodes/<id>/): not "." or "..".
bool nodeIdName(const std::string& value);
std::string sha256Hex(std::string_view value);
// `Authorization: Bearer <token>` as the management endpoints and the control plane take
// it: "" when it is the token, else what was wrong -- "missing", "malformed" (repeated, or not Bearer)
// or "wrong". Compared in time independent of where the bytes differ; the token is never echoed.
std::string bearerRefusal(const std::string& token, const kj::HttpHeaders& headers);
std::string jsonEscape(const std::string& value);
// A management endpoint without its token: 401, the same answer on every listener (and the control plane's).
kj::Promise<void> sendUnauthorized(kj::HttpService::Response& response, const kj::HttpHeaderTable& table);
uint64_t wallTimeMsForTest();
double monotonicTimeMs();

// The directory of this executable (the release keeps workerd beside it, the runtime in ../runtime).
std::filesystem::path executableDirectory();
// The SHA-256 of the launcher beside this executable, "none" when there is none (macOS, or a
// build without it). Read once.
const std::string& launcherSha256();
// What `--version` prints (with this daemon's name) and the state endpoint shows: this release,
// the source it was built from, and the workerd it expects; also the launcher's hash.
std::string versionJson();
std::string versionText(const char* name);

// The rendezvous order of a Worker's candidates (its Home first): the placement of the node and the
// deployment tool's trial order.
std::vector<std::string> rendezvousOrder(const std::string& worker,
    const std::vector<std::string>& ids);

}  // namespace coworkerd
