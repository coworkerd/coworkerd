// coworkerd-apiserver: the control plane's
// API and, as its internal subcommand `tool`, the deployment tool.
#include "config.h"
#include "control.h"
#include "deployment.h"
#include "json.h"
#include "log-cleaning.h"
#include "log-sink.h"
#include "object-store.h"
#include "object-state.h"
#include "support.h"

#include <capnp/compat/byte-stream.h>
#include <capnp/compat/http-over-capnp.h>
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>
#include <kj/compat/http.h>
#include <openssl/sha.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <queue>
#include <optional>
#include <random>
#include <unistd.h>
#include <sstream>
#include <cerrno>
#include <cstring>
#include <strings.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>


namespace fs = std::filesystem;


namespace {

// The configuration, the helpers and the cleaning both daemons share (common/).
using coworkerd::ServerConfig;
using coworkerd::SecretRedactions;
using coworkerd::bearerRefusal;
using coworkerd::cleanReason;
using coworkerd::clockOffsetMsForTest;
using coworkerd::executableArgument;
using coworkerd::executableDirectory;
using coworkerd::jsonEscape;
using coworkerd::launcherSha256;
using coworkerd::monotonicTimeMs;
using coworkerd::readFile;
using coworkerd::redactSecrets;
using coworkerd::runLogClock;
using coworkerd::safeName;
using coworkerd::secretRedactions;
using coworkerd::sendUnauthorized;
using coworkerd::sha256Hex;
using coworkerd::trialClean;
using coworkerd::versionJson;
using coworkerd::versionText;
using coworkerd::wallTimeMsForTest;

// `coworkerd-apiserver tool drain --server URL [--wait] [--timeout-ms N]`: starts the
// Server's exit (POST /__coworkerd/drain). With --wait it follows the exit until the Server
// has revoked its lease (GET /__coworkerd/drain?wait=1 answers just before the process exits)
// and no longer answers. Prints the Server's JSON answers, then a summary line; exits 0 once
// the Server is gone, 1 when it did not start draining or vanished before reporting its end,
// 2 on timeout.
int runDrainTool(int argc, char** argv) {
  constexpr auto USAGE = "usage: coworkerd-apiserver tool drain --server URL [--wait] [--timeout-ms N] [--token TOKEN]";
  std::string server;
  bool wait = false;
  uint64_t timeoutMs = 600000;
  // The management token: --token, else COWORKERD_MANAGEMENT_TOKEN.
  std::string token;
  if (const char* value = std::getenv("COWORKERD_MANAGEMENT_TOKEN")) token = value;
  for (int i = 0; i < argc; ++i) {
    std::string flag = argv[i];
    if (flag == "--wait") wait = true;
    else if (flag == "--server" && i + 1 < argc) server = argv[++i];
    else if (flag == "--timeout-ms" && i + 1 < argc) timeoutMs = std::stoull(argv[++i]);
    else if (flag == "--token" && i + 1 < argc) token = argv[++i];
    else KJ_FAIL_REQUIRE(USAGE, flag.c_str());
  }
  KJ_REQUIRE(!server.empty() && timeoutMs > 0, USAGE);
  while (server.ends_with('/')) server.pop_back();
  auto io = kj::setupAsyncIo();
  auto table = kj::HttpHeaderTable::Builder().build();
  auto& timer = io.provider->getTimer();
  auto send = [&](kj::HttpMethod method, const std::string& path, uint64_t limitMs) {
    // A fresh client per call: no pooled connection to a Server that is about to exit.
    auto client = kj::newHttpClient(timer, *table, io.provider->getNetwork(), kj::none);
    kj::HttpHeaders headers(*table);
    if (!token.empty()) headers.addPtr("Authorization", kj::str("Bearer ", token.c_str()));
    auto url = server + path;
    auto request = client->request(method, url.c_str(), headers, uint64_t(0));
    request.body = nullptr;
    auto response = timer.timeoutAfter(limitMs * kj::MILLISECONDS, kj::mv(request.response)).wait(io.waitScope);
    auto text = timer.timeoutAfter(limitMs * kj::MILLISECONDS, response.body->readAllText(64 << 20)).wait(io.waitScope);
    return std::pair<unsigned, std::string>(response.statusCode, text.cStr());
  };
  const auto started = monotonicTimeMs();
  auto [status, body] = send(kj::HttpMethod::POST, "/__coworkerd/drain", 30000);
  std::cout << body << std::endl;
  if (status != 200) {
    std::cerr << "the Server did not start draining (HTTP " << status << ")"
              << (status == 401 ? ": it requires the management token (--token or COWORKERD_MANAGEMENT_TOKEN)"
                                : "; a Server before v38 has no drain") << std::endl;
    return 1;
  }
  if (!wait) return 0;
  std::string record;
  // A Server with its drain turned off (drainTimeoutMs 0) exits at once: there is no end to report.
  const bool immediate = body.find("\"phase\":\"immediate\"") != std::string::npos;
  if (immediate) record = body;
  else try {
    auto remaining = static_cast<uint64_t>(std::max(1.0, double(timeoutMs) - (monotonicTimeMs() - started)));
    auto [finalStatus, finalBody] = send(kj::HttpMethod::GET, "/__coworkerd/drain?wait=1", remaining);
    std::cout << finalBody << std::endl;
    if (finalStatus != 200) { std::cerr << "unexpected answer while following the drain: HTTP " << finalStatus << std::endl; return 1; }
    record = finalBody;
  } catch (const kj::Exception& error) {
    if (error.getType() == kj::Exception::Type::OVERLOADED || monotonicTimeMs() - started >= double(timeoutMs)) {
      std::cerr << "timed out following the drain: " << error.getDescription().cStr() << std::endl;
      return 2;
    }
    std::cerr << "the Server exited before it reported the end of its drain: " << error.getDescription().cStr() << std::endl;
    return 1;
  }
  // The Server exits right after that answer: wait until its address no longer answers.
  const auto until = monotonicTimeMs() + 10000;
  for (;;) {
    try {
      send(kj::HttpMethod::GET, "/__coworkerd/ready", 1000);
    } catch (const kj::Exception&) {
      break;
    }
    if (monotonicTimeMs() > until) { std::cerr << "the Server still answers after its drain ended" << std::endl; return 2; }
    timer.afterDelay(20 * kj::MILLISECONDS).wait(io.waitScope);
  }
  capnp::MallocMessageBuilder message;
  auto json = message.initRoot<capnp::JsonValue>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(record.data(), record.size()), json);
  auto revoke = coworkerd::json::field(json, "revokeStatus");
  auto outcome = coworkerd::json::field(json, "outcome");
  std::cout << "{\"server\":\"" << jsonEscape(server) << "\",\"exited\":true,\"elapsedMs\":" << std::setprecision(17)
            << monotonicTimeMs() - started << ",\"outcome\":\"" << (outcome.isString() ? outcome.getString().cStr() : "")
            << "\",\"revokeStatus\":\"" << jsonEscape(revoke.isString() ? revoke.getString().cStr() : "") << "\"}" << std::endl;
  return 0;
}

// ---- The control plane (/v1 by tenant): `coworkerd-apiserver --config FILE` ----
// The API of docs/control-plane-api.md section 2, in front of the deployment tool's own operations
// (deployment-tool.c++) and the tenants, credentials and usage (tenants.c++). The configuration file names the object
// store with the deploy identity (storage), the administrator's credential
// (control.adminCredential or COWORKERD_ADMIN_CREDENTIAL, required), the nodes' management token (management.token
// or COWORKERD_MANAGEMENT_TOKEN: what this sends to the nodes' trial, state and log endpoints) and
// control.listenAddress (default 0.0.0.0:8940). Every /v1/ request takes `Authorization: Bearer <credential>`: the
// administrator's, or a tenant's (scope tenant:<t>, created through the API). It keeps no state of its own: the
// desired state is the object store's (a mirror in memory), the members and the usage are read from the store and the
// nodes, at the addresses their heartbeats advertise, when it is asked.
//
//   GET    /v1/status                                        any: the cluster's nodes, versions, this instance
//   GET    /v1/nodes, /v1/nodes/{node}                       admin: each node's state endpoint, summarized
//   GET    /v1/usage?from&to&by=hour                         admin: every tenant's hourly usage
//   POST   /v1/tenants, GET /v1/tenants                      admin
//   GET    /v1/tenants/{t}                                   admin, t: quotas, hostnames, counts, a day's usage
//   PATCH  /v1/tenants/{t}, DELETE /v1/tenants/{t}           admin (the deletion cascades: section 5)
//   POST|GET /v1/tenants/{t}/credentials                     admin; DELETE .../{keyId}: admin, t (revoke)
//   GET    /v1/tenants/{t}/usage?from&to&by=hour             admin, t
//   GET    /v1/tenants/{t}/workers                           t: Workers, versions, active versions, routes, limits
//   GET|PATCH|DELETE /v1/tenants/{t}/workers/{w}             t: look, set limits {"limits":{...}}, remove
//   PUT    .../workers/{w}/versions/{v}[?activate=true]      t: a package (tar: manifest.json, {w}/{v}/...), trial-loaded
//                                                            and published only if it loads; activate also flips it
//   POST   .../workers/{w}/versions/{v}:activate             t: activate (roll back)
//   DELETE .../workers/{w}/versions/{v}                      t: an inactive version and its unshared objects
//   PUT    .../workers/{w}/routes                            t: {"routes":[{"host","pathPrefix"}]}, hostnames checked
//   GET    .../workers/{w}/secrets, PUT|GET|DELETE .../secrets/{name}   t: names and lengths, never a value
//   GET    .../workers/{w}/logs[?since&limit&follow]         t: the Worker's lines from every node, merged by time
//
// Errors are {"code","reason"} with the reason cleaned: the codes of docs/guide/errors.md and
// TENANT_NOT_FOUND (404), FORBIDDEN (403: a tenant credential acting on another tenant, or an administrator's
// endpoint), QUOTA_EXCEEDED (422, the reason names the quota), HOSTNAME_NOT_OWNED (403).

struct ControlSettings {
  std::string listenAddress, token, tokenSource, configFile;
  // The desired state's mirror: its hints read again every pollIntervalMs; the secrets key that seals secrets' values.
  uint32_t pollIntervalMs = 5000, startupTimeoutMs = 15000;
  std::string secretsKeyFile, secretsKeySource;
  std::string adminCredential, adminSource;
  bool pathRouting = true;
  fs::path temporaryDirectory;
  coworkerd::ObjectStoreConfig storage;
  std::string storageCredentials;
  uint64_t maxUploadBytes = 0;
  uint32_t serverTimeoutMs = 2000;
  uint64_t startedAtUnixMs = 0;
  std::string instance;
  // The members: the heartbeats under members/ in the bucket.
  uint32_t memberTtlSeconds = 15, memberGraceMs = 10000;
  // The usage objects' retention (usage.retentionHours): the daily cleanup removes what is older, for the nodes that left.
  uint32_t usageRetentionHours = 744;
};

// A path segment of the API: a deployment name (letters, digits, '-', '_' and '.').
bool controlName(const std::string& value) { return safeName(value) && value.size() <= 255; }

std::map<std::string, std::string> controlQuery(const std::string& url) {
  std::map<std::string, std::string> result;
  const auto mark = url.find('?');
  if (mark == std::string::npos) return result;
  std::istringstream fields(url.substr(mark + 1));
  for (std::string field; std::getline(fields, field, '&');) {
    if (field.empty()) continue;
    const auto equals = field.find('=');
    result[field.substr(0, equals)] = equals == std::string::npos ? std::string() : field.substr(equals + 1);
  }
  return result;
}

std::vector<std::string> controlSegments(std::string path) {
  if (auto mark = path.find('?'); mark != std::string::npos) path.resize(mark);
  std::vector<std::string> segments;
  std::istringstream parts(path);
  for (std::string part; std::getline(parts, part, '/');) if (!part.empty()) segments.push_back(part);
  return segments;
}

// The package of a version: a tar archive (ustar; pax "path" records for long names) of
// manifest.json and the files below <workerId>/<versionId>/, unpacked into `directory` (new and empty):
// regular files only, every path relative and normalized, no file written twice, none outside.
size_t unpackPackage(kj::ArrayPtr<const kj::byte> archive, const fs::path& directory, const std::string& workerId,
    const std::string& versionId) {
  auto octal = [](const kj::byte* field, size_t size) {
    uint64_t value = 0;
    size_t i = 0;
    while (i < size && field[i] == ' ') ++i;
    bool digits = false;
    for (; i < size && field[i] >= '0' && field[i] <= '7'; ++i) {
      KJ_REQUIRE(value < (uint64_t(1) << 60), "invalid package: a tar number is too large");
      value = value * 8 + (field[i] - '0');
      digits = true;
    }
    for (; i < size; ++i) KJ_REQUIRE(field[i] == ' ' || field[i] == 0, "invalid package: a tar number is not octal");
    KJ_REQUIRE(digits, "invalid package: a tar number is empty");
    return value;
  };
  auto text = [](const kj::byte* field, size_t size) {
    size_t length = 0;
    while (length < size && field[length]) ++length;
    return std::string(reinterpret_cast<const char*>(field), length);
  };
  const std::string prefix = workerId + "/" + versionId + "/";
  std::set<std::string> written;
  std::optional<std::string> paxPath;
  size_t at = 0;
  bool ended = false;
  while (at + 512 <= archive.size()) {
    const kj::byte* header = archive.begin() + at;
    if (std::all_of(header, header + 512, [](kj::byte b) { return b == 0; })) { ended = true; break; }
    uint64_t sum = 0;
    for (size_t i = 0; i < 512; ++i) sum += (i >= 148 && i < 156) ? ' ' : header[i];
    KJ_REQUIRE(octal(header + 148, 8) == sum, "invalid package: a tar header's checksum is wrong");
    const char type = static_cast<char>(header[156]);
    const uint64_t size = octal(header + 124, 12);
    at += 512;
    const uint64_t padded = (size + 511) / 512 * 512;
    KJ_REQUIRE(padded <= archive.size() - at, "invalid package: a tar entry is truncated");
    const auto content = archive.slice(at, at + size);
    at += padded;
    if (type == 'x') {
      // pax records "<length> <key>=<value>\n"; only the path is used.
      std::string records(reinterpret_cast<const char*>(content.begin()), content.size());
      for (size_t start = 0; start < records.size();) {
        const auto space = records.find(' ', start);
        KJ_REQUIRE(space != std::string::npos && space > start, "invalid package: a pax record");
        const auto length = std::stoull(records.substr(start, space - start));
        KJ_REQUIRE(length > space - start && start + length <= records.size() && records[start + length - 1] == '\n',
            "invalid package: a pax record");
        const auto record = records.substr(space + 1, start + length - space - 2);
        if (record.starts_with("path=")) paxPath = record.substr(5);
        start += length;
      }
      continue;
    }
    std::string name;
    if (paxPath) {
      name = *paxPath;
      paxPath.reset();
    } else {
      name = text(header, 100);
      if (std::string_view(reinterpret_cast<const char*>(header + 257), 5) == "ustar") {
        if (auto before = text(header + 345, 155); !before.empty()) name = before + "/" + name;
      }
    }
    if (type == '5') continue;  // directories are made as their files need them
    KJ_REQUIRE(type == '0' || type == 0, "invalid package: only regular files and directories", int(type));
    KJ_REQUIRE(!name.empty() && name.size() <= 4096 && !name.starts_with("/") &&
        std::none_of(name.begin(), name.end(), [](unsigned char c) { return c < 0x20 || c == 0x7f || c == '\\'; }),
        "invalid package: an unsafe file name");
    for (const auto& segment : controlSegments("/" + name))
      KJ_REQUIRE(segment != "." && segment != "..", "invalid package: an unsafe file name");
    KJ_REQUIRE(name.find("//") == std::string::npos && !name.ends_with("/"), "invalid package: an unsafe file name");
    KJ_REQUIRE(name == "manifest.json" || name.starts_with(prefix),
        "invalid package: its files are manifest.json and those below <worker>/<version>/", prefix.c_str());
    KJ_REQUIRE(written.insert(name).second, "invalid package: a file appears twice");
    KJ_REQUIRE(written.size() <= 30000, "invalid package: more than 30,000 files");
    const auto path = directory / name;
    fs::create_directories(path.parent_path());
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    KJ_REQUIRE(fd >= 0, "cannot unpack the package", strerror(errno));
    auto closing = kj::defer([fd] { ::close(fd); });
    for (size_t done = 0; done < content.size();) {
      const auto n = ::write(fd, content.begin() + done, content.size() - done);
      if (n < 0 && errno == EINTR) continue;
      KJ_REQUIRE(n > 0, "cannot unpack the package", strerror(errno));
      done += n;
    }
  }
  KJ_REQUIRE(ended || at == archive.size(), "invalid package: data after the tar entries");
  KJ_REQUIRE(written.contains("manifest.json"), "invalid package: no manifest.json");
  return written.size();
}

// A response that remembers its status, for the request's log line.
class ControlResponse final: public kj::HttpService::Response {
public:
  explicit ControlResponse(kj::HttpService::Response& inner): inner(inner) {}
  kj::Own<kj::AsyncOutputStream> send(uint statusCode, kj::StringPtr text, const kj::HttpHeaders& headers,
      kj::Maybe<uint64_t> expectedBodySize = kj::none) override {
    status = statusCode;
    return inner.send(statusCode, text, headers, expectedBodySize);
  }
  kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders& headers) override { return inner.acceptWebSocket(headers); }
  uint status = 0;
  std::string code, caller;

private:
  kj::HttpService::Response& inner;
};

class ControlService final: public kj::HttpService {
public:
  ControlService(const ControlSettings& settings, coworkerd::ObjectStore& storage,
      coworkerd::ManifestStore& state, kj::Timer& timer, kj::Network& network, const kj::HttpHeaderTable& headerTable)
      : settings(settings), storage(storage), state(state), timer(timer), network(network), headerTable(headerTable),
        context{storage, state, timer, network, headerTable, settings.token, settings.memberTtlSeconds, settings.memberGraceMs} {}

  kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& headers,
      kj::AsyncInputStream& body, Response& response) override {
    auto recorded = kj::heap<ControlResponse>(response);
    auto& tracked = *recorded;
    const auto started = monotonicTimeMs();
    std::string path = url.cStr();
    if (auto mark = path.find('?'); mark != std::string::npos) path.resize(mark);
    if (path.size() > 200) path.resize(200);
    const auto methodName = std::string(kj::str(method).cStr());
    return kj::evalNow([&]() { return route(method, url, headers, body, tracked); })
        .catch_([this, &tracked](kj::Exception&& error) -> kj::Promise<void> {
          // Anything unexpected: one cleaned sentence.
          std::cout << "[control] {\"event\":\"failure\",\"reason\":\"" << jsonEscape(cleanReason(error.getDescription().cStr()))
                    << "\"}" << std::endl;
          if (tracked.status) return kj::mv(error);
          // The object store out of reach where an answer needed it (and no mirror could stand in).
          if (std::string(error.getDescription().cStr()).find("object store") != std::string::npos)
            return fail(tracked, 503, "STORAGE_UNAVAILABLE", error.getDescription().cStr());
          return fail(tracked, 500, "INTERNAL", error.getDescription().cStr());
        })
        .then([&tracked, started, path = std::move(path), methodName = std::move(methodName)]() {
          // Never the body, the query or a credential: method, path, status, code, the caller's scope, time.
          std::cout << "[control] {\"event\":\"request\",\"method\":\"" << methodName << "\",\"path\":\"" << jsonEscape(path)
                    << "\",\"status\":" << tracked.status << ",\"code\":" << (tracked.code.empty() ? "null" : "\"" + tracked.code + "\"")
                    << ",\"caller\":" << (tracked.caller.empty() ? "null" : "\"" + jsonEscape(tracked.caller) + "\"")
                    << ",\"elapsedMs\":" << std::setprecision(6) << monotonicTimeMs() - started
                    << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
        })
        .attach(kj::mv(recorded));
  }

private:
  using Caller = coworkerd::Caller;
  using ApiAnswer = coworkerd::ApiAnswer;

  kj::Promise<void> route(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& headers,
      kj::AsyncInputStream& body, ControlResponse& response) {
    const auto segments = controlSegments(url.cStr());
    const auto query = controlQuery(url.cStr());
    if (segments.empty() || segments[0] != "v1") co_return co_await fail(response, 404, "NOT_FOUND", "no such endpoint");
    // No credential at all: refused before anything else (the health check's probe).
    bool presented = false;
    headers.forEach([&](kj::StringPtr name, kj::StringPtr) { if (name.size() == 13 && strncasecmp(name.cStr(), "authorization", 13) == 0) presented = true; });
    if (!presented) {
      std::cout << "[control] {\"event\":\"unauthorized\",\"method\":\"" << kj::str(method).cStr() << "\",\"authorization\":\"missing\","
                   "\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
      response.code = "UNAUTHORIZED";
      co_return co_await sendJson(response, 401, "{\"code\":\"UNAUTHORIZED\",\"reason\":\"this endpoint requires Authorization: Bearer <credential> "
          "(the administrator's or a tenant's)\"}", "Bearer realm=\"coworkerd\"");
    }
    // The credential from the mirror of the credentials index: every read of /v1 is the mirror's, so reads go on while
    // the object store cannot be reached; writes then answer STORAGE_UNAVAILABLE.
    auto authenticated = coworkerd::authenticate(state, settings.adminCredential, headers);
    if (!authenticated.caller) {
      std::string path = url.cStr();
      if (auto mark = path.find('?'); mark != std::string::npos) path.resize(mark);
      if (path.size() > 200) path.resize(200);
      std::cout << "[control] {\"event\":\"unauthorized\",\"method\":\"" << kj::str(method).cStr() << "\",\"path\":\""
                << jsonEscape(path) << "\",\"authorization\":\"" << authenticated.kind << "\",\"observedAtUnixMs\":" << wallTimeMsForTest()
                << "}" << std::endl;
      response.code = "UNAUTHORIZED";
      kj::HttpHeaders extra(headerTable);
      co_return co_await sendJson(response, 401, "{\"code\":\"UNAUTHORIZED\",\"reason\":\"" + jsonEscape(authenticated.refusal.reason) + "\"}",
          "Bearer realm=\"coworkerd\"");
    }
    const Caller caller = *authenticated.caller;
    response.caller = caller.admin ? "admin" : "tenant:" + caller.tenant;
    const auto n = segments.size();
    auto is = [&](kj::HttpMethod expected) { return method == expected; };
    auto adminOnly = [&]() -> kj::Promise<void> {
      return fail(response, 403, "FORBIDDEN", "this endpoint is the administrator's; a tenant credential (" + caller.tenant + ") cannot use it");
    };
    if (n == 2 && segments[1] == "status") {
      if (!is(kj::HttpMethod::GET)) co_return co_await methodNotAllowed(response, "GET");
      co_return co_await sendJson(response, 200, co_await status(caller));
    }
    if (segments[1] == "nodes" && n <= 3) {
      if (!caller.admin) co_return co_await adminOnly();
      if (!is(kj::HttpMethod::GET)) co_return co_await methodNotAllowed(response, "GET");
      if (n == 3 && !controlName(segments[2])) co_return co_await fail(response, 400, "INVALID_REQUEST", "invalid node id");
      co_return co_await nodes(n == 3 ? std::optional<std::string>(segments[2]) : std::nullopt, response);
    }
    if (n == 2 && segments[1] == "usage") {
      if (!caller.admin) co_return co_await adminOnly();
      if (!is(kj::HttpMethod::GET)) co_return co_await methodNotAllowed(response, "GET");
      co_return co_await usage(std::nullopt, query, response);
    }
    if (segments[1] != "tenants") co_return co_await fail(response, 404, "NOT_FOUND", "no such endpoint");
    if (n == 2) {
      if (!caller.admin) co_return co_await adminOnly();
      if (is(kj::HttpMethod::POST)) co_return co_await send(response, co_await coworkerd::createTenant(state, co_await readBody(body)));
      if (is(kj::HttpMethod::GET)) co_return co_await send(response, coworkerd::listTenants(state));
      co_return co_await methodNotAllowed(response, "GET, POST");
    }
    const auto tenant = segments[2];
    if (!coworkerd::tenantName(tenant))
      co_return co_await fail(response, 400, "INVALID_REQUEST", "invalid tenant name ([a-z0-9][a-z0-9-]{0,62})");
    if (!caller.mayAct(tenant))
      co_return co_await fail(response, 403, "FORBIDDEN", "a credential of tenant " + caller.tenant + " cannot act on tenant " + tenant);
    if (n == 3) {
      if (is(kj::HttpMethod::GET)) co_return co_await tenantSummary(tenant, response);
      if (!caller.admin) co_return co_await adminOnly();
      if (is(kj::HttpMethod::PATCH)) co_return co_await send(response, co_await coworkerd::updateTenant(state, tenant, co_await readBody(body)));
      if (is(kj::HttpMethod::DELETE)) co_return co_await send(response, co_await coworkerd::deleteTenant(context, tenant));
      co_return co_await methodNotAllowed(response, "GET, PATCH, DELETE");
    }
    // Below a tenant: it exists, and only an active one is written to.
    auto read = coworkerd::readTenant(state, tenant);
    if (!read.tenant) co_return co_await fail(response, 404, "TENANT_NOT_FOUND", "there is no tenant " + tenant);
    const auto record = *read.tenant;
    if (!is(kj::HttpMethod::GET) && record.status != "active")
      co_return co_await fail(response, 409, "CONFLICT", "tenant " + tenant + " is being deleted");
    const auto& area = segments[3];
    if (area == "credentials") {
      if (n == 4) {
        if (!caller.admin) co_return co_await adminOnly();
        if (is(kj::HttpMethod::POST))
          co_return co_await send(response, co_await coworkerd::createCredential(state, tenant, co_await readBody(body)));
        if (is(kj::HttpMethod::GET)) co_return co_await send(response, coworkerd::listCredentials(state, tenant));
        co_return co_await methodNotAllowed(response, "GET, POST");
      }
      if (n == 5) {
        // A tenant revokes its own credentials; the administrator any tenant's.
        if (!is(kj::HttpMethod::DELETE)) co_return co_await methodNotAllowed(response, "DELETE");
        co_return co_await send(response, co_await coworkerd::revokeCredential(state, tenant, segments[4]));
      }
    }
    if (area == "usage" && n == 4) {
      if (!is(kj::HttpMethod::GET)) co_return co_await methodNotAllowed(response, "GET");
      co_return co_await usage(tenant, query, response);
    }
    if (area != "workers") co_return co_await fail(response, 404, "NOT_FOUND", "no such endpoint");
    if (n == 4) {
      if (!is(kj::HttpMethod::GET)) co_return co_await methodNotAllowed(response, "GET");
      co_return co_await answer(response, co_await coworkerd::controlWorkers(state, tenant), "deploy");
    }
    const auto& worker = segments[4];
    if (!coworkerd::apiName(worker)) co_return co_await fail(response, 400, "INVALID_REQUEST", "invalid Worker name ([a-z0-9][a-z0-9-]{0,62})");
    const auto id = coworkerd::workerFullId(tenant, worker);
    if (n == 5) {
      if (is(kj::HttpMethod::GET)) co_return co_await answer(response, co_await coworkerd::controlWorkers(state, tenant, id), "deploy");
      if (is(kj::HttpMethod::PATCH)) co_return co_await limits(record, id, body, response);
      if (is(kj::HttpMethod::DELETE)) co_return co_await answer(response, co_await coworkerd::controlRemove(context, id), "deploy");
      co_return co_await methodNotAllowed(response, "GET, PATCH, DELETE");
    }
    const auto& part = segments[5];
    if (part == "versions" && n == 7) {
      std::string version = segments[6];
      const bool activation = version.ends_with(":activate");
      if (activation) version.resize(version.size() - 9);
      if (!controlName(version)) co_return co_await fail(response, 400, "INVALID_REQUEST", "invalid version name");
      if (activation) {
        if (!is(kj::HttpMethod::POST)) co_return co_await methodNotAllowed(response, "POST");
        auto text = co_await readBody(body);
        if (text.find_first_not_of(" \t\r\n{}") != std::string::npos)
          co_return co_await fail(response, 400, "INVALID_REQUEST", "the activation takes no body (routes: PUT .../routes)");
        co_return co_await answer(response, co_await coworkerd::controlActivate(context, id, version, std::nullopt), "deploy");
      }
      if (is(kj::HttpMethod::PUT)) {
        auto activate = query.find("activate");
        if (activate != query.end() && activate->second != "true" && activate->second != "false")
          co_return co_await fail(response, 400, "INVALID_REQUEST", "activate=true or activate=false");
        co_return co_await upload(record, worker, version, activate != query.end() && activate->second == "true", body, response);
      }
      if (is(kj::HttpMethod::DELETE))
        co_return co_await answer(response, co_await coworkerd::controlDeleteVersion(context, id, version), "deploy");
      co_return co_await methodNotAllowed(response, "PUT, DELETE (and POST .../{version}:activate)");
    }
    if (part == "routes" && n == 6) {
      if (!is(kj::HttpMethod::PUT)) co_return co_await methodNotAllowed(response, "PUT");
      co_return co_await routes(record, id, body, response);
    }
    if (part == "secrets" && (n == 6 || n == 7)) co_return co_await secrets(record, id, n == 7 ? segments[6] : std::string(), method, body, response);
    if (part == "logs" && n == 6) {
      if (!is(kj::HttpMethod::GET)) co_return co_await methodNotAllowed(response, "GET");
      co_return co_await logs(id, query, response);
    }
    co_return co_await fail(response, 404, "NOT_FOUND", "no such endpoint");
  }

  kj::Promise<std::string> readBody(kj::AsyncInputStream& body, uint64_t limit = 64 * 1024) {
    auto text = co_await body.readAllText(limit);
    co_return std::string(text.cStr(), text.size());
  }

  // Where a tenant may route: a hostname it owns, or the default host * under /<tenant>/ (unless path routing
  // is off; /<tenant> itself is written as /<tenant>/, defaultHostPrefix). Empty when it may.
  std::string routeRefusal(const coworkerd::TenantRecord& tenant, const coworkerd::RouteDefinition& route) const {
    if (route.host == "*") {
      if (!settings.pathRouting)
        return "path routes under the default host are off on this cluster (control.pathRouting); route a hostname of tenant " + tenant.name;
      const auto prefix = "/" + tenant.name + "/";
      if (coworkerd::defaultHostPrefix(tenant.name, route.pathPrefix).starts_with(prefix)) return {};
      return "a route of tenant " + tenant.name + " under the default host * starts with " + prefix + " (not " + route.pathPrefix + ")";
    }
    if (std::find(tenant.hostnames.begin(), tenant.hostnames.end(), route.host) != tenant.hostnames.end()) return {};
    return "hostname " + route.host + " is not one of tenant " + tenant.name + "'s hostnames";
  }

  // PUT .../workers/{w}/versions/{v}: the quotas, the package, the routes' hostnames, then the tool's upload or upsert.
  kj::Promise<void> upload(coworkerd::TenantRecord tenant, std::string worker, std::string version, bool activate,
      kj::AsyncInputStream& body, ControlResponse& response) {
    const auto id = coworkerd::workerFullId(tenant.name, worker);
    const uint64_t limit = std::min<uint64_t>(settings.maxUploadBytes, tenant.quotas.packageBytes);
    auto tooLarge = [&]() {
      if (tenant.quotas.packageBytes < settings.maxUploadBytes)
        return fail(response, 422, "QUOTA_EXCEEDED", "packageBytes: the package is larger than tenant " + tenant.name + "'s quota of " +
            std::to_string(tenant.quotas.packageBytes) + " bytes");
      return fail(response, 413, "PACKAGE_TOO_LARGE", "the package is larger than " + std::to_string(settings.maxUploadBytes) +
          " bytes (control.maxUploadBytes)");
    };
    KJ_IF_SOME(length, body.tryGetLength()) {
      if (length > limit) co_return co_await tooLarge();
    }
    kj::Array<kj::byte> archive;
    bool large = false;
    try {
      archive = co_await body.readAllBytes(limit);
    } catch (const kj::Exception&) {
      large = true;
    }
    if (large) co_return co_await tooLarge();
    auto pattern = (settings.temporaryDirectory / "coworkerd-control-XXXXXX").string();
    std::vector<char> name(pattern.begin(), pattern.end());
    name.push_back('\0');
    if (!mkdtemp(name.data())) co_return co_await fail(response, 500, "INTERNAL", std::string("cannot make a temporary directory: ") + strerror(errno));
    const fs::path directory(name.data());
    auto removing = kj::defer([directory] { std::error_code ignored; fs::remove_all(directory, ignored); });
    size_t files = 0;
    std::optional<std::string> invalid;
    coworkerd::DeploymentInput input;
    try {
      files = unpackPackage(archive.asPtr(), directory, worker, version);
      input = coworkerd::readDeploymentManifest(directory / "manifest.json", tenant.name);
    } catch (const kj::Exception& error) {
      invalid = error.getDescription().cStr();
    }
    if (invalid) co_return co_await fail(response, 400, "INVALID_REQUEST", *invalid);
    archive = nullptr;
    // The tenant's quotas (control-plane-api.md section 3), against one read of its state.
    auto counts = coworkerd::tenantCounts(state, tenant.name);
    if (!counts.hasWorker(id) && counts.versions.size() >= tenant.quotas.workers)
      co_return co_await fail(response, 422, "QUOTA_EXCEEDED", "workers: tenant " + tenant.name + " has " +
          std::to_string(counts.versions.size()) + " Workers, its quota is " + std::to_string(tenant.quotas.workers));
    const auto known = counts.versions.find(id);
    const size_t versions = known == counts.versions.end() ? 0 : known->second.size();
    if ((known == counts.versions.end() || !known->second.contains(version)) && versions >= tenant.quotas.versionsPerWorker)
      co_return co_await fail(response, 422, "QUOTA_EXCEEDED", "versionsPerWorker: Worker " + worker + " has " + std::to_string(versions) +
          " versions, the quota is " + std::to_string(tenant.quotas.versionsPerWorker) + "; delete an old version first");
    // The asset quotas of the package the deployment reads: the version's packagePath (inside the package,
    // readDeploymentManifest), else its default place.
    auto package = directory / worker / version / "package.json";
    if (auto uploaded = input.versions.find(id + "@" + version); uploaded != input.versions.end() && !uploaded->second->packagePath.empty())
      package = uploaded->second->packagePath;
    if (fs::exists(package)) {
      std::optional<std::string> problem;
      try {
        auto contents = coworkerd::readPackage(readFile(package));
        if (contents.assets.size() > tenant.quotas.assetFiles)
          problem = "assetFiles: the version has " + std::to_string(contents.assets.size()) + " static assets, the quota is " +
              std::to_string(tenant.quotas.assetFiles);
        else if (contents.assetBytes > tenant.quotas.assetBytes)
          problem = "assetBytes: the version's static assets have " + std::to_string(contents.assetBytes) + " bytes, the quota is " +
              std::to_string(tenant.quotas.assetBytes);
      } catch (const kj::Exception&) {}  // the deployment's own checks say what is wrong with it
      if (problem) co_return co_await fail(response, 422, "QUOTA_EXCEEDED", *problem);
    }
    if (activate) for (const auto& route : input.routes) {
      if (auto refused = routeRefusal(tenant, route); !refused.empty())
        co_return co_await fail(response, 403, "HOSTNAME_NOT_OWNED", refused);
    }
    auto result = co_await coworkerd::controlDeploy(context, directory / "manifest.json", id, version, activate);
    co_return co_await answer(response, result, "deploy", [&](std::string record) {
      return "{\"tenant\":\"" + tenant.name + "\",\"worker\":\"" + worker + "\",\"workerId\":\"" + id + "\",\"versionId\":\"" + version +
          "\",\"activated\":" + (activate ? "true" : "false") + ",\"files\":" + std::to_string(files) + ",\"record\":" + record + "}";
    });
  }

  // PUT .../workers/{w}/routes {"routes":[{"host":"api.acme.example","pathPrefix":"/"}]}: the Worker's whole set.
  kj::Promise<void> routes(coworkerd::TenantRecord tenant, std::string id, kj::AsyncInputStream& body, ControlResponse& response) {
    std::vector<coworkerd::RouteDefinition> list;
    std::optional<std::string> invalid;
    try {
      auto text = co_await body.readAllText(64 * 1024);
      capnp::MallocMessageBuilder message;
      auto json = message.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(text.cStr(), text.size()), json);
      KJ_REQUIRE(json.isObject(), "the body is {\"routes\":[...]}");
      for (auto field : json.asReader().getObject()) {
        KJ_REQUIRE(field.getName() == "routes" && field.getValue().isArray(), "the body is {\"routes\":[{\"host\",\"pathPrefix\"}]}");
        for (auto entry : field.getValue().getArray()) {
          KJ_REQUIRE(entry.isObject(), "a route is {\"host\",\"pathPrefix\"}");
          list.push_back({coworkerd::json::text(entry, "host"), coworkerd::json::text(entry, "pathPrefix"), id});
        }
      }
      KJ_REQUIRE(list.size() <= 64, "at most 64 routes per Worker");
    } catch (const kj::Exception& error) {
      invalid = error.getDescription().cStr();
    }
    if (invalid) co_return co_await fail(response, 400, "INVALID_REQUEST", "PUT .../routes takes {\"routes\":[{\"host\",\"pathPrefix\"}]}: " + *invalid);
    for (const auto& route : list) {
      if (auto refused = routeRefusal(tenant, route); !refused.empty())
        co_return co_await fail(response, 403, "HOSTNAME_NOT_OWNED", refused);
    }
    co_return co_await answer(response, co_await coworkerd::controlActivate(context, id, {}, list), "deploy");
  }

  // PATCH .../workers/{w} {"limits":{"cpuMs":50,"memoryMiB":64}}: within the tenant's ceilings.
  kj::Promise<void> limits(coworkerd::TenantRecord tenant, std::string id, kj::AsyncInputStream& body, ControlResponse& response) {
    coworkerd::WorkerLimits value;
    std::optional<std::string> invalid;
    try {
      auto text = co_await body.readAllText(8192);
      capnp::MallocMessageBuilder message;
      auto json = message.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(text.cStr(), text.size()), json);
      KJ_REQUIRE(json.isObject(), "the body is {\"limits\":{...}}");
      bool seen = false;
      for (auto field : json.asReader().getObject()) {
        KJ_REQUIRE(field.getName() == "limits", "the body is {\"limits\":{\"cpuMs\",\"memoryMiB\"}}");
        value = coworkerd::readWorkerLimits(field.getValue());
        seen = true;
      }
      KJ_REQUIRE(seen, "the body is {\"limits\":{\"cpuMs\",\"memoryMiB\"}}");
    } catch (const kj::Exception& error) {
      invalid = error.getDescription().cStr();
    }
    if (invalid) co_return co_await fail(response, 400, "INVALID_REQUEST", *invalid);
    if (value.cpuMs > tenant.quotas.maxWorkerCpuMs)
      co_return co_await fail(response, 422, "QUOTA_EXCEEDED", "maxWorkerCpuMs: " + std::to_string(value.cpuMs) + " ms is above tenant " +
          tenant.name + "'s ceiling of " + std::to_string(tenant.quotas.maxWorkerCpuMs) + " ms");
    if (value.memoryMiB > tenant.quotas.maxWorkerMemoryMiB)
      co_return co_await fail(response, 422, "QUOTA_EXCEEDED", "maxWorkerMemoryMiB: " + std::to_string(value.memoryMiB) + " MiB is above tenant " +
          tenant.name + "'s ceiling of " + std::to_string(tenant.quotas.maxWorkerMemoryMiB) + " MiB");
    co_return co_await answer(response, co_await coworkerd::controlLimits(context, id, value), "deploy");
  }

  // .../secrets (GET: names and lengths) and .../secrets/{name} (PUT: the value is the body, never logged; GET: its
  // name and length; DELETE).
  kj::Promise<void> secrets(coworkerd::TenantRecord tenant, std::string id, std::string name, kj::HttpMethod method,
      kj::AsyncInputStream& body, ControlResponse& response) {
    if (name.empty()) {
      if (method != kj::HttpMethod::GET) co_return co_await methodNotAllowed(response, "GET");
      co_return co_await answer(response, co_await coworkerd::controlSecretList(context, id), "secret");
    }
    if (!coworkerd::environmentName(name))
      co_return co_await fail(response, 400, "INVALID_REQUEST", "a secret name is a JavaScript identifier of ASCII letters, digits, '_' and '$'");
    if (method == kj::HttpMethod::PUT) {
      std::string value;
      bool tooLarge = false;
      try {
        auto bytes = co_await body.readAllBytes(64 * 1024);
        value.assign(reinterpret_cast<const char*>(bytes.begin()), bytes.size());
      } catch (const kj::Exception&) {
        tooLarge = true;
      }
      if (tooLarge) co_return co_await fail(response, 413, "SECRET_TOO_LARGE", "a secret value is at most 5 KiB");
      auto counts = coworkerd::tenantCounts(state, tenant.name);
      const auto known = counts.secrets.find(id);
      const bool exists = known != counts.secrets.end() && known->second.contains(name);
      if (!exists && counts.secretTotal >= tenant.quotas.secrets)
        co_return co_await fail(response, 422, "QUOTA_EXCEEDED", "secrets: tenant " + tenant.name + " has " +
            std::to_string(counts.secretTotal) + " secrets, its quota is " + std::to_string(tenant.quotas.secrets));
      co_return co_await answer(response, co_await coworkerd::controlSecretPut(context, id, name, std::move(value)), "secret");
    }
    if (method == kj::HttpMethod::DELETE)
      co_return co_await answer(response, co_await coworkerd::controlSecretDelete(context, id, name), "secret");
    if (method == kj::HttpMethod::GET) {
      auto listed = co_await coworkerd::controlSecretList(context, id);
      capnp::MallocMessageBuilder message;
      auto json = message.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(listed.data(), listed.size()), json);
      if (coworkerd::json::boolean(json, "ok")) {
        auto record = coworkerd::json::field(json, "record");
        auto list = coworkerd::json::field(record, "secrets");
        if (list.isArray()) for (auto entry : list.getArray()) {
          if (coworkerd::json::text(entry, "name") != name) continue;
          co_return co_await sendJson(response, 200, "{\"workerId\":\"" + jsonEscape(id) + "\"," +
              std::string(capnp::JsonCodec().encodeRaw(entry).cStr()).substr(1));
        }
      }
      co_return co_await fail(response, 404, "SECRET_NOT_FOUND", "secret " + name + " of Worker " + id + " is not set");
    }
    co_return co_await methodNotAllowed(response, "PUT, GET, DELETE");
  }

  // The members (the live heartbeats under members/, each one's address read once per epoch) and what each one's state
  // endpoint says, in parallel.
  struct Member { std::string id, address, state, epoch; uint64_t startedAtUnixMs = 0; int64_t heartbeatAgeMs = -1; };
  coworkerd::StoreMemberCache memberRecords;
  // The usage objects this process read, by ETag (a past hour's object is read once).
  coworkerd::UsageCache usageCache;
  // The members last listed: while the object store cannot be listed they stand in (the nodes' own state endpoints
  // still answer for themselves).
  std::optional<std::vector<Member>> listedMembers;
  kj::Promise<std::vector<Member>> members() {
    std::vector<Member> result;
    std::optional<std::string> failure;
    try {
      for (auto& member : co_await coworkerd::readStoreMembers(storage, settings.memberTtlSeconds, settings.memberGraceMs, &memberRecords)) {
        if (!controlName(member.id)) continue;
        result.push_back({member.id, member.address, member.state, std::to_string(member.epoch), member.startedAtUnixMs,
            member.heartbeatAgeMs});
      }
    } catch (const kj::Exception& error) {
      failure = error.getDescription().cStr();
    }
    if (failure) {
      if (listedMembers) co_return *listedMembers;
      KJ_FAIL_REQUIRE("object store request failed: the members cannot be listed", failure->c_str());
    }
    listedMembers = result;
    co_return result;
  }

  struct Fetched { bool ok = false; uint status = 0; std::string body, error; };
  kj::Promise<Fetched> fetch(std::string url) {
    Fetched result;
    try {
      auto client = kj::newHttpClient(timer, headerTable, network, kj::none);
      kj::HttpHeaders headers(headerTable);
      headers.setPtr(kj::HttpHeaderId::CONNECTION, "close");
      if (!settings.token.empty()) headers.addPtr("Authorization", kj::str("Bearer ", settings.token.c_str()));
      auto call = client->request(kj::HttpMethod::GET, url.c_str(), headers, uint64_t(0));
      call.body = nullptr;
      auto work = [&]() -> kj::Promise<void> {
        auto answer = co_await call.response;
        auto text = co_await answer.body->readAllText(64 << 20);
        result.status = answer.statusCode;
        result.body = text.cStr();
      };
      co_await timer.timeoutAfter(settings.serverTimeoutMs * kj::MILLISECONDS, work());
      result.ok = result.status == 200;
      if (!result.ok) result.error = "HTTP " + std::to_string(result.status);
    } catch (const kj::Exception& error) {
      result.error = cleanReason(error.getDescription().cStr());
    }
    co_return result;
  }

public:
  // This control plane's own addresses for every member's cluster set (POST /__coworkerd/control-plane: node/main.c++,
  // ControlPlaneAddresses), so that tenant code on no node can reach this API whatever its egress rules allow. A
  // member that does not answer gets the next announcement.
  kj::Promise<void> announce(std::string body) {
    std::vector<Member> list;
    try { list = co_await members(); } catch (const kj::Exception&) { co_return; }
    kj::Vector<kj::Promise<void>> pending;
    for (const auto& member : list) pending.add(post("http://" + member.address + "/__coworkerd/control-plane", body));
    co_await kj::joinPromises(pending.releaseAsArray());
  }

private:
  kj::Promise<void> post(std::string url, std::string body) {
    try {
      auto client = kj::newHttpClient(timer, headerTable, network, kj::none);
      kj::HttpHeaders headers(headerTable);
      headers.setPtr(kj::HttpHeaderId::CONNECTION, "close");
      headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
      if (!settings.token.empty()) headers.addPtr("Authorization", kj::str("Bearer ", settings.token.c_str()));
      auto call = client->request(kj::HttpMethod::POST, url.c_str(), headers, body.size());
      auto work = [&]() -> kj::Promise<void> {
        co_await call.body->write(kj::arrayPtr(body.data(), body.size()).asBytes());
        call.body = nullptr;
        auto answer = co_await call.response;
        co_await answer.body->readAllText(64 * 1024);
      };
      co_await timer.timeoutAfter(settings.serverTimeoutMs * kj::MILLISECONDS, work());
    } catch (const kj::Exception&) {}
  }

  static std::string pick(capnp::JsonValue::Reader object, std::initializer_list<const char*> names) {
    std::string picked = "{";
    if (object.isObject()) for (auto name : names) {
      auto value = coworkerd::json::field(object, name);
      if (value.isNull()) continue;
      picked += std::string(picked.size() > 1 ? "," : "") + "\"" + name + "\":" + capnp::JsonCodec().encodeRaw(value).cStr();
    }
    return picked + "}";
  }

  // One node's summary from its state endpoint: what an operator reads first, in the node's own words. `detail`
  // (GET /v1/nodes) adds its sandbox, rotation, quarantine and suspects, usage and tenants.
  std::string nodeSummary(const Member& member, const Fetched& state, bool detail, bool tenantView) {
    std::string out = "{\"nodeId\":\"" + jsonEscape(member.id) + "\",\"state\":\"" + jsonEscape(member.state) + "\",\"reachable\":" +
        (state.ok ? "true" : "false");
    if (!tenantView) out += ",\"address\":\"" + jsonEscape(member.address) + "\",\"epoch\":\"" + jsonEscape(member.epoch) +
        "\",\"startedAtUnixMs\":" + std::to_string(member.startedAtUnixMs) + ",\"backend\":\"objectStore\",\"lastHeartbeatAgeMs\":" +
        (member.heartbeatAgeMs >= 0 ? std::to_string(member.heartbeatAgeMs) : std::string("null"));
    if (!state.ok) return out + (tenantView ? "}" : ",\"problem\":\"" + jsonEscape(state.error) + "\"}");
    try {
      capnp::MallocMessageBuilder message;
      auto json = message.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(state.body.data(), state.body.size()), json);
      auto reader = json.asReader();
      using coworkerd::json::field;
      out += ",\"version\":" + pick(field(reader, "version"), tenantView ? std::initializer_list<const char*>{"server"}
          : std::initializer_list<const char*>{"server", "source", "workerdUpstream", "patchSeriesSha256"});
      if (tenantView) return out + "}";
      out += ",\"server\":" + pick(field(reader, "server"), {"binarySha256", "pid", "startedAtUnixMs"});
      out += ",\"drain\":" + pick(field(reader, "drain"), {"draining", "phase"});
      out += ",\"deployment\":" + pick(field(reader, "deployment"), {"revision", "generation", "workerCount",
          "validWorkerCount", "invalidWorkerCount", "routeCount", "effectiveRouteCount", "watchConnected"});
      out += ",\"processes\":[";
      auto processes = field(reader, "processes");
      auto entries = field(processes, "entries");
      if (entries.isArray()) {
        size_t index = 0;
        for (auto entry : entries.getArray()) {
          out += std::string(index++ ? "," : "") + pick(entry, {"processId", "processClass", "generation", "pid", "state",
              "residentBytes", "inFlight", "restartCount", "rotationCount", "spawnedAtUnixMs", "binarySha256"});
        }
      }
      out += "]";
      if (detail) {
        out += ",\"configuration\":" + pick(field(processes, "configuration"), {"count", "quarantineProcesses", "cpuLimitMs", "memoryLimitMiB"});
        out += ",\"sandbox\":" + pick(field(processes, "sandbox"), {"mode", "active", "reason"});
        out += ",\"rotation\":" + pick(field(processes, "rotation"), {"maxAgeMs", "maxRequests", "residentBytes", "pending", "completed"});
        auto suspects = field(field(processes, "suspects"), "entries");
        out += ",\"suspects\":" + std::string(suspects.isArray() ? capnp::JsonCodec().encodeRaw(suspects).cStr() : "[]");
        auto usage = field(reader, "usage");
        out += ",\"usage\":" + std::string(usage.isObject() ? capnp::JsonCodec().encodeRaw(usage).cStr() : "null");
        auto tenants = field(reader, "tenants");
        out += ",\"tenants\":" + std::string(tenants.isObject() ? capnp::JsonCodec().encodeRaw(tenants).cStr() : "null");
      }
    } catch (const kj::Exception& error) {
      out += ",\"problem\":\"" + jsonEscape(cleanReason(error.getDescription().cStr())) + "\"";
    }
    return out + '}';
  }

  kj::Promise<std::string> status(const Caller& caller) {
    auto list = co_await members();
    kj::Vector<kj::Promise<Fetched>> pending;
    for (const auto& member : list) pending.add(fetch("http://" + member.address + "/__coworkerd/state"));
    auto states = co_await kj::joinPromises(pending.releaseAsArray());
    std::string out = "{\"apiserver\":{\"version\":" + versionJson() + ",\"instance\":\"" + jsonEscape(settings.instance) +
        "\",\"startedAtUnixMs\":" + std::to_string(settings.startedAtUnixMs);
    // Two fields of 0.3 stay, null: /v1 only adds fields (the endpoint and the revision of a store 0.4 does not have).
    if (caller.admin) out += ",\"etcdEndpoint\":null,\"storageEndpoint\":\"" +
        jsonEscape(settings.storage.endpoint) + "\",\"pathRouting\":" + (settings.pathRouting ? "true" : "false");
    // The desired state's revisions (the store's indexes as this control plane's mirror holds them).
    out += "},\"caller\":" + std::string(caller.admin ? "{\"admin\":true}" : "{\"admin\":false,\"tenant\":\"" + caller.tenant + "\"}") +
        ",\"etcdRevision\":null,\"storage\":{\"tenantsRevision\":\"" + std::to_string(state.index().revision) +
        "\",\"credentialsRevision\":\"" + std::to_string(state.credentials().revision) + "\",\"client\":" +
        state.objects().stats().json() + "},\"nodes\":[";
    for (size_t i = 0; i < list.size(); ++i) out += (i ? "," : "") + nodeSummary(list[i], states[i], false, !caller.admin);
    co_return out + "]}";
  }

  kj::Promise<void> nodes(std::optional<std::string> only, ControlResponse& response) {
    auto list = co_await members();
    if (only) {
      std::erase_if(list, [&](const Member& member) { return member.id != *only; });
      if (list.empty()) co_return co_await fail(response, 404, "NODE_NOT_FOUND", "node " + *only + " is not a member (no live member key)");
    }
    kj::Vector<kj::Promise<Fetched>> pending;
    for (const auto& member : list) pending.add(fetch("http://" + member.address + "/__coworkerd/state"));
    auto states = co_await kj::joinPromises(pending.releaseAsArray());
    std::string out = only ? std::string() : "{\"nodes\":[";
    for (size_t i = 0; i < list.size(); ++i) out += (i ? "," : "") + nodeSummary(list[i], states[i], true, false);
    co_return co_await sendJson(response, 200, only ? out : out + "]}");
  }

  // ?from&to (Unix ms; default the last 24 hours) &by=hour (the only grouping).
  kj::Promise<void> usage(std::optional<std::string> tenant, const std::map<std::string, std::string>& query, ControlResponse& response) {
    uint64_t to = wallTimeMsForTest(), from = 0;
    bool valid = true;
    auto number = [&](const char* name, uint64_t& out) {
      auto found = query.find(name);
      if (found == query.end()) return false;
      const auto& text = found->second;
      valid = valid && !text.empty() && text.size() <= 16 && std::all_of(text.begin(), text.end(), [](char c) { return c >= '0' && c <= '9'; });
      if (valid) out = std::stoull(text);
      return true;
    };
    number("to", to);
    if (!number("from", from)) from = to > 86400000 ? to - 86400000 : 0;
    for (const auto& [key, value] : query) valid = valid && (key == "from" || key == "to" || (key == "by" && value == "hour"));
    if (!valid) co_return co_await fail(response, 400, "INVALID_REQUEST", "usage?from=<unix ms>&to=<unix ms>&by=hour");
    // The usage objects every node writes (nodes/<nodeId>/usage/<hour>), summed.
    std::optional<std::string> unreachable;
    coworkerd::ApiAnswer result;
    try {
      result = co_await coworkerd::usage(storage, usageCache, tenant, from, to);
    } catch (const kj::Exception& error) {
      unreachable = error.getDescription().cStr();
    }
    if (unreachable)
      co_return co_await fail(response, 503, "STORAGE_UNAVAILABLE", "the usage objects cannot be read from the object store (" +
          settings.storage.endpoint + "): " + *unreachable);
    co_return co_await send(response, result);
  }

  // GET /v1/tenants/{t}: the record, what it holds, and its usage of the last 24 hours.
  kj::Promise<void> tenantSummary(std::string tenant, ControlResponse& response) {
    auto read = coworkerd::readTenant(state, tenant);
    if (!read.tenant) co_return co_await fail(response, 404, "TENANT_NOT_FOUND", "there is no tenant " + tenant);
    auto counts = coworkerd::tenantCounts(state, tenant);
    uint64_t versions = 0;
    for (const auto& [id, names] : counts.versions) versions += names.size();
    const auto now = wallTimeMsForTest();
    // The last day's usage from the nodes' objects; null while the store cannot be read.
    coworkerd::ApiAnswer day;
    day.status = 503;
    try { day = co_await coworkerd::usage(storage, usageCache, tenant, now > 86400000 ? now - 86400000 : 0, now + 1); }
    catch (const kj::Exception&) {}
    std::string total = "null";
    if (day.status == 200) {
      capnp::MallocMessageBuilder message;
      auto json = message.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(day.body.data(), day.body.size()), json);
      total = capnp::JsonCodec().encodeRaw(coworkerd::json::field(json, "total")).cStr();
    }
    auto record = coworkerd::tenantRecordJson(*read.tenant);
    if (read.tenant->deletion == "null") { record.pop_back(); record += ",\"deletion\":null}"; }
    co_return co_await sendJson(response, 200, "{\"tenant\":" + record + ",\"counts\":{\"workers\":" + std::to_string(counts.versions.size()) +
        ",\"versions\":" + std::to_string(versions) + ",\"secrets\":" + std::to_string(counts.secretTotal) + "},\"usageLast24h\":" + total + "}");
  }

  // One Worker's lines from every member, merged by time (then node, then sequence).
  struct LogLine { uint64_t atUnixMs, seq; std::string nodeId, json; };
  struct LogCursor { std::string address, epoch; uint64_t next = 0; bool started = false; };
  struct LogBatch { std::vector<LogLine> lines; std::string nodes; };
  kj::Promise<LogBatch> collect(const std::string& worker, uint64_t since, uint64_t limit,
      std::map<std::string, LogCursor>& cursors, const std::vector<Member>& list) {
    kj::Vector<kj::Promise<Fetched>> pending;
    for (const auto& member : list) {
      auto& cursor = cursors[member.id];
      cursor.address = member.address;
      std::string url = "http://" + member.address + "/__coworkerd/logs?worker=" + worker + "&limit=" + std::to_string(limit);
      url += cursor.started ? "&after=" + std::to_string(cursor.next) : "&since=" + std::to_string(since);
      pending.add(fetch(url));
    }
    auto answers = co_await kj::joinPromises(pending.releaseAsArray());
    LogBatch batch;
    for (size_t i = 0; i < list.size(); ++i) {
      const auto& member = list[i];
      auto& cursor = cursors[member.id];
      if (i) batch.nodes += ',';
      batch.nodes += "{\"nodeId\":\"" + jsonEscape(member.id) + "\",\"reachable\":" + (answers[i].ok ? "true" : "false");
      if (!answers[i].ok) {
        batch.nodes += ",\"problem\":\"" + jsonEscape(answers[i].error) + "\"}";
        continue;
      }
      try {
        capnp::MallocMessageBuilder message;
        auto json = message.initRoot<capnp::JsonValue>();
        capnp::JsonCodec().decodeRaw(kj::arrayPtr(answers[i].body.data(), answers[i].body.size()), json);
        const auto epoch = coworkerd::json::text(json, "epoch");
        // A node that restarted numbers its lines from 1 again: its new lines from now on.
        const bool restarted = cursor.started && epoch != cursor.epoch;
        cursor.epoch = epoch;
        auto next = coworkerd::json::field(json, "next");
        if (next.isNumber()) cursor.next = static_cast<uint64_t>(next.getNumber());
        cursor.started = true;
        if (!restarted) {
          for (auto entry : coworkerd::json::field(json, "lines").getArray()) {
            auto at = coworkerd::json::field(entry, "atUnixMs"), seq = coworkerd::json::field(entry, "seq");
            std::string text = capnp::JsonCodec().encodeRaw(entry).cStr();
            text.pop_back();
            text += ",\"nodeId\":\"" + jsonEscape(member.id) + "\"}";
            batch.lines.push_back({at.isNumber() ? static_cast<uint64_t>(at.getNumber()) : 0,
                seq.isNumber() ? static_cast<uint64_t>(seq.getNumber()) : 0, member.id, std::move(text)});
          }
        }
        for (const char* name : {"keptLines", "droppedLines", "truncatedLines", "evictedLines", "bufferBytes", "next"}) {
          auto value = coworkerd::json::field(json, name);
          if (!value.isNull()) batch.nodes += std::string(",\"") + name + "\":" + capnp::JsonCodec().encodeRaw(value).cStr();
        }
        batch.nodes += "}";
      } catch (const kj::Exception& error) {
        batch.nodes += ",\"problem\":\"" + jsonEscape(cleanReason(error.getDescription().cStr())) + "\"}";
      }
    }
    std::sort(batch.lines.begin(), batch.lines.end(), [](const LogLine& a, const LogLine& b) {
      return std::tie(a.atUnixMs, a.nodeId, a.seq) < std::tie(b.atUnixMs, b.nodeId, b.seq);
    });
    co_return batch;
  }

  kj::Promise<void> logs(std::string worker, const std::map<std::string, std::string>& query, ControlResponse& response) {
    uint64_t since = 0, limit = 200;
    bool valid = true;
    auto number = [&](const char* name, uint64_t& out) {
      auto found = query.find(name);
      if (found == query.end()) return;
      const auto& text = found->second;
      valid = valid && !text.empty() && text.size() <= 19 && std::all_of(text.begin(), text.end(), [](char c) { return c >= '0' && c <= '9'; });
      if (valid) out = std::stoull(text);
    };
    number("since", since);
    number("limit", limit);
    auto follow = query.find("follow");
    const bool following = follow != query.end() && (follow->second == "1" || follow->second == "true" || follow->second.empty());
    valid = valid && limit > 0 && limit <= 5000 && (follow == query.end() || following || follow->second == "0" || follow->second == "false");
    for (const auto& [key, value] : query) valid = valid && (key == "since" || key == "limit" || key == "follow");
    if (!valid)
      co_return co_await fail(response, 400, "INVALID_REQUEST", "GET .../logs[?since=<unix ms>][&limit=1..5000][&follow]");
    std::map<std::string, LogCursor> cursors;
    auto list = co_await members();
    auto first = co_await collect(worker, since, limit, cursors, list);
    // The newest `limit` lines of all nodes together.
    if (first.lines.size() > limit) first.lines.erase(first.lines.begin(), first.lines.end() - static_cast<long>(limit));
    if (!following) {
      std::string out = "{\"workerId\":\"" + jsonEscape(worker) + "\",\"nodes\":[" + first.nodes + "],\"lines\":[";
      for (size_t i = 0; i < first.lines.size(); ++i) out += (i ? "," : "") + first.lines[i].json;
      co_return co_await sendJson(response, 200, out + "]}");
    }
    // Server-sent events: every line as `data: <JSON>`; a comment every 15 s keeps idle connections open.
    kj::HttpHeaders headers(headerTable);
    headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "text/event-stream");
    headers.addPtrPtr("Cache-Control", "no-cache");
    auto stream = response.send(200, "OK", headers);
    std::string text = "event: nodes\ndata: [" + first.nodes + "]\n\n";
    for (const auto& line : first.lines) text += "data: " + line.json + "\n\n";
    co_await stream->write(kj::arrayPtr(text.data(), text.size()).asBytes());
    auto lastMembers = monotonicTimeMs(), lastWrite = monotonicTimeMs();
    for (;;) {
      co_await timer.afterDelay(500 * kj::MILLISECONDS);
      if (monotonicTimeMs() - lastMembers > 5000) {
        // Members that joined since start with their lines from now on.
        auto now = co_await members();
        for (const auto& member : now) if (!cursors.contains(member.id)) since = wallTimeMsForTest();
        list = std::move(now);
        lastMembers = monotonicTimeMs();
      }
      auto batch = co_await collect(worker, since, 5000, cursors, list);
      text.clear();
      for (const auto& line : batch.lines) text += "data: " + line.json + "\n\n";
      if (text.empty() && monotonicTimeMs() - lastWrite < 15000) continue;
      if (text.empty()) text = ": keep-alive\n\n";
      co_await stream->write(kj::arrayPtr(text.data(), text.size()).asBytes());
      lastWrite = monotonicTimeMs();
    }
  }

  // An operation's answer {"ok",...} from deployment-tool.c++, as the HTTP status and body.
  kj::Promise<void> answer(ControlResponse& response, const std::string& result, const char* area,
      std::function<std::string(std::string)> wrap = {}) {
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(result.data(), result.size()), json);
    auto record = coworkerd::json::field(json, "record");
    std::string recordText = record.isNull() ? "null" : capnp::JsonCodec().encodeRaw(record).cStr();
    if (coworkerd::json::boolean(json, "ok")) {
      co_return co_await sendJson(response, 200, wrap ? wrap(recordText) : recordText);
    }
    const std::string reason = coworkerd::json::text(json, "reason"), kind = coworkerd::json::text(json, "kind");
    auto has = [&](std::string_view part) { return reason.find(part) != std::string::npos; };
    uint status = 422;
    std::string code = std::string(area) == "secret" ? "SECRET_REFUSED" : "DEPLOYMENT_REFUSED", extra;
    // The tenant's record as the write found it in its own transaction's read (deployment-tool.c++, requireWritable).
    if (reason.starts_with("there is no tenant ")) {
      status = 404; code = "TENANT_NOT_FOUND";
    } else if (reason.starts_with("tenant ") && has(" is being deleted; nothing written")) {
      status = 409; code = "CONFLICT";
    } else if (has("trial load of ") && has(" failed on node ")) {
      status = 422;
      code = "LOAD_FAILED";
      // The trial's verdict: its category and cleaned message (the node cleaned them).
      try {
        auto versions = coworkerd::json::field(coworkerd::json::field(record, "trial"), "versions");
        if (versions.isArray() && versions.getArray().size()) {
          auto verdict = coworkerd::json::field(versions.getArray()[versions.getArray().size() - 1], "verdict");
          for (const char* name : {"category", "message", "nodeId"}) {
            auto value = coworkerd::json::field(verdict, name);
            if (value.isString()) extra += std::string(",\"") + name + "\":" + capnp::JsonCodec().encodeRaw(value).cStr();
          }
        }
      } catch (const kj::Exception&) {}
    } else if (has("run the trial load")) {
      status = 503; code = "TRIAL_UNAVAILABLE";
    } else if (has("deployment label conflict")) {
      status = 409; code = "VERSION_EXISTS";
    } else if (has("期间有其他部署") || has("another write changed") || has("kept changing while a definition was published")) {
      status = 409; code = "CONFLICT";
    } else if (has("was never uploaded")) {
      status = 404; code = "VERSION_NOT_FOUND";
    } else if (has(" is not deployed")) {
      status = 404; code = "WORKER_NOT_FOUND";
    } else if (has(" is not set")) {
      status = 404; code = "SECRET_NOT_FOUND";
    } else if (has("object store") || has("object HEAD failed") || has("object PUT failed") || has("object create failed") ||
        has("object listing failed") || kind == "disconnected" || kind == "overloaded") {
      // The desired state and the packages are the object store's: a write that cannot reach it changed nothing it
      // did not report.
      status = 503; code = "STORAGE_UNAVAILABLE";
    }
    response.code = code;
    const std::string body = "{\"code\":\"" + code + "\",\"reason\":\"" + jsonEscape(cleanReason(reason)) + "\"" + extra +
        (code == "LOAD_FAILED" ? ",\"record\":" + recordText : std::string()) + "}";
    co_return co_await sendJson(response, status, body);
  }

  kj::Promise<void> send(ControlResponse& response, const ApiAnswer& result) {
    if (result.status == 200) return sendJson(response, 200, result.body);
    return fail(response, result.status, result.code.c_str(), result.reason);
  }
  kj::Promise<void> fail(ControlResponse& response, uint status, const char* code, const std::string& reason) {
    response.code = code;
    return sendJson(response, status, "{\"code\":\"" + std::string(code) + "\",\"reason\":\"" + jsonEscape(cleanReason(reason)) + "\"}");
  }
  kj::Promise<void> methodNotAllowed(ControlResponse& response, const char* allowed) {
    return fail(response, 405, "METHOD_NOT_ALLOWED", std::string("allowed: ") + allowed);
  }
  kj::Promise<void> sendJson(kj::HttpService::Response& response, uint status, std::string body, const char* authenticate = nullptr) {
    kj::HttpHeaders headers(headerTable);
    headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
    if (authenticate) headers.addPtrPtr("WWW-Authenticate", authenticate);
    const char* text = status == 200 ? "OK" : status == 400 ? "Bad Request" : status == 401 ? "Unauthorized" : status == 403 ? "Forbidden"
        : status == 404 ? "Not Found" : status == 405 ? "Method Not Allowed" : status == 409 ? "Conflict" : status == 413 ? "Payload Too Large"
        : status == 422 ? "Unprocessable Content" : status == 500 ? "Internal Server Error" : "Service Unavailable";
    auto owned = kj::heap<std::string>(std::move(body));
    auto stream = response.send(status, text, headers, owned->size());
    co_await stream->write(kj::arrayPtr(owned->data(), owned->size()).asBytes());
  }

  const ControlSettings& settings;
  coworkerd::ObjectStore& storage;
  coworkerd::ManifestStore& state;
  kj::Timer& timer;
  kj::Network& network;
  const kj::HttpHeaderTable& headerTable;
  coworkerd::ControlContext context;
};

// What this control plane announces as its own addresses (ControlService::announce): its listen address's host, or
// every address of this host when it listens on all of them.
std::vector<std::string> controlAddresses(const std::string& listenAddress) {
  auto host = listenAddress.substr(0, listenAddress.rfind(':'));
  if (host.starts_with('[') && host.ends_with(']')) host = host.substr(1, host.size() - 2);
  if (!host.empty() && host != "0.0.0.0" && host != "::" && host != "*") return {host};
  std::vector<std::string> addresses;
  ifaddrs* interfaces = nullptr;
  if (getifaddrs(&interfaces) != 0) return addresses;
  for (auto* entry = interfaces; entry != nullptr; entry = entry->ifa_next) {
    if (entry->ifa_addr == nullptr) continue;
    char text[INET6_ADDRSTRLEN] = {};
    if (entry->ifa_addr->sa_family == AF_INET)
      inet_ntop(AF_INET, &reinterpret_cast<const sockaddr_in*>(entry->ifa_addr)->sin_addr, text, sizeof(text));
    else if (entry->ifa_addr->sa_family == AF_INET6)
      inet_ntop(AF_INET6, &reinterpret_cast<const sockaddr_in6*>(entry->ifa_addr)->sin6_addr, text, sizeof(text));
    else continue;
    std::string address = text;
    if (address.find('%') != std::string::npos) continue;  // a scoped link-local address: never a cluster's own
    if (std::find(addresses.begin(), addresses.end(), address) == addresses.end() && addresses.size() < 64) addresses.push_back(address);
  }
  freeifaddrs(interfaces);
  return addresses;
}

// The mirror's poll: the indexes' hints every pollIntervalMs (writes of another control plane instance; this one's
// own are in the mirror as it makes them). A failure leaves the mirror as it is and is counted.
kj::Promise<void> pollForever(coworkerd::ManifestStore& state, kj::Timer& timer, uint32_t intervalMs) {
  for (;;) {
    co_await timer.afterDelay(intervalMs * kj::MILLISECONDS);
    std::string failure;
    try { co_await state.refresh(); } catch (const kj::Exception& error) { failure = cleanReason(error.getDescription().cStr()); }
    if (!failure.empty()) {
      state.notePollFailure(failure);
      if (state.counters().pollFailures == 1 || state.counters().pollFailures % 60 == 0)
        std::cout << "[control] {\"event\":\"poll-failed\",\"failures\":" << state.counters().pollFailures << ",\"reason\":\""
                  << jsonEscape(failure) << "\"}" << std::endl;
    }
  }
}

// The usage objects of the hours past usage.retentionHours (of every node: a node removes its own, so what is left is the
// nodes' that left), removed at the start and then once a day. A failure is logged; the next day tries again.
constexpr uint64_t USAGE_CLEANUP_MS = 86400000;
kj::Promise<void> usageCleanupForever(coworkerd::ObjectStore& storage, kj::Timer& timer, uint32_t retentionHours) {
  for (;;) {
    std::string outcome;
    try {
      auto cleaned = co_await coworkerd::cleanUsage(storage, retentionHours, wallTimeMsForTest());
      outcome = ",\"ok\":true,\"cleanup\":" + cleaned.json();
    } catch (const kj::Exception& error) {
      outcome = ",\"ok\":false,\"reason\":\"" + jsonEscape(cleanReason(error.getDescription().cStr())) + "\"";
    }
    std::cout << "[control] {\"event\":\"usage-cleanup\",\"retentionHours\":" << retentionHours << outcome
              << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
    co_await timer.afterDelay(USAGE_CLEANUP_MS * kj::MILLISECONDS);
  }
}

// Every member hears this control plane's addresses at startup and every ANNOUNCE_MS after.
constexpr uint32_t ANNOUNCE_MS = 10000;
kj::Promise<void> announceForever(ControlService& service, kj::Timer& timer, std::string body) {
  for (;;) {
    co_await service.announce(body);
    co_await timer.afterDelay(ANNOUNCE_MS * kj::MILLISECONDS);
  }
}

int runControlPlane(const std::vector<std::string>& args) {
  ControlSettings settings;
  for (size_t i = 0; i < args.size(); ++i) {
    if (args[i] == "--config" && i + 1 < args.size()) settings.configFile = args[++i];
    else if (args[i].starts_with("--config=")) settings.configFile = args[i].substr(9);
    else KJ_FAIL_REQUIRE("unknown argument; usage: coworkerd-apiserver --config FILE", args[i].c_str());
  }
  KJ_REQUIRE(!settings.configFile.empty(), "control requires --config FILE");
  settings.configFile = fs::absolute(settings.configFile).string();
  auto config = ServerConfig::load(settings.configFile);
  KJ_REQUIRE(config.desiredState.objectStore.backend == "s3" && !config.desiredState.objectStore.endpoint.empty(),
      "the control plane keeps no state of its own: the desired state is the object store's -- set storage.endpoint (and "
      "storage.credentialsFile, the deploy identity)");
  // The test clock offset (clockOffsetMsForTest): the usage's hours as the nodes with the same offset count them.
  clockOffsetMsForTest = config.clockOffsetMsForTest;
  // The secrets key seals every secret's value before it is written (COWORKERD_SECRETS_KEY_FILE overrides the file's).
  settings.secretsKeyFile = config.secretsKeyFile;
  settings.secretsKeySource = settings.secretsKeyFile.empty() ? "unset" : "config";
  if (const char* file = std::getenv("COWORKERD_SECRETS_KEY_FILE"); file != nullptr && *file) {
    settings.secretsKeyFile = file;
    settings.secretsKeySource = "environment";
  }
  KJ_REQUIRE(!settings.secretsKeyFile.empty(), "the control plane seals secrets' values with the cluster's secrets key: set "
      "secrets.keyFile (or COWORKERD_SECRETS_KEY_FILE)");
  auto secretsKey = coworkerd::SecretsKey::load(settings.secretsKeyFile);
  settings.pollIntervalMs = config.controlPollIntervalMs;
  settings.startupTimeoutMs = config.desiredState.startupTimeoutMs;
  settings.storage = config.desiredState.objectStore;
  settings.memberTtlSeconds = config.membership.ttlSeconds;
  settings.memberGraceMs = config.membership.livenessGraceMs;
  settings.usageRetentionHours = config.usage.retentionHours;
  settings.storageCredentials = !config.storageCredentialsFile.empty() ? "file" : !settings.storage.accessKey.empty() ? "inline"
      : std::getenv("AWS_ACCESS_KEY_ID") ? "environment" : "none";
  settings.listenAddress = config.controlListenAddress.empty() ? "0.0.0.0:8940" : config.controlListenAddress;
  settings.pathRouting = config.controlPathRouting;
  // The nodes' management token (what this sends to their trial, state and log endpoints).
  settings.token = config.managementToken;
  settings.tokenSource = settings.token.empty() ? "unset" : "config";
  if (const char* token = std::getenv("COWORKERD_MANAGEMENT_TOKEN"); token != nullptr && *token) {
    ServerConfig::checkToken(token, "COWORKERD_MANAGEMENT_TOKEN");
    settings.token = token;
    settings.tokenSource = "environment";
  }
  // The administrator's credential of /v1.
  settings.adminCredential = config.controlAdminCredential;
  settings.adminSource = settings.adminCredential.empty() ? "" : "config";
  if (const char* admin = std::getenv("COWORKERD_ADMIN_CREDENTIAL"); admin != nullptr && *admin) {
    ServerConfig::checkToken(admin, "COWORKERD_ADMIN_CREDENTIAL");
    settings.adminCredential = admin;
    settings.adminSource = "environment";
  }
  KJ_REQUIRE(!settings.adminCredential.empty(), "the control plane deploys code and requires the administrator's credential: "
      "set control.adminCredential or COWORKERD_ADMIN_CREDENTIAL");
  const char* temporary = std::getenv("TMPDIR");
  settings.temporaryDirectory = !config.controlTemporaryDirectory.empty() ? fs::path(config.controlTemporaryDirectory)
      : temporary && *temporary ? fs::path(temporary) : fs::path("/tmp");
  settings.maxUploadBytes = config.controlMaxUploadBytes;
  settings.serverTimeoutMs = config.controlServerTimeoutMs;
  KJ_REQUIRE(fs::is_directory(settings.temporaryDirectory), "the control plane's temporary directory does not exist",
      settings.temporaryDirectory.string().c_str());
  settings.startedAtUnixMs = wallTimeMsForTest();
  {
    char host[256] = {};
    gethostname(host, sizeof(host) - 1);
    settings.instance = std::string(host[0] ? host : "apiserver") + ":" + std::to_string(::getpid());
  }

  // SIGTERM and SIGINT end the API between requests' turns (captured before the event loop exists).
  kj::UnixEventPort::captureSignal(SIGTERM);
  kj::UnixEventPort::captureSignal(SIGINT);
  auto io = kj::setupAsyncIo();
  auto headerTable = kj::HttpHeaderTable::Builder().build();
  auto& timer = io.provider->getTimer();
  coworkerd::ObjectStore storage(settings.storage, io.provider->getNetwork(), timer);
  coworkerd::ManifestStore state(storage, timer, std::move(secretsKey));
  // The revisions this control plane writes name it: its host and process, and a random value of this start.
  state.setWriterInstance(settings.instance + "/" + coworkerd::newWriterInstance());
  // The mirror read in full before the API answers: the cluster's marker (none: not a cluster's bucket, refused at once),
  // the tenant index, every manifest it names, the credentials index. While the store cannot be reached the start
  // waits, up to desiredState.startupTimeoutMs.
  {
    const auto started = monotonicTimeMs();
    for (uint32_t attempt = 1;; ++attempt) {
      std::string reason;
      try {
        state.load(false, true).wait(io.waitScope);
        break;
      } catch (const kj::Exception& error) {
        reason = error.getDescription().cStr();
      }
      KJ_REQUIRE(reason.find("cluster/format/1") == std::string::npos, reason.c_str());
      KJ_REQUIRE(monotonicTimeMs() - started < settings.startupTimeoutMs, "the desired state cannot be read from the object store",
          settings.storage.endpoint.c_str(), cleanReason(reason).c_str());
      timer.afterDelay(250 * kj::MILLISECONDS).wait(io.waitScope);
    }
  }
  ControlService service(settings, storage, state, timer, io.provider->getNetwork(), *headerTable);
  kj::HttpServer server(timer, *headerTable, service);
  auto address = io.provider->getNetwork().parseAddress(settings.listenAddress.c_str()).wait(io.waitScope);
  auto listener = address->listen();
  std::cout << "[control] {\"event\":\"configuration\",\"version\":" << versionJson() << ",\"configFile\":\""
            << jsonEscape(settings.configFile) << "\",\"listenAddress\":\"" << jsonEscape(settings.listenAddress)
            << "\",\"storage\":{\"endpoint\":\""
            << jsonEscape(settings.storage.endpoint) << "\",\"bucket\":\"" << jsonEscape(settings.storage.bucket)
            << "\",\"credentials\":\"" << settings.storageCredentials << "\"},\"adminCredential\":\"set\",\"adminCredentialSource\":\""
            << settings.adminSource << "\",\"managementToken\":\"" << (settings.token.empty() ? "unset" : "set") << "\",\"tokenSource\":\""
            << settings.tokenSource << "\",\"pathRouting\":" << (settings.pathRouting ? "true" : "false")
            << ",\"temporaryDirectory\":\"" << jsonEscape(settings.temporaryDirectory.string())
            << "\",\"maxUploadBytes\":" << settings.maxUploadBytes << ",\"serverTimeoutMs\":" << settings.serverTimeoutMs
            << ",\"membership\":{\"backend\":\"objectStore\",\"ttlSeconds\":"
            << settings.memberTtlSeconds << ",\"livenessGraceMs\":" << settings.memberGraceMs << "},\"usage\":{\"retentionHours\":"
            << settings.usageRetentionHours << ",\"cleanupIntervalMs\":" << USAGE_CLEANUP_MS << "},\"clockOffsetMsForTest\":"
            << clockOffsetMsForTest << ",\"desiredState\":{\"pollIntervalMs\":"
            << settings.pollIntervalMs << ",\"secretsKeyFile\":\"" << jsonEscape(settings.secretsKeyFile) << "\",\"secretsKeySource\":\""
            << settings.secretsKeySource << "\",\"secretsKey\":\"" << state.secretsKey()->fingerprint() << "\",\"state\":" << state.json()
            << "}}" << std::endl;
  std::cout << "[control] ready http=" << settings.listenAddress << std::endl;
  auto stop = io.unixEventPort.onSignal(SIGTERM).exclusiveJoin(io.unixEventPort.onSignal(SIGINT));
  const auto announced = controlAddresses(settings.listenAddress);
  std::string body = "{\"instance\":\"" + jsonEscape(settings.instance) + "\",\"addresses\":[";
  for (size_t i = 0; i < announced.size(); ++i) body += (i ? ",\"" : "\"") + jsonEscape(announced[i]) + "\"";
  body += "]}";
  std::cout << "[control] {\"event\":\"announce\",\"addresses\":" << body.substr(body.find('[')) << std::endl;
  server.listenHttp(*listener).exclusiveJoin(stop.ignoreResult()).exclusiveJoin(runLogClock(timer))
      .exclusiveJoin(announceForever(service, timer, std::move(body)))
      .exclusiveJoin(pollForever(state, timer, settings.pollIntervalMs))
      .exclusiveJoin(usageCleanupForever(storage, timer, settings.usageRetentionHours)).wait(io.waitScope);
  std::cout << "[control] {\"event\":\"stopped\"}" << std::endl;
  return 0;
}

constexpr const char* USAGE =
    "usage: coworkerd-apiserver --config FILE  (the control plane's API, default 0.0.0.0:8940)\n"
    "       coworkerd-apiserver --version\n"
    "       coworkerd-apiserver tool <init|apply|upsert|remove|show|compile|secret|rotate|drain> ...\n";

}  // namespace

int main(int argc, char** argv) {
  std::unique_ptr<coworkerd::AsyncLog> logging;
  try {
    executableArgument = argc > 0 ? argv[0] : "coworkerd-apiserver";
    std::vector<std::string> args(argv + 1, argv + argc);
    if (args.size() == 1 && (args[0] == "--version" || args[0] == "version")) {
      std::cout << versionText("coworkerd-apiserver");
      return 0;
    }
    if (args.empty() || (args.size() == 1 && (args[0] == "--help" || args[0] == "-h" || args[0] == "help"))) {
      (args.empty() ? std::cerr : std::cout) << USAGE;
      return args.empty() ? 2 : 0;
    }
    if (args[0] != "tool") {
      // The control plane: its own process, with the asynchronous log sink of the Servers.
      logging = std::make_unique<coworkerd::AsyncLog>();
      return runControlPlane(args);
    }
    // The deployment tool (`tool`), kept for the tests and for this daemon.
    kj::UnixEventPort::captureSignal(SIGTERM);
    kj::UnixEventPort::captureSignal(SIGINT);
    if (args.size() >= 2 && args[1] == "rotate") return coworkerd::runRotationTool(argc - 3, argv + 3);
    if (args.size() >= 2 && args[1] == "drain") return runDrainTool(argc - 3, argv + 3);
    if (args.size() >= 2) return coworkerd::runDeploymentTool(argc - 2, argv + 2);
    std::cerr << USAGE;
    return 2;
  } catch (const kj::Exception& error) {
    std::cerr << "coworkerd-apiserver failed: " << error.getDescription().cStr() << std::endl;
    return 1;
  } catch (const std::exception& error) {
    std::cerr << "coworkerd-apiserver failed: " << error.what() << std::endl;
    return 1;
  }
}
