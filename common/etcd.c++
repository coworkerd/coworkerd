#include "etcd.h"

#include <capnp/message.h>
#include <kj/encoding.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <sys/stat.h>

namespace coworkerd {
namespace {
constexpr size_t MAX_JSON_BYTES = 16 * 1024 * 1024;
std::string prefixEnd(std::string prefix) {
  KJ_REQUIRE(!prefix.empty(), "etcd prefix must not be empty");
  ++prefix.back();
  return prefix;
}

class JsonFrames {
public:
  explicit JsonFrames(kj::AsyncInputStream& input): input(input) {}
  kj::Promise<std::string> next() {
    while (true) {
      auto end = pending.find('\n');
      if (end != std::string::npos) {
        auto frame = pending.substr(0, end);
        pending.erase(0, end + 1);
        if (!frame.empty()) co_return frame;
        continue;
      }
      std::array<char, 8192> buffer;
      auto count = co_await input.tryRead(buffer.data(), 1, buffer.size());
      KJ_REQUIRE(count > 0, "etcd JSON stream ended");
      pending.append(buffer.data(), count);
      KJ_REQUIRE(pending.size() <= MAX_JSON_BYTES, "etcd JSON frame is too large");
    }
  }
private:
  kj::AsyncInputStream& input;
  std::string pending;
};
}  // namespace

namespace etcd {
std::string quote(const std::string& value) {
  capnp::MallocMessageBuilder message;
  auto json = message.initRoot<Json>();
  json.setString(value.c_str());
  return capnp::JsonCodec().encodeRaw(json).cStr();
}
Json::Reader field(Json::Reader object, const char* name) {
  KJ_REQUIRE(object.isObject(), "expected a JSON object");
  for (auto entry : object.getObject()) if (entry.getName() == name) return entry.getValue();
  return Json::Reader();
}
std::string text(Json::Reader object, const char* name) {
  auto value = field(object, name);
  KJ_REQUIRE(value.isString(), "expected a JSON string", name);
  return value.getString().cStr();
}
uint64_t integer(Json::Reader object, const char* name) {
  auto value = field(object, name);
  if (value.isNull()) return 0;
  KJ_REQUIRE(value.isString(), "expected an etcd integer string", name);
  std::string string = value.getString().cStr();
  KJ_REQUIRE(!string.empty() && std::all_of(string.begin(), string.end(),
      [](unsigned char c) { return std::isdigit(c); }), "invalid etcd integer", name);
  size_t end = 0;
  auto result = std::stoull(string, &end);
  KJ_REQUIRE(end == string.size(), "invalid etcd integer", name);
  return result;
}
bool boolean(Json::Reader object, const char* name) {
  auto value = field(object, name);
  return value.isBoolean() && value.getBoolean();
}
std::string base64(const std::string& value) {
  return kj::encodeBase64(kj::arrayPtr(reinterpret_cast<const kj::byte*>(value.data()), value.size())).cStr();
}
std::string unbase64(const std::string& value) {
  auto result = kj::decodeBase64(kj::arrayPtr(value.data(), value.size()));
  KJ_REQUIRE(!result.hadErrors, "invalid base64 in etcd response");
  return {reinterpret_cast<const char*>(result.begin()), result.size()};
}
}  // namespace etcd
using namespace etcd;

// ---- etcd's authentication ----
namespace {
struct EtcdAuthState {
  bool loaded = false;
  std::optional<EtcdCredentials> credentials;
  std::string fileSignature;  // of the credentials file as last read
  uint64_t rereads = 0;
  std::map<std::string, std::string> tokens;  // by endpoint
  uint64_t authentications = 0, renewals = 0, failures = 0, refusedTokens = 0;
  std::string lastError;
};
EtcdAuthState& authState() {
  static EtcdAuthState state;
  return state;
}
std::string signatureOf(const std::string& path) {
  struct stat info{};
  if (::stat(path.c_str(), &info) != 0) return "missing";
#ifdef __APPLE__
  const auto modified = info.st_mtimespec;
#else
  const auto modified = info.st_mtim;
#endif
  return std::to_string(info.st_ino) + ":" + std::to_string(info.st_size) + ":" + std::to_string(modified.tv_sec) + "." +
      std::to_string(modified.tv_nsec);
}
// A request refused for its token (expired, unknown after a restart, or issued before the auth store changed):
// authenticate again and repeat it once.
bool tokenRefused(uint statusCode, const std::string& body) {
  return statusCode == 401 || body.find("invalid auth token") != std::string::npos ||
      body.find("auth store is old") != std::string::npos;
}
}  // namespace

EtcdCredentials readEtcdCredentials(const std::string& path) {
  struct stat info{};
  KJ_REQUIRE(::stat(path.c_str(), &info) == 0, "cannot read the etcd credentials file", path.c_str());
  KJ_REQUIRE(S_ISREG(info.st_mode) && (info.st_mode & 077) == 0,
      "the etcd credentials file must be a regular file readable by its owner only (mode 0600)", path.c_str());
  std::ifstream input(path, std::ios::binary);
  KJ_REQUIRE(input.good(), "cannot read the etcd credentials file", path.c_str());
  std::stringstream buffer;
  buffer << input.rdbuf();
  const auto text = buffer.str();
  capnp::MallocMessageBuilder message;
  auto json = message.initRoot<Json>();
  try {
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(text.data(), text.size()), json);
  } catch (const kj::Exception&) {
    KJ_FAIL_REQUIRE("the etcd credentials file is not JSON", path.c_str());
  }
  KJ_REQUIRE(json.isObject(), "the etcd credentials file must be {\"user\":...,\"password\":...}", path.c_str());
  EtcdCredentials result;
  result.file = path;
  for (auto entry : json.getObject()) {
    const std::string name = entry.getName().cStr();
    auto value = entry.getValue();
    KJ_REQUIRE((name == "user" || name == "password") && value.isString(),
        "the etcd credentials file holds user and password strings only", path.c_str());
    (name == "user" ? result.user : result.password) = value.getString().cStr();
  }
  KJ_REQUIRE(!result.user.empty() && result.user.size() <= 128 && !result.password.empty() && result.password.size() <= 4096,
      "the etcd credentials file needs a user (at most 128 bytes) and a password (at most 4096)", path.c_str());
  KJ_REQUIRE(std::all_of(result.user.begin(), result.user.end(),
      [](unsigned char c) { return std::isalnum(c) || c == '-' || c == '_' || c == '.'; }),
      "the etcd user name may hold letters, digits, '-', '_' and '.' only", path.c_str());
  return result;
}
const EtcdCredentials* etcdCredentials() {
  auto& state = authState();
  if (!state.loaded) {
    state.loaded = true;
    if (const char* file = std::getenv("COWORKERD_ETCD_CREDENTIALS_FILE"); file != nullptr && *file) {
      state.fileSignature = signatureOf(file);
      state.credentials = readEtcdCredentials(file);
      state.credentials->origin = "environment";
    }
  }
  return state.credentials ? &*state.credentials : nullptr;
}
void useEtcdCredentialsFile(const std::string& path) {
  auto& state = authState();
  // COWORKERD_ETCD_CREDENTIALS_FILE, when set, overrides the configuration's file.
  if (path.empty() || (etcdCredentials() != nullptr && state.credentials->origin == "environment")) return;
  state.fileSignature = signatureOf(path);
  state.credentials = readEtcdCredentials(path);
  state.credentials->origin = "configuration";
  state.tokens.clear();
}
std::string etcdAuthJson() {
  auto& state = authState();
  const auto* credentials = etcdCredentials();
  std::ostringstream out;
  out << "{\"enabled\":" << (credentials ? "true" : "false");
  if (credentials) {
    out << ",\"user\":" << quote(credentials->user) << ",\"file\":" << quote(credentials->file)
        << ",\"origin\":" << quote(credentials->origin);
  }
  out << ",\"authentications\":" << state.authentications << ",\"renewals\":" << state.renewals << ",\"fileRereads\":" << state.rereads
      << ",\"refusedTokens\":" << state.refusedTokens << ",\"failures\":" << state.failures
      << ",\"lastError\":" << quote(state.lastError) << "}";
  return out.str();
}
kj::Promise<std::string> EtcdClient::token(bool renew) {
  const auto* credentials = etcdCredentials();
  if (credentials == nullptr) co_return std::string();
  auto& state = authState();
  // A renewal reads the file again when it changed (a password rotation: the file first, then etcd), keeping the
  // identity it had when the new file cannot be read.
  if (renew && signatureOf(credentials->file) != state.fileSignature) {
    try {
      auto next = readEtcdCredentials(credentials->file);
      next.origin = credentials->origin;
      state.fileSignature = signatureOf(credentials->file);
      state.credentials = std::move(next);
      credentials = &*state.credentials;
      ++state.rereads;
    } catch (const kj::Exception& error) {
      state.lastError = std::string("cannot read the credentials file again: ") + error.getDescription().cStr();
    }
  }
  if (!renew) {
    auto found = state.tokens.find(endpoint);
    if (found != state.tokens.end()) co_return found->second;
  }
  // The password goes only into this request's body; no error or log line carries it.
  const auto payload = "{\"name\":" + quote(credentials->user) + ",\"password\":" + quote(credentials->password) + "}";
  auto client = newClient();
  kj::HttpHeaders requestHeaders(headers);
  requestHeaders.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
  requestHeaders.setPtr(kj::HttpHeaderId::CONNECTION, "close");
  auto call = client->request(kj::HttpMethod::POST, (endpoint + "/v3/auth/authenticate").c_str(), requestHeaders, payload.size());
  co_await call.body->write(kj::arrayPtr(payload.data(), payload.size()).asBytes());
  call.body = nullptr;
  auto response = co_await call.response;
  std::string body = (co_await response.body->readAllText(64 * 1024)).cStr();
  std::string token;
  if (response.statusCode == 200) {
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(body.data(), body.size()), json);
    auto value = field(json, "token");
    if (value.isString()) token = value.getString().cStr();
  }
  if (token.empty()) {
    ++state.failures;
    state.lastError = "authentication as " + credentials->user + " failed: HTTP " + std::to_string(response.statusCode) +
        " " + body.substr(0, 300);
    KJ_FAIL_REQUIRE("etcd authentication failed", endpoint.c_str(), credentials->user.c_str(), response.statusCode, body.substr(0, 300).c_str());
  }
  ++(renew ? state.renewals : state.authentications);
  state.tokens[endpoint] = token;
  co_return token;
}

EtcdClient::EtcdClient(std::string endpoint, uint32_t timeoutMs, kj::Network& network,
    kj::Timer& timer, const kj::HttpHeaderTable& headers)
    : endpoint(std::move(endpoint)), timeoutMs(timeoutMs), network(network), timer(timer), headers(headers) {
  KJ_REQUIRE(this->endpoint.starts_with("http://") && !this->endpoint.ends_with('/'),
      "etcd endpoint must be an HTTP gateway URL without a trailing slash");
  KJ_REQUIRE(timeoutMs > 0, "etcd request timeout must be positive");
}
kj::Own<kj::HttpClient> EtcdClient::newClient() {
  return kj::newHttpClient(timer, headers, network, kj::none);
}
kj::Promise<std::string> EtcdClient::requestInner(std::string path, std::string payload, bool firstFrame) {
  for (bool renewed = false;; renewed = true) {
    // The process's identity, when it has one; a refused token is renewed once.
    const auto authorization = co_await token(renewed);
    auto client = newClient();
    kj::HttpHeaders requestHeaders(headers);
    requestHeaders.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
    requestHeaders.setPtr(kj::HttpHeaderId::CONNECTION, "close");
    if (!authorization.empty()) requestHeaders.add(kj::str("Authorization"), kj::heapString(authorization.data(), authorization.size()));
    auto call = client->request(kj::HttpMethod::POST, (endpoint + path).c_str(), requestHeaders, payload.size());
    co_await call.body->write(kj::arrayPtr(payload.data(), payload.size()).asBytes());
    call.body = nullptr;
    auto response = co_await call.response;
    std::string result;
    if (firstFrame && response.statusCode == 200) {
      JsonFrames frames(*response.body);
      result = co_await frames.next();
    } else result = (co_await response.body->readAllText(MAX_JSON_BYTES)).cStr();
    if (!authorization.empty() && tokenRefused(response.statusCode, result)) {
      ++authState().refusedTokens;
      if (!renewed) continue;
    }
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(result.data(), result.size()), json);
    auto code = field(json, "code");
    const bool revoked = path == "/v3/lease/revoke" && response.statusCode == 404 &&
        code.isNumber() && code.getNumber() == 5;
    KJ_REQUIRE(response.statusCode == 200 || revoked, "etcd HTTP request failed", path.c_str(), response.statusCode, result.c_str());
    KJ_REQUIRE(field(json, "error").isNull(), "etcd gateway error", result.c_str());
    co_return result;
  }
}
kj::Promise<std::string> EtcdClient::request(std::string path, std::string payload, bool firstFrame) {
  return timer.timeoutAfter(timeoutMs * kj::MILLISECONDS,
      requestInner(std::move(path), std::move(payload), firstFrame));
}
EtcdKv EtcdClient::decodeKv(Json::Reader kv) {
  auto lease = field(kv, "lease");
  auto value = field(kv, "value");
  // etcd omits an empty value from JSON.
  return {unbase64(text(kv, "key")), value.isString() ? unbase64(value.getString().cStr()) : std::string(),
      lease.isString() ? lease.getString().cStr() : "", integer(kv, "mod_revision"),
      integer(kv, "create_revision"), integer(kv, "version")};
}
kj::Promise<EtcdSnapshot> EtcdClient::range(const std::string& prefix) {
  return range(prefix, 0);
}
kj::Promise<EtcdSnapshot> EtcdClient::range(const std::string& prefix, uint64_t revision) {
  // Pages keep every response far below the frame limit; all pages read the
  // revision of the first one, so the result is one consistent snapshot.
  constexpr const char* PAGE_KEYS = "2000";
  EtcdSnapshot snapshot;
  snapshot.revision = revision;
  std::string from = prefix;
  while (true) {
    auto response = co_await request("/v3/kv/range", "{\"key\":" + quote(base64(from)) +
        ",\"range_end\":" + quote(base64(prefixEnd(prefix))) + ",\"limit\":\"" + PAGE_KEYS + "\"" +
        (snapshot.revision ? ",\"revision\":\"" + std::to_string(snapshot.revision) + "\"" : std::string()) + "}");
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
    if (!snapshot.revision) snapshot.revision = integer(field(json, "header"), "revision");
    auto kvs = field(json, "kvs");
    std::string last;
    if (kvs.isArray()) for (auto kv : kvs.getArray()) {
      auto value = decodeKv(kv);
      KJ_REQUIRE(value.key.starts_with(prefix), "unexpected etcd range key");
      last = value.key;
      snapshot.values.emplace(value.key, std::move(value));
    }
    if (!boolean(json, "more")) break;
    KJ_REQUIRE(!last.empty(), "etcd range reported more keys without returning any");
    from = last + '\0';
  }
  co_return snapshot;
}
kj::Promise<kj::HttpClient::Response> EtcdClient::openWatch(const std::string& prefix,
    uint64_t revision, kj::HttpClient& client) {
  // The process's identity, when it has one (a watch refused for its token says so in its first frame).
  const auto authorization = co_await timer.timeoutAfter(timeoutMs * kj::MILLISECONDS, token(false));
  kj::HttpHeaders requestHeaders(headers);
  requestHeaders.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
  if (!authorization.empty()) requestHeaders.add(kj::str("Authorization"), kj::heapString(authorization.data(), authorization.size()));
  auto payload = "{\"create_request\":{\"key\":" + quote(base64(prefix)) +
      ",\"range_end\":" + quote(base64(prefixEnd(prefix))) + ",\"start_revision\":" +
      quote(std::to_string(revision)) + ",\"progress_notify\":true}}";
  auto call = client.request(kj::HttpMethod::POST, (endpoint + "/v3/watch").c_str(), requestHeaders, payload.size());
  co_await timer.timeoutAfter(timeoutMs * kj::MILLISECONDS,
      call.body->write(kj::arrayPtr(payload.data(), payload.size()).asBytes()));
  call.body = nullptr;
  auto response = co_await timer.timeoutAfter(timeoutMs * kj::MILLISECONDS, kj::mv(call.response));
  KJ_REQUIRE(response.statusCode == 200, "etcd HTTP watch failed", response.statusCode);
  co_return response;
}

EtcdWatcher::EtcdWatcher(EtcdClient& client, std::string prefix, kj::Timer& timer,
    uint32_t retryMs, Apply apply)
    : client(client), prefix(std::move(prefix)), timer(timer), retryMs(retryMs), apply(std::move(apply)) {}
EtcdWatcher::EtcdWatcher(EtcdClient& client, std::string prefix, kj::Timer& timer,
    uint32_t retryMs, ApplyChanges applyChanges)
    : client(client), prefix(std::move(prefix)), timer(timer), retryMs(retryMs), applyChanges(std::move(applyChanges)) {}
kj::Promise<void> EtcdWatcher::fullRead() {
  auto next = co_await client.range(prefix);
  KJ_REQUIRE(next.revision >= status.revision, "etcd snapshot revision went backwards");
  if (applyChanges) {
    EtcdBatch batch{next.revision, true, {}};
    batch.changes.reserve(next.values.size());
    for (auto& [key, kv] : next.values) batch.changes.push_back({key, std::move(kv)});
    co_await applyChanges(batch);
    deliveryFailed = false;
    mirror.revision = next.revision;
  } else {
    co_await apply(next);
    mirror = std::move(next);
  }
  status.revision = mirror.revision;
  if (hasSnapshot) ++status.fullRereads;
  hasSnapshot = true;
  std::cout << "[etcd-watch] full-read prefix=" << prefix << " revision=" << status.revision
            << " rereads=" << status.fullRereads << std::endl;
}
kj::Promise<EtcdWatcher::WatchEnd> EtcdWatcher::consume(kj::AsyncInputStream& body) {
  JsonFrames frames(body);
  while (true) {
    // Rotate on idle reads, not while a consumer is applying a delayed batch.
    auto frame = co_await frames.next().exclusiveJoin(timer.afterDelay(2000 * kj::MILLISECONDS)
        .then([] { return std::string(); }));
    if (frame.empty()) co_return WatchEnd::ROTATE;
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(frame.data(), frame.size()), json);
    KJ_REQUIRE(field(json, "error").isNull(), "etcd watch gateway error", frame.c_str());
    auto result = field(json, "result");
    if (boolean(result, "canceled")) {
      // A watch created with a token etcd no longer accepts: the next attempt authenticates again.
      if (tokenRefused(0, frame) && etcdCredentials() != nullptr) {
        ++authState().refusedTokens;
        authState().tokens.erase(client.endpoint);
      }
      auto compacted = integer(result, "compact_revision");
      KJ_REQUIRE(compacted > 0, "etcd canceled watch", frame.c_str());
      ++status.compactions;
      status.connected = false;
      std::cout << "[etcd-watch] watch compacted prefix=" << prefix
                << " requested-revision=" << status.revision + 1 << " compact-revision=" << compacted << std::endl;
      co_return WatchEnd::COMPACTED;
    }
    KJ_REQUIRE(!boolean(result, "fragment"), "STOP: etcd watch fragmented a revision");
    status.connected = true;
    // The creation header can be newer than historical events still pending.
    if (boolean(result, "created")) continue;
    auto events = field(result, "events");
    if (!events.isArray()) {
      status.revision = std::max(status.revision, integer(field(result, "header"), "revision"));
      mirror.revision = status.revision;
      continue;
    }
    if (applyChanges) {
      // Only the changed keys; the consumer keeps its own state.
      EtcdBatch batch{status.revision, false, {}};
      for (auto event : events.getArray()) {
        auto kv = field(event, "kv");
        auto revision = integer(kv, "mod_revision");
        if (revision <= status.revision) continue;
        auto key = unbase64(text(kv, "key"));
        KJ_REQUIRE(key.starts_with(prefix), "unexpected etcd watch key");
        auto type = field(event, "type");
        if (type.isString() && type.getString() == "DELETE") batch.changes.push_back({key, std::nullopt});
        else batch.changes.push_back({key, EtcdClient::decodeKv(kv)});
        batch.revision = std::max(batch.revision, revision);
      }
      if (!batch.changes.empty()) {
        try { co_await applyChanges(batch); }
        catch (...) { deliveryFailed = true; throw; }
        mirror.revision = batch.revision;
        status.revision = batch.revision;
      }
      continue;
    }
    auto next = mirror;
    bool changed = false;
    for (auto event : events.getArray()) {
      auto kv = field(event, "kv");
      auto revision = integer(kv, "mod_revision");
      if (revision <= status.revision) continue;
      auto key = unbase64(text(kv, "key"));
      KJ_REQUIRE(key.starts_with(prefix), "unexpected etcd watch key");
      auto type = field(event, "type");
      if (type.isString() && type.getString() == "DELETE") next.values.erase(key);
      else next.values[key] = EtcdClient::decodeKv(kv);
      next.revision = std::max(next.revision, revision);
      changed = true;
    }
    if (changed) {
      co_await apply(next);
      mirror = std::move(next);
      status.revision = mirror.revision;
    }
  }
}
kj::Promise<EtcdWatcher::WatchEnd> EtcdWatcher::watchOnce() {
  auto http = client.newClient();
  auto response = co_await client.openWatch(prefix, status.revision + 1, *http);
  co_return co_await consume(*response.body);
}
kj::Promise<void> EtcdWatcher::run() {
  bool reread = false;
  while (true) {
    bool disconnected = false;
    try {
      if (reread || deliveryFailed) { co_await fullRead(); reread = false; }
      reread = co_await watchOnce() == WatchEnd::COMPACTED;
    } catch (const kj::Exception& error) {
      status.connected = false;
      status.lastError = error.getDescription().cStr();
      if (status.lastError.find("STOP:") != std::string::npos) throw;
      std::cerr << "[etcd-watch] watch failed prefix=" << prefix << " revision=" << status.revision
                << " error=" << status.lastError << std::endl;
      disconnected = true;
    }
    if (disconnected) co_await timer.afterDelay(retryMs * kj::MILLISECONDS);
  }
}
}  // namespace coworkerd
