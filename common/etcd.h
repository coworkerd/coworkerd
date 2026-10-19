#pragma once

#include <capnp/compat/json.h>
#include <kj/async-io.h>
#include <kj/compat/http.h>

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace coworkerd {
namespace etcd {
using Json = capnp::JsonValue;
std::string quote(const std::string& value);
Json::Reader field(Json::Reader object, const char* name);
std::string text(Json::Reader object, const char* name);
uint64_t integer(Json::Reader object, const char* name);
bool boolean(Json::Reader object, const char* name);
std::string base64(const std::string& value);
std::string unbase64(const std::string& value);
}

// etcd's authentication. One identity per process: the JSON file {"user":"...","password":"..."} that
// COWORKERD_ETCD_CREDENTIALS_FILE names, else the one given to useEtcdCredentialsFile() (the configuration's
// etcdCredentialsFile, ServerConfig::load). With one, every EtcdClient of the process authenticates
// (/v3/auth/authenticate), sends its token with each request and watch, and authenticates again once when etcd
// refuses the token (expired, or the auth store changed); without one, requests carry no credentials.
// The file must be readable by its owner only. Errors name the file, never its content.
struct EtcdCredentials {
  std::string user, password, file, origin;  // origin: "environment" or "configuration"
};
EtcdCredentials readEtcdCredentials(const std::string& path);
void useEtcdCredentialsFile(const std::string& path);
// The process's identity, or nullptr when it has none (read once, at the first request).
const EtcdCredentials* etcdCredentials();
// For the state endpoints: whether, which user, from where, and the authentication counts; never the password.
std::string etcdAuthJson();

struct EtcdKv {
  std::string key;
  std::string value;
  std::string lease;
  uint64_t modRevision = 0;
  uint64_t createRevision = 0;
  uint64_t version = 0;
};
struct EtcdSnapshot {
  uint64_t revision = 0;
  std::map<std::string, EtcdKv> values;
};
// One complete watch response (possibly several revisions) or a full read.
struct EtcdChange {
  std::string key;
  std::optional<EtcdKv> kv;  // nullopt: deleted
};
struct EtcdBatch {
  uint64_t revision = 0;
  bool full = false;  // every key of the prefix at `revision`, replacing earlier state
  std::vector<EtcdChange> changes;
};
struct EtcdWatchView {
  uint64_t revision = 0;
  uint64_t fullRereads = 0;
  uint64_t compactions = 0;
  bool connected = false;
  std::string lastError;
};

class EtcdClient {
public:
  EtcdClient(std::string endpoint, uint32_t timeoutMs, kj::Network& network,
      kj::Timer& timer, const kj::HttpHeaderTable& headers);
  kj::Promise<std::string> request(std::string path, std::string payload, bool firstFrame = false);
  kj::Promise<EtcdSnapshot> range(const std::string& prefix);
  // The prefix at a given revision (several prefixes read as one snapshot); 0 is the latest.
  kj::Promise<EtcdSnapshot> range(const std::string& prefix, uint64_t revision);
  kj::Promise<kj::HttpClient::Response> openWatch(const std::string& prefix,
      uint64_t revision, kj::HttpClient& client);
  kj::Own<kj::HttpClient> newClient();
  static EtcdKv decodeKv(etcd::Json::Reader kv);

private:
  std::string endpoint;
  uint32_t timeoutMs;
  kj::Network& network;
  kj::Timer& timer;
  const kj::HttpHeaderTable& headers;
  kj::Promise<std::string> requestInner(std::string path, std::string payload, bool firstFrame);
  // The token of the process's identity for this endpoint ("" without one); `renew` authenticates again.
  kj::Promise<std::string> token(bool renew);
  friend class EtcdWatcher;
};

// One implementation for all authorities. A callback receives either an
// entire prefix mirror (Apply) or only the changed keys (ApplyChanges) after
// each full read or complete watch response, never events split into
// individual key updates. The cursor advances only after delivery. A changes
// consumer whose callback fails gets a full read before the watch resumes.
class EtcdWatcher {
public:
  using Apply = std::function<kj::Promise<void>(const EtcdSnapshot&)>;
  using ApplyChanges = std::function<kj::Promise<void>(const EtcdBatch&)>;
  EtcdWatcher(EtcdClient& client, std::string prefix, kj::Timer& timer,
      uint32_t retryMs, Apply apply);
  EtcdWatcher(EtcdClient& client, std::string prefix, kj::Timer& timer,
      uint32_t retryMs, ApplyChanges applyChanges);
  const EtcdWatchView& view() const { return status; }
  kj::Promise<void> fullRead();
  kj::Promise<void> run();

private:
  enum class WatchEnd { ROTATE, COMPACTED };
  EtcdClient& client;
  std::string prefix;
  kj::Timer& timer;
  uint32_t retryMs;
  Apply apply;
  ApplyChanges applyChanges;
  bool deliveryFailed = false;
  EtcdSnapshot mirror;
  EtcdWatchView status;
  bool hasSnapshot = false;
  kj::Promise<WatchEnd> consume(kj::AsyncInputStream& body);
  kj::Promise<WatchEnd> watchOnce();
};
}  // namespace coworkerd
