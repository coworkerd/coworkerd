// The peer listener's authentication: see peer-auth.h.
#include "peer-auth.h"
#include "support.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/debug.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <sys/stat.h>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <set>
#include <sstream>
#include <strings.h>

namespace coworkerd {
namespace {

constexpr const char* KEY = "x-coworkerd-peer-key";
constexpr const char* TIME = "x-coworkerd-peer-time";
constexpr const char* NONCE = "x-coworkerd-peer-nonce";
constexpr const char* TARGET = "x-coworkerd-peer-target";
constexpr const char* SIGNATURE = "x-coworkerd-peer-signature";
constexpr size_t MAX_NONCES = 1'000'000;
constexpr uint64_t RELOAD_CHECK_MS = 1000;

std::string hex(const unsigned char* bytes, size_t size) {
  static const char* digits = "0123456789abcdef";
  std::string out;
  out.reserve(size * 2);
  for (size_t i = 0; i < size; ++i) {
    out.push_back(digits[bytes[i] >> 4]);
    out.push_back(digits[bytes[i] & 15]);
  }
  return out;
}
std::optional<std::string> unhex(const std::string& text) {
  if (text.size() % 2) return std::nullopt;
  std::string out;
  for (size_t i = 0; i < text.size(); i += 2) {
    auto digit = [](char c) -> int {
      if (c >= '0' && c <= '9') return c - '0';
      if (c >= 'a' && c <= 'f') return c - 'a' + 10;
      if (c >= 'A' && c <= 'F') return c - 'A' + 10;
      return -1;
    };
    const int high = digit(text[i]), low = digit(text[i + 1]);
    if (high < 0 || low < 0) return std::nullopt;
    out.push_back(static_cast<char>(high * 16 + low));
  }
  return out;
}
std::string hmac(const std::string& key, const std::string& message) {
  unsigned char out[EVP_MAX_MD_SIZE];
  size_t length = 0;
  KJ_REQUIRE(EVP_Q_mac(nullptr, "HMAC", nullptr, "SHA256", nullptr, key.data(), key.size(),
      reinterpret_cast<const unsigned char*>(message.data()), message.size(), out, sizeof(out), &length) != nullptr,
      "HMAC-SHA256 failed");
  return hex(out, length);
}
std::string canonical(kj::StringPtr method, kj::StringPtr path, const std::string& target, const std::string& time,
    const std::string& nonce, std::string_view body) {
  return std::string("coworkerd-peer-v1\n") + method.cStr() + "\n" + path.cStr() + "\n" + target + "\n" + time + "\n" + nonce +
      "\n" + sha256Hex(body);
}
}  // namespace

std::string PeerAuth::Key::tag() const { return id + "@" + sha256Hex(secret).substr(0, 16); }

PeerAuth::PeerAuth(const PeerAuthConfig& config, std::string serverId, kj::Timer& timer)
    : config(config), serverId(std::move(serverId)), timer(timer) {
  if (config.keyFile.empty()) return;
  keys = load();
  signature = fileSignature();
  std::cout << "[peer-auth] {\"serverId\":\"" << jsonEscape(this->serverId) << "\",\"event\":\"keys\",\"keyFile\":\""
            << jsonEscape(config.keyFile) << "\",\"keys\":[";
  for (size_t i = 0; i < keys.size(); ++i) std::cout << (i ? "," : "") << "\"" << jsonEscape(keys[i].id) << "\"";
  std::cout << "],\"signing\":\"" << jsonEscape(keys.front().id) << "\",\"windowMs\":" << config.windowMs << "}" << std::endl;
}
PeerAuth::~PeerAuth() noexcept(false) {}

// The key file: one or two keys, ids unique, secrets of 32 to 64 bytes in hex; a regular file only its owner reads.
// Errors name the file and the rule, never a secret.
std::vector<PeerAuth::Key> PeerAuth::load() const {
  const auto& path = config.keyFile;
  struct stat info{};
  KJ_REQUIRE(::stat(path.c_str(), &info) == 0, "cannot read the peer key file", path.c_str());
  KJ_REQUIRE(S_ISREG(info.st_mode) && (info.st_mode & 077) == 0,
      "the peer key file must be a regular file readable by its owner only (mode 0600)", path.c_str());
  const auto text = readFile(path);
  capnp::MallocMessageBuilder message;
  auto json = message.initRoot<capnp::JsonValue>();
  try {
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(text.data(), text.size()), json);
  } catch (const kj::Exception&) {
    KJ_FAIL_REQUIRE("the peer key file is not JSON", path.c_str());
  }
  auto bad = [&]() {
    KJ_FAIL_REQUIRE("the peer key file must be {\"keys\":[{\"id\":\"...\",\"secret\":\"<64 to 128 hex digits>\"}, one or two]}",
        path.c_str());
  };
  if (!json.isObject() || json.getObject().size() != 1 || json.getObject()[0].getName() != "keys" ||
      !json.getObject()[0].getValue().isArray()) bad();
  std::vector<Key> result;
  std::set<std::string> ids;
  for (auto entry : json.getObject()[0].getValue().getArray()) {
    if (!entry.isObject()) bad();
    std::string id, secretText;
    for (auto field : entry.getObject()) {
      if (!field.getValue().isString()) bad();
      const std::string name = field.getName().cStr();
      if (name == "id") id = field.getValue().getString().cStr();
      else if (name == "secret") secretText = field.getValue().getString().cStr();
      else bad();
    }
    if (!safeName(id) || id.size() > 64 || secretText.size() < 64 || secretText.size() > 128) bad();
    auto secret = unhex(secretText);
    if (!secret) bad();
    KJ_REQUIRE(ids.insert(id).second, "the peer key file names a key twice", path.c_str(), id.c_str());
    result.push_back({id, *secret});
  }
  if (result.empty() || result.size() > 2) bad();
  return result;
}

std::string PeerAuth::fileSignature() const {
  struct stat info{};
  if (::stat(config.keyFile.c_str(), &info) != 0) return "missing";
#ifdef __APPLE__
  const auto modified = info.st_mtimespec;
#else
  const auto modified = info.st_mtim;
#endif
  return std::to_string(info.st_ino) + ":" + std::to_string(info.st_size) + ":" + std::to_string(modified.tv_sec) + "." +
      std::to_string(modified.tv_nsec) + ":" + std::to_string(info.st_mode);
}

void PeerAuth::apply(std::vector<Key> next) {
  const std::string previousSigning = keys.empty() ? "" : keys.front().tag();
  std::set<std::string> nextTags;
  for (const auto& key : next) nextTags.insert(key.tag());
  // A key that left the file (or whose secret changed under the same id): its sessions are closed windowMs from now.
  // A key that came back before then is accepted again, its sessions kept.
  for (const auto& key : keys) {
    if (nextTags.contains(key.tag())) continue;
    auto& retirement = retirements[key.tag()];
    if (retirement.at == 0) retirement.at = monotonicTimeMs() + config.windowMs;
  }
  for (const auto& tag : nextTags) {
    auto found = retirements.find(tag);
    if (found != retirements.end()) found->second.at = 0;
  }
  keys = std::move(next);
  if (keys.front().tag() != previousSigning) ++generation;
}

PeerAuth::Verdict PeerAuth::verify(const kj::HttpHeaders& headers, kj::StringPtr method, kj::StringPtr path, std::string_view body) {
  if (!enabled()) return {};
  std::map<std::string, std::string> values;
  bool repeated = false;
  headers.forEach([&](kj::StringPtr name, kj::StringPtr value) {
    for (const char* header : {KEY, TIME, NONCE, TARGET, SIGNATURE}) {
      if (strcasecmp(name.cStr(), header) != 0) continue;
      if (values.contains(header)) repeated = true;
      values[header] = value.cStr();
    }
  });
  if (values.empty()) return {"missing", ""};
  if (repeated || values.size() != 5) return {"malformed", ""};
  const auto& keyId = values[KEY];
  const auto& time = values[TIME];
  const auto& nonce = values[NONCE];
  if (time.empty() || time.size() > 16 || !std::all_of(time.begin(), time.end(), [](char c) { return c >= '0' && c <= '9'; }) ||
      nonce.size() != 32 || !unhex(nonce) || values[SIGNATURE].size() != 64) return {"malformed", keyId};
  auto key = std::find_if(keys.begin(), keys.end(), [&](const Key& k) { return k.id == keyId; });
  if (key == keys.end()) return {"unknown-key", keyId};
  if (values[TARGET] != serverId) return {"wrong-target", keyId};
  const double now = static_cast<double>(wallTimeMsForTest());
  const double at = std::stod(time);
  if (std::fabs(now - at) > config.windowMs) return {"expired", keyId};
  const auto expected = hmac(key->secret, canonical(method, path, values[TARGET], time, nonce, body));
  const auto& presented = values[SIGNATURE];
  if (CRYPTO_memcmp(expected.data(), presented.data(), expected.size()) != 0) return {"bad-signature", keyId};
  const auto tag = key->tag();
  // A nonce is remembered until its signature expires; only a signature that verified gets that far.
  for (auto it = nonces.begin(); it != nonces.end();) it = it->second < now ? nonces.erase(it) : std::next(it);
  if (nonces.contains(nonce)) return {"replayed", keyId};
  if (nonces.size() >= MAX_NONCES) return {"replay-cache-full", keyId};
  nonces.emplace(nonce, at + config.windowMs);
  ++accepted[keyId];
  return {"", keyId, tag};
}

void PeerAuth::sign(kj::HttpHeaders& headers, kj::StringPtr method, kj::StringPtr path, const std::string& targetServerId,
    std::string_view body) {
  if (!enabled()) return;
  unsigned char random[16];
  KJ_REQUIRE(RAND_bytes(random, sizeof(random)) == 1, "cannot draw a nonce");
  const auto nonce = hex(random, sizeof(random));
  const auto time = std::to_string(wallTimeMsForTest());
  const auto& key = keys.front();
  headers.add(kj::str(KEY), kj::str(key.id.c_str()));
  headers.add(kj::str(TIME), kj::str(time.c_str()));
  headers.add(kj::str(NONCE), kj::str(nonce.c_str()));
  headers.add(kj::str(TARGET), kj::str(targetServerId.c_str()));
  headers.add(kj::str(SIGNATURE), kj::str(hmac(key.secret, canonical(method, path, targetServerId, time, nonce, body)).c_str()));
  ++signedRequests;
}

void PeerAuth::refused(const char* listener, kj::StringPtr method, const Verdict& verdict) {
  ++refusedByReason[verdict.refusal];
  std::cout << "[peer-auth] {\"serverId\":\"" << jsonEscape(serverId) << "\",\"event\":\"refused\",\"listener\":\"" << listener
            << "\",\"method\":\"" << method.cStr() << "\",\"reason\":\"" << verdict.refusal << "\",\"keyId\":\""
            << jsonEscape(verdict.keyId.substr(0, 64)) << "\",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
}

kj::Promise<void> PeerAuth::retired(const std::string& keyTag) {
  auto& retirement = retirements[keyTag];
  if (!retirement.fulfiller) {
    auto pair = kj::newPromiseAndFulfiller<void>();
    retirement.fulfiller = kj::mv(pair.fulfiller);
    retirement.promise = pair.promise.fork();
  }
  return retirement.promise.addBranch();
}

void PeerAuth::sessionRetired(const std::string& keyTag) {
  ++retiredSessions;
  std::cout << "[peer-auth] {\"serverId\":\"" << jsonEscape(serverId) << "\",\"event\":\"session-retired\",\"key\":\""
            << jsonEscape(keyTag) << "\",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
}

kj::Promise<void> PeerAuth::run() {
  if (!enabled()) co_await kj::Promise<void>(kj::NEVER_DONE);
  while (true) {
    co_await timer.afterDelay(RELOAD_CHECK_MS * kj::MILLISECONDS);
    if (auto current = fileSignature(); current != signature) {
      signature = current;
      try {
        auto next = load();
        apply(std::move(next));
        ++reloads;
        lastReloadError.clear();
        std::cout << "[peer-auth] {\"serverId\":\"" << jsonEscape(serverId) << "\",\"event\":\"keys-reloaded\",\"keys\":[";
        for (size_t i = 0; i < keys.size(); ++i) std::cout << (i ? "," : "") << "\"" << jsonEscape(keys[i].id) << "\"";
        std::cout << "],\"signing\":\"" << jsonEscape(keys.front().id) << "\",\"signingGeneration\":" << generation
                  << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
      } catch (const kj::Exception& error) {
        // A bad edit keeps the keys in use: the cluster keeps working while the file is fixed.
        ++reloadFailures;
        lastReloadError = error.getDescription().cStr();
        std::cout << "[peer-auth] {\"serverId\":\"" << jsonEscape(serverId) << "\",\"event\":\"reload-failed\",\"error\":\""
                  << jsonEscape(lastReloadError.substr(0, 400)) << "\",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
      }
    }
    const double now = monotonicTimeMs();
    for (auto it = retirements.begin(); it != retirements.end();) {
      auto& retirement = it->second;
      if (retirement.at != 0 && retirement.at <= now) {
        if (retirement.fulfiller) retirement.fulfiller->fulfill();
        it = retirements.erase(it);
      } else ++it;
    }
  }
}

std::string PeerAuth::json() const {
  std::ostringstream out;
  out << "{\"enabled\":" << (enabled() ? "true" : "false") << ",\"keyFile\":\"" << jsonEscape(config.keyFile)
      << "\",\"windowMs\":" << config.windowMs << ",\"keys\":[";
  for (size_t i = 0; i < keys.size(); ++i) out << (i ? "," : "") << "\"" << jsonEscape(keys[i].id) << "\"";
  out << "],\"signingKey\":\"" << (keys.empty() ? "" : jsonEscape(keys.front().id)) << "\",\"signingGeneration\":" << generation
      << ",\"signed\":" << signedRequests << ",\"accepted\":{";
  size_t index = 0;
  for (const auto& [id, count] : accepted) out << (index++ ? "," : "") << "\"" << jsonEscape(id) << "\":" << count;
  out << "},\"refused\":{";
  index = 0;
  for (const auto& [reason, count] : refusedByReason) out << (index++ ? "," : "") << "\"" << jsonEscape(reason) << "\":" << count;
  out << "},\"reloads\":" << reloads << ",\"reloadFailures\":" << reloadFailures << ",\"lastReloadError\":\""
      << jsonEscape(lastReloadError.substr(0, 400)) << "\",\"retiringKeys\":"
      << std::count_if(retirements.begin(), retirements.end(), [](const auto& entry) { return entry.second.at != 0; })
      << ",\"retiredSessions\":" << retiredSessions << ",\"nonces\":" << nonces.size() << "}";
  return out.str();
}

}  // namespace coworkerd
