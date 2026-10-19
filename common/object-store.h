#pragma once

#include <kj/async-io.h>
#include <kj/compat/http.h>

#include <filesystem>
#include <string>

namespace coworkerd {

struct ObjectStoreConfig {
  std::string backend = "s3";
  std::filesystem::path directory;
  std::string endpoint;
  std::string bucket = "coworkerd";
  std::string region = "us-east-1";
  std::string accessKey;
  std::string secretKey;
  uint32_t timeoutMs = 30000;
};

struct ObjectResponse {
  uint status;
  std::string bytes;
};

// Credentials are never included in diagnostics. This client deliberately uses
// only path addressing and signed, single-chunk payloads, without an AWS SDK.
class ObjectStore {
public:
  ObjectStore(ObjectStoreConfig config, kj::Network& network, kj::Timer& timer);
  kj::Promise<bool> exists(std::string key);
  kj::Promise<void> put(std::string key, std::string bytes);
  kj::Promise<ObjectResponse> get(std::string key);
  // Also used by the independent authentication probe (bucket creation and
  // unsigned requests). Normal callers use the three operations above.
  kj::Promise<ObjectResponse> request(kj::HttpMethod method, std::string key,
      std::string bytes = {}, bool sign = true);
  const ObjectStoreConfig& config() const { return settings; }

private:
  ObjectStoreConfig settings;
  kj::Network& network;
  kj::Timer& timer;
  kj::Own<kj::HttpHeaderTable> table;
  kj::HttpHeaderId dateHeader;
  kj::HttpHeaderId hashHeader;
  kj::HttpHeaderId authorizationHeader;
  kj::Promise<ObjectResponse> requestInner(kj::HttpMethod method, std::string key,
      std::string bytes, bool sign);
};

std::string objectSha256(const std::string& bytes);
}  // namespace coworkerd
