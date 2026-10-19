#include "object-store.h"

#include <kj/debug.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <sstream>

namespace coworkerd {
namespace {
std::string hex(const unsigned char* bytes, size_t size) {
  constexpr char alphabet[] = "0123456789abcdef";
  std::string result(size * 2, '0');
  for (size_t i = 0; i < size; ++i) {
    result[i * 2] = alphabet[bytes[i] >> 4];
    result[i * 2 + 1] = alphabet[bytes[i] & 15];
  }
  return result;
}
std::string hmac(const std::string& key, const std::string& bytes) {
  unsigned char output[EVP_MAX_MD_SIZE];
  unsigned int size = 0;
  KJ_REQUIRE(HMAC(EVP_sha256(), key.data(), key.size(),
      reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), output, &size) != nullptr,
      "S3 signing failed");
  return {reinterpret_cast<const char*>(output), size};
}
std::string env(const char* name) {
  auto value = std::getenv(name);
  return value ? value : "";
}
std::string encode(const std::string& value) {
  constexpr char alphabet[] = "0123456789ABCDEF";
  std::string result;
  for (unsigned char c : value) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') result += c;
    else { result += '%'; result += alphabet[c >> 4]; result += alphabet[c & 15]; }
  }
  return result;
}
bool safeKey(const std::string& key) {
  return key.empty() || ((key.starts_with("bundles/") || key.starts_with("definitions/") ||
      key.starts_with("modules/") || key.starts_with("packages/")) &&
      key.find("..") == std::string::npos && std::all_of(key.begin(), key.end(), [](unsigned char c) {
        return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '/';
      }));
}
}  // namespace

std::string objectSha256(const std::string& bytes) {
  unsigned char digest[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), digest);
  return hex(digest, sizeof(digest));
}

ObjectStore::ObjectStore(ObjectStoreConfig config, kj::Network& network, kj::Timer& timer)
    : settings(std::move(config)), network(network), timer(timer) {
  kj::HttpHeaderTable::Builder builder;
  dateHeader = builder.add("x-amz-date");
  hashHeader = builder.add("x-amz-content-sha256");
  authorizationHeader = builder.add("authorization");
  table = builder.build();
  KJ_REQUIRE(settings.timeoutMs > 0, "object store timeout must be positive");
  if (settings.backend == "directory") {
    KJ_REQUIRE(!settings.directory.empty(), "object directory is required");
    return;
  }
  KJ_REQUIRE(settings.backend == "s3", "unknown object store backend");
  KJ_REQUIRE(settings.endpoint.starts_with("http://") &&
      settings.endpoint.find('/', 7) == std::string::npos &&
      settings.endpoint.find('@') == std::string::npos &&
      settings.endpoint.find('?') == std::string::npos, "S3 endpoint must be an HTTP origin");
  KJ_REQUIRE(!settings.bucket.empty() && std::all_of(settings.bucket.begin(), settings.bucket.end(),
      [](unsigned char c) { return std::isalnum(c) || c == '-' || c == '.'; }), "invalid S3 bucket");
  if (settings.accessKey.empty()) settings.accessKey = env("AWS_ACCESS_KEY_ID");
  if (settings.secretKey.empty()) settings.secretKey = env("AWS_SECRET_ACCESS_KEY");
  KJ_REQUIRE(!settings.accessKey.empty() && !settings.secretKey.empty(), "S3 credentials are required");
}

kj::Promise<ObjectResponse> ObjectStore::request(kj::HttpMethod method, std::string key,
    std::string bytes, bool sign) {
  KJ_REQUIRE(safeKey(key), "invalid object key");
  return timer.timeoutAfter(settings.timeoutMs * kj::MILLISECONDS,
      requestInner(method, std::move(key), std::move(bytes), sign)).catch_([](kj::Exception&& error) -> kj::Promise<ObjectResponse> {
    // Presigned URLs contain an access-key identifier. Never propagate a KJ
    // HTTP diagnostic containing that URL into logs or status endpoints.
    if (error.getType() == kj::Exception::Type::OVERLOADED) {
      return KJ_EXCEPTION(OVERLOADED, "object store request timeout");
    }
    return KJ_EXCEPTION(FAILED, "object store request failed");
  });
}

kj::Promise<ObjectResponse> ObjectStore::requestInner(kj::HttpMethod method, std::string key,
    std::string bytes, bool sign) {
  if (settings.backend == "directory") {
    const auto path = settings.directory / key;
    if (method == kj::HttpMethod::PUT) {
      std::filesystem::create_directories(path.parent_path());
      const auto temporary = path.string() + ".tmp";
      std::ofstream file(temporary, std::ios::binary | std::ios::trunc);
      file.write(bytes.data(), bytes.size());
      file.close();
      KJ_REQUIRE(file.good(), "object directory write failed");
      std::filesystem::rename(temporary, path);
      co_return ObjectResponse{200, {}};
    }
    if (!std::filesystem::is_regular_file(path)) co_return ObjectResponse{404, {}};
    if (method == kj::HttpMethod::HEAD) co_return ObjectResponse{200, {}};
    KJ_REQUIRE(method == kj::HttpMethod::GET, "unsupported directory operation");
    std::ifstream file(path, std::ios::binary);
    std::ostringstream output;
    output << file.rdbuf();
    KJ_REQUIRE(file.good() || file.eof(), "object directory read failed");
    co_return ObjectResponse{200, output.str()};
  }

  const std::string host = settings.endpoint.substr(7);
  const std::string path = '/' + settings.bucket + (key.empty() ? "" : '/' + key);
  const std::string payloadHash = objectSha256(bytes);
  kj::HttpHeaders headers(*table);
  headers.setPtr(kj::HttpHeaderId::HOST, host.c_str());
  headers.setPtr(kj::HttpHeaderId::CONNECTION, "close");
  std::string timestamp, authorization;
  std::string query;
  if (sign) {
    std::time_t now = std::time(nullptr);
    std::tm utc;
    gmtime_r(&now, &utc);
    char formatted[17];
    std::strftime(formatted, sizeof(formatted), "%Y%m%dT%H%M%SZ", &utc);
    timestamp = formatted;
    const auto day = timestamp.substr(0, 8);
    const auto scope = day + '/' + settings.region + "/s3/aws4_request";
    const std::string signedHeaders = "host;x-amz-content-sha256;x-amz-date";
    // SeaweedFS 3.97 incorrectly requires Write during header-signature
    // verification even for GET/HEAD. Its SigV4 query verifier checks Read.
    // Use standard presigning for reads; retain real payload signing for PUT.
    const bool read = method == kj::HttpMethod::GET || method == kj::HttpMethod::HEAD;
    if (read) query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=" +
        encode(settings.accessKey + '/' + scope) + "&X-Amz-Date=" + timestamp +
        "&X-Amz-Expires=60&X-Amz-SignedHeaders=" + encode(signedHeaders);
    const auto canonical = std::string(kj::str(method).cStr()) + '\n' + path + '\n' + query + '\n' +
        "host:" + host + "\nx-amz-content-sha256:" + payloadHash + "\nx-amz-date:" + timestamp +
        "\n\n" + signedHeaders + '\n' + payloadHash;
    const auto stringToSign = "AWS4-HMAC-SHA256\n" + timestamp + '\n' + scope + '\n' + objectSha256(canonical);
    auto signingKey = hmac(hmac(hmac(hmac("AWS4" + settings.secretKey, day), settings.region), "s3"), "aws4_request");
    auto signature = hmac(signingKey, stringToSign);
    authorization = "AWS4-HMAC-SHA256 Credential=" + settings.accessKey + '/' + scope +
        ", SignedHeaders=" + signedHeaders + ", Signature=" +
        hex(reinterpret_cast<const unsigned char*>(signature.data()), signature.size());
    headers.setPtr(dateHeader, timestamp.c_str());
    headers.setPtr(hashHeader, payloadHash.c_str());
    if (read) query += "&X-Amz-Signature=" + hex(reinterpret_cast<const unsigned char*>(signature.data()), signature.size());
    else headers.setPtr(authorizationHeader, authorization.c_str());
  }
  auto client = kj::newHttpClient(timer, *table, network, kj::none);
  const auto url = settings.endpoint + path + (query.empty() ? "" : '?' + query);
  auto call = client->request(method, url.c_str(), headers, bytes.size());
  if (!bytes.empty()) co_await call.body->write(kj::arrayPtr(bytes.data(), bytes.size()).asBytes());
  call.body = nullptr;
  auto response = co_await call.response;
  auto body = co_await response.body->readAllBytes(64 * 1024 * 1024);
  co_return ObjectResponse{response.statusCode,
      std::string(reinterpret_cast<const char*>(body.begin()), body.size())};
}

kj::Promise<bool> ObjectStore::exists(std::string key) {
  auto response = co_await request(kj::HttpMethod::HEAD, std::move(key));
  KJ_REQUIRE(response.status == 200 || response.status == 404, "object HEAD failed", response.status);
  co_return response.status == 200;
}
kj::Promise<void> ObjectStore::put(std::string key, std::string bytes) {
  auto response = co_await request(kj::HttpMethod::PUT, std::move(key), std::move(bytes));
  KJ_REQUIRE(response.status == 200, "object PUT failed", response.status);
}
kj::Promise<ObjectResponse> ObjectStore::get(std::string key) {
  return request(kj::HttpMethod::GET, std::move(key));
}
}  // namespace coworkerd
