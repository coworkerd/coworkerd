#include "object-store.h"

#include <kj/debug.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <algorithm>
#include <array>
#include <cstdio>
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
bool contentKey(const std::string& key) {
  return (key.starts_with("bundles/") || key.starts_with("definitions/") ||
      key.starts_with("modules/") || key.starts_with("packages/")) &&
      key.find("..") == std::string::npos && std::all_of(key.begin(), key.end(), [](unsigned char c) {
        return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '/';
      });
}
bool stateSegment(std::string_view segment) {
  return !segment.empty() && segment != "." && segment != ".." &&
      std::all_of(segment.begin(), segment.end(), [](unsigned char c) {
        return std::isalnum(c) || c == '.' || c == '_' || c == '-';
      });
}
// One phase of a request bounded in time: its timeout counted and named (connect, write, answer, read).
template <typename T>
kj::Promise<T> within(kj::Timer& timer, uint32_t ms, kj::Promise<T> promise, uint64_t& timeouts, const char* phase) {
  return timer.timeoutAfter(ms * kj::MILLISECONDS, kj::mv(promise))
      .catch_([&timeouts, phase](kj::Exception&& error) -> kj::Promise<T> {
        if (error.getType() != kj::Exception::Type::OVERLOADED) return kj::mv(error);
        ++timeouts;
        return kj::Exception(kj::Exception::Type::OVERLOADED, __FILE__, __LINE__,
            kj::str("object store request timeout (", phase, ")"));
      });
}
// A key of the cluster's state; a listing's prefix may end with '/' or in the middle of a segment.
bool stateKey(const std::string& key, bool prefix) {
  if (!key.starts_with("members/") && !key.starts_with("check/") && !key.starts_with("cluster/") && !key.starts_with("tenants/") &&
      !key.starts_with("nodes/"))
    return false;
  const std::string_view rest(key);
  size_t start = 0;
  while (true) {
    const auto end = rest.find('/', start);
    const auto segment = rest.substr(start, end == std::string_view::npos ? std::string_view::npos : end - start);
    if (end == std::string_view::npos) {
      if (segment.empty()) return prefix;
      return prefix ? std::all_of(segment.begin(), segment.end(), [](unsigned char c) {
        return std::isalnum(c) || c == '.' || c == '_' || c == '-';
      }) : stateSegment(segment);
    }
    if (!stateSegment(segment)) return false;
    start = end + 1;
  }
}
bool safeKey(const std::string& key) {
  return key.empty() || contentKey(key) || stateKey(key, false);
}

// The text of the first <name>...</name> in `xml`, its entities replaced; "" when there is none.
std::string unescapeXml(std::string_view text) {
  std::string result;
  for (size_t i = 0; i < text.size(); ++i) {
    if (text[i] != '&') { result += text[i]; continue; }
    const auto end = text.find(';', i);
    if (end == std::string_view::npos || end - i > 10) { result += text[i]; continue; }
    const auto entity = text.substr(i + 1, end - i - 1);
    uint32_t code = 0;
    if (entity == "amp") code = '&';
    else if (entity == "lt") code = '<';
    else if (entity == "gt") code = '>';
    else if (entity == "quot") code = '"';
    else if (entity == "apos") code = '\'';
    else if (entity.size() > 1 && entity[0] == '#') {
      const bool hexadecimal = entity[1] == 'x' || entity[1] == 'X';
      const std::string digits(entity.substr(hexadecimal ? 2 : 1));
      char* stop = nullptr;
      const auto parsed = std::strtoul(digits.c_str(), &stop, hexadecimal ? 16 : 10);
      if (!digits.empty() && stop && *stop == 0 && parsed > 0 && parsed < 0x110000) code = static_cast<uint32_t>(parsed);
    }
    if (code == 0) { result += text[i]; continue; }
    if (code < 0x80) result += static_cast<char>(code);
    else if (code < 0x800) { result += static_cast<char>(0xc0 | (code >> 6)); result += static_cast<char>(0x80 | (code & 0x3f)); }
    else if (code < 0x10000) {
      result += static_cast<char>(0xe0 | (code >> 12)); result += static_cast<char>(0x80 | ((code >> 6) & 0x3f));
      result += static_cast<char>(0x80 | (code & 0x3f));
    } else {
      result += static_cast<char>(0xf0 | (code >> 18)); result += static_cast<char>(0x80 | ((code >> 12) & 0x3f));
      result += static_cast<char>(0x80 | ((code >> 6) & 0x3f)); result += static_cast<char>(0x80 | (code & 0x3f));
    }
    i = end;
  }
  return result;
}
std::string element(std::string_view xml, std::string_view name) {
  const auto open = "<" + std::string(name) + ">", close = "</" + std::string(name) + ">";
  const auto start = xml.find(open);
  if (start == std::string_view::npos) return {};
  const auto end = xml.find(close, start + open.size());
  if (end == std::string_view::npos) return {};
  return unescapeXml(xml.substr(start + open.size(), end - start - open.size()));
}
// An error answer's code (<Error><Code>NoSuchBucket</Code>...): what a diagnostic may name, never the rest of it
// (a signature mismatch echoes the canonical request, with the access key's id).
std::string errorCode(const std::string& body) {
  auto code = element(body, "Code");
  if (code.size() > 64 || !std::all_of(code.begin(), code.end(), [](unsigned char c) { return std::isalnum(c); })) return {};
  return code;
}
}  // namespace

std::string objectErrorCode(const std::string& body) { return errorCode(body); }

std::string objectSha256(const std::string& bytes) {
  unsigned char digest[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), digest);
  return hex(digest, sizeof(digest));
}

uint64_t httpDateUnixMs(std::string_view text) {
  if (text.size() < 29) return 0;
  const std::string date(text.substr(0, 29));
  char weekday[4] = {}, month[4] = {}, zone[4] = {};
  int day = 0, year = 0, hour = 0, minute = 0, second = 0;
  if (std::sscanf(date.c_str(), "%3c, %2d %3c %4d %2d:%2d:%2d %3c", weekday, &day, month, &year, &hour, &minute,
      &second, zone) != 8 || std::string_view(zone) != "GMT") return 0;
  constexpr std::string_view MONTHS = "JanFebMarAprMayJunJulAugSepOctNovDec";
  const auto index = MONTHS.find(month);
  if (index == std::string_view::npos || index % 3 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 60 ||
      year < 1970) return 0;
  std::tm parts{};
  parts.tm_year = year - 1900;
  parts.tm_mon = static_cast<int>(index / 3);
  parts.tm_mday = day;
  parts.tm_hour = hour;
  parts.tm_min = minute;
  parts.tm_sec = second;
  const auto seconds = timegm(&parts);
  return seconds <= 0 ? 0 : static_cast<uint64_t>(seconds) * 1000;
}

uint64_t isoTimeUnixMs(std::string_view text) {
  const std::string value(text);
  int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0, consumed = 0;
  if (std::sscanf(value.c_str(), "%4d-%2d-%2dT%2d:%2d:%2d%n", &year, &month, &day, &hour, &minute, &second,
      &consumed) != 6 || year < 1970 || month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 ||
      second > 60) return 0;
  size_t at = static_cast<size_t>(consumed);
  uint64_t millis = 0;
  if (at < value.size() && value[at] == '.') {
    uint64_t scale = 100;
    for (++at; at < value.size() && std::isdigit(static_cast<unsigned char>(value[at])); ++at) {
      millis += (value[at] - '0') * scale;
      scale /= 10;
    }
  }
  if (at + 1 != value.size() || value[at] != 'Z') return 0;
  std::tm parts{};
  parts.tm_year = year - 1900;
  parts.tm_mon = month - 1;
  parts.tm_mday = day;
  parts.tm_hour = hour;
  parts.tm_min = minute;
  parts.tm_sec = second;
  const auto seconds = timegm(&parts);
  return seconds <= 0 ? 0 : static_cast<uint64_t>(seconds) * 1000 + millis;
}

ObjectStore::ObjectStore(ObjectStoreConfig config, kj::Network& network, kj::Timer& timer)
    : settings(std::move(config)), network(network), timer(timer) {
  kj::HttpHeaderTable::Builder builder;
  dateHeader = builder.add("x-amz-date");
  hashHeader = builder.add("x-amz-content-sha256");
  authorizationHeader = builder.add("authorization");
  ifNoneMatchHeader = builder.add("If-None-Match");
  table = builder.build();
  KJ_REQUIRE(settings.timeoutMs > 0 && settings.connectTimeoutMs > 0 && settings.readTimeoutMs > 0 && settings.createTimeoutMs > 0,
      "object store timeouts must be positive");
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
  return send({method, std::move(key), std::move(bytes), sign});
}

std::string ObjectStoreStats::json() const {
  return "{\"requests\":" + std::to_string(requests) + ",\"retries\":" + std::to_string(retries) + ",\"connectTimeouts\":" +
      std::to_string(connectTimeouts) + ",\"readTimeouts\":" + std::to_string(readTimeouts) + ",\"failures\":" +
      std::to_string(failures) + ",\"createsReadBack\":" + std::to_string(createsReadBack) + ",\"createsConfirmed\":" +
      std::to_string(createsConfirmed) + ",\"createsFoundOther\":" + std::to_string(createsFoundOther) +
      ",\"createsUnknown\":" + std::to_string(createsUnknown) + "}";
}

kj::Exception ObjectStore::sanitized(const kj::Exception& error) {
  // Presigned URLs contain an access-key identifier. Never propagate a KJ HTTP diagnostic containing that URL into logs
  // or status endpoints: only this client's own words.
  if (error.getType() == kj::Exception::Type::OVERLOADED) {
    const kj::StringPtr description = error.getDescription();
    return kj::Exception(kj::Exception::Type::OVERLOADED, __FILE__, __LINE__, description.startsWith("object store request timeout (")
        ? kj::str(description) : kj::str("object store request timeout"));
  }
  return kj::Exception(kj::Exception::Type::FAILED, __FILE__, __LINE__, kj::str("object store request failed"));
}

kj::Promise<ObjectResponse> ObjectStore::send(Call call) {
  KJ_REQUIRE(safeKey(call.key), "invalid object key");
  if (settings.backend == "directory") co_return co_await exchange(call);
  // GET, HEAD, a listing and DELETE may be sent again; an overwrite may not (a create decides for itself: create()).
  const bool idempotent = call.method != kj::HttpMethod::PUT && call.method != kj::HttpMethod::POST;
  for (uint32_t attempt = 1;; ++attempt) {
    ++statistics.requests;
    kj::Maybe<kj::Exception> failure;
    try {
      co_return co_await exchange(call);
    } catch (...) {
      failure = kj::getCaughtExceptionAsKj();
    }
    KJ_IF_SOME(error, failure) {
      if (error.getType() != kj::Exception::Type::OVERLOADED) ++statistics.failures;
      if (!idempotent || attempt >= 2) kj::throwFatalException(sanitized(error));
      ++statistics.retries;
    }
  }
}

kj::Promise<ObjectResponse> ObjectStore::exchange(const Call& call) {
  const auto method = call.method;
  const auto& key = call.key;
  const auto& bytes = call.bytes;
  if (settings.backend == "directory") {
    KJ_REQUIRE(call.query.empty() && !call.ifNoneMatch, "the object directory backend keeps no cluster state");
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
  if (call.ifNoneMatch) headers.setPtr(ifNoneMatchHeader, "*");
  std::string timestamp, authorization;
  // The query in canonical order (names and values encoded, sorted by name): the signature covers it.
  std::vector<std::pair<std::string, std::string>> parameters;
  for (const auto& [name, value] : call.query) parameters.emplace_back(encode(name), encode(value));
  // SeaweedFS 3.97 incorrectly requires Write during header-signature
  // verification even for GET/HEAD. Its SigV4 query verifier checks Read.
  // Reads are presigned the standard way (only host signed, UNSIGNED-PAYLOAD): SeaweedFS 4.48 refuses a presigned
  // read that also signs x-amz-content-sha256 and x-amz-date (SignatureDoesNotMatch), which 3.97 accepted. Writes
  // keep the real payload signature in the Authorization header.
  const bool read = method == kj::HttpMethod::GET || method == kj::HttpMethod::HEAD;
  std::string query, signature;
  auto joined = [&]() {
    std::sort(parameters.begin(), parameters.end());
    std::string result;
    for (const auto& [name, value] : parameters) result += (result.empty() ? "" : "&") + name + '=' + value;
    return result;
  };
  if (call.sign) {
    std::time_t now = std::time(nullptr);
    std::tm utc;
    gmtime_r(&now, &utc);
    char formatted[17];
    std::strftime(formatted, sizeof(formatted), "%Y%m%dT%H%M%SZ", &utc);
    timestamp = formatted;
    const auto day = timestamp.substr(0, 8);
    const auto scope = day + '/' + settings.region + "/s3/aws4_request";
    const std::string signedHeaders = read ? "host" : "host;x-amz-content-sha256;x-amz-date";
    if (read) {
      parameters.emplace_back("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
      parameters.emplace_back("X-Amz-Credential", encode(settings.accessKey + '/' + scope));
      parameters.emplace_back("X-Amz-Date", timestamp);
      parameters.emplace_back("X-Amz-Expires", "60");
      parameters.emplace_back("X-Amz-SignedHeaders", encode(signedHeaders));
    }
    query = joined();
    const auto canonical = read
        ? std::string(kj::str(method).cStr()) + '\n' + path + '\n' + query + "\nhost:" + host + "\n\nhost\nUNSIGNED-PAYLOAD"
        : std::string(kj::str(method).cStr()) + '\n' + path + '\n' + query + '\n' +
          "host:" + host + "\nx-amz-content-sha256:" + payloadHash + "\nx-amz-date:" + timestamp +
          "\n\n" + signedHeaders + '\n' + payloadHash;
    const auto stringToSign = "AWS4-HMAC-SHA256\n" + timestamp + '\n' + scope + '\n' + objectSha256(canonical);
    auto signingKey = hmac(hmac(hmac(hmac("AWS4" + settings.secretKey, day), settings.region), "s3"), "aws4_request");
    auto digest = hmac(signingKey, stringToSign);
    signature = hex(reinterpret_cast<const unsigned char*>(digest.data()), digest.size());
    if (read) {
      query += "&X-Amz-Signature=" + signature;
    } else {
      authorization = "AWS4-HMAC-SHA256 Credential=" + settings.accessKey + '/' + scope +
          ", SignedHeaders=" + signedHeaders + ", Signature=" + signature;
      headers.setPtr(dateHeader, timestamp.c_str());
      headers.setPtr(hashHeader, payloadHash.c_str());
      headers.setPtr(authorizationHeader, authorization.c_str());
    }
  } else {
    query = joined();
  }
  // A connection of its own (the request says Connection: close), established within connectTimeoutMs; then every write
  // of the request's body and every read of the answer's within readTimeoutMs of the one before, and the answer itself
  // within readTimeoutMs -- a create's within createTimeoutMs.
  auto connection = co_await within(timer, settings.connectTimeoutMs, [](kj::Network& network, std::string host)
      -> kj::Promise<kj::Own<kj::AsyncIoStream>> {
    auto address = co_await network.parseAddress(host.c_str(), 80);
    co_return co_await address->connect();
  }(network, host), statistics.connectTimeouts, "connect");
  auto client = kj::newHttpClient(*table, *connection);
  const auto target = path + (query.empty() ? "" : '?' + query);
  auto request = client->request(method, target.c_str(), headers, bytes.size());
  constexpr size_t CHUNK = 64 * 1024;
  for (size_t at = 0; at < bytes.size(); at += CHUNK) {
    co_await within(timer, settings.readTimeoutMs,
        request.body->write(kj::arrayPtr(bytes.data() + at, std::min(CHUNK, bytes.size() - at)).asBytes()),
        statistics.readTimeouts, "write");
  }
  request.body = nullptr;
  auto response = co_await within(timer, call.ifNoneMatch ? settings.createTimeoutMs : settings.readTimeoutMs,
      kj::mv(request.response), statistics.readTimeouts, "answer");
  uint64_t date = 0;
  KJ_IF_SOME(value, response.headers->get(kj::HttpHeaderId::DATE)) {
    date = httpDateUnixMs(std::string_view(value.begin(), value.size()));
  }
  std::string body;
  std::string buffer(CHUNK, '\0');
  while (true) {
    const size_t read = co_await within(timer, settings.readTimeoutMs, response.body->tryRead(buffer.data(), 1, buffer.size()),
        statistics.readTimeouts, "read");
    if (read == 0) break;
    body.append(buffer.data(), read);
    KJ_REQUIRE(body.size() <= 64 * 1024 * 1024, "object store answer larger than 64 MiB");
  }
  co_return ObjectResponse{response.statusCode, std::move(body), date};
}

kj::Promise<bool> ObjectStore::exists(std::string key) {
  auto response = co_await request(kj::HttpMethod::HEAD, std::move(key));
  KJ_REQUIRE(response.status == 200 || response.status == 404, "object HEAD failed", response.status);
  co_return response.status == 200;
}
kj::Promise<ObjectResponse> ObjectStore::put(std::string key, std::string bytes) {
  auto response = co_await request(kj::HttpMethod::PUT, std::move(key), std::move(bytes));
  KJ_REQUIRE(response.status == 200, "object PUT failed", response.status);
  co_return response;
}
kj::Promise<ObjectResponse> ObjectStore::get(std::string key) {
  return request(kj::HttpMethod::GET, std::move(key));
}

kj::Promise<CreateResult> ObjectStore::create(std::string key, std::string bytes) {
  KJ_REQUIRE(settings.backend == "s3", "the object directory backend keeps no cluster state");
  KJ_REQUIRE(!key.empty(), "create needs a key");
  KJ_REQUIRE(safeKey(key), "invalid object key");
  const Call call{kj::HttpMethod::PUT, key, bytes, true, {}, true};
  for (uint32_t attempt = 1;; ++attempt) {
    ++statistics.requests;
    kj::Maybe<kj::Exception> failure;
    ObjectResponse response{};
    try {
      response = co_await exchange(call);
    } catch (...) {
      failure = kj::getCaughtExceptionAsKj();
    }
    if (failure == kj::none) {
      if (response.status == 200) co_return CreateResult{true, response.status, response.dateUnixMs, attempt > 1};
      KJ_REQUIRE(response.status == 412 || response.status == 409, "object create failed", response.status,
          errorCode(response.bytes).c_str());
      // The first create was answered: the key was another writer's.
      if (attempt == 1) co_return CreateResult{false, response.status, response.dateUnixMs};
    } else {
      KJ_IF_SOME(error, failure) { if (error.getType() != kj::Exception::Type::OVERLOADED) ++statistics.failures; }
    }
    // No answer came back (or this is the second create, after one whose answer was lost): the key read back decides.
    ++statistics.createsReadBack;
    auto back = co_await get(key);
    if (back.status == 200) {
      const bool own = back.bytes == bytes;
      ++(own ? statistics.createsConfirmed : statistics.createsFoundOther);
      co_return CreateResult{own, own ? 200u : 412u, back.dateUnixMs, true};
    }
    KJ_REQUIRE(back.status == 404, "object store read failed", back.status);
    if (attempt >= 2) {
      ++statistics.createsUnknown;
      KJ_IF_SOME(error, failure) { kj::throwFatalException(sanitized(error)); }
      KJ_FAIL_REQUIRE("object create failed: the key is absent after a conflicting answer", response.status);
    }
    // Absent: the first create did not take effect. Once more, on a new connection.
    ++statistics.retries;
  }
}

kj::Promise<ObjectListing> ObjectStore::list(std::string prefix, uint32_t maxKeys) {
  KJ_REQUIRE(settings.backend == "s3", "the object directory backend keeps no cluster state");
  KJ_REQUIRE(stateKey(prefix, true), "invalid object prefix");
  KJ_REQUIRE(maxKeys >= 1 && maxKeys <= 1000, "a listing page holds 1 to 1000 keys");
  ObjectListing listing;
  std::string token;
  while (true) {
    Call call{kj::HttpMethod::GET, "", "", true,
        {{"list-type", "2"}, {"max-keys", std::to_string(maxKeys)}, {"prefix", prefix}}};
    if (!token.empty()) call.query.emplace_back("continuation-token", token);
    auto response = co_await send(std::move(call));
    KJ_REQUIRE(response.status == 200, "object listing failed", response.status, errorCode(response.bytes).c_str());
    const std::string_view xml(response.bytes);
    KJ_REQUIRE(xml.find("<ListBucketResult") != std::string_view::npos, "object listing answer is not a listing");
    if (++listing.pages == 1) listing.dateUnixMs = response.dateUnixMs;
    for (size_t at = xml.find("<Contents>"); at != std::string_view::npos; at = xml.find("<Contents>", at)) {
      const auto end = xml.find("</Contents>", at);
      KJ_REQUIRE(end != std::string_view::npos, "object listing answer is cut short");
      const auto block = xml.substr(at, end - at);
      ListedObject object;
      object.key = element(block, "Key");
      object.lastModifiedUnixMs = isoTimeUnixMs(element(block, "LastModified"));
      object.etag = element(block, "ETag");
      if (object.etag.size() >= 2 && object.etag.front() == '"' && object.etag.back() == '"')
        object.etag = object.etag.substr(1, object.etag.size() - 2);
      const auto size = element(block, "Size");
      object.size = size.empty() ? 0 : std::strtoull(size.c_str(), nullptr, 10);
      object.pageDateUnixMs = response.dateUnixMs;
      KJ_REQUIRE(object.key.starts_with(prefix) && object.lastModifiedUnixMs > 0,
          "object listing entry without a key under the prefix or a LastModified");
      listing.objects.push_back(std::move(object));
      at = end;
    }
    if (element(xml, "IsTruncated") != "true") break;
    token = element(xml, "NextContinuationToken");
    KJ_REQUIRE(!token.empty(), "object listing is truncated without a continuation token");
    KJ_REQUIRE(listing.pages < 100000, "object listing does not end");
  }
  co_return listing;
}

kj::Promise<ObjectResponse> ObjectStore::remove(std::string key) {
  KJ_REQUIRE(settings.backend == "s3", "the object directory backend keeps no cluster state");
  KJ_REQUIRE(!key.empty(), "remove needs a key");
  auto response = co_await request(kj::HttpMethod::DELETE, std::move(key));
  KJ_REQUIRE(response.status == 204 || response.status == 200 || response.status == 404, "object DELETE failed",
      response.status, errorCode(response.bytes).c_str());
  co_return response;
}
}  // namespace coworkerd
