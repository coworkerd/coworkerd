#include "support.h"

#include <kj/debug.h>
#include <openssl/sha.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <fstream>
#include <sstream>
#include <strings.h>

namespace fs = std::filesystem;

namespace coworkerd {

int64_t clockOffsetMsForTest = 0;
const char* executableArgument = "";

std::string readFile(const fs::path& path) {
  std::ifstream input(path, std::ios::binary);
  KJ_REQUIRE(input.good(), "cannot open file", path.string().c_str());
  std::ostringstream output;
  output << input.rdbuf();
  KJ_REQUIRE(input.good() || input.eof(), "cannot read file", path.string().c_str());
  return output.str();
}

bool safeName(const std::string& value) {
  return !value.empty() && std::all_of(value.begin(), value.end(), [](unsigned char ch) {
    return std::isalnum(ch) || ch == '-' || ch == '_' || ch == '.';
  });
}

std::string sha256Hex(std::string_view value) {
  unsigned char digest[SHA256_DIGEST_LENGTH];
  KJ_REQUIRE(
      SHA256(reinterpret_cast<const unsigned char*>(value.data()), value.size(), digest) == digest,
      "SHA-256 failed");
  constexpr char HEX[] = "0123456789abcdef";
  std::string result(SHA256_DIGEST_LENGTH * 2, '0');
  for (size_t index = 0; index < SHA256_DIGEST_LENGTH; ++index) {
    result[index * 2] = HEX[digest[index] >> 4];
    result[index * 2 + 1] = HEX[digest[index] & 0x0f];
  }
  return result;
}

// `Authorization: Bearer <token>` as the management endpoints and the control plane take
// it: "" when it is the token, else what was wrong -- "missing", "malformed" (repeated, or not Bearer)
// or "wrong". Compared in time independent of where the bytes differ; the token is never echoed.
std::string bearerRefusal(const std::string& token, const kj::HttpHeaders& headers) {
  std::optional<std::string> authorization;
  bool repeated = false;
  headers.forEach([&](kj::StringPtr name, kj::StringPtr value) {
    if (name.size() != 13 || strncasecmp(name.cStr(), "authorization", 13) != 0) return;
    if (authorization) repeated = true; else authorization = std::string(value.cStr(), value.size());
  });
  if (!authorization) return "missing";
  std::string_view presented(*authorization);
  if (repeated || !presented.starts_with("Bearer ")) return "malformed";
  presented.remove_prefix(7);
  unsigned char difference = presented.size() == token.size() ? 0 : 1;
  for (size_t i = 0; i < token.size(); ++i) difference |= static_cast<unsigned char>(token[i] ^ (i < presented.size() ? presented[i] : 0));
  return difference == 0 ? std::string() : "wrong";
}

std::string jsonEscape(const std::string& value) {
  std::string result;
  result.reserve(value.size() + 8);
  for (unsigned char ch : value) {
    switch (ch) {
      case '"': result += "\\\""; break;
      case '\\': result += "\\\\"; break;
      case '\b': result += "\\b"; break;
      case '\f': result += "\\f"; break;
      case '\n': result += "\\n"; break;
      case '\r': result += "\\r"; break;
      case '\t': result += "\\t"; break;
      default:
        if (ch < 0x20) {
          const char* hex = "0123456789abcdef";
          result += "\\u00";
          result += hex[ch >> 4];
          result += hex[ch & 0x0f];
        } else {
          result += static_cast<char>(ch);
        }
    }
  }
  return result;
}

// A management endpoint without its token: 401, the same answer on every listener.
kj::Promise<void> sendUnauthorized(kj::HttpService::Response& response, const kj::HttpHeaderTable& table) {
  const std::string body = "{\"code\":\"UNAUTHORIZED\",\"reason\":\"this management endpoint requires "
      "Authorization: Bearer <management token>\"}";
  kj::HttpHeaders headers(table);
  headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
  headers.addPtrPtr("WWW-Authenticate", "Bearer realm=\"coworkerd\"");
  auto stream = response.send(401, "Unauthorized", headers, body.size());
  co_await stream->write(kj::arrayPtr(body.data(), body.size()).asBytes());
}

uint64_t wallTimeMsForTest() {
  auto value = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  value += clockOffsetMsForTest;
  KJ_REQUIRE(value >= 0, "system clock precedes Unix epoch");
  return static_cast<uint64_t>(value);
}

double monotonicTimeMs() {
  const auto value = std::chrono::duration<double, std::milli>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
  KJ_REQUIRE(std::isfinite(value) && value >= 0, "monotonic clock returned an invalid value");
  return value;
}

// The directory of this executable (the release keeps workerd beside it, the runtime in ../runtime).
fs::path executableDirectory() {
  if (fs::exists("/proc/self/exe")) return fs::read_symlink("/proc/self/exe").parent_path();
  std::error_code error;
  auto path = fs::canonical(executableArgument, error);
  return error ? fs::current_path() : path.parent_path();
}

// The SHA-256 of the launcher beside this executable, "none" when there is none (macOS, or a
// build without it). Read once.
const std::string& launcherSha256() {
  static const std::string value = [] {
    std::error_code error;
    const auto path = executableDirectory() / "coworkerd-launcher";
    if (!fs::is_regular_file(path, error)) return std::string("none");
    try { return sha256Hex(readFile(path)); } catch (...) { return std::string("unreadable"); }
  }();
  return value;
}

// What `--version` prints and the state endpoint shows: this release, the source it was built
// from, and the workerd it expects -- the upstream version and the hash of the patch series (the
// concatenated patch files in the order of patches/workerd/series); also the launcher's hash.
std::string versionJson() {
  return std::string("{\"server\":\"") + jsonEscape(COWORKERD_VERSION) + "\",\"source\":\"" + jsonEscape(COWORKERD_SOURCE) +
      "\",\"workerdUpstream\":\"" + jsonEscape(COWORKERD_WORKERD_UPSTREAM) + "\",\"patchSeries\":\"" +
      jsonEscape(COWORKERD_PATCH_SERIES) + "\",\"patchSeriesSha256\":\"" + jsonEscape(COWORKERD_PATCH_SERIES_SHA256) +
      "\",\"launcherSha256\":\"" + jsonEscape(launcherSha256()) + "\"}";
}
std::string versionText(const char* name) {
  return std::string(name) + " " + COWORKERD_VERSION + "\nsource " + COWORKERD_SOURCE + "\nworkerd " +
      COWORKERD_WORKERD_UPSTREAM + " + patches " + COWORKERD_PATCH_SERIES + "\npatch series sha256 " +
      COWORKERD_PATCH_SERIES_SHA256 + "\nlauncher sha256 " + launcherSha256() + "\n";
}

std::vector<std::string> rendezvousOrder(const std::string& worker, const std::vector<std::string>& ids) {
  std::vector<std::pair<uint64_t, std::string>> scores;
  for (const auto& id : ids) {
    auto bytes = worker + std::string(1, '\0') + id;
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), digest);
    uint64_t score = 0;
    for (size_t i = 0; i < 8; ++i) score = (score << 8) | digest[i];
    scores.emplace_back(score, id);
  }
  std::sort(scores.begin(), scores.end(), [](const auto& a, const auto& b) {
    return a.first != b.first ? a.first > b.first : a.second < b.second;
  });
  std::vector<std::string> result;
  for (const auto& score : scores) result.push_back(score.second);
  return result;
}

}  // namespace coworkerd
