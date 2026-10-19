#include "code-package.h"
#include "object-store.h"
#include <capnp/message.h>
#include <kj/debug.h>
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <set>
#include <sstream>

namespace coworkerd {
using namespace etcd;
namespace {
std::string read(const std::filesystem::path& path) {
  std::ifstream file(path, std::ios::binary);
  KJ_REQUIRE(file.good(), "cannot read package file", path.string().c_str());
  std::ostringstream out; out << file.rdbuf();
  KJ_REQUIRE(file.good() || file.eof(), "package read failed");
  return out.str();
}
bool validHash(const std::string& s) {
  return s.size() == 64 && std::all_of(s.begin(), s.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  });
}
bool validContentType(const std::string& value) {
  return !value.empty() && value.size() <= 256 && value.front() != ' ' && value.back() != ' ' &&
      std::all_of(value.begin(), value.end(), [](unsigned char c) { return c >= 0x20 && c < 0x7f; });
}
uint64_t byteCount(Json::Reader value, uint64_t limit, const char* what) {
  KJ_REQUIRE(value.isNumber() && value.getNumber() >= 0 && value.getNumber() <= static_cast<double>(limit) &&
      std::floor(value.getNumber()) == value.getNumber(), what);
  return static_cast<uint64_t>(value.getNumber());
}
uint64_t environment(const char* name, uint64_t fallback) {
  auto value = std::getenv(name);
  if (!value || !*value) return fallback;
  char* end = nullptr;
  auto parsed = std::strtoull(value, &end, 10);
  KJ_REQUIRE(*end == '\0' && parsed > 0, "invalid asset limit", name);
  return parsed;
}
}  // namespace

AssetLimits AssetLimits::fromEnvironment() {
  AssetLimits limits;
  limits.maxCount = environment("COWORKERD_ASSET_MAX_COUNT", limits.maxCount);
  limits.maxFileBytes = environment("COWORKERD_ASSET_MAX_FILE_BYTES", limits.maxFileBytes);
  limits.maxTotalBytes = environment("COWORKERD_ASSET_MAX_TOTAL_BYTES", limits.maxTotalBytes);
  return limits;
}

bool moduleSafeName(const std::string& name) {
  if (name.empty() || name.front() == '/' || name.starts_with("__coworkerd_") ||
      name.find('\\') != std::string::npos || name.find(':') != std::string::npos) return false;
  for (unsigned char c : name) if (c < 32 || c == 127) return false;
  std::istringstream in(name); std::string part;
  while (std::getline(in, part, '/')) if (part.empty() || part == "." || part == "..") return false;
  return name.back() != '/';
}
bool assetSafePath(const std::string& path) {
  if (path.size() < 2 || path.size() > 4096 || path.front() != '/' || path.back() == '/') return false;
  for (unsigned char c : path) if (c < 32 || c == 127 || c == '\\') return false;
  size_t start = 1;
  while (start <= path.size()) {
    auto end = path.find('/', start);
    if (end == std::string::npos) end = path.size();
    auto segment = path.substr(start, end - start);
    if (segment.empty() || segment == "." || segment == "..") return false;
    start = end + 1;
  }
  return true;
}
std::string canonicalJson(Json::Reader value) {
  if (value.isObject()) {
    std::map<std::string, std::string> members;
    for (auto member : value.getObject()) {
      KJ_REQUIRE(members.emplace(member.getName().cStr(), canonicalJson(member.getValue())).second,
          "duplicate JSON field");
    }
    std::string out = "{";
    for (const auto& [name, encoded] : members) {
      if (out.size() > 1) out += ',';
      out += quote(name) + ':' + encoded;
    }
    return out + '}';
  }
  if (value.isArray()) {
    std::string out = "[";
    for (auto member : value.getArray()) { if (out.size() > 1) out += ','; out += canonicalJson(member); }
    return out + ']';
  }
  return capnp::JsonCodec().encodeRaw(value).cStr();
}
std::string canonicalJson(const std::string& bytes) {
  capnp::MallocMessageBuilder message; auto value = message.initRoot<Json>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), value);
  return canonicalJson(value.asReader());
}
std::string packageJson(CodePackage modules, CodeAssets assets) {
  std::sort(modules.begin(), modules.end(), [](const auto& a, const auto& b) { return a.name < b.name; });
  std::sort(assets.begin(), assets.end(), [](const auto& a, const auto& b) { return a.path < b.path; });
  std::string out = "{";
  if (!assets.empty()) {
    out += "\"assets\":[";
    bool first = true;
    for (const auto& a : assets) {
      if (!first) out += ','; first = false;
      out += "{\"bytes\":" + std::to_string(a.bytes) + ",\"contentType\":" + quote(a.contentType) +
          ",\"path\":" + quote(a.path) + ",\"sha256\":" + quote(a.sha256) + '}';
    }
    out += "],";
  }
  out += "\"modules\":["; bool first = true;
  for (const auto& m : modules) {
    if (!first) out += ','; first = false;
    out += "{\"bytes\":" + std::to_string(m.bytes) + ",\"name\":" + quote(m.name) +
        ",\"sha256\":" + quote(m.sha256) + ",\"type\":" + quote(m.type) + '}';
  }
  return out + "]}";
}
std::string packageJson(const PackageContents& contents) { return packageJson(contents.modules, contents.assets); }
PackageContents readPackage(const std::string& manifest, const AssetLimits& limits) {
  capnp::MallocMessageBuilder message; auto value = message.initRoot<Json>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(manifest.data(), manifest.size()), value);
  KJ_REQUIRE(value.isObject(), "package must be a JSON object");
  for (auto member : value.getObject()) {
    KJ_REQUIRE(member.getName() == "modules" || member.getName() == "assets", "package must contain only modules and assets");
  }
  auto modules = field(value, "modules"), assets = field(value, "assets");
  const bool hasAssets = !assets.isNull();
  KJ_REQUIRE(!hasAssets || (assets.isArray() && assets.getArray().size() > 0), "package assets must be a non-empty list when present");
  KJ_REQUIRE(modules.isArray() && (modules.getArray().size() > 0 || hasAssets), "package modules required");
  PackageContents result; std::set<std::string> names; uint64_t total = 0;
  for (auto m : modules.getArray()) {
    KJ_REQUIRE(m.isObject() && m.getObject().size() == 4, "invalid package module fields");
    CodeModule module{text(m, "name"), text(m, "type"), text(m, "sha256"), 0};
    module.bytes = byteCount(field(m, "bytes"), 10 * 1024 * 1024, "invalid module byte count");
    total += module.bytes;
    KJ_REQUIRE(total <= 10 * 1024 * 1024, "package exceeds 10 MiB uncompressed");
    KJ_REQUIRE(moduleSafeName(module.name), "unsafe or reserved module name", module.name.c_str());
    KJ_REQUIRE(names.insert(module.name).second, "duplicate module name");
    KJ_REQUIRE(validHash(module.sha256), "invalid module SHA-256");
    KJ_REQUIRE((std::set<std::string>{"esm", "cjs", "text", "data", "json", "wasm"}.contains(module.type)),
        "unsupported module type", module.type.c_str());
    result.modules.push_back(std::move(module));
  }
  if (!hasAssets) return result;
  KJ_REQUIRE(assets.getArray().size() <= limits.maxCount, "package has more static assets than allowed",
      assets.getArray().size(), limits.maxCount);
  std::set<std::string> paths;
  for (auto a : assets.getArray()) {
    KJ_REQUIRE(a.isObject() && a.getObject().size() == 4, "invalid package asset fields");
    CodeAsset asset{text(a, "path"), text(a, "sha256"), text(a, "contentType"), 0};
    auto size = field(a, "bytes");
    KJ_REQUIRE(size.isNumber() && size.getNumber() >= 0 && std::floor(size.getNumber()) == size.getNumber(),
        "invalid asset byte count", asset.path.c_str());
    KJ_REQUIRE(size.getNumber() <= static_cast<double>(limits.maxFileBytes), "static asset is larger than allowed",
        asset.path.c_str(), static_cast<uint64_t>(size.getNumber()), limits.maxFileBytes);
    asset.bytes = static_cast<uint64_t>(size.getNumber());
    result.assetBytes += asset.bytes;
    KJ_REQUIRE(result.assetBytes <= limits.maxTotalBytes, "static assets together are larger than allowed",
        result.assetBytes, limits.maxTotalBytes);
    KJ_REQUIRE(assetSafePath(asset.path), "unsafe static asset path", asset.path.c_str());
    KJ_REQUIRE(paths.insert(asset.path).second, "duplicate static asset path", asset.path.c_str());
    KJ_REQUIRE(validHash(asset.sha256), "invalid asset SHA-256", asset.path.c_str());
    KJ_REQUIRE(validContentType(asset.contentType), "invalid asset content type", asset.path.c_str());
    result.assets.push_back(std::move(asset));
  }
  return result;
}
CodePackage readCodePackage(const std::string& manifest) { return readPackage(manifest).modules; }
std::string packageSource(const std::string& manifest, const std::map<std::string, std::string>& modules) {
  std::string out = "{\"manifest\":" + quote(manifest) + ",\"modules\":{";
  bool first = true;
  for (const auto& [hash, bytes] : modules) { if (!first) out += ','; first = false; out += quote(hash) + ':' + quote(base64(bytes)); }
  return out + "}}";
}
std::map<std::string, std::string> packageObjects(const std::filesystem::path& manifestPath,
    const std::string& expectedHash, const std::string& mainModule, bool assets, const AssetLimits& limits) {
  auto contents = readPackage(read(manifestPath), limits);
  auto manifest = packageJson(contents);
  KJ_REQUIRE(objectSha256(manifest) == expectedHash, "package manifest hash mismatch");
  KJ_REQUIRE(assets == !contents.assets.empty(), assets ? "assets configuration without static assets in the package"
      : "package has static assets but the version has no assets configuration");
  bool main = mainModule.empty() && !contents.assets.empty();
  std::map<std::string, std::string> objects{{"packages/" + expectedHash, manifest}};
  const auto directory = std::filesystem::canonical(manifestPath.parent_path());
  for (const auto& m : contents.modules) {
    if (m.name == mainModule) { main = m.type == "esm" || m.type == "cjs"; }
    auto path = std::filesystem::canonical(directory / m.name);
    auto relative = path.lexically_relative(directory);
    KJ_REQUIRE(!relative.empty() && *relative.begin() != "..", "module symlink leaves package directory");
    auto bytes = read(path);
    KJ_REQUIRE(bytes.size() == m.bytes && objectSha256(bytes) == m.sha256, "module bytes/hash mismatch", m.name.c_str());
    if (m.type == "json") canonicalJson(bytes);
    objects.emplace("modules/" + m.sha256, std::move(bytes));
  }
  KJ_REQUIRE(main, mainModule.empty() ? "a package without a main module must have static assets"
      : "main module must exist and be esm or cjs");
  if (!contents.assets.empty()) {
    // Under the reserved module prefix: no module name can reach these files.
    const auto root = std::filesystem::canonical(directory / "__coworkerd_assets");
    for (const auto& a : contents.assets) {
      auto path = std::filesystem::canonical(root / a.path.substr(1));
      auto relative = path.lexically_relative(root);
      KJ_REQUIRE(!relative.empty() && *relative.begin() != "..", "asset leaves the package's assets directory", a.path.c_str());
      auto bytes = read(path);
      KJ_REQUIRE(bytes.size() == a.bytes && objectSha256(bytes) == a.sha256, "static asset bytes/hash mismatch", a.path.c_str());
      objects.emplace("modules/" + a.sha256, std::move(bytes));
    }
  }
  return objects;
}
}  // namespace coworkerd
