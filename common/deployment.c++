#include "deployment.h"
#include "coworkerd.capnp.h"
#include "code-package.h"
#include "generated/compatibility-flags.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <openssl/sha.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <deque>
#include <fstream>
#include <functional>
#include <iostream>
#include <set>
#include <sstream>

namespace coworkerd {
namespace {
namespace rpc = v1::rpc;

std::string read(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  KJ_REQUIRE(input.good(), "cannot open file", path.string().c_str());
  std::ostringstream out;
  out << input.rdbuf();
  KJ_REQUIRE(input.good() || input.eof(), "cannot read file", path.string().c_str());
  return out.str();
}

std::string quote(const std::string& value) {
  capnp::MallocMessageBuilder message;
  auto json = message.initRoot<capnp::JsonValue>();
  json.setString(value.c_str());
  return capnp::JsonCodec().encodeRaw(json).cStr();
}

bool hashValid(const std::string& hash) {
  return hash.size() == 64 && std::all_of(hash.begin(), hash.end(), [](unsigned char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  });
}

void uint32(std::string& out, size_t size) {
  KJ_REQUIRE(size <= 0xffffffff, "definition field too large");
  for (int shift = 24; shift >= 0; shift -= 8) out += static_cast<char>(size >> shift);
}

void stringField(std::string& out, const std::string& value) {
  uint32(out, value.size());
  out += value;
}

template <typename T>
void decodeStrict(const std::string& bytes, typename T::Builder target) {
  capnp::JsonCodec codec;
  codec.setRejectUnknownFields(true);
  codec.decode(kj::arrayPtr(bytes.data(), bytes.size()), target);
}

std::string describe(const kj::Exception& error) { return error.getDescription().cStr(); }

std::string workerElement(const std::string& id, const VersionDefinition& version) {
  return std::string("worker") + '\0' + id + '\0' + version.versionId + '\0' + version.definitionHash;
}

std::string routeElement(const RouteDefinition& route) {
  return std::string("route") + '\0' + route.host + '\0' + route.pathPrefix + '\0' + route.workerId;
}

bool routeHostValid(const std::string& host) {
  return !host.empty() && host.find('/') == std::string::npos;
}

}  // namespace

bool deploymentSafeName(const std::string& value) {
  return !value.empty() && value != "." && value != ".." &&
      std::all_of(value.begin(), value.end(), [](unsigned char c) {
    return std::isalnum(c) || c == '-' || c == '_' || c == '.';
  });
}

bool environmentName(const std::string& value) {
  auto first = [](unsigned char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '$'; };
  return !value.empty() && first(value[0]) && std::all_of(value.begin(), value.end(),
      [&](unsigned char c) { return first(c) || (c >= '0' && c <= '9'); });
}

std::string deploymentSha256(const std::string& bytes) {
  unsigned char digest[SHA256_DIGEST_LENGTH];
  KJ_REQUIRE(SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), digest) == digest,
      "SHA-256 failed");
  const char* hex = "0123456789abcdef";
  std::string result;
  for (auto byte : digest) { result += hex[byte >> 4]; result += hex[byte & 15]; }
  return result;
}

VersionPin versionPin(const VersionDefinition& version) {
  return {version.workerId, version.versionId, version.contentHash, version.definitionHash};
}

namespace {
// JavaScript string length (UTF-16 code units) of UTF-8 text, as wrangler measures a rule.
size_t utf16Length(const std::string& text) {
  size_t units = 0;
  for (unsigned char c : text) {
    if ((c & 0xc0) != 0x80) ++units;
    if (c >= 0xf0) ++units;
  }
  return units;
}
// Cloudflare's checks of run_worker_first rules (wrangler 4.136.1 parseStaticRouting).
void validateRunWorkerFirstRules(const std::vector<std::string>& rules) {
  if (rules.empty()) return;
  KJ_REQUIRE(rules.size() <= 100, "run_worker_first has more than 100 rules");
  std::vector<std::string> positive, negative;
  for (const auto& rule : rules) {
    KJ_REQUIRE(std::none_of(rule.begin(), rule.end(), [](unsigned char c) { return c < 0x20 || c == 0x7f; }),
        "run_worker_first rule has control characters");
    if (rule.starts_with("!/")) negative.push_back(rule);
    else if (rule.starts_with("/")) positive.push_back(rule);
    else KJ_FAIL_REQUIRE(rule.starts_with("!") ? "run_worker_first negative rules must start with '!/'"
        : "run_worker_first rules must start with '/' or '!/'", rule.c_str());
  }
  KJ_REQUIRE(!positive.empty(), "run_worker_first has only negative rules; at least one rule must not start with '!'");
  for (const auto* list : {&positive, &negative}) {
    std::set<std::string> seen;
    for (const auto& rule : *list) {
      KJ_REQUIRE(utf16Length(rule) <= 100, "run_worker_first rules must be at most 100 characters", rule.c_str());
      KJ_REQUIRE(seen.insert(rule).second, "duplicate run_worker_first rule", rule.c_str());
      if (!rule.ends_with("*")) continue;
      for (const auto& other : *list) {
        KJ_REQUIRE(other == rule || !other.starts_with(rule.substr(0, rule.size() - 1)),
            "run_worker_first rule is redundant", other.c_str(), rule.c_str());
      }
    }
  }
}
}  // namespace

namespace {
// A Durable Object class's name: a JavaScript identifier, never one of the bridge's own (__coworkerd_...).
bool durableClassName(const std::string& name) {
  return environmentName(name) && !name.starts_with("__coworkerd");
}
// The Durable Object Bindings and classes of a v2 version: Binding names share env with variables, secrets and the other
// Bindings (`names`), each Binding names a declared class, a class is declared or deleted (once), and a rename names a
// class that is no longer declared and one that is.
void validateDurableObjects(const VersionDefinition& version, std::set<std::string>& names) {
  KJ_REQUIRE(version.durableObjects.size() <= MAX_DURABLE_OBJECT_CLASSES, "a version has more than 64 Durable Object Bindings");
  KJ_REQUIRE(version.declaredClasses.size() + version.deletedClasses.size() + version.renamedClasses.size() <= MAX_DURABLE_OBJECT_CLASSES,
      "a version names more than 64 Durable Object classes");
  std::set<std::string> declared, deleted;
  for (const auto& name : version.declaredClasses) {
    KJ_REQUIRE(durableClassName(name), "invalid Durable Object class name", name.c_str());
    KJ_REQUIRE(declared.insert(name).second, "a Durable Object class is declared twice", name.c_str());
  }
  for (const auto& name : version.deletedClasses) {
    KJ_REQUIRE(durableClassName(name), "invalid Durable Object class name", name.c_str());
    KJ_REQUIRE(!declared.contains(name), "a Durable Object class is both declared and deleted", name.c_str());
    KJ_REQUIRE(deleted.insert(name).second, "a Durable Object class is deleted twice", name.c_str());
  }
  for (const auto& rename : version.renamedClasses) {
    KJ_REQUIRE(durableClassName(rename.from) && durableClassName(rename.to) && rename.from != rename.to,
        "invalid Durable Object class rename", rename.from.c_str(), rename.to.c_str());
    KJ_REQUIRE(!declared.contains(rename.from), "a renamed Durable Object class is still declared", rename.from.c_str());
  }
  for (const auto& binding : version.durableObjects) {
    KJ_REQUIRE(environmentName(binding.binding), "invalid Durable Object Binding name", binding.binding.c_str());
    KJ_REQUIRE(names.insert(binding.binding).second, "duplicate Durable Object Binding or a collision with a variable, secret or Binding",
        binding.binding.c_str());
    // A database Binding names one of the tenant's databases, not a class of this version: its class is the
    // platform's own SQL Worker class, which no version declares.
    if (!binding.database.empty()) {
      KJ_REQUIRE(binding.className == DATABASE_CLASS_NAME, "a database Binding names another class",
          binding.binding.c_str(), binding.className.c_str());
      KJ_REQUIRE(deploymentSafeName(binding.database), "invalid database name", binding.database.c_str());
      continue;
    }
    KJ_REQUIRE(declared.contains(binding.className), "a Durable Object Binding names a class the version does not declare",
        binding.binding.c_str(), binding.className.c_str());
  }
}
// The Durable Object part of a definition's encoding and JSON, in a fixed order: Bindings by name, declared and deleted
// classes sorted, renames in their order.
std::vector<DurableObjectBindingDefinition> sortedDurableBindings(const VersionDefinition& version) {
  auto bindings = version.durableObjects;
  std::sort(bindings.begin(), bindings.end(), [](const auto& a, const auto& b) { return a.binding < b.binding; });
  return bindings;
}
std::vector<std::string> sortedNames(std::vector<std::string> names) { std::sort(names.begin(), names.end()); return names; }
}  // namespace

void validateDefinition(const VersionDefinition& version) {
  // A Worker's full name; its Bindings stay within its tenant (cross-tenant Bindings are not in this version).
  KJ_REQUIRE(workerFullName(version.workerId), "invalid workerId (<tenant>/<worker>)");
  const auto tenant = tenantOf(version.workerId);
  KJ_REQUIRE(deploymentSafeName(version.versionId), "invalid versionId");
  KJ_REQUIRE(version.format == "v1" || version.format == "v2", "unsupported definition format");
  if (version.format == "v2" && version.mainModule.empty()) {
    KJ_REQUIRE(version.assets, "a version without a main module must declare static assets");
  } else {
    KJ_REQUIRE(version.format == "v2" ? moduleSafeName(version.mainModule) : deploymentSafeName(version.mainModule),
        "unsafe mainModule");
  }
  if (version.assets) {
    KJ_REQUIRE(version.format == "v2", "static assets require format v2");
    KJ_REQUIRE(version.assetsNotFoundHandling == "none" || version.assetsNotFoundHandling == "404-page" ||
        version.assetsNotFoundHandling == "single-page-application",
        "assets.notFoundHandling must be none, 404-page or single-page-application", version.assetsNotFoundHandling.c_str());
  }
  KJ_REQUIRE(!version.compatibilityDate.empty(), "compatibilityDate is required");
  KJ_REQUIRE(hashValid(version.contentHash), "contentHash must be 64 lowercase SHA-256 hex characters");
  std::set<std::string> names;
  size_t assetBindings = 0;
  for (const auto& binding : version.bindings) {
    if (binding.assets) {
      KJ_REQUIRE(deploymentSafeName(binding.name) && binding.workerId.empty() && binding.entrypoint.empty(),
          "invalid ASSETS Binding: a name, and neither workerId nor entrypoint");
      KJ_REQUIRE(names.insert(binding.name).second, "duplicate Binding name", binding.name.c_str());
      ++assetBindings;
      continue;
    }
    KJ_REQUIRE(deploymentSafeName(binding.name) && workerFullName(binding.workerId),
        "invalid Binding name or workerId");
    KJ_REQUIRE(tenantOf(binding.workerId) == tenant, "a Binding names a Worker of another tenant", binding.name.c_str());
    KJ_REQUIRE(names.insert(binding.name).second, "duplicate Binding name", binding.name.c_str());
    KJ_REQUIRE(binding.entrypoint.empty() || deploymentSafeName(binding.entrypoint), "invalid Binding entrypoint");
  }
  if (assetBindings) {
    KJ_REQUIRE(assetBindings == 1, "a version has at most one ASSETS Binding");
    KJ_REQUIRE(version.assets && version.hasCode(), "an ASSETS Binding requires static assets and Worker code");
  }
  if (version.assetsRunWorkerFirst || !version.assetsRunWorkerFirstRules.empty()) {
    KJ_REQUIRE(version.assets && version.hasCode(), "run_worker_first requires static assets and Worker code");
    KJ_REQUIRE(!version.assetsRunWorkerFirst || version.assetsRunWorkerFirstRules.empty(),
        "run_worker_first is either true or a list of rules");
    validateRunWorkerFirstRules(version.assetsRunWorkerFirstRules);
  }
  if (version.format == "v2") {
    const auto& date = version.compatibilityDate;
    KJ_REQUIRE(date.size() == 10 && date[4] == '-' && date[7] == '-' &&
        std::all_of(date.begin(), date.begin() + 4, [](char c) { return c >= '0' && c <= '9'; }) &&
        std::all_of(date.begin() + 5, date.begin() + 7, [](char c) { return c >= '0' && c <= '9'; }) &&
        std::all_of(date.begin() + 8, date.end(), [](char c) { return c >= '0' && c <= '9'; }),
        "invalid compatibility date");
    auto calendar = std::chrono::year_month_day{std::chrono::year{std::stoi(date.substr(0, 4))},
        std::chrono::month{static_cast<unsigned>(std::stoi(date.substr(5, 2)))},
        std::chrono::day{static_cast<unsigned>(std::stoi(date.substr(8, 2)))}};
    KJ_REQUIRE(calendar.ok() && date <= MAX_COMPATIBILITY_DATE, "compatibility date exceeds workerd maximum or is invalid");
    std::set<std::string> flags;
    for (const auto& flag : version.compatibilityFlags) {
      KJ_REQUIRE(std::find(COMPATIBILITY_FLAGS.begin(), COMPATIBILITY_FLAGS.end(), flag) != COMPATIBILITY_FLAGS.end(),
          "unsupported or experimental compatibility flag", flag.c_str());
      KJ_REQUIRE(flag != "disable_request_signal", "disable_request_signal is forbidden");
      KJ_REQUIRE(flags.insert(flag).second, "duplicate compatibility flag");
    }
    KJ_REQUIRE(version.variables.size() <= 64, "environment exceeds 64 variables");
    for (const auto& var : version.variables) {
      KJ_REQUIRE(environmentName(var.name), "invalid environment variable name");
      KJ_REQUIRE(names.insert(var.name).second, "duplicate variable or variable/Binding collision", var.name.c_str());
      KJ_REQUIRE(var.type == "text" || var.type == "json", "unsupported variable type");
      KJ_REQUIRE(var.value.size() <= 5 * 1024, "environment variable exceeds 5 KiB");
      if (var.type == "json") { KJ_REQUIRE(canonicalJson(var.value) == var.value, "JSON variable must be canonical"); }
    }
    // Secrets join env through the same channel as variables: names are
    // identifiers and collide with no variable, Binding or other secret.
    KJ_REQUIRE(version.secrets.size() <= MAX_SECRETS, "a version declares more than 64 secrets");
    for (const auto& name : version.secrets) {
      KJ_REQUIRE(environmentName(name), "invalid secret name", name.c_str());
      KJ_REQUIRE(names.insert(name).second, "duplicate secret or secret/variable/Binding collision", name.c_str());
    }
    validateDurableObjects(version, names);
  } else {
    KJ_REQUIRE(!version.hasDurableObjects(), "Durable Objects require format v2");
    KJ_REQUIRE(version.compatibilityFlags.empty() && version.variables.empty() && version.secrets.empty() &&
        std::all_of(version.bindings.begin(), version.bindings.end(), [](const auto& b) { return b.entrypoint.empty(); }),
        "v2 configuration requires format v2");
  }
  KJ_REQUIRE(version.definitionHash == deploymentSha256(definitionEncoding(version)),
      "definition hash mismatch", version.key().c_str());
}

std::string definitionEncoding(const VersionDefinition& version) {
  std::string out = version.format == "v2" ? "coworkerd-definition-v2" : "coworkerd-definition-v1";
  out += '\0';
  for (const auto* value : {&version.workerId, &version.versionId, &version.compatibilityDate,
      &version.mainModule, &version.contentHash}) stringField(out, *value);
  if (version.format == "v2") {
    auto flags = version.compatibilityFlags; std::sort(flags.begin(), flags.end());
    uint32(out, flags.size()); for (const auto& flag : flags) stringField(out, flag);
    auto variables = version.variables;
    std::sort(variables.begin(), variables.end(), [](const auto& a, const auto& b) { return a.name < b.name; });
    uint32(out, variables.size());
    for (const auto& v : variables) { stringField(out, v.name); stringField(out, v.type); stringField(out, v.value); }
  }
  // Bindings to Workers; the ASSETS Binding names no Worker and is encoded with the assets.
  auto bindings = version.bindings;
  std::erase_if(bindings, [](const auto& binding) { return binding.assets; });
  std::sort(bindings.begin(), bindings.end(), [](const auto& a, const auto& b) { return a.name < b.name; });
  uint32(out, bindings.size());
  for (const auto& binding : bindings) {
    stringField(out, binding.name); stringField(out, binding.workerId);
    if (version.format == "v2") stringField(out, binding.entrypoint.empty() ? "default" : binding.entrypoint);
  }
  // Appended only when present: definitions without assets keep their bytes and hashes,
  // and so do definitions with assets but without run_worker_first or an ASSETS Binding.
  if (version.assets) {
    stringField(out, "assets"); stringField(out, version.assetsNotFoundHandling);
    if (version.assetsRunWorkerFirst) stringField(out, "runWorkerFirst");
    if (!version.assetsRunWorkerFirstRules.empty()) {
      auto rules = version.assetsRunWorkerFirstRules; std::sort(rules.begin(), rules.end());
      stringField(out, "runWorkerFirstRules"); uint32(out, rules.size());
      for (const auto& rule : rules) stringField(out, rule);
    }
    if (const auto* binding = version.assetsBinding()) { stringField(out, "binding"); stringField(out, binding->name); }
  }
  // Secret names, sorted, appended only when declared: definitions without
  // secrets keep their bytes and hashes. Values are never part of a definition.
  if (!version.secrets.empty()) {
    auto secrets = version.secrets; std::sort(secrets.begin(), secrets.end());
    stringField(out, "secrets"); uint32(out, secrets.size());
    for (const auto& name : secrets) stringField(out, name);
  }
  // Durable Objects, appended only when the version has any: definitions without them keep their bytes and hashes.
  if (version.hasDurableObjects()) {
    stringField(out, "durableObjects");
    const auto bindings = sortedDurableBindings(version);
    uint32(out, bindings.size());
    for (const auto& binding : bindings) { stringField(out, binding.binding); stringField(out, binding.className); }
    for (const auto& list : {sortedNames(version.declaredClasses), sortedNames(version.deletedClasses)}) {
      uint32(out, list.size());
      for (const auto& name : list) stringField(out, name);
    }
    uint32(out, version.renamedClasses.size());
    for (const auto& rename : version.renamedClasses) { stringField(out, rename.from); stringField(out, rename.to); }
    // The database Bindings, appended only when the version has any: a version with Durable Objects alone keeps its
    // bytes and its hash.
    std::vector<const DurableObjectBindingDefinition*> d1;
    for (const auto& binding : bindings) if (!binding.database.empty()) d1.push_back(&binding);
    if (!d1.empty()) {
      stringField(out, "databases");
      uint32(out, d1.size());
      for (const auto* binding : d1) { stringField(out, binding->binding); stringField(out, binding->database); }
    }
  }
  return out;
}

namespace {
// The assets object of a definition: run_worker_first only when set.
std::string assetsJson(const VersionDefinition& version, const std::string& extra = {}) {
  std::string out = "{\"notFoundHandling\":" + quote(version.assetsNotFoundHandling);
  if (version.assetsRunWorkerFirst) out += ",\"runWorkerFirst\":true";
  if (!version.assetsRunWorkerFirstRules.empty()) {
    auto rules = version.assetsRunWorkerFirstRules; std::sort(rules.begin(), rules.end());
    out += ",\"runWorkerFirstRules\":[";
    for (size_t i = 0; i < rules.size(); ++i) { if (i) out += ','; out += quote(rules[i]); }
    out += ']';
  }
  return out + extra + '}';
}
}  // namespace

std::string definitionJson(const VersionDefinition& version) {
  std::string out = "{\"workerId\":" + quote(version.workerId) + ",\"versionId\":" + quote(version.versionId) +
      ",\"compatibilityDate\":" + quote(version.compatibilityDate) + ",\"mainModule\":" + quote(version.mainModule) +
      ",\"contentHash\":" + quote(version.contentHash) + ",\"definitionHash\":" + quote(version.definitionHash) +
      ",\"bindings\":[";
  auto bindings = version.bindings;
  std::sort(bindings.begin(), bindings.end(), [](const auto& a, const auto& b) { return a.name < b.name; });
  size_t index = 0;
  for (const auto& binding : bindings) {
    if (index++) out += ',';
    if (binding.assets) { out += "{\"name\":" + quote(binding.name) + ",\"assets\":true}"; continue; }
    out += "{\"name\":" + quote(binding.name) + ",\"workerId\":" + quote(binding.workerId);
    if (version.format == "v2") out += ",\"entrypoint\":" + quote(binding.entrypoint.empty() ? "default" : binding.entrypoint);
    out += '}';
  }
  out += ']';
  if (version.format == "v2") {
    out += ",\"format\":\"v2\",\"compatibilityFlags\":[";
    auto flags = version.compatibilityFlags; std::sort(flags.begin(), flags.end());
    for (size_t i = 0; i < flags.size(); ++i) { if (i) out += ','; out += quote(flags[i]); }
    out += "],\"variables\":[";
    auto variables = version.variables;
    std::sort(variables.begin(), variables.end(), [](const auto& a, const auto& b) { return a.name < b.name; });
    for (size_t i = 0; i < variables.size(); ++i) {
      if (i) out += ',';
      const auto& v = variables[i];
      out += "{\"name\":" + quote(v.name) + ",\"type\":" + quote(v.type) + ",\"value\":" + quote(v.value) + '}';
    }
    out += ']';
  }
  if (version.assets) out += ",\"assets\":" + assetsJson(version);
  if (!version.secrets.empty()) {
    auto secrets = version.secrets; std::sort(secrets.begin(), secrets.end());
    out += ",\"secrets\":[";
    for (size_t i = 0; i < secrets.size(); ++i) { if (i) out += ','; out += quote(secrets[i]); }
    out += ']';
  }
  if (version.hasDurableObjects()) out += ",\"durableObjects\":" + durableObjectsJson(version) + ",\"classes\":" + durableClassesJson(version);
  return out + '}';
}

std::string durableObjectsJson(const VersionDefinition& version) {
  std::string out = "[";
  for (const auto& binding : sortedDurableBindings(version))
    out += std::string(out.size() > 1 ? "," : "") + "{\"binding\":" + quote(binding.binding) + ",\"className\":" +
        quote(binding.className) + (binding.database.empty() ? std::string() : ",\"database\":" + quote(binding.database)) + '}';
  return out + ']';
}
std::string durableClassesJson(const VersionDefinition& version) {
  auto list = [](const std::vector<std::string>& names) {
    std::string out = "[";
    for (const auto& name : names) out += std::string(out.size() > 1 ? "," : "") + quote(name);
    return out + ']';
  };
  std::string out = "{\"declared\":" + list(sortedNames(version.declaredClasses)) + ",\"deleted\":" +
      list(sortedNames(version.deletedClasses)) + ",\"renamed\":[";
  for (size_t i = 0; i < version.renamedClasses.size(); ++i)
    out += std::string(i ? "," : "") + "{\"from\":" + quote(version.renamedClasses[i].from) + ",\"to\":" + quote(version.renamedClasses[i].to) + '}';
  return out + "]}";
}

std::string versionStatusJson(const VersionDefinition& version, const std::string& extra) {
  std::string out = "{\"format\":" + quote(version.format) + ",\"packageHash\":" +
      quote(version.format == "v2" ? version.contentHash : "") + ",\"compatibilityDate\":" +
      quote(version.compatibilityDate) + ",\"compatibilityFlags\":[";
  for (size_t i = 0; i < version.compatibilityFlags.size(); ++i) { if (i) out += ','; out += quote(version.compatibilityFlags[i]); }
  out += ']';
  if (version.assets) {
    const auto* binding = version.assetsBinding();
    out += ",\"assets\":" + assetsJson(version, std::string(",\"code\":") + (version.hasCode() ? "true" : "false") +
        (binding ? ",\"binding\":" + quote(binding->name) : std::string()));
  }
  if (!extra.empty()) out += ',' + extra;
  return out + '}';
}

std::string routesJson(const std::vector<RouteDefinition>& routes) {
  std::string out = "[";
  size_t index = 0;
  for (const auto& route : routes) {
    if (index++) out += ',';
    out += routeValueJson(route);
  }
  return out + ']';
}

std::string deploymentRouteKey(const std::string& host, const std::string& pathPrefix) {
  return "routes/" + host + pathPrefix;
}
std::string deploymentTenantPrefix(const std::string& tenant) { return tenant + "/"; }
std::string deploymentActiveKey(const std::string& workerId) {
  return tenantOf(workerId) + "/active/" + workerNameOf(workerId);
}
std::string deploymentDefinitionKey(const std::string& workerId, const std::string& versionId) {
  return tenantOf(workerId) + "/definitions/" + workerNameOf(workerId) + '/' + versionId;
}
std::string deploymentBinderKey(const std::string& target, const std::string& binder) {
  return tenantOf(binder) + "/binders/" + workerNameOf(target) + '/' + workerNameOf(binder);
}
std::string deploymentSecretKey(const std::string& workerId, const std::string& name) {
  return tenantOf(workerId) + "/secrets/" + workerNameOf(workerId) + '/' + name;
}
std::string deploymentNamespacesKey(const std::string& workerId) {
  return tenantOf(workerId) + "/namespaces/" + workerNameOf(workerId);
}

std::string namespacesStateJson(const std::map<std::string, DurableObjectNamespace>& namespaces) {
  std::string out = "{";
  for (const auto& [name, entry] : namespaces)
    out += std::string(out.size() > 1 ? "," : "") + quote(name) + ":{\"id\":" + quote(entry.id) + ",\"status\":" + quote(entry.status) +
        ",\"shards\":" + std::to_string(entry.shards) + '}';
  return out + '}';
}
std::map<std::string, DurableObjectNamespace> decodeNamespacesState(const std::string& bytes,
    bool databases, size_t* invalidEntries) {
  if (invalidEntries) *invalidEntries = 0;
  capnp::MallocMessageBuilder message;
  auto json = message.initRoot<capnp::JsonValue>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), json);
  KJ_REQUIRE(json.isObject(), "a Worker's namespaces are an object");
  std::map<std::string, DurableObjectNamespace> namespaces;
  for (auto entry : json.getObject()) {
    try {
    const std::string name = entry.getName().cStr();
    KJ_REQUIRE(databases ? apiName(name) : durableClassName(name), "invalid namespace name", name.c_str());
    auto value = entry.getValue();
    DurableObjectNamespace item;
    item.id = json::text(value, "id");
    item.status = json::text(value, "status");
    if (auto shards = json::field(value, "shards"); shards.isNumber()) {
      KJ_REQUIRE(shards.getNumber() >= 1 && shards.getNumber() <= 1048576 && shards.getNumber() == std::floor(shards.getNumber()),
          "a namespace has 1 to 1048576 shards", name.c_str());
      item.shards = static_cast<uint32_t>(shards.getNumber());
    }
    KJ_REQUIRE(hashValid(item.id), "a namespace id is 64 lowercase hexadecimal characters", name.c_str());
    KJ_REQUIRE(item.status == "active" || item.status == "deleting", "a namespace's status is active or deleting", name.c_str());
    namespaces.emplace(name, std::move(item));
    } catch (const kj::Exception&) {
      if (invalidEntries) ++*invalidEntries;
    }
  }
  return namespaces;
}

std::string secretRecordJson(const std::string& value, uint64_t setAtUnixMs) {
  return "{\"value\":" + quote(value) + ",\"setAtUnixMs\":" + std::to_string(setAtUnixMs) + '}';
}

SecretValue decodeSecretRecord(const std::string& bytes) {
  SecretValue result;
  // Decoding errors may quote the input, which holds the value: every reason
  // here is fixed text.
  try {
    capnp::MallocMessageBuilder message;
    auto record = message.initRoot<rpc::SecretRecord>();
    decodeStrict<rpc::SecretRecord>(bytes, record);
    if (!record.hasValue()) { result.error = "secret record has no value"; return result; }
    const auto text = record.getValue();
    result.value.assign(text.begin(), text.size());
    const auto at = record.getSetAtUnixMs();
    if (!(at >= 0 && at < 9007199254740992.0) || at != std::floor(at)) {
      result.value.clear(); result.error = "secret record setAtUnixMs is not a non-negative integer"; return result;
    }
    result.setAtUnixMs = static_cast<uint64_t>(at);
  } catch (const kj::Exception&) {
    result.value.clear(); result.error = "secret record is not {\"value\":string,\"setAtUnixMs\":number}"; return result;
  }
  result.error = secretValueProblem(result.value);
  if (!result.error.empty()) result.value.clear();
  return result;
}
std::string activePointerJson(const std::string& versionId, const std::string& definitionHash, const WorkerLimits& limits) {
  return "{\"versionId\":" + quote(versionId) + ",\"definitionHash\":" + quote(definitionHash) +
      (limits.empty() ? std::string() : ",\"limits\":" + workerLimitsJson(limits)) + '}';
}
// The route's tenant and Worker name (the key is global; ownership is in the value).
std::string routeValueJson(const RouteDefinition& route) {
  return "{\"host\":" + quote(route.host) + ",\"pathPrefix\":" + quote(route.pathPrefix) +
      ",\"tenant\":" + quote(tenantOf(route.workerId)) + ",\"worker\":" + quote(workerNameOf(route.workerId)) + '}';
}

namespace {
void readDurableObjects(VersionDefinition& version, capnp::List<rpc::DurableObjectBinding>::Reader bindings,
    kj::Maybe<rpc::DurableObjectClasses::Reader> classes) {
  for (auto binding : bindings)
    version.durableObjects.push_back(
        {binding.getBinding().cStr(), binding.getClassName().cStr(), binding.getDatabase().cStr()});
  KJ_IF_SOME(value, classes) {
    for (auto name : value.getDeclared()) version.declaredClasses.emplace_back(name.cStr());
    for (auto name : value.getDeleted()) version.deletedClasses.emplace_back(name.cStr());
    for (auto rename : value.getRenamed()) version.renamedClasses.push_back({rename.getFrom().cStr(), rename.getTo().cStr()});
  }
}
}  // namespace

std::shared_ptr<const VersionDefinition> decodeDeploymentDefinition(const std::string& bytes) {
  capnp::MallocMessageBuilder message;
  auto value = message.initRoot<rpc::DeploymentDefinition>();
  decodeStrict<rpc::DeploymentDefinition>(bytes, value);
  auto version = std::make_shared<VersionDefinition>();
  version->workerId = value.getWorkerId().cStr();
  version->versionId = value.getVersionId().cStr();
  version->compatibilityDate = value.getCompatibilityDate().cStr();
  version->mainModule = value.getMainModule().cStr();
  version->contentHash = value.getContentHash().cStr();
  version->definitionHash = value.getDefinitionHash().cStr();
  version->format = value.getFormat().cStr();
  for (auto flag : value.getCompatibilityFlags()) version->compatibilityFlags.emplace_back(flag.cStr());
  for (auto v : value.getVariables()) version->variables.push_back({v.getName().cStr(), v.getType().cStr(), v.getValue().cStr()});
  for (auto binding : value.getBindings()) version->bindings.push_back({binding.getName().cStr(), binding.getWorkerId().cStr(),
      binding.getEntrypoint().cStr(), binding.getAssets()});
  if (value.hasAssets()) {
    version->assets = true;
    auto assets = value.getAssets();
    version->assetsNotFoundHandling = assets.getNotFoundHandling().cStr();
    version->assetsRunWorkerFirst = assets.getRunWorkerFirst();
    for (auto rule : assets.getRunWorkerFirstRules()) version->assetsRunWorkerFirstRules.emplace_back(rule.cStr());
  }
  for (auto name : value.getSecrets()) version->secrets.emplace_back(name.cStr());
  readDurableObjects(*version, value.getDurableObjects(), value.hasClasses() ? kj::Maybe(value.getClasses()) : kj::none);
  return version;
}

DeploymentInput readDeploymentManifest(const std::filesystem::path& manifestPath, const std::string& tenant) {
  KJ_REQUIRE(tenantName(tenant), "invalid tenant", tenant.c_str());
  auto qualified = [&](const std::string& name) { return workerFullId(tenant, name); };
  auto bytes = read(manifestPath);
  capnp::MallocMessageBuilder message;
  auto manifest = message.initRoot<rpc::Manifest>();
  capnp::JsonCodec json;
  json.setRejectUnknownFields(true);
  json.decode(kj::arrayPtr(bytes.data(), bytes.size()), manifest);
  DeploymentInput input;
  std::set<std::string> workers;
  const std::string date = manifest.getCompatibilityDate().cStr();
  for (auto worker : manifest.getWorkers()) {
    const std::string name = worker.getWorkerId().cStr();
    KJ_REQUIRE(deploymentSafeName(name), "invalid workerId");
    const std::string id = qualified(name);
    KJ_REQUIRE(workers.insert(id).second, "duplicate workerId");
    for (auto version : worker.getVersions()) {
      auto value = std::make_shared<VersionDefinition>();
      value->workerId = id;
      value->versionId = version.getVersionId().cStr();
      value->format = version.getFormat().cStr();
      value->compatibilityDate = value->format == "v2" ? version.getCompatibilityDate().cStr() : date;
      value->mainModule = version.getMainModule().cStr();
      value->contentHash = version.getContentHash().cStr();
      for (auto binding : version.getBindings()) {
        // A Binding names a Worker of the same tenant; the ASSETS Binding names none.
        const std::string target = binding.getWorkerId().cStr();
        KJ_REQUIRE(binding.getAssets() || deploymentSafeName(target), "invalid Binding workerId", target.c_str());
        value->bindings.push_back({binding.getName().cStr(), binding.getAssets() ? target : qualified(target),
            binding.getEntrypoint().cStr(), binding.getAssets()});
      }
      for (auto flag : version.getCompatibilityFlags()) value->compatibilityFlags.emplace_back(flag.cStr());
      for (auto v : version.getVariables()) {
        std::string type = v.getType().cStr(), raw = v.getValue().cStr();
        value->variables.push_back({v.getName().cStr(), type, type == "json" ? canonicalJson(raw) : raw});
      }
      if (version.hasAssets()) {
        value->assets = true;
        value->assetsNotFoundHandling = version.getAssets().getNotFoundHandling().cStr();
        if (value->assetsNotFoundHandling.empty()) value->assetsNotFoundHandling = "none";
        value->assetsRunWorkerFirst = version.getAssets().getRunWorkerFirst();
        for (auto rule : version.getAssets().getRunWorkerFirstRules()) value->assetsRunWorkerFirstRules.emplace_back(rule.cStr());
      }
      for (auto name : version.getSecrets()) value->secrets.emplace_back(name.cStr());
      readDurableObjects(*value, version.getDurableObjects(), version.hasClasses() ? kj::Maybe(version.getClasses()) : kj::none);
      // A version's own package file inside the package: a relative path below the manifest's directory, never an
      // absolute one or one that leaves it through "..".
      if (const std::filesystem::path given = version.getPackagePath().cStr(); !given.empty()) {
        const auto normal = given.lexically_normal();
        KJ_REQUIRE(given.is_relative() && !normal.empty() && *normal.begin() != ".." && normal != ".",
            "packagePath must be a relative path inside the package (no absolute path, no ..)", given.string().c_str());
        value->packagePath = manifestPath.parent_path() / normal;
      }
      value->definitionHash = deploymentSha256(definitionEncoding(*value));
      validateDefinition(*value);
      KJ_REQUIRE(input.versions.emplace(value->key(), value).second, "duplicate Worker Version");
    }
    std::string active = worker.getActiveVersion().cStr();
    if (!active.empty()) {
      auto found = input.versions.find(id + "@" + active);
      KJ_REQUIRE(found != input.versions.end(), "active Worker Version is missing", id.c_str(), active.c_str());
      input.active.emplace(id, versionPin(*found->second));
    }
  }
  for (auto route : manifest.getRoutes()) {
    const std::string target = route.getWorkerId().cStr();
    KJ_REQUIRE(deploymentSafeName(target), "invalid route workerId", target.c_str());
    input.routes.push_back({route.getHost().cStr(), route.getPathPrefix().cStr(), qualified(target)});
  }
  return input;
}

DeploymentCode readDeploymentCode(const DeploymentInput& input, const std::filesystem::path& codeRoot) {
  DeploymentCode code;
  for (const auto& [label, version] : input.versions) {
    if (version->format == "v2") {
      // A package directory is named by the Worker's name in its tenant, as the manifest names it.
      auto path = version->packagePath.empty() ? codeRoot / workerNameOf(version->workerId) / version->versionId / "package.json"
          : version->packagePath;
      auto objects = packageObjects(path, version->contentHash, version->mainModule, version->assets);
      // The runtime loads the manifest and the code modules only; asset bytes stay in the object store.
      const auto& manifest = objects.at("packages/" + version->contentHash);
      auto contents = readPackage(manifest);
      std::map<std::string, std::string> modules;
      for (const auto& m : contents.modules) modules.emplace(m.sha256, objects.at("modules/" + m.sha256));
      for (const auto& a : contents.assets) code.assetObjects.insert("modules/" + a.sha256);
      code.sources.emplace(version->definitionHash, packageSource(manifest, modules));
      code.objects.merge(objects);
      continue;
    }
    auto source = read(codeRoot / workerNameOf(version->workerId) / version->versionId / version->mainModule);
    KJ_REQUIRE(deploymentSha256(source) == version->contentHash,
        "Worker source does not match manifest contentHash", label.c_str());
    code.objects.emplace("bundles/" + version->contentHash, source);
    code.sources.emplace(version->definitionHash, std::move(source));
  }
  return code;
}

namespace {
int hexValue(char c) {
  return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : c >= 'A' && c <= 'F' ? c - 'A' + 10 : -1;
}
bool validUtf8(const std::string& s) {
  for (size_t i = 0; i < s.size();) {
    const auto c = static_cast<unsigned char>(s[i]);
    size_t n = c < 0x80 ? 0 : (c >> 5) == 0x6 ? 1 : (c >> 4) == 0xe ? 2 : (c >> 3) == 0x1e ? 3 : 4;
    if (n == 4 || i + n >= s.size() + (n == 0)) return false;
    uint32_t code = n == 0 ? c : c & (0x3f >> n);
    for (size_t k = 1; k <= n; ++k) {
      const auto d = static_cast<unsigned char>(s[i + k]);
      if ((d >> 6) != 0x2) return false;
      code = code << 6 | (d & 0x3f);
    }
    if ((n == 1 && code < 0x80) || (n == 2 && code < 0x800) || (n == 3 && code < 0x10000) || code > 0x10ffff ||
        (code >= 0xd800 && code <= 0xdfff)) return false;
    i += n + 1;
  }
  return true;
}
// decodeURIComponent: the segment unchanged when it does not decode.
std::string decodeSegment(const std::string& segment) {
  if (segment.find('%') == std::string::npos) return segment;
  std::string out;
  for (size_t i = 0; i < segment.size(); ++i) {
    if (segment[i] != '%') { out += segment[i]; continue; }
    if (i + 2 >= segment.size() || hexValue(segment[i + 1]) < 0 || hexValue(segment[i + 2]) < 0) return segment;
    out += static_cast<char>(hexValue(segment[i + 1]) * 16 + hexValue(segment[i + 2]));
    i += 2;
  }
  return validUtf8(out) ? out : segment;
}
std::string lowercase(std::string s) {
  for (auto& c : s) if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
  return s;
}
}  // namespace

std::string secretValueProblem(const std::string& value) {
  if (value.size() > MAX_SECRET_BYTES) return "secret value exceeds 5 KiB";
  if (value.find('\0') != std::string::npos) return "secret value contains NUL";
  if (!validUtf8(value)) return "secret value is not UTF-8 text";
  return {};
}

namespace {
class AssetResolver {
public:
  AssetResolver(const AssetIndex& index, const std::string& notFoundHandling) : index(index), notFoundHandling(notFoundHandling) {}
  AssetIntent intent(const std::string& path, bool skipRedirects) const {
    const auto* exact = index.find(path);
    const auto size = path.size();
    auto hit = [](const CodeAsset* asset) { return AssetIntent{asset, 200, false}; };
    if (path.ends_with("/index")) {
      if (exact) return hit(exact);
      if (auto* file = redirected(path + ".html", path.substr(0, size - 5), skipRedirects)) return hit(file);
      if (auto* file = redirected(path.substr(0, size - 6) + ".html", path.substr(0, size - 6), skipRedirects)) return hit(file);
    } else if (path.ends_with("/index.html")) {
      if (auto* file = redirected(path, path.substr(0, size - 10), skipRedirects)) return hit(file);
      if (auto* file = redirected(path.substr(0, size - 11) + ".html", path.substr(0, size - 11), skipRedirects)) return hit(file);
    } else if (path.ends_with("/")) {
      if (auto* file = index.find(path + "index.html")) return hit(file);
      if (auto* file = redirected(path.substr(0, size - 1) + ".html", path.substr(0, size - 1), skipRedirects)) return hit(file);
    } else if (path.ends_with(".html")) {
      if (auto* file = redirected(path, path.substr(0, size - 5), skipRedirects)) return hit(file);
      if (auto* file = redirected(path.substr(0, size - 5) + "/index.html", path.substr(0, size - 5) + "/", skipRedirects)) return hit(file);
    }
    if (exact) return hit(exact);
    if (auto* file = index.find(path + ".html")) return hit(file);
    if (auto* file = redirected(path + "/index.html", path + "/", skipRedirects)) return hit(file);
    return notFound(path);
  }

private:
  // Cloudflare redirects to `destination` when it is not an asset itself and
  // resolves (without redirects) to `file`; `file` is served here directly.
  const CodeAsset* redirected(const std::string& file, const std::string& destination, bool skipRedirects) const {
    if (skipRedirects || index.find(destination)) return nullptr;
    const auto* asset = index.find(file);
    return asset && intent(destination, true).asset == asset ? asset : nullptr;
  }
  AssetIntent notFound(const std::string& path) const {
    if (notFoundHandling == "single-page-application") {
      if (auto* page = index.find("/index.html")) return {page, 200, true};
    } else if (notFoundHandling == "404-page") {
      for (std::string directory = path; !directory.empty();) {
        const auto slash = directory.rfind('/');
        directory = slash == std::string::npos ? directory.substr(0, directory.size() - 1) : directory.substr(0, slash);
        if (auto* page = index.find(directory + "/404.html")) return {page, 404, true};
      }
    }
    return {};
  }
  const AssetIndex& index;
  const std::string& notFoundHandling;
};
}  // namespace

std::string assetRequestPath(const std::string& rawPath) {
  std::string path = rawPath;
  std::replace(path.begin(), path.end(), '\\', '/');
  // Dot segments as a URL parser removes them (".", "..", and their %2e forms).
  std::vector<std::string> segments;
  size_t start = 0;
  if (path.starts_with("/")) start = 1;
  while (true) {
    const auto end = path.find('/', start);
    auto segment = path.substr(start, end == std::string::npos ? std::string::npos : end - start);
    const auto lower = lowercase(segment);
    const bool last = end == std::string::npos;
    if (lower == ".." || lower == ".%2e" || lower == "%2e." || lower == "%2e%2e") {
      if (!segments.empty()) segments.pop_back();
      if (last) segments.push_back("");
    } else if (lower == "." || lower == "%2e") {
      if (last) segments.push_back("");
    } else {
      segments.push_back(std::move(segment));
    }
    if (last) break;
    start = end + 1;
  }
  std::string decoded;
  for (const auto& segment : segments) decoded += '/' + decodeSegment(segment);
  if (decoded.empty()) decoded = "/";
  std::string collapsed;
  for (char c : decoded) if (c != '/' || collapsed.empty() || collapsed.back() != '/') collapsed += c;
  return collapsed;
}

AssetIntent resolveAsset(const AssetIndex& index, const std::string& path, const std::string& notFoundHandling) {
  return AssetResolver(index, notFoundHandling).intent(path, false);
}

std::string assetUrlPath(const std::string& rawPath) {
  std::string path = rawPath;
  std::replace(path.begin(), path.end(), '\\', '/');
  std::vector<std::string> segments;
  size_t start = path.starts_with("/") ? 1 : 0;
  while (true) {
    const auto end = path.find('/', start);
    auto segment = path.substr(start, end == std::string::npos ? std::string::npos : end - start);
    const auto lower = lowercase(segment);
    const bool last = end == std::string::npos;
    if (lower == ".." || lower == ".%2e" || lower == "%2e." || lower == "%2e%2e") {
      if (!segments.empty()) segments.pop_back();
      if (last) segments.push_back("");
    } else if (lower == "." || lower == "%2e") {
      if (last) segments.push_back("");
    } else {
      segments.push_back(std::move(segment));
    }
    if (last) break;
    start = end + 1;
  }
  // The URL path percent-encode set; existing escapes stay as they are.
  const char* hex = "0123456789ABCDEF";
  std::string out;
  for (const auto& segment : segments) {
    out += '/';
    for (unsigned char c : segment) {
      if (c <= 0x20 || c >= 0x7f || c == '"' || c == '#' || c == '<' || c == '>' || c == '?' || c == '`' || c == '{' || c == '}') {
        out += '%'; out += hex[c >> 4]; out += hex[c & 15];
      } else {
        out += static_cast<char>(c);
      }
    }
  }
  return out.empty() ? "/" : out;
}

bool assetRuleMatches(const std::string& rule, const std::string& path) {
  size_t r = 0, p = 0, star = std::string::npos, resume = 0;
  while (p < path.size()) {
    if (r < rule.size() && rule[r] == '*') { star = r++; resume = p; }
    else if (r < rule.size() && rule[r] == path[p]) { ++r; ++p; }
    else if (star != std::string::npos) { r = star + 1; p = ++resume; }
    else return false;
  }
  while (r < rule.size() && rule[r] == '*') ++r;
  return r == rule.size();
}

bool assetsNavigationPrefersAssets(const VersionDefinition& version) {
  const auto& flags = version.compatibilityFlags;
  auto has = [&](const char* flag) { return std::find(flags.begin(), flags.end(), flag) != flags.end(); };
  if (has("assets_navigation_prefers_asset_serving")) return true;
  return version.compatibilityDate >= "2025-04-01" && !has("assets_navigation_has_no_effect");
}

AssetRoute routeAsset(const VersionDefinition& version, const AssetIndex& index, const std::string& rawPath, bool navigate) {
  using Target = AssetRoute::Target;
  using Reason = AssetRoute::Reason;
  if (!version.hasCode()) return {Target::ASSETS, Reason::NO_CODE};
  const auto& rules = version.assetsRunWorkerFirstRules;
  if (!rules.empty()) {
    const auto path = assetUrlPath(rawPath);
    for (const auto& rule : rules) {
      if (rule.starts_with("!") && assetRuleMatches(rule.substr(1), path)) return {Target::ASSETS, Reason::EXCLUDED};
    }
    for (const auto& rule : rules) {
      if (!rule.starts_with("!") && assetRuleMatches(rule, path)) return {Target::WORKER, Reason::RULE};
    }
  }
  if (version.assetsRunWorkerFirst) return {Target::WORKER, Reason::RUN_WORKER_FIRST};
  // canFetch: not_found_handling counts with rules, or for a navigation request
  // when the compatibility flag is on; otherwise only assets that exist.
  const bool navigation = rules.empty() && navigate && assetsNavigationPrefersAssets(version);
  const auto intent = resolveAsset(index, assetRequestPath(rawPath),
      !rules.empty() || navigation ? version.assetsNotFoundHandling : std::string("none"));
  if (!intent.asset) return {Target::WORKER, Reason::MISS};
  return {Target::ASSETS, intent.fallback && navigation ? Reason::NAVIGATION : Reason::INTENT};
}

namespace {
// The element's SHA-256 as four big-endian 64-bit limbs, most significant first.
std::array<uint64_t, 4> digestLimbs(const std::string& element) {
  unsigned char digest[SHA256_DIGEST_LENGTH];
  KJ_REQUIRE(SHA256(reinterpret_cast<const unsigned char*>(element.data()), element.size(), digest) == digest,
      "SHA-256 failed");
  std::array<uint64_t, 4> limbs{};
  for (size_t i = 0; i < SHA256_DIGEST_LENGTH; ++i) limbs[i / 8] = limbs[i / 8] << 8 | digest[i];
  return limbs;
}
}  // namespace

void SetDigest::add(const std::string& element) {
  auto value = digestLimbs(element);
  unsigned __int128 carry = 0;
  for (int limb = 3; limb >= 0; --limb) {
    unsigned __int128 sum = static_cast<unsigned __int128>(limbs[limb]) + value[limb] + carry;
    limbs[limb] = static_cast<uint64_t>(sum);
    carry = sum >> 64;
  }
}

void SetDigest::remove(const std::string& element) {
  auto value = digestLimbs(element);
  unsigned __int128 borrow = 0;
  for (int limb = 3; limb >= 0; --limb) {
    unsigned __int128 subtrahend = static_cast<unsigned __int128>(value[limb]) + borrow;
    borrow = limbs[limb] < subtrahend ? 1 : 0;
    limbs[limb] = static_cast<uint64_t>(static_cast<unsigned __int128>(limbs[limb]) - subtrahend);
  }
}

std::string SetDigest::hex() const {
  const char* digits = "0123456789abcdef";
  std::string out;
  for (auto limb : limbs) for (int shift = 60; shift >= 0; shift -= 4) out += digits[(limb >> shift) & 15];
  return out;
}

DeploymentState::ApplyStats DeploymentState::apply(const std::vector<Change>& changes, bool full, uint64_t revision) {
  ApplyStats stats;
  stats.keys = changes.size();
  // Served definition of each Worker before this batch, recorded before its
  // first modification.
  std::map<std::string, std::shared_ptr<const VersionDefinition>> before;
  auto observe = [&](const std::string& id) {
    if (before.contains(id)) return;
    auto found = workerMap.find(id);
    before.emplace(id, found != workerMap.end() && found->second.valid ? found->second.version : nullptr);
  };
  if (full) {
    for (const auto& [id, worker] : workerMap) observe(id);
    const auto require = requireSecretValues;
    *this = DeploymentState();
    requireSecretValues = require;
  }
  std::set<std::string> touched, routeKeys;
  bool sawGeneration = false;
  for (const auto& change : changes) {
    const auto& key = change.key;
    auto unknown = [&] { if (change.value) unknownKeys.insert(key); else unknownKeys.erase(key); };
    if (key.starts_with("routes/")) {
      auto route = key.substr(7);
      setRoute(route, change.value);
      routeKeys.insert(route);
      continue;
    }
    if (key == "generation") {
      sawGeneration = true;
      generationValue = 0; generationError.clear();
      if (!change.value) continue;
      try {
        capnp::MallocMessageBuilder message;
        auto value = message.initRoot<capnp::JsonValue>();
        capnp::JsonCodec().decodeRaw(kj::arrayPtr(change.value->data(), change.value->size()), value);
        KJ_REQUIRE(value.isString(), "deployment generation must be a JSON decimal string");
        std::string text = value.getString().cStr();
        KJ_REQUIRE(!text.empty() && text.size() <= 19 && std::all_of(text.begin(), text.end(),
            [](unsigned char c) { return std::isdigit(c); }), "deployment generation must be a JSON decimal string");
        generationValue = std::stoull(text);
      } catch (const kj::Exception& error) {
        generationError = describe(error);
      }
      continue;
    }
    // Every other key is below its tenant: <tenant>/{definitions,active,secrets,namespaces,binders}/...
    const auto tenantEnd = key.find('/');
    const auto tenant = tenantEnd == std::string::npos ? std::string() : key.substr(0, tenantEnd);
    const auto local = tenantEnd == std::string::npos ? std::string() : key.substr(tenantEnd + 1);
    if (!tenantName(tenant)) { unknown(); continue; }
    if (local.starts_with("secrets/")) {
      // <tenant>/secrets/<worker>/<name>: a value changes no definition and no pin;
      // only a change of usability can change the Worker's validity.
      auto rest = local.substr(8);
      auto slash = rest.find('/');
      if (slash == std::string::npos || !deploymentSafeName(rest.substr(0, slash)) || !environmentName(rest.substr(slash + 1))) {
        unknown();
        continue;
      }
      auto worker = workerFullId(tenant, rest.substr(0, slash)), name = rest.substr(slash + 1);
      auto& names = secretMap[worker];
      auto found = names.find(name);
      const bool usableBefore = found != names.end() && found->second.error.empty();
      if (found != names.end()) { --secretKeys; if (!found->second.error.empty()) --unusableSecrets; names.erase(found); }
      bool usableAfter = false;
      if (change.value) {
        auto decoded = decodeSecretRecord(*change.value);
        usableAfter = decoded.error.empty();
        ++secretKeys; if (!usableAfter) ++unusableSecrets;
        names.emplace(name, std::move(decoded));
      }
      if (names.empty()) secretMap.erase(worker);
      if (usableBefore != usableAfter && workerMap.contains(worker)) { observe(worker); retract(worker); touched.insert(worker); }
      continue;
    }
    if (local == "databases") {
      // <tenant>/databases: the tenant's databases, each a namespace of its own. Like the namespaces key, it changes
      // no definition, pin or validity -- a request to a database's object reads it when it is dispatched.
      databaseMap.erase(tenant);
      invalidNamespaceEntries.erase(key);
      if (!change.value) { unknownKeys.erase(key); continue; }
      try {
        auto decoded = decodeNamespacesState(*change.value, true, &invalidNamespaceEntries[key]);
        if (!invalidNamespaceEntries[key]) invalidNamespaceEntries.erase(key);
        if (!decoded.empty()) databaseMap.emplace(tenant, std::move(decoded));
        unknownKeys.erase(key);
      } catch (const kj::Exception&) {
        unknown();
      }
      continue;
    }
    if (local.starts_with("namespaces/")) {
      // <tenant>/namespaces/<worker>: the Worker's Durable Object namespaces. They change no definition, pin or validity:
      // a request to an object reads them when it is dispatched.
      const auto name = local.substr(11);
      if (!deploymentSafeName(name)) { unknown(); continue; }
      const auto worker = workerFullId(tenant, name);
      namespaceMap.erase(worker);
      invalidNamespaceEntries.erase(key);
      if (!change.value) { unknownKeys.erase(key); continue; }
      try {
        auto decoded = decodeNamespacesState(*change.value, false, &invalidNamespaceEntries[key]);
        if (!invalidNamespaceEntries[key]) invalidNamespaceEntries.erase(key);
        if (!decoded.empty()) namespaceMap.emplace(worker, std::move(decoded));
        unknownKeys.erase(key);
      } catch (const kj::Exception&) {
        unknown();
      }
      continue;
    }
    if (local.starts_with("definitions/")) {
      auto rest = local.substr(12);
      auto slash = rest.find('/');
      if (slash == std::string::npos || slash == 0 || rest.find('/', slash + 1) != std::string::npos) {
        unknown();
        continue;
      }
      auto worker = workerFullId(tenant, rest.substr(0, slash)), version = rest.substr(slash + 1), label = worker + "@" + version;
      // Only the definition a pointer names can change a Worker; publishing a
      // version before its flip touches nobody.
      auto pointed = workerMap.find(worker);
      if (pointed != workerMap.end() && pointed->second.versionId == version) {
        observe(worker); retract(worker); touched.insert(worker);
      }
      if (!change.value) { definitionMap.erase(label); invalidDefinitions.erase(label); continue; }
      Definition entry;
      try {
        auto decoded = decodeDeploymentDefinition(*change.value);
        KJ_REQUIRE(decoded->key() == label, "definition key/label mismatch");
        validateDefinition(*decoded);
        entry.version = decoded;
        stats.definitions.push_back(std::move(decoded));
      } catch (const kj::Exception& error) {
        entry.error = describe(error);
      }
      if (entry.error.empty()) invalidDefinitions.erase(label); else invalidDefinitions.insert(label);
      definitionMap[label] = std::move(entry);
    } else if (local.starts_with("active/")) {
      auto worker = workerFullId(tenant, local.substr(7));
      observe(worker); retract(worker); touched.insert(worker);
      if (!change.value) {
        auto found = workerMap.find(worker);
        if (found == workerMap.end()) continue;
        for (const auto& target : found->second.targets) {
          auto edges = binders.find(target);
          if (edges != binders.end() && edges->second.erase(worker) && edges->second.empty()) binders.erase(edges);
        }
        invalidWorkers.erase(worker);
        workerMap.erase(found);
        continue;
      }
      auto& entry = workerMap[worker];
      entry.versionId.clear(); entry.definitionHash.clear(); entry.pointerError.clear(); entry.limits = {};
      try {
        KJ_REQUIRE(workerFullName(worker), "invalid active pointer: unsafe workerId");
        capnp::MallocMessageBuilder message;
        auto value = message.initRoot<rpc::ActiveVersion>();
        decodeStrict<rpc::ActiveVersion>(*change.value, value);
        entry.versionId = value.getVersionId().cStr();
        entry.definitionHash = value.getDefinitionHash().cStr();
        if (value.hasLimits()) entry.limits = {value.getLimits().getCpuMs(), value.getLimits().getMemoryMiB()};
        KJ_REQUIRE(deploymentSafeName(entry.versionId), "invalid active pointer: unsafe versionId");
        KJ_REQUIRE(hashValid(entry.definitionHash), "invalid active pointer: definitionHash is not SHA-256 hex");
      } catch (const kj::Exception& error) {
        entry.pointerError = describe(error);
        if (!entry.pointerError.starts_with("invalid active pointer")) entry.pointerError = "invalid active pointer: " + entry.pointerError;
      }
    } else if (local.starts_with("binders/")) {
      // The tool's reverse Binding index; counted for status only.
      if (change.value) binderKeys.insert(key); else binderKeys.erase(key);
    } else {
      unknown();
    }
  }
  if (full && !sawGeneration) generationValue = 0;
  stats.workers = touched.size();
  for (const auto& id : touched) if (workerMap.contains(id)) refreshOwn(id);
  // Every Worker whose closure can contain a changed Worker binds it, directly
  // or through other Workers.
  std::set<std::string> affected = touched;
  std::deque<std::string> queue(touched.begin(), touched.end());
  while (!queue.empty()) {
    auto edges = binders.find(queue.front());
    queue.pop_front();
    if (edges == binders.end()) continue;
    for (const auto& binder : edges->second) if (affected.insert(binder).second) queue.push_back(binder);
  }
  for (const auto& id : affected) {
    if (!workerMap.contains(id)) continue;
    observe(id);
    evaluate(id, stats);
    ++stats.evaluated;
  }
  for (const auto& id : affected) {
    auto found = workerRoutes.find(id);
    if (found != workerRoutes.end()) routeKeys.insert(found->second.begin(), found->second.end());
  }
  for (const auto& key : routeKeys) if (routeMap.contains(key)) { evaluateRoute(key); ++stats.routes; }
  for (const auto& [id, old] : before) {
    auto found = workerMap.find(id);
    auto now = found != workerMap.end() && found->second.valid ? found->second.version : nullptr;
    if ((old ? old->definitionHash : "") != (now ? now->definitionHash : "")) stats.served.push_back({id, old, now});
  }
  revisionValue = revision;
  return stats;
}

void DeploymentState::retract(const std::string& id) {
  auto found = workerMap.find(id);
  if (found == workerMap.end() || !found->second.counted) return;
  auto& worker = found->second;
  validSet.remove(workerElement(id, *worker.version));
  --validWorkers;
  auto size = closureSizes.find(worker.closure->size());
  if (--size->second == 0) closureSizes.erase(size);
  worker.counted = false;
}

void DeploymentState::refreshOwn(const std::string& id) {
  auto& worker = workerMap.at(id);
  auto previous = std::move(worker.targets);
  worker.targets.clear();
  worker.own = false; worker.ownReason.clear(); worker.version.reset(); worker.missingSecrets.clear();
  if (!worker.pointerError.empty()) {
    worker.ownReason = worker.pointerError;
  } else {
    const auto label = id + "@" + worker.versionId;
    auto definition = definitionMap.find(label);
    if (definition == definitionMap.end()) {
      worker.ownReason = "active Worker Version is missing: " + label;
    } else if (!definition->second.version) {
      worker.ownReason = "active definition is invalid: " + label + ": " + definition->second.error;
    } else if (definition->second.version->definitionHash != worker.definitionHash) {
      worker.ownReason = "active definitionHash mismatch: " + label + " pointer " + worker.definitionHash.substr(0, 12) +
          " definition " + definition->second.version->definitionHash.substr(0, 12);
    } else if (worker.missingSecrets = missingSecrets(*definition->second.version); !worker.missingSecrets.empty()) {
      // Names only: the reason, the status and the logs never carry a value.
      worker.ownReason = worker.missingSecrets.size() == 1 ? "missing secret: " : "missing secrets: ";
      for (size_t i = 0; i < worker.missingSecrets.size(); ++i) worker.ownReason += (i ? ", " : "") + worker.missingSecrets[i];
    } else {
      worker.own = true;
      worker.version = definition->second.version;
      auto bindings = worker.version->bindings;
      std::sort(bindings.begin(), bindings.end(), [](const auto& a, const auto& b) { return a.name < b.name; });
      for (const auto& binding : bindings) {
        if (binding.assets) continue;  // the version's own asset layer, no Worker
        if (std::find(worker.targets.begin(), worker.targets.end(), binding.workerId) == worker.targets.end())
          worker.targets.push_back(binding.workerId);
      }
    }
  }
  for (const auto& target : previous) {
    auto edges = binders.find(target);
    if (edges != binders.end() && edges->second.erase(id) && edges->second.empty()) binders.erase(edges);
  }
  for (const auto& target : worker.targets) binders[target].insert(id);
}

void DeploymentState::evaluate(const std::string& id, ApplyStats&) {
  retract(id);
  auto& worker = workerMap.at(id);
  worker.valid = false; worker.reason.clear(); worker.closure.reset();
  if (!worker.own) {
    worker.reason = worker.ownReason;
  } else {
    auto pins = std::make_shared<VersionPins>();
    std::string failure;
    std::function<bool(const std::string&, const std::string&)> visit =
        [&](const std::string& node, const std::string& parent) -> bool {
      if (pins->contains(node)) return true;
      auto found = workerMap.find(node);
      if (found == workerMap.end()) {
        failure = "Binding target has no active version: " + node + " (bound by " + parent + ")";
        return false;
      }
      if (!found->second.own) {
        failure = "Binding target " + node + " is invalid: " + found->second.ownReason;
        return false;
      }
      if (node != id && !found->second.version->hasCode()) {
        // Bindings call Worker code; a static-assets-only Worker has none.
        failure = "Binding target " + node + " has no Worker code (static assets only)";
        return false;
      }
      if (pins->size() == MAX_DEPLOYMENT_CLOSURE) {
        failure = "deployment closure exceeds limit 64";
        return false;
      }
      pins->emplace(node, versionPin(*found->second.version));
      for (const auto& target : found->second.targets) if (!visit(target, node)) return false;
      return true;
    };
    if (visit(id, id)) {
      worker.valid = true;
      worker.closure = std::move(pins);
    } else {
      worker.reason = failure;
    }
  }
  if (worker.valid) {
    validSet.add(workerElement(id, *worker.version));
    ++validWorkers;
    ++closureSizes[worker.closure->size()];
    worker.counted = true;
    invalidWorkers.erase(id);
  } else {
    invalidWorkers.insert(id);
  }
}

void DeploymentState::setRoute(const std::string& key, const std::optional<std::string>& value) {
  auto found = routeMap.find(key);
  if (found != routeMap.end()) {
    auto& route = found->second;
    if (route.counted) { routeSet.remove(routeElement(route.value)); --effectiveRoutes; }
    if (!route.value.workerId.empty()) {
      auto keys = workerRoutes.find(route.value.workerId);
      if (keys != workerRoutes.end() && keys->second.erase(key) && keys->second.empty()) workerRoutes.erase(keys);
    }
    if (!route.slotPrefix.empty()) {
      auto length = slotLengths.find(route.slotPrefix.size());
      if (--length->second == 0) slotLengths.erase(length);
    }
    invalidRoutes.erase(key);
    routeMap.erase(found);
  }
  if (!value) return;
  Route route;
  auto slash = key.find('/');
  if (slash != std::string::npos && slash > 0) {
    route.slotHost = key.substr(0, slash);
    route.slotPrefix = key.substr(slash);
  }
  std::string tenant, worker;
  try {
    // {host, pathPrefix, tenant, worker}: the route's owner is in its value.
    capnp::MallocMessageBuilder message;
    auto decoded = message.initRoot<rpc::RouteRecord>();
    decodeStrict<rpc::RouteRecord>(*value, decoded);
    tenant = decoded.getTenant().cStr();
    worker = decoded.getWorker().cStr();
    route.value = {decoded.getHost().cStr(), decoded.getPathPrefix().cStr(), workerFullId(tenant, worker)};
  } catch (const kj::Exception& error) {
    route.error = "route value is invalid: " + describe(error);
  }
  if (route.error.empty()) {
    if (route.slotPrefix.empty()) route.error = "route key is not <host><pathPrefix>";
    else if (!routeHostValid(route.value.host)) route.error = "route host must be non-empty and contain no '/'";
    else if (!route.value.pathPrefix.starts_with('/')) route.error = "route pathPrefix must start with /";
    else if (!tenantName(tenant) || !deploymentSafeName(worker)) route.error = "route tenant or worker is invalid";
    else if (route.value.host + route.value.pathPrefix != key) route.error = "route key does not match value";
  }
  if (!route.error.empty() && (!tenantName(tenant) || !deploymentSafeName(worker))) route.value.workerId.clear();
  if (!route.value.workerId.empty()) workerRoutes[route.value.workerId].insert(key);
  if (!route.slotPrefix.empty()) ++slotLengths[route.slotPrefix.size()];
  routeMap.emplace(key, std::move(route));
}

void DeploymentState::evaluateRoute(const std::string& key) {
  auto& route = routeMap.at(key);
  if (route.counted) { routeSet.remove(routeElement(route.value)); --effectiveRoutes; route.counted = false; }
  route.effective = false; route.reason.clear();
  if (!route.error.empty()) {
    route.reason = route.error;
  } else {
    auto worker = workerMap.find(route.value.workerId);
    if (worker == workerMap.end()) route.reason = "route Worker has no active version: " + route.value.workerId;
    else if (!worker->second.valid) route.reason = "route Worker is invalid: " + route.value.workerId + ": " + worker->second.reason;
    else route.effective = true;
  }
  if (route.effective) {
    routeSet.add(routeElement(route.value));
    ++effectiveRoutes;
    route.counted = true;
    invalidRoutes.erase(key);
  } else {
    invalidRoutes.insert(key);
  }
}

DeploymentState DeploymentState::fromInput(const DeploymentInput& input, uint64_t revision, uint64_t generation,
    bool requireSecretValues) {
  std::vector<Change> changes;
  for (const auto& [label, version] : input.versions) {
    changes.push_back({deploymentDefinitionKey(version->workerId, version->versionId), definitionJson(*version)});
  }
  for (const auto& [worker, pin] : input.active) {
    changes.push_back({deploymentActiveKey(worker), activePointerJson(pin.versionId, pin.definitionHash)});
  }
  std::set<std::string> keys;
  for (const auto& route : input.routes) {
    KJ_REQUIRE(routeHostValid(route.host), "route host must be non-empty and contain no '/'", route.host.c_str());
    KJ_REQUIRE(route.pathPrefix.starts_with('/'), "route pathPrefix must start with /");
    auto key = deploymentRouteKey(route.host, route.pathPrefix);
    KJ_REQUIRE(keys.insert(key).second, "duplicate route", route.host.c_str(), route.pathPrefix.c_str());
    changes.push_back({key, routeValueJson(route)});
  }
  DeploymentState state;
  state.requireSecretValues = requireSecretValues;
  state.apply(changes, true, revision);
  state.generationValue = generation;
  return state;
}

std::string requestHost(const std::string& host) {
  std::string out = host;
  for (auto& c : out) if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
  if (out.size() > 1 && out.back() == '.') out.pop_back();
  return out;
}

std::string defaultHostPrefix(const std::string& tenant, const std::string& pathPrefix) {
  return pathPrefix == "/" + tenant ? pathPrefix + "/" : pathPrefix;
}

DeploymentState::Selection DeploymentState::route(const std::string& rawHost, const std::string& path, const std::string* owner) const {
  Selection selection;
  const auto host = requestHost(rawHost);
  if (host.find('/') != std::string::npos) return selection;
  for (auto length = slotLengths.rbegin(); length != slotLengths.rend(); ++length) {
    if (length->first > path.size()) continue;
    const auto prefix = path.substr(0, length->first);
    // By path segments: the prefix is the whole path, ends with '/', or the path goes on with a new segment.
    if (path.size() != prefix.size() && !prefix.ends_with('/') && path[prefix.size()] != '/') continue;
    for (const auto& candidate : {host, std::string("*")}) {
      auto found = routeMap.find(candidate + prefix);
      if (found == routeMap.end() || found->second.slotPrefix != prefix || found->second.slotHost != candidate) continue;
      const auto& route = found->second;
      // A tenant's hostname is served by that tenant's routes only.
      if (owner && !route.value.workerId.empty() && tenantOf(route.value.workerId) != *owner) continue;
      selection.routeKey = found->first;
      selection.workerId = route.value.workerId;
      if (!route.error.empty()) {
        selection.kind = Selection::Kind::ROUTE_INVALID;
        selection.reason = route.error;
        return selection;
      }
      auto worker = workerMap.find(route.value.workerId);
      if (worker == workerMap.end() || !worker->second.valid) {
        selection.kind = Selection::Kind::WORKER_INVALID;
        selection.reason = route.reason;
        return selection;
      }
      selection.kind = Selection::Kind::SELECTED;
      selection.version = worker->second.version;
      selection.pins = worker->second.closure;
      return selection;
    }
  }
  return selection;
}

const DeploymentState::Worker* DeploymentState::worker(const std::string& id) const {
  auto found = workerMap.find(id);
  return found == workerMap.end() ? nullptr : &found->second;
}

bool DeploymentState::valid(const std::string& id) const {
  auto found = workerMap.find(id);
  return found != workerMap.end() && found->second.valid;
}

std::shared_ptr<const VersionDefinition> DeploymentState::definition(const std::string& label) const {
  auto found = definitionMap.find(label);
  return found == definitionMap.end() ? nullptr : found->second.version;
}

std::string DeploymentState::firstProblem(const std::string& tenant) const {
  for (const auto& id : invalidWorkers) {
    if (!tenant.empty() && tenantOf(id) != tenant) continue;
    return workerMap.at(id).reason + " (Worker " + id + ")";
  }
  for (const auto& key : invalidRoutes) {
    const auto& route = routeMap.at(key);
    // A route belongs to its owner's tenant; one without a readable owner counts for every tenant.
    if (!tenant.empty() && !route.value.workerId.empty() && tenantOf(route.value.workerId) != tenant) continue;
    return route.reason + " (route " + key + ")";
  }
  return {};
}

std::string DeploymentState::statusFields(size_t limit) const {
  std::ostringstream out;
  out << "\"workerCount\":" << workerMap.size() << ",\"validWorkerCount\":" << validWorkers
      << ",\"invalidWorkerCount\":" << invalidWorkers.size() << ",\"invalidWorkers\":[";
  size_t index = 0;
  for (const auto& id : invalidWorkers) {
    if (index == limit) break;
    if (index++) out << ',';
    const auto& worker = workerMap.at(id);
    out << "{\"workerId\":" << quote(id) << ",\"reason\":" << quote(worker.reason);
    if (!worker.missingSecrets.empty()) {
      out << ",\"missingSecrets\":[";
      for (size_t i = 0; i < worker.missingSecrets.size(); ++i) out << (i ? "," : "") << quote(worker.missingSecrets[i]);
      out << ']';
    }
    out << '}';
  }
  out << "],\"invalidWorkersTruncated\":" << (invalidWorkers.size() > limit ? "true" : "false")
      << ",\"routeCount\":" << routeMap.size() << ",\"effectiveRouteCount\":" << effectiveRoutes
      << ",\"invalidRouteCount\":" << invalidRoutes.size() << ",\"invalidRoutes\":[";
  index = 0;
  for (const auto& key : invalidRoutes) {
    if (index == limit) break;
    if (index++) out << ',';
    out << "{\"key\":" << quote(key) << ",\"reason\":" << quote(routeMap.at(key).reason) << '}';
  }
  out << "],\"invalidRoutesTruncated\":" << (invalidRoutes.size() > limit ? "true" : "false")
      << ",\"definitionCount\":" << definitionMap.size() << ",\"invalidDefinitionCount\":" << invalidDefinitions.size()
      << ",\"invalidDefinitions\":[";
  index = 0;
  for (const auto& label : invalidDefinitions) {
    if (index == limit) break;
    if (index++) out << ',';
    out << "{\"label\":" << quote(label) << ",\"reason\":" << quote(definitionMap.at(label).error) << '}';
  }
  out << "],\"unknownKeyCount\":" << unknownKeys.size() << ",\"unknownKeys\":[";
  index = 0;
  for (const auto& key : unknownKeys) {
    if (index == limit) break;
    if (index++) out << ',';
    out << quote(key);
  }
  size_t invalidNamespaceCount = 0;
  for (const auto& [key, count] : invalidNamespaceEntries) invalidNamespaceCount += count;
  out << "],\"invalidNamespaceEntryCount\":" << invalidNamespaceCount
      << ",\"binderKeyCount\":" << binderKeys.size() << ",\"generationError\":" << quote(generationError)
      << ",\"maxClosureSize\":" << maxClosureSize()
      << ",\"validSetDigest\":\"" << validSet.hex() << "\",\"effectiveRouteDigest\":\"" << routeSet.hex() << '"';
  // Only when secret keys exist (a state without secrets keeps its fields):
  // counts, and the unusable keys with what is wrong -- never a value.
  if (secretKeys) {
    out << ",\"secretKeyCount\":" << secretKeys << ",\"unusableSecretKeyCount\":" << unusableSecrets << ",\"unusableSecretKeys\":[";
    index = 0;
    for (const auto& [worker, names] : secretMap) for (const auto& [name, record] : names) {
      if (record.error.empty() || index == limit) continue;
      if (index++) out << ',';
      out << "{\"key\":" << quote(deploymentSecretKey(worker, name)) << ",\"reason\":" << quote(record.error) << '}';
    }
    out << ']';
  }
  return out.str();
}

const std::map<std::string, DurableObjectNamespace>* DeploymentState::namespaces(const std::string& workerId) const {
  auto found = namespaceMap.find(workerId);
  return found == namespaceMap.end() ? nullptr : &found->second;
}
const DurableObjectNamespace* DeploymentState::durableNamespace(const std::string& workerId, const std::string& className) const {
  const auto* all = namespaces(workerId);
  if (!all) return nullptr;
  auto found = all->find(className);
  return found == all->end() ? nullptr : &found->second;
}

const std::map<std::string, DurableObjectNamespace>* DeploymentState::databases(const std::string& tenant) const {
  auto found = databaseMap.find(tenant);
  return found == databaseMap.end() ? nullptr : &found->second;
}
const DurableObjectNamespace* DeploymentState::database(const std::string& tenant, const std::string& name) const {
  const auto* all = databases(tenant);
  if (!all) return nullptr;
  auto found = all->find(name);
  return found == all->end() ? nullptr : &found->second;
}

std::optional<DeploymentState::NamespaceOwner> DeploymentState::namespaceOwner(const std::string& namespaceId) const {
  for (const auto& [workerId, spaces] : namespaceMap) {
    for (const auto& [className, space] : spaces) {
      if (space.id != namespaceId) continue;
      return NamespaceOwner{tenantOf(workerId), workerId, className, {}, space.status, space.shards};
    }
  }
  for (const auto& [tenant, names] : databaseMap) {
    for (const auto& [name, space] : names) {
      if (space.id != namespaceId) continue;
      return NamespaceOwner{tenant, {}, DATABASE_CLASS_NAME, name, space.status, space.shards};
    }
  }
  return std::nullopt;
}

const SecretValue* DeploymentState::secret(const std::string& workerId, const std::string& name) const {
  auto worker = secretMap.find(workerId);
  if (worker == secretMap.end()) return nullptr;
  auto found = worker->second.find(name);
  return found == worker->second.end() || !found->second.error.empty() ? nullptr : &found->second;
}

std::vector<std::string> DeploymentState::missingSecrets(const VersionDefinition& version) const {
  std::vector<std::string> missing;
  if (!requireSecretValues) return missing;
  for (const auto& name : version.secrets) if (!secret(version.workerId, name)) missing.push_back(name);
  std::sort(missing.begin(), missing.end());
  return missing;
}

std::string DeploymentState::secretsStatusField(const VersionDefinition& version) const {
  if (version.secrets.empty()) return {};
  auto names = version.secrets; std::sort(names.begin(), names.end());
  std::string out = "\"secrets\":[";
  for (size_t i = 0; i < names.size(); ++i) {
    const auto* value = secret(version.workerId, names[i]);
    out += (i ? ",{\"name\":" : "{\"name\":") + quote(names[i]) + ",\"bytes\":" +
        (value ? std::to_string(value->value.size()) : std::string("null")) + '}';
  }
  return out + ']';
}

void DefinitionIndex::remember(std::shared_ptr<const VersionDefinition> version) {
  if (hashes.contains(version->definitionHash)) return;
  validateDefinition(*version);
  labels[version->key()].insert(version->definitionHash);
  hashes.emplace(version->definitionHash, std::move(version));
}

size_t DefinitionIndex::retain(const std::map<std::string, std::string>& named) {
  size_t forgotten = 0;
  for (auto entry = labels.begin(); entry != labels.end();) {
    auto found = named.find(entry->first);
    if (found != named.end()) std::erase_if(entry->second, [&](const std::string& hash) { return hash != found->second; });
    if (found == named.end() || entry->second.empty()) {
      entry = labels.erase(entry);
      ++forgotten;
    } else {
      ++entry;
    }
  }
  for (const auto& [label, hash] : named) if (hashes.contains(hash)) labels[label] = {hash};
  return forgotten;
}

std::shared_ptr<const VersionDefinition> DefinitionIndex::find(const VersionPin& value, std::string& reason) const {
  auto found = hashes.find(value.definitionHash);
  if (found != hashes.end()) {
    if (versionPin(*found->second) == value) return found->second;
    reason = "定义不一致";
  } else {
    reason = labels.contains(value.workerId + "@" + value.versionId) ? "定义不一致" : "定义未知";
  }
  return nullptr;
}

std::shared_ptr<const VersionDefinition> DefinitionIndex::byHash(const std::string& hash) const {
  auto found = hashes.find(hash);
  return found == hashes.end() ? nullptr : found->second;
}

std::shared_ptr<const VersionDefinition> DefinitionIndex::exact(const std::string& label, const std::string& hash) const {
  if (!hash.empty()) {
    auto found = hashes.find(hash);
    KJ_REQUIRE(found != hashes.end() && found->second->key() == label, "unknown Worker Version definition", label.c_str());
    return found->second;
  }
  auto known = labels.find(label);
  KJ_REQUIRE(known != labels.end(), "unknown Worker Version", label.c_str());
  KJ_REQUIRE(known->second.size() == 1, "Worker Version label names several definitions; a definition hash is required",
      label.c_str());
  return hashes.at(*known->second.begin());
}

}  // namespace coworkerd
