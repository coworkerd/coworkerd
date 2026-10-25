#include "tenant-manifest.h"
#include "object-state.h"
#include "support.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/debug.h>
#include <kj/encoding.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <sys/stat.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <set>

namespace coworkerd {
namespace {
using json::Json;
using json::field;
using json::quote;

constexpr size_t NONCE_BYTES = 12, TAG_BYTES = 16;

bool hexDigest(const std::string& value) {
  return value.size() == 64 && std::all_of(value.begin(), value.end(),
      [](unsigned char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
}

// A JSON integer below 2^53 (absent: `fallback`).
uint64_t number(Json::Reader object, const char* name, uint64_t fallback = 0) {
  auto value = field(object, name);
  if (value.isNull()) return fallback;
  KJ_REQUIRE(value.isNumber() && std::isfinite(value.getNumber()) && value.getNumber() >= 0 &&
      value.getNumber() < 9007199254740992.0 && std::floor(value.getNumber()) == value.getNumber(),
      "expected a non-negative integer", name);
  return static_cast<uint64_t>(value.getNumber());
}
std::string string(Json::Reader object, const char* name) {
  auto value = field(object, name);
  KJ_REQUIRE(value.isString(), "expected a string", name);
  return value.getString().cStr();
}
template <typename F>
auto decoded(const std::string& bytes, F&& read) {
  capnp::MallocMessageBuilder message;
  auto json = message.initRoot<Json>();
  capnp::JsonCodec codec;
  codec.setMaxNestingDepth(32);
  codec.decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), json);
  KJ_REQUIRE(json.isObject(), "expected a JSON object");
  return read(json.asReader());
}
std::string encodeRaw(Json::Reader value) { return capnp::JsonCodec().encodeRaw(value).cStr(); }

std::string randomHex(size_t bytes) {
  std::string raw(bytes, '\0');
  KJ_REQUIRE(RAND_bytes(reinterpret_cast<unsigned char*>(raw.data()), static_cast<int>(bytes)) == 1, "cannot draw random bytes");
  constexpr char alphabet[] = "0123456789abcdef";
  std::string out;
  for (unsigned char c : raw) { out += alphabet[c >> 4]; out += alphabet[c & 15]; }
  return out;
}
// The object's writer, when it names one (objects written before writers named themselves do not).
WriterIdentity readWriter(Json::Reader object) {
  WriterIdentity writer;
  auto value = field(object, "writer");
  if (value.isNull()) return writer;
  KJ_REQUIRE(value.isObject(), "writer is an object");
  writer.instance = string(value, "instance");
  writer.nonce = string(value, "nonce");
  writer.writtenAtUnixMs = number(value, "writtenAtUnixMs");
  return writer;
}

std::string base64(const unsigned char* bytes, size_t size) {
  return kj::encodeBase64(kj::arrayPtr(reinterpret_cast<const kj::byte*>(bytes), size)).cStr();
}
std::optional<std::string> unbase64(const std::string& text) {
  auto result = kj::decodeBase64(kj::arrayPtr(text.data(), text.size()));
  if (result.hadErrors) return std::nullopt;
  return std::string(reinterpret_cast<const char*>(result.begin()), result.size());
}

std::string hostnamesJson(const std::vector<std::string>& hostnames) {
  std::string out = "[";
  for (size_t i = 0; i < hostnames.size(); ++i) out += (i ? "," : "") + quote(hostnames[i]);
  return out + ']';
}
std::vector<std::string> readHostnames(Json::Reader value) {
  std::vector<std::string> hosts;
  if (value.isNull()) return hosts;
  KJ_REQUIRE(value.isArray(), "hostnames is a list");
  for (auto host : value.getArray()) {
    KJ_REQUIRE(host.isString() && hostnameValid(host.getString().cStr()), "invalid hostname");
    hosts.emplace_back(host.getString().cStr());
  }
  return hosts;
}
}  // namespace

std::string newWriterInstance() { return randomHex(16); }
std::string newNamespaceId() { return randomHex(32); }
WriterIdentity newWriter(const std::string& instance) { return {instance, randomHex(8), realClockUnixMs()}; }
std::string writerJson(const WriterIdentity& writer) {
  return "{\"instance\":" + quote(writer.instance) + ",\"writtenAtUnixMs\":" + std::to_string(writer.writtenAtUnixMs) +
      ",\"nonce\":" + quote(writer.nonce) + "}";
}
std::string clusterFormatBytes(const WriterIdentity& writer) {
  return "{\"format\":1,\"kind\":\"coworkerd-cluster\",\"writer\":" + writerJson(writer) + "}\n";
}
std::string indexHintKey(const std::string& prefix) { return prefix + "/current"; }
std::string tenantManifestPrefix(const std::string& tenant) { return std::string(TENANT_OBJECTS_PREFIX) + tenant + "/manifest"; }
std::string tenantHintKey(const std::string& tenant) { return std::string(TENANT_OBJECTS_PREFIX) + tenant + "/current"; }
std::string tenantObjectsPrefix(const std::string& tenant) { return std::string(TENANT_OBJECTS_PREFIX) + tenant + "/"; }
std::string hintJson(uint64_t revision) { return "{\"rev\":" + std::to_string(revision) + "}"; }
std::optional<uint64_t> parseHint(const std::string& bytes) {
  try {
    return decoded(bytes, [](Json::Reader json) -> std::optional<uint64_t> {
      const auto rev = number(json, "rev");
      if (rev == 0) return std::nullopt;
      return rev;
    });
  } catch (const kj::Exception&) {
    return std::nullopt;
  }
}

// ---- The tenant index ----
std::string tenantIndexJson(const TenantIndex& index) {
  std::string out = "{\"revision\":" + std::to_string(index.revision) + ",\"writtenAtUnixMs\":" +
      std::to_string(index.writtenAtUnixMs) + ",\"writer\":" + writerJson(index.writer) + ",\"tenants\":{";
  size_t i = 0;
  for (const auto& [name, entry] : index.tenants) {
    out += (i++ ? "," : "") + quote(name) + ":{\"status\":" + quote(entry.status) + ",\"hostnames\":" +
        hostnamesJson(entry.hostnames) + ",\"manifestRev\":" + std::to_string(entry.manifestRev) + "}";
  }
  return out + "}}";
}
TenantIndex decodeTenantIndex(const std::string& bytes) {
  return decoded(bytes, [](Json::Reader json) {
    TenantIndex index;
    index.revision = number(json, "revision");
    index.writtenAtUnixMs = number(json, "writtenAtUnixMs");
    index.writer = readWriter(json);
    auto tenants = field(json, "tenants");
    KJ_REQUIRE(tenants.isObject(), "the tenant index has no tenants object");
    for (auto entry : tenants.getObject()) {
      const std::string name = entry.getName().cStr();
      KJ_REQUIRE(tenantName(name), "the tenant index names an invalid tenant");
      auto value = entry.getValue();
      TenantIndexEntry item;
      item.status = string(value, "status");
      KJ_REQUIRE(item.status == "active" || item.status == "deleting", "invalid tenant status in the index");
      item.hostnames = readHostnames(field(value, "hostnames"));
      item.manifestRev = number(value, "manifestRev");
      KJ_REQUIRE(item.manifestRev > 0, "the tenant index names no manifest revision");
      index.tenants.emplace(name, std::move(item));
    }
    return index;
  });
}

// ---- A tenant's manifest ----
std::string tenantManifestJson(const TenantManifest& manifest) {
  std::string out = "{\"tenant\":" + quote(manifest.tenant) + ",\"revision\":" + std::to_string(manifest.revision) +
      ",\"generation\":" + std::to_string(manifest.generation) + ",\"writtenAtUnixMs\":" +
      std::to_string(manifest.writtenAtUnixMs) + ",\"writer\":" + writerJson(manifest.writer) +
      ",\"record\":" + (manifest.record ? tenantRecordJson(*manifest.record) : "null") +
      ",\"workers\":{";
  size_t w = 0;
  for (const auto& [name, worker] : manifest.workers) {
    out += (w++ ? "," : "") + quote(name) + ":{";
    if (worker.active) {
      out += "\"active\":{\"versionId\":" + quote(worker.active->versionId) + ",\"definitionHash\":" +
          quote(worker.active->definitionHash) + ",\"limits\":" + workerLimitsJson(worker.active->limits) + "},";
    }
    out += "\"versions\":{";
    size_t v = 0;
    for (const auto& [id, version] : worker.versions) {
      out += (v++ ? "," : "") + quote(id) + ":{\"definitionHash\":" + quote(version.definitionHash) +
          ",\"uploadedRevision\":" + std::to_string(version.uploadedRevision) + "}";
    }
    out += "},\"routes\":[";
    for (size_t r = 0; r < worker.routes.size(); ++r) {
      out += (r ? "," : "") + std::string("{\"host\":") + quote(worker.routes[r].host) + ",\"pathPrefix\":" +
          quote(worker.routes[r].pathPrefix) + "}";
    }
    out += "],\"secrets\":{";
    size_t s = 0;
    for (const auto& [secret, sealed] : worker.secrets) {
      out += (s++ ? "," : "") + quote(secret) + ":{\"nonce\":" + quote(sealed.nonce) + ",\"ciphertext\":" +
          quote(sealed.ciphertext) + ",\"setAtUnixMs\":" + std::to_string(sealed.setAtUnixMs) + "}";
    }
    out += '}';
    // Durable Object namespaces only where a Worker has some: a manifest without them keeps its shape.
    if (!worker.namespaces.empty()) {
      out += ",\"namespaces\":{";
      size_t n = 0;
      for (const auto& [className, entry] : worker.namespaces)
        out += (n++ ? "," : "") + quote(className) + ":" + manifestNamespaceJson(entry);
      out += '}';
    }
    out += '}';
  }
  return out + "}}";
}

std::string manifestNamespaceJson(const ManifestNamespace& entry) {
  return "{\"id\":" + quote(entry.id) + ",\"createdRevision\":" + std::to_string(entry.createdRevision) + ",\"status\":" +
      quote(entry.status) + ",\"shards\":" + std::to_string(entry.shards) + ",\"tier\":" + quote(entry.tier) + "}";
}

TenantManifest decodeTenantManifest(const std::string& bytes) {
  return decoded(bytes, [](Json::Reader json) {
    TenantManifest manifest;
    manifest.tenant = string(json, "tenant");
    KJ_REQUIRE(tenantName(manifest.tenant), "a manifest names an invalid tenant");
    manifest.revision = number(json, "revision");
    manifest.generation = number(json, "generation");
    manifest.writtenAtUnixMs = number(json, "writtenAtUnixMs");
    manifest.writer = readWriter(json);
    if (auto record = field(json, "record"); !record.isNull()) {
      manifest.record = decodeTenantRecord(encodeRaw(record));
      KJ_REQUIRE(manifest.record->name == manifest.tenant, "a manifest's record names another tenant");
    }
    auto workers = field(json, "workers");
    KJ_REQUIRE(workers.isObject(), "a manifest has no workers object");
    for (auto entry : workers.getObject()) {
      const std::string name = entry.getName().cStr();
      KJ_REQUIRE(deploymentSafeName(name) && workerFullName(workerFullId(manifest.tenant, name)), "a manifest names an invalid Worker");
      auto value = entry.getValue();
      ManifestWorker worker;
      if (auto active = field(value, "active"); !active.isNull()) {
        ManifestPointer pointer{string(active, "versionId"), string(active, "definitionHash"), {}};
        KJ_REQUIRE(deploymentSafeName(pointer.versionId) && hexDigest(pointer.definitionHash), "invalid active pointer");
        if (auto limits = field(active, "limits"); !limits.isNull()) pointer.limits = readWorkerLimits(limits);
        worker.active = pointer;
      }
      if (auto versions = field(value, "versions"); !versions.isNull()) {
        KJ_REQUIRE(versions.isObject(), "versions is an object");
        for (auto version : versions.getObject()) {
          const std::string id = version.getName().cStr();
          ManifestVersion item{string(version.getValue(), "definitionHash"), number(version.getValue(), "uploadedRevision")};
          KJ_REQUIRE(deploymentSafeName(id) && hexDigest(item.definitionHash), "invalid version entry");
          worker.versions.emplace(id, std::move(item));
        }
      }
      if (auto routes = field(value, "routes"); !routes.isNull()) {
        KJ_REQUIRE(routes.isArray(), "routes is a list");
        for (auto route : routes.getArray()) {
          ManifestRoute item{string(route, "host"), string(route, "pathPrefix")};
          KJ_REQUIRE(!item.host.empty() && item.host.find('/') == std::string::npos && item.pathPrefix.starts_with('/'),
              "invalid route");
          worker.routes.push_back(std::move(item));
        }
        std::sort(worker.routes.begin(), worker.routes.end());
        worker.routes.erase(std::unique(worker.routes.begin(), worker.routes.end()), worker.routes.end());
      }
      if (auto secrets = field(value, "secrets"); !secrets.isNull()) {
        KJ_REQUIRE(secrets.isObject(), "secrets is an object");
        for (auto secret : secrets.getObject()) {
          const std::string secretName = secret.getName().cStr();
          KJ_REQUIRE(environmentName(secretName), "invalid secret name");
          // A sealed record that does not read is kept as it is: the Worker that declares it is not valid (no value).
          ManifestSecret sealed;
          auto record = secret.getValue();
          if (record.isObject()) {
            if (auto nonce = field(record, "nonce"); nonce.isString()) sealed.nonce = nonce.getString().cStr();
            if (auto text = field(record, "ciphertext"); text.isString()) sealed.ciphertext = text.getString().cStr();
            try { sealed.setAtUnixMs = number(record, "setAtUnixMs"); } catch (const kj::Exception&) {}
          }
          worker.secrets.emplace(secretName, std::move(sealed));
        }
      }
      if (auto namespaces = field(value, "namespaces"); !namespaces.isNull()) {
        KJ_REQUIRE(namespaces.isObject(), "namespaces is an object");
        for (auto entry : namespaces.getObject()) {
          const std::string className = entry.getName().cStr();
          KJ_REQUIRE(environmentName(className), "invalid Durable Object class name");
          auto record = entry.getValue();
          ManifestNamespace item;
          item.id = string(record, "id");
          item.createdRevision = number(record, "createdRevision");
          item.status = string(record, "status");
          item.shards = static_cast<uint32_t>(number(record, "shards"));
          item.tier = string(record, "tier");
          KJ_REQUIRE(hexDigest(item.id), "a namespace id is 64 hexadecimal digits");
          KJ_REQUIRE(item.status == "active" || item.status == "deleting", "a namespace's status is active or deleting");
          KJ_REQUIRE(item.shards > 0 && !item.tier.empty(), "a namespace has shards and a tier");
          worker.namespaces.emplace(className, std::move(item));
        }
      }
      manifest.workers.emplace(name, std::move(worker));
    }
    return manifest;
  });
}

bool sameContent(const TenantManifest& a, const TenantManifest& b) {
  const auto record = [](const TenantManifest& m) { return m.record ? tenantRecordJson(*m.record) : std::string("null"); };
  return a.tenant == b.tenant && record(a) == record(b) && a.workers == b.workers;
}

TenantIndexEntry indexEntryOf(const TenantManifest& manifest) {
  return {manifest.status(), manifest.hostnames(), manifest.revision};
}

// ---- The credentials index ----
std::string credentialIndexJson(const CredentialIndex& index) {
  std::string out = "{\"revision\":" + std::to_string(index.revision) + ",\"writtenAtUnixMs\":" +
      std::to_string(index.writtenAtUnixMs) + ",\"writer\":" + writerJson(index.writer) + ",\"credentials\":{";
  size_t i = 0;
  for (const auto& [keyId, record] : index.credentials) {
    auto value = credentialRecordJson(record);
    // The tenant beside its scope, for readers that look a credential's tenant up by name.
    value = "{\"tenant\":" + quote(record.scope.substr(7)) + "," + value.substr(1);
    out += (i++ ? "," : "") + quote(keyId) + ":" + value;
  }
  return out + "}}";
}
CredentialIndex decodeCredentialIndex(const std::string& bytes) {
  return decoded(bytes, [](Json::Reader json) {
    CredentialIndex index;
    index.revision = number(json, "revision");
    index.writtenAtUnixMs = number(json, "writtenAtUnixMs");
    index.writer = readWriter(json);
    auto credentials = field(json, "credentials");
    KJ_REQUIRE(credentials.isObject(), "the credentials index has no credentials object");
    for (auto entry : credentials.getObject()) {
      auto record = decodeCredentialRecord(encodeRaw(entry.getValue()));
      KJ_REQUIRE(record.keyId == entry.getName().cStr(), "a credential's key id differs from its entry");
      index.credentials.emplace(record.keyId, std::move(record));
    }
    return index;
  });
}

// ---- Secrets at rest ----
SecretsKey SecretsKey::load(const std::filesystem::path& file) {
  struct stat info{};
  KJ_REQUIRE(::stat(file.c_str(), &info) == 0, "cannot read the secrets key file (secrets.keyFile)", file.string().c_str());
  KJ_REQUIRE(S_ISREG(info.st_mode) && (info.st_mode & 077) == 0,
      "the secrets key file must be a regular file readable by its owner only (mode 0600)", file.string().c_str());
  std::string text;
  try { text = readFile(file); } catch (const kj::Exception&) {
    KJ_FAIL_REQUIRE("cannot read the secrets key file (secrets.keyFile)", file.string().c_str());
  }
  std::string hex;
  try {
    hex = decoded(text, [](Json::Reader json) {
      KJ_REQUIRE(json.getObject().size() == 1, "one field");
      return string(json, "key");
    });
  } catch (const kj::Exception&) {
    KJ_FAIL_REQUIRE("the secrets key file must be {\"key\":\"<64 hexadecimal digits>\"}", file.string().c_str());
  }
  KJ_REQUIRE(hexDigest(hex), "the secrets key file must be {\"key\":\"<64 hexadecimal digits>\"} (32 bytes)", file.string().c_str());
  SecretsKey key;
  for (size_t i = 0; i < 32; ++i) key.key[i] = static_cast<unsigned char>(std::stoul(hex.substr(i * 2, 2), nullptr, 16));
  hex.assign(hex.size(), '0');
  text.assign(text.size(), '0');
  key.print = sha256Hex(std::string("coworkerd-secrets-key") + std::string(reinterpret_cast<const char*>(key.key.data()), 32)).substr(0, 12);
  return key;
}

std::string SecretsKey::generateJson() {
  std::array<unsigned char, 32> bytes{};
  KJ_REQUIRE(RAND_bytes(bytes.data(), bytes.size()) == 1, "cannot draw a secrets key");
  std::string out = "{\"key\":\"";
  for (auto byte : bytes) { out += "0123456789abcdef"[byte >> 4]; out += "0123456789abcdef"[byte & 15]; }
  return out + "\"}\n";
}

std::string secretAad(const std::string& tenant, const std::string& worker, const std::string& name) {
  return tenant + "/" + worker + "/" + name;
}

ManifestSecret SecretsKey::seal(const std::string& aad, const std::string& plaintext, uint64_t setAtUnixMs) const {
  std::array<unsigned char, NONCE_BYTES> nonce{};
  KJ_REQUIRE(RAND_bytes(nonce.data(), nonce.size()) == 1, "cannot draw a nonce");
  std::string out(plaintext.size() + TAG_BYTES, '\0');
  auto* ctx = EVP_CIPHER_CTX_new();
  KJ_REQUIRE(ctx != nullptr, "cannot seal a secret");
  KJ_DEFER(EVP_CIPHER_CTX_free(ctx));
  int length = 0, final = 0;
  auto* bytes = reinterpret_cast<unsigned char*>(out.data());
  KJ_REQUIRE(EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) == 1 &&
      EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, NONCE_BYTES, nullptr) == 1 &&
      EVP_EncryptInit_ex(ctx, nullptr, nullptr, key.data(), nonce.data()) == 1 &&
      EVP_EncryptUpdate(ctx, nullptr, &length, reinterpret_cast<const unsigned char*>(aad.data()), static_cast<int>(aad.size())) == 1 &&
      EVP_EncryptUpdate(ctx, bytes, &length, reinterpret_cast<const unsigned char*>(plaintext.data()),
          static_cast<int>(plaintext.size())) == 1 &&
      EVP_EncryptFinal_ex(ctx, bytes + length, &final) == 1 &&
      EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, TAG_BYTES, bytes + plaintext.size()) == 1,
      "cannot seal a secret");
  ManifestSecret sealed{base64(nonce.data(), nonce.size()), base64(bytes, out.size()), setAtUnixMs};
  std::fill(out.begin(), out.end(), '\0');
  return sealed;
}

std::optional<std::string> SecretsKey::open(const std::string& aad, const ManifestSecret& sealed) const {
  auto nonce = unbase64(sealed.nonce), text = unbase64(sealed.ciphertext);
  if (!nonce || !text || nonce->size() != NONCE_BYTES || text->size() < TAG_BYTES) return std::nullopt;
  const size_t size = text->size() - TAG_BYTES;
  std::string plain(size, '\0');
  auto* ctx = EVP_CIPHER_CTX_new();
  if (ctx == nullptr) return std::nullopt;
  KJ_DEFER(EVP_CIPHER_CTX_free(ctx));
  int length = 0, final = 0;
  auto* tag = reinterpret_cast<unsigned char*>(text->data()) + size;
  const bool opened = EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) == 1 &&
      EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, NONCE_BYTES, nullptr) == 1 &&
      EVP_DecryptInit_ex(ctx, nullptr, nullptr, key.data(), reinterpret_cast<const unsigned char*>(nonce->data())) == 1 &&
      EVP_DecryptUpdate(ctx, nullptr, &length, reinterpret_cast<const unsigned char*>(aad.data()), static_cast<int>(aad.size())) == 1 &&
      EVP_DecryptUpdate(ctx, reinterpret_cast<unsigned char*>(plain.data()), &length,
          reinterpret_cast<const unsigned char*>(text->data()), static_cast<int>(size)) == 1 &&
      EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, TAG_BYTES, tag) == 1 &&
      EVP_DecryptFinal_ex(ctx, reinterpret_cast<unsigned char*>(plain.data()) + length, &final) == 1;
  if (!opened) {
    std::fill(plain.begin(), plain.end(), '\0');
    return std::nullopt;
  }
  return plain;
}

std::optional<size_t> sealedLength(const ManifestSecret& sealed) {
  auto text = unbase64(sealed.ciphertext);
  if (!text || text->size() < TAG_BYTES) return std::nullopt;
  return text->size() - TAG_BYTES;
}

// ---- Into the deployment state ----
std::map<std::string, std::string> tenantStateKeys(const TenantManifest& manifest, const DefinitionBytes& definitions,
    const SecretOpener& open) {
  std::map<std::string, std::string> keys;
  const auto& tenant = manifest.tenant;
  for (const auto& [name, worker] : manifest.workers) {
    const auto id = workerFullId(tenant, name);
    if (worker.active) {
      keys.emplace(deploymentActiveKey(id), activePointerJson(worker.active->versionId, worker.active->definitionHash,
          worker.active->limits));
    }
    for (const auto& [versionId, version] : worker.versions) {
      auto found = definitions.find(version.definitionHash);
      if (found != definitions.end()) keys.emplace(deploymentDefinitionKey(id, versionId), found->second);
    }
    for (const auto& [secret, sealed] : worker.secrets) {
      auto value = open ? open(tenant, name, secret, sealed) : std::nullopt;
      // A value that does not open has no "value": the deployment state counts it unusable, names only.
      keys.emplace(deploymentSecretKey(id, secret), value ? secretRecordJson(*value, sealed.setAtUnixMs)
          : "{\"setAtUnixMs\":" + std::to_string(sealed.setAtUnixMs) + "}");
    }
    if (!worker.namespaces.empty()) {
      std::map<std::string, DurableObjectNamespace> namespaces;
      for (const auto& [className, entry] : worker.namespaces) namespaces.emplace(className, DurableObjectNamespace{entry.id, entry.status});
      keys.emplace(deploymentNamespacesKey(id), namespacesStateJson(namespaces));
    }
  }
  return keys;
}

std::map<std::string, std::string> routeStateKeys(const ManifestView& tenants, size_t* conflicts) {
  std::map<std::string, std::string> keys;
  size_t clashes = 0;
  for (const auto& [tenant, manifest] : tenants) {
    for (const auto& [name, worker] : manifest->workers) {
      for (const auto& route : worker.routes) {
        const auto key = deploymentRouteKey(route.host, route.pathPrefix);
        auto [entry, added] = keys.emplace(key, routeValueJson({route.host, route.pathPrefix, workerFullId(tenant, name)}));
        if (!added) ++clashes;
      }
    }
  }
  if (conflicts) *conflicts = clashes;
  return keys;
}

std::string generationValue(const ManifestView& tenants) {
  uint64_t sum = 0;
  for (const auto& [tenant, manifest] : tenants) sum += manifest->generation;
  return quote(std::to_string(sum));
}

std::set<std::string> namedDefinitions(const TenantManifest& manifest) {
  std::set<std::string> hashes;
  for (const auto& [name, worker] : manifest.workers) {
    if (worker.active) hashes.insert(worker.active->definitionHash);
    for (const auto& [id, version] : worker.versions) hashes.insert(version.definitionHash);
  }
  return hashes;
}

bool definitionMatches(const std::string& hash, const std::string& bytes) {
  try {
    auto version = decodeDeploymentDefinition(bytes);
    return version->definitionHash == hash && deploymentSha256(definitionEncoding(*version)) == hash;
  } catch (const kj::Exception&) {
    return false;
  }
}

}  // namespace coworkerd
