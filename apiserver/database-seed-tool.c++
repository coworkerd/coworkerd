#include "database-seed-tool.h"
#include "manifests.h"
#include "deployment.h"
#include "json.h"
#include <capnp/message.h>
#include <kj/debug.h>
#include <openssl/evp.h>
#include <algorithm>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace coworkerd {
namespace {
using json::quote;
std::string wide(uint64_t value) { std::ostringstream out; out << std::setw(20) << std::setfill('0') << value; return out.str(); }
bool identifier(const std::string& value) {
  return value.size() == 64 && std::all_of(value.begin(), value.end(), [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
}
std::string databaseObject(const std::string& ns) {
  auto digit = [](char c) { return c <= '9' ? c - '0' : c - 'a' + 10; }; std::string bytes;
  for (size_t at = 0; at < ns.size(); at += 2) bytes += char(digit(ns[at]) * 16 + digit(ns[at + 1]));
  return objectSha256(bytes + "name" + DATABASE_OBJECT_NAME);
}
kj::Promise<uint64_t> verifyFirstManifest(ObjectStore& store, const std::string& ns) {
  const auto objectId = databaseObject(ns); const auto prefix = "do/" + ns + "/objects/" + objectId + "/";
  auto response = co_await store.get(prefix + "manifest/" + wide(1));
  KJ_REQUIRE(response.status == 200 && response.bytes.size() <= (4u << 20), "destination manifest 1 is not published");
  capnp::MallocMessageBuilder message; auto root = message.initRoot<capnp::JsonValue>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.bytes.data(), response.bytes.size()), root);
  KJ_REQUIRE(json::text(root, "namespaceId") == ns && json::text(root, "objectId") == objectId && json::integer(root, "revision") == 1,
      "destination first-manifest identity differs");
  auto snapshot = json::field(root, "snapshot"); KJ_REQUIRE(snapshot.isObject(), "destination has no first snapshot");
  KJ_REQUIRE(json::field(root, "segments").getArray().size() == 0 && json::field(root, "isolated").getArray().size() == 0,
      "first seed manifest must contain only the target snapshot");
  auto facets = json::field(snapshot, "facets").getArray(); KJ_REQUIRE(facets.size() == 1, "destination is not a root database");
  auto facet = facets[0]; auto id = json::field(facet, "facet"), parent = json::field(facet, "parent");
  KJ_REQUIRE(id.isNumber() && id.getNumber() == 0 && parent.isNumber() && parent.getNumber() == 0 && json::integer(facet, "upToSeq") == 0,
      "destination first snapshot has noninitial database accounting");
  const auto bytes = json::integer(facet, "bytes"), parts = json::integer(facet, "parts"), sequence = json::integer(snapshot, "sequence");
  const auto hash = json::text(facet, "sha256"); const auto key = prefix + "snapshot/" + wide(sequence) + "/";
  auto index = co_await store.get(key + "index"); KJ_REQUIRE(index.status == 200 && index.bytes.size() <= (4u << 20), "destination snapshot index is absent");
  capnp::MallocMessageBuilder indexMessage; auto indexed = indexMessage.initRoot<capnp::JsonValue>(); capnp::JsonCodec().decodeRaw(kj::arrayPtr(index.bytes.data(), index.bytes.size()), indexed);
  auto indexedFacets = json::field(indexed, "facets").getArray();
  KJ_REQUIRE(json::integer(indexed, "sequence") == sequence && json::integer(indexed, "bytes") == bytes && indexedFacets.size() == 1 &&
      json::integer(indexedFacets[0], "bytes") == bytes && json::integer(indexedFacets[0], "parts") == parts && json::text(indexedFacets[0], "sha256") == hash,
      "destination snapshot index differs from manifest 1");
  const auto partBytes = json::integer(indexed, "partBytes"); KJ_REQUIRE(partBytes > 0 && partBytes <= (64u << 20) && parts == (bytes + partBytes - 1) / partBytes,
      "destination snapshot partition is invalid");
  auto* digest = EVP_MD_CTX_new(); KJ_REQUIRE(digest && EVP_DigestInit_ex(digest, EVP_sha256(), nullptr) == 1, "cannot verify seed image");
  auto release = kj::defer([&]() { EVP_MD_CTX_free(digest); }); uint64_t read = 0;
  for (uint64_t part = 0; part < parts; ++part) {
    auto body = co_await store.get(key + wide(0) + "-" + wide(part));
    KJ_REQUIRE(body.status == 200 && body.bytes.size() == std::min(partBytes, bytes - read), "destination snapshot part is absent or has wrong length");
    KJ_REQUIRE(EVP_DigestUpdate(digest, body.bytes.data(), body.bytes.size()) == 1, "cannot hash destination part"); read += body.bytes.size();
  }
  unsigned char value[32]; unsigned int length; KJ_REQUIRE(EVP_DigestFinal_ex(digest, value, &length) == 1 && length == 32, "cannot finish destination digest");
  constexpr char HEX[] = "0123456789abcdef"; std::string actual; for (auto byte : value) { actual += HEX[byte >> 4]; actual += HEX[byte & 15]; }
  KJ_REQUIRE(read == bytes && actual == hash && json::integer(root, "databaseBytes") == bytes, "destination image differs from first manifest"); co_return bytes;
}
kj::Promise<uint64_t> reportedTenantBytes(ObjectStore& objects, const std::string& tenant) {
  uint64_t total = 0; auto listing = co_await objects.list("nodes/");
  for (const auto& entry : listing.objects) if (entry.key.ends_with("/durable-usage")) {
    auto response = co_await objects.get(entry.key); if (response.status != 200) continue;
    capnp::MallocMessageBuilder message; auto value = message.initRoot<capnp::JsonValue>(); capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.bytes.data(), response.bytes.size()), value);
    auto tenants = json::field(value, "tenants"); if (tenants.isObject()) total += json::integer(tenants, tenant.c_str());
  }
  co_return total;
}
}
int runDatabaseSeedRegister(int argc, char** argv) {
  ObjectStoreConfig config; std::string tenant, name, ns;
  if (auto value = std::getenv("COWORKERD_STORAGE_ENDPOINT")) config.endpoint = value;
  if (auto value = std::getenv("COWORKERD_STORAGE_BUCKET")) config.bucket = value;
  for (int at = 0; at < argc; at += 2) {
    KJ_REQUIRE(at + 1 < argc, "missing database-register option value"); const std::string option = argv[at], value = argv[at + 1];
    if (option == "--tenant") tenant = value; else if (option == "--name") name = value; else if (option == "--namespace") ns = value;
    else if (option == "--storage-endpoint") config.endpoint = value; else if (option == "--storage-bucket") config.bucket = value;
    else if (option == "--storage-region") config.region = value; else KJ_FAIL_REQUIRE("unknown database-register option", option.c_str());
  }
  KJ_REQUIRE(tenantName(tenant) && tenantName(name) && identifier(ns), "database-register requires --tenant, --name, and --namespace");
  auto io = kj::setupAsyncIo(); ObjectStore objects(config, io.provider->getNetwork(), io.provider->getTimer());
  ManifestStore state(objects, io.provider->getTimer(), std::nullopt, std::cerr); state.load(false, false).wait(io.waitScope);
  const auto bytes = verifyFirstManifest(objects, ns).wait(io.waitScope); const auto reported = reportedTenantBytes(objects, tenant).wait(io.waitScope);
  for (const auto& [owner, manifest] : state.tenants()) {
    for (const auto& [database, entry] : manifest.databases) KJ_REQUIRE(entry.id != ns, "namespace is already registered");
    for (const auto& [worker, entry] : manifest.workers) for (const auto& [className, space] : entry.namespaces) KJ_REQUIRE(space.id != ns, "namespace belongs to a Worker");
  }
  auto written = state.writeTenant(tenant, [&](TenantManifest& next) -> kj::Promise<void> {
    KJ_REQUIRE(next.record.has_value() && next.record->status == "active", "tenant is not active");
    KJ_REQUIRE(!next.databases.contains(name), "database name already exists");
    for (const auto& [existing, space] : next.databases) KJ_REQUIRE(space.id != ns, "namespace is already registered");
    size_t namespaces = next.databases.size() + 1;
    for (const auto& [worker, entry] : next.workers) namespaces += entry.namespaces.size();
    KJ_REQUIRE(namespaces <= next.record->quotas.durableObjectNamespaces, "durableObjectNamespaces quota exceeded");
    KJ_REQUIRE(!next.record->quotas.durableObjectBytes || (reported <= next.record->quotas.durableObjectBytes && bytes <= next.record->quotas.durableObjectBytes - reported),
        "durableObjectBytes quota exceeded");
    next.databases.emplace(name, ManifestNamespace{ns, next.revision + 1, "active", 1}); co_return;
  }, false).wait(io.waitScope);
  std::cout << "{\"ok\":true,\"tenant\":" << quote(tenant) << ",\"database\":" << quote(name) << ",\"namespaceId\":" << quote(ns)
      << ",\"revision\":" << written.manifest.revision << ",\"bytes\":\"" << bytes << "\"}" << std::endl; return 0;
}
} // namespace coworkerd
