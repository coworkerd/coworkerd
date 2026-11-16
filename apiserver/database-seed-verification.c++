#include "database-time-travel.h"
#include "manifests.h"
#include "deployment.h"
#include "json.h"
#include <capnp/message.h>
#include <kj/debug.h>
#include <algorithm>
#include <openssl/evp.h>
#include <cmath>
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
uint64_t integer(capnp::JsonValue::Reader object, const char* name) {
  auto value = json::field(object, name);
  if (!value.isNumber()) return json::integer(object, name);
  const auto n = value.getNumber();
  KJ_REQUIRE(std::isfinite(n) && n >= 0 && n < 9007199254740992.0 && std::floor(n) == n, "invalid snapshot integer");
  return uint64_t(n);
}
std::string databaseObject(const std::string& ns) {
  auto digit = [](char c) { return c <= '9' ? c - '0' : c - 'a' + 10; }; std::string bytes;
  for (size_t at = 0; at < ns.size(); at += 2) bytes += char(digit(ns[at]) * 16 + digit(ns[at + 1]));
  return objectSha256(bytes + "name" + DATABASE_OBJECT_NAME);
}
} // namespace
kj::Promise<uint64_t> verifySeedFirstManifest(ObjectStore& store, const std::string& ns, const std::string& expectedSha256, bool verifyContent) {
  const auto objectId = databaseObject(ns); const auto prefix = "do/" + ns + "/objects/" + objectId + "/";
  auto response = co_await store.get(prefix + "manifest/" + wide(1));
  KJ_REQUIRE(response.status == 200 && response.bytes.size() <= (4u << 20), "destination manifest 1 is not published");
  if (!expectedSha256.empty()) KJ_REQUIRE(objectSha256(response.bytes) == expectedSha256, "destination manifest 1 digest differs");
  auto later = co_await store.get(prefix + "manifest/" + wide(2));
  requireStorage(later.status == 200 || later.status == 404, "cannot check target use");
  KJ_REQUIRE(later.status == 404, "destination already exists and is in use");
  capnp::MallocMessageBuilder message; auto root = message.initRoot<capnp::JsonValue>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.bytes.data(), response.bytes.size()), root);
  KJ_REQUIRE(json::text(root, "namespaceId") == ns && json::text(root, "objectId") == objectId && integer(root, "revision") == 1,
      "destination first-manifest identity differs");
  auto snapshot = json::field(root, "snapshot"); KJ_REQUIRE(snapshot.isObject(), "destination has no first snapshot");
  KJ_REQUIRE(json::field(root, "segments").getArray().size() == 0 && json::field(root, "isolated").getArray().size() == 0,
      "first seed manifest must contain only the target snapshot");
  auto facets = json::field(snapshot, "facets").getArray(); KJ_REQUIRE(facets.size() == 1, "destination is not a root database");
  auto facet = facets[0]; auto id = json::field(facet, "facet"), parent = json::field(facet, "parent");
  KJ_REQUIRE(id.isNumber() && id.getNumber() == 0 && parent.isNumber() && parent.getNumber() == 0 && integer(facet, "upToSeq") == 0,
      "destination first snapshot has noninitial database accounting");
  const auto bytes = integer(facet, "bytes"), parts = integer(facet, "parts"), sequence = integer(snapshot, "sequence");
  const auto hash = json::text(facet, "sha256"); const auto key = prefix + "snapshot/" + wide(sequence) + "/";
  auto index = co_await store.get(key + "index"); KJ_REQUIRE(index.status == 200 && index.bytes.size() <= (4u << 20), "destination snapshot index is absent");
  capnp::MallocMessageBuilder indexMessage; auto indexed = indexMessage.initRoot<capnp::JsonValue>(); capnp::JsonCodec().decodeRaw(kj::arrayPtr(index.bytes.data(), index.bytes.size()), indexed);
  auto indexedFacets = json::field(indexed, "facets").getArray();
  KJ_REQUIRE(integer(indexed, "sequence") == sequence && integer(indexed, "bytes") == bytes && indexedFacets.size() == 1 &&
      integer(indexedFacets[0], "bytes") == bytes && integer(indexedFacets[0], "parts") == parts && json::text(indexedFacets[0], "sha256") == hash,
      "destination snapshot index differs from manifest 1");
  const auto partBytes = integer(indexed, "partBytes"); KJ_REQUIRE(partBytes > 0 && partBytes <= (64u << 20) && parts == (bytes + partBytes - 1) / partBytes,
      "destination snapshot partition is invalid");
  KJ_REQUIRE(identifier(hash) && integer(root, "shard") == 0 && integer(root, "shardEpoch") > 0 &&
      integer(snapshot, "bytes") == bytes && integer(snapshot, "parts") == parts && sequence > 0 &&
      integer(root, "databaseBytes") == bytes && json::text(facet, "name").empty(), "destination first snapshot accounting differs");
  auto writer = json::field(root, "writer");
  KJ_REQUIRE(json::text(snapshot, "nodeId") == json::text(writer, "nodeId") &&
      integer(snapshot, "nodeEpoch") == integer(writer, "nodeEpoch") &&
      integer(snapshot, "shardEpoch") == integer(root, "shardEpoch") &&
      json::text(indexed, "nodeId") == json::text(snapshot, "nodeId") &&
      integer(indexed, "nodeEpoch") == integer(snapshot, "nodeEpoch") &&
      integer(indexed, "atUnixMs") == integer(snapshot, "atUnixMs") &&
      integer(indexedFacets[0], "facet") == 0 && integer(indexedFacets[0], "parent") == 0 &&
      integer(indexedFacets[0], "upToSeq") == 0 && json::text(indexedFacets[0], "name").empty(),
      "destination snapshot index differs from manifest 1");
  auto* digest = verifyContent ? EVP_MD_CTX_new() : nullptr;
  auto release = kj::defer([&]() { if (digest) EVP_MD_CTX_free(digest); });
  if (verifyContent) KJ_REQUIRE(digest && EVP_DigestInit_ex(digest, EVP_sha256(), nullptr) == 1, "cannot verify seed image");
  uint64_t read = 0;
  for (uint64_t part = 0; part < parts; ++part) {
    const auto expected = std::min(partBytes, bytes - read);
    if (verifyContent) {
      auto body = co_await store.get(key + wide(0) + "-" + wide(part));
      KJ_REQUIRE(body.status == 200 && body.bytes.size() == expected, "destination snapshot part is absent or has wrong length");
      KJ_REQUIRE(EVP_DigestUpdate(digest, body.bytes.data(), body.bytes.size()) == 1, "cannot hash destination part");
    } else {
      auto size = co_await store.headSize(key + wide(0) + "-" + wide(part));
      KJ_REQUIRE(size.has_value() && *size == expected, "destination snapshot part is absent or has wrong length");
    }
    read += expected;
  }
  if (verifyContent) {
    unsigned char value[32]; unsigned int length;
    KJ_REQUIRE(EVP_DigestFinal_ex(digest, value, &length) == 1 && length == 32, "cannot finish destination digest");
    constexpr char HEX[] = "0123456789abcdef"; std::string actual;
    for (auto byte : value) { actual += HEX[byte >> 4]; actual += HEX[byte & 15]; }
    KJ_REQUIRE(actual == hash, "destination image differs from first manifest");
  }
  KJ_REQUIRE(read == bytes, "destination image differs from first manifest"); co_return bytes;
}
} // namespace coworkerd
