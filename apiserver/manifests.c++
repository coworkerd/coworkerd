#include "manifests.h"
#include "support.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/debug.h>
#include <openssl/rand.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <sstream>

namespace coworkerd {
namespace {
// Old revisions of each sequence that stay: a reader that holds an index names a manifest at most this many writes old.
constexpr uint64_t KEEP_REVISIONS = 20;
constexpr size_t FETCH_CONCURRENCY = 8;
constexpr uint32_t INDEX_ATTEMPTS = 16;

uint64_t nowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
[[noreturn]] void refuseWrite(const std::string& reason) {
  kj::throwFatalException(kj::Exception(kj::Exception::Type::FAILED, __FILE__, __LINE__, kj::str(reason.c_str())));
}
bool formatOne(const std::string& bytes) {
  try {
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), json);
    auto format = etcd::field(json.asReader(), "format");
    return format.isNumber() && format.getNumber() == 1;
  } catch (const kj::Exception&) {
    return false;
  }
}
}  // namespace

struct ManifestStore::Turn {
  kj::Own<kj::PromiseFulfiller<void>> done;
  explicit Turn(kj::Own<kj::PromiseFulfiller<void>> done): done(kj::mv(done)) {}
  ~Turn() noexcept(false) { if (done->isWaiting()) done->fulfill(); }
};

ManifestStore::ManifestStore(ObjectStore& store, kj::Timer& timer, std::optional<SecretsKey> key, std::ostream& log)
    : store(store), timer(timer), key(std::move(key)), log(log) {
  KJ_REQUIRE(store.config().backend == "s3", "the desired state lives in an S3 object store (not an object directory)");
}

kj::Promise<kj::Own<ManifestStore::Turn>> ManifestStore::turn(const std::string& aggregate) {
  auto paf = kj::newPromiseAndFulfiller<void>();
  kj::Promise<void> previous = kj::READY_NOW;
  if (auto found = turns.find(aggregate); found != turns.end()) previous = found->second.addBranch();
  turns.insert_or_assign(aggregate, paf.promise.fork());
  try { co_await previous; } catch (const kj::Exception&) {}
  co_return kj::heap<Turn>(kj::mv(paf.fulfiller));
}

kj::Promise<void> ManifestStore::pause(uint32_t attempt) {
  uint32_t random = 0;
  KJ_REQUIRE(RAND_bytes(reinterpret_cast<unsigned char*>(&random), sizeof(random)) == 1, "cannot draw a retry pause");
  const uint32_t ceiling = std::min<uint32_t>(160, 5u << std::min<uint32_t>(attempt, 5));
  co_await timer.afterDelay((5 + random % (ceiling - 4)) * kj::MILLISECONDS);
}

const TenantManifest* ManifestStore::tenant(const std::string& name) const {
  auto found = manifests.find(name);
  return found == manifests.end() ? nullptr : &found->second;
}

void ManifestStore::adoptManifest(TenantManifest manifest) {
  auto& slot = manifests[manifest.tenant];
  if (slot.revision <= manifest.revision) slot = std::move(manifest);
}

kj::Promise<void> ManifestStore::readManifest(std::string tenant, uint64_t revision) {
  auto response = co_await store.get(revisionKey(tenantManifestPrefix(tenant), revision));
  if (response.status == 404) {
    // Pruned meanwhile, or not written where the index says: the tenant's latest.
    co_await reloadTenant(tenant);
    co_return;
  }
  KJ_REQUIRE(response.status == 200, "object store read failed", response.status);
  auto manifest = decodeTenantManifest(response.bytes);
  KJ_REQUIRE(manifest.tenant == tenant && manifest.revision == revision, "a manifest names another tenant or revision than its key",
      tenant.c_str(), revision);
  adoptManifest(std::move(manifest));
}

kj::Promise<void> ManifestStore::reloadTenant(std::string tenant) {
  auto latest = co_await readLatestRevision(store, tenantManifestPrefix(tenant), tenantHintKey(tenant), 0, true);
  if (!latest.revision) co_return;
  auto manifest = decodeTenantManifest(latest.bytes);
  KJ_REQUIRE(manifest.tenant == tenant && manifest.revision == latest.revision, "a manifest names another tenant or revision than its key",
      tenant.c_str(), latest.revision);
  adoptManifest(std::move(manifest));
}

kj::Promise<void> ManifestStore::adoptIndex(TenantIndex index) {
  if (index.revision < tenantIndex.revision) co_return;
  std::vector<std::pair<std::string, uint64_t>> wanted;
  for (const auto& [name, entry] : index.tenants) {
    auto found = manifests.find(name);
    if (found == manifests.end() || found->second.revision < entry.manifestRev) wanted.emplace_back(name, entry.manifestRev);
  }
  for (size_t start = 0; start < wanted.size(); start += FETCH_CONCURRENCY) {
    kj::Vector<kj::Promise<void>> reads;
    for (size_t i = start; i < std::min(wanted.size(), start + FETCH_CONCURRENCY); ++i)
      reads.add(readManifest(wanted[i].first, wanted[i].second));
    co_await kj::joinPromises(reads.releaseAsArray());
  }
  if (index.revision < tenantIndex.revision) co_return;  // a newer one arrived while the manifests were read
  // Tenants the index no longer names leave the mirror, unless this process wrote them and has not indexed them yet.
  for (auto it = manifests.begin(); it != manifests.end();) {
    if (!index.tenants.contains(it->first) && !unindexed.contains(it->first) && !removing.contains(it->first)) it = manifests.erase(it);
    else ++it;
  }
  tenantIndex = std::move(index);
}

kj::Promise<void> ManifestStore::load(bool create, bool credentials) {
  auto marker = co_await store.get(CLUSTER_FORMAT_KEY);
  if (marker.status == 404) {
    KJ_REQUIRE(create, "the bucket holds no cluster/format/1: it is not a Coworkerd cluster's bucket (init writes the marker)",
        store.config().bucket.c_str(), store.config().endpoint.c_str());
    const auto bytes = markerBytes();
    auto created = co_await store.create(CLUSTER_FORMAT_KEY, bytes);
    marker = created.created ? ObjectResponse{200, bytes} : co_await store.get(CLUSTER_FORMAT_KEY);
  }
  KJ_REQUIRE(marker.status == 200, "object store read failed", marker.status);
  KJ_REQUIRE(formatOne(marker.bytes), "cluster/format/1 does not name format 1: another format's cluster", store.config().bucket.c_str());
  auto latest = co_await readLatestRevision(store, TENANT_INDEX_PREFIX, indexHintKey(TENANT_INDEX_PREFIX), 0, true);
  if (latest.revision) {
    auto index = decodeTenantIndex(latest.bytes);
    KJ_REQUIRE(index.revision == latest.revision, "the tenant index names another revision than its key", latest.revision);
    co_await adoptIndex(std::move(index));
  }
  if (credentials) {
    auto read = co_await readLatestRevision(store, CREDENTIAL_INDEX_PREFIX, indexHintKey(CREDENTIAL_INDEX_PREFIX), 0, true);
    if (read.revision) {
      auto index = decodeCredentialIndex(read.bytes);
      KJ_REQUIRE(index.revision == read.revision, "the credentials index names another revision than its key", read.revision);
      if (index.revision > credentialIndex.revision) credentialIndex = std::move(index);
    }
  }
}

void ManifestStore::notePollFailure(const std::string& reason) {
  ++stats.pollFailures;
  stats.lastPollError = reason;
}

kj::Promise<void> ManifestStore::refresh() {
  ++stats.polls;
  stats.lastPollAtUnixMs = nowMs();
  auto hint = co_await readHintRevision(store, indexHintKey(TENANT_INDEX_PREFIX));
  if (hint && *hint > tenantIndex.revision) {
    auto latest = co_await readLatestRevision(store, TENANT_INDEX_PREFIX, indexHintKey(TENANT_INDEX_PREFIX), 0, true);
    if (latest.revision > tenantIndex.revision) co_await adoptIndex(decodeTenantIndex(latest.bytes));
  }
  auto credentialHint = co_await readHintRevision(store, indexHintKey(CREDENTIAL_INDEX_PREFIX));
  if (credentialHint && *credentialHint > credentialIndex.revision) {
    auto latest = co_await readLatestRevision(store, CREDENTIAL_INDEX_PREFIX, indexHintKey(CREDENTIAL_INDEX_PREFIX), 0, true);
    if (latest.revision > credentialIndex.revision) credentialIndex = decodeCredentialIndex(latest.bytes);
  }
  // An index write that failed after its manifest was committed is retried here.
  if (!unindexed.empty() || !removing.empty()) co_await syncIndex();
  stats.lastPollError.clear();
}

kj::Promise<ManifestStore::Written> ManifestStore::writeTenant(std::string name,
    std::function<kj::Promise<void>(TenantManifest&)> change, bool deployment, uint32_t maxAttempts) {
  KJ_REQUIRE(tenantName(name), "invalid tenant", name.c_str());
  auto held = co_await turn("tenant:" + name);
  Written result;
  result.submittedAtUnixMs = nowMs();
  // A tenant the mirror does not hold: what the store has (the leftovers of a deleted tenant, a tenant only the
  // operations tool writes) is its current manifest.
  if (!manifests.contains(name)) co_await reloadTenant(name);
  const auto prefix = tenantManifestPrefix(name);
  for (uint32_t attempt = 1;; ++attempt) {
    TenantManifest base;
    if (const auto* current = tenant(name)) base = *current;
    else base.tenant = name;
    TenantManifest next = base;
    co_await change(next);
    next.tenant = name;
    result.attempts = attempt;
    if (base.revision && sameContent(next, base)) {
      result.manifest = std::move(base);
      co_return result;
    }
    next.revision = base.revision + 1;
    next.generation = base.generation + (deployment ? 1 : 0);
    next.writer = newWriter(instance);
    next.writtenAtUnixMs = next.writer.writtenAtUnixMs;
    const auto started = std::chrono::steady_clock::now();
    auto created = co_await store.create(revisionKey(prefix, next.revision), tenantManifestJson(next));
    const auto waited = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started).count());
    if (created.created) {
      ++stats.writes;
      result.changed = true;
      result.committedAtUnixMs = nowMs();
      result.manifest = next;
      unindexed.insert(name);
      adoptManifest(std::move(next));
      log << "[state] {\"event\":\"manifest\",\"tenant\":\"" << name << "\",\"revision\":" << result.manifest.revision
                << ",\"generation\":" << result.manifest.generation << ",\"attempts\":" << attempt << "}" << std::endl;
      // Committed: the hint and the old revisions follow, and a failure there only makes a later reader list.
      try {
        co_await publishHint(store, prefix, tenantHintKey(name), result.manifest.revision);
        co_await pruneRevision(store, prefix, result.manifest.revision, KEEP_REVISIONS);
      } catch (const kj::Exception& error) {
        log << "[state] {\"event\":\"hint-failed\",\"tenant\":\"" << name << "\",\"reason\":\""
                  << jsonEscape(error.getDescription().cStr()) << "\"}" << std::endl;
      }
      // The tenant stays this writer's until the index names the revision: the next writer of the tenant in this
      // process reads an index that has it (a creation is then never taken for an interrupted one).
      try {
        result.indexRevision = co_await syncIndex();
      } catch (const kj::Exception& error) {
        result.indexed = false;
        log << "[state] {\"event\":\"index-pending\",\"tenant\":\"" << name << "\",\"reason\":\""
                  << jsonEscape(error.getDescription().cStr()) << "\"}" << std::endl;
      }
      co_return result;
    }
    ++stats.conflicts;
    result.conflictWaitsMs.push_back(waited);
    if (attempt >= maxAttempts)
      refuseWrite("期间有其他部署 (another writer created revision " + std::to_string(next.revision) + " of tenant " + name +
          "'s manifest first; " + std::to_string(attempt) + " attempts); nothing written");
    ++stats.retries;
    co_await pause(attempt);
    co_await reloadTenant(name);
  }
}

kj::Promise<uint64_t> ManifestStore::syncIndex() {
  auto held = co_await turn("index");
  for (uint32_t attempt = 1;; ++attempt) {
    TenantIndex next = tenantIndex;
    std::set<std::string> settled;
    for (const auto& name : unindexed) {
      auto manifest = manifests.find(name);
      if (manifest == manifests.end()) { settled.insert(name); continue; }
      auto entry = next.tenants.find(name);
      if (entry != next.tenants.end()) {
        if (entry->second.manifestRev >= manifest->second.revision) { settled.insert(name); continue; }
        entry->second = indexEntryOf(manifest->second);
        continue;
      }
      // Not in the index: a tenant this process created (or the tool's first write of one), or one a deletion took out
      // meanwhile. Only an active manifest that is still the tenant's latest enters it.
      auto latest = co_await readLatestRevision(store, tenantManifestPrefix(name), tenantHintKey(name), 0, true);
      if (latest.revision != manifest->second.revision || manifest->second.status() != "active") { settled.insert(name); continue; }
      next.tenants.emplace(name, indexEntryOf(manifest->second));
    }
    for (const auto& name : removing) next.tenants.erase(name);
    for (const auto& name : settled) unindexed.erase(name);
    if (next.tenants == tenantIndex.tenants) {
      for (auto it = unindexed.begin(); it != unindexed.end();) {
        auto entry = tenantIndex.tenants.find(*it);
        auto manifest = manifests.find(*it);
        if (manifest == manifests.end() || (entry != tenantIndex.tenants.end() && entry->second.manifestRev >= manifest->second.revision))
          it = unindexed.erase(it);
        else ++it;
      }
      co_return tenantIndex.revision;
    }
    next.revision = tenantIndex.revision + 1;
    next.writer = newWriter(instance);
    next.writtenAtUnixMs = next.writer.writtenAtUnixMs;
    auto created = co_await store.create(revisionKey(TENANT_INDEX_PREFIX, next.revision), tenantIndexJson(next));
    if (created.created) {
      ++stats.indexWrites;
      tenantIndex = next;
      for (auto it = unindexed.begin(); it != unindexed.end();) {
        auto entry = tenantIndex.tenants.find(*it);
        auto manifest = manifests.find(*it);
        if (manifest == manifests.end() || (entry != tenantIndex.tenants.end() && entry->second.manifestRev >= manifest->second.revision))
          it = unindexed.erase(it);
        else ++it;
      }
      try {
        co_await publishHint(store, TENANT_INDEX_PREFIX, indexHintKey(TENANT_INDEX_PREFIX), next.revision);
        co_await pruneRevision(store, TENANT_INDEX_PREFIX, next.revision, KEEP_REVISIONS);
      } catch (const kj::Exception& error) {
        log << "[state] {\"event\":\"hint-failed\",\"index\":\"tenants\",\"reason\":\""
                  << jsonEscape(error.getDescription().cStr()) << "\"}" << std::endl;
      }
      co_return next.revision;
    }
    ++stats.indexConflicts;
    KJ_REQUIRE(attempt < INDEX_ATTEMPTS, "other writers kept creating the tenant index's next revision first", attempt);
    co_await pause(attempt);
    auto latest = co_await readLatestRevision(store, TENANT_INDEX_PREFIX, indexHintKey(TENANT_INDEX_PREFIX), 0, true);
    if (latest.revision) co_await adoptIndex(decodeTenantIndex(latest.bytes));
  }
}

kj::Promise<ManifestStore::Forgotten> ManifestStore::forgetTenant(std::string name) {
  Forgotten result;
  removing.insert(name);
  result.indexRevision = co_await syncIndex();
  auto listing = co_await store.list(tenantObjectsPrefix(name));
  // The hint and the newest revisions last: an interrupted removal leaves a readable latest revision behind.
  std::vector<std::string> keys;
  for (const auto& object : listing.objects) keys.push_back(object.key);
  std::sort(keys.begin(), keys.end());
  for (const auto& key : keys) {
    try { co_await store.remove(key); ++result.removed; } catch (const kj::Exception&) { ++result.failed; }
  }
  if (!result.failed) {
    manifests.erase(name);
    unindexed.erase(name);
    removing.erase(name);
  }
  co_return result;
}

kj::Promise<ManifestStore::WrittenCredentials> ManifestStore::writeCredentials(std::function<void(CredentialIndex&)> change,
    uint32_t maxAttempts) {
  auto held = co_await turn("credentials");
  WrittenCredentials result;
  for (uint32_t attempt = 1;; ++attempt) {
    CredentialIndex next = credentialIndex;
    change(next);
    result.attempts = attempt;
    // The same credentials (revision, time and writer aside): nothing to write.
    const auto content = [](const CredentialIndex& index) {
      CredentialIndex bare;
      bare.credentials = index.credentials;
      return credentialIndexJson(bare);
    };
    if (next.credentials.size() == credentialIndex.credentials.size() && content(next) == content(credentialIndex)) {
      result.index = credentialIndex;
      co_return result;
    }
    next.revision = credentialIndex.revision + 1;
    next.writer = newWriter(instance);
    next.writtenAtUnixMs = next.writer.writtenAtUnixMs;
    auto created = co_await store.create(revisionKey(CREDENTIAL_INDEX_PREFIX, next.revision), credentialIndexJson(next));
    if (created.created) {
      ++stats.credentialWrites;
      credentialIndex = next;
      result.index = next;
      result.changed = true;
      try {
        co_await publishHint(store, CREDENTIAL_INDEX_PREFIX, indexHintKey(CREDENTIAL_INDEX_PREFIX), next.revision);
        co_await pruneRevision(store, CREDENTIAL_INDEX_PREFIX, next.revision, KEEP_REVISIONS);
      } catch (const kj::Exception& error) {
        log << "[state] {\"event\":\"hint-failed\",\"index\":\"credentials\",\"reason\":\""
                  << jsonEscape(error.getDescription().cStr()) << "\"}" << std::endl;
      }
      co_return result;
    }
    ++stats.credentialConflicts;
    if (attempt >= maxAttempts)
      refuseWrite("another write changed the credentials (" + std::to_string(attempt) + " attempts); nothing written");
    co_await pause(attempt);
    auto latest = co_await readLatestRevision(store, CREDENTIAL_INDEX_PREFIX, indexHintKey(CREDENTIAL_INDEX_PREFIX), 0, true);
    if (latest.revision > credentialIndex.revision) credentialIndex = decodeCredentialIndex(latest.bytes);
  }
}

kj::Promise<void> ManifestStore::fetchDefinitions(std::set<std::string> hashes) {
  std::vector<std::string> wanted;
  for (const auto& hash : hashes) if (!definitionBytes.contains(hash)) wanted.push_back(hash);
  for (size_t start = 0; start < wanted.size(); start += FETCH_CONCURRENCY) {
    kj::Vector<kj::Promise<void>> reads;
    for (size_t i = start; i < std::min(wanted.size(), start + FETCH_CONCURRENCY); ++i) {
      reads.add([](ManifestStore& self, std::string hash) -> kj::Promise<void> {
        ++self.stats.definitionFetches;
        auto response = co_await self.store.get("definitions/" + hash);
        if (response.status != 200) {
          KJ_REQUIRE(response.status == 404, "object store read failed", response.status);
          self.failedDefinitions[hash] = "missing";
          co_return;
        }
        if (!definitionMatches(hash, response.bytes)) {
          self.failedDefinitions[hash] = "content hash mismatch";
          co_return;
        }
        self.failedDefinitions.erase(hash);
        self.parsedDefinitions[hash] = decodeDeploymentDefinition(response.bytes);
        self.definitionBytes[hash] = std::move(response.bytes);
      }(*this, wanted[i]));
    }
    co_await kj::joinPromises(reads.releaseAsArray());
  }
}

void ManifestStore::rememberDefinition(const VersionDefinition& version) {
  if (definitionBytes.contains(version.definitionHash)) return;
  auto bytes = definitionJson(version);
  if (!definitionMatches(version.definitionHash, bytes)) return;
  parsedDefinitions[version.definitionHash] = decodeDeploymentDefinition(bytes);
  definitionBytes[version.definitionHash] = std::move(bytes);
  failedDefinitions.erase(version.definitionHash);
}

kj::Promise<std::vector<std::shared_ptr<const VersionDefinition>>> ManifestStore::allDefinitions(
    std::function<bool(const std::string&)> tenantFilter) {
  std::set<std::string> hashes;
  for (const auto& [name, manifest] : manifests) if (tenantFilter(name)) hashes.merge(namedDefinitions(manifest));
  co_await fetchDefinitions(hashes);
  std::vector<std::shared_ptr<const VersionDefinition>> out;
  for (const auto& hash : hashes) if (auto version = definition(hash)) out.push_back(version);
  co_return out;
}

std::shared_ptr<const VersionDefinition> ManifestStore::definition(const std::string& hash) const {
  auto found = parsedDefinitions.find(hash);
  return found == parsedDefinitions.end() ? nullptr : found->second;
}

DeploymentState ManifestStore::stateOf(const std::string& name, const TenantManifest* replacement) const {
  ManifestView view;
  for (const auto& [tenant, manifest] : manifests) view.emplace(tenant, &manifest);
  if (replacement) view[name] = replacement;
  else if (auto found = manifests.find(name); found != manifests.end()) view[name] = &found->second;
  const auto& keyRef = key;
  SecretOpener open = [&keyRef](const std::string& tenant, const std::string& worker, const std::string& secret,
      const ManifestSecret& sealed) -> std::optional<std::string> {
    if (keyRef) {
      auto value = keyRef->open(secretAad(tenant, worker, secret), sealed);
      if (!value) return std::nullopt;
      std::fill(value->begin(), value->end(), '\0');
      return std::string();
    }
    return sealedLength(sealed) ? std::optional<std::string>(std::string()) : std::nullopt;
  };
  std::vector<DeploymentState::Change> changes;
  if (auto found = view.find(name); found != view.end()) {
    for (auto& [stateKey, value] : tenantStateKeys(*found->second, definitionBytes, open)) changes.push_back({stateKey, value});
  }
  for (auto& [stateKey, value] : routeStateKeys(view)) changes.push_back({stateKey, value});
  changes.push_back({"generation", generationValue(view)});
  DeploymentState state;
  state.apply(changes, true, tenantIndex.revision);
  return state;
}

std::string ManifestStore::json() const {
  std::ostringstream out;
  out << "{\"tenantsRevision\":\"" << tenantIndex.revision << "\",\"credentialsRevision\":\"" << credentialIndex.revision
      << "\",\"tenants\":" << tenantIndex.tenants.size() << ",\"credentials\":" << credentialIndex.credentials.size()
      << ",\"writes\":" << stats.writes << ",\"conflicts\":" << stats.conflicts << ",\"retries\":" << stats.retries
      << ",\"indexWrites\":" << stats.indexWrites << ",\"indexConflicts\":" << stats.indexConflicts
      << ",\"credentialWrites\":" << stats.credentialWrites << ",\"credentialConflicts\":" << stats.credentialConflicts
      << ",\"unindexed\":" << unindexed.size() << ",\"polls\":" << stats.polls << ",\"pollFailures\":" << stats.pollFailures
      << ",\"lastPollAtUnixMs\":" << stats.lastPollAtUnixMs << ",\"lastPollError\":\"" << jsonEscape(stats.lastPollError)
      << "\",\"definitions\":" << definitionBytes.size() << ",\"definitionFetches\":" << stats.definitionFetches
      << ",\"definitionFailures\":" << failedDefinitions.size() << ",\"instance\":\"" << instance
      << "\",\"client\":" << store.stats().json() << "}";
  return out.str();
}

}  // namespace coworkerd
