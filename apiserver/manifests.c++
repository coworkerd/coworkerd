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
// Another writer kept winning: a conflict (WRITE_CONFLICT_DETAIL), never a reason's words.
[[noreturn]] void refuseWrite(const std::string& reason) { failConflict(reason); }
bool formatOne(const std::string& bytes) {
  try {
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), json);
    auto format = json::field(json.asReader(), "format");
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
  requireStorage(response.status == 200, "object store read failed: HTTP " + std::to_string(response.status));
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

kj::Promise<void> ManifestStore::load(bool create, bool credentials, bool repairing) {
  auto marker = co_await store.get(CLUSTER_FORMAT_KEY);
  if (marker.status == 404) {
    if (!create)
      KJ_FAIL_REQUIRE(kj::str("the bucket ", store.config().bucket.c_str(), " at ", store.config().endpoint.c_str(),
          " holds no cluster/format/1: it is not a Coworkerd cluster's bucket (coworkerd-apiserver tool init writes the "
          "marker of a new cluster's bucket); nothing written"));
    const auto bytes = markerBytes();
    auto created = co_await store.create(CLUSTER_FORMAT_KEY, bytes);
    marker = created.created ? ObjectResponse{200, bytes} : co_await store.get(CLUSTER_FORMAT_KEY);
  }
  requireStorage(marker.status == 200, "object store read failed: HTTP " + std::to_string(marker.status) + " (" + CLUSTER_FORMAT_KEY + ")");
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
  if (!repairing) co_return;
  // What a writer left half done (a hint, an index): completed now; what fails here the first poll tries again.
  try {
    co_await repairIndex();
    if (credentials) co_await repairCredentials();
    co_await repair();
    if (!unindexed.empty()) co_await syncIndex();
  } catch (const kj::Exception& error) {
    log << "[state] {\"event\":\"repair-failed\",\"phase\":\"load\",\"reason\":\"" << jsonEscape(error.getDescription().cStr())
        << "\"}" << std::endl;
  }
}

void ManifestStore::notePollFailure(const std::string& reason) {
  ++stats.pollFailures;
  stats.lastPollError = reason;
}

kj::Promise<void> ManifestStore::refresh() {
  ++stats.polls;
  stats.lastPollAtUnixMs = nowMs();
  co_await repairIndex();
  co_await repairCredentials();
  // Hints this process failed to write, then what any writer left half done.
  for (auto pending = hintPending; const auto& name : pending) {
    if (const auto* manifest = tenant(name); manifest && !removing.contains(name)) co_await writeTenantHint(name, manifest->revision);
    else hintPending.erase(name);
  }
  co_await repair();
  // An index write that failed after its manifest was committed is retried here.
  if (!unindexed.empty() || !removing.empty() || tenantIndexHintPending) co_await syncIndex();
  stats.lastPollError.clear();
}

kj::Promise<void> ManifestStore::writeTenantHint(const std::string& name, uint64_t revision) {
  const auto prefix = tenantManifestPrefix(name);
  hintPending.insert(name);
  try {
    co_await publishHint(store, prefix, tenantHintKey(name), revision);
  } catch (const kj::Exception& error) {
    log << "[state] {\"event\":\"hint-failed\",\"tenant\":\"" << name << "\",\"revision\":" << revision << ",\"reason\":\""
        << jsonEscape(error.getDescription().cStr()) << "\"}" << std::endl;
    throw;
  }
  hintPending.erase(name);
  knownHints.erase(name);
  // The old revisions: a failure there only leaves one more object behind.
  try {
    co_await pruneRevision(store, prefix, revision, KEEP_REVISIONS);
  } catch (const kj::Exception& error) {
    log << "[state] {\"event\":\"prune-failed\",\"tenant\":\"" << name << "\",\"reason\":\""
        << jsonEscape(error.getDescription().cStr()) << "\"}" << std::endl;
  }
}

kj::Promise<void> ManifestStore::writeIndexHint(const char* prefix, uint64_t revision, bool& pending) {
  pending = true;
  try {
    co_await publishHint(store, prefix, indexHintKey(prefix), revision);
  } catch (const kj::Exception& error) {
    log << "[state] {\"event\":\"hint-failed\",\"index\":\"" << (std::string(prefix) == TENANT_INDEX_PREFIX ? "tenants" : "credentials")
        << "\",\"revision\":" << revision << ",\"reason\":\"" << jsonEscape(error.getDescription().cStr()) << "\"}" << std::endl;
    throw;
  }
  pending = false;
  try {
    co_await pruneRevision(store, prefix, revision, KEEP_REVISIONS);
  } catch (const kj::Exception& error) {
    log << "[state] {\"event\":\"prune-failed\",\"index\":\"" << prefix << "\",\"reason\":\""
        << jsonEscape(error.getDescription().cStr()) << "\"}" << std::endl;
  }
}

// The tenant index's hint, and whether the revision after the newest known exists (a writer that stopped after its
// create and before its hint, or this process's hint write that failed): the latest adopted, the hint rewritten.
kj::Promise<void> ManifestStore::repairIndex() {
  const auto hint = co_await readHintRevision(store, indexHintKey(TENANT_INDEX_PREFIX));
  const uint64_t hinted = hint.value_or(0);
  uint64_t latest = std::max(hinted, tenantIndex.revision);
  if (hinted > tenantIndex.revision || co_await store.exists(revisionKey(TENANT_INDEX_PREFIX, latest + 1))) {
    auto read = co_await readLatestRevision(store, TENANT_INDEX_PREFIX, indexHintKey(TENANT_INDEX_PREFIX), 0, true);
    if (read.revision > tenantIndex.revision) co_await adoptIndex(decodeTenantIndex(read.bytes));
    latest = std::max(latest, read.revision);
  }
  if (latest > hinted) {
    auto turnHeld = co_await turn("index");
    co_await writeIndexHint(TENANT_INDEX_PREFIX, std::max(latest, tenantIndex.revision), tenantIndexHintPending);
    ++stats.repairedIndexHints;
    log << "[state] {\"event\":\"hint-repaired\",\"index\":\"tenants\",\"from\":" << hinted << ",\"to\":" << latest << "}" << std::endl;
  }
}

kj::Promise<void> ManifestStore::repairCredentials() {
  const auto hint = co_await readHintRevision(store, indexHintKey(CREDENTIAL_INDEX_PREFIX));
  const uint64_t hinted = hint.value_or(0);
  uint64_t latest = std::max(hinted, credentialIndex.revision);
  if (hinted > credentialIndex.revision || co_await store.exists(revisionKey(CREDENTIAL_INDEX_PREFIX, latest + 1))) {
    auto read = co_await readLatestRevision(store, CREDENTIAL_INDEX_PREFIX, indexHintKey(CREDENTIAL_INDEX_PREFIX), 0, true);
    if (read.revision > credentialIndex.revision) credentialIndex = decodeCredentialIndex(read.bytes);
    latest = std::max(latest, read.revision);
  }
  if (latest > hinted) {
    auto turnHeld = co_await turn("credentials");
    co_await writeIndexHint(CREDENTIAL_INDEX_PREFIX, std::max(latest, credentialIndex.revision), credentialsHintPending);
    ++stats.repairedIndexHints;
    log << "[state] {\"event\":\"hint-repaired\",\"index\":\"credentials\",\"from\":" << hinted << ",\"to\":" << latest << "}" << std::endl;
  }
}

kj::Promise<void> ManifestStore::repair() {
  ++stats.repairs;
  struct Listed { uint64_t latest = 0; bool hint = false; std::string hintEtag; };
  std::map<std::string, Listed> listed;
  for (const auto& object : (co_await store.list(TENANT_OBJECTS_PREFIX)).objects) {
    const std::string_view rest = std::string_view(object.key).substr(std::string_view(TENANT_OBJECTS_PREFIX).size());
    const auto slash = rest.find('/');
    if (slash == std::string_view::npos) continue;
    const std::string name(rest.substr(0, slash));
    if (!tenantName(name)) continue;
    if (object.key == tenantHintKey(name)) {
      listed[name].hint = true;
      listed[name].hintEtag = object.etag;
    } else if (auto revision = parseRevision(object.key, tenantManifestPrefix(name))) {
      listed[name].latest = std::max(listed[name].latest, *revision);
    }
  }
  for (const auto& [name, found] : listed) {
    if (!found.latest || removing.contains(name)) continue;
    const auto entry = tenantIndex.tenants.find(name);
    const bool named = entry != tenantIndex.tenants.end();
    const auto* current = tenant(name);
    if (!current || current->revision < found.latest) {
      if (named) {
        // A revision of an indexed tenant newer than the mirror holds: written by a writer that did not index it.
        co_await readManifest(name, found.latest);
      } else if (auto seen = examined.find(name); seen == examined.end() || seen->second != found.latest) {
        // A tenant the index does not name: only an active manifest is one to index (a deletion's leftovers stay out).
        examined[name] = found.latest;
        auto object = co_await store.get(revisionKey(tenantManifestPrefix(name), found.latest));
        if (object.status == 404) continue;
        requireStorage(object.status == 200, "object store read failed: HTTP " + std::to_string(object.status));
        auto manifest = decodeTenantManifest(object.bytes);
        if (manifest.tenant != name || manifest.revision != found.latest || manifest.status() != "active") continue;
        adoptManifest(std::move(manifest));
      }
      current = tenant(name);
    }
    if (!current) continue;
    const bool lags = named ? entry->second.manifestRev < current->revision : current->status() == "active";
    auto refused = indexRefusals.find(name);
    if (lags && (refused == indexRefusals.end() || refused->second.first != current->revision) && !unindexed.contains(name)) {
      unindexed.insert(name);
      ++stats.repairedManifests;
      log << "[state] {\"event\":\"index-repair\",\"tenant\":\"" << name << "\",\"revision\":" << current->revision
          << ",\"indexed\":" << (named ? entry->second.manifestRev : 0) << "}" << std::endl;
    }
    // The tenant's hint (only for a tenant the index names or is to name: nothing is written back below a deleted
    // tenant's prefix), read once per change of its ETag.
    if (!named && !unindexed.contains(name)) continue;
    if (found.hint) {
      auto known = knownHints.find(name);
      if (known != knownHints.end() && known->second.first == found.hintEtag && known->second.second >= found.latest) continue;
    }
    const auto hinted = found.hint ? (co_await readHintRevision(store, tenantHintKey(name))).value_or(0) : 0;
    if (hinted >= found.latest) {
      knownHints[name] = {found.hintEtag, hinted};
      continue;
    }
    auto held = co_await turn("tenant:" + name);
    co_await writeTenantHint(name, found.latest);
    ++stats.repairedHints;
    log << "[state] {\"event\":\"hint-repaired\",\"tenant\":\"" << name << "\",\"from\":" << hinted << ",\"to\":" << found.latest
        << "}" << std::endl;
  }
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
      // Already in the latest manifest (a request tried again -- perhaps after an attempt that created its revision
      // and was answered with a failure): it completes as a write does, its hint and the index brought up to it.
      result.manifest = base;
      co_await complete(name, base, result);
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
      co_await complete(name, base, result);
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

// A tenant's revision made visible: its hint, then the index -- a failure of either thrown (the store's: the write is not
// done; the revision stays and is completed later) -- then the index's verdict on it.
kj::Promise<void> ManifestStore::complete(const std::string& name, const TenantManifest& base, Written& result) {
  const auto revision = result.manifest.revision;
  if (result.changed || hintPending.contains(name)) co_await writeTenantHint(name, revision);
  auto entry = tenantIndex.tenants.find(name);
  const bool lags = entry != tenantIndex.tenants.end() ? entry->second.manifestRev < revision : result.manifest.status() == "active";
  if (lags) unindexed.insert(name);
  if (result.changed || lags || tenantIndexHintPending) {
    try {
      result.indexRevision = co_await syncIndex();
    } catch (const kj::Exception& error) {
      result.indexed = false;
      log << "[state] {\"event\":\"index-failed\",\"tenant\":\"" << name << "\",\"revision\":" << revision << ",\"reason\":\""
          << jsonEscape(error.getDescription().cStr()) << "\"}" << std::endl;
      throw;
    }
  } else {
    result.indexRevision = tenantIndex.revision;
  }
  if (auto refused = indexRefusals.find(name); refused != indexRefusals.end() && refused->second.first == revision) {
    const auto reason = refused->second.second;
    co_await revert(name, base);
    failConflict(reason + "; nothing written");
  }
  auto now = tenantIndex.tenants.find(name);
  result.indexed = now != tenantIndex.tenants.end() ? now->second.manifestRev >= revision : result.manifest.status() != "active";
}

// A write the index refused (a hostname another tenant holds): an update's record goes back to what it was before the
// write (as a next revision, indexed); a creation -- a tenant the index does not name -- has its objects removed.
kj::Promise<void> ManifestStore::revert(const std::string& name, const TenantManifest& base) {
  const auto* current = tenant(name);
  if (!current) co_return;
  if (!tenantIndex.tenants.contains(name) || !base.record) {
    for (const auto& object : (co_await store.list(tenantObjectsPrefix(name))).objects) co_await store.remove(object.key);
    manifests.erase(name);
    unindexed.erase(name);
    hintPending.erase(name);
    knownHints.erase(name);
    indexRefusals.erase(name);
    ++stats.reverted;
    log << "[state] {\"event\":\"reverted\",\"tenant\":\"" << name << "\",\"removed\":true}" << std::endl;
    co_return;
  }
  TenantManifest next = *current;
  next.record = base.record;
  next.revision = current->revision + 1;
  next.writer = newWriter(instance);
  next.writtenAtUnixMs = next.writer.writtenAtUnixMs;
  auto created = co_await store.create(revisionKey(tenantManifestPrefix(name), next.revision), tenantManifestJson(next));
  if (!created.created) refuseWrite("期间有其他部署 (another writer created revision " + std::to_string(next.revision) + " of tenant " +
      name + "'s manifest while its refused write went back)");
  ++stats.writes;
  ++stats.reverted;
  indexRefusals.erase(name);
  unindexed.insert(name);
  adoptManifest(next);
  log << "[state] {\"event\":\"reverted\",\"tenant\":\"" << name << "\",\"revision\":" << next.revision << "}" << std::endl;
  co_await writeTenantHint(name, next.revision);
  co_await syncIndex();
}

kj::Promise<uint64_t> ManifestStore::syncIndex() {
  auto held = co_await turn("index");
  auto settle = [this]() {
    for (auto it = unindexed.begin(); it != unindexed.end();) {
      auto entry = tenantIndex.tenants.find(*it);
      auto manifest = manifests.find(*it);
      if (manifest == manifests.end() || (entry != tenantIndex.tenants.end() && entry->second.manifestRev >= manifest->second.revision))
        it = unindexed.erase(it);
      else ++it;
    }
  };
  for (uint32_t attempt = 1;; ++attempt) {
    TenantIndex next = tenantIndex;
    std::set<std::string> settled;
    for (const auto& name : unindexed) {
      auto manifest = manifests.find(name);
      if (manifest == manifests.end()) { settled.insert(name); continue; }
      auto entry = next.tenants.find(name);
      if (entry != next.tenants.end() && entry->second.manifestRev >= manifest->second.revision) { settled.insert(name); continue; }
      if (entry == next.tenants.end()) {
        // Not in the index: a tenant this process created (or the tool's first write of one), or one a deletion took out
        // meanwhile. Only an active manifest that is still the tenant's latest enters it.
        auto latest = co_await readLatestRevision(store, tenantManifestPrefix(name), tenantHintKey(name), 0, true);
        if (latest.revision != manifest->second.revision || manifest->second.status() != "active") { settled.insert(name); continue; }
      }
      // A hostname is one tenant's, and the index -- one aggregate -- decides: an entry that would name a hostname another
      // entry names keeps its previous revision (the writer goes back to it, revert()).
      std::string taken;
      for (const auto& host : manifest->second.hostnames()) {
        for (const auto& [other, otherEntry] : next.tenants) {
          if (other == name || std::find(otherEntry.hostnames.begin(), otherEntry.hostnames.end(), host) == otherEntry.hostnames.end())
            continue;
          taken = "hostname " + host + " belongs to tenant " + other;
          break;
        }
        if (!taken.empty()) break;
      }
      if (!taken.empty()) {
        settled.insert(name);
        if (auto refused = indexRefusals.find(name); refused == indexRefusals.end() || refused->second.first != manifest->second.revision) {
          ++stats.hostnameRefusals;
          log << "[state] {\"event\":\"index-refused\",\"tenant\":\"" << name << "\",\"revision\":" << manifest->second.revision
              << ",\"reason\":\"" << jsonEscape(taken) << "\"}" << std::endl;
        }
        indexRefusals[name] = {manifest->second.revision, taken};
        continue;
      }
      next.tenants[name] = indexEntryOf(manifest->second);
    }
    // A tenant being forgotten leaves the index -- unless the name was created again meanwhile (its manifest active).
    for (const auto& name : removing) {
      auto manifest = manifests.find(name);
      if (manifest == manifests.end() || manifest->second.status() != "active") next.tenants.erase(name);
    }
    for (const auto& name : settled) unindexed.erase(name);
    if (next.tenants == tenantIndex.tenants) {
      settle();
      if (tenantIndexHintPending) co_await writeIndexHint(TENANT_INDEX_PREFIX, tenantIndex.revision, tenantIndexHintPending);
      co_return tenantIndex.revision;
    }
    next.revision = tenantIndex.revision + 1;
    next.writer = newWriter(instance);
    next.writtenAtUnixMs = next.writer.writtenAtUnixMs;
    auto created = co_await store.create(revisionKey(TENANT_INDEX_PREFIX, next.revision), tenantIndexJson(next));
    if (created.created) {
      ++stats.indexWrites;
      tenantIndex = next;
      settle();
      // Visible to the nodes only with the hint: a failure here fails the write (the poll writes it later).
      co_await writeIndexHint(TENANT_INDEX_PREFIX, next.revision, tenantIndexHintPending);
      co_return next.revision;
    }
    ++stats.indexConflicts;
    if (attempt >= INDEX_ATTEMPTS)
      refuseWrite("other writers kept creating the tenant index's next revision first (" + std::to_string(attempt) + " attempts)");
    co_await pause(attempt);
    auto latest = co_await readLatestRevision(store, TENANT_INDEX_PREFIX, indexHintKey(TENANT_INDEX_PREFIX), 0, true);
    if (latest.revision) co_await adoptIndex(decodeTenantIndex(latest.bytes));
  }
}

kj::Promise<ManifestStore::Forgotten> ManifestStore::forgetTenant(std::string name) {
  auto held = co_await turn("tenant:" + name);
  Forgotten result;
  // Out of the index first; the name is not kept from the index afterwards, whatever the removals below meet.
  removing.insert(name);
  try {
    result.indexRevision = co_await syncIndex();
  } catch (const kj::Exception&) {
    removing.erase(name);
    throw;
  }
  removing.erase(name);
  auto listing = co_await store.list(tenantObjectsPrefix(name));
  // The hint first (a reader without it lists the prefix), then the revisions oldest first: a removal that stops half
  // way leaves the newest revision readable, where the deletion run again finds the tenant.
  std::vector<std::string> keys;
  for (const auto& object : listing.objects) keys.push_back(object.key);
  std::sort(keys.begin(), keys.end());
  for (const auto& key : keys) {
    try { co_await store.remove(key); ++result.removed; } catch (const kj::Exception&) { ++result.failed; }
  }
  if (!result.failed) {
    manifests.erase(name);
    unindexed.erase(name);
    hintPending.erase(name);
    knownHints.erase(name);
    examined.erase(name);
    indexRefusals.erase(name);
  }
  co_return result;
}

kj::Promise<ManifestStore::WrittenCredentials> ManifestStore::writeCredentials(
    std::function<kj::Promise<void>(CredentialIndex&)> change, uint32_t maxAttempts, bool always) {
  auto held = co_await turn("credentials");
  WrittenCredentials result;
  for (uint32_t attempt = 1;; ++attempt) {
    CredentialIndex next = credentialIndex;
    co_await change(next);
    result.attempts = attempt;
    // The same credentials (revision, time and writer aside): nothing to write.
    const auto content = [](const CredentialIndex& index) {
      CredentialIndex bare;
      bare.credentials = index.credentials;
      return credentialIndexJson(bare);
    };
    if (!always && next.credentials.size() == credentialIndex.credentials.size() && content(next) == content(credentialIndex)) {
      result.index = credentialIndex;
      if (credentialsHintPending) co_await writeIndexHint(CREDENTIAL_INDEX_PREFIX, credentialIndex.revision, credentialsHintPending);
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
      // The other instances (and this one after a restart) read the credentials through the hint: a failure here fails
      // the write (the poll writes the hint later).
      co_await writeIndexHint(CREDENTIAL_INDEX_PREFIX, next.revision, credentialsHintPending);
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
          requireStorage(response.status == 404, "object store read failed: HTTP " + std::to_string(response.status));
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
      << ",\"unindexed\":" << unindexed.size() << ",\"hintsPending\":" << hintPending.size()
      << ",\"indexHintPending\":" << (tenantIndexHintPending ? "true" : "false")
      << ",\"credentialsHintPending\":" << (credentialsHintPending ? "true" : "false")
      << ",\"repairs\":" << stats.repairs << ",\"repairedHints\":" << stats.repairedHints
      << ",\"repairedIndexHints\":" << stats.repairedIndexHints << ",\"repairedManifests\":" << stats.repairedManifests
      << ",\"hostnameRefusals\":" << stats.hostnameRefusals << ",\"reverted\":" << stats.reverted
      << ",\"polls\":" << stats.polls << ",\"pollFailures\":" << stats.pollFailures
      << ",\"lastPollAtUnixMs\":" << stats.lastPollAtUnixMs << ",\"lastPollError\":\"" << jsonEscape(stats.lastPollError)
      << "\",\"definitions\":" << definitionBytes.size() << ",\"definitionFetches\":" << stats.definitionFetches
      << ",\"definitionFailures\":" << failedDefinitions.size() << ",\"instance\":\"" << instance
      << "\",\"client\":" << store.stats().json() << "}";
  return out.str();
}

}  // namespace coworkerd
