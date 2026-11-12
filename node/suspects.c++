#include "suspects.h"
#include "tenancy.h"
#include <capnp/message.h>
#include <algorithm>
#include <vector>
#include <chrono>
#include <cmath>
#include <iostream>
#include <optional>
#include <sstream>
#include <string_view>

namespace coworkerd {
namespace {
uint64_t wall() { return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count(); }
double mono() { return std::chrono::duration<double, std::milli>(
    std::chrono::steady_clock::now().time_since_epoch()).count(); }
// One suspicion of a node's object: the tenant is its field's name.
Suspect decode(const std::string& tenant, json::Json::Reader value) {
  auto integer = [&](const char* name) -> uint64_t {
    auto field = json::field(value, name);
    KJ_REQUIRE(field.isNumber() && std::isfinite(field.getNumber()) && field.getNumber() >= 0 &&
        field.getNumber() <= 9007199254740991.0 && std::floor(field.getNumber()) == field.getNumber(),
        "invalid suspect integer", name);
    return static_cast<uint64_t>(field.getNumber());
  };
  Suspect record{tenant, json::text(value, "workerId"), json::text(value, "sourceNodeId"),
      json::text(value, "processId"), json::text(value, "invocationId"),
      integer("generation"), integer("count"), integer("recognizedAtUnixMs"), integer("expiresAtUnixMs"),
      integer("durationMs"), true};
  KJ_REQUIRE(tenantName(record.tenant) && tenantOf(record.workerId) == record.tenant && !record.sourceServerId.empty() &&
      !record.invocationId.empty() && record.count > 0 && record.durationMs > 0 &&
      record.expiresAtUnixMs == record.recognizedAtUnixMs + record.durationMs,
      "invalid suspect record");
  return record;
}
// A suspicion as a node's object holds it (beside the others, under its tenant's name).
std::string objectRecord(const Suspect& record) {
  return "{\"workerId\":" + json::quote(record.workerId) + ",\"sourceNodeId\":" + json::quote(record.sourceServerId) +
      ",\"processId\":" + json::quote(record.processId) + ",\"generation\":" + std::to_string(record.generation) +
      ",\"invocationId\":" + json::quote(record.invocationId) + ",\"count\":" + std::to_string(record.count) +
      ",\"recognizedAtUnixMs\":" + std::to_string(record.recognizedAtUnixMs) +
      ",\"expiresAtUnixMs\":" + std::to_string(record.expiresAtUnixMs) + ",\"durationMs\":" + std::to_string(record.durationMs) + "}";
}
}
std::string Suspect::json(bool includeState) const {
  return "{\"tenant\":" + json::quote(tenant) + ",\"workerId\":" + json::quote(workerId) + ",\"sourceNodeId\":" + json::quote(sourceServerId) +
      ",\"processId\":" + json::quote(processId) + ",\"generation\":" + std::to_string(generation) +
      ",\"invocationId\":" + json::quote(invocationId) + ",\"count\":" + std::to_string(count) +
      ",\"recognizedAtUnixMs\":" + std::to_string(recognizedAtUnixMs) +
      ",\"expiresAtUnixMs\":" + std::to_string(expiresAtUnixMs) +
      ",\"durationMs\":" + std::to_string(durationMs) + ",\"synced\":" + (synced ? "true" : "false") +
      (includeState ? std::string(",\"state\":\"") + (probation ? "probation" : "suspected") + "\"" : "") + "}";
}
Suspects::Suspects(Membership* membership, std::string serverId, uint32_t baseMs, uint32_t maxMs, kj::Timer& timer,
    bool probationEnabled, uint32_t probationLimit)
    : membership(membership), serverId(std::move(serverId)), baseMs(baseMs), maxMs(maxMs),
      probationEnabled(probationEnabled), probationLimit(probationLimit), timer(timer) {
  KJ_REQUIRE(baseMs > 0 && maxMs >= baseMs, "invalid suspicion durations");
  KJ_REQUIRE(probationLimit > 0, "suspectProbationLimit must be positive");
  // The members' objects, as the membership reads them (installed before it starts: its first listing adopts them).
  if (keepsObjects()) {
    membership->onSuspects([this](const std::string& nodeId, const std::string& epoch, const std::string& bytes) {
      return adopt(nodeId, epoch, bytes);
    });
    membership->ownSuspects([this]() { return ownObject(); });
  }
}
Suspects::~Suspects() noexcept(false) {
  if (keepsObjects()) {
    membership->onSuspects(nullptr);
    membership->ownSuspects(nullptr);
  }
}
bool Suspects::keepsObjects() const { return membership != nullptr && membership->view().backend == "objectStore"; }
void Suspects::onTenantUnavailable(std::function<bool(const std::string&)> predicate) {
  tenantUnavailable = std::move(predicate);
}
bool Suspects::contains(const std::string& tenant) const {
  if (probationEnabled) return entries.contains(tenant);
  return unexpired(tenant);
}
bool Suspects::unexpired(const std::string& tenant) const {
  auto found = deadlines.find(tenant);
  return found != deadlines.end() && found->second > mono();
}
void Suspects::enterProbation(Suspect& record) {
  if (record.probation) return;
  record.probation = true;
  std::cout << "[suspect] {\"event\":\"probation\",\"nodeId\":" << json::quote(serverId)
      << ",\"observedAtUnixMs\":" << wall() << ",\"record\":" << record.json(true) << "}" << std::endl;
}
uint64_t Suspects::probationTicket(const std::string& tenant) {
  if (!probationEnabled) return 0;
  auto found = entries.find(tenant);
  if (found == entries.end() || unexpired(tenant)) return 0;
  enterProbation(found->second);
  return found->second.revision;
}
void Suspects::completed(const std::string& worker, uint64_t ticket, const std::string& process,
    uint64_t generation, const std::string& invocation) {
  if (!ticket || !probationEnabled) return;
  const auto tenant = tenantOf(worker);
  auto found = entries.find(tenant);
  if (found == entries.end() || found->second.revision != ticket || unexpired(tenant)) return;
  enterProbation(found->second);
  std::cout << "[suspect] {\"event\":\"released\",\"nodeId\":" << json::quote(serverId)
      << ",\"observedAtUnixMs\":" << wall() << ",\"record\":" << found->second.json(true)
      << ",\"completion\":{\"workerId\":" << json::quote(worker) << ",\"processId\":" << json::quote(process)
      << ",\"generation\":" << generation << ",\"invocationId\":" << json::quote(invocation) << "}}" << std::endl;
  deadlines.erase(tenant);
  entries.erase(found);
}
void Suspects::remember(Suspect record, const char* event) {
  const auto now = wall();
  if (record.expiresAtUnixMs <= now) return;
  auto previous = entries.find(record.tenant);
  const bool same = previous != entries.end() && previous->second.invocationId == record.invocationId &&
      previous->second.sourceServerId == record.sourceServerId && previous->second.expiresAtUnixMs == record.expiresAtUnixMs;
  record.revision = same ? previous->second.revision : ++nextRevision;
  record.probation = same && previous->second.probation;
  counts[record.tenant] = std::max(counts[record.tenant], record.count);
  deadlines[record.tenant] = mono() + (record.expiresAtUnixMs - now);
  entries[record.tenant] = record;
  std::cout << "[suspect] {\"event\":" << json::quote(event) << ",\"nodeId\":" << json::quote(serverId)
      << ",\"observedAtUnixMs\":" << now << ",\"record\":" << record.json(probationEnabled) << "}" << std::endl;
}
void Suspects::confirm(const std::string& worker, const std::string& process,
    uint64_t generation, const std::string& invocation) {
  // A culprit names a Worker of a tenant; anything else (an unattributed row) is no suspicion.
  const auto tenant = tenantOf(worker);
  if (!tenantName(tenant)) return;
  uint64_t duration = baseMs;
  if (contains(tenant)) duration = std::min<uint64_t>(uint64_t(maxMs), entries.at(tenant).durationMs * 2);
  const auto now = wall();
  Suspect record{tenant, worker, serverId, process, invocation, generation, ++counts[tenant], now, now + duration, duration, false};
  remember(record, "recognized");
  // Written at once (the maintenance loop wakes up).
  if (wake && wake->isWaiting()) wake->fulfill();
}
void Suspects::clear(const std::string& tenant, const char* reason) {
  auto found = entries.find(tenant);
  if (found == entries.end()) return;
  std::cout << "[suspect] {\"event\":\"cleared\",\"reason\":" << json::quote(reason)
      << ",\"nodeId\":" << json::quote(serverId) << ",\"observedAtUnixMs\":" << wall()
      << ",\"record\":" << found->second.json(true) << "}" << std::endl;
  if (std::string_view(reason) == "tenant-unavailable") ++clearedTenantUnavailable;
  else ++clearedCapacity;
  deadlines.erase(tenant);
  counts.erase(tenant);
  entries.erase(found);
}
void Suspects::expire() {
  for (auto it = entries.begin(); it != entries.end();) {
    if (unexpired(it->first)) { ++it; continue; }
    if (probationEnabled) {
      enterProbation(it->second);
      if (tenantUnavailable && tenantUnavailable(it->first)) {
        const auto tenant = (it++)->first;
        clear(tenant, "tenant-unavailable");
      } else ++it;
      continue;
    }
    std::cout << "[suspect] {\"event\":\"expired\",\"nodeId\":" << json::quote(serverId)
        << ",\"observedAtUnixMs\":" << wall() << ",\"record\":" << it->second.json() << "}" << std::endl;
    deadlines.erase(it->first); it = entries.erase(it);
  }
  if (!probationEnabled) return;
  std::vector<std::pair<double, std::string>> probation;
  for (const auto& [tenant, record] : entries)
    if (record.probation) probation.emplace_back(deadlines.at(tenant), tenant);
  if (probation.size() <= probationLimit) return;
  std::sort(probation.begin(), probation.end());
  for (size_t i = 0; i < probation.size() - probationLimit; ++i) clear(probation[i].second, "capacity");
}
std::string Suspects::ownObject() const {
  std::string out;
  for (const auto& [tenant, record] : entries) {
    if (record.sourceServerId != serverId || !unexpired(tenant)) continue;
    out += (out.empty() ? "{" : ",") + json::quote(tenant) + ":" + objectRecord(record);
  }
  return out.empty() ? out : out + "}";
}
bool Suspects::adopt(const std::string& nodeId, const std::string& epoch, const std::string& bytes) {
  // A member's object holds that member's suspicions only. A missing object never erases one: known suspicions expire on
  // their recorded deadline, even offline.
  capnp::MallocMessageBuilder message;
  auto value = message.initRoot<json::Json>();
  try {
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), value);
    KJ_REQUIRE(value.isObject(), "a suspects object is a JSON object");
  } catch (const kj::Exception& error) {
    ++invalidObjects;
    lastError = "the suspects object of " + nodeId + " epoch " + epoch + " does not read: " + error.getDescription().cStr();
    return false;
  }
  ++adoptedObjects;
  bool valid = true;
  for (auto entry : value.asReader().getObject()) {
    Suspect record;
    try { record = decode(entry.getName().cStr(), entry.getValue()); } catch (const kj::Exception&) {
      ++invalidObjects; valid = false; continue;
    }
    if (record.sourceServerId != nodeId) { ++invalidObjects; valid = false; continue; }
    if (record.expiresAtUnixMs <= wall()) continue;
    auto local = entries.find(record.tenant);
    if (local != entries.end() && local->second.invocationId == record.invocationId) {
      if (record.expiresAtUnixMs > local->second.expiresAtUnixMs) { remember(record, "shared"); ++adoptions; }
    } else if (local == entries.end() || record.expiresAtUnixMs >= local->second.expiresAtUnixMs) {
      // This node's own suspicions of an earlier epoch (left by a process that ended without removing them) come back
      // as its own and are written again with the new epoch's object.
      remember(record, record.sourceServerId == serverId ? "restored" : "shared");
      ++adoptions;
    }
  }
  return valid;
}
kj::Promise<void> Suspects::maintain() {
  while (true) {
    expire();
    if (keepsObjects()) {
      const auto& view = membership->view();
      const bool live = !view.epoch.empty() && view.leaseStatus == "active" && !view.selfExpired && !view.identityTaken;
      const auto body = ownObject();
      if (live && view.epoch != writtenEpoch && body.empty()) {
        // A new epoch has no object of its own yet; nothing to write.
        writtenEpoch = view.epoch;
        writtenBody.clear();
      }
      const bool due = body != writtenBody || view.epoch != writtenEpoch || (!body.empty() && mono() - writtenAt >= REWRITE_MS);
      if (live && due) {
        const auto epoch = view.epoch;
        try {
          co_await membership->writeSuspects(body);
          body.empty() ? ++removals : ++writes;
          writtenBody = body;
          writtenEpoch = epoch;
          writtenAt = mono();
          lastWriteAtUnixMs = wall();
          lastError.clear();
          // This node's suspicions the object now holds are synchronized.
          for (auto& [tenant, record] : entries) {
            if (record.sourceServerId != serverId || record.synced || !unexpired(tenant)) continue;
            auto synchronized = record;
            synchronized.synced = true;
            remember(synchronized, "synchronized");
          }
        } catch (const kj::Exception& error) {
          ++writeFailures;
          lastError = error.getDescription().cStr();
        }
      }
    }
    auto paf = kj::newPromiseAndFulfiller<void>();
    wake = kj::mv(paf.fulfiller);
    co_await paf.promise.exclusiveJoin(timer.afterDelay(CHECK_MS * kj::MILLISECONDS));
    wake = nullptr;
  }
}
kj::Promise<void> Suspects::run() { return maintain(); }
std::string Suspects::json() const {
  std::ostringstream out;
  out << "{\"configuration\":{\"baseMs\":" << baseMs << ",\"maxMs\":" << maxMs
      << (probationEnabled ? ",\"probation\":true,\"probationLimit\":" + std::to_string(probationLimit) : "")
      << "},\"backend\":" << (keepsObjects() ? "\"objectStore\"" : "\"local\"")
      << (probationEnabled ? ",\"cleared\":{\"tenantUnavailable\":" + std::to_string(clearedTenantUnavailable) +
          ",\"capacity\":" + std::to_string(clearedCapacity) + "}" : "")
      << ",\"objectEpoch\":" << json::quote(writtenEpoch) << ",\"objectSuspects\":" << [&] {
           size_t count = 0;
           for (const auto& [tenant, record] : entries) if (record.sourceServerId == serverId && record.synced && unexpired(tenant)) ++count;
           return count;
         }()
      << ",\"writes\":" << writes << ",\"removals\":" << removals << ",\"writeFailures\":" << writeFailures
      << ",\"adoptedObjects\":" << adoptedObjects << ",\"adoptions\":" << adoptions << ",\"invalidObjects\":" << invalidObjects
      << ",\"lastWriteAtUnixMs\":" << lastWriteAtUnixMs << ",\"lastError\":" << json::quote(lastError) << ",\"entries\":[";
  size_t i = 0;
  for (const auto& [tenant, record] : entries) if (contains(tenant)) {
    if (i++) out << ',';
    auto visible = record;
    if (probationEnabled && !unexpired(tenant)) visible.probation = true;
    out << visible.json(probationEnabled);
  }
  out << "]}";
  return out.str();
}
}  // namespace coworkerd
