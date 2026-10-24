#include "suspects.h"
#include "tenancy.h"
#include <capnp/message.h>
#include <chrono>
#include <cmath>
#include <iostream>
#include <optional>
#include <sstream>

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
std::string Suspect::json() const {
  return "{\"tenant\":" + json::quote(tenant) + ",\"workerId\":" + json::quote(workerId) + ",\"sourceNodeId\":" + json::quote(sourceServerId) +
      ",\"processId\":" + json::quote(processId) + ",\"generation\":" + std::to_string(generation) +
      ",\"invocationId\":" + json::quote(invocationId) + ",\"count\":" + std::to_string(count) +
      ",\"recognizedAtUnixMs\":" + std::to_string(recognizedAtUnixMs) +
      ",\"expiresAtUnixMs\":" + std::to_string(expiresAtUnixMs) +
      ",\"durationMs\":" + std::to_string(durationMs) + ",\"synced\":" + (synced ? "true" : "false") + "}";
}
Suspects::Suspects(Membership* membership, std::string serverId, uint32_t baseMs, uint32_t maxMs, kj::Timer& timer)
    : membership(membership), serverId(std::move(serverId)), baseMs(baseMs), maxMs(maxMs), timer(timer) {
  KJ_REQUIRE(baseMs > 0 && maxMs >= baseMs, "invalid suspicion durations");
  // The members' objects, as the membership reads them (installed before it starts: its first listing adopts them).
  if (keepsObjects()) {
    membership->onSuspects([this](const std::string& nodeId, const std::string& epoch, const std::string& bytes) {
      adopt(nodeId, epoch, bytes);
    });
  }
}
Suspects::~Suspects() noexcept(false) {
  if (keepsObjects()) membership->onSuspects(nullptr);
}
bool Suspects::keepsObjects() const { return membership != nullptr && membership->view().backend == "objectStore"; }
bool Suspects::contains(const std::string& tenant) const {
  auto found = deadlines.find(tenant);
  return found != deadlines.end() && found->second > mono();
}
void Suspects::remember(Suspect record, const char* event) {
  const auto now = wall();
  if (record.expiresAtUnixMs <= now) return;
  counts[record.tenant] = std::max(counts[record.tenant], record.count);
  deadlines[record.tenant] = mono() + (record.expiresAtUnixMs - now);
  entries[record.tenant] = record;
  std::cout << "[suspect] {\"event\":" << json::quote(event) << ",\"nodeId\":" << json::quote(serverId)
      << ",\"observedAtUnixMs\":" << now << ",\"record\":" << record.json() << "}" << std::endl;
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
void Suspects::expire() {
  for (auto it = entries.begin(); it != entries.end();) {
    if (contains(it->first)) { ++it; continue; }
    std::cout << "[suspect] {\"event\":\"expired\",\"nodeId\":" << json::quote(serverId)
        << ",\"observedAtUnixMs\":" << wall() << ",\"record\":" << it->second.json() << "}" << std::endl;
    deadlines.erase(it->first); it = entries.erase(it);
  }
}
std::string Suspects::ownObject() const {
  std::string out;
  for (const auto& [tenant, record] : entries) {
    if (record.sourceServerId != serverId || !contains(tenant)) continue;
    out += (out.empty() ? "{" : ",") + json::quote(tenant) + ":" + objectRecord(record);
  }
  return out.empty() ? out : out + "}";
}
void Suspects::adopt(const std::string& nodeId, const std::string& epoch, const std::string& bytes) {
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
    return;
  }
  ++adoptedObjects;
  for (auto entry : value.asReader().getObject()) {
    Suspect record;
    try { record = decode(entry.getName().cStr(), entry.getValue()); } catch (const kj::Exception&) { ++invalidObjects; continue; }
    if (record.sourceServerId != nodeId || record.expiresAtUnixMs <= wall()) continue;
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
            if (record.sourceServerId != serverId || record.synced || !contains(tenant)) continue;
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
      << "},\"backend\":" << (keepsObjects() ? "\"objectStore\"" : "\"local\"")
      << ",\"objectEpoch\":" << json::quote(writtenEpoch) << ",\"objectSuspects\":" << [&] {
           size_t count = 0;
           for (const auto& [tenant, record] : entries) if (record.sourceServerId == serverId && record.synced && contains(tenant)) ++count;
           return count;
         }()
      << ",\"writes\":" << writes << ",\"removals\":" << removals << ",\"writeFailures\":" << writeFailures
      << ",\"adoptedObjects\":" << adoptedObjects << ",\"adoptions\":" << adoptions << ",\"invalidObjects\":" << invalidObjects
      << ",\"lastWriteAtUnixMs\":" << lastWriteAtUnixMs << ",\"lastError\":" << json::quote(lastError) << ",\"entries\":[";
  size_t i = 0;
  for (const auto& [tenant, record] : entries) if (contains(tenant)) {
    if (i++) out << ',';
    out << record.json();
  }
  out << "]}";
  return out.str();
}
}  // namespace coworkerd
