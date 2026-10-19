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
// Every node's records: nodes/<nodeId>/suspects/<tenant> is one of them.
constexpr const char* PREFIX = NODES_PREFIX;
// The node and tenant of a suspicion key, or nothing.
std::optional<std::pair<std::string, std::string>> suspectKey(const std::string& key) {
  if (!key.starts_with(PREFIX)) return std::nullopt;
  const auto rest = key.substr(std::string_view(PREFIX).size());
  const auto slash = rest.find('/');
  if (slash == std::string::npos || rest.compare(slash, 10, "/suspects/") != 0) return std::nullopt;
  auto node = rest.substr(0, slash), tenant = rest.substr(slash + 10);
  if (node.empty() || !tenantName(tenant)) return std::nullopt;
  return std::pair{node, tenant};
}
uint64_t wall() { return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count(); }
double mono() { return std::chrono::duration<double, std::milli>(
    std::chrono::steady_clock::now().time_since_epoch()).count(); }
Suspect decode(const std::string& bytes) {
  capnp::MallocMessageBuilder message;
  auto value = message.initRoot<etcd::Json>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), value);
  auto integer = [&](const char* name) -> uint64_t {
    auto field = etcd::field(value, name);
    if (field.isString()) return etcd::integer(value, name);
    KJ_REQUIRE(field.isNumber() && std::isfinite(field.getNumber()) && field.getNumber() >= 0 &&
        field.getNumber() <= 9007199254740991.0 && std::floor(field.getNumber()) == field.getNumber(),
        "invalid suspect integer", name);
    return static_cast<uint64_t>(field.getNumber());
  };
  Suspect record{etcd::text(value, "tenant"), etcd::text(value, "workerId"), etcd::text(value, "sourceNodeId"),
      etcd::text(value, "processId"), etcd::text(value, "invocationId"),
      integer("generation"), integer("count"), integer("recognizedAtUnixMs"), integer("expiresAtUnixMs"),
      integer("durationMs"), true};
  KJ_REQUIRE(tenantName(record.tenant) && tenantOf(record.workerId) == record.tenant && !record.sourceServerId.empty() &&
      !record.invocationId.empty() && record.count > 0 && record.durationMs > 0 &&
      record.expiresAtUnixMs == record.recognizedAtUnixMs + record.durationMs,
      "invalid suspect record");
  return record;
}
}
std::string Suspect::json() const {
  return "{\"tenant\":" + etcd::quote(tenant) + ",\"workerId\":" + etcd::quote(workerId) + ",\"sourceNodeId\":" + etcd::quote(sourceServerId) +
      ",\"processId\":" + etcd::quote(processId) + ",\"generation\":" + std::to_string(generation) +
      ",\"invocationId\":" + etcd::quote(invocationId) + ",\"count\":" + std::to_string(count) +
      ",\"recognizedAtUnixMs\":" + std::to_string(recognizedAtUnixMs) +
      ",\"expiresAtUnixMs\":" + std::to_string(expiresAtUnixMs) +
      ",\"durationMs\":" + std::to_string(durationMs) + ",\"synced\":" + (synced ? "true" : "false") + "}";
}
Suspects::Suspects(const MembershipConfig& authority, std::string serverId,
    uint32_t baseMs, uint32_t maxMs, kj::Network& network, kj::Timer& timer,
    const kj::HttpHeaderTable& headers)
    : config(authority), serverId(std::move(serverId)), baseMs(baseMs), maxMs(maxMs), timer(timer) {
  KJ_REQUIRE(baseMs > 0 && maxMs >= baseMs, "invalid suspicion durations");
  if (config.backend == "etcd") {
    client = std::make_unique<EtcdClient>(config.endpoint, config.requestTimeoutMs, network, timer, headers);
    // Changes only: the prefix also holds every node's member key and usage rows.
    watcher = std::make_unique<EtcdWatcher>(*client, PREFIX, timer, config.retryMs,
        EtcdWatcher::ApplyChanges([this](const EtcdBatch& batch) { return apply(batch); }));
  }
}
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
  std::cout << "[suspect] {\"event\":" << etcd::quote(event) << ",\"nodeId\":" << etcd::quote(serverId)
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
  if (client) pending[tenant] = std::move(record);
}
void Suspects::expire() {
  for (auto it = entries.begin(); it != entries.end();) {
    if (contains(it->first)) { ++it; continue; }
    std::cout << "[suspect] {\"event\":\"expired\",\"nodeId\":" << etcd::quote(serverId)
        << ",\"observedAtUnixMs\":" << wall() << ",\"record\":" << it->second.json() << "}" << std::endl;
    deadlines.erase(it->first); pending.erase(it->first); it = entries.erase(it);
  }
}
kj::Promise<void> Suspects::apply(const EtcdBatch& batch) {
  // A missing authority key cannot erase a locally recognized, unsynchronized
  // offender. Known suspicions expire on their recorded deadline, even offline.
  for (const auto& change : batch.changes) {
    auto key = suspectKey(change.key);
    if (!key || !change.kv) continue;
    Suspect record;
    try { record = decode(change.kv->value); } catch (const kj::Exception&) { continue; }  // not a suspicion's record
    if (record.tenant != key->second || record.sourceServerId != key->first || change.kv->lease.empty()) continue;
    if (record.expiresAtUnixMs <= wall()) continue;
    auto local = entries.find(record.tenant);
    if (local != entries.end() && local->second.invocationId == record.invocationId) {
      if (!local->second.synced || record.expiresAtUnixMs > local->second.expiresAtUnixMs)
        remember(record, "synchronized");
      auto dirty = pending.find(record.tenant);
      if (dirty != pending.end() && dirty->second.invocationId == record.invocationId) pending.erase(dirty);
    } else if (local == entries.end() || record.expiresAtUnixMs >= local->second.expiresAtUnixMs) {
      remember(record, "shared");
    }
  }
  expire();
  co_return;
}
kj::Promise<void> Suspects::start() {
  if (!watcher) co_return;
  const auto until = mono() + config.startupTimeoutMs;
  while (true) {
    try { co_await watcher->fullRead(); co_return; }
    catch (const kj::Exception& error) {
      lastError = error.getDescription().cStr();
      KJ_REQUIRE(mono() < until, "suspect authority unavailable at startup", lastError.c_str());
    }
    co_await timer.afterDelay(config.retryMs * kj::MILLISECONDS);
  }
}
kj::Promise<void> Suspects::publish(Suspect record) {
  // This node's own key: no other node writes it (each node's etcd user writes only below its own prefix).
  auto key = nodeSuspectKey(serverId, record.tenant);
  auto snapshot = co_await client->range(key);
  auto found = snapshot.values.find(key);
  uint64_t revision = 0;
  if (found != snapshot.values.end()) {
    revision = found->second.modRevision;
    Suspect previous;
    bool readable = true;
    try { previous = decode(found->second.value); } catch (const kj::Exception&) { readable = false; }
    if (readable && previous.invocationId == record.invocationId) {
      EtcdBatch echo{found->second.modRevision, false, {{key, found->second}}};
      co_await apply(echo);
      co_return;
    }
    if (readable) {
      record.count = std::max(record.count, previous.count + 1);
      if (previous.expiresAtUnixMs > record.recognizedAtUnixMs)
        record.durationMs = std::min<uint64_t>(maxMs, std::max(record.durationMs, previous.durationMs * 2));
      record.expiresAtUnixMs = record.recognizedAtUnixMs + record.durationMs;
    }
  }
  if (record.expiresAtUnixMs <= wall()) co_return;
  auto remaining = record.expiresAtUnixMs - wall();
  auto grant = co_await client->request("/v3/lease/grant", "{\"TTL\":" + std::to_string((remaining + 999) / 1000) + "}");
  capnp::MallocMessageBuilder grantMessage;
  auto lease = grantMessage.initRoot<etcd::Json>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(grant.data(), grant.size()), lease);
  auto leaseId = etcd::text(lease, "ID");
  KJ_REQUIRE(!leaseId.empty(), "missing suspect lease");
  record.synced = true;
  auto result = co_await client->request("/v3/kv/txn", "{\"compare\":[{\"key\":" + etcd::quote(etcd::base64(key)) +
      ",\"target\":\"MOD\",\"result\":\"EQUAL\",\"mod_revision\":" + etcd::quote(std::to_string(revision)) +
      "}],\"success\":[{\"request_put\":{\"key\":" + etcd::quote(etcd::base64(key)) +
      ",\"value\":" + etcd::quote(etcd::base64(record.json())) + ",\"lease\":" + etcd::quote(leaseId) + "}}]}");
  capnp::MallocMessageBuilder resultMessage;
  auto transaction = resultMessage.initRoot<etcd::Json>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(result.data(), result.size()), transaction);
  if (!etcd::boolean(transaction, "succeeded")) co_return;
  auto local = entries.find(record.tenant);
  if (local != entries.end() && local->second.invocationId == record.invocationId) remember(record, "synchronized");
  auto dirty = pending.find(record.tenant);
  if (dirty != pending.end() && dirty->second.invocationId == record.invocationId) pending.erase(dirty);
  lastError.clear();
}
kj::Promise<void> Suspects::maintain() {
  while (true) {
    expire();
    if (client && !pending.empty()) {
      const auto record = pending.begin()->second;
      try { co_await publish(record); }
      catch (const kj::Exception& error) { lastError = error.getDescription().cStr(); }
    }
    co_await timer.afterDelay(config.retryMs * kj::MILLISECONDS);
  }
}
kj::Promise<void> Suspects::run() {
  if (watcher) return maintain().exclusiveJoin(watcher->run());
  return maintain();
}
std::string Suspects::json() const {
  std::ostringstream out;
  out << "{\"configuration\":{\"baseMs\":" << baseMs << ",\"maxMs\":" << maxMs
      << "},\"watchConnected\":" << (watcher && watcher->view().connected ? "true" : "false")
      << ",\"revision\":" << (watcher ? watcher->view().revision : 0)
      << ",\"watchLastError\":" << etcd::quote(watcher ? watcher->view().lastError : "")
      << ",\"lastError\":" << etcd::quote(lastError) << ",\"entries\":[";
  size_t i = 0;
  for (const auto& [tenant, record] : entries) if (contains(tenant)) {
    if (i++) out << ',';
    out << record.json();
  }
  out << "]}";
  return out.str();
}
}  // namespace coworkerd
