#include "alarm-discovery.h"
#include "json.h"
#include "support.h"
#include <kj/debug.h>
#include <algorithm>
#include <charconv>
#include <limits>

namespace coworkerd {
namespace {
const std::string ROOT = "alarm-wake/by-owner/";
bool hex(const std::string& s, size_t n) {
  return s.size() == n && std::all_of(s.begin(), s.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  });
}
template <typename T> bool number(const std::string& s, T& value, size_t width = 0) {
  if (s.empty() || (width && s.size() != width)) return false;
  auto [end, error] = std::from_chars(s.data(), s.data() + s.size(), value);
  return error == std::errc() && end == s.data() + s.size();
}
std::string wide(uint64_t n) { auto s = std::to_string(n);return std::string(20 - s.size(), '0') + s; }
std::string prefix(const ShardRoute& r, const std::string& ns, uint32_t shard) {
  return ROOT + r.nodeId + "/" + wide(r.nodeEpoch) + "/" + ns + "/" + std::to_string(shard) + "/" + wide(r.epoch) + "/";
}
}
std::optional<AlarmDiscovery::Marker> AlarmDiscovery::parse(const std::string& key) {
  if (!key.starts_with(ROOT)) return std::nullopt;
  std::vector<std::string> parts;size_t at = ROOT.size();
  for (;;) { auto end = key.find('/', at);parts.push_back(key.substr(at, end - at));if (end == key.npos) break;at = end + 1; }
  if (parts.size() != 6 || parts[0].empty() || !hex(parts[2], 64) || !hex(parts[5], 32)) return std::nullopt;
  Marker m;m.node = parts[0];m.ns = parts[2];
  if (!number(parts[1], m.member, 20) || !m.member || !number(parts[3], m.shard) ||
      parts[3] != std::to_string(m.shard) || !number(parts[4], m.epoch, 20) || !m.epoch) return std::nullopt;
  return m;
}
AlarmDiscovery::AlarmDiscovery(Settings settings, ObjectStore& store, kj::Timer& timer,
    const MembershipView& membership, uint32_t& readActive, std::function<bool()> ready, Take take, Tenant tenant)
    : settings(std::move(settings)), store(store), timer(timer), membership(membership), readActive(readActive),
      ready(std::move(ready)), take(std::move(take)), tenant(std::move(tenant)), tasks(*this) {
  tasks.add(kj::evalLater([this]() { return loop(); }));
}
AlarmDiscovery::~AlarmDiscovery() noexcept(false) { stopping = true; }
bool AlarmDiscovery::living(const std::string& node, uint64_t epoch) const {
  auto it = membership.members.find(node);
  return it != membership.members.end() && it->second.epoch == std::to_string(epoch);
}
void AlarmDiscovery::directory(std::string value) {
  if (queuedDirectories.insert(value).second) directories.push_back(std::move(value));
}
void AlarmDiscovery::candidate(std::string ns, uint32_t shard, std::string marker) {
  const auto key = ns + "/" + std::to_string(shard);auto [it, inserted] = candidates.try_emplace(key);
  auto& c = it->second;
  if (inserted) { c.ns = std::move(ns);c.shard = shard;c.tenant = tenant(c.ns);c.order = ++serial;++discovered; }
  if (!marker.empty() && !c.markers.insert(std::move(marker)).second) return;
  ++c.version;
  if (inserted) runnable.emplace(c.tenant, c.order, key);
  peakPending = std::max<uint64_t>(peakPending, candidates.size());
}
void AlarmDiscovery::loadPending() {
  std::error_code error;
  for (std::filesystem::directory_iterator it(settings.pendingDirectory, error), end; !error && it != end; it.increment(error)) {
    const auto name = it->path().filename().string();
    if (name.size() < 74 || name[64] != '-' || !name.ends_with(".pending") || !hex(name.substr(0, 64), 64)) continue;
    uint32_t shard = 0;const auto part = name.substr(65, name.size() - 65 - 8);
    if (number(part, shard) && part == std::to_string(shard)) candidate(name.substr(0, 64), shard);
  }
}
void AlarmDiscovery::failed(const kj::Exception& error) {
  ++failures;const std::string_view text(error.getDescription().cStr());
  if (text.find("HTTP 403") != text.npos || text.find("AccessDenied") != text.npos) {
    ++denials;access = false;nextRoot = monotonicTimeMs() + 30000;
  }
}
void AlarmDiscovery::taskFailed(kj::Exception&& error) { failed(error); }
kj::Promise<ObjectListingPage> AlarmDiscovery::list(std::string key, uint32_t count, std::string token, std::string delimiter) {
  while (readActive >= settings.reads) co_await timer.afterDelay(10 * kj::MILLISECONDS);
  ++readActive;peakReads = std::max(peakReads, readActive);++lists;
  KJ_DEFER(--readActive);
  co_return co_await store.listPage(key, count, token, delimiter);
}
kj::Promise<void> AlarmDiscovery::scan(std::string key) {
  KJ_DEFER(--scans;queuedDirectories.erase(key));
  try {
    const auto remainder = key.substr(ROOT.size());
    const bool root = key == ROOT, node = !root && std::count(remainder.begin(), remainder.end(), '/') == 1;
    std::string token;
    do {
      while (candidates.size() >= 4096) co_await timer.afterDelay(20 * kj::MILLISECONDS);
      auto page = co_await list(key, 1000, token, root || node ? "/" : "");
      if (root) access = true;
      if (root || node) {
        for (auto& group: page.commonPrefixes) {
          if (root) { directory(group);continue; }
          const auto id = remainder.substr(0, remainder.size() - 1);
          const auto part = group.substr(key.size(), group.size() - key.size() - 1);uint64_t epoch = 0;
          if (number(part, epoch, 20) && epoch) {
            if (!living(id, epoch) || id == settings.nodeId) directory(group);
          } else if (id == settings.nodeId) directory(group);
        }
      }
      for (const auto& object: page.objects) {
        if (auto m = parse(object.key); m && object.size == 0) {
          if (!living(m->node, m->member)) candidate(m->ns, m->shard, object.key);
        } else if (object.key.starts_with(ROOT + settings.nodeId + "/") &&
            object.lastModifiedUnixMs && page.dateUnixMs > object.lastModifiedUnixMs + 60000) {
          co_await store.remove(object.key);++removed;
        }
      }
      token = std::move(page.nextToken);
    } while (!token.empty());
  } catch (const kj::Exception& error) { failed(error); }
}
void AlarmDiscovery::finish(const std::string& key, uint64_t version, bool done) {
  auto it = candidates.find(key);if (it == candidates.end()) return;
  auto& c = it->second;c.active = false;
  if (done && c.version == version) { candidates.erase(it);return; }
  delayed.emplace(monotonicTimeMs() + 2000, key);
}
kj::Promise<void> AlarmDiscovery::process(std::string key) {
  KJ_DEFER(--active);
  auto& c = candidates.at(key);const auto version = c.version;const auto ns = c.ns;const auto shard = c.shard;
  const auto markers = c.markers;bool done = false;
  try {
    // Empty hints are garbage, and must never cause an otherwise empty shard to acquire an epoch.
    auto hints = co_await list("do/" + ns + "/shards/" + std::to_string(shard) + "/alarms/", 1);
    const bool empty = hints.objects.empty();std::string currentPrefix;
    if (!empty) {
      auto route = co_await take(ns, shard);
      if (route.kind == ShardRoute::Kind::LOCAL) ++acquired;
      else if (route.kind == ShardRoute::Kind::FORWARD) ++forwarded;
      if ((route.kind == ShardRoute::Kind::LOCAL || route.kind == ShardRoute::Kind::FORWARD) && route.epoch) {
        currentPrefix = prefix(route, ns, shard);
        auto own = co_await list(currentPrefix, 1000);
        done = std::any_of(own.objects.begin(), own.objects.end(), [&](const auto& o) {
          return o.size == 0 && o.key.starts_with(currentPrefix) && hex(o.key.substr(currentPrefix.size()), 32);
        });
      }
    } else done = true;
    if (done) for (const auto& marker: markers) {
      if (!empty && marker.starts_with(currentPrefix)) continue;
      co_await store.remove(marker);++removed;
    }
  } catch (const kj::Exception& error) { failed(error); }
  finish(key, version, done);
}
kj::Promise<void> AlarmDiscovery::loop() {
  for (;;) {
    co_await timer.afterDelay(10 * kj::MILLISECONDS);
    const auto now = monotonicTimeMs();
    const auto delay = lastLoop ? std::max(0.0, now - lastLoop - 10) : 0;
    maxLoopDelayMs = std::max(maxLoopDelayMs, delay);lastLoop = now;
    const auto second = uint64_t(now / 1000);auto& sample = loopSeconds[second % loopSeconds.size()];
    if (sample.first != second) sample = {second, delay};else sample.second = std::max(sample.second, delay);
    if (!ready() || membership.backend != "objectStore" || membership.selfExpired || membership.identityTaken || membership.draining) continue;
    uint64_t epoch = 0;if (!number(membership.epoch, epoch) || !epoch) continue;
    if (epoch != memberEpoch) { memberEpoch = epoch;loadPending();nextRoot = 0; }
    std::map<std::string, std::string> next;
    for (const auto& [id, member]: membership.members) next[id] = member.epoch;
    if (next != members) {
      for (const auto& [id, old]: members) if (!next.contains(id) || next.at(id) != old) {
        directory(ROOT + id + "/");lastGoneNode = id;lastGoneMs = now;
      }
      members = std::move(next);
    }
    if (now >= nextRoot) { directory(ROOT);nextRoot = now + settings.intervalMs; }
    if (!scans && !directories.empty()) {
      auto it = access ? directories.begin() : std::find(directories.begin(), directories.end(), ROOT);
      if (it != directories.end()) { auto key = std::move(*it);directories.erase(it);++scans;tasks.add(scan(std::move(key))); }
    }
    unsigned slice = 0;
    while (!delayed.empty() && delayed.begin()->first <= now && slice++ < 64) {
      auto key = std::move(delayed.begin()->second);delayed.erase(delayed.begin());auto it = candidates.find(key);
      if (it != candidates.end() && !it->second.active) { it->second.order = ++serial;runnable.emplace(it->second.tenant, it->second.order, key); }
    }
    while (access && active < settings.takeovers && !runnable.empty()) {
      auto it = runnable.upper_bound({lastTenant, std::numeric_limits<uint64_t>::max(), ""});if (it == runnable.end()) it = runnable.begin();
      auto key = std::get<2>(*it);lastTenant = std::get<0>(*it);runnable.erase(it);
      auto found = candidates.find(key);if (found == candidates.end() || found->second.active) continue;
      found->second.active = true;++active;peakActive = std::max<uint64_t>(peakActive, active);tasks.add(process(std::move(key)));
    }
  }
}
std::string AlarmDiscovery::json() const {
  std::string samples = "[";
  for (const auto& [second, delay]: loopSeconds) if (second) {
    if (samples.size() > 1) samples += ",";
    samples += "{\"monotonicSecond\":" + std::to_string(second) + ",\"maxDelayMs\":" + std::to_string(delay) + "}";
  }
  samples += "]";
  return "{\"state\":" + json::quote(access ? "enabled" : denials ? "disabled-access-denied" : "checking") +
      ",\"scanIntervalMs\":" + std::to_string(settings.intervalMs) + ",\"takeoverLimit\":" + std::to_string(settings.takeovers) +
      ",\"readLimit\":" + std::to_string(settings.reads) + ",\"active\":" + std::to_string(active) +
      ",\"peakActive\":" + std::to_string(peakActive) + ",\"sharedPeakReads\":" + std::to_string(peakReads) +
      ",\"pending\":" + std::to_string(candidates.size()) + ",\"peakPending\":" + std::to_string(peakPending) +
      ",\"discovered\":" + std::to_string(discovered) + ",\"localResults\":" + std::to_string(acquired) +
      ",\"forwardedResults\":" + std::to_string(forwarded) + ",\"removed\":" + std::to_string(removed) +
      ",\"listRequests\":" + std::to_string(lists) + ",\"failures\":" + std::to_string(failures) +
      ",\"denials\":" + std::to_string(denials) + ",\"lastGoneNode\":" + json::quote(lastGoneNode) +
      ",\"lastGoneMonotonicMs\":" + std::to_string(lastGoneMs) + ",\"probeMonotonicMs\":" + std::to_string(monotonicTimeMs()) +
      ",\"loopProbeIntervalMs\":10,\"maxLoopDelayMs\":" + std::to_string(maxLoopDelayMs) + ",\"loopSeconds\":" + samples + "}";
}
}  // namespace coworkerd
