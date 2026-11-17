#include "alarm-discovery.h"
#include "json.h"
#include "support.h"
#include <kj/debug.h>
#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <limits>
#include <time.h>

namespace coworkerd {
namespace {
const std::string ROOT = "alarm-wake/by-owner/";
uint64_t threadTime() { timespec t{};clock_gettime(CLOCK_THREAD_CPUTIME_ID, &t);return uint64_t(t.tv_sec) * 1000000000ull + t.tv_nsec; }
bool hex(const std::string& s, size_t n) {
  return s.size() == n && std::all_of(s.begin(), s.end(), [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
}
template <typename T> bool number(const std::string& s, T& value, size_t width = 0) {
  if (s.empty() || (width && s.size() != width)) return false;
  auto [end, error] = std::from_chars(s.data(), s.data() + s.size(), value);
  return error == std::errc() && end == s.data() + s.size();
}
std::string wide(uint64_t n) { auto s = std::to_string(n);return std::string(20 - s.size(), '0') + s; }
std::string prefix(const ShardRoute& r, const std::string& ns, uint32_t shard) {
  auto value = ROOT + r.nodeId + "/" + wide(r.nodeEpoch) + "/" + ns + "/" + std::to_string(shard) + "/";
  return r.epoch ? value + wide(r.epoch) + "/" : value;
}
}
AlarmDiscovery::Scope::Scope(AlarmDiscovery& owner): owner(owner) { if (owner.measureCpu && !owner.cpuDepth++) started = threadTime(); }
AlarmDiscovery::Scope::~Scope() { if (owner.measureCpu && !--owner.cpuDepth) owner.cpuNs += threadTime() - started; }
std::optional<AlarmDiscovery::Marker> AlarmDiscovery::parse(const std::string& key) {
  if (!key.starts_with(ROOT)) return std::nullopt;
  std::vector<std::string> parts;size_t at = ROOT.size();
  for (;;) { auto end = key.find('/', at);parts.push_back(key.substr(at, end - at));if (end == key.npos) break;at = end + 1; }
  if (parts.size() != 6 || parts[0].empty() || !hex(parts[2], 64) || !hex(parts[5], 32)) return std::nullopt;
  Marker m;m.node = parts[0];m.ns = parts[2];
  if (!number(parts[1], m.member, 20) || !m.member || !number(parts[3], m.shard) || parts[3] != std::to_string(m.shard) ||
      !number(parts[4], m.epoch, 20) || !m.epoch) return std::nullopt;
  return m;
}
AlarmDiscovery::AlarmDiscovery(Settings settings, ObjectStore& store, kj::Timer& timer, const MembershipView& membership,
    AlarmPublication::ReadBudget& budget, std::function<bool()> ready, Take take, Tenant tenant, Rank rank,
    Valid active,
    std::function<void(const std::string&, uint32_t, uint64_t)> adopt,
    std::function<bool(const std::string&, uint32_t)> forgetPending, Held held, Published published, WaitPublished waitPublished, HintsReady hintsReady, Retire retire)
    : settings(std::move(settings)), store(store), timer(timer), membership(membership), readBudget(budget),
      ready(std::move(ready)), take(std::move(take)), tenant(std::move(tenant)), rank(std::move(rank)),
      namespaceActive(std::move(active)), held(std::move(held)), published(std::move(published)), waitPublished(std::move(waitPublished)), hintsReady(std::move(hintsReady)), retire(std::move(retire)), adopt(std::move(adopt)), forgetPending(std::move(forgetPending)), tasks(*this) {
  if (const auto* v = std::getenv("COWORKERD_ALARM_THREAD_CPU_FOR_TEST")) measureCpu = std::string_view(v) == "1";
  tasks.add(kj::evalLater([this]() { return loop(); }));
}
AlarmDiscovery::~AlarmDiscovery() noexcept(false) { stopping = true; }
bool AlarmDiscovery::living(const std::string& node, uint64_t epoch) const {
  auto it = membership.members.find(node);
  return it != membership.members.end() && it->second.epoch == std::to_string(epoch) && it->second.state != "draining";
}
void AlarmDiscovery::directory(std::string key, bool urgent) {
  if (queuedDirectories.insert(key).second) { if (urgent) directories.push_front(std::move(key));else directories.push_back(std::move(key)); }
}
void AlarmDiscovery::erase(const std::string& key) {
  evictable.erase(key);
  auto it = candidates.find(key);if (it == candidates.end()) return;auto& c = it->second;
  runnable.erase({c.priority, c.tenant, c.order, key});if (c.next) delayed.erase({c.next, key});
  if (c.owner) { auto w = ownerWaiters.find({c.owner->nodeId, c.owner->nodeEpoch});if (w != ownerWaiters.end()) { w->second.erase(key);if (w->second.empty()) ownerWaiters.erase(w); } }
  if (c.awaitingHints) --adoptions;
  candidates.erase(it);
}
void AlarmDiscovery::schedule(const std::string& key, double at) {
  auto& c = candidates.at(key);runnable.erase({c.priority, c.tenant, c.order, key});if (c.next) delayed.erase({c.next, key});
  c.next = at;c.order = ++serial;
  if (at > monotonicTimeMs()) delayed.emplace(at, key);else { c.next = 0;runnable.emplace(c.priority, c.tenant, c.order, key); }
  if (c.failures && !c.active && !c.watching && !c.awaitingHints) evictable.insert(key);
}
bool AlarmDiscovery::candidate(std::string ns, uint32_t shard, std::string marker, bool local) {
  Scope measured(*this);if (!namespaceActive(ns, shard)) { ++inactiveDropped;return true; }
  const auto key = ns + "/" + std::to_string(shard);
  if (!candidates.contains(key) && candidates.size() >= 16384) {
    // Only already-attempted retries may be displaced. Unprocessed rows stay in a resumable LIST cursor.
    if (!evictable.empty()) { const auto victim = *evictable.begin();erase(victim);++evicted; }
    else return false;
  }
  auto [it, inserted] = candidates.try_emplace(key);auto& c = it->second;
  if (inserted) { c.ns = std::move(ns);c.shard = shard;c.tenant = tenant(c.ns);++discovered; }
  c.localPending |= local;
  if (!marker.empty() && !c.markers.insert(marker).second) return true;
  ++c.version;
  if (inserted) {
    uint32_t r = 0;if (!marker.empty()) if (auto m = parse(marker)) r = rank(c.ns, c.shard, m->node);
    if (r != UINT32_MAX && r) { c.priority = 2;c.waitedMs = r * 1000;++deferred;schedule(key, monotonicTimeMs() + c.waitedMs); }
    else schedule(key, 0);
  }
  peakPending = std::max<uint64_t>(peakPending, candidates.size());pump();return true;
}
void AlarmDiscovery::cache(const std::string& key, ShardRoute route) {
  auto& c = candidates.at(key);const bool queued = runnable.erase({c.priority, c.tenant, c.order, key}) != 0;
  if (c.owner) { auto w = ownerWaiters.find({c.owner->nodeId, c.owner->nodeEpoch});if (w != ownerWaiters.end()) w->second.erase(key); }
  c.owner = std::move(route);c.forceResolve = false;if (c.owner->kind == ShardRoute::Kind::LOCAL) c.priority = 0;ownerWaiters[{c.owner->nodeId, c.owner->nodeEpoch}].insert(key);
  if (queued) runnable.emplace(c.priority, c.tenant, c.order, key);
}
void AlarmDiscovery::uncache(const std::string& key) {
  auto& c = candidates.at(key);const bool queued = runnable.erase({c.priority, c.tenant, c.order, key}) != 0;
  if (c.owner) {
    auto it = ownerWaiters.find({c.owner->nodeId, c.owner->nodeEpoch});
    if (it != ownerWaiters.end()) { it->second.erase(key);if (it->second.empty()) ownerWaiters.erase(it); }
  }
  if (c.owner && c.owner->kind == ShardRoute::Kind::LOCAL) c.forceResolve = true;
  if (c.awaitingHints) { --adoptions;c.awaitingHints = false; }
  c.watching = false;c.watchSerial = ++serial;c.owner.reset();c.waitedMs = 0;c.priority = 1;
  if (queued) runnable.emplace(c.priority, c.tenant, c.order, key);
}
void AlarmDiscovery::loadPending() {
  std::error_code error;
  for (std::filesystem::directory_iterator it(settings.pendingDirectory, error), end; !error && it != end; it.increment(error)) {
    const auto name = it->path().filename().string();
    if (name.size() < 74 || name[64] != '-' || !name.ends_with(".pending") || !hex(name.substr(0, 64), 64)) continue;
    uint32_t shard = 0;const auto part = name.substr(65, name.size() - 73);
    if (number(part, shard) && part == std::to_string(shard)) candidate(name.substr(0, 64), shard, {}, true);
  }
}
void AlarmDiscovery::failed(const kj::Exception&) { Scope measured(*this);++failures; }
void AlarmDiscovery::taskFailed(kj::Exception&& error) { failed(error); }
kj::Promise<ObjectListingPage> AlarmDiscovery::list(std::string key, uint32_t count, std::string token, std::string delimiter) {
  auto permit = co_await readBudget.acquire();peakReads = std::max(peakReads, readBudget.active);++lists;
  try { co_return co_await store.listPage(key, count, token, delimiter); }
  catch (const kj::Exception& error) {
    const std::string_view text(error.getDescription().cStr());
    if (key.starts_with(ROOT) && (text.find("HTTP 403") != text.npos || text.find("AccessDenied") != text.npos)) {
      ++denials;access = false;nextRoot = monotonicTimeMs() + 30000;
    }
    throw;
  }
}
kj::Promise<void> AlarmDiscovery::scan(std::string key) {
  bool paused = false;
  KJ_DEFER(--scans;if (!paused) { queuedDirectories.erase(key);scanCursors.erase(key); });
  try {
    const auto remainder = key.substr(ROOT.size());const bool root = key == ROOT;
    const bool node = !root && std::count(remainder.begin(), remainder.end(), '/') == 1;
    auto& cursor = scanCursors[key];
    for (;;) {
      if (!cursor.loaded) {
        auto page = co_await list(key, 1000, cursor.token, root || node ? "/" : "");
        Scope measured(*this);if (root) access = true;
        if (root || node) for (auto& group: page.commonPrefixes) {
          if (root) { directory(group);continue; }
          const auto id = remainder.substr(0, remainder.size() - 1);const auto part = group.substr(key.size(), group.size() - key.size() - 1);uint64_t epoch = 0;
          if (number(part, epoch, 20) && epoch) { if (!living(id, epoch) && !(id == settings.nodeId && epoch == memberEpoch)) directory(group); }
          else if (id == settings.nodeId) directory(group);
        }
        cursor.page = std::move(page.objects);cursor.token = std::move(page.nextToken);
        cursor.at = 0;cursor.date = page.dateUnixMs;cursor.loaded = true;
      }
      while (cursor.at < cursor.page.size()) {
        const auto object = cursor.page[cursor.at];
        if (auto m = parse(object.key);m && object.size == 0) {
          if (!living(m->node, m->member) && !candidate(m->ns, m->shard, object.key)) {
            paused = true;pausedDirectories.push_back(key);++scanPauses;co_return;
          }
        } else if (object.key.starts_with(ROOT + settings.nodeId + "/") && object.lastModifiedUnixMs && cursor.date > object.lastModifiedUnixMs + 60000) {
          try { co_await store.remove(object.key);++removed; } catch (const kj::Exception&) { ++failures; }
        }
        ++cursor.at;
      }
      if (cursor.token.empty()) break;
      cursor.page.clear();cursor.loaded = false;
    }
  } catch (const kj::Exception& error) { failed(error); }
}
kj::Promise<bool> AlarmDiscovery::hints(std::string ns, uint32_t shard) {
  const auto pre = "do/" + ns + "/shards/" + std::to_string(shard) + "/alarms/";std::string token;
  do {
    auto page = co_await list(pre, 1000, token);
    for (const auto& object: page.objects) {
      const auto part = object.key.substr(pre.size());
      if (part.size() == 66 && part.ends_with("/0") && hex(part.substr(0, 64), 64)) co_return true;
    }
    token = std::move(page.nextToken);
  } while (!token.empty());
  co_return false;
}
kj::Promise<bool> AlarmDiscovery::sourcePresent(std::string key) {
  const auto markers = candidates.at(key).markers;if (markers.empty()) co_return true;
  std::set<std::string> checked;
  for (const auto& marker: markers) {
    const auto pre = marker.substr(0, marker.rfind('/') + 1);if (!checked.insert(pre).second) continue;
    auto page = co_await list(pre, 1000);
    for (const auto& object: page.objects) if (object.size == 0 && markers.contains(object.key)) co_return true;
  }
  co_return false;
}
kj::Promise<void> AlarmDiscovery::process(std::string key) {
  bool closed = false;
  KJ_DEFER(
    --active;
    if (auto it = candidates.find(key);it != candidates.end()) {
      it->second.active = false;
      if (!closed) { it->second.failures = std::min(6u, it->second.failures + 1);schedule(key, monotonicTimeMs() + std::min(60000u, 1000u << it->second.failures)); }
      if (it->second.failures && !it->second.watching && !it->second.awaitingHints) evictable.insert(key);
    }
    pump();
  );
  auto& c = candidates.at(key);const auto version = c.version;const auto ns = c.ns;const auto shard = c.shard;
  bool done = false, drop = false;uint32_t delay = 0;
  try {
    if (!namespaceActive(ns, shard)) { ++inactiveDropped;drop = true; }
    else {
      if (c.owner && ((c.owner->kind == ShardRoute::Kind::LOCAL && held(ns, shard) != c.owner->epoch) || !living(c.owner->nodeId, c.owner->nodeEpoch))) uncache(key);
      const bool local = c.owner && c.owner->kind == ShardRoute::Kind::LOCAL;
      if (!c.forceResolve && !local && (c.waitedMs || c.owner || c.failures) && !co_await sourcePresent(key)) done = true;
      else {
        bool empty = false;std::string currentPrefix;
        if (!c.owner) {
          if (!co_await hints(ns, shard)) { empty = true;done = true; }
          else {
            const auto taking = monotonicTimeMs();auto route = co_await take(ns, shard, c.waitedMs);
            ++takeCalls;takeNs += uint64_t((monotonicTimeMs() - taking) * 1000000);
            if (route.code == "DO_BACKGROUND_DEFERRED") { delay = route.backgroundDelayMs;c.waitedMs += delay;++deferred; }
            else if (route.kind == ShardRoute::Kind::LOCAL || route.kind == ShardRoute::Kind::FORWARD) {
              if (route.kind == ShardRoute::Kind::LOCAL) { ++acquired;c.localPending = true;c.handoffStarted = taking; }else ++forwarded;
              cache(key, std::move(route));
            }
          }
        }
        if (c.owner) {
          const auto owner = *c.owner;currentPrefix = prefix(owner, ns, shard);
          if (owner.kind == ShardRoute::Kind::LOCAL) {
            if (held(ns, shard) != owner.epoch) uncache(key);
            else {
              auto state = published(ns, shard, owner.epoch);
              if (state == AlarmPublication::PublicationState::PENDING) {
                adopt(ns, shard, owner.epoch);state = published(ns, shard, owner.epoch);
              }
              if (state == AlarmPublication::PublicationState::PENDING) {
                const bool adoptionOnly = !hintsReady(ns, shard, owner.epoch);
                if (adoptionOnly && !c.awaitingHints) { c.awaitingHints = true;++adoptions; }
                c.watching = true;const auto token = c.watchSerial = ++serial;
                tasks.add(kj::evalLater([this, key, epoch = owner.epoch, adoptionOnly, token]() { return watch(key, epoch, adoptionOnly, token); }));
                closed = true;co_return;
              }
              done = true;
              if (held(ns, shard) != owner.epoch) { uncache(key);done = false; }
            }
          } else {
            auto own = co_await list(currentPrefix, 1000);
            const bool exists = std::any_of(own.objects.begin(), own.objects.end(), [&](const auto& object) {
              auto m = parse(object.key);return object.size == 0 && m && m->node == owner.nodeId && m->member == owner.nodeEpoch && m->ns == ns && m->shard == shard && (!owner.epoch || m->epoch == owner.epoch);
            });
            done = exists && owner.epoch && living(owner.nodeId, owner.nodeEpoch);
          }
        }
        if (done) {
          const auto markers = c.markers;
          for (const auto& marker: markers) {
            if (!empty && marker.starts_with(currentPrefix)) continue;
            if (c.owner && c.owner->kind == ShardRoute::Kind::LOCAL && !retire(ns, shard, c.owner->epoch, marker)) continue;
            co_await store.remove(marker);++removed;
          }
        }
      }
    }
    if (done && c.localPending) done = forgetPending(ns, shard);
  } catch (const kj::Exception& error) { failed(error); }
  catch (const std::exception&) { ++failures; }
  Scope measured(*this);
  if (done && c.handoffStarted) { ++handoffJobs;handoffNs += uint64_t((monotonicTimeMs() - c.handoffStarted) * 1000000);c.handoffStarted = 0; }
  if (drop || (done && c.version == version)) { closed = true;erase(key);co_return; }
  if (!delay) { c.failures = std::min(6u, c.failures + 1);delay = std::min(60000u, 1000u << c.failures); }
  schedule(key, monotonicTimeMs() + delay);closed = true;
}
kj::Promise<void> AlarmDiscovery::watch(std::string key, uint64_t epoch, bool adoptionOnly, uint64_t serial) {
  auto it = candidates.find(key);if (it == candidates.end()) co_return;
  const auto ns = it->second.ns;const auto shard = it->second.shard;
  auto state = AlarmPublication::PublicationState::PENDING;
  try { state = co_await waitPublished(ns, shard, epoch, adoptionOnly); }
  catch (const kj::Exception& error) { failed(error); }
  catch (const std::exception&) { ++failures; }
  it = candidates.find(key);if (it == candidates.end() || it->second.watchSerial != serial) co_return;
  auto& c = it->second;c.watching = false;
  if (c.awaitingHints) { --adoptions;c.awaitingHints = false; }
  if (state != AlarmPublication::PublicationState::PENDING || (adoptionOnly && hintsReady(ns, shard, epoch))) schedule(key, 0);
  else { c.failures = std::min(6u, c.failures + 1);schedule(key, monotonicTimeMs() + std::min(60000u, 1000u << c.failures)); }
  pump();
}
kj::Promise<void> AlarmDiscovery::loop() {
  for (;;) {
    co_await timer.afterDelay(200 * kj::MILLISECONDS);Scope measured(*this);
    if (!ready() || membership.backend != "objectStore" || !membership.durableProcesses || membership.selfExpired || membership.identityTaken || membership.draining) continue;
    const auto now = monotonicTimeMs();uint64_t epoch = 0;if (!number(membership.epoch, epoch) || !epoch) continue;
    if (epoch != memberEpoch) { memberEpoch = epoch;loadPending();nextRoot = 0; }
    if (memberChanges != membership.memberChanges) {
      memberChanges = membership.memberChanges;std::map<std::string, std::string> next;
      for (const auto& [id, member]: membership.members) if (member.state != "draining") next[id] = member.epoch;
      for (const auto& [id, old]: members) if (!next.contains(id) || next.at(id) != old) {
        directory(ROOT + id + "/", true);uint64_t oldEpoch = 0;if (number(old, oldEpoch)) invalidOwners.emplace_back(id, oldEpoch);
      }
      members = std::move(next);
    }
    unsigned slice = 0;
    while (!invalidOwners.empty() && slice++ < 64) {
      auto w = ownerWaiters.find(invalidOwners.front());
      if (w == ownerWaiters.end() || w->second.empty()) { if (w != ownerWaiters.end()) ownerWaiters.erase(w);invalidOwners.pop_front();continue; }
      const auto key = *w->second.begin();w->second.erase(w->second.begin());auto it = candidates.find(key);
      if (it != candidates.end() && !it->second.active) { uncache(key);it->second.failures = 0;schedule(key, 0); }
    }
    if (now >= nextRoot) { directory(ROOT);nextRoot = now + settings.intervalMs;loadPending(); }
    pumpDirectories();
    slice = 0;
    while (!delayed.empty() && delayed.begin()->first <= now && slice++ < 256) {
      const auto key = delayed.begin()->second;delayed.erase(delayed.begin());auto it = candidates.find(key);
      if (it != candidates.end() && !it->second.active) { it->second.next = 0;runnable.emplace(it->second.priority, it->second.tenant, it->second.order, key); }
    }
    pump();
  }
}
void AlarmDiscovery::pump() {
  if (pumping || stopping || !ready() || membership.selfExpired || membership.identityTaken || membership.draining) return;
  pumping = true;KJ_DEFER(pumping = false);unsigned slice = 0;
    pumpDirectories();
    while (slice++ < 64 && access && active < settings.takeovers && !runnable.empty()) {
      const auto priority = std::get<0>(*runnable.begin());
      auto it = runnable.upper_bound({priority, lastTenant, UINT64_MAX, ""});
      if (it == runnable.end() || std::get<0>(*it) != priority) it = runnable.begin();
      const auto candidateKey = std::get<3>(*it);auto pending = candidates.find(candidateKey);
      if (pending != candidates.end() && (!pending->second.owner || pending->second.owner->kind != ShardRoute::Kind::LOCAL) && active + adoptions >= settings.takeovers) return;
      const auto key = std::get<3>(*it);lastTenant = std::get<1>(*it);runnable.erase(it);auto found = candidates.find(key);
      if (found == candidates.end() || found->second.active) continue;
      evictable.erase(key);found->second.active = true;++active;peakActive = std::max<uint64_t>(peakActive, active);tasks.add(process(key));
    }
}
void AlarmDiscovery::pumpDirectories() {
  for (unsigned n = 0;n < 2 && scans < 2;++n) {
    std::string key;
    if (access && !pausedDirectories.empty() && (candidates.size() < 16384 || !evictable.empty())) {
      key = std::move(pausedDirectories.front());pausedDirectories.pop_front();++scanResumes;
    } else {
      auto it = std::find_if(directories.begin(), directories.end(), [this](const auto& key) {
        if (!access) return key == ROOT;
        const auto rest = key.substr(ROOT.size());
        return candidates.size() < 16384 || !evictable.empty() || key == ROOT ||
            std::count(rest.begin(), rest.end(), '/') == 1 || scanCursors.size() < 2;
      });
      if (it == directories.end()) break;
      key = std::move(*it);directories.erase(it);
    }
    ++scans;tasks.add(kj::evalLater([this, key]() { return scan(key); }));
  }
}
std::string AlarmDiscovery::json() const {
  const auto state = !membership.durableProcesses ? "disabled-no-durable-processes" : access ? "enabled" : denials ? "disabled-access-denied" : "checking";
  return "{\"state\":" + json::quote(state) + ",\"scanIntervalMs\":" + std::to_string(settings.intervalMs) +
      ",\"takeoverLimit\":" + std::to_string(settings.takeovers) + ",\"readLimit\":" + std::to_string(AlarmPublication::ReadBudget::limit) +
      ",\"active\":" + std::to_string(active) + ",\"pendingHintAdoptions\":" + std::to_string(adoptions) + ",\"peakActive\":" + std::to_string(peakActive) +
      ",\"sharedPeakReads\":" + std::to_string(peakReads) + ",\"pending\":" + std::to_string(candidates.size()) +
      ",\"peakPending\":" + std::to_string(peakPending) + ",\"candidateLimit\":16384,\"evicted\":" + std::to_string(evicted) +
      ",\"pausedScans\":" + std::to_string(pausedDirectories.size()) + ",\"scanPauses\":" + std::to_string(scanPauses) + ",\"scanResumes\":" + std::to_string(scanResumes) +
      ",\"discovered\":" + std::to_string(discovered) + ",\"localResults\":" + std::to_string(acquired) +
      ",\"forwardedResults\":" + std::to_string(forwarded) + ",\"removed\":" + std::to_string(removed) +
      ",\"listRequests\":" + std::to_string(lists) + ",\"failures\":" + std::to_string(failures) +
      ",\"denials\":" + std::to_string(denials) + ",\"inactiveDropped\":" + std::to_string(inactiveDropped) +
      ",\"deferred\":" + std::to_string(deferred) + ",\"takeCalls\":" + std::to_string(takeCalls) +
      ",\"takeNs\":" + std::to_string(takeNs) + ",\"handoffJobs\":" + std::to_string(handoffJobs) + ",\"handoffNs\":" + std::to_string(handoffNs) + ",\"cpuNs\":" + std::to_string(cpuNs) + ",\"tickMs\":200}";
}
}  // namespace coworkerd
