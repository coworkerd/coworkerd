#include "secret-dictionary.h"
#include "log-cleaning.h"
#include "support.h"

#include <algorithm>
#include <cmath>
#include <sstream>

namespace coworkerd {

namespace {
// A value's needles: itself and its JSON-escaped form (the forms a line can hold).
std::vector<std::string> needlesOf(const std::string& value) {
  std::vector<std::string> needles;
  if (!value.empty()) addRedaction(needles, value);
  return needles;
}
size_t bytesOf(const std::string& value) {
  size_t bytes = 0;
  for (const auto& needle : needlesOf(value)) bytes += platformLogNeedleBytes(needle.size());
  return bytes;
}
std::string kib(size_t bytes) { return std::to_string((bytes + 1023) / 1024) + " KiB"; }
}  // namespace

SecretDictionary::SecretDictionary(Limits limits, Sink sink): limits(limits), sink(std::move(sink)) {}

void SecretDictionary::setCurrent(std::map<std::string, std::set<std::string>> values) {
  if (values == current) return;
  current = std::move(values);
  ++revision;
}

bool SecretDictionary::holds(const std::string& value) const {
  const auto needles = needlesOf(value);
  return !needles.empty() && std::all_of(needles.begin(), needles.end(),
      [&](const auto& needle) { return published.contains(needle); });
}

size_t SecretDictionary::earlierBytes(const std::string& tenant) const {
  const auto now = current.find(tenant);
  size_t bytes = 0;
  for (auto at = held.lower_bound({tenant, std::string()}); at != held.end() && at->first.first == tenant; ++at)
    if (now == current.end() || !now->second.contains(at->first.second)) bytes += at->second.bytes;
  return bytes;
}

std::vector<std::string> SecretDictionary::earlierValues() const {
  std::vector<std::string> values;
  for (const auto& [pair, entry] : held) {
    const auto now = current.find(pair.first);
    if (now == current.end() || !now->second.contains(pair.second)) values.push_back(pair.second);
  }
  return values;
}

void SecretDictionary::unhold(const std::string& tenant, const std::string& value) {
  auto found = held.find({tenant, value});
  if (found == held.end() || --found->second.holders > 0) return;
  held.erase(found);
  for (const auto& needle : needlesOf(value)) {
    auto ref = needleRefs.find(needle);
    if (ref == needleRefs.end() || --ref->second > 0) continue;
    needleRefs.erase(ref);
    liveBytes -= platformLogNeedleBytes(needle.size());
    --livePatterns;
  }
}

bool SecretDictionary::hold(Holder& holder, const std::set<std::string>& values, double nowMs) {
  std::vector<std::string> added, needles;
  for (const auto& value : values) {
    if (value.empty() || holder.values.contains(value)) continue;
    added.push_back(value);
    auto& entry = held[{holder.tenant, value}];
    if (++entry.holders > 1) continue;
    entry.bytes = bytesOf(value);
    for (auto& needle : needlesOf(value)) {
      if (++needleRefs[needle] == 1) {
        liveBytes += platformLogNeedleBytes(needle.size());
        ++livePatterns;
      }
      needles.push_back(std::move(needle));
    }
  }
  if (!publish(needles, nowMs)) {
    for (const auto& value : added) unhold(holder.tenant, value);
    return false;
  }
  holder.values.insert(added.begin(), added.end());
  const auto now = current.find(holder.tenant);
  if (std::any_of(added.begin(), added.end(), [&](const auto& value) {
        return now == current.end() || !now->second.contains(value);
      })) ++revision;
  return true;
}

// An admitted call is sent with the values current when its descriptor is
// built: a reserved value that left the view is never sent, and goes at once.
bool SecretDictionary::pruneReservation(Holder& holder) {
  const auto now = current.find(holder.tenant);
  bool pruned = false;
  for (auto value = holder.values.begin(); value != holder.values.end();) {
    if (now != current.end() && now->second.contains(*value)) { ++value; continue; }
    unhold(holder.tenant, *value);
    value = holder.values.erase(value);
    pruned = true;
  }
  return pruned;
}

bool SecretDictionary::publish(const std::vector<std::string>& needles, double nowMs) {
  std::vector<std::string> missing;
  for (const auto& needle : needles) if (!published.contains(needle)) missing.push_back(needle);
  if (missing.empty()) return true;
  if (sink.append && sink.append(missing)) {
    for (auto& needle : missing) {
      publishedBytes += platformLogNeedleBytes(needle.size());
      published.insert(std::move(needle));
    }
    return true;
  }
  // The region is full of released needles, or this is too much for it: rebuild
  // it with what is held now (the missing needles are held already).
  return compact(nowMs);
}

bool SecretDictionary::compact(double nowMs) {
  std::vector<std::string> live;
  live.reserve(needleRefs.size());
  for (const auto& [needle, count] : needleRefs) live.push_back(needle);
  if (!sink.replace || !sink.replace(live)) {
    ++sinkFailures;
    return false;
  }
  std::erase_if(published, [&](const auto& needle) { return !needleRefs.contains(needle); });
  publishedBytes = liveBytes;
  lastCompactMs = nowMs;
  ++compactions;
  return true;
}

void SecretDictionary::countRefusal(const std::string& tenant, bool full) {
  ++(full ? refusedFull : refusedEarlier);
  auto found = refusedByTenant.find(tenant);
  if (found == refusedByTenant.end()) {
    if (refusedByTenant.size() >= 256) return;
    found = refusedByTenant.emplace(tenant, std::pair<uint64_t, uint64_t>{0, 0}).first;
  }
  ++(full ? found->second.first : found->second.second);
}

// When the tenant's earlier values are all released if their isolates get no
// more calls: an isolate is evicted idleEvictMs after its last call, then the
// grace passes. A process that names no isolates keeps them until it is replaced.
std::string SecretDictionary::recovery(const std::string& tenant, double nowMs) const {
  const auto now = current.find(tenant);
  double latest = nowMs;
  for (const auto& [key, holder] : holders) {
    if (holder.tenant != tenant) continue;
    const bool earlier = std::any_of(holder.values.begin(), holder.values.end(), [&](const auto& value) {
      return now == current.end() || !now->second.contains(value);
    });
    if (!earlier) continue;
    if (!holder.processId.empty() && (!holder.native || limits.idleEvictMs <= 0))
      return "until the process that holds them is replaced";
    const auto until = holder.processId.empty() ? holder.untilMs
        : std::max(holder.untilMs, holder.lastMs + limits.idleEvictMs);
    latest = std::max(latest, holder.releasedMs ? *holder.releasedMs + limits.graceMs : until + limits.graceMs);
  }
  return "within about " + std::to_string(static_cast<uint64_t>(std::ceil(std::max(1.0, latest - nowMs) / 1000))) +
      " s if those isolates get no more calls";
}

std::optional<std::string> SecretDictionary::admit(const std::string& tenant, const std::set<std::string>& values,
    double nowMs, double untilMs) {
  if (values.empty()) return std::nullopt;
  if (const auto earlier = earlierBytes(tenant); earlier > limits.tenantEarlierBytes) {
    countRefusal(tenant, false);
    return "this node still holds " + kib(earlier) + " of this tenant's earlier secret values for isolates that may "
        "still use them (at most " + kib(limits.tenantEarlierBytes) + " per tenant): its calls with secrets are refused "
        "here until they are released, " + recovery(tenant, nowMs);
  }
  size_t addBytes = 0, addPatterns = 0;
  std::set<std::string> counted;
  for (const auto& value : values) {
    if (held.contains({tenant, value})) continue;
    for (auto& needle : needlesOf(value)) {
      if (needleRefs.contains(needle) || !counted.insert(needle).second) continue;
      addBytes += platformLogNeedleBytes(needle.size());
      ++addPatterns;
    }
  }
  const auto full = [&] {
    countRefusal(tenant, true);
    return "this node's secret dictionary is full (" + kib(liveBytes) + " of " + kib(limits.nodeBytes) + ", " +
        std::to_string(livePatterns) + " of " + std::to_string(limits.nodePatterns) + " values): this tenant's calls "
        "with secrets are refused here, and another node may take them";
  };
  if (liveBytes + addBytes > limits.nodeBytes || livePatterns + addPatterns > limits.nodePatterns) return full();
  auto& holder = holders[Key{tenant, std::string(), 0, std::string()}];
  holder.tenant = tenant;
  if (pruneReservation(holder)) ++revision;
  if (!hold(holder, values, nowMs)) {
    if (holder.values.empty()) holders.erase(Key{tenant, std::string(), 0, std::string()});
    return full();
  }
  holder.untilMs = std::max(holder.untilMs, untilMs);
  holder.lastMs = nowMs;
  holder.releasedMs.reset();
  return std::nullopt;
}

bool SecretDictionary::sent(const std::string& tenant, const std::set<std::string>& values,
    const std::string& processId, uint64_t generation, const std::string& revision, bool native, double nowMs,
    double untilMs) {
  if (values.empty()) return true;
  const Key key{tenant, processId, generation, revision};
  auto [at, created] = holders.try_emplace(key);
  auto& holder = at->second;
  if (created) {
    holder.tenant = tenant;
    holder.processId = processId;
    holder.generation = generation;
    holder.revision = revision;
    holder.native = native;
  }
  if (!hold(holder, values, nowMs)) {
    if (created) holders.erase(at);
    return false;
  }
  holder.untilMs = std::max(holder.untilMs, untilMs);
  holder.lastMs = nowMs;
  holder.releasedMs.reset();
  return true;
}

void SecretDictionary::collect(double nowMs, const std::vector<Residents>& residents) {
  std::map<std::pair<std::string, uint64_t>, const Residents*> live;
  for (const auto& entry : residents) live[{entry.processId, entry.generation}] = &entry;
  bool changed = false;
  for (auto at = holders.begin(); at != holders.end();) {
    auto& holder = at->second;
    if (holder.processId.empty()) changed |= pruneReservation(holder);
    bool holding = nowMs < holder.untilMs && !holder.values.empty();
    if (!holding && !holder.processId.empty()) {
      const auto found = live.find({holder.processId, holder.generation});
      holding = found != live.end() && (!found->second->known || found->second->revisions.contains(holder.revision));
    }
    if (holding) {
      holder.releasedMs.reset();
      ++at;
      continue;
    }
    if (!holder.releasedMs) holder.releasedMs = nowMs;
    if (nowMs - *holder.releasedMs < limits.graceMs) {
      ++at;
      continue;
    }
    for (const auto& value : holder.values) unhold(holder.tenant, value);
    at = holders.erase(at);
    changed = true;
  }
  if (changed) ++revision;
  if (publishedBytes > liveBytes && nowMs - lastCompactMs >= limits.compactEveryMs) compact(nowMs);
}

std::string SecretDictionary::json(double nowMs) const {
  struct Row { std::string tenant; size_t bytes = 0, earlier = 0; uint64_t full = 0, refusedEarlier = 0; };
  std::map<std::string, Row> rows;
  for (const auto& [pair, entry] : held) {
    auto& row = rows[pair.first];
    row.tenant = pair.first;
    const auto bytes = entry.bytes;
    row.bytes += bytes;
    const auto now = current.find(pair.first);
    if (now == current.end() || !now->second.contains(pair.second)) row.earlier += bytes;
  }
  for (const auto& [tenant, counts] : refusedByTenant) {
    auto& row = rows[tenant];
    row.tenant = tenant;
    row.full = counts.first;
    row.refusedEarlier = counts.second;
  }
  std::vector<Row> ordered;
  for (auto& [tenant, row] : rows) ordered.push_back(std::move(row));
  std::sort(ordered.begin(), ordered.end(), [](const auto& a, const auto& b) {
    return std::tie(b.bytes, b.full, b.refusedEarlier, a.tenant) < std::tie(a.bytes, a.full, a.refusedEarlier, b.tenant);
  });
  size_t earlier = 0;
  for (const auto& row : ordered) earlier += row.earlier;
  const auto usage = platformLogRedactionUsage();
  std::ostringstream out;
  out << "{\"bytes\":" << liveBytes << ",\"values\":" << livePatterns << ",\"limitBytes\":" << limits.nodeBytes
      << ",\"limitValues\":" << limits.nodePatterns << ",\"tenantEarlierLimitBytes\":" << limits.tenantEarlierBytes
      << ",\"earlierBytes\":" << earlier << ",\"holders\":" << holders.size() << ",\"heldValues\":" << held.size()
      << ",\"publishedBytes\":" << publishedBytes << ",\"compactions\":" << compactions
      << ",\"sharedGeneration\":" << usage.generation << ",\"sharedBytes\":" << usage.bytes
      << ",\"sinkFailures\":" << sinkFailures << ",\"refused\":{\"full\":" << refusedFull << ",\"earlier\":"
      << refusedEarlier << "},\"tenants\":[";
  for (size_t i = 0; i < ordered.size() && i < 16; ++i) {
    const auto& row = ordered[i];
    out << (i ? "," : "") << "{\"tenant\":\"" << jsonEscape(row.tenant) << "\",\"bytes\":" << row.bytes
        << ",\"earlierBytes\":" << row.earlier << ",\"refusedFull\":" << row.full << ",\"refusedEarlier\":"
        << row.refusedEarlier << "}";
  }
  out << "],\"tenantCount\":" << ordered.size() << ",\"observedAtMonotonicMs\":" << static_cast<uint64_t>(nowMs) << "}";
  return out.str();
}

}  // namespace coworkerd
