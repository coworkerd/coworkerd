#include "database-time-travel.h"
#include "json.h"
#include "support.h"
#include <capnp/message.h>
#include <kj/debug.h>
#include <kj/encoding.h>
#include <algorithm>
#include <charconv>
#include <cmath>
#include <iomanip>
#include <limits>
#include <sstream>

namespace coworkerd {
namespace {
using json::quote;
constexpr kj::Exception::DetailTypeId REFUSAL_DETAIL = 0x636f776b2d74746cull;
[[noreturn]] void refuse(uint status, const std::string& code, const std::string& reason) {
  const auto text = std::to_string(status) + "\n" + code + "\n" + reason;
  auto detail = kj::heapArray<kj::byte>(text.size());
  std::copy(text.begin(), text.end(), detail.begin());
  auto error = KJ_EXCEPTION(FAILED, reason.c_str()); error.setDetail(REFUSAL_DETAIL, kj::mv(detail));
  throw kj::mv(error);
}
bool hexId(const std::string& value) {
  return value.size() == 64 && std::all_of(value.begin(), value.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
}
bool operation(const std::string& value) {
  return value.size() >= 16 && value.size() <= 128 && std::all_of(value.begin(), value.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '-'; });
}
uint64_t decimal(const std::string& text) {
  uint64_t value = 0; auto r = std::from_chars(text.data(), text.data() + text.size(), value);
  if (text.empty() || r.ec != std::errc() || r.ptr != text.data() + text.size()) refuse(400, "INVALID_REQUEST", "expected a positive decimal integer");
  return value;
}
uint64_t integer(capnp::JsonValue::Reader root, const char* name) {
  auto value = json::field(root, name);
  if (!value.isNumber()) return json::integer(root, name);
  const auto n = value.getNumber();
  KJ_REQUIRE(std::isfinite(n) && n >= 0 && n < 9007199254740992.0 && std::floor(n) == n, "invalid history integer");
  return uint64_t(n);
}
std::string wide(uint64_t value) { std::ostringstream out; out << std::setw(20) << std::setfill('0') << value; return out.str(); }
std::string objectId(const std::string& ns) {
  std::string bytes;
  for (size_t at = 0; at < ns.size(); at += 2) bytes += char(std::stoul(ns.substr(at, 2), nullptr, 16));
  return objectSha256(bytes + "name" + DATABASE_OBJECT_NAME);
}
std::string manifestKey(const std::string& ns, uint64_t rev) {
  return "do/" + ns + "/objects/" + objectId(ns) + "/manifest/" + wide(rev);
}
std::string lengthField(const std::string& field) { return std::to_string(field.size()) + ":" + field; }
ApiAnswer failure(const kj::Exception& error) {
  KJ_IF_SOME(detail, error.getDetail(REFUSAL_DETAIL)) {
    std::string text(reinterpret_cast<const char*>(detail.begin()), detail.size());
    const auto a = text.find('\n'), b = text.find('\n', a + 1);
    return ApiAnswer::error(uint(decimal(text.substr(0, a))), text.substr(a + 1, b - a - 1), text.substr(b + 1));
  }
  if (isStorageFailure(error)) return ApiAnswer::error(503, "STORAGE_UNAVAILABLE", error.getDescription().cStr());
  if (isWriteConflict(error)) return ApiAnswer::error(409, "CONFLICT", error.getDescription().cStr());
  return ApiAnswer::error(409, "RESTORE_FAILED", error.getDescription().cStr());
}
}
std::string DatabaseTimeTravel::Point::bookmark() const {
  auto text = json::base64(namespaceId + "/" + std::to_string(revision) + "/" + sha256);
  std::replace(text.begin(), text.end(), '+', '-'); std::replace(text.begin(), text.end(), '/', '_');
  while (text.ends_with('=')) text.pop_back();
  return "cwk1_" + text;
}
std::string DatabaseTimeTravel::Point::json() const {
  return "{\"revision\":\"" + std::to_string(revision) + "\",\"writtenAtUnixMs\":\"" + std::to_string(writtenAtUnixMs) +
      "\",\"bookmark\":" + quote(bookmark()) + "}";
}
// Idle requests cannot permanently reserve another tenant's capacity. In-flight
// callers retain shared ownership even if an idle cache entry expires.
void DatabaseTimeTravel::expireJobs() {
  const auto now = monotonicTimeMs();
  for (auto it = jobs.begin(); it != jobs.end();) {
    const auto& job = *it->second;
    if (!job.registering && now - job.touchedAtMs >= (job.result.empty() ? 60000 : 600000)) it = jobs.erase(it);
    else ++it;
  }
}
void DatabaseTimeTravel::remember(const std::string& key, std::shared_ptr<Intent> intent) {
  expireJobs();
  if (auto found = jobs.find(key); found != jobs.end()) {
    const auto& prior = *found->second;
    if (prior.namespaceId != intent->namespaceId || prior.name != intent->name || prior.sourceDatabase != intent->sourceDatabase)
      refuse(409, "CONFLICT", "operationId already has different restore parameters");
    return;
  }
  size_t tenantCount = 0;
  for (const auto& [id, job] : jobs) if (job->tenant == intent->tenant) ++tenantCount;
  auto evictCompleted = [&](bool sameTenant) {
    auto oldest = jobs.end();
    for (auto it = jobs.begin(); it != jobs.end(); ++it)
      if (!it->second->registering && !it->second->result.empty() && (!sameTenant || it->second->tenant == intent->tenant) &&
          (oldest == jobs.end() || it->second->touchedAtMs < oldest->second->touchedAtMs)) oldest = it;
    if (oldest == jobs.end()) return false;
    jobs.erase(oldest); return true;
  };
  if (tenantCount >= 16 && !evictCompleted(true)) refuse(429, "RESTORE_BUSY", "this tenant has 16 pending restore requests; retry after idle requests expire");
  if (jobs.size() >= 1024 && !evictCompleted(false)) refuse(503, "RESTORE_BUSY", "this control plane has 1024 cached restore requests; retry later");
  intent->touchedAtMs = monotonicTimeMs(); jobs[key] = std::move(intent);
}
std::string DatabaseTimeTravel::source(const std::string& tenant, const std::string& database) const {
  const auto* manifest = context.state.tenant(tenant);
  if (!manifest || !manifest->record || manifest->record->status != "active") refuse(409, "CONFLICT", "the source tenant must be active");
  auto found = manifest->databases.find(database);
  if (found == manifest->databases.end()) refuse(404, "NOT_FOUND", "there is no database " + database + " in this tenant");
  if (found->second.status != "active") refuse(409, "CONFLICT", "the source database must be active");
  return found->second.id;
}
kj::Promise<std::optional<DatabaseTimeTravel::Point>> DatabaseTimeTravel::read(std::string ns, uint64_t revision) {
  auto response = co_await context.storage.get(manifestKey(ns, revision));
  if (response.status == 404) co_return std::nullopt;
  requireStorage(response.status == 200 && response.bytes.size() <= (4u << 20), "history manifest is unavailable or too large");
  capnp::MallocMessageBuilder message; auto root = message.initRoot<capnp::JsonValue>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.bytes.data(), response.bytes.size()), root);
  KJ_REQUIRE(json::text(root, "namespaceId") == ns && json::text(root, "objectId") == objectId(ns) &&
      integer(root, "revision") == revision && integer(root, "shard") == 0, "history manifest identity differs");
  Point point{ns, objectId(ns), objectSha256(response.bytes), revision, integer(root, "writtenAtUnixMs"), integer(root, "shardEpoch"), integer(root, "databaseBytes")};
  if (!point.writtenAtUnixMs) refuse(409, "HISTORY_TIME_UNAVAILABLE", "this published revision has no generation time");
  co_return point;
}
kj::Promise<DatabaseTimeTravel::Point> DatabaseTimeTravel::resolve(std::string ns, const std::map<std::string, std::string>& selector) {
  auto bookmark = selector.find("bookmark"), timestamp = selector.find("timestamp");
  if (bookmark != selector.end() && timestamp != selector.end()) refuse(400, "INVALID_REQUEST", "choose bookmark or timestamp");
  if (bookmark != selector.end()) {
    auto text = bookmark->second;
    if (!text.starts_with("cwk1_") || text.size() > 512) refuse(400, "INVALID_BOOKMARK", "invalid database bookmark");
    text = text.substr(5); std::replace(text.begin(), text.end(), '-', '+'); std::replace(text.begin(), text.end(), '_', '/');
    while (text.size() % 4) text += '=';
    auto decoded = kj::decodeBase64(kj::arrayPtr(text.data(), text.size()));
    if (decoded.hadErrors) refuse(400, "INVALID_BOOKMARK", "invalid database bookmark");
    std::string value(reinterpret_cast<const char*>(decoded.begin()), decoded.size());
    const auto first = value.find('/'), second = value.find('/', first == std::string::npos ? 0 : first + 1);
    if (first == std::string::npos || second == std::string::npos || value.substr(0, first) != ns)
      refuse(400, "INVALID_BOOKMARK", "bookmark belongs to another database");
    const auto rev = decimal(value.substr(first + 1, second - first - 1));
    auto point = co_await read(ns, rev);
    if (!point || point->sha256 != value.substr(second + 1)) refuse(400, "INVALID_BOOKMARK", "bookmark differs from published history");
    co_return *point;
  }
  // Revision objects are contiguous and immutable. Find the latest by existence,
  // using exact GETs (exponential bound, then binary search), never a listing.
  auto first = co_await read(ns, 1);
  if (!first) refuse(404, "NO_PUBLISHED_HISTORY", "this database has no published recovery point yet");
  uint64_t lo = 1, hi = 2; Point latest = *first;
  for (;;) {
    auto probe = co_await read(ns, hi);
    if (!probe) break;
    lo = hi; latest = *probe;
    if (hi > UINT64_MAX / 2) refuse(409, "HISTORY_LIMIT", "history revision bound exceeded");
    hi *= 2;
  }
  while (hi - lo > 1) {
    const auto mid = lo + (hi - lo) / 2; auto probe = co_await read(ns, mid);
    if (probe) { lo = mid; latest = *probe; } else hi = mid;
  }
  if (timestamp == selector.end()) co_return latest;
  const auto at = decimal(timestamp->second);
  // Clock offsets can change at an owner takeover. Shard epochs are monotone
  // (the publisher fences lower epochs), so binary-search each owner interval
  // backwards. Revision order, rather than wall-clock order across owners, wins.
  uint64_t end = latest.revision;
  while (end) {
    auto last = co_await read(ns, end); KJ_REQUIRE(last.has_value(), "history has a revision gap");
    if (last->writtenAtUnixMs <= at) co_return *last;
    uint64_t left = 1, right = end;
    while (left < right) {
      const auto mid = left + (right - left) / 2; auto probe = co_await read(ns, mid); KJ_REQUIRE(probe.has_value(), "history has a revision gap");
      if (probe->shardEpoch < last->shardEpoch) left = mid + 1; else right = mid;
    }
    const auto start = left; auto begin = co_await read(ns, start); KJ_REQUIRE(begin.has_value(), "history has a revision gap");
    if (begin->writtenAtUnixMs <= at) {
      uint64_t low = start, high = end; Point chosen = *begin;
      while (high - low > 1) {
        const auto mid = low + (high - low) / 2; auto probe = co_await read(ns, mid); KJ_REQUIRE(probe.has_value(), "history has a revision gap");
        if (probe->writtenAtUnixMs <= at) { low = mid; chosen = *probe; } else high = mid;
      }
      co_return chosen;
    }
    end = start - 1;
  }
  refuse(422, "HISTORY_BEFORE_FIRST", "the requested time precedes the first matching published revision");
}
kj::Promise<uint64_t> DatabaseTimeTravel::usage(std::string tenant) {
  uint64_t total = 0;
  // Only the membership table is listed; durable usage is an exact GET per id.
  for (const auto& member : co_await readStoreMembers(context.storage, context.memberTtlSeconds, context.memberGraceMs)) {
    auto response = co_await context.storage.get("nodes/" + member.id + "/durable-usage");
    if (response.status == 404) continue;
    requireStorage(response.status == 200, "tenant durable usage is unavailable");
    capnp::MallocMessageBuilder message; auto value = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.bytes.data(), response.bytes.size()), value);
    auto tenants = json::field(value, "tenants");
    if (tenants.isObject()) total += integer(tenants, tenant.c_str());
  }
  co_return total;
}
void DatabaseTimeTravel::precheck(const TenantManifest& tenant, const std::string& name, const std::string& ns,
    uint64_t bytes, uint64_t reported) const {
  if (!tenant.record || tenant.record->status != "active") refuse(409, "CONFLICT", "tenant must be active");
  if (tenant.databases.contains(name)) refuse(409, "CONFLICT", "destination database name already exists");
  size_t count = tenant.databases.size() + 1;
  for (const auto& [database, entry] : tenant.databases) if (entry.id == ns) refuse(409, "CONFLICT", "target namespace is already registered");
  for (const auto& [worker, entry] : tenant.workers) {
    count += entry.namespaces.size();
    for (const auto& [className, space] : entry.namespaces) if (space.id == ns) refuse(409, "CONFLICT", "target namespace belongs to a Worker");
  }
  if (count > tenant.record->quotas.durableObjectNamespaces) refuse(422, "QUOTA_EXCEEDED", "durableObjectNamespaces quota exceeded (deleting namespaces also count)");
  const auto quota = tenant.record->quotas.durableObjectBytes;
  if (quota && (reported > quota || bytes > quota - reported)) refuse(422, "QUOTA_EXCEEDED", "durableObjectBytes quota exceeded");
}
std::string DatabaseTimeTravel::destination(const Intent& intent) const {
  KJ_REQUIRE(context.state.secretsKey().has_value(), "the existing control-plane secrets key is required");
  const auto& p = intent.point;
  return context.state.secretsKey()->restoreNamespaceId(lengthField(intent.tenant) + lengthField(p.namespaceId) +
      lengthField(std::to_string(p.revision)) + lengthField(p.sha256) + lengthField(intent.operationId));
}
std::optional<std::string> DatabaseTimeTravel::registered(const Intent& intent) const {
  const auto* manifest = context.state.tenant(intent.tenant); if (!manifest) return std::nullopt;
  auto indexed = context.state.index().tenants.find(intent.tenant);
  if (indexed == context.state.index().tenants.end() || indexed->second.manifestRev < manifest->revision) return std::nullopt;
  auto found = manifest->databases.find(intent.name);
  if (found == manifest->databases.end() || found->second.id != intent.namespaceId) return std::nullopt;
  if (found->second.status != "active") refuse(409, "CONFLICT", "the restored database is being deleted");
  return "{\"known\":true,\"phase\":\"completed\",\"operationId\":" + quote(intent.operationId) +
      ",\"database\":" + quote(intent.name) + ",\"namespaceId\":" + quote(intent.namespaceId) + ",\"point\":" + intent.point.json() + "}";
}
kj::Promise<std::optional<std::string>> DatabaseTimeTravel::recognize(std::shared_ptr<Intent> intent) {
  {
    const auto* current = context.state.tenant(intent->tenant);
    if (!current) co_return std::nullopt;
    auto entry = current->databases.find(intent->name);
    if (entry == current->databases.end() || entry->second.id != intent->namespaceId) co_return std::nullopt;
  }
  // A lost reply may have left a created tenant revision before its index CAS.
  // Complete the normal conditional write and index visibility,
  // before interpreting the in-memory catalog entry as a successful receipt.
  co_await context.state.writeTenant(intent->tenant, [&](TenantManifest& next) -> kj::Promise<void> {
    if (destination(*intent) != intent->namespaceId) refuse(409, "CONFLICT", "restore namespace provenance differs");
    auto found = next.databases.find(intent->name);
    if (found == next.databases.end() || found->second.id != intent->namespaceId || found->second.status != "active")
      refuse(409, "CONFLICT", "the restored database changed during registration retry");
    co_return;
  }, false);
  co_return registered(*intent);
}
kj::Promise<ApiAnswer> DatabaseTimeTravel::info(std::string tenant, std::string database, const std::map<std::string, std::string>& selector) {
  try {
    auto point = co_await resolve(source(tenant, database), selector);
    co_return ApiAnswer::ok("{\"database\":" + quote(database) + ",\"point\":" + point.json() + "}");
  } catch (const kj::Exception& error) { co_return failure(error); }
}
kj::Promise<DatabaseTimeTravel::NodeAnswer> DatabaseTimeTravel::node(std::string address, bool start, std::string input) {
  NodeAnswer answer;
  try {
    auto client = kj::newHttpClient(context.timer, context.headers, context.network, kj::none);
    kj::HttpHeaders headers(context.headers); headers.addPtr("Authorization", kj::str("Bearer ", context.token.c_str()));
    headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json"); headers.setPtr(kj::HttpHeaderId::CONNECTION, "close");
    if (!address.starts_with("http://") && !address.starts_with("https://")) address = "http://" + address;
    auto request = client->request(start ? kj::HttpMethod::POST : kj::HttpMethod::GET,
        (address + "/__coworkerd/database-seed/" + (start ? "start" : "progress/" + input)).c_str(), headers, start ? input.size() : 0);
    auto work = [&]() -> kj::Promise<void> {
      if (start) co_await request.body->write(kj::arrayPtr(input.data(), input.size()).asBytes());
      request.body = nullptr; auto response = co_await request.response;
      auto body = co_await response.body->readAllText(65536); answer.status = response.statusCode; answer.body.assign(body.cStr(), body.size());
    };
    co_await context.timer.timeoutAfter(15000 * kj::MILLISECONDS, work());
  } catch (const kj::Exception&) { answer.status = 0; }
  co_return answer;
}
kj::Promise<ApiAnswer> DatabaseTimeTravel::start(std::string tenant, std::string database, std::string body) {
  try {
    capnp::MallocMessageBuilder message; auto root = message.initRoot<capnp::JsonValue>();
    try { capnp::JsonCodec().decodeRaw(kj::arrayPtr(body.data(), body.size()), root); }
    catch (const kj::Exception&) { refuse(400, "INVALID_REQUEST", "invalid restore JSON"); }
    if (!root.isObject()) refuse(400, "INVALID_REQUEST", "restore JSON must be an object");
    auto text = [&](const char* name) {
      auto field = json::field(root, name);
      if (!field.isString()) refuse(400, "INVALID_REQUEST", std::string(name) + " must be a string");
      return std::string(field.getString().cStr());
    };
    auto intent = std::make_shared<Intent>(); intent->tenant = tenant; intent->sourceDatabase = database;
    intent->name = text("name"); intent->operationId = text("operationId");
    if (!tenantName(intent->name) || !operation(intent->operationId)) refuse(400, "INVALID_REQUEST", "invalid destination name or operationId (16-128 alphanumeric/hyphen characters)");
    std::map<std::string, std::string> selector;
    for (const auto* name : {"bookmark", "timestamp"}) if (!json::field(root, name).isNull()) selector[name] = text(name);
    if (selector.size() != 1) refuse(400, "INVALID_REQUEST", "restore requires exactly one bookmark or timestamp");
    intent->point = co_await resolve(source(tenant, database), selector);
    intent->namespaceId = destination(*intent);
    expireJobs();
    const auto key = tenant + "/" + intent->operationId;
    if (auto old = jobs.find(key); old != jobs.end()) {
      const auto& prior = *old->second;
      if (prior.namespaceId != intent->namespaceId || prior.name != intent->name || prior.sourceDatabase != database)
        refuse(409, "CONFLICT", "operationId already has different restore parameters");
      intent = old->second;
    }
    if (auto done = co_await recognize(intent)) { intent->result = *done; co_return ApiAnswer::ok(*done); }
    const auto reported = co_await usage(tenant);
    // No mirror pointer survives an await. Deletion/recreation may replace the
    // tenant while history and usage are being read.
    if (source(tenant, database) != intent->point.namespaceId) refuse(409, "CONFLICT", "the source database changed while starting restore");
    {
      const auto* current = context.state.tenant(tenant);
      if (!current) refuse(409, "CONFLICT", "the source tenant disappeared while starting restore");
      precheck(*current, intent->name, intent->namespaceId, intent->point.bytes, reported);
    }
    remember(key, intent); intent = jobs.at(key); intent->touchedAtMs = monotonicTimeMs();
    auto removeUnaccepted = kj::defer([this, key, intent]() {
      auto found = jobs.find(key);
      if (!intent->accepted && found != jobs.end() && found->second == intent) jobs.erase(found);
    });
    const auto& p = intent->point;
    const auto input = "{\"source\":{\"namespaceId\":" + quote(p.namespaceId) + ",\"objectId\":" + quote(p.objectId) +
        ",\"revision\":\"" + std::to_string(p.revision) + "\",\"sha256\":" + quote(p.sha256) + "},\"destination\":{\"namespaceId\":" +
        quote(intent->namespaceId) + ",\"objectId\":" + quote(objectId(intent->namespaceId)) + ",\"tenant\":" + quote(tenant) + "}}";
    for (const auto& member : co_await readStoreMembers(context.storage, context.memberTtlSeconds, context.memberGraceMs)) {
      auto answer = co_await node(member.address, true, input);
      if (answer.status == 202) { intent->accepted = true; intent->touchedAtMs = monotonicTimeMs(); co_return ApiAnswer::ok("{\"known\":true,\"operationId\":" + quote(intent->operationId) +
          ",\"namespaceId\":" + quote(intent->namespaceId) + ",\"phase\":\"started\",\"point\":" + p.json() + "}"); }
    }
    refuse(503, "RESTORE_UNAVAILABLE", "no target owner accepted the restore; retry with the same operationId and bookmark");
  } catch (const kj::Exception& error) { co_return failure(error); }
}
kj::Promise<ApiAnswer> DatabaseTimeTravel::finish(std::shared_ptr<Intent> intent, std::string manifestSha256) {
  if (destination(*intent) != intent->namespaceId) refuse(409, "CONFLICT", "restore namespace provenance differs");
  const auto bytes = co_await verifySeedFirstManifest(context.storage, intent->namespaceId, manifestSha256);
  const auto reported = co_await usage(intent->tenant);
  co_await context.state.writeTenant(intent->tenant, [&](TenantManifest& next) -> kj::Promise<void> {
    // Recompute inside this tenant conditional update. The tenant in the MAC
    // prevents cross-tenant collisions; no scan of unrelated tenants is needed.
    if (destination(*intent) != intent->namespaceId) refuse(409, "CONFLICT", "restore namespace provenance differs");
    if (auto found = next.databases.find(intent->name); found != next.databases.end() && found->second.id == intent->namespaceId && found->second.status == "active") co_return;
    auto source = next.databases.find(intent->sourceDatabase);
    if (source == next.databases.end() || source->second.status != "active" || source->second.id != intent->point.namespaceId)
      refuse(409, "CONFLICT", "the source database is no longer active");
    precheck(next, intent->name, intent->namespaceId, bytes, reported);
    next.databases.emplace(intent->name, ManifestNamespace{intent->namespaceId, next.revision + 1, "active", 1});
    co_return;
  }, false);
  auto done = registered(*intent); KJ_REQUIRE(done.has_value(), "restore registration is not visible"); intent->result = *done;
  co_return ApiAnswer::ok(*done);
}
kj::Promise<ApiAnswer> DatabaseTimeTravel::progress(std::string tenant, std::string operationId) {
  try {
    if (!operation(operationId)) refuse(400, "INVALID_REQUEST", "invalid operationId");
    expireJobs();
    auto found = jobs.find(tenant + "/" + operationId);
    if (found == jobs.end()) co_return ApiAnswer::ok("{\"known\":false}");
    auto intent = found->second; intent->touchedAtMs = monotonicTimeMs();
    if (auto done = co_await recognize(intent)) co_return ApiAnswer::ok(*done);
    if (intent->registering) co_return ApiAnswer::ok("{\"known\":true,\"phase\":\"registering\"}");
    std::optional<std::pair<std::string, std::string>> pending, failed;
    for (const auto& member : co_await readStoreMembers(context.storage, context.memberTtlSeconds, context.memberGraceMs)) {
      auto answer = co_await node(member.address, false, intent->namespaceId);
      if (answer.status != 200) continue;
      capnp::MallocMessageBuilder message; auto root = message.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(answer.body.data(), answer.body.size()), root);
      if (!json::boolean(root, "known")) continue;
      const auto phase = json::text(root, "phase");
      if (phase == "completed") {
        intent->registering = true; auto release = kj::defer([&]() { intent->registering = false; });
        co_return co_await finish(intent, json::text(json::field(root, "result"), "manifestSha256"));
      }
      // An old owner may still cache a failed attempt after the new owner has
      // finished. Read all known jobs before returning a failure. A completed
      // immutable R1 receipt wins; otherwise an active attempt wins over failure.
      if (phase == "failed") failed = std::make_pair(phase, answer.body);
      else pending = std::make_pair(phase, answer.body);
    }
    const auto& observed = pending ? pending : failed;
    if (observed) {
      if (!pending) intent->result = observed->second;
      co_return ApiAnswer::ok("{\"known\":true,\"operationId\":" + quote(operationId) + ",\"phase\":" + quote(observed->first) +
          ",\"point\":" + intent->point.json() + ",\"node\":" + observed->second + "}");
    }
    co_return ApiAnswer::ok("{\"known\":false}");
  } catch (const kj::Exception& error) { co_return failure(error); }
}
} // namespace coworkerd
