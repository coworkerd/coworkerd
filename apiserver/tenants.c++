// Tenants, credentials and usage (docs/control-plane-api.md sections 1-5): the tenants' records in their manifests and
// the credentials index on the object store (manifests.h), which the control plane alone writes and reads from its
// mirror, and the usage rows each node writes below its own /coworkerd/v2/nodes/<nodeId>/usage/ in etcd, which the
// control plane only reads and sums.
#include "control.h"
#include "support.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <functional>
#include <map>
#include <random>
#include <set>
#include <sstream>
#include <strings.h>

namespace coworkerd {
namespace {
using namespace etcd;

uint64_t nowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
std::string randomHex(size_t bytes) {
  std::ifstream source("/dev/urandom", std::ios::binary);
  std::string raw(bytes, '\0');
  source.read(raw.data(), static_cast<std::streamsize>(bytes));
  KJ_REQUIRE(source.good(), "cannot read /dev/urandom");
  static const char* digits = "0123456789abcdef";
  std::string out;
  for (unsigned char c : raw) { out += digits[c >> 4]; out += digits[c & 15]; }
  return out;
}
bool constantEquals(const std::string& a, const std::string& b) {
  unsigned char difference = a.size() == b.size() ? 0 : 1;
  for (size_t i = 0; i < a.size(); ++i) difference |= static_cast<unsigned char>(a[i] ^ (i < b.size() ? b[i] : 0));
  return difference == 0;
}
std::string prefixEnd(std::string prefix) { ++prefix.back(); return prefix; }

ApiAnswer invalid(std::string reason) { return ApiAnswer::error(400, "INVALID_REQUEST", std::move(reason)); }
ApiAnswer notFound(const std::string& tenant) {
  return ApiAnswer::error(404, "TENANT_NOT_FOUND", "there is no tenant " + tenant);
}
ApiAnswer quota(const std::string& reason) { return ApiAnswer::error(422, "QUOTA_EXCEEDED", reason); }

// A request body: a JSON object (or nothing, when `empty` allows it).
struct Body {
  capnp::MallocMessageBuilder message;
  capnp::JsonValue::Reader json;
  std::string problem;
};
std::unique_ptr<Body> parseBody(const std::string& text, bool empty = false) {
  auto body = std::make_unique<Body>();
  auto root = body->message.initRoot<Json>();
  try {
    if (empty && text.find_first_not_of(" \t\r\n") == std::string::npos) capnp::JsonCodec().decodeRaw("{}", root);
    else capnp::JsonCodec().decodeRaw(kj::arrayPtr(text.data(), text.size()), root);
    if (!root.isObject()) body->problem = "the body is a JSON object";
  } catch (const kj::Exception& error) {
    body->problem = std::string("the body is not JSON: ") + error.getDescription().cStr();
  }
  body->json = root.asReader();
  return body;
}


// A refusal of a change: the ManifestStore's change functions keep its answer in `slot` and end with an exception (a
// KJ coroutine carries only KJ's), which the caller turns back into that answer.
[[noreturn]] void refuseWith(std::optional<ApiAnswer>& slot, ApiAnswer answer) {
  slot = std::move(answer);
  kj::throwFatalException(KJ_EXCEPTION(FAILED, "refused"));
}

// A failure of the store (or any other) during a write: 503 STORAGE_UNAVAILABLE when it is the store's.
ApiAnswer writeFailure(const kj::Exception& error, const char* what) {
  std::string reason = error.getDescription().cStr();
  if (reason.find("object store") != std::string::npos || error.getType() == kj::Exception::Type::DISCONNECTED ||
      error.getType() == kj::Exception::Type::OVERLOADED)
    return ApiAnswer::error(503, "STORAGE_UNAVAILABLE", std::string("the object store cannot be reached; ") + what + " was not written: " + reason);
  if (reason.find("期间有其他部署") != std::string::npos || reason.find("another write") != std::string::npos)
    return ApiAnswer::error(409, "CONFLICT", reason);
  return ApiAnswer::error(500, "INTERNAL", reason);
}

// Every tenant's record the mirror holds, by name.
std::map<std::string, TenantRecord> recordsOf(const ManifestStore& state) {
  std::map<std::string, TenantRecord> result;
  for (const auto& [name, manifest] : state.tenants()) if (manifest.record) result.emplace(name, *manifest.record);
  return result;
}

// The hostnames, quotas and default limits a body names (create and update); a problem as an answer.
std::optional<ApiAnswer> applyTenantFields(TenantRecord& tenant, capnp::JsonValue::Reader json, bool creating,
    bool& hostnamesChanged) {
  hostnamesChanged = false;
  try {
    for (auto entry : json.getObject()) {
      const std::string name = entry.getName().cStr();
      auto value = entry.getValue();
      if (name == "name") {
        if (!creating) return invalid("a tenant's name cannot change");
        continue;
      }
      if (name == "hostnames") {
        if (!value.isArray()) return invalid("hostnames is a list of hostnames");
        std::vector<std::string> hosts;
        std::set<std::string> seen;
        for (auto host : value.getArray()) {
          if (!host.isString() || !hostnameValid(host.getString().cStr()))
            return invalid("a hostname is lowercase DNS labels of a-z, 0-9 and '-' with at least one dot (\"*\" is the default host of path routes)");
          if (!seen.insert(host.getString().cStr()).second) return invalid("a hostname appears twice");
          hosts.emplace_back(host.getString().cStr());
        }
        std::sort(hosts.begin(), hosts.end());
        hostnamesChanged = hosts != tenant.hostnames;
        tenant.hostnames = std::move(hosts);
      } else if (name == "quotas") {
        applyTenantQuotas(tenant.quotas, value);
      } else if (name == "defaultLimits") {
        tenant.defaultLimits = readWorkerLimits(value);
      } else {
        return invalid("unknown field " + name + " (name, hostnames, quotas, defaultLimits)");
      }
    }
  } catch (const kj::Exception& error) {
    return invalid(error.getDescription().cStr());
  }
  if (tenant.hostnames.size() > tenant.quotas.hostnames)
    return quota("hostnames: " + std::to_string(tenant.hostnames.size()) + " hostnames, the quota is " +
        std::to_string(tenant.quotas.hostnames));
  if (tenant.defaultLimits.cpuMs > tenant.quotas.maxWorkerCpuMs || tenant.defaultLimits.memoryMiB > tenant.quotas.maxWorkerMemoryMiB)
    return quota("defaultLimits: above maxWorkerCpuMs or maxWorkerMemoryMiB");
  return std::nullopt;
}

std::string tenantAnswer(const TenantRecord& tenant, const std::string& extra = {}) {
  auto record = tenantRecordJson(tenant);
  if (tenant.deletion == "null") { record.pop_back(); record += ",\"deletion\":null}"; }
  return "{\"tenant\":" + record + extra + "}";
}

// A raw range [start, end) with its pages, keys only or with values.
kj::Promise<EtcdSnapshot> rangeBetween(EtcdClient& client, std::string start, std::string end, bool keysOnly = false) {
  EtcdSnapshot snapshot;
  std::string from = start;
  while (true) {
    auto response = co_await client.request("/v3/kv/range", "{\"key\":" + quote(base64(from)) + ",\"range_end\":" +
        quote(base64(end)) + ",\"limit\":\"2000\"" + (keysOnly ? ",\"keys_only\":true" : "") +
        (snapshot.revision ? ",\"revision\":\"" + std::to_string(snapshot.revision) + "\"" : std::string()) + "}");
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
    if (!snapshot.revision) snapshot.revision = integer(field(json, "header"), "revision");
    auto kvs = field(json, "kvs");
    std::string last;
    if (kvs.isArray()) for (auto kv : kvs.getArray()) {
      auto value = EtcdClient::decodeKv(kv);
      last = value.key;
      snapshot.values.emplace(value.key, std::move(value));
    }
    if (!boolean(json, "more")) break;
    KJ_REQUIRE(!last.empty(), "etcd range reported more keys without returning any");
    from = last + '\0';
  }
  co_return snapshot;
}
}  // namespace

Authenticated authenticate(const ManifestStore& state, const std::string& adminCredential, const kj::HttpHeaders& headers) {
  std::optional<std::string> authorization;
  bool repeated = false;
  headers.forEach([&](kj::StringPtr name, kj::StringPtr value) {
    if (name.size() != 13 || strncasecmp(name.cStr(), "authorization", 13) != 0) return;
    if (authorization) repeated = true; else authorization = std::string(value.cStr(), value.size());
  });
  auto refuse = [](std::string kind) {
    return Authenticated{std::nullopt, ApiAnswer::error(401, "UNAUTHORIZED",
        "this endpoint requires Authorization: Bearer <credential> (the administrator's or a tenant's)"), std::move(kind)};
  };
  if (!authorization) return refuse("missing");
  if (repeated || !authorization->starts_with("Bearer ")) return refuse("malformed");
  const auto presented = authorization->substr(7);
  if (!adminCredential.empty() && bearerRefusal(adminCredential, headers).empty()) return Authenticated{Caller{true, {}, {}}, {}, "admin"};
  auto token = parseCredential(presented);
  if (!token) return refuse("wrong");
  const auto& credentials = state.credentials().credentials;
  auto found = credentials.find(token->keyId);
  if (found == credentials.end()) return refuse("wrong");
  const auto& record = found->second;
  if (record.keyId != token->keyId || !constantEquals(sha256Hex(token->secret), record.sha256)) return refuse("wrong");
  if (record.revokedAtUnixMs) return refuse("revoked");
  if (record.expiresAtUnixMs && nowMs() >= record.expiresAtUnixMs) return refuse("expired");
  return Authenticated{Caller{false, record.scope.substr(7), record.keyId}, {}, "tenant"};
}

ReadTenant readTenant(const ManifestStore& state, const std::string& tenant) {
  ReadTenant result;
  result.revision = state.index().revision;
  const auto* manifest = state.tenant(tenant);
  if (!manifest || !manifest->record) return result;
  result.tenant = *manifest->record;
  result.manifestRevision = manifest->revision;
  return result;
}

kj::Promise<ApiAnswer> createTenant(ManifestStore& state, std::string text) {
  std::optional<ApiAnswer> refused;
  auto body = parseBody(text);
  if (!body->problem.empty()) co_return invalid(body->problem);
  auto name = field(body->json, "name");
  if (!name.isString() || !tenantName(name.getString().cStr()))
    co_return invalid("name: a tenant is [a-z0-9][a-z0-9-]{0,62} (\"routes\" and \"generation\" are reserved)");
  TenantRecord tenant;
  tenant.name = name.getString().cStr();
  bool hostnamesChanged = false;
  if (auto problem = applyTenantFields(tenant, body->json, true, hostnamesChanged)) co_return *problem;
  try {
    auto written = co_await state.writeTenant(tenant.name, [&](TenantManifest& next) -> kj::Promise<void> {
      // A tenant whose record is active exists; a manifest left by a deletion (deleting) or by a creation that was
      // interrupted before the index named it is replaced: the tenant starts empty.
      if (next.record && next.record->status == "active" && state.index().tenants.contains(tenant.name))
        refuseWith(refused, ApiAnswer::error(409, "CONFLICT", "tenant " + tenant.name + " already exists"));
      if (next.record && next.record->status == "deleting" && state.index().tenants.contains(tenant.name))
        refuseWith(refused, ApiAnswer::error(409, "CONFLICT", "tenant " + tenant.name + " is being deleted"));
      // Hostnames are unique across tenants (the other tenants as the mirror holds them).
      for (const auto& [other, record] : recordsOf(state)) {
        if (other == tenant.name) continue;
        for (const auto& host : record.hostnames)
          if (std::find(tenant.hostnames.begin(), tenant.hostnames.end(), host) != tenant.hostnames.end())
            refuseWith(refused, ApiAnswer::error(409, "CONFLICT", "hostname " + host + " belongs to tenant " + other));
      }
      tenant.createdAtUnixMs = tenant.updatedAtUnixMs = nowMs();
      next.workers.clear();
      next.record = tenant;
      co_return;
    }, false);
    co_return ApiAnswer::ok(tenantAnswer(*written.manifest.record));
  } catch (const kj::Exception& error) {
    if (refused) co_return *refused;
    co_return writeFailure(error, "the tenant");
  }
}

ApiAnswer listTenants(const ManifestStore& state) {
  std::string out = "{\"revision\":\"" + std::to_string(state.index().revision) + "\",\"tenants\":[";
  size_t index = 0;
  for (const auto& [name, record] : recordsOf(state)) out += (index++ ? "," : "") + tenantRecordJson(record);
  return ApiAnswer::ok(out + "]}");
}

kj::Promise<ApiAnswer> updateTenant(ManifestStore& state, std::string tenantName, std::string text) {
  std::optional<ApiAnswer> refused;
  auto body = parseBody(text);
  if (!body->problem.empty()) co_return invalid(body->problem);
  TenantRecord result;
  try {
    co_await state.writeTenant(tenantName, [&](TenantManifest& next) -> kj::Promise<void> {
      if (!next.record) refuseWith(refused, notFound(tenantName));
      auto tenant = *next.record;
      if (tenant.status != "active") refuseWith(refused, ApiAnswer::error(409, "CONFLICT", "tenant " + tenantName + " is being deleted"));
      const auto previous = tenant.hostnames;
      bool hostnamesChanged = false;
      if (auto problem = applyTenantFields(tenant, body->json, false, hostnamesChanged)) refuseWith(refused, *problem);
      if (hostnamesChanged) {
        for (const auto& [other, record] : recordsOf(state)) {
          if (other == tenantName) continue;
          for (const auto& host : record.hostnames)
            if (std::find(tenant.hostnames.begin(), tenant.hostnames.end(), host) != tenant.hostnames.end())
              refuseWith(refused, ApiAnswer::error(409, "CONFLICT", "hostname " + host + " belongs to tenant " + other));
        }
        // A hostname still routed by one of the tenant's Workers stays until its route goes.
        for (const auto& [worker, entry] : next.workers) {
          for (const auto& route : entry.routes) {
            if (std::find(previous.begin(), previous.end(), route.host) != previous.end() &&
                std::find(tenant.hostnames.begin(), tenant.hostnames.end(), route.host) == tenant.hostnames.end())
              refuseWith(refused, ApiAnswer::error(409, "CONFLICT", "hostname " + route.host + " is still routed by a Worker of tenant " +
                  tenantName + "; replace that Worker's routes first"));
          }
        }
      }
      tenant.updatedAtUnixMs = nowMs();
      next.record = tenant;
      result = tenant;
      co_return;
    }, false);
  } catch (const kj::Exception& error) {
    if (refused) co_return *refused;
    co_return writeFailure(error, "the tenant");
  }
  co_return ApiAnswer::ok(tenantAnswer(result));
}

kj::Promise<ApiAnswer> deleteTenant(ControlContext& context, std::string name) {
  std::optional<ApiAnswer> refused;
  auto& state = context.state;
  // Each step's record joins the tenant's `deletion` (visible in GET /v1/tenants/{t} while it runs).
  std::vector<std::string> steps;
  // The tenant's routes and pointers stay while it is deleted: the nodes refuse its Workers (TENANT_UNAVAILABLE, the record
  // says deleting) and no other tenant takes its routes before it has left the index, where they go with it.
  auto record = [&](const std::string& step, const std::string& result) -> kj::Promise<void> {
    steps.push_back("{\"step\":" + quote(step) + ",\"atUnixMs\":" + std::to_string(nowMs()) + ",\"result\":" + result + "}");
    co_await state.writeTenant(name, [&](TenantManifest& next) -> kj::Promise<void> {
      if (!next.record) refuseWith(refused, notFound(name));
      auto tenant = *next.record;
      // Keep the first start of a deletion that runs again.
      uint64_t started = nowMs();
      if (tenant.deletion != "null") {
        capnp::MallocMessageBuilder message;
        auto json = message.initRoot<Json>();
        capnp::JsonCodec().decodeRaw(kj::arrayPtr(tenant.deletion.data(), tenant.deletion.size()), json);
        auto value = field(json, "startedAtUnixMs");
        if (value.isNumber()) started = static_cast<uint64_t>(value.getNumber());
      }
      std::string deletion = "{\"startedAtUnixMs\":" + std::to_string(started) + ",\"steps\":[";
      for (size_t i = 0; i < steps.size(); ++i) deletion += (i ? "," : "") + steps[i];
      tenant.status = "deleting";
      tenant.deletion = deletion + "]}";
      tenant.updatedAtUnixMs = nowMs();
      next.record = tenant;
      co_return;
    }, false);
  };
  if (!readTenant(state, name).tenant) co_return notFound(name);
  auto incomplete = [&](const std::string& where, const std::string& reason) {
    return ApiAnswer::error(503, "DELETION_INCOMPLETE", "the deletion of tenant " + name + " stopped at " + where + ": " + reason +
        "; DELETE it again to resume");
  };
  // 1. Marked deleting: the nodes stop serving it, the API refuses its writes.
  try {
    co_await record("marked", "{\"status\":\"deleting\"}");
  } catch (const kj::Exception& error) {
    if (refused) co_return *refused;
    co_return writeFailure(error, "the deletion's mark");
  }
  // 2. The objects only it uses.
  try {
    auto objects = co_await controlDeleteTenantObjects(context, name);
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(objects.data(), objects.size()), json);
    if (!boolean(json, "ok")) {
      const auto reason = etcd::text(json, "reason");
      try { co_await record("objects", objects); } catch (const kj::Exception&) {}
      co_return incomplete("its objects", reason);
    }
    co_await record("objects", capnp::JsonCodec().encodeRaw(field(json, "record")).cStr());
    // 3. Its credentials.
    size_t deleted = 0;
    co_await state.writeCredentials([&](CredentialIndex& next) {
      deleted = std::erase_if(next.credentials, [&](const auto& entry) { return entry.second.scope == "tenant:" + name; });
    });
    co_await record("credentials", "{\"deleted\":" + std::to_string(deleted) + "}");
  } catch (const kj::Exception& error) {
    if (refused) co_return *refused;
    co_return incomplete("its objects or credentials", error.getDescription().cStr());
  }
  // 4. Out of the index; then its manifests and their hint.
  ManifestStore::Forgotten forgotten;
  try {
    forgotten = co_await state.forgetTenant(name);
  } catch (const kj::Exception& error) {
    co_return incomplete("its removal from the index", error.getDescription().cStr());
  }
  std::string out = "{\"tenant\":" + quote(name) + ",\"deleted\":true,\"steps\":[";
  for (size_t i = 0; i < steps.size(); ++i) out += (i ? "," : "") + steps[i];
  co_return ApiAnswer::ok(out + "],\"index\":{\"revision\":\"" + std::to_string(forgotten.indexRevision) + "\"},\"manifests\":{\"removed\":" +
      std::to_string(forgotten.removed) + ",\"failed\":" + std::to_string(forgotten.failed) + "}}");
}

kj::Promise<ApiAnswer> createCredential(ManifestStore& state, std::string tenantName, std::string text) {
  std::optional<ApiAnswer> refused;
  auto body = parseBody(text, true);
  if (!body->problem.empty()) co_return invalid(body->problem);
  CredentialRecord record;
  record.scope = "tenant:" + tenantName;
  try {
    for (auto entry : body->json.getObject()) {
      const std::string name = entry.getName().cStr();
      auto value = entry.getValue();
      if (name == "description") {
        if (!value.isString() || value.getString().size() > 200) co_return invalid("description is a string of at most 200 bytes");
        record.description = value.getString().cStr();
      } else if (name == "expiresAtUnixMs") {
        if (!value.isNumber() || value.getNumber() <= double(nowMs())) co_return invalid("expiresAtUnixMs is a time in the future");
        record.expiresAtUnixMs = static_cast<uint64_t>(value.getNumber());
      } else {
        co_return invalid("unknown field " + name + " (description, expiresAtUnixMs)");
      }
    }
  } catch (const kj::Exception& error) {
    co_return invalid(error.getDescription().cStr());
  }
  auto read = readTenant(state, tenantName);
  if (!read.tenant) co_return notFound(tenantName);
  if (read.tenant->status != "active") co_return ApiAnswer::error(409, "CONFLICT", "tenant " + tenantName + " is being deleted");
  CredentialToken token{randomHex(8), randomHex(32)};
  record.keyId = token.keyId;
  record.sha256 = sha256Hex(token.secret);
  record.createdAtUnixMs = nowMs();
  try {
    co_await state.writeCredentials([&](CredentialIndex& next) {
      // A key id drawn twice (64 random bits) is drawn again by the next request.
      if (next.credentials.contains(record.keyId)) refuseWith(refused, ApiAnswer::error(409, "CONFLICT", "a key id was drawn twice; try again"));
      next.credentials.emplace(record.keyId, record);
    });
  } catch (const kj::Exception& error) {
    if (refused) co_return *refused;
    co_return writeFailure(error, "the credential");
  }
  // The credential's text is in this answer only; the store keeps its hash.
  co_return ApiAnswer::ok("{\"keyId\":" + quote(record.keyId) + ",\"credential\":" + quote(credentialText(token)) +
      ",\"scope\":" + quote(record.scope) + ",\"description\":" + quote(record.description) + ",\"createdAtUnixMs\":" +
      std::to_string(record.createdAtUnixMs) + ",\"expiresAtUnixMs\":" + std::to_string(record.expiresAtUnixMs) + "}");
}

ApiAnswer listCredentials(const ManifestStore& state, std::string tenantName) {
  if (!readTenant(state, tenantName).tenant) return notFound(tenantName);
  std::string out = "{\"tenant\":" + quote(tenantName) + ",\"credentials\":[";
  size_t index = 0;
  for (const auto& [keyId, record] : state.credentials().credentials) {
    if (record.scope != "tenant:" + tenantName) continue;
    // Never the hash.
    out += std::string(index++ ? "," : "") + "{\"keyId\":" + quote(record.keyId) + ",\"scope\":" + quote(record.scope) +
        ",\"description\":" + quote(record.description) + ",\"createdAtUnixMs\":" + std::to_string(record.createdAtUnixMs) +
        ",\"expiresAtUnixMs\":" + std::to_string(record.expiresAtUnixMs) + ",\"revokedAtUnixMs\":" +
        std::to_string(record.revokedAtUnixMs) + "}";
  }
  return ApiAnswer::ok(out + "]}");
}

kj::Promise<ApiAnswer> revokeCredential(ManifestStore& state, std::string tenantName, std::string keyId) {
  std::optional<ApiAnswer> refused;
  if (keyId.size() != 16 || !std::all_of(keyId.begin(), keyId.end(), [](unsigned char c) { return std::isxdigit(c) && !std::isupper(c); }))
    co_return invalid("a key id is 16 lowercase hex characters");
  uint64_t revokedAt = 0;
  bool unchanged = false;
  try {
    co_await state.writeCredentials([&](CredentialIndex& next) {
      auto found = next.credentials.find(keyId);
      if (found == next.credentials.end() || found->second.scope != "tenant:" + tenantName)
        refuseWith(refused, ApiAnswer::error(404, "CREDENTIAL_NOT_FOUND", "tenant " + tenantName + " has no credential " + keyId));
      if (found->second.revokedAtUnixMs) { unchanged = true; revokedAt = found->second.revokedAtUnixMs; return; }
      found->second.revokedAtUnixMs = revokedAt = nowMs();
    });
  } catch (const kj::Exception& error) {
    if (refused) co_return *refused;
    co_return writeFailure(error, "the revocation");
  }
  co_return ApiAnswer::ok("{\"keyId\":" + quote(keyId) + ",\"revoked\":true,\"revokedAtUnixMs\":" + std::to_string(revokedAt) +
      (unchanged ? ",\"unchanged\":true}" : "}"));
}


kj::Promise<std::vector<std::string>> nodeIds(EtcdClient& client) {
  // A skip scan: one key per step (keys only), then past that node's keys -- two reads per node however many rows it
  // wrote. A node's keys are "nodes/<id>" and "nodes/<id>/...": ids that extend <id> with '-', '_' or '.' sort between
  // the two and are met on the way; '0' is the byte after '/'.
  const std::string prefix = NODES_PREFIX, end = prefixEnd(prefix);
  std::vector<std::string> ids;
  std::string cursor = prefix;
  for (size_t steps = 0; steps < 100000; ++steps) {
    auto response = co_await client.request("/v3/kv/range", "{\"key\":" + quote(base64(cursor)) + ",\"range_end\":" +
        quote(base64(end)) + ",\"limit\":\"1\",\"keys_only\":true}");
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), json);
    auto kvs = field(json, "kvs");
    if (!kvs.isArray() || kvs.getArray().size() == 0) break;
    const auto key = unbase64(text(kvs.getArray()[0], "key"));
    if (!key.starts_with(prefix)) break;
    const auto rest = key.substr(prefix.size());
    const auto slash = rest.find('/');
    const auto id = rest.substr(0, slash);
    if (!id.empty() && (ids.empty() || ids.back() != id)) ids.push_back(id);
    cursor = slash == std::string::npos ? key + '\0' : prefix + id + "0";
  }
  std::sort(ids.begin(), ids.end());
  ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
  co_return ids;
}

kj::Promise<ApiAnswer> usage(EtcdClient& client, std::optional<std::string> tenant, uint64_t from, uint64_t to) {
  if (to <= from) co_return invalid("to must be after from");
  if (to - from > 366ull * 24 * 3600 * 1000) co_return invalid("at most a year at a time");
  const auto firstHour = usageHour(from), lastHour = usageHour(to - 1);
  auto ids = co_await nodeIds(client);
  struct Sum { uint64_t requests = 0, logLines = 0, egressBytes = 0, rows = 0; double cpuMs = 0, isolateMs = 0; std::set<std::string> nodes; };
  std::map<std::pair<std::string, std::string>, Sum> sums;  // (hour, tenant)
  Sum total;
  for (const auto& id : ids) {
    const auto prefix = nodeUsagePrefix(id);
    auto raw = co_await rangeBetween(client, prefix + firstHour, prefix + lastHour + "0");
    for (const auto& [key, kv] : raw.values) {
      UsageRow row;
      try { row = decodeUsageRow(kv.value); } catch (const kj::Exception&) { continue; }
      if (key != nodeUsageKey(id, row.hour, row.tenant) || row.nodeId != id) continue;
      if (tenant && row.tenant != *tenant) continue;
      for (auto* sum : {&sums[{row.hour, row.tenant}], &total}) {
        sum->requests += row.requests; sum->logLines += row.logLines; sum->egressBytes += row.egressBytes;
        sum->cpuMs += row.cpuMs; sum->isolateMs += row.isolateMs; ++sum->rows; sum->nodes.insert(id);
      }
    }
  }
  auto fields = [](const Sum& sum) {
    auto row = usageRowJson({"x", "1970-01-01T00", "x", sum.requests, sum.logLines, sum.egressBytes, sum.cpuMs, sum.isolateMs, 0});
    // The counting fields of a row: requests .. isolateMs.
    const auto begin = row.find(",\"requests\":"), end = row.find(",\"updatedAtUnixMs\":");
    return row.substr(begin + 1, end - begin - 1) + ",\"nodes\":" + std::to_string(sum.nodes.size()) + ",\"nodeRows\":" + std::to_string(sum.rows);
  };
  std::string out = "{" + (tenant ? "\"tenant\":" + quote(*tenant) + "," : std::string()) + "\"from\":" + std::to_string(from) +
      ",\"to\":" + std::to_string(to) + ",\"by\":\"hour\",\"nodes\":[";
  for (size_t i = 0; i < ids.size(); ++i) out += (i ? "," : "") + quote(ids[i]);
  out += "],\"rows\":[";
  size_t index = 0;
  for (const auto& [key, sum] : sums) {
    out += std::string(index++ ? "," : "") + "{\"hour\":" + quote(key.first) + ",\"startUnixMs\":" +
        std::to_string(usageHourStartMs(key.first)) + (tenant ? std::string() : ",\"tenant\":" + quote(key.second)) + "," + fields(sum) + "}";
  }
  co_return ApiAnswer::ok(out + "],\"total\":{" + fields(total) + "}}");
}

}  // namespace coworkerd
