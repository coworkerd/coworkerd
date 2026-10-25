// Tenants, credentials and usage (docs/control-plane-api.md sections 1-5): the tenants' records in their manifests and
// the credentials index on the object store (manifests.h), which the control plane alone writes and reads from its
// mirror, and the usage objects each node writes below its own nodes/<nodeId>/usage/, which the control plane reads and
// sums, and once a day removes past their retention (a node removes its own; these are the ones of the nodes that left).
#include "control.h"
#include "object-state.h"
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
using namespace json;

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

// A failure during a write, by its kind (object-store.h): the store's is 503 STORAGE_UNAVAILABLE, other writers that
// kept winning 409 CONFLICT, anything else 500 INTERNAL.
ApiAnswer writeFailure(const kj::Exception& error, const char* what) {
  std::string reason = error.getDescription().cStr();
  if (isStorageFailure(error))
    return ApiAnswer::error(503, "STORAGE_UNAVAILABLE", std::string("the object store failed; ") + what + " was not written: " + reason);
  if (isWriteConflict(error)) return ApiAnswer::error(409, "CONFLICT", reason);
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
  // A credential acts only on a tenant the index names (a deletion removes its credentials; one that outlived it never
  // acts on a tenant created again under the name).
  if (!state.index().tenants.contains(record.scope.substr(7))) return refuse("wrong");
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
  // A deletion that stopped after the tenant left the index is resumed from what the store holds.
  if (!readTenant(state, name).tenant) {
    try { co_await state.reloadTenant(name); } catch (const kj::Exception& error) { co_return writeFailure(error, "the deletion"); }
    const auto* left = state.tenant(name);
    if (!left || !left->record || left->record->status != "deleting") co_return notFound(name);
  }
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
      const auto reason = json::text(json, "reason");
      try { co_await record("objects", objects); } catch (const kj::Exception&) {}
      co_return incomplete("its objects", reason);
    }
    co_await record("objects", capnp::JsonCodec().encodeRaw(field(json, "record")).cStr());
    // 3. Its credentials.
    // A revision even when the tenant has no credential: a creation another control plane computed from an index read
    // before this one meets it and judges the tenant again (createCredential).
    size_t deleted = 0;
    co_await state.writeCredentials([&](CredentialIndex& next) -> kj::Promise<void> {
      deleted = std::erase_if(next.credentials, [&](const auto& entry) { return entry.second.scope == "tenant:" + name; });
      co_return;
    }, 8, true);
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
  if (forgotten.failed)
    co_return incomplete("its manifests", std::to_string(forgotten.failed) + " of " + std::to_string(forgotten.removed + forgotten.failed) +
        " objects below tenants/" + name + "/ could not be removed");
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
    co_await state.writeCredentials([&](CredentialIndex& next) -> kj::Promise<void> {
      // The tenant as the store has it now (its latest manifest), within the credentials index's turn and again at every
      // attempt: a deletion marks the tenant before it removes its credentials in this same aggregate, so a creation
      // either comes before that removal or sees the mark -- or no tenant, or one created again since (another
      // createdAtUnixMs): either way not the tenant this credential was asked for.
      auto latest = co_await readLatestRevision(state.objects(), tenantManifestPrefix(tenantName), tenantHintKey(tenantName), 0, true);
      std::optional<TenantRecord> current;
      if (latest.revision) current = decodeTenantManifest(latest.bytes).record;
      if (!current || current->createdAtUnixMs != read.tenant->createdAtUnixMs)
        refuseWith(refused, ApiAnswer::error(409, "CONFLICT", "tenant " + tenantName + " was deleted while its credential was created"));
      if (current->status != "active")
        refuseWith(refused, ApiAnswer::error(409, "CONFLICT", "tenant " + tenantName + " is being deleted"));
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

// ---- The tenant's databases and its Workers' Durable Object namespaces ----
namespace {
std::string namespaceRecordJson(const std::string& name, const ManifestNamespace& entry) {
  return "{\"name\":" + quote(name) + ",\"namespaceId\":" + quote(entry.id) + ",\"status\":" + quote(entry.status) +
      ",\"shards\":" + std::to_string(entry.shards) + ",\"createdRevision\":" + std::to_string(entry.createdRevision) + "}";
}
}  // namespace

// POST /v1/tenants/{t}/databases/{d}: a namespace of one shard whose class is the platform's SQL Worker. Creating one
// that is already there answers it (the same request twice is one database), one that is being deleted is refused.
kj::Promise<ApiAnswer> createDatabase(ManifestStore& state, std::string tenantName, std::string name) {
  std::optional<ApiAnswer> refused;
  ManifestNamespace made;
  uint64_t revision = 0;
  try {
    auto written = co_await state.writeTenant(tenantName, [&](TenantManifest& next) -> kj::Promise<void> {
      if (!next.record) refuseWith(refused, notFound(tenantName));
      if (next.record->status != "active")
        refuseWith(refused, ApiAnswer::error(409, "CONFLICT", "tenant " + tenantName + " is being deleted"));
      auto found = next.databases.find(name);
      if (found != next.databases.end()) {
        if (found->second.status != "active")
          refuseWith(refused, ApiAnswer::error(409, "CONFLICT", "database " + name + " of tenant " + tenantName +
              " is being deleted"));
        made = found->second;
        refuseWith(refused, ApiAnswer::ok("{\"tenant\":" + quote(tenantName) + ",\"database\":" +
            namespaceRecordJson(name, made) + "}"));
      }
      // Every namespace of the tenant counts, the deleting ones and the Workers' classes included.
      size_t namespaces = next.databases.size() + 1;
      for (const auto& [worker, entry] : next.workers) namespaces += entry.namespaces.size();
      const auto quota = next.record->quotas.durableObjectNamespaces;
      if (namespaces > quota)
        refuseWith(refused, ApiAnswer::error(422, "QUOTA_EXCEEDED", "durableObjectNamespaces: tenant " + tenantName +
            " would have " + std::to_string(namespaces) + " Durable Object namespaces, its quota is " +
            std::to_string(quota)));
      made = ManifestNamespace{newNamespaceId(), next.revision + 1, "active", 1};
      next.databases.emplace(name, made);
      co_return;
    }, false);
    revision = written.manifest.revision;
  } catch (const kj::Exception& error) {
    if (refused) co_return *refused;
    co_return writeFailure(error, "the database");
  }
  // The revision a caller waits for: a database is usable once every node has read it (the deployment poll).
  co_return ApiAnswer::ok("{\"tenant\":" + quote(tenantName) + ",\"revision\":" + std::to_string(revision) +
      ",\"database\":" + namespaceRecordJson(name, made) + "}");
}

ApiAnswer listDatabases(const ManifestStore& state, std::string tenantName) {
  auto read = readTenant(state, tenantName);
  if (!read.tenant) return notFound(tenantName);
  const auto* manifest = state.tenant(tenantName);
  std::string out = "{\"tenant\":" + quote(tenantName) + ",\"databases\":[";
  size_t index = 0;
  if (manifest != nullptr)
    for (const auto& [name, entry] : manifest->databases)
      out += std::string(index++ ? "," : "") + namespaceRecordJson(name, entry);
  return ApiAnswer::ok(out + "]}");
}

ApiAnswer readDatabaseRecord(const ManifestStore& state, std::string tenantName, std::string name) {
  auto read = readTenant(state, tenantName);
  if (!read.tenant) return notFound(tenantName);
  const auto* manifest = state.tenant(tenantName);
  if (manifest == nullptr) return ApiAnswer::error(404, "NOT_FOUND", "tenant " + tenantName + " has no database " + name);
  auto found = manifest->databases.find(name);
  if (found == manifest->databases.end())
    return ApiAnswer::error(404, "NOT_FOUND", "tenant " + tenantName + " has no database " + name);
  return ApiAnswer::ok("{\"tenant\":" + quote(tenantName) + ",\"database\":" + namespaceRecordJson(name, found->second) + "}");
}

// DELETE /v1/tenants/{t}/databases/{d}: marked deleting, as a version's deleted_classes marks a class -- no Binding
// to it, no new request, and its objects' data stays until a later release's cleanup.
kj::Promise<ApiAnswer> deleteDatabase(ManifestStore& state, std::string tenantName, std::string name) {
  std::optional<ApiAnswer> refused;
  ManifestNamespace marked;
  uint64_t revision = 0;
  try {
    co_await state.writeTenant(tenantName, [&](TenantManifest& next) -> kj::Promise<void> {
      if (!next.record) refuseWith(refused, notFound(tenantName));
      auto found = next.databases.find(name);
      if (found == next.databases.end())
        refuseWith(refused, ApiAnswer::error(404, "NOT_FOUND", "tenant " + tenantName + " has no database " + name));
      found->second.status = "deleting";
      marked = found->second;
      co_return;
    }, false).then([&](ManifestStore::Written written) { revision = written.manifest.revision; });
  } catch (const kj::Exception& error) {
    if (refused) co_return *refused;
    co_return writeFailure(error, "the database");
  }
  co_return ApiAnswer::ok("{\"tenant\":" + quote(tenantName) + ",\"revision\":" + std::to_string(revision) +
      ",\"database\":" + namespaceRecordJson(name, marked) + "}");
}

ApiAnswer listNamespaceRecords(const ManifestStore& state, std::string tenantName, std::string worker) {
  auto read = readTenant(state, tenantName);
  if (!read.tenant) return notFound(tenantName);
  const auto* manifest = state.tenant(tenantName);
  const ManifestWorker* entry = nullptr;
  if (manifest != nullptr) {
    auto found = manifest->workers.find(worker);
    if (found != manifest->workers.end()) entry = &found->second;
  }
  if (entry == nullptr)
    return ApiAnswer::error(404, "NOT_FOUND", "tenant " + tenantName + " has no Worker " + worker);
  std::string out = "{\"tenant\":" + quote(tenantName) + ",\"worker\":" + quote(worker) + ",\"namespaces\":[";
  size_t index = 0;
  for (const auto& [className, space] : entry->namespaces)
    out += std::string(index++ ? "," : "") + namespaceRecordJson(className, space);
  return ApiAnswer::ok(out + "]}");
}

kj::Promise<ApiAnswer> deleteNamespaceRecord(ManifestStore& state, std::string tenantName, std::string worker,
    std::string className) {
  std::optional<ApiAnswer> refused;
  ManifestNamespace marked;
  uint64_t revision = 0;
  try {
    co_await state.writeTenant(tenantName, [&](TenantManifest& next) -> kj::Promise<void> {
      if (!next.record) refuseWith(refused, notFound(tenantName));
      auto found = next.workers.find(worker);
      if (found == next.workers.end())
        refuseWith(refused, ApiAnswer::error(404, "NOT_FOUND", "tenant " + tenantName + " has no Worker " + worker));
      auto space = found->second.namespaces.find(className);
      if (space == found->second.namespaces.end())
        refuseWith(refused, ApiAnswer::error(404, "NOT_FOUND", "Worker " + worker + " of tenant " + tenantName +
            " has no Durable Object namespace for class " + className));
      space->second.status = "deleting";
      marked = space->second;
      co_return;
    }, false).then([&](ManifestStore::Written written) { revision = written.manifest.revision; });
  } catch (const kj::Exception& error) {
    if (refused) co_return *refused;
    co_return writeFailure(error, "the namespace");
  }
  co_return ApiAnswer::ok("{\"tenant\":" + quote(tenantName) + ",\"worker\":" + quote(worker) + ",\"revision\":" +
      std::to_string(revision) + ",\"namespace\":" + namespaceRecordJson(className, marked) + "}");
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
    co_await state.writeCredentials([&](CredentialIndex& next) -> kj::Promise<void> {
      auto found = next.credentials.find(keyId);
      if (found == next.credentials.end() || found->second.scope != "tenant:" + tenantName)
        refuseWith(refused, ApiAnswer::error(404, "CREDENTIAL_NOT_FOUND", "tenant " + tenantName + " has no credential " + keyId));
      if (found->second.revokedAtUnixMs) { unchanged = true; revokedAt = found->second.revokedAtUnixMs; co_return; }
      found->second.revokedAtUnixMs = revokedAt = nowMs();
    });
  } catch (const kj::Exception& error) {
    if (refused) co_return *refused;
    co_return writeFailure(error, "the revocation");
  }
  co_return ApiAnswer::ok("{\"keyId\":" + quote(keyId) + ",\"revoked\":true,\"revokedAtUnixMs\":" + std::to_string(revokedAt) +
      (unchanged ? ",\"unchanged\":true}" : "}"));
}


kj::Promise<ApiAnswer> usage(ObjectStore& storage, UsageCache& cache, std::optional<std::string> tenant, uint64_t from,
    uint64_t to) {
  if (to <= from) co_return invalid("to must be after from");
  if (to - from > 366ull * 24 * 3600 * 1000) co_return invalid("at most a year at a time");
  const auto firstHour = usageHour(from), lastHour = usageHour(to - 1);
  // The usage objects (every node's hours) and the members: the nodes are the ones with objects under either.
  auto listings = co_await kj::joinPromises(kj::arr(storage.list(NODES_OBJECT_PREFIX), storage.list(MEMBERS_PREFIX)));
  std::set<std::string> ids;
  for (const auto& [id, node] : readMemberListing(listings[1])) ids.insert(id);
  struct Wanted { std::string key, etag, node; };
  std::vector<Wanted> wanted;
  std::set<std::string> listed;
  for (const auto& object : listings[0].objects) {
    auto parsed = parseNodeUsageKey(object.key);
    if (!parsed) continue;
    ids.insert(parsed->first);
    listed.insert(object.key);
    if (parsed->second >= firstHour && parsed->second <= lastHour) wanted.push_back({object.key, object.etag, parsed->first});
  }
  std::erase_if(cache.objects, [&](const auto& entry) { return !listed.contains(entry.first); });
  // Read what changed since the copy this process holds (the past hours do not), eight at a time.
  std::vector<const Wanted*> stale;
  for (const auto& object : wanted) {
    auto found = cache.objects.find(object.key);
    if (found == cache.objects.end() || found->second.first != object.etag || object.etag.empty()) stale.push_back(&object);
  }
  for (size_t start = 0; start < stale.size(); start += 8) {
    kj::Vector<kj::Promise<ObjectResponse>> reads;
    for (size_t i = start; i < std::min(stale.size(), start + 8); ++i) reads.add(storage.get(stale[i]->key));
    auto answers = co_await kj::joinPromises(reads.releaseAsArray());
    for (size_t i = 0; i < answers.size(); ++i) {
      const auto& object = *stale[start + i];
      if (answers[i].status == 404) { cache.objects.erase(object.key); continue; }  // removed since the listing
      requireStorage(answers[i].status == 200, "object store read failed: HTTP " + std::to_string(answers[i].status));
      ++cache.reads;
      try {
        cache.objects[object.key] = {object.etag, decodeNodeUsageHour(answers[i].bytes)};
      } catch (const kj::Exception&) {
        cache.objects.erase(object.key);  // not a usage object: left out
      }
    }
  }
  struct Sum { UsageCounts counts; uint64_t rows = 0; std::set<std::string> nodes; };
  std::map<std::pair<std::string, std::string>, Sum> sums;  // (hour, tenant)
  Sum total;
  for (const auto& object : wanted) {
    auto found = cache.objects.find(object.key);
    if (found == cache.objects.end()) continue;
    const auto& stored = found->second.second;
    // An object names the node and the hour of its key.
    if (stored.nodeId != object.node || nodeUsageKey(stored.nodeId, stored.hour) != object.key) continue;
    for (const auto& [name, counts] : stored.tenants) {
      if (tenant && name != *tenant) continue;
      for (auto* sum : {&sums[{stored.hour, name}], &total}) {
        sum->counts.add(counts);
        ++sum->rows;
        sum->nodes.insert(stored.nodeId);
      }
    }
  }
  auto fields = [](const Sum& sum) {
    return usageCountsFields(sum.counts) + ",\"nodes\":" + std::to_string(sum.nodes.size()) + ",\"nodeRows\":" + std::to_string(sum.rows);
  };
  std::string out = "{" + (tenant ? "\"tenant\":" + quote(*tenant) + "," : std::string()) + "\"from\":" + std::to_string(from) +
      ",\"to\":" + std::to_string(to) + ",\"by\":\"hour\",\"nodes\":[";
  size_t n = 0;
  for (const auto& id : ids) out += (n++ ? "," : "") + quote(id);
  out += "],\"rows\":[";
  size_t index = 0;
  for (const auto& [key, sum] : sums) {
    out += std::string(index++ ? "," : "") + "{\"hour\":" + quote(key.first) + ",\"startUnixMs\":" +
        std::to_string(usageHourStartMs(key.first)) + (tenant ? std::string() : ",\"tenant\":" + quote(key.second)) + "," + fields(sum) + "}";
  }
  co_return ApiAnswer::ok(out + "],\"total\":{" + fields(total) + "}}");
}

kj::Promise<UsageCleanup> cleanUsage(ObjectStore& storage, uint32_t retentionHours, uint64_t nowUnixMs) {
  UsageCleanup result;
  const uint64_t keep = uint64_t(retentionHours) * 3600000;
  if (!retentionHours || nowUnixMs <= keep) co_return result;
  result.cutoff = usageHour(nowUnixMs - keep);
  auto listing = co_await storage.list(NODES_OBJECT_PREFIX);
  for (const auto& object : listing.objects) {
    auto parsed = parseNodeUsageKey(object.key);
    if (!parsed) continue;
    ++result.listed;
    if (parsed->second >= result.cutoff) continue;
    auto removed = co_await storage.remove(object.key);
    requireStorage(removed.status == 200 || removed.status == 204 || removed.status == 404,
        "object store removal failed: HTTP " + std::to_string(removed.status));
    ++result.removed;
  }
  co_return result;
}

std::string UsageCleanup::json() const {
  return "{\"cutoff\":" + quote(cutoff) + ",\"listed\":" + std::to_string(listed) + ",\"removed\":" + std::to_string(removed) + "}";
}

}  // namespace coworkerd
