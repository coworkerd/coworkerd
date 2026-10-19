// Tenants, credentials and usage (docs/control-plane-api.md sections 1-5): the records below
// /coworkerd/v2/tenants/ and credentials/, which the control plane alone writes, and the usage rows each node writes
// below its own /coworkerd/v2/nodes/<nodeId>/usage/, which the control plane only reads and sums.
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

// One transaction; true when it committed.
struct Txn {
  std::vector<std::string> compares, operations;
  void absent(const std::string& key) {
    compares.push_back("{\"target\":\"VERSION\",\"result\":\"EQUAL\",\"key\":" + quote(base64(key)) + ",\"version\":\"0\"}");
  }
  void unchanged(const std::string& key, uint64_t modRevision) {
    compares.push_back("{\"target\":\"MOD\",\"result\":\"EQUAL\",\"key\":" + quote(base64(key)) + ",\"mod_revision\":\"" +
        std::to_string(modRevision) + "\"}");
  }
  // No key below the prefix was written after `revision`.
  void untouchedSince(const std::string& prefix, uint64_t revision) {
    compares.push_back("{\"target\":\"MOD\",\"result\":\"LESS\",\"key\":" + quote(base64(prefix)) + ",\"range_end\":" +
        quote(base64(prefixEnd(prefix))) + ",\"mod_revision\":\"" + std::to_string(revision + 1) + "\"}");
  }
  void put(const std::string& key, const std::string& value) {
    operations.push_back("{\"request_put\":{\"key\":" + quote(base64(key)) + ",\"value\":" + quote(base64(value)) + "}}");
  }
  void remove(const std::string& key) {
    operations.push_back("{\"request_delete_range\":{\"key\":" + quote(base64(key)) + "}}");
  }
  kj::Promise<bool> commit(EtcdClient& client) const {
    std::string json = "{\"compare\":[";
    for (size_t i = 0; i < compares.size(); ++i) json += (i ? "," : "") + compares[i];
    json += "],\"success\":[";
    for (size_t i = 0; i < operations.size(); ++i) json += (i ? "," : "") + operations[i];
    json += "]}";
    auto response = co_await client.request("/v3/kv/txn", json);
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(response.data(), response.size()), value);
    co_return boolean(value, "succeeded");
  }
};

kj::Promise<void> pause(kj::Timer& timer, uint32_t attempt) {
  static thread_local std::mt19937 random(std::random_device{}());
  const auto ceiling = std::min<uint32_t>(10u << std::min<uint32_t>(attempt, 4u), 160u);
  co_await timer.afterDelay(std::uniform_int_distribution<uint32_t>(ceiling / 2, ceiling)(random) * kj::MILLISECONDS);
}

std::string tenantKey(const std::string& tenant) { return std::string(TENANTS_PREFIX) + tenant; }
std::string credentialKey(const std::string& keyId) { return std::string(CREDENTIALS_PREFIX) + keyId; }

// Every tenant record of one read (unreadable records are skipped: no API wrote them).
std::map<std::string, std::pair<TenantRecord, uint64_t>> tenantsOf(const EtcdSnapshot& raw) {
  std::map<std::string, std::pair<TenantRecord, uint64_t>> result;
  for (const auto& [key, kv] : raw.values) {
    try {
      auto record = decodeTenantRecord(kv.value);
      // The name copied first: the pair's construction moves the record.
      const auto name = record.name;
      if (key == tenantKey(name)) result.emplace(name, std::pair{std::move(record), kv.modRevision});
    } catch (const kj::Exception&) {}
  }
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

kj::Promise<Authenticated> authenticate(EtcdClient& client, const std::string& adminCredential, const kj::HttpHeaders& headers) {
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
  if (!authorization) co_return refuse("missing");
  if (repeated || !authorization->starts_with("Bearer ")) co_return refuse("malformed");
  const auto presented = authorization->substr(7);
  if (!adminCredential.empty() && bearerRefusal(adminCredential, headers).empty()) co_return Authenticated{Caller{true, {}, {}}, {}, "admin"};
  auto token = parseCredential(presented);
  if (!token) co_return refuse("wrong");
  auto raw = co_await client.range(credentialKey(token->keyId));
  auto found = raw.values.find(credentialKey(token->keyId));
  if (found == raw.values.end()) co_return refuse("wrong");
  CredentialRecord record;
  try { record = decodeCredentialRecord(found->second.value); } catch (const kj::Exception&) { co_return refuse("wrong"); }
  if (record.keyId != token->keyId || !constantEquals(sha256Hex(token->secret), record.sha256)) co_return refuse("wrong");
  if (record.revokedAtUnixMs) co_return refuse("revoked");
  if (record.expiresAtUnixMs && nowMs() >= record.expiresAtUnixMs) co_return refuse("expired");
  co_return Authenticated{Caller{false, record.scope.substr(7), record.keyId}, {}, "tenant"};
}

kj::Promise<ReadTenant> readTenant(EtcdClient& client, const std::string& tenant) {
  ReadTenant result;
  auto raw = co_await client.range(tenantKey(tenant));
  result.revision = raw.revision;
  auto found = raw.values.find(tenantKey(tenant));
  if (found == raw.values.end()) co_return result;
  result.tenant = decodeTenantRecord(found->second.value);
  result.modRevision = found->second.modRevision;
  co_return result;
}

kj::Promise<ApiAnswer> createTenant(EtcdClient& client, kj::Timer& timer, std::string text) {
  auto body = parseBody(text);
  if (!body->problem.empty()) co_return invalid(body->problem);
  auto name = field(body->json, "name");
  if (!name.isString() || !tenantName(name.getString().cStr()))
    co_return invalid("name: a tenant is [a-z0-9][a-z0-9-]{0,62} (\"routes\" and \"generation\" are reserved)");
  TenantRecord tenant;
  tenant.name = name.getString().cStr();
  bool hostnamesChanged = false;
  if (auto problem = applyTenantFields(tenant, body->json, true, hostnamesChanged)) co_return *problem;
  for (uint32_t attempt = 0; attempt < 9; ++attempt) {
    auto raw = co_await client.range(TENANTS_PREFIX);
    auto tenants = tenantsOf(raw);
    if (raw.values.contains(tenantKey(tenant.name)))
      co_return ApiAnswer::error(409, "CONFLICT", "tenant " + tenant.name + " already exists");
    for (const auto& [other, entry] : tenants) for (const auto& host : entry.first.hostnames)
      if (std::find(tenant.hostnames.begin(), tenant.hostnames.end(), host) != tenant.hostnames.end())
        co_return ApiAnswer::error(409, "CONFLICT", "hostname " + host + " belongs to tenant " + other);
    tenant.createdAtUnixMs = tenant.updatedAtUnixMs = nowMs();
    Txn txn;
    txn.absent(tenantKey(tenant.name));
    // Hostnames are unique across tenants: no tenant changed since the read.
    txn.untouchedSince(TENANTS_PREFIX, raw.revision);
    txn.put(tenantKey(tenant.name), tenantRecordJson(tenant));
    if (co_await txn.commit(client)) co_return ApiAnswer::ok(tenantAnswer(tenant));
    co_await pause(timer, attempt);
  }
  co_return ApiAnswer::error(409, "CONFLICT", "other tenant writes kept changing the tenants (9 attempts); nothing written");
}

kj::Promise<ApiAnswer> listTenants(EtcdClient& client) {
  auto raw = co_await client.range(TENANTS_PREFIX);
  std::string out = "{\"revision\":\"" + std::to_string(raw.revision) + "\",\"tenants\":[";
  size_t index = 0;
  for (const auto& [name, entry] : tenantsOf(raw)) {
    auto record = tenantRecordJson(entry.first);
    out += (index++ ? "," : "") + record;
  }
  co_return ApiAnswer::ok(out + "]}");
}

kj::Promise<ApiAnswer> updateTenant(EtcdClient& client, kj::Timer& timer, std::string tenantName, std::string text) {
  auto body = parseBody(text);
  if (!body->problem.empty()) co_return invalid(body->problem);
  for (uint32_t attempt = 0; attempt < 9; ++attempt) {
    auto raw = co_await client.range(TENANTS_PREFIX);
    auto tenants = tenantsOf(raw);
    auto found = tenants.find(tenantName);
    if (found == tenants.end()) co_return notFound(tenantName);
    auto [tenant, modRevision] = found->second;
    if (tenant.status != "active") co_return ApiAnswer::error(409, "CONFLICT", "tenant " + tenantName + " is being deleted");
    const auto previous = tenant.hostnames;
    bool hostnamesChanged = false;
    if (auto problem = applyTenantFields(tenant, body->json, false, hostnamesChanged)) co_return *problem;
    if (hostnamesChanged) {
      for (const auto& [other, entry] : tenants) {
        if (other == tenantName) continue;
        for (const auto& host : entry.first.hostnames)
          if (std::find(tenant.hostnames.begin(), tenant.hostnames.end(), host) != tenant.hostnames.end())
            co_return ApiAnswer::error(409, "CONFLICT", "hostname " + host + " belongs to tenant " + other);
      }
      // A hostname still routed by one of the tenant's Workers stays until its route goes.
      auto routes = co_await client.range(std::string(DEPLOYMENT_PREFIX) + "routes/");
      for (const auto& [key, kv] : routes.values) {
        std::string host, owner;
        try {
          capnp::MallocMessageBuilder message;
          auto json = message.initRoot<Json>();
          capnp::JsonCodec().decodeRaw(kj::arrayPtr(kv.value.data(), kv.value.size()), json);
          host = etcd::text(json, "host"); owner = etcd::text(json, "tenant");
        } catch (const kj::Exception&) { continue; }
        if (owner == tenantName && std::find(previous.begin(), previous.end(), host) != previous.end() &&
            std::find(tenant.hostnames.begin(), tenant.hostnames.end(), host) == tenant.hostnames.end())
          co_return ApiAnswer::error(409, "CONFLICT", "hostname " + host + " is still routed by a Worker of tenant " + tenantName +
              "; replace that Worker's routes first");
      }
    }
    tenant.updatedAtUnixMs = nowMs();
    Txn txn;
    txn.unchanged(tenantKey(tenantName), modRevision);
    if (hostnamesChanged) txn.untouchedSince(TENANTS_PREFIX, raw.revision);
    txn.put(tenantKey(tenantName), tenantRecordJson(tenant));
    if (co_await txn.commit(client)) co_return ApiAnswer::ok(tenantAnswer(tenant));
    co_await pause(timer, attempt);
  }
  co_return ApiAnswer::error(409, "CONFLICT", "other writes kept changing tenant " + tenantName + " (9 attempts); nothing written");
}

kj::Promise<ApiAnswer> deleteTenant(ControlContext& context, std::string name) {
  auto& client = context.etcd;
  // Each step's record joins the tenant's `deletion` (visible in GET /v1/tenants/{t} while it runs).
  std::vector<std::string> steps;
  auto record = [&](const std::string& step, const std::string& result) -> kj::Promise<bool> {
    steps.push_back("{\"step\":" + quote(step) + ",\"atUnixMs\":" + std::to_string(nowMs()) + ",\"result\":" + result + "}");
    for (uint32_t attempt = 0; attempt < 9; ++attempt) {
      auto read = co_await readTenant(client, name);
      if (!read.tenant) co_return false;
      auto tenant = *read.tenant;
      std::string deletion = "{\"startedAtUnixMs\":";
      // Keep the first start of a deletion that runs again.
      uint64_t started = nowMs();
      if (tenant.deletion != "null") {
        capnp::MallocMessageBuilder message;
        auto json = message.initRoot<Json>();
        capnp::JsonCodec().decodeRaw(kj::arrayPtr(tenant.deletion.data(), tenant.deletion.size()), json);
        auto value = field(json, "startedAtUnixMs");
        if (value.isNumber()) started = static_cast<uint64_t>(value.getNumber());
      }
      deletion += std::to_string(started) + ",\"steps\":[";
      for (size_t i = 0; i < steps.size(); ++i) deletion += (i ? "," : "") + steps[i];
      tenant.status = "deleting";
      tenant.deletion = deletion + "]}";
      tenant.updatedAtUnixMs = nowMs();
      Txn txn;
      txn.unchanged(tenantKey(name), read.modRevision);
      txn.put(tenantKey(name), tenantRecordJson(tenant));
      if (co_await txn.commit(client)) co_return true;
      co_await pause(context.timer, attempt);
    }
    KJ_FAIL_REQUIRE("the tenant record kept changing while it was being deleted");
  };
  auto read = co_await readTenant(client, name);
  if (!read.tenant) co_return notFound(name);
  // 1. Marked deleting: the nodes stop serving it, the API refuses its writes.
  co_await record("marked", "{\"status\":\"deleting\"}");
  // 2. Its routes, pointers, the objects only it uses, and its keys below deploy/<tenant>/.
  auto state = co_await controlDeleteTenantState(context, name);
  {
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<Json>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(state.data(), state.size()), json);
    if (!boolean(json, "ok")) {
      co_await record("deployment", state);
      co_return ApiAnswer::error(503, "DELETION_INCOMPLETE", "the deletion of tenant " + name + " stopped at its deployment state: " +
          etcd::text(json, "reason") + "; DELETE it again to resume");
    }
    co_await record("deployment", capnp::JsonCodec().encodeRaw(field(json, "record")).cStr());
  }
  // 3. Its credentials.
  auto credentials = co_await client.range(CREDENTIALS_PREFIX);
  std::vector<std::string> keys;
  for (const auto& [key, kv] : credentials.values) {
    try { if (decodeCredentialRecord(kv.value).scope == "tenant:" + name) keys.push_back(key); } catch (const kj::Exception&) {}
  }
  for (size_t start = 0; start < keys.size(); start += 100) {
    Txn txn;
    for (size_t i = start; i < std::min(keys.size(), start + 100); ++i) txn.remove(keys[i]);
    KJ_REQUIRE(co_await txn.commit(client), "a credential deletion failed");
  }
  co_await record("credentials", "{\"deleted\":" + std::to_string(keys.size()) + "}");
  // 4. The record.
  auto last = co_await readTenant(client, name);
  if (last.tenant) {
    Txn txn;
    txn.unchanged(tenantKey(name), last.modRevision);
    txn.remove(tenantKey(name));
    if (!co_await txn.commit(client))
      co_return ApiAnswer::error(503, "DELETION_INCOMPLETE", "the tenant record changed at the last step; DELETE it again to finish");
  }
  std::string out = "{\"tenant\":" + quote(name) + ",\"deleted\":true,\"steps\":[";
  for (size_t i = 0; i < steps.size(); ++i) out += (i ? "," : "") + steps[i];
  co_return ApiAnswer::ok(out + "]}");
}

kj::Promise<ApiAnswer> createCredential(EtcdClient& client, kj::Timer& timer, std::string tenantName, std::string text) {
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
  for (uint32_t attempt = 0; attempt < 9; ++attempt) {
    auto read = co_await readTenant(client, tenantName);
    if (!read.tenant) co_return notFound(tenantName);
    if (read.tenant->status != "active") co_return ApiAnswer::error(409, "CONFLICT", "tenant " + tenantName + " is being deleted");
    CredentialToken token{randomHex(8), randomHex(32)};
    record.keyId = token.keyId;
    record.sha256 = sha256Hex(token.secret);
    record.createdAtUnixMs = nowMs();
    Txn txn;
    txn.absent(credentialKey(token.keyId));
    txn.unchanged(tenantKey(tenantName), read.modRevision);
    txn.put(credentialKey(token.keyId), credentialRecordJson(record));
    if (co_await txn.commit(client)) {
      // The credential's text is in this answer only; etcd keeps its hash.
      co_return ApiAnswer::ok("{\"keyId\":" + quote(record.keyId) + ",\"credential\":" + quote(credentialText(token)) +
          ",\"scope\":" + quote(record.scope) + ",\"description\":" + quote(record.description) + ",\"createdAtUnixMs\":" +
          std::to_string(record.createdAtUnixMs) + ",\"expiresAtUnixMs\":" + std::to_string(record.expiresAtUnixMs) + "}");
    }
    co_await pause(timer, attempt);
  }
  co_return ApiAnswer::error(409, "CONFLICT", "tenant " + tenantName + " kept changing (9 attempts); no credential created");
}

kj::Promise<ApiAnswer> listCredentials(EtcdClient& client, std::string tenantName) {
  auto read = co_await readTenant(client, tenantName);
  if (!read.tenant) co_return notFound(tenantName);
  auto raw = co_await client.range(CREDENTIALS_PREFIX);
  std::string out = "{\"tenant\":" + quote(tenantName) + ",\"credentials\":[";
  size_t index = 0;
  for (const auto& [key, kv] : raw.values) {
    CredentialRecord record;
    try { record = decodeCredentialRecord(kv.value); } catch (const kj::Exception&) { continue; }
    if (record.scope != "tenant:" + tenantName) continue;
    // Never the hash.
    out += std::string(index++ ? "," : "") + "{\"keyId\":" + quote(record.keyId) + ",\"scope\":" + quote(record.scope) +
        ",\"description\":" + quote(record.description) + ",\"createdAtUnixMs\":" + std::to_string(record.createdAtUnixMs) +
        ",\"expiresAtUnixMs\":" + std::to_string(record.expiresAtUnixMs) + ",\"revokedAtUnixMs\":" +
        std::to_string(record.revokedAtUnixMs) + "}";
  }
  co_return ApiAnswer::ok(out + "]}");
}

kj::Promise<ApiAnswer> revokeCredential(EtcdClient& client, kj::Timer& timer, std::string tenantName, std::string keyId) {
  if (keyId.size() != 16 || !std::all_of(keyId.begin(), keyId.end(), [](unsigned char c) { return std::isxdigit(c) && !std::isupper(c); }))
    co_return invalid("a key id is 16 lowercase hex characters");
  for (uint32_t attempt = 0; attempt < 9; ++attempt) {
    auto raw = co_await client.range(credentialKey(keyId));
    auto found = raw.values.find(credentialKey(keyId));
    CredentialRecord record;
    bool readable = found != raw.values.end();
    if (readable) { try { record = decodeCredentialRecord(found->second.value); } catch (const kj::Exception&) { readable = false; } }
    if (!readable || record.scope != "tenant:" + tenantName)
      co_return ApiAnswer::error(404, "CREDENTIAL_NOT_FOUND", "tenant " + tenantName + " has no credential " + keyId);
    if (record.revokedAtUnixMs) co_return ApiAnswer::ok("{\"keyId\":" + quote(keyId) + ",\"revoked\":true,\"revokedAtUnixMs\":" +
        std::to_string(record.revokedAtUnixMs) + ",\"unchanged\":true}");
    record.revokedAtUnixMs = nowMs();
    Txn txn;
    txn.unchanged(credentialKey(keyId), found->second.modRevision);
    txn.put(credentialKey(keyId), credentialRecordJson(record));
    if (co_await txn.commit(client))
      co_return ApiAnswer::ok("{\"keyId\":" + quote(keyId) + ",\"revoked\":true,\"revokedAtUnixMs\":" + std::to_string(record.revokedAtUnixMs) + "}");
    co_await pause(timer, attempt);
  }
  co_return ApiAnswer::error(409, "CONFLICT", "credential " + keyId + " kept changing (9 attempts)");
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
