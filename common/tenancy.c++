#include "tenancy.h"
#include "deployment.h"

#include <capnp/message.h>

#include <algorithm>
#include <cmath>
#include <ctime>
#include <set>

namespace coworkerd {
using namespace etcd;

namespace {
bool lowerAlnum(unsigned char c) { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'); }

// A JSON integer field: absent or null gives `fallback`; otherwise a non-negative integer below 2^53.
uint64_t number(Json::Reader object, const char* name, uint64_t fallback = 0) {
  auto value = field(object, name);
  if (value.isNull()) return fallback;
  KJ_REQUIRE(value.isNumber() && std::isfinite(value.getNumber()) && value.getNumber() >= 0 &&
      value.getNumber() < 9007199254740992.0 && std::floor(value.getNumber()) == value.getNumber(),
      "expected a non-negative integer", name);
  return static_cast<uint64_t>(value.getNumber());
}
double decimal(Json::Reader object, const char* name) {
  auto value = field(object, name);
  if (value.isNull()) return 0;
  KJ_REQUIRE(value.isNumber() && std::isfinite(value.getNumber()) && value.getNumber() >= 0, "expected a non-negative number", name);
  return value.getNumber();
}
std::string string(Json::Reader object, const char* name) {
  auto value = field(object, name);
  if (value.isNull()) return {};
  KJ_REQUIRE(value.isString(), "expected a string", name);
  return value.getString().cStr();
}
template <typename F>
auto decoded(const std::string& bytes, F&& read) {
  capnp::MallocMessageBuilder message;
  auto json = message.initRoot<Json>();
  capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), json);
  KJ_REQUIRE(json.isObject(), "expected a JSON object");
  return read(json.asReader());
}
std::string digits(double value) {
  // Milliseconds with microsecond precision, as JSON.
  char text[64];
  std::snprintf(text, sizeof(text), "%.3f", value);
  std::string out = text;
  while (out.back() == '0') out.pop_back();
  if (out.back() == '.') out.pop_back();
  return out;
}
}  // namespace

bool apiName(const std::string& value) {
  return !value.empty() && value.size() <= 63 && lowerAlnum(value[0]) &&
      std::all_of(value.begin(), value.end(), [](unsigned char c) { return lowerAlnum(c) || c == '-'; });
}
bool tenantName(const std::string& value) {
  return apiName(value) && value != "routes" && value != "generation";
}
bool workerFullName(const std::string& value) {
  const auto slash = value.find('/');
  return slash != std::string::npos && tenantName(value.substr(0, slash)) && value.size() - slash - 1 <= 255 &&
      deploymentSafeName(value.substr(slash + 1));
}
std::string tenantOf(const std::string& fullName) {
  const auto slash = fullName.find('/');
  return slash == std::string::npos ? std::string() : fullName.substr(0, slash);
}
std::string workerNameOf(const std::string& fullName) {
  const auto slash = fullName.find('/');
  return slash == std::string::npos ? fullName : fullName.substr(slash + 1);
}
std::string workerFullId(const std::string& tenant, const std::string& worker) { return tenant + "/" + worker; }

std::string workerLimitsJson(const WorkerLimits& limits) {
  std::string out = "{";
  if (limits.cpuMs) out += "\"cpuMs\":" + std::to_string(limits.cpuMs);
  if (limits.memoryMiB) out += std::string(out.size() > 1 ? "," : "") + "\"memoryMiB\":" + std::to_string(limits.memoryMiB);
  return out + '}';
}
WorkerLimits readWorkerLimits(Json::Reader object) {
  KJ_REQUIRE(object.isObject(), "limits is an object {\"cpuMs\",\"memoryMiB\"}");
  WorkerLimits limits;
  for (auto entry : object.getObject()) {
    const std::string name = entry.getName().cStr();
    KJ_REQUIRE(name == "cpuMs" || name == "memoryMiB", "unknown limit", name.c_str());
    if (entry.getValue().isNull()) continue;
    const auto value = number(object, name.c_str());
    if (value == 0 || value > 4294967295ull)
      KJ_FAIL_REQUIRE((name + " must be a positive whole number (" + (name == "cpuMs" ? "milliseconds" : "MiB") +
          "); 0 is refused -- leave the field out to name no limit").c_str());
    (name == "cpuMs" ? limits.cpuMs : limits.memoryMiB) = static_cast<uint32_t>(value);
  }
  return limits;
}

namespace {
struct QuotaField { const char* name; uint64_t TenantQuotas::* field; };
constexpr QuotaField QUOTA_FIELDS[] = {
  {"workers", &TenantQuotas::workers}, {"versionsPerWorker", &TenantQuotas::versionsPerWorker},
  {"packageBytes", &TenantQuotas::packageBytes}, {"assetFiles", &TenantQuotas::assetFiles},
  {"assetBytes", &TenantQuotas::assetBytes}, {"secrets", &TenantQuotas::secrets}, {"hostnames", &TenantQuotas::hostnames},
  {"logLinesPerSecond", &TenantQuotas::logLinesPerSecond}, {"logBufferBytes", &TenantQuotas::logBufferBytes},
  {"maxWorkerCpuMs", &TenantQuotas::maxWorkerCpuMs}, {"maxWorkerMemoryMiB", &TenantQuotas::maxWorkerMemoryMiB},
};
}  // namespace

std::string tenantQuotasJson(const TenantQuotas& quotas) {
  std::string out = "{";
  for (const auto& entry : QUOTA_FIELDS)
    out += std::string(out.size() > 1 ? "," : "") + "\"" + entry.name + "\":" + std::to_string(quotas.*(entry.field));
  return out + '}';
}
void applyTenantQuotas(TenantQuotas& quotas, Json::Reader object) {
  KJ_REQUIRE(object.isObject(), "quotas is an object");
  for (auto entry : object.getObject()) {
    const std::string name = entry.getName().cStr();
    auto found = std::find_if(std::begin(QUOTA_FIELDS), std::end(QUOTA_FIELDS), [&](const auto& f) { return name == f.name; });
    KJ_REQUIRE(found != std::end(QUOTA_FIELDS), "unknown quota", name.c_str());
    const auto value = number(object, name.c_str());
    KJ_REQUIRE(value > 0, "a quota is a positive integer", name.c_str());
    if (name == "logLinesPerSecond" || name == "maxWorkerCpuMs" || name == "maxWorkerMemoryMiB") {
      KJ_REQUIRE(value <= 4294967295ull, "quota too large", name.c_str());
    }
    if (name == "logBufferBytes") {
      KJ_REQUIRE(value >= 4096, "logBufferBytes is at least 4096", name.c_str());
    }
    quotas.*(found->field) = value;
  }
}

bool hostnameValid(const std::string& host) {
  if (host.empty() || host.size() > 253 || host.front() == '.' || host.back() == '.' || host.find('.') == std::string::npos)
    return false;
  size_t label = 0;
  for (size_t i = 0; i <= host.size(); ++i) {
    if (i == host.size() || host[i] == '.') {
      if (label == 0 || label > 63 || host[i - 1] == '-' || host[i - label] == '-') return false;
      label = 0;
      continue;
    }
    if (!lowerAlnum(static_cast<unsigned char>(host[i])) && host[i] != '-') return false;
    ++label;
  }
  return true;
}

std::string tenantRecordJson(const TenantRecord& tenant) {
  std::string out = "{\"name\":" + quote(tenant.name) + ",\"status\":" + quote(tenant.status) +
      ",\"createdAtUnixMs\":" + std::to_string(tenant.createdAtUnixMs) + ",\"updatedAtUnixMs\":" +
      std::to_string(tenant.updatedAtUnixMs) + ",\"hostnames\":[";
  for (size_t i = 0; i < tenant.hostnames.size(); ++i) out += (i ? "," : "") + quote(tenant.hostnames[i]);
  out += "],\"quotas\":" + tenantQuotasJson(tenant.quotas) + ",\"defaultLimits\":" + workerLimitsJson(tenant.defaultLimits);
  if (tenant.deletion != "null" && !tenant.deletion.empty()) out += ",\"deletion\":" + tenant.deletion;
  return out + '}';
}
TenantRecord decodeTenantRecord(const std::string& bytes) {
  return decoded(bytes, [](Json::Reader json) {
    TenantRecord tenant;
    tenant.name = string(json, "name");
    KJ_REQUIRE(tenantName(tenant.name), "invalid tenant record: name");
    tenant.status = string(json, "status");
    if (tenant.status.empty()) tenant.status = "active";
    KJ_REQUIRE(tenant.status == "active" || tenant.status == "deleting", "invalid tenant record: status");
    tenant.createdAtUnixMs = number(json, "createdAtUnixMs");
    tenant.updatedAtUnixMs = number(json, "updatedAtUnixMs");
    auto hosts = field(json, "hostnames");
    if (hosts.isArray()) for (auto host : hosts.getArray()) {
      KJ_REQUIRE(host.isString() && hostnameValid(host.getString().cStr()), "invalid tenant record: hostnames");
      tenant.hostnames.emplace_back(host.getString().cStr());
    }
    auto quotas = field(json, "quotas");
    if (quotas.isObject()) {
      // Lenient: a quota a later version adds is ignored here.
      for (const auto& entry : QUOTA_FIELDS) {
        auto value = field(quotas, entry.name);
        if (!value.isNull()) tenant.quotas.*(entry.field) = number(quotas, entry.name);
      }
    }
    auto limits = field(json, "defaultLimits");
    if (limits.isObject()) {
      tenant.defaultLimits.cpuMs = static_cast<uint32_t>(std::min<uint64_t>(number(limits, "cpuMs"), 4294967295ull));
      tenant.defaultLimits.memoryMiB = static_cast<uint32_t>(std::min<uint64_t>(number(limits, "memoryMiB"), 4294967295ull));
    }
    auto deletion = field(json, "deletion");
    if (deletion.isObject()) tenant.deletion = capnp::JsonCodec().encodeRaw(deletion).cStr();
    return tenant;
  });
}

std::string credentialRecordJson(const CredentialRecord& record) {
  return "{\"keyId\":" + quote(record.keyId) + ",\"scope\":" + quote(record.scope) + ",\"sha256\":" + quote(record.sha256) +
      ",\"description\":" + quote(record.description) + ",\"createdAtUnixMs\":" + std::to_string(record.createdAtUnixMs) +
      ",\"expiresAtUnixMs\":" + std::to_string(record.expiresAtUnixMs) + ",\"revokedAtUnixMs\":" +
      std::to_string(record.revokedAtUnixMs) + '}';
}
CredentialRecord decodeCredentialRecord(const std::string& bytes) {
  return decoded(bytes, [](Json::Reader json) {
    CredentialRecord record{string(json, "keyId"), string(json, "scope"), string(json, "sha256"), string(json, "description"),
        number(json, "createdAtUnixMs"), number(json, "expiresAtUnixMs"), number(json, "revokedAtUnixMs")};
    KJ_REQUIRE(record.keyId.size() == 16 && record.sha256.size() == 64 && record.scope.starts_with("tenant:") &&
        tenantName(record.scope.substr(7)), "invalid credential record");
    return record;
  });
}
std::optional<CredentialToken> parseCredential(const std::string& value) {
  auto hex = [](std::string_view text, size_t size) {
    return text.size() == size && std::all_of(text.begin(), text.end(), [](unsigned char c) {
      return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    });
  };
  if (value.size() != 4 + 16 + 1 + 64 || !value.starts_with("cwk_") || value[20] != '_') return std::nullopt;
  std::string_view text(value);
  if (!hex(text.substr(4, 16), 16) || !hex(text.substr(21), 64)) return std::nullopt;
  return CredentialToken{std::string(text.substr(4, 16)), std::string(text.substr(21))};
}
std::string credentialText(const CredentialToken& token) { return "cwk_" + token.keyId + "_" + token.secret; }

std::string usageRowJson(const UsageRow& row) {
  return "{\"tenant\":" + quote(row.tenant) + ",\"hour\":" + quote(row.hour) + ",\"nodeId\":" + quote(row.nodeId) +
      ",\"requests\":" + std::to_string(row.requests) + ",\"cpuMs\":" + digits(row.cpuMs) + ",\"egressBytes\":" +
      std::to_string(row.egressBytes) + ",\"logLines\":" + std::to_string(row.logLines) + ",\"isolateMs\":" +
      digits(row.isolateMs) + ",\"updatedAtUnixMs\":" + std::to_string(row.updatedAtUnixMs) + '}';
}
UsageRow decodeUsageRow(const std::string& bytes) {
  return decoded(bytes, [](Json::Reader json) {
    UsageRow row{string(json, "tenant"), string(json, "hour"), string(json, "nodeId"), number(json, "requests"),
        number(json, "logLines"), number(json, "egressBytes"), decimal(json, "cpuMs"), decimal(json, "isolateMs"),
        number(json, "updatedAtUnixMs")};
    KJ_REQUIRE(tenantName(row.tenant) && !row.nodeId.empty(), "invalid usage row");
    usageHourStartMs(row.hour);
    return row;
  });
}
std::string usageHour(uint64_t unixMs) {
  const time_t seconds = static_cast<time_t>(unixMs / 1000);
  std::tm utc{};
  gmtime_r(&seconds, &utc);
  char text[32];
  std::strftime(text, sizeof(text), "%Y-%m-%dT%H", &utc);
  return text;
}
uint64_t usageHourStartMs(const std::string& hour) {
  KJ_REQUIRE(hour.size() == 13 && hour[4] == '-' && hour[7] == '-' && hour[10] == 'T', "invalid usage hour", hour.c_str());
  for (size_t i : {0, 1, 2, 3, 5, 6, 8, 9, 11, 12}) KJ_REQUIRE(hour[i] >= '0' && hour[i] <= '9', "invalid usage hour", hour.c_str());
  std::tm utc{};
  utc.tm_year = std::stoi(hour.substr(0, 4)) - 1900;
  utc.tm_mon = std::stoi(hour.substr(5, 2)) - 1;
  utc.tm_mday = std::stoi(hour.substr(8, 2));
  utc.tm_hour = std::stoi(hour.substr(11, 2));
  KJ_REQUIRE(utc.tm_mon >= 0 && utc.tm_mon < 12 && utc.tm_mday >= 1 && utc.tm_mday <= 31 && utc.tm_hour < 24,
      "invalid usage hour", hour.c_str());
  const auto seconds = timegm(&utc);
  KJ_REQUIRE(seconds >= 0 && usageHour(uint64_t(seconds) * 1000) == hour, "invalid usage hour", hour.c_str());
  return uint64_t(seconds) * 1000;
}

std::string nodeMemberKey(const std::string& nodeId) { return std::string(NODES_PREFIX) + nodeId; }
std::string nodeSuspectKey(const std::string& nodeId, const std::string& tenant) {
  return std::string(NODES_PREFIX) + nodeId + "/suspects/" + tenant;
}
std::string nodeUsagePrefix(const std::string& nodeId) { return std::string(NODES_PREFIX) + nodeId + "/usage/"; }
std::string nodeUsageKey(const std::string& nodeId, const std::string& hour, const std::string& tenant) {
  return nodeUsagePrefix(nodeId) + hour + "/" + tenant;
}

}  // namespace coworkerd
