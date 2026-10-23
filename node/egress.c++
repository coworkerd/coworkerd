// Tenant Workers' outbound requests: see egress.h.
#include "egress.h"
#include "support.h"
#include "tenancy.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/compat/tls.h>
#include <kj/debug.h>
#include <openssl/rand.h>

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <strings.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <thread>
#include <tuple>
#include <variant>

namespace coworkerd {
namespace {

constexpr const char* SOURCE_HEADER = "x-coworkerd-egress-source";
constexpr const char* ERROR_HEADER = "x-coworkerd-egress-error";
constexpr size_t MAX_HOST_BYTES = 253;
constexpr size_t PUMP_BUFFER_BYTES = 65536;
constexpr size_t DNS_CACHE_ENTRIES = 4096;
constexpr uint64_t CLUSTER_REFRESH_MS = 5000;
// Limit refusals are counted each, but logged at most once a second per tenant (with the count of the others).
constexpr double LIMIT_LINE_INTERVAL_MS = 1000;

bool sameText(kj::StringPtr a, const char* b) { return strcasecmp(a.cStr(), b) == 0; }
bool startsWithText(kj::StringPtr a, const char* prefix) { return strncasecmp(a.cStr(), prefix, strlen(prefix)) == 0; }
std::string lower(std::string value) {
  for (auto& c : value) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return value;
}

// ---- Addresses and the built-in classes -----------------------------------------------------------------------
struct Address {
  bool v6 = false;
  std::array<uint8_t, 16> bytes{};  // IPv4: the first four
  bool operator<(const Address& other) const { return std::tie(v6, bytes) < std::tie(other.v6, other.bytes); }
  bool operator==(const Address& other) const { return v6 == other.v6 && bytes == other.bytes; }
  std::string text() const {
    char buffer[INET6_ADDRSTRLEN] = {};
    inet_ntop(v6 ? AF_INET6 : AF_INET, bytes.data(), buffer, sizeof(buffer));
    return buffer;
  }
};
std::optional<Address> parseIp(const std::string& text) {
  Address address;
  if (inet_pton(AF_INET, text.c_str(), address.bytes.data()) == 1) return address;
  address.v6 = true;
  if (inet_pton(AF_INET6, text.c_str(), address.bytes.data()) == 1) return address;
  return std::nullopt;
}
std::optional<Address> fromSockaddr(const sockaddr* value) {
  Address address;
  if (value->sa_family == AF_INET) {
    memcpy(address.bytes.data(), &reinterpret_cast<const sockaddr_in*>(value)->sin_addr, 4);
    return address;
  }
  if (value->sa_family == AF_INET6) {
    address.v6 = true;
    memcpy(address.bytes.data(), &reinterpret_cast<const sockaddr_in6*>(value)->sin6_addr, 16);
    return address;
  }
  return std::nullopt;
}
Address v4At(const Address& address, size_t offset) {
  Address inner;
  memcpy(inner.bytes.data(), address.bytes.data() + offset, 4);
  return inner;
}

struct Range {
  Address base;
  int bits = 0;
  bool contains(const Address& address) const {
    if (address.v6 != base.v6) return false;
    const int whole = bits / 8, rest = bits % 8;
    if (memcmp(address.bytes.data(), base.bytes.data(), whole) != 0) return false;
    if (rest == 0) return true;
    const uint8_t mask = static_cast<uint8_t>(0xff << (8 - rest));
    return (address.bytes[whole] & mask) == (base.bytes[whole] & mask);
  }
};
std::optional<Range> parseRange(const std::string& text) {
  const auto slash = text.find('/');
  auto address = parseIp(text.substr(0, slash));
  if (!address) return std::nullopt;
  const int limit = address->v6 ? 128 : 32;
  int bits = limit;
  if (slash != std::string::npos) {
    const auto digits = text.substr(slash + 1);
    if (digits.empty() || digits.size() > 3 || !std::all_of(digits.begin(), digits.end(), [](char c) { return c >= '0' && c <= '9'; }))
      return std::nullopt;
    bits = std::stoi(digits);
    if (bits > limit) return std::nullopt;
  }
  return Range{*address, bits};
}
Range range(const char* text) {
  auto value = parseRange(text);
  KJ_ASSERT(value.has_value(), "a built-in range", text);
  return *value;
}

struct Cluster {
  std::set<Address> addresses;
  std::vector<Range> ranges;
  bool contains(const Address& address) const {
    return addresses.contains(address) || std::any_of(ranges.begin(), ranges.end(), [&](const Range& r) { return r.contains(address); });
  }
};

// The IPv4 addresses an IPv6 form carries, judged in its place: an IPv4-mapped address, NAT64's well-known prefix and
// 6to4 carry one. The local-use NAT64 prefix 64:ff9b:1::/48 may carry one at any of the places RFC 6052 allows inside
// it (a /48, /56, /64 or /96 translator prefix), and which one a translator uses is not known here, so every reading
// whose other bits are as RFC 6052 requires -- bits 64 to 71 zero and the bits after the address zero -- counts, and
// an address that reads as several is judged by all of them. None for any other address.
std::vector<Address> carried(const Address& address) {
  if (!address.v6) return {};
  static const Range MAPPED = range("::ffff:0:0/96"), NAT64 = range("64:ff9b::/96"), LOCAL_NAT64 = range("64:ff9b:1::/48"),
      SIX_TO_FOUR = range("2002::/16");
  if (MAPPED.contains(address) || NAT64.contains(address)) return {v4At(address, 12)};
  if (SIX_TO_FOUR.contains(address)) return {v4At(address, 2)};
  if (!LOCAL_NAT64.contains(address)) return {};
  if (address.bytes[8] != 0) return {};  // RFC 6052: the byte at bits 64 to 71 is zero in every embedding
  std::vector<Address> inner;
  // {the bytes of the address} with {the bytes that must be zero after it}, by translator prefix length.
  static const std::vector<std::pair<std::array<int, 4>, std::array<int, 5>>> READINGS = {
    {{6, 7, 9, 10}, {11, 12, 13, 14, 15}},   // /48
    {{7, 9, 10, 11}, {12, 13, 14, 15, 8}},   // /56
    {{9, 10, 11, 12}, {13, 14, 15, 8, 8}},   // /64
    {{12, 13, 14, 15}, {8, 8, 8, 8, 8}},     // /96 (nothing follows it)
  };
  for (const auto& [bytes, zeroes] : READINGS) {
    if (!std::all_of(zeroes.begin(), zeroes.end(), [&](int at) { return address.bytes[at] == 0; })) continue;
    Address one;
    for (size_t i = 0; i < bytes.size(); ++i) one.bytes[i] = address.bytes[bytes[i]];
    if (std::find(inner.begin(), inner.end(), one) == inner.end()) inner.push_back(one);
  }
  return inner;
}
// What a destination address is judged as: the IPv4 addresses it carries, else itself.
std::vector<Address> judged(const Address& address) {
  auto inner = carried(address);
  return inner.empty() ? std::vector<Address>{address} : inner;
}

// The class of an address (as judged()): "" for a public one; "private-network" for an internal one, which only a
// tenant's allowInternal opens; any other name is a denial nothing opens (docs/egress-and-auth.md, section 3).
std::string classify(const Address& address, const Cluster& cluster) {
  if (cluster.contains(address)) return "cluster";
  static const std::vector<std::pair<Range, const char*>> V4 = {
    {range("169.254.169.254/32"), "metadata"}, {range("169.254.170.2/32"), "metadata"},
    {range("169.254.169.253/32"), "metadata"}, {range("100.100.100.200/32"), "metadata"},
    {range("168.63.129.16/32"), "metadata"},
    {range("0.0.0.0/8"), "unspecified"}, {range("127.0.0.0/8"), "loopback"}, {range("169.254.0.0/16"), "link-local"},
    {range("10.0.0.0/8"), "private-network"}, {range("172.16.0.0/12"), "private-network"},
    {range("192.168.0.0/16"), "private-network"}, {range("100.64.0.0/10"), "private-network"},
    {range("198.18.0.0/15"), "private-network"},
    {range("192.0.0.0/24"), "reserved"}, {range("192.0.2.0/24"), "reserved"}, {range("198.51.100.0/24"), "reserved"},
    {range("203.0.113.0/24"), "reserved"}, {range("192.88.99.0/24"), "reserved"}, {range("224.0.0.0/4"), "reserved"},
    {range("240.0.0.0/4"), "reserved"},
  };
  // The clouds' IPv6 metadata services sit in the unique-local range, which allowInternal could otherwise open: Amazon
  // EC2's instance metadata, DNS and time services, Compute Engine's, Akamai's (Linode) and Oracle Cloud's metadata.
  static const std::vector<std::pair<Range, const char*>> V6 = {
    {range("fd00:ec2::254/128"), "metadata"}, {range("fd00:ec2::253/128"), "metadata"}, {range("fd00:ec2::123/128"), "metadata"},
    {range("fd00:ec2::23/128"), "metadata"}, {range("fd20:ce::254/128"), "metadata"}, {range("fd00:a9fe:a9fe::1/128"), "metadata"},
    {range("fd00:c1::a9fe:a9fe/128"), "metadata"},
    {range("::/128"), "unspecified"}, {range("::1/128"), "loopback"}, {range("fe80::/10"), "link-local"},
    {range("fc00::/7"), "private-network"}, {range("fec0::/10"), "private-network"},
    {range("2001::/32"), "reserved"}, {range("2001:db8::/32"), "reserved"}, {range("3fff::/20"), "reserved"},
    {range("100::/64"), "reserved"}, {range("ff00::/8"), "reserved"},
  };
  if (!address.v6) {
    for (const auto& [r, name] : V4) if (r.contains(address)) return name;
    return "";
  }
  // An IPv6 form that carries an IPv4 address is classified by it (callers judge such addresses first).
  if (auto inner = carried(address); !inner.empty()) {
    std::string worst = "";
    for (const auto& one : inner) {
      auto name = classify(one, cluster);
      if (!name.empty() && (worst.empty() || worst == "private-network")) worst = name;
    }
    return worst;
  }
  for (const auto& [r, name] : V6) if (r.contains(address)) return name;
  // Outside 2000::/3, the global unicast space, nothing is public.
  if ((address.bytes[0] & 0xe0) != 0x20) return "reserved";
  return "";
}

// ---- Rules ----------------------------------------------------------------------------------------------------
bool validHost(const std::string& host) {
  if (host.empty() || host.size() > MAX_HOST_BYTES || host.front() == '.' || host.front() == '-') return false;
  if (host.find("..") != std::string::npos) return false;
  return std::all_of(host.begin(), host.end(), [](char c) {
    return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '-' || c == '_';
  });
}
std::optional<uint16_t> parsePort(const std::string& text) {
  if (text.empty() || text.size() > 5 || !std::all_of(text.begin(), text.end(), [](char c) { return c >= '0' && c <= '9'; }))
    return std::nullopt;
  const int value = std::stoi(text);
  if (value < 1 || value > 65535) return std::nullopt;
  return static_cast<uint16_t>(value);
}

struct Rule {
  enum class Kind { NAME, SUFFIX, RANGE } kind = Kind::NAME;
  std::string name;  // NAME: the host; SUFFIX: ".example.com"
  Range addresses;
  uint16_t port = 0;  // 0: any port
  std::string text;
  bool matchesName(const std::string& host, uint16_t requested) const {
    if (port != 0 && port != requested) return false;
    if (kind == Kind::NAME) return host == name;
    if (kind == Kind::SUFFIX) return host.size() > name.size() && host.ends_with(name);
    return false;
  }
  bool matchesAddress(const Address& address, uint16_t requested) const {
    if (port != 0 && port != requested) return false;
    return kind == Kind::RANGE && addresses.contains(address);
  }
};
// A name (api.example.com), a wildcard (*.example.com), an address or CIDR (203.0.113.7, 198.51.100.0/24,
// [2001:db8::/32]), each with an optional :PORT.
Rule parseRule(const std::string& raw, const char* list) {
  Rule rule;
  rule.text = raw;
  std::string text = lower(raw);
  auto bad = [&]() { KJ_FAIL_REQUIRE("invalid egress rule (HOST[:PORT], *.DOMAIN[:PORT], ADDRESS[/BITS][:PORT], [IPV6[/BITS]][:PORT])",
      list, raw.c_str()); };
  std::string host = text, port;
  if (text.starts_with('[')) {
    const auto close = text.find(']');
    if (close == std::string::npos) bad();
    host = text.substr(1, close - 1);
    const auto rest = text.substr(close + 1);
    if (!rest.empty()) {
      if (rest.front() != ':') bad();
      port = rest.substr(1);
    }
    auto value = parseRange(host);
    if (!value || !value->base.v6) bad();
    rule.kind = Rule::Kind::RANGE;
    rule.addresses = *value;
  } else {
    // At most one colon outside brackets: HOST:PORT (an IPv6 address needs its brackets).
    const auto colon = text.find(':');
    if (colon != std::string::npos) {
      if (text.find(':', colon + 1) != std::string::npos) bad();
      host = text.substr(0, colon);
      port = text.substr(colon + 1);
    }
    if (auto value = parseRange(host)) {
      rule.kind = Rule::Kind::RANGE;
      rule.addresses = *value;
    } else if (host.starts_with("*.")) {
      rule.kind = Rule::Kind::SUFFIX;
      rule.name = host.substr(1);
      if (!validHost(rule.name.substr(1))) bad();
    } else {
      rule.kind = Rule::Kind::NAME;
      rule.name = host;
      if (!validHost(host)) bad();
    }
  }
  if (!port.empty()) {
    auto value = parsePort(port);
    if (!value) bad();
    rule.port = *value;
  }
  return rule;
}
std::vector<Rule> parseRules(const std::vector<std::string>& texts, const char* list) {
  std::vector<Rule> rules;
  for (const auto& text : texts) rules.push_back(parseRule(text, list));
  return rules;
}

struct TenantRules {
  std::vector<Rule> allow, deny, allowInternal;
  uint32_t concurrency = 0, requestsPerSecond = 0;
};

// ---- The destination of a request -----------------------------------------------------------------------------
struct Destination {
  bool tls = false;
  std::string scheme, host, authority, target;  // authority: host[:port] as written; target: path and query
  uint16_t port = 0;
  std::optional<Address> literal;
};
// The request line's URL in the proxy form: http(s)://authority/path?query (a fragment is never sent). Empty
// optional with the rule's name on failure.
std::variant<Destination, std::string> parseDestination(const std::string& url) {
  Destination d;
  const auto schemeEnd = url.find("://");
  if (schemeEnd == std::string::npos) return std::string("scheme");
  d.scheme = lower(url.substr(0, schemeEnd));
  if (d.scheme == "https") d.tls = true;
  else if (d.scheme != "http") return std::string("scheme");
  const auto authorityStart = schemeEnd + 3;
  const auto authorityEnd = url.find_first_of("/?#", authorityStart);
  d.authority = url.substr(authorityStart, authorityEnd == std::string::npos ? std::string::npos : authorityEnd - authorityStart);
  std::string rest = authorityEnd == std::string::npos ? std::string() : url.substr(authorityEnd);
  if (auto hash = rest.find('#'); hash != std::string::npos) rest.resize(hash);
  d.target = rest.empty() || rest.front() != '/' ? "/" + rest : rest;
  if (d.authority.empty() || d.authority.find('@') != std::string::npos) return std::string("url");
  std::string port;
  if (d.authority.front() == '[') {
    const auto close = d.authority.find(']');
    if (close == std::string::npos) return std::string("url");
    d.host = lower(d.authority.substr(1, close - 1));
    const auto rest2 = d.authority.substr(close + 1);
    if (!rest2.empty()) {
      if (rest2.front() != ':') return std::string("url");
      port = rest2.substr(1);
    }
    d.literal = parseIp(d.host);
    if (!d.literal || !d.literal->v6) return std::string("url");
  } else {
    const auto colon = d.authority.rfind(':');
    d.host = lower(colon == std::string::npos ? d.authority : d.authority.substr(0, colon));
    if (colon != std::string::npos) port = d.authority.substr(colon + 1);
    while (!d.host.empty() && d.host.back() == '.') d.host.pop_back();  // the absolute form of a name
    d.literal = parseIp(d.host);
    if (d.literal && d.literal->v6) return std::string("url");
    if (!d.literal && !validHost(d.host)) return std::string("url");
  }
  if (port.empty()) d.port = d.tls ? 443 : 80;
  else if (auto value = parsePort(port)) d.port = *value;
  else return std::string("url");
  return d;
}

// The host of a URL the destination parser refused, for the [egress] line: what follows the scheme up to the path,
// without user information and port, lower case; "" when it is not a host name or address.
std::string hostOf(const std::string& url) {
  const auto scheme = url.find("://");
  std::string rest = scheme == std::string::npos ? url : url.substr(scheme + 3);
  rest = rest.substr(0, rest.find_first_of("/?#"));
  if (const auto at = rest.rfind('@'); at != std::string::npos) rest = rest.substr(at + 1);
  if (rest.starts_with('[')) {
    const auto close = rest.find(']');
    rest = close == std::string::npos ? std::string() : rest.substr(1, close - 1);
  } else if (const auto colon = rest.rfind(':'); colon != std::string::npos) {
    rest.resize(colon);
  }
  rest = lower(rest);
  const bool plain = rest.size() <= MAX_HOST_BYTES && std::all_of(rest.begin(), rest.end(), [](char c) {
    return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '-' || c == '_' || c == ':';
  });
  return plain ? rest : std::string();
}

// Headers that belong to one connection (RFC 9110, section 7.6.1), or that KJ writes itself.
bool connectionHeader(kj::StringPtr name) {
  for (const char* header : {"connection", "keep-alive", "proxy-connection", "proxy-authenticate", "proxy-authorization", "te",
           "trailer", "transfer-encoding", "upgrade", "content-length", "host"})
    if (sameText(name, header)) return true;
  return false;
}

// The WebSocket client's keys (KJ needs an entropy source to open one).
class RandomBytes final: public kj::EntropySource {
public:
  void generate(kj::ArrayPtr<kj::byte> buffer) override {
    KJ_REQUIRE(RAND_bytes(buffer.begin(), static_cast<int>(buffer.size())) == 1, "cannot draw random bytes");
  }
};

struct Counts {
  uint64_t requests = 0, allowed = 0, deniedTotal = 0, failedTotal = 0, requestBytes = 0, responseBytes = 0;
  uint32_t inFlight = 0, maxInFlight = 0;
  std::map<std::string, uint64_t> denied, failed;
  uint64_t suppressedLines = 0;
  double tokens = -1, refilledAtMs = 0, lastLimitLineMs = -1e18;
};

std::string countsJson(const std::map<std::string, uint64_t>& values) {
  std::string out = "{";
  for (const auto& [name, count] : values) {
    if (out.size() > 1) out += ',';
    out += "\"" + jsonEscape(name) + "\":" + std::to_string(count);
  }
  return out + "}";
}

}  // namespace

// ---- The service ----------------------------------------------------------------------------------------------
struct EgressService::Impl {
  Impl(const EgressConfig& config, std::string serverId, kj::Network& network, kj::Timer& timer,
      const kj::HttpHeaderTable& headerTable, std::function<std::vector<std::string>()> clusterEndpoints,
      std::function<std::optional<EgressSource>(const std::string&)> source,
      std::function<void(const std::string&, uint64_t)> metered)
      : config(config), serverId(std::move(serverId)), network(network), timer(timer), headerTable(headerTable),
        clusterEndpoints(std::move(clusterEndpoints)), source(std::move(source)), metered(std::move(metered)) {
    for (const auto& [tenant, policy] : config.tenants) {
      auto& rules = tenants[tenant];
      rules.allow = parseRules(policy.allow, "allow");
      rules.deny = parseRules(policy.deny, "deny");
      rules.allowInternal = parseRules(policy.allowInternal, "allowInternal");
      rules.concurrency = policy.concurrency;
      rules.requestsPerSecond = policy.requestsPerSecond;
    }
    for (const auto& text : config.clusterAddresses) {
      // An address or CIDR here is denied as written; a name is resolved at every refresh.
      if (auto value = parseRange(text)) staticCluster.ranges.push_back(*value);
      else if (auto [host, port] = splitHostPort(text); validHost(host)) configuredNames.push_back(host);
      else KJ_FAIL_REQUIRE("invalid egress.clusterAddresses entry (HOST[:PORT], ADDRESS or CIDR)", text.c_str());
    }
    kj::TlsContext::Options options;
    options.useSystemTrustStore = config.systemTrustStore;
    if (!config.caFile.empty()) {
      std::ifstream input(config.caFile);
      KJ_REQUIRE(input.good(), "cannot read egress.caFile", config.caFile.c_str());
      std::stringstream text;
      text << input.rdbuf();
      const std::string pem = text.str();
      const std::string begin = "-----BEGIN CERTIFICATE-----", end = "-----END CERTIFICATE-----";
      for (size_t at = pem.find(begin); at != std::string::npos; at = pem.find(begin, at + 1)) {
        const auto stop = pem.find(end, at);
        KJ_REQUIRE(stop != std::string::npos, "egress.caFile holds a truncated certificate", config.caFile.c_str());
        const std::string one = pem.substr(at, stop + end.size() - at);
        certificates.emplace_back(kj::StringPtr(one.c_str()));
      }
      KJ_REQUIRE(!certificates.empty(), "egress.caFile holds no PEM certificate", config.caFile.c_str());
    }
    options.trustedCertificates = kj::arrayPtr(certificates.data(), certificates.size());
    tls = kj::heap<kj::TlsContext>(kj::mv(options));
    auto ready = kj::newPromiseAndFulfiller<void>();
    readyPromise = ready.promise.fork();
    readyFulfiller = kj::mv(ready.fulfiller);
  }

  static std::pair<std::string, std::string> splitHostPort(const std::string& text) {
    if (text.starts_with('[')) return {"", ""};
    const auto colon = text.rfind(':');
    if (colon == std::string::npos) return {lower(text), ""};
    return {lower(text.substr(0, colon)), text.substr(colon + 1)};
  }

  // ---- Resolution (the node's own; getaddrinfo off the event loop) ----
  kj::Promise<std::vector<Address>> lookup(std::string host) {
    auto pair = kj::newPromiseAndCrossThreadFulfiller<std::vector<Address>>();
    // Held through a shared_ptr: the thread's state must be destructible without throwing (KJ's Own may throw).
    auto fulfiller = std::make_shared<kj::Own<kj::CrossThreadPromiseFulfiller<std::vector<Address>>>>(kj::mv(pair.fulfiller));
    std::thread([host = std::move(host), fulfiller]() mutable {
      addrinfo hints{};
      hints.ai_family = AF_UNSPEC;
      hints.ai_socktype = SOCK_STREAM;
      hints.ai_flags = AI_ADDRCONFIG;
      addrinfo* result = nullptr;
      const int code = getaddrinfo(host.c_str(), nullptr, &hints, &result);
      if (code != 0) {
        (*fulfiller)->reject(KJ_EXCEPTION(FAILED, "name resolution failed", gai_strerror(code)));
        return;
      }
      std::vector<Address> addresses;
      for (auto* entry = result; entry != nullptr; entry = entry->ai_next) {
        auto address = fromSockaddr(entry->ai_addr);
        if (address && std::find(addresses.begin(), addresses.end(), *address) == addresses.end()) addresses.push_back(*address);
      }
      freeaddrinfo(result);
      if (addresses.empty()) (*fulfiller)->reject(KJ_EXCEPTION(FAILED, "name resolution gave no address"));
      else (*fulfiller)->fulfill(kj::mv(addresses));
    }).detach();
    return kj::mv(pair.promise);
  }
  kj::Promise<std::vector<Address>> resolve(const std::string& host) {
    const double now = monotonicTimeMs();
    if (config.dnsCacheMs > 0) {
      auto found = dnsCache.find(host);
      if (found != dnsCache.end() && found->second.second > now) {
        ++dnsHits;
        co_return found->second.first;
      }
    }
    ++dnsLookups;
    std::vector<Address> addresses;
    try {
      addresses = co_await lookup(host);
    } catch (const kj::Exception&) {
      ++dnsFailures;
      throw;
    }
    if (config.dnsCacheMs > 0) {
      if (dnsCache.size() >= DNS_CACHE_ENTRIES) dnsCache.clear();
      dnsCache[host] = {addresses, monotonicTimeMs() + config.dnsCacheMs};
    }
    co_return addresses;
  }

  // ---- The cluster's own addresses ----
  // The endpoints as the node knows them now; a request that sees others than the last refresh did (a member joined)
  // waits for a refresh before anything is checked.
  std::string endpointsSignature() {
    std::string text;
    for (const auto& endpoint : clusterEndpoints()) text += endpoint + "\n";
    return text;
  }
  kj::Promise<void> fresh() {
    if (endpointsSignature() == refreshedSignature) return kj::READY_NOW;
    // A settled refresh is released here, never from its own continuation (KJ's fork must outlive that).
    if (refreshSettled) { refreshing = kj::none; refreshSettled = false; }
    KJ_IF_SOME(running, refreshing) return running.addBranch();
    auto promise = refreshCluster().then([this]() { refreshSettled = true; }, [this](kj::Exception&& error) {
      refreshSettled = true;
      kj::throwFatalException(kj::mv(error));
    }).fork();
    auto branch = promise.addBranch();
    refreshing = kj::mv(promise);
    return branch;
  }
  kj::Promise<void> refreshCluster() {
    const auto signature = endpointsSignature();
    Cluster next = staticCluster;
    // This node's own interfaces: whatever it listens on is reachable there.
    ifaddrs* interfaces = nullptr;
    if (getifaddrs(&interfaces) == 0) {
      for (auto* entry = interfaces; entry != nullptr; entry = entry->ifa_next) {
        if (entry->ifa_addr == nullptr) continue;
        if (auto address = fromSockaddr(entry->ifa_addr)) next.addresses.insert(*address);
      }
      freeifaddrs(interfaces);
    }
    std::set<std::string> names(configuredNames.begin(), configuredNames.end());
    for (const auto& endpoint : clusterEndpoints()) {
      std::string text = endpoint;
      if (auto scheme = text.find("://"); scheme != std::string::npos) text = text.substr(scheme + 3);
      if (auto slash = text.find('/'); slash != std::string::npos) text.resize(slash);
      if (text.starts_with('[')) {
        if (auto close = text.find(']'); close != std::string::npos)
          if (auto address = parseIp(text.substr(1, close - 1))) next.addresses.insert(*address);
        continue;
      }
      auto [host, port] = splitHostPort(text);
      if (auto address = parseIp(host)) next.addresses.insert(*address);
      else if (validHost(host)) names.insert(host);
    }
    std::string failures;
    for (const auto& name : names) {
      try {
        auto addresses = co_await timer.timeoutAfter(config.timeoutMs * kj::MILLISECONDS, lookup(name));
        clusterNames[name] = addresses;
      } catch (const kj::Exception& error) {
        // A name that does not resolve now keeps the addresses it had: the set never shrinks on a failure.
        failures += (failures.empty() ? "" : "; ") + name + ": " + error.getDescription().cStr();
      }
      auto known = clusterNames.find(name);
      if (known != clusterNames.end()) next.addresses.insert(known->second.begin(), known->second.end());
    }
    cluster = std::move(next);
    refreshedSignature = signature;
    clusterRefreshedAtUnixMs = wallTimeMsForTest();
    ++clusterRefreshes;
    clusterLastError = failures;
  }

  // ---- Limits ----
  std::string limit(Counts& counts, const TenantRules* rules) {
    const uint32_t concurrency = rules && rules->concurrency ? rules->concurrency : config.concurrency;
    const uint32_t perSecond = rules && rules->requestsPerSecond ? rules->requestsPerSecond : config.requestsPerSecond;
    if (counts.inFlight >= concurrency) return "concurrency-limit";
    const double now = monotonicTimeMs();
    if (counts.tokens < 0) counts.tokens = perSecond;
    else counts.tokens = std::min<double>(perSecond, counts.tokens + (now - counts.refilledAtMs) * perSecond / 1000.0);
    counts.refilledAtMs = now;
    if (counts.tokens < 1) return "rate-limit";
    counts.tokens -= 1;
    return "";
  }

  // ---- The policy ----
  // "" when every address may be reached on this port; else the first rule that refuses. Each address is judged as
  // the IPv4 addresses it carries (judged()), each of which must pass: the tenant's deny first, then the built-in
  // classes, then allowInternal for a private network, and allow or the default policy for a public address --
  // allowInternal is the administrator's permission for internal destinations only. A rule written as an address or
  // CIDR matches the address as given or as judged; a name or wildcard rule matches a request naming a host by that
  // name, and an IP-literal URL by the addresses the rule stands for (`named`, ruleAddresses()).
  std::string decide(const TenantRules* rules, const Destination& d, const std::vector<Address>& addresses,
      const std::map<const Rule*, std::vector<Address>>& named = {}) const {
    static const TenantRules NONE;
    const auto& r = rules ? *rules : NONE;
    auto matches = [&](const Rule& rule, const Address& given, const Address& address) {
      if (rule.port != 0 && rule.port != d.port) return false;
      if (rule.kind == Rule::Kind::RANGE) return rule.addresses.contains(given) || rule.addresses.contains(address);
      if (!d.literal) return rule.matchesName(d.host, d.port);
      auto found = named.find(&rule);
      return found != named.end() &&
          std::any_of(found->second.begin(), found->second.end(), [&](const Address& a) { return a == given || a == address; });
    };
    auto any = [&](const std::vector<Rule>& list, const Address& given, const Address& address) {
      return std::any_of(list.begin(), list.end(), [&](const Rule& rule) { return matches(rule, given, address); });
    };
    for (const auto& given : addresses) {
      if (cluster.contains(given)) return "cluster";
      for (const auto& address : judged(given)) {
        if (any(r.deny, given, address)) return "tenant-deny";
        const auto builtIn = classify(address, cluster);
        if (builtIn == "private-network") {
          if (any(r.allowInternal, given, address)) continue;
          return builtIn;
        }
        if (!builtIn.empty()) return builtIn;
        if (any(r.allow, given, address)) continue;
        if (config.defaultPolicy == "public") {
          if (std::find(config.publicPorts.begin(), config.publicPorts.end(), d.port) != config.publicPorts.end()) continue;
          return "port";
        }
        return "default-deny";
      }
    }
    return "";
  }

  // For an IP-literal URL, the addresses each name and wildcard rule of the tenant stands for: a name's own resolution
  // (one that does not resolve stands for none), and for a wildcard the addresses of the names below it that this node
  // resolved within egress.dnsCacheMs -- a domain's names cannot be listed, only the ones seen.
  kj::Promise<std::map<const Rule*, std::vector<Address>>> ruleAddresses(const TenantRules& rules, kj::TimePoint deadline) {
    std::map<const Rule*, std::vector<Address>> out;
    std::vector<const Rule*> names;
    const double now = monotonicTimeMs();
    for (const auto* list : {&rules.deny, &rules.allow, &rules.allowInternal}) {
      for (const auto& rule : *list) {
        if (rule.kind == Rule::Kind::NAME) names.push_back(&rule);
        if (rule.kind != Rule::Kind::SUFFIX) continue;
        auto& addresses = out[&rule];
        for (const auto& [host, entry] : dnsCache)
          if (entry.second > now && host.size() > rule.name.size() && host.ends_with(rule.name))
            addresses.insert(addresses.end(), entry.first.begin(), entry.first.end());
      }
    }
    kj::Vector<kj::Promise<void>> lookups;
    for (const auto* rule : names) {
      lookups.add(timer.timeoutAt(deadline, resolve(rule->name)).then(
          [&out, rule](std::vector<Address> addresses) { out[rule] = std::move(addresses); }, [](kj::Exception&&) {}));
    }
    co_await kj::joinPromises(lookups.releaseAsArray());
    co_return out;
  }

  // ---- One exchange ----
  struct Exchange {
    std::string tenant = "-", workerId, versionId, processId = "-", method, scheme, host, address, result, rule;
    uint16_t port = 0;
    uint32_t status = 0;
    uint64_t requestBytes = 0, responseBytes = 0;
    double startedAt = monotonicTimeMs();
    bool websocket = false;
  };

  void log(Exchange& ex, Counts* counts) {
    const bool limitRefusal = ex.result == "denied" && (ex.rule == "concurrency-limit" || ex.rule == "rate-limit");
    uint64_t suppressed = 0;
    if (counts && limitRefusal) {
      const double now = monotonicTimeMs();
      if (now - counts->lastLimitLineMs < LIMIT_LINE_INTERVAL_MS) { ++counts->suppressedLines; return; }
      counts->lastLimitLineMs = now;
      suppressed = counts->suppressedLines;
      counts->suppressedLines = 0;
    }
    std::ostringstream line;
    line << std::setprecision(6) << "[egress] {\"serverId\":\"" << jsonEscape(serverId) << "\",\"tenant\":\"" << jsonEscape(ex.tenant)
         << "\",\"workerId\":\"" << jsonEscape(ex.workerId) << "\",\"versionId\":\"" << jsonEscape(ex.versionId)
         << "\",\"processId\":\"" << jsonEscape(ex.processId) << "\",\"method\":\"" << jsonEscape(ex.method)
         << "\",\"scheme\":\"" << jsonEscape(ex.scheme) << "\",\"host\":\"" << jsonEscape(ex.host.substr(0, MAX_HOST_BYTES))
         << "\",\"port\":" << ex.port << ",\"address\":\"" << jsonEscape(ex.address) << "\",\"result\":\"" << ex.result
         << "\",\"rule\":" << (ex.rule.empty() ? std::string("null") : "\"" + jsonEscape(ex.rule) + "\"")
         << ",\"status\":" << ex.status << ",\"websocket\":" << (ex.websocket ? "true" : "false")
         << ",\"requestBytes\":" << ex.requestBytes << ",\"responseBytes\":" << ex.responseBytes
         << ",\"durationMs\":" << (monotonicTimeMs() - ex.startedAt);
    if (limitRefusal) line << ",\"suppressedSinceLastLine\":" << suppressed;
    line << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}";
    std::cout << line.str() << std::endl;
  }

  kj::Promise<void> refuse(kj::HttpService::Response& response, Exchange& ex, Counts* counts, const char* code,
      std::string reason, uint32_t status) {
    const bool denied = std::string(code) == "EGRESS_DENIED";
    ex.result = denied ? "denied" : "failed";
    ex.rule = reason;
    ex.status = status;
    if (counts) {
      ++(denied ? counts->deniedTotal : counts->failedTotal);
      ++(denied ? counts->denied : counts->failed)[reason];
    }
    ++(denied ? totalDenied : totalFailed);
    log(ex, counts);
    kj::HttpHeaders headers(headerTable);
    headers.add(kj::str(ERROR_HEADER), kj::str(code, ": ", reason.c_str()));
    auto body = response.send(status, denied ? "Forbidden" : "Bad Gateway", headers, uint64_t(0));
    co_return;
  }

  // Copies `size` bytes or until the end, counting what was written; each read waits at most timeoutMs.
  kj::Promise<void> pump(kj::AsyncInputStream& from, kj::AsyncOutputStream& to, uint64_t& counted, Counts& counts, bool request,
      const std::string& tenant) {
    auto buffer = kj::heapArray<kj::byte>(PUMP_BUFFER_BYTES);
    while (true) {
      const size_t read = co_await timer.timeoutAfter(config.timeoutMs * kj::MILLISECONDS,
          from.tryRead(buffer.begin(), 1, buffer.size()));
      if (read == 0) co_return;
      co_await to.write(buffer.first(read));
      counted += read;
      (request ? counts.requestBytes : counts.responseBytes) += read;
      (request ? totalRequestBytes : totalResponseBytes) += read;
      if (metered) metered(tenant, read);
    }
  }

  kj::Promise<void> pumpMessages(kj::WebSocket& from, kj::WebSocket& to, uint64_t& counted, Counts& counts, bool request,
      const std::string& tenant) {
    while (true) {
      auto message = co_await from.receive();
      KJ_SWITCH_ONEOF(message) {
        KJ_CASE_ONEOF(text, kj::String) {
          co_await to.send(text);
          counted += text.size();
          (request ? counts.requestBytes : counts.responseBytes) += text.size();
          (request ? totalRequestBytes : totalResponseBytes) += text.size();
          if (metered) metered(tenant, text.size());
        }
        KJ_CASE_ONEOF(bytes, kj::Array<kj::byte>) {
          co_await to.send(bytes);
          counted += bytes.size();
          (request ? counts.requestBytes : counts.responseBytes) += bytes.size();
          (request ? totalRequestBytes : totalResponseBytes) += bytes.size();
          if (metered) metered(tenant, bytes.size());
        }
        KJ_CASE_ONEOF(close, kj::WebSocket::Close) {
          co_await to.close(close.code, close.reason);
          co_return;
        }
      }
    }
  }

  kj::Promise<void> handle(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody, kj::HttpService::Response& response) {
    Exchange ex;
    ex.method = kj::str(method).cStr();
    ++totalRequests;
    // Who sent it: the runtime's header, with the token of a live process.
    std::optional<std::string> sourceHeader;
    bool repeated = false;
    headers.forEach([&](kj::StringPtr name, kj::StringPtr value) {
      if (!sameText(name, SOURCE_HEADER)) return;
      if (sourceHeader) repeated = true; else sourceHeader = std::string(value.cStr());
    });
    if (sourceHeader && !repeated) {
      const auto space = sourceHeader->find(' ');
      const auto token = sourceHeader->substr(0, space);
      if (auto process = source(token); process && space != std::string::npos) {
        try {
          capnp::MallocMessageBuilder message;
          auto json = message.initRoot<capnp::JsonValue>();
          const auto text = sourceHeader->substr(space + 1);
          capnp::JsonCodec().decodeRaw(kj::arrayPtr(text.data(), text.size()), json);
          if (json.isObject()) for (auto field : json.getObject()) {
            if (!field.getValue().isString()) continue;
            const std::string name = field.getName().cStr(), value = field.getValue().getString().cStr();
            if (name == "workerId") ex.workerId = value;
            else if (name == "versionId") ex.versionId = value;
          }
        } catch (const kj::Exception&) {}
        // The Worker's full name: its tenant is whose limits, rules and usage this request counts against.
        if (workerFullName(ex.workerId) && safeName(ex.versionId)) {
          ex.processId = process->processId + ":" + std::to_string(process->generation);
          ex.tenant = tenantOf(ex.workerId);
        }
      }
    }
    if (ex.tenant == "-") {
      ++unknownSources;
      co_return co_await refuse(response, ex, nullptr, "EGRESS_DENIED", "unknown-source", 403);
    }
    auto& counts = tenantCounts[ex.tenant];
    ++counts.requests;
    const TenantRules* rules = nullptr;
    if (auto found = tenants.find(ex.tenant); found != tenants.end()) rules = &found->second;
    // The cluster's addresses are known (and follow membership) before anything is checked.
    co_await readyPromise.addBranch();
    co_await fresh();
    auto parsed = parseDestination(url.cStr());
    if (auto* rule = std::get_if<std::string>(&parsed)) {
      // The host the URL names, never its user information, path or query.
      ex.host = hostOf(url.cStr());
      co_return co_await refuse(response, ex, &counts, "EGRESS_DENIED", *rule, 403);
    }
    auto d = std::get<Destination>(kj::mv(parsed));
    ex.scheme = d.scheme;
    ex.host = d.host;
    ex.port = d.port;
    ex.websocket = headers.isWebSocket();
    if (auto refused = limit(counts, rules); !refused.empty())
      co_return co_await refuse(response, ex, &counts, "EGRESS_DENIED", refused, 429);
    ++counts.inFlight;
    counts.maxInFlight = std::max(counts.maxInFlight, counts.inFlight);
    KJ_DEFER(--counts.inFlight);
    // A rule on the name refuses before anything is resolved.
    if (rules && !d.literal) {
      for (const auto& rule : rules->deny)
        if (rule.matchesName(d.host, d.port)) co_return co_await refuse(response, ex, &counts, "EGRESS_DENIED", "tenant-deny", 403);
    }
    const auto deadline = timer.now() + config.timeoutMs * kj::MILLISECONDS;
    std::vector<Address> addresses;
    if (d.literal) addresses = {*d.literal};
    else {
      std::string failed;
      try {
        addresses = co_await timer.timeoutAt(deadline, resolve(d.host));
      } catch (const kj::Exception& error) {
        failed = error.getType() == kj::Exception::Type::OVERLOADED ? "timeout" : "dns";
      }
      if (!failed.empty()) co_return co_await refuse(response, ex, &counts, "EGRESS_FAILED", failed, 502);
    }
    // An IP-literal URL meets the tenant's name and wildcard rules through the addresses they stand for.
    std::map<const Rule*, std::vector<Address>> named;
    if (rules && d.literal) named = co_await ruleAddresses(*rules, deadline);
    // Every address the name has now is checked; the connection goes only to these.
    if (auto refused = decide(rules, d, addresses, named); !refused.empty()) {
      ex.address = addresses.front().text();
      co_return co_await refuse(response, ex, &counts, "EGRESS_DENIED", refused, 403);
    }
    kj::Own<kj::AsyncIoStream> stream;
    std::string failure;
    for (const auto& address : addresses) {
      sockaddr_storage storage{};
      uint length;
      if (address.v6) {
        auto* in = reinterpret_cast<sockaddr_in6*>(&storage);
        in->sin6_family = AF_INET6;
        in->sin6_port = htons(d.port);
        memcpy(&in->sin6_addr, address.bytes.data(), 16);
        length = sizeof(sockaddr_in6);
      } else {
        auto* in = reinterpret_cast<sockaddr_in*>(&storage);
        in->sin_family = AF_INET;
        in->sin_port = htons(d.port);
        memcpy(&in->sin_addr, address.bytes.data(), 4);
        length = sizeof(sockaddr_in);
      }
      try {
        auto target = network.getSockaddr(&storage, length);
        stream = co_await timer.timeoutAt(deadline, target->connect());
        ex.address = address.text();
        break;
      } catch (const kj::Exception& error) {
        failure = error.getType() == kj::Exception::Type::OVERLOADED ? "timeout" : "connect";
        if (failure == "timeout") break;
      }
    }
    if (!stream) {
      ex.address = addresses.front().text();
      co_return co_await refuse(response, ex, &counts, "EGRESS_FAILED", failure.empty() ? "connect" : failure, 502);
    }
    if (d.tls) {
      std::string failed;
      try {
        stream = co_await timer.timeoutAt(deadline, tls->wrapClient(kj::mv(stream), kj::StringPtr(d.host.c_str())));
      } catch (const kj::Exception& error) {
        failed = error.getType() == kj::Exception::Type::OVERLOADED ? "timeout" : "tls";
      }
      if (!failed.empty()) co_return co_await refuse(response, ex, &counts, "EGRESS_FAILED", failed, 502);
    }
    // The request as the tenant wrote it, less what belongs to the connection and this node's own header.
    kj::HttpHeaders upstreamHeaders(headerTable);
    headers.forEach([&](kj::StringPtr name, kj::StringPtr value) {
      if (connectionHeader(name) || startsWithText(name, "x-coworkerd-egress")) return;
      if (ex.websocket && startsWithText(name, "sec-websocket-") && !sameText(name, "sec-websocket-protocol")) return;
      upstreamHeaders.add(kj::str(name), kj::str(value));
    });
    upstreamHeaders.set(kj::HttpHeaderId::HOST, kj::str(d.authority.c_str()));
    kj::HttpClientSettings settings;
    settings.entropySource = entropy;
    auto client = kj::newHttpClient(headerTable, *stream, settings);
    if (ex.websocket) {
      kj::HttpClient::WebSocketResponse opened;
      std::string failed;
      try {
        opened = co_await timer.timeoutAt(deadline, client->openWebSocket(d.target.c_str(), upstreamHeaders));
      } catch (const kj::Exception& error) {
        failed = error.getType() == kj::Exception::Type::OVERLOADED ? "timeout" : "protocol";
      }
      if (!failed.empty()) co_return co_await refuse(response, ex, &counts, "EGRESS_FAILED", failed, 502);
      ex.status = opened.statusCode;
      kj::HttpHeaders downstreamHeaders(headerTable);
      opened.headers->forEach([&](kj::StringPtr name, kj::StringPtr value) {
        if (connectionHeader(name) || startsWithText(name, "x-coworkerd-egress") ||
            (startsWithText(name, "sec-websocket-") && !sameText(name, "sec-websocket-protocol"))) return;
        downstreamHeaders.add(kj::str(name), kj::str(value));
      });
      KJ_SWITCH_ONEOF(opened.webSocketOrBody) {
        KJ_CASE_ONEOF(upstream, kj::Own<kj::WebSocket>) {
          ++counts.allowed;
          ++totalAllowed;
          auto downstream = response.acceptWebSocket(downstreamHeaders);
          ex.result = "allowed";
          try {
            co_await kj::joinPromisesFailFast(kj::arr(
                pumpMessages(*downstream, *upstream, ex.requestBytes, counts, true, ex.tenant),
                pumpMessages(*upstream, *downstream, ex.responseBytes, counts, false, ex.tenant)));
          } catch (const kj::Exception&) {
            ex.result = "allowed";
            ex.rule = "closed";
          }
          log(ex, &counts);
          co_return;
        }
        KJ_CASE_ONEOF(body, kj::Own<kj::AsyncInputStream>) {
          ++counts.allowed;
          ++totalAllowed;
          ex.result = "allowed";
          auto out = response.send(opened.statusCode, opened.statusText, downstreamHeaders, body->tryGetLength());
          try {
            co_await pump(*body, *out, ex.responseBytes, counts, false, ex.tenant);
          } catch (const kj::Exception&) {
            ex.rule = "closed";
          }
          log(ex, &counts);
          co_return;
        }
      }
      KJ_UNREACHABLE;
    }
    auto expected = requestBody.tryGetLength();
    auto call = client->request(method, d.target.c_str(), upstreamHeaders, expected);
    // The request body goes out while the response is awaited (a destination may answer before it read it all).
    auto sent = pump(requestBody, *call.body, ex.requestBytes, counts, true, ex.tenant)
        .then([body = kj::mv(call.body)]() mutable { body = nullptr; }).fork();
    kj::HttpClient::Response upstream;
    std::string failed;
    try {
      upstream = co_await timer.timeoutAt(deadline, kj::mv(call.response));
    } catch (const kj::Exception& error) {
      failed = error.getType() == kj::Exception::Type::OVERLOADED ? "timeout" : "protocol";
    }
    if (!failed.empty()) co_return co_await refuse(response, ex, &counts, "EGRESS_FAILED", failed, 502);
    ex.status = upstream.statusCode;
    ex.result = "allowed";
    ++counts.allowed;
    ++totalAllowed;
    kj::HttpHeaders downstreamHeaders(headerTable);
    upstream.headers->forEach([&](kj::StringPtr name, kj::StringPtr value) {
      if (connectionHeader(name) || startsWithText(name, "x-coworkerd-egress")) return;
      downstreamHeaders.add(kj::str(name), kj::str(value));
    });
    // A HEAD response keeps the length the destination announced (KJ sends it, and no body).
    kj::Maybe<uint64_t> length = upstream.body->tryGetLength();
    if (method == kj::HttpMethod::HEAD) {
      length = uint64_t(0);
      KJ_IF_SOME(announced, upstream.headers->get(kj::HttpHeaderId::CONTENT_LENGTH)) {
        KJ_IF_SOME(value, announced.tryParseAs<uint64_t>()) length = value;
      }
    }
    auto out = response.send(upstream.statusCode, upstream.statusText, downstreamHeaders, length);
    try {
      co_await pump(*upstream.body, *out, ex.responseBytes, counts, false, ex.tenant);
      co_await sent.addBranch();
    } catch (const kj::Exception&) {
      ex.rule = "closed";
    }
    log(ex, &counts);
  }

  std::string json() const {
    std::ostringstream out;
    out << "{\"mode\":\"" << config.mode << "\",\"defaultPolicy\":\"" << config.defaultPolicy << "\",\"publicPorts\":[";
    for (size_t i = 0; i < config.publicPorts.size(); ++i) out << (i ? "," : "") << config.publicPorts[i];
    out << "],\"timeoutMs\":" << config.timeoutMs << ",\"concurrency\":" << config.concurrency
        << ",\"requestsPerSecond\":" << config.requestsPerSecond << ",\"dnsCacheMs\":" << config.dnsCacheMs
        << ",\"caFile\":\"" << jsonEscape(config.caFile) << "\",\"trustedCertificates\":" << certificates.size()
        << ",\"systemTrustStore\":" << (config.systemTrustStore ? "true" : "false") << ",\"tenantPolicies\":[";
    size_t index = 0;
    for (const auto& [tenant, rules] : tenants) {
      out << (index++ ? "," : "") << "{\"tenant\":\"" << jsonEscape(tenant) << "\",\"allow\":" << rules.allow.size()
          << ",\"deny\":" << rules.deny.size() << ",\"allowInternal\":" << rules.allowInternal.size()
          << ",\"concurrency\":" << rules.concurrency << ",\"requestsPerSecond\":" << rules.requestsPerSecond << "}";
    }
    out << "],\"cluster\":{\"refreshes\":" << clusterRefreshes << ",\"refreshedAtUnixMs\":" << clusterRefreshedAtUnixMs
        << ",\"names\":" << clusterNames.size() << ",\"ranges\":" << cluster.ranges.size() << ",\"addresses\":[";
    index = 0;
    for (const auto& address : cluster.addresses) {
      if (index == 64) { out << ",\"...\""; break; }
      out << (index++ ? "," : "") << "\"" << address.text() << "\"";
    }
    out << "],\"lastError\":\"" << jsonEscape(clusterLastError.substr(0, 500)) << "\"},\"dns\":{\"lookups\":" << dnsLookups
        << ",\"cacheHits\":" << dnsHits << ",\"failures\":" << dnsFailures << ",\"entries\":" << dnsCache.size()
        << "},\"totals\":{\"requests\":" << totalRequests << ",\"allowed\":" << totalAllowed << ",\"denied\":" << totalDenied
        << ",\"failed\":" << totalFailed << ",\"unknownSources\":" << unknownSources << ",\"requestBytes\":" << totalRequestBytes
        << ",\"responseBytes\":" << totalResponseBytes << ",\"egressBytes\":" << totalRequestBytes + totalResponseBytes
        << "},\"tenants\":{";
    index = 0;
    for (const auto& [tenant, c] : tenantCounts) {
      out << (index++ ? "," : "") << "\"" << jsonEscape(tenant) << "\":{\"requests\":" << c.requests << ",\"allowed\":" << c.allowed
          << ",\"denied\":" << countsJson(c.denied) << ",\"failed\":" << countsJson(c.failed) << ",\"inFlight\":" << c.inFlight
          << ",\"maxInFlight\":" << c.maxInFlight << ",\"requestBytes\":" << c.requestBytes << ",\"responseBytes\":" << c.responseBytes
          << ",\"egressBytes\":" << c.requestBytes + c.responseBytes << ",\"suppressedLines\":" << c.suppressedLines << "}";
    }
    out << "}}";
    return out.str();
  }

  const EgressConfig config;
  const std::string serverId;
  kj::Network& network;
  kj::Timer& timer;
  const kj::HttpHeaderTable& headerTable;
  std::function<std::vector<std::string>()> clusterEndpoints;
  std::function<std::optional<EgressSource>(const std::string&)> source;
  std::function<void(const std::string&, uint64_t)> metered;
  std::map<std::string, TenantRules> tenants;
  std::vector<kj::TlsCertificate> certificates;
  kj::Own<kj::TlsContext> tls;
  RandomBytes entropy;
  Cluster staticCluster, cluster;
  std::string refreshedSignature;
  kj::Maybe<kj::ForkedPromise<void>> refreshing;
  bool refreshSettled = false;
  std::vector<std::string> configuredNames;
  std::map<std::string, std::vector<Address>> clusterNames;
  uint64_t clusterRefreshes = 0, clusterRefreshedAtUnixMs = 0;
  std::string clusterLastError;
  kj::ForkedPromise<void> readyPromise = nullptr;
  kj::Own<kj::PromiseFulfiller<void>> readyFulfiller;
  std::map<std::string, std::pair<std::vector<Address>, double>> dnsCache;
  uint64_t dnsLookups = 0, dnsHits = 0, dnsFailures = 0;
  std::map<std::string, Counts> tenantCounts;
  uint64_t totalRequests = 0, totalAllowed = 0, totalDenied = 0, totalFailed = 0, unknownSources = 0;
  uint64_t totalRequestBytes = 0, totalResponseBytes = 0;
};

EgressService::EgressService(const EgressConfig& config, std::string serverId, kj::Network& network, kj::Timer& timer,
    const kj::HttpHeaderTable& headerTable, std::function<std::vector<std::string>()> clusterEndpoints,
    std::function<std::optional<EgressSource>(const std::string&)> source,
    std::function<void(const std::string& tenant, uint64_t bytes)> metered)
    : impl(kj::heap<Impl>(config, std::move(serverId), network, timer, headerTable, std::move(clusterEndpoints), std::move(source),
          std::move(metered))) {}
EgressService::~EgressService() noexcept(false) {}

kj::Promise<void> EgressService::request(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& headers,
    kj::AsyncInputStream& requestBody, Response& response) {
  return impl->handle(method, url, headers, requestBody, response);
}

kj::Promise<void> EgressService::run() {
  while (true) {
    co_await impl->refreshCluster();
    if (impl->readyFulfiller->isWaiting()) {
      impl->readyFulfiller->fulfill();
      std::cout << "[egress] {\"serverId\":\"" << jsonEscape(impl->serverId) << "\",\"event\":\"ready\",\"clusterAddresses\":"
                << impl->cluster.addresses.size() << ",\"clusterRanges\":" << impl->cluster.ranges.size()
                << ",\"defaultPolicy\":\"" << impl->config.defaultPolicy << "\"}" << std::endl;
    }
    co_await impl->timer.afterDelay(CLUSTER_REFRESH_MS * kj::MILLISECONDS);
  }
}

std::string EgressService::json() const { return impl->json(); }

uint64_t EgressService::egressBytes(const std::string& tenant) const {
  auto found = impl->tenantCounts.find(tenant);
  return found == impl->tenantCounts.end() ? 0 : found->second.requestBytes + found->second.responseBytes;
}

void EgressService::check(const EgressConfig& config) {
  for (const auto& [tenant, policy] : config.tenants) {
    parseRules(policy.allow, "allow");
    parseRules(policy.deny, "deny");
    parseRules(policy.allowInternal, "allowInternal");
  }
  for (const auto& text : config.clusterAddresses) {
    if (parseRange(text)) continue;
    auto [host, port] = Impl::splitHostPort(text);
    KJ_REQUIRE(validHost(host), "invalid egress.clusterAddresses entry (HOST[:PORT], ADDRESS or CIDR)", text.c_str());
  }
}

}  // namespace coworkerd
