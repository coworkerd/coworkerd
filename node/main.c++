#include "coworkerd.capnp.h"
#include "config.h"
#include "log-cleaning.h"
#include "log-sink.h"
#include "support.h"
#include "membership.h"
#include "object-state.h"
#include "deployment.h"
#include "desired-state.h"
#include "object-store.h"
#include "process-pool.h"
#include "async-log.h"
#include "memory-watermark.h"
#include "egress.h"
#include "json.h"
#include "peer-auth.h"
#include "tenancy.h"
#include "tenant-manifest.h"
#include "tenants.h"
#include "usage.h"

#include <capnp/compat/byte-stream.h>
#include <capnp/compat/http-over-capnp.h>
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>
#include <kj/compat/http.h>
#include <openssl/sha.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <queue>
#include <optional>
#include <random>
#include <unistd.h>
#include <sstream>
#include <cerrno>
#include <cstring>
#include <strings.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>


namespace rpc = coworkerd::v1::rpc;
namespace fs = std::filesystem;

namespace coworkerd {
// `coworkerd check storage ENDPOINT BUCKET` (storage-probe.c++): the object store's self-check.
int runStorageCheck(int argc, char** argv);
}  // namespace coworkerd

namespace {

// The configuration, the helpers and the cleaning both daemons share (common/).
using coworkerd::ServerConfig;
using coworkerd::SecretRedactions;
using coworkerd::bearerRefusal;
using coworkerd::cleanReason;
using coworkerd::clockOffsetMsForTest;
using coworkerd::executableArgument;
using coworkerd::executableDirectory;
using coworkerd::jsonEscape;
using coworkerd::launcherSha256;
using coworkerd::monotonicTimeMs;
using coworkerd::readFile;
using coworkerd::redactSecrets;
using coworkerd::runLogClock;
using coworkerd::safeName;
using coworkerd::secretRedactions;
using coworkerd::sendUnauthorized;
using coworkerd::sha256Hex;
using coworkerd::trialClean;
using coworkerd::versionJson;
using coworkerd::versionText;
using coworkerd::wallTimeMsForTest;

class AsyncLogCallback final: public kj::ExceptionCallback {
public:
  void logMessage(kj::LogSeverity severity, const char* file, int line, int depth, kj::String&& text) override {
    // Match KJ's original stderr format, through the complete-line sink. An exception KJ logs may
    // carry what a Worker threw: secret values hidden, and one line (no forged line from a
    // newline in a Worker's message). Unlike a reason it keeps its length and paths: a diagnostic.
    auto message = redactSecrets(kj::str(kj::repeat('_', depth), file, ":", line, ": ", severity, ": ", text).cStr());
    for (auto& c : message) if (static_cast<unsigned char>(c) < 0x20 && c != '\t') c = ' ';
    std::cerr << message << '\n';
  }
};

bool validSha256(const std::string& value) {
  return value.size() == SHA256_DIGEST_LENGTH * 2 &&
      std::all_of(value.begin(), value.end(), [](unsigned char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
  });
}

// The path of a request to the source socket, without the scheme and authority and without its query (requestPath()
// below is the same for the request targets a Binding call carries; this one is needed before it).
std::string sourceRequestPath(kj::StringPtr rawUrl) {
  std::string path = std::string(rawUrl.cStr()).substr(0, std::string(rawUrl.cStr()).find('?'));
  auto scheme = path.find("://");
  if (scheme != std::string::npos) {
    auto pathStart = path.find('/', scheme + 3);
    path = pathStart == std::string::npos ? "/" : path.substr(pathStart);
  }
  return path;
}
std::string queryParameter(kj::StringPtr rawUrl, std::string_view name, bool required = true,
    std::string_view expectedPath = "/v1/source") {
  std::string url = rawUrl.cStr();
  auto queryStart = url.find('?');
  KJ_REQUIRE(queryStart != std::string::npos, "source request query is required");
  const auto path = sourceRequestPath(rawUrl);
  KJ_REQUIRE(path == expectedPath, "unknown source path", path.c_str());

  auto query = std::string_view(url).substr(queryStart + 1);
  while (!query.empty()) {
    auto separator = query.find('&');
    auto part = query.substr(0, separator);
    auto equals = part.find('=');
    if (equals != std::string_view::npos && part.substr(0, equals) == name) {
      // Percent-decoded: the runtime's URLSearchParams writes a full name's '/' as %2F.
      std::string value;
      const auto raw = part.substr(equals + 1);
      for (size_t i = 0; i < raw.size(); ++i) {
        auto hex = [](char c) { return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : c >= 'A' && c <= 'F' ? c - 'A' + 10 : -1; };
        if (raw[i] == '%' && i + 2 < raw.size() && hex(raw[i + 1]) >= 0 && hex(raw[i + 2]) >= 0) {
          value += static_cast<char>(hex(raw[i + 1]) * 16 + hex(raw[i + 2]));
          i += 2;
        } else {
          value += raw[i];
        }
      }
      return value;
    }
    if (separator == std::string_view::npos) break;
    query.remove_prefix(separator + 1);
  }
  if (!required) return {};
  KJ_FAIL_REQUIRE("missing source query parameter", std::string(name).c_str());
}

uint64_t environmentUnsigned(
    const char* name,
    uint64_t defaultValue,
    uint64_t maximum) {
  const char* raw = std::getenv(name);
  if (raw == nullptr) return defaultValue;
  std::string value(raw);
  KJ_REQUIRE(!value.empty(), "environment value is empty", name);
  size_t parsed = 0;
  uint64_t result = 0;
  try {
    result = std::stoull(value, &parsed, 10);
  } catch (const std::exception&) {
    KJ_FAIL_REQUIRE("environment value is not an unsigned integer", name, value.c_str());
  }
  KJ_REQUIRE(parsed == value.size(), "environment value has trailing characters", name);
  KJ_REQUIRE(result <= maximum, "environment value is too large", name, result, maximum);
  return result;
}

enum class LeaseMode {
  NAIVE,
  CORRECT,
};

struct LeaseConfig {
  LeaseMode mode;
  uint16_t rootBudget;
  uint16_t grantCap;
  uint64_t ttlMs;

  // The naive lease is a test-only counterexample: only the explicit command-line switch
  // --experimental-naive-lease selects it. COWORKERD_LEASE_MODE may still say "correct".
  static LeaseConfig fromEnvironment(bool experimentalNaive) {
    const char* rawMode = std::getenv("COWORKERD_LEASE_MODE");
    std::string mode = rawMode == nullptr ? "correct" : rawMode;
    if (mode == "naive") {
      KJ_FAIL_REQUIRE("COWORKERD_LEASE_MODE=naive is no longer accepted: the naive lease is a test-only counterexample, "
          "selected by the command-line switch --experimental-naive-lease alone");
    }
    KJ_REQUIRE(mode == "correct", "COWORKERD_LEASE_MODE must be correct", mode.c_str());
    auto ttlMs = environmentUnsigned(
        "COWORKERD_LEASE_TTL_MS", 30'000, 86'400'000);
    KJ_REQUIRE(ttlMs > 0, "COWORKERD_LEASE_TTL_MS must be positive");
    return {
      experimentalNaive ? LeaseMode::NAIVE : LeaseMode::CORRECT,
      static_cast<uint16_t>(environmentUnsigned(
          "COWORKERD_SUBREQUEST_BUDGET", 8, 65'535)),
      static_cast<uint16_t>(environmentUnsigned(
          "COWORKERD_SUBREQUEST_GRANT", 1, 65'535)),
      ttlMs,
    };
  }

  bool naive() const {
    return mode == LeaseMode::NAIVE;
  }
};

double remainingLeaseMs(double expiresAtMonotonicMs) {
  const auto remaining = expiresAtMonotonicMs - monotonicTimeMs();
  return remaining > 0 ? remaining : 0;
}

using coworkerd::VersionDefinition;
using coworkerd::VersionPin;
using coworkerd::VersionPins;
using coworkerd::SharedVersionPins;

struct PeerDefinition {
  std::string serverId;
  std::string address;
};

struct PlacementDefinition {
  std::string workerId;
  std::string serverId;
};

using coworkerd::OpenEvent;
struct ProcessUnavailable { bool quarantine = false; };

// Capture request stages without writing a log line at each measurement point.
// All timestamps use the Server's monotonic clock; output is flushed as one line.
struct PathTiming {
  std::string kind, target, invocationId, treeId;
  double started = monotonicTimeMs();
  std::vector<std::pair<std::string, double>> stages;
  bool emitted = false;
  explicit PathTiming(std::string kind): kind(std::move(kind)) { mark("received"); }
  void mark(std::string stage) { stages.emplace_back(std::move(stage), monotonicTimeMs()); }
  void emit() {
    if (emitted) return;
    emitted = true;
    std::ostringstream out;
    out << std::setprecision(17) << "[path-time] {\"kind\":\"" << kind
        << "\",\"target\":\"" << jsonEscape(target) << "\",\"invocationId\":\"" << invocationId
        << "\",\"treeId\":\"" << treeId << "\",\"startedMonotonicMicros\":" << started * 1000
        << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << ",\"stages\":[";
    for (size_t i = 0; i < stages.size(); ++i) {
      if (i) out << ',';
      out << "{\"stage\":\"" << stages[i].first << "\",\"elapsedMicros\":" << (stages[i].second - started) * 1000 << '}';
    }
    out << "]}";
    std::cout << out.str() << std::endl;
  }
};
using SharedPathTiming = std::shared_ptr<PathTiming>;
SharedPathTiming pathTiming(std::string kind) {
  return std::getenv("COWORKERD_PATH_TIMING") ? std::make_shared<PathTiming>(std::move(kind)) : nullptr;
}
void pathMark(const SharedPathTiming& timing, const char* stage) { if (timing) timing->mark(stage); }

struct ExecutionRecord {
  uint64_t sequence;
  std::string processId;
  uint64_t processGeneration;
  std::string treeId;
  std::string invocationId;
  std::string target;
  std::string kind;
  std::string parentInvocationId;
  uint64_t callSequence;
  uint64_t orderSequence;
  uint16_t grant;
  uint16_t remainingAfter;
  uint16_t depth;
  double expiresAtMonotonicMs;
  std::string ingressServerId;
  bool coldStart = false;
};

struct DebitRecord {
  uint64_t sequence;
  std::string treeId;
  std::string parentInvocationId;
  std::string target;
  std::string targetServerId;
  uint16_t grant;
  uint16_t charged;
  uint16_t remainingAfter;
  uint16_t depth;
  double expiresAtMonotonicMs;
};

struct InvocationLeaseState {
  std::string treeId;
  uint16_t remainingSubrequests;
  uint16_t grant;
  uint16_t depth;
  double expiresAtMonotonicMs;
  SharedVersionPins pins;
  std::string ingressServerId;
  // A WebSocket connection's lease (ServerCore::renewLease): the deadline the runtime was given, which its Binding calls
  // present, once the lease was renewed past it (0: never renewed), and whether the open connection holds the lease, so
  // that its expiry refuses the calls of its handlers until the next message renews it but does not remove it.
  double presentedExpiresAtMonotonicMs = 0;
  bool held = false;
  bool presents(double value) const { return value == (presentedExpiresAtMonotonicMs > 0 ? presentedExpiresAtMonotonicMs : expiresAtMonotonicMs); }
};

struct ChildAllocation {
  std::string treeId;
  std::string parentInvocationId;
  uint64_t callSequence;
  uint64_t orderSequence;
  uint16_t grant;
  uint16_t charged;
  uint16_t remainingAfter;
  uint16_t depth;
  double expiresAtMonotonicMs;
  SharedVersionPins pins;
  bool ingressRoot = false;
  std::string ingressServerId;
  SharedPathTiming timing;
  std::string entrypoint;
};

// A request to a Durable Object (docs/compatibility.md, section 7): the object's namespace, id and class, and how many
// Durable Object calls led to it (0 from a stateless Worker; a call from an object adds one to that object's).
struct DurableTarget {
  std::string namespaceId, objectId, className;
  uint16_t hops = 0;
  std::string key() const { return namespaceId + "/" + objectId; }
};
// More Durable Object calls than this in a row are refused (DO_HOP_LIMIT).
constexpr uint16_t MAX_DURABLE_HOPS = 8;

struct LeaseExpiry {
  double expiresAtMonotonicMs;
  std::string invocationId;

  bool operator>(const LeaseExpiry& other) const {
    if (expiresAtMonotonicMs != other.expiresAtMonotonicMs) {
      return expiresAtMonotonicMs > other.expiresAtMonotonicMs;
    }
    return invocationId > other.invocationId;
  }
};

struct InvocationRejection {
  uint status = 503;
  std::string code;
  std::string reason;
  std::string loadBand;
  uint32_t inFlight = 0;
  uint32_t limit = 0;
  uint32_t avoidMs = 0;
  std::string category;  // LOAD_FAILED: package-unavailable, evaluation, limit, secret-missing

  std::string json() const {
    std::ostringstream out;
    out << "{\"code\":\"" << jsonEscape(code) << "\",\"reason\":\"" << jsonEscape(cleanReason(reason))
        << "\",\"loadBand\":\"" << loadBand << "\",\"inFlight\":" << inFlight
        << ",\"limit\":" << limit << ",\"avoidMs\":" << avoidMs << ",\"status\":" << status;
    if (!category.empty()) out << ",\"category\":\"" << jsonEscape(category) << "\"";
    out << "}";
    return out.str();
  }
};

// A load failure is the version's own and the same everywhere -- except a code package
// this Server could not get or verify, which another Server (another cache) may have.
constexpr const char* PACKAGE_UNAVAILABLE = "package-unavailable";
bool rerouteAfter(const InvocationRejection& refusal) {
  return refusal.code != "RESULT_UNCERTAIN" && (refusal.code != "LOAD_FAILED" || refusal.category == PACKAGE_UNAVAILABLE);
}

class ServerCore;
struct AdmissionSlot {
  std::function<void()> release;
  std::shared_ptr<coworkerd::ProcessCall> process;
  SharedPathTiming timing;
  std::string entrypoint;
  // An ingress root's (here or forwarded from another ingress), and that ingress: its response may become a WebSocket
  // connection (AdmissionHttpService), which gives the slot back at the 101.
  ServerCore* server = nullptr;
  bool root = false;
  std::string ingressNodeId;
  // A request to a Durable Object (DurableDispatch): its response may become a WebSocket connection too, and its outcome
  // -- the response's status, or the code of a failure before it -- goes to `outcome` once.
  bool durable = false;
  std::function<void(uint status, const std::string& code)> outcome;
  void report(uint status, const std::string& code) { if (outcome) { auto callback = std::move(outcome); outcome = {}; callback(status, code); } }
  explicit AdmissionSlot(std::function<void()> release): release(std::move(release)) {}
  ~AdmissionSlot() { finish(); report(0, ""); }
  void finish() { if (process) process->finish(); if (release) { auto callback = std::move(release); release = {}; callback(); } }
};

rpc::EventDispatcher::Client trackAdmission(rpc::EventDispatcher::Client dispatcher,
    std::shared_ptr<AdmissionSlot> slot, capnp::HttpOverCapnpFactory& factory, const kj::HttpHeaderTable& headers);

// The outcome of the load check before a dispatch (ServerCore::loadCheck).
struct LoadCheck {
  std::optional<InvocationRejection> refusal;
  uint64_t sequence = 0;    // reserved for the call the check loaded for (0: no check ran)
  bool coldLoaded = false;  // the check loaded the version for that call
};

// The trial load's verdict (defined with it below) and the cleaning of its message (trialClean,
// log-cleaning.h); the load check before a dispatch classifies and cleans with them.
struct TrialVerdict {
  std::string category;  // empty: the version loads
  std::string limit;     // "cpu" or "memory" for LIMIT_EXCEEDED
  std::string message;
};
TrialVerdict classifyTrialFailure(const std::string& stage, const std::string& name, const std::string& raw,
    bool cpuExceeded, bool memoryExceeded, uint32_t cpuLimitMs, uint32_t memoryLimitMiB);

void phaseTiming(const char* stage, double started, const std::shared_ptr<coworkerd::ProcessCall>& call = {});

// ---- Inbound WebSocket connections (docs/compatibility.md, section 7) ----
// A connection is a request whose Worker answered 101. It has a relay on the node whose ingress holds the client's
// connection and one on the node that executes the Worker -- back to back when that is the same node --, each passing
// messages and close frames on as they come (WebSocketRelay, below). The registry holds this node's relays: the drain and
// a retiring process generation tell theirs to go away (1001), it counts the ingress's connections per tenant against
// ingress.webSocketsPerTenant, and it keeps the counts the state endpoint shows.
class WebSocketRegistry final {
public:
  enum class Role { INGRESS, EXECUTION };
  static const char* roleName(Role role) { return role == Role::INGRESS ? "ingress" : "execution"; }
  class Relay {
  public:
    virtual void goAway(const std::string& cause) = 0;
    Role role = Role::INGRESS;
    std::string tenant, processId;
    uint64_t generation = 0;
  protected:
    ~Relay() = default;
  };
  // What a relay counted when it ended: the close code its outer side ended with (1006: no close frame), who closed
  // first, why the node ended it, its messages.
  struct Ended {
    uint16_t code = 1006;
    std::string direction, cause;
    uint64_t toWorker = 0, toClient = 0;
  };
  explicit WebSocketRegistry(uint32_t perTenant): perTenant(perTenant) {}
  uint32_t limitPerTenant() const { return perTenant; }
  // An upgrade at the ingress takes one of its tenant's places before it is sent on, and gives it back when its request
  // ends -- a refused upgrade at once, a connection when it closes --, so that upgrades in flight count as well. False:
  // the tenant holds as many places on this node as the limit allows.
  bool reserve(const std::string& tenant) {
    if (perTenant != 0 && placesOf(tenant) >= perTenant) {
      ++refusedByTenant[tenant];
      return false;
    }
    ++places[tenant];
    return true;
  }
  void release(const std::string& tenant) {
    auto found = places.find(tenant);
    KJ_ASSERT(found != places.end() && found->second > 0, "WebSocket place underflow");
    if (--found->second == 0) places.erase(found);
  }
  uint32_t placesOf(const std::string& tenant) const {
    auto found = places.find(tenant);
    return found == places.end() ? 0 : found->second;
  }
  void opened(Relay& relay) {
    relays.insert(&relay);
    ++open[index(relay.role)][relay.tenant];
    ++openedTotal[index(relay.role)];
  }
  void closed(Relay& relay, const Ended& ended) {
    relays.erase(&relay);
    auto& tenants = open[index(relay.role)];
    if (auto found = tenants.find(relay.tenant); found != tenants.end() && --found->second == 0) tenants.erase(found);
    auto& totals = closedTotals[index(relay.role)];
    ++totals.codes[std::to_string(ended.code)];
    if (!ended.cause.empty()) ++totals.causes[ended.cause];
    totals.toWorker += ended.toWorker;
    totals.toClient += ended.toClient;
  }
  // Every relay here (the drain), or those executing on one process generation (its rotation), told to go away.
  uint32_t goAway(const std::string& cause) { return goAwayWhere(cause, [](const Relay&) { return true; }); }
  uint32_t goAwayGeneration(const std::string& processId, uint64_t generation, const std::string& cause) {
    return goAwayWhere(cause, [&](const Relay& relay) {
      return relay.role == Role::EXECUTION && relay.processId == processId && relay.generation == generation;
    });
  }
  size_t openCount() const { return relays.size(); }
  size_t openCount(Role role) const {
    return static_cast<size_t>(std::count_if(relays.begin(), relays.end(), [role](const Relay* relay) { return relay->role == role; }));
  }
  std::string json() const {
    auto counts = [](const std::map<std::string, uint64_t>& values) {
      std::string out = "{";
      for (const auto& [key, count] : values) out += (out.size() > 1 ? "," : "") + ("\"" + jsonEscape(key) + "\":") + std::to_string(count);
      return out + "}";
    };
    std::string out = "{\"limitPerTenant\":" + std::to_string(perTenant) + ",\"open\":" + std::to_string(relays.size());
    for (auto role : {Role::INGRESS, Role::EXECUTION}) {
      const auto i = index(role);
      uint64_t total = 0;
      for (const auto& [tenant, count] : open[i]) total += count;
      out += ",\"" + std::string(roleName(role)) + "\":{\"open\":" + std::to_string(total) + ",\"tenants\":" + counts(open[i]) +
          ",\"opened\":" + std::to_string(openedTotal[i]) + ",\"closeCodes\":" + counts(closedTotals[i].codes) +
          ",\"endedBy\":" + counts(closedTotals[i].causes) + ",\"messages\":{\"toWorker\":" + std::to_string(closedTotals[i].toWorker) +
          ",\"toClient\":" + std::to_string(closedTotals[i].toClient) + "}}";
    }
    return out + ",\"places\":" + counts({places.begin(), places.end()}) + ",\"refused\":" + counts(refusedByTenant) +
        ",\"toldToGoAway\":" + counts(goneAway) + "}";
  }

private:
  static size_t index(Role role) { return role == Role::INGRESS ? 0 : 1; }
  template <typename Which>
  uint32_t goAwayWhere(const std::string& cause, Which which) {
    std::vector<Relay*> chosen;
    for (auto* relay : relays) if (which(*relay)) chosen.push_back(relay);
    for (auto* relay : chosen) relay->goAway(cause);
    goneAway[cause] += chosen.size();
    return static_cast<uint32_t>(chosen.size());
  }
  struct Totals { std::map<std::string, uint64_t> codes, causes; uint64_t toWorker = 0, toClient = 0; };
  const uint32_t perTenant;
  std::set<Relay*> relays;
  std::map<std::string, uint32_t> places;
  std::array<std::map<std::string, uint64_t>, 2> open;
  std::array<uint64_t, 2> openedTotal{};
  std::array<Totals, 2> closedTotals;
  std::map<std::string, uint64_t> refusedByTenant, goneAway;
};

class ServerCore final {
public:
  static constexpr size_t MAX_EXECUTION_RECORDS = 1024;
  static constexpr uint64_t LEASE_REAPER_INTERVAL_MS = 100;

  ServerCore(
      coworkerd::DesiredState& desiredState,
      coworkerd::ProcessPool& runtime,
      coworkerd::MemoryWatermark& memory,
      LeaseConfig leaseConfig,
      ServerConfig serverConfig,
      coworkerd::Membership& membership,
      capnp::HttpOverCapnpFactory& factory, const kj::HttpHeaderTable& headers, kj::Timer& timer)
      : desiredState(desiredState),
        runtime(runtime),
        memory(memory),
        leaseConfig(leaseConfig),
        serverConfig(std::move(serverConfig)),
        membership(membership), factory(factory), headers(headers), timer(timer) {
    memory.setTestBand(this->serverConfig.loadBandForTest);
    // Every cleaning hides the values of the secrets this Server's deployment view holds.
    auto& redactions = secretRedactions();
    redactions.stamp = [this]() {
      const auto& state = this->desiredState.state();
      return std::pair<uint64_t, uint64_t>(state.revision(), state.generation());
    };
    redactions.values = [this]() {
      std::vector<std::string> values;
      for (const auto& [worker, names] : this->desiredState.state().secrets())
        for (const auto& [name, secret] : names) if (!secret.value.empty()) values.push_back(secret.value);
      return values;
    };
    if (leaseConfig.naive()) {
      std::cout << "[server] {\"event\":\"experimental-naive-lease\",\"warning\":\"the naive lease is a test-only counterexample: "
                   "it does not conserve the subrequest budget across Servers\"}" << std::endl;
    }
    std::cout << "[server] lease mode=" << (leaseConfig.naive() ? "naive" : "correct")
              << " root-budget=" << leaseConfig.rootBudget
              << " grant-cap=" << leaseConfig.grantCap
              << " ttl-ms=" << leaseConfig.ttlMs
              << " node-id=" << this->serverConfig.serverId
              << " placements=" << this->serverConfig.placements.size()
              << " membership=" << membership.view().backend
              << " epoch=" << membership.view().epoch
              << " load-check=" << (this->serverConfig.loadCheck ? "on" : "off") << std::endl;
  }
  ~ServerCore() noexcept(false) {
    auto& redactions = secretRedactions();
    redactions.stamp = nullptr;
    redactions.values = nullptr;
  }

  // ---- Tenants: their records (being deleted, or gone: not served) and their usage here ----
  void setTenancy(coworkerd::TenantView* view, coworkerd::TenantUsage* counters) { tenants = view; usage = counters; }
  bool tenantDeleting(const std::string& tenant) const { return tenants && tenants->deleting(tenant); }
  // Why a tenant's Workers start nothing here ("" when they may): its record says deleting, or there is none.
  std::string tenantUnavailable(const std::string& tenant) const { return tenants ? tenants->unavailable(tenant) : std::string(); }
  // Each tenant this node knows of: its record's status and log quota, its Workers here (valid of all), whether it
  // is a suspect and which quarantine process is its shard, and the limits its Workers name (their own, else the
  // tenant's default, else this node's).
  // What one Worker's executions are limited to, as the control plane stored it: the Worker's own
  // value, else its tenant's default, and 0 for each limit neither names -- this node's own value applies
  // there. The processes apply exactly this: it goes in the invocation descriptor, the runtime bridge
  // puts it in the loader key, and the isolate is created under it.
  coworkerd::WorkerLimits namedLimits(const std::string& workerId) const {
    coworkerd::WorkerLimits named;
    const auto* record = tenants ? tenants->tenant(coworkerd::tenantOf(workerId)) : nullptr;
    if (record) named = record->defaultLimits;
    const auto* worker = desiredState.state().worker(workerId);
    if (worker) {
      if (worker->limits.cpuMs) named.cpuMs = worker->limits.cpuMs;
      if (worker->limits.memoryMiB) named.memoryMiB = worker->limits.memoryMiB;
    }
    return named;
  }
  // The largest memory limit any Worker here names, for the process-wide WebAssembly memory cap.
  uint32_t maxNamedMemoryMiB() const {
    uint32_t largest = 0;
    for (const auto& [id, worker] : desiredState.state().workers()) largest = std::max(largest, namedLimits(id).memoryMiB);
    return largest;
  }
  std::string tenantsJson() const {
    struct Summary { size_t workers = 0, valid = 0; std::vector<std::string> limits; };
    std::map<std::string, Summary> summary;
    for (const auto& [id, worker] : desiredState.state().workers()) {
      auto& entry = summary[coworkerd::tenantOf(id)];
      ++entry.workers;
      if (worker.valid) ++entry.valid;
      const auto* record = tenants ? tenants->tenant(coworkerd::tenantOf(id)) : nullptr;
      const auto named = namedLimits(id);
      coworkerd::WorkerLimits effective{runtime.cpuLimitMs(), runtime.memoryLimitMiB()};
      std::string source = "node";
      if (record && record->defaultLimits.cpuMs) { effective.cpuMs = record->defaultLimits.cpuMs; source = "tenant"; }
      if (record && record->defaultLimits.memoryMiB) { effective.memoryMiB = record->defaultLimits.memoryMiB; source = "tenant"; }
      if (worker.limits.cpuMs) { effective.cpuMs = worker.limits.cpuMs; source = "worker"; }
      if (worker.limits.memoryMiB) { effective.memoryMiB = worker.limits.memoryMiB; source = "worker"; }
      // `applied` is what the processes run this Worker under, which is what it names: the pool
      // resolves the same way (ProcessPool::effectiveLimits), and a value neither the Worker nor its tenant
      // names is this node's.
      const auto applied = runtime.effectiveLimits(id);
      if (source != "node" && entry.limits.size() < 256)
        entry.limits.push_back("{\"workerId\":\"" + jsonEscape(id) + "\",\"named\":" + coworkerd::workerLimitsJson(effective) +
            ",\"source\":\"" + source + "\",\"applied\":" + coworkerd::workerLimitsJson(applied) +
            ",\"namedByWorkerOrTenant\":" + coworkerd::workerLimitsJson(named) + "}");
    }
    std::string out = "{\"view\":" + (tenants ? tenants->json() : std::string("null")) + ",\"tenants\":{";
    size_t index = 0;
    for (const auto& [tenant, entry] : summary) {
      const bool suspect = runtime.quarantined(tenant + "/-");
      out += std::string(index++ ? "," : "") + "\"" + jsonEscape(tenant) + "\":{\"workers\":" + std::to_string(entry.workers) +
          ",\"validWorkers\":" + std::to_string(entry.valid) + ",\"deleting\":" + (tenantDeleting(tenant) ? "true" : "false") +
          ",\"suspect\":" + (suspect ? "true" : "false") + ",\"quarantineShard\":\"" + runtime.quarantineShard(tenant) + "\"" +
          ",\"workerLimits\":[";
      for (size_t i = 0; i < entry.limits.size(); ++i) out += (i ? "," : "") + entry.limits[i];
      out += "]}";
    }
    return out + "}}";
  }
  std::string usageJson() const { return usage ? usage->json() : "null"; }
  // The last minute's usage, written while this node is still allowed to (the drain's step `usage-flushed`).
  kj::Promise<std::string> flushUsage() {
    if (usage == nullptr) return kj::Promise<std::string>(std::string("null"));
    return usage->flushNow();
  }

  // ---- The effective configuration and the management token ----
  void setConfiguration(std::string json, std::string launchJson) { configuration = std::move(json); launch = std::move(launchJson); }
  const std::string& configurationJson() const { return configuration; }
  bool managementTokenSet() const { return !serverConfig.managementToken.empty(); }
  // With a token set, every /__coworkerd/ path but ready and probe takes `Authorization: Bearer <token>`.
  // Empty when the request may proceed, else what was wrong: "missing", "malformed" or "wrong".
  std::string managementRefusal(kj::StringPtr url, const kj::HttpHeaders& headers) const {
    if (serverConfig.managementToken.empty()) return {};
    std::string_view path(url.cStr());
    if (auto query = path.find('?'); query != std::string_view::npos) path = path.substr(0, query);
    if (!path.starts_with("/__coworkerd/") || path == "/__coworkerd/ready" || path == "/__coworkerd/probe") return {};
    return bearerRefusal(serverConfig.managementToken, headers);
  }
  // One line per refused request: listener, method, path (no query), what was wrong; never a token.
  void managementRefused(const char* listener, kj::HttpMethod method, kj::StringPtr url, const std::string& kind) {
    ++managementUnauthorized[kind];
    std::string path = url.cStr();
    if (auto query = path.find('?'); query != std::string::npos) path.resize(query);
    if (path.size() > 200) path.resize(200);
    std::cout << "[management] {\"nodeId\":\"" << jsonEscape(serverConfig.serverId) << "\",\"event\":\"unauthorized\",\"listener\":\""
              << listener << "\",\"method\":\"" << kj::str(method).cStr() << "\",\"path\":\"" << jsonEscape(path)
              << "\",\"authorization\":\"" << kind << "\",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
  }
  std::string managementJson() const {
    return "{\"tokenSet\":" + std::string(managementTokenSet() ? "true" : "false") + ",\"unauthorized\":" + countsJson(managementUnauthorized) + "}";
  }

  // The route and the root's closure come from one revision of the state; a hostname registered to a tenant is
  // routed among that tenant's routes only.
  coworkerd::DeploymentState::Selection route(const std::string& host, const std::string& path) const {
    const auto* owner = tenants ? tenants->owner(coworkerd::requestHost(host)) : nullptr;
    auto selection = desiredState.state().route(host, path, owner);
    if (selection.kind != coworkerd::DeploymentState::Selection::Kind::SELECTED) ++routeRejections[selection.kind];
    return selection;
  }

  std::shared_ptr<const VersionDefinition> exact(const std::string& key, const std::string& hash = {}) const {
    return desiredState.exact(key, hash);
  }

  bool perCallForTest() const {
    return desiredState.config().versionResolutionForTest == "per-call";
  }

  kj::Promise<std::shared_ptr<const VersionDefinition>> availableVersion(
      const std::string& workerId, const SharedVersionPins& pins, std::string& reason, double deadline) {
    VersionPin pin;
    if (perCallForTest()) {
      // Control mode: each executing Server resolves its own current pointer.
      const auto* worker = desiredState.state().worker(workerId);
      if (!worker || !worker->valid) { reason = "定义未知：Worker absent from version pins"; co_return nullptr; }
      pin = coworkerd::versionPin(*worker->version);
    } else {
      auto found = pins->find(workerId);
      if (found == pins->end()) { reason = "定义未知：Worker absent from version pins"; co_return nullptr; }
      pin = found->second;
    }
    co_return co_await desiredState.resolve(std::move(pin), deadline - monotonicTimeMs(), reason);
  }

  bool pinMatches(const SharedVersionPins& pins, const VersionPin& value) const {
    auto found = pins->find(value.workerId);
    return found != pins->end() && found->second == value;
  }

  kj::Promise<std::string> ensureCode(const VersionDefinition& version, double deadline) {
    return desiredState.ensureCode(version, deadline - monotonicTimeMs());
  }

  // The executing Server gives a version its secrets from its own deployment
  // view, whatever the caller saw: without a usable value for every declared
  // name nothing executes (explicit, retryable elsewhere). Names only.
  std::optional<InvocationRejection> secretRefusal(const VersionDefinition& version, bool root) {
    auto missing = desiredState.state().missingSecrets(version);
    if (missing.empty()) return std::nullopt;
    std::string reason = missing.size() == 1 ? "missing secret on this Server: " : "missing secrets on this Server: ";
    for (size_t i = 0; i < missing.size(); ++i) reason += (i ? ", " : "") + missing[i];
    reason += " (Worker " + version.workerId + ")";
    ++secretUnavailableRejections;
    lastRejection = reason;
    return refusal("SECRET_UNAVAILABLE", std::move(reason), 503, root);
  }

  // Static assets: answered by the asset layer without workerd, a lease or
  // placement, for the ingress, Service Bindings to a Worker with assets and the
  // ASSETS Binding.
  enum class AssetSource { INGRESS, BINDING, ASSETS_BINDING };
  struct AssetCounters {
    uint64_t requests = 0, hits = 0, misses = 0, fallbacks = 0, notModified = 0, bytes = 0,
        toWorker = 0, notFound = 0, methodNotAllowed = 0, unavailable = 0,
        runWorkerFirst = 0, excluded = 0, navigationFallbacks = 0;
  };
  AssetCounters& assetCounters(const std::string& workerId, AssetSource source) {
    return assetStats[workerId][static_cast<size_t>(source)];
  }
  kj::Promise<coworkerd::SharedAssetIndex> assetIndex(const VersionDefinition& version, double deadline, std::string& reason) {
    return desiredState.assets(version, deadline - monotonicTimeMs(), reason);
  }
  // A version by its exact identity (the caller's own, for an ASSETS Binding call).
  kj::Promise<std::shared_ptr<const VersionDefinition>> resolveVersion(VersionPin pin, double deadline, std::string& reason) {
    return desiredState.resolve(std::move(pin), deadline - monotonicTimeMs(), reason);
  }
  // The version pins of a live parent lease. An ASSETS Binding call runs no child
  // invocation: it takes no subrequest budget, but the lease it was granted under
  // must still be live, as for any Binding call.
  // `deadline`: the lease's own (a WebSocket connection's lease is renewed past the one presented).
  SharedVersionPins livePins(const std::string& treeId, const std::string& parentInvocationId,
      double presentedExpiresAtMonotonicMs, double* deadline = nullptr) {
    const auto nowMonotonicMs = monotonicTimeMs();
    expireLeases(nowMonotonicMs);
    auto parent = leases.find(parentInvocationId);
    if (parent == leases.end() || parent->second.expiresAtMonotonicMs <= nowMonotonicMs ||
        !parent->second.presents(presentedExpiresAtMonotonicMs)) {
      ++expiredLeaseRejections;
      lastRejection = "lease expired before execution for parent=" + parentInvocationId + " tree=" + treeId +
          " (ASSETS Binding) nowMonotonicMs=" + std::to_string(nowMonotonicMs) +
          " presentedExpiresAtMonotonicMs=" + std::to_string(presentedExpiresAtMonotonicMs);
      std::cerr << "[server] reject " << cleanReason(lastRejection) << std::endl;
      KJ_FAIL_REQUIRE(lastRejection.c_str());
    }
    if (deadline) *deadline = parent->second.expiresAtMonotonicMs;
    return parent->second.pins;
  }
  kj::Promise<coworkerd::AssetRead> assetBytes(const coworkerd::CodeAsset& asset, double deadline) {
    return desiredState.assetBytes(asset, deadline - monotonicTimeMs());
  }
  double rootDeadline() const { return monotonicTimeMs() + leaseConfig.ttlMs; }

  const ServerConfig& placementConfig() const {
    return serverConfig;
  }

  const coworkerd::Member* member(const std::string& serverId) const {
    const auto& members = membership.view().members;
    auto found = members.find(serverId);
    return found == members.end() ? nullptr : &found->second;
  }

  const std::string& epoch() const { return membership.view().epoch; }

  bool holdsEpoch(const std::string& candidate) const {
    return membership.view().heldEpochs.contains(candidate);
  }

  std::string invocationId(uint64_t sequence) const {
    return serverConfig.serverId + "." + membership.view().epoch +
        "-invocation-" + std::to_string(sequence);
  }

  std::string targetServer(const std::string& workerId) const {
    return serverConfig.targetServer(workerId);
  }

  bool shouldForward(const std::string& workerId, bool alreadyPlaced) const {
    return !alreadyPlaced && targetServer(workerId) != serverConfig.serverId;
  }

  bool hot(const std::string& hash) const {
    auto found = desiredState.byHash(hash);
    return found && runtime.hot(found->workerId, hash, serverConfig.idleMs);
  }

  std::vector<std::string> candidates(const std::string& workerId) const {
    std::vector<std::string> ids;
    bool self = false;
    // A member that announced its exit takes no new work. This Server, once draining,
    // ranks after every other candidate: only the trees it still runs place work here, and
    // only when no other Server takes it.
    // (objectStore) So does this Server once its epoch expired by its own clock: the others drop it; it still serves
    // the calls at its ingress when no other member takes them, and the calls from the others addressed to an epoch
    // it held.
    const bool lastResort = this->lastResort();
    for (const auto& [id, member] : membership.view().members) {
      if (id == serverConfig.serverId && lastResort) self = true;
      else if (id == serverConfig.serverId || member.state != "draining") ids.push_back(id);
    }
    auto order = coworkerd::rendezvousOrder(workerId, ids);
    if (self) order.push_back(serverConfig.serverId);
    return order;
  }

  // This Server places work on itself only when no other member takes it: draining, or its epoch expired by its own
  // clock (candidates() ranks it last for the same reason).
  bool lastResort() const { return drain.draining || membership.view().selfExpired; }
  std::string processPlacement(const std::vector<std::string>& workers) { return runtime.placementJson(workers); }
  // Ready to take work: the ordinary processes (a quarantine shard restarting after its tenant's crash takes nothing from
  // the others; its state is in the state endpoint's processes and readiness).
  bool processesReady() const { return runtime.ready(); }
  // Process rotation: queue slots (all when empty) and report progress.
  std::string requestRotation(const std::vector<std::string>& processes) { return runtime.requestRotation(processes, "manual"); }
  std::string rotationJson(const std::vector<uint64_t>& requests) const { return runtime.rotationJson(requests); }

  // ---- Inbound WebSocket connections ----
  WebSocketRegistry& webSockets() { return webSocketRegistry; }
  kj::Timer& clock() { return timer; }
  // A connection's lease, on the node that executes it: held while the connection is open, and renewed at each message
  // toward the Worker -- an event of the call tree, as workerd tops up the request's CPU for it -- as a new root's would
  // be: the deadline becomes now plus the lease time, and the budget of subrequests starts again from the root's own
  // (its grant), so that the message's Binding calls are neither refused as expired however long the connection has
  // lasted nor limited by the calls of earlier messages. The budget is conserved per message: its Binding calls and
  // their own share it, a child never getting more than its parent has left. Its Binding calls present the deadline the
  // runtime was given at the start (presents()).
  void holdLease(const std::string& invocationId) {
    auto lease = leases.find(invocationId);
    if (lease != leases.end()) lease->second.held = true;
  }
  void renewLease(const std::string& invocationId) {
    auto lease = leases.find(invocationId);
    if (lease == leases.end()) return;
    auto& state = lease->second;
    state.remainingSubrequests = state.grant;
    const auto renewed = monotonicTimeMs() + static_cast<double>(leaseConfig.ttlMs);
    if (renewed <= state.expiresAtMonotonicMs) return;
    if (state.presentedExpiresAtMonotonicMs == 0) state.presentedExpiresAtMonotonicMs = state.expiresAtMonotonicMs;
    state.expiresAtMonotonicMs = renewed;
    leaseExpiries.push({renewed, invocationId});
    ++renewedLeases;
  }
  // The connection ended: its lease expires as any other from here on.
  void releaseLease(const std::string& invocationId) {
    auto lease = leases.find(invocationId);
    if (lease == leases.end()) return;
    lease->second.held = false;
    leaseExpiries.push({lease->second.expiresAtMonotonicMs, invocationId});
  }

  // ---- A Worker's log lines: 256 KB an event ----
  // workerd caps a request's trace at 256 KB, but gives a streaming tail every line (runtime.js, TenantLog); the node
  // keeps the cap, per event: a request, or a message of a WebSocket connection toward the Worker (nextLogEvent, from
  // the connection's relay). An object's events are the object's: workerd records an object's lines under its latest
  // request, whichever of its requests or connections wrote them, so a request to an object (startObjectLogEvent) and a
  // message on any of its connections start the object's next event. The lines of an event past it are cut, one
  // warning line in their place (SourceService::log). A line that names no invocation counts against its post's own
  // budget.
  static constexpr uint64_t LOG_EVENT_BYTES = 256 * 1024;
  struct LogBudget { uint64_t bytes = 0; bool warned = false; double lastAt = 0; };
  // The budget an invocation's lines count against: its object's, or its own.
  std::string logEventKey(const std::string& invocationId) const {
    auto object = logObjects.find(invocationId);
    return object == logObjects.end() ? invocationId : object->second.first;
  }
  void startObjectLogEvent(const std::string& invocationId, const std::string& objectKey) {
    pruneLogEvents();
    const auto now = monotonicTimeMs();
    logObjects[invocationId] = {"object:" + objectKey, now};
    logEvents["object:" + objectKey] = {0, false, now};
  }
  void nextLogEvent(const std::string& invocationId) {
    if (auto found = logEvents.find(logEventKey(invocationId)); found != logEvents.end()) found->second = {0, false, monotonicTimeMs()};
  }
  // Events quiet for ten minutes are over: their invocations ended long ago, or lines no longer come.
  void pruneLogEvents() {
    if (logEvents.size() < 65536 && logObjects.size() < 65536) return;
    const auto now = monotonicTimeMs();
    std::erase_if(logEvents, [&](const auto& item) { return now - item.second.lastAt > 600000; });
    std::erase_if(logObjects, [&](const auto& item) { return now - item.second.second > 600000; });
  }
  // Whether a line of `bytes` fits in its event's budget; `warn` is set for the first line of an event that does not.
  bool chargeLogLine(const std::string& invocationId, uint64_t bytes, LogBudget& own, bool& warn) {
    LogBudget* budget = &own;
    if (!invocationId.empty()) {
      pruneLogEvents();
      const auto now = monotonicTimeMs();
      if (auto object = logObjects.find(invocationId); object != logObjects.end()) object->second.second = now;
      budget = &logEvents[logEventKey(invocationId)];
      budget->lastAt = now;
    }
    warn = false;
    if (budget->bytes + bytes <= LOG_EVENT_BYTES) {
      budget->bytes += bytes;
      return true;
    }
    if (!budget->warned) {
      budget->warned = warn = true;
      ++logEventsOverBudget;
    }
    return false;
  }
  uint64_t logEventsOverBudgetCount() const { return logEventsOverBudget; }

  // ---- The Server's exit ----
  // Once draining: new roots at the ingress and every invocation arriving from another
  // Server are refused before execution (SERVER_DRAINING, retryable elsewhere); what is in
  // flight continues. The DrainController (run()) drives the steps and keeps the record here.
  struct DrainStep { std::string step; uint64_t atUnixMs; };
  struct DrainRecord {
    bool draining = false, finished = false;
    std::string trigger, phase = "serving", outcome, emergency;
    double startedAt = 0;
    uint64_t startedAtUnixMs = 0, drainedAtUnixMs = 0, processesClosedAtUnixMs = 0, exitAtUnixMs = 0, emergencyAtUnixMs = 0;
    std::string inFlightAtStart = "null", inFlightAtDeadline = "null", inFlightAtEmergency = "null", progress = "null";
    std::string close = "null", usage = "null";
    uint32_t killedCalls = 0, killedProcesses = 0, emergencyKilledCalls = 0, emergencyKilledProcesses = 0;
    uint64_t killedContexts = 0, emergencyKilledContexts = 0, cutIngress = 0;
    // The WebSocket connections at the start, all told to go away (1001) then: those whose client this node's ingress
    // held, and those it executed (a connection of this node's own ingress executed here is in both).
    uint32_t webSocketsIngress = 0, webSocketsExecution = 0, webSocketsToldToGoAway = 0;
    std::map<std::string, uint64_t> refused;
    int exitCode = -1;
    uint64_t deploymentRevisionAtStart = 0, deploymentGenerationAtStart = 0;
    std::vector<DrainStep> steps;
  };
  bool draining() const { return drain.draining; }
  DrainRecord& drainRecord() { return drain; }
  uint32_t drainTimeoutMs() const { return serverConfig.drainTimeoutMs; }
  void onDrainRequested(std::function<void(std::string)> callback) { drainRequested = std::move(callback); }
  // POST /__coworkerd/drain: the same as the first SIGTERM.
  void requestDrain(const std::string& trigger) { if (drainRequested) drainRequested(trigger); }
  uint32_t ingressInFlight() const { return ingressRequests; }
  uint32_t admittedInFlight() const { return inFlightRoots + inFlightNested; }
  void ingressBegin() { ++ingressRequests; }
  void ingressEnd() { KJ_ASSERT(ingressRequests > 0, "ingress count underflow"); --ingressRequests; }
  std::string inFlightJson(const std::string& processes) const {
    return "{\"ingress\":" + std::to_string(ingressRequests) + ",\"proxyingIngress\":" + std::to_string(proxyingIngress) +
        ",\"roots\":" + std::to_string(inFlightRoots) + ",\"nested\":" + std::to_string(inFlightNested) +
        ",\"processes\":" + processes + "}";
  }
  void drainStep(const std::string& step, const std::string& detail = {}) {
    const auto at = wallTimeMsForTest();
    drain.steps.push_back({step, at});
    std::cout << std::setprecision(17) << "[server-drain] {\"nodeId\":\"" << jsonEscape(serverConfig.serverId)
              << "\",\"event\":\"" << step << "\",\"phase\":\"" << drain.phase << "\",\"atUnixMs\":" << at
              << ",\"sinceStartMs\":" << (drain.startedAt > 0 ? monotonicTimeMs() - drain.startedAt : 0.0)
              << (detail.empty() ? "" : ",") << detail << "}" << std::endl;
  }
  void beginDrain(const std::string& trigger, const std::string& processes) {
    drain.draining = true;
    drain.trigger = trigger;
    drain.phase = "draining";
    drain.startedAt = monotonicTimeMs();
    drain.startedAtUnixMs = wallTimeMsForTest();
    drain.inFlightAtStart = inFlightJson(processes);
    drain.deploymentRevisionAtStart = desiredState.state().revision();
    drain.deploymentGenerationAtStart = desiredState.state().generation();
    // The WebSocket connections end now, with 1001 (going away): the clients reconnect through the platform's front, which
    // no longer sends them here; a connection forwarded here from another ingress ends there with the same code.
    drain.webSocketsIngress = static_cast<uint32_t>(webSocketRegistry.openCount(WebSocketRegistry::Role::INGRESS));
    drain.webSocketsExecution = static_cast<uint32_t>(webSocketRegistry.openCount(WebSocketRegistry::Role::EXECUTION));
    drain.webSocketsToldToGoAway = webSocketRegistry.goAway("drain");
    drainStep("announce", "\"trigger\":\"" + jsonEscape(trigger) + "\",\"timeoutMs\":" + std::to_string(serverConfig.drainTimeoutMs) +
        ",\"inFlight\":" + drain.inFlightAtStart + ",\"webSockets\":" + std::to_string(drain.webSocketsToldToGoAway));
  }
  InvocationRejection drainingRefusal(bool root) {
    auto value = refusal("SERVER_DRAINING", "the Server is draining for its exit; nothing new starts here", 503, root);
    // The Server is leaving: other Servers avoid it as long as they may (they also drop it
    // from their candidates once their member table shows it draining).
    value.avoidMs = serverConfig.avoidMaxMs;
    return value;
  }
  void countDrainRefusal(const std::string& source) { ++drain.refused[source]; }
  // At the deadline, the ingress requests still in flight are cut (the ones executing here
  // are lost with their processes first): RESULT_UNCERTAIN, counted apart.
  kj::Promise<void> ingressCutoff() { return cutoff.addBranch(); }
  void cutIngress() { if (cutoffFulfiller) cutoffFulfiller->reject(KJ_EXCEPTION(DISCONNECTED, DRAIN_CUT)); }
  static bool drainCut(const kj::Exception& error) { return error.getDescription().startsWith(DRAIN_CUT); }
  void countCutIngress() { ++drain.cutIngress; }
  // GET /__coworkerd/drain?wait=1 answers once the lease is revoked, just before the exit.
  kj::Promise<std::string> waitForDrainEnd() {
    if (drain.finished) return drainJson();
    auto paf = kj::newPromiseAndFulfiller<std::string>();
    drainWaiters.push_back(kj::mv(paf.fulfiller));
    return kj::mv(paf.promise);
  }
  void releaseDrainWaiters() {
    drain.finished = true;
    for (auto& waiter : drainWaiters) waiter->fulfill(drainJson());
    drainWaiters.clear();
  }
  uint32_t pendingDrainResponses = 0;
  void setServerBinary(std::string path, std::string sha256) { serverBinaryPath = std::move(path); serverBinarySha256 = std::move(sha256); }
  std::string serverJson() const {
    return "{\"binaryPath\":\"" + jsonEscape(serverBinaryPath) + "\",\"binarySha256\":\"" + serverBinarySha256 +
        "\",\"pid\":" + std::to_string(::getpid()) + ",\"startedAtUnixMs\":" + std::to_string(serverStartedAtUnixMs) +
        ",\"drainTimeoutMs\":" + std::to_string(serverConfig.drainTimeoutMs) + "}";
  }
  std::string drainJson() const {
    const auto& r = drain;
    std::ostringstream out;
    out << std::setprecision(17) << "{\"draining\":" << (r.draining ? "true" : "false") << ",\"phase\":\"" << r.phase
        << "\",\"trigger\":\"" << jsonEscape(r.trigger) << "\",\"timeoutMs\":" << serverConfig.drainTimeoutMs
        << ",\"startedAtUnixMs\":" << r.startedAtUnixMs << ",\"announce\":{\"status\":\"" << jsonEscape(membership.view().drainAnnounce)
        << "\",\"announcedAtUnixMs\":" << membership.view().drainAnnouncedAtUnixMs << ",\"attempts\":" << membership.view().drainAnnounceAttempts
        << "},\"outcome\":\"" << r.outcome << "\",\"drainedAtUnixMs\":" << r.drainedAtUnixMs
        << ",\"inFlightAtStart\":" << r.inFlightAtStart << ",\"inFlightNow\":" << inFlightJson(r.progress)
        << ",\"inFlightAtDeadline\":" << r.inFlightAtDeadline << ",\"killedCalls\":" << r.killedCalls
        << ",\"killedContexts\":" << r.killedContexts << ",\"killedProcesses\":" << r.killedProcesses
        << ",\"cutIngress\":" << r.cutIngress << ",\"webSockets\":{\"ingress\":" << r.webSocketsIngress
        << ",\"execution\":" << r.webSocketsExecution << ",\"toldToGoAway\":" << r.webSocketsToldToGoAway << "},\"refused\":{";
    size_t n = 0;
    for (const auto& [source, count] : r.refused) out << (n++ ? "," : "") << "\"" << source << "\":" << count;
    out << "},\"processesClosedAtUnixMs\":" << r.processesClosedAtUnixMs << ",\"close\":" << r.close << ",\"usage\":" << r.usage
        << ",\"revokeStatus\":\"" << jsonEscape(membership.view().revokeStatus) << "\",\"revokedAtUnixMs\":" << membership.view().revokedAtUnixMs
        << ",\"exitAtUnixMs\":" << r.exitAtUnixMs << ",\"exitCode\":" << r.exitCode
        << ",\"deployment\":{\"revisionAtStart\":" << r.deploymentRevisionAtStart << ",\"generationAtStart\":" << r.deploymentGenerationAtStart
        << ",\"revisionNow\":" << desiredState.state().revision() << ",\"generationNow\":" << desiredState.state().generation() << "}"
        << ",\"emergency\":";
    if (r.emergency.empty()) out << "null";
    else out << "{\"trigger\":\"" << jsonEscape(r.emergency) << "\",\"atUnixMs\":" << r.emergencyAtUnixMs
             << ",\"inFlight\":" << r.inFlightAtEmergency << ",\"killedCalls\":" << r.emergencyKilledCalls
             << ",\"killedContexts\":" << r.emergencyKilledContexts << ",\"killedProcesses\":" << r.emergencyKilledProcesses << "}";
    out << ",\"steps\":[";
    for (size_t i = 0; i < r.steps.size(); ++i)
      out << (i ? "," : "") << "{\"step\":\"" << r.steps[i].step << "\",\"atUnixMs\":" << r.steps[i].atUnixMs << "}";
    out << "]}";
    return out.str();
  }
  std::shared_ptr<coworkerd::ProcessCall> selectProcess(const std::string& worker) {
    auto started = monotonicTimeMs();
    auto call = runtime.select(worker);
    phaseTiming("process-selection", started);
    if (!call) throw ProcessUnavailable{runtime.quarantined(worker)};
    return call;
  }

  InvocationRejection refusal(std::string code, std::string reason, uint status = 503, bool root = false) {
    return {status, std::move(code), std::move(reason), memory.effectiveBand(),
        inFlightRoots + inFlightNested, root ? serverConfig.rootLimit : serverConfig.nestedLimit,
        std::min(serverConfig.rejectionAvoidMs, serverConfig.avoidMaxMs)};
  }

  void observeRefusal(const InvocationRejection& refusal, const std::string& source = "localBinding") {
    ++placementRejections[refusal.code];
    if (refusal.code == "QUARANTINE_UNAVAILABLE") decision("quarantineUnavailable", source == "localIngress" || source == "peerIngress");
    ++placementRejectionReasons[cleanReason(refusal.reason)];
    observeLocalRefusal(refusal, source);
  }

  void observeLocalRefusal(const InvocationRejection& refusal, const std::string& source) {
    observeCounter("refusal", source, refusal.code);
    std::cout << "[refusal-count] {\"nodeId\":\"" << serverConfig.serverId
              << "\",\"source\":\"" << source << "\",\"code\":\"" << refusal.code
              << "\",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
  }

  InvocationRejection processRefusal(const ProcessUnavailable& error, bool root = false) {
    return refusal(error.quarantine ? "QUARANTINE_UNAVAILABLE" : "PROCESS_UNAVAILABLE",
        error.quarantine ? "no ready quarantine process before dispatch" : "no ready workerd process before dispatch", 503, root);
  }

  std::optional<InvocationRejection> admission(const std::string& worker, const std::string& hash, bool root, double deadline,
      bool record = true, bool forwardedRoot = false, const DurableTarget* durable = nullptr) {
    auto started = monotonicTimeMs();
    auto timing = kj::defer([&] { phaseTiming("admission", started); });
    auto rejected = [&](std::string code, std::string reason, uint status = 503) {
      auto value = refusal(std::move(code), std::move(reason), status, root);
      if (record) observeRefusal(value, root ? (forwardedRoot ? "peerIngress" : "localIngress") : "localBinding");
      return std::optional<InvocationRejection>(std::move(value));
    };
    if (remainingLeaseMs(deadline) == 0) {
      if (record) rejectExpiredLease("lease expired before admission");
      return rejected("LEASE_EXPIRED", "lease expired before admission", 410);
    }
    if (inFlightRoots + inFlightNested >= (root ? serverConfig.rootLimit : serverConfig.nestedLimit))
      return rejected("OVERLOADED", "concurrency limit reached");
    const auto band = memory.effectiveBand();
    if ((!root || forwardedRoot) && (band == "red" ||
        (band == "yellow" && !(durable ? runtime.durableHot(hash) : hot(hash)))))
      return rejected("LOAD_BAND_REJECTED", band == "red" ?
          (forwardedRoot ? "red band rejects forwarded roots" : "red band rejects nested invocations") : "yellow band rejects cold version");
    if (durable) {
      if (!runtime.durableAvailable(durable->key()))
        return rejected("DO_UNAVAILABLE", "the durable process of this object (" + runtime.durableProcessFor(durable->key()) + ") is not ready");
      return std::nullopt;
    }
    if (!runtime.available(worker)) return rejected(runtime.quarantined(worker) ? "QUARANTINE_UNAVAILABLE" : "PROCESS_UNAVAILABLE",
        runtime.quarantined(worker) ? "no ready quarantine process before dispatch" : "no ready workerd process before dispatch");
    return std::nullopt;
  }

  std::shared_ptr<AdmissionSlot> reserve(bool root) {
    if (root) ++inFlightRoots; else ++inFlightNested;
    auto slot = std::make_shared<AdmissionSlot>([this, root]() {
      auto& count = root ? inFlightRoots : inFlightNested;
      KJ_ASSERT(count > 0, "admission count underflow");
      --count;
    });
    slot->server = this;
    slot->root = root;
    return slot;
  }

  void setLoadBand(std::string value) {
    KJ_REQUIRE(value == "green" || value == "yellow" || value == "red", "invalid loadBandForTest");
    serverConfig.loadBandForTest = std::move(value);
    memory.setTestBand(serverConfig.loadBandForTest);
  }

  void decision(const std::string& kind, bool ingress = false) {
    ++placementDecisions[kind];
    ++decisionsByKind[ingress ? "ingress" : "binding"][kind];
    std::cout << "[decision] {\"nodeId\":\"" << serverConfig.serverId << "\",\"kind\":\""
              << (ingress ? "ingress" : "binding") << "\",\"decision\":\"" << kind
              << "\",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
  }
  void proxyIngress(bool begin) {
    if (begin) ++proxyingIngress;
    else { KJ_ASSERT(proxyingIngress > 0); --proxyingIngress; }
  }
  void observeAttempt(const std::string& target, bool ingress, const std::optional<InvocationRejection>& failure) {
    observeCounter(ingress ? "ingressAttempt" : "bindingAttempt", target, "");
    if (failure && failure->code != "RESULT_UNCERTAIN")
      observeCounter(ingress ? "ingressRejected" : "bindingRejected", target, failure->code);
  }
  void observeStaticAttempt(const std::string& target, const std::optional<InvocationRejection>& failure) {
    observeAttempt(target, false, failure);
    std::cout << "[static-attempt] {\"nodeId\":\"" << target
              << "\",\"kind\":\"binding\",\"failure\":" << (failure ? failure->json() : "null")
              << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
  }
  bool drawHome() { return std::generate_canonical<double, 53>(random) < serverConfig.homeProbability; }
  void decisionTiming(double started) {
    auto us = (monotonicTimeMs() - started) * 1000;
    if (decisionTimes.size() == MAX_EXECUTION_RECORDS) decisionTimes.pop_front();
    decisionTimes.push_back(us);
    std::cout << "[decision-time] " << std::setprecision(17) << us << std::endl;
  }
  std::string nextPlacementId() { return invocationId(nextPlacementSequence++) + "-placement"; }
  ChildAllocation allocateRoot(SharedVersionPins pins, double deadline) {
    auto tree = serverConfig.serverId + "." + epoch() + "-tree-" + std::to_string(nextTreeSequence++);
    std::cout << "[root] {\"nodeId\":\"" << serverConfig.serverId << "\",\"treeId\":\"" << tree
              << "\",\"grant\":" << leaseConfig.rootBudget << ",\"remainingMs\":" << remainingLeaseMs(deadline)
              << ",\"expiresAtMonotonicMs\":" << std::setprecision(17) << deadline
              << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
    return {tree, {}, 0, 0, leaseConfig.rootBudget, 0, leaseConfig.rootBudget, 0, deadline,
        std::move(pins), true, serverConfig.serverId};
  }
  void reroute(const std::string& code) { ++placementReroutes[code]; }

  std::string debugPlacement(const std::vector<std::string>& workers) const {
    std::ostringstream out; out << "[";
    size_t n = 0;
    for (const auto& worker : workers) {
      auto order = candidates(worker);
      if (n++) out << ',';
      out << "{\"workerId\":\"" << jsonEscape(worker) << "\",\"home\":\""
          << (order.empty() ? "" : order.front()) << "\",\"candidates\":[";
      for (size_t i = 0; i < order.size(); ++i) { if (i) out << ','; out << "\"" << order[i] << "\""; }
      out << "]}";
    }
    out << "]"; return out.str();
  }

  std::string placementJson() const {
    std::ostringstream out; out << std::setprecision(17) << "{\"inFlight\":{\"roots\":" << inFlightRoots
        << ",\"nested\":" << inFlightNested << ",\"total\":" << inFlightRoots + inFlightNested
        << "},\"rootLimit\":" << serverConfig.rootLimit << ",\"nestedLimit\":" << serverConfig.nestedLimit
        << ",\"loadBand\":\"" << memory.effectiveBand() << "\",\"loadBandForTest\":\"" << serverConfig.loadBandForTest << "\",\"p\":" << serverConfig.homeProbability
        << ",\"idleMs\":" << serverConfig.idleMs << ",\"hotVersions\":[";
    size_t n = 0;
    for (const auto& [hash, when] : lastExecution) if (hot(hash)) {
      if (n++) out << ',';
      out << "{\"definitionHash\":\"" << hash << "\",\"lastExecutionMonotonicMs\":" << when << "}";
    }
    out << "],\"hotVersionCount\":" << n << ",\"lastExecutionSize\":" << lastExecution.size();
    auto counts = [&out](const char* key, const auto& values) {
      out << ",\"" << key << "\":{"; size_t n = 0;
      // Keys are codes, decisions and refusal reasons; a reason may hold any text (a load failure's).
      for (const auto& [k, v] : values) { if (n++) out << ','; out << "\"" << jsonEscape(k) << "\":" << v; }
      out << "}";
    };
    counts("decisions", placementDecisions); counts("reroutes", placementReroutes); counts("rejections", placementRejections); counts("rejectionsByReason", placementRejectionReasons);
    out << ",\"ingressPlacement\":" << (serverConfig.ingressPlacement ? "true" : "false")
        << ",\"proxyingIngress\":" << proxyingIngress
        << ",\"observability\":" << countersJson()
        << ",\"coldStarts\":{";
    size_t coldIndex = 0;
    for (const auto& [key, count] : coldStarts) {
      if (coldIndex++) out << ',';
      out << "\"" << jsonEscape(key) << "\":" << count;
    }
    out << "},\"decisionsByKind\":{";
    size_t kindIndex = 0;
    for (const auto& [kind, decisions] : decisionsByKind) {
      if (kindIndex++) out << ',';
      out << "\"" << kind << "\":{"; size_t decisionIndex = 0;
      for (const auto& [name, count] : decisions) {
        if (decisionIndex++) out << ',';
        out << "\"" << name << "\":" << count;
      }
      out << '}';
    }
    out << '}';
    out << ",\"decisionMicros\":[";
    for (size_t i = 0; i < decisionTimes.size(); ++i) { if (i) out << ','; out << decisionTimes[i]; }
    out << "]}"; return out.str();
  }

  // ---- The load check ----
  // Before a version's first dispatch to a workerd process -- and again once the last check there is
  // older than the placement idle period (bounded by the idle eviction period), so that an isolate
  // evicted meanwhile is checked when it loads again -- the Server loads the version there as a trial
  // load does (the runtime loads it, its bridge reports the exports, no tenant handler runs and no
  // Binding is given) and dispatches the call only if it loads and the entrypoint the call names can be
  // dispatched to. A failure is an explicit refusal before anything of the call executed, LOAD_FAILED,
  // with a category -- package-unavailable (the code package could not be fetched or verified here:
  // another Server, another cache, may have it), evaluation (syntax, imports, a top-level throw, a
  // missing entrypoint, compatibility flags), limit (CPU or memory while loading) or secret-missing --
  // and a reason cleaned as a trial's message is. The check has its own invocation id (its process
  // call, limit rows and culprit are its own, as a trial's) and fetches the code for the call it
  // precedes: that call's sequence is reserved here, and the cold start, if the check loaded the
  // version, is that call's. A process that exits during the check with the check as its culprit is
  // a limit failure; one that exits without a culprit gives RESULT_UNCERTAIN, as without the
  // check: the version may be what killed it, so the call is never tried elsewhere.
  double loadCheckFreshMs() const {
    const auto evict = serverConfig.processPool.idleEvictMs;
    return evict > 0 ? std::min<double>(evict, serverConfig.idleMs) : serverConfig.idleMs;
  }
  static std::string loadKey(const coworkerd::ProcessCall& process, const std::string& hash, const std::string& entrypoint) {
    return process.processId + ":" + std::to_string(process.generation) + ":" + hash + "#" + (entrypoint.empty() ? "default" : entrypoint);
  }
  void confirmLoaded(const coworkerd::ProcessCall& process, const std::string& hash, const std::string& entrypoint) {
    loadConfirmed[loadKey(process, hash, entrypoint)] = monotonicTimeMs();
  }
  // One load probe on a selected process: what the runtime reported, or how the probe failed.
  struct LoadProbe {
    std::string checkId, processId, report, failure, culprit, workerId;
    uint64_t generation = 0;
    bool timedOut = false, exited = false, cpuExceeded = false, memoryExceeded = false;
    double elapsedMs = 0;
  };
  kj::Promise<LoadProbe> loadProbe(const VersionDefinition& version, std::shared_ptr<coworkerd::ProcessCall> process,
      const std::string& entrypoint, double deadline, const std::string& forInvocation) {
    LoadProbe probe;
    probe.checkId = serverConfig.serverId + "." + epoch() + "-load-" + std::to_string(nextLoadCheckSequence++);
    probe.workerId = version.workerId;
    process->workerId = version.workerId; process->versionId = version.versionId;
    process->invocationId = probe.checkId; process->treeId = probe.checkId;
    probe.processId = process->processId;
    probe.generation = process->generation;
    auto descriptor = encodeDescriptor(version, probe.checkId, probe.checkId, 0, 0, 0, 0, deadline, std::make_shared<const VersionPins>(),
        *process, serverConfig.serverId, entrypoint, true, true, forInvocation);
    const auto started = monotonicTimeMs();
    auto opened = runtime.open(process, descriptor, version.definitionHash);
    // Kept until the probe ends: dropping it would cancel the dispatch.
    auto ready = kj::mv(opened.ready);
    auto remaining = [deadline]() { return static_cast<int64_t>(std::max(0.0, deadline - monotonicTimeMs()) * 1000) * kj::MICROSECONDS; };
    try {
      auto getHttp = opened.dispatcher.getHttpServiceRequest().send();
      auto service = factory.capnpToKj(getHttp.getHttp());
      auto client = kj::newHttpClient(*service);
      kj::HttpHeaders requestHeaders(headers);
      auto call = client->request(kj::HttpMethod::GET, "http://coworkerd-load/", requestHeaders, uint64_t(0));
      auto exit = process->onExit().then([]() -> kj::HttpClient::Response { KJ_UNREACHABLE; });
      auto response = co_await timer.timeoutAfter(remaining(), kj::mv(call.response).exclusiveJoin(kj::mv(exit)));
      auto body = co_await timer.timeoutAfter(remaining(), response.body->readAllText(65536));
      KJ_REQUIRE(response.statusCode == 200, "the runtime answered the load check with an HTTP error", response.statusCode);
      probe.report = body.cStr();
    } catch (const kj::Exception& error) {
      probe.failure = error.getDescription().cStr();
      probe.timedOut = monotonicTimeMs() >= deadline;
    }
    if (!probe.failure.empty() && !probe.timedOut) {
      // The connection to workerd failed: let the pool observe the exit (it reads the culprit then), bounded.
      std::string ended;
      try { co_await timer.timeoutAfter(1000 * kj::MILLISECONDS, process->onExit()); }
      catch (const kj::Exception& error) { ended = error.getDescription().cStr(); }
      probe.exited = ended.find("exited") != std::string::npos;
      if (probe.exited) probe.culprit = culpritOf(probe.checkId);
    }
    // Rows of this probe's own identity: the limits its load (or its request) hit.
    probe.cpuExceeded = process->cpuLimitExceeded();
    probe.memoryExceeded = process->memoryLimitExceeded();
    process->finish();
    probe.elapsedMs = monotonicTimeMs() - started;
    co_return probe;
  }
  kj::Promise<LoadCheck> loadCheck(std::shared_ptr<const VersionDefinition> version, std::string entrypoint, double deadline, bool root) {
    if (!serverConfig.loadCheck || !version->hasCode()) co_return LoadCheck{};
    // The process the dispatch will select (the same selection, made again there); none: the
    // dispatch refuses.
    auto process = runtime.select(version->workerId);
    if (!process) co_return LoadCheck{};
    auto key = loadKey(*process, version->definitionHash, entrypoint);
    if (auto found = loadConfirmed.find(key); found != loadConfirmed.end() && monotonicTimeMs() - found->second <= loadCheckFreshMs())
      co_return LoadCheck{};
    LoadCheck result;
    result.sequence = nextSequence++;
    const auto forInvocation = invocationId(result.sequence);
    const std::string entrypointName = entrypoint.empty() ? "default" : entrypoint;
    std::string category, detail, message;
    bool coldLoad = false;
    LoadProbe probe;
    for (int attempt = 0;; ++attempt) {
      probe = co_await loadProbe(*version, std::move(process), entrypoint, deadline, forInvocation);
      category.clear(); detail.clear(); message.clear(); coldLoad = false; result.refusal.reset();
      bool limitWithoutEvidence = false;
      if (probe.timedOut) {
        result.refusal = refusal("LEASE_EXPIRED", "lease expired during the load check", 410, root);
      } else if (!probe.culprit.empty()) {
        category = "limit"; detail = "LIMIT_EXCEEDED";
        message = probe.culprit == "memory"
            ? "memory limit exceeded while loading: the workerd process died of a fatal out-of-memory and this load was its culprit"
            : "CPU limit exceeded while loading: the workerd process was killed and this load was its culprit";
      } else if (!probe.failure.empty()) {
        // Nothing of the call ran, but the cause is unknown: as without the check, never tried elsewhere.
        result.refusal = refusal("RESULT_UNCERTAIN", probe.exited ? "the workerd process exited during the load check (no culprit recorded)"
            : "the load check could not reach the runtime", 503, root);
      } else {
        capnp::MallocMessageBuilder arena;
        auto json = arena.initRoot<capnp::JsonValue>();
        capnp::JsonCodec().decodeRaw(kj::arrayPtr(probe.report.data(), probe.report.size()), json);
        using coworkerd::json::field;
        auto flag = [&](const char* name) { auto value = field(json, name); return value.isBoolean() && value.getBoolean(); };
        auto string = [&](const char* name) { auto value = field(json, name); return value.isString() ? std::string(value.getString().cStr()) : std::string(); };
        coldLoad = flag("coldLoad");
        if (flag("ok")) {
          if (!flag("entrypointUsable")) {
            category = "evaluation"; detail = "ENTRYPOINT_MISSING";
            message = entrypointName == "default" ? (flag("hasDefault") ? "the default export has no fetch handler" : "there is no default export")
                : "the entrypoint " + entrypointName + " is missing or has no fetch handler";
          }
        } else if (string("stage") == "code") {
          const auto codeStage = string("codeStage"), text = string("message");
          if (codeStage == "secrets") { category = "secret-missing"; detail = "SECRET_MISSING"; message = text; }
          else if (codeStage == "decode") { category = "evaluation"; detail = "INVALID_MODULE"; message = "a module or variable cannot be decoded: " + string("name") + ": " + text; }
          else { category = PACKAGE_UNAVAILABLE; detail = "PACKAGE_UNAVAILABLE"; message = "the code package could not be fetched or verified: " + text; }
        } else if (string("message").starts_with("Trusted ")) {
          // Our own loader failed in a load another call started (they share the isolate): the package.
          category = PACKAGE_UNAVAILABLE; detail = "PACKAGE_UNAVAILABLE"; message = "the code package could not be fetched or verified: " + string("message");
        } else {
          const auto applied = runtime.effectiveLimits(probe.workerId);
          auto verdict = classifyTrialFailure("load", string("name"), string("message"), probe.cpuExceeded, probe.memoryExceeded,
              applied.cpuMs, applied.memoryMiB);
          category = verdict.category == "LIMIT_EXCEEDED" ? "limit" : "evaluation";
          detail = verdict.category.empty() ? "LOAD_FAILED" : verdict.category;
          message = verdict.message;
          // A limit without a row of this probe's identity was another request's: the isolate this probe
          // entered had just been condemned. A fresh one is loaded by the next probe.
          limitWithoutEvidence = category == "limit" && !probe.cpuExceeded && !probe.memoryExceeded;
        }
      }
      if (!limitWithoutEvidence || attempt > 0) break;
      process = runtime.select(version->workerId);
      if (!process) break;
      key = loadKey(*process, version->definitionHash, entrypoint);
    }
    if (!category.empty()) {
      auto [secrets, variables] = trialRedactions(*version);
      auto value = refusal("LOAD_FAILED", trialClean(message, secrets, variables), 503, root);
      value.category = category;
      result.refusal = std::move(value);
      ++loadFailures[category];
      ++loadFailureDetails[detail];
    }
    ++loadChecks;
    if (coldLoad) ++loadCheckColdLoads;
    if (!result.refusal) {
      loadConfirmed[key] = monotonicTimeMs();
      result.coldLoaded = coldLoad;
    } else {
      loadConfirmed.erase(key);
    }
    auto quoted = [](const std::string& value) { return value.empty() ? std::string("null") : "\"" + jsonEscape(value) + "\""; };
    std::cout << std::setprecision(17) << "[load-check] {\"nodeId\":\"" << jsonEscape(serverConfig.serverId) << "\",\"invocationId\":\""
              << probe.checkId << "\",\"forInvocationId\":\"" << forInvocation << "\",\"target\":\"" << jsonEscape(version->key())
              << "\",\"definitionHash\":\"" << version->definitionHash << "\",\"entrypoint\":\"" << jsonEscape(entrypointName)
              << "\",\"processId\":\"" << probe.processId << "\",\"processGeneration\":" << probe.generation
              << ",\"ok\":" << (result.refusal ? "false" : "true") << ",\"code\":" << quoted(result.refusal ? result.refusal->code : "")
              << ",\"category\":" << quoted(category) << ",\"detail\":" << quoted(detail)
              << ",\"reasonBytes\":" << (result.refusal ? result.refusal->reason.size() : 0) << ",\"coldLoad\":" << (coldLoad ? "true" : "false")
              << ",\"elapsedMs\":" << probe.elapsedMs << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
    co_return result;
  }
  // One line per load failure, on the Server that decided what follows: where it failed, its category
  // and cleaned reason, and whether the call was tried on another candidate.
  void loadFailed(const VersionDefinition& version, const InvocationRejection& failure, const std::string& servedBy,
      const std::string& source, bool rerouted) {
    if (rerouted) ++loadFailureReroutes;
    std::cout << "[load-failed] {\"nodeId\":\"" << jsonEscape(serverConfig.serverId) << "\",\"servedBy\":\"" << jsonEscape(servedBy)
              << "\",\"workerId\":\"" << jsonEscape(version.workerId) << "\",\"versionId\":\"" << jsonEscape(version.versionId)
              << "\",\"definitionHash\":\"" << version.definitionHash << "\",\"category\":\"" << jsonEscape(failure.category)
              << "\",\"reason\":\"" << jsonEscape(cleanReason(failure.reason)) << "\",\"source\":\"" << source
              << "\",\"rerouted\":" << (rerouted ? "true" : "false") << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
  }
  // A backend result that arrived after its ingress response was complete: recorded only.
  void lateBackendFailure(const std::string& target, const kj::Exception& error, uint64_t completedAtUnixMs) {
    ++lateBackendFailures;
    // Cleaned before it is shortened: a cut must not leave part of a secret value.
    const std::string detail = cleanReason(error.getDescription().cStr());
    std::cout << "[late-backend-failure] {\"nodeId\":\"" << jsonEscape(serverConfig.serverId) << "\",\"target\":\"" << jsonEscape(target)
              << "\",\"type\":\"" << kj::str(error.getType()).cStr() << "\",\"detail\":\"" << jsonEscape(detail)
              << "\",\"responseCompletedAtUnixMs\":" << completedAtUnixMs << ",\"failedAtUnixMs\":" << wallTimeMsForTest()
              << ",\"lateBackendFailures\":" << lateBackendFailures << "}" << std::endl;
  }

  OpenEvent open(std::shared_ptr<const VersionDefinition> version, std::string kind,
      SharedVersionPins pins, double deadline = 0, std::shared_ptr<AdmissionSlot> slot = {}, const LoadCheck& check = {}) {
    auto process = selectProcess(version->workerId);
    const auto nowMonotonicMs = monotonicTimeMs();
    expireLeases(nowMonotonicMs);
    auto sequence = check.sequence ? check.sequence : nextSequence++;
    auto id = invocationId(sequence);
    auto expires = deadline > 0 ? deadline : nowMonotonicMs + static_cast<double>(leaseConfig.ttlMs);
    KJ_REQUIRE(std::isfinite(expires), "lease deadline overflow");
    addLease(id, {id, leaseConfig.rootBudget, leaseConfig.rootBudget, 0, expires, pins, serverConfig.serverId});
    return execute(std::move(version), sequence, id, id, std::move(kind), {}, 0, 0,
        leaseConfig.rootBudget, leaseConfig.rootBudget, 0, expires, pins, std::move(slot), std::move(process), serverConfig.serverId,
        check.coldLoaded);
  }

  // ---- Durable Objects ----
  bool durableReady(const std::string& objectKey) { return runtime.durableAvailable(objectKey); }
  std::string durableProcessFor(const std::string& objectKey) const { return runtime.durableProcessFor(objectKey); }
  // How many Durable Object calls led to an object's requests: the most of those in flight, else its latest one's. A call
  // the object makes counts one more (DO_HOP_LIMIT beyond MAX_DURABLE_HOPS): the node's own count, whatever the tenant's
  // code sends.
  struct ObjectActivity { std::multiset<uint16_t> inFlight; uint16_t latest = 0; double lastAt = 0; };
  uint16_t objectHops(const std::string& objectKey) const {
    auto found = objects.find(objectKey);
    if (found == objects.end()) return 0;
    return found->second.inFlight.empty() ? found->second.latest : *found->second.inFlight.rbegin();
  }
  void objectBegin(const std::string& objectKey, uint16_t hops) {
    auto& entry = objects[objectKey];
    entry.inFlight.insert(hops); entry.latest = hops; entry.lastAt = monotonicTimeMs();
    // Bounded: objects without requests in flight are forgotten after ten minutes, or sooner beyond 65536 entries.
    if (objects.size() > 65536 || monotonicTimeMs() - lastObjectPrune > 60000) {
      lastObjectPrune = monotonicTimeMs();
      const double keep = objects.size() > 65536 ? 60000 : 600000;
      std::erase_if(objects, [&](const auto& item) { return item.second.inFlight.empty() && lastObjectPrune - item.second.lastAt > keep; });
    }
  }
  void objectEnd(const std::string& objectKey, uint16_t hops) {
    auto found = objects.find(objectKey);
    if (found == objects.end()) return;
    if (auto at = found->second.inFlight.find(hops); at != found->second.inFlight.end()) found->second.inFlight.erase(at);
    found->second.lastAt = monotonicTimeMs();
  }
  // The version a request to an object runs: its Worker's active one, whose definition declares the class, with the
  // class's namespace active and the one the request names. Null with a code and a reason.
  std::shared_ptr<const VersionDefinition> durableVersion(const std::string& workerId, const DurableTarget& target,
      std::string& code, std::string& reason) const {
    const auto* worker = desiredState.state().worker(workerId);
    if (!worker || !worker->valid || !worker->version) {
      code = "WORKER_INVALID";
      reason = "Worker " + workerId + " has no valid active version" + (worker && !worker->reason.empty() ? ": " + worker->reason : std::string());
      return nullptr;
    }
    const auto* space = desiredState.state().durableNamespace(workerId, target.className);
    if (!worker->version->declares(target.className) || !space || space->id != target.namespaceId || space->status != "active") {
      code = "DO_NAMESPACE_UNAVAILABLE";
      reason = "Durable Object class " + target.className + " of Worker " + workerId + ": " +
          (!worker->version->declares(target.className) ? "the active version " + worker->version->versionId + " does not declare it"
              : !space ? std::string("it has no namespace")
              : space->id != target.namespaceId ? std::string("the request names another namespace")
              : std::string("its namespace is being deleted"));
      return nullptr;
    }
    return worker->version;
  }
  // A request to an object, on the object's durable process: from a stateless Worker a child of its call tree
  // (`allocation`, charged to its budget as a Binding call is), from an object the root of a new call tree.
  OpenEvent openDurable(std::shared_ptr<const VersionDefinition> version, const DurableTarget& target,
      std::optional<ChildAllocation> allocation, std::shared_ptr<AdmissionSlot> slot) {
    auto process = runtime.selectDurable(target.key());
    if (!process) throw ProcessUnavailable{false};
    const auto nowMonotonicMs = monotonicTimeMs();
    expireLeases(nowMonotonicMs);
    const auto sequence = nextSequence++;
    auto id = invocationId(sequence);
    // The request is the object's next event for its log lines.
    startObjectLogEvent(id, target.key());
    if (allocation) {
      if (slot) slot->timing = allocation->timing;
      addLease(id, {allocation->treeId, allocation->grant, allocation->grant, allocation->depth, allocation->expiresAtMonotonicMs,
          allocation->pins, allocation->ingressServerId});
      return execute(std::move(version), sequence, std::move(id), allocation->treeId, "durable", allocation->parentInvocationId,
          allocation->callSequence, allocation->orderSequence, allocation->grant, allocation->remainingAfter, allocation->depth,
          allocation->expiresAtMonotonicMs, allocation->pins, std::move(slot), std::move(process), allocation->ingressServerId, false, &target);
    }
    auto tree = serverConfig.serverId + "." + epoch() + "-tree-" + std::to_string(nextTreeSequence++);
    const auto expires = nowMonotonicMs + static_cast<double>(leaseConfig.ttlMs);
    auto pins = std::make_shared<const VersionPins>(VersionPins{{version->workerId, coworkerd::versionPin(*version)}});
    addLease(id, {tree, leaseConfig.rootBudget, leaseConfig.rootBudget, 0, expires, pins, serverConfig.serverId});
    return execute(std::move(version), sequence, std::move(id), tree, "durable", {}, 0, 0, leaseConfig.rootBudget,
        leaseConfig.rootBudget, 0, expires, pins, std::move(slot), std::move(process), serverConfig.serverId, false, &target);
  }

  ChildAllocation allocateChild(
      std::string treeId,
      std::string parentInvocationId,
      uint64_t callSequence,
      uint64_t orderSequence,
      double presentedExpiresAtMonotonicMs) {
    const auto nowMonotonicMs = monotonicTimeMs();
    expireLeases(nowMonotonicMs);
    // Coworkerd Servers are assumed to share one trust domain. The trusted runtime
    // descriptor and RemoteInvocation fields are accepted directly. Crossing a
    // trust-domain boundary requires a new authentication and authorization design.
    auto parent = leases.find(parentInvocationId);
    if (parent == leases.end() ||
        parent->second.expiresAtMonotonicMs <= nowMonotonicMs ||
        !parent->second.presents(presentedExpiresAtMonotonicMs)) {
      if (parent != leases.end() && parent->second.expiresAtMonotonicMs <= nowMonotonicMs && !parent->second.held) {
        leases.erase(parent);
      }
      ++expiredLeaseRejections;
      lastRejection = "lease expired before execution for parent=" +
          parentInvocationId + " tree=" + treeId +
          " nowMonotonicMs=" + std::to_string(nowMonotonicMs) +
          " presentedExpiresAtMonotonicMs=" + std::to_string(presentedExpiresAtMonotonicMs);
      std::cerr << "[server] reject " << cleanReason(lastRejection) << std::endl;
      KJ_FAIL_REQUIRE(lastRejection.c_str());
    }
    auto expiresAtMonotonicMs = parent->second.expiresAtMonotonicMs;
    auto depth = static_cast<uint16_t>(parent->second.depth + 1);
    KJ_REQUIRE(depth > parent->second.depth, "invocation depth overflow");
    auto parentRemainingBefore = parent->second.remainingSubrequests;
    uint16_t grant;
    uint16_t charged;
    uint16_t remainingAfter;

    if (leaseConfig.naive()) {
      // Deliberately retain the bug of the test-only counterexample: each
      // child receives a copy of its parent's remaining budget.
      grant = parentRemainingBefore;
      charged = 0;
      remainingAfter = parentRemainingBefore;
    } else {
      if (parentRemainingBefore < 1) {
        ++budgetRejections;
        lastRejection = "subrequest budget exhausted before execution for parent=" +
            parentInvocationId + " tree=" + treeId;
        std::cerr << "[server] reject " << cleanReason(lastRejection) << std::endl;
        KJ_FAIL_REQUIRE(lastRejection.c_str());
      }
      grant = std::min<uint16_t>(
          leaseConfig.grantCap,
          static_cast<uint16_t>(parentRemainingBefore - 1));
      charged = static_cast<uint16_t>(1 + grant);
      parent->second.remainingSubrequests =
          static_cast<uint16_t>(parentRemainingBefore - charged);
      remainingAfter = parent->second.remainingSubrequests;
    }

    if (leaseConfig.naive()) {
      auto orderKey = treeId + "#" + std::to_string(depth);
      auto& expected = nextArrivalOrder[orderKey];
      if (expected == 0) expected = 1;
      if (orderSequence != expected) {
        ++orderRejections;
        lastRejection = "Binding arrival out of order: expected=" +
            std::to_string(expected) + " presented=" +
            std::to_string(orderSequence) + " tree=" + treeId +
            " depth=" + std::to_string(depth);
        std::cerr << "[server] reject " << cleanReason(lastRejection) << std::endl;
        KJ_FAIL_REQUIRE(lastRejection.c_str());
      }
      ++expected;
    }

    return {
      std::move(treeId),
      std::move(parentInvocationId),
      callSequence,
      orderSequence,
      grant,
      charged,
      remainingAfter,
      depth,
      expiresAtMonotonicMs,
      parent->second.pins,
      false,
      parent->second.ingressServerId,
    };
  }

  OpenEvent executeAllocated(
      std::shared_ptr<const VersionDefinition> version,
      std::string kind,
      ChildAllocation allocation, std::shared_ptr<AdmissionSlot> slot = {}, const LoadCheck& check = {}) {
    if (slot) { slot->timing = allocation.timing; slot->entrypoint = allocation.entrypoint; }
    auto process = selectProcess(version->workerId);
    auto sequence = check.sequence ? check.sequence : nextSequence++;
    auto invocationId = this->invocationId(sequence);
    addLease(invocationId, {
      allocation.treeId,
      allocation.grant,
      allocation.grant,
      allocation.depth,
      allocation.expiresAtMonotonicMs,
      allocation.pins,
      allocation.ingressServerId,
    });
    return execute(
        std::move(version),
        sequence,
        std::move(invocationId),
        std::move(allocation.treeId),
        std::move(kind),
        std::move(allocation.parentInvocationId),
        allocation.callSequence,
        allocation.orderSequence,
        allocation.grant,
        allocation.remainingAfter,
        allocation.depth,
        allocation.expiresAtMonotonicMs,
        allocation.pins, std::move(slot), std::move(process), allocation.ingressServerId, check.coldLoaded);
  }

  OpenEvent openGranted(
      std::shared_ptr<const VersionDefinition> version,
      std::string treeId,
      std::string parentInvocationId,
      uint16_t grant,
      uint16_t depth,
      double expiresAtMonotonicMs,
      uint64_t callSequence,
      uint64_t orderSequence,
      SharedVersionPins pins, std::shared_ptr<AdmissionSlot> slot = {},
      bool ingressRoot = false, std::string ingressServerId = {}, const LoadCheck& check = {}) {
    auto process = selectProcess(version->workerId);
    const auto nowMonotonicMs = monotonicTimeMs();
    expireLeases(nowMonotonicMs);
    if (expiresAtMonotonicMs <= nowMonotonicMs) {
      ++expiredLeaseRejections;
      lastRejection = "lease expired before remote execution for parent=" +
          parentInvocationId + " tree=" + treeId;
      KJ_FAIL_REQUIRE(lastRejection.c_str());
    }
    auto sequence = check.sequence ? check.sequence : nextSequence++;
    auto invocationId = this->invocationId(sequence);
    addLease(invocationId, {treeId, grant, grant, depth, expiresAtMonotonicMs, pins, ingressServerId});
    return execute(
        std::move(version),
        sequence,
        std::move(invocationId),
        std::move(treeId),
        ingressRoot ? "ingress" : "peer",
        std::move(parentInvocationId),
        callSequence,
        orderSequence,
        grant,
        grant,
        depth,
        expiresAtMonotonicMs,
        pins, std::move(slot), std::move(process), std::move(ingressServerId), check.coldLoaded);
  }

  void recordForward(
      const VersionDefinition& version,
      const ChildAllocation& allocation,
      const std::string& targetServerId) {
    ++forwardedInvocations;
    auto sequence = nextDebitSequence++;
    if (debits.size() == MAX_EXECUTION_RECORDS) debits.pop_front();
    debits.push_back({
      sequence,
      allocation.treeId,
      allocation.parentInvocationId,
      version.key(),
      targetServerId,
      allocation.grant,
      allocation.charged,
      allocation.remainingAfter,
      allocation.depth,
      allocation.expiresAtMonotonicMs,
    });
  }

  void recordRelay(
      const std::string& treeId,
      const std::string& parentInvocationId,
      const std::string& target,
      const std::string& targetServerId,
      uint16_t grant,
      uint16_t depth,
      double expiresAtMonotonicMs) {
    ++forwardedInvocations;
    auto sequence = nextDebitSequence++;
    if (debits.size() == MAX_EXECUTION_RECORDS) debits.pop_front();
    debits.push_back({
      sequence, treeId, parentInvocationId, target, targetServerId,
      grant, 0, grant, depth, expiresAtMonotonicMs,
    });
  }

  void observeRemoteInvocationSize(uint64_t bytes, size_t pins) {
    remoteInvocationBytes = bytes;
    remoteInvocationPinCount = pins;
  }

  void rejectPeerUnavailable(std::string detail) {
    ++peerUnavailableRejections;
    lastRejection = std::move(detail);
  }

  void rejectExpiredLease(std::string detail) {
    ++expiredLeaseRejections;
    lastRejection = std::move(detail);
  }

  void rejectVersionUnavailable(std::string detail) {
    ++versionUnavailableRejections;
    if (detail.find("定义未知") != std::string::npos) ++versionUnknownRejections;
    else if (detail.find("定义不一致") != std::string::npos) ++versionInconsistentRejections;
    else if (detail.find("代码不可用") != std::string::npos) ++versionCodeRejections;
    lastRejection = std::move(detail);
  }

  void rejectPinMismatch(std::string detail) {
    ++versionPinMismatchRejections;
    lastRejection = std::move(detail);
  }

  void rejectPlacementHops(std::string detail) {
    ++placementHopRejections;
    lastRejection = std::move(detail);
  }

private:
  OpenEvent execute(
      std::shared_ptr<const VersionDefinition> version,
      uint64_t sequence,
      std::string invocationId,
      std::string treeId,
      std::string kind,
      std::string parentInvocationId,
      uint64_t callSequence,
      uint64_t orderSequence,
      uint16_t grant,
      uint16_t remainingAfter,
      uint16_t depth,
      double expiresAtMonotonicMs,
      SharedVersionPins pins, std::shared_ptr<AdmissionSlot> slot, std::shared_ptr<coworkerd::ProcessCall> process,
      std::string ingressServerId = {}, bool coldLoaded = false, const DurableTarget* durable = nullptr) {

    auto timing = slot ? slot->timing : nullptr;
    if (timing) { timing->invocationId = invocationId; timing->treeId = treeId; timing->target = version->key(); }
    pathMark(timing, "open-execution");
    auto descriptor = encodeDescriptor(
        *version,
        invocationId,
        treeId,
        grant,
        grant,
        orderSequence,
        depth,
        expiresAtMonotonicMs,
        pins, *process, ingressServerId, slot ? slot->entrypoint : "", false, false, {}, coldLoaded, durable);
    pathMark(timing, "descriptor");
    process->workerId = version->workerId; process->versionId = version->versionId;
    process->invocationId = invocationId; process->treeId = treeId;
    if (slot) { slot->process = process; slot->ingressNodeId = ingressServerId; }
    if (executions.size() == MAX_EXECUTION_RECORDS) {
      executions.pop_front();
      ++droppedExecutions;
    }
    executions.push_back({
      sequence, process->processId, process->generation,
      std::move(treeId),
      invocationId,
      version->key(),
      std::move(kind),
      std::move(parentInvocationId),
      callSequence,
      orderSequence,
      grant,
      remainingAfter,
      depth,
      expiresAtMonotonicMs,
      ingressServerId,
      // The load check before this dispatch loaded the version: this call's cold start.
      coldLoaded,
    });
    std::cout << "[server] execute target=" << version->key()
              << " kind=" << executions.back().kind
              << " tree=" << executions.back().treeId
              << " invocation=" << invocationId << std::endl;
    lastExecution[version->definitionHash] = monotonicTimeMs();
    if (usage) usage->request(coworkerd::tenantOf(version->workerId));
    std::cout << "[execution] {\"nodeId\":\"" << serverConfig.serverId
              << "\",\"processId\":\"" << process->processId << "\",\"processGeneration\":" << process->generation
              << ",\"sequence\":" << sequence << ",\"treeId\":\"" << executions.back().treeId
              << "\",\"invocationId\":\"" << invocationId << "\",\"target\":\"" << version->key()
              << "\",\"definitionHash\":\"" << version->definitionHash << "\",\"kind\":\"" << executions.back().kind
              << "\",\"parentInvocationId\":\"" << executions.back().parentInvocationId
              << "\",\"grant\":" << grant << ",\"remainingAfter\":" << remainingAfter << ",\"depth\":" << depth
              << ",\"expiresAtMonotonicMs\":" << std::setprecision(17) << expiresAtMonotonicMs
              << ",\"ingressNodeId\":\"" << jsonEscape(ingressServerId) << "\",\"coldStart\":null"
              << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
    auto started = monotonicTimeMs();
    pathMark(timing, "execution-logs");
    auto opened = runtime.open(process, descriptor, version->definitionHash);
    pathMark(timing, "open-runtime-send");
    phaseTiming("open-runtime-send", started, process);
    opened.ready = kj::mv(opened.ready).then([started, process, timing] {
      pathMark(timing, "open-runtime-response"); phaseTiming("open-runtime-response", started, process);
    });
    if (slot) opened.dispatcher = trackAdmission(std::move(opened.dispatcher), std::move(slot), factory, headers);
    return opened;
  }

public:

  void reload() {
    desiredState.reload();
  }

  std::string deploymentJson() const {
    return "{\"nodeId\":\"" + jsonEscape(serverConfig.serverId) + "\",\"deployment\":" + desiredState.json(false) +
        ",\"routeRejections\":" + routeRejectionsJson() + ",\"assets\":" + assetsJson() +
        ",\"serverNowUnixMs\":" + std::to_string(static_cast<uint64_t>(wallTimeMsForTest())) + '}';
  }

  void observeSequenceMapSize(uint64_t size) {
    nextChildSequencesSize = size;
  }

  bool requiresChildSequence() const {
    return leaseConfig.naive();
  }

  void expireForState() {
    expireLeases(monotonicTimeMs());
  }

  void reapExpiredLeases() {
    expireLeases(monotonicTimeMs());
    std::erase_if(lastExecution, [&](const auto& entry) { return monotonicTimeMs() - entry.second > serverConfig.idleMs; });
    if (leaseReaperRuns % 50 == 0) {
      const auto stale = 2 * loadCheckFreshMs();
      std::erase_if(loadConfirmed, [&](const auto& entry) { return monotonicTimeMs() - entry.second > stale; });
    }
    ++leaseReaperRuns;
  }

  kj::Promise<std::string> sourceJson(
      const std::string& workerId,
      const std::string& versionId,
      const std::string& expectedHash,
      const std::string& invocationId = {}, const std::string& processId = {},
      const std::string& generation = {}, const std::string& definitionHash = {}) {
    KJ_REQUIRE(coworkerd::workerFullName(workerId), "invalid source workerId");
    KJ_REQUIRE(safeName(versionId), "invalid source versionId");
    KJ_REQUIRE(validSha256(expectedHash), "invalid source contentHash");
    // Identity is the definition hash; the label alone may name several.
    auto version = exact(workerId + "@" + versionId, validSha256(definitionHash) ? definitionHash : std::string());
    KJ_REQUIRE(version->contentHash == expectedHash, "source contentHash mismatch");
    if (!invocationId.empty()) {
      KJ_REQUIRE(safeName(invocationId) && safeName(processId) && validSha256(definitionHash) &&
          version->definitionHash == definitionHash, "invalid loader observation");
      const auto key = processId + ":" + generation + ":" + definitionHash;
      if (coldStarts.emplace(key, 1).second) {
        for (auto& execution : executions) if (execution.invocationId == invocationId) execution.coldStart = true;
        std::cout << "[cold-start] {\"nodeId\":\"" << serverConfig.serverId << "\",\"processId\":\""
                  << processId << "\",\"processGeneration\":" << generation << ",\"invocationId\":\"" << invocationId
                  << "\",\"target\":\"" << version->key() << "\",\"definitionHash\":\"" << definitionHash
                  << "\",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
      }
    }
    auto source = co_await desiredState.source(*version);
    ++sourceFetches;
    std::ostringstream output;
    output << "{\"workerId\":\"" << jsonEscape(version->workerId)
           << "\",\"versionId\":\"" << jsonEscape(version->versionId)
           << "\",\"contentHash\":\"" << jsonEscape(version->contentHash)
           << "\",\"source\":\"" << jsonEscape(source) << "\"}";
    co_return output.str();
  }

  std::string stateJson(const std::string& peerConnectionsJson = "{}", const std::string& avoidanceJson = "[]") {
    expireForState();
    std::set<const VersionPins*> pinTables;
    size_t pinReferences = 0;
    for (const auto& [id, lease] : leases) {
      pinTables.insert(lease.pins.get());
      pinReferences += lease.pins->size();
    }
    std::ostringstream output;
    output << std::setprecision(17);
    output << "{\"nodeId\":\"" << jsonEscape(serverConfig.serverId)
           << "\",\"peerListenAddress\":\""
           << jsonEscape(serverConfig.peerListenAddress)
           << "\",\"membership\":" << membership.view().json()
           << ",\"server\":" << serverJson() << ",\"version\":" << versionJson()
           << ",\"launch\":" << launch << ",\"configuration\":" << configuration << ",\"management\":" << managementJson()
           << ",\"drain\":" << drainJson()
           << ",\"tenants\":" << tenantsJson()
           << ",\"usage\":" << usageJson()
           << ",\"processes\":" << runtime.json(serverConfig.idleMs)
           << ",\"memory\":" << memory.json()
           << ",\"logging\":" << coworkerd::AsyncLog::stateJson()
           << ",\"cpuLimitMs\":" << runtime.cpuLimitMs()
           << ",\"memoryLimitMiB\":" << runtime.memoryLimitMiB()
           << ",\"bundles\":" << desiredState.bundlesJson()
           << ",\"deployment\":" << desiredState.json()
           << ",\"generation\":" << desiredState.state().generation()
           << ",\"routeRejections\":" << routeRejectionsJson()
           << ",\"assets\":" << assetsJson()
           << ",\"maxExecutionRecords\":" << MAX_EXECUTION_RECORDS
           << ",\"droppedExecutions\":" << droppedExecutions
           << ",\"leaseCount\":" << leases.size()
           << ",\"leaseVersionPinTables\":" << pinTables.size()
           << ",\"leaseVersionPinReferences\":" << pinReferences
           << ",\"remoteInvocationObjectBytes\":" << remoteInvocationBytes
           << ",\"remoteInvocationPinCount\":" << remoteInvocationPinCount
           << ",\"nextChildSequencesSize\":" << nextChildSequencesSize
           << ",\"leaseMode\":\"" << (leaseConfig.naive() ? "naive" : "correct") << "\""
           << ",\"leaseTtlMs\":" << leaseConfig.ttlMs
           << ",\"clockOffsetMsForTest\":" << serverConfig.clockOffsetMsForTest
           << ",\"serverNowUnixMs\":" << wallTimeMsForTest()
           << ",\"serverNowMonotonicMs\":" << monotonicTimeMs()
           << ",\"expiredLeases\":" << expiredLeases
           << ",\"expiredLeaseRejections\":" << expiredLeaseRejections
           << ",\"renewedLeases\":" << renewedLeases
           << ",\"leaseReaperIntervalMs\":" << LEASE_REAPER_INTERVAL_MS
           << ",\"leaseReaperRuns\":" << leaseReaperRuns
           << ",\"rootBudget\":" << leaseConfig.rootBudget
           << ",\"grantCap\":" << leaseConfig.grantCap
           << ",\"budgetRejections\":" << budgetRejections
           << ",\"orderRejections\":" << orderRejections
           << ",\"forwardedInvocations\":" << forwardedInvocations
           << ",\"peerUnavailableRejections\":" << peerUnavailableRejections
           << ",\"placement\":" << [&]() { auto value = placementJson(); value.pop_back(); return value + ",\"avoidance\":" + avoidanceJson + "}"; }()
           << ",\"peerConnections\":" << peerConnectionsJson
           << ",\"versionUnavailableRejections\":" << versionUnavailableRejections
           << ",\"versionUnavailableByReason\":{\"definitionUnknown\":" << versionUnknownRejections
           << ",\"definitionInconsistent\":" << versionInconsistentRejections
           << ",\"codeUnavailable\":" << versionCodeRejections << "}"
           << ",\"versionPinMismatchRejections\":" << versionPinMismatchRejections
           << ",\"secretUnavailableRejections\":" << secretUnavailableRejections
           << ",\"placementHopRejections\":" << placementHopRejections
           << ",\"receivedPeerInvocations\":" << receivedPeerInvocations
           << ",\"lastRejection\":\"" << jsonEscape(cleanReason(lastRejection)) << "\""
           << ",\"sourceFetches\":" << sourceFetches
           << ",\"trialLoads\":" << trialLoads << ",\"trialFailures\":" << [&] {
                std::string out = "{";
                for (const auto& [category, count] : trialFailures) {
                  if (out.size() > 1) out += ',';
                  out += "\"" + jsonEscape(category) + "\":" + std::to_string(count);
                }
                return out + '}';
              }()
           << ",\"trialProbes\":" << trialProbes << ",\"trialColdLoads\":" << trialColdLoads << ",\"recentTrials\":[" << [&] {
                std::string out;
                for (const auto& record : recentTrials) { if (!out.empty()) out += ','; out += record; }
                return out;
              }() << ']'
           << ",\"loadCheck\":{\"enabled\":" << (serverConfig.loadCheck ? "true" : "false") << ",\"freshMs\":" << loadCheckFreshMs()
           << ",\"checks\":" << loadChecks << ",\"coldLoads\":" << loadCheckColdLoads << ",\"confirmed\":" << loadConfirmed.size() << "}"
           << ",\"loadFailures\":" << countsJson(loadFailures) << ",\"loadFailureDetails\":" << countsJson(loadFailureDetails)
           << ",\"loadFailureReroutes\":" << loadFailureReroutes << ",\"lateBackendFailures\":" << lateBackendFailures
           << ",\"ingressPlacement\":" << (serverConfig.ingressPlacement ? "true" : "false")
           << ",\"proxyingIngress\":" << proxyingIngress
           << ",\"leases\":[";
    size_t leaseIndex = 0;
    for (const auto& [invocationId, lease] : leases) {
      if (leaseIndex++ > 0) output << ',';
      output << "{\"treeId\":\"" << jsonEscape(lease.treeId)
             << "\",\"invocationId\":\"" << jsonEscape(invocationId)
             << "\",\"grant\":" << lease.grant
             << ",\"remainingSubrequests\":" << lease.remainingSubrequests
             << ",\"depth\":" << lease.depth
             << ",\"expiresAtMonotonicMs\":" << lease.expiresAtMonotonicMs << "}";
    }
    output << "],\"debits\":[";
    for (size_t index = 0; index < debits.size(); ++index) {
      if (index > 0) output << ',';
      const auto& record = debits[index];
      output << "{\"sequence\":" << record.sequence
             << ",\"treeId\":\"" << jsonEscape(record.treeId)
             << "\",\"parentInvocationId\":\""
             << jsonEscape(record.parentInvocationId)
             << "\",\"target\":\"" << jsonEscape(record.target)
             << "\",\"targetNodeId\":\""
             << jsonEscape(record.targetServerId)
             << "\",\"grant\":" << record.grant
             << ",\"charged\":" << record.charged
             << ",\"remainingAfter\":" << record.remainingAfter
             << ",\"depth\":" << record.depth
             << ",\"expiresAtMonotonicMs\":" << record.expiresAtMonotonicMs << "}";
    }
    output << "],\"peerOrigins\":{";
    size_t originIndex = 0;
    for (const auto& [origin, count] : peerOrigins) {
      if (originIndex++ > 0) output << ',';
      output << "\"" << jsonEscape(origin) << "\":" << count;
    }
    output << "},\"executions\":[";
    for (size_t index = 0; index < executions.size(); ++index) {
      if (index > 0) output << ',';
      const auto& record = executions[index];
      output << "{\"sequence\":" << record.sequence
             << ",\"processId\":\"" << record.processId << "\",\"processGeneration\":" << record.processGeneration
             << ",\"treeId\":\"" << jsonEscape(record.treeId)
             << "\",\"invocationId\":\"" << jsonEscape(record.invocationId)
             << "\",\"target\":\"" << jsonEscape(record.target)
             << "\",\"kind\":\"" << jsonEscape(record.kind)
             << "\",\"parentInvocationId\":\"" << jsonEscape(record.parentInvocationId)
             << "\",\"callSequence\":" << record.callSequence
             << ",\"orderSequence\":" << record.orderSequence
             << ",\"grant\":" << record.grant
             << ",\"remainingAfter\":" << record.remainingAfter
             << ",\"depth\":" << record.depth
             << ",\"ingressNodeId\":\"" << jsonEscape(record.ingressServerId)
             << "\",\"coldStart\":" << (record.coldStart ? "true" : "false")
             << ",\"expiresAtMonotonicMs\":" << record.expiresAtMonotonicMs << "}";
    }
    output << "]";
    for (const auto& [name, section] : stateSections) output << ",\"" << name << "\":" << section();
    output << "}";
    return output.str();
  }

  // Sections the state endpoint appends after the ones above, each from its own part of the node (egress,
  // peerAuth, storage).
  void addStateSection(std::string name, std::function<std::string()> section) {
    stateSections.emplace_back(std::move(name), std::move(section));
  }
  std::vector<std::pair<std::string, std::function<std::string()>>> stateSections;

  std::string lastRejectionReason() const {
    return lastRejection;
  }

  static std::string countsJson(const std::map<std::string, uint64_t>& values) {
    std::string out = "{";
    for (const auto& [name, count] : values) {
      if (out.size() > 1) out += ',';
      out += "\"" + jsonEscape(name) + "\":" + std::to_string(count);
    }
    return out + '}';
  }

  void observePeerOrigin(const std::string& originServerId) {
    ++receivedPeerInvocations;
    ++peerOrigins[originServerId];
  }

  // Trial loads: a deployment asks this Server to load a version before
  // its pointer flips. The probe takes the ordinary process selection
  // (quarantine included) and runtime path, with the ordinary CPU and memory
  // limits and culprit attribution, but it is no tenant request: no admission
  // slot, lease, execution record or placement decision, and nothing it runs
  // can make a subrequest. Trials are counted on their own.
  uint64_t deploymentRevision() const { return desiredState.state().revision(); }
  // The desired state read now (a trial load naming a revision this node has not applied yet).
  kj::Promise<void> refreshDesiredState() { return desiredState.refresh(); }
  std::shared_ptr<const VersionDefinition> definitionByHash(const std::string& hash) const { return desiredState.byHash(hash); }
  std::vector<std::string> missingSecrets(const VersionDefinition& version) const {
    return desiredState.state().missingSecrets(version);
  }
  // What a trial report must not repeat: the values of the version's secrets
  // (this Server's view) and of its variables.
  std::pair<std::vector<std::string>, std::vector<std::string>> trialRedactions(const VersionDefinition& version) const {
    std::pair<std::vector<std::string>, std::vector<std::string>> values;
    for (const auto& name : version.secrets) {
      if (const auto* secret = desiredState.state().secret(version.workerId, name)) values.first.push_back(secret->value);
    }
    for (const auto& variable : version.variables) values.second.push_back(variable.value);
    return values;
  }
  uint32_t cpuLimitMs() const { return runtime.cpuLimitMs(); }
  uint32_t memoryLimitMiB() const { return runtime.memoryLimitMiB(); }
  // What a Worker's executions here are limited to: what it or its tenant names, else this node's.
  coworkerd::WorkerLimits effectiveLimits(const std::string& workerId) const { return runtime.effectiveLimits(workerId); }
  // A trial's process: the tenant's quarantine shard, never an ordinary process.
  std::shared_ptr<coworkerd::ProcessCall> trialProcess(const std::string& worker) { return runtime.selectTrial(worker); }
  bool quarantined(const std::string& worker) const { return runtime.quarantined(worker); }
  bool trialsQuarantined() const { return serverConfig.processPool.quarantineProcesses > 0; }
  std::string nextTrialInvocationId() {
    return serverConfig.serverId + "." + epoch() + "-trial-" + std::to_string(nextTrialSequence++);
  }
  OpenEvent openTrial(const VersionDefinition& version, const std::shared_ptr<coworkerd::ProcessCall>& process,
      const std::string& invocationId, double deadline) {
    auto descriptor = encodeDescriptor(version, invocationId, invocationId, 0, 0, 0, 0, deadline,
        std::make_shared<const VersionPins>(), *process, serverConfig.serverId, "", true);
    process->workerId = version.workerId; process->versionId = version.versionId;
    process->invocationId = invocationId; process->treeId = invocationId;
    return runtime.open(process, descriptor, version.definitionHash);
  }
  // The culprit a workerd exit or kill confirmed for this invocation: "memory"
  // (fatal OOM), "cpu" (watchdog kill), or empty.
  std::string culpritOf(const std::string& invocationId) {
    auto text = runtime.json(serverConfig.idleMs);
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(text.data(), text.size()), json);
    for (auto [list, kind] : {std::pair{"recentMemoryCrashes", "memory"}, std::pair{"recentKills", "cpu"}}) {
      auto entries = coworkerd::json::field(json, list);
      if (!entries.isArray()) continue;
      for (auto entry : entries.getArray()) {
        auto culprit = entry.isObject() ? coworkerd::json::field(entry, "culprit") : capnp::JsonValue::Reader();
        if (!culprit.isObject()) continue;
        auto id = coworkerd::json::field(culprit, "invocationId");
        if (id.isString() && id.getString() == invocationId.c_str()) return kind;
      }
    }
    return {};
  }
  void recordTrial(const std::string& category, bool dispatched, bool coldLoad, std::string record) {
    ++trialLoads;
    if (dispatched) ++trialProbes;
    if (coldLoad) ++trialColdLoads;
    if (!category.empty()) ++trialFailures[category];
    if (recentTrials.size() == 32) recentTrials.pop_front();
    recentTrials.push_back(std::move(record));
  }

private:
  kj::String encodeDescriptor(
      const VersionDefinition& version,
      const std::string& invocationId,
      const std::string& treeId,
      uint16_t remainingSubrequests,
      uint16_t grant,
      uint64_t orderSequence,
      uint16_t depth,
      double expiresAtMonotonicMs,
      const SharedVersionPins& pins, const coworkerd::ProcessCall& process,
      const std::string& ingressServerId, const std::string& entrypoint, bool trial = false,
      bool loadCheck = false, const std::string& sourceInvocationId = {}, bool coldLoaded = false,
      const DurableTarget* durable = nullptr) const {
    capnp::MallocMessageBuilder message;
    auto descriptor = message.initRoot<rpc::InvocationDescriptor>();
    descriptor.setServerId(serverConfig.serverId.c_str());
    descriptor.setProcessId(process.processId.c_str());
    descriptor.setProcessGeneration(process.generation);
    descriptor.setIngressServerId(ingressServerId.c_str());
    descriptor.setWorkerId(version.workerId.c_str());
    descriptor.setVersionId(version.versionId.c_str());
    descriptor.setCompatibilityDate(version.compatibilityDate.c_str());
    descriptor.setMainModule(version.mainModule.c_str());
    descriptor.setContentHash(version.contentHash.c_str());
    descriptor.setDefinitionHash(version.definitionHash.c_str());
    descriptor.setFormat(version.format.c_str());
    descriptor.setEntrypoint(entrypoint.c_str());
    auto flags = descriptor.initCompatibilityFlags(version.compatibilityFlags.size());
    for (size_t i = 0; i < version.compatibilityFlags.size(); ++i) flags.set(i, version.compatibilityFlags[i].c_str());
    auto variables = descriptor.initVariables(version.variables.size());
    for (size_t i = 0; i < version.variables.size(); ++i) {
      variables[i].setName(version.variables[i].name.c_str()); variables[i].setType(version.variables[i].type.c_str());
      variables[i].setValue(version.variables[i].value.c_str());
    }
    // Secrets travel with the definition to the runtime, which merges them into
    // env when it loads the Worker: the values of this Server's deployment view
    // (a RemoteInvocation never carries them). Every dispatch checks them first
    // (secretRefusal), synchronously before this point.
    if (!version.secrets.empty()) {
      const auto& state = desiredState.state();
      auto secrets = descriptor.initSecrets(version.secrets.size());
      for (size_t i = 0; i < version.secrets.size(); ++i) {
        const auto* secret = state.secret(version.workerId, version.secrets[i]);
        KJ_REQUIRE(secret != nullptr, "secret unavailable at dispatch", version.workerId.c_str(), version.secrets[i].c_str());
        secrets[i].setName(version.secrets[i].c_str());
        secrets[i].setValue(capnp::Text::Reader(secret->value.data(), secret->value.size()));
      }
    }
    descriptor.setInvocationId(invocationId.c_str());
    descriptor.setTreeId(treeId.c_str());
    auto lease = descriptor.initLease();
    lease.setRemainingSubrequests(remainingSubrequests);
    lease.setGrant(grant);
    lease.setExpiresAtMonotonicMs(expiresAtMonotonicMs);
    descriptor.setOrderSequence(orderSequence);
    descriptor.setDepth(depth);
    descriptor.setRequiresChildSequence(leaseConfig.naive());
    descriptor.setColdLoaded(coldLoaded);
    // The limits this Worker names: the runtime bridge puts them in the loader key, so the version's
    // isolate is created under them. A limit it does not name stays 0 here and the process's value applies
    // (and the loader key of a Worker that names none has no limits segment).
    const auto limits = namedLimits(version.workerId);
    descriptor.setCpuLimitMs(limits.cpuMs);
    descriptor.setMemoryLimitMiB(limits.memoryMiB);
    if (trial) {
      // A trial load gets no Binding: nothing it may run could call one. The load check is one. Its bridge checks the
      // Durable Object classes the version declares among the exports.
      descriptor.setTrial(true);
      descriptor.setLoadCheck(loadCheck);
      if (!sourceInvocationId.empty()) descriptor.setSourceInvocationId(sourceInvocationId.c_str());
      descriptor.initBindings(0);
      if (!version.declaredClasses.empty()) {
        auto classes = descriptor.initDurableObjectClasses(version.declaredClasses.size());
        for (size_t i = 0; i < version.declaredClasses.size(); ++i) classes.set(i, version.declaredClasses[i].c_str());
      }
      capnp::JsonCodec json;
      return json.encode(descriptor.asReader());
    }
    // The classes the version declares (its bridge exports each one for the objects' Hosts, whichever call loads the
    // isolate first), and its Durable Object namespace Bindings, with this node's view of each class's namespace: a
    // namespace that cannot be used has no id (its Binding refuses every call).
    if (!version.declaredClasses.empty()) {
      auto classes = descriptor.initDurableObjectClasses(version.declaredClasses.size());
      for (size_t i = 0; i < version.declaredClasses.size(); ++i) classes.set(i, version.declaredClasses[i].c_str());
    }
    if (!version.durableObjects.empty()) {
      auto namespaces = descriptor.initDurableObjects(version.durableObjects.size());
      for (size_t i = 0; i < version.durableObjects.size(); ++i) {
        const auto& binding = version.durableObjects[i];
        namespaces[i].setName(binding.binding.c_str());
        namespaces[i].setClassName(binding.className.c_str());
        const auto* space = desiredState.state().durableNamespace(version.workerId, binding.className);
        namespaces[i].setNamespaceId(space && space->status == "active" ? space->id.c_str() : "");
      }
    }
    if (durable) {
      // A request to an object: its Host runs the Worker's class with the version's variables, secrets and Durable Object
      // Bindings; Service Bindings are not given to a class in this release.
      auto target = descriptor.initDurableObject();
      target.setNamespaceId(durable->namespaceId.c_str());
      target.setObjectId(durable->objectId.c_str());
      target.setClassName(durable->className.c_str());
      target.setHops(durable->hops);
      descriptor.initBindings(0);
      capnp::JsonCodec json;
      return json.encode(descriptor.asReader());
    }
    auto bindingPins = pins;
    if (perCallForTest()) {
      const auto* worker = desiredState.state().worker(version.workerId);
      if (worker && worker->valid) bindingPins = worker->closure;
    }
    auto bindings = descriptor.initBindings(version.bindings.size());
    for (size_t index = 0; index < version.bindings.size(); ++index) {
      bindings[index].setName(version.bindings[index].name.c_str());
      if (version.bindings[index].assets) {
        // The ASSETS Binding: this version's own asset layer.
        bindings[index].setAssets(true);
        bindings[index].setWorkerId(version.workerId.c_str());
        bindings[index].setVersionId(version.versionId.c_str());
        bindings[index].setContentHash(version.contentHash.c_str());
        bindings[index].setDefinitionHash(version.definitionHash.c_str());
        continue;
      }
      bindings[index].setWorkerId(version.bindings[index].workerId.c_str());
      const auto& target = bindingPins->at(version.bindings[index].workerId);
      bindings[index].setVersionId(target.versionId.c_str());
      bindings[index].setContentHash(target.contentHash.c_str());
      bindings[index].setDefinitionHash(target.definitionHash.c_str());
      bindings[index].setEntrypoint(version.bindings[index].entrypoint.c_str());
    }
    capnp::JsonCodec json;
    return json.encode(descriptor.asReader());
  }

  void addLease(std::string invocationId, InvocationLeaseState state) {
    auto expiresAtMonotonicMs = state.expiresAtMonotonicMs;
    auto [_, inserted] = leases.emplace(invocationId, std::move(state));
    KJ_ASSERT(inserted, "duplicate invocation lease");
    leaseExpiries.push({expiresAtMonotonicMs, std::move(invocationId)});
  }

  void expireLeases(double nowMonotonicMs) {
    while (!leaseExpiries.empty() &&
        leaseExpiries.top().expiresAtMonotonicMs <= nowMonotonicMs) {
      auto expiry = leaseExpiries.top();
      leaseExpiries.pop();
      auto lease = leases.find(expiry.invocationId);
      if (lease != leases.end() && !lease->second.held &&
          lease->second.expiresAtMonotonicMs == expiry.expiresAtMonotonicMs) {
        leases.erase(lease);
        ++expiredLeases;
      }
    }
  }

  coworkerd::DesiredState& desiredState;
  coworkerd::ProcessPool& runtime;
  coworkerd::MemoryWatermark& memory;
  std::deque<ExecutionRecord> executions;
  std::deque<DebitRecord> debits;
  std::map<std::string, InvocationLeaseState> leases;
  std::priority_queue<
      LeaseExpiry,
      std::vector<LeaseExpiry>,
      std::greater<LeaseExpiry>> leaseExpiries;
  std::map<std::string, uint64_t> nextArrivalOrder;
  LeaseConfig leaseConfig;
  ServerConfig serverConfig;
  WebSocketRegistry webSocketRegistry{serverConfig.webSocketsPerTenant};
  coworkerd::Membership& membership;
  capnp::HttpOverCapnpFactory& factory;
  const kj::HttpHeaderTable& headers;
  kj::Timer& timer;
  // The load check: when a version last loaded on a process (process:generation:hash#entrypoint).
  std::map<std::string, double> loadConfirmed;
  // Durable Objects: each object's requests in flight here and how many calls led to them (objectHops), by its key.
  std::map<std::string, ObjectActivity> objects;
  double lastObjectPrune = 0;
  uint64_t nextLoadCheckSequence = 1, loadChecks = 0, loadCheckColdLoads = 0, loadFailureReroutes = 0, lateBackendFailures = 0;
  std::map<std::string, uint64_t> loadFailures, loadFailureDetails;
  uint32_t inFlightRoots = 0;
  uint32_t inFlightNested = 0;
  uint64_t nextPlacementSequence = 1;
  uint64_t nextTreeSequence = 1;
  uint64_t nextTrialSequence = 1;
  uint64_t trialLoads = 0, trialProbes = 0, trialColdLoads = 0;
  std::map<std::string, uint64_t> trialFailures;
  std::deque<std::string> recentTrials;
  uint32_t proxyingIngress = 0;
  std::map<std::string, uint64_t> coldStarts, counterTotals;
  std::map<std::string, std::map<std::string, uint64_t>> decisionsByKind;
  std::deque<std::pair<double, std::string>> counterEvents;
  void observeCounter(const std::string& category, const std::string& target, const std::string& code) {
    const auto now = monotonicTimeMs();
    while (!counterEvents.empty() && counterEvents.front().first <= now - 60000) counterEvents.pop_front();
    auto key = category + "|" + target + "|" + code;
    ++counterTotals[key]; counterEvents.emplace_back(now, std::move(key));
  }
  std::string countersJson() const {
    std::map<std::string, uint64_t> window;
    auto now = monotonicTimeMs();
    for (const auto& [when, key] : counterEvents) if (when > now - 60000) ++window[key];
    std::ostringstream out;
    auto write = [&](const auto& values) {
      out << '{'; size_t index = 0;
      for (const auto& [key, count] : values) {
        if (index++) out << ',';
        out << "\"" << jsonEscape(key) << "\":" << count;
      }
      out << '}';
    };
    out << "{\"windowMs\":60000,\"total\":"; write(counterTotals);
    out << ",\"window60s\":"; write(window); out << '}';
    return out.str();
  }
  std::map<std::string, double> lastExecution;
  std::map<std::string, uint64_t> placementDecisions, placementReroutes, placementRejections, placementRejectionReasons;
  std::deque<double> decisionTimes;
  std::mt19937_64 random{std::random_device{}()};
  uint64_t droppedExecutions = 0;
  uint64_t nextChildSequencesSize = 0;
  uint64_t expiredLeases = 0;
  uint64_t expiredLeaseRejections = 0;
  uint64_t renewedLeases = 0;
  // The current log event of each invocation or object (nextLogEvent, chargeLogLine), and the object of each request to
  // an object, with when its lines last came.
  std::map<std::string, LogBudget> logEvents;
  std::map<std::string, std::pair<std::string, double>> logObjects;
  uint64_t logEventsOverBudget = 0;
  uint64_t leaseReaperRuns = 0;
  uint64_t sourceFetches = 0;
  uint64_t nextSequence = 1;
  uint64_t nextDebitSequence = 1;
  uint64_t budgetRejections = 0;
  uint64_t orderRejections = 0;
  uint64_t forwardedInvocations = 0;
  uint64_t peerUnavailableRejections = 0;
  uint64_t versionUnavailableRejections = 0;
  uint64_t secretUnavailableRejections = 0;
  mutable std::map<coworkerd::DeploymentState::Selection::Kind, uint64_t> routeRejections;
  std::map<std::string, std::array<AssetCounters, 3>> assetStats;
  static std::string assetCountersJson(const AssetCounters& c) {
    return "{\"requests\":" + std::to_string(c.requests) + ",\"hits\":" + std::to_string(c.hits) +
        ",\"misses\":" + std::to_string(c.misses) + ",\"fallbacks\":" + std::to_string(c.fallbacks) +
        ",\"notModified\":" + std::to_string(c.notModified) + ",\"bytes\":" + std::to_string(c.bytes) +
        ",\"toWorker\":" + std::to_string(c.toWorker) + ",\"notFound\":" + std::to_string(c.notFound) +
        ",\"methodNotAllowed\":" + std::to_string(c.methodNotAllowed) + ",\"unavailable\":" + std::to_string(c.unavailable) +
        ",\"runWorkerFirst\":" + std::to_string(c.runWorkerFirst) + ",\"excluded\":" + std::to_string(c.excluded) +
        ",\"navigationFallbacks\":" + std::to_string(c.navigationFallbacks) + '}';
  }
  // Per Worker: the totals, then the same counters by source.
  std::string assetsJson() const {
    std::string out = "{";
    for (const auto& [id, sources] : assetStats) {
      AssetCounters total;
      for (const auto& c : sources) {
        total.requests += c.requests; total.hits += c.hits; total.misses += c.misses; total.fallbacks += c.fallbacks;
        total.notModified += c.notModified; total.bytes += c.bytes; total.toWorker += c.toWorker; total.notFound += c.notFound;
        total.methodNotAllowed += c.methodNotAllowed; total.unavailable += c.unavailable; total.runWorkerFirst += c.runWorkerFirst;
        total.excluded += c.excluded; total.navigationFallbacks += c.navigationFallbacks;
      }
      if (out.size() > 1) out += ',';
      auto totals = assetCountersJson(total);
      totals.pop_back();
      out += "\"" + jsonEscape(id) + "\":" + totals + ",\"sources\":{\"ingress\":" + assetCountersJson(sources[0]) +
          ",\"binding\":" + assetCountersJson(sources[1]) + ",\"assetsBinding\":" + assetCountersJson(sources[2]) + "}}";
    }
    return out + '}';
  }
  std::string routeRejectionsJson() const {
    using Kind = coworkerd::DeploymentState::Selection::Kind;
    auto count = [&](Kind kind) { auto found = routeRejections.find(kind); return found == routeRejections.end() ? 0 : found->second; };
    return "{\"ROUTE_NOT_FOUND\":" + std::to_string(count(Kind::NOT_FOUND)) + ",\"ROUTE_INVALID\":" +
        std::to_string(count(Kind::ROUTE_INVALID)) + ",\"WORKER_INVALID\":" + std::to_string(count(Kind::WORKER_INVALID)) + '}';
  }
  uint64_t versionUnknownRejections = 0;
  uint64_t versionInconsistentRejections = 0;
  uint64_t versionCodeRejections = 0;
  uint64_t versionPinMismatchRejections = 0;
  uint64_t placementHopRejections = 0;
  uint64_t receivedPeerInvocations = 0;
  uint64_t remoteInvocationBytes = 0;
  size_t remoteInvocationPinCount = 0;
  std::map<std::string, uint64_t> peerOrigins;
  std::string lastRejection;
  // The Server's exit.
  static constexpr const char* DRAIN_CUT = "coworkerd server drain deadline: request cut";
  DrainRecord drain;
  std::function<void(std::string)> drainRequested;
  uint32_t ingressRequests = 0;
  kj::Own<kj::PromiseFulfiller<void>> cutoffFulfiller;
  kj::ForkedPromise<void> cutoff = [this] {
    auto paf = kj::newPromiseAndFulfiller<void>();
    cutoffFulfiller = kj::mv(paf.fulfiller);
    return paf.promise.fork();
  }();
  std::vector<kj::Own<kj::PromiseFulfiller<std::string>>> drainWaiters;
  std::string serverBinaryPath, serverBinarySha256;
  uint64_t serverStartedAtUnixMs = wallTimeMsForTest();
  // The effective configuration (secrets shown as set or not) and the refused management requests.
  std::string configuration = "null", launch = "null";
  // The tenants' records and usage (set by run()).
  coworkerd::TenantView* tenants = nullptr;
  coworkerd::TenantUsage* usage = nullptr;
  std::map<std::string, uint64_t> managementUnauthorized;
};

class LeaseRejectionHttpService final: public kj::HttpService {
public:
  LeaseRejectionHttpService(
      const kj::HttpHeaderTable& headerTable,
      uint status,
      std::string statusText,
      std::string body)
      : headerTable(headerTable),
        status(status),
        statusText(std::move(statusText)),
        body(std::move(body)) {}

  kj::Promise<void> request(
      kj::HttpMethod,
      kj::StringPtr,
      const kj::HttpHeaders&,
      kj::AsyncInputStream&,
      Response& response) override {
    return respond(response);
  }

  kj::Promise<void> respond(Response& response) {
    kj::HttpHeaders headers(headerTable);
    headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
    auto output = response.send(status, statusText.c_str(), headers, body.size());
    co_await output->write(kj::arrayPtr(
        reinterpret_cast<const kj::byte*>(body.data()), body.size()));
  }

private:
  const kj::HttpHeaderTable& headerTable;
  uint status;
  std::string statusText;
  std::string body;
};

class LeaseRejectionEventDispatcher final: public rpc::EventDispatcher::Server {
public:
  LeaseRejectionEventDispatcher(
      capnp::HttpOverCapnpFactory& factory,
      const kj::HttpHeaderTable& headerTable,
      uint status,
      std::string statusText,
      std::string body)
      : factory(factory),
        headerTable(headerTable),
        status(status),
        statusText(std::move(statusText)),
        body(std::move(body)) {}

  kj::Promise<void> getHttpService(GetHttpServiceContext context) override {
    auto service = kj::heap<LeaseRejectionHttpService>(
        headerTable, status, statusText, body);
    context.initResults(capnp::MessageSize{4, 1})
        .setHttp(factory.kjToCapnp(kj::mv(service)));
    return kj::READY_NOW;
  }

private:
  capnp::HttpOverCapnpFactory& factory;
  const kj::HttpHeaderTable& headerTable;
  uint status;
  std::string statusText;
  std::string body;
};

std::string rejectionJson(
    const std::string& code, const std::string& reason, double expiresAtMonotonicMs) {
  std::ostringstream body;
  body << std::setprecision(17)
       << "{\"error\":\""
       << (code == "CPU_LIMIT_EXCEEDED" ? "CPU limit exceeded during execution" :
           code == "MEMORY_LIMIT_EXCEEDED" ? "Memory limit exceeded during execution" :
           code == "RESULT_UNCERTAIN" ? "execution result uncertain" :
           "execution rejected before start") << "\","
       << "\"reason\":\"" << jsonEscape(cleanReason(reason)) << "\",\"code\":\""
       << jsonEscape(code) << "\",\"observedAtUnixMs\":"
       << wallTimeMsForTest() << ",\"expiresAtMonotonicMs\":"
       << expiresAtMonotonicMs << "}";
  return body.str();
}

rpc::EventDispatcher::Client makeRejectionDispatcher(
    capnp::HttpOverCapnpFactory& factory,
    const kj::HttpHeaderTable& headerTable,
    uint status,
    std::string statusText,
    std::string code,
    std::string reason,
    double expiresAtMonotonicMs = 0) {
  return rpc::EventDispatcher::Client(kj::heap<LeaseRejectionEventDispatcher>(
      factory, headerTable, status, std::move(statusText),
      rejectionJson(code, reason, expiresAtMonotonicMs)));
}

const char* statusText(uint status) {
  switch (status) {
    case 200: return "OK";
    case 400: return "Bad Request";
    case 401: return "Unauthorized";
    case 403: return "Forbidden";
    case 404: return "Not Found";
    case 405: return "Method Not Allowed";
    case 409: return "Conflict";
    case 410: return "Gone";
    case 413: return "Payload Too Large";
    case 429: return "Too Many Requests";
    case 500: return "Internal Server Error";
    case 508: return "Loop Detected";
    default: return "Service Unavailable";
  }
}

// A Host header's host (RFC 3986 section 3.2.2): an IP literal keeps its brackets ([::1]:8080 is [::1]), a name or an
// IPv4 address ends at the port's ':'.
std::string hostWithoutPort(const std::string& host) {
  if (host.starts_with('[')) {
    const auto close = host.find(']');
    return close == std::string::npos ? host : host.substr(0, close + 1);
  }
  const auto colon = host.find(':');
  return colon == std::string::npos ? host : host.substr(0, colon);
}

std::string refusalBody(const InvocationRejection& refusal, double deadline) {
  auto body = refusal.json();
  body.pop_back();
  body += std::string(",\"error\":\"") + (refusal.code == "RESULT_UNCERTAIN" ? "execution result uncertain" : "execution rejected before start") + "\",\"observedAtUnixMs\":" +
      std::to_string(wallTimeMsForTest()) + ",\"expiresAtMonotonicMs\":" + std::to_string(deadline) + "}";
  return body;
}

rpc::EventDispatcher::Client rejectedDispatcher(capnp::HttpOverCapnpFactory& factory,
    const kj::HttpHeaderTable& headers, const InvocationRejection& refusal, double deadline) {
  return rpc::EventDispatcher::Client(kj::heap<LeaseRejectionEventDispatcher>(
      factory, headers, refusal.status, statusText(refusal.status), refusalBody(refusal, deadline)));
}

void writeRefusal(rpc::InvocationOpenResult::Builder result, const InvocationRejection& value) {
  auto refusal = result.initRejected();
  refusal.setCode(value.code.c_str()); refusal.setReason(cleanReason(value.reason).c_str());
  refusal.setLoadBand(value.loadBand.c_str()); refusal.setInFlight(value.inFlight);
  refusal.setLimit(value.limit); refusal.setAvoidMs(value.avoidMs); refusal.setStatus(value.status);
  if (!value.category.empty()) refusal.setCategory(value.category.c_str());
}

InvocationRejection readRefusal(rpc::InvocationRejection::Reader value) {
  return {value.getStatus(), value.getCode().cStr(), value.getReason().cStr(), value.getLoadBand().cStr(),
      value.getInFlight(), value.getLimit(), value.getAvoidMs(), value.getCategory().cStr()};
}

// One relay of an inbound WebSocket connection (WebSocketRegistry): `outer` toward the client -- the client itself at the
// ingress, the ingress node at the node that executes it -- and `inner` toward the Worker -- the executing node, or
// workerd. Text and binary messages and close frames pass on as they come, each direction in order (fragments, pings and
// pongs are KJ's own); a message is at most MAX_MESSAGE_BYTES, workerd's default. A close frame from one side goes to
// the other, which has CLOSE_GRACE_MS to answer with its own; without an answer the relay answers the first side itself
// and ends. A side that ends without a close frame ends the other abruptly, so its peer sees an abnormal closure (1006):
// the client vanished, or the node the connection runs on can no longer be reached. On the executing node a Worker side
// that fails -- a limit ended the request, its process exited -- closes the ingress side with 1011 and the code an HTTP
// answer would carry (CPU_LIMIT_EXCEEDED, MEMORY_LIMIT_EXCEEDED, RESULT_UNCERTAIN). goAway() closes both sides with 1001.
// One [websocket] line when the relay starts and one when it ends.
class WebSocketRelay final: public WebSocketRegistry::Relay {
public:
  static constexpr size_t MAX_MESSAGE_BYTES = 32u << 20;
  static constexpr uint32_t CLOSE_GRACE_MS = 2000;
  // The close capnp's HTTP-over-capnp sends on a WebSocket whose request failed with OVERLOADED: workerd ends a request
  // that way when a limit ended it.
  static constexpr const char* OVERLOAD_REASON = "Service overloaded; retry later.";
  struct Identity {
    std::string nodeId, workerId, versionId, treeId, invocationId;
    // The other node of the connection: the executing one at the ingress (empty while it is this one), the ingress one
    // at the executing node.
    std::string peerNodeId;
    bool forwarded = false;
    std::string protocol;
    double handshakeMs = 0;
  };
  struct Hooks {
    std::function<void()> message;           // each message toward the Worker
    std::function<std::string()> limitCode;  // the code a limit ended the request with, "" when none did
    std::function<void()> ended;
  };
  WebSocketRelay(WebSocketRegistry& registry, kj::Timer& timer, WebSocketRegistry::Role role, Identity identity,
      kj::Own<kj::WebSocket> outerSocket, kj::Own<kj::WebSocket> innerSocket, Hooks hooks)
      : registry(registry), timer(timer), identity(std::move(identity)), hooks(std::move(hooks)),
        outer(std::move(outerSocket)), inner(std::move(innerSocket)), outerWriter(*outer), innerWriter(*inner) {
    this->role = role;
    this->tenant = coworkerd::tenantOf(this->identity.workerId);
  }
  ~WebSocketRelay() noexcept(false) {
    if (!started) return;
    // Ended from outside before its own end: the request it runs in was canceled -- at the executing node, because the
    // ingress side went away (its peer session broke).
    if (!runEnded && cause.empty()) {
      cause = role == WebSocketRegistry::Role::EXECUTION ? "ingress-lost" : "cancelled";
      if (direction.empty()) direction = "client";
    }
    registry.closed(*this, ended());
    log("close");
    if (hooks.ended) hooks.ended();
  }
  void setPlace(std::string processId, uint64_t generation) { this->processId = std::move(processId); this->generation = generation; }
  // Until both directions have ended -- each with its close frame, or one abruptly --, or the grace after the first
  // close ran out.
  kj::Promise<void> run() {
    started = true;
    startedAt = monotonicTimeMs();
    registry.opened(*this);
    log("open");
    co_await kj::joinPromises(kj::arr(pump(true), pump(false))).exclusiveJoin(end.addBranch());
    runEnded = true;
  }
  void goAway(const std::string& why) override {
    if (closing) return;
    nodeClose(1001, why == "drain" ? "the node is draining" : "the process is being replaced", why);
  }
  // The executing node: the request toward workerd ended -- its failure, if it failed.
  void innerSettled(kj::Maybe<kj::Exception> failure) {
    innerFailed = failure != kj::none;
    if (innerDoneFulfiller->isWaiting()) innerDoneFulfiller->fulfill();
  }

private:
  // Sends on one socket in order, one at a time (a pump and goAway() both send), nothing after its close.
  class Writer {
  public:
    explicit Writer(kj::WebSocket& socket): socket(socket) {}
    kj::Promise<void> send(kj::WebSocket::Message message) {
      if (closed) return kj::READY_NOW;
      return enqueue([this, message = kj::mv(message)]() mutable -> kj::Promise<void> {
        KJ_SWITCH_ONEOF(message) {
          KJ_CASE_ONEOF(text, kj::String) { return socket.send(text).attach(kj::mv(text)); }
          KJ_CASE_ONEOF(bytes, kj::Array<kj::byte>) { return socket.send(bytes).attach(kj::mv(bytes)); }
          KJ_CASE_ONEOF(close, kj::WebSocket::Close) { KJ_FAIL_ASSERT("a close frame goes through close()"); }
        }
        KJ_UNREACHABLE;
      });
    }
    kj::Promise<void> close(uint16_t code, std::string reason) {
      if (closed) return kj::READY_NOW;
      closed = true;
      closeCode = code;
      closeReason = reason;
      return enqueue([this, code, reason = kj::str(reason.c_str())]() mutable { return socket.close(code, reason).attach(kj::mv(reason)); });
    }
    bool hasClosed() const { return closed; }
    uint16_t sentCode() const { return closeCode; }
    const std::string& sentReason() const { return closeReason; }
  private:
    template <typename Step>
    kj::Promise<void> enqueue(Step&& step) {
      auto next = last.addBranch().then(kj::fwd<Step>(step)).fork();
      last = kj::mv(next);
      return last.addBranch();
    }
    kj::WebSocket& socket;
    kj::ForkedPromise<void> last = kj::Promise<void>(kj::READY_NOW).fork();
    bool closed = false;
    uint16_t closeCode = 0;
    std::string closeReason;
  };

  // One direction: toward the Worker reads `outer` and writes `inner`, toward the client the other way round.
  kj::Promise<void> pump(bool towardWorker) {
    auto& from = towardWorker ? *outer : *inner;
    auto& to = towardWorker ? innerWriter : outerWriter;
    while (true) {
      kj::Maybe<kj::WebSocket::Message> received;
      bool failed = false;
      try {
        received = co_await from.receive(MAX_MESSAGE_BYTES);
      } catch (const kj::Exception&) {
        failed = true;
      }
      if (failed) { co_await sideFailed(!towardWorker); co_return; }
      auto message = kj::mv(KJ_ASSERT_NONNULL(received));
      if (message.is<kj::WebSocket::Close>()) {
        auto& close = message.get<kj::WebSocket::Close>();
        co_await closeFrom(towardWorker, close.code, close.reason.cStr());
        co_return;
      }
      ++(towardWorker ? toWorker : toClient);
      if (towardWorker && hooks.message) hooks.message();
      bool sent = true;
      try {
        co_await to.send(kj::mv(message));
      } catch (const kj::Exception&) {
        sent = false;
      }
      if (!sent) { co_await sideFailed(towardWorker); co_return; }
    }
  }

  // A close frame from the client side (towardWorker) or the Worker side, passed on to the other; at the executing node
  // the close capnp sends for a request a limit ended becomes 1011 with the limit's code.
  kj::Promise<void> closeFrom(bool towardWorker, uint16_t code, std::string reason) {
    if (direction.empty()) { direction = towardWorker ? "client" : "worker"; firstCode = code; firstReason = reason; }
    (towardWorker ? outerClosed : innerClosed) = true;
    if (!towardWorker && role == WebSocketRegistry::Role::EXECUTION && code == 1013 && reason == OVERLOAD_REASON) {
      auto limit = co_await failureCode(false);
      if (!limit.empty()) { code = 1011; reason = limit; cause = "limit"; failure = limit; }
    }
    beginClosing();
    try {
      co_await (towardWorker ? innerWriter : outerWriter).close(code, reason);
    } catch (const kj::Exception&) {}
  }

  // A side ended without a close frame (its socket failed). The other ends abruptly, except at the executing node, where a
  // failed Worker side closes the ingress side with 1011.
  kj::Promise<void> sideFailed(bool innerSide) {
    if ((innerSide ? innerBroken : outerBroken)) co_return;
    (innerSide ? innerBroken : outerBroken) = true;
    if (innerSide && role == WebSocketRegistry::Role::EXECUTION && !outerBroken && !outerWriter.hasClosed()) {
      auto code = co_await failureCode(true);
      if (direction.empty()) direction = "node";
      if (cause.empty()) cause = code == "RESULT_UNCERTAIN" ? "worker-lost" : "limit";
      failure = code;
      beginClosing();
      try { co_await outerWriter.close(1011, code); } catch (const kj::Exception&) {}
      co_return;
    }
    if (cause.empty()) {
      cause = innerSide ? (identity.forwarded ? "peer-lost" : "worker-lost") : (role == WebSocketRegistry::Role::INGRESS ? "client-lost" : "ingress-lost");
      if (direction.empty()) direction = innerSide ? "worker" : "client";
    }
    finish();
  }

  // The code a failed Worker side ends the connection with: the limit that ended the request, else RESULT_UNCERTAIN. The
  // request toward workerd ends after its socket (capnp closes the socket, then rethrows), so its outcome is awaited a
  // little when no limit row is there yet.
  kj::Promise<std::string> failureCode(bool failed) {
    auto code = hooks.limitCode ? hooks.limitCode() : std::string();
    if (code.empty()) {
      co_await innerDone.addBranch().exclusiveJoin(timer.afterDelay(1000 * kj::MILLISECONDS));
      code = hooks.limitCode ? hooks.limitCode() : std::string();
    }
    if (code.empty() && (failed || innerFailed)) code = "RESULT_UNCERTAIN";
    co_return code;
  }

  // The node ends the connection: both sides get `code` (1001 going away).
  void nodeClose(uint16_t code, const std::string& reason, const std::string& why) {
    if (direction.empty()) { direction = "node"; firstCode = code; firstReason = reason; }
    if (cause.empty()) cause = why;
    beginClosing();
    if (!outerBroken) tasks.add(outerWriter.close(code, reason).catch_([](kj::Exception&&) {}));
    if (!innerBroken) tasks.add(innerWriter.close(code, reason).catch_([](kj::Exception&&) {}));
  }

  // After the first close frame the other side has the grace to answer; then the relay answers what is still open with the
  // first side's code and ends.
  void beginClosing() {
    if (closing) return;
    closing = true;
    grace = timer.afterDelay(CLOSE_GRACE_MS * kj::MILLISECONDS).then([this]() -> kj::Promise<void> {
      if (cause.empty()) cause = "grace";
      auto answer = [this](Writer& writer, bool broken) -> kj::Promise<void> {
        if (broken || writer.hasClosed()) return kj::READY_NOW;
        return writer.close(firstCode ? firstCode : 1000, firstReason).catch_([](kj::Exception&&) {});
      };
      return kj::joinPromises(kj::arr(answer(outerWriter, outerBroken), answer(innerWriter, innerBroken)))
          .exclusiveJoin(timer.afterDelay(200 * kj::MILLISECONDS));
    }).then([this]() { finish(); }).eagerlyEvaluate(nullptr);
  }
  void finish() { if (endFulfiller->isWaiting()) endFulfiller->fulfill(); }

  WebSocketRegistry::Ended ended() const {
    WebSocketRegistry::Ended out;
    // The code of the connection as the outer side has it: the one it sent first, else the one it was sent, else none.
    out.code = direction == "client" && outerClosed ? firstCode : outerWriter.hasClosed() ? outerWriter.sentCode() : 1006;
    if (out.code == 0) out.code = 1006;
    out.direction = direction.empty() ? "node" : direction;
    out.cause = cause;
    out.toWorker = toWorker;
    out.toClient = toClient;
    return out;
  }
  void log(const char* event) const {
    std::ostringstream out;
    out << std::setprecision(17) << "[websocket] {\"event\":\"" << event << "\",\"nodeId\":\"" << jsonEscape(identity.nodeId)
        << "\",\"role\":\"" << WebSocketRegistry::roleName(role) << "\",\"tenant\":\"" << jsonEscape(tenant)
        << "\",\"workerId\":\"" << jsonEscape(identity.workerId) << "\",\"versionId\":\"" << jsonEscape(identity.versionId)
        << "\",\"treeId\":\"" << jsonEscape(identity.treeId) << "\",\"invocationId\":\"" << jsonEscape(identity.invocationId)
        << "\",\"peerNodeId\":\"" << jsonEscape(identity.peerNodeId) << "\",\"forwarded\":" << (identity.forwarded ? "true" : "false");
    if (role == WebSocketRegistry::Role::EXECUTION)
      out << ",\"processId\":\"" << jsonEscape(processId) << "\",\"processGeneration\":" << generation;
    if (std::string_view(event) == "open") {
      out << ",\"protocol\":\"" << jsonEscape(identity.protocol) << "\",\"handshakeMs\":" << identity.handshakeMs;
    } else {
      const auto e = ended();
      const auto& reason = direction == "client" && outerClosed ? firstReason : outerWriter.hasClosed() ? outerWriter.sentReason() : std::string();
      out << ",\"direction\":\"" << e.direction << "\",\"code\":" << e.code << ",\"reason\":\"" << jsonEscape(cleanReason(reason))
          << "\",\"cause\":\"" << e.cause << "\",\"messages\":{\"toWorker\":" << e.toWorker
          << ",\"toClient\":" << e.toClient << "},\"durationMs\":" << monotonicTimeMs() - startedAt;
    }
    out << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}";
    std::cout << out.str() << std::endl;
  }
  WebSocketRegistry& registry;
  kj::Timer& timer;
  Identity identity;
  Hooks hooks;
  kj::Own<kj::WebSocket> outer, inner;
  Writer outerWriter, innerWriter;
  struct Errors final: public kj::TaskSet::ErrorHandler { void taskFailed(kj::Exception&&) override {} } errors;
  kj::TaskSet tasks{errors};
  static kj::ForkedPromise<void> fulfilledBy(kj::Own<kj::PromiseFulfiller<void>>& fulfiller) {
    auto paf = kj::newPromiseAndFulfiller<void>();
    fulfiller = kj::mv(paf.fulfiller);
    return paf.promise.fork();
  }
  kj::Own<kj::PromiseFulfiller<void>> endFulfiller, innerDoneFulfiller;
  kj::ForkedPromise<void> end = fulfilledBy(endFulfiller), innerDone = fulfilledBy(innerDoneFulfiller);
  kj::Promise<void> grace = nullptr;
  bool started = false, runEnded = false, closing = false, outerClosed = false, innerClosed = false, outerBroken = false,
      innerBroken = false, innerFailed = false;
  double startedAt = 0;
  uint64_t toWorker = 0, toClient = 0;
  // Who closed first (client, worker, node), with what; why the node ended it (drain, rotation, limit, peer-lost, ...);
  // the code a failed Worker side closed the ingress side with.
  std::string direction, cause, failure, firstReason;
  uint16_t firstCode = 0;
};

class TrackingHttpResponse final: public kj::HttpService::Response {
public:
  explicit TrackingHttpResponse(Response& inner, SharedPathTiming timing = {}): inner(inner), timing(std::move(timing)) {}

  kj::Own<kj::AsyncOutputStream> send(
      uint status, kj::StringPtr statusText, const kj::HttpHeaders& headers,
      kj::Maybe<uint64_t> expectedBodySize = kj::none) override {
    pathMark(timing, "workerd-headers");
    auto body = inner.send(status, statusText, headers, expectedBodySize);
    started = true;
    sentStatus = status;
    return body;
  }

  kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders& headers) override {
    auto socket = inner.acceptWebSocket(headers);
    started = true;
    sentStatus = 101;
    return socket;
  }

  bool hasStarted() const { return started; }
  uint status() const { return sentStatus; }

private:
  Response& inner;
  SharedPathTiming timing;
  bool started = false;
  uint sentStatus = 0;
};

void phaseTiming(const char* stage, double started, const std::shared_ptr<coworkerd::ProcessCall>& call) {
  if (std::getenv("COWORKERD_TIMING") == nullptr) return;
  std::cout << "[phase-time] {\"stage\":\"" << stage << "\",\"elapsedMicros\":"
      << std::setprecision(17) << (monotonicTimeMs() - started) * 1000;
  if (call) std::cout << ",\"invocationId\":\"" << call->invocationId << "\",\"treeId\":\"" << call->treeId
      << "\",\"processId\":\"" << call->processId << "\",\"processGeneration\":" << call->generation;
  std::cout << "}" << std::endl;
}
// A response header by name, whatever its case ("" when absent).
std::string headerValue(const kj::HttpHeaders& headers, std::string_view name) {
  std::string value;
  headers.forEach([&](kj::StringPtr key, kj::StringPtr text) {
    if (value.empty() && key.size() == name.size() &&
        std::equal(name.begin(), name.end(), key.begin(), [](char a, char b) { return std::tolower(a) == std::tolower(b); }))
      value = text.cStr();
  });
  return value;
}

// The response of an ingress root on the node that executes it -- this node's ingress or another's --, when the request
// asks for a WebSocket. When the Worker accepts it (101), the response's WebSocket toward the ingress and one end of a
// pipe, whose other end goes back toward workerd, become the connection's relay on this node (WebSocketRelay): the
// connection holds its call tree's lease, renewed at each message toward the Worker, and gives its admission slot back
// at once -- between two messages it is not executing --; a limit that ends the request closes it with 1011 and the
// limit's code. Otherwise it is an HTTP response like TrackingHttpResponse's.
class ExecutionResponse final: public kj::HttpService::Response {
public:
  ExecutionResponse(Response& inner, std::shared_ptr<AdmissionSlot> slot)
      : inner(inner), slot(std::move(slot)), startedAt(monotonicTimeMs()) {}
  kj::Own<kj::AsyncOutputStream> send(
      uint status, kj::StringPtr statusText, const kj::HttpHeaders& headers,
      kj::Maybe<uint64_t> expectedBodySize = kj::none) override {
    pathMark(slot->timing, "workerd-headers");
    auto body = inner.send(status, statusText, headers, expectedBodySize);
    started = true;
    return body;
  }
  kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders& headers) override {
    pathMark(slot->timing, "workerd-headers");
    auto ingress = inner.acceptWebSocket(headers);
    started = true;
    auto& server = *slot->server;
    auto process = slot->process;
    KJ_ASSERT(process != nullptr, "a WebSocket answered without a process call");
    WebSocketRelay::Identity identity;
    identity.nodeId = server.placementConfig().serverId;
    identity.workerId = process->workerId; identity.versionId = process->versionId;
    identity.treeId = process->treeId; identity.invocationId = process->invocationId;
    identity.peerNodeId = slot->ingressNodeId;
    identity.forwarded = !slot->ingressNodeId.empty() && slot->ingressNodeId != identity.nodeId;
    identity.protocol = headerValue(headers, "sec-websocket-protocol");
    identity.handshakeMs = monotonicTimeMs() - startedAt;
    const auto invocation = process->invocationId;
    WebSocketRelay::Hooks hooks;
    hooks.message = [&server, invocation]() {
      server.renewLease(invocation);
      server.nextLogEvent(invocation);
    };
    hooks.limitCode = [process]() -> std::string {
      if (process->memoryLimitExceeded()) return "MEMORY_LIMIT_EXCEEDED";
      if (process->cpuLimitExceeded()) return "CPU_LIMIT_EXCEEDED";
      return {};
    };
    hooks.ended = [&server, invocation]() { server.releaseLease(invocation); };
    slot->report(101, "");
    auto pipe = kj::newWebSocketPipe();
    relay = kj::heap<WebSocketRelay>(server.webSockets(), server.clock(), WebSocketRegistry::Role::EXECUTION, std::move(identity),
        kj::mv(ingress), kj::mv(pipe.ends[1]), std::move(hooks));
    relay->setPlace(process->processId, process->generation);
    server.holdLease(invocation);
    // Between two messages the connection is not executing: the node's admission count and the process's calls in flight
    // leave it; its request context stays open in workerd until the connection ends.
    slot->finish();
    running = relay->run().fork();
    return kj::mv(pipe.ends[0]);
  }
  bool hasStarted() const { return started; }
  bool upgraded() const { return relay.get() != nullptr; }
  // The request toward workerd ended (its failure, if it failed): the relay may still be closing the connection.
  kj::Promise<void> settled(kj::Maybe<kj::Exception> failure) {
    relay->innerSettled(kj::mv(failure));
    return KJ_ASSERT_NONNULL(running).addBranch();
  }

private:
  Response& inner;
  std::shared_ptr<AdmissionSlot> slot;
  double startedAt;
  bool started = false;
  kj::Own<WebSocketRelay> relay;
  kj::Maybe<kj::ForkedPromise<void>> running;  // destroyed before the relay
};

class AdmissionHttpService final: public kj::HttpService {
public:
  AdmissionHttpService(kj::Own<kj::HttpService> inner, std::shared_ptr<AdmissionSlot> slot,
      const kj::HttpHeaderTable& headers): inner(std::move(inner)), slot(std::move(slot)), headers(headers) {}
  kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& requestHeaders,
      kj::AsyncInputStream& body, Response& response) override {
    auto finish = kj::defer([slot = slot] { slot->finish(); });
    // An ingress root's upgrade, or one to a Durable Object, may become a WebSocket connection relayed here.
    if ((slot->root || slot->durable) && slot->server != nullptr && method == kj::HttpMethod::GET && requestHeaders.isWebSocket())
      co_return co_await webSocketRequest(method, url, requestHeaders, body, response);
    auto tracked = kj::heap<TrackingHttpResponse>(response, slot->timing);
    auto started = monotonicTimeMs();
    std::string failure;
    try {
      pathMark(slot->timing, "http-dispatch");
      auto request = inner->request(method, url, requestHeaders, body, *tracked);
      if (slot->process) request = kj::mv(request).exclusiveJoin(slot->process->onExit());
      co_await kj::mv(request).attach(std::move(finish));
      phaseTiming("request-through-body", started, slot->process);
      pathMark(slot->timing, "workerd-end");
      if (slot->timing && slot->timing->kind == "binding") slot->timing->emit();
      slot->report(tracked->status(), limitCode());
      co_return;
    } catch (const kj::Exception& error) {
      if (tracked->hasStarted()) { slot->report(tracked->status(), limitCode()); throw; }
      failure = error.getDescription().cStr();
    }
    co_await refuse(*tracked, failure);
  }
private:
  // An ingress root that asks for a WebSocket. Once the Worker answered 101 the connection is the response: when the request
  // toward workerd ends, its relay finishes the closing (1011 and a code for a request a limit ended), and the request here
  // ends normally after it, so that every node on the way keeps the connection until its close frames have passed.
  kj::Promise<void> webSocketRequest(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& requestHeaders,
      kj::AsyncInputStream& body, Response& response) {
    auto tracked = kj::heap<ExecutionResponse>(response, slot);
    kj::Maybe<kj::Exception> failed;
    std::string failure;
    try {
      pathMark(slot->timing, "http-dispatch");
      auto request = inner->request(method, url, requestHeaders, body, *tracked);
      if (slot->process) request = kj::mv(request).exclusiveJoin(slot->process->onExit());
      co_await request;
    } catch (const kj::Exception& error) {
      failed = error.clone();
      failure = error.getDescription().cStr();
    }
    if (tracked->upgraded()) {
      co_await tracked->settled(kj::mv(failed));
      co_return;
    }
    KJ_IF_SOME(error, failed) {
      if (tracked->hasStarted()) kj::throwFatalException(kj::mv(error));
      co_await refuse(*tracked, failure);
    }
  }
  // A failure before the response started: the code the call's limits or its process say.
  kj::Promise<void> refuse(kj::HttpService::Response& tracked, const std::string& failure) {
    const auto code = limitCode();
    slot->report(503, code.empty() ? "RESULT_UNCERTAIN" : code);
    auto rejection = kj::heap<LeaseRejectionHttpService>(headers, 503, "Service Unavailable",
        rejectionJson(code.empty() ? "RESULT_UNCERTAIN" : code, failure, 0));
    co_await rejection->respond(tracked);
  }
  // The limit that ended the call, if one did ("" otherwise).
  std::string limitCode() const {
    if (slot->process && slot->process->memoryLimitExceeded()) return "MEMORY_LIMIT_EXCEEDED";
    if (slot->process && slot->process->cpuLimitExceeded()) return "CPU_LIMIT_EXCEEDED";
    return {};
  }
  kj::Own<kj::HttpService> inner;
  std::shared_ptr<AdmissionSlot> slot;
  const kj::HttpHeaderTable& headers;
};
class AdmissionDispatcher final: public rpc::EventDispatcher::Server {
public:
  AdmissionDispatcher(rpc::EventDispatcher::Client inner, std::shared_ptr<AdmissionSlot> slot,
      capnp::HttpOverCapnpFactory& factory, const kj::HttpHeaderTable& headers)
      : inner(std::move(inner)), slot(std::move(slot)), factory(factory), headers(headers) {}
  kj::Promise<void> getHttpService(GetHttpServiceContext context) override {
    const auto started = monotonicTimeMs();
    pathMark(slot->timing, "http-service");
    auto call = inner.getHttpServiceRequest().send();
    context.initResults().setHttp(factory.kjToCapnp(kj::heap<AdmissionHttpService>(
        factory.capnpToKj(call.getHttp()), slot, headers)));
    phaseTiming("in-flight-wrapper", started, slot->process);
    // Resolution failures travel through the pipelined service and are classified there.
    return kj::READY_NOW;
  }
private:
  rpc::EventDispatcher::Client inner;
  std::shared_ptr<AdmissionSlot> slot;
  capnp::HttpOverCapnpFactory& factory;
  const kj::HttpHeaderTable& headers;
};
rpc::EventDispatcher::Client trackAdmission(rpc::EventDispatcher::Client dispatcher,
    std::shared_ptr<AdmissionSlot> slot, capnp::HttpOverCapnpFactory& factory, const kj::HttpHeaderTable& headers) {
  return rpc::EventDispatcher::Client(kj::heap<AdmissionDispatcher>(std::move(dispatcher), std::move(slot), factory, headers));
}

struct InvocationAttempt {
  rpc::EventDispatcher::Client dispatcher = nullptr;
  std::optional<InvocationRejection> failure;
  bool retryable = false;
  bool connectionFailure = false;
};

struct RemoteInvocationData {
  std::string workerId;
  std::string versionId;
  std::string contentHash;
  std::string treeId;
  std::string parentInvocationId;
  uint16_t subrequestGrant;
  double expiresAtMonotonicMs;
  uint16_t depth;
  uint8_t placementHops;
  bool alreadyPlaced;
  std::string originServerId;
  uint64_t callSequence;
  uint64_t orderSequence;
  SharedVersionPins pins;
  std::string definitionHash;
  bool ingressRoot = false;
  std::string ingressServerId;
  std::string entrypoint;
};

class PeerConnection final {
public:
  PeerConnection(
      uint64_t generation,
      kj::Own<kj::NetworkAddress> address,
      kj::Own<kj::HttpClient> connector,
      kj::Own<kj::AsyncIoStream> stream,
      kj::Own<capnp::TwoPartyClient> rpcSystem,
      rpc::PeerServer::Client peer)
      : generation(generation),
        address(std::move(address)),
        connector(std::move(connector)),
        stream(std::move(stream)),
        rpcSystem(std::move(rpcSystem)),
        peer(std::move(peer)) {}

  rpc::PeerServer::Client& client() {
    return peer;
  }

  kj::Promise<void> onDisconnect() {
    return rpcSystem->onDisconnect();
  }

  bool isDisconnected() const {
    return disconnected;
  }

  bool markDisconnected() {
    if (disconnected) return false;
    disconnected = true;
    return true;
  }

  uint64_t getGeneration() const {
    return generation;
  }

private:
  uint64_t generation;
  kj::Own<kj::NetworkAddress> address;
  kj::Own<kj::HttpClient> connector;
  kj::Own<kj::AsyncIoStream> stream;
  kj::Own<capnp::TwoPartyClient> rpcSystem;
  rpc::PeerServer::Client peer;
  bool disconnected = false;
};

class AttachedHttpService final: public kj::HttpService {
public:
  AttachedHttpService(
      kj::Own<kj::HttpService> inner,
      kj::Rc<PeerConnection> connection,
      const kj::HttpHeaderTable& headerTable,
      std::string targetServerId,
      double expiresAtMonotonicMs)
      : inner(std::move(inner)), connection(std::move(connection)),
        headerTable(headerTable), targetServerId(std::move(targetServerId)),
        expiresAtMonotonicMs(expiresAtMonotonicMs) {}

  kj::Promise<void> request(
      kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody,
      Response& response) override {
    auto tracked = kj::heap<TrackingHttpResponse>(response);
    auto request = kj::evalNow([&]() {
      return inner->request(method, url, headers, requestBody, *tracked);
    });
    return request.catch_([this, ptr = tracked.get()](kj::Exception&& error)
        -> kj::Promise<void> {
      if (ptr->hasStarted()) return std::move(error);
      // This request has been dispatched. No failure here proves non-execution.
      auto reason = "peer request result uncertain node=" + targetServerId +
          " detail=" + error.getDescription().cStr();
      std::cerr << "[server] RESULT_UNCERTAIN: " << cleanReason(reason) << std::endl;
      auto rejection = kj::heap<LeaseRejectionHttpService>(
          headerTable, 503, "Service Unavailable",
          rejectionJson("RESULT_UNCERTAIN", reason, expiresAtMonotonicMs));
      return rejection->respond(*ptr).attach(std::move(rejection));
    }).attach(std::move(tracked), connection.addRef());
  }

private:
  kj::Own<kj::HttpService> inner;
  kj::Rc<PeerConnection> connection;
  const kj::HttpHeaderTable& headerTable;
  std::string targetServerId;
  double expiresAtMonotonicMs;
};

class PeerProxyEventDispatcher final: public rpc::EventDispatcher::Server {
public:
  PeerProxyEventDispatcher(
      rpc::EventDispatcher::Client remote,
      kj::Rc<PeerConnection> connection,
      capnp::HttpOverCapnpFactory& factory,
      const kj::HttpHeaderTable& headerTable,
      std::string targetServerId,
      double expiresAtMonotonicMs)
      : remote(std::move(remote)),
        connection(std::move(connection)),
        factory(factory), headerTable(headerTable),
        targetServerId(std::move(targetServerId)), expiresAtMonotonicMs(expiresAtMonotonicMs) {}

  kj::Promise<void> getHttpService(GetHttpServiceContext context) override {
    auto response = remote.getHttpServiceRequest().send();
    auto service = factory.capnpToKj(response.getHttp());
    auto attached = kj::heap<AttachedHttpService>(
        std::move(service), connection.addRef(), headerTable,
        targetServerId, expiresAtMonotonicMs);
    context.initResults(capnp::MessageSize{4, 1})
        .setHttp(factory.kjToCapnp(std::move(attached)));
    // The pipelined HTTP capability carries resolution failures into the service
    // wrapper, where pre-header failures can be classified as uncertain.
    return kj::READY_NOW;
  }

private:
  rpc::EventDispatcher::Client remote;
  kj::Rc<PeerConnection> connection;
  capnp::HttpOverCapnpFactory& factory;
  const kj::HttpHeaderTable& headerTable;
  std::string targetServerId;
  double expiresAtMonotonicMs;
};

class PeerForwarder final {
public:
  PeerForwarder(
      ServerCore& server,
      kj::Network& network,
      kj::Timer& timer,
      const kj::HttpHeaderTable& headerTable,
      capnp::HttpOverCapnpFactory& factory,
      coworkerd::PeerAuth& peerAuth)
      : server(server),
        network(network),
        timer(timer),
        headerTable(headerTable),
        factory(factory),
        peerAuth(peerAuth) {}

  std::string stateJson() const {
    std::ostringstream output;
    output << "{";
    size_t index = 0;
    for (const auto& [serverId, state] : peerStates) {
      if (index++ > 0) output << ',';
      bool connected = false;
      bool connecting = false;
      KJ_IF_SOME(connection, state.connection) {
        connected = !connection->isDisconnected();
      }
      KJ_IF_SOME(_promise, state.connecting) {
        connecting = !state.connectingSettled;
      }
      output << "\"" << jsonEscape(serverId) << "\":{";
      output << "\"connected\":" << (connected ? "true" : "false")
             << ",\"connecting\":" << (connecting ? "true" : "false")
             << ",\"connectAttempts\":" << state.connectAttempts
             << ",\"successfulConnections\":" << state.successfulConnections
             << ",\"disconnects\":" << state.disconnects
             << ",\"peerUnavailableResponses\":" << state.peerUnavailableResponses
             << "}";
    }
    output << "}";
    return output.str();
  }

  rpc::EventDispatcher::Client open(const std::string& targetServerId, RemoteInvocationData invocation) {
    const auto deadline = invocation.expiresAtMonotonicMs;
    auto promise = tryOpen(targetServerId, std::move(invocation)).then(
        [this, deadline, targetServerId](InvocationAttempt&& result) -> rpc::EventDispatcher::Client {
      server.observeStaticAttempt(targetServerId, result.failure);
      if (!result.failure) return std::move(result.dispatcher);
      return rejectedDispatcher(factory, headerTable, *result.failure, deadline);
    });
    return rpc::EventDispatcher::Client(std::move(promise));
  }

  kj::Promise<InvocationAttempt> tryOpen(std::string targetServerId, RemoteInvocationData invocation) {
    const auto& config = server.placementConfig();
    auto peer = server.member(targetServerId);
    if (peer == nullptr) {
      auto reason = "target is not in membership table (不在成员表): node=" + targetServerId;
      server.rejectPeerUnavailable(reason);
      co_return InvocationAttempt{nullptr, server.refusal("PEER_UNAVAILABLE", reason), true};
    }
    const auto identity = peer->address + "#" + peer->epoch;
    const auto address = peer->address;
    const auto epoch = peer->epoch;
    const auto deadline = invocation.expiresAtMonotonicMs;
    if (remainingLeaseMs(deadline) == 0) {
      server.rejectExpiredLease("lease expired before forwarding");
      co_return InvocationAttempt{nullptr, server.refusal("LEASE_EXPIRED", "lease expired before forwarding", 410), true};
    }
    kj::Maybe<kj::Rc<PeerConnection>> connected;
    try {
      connected = co_await getConnection(targetServerId, address, identity, config.peerTimeoutMs);
    } catch (const kj::Exception& error) {
      auto reason = "peer unavailable node=" + targetServerId + " detail=" + error.getDescription().cStr();
      server.rejectPeerUnavailable(reason);
      ++peerStates[targetServerId].peerUnavailableResponses;
      co_return InvocationAttempt{nullptr, server.refusal("PEER_UNAVAILABLE", reason), true, true};
    }
    auto connection = KJ_ASSERT_NONNULL(connected).addRef();
    auto current = server.member(targetServerId);
    // A watch update may observe re-registration while the connection opens.
    // Keep the captured epoch; the receiver knows whether this instance held it.
    if (current == nullptr || current->address != address) {
      auto reason = "target is not in membership table (不在成员表) or its address changed before dispatch: node=" + targetServerId;
      server.rejectPeerUnavailable(reason);
      co_return InvocationAttempt{nullptr, server.refusal("PEER_UNAVAILABLE", reason), true};
    }
    auto request = connection->client().openInvocationRequest();
    auto remote = request.initInvocation();
    remote.setWorkerId(invocation.workerId.c_str()); remote.setVersionId(invocation.versionId.c_str());
    remote.setContentHash(invocation.contentHash.c_str()); remote.setDefinitionHash(invocation.definitionHash.c_str());
    remote.setTreeId(invocation.treeId.c_str()); remote.setParentInvocationId(invocation.parentInvocationId.c_str());
    remote.setSubrequestGrant(invocation.subrequestGrant);
    remote.setDepth(invocation.depth); remote.setPlacementHops(invocation.placementHops);
    remote.setAlreadyPlaced(invocation.alreadyPlaced); remote.setOriginServerId(invocation.originServerId.c_str());
    remote.setIngressRoot(invocation.ingressRoot); remote.setIngressServerId(invocation.ingressServerId.c_str());
    remote.setEntrypoint(invocation.entrypoint.c_str());
    remote.setCallSequence(invocation.callSequence); remote.setOrderSequence(invocation.orderSequence);
    remote.setDestinationEpoch(epoch.c_str());
    auto pins = remote.initVersionPins(invocation.pins->size()); size_t index = 0;
    for (const auto& [worker, pin] : *invocation.pins) {
      pins[index].setWorkerId(pin.workerId.c_str()); pins[index].setVersionId(pin.versionId.c_str());
      pins[index].setContentHash(pin.contentHash.c_str()); pins[index++].setDefinitionHash(pin.definitionHash.c_str());
    }
    server.observeRemoteInvocationSize(remote.asReader().totalSize().wordCount * sizeof(capnp::word), invocation.pins->size());
    // Sample after building the message. Emit the diagnostic only once the peer
    // result arrives, so synchronous log flushing is not part of the wire delay.
    const auto remainingCapturedAtMonotonicMs = monotonicTimeMs();
    const auto remaining = std::max(0.0, deadline - remainingCapturedAtMonotonicMs);
    if (remaining == 0) {
      server.rejectExpiredLease("lease expired before forwarding");
      co_return InvocationAttempt{nullptr, server.refusal("LEASE_EXPIRED", "lease expired before forwarding", 410), true};
    }
    remote.setRemainingMs(remaining);
    auto recordSend = [&] {
      std::cout << "[peer-send] {\"nodeId\":\"" << targetServerId << "\",\"treeId\":\"" << invocation.treeId
                << "\",\"remainingMs\":" << std::setprecision(17) << remaining
                << ",\"capturedAtMonotonicMs\":" << remainingCapturedAtMonotonicMs
                << ",\"expiresAtMonotonicMs\":" << deadline
                << ",\"ingressRoot\":" << (invocation.ingressRoot ? "true" : "false")
                << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
    };
    try {
      // No capability is exposed to workerd until this explicit result arrives.
      auto started = monotonicTimeMs();
      auto response = co_await timer.timeoutAfter(config.peerTimeoutMs * kj::MILLISECONDS, request.send());
      recordSend();
      phaseTiming("peer-open-response", started);
      auto result = response.getResult();
      if (result.isRejected()) {
        auto refused = readRefusal(result.getRejected());
        const bool retry = rerouteAfter(refused);
        co_return InvocationAttempt{nullptr, std::move(refused), retry};
      }
      co_return InvocationAttempt{rpc::EventDispatcher::Client(kj::heap<PeerProxyEventDispatcher>(
          result.getAccepted(), connection.addRef(), factory, headerTable, targetServerId, deadline)), std::nullopt, false};
    } catch (const kj::Exception& error) {
      recordSend();
      markConnectionDisconnected(targetServerId, connection);
      auto reason = "peer open result uncertain node=" + targetServerId + " detail=" + error.getDescription().cStr();
      std::cerr << "[server] RESULT_UNCERTAIN: " << cleanReason(reason) << std::endl;
      // Once openInvocation has been sent, transport failure is never proof of non-execution.
      co_return InvocationAttempt{nullptr, server.refusal("RESULT_UNCERTAIN", reason), false};
    }
  }

  std::string avoidanceJson() const {
    std::ostringstream out; out << "["; size_t index = 0;
    for (const auto& [id, value] : avoidance) if (value.until > monotonicTimeMs()) {
      if (index++) out << ',';
      out << "{\"nodeId\":\"" << id << "\",\"untilMonotonicMs\":" << std::setprecision(17) << value.until
          << ",\"remainingMs\":" << value.until - monotonicTimeMs() << ",\"failures\":" << value.failures << "}";
    }
    out << "]"; return out.str();
  }

  // `forwarded`: the call went to another node; `placedOn`: the node it went to.
  kj::Promise<rpc::EventDispatcher::Client> place(std::shared_ptr<const VersionDefinition> version,
      ChildAllocation allocation, bool* forwarded = nullptr, std::string* placedOn = nullptr) {
    if (allocation.timing) allocation.timing->treeId = allocation.treeId;
    pathMark(allocation.timing, "placement-start");
    const auto& config = server.placementConfig();
    const auto decisionStart = monotonicTimeMs();
    const auto placementId = server.nextPlacementId();
    auto ranked = server.candidates(version->workerId);
    const auto home = ranked.empty() ? "" : ranked.front();
    // A draining Server, or one whose epoch expired by its own clock, is the last of its own candidates: Home, the
    // others, then itself.
    const bool lastResort = server.lastResort();
    const bool selfRanked = std::find(ranked.begin(), ranked.end(), config.serverId) != ranked.end();
    std::vector<std::string> ordered;
    if (!home.empty()) ordered.push_back(home);
    if (!lastResort && home != config.serverId && selfRanked) ordered.push_back(config.serverId);
    for (const auto& id : ranked) if (id != home && id != config.serverId) ordered.push_back(id);
    if (lastResort && home != config.serverId && selfRanked) ordered.push_back(config.serverId);
    if (forwarded) *forwarded = false;
    auto avoidedHome = avoidance.find(home);
    bool homeFailed = avoidedHome != avoidance.end() && avoidedHome->second.until > monotonicTimeMs();
    bool localFirst = !lastResort && selfRanked &&
        server.hot(version->definitionHash) &&
        !server.admission(version->workerId, version->definitionHash, allocation.ingressRoot, allocation.expiresAtMonotonicMs, false);
    if (localFirst && home != config.serverId && server.drawHome()) {
      localFirst = false; server.decision("probabilityHome", allocation.ingressRoot);
    } else if (localFirst) {
      server.decision(home == config.serverId ? "localHome" : "localHot", allocation.ingressRoot);
      ordered.erase(std::remove(ordered.begin(), ordered.end(), config.serverId), ordered.end());
      ordered.insert(ordered.begin(), config.serverId);
    }
    // Preserve Home -> local -> ranked order within both avoidance partitions.
    // A hot, admitted local version stays first; no candidate is removed.
    auto first = ordered.begin(); if (localFirst && first != ordered.end()) ++first;
    std::stable_partition(first, ordered.end(), [this](const auto& id) {
      auto found = avoidance.find(id); return found == avoidance.end() || found->second.until <= monotonicTimeMs();
    });
    server.decisionTiming(decisionStart);
    pathMark(allocation.timing, "placement-decision");
    server.recordForward(*version, allocation, ordered.empty() ? "" : ordered.front());
    std::string rankedCandidates = "[";
    for (const auto& id : ranked) {
      if (rankedCandidates.size() > 1) rankedCandidates += ',';
      rankedCandidates += '"' + jsonEscape(id) + '"';
    }
    rankedCandidates += ']';
    std::cout << "[allocation] {\"placementId\":\"" << placementId << "\",\"treeId\":\"" << allocation.treeId
              << "\",\"parentInvocationId\":\"" << allocation.parentInvocationId << "\",\"target\":\"" << version->key()
              << "\",\"grant\":" << allocation.grant << ",\"charged\":" << allocation.charged
              << ",\"remainingAfter\":" << allocation.remainingAfter << ",\"initialHops\":" << unsigned(config.initialPlacementHops)
              << ",\"kind\":\"" << (allocation.ingressRoot ? "ingress" : "binding") << "\",\"ingressNodeId\":\""
              << jsonEscape(allocation.ingressServerId) << "\""
              << ",\"rankedCandidates\":" << rankedCandidates
              << ",\"expiresAtMonotonicMs\":" << std::setprecision(17) << allocation.expiresAtMonotonicMs << "}" << std::endl;
    uint8_t hops = config.initialPlacementHops;
    pathMark(allocation.timing, "placement-logs");
    std::string failures = "["; size_t attempted = 0;
    bool packagesUnavailable = true;  // every candidate tried failed to load the package
    for (const auto& id : ordered) {
      if (hops == 0) break;
      --hops;
      InvocationAttempt result;
      const auto startedAt = wallTimeMsForTest();
      if (id == config.serverId) {
        pathMark(allocation.timing, "admission-start");
        auto refused = server.admission(version->workerId, version->definitionHash, allocation.ingressRoot, allocation.expiresAtMonotonicMs, false);
        pathMark(allocation.timing, "admission-end");
        if (refused) result = {nullptr, std::move(refused), true};
        else {
          auto slot = server.reserve(allocation.ingressRoot);
          pathMark(allocation.timing, "code-start");
          auto unavailable = co_await server.ensureCode(*version, allocation.expiresAtMonotonicMs);
          pathMark(allocation.timing, "code-end");
          if (!unavailable.empty()) {
            server.rejectVersionUnavailable(unavailable);
            result = {nullptr, server.refusal("VERSION_UNAVAILABLE", unavailable, 409), true};
          } else if (remainingLeaseMs(allocation.expiresAtMonotonicMs) == 0) {
            result = {nullptr, server.refusal("LEASE_EXPIRED", "lease expired before execution", 410), true};
          } else if (auto missing = server.secretRefusal(*version, allocation.ingressRoot)) {
            result = {nullptr, std::move(missing), true};
          } else if (auto check = co_await server.loadCheck(version, allocation.entrypoint, allocation.expiresAtMonotonicMs,
                         allocation.ingressRoot); check.refusal) {
            // Nothing of the call ran: another candidate only for a package this Server lacks.
            const bool retry = rerouteAfter(*check.refusal);
            result = {nullptr, std::move(check.refusal), retry};
          } else {
            try {
              auto opened = server.executeAllocated(version, allocation.ingressRoot ? "ingress" : "binding", allocation, std::move(slot), check);
              // Dispatch is committed. Pipeline HTTP now; its wrapper reports uncertainty.
              result = {std::move(opened.dispatcher), std::nullopt, false};
            } catch (const ProcessUnavailable& error) {
              result = {nullptr, server.processRefusal(error), true};
            } catch (const kj::Exception& error) {
              result = {nullptr, server.refusal("RESULT_UNCERTAIN", error.getDescription().cStr()), false};
            }
          }
        }
      } else {
        const auto& pin = allocation.pins->at(version->workerId);
        result = co_await tryOpen(id, {version->workerId, pin.versionId, pin.contentHash,
          allocation.treeId, allocation.parentInvocationId, allocation.grant, allocation.expiresAtMonotonicMs,
          allocation.depth, hops, true, config.serverId, allocation.callSequence, allocation.orderSequence,
          allocation.pins, pin.definitionHash, allocation.ingressRoot, allocation.ingressServerId, allocation.entrypoint});
      }
      server.observeAttempt(id, allocation.ingressRoot, result.failure);
      if (id == config.serverId && result.failure && result.retryable)
      {
        server.observeRefusal(*result.failure, allocation.ingressRoot ? "localIngress" : "localBinding");
        std::cout << "[refusal] {\"treeId\":\"" << allocation.treeId << "\",\"nodeId\":\"" << id
                  << "\",\"source\":\"" << (allocation.ingressRoot ? "localIngress" : "localBinding")
                  << "\",\"observedAtUnixMs\":" << wallTimeMsForTest()
                  << ",\"failure\":" << result.failure->json() << "}" << std::endl;
      }
      auto rank = std::find(ranked.begin(), ranked.end(), id) - ranked.begin() + 1;
      std::cout << "[attempt] {\"placementId\":\"" << placementId << "\",\"treeId\":\"" << allocation.treeId
                << "\",\"parentInvocationId\":\"" << allocation.parentInvocationId << "\",\"target\":\"" << version->key()
                << "\",\"nodeId\":\"" << id << "\",\"home\":\"" << home << "\",\"attempt\":" << attempted + 1
                << ",\"rank\":" << rank << ",\"hopsRemaining\":" << unsigned(hops)
                << ",\"startedAtUnixMs\":" << startedAt << ",\"observedAtUnixMs\":" << wallTimeMsForTest()
                << ",\"grant\":" << allocation.grant << ",\"definitionHash\":\"" << version->definitionHash
                << "\",\"kind\":\"" << (allocation.ingressRoot ? "ingress" : "binding")
                << "\",\"pinsSha256\":\"" << pinsHash(allocation.pins) << "\",\"expiresAtMonotonicMs\":" << std::setprecision(17) << allocation.expiresAtMonotonicMs
                << ",\"retryable\":" << (result.retryable ? "true" : "false")
                << ",\"connectionFailure\":" << (result.connectionFailure ? "true" : "false")
                << ",\"failure\":" << (result.failure ? result.failure->json() : "null") << "}" << std::endl;
      ++attempted;
      pathMark(allocation.timing, "attempt-logs");
      if (result.failure && result.failure->code == "LOAD_FAILED")
        server.loadFailed(*version, *result.failure, id, allocation.ingressRoot ? "ingress" : "binding",
            result.retryable && attempted < ordered.size() && hops > 0);
      if (result.failure) packagesUnavailable = packagesUnavailable && result.failure->code == "LOAD_FAILED" &&
          result.failure->category == PACKAGE_UNAVAILABLE;
      if (!result.failure) {
        avoidance.erase(id);
        if (forwarded) *forwarded = id != config.serverId;
        if (placedOn) *placedOn = id;
        if (!localFirst) server.decision(id == config.serverId && homeFailed ? "homeFailureLocal" :
            id == config.serverId && id == home ? "localHome" : id == home ? "toHome" : "candidate" + std::to_string(rank), allocation.ingressRoot);
        co_return std::move(result.dispatcher);
      }
      if (!result.retryable) co_return rejectedDispatcher(factory, headerTable, *result.failure, allocation.expiresAtMonotonicMs);
      // The call's lease ran out: no other candidate can take it, and none is to be avoided for it (a refusal of this
      // Server's own was counted above).
      if (result.failure->code == "LEASE_EXPIRED") {
        if (id != config.serverId) server.observeLocalRefusal(*result.failure, allocation.ingressRoot ? "localIngress" : "localBinding");
        co_return rejectedDispatcher(factory, headerTable, *result.failure, allocation.expiresAtMonotonicMs);
      }
      if (id == home) homeFailed = true;
      if (attempted > 1) failures += ',';
      failures += "{\"nodeId\":\"" + id + "\",\"failure\":" + result.failure->json() + "}";
      auto& avoid = avoidance[id];
      auto delay = std::min(result.failure->avoidMs, config.avoidMaxMs);
      if (result.connectionFailure) {
        avoid.failures = std::min<uint32_t>(avoid.failures + 1, 32);
        delay = std::min<uint64_t>(uint64_t(config.connectBackoffMs) << (avoid.failures - 1), config.avoidMaxMs);
      } else avoid.failures = 0;
      avoid.until = monotonicTimeMs() + delay;
      if (attempted < ordered.size() && hops > 0) server.reroute(result.failure->code);
    }
    failures += ']';
    auto refusal = server.refusal("PLACEMENT_EXHAUSTED", hops == 0 ? "placement hops exhausted" : "all candidates rejected or unavailable", 503, allocation.ingressRoot);
    if (attempted > 0 && packagesUnavailable) {
      // No candidate could load the code package: the load failure is the answer.
      refusal = server.refusal("LOAD_FAILED", "the code package could not be fetched or verified on any candidate (" +
          std::to_string(attempted) + (attempted == 1 ? " attempt)" : " attempts)"), 503, allocation.ingressRoot);
      refusal.category = PACKAGE_UNAVAILABLE;
    }
    server.observeLocalRefusal(refusal, allocation.ingressRoot ? "localIngress" : "localBinding");
    auto body = refusal.json(); body.pop_back();
    body += ",\"attempts\":" + failures + ",\"hopsExhausted\":" + (hops == 0 ? "true" : "false") +
        ",\"error\":\"execution rejected before start\"}";
    co_return rpc::EventDispatcher::Client(kj::heap<LeaseRejectionEventDispatcher>(factory, headerTable, 503,
        "Service Unavailable", std::move(body)));
  }


private:
  static std::string pinsHash(const SharedVersionPins& pins) {
    std::string value;
    for (const auto& [id, pin] : *pins) value += id + "\n" + pin.definitionHash + "\n";
    return sha256Hex(value);
  }
  struct Avoidance { double until = 0; uint32_t failures = 0; };
  std::map<std::string, Avoidance> avoidance;

  struct PeerState {
    std::string memberIdentity;
    kj::Maybe<kj::Rc<PeerConnection>> connection;
    kj::Maybe<kj::ForkedPromise<kj::Rc<PeerConnection>>> connecting;
    // The attempt in `connecting` has settled. Its fork is released by the next getConnection(),
    // never from the fork's own continuation: when every caller waiting on it was cancelled, that
    // would destroy the fork while it runs the continuation (a use after free in KJ's ForkHub).
    bool connectingSettled = false;
    uint64_t generation = 0;
    uint64_t connectAttempts = 0;
    uint64_t successfulConnections = 0;
    uint64_t disconnects = 0;
    uint64_t peerUnavailableResponses = 0;
    uint64_t signingGeneration = 0;
  };

  kj::Promise<kj::Rc<PeerConnection>> getConnection(
      const std::string& targetServerId,
      const std::string& addressText,
      const std::string& memberIdentity,
      uint32_t timeoutMs) {
    auto& state = peerStates[targetServerId];
    // A connection signed with an earlier signing key is not reused (key rotation); its calls in flight finish
    // on it, and it closes once they have.
    if (state.memberIdentity != memberIdentity || state.signingGeneration != peerAuth.signingGeneration()) {
      state.connection = kj::none;
      state.connecting = kj::none;
      state.connectingSettled = false;
      ++state.generation;
      state.memberIdentity = memberIdentity;
      state.signingGeneration = peerAuth.signingGeneration();
    }
    KJ_IF_SOME(connection, state.connection) {
      if (!connection->isDisconnected()) return connection.addRef();
      state.connection = kj::none;
    }
    if (state.connectingSettled) {
      state.connecting = kj::none;
      state.connectingSettled = false;
    }
    KJ_IF_SOME(connecting, state.connecting) {
      return connecting.addBranch();
    }

    const auto generation = ++state.generation;
    ++state.connectAttempts;
    state.connectingSettled = false;
    auto connected = connectPeer(targetServerId, addressText, generation, timeoutMs)
        .then([this, targetServerId, generation](kj::Rc<PeerConnection>&& connection) {
      auto found = peerStates.find(targetServerId);
      if (found != peerStates.end() && found->second.generation == generation) {
        found->second.connection = connection.addRef();
        found->second.connectingSettled = true;
        ++found->second.successfulConnections;
      }
      watchDisconnect(targetServerId, connection.addRef());
      return std::move(connection);
    }).catch_([this, targetServerId, generation](kj::Exception&& error)
                  -> kj::Promise<kj::Rc<PeerConnection>> {
      auto found = peerStates.find(targetServerId);
      if (found != peerStates.end() && found->second.generation == generation) {
        found->second.connectingSettled = true;
      }
      return kj::mv(error);
    }).fork();
    state.connecting = kj::mv(connected);
    return KJ_ASSERT_NONNULL(state.connecting).addBranch();
  }

  kj::Promise<kj::Rc<PeerConnection>> connectPeer(
      const std::string& targetServerId,
      const std::string& addressText,
      uint64_t generation,
      uint32_t timeoutMs) {
    return timer.timeoutAfter(
        timeoutMs * kj::MILLISECONDS,
        network.parseAddress(addressText.c_str()).then(
            [this, generation, timeoutMs, targetServerId](kj::Own<kj::NetworkAddress>&& address)
                -> kj::Promise<kj::Rc<PeerConnection>> {
      auto connector = kj::newHttpClient(timer, headerTable, *address);
      // The connection is signed for its target with the cluster key (peer-auth.h); every call it carries is
      // inside the authenticated session.
      kj::HttpHeaders connectHeaders(headerTable);
      peerAuth.sign(connectHeaders, "CONNECT", "coworkerd-peer", targetServerId);
      auto tunnel = connector->connect(
          "coworkerd-peer", connectHeaders, {});
      auto stream = std::move(tunnel.connection);
      auto rpcSystem = kj::heap<capnp::TwoPartyClient>(*stream);
      auto peerClient = rpcSystem->bootstrap().castAs<rpc::PeerServer>();
      auto connection = kj::rc<PeerConnection>(
          generation,
          std::move(address),
          std::move(connector),
          std::move(stream),
          std::move(rpcSystem),
          std::move(peerClient));
      return timer.timeoutAfter(
          timeoutMs * kj::MILLISECONDS, std::move(tunnel.status))
          .then([connection = connection.addRef()](
                    kj::HttpClient::ConnectRequest::Status&& status) mutable {
        KJ_REQUIRE(status.statusCode == 200, "peer rejected Cap'n Proto CONNECT");
        return std::move(connection);
      }).attach(std::move(connection));
    }));
  }

  void watchDisconnect(
      const std::string& targetServerId,
      kj::Rc<PeerConnection> connection) {
    connection->onDisconnect()
        .then([this, targetServerId, connection = connection.addRef()]() mutable {
      markConnectionDisconnected(targetServerId, connection);
    }).catch_([this, targetServerId, connection = connection.addRef()](
                  kj::Exception&&) mutable {
      markConnectionDisconnected(targetServerId, connection);
    }).detach([](kj::Exception&&) {});
  }

  void markConnectionDisconnected(
      const std::string& targetServerId,
      kj::Rc<PeerConnection>& connection) {
    if (!connection->markDisconnected()) return;
    auto found = peerStates.find(targetServerId);
    if (found == peerStates.end()) return;
    ++found->second.disconnects;
    if (found->second.generation == connection->getGeneration()) {
      found->second.connection = kj::none;
      // A settled attempt still holds this connection as its result (never called from the
      // attempt's own continuation, so it can be released here).
      if (found->second.connectingSettled) {
        found->second.connecting = kj::none;
        found->second.connectingSettled = false;
      }
      ++found->second.generation;
    }
  }

  ServerCore& server;
  kj::Network& network;
  kj::Timer& timer;
  const kj::HttpHeaderTable& headerTable;
  capnp::HttpOverCapnpFactory& factory;
  coworkerd::PeerAuth& peerAuth;
  std::map<std::string, PeerState> peerStates;
};

class PeerBootstrap final: public rpc::PeerServer::Server {
public:
  PeerBootstrap(
      ServerCore& server,
      PeerForwarder& peerForwarder,
      capnp::HttpOverCapnpFactory& factory,
      const kj::HttpHeaderTable& headerTable)
      : server(server),
        peerForwarder(peerForwarder),
        factory(factory),
        headerTable(headerTable) {}

  kj::Promise<void> openInvocation(OpenInvocationContext context) override {
    auto remote = context.getParams().getInvocation();
    const auto remainingMs = remote.getRemainingMs();
    const auto receivedAtMonotonicMs = monotonicTimeMs();
    KJ_REQUIRE(
        std::isfinite(remainingMs) && remainingMs >= 0,
        "remote lease remainingMs is invalid");
    KJ_REQUIRE(
        std::isfinite(receivedAtMonotonicMs + remainingMs),
        "remote lease deadline overflow");
    // The destination starts its local deadline when the request arrives. This leaves up to
    // one one-way network delay of extra lease time, which is an accepted tradeoff.
    const auto expiresAtMonotonicMs = receivedAtMonotonicMs + remainingMs;
    auto pins = std::make_shared<VersionPins>();
    KJ_REQUIRE(remote.getVersionPins().size() > 0 &&
        remote.getVersionPins().size() <= coworkerd::MAX_DEPLOYMENT_CLOSURE, "invalid remote version pin count");
    for (auto pin : remote.getVersionPins()) {
      VersionPin value{pin.getWorkerId().cStr(), pin.getVersionId().cStr(),
          pin.getContentHash().cStr(), pin.getDefinitionHash().cStr()};
      KJ_REQUIRE(coworkerd::workerFullName(value.workerId) && safeName(value.versionId) &&
          validSha256(value.contentHash) && validSha256(value.definitionHash), "invalid remote version pin");
      KJ_REQUIRE(pins->emplace(value.workerId, value).second, "duplicate remote version pin");
    }
    RemoteInvocationData invocation{
      remote.getWorkerId().cStr(),
      remote.getVersionId().cStr(),
      remote.getContentHash().cStr(),
      remote.getTreeId().cStr(),
      remote.getParentInvocationId().cStr(),
      remote.getSubrequestGrant(),
      expiresAtMonotonicMs,
      remote.getDepth(),
      remote.getPlacementHops(),
      remote.getAlreadyPlaced(),
      remote.getOriginServerId().cStr(),
      remote.getCallSequence(),
      remote.getOrderSequence(),
      pins,
      remote.getDefinitionHash().cStr(),
      remote.getIngressRoot(),
      remote.getIngressServerId().cStr(),
      remote.getEntrypoint().cStr(),
    };
    KJ_REQUIRE(coworkerd::workerFullName(invocation.workerId), "invalid remote workerId");
    KJ_REQUIRE(safeName(invocation.versionId), "invalid remote versionId");
    KJ_REQUIRE(validSha256(invocation.contentHash), "invalid remote contentHash");
    KJ_REQUIRE(safeName(invocation.treeId), "invalid remote treeId");
    KJ_REQUIRE((invocation.ingressRoot ? invocation.parentInvocationId.empty() : safeName(invocation.parentInvocationId)),
        "invalid remote parentInvocationId");
    KJ_REQUIRE(!invocation.ingressRoot || (invocation.depth == 0 && invocation.alreadyPlaced &&
        safeName(invocation.ingressServerId)), "invalid forwarded root identity");
    KJ_REQUIRE(safeName(invocation.originServerId), "invalid remote originServerId");
    KJ_REQUIRE(invocation.expiresAtMonotonicMs > 0, "missing remote lease deadline");
    server.observePeerOrigin(invocation.originServerId);
    std::cout << "[peer-open] {\"nodeId\":\"" << server.placementConfig().serverId
              << "\",\"treeId\":\"" << invocation.treeId << "\",\"remainingMs\":" << remainingMs
              << ",\"expiresAtMonotonicMs\":" << std::setprecision(17) << expiresAtMonotonicMs
              << ",\"depth\":" << invocation.depth << ",\"grant\":" << invocation.subrequestGrant
              << ",\"ingressRoot\":" << (invocation.ingressRoot ? "true" : "false")
              << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;

    auto reject = [&](InvocationRejection refusal) {
      server.observeRefusal(refusal, invocation.ingressRoot ? "peerIngress" : "peerBinding");
      std::cout << "[refusal] {\"treeId\":\"" << invocation.treeId << "\",\"parentInvocationId\":\""
                << invocation.parentInvocationId << "\",\"target\":\"" << invocation.workerId << "@" << invocation.versionId
                << "\",\"source\":\"" << (invocation.ingressRoot ? "peerIngress" : "peerBinding")
                << "\",\"nodeId\":\"" << server.placementConfig().serverId << "\",\"observedAtUnixMs\":"
                << wallTimeMsForTest() << ",\"failure\":" << refusal.json() << "}" << std::endl;
      writeRefusal(context.initResults().initResult(), refusal);
    };
    if (server.draining()) {
      // An invocation from another Server after the announcement: explicitly refused,
      // nothing executed; the sender reroutes as for any refusal.
      server.countDrainRefusal(invocation.ingressRoot ? "peerIngress" : "peerBinding");
      reject(server.drainingRefusal(invocation.ingressRoot));
      co_return;
    }
    // (objectStore) A node whose epoch expired by its own clock still serves the stateless calls addressed to any epoch
    // it held (held epochs accumulate): a store outage longer than the TTL fails no call. Refusal after expiry is
    // ownership work's (0.5).
    auto destinationEpoch = std::string(remote.getDestinationEpoch().cStr());
    if (!destinationEpoch.empty() && !server.holdsEpoch(destinationEpoch)) {
      reject(server.refusal("PEER_UNAVAILABLE", "target epoch changed before execution"));
      co_return;
    }
    if (!server.pinMatches(invocation.pins, {invocation.workerId, invocation.versionId,
        invocation.contentHash, invocation.definitionHash})) {
      auto reason = "RemoteInvocation version fields disagree with version pins";
      server.rejectPinMismatch(reason);
      reject(server.refusal("VERSION_PIN_MISMATCH", reason, 409));
      co_return;
    }
    std::string unavailable;
    auto version = co_await server.availableVersion(invocation.workerId, invocation.pins, unavailable, invocation.expiresAtMonotonicMs);
    if (version == nullptr) {
      server.rejectVersionUnavailable(unavailable);
      reject(server.refusal("VERSION_UNAVAILABLE", unavailable, 409));
      co_return;
    }
    const auto& config = server.placementConfig();
    auto alreadyPlaced = invocation.alreadyPlaced && !config.disableAlreadyPlacedForTest;
    if (!invocation.ingressRoot && server.shouldForward(invocation.workerId, alreadyPlaced)) {
      if (invocation.placementHops == 0) {
        auto reason = "placement hops exhausted target=" + invocation.workerId + "@" + invocation.versionId + " node=" + config.serverId;
        server.rejectPlacementHops(reason);
        reject(server.refusal("PLACEMENT_HOPS_EXHAUSTED", reason, 508));
        co_return;
      }
      auto target = server.targetServer(invocation.workerId);
      server.recordRelay(invocation.treeId, invocation.parentInvocationId, version->key(), target,
          invocation.subrequestGrant, invocation.depth, invocation.expiresAtMonotonicMs);
      --invocation.placementHops; invocation.alreadyPlaced = true;
      auto result = co_await peerForwarder.tryOpen(target, invocation);
      if (result.failure && result.retryable) reject(*result.failure);
      else if (result.failure) context.initResults().initResult().setAccepted(
          rejectedDispatcher(factory, headerTable, *result.failure, invocation.expiresAtMonotonicMs));
      else context.initResults().initResult().setAccepted(result.dispatcher);
      co_return;
    }
    if (auto refusal = server.admission(version->workerId, version->definitionHash, invocation.ingressRoot,
        invocation.expiresAtMonotonicMs, false, invocation.ingressRoot)) {
      reject(*refusal); co_return;
    }
    auto slot = server.reserve(invocation.ingressRoot);
    slot->entrypoint = invocation.entrypoint;
    unavailable = co_await server.ensureCode(*version, invocation.expiresAtMonotonicMs);
    if (!unavailable.empty()) {
      server.rejectVersionUnavailable(unavailable);
      reject(server.refusal("VERSION_UNAVAILABLE", unavailable, 409));
      co_return;
    }
    if (remainingLeaseMs(invocation.expiresAtMonotonicMs) == 0) {
      server.rejectExpiredLease("lease expired before remote execution");
      reject(server.refusal("LEASE_EXPIRED", "lease expired before remote execution", 410)); co_return;
    }
    if (auto missing = server.secretRefusal(*version, invocation.ingressRoot)) { reject(*missing); co_return; }
    // The load check: a version that does not load here is refused before acceptance; the sender
    // decides whether another candidate follows (only for a package this Server lacks).
    auto check = co_await server.loadCheck(version, invocation.entrypoint, invocation.expiresAtMonotonicMs, invocation.ingressRoot);
    if (check.refusal) { reject(*check.refusal); co_return; }
    try {
      auto opened = server.openGranted(std::move(version), invocation.treeId, invocation.parentInvocationId,
          invocation.subrequestGrant, invocation.depth, invocation.expiresAtMonotonicMs, invocation.callSequence,
          invocation.orderSequence, invocation.pins, std::move(slot), invocation.ingressRoot, invocation.ingressServerId, check);
      context.initResults().initResult().setAccepted(opened.dispatcher);
      // Acceptance commits dispatch; later failures travel through AdmissionHttpService.
      co_return;
    } catch (const ProcessUnavailable& error) {
      reject(server.processRefusal(error));
    }
  }

private:
  ServerCore& server;
  PeerForwarder& peerForwarder;
  capnp::HttpOverCapnpFactory& factory;
  const kj::HttpHeaderTable& headerTable;
};

// The control plane's own addresses, as every control plane announces them to every member (POST
// /__coworkerd/control-plane on the peer listener, with the management token, every few seconds): they join the
// cluster's addresses, which tenant code cannot reach whatever its egress rules allow -- with nothing to configure
// (egress.clusterAddresses remains for what no one announces). At most 16 instances of 64 addresses each; an instance
// that stopped announcing is forgotten after ten minutes.
class ControlPlaneAddresses final {
public:
  // {"instance":"<host:pid>","addresses":["<IP address or host name>",...]}; the answer for the control plane.
  std::string announce(const std::string& body) {
    std::string instance;
    std::vector<std::string> addresses;
    try {
      capnp::MallocMessageBuilder message;
      auto json = message.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(body.data(), body.size()), json);
      instance = coworkerd::json::text(json, "instance");
      auto list = coworkerd::json::field(json, "addresses");
      KJ_REQUIRE(list.isArray() && list.getArray().size() <= 64, "addresses");
      for (auto entry : list.getArray()) {
        KJ_REQUIRE(entry.isString(), "address");
        auto value = std::string(entry.getString().cStr());
        KJ_REQUIRE(!value.empty() && value.size() <= 255 && std::all_of(value.begin(), value.end(), [](char c) {
          return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '-' || c == ':' ||
              c == '[' || c == ']';
        }), "address");
        // An IPv6 address in brackets, as the egress policy reads HOST[:PORT] entries.
        if (value.find(':') != std::string::npos && !value.starts_with('[')) value = "[" + value + "]";
        addresses.push_back(std::move(value));
      }
      KJ_REQUIRE(!instance.empty() && instance.size() <= 128, "instance");
    } catch (const kj::Exception&) {
      ++refused;
      return "{\"ok\":false,\"reason\":\"the body is {\\\"instance\\\",\\\"addresses\\\":[...]} of at most 64 addresses\"}";
    }
    expire();
    if (!instances.contains(instance) && instances.size() >= 16) instances.erase(oldest());
    instances[instance] = {std::move(addresses), monotonicTimeMs()};
    ++announcements;
    return "{\"ok\":true,\"addresses\":" + std::to_string(instances[instance].addresses.size()) + "}";
  }
  std::vector<std::string> endpoints() {
    expire();
    std::vector<std::string> out;
    for (const auto& [instance, entry] : instances) out.insert(out.end(), entry.addresses.begin(), entry.addresses.end());
    return out;
  }
  std::string json() {
    expire();
    std::string out = "{\"announcements\":" + std::to_string(announcements) + ",\"refused\":" + std::to_string(refused) + ",\"instances\":[";
    size_t index = 0;
    for (const auto& [instance, entry] : instances) {
      out += std::string(index++ ? "," : "") + "{\"instance\":\"" + jsonEscape(instance) + "\",\"addresses\":[";
      for (size_t i = 0; i < entry.addresses.size(); ++i) out += (i ? ",\"" : "\"") + jsonEscape(entry.addresses[i]) + "\"";
      out += "],\"seenMsAgo\":" + std::to_string(static_cast<uint64_t>(monotonicTimeMs() - entry.seenAtMs)) + "}";
    }
    return out + "]}";
  }

private:
  struct Entry { std::vector<std::string> addresses; double seenAtMs = 0; };
  static constexpr double FORGET_MS = 10 * 60 * 1000;
  void expire() {
    const auto now = monotonicTimeMs();
    std::erase_if(instances, [&](const auto& entry) { return now - entry.second.seenAtMs > FORGET_MS; });
  }
  std::string oldest() const {
    auto found = std::min_element(instances.begin(), instances.end(),
        [](const auto& a, const auto& b) { return a.second.seenAtMs < b.second.seenAtMs; });
    return found->first;
  }
  std::map<std::string, Entry> instances;
  uint64_t announcements = 0, refused = 0;
};

// GET /__coworkerd/logs?worker=<id>[&after=<seq>][&since=<unixMs>][&limit=<n>], on the ingress and the
// peer listener (the control plane reads every member at its advertised address): one Worker's lines as
// this Server's workerd output reader keeps them (WorkerdOutput::workerLogs), with this Server's id.
// After the cursor `after`, oldest first; else the newest `limit` (default 200) from `since` on.
class WorkerLogsEndpoint final {
public:
  WorkerLogsEndpoint(std::string serverId, kj::LowLevelAsyncIoProvider& provider, kj::Timer& timer,
      const kj::HttpHeaderTable& headerTable)
      : serverId(std::move(serverId)), provider(provider), timer(timer), headerTable(headerTable) {}

  static bool matches(kj::StringPtr url) {
    std::string_view path(url.cStr());
    return path == "/__coworkerd/logs" || path.starts_with("/__coworkerd/logs?");
  }

  kj::Promise<void> serve(kj::HttpMethod method, kj::StringPtr url, kj::HttpService::Response& response) {
    if (method != kj::HttpMethod::GET) {
      co_return co_await send(response, 405, "Method Not Allowed",
          "{\"code\":\"METHOD_NOT_ALLOWED\",\"reason\":\"GET /__coworkerd/logs?worker=<id>\"}");
    }
    std::string worker;
    uint64_t after = UINT64_MAX, since = 0, limit = 200;
    bool valid = true;
    std::string query(url.cStr());
    query = query.find('?') == std::string::npos ? std::string() : query.substr(query.find('?') + 1);
    auto number = [&](const std::string& text, uint64_t& out) {
      valid = valid && !text.empty() && text.size() <= 19 && std::all_of(text.begin(), text.end(), [](char c) { return c >= '0' && c <= '9'; });
      if (valid) out = std::stoull(text);
    };
    std::istringstream fields(query);
    for (std::string field; std::getline(fields, field, '&');) {
      const auto equals = field.find('=');
      const auto key = field.substr(0, equals), value = equals == std::string::npos ? std::string() : field.substr(equals + 1);
      if (key == "worker") worker = value;
      else if (key == "after") number(value, after);
      else if (key == "since") number(value, since);
      else if (key == "limit") number(value, limit);
      else if (!field.empty()) valid = false;
    }
    if (!valid || !coworkerd::workerFullName(worker) || limit == 0 || limit > 10000) {
      co_return co_await send(response, 400, "Bad Request", "{\"code\":\"INVALID_LOG_QUERY\",\"reason\":\"worker=<tenant>/<worker> "
          "(required), after=<seq>, since=<unix ms>, limit=1..10000\"}");
    }
    std::string answer, failure;
    try {
      answer = co_await timer.timeoutAfter(2 * kj::SECONDS,
          coworkerd::WorkerdOutput::workerLogs(provider, worker, after, since, static_cast<uint32_t>(limit)));
    } catch (const kj::Exception& error) {
      failure = error.getDescription().cStr();
    }
    if (!answer.starts_with("{\"epoch\":")) {
      co_return co_await send(response, 503, "Service Unavailable", "{\"code\":\"LOGS_UNAVAILABLE\",\"reason\":\"" +
          jsonEscape(cleanReason(failure.empty() ? "the workerd output reader gave no answer" : failure)) + "\"}");
    }
    while (!answer.empty() && answer.back() == '\n') answer.pop_back();
    co_return co_await send(response, 200, "OK", "{\"nodeId\":\"" + jsonEscape(serverId) + "\"," + answer.substr(1));
  }

private:
  kj::Promise<void> send(kj::HttpService::Response& response, uint status, kj::StringPtr text, std::string body) {
    kj::HttpHeaders headers(headerTable);
    headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
    auto stream = response.send(status, text, headers, body.size());
    co_await stream->write(kj::arrayPtr(body.data(), body.size()).asBytes());
  }

  const std::string serverId;
  kj::LowLevelAsyncIoProvider& provider;
  kj::Timer& timer;
  const kj::HttpHeaderTable& headerTable;
};

class PeerRpcService final: public kj::HttpService {
public:
  PeerRpcService(
      ServerCore& server,
      PeerForwarder& peerForwarder,
      capnp::HttpOverCapnpFactory& factory,
      const kj::HttpHeaderTable& headerTable,
      kj::Function<kj::Promise<void>(kj::HttpMethod, kj::AsyncInputStream&, Response&)> trial,
      WorkerLogsEndpoint& logs, kj::Function<std::string()> state, coworkerd::PeerAuth& peerAuth,
      ControlPlaneAddresses& controlPlane)
      : server(server), headerTable(headerTable),
        rpcServer(rpc::PeerServer::Client(kj::heap<PeerBootstrap>(
            server, peerForwarder, factory, headerTable))), trial(kj::mv(trial)), logs(logs), state(kj::mv(state)),
        peerAuth(peerAuth), controlPlane(controlPlane) {}

  // Besides the Cap'n Proto CONNECT, the peer listener (the address a member
  // advertises) answers the deployment tool's trial loads, and the control
  // plane's reads of the state and the Workers' logs and its announcements of its
  // own addresses, with the management token when one is set. A handler's failure is answered as JSON, its reason
  // cleaned: 400, or 413 for a body over its limit.
  kj::Promise<void> request(
      kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& body,
      Response& response) override {
    auto tracked = kj::heap<TrackingHttpResponse>(response);
    auto answered = kj::evalNow([&]() { return route(method, url, headers, body, *tracked); });
    return answered.catch_([this, ptr = tracked.get()](kj::Exception&& error) -> kj::Promise<void> {
      if (ptr->hasStarted()) return kj::mv(error);
      const auto reason = cleanReason(error.getDescription().cStr());
      std::cerr << "[server] peer request failed: " << reason << std::endl;
      return sendJson(*ptr, 400, "Bad Request", "{\"code\":\"INVALID_REQUEST\",\"reason\":\"" + jsonEscape(reason) + "\"}");
    }).attach(kj::mv(tracked));
  }

  kj::Promise<void> sendJson(Response& response, uint status, kj::StringPtr statusText, std::string text) {
    auto body = kj::heap<std::string>(std::move(text));
    kj::HttpHeaders headers(headerTable);
    headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
    auto stream = response.send(status, statusText, headers, body->size());
    auto write = stream->write(kj::arrayPtr(body->data(), body->size()).asBytes());
    return write.attach(kj::mv(stream), kj::mv(body));
  }

  kj::Promise<void> route(
      kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& body,
      Response& response) {
    if (auto refused = server.managementRefusal(url, headers); !refused.empty()) {
      server.managementRefused("peer", method, url, refused);
      return sendUnauthorized(response, headerTable);
    }
    if (url == "/__coworkerd/trial-load") return trial(method, body, response);
    if (WorkerLogsEndpoint::matches(url)) return logs.serve(method, url, response);
    if (url == "/__coworkerd/control-plane" && method == kj::HttpMethod::POST) {
      // At most 16 KiB (read with one byte more: KJ's limit fails once it is reached).
      constexpr uint64_t LIMIT = 16 * 1024;
      return body.readAllText(LIMIT + 1).then([this, &response](kj::String text) -> kj::Promise<void> {
        if (text.size() > LIMIT)
          return sendJson(response, 413, "Payload Too Large", "{\"ok\":false,\"reason\":\"an announcement is at most 16 KiB\"}");
        auto answer = controlPlane.announce(std::string(text.cStr(), text.size()));
        const bool ok = answer.starts_with("{\"ok\":true");
        return sendJson(response, ok ? 200 : 400, ok ? "OK" : "Bad Request", std::move(answer));
      }, [this, &response](kj::Exception&&) -> kj::Promise<void> {
        return sendJson(response, 413, "Payload Too Large", "{\"ok\":false,\"reason\":\"an announcement is at most 16 KiB\"}");
      });
    }
    if (url == "/__coworkerd/state" && method == kj::HttpMethod::GET) {
      auto text = kj::heap<std::string>(state());
      kj::HttpHeaders headers(headerTable);
      headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
      auto stream = response.send(200, "OK", headers, text->size());
      auto write = stream->write(kj::arrayPtr(text->data(), text->size()).asBytes());
      return write.attach(kj::mv(stream), kj::mv(text));
    }
    return response.sendError(404, "Cap'n Proto CONNECT required", headerTable);
  }

  kj::Promise<void> connect(
      kj::StringPtr host,
      const kj::HttpHeaders& headers,
      kj::AsyncIoStream& connection,
      ConnectResponse& response,
      kj::HttpConnectSettings) override {
    KJ_REQUIRE(host == "coworkerd-peer", "invalid peer Cap'n Proto CONNECT host");
    // A peer session only with the cluster key's signature (peer-auth.h); refused, nothing of the session exists.
    auto verdict = peerAuth.verify(headers, "CONNECT", host);
    if (!verdict.refusal.empty()) {
      peerAuth.refused("peer", "CONNECT", verdict);
      response.reject(401, "Unauthorized", kj::HttpHeaders(headerTable), uint64_t(0));
      return kj::READY_NOW;
    }
    response.accept(200, "OK", kj::HttpHeaders(headerTable));
    if (verdict.keyTag.empty()) return rpcServer.accept(connection);
    // A session accepted with a key that later leaves the key file ends windowMs after it left.
    return rpcServer.accept(connection).exclusiveJoin(peerAuth.retired(verdict.keyTag).then(
        [this, tag = verdict.keyTag]() { peerAuth.sessionRetired(tag); }));
  }

private:
  ServerCore& server;
  const kj::HttpHeaderTable& headerTable;
  capnp::TwoPartyServer rpcServer;
  kj::Function<kj::Promise<void>(kj::HttpMethod, kj::AsyncInputStream&, Response&)> trial;
  WorkerLogsEndpoint& logs;
  kj::Function<std::string()> state;
  coworkerd::PeerAuth& peerAuth;
  ControlPlaneAddresses& controlPlane;
};

// The path of a request target, origin-form ("/a?b") or absolute
// ("http://host/a?b", as a Binding call carries it), without query or fragment.
std::string requestPath(kj::StringPtr url) {
  std::string value = url.cStr();
  if (!value.starts_with("/")) {
    const auto scheme = value.find("://");
    const auto path = scheme == std::string::npos ? std::string::npos : value.find('/', scheme + 3);
    const auto end = scheme == std::string::npos ? std::string::npos : value.find_first_of("?#", scheme + 3);
    value = path == std::string::npos || (end != std::string::npos && end < path) ? "/" : value.substr(path);
  }
  const auto end = value.find_first_of("?#");
  if (end != std::string::npos) value.resize(end);
  return value;
}

// The asset layer of a version with assets: Cloudflare's router and
// asset worker as wrangler 4.136.1 implements them. It sends a request to the
// Worker or answers it from the assets, without workerd, a lease or placement:
// at the ingress, for a Service Binding call to a Worker with assets, and for
// the ASSETS Binding, whose calls the assets always answer (env.ASSETS.fetch).
class AssetLayer final {
public:
  using Source = ServerCore::AssetSource;
  AssetLayer(ServerCore& server, const kj::HttpHeaderTable& headerTable): server(server), headerTable(headerTable) {}

  // False when the request belongs to the Worker.
  kj::Promise<bool> serve(const VersionDefinition& version, Source source, kj::HttpMethod method, const std::string& path,
      const kj::HttpHeaders& headers, kj::HttpService::Response& response, double deadline) {
    auto& counters = server.assetCounters(version.workerId, source);
    ++counters.requests;
    std::string reason;
    auto index = co_await server.assetIndex(version, deadline, reason);
    if (!index) {
      ++counters.unavailable;
      co_await sendText(response, 503, "Service Unavailable", rejectionJson("ASSET_UNAVAILABLE", reason, deadline), "application/json");
      co_return true;
    }
    if (source != Source::ASSETS_BINDING) {
      using Route = coworkerd::AssetRoute;
      const auto route = coworkerd::routeAsset(version, *index, path, requestHeader(headers, "sec-fetch-mode") == "navigate");
      if (route.target == Route::Target::WORKER) {
        ++counters.toWorker;
        if (route.reason == Route::Reason::MISS) ++counters.misses; else ++counters.runWorkerFirst;
        co_return false;
      }
      if (route.reason == Route::Reason::EXCLUDED) ++counters.excluded;
      if (route.reason == Route::Reason::NAVIGATION) ++counters.navigationFallbacks;
    }
    // The asset worker: html_handling lookups, then not_found_handling.
    auto intent = coworkerd::resolveAsset(*index, coworkerd::assetRequestPath(path), version.assetsNotFoundHandling);
    if (!intent.asset) {
      ++counters.misses;
      ++counters.notFound;
      response.send(404, "Not Found", kj::HttpHeaders(headerTable), uint64_t(0));
      co_return true;
    }
    // A path the asset layer answers takes only GET and HEAD, with or without a Worker.
    if (method != kj::HttpMethod::GET && method != kj::HttpMethod::HEAD) {
      ++counters.methodNotAllowed;
      response.send(405, "Method Not Allowed", kj::HttpHeaders(headerTable), uint64_t(0));
      co_return true;
    }
    if (intent.fallback) { ++counters.misses; ++counters.fallbacks; } else ++counters.hits;
    const auto& asset = *intent.asset;
    const auto status = intent.status;
    const kj::StringPtr text = status == 404 ? "Not Found" : "OK";
    kj::HttpHeaders out(headerTable);
    auto etag = kj::str("\"", asset.sha256.c_str(), "\"");
    out.addPtrPtr("ETag", etag);
    out.setPtr(kj::HttpHeaderId::CONTENT_TYPE, asset.contentType.c_str());
    if (!requestHeader(headers, "authorization") && !requestHeader(headers, "range"))
      out.addPtrPtr("Cache-Control", "public, max-age=0, must-revalidate");
    auto condition = requestHeader(headers, "if-none-match");
    if (condition && etagMatches(*condition, etag)) {
      ++counters.notModified;
      response.send(304, "Not Modified", out, uint64_t(0));
      co_return true;
    }
    if (method == kj::HttpMethod::HEAD) {
      auto length = kj::str(asset.bytes);
      out.setPtr(kj::HttpHeaderId::CONTENT_LENGTH, length);
      response.send(status, text, out, asset.bytes);
      co_return true;
    }
    // Bytes from the local module cache, fetched by hash on first use and
    // verified on every read: a failure is an explicit 503, never other bytes.
    auto read = co_await server.assetBytes(asset, deadline);
    if (!read.reason.empty()) {
      ++counters.unavailable;
      co_await sendText(response, 503, "Service Unavailable", rejectionJson("ASSET_UNAVAILABLE", read.reason, deadline), "application/json");
      co_return true;
    }
    counters.bytes += read.bytes.size();
    auto body = response.send(status, text, out, read.bytes.size());
    co_await body->write(kj::arrayPtr(reinterpret_cast<const kj::byte*>(read.bytes.data()), read.bytes.size()));
    co_return true;
  }

  // A request header by case-insensitive name; repeated fields joined as a list.
  static std::optional<std::string> requestHeader(const kj::HttpHeaders& headers, kj::StringPtr name) {
    std::optional<std::string> found;
    headers.forEach([&](kj::StringPtr key, kj::StringPtr value) {
      if (key.size() != name.size()) return;
      for (size_t i = 0; i < key.size(); ++i) if (std::tolower(static_cast<unsigned char>(key[i])) != name[i]) return;
      found = found ? *found + ", " + value.cStr() : std::string(value.cStr());
    });
    return found;
  }
  // If-None-Match: "*" or a list with the entity tag, strong or weak.
  static bool etagMatches(const std::string& condition, kj::StringPtr etag) {
    std::istringstream list(condition);
    for (std::string item; std::getline(list, item, ',');) {
      const auto first = item.find_first_not_of(" \t"), last = item.find_last_not_of(" \t");
      if (first == std::string::npos) continue;
      item = item.substr(first, last - first + 1);
      if (item == "*" || item == etag.cStr() || item == std::string("W/") + etag.cStr()) return true;
    }
    return false;
  }

private:
  kj::Promise<void> sendText(kj::HttpService::Response& response, uint status, kj::StringPtr statusText, std::string text,
      kj::StringPtr contentType) {
    kj::HttpHeaders headers(headerTable);
    headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, contentType);
    auto body = response.send(status, statusText, headers, text.size());
    co_await body->write(kj::arrayPtr(reinterpret_cast<const kj::byte*>(text.data()), text.size()));
  }

  ServerCore& server;
  const kj::HttpHeaderTable& headerTable;
};

// Dispatch of a Binding call to Worker code once its child is allocated:
// dynamic or static placement, admission, code and execution.
class BindingDispatch final {
public:
  BindingDispatch(ServerCore& server, PeerForwarder& peerForwarder, capnp::HttpOverCapnpFactory& factory,
      const kj::HttpHeaderTable& headerTable)
      : server(server), peerForwarder(peerForwarder), factory(factory), headerTable(headerTable) {}

  struct Dispatched {
    rpc::EventDispatcher::Client dispatcher;
    kj::Promise<void> ready = kj::READY_NOW;  // the opened event, for a local execution
  };

  kj::Promise<Dispatched> dispatch(std::shared_ptr<const VersionDefinition> version, ChildAllocation allocation,
      std::string workerId) {
    const auto expiresAtMonotonicMs = allocation.expiresAtMonotonicMs;
    auto timing = allocation.timing;
    try {
      if (!server.placementConfig().hasPlacement(workerId)) {
        co_return Dispatched{co_await peerForwarder.place(std::move(version), std::move(allocation))};
      }
      server.decision("staticOverride");
      auto targetServerId = server.targetServer(workerId);
      if (targetServerId != server.placementConfig().serverId) {
        auto placementHops = server.placementConfig().initialPlacementHops;
        KJ_REQUIRE(placementHops > 0, "placement hops exhausted before first forward");
        server.recordForward(*version, allocation, targetServerId);
        RemoteInvocationData invocation{
          workerId,
          allocation.pins->at(workerId).versionId,
          allocation.pins->at(workerId).contentHash,
          allocation.treeId,
          allocation.parentInvocationId,
          allocation.grant,
          allocation.expiresAtMonotonicMs,
          allocation.depth,
          static_cast<uint8_t>(placementHops - 1),
          true,
          server.placementConfig().serverId,
          allocation.callSequence,
          allocation.orderSequence,
          allocation.pins,
          allocation.pins->at(workerId).definitionHash,
          false,
          allocation.ingressServerId,
          allocation.entrypoint,
        };
        co_return Dispatched{peerForwarder.open(targetServerId, std::move(invocation))};
      }
      if (auto refusal = server.admission(version->workerId, version->definitionHash, false, expiresAtMonotonicMs)) {
        server.observeStaticAttempt(targetServerId, refusal);
        co_return Dispatched{rejectedDispatcher(factory, headerTable, *refusal, expiresAtMonotonicMs)};
      }
      pathMark(timing, "admission-end");
      auto slot = server.reserve(false);
      pathMark(timing, "code-start");
      auto unavailable = co_await server.ensureCode(*version, expiresAtMonotonicMs);
      pathMark(timing, "code-end");
      if (!unavailable.empty()) {
        server.rejectVersionUnavailable(unavailable);
        server.observeLocalRefusal(server.refusal("VERSION_UNAVAILABLE", unavailable, 409), "localBinding");
        server.observeStaticAttempt(targetServerId, server.refusal("VERSION_UNAVAILABLE", unavailable, 409));
        co_return Dispatched{makeRejectionDispatcher(factory, headerTable, 409, "Conflict",
            "VERSION_UNAVAILABLE", unavailable, expiresAtMonotonicMs)};
      }
      if (auto missing = server.secretRefusal(*version, false)) {
        server.observeLocalRefusal(*missing, "localBinding");
        server.observeStaticAttempt(targetServerId, missing);
        co_return Dispatched{rejectedDispatcher(factory, headerTable, *missing, expiresAtMonotonicMs)};
      }
      auto check = co_await server.loadCheck(version, allocation.entrypoint, expiresAtMonotonicMs, false);
      if (check.refusal) {
        server.observeLocalRefusal(*check.refusal, "localBinding");
        server.observeStaticAttempt(targetServerId, check.refusal);
        if (check.refusal->code == "LOAD_FAILED") server.loadFailed(*version, *check.refusal, targetServerId, "binding", false);
        co_return Dispatched{rejectedDispatcher(factory, headerTable, *check.refusal, expiresAtMonotonicMs)};
      }
      auto opened = server.executeAllocated(std::move(version), "binding", std::move(allocation), std::move(slot), check);
      server.observeStaticAttempt(targetServerId, std::nullopt);
      pathMark(timing, "attempt-logs");
      co_return Dispatched{std::move(opened.dispatcher), std::move(opened.ready)};
    } catch (const ProcessUnavailable& error) {
      server.observeLocalRefusal(server.processRefusal(error), "localBinding");
      server.observeStaticAttempt(server.placementConfig().serverId, server.processRefusal(error));
      co_return Dispatched{rejectedDispatcher(factory, headerTable, server.processRefusal(error), expiresAtMonotonicMs)};
    }
  }

private:
  ServerCore& server;
  PeerForwarder& peerForwarder;
  capnp::HttpOverCapnpFactory& factory;
  const kj::HttpHeaderTable& headerTable;
};

// A Binding call answered by the asset layer first. For a Service Binding to a
// Worker with assets (default entrypoint) the request goes to the Worker when
// the asset layer routes it there: dispatch happens then, with the child
// allocated when the call started. For the ASSETS Binding the assets answer.
class AssetFirstHttpService final: public kj::HttpService {
public:
  AssetFirstHttpService(AssetLayer& assets, BindingDispatch& dispatch, capnp::HttpOverCapnpFactory& factory,
      std::shared_ptr<const VersionDefinition> version, std::optional<ChildAllocation> allocation, std::string workerId,
      double deadline)
      : assets(assets), dispatch(dispatch), factory(factory), version(std::move(version)),
        allocation(std::move(allocation)), workerId(std::move(workerId)), deadline(deadline) {}

  kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& headers,
      kj::AsyncInputStream& body, Response& response) override {
    KJ_REQUIRE(!used, "a Binding call carries one request");
    used = true;
    const auto source = allocation ? AssetLayer::Source::BINDING : AssetLayer::Source::ASSETS_BINDING;
    if (co_await assets.serve(*version, source, method, requestPath(url), headers, response, deadline)) co_return;
    KJ_ASSERT(allocation.has_value(), "the ASSETS Binding never reaches a Worker");
    auto dispatched = co_await dispatch.dispatch(version, std::move(*allocation), workerId);
    auto ready = kj::mv(dispatched.ready).eagerlyEvaluate([](kj::Exception&&) {});
    auto http = factory.capnpToKj(dispatched.dispatcher.getHttpServiceRequest().send().getHttp());
    co_await http->request(method, url, headers, body, response);
  }

private:
  AssetLayer& assets;
  BindingDispatch& dispatch;
  capnp::HttpOverCapnpFactory& factory;
  std::shared_ptr<const VersionDefinition> version;
  std::optional<ChildAllocation> allocation;
  std::string workerId;
  double deadline;
  bool used = false;
};

class AssetFirstDispatcher final: public rpc::EventDispatcher::Server {
public:
  AssetFirstDispatcher(capnp::HttpOverCapnpFactory& factory, kj::Own<AssetFirstHttpService> service)
      : factory(factory), service(std::move(service)) {}
  kj::Promise<void> getHttpService(GetHttpServiceContext context) override {
    KJ_REQUIRE(service.get() != nullptr, "a Binding call carries one request");
    context.initResults(capnp::MessageSize{4, 1}).setHttp(factory.kjToCapnp(kj::mv(service)));
    return kj::READY_NOW;
  }
private:
  capnp::HttpOverCapnpFactory& factory;
  kj::Own<AssetFirstHttpService> service;
};

// A request to a Durable Object (docs/compatibility.md, section 7): from a stateless Worker's namespace Binding, or from
// an object's. In this release the owner of every object is this node: the request runs on the object's durable process
// (ProcessPool::selectDurable: d0, ... by the object's id), whose runtime gives it to the object's Host -- the active
// version of the Worker, its class declared and its namespace active. A durable process being replaced or restarted is
// waited for, at most DURABLE_WAIT_MS within the request's lease. One [do] line per request, with its outcome.
class DurableDispatch final {
public:
  static constexpr double DURABLE_WAIT_MS = 5000;
  DurableDispatch(ServerCore& server, capnp::HttpOverCapnpFactory& factory, const kj::HttpHeaderTable& headerTable)
      : server(server), factory(factory), headerTable(headerTable) {}

  struct Request {
    std::string workerId;
    DurableTarget target;
    // A stateless Worker's call: its child allocation in the caller's call tree. None: an object's call.
    std::optional<ChildAllocation> allocation;
    SharedPathTiming timing;
  };
  kj::Promise<BindingDispatch::Dispatched> dispatch(Request request) {
    const auto started = monotonicTimeMs();
    const auto deadline = request.allocation ? request.allocation->expiresAtMonotonicMs : server.rootDeadline();
    const auto target = request.target;
    const auto workerId = request.workerId;
    const std::string caller = request.allocation ? "worker" : "object";
    auto refused = [&](InvocationRejection refusal, const std::string& versionId = {}) {
      server.observeLocalRefusal(refusal, "durableObject");
      log(workerId, versionId, target, caller, nullptr, refusal.status, refusal.code, started);
      return BindingDispatch::Dispatched{rejectedDispatcher(factory, headerTable, refusal, deadline)};
    };
    if (target.hops > MAX_DURABLE_HOPS)
      co_return refused(server.refusal("DO_HOP_LIMIT", "more than " + std::to_string(MAX_DURABLE_HOPS) +
          " Durable Object calls in a row (an object calling an object, ...)", 508));
    std::string code, reason;
    auto version = server.durableVersion(workerId, target, code, reason);
    if (!version) co_return refused(server.refusal(code, reason, code == "WORKER_INVALID" ? 503 : 409));
    if (const auto unavailable = server.tenantUnavailable(coworkerd::tenantOf(workerId)); !unavailable.empty())
      co_return refused(server.refusal("TENANT_UNAVAILABLE", unavailable, 503), version->versionId);
    // The object's process restarting or being replaced: its requests wait for the next generation.
    const auto waitUntil = std::min(deadline, started + DURABLE_WAIT_MS);
    while (!server.durableReady(target.key()) && monotonicTimeMs() < waitUntil) co_await server.clock().afterDelay(10 * kj::MILLISECONDS);
    if (auto refusal = server.admission(version->workerId, version->definitionHash, false, deadline, false, false, &target))
      co_return refused(*refusal, version->versionId);
    auto slot = server.reserve(false);
    slot->durable = true;
    slot->timing = request.timing;
    auto unavailable = co_await server.ensureCode(*version, deadline);
    if (!unavailable.empty()) {
      server.rejectVersionUnavailable(unavailable);
      co_return refused(server.refusal("VERSION_UNAVAILABLE", unavailable, 409), version->versionId);
    }
    if (auto missing = server.secretRefusal(*version, false)) co_return refused(*missing, version->versionId);
    try {
      auto opened = server.openDurable(version, target, std::move(request.allocation), slot);
      // The object's request is in flight until its slot is released (its response complete, or its WebSocket answered).
      server.objectBegin(target.key(), target.hops);
      slot->release = [previous = std::move(slot->release), &server = server, key = target.key(), hops = target.hops]() {
        previous();
        server.objectEnd(key, hops);
      };
      slot->outcome = [this, workerId, versionId = version->versionId, target, caller, process = slot->process, started](
          uint status, const std::string& code) { log(workerId, versionId, target, caller, process.get(), status, code, started); };
      co_return BindingDispatch::Dispatched{std::move(opened.dispatcher), std::move(opened.ready)};
    } catch (const ProcessUnavailable&) {
      co_return refused(server.refusal("DO_UNAVAILABLE", "the durable process of this object (" +
          server.durableProcessFor(target.key()) + ") is not ready", 503), version->versionId);
    }
  }

private:
  // [do]: the object's class and the first 12 hexadecimal digits of its namespace and id, how many calls led to it, its
  // process, and the outcome -- the response's status, the code of a refusal or of the limit that ended it.
  void log(const std::string& workerId, const std::string& versionId, const DurableTarget& target, const std::string& caller,
      const coworkerd::ProcessCall* process, uint status, const std::string& code, double started) {
    std::cout << std::setprecision(17) << "[do] {\"nodeId\":\"" << jsonEscape(server.placementConfig().serverId) << "\",\"workerId\":\""
              << jsonEscape(workerId) << "\",\"versionId\":\"" << jsonEscape(versionId) << "\",\"className\":\"" << jsonEscape(target.className)
              << "\",\"namespaceId\":\"" << target.namespaceId.substr(0, 12) << "\",\"objectId\":\"" << target.objectId.substr(0, 12)
              << "\",\"hops\":" << target.hops << ",\"caller\":\"" << caller << "\",\"invocationId\":\""
              << (process ? jsonEscape(process->invocationId) : std::string()) << "\",\"processId\":\"" << (process ? process->processId : std::string())
              << "\",\"processGeneration\":" << (process ? process->generation : 0) << ",\"status\":" << status
              << ",\"code\":" << (code.empty() ? std::string("null") : "\"" + jsonEscape(code) + "\"") << ",\"elapsedMs\":"
              << monotonicTimeMs() - started << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << "}" << std::endl;
  }
  ServerCore& server;
  capnp::HttpOverCapnpFactory& factory;
  const kj::HttpHeaderTable& headerTable;
};

class BindingBootstrap final: public rpc::WorkerdBootstrap::Server {
public:
  BindingBootstrap(
      ServerCore& server,
      PeerForwarder& peerForwarder,
      capnp::HttpOverCapnpFactory& factory,
      const kj::HttpHeaderTable& headerTable)
      : server(server),
        factory(factory),
        headerTable(headerTable),
        assets(server, headerTable),
        dispatcher(server, peerForwarder, factory, headerTable),
        durable(server, factory, headerTable) {}

  kj::Promise<void> startEvent(StartEventContext context) override {
    auto timing = pathTiming("binding");
    auto params = context.getParams();
    auto metadata = params.getCfBlobJson();
    KJ_REQUIRE(metadata.size() > 0, "missing trusted Binding descriptor");
    capnp::MallocMessageBuilder message;
    auto descriptor = message.initRoot<rpc::BindingDescriptor>();
    capnp::JsonCodec json;
    json.setRejectUnknownFields(true);
    json.decode(metadata, descriptor);
    auto workerId = std::string(descriptor.getWorkerId().cStr());
    auto versionId = std::string(descriptor.getVersionId().cStr());
    auto contentHash = std::string(descriptor.getContentHash().cStr());
    auto entrypoint = std::string(descriptor.getEntrypoint().cStr());
    KJ_REQUIRE(entrypoint.empty() || safeName(entrypoint), "invalid Binding entrypoint");
    auto definitionHash = std::string(descriptor.getDefinitionHash().cStr());
    auto treeId = std::string(descriptor.getTreeId().cStr());
    auto parentInvocationId = std::string(descriptor.getParentInvocationId().cStr());
    auto callSequence = descriptor.getCallSequence();
    auto orderSequence = descriptor.getOrderSequence();
    auto expiresAtMonotonicMs = descriptor.getExpiresAtMonotonicMs();
    // An ASSETS Binding call names the calling version itself and allocates no child.
    const bool assetsCall = descriptor.getAssets();
    server.observeSequenceMapSize(descriptor.getSequenceMapSize());
    if (descriptor.hasDurableObject()) {
      // A call to a Durable Object of the calling Worker (runtime.js, DurableNamespace): from a stateless Worker a Binding
      // call of its call tree, from an object (callerObjectId) a call of that object's.
      auto call = descriptor.getDurableObject();
      DurableDispatch::Request request;
      request.workerId = workerId;
      request.timing = timing;
      request.target = {call.getNamespaceId().cStr(), call.getObjectId().cStr(), call.getClassName().cStr(), 0};
      const std::string callerNamespace = call.getCallerNamespaceId().cStr(), callerObject = call.getCallerObjectId().cStr();
      KJ_REQUIRE(coworkerd::workerFullName(workerId) && safeName(versionId) && validSha256(contentHash) && validSha256(definitionHash),
          "invalid Durable Object call: the calling version");
      KJ_REQUIRE(validSha256(request.target.namespaceId) && validSha256(request.target.objectId) &&
          coworkerd::environmentName(request.target.className), "invalid Durable Object call: namespace, object id or class");
      KJ_REQUIRE(!assetsCall && entrypoint.empty(), "a Durable Object call names no entrypoint");
      try {
        if (callerObject.empty()) {
          KJ_REQUIRE(callerNamespace.empty(), "invalid Durable Object call: a caller namespace without an object");
          KJ_REQUIRE(!treeId.empty() && safeName(treeId) && !parentInvocationId.empty() && safeName(parentInvocationId) &&
              expiresAtMonotonicMs > 0, "invalid Durable Object call: the caller's call tree");
          auto allocation = server.allocateChild(treeId, parentInvocationId, callSequence, orderSequence, expiresAtMonotonicMs);
          expiresAtMonotonicMs = allocation.expiresAtMonotonicMs;
          allocation.timing = timing;
          if (!server.perCallForTest() && !server.pinMatches(allocation.pins, {workerId, versionId, contentHash, definitionHash})) {
            auto reason = "Durable Object call: the calling version disagrees with its lease's version pins";
            server.rejectPinMismatch(reason);
            server.observeLocalRefusal(server.refusal("VERSION_PIN_MISMATCH", reason, 409), "durableObject");
            context.initResults().setDispatcher(makeRejectionDispatcher(factory, headerTable, 409, "Conflict",
                "VERSION_PIN_MISMATCH", reason, expiresAtMonotonicMs));
            co_return;
          }
          request.allocation = std::move(allocation);
        } else {
          KJ_REQUIRE(validSha256(callerNamespace) && validSha256(callerObject), "invalid Durable Object call: the calling object");
          request.target.hops = static_cast<uint16_t>(server.objectHops(callerNamespace + "/" + callerObject) + 1);
        }
      } catch (const kj::Exception& error) {
        std::string reason = error.getDescription().cStr();
        const bool budget = reason.find("subrequest budget exhausted") != std::string::npos;
        const bool expired = reason.find("lease expired before execution") != std::string::npos;
        if (!budget && !expired) throw;
        auto detail = server.lastRejectionReason();
        if (detail.empty()) detail = reason;
        const char* code = budget ? "SUBREQUEST_BUDGET_EXHAUSTED" : "LEASE_EXPIRED";
        server.observeLocalRefusal(server.refusal(code, detail, budget ? 429 : 410), "durableObject");
        context.initResults().setDispatcher(makeRejectionDispatcher(factory, headerTable, budget ? 429 : 410,
            budget ? "Too Many Requests" : "Gone", code, std::move(detail), expiresAtMonotonicMs));
        co_return;
      }
      auto dispatched = co_await durable.dispatch(std::move(request));
      context.initResults().setDispatcher(std::move(dispatched.dispatcher));
      co_return co_await std::move(dispatched.ready);
    }
    KJ_REQUIRE(coworkerd::workerFullName(workerId), "invalid bound workerId");
    KJ_REQUIRE(safeName(versionId), "invalid bound versionId");
    KJ_REQUIRE(validSha256(contentHash), "invalid bound contentHash");
    KJ_REQUIRE(!treeId.empty(), "missing bound treeId");
    KJ_REQUIRE(safeName(treeId), "invalid bound treeId");
    KJ_REQUIRE(!parentInvocationId.empty(), "missing bound parent invocationId");
    KJ_REQUIRE(safeName(parentInvocationId), "invalid bound parent invocationId");
    KJ_REQUIRE(expiresAtMonotonicMs > 0, "missing bound lease deadline");
    if (server.requiresChildSequence() && !assetsCall) {
      KJ_REQUIRE(callSequence > 0, "missing bound call sequence");
      KJ_REQUIRE(orderSequence > 0, "missing bound order sequence");
    } else {
      KJ_REQUIRE(callSequence == 0, "unexpected bound call sequence");
      KJ_REQUIRE(orderSequence == 0, "unexpected bound order sequence");
    }
    pathMark(timing, "binding-descriptor");
    try {
      if (assetsCall) {
        KJ_REQUIRE(entrypoint.empty(), "an ASSETS Binding call names no entrypoint");
        auto pins = server.livePins(treeId, parentInvocationId, expiresAtMonotonicMs, &expiresAtMonotonicMs);
        VersionPin pin{workerId, versionId, contentHash, definitionHash};
        if (!server.perCallForTest() && !server.pinMatches(pins, pin)) {
          auto reason = "ASSETS Binding version fields disagree with parent lease version pins";
          server.rejectPinMismatch(reason);
          server.observeLocalRefusal(server.refusal("VERSION_PIN_MISMATCH", reason, 409), "localBinding");
          context.initResults().setDispatcher(makeRejectionDispatcher(factory, headerTable, 409, "Conflict",
              "VERSION_PIN_MISMATCH", reason, expiresAtMonotonicMs));
          co_return;
        }
        std::string unavailable;
        auto version = co_await server.resolveVersion(pin, expiresAtMonotonicMs, unavailable);
        if (version != nullptr && !(version->assets && version->assetsBinding())) {
          version = nullptr;
          unavailable = "定义不一致：the version has no ASSETS Binding";
        }
        if (version == nullptr) {
          server.rejectVersionUnavailable(unavailable);
          server.observeLocalRefusal(server.refusal("VERSION_UNAVAILABLE", unavailable, 409), "localBinding");
          context.initResults().setDispatcher(makeRejectionDispatcher(factory, headerTable, 409, "Conflict",
              "VERSION_UNAVAILABLE", unavailable, expiresAtMonotonicMs));
          co_return;
        }
        context.initResults().setDispatcher(rpc::EventDispatcher::Client(kj::heap<AssetFirstDispatcher>(factory,
            kj::heap<AssetFirstHttpService>(assets, dispatcher, factory, std::move(version), std::nullopt, workerId,
                expiresAtMonotonicMs))));
        co_return;
      }
      auto allocation = server.allocateChild(
          treeId,
          parentInvocationId,
          callSequence,
          orderSequence,
          expiresAtMonotonicMs);
      // From here on the lease's own deadline: a WebSocket connection's is renewed past the one its runtime presents.
      expiresAtMonotonicMs = allocation.expiresAtMonotonicMs;
      allocation.timing = timing;
      allocation.entrypoint = entrypoint;
      pathMark(timing, "allocate-child");
      if (!server.perCallForTest() && !server.pinMatches(allocation.pins,
          {workerId, versionId, contentHash, definitionHash})) {
        auto reason = "Binding version fields disagree with parent lease version pins";
        server.rejectPinMismatch(reason);
        server.observeLocalRefusal(server.refusal("VERSION_PIN_MISMATCH", reason, 409), "localBinding");
        context.initResults().setDispatcher(makeRejectionDispatcher(factory, headerTable, 409, "Conflict",
            "VERSION_PIN_MISMATCH", reason, expiresAtMonotonicMs));
        co_return;
      }
      std::string unavailable;
      auto version = co_await server.availableVersion(workerId, allocation.pins, unavailable, expiresAtMonotonicMs);
      pathMark(timing, "resolve-definition");
      if (version == nullptr) {
        server.rejectVersionUnavailable(unavailable);
        server.observeLocalRefusal(server.refusal("VERSION_UNAVAILABLE", unavailable, 409), "localBinding");
        context.initResults().setDispatcher(makeRejectionDispatcher(factory, headerTable, 409, "Conflict",
            "VERSION_UNAVAILABLE", unavailable, expiresAtMonotonicMs));
        co_return;
      }
      if (version->assets && (entrypoint.empty() || entrypoint == "default")) {
        // As at the ingress, the target's asset layer runs before its Worker
        // (wrangler routes a Binding without a named entrypoint through it).
        context.initResults().setDispatcher(rpc::EventDispatcher::Client(kj::heap<AssetFirstDispatcher>(factory,
            kj::heap<AssetFirstHttpService>(assets, dispatcher, factory, std::move(version), std::move(allocation), workerId,
                expiresAtMonotonicMs))));
        co_return;
      }
      auto dispatched = co_await dispatcher.dispatch(std::move(version), std::move(allocation), workerId);
      context.initResults().setDispatcher(std::move(dispatched.dispatcher));
      co_return co_await std::move(dispatched.ready);
    } catch (const kj::Exception& error) {
      std::string reason = error.getDescription().cStr();
      const bool budget = reason.find("subrequest budget exhausted") != std::string::npos;
      const bool order = reason.find("Binding arrival out of order") != std::string::npos;
      const bool expired = reason.find("lease expired before execution") != std::string::npos;
      if (!budget && !order && !expired) throw;

      const uint status = budget ? 429 : expired ? 410 : 409;
      const char* statusText =
          budget ? "Too Many Requests" : expired ? "Gone" : "Conflict";
      auto detail = server.lastRejectionReason();
      if (detail.empty()) detail = reason;
      server.observeLocalRefusal(server.refusal(budget ? "SUBREQUEST_BUDGET_EXHAUSTED" :
          expired ? "LEASE_EXPIRED" : "BINDING_ARRIVAL_OUT_OF_ORDER", detail, status), "localBinding");
      context.initResults().setDispatcher(makeRejectionDispatcher(
          factory,
          headerTable,
          status,
          statusText,
          budget ? "SUBREQUEST_BUDGET_EXHAUSTED" :
              expired ? "LEASE_EXPIRED" : "BINDING_ARRIVAL_OUT_OF_ORDER",
          std::move(detail),
          expiresAtMonotonicMs));
      co_return;
    }
  }

private:
  ServerCore& server;
  capnp::HttpOverCapnpFactory& factory;
  const kj::HttpHeaderTable& headerTable;
  AssetLayer assets;
  BindingDispatch dispatcher;
  DurableDispatch durable;
};

class BindingRpcService final: public kj::HttpService {
public:
  BindingRpcService(
      ServerCore& server,
      PeerForwarder& peerForwarder,
      capnp::HttpOverCapnpFactory& factory,
      const kj::HttpHeaderTable& headerTable)
      : headerTable(headerTable),
        rpcServer(rpc::WorkerdBootstrap::Client(
            kj::heap<BindingBootstrap>(
                server, peerForwarder, factory, headerTable))) {}

  kj::Promise<void> request(
      kj::HttpMethod,
      kj::StringPtr,
      const kj::HttpHeaders&,
      kj::AsyncInputStream&,
      Response& response) override {
    return response.sendError(404, "Cap'n Proto CONNECT required", headerTable);
  }

  kj::Promise<void> connect(
      kj::StringPtr host,
      const kj::HttpHeaders&,
      kj::AsyncIoStream& connection,
      ConnectResponse& response,
      kj::HttpConnectSettings) override {
    KJ_REQUIRE(host == "coworkerd-server", "invalid Cap'n Proto CONNECT host");
    response.accept(200, "OK", kj::HttpHeaders(headerTable));
    return rpcServer.accept(connection);
  }

private:
  const kj::HttpHeaderTable& headerTable;
  capnp::TwoPartyServer rpcServer;
};

class SourceService final: public kj::HttpService, public kj::HttpServerCallbacks {
public:
  SourceService(ServerCore& server, kj::LowLevelAsyncIoProvider& provider, kj::Timer& timer, const kj::HttpHeaderTable& headerTable)
      : server(server), provider(provider), timer(timer), headerTable(headerTable) {}

  // Cold reloads can coincide with HTTP idle expiry. KJ owns the Connection header;
  // use its callback to close source connections after each complete response.
  bool shouldClose() override { return true; }

  kj::Promise<void> request(
      kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders&,
      kj::AsyncInputStream& body,
      Response& response) override {
    return serve(method, url, body, response).catch_([this, &response](kj::Exception&& error) {
      std::cerr << "[server] source request failed: "
                << cleanReason(error.getDescription().cStr()) << std::endl;
      return response.sendError(404, "Coworkerd source not found", headerTable);
    });
  }

  std::string logJson() const {
    return "{\"posts\":" + std::to_string(logPosts) + ",\"lines\":" + std::to_string(logLines) + ",\"cutLines\":" +
        std::to_string(logCutLines) + ",\"refusedPosts\":" + std::to_string(logRefusedPosts) + ",\"eventsOverBudget\":" +
        std::to_string(server.logEventsOverBudgetCount()) + "}";
  }

private:
  // A Worker's console lines, from the runtime of one of this node's processes (runtime/runtime.js, TenantLog: the
  // streaming tail workerd gives each Worker it loads, bound to that Worker's identity by the runtime; only the
  // runtimes reach this socket), as they are written, with the invocation they belong to. They are kept as that
  // Worker's lines (WorkerdOutput::appendTenantLines); what a line says never decides whose it is. An event's lines past
  // 256 KB (ServerCore::chargeLogLine) and lines beyond what one append carries are cut and counted.
  static constexpr size_t MAX_LOG_POST_BYTES = 16 * 1024 * 1024, MAX_LOG_APPEND_BYTES = 2 * 1024 * 1024;
  static constexpr const char* LOG_EVENT_EXCEEDED = "Log size limit exceeded: more than 256 KB of lines in one event (a request, "
      "a request to a Durable Object, or a message of a WebSocket connection); the event's later lines are not kept";
  kj::Promise<void> log(kj::StringPtr url, kj::AsyncInputStream& body, Response& response) {
    const auto processId = queryParameter(url, "processId", true, "/v1/log");
    auto text = co_await body.readAllText(MAX_LOG_POST_BYTES);
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(text.cStr(), text.size()), json);
    const auto workerId = coworkerd::json::text(json, "workerId"), versionId = coworkerd::json::text(json, "versionId");
    const auto invocationField = coworkerd::json::field(json, "invocationId");
    const std::string invocationId = invocationField.isString() ? invocationField.getString().cStr() : "";
    if (!coworkerd::workerFullName(workerId) || !safeName(versionId) || versionId.size() > 255 ||
        !(invocationId.empty() || (safeName(invocationId) && invocationId.size() <= 255))) {
      ++logRefusedPosts;
      KJ_FAIL_REQUIRE("a log post names an invalid Worker, version or invocation");
    }
    auto entries = coworkerd::json::field(json, "lines");
    KJ_REQUIRE(entries.isArray(), "a log post holds {workerId, versionId, invocationId, lines}");
    std::string lines;
    uint64_t kept = 0, cut = 0;
    ServerCore::LogBudget own;
    auto append = [&](const std::string& name, std::string_view text) {
      auto line = "{\"workerId\":\"" + workerId + "\",\"versionId\":\"" + versionId + "\",\"level\":\"" + name + "\",\"message\":\"" +
          jsonEscape(std::string(text)) + "\"}\n";
      if (lines.size() + line.size() > MAX_LOG_APPEND_BYTES) { ++cut; return; }
      lines += line;
      ++kept;
    };
    for (auto entry : entries.getArray()) {
      auto level = coworkerd::json::field(entry, "level"), text = coworkerd::json::field(entry, "message");
      if (!level.isString() || !text.isString()) { ++cut; continue; }
      const std::string name(level.getString().cStr());
      if (name != "debug" && name != "info" && name != "log" && name != "warn" && name != "error") { ++cut; continue; }
      const std::string_view value(text.getString().begin(), text.getString().size());
      bool warn = false;
      if (!server.chargeLogLine(invocationId, value.size(), own, warn)) {
        ++cut;
        if (warn) append("warn", LOG_EVENT_EXCEEDED);
        continue;
      }
      append(name, value);
    }
    ++logPosts;
    logLines += kept;
    logCutLines += cut;
    std::string answer = "{\"ok\":true,\"accepted\":0}";
    if (kept) answer = co_await timer.timeoutAfter(2 * kj::SECONDS, coworkerd::WorkerdOutput::appendTenantLines(provider, processId, lines));
    kj::HttpHeaders headers(headerTable);
    headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
    auto stream = response.send(200, "OK", headers, answer.size());
    co_await stream->write(kj::arrayPtr(answer.data(), answer.size()).asBytes());
  }

  kj::Promise<void> serve(
      kj::HttpMethod method,
      kj::StringPtr url,
      kj::AsyncInputStream& body,
      Response& response) {
    if (method == kj::HttpMethod::POST && requestPath(url) == "/v1/log") co_return co_await log(url, body, response);
    KJ_REQUIRE(method == kj::HttpMethod::GET, "source endpoint requires GET");
    auto workerId = queryParameter(url, "workerId");
    auto versionId = queryParameter(url, "versionId");
    auto contentHash = queryParameter(url, "contentHash");
    auto text = co_await server.sourceJson(workerId, versionId, contentHash,
        queryParameter(url, "invocationId", false), queryParameter(url, "processId", false),
        queryParameter(url, "processGeneration", false), queryParameter(url, "definitionHash", false));
    kj::HttpHeaders headers(headerTable);
    headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
    auto stream = response.send(200, "OK", headers, text.size());
    co_await stream->write(kj::arrayPtr(
        reinterpret_cast<const kj::byte*>(text.data()), text.size()));
  }

  ServerCore& server;
  kj::LowLevelAsyncIoProvider& provider;
  kj::Timer& timer;
  const kj::HttpHeaderTable& headerTable;
  uint64_t logPosts = 0, logLines = 0, logCutLines = 0, logRefusedPosts = 0;
};

// Forwards an ingress request to the Worker that serves it, and its response to the client. The ingress
// handling ends when the response is complete -- its last byte written to the client when its
// length is known (no byte for HEAD, 204, 304 or an empty body) -- not when the backend's RPC returns:
// the client may already reuse the connection for its next request. A backend result that arrives
// after that is only recorded (one line, `lateBackendFailures`) and leaves the connection alone. A
// response of unknown length ends with the backend's return: over Cap'n Proto its end cannot be told
// from a backend that died (a dropped body stream is ended implicitly). A backend that fails before
// the response is complete fails this request (the client sees it cut off). The backend's
// call is never cancelled by an early end, so the call accounting -- in-flight calls, the contexts a
// drain waits for -- still follows the backend.
// An upgrade request at the ingress (GatewayService): one of its tenant's places under ingress.webSocketsPerTenant, held
// until the request ends, and what the relay of the connection it may become names.
struct IngressWebSocket {
  WebSocketRegistry& registry;
  std::string tenant, workerId, versionId, treeId, executingNodeId;
  bool forwarded = false;
  double receivedAtMonotonicMs = monotonicTimeMs();
  IngressWebSocket(WebSocketRegistry& registry, std::string tenant): registry(registry), tenant(std::move(tenant)) {}
  ~IngressWebSocket() { registry.release(tenant); }
};

class HttpForwarder final: private kj::TaskSet::ErrorHandler {
public:
  HttpForwarder(ServerCore& server, capnp::HttpOverCapnpFactory& factory): server(server), factory(factory), tasks(*this) {}

  // `webSocket`: the request is an upgrade the ingress let through; a 101 from the Worker makes it a connection, relayed
  // here until it ends.
  kj::Promise<void> forward(
      OpenEvent opened,
      kj::HttpMethod method,
      kj::String url,
      kj::HttpHeaders headers,
      kj::AsyncInputStream& requestBody,
      kj::HttpService::Response& response, SharedPathTiming timing = {}, std::string target = {},
      std::shared_ptr<IngressWebSocket> webSocket = {}) {
    auto exchange = kj::heap<Exchange>(server, response, method, timing, std::move(target), std::move(webSocket));
    auto& ref = *exchange;
    auto getHttp = opened.dispatcher.getHttpServiceRequest().send();
    auto workerService = factory.capnpToKj(getHttp.getHttp());
    auto body = kj::newOneWayPipe(requestBody.tryGetLength());
    auto ownHeaders = kj::heap(kj::mv(headers));
    auto backend = workerService->request(method, url, *ownHeaders, *body.in, ref);
    ref.track(kj::mv(backend).attach(kj::mv(body.in), kj::mv(workerService), kj::mv(getHttp), kj::mv(url),
        kj::mv(ownHeaders), kj::mv(opened.ready)));
    auto upload = requestBody.pumpTo(*body.out)
        .then([out = kj::mv(body.out)](uint64_t) mutable {
      // Destroying the request body writes HTTP EOF. Do this as soon as upload completes instead
      // of retaining it until the response pump also finishes, which would deadlock duplex echo.
      out = nullptr;
    });
    co_await kj::joinPromisesFailFast(kj::arr(kj::mv(upload), ref.whenDone()));
    pathMark(timing, "response-end");
    if (timing) timing->emit();
    // A connection's relay ended: its socket to the client goes before the request here returns, as KJ requires.
    ref.endWebSocket();
    // The response is complete; the backend's return may still be on its way.
    if (!ref.backendSettled()) tasks.add(ref.releaseBackend().attach(kj::mv(exchange)));
  }

private:
  // The response the backend sends, relayed to the client as it arrives.
  class Exchange final: public kj::HttpService::Response {
  public:
    Exchange(ServerCore& server, kj::HttpService::Response& client, kj::HttpMethod method, SharedPathTiming timing,
        std::string target, std::shared_ptr<IngressWebSocket> webSocket)
        : server(server), client(client), method(method), timing(std::move(timing)), target(std::move(target)),
          webSocket(std::move(webSocket)) {
      auto paf = kj::newPromiseAndFulfiller<void>();
      done = kj::mv(paf.promise);
      doneFulfiller = kj::mv(paf.fulfiller);
    }
    void track(kj::Promise<void> request) {
      backend = request.then([this]() { settled(kj::none); }, [this](kj::Exception&& error) { settled(kj::mv(error)); })
          .eagerlyEvaluate(nullptr);
    }
    // Resolves once the response is complete or the backend returned; rejects when the backend failed first.
    kj::Promise<void> whenDone() { return kj::mv(done); }
    bool backendSettled() const { return isSettled; }
    kj::Promise<void> releaseBackend() { return kj::mv(backend); }

    kj::Own<kj::AsyncOutputStream> send(uint status, kj::StringPtr statusText, const kj::HttpHeaders& headers,
        kj::Maybe<uint64_t> expectedBodySize = kj::none) override {
      KJ_REQUIRE(!started, "the backend sent a second response");
      pathMark(timing, "response-headers");
      started = true;
      expected = expectedBodySize;
      clientBody = client.send(status, statusText, headers, expectedBodySize);
      bool empty = method == kj::HttpMethod::HEAD;
      KJ_IF_SOME(size, expected) { empty = empty || size == 0; }
      if (empty) {
        complete();
        return kj::heap<kj::NullStream>();
      }
      return kj::heap<Relay>(*this);
    }
    // The Worker answered the upgrade with 101: KJ answers the client's handshake (Sec-WebSocket-Accept) with the Worker's
    // headers, and the connection's relay on this node runs between the client and a pipe whose other end goes back
    // toward the node that executes it (this one or another).
    kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders& headers) override {
      KJ_REQUIRE(webSocket != nullptr && !started, "a WebSocket was not requested");
      pathMark(timing, "response-headers");
      started = true;
      auto socket = client.acceptWebSocket(headers);
      WebSocketRelay::Identity identity;
      identity.nodeId = server.placementConfig().serverId;
      identity.workerId = webSocket->workerId; identity.versionId = webSocket->versionId; identity.treeId = webSocket->treeId;
      identity.forwarded = webSocket->forwarded;
      identity.peerNodeId = webSocket->forwarded ? webSocket->executingNodeId : std::string();
      identity.protocol = headerValue(headers, "sec-websocket-protocol");
      identity.handshakeMs = monotonicTimeMs() - webSocket->receivedAtMonotonicMs;
      auto pipe = kj::newWebSocketPipe();
      relay = kj::heap<WebSocketRelay>(server.webSockets(), server.clock(), WebSocketRegistry::Role::INGRESS, std::move(identity),
          kj::mv(socket), kj::mv(pipe.ends[1]), WebSocketRelay::Hooks{});
      relayTask = relay->run().then([this]() { complete(); }, [this](kj::Exception&&) { complete(); }).eagerlyEvaluate(nullptr);
      return kj::mv(pipe.ends[0]);
    }
    // The connection's place under ingress.webSocketsPerTenant goes back with it, not with this exchange, which may wait a
    // little longer for the backend's return.
    void endWebSocket() {
      relayTask = kj::none;
      relay = nullptr;
      if (started) webSocket = nullptr;
    }

  private:
    class Relay final: public kj::AsyncOutputStream {
    public:
      explicit Relay(Exchange& exchange): exchange(exchange) {}
      kj::Promise<void> write(kj::ArrayPtr<const kj::byte> buffer) override { return exchange.write(buffer.size(), buffer); }
      kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const kj::byte>> pieces) override {
        uint64_t size = 0;
        for (auto& piece : pieces) size += piece.size();
        return exchange.write(size, pieces);
      }
      kj::Promise<void> whenWriteDisconnected() override {
        return exchange.clientBody.get() != nullptr ? exchange.clientBody->whenWriteDisconnected() : kj::Promise<void>(kj::NEVER_DONE);
      }
    private:
      Exchange& exchange;
    };

    template <typename Data>
    kj::Promise<void> write(uint64_t size, Data data) {
      if (size == 0) return kj::READY_NOW;
      KJ_REQUIRE(clientBody.get() != nullptr && !completed, "response body written after its end");
      written += size;
      return clientBody->write(data).then([this]() {
        KJ_IF_SOME(length, expected) { if (written >= length) complete(); }
      });
    }
    void complete() {
      if (completed) return;
      completed = true;
      completedAtUnixMs = wallTimeMsForTest();
      clientBody = nullptr;  // the response is whole; the client may reuse its connection
      if (doneFulfiller->isWaiting()) doneFulfiller->fulfill();
    }
    void settled(kj::Maybe<kj::Exception> failure) {
      isSettled = true;
      // A connection ends with its relay, which sees the backend's failure on its own side.
      if (relay.get() != nullptr) return;
      KJ_IF_SOME(error, failure) {
        if (completed) {
          server.lateBackendFailure(target, error, completedAtUnixMs);
          return;
        }
        if (clientBody.get() != nullptr) {
          clientBody->abortWrite(error.clone());
          clientBody = nullptr;
        }
        if (doneFulfiller->isWaiting()) doneFulfiller->reject(kj::mv(error));
        return;
      }
      if (!completed && !started) {
        if (doneFulfiller->isWaiting()) doneFulfiller->reject(KJ_EXCEPTION(FAILED, "the backend returned without a response"));
        return;
      }
      // A body of unknown length ends with the backend; one shorter than its length is left to the
      // client connection's writer, which closes the connection.
      clientBody = nullptr;
      completed = true;
      if (doneFulfiller->isWaiting()) doneFulfiller->fulfill();
    }

    ServerCore& server;
    kj::HttpService::Response& client;
    kj::HttpMethod method;
    SharedPathTiming timing;
    std::string target;
    kj::Promise<void> done = nullptr;
    kj::Own<kj::PromiseFulfiller<void>> doneFulfiller;
    kj::Own<kj::AsyncOutputStream> clientBody;
    kj::Maybe<uint64_t> expected;
    uint64_t written = 0, completedAtUnixMs = 0;
    bool started = false, completed = false, isSettled = false;
    std::shared_ptr<IngressWebSocket> webSocket;
    kj::Own<WebSocketRelay> relay;
    kj::Maybe<kj::Promise<void>> relayTask;  // destroyed before the relay
    // Last: destroyed first, with the backend's relay into this exchange.
    kj::Promise<void> backend = nullptr;
  };

  void taskFailed(kj::Exception&& error) override {
    std::cerr << "[server] forwarder task failed: " << cleanReason(error.getDescription().cStr()) << std::endl;
  }

  ServerCore& server;
  capnp::HttpOverCapnpFactory& factory;
  kj::TaskSet tasks;
};

// ---------------------------------------------------------------------------
// Trial load: POST /__coworkerd/trial-load, served on the HTTP listener
// and on the peer listener (a member's advertised address). The body names a
// published version, {"workerId","versionId","definitionHash"}, with optional
// "contentHash" (lets the definition be fetched from the object store),
// "revision" (the tenant index's revision that published it, which this Server's view
// must reach first), "timeoutMs" (whole trial, default 5000) and "routed"
// (the Worker has routes, so its default export must serve fetch). The answer
// is 200 with a verdict, 400 for a malformed request.
//
// Verdict categories: MISSING_MODULE, SYNTAX_ERROR, TOP_LEVEL_THROW,
// TOP_LEVEL_AWAIT (a top-level await that never settles: workerd refuses it
// at once), ENTRYPOINT_MISSING, COMPATIBILITY_FLAG (a node:* module the
// version's flags do not provide), INVALID_MODULE (Wasm or data that cannot be
// compiled or decoded), LIMIT_EXCEEDED (CPU or memory, while loading),
// TIMEOUT (while loading), LOAD_FAILED (anything else, e.g. the process died
// without a culprit), and UNAVAILABLE (this Server could not try: no
// definition, secrets or code in time, no process, another definition).
// Messages are cleaned: no absolute path, no value of the version's secrets
// or variables, no JSON input, at most 400 bytes.

struct TrialRequest {
  std::string workerId, versionId, definitionHash, contentHash;
  uint64_t revision = 0;
  uint32_t timeoutMs = 5000;
  bool routed = false;
};

// A failed load, as the runtime reported it ("Failed to start Worker:\n
// Uncaught Error: No such module ...\n  at worker.js:1:7"), and what the
// process's limit events say about this probe.
TrialVerdict classifyTrialFailure(const std::string& stage, const std::string& name, const std::string& raw,
    bool cpuExceeded, bool memoryExceeded, uint32_t cpuLimitMs, uint32_t memoryLimitMiB) {
  std::string text = raw;
  for (const char* prefix : {"Failed to start Worker:", "\n", " ", "Uncaught "}) {
    if (text.starts_with(prefix)) text = text.substr(std::string_view(prefix).size());
  }
  std::vector<std::string> lines;
  for (size_t start = 0; start <= text.size();) {
    auto end = text.find('\n', start);
    if (end == std::string::npos) end = text.size();
    auto line = text.substr(start, end - start);
    while (!line.empty() && (line.front() == ' ' || line.front() == '\t')) line.erase(0, 1);
    if (!line.empty()) lines.push_back(line);
    start = end + 1;
  }
  const std::string head = lines.empty() ? std::string() : lines.front();
  // Where it happened: an import or the first frame in the version's own modules.
  std::string where;
  for (size_t i = 1; i < lines.size() && where.empty(); ++i) {
    const auto& line = lines[i];
    if (line.starts_with("imported from ")) where = line;
    else if (line.starts_with("at ") && line.find("node-internal:") == std::string::npos &&
        line.find("__coworkerd_entrypoint__.js") == std::string::npos) where = line;
  }
  auto colon = head.find(": ");
  std::string error = colon == std::string::npos ? std::string() : head.substr(0, colon);
  if (!std::all_of(error.begin(), error.end(), [](unsigned char c) { return std::isalnum(c) || c == '_'; })) error.clear();
  const std::string detail = where.empty() ? head : head + " (" + where + ")";
  if (memoryExceeded || head.find("MEMORY_LIMIT_EXCEEDED") != std::string::npos)
    return {"LIMIT_EXCEEDED", "memory", "memory limit exceeded while loading (" + std::to_string(memoryLimitMiB) + " MiB per isolate)"};
  if (cpuExceeded || head.find("CPU_LIMIT_EXCEEDED") != std::string::npos)
    return {"LIMIT_EXCEEDED", "cpu", "CPU limit exceeded while loading (" + std::to_string(cpuLimitMs) + " ms)"};
  if (stage == "code") {
    // The runtime builds the modules from verified bytes: a JSON module that
    // does not parse or a text module that is not UTF-8 is the version's
    // fault; anything else is this Server failing to hand over the code.
    if (name == "SyntaxError" || name == "TypeError") return {"INVALID_MODULE", "", "a module cannot be decoded: " + name + ": " + text};
    return {"UNAVAILABLE", "", "the runtime could not read the code: " + text};
  }
  if (head.find("does not provide an export named 'default'") != std::string::npos &&
      head.find("__coworkerd_tenant__.js") != std::string::npos)
    return {"ENTRYPOINT_MISSING", "", "the script has no default export"};
  const std::string missing = "Error: No such module \"";
  if (head.starts_with(missing)) {
    const auto module = head.substr(missing.size(), head.find('"', missing.size()) - missing.size());
    if (module.starts_with("node:"))
      return {"COMPATIBILITY_FLAG", "", detail + "; node:* modules need the nodejs_compat compatibility flag"};
    return {"MISSING_MODULE", "", detail};
  }
  if (error == "SyntaxError") return {"SYNTAX_ERROR", "", detail};
  if (error == "CompileError" || error == "LinkError" || head.find("WebAssembly") != std::string::npos ||
      head.find("WasmModuleObject") != std::string::npos)
    return {"INVALID_MODULE", "", detail};
  if (head == "Error: Top-level await in module is unsettled.")
    return {"TOP_LEVEL_AWAIT", "", "a top-level await never settles (workerd refuses the module while loading)"};
  if (head.empty()) return {"LOAD_FAILED", "", text};
  return {"TOP_LEVEL_THROW", "", detail};
}

class TrialService final {
public:
  static constexpr const char* PATH = "/__coworkerd/trial-load";
  TrialService(ServerCore& server, kj::Timer& timer, capnp::HttpOverCapnpFactory& factory, const kj::HttpHeaderTable& headers)
      : server(server), timer(timer), factory(factory), headers(headers) {}

  static constexpr uint64_t BODY_LIMIT = 8192;
  kj::Promise<void> serve(kj::HttpMethod method, kj::AsyncInputStream& body, kj::HttpService::Response& response) {
    if (method != kj::HttpMethod::POST) {
      co_return co_await send(response, 405, "Method Not Allowed", "{\"error\":\"trial-load requires POST\"}");
    }
    // At most BODY_LIMIT bytes (read with one more: KJ's limit fails once it is reached).
    kj::Maybe<kj::String> read;
    try { read = co_await body.readAllText(BODY_LIMIT + 1); } catch (const kj::Exception&) {}
    if (read == kj::none || KJ_ASSERT_NONNULL(read).size() > BODY_LIMIT) {
      co_return co_await send(response, 413, "Payload Too Large",
          "{\"error\":\"invalid trial request\",\"reason\":\"a trial request is at most 8192 bytes\"}");
    }
    auto text = kj::mv(KJ_ASSERT_NONNULL(read));
    TrialRequest request;
    std::string invalid;
    try { request = parse(text); }
    catch (const kj::Exception& error) { invalid = error.getDescription().cStr(); }
    if (!invalid.empty()) {
      co_return co_await send(response, 400, "Bad Request",
          "{\"error\":\"invalid trial request\",\"reason\":\"" + jsonEscape(invalid) + "\"}");
    }
    auto answer = co_await run(std::move(request));
    co_return co_await send(response, 200, "OK", answer);
  }

private:
  ServerCore& server;
  kj::Timer& timer;
  capnp::HttpOverCapnpFactory& factory;
  const kj::HttpHeaderTable& headers;

  static TrialRequest parse(kj::StringPtr text) {
    capnp::MallocMessageBuilder message;
    auto json = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(text.asArray(), json);
    KJ_REQUIRE(json.isObject(), "the body must be a JSON object");
    TrialRequest request;
    for (auto entry : json.getObject()) {
      const std::string name = entry.getName().cStr();
      auto value = entry.getValue();
      auto string = [&]() { KJ_REQUIRE(value.isString(), "expected a string", name.c_str()); return std::string(value.getString().cStr()); };
      if (name == "workerId") request.workerId = string();
      else if (name == "versionId") request.versionId = string();
      else if (name == "definitionHash") request.definitionHash = string();
      else if (name == "contentHash") request.contentHash = string();
      else if (name == "revision") {
        auto digits = value.isString() ? string() : std::string();
        if (value.isNumber()) digits = std::to_string(static_cast<uint64_t>(value.getNumber()));
        KJ_REQUIRE(!digits.empty() && digits.size() <= 19 &&
            std::all_of(digits.begin(), digits.end(), [](unsigned char c) { return c >= '0' && c <= '9'; }), "invalid revision");
        request.revision = std::stoull(digits);
      } else if (name == "timeoutMs") {
        KJ_REQUIRE(value.isNumber() && value.getNumber() >= 1 && value.getNumber() <= 60000, "timeoutMs must be 1..60000");
        request.timeoutMs = static_cast<uint32_t>(value.getNumber());
      } else if (name == "routed") {
        KJ_REQUIRE(value.isBoolean(), "routed must be a boolean");
        request.routed = value.getBoolean();
      } else KJ_FAIL_REQUIRE("unknown field", name.c_str());
    }
    KJ_REQUIRE(coworkerd::workerFullName(request.workerId) && safeName(request.versionId), "invalid workerId or versionId");
    KJ_REQUIRE(validSha256(request.definitionHash), "invalid definitionHash");
    KJ_REQUIRE(request.contentHash.empty() || validSha256(request.contentHash), "invalid contentHash");
    return request;
  }

  static int64_t micros(double ms) { return static_cast<int64_t>(std::max(0.0, ms) * 1000); }
  static constexpr double VIEW_WAIT_MS = 1000;

  kj::Promise<std::string> run(TrialRequest request) {
    const auto started = monotonicTimeMs();
    const auto startedAtUnixMs = wallTimeMsForTest();
    const double deadline = started + request.timeoutMs;
    const auto& config = server.placementConfig();
    TrialVerdict verdict;
    std::string invocationId, processId, entrypoint = "null", culprit;
    uint64_t generation = 0;
    bool dispatched = false, coldLoad = false, quarantined = false;
    double waitMs = 0, codeMs = 0, probeMs = 0, loadMs = -1;
    std::vector<std::string> secrets, variables;

    // The definition by its hash: this Server's index, else (with the
    // content hash) the object store, where it is published before its
    // key; else wait for this Server's view to apply the key. A version
    // with secrets also waits for the view to reach the revision the tool
    // read (the values it validated reach the runtime with the definition);
    // a version without needs nothing else from the view. A revision this
    // Server has not applied yet makes it read the desired state at once
    // rather than at its next poll -- also when the definition is not found:
    // a view behind the tool's may still name the label with another
    // definition (a tenant deleted and created again since this Server's last
    // poll), which refuses the fetch until the view forgets it, so the fetch
    // is tried again after that read. Normally all of this takes
    // milliseconds; a Server cut off from the object store gives up after
    // VIEW_WAIT_MS, so that the tool soon tries another.
    const double viewDeadline = std::min(deadline, started + VIEW_WAIT_MS);
    std::shared_ptr<const VersionDefinition> version;
    std::string waiting;
    bool fetched = false, refreshed = false;
    while (true) {
      version = server.definitionByHash(request.definitionHash);
      if (!version && !request.contentHash.empty() && !fetched) {
        fetched = true;
        std::string reason;
        version = co_await server.resolveVersion({request.workerId, request.versionId, request.contentHash,
            request.definitionHash}, viewDeadline, reason);
      }
      waiting = !version ? "the definition" :
          !version->secrets.empty() && server.deploymentRevision() < request.revision
              ? "deployment revision " + std::to_string(request.revision) + " (the version's secrets)" :
          !server.missingSecrets(*version).empty() ? "the values of the version's secrets" : "";
      if (waiting.empty() || monotonicTimeMs() >= viewDeadline) break;
      if (!refreshed && (version || server.deploymentRevision() < request.revision)) {
        refreshed = true;
        if (!version) fetched = false;
        co_await server.refreshDesiredState().exclusiveJoin(timer.afterDelay(
            static_cast<int64_t>(std::max(0.0, viewDeadline - monotonicTimeMs()) * 1000) * kj::MICROSECONDS));
        continue;
      }
      co_await timer.afterDelay(5 * kj::MILLISECONDS);
    }
    waitMs = monotonicTimeMs() - started;
    // Not getting as far as loading is this Server's condition, not a
    // verdict on the code: UNAVAILABLE, and the tool tries another member.
    if (!waiting.empty()) {
      verdict = {"UNAVAILABLE", "", "timed out before loading: this Server does not have " + waiting};
    } else if (version->workerId != request.workerId || version->versionId != request.versionId) {
      verdict = {"UNAVAILABLE", "", "the definition hash names another Worker version"};
    } else if (!version->hasCode()) {
      verdict.message = "nothing to load: the version serves static assets only";
    } else {
      std::tie(secrets, variables) = server.trialRedactions(*version);
      const auto codeStarted = monotonicTimeMs();
      auto unavailable = co_await server.ensureCode(*version, deadline);
      codeMs = monotonicTimeMs() - codeStarted;
      auto process = unavailable.empty() ? server.trialProcess(version->workerId) : nullptr;
      if (!unavailable.empty()) {
        verdict = {"UNAVAILABLE", "", monotonicTimeMs() >= deadline ? "timed out before loading: fetching the code" : unavailable};
      } else if (!process) {
        verdict = {"UNAVAILABLE", "", server.trialsQuarantined() ? "no ready quarantine process" : "no ready workerd process"};
      } else {
        invocationId = server.nextTrialInvocationId();
        processId = process->processId;
        generation = process->generation;
        dispatched = true;
        const auto probeStarted = monotonicTimeMs();
        auto opened = server.openTrial(*version, process, invocationId, deadline);
        // Kept until the probe ends: dropping it would cancel the dispatch.
        auto ready = kj::mv(opened.ready);
        std::string report, failure;
        bool timedOut = false;
        try {
          auto getHttp = opened.dispatcher.getHttpServiceRequest().send();
          auto service = factory.capnpToKj(getHttp.getHttp());
          auto client = kj::newHttpClient(*service);
          kj::HttpHeaders requestHeaders(headers);
          auto call = client->request(kj::HttpMethod::GET, "http://coworkerd-trial/", requestHeaders, uint64_t(0));
          auto exit = process->onExit().then([]() -> kj::HttpClient::Response { KJ_UNREACHABLE; });
          auto response = co_await timer.timeoutAfter(micros(deadline - monotonicTimeMs()) * kj::MICROSECONDS,
              kj::mv(call.response).exclusiveJoin(kj::mv(exit)));
          auto body = co_await timer.timeoutAfter(micros(deadline - monotonicTimeMs()) * kj::MICROSECONDS,
              response.body->readAllText(65536));
          KJ_REQUIRE(response.statusCode == 200, "the runtime answered the trial with an HTTP error", response.statusCode);
          report = body.cStr();
        } catch (const kj::Exception& error) {
          failure = error.getDescription().cStr();
          timedOut = monotonicTimeMs() >= deadline;
        }
        bool exited = false;
        if (!failure.empty() && !timedOut) {
          // The connection to workerd failed: let the pool observe the exit
          // (it reads the culprit then), bounded.
          std::string ended;
          try { co_await timer.timeoutAfter(1000 * kj::MILLISECONDS, process->onExit()); }
          catch (const kj::Exception& error) { ended = error.getDescription().cStr(); }
          exited = ended.find("exited") != std::string::npos;
          if (exited) culprit = server.culpritOf(invocationId);
        }
        // The probe ends here, whatever happened: its process call is released,
        // and on a timeout the request into workerd is cancelled.
        const bool cpuExceeded = process->cpuLimitExceeded(), memoryExceeded = process->memoryLimitExceeded();
        process->finish();
        probeMs = monotonicTimeMs() - probeStarted;
        if (timedOut) {
          verdict = {"TIMEOUT", "", "timed out while loading (" + std::to_string(request.timeoutMs) + " ms); the probe was cancelled"};
        } else if (!culprit.empty()) {
          quarantined = server.quarantined(version->workerId);
          verdict = {"LIMIT_EXCEEDED", culprit, culprit == "memory"
              ? "memory limit exceeded while loading: the workerd process died of a fatal out-of-memory and this version was its culprit"
              : "CPU limit exceeded while loading: the workerd process was killed and this version was its culprit"};
        } else if (!failure.empty()) {
          verdict = {"LOAD_FAILED", "", exited ? "the workerd process exited during the trial (no culprit recorded)"
              : "the trial could not reach the runtime: " + failure};
        } else {
          capnp::MallocMessageBuilder message;
          auto json = message.initRoot<capnp::JsonValue>();
          capnp::JsonCodec().decodeRaw(kj::arrayPtr(report.data(), report.size()), json);
          using coworkerd::json::field;
          auto flag = [&](const char* name) { auto value = field(json, name); return value.isBoolean() && value.getBoolean(); };
          auto string = [&](const char* name) { auto value = field(json, name); return value.isString() ? std::string(value.getString().cStr()) : std::string(); };
          coldLoad = flag("coldLoad");
          if (auto value = field(json, "loadMs"); value.isNumber()) loadMs = value.getNumber();
          if (flag("ok")) {
            const auto kind = string("defaultKind");
            std::vector<std::string> named;
            if (auto list = field(json, "namedEntrypoints"); list.isArray())
              for (auto item : list.getArray()) if (item.isString()) named.push_back(item.getString().cStr());
            std::ostringstream out;
            out << "{\"hasDefault\":" << (flag("hasDefault") ? "true" : "false") << ",\"defaultKind\":"
                << (kind.empty() ? "null" : "\"" + jsonEscape(kind) + "\"") << ",\"namedEntrypoints\":[";
            for (size_t i = 0; i < named.size(); ++i) out << (i ? "," : "") << "\"" << jsonEscape(named[i]) << "\"";
            out << "]}";
            entrypoint = out.str();
            std::string names;
            for (const auto& name : named) names += (names.empty() ? "" : ", ") + name;
            if (kind == "class" || kind == "object") {
              verdict.message = "the default export serves fetch (" + kind + ")";
              // The isolate stays for the next call; the load check before it need not load again.
              server.confirmLoaded(*process, version->definitionHash, "");
            }
            else if (flag("hasDefault")) verdict = {"ENTRYPOINT_MISSING", "", "the default export has no fetch handler"};
            else if (request.routed) verdict = {"ENTRYPOINT_MISSING", "", "there is no default export, and the Worker has routes"};
            else if (!named.empty()) verdict.message = "no default export; named entrypoints serve fetch: " + names;
            else verdict = {"ENTRYPOINT_MISSING", "", "there is no default export and no other entrypoint that serves fetch"};
            // Every Durable Object class the version declares is an exported class that extends DurableObject or has fetch.
            if (auto classes = field(json, "durableObjectClasses"); verdict.category.empty() && classes.isArray()) {
              for (auto entry : classes.getArray()) {
                auto usable = field(entry, "usable"), name = field(entry, "name"), why = field(entry, "reason");
                if (usable.isBoolean() && usable.getBoolean()) continue;
                verdict = {"ENTRYPOINT_MISSING", "", "the Durable Object class " + std::string(name.isString() ? name.getString().cStr() : "?") +
                    " " + (why.isString() ? why.getString().cStr() : "cannot be used")};
                break;
              }
            }
          } else {
            const auto applied = server.effectiveLimits(request.workerId);
            verdict = classifyTrialFailure(string("stage"), string("name"), string("message"), cpuExceeded, memoryExceeded,
                applied.cpuMs, applied.memoryMiB);
          }
        }
      }
    }
    verdict.message = trialClean(verdict.message, secrets, variables);
    const auto elapsedMs = monotonicTimeMs() - started;
    const bool ok = verdict.category.empty();
    auto quoted = [](const std::string& value) { return value.empty() ? std::string("null") : "\"" + jsonEscape(value) + "\""; };
    std::ostringstream out;
    out << std::setprecision(6) << "{\"nodeId\":\"" << jsonEscape(config.serverId) << "\",\"workerId\":\"" << request.workerId
        << "\",\"versionId\":\"" << request.versionId << "\",\"definitionHash\":\"" << request.definitionHash
        << "\",\"ok\":" << (ok ? "true" : "false") << ",\"category\":" << quoted(verdict.category)
        << ",\"limit\":" << quoted(verdict.limit) << ",\"message\":" << quoted(verdict.message)
        << ",\"culprit\":" << (culprit.empty() ? "null" : "\"confirmed\"") << ",\"quarantined\":" << (quarantined ? "true" : "false")
        << ",\"dispatched\":" << (dispatched ? "true" : "false") << ",\"invocationId\":" << quoted(invocationId)
        << ",\"processId\":" << quoted(processId) << ",\"processGeneration\":" << (dispatched ? std::to_string(generation) : "null")
        << ",\"coldLoad\":" << (dispatched && verdict.category != "TIMEOUT" ? (coldLoad ? "true" : "false") : "null")
        << ",\"entrypoint\":" << entrypoint << ",\"routed\":" << (request.routed ? "true" : "false")
        << ",\"timeoutMs\":" << request.timeoutMs << ",\"elapsedMs\":" << elapsedMs << ",\"waitMs\":" << waitMs
        << ",\"codeMs\":" << codeMs << ",\"probeMs\":" << probeMs << ",\"loadMs\":" << (loadMs < 0 ? std::string("null") : std::to_string(loadMs))
        << ",\"requestedRevision\":\"" << request.revision << "\",\"viewRevision\":\"" << server.deploymentRevision()
        << "\",\"startedAtUnixMs\":" << startedAtUnixMs << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << '}';
    auto answer = out.str();
    server.recordTrial(verdict.category, dispatched, coldLoad, answer);
    // The log line names the category and the message's length, never the message.
    std::cout << "[trial-load] {\"nodeId\":\"" << jsonEscape(config.serverId) << "\",\"target\":\"" << request.workerId << "@"
              << request.versionId << "\",\"definitionHash\":\"" << request.definitionHash << "\",\"ok\":" << (ok ? "true" : "false")
              << ",\"category\":" << quoted(verdict.category) << ",\"limit\":" << quoted(verdict.limit)
              << ",\"messageBytes\":" << verdict.message.size() << ",\"dispatched\":" << (dispatched ? "true" : "false")
              << ",\"invocationId\":" << quoted(invocationId) << ",\"processId\":" << quoted(processId)
              << ",\"coldLoad\":" << (coldLoad ? "true" : "false") << ",\"elapsedMs\":" << elapsedMs
              << ",\"observedAtUnixMs\":" << wallTimeMsForTest() << '}' << std::endl;
    co_return answer;
  }

  kj::Promise<void> send(kj::HttpService::Response& response, uint status, kj::StringPtr statusText, std::string text) {
    kj::HttpHeaders responseHeaders(headers);
    responseHeaders.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
    auto body = response.send(status, statusText, responseHeaders, text.size());
    co_await body->write(kj::arrayPtr(reinterpret_cast<const kj::byte*>(text.data()), text.size()));
  }
};

class GatewayService final: public kj::HttpService, public kj::HttpServerCallbacks {
public:
  GatewayService(
      ServerCore& server,
      PeerForwarder& peerForwarder,
      HttpForwarder& forwarder,
      TrialService& trials,
      WorkerLogsEndpoint& logs,
      const kj::HttpHeaderTable& headerTable)
      : server(server), peerForwarder(peerForwarder), forwarder(forwarder), trials(trials), logs(logs),
        headerTable(headerTable), assets(server, headerTable) {}

  // A draining Server closes each connection after its response: clients stop reusing
  // connections to it, so none is open when the ingress closes.
  bool shouldClose() override { return server.draining(); }

  kj::Promise<void> request(
      kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody,
      Response& response) override {
    auto tracked = kj::heap<TrackingHttpResponse>(response);
    auto request = kj::evalNow([&]() {
      return routeRequest(method, url, headers, requestBody, *tracked);
    });
    // A request (not a management endpoint) still in flight at the drain deadline is cut.
    if (!std::string_view(url.cStr()).starts_with("/__coworkerd/")) request = request.exclusiveJoin(server.ingressCutoff());
    return request.catch_([this, ptr = tracked.get()](kj::Exception&& error)
        -> kj::Promise<void> {
      if (ServerCore::drainCut(error)) {
        server.countCutIngress();
        std::cerr << "[server] RESULT_UNCERTAIN: ingress request cut at the drain deadline" << std::endl;
        if (ptr->hasStarted()) return std::move(error);
        return sendText(*ptr, 503, "Service Unavailable",
            rejectionJson("RESULT_UNCERTAIN", "request cut at the Server's drain deadline", 0), "application/json");
      }
      std::cerr << "[server] request failed: " << cleanReason(error.getDescription().cStr()) << std::endl;
      if (ptr->hasStarted()) return std::move(error);
      return ptr->sendError(500, "Coworkerd execution failed", headerTable);
    }).attach(std::move(tracked));
  }

private:
  kj::Promise<void> routeRequest(
      kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody,
      Response& response) {
    // The management endpoints: with a token set, nothing under /__coworkerd/ but ready and
    // probe runs without it.
    if (auto refused = server.managementRefusal(url, headers); !refused.empty()) {
      server.managementRefused("ingress", method, url, refused);
      co_return co_await sendUnauthorized(response, headerTable);
    }
    if (url == "/__coworkerd/probe") {
      // Liveness: the process serves, whatever its readiness or drain.
      co_return co_await sendText(response, 200, "OK", "alive\n", "text/plain");
    }
    if (url == "/__coworkerd/ready") {
      // Not ready once draining: a front load balancer takes the Server out.
      const bool ready = server.processesReady() && !server.draining();
      co_return co_await sendText(response, ready ? 200 : 503, ready ? "OK" : "Service Unavailable",
          server.draining() ? "draining\n" : "ready\n", "text/plain");
    }
    if (url == "/__coworkerd/drain" || url == "/__coworkerd/drain?wait=1") {
      // POST starts the exit, as the first SIGTERM does (idempotent); GET shows its progress;
      // GET ?wait=1 answers once the lease is revoked, just before the process exits.
      if (method == kj::HttpMethod::POST && url == "/__coworkerd/drain") {
        server.requestDrain("endpoint");
        co_return co_await sendText(response, 200, "OK", server.drainJson(), "application/json");
      }
      if (method != kj::HttpMethod::GET) {
        co_return co_await sendText(response, 405, "Method Not Allowed",
            "{\"code\":\"METHOD_NOT_ALLOWED\",\"reason\":\"POST /__coworkerd/drain starts the exit; GET shows it\"}", "application/json");
      }
      if (url == "/__coworkerd/drain") co_return co_await sendText(response, 200, "OK", server.drainJson(), "application/json");
      if (!server.draining()) {
        co_return co_await sendText(response, 409, "Conflict",
            "{\"code\":\"NOT_DRAINING\",\"reason\":\"the Server is not draining\"}", "application/json");
      }
      ++server.pendingDrainResponses;
      KJ_DEFER(--server.pendingDrainResponses);
      auto record = co_await server.waitForDrainEnd();
      co_return co_await sendText(response, 200, "OK", record, "application/json");
    }
    if (std::string(url.cStr()).starts_with("/__coworkerd/process-placement?workers=")) {
      auto query = std::string(url.cStr()).substr(std::string("/__coworkerd/process-placement?workers=").size());
      std::vector<std::string> workers; std::istringstream input(query); std::string worker;
      while (std::getline(input, worker, ',')) { KJ_REQUIRE(coworkerd::workerFullName(worker), "invalid debug workerId"); workers.push_back(worker); }
      co_return co_await sendText(response, 200, "OK", server.processPlacement(workers), "application/json");
    }
    if (std::string(url.cStr()).starts_with("/__coworkerd/placement?")) {
      auto query = std::string(url.cStr()).substr(std::string("/__coworkerd/placement?workers=").size());
      std::vector<std::string> workers; std::istringstream input(query); std::string worker;
      while (std::getline(input, worker, ',')) { KJ_REQUIRE(coworkerd::workerFullName(worker), "invalid debug workerId"); workers.push_back(worker); }
      co_return co_await sendText(response, 200, "OK", server.debugPlacement(workers), "application/json");
    }
    if (url == "/__coworkerd/load-band") {
      if (method != kj::HttpMethod::POST) {
        co_return co_await sendText(response, 405, "Method Not Allowed",
            "{\"code\":\"METHOD_NOT_ALLOWED\",\"reason\":\"load-band requires POST\"}", "application/json");
      }
      auto text = co_await requestBody.readAllText(64);
      server.setLoadBand(text.cStr());
      co_return co_await sendText(response, 200, "OK", server.placementJson(), "application/json");
    }
    if (url == "/__coworkerd/deployment") {
      co_return co_await sendText(response, 200, "OK", server.deploymentJson(), "application/json");
    }
    if (url == TrialService::PATH) {
      co_return co_await trials.serve(method, requestBody, response);
    }
    if (WorkerLogsEndpoint::matches(url)) {
      co_return co_await logs.serve(method, url, response);
    }
    if (url == "/__coworkerd/state") {
      co_return co_await sendText(
          response, 200, "OK",
          server.stateJson(peerForwarder.stateJson(), peerForwarder.avoidanceJson()), "application/json");
    }
    {
      // Process rotation. POST /__coworkerd/rotate?all=1 or ?process=p0&process=p1;
      // GET /__coworkerd/rotation[?requests=1,2].
      std::string target = url.cStr(), query;
      if (auto mark = target.find('?'); mark != std::string::npos) { query = target.substr(mark + 1); target.resize(mark); }
      if (target == "/__coworkerd/rotate" || target == "/__coworkerd/rotation") {
        std::vector<std::string> processes;
        std::vector<uint64_t> requests;
        bool all = false, valid = true;
        std::istringstream fields(query);
        for (std::string field; std::getline(fields, field, '&');) {
          if (field.empty()) continue;
          if (field == "all=1") all = true;
          else if (field.starts_with("process=") && safeName(field.substr(8))) processes.push_back(field.substr(8));
          else if (field.starts_with("requests=")) {
            std::istringstream ids(field.substr(9));
            for (std::string id; std::getline(ids, id, ',');) {
              valid = valid && !id.empty() && id.size() <= 18 && std::all_of(id.begin(), id.end(), [](char c) { return c >= '0' && c <= '9'; });
              if (valid) requests.push_back(std::stoull(id));
            }
          } else valid = false;
        }
        const bool rotate = target == "/__coworkerd/rotate";
        uint status = 200;
        std::string body;
        if (rotate && method != kj::HttpMethod::POST) {
          status = 405; body = "{\"code\":\"METHOD_NOT_ALLOWED\",\"reason\":\"rotate requires POST\"}";
        } else if (!valid || (rotate && (all == !processes.empty() || !requests.empty())) || (!rotate && (all || !processes.empty()))) {
          status = 400; body = "{\"code\":\"INVALID_ROTATION_REQUEST\",\"reason\":\"use ?all=1 or ?process=ID (repeatable) to rotate, ?requests=N,... to follow\"}";
        } else {
          try {
            body = rotate ? server.requestRotation(all ? std::vector<std::string>() : processes) : server.rotationJson(requests);
          } catch (const kj::Exception& error) {
            status = 400;
            body = "{\"code\":\"INVALID_ROTATION_REQUEST\",\"reason\":\"" + jsonEscape(error.getDescription().cStr()) + "\"}";
          }
        }
        co_return co_await sendText(response, status, statusText(status), body, "application/json");
      }
    }
    if (url == "/__coworkerd/reload") {
      if (method != kj::HttpMethod::POST) {
        co_return co_await sendText(response, 405, "Method Not Allowed",
            "{\"code\":\"METHOD_NOT_ALLOWED\",\"reason\":\"reload requires POST\"}", "application/json");
      }
      if (server.draining()) {
        co_return co_await sendText(response, 409, "Conflict",
            "{\"code\":\"SERVER_DRAINING\",\"reason\":\"a draining Server applies no deployment change\"}", "application/json");
      }
      if (server.placementConfig().desiredState.backend != "file") {
        co_return co_await sendText(response, 409, "Conflict",
            "{\"code\":\"RELOAD_UNAVAILABLE\",\"reason\":\"reload is only available for the file backend\"}", "application/json");
      }
      server.reload();
      co_return co_await sendText(
          response, 200, "OK",
          server.stateJson(peerForwarder.stateJson(), peerForwarder.avoidanceJson()), "application/json");
    }

    if (server.draining()) {
      // A new root while draining: refused before anything executes; the client (or the
      // front load balancer) sends it to another Server.
      auto refusal = server.drainingRefusal(true);
      server.observeLocalRefusal(refusal, "localIngress");
      server.countDrainRefusal("localIngress");
      co_return co_await sendText(response, 503, "Service Unavailable",
          rejectionJson(refusal.code, refusal.reason, 0), "application/json");
    }
    // Counted until the response is complete, wherever the root executes (the Server's drain).
    server.ingressBegin();
    KJ_DEFER(server.ingressEnd());
    auto timing = pathTiming("ingress");
    auto hostHeader = headers.get(kj::HttpHeaderId::HOST);
    std::string host = "localhost";
    KJ_IF_SOME(value, hostHeader) {
      host = value.cStr();
    }
    host = hostWithoutPort(host);
    std::string path = url.cStr();
    auto query = path.find('?');
    if (query != std::string::npos) path.resize(query);
    const auto deadline = server.rootDeadline();
    auto selected = server.route(host, path);
    if (selected.kind != coworkerd::DeploymentState::Selection::Kind::SELECTED) {
      // Nothing executed: no route, a broken route, or a Worker whose
      // deployment state is invalid at this revision.
      using Kind = coworkerd::DeploymentState::Selection::Kind;
      const bool missing = selected.kind == Kind::NOT_FOUND;
      const std::string code = missing ? "ROUTE_NOT_FOUND" : selected.kind == Kind::ROUTE_INVALID ? "ROUTE_INVALID" : "WORKER_INVALID";
      const std::string reason = missing ? "no route matched " + host + path :
          (selected.kind == Kind::ROUTE_INVALID ? "route " : "Worker ") +
          (selected.kind == Kind::ROUTE_INVALID ? selected.routeKey : selected.workerId) + ": " + selected.reason;
      auto failure = server.refusal(code, reason, missing ? 404 : 503, true);
      server.observeLocalRefusal(failure, "localIngress");
      co_return co_await sendText(response, failure.status, statusText(failure.status),
          rejectionJson(code, reason, deadline), "application/json");
    }
    // A tenant being deleted, or one whose record is gone: nothing of it starts here any more.
    if (const auto unavailable = server.tenantUnavailable(coworkerd::tenantOf(selected.version->workerId)); !unavailable.empty()) {
      auto failure = server.refusal("TENANT_UNAVAILABLE", unavailable, 503, true);
      server.observeLocalRefusal(failure, "localIngress");
      co_return co_await sendText(response, 503, "Service Unavailable",
          rejectionJson(failure.code, failure.reason, deadline), "application/json");
    }
    if (selected.version->assets && co_await assets.serve(*selected.version, AssetLayer::Source::INGRESS, method, path, headers,
        response, deadline)) co_return;
    if (timing) timing->target = selected.version->key();
    pathMark(timing, "route");
    // An upgrade to a WebSocket takes one of its tenant's places on this node before anything is sent on: beyond
    // ingress.webSocketsPerTenant it is refused here, and nothing executes. The request keeps its handshake headers.
    std::shared_ptr<IngressWebSocket> webSocket;
    if (method == kj::HttpMethod::GET && headers.isWebSocket()) {
      const auto tenant = coworkerd::tenantOf(selected.version->workerId);
      auto& sockets = server.webSockets();
      if (!sockets.reserve(tenant)) {
        auto refused = server.refusal("WEBSOCKET_LIMIT", "the tenant holds " + std::to_string(sockets.limitPerTenant()) +
            " WebSocket connections on this node, the limit of ingress.webSocketsPerTenant", 429, true);
        refused.inFlight = sockets.placesOf(tenant);
        refused.limit = sockets.limitPerTenant();
        server.observeLocalRefusal(refused, "localIngress");
        co_return co_await sendText(response, 429, "Too Many Requests", refusalBody(refused, deadline), "application/json");
      }
      webSocket = std::make_shared<IngressWebSocket>(sockets, tenant);
      webSocket->workerId = selected.version->workerId;
      webSocket->versionId = selected.version->versionId;
    }
    if (server.placementConfig().ingressPlacement && !server.placementConfig().hasPlacement(selected.version->workerId)) {
      auto allocation = server.allocateRoot(selected.pins, deadline);
      allocation.timing = timing;
      pathMark(timing, "allocate-root");
      if (webSocket) webSocket->treeId = allocation.treeId;
      bool proxy = false;
      std::string placedOn;
      auto dispatcher = co_await peerForwarder.place(selected.version, allocation, &proxy, &placedOn);
      if (webSocket) { webSocket->forwarded = proxy; webSocket->executingNodeId = placedOn; }
      if (proxy) server.proxyIngress(true);
      auto proxySlot = kj::defer([&] { if (proxy) server.proxyIngress(false); });
      co_return co_await forwarder.forward({std::move(dispatcher), kj::READY_NOW}, method,
          kj::str("http://", host.c_str(), url), headers.clone(), requestBody, response, timing, selected.version->key(),
          std::move(webSocket));
    }
    auto refusal = server.admission(selected.version->workerId, selected.version->definitionHash, true, deadline);
    pathMark(timing, "admission-end");
    if (refusal) co_return co_await sendText(response, refusal->status, statusText(refusal->status), refusalBody(*refusal, deadline),
        "application/json");
    auto slot = server.reserve(true);
    slot->timing = timing;
    pathMark(timing, "code-start");
    std::string unavailable;
    unavailable = co_await server.ensureCode(*selected.version, deadline);
    pathMark(timing, "code-end");
    if (!unavailable.empty()) {
      server.rejectVersionUnavailable(unavailable);
      server.observeLocalRefusal(server.refusal("VERSION_UNAVAILABLE", unavailable, 409, true), "localIngress");
      co_return co_await sendText(response, 409, "Conflict",
          rejectionJson("VERSION_UNAVAILABLE", unavailable, deadline), "application/json");
    }
    if (remainingLeaseMs(deadline) == 0) {
      auto failure = server.refusal("LEASE_EXPIRED", "lease expired before root execution", 410, true);
      server.observeLocalRefusal(failure, "localIngress");
      co_return co_await sendText(response, 410, "Gone", refusalBody(failure, deadline), "application/json");
    }
    if (auto missing = server.secretRefusal(*selected.version, true)) {
      server.observeLocalRefusal(*missing, "localIngress");
      co_return co_await sendText(response, missing->status, statusText(missing->status),
          rejectionJson(missing->code, missing->reason, deadline), "application/json");
    }
    auto check = co_await server.loadCheck(selected.version, "", deadline, true);
    if (check.refusal) {
      server.observeLocalRefusal(*check.refusal, "localIngress");
      if (check.refusal->code == "LOAD_FAILED")
        server.loadFailed(*selected.version, *check.refusal, server.placementConfig().serverId, "ingress", false);
      co_return co_await sendText(response, check.refusal->status, statusText(check.refusal->status),
          refusalBody(*check.refusal, deadline), "application/json");
    }
    std::optional<InvocationRejection> processFailure;
    try {
    auto targetKey = selected.version->key();
    auto opening = slot;
    auto opened = server.open(std::move(selected.version), "ingress", std::move(selected.pins), deadline, std::move(slot), check);
    if (webSocket && opening->process) webSocket->treeId = opening->process->treeId;
    auto forwardedUrl = kj::str("http://", host.c_str(), url);
    auto forwardedHeaders = headers.clone();
    co_return co_await forwarder.forward(
        std::move(opened),
        method,
        std::move(forwardedUrl),
        std::move(forwardedHeaders),
        requestBody,
        response, timing, std::move(targetKey), std::move(webSocket));
    } catch (const ProcessUnavailable& error) {
      processFailure = server.processRefusal(error, true);
    }
    server.observeLocalRefusal(*processFailure, "localIngress");
    co_return co_await sendText(response, 503, "Service Unavailable", refusalBody(*processFailure, deadline), "application/json");
  }

  kj::Promise<void> sendText(
      Response& response,
      uint status,
      kj::StringPtr statusText,
      std::string text,
      kj::StringPtr contentType) {
    kj::HttpHeaders headers(headerTable);
    headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, contentType);
    auto body = response.send(status, statusText, headers, text.size());
    co_await body->write(kj::arrayPtr(
        reinterpret_cast<const kj::byte*>(text.data()), text.size()));
  }

  ServerCore& server;
  PeerForwarder& peerForwarder;
  HttpForwarder& forwarder;
  TrialService& trials;
  WorkerLogsEndpoint& logs;
  const kj::HttpHeaderTable& headerTable;
  AssetLayer assets;
};

template <typename T>
kj::ForkedPromise<T> forkedFulfiller(kj::Own<kj::PromiseFulfiller<T>>& fulfiller) {
  auto paf = kj::newPromiseAndFulfiller<T>();
  fulfiller = kj::mv(paf.fulfiller);
  return paf.promise.fork();
}

// The Server's exit: announce (readiness off, new roots and invocations from other
// Servers refused, member value draining), drain the in-flight call trees (a rotation's drain
// criterion; at the deadline what is left is lost with its process), close every process (a
// rotation's close, no new generation), revoke the lease, exit 0. The first SIGTERM or SIGINT, or
// POST /__coworkerd/drain, starts it; the next signal exits at once after killing every process
// (exit code 2). A drain period of 0 stops serving, revokes the lease and exits without draining.
// Every step is a [server-drain] line and part of the record the state endpoint shows.
class DrainController final: private kj::TaskSet::ErrorHandler {
public:
  // `drainConnections` closes the ingress's idle connections and resolves once none is open.
  DrainController(ServerCore& server, coworkerd::ProcessPool& runtime, coworkerd::Membership& membership,
      kj::Timer& timer, uint32_t announceDelayMs, std::function<kj::Promise<void>()> drainConnections)
      : server(server), runtime(runtime), membership(membership), timer(timer),
        timeoutMs(server.drainTimeoutMs()), announceDelayMs(announceDelayMs), drainConnections(std::move(drainConnections)) {
    server.onDrainRequested([this](std::string trigger) { begin(std::move(trigger)); });
  }
  ~DrainController() noexcept(false) { server.onDrainRequested({}); }
  kj::Promise<int> done() { return finished.addBranch(); }
  kj::Promise<void> started() { return start.addBranch(); }
  // Resolves when the ingress listener is to close (new connections are refused from then on).
  kj::Promise<void> ingressClosed() { return ingress.addBranch(); }

  // The first signal starts the exit (or joins one already started); the next one is the emergency.
  kj::Promise<void> signals(kj::Promise<int> first, kj::UnixEventPort& port) {
    begin(signalName(co_await first));
    auto next = co_await port.onSignal(SIGTERM).exclusiveJoin(port.onSignal(SIGINT));
    emergency(signalName(next.si_signo));
    co_await kj::Promise<void>(kj::NEVER_DONE);
  }

  // (objectStore) Another process holds this node's id: the same drain, ending with exit code 3 (the supervisor
  // restarts it, as a new epoch).
  void identityTaken(std::string reason) {
    exitCode = 3;
    server.drainStep("identity-taken", "\"reason\":\"" + jsonEscape(cleanReason(reason)) + "\"");
    begin("identity-taken");
  }

  void begin(std::string trigger) {
    if (begun) return;
    begun = true;
    if (timeoutMs == 0) {
      // The drain is off: stop serving, revoke the lease and exit.
      auto& r = server.drainRecord();
      r.trigger = trigger; r.phase = "immediate";
      server.drainStep("immediate", "\"trigger\":\"" + jsonEscape(trigger) + "\"");
      complete(exitCode);
      return;
    }
    startedAt = monotonicTimeMs();
    server.beginDrain(trigger, runtime.drainState(startedAt, timeoutMs).json);
    runtime.beginShutdown();
    startFulfiller->fulfill();
    tasks.add(announce());
    tasks.add(sequence());
  }

  void emergency(std::string trigger) {
    if (emergencyBegun || completed) return;
    emergencyBegun = true;
    auto& r = server.drainRecord();
    r.emergency = trigger;
    r.emergencyAtUnixMs = wallTimeMsForTest();
    r.inFlightAtEmergency = server.inFlightJson(runtime.drainState(startedAt, timeoutMs).json);
    // What is in flight is lost with its process (each kill record lists it).
    auto killed = runtime.killAll("server emergency exit");
    r.emergencyKilledCalls = killed.calls; r.emergencyKilledContexts = killed.contexts;
    r.emergencyKilledProcesses = killed.processes;
    server.drainStep("emergency", "\"trigger\":\"" + jsonEscape(trigger) + "\",\"inFlight\":" + r.inFlightAtEmergency +
        ",\"killedCalls\":" + std::to_string(killed.calls) + ",\"killedContexts\":" + std::to_string(killed.contexts) +
        ",\"killedProcesses\":" + std::to_string(killed.processes));
    tasks.add(emergencyExit());
  }

private:
  static std::string signalName(int signal) { return signal == SIGINT ? "SIGINT" : "SIGTERM"; }
  void complete(int code) {
    if (completed) return;
    completed = true;
    finish->fulfill(kj::cp(code));
  }
  void taskFailed(kj::Exception&& error) override {
    server.drainStep("failed", "\"error\":\"" + jsonEscape(cleanReason(error.getDescription().cStr())) + "\"");
    complete(1);
  }

  // The authority learns of the exit; retried while the drain lasts. The Server refuses new
  // work from the announcement on whether or not this has succeeded.
  kj::Promise<void> announce() {
    if (announceDelayMs) co_await timer.afterDelay(announceDelayMs * kj::MILLISECONDS);
    while (!completed && server.drainRecord().phase != "exited") {
      try {
        co_await membership.announceDraining();
        server.drainStep("announced", "\"announcedAtUnixMs\":" + std::to_string(membership.view().drainAnnouncedAtUnixMs));
        co_return;
      } catch (const kj::Exception& error) {
        server.drainStep("announce-failed", "\"error\":\"" + jsonEscape(cleanReason(error.getDescription().cStr())) + "\"");
        const auto& status = membership.view().drainAnnounce;
        if (status.starts_with("failed: no live lease") || status.starts_with("failed: membership stopped")) co_return;
      }
      co_await timer.afterDelay(250 * kj::MILLISECONDS);
    }
  }

  kj::Promise<void> sequence() {
    auto& r = server.drainRecord();
    const double deadline = startedAt + timeoutMs;
    double lastProgress = 0;
    // 2. Drain: ingress requests (wherever their root executes), executions admitted here (for
    //    this or another Server) and the waitUntil work of every process reach zero.
    while (true) {
      auto state = runtime.drainState(startedAt, timeoutMs);
      const auto now = monotonicTimeMs();
      if (now - lastProgress >= 100) { r.progress = state.json; lastProgress = now; }
      if (server.ingressInFlight() == 0 && server.admittedInFlight() == 0 && state.idle) {
        r.outcome = state.contextsVisible ? "drained" : "waited";
        r.progress = state.json;
        break;
      }
      if (now >= deadline) {
        // What is left is lost with its process: RESULT_UNCERTAIN, counted apart.
        r.outcome = "timeout";
        r.inFlightAtDeadline = server.inFlightJson(state.json);
        auto killed = runtime.killBusy(startedAt, timeoutMs, "server drain timeout");
        r.killedCalls = killed.calls; r.killedContexts = killed.contexts; r.killedProcesses = killed.processes;
        server.drainStep("deadline", "\"inFlight\":" + r.inFlightAtDeadline + ",\"killedCalls\":" + std::to_string(killed.calls) +
            ",\"killedContexts\":" + std::to_string(killed.contexts) + ",\"killedProcesses\":" + std::to_string(killed.processes));
        // The calls lost with their processes answer RESULT_UNCERTAIN on their own; then the ingress
        // requests still waiting on another Server are cut. Both waits are bounded: the processes are
        // closed next anyway.
        auto until = monotonicTimeMs() + 2000;
        while (server.admittedInFlight() > 0 && monotonicTimeMs() < until) co_await timer.afterDelay(5 * kj::MILLISECONDS);
        if (server.ingressInFlight() > 0) server.cutIngress();
        until = monotonicTimeMs() + 2000;
        while ((server.ingressInFlight() > 0 || server.admittedInFlight() > 0) && monotonicTimeMs() < until)
          co_await timer.afterDelay(5 * kj::MILLISECONDS);
        r.progress = runtime.drainState(startedAt, timeoutMs).json;
        break;
      }
      co_await timer.afterDelay(5 * kj::MILLISECONDS);
    }
    r.drainedAtUnixMs = wallTimeMsForTest();
    r.phase = "closing";
    server.drainStep("drained", "\"outcome\":\"" + r.outcome + "\",\"killedCalls\":" + std::to_string(r.killedCalls) +
        ",\"cutIngress\":" + std::to_string(r.cutIngress) + ",\"inFlight\":" + server.inFlightJson(r.progress));
    // 3. Close every slot's processes; nothing new is started.
    r.close = co_await runtime.closeAll();
    r.processesClosedAtUnixMs = wallTimeMsForTest();
    r.phase = "leaving";
    server.drainStep("closed", "\"close\":" + r.close);
    // 4. The last minute of usage: every process is closed, so each execution's row has been read and
    //    counted; this is the last point at which this node may write to its own prefix, and the only one at
    //    which nothing it counted is lost.
    r.usage = co_await server.flushUsage();
    server.drainStep("usage-flushed", "\"usage\":" + r.usage);
    // 5. Leave: the lease is revoked and the member key disappears at once.
    co_await membership.stop();
    r.phase = "closing-ingress";
    r.exitCode = exitCode;
    server.drainStep("revoked", "\"revokeStatus\":\"" + jsonEscape(membership.view().revokeStatus) + "\"");
    server.releaseDrainWaiters();
    // 6. The ingress closes while the event loop still runs, so that nothing a client sends is left
    //    unread at the exit: the listener first (a new connection is refused, which a client or front
    //    load balancer can retry elsewhere), then a short grace in which requests still arriving on open
    //    connections get the 503 (each response closes its connection), then the idle connections close.
    ingressFulfiller->fulfill();
    co_await timer.afterDelay(CONNECTION_GRACE_MS * kj::MILLISECONDS);
    co_await drainConnections().exclusiveJoin(timer.afterDelay(1000 * kj::MILLISECONDS));
    // The answers to GET /__coworkerd/drain?wait=1 leave before the process does.
    const auto until = monotonicTimeMs() + 500;
    while (server.pendingDrainResponses > 0 && monotonicTimeMs() < until) co_await timer.afterDelay(5 * kj::MILLISECONDS);
    r.phase = "exited";
    r.exitAtUnixMs = wallTimeMsForTest();
    server.drainStep("connections-closed");
    server.drainStep("exit", "\"record\":" + server.drainJson());
    complete(exitCode);
  }

  kj::Promise<void> emergencyExit() {
    // A short wait for the revocation at most; the member value already says draining.
    co_await membership.stop().exclusiveJoin(timer.afterDelay(250 * kj::MILLISECONDS));
    auto& r = server.drainRecord();
    r.phase = "exited";
    r.exitCode = 2;
    r.exitAtUnixMs = wallTimeMsForTest();
    server.drainStep("exit", "\"record\":" + server.drainJson());
    server.releaseDrainWaiters();
    complete(2);
  }

  ServerCore& server;
  coworkerd::ProcessPool& runtime;
  coworkerd::Membership& membership;
  kj::Timer& timer;
  const uint32_t timeoutMs, announceDelayMs;
  std::function<kj::Promise<void>()> drainConnections;
  static constexpr uint32_t CONNECTION_GRACE_MS = 100;
  double startedAt = 0;
  bool begun = false, emergencyBegun = false, completed = false;
  // 0, or 3 once another process holds this node's id.
  int exitCode = 0;
  kj::Own<kj::PromiseFulfiller<int>> finish;
  kj::ForkedPromise<int> finished = forkedFulfiller(finish);
  kj::Own<kj::PromiseFulfiller<void>> startFulfiller;
  kj::ForkedPromise<void> start = forkedFulfiller(startFulfiller);
  kj::Own<kj::PromiseFulfiller<void>> ingressFulfiller;
  kj::ForkedPromise<void> ingress = forkedFulfiller(ingressFulfiller);
  kj::TaskSet tasks{*this};
};

kj::Promise<void> runLeaseReaper(ServerCore& server, kj::Timer& timer) {
  while (true) {
    co_await timer.afterDelay(
        ServerCore::LEASE_REAPER_INTERVAL_MS * kj::MILLISECONDS);
    server.reapExpiredLeases();
  }
}

// A Unix socket at `path`: ENOENT when there is none, ECONNREFUSED when nothing listens on it (a
// process that is gone left it behind), 0 when a listener accepts or queues the connection (EAGAIN or
// EINPROGRESS: its backlog is full or the connection is under way), any other errno when it cannot be told.
int probeUnixSocket(const std::string& path) {
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  if (path.size() >= sizeof(address.sun_path)) return ENAMETOOLONG;
  memcpy(address.sun_path, path.c_str(), path.size() + 1);
  const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) return errno;
  fcntl(fd, F_SETFD, FD_CLOEXEC);
  fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
  const int error = ::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0 ? 0 : errno;
  ::close(fd);
  return error == EAGAIN || error == EWOULDBLOCK || error == EINPROGRESS ? 0 : error;
}

// The sockets a Server starting in `stack` removes as left behind (binding.sock, source.sock and the
// processes' <slot>-<generation>.sock in <stack>/run), and the first one a process still listens on, or
// that cannot be probed, with what connect() said. Then another Server runs in this stack.
std::optional<std::pair<std::string, int>> liveStackSocket(const fs::path& stack) {
  const auto runDirectory = stack / "run";
  std::vector<std::string> paths = {(runDirectory / "binding.sock").string(), (runDirectory / "source.sock").string(),
      (runDirectory / "egress.sock").string()};
  std::error_code unreadable;
  for (const auto& entry : fs::directory_iterator(runDirectory, unreadable)) {
    const auto name = entry.path().filename().string();
    if (name.ends_with(".sock") && (name.starts_with("p") || name.starts_with("q")) && name.find('-') != std::string::npos)
      paths.push_back(entry.path().string());
  }
  for (const auto& path : paths) {
    const int error = probeUnixSocket(path);
    if (error != ENOENT && error != ECONNREFUSED) return std::pair{path, error};
  }
  return std::nullopt;
}

// ---- How the Server starts ----
// `coworkerd --config FILE` reads everything from one file with defaults; the
// positional form (`<manifest> <workerd> <http> <binding-rpc> <source>
// [server-config]`) works as well. Both resolve to a Launch.
struct Launch {
  std::string form;              // "serve" or "positional"
  fs::path configFile;           // empty: the positional form without a configuration file
  fs::path stackDirectory, manifestPath;
  std::string workerd, runtime, listenAddress;
  std::string ignoredBindingAddress, ignoredSourceAddress;  // positional, unused
  bool experimentalNaiveLease = false;
  ServerConfig config;
};

constexpr const char* USAGE =
    "usage: coworkerd --config FILE\n"
    "       coworkerd check --config FILE  (the node's self-check: the configuration, the object store, the sandbox)\n"
    "       coworkerd check storage ENDPOINT BUCKET  (the object store's self-check)\n"
    "       coworkerd --version\n"
    "       coworkerd <manifest> <workerd> <http-address> <binding-rpc-address> <source-address> [server-config]"
    "  (the regressions' form)\n";

Launch serveLaunch(const std::vector<std::string>& args, bool naive) {
  Launch launch;
  launch.form = "serve";
  launch.experimentalNaiveLease = naive;
  for (size_t i = 0; i < args.size(); ++i) {
    if (args[i] == "--config" && i + 1 < args.size()) launch.configFile = args[++i];
    else if (args[i].starts_with("--config=")) launch.configFile = args[i].substr(9);
    else KJ_FAIL_REQUIRE("unknown argument; usage: coworkerd --config FILE", args[i].c_str());
  }
  KJ_REQUIRE(!launch.configFile.empty(), "serve requires --config FILE");
  launch.configFile = fs::absolute(launch.configFile);
  auto& c = launch.config = ServerConfig::load(launch.configFile);
  KJ_REQUIRE(c.serverIdGiven, "the configuration needs a nodeId");
  launch.stackDirectory = fs::absolute(c.stackDirectory.empty() ? "/var/lib/coworkerd" : c.stackDirectory).lexically_normal();
  launch.manifestPath = launch.stackDirectory / "workers" / "manifest.json";
  const auto self = executableDirectory();
  launch.workerd = fs::absolute(c.workerdPath.empty() ? self / "workerd" : fs::path(c.workerdPath)).lexically_normal().string();
  launch.runtime = fs::absolute(c.runtimePath.empty() ? self / ".." / "runtime" / "runtime.js" : fs::path(c.runtimePath))
      .lexically_normal().string();
  launch.listenAddress = c.listenAddress.empty() ? "0.0.0.0:8930" : c.listenAddress;
  if (c.peerListenAddress.empty()) c.peerListenAddress = "0.0.0.0:8934";
  KJ_REQUIRE(c.desiredState.backend == "objectStore" && !c.desiredState.objectStore.endpoint.empty(),
      "serve reads the desired state from the object store: set storage.endpoint (the storage block)");
  KJ_REQUIRE(c.membership.backend == "objectStore",
      "serve keeps the members, the suspects and the usage on the object store: membership.backend objectStore (the default "
      "with the storage block)");
  if (c.membership.advertisedAddress.empty()) {
    // What the other Servers dial: this host's name and the peer listener's port.
    char name[256] = {};
    KJ_REQUIRE(gethostname(name, sizeof(name) - 1) == 0 && name[0], "cannot read the host name; set advertisedAddress");
    c.membership.advertisedAddress = std::string(name) + c.peerListenAddress.substr(c.peerListenAddress.rfind(':'));
  }
  if (c.cacheFollowsStack || (c.desiredState.cache.directory.empty() && !c.desiredState.objectStore.endpoint.empty()))
    c.desiredState.cache.directory = launch.stackDirectory / "cache";
  return launch;
}

Launch positionalLaunch(const std::vector<std::string>& args, bool naive) {
  KJ_REQUIRE(args.size() == 5 || args.size() == 6, USAGE);
  Launch launch;
  launch.form = "positional";
  launch.experimentalNaiveLease = naive;
  launch.manifestPath = args[0];
  launch.workerd = fs::absolute(args[1]).string();
  launch.listenAddress = args[2];
  launch.ignoredBindingAddress = args[3];
  launch.ignoredSourceAddress = args[4];
  if (args.size() == 6) launch.configFile = args[5];
  launch.config = ServerConfig::load(launch.configFile);
  KJ_REQUIRE(!launch.config.servePathsGiven(),
      "stackDirectory, workerdPath, runtimePath and listenAddress are for `serve`; the positional form takes them as arguments");
  // <stack>/workers/manifest.json -> <stack>.
  launch.stackDirectory = fs::absolute(launch.manifestPath).parent_path().parent_path();
  launch.runtime = (launch.stackDirectory / "runtime/runtime.js").string();
  if (launch.config.cacheFollowsStack) launch.config.desiredState.cache.directory = launch.stackDirectory / "cache";
  return launch;
}

// The process pool's own environment overrides (process-pool.c++), applied here as well so that the
// configuration a Server reports is the one it runs; the pool applies the same values again.
void applyProcessPoolEnvironment(coworkerd::ProcessPoolConfig& pool, uint64_t memoryBudgetBytes) {
  pool.cpuLimitMs = static_cast<uint32_t>(environmentUnsigned("COWORKERD_CPU_LIMIT_MS", pool.cpuLimitMs, UINT32_MAX));
  pool.memoryLimitMiB = static_cast<uint32_t>(environmentUnsigned("COWORKERD_MEMORY_LIMIT_MIB", pool.memoryLimitMiB, UINT32_MAX));
  pool.quarantineProcesses = static_cast<uint32_t>(environmentUnsigned("COWORKERD_QUARANTINE_PROCESSES", pool.quarantineProcesses, 256));
  for (auto [name, field] : {std::pair{"COWORKERD_IDLE_EVICT_MS", &pool.idleEvictMs},
                             std::pair{"COWORKERD_IDLE_DESTROY_INTERVAL_MS", &pool.idleDestroyIntervalMs},
                             std::pair{"COWORKERD_ROTATION_DRAIN_TIMEOUT_MS", &pool.rotationDrainTimeoutMs},
                             std::pair{"COWORKERD_ROTATION_TERM_GRACE_MS", &pool.rotationTermGraceMs},
                             std::pair{"COWORKERD_ROTATION_BINARY_CHECK_MS", &pool.rotationBinaryCheckMs},
                             std::pair{"COWORKERD_ROTATION_RETRY_MS", &pool.rotationRetryMs}})
    *field = static_cast<uint32_t>(environmentUnsigned(name, *field, UINT32_MAX));
  for (auto [name, field] : {std::pair{"COWORKERD_ROTATION_MAX_AGE_MS", &pool.rotationMaxAgeMs},
                             std::pair{"COWORKERD_ROTATION_MAX_REQUESTS", &pool.rotationMaxRequests},
                             std::pair{"COWORKERD_ROTATION_RESIDENT_BYTES", &pool.rotationResidentBytes}})
    *field = environmentUnsigned(name, *field, UINT64_MAX);
  if (std::getenv("COWORKERD_ROTATION_RESIDENT_BYTES")) {
    pool.rotationResidentBytesDefault = false;
    pool.rotationResidentBytesSource = "environment";
  }
  // The memory threshold nobody gave: 70% of this node's memory, so that a process whose memory keeps growing is
  // replaced before the node's memory runs out, whatever else rotates it.
  if (pool.rotationResidentBytesDefault) {
    const auto [bytes, source] = coworkerd::MemoryWatermark::nodeMemory(memoryBudgetBytes);
    pool.rotationResidentBytes = bytes / 10 * 7;
    pool.rotationResidentBytesDefault = false;
    pool.rotationResidentBytesSource = "default: 70% of " + source + " (" + std::to_string(bytes) + " bytes)";
  }
  for (auto [name, field] : {std::pair{"COWORKERD_GC_BUDGET_MS", &pool.gcBudgetMs},
                             std::pair{"COWORKERD_GC_REFILL_MS_PER_S", &pool.gcRefillMsPerSecond},
                             std::pair{"COWORKERD_GC_LONG_PAUSE_MS", &pool.gcLongPauseMs},
                             std::pair{"COWORKERD_IDLE_GC_MS", &pool.gcIdleCollectAfterMs},
                             std::pair{"COWORKERD_IDLE_GC_MIN_MIB", &pool.gcIdleCollectMinGrowthMiB},
                             std::pair{"COWORKERD_GC_INITIAL_OLD_SPACE_MIB", &pool.gcInitialOldSpaceMiB},
                             std::pair{"COWORKERD_STATIC_HEAP_MIB", &pool.gcRuntimeHeapMiB}})
    *field = static_cast<uint32_t>(environmentUnsigned(name, *field, UINT32_MAX));
  if (const char* mode = std::getenv("COWORKERD_SANDBOX_MODE")) pool.sandboxMode = mode;
  if (const char* mode = std::getenv("COWORKERD_SANDBOX_SYSCALLS")) pool.syscallsMode = mode;
  if (const char* path = std::getenv("COWORKERD_SANDBOX_ALLOWLIST")) pool.allowlistPath = path;
  if (const char* mode = std::getenv("COWORKERD_SANDBOX_SYSCALLS_STEADY")) pool.steadySyscallsMode = mode;
  if (const char* path = std::getenv("COWORKERD_SANDBOX_STEADY_ALLOWLIST")) pool.steadyAllowlistPath = path;
  // The launcher beside this executable, as workerd is in a release.
  // A limit the configuration cannot name (common/config.c++ refuses 0 there): the environment may still turn one
  // off, and then the node has no culprit to name and no suspect to quarantine. Said once, at startup.
  if (pool.cpuLimitMs == 0 || pool.memoryLimitMiB == 0) {
    std::cout << "[server] {\"event\":\"limits\",\"cpuLimitMs\":" << pool.cpuLimitMs << ",\"memoryLimitMiB\":" << pool.memoryLimitMiB
              << ",\"warning\":\"a limit of 0 comes from the environment (COWORKERD_CPU_LIMIT_MS, COWORKERD_MEMORY_LIMIT_MIB): "
                 "no culprit is named for it and no tenant goes to the quarantine processes\"}" << std::endl;
  }
  if (pool.launcherPath.empty()) pool.launcherPath = (executableDirectory() / "coworkerd-launcher").string();
  // The system call allowlist of this build, beside the Server as the release package has it.
  if (pool.allowlistPath.empty()) pool.allowlistPath = (executableDirectory() / "coworkerd-syscalls-startup.txt").string();
  // The steady-state table of the same build, beside it as well.
  if (pool.steadyAllowlistPath.empty()) pool.steadyAllowlistPath = (executableDirectory() / "coworkerd-syscalls-steady.txt").string();
  pool.validate();
}

// The shortest text that reads back as the same double (0.05, not 0.050000000000000003).
std::string shortestNumber(double value) {
  char buffer[64];
  auto [end, error] = std::to_chars(buffer, buffer + sizeof(buffer), value);
  KJ_REQUIRE(error == std::errc(), "cannot format a number");
  return std::string(buffer, end);
}

// The configuration this Server runs with, after defaults and environment overrides, as one
// JSON object: printed at startup and shown by the state endpoint. Credentials and the token appear
// only as whether and where they are set.
std::string configurationJson(const Launch& launch, const ServerConfig& c, const LeaseConfig& lease) {
  auto q = [](const std::string& value) { return "\"" + jsonEscape(value) + "\""; };
  auto b = [](bool value) { return value ? "true" : "false"; };
  const auto& m = c.membership;
  const auto& d = c.desiredState;
  const auto& o = d.objectStore;
  const auto& p = c.processPool;
  const std::string credentials = !c.storageCredentialsFile.empty() ? "file" : !o.accessKey.empty() ? "inline"
      : std::getenv("AWS_ACCESS_KEY_ID") ? "environment" : "none";
  std::ostringstream out;
  out << std::setprecision(17) << "{\"nodeId\":" << q(c.serverId) << ",\"stackDirectory\":" << q(launch.stackDirectory.string())
      << ",\"workerdPath\":" << q(launch.workerd) << ",\"runtimePath\":" << q(launch.runtime)
      << ",\"listenAddress\":" << q(launch.listenAddress) << ",\"peerListenAddress\":" << q(c.peerListenAddress)
      << ",\"advertisedAddress\":" << q(m.advertisedAddress)
      << ",\"membership\":{\"backend\":" << q(m.backend) << ",\"ttlSeconds\":" << m.ttlSeconds
      << ",\"requestTimeoutMs\":" << m.requestTimeoutMs << ",\"startupTimeoutMs\":" << m.startupTimeoutMs << ",\"retryMs\":" << m.retryMs
      << (m.backend == "objectStore" ? ",\"heartbeatIntervalMs\":" + std::to_string(m.heartbeatIntervalMs) + ",\"listIntervalMs\":" +
          std::to_string(m.listIntervalMs) + ",\"livenessGraceMs\":" + std::to_string(m.livenessGraceMs) + ",\"clockSkewToleranceMs\":" +
          std::to_string(m.clockSkewToleranceMs) + ",\"createSettleMs\":" + std::to_string(m.createSettleMs) : std::string())
      << "},\"desiredState\":{\"backend\":" << q(d.backend) << ",\"endpoint\":" << q(d.endpoint)
      << ",\"codeDirectory\":" << q(d.codeDirectory.string()) << ",\"requestTimeoutMs\":" << d.requestTimeoutMs
      << ",\"startupTimeoutMs\":" << d.startupTimeoutMs << ",\"retryMs\":" << d.retryMs
      << "},\"storage\":{\"backend\":" << q(o.backend) << ",\"directory\":" << q(o.directory.string()) << ",\"endpoint\":" << q(o.endpoint)
      << ",\"bucket\":" << q(o.bucket) << ",\"region\":" << q(o.region) << ",\"timeoutMs\":" << o.timeoutMs
      << ",\"credentials\":" << q(credentials) << ",\"credentialsFile\":" << q(c.storageCredentialsFile)
      << ",\"cacheDirectory\":" << q(d.cache.directory.string()) << ",\"cacheMaxBytes\":" << d.cache.maxBytes
      << ",\"prefetchConcurrency\":" << d.cache.prefetchConcurrency
      << "},\"processPool\":{\"count\":" << p.count << ",\"quarantineProcesses\":" << p.quarantineProcesses
      << ",\"inFlightLimit\":" << p.inFlightLimit << ",\"probeIntervalMs\":" << p.probeIntervalMs << ",\"probeOverdueMs\":" << p.probeOverdueMs
      << ",\"stuckTimeoutMs\":" << p.stuckTimeoutMs << ",\"restartBackoffMs\":" << p.restartBackoffMs << ",\"restartMaxMs\":" << p.restartMaxMs
      << ",\"watchdog\":" << b(p.watchdog) << ",\"idleEvictMs\":" << p.idleEvictMs << ",\"idleDestroyIntervalMs\":" << p.idleDestroyIntervalMs
      << "},\"limits\":{\"cpuLimitMs\":" << p.cpuLimitMs << ",\"memoryLimitMiB\":" << p.memoryLimitMiB
      << "},\"rotation\":{\"drainTimeoutMs\":" << p.rotationDrainTimeoutMs << ",\"termGraceMs\":" << p.rotationTermGraceMs
      << ",\"binaryCheckMs\":" << p.rotationBinaryCheckMs << ",\"maxAgeMs\":" << p.rotationMaxAgeMs << ",\"maxRequests\":" << p.rotationMaxRequests
      << ",\"residentBytes\":" << p.rotationResidentBytes << ",\"residentBytesSource\":" << q(p.rotationResidentBytesSource)
      << ",\"retryMs\":" << p.rotationRetryMs
      << "},\"gc\":{\"budgetMs\":" << p.gcBudgetMs << ",\"refillMsPerSecond\":" << p.gcRefillMsPerSecond
      << ",\"longPauseMs\":" << p.gcLongPauseMs << ",\"idleCollectAfterMs\":" << p.gcIdleCollectAfterMs
      << ",\"idleCollectMinGrowthMiB\":" << p.gcIdleCollectMinGrowthMiB << ",\"initialOldSpaceMiB\":" << p.gcInitialOldSpaceMiB
      << ",\"runtimeHeapMiB\":" << p.gcRuntimeHeapMiB
      << "},\"sandbox\":{\"mode\":" << q(p.sandboxMode) << ",\"launcherPath\":" << q(p.launcherPath)
      << ",\"syscalls\":{\"mode\":" << q(p.syscallsMode) << ",\"allowlistPath\":" << q(p.allowlistPath)
      << ",\"steady\":{\"mode\":" << q(p.steadySyscallsMode) << ",\"allowlistPath\":" << q(p.steadyAllowlistPath) << "}}"
      << "},\"placement\":{\"ingressPlacement\":" << b(c.ingressPlacement) << ",\"rootLimit\":" << c.rootLimit << ",\"nestedLimit\":" << c.nestedLimit
      << ",\"idleMs\":" << c.idleMs << ",\"homeProbability\":" << shortestNumber(c.homeProbability) << ",\"avoidMaxMs\":" << c.avoidMaxMs
      << ",\"rejectionAvoidMs\":" << c.rejectionAvoidMs << ",\"connectBackoffMs\":" << c.connectBackoffMs
      << ",\"initialPlacementHops\":" << unsigned(c.initialPlacementHops) << ",\"peerTimeoutMs\":" << c.peerTimeoutMs
      << ",\"staticPeers\":" << c.peers.size() << ",\"staticPlacements\":" << c.placements.size()
      << "},\"memoryWatermark\":{\"enabled\":" << b(c.memoryWatermarkEnabled) << ",\"budgetBytes\":" << c.memoryBudgetBytes
      << "},\"suspects\":{\"baseMs\":" << c.suspectBaseMs << ",\"maxMs\":" << c.suspectMaxMs
      << "},\"drainTimeoutMs\":" << c.drainTimeoutMs << ",\"loadCheck\":" << b(c.loadCheck)
      << ",\"ingress\":{\"webSocketsPerTenant\":" << c.webSocketsPerTenant << "}"
      << ",\"durableObjects\":{\"processes\":" << c.processPool.durableProcesses << "}"
      << ",\"lease\":{\"mode\":" << q(lease.naive() ? "naive" : "correct") << ",\"ttlMs\":" << lease.ttlMs
      << ",\"rootBudget\":" << lease.rootBudget << ",\"grantCap\":" << lease.grantCap
      << "},\"logs\":{\"tenantBufferBytes\":" << c.logs.tenantBufferBytes << ",\"totalBufferBytes\":" << c.logs.totalBufferBytes
      << ",\"linesPerSecond\":" << c.logs.linesPerSecond << ",\"maxLineBytes\":" << c.logs.maxLineBytes
      << ",\"workerdLogMaxBytes\":" << c.logs.sinkMaxBytes << ",\"workerdLogFiles\":" << c.logs.sinkFiles
      << "},\"usage\":{\"flushIntervalMs\":" << c.usage.flushIntervalMs << ",\"retentionHours\":" << c.usage.retentionHours
      << ",\"sampleIntervalMs\":" << c.usage.sampleIntervalMs
      << "},\"management\":{\"token\":" << q(c.managementToken.empty() ? "unset" : "set")
      << "},\"egress\":{\"mode\":" << q(c.egress.mode) << ",\"defaultPolicy\":" << q(c.egress.defaultPolicy) << ",\"publicPorts\":["
      << [&] { std::string out; for (auto port : c.egress.publicPorts) out += (out.empty() ? "" : ",") + std::to_string(port); return out; }()
      << "],\"timeoutMs\":" << c.egress.timeoutMs << ",\"concurrency\":" << c.egress.concurrency
      << ",\"requestsPerSecond\":" << c.egress.requestsPerSecond << ",\"dnsCacheMs\":" << c.egress.dnsCacheMs
      << ",\"caFile\":" << q(c.egress.caFile) << ",\"systemTrustStore\":" << b(c.egress.systemTrustStore)
      << ",\"clusterAddresses\":" << c.egress.clusterAddresses.size() << ",\"tenants\":" << c.egress.tenants.size()
      << "},\"peerAuth\":{\"keyFile\":" << q(c.peerAuth.keyFile) << ",\"windowMs\":" << c.peerAuth.windowMs
      << "},\"forTest\":{\"clockOffsetMs\":" << c.clockOffsetMsForTest << ",\"disableAlreadyPlaced\":" << b(c.disableAlreadyPlacedForTest)
      << ",\"loadBand\":" << q(c.loadBandForTest) << ",\"equalLimits\":" << b(c.equalLimitsForTest)
      << ",\"versionResolution\":" << q(d.versionResolutionForTest) << ",\"deploymentApplyDelayMs\":" << d.deploymentApplyDelayMsForTest
      << ",\"drainAnnounceDelayMs\":" << c.drainAnnounceDelayMsForTest << "}}";
  return out.str();
}

int run(Launch launch) {
  auto io = kj::setupAsyncIo();
  auto& waitScope = io.waitScope;
  auto& timer = io.provider->getTimer();

  capnp::ByteStreamFactory byteStreamFactory;
  kj::HttpHeaderTable::Builder headerTableBuilder;
  capnp::HttpOverCapnpFactory httpOverCapnpFactory(
      byteStreamFactory,
      headerTableBuilder,
      capnp::HttpOverCapnpFactory::LEVEL_2);
  auto headerTable = headerTableBuilder.build();

  const auto& manifestPath = launch.manifestPath;
  auto serverConfig = std::move(launch.config);
  serverConfig.drainTimeoutMs = static_cast<uint32_t>(
      environmentUnsigned("COWORKERD_DRAIN_TIMEOUT_MS", serverConfig.drainTimeoutMs, 86'400'000));
  serverConfig.drainAnnounceDelayMsForTest = static_cast<uint32_t>(
      environmentUnsigned("COWORKERD_DRAIN_ANNOUNCE_DELAY_MS_FOR_TEST", 0, 600'000));
  serverConfig.loadCheck = environmentUnsigned("COWORKERD_LOAD_CHECK", serverConfig.loadCheck ? 1 : 0, 1) == 1;
  // The management token: the environment's, when set, over the file's.
  std::string tokenSource = serverConfig.managementToken.empty() ? "" : "config";
  if (const char* token = std::getenv("COWORKERD_MANAGEMENT_TOKEN"); token != nullptr && *token) {
    ServerConfig::checkToken(token, "COWORKERD_MANAGEMENT_TOKEN");
    serverConfig.managementToken = token;
    tokenSource = "environment";
  }
  clockOffsetMsForTest = serverConfig.clockOffsetMsForTest;
  if (serverConfig.peerListenAddress.empty()) {
    serverConfig.peerListenAddress = "127.0.0.1:0";
  }
  // Environment overrides of the runtime file, the memory watermark and the process pool,
  // applied before anything starts so that the configuration printed is the one in effect.
  const char* runtimeFile = std::getenv("COWORKERD_RUNTIME_FILE");
  if (runtimeFile) launch.runtime = fs::absolute(runtimeFile).string();
  if (auto* enabled = std::getenv("COWORKERD_MEMORY_WATERMARK_ENABLED")) {
    KJ_REQUIRE(std::string(enabled) == "0" || std::string(enabled) == "1", "invalid memory watermark override");
    serverConfig.memoryWatermarkEnabled = std::string(enabled) == "1";
  }
  applyProcessPoolEnvironment(serverConfig.processPool, serverConfig.memoryBudgetBytes);
  // Outbound requests and the peer key, as the file says or these override; the rules are checked here.
  if (const char* mode = std::getenv("COWORKERD_EGRESS_MODE"); mode != nullptr && *mode) serverConfig.egress.mode = mode;
  if (const char* policy = std::getenv("COWORKERD_EGRESS_DEFAULT_POLICY"); policy != nullptr && *policy) serverConfig.egress.defaultPolicy = policy;
  if (const char* file = std::getenv("COWORKERD_PEER_KEY_FILE"); file != nullptr && *file) serverConfig.peerAuth.keyFile = file;
  KJ_REQUIRE(serverConfig.egress.mode == "on" || serverConfig.egress.mode == "off", "egress.mode must be on or off");
  KJ_REQUIRE(serverConfig.egress.defaultPolicy == "public" || serverConfig.egress.defaultPolicy == "deny",
      "egress.defaultPolicy must be public or deny");
  coworkerd::EgressService::check(serverConfig.egress);
  auto leaseConfig = LeaseConfig::fromEnvironment(launch.experimentalNaiveLease);
  const auto configuration = configurationJson(launch, serverConfig, leaseConfig);
  std::cout << "[server] {\"event\":\"configuration\",\"version\":\"" << jsonEscape(COWORKERD_VERSION) << "\",\"form\":\""
            << launch.form << "\",\"configFile\":\"" << jsonEscape(launch.configFile.string()) << "\",\"tokenSource\":\""
            << tokenSource << "\",\"configuration\":" << configuration << "}" << std::endl;
  if (serverConfig.managementToken.empty()) {
    // The management endpoints (state, drain, rotation, trial loads, every tenant's log lines) take the token; a node
    // without one starts only where its configuration says so, for development.
    KJ_REQUIRE(serverConfig.managementAllowWithoutToken, "no management token: set management.token or "
        "COWORKERD_MANAGEMENT_TOKEN (16 to 4096 printable characters); a development node may start without one with "
        "management.allowWithoutToken: true -- its management endpoints are then open to whoever reaches it");
    std::cout << "[server] {\"event\":\"management\",\"tokenSet\":false,\"allowWithoutToken\":true,\"warning\":\"the "
                 "management endpoints have no token: whoever reaches this Server's ingress or peer address can read its state "
                 "and every tenant's log lines, drain, rotate and trial-load; set management.token or COWORKERD_MANAGEMENT_TOKEN\"}"
              << std::endl;
  }
  coworkerd::PeerAuth peerAuth(serverConfig.peerAuth, serverConfig.serverId, timer);
  if (!peerAuth.enabled()) {
    std::cout << "[server] {\"event\":\"peer-auth\",\"enabled\":false,\"warning\":\"peer connections are not authenticated: "
                 "whoever reaches this Server's peer address can open calls on it; set peerAuth.keyFile (the cluster key)\"}" << std::endl;
  }
  auto membership = coworkerd::newMembership(serverConfig.membership, serverConfig.serverId,
      serverConfig.peerListenAddress, serverConfig.peers,
      io.provider->getNetwork(), timer, *headerTable, serverConfig.desiredState.objectStore);
  auto desiredState = coworkerd::newDesiredState(serverConfig.desiredState, manifestPath,
      io.provider->getNetwork(), timer, *headerTable);
  const auto stackDirectory = launch.stackDirectory;
  // The suspects ride the membership (its objects beside the heartbeat; adopted in its listing cycle).
  coworkerd::Suspects suspects(membership.get(), serverConfig.serverId, serverConfig.suspectBaseMs,
      serverConfig.suspectMaxMs, timer);
  // The tenants' records: deletion, log quotas (handed to the log drainer), default limits -- from the desired state's
  // manifests, at every revision it applies.
  coworkerd::TenantView tenantView;
  desiredState->onTenants([&tenantView](const std::map<std::string, std::optional<coworkerd::TenantRecord>>& records,
      uint64_t revision, bool known) { tenantView.update(records, revision, known); });
  struct QuotaTasks final: kj::TaskSet::ErrorHandler {
    void taskFailed(kj::Exception&& error) override {
      std::cerr << "[tenant] log quota not handed to the drainer: " << error.getDescription().cStr() << std::endl;
    }
  } quotaErrors;
  kj::TaskSet quotaTasks(quotaErrors);
  tenantView.onQuota([&](const std::string& tenant, uint64_t lines, uint64_t bytes) {
    quotaTasks.add(coworkerd::WorkerdOutput::tenantQuota(*io.lowLevelProvider, tenant, lines, bytes).then([tenant](std::string answer) {
      std::cout << "[tenant] {\"event\":\"log-quota\",\"tenant\":\"" << tenant << "\",\"drainer\":" << answer << "}" << std::endl;
    }));
  });
  serverConfig.processPool.heatIdleMs = serverConfig.idleMs;
  coworkerd::MemoryWatermark memory(serverConfig.memoryWatermarkEnabled, serverConfig.memoryBudgetBytes);
  serverConfig.processPool.memoryBandFd = memory.channelDescriptor();
  // workerd calls back (the runtime's SERVER and SOURCE services) over Unix sockets in the stack's
  // run directory, the one directory a sandboxed workerd needs. The loopback TCP addresses
  // of the command line are not listened on.
  const auto runDirectory = stackDirectory / "run";
  fs::create_directories(runDirectory);
  fs::permissions(runDirectory, fs::perms::owner_all, fs::perm_options::replace);
  const auto bindingSocket = (runDirectory / "binding.sock").string(), sourceSocket = (runDirectory / "source.sock").string();
  // The tenant Workers' outbound requests arrive here (egress.h).
  const auto egressSocket = (runDirectory / "egress.sock").string();
  const bool egressOn = serverConfig.egress.on();
  // Left behind by an earlier Server in this stack: removed only when nothing listens on them (main()
  // refused to start while a process did, and this checks again).
  for (const auto& socket : {bindingSocket, sourceSocket, egressSocket}) {
    const int error = probeUnixSocket(socket);
    KJ_REQUIRE(error == ENOENT || error == ECONNREFUSED, "another Server runs in this stack", socket.c_str(),
               error ? strerror(error) : "the socket accepts connections");
    if (error == ECONNREFUSED) fs::remove(socket);
  }
  const auto bindingSocketAddress = "unix:" + bindingSocket, sourceSocketAddress = "unix:" + sourceSocket;
  std::cout << "[server] {\"event\":\"callback-sockets\",\"binding\":\"" << jsonEscape(bindingSocketAddress)
            << "\",\"source\":\"" << jsonEscape(sourceSocketAddress) << "\",\"ignoredBindingAddress\":\""
            << jsonEscape(launch.ignoredBindingAddress) << "\",\"ignoredSourceAddress\":\"" << jsonEscape(launch.ignoredSourceAddress)
            << "\"}" << std::endl;
  coworkerd::ProcessPool runtime(serverConfig.processPool, launch.workerd, launch.runtime,
      stackDirectory.string(), bindingSocketAddress, sourceSocketAddress, *io.provider, io.unixEventPort,
      *headerTable, suspects);
  if (egressOn) runtime.enableEgress("unix:" + egressSocket);
  // What the egress policy denies as the cluster's own, besides the members it learns from the membership (egress.h).
  const auto egressConfig = serverConfig.egress;
  std::vector<std::string> ownEndpoints = {launch.listenAddress, serverConfig.peerListenAddress,
      serverConfig.membership.advertisedAddress, serverConfig.desiredState.objectStore.endpoint};
  for (const auto& [peerId, address] : serverConfig.peers) ownEndpoints.push_back(address);
  // Each tenant's usage here: one object per hour below this node's own prefix, written with the membership's identity
  // (a node without the object store membership only counts).
  coworkerd::TenantUsage tenantUsage(serverConfig.usage, serverConfig.serverId,
      serverConfig.membership.backend == "objectStore" ? std::optional(serverConfig.desiredState.objectStore) : std::nullopt,
      io.provider->getNetwork(),
      timer, {[&runtime]() { return runtime.takeCpuUsage(); }, [&runtime]() { return runtime.residentIsolatesByTenant(); },
      [&io]() { return coworkerd::WorkerdOutput::tenantLines(*io.lowLevelProvider); }});
  bool shuttingDown = false;
  int exitCode = 0;
  auto shutdown = io.unixEventPort.onSignal(SIGTERM)
      .exclusiveJoin(io.unixEventPort.onSignal(SIGINT))
      .then([&shuttingDown](siginfo_t&& info) { shuttingDown = true; return int(info.si_signo); }).fork();
  try {
    desiredState->start().exclusiveJoin(shutdown.addBranch().ignoreResult()).wait(waitScope);
    membership->start().exclusiveJoin(shutdown.addBranch().ignoreResult()).wait(waitScope);
    tenantUsage.start().exclusiveJoin(shutdown.addBranch().ignoreResult()).wait(waitScope);
    if (shuttingDown) {
      membership->stop().wait(waitScope);
      return 0;
    }
  ServerCore server(
      *desiredState, runtime, memory, leaseConfig, std::move(serverConfig), *membership, httpOverCapnpFactory, *headerTable,
      timer);
  server.setTenancy(&tenantView, &tenantUsage);
  // The limits each Worker names: the pool reads the limit rows its processes write against them, and
  // sizes a new generation's WebAssembly memory cap by the largest of them.
  runtime.setWorkerLimits({[&server](const std::string& workerId) { return server.namedLimits(workerId); },
                           [&server]() { return server.maxNamedMemoryMiB(); }});
  server.setConfiguration(configuration, "{\"form\":\"" + launch.form + "\",\"configFile\":\"" + jsonEscape(launch.configFile.string()) +
      "\",\"experimentalNaiveLease\":" + (launch.experimentalNaiveLease ? "true" : "false") + "}");
  PeerForwarder peerForwarder(
      server, io.provider->getNetwork(), timer, *headerTable, httpOverCapnpFactory, peerAuth);
  // The control plane's addresses as it announces them (ControlPlaneAddresses), beside the members' and this node's own.
  ControlPlaneAddresses controlPlane;
  coworkerd::EgressService egress(egressConfig, server.placementConfig().serverId, io.provider->getNetwork(), timer, *headerTable,
      [&membership, &ownEndpoints, &controlPlane]() {
        auto endpoints = ownEndpoints;
        for (const auto& [id, member] : membership->view().members) endpoints.push_back(member.address);
        for (auto& address : controlPlane.endpoints()) endpoints.push_back(std::move(address));
        return endpoints;
      },
      [&runtime](const std::string& token) -> std::optional<coworkerd::EgressSource> {
        auto found = runtime.egressSource(token);
        if (!found) return std::nullopt;
        return coworkerd::EgressSource{found->first, found->second};
      },
      // The tenant's hourly usage rows: egressBytes.
      [&tenantUsage](const std::string& tenant, uint64_t bytes) { tenantUsage.egress(tenant, bytes); });
  server.addStateSection("egress", [&egress]() { return egress.json(); });
  server.addStateSection("controlPlane", [&controlPlane]() { return controlPlane.json(); });
  server.addStateSection("peerAuth", [&peerAuth]() { return peerAuth.json(); });
  // The object store as this node uses it: the membership's measurements, and the tenant index's revision it has applied
  // (what a deployment waits for: cowctl, /v1/nodes).
  if (membership->view().backend == "objectStore" || server.placementConfig().desiredState.backend == "objectStore") {
    server.addStateSection("storage", [&membership, &desiredState, objectState = server.placementConfig().desiredState.backend == "objectStore"]() {
      std::string out = membership->view().backend == "objectStore" ? membership->view().storageJson() : std::string("{}");
      if (!objectState) return out;
      out.pop_back();
      return out + (out.size() > 1 ? "," : "") + "\"appliedTenantsRevision\":\"" + std::to_string(desiredState->state().revision()) + "\"}";
    });
  }
  // Inbound WebSocket connections: a retiring process generation's go away (1001) at the rotation's switch.
  runtime.setRetiring([&server](const std::string& processId, uint64_t generation) {
    return server.webSockets().goAwayGeneration(processId, generation, "rotation");
  });
  server.addStateSection("webSockets", [&server]() { return server.webSockets().json(); });
  server.addStateSection("readiness", [&server, &runtime]() {
    return "{\"ready\":" + std::string(server.processesReady() && !server.draining() ? "true" : "false") + ",\"quarantine\":" +
        runtime.quarantineReadinessJson() + "}";
  });
  HttpForwarder forwarder(server, httpOverCapnpFactory);
  TrialService trials(server, timer, httpOverCapnpFactory, *headerTable);
  WorkerLogsEndpoint workerLogs(server.placementConfig().serverId, *io.lowLevelProvider, timer, *headerTable);
  GatewayService gateway(server, peerForwarder, forwarder, trials, workerLogs, *headerTable);
  BindingRpcService bindingRpc(
      server, peerForwarder, httpOverCapnpFactory, *headerTable);
  PeerRpcService peerRpc(
      server, peerForwarder, httpOverCapnpFactory, *headerTable,
      [&trials](kj::HttpMethod method, kj::AsyncInputStream& body, kj::HttpService::Response& response) {
        return trials.serve(method, body, response);
      },
      workerLogs, [&server, &peerForwarder]() { return server.stateJson(peerForwarder.stateJson(), peerForwarder.avoidanceJson()); },
      peerAuth, controlPlane);
  SourceService source(server, *io.lowLevelProvider, timer, *headerTable);
  server.addStateSection("tenantLogPosts", [&source]() { return source.logJson(); });
  kj::HttpServer httpServer(timer, *headerTable, gateway, {.callbacks = gateway});
  kj::HttpServer bindingServer(timer, *headerTable, bindingRpc);
  kj::HttpServer peerServer(timer, *headerTable, peerRpc);
  kj::HttpServer sourceServer(timer, *headerTable, source, {.callbacks = source});
  kj::HttpServer egressServer(timer, *headerTable, egress);

  auto httpAddress = io.provider->getNetwork().parseAddress(launch.listenAddress.c_str()).wait(waitScope);
  auto bindingAddress =
      io.provider->getNetwork().parseAddress(bindingSocketAddress.c_str()).wait(waitScope);
  auto sourceAddress =
      io.provider->getNetwork().parseAddress(sourceSocketAddress.c_str()).wait(waitScope);
  auto peerAddressText = server.placementConfig().peerListenAddress;
  auto peerAddress = io.provider->getNetwork()
      .parseAddress(peerAddressText.c_str()).wait(waitScope);
  auto httpListener = httpAddress->listen();
  auto bindingListener = bindingAddress->listen();
  auto peerListener = peerAddress->listen();
  auto sourceListener = sourceAddress->listen();
  kj::Own<kj::ConnectionReceiver> egressListener;
  if (egressOn) egressListener = io.provider->getNetwork().parseAddress(("unix:" + egressSocket).c_str()).wait(waitScope)->listen();

  {
    // The bytes this process runs (/proc/self/exe stays the original file even after it is
    // replaced on disk), for the state endpoint.
    const bool proc = fs::exists("/proc/self/exe");
    const auto self = proc ? fs::read_symlink("/proc/self/exe") : fs::absolute(executableArgument);
    server.setServerBinary(self.string(), sha256Hex(readFile(proc ? fs::path("/proc/self/exe") : self)));
  }
  DrainController drain(server, runtime, *membership, timer, server.placementConfig().drainAnnounceDelayMsForTest,
      [&httpServer]() { return httpServer.drain(); });
  // Another process holds this node's id: it writes the node's usage objects from now on, this one none (not even the
  // drain's last flush), then this one drains and exits.
  membership->onIdentityTaken([&drain, &tenantUsage](std::string reason) {
    tenantUsage.stop("another process holds this node's id");
    drain.identityTaken(std::move(reason));
  });

  std::cout << "[server] ready http=" << launch.listenAddress
            << " binding-rpc=" << bindingSocketAddress
            << " peer-rpc=" << peerAddressText
            << " source=" << sourceSocketAddress
            << " egress=" << (egressOn ? "unix:" + egressSocket : std::string("off"))
            << " workerd=" << launch.workerd << std::endl;

  httpServer.listenHttp(*httpListener).exclusiveJoin(drain.ingressClosed())
      .then([&httpListener]() -> kj::Promise<void> { httpListener = nullptr; return kj::NEVER_DONE; })
      .exclusiveJoin(bindingServer.listenHttp(*bindingListener))
      .exclusiveJoin(peerServer.listenHttp(*peerListener))
      .exclusiveJoin(sourceServer.listenHttp(*sourceListener))
      .exclusiveJoin(egressOn ? egressServer.listenHttp(*egressListener).exclusiveJoin(egress.run()) : kj::Promise<void>(kj::NEVER_DONE))
      .exclusiveJoin(peerAuth.run())
      .exclusiveJoin(runtime.run())
      .exclusiveJoin(memory.run(timer))
      .exclusiveJoin(suspects.run())
      .exclusiveJoin(tenantUsage.run())
      .exclusiveJoin(runLeaseReaper(server, timer))
      .exclusiveJoin(runLogClock(timer))
      .exclusiveJoin(membership->run())
      // A draining Server applies no further deployment change: the desired state's poll stops at the announcement.
      .exclusiveJoin(desiredState->run().exclusiveJoin(drain.started())
          .then([]() -> kj::Promise<void> { return kj::NEVER_DONE; }))
      .exclusiveJoin(drain.signals(shutdown.addBranch(), io.unixEventPort))
      .exclusiveJoin(drain.done().then([&exitCode](int code) { exitCode = code; }))
      .wait(waitScope);
  } catch (...) {
    membership->stop().wait(waitScope);
    throw;
  }
  membership->stop().wait(waitScope);
  return exitCode;
}

// `coworkerd check --config FILE`: the node's self-check, against the configuration `coworkerd --config FILE`
// starts with -- the configuration itself, the object store (with this node's identity), the
// desired state there (the cluster's marker, the secrets key) and the sandbox's prerequisites: the process pool's startup check, which runs the
// launcher's --check when the sandbox is on (it prints its own [sandbox] line). One [check] line per check, then the
// result; exits 0 when this node would start, 1 otherwise. It changes nothing: the startup check runs in a
// temporary directory, not in the node's stack. `coworkerd check storage ENDPOINT BUCKET` is the object store's own
// self-check.
int runSelfCheck(const std::vector<std::string>& args) {
  const std::vector<std::string> options(args.begin() + 1, args.end());
  KJ_REQUIRE(!options.empty() && (options[0] == "--config" || options[0].starts_with("--config=")),
      "usage: coworkerd check --config FILE, or coworkerd check storage ENDPOINT BUCKET");
  bool ok = true;
  auto report = [&ok](const std::string& check, bool passed, const std::string& fields) {
    ok = ok && passed;
    std::cout << "[check] {\"check\":\"" << check << "\",\"ok\":" << (passed ? "true" : "false") << fields << "}" << std::endl;
  };
  std::optional<Launch> launch;
  try {
    launch = serveLaunch(options, false);
    const char* runtimeFile = std::getenv("COWORKERD_RUNTIME_FILE");
    if (runtimeFile) launch->runtime = fs::absolute(runtimeFile).string();
    applyProcessPoolEnvironment(launch->config.processPool, launch->config.memoryBudgetBytes);
    report("configuration", true, ",\"configFile\":\"" + jsonEscape(launch->configFile.string()) + "\",\"nodeId\":\"" +
        jsonEscape(launch->config.serverId) + "\"");
  } catch (const kj::Exception& error) {
    report("configuration", false, ",\"reason\":\"" + jsonEscape(cleanReason(error.getDescription().cStr())) + "\"");
    std::cout << "[check] {\"event\":\"result\",\"ok\":false}" << std::endl;
    return 1;
  }
  auto& c = launch->config;
  auto io = kj::setupAsyncIo();
  auto& timer = io.provider->getTimer();
  auto headerTable = kj::HttpHeaderTable::Builder().build();
  // The management token (management.token, else COWORKERD_MANAGEMENT_TOKEN): without one the node does not start,
  // unless management.allowWithoutToken says it may (development).
  {
    std::string token = c.managementToken, source = token.empty() ? "unset" : "config";
    std::string problem;
    if (const char* value = std::getenv("COWORKERD_MANAGEMENT_TOKEN"); value != nullptr && *value) {
      token = value;
      source = "environment";
      try { ServerConfig::checkToken(token, "COWORKERD_MANAGEMENT_TOKEN"); }
      catch (const kj::Exception& error) { problem = cleanReason(error.getDescription().cStr()); }
    }
    if (problem.empty() && token.empty() && !c.managementAllowWithoutToken)
      problem = "no management token (management.token or COWORKERD_MANAGEMENT_TOKEN): the node would not start "
          "(management.allowWithoutToken: true starts a development node without one)";
    report("management", problem.empty(), std::string(",\"tokenSet\":") + (token.empty() ? "false" : "true") + ",\"tokenSource\":\"" +
        source + "\",\"allowWithoutToken\":" + (c.managementAllowWithoutToken ? "true" : "false") +
        (problem.empty() ? std::string() : ",\"reason\":\"" + jsonEscape(problem) + "\""));
  }
  // The outbound rules and the peer key file, with the node's environment overrides.
  if (const char* mode = std::getenv("COWORKERD_EGRESS_MODE"); mode != nullptr && *mode) c.egress.mode = mode;
  if (const char* policy = std::getenv("COWORKERD_EGRESS_DEFAULT_POLICY"); policy != nullptr && *policy) c.egress.defaultPolicy = policy;
  if (const char* file = std::getenv("COWORKERD_PEER_KEY_FILE"); file != nullptr && *file) c.peerAuth.keyFile = file;
  try {
    KJ_REQUIRE(c.egress.mode == "on" || c.egress.mode == "off", "egress.mode must be on or off");
    KJ_REQUIRE(c.egress.defaultPolicy == "public" || c.egress.defaultPolicy == "deny", "egress.defaultPolicy must be public or deny");
    coworkerd::EgressService::check(c.egress);
    report("egress", true, ",\"mode\":\"" + jsonEscape(c.egress.mode) + "\",\"defaultPolicy\":\"" + jsonEscape(c.egress.defaultPolicy) +
        "\",\"tenants\":" + std::to_string(c.egress.tenants.size()));
  } catch (const kj::Exception& error) {
    report("egress", false, ",\"reason\":\"" + jsonEscape(cleanReason(error.getDescription().cStr())) + "\"");
  }
  try {
    coworkerd::PeerAuth keys(c.peerAuth, c.serverId, timer);
    report("peer-auth", true, std::string(",\"enabled\":") + (keys.enabled() ? "true" : "false") + ",\"keyFile\":\"" +
        jsonEscape(c.peerAuth.keyFile) + "\"");
  } catch (const kj::Exception& error) {
    report("peer-auth", false, ",\"reason\":\"" + jsonEscape(cleanReason(error.getDescription().cStr())) + "\"");
  }
  const auto& store = c.desiredState.objectStore;
  try {
    coworkerd::ObjectStore storage(store, io.provider->getNetwork(), timer);
    // An object that does not exist: the store answers, and accepts this node's identity, without a write.
    const bool found = storage.exists("bundles/" + std::string(64, '0')).wait(io.waitScope);
    report("storage", true, ",\"backend\":\"" + jsonEscape(store.backend) + "\",\"endpoint\":\"" + jsonEscape(store.endpoint) +
        "\",\"bucket\":\"" + jsonEscape(store.bucket) + "\",\"probeFound\":" + (found ? "true" : "false"));
  } catch (const kj::Exception& error) {
    report("storage", false, ",\"backend\":\"" + jsonEscape(store.backend) + "\",\"endpoint\":\"" + jsonEscape(store.endpoint) +
        "\",\"bucket\":\"" + jsonEscape(store.bucket) + "\",\"reason\":\"" + jsonEscape(cleanReason(error.getDescription().cStr())) + "\"");
  }
  // The desired state on the object store: the secrets key opens (its file, its format), the bucket holds the cluster's
  // marker (without it the node refuses to start), and the tenant index's hint reads.
  {
    const auto& d = c.desiredState;
    std::string fields = ",\"backend\":\"" + jsonEscape(d.backend) + "\",\"pollIntervalMs\":" + std::to_string(d.pollIntervalMs) +
        ",\"secretsKeyFile\":\"" + jsonEscape(d.secretsKeyFile) + "\"";
    try {
      auto key = coworkerd::SecretsKey::load(d.secretsKeyFile);
      fields += ",\"secretsKey\":\"" + key.fingerprint() + "\"";
      coworkerd::ObjectStore storage(store, io.provider->getNetwork(), timer);
      const auto marker = storage.get(coworkerd::CLUSTER_FORMAT_KEY).wait(io.waitScope);
      KJ_REQUIRE(marker.status == 200 || marker.status == 404, "object store read failed", marker.status);
      auto hint = coworkerd::readHintRevision(storage, coworkerd::indexHintKey(coworkerd::TENANT_INDEX_PREFIX)).wait(io.waitScope);
      fields += std::string(",\"clusterFormat\":") + (marker.status == 200 ? "true" : "false") + ",\"tenantsRevision\":" +
          (hint ? "\"" + std::to_string(*hint) + "\"" : std::string("null"));
      if (marker.status != 200)
        fields += ",\"reason\":\"the bucket holds no cluster/format/1 (the cluster's init writes it): the node would not start\"";
      report("desiredState", marker.status == 200, fields);
    } catch (const kj::Exception& error) {
      report("desiredState", false, fields + ",\"reason\":\"" + jsonEscape(cleanReason(error.getDescription().cStr())) + "\"");
    }
  }
  // The membership's backend; on the object store also what the node's start needs: a listing of members/ (List)
  // whose Date header is near this machine's clock, and a write and removal below the node's own prefix.
  {
    const auto& m = c.membership;
    std::string fields = ",\"backend\":\"" + jsonEscape(m.backend) + "\",\"ttlSeconds\":" + std::to_string(m.ttlSeconds);
    if (m.backend != "objectStore") {
      report("membership", true, fields);
    } else try {
      fields += ",\"heartbeatIntervalMs\":" + std::to_string(m.heartbeatIntervalMs) + ",\"listIntervalMs\":" +
          std::to_string(m.listIntervalMs) + ",\"livenessGraceMs\":" + std::to_string(m.livenessGraceMs) +
          ",\"clockSkewToleranceMs\":" + std::to_string(m.clockSkewToleranceMs) + ",\"createSettleMs\":" + std::to_string(m.createSettleMs);
      coworkerd::ObjectStore storage(store, io.provider->getNetwork(), timer);
      coworkerd::ClockSample sample;
      sample.sentUnixMs = coworkerd::realClockUnixMs();
      auto listing = storage.list(coworkerd::MEMBERS_PREFIX).wait(io.waitScope);
      sample.receivedUnixMs = coworkerd::realClockUnixMs();
      sample.dateUnixMs = listing.dateUnixMs;
      const auto clock = coworkerd::checkClock(sample, m.clockSkewToleranceMs);
      const auto live = coworkerd::liveMembers(coworkerd::readMemberListing(listing), m.ttlSeconds, m.livenessGraceMs);
      const auto key = coworkerd::memberProbePrefix(c.serverId) + "check-" + std::to_string(sample.sentUnixMs) + "-" +
          std::to_string(getpid());
      const auto created = storage.create(key, "coworkerd check --config\n").wait(io.waitScope);
      storage.remove(key).wait(io.waitScope);
      const bool clockOk = clock.readable && !clock.exceeds;
      fields += ",\"liveMembers\":" + std::to_string(live.size()) + ",\"dateHeader\":" + (clock.readable ? "true" : "false") +
          ",\"clockSkewMs\":" + std::to_string(clock.skewMs) + ",\"ownPrefixWritable\":" + (created.created ? "true" : "false");
      if (!clockOk) fields += ",\"reason\":\"" + std::string(clock.readable
          ? "this machine's clock differs from the object store's by more than membership.clockSkewToleranceMs: the node would refuse to start"
          : "the object store's answers carry no Date header: the node would refuse to start") + "\"";
      else if (!created.created) fields += ",\"reason\":\"a new key below members/" + jsonEscape(c.serverId) + "/ was reported as existing\"";
      report("membership", clockOk && created.created, fields);
    } catch (const kj::Exception& error) {
      report("membership", false, fields + ",\"reason\":\"" + jsonEscape(cleanReason(error.getDescription().cStr())) + "\"");
    }
  }
  const char* temporary = std::getenv("TMPDIR");
  std::string pattern = (fs::path(temporary && *temporary ? temporary : "/tmp") / "coworkerd-check-XXXXXX").string();
  KJ_REQUIRE(mkdtemp(pattern.data()) != nullptr, "cannot create the check's temporary directory", strerror(errno));
  const fs::path directory = pattern;
  try {
    coworkerd::Suspects suspects(nullptr, c.serverId, c.suspectBaseMs, c.suspectMaxMs, timer);
    c.processPool.heatIdleMs = c.idleMs;
    const auto run = directory / "run";
    coworkerd::ProcessPool pool(c.processPool, launch->workerd, launch->runtime, directory.string(),
        "unix:" + (run / "binding.sock").string(), "unix:" + (run / "source.sock").string(), *io.provider, io.unixEventPort,
        *headerTable, suspects);
    report("sandbox", true, ",\"mode\":\"" + jsonEscape(c.processPool.sandboxMode) + "\",\"syscalls\":\"" +
        jsonEscape(c.processPool.syscallsMode) + "\",\"workerd\":\"" + jsonEscape(launch->workerd) + "\"");
  } catch (const kj::Exception& error) {
    report("sandbox", false, ",\"mode\":\"" + jsonEscape(c.processPool.sandboxMode) + "\",\"reason\":\"" +
        jsonEscape(cleanReason(error.getDescription().cStr())) + "\"");
  }
  std::error_code ignored;
  fs::remove_all(directory, ignored);
  std::cout << "[check] {\"event\":\"result\",\"ok\":" << (ok ? "true" : "false") << "}" << std::endl;
  return ok ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
  std::unique_ptr<coworkerd::AsyncLog> logging;
  std::unique_ptr<coworkerd::WorkerdOutput> workerdOutput;
  try {
    executableArgument = argc > 0 ? argv[0] : "coworkerd";
    std::vector<std::string> args(argv + 1, argv + argc);
    if (args.size() == 1 && (args[0] == "--version" || args[0] == "version")) {
      std::cout << versionText("coworkerd");
      return 0;
    }
    if (args.empty() || (args.size() == 1 && (args[0] == "--help" || args[0] == "-h" || args[0] == "help"))) {
      (args.empty() ? std::cerr : std::cout) << USAGE;
      return args.empty() ? 2 : 0;
    }
    // The self-checks: the object store's and the node's.
    if (args[0] == "check" && args.size() >= 2 && args[1] == "storage") return coworkerd::runStorageCheck(argc - 2, argv + 2);
    if (args[0] == "check") return runSelfCheck(args);
    std::optional<Launch> launch;
    // The test-only naive lease: this explicit switch, in either form, and nothing else.
    bool naive = false;
    std::erase_if(args, [&naive](const std::string& value) {
      if (value != "--experimental-naive-lease") return false;
      naive = true;
      return true;
    });
    launch = !args.empty() && (args[0] == "--config" || args[0].starts_with("--config=")) ? serveLaunch(args, naive)
        : positionalLaunch(args, naive);
    // A process still listens on a socket of this stack's run directory: another Server runs in this
    // stack, and this one does not start -- before anything changes: no log file opened, no socket
    // removed, no member registered.
    const auto stack = launch->stackDirectory;
    // The processes' sockets live in <stack>/run, and a Unix socket path is short.
    KJ_REQUIRE((stack / "run" / "q99-999999.sock").string().size() < sizeof(sockaddr_un::sun_path),
        "the stack directory's path is too long for the Unix sockets of its run/ directory", stack.string().c_str());
    if (auto live = liveStackSocket(stack)) {
      std::cout << "[server] {\"event\":\"start-refused\",\"stack\":\"" << jsonEscape(stack.string())
                << "\",\"socket\":\"" << jsonEscape(live->first) << "\",\"connect\":\""
                << jsonEscape(live->second ? strerror(live->second) : "accepted")
                << "\",\"reason\":\"another Server runs in this stack: a process listens on "
                << jsonEscape(live->first) << "; nothing was changed\"}" << std::endl;
      return 1;
    }
    fs::create_directories(stack / "logs");
    logging = std::make_unique<coworkerd::AsyncLog>();
    // The Workers' logs: the configuration's, then these environment overrides.
    auto& logs = launch->config.logs;
    logs.tenantBufferBytes = environmentUnsigned("COWORKERD_TENANT_LOG_BUFFER_BYTES", logs.tenantBufferBytes, UINT32_MAX);
    logs.totalBufferBytes = environmentUnsigned("COWORKERD_WORKER_LOG_TOTAL_BYTES", logs.totalBufferBytes, UINT64_MAX);
    logs.linesPerSecond = static_cast<uint32_t>(environmentUnsigned("COWORKERD_WORKER_LOG_LINES_PER_SECOND", logs.linesPerSecond, UINT32_MAX));
    logs.maxLineBytes = static_cast<uint32_t>(environmentUnsigned("COWORKERD_WORKER_LOG_MAX_LINE_BYTES", logs.maxLineBytes, UINT32_MAX));
    logs.sinkMaxBytes = environmentUnsigned("COWORKERD_WORKERD_LOG_MAX_BYTES", logs.sinkMaxBytes, UINT64_MAX);
    logs.sinkFiles = static_cast<uint32_t>(environmentUnsigned("COWORKERD_WORKERD_LOG_FILES", logs.sinkFiles, 64));
    workerdOutput = std::make_unique<coworkerd::WorkerdOutput>((stack / "logs" / "workerd.log").string(), logs);
    kj::UnixEventPort::captureSignal(SIGTERM);
    kj::UnixEventPort::captureSignal(SIGINT);
    AsyncLogCallback logCallback;
    return run(std::move(*launch));
  } catch (const kj::Exception& error) {
    std::cerr << "coworkerd failed: " << error.getDescription().cStr() << std::endl;
    return 1;
  } catch (const std::exception& error) {
    std::cerr << "coworkerd failed: " << error.what() << std::endl;
    return 1;
  }
}
