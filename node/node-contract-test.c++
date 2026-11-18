#include "bundle-cache.h"
#include "code-package.h"
#include "invocation-contract.h"
#include "json.h"
#include "process-culprit.h"
#include "manifests.h"

#include <bit>
#include <capnp/message.h>
#include <cmath>
#include <fstream>
#include <kj/async-io.h>
#include <limits>
#include <sstream>
#include <string_view>
#include <unistd.h>

namespace coworkerd {
namespace {

struct TempDir {
  std::string path = (std::filesystem::temp_directory_path() / "coworkerd-contract-XXXXXX").string();
  TempDir() { KJ_REQUIRE(::mkdtemp(path.data()) != nullptr); }
  ~TempDir() { std::error_code ignored; std::filesystem::remove_all(path, ignored); }
  std::filesystem::path operator/(const char* file) const { return std::filesystem::path(path) / file; }
};
void write(const std::filesystem::path& path, const std::string& bytes) {
  std::ofstream file(path, std::ios::binary);
  KJ_REQUIRE(bool(file << bytes), path.c_str());
}

auto number(const std::string& bytes, const char* field) {
  capnp::MallocMessageBuilder message;
  auto root = message.initRoot<json::Json>();
  capnp::JsonCodec().decodeRaw(kj::StringPtr(bytes.c_str()), root);
  auto value = json::field(root, field);
  KJ_REQUIRE(value.isNumber(), field);
  return value.getNumber();
}
std::string deadlineJson(double value) {
  std::ostringstream body;
  exactDeadlinePrecision(body) << "{\"expiresAtMonotonicMs\":" << value << '}';
  return body.str();
}
void deadlines() {
  // v133's archived 4.11 server log, parent s2.1-invocation-7, tree s1.1-tree-3.
  constexpr double historical = 116947232.28310399;
  KJ_REQUIRE(bool(deadlineJson(historical) == "{\"expiresAtMonotonicMs\":116947232.28310399}"));
  const double fractional = 123456789.12345678;
  for (const auto deadline : {0.0, 0.125, historical, fractional, std::nextafter(fractional, 0.0),
      std::nextafter(fractional, std::numeric_limits<double>::infinity()), 9007199254740991.0}) {
    const auto wire = deadlineJson(deadline);
    const auto parsed = number(wire, "expiresAtMonotonicMs");
    KJ_REQUIRE(std::bit_cast<uint64_t>(parsed) == std::bit_cast<uint64_t>(deadline), wire.c_str());
    KJ_REQUIRE(wire == deadlineJson(parsed), "forwarded deadline changed decimal text", wire.c_str());
  }
}
void leases() {
  const auto deadline = 1000.125;
  KJ_REQUIRE(presentsLease(deadline, deadline, 0));
  KJ_REQUIRE(!presentsLease(std::nextafter(deadline, 0.0), deadline, 0));
  KJ_REQUIRE(remainingLeaseMs(deadline, 999.875) == 0.25);
  KJ_REQUIRE(remainingLeaseMs(deadline, deadline) == 0);
  KJ_REQUIRE(remainingLeaseMs(deadline, 1000.375) == 0);
  // A held WebSocket lease retains the presented value when its real deadline changes.
  KJ_REQUIRE(presentsLease(deadline, 2000.125, deadline));
  KJ_REQUIRE(!presentsLease(2000.125, 2000.125, deadline));
}
void treeLease() {
  struct Parent { double expiresAtMonotonicMs; std::string ingressServerId; };
  const Parent root{116947232.28310399, "first-ingress"};
  const auto child = inheritLease(root);
  const Parent forwarded{child.deadline, child.ingressServerId};
  const auto grandchild = inheritLease(forwarded);
  KJ_REQUIRE(std::bit_cast<uint64_t>(root.expiresAtMonotonicMs) == std::bit_cast<uint64_t>(grandchild.deadline));
  KJ_REQUIRE(grandchild.ingressServerId == "first-ingress");
}

void budget() {
  auto child = childBudget(8, 3, false);
  KJ_REQUIRE(child.grant == 3 && child.charged == 4 && child.remaining == 4);
  KJ_REQUIRE(childBudget(1, 3, false).remaining == 0);
  auto naive = childBudget(8, 3, true);
  KJ_REQUIRE(naive.grant == 8 && naive.charged == 0 && naive.remaining == 8);
}
void identity() {
  KJ_REQUIRE(invocationName("n1", "epoch-9", "-tree-", 12) == "n1.epoch-9-tree-12");
  KJ_REQUIRE(invocationName("n1", "epoch-9", "-invocation-", 12) == "n1.epoch-9-invocation-12");
  KJ_REQUIRE(invocationName("n1", "epoch-9", "-tree-", 13) != invocationName("n1", "epoch-9", "-tree-", 12));
  KJ_REQUIRE(invocationName("n2", "epoch-9", "-tree-", 12) != invocationName("n1", "epoch-9", "-tree-", 12));
}
void admission() {
  TenantAdmissionCounts tenant, other;
  KJ_REQUIRE(tenant.empty());
  ++tenant.active(true);
  ++tenant.active(true);
  ++tenant.active(false);
  KJ_REQUIRE(tenant.roots == 2 && tenant.nested == 1 && other.empty());
  KJ_REQUIRE(tenantAtLimit(tenant.active(true), 2, false));
  KJ_REQUIRE(!tenantAtLimit(tenant.active(true), 2, true));
  --tenant.active(true);
  KJ_REQUIRE(!tenantAtLimit(tenant.active(true), 2, false));
  --tenant.active(false);
  --tenant.active(true);
  KJ_REQUIRE(tenant.empty());
  ++tenant.rootRejections;
  KJ_REQUIRE(!tenant.empty(), "idle tenants keep their refusal counters");
}
void cpuCulpritCurrentBehavior() {
  const auto matches = [](bool live, uint32_t pid, uint32_t limit, uint64_t used,
      const char* worker, const char* version, const char* event = "culprit") {
    return cpuCulpritMatches(event, pid, 11, limit, 100, used, live, worker, "tenant/worker", version, "v1");
  };
  KJ_REQUIRE(matches(true, 11, 100, 100000000, "tenant/worker", "v1"));
  KJ_REQUIRE(!matches(false, 11, 100, 100000000, "tenant/worker", "v1"), "completed call is not attributed by current CPU behavior");
  KJ_REQUIRE(!matches(true, 11, 100, 100000000, "tenant/other", "v1"));
  KJ_REQUIRE(!matches(true, 11, 100, 100000000, "tenant/worker", "v2"));
  KJ_REQUIRE(!matches(true, 12, 100, 100000000, "tenant/worker", "v1"));
  KJ_REQUIRE(!matches(true, 11, 101, 101000000, "tenant/worker", "v1"));
  KJ_REQUIRE(!matches(true, 11, 100, 99999999, "tenant/worker", "v1"));
  KJ_REQUIRE(!matches(true, 11, 100, 100000000, "tenant/worker", "v1", "memory-culprit"));
}
void memoryCulprit() {
  struct Call { std::string invocationId, workerId, versionId; bool dispatched; };
  Call active{"in-flight", "tenant/worker", "v1", true};
  Call queued{"queued", "tenant/worker", "v1", false};
  std::map<std::string, const Call*> live{{active.invocationId, &active}, {queued.invocationId, &queued}};
  std::vector<Call> recent{{"completed", "tenant/worker", "v1", true}};
  KJ_REQUIRE(memoryCulpritDispatch(live, recent, "in-flight", "tenant/worker", "v1"));
  KJ_REQUIRE(memoryCulpritDispatch(live, recent, "completed", "tenant/worker", "v1"));
  KJ_REQUIRE(!memoryCulpritDispatch(live, recent, "completed", "tenant/worker", "v2"));
  KJ_REQUIRE(!memoryCulpritDispatch(live, recent, "completed", "tenant/other", "v1"));
  KJ_REQUIRE(!memoryCulpritDispatch(live, recent, "not-dispatched", "tenant/worker", "v1"));
  KJ_REQUIRE(!memoryCulpritDispatch(live, recent, "queued", "tenant/worker", "v1"));
}
void package() {
  TempDir path;
  const std::string bytes = "export default {}";
  const auto hash = objectSha256(bytes);
  const auto manifest = packageJson(CodePackage{{"main.js", "esm", hash, bytes.size()},
      {"second.js", "esm", hash, bytes.size()}});
  const auto manifestHash = objectSha256(manifest);
  write(path / "manifest.json", manifest);
  write(path / "main.js", bytes);
  write(path / "second.js", bytes);
  const auto objects = packageObjects(path / "manifest.json", manifestHash, "main.js");
  KJ_REQUIRE(objects.size() == 2 && objects.at("modules/" + hash) == bytes &&
      objects.at("packages/" + manifestHash) == manifest);
  // Same byte count, different digest, while the other filename still has the right bytes.
  write(path / "second.js", std::string(bytes.size(), '!'));
  bool rejected = false;
  try { packageObjects(path / "manifest.json", manifestHash, "main.js"); }
  catch (const kj::Exception& error) {
    KJ_REQUIRE(std::string_view(error.getDescription().cStr()).find("hash mismatch") != std::string_view::npos,
        error.getDescription());
    rejected = true;
  }
  KJ_REQUIRE(rejected, "bad bytes entered the package object map");
}
void moduleTypes() {
  CodePackage modules;
  for (auto type : {"esm", "cjs", "text", "data", "json", "wasm"})
    modules.push_back({type, type, objectSha256(""), 0});
  auto decoded = readCodePackage(packageJson(modules));
  KJ_REQUIRE(decoded.size() == modules.size());
  for (const auto& module : decoded) KJ_REQUIRE(module.name == module.type);
  modules[0].type = "unsupported";
  bool rejected = false;
  try { readCodePackage(packageJson(modules)); }
  catch (const kj::Exception&) { rejected = true; }
  KJ_REQUIRE(rejected);
}
void cache() {
  TempDir path;
  auto io = kj::setupAsyncIo();
  auto& timer = io.provider->getTimer();
  ObjectStoreConfig settings;
  settings.backend = "directory";
  settings.directory = (path / "store").string();
  ObjectStore storage(settings, io.provider->getNetwork(), timer);
  BundleCacheConfig config;
  config.directory = path / "cache";
  BundleCache cache(storage, config, timer, "modules/");
  const auto first = objectSha256("first"), second = objectSha256("second");
  storage.put("modules/" + first, "first").wait(io.waitScope);
  storage.put("modules/" + second, "second").wait(io.waitScope);
  auto a = cache.read(first, 1000);
  auto again = cache.read(first, 1000);
  auto b = cache.read(second, 1000);
  KJ_REQUIRE(a.wait(io.waitScope).bytes == "first");
  KJ_REQUIRE(again.wait(io.waitScope).bytes == "first");
  KJ_REQUIRE(b.wait(io.waitScope).bytes == "second");
  KJ_REQUIRE(number(cache.json(), "fetchStarted") == 2 && number(cache.json(), "singleFlightMerged") == 1);
  write(config.directory / first, "wrong");
  auto bad = cache.read(first, 1000).wait(io.waitScope);
  KJ_REQUIRE(bad.bytes.empty() && bad.reason == "code unavailable: cached content hash mismatch");
  KJ_REQUIRE(!std::filesystem::exists(config.directory / first));
  KJ_REQUIRE(cache.read(first, 1000).wait(io.waitScope).bytes == "first");
}
void rejectedManifest() {
  class EmptyStore final: public kj::HttpService {
  public:
    explicit EmptyStore(const kj::HttpHeaderTable& table): table(table) {}
    kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders&,
        kj::AsyncInputStream&, Response& response) override {
      kj::HttpHeaders headers(table);
      if (method != kj::HttpMethod::GET) ++writes;
      if (method == kj::HttpMethod::GET && std::string_view(url.cStr()).find("list-type=2") != std::string_view::npos) {
        static constexpr char bytes[] = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
        auto body = response.send(200, "OK", headers, uint64_t(sizeof bytes - 1));
        co_await body->write(kj::arrayPtr(bytes, sizeof bytes - 1).asBytes());
      } else {
        response.send(404, "Not Found", headers, uint64_t(0));
      }
    }
    uint32_t writes = 0;
  private:
    const kj::HttpHeaderTable& table;
  };
  auto io = kj::setupAsyncIo();
  auto& timer = io.provider->getTimer();
  auto& network = io.provider->getNetwork();
  auto address = network.parseAddress("127.0.0.1", 0).wait(io.waitScope);
  auto listener = address->listen();
  kj::HttpHeaderTable table;
  EmptyStore service(table);
  kj::HttpServer server(timer, table, service);
  auto http = server.listenHttp(*listener).eagerlyEvaluate(nullptr);
  ObjectStoreConfig config;
  config.endpoint = "http://127.0.0.1:" + std::to_string(listener->getPort());
  config.accessKey = "test-only";
  config.secretKey = "test-only";
  ObjectStore storage(config, network, timer);
  std::ostringstream messages;
  ManifestStore manifests(storage, timer, std::nullopt, messages);
  const auto prefix = tenantManifestPrefix("test-tenant");
  bool rejected = false;
  try {
    manifests.writeTenant("test-tenant", [](TenantManifest& draft) -> kj::Promise<void> {
      draft.workers.try_emplace("invalid-worker");
      KJ_FAIL_REQUIRE("reject candidate manifest");
    }, true).wait(io.waitScope);
  } catch (const kj::Exception& error) {
    KJ_REQUIRE(std::string_view(error.getDescription().cStr()).find("reject candidate manifest") != std::string_view::npos,
        error.getDescription());
    rejected = true;
  }
  KJ_REQUIRE(rejected && manifests.tenant("test-tenant") == nullptr);
  KJ_REQUIRE(service.writes == 0 && storage.get(revisionKey(prefix, 1)).wait(io.waitScope).status == 404,
      "a validation failure must not leave a revision object behind");
}
}  // namespace
}  // namespace coworkerd

int main(int argc, char* argv[]) {
  using namespace coworkerd;
  KJ_REQUIRE(argc == 2);
  const std::string_view name(argv[1]);
  if (name == "deadlines") deadlines();
  else if (name == "leases") leases();
  else if (name == "tree-lease") treeLease();
  else if (name == "budget") budget();
  else if (name == "identity") identity();
  else if (name == "admission") admission();
  else if (name == "cpu-culprit-current-behavior") cpuCulpritCurrentBehavior();
  else if (name == "memory-culprit") memoryCulprit();
  else if (name == "package") package();
  else if (name == "module-types") moduleTypes();
  else if (name == "cache") cache();
  else if (name == "rejected-manifest") rejectedManifest();
  else KJ_FAIL_REQUIRE("unknown contract test", argv[1]);
}
