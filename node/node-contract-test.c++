#include "bundle-cache.h"
#include "code-package.h"
#include "invocation-contract.h"
#include "event-loop.h"
#include "trial-verdict.h"
#include "json.h"
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

void eventLoopProgress() {
  class Port final: public kj::EventPort {
  public:
    bool wait() override { KJ_FAIL_REQUIRE("runnable continuation chain must not sleep"); }
    bool poll() override {
      ++polls;
      largestBatch = kj::max(largestBatch, callbacks - atLastPoll);
      atLastPoll = callbacks;
      return false;
    }
    kj::Promise<void> burst(unsigned remaining) {
      return kj::evalLater([this, remaining]() -> kj::Promise<void> {
        ++callbacks;
        if (remaining == 1) return kj::READY_NOW;
        return burst(remaining - 1);
      });
    }
    unsigned callbacks = 0, polls = 0, atLastPoll = 0, largestBatch = 0;
  } port;
  kj::EventLoop loop(port);
  kj::WaitScope scope(loop);
  configureNodeEventLoop(scope);
  port.burst(1000).wait(scope);
  KJ_REQUIRE(port.callbacks == 1000 && port.polls > 0 && port.largestBatch <= 4,
      "ready I/O must be checked during a continuation burst", port.polls, port.largestBatch);
}

void trialVerdicts() {
  auto bridge = classifyTrialFailure("bridge", "Error", "Missing trusted ordinary worker plan", false, false, 30000, 128);
  KJ_REQUIRE(bridge.category == "LOAD_FAILED" && bridge.limit.empty() &&
      bridge.message == "the trial could not reach the runtime: remote exception: remote.jsg.Error: Missing trusted ordinary worker plan");
  auto decode = classifyTrialFailure("code", "SyntaxError", "invalid JSON", false, false, 30000, 128);
  KJ_REQUIRE(decode.category == "INVALID_MODULE");
  KJ_REQUIRE(classifyTrialFailure("code", "SyntaxError", "Uncaught SyntaxError: invalid JSON", false, false, 30000, 128).message == "a module cannot be decoded: SyntaxError: invalid JSON");
  auto unavailable = classifyTrialFailure("code", "Error", "Trusted source fetch failed: 503", false, false, 30000, 128);
  KJ_REQUIRE(unavailable.category == "UNAVAILABLE");
  auto cpu = classifyTrialFailure("evaluation", "Error", "CPU_LIMIT_EXCEEDED", true, false, 30000, 128);
  auto memory = classifyTrialFailure("evaluation", "Error", "MEMORY_LIMIT_EXCEEDED", false, true, 30000, 128);
  KJ_REQUIRE(cpu.category == "LIMIT_EXCEEDED" && cpu.limit == "cpu" && memory.limit == "memory");
  KJ_REQUIRE(classifyTrialFailure("evaluation", "Error", "Error: tenant top-level throw", false, false, 30000, 128).category == "TOP_LEVEL_THROW");
  auto native = classifyTrialFailure("evaluation", "Error", "Uncaught Error: top-level failure\nat index.js:1:6", false, false, 30000, 128, true);
  KJ_REQUIRE(native.message == "Error: top-level failure (at index.js:1:7)");
  KJ_REQUIRE(classifyTrialFailure("evaluation", "Error", "Error: failure\nat index.js:1:" + std::string(80, '9'), false, false, 30000, 128, true).category == "TOP_LEVEL_THROW");
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
class BundleStoreFault final: public kj::HttpService {
public:
  enum class Mode { MISSING, CORRUPT, CUT, SLOW, SLOW_FAILURE, GOOD };
  explicit BundleStoreFault(const kj::HttpHeaderTable& table): table(table) {}
  kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr, const kj::HttpHeaders&,
      kj::AsyncInputStream&, Response& response) override {
    KJ_REQUIRE(method == kj::HttpMethod::GET);
    ++requests;
    kj::HttpHeaders headers(table);
    if (mode == Mode::MISSING) {
      response.send(404, "Not Found", headers, uint64_t(0));
      co_return;
    }
    if (mode == Mode::SLOW_FAILURE) {
      waiting.fulfiller->fulfill();
      co_await kj::mv(release.promise);
      response.send(503, "Service Unavailable", headers, uint64_t(0));
      co_return;
    }
    auto body = response.send(200, "OK", headers, uint64_t(4));
    if (mode == Mode::CUT) {
      co_await body->write(kj::arrayPtr("g", 1).asBytes());
      ++cuts;
      KJ_FAIL_REQUIRE("test store intentionally terminated a partial response");
    }
    if (mode == Mode::SLOW) {
      waiting.fulfiller->fulfill();
      co_await kj::mv(release.promise);
    }
    co_await body->write(kj::arrayPtr(mode == Mode::CORRUPT ? "evil" : "good", 4).asBytes());
  }
  kj::PromiseFulfillerPair<void> waiting = kj::newPromiseAndFulfiller<void>();
  kj::PromiseFulfillerPair<void> release = kj::newPromiseAndFulfiller<void>();
  Mode mode = Mode::MISSING;
  unsigned requests = 0;
  unsigned cuts = 0;
private:
  const kj::HttpHeaderTable& table;
};

void cacheFaults(bool slow) {
  TempDir path;
  auto io = kj::setupAsyncIo();
  auto& timer = io.provider->getTimer();
  auto& network = io.provider->getNetwork();
  auto listener = network.parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
  kj::HttpHeaderTable table;
  BundleStoreFault service(table);
  kj::HttpServer server(timer, table, service);
  auto http = server.listenHttp(*listener).eagerlyEvaluate(nullptr);
  ObjectStoreConfig config;
  config.endpoint = "http://127.0.0.1:" + std::to_string(listener->getPort());
  config.accessKey = "test-only";
  config.secretKey = "test-only";
  ObjectStore storage(config, network, timer);
  BundleCacheConfig settings;
  settings.directory = path / "cache";
  kj::TimerImpl clock(timer.now());
  BundleCache cache(storage, settings, slow ? static_cast<kj::Timer&>(clock) : timer, "bundles/");
  const auto key = objectSha256("good");
  const auto expectEmpty = [&] {
    KJ_REQUIRE(std::filesystem::is_empty(settings.directory), "unverified bytes reached disk cache");
    KJ_REQUIRE(number(cache.json(), "entries") == 0);
  };
  using Mode = BundleStoreFault::Mode;
  if (slow) {
    service.mode = Mode::SLOW;
    auto request = cache.read(key, 20).eagerlyEvaluate(nullptr);
    timer.timeoutAfter(2 * kj::SECONDS, kj::mv(service.waiting.promise)).wait(io.waitScope);
    clock.advanceTo(clock.now() + 19 * kj::MILLISECONDS);
    KJ_REQUIRE(!request.poll(io.waitScope), "the caller is still waiting before its deadline");
    clock.advanceTo(clock.now() + 1 * kj::MILLISECONDS);
    KJ_REQUIRE(request.poll(io.waitScope), "the caller must stop waiting at its deadline");
    auto timedOut = request.wait(io.waitScope);
    KJ_REQUIRE(timedOut.bytes.empty() && timedOut.reason == "code unavailable: fetch timed out (call deadline)");
    expectEmpty();
    auto joined = cache.read(key, 100).eagerlyEvaluate(nullptr);
    service.release.fulfiller->fulfill();
    KJ_REQUIRE(timer.timeoutAfter(2 * kj::SECONDS, kj::mv(joined)).wait(io.waitScope).bytes == "good");
    KJ_REQUIRE(service.requests == 1 && number(cache.json(), "singleFlightMerged") == 1);
  } else {
    auto unavailable = [&](Mode mode, const char* reason) {
      service.mode = mode;
      auto result = cache.read(key, 2000).wait(io.waitScope);
      KJ_REQUIRE(result.bytes.empty() && result.reason == reason, result.reason.c_str());
      expectEmpty();
    };
    unavailable(Mode::MISSING, "code unavailable: object not found");
    unavailable(Mode::CORRUPT, "code unavailable: content hash mismatch");
    unavailable(Mode::CUT, "code unavailable: fetch failed");
    KJ_REQUIRE(service.cuts > 0, "the fake store must cut a response after writing part of its body");
    service.mode = Mode::GOOD;
    KJ_REQUIRE(cache.read(key, 2000).wait(io.waitScope).bytes == "good");
  }
  KJ_REQUIRE(number(cache.json(), "entries") == 1 && number(cache.json(), "fetchSucceeded") == 1);
}

void cacheSingleFlight() {
  using Mode = BundleStoreFault::Mode;
  for (const auto failure : {false, true}) {
    TempDir path;
    auto io = kj::setupAsyncIo();
    auto& timer = io.provider->getTimer();
    auto& network = io.provider->getNetwork();
    auto listener = network.parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
    kj::HttpHeaderTable table;
    BundleStoreFault service(table);
    service.mode = failure ? Mode::SLOW_FAILURE : Mode::SLOW;
    kj::HttpServer server(timer, table, service);
    auto http = server.listenHttp(*listener).eagerlyEvaluate(nullptr);
    ObjectStoreConfig config;
    config.endpoint = "http://127.0.0.1:" + std::to_string(listener->getPort());
    config.accessKey = "test-only";
    config.secretKey = "test-only";
    ObjectStore storage(config, network, timer);
    BundleCacheConfig settings;
    settings.directory = path / "cache";
    BundleCache cache(storage, settings, timer);
    const auto key = objectSha256("good");
    std::vector<kj::Promise<BundleCache::Read>> reads;
    for (unsigned i = 0; i < 32; ++i) reads.push_back(cache.read(key, 2000).eagerlyEvaluate(nullptr));
    timer.timeoutAfter(2 * kj::SECONDS, kj::mv(service.waiting.promise)).wait(io.waitScope);
    for (auto& read : reads) KJ_REQUIRE(!read.poll(io.waitScope), "all 32 callers must wait for the same GET");
    KJ_REQUIRE(service.requests == 1 && number(cache.json(), "singleFlightMerged") == 31);
    KJ_REQUIRE(std::filesystem::is_empty(settings.directory));
    service.release.fulfiller->fulfill();
    for (auto& read : reads) {
      auto result = timer.timeoutAfter(2 * kj::SECONDS, kj::mv(read)).wait(io.waitScope);
      if (failure) KJ_REQUIRE(result.bytes.empty() && result.reason == "code unavailable: fetch failed HTTP 503");
      else KJ_REQUIRE(result.bytes == "good" && result.reason.empty());
    }
    KJ_REQUIRE(service.requests == 1);
    if (failure) KJ_REQUIRE(std::filesystem::is_empty(settings.directory));
    service.mode = Mode::GOOD;
    KJ_REQUIRE(cache.read(key, 2000).wait(io.waitScope).bytes == "good");
    KJ_REQUIRE(cache.read(key, 2000).wait(io.waitScope).bytes == "good");
    KJ_REQUIRE(service.requests == (failure ? 2 : 1));
    KJ_REQUIRE(number(cache.json(), "entries") == 1 && number(cache.json(), "fetchSucceeded") == 1);
  }
}

void cacheCapacity() {
  TempDir path;
  auto io = kj::setupAsyncIo();
  auto& timer = io.provider->getTimer();
  ObjectStoreConfig config;
  config.backend = "directory";
  config.directory = (path / "store").string();
  ObjectStore storage(config, io.provider->getNetwork(), timer);
  BundleCacheConfig settings;
  settings.directory = path / "cache";
  settings.maxBytes = 5;
  BundleCache cache(storage, settings, timer, "bundles/");
  for (auto data : {"aaa", "bbb", "too big"}) {
    storage.put("bundles/" + objectSha256(data), data).wait(io.waitScope);
  }
  const auto first = objectSha256("aaa"), second = objectSha256("bbb"), large = objectSha256("too big");
  KJ_REQUIRE(cache.read(first, 1000).wait(io.waitScope).bytes == "aaa");
  KJ_REQUIRE(cache.read(second, 1000).wait(io.waitScope).bytes == "bbb");
  KJ_REQUIRE(!std::filesystem::exists(settings.directory / first));
  KJ_REQUIRE(number(cache.json(), "bytes") == 3 && number(cache.json(), "evictions") == 1);
  auto overLimit = cache.read(large, 1000).wait(io.waitScope);
  KJ_REQUIRE(overLimit.bytes.empty() && overLimit.reason == "code unavailable: cache size limit exceeded");
  KJ_REQUIRE(!std::filesystem::exists(settings.directory / large));
  KJ_REQUIRE(cache.read(first, 1000).wait(io.waitScope).bytes == "aaa");
  KJ_REQUIRE(!std::filesystem::exists(settings.directory / second));
  KJ_REQUIRE(number(cache.json(), "bytes") == 3 && number(cache.json(), "evictions") == 2);
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
  else if (name == "event-loop") eventLoopProgress();
  else if (name == "trial-verdicts") trialVerdicts();
  else if (name == "leases") leases();
  else if (name == "tree-lease") treeLease();
  else if (name == "budget") budget();
  else if (name == "identity") identity();
  else if (name == "package") package();
  else if (name == "module-types") moduleTypes();
  else if (name == "cache") cache();
  else if (name == "cache-unavailable") cacheFaults(false);
  else if (name == "cache-deadline") cacheFaults(true);
  else if (name == "cache-capacity") cacheCapacity();
  else if (name == "cache-single-flight") cacheSingleFlight();
  else if (name == "rejected-manifest") rejectedManifest();
  else KJ_FAIL_REQUIRE("unknown contract test", argv[1]);
}
