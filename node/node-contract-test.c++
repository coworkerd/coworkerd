#include "bundle-cache.h"
#include "config.h"
#include "code-package.h"
#include "invocation-contract.h"
#include "peer-liveness.h"
#include "request-lease.h"
#include "ingress-descriptors.h"
#include "event-loop.h"
#include "memory-events.h"
#include "tenant-line.h"
#include "trial-verdict.h"
#include "tenant-execution.h"
#include "process-heat.h"
#include "cpu-capacity.h"
#include "node-cpu.h"
#include "isolate-saturation.h"
#include "placement-contract-test.h"
#include "json.h"
#include "process-culprit.h"
#include "manifests.h"

#include <array>
#include <bit>
#include <capnp/message.h>
#include <cmath>
#include <fstream>
#include <kj/async-io.h>
#include <kj/compat/http.h>
#include <limits>
#include <sstream>
#include <string_view>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/wait.h>
#ifdef __linux__
#include <sched.h>
#endif

namespace coworkerd {
namespace {

struct TempDir {
  std::string path = (std::filesystem::temp_directory_path() / "coworkerd-contract-XXXXXX").string();
  TempDir() { KJ_REQUIRE(::mkdtemp(path.data()) != nullptr); }
  ~TempDir() { std::error_code ignored; std::filesystem::remove_all(path, ignored); }
  std::filesystem::path operator/(const char* file) const { return std::filesystem::path(path) / file; }
};

void peerHalfClose() {
  auto io = kj::setupAsyncIo();
  int fds[2];
  KJ_REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
  auto server = io.lowLevelProvider->wrapSocketFd(fds[0], kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);
  auto client = io.lowLevelProvider->wrapSocketFd(fds[1], kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);

  auto halfClosed = server->whenPeerClosedWrite();
  kj::byte request = 'q';
  client->write(kj::arrayPtr(&request, 1)).wait(io.waitScope);
  KJ_REQUIRE(!halfClosed.poll(io.waitScope), "readable data alone is not a half-close");
  client->shutdownWrite();
  io.provider->getTimer().timeoutAfter(2 * kj::SECONDS, kj::mv(halfClosed)).wait(io.waitScope);

  kj::byte received = 0;
  KJ_REQUIRE(server->tryRead(&received, 1, 1).wait(io.waitScope) == 1 && received == request,
      "half-close notification must preserve unread input");
  KJ_REQUIRE(server->whenPeerClosedWrite().poll(io.waitScope), "an observed half-close must stay visible");

  kj::byte response = 'a';
  server->write(kj::arrayPtr(&response, 1)).wait(io.waitScope);
  KJ_REQUIRE(client->tryRead(&received, 1, 1).wait(io.waitScope) == 1 && received == response,
      "the peer can still read after shutting down its writes");
}

void httpReadBeforeFin() {
  struct SilentService final: kj::HttpService {
    kj::PromiseFulfillerPair<void> started = kj::newPromiseAndFulfiller<void>();
    kj::PromiseFulfillerPair<void> read = kj::newPromiseAndFulfiller<void>();
    std::array<char, 4> received{};
    kj::Promise<void> request(kj::HttpMethod, kj::StringPtr, const kj::HttpHeaders&,
        kj::AsyncInputStream& body, Response&) override {
      started.fulfiller->fulfill();
      co_await read.promise;
      co_await body.tryRead(received.data(), received.size(), received.size());
      co_await kj::Promise<void>(kj::NEVER_DONE);
    }
  } service;
  auto io = kj::setupAsyncIo();
  auto& timer = io.provider->getTimer();
  auto table = kj::HttpHeaderTable::Builder().build();
  kj::HttpServer server(timer, *table, service, {.cancelHttpOnClientDisconnect = true});
  int fds[2];
  KJ_REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
  auto served = server.listenHttp(io.lowLevelProvider->wrapSocketFd(fds[0], kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP));
  auto client = io.lowLevelProvider->wrapSocketFd(fds[1], kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);
  client->write(kj::StringPtr("POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\nbody").asBytes())
      .wait(io.waitScope);
  client->shutdownWrite();
  timer.timeoutAfter(2 * kj::SECONDS, kj::mv(service.started.promise)).wait(io.waitScope);
  KJ_REQUIRE(!served.poll(io.waitScope), "a FIN must leave the buffered request body available");
  service.read.fulfiller->fulfill();
  timer.timeoutAfter(2 * kj::SECONDS, kj::mv(served)).wait(io.waitScope);
  KJ_REQUIRE(std::string_view(service.received.data(), service.received.size()) == "body",
      "the parser delivered all request bytes before cancellation");
}

void peerSessionLiveness() {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  const auto start = kj::origin<kj::TimePoint>();
  for (const bool stall : {false, true}) {
    kj::TimerImpl timer(start);
    PeerLivenessClock clock(timer);
    PeerReceiveLiveness receiver(clock);
    auto expired = receiver.whenExpired();
    const auto advance = [&](kj::Duration elapsed) {
      const auto target = start + elapsed;
      while (timer.now() < target) {
        timer.advanceTo(kj::min(target, timer.now() + 250 * kj::MILLISECONDS));
        waitScope.poll();
      }
    };
    unsigned sent = 0;
    bool timedOut = false;
    auto unanswered = kj::newPromiseAndFulfiller<void>();
    auto inFlight = runPeerKeepalive(clock, [&]() -> kj::Promise<void> {
      ++sent;
      if (sent == 1) {
        receiver.receivedPing();
        return kj::READY_NOW;
      }
      if (sent == 2) return kj::mv(unanswered.promise);
      return kj::NEVER_DONE;
    }).catch_([&](kj::Exception&&) { timedOut = true; });
    advance(1999 * kj::MILLISECONDS);
    KJ_REQUIRE(sent == 0);
    advance(2 * kj::SECONDS);
    KJ_REQUIRE(sent == 1);
    advance(4 * kj::SECONDS);
    KJ_REQUIRE(sent == 2);
    auto lastAcknowledgement = 2 * kj::SECONDS;

    if (stall) {
      // Let the ready timer events run first on resume. The ping and its
      // reply are still buffered in the transport at this point.
      timer.advanceTo(start + 10 * kj::SECONDS);
      waitScope.poll();
      KJ_REQUIRE(!expired.poll(waitScope) && !inFlight.poll(waitScope),
          "a local stall must not expire the session before the loop reads queued traffic");
      auto queued = kj::evalLater([&]() {
        receiver.receivedPing();
        unanswered.fulfiller->fulfill();
      });
      queued.wait(waitScope);
      waitScope.poll();
      KJ_REQUIRE(!expired.poll(waitScope) && !inFlight.poll(waitScope));
      lastAcknowledgement = 10 * kj::SECONDS;
    }

    const auto cutoff = lastAcknowledgement + PEER_KEEPALIVE_TIMEOUT;
    advance(cutoff - kj::MILLISECONDS);
    KJ_REQUIRE(!expired.poll(waitScope) && !inFlight.poll(waitScope));
    advance(cutoff);
    KJ_REQUIRE(expired.poll(waitScope), "the receiving session expires five seconds after the last ping");
    KJ_REQUIRE(inFlight.poll(waitScope), "the outgoing session expires five seconds after the last acknowledgement");
    inFlight.wait(waitScope);
    KJ_REQUIRE(timedOut && !unanswered.fulfiller->isWaiting());
    KJ_REQUIRE(kj::runCatchingExceptions([&]() { receiver.receivedPing(); }) != kj::none,
        "a late ping cannot reopen a session");
  }
}

void liveRequestLease() {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  const auto start = kj::origin<kj::TimePoint>();
  kj::TimerImpl clock(start);
  RequestLease root(clock);
  auto closing = root.whenClosing();
  auto closed = root.whenClosed();
  clock.advanceTo(start + 120 * kj::SECONDS);
  KJ_REQUIRE(!closing.poll(waitScope) && !closed.poll(waitScope) && std::isinf(root.remainingMs()));
  root.beginDrain();
  KJ_REQUIRE(closing.poll(waitScope) && root.remainingMs() == 30'000);
  clock.advanceTo(start + 140 * kj::SECONDS);
  root.beginDrain();
  KJ_REQUIRE(root.remainingMs() == 10'000, "a duplicate drain must keep the original deadline");
  clock.advanceTo(start + 149999 * kj::MILLISECONDS);
  KJ_REQUIRE(!closed.poll(waitScope));
  clock.advanceTo(start + 150 * kj::SECONDS);
  KJ_REQUIRE(closed.poll(waitScope) && root.isClosed() && root.remainingMs() == 0);

  RequestLease alarm(clock);
  alarm.startFixed(15 * kj::MINUTES);
  alarm.beginDrain();
  KJ_REQUIRE(alarm.remainingMs() == 900'000, "an alarm has a fixed wall limit");
  alarm.close();
  KJ_REQUIRE(alarm.whenClosing().poll(waitScope) && alarm.whenClosed().poll(waitScope));
}

void write(const std::filesystem::path& path, const std::string& bytes) {
  std::ofstream file(path, std::ios::binary);
  KJ_REQUIRE(bool(file << bytes), path.c_str());
}

void nodeCpuUse() {
  TempDir directory;
  const auto root = std::filesystem::path(directory.path);
  const auto group = root / "system.slice/worker.service";
  std::filesystem::create_directories(group);
  write(root / "cpu.max", "50000 100000\n");
  std::istringstream membership("9:cpu:/legacy\n0::/system.slice/worker.service\n");
  const auto ownGroup = cpuCgroupFromMembership(membership, root);
  KJ_REQUIRE(ownGroup == group);  // The service has a cpu.stat and inherits its parent quota.
  KJ_REQUIRE(cpusWithinQuota(8, ownGroup, root) == 1);
  std::istringstream legacyOnly("9:cpu:/legacy\n");
  KJ_REQUIRE(cpuCgroupFromMembership(legacyOnly, root).empty());

  const auto stat = ownGroup / "cpu.stat";
  NodeCpuUse cpu(stat, 0.5);
  auto sample = [&](double nowMs, uint64_t usec) {
    write(stat, "user_usec 10\nusage_usec " + std::to_string(usec) + "\nsystem_usec 0\n");
    cpu.sample(nowMs);
  };
  sample(0, 0);
  sample(1000, 500000);
  KJ_REQUIRE(!cpu.full(1000), "two seconds of history are required");
  sample(2000, 949999);
  KJ_REQUIRE(!cpu.full(2000), "one microsecond below the threshold");
  sample(3000, 1425000);
  KJ_REQUIRE(cpu.full(3000), "950 permille of half a CPU");
  KJ_REQUIRE(!cpu.full(5001), "stale samples do not refuse");
  sample(4000, 1425000);
  KJ_REQUIRE(!cpu.full(4000), "idle clears saturation as the window moves");
  sample(5000, 100000);
  KJ_REQUIRE(!cpu.full(5000), "a counter reset discards the old window");
  write(stat, "user_usec 10\n");
  cpu.sample(6000);
  KJ_REQUIRE(!cpu.full(6000), "missing usage clears the reading");
}

void isolateSaturation() {
  const auto readings = parseIsolateSaturation(R"JSON({"residentWorkers":[
    {"planRevision":"agree", "instance":2, "isolate":"worker-a", "isolateBusy":{"throughMonotonicMs":5000,"permille":900}},
    {"planRevision":"agree", "instance":1, "isolate":"worker-a", "isolateBusy":{"throughMonotonicMs":4800,"permille":1000}},
    {"planRevision":"below", "instance":1, "isolate":"worker-b", "isolateBusy":{"throughMonotonicMs":5000,"permille":899}},
    {"planRevision":"loading", "instance":1, "isolate":"worker-c", "isolateBusy":{"throughMonotonicMs":5000,"permille":1000}},
    {"planRevision":"loading", "instance":2, "isolate":"worker-c"},
    {"planRevision":"multiple", "instance":1, "isolate":"worker-d", "isolateBusy":{"throughMonotonicMs":5000,"permille":1000}},
    {"planRevision":"multiple", "instance":2, "isolate":"worker-e", "isolateBusy":{"throughMonotonicMs":5000,"permille":1000}},
    {"planRevision":"missing", "instance":1, "isolate":"worker-f"},
    {"planRevision":"invalid", "instance":1, "isolate":"worker-g", "isolateBusy":{"throughMonotonicMs":5000,"permille":1001}}
  ]})JSON");
  KJ_REQUIRE(readings.size() == 1 && readings.at("agree") == 5000,
      "only the latest valid, unambiguous sample at the inclusive threshold should survive");
  KJ_REQUIRE(hasSaturatedPlan(readings, "agree", 7500));
  KJ_REQUIRE(!hasSaturatedPlan(readings, "agree", 7501), "stale window end");
  KJ_REQUIRE(!hasSaturatedPlan(readings, "agree", 4999), "future window end");
  KJ_REQUIRE(!hasSaturatedPlan(readings, "absent", 5000));
  KJ_REQUIRE(parseIsolateSaturation("{").empty());
  KJ_REQUIRE(parseIsolateSaturation(R"({"residentWorkers":[]})").empty());
}

void nativeConfig() {
  TempDir directory;
  write(directory / "default.json", "{}");
  const auto direct = ServerConfig::load({});
  const auto decoded = ServerConfig::load(directory / "default.json");
  for (const auto* config : {&direct, &decoded}) {
    KJ_REQUIRE(config->processPool.ordinaryServer == "threaded");
    KJ_REQUIRE(config->processPool.tenantInFlightLimit == 0);
    KJ_REQUIRE(config->ingressKeepalive.idleSeconds == 30 && config->ingressKeepalive.intervalSeconds == 10 &&
        config->ingressKeepalive.probes == 3);
    KJ_REQUIRE(config->processPool.tenantAdmissionWaitMs == 1000);
    KJ_REQUIRE(config->processPool.cpuLimitMs == 30000 && config->processPool.durableCpuLimitMs == 200);
  }
  write(directory / "limits.json", "{\"cpuLimitMs\":200,\"memoryLimitMiB\":64}");
  const auto overridden = ServerConfig::load(directory / "limits.json");
  KJ_REQUIRE(overridden.processPool.cpuLimitMs == 200 && overridden.processPool.durableCpuLimitMs == 200 &&
      overridden.processPool.memoryLimitMiB == 64);
  write(directory / "keepalive.json", R"({"ingress":{"tcpKeepalive":{"idleSeconds":2,"intervalSeconds":1,"probes":2}}})");
  const auto keepalive = ServerConfig::load(directory / "keepalive.json").ingressKeepalive;
  KJ_REQUIRE(keepalive.idleSeconds == 2 && keepalive.intervalSeconds == 1 && keepalive.probes == 2);
  write(directory / "keepalive-invalid.json", R"({"ingress":{"tcpKeepalive":{"probes":0}}})");
  KJ_REQUIRE(kj::runCatchingExceptions([&] { ServerConfig::load(directory / "keepalive-invalid.json"); }) != kj::none);
  write(directory / "disabled.json", "{\"processPool\":{\"tenantInFlightLimit\":0,\"tenantAdmissionWaitMs\":25}}");
  const auto disabled = ServerConfig::load(directory / "disabled.json");
  KJ_REQUIRE(disabled.processPool.tenantInFlightLimit == 0 && disabled.processPool.tenantAdmissionWaitMs == 25);
  disabled.processPool.validate();
}

void ingressDescriptorPlan() {
  // Default pool budget for one ordinary, two quarantine and one durable process plus a replacement.
  constexpr uint64_t pool = 11136;
  struct Case {
    const char* name;
    uint64_t soft, hard;
    uint32_t configured, requested, sockets;
    uint64_t effectiveSoft;
  };
  for (const auto& row : {
      Case{"low inherited soft", 1024, 524288, 0, 2048, 2048, 15488},
      Case{"service soft limit", 65536, 65536, 0, 2048, 2048, 65536},
      Case{"unlimited", UINT64_MAX, UINT64_MAX, 0, 2048, 2048, UINT64_MAX},
      Case{"one slot", 1024, 11395, 0, 2048, 1, 11394},
      Case{"zero slots", 1024, 11393, 0, 2048, 0, 1024},
      Case{"above hard limit", 1024, 65536, 30000, 30000, 27072, 65536},
      Case{"above default", 1024, 65536, 20000, 20000, 20000, 51392},
      Case{"small override", 1024, 524288, 2, 2, 2, 11396},
  }) {
    const auto plan = planIngressDescriptors(row.soft, row.hard, pool, row.configured);
    KJ_REQUIRE(plan.requested == row.requested && plan.webSockets == row.sockets &&
        plan.softLimit == row.effectiveSoft, row.name, plan.requested, plan.webSockets, plan.softLimit);
  }
}

void tenantExecution() {
  KJ_REQUIRE(makeTenantExecutionState(0) == nullptr);
  KJ_REQUIRE(makeTenantExecutionState(2) != nullptr);
  for (const auto [cpus, expected] : {std::pair{0u, 1u}, {1u, 1u}, {2u, 1u}, {3u, 2u}, {4u, 2u},
      {5u, 3u}, {16u, 13u}, {64u, 61u}}) KJ_REQUIRE(tenantExecutionShare(cpus) == expected);
  KJ_REQUIRE(quotaCpuCount(150000, 100000) == 2 && quotaCpuCount(200000, 100000) == 2);
  KJ_REQUIRE(quotaCpuCount(UINT64_MAX, 1) == UINT32_MAX && quotaCpuCount(1, 0) == 0);
  TempDir directory;
  const auto leaf = std::filesystem::path(directory.path) / "parent/leaf";
  std::filesystem::create_directories(leaf);
  write(leaf / "cpu.max", "max 100000\n");
  write(leaf.parent_path() / "cpu.max", "150000 100000\n");
  write(std::filesystem::path(directory.path) / "cpu.max", "400000 100000\n");
  KJ_REQUIRE(cpusWithinQuota(16, leaf, directory.path) == 2);
  KJ_REQUIRE(cpusWithinQuota(1, leaf, directory.path) == 1);
  write(leaf / "cpu.max", "100000 100000\n");
  KJ_REQUIRE(cpusWithinQuota(16, leaf, directory.path) == 1);
  KJ_REQUIRE(!tenantExecutionLimited(100, 1, 13));
  KJ_REQUIRE(tenantExecutionLimited(100, 2, 13));
  KJ_REQUIRE(!tenantExecutionLimited(12, 2, 13));
  KJ_REQUIRE(!tenantExecutionLimited(100, 2, 0));
#ifdef __linux__
  const auto child = fork(); KJ_REQUIRE(child >= 0);
  if (!child) {
    cpu_set_t available, two;
    if (sched_getaffinity(0, sizeof(available), &available) != 0) _exit(1);
    CPU_ZERO(&two); unsigned count = 0;
    for (unsigned cpu = 0; cpu < CPU_SETSIZE && count < 2; ++cpu) if (CPU_ISSET(cpu, &available)) {
      CPU_SET(cpu, &two); ++count;
    }
    if (sched_setaffinity(0, sizeof(two), &two) != 0) _exit(1);
    const auto actual = availableProcessCpus();
    _exit(actual >= 1 && actual <= count ? 0 : 1);
  }
  int status = 0; KJ_REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0);
#endif
  kj::EventLoop loop; kj::WaitScope scope(loop);
  kj::TimerImpl timer(kj::origin<kj::TimePoint>());
  TenantExecutionQueue queue;
  auto state = std::make_shared<TenantExecutionState>(2);
  state->enter("x"); state->enter("x"); state->enter("y");
  KJ_REQUIRE(state->active() == 2 && state->count("x") == 2 && state->limited("x"));
  auto first = queue.wait(timer, 1000 * kj::MILLISECONDS, 2, "x", {state});
  auto second = queue.wait(timer, 1000 * kj::MILLISECONDS, 2, "x", {state});
  KJ_REQUIRE(!first.poll(scope) && !second.poll(scope) && queue.active() == 2 && state->queued() == 2);
  auto full = queue.wait(timer, 1000 * kj::MILLISECONDS, 2, "x", {state});
  KJ_REQUIRE(full.wait(scope) == TenantExecutionQueue::FULL && queue.active() == 2);
  timer.advanceTo(timer.now() + 5 * kj::MILLISECONDS);
  KJ_REQUIRE(!first.poll(scope) && !second.poll(scope));
  state->leave("x");
  KJ_REQUIRE(first.poll(scope) && !second.poll(scope));
  KJ_REQUIRE(first.wait(scope) == TenantExecutionQueue::READY && queue.active() == 1);
  state->enter("x");
  second = nullptr;
  KJ_REQUIRE(queue.active() == 0 && state->queued() == 0 && state->count("x") == 2);

  auto borrowed = std::make_shared<TenantExecutionState>(2);
  for (unsigned i = 0; i < 4; ++i) borrowed->enter("x");
  KJ_REQUIRE(borrowed->active() == 1 && !borrowed->limited("x"));
  borrowed->enter("y");
  auto allFirst = queue.wait(timer, 1000 * kj::MILLISECONDS, 2, "x", {borrowed});
  auto allSecond = queue.wait(timer, 1000 * kj::MILLISECONDS, 2, "x", {borrowed});
  KJ_REQUIRE(!allFirst.poll(scope) && !allSecond.poll(scope));
  borrowed->leave("y");
  KJ_REQUIRE(borrowed->active() == 1 && borrowed->count("x") == 4);
  KJ_REQUIRE(allFirst.poll(scope) && allSecond.poll(scope));
  KJ_REQUIRE(allFirst.wait(scope) == TenantExecutionQueue::READY && allSecond.wait(scope) == TenantExecutionQueue::READY);
  KJ_REQUIRE(queue.active() == 0 && borrowed->queued() == 0);

  auto other = std::make_shared<TenantExecutionState>(2);
  other->enter("x"); other->enter("x"); other->enter("y");
  auto either = queue.wait(timer, 1000 * kj::MILLISECONDS, 2, "x", {state, other});
  KJ_REQUIRE(!either.poll(scope) && state->queued() == 1 && other->queued() == 1);
  other->leave("x");
  KJ_REQUIRE(either.poll(scope) && either.wait(scope) == TenantExecutionQueue::READY);
  KJ_REQUIRE(queue.active() == 0 && state->queued() == 0 && other->queued() == 0);

  auto expired = queue.wait(timer, 10 * kj::MILLISECONDS, 2, "x", {state});
  KJ_REQUIRE(!expired.poll(scope) && state->count("x") == 2);
  timer.advanceTo(timer.now() + 11 * kj::MILLISECONDS);
  KJ_REQUIRE(expired.poll(scope) && expired.wait(scope) == TenantExecutionQueue::EXPIRED);
  KJ_REQUIRE(queue.active() == 0 && state->queued() == 0 && state->count("x") == 2);
  auto late = queue.wait(timer, 10 * kj::MILLISECONDS, 2, "x", {state});
  KJ_REQUIRE(!late.poll(scope));
  timer.advanceTo(timer.now() + 11 * kj::MILLISECONDS); state->leave("x");
  KJ_REQUIRE(late.poll(scope) && late.wait(scope) == TenantExecutionQueue::EXPIRED);
  KJ_REQUIRE(queue.active() == 0 && state->queued() == 0);

  auto disabled = std::make_shared<TenantExecutionState>(0);
  for (unsigned i = 0; i < 100; ++i) disabled->enter("x");
  disabled->enter("y");
  KJ_REQUIRE(disabled->active() == 2 && !disabled->limited("x"));
  KJ_REQUIRE(queue.wait(timer, 1000 * kj::MILLISECONDS, 2, "x", {disabled}).wait(scope) == TenantExecutionQueue::READY);
  KJ_REQUIRE(queue.active() == 0 && disabled->queued() == 0);

  ProcessHeat heat;
  heat.touch("a", 0); heat.touch("b", 1); heat.touch("c", 2); heat.touch("a", 3);
  heat.expire(4, 2);
  KJ_REQUIRE(heat.size() == 2 && heat.find("b") == heat.end() && heat.find("a")->second == 3);
  heat.touch("c", 4); heat.expire(6, 2);
  KJ_REQUIRE(heat.size() == 1 && heat.find("c") != heat.end());
  heat.expire(6.001, 2); KJ_REQUIRE(heat.size() == 0);
  for (unsigned i = 0; i < 1000; ++i) heat.touch("same", i);
  KJ_REQUIRE(heat.size() == 1); heat.expire(1000, 0); KJ_REQUIRE(heat.size() == 0);
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
  // Ready I/O is checked during the burst, every NODE_BUSY_POLL_INTERVAL turns: not every other turn (each poll
  // is system calls on the node's main thread), and not never.
  KJ_REQUIRE(port.callbacks == 1000 && port.polls > 0 && port.largestBatch <= NODE_BUSY_POLL_INTERVAL + 1 &&
      port.largestBatch >= NODE_BUSY_POLL_INTERVAL / 4,
      "ready I/O must be checked during a continuation burst, every so many turns", port.polls, port.largestBatch);
  // The interval itself: 32 to 128 turns, not the old 1 (a poll every other turn).
  KJ_REQUIRE(NODE_BUSY_POLL_INTERVAL >= 32 && NODE_BUSY_POLL_INTERVAL <= 128, NODE_BUSY_POLL_INTERVAL);
  KJ_REQUIRE(port.largestBatch >= 8, "ready I/O polls must leave room for a batch of continuations", port.largestBatch);
}

// A line its server already cut (the threaded server forwards at most 4,096 bytes of a message, and its size) is
// marked truncated with its full size, as a line past the drainer's own limit is; a short line is not
// (log-quota-isolation: the threaded path lost the mark and the count).
void tenantLineTruncation() {
  auto parse = [](const std::string& text) { return parseTenantLine(std::string_view(text).substr(0, text.size() - 1)); };
  const auto earlier = tenantLineText("acme/noisy", "v1", "log", std::string(4096, 'x'), 5000);
  auto line = parse(earlier);
  KJ_REQUIRE(line.has_value() && line->messageBytes == 5000 && line->message.size() == 4096);
  auto keep = tenantLineKeep(*line, 4096);
  KJ_REQUIRE(keep.truncated && keep.bytes == 4096 && keep.messageBytes == 5000);
  const auto own = tenantLineText("acme/noisy", "v1", "log", std::string(5000, 'y'));
  line = parse(own);
  KJ_REQUIRE(line.has_value() && line->messageBytes == 0);
  keep = tenantLineKeep(*line, 4096);
  KJ_REQUIRE(keep.truncated && keep.bytes == 4096 && keep.messageBytes == 5000);
  // A message of 4,096 quotes its server cut from 5,000 raw bytes: escaped, the text kept is 8,192 bytes, more than the
  // raw size; under a drainer limit above it the line is still marked, with the raw size its server sent.
  std::string quotes;
  for (int at = 0; at < 4096; ++at) quotes += "\\\"";
  line = parse(tenantLineText("acme/noisy", "v1", "log", quotes, 5000));
  KJ_REQUIRE(line.has_value() && line->message.size() == 8192 && line->messageBytes == 5000);
  keep = tenantLineKeep(*line, 1 << 20);
  KJ_REQUIRE(keep.truncated && keep.bytes == 8192 && keep.messageBytes == 5000, keep.messageBytes);
  line = parse(tenantLineText("acme/quiet", "v1", "info", "hello"));
  KJ_REQUIRE(line.has_value() && !tenantLineKeep(*line, 4096).truncated && tenantLineKeep(*line, 4096).bytes == 5);
  KJ_REQUIRE(!parseTenantLine("{\"workerId\":\"acme/quiet\",\"versionId\":\"v1\",\"level\":\"info\",\"message\":\"x\",\"messageBytes\":}").has_value());
}

// A Durable Object call its node's secret dictionary could not take is SECRET_UNAVAILABLE, as at its admission,
// whether admission or the descriptor finds it; a durable process not ready stays DO_UNAVAILABLE.
void durableNotSent() {
  KJ_REQUIRE(std::string_view(durableNotSentCode(true)) == "SECRET_UNAVAILABLE");
  KJ_REQUIRE(std::string_view(durableNotSentCode(false)) == "DO_UNAVAILABLE");
  KJ_REQUIRE(acceptsForwardedAlarm(true, 0));
  KJ_REQUIRE(acceptsForwardedAlarm(false, 1));
  KJ_REQUIRE(!acceptsForwardedAlarm(true, 1));
}

// The process pool refreshes once per turn: the first call of a turn does the work, the later ones of that turn
// do not, and the next turn does it again.
void oncePerTurn() {
  kj::EventLoop loop;
  kj::WaitScope scope(loop);
  struct Handler final: kj::TaskSet::ErrorHandler { void taskFailed(kj::Exception&& e) override { kj::throwFatalException(kj::mv(e)); } } handler;
  kj::TaskSet tasks(handler);
  OncePerTurn turn(tasks);
  unsigned done = 0;
  auto ask = [&]() { if (turn.first()) ++done; };
  ask(); ask(); ask();
  KJ_REQUIRE(done == 1, done);
  kj::evalLater([&]() { ask(); ask(); }).wait(scope);
  KJ_REQUIRE(done == 2, done);
  kj::evalLater([&]() {}).wait(scope);
  ask();
  KJ_REQUIRE(done == 3, done);
}

// memory.events stays open and is read from its start: a counter rewritten in place after the file's name is gone
// is still read.
void memoryEvents() {
  KJ_REQUIRE(oomKillsIn("low 0\nhigh 0\nmax 4\noom 2\noom_kill 3\noom_group_kill 0\n") == 3);
  KJ_REQUIRE(oomKillsIn("low 0\n") == 0);
  char path[] = "/tmp/coworkerd-memory-events-XXXXXX";
  const int fd = mkstemp(path);
  KJ_REQUIRE(fd >= 0);
  const std::string first = "low 0\noom_kill 3\n";
  KJ_REQUIRE(::write(fd, first.data(), first.size()) == ssize_t(first.size()));
  MemoryEventsReader reader(path);
  KJ_REQUIRE(reader.oomKills() == 3);
  KJ_REQUIRE(unlink(path) == 0);
  const std::string second = "low 0\noom_kill 12\n";
  KJ_REQUIRE(pwrite(fd, second.data(), second.size(), 0) == ssize_t(second.size()));
  KJ_REQUIRE(reader.oomKills() == 12, reader.oomKills());
  close(fd);
}

// SIGTERM and SIGINT reach the node through its signalfd: one raised before anyone waits is not lost, a wait
// with nothing pending waits, and they arrive in order.
void shutdownSignals() {
  ShutdownSignals signals;
  auto io = kj::setupAsyncIo();
  KJ_REQUIRE(raise(SIGTERM) == 0);
  KJ_REQUIRE(signals.next(io.unixEventPort).wait(io.waitScope) == SIGTERM);
  auto later = signals.next(io.unixEventPort);
  KJ_REQUIRE(!later.poll(io.waitScope), "no signal is pending yet");
  KJ_REQUIRE(kill(getpid(), SIGINT) == 0);
  KJ_REQUIRE(later.wait(io.waitScope) == SIGINT);
  KJ_REQUIRE(kill(getpid(), SIGTERM) == 0);
  KJ_REQUIRE(signals.next(io.unixEventPort).wait(io.waitScope) == SIGTERM);
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
  auto native = classifyTrialFailure("evaluation", "Error", "Uncaught Error: top-level failure\nat index.js:1:7", false, false, 30000, 128);
  KJ_REQUIRE(native.message == "Error: top-level failure (at index.js:1:7)");
  KJ_REQUIRE(classifyTrialFailure("evaluation", "Error", "Error: failure\nat index.js:1:" + std::string(80, '9'), false, false, 30000, 128).category == "TOP_LEVEL_THROW");
  // The call path classifies other failures at the stage the native server
  // reported; a report without a stage keeps the general load classification.
  KJ_REQUIRE(classifyLoadRejection("", "Error", "Missing trusted ordinary worker plan", false, false, 30000, 128).category == "TOP_LEVEL_THROW");
  KJ_REQUIRE(classifyLoadRejection("evaluation", "Error", "Error: tenant top-level throw", false, false, 30000, 128).category == "TOP_LEVEL_THROW");
  KJ_REQUIRE(classifyLoadRejection("code", "SyntaxError", "invalid JSON", false, false, 30000, 128).category == "INVALID_MODULE");
  // Both call-path sites answer a bridge failure as the platform's: PROCESS_UNAVAILABLE, which placement
  // retries on the next node, never a LOAD_FAILED of the version, and no word about a trial.
  const auto platform = callPathPlatformFailure("bridge", "Missing trusted ordinary worker plan");
  const bool mapped = platform.has_value() && platform->code == "PROCESS_UNAVAILABLE" && platform->category == "bridge" &&
      platform->reason == "the native process could not get this call's plan from the node: Missing trusted ordinary worker plan" &&
      platform->reason.find("trial") == std::string::npos;
  KJ_REQUIRE(mapped);
  KJ_REQUIRE(placementRetryable(platform->code, platform->category) && !placementRetryable("LOAD_FAILED", "evaluation"));
  const bool tenantStages = !callPathPlatformFailure("evaluation", "Error: tenant top-level throw").has_value() &&
      !callPathPlatformFailure("code", "x").has_value() && !callPathPlatformFailure("", "Missing trusted ordinary worker plan").has_value();
  KJ_REQUIRE(tenantStages);
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
  // A fractional monotonic timestamp from a captured server log must survive JSON serialization.
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
  InvocationBudget root(DEFAULT_SUBREQUEST_LIMIT, DEFAULT_D1_QUERY_LIMIT);
  for (unsigned call = 0; call < 100; ++call) {
    KJ_REQUIRE(root.debit(false) == BudgetDebit::charged);
  }
  InvocationBudget child(2, DEFAULT_D1_QUERY_LIMIT);
  KJ_REQUIRE(child.debit(false) == BudgetDebit::charged);
  KJ_REQUIRE(child.debit(false) == BudgetDebit::charged);
  KJ_REQUIRE(child.debit(false) == BudgetDebit::subrequestsExhausted);
  KJ_REQUIRE(child.remaining == 0 && child.d1Remaining == 1'000);
  KJ_REQUIRE(root.remaining == 9'900 && root.d1Remaining == 1'000);

  InvocationBudget wide(MAX_SUBREQUEST_LIMIT, DEFAULT_D1_QUERY_LIMIT);
  KJ_REQUIRE(wide.subrequestLimit == 10'000'000);
  KJ_REQUIRE(wide.debit(false) == BudgetDebit::charged && wide.remaining == 9'999'999);

  InvocationBudget database(3, 1);
  KJ_REQUIRE(database.debit(true) == BudgetDebit::charged);
  KJ_REQUIRE(database.remaining == 2 && database.d1Remaining == 0);
  KJ_REQUIRE(database.debit(true) == BudgetDebit::d1Exhausted && database.remaining == 2);
  KJ_REQUIRE(child.debit(true) == BudgetDebit::subrequestsExhausted && child.d1Remaining == 1'000);

  InvocationBudget socket(2, 50);
  KJ_REQUIRE(socket.debit(true) == BudgetDebit::charged);
  KJ_REQUIRE(socket.debit(false) == BudgetDebit::charged);
  socket.refill();
  KJ_REQUIRE(socket.remaining == 2 && socket.d1Remaining == 50);
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
  using Decision = TenantAdmission;
  KJ_REQUIRE(tenantAdmission(tenant.active(true), 2, false, 2, 1, 128, 0) == Decision::Refused);
  KJ_REQUIRE(tenantAdmission(tenant.active(true), 2, true, 2, 1, 128, 0) == Decision::Allowed);
  --tenant.active(true);
  KJ_REQUIRE(tenantAdmission(tenant.active(true), 2, false, 2, 1, 128, 0) == Decision::Allowed);
  KJ_REQUIRE(tenantAdmission(16, 0, false, 128, 0, 128, 0) == Decision::Allowed);
  KJ_REQUIRE(tenantAdmission(16, 16, false, 85, 10, 128, 75) == Decision::Borrowed);
  KJ_REQUIRE(tenantAdmission(16, 16, false, 86, 10, 128, 75) == Decision::Refused);
  KJ_REQUIRE(tenantAdmission(19, 16, true, 100, 10, 128, 75) == Decision::Allowed);
  KJ_REQUIRE(tenantAdmission(16, 16, false, 76, 211, 384, 75) == Decision::Borrowed);
  KJ_REQUIRE(tenantAdmission(16, 16, false, 76, 212, 384, 75) == Decision::Refused);
  KJ_REQUIRE(tenantAdmission(16, 16, false, UINT32_MAX - 1, 0, UINT32_MAX, 100) == Decision::Borrowed);
  KJ_REQUIRE(tenantAdmission(16, 16, false, UINT32_MAX - 1, 1, UINT32_MAX, 100) == Decision::Refused);
  TenantAdmissionCounts borrowed{.roots = 20, .nested = 12};
  KJ_REQUIRE(borrowed.borrowed(true, 16) == 4 && borrowed.borrowed(false, 16) == 0);
  KJ_REQUIRE(borrowed.borrowed(true, 0) == 0);
  --borrowed.active(true);
  KJ_REQUIRE(borrowed.borrowed(true, 16) == 3);
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
  KJ_REQUIRE(crashCulpritDispatch(live, recent, "in-flight", "tenant/worker", "v1"));
  KJ_REQUIRE(crashCulpritDispatch(live, recent, "completed", "tenant/worker", "v1"));
  KJ_REQUIRE(!crashCulpritDispatch(live, recent, "completed", "tenant/worker", "v2"));
  KJ_REQUIRE(!crashCulpritDispatch(live, recent, "completed", "tenant/other", "v1"));
  KJ_REQUIRE(!crashCulpritDispatch(live, recent, "not-dispatched", "tenant/worker", "v1"));
  KJ_REQUIRE(!crashCulpritDispatch(live, recent, "queued", "tenant/worker", "v1"));
  // The production decoder reads the field the native server writes.
  capnp::MallocMessageBuilder message;
  auto report = message.initRoot<v1::rpc::CpuReport>();
  capnp::JsonCodec().decode(kj::StringPtr("{\"event\":\"memory-culprit\",\"pid\":7,\"workerId\":\"tenant/worker\","
      "\"versionId\":\"v1\",\"invocationId\":\"in-flight\",\"isolate\":\"i\",\"memoryLimitMiB\":128,"
      "\"oomIsolateHeapReported\":true,\"oomIsolateHeapBytes\":110592}"), report);
  KJ_REQUIRE(report.getOomIsolateHeapReported() && report.getOomIsolateHeapBytes() == 110592 &&
      !memoryCulpritOwnsOom(report.asReader(), false));
  const auto limitBytes = uint64_t(128) << 20;
  report.setOomIsolateHeapBytes(limitBytes);
  KJ_REQUIRE(!memoryCulpritOwnsOom(report.asReader(), false), "last allocator in a shared cage, even at its limit");
  report.setOomIsolateLocal(true);
  KJ_REQUIRE(memoryCulpritOwnsOom(report.asReader(), false));
  report.setOomIsolateHeapBytes(limitBytes - 1);
  KJ_REQUIRE(!memoryCulpritOwnsOom(report.asReader(), false));
  report.setOomIsolateHeapBytes(limitBytes);
  report.setOomIsolateHeapReported(false);
  KJ_REQUIRE(!memoryCulpritOwnsOom(report.asReader(), false));
  capnp::MallocMessageBuilder stock;
  auto old = stock.initRoot<v1::rpc::CpuReport>();
  capnp::JsonCodec().decode(kj::StringPtr("{\"event\":\"memory-culprit\",\"pid\":7,\"memoryLimitMiB\":128}"), old);
  KJ_REQUIRE(!old.getOomIsolateHeapReported() && !memoryCulpritOwnsOom(old.asReader(), false));
  KJ_REQUIRE(memoryCulpritOwnsOom(old.asReader(), true), "legacy durable OOM evidence");
  report.setOomIsolateLocal(false);
  report.setOomIsolateHeapReported(true);
  KJ_REQUIRE(memoryCulpritOwnsOom(report.asReader(), true), "threaded durable heap evidence");
  report.setOomIsolateHeapBytes(limitBytes - 1);
  KJ_REQUIRE(!memoryCulpritOwnsOom(report.asReader(), true));
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
  if (name == "native-config") nativeConfig();
  else if (name == "ingress-descriptor-plan") ingressDescriptorPlan();
  else if (name == "peer-half-close") { peerHalfClose(); httpReadBeforeFin(); }
  else if (name == "peer-session-liveness") peerSessionLiveness();
  else if (name == "live-request-lease") liveRequestLease();
  else if (name == "tenant-execution") tenantExecution();
  else if (name == "deadlines") deadlines();
  else if (name == "event-loop") eventLoopProgress();
  else if (name == "shutdown-signals") shutdownSignals();
  else if (name == "once-per-turn") oncePerTurn();
  else if (name == "durable-not-sent") durableNotSent();
  else if (name == "tenant-line-truncation") tenantLineTruncation();
  else if (name == "memory-events") memoryEvents();
  else if (name == "trial-verdicts") trialVerdicts();
  else if (name == "leases") leases();
  else if (name == "tree-lease") treeLease();
  else if (name == "budget") budget();
  else if (name == "identity") identity();
  else if (name == "admission") admission();
  else if (name == "placement") placementContracts();
  else if (name == "cpu-saturation") nodeCpuUse();
  else if (name == "isolate-saturation") isolateSaturation();
  else if (name == "cpu-culprit-current-behavior") cpuCulpritCurrentBehavior();
  else if (name == "memory-culprit") memoryCulprit();
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
