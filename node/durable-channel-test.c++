#include "durable-log.h"
#include "durable-shards.h"
#include "parked-websocket.h"

#include "support.h"
#include "json.h"
#include "object-state.h"

#include <capnp/compat/http-over-capnp.h>
#include <kj/async-io.h>
#include <kj/timer.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cmath>
#include <cstring>
#include <fstream>
#include <optional>
#include <random>

namespace coworkerd {

struct DurableLogChannelTest {
  static void recoveryCompletion() {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-completed-rebuild-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    kj::TimerImpl clock(kj::origin<kj::TimePoint>());
    ObjectStoreConfig store;
    store.backend = "directory"; store.directory = temp; store.endpoint = "unused";
    MembershipView membership;
    DurableLog log({"node", "instance", temp, {}}, store, io.provider->getNetwork(), clock,
        *io.lowLevelProvider, membership, {});
    const DurableLog::ObjectIdentity identity{std::string(64, '1'), std::string(64, '2'), "tenant", 1, 1, 0};
    const auto name = durableHostName(identity.namespaceId, identity.objectId, identity.dataEpoch);
    auto& object = log.objects[name];
    object.objectName = name;
    object.shardEpoch = identity.shardEpoch;
    object.state = DurableLog::ObjectState::REBUILDING;

    auto completed = kj::newPromiseAndFulfiller<void>();
    auto canceler = std::make_unique<kj::Canceler>();
    auto pending = canceler->wrap(kj::mv(completed.promise)).fork();
    log.rebuilding.emplace(name, kj::mv(pending));
    log.rebuildCancelers.emplace(name, std::move(canceler));
    auto firstOpen = log.openObject(identity, false).eagerlyEvaluate(nullptr);
    KJ_REQUIRE(!firstOpen.poll(io.waitScope), "the first open waits for the rebuild");

    // The rebuilt image is local, and the adapter still has to deliver the result to the waiter.
    object.state = DurableLog::ObjectState::LOCAL;
    log.settledRebuilds.insert(name);

    auto cleanup = log.healLoop().eagerlyEvaluate(nullptr);
    io.waitScope.poll();
    clock.advanceTo(clock.now() + 100 * kj::MILLISECONDS);
    io.waitScope.poll();
    KJ_REQUIRE(!log.rebuilding.contains(name) && !log.rebuildCancelers.contains(name),
        "the cleanup removes the settled rebuild");
    completed.fulfiller->fulfill();
    const auto result = firstOpen.wait(io.waitScope);
    KJ_REQUIRE(result.ready, result.code.c_str(), result.reason.c_str());
  }

  static void recoveryAdmission() {
    auto io = kj::setupAsyncIo();
    kj::TimerImpl clock(kj::origin<kj::TimePoint>());
    RecoveryGate metadata(clock, 2, 16, 4);
    auto advance = [&](kj::Duration by) { clock.advanceTo(clock.now() + by); io.waitScope.poll(); };
    auto one = metadata.acquire().wait(io.waitScope);
    auto two = metadata.acquire().wait(io.waitScope);
    auto three = metadata.acquire();
    auto four = metadata.acquire();
    auto five = metadata.acquire();
    KJ_REQUIRE(metadata.active() == 2 && !three.poll(io.waitScope));
    one = nullptr;
    two = nullptr;
    KJ_REQUIRE(three.poll(io.waitScope) && four.poll(io.waitScope));
    three = nullptr;
    four = nullptr;
    KJ_REQUIRE(metadata.started() == 4 && metadata.active() == 0 && !five.poll(io.waitScope));
    advance(62499 * kj::MICROSECONDS);
    KJ_REQUIRE(!five.poll(io.waitScope), "the fifth request must wait for the first refill");
    advance(kj::MICROSECONDS);
    KJ_REQUIRE(five.poll(io.waitScope) && metadata.started() == 5 && metadata.peakActive() == 2);
    five = nullptr;
    advance(kj::SECONDS);
    for (int i = 0; i != 4; ++i) metadata.acquire().wait(io.waitScope) = nullptr;
    KJ_REQUIRE(!metadata.acquire().poll(io.waitScope), "an idle bucket refills to four tokens");

    RecoveryGate sharedMetadata(clock, 1, 16, 1);
    auto held = sharedMetadata.acquire().wait(io.waitScope);
    auto canceled = sharedMetadata.acquireCoordination();
    auto bulkFirst = sharedMetadata.acquire();
    auto bulkSecond = sharedMetadata.acquire();
    auto discovery = sharedMetadata.acquireCoordination();
    auto heartbeat = sharedMetadata.acquireCoordination();
    canceled = nullptr;
    held = nullptr;
    KJ_REQUIRE(!discovery.poll(io.waitScope), "coordination waits for the same token bucket");
    advance(62500 * kj::MICROSECONDS);
    KJ_REQUIRE(discovery.poll(io.waitScope) && !bulkFirst.poll(io.waitScope));
    discovery = nullptr;
    advance(62500 * kj::MICROSECONDS);
    KJ_REQUIRE(bulkFirst.poll(io.waitScope) && !bulkSecond.poll(io.waitScope) && !heartbeat.poll(io.waitScope));
    bulkFirst = nullptr;
    advance(62500 * kj::MICROSECONDS);
    KJ_REQUIRE(heartbeat.poll(io.waitScope) && !bulkSecond.poll(io.waitScope));
    heartbeat = nullptr;
    advance(62500 * kj::MICROSECONDS);
    KJ_REQUIRE(bulkSecond.poll(io.waitScope) && sharedMetadata.started() == 5 && sharedMetadata.peakActive() == 1);

    RecoveryGate connections(clock, 4);
    std::vector<kj::Own<RecoveryGate::Permit>> connecting;
    for (const char* target : {"one", "two", "three", "four"})
      connecting.push_back(connections.acquire(target).wait(io.waitScope));
    auto repeat = connections.acquire("one");
    auto fifth = connections.acquire("five");
    KJ_REQUIRE(!repeat.poll(io.waitScope) && !fifth.poll(io.waitScope) && connections.active() == 4);
    connecting.back() = nullptr;
    KJ_REQUIRE(fifth.poll(io.waitScope) && !repeat.poll(io.waitScope), "a busy target cannot block a healthy target");
    connecting.front() = nullptr;
    KJ_REQUIRE(repeat.poll(io.waitScope) && connections.peakActive() == 4);
    repeat = nullptr;
    fifth = nullptr;
    connecting.clear();
    KJ_REQUIRE(connections.active() == 0, "abandoning a fulfilled promise returns its permit");

    struct Transport {
      explicit Transport(kj::Own<kj::AsyncIoStream> stream): stream(kj::mv(stream)) {}
      uint64_t getGeneration() const { return 2; }
      bool markTailFailed() { return !std::exchange(failed, true); }
      bool canFetchTails() const { return !failed; }
      kj::Own<kj::AsyncIoStream> stream;
      bool failed = false;
    };
    auto pipe = io.provider->newTwoWayPipe();
    PeerConnectionReuse<Transport> cached;
    cached.memberIdentity = "holder#19";
    cached.generation = 2;
    cached.connection = kj::rc<Transport>(kj::mv(pipe.ends[0]));
    auto replication = KJ_ASSERT_NONNULL(cached.connection).addRef();
    KJ_REQUIRE(!cached.markStale("holder#18", 2) && !cached.markStale("holder#19", 1) && !cached.stale);
    unsigned marks = 0;
    auto tail = bindDurableTailConnection(DurableTailTarget{"holder", "holder", "19", ""},
        replication.addRef(), [](auto, DurableTailAsk, std::string) -> kj::Promise<DurableTailAnswer> {
      return KJ_EXCEPTION(DISCONNECTED, "a test tail request failed");
    }, [&](uint64_t generation) {
      if (cached.markStale("holder#19", generation)) ++marks;
    });
    char byte = 0, sent = 'v';
    auto reading = replication->stream->tryRead(&byte, 1, 1).eagerlyEvaluate(nullptr);
    try {
      tail->page({}).wait(io.waitScope);
      KJ_FAIL_REQUIRE("the test tail should fail");
    } catch (const kj::Exception& error) {
      KJ_REQUIRE(error.getType() == kj::Exception::Type::DISCONNECTED);
      tail->failed();
    }
    tail->failed();
    KJ_REQUIRE(marks == 1 && cached.stale && cached.connection != kj::none && !tail->usable());
    cached.connection = kj::none;  // A replacement has removed the stale cache entry.
    tail.reset();
    pipe.ends[1]->write(kj::arrayPtr(&sent, 1).asBytes()).wait(io.waitScope);
    KJ_REQUIRE(reading.wait(io.waitScope) == 1 && byte == sent, "the existing stream survives a tail failure");
  }

  static void webSocketFrameBudget() {
    auto io = kj::setupAsyncIo();
    DurableSocketFrameBudget budget(16);
    auto submit = [&](uint64_t bytes, kj::Promise<void> delivery) -> kj::Promise<void> {
      auto reservation = budget.tryReserve(bytes);
      KJ_REQUIRE(reservation.has_value(), "delivery must fit");
      co_await delivery;
    };
    auto gate = kj::newPromiseAndFulfiller<void>();
    auto delivered = submit(3, kj::mv(gate.promise));
    auto cancelled = submit(5, kj::NEVER_DONE);
    KJ_REQUIRE(budget.usedBytes() == 16 && budget.limitBytes() == 16);
    KJ_REQUIRE(!budget.tryReserve(1).has_value());
    KJ_REQUIRE(budget.tryReserve(0).has_value() && budget.usedBytes() == 16);
    gate.fulfiller->fulfill();
    delivered.wait(io.waitScope);
    KJ_REQUIRE(budget.usedBytes() == 10);
    auto replacement = budget.tryReserve(3);
    KJ_REQUIRE(replacement.has_value() && budget.usedBytes() == 16);
    cancelled = nullptr;
    KJ_REQUIRE(budget.usedBytes() == 6);
    replacement.reset();
    KJ_REQUIRE(!budget.tryReserve(UINT64_MAX).has_value() && budget.refusals() == 2);
    KJ_REQUIRE(budget.usedBytes() == 0 && budget.tryReserve(8).has_value());
    KJ_REQUIRE(budget.usedBytes() == 0);

    KJ_REQUIRE(!durableSocketRefusal(123, true).has_value());
    KJ_REQUIRE(durableSocketRefusal(124, true)->code == 1002);
    KJ_REQUIRE(durableSocketRefusal(0, false)->code == 1013);
  }

  static void webSocketUploadBudget() {
    auto io = kj::setupAsyncIo();
    DurableSocketFrameBudget budget(8);
    enum class Scenario { COMPLETE, CANCEL_REQUEST, REPLY_WITHOUT_READING };
    class Home final: public kj::HttpService {
    public:
      Home(kj::PromiseFulfiller<void>& entered, kj::Promise<void> allowRead,
          kj::Promise<void> allowResponse, const kj::HttpHeaderTable& headers, Scenario scenario)
          : entered(entered), allowRead(kj::mv(allowRead)), allowResponse(kj::mv(allowResponse)),
            headers(headers), scenario(scenario) {}
      kj::Promise<void> request(kj::HttpMethod, kj::StringPtr, const kj::HttpHeaders&,
          kj::AsyncInputStream& body, Response& response) override {
        entered.fulfill();
        if (scenario != Scenario::REPLY_WITHOUT_READING) {
          co_await allowRead;
          char message[4];
          co_await body.read(kj::arrayPtr(message, sizeof(message)).asBytes());
          KJ_REQUIRE(std::string_view(message, sizeof(message)) == "hold");
          co_await allowResponse;
        }
        response.send(204, "No Content", kj::HttpHeaders(headers), uint64_t{0});
      }
    private:
      kj::PromiseFulfiller<void>& entered;
      kj::Promise<void> allowRead, allowResponse;
      const kj::HttpHeaderTable& headers;
      Scenario scenario;
    };
    class Reply final: public kj::HttpService::Response {
    public:
      kj::Own<kj::AsyncOutputStream> send(uint statusCode, kj::StringPtr, const kj::HttpHeaders&,
          kj::Maybe<uint64_t>) override {
        status = statusCode;
        return kj::heap<kj::NullStream>();
      }
      kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders&) override {
        KJ_FAIL_REQUIRE("unexpected WebSocket upgrade");
      }
      uint status = 0;
    };
    kj::HttpHeaderTable::Builder builder;
    capnp::HttpOverCapnpFactory::HeaderIdBundle ids(builder);
    auto headers = builder.build();
    capnp::ByteStreamFactory streams;
    capnp::HttpOverCapnpFactory factory(streams, kj::mv(ids), capnp::HttpOverCapnpFactory::LEVEL_2);
    for (auto scenario : {Scenario::COMPLETE, Scenario::CANCEL_REQUEST, Scenario::REPLY_WITHOUT_READING}) {
      auto entered = kj::newPromiseAndFulfiller<void>();
      auto allowRead = kj::newPromiseAndFulfiller<void>();
      auto allowResponse = kj::newPromiseAndFulfiller<void>();
      auto home = factory.kjToCapnp(kj::heap<Home>(*entered.fulfiller, kj::mv(allowRead.promise),
          kj::mv(allowResponse.promise), *headers, scenario));
      auto credit = budget.tryReserve(4);
      KJ_REQUIRE(credit.has_value());
      auto message = kj::heap<kj::String>(kj::str("hold"));
      auto bytes = message->asBytes();
      auto service = factory.capnpToKj(kj::mv(home), message.attach(std::move(*credit)));
      auto pipe = kj::newOneWayPipe(bytes.size());
      auto upload = pipe.out->write(bytes)
          .then([out = kj::mv(pipe.out)]() mutable { out = nullptr; }).eagerlyEvaluate(nullptr);
      kj::HttpHeaders requestHeaders(*headers);
      Reply reply;
      auto request = service->request(kj::HttpMethod::POST, "http://coworkerd-node/socket",
          requestHeaders, *pipe.in, reply);
      entered.promise.wait(io.waitScope);
      if (scenario != Scenario::REPLY_WITHOUT_READING) {
        KJ_REQUIRE(budget.usedBytes() == 8, "the home has not read the request body");
      }
      switch (scenario) {
        case Scenario::COMPLETE:
          allowRead.fulfiller->fulfill();
          KJ_REQUIRE(!request.poll(io.waitScope) && budget.usedBytes() == 0,
              "the upload has settled and the home still holds the reply");
          upload.wait(io.waitScope);
          allowResponse.fulfiller->fulfill();
          request.wait(io.waitScope);
          KJ_REQUIRE(reply.status == 204);
          break;
        case Scenario::CANCEL_REQUEST:
          request = nullptr;
          KJ_REQUIRE(budget.usedBytes() == 0 && !upload.poll(io.waitScope),
              "canceling the request returns its credit while the pipe write is still waiting");
          upload = nullptr;
          break;
        case Scenario::REPLY_WITHOUT_READING:
          request.wait(io.waitScope);
          KJ_REQUIRE(reply.status == 204 && budget.usedBytes() == 0 && !upload.poll(io.waitScope),
              "a reply without a read returns credit while the pipe write is still waiting");
          upload = nullptr;
          break;
      }
    }
  }

  static void parkedWebSocket() {
    using Socket = ParkedWebSocket;
    using Action = Socket::Frame::Action;
    const Socket::Owner owner{4, 8}, staleOwner{4, 7};
    const Socket::Home first{"upgrade", 11}, next{"wake", 12};
    Socket socket(owner, first);
    KJ_REQUIRE(socket.configure(owner, first, std::pair{std::string("ping"), std::string("pong")}));
    const auto current = socket.receive(owner, "message");
    KJ_REQUIRE(current.action == Action::FORWARD && current.position == 1);
    KJ_REQUIRE(!socket.canPark(owner, first, current.position));
    KJ_REQUIRE(socket.complete(owner, current.position));
    KJ_REQUIRE(!socket.canPark(staleOwner, first, current.position));
    KJ_REQUIRE(!socket.canPark(owner, first, current.position - 1));

    const Socket::Snapshot saved{.tags = {"room", "member"}, .attachment = std::string("saved")};
    KJ_REQUIRE(socket.park(owner, first, current.position, saved));
    KJ_REQUIRE(!socket.authorized(owner, first));
    const auto automatic = socket.receive(owner, "ping");
    KJ_REQUIRE(automatic.action == Action::ANSWER && automatic.answer == "pong");
    KJ_REQUIRE(!socket.complete(owner, automatic.position));
    KJ_REQUIRE(socket.answered(owner, automatic.position, 1000));

    const auto wake = socket.receive(owner, "ping ");
    KJ_REQUIRE(wake.action == Action::WAKE && wake.position == automatic.position + 1);
    KJ_REQUIRE(socket.receive(owner, std::nullopt).action == Action::BACKPRESSURE);
    const auto* restored = socket.parked(owner);
    KJ_REQUIRE(restored && restored->tags == saved.tags && restored->attachment == saved.attachment);
    KJ_REQUIRE(restored->autoResponseTime == 1000);
    KJ_REQUIRE(!socket.complete(staleOwner, wake.position));
    socket.adopt(staleOwner, next);
    KJ_REQUIRE(!socket.authorized(owner, next));
    socket.adopt(owner, next);
    KJ_REQUIRE(socket.authorized(owner, next));
    KJ_REQUIRE(socket.adoptable(owner, next) == restored);
    KJ_REQUIRE(!socket.adoptable(owner, first));
    KJ_REQUIRE(socket.complete(owner, wake.position));
    KJ_REQUIRE(!socket.park(owner, first, wake.position, saved));
    KJ_REQUIRE(socket.park(owner, next, wake.position, saved));
    socket.fence(owner);
    KJ_REQUIRE(socket.receive(owner, "ping").action == Action::REFUSE);
    KJ_REQUIRE(!socket.parked(owner));
  }

  static void threadedAlarms() {
    auto io = kj::setupAsyncIo();
    MembershipView membership;
    DurableLog log({.config = {}, .objectStore = false},
        {}, io.provider->getNetwork(), io.provider->getTimer(), *io.lowLevelProvider, membership, {});
    double now = 1000;
    uint64_t wall = 500000;
    log.alarmMonotonicForTest = [&] { return now; };
    log.alarmWallForTest = [&] { return wall; };
    struct Attempt {
      DurableAlarm alarm;
      kj::Own<kj::PromiseFulfiller<AlarmResult>> result;
    };
    std::vector<Attempt> attempts;
    log.setAlarmHook([&](DurableAlarm alarm) {
      KJ_REQUIRE(log.alarmFireIsCurrent(alarm));
      auto pending = kj::newPromiseAndFulfiller<AlarmResult>();
      attempts.push_back({std::move(alarm), kj::mv(pending.fulfiller)});
      return kj::mv(pending.promise);
    });
    auto flush = [&] {
      log.pumpAlarmFires();
      KJ_REQUIRE(io.waitScope.poll(1000) < 1000, "alarm scheduler did not become idle");
    };
    auto opened = [&](std::string objectId) {
      const auto name = durableHostName("namespace", objectId, 3);
      auto& object = log.objects[name];
      object.objectName = name;
      object.namespaceId = "namespace";
      object.objectId = objectId;
      object.tenant = "tenant";
      object.dataEpoch = 3;
      object.shardEpoch = 7;
      object.state = DurableLog::ObjectState::LOCAL;
      return name;
    };
    auto complete = [&](AlarmOutcome outcome, uint64_t atUnixMs = 0) {
      attempts.back().result->fulfill(AlarmResult{outcome, atUnixMs});
      flush();
    };

    const auto name = opened("retry");
    const auto key = "namespace/retry/0";
    log.alarmReported(name, 0, wall + 1);
    flush();
    KJ_REQUIRE(attempts.empty());
    ++wall;
    flush();
    KJ_REQUIRE(attempts.size() == 1 && attempts.back().alarm.retryCount == 0);
    complete(AlarmOutcome::UNAVAILABLE);
    KJ_REQUIRE(log.alarms.at(key).retries == 0);
    now += 1000;
    flush();
    KJ_REQUIRE(attempts.size() == 2 && attempts.back().alarm.retryCount == 0);
    for (uint32_t retry = 1; retry <= 6; ++retry) {
      const auto count = attempts.size();
      complete(AlarmOutcome::FAILED);
      const auto due = log.alarms.at(key).notBeforeMs;
      const double nominal = 2000u << (retry - 1);
      KJ_REQUIRE(due - now >= nominal * 0.875 && due - now < nominal * 1.125);
      now = due - 1;
      flush();
      KJ_REQUIRE(attempts.size() == count);
      now = due;
      flush();
      KJ_REQUIRE(attempts.size() == count + 1 && attempts.back().alarm.retryCount == retry &&
          !attempts.back().alarm.clear);
      if (retry == 1) {
        complete(AlarmOutcome::DEFERRED);
        KJ_REQUIRE(log.alarms.at(key).retries == 1 && attempts.size() == count + 1);
        log.alarmCapacityAvailable("node");
        flush();
        KJ_REQUIRE(attempts.size() == count + 2 && attempts.back().alarm.retryCount == 1);
      }
    }
    const auto count = attempts.size();
    complete(AlarmOutcome::FAILED);
    KJ_REQUIRE(attempts.size() == count + 1 && attempts.back().alarm.clear && log.alarmRetries == 6);
    const auto exhausted = attempts.back().alarm;
    log.alarmReported(name, 0, ++wall);
    flush();
    KJ_REQUIRE(attempts.size() == count + 1 && !log.alarmFireIsCurrent(exhausted),
        "a replacement schedule must wait for the old dispatch, whose token was invalidated");
    complete(AlarmOutcome::CLEARED);
    KJ_REQUIRE(attempts.size() == count + 2 && attempts.back().alarm.retryCount == 0 &&
        !attempts.back().alarm.clear && log.alarmFireIsCurrent(attempts.back().alarm));

    complete(AlarmOutcome::FAILED);
    now = log.alarms.at(key).notBeforeMs;
    flush();
    KJ_REQUIRE(attempts.back().alarm.retryCount == 1);
    complete(AlarmOutcome::FIRED);
    now = log.alarms.at(key).notBeforeMs;
    flush();
    KJ_REQUIRE(attempts.back().alarm.retryCount == 0 && log.alarms.at(key).retries == 0);

    complete(AlarmOutcome::NOT_DUE, wall + 50);
    KJ_REQUIRE(log.alarms.at(key).atUnixMs == wall + 50);
    KJ_REQUIRE(log.objects.at(name).alarms.at(0) == wall + 50);
    wall += 50;
    flush();
    KJ_REQUIRE(attempts.back().alarm.atUnixMs == wall);
    complete(AlarmOutcome::NOT_DUE);
    KJ_REQUIRE(!log.alarms.contains(key) && log.objects.at(name).alarms.at(0) == 0);
    log.alarmReported(name, 0, wall);
    flush();
    const auto beforeStop = attempts.size();
    complete(AlarmOutcome::STOP);
    KJ_REQUIRE(attempts.size() == beforeStop + 1 && attempts.back().alarm.clear &&
        attempts.back().alarm.retryCount == 0);
    complete(AlarmOutcome::UNAVAILABLE);
    now += 1000;
    flush();
    KJ_REQUIRE(attempts.size() == beforeStop + 2 && attempts.back().alarm.clear);
    complete(AlarmOutcome::CLEARED);
    KJ_REQUIRE(!log.alarms.contains(key));
    log.alarmReported(name, 0, wall);
    flush();
    const auto beforeParallel = attempts.size();

    for (const auto* objectId : {"first", "second", "third"}) log.alarmReported(opened(objectId), 0, wall);
    flush();
    KJ_REQUIRE(log.activeAlarmFires == 4 && attempts.size() == beforeParallel + 3);
    log.shardGone("namespace", 0, "test ownership change");
    KJ_REQUIRE(log.alarms.empty() && !log.alarmFireIsCurrent(attempts.back().alarm));
    for (auto& attempt : attempts) {
      if (attempt.result->isWaiting()) attempt.result->fulfill(AlarmResult{AlarmOutcome::UNAVAILABLE});
    }
    flush();
    KJ_REQUIRE(log.activeAlarmFires == 0 && log.alarms.empty());
  }

  // The lease of a membership epoch, by the tracker alone. TTL 5 s, interval 1 s.
  static void leaseTracker() {
    using V = LeaseTracker::Verdict;
    {
      LeaseTracker lease(5000, 1000);
      lease.begin(7);
      // Before its first success an epoch holds nothing, and nothing is fenced by asking.
      KJ_REQUIRE(lease.judge(7, 0).verdict == V::NOT_HELD && !lease.fenced(7));
      const auto first = lease.sent(7, 0, 0);
      KJ_REQUIRE(first != 0 && lease.judge(7, 10).verdict == V::WAIT && lease.judge(7, 10).untilMs == 1000);
      KJ_REQUIRE(lease.settled(first, true, 20));
      const auto held = lease.judge(7, 4999.9);
      KJ_REQUIRE(held.verdict == V::HELD && held.untilMs == 5000);
      // At S + T exactly the slot on the TTL (5000) has come and not gone out: that slot's own window, to 6000, is
      // all that is left; at its end, with nothing in flight, expired and fenced for good.
      const auto last = lease.judge(7, 5000);
      KJ_REQUIRE(last.verdict == V::WAIT && last.untilMs == 6000 && !lease.fenced(7));
      KJ_REQUIRE(lease.judge(7, 6000).verdict == V::EXPIRED && lease.fenced(7));
      KJ_REQUIRE(lease.sent(7, 6001, 5000) == 0, "a fenced epoch sends no heartbeat");
      KJ_REQUIRE(lease.judge(7, 6002).verdict == V::EXPIRED);
    }
    // Case 3: a boundary heartbeat in flight -- in time (renews), failed, successful after its end.
    for (const auto outcome : {"in-time", "failed", "after-end"}) {
      LeaseTracker lease(5000, 1000);
      lease.begin(8);
      KJ_REQUIRE(lease.settled(lease.sent(8, 0, 0), true, 5));
      for (double slot = 1000; slot < 5000; slot += 1000) lease.settled(lease.sent(8, slot, slot), false, slot + 900);
      // The slot on the TTL itself goes out a little late: its end stays slot + I.
      const auto boundary = lease.sent(8, 5000.2, 5000);
      const auto wait = lease.judge(8, 5000.5);
      KJ_REQUIRE(wait.verdict == V::WAIT && wait.untilMs == 6000, outcome);
      if (std::string_view(outcome) == "in-time") {
        KJ_REQUIRE(lease.settled(boundary, true, 5500));
        const auto renewed = lease.judge(8, 10000.1);
        KJ_REQUIRE(renewed.verdict == V::HELD && std::abs(renewed.untilMs - 10000.2) < 1e-6, outcome, renewed.untilMs);
        // The next slot on the TTL from the new D (10000) has come but not gone out: its window to 11000 is left.
        KJ_REQUIRE(lease.judge(8, 10000.2).verdict == V::WAIT && lease.judge(8, 11000).verdict == V::EXPIRED, outcome);
      } else if (std::string_view(outcome) == "failed") {
        KJ_REQUIRE(!lease.settled(boundary, false, 5500));
        KJ_REQUIRE(lease.judge(8, 5500).verdict == V::EXPIRED, outcome);
      } else {
        // Its end passed while it was in flight: no WAIT past 6000, and the success handled later renews nothing.
        KJ_REQUIRE(lease.judge(8, 6000).verdict == V::EXPIRED, outcome);
        KJ_REQUIRE(!lease.settled(boundary, true, 6000.1) && lease.lateSuccesses == 1, outcome);
        KJ_REQUIRE(lease.judge(8, 6001).verdict == V::EXPIRED, outcome);
      }
    }
    {
      // A heartbeat sent after the stall, for a slot beyond D + T, never holds an answer back, and its success after
      // the lapse does not revive the epoch.
      LeaseTracker lease(5000, 1000);
      lease.begin(9);
      KJ_REQUIRE(lease.settled(lease.sent(9, 0, 0), true, 1));
      const auto late = lease.sent(9, 9000, 9000);
      KJ_REQUIRE(late != 0 && !lease.settled(late, true, 9100) && lease.fenced(9));
      KJ_REQUIRE(lease.judge(9, 9200).verdict == V::EXPIRED);
    }
    {
      // Out of order: a newer success first; the older one handled after its end moves nothing back, and an old
      // failure does not expire an epoch that a newer heartbeat renewed.
      LeaseTracker lease(5000, 1000);
      lease.begin(10);
      KJ_REQUIRE(lease.settled(lease.sent(10, 0, 0), true, 1));
      const auto older = lease.sent(10, 1000, 1000), newer = lease.sent(10, 2000, 2000);
      KJ_REQUIRE(lease.settled(newer, true, 2050));
      // Times only go forward here (a reading that goes back fences, see below): S stays 2000, so held until 7000.
      KJ_REQUIRE(!lease.settled(older, true, 2060) && lease.judge(10, 2070).untilMs == 7000);
      const auto third = lease.sent(10, 3000, 3000), fourth = lease.sent(10, 4000, 4000);
      KJ_REQUIRE(lease.settled(fourth, true, 4010) && !lease.settled(third, false, 4020));
      KJ_REQUIRE(lease.judge(10, 8999).verdict == V::HELD && lease.judge(10, 9000).verdict == V::WAIT);
      KJ_REQUIRE(lease.judge(10, 10000).verdict == V::EXPIRED);
    }
    {
      // A heartbeat whose request was abandoned holds nothing back any more; a given-up PUT that reaches the store
      // later has no callback here at all.
      LeaseTracker lease(5000, 1000);
      lease.begin(11);
      KJ_REQUIRE(lease.settled(lease.sent(11, 0, 0), true, 1));
      const auto abandoned = lease.sent(11, 5000, 5000);
      KJ_REQUIRE(lease.judge(11, 5100).verdict == V::WAIT);
      lease.forget(abandoned);
      KJ_REQUIRE(lease.judge(11, 5200).verdict == V::EXPIRED);
    }
    {
      // Case 4: after a new epoch is registered, the old epoch's answers and fences touch nothing of it.
      LeaseTracker lease(5000, 1000);
      lease.begin(12);
      KJ_REQUIRE(lease.settled(lease.sent(12, 0, 0), true, 1));
      // In flight, and still within its own end, when the next epoch is registered: its success is the old epoch's.
      const auto old = lease.sent(12, 4500, 4500);
      lease.begin(13);
      KJ_REQUIRE(!lease.settled(old, true, 4700) && lease.judge(13, 4701).verdict == V::NOT_HELD && !lease.fenced(13));
      lease.fence(12, "late");
      KJ_REQUIRE(lease.judge(12, 4702).verdict == V::EXPIRED && !lease.fenced(13));
      KJ_REQUIRE(lease.settled(lease.sent(13, 4703, 4703), true, 4704) && lease.judge(13, 4705).verdict == V::HELD);
    }
    {
      // The slot on the TTL after two failed heartbeats, with the production default (TTL 15 s, interval 5 s) and the
      // conversions writeHeartbeat makes: the epoch's first heartbeat takes its send reading as its slot, every later
      // one the renewing slot plus the cadence's own distance (cadenceSlot), so the slot on the TTL is exactly D + T
      // while S, a send reading, is a little later. Neither the moment between S + T and that heartbeat going out, nor
      // the heartbeat in flight, may expire the epoch (report problem A).
      LeaseTracker lease(15000, 5000);
      lease.begin(1);
      KJ_REQUIRE(lease.settled(lease.sent(1, 1000.300, 1000.300), true, 1000.400));
      KJ_REQUIRE(lease.settled(lease.sent(1, 6000.904, lease.cadenceSlot(5000)), true, 6001.000));  // D 6000.300, S 6000.904
      KJ_REQUIRE(!lease.settled(lease.sent(1, 11000.801, lease.cadenceSlot(5000)), false, 11010));
      KJ_REQUIRE(!lease.settled(lease.sent(1, 16000.502, lease.cadenceSlot(10000)), false, 16010));
      // S + T = 21000.904: the slot on the TTL (21000.300) has come, its heartbeat not gone out yet.
      const auto waiting = lease.judge(1, 21000.950);
      KJ_REQUIRE(waiting.verdict == V::WAIT && std::abs(waiting.untilMs - 26000.300) < 1e-6, waiting.untilMs);
      const auto boundary = lease.sent(1, 21001.200, lease.cadenceSlot(15000));
      KJ_REQUIRE(boundary != 0 && lease.judge(1, 21001.500).verdict == V::WAIT);
      KJ_REQUIRE(lease.settled(boundary, true, 21050) && lease.judge(1, 26000).verdict == V::HELD && !lease.fenced(1));
      // The same heartbeat never sent: the epoch expires at that slot's end, not before.
      LeaseTracker silent(15000, 5000);
      silent.begin(2);
      KJ_REQUIRE(silent.settled(silent.sent(2, 1000.300, 1000.300), true, 1000.400));
      KJ_REQUIRE(silent.judge(2, 16000.299).verdict == V::HELD && silent.judge(2, 16000.400).verdict == V::WAIT);
      KJ_REQUIRE(silent.judge(2, 21000.299).verdict == V::WAIT && silent.judge(2, 21000.300).verdict == V::EXPIRED);
    }
    {
      // slotFor, the computation writeHeartbeat uses, with jittered readings: the renewing heartbeat's send reading is
      // 0.604 ms after its slot and the one on the TTL is read 0.890 ms late, the one later than the other. Its slot
      // must still be D + T exactly (a slot from the readings, sent - late, lands 0.006 ms past it and never qualifies).
      LeaseTracker lease(15000, 5000);
      lease.begin(3);
      KJ_REQUIRE(lease.slotFor(true, 1000.300, 0.000, 0) == 1000.300, "an epoch's first heartbeat takes its reading");
      KJ_REQUIRE(lease.settled(lease.sent(3, 1000.300, lease.slotFor(false, 1000.300, 0, 0)), true, 1000.400));
      KJ_REQUIRE(lease.settled(lease.sent(3, 6000.904, lease.slotFor(true, 6000.904, 0.600, 5000)), true, 6001.000));
      // The distances are counted from the renewing heartbeat's own slot (writeHeartbeat: due - lastHeartbeatOkDue).
      KJ_REQUIRE(!lease.settled(lease.sent(3, 11000.801, lease.slotFor(true, 11000.801, 0.497, 5000)), false, 11010));
      KJ_REQUIRE(!lease.settled(lease.sent(3, 16000.950, lease.slotFor(true, 16000.950, 0.646, 10000)), false, 16010));
      const auto slot = lease.slotFor(true, 21001.200, 0.890, 15000);
      KJ_REQUIRE(std::abs(slot - 21000.300) < 1e-6, slot);
      const auto boundary = lease.sent(3, 21001.200, slot);
      KJ_REQUIRE(lease.judge(3, 21001.500).verdict == V::WAIT, "the heartbeat on the TTL must keep the answer back");
      KJ_REQUIRE(lease.settled(boundary, true, 21050) && lease.judge(3, 26000).verdict == V::HELD);
    }
    {
      // A clock that cannot be read, or goes back, fences.
      LeaseTracker unread(5000, 1000), back(5000, 1000);
      unread.begin(1);
      KJ_REQUIRE(unread.settled(unread.sent(1, 0, 0), true, 1));
      KJ_REQUIRE(unread.judge(1, -1).verdict == V::EXPIRED && unread.fenced(1));
      back.begin(1);
      KJ_REQUIRE(back.settled(back.sent(1, 100, 100), true, 101) && back.judge(1, 200).verdict == V::HELD);
      KJ_REQUIRE(back.judge(1, 150).verdict == V::EXPIRED && back.fenced(1));
    }
  }

  // The confirmation path of the real log, with the lease hook a test controls. The log has no peers, so each
  // record is confirmed by the local flush (the path that can block on a slow disk).
  static void leaseConfirm(const char* scenario) {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-lease-test-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    // The event loop's timer never moves here: what decides is the lease clock read at each confirmation (case 5).
    kj::TimerImpl clock(kj::origin<kj::TimePoint>());
    ObjectStoreConfig store;
    store.backend = "directory";
    store.directory = temp;
    store.endpoint = "unused";
    MembershipView membership;
    const std::string_view name(scenario);
    // The membership's current epoch; "old-epoch": it registered epoch 2 while this log still writes under 1.
    membership.epoch = name == "old-epoch" ? "2" : "1";
    DurableLog log({"node", "instance", temp, {}}, store, io.provider->getNetwork(), clock, *io.lowLevelProvider,
        membership, {});
    log.settings.config.peers = 0;
    log.setNodeEpoch(1).wait(io.waitScope);
    KJ_REQUIRE(!log.logAppendFailed, "the test log could not be opened");
    // The real tracker, on the real lease clock, with a TTL the scenario sets (wide margins: a busy machine must not
    // make the setup itself outlive a lease that should still hold).
    const double ttl = name == "expired" || name == "stale-timer" ? 300 : name == "slow-flush" ? 2000 : 60000;
    LeaseTracker tracker(ttl, 1000);
    tracker.begin(name == "old-epoch" ? 2 : 1);
    const auto start = leaseClockMs();
    if (name != "not-held") KJ_REQUIRE(tracker.settled(tracker.sent(tracker.epoch(), start, start), true, leaseClockMs()));
    std::optional<LeaseTracker::Judgment> forced;
    uint32_t lost = 0;
    double judgedAt = 0;
    bool throwAfterAppend = name == "completion-error";
    log.setLeaseHooks([&](uint64_t epoch, double now) {
          judgedAt = now;
          if (throwAfterAppend && log.appendedSeq == 1) {
            throwAfterAppend = false;
            KJ_FAIL_REQUIRE("fixture failed the first post-append lease check");
          }
          return forced ? *forced : tracker.judge(epoch, now);
        }, [&](std::string) { ++lost; });
    const int peer = log.openChannel("d0");
    KJ_REQUIRE(peer >= 0);
    auto& channel = log.channels.at("d0");
    const auto ns = std::string(64, '1'), id = std::string(64, '3');
    const auto host = durableHostName(ns, id, 1);
    auto& object = log.objects[host];
    log.locate(object, ns, id, 1);
    object.objectName = host;
    object.state = DurableLog::ObjectState::LOCAL;
    object.shardEpoch = 1;
    std::filesystem::create_directories(object.positionPath(temp).parent_path());
    auto take = [&](uint64_t request) {
      LogRecord record;
      record.kind = LogRecordKind::RESET;
      record.objectName = host;
      record.storageId = object.storageId;
      record.dataEpoch = 1;
      log.takeRecord(channel, std::move(record), request);
      io.waitScope.poll();
    };
    // Every TO_ACK the log wrote to the durable process so far: request -> status.
    std::map<uint64_t, uint8_t> acks;
    auto answers = [&]() {
      log.waitForLogWrites().wait(io.waitScope);
      io.waitScope.poll();
      char buffer[4096];
      std::string bytes;
      for (ssize_t n; (n = ::recv(peer, buffer, sizeof buffer, MSG_DONTWAIT)) > 0;) bytes.append(buffer, size_t(n));
      for (size_t at = 0; at + 4 <= bytes.size();) {
        uint32_t size = 0;
        std::memcpy(&size, bytes.data() + at, 4);
        const auto* message = reinterpret_cast<const unsigned char*>(bytes.data() + at + 4);
        if (size == 10 && message[0] == 1) {
          uint64_t request = 0;
          std::memcpy(&request, message + 1, 8);
          acks[request] = message[9];
        }
        at += 4 + size;
      }
      return acks;
    };
    const auto confirmed = [&]() { return log.confirmations; };
    if (name == "held") {
      take(41);
      KJ_REQUIRE(answers().at(41) == 0 && confirmed() == 1 && lost == 0, "a held lease confirms");
    } else if (name == "expired" || name == "stale-timer") {
      // The lease ends by the lease clock while the event loop's timer still reads its origin.
      ::usleep(600000);
      KJ_REQUIRE(clock.now() == kj::origin<kj::TimePoint>());
      const auto before = log.nextSeq;
      take(42);
      KJ_REQUIRE(answers().at(42) == 1 && confirmed() == 0 && log.nextSeq == before && lost == 1,
          "an expired lease refuses the record before it enters the log");
      KJ_REQUIRE(tracker.fenced(1) && log.leaseRefusals == 1);
      if (name == "stale-timer") {
        // Case 5: what was judged is a fresh reading of the lease clock, past the TTL, not the event loop's time.
        KJ_REQUIRE(judgedAt - start >= 600 && std::abs(leaseClockMs() - judgedAt) < 5000 &&
            clock.now() == kj::origin<kj::TimePoint>(), judgedAt - start);
      }
      // Fenced: later records of the epoch are refused as well, and the hook is not called again for it.
      take(43);
      KJ_REQUIRE(answers().at(43) == 1 && confirmed() == 0 && lost == 1);
    } else if (name == "slow-flush") {
      // Case 2: the local flush itself takes longer than what is left of the lease. Taken while held; judged again
      // after the flush, the confirmation is refused (a check only before the flush would have confirmed it).
      const auto trigger = std::filesystem::path(temp) / "slow-flush";
      { std::ofstream out(trigger); out << "4000"; }
      log.slowFlushForTest = trigger.string();
      take(44);
      auto flushed = log.waitForLogWrites();
      io.provider->getTimer().afterDelay(20 * kj::MILLISECONDS).wait(io.waitScope);
      KJ_REQUIRE(!flushed.poll(io.waitScope) && log.inFlight.contains(1) && confirmed() == 0,
          "the main loop's timer runs while the output gate waits for the log worker");
      flushed.wait(io.waitScope);
      KJ_REQUIRE(!std::filesystem::exists(trigger), "the slow flush did not run");
      KJ_REQUIRE(answers().at(44) == 1 && confirmed() == 0 && lost == 1 && tracker.fenced(1),
          "a lease that ended during the local flush must refuse the confirmation");
      KJ_REQUIRE(log.isolated.size() == 1 && log.inFlight.empty(), "the refused record is excluded like any other");
    } else if (name == "completion-error") {
      take(61);
      KJ_REQUIRE(!answers().contains(61) && !log.logWriting && !throwAfterAppend,
          "an error after I/O must release the batch slot");
      take(62);
      KJ_REQUIRE(answers().at(61) == 0 && answers().at(62) == 0 && confirmed() == 2,
          "a later batch progresses and confirms both writes");
    } else if (name == "rotate") {
      const auto original = log.logPath;
      const auto trigger = std::filesystem::path(temp) / "slow-flush";
      { std::ofstream out(trigger); out << "150"; }
      log.slowFlushForTest = trigger.string();
      take(51);
      take(52);
      auto rotation = log.setNodeEpoch(2);
      KJ_REQUIRE(!rotation.poll(io.waitScope) && log.nodeEpoch == 1,
          "the descriptor remains on the old epoch while any append is outstanding");
      rotation.wait(io.waitScope);
      KJ_REQUIRE(answers().at(51) == 1 && answers().at(52) == 1 && confirmed() == 0,
          "the old epoch's pending records could not acknowledge after rotation");
      KJ_REQUIRE(log.nextSeq == 0 && log.appendedSeq == 0 && log.flushedSeq == 0 && log.nodeEpoch == 2);
      const auto old = std::filesystem::file_size(original);
      KJ_REQUIRE(old > 0 && std::filesystem::file_size(log.logPath) == 0,
          "the old epoch drained and the new append file is empty");
      KJ_REQUIRE(log.earlierExcluded.at(1).size() == 2 && log.predecessorLogs.at(1).complete);
    } else if (name == "wait") {
      // A heartbeat in flight keeps the answer back; nothing is confirmed until it settles.
      forced = LeaseTracker::Judgment{LeaseTracker::Verdict::WAIT, leaseClockMs() + 30000, "in flight"};
      take(45);
      take(0);  // an internal record (request 0) is held back by the same rule
      KJ_REQUIRE(!answers().contains(45) && confirmed() == 0 && log.inFlight.size() == 2 && log.leaseWaits >= 1);
      bool scheduled = false;
      KJ_IF_SOME(wake, clock.nextEvent()) { scheduled = wake > clock.now(); }
      KJ_REQUIRE(scheduled, "a WAIT schedules its own judgment at the heartbeat's end");
      forced = LeaseTracker::Judgment{LeaseTracker::Verdict::HELD, 0, ""};
      log.leaseSettled();
      KJ_REQUIRE(answers().at(45) == 0 && confirmed() == 2 && log.inFlight.empty() && lost == 0);
    } else if (name == "wait-expired") {
      forced = LeaseTracker::Judgment{LeaseTracker::Verdict::WAIT, leaseClockMs() + 30000, "in flight"};
      take(46);
      take(0);
      KJ_REQUIRE(!answers().contains(46) && confirmed() == 0);
      forced = LeaseTracker::Judgment{LeaseTracker::Verdict::EXPIRED, 0, "the lease expired"};
      log.leaseSettled();
      KJ_REQUIRE(answers().at(46) == 1 && confirmed() == 0 && log.inFlight.empty() && log.isolated.size() == 2 &&
          lost == 1, "a WAIT that ends in expiry refuses every record that waited, internal ones included");
    } else if (name == "old-epoch") {
      // Report problem B: after the membership expired epoch 1 and registered 2 (and may already hold shards for it),
      // a write still judged under 1 is refused, but gives up nothing: epoch 1's shards went when it expired.
      take(47);
      KJ_REQUIRE(answers().at(47) == 1 && confirmed() == 0 && lost == 0 && !tracker.fenced(2), "an older epoch's refusal "
          "must not give up the shards of the membership's current epoch");
    } else if (name == "not-held") {
      // Report problem F: a current epoch whose first heartbeat has not renewed it yet refuses the write and gives up
      // nothing (the membership's own expiry check decides that epoch).
      take(48);
      take(49);
      KJ_REQUIRE(answers().at(48) == 1 && answers().at(49) == 1 && confirmed() == 0 && lost == 0 && !tracker.fenced(1));
    } else {
      KJ_FAIL_REQUIRE("unknown lease scenario", scenario);
    }
    log.closeChannel("d0");
  }

  // A peer that answers a newer epoch's tail request raises its floor for the shard.
  // "raise": a verified newer epoch fences older appends; what was held stays and is answered; the sequence answered
  // stays below the refused record until the owner drops it. "unverified": numbers the store does not show raise
  // nothing, and the real newer owner still gets in. "old-ask": an older request lowers nothing.
  static void peerFence(const char* scenario) {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-peer-fence-test-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    kj::TimerImpl clock(kj::origin<kj::TimePoint>());
    ObjectStoreConfig store;
    store.backend = "directory";
    store.directory = temp;
    store.endpoint = "unused";
    const std::string_view name(scenario);
    const std::string ns(64, '1');
    auto objectName = [&](unsigned n) { return ns + "/" + std::string(63, '2') + char('0' + n); };
    MembershipView membership;
    membership.heldEpochs.insert("9");
    DurableLog log({"peer", "instance", temp, {}}, store, io.provider->getNetwork(), clock, *io.lowLevelProvider,
        membership, {});
    log.nodeEpoch = 9;
    auto epochObject = [&](uint32_t shard, uint64_t epoch, std::optional<uint32_t> bodyShard = std::nullopt) {
      ShardEpoch record;
      record.namespaceId = ns; record.shard = bodyShard.value_or(shard); record.epoch = epoch;
      record.nodeId = "node-3"; record.nodeEpoch = 7; record.dataEpoch = 1; record.reason = "timeout";
      record.instance = "i"; record.nonce = "n"; record.acquiredAtUnixMs = 1;
      // The directory backend has no create-if-absent: a plain put stands in for the owner's create.
      log.store->put(shardEpochKey(ns, shard, epoch), shardEpochJson(record)).wait(io.waitScope);
    };
    auto record = [&](uint64_t ownerEpoch, uint64_t seq, uint32_t shard, uint64_t shardEpoch, unsigned object) {
      LogRecord r;
      r.nodeEpoch = ownerEpoch; r.nodeSeq = seq; r.shard = shard; r.shardEpoch = shardEpoch;
      r.objectName = objectName(object); r.dataEpoch = 1; r.kind = LogRecordKind::RESET; r.txn = seq;
      return encodeLogRecord(r);
    };
    uint64_t stream = 0;
    KJ_REQUIRE(log.openIncoming("owner", 2, "old", "9", 1, stream).empty());
    auto append = [&](uint64_t seq, uint32_t shard, uint64_t shardEpoch, unsigned object = 1) {
      return log.appendIncoming(stream, frameRecords({record(2, seq, shard, shardEpoch, object)}));
    };
    auto ask = [&](uint64_t shardEpoch) {
      DurableTailAsk out;
      out.namespaceId = ns; out.shard = 1; out.shardEpoch = shardEpoch; out.ownerNodeId = "owner"; out.ownerNodeEpoch = 2;
      return out;
    };
    auto raise = [&](uint64_t shardEpoch) { log.raisePeerFence(ask(shardEpoch)).wait(io.waitScope); };
    if (name == "raise") {
      epochObject(1, 4);
      KJ_REQUIRE(append(1, 1, 3).refusal.empty());
      raise(4);
      KJ_REQUIRE(log.peerFenceRaises == 1 && log.knownEpoch(ns, 1) == 4, "a verified newer epoch raises the floor",
          log.peerFenceRaises, log.knownEpoch(ns, 1));
      // What was held before the raise stays, and the newer owner gets it.
      auto tail = log.tailFor(ask(4));
      KJ_REQUIRE(tail.refusal.empty() && tail.records.size() == 1 && tail.records[0].nodeSeq == 1,
          "the held record is answered after the raise", tail.refusal.c_str(), tail.records.size());
      const auto refused = append(2, 1, 3);
      KJ_REQUIRE(refused.refusal.starts_with("fenced") && refused.accepted == 1, "an older epoch's append is refused",
          refused.refusal.c_str(), refused.accepted);
      // A later record of another shard is held, but the answer stays below the refused one.
      const auto other = append(3, 2, 3, 2);
      KJ_REQUIRE(other.refusal.empty() && other.accepted == 1 && log.incoming.at(stream).held.contains(3),
          "the answer must not reach a refused record", other.accepted);
      const auto again = log.tailFor(ask(4));
      KJ_REQUIRE(again.records.size() == 1 && again.records[0].nodeSeq == 1, "the refused record is in no answer");
      // The owner drops it (it was never confirmed): the answer moves on.
      log.dropIncoming(stream, {2});
      KJ_REQUIRE(append(4, 2, 3, 2).accepted == 4, "after the drop the answer covers what is held");
      // The same epoch again checks nothing more; a record of the floor's epoch itself is taken.
      raise(4);
      KJ_REQUIRE(log.peerFenceChecks == 1);
      uint64_t newer = 0;
      KJ_REQUIRE(log.openIncoming("node-3", 7, "new", "9", 1, newer).empty());
      LogRecord r;
      r.nodeEpoch = 7; r.nodeSeq = 1; r.shard = 1; r.shardEpoch = 4; r.objectName = objectName(1); r.dataEpoch = 1;
      r.kind = LogRecordKind::RESET; r.txn = 1;
      const auto taken = log.appendIncoming(newer, frameRecords({encodeLogRecord(r)}));
      KJ_REQUIRE(taken.refusal.empty() && taken.accepted == 1, "the newer owner's records are taken", taken.refusal.c_str());
    } else if (name == "unverified") {
      KJ_REQUIRE(append(1, 1, 3).refusal.empty());
      // No object at that epoch: an arbitrarily high number raises nothing.
      raise(9);
      // An object at the key whose bytes name another shard is not that epoch either.
      epochObject(1, 8, 2);
      raise(8);
      KJ_REQUIRE(log.peerFenceRaises == 0 && log.peerFenceUnverified == 2 && log.knownEpoch(ns, 1) == 0 &&
          log.ownerFenceThrough.empty(), "an unverified epoch must not raise the floor", log.peerFenceRaises,
          log.knownEpoch(ns, 1));
      const auto still = append(2, 1, 3);
      KJ_REQUIRE(still.refusal.empty() && still.accepted == 2, "nothing is fenced by an unverified number", still.refusal.c_str());
      auto tail = log.tailFor(ask(4));
      KJ_REQUIRE(tail.refusal.empty() && tail.records.size() == 2, "the legitimate newer owner still gets its tail");
      // The real newer owner, whose epoch the store shows, raises it as usual.
      epochObject(1, 4);
      raise(4);
      KJ_REQUIRE(log.peerFenceRaises == 1 && log.knownEpoch(ns, 1) == 4);
    } else if (name == "old-ask") {
      epochObject(1, 3);
      epochObject(1, 5);
      raise(5);
      KJ_REQUIRE(log.knownEpoch(ns, 1) == 5);
      // An older (also real) epoch's request comes late: the floor stays where it is.
      raise(3);
      KJ_REQUIRE(log.knownEpoch(ns, 1) == 5 && log.ownerFenceThrough.at({ns, 1}) == 4,
          "an older request must not lower the floor", log.knownEpoch(ns, 1));
      auto tail = log.tailFor(ask(3));
      KJ_REQUIRE(!tail.refusal.empty(), "an older epoch gets no tail once a newer one asked");
      KJ_REQUIRE(append(1, 1, 4).refusal.starts_with("fenced"), "an append below the floor stays refused");
    } else if (name == "coalesce") {
      // The store does not show epoch 6 yet: requests of it meanwhile share one read, a failure is kept for a
      // second (answered at once, no read), and a background read raises the floor once the store shows it.
      auto first = log.raisePeerFence(ask(6));
      auto second = log.raisePeerFence(ask(6));
      first.wait(io.waitScope);
      second.wait(io.waitScope);
      KJ_REQUIRE(log.peerFenceChecks == 1 && log.peerFenceUnverified == 1 && log.knownEpoch(ns, 1) == 0,
          "concurrent requests of one epoch share one read", log.peerFenceChecks, log.peerFenceUnverified);
      raise(6);
      KJ_REQUIRE(log.peerFenceChecks == 1 && log.peerFenceUnverified == 1, "a failure is kept for a second");
      // The first background read, a second later, still finds nothing (the next is due two seconds after it).
      clock.advanceTo(clock.now() + 1001 * kj::MILLISECONDS);
      io.waitScope.poll();
      KJ_REQUIRE(log.peerFenceUnverified == 2 && log.knownEpoch(ns, 1) == 0, log.peerFenceUnverified);
      // Past the second, a request of the epoch reads again and fails again, but starts no second chain of rereads:
      // the store showing the epoch now, nothing reads it before the one chain's next read.
      raise(6);
      KJ_REQUIRE(log.peerFenceChecks == 2 && log.peerFenceUnverified == 3, log.peerFenceChecks);
      epochObject(1, 6);
      clock.advanceTo(clock.now() + 1001 * kj::MILLISECONDS);
      io.waitScope.poll();
      KJ_REQUIRE(log.knownEpoch(ns, 1) == 0, "one chain of background rereads per epoch");
      clock.advanceTo(clock.now() + 1001 * kj::MILLISECONDS);
      io.waitScope.poll();
      KJ_REQUIRE(log.knownEpoch(ns, 1) == 6 && log.peerFenceRaises == 1 && log.fenceRereading.empty(),
          "a background read raises the floor", log.knownEpoch(ns, 1));
      // A newer epoch the store does not show is read on its own.
      raise(9);
      KJ_REQUIRE(log.peerFenceChecks == 3 && log.knownEpoch(ns, 1) == 6);
    } else if (name == "outage") {
      // The store never shows epoch 6 and a request of it comes every second for 40 s: about one read a second in
      // all -- the request's own once the failure kept for a second has expired, and one chain of background rereads
      // (1, 2, 4, 8, 16 s), started again only after it ends. Here: 40 + 5 + 3 = 48.
      for (int second = 0; second < 40; ++second) {
        raise(6);
        clock.advanceTo(clock.now() + 1000 * kj::MILLISECONDS);
        io.waitScope.poll();
      }
      KJ_REQUIRE(log.peerFenceUnverified <= 50 && log.knownEpoch(ns, 1) == 0, "the reads of one epoch are bounded",
          log.peerFenceUnverified);
    } else {
      KJ_FAIL_REQUIRE("unknown peer-fence scenario", scenario);
    }
  }

  // An owner reads a peer's answer as covering every record up to it. A record the peer refused
  // -- fenced, its buffer full, or anything else -- must never count as held there unless the peer takes it later.
  // The rule is the owner's (it knows the record of every call); the peer holds its answers back for the fence only.
  static LogRecord testRecord(const std::string& ns, uint64_t ownerEpoch, uint64_t seq, uint32_t shard,
      uint64_t shardEpoch, size_t padding = 0) {
    LogRecord r;
    r.nodeEpoch = ownerEpoch; r.nodeSeq = seq; r.shard = shard; r.shardEpoch = shardEpoch;
    r.objectName = ns + "/" + std::string(63, '2') + char('0' + shard); r.dataEpoch = 1;
    r.kind = LogRecordKind::RESET; r.txn = seq; r.facetName = std::string(padding, 'p');
    return r;
  }

  // Peer side, with fenced refusals holding back cumulative answers. "fenced" (an owner of the older version,
  // which reads answers cumulatively):
  // record 1 taken, record 2 refused as fenced, record 3 (another shard) taken -- every answer stays below 2, through
  // a reconnect too, until a trim covers 2. "full": a full buffer does not hold the answers back (that would stall
  // every later record of the stream until the owner gives 2 up); the owner of this version does not count 2 anyway.
  static void refusedPeer(const char* scenario) {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-refused-peer-test-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    kj::TimerImpl clock(kj::origin<kj::TimePoint>());
    ObjectStoreConfig store;
    store.backend = "directory"; store.directory = temp; store.endpoint = "unused";
    const std::string_view name(scenario);
    const std::string ns(64, '1');
    MembershipView membership;
    membership.heldEpochs.insert("9");
    DurableLog log({"peer", "instance", temp, {}}, store, io.provider->getNetwork(), clock, *io.lowLevelProvider,
        membership, {});
    log.nodeEpoch = 9;
    log.settings.config.peerBufferBytes = 32768;
    uint64_t stream = 0;
    KJ_REQUIRE(log.openIncoming("owner", 2, "old", "9", 1, stream).empty());
    uint64_t oldOwnerAcked = 0;  // what an owner using cumulative answers counts: the highest answer it saw
    auto send = [&](const LogRecord& r) {
      auto result = log.appendIncoming(stream, frameRecords({encodeLogRecord(r)}));
      oldOwnerAcked = std::max(oldOwnerAcked, result.accepted);
      return result;
    };
    const auto one = testRecord(ns, 2, 1, 2, 3), three = testRecord(ns, 2, 3, 2, 3), four = testRecord(ns, 2, 4, 2, 3);
    auto two = testRecord(ns, 2, 2, 1, 3);
    KJ_REQUIRE(send(one).accepted == 1);
    if (name == "fenced") {
      log.fenceOwner(ns, 1, 3, "test", false);
      KJ_REQUIRE(send(two).refusal.starts_with("fenced"));
      const auto later = send(three);
      KJ_REQUIRE(later.refusal.empty() && log.incoming.at(stream).held.contains(3) && later.accepted == 1,
          "a later record is taken, but the answer must not reach the refused one", later.accepted);
      uint64_t again = 0;
      KJ_REQUIRE(log.openIncoming("owner", 2, "old", "9", 1, again).empty() && again == stream);
      KJ_REQUIRE(send(one).accepted == 1 && send(three).accepted == 1 && send(two).refusal.starts_with("fenced"),
          "after a reconnect");
      KJ_REQUIRE(oldOwnerAcked < 2, "an owner reading the answers cumulatively would count the refused record");
      log.trimIncoming(stream, 2);
      KJ_REQUIRE(send(four).accepted == 4, "a trim covering the refused record releases the answer");
    } else if (name == "full") {
      two.facetName = std::string(40000, 'p');
      KJ_REQUIRE(send(two).refusal.starts_with("full"));
      const auto later = send(three);
      KJ_REQUIRE(later.refusal.empty() && later.accepted == 3 && log.incoming.at(stream).fencedSeqs.empty(),
          "a full buffer does not hold the answers back", later.accepted);
    } else if (name == "resend-held") {
      // A record this node holds, sent again after a reconnect once its shard's floor has gone up, is taken as a
      // duplicate (it was taken before the floor and is in the newer owner's tail); a new record below the floor is
      // still refused, and the answers are not held back by the duplicate.
      KJ_REQUIRE(send(two).accepted == 2);
      log.fenceOwner(ns, 1, 3, "test", false);
      uint64_t again = 0;
      KJ_REQUIRE(log.openIncoming("owner", 2, "old", "9", 1, again).empty() && again == stream);
      KJ_REQUIRE(send(one).accepted == 2, "a held record of another shard");
      const auto resent = send(two);
      KJ_REQUIRE(resent.refusal.empty() && resent.accepted == 2 && log.incoming.at(stream).fencedSeqs.empty(),
          "a held record below the floor is a duplicate, not refused", resent.refusal.c_str());
      auto five = testRecord(ns, 2, 5, 1, 3);
      KJ_REQUIRE(send(five).refusal.starts_with("fenced"), "a new record below the floor is refused");
      // Other bytes under the held sequence 2 are not that record: checked like a new one, and refused.
      auto changed = two;
      changed.txn = 99;
      KJ_REQUIRE(send(changed).refusal.starts_with("fenced"), "a different record under a held sequence is refused");
    } else if (name == "full-resend") {
      // At its limit, the peer still takes a record it already holds, sent again after a reconnect: that record needs
      // no room, and refusing it would fail a write the peer can confirm.
      log.settings.config.peerBufferBytes = log.incomingBytes + 1;
      uint64_t again = 0;
      KJ_REQUIRE(log.openIncoming("owner", 2, "old", "9", 1, again).empty() && again == stream);
      const auto resent = send(one);
      KJ_REQUIRE(resent.refusal.empty() && resent.accepted == 1, "a held record sent again is taken", resent.refusal.c_str());
      KJ_REQUIRE(send(three).refusal.starts_with("full"), "a new record is still refused");
    } else {
      KJ_FAIL_REQUIRE("unknown refused-peer scenario", scenario);
    }
  }

  struct FakeSink final: DurableLog::PeerSink {
    struct Call {
      uint64_t sequence = 0;  // 0: a piece that does not begin the record
      bool partial = false;
      kj::Own<kj::PromiseFulfiller<Accepted>> fulfiller;
    };
    explicit FakeSink(std::deque<Call>& calls): calls(calls) {}
    kj::Promise<Accepted> append(std::string records, bool partial) override {
      uint64_t sequence = 0;
      if (records.size() >= 13 && records[4] >= 1 && records[4] <= 4)
        for (int i = 0; i < 8; ++i) sequence |= uint64_t(uint8_t(records[5 + i])) << (8 * i);
      auto paf = kj::newPromiseAndFulfiller<Accepted>();
      calls.push_back({sequence, partial, kj::mv(paf.fulfiller)});
      return kj::mv(paf.promise);
    }
    kj::Promise<void> trim(uint64_t) override { return kj::READY_NOW; }
    kj::Promise<void> drop(std::vector<uint64_t>) override { return kj::READY_NOW; }
    std::deque<Call>& calls;
  };

  // A peer in this process for the "pair" scenarios: the real append path of another DurableLog.
  struct LocalSink final: DurableLog::PeerSink {
    LocalSink(DurableLog& peer, uint64_t stream): peer(peer), stream(stream) {}
    kj::Promise<Accepted> append(std::string records, bool partial) override {
      auto result = peer.appendIncoming(stream, records, partial);
      return Accepted{result.accepted, result.refusal};
    }
    kj::Promise<void> trim(uint64_t upTo) override { peer.trimIncoming(stream, upTo); return kj::READY_NOW; }
    kj::Promise<void> drop(std::vector<uint64_t> sequences) override {
      peer.dropIncoming(stream, sequences);
      return kj::READY_NOW;
    }
    DurableLog& peer;
    uint64_t stream;
  };

  // Owner side, with acknowledgements accounted for per record. "<reason>" and "<reason>-reconnect"
  // (reason: fenced, full, cut-short, limit, corrupt), with a peer of the older version that answers with its
  // highest sequence: records 1, 2, 3 (three objects) go out; the peer answers 1 with 3, refuses 2 for the reason
  // and takes 3. Only 1 and 3 may be confirmed;
  // with "-reconnect" the session is then replaced and everything sent again, the peer answering as before. A full
  // buffer is tried again on the same session after FULL_RETRY_FIRST_MS: taken then, 2 is confirmed. "chunk": record
  // 2 is larger than APPEND_CHUNK_BYTES, its first piece answered with 3 and its last refused as fenced: 2 must not
  // count from its first piece's answer. "pair-full", "pair-fenced": the peer is a real DurableLog, its buffer too
  // small for 2, or its floor above 2's shard epoch. Full: 3 is confirmed at once, 2 after a retry once the buffer
  // has room. Fenced: the peer holds its answers back, 3 waits until the owner gives 2 up (the stream is opened again
  // without it), then 3 is confirmed.
  static void refusedOwner(const char* scenario) {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-refused-owner-test-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    const auto peerTemp = temp + "/peer";
    std::filesystem::create_directories(peerTemp);
    auto io = kj::setupAsyncIo();
    kj::TimerImpl clock(kj::origin<kj::TimePoint>());
    ObjectStoreConfig store;
    store.backend = "directory"; store.directory = temp; store.endpoint = "unused";
    ObjectStoreConfig peerStore = store;
    peerStore.directory = peerTemp;
    std::string name(scenario);
    const bool reconnect = name.ends_with("-reconnect");
    if (reconnect) name.resize(name.size() - 10);
    const bool pair = name.starts_with("pair-"), chunk = name == "chunk";
    const std::string ns(64, '1');
    MembershipView membership;
    membership.epoch = "1";
    Member member;
    member.id = "peer"; member.address = "unused:1"; member.epoch = "5"; member.startedAtUnixMs = 1;
    member.durableProcesses = 1;
    membership.members["peer"] = member;
    MembershipView peerMembership;
    peerMembership.heldEpochs.insert("5");
    DurableLog peer({"peer", "instance", peerTemp, {}}, peerStore, io.provider->getNetwork(), clock,
        *io.lowLevelProvider, peerMembership, {});
    peer.nodeEpoch = 5;
    peer.settings.config.peerBufferBytes = 32768;
    std::deque<FakeSink::Call> calls;
    uint32_t opened = 0;
    std::map<uint32_t, uint64_t> knownFor;  // the shards' view of this node ("fenced-known", "reacquired")
    DurableLog::Hooks hooks;
    hooks.knownShardEpoch = [&](const std::string&, uint32_t shard) {
      const auto found = knownFor.find(shard);
      return found == knownFor.end() ? uint64_t(0) : found->second;
    };
    hooks.openStream = [&](std::string, uint64_t ownerEpoch, uint64_t firstSeq)
        -> kj::Promise<kj::Own<DurableLog::PeerSink>> {
      ++opened;
      if (name == "unreachable") return kj::NEVER_DONE;
      if (!pair) return kj::Own<DurableLog::PeerSink>(kj::heap<FakeSink>(calls));
      uint64_t stream = 0;
      KJ_REQUIRE(peer.openIncoming("node", ownerEpoch, "instance", "5", firstSeq, stream).empty());
      return kj::Own<DurableLog::PeerSink>(kj::heap<LocalSink>(peer, stream));
    };
    DurableLog log({"node", "instance", temp, {}}, store, io.provider->getNetwork(), clock, *io.lowLevelProvider,
        membership, std::move(hooks));
    log.settings.config.peers = 1;
    log.setNodeEpoch(1).wait(io.waitScope);
    KJ_REQUIRE(!log.logAppendFailed);
    const int process = log.openChannel("d0");
    KJ_REQUIRE(process >= 0);
    auto& channel = log.channels.at("d0");
    std::vector<std::string> hosts;
    for (char c : {'3', '4', '5'}) {
      const auto id = std::string(64, c);
      const auto host = durableHostName(ns, id, 1);
      auto& object = log.objects[host];
      log.locate(object, ns, id, 1);
      object.objectName = host;
      object.state = DurableLog::ObjectState::LOCAL;
      object.shardEpoch = 1;
      object.shard = uint32_t(c - '0');  // three shards: a fence of one leaves the others
      std::filesystem::create_directories(object.positionPath(temp).parent_path());
      hosts.push_back(host);
    }
    if (name == "pair-fenced") peer.fenceOwner(ns, log.objects.at(hosts[1]).shard, 1, "test", false);
    const bool drop = name == "fenced-drop" || name == "fenced-unshown" || name == "full-keeps" ||
        name == "fenced-known" || name == "reacquired";
    const bool resend = name == "fenced-drop-resend";
    if (name == "fenced-drop" || name == "full-keeps" || resend) {
      // The store shows the next epoch of record 2's shard (4): another node has taken it.
      ShardEpoch next;
      next.namespaceId = ns; next.shard = 4; next.epoch = 2; next.nodeId = "node-3"; next.nodeEpoch = 7;
      next.dataEpoch = 1; next.reason = "timeout"; next.instance = "i"; next.nonce = "n"; next.acquiredAtUnixMs = 1;
      log.store->put(shardEpochKey(ns, 4, 2), shardEpochJson(next)).wait(io.waitScope);
    }
    for (size_t i = 0; i < hosts.size(); ++i) {
      LogRecord record;
      record.kind = LogRecordKind::RESET;
      record.objectName = hosts[i];
      record.storageId = log.objects.at(hosts[i]).storageId;
      record.dataEpoch = 1;
      if (i == 1 && name == "pair-full") record.facetName = std::string(40000, 'p');  // too large for the peer
      if (i == 1 && chunk) record.frameBytes = std::string(5u << 20, 'f');  // two pieces
      log.takeRecord(channel, std::move(record), 101 + i);
    }
    log.waitForLogWrites().wait(io.waitScope);
    io.waitScope.poll();
    std::map<uint64_t, uint8_t> acks;
    auto answers = [&]() {
      log.waitForLogWrites().wait(io.waitScope);
      io.waitScope.poll();
      char buffer[4096];
      std::string bytes;
      for (ssize_t n; (n = ::recv(process, buffer, sizeof buffer, MSG_DONTWAIT)) > 0;) bytes.append(buffer, size_t(n));
      for (size_t at = 0; at + 4 <= bytes.size();) {
        uint32_t size = 0;
        std::memcpy(&size, bytes.data() + at, 4);
        const auto* message = reinterpret_cast<const unsigned char*>(bytes.data() + at + 4);
        if (size == 10 && message[0] == 1) {
          uint64_t request = 0;
          std::memcpy(&request, message + 1, 8);
          acks[request] = message[9];
        }
        at += 4 + size;
      }
      return acks;
    };
    auto later = [&](int64_t ms) {
      clock.advanceTo(clock.now() + ms * kj::MILLISECONDS);
      answers();
    };
    auto write = [&](size_t i, uint64_t request) {
      LogRecord record;
      record.kind = LogRecordKind::RESET;
      record.objectName = hosts[i];
      record.storageId = log.objects.at(hosts[i]).storageId;
      record.dataEpoch = 1;
      log.takeRecord(channel, std::move(record), request);
      log.waitForLogWrites().wait(io.waitScope);
    };
    if (name == "unreachable") {
      log.waitForLogWrites().wait(io.waitScope);
      clock.advanceTo(KJ_ASSERT_NONNULL(clock.nextEvent()));
      answers();
      KJ_REQUIRE(opened == 1 && calls.empty() && acks.empty() && log.confirmations == 0 &&
          log.inFlight.size() == hosts.size(), "an unreachable selected peer must not allow local confirmation");
    } else if (resend) {
      // Records 1, 2, 3 (shards 3, 4, 5) are taken and confirmed; then a write to shard 4 is refused as fenced and
      // the store shows epoch 2 of it -- the shard is given up. When the session reopens, the given-up shard's
      // confirmed record (2) is not sent again; the others are, and a new write to shard 5 is confirmed as usual.
      for (uint64_t i = 0; i < 3; ++i) calls[i].fulfiller->fulfill({i + 1, ""});
      answers();
      KJ_REQUIRE(log.confirmations == 3);
      write(1, 104);
      answers();
      KJ_REQUIRE(calls.size() == 4 && calls[3].sequence == 4);
      calls[3].fulfiller->fulfill({3, "fenced: this node knows a newer epoch of the shard"});
      answers();
      answers();
      KJ_REQUIRE(log.ownerFenced(ns, 4, 1) && log.fencedShardsLost == 1 && acks.at(104) != 0);
      calls.clear();
      log.peers.at("peer").attemptedAtMs = 0;
      log.drivePeers();
      io.waitScope.poll();
      std::vector<uint64_t> again;
      for (const auto& call : calls) again.push_back(call.sequence);
      KJ_REQUIRE(opened == 2 && again == std::vector<uint64_t>({1, 3}), "a given-up shard's records are not sent again",
          again.size());
      for (auto& call : calls) call.fulfiller->fulfill({3, ""});
      calls.clear();
      write(2, 105);
      answers();
      KJ_REQUIRE(calls.size() == 1 && calls[0].sequence == 5);
      calls[0].fulfiller->fulfill({5, ""});
      answers();
      KJ_REQUIRE(acks.at(105) == 0 && log.confirmations == 4, "another shard's write is confirmed after the reopen");
    } else if (drop) {
      // "fenced-drop": a peer refuses record 2 as fenced and the store shows epoch 2 of its shard -- this node
      // gives the shard up at once: record 2 fails now (not at confirmTimeoutMs), a later write to that object is
      // refused at once, the other shards are untouched. "fenced-unshown": the store does not show it -- nothing is
      // given up, until a background read finds it. "full-keeps": a full buffer gives nothing up and reads nothing,
      // the next epoch shown or not. "fenced-known": this node's shards' view already has epoch 2 -- given up with no
      // read, epoch 1 only. "reacquired": this node holds the shard again at epoch 2 when the peer's refusal of
      // record 2 (epoch 1) comes back -- epoch 1 is fenced (record 2 fails), epoch 2 is not: the object, now at
      // epoch 2, takes writes.
      if (name == "fenced-known") knownFor[4] = 2;
      if (name == "reacquired") {
        log.objects.at(hosts[1]).shardEpoch = 2;
        knownFor[4] = 2;
      }
      calls[0].fulfiller->fulfill({1, ""});
      calls[1].fulfiller->fulfill({1, name == "full-keeps" ?
          "full: this node buffers durableObjects.peerBufferBytes for the owners it is a peer of" :
          "fenced: this node knows a newer epoch of the shard"});
      calls[2].fulfiller->fulfill({3, ""});
      answers();
      answers();
      KJ_REQUIRE(acks.at(101) == 0 && acks.at(103) == 0, "the other shards' records are confirmed");
      const bool lost = log.ownerFenced(ns, 4, 1);
      if (name == "fenced-drop") {
        KJ_REQUIRE(lost && !log.ownerFenced(ns, 4, 2) && log.fencedShardsLost == 1 && acks.contains(102) &&
            acks.at(102) != 0 && !log.inFlight.contains(2) &&
            log.objects.at(hosts[1]).state == DurableLog::ObjectState::GONE,
            "a shard the store shows lost is given up at once, epoch 1 only", log.fencedShardsLost);
        KJ_REQUIRE(!log.ownerFenced(ns, 3, 1) && !log.ownerFenced(ns, 5, 1), "only that shard is given up");
        for (size_t i : {1u, 2u}) {
          LogRecord record;
          record.kind = LogRecordKind::RESET;
          record.objectName = hosts[i];
          record.storageId = log.objects.at(hosts[i]).storageId;
          record.dataEpoch = 1;
          log.takeRecord(channel, std::move(record), 104 + i);
        }
        answers();
        KJ_REQUIRE(acks.contains(105) && acks.at(105) != 0 && !acks.contains(106) && log.inFlight.size() == 1,
            "a later write to the lost shard is refused at once; another shard's waits for its peer");
      } else if (name == "fenced-unshown") {
        KJ_REQUIRE(!lost && log.fencedShardChecks == 1 && log.fencedShardsLost == 0 && !acks.contains(102) &&
            log.inFlight.contains(2) && log.peerFenceUnverified == 1, "nothing is given up on an unshown epoch");
        // The store shows it later: the background read gives the shard up, counted and logged the same way.
        ShardEpoch next;
        next.namespaceId = ns; next.shard = 4; next.epoch = 2; next.nodeId = "node-3"; next.nodeEpoch = 7;
        next.dataEpoch = 1; next.reason = "timeout"; next.instance = "i"; next.nonce = "n"; next.acquiredAtUnixMs = 1;
        log.store->put(shardEpochKey(ns, 4, 2), shardEpochJson(next)).wait(io.waitScope);
        later(int64_t(DurableLog::OWNER_REREAD_MS) + 1);
        answers();
        KJ_REQUIRE(log.ownerFenced(ns, 4, 1) && log.fencedShardsLost == 1 && acks.at(102) != 0,
            "a background read that finds the epoch gives the shard up", log.fencedShardsLost);
      } else if (name == "fenced-known") {
        KJ_REQUIRE(lost && !log.ownerFenced(ns, 4, 2) && log.fencedShardsLost == 1 && log.peerFenceChecks == 0 &&
            acks.at(102) != 0, "a newer epoch this node already knows gives the shard up with no read, epoch 1 only",
            log.fencedShardsLost);
      } else if (name == "reacquired") {
        log.checkConfirmations();  // the epoch-1 record pending is excluded (what the next confirmation pass does)
        answers();
        KJ_REQUIRE(lost && !log.ownerFenced(ns, 4, 2) && log.fencedShardsLost == 1 && log.peerFenceChecks == 0 &&
            acks.at(102) != 0 && log.objects.at(hosts[1]).state == DurableLog::ObjectState::LOCAL,
            "only the refused epoch is fenced");
        write(1, 107);
        answers();
        KJ_REQUIRE(!acks.contains(107), "a write at the shard's new epoch is not refused");
      } else {
        KJ_REQUIRE(!lost && log.fencedShardChecks == 0 && log.peerFenceChecks == 0 && !acks.contains(102) &&
            log.inFlight.contains(2), "a full buffer gives nothing up");
      }
    } else if (name == "pair-full") {
      answers();
      KJ_REQUIRE(log.confirmations == 2 && acks.at(101) == 0 && acks.at(103) == 0 && !acks.contains(102) &&
          log.inFlight.contains(2), "record 2 refused (full) is not counted; 3 is not held up", log.confirmations);
      peer.settings.config.peerBufferBytes = 1u << 20;
      later(DurableLog::FULL_RETRY_FIRST_MS * 3 / 2 + 1);  // the first wait and its jitter
      KJ_REQUIRE(log.confirmations == 3 && acks.at(102) == 0 && log.fullRetriesSent == 1 && opened == 1,
          "record 2, sent again on the same session and taken, is confirmed", log.confirmations);
    } else if (name == "pair-fenced") {
      answers();
      KJ_REQUIRE(log.confirmations == 1 && acks.at(101) == 0 && !acks.contains(102) && !acks.contains(103),
          "the peer holds its answers back below the fenced record", log.confirmations);
      // The write of record 2 is given up (what confirmTimeoutMs does): isolated, dropped, the stream opened again.
      log.isolateObjectPending(log.objects.at(hosts[1]), "test: not confirmed in time");
      log.peers.at("peer").attemptedAtMs = 0;
      log.drivePeers();
      answers();
      KJ_REQUIRE(opened == 2 && acks.at(102) != 0 && acks.at(103) == 0 && log.confirmations == 2 && log.inFlight.empty(),
          "after record 2 is dropped, record 3 is confirmed", opened, log.confirmations);
    } else if (chunk) {
      KJ_REQUIRE(calls.size() == 4 && calls[1].sequence == 2 && calls[1].partial && !calls[2].partial &&
          calls[3].sequence == 3, "record 2 went in two pieces", calls.size());
      calls[0].fulfiller->fulfill({3, ""});
      calls[1].fulfiller->fulfill({3, ""});
      answers();
      KJ_REQUIRE(log.confirmations == 1 && !acks.contains(102), "the answer to a record's first piece does not hold "
          "the record", log.confirmations);
      calls[2].fulfiller->fulfill({3, "fenced: this node knows a newer epoch of the shard"});
      calls[3].fulfiller->fulfill({3, ""});
      answers();
      KJ_REQUIRE(log.confirmations == 2 && !acks.contains(102) && acks.at(103) == 0 && log.inFlight.contains(2));
    } else {
      const std::string reason = name == "fenced" ? "fenced: this node knows a newer epoch of the shard" :
          name == "full" ? "full: this node buffers durableObjects.peerBufferBytes for the owners it is a peer of" :
          name == "cut-short" ? "the records are cut short: a record is cut short" :
          name == "limit" ? "a record exceeds the recoverable RPC limit" :
          name == "corrupt" ? "a record does not add up: checksum" : "";
      KJ_REQUIRE(!reason.empty(), "unknown refused-owner scenario", scenario);
      KJ_REQUIRE(calls.size() == 3 && calls[0].sequence == 1 && calls[2].sequence == 3, "the backlog went to the peer",
          calls.size());
      calls[0].fulfiller->fulfill({3, ""});
      answers();
      KJ_REQUIRE(log.confirmations == 1 && acks.contains(101) && !acks.contains(102),
          "an answer to record 1 must not account for record 2, whose own answer has not come", log.confirmations);
      calls[1].fulfiller->fulfill({1, reason});
      calls[2].fulfiller->fulfill({3, ""});
      answers();
      KJ_REQUIRE(log.confirmations == 2 && acks.at(101) == 0 && acks.at(103) == 0 && !acks.contains(102) &&
          log.inFlight.contains(2), "a record the peer refused must never count as held there", scenario,
          log.confirmations);
      if (reconnect) {
        // The session is replaced (a broken connection): everything not sealed goes out again; the older peer
        // answers record 1 with its highest sequence before its refusal of 2 comes back.
        calls.clear();
        log.retirePeer(log.peers.at("peer"));
        log.peers.at("peer").attemptedAtMs = 0;
        log.drivePeers();
        io.waitScope.poll();
        KJ_REQUIRE(opened == 2 && calls.size() == 3 && calls[1].sequence == 2, calls.size());
        calls[0].fulfiller->fulfill({3, ""});
        answers();
        KJ_REQUIRE(log.confirmations == 2 && !acks.contains(102), "after a reconnect an old answer covers nothing new");
        calls[1].fulfiller->fulfill({3, reason});
        calls[2].fulfiller->fulfill({3, ""});
        answers();
        KJ_REQUIRE(log.confirmations == 2 && !acks.contains(102) && log.inFlight.contains(2),
            "a record refused again after a reconnect is not held", scenario);
      } else if (name == "full") {
        // Tried again on the same session after FULL_RETRY_FIRST_MS; taken this time.
        later(DurableLog::FULL_RETRY_FIRST_MS * 3 / 2 + 1);  // the first wait and its jitter
        KJ_REQUIRE(calls.size() == 4 && calls[3].sequence == 2 && log.fullRetriesSent == 1, calls.size());
        calls[3].fulfiller->fulfill({3, ""});
        answers();
        KJ_REQUIRE(log.confirmations == 3 && acks.at(102) == 0, "the record taken on its retry is confirmed");
      } else {
        later(10 * DurableLog::FULL_RETRY_FIRST_MS);
        KJ_REQUIRE(calls.size() == 3 && log.fullRetriesSent == 0, "only a full buffer is tried again");
      }
    }
    log.closeChannel("d0");
  }

  static void run(bool indexOnly) {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-channel-test-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    // Deliberately never advance this clock: the read yields after its first slice and stays
    // there until the same close callback the process pool uses when it reaps the peer.
    kj::TimerImpl clock(kj::origin<kj::TimePoint>());
    ObjectStoreConfig store;
    store.backend = "directory";
    store.directory = temp;
    store.endpoint = "unused";  // A nonempty endpoint enables the channel; the directory backend uses no network.
    MembershipView membership;
    DurableLog log({"node", "instance", temp, {}}, store, io.provider->getNetwork(), clock, *io.lowLevelProvider,
        membership, {});
    if (indexOnly) {
      const auto check = [&]() {
        decltype(log.unsealedByObject) expected;
        for (const auto& [sequence, record] : log.parsed) expected[record.objectName].insert(sequence);
        KJ_REQUIRE(bool(expected == log.unsealedByObject), "parsed records and object index differ");
      };
      const auto insert = [&](uint64_t sequence, const char* object) {
        LogRecord record;
        record.nodeSeq = sequence;
        record.objectName = object;
        log.rememberParsedRecord(record);
      };
      log.setNodeEpoch(1).wait(io.waitScope);
      insert(1, "object-a");
      insert(2, "object-b");
      insert(3, "object-a");
      check();
      // Seal commits and fenced-tail retirement both remove metadata through this path.
      log.forgetParsedRecord(1);
      log.forgetParsedRecord(1);
      check();
      KJ_REQUIRE(log.parsed.size() == 2 && log.unsealedByObject.at("object-a").contains(3));
      log.forgetParsedRecord(2);
      check();
      KJ_REQUIRE(!log.unsealedByObject.contains("object-b"));
      log.setNodeEpoch(2).wait(io.waitScope);
      check();
      KJ_REQUIRE(log.parsed.empty() && log.unsealedByObject.empty());
      return;
    }
    constexpr int COUNT = 256;
    std::vector<std::string> objects;
    std::string burst;
    const auto appendLe = [](std::string& out, uint64_t value, int bytes) {
      for (int i = 0; i < bytes; ++i) out.push_back(char(value >> (8 * i)));
    };
    const int peer = log.openChannel("d0-1", 1);
    KJ_REQUIRE(peer >= 0);
    auto& channel = log.channels.at("d0-1");
    channel.pid = 321;  // The child has completed the separately tested readiness handshake.
    for (int i = 0; i < COUNT; ++i) {
      auto hex = [](int i) { return i < 10 ? char('0' + i) : char('a' + i - 10); };
      auto ns = std::string(64, '1'), id = std::string(62, '2') + hex(i / 16) + hex(i % 16);
      const auto name = durableHostName(ns, id, 1);
      auto& object = log.objects[name];
      log.locate(object, ns, id, 1);
      std::filesystem::create_directories(object.positionPath(temp).parent_path());
      object.objectName = name;
      object.state = DurableLog::ObjectState::LOCAL;
      object.cacheOpen = true;
      object.openEpoch = 1;
      object.openGeneration = 2;
      auto& facet = object.facets[0];
      facet.open = true;
      facet.processId = channel.processId;
      facet.openEpoch = 1;
      facet.openGeneration = 2;
      const auto key = name + "/0";
      const uint64_t openId = i + 1;
      channel.openFacets.emplace(key, DurableLog::Channel::Open{{1, 2}, openId});
      std::string closed(1, '\x07');  // FROM_CLOSED, as the durable shipper writes it.
      appendLe(closed, openId, 8);
      appendLe(closed, key.size(), 2);
      closed += key;
      appendLe(closed, 1, 8);  // time of close
      appendLe(closed, 8192 + i, 8);
      appendLe(burst, closed.size(), 4);
      burst += closed;
      objects.push_back(name);
    }
    // A process can also leave a partial length-prefixed message; ignore rather than dispatch it.
    const auto completeBytes = burst.size();
    appendLe(burst, 25, 4);
    burst.push_back('\x07');
    // Queue the whole burst before polling so the first read can deliver all of it.
    int sendBuffer = 128 * 1024;
    KJ_REQUIRE(::setsockopt(peer, SOL_SOCKET, SO_SNDBUF, &sendBuffer, sizeof sendBuffer) == 0);
    KJ_REQUIRE(::send(peer, burst.data(), burst.size(), MSG_DONTWAIT) == static_cast<ssize_t>(burst.size()));
    ::close(peer);
    io.waitScope.poll();
    const auto handled = channel.messages;
    bool suspended = false;
    KJ_IF_SOME(wake, clock.nextEvent()) { suspended = wake == clock.now() + 1 * kj::MICROSECONDS; }
    KJ_REQUIRE(handled > 0 && handled <= 16 && suspended,
        "reader must be suspended between slices with the burst in memory", handled);
    const auto bytesLeft = channel.inbox.size();
    log.closeChannel("d0-1");
    KJ_REQUIRE(!log.channels.contains("d0-1"));
    for (int i = 0; i < COUNT; ++i) {
      const auto& object = log.objects.at(objects[i]);
      const auto& facet = object.facets.at(0);
      KJ_REQUIRE(facet.databaseBytes == uint64_t(8192 + i), "buffered close frame was lost", i);
      KJ_REQUIRE(!facet.open && !object.cacheOpen && object.state == DurableLog::ObjectState::LOCAL,
          "a confirmed close must not mark the object's files for repair", i);
    }
    KJ_REQUIRE(bytesLeft == burst.size() - (completeBytes / COUNT) * handled,
        "close drain would replay the processed prefix", handled, bytesLeft);
    KJ_IF_SOME(wake, clock.nextEvent()) {
      KJ_REQUIRE(wake > clock.now() + 1 * kj::MICROSECONDS, "close must cancel the suspended reader");
    }
  }

  static void threadedChannelEof() {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-threaded-eof-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    auto& timer = io.provider->getTimer();
    kj::HttpHeaderTable::Builder builder;
    auto ifNoneMatch = builder.add("If-None-Match");
    auto table = builder.build();
    HeldStore store(*table, ifNoneMatch);
    auto listener = io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
    kj::HttpServer server(timer, *table, store);
    auto listening = server.listenHttp(*listener).eagerlyEvaluate(nullptr);
    ObjectStoreConfig config;
    config.bucket = "b";
    config.endpoint = "http://127.0.0.1:" + std::to_string(listener->getPort());
    config.accessKey = "test-only";
    config.secretKey = "test-only";
    MembershipView membership;
    membership.heldEpochs.insert("1");
    size_t closeCalls = 0;
    DurableLog::Hooks hooks;
    hooks.closeObjects = [&](std::string) -> kj::Promise<void> { ++closeCalls; co_return; };
    DurableLog log({"node", "instance", temp, {}}, config, io.provider->getNetwork(), timer,
        *io.lowLevelProvider, membership, std::move(hooks));
    log.setNodeEpoch(1).wait(io.waitScope);

    const std::string ns(64, '1'), id(64, '2');
    store.objects[shardEpochKey(ns, 0, 1)] =
        shardEpochJson({ns, "node", "first", "instance", "nonce", "", 0, 1, 1, 1, 1});
    const auto name = durableHostName(ns, id, 1);
    auto& object = log.objects[name];
    log.locate(object, ns, id, 1);
    object.objectName = name;
    object.tenant = "tenant";
    object.state = DurableLog::ObjectState::LOCAL;
    object.shardEpoch = object.openEpoch = object.openGeneration = 1;

    const int peer = log.openChannel("d0-7", 7);
    KJ_REQUIRE(peer >= 0);
    auto& channel = log.channels.at("d0-7");
    object.dispatchedChannel = channel.processId;
    auto& facet = object.facets[0];
    facet.processId = channel.processId;
    facet.open = true;
    channel.openFacets[name + "/0"] = {{object.openEpoch, object.openGeneration}, 1};
    ::close(peer);
    timer.timeoutAfter(5 * kj::SECONDS, kj::mv(channel.reader)).wait(io.waitScope);

    KJ_REQUIRE(!channel.open && channel.openFacets.empty() && !facet.open,
        "EOF must close the facet registration");
    KJ_REQUIRE(object.state == DurableLog::ObjectState::REPAIR,
        "EOF with an open facet must mark the local image for repair");
    log.closeChannel("d0-7");  // The pool has reaped the old generation.
    timer.timeoutAfter(5 * kj::SECONDS, log.rebuild(name, "repair after threaded EOF", false)).wait(io.waitScope);
    KJ_REQUIRE(object.state == DurableLog::ObjectState::LOCAL && closeCalls == 0,
        "repair after the generation exits must preserve the other durable processes");
  }

  static void threadedChannel() {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-threaded-channel-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    kj::TimerImpl clock(kj::origin<kj::TimePoint>());
    ObjectStoreConfig store;
    store.backend = "directory"; store.directory = temp; store.endpoint = "unused";
    MembershipView membership;
    DurableLog log({"node", "instance", temp, {}}, store, io.provider->getNetwork(), clock,
        *io.lowLevelProvider, membership, {});
    log.setNodeEpoch(1).wait(io.waitScope);
    const int peer = log.openChannel("d0-7", 7);
    KJ_REQUIRE(peer >= 0);
    auto child = io.lowLevelProvider->wrapSocketFd(peer, kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);
    auto& channel = log.channels.at("d0-7");
    const auto integer = [](std::string& out, uint64_t value, int size) {
      for (int i = 0; i < size; ++i) out.push_back(char(value >> (i * 8)));
    };
    const auto text = [&](std::string& out, std::string_view value) {
      integer(out, value.size(), 2);
      out += value;
    };
    const auto receive = [&]() {
      auto& timer = io.provider->getTimer();
      kj::byte length[4];
      timer.timeoutAfter(kj::SECONDS, child->read(kj::arrayPtr(length, sizeof(length)))).wait(io.waitScope);
      uint32_t size = 0;
      for (unsigned i = 0; i < sizeof(length); ++i) size |= uint32_t(length[i]) << (8 * i);
      KJ_REQUIRE(size > 0 && size <= 1024, "invalid node WAL reply length", size);
      std::string reply(size, '\0');
      timer.timeoutAfter(kj::SECONDS,
          child->read(kj::arrayPtr(reinterpret_cast<kj::byte*>(reply.data()), reply.size()))).wait(io.waitScope);
      return reply;
    };
    const auto expectReply = [&](uint8_t type, uint64_t id, std::optional<uint8_t> status = {}) {
      std::string expected(1, char(type));
      integer(expected, id, 8);
      if (status) integer(expected, *status, 1);
      KJ_REQUIRE(receive() == expected, "unexpected node WAL reply", type, id);
    };
    const auto hello = [&](uint64_t generation) {
      std::string body(1, '\x01');
      integer(body, 321, 4);
      integer(body, 0, 8);
      integer(body, 1, 4);
      integer(body, generation, 8);
      return body;
    };
    KJ_IF_SOME(error, kj::runCatchingExceptions([&] { log.onMessage(channel, hello(8)); })) {
      KJ_REQUIRE(error.getDescription().contains("threaded WAL readiness mismatch"));
    } else {
      KJ_FAIL_REQUIRE("another generation negotiated this WAL channel");
    }
    KJ_REQUIRE(channel.pid == 0);
    log.onMessage(channel, hello(7));
    KJ_REQUIRE(channel.pid == 321);
    std::string ready(1, '\x05');
    integer(ready, 1, 4);
    integer(ready, 7, 8);
    KJ_REQUIRE(receive() == ready);

    const std::string ns(64, '1'), id(64, '2');
    const auto name = durableHostName(ns, id, 1);
    const auto key = name + "/0";
    auto& object = log.objects[name];
    log.locate(object, ns, id, 1);
    std::filesystem::create_directories(object.positionPath(temp).parent_path());
    object.objectName = name;
    object.state = DurableLog::ObjectState::LOCAL;
    object.openEpoch = 1;
    object.openGeneration = 2;
    object.dispatchedChannel = channel.processId;
    const auto forOpen = [&](uint8_t type, uint64_t openId) {
      std::string body(1, char(type));
      integer(body, openId, 8);
      text(body, key);
      return body;
    };
    const auto open = [&](uint64_t openId, uint32_t frames = 0) {
      auto body = forOpen(3, openId);
      text(body, name);
      text(body, object.storageId);
      integer(body, 0, 4); // root facet
      integer(body, 0, 4); // parent
      text(body, "");
      integer(body, 0, 4); // salts
      integer(body, 0, 4);
      integer(body, frames, 4);
      integer(body, 0, 8); // WAL bytes
      integer(body, 8192, 8); // database bytes
      log.onMessage(channel, body);
    };
    const auto close = [&](uint64_t openId, uint64_t bytes) {
      auto body = forOpen(7, openId);
      integer(body, 0, 8);
      integer(body, bytes, 8);
      log.onMessage(channel, body);
    };
    const auto checkpoint = [&](uint64_t openId, uint64_t bytes) {
      auto body = forOpen(4, openId);
      integer(body, 0, 8); // WAL generation
      for (int i = 0; i < 4; ++i) integer(body, 0, 4); // salts, frames and confirmed frames
      integer(body, bytes, 8);
      log.onMessage(channel, body);
    };
    open(11);
    expectReply(7, 11, 0);
    close(11, 8192);
    open(21);
    expectReply(7, 21, 0);
    std::string staleFrames(1, '\x02');
    integer(staleFrames, 11, 8); // old open ID
    integer(staleFrames, 101, 8); // request ID
    text(staleFrames, key);
    text(staleFrames, name);
    text(staleFrames, object.storageId);
    integer(staleFrames, 0, 4); // root facet
    integer(staleFrames, 0, 4); // parent
    text(staleFrames, "");
    integer(staleFrames, 1, 8); // WAL generation
    for (const auto value : {0, 0, 4096, 1, 1, 1}) integer(staleFrames, value, 4);
    for (unsigned i = 0; i < 3; ++i) integer(staleFrames, 0, 8); // timestamps
    staleFrames.append(1 + 24 + 4096, '\0'); // no WAL header, one frame
    log.onMessage(channel, staleFrames);
    expectReply(1, 101, 1);
    KJ_REQUIRE(channel.frames == 0 && log.nextSeq == 0, "a stale open cannot submit frames");
    checkpoint(21, 12288);
    checkpoint(11, 1);
    close(11, 2);
    KJ_REQUIRE(channel.openFacets.at(key).id == 21 && object.facets.at(0).open && object.cacheOpen);
    KJ_REQUIRE(object.facets.at(0).databaseBytes == 12288 && log.checkpoints == 1);
    KJ_REQUIRE(!log.drainChannel("d0-7") && channel.draining,
        "drain must wait for the current SQLite open");
    expectReply(6, 21);
    close(21, 16384);
    KJ_REQUIRE(!channel.openFacets.contains(key) && !object.facets.at(0).open && !object.cacheOpen);
    KJ_REQUIRE(object.facets.at(0).databaseBytes == 16384);
    KJ_REQUIRE(log.drainChannel("d0-7"), "the current database close completes the drain");

    channel.draining = false;
    const DurableLog::ObjectIdentity identity{ns, id, "tenant", 1, 1, 0};
    object.shardEpoch = 1;
    KJ_REQUIRE(log.checkObjectDispatch(identity, "d0", 7, "old-definition").ready);
    log.objectDispatched(identity, "d0", 7);
    open(25);
    expectReply(7, 25, 0);
    KJ_REQUIRE(log.checkObjectDispatch(identity, "d0", 7, "new-definition").code == "DO_VERSION_DRAINING");
    expectReply(6, 25);
    close(25, 16384);
    KJ_REQUIRE(log.checkObjectDispatch(identity, "d0", 7, "new-definition").code == "DO_VERSION_DRAINING",
        "the live call keeps its old definition even after SQLite close");
    log.objectRequestFinished(identity, "d0", 7);
    const auto stored = object.storageId;
    KJ_REQUIRE(log.checkObjectDispatch(identity, "d0", 7, "new-definition").ready &&
        object.threadedDefinition == "new-definition" && object.storageId == stored);
    clock.advanceTo(clock.now() + kj::MILLISECONDS);
    log.objectDispatched(identity, "d0", 7);
    open(24);
    expectReply(7, 24, 0);
    KJ_REQUIRE(log.checkObjectDispatch(identity, "d0", 7, "third-definition").code == "DO_VERSION_DRAINING");
    expectReply(6, 24);
    const auto forcedAt = clock.now() + 30 * kj::SECONDS;
    clock.advanceTo(forcedAt - kj::MILLISECONDS);
    {
      kj::byte unexpected;
      KJ_REQUIRE(!child->tryRead(&unexpected, 1, 1).poll(io.waitScope),
          "a completed drain must not close the replacement, and its own drain must wait for the deadline");
    }
    KJ_REQUIRE(log.checkObjectDispatch(identity, "d0", 7, "third-definition").code == "DO_VERSION_DRAINING");
    clock.advanceTo(forcedAt);
    expectReply(8, 24);
    KJ_REQUIRE(!log.checkObjectDispatch(identity, "d0", 7, "third-definition").generationMustClose,
        "an unacknowledged close can still finish within the confirmation interval");
    clock.advanceTo(forcedAt + 5 * kj::SECONDS);
    KJ_REQUIRE(log.checkObjectDispatch(identity, "d0", 7, "third-definition").generationMustClose,
        "lack of a close acknowledgement enables the generation fallback");
    close(24, 16384);
    const auto acknowledged = log.checkObjectDispatch(identity, "d0", 7, "third-definition");
    KJ_REQUIRE(acknowledged.code == "DO_VERSION_DRAINING" && !acknowledged.generationMustClose,
        "an acknowledged close does not replace the process while the last call unwinds");
    log.objectRequestFinished(identity, "d0", 7);
    KJ_REQUIRE(log.checkObjectDispatch(identity, "d0", 7, "third-definition").ready);
    log.objectDispatched(identity, "d0", 7);
    KJ_REQUIRE(log.checkObjectDispatch(identity, "d0", 7, "fourth-definition").code == "DO_VERSION_DRAINING");
    clock.advanceTo(clock.now() + 30 * kj::SECONDS);
    {
      kj::byte unexpected;
      KJ_REQUIRE(!child->tryRead(&unexpected, 1, 1).poll(io.waitScope),
          "the deadline has no open ID to address until the root registers");
    }
    open(28);
    expectReply(7, 28, 1);
    expectReply(8, 28);
    unsigned resetReports = 0;
    log.setActorResetHook([&](const DurableLog::ObjectIdentity& reset, std::string_view code) {
      KJ_REQUIRE(reset.namespaceId == ns && reset.objectId == id && reset.dataEpoch == 1);
      KJ_REQUIRE(code == "CPU_LIMIT_EXCEEDED");
      ++resetReports;
    });
    const auto reportReset = [&](uint64_t openId) {
      std::string body(1, char(11));
      integer(body, openId, 8);
      text(body, name);
      text(body, "CPU_LIMIT_EXCEEDED");
      log.onMessage(channel, body);
    };
    reportReset(24);
    reportReset(28);
    KJ_REQUIRE(resetReports == 1, "only the latest open can report an actor reset");
    close(28, 16384);
    log.objectRequestFinished(identity, "d0", 7);
    KJ_REQUIRE(log.checkObjectDispatch(identity, "d0", 7, "fourth-definition").ready);
    log.objectDispatched(identity, "d0", 7);
    bool namespaceActive = true;
    log.setNamespaceHook([&](const std::string&) { return namespaceActive; });
    open(26);
    expectReply(7, 26, 0);
    namespaceActive = false;
    KJ_REQUIRE(log.drainThreadedNamespaces({ns}).empty());
    expectReply(6, 26);
    close(26, 16384);
    log.objectRequestFinished(identity, "d0", 7);
    open(27);  // A descriptor sent just before the deletion reached the child late.
    expectReply(7, 27, 1);
    expectReply(6, 27);
    close(27, 16384);
    KJ_REQUIRE(log.drainThreadedNamespaces({ns}) == std::vector<std::string>{ns});
    namespaceActive = true;

    open(29, 1);
    std::string broken(1, '\x03');
    integer(broken, 29, 8);
    KJ_REQUIRE(receive().starts_with(broken), "the node must report the position mismatch to this open");
    expectReply(7, 29, 1);
    KJ_REQUIRE(object.state == DurableLog::ObjectState::REPAIR);
    close(29, 16384);

    object.state = DurableLog::ObjectState::LOCAL;
    open(30);
    expectReply(7, 30, 0);
    object.state = DurableLog::ObjectState::REBUILDING;
    object.holders[channel.processId].requests = 1;
    auto closing = log.closeThreadedObject(name);
    KJ_REQUIRE(!closing.poll(io.waitScope), "recovery must wait for SQLite to close");
    expectReply(6, 30);
    close(30, 16384);
    clock.advanceTo(clock.now() + 10 * kj::MILLISECONDS);
    KJ_REQUIRE(!closing.poll(io.waitScope), "a dispatched call still holds the old image");
    object.holders[channel.processId].requests = 0;
    clock.advanceTo(clock.now() + 10 * kj::MILLISECONDS);
    KJ_REQUIRE(closing.poll(io.waitScope) && closing.wait(io.waitScope));

    object.state = DurableLog::ObjectState::LOCAL;
    open(31);
    expectReply(7, 31, 0);
    object.state = DurableLog::ObjectState::REBUILDING;
    auto lost = log.closeThreadedObject(name);
    KJ_REQUIRE(!lost.poll(io.waitScope));
    expectReply(6, 31);
    // Deliver a whole message with a truncated checkpoint body before EOF.
    const auto incompleteCheckpoint = forOpen(4, 31);
    integer(channel.inbox, incompleteCheckpoint.size(), 4);
    channel.inbox += incompleteCheckpoint;
    const auto completedCheckpoints = log.checkpoints;
    const auto receivedMessages = channel.messages;
    log.channelEnded(channel, "test channel EOF");
    KJ_REQUIRE(channel.messages == receivedMessages + 1 && log.checkpoints == completedCheckpoints,
        "a truncated checkpoint report cannot advance the checkpoint state");
    clock.advanceTo(clock.now() + 10 * kj::MILLISECONDS);
    KJ_REQUIRE(lost.poll(io.waitScope) && !lost.wait(io.waitScope),
        "EOF without process exit is not proof of close");

    // Model a ready image and replacement generation. Even then, EOF cannot
    // authorize dispatch while the process pool still tracks the old owner.
    object.state = DurableLog::ObjectState::LOCAL;
    const int replacement = log.openChannel("d0-8", 8);
    KJ_REQUIRE(replacement >= 0);
    KJ_DEFER(::close(replacement));
    log.onMessage(log.channels.at("d0-8"), hello(8));
    KJ_REQUIRE(log.checkObjectDispatch(identity, "d0", 8).code == "DO_UNAVAILABLE");
    log.closeChannel("d0-7");  // Called by the process pool after reaping the generation.
    KJ_REQUIRE(log.checkObjectDispatch(identity, "d0", 8).ready,
        "generation exit can release the old image without a CLOSED record");
  }

  // The restored holder's reseal picks and builds its segments as it did on the event loop, byte for byte. The
  // reference is the 42af4392 selection, which encoded every record on the loop; it runs on a copy of what is left
  // before each segment, then selectEarlier (sizes computed, records copied in slices) and buildEarlierSegment run on
  // the log itself, and the segment's objects are retired the way a published manifest retires them.
  struct EarlierReference {
    SegmentHeader header;
    std::string body;
    std::set<std::string> touched;
    std::map<std::string, uint64_t> sourceEpochs;
  };
  static EarlierReference earlierReference(const std::map<uint64_t, LogRecord>& held, SegmentHeader header,
      uint64_t segmentBytes, const std::string& nodeId) {
    // 42af4392's constants and budgets (durable-log.c++): the store's read limit, the envelope and an index entry.
    constexpr uint64_t STORE_SEGMENT_BYTES = 64u << 20;
    const auto host = [](const LogRecord& record) { return record.objectName + "/" + std::to_string(record.dataEpoch); };
    EarlierReference out;
    std::vector<std::string> encoded;
    std::map<std::pair<std::string, uint32_t>, SegmentIndexEntry> index;
    uint64_t wireBytes = 1024 + json::quote(nodeId).size() + json::quote("earlier-epoch").size();
    for (const auto& [sequence, record] : held) {
      auto bytes = encodeLogRecord(record);
      const auto extra = bytes.size() + 4 +
          (index.contains({record.objectName, record.facetId}) ? 0 : 256 + json::quote(record.objectName).size());
      if (!encoded.empty() && (header.bytes + bytes.size() > segmentBytes || wireBytes + extra > STORE_SEGMENT_BYTES)) break;
      KJ_REQUIRE(wireBytes + extra <= STORE_SEGMENT_BYTES);
      wireBytes += extra;
      header.bytes += bytes.size();
      encoded.push_back(std::move(bytes));
      ++header.records;
      if (!header.firstSeq) header.firstSeq = sequence;
      header.lastSeq = sequence;
      auto& entry = index[{record.objectName, record.facetId}];
      entry.objectName = record.objectName;
      entry.facetId = record.facetId;
      if (!entry.records) entry.firstTxn = record.txn;
      entry.lastTxn = record.txn;
      ++entry.records;
      out.touched.insert(host(record));
      out.sourceEpochs[host(record)] = std::max(out.sourceEpochs[host(record)], record.shardEpoch);
    }
    for (auto& [key, entry] : index) header.objects.push_back(entry);
    out.body = encodeSegment(header, encoded);
    out.header = header;
    return out;
  }

  static void earlierSegment() {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-earlier-segment-test-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    kj::TimerImpl clock(kj::origin<kj::TimePoint>());
    ObjectStoreConfig store;
    store.backend = "directory"; store.directory = temp; store.endpoint = "unused";
    MembershipView membership;
    membership.heldEpochs.insert("9");
    const std::string ns(64, 'a');
    constexpr uint64_t EPOCH = 3;
    std::mt19937 random(58);
    auto text = [&](size_t size) {
      std::string out(size, '\0');
      for (auto& c : out) c = char('a' + random() % 26);
      return out;
    };
    uint32_t segmentsSeen = 0, yieldsSeen = 0;
    for (int round = 0; round < 8; ++round) {
      DurableLog log({"peer", "instance", temp + "/" + std::to_string(round), {}}, store, io.provider->getNetwork(), clock,
          *io.lowLevelProvider, membership, {});
      log.nodeEpoch = 9;
      const uint64_t sizes[] = {12u << 10, 96u << 10, 16u << 20};
      log.settings.config.segmentBytes = sizes[round % 3];
      log.earlierSliceBytes = round % 2 ? 1 : 32u << 10;
      std::map<uint64_t, LogRecord> model;
      const int count = 150 + int(random() % 250);
      uint64_t sequence = 1 + random() % 5;
      for (int i = 0; i < count; ++i) {
        LogRecord r;
        r.kind = random() % 5 == 0 ? LogRecordKind::RESET : LogRecordKind::FRAMES;
        r.nodeEpoch = EPOCH; r.nodeSeq = sequence; r.shardEpoch = 1 + random() % 2; r.shard = 0;
        char id[65];
        std::snprintf(id, sizeof(id), "%016llx%048x", (unsigned long long) (1 + random() % 24) * 1024, 0);
        r.objectName = ns + "/" + id;
        r.dataEpoch = 1 + random() % 2;
        r.storageId = text(random() % 20);
        r.facetId = random() % 3; r.parentFacetId = 0; r.facetName = text(random() % 12);
        r.txn = uint64_t(i + 1); r.generation = random() % 4; r.salt1 = random(); r.salt2 = random();
        r.pageSize = 4096; r.firstFrame = random() % 100; r.frames = 1 + random() % 3; r.commits = 1;
        if (random() % 3 == 0) r.header = text(32);
        if (r.kind == LogRecordKind::FRAMES) r.frameBytes = text(random() % 3 == 0 ? random() % 40000 : random() % 3000);
        KJ_REQUIRE(encodedLogRecordSize(r) == encodeLogRecord(r).size(), "the computed size is the encoded size",
            encodedLogRecordSize(r), encodeLogRecord(r).size());
        model[sequence] = r;
        sequence += 1 + random() % 3;
      }
      if (round % 4 == 3) {
        // A segment that ends exactly at segmentBytes: the record that fills it is kept.
        uint64_t sum = 0;
        const int fill = 2 + int(random() % 5);
        int taken = 0;
        for (const auto& [seq, record] : model) {
          if (taken++ == fill) break;
          sum += encodedLogRecordSize(record);
        }
        log.settings.config.segmentBytes = sum;
      }
      auto& held = log.earlier[EPOCH];
      held = model;
      auto& info = log.predecessorLogs[EPOCH];
      for (const auto& [seq, record] : held) {
        log.indexEarlierRecord({EPOCH, seq}, record);
        log.noteEarlierUnpublished(info, record);
      }
      SegmentHeader base;
      base.nodeId = "peer"; base.nodeEpoch = EPOCH; base.sealedAtUnixMs = 1234; base.reason = "earlier-epoch";
      for (uint32_t segment = 1;; ++segment) {
        auto found = log.earlier.find(EPOCH);
        if (found == log.earlier.end()) break;
        KJ_REQUIRE(segment < 10000, "the reseal makes progress");
        auto expectedHeader = base;
        expectedHeader.segment = segment;
        const auto expected = earlierReference(found->second, expectedHeader, log.settings.config.segmentBytes, "peer");
        auto promise = log.selectEarlier(EPOCH, std::nullopt);
        while (!promise.poll(io.waitScope)) { clock.advanceTo(clock.now() + 1 * kj::MICROSECONDS); ++yieldsSeen; }
        auto selection = promise.wait(io.waitScope);
        KJ_REQUIRE(!selection.aborted && !selection.input.records.empty(), "a segment is chosen", round, segment);
        auto header = base;
        header.segment = segment;
        auto built = buildEarlierSegment(header, std::move(selection.input));
        KJ_REQUIRE(built.body == expected.body, "the segment's bytes are the event loop's", round, segment,
            built.body.size(), expected.body.size());
        KJ_REQUIRE(built.sha256 == objectSha256(expected.body), "the digest is the event loop's", round, segment);
        KJ_REQUIRE(built.header.records == expected.header.records && built.header.bytes == expected.header.bytes &&
            built.header.firstSeq == expected.header.firstSeq && built.header.lastSeq == expected.header.lastSeq,
            "the segment's header is the event loop's", round, segment);
        KJ_REQUIRE(built.touched == expected.touched && built.sourceEpochs == expected.sourceEpochs,
            "the segment names the same objects", round, segment);
        ++segmentsSeen;
        // The manifests are written: every object of the segment gives up its records in the segment's range.
        for (const auto& host : built.touched)
          log.retireEarlierObject(EPOCH, host, built.header.firstSeq, built.header.lastSeq);
        std::erase_if(model, [&](const auto& entry) {
          return entry.first >= built.header.firstSeq && entry.first <= built.header.lastSeq;
        });
        const auto left = log.earlier.find(EPOCH);
        KJ_REQUIRE((left == log.earlier.end() ? model.empty() : left->second.size() == model.size()),
            "retiring takes exactly the segment's records", round, segment);
        uint64_t counted = 0;
        for (const auto& [name, records] : info.unpublishedObjects) counted += records;
        KJ_REQUIRE(counted == model.size(), "the per-object counts follow the retired records", counted, model.size());
        for (const auto& [name, positions] : log.earlierByObject)
          for (const auto& position : positions)
            KJ_REQUIRE(position.first != EPOCH || model.contains(position.second), "no index entry outlives its record");
      }
      KJ_REQUIRE(model.empty());
    }
    KJ_REQUIRE(segmentsSeen > 16 && yieldsSeen > 100, "several segments per round and slices that yield",
        segmentsSeen, yieldsSeen);
  }

  // A restored holder answers a tail request naming its object for that object. Random objects in two shards of
  // one earlier epoch, each published, partly published, being published (a manifest write in flight) or untouched,
  // some with an excluded record; every per-object and per-shard answer against the truth computed from what was
  // generated. With a deliberately wrong decision (shard-wide only, or a write in flight taken for done) some answer
  // must differ, or the test would not see that bug.
  static bool restoredTailCase(unsigned seed, DurableLog::RestoredTailControl control) {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-restored-tail-test-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    kj::TimerImpl clock(kj::origin<kj::TimePoint>());
    ObjectStoreConfig store;
    store.backend = "directory"; store.directory = temp; store.endpoint = "unused";
    MembershipView membership;
    membership.heldEpochs.insert("9");
    DurableLog log({"peer", "instance", temp, {}}, store, io.provider->getNetwork(), clock, *io.lowLevelProvider,
        membership, {});
    log.nodeEpoch = 9;
    log.diskTrusted = true;
    log.sameBootCache = true;
    log.restoredTailControlForTest = control;
    constexpr uint64_t EPOCH = 3;
    auto& info = log.predecessorLogs[EPOCH];
    info.complete = true;
    const std::string ns(64, 'b');
    std::mt19937 random(seed);
    enum State { UNTOUCHED, PUBLISHED, PARTIAL, PUBLISHING };
    struct Object { std::string id, name; uint32_t shard; State state; std::vector<uint64_t> sequences; bool excluded; };
    std::vector<Object> objects;
    uint64_t sequence = 1;
    const int count = 12 + int(random() % 20);
    for (int i = 0; i < count; ++i) {
      char id[65];
      std::snprintf(id, sizeof(id), "%016llx%048x", (unsigned long long) (i + 1) * 1024, i);
      Object object{id, ns + "/" + id, uint32_t(i % 2), State(random() % 4), {}, random() % 6 == 0};
      // At least one published object in each shard beside unpublished ones, one write in flight, and one object
      // whose records are all named but for an excluded one.
      if (i < 2) object.state = PUBLISHED;
      if (i == 2 || i == 3) object.state = UNTOUCHED;
      if (i == 4) object.state = PUBLISHING;
      if (i == 5) { object.state = PUBLISHED; object.excluded = true; }
      if (i == 6) object.state = PUBLISHED;  // its data epoch 1 only: data epoch 2 below stays
      objects.push_back(object);
    }
    std::map<uint64_t, std::pair<size_t, LogRecord>> excludedRecords;
    // Interleaved records, as the fixture's writes are.
    for (int round = 0; round < 6; ++round) {
      for (size_t i = 0; i < objects.size(); ++i) {
        if (random() % 3 == 0) continue;
        LogRecord r;
        r.kind = LogRecordKind::RESET; r.nodeEpoch = EPOCH; r.nodeSeq = sequence; r.shard = objects[i].shard;
        r.shardEpoch = 1; r.objectName = objects[i].name; r.dataEpoch = 1; r.txn = sequence;
        log.earlier[EPOCH][sequence] = r;
        log.indexEarlierRecord({EPOCH, sequence}, r);
        log.noteEarlierUnpublished(info, r);
        objects[i].sequences.push_back(sequence++);
      }
    }
    {
      // Object 6 has records of a second data epoch too (the same object name, another host name).
      LogRecord r;
      r.kind = LogRecordKind::RESET; r.nodeEpoch = EPOCH; r.nodeSeq = sequence; r.shard = objects[6].shard;
      r.shardEpoch = 1; r.objectName = objects[6].name; r.dataEpoch = 2; r.txn = sequence;
      log.earlier[EPOCH][sequence] = r;
      log.indexEarlierRecord({EPOCH, sequence}, r);
      log.noteEarlierUnpublished(info, r);
      objects[6].sequences.push_back(sequence++);
    }
    for (size_t i = 0; i < objects.size(); ++i) {
      if (objects[i].sequences.empty()) {  // every object has a record
        LogRecord r;
        r.kind = LogRecordKind::RESET; r.nodeEpoch = EPOCH; r.nodeSeq = sequence; r.shard = objects[i].shard;
        r.shardEpoch = 1; r.objectName = objects[i].name; r.dataEpoch = 1; r.txn = sequence;
        log.earlier[EPOCH][sequence] = r;
        log.indexEarlierRecord({EPOCH, sequence}, r);
        log.noteEarlierUnpublished(info, r);
        objects[i].sequences.push_back(sequence++);
      }
      if (objects[i].excluded) {
        LogRecord r;
        r.kind = LogRecordKind::RESET; r.nodeEpoch = EPOCH; r.nodeSeq = sequence; r.shard = objects[i].shard;
        r.shardEpoch = 1; r.objectName = objects[i].name; r.dataEpoch = 1; r.txn = sequence;
        log.earlierExcluded[EPOCH][sequence] = r;
        log.noteEarlierUnpublished(info, r);
        excludedRecords[sequence++] = {i, r};
      }
    }
    std::vector<bool> truth(objects.size()), excludedLeft(objects.size());
    for (size_t i = 0; i < objects.size(); ++i) {
      auto& object = objects[i];
      const auto host = object.name + "/1";
      if (object.state == PUBLISHED || object.state == PARTIAL) {
        const auto upTo = object.state == PUBLISHED ? object.sequences.back()
            : object.sequences[object.sequences.size() / 2] - 1;
        log.retireEarlierObject(EPOCH, host, 0, upTo);
        if (object.state == PARTIAL && upTo < object.sequences.front()) object.state = UNTOUCHED;
      }
      if (object.state == PUBLISHED && object.excluded && (i == 5 || random() % 2 == 0)) excludedLeft[i] = true;
      else if (object.state == PUBLISHED && object.excluded) {
        for (auto it = log.earlierExcluded[EPOCH].begin(); it != log.earlierExcluded[EPOCH].end();) {
          if (it->second.objectName == object.name) { log.noteEarlierRetired(EPOCH, it->second); it = log.earlierExcluded[EPOCH].erase(it); }
          else ++it;
        }
      }
      if (object.state == PUBLISHING) log.earlierPublishing.insert(object.name);
      truth[i] = object.state != PUBLISHED || excludedLeft[i] || i == 6;
    }
    auto ask = [&](uint32_t shard, const std::string& id) {
      DurableTailAsk out;
      out.namespaceId = ns; out.shard = shard; out.shardEpoch = 2; out.ownerNodeId = "peer"; out.ownerNodeEpoch = EPOCH;
      out.objectId = id;
      return log.tailFor(out, false);
    };
    auto pending = [](const DurableTailAnswer& answer) {
      if (answer.refusal == "the restored owner shard still has unpublished records") return true;
      KJ_REQUIRE(answer.refusal.empty() && answer.ownerConfirmed && answer.records.empty(),
          "an answer is either pending or an owner's empty tail", answer.refusal.c_str());
      return false;
    };
    bool matched = true;
    for (size_t i = 0; i < objects.size(); ++i)
      if (pending(ask(objects[i].shard, objects[i].id)) != truth[i]) matched = false;
    for (uint32_t shard = 0; shard < 2; ++shard) {
      bool any = false;
      for (size_t i = 0; i < objects.size(); ++i) any = any || (objects[i].shard == shard && truth[i]);
      if (pending(ask(shard, "")) != any) matched = false;
    }
    // A shard count the per-object counts do not account for (durable-tail-equivalence makes one): requests
    // naming an object are answered for the shard again.
    ++info.unpublishedShards[{ns, 0}];
    for (size_t i = 0; i < objects.size(); ++i)
      if (objects[i].shard == 0 && !pending(ask(0, objects[i].id))) matched = false;
    return matched;
  }


  // An object's records leave its earlier epoch only after the manifest that names them is written. A stand-in
  // for S3 (listing, reads, create-if-absent) holds or fails the manifest create of one object: while it is held a
  // request naming that object is still told there is more, and after a failed create the records stay.
  class HeldStore final: public kj::HttpService {
  public:
    HeldStore(const kj::HttpHeaderTable& table, kj::HttpHeaderId ifNoneMatch): table(table), ifNoneMatch(ifNoneMatch) {}
    kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr url, const kj::HttpHeaders& headers,
        kj::AsyncInputStream& body, Response& response) override {
      const std::string target(url.cStr());
      const auto question = target.find('?');
      const auto path = decode(target.substr(0, question));
      const auto query = question == std::string::npos ? std::string() : target.substr(question + 1);
      const auto key = path.size() > 3 ? path.substr(3) : std::string();  // "/b/<key>"
      if (method == kj::HttpMethod::GET && query.find("list-type=2") != std::string::npos) {
        const auto prefix = decode(parameter(query, "prefix"));
        if (auto gate = reads.find(prefix); gate != reads.end()) {
          heldReads.insert(prefix);
          co_await gate->second.addBranch();
        }
        std::string xml = "<ListBucketResult><IsTruncated>false</IsTruncated>";
        for (const auto& [name, bytes] : objects) {
          if (!name.starts_with(prefix)) continue;
          xml += "<Contents><Key>" + name + "</Key><LastModified>2026-11-20T00:00:00.000Z</LastModified><Size>" +
              std::to_string(bytes.size()) + "</Size></Contents>";
        }
        co_await send(response, 200, xml + "</ListBucketResult>");
        co_return;
      }
      if (method == kj::HttpMethod::GET) {
        if (auto gate = reads.find(key); gate != reads.end()) {
          heldReads.insert(key);
          co_await gate->second.addBranch();
        }
        if (unreadable.contains(key)) {
          co_await send(response, 503, "");
          co_return;
        }
        auto found = objects.find(key);
        co_await send(response, found == objects.end() ? 404 : 200, found == objects.end() ? std::string() : found->second);
        co_return;
      }
      auto bytes = co_await body.readAllBytes();
      std::string text(bytes.asChars().begin(), bytes.size());
      if (!hold.empty() && key.find(hold) != std::string::npos) {
        ++held;
        if (parked) parked->fulfill();
        co_await release.addBranch();
      }
      if (!fail.empty() && key.find(fail) != std::string::npos) {
        ++failed;
        co_await send(response, 503, "");
        co_return;
      }
      if (headers.get(ifNoneMatch) != kj::none && objects.contains(key)) {
        co_await send(response, 412, "");
        co_return;
      }
      objects[key] = std::move(text);
      ++written;
      co_await send(response, 200, "");
    }
    std::map<std::string, std::string> objects;
    std::set<std::string> unreadable;
    std::map<std::string, kj::ForkedPromise<void>> reads;
    std::set<std::string> heldReads;
    std::string hold, fail;
    bool sendStoreTimes = false;
    uint32_t held = 0, failed = 0, written = 0;
    kj::Own<kj::PromiseFulfiller<void>> parked;
    kj::PromiseFulfillerPair<void> gate = kj::newPromiseAndFulfiller<void>();
    kj::ForkedPromise<void> release = gate.promise.fork();
  private:
    static std::string decode(const std::string& text) {
      std::string out;
      for (size_t i = 0; i < text.size(); ++i) {
        if (text[i] == '%' && i + 2 < text.size()) { out.push_back(char(std::stoi(text.substr(i + 1, 2), nullptr, 16))); i += 2; }
        else out.push_back(text[i]);
      }
      return out;
    }
    static std::string parameter(const std::string& query, const std::string& name) {
      for (size_t at = 0; at <= query.size();) {
        const auto end = std::min(query.find('&', at), query.size());
        const auto pair = query.substr(at, end - at);
        if (pair.starts_with(name + "=")) return pair.substr(name.size() + 1);
        at = end + 1;
      }
      return {};
    }
    kj::Promise<void> send(Response& response, uint32_t status, std::string text) {
      kj::HttpHeaders out(table);
      if (sendStoreTimes) {
        out.add("Date", "Fri, 20 Nov 2026 00:00:10 GMT");
        out.add("Last-Modified", "Fri, 20 Nov 2026 00:00:00 GMT");
      }
      auto stream = response.send(status, status == 200 ? "OK" : "Error", out, uint64_t(text.size()));
      if (!text.empty()) co_await stream->write(kj::arrayPtr(text.data(), text.size()).asBytes());
    }
    const kj::HttpHeaderTable& table;
    kj::HttpHeaderId ifNoneMatch;
  };

  static void recoveryRequeue() {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-recovery-queue-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    auto& timer = io.provider->getTimer();
    kj::HttpHeaderTable::Builder builder;
    auto ifNoneMatch = builder.add("If-None-Match");
    auto table = builder.build();
    HeldStore store(*table, ifNoneMatch);
    auto listener = io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
    kj::HttpServer server(timer, *table, store);
    auto listening = server.listenHttp(*listener).eagerlyEvaluate(nullptr);
    ObjectStoreConfig config;
    config.bucket = "b";
    config.endpoint = "http://127.0.0.1:" + std::to_string(listener->getPort());
    config.accessKey = "test-only";
    config.secretKey = "test-only";
    MembershipView membership;
    membership.heldEpochs.insert("9");
    const std::string ns(64, 'a');
    store.objects[shardEpochKey(ns, 1, 2)] = shardEpochJson(
        {ns, "new", "timeout", "new-instance", "nonce", "old", 1, 2, 9, 2, 1, 1, 7});
    store.objects[shardEpochKey(ns, 1, 1)] = shardEpochJson(
        {ns, "old", "first", "old-instance", "nonce", "", 1, 1, 7, 1, 1});
    store.objects[memberRegistrationKey("old", 7)] = memberRecordJson({"old", "old:8934", "active", "old-instance", 7});
    auto owner = kj::newPromiseAndFulfiller<void>();
    auto ownerReply = owner.promise.fork();
    unsigned ownerCalls = 0;
    DurableLog::Hooks hooks;
    hooks.fetchTail = [&](std::string node, DurableTailAsk) -> kj::Promise<DurableTailAnswer> {
      KJ_REQUIRE(node == "old");
      ++ownerCalls;
      co_await ownerReply.addBranch();
      co_return DurableTailAnswer{.ownerConfirmed = true};
    };
    DurableLog::Settings settings{"new", "new-instance", temp, {}};
    settings.config.rebuildConcurrency = 1;
    DurableLog log(settings, config, io.provider->getNetwork(), timer, *io.lowLevelProvider, membership, std::move(hooks));
    log.nodeEpoch = 9;
    std::filesystem::create_directories(log.logDirectory());
    KJ_REQUIRE(log.initializeRecovery());

    auto until = [&](const char* phase, auto predicate) {
      auto end = timer.now() + 4 * kj::SECONDS;
      while (!predicate() && timer.now() < end) timer.afterDelay(5 * kj::MILLISECONDS).wait(io.waitScope);
      KJ_REQUIRE(predicate(), "rebuild did not reach the expected phase", phase, ownerCalls, log.json().c_str());
    };
    auto id = [&](char digit) { return DurableLog::ObjectIdentity{ns, std::string(64, digit), "", 2, 2, 1}; };
    auto name = [&](char digit) { return durableHostName(ns, id(digit).objectId, 2); };
    auto open = [&](char digit, bool background) {
      return log.openObject(id(digit), false, background, false).wait(io.waitScope);
    };
    auto holdManifest = [&](char digit) {
      const auto prefix = objectManifestPrefix(ns, id(digit).objectId) + "/";
      auto gate = kj::newPromiseAndFulfiller<void>();
      store.reads.emplace(prefix, gate.promise.fork());
      return std::pair(prefix, kj::mv(gate.fulfiller));
    };
    auto cancel = [&](char digit) { log.rebuildCancelers.at(name(digit))->cancel("fixture cancellation"); };
    auto check = [&](uint32_t active, uint32_t background) {
      KJ_REQUIRE(log.activeRebuilds == active && log.activeBackgroundRebuilds == background &&
          background <= active && active <= settings.config.rebuildConcurrency);
    };

    // A new shard reads its first epoch under the preparation slot even
    // while both waiting-recovery metadata permits are occupied.
    const std::string newNs(64, 'b');
    const auto newEpoch = shardEpochKey(newNs, 0, 1);
    store.objects[newEpoch] = shardEpochJson(
        {newNs, "new", "first", "new-instance", "nonce", "", 0, 1, 9, 1, 1});
    {
      auto busyOne = log.recoveryMetadata.acquire().wait(io.waitScope);
      auto busyTwo = log.recoveryMetadata.acquire().wait(io.waitScope);
      auto heldEpoch = kj::newPromiseAndFulfiller<void>();
      store.reads.emplace(newEpoch, heldEpoch.promise.fork());
      auto firstAccess = log.openObject({newNs, std::string(64, '1'), "", 1, 1, 0}, false)
          .eagerlyEvaluate(nullptr);
      until("first epoch read", [&] { return store.heldReads.contains(newEpoch); });
      check(1, 0);
      heldEpoch.fulfiller->fulfill();
      KJ_REQUIRE(firstAccess.wait(io.waitScope).ready && log.recoveryMetadata.started() == 2 &&
          log.waitingRecoveryPublication.started() == 0);
      check(0, 0);
    }

    auto first = holdManifest('1');
    KJ_REQUIRE(open('1', true).code == "DO_ALARM_REBUILDING");
    until("initial manifest read", [&] { return store.heldReads.contains(first.first); });
    check(1, 1);
    KJ_REQUIRE(open('2', true).code == "DO_ALARM_REBUILDING");
    KJ_REQUIRE(log.queuedRebuildIndex.contains(name('2')));
    cancel('2');
    check(1, 1);
    KJ_REQUIRE(!log.queuedRebuildIndex.contains(name('2')));
    first.second->fulfill();
    until("tail wait", [&] { return ownerCalls == 1 && log.rebuildRuns.at(name('1'))->parkedAtMs != 0; });
    check(0, 0);
    KJ_REQUIRE(log.objectOf(name('1'))->state == DurableLog::ObjectState::REBUILDING);
    KJ_REQUIRE(open('1', false).code == "DO_ALARM_REBUILDING");
    KJ_REQUIRE(!log.rebuildRuns.at(name('1'))->background && log.backgroundRebuildMetadataBytes == 0);

    auto blocker = holdManifest('3');
    KJ_REQUIRE(open('3', false).code == "DO_ALARM_REBUILDING");
    until("held foreground preparation", [&] { return store.heldReads.contains(blocker.first); });
    check(1, 0);
    owner.fulfiller->fulfill();
    until("replay admission", [&] { return log.queuedRebuildIndex.contains(name('1')); });
    KJ_REQUIRE(log.rebuildRuns.at(name('1'))->phase == DurableLog::RebuildPhase::RESUMING);
    cancel('3');
    check(1, 0);  // The waking rebuild owns the slot before its coroutine resumes.
    cancel('1');
    check(0, 0);
    KJ_REQUIRE(log.rebuildRuns.empty() && log.queuedRebuildIndex.empty() && log.rebuildMetadataBytes == 0);

    owner = kj::newPromiseAndFulfiller<void>();
    ownerReply = owner.promise.fork();
    KJ_REQUIRE(open('4', true).code == "DO_ALARM_REBUILDING");
    until("second tail wait", [&] { return ownerCalls == 2; });
    check(0, 0);
    cancel('4');
    check(0, 0);
    KJ_REQUIRE(log.rebuildMetadataBytes == 0 && log.backgroundRebuildMetadataBytes == 0);

    // A returning owner can release a waiting recovery with a complete proof.
    // Its publication takes the same permit as a bounded cut.
    const auto publications = log.waitingRecoveryPublication.started();
    auto returning = log.openObject(id('b'), false).eagerlyEvaluate(nullptr);
    until("waiting for the returning owner", [&] { return ownerCalls == 3 && log.rebuildRuns.at(name('b'))->parkedAtMs != 0; });
    owner.fulfiller->fulfill();
    KJ_REQUIRE(returning.wait(io.waitScope).ready && log.waitingRecoveryPublication.started() == publications + 1);
    check(0, 0);

    // Hold/cancel a real preparation after filling the background share with
    // queued jobs. A request can still enter the foreground queue.
    auto budgetBlocker = holdManifest('5');
    log.rebuildMetadataLimit = 4 * DurableLog::REBUILD_ENTRY_BYTES;
    for (char digit : {'5', '6', '7'}) KJ_REQUIRE(open(digit, true).code == "DO_ALARM_REBUILDING");
    KJ_REQUIRE(open('8', true).code == "DO_RECOVERING");
    KJ_REQUIRE(open('9', false).code == "DO_ALARM_REBUILDING" && log.queuedRebuildIndex.contains(name('9')));
    for (char digit : {'9', '7', '6', '5'}) cancel(digit);
    check(0, 0);

    // Let entry admission succeed and exhaust the remaining bytes inside
    // fetchTails. The real open must surface a retryable recovery refusal.
    log.rebuildMetadataLimit = DurableLog::REBUILD_ENTRY_BYTES + 32;
    const auto refused = log.openObject(id('a'), false).wait(io.waitScope);
    KJ_REQUIRE(!refused.ready && refused.status == 503 && refused.code == "DO_RECOVERING" &&
        log.rebuildMetadataRefusals == 2 && log.rebuildMetadataBytes == 0);

    const auto memberKey = memberRegistrationKey("old", 7);
    store.unreadable.insert(memberKey);
    log.recoveryDiscoveryAtMs = 0;
    const auto discovery = log.tailTargets().wait(io.waitScope);
    KJ_REQUIRE(discovery->addresses.contains("old/7@old:8934") && discovery->unresolved.contains(memberKey),
        "a failed later discovery preserves a previously known holder");

    // A first epoch that names a remote owner needs that owner's proof.
    // The rebuild parks and its discovery waits for a permit.
    const std::string foreignNs(64, 'c'), foreignId(64, '1');
    const auto foreignName = durableHostName(foreignNs, foreignId, 1);
    store.objects[shardEpochKey(foreignNs, 0, 1)] = shardEpochJson(
        {foreignNs, "old", "first", "old-instance", "nonce", "", 0, 1, 7, 1, 1});
    log.rebuildMetadataLimit = DurableLog::REBUILD_METADATA_LIMIT;
    log.recoveryDiscoveryAtMs = 0;
    auto busyOne = log.recoveryMetadata.acquire().wait(io.waitScope);
    auto busyTwo = log.recoveryMetadata.acquire().wait(io.waitScope);
    const auto metadataStarts = log.recoveryMetadata.started();
    auto heldDiscovery = kj::newPromiseAndFulfiller<void>();
    store.reads.emplace(MEMBERS_PREFIX, heldDiscovery.promise.fork());
    auto foreignAccess = log.openObject({foreignNs, foreignId, "", 1, 1, 0}, false).eagerlyEvaluate(nullptr);
    until("foreign first epoch parks", [&] {
      auto run = log.rebuildRuns.find(foreignName);
      return run != log.rebuildRuns.end() && run->second->parkedAtMs != 0;
    });
    check(0, 0);
    KJ_REQUIRE(!foreignAccess.poll(io.waitScope) && !store.heldReads.contains(MEMBERS_PREFIX) &&
        log.recoveryMetadata.started() == metadataStarts);
    busyOne = nullptr;
    until("foreign first epoch discovery", [&] { return store.heldReads.contains(MEMBERS_PREFIX); });
    KJ_REQUIRE(log.recoveryMetadata.started() == metadataStarts + 1 && !foreignAccess.poll(io.waitScope));
    check(0, 0);
    log.rebuildCancelers.at(foreignName)->cancel("fixture cancellation");
    heldDiscovery.fulfiller->fulfill();
    KJ_REQUIRE(!foreignAccess.wait(io.waitScope).ready);
    busyTwo = nullptr;

    // Exercise a real acquisition and reuse only the authority it read from
    // the store for that shard and ownership epoch.
    membership.epoch = "9";
    membership.conditionalPutAtomic = "true";
    DurableShards shards({"new"}, config, io.provider->getNetwork(), timer, membership, [] { return false; });
    const std::string reusedNs(64, 'd');
    const auto predecessorKey = shardEpochKey(reusedNs, 0, 1);
    const auto predecessor = shardEpochJson(
        {reusedNs, "old", "first", "old-instance", "nonce", "", 0, 1, 7, 1, 1});
    store.objects[predecessorKey] = predecessor;
    store.objects[shardReleasedKey(reusedNs, 0, 1)] = "";
    store.sendStoreTimes = true;
    const auto acquired = shards.accept(reusedNs, 0, 0, "peer").wait(io.waitScope);
    KJ_REQUIRE(acquired.kind == ShardRoute::Kind::LOCAL && acquired.epoch == 2);
    KJ_REQUIRE(!shards.heldPredecessor(reusedNs, 0, 2, 2) && !shards.heldPredecessor(reusedNs, 0, 3, 1) &&
        !shards.heldPredecessor(reusedNs, 1, 2, 1));
    log.setPredecessorEpochHook([&](const std::string& ns, uint32_t shard, uint64_t held, uint64_t source) {
      return shards.heldPredecessor(ns, shard, held, source);
    });
    const DurableLog::ObjectIdentity reused{reusedNs, std::string(64, '1'), "", 2, 2, 0};
    store.unreadable.insert(predecessorKey);
    const auto started = log.recoveryMetadata.started();
    const auto response = log.readSourceEpoch(reused, 1, false).wait(io.waitScope);
    KJ_REQUIRE(response->bytes == predecessor && response->lastModifiedUnixMs > 0 &&
        log.recoveryMetadata.started() == started && log.reusedPredecessorEpochs == 1);
    shards.dropAll("fixture");
    KJ_REQUIRE(!shards.heldPredecessor(reusedNs, 0, 2, 1));
  }

  static LogRecord recoveryPage(const std::string& name, const std::string& page) {
    auto record = testRecord(name.substr(0, 64), 7, 1, 1, 1);
    record.objectName = name;
    record.kind = LogRecordKind::FRAMES;
    record.pageSize = page.size();
    record.frames = record.firstFrame = record.commits = record.generation = 1;
    auto setWord = [](std::string& bytes, size_t at, uint32_t word) {
      for (unsigned i = 0; i < 4; ++i) bytes[at + i] = char(word >> (24 - 8 * i));
    };
    auto word = [](const std::string& bytes, size_t at) {
      uint32_t value = 0;
      for (unsigned i = 0; i < 4; ++i) value = (value << 8) | uint8_t(bytes[at + i]);
      return value;
    };
    uint32_t first = 0, second = 0;
    auto checksum = [&](const std::string& bytes, size_t at, size_t end) {
      for (; at < end; at += 8) {
        first += word(bytes, at) + second;
        second += word(bytes, at + 4) + first;
      }
    };
    record.header.resize(32);
    setWord(record.header, 0, 0x377f0683);
    setWord(record.header, 4, 3007000);
    setWord(record.header, 8, page.size());
    checksum(record.header, 0, 24);
    setWord(record.header, 24, first);
    setWord(record.header, 28, second);
    record.frameBytes.resize(24);
    setWord(record.frameBytes, 0, 1);  // page one
    setWord(record.frameBytes, 4, 1);  // one page at commit
    checksum(record.frameBytes, 0, 8);
    checksum(page, 0, page.size());
    setWord(record.frameBytes, 16, first);
    setWord(record.frameBytes, 20, second);
    record.frameBytes += page;
    return record;
  }

  static void recoveryFinalPerObject() {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-recovery-page-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    auto& timer = io.provider->getTimer();
    kj::HttpHeaderTable::Builder builder;
    auto ifNoneMatch = builder.add("If-None-Match");
    auto table = builder.build();
    HeldStore store(*table, ifNoneMatch);
    auto listener = io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
    kj::HttpServer server(timer, *table, store);
    auto listening = server.listenHttp(*listener).eagerlyEvaluate(nullptr);
    ObjectStoreConfig config;
    config.bucket = "b";
    config.endpoint = "http://127.0.0.1:" + std::to_string(listener->getPort());
    config.accessKey = "test-only";
    config.secretKey = "test-only";
    MembershipView membership;
    membership.heldEpochs.insert("9");
    const std::string ns(64, 'c'), slow(64, '1'), healthy(64, '2'), page(512, 'v');
    store.objects[shardEpochKey(ns, 1, 2)] = shardEpochJson(
        {ns, "new", "timeout", "new-instance", "nonce", "old", 1, 2, 9, 2, 1, 1, 7});
    store.objects[shardEpochKey(ns, 1, 1)] = shardEpochJson(
        {ns, "old", "first", "old-instance", "nonce", "", 1, 1, 7, 1, 1});
    store.objects[memberRegistrationKey("old", 7)] = memberRecordJson({"old", "old:8934", "active", "old-instance", 7});
    store.objects[memberRegistrationKey("peer", 8)] = memberRecordJson({"peer", "peer:8934", "active", "peer-instance", 8});
    auto started = kj::newPromiseAndFulfiller<void>();
    auto bothBorrowed = kj::newPromiseAndFulfiller<void>();
    bool failed = false;
    unsigned firstBorrows = 0, replacements = 0, healthyPages = 0;
    std::shared_ptr<DurableTailConnection> first;
    DurableLog::Hooks hooks;
    hooks.connectTail = [&](DurableTailTarget target, DurableTailRound round)
        -> kj::Promise<std::shared_ptr<DurableTailConnection>> {
      if (target.nodeId == "old") throw KJ_EXCEPTION(DISCONNECTED, "the source owner is gone");
      KJ_REQUIRE(round.final);
      if (!failed) {
        if (!first) {
          first = std::make_shared<DurableTailConnection>();
          first->target = target;
          first->generation = 1;
          first->usable = [&] { return !failed; };
          first->failed = [&] { failed = true; };
          first->page = [&](DurableTailAsk ask) -> kj::Promise<DurableTailAnswer> {
            KJ_REQUIRE(ask.objectId == slow && started.fulfiller->isWaiting());
            started.fulfiller->fulfill();
            co_await kj::Promise<void>(kj::NEVER_DONE);
            KJ_UNREACHABLE;
          };
        }
        if (++firstBorrows == 2) bothBorrowed.fulfiller->fulfill();
        co_return first;
      }
      ++replacements;
      auto replacement = std::make_shared<DurableTailConnection>();
      replacement->target = target;
      replacement->generation = 2;
      replacement->page = [&](DurableTailAsk ask) -> kj::Promise<DurableTailAnswer> {
        KJ_REQUIRE(ask.objectId == healthy);
        ++healthyPages;
        co_return DurableTailAnswer{.records = {recoveryPage(ns + "/" + healthy, page)},
            .firstSeq = 1, .lastSeq = 1, .count = 1};
      };
      co_return replacement;
    };
    DurableLog::Settings settings{"new", "new-instance", temp, {}, true, 0, 1000};
    settings.config.confirmTimeoutMs = 100;
    settings.config.peerBufferBytes = 1u << 20;
    settings.config.recoverParallelism = 1;
    DurableLog log(settings, config, io.provider->getNetwork(), timer, *io.lowLevelProvider, membership, std::move(hooks));
    log.nodeEpoch = 9;
    std::filesystem::create_directories(log.logDirectory());
    KJ_REQUIRE(log.initializeRecovery());
    auto a = log.openObject({ns, slow, "", 2, 2, 1}, false).eagerlyEvaluate(nullptr);
    timer.timeoutAfter(5 * kj::SECONDS, kj::mv(started.promise)).wait(io.waitScope);
    auto b = log.openObject({ns, healthy, "", 2, 2, 1}, false).eagerlyEvaluate(nullptr);
    timer.timeoutAfter(5 * kj::SECONDS, kj::mv(bothBorrowed.promise)).wait(io.waitScope);
    KJ_REQUIRE(!failed && log.tailPages.active() == 1, "both objects borrow the same connection before its failure");
    KJ_REQUIRE(a.wait(io.waitScope).ready && b.wait(io.waitScope).ready);
    KJ_REQUIRE(log.waitingRecoveryPublication.started() == 2, "each cut takes a publication permit");
    KJ_REQUIRE(failed && replacements == 1 && healthyPages == 1, "the queued object gets its own final page");
    const auto* image = log.objectOf(durableHostName(ns, healthy, 2));
    std::ifstream file(log.databasePath(*image, 0), std::ios::binary);
    KJ_REQUIRE(std::string(std::istreambuf_iterator<char>(file), {}) == page,
        "the second object's only surviving page is in its rebuilt image");
  }

  static void recoveryCollection(bool live, bool local = false) {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-recovery-test-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    auto& timer = io.provider->getTimer();
    kj::HttpHeaderTable::Builder builder;
    const auto ifNoneMatch = builder.add("If-None-Match");
    auto table = builder.build();
    HeldStore store(*table, ifNoneMatch);
    store.sendStoreTimes = true;
    auto listener = io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
    kj::HttpServer server(timer, *table, store);
    auto listening = server.listenHttp(*listener).eagerlyEvaluate(nullptr);
    ObjectStoreConfig config;
    config.bucket = "b";
    config.endpoint = "http://127.0.0.1:" + std::to_string(listener->getPort());
    config.accessKey = "test-only";
    config.secretKey = "test-only";
    MembershipView membership;
    membership.heldEpochs.insert("9");
    membership.grantedTtlSeconds = 15;
    membership.livenessGraceMs = 10000;
    const std::string ns(64, 'b'), id(64, '1');
    store.objects[shardEpochKey(ns, 1, 2)] = shardEpochJson(
        {ns, "new", "timeout", "new-instance", "nonce", local ? "new" : "old", 1, 2, 9, local ? 1u : 2u, 1, 1, 7});
    store.objects[shardEpochKey(ns, 1, 1)] = shardEpochJson(
        {ns, local ? "new" : "old", "first", "old-instance", "nonce", "", 1, 1, 7, 1, 1});
    store.objects[memberRegistrationKey("old", 7)] = memberRecordJson({"old", "old:8934", "active", "old-instance", 7});
    store.objects[memberRegistrationKey("peer", 8)] = memberRecordJson({"peer", "peer:8934", "active", "peer-instance", 8});
    if (live)
      store.objects[memberHeartbeatKey("old", 7)] = store.objects.at(memberRegistrationKey("old", 7));
    auto ownerReply = kj::newPromiseAndFulfiller<void>();
    auto ownerReady = ownerReply.promise.fork();
    unsigned peerCalls = 0, earlyFinalCalls = 0, ownerFailures = 0, ownerCalls = 0;
    bool ownerReleased = false;
    const auto started = monotonicTimeMs();
    constexpr unsigned WAIT_MS = 300;
    DurableLog::Hooks hooks;
    hooks.connectTail = [&](DurableTailTarget target, DurableTailRound round) -> kj::Promise<std::shared_ptr<DurableTailConnection>> {
      auto handle = std::make_shared<DurableTailConnection>();
      handle->target = target;
      handle->generation = 1;
      handle->failed = [&] { ++ownerFailures; };
      handle->page = [&, target, round](DurableTailAsk ask) -> kj::Promise<DurableTailAnswer> {
        if (target.nodeId == "old") {
          ++ownerCalls;
          if (live) {
            if (ask.afterSeq == 0) {
              auto first = testRecord(ns, 7, 1, 1, 1);
              first.objectName = ns + "/" + id;
              co_return DurableTailAnswer{.records = {std::move(first)}, .firstSeq = 1, .lastSeq = 1,
                  .count = 1, .more = true, .ownerConfirmed = true};
            }
            if (!ownerReleased) {
              co_return DurableTailAnswer{.refusal = "the owner has not finished preparing its answer"};
            }
            co_await ownerReady.addBranch();
            co_return DurableTailAnswer{.ownerConfirmed = true};
          }
          throw KJ_EXCEPTION(DISCONNECTED, "test owner transport failed");
        }
        if (round.final) {
          ++peerCalls;
          if (monotonicTimeMs() < started + WAIT_MS) ++earlyFinalCalls;
        }
        co_return DurableTailAnswer{};
      };
      co_return handle;
    };
    DurableLog log({"new", "new-instance", temp, {}, true, WAIT_MS}, config, io.provider->getNetwork(),
        timer, *io.lowLevelProvider, membership, std::move(hooks));
    log.nodeEpoch = 9;
    const auto dataEpoch = local ? 1u : 2u;
    const auto name = durableHostName(ns, id, dataEpoch);
    auto& object = log.objects[name];
    object.namespaceId = ns;
    object.objectId = id;
    object.objectName = name;
    object.shard = 1;
    object.shardEpoch = 2;
    object.dataEpoch = dataEpoch;
    object.state = DurableLog::ObjectState::REBUILDING;
    DurableLog::RebuildRun run;
    std::optional<ObjectManifest> manifest;
    if (local) {
      log.diskTrusted = log.sameBootCache = true;
      log.predecessorLogs[7].complete = true;
      run.reserved = true;
      auto prepared = kj::newPromiseAndFulfiller<bool>();
      auto collection = log.fetchTails({ns, id, "", dataEpoch, 2, 1}, manifest, started, run,
          kj::mv(prepared.fulfiller)).eagerlyEvaluate(nullptr);
      KJ_REQUIRE(timer.timeoutAfter(5 * kj::SECONDS, kj::mv(prepared.promise)).wait(io.waitScope),
          "a trusted restart with complete local evidence keeps its preparation slot");
      KJ_REQUIRE(!collection.wait(io.waitScope).cut && log.recoveryMetadata.started() == 0 &&
          ownerCalls == 0 && peerCalls == 0, "local evidence needs no waiting-recovery probes");
      return;
    }
    auto collection = log.fetchTails({ns, id, "", 2, 2, 1}, manifest, started, run).eagerlyEvaluate(nullptr);
    if (live) {
      const auto limit = timer.now() + 3 * kj::SECONDS;
      while (log.cutsHeldOwnerLive == 0 && timer.now() < limit) timer.afterDelay(20 * kj::MILLISECONDS).wait(io.waitScope);
      KJ_REQUIRE(!collection.poll(io.waitScope) && log.cutsHeldOwnerLive == 1 && run.owner == "old" && peerCalls == 0);
      KJ_REQUIRE(object.recoveryTails.size() == 1, "the incomplete proof retains its verified prefix while waiting");
      // Release the final page of the owner's proof.
      ownerReleased = true;
      ownerReply.fulfiller->fulfill();
    }
    const auto result = timer.timeoutAfter(5 * kj::SECONDS, kj::mv(collection)).wait(io.waitScope);
    KJ_REQUIRE(ownerCalls > 0 && result.cut != live && earlyFinalCalls == 0);
    if (!live) KJ_REQUIRE(peerCalls > 0 && ownerFailures > 0, "a real tail failure marks the transfer handle");
    else KJ_REQUIRE(peerCalls == 0 && ownerFailures == 0);
    log.rebuildMetadataBytes -= run.metadataBytes;
    log.rebuildMetadataBytes = DurableLog::REBUILD_METADATA_LIMIT;
    const auto ready = log.openObject({ns, std::string(64, '2'), "", 2, 2, 1}, false).wait(io.waitScope);
    KJ_REQUIRE(!ready.ready && ready.status == 503 && ready.code == "DO_RECOVERING" && log.rebuildMetadataRefusals == 1);
  }

  static void publishGate() {
    auto temp = (std::filesystem::temp_directory_path() / "coworkerd-publish-gate-test-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    kj::TimerImpl clock(kj::origin<kj::TimePoint>());
    kj::HttpHeaderTable::Builder builder;
    const auto ifNoneMatch = builder.add("If-None-Match");
    auto table = builder.build();
    HeldStore store(*table, ifNoneMatch);
    auto listener = io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(io.waitScope)->listen();
    kj::HttpServer server(io.provider->getTimer(), *table, store);
    auto http = server.listenHttp(*listener).eagerlyEvaluate(nullptr);
    ObjectStoreConfig config;
    config.endpoint = "http://127.0.0.1:" + std::to_string(listener->getPort());
    config.bucket = "b";
    config.accessKey = "test-only";
    config.secretKey = "test-only";
    MembershipView membership;
    membership.heldEpochs.insert("9");
    DurableLog log({"peer", "instance", temp, {}}, config, io.provider->getNetwork(), clock, *io.lowLevelProvider,
        membership, {});
    log.nodeEpoch = 9;
    log.diskTrusted = true;
    log.sameBootCache = true;
    constexpr uint64_t EPOCH = 3;
    auto& info = log.predecessorLogs[EPOCH];
    info.complete = true;
    const std::string ns(64, 'c');
    auto id = [](int n) {
      char out[65];
      std::snprintf(out, sizeof(out), "%016llx%048x", (unsigned long long) n * 1024, n);
      return std::string(out);
    };
    for (uint64_t sequence = 1; sequence <= 6; ++sequence) {
      LogRecord r;
      r.kind = LogRecordKind::RESET; r.nodeEpoch = EPOCH; r.nodeSeq = sequence; r.shard = 0; r.shardEpoch = 1;
      r.objectName = ns + "/" + id(sequence % 2 ? 1 : 2); r.dataEpoch = 1; r.txn = sequence;
      log.earlier[EPOCH][sequence] = r;
      log.indexEarlierRecord({EPOCH, sequence}, r);
      log.noteEarlierUnpublished(info, r);
    }
    SegmentHeader header;
    header.nodeId = "peer"; header.nodeEpoch = EPOCH; header.segment = 1; header.firstSeq = 1; header.lastSeq = 6;
    header.reason = "earlier-epoch";
    auto pending = [&](const std::string& objectId) {
      DurableTailAsk ask;
      ask.namespaceId = ns; ask.shard = 0; ask.shardEpoch = 2; ask.ownerNodeId = "peer"; ask.ownerNodeEpoch = EPOCH;
      ask.objectId = objectId;
      const auto answer = log.tailFor(ask, false);
      if (answer.refusal == "the restored owner shard still has unpublished records") return true;
      KJ_REQUIRE(answer.refusal.empty() && answer.ownerConfirmed, "an answer is pending or an owner's empty tail",
          answer.refusal.c_str());
      return false;
    };
    // The first object's manifest create is held.
    store.hold = id(1);
    auto parked = kj::newPromiseAndFulfiller<void>();
    store.parked = kj::mv(parked.fulfiller);
    auto publishing = log.publishEarlierObject(EPOCH, header, ns + "/" + id(1) + "/1", 1).eagerlyEvaluate(nullptr);
    parked.promise.wait(io.waitScope);
    KJ_REQUIRE(pending(id(1)), "while its manifest write is held, a request naming the object is told there is more");
    KJ_REQUIRE(log.earlierPublishing.contains(ns + "/" + id(1)) && log.earlier.at(EPOCH).size() == 6,
        "a manifest being written retires nothing");
    store.gate.fulfiller->fulfill();
    publishing.wait(io.waitScope);
    KJ_REQUIRE(store.written == 1 && log.earlierManifests == 1 && log.earlierPublishing.empty(), store.written);
    KJ_REQUIRE(!pending(id(1)) && log.earlier.at(EPOCH).size() == 3, "once the manifest is written the object's tail is empty");
    // The second object's manifest create fails.
    store.fail = id(2);
    log.publishEarlierObject(EPOCH, header, ns + "/" + id(2) + "/1", 1).wait(io.waitScope);
    KJ_REQUIRE(store.failed == 1 && log.earlierManifests == 1, store.failed);
    KJ_REQUIRE(pending(id(2)) && log.earlier.at(EPOCH).size() == 3, "a failed manifest write retires nothing");
  }

  static void restoredTail(bool negativeControls) {
    using Control = DurableLog::RestoredTailControl;
    if (!negativeControls) {
      for (unsigned seed = 1; seed <= 40; ++seed)
        KJ_REQUIRE(restoredTailCase(seed, Control::NONE), "per-object answers match the generated truth", seed);
      return;
    }
    for (const auto control : {Control::SHARD_ONLY, Control::IGNORE_PUBLISHING}) {
      bool caught = false;
      for (unsigned seed = 1; seed <= 40 && !caught; ++seed) caught = !restoredTailCase(seed, control);
      KJ_REQUIRE(caught, "a wrong per-object decision is detected", int(control));
    }
  }
};

}  // namespace coworkerd

namespace coworkerd { void runDurableTailTests(bool negativeControls); }
namespace coworkerd { void runDurableSegmentTests(bool negativeControls); }
namespace coworkerd { void runDurableSegmentCacheTests(); }

int main(int argc, char* argv[]) {
  const std::string_view mode = argc >= 2 ? argv[1] : "";
  if (mode == "tail" || mode == "tail-negative") coworkerd::runDurableTailTests(mode == "tail-negative");
  else if (mode == "segment" || mode == "segment-negative") coworkerd::runDurableSegmentTests(mode == "segment-negative");
  else if (mode == "segment-cache") coworkerd::runDurableSegmentCacheTests();
  else if (mode == "lease-tracker") coworkerd::DurableLogChannelTest::leaseTracker();
  else if (mode == "lease-confirm") coworkerd::DurableLogChannelTest::leaseConfirm(argc == 3 ? argv[2] : "");
  else if (mode == "peer-fence") coworkerd::DurableLogChannelTest::peerFence(argc == 3 ? argv[2] : "");
  else if (mode == "refused-peer") coworkerd::DurableLogChannelTest::refusedPeer(argc == 3 ? argv[2] : "");
  else if (mode == "refused-owner") coworkerd::DurableLogChannelTest::refusedOwner(argc == 3 ? argv[2] : "");
  else if (mode == "earlier-segment") coworkerd::DurableLogChannelTest::earlierSegment();
  else if (mode == "threaded-channel") coworkerd::DurableLogChannelTest::threadedChannel();
  else if (mode == "threaded-channel-eof") coworkerd::DurableLogChannelTest::threadedChannelEof();
  else if (mode == "parked-websocket") coworkerd::DurableLogChannelTest::parkedWebSocket();
  else if (mode == "websocket-frame-budget") coworkerd::DurableLogChannelTest::webSocketFrameBudget();
  else if (mode == "websocket-upload-budget") coworkerd::DurableLogChannelTest::webSocketUploadBudget();
  else if (mode == "threaded-alarms") coworkerd::DurableLogChannelTest::threadedAlarms();
  else if (mode == "publish-gate") coworkerd::DurableLogChannelTest::publishGate();
  else if (mode == "recovery-admission") coworkerd::DurableLogChannelTest::recoveryAdmission();
  else if (mode == "recovery-completion") coworkerd::DurableLogChannelTest::recoveryCompletion();
  else if (mode == "recovery-requeue") coworkerd::DurableLogChannelTest::recoveryRequeue();
  else if (mode == "recovery-final-per-object") coworkerd::DurableLogChannelTest::recoveryFinalPerObject();
  else if (mode == "recovery-final-round" || mode == "recovery-live-owner" || mode == "recovery-local-restart")
    coworkerd::DurableLogChannelTest::recoveryCollection(mode == "recovery-live-owner", mode == "recovery-local-restart");
  else if (mode == "restored-tail" || mode == "restored-tail-negative")
    coworkerd::DurableLogChannelTest::restoredTail(mode == "restored-tail-negative");
  else coworkerd::DurableLogChannelTest::run(mode == "index");
}
