#include "durable-log.h"

#include "support.h"

#include <kj/async-io.h>
#include <kj/timer.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cmath>
#include <cstring>
#include <fstream>
#include <optional>

namespace coworkerd {

struct DurableLogChannelTest {
  // r46: the lease of a membership epoch, by the tracker alone (results/r45/DESIGN.md 10.1). TTL 5 s, interval 1 s.
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
      // At S + T exactly, with nothing in flight: expired and fenced for good.
      KJ_REQUIRE(lease.judge(7, 5000).verdict == V::EXPIRED && lease.fenced(7));
      KJ_REQUIRE(lease.sent(7, 5001, 5000) == 0, "a fenced epoch sends no heartbeat");
      KJ_REQUIRE(lease.judge(7, 5002).verdict == V::EXPIRED);
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
        KJ_REQUIRE(lease.judge(8, 10000.2).verdict == V::EXPIRED, outcome);
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
      KJ_REQUIRE(lease.judge(10, 8999).verdict == V::HELD && lease.judge(10, 9000).verdict == V::EXPIRED);
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

  // r46: the confirmation path of the real log, with the lease hook a test controls. The log has no peers, so each
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
    DurableLog log({"node", "instance", temp, {}}, store, io.provider->getNetwork(), clock, *io.lowLevelProvider,
        membership, {});
    log.settings.config.peers = 0;
    log.setNodeEpoch(1);
    KJ_REQUIRE(log.logFd >= 0, "the test log could not be opened");
    // The real tracker, on the real lease clock, with a TTL the scenario sets.
    const std::string_view name(scenario);
    const double ttl = name == "expired" || name == "stale-timer" ? 50 : name == "slow-flush" ? 100 : 60000;
    LeaseTracker tracker(ttl, 1000);
    tracker.begin(1);
    const auto start = leaseClockMs();
    KJ_REQUIRE(tracker.settled(tracker.sent(1, start, start), true, leaseClockMs()));
    std::optional<LeaseTracker::Judgment> forced;
    uint32_t lost = 0;
    double judgedAt = 0;
    log.setLeaseHooks([&](uint64_t epoch, double now) {
          judgedAt = now;
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
      ::usleep(80000);
      KJ_REQUIRE(clock.now() == kj::origin<kj::TimePoint>());
      const auto before = log.nextSeq;
      take(42);
      KJ_REQUIRE(answers().at(42) == 1 && confirmed() == 0 && log.nextSeq == before && lost == 1,
          "an expired lease refuses the record before it enters the log");
      KJ_REQUIRE(tracker.fenced(1) && log.leaseRefusals == 1);
      if (name == "stale-timer") {
        // Case 5: what was judged is a fresh reading of the lease clock, past the TTL, not the event loop's time.
        KJ_REQUIRE(judgedAt - start >= 80 && std::abs(leaseClockMs() - judgedAt) < 50 &&
            clock.now() == kj::origin<kj::TimePoint>(), judgedAt - start);
      }
      // Fenced: later records of the epoch are refused as well, and the hook is not called again for it.
      take(43);
      KJ_REQUIRE(answers().at(43) == 1 && confirmed() == 0 && lost == 1);
    } else if (name == "slow-flush") {
      // Case 2: the local flush itself takes longer than what is left of the lease. Taken while held; judged again
      // after the flush, the confirmation is refused (a check only before the flush would have confirmed it).
      const auto trigger = std::filesystem::path(temp) / "slow-flush";
      { std::ofstream out(trigger); out << "250"; }
      log.slowFlushForTest = trigger.string();
      take(44);
      KJ_REQUIRE(!std::filesystem::exists(trigger), "the slow flush did not run");
      KJ_REQUIRE(answers().at(44) == 1 && confirmed() == 0 && lost == 1 && tracker.fenced(1),
          "a lease that ended during the local flush must refuse the confirmation");
      KJ_REQUIRE(log.isolated.size() == 1 && log.inFlight.empty(), "the refused record is excluded like any other");
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
    } else {
      KJ_FAIL_REQUIRE("unknown lease scenario", scenario);
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
      log.setNodeEpoch(1);
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
      log.setNodeEpoch(2);
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
    const int peer = log.openChannel("d0");
    KJ_REQUIRE(peer >= 0);
    auto& channel = log.channels.at("d0");
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
      facet.processId = "d0";
      facet.openEpoch = 1;
      facet.openGeneration = 2;
      const auto key = name + "/0";
      channel.openFacets.emplace(key, std::pair<uint64_t, uint64_t>{1, 2});
      std::string closed(1, '\x07');  // FROM_CLOSED, as the durable shipper writes it.
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
    log.closeChannel("d0");
    KJ_REQUIRE(!log.channels.contains("d0"));
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
};

}  // namespace coworkerd

namespace coworkerd { void runDurableTailTests(bool negativeControls); }

int main(int argc, char* argv[]) {
  const std::string_view mode = argc >= 2 ? argv[1] : "";
  if (mode == "tail" || mode == "tail-negative") coworkerd::runDurableTailTests(mode == "tail-negative");
  else if (mode == "lease-tracker") coworkerd::DurableLogChannelTest::leaseTracker();
  else if (mode == "lease-confirm") coworkerd::DurableLogChannelTest::leaseConfirm(argc == 3 ? argv[2] : "");
  else coworkerd::DurableLogChannelTest::run(mode == "index");
}
