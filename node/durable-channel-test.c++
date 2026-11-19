#include "durable-log.h"
#include "durable-shards.h"

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
    const std::string_view name(scenario);
    // The membership's current epoch; "old-epoch": it registered epoch 2 while this log still writes under 1.
    membership.epoch = name == "old-epoch" ? "2" : "1";
    DurableLog log({"node", "instance", temp, {}}, store, io.provider->getNetwork(), clock, *io.lowLevelProvider,
        membership, {});
    log.settings.config.peers = 0;
    log.setNodeEpoch(1);
    KJ_REQUIRE(log.logFd >= 0, "the test log could not be opened");
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

  // r48: a peer that answers a newer epoch's tail request raises its floor for the shard (results/r45/DESIGN.md 10.4).
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

  // r48 sections 6.1-6.2: an owner reads a peer's answer as covering every record up to it. A record the peer refused
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

  // Peer side, this node running r48. "fenced" (an owner of the older version, which reads answers cumulatively):
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
    uint64_t oldOwnerAcked = 0;  // what an owner without r48 counts: the highest answer it saw
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

  // A peer in this process for the "pair" scenarios: the real append path of another DurableLog (r48 on both ends).
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

  // Owner side, this node running r48. "<reason>" and "<reason>-reconnect" (reason: fenced, full, cut-short, limit,
  // corrupt), with a peer of the older version that answers with its highest sequence: records 1, 2, 3 (three
  // objects) go out; the peer answers 1 with 3, refuses 2 for the reason and takes 3. Only 1 and 3 may be confirmed;
  // with "-reconnect" the session is then replaced and everything sent again, the peer answering as before. A full
  // buffer is tried again on the same session after FULL_RETRY_FIRST_MS: taken then, 2 is confirmed. "chunk": record
  // 2 is larger than APPEND_CHUNK_BYTES, its first piece answered with 3 and its last refused as fenced: 2 must not
  // count from its first piece's answer. "pair-full", "pair-fenced": the peer is a real r48 DurableLog, its buffer too
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
    DurableLog::Hooks hooks;
    hooks.openStream = [&](std::string, uint64_t ownerEpoch, uint64_t firstSeq)
        -> kj::Promise<kj::Own<DurableLog::PeerSink>> {
      ++opened;
      if (!pair) return kj::Own<DurableLog::PeerSink>(kj::heap<FakeSink>(calls));
      uint64_t stream = 0;
      KJ_REQUIRE(peer.openIncoming("node", ownerEpoch, "instance", "5", firstSeq, stream).empty());
      return kj::Own<DurableLog::PeerSink>(kj::heap<LocalSink>(peer, stream));
    };
    DurableLog log({"node", "instance", temp, {}}, store, io.provider->getNetwork(), clock, *io.lowLevelProvider,
        membership, std::move(hooks));
    log.settings.config.peers = 1;
    log.setNodeEpoch(1);
    KJ_REQUIRE(log.logFd >= 0);
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
    io.waitScope.poll();
    std::map<uint64_t, uint8_t> acks;
    auto answers = [&]() {
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
    if (name == "pair-full") {
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
  else if (mode == "peer-fence") coworkerd::DurableLogChannelTest::peerFence(argc == 3 ? argv[2] : "");
  else if (mode == "refused-peer") coworkerd::DurableLogChannelTest::refusedPeer(argc == 3 ? argv[2] : "");
  else if (mode == "refused-owner") coworkerd::DurableLogChannelTest::refusedOwner(argc == 3 ? argv[2] : "");
  else coworkerd::DurableLogChannelTest::run(mode == "index");
}
