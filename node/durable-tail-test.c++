#include "durable-log.h"
#include "coworkerd.capnp.h"
#include "durable-tail-legacy-test.capnp.h"
#include <capnp/message.h>
#include <kj/async-io.h>
#include <algorithm>
#include <array>
#include <concepts>
#include <random>
#include <unistd.h>

namespace coworkerd {
namespace {
enum class Control { NONE, IGNORE_OBJECT, STALE_CURSOR, DROP_PROOF, TRUNCATED_PAGE,
    SAME_OBJECT_CONFLICT, CROSS_OBJECT_CONFLICT, DESTINATION_FENCE, SHARD_FENCE };
}  // namespace

struct DurableTailTest {
  static void run(unsigned seed, Control control = Control::NONE) {
    constexpr size_t PAGE_BYTES = 4096;
    auto temp = (std::filesystem::temp_directory_path() / "durable-tail-XXXXXX").string();
    KJ_REQUIRE(::mkdtemp(temp.data()) != nullptr);
    KJ_DEFER({ std::error_code ignored; std::filesystem::remove_all(temp, ignored); });
    auto io = kj::setupAsyncIo();
    auto& timer = io.provider->getTimer();
    ObjectStoreConfig store;
    store.backend = "directory"; store.directory = temp; store.endpoint = "unused";
    MembershipView membership;
    membership.heldEpochs.insert("3"); membership.heldEpochs.insert("9");
    uint64_t known = 0;
    DurableLog::Hooks hooks;
    hooks.knownShardEpoch = [&](const std::string&, uint32_t) { return known; };
    DurableLog::Settings settings{"reply", "instance", temp, {}};
    settings.config.peerBufferBytes = 512u << 20;
    DurableLog log(std::move(settings), store, io.provider->getNetwork(), timer,
        *io.lowLevelProvider, membership, std::move(hooks));
    log.nodeEpoch = 9;
    std::mt19937 random(seed);
    const std::string ns(64,'1');
    auto object = [](unsigned n) { return std::string(63,'2') + char('0' + n); };
    auto record = [&](uint64_t epoch, uint64_t seq, unsigned obj, uint64_t shardEpoch, uint32_t shard) {
      LogRecord r;
      r.nodeEpoch=epoch; r.nodeSeq=seq; r.shardEpoch=shardEpoch; r.shard=shard;
      r.objectName=ns+"/"+object(obj); r.dataEpoch=1+(seq%5);
      r.kind=(seq%2) ? LogRecordKind::DELETED : LogRecordKind::RESET;
      r.txn=seq; return r;
    };
    auto incoming = [&](uint64_t epoch, std::string instance, uint64_t first) {
      uint64_t out=0;
      KJ_REQUIRE(log.openIncoming("sender",epoch,instance,"9",first,out).empty());
      return out;
    };
    auto s1=incoming(2,"one",1), s2=incoming(2,"two",8), s3=incoming(4,"three",1);
    // Receive every history entry through the real append decoder; current and
    // earlier records are staged at the same indexes used by recovery.
    std::map<uint64_t, std::vector<std::string>> streams;
    std::map<uint64_t,std::vector<LogRecord>> reference;
    auto remember = [&](const LogRecord& r) {
      if (r.shard==1 && r.shardEpoch<=3) reference[r.nodeEpoch].push_back(r);
    };
    constexpr unsigned count = 270;
    auto make = [&](uint64_t e, uint64_t seq, unsigned obj, uint64_t se, uint32_t s) {
      return record(e,seq,obj,se,s);
    };
    for (uint64_t seq=1;seq<=count;++seq) {
      const auto obj=seq==5 || seq==17 || seq==41 || seq==78 ? 0u : 1u+(random()%4);
      const auto shardEpoch=seq%11==0 ? 5u : 1u+(random()%3);
      const uint32_t shard=seq%13==0 ? 2u : 1u;
      auto r=make(2,seq,obj,shardEpoch,shard);
      auto encoded=encodeLogRecord(r);
      streams[s1].push_back(encoded);
      if (seq>3 && seq!=77 && seq!=78 && seq!=80) remember(r);
      if (seq%19==0 && seq>=8) streams[s2].push_back(encoded);
      r=make(4,seq,seq%7==0?0:obj,shardEpoch,shard);
      streams[s3].push_back(encodeLogRecord(r)); remember(r);
      for (const uint64_t e : {3,9}) {
        r=make(e,seq,seq%5==0?0:obj,shardEpoch,shard);
        if (e==9) {
          log.records.emplace(seq, std::make_shared<const std::string>(encodeLogRecord(r)));
          log.appendedSeq = seq;
          log.rememberParsedRecord(r);
          if (seq%7!=0) log.confirmed.insert(seq);
          if (seq%11==0) log.isolated.insert(seq);
          if (seq%7!=0 && seq%11!=0) remember(r);
        } else {
          log.earlier[e].emplace(seq,r);log.indexEarlierRecord({e,seq},r); remember(r);
        }
      }
    }
    for (auto& [stream, rows] : streams)
      for (size_t offset=0;offset<rows.size();offset+=1000) {
        const auto last=std::min(rows.size(),offset+1000);
        const std::vector<std::string> batch(rows.begin()+offset,rows.begin()+last);
        KJ_REQUIRE(log.appendIncoming(stream,frameRecords(batch)).refusal.empty());
      }
    log.trimIncoming(s1,3);
    log.dropIncoming(s1,{77,78,80});
    log.trimIncoming(s2,9);
    KJ_REQUIRE(log.incoming.at(s1).positions.contains(77)==false);
    using New = v1::rpc::DurableTailRequest;
    using Old = tailtest::legacy::DurableTailRequest;
    const auto transfer = [&]<typename Writer, typename Reader>(const DurableTailAsk& ask) {
      capnp::MallocMessageBuilder wire;
      auto fields=wire.initRoot<Writer>();
      fields.setNamespaceId(ask.namespaceId.c_str());fields.setShard(ask.shard);fields.setShardEpoch(ask.shardEpoch);
      fields.setOwnerNodeId(ask.ownerNodeId.c_str());fields.setOwnerNodeEpoch(ask.ownerNodeEpoch);fields.setAfterSeq(ask.afterSeq);
      fields.setDestinationEpoch("9");
      if constexpr (std::same_as<Writer,New>) fields.setObjectId(ask.objectId.c_str());
      const auto spans=wire.getSegmentsForOutput();
      capnp::SegmentArrayMessageReader copy(spans);
      const auto r=copy.getRoot<Reader>();
      DurableTailAsk parsed;
      parsed.namespaceId=r.getNamespaceId().cStr();parsed.shard=r.getShard();parsed.shardEpoch=r.getShardEpoch();
      parsed.ownerNodeId=r.getOwnerNodeId().cStr();parsed.ownerNodeEpoch=r.getOwnerNodeEpoch();parsed.afterSeq=r.getAfterSeq();
      if constexpr (std::same_as<Reader,New>) parsed.objectId=r.getObjectId().cStr();
      KJ_REQUIRE(r.getDestinationEpoch()=="9");
      return parsed;
    };
    struct Answer { std::vector<std::string> local; unsigned pages=0; bool includedOther=false; };
    const auto drain=[&](DurableTailAsk ask, unsigned mode, uint64_t expectedDrop) {
      Answer output;
      while (true) {
        DurableTailAsk served;
        if (mode==0) served=transfer.template operator()<New,New>(ask);
        if (mode==1) served=transfer.template operator()<New,Old>(ask);
        if (mode==2) served=transfer.template operator()<Old,New>(ask);
        if (mode==3) { served=ask; served.objectId.clear(); }
        if (mode == 0 && control == Control::IGNORE_OBJECT) served.objectId.clear();
        if (mode == 0 && control == Control::STALE_CURSOR && ask.afterSeq != 0) served.afterSeq = 0;
        auto answer = log.tailForPage(served, false, PAGE_BYTES);
        if (mode == 0 && control == Control::DROP_PROOF) answer.droppedTo = 0;
        if (mode == 0 && control == Control::TRUNCATED_PAGE) answer.more = false;
        KJ_REQUIRE(answer.refusal.empty(), "unfenced probe",answer.refusal.c_str());
        KJ_REQUIRE(answer.droppedTo==expectedDrop, "preserve source drop watermark");
        KJ_REQUIRE(answer.count==answer.records.size(), "page count");
        uint64_t last=ask.afterSeq;
        for (const auto& r : answer.records) {
          KJ_REQUIRE(r.nodeEpoch==ask.ownerNodeEpoch && r.shard==ask.shard && r.shardEpoch<=ask.shardEpoch &&
              r.nodeSeq>last, "monotonic requested cursor and fence");
          last=r.nodeSeq;
          if (r.objectName==ns+"/"+ask.objectId) output.local.push_back(encodeLogRecord(r));
          else output.includedOther=true;
        }
        KJ_REQUIRE(mode!=0 || !output.includedOther,"object-aware wire excludes other objects");
        KJ_REQUIRE(answer.records.empty() ? answer.lastSeq==0 : last==answer.lastSeq, "lastSeq is the last returned position");
        ++output.pages;
        if (!answer.more) return output;
        KJ_REQUIRE(last>ask.afterSeq && output.pages<=260,"pager must advance");
        ask.afterSeq=last;
      }
    };
    for (const auto& [owner, epoch] : {std::pair{"sender",2},std::pair{"sender",4},std::pair{"reply",3},std::pair{"reply",9}}) {
      const auto oldFirst = log.tailForPage(DurableTailAsk{ns,owner,1,3,uint64_t(epoch),0,{}}, false, PAGE_BYTES);
      const std::array<uint64_t,4> cursors{0,17,41,oldFirst.lastSeq};
      for (unsigned id : {0,1,5}) for (uint64_t cursor : cursors) {
        DurableTailAsk ask{ns,owner,1,3,uint64_t(epoch),cursor,object(id)};
        auto original=drain(ask,3,epoch==2?9:0);
        auto bothNew=drain(ask,0,epoch==2?9:0);
        auto oldReply=drain(ask,1,epoch==2?9:0);
        auto oldRequest=drain(ask,2,epoch==2?9:0);
        std::vector<std::string> expected;
        const auto wanted=ns+"/"+object(id);
        for (const auto& record : reference[epoch])
          if (record.objectName==wanted && record.nodeSeq>cursor) expected.push_back(encodeLogRecord(record));
        KJ_REQUIRE(bool(original.local==expected), "legacy result matches independent record oracle", id, cursor, epoch);
        KJ_REQUIRE(bool(original.local==bothNew.local && original.local==oldReply.local && original.local==oldRequest.local),
            "all three paths return the original bytes", id, cursor, epoch);
        KJ_REQUIRE(oldReply.pages==original.pages && oldRequest.pages==original.pages && oldReply.includedOther==original.includedOther &&
            oldRequest.includedOther==original.includedOther, "legacy peers keep shard paging");
        if (epoch==2 && id==0 && cursor==0)
          KJ_REQUIRE(original.pages>1 && bothNew.pages==1 && bothNew.local.size()>1 && oldReply.includedOther && oldRequest.includedOther,
              "underfilled object page finishes with unrelated rows still in shard");
        if (id==5) KJ_REQUIRE(bothNew.local.empty() && bothNew.pages==1, "absent object is a complete empty page");
        if (epoch==2 && id==1 && cursor==0) KJ_REQUIRE(bothNew.pages>1, "a target spanning its own pages");
      }
    }
    DurableTailAsk ask{ns,"sender",1,3,2,280,object(0)};
    auto a=encodeLogRecord(record(2,290,0,1,1));
    KJ_REQUIRE(log.appendIncoming(s1,frameRecords({a})).refusal.empty());
    KJ_REQUIRE(log.appendIncoming(s2,frameRecords({a})).refusal.empty());
    KJ_REQUIRE(log.tailFor(ask,false).count==1 && log.tailFor(ask,false).refusal.empty(), "identical duplicates collapse");
    auto s4=incoming(2,"conflicting",1);
    auto different=encodeLogRecord(record(2,290,1,1,1));
    KJ_REQUIRE(log.appendIncoming(s4,frameRecords({different})).refusal.empty());
    auto crossConflict = log.tailFor(ask, false).refusal;
    if (control == Control::CROSS_OBJECT_CONFLICT) crossConflict.clear();
    KJ_REQUIRE(crossConflict == "conflicting bytes for one tail record", "duplicate identity in a different index must refuse");
    auto unrelated = ask;
    unrelated.objectId = object(5);
    auto unrelatedAnswer = log.tailFor(unrelated, false);
    KJ_REQUIRE(unrelatedAnswer.refusal.empty() && unrelatedAnswer.records.empty(),
        "an unrelated position must not refuse an empty object tail");
    log.dropIncoming(s4,{290});
    KJ_REQUIRE(log.tailFor(ask,false).count==1 && log.tailFor(ask,false).refusal.empty(), "drop removes the conflict");
    s4=incoming(2,"conflicting",1);
    auto changed=record(2,290,0,1,1);changed.txn+=1000;
    auto s5=incoming(2,"different bytes",1);
    KJ_REQUIRE(log.appendIncoming(s5,frameRecords({encodeLogRecord(changed)})).refusal.empty());
    auto sameConflict = log.tailFor(ask, false).refusal;
    if (control == Control::SAME_OBJECT_CONFLICT) sameConflict.clear();
    KJ_REQUIRE(sameConflict == "conflicting bytes for one tail record", "same-object conflicting bytes must refuse");
    log.closeIncoming(s5);
    known=4;
    auto shardFence = log.tailFor(ask, true).refusal;
    if (control == Control::SHARD_FENCE) shardFence.clear();
    KJ_REQUIRE(shardFence == "the requesting shard epoch has been fenced", "shard fence remains active");
    known=0;
    ask.ownerNodeId="reply";ask.ownerNodeEpoch=6;
    log.diskTrusted=true;log.sameBootCache=true;log.predecessorLogs[6].complete=true;
    auto wrongDestinationIsAccepted = log.canAnswerRestoredTail(ask, "5");
    if (control == Control::DESTINATION_FENCE) wrongDestinationIsAccepted = true;
    KJ_REQUIRE(log.canAnswerRestoredTail(ask, "6") && !wrongDestinationIsAccepted,
        "destination epoch matches only the complete predecessor");
    log.predecessorLogs[6].unpublishedShards[{ns,1}]=2;
    KJ_REQUIRE(log.tailFor(ask,false).refusal=="the restored owner shard still has unpublished records", "shard-wide unpublished proof remains active");
  }
};

void runDurableTailTests(bool negativeControls) {
  if (!negativeControls) {
    for (unsigned seed = 113; seed < 145; ++seed) DurableTailTest::run(seed);
    return;
  }
  // Check that the same assertions used above reject specific bad wire/page
  // outcomes. No control changes the daemon or its inputs outside this test.
  const std::pair<Control, std::string_view> controls[]{
    {Control::IGNORE_OBJECT, "object-aware wire excludes other objects"},
    {Control::STALE_CURSOR, "monotonic requested cursor"},
    {Control::DROP_PROOF, "preserve source drop watermark"},
    {Control::TRUNCATED_PAGE, "all three paths return the original bytes"},
    {Control::SAME_OBJECT_CONFLICT, "same-object conflicting bytes"},
    {Control::CROSS_OBJECT_CONFLICT, "duplicate identity in a different index"},
    {Control::DESTINATION_FENCE, "destination epoch matches"},
    {Control::SHARD_FENCE, "shard fence remains active"},
  };
  for (const auto& [control, expected] : controls) {
    bool rejected = false;
    try {
      DurableTailTest::run(113, control);
    } catch (const kj::Exception& ex) {
      KJ_REQUIRE(std::string_view(ex.getDescription().cStr()).find(expected) != std::string_view::npos,
          "negative control failed for a different reason", ex.getDescription());
      rejected = true;
    }
    KJ_REQUIRE(rejected, "regression was not detected", expected.data());
  }
}

}  // namespace coworkerd
