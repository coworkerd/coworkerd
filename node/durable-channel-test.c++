#include "durable-log.h"

#include <kj/async-io.h>
#include <kj/timer.h>
#include <sys/wait.h>
#include <unistd.h>

namespace coworkerd {

struct DurableLogChannelTest {
  static void run() {
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
    constexpr int COUNT = 32;
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
    const auto child = ::fork();
    KJ_REQUIRE(child != -1);
    if (child == 0) {
      const auto n = ::write(peer, burst.data(), burst.size());
      ::_exit(n == static_cast<ssize_t>(burst.size()) ? 0 : 1);
    }
    ::close(peer);
    int status = 0;
    KJ_REQUIRE(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0);
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

int main() {
  coworkerd::DurableLogChannelTest::run();
}
