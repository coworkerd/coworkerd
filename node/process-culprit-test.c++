#include "process-culprit.h"
#include <deque>
#include <map>
#include <string_view>

namespace coworkerd {
namespace {
struct Call { std::string workerId, versionId, invocationId; bool dispatched = false; };
struct Process {
  bool threaded = true, durable = false;
  uint32_t pid = 11;
  std::string culpritRow;
  std::map<std::string, Call*> calls;
  std::deque<Call> recentDispatched;
  void pullEvents() {}
};
void cpu() {
  Process process;
  Call active{"tenant/worker", "v1", "call", true};
  process.calls.emplace("call", &active);
  uint32_t confirmations = 0;
  bool label = true;
  auto report = [&](const char* worker = "tenant/worker", const char* version = "v1",
      uint32_t pid = 11, uint32_t limit = 100, uint64_t used = 100000000) {
    process.culpritRow = "{\"event\":\"culprit\",\"pid\":" + std::to_string(pid) +
        ",\"workerId\":\"" + worker + "\",\"versionId\":\"" + version +
        "\",\"invocationId\":\"call\",\"isolate\":\"isolate\",\"cpuLimitMs\":" +
        std::to_string(limit) + ",\"cpuUsedNs\":\"" + std::to_string(used) + "\"}";
  };
  auto run = [&] {
    return confirmedCpuCulprit(process, [](const std::string&, bool) {
      struct Limits { uint32_t cpuMs = 100; }; return Limits{};
    }, [&](Process&, const std::string& isolate, const std::string& worker, const std::string& version) {
      return label && isolate == "isolate" && worker == "tenant/worker" && version == "v1";
    }, [&](const std::string& worker) { KJ_REQUIRE(worker == "tenant/worker"); ++confirmations; });
  };
  report(); KJ_REQUIRE(run() != "null" && confirmations == 1, "live dispatched call");
  active.dispatched = false; KJ_REQUIRE(run() == "null"); active.dispatched = true;
  process.calls.clear(); process.recentDispatched.push_back(active);
  KJ_REQUIRE(run() != "null" && confirmations == 2, "S1 attributes a just-completed CPU call");
  process.threaded = false; KJ_REQUIRE(run() == "null"); process.threaded = true;
  report("other/worker"); KJ_REQUIRE(run() == "null");
  report("tenant/worker", "v2"); KJ_REQUIRE(run() == "null");
  report("tenant/worker", "v1", 12); KJ_REQUIRE(run() == "null");
  report("tenant/worker", "v1", 11, 101); KJ_REQUIRE(run() == "null");
  report("tenant/worker", "v1", 11, 100, 99999999); KJ_REQUIRE(run() == "null");
  report(); label = false; KJ_REQUIRE(run() == "null"); label = true;
  process.recentDispatched.clear(); KJ_REQUIRE(run() == "null" && confirmations == 2);
}
void memory() {
  Call active{"tenant/worker", "v1", "active", true}, queued{"tenant/worker", "v1", "queued", false};
  std::map<std::string, Call*> live{{"active", &active}, {"queued", &queued}};
  std::deque<Call> recent{{"tenant/worker", "v1", "completed", true}};
  KJ_REQUIRE(memoryCulpritDispatch(live, recent, "active", "tenant/worker", "v1"));
  KJ_REQUIRE(memoryCulpritDispatch(live, recent, "completed", "tenant/worker", "v1"));
  KJ_REQUIRE(!memoryCulpritDispatch(live, recent, "completed", "tenant/worker", "v2"));
  KJ_REQUIRE(!memoryCulpritDispatch(live, recent, "completed", "other/worker", "v1"));
  KJ_REQUIRE(!memoryCulpritDispatch(live, recent, "queued", "tenant/worker", "v1"));
  KJ_REQUIRE(!memoryCulpritDispatch(live, recent, "missing", "tenant/worker", "v1"));
}
}  // namespace
}  // namespace coworkerd
int main(int argc, char* argv[]) {
  KJ_REQUIRE(argc == 2);
  if (std::string_view(argv[1]) == "cpu") coworkerd::cpu();
  else if (std::string_view(argv[1]) == "memory") coworkerd::memory();
  else KJ_FAIL_REQUIRE("unknown culprit test");
}
