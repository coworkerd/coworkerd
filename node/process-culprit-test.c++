#include "process-culprit.h"
#include <csignal>
#include <deque>
#include <map>
#include <string_view>

namespace coworkerd {
namespace {
constexpr int fatalStatus = 75 << 8;  // The exit code occupies the high byte of waitpid status.
struct Call { std::string workerId, versionId, invocationId; bool dispatched = false; };
struct Process {
  bool threaded = true, durable = false;
  uint32_t pid = 11;
  bool oomSignalled = false;
  std::string culpritRow, oomCulpritRow;
  FatalExitEvidence fatalEvidence;
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
  KJ_REQUIRE(run() != "null" && confirmations == 2, "watchdog attributes a just-completed CPU call");
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
  KJ_REQUIRE(crashCulpritDispatch(live, recent, "active", "tenant/worker", "v1"));
  KJ_REQUIRE(crashCulpritDispatch(live, recent, "completed", "tenant/worker", "v1"));
  KJ_REQUIRE(!crashCulpritDispatch(live, recent, "completed", "tenant/worker", "v2"));
  KJ_REQUIRE(!crashCulpritDispatch(live, recent, "completed", "other/worker", "v1"));
  KJ_REQUIRE(!crashCulpritDispatch(live, recent, "queued", "tenant/worker", "v1"));
  KJ_REQUIRE(!crashCulpritDispatch(live, recent, "missing", "tenant/worker", "v1"));
}
void fatal() {
  Process process;
  Call active{"tenant/worker", "v1", "call", true};
  process.calls.emplace("call", &active);
  auto row = [](int pid, std::string_view worker, std::string_view version, std::string_view isolate) {
    return "{\"event\":\"v8-fatal\",\"pid\":" + std::to_string(pid) +
        ",\"workerId\":\"" + std::string(worker) + "\",\"versionId\":\"" + std::string(version) +
        "\",\"invocationId\":\"call\",\"isolate\":\"" + std::string(isolate) + "\"}";
  };
  auto report = [&](int pid, std::string_view worker, std::string_view version, std::string_view isolate) {
    process.fatalEvidence = {};
    process.fatalEvidence.record("v8-fatal", row(pid, worker, version, isolate));
  };
  const std::string digest(64, 'a');
  const std::string label = "coworkerd-v1:tenant/worker@v1#" + digest;
  auto good = [&] { report(11, "tenant/worker", "v1", label); };
  int confirmations = 0;
  auto run = [&](int status = fatalStatus) {
    return confirmedFatalCulprit(process, status, [&](const std::string& worker, const std::string& invocation) {
      KJ_ASSERT(worker == "tenant/worker" && invocation == "call");
      ++confirmations;
    });
  };
  KJ_REQUIRE(!fatalExitMatches(process, fatalStatus) && run() == "null");
  good();
  KJ_REQUIRE(fatalExitMatches(process, fatalStatus));
  KJ_REQUIRE(run(0) == "null" && run(74 << 8) == "null" && run(SIGKILL) == "null");
  process.oomSignalled = true;
  KJ_REQUIRE(!fatalExitMatches(process, fatalStatus) && run() == "null");
  process.oomSignalled = false;
  process.oomCulpritRow = "{\"event\":\"memory-culprit\"}";
  KJ_REQUIRE(!fatalExitMatches(process, fatalStatus) && run() == "null");
  process.oomCulpritRow.clear();
  const auto validRow = row(11, "tenant/worker", "v1", label);
  process.fatalEvidence.record("v8-fatal", validRow);
  KJ_REQUIRE(fatalExitMatches(process, fatalStatus) && run() == "null");
  process.fatalEvidence.record("v8-fatal", validRow);
  KJ_REQUIRE(run() == "null");
  const std::string unknown = "{\"event\":\"v8-fatal-undetermined\",\"pid\":11}";
  good();
  process.fatalEvidence.record("v8-fatal-undetermined", unknown);
  KJ_REQUIRE(run() == "null");
  process.fatalEvidence = {};
  process.fatalEvidence.record("v8-fatal-undetermined", unknown);
  KJ_REQUIRE(fatalExitMatches(process, fatalStatus) && run() == "null");
  process.fatalEvidence.record("v8-fatal", validRow);
  KJ_REQUIRE(run() == "null" && confirmations == 0);
  good();
  active.dispatched = false; KJ_REQUIRE(run() == "null"); active.dispatched = true;
  report(12, "tenant/worker", "v1", label); KJ_REQUIRE(run() == "null");
  report(11, "other/worker", "v1", "coworkerd-v1:other/worker@v1#" + digest); KJ_REQUIRE(run() == "null");
  report(11, "tenant/worker", "v2", "coworkerd-v1:tenant/worker@v2#" + digest); KJ_REQUIRE(run() == "null");
  report(11, "tenant/worker", "v1", "coworkerd-v1:other/worker@v1#" + digest); KJ_REQUIRE(run() == "null");
  report(11, "tenant/worker", "v1", "coworkerd-v1:tenant/worker@v1#7"); KJ_REQUIRE(run() == "null");
  good(); KJ_REQUIRE(run() != "null" && confirmations == 1);
  process.calls.clear(); process.recentDispatched.push_back(active);
  KJ_REQUIRE(run() != "null" && confirmations == 2);
  process.recentDispatched.clear(); KJ_REQUIRE(run() == "null" && confirmations == 2);
}
}  // namespace
}  // namespace coworkerd
int main(int argc, char* argv[]) {
  KJ_REQUIRE(argc == 2);
  if (std::string_view(argv[1]) == "cpu") coworkerd::cpu();
  else if (std::string_view(argv[1]) == "memory") coworkerd::memory();
  else if (std::string_view(argv[1]) == "fatal") coworkerd::fatal();
  else KJ_FAIL_REQUIRE("unknown culprit test");
}
