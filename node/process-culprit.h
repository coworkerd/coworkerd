#pragma once

#include "coworkerd.capnp.h"
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>

namespace coworkerd {

// The current CPU watchdog evidence only matches a live dispatched invocation.
inline bool cpuCulpritMatches(std::string_view event, uint32_t reportedPid, uint32_t pid,
    uint32_t reportedLimit, uint32_t limit, uint64_t usedNs, bool dispatched,
    std::string_view reportedWorker, std::string_view worker,
    std::string_view reportedVersion, std::string_view version) {
  return limit && event == "culprit" && reportedPid == pid && reportedLimit == limit &&
      usedNs >= uint64_t(limit) * 1000000 && dispatched &&
      reportedWorker == worker && reportedVersion == version;
}

// Validate both active and recently completed invocations before attribution.
template <typename Process, typename Limits, typename Labelled, typename Confirmed>
std::string confirmedCpuCulprit(Process& p, Limits limits, Labelled labelled, Confirmed confirmed) {
  p.pullEvents();
  const auto row = p.culpritRow;
  if (row.empty() || row.size() > 16384) return "null";
  try {
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<v1::rpc::CpuReport>();
    capnp::JsonCodec().decode(kj::StringPtr(row.c_str()), value);
    auto call = p.calls.find(value.getInvocationId().cStr());
    // The limit this Worker's executions run under (its own where it names one): the row has to be
    // against that limit, and to have spent it.
    const std::string worker = value.getWorkerId().cStr(), version = value.getVersionId().cStr();
    const bool recent = p.threaded && std::any_of(p.recentDispatched.begin(), p.recentDispatched.end(), [&](const auto& completed) {
      return completed.workerId == worker && completed.versionId == version && completed.invocationId == value.getInvocationId().cStr();
    });
    const auto limit = call == p.calls.end() ? (recent ? limits(worker, p.durable).cpuMs : 0u) : limits(call->second->workerId, p.durable).cpuMs;
    if (!limit) return "null";
    const bool live = call != p.calls.end();
    if (!cpuCulpritMatches(value.getEvent().cStr(), value.getPid(), static_cast<uint32_t>(p.pid),
        value.getCpuLimitMs(), limit, value.getCpuUsedNs(), live ? call->second->dispatched : recent,
        worker, live ? call->second->workerId : worker, version, live ? call->second->versionId : version)) return "null";
    if (!labelled(p, value.getIsolate().cStr(), worker, version)) return "null";
    confirmed(worker);
    return capnp::JsonCodec().encode(value.asReader()).cStr();
  } catch (const kj::Exception&) { return "null"; }
}

// The last allocator's heap sample cannot prove ownership of a shared-cage OOM,
// even when the sample reaches the Worker's limit.
inline bool memoryCulpritOwnsOom(v1::rpc::CpuReport::Reader report, bool durable) {
  const bool heapReported = report.getOomIsolateHeapReported();
  const auto limit = report.getMemoryLimitMiB();
  const bool heapAtLimit = report.getOomIsolateHeapBytes() >= (uint64_t(limit) << 20);
  if (durable) return !heapReported || heapAtLimit;
  return report.getOomIsolateLocal() && heapReported && limit > 0 && heapAtLimit;
}

// A fatal row can match a completed dispatch in the same process generation.
template<class Calls, class Recent>
bool crashCulpritDispatch(const Calls& live, const Recent& recent, const std::string& invocation,
    const std::string& worker, const std::string& version) {
  auto current = live.find(invocation);
  if (current != live.end() && current->second->dispatched &&
      current->second->workerId == worker && current->second->versionId == version) return true;
  return std::any_of(recent.begin(), recent.end(), [&](const auto& call) {
    return call.invocationId == invocation && call.workerId == worker && call.versionId == version;
  });
}

// The inherited event pipe belongs to one process generation. The isolate label
// is formed by the kernel, including for a cold load before its first lifecycle snapshot.
template <typename Process, typename Confirmed>
std::string confirmedFatalCulprit(const Process& p, bool fatalExit, Confirmed confirmed) {
  if (!fatalExit || p.fatalCulpritRow.empty() || p.fatalCulpritRow.size() >= 4096) return "null";
  try {
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<v1::rpc::CpuReport>();
    capnp::JsonCodec().decode(kj::StringPtr(p.fatalCulpritRow.c_str()), value);
    if (value.getEvent() != "v8-fatal" || value.getPid() != static_cast<uint32_t>(p.pid)) return "null";
    const std::string worker = value.getWorkerId().cStr(), version = value.getVersionId().cStr();
    const std::string invocation = value.getInvocationId().cStr();
    if (worker.empty() || version.empty() || invocation.empty() ||
        !crashCulpritDispatch(p.calls, p.recentDispatched, invocation, worker, version)) return "null";
    const std::string prefix = "coworkerd-v1:" + worker + "@" + version + "#";
    std::string_view isolate = value.getIsolate().cStr();
    if (!isolate.starts_with(prefix)) return "null";
    isolate.remove_prefix(prefix.size());
    if (isolate.size() != 64 || !std::all_of(isolate.begin(), isolate.end(), [](char digit) {
          return (digit >= '0' && digit <= '9') || (digit >= 'a' && digit <= 'f');
        })) return "null";
    auto row = capnp::JsonCodec().encode(value.asReader());
    confirmed(worker, invocation);
    return row.cStr();
  } catch (const kj::Exception&) { return "null"; }
}

}  // namespace coworkerd
