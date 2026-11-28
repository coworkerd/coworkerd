#pragma once

#include <algorithm>
#include <array>
#include <span>
#include <cstdint>
#include <limits>
#include <optional>
#include <string_view>

namespace coworkerd {

// Leave one eighth of the usable resource for work from admitted trees.
inline constexpr uint64_t nestedCallReserve(uint64_t available) {
  return available / 8 + (available % 8 != 0);
}

inline constexpr uint32_t MAX_ORDINARY_CALL_CEILING = 1048576;
inline constexpr uint64_t CALL_THREAD_STACK_BYTES = 256ull << 10;

enum class CallResource {
  NodeMemory, NodeTasks, NodeDescriptors, ProcessMemory, ProcessTasks, ProcessDescriptors, ProcessCeiling,
};

constexpr std::string_view callResourceName(CallResource resource) {
  switch (resource) {
    case CallResource::NodeMemory: return "node-memory";
    case CallResource::NodeTasks: return "node-tasks";
    case CallResource::NodeDescriptors: return "node-descriptors";
    case CallResource::ProcessMemory: return "process-memory";
    case CallResource::ProcessTasks: return "process-tasks";
    case CallResource::ProcessDescriptors: return "process-descriptors";
    case CallResource::ProcessCeiling: return "process-ceiling";
  }
  return {};
}

constexpr auto CALL_RESOURCES = std::array{
  CallResource::NodeMemory, CallResource::NodeTasks, CallResource::NodeDescriptors,
  CallResource::ProcessMemory, CallResource::ProcessTasks, CallResource::ProcessDescriptors, CallResource::ProcessCeiling
};

constexpr std::optional<CallResource> parseCallResource(std::string_view name) {
  for (const auto resource : CALL_RESOURCES) if (callResourceName(resource) == name) return resource;
  return std::nullopt;
}

struct CallResourceLevel {
  CallResource resource;
  uint64_t current;
  uint64_t limit;
  uint64_t capacity;
};

// The leading refusal byte has been read. Treat a partial or invalid proof as an uncertain result.
inline std::optional<CallResourceLevel> parseOrdinaryResourceRefusal(std::span<const char, 17> bytes) {
  CallResource kind;
  switch (bytes[0]) {
    case 'M': kind = CallResource::ProcessMemory; break;
    case 'T': kind = CallResource::ProcessTasks; break;
    case 'F': kind = CallResource::ProcessDescriptors; break;
    case 'C': kind = CallResource::ProcessCeiling; break;
    default: return std::nullopt;
  }
  auto decode = [](std::span<const char, 8> wire) {
    uint64_t value = 0;
    for (const auto byte : wire) value = (value << 8) | static_cast<unsigned char>(byte);
    return value;
  };
  const auto current = decode(bytes.subspan<1, 8>());
  const auto budget = decode(bytes.subspan<9, 8>());
  if (budget == 0 && current != 0) return std::nullopt;
  return CallResourceLevel{kind, current, budget, 0};
}

struct CallResourceBudget {
  CallResource resource;
  uint64_t baseline;
  uint64_t capacity;  // Zero when there is no usable reading.
  uint64_t perCall;
  uint64_t observed = 0;
};

inline std::optional<CallResourceLevel> callResourceRefusal(
    const CallResourceBudget& budget, uint64_t active, bool root, bool alreadyCharged) {
  if (budget.capacity == 0) return std::nullopt;
  const auto available = budget.capacity - std::min(budget.baseline, budget.capacity);
  const auto reserve = root ? std::min(nestedCallReserve(available), available - std::min(available, budget.perCall)) : 0;
  const auto limit = budget.capacity - reserve;
  const auto calls = active - (alreadyCharged && active > 0);
  const auto maximum = std::numeric_limits<uint64_t>::max();
  const auto accounted = budget.perCall > 0 && calls > (maximum - budget.baseline) / budget.perCall ?
      maximum : budget.baseline + calls * budget.perCall;
  // The latest observation may include this call, but a reserved slot still must fit within the resource boundary.
  const auto observed = budget.observed - (alreadyCharged ? std::min(budget.observed, budget.perCall) : 0);
  const auto current = std::max(accounted, observed);
  if (current <= limit && budget.perCall <= limit - current) return std::nullopt;
  return CallResourceLevel{budget.resource, current, limit, budget.capacity};
}

}  // namespace coworkerd
