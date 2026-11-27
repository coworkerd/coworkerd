#include "isolate-saturation.h"
#include "json.h"

#include <capnp/message.h>

namespace coworkerd {

SaturatedPlans parseIsolateSaturation(std::string_view snapshot) {
  // Use the newest instance of one isolate label. Multiple labels for one plan
  // revision can refer to different resolved bindings, so that plan is ambiguous.
  struct Reading {
    std::string label;
    double instance, throughMs = 0;
    bool busy = false, ambiguous = false;
  };
  std::map<std::string, Reading> readings;
  try {
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(snapshot.data(), snapshot.size()), value);
    const auto residents = json::field(value, "residentWorkers");
    if (!residents.isArray()) return {};
    for (auto row : residents.getArray()) {
      const auto plan = json::field(row, "planRevision");
      const auto instance = json::field(row, "instance");
      const auto label = json::field(row, "isolate");
      if (!plan.isString() || !instance.isNumber() || !label.isString()) continue;
      Reading reading{label.getString().cStr(), instance.getNumber()};
      const auto busy = json::field(row, "isolateBusy");
      if (busy.isObject()) {
        const auto through = json::field(busy, "throughMonotonicMs");
        const auto permille = json::field(busy, "permille");
        if (through.isNumber() && permille.isNumber() &&
            permille.getNumber() >= WORKER_BUSY_PERMILLE && permille.getNumber() <= 1000) {
          reading.busy = true;
          reading.throughMs = through.getNumber();
        }
      }
      auto [previous, inserted] = readings.try_emplace(plan.getString().cStr(), std::move(reading));
      if (!inserted && !previous->second.ambiguous) {
        if (reading.label != previous->second.label) previous->second.ambiguous = true;
        else if (reading.instance > previous->second.instance) previous->second = std::move(reading);
      }
    }
  } catch (const kj::Exception&) {
    return {};
  }
  SaturatedPlans saturated;
  for (const auto& [plan, reading] : readings)
    if (reading.busy && !reading.ambiguous) saturated.emplace(plan, reading.throughMs);
  return saturated;
}

bool hasSaturatedPlan(const SaturatedPlans& saturated, std::string_view planRevision, double nowMs) {
  const auto sample = saturated.find(planRevision);
  return sample != saturated.end() && sample->second <= nowMs && nowMs - sample->second <= 2500;
}

}  // namespace coworkerd
