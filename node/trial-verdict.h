#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace coworkerd {

struct TrialVerdict {
  std::string category;  // empty: the version loads
  std::string limit;     // "cpu" or "memory" for LIMIT_EXCEEDED
  std::string message;
};
inline TrialVerdict classifyTrialFailure(const std::string& stage, const std::string& name, const std::string& raw,
    bool cpuExceeded, bool memoryExceeded, uint32_t cpuLimitMs, uint32_t memoryLimitMiB) {
  if (stage == "bridge") return {"LOAD_FAILED", "",
      "the trial could not reach the runtime: remote exception: remote.jsg.Error: " + raw};
  std::string text = raw;
  for (const char* prefix : {"Failed to start Worker:", "\n", " ", "Uncaught "}) {
    if (text.starts_with(prefix)) text = text.substr(std::string_view(prefix).size());
  }
  std::vector<std::string> lines;
  for (size_t start = 0; start <= text.size();) {
    auto end = text.find('\n', start);
    if (end == std::string::npos) end = text.size();
    auto line = text.substr(start, end - start);
    while (!line.empty() && (line.front() == ' ' || line.front() == '\t')) line.erase(0, 1);
    if (!line.empty()) lines.push_back(line);
    start = end + 1;
  }
  const std::string head = lines.empty() ? std::string() : lines.front();
  // Where it happened: an import or the first frame in the version's own modules.
  std::string where;
  for (size_t i = 1; i < lines.size() && where.empty(); ++i) {
    const auto& line = lines[i];
    if (line.starts_with("imported from ")) where = line;
    else if (line.starts_with("at ") && line.find("node-internal:") == std::string::npos &&
        line.find("__coworkerd_entrypoint__.js") == std::string::npos) where = line;
  }
  auto colon = head.find(": ");
  std::string error = colon == std::string::npos ? std::string() : head.substr(0, colon);
  if (!std::all_of(error.begin(), error.end(), [](unsigned char c) { return std::isalnum(c) || c == '_'; })) error.clear();
  const std::string detail = where.empty() ? head : head + " (" + where + ")";
  if (memoryExceeded || head.find("MEMORY_LIMIT_EXCEEDED") != std::string::npos)
    return {"LIMIT_EXCEEDED", "memory", "memory limit exceeded while loading (" + std::to_string(memoryLimitMiB) + " MiB per isolate)"};
  if (cpuExceeded || head.find("CPU_LIMIT_EXCEEDED") != std::string::npos)
    return {"LIMIT_EXCEEDED", "cpu", "CPU limit exceeded while loading (" + std::to_string(cpuLimitMs) + " ms)"};
  if (stage == "code") {
    // The runtime builds the modules from verified bytes: a JSON module that
    // does not parse or a text module that is not UTF-8 is the version's
    // fault; anything else is this Server failing to hand over the code.
    if (name == "SyntaxError" || name == "TypeError") {
      auto message = text.starts_with(name + ": ") ? text.substr(name.size() + 2) : text;
      return {"INVALID_MODULE", "", "a module cannot be decoded: " + name + ": " + message};
    }
    return {"UNAVAILABLE", "", "the runtime could not read the code: " + text};
  }
  if (head.find("does not provide an export named 'default'") != std::string::npos &&
      head.find("__coworkerd_tenant__.js") != std::string::npos)
    return {"ENTRYPOINT_MISSING", "", "the script has no default export"};
  const std::string missing = "Error: No such module \"";
  if (head.starts_with(missing)) {
    const auto module = head.substr(missing.size(), head.find('"', missing.size()) - missing.size());
    if (module.starts_with("node:"))
      return {"COMPATIBILITY_FLAG", "", detail + "; node:* modules need the nodejs_compat compatibility flag"};
    return {"MISSING_MODULE", "", detail};
  }
  if (error == "SyntaxError") return {"SYNTAX_ERROR", "", detail};
  if (error == "CompileError" || error == "LinkError" || head.find("WebAssembly") != std::string::npos ||
      head.find("WasmModuleObject") != std::string::npos)
    return {"INVALID_MODULE", "", detail};
  if (head == "Error: Top-level await in module is unsettled.")
    return {"TOP_LEVEL_AWAIT", "", "a top-level await never settles (workerd refuses the module while loading)"};
  if (head.empty()) return {"LOAD_FAILED", "", text};
  return {"TOP_LEVEL_THROW", "", detail};
}

// A failed load on the call path (the load check, and a dispatch decision the
// native server confirms): classified at the stage the runtime reported, so a
// bridge failure is not a tenant's top-level throw. Without a reported stage
// the general load classification applies.
inline TrialVerdict classifyLoadRejection(const std::string& reportedStage, const std::string& name,
    const std::string& raw, bool cpuExceeded, bool memoryExceeded, uint32_t cpuLimitMs, uint32_t memoryLimitMiB) {
  return classifyTrialFailure(reportedStage.empty() ? "load" : reportedStage, name, raw, cpuExceeded, memoryExceeded,
      cpuLimitMs, memoryLimitMiB);
}

}  // namespace coworkerd
