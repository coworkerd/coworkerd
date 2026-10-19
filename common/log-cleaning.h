#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

// The cleaning of every reason a caller receives and of the log lines that carry one, which both
// daemons apply.
namespace coworkerd {

// A value and its JSON-escaped form, the forms a reason or a log line can hold.
void addRedaction(std::vector<std::string>& needles, const std::string& value);
void sortRedactions(std::vector<std::string>& needles);
// Every occurrence of each needle (longest first) replaced.
std::string redactNeedles(std::string text, const std::vector<std::string>& needles);

// The values of every secret in this Server's deployment view, hidden by every cleaning;
// ServerCore provides them (the tools have none). Rebuilt when the view's revision or generation
// changes. Used on the event loop's thread only.
struct SecretRedactions {
  std::function<std::pair<uint64_t, uint64_t>()> stamp;
  std::function<std::vector<std::string>()> values;
  std::pair<uint64_t, uint64_t> builtFor{UINT64_MAX, UINT64_MAX};
  std::vector<std::string> cached;
  const std::vector<std::string>& needles() {
    if (!stamp) { cached.clear(); builtFor = {UINT64_MAX, UINT64_MAX}; return cached; }
    const auto now = stamp();
    if (now != builtFor) {
      cached.clear();
      for (const auto& value : values()) if (!value.empty()) addRedaction(cached, value);
      sortRedactions(cached);
      builtFor = now;
    }
    return cached;
  }
};
SecretRedactions& secretRedactions();

std::string trialRedact(std::string text, const std::vector<std::string>& secrets, const std::vector<std::string>& variables);
// The secret values only, nothing shortened (KJ's own log lines).
std::string redactSecrets(std::string text);
std::string withoutStackFrames(const std::string& text);
std::string trialWithoutPaths(const std::string& text);
std::string trialWithoutJsonInput(const std::string& text);
std::string finishClean(std::string text);
std::string trialClean(std::string text, const std::vector<std::string>& secrets, const std::vector<std::string>& variables);
// Every refusal or failure reason a caller receives, and every log line that carries one:
// the values of every secret in this Server's deployment view hidden, stack frames dropped, then
// the cleaning of trialClean (paths, JSON input, control characters, at most 400 bytes). Defined
// with trialClean below.
std::string cleanReason(const std::string& text);

}  // namespace coworkerd
