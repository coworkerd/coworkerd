#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
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

// The forked log writers' dictionary of secret values. Both daemons create it
// before their log writers fork; the daemon's event loop is its only writer
// (node/secret-dictionary.h decides what it holds). Two regions: an addition is
// appended to the active one; a rebuild, which drops values, writes the other
// one and then makes it active, and a sequence number tells a writer that
// copied a region while it was rewritten to copy it again. A region that cannot
// take an addition refuses it (false, nothing added): the caller then does not
// send the value anywhere, so the dictionary's size never silences a writer.
void preparePlatformLogCleaning();
bool appendPlatformLogRedactions(const std::vector<std::string>& needles);
bool replacePlatformLogRedactions(const std::vector<std::string>& needles);
struct PlatformLogRedactionUsage {
  size_t bytes = 0, patterns = 0, capacityBytes = 0;
  uint64_t generation = 0;
};
PlatformLogRedactionUsage platformLogRedactionUsage();
// What one needle takes in a region.
constexpr size_t platformLogNeedleBytes(size_t size) { return size + sizeof(uint32_t); }
// A value leaves the shared dictionary this long after nothing on the node can
// print it any more (node/secret-dictionary.h). A writer drops a queued line
// older than LOG_LINE_STALE_MS instead of cleaning it late, counted as stale:
// the margin covers the time between a process printing a line and the node
// queuing it.
constexpr double SECRET_RELEASE_GRACE_MS = 60000;
constexpr uint32_t LOG_LINE_STALE_MS = 50000;
static_assert(LOG_LINE_STALE_MS + 10000 <= SECRET_RELEASE_GRACE_MS);

// Used only by the log writers, never by the pipe reader or request threads.
// Keep possible secret prefixes across records, pipe chunks and newlines.
class PlatformLogCleaner {
public:
  std::string append(std::string_view input, bool final = false);
  bool suppressed() const { return blocked; }
private:
  bool refresh();
  void addNeedle(std::string needle);
  void scan(size_t pattern, size_t begin);
  uint64_t generation = UINT64_MAX;
  size_t consumed = 0;
  std::vector<std::string> needles;
  std::string pending;
  std::vector<std::vector<uint32_t>> failures;
  std::vector<size_t> states;
  std::vector<uint8_t> marks;
  bool emittingRedaction = false;
  bool blocked = false;
  bool rescanAll = false;
};

// The values of every secret in this Server's deployment view, hidden by every cleaning;
// ServerCore provides them (the tools have none), and the earlier values some
// isolate on this node may still hold (node/secret-dictionary.h). Rebuilt when
// the view's revision or generation, or the earlier values, change. Used on the
// event loop's thread only. The forked log writers read the shared dictionary.
struct SecretRedactions {
  std::function<std::pair<uint64_t, uint64_t>()> stamp;
  std::function<std::vector<std::string>()> values;
  std::function<uint64_t()> earlierRevision;
  std::function<std::vector<std::string>()> earlierValues;
  std::pair<uint64_t, uint64_t> builtFor{UINT64_MAX, UINT64_MAX};
  uint64_t earlierFor = UINT64_MAX;
  std::vector<std::string> cached;
  const std::vector<std::string>& needles() {
    if (!stamp) { cached.clear(); builtFor = {UINT64_MAX, UINT64_MAX}; earlierFor = UINT64_MAX; return cached; }
    const auto now = stamp();
    const auto earlier = earlierRevision ? earlierRevision() : 0;
    if (now != builtFor || earlier != earlierFor) {
      cached.clear();
      for (const auto& value : values()) if (!value.empty()) addRedaction(cached, value);
      if (earlierValues) for (const auto& value : earlierValues()) if (!value.empty()) addRedaction(cached, value);
      sortRedactions(cached);
      builtFor = now;
      earlierFor = earlier;
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
