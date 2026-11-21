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

// The longest needle the shared dictionary takes: a secret value is at most 5 KiB, its JSON-escaped form at most
// six times that.
constexpr size_t PLATFORM_LOG_NEEDLE_MAX_BYTES = 65535;
#define COWORKERD_CLEANER_AUTOMATON 1

// Every needle at once (Aho-Corasick): one step per byte of output whatever the number of needles, where one
// search per needle (KMP) took a step per byte per needle. Nodes are laid out so that a needle's own continuation
// is the next node; only branch points keep a list of further children.
class NeedleAutomaton {
public:
  void build(const std::vector<std::string_view>& needles);
  // The state after one more byte, from a state (0 is the start).
  uint32_t step(uint32_t state, uint8_t byte) const;
  // How many bytes of input the state stands for (the longest held prefix of a needle), and the longest needle
  // that ends where the state stands (0: none).
  uint16_t depth(uint32_t state) const { return nodeDepth[state]; }
  uint16_t match(uint32_t state) const { return nodeMatch[state]; }
  size_t patterns() const { return count; }
  size_t bytes() const;  // memory held
private:
  uint32_t child(uint32_t node, uint8_t byte) const;
  std::vector<uint8_t> label;          // the byte on the edge into the node
  std::vector<uint32_t> fail, extra;   // extra: bit 31 = the next node is a child; the rest = 1 + a branch index
  std::vector<uint16_t> nodeDepth, nodeMatch;
  std::vector<std::pair<uint32_t, uint32_t>> branches;  // (first, count) into edges, sorted by byte
  std::vector<std::pair<uint8_t, uint32_t>> edges;
  uint32_t root[256] = {};
  size_t count = 0;
};

// Used only by the log writers, never by the pipe reader or request threads.
// Keep possible secret prefixes across records, pipe chunks and newlines, per
// stream: a writer of several processes' output keeps one Stream for each of
// their streams, so a value printed over several lines (a PEM key) is still
// found when other processes' lines come in between. The needles are two
// automata the writer's streams share: those of the last full
// load, and those appended since (rebuilt with every addition, merged into the
// first once they reach a quarter of it), so an addition never rebuilds the
// whole dictionary.
class PlatformLogCleaner {
public:
  struct Stream {
    std::string pending;
    std::vector<uint8_t> marks;
    uint32_t mainState = 0, recentState = 0;
    uint64_t mainBuilt = 0, recentBuilt = 0;  // the automata its states belong to
    bool emittingRedaction = false;
    bool rescanAll = false;
  };
  // A stream's next bytes; `final` at its end (all held bytes are written).
  std::string append(Stream& stream, std::string_view input, bool final = false);
  // The cleaner's own stream (a writer of one stream).
  std::string append(std::string_view input, bool final = false) { return append(own, input, final); }
  // Whether the last append dropped its block.
  bool suppressed() const { return blocked; }
  // Readings: the needles held and the memory of their automata.
  size_t patterns() const { return main.patterns() + recent.patterns(); }
  size_t automatonBytes() const { return main.bytes() + recent.bytes(); }
private:
  bool refresh();
  void rebuildRecent();
  void rebuildMain();
  void scan(const NeedleAutomaton& automaton, Stream& stream, uint32_t& state, size_t begin);
  void bringUpToDate(Stream& stream);
  uint64_t generation = UINT64_MAX;
  size_t consumed = 0;
  std::vector<std::string> needles;   // all of them: the first `mainCount` built into `main`, the rest into `recent`
  size_t mainCount = 0, mainBytes = 0, recentBytes = 0;
  NeedleAutomaton main, recent;
  uint64_t mainBuilt = 1, recentBuilt = 1;
  Stream own;
  bool blocked = false;
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
