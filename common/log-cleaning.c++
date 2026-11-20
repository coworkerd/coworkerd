#include "log-cleaning.h"
#include "support.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string_view>
#include <sys/mman.h>
#include <tuple>
#include <kj/debug.h>
#include <unordered_set>
#include <unistd.h>

namespace coworkerd {

// A value and its JSON-escaped form, the forms a reason or a log line can hold.
void addRedaction(std::vector<std::string>& needles, const std::string& value) {
  needles.push_back(value);
  if (auto escaped = jsonEscape(value); escaped != value) needles.push_back(escaped);
}
void sortRedactions(std::vector<std::string>& needles) {
  std::sort(needles.begin(), needles.end(), [](const auto& a, const auto& b) { return a.size() > b.size(); });
}
// Every occurrence of each needle (longest first) replaced.
std::string redactNeedles(std::string text, const std::vector<std::string>& needles) {
  const std::string mark = "[redacted]";
  for (const auto& needle : needles) {
    if (needle.size() > text.size()) continue;
    for (auto at = text.find(needle); at != std::string::npos; at = text.find(needle, at + mark.size())) {
      text.replace(at, needle.size(), mark);
    }
  }
  return text;
}

namespace {
// Each region holds at most twice the node's dictionary limit
// (node/secret-dictionary.h): the limit is checked when a call is admitted, and
// a value that changed before its descriptor was built is still added.
constexpr size_t LOG_REDACTION_REGION_BYTES = 32 * 1024 * 1024;
constexpr size_t LOG_PENDING_BYTES = 64 * 1024;
constexpr size_t LOG_BLOCK_BYTES = 8 * 1024 * 1024;
struct SharedLogRedactions {
  // The active region is generation % 2. A region's sequence is odd while the
  // writer rebuilds it; `published` bytes of it are complete.
  std::atomic<uint64_t> generation{0};
  struct Region {
    std::atomic<uint64_t> sequence{0};
    std::atomic<size_t> published{0};
    char bytes[LOG_REDACTION_REGION_BYTES];
  } regions[2];
};
static_assert(std::atomic<uint64_t>::is_always_lock_free);
static_assert(std::atomic<size_t>::is_always_lock_free);
struct LogRedactions {
  SharedLogRedactions* shared;
  // The writer's own record of the active region (the daemon's event loop only).
  std::unordered_set<std::string> active;
  LogRedactions() {
    auto mapping = mmap(nullptr, sizeof(SharedLogRedactions), PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANON, -1, 0);
    if (mapping == MAP_FAILED) throw std::runtime_error("could not create diagnostic redactions");
    shared = new (mapping) SharedLogRedactions;
  }
};
LogRedactions& logRedactions() {
  // The mapping must outlive the daemon's log objects and their forked writers.
  static auto* value = new LogRedactions;
  return *value;
}
size_t writeNeedle(char* at, const std::string& needle) {
  const auto size = static_cast<uint32_t>(needle.size());
  std::memcpy(at, &size, sizeof(size));
  std::memcpy(at + sizeof(size), needle.data(), size);
  return platformLogNeedleBytes(size);
}
}

void preparePlatformLogCleaning() { (void)logRedactions(); }

bool appendPlatformLogRedactions(const std::vector<std::string>& needles) {
  auto& owner = logRedactions();
  auto& region = owner.shared->regions[owner.shared->generation.load(std::memory_order_relaxed) % 2];
  const auto used = region.published.load(std::memory_order_relaxed);
  std::vector<const std::string*> added;
  std::unordered_set<std::string_view> batch;
  size_t bytes = 0;
  for (const auto& needle : needles) {
    if (needle.empty() || owner.active.contains(needle) || !batch.insert(needle).second) continue;
    if (needle.size() > PLATFORM_LOG_NEEDLE_MAX_BYTES) return false;
    bytes += platformLogNeedleBytes(needle.size());
    added.push_back(&needle);
  }
  if (added.empty()) return true;
  if (bytes > LOG_REDACTION_REGION_BYTES - used) return false;
  size_t at = used;
  for (const auto* needle : added) at += writeNeedle(region.bytes + at, *needle);
  for (const auto* needle : added) owner.active.insert(*needle);
  // Complete before a reader sees it, and before any descriptor carries it.
  region.published.store(at, std::memory_order_release);
  return true;
}

bool replacePlatformLogRedactions(const std::vector<std::string>& needles) {
  auto& owner = logRedactions();
  auto* shared = owner.shared;
  std::unordered_set<std::string> kept;
  size_t bytes = 0;
  for (const auto& needle : needles) {
    if (needle.size() > PLATFORM_LOG_NEEDLE_MAX_BYTES) return false;
    if (needle.empty() || kept.contains(needle)) continue;
    bytes += platformLogNeedleBytes(needle.size());
    if (bytes > LOG_REDACTION_REGION_BYTES) return false;
    kept.insert(needle);
  }
  const auto generation = shared->generation.load(std::memory_order_relaxed);
  auto& region = shared->regions[(generation + 1) % 2];
  const auto sequence = region.sequence.load(std::memory_order_relaxed);
  region.sequence.store(sequence + 1, std::memory_order_relaxed);
  std::atomic_thread_fence(std::memory_order_release);
  size_t at = 0;
  for (const auto& needle : kept) at += writeNeedle(region.bytes + at, needle);
  region.published.store(at, std::memory_order_relaxed);
  region.sequence.store(sequence + 2, std::memory_order_release);
  shared->generation.store(generation + 1, std::memory_order_release);
  owner.active = std::move(kept);
  return true;
}

PlatformLogRedactionUsage platformLogRedactionUsage() {
  auto& owner = logRedactions();
  const auto generation = owner.shared->generation.load(std::memory_order_relaxed);
  return {owner.shared->regions[generation % 2].published.load(std::memory_order_relaxed), owner.active.size(),
      LOG_REDACTION_REGION_BYTES, generation};
}

namespace {
constexpr uint32_t NEXT_IS_CHILD = 1u << 31;
}

void NeedleAutomaton::build(const std::vector<std::string_view>& input) {
  std::vector<std::string_view> sorted;
  for (auto needle : input) if (!needle.empty()) sorted.push_back(needle);
  std::sort(sorted.begin(), sorted.end());
  sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());
  label.assign(1, 0); fail.assign(1, 0); extra.assign(1, 0); nodeDepth.assign(1, 0); nodeMatch.assign(1, 0);
  branches.clear(); edges.clear();
  std::fill(std::begin(root), std::end(root), 0u);
  count = sorted.size();
  size_t total = 1;
  for (auto needle : sorted) total += needle.size();
  label.reserve(total); fail.reserve(total); extra.reserve(total); nodeDepth.reserve(total); nodeMatch.reserve(total);
  // Sorted, a needle shares its longest common prefix among the earlier ones with the one before it, so it starts
  // from that one's path; its own nodes follow one another.
  std::vector<std::tuple<uint32_t, uint8_t, uint32_t>> more;  // (parent, byte, child) beside a consecutive child
  std::vector<uint32_t> path{0};
  std::string_view previous;
  for (auto needle : sorted) {
    KJ_REQUIRE(needle.size() <= PLATFORM_LOG_NEEDLE_MAX_BYTES);
    size_t common = 0;
    while (common < previous.size() && common < needle.size() && previous[common] == needle[common]) ++common;
    path.resize(common + 1);
    uint32_t node = path.back();
    for (size_t at = common; at < needle.size(); ++at) {
      const auto created = static_cast<uint32_t>(label.size());
      const auto byte = static_cast<uint8_t>(needle[at]);
      label.push_back(byte); fail.push_back(0); extra.push_back(0);
      nodeDepth.push_back(static_cast<uint16_t>(at + 1)); nodeMatch.push_back(0);
      if (node == 0) root[byte] = created;
      else if (created == node + 1) extra[node] |= NEXT_IS_CHILD;
      else more.emplace_back(node, byte, created);
      node = created;
      path.push_back(node);
    }
    nodeMatch[node] = static_cast<uint16_t>(needle.size());
    previous = needle;
  }
  std::sort(more.begin(), more.end());
  for (size_t at = 0; at < more.size();) {
    const auto parent = std::get<0>(more[at]);
    const auto first = static_cast<uint32_t>(edges.size());
    for (; at < more.size() && std::get<0>(more[at]) == parent; ++at) edges.emplace_back(std::get<1>(more[at]), std::get<2>(more[at]));
    branches.emplace_back(first, static_cast<uint32_t>(edges.size()) - first);
    extra[parent] |= static_cast<uint32_t>(branches.size());
  }
  // Failure links breadth first: a node's falls back from its parent's.
  std::vector<uint32_t> queue;
  queue.reserve(label.size());
  for (auto node : root) if (node) queue.push_back(node);
  for (size_t at = 0; at < queue.size(); ++at) {
    const auto node = queue[at];
    auto visit = [&](uint32_t next) {
      const auto byte = label[next];
      uint32_t back = fail[node];
      while (true) {
        const auto found = child(back, byte);
        if (found && found != next) { fail[next] = found; break; }
        if (back == 0) { fail[next] = 0; break; }
        back = fail[back];
      }
      if (nodeMatch[next] == 0) nodeMatch[next] = nodeMatch[fail[next]];
      queue.push_back(next);
    };
    if (extra[node] & NEXT_IS_CHILD) visit(node + 1);
    if (const auto index = extra[node] & ~NEXT_IS_CHILD) {
      const auto [first, size] = branches[index - 1];
      for (uint32_t edge = first; edge < first + size; ++edge) visit(edges[edge].second);
    }
  }
}

uint32_t NeedleAutomaton::child(uint32_t node, uint8_t byte) const {
  if (node == 0) return root[byte];
  if ((extra[node] & NEXT_IS_CHILD) && label[node + 1] == byte) return node + 1;
  if (const auto index = extra[node] & ~NEXT_IS_CHILD) {
    const auto [first, size] = branches[index - 1];
    for (uint32_t edge = first; edge < first + size; ++edge)
      if (edges[edge].first == byte) return edges[edge].second;
  }
  return 0;
}

uint32_t NeedleAutomaton::step(uint32_t state, uint8_t byte) const {
  while (true) {
    if (const auto next = child(state, byte)) return next;
    if (state == 0) return 0;
    state = fail[state];
  }
}

size_t NeedleAutomaton::bytes() const {
  return label.capacity() + (fail.capacity() + extra.capacity()) * sizeof(uint32_t) +
      (nodeDepth.capacity() + nodeMatch.capacity()) * sizeof(uint16_t) +
      branches.capacity() * sizeof(branches[0]) + edges.capacity() * sizeof(edges[0]) + sizeof(root);
}

void PlatformLogCleaner::rebuildMain() {
  std::vector<std::string_view> all(needles.begin(), needles.end());
  main.build(all);
  recent.build({});
  mainCount = needles.size();
  mainBytes = 0;
  for (const auto& needle : needles) mainBytes += needle.size();
  recentBytes = 0;
  mainState = recentState = 0;
}

void PlatformLogCleaner::rebuildRecent() {
  std::vector<std::string_view> added(needles.begin() + mainCount, needles.end());
  recent.build(added);
  recentState = 0;
}

// The active region's needles, read under its sequence number. A rebuilt
// dictionary replaces every needle; the bytes already marked stay hidden.
// False only while the writer rebuilds faster than this copy completes.
bool PlatformLogCleaner::refresh() {
  auto* shared = logRedactions().shared;
  for (int attempt = 0; attempt < 2000; ++attempt) {
    const auto current = shared->generation.load(std::memory_order_acquire);
    auto& region = shared->regions[current % 2];
    const auto sequence = region.sequence.load(std::memory_order_acquire);
    if (sequence & 1) { usleep(50); continue; }
    const bool reload = current != generation;
    const auto published = region.published.load(std::memory_order_acquire);
    std::vector<std::string> loaded;
    size_t at = reload ? 0 : consumed;
    bool torn = published > LOG_REDACTION_REGION_BYTES;
    while (!torn && at < published) {
      uint32_t size;
      if (published - at < sizeof(size)) { torn = true; break; }
      std::memcpy(&size, region.bytes + at, sizeof(size));
      if (size == 0 || size > published - at - sizeof(size) || size > PLATFORM_LOG_NEEDLE_MAX_BYTES) { torn = true; break; }
      loaded.emplace_back(region.bytes + at + sizeof(size), size);
      at += platformLogNeedleBytes(size);
    }
    std::atomic_thread_fence(std::memory_order_acquire);
    if (torn || region.sequence.load(std::memory_order_relaxed) != sequence ||
        shared->generation.load(std::memory_order_acquire) != current) continue;
    consumed = at;
    if (reload) {
      needles = std::move(loaded);
      generation = current;
      rebuildMain();
      scan(main, mainState, 0);
      return true;
    }
    if (loaded.empty()) return true;
    for (auto& needle : loaded) {
      recentBytes += needle.size();
      needles.push_back(std::move(needle));
    }
    // A new needle is looked for in the bytes held back as well.
    if (recentBytes > std::max<size_t>(64 * 1024, mainBytes / 4)) {
      rebuildMain();
      scan(main, mainState, 0);
    } else {
      rebuildRecent();
      scan(recent, recentState, 0);
    }
    return true;
  }
  return false;
}

// Match against the original stream, including bytes suppressed by a block
// quota. An automaton's state retains only prefixes that can still form a
// value. A match marks its bytes; the run the last match marked is kept so that
// overlapping matches (a long periodic value) mark each byte once.
void PlatformLogCleaner::scan(const NeedleAutomaton& automaton, uint32_t& state, size_t begin) {
  if (automaton.patterns() == 0) return;
  size_t runFrom = 0, runTo = 0;
  for (size_t at = begin; at < pending.size(); ++at) {
    state = automaton.step(state, static_cast<uint8_t>(pending[at]));
    const auto length = automaton.match(state);
    if (!length) continue;
    const size_t from = at + 1 - length, to = at + 1;
    if (runTo <= from || runTo == 0) {
      for (size_t pos = from; pos < to; ++pos) marks[pos] |= 1;
      runFrom = from;
    } else {
      for (size_t pos = from; pos < runFrom; ++pos) marks[pos] |= 1;
      for (size_t pos = runTo; pos < to; ++pos) marks[pos] |= 1;
      runFrom = std::min(runFrom, from);
    }
    runTo = to;
  }
}

std::string PlatformLogCleaner::append(std::string_view input, bool final) {
  blocked = false;
  if (!refresh()) {
    // No consistent copy of the dictionary now (the writer kept rebuilding it):
    // this block is held back unwritten (counted as suppressed), and every
    // needle looks at it again once a copy succeeds.
    blocked = true;
    rescanAll = true;
    pending.append(input);
    marks.resize(pending.size(), 2);
    return {};
  }
  if (rescanAll) {
    rescanAll = false;
    mainState = recentState = 0;
    scan(main, mainState, 0);
    scan(recent, recentState, 0);
  }
  if (needles.empty() && pending.empty()) {
    if (!input.empty()) emittingRedaction = false;
    return std::string(input);
  }

  // Charge each block once, independent of the dictionary's size. Mark dropped
  // bytes so a retained prefix can never reappear in later output.
  blocked = input.size() > LOG_BLOCK_BYTES;
  std::string output;
  do {
    const auto begin = pending.size();
    const auto take = std::min(input.size(), LOG_PENDING_BYTES);
    pending.append(input.substr(0, take));
    marks.resize(pending.size(), blocked ? 2 : 0);
    input.remove_prefix(take);
    scan(main, mainState, begin);
    scan(recent, recentState, begin);
    const size_t hold = std::max(main.patterns() ? main.depth(mainState) : 0, recent.patterns() ? recent.depth(recentState) : 0);
    const auto cut = final && input.empty() ? pending.size() : pending.size() - hold;
    for (size_t at = 0; at < cut; ++at) {
      if (marks[at] & 2) continue;
      if (marks[at] & 1) {
        if (!emittingRedaction && !blocked) output += "[redacted]";
        emittingRedaction = true;
      } else {
        if (!blocked) output += pending[at];
        emittingRedaction = false;
      }
    }
    pending.erase(0, cut);
    marks.erase(marks.begin(), marks.begin() + cut);
  } while (!input.empty());
  if (final) {
    mainState = recentState = 0;
    emittingRedaction = false;
  }
  return output;
}

SecretRedactions& secretRedactions() {
  static SecretRedactions value;
  return value;
}

// Every occurrence of a value (and of its JSON-escaped form) replaced. Secret
// values are hidden whatever their length; variable values from 4 bytes on.
// The values of every secret this Server knows are hidden as well.
std::string trialRedact(std::string text, const std::vector<std::string>& secrets, const std::vector<std::string>& variables) {
  std::vector<std::string> needles = secretRedactions().needles();
  for (const auto& value : secrets) if (!value.empty()) addRedaction(needles, value);
  for (const auto& value : variables) if (value.size() >= 4) addRedaction(needles, value);
  sortRedactions(needles);
  return redactNeedles(std::move(text), needles);
}

std::string redactSecrets(std::string text) {
  return redactNeedles(std::move(text), secretRedactions().needles());
}

// The frames of a JavaScript stack ("    at fetch (index.js:3:11)") after a description's first line.
std::string withoutStackFrames(const std::string& text) {
  if (text.find('\n') == std::string::npos) return text;
  std::string out;
  for (size_t start = 0, line = 0; start <= text.size(); ++line) {
    auto end = text.find('\n', start);
    if (end == std::string::npos) end = text.size();
    const auto value = std::string_view(text).substr(start, end - start);
    const auto first = value.find_first_not_of(" \t");
    const bool frame = first != std::string_view::npos && value.substr(first).starts_with("at ");
    if (line == 0 || !frame) {
      if (!out.empty()) out += '\n';
      out += value;
    }
    start = end + 1;
  }
  return out;
}

// Absolute file paths (two or more segments) and file: URLs become "<path>".
std::string trialWithoutPaths(const std::string& text) {
  const std::string_view starts = " \t\n\"'(<[=,", ends = " \t\n\"')>],";
  std::string out;
  for (size_t i = 0; i < text.size();) {
    const bool boundary = i == 0 || starts.find(text[i - 1]) != std::string_view::npos;
    const auto rest = std::string_view(text).substr(i);
    if (boundary && (rest.starts_with("file:") || (rest.size() > 1 && rest[0] == '/' && rest[1] != '/'))) {
      size_t end = i;
      while (end < text.size() && ends.find(text[end]) == std::string_view::npos) ++end;
      const auto token = std::string_view(text).substr(i, end - i);
      if (token.starts_with("file:") || token.find('/', 1) != std::string_view::npos) {
        out += "<path>";
        i = end;
        continue;
      }
    }
    out += text[i++];
  }
  return out;
}

// The quoted input in V8's JSON.parse errors ('... "abc" is not valid JSON').
std::string trialWithoutJsonInput(const std::string& text) {
  if (text.find("not valid JSON") == std::string::npos) return text;
  std::string out;
  bool quoted = false;
  for (char c : text) {
    if (c == '"') { out += quoted ? "…\"" : "\""; quoted = !quoted; continue; }
    if (!quoted) out += c;
  }
  return out;
}

// The last steps of every cleaning: no tenant module name, no control character, at most 400 bytes.
std::string finishClean(std::string text) {
  // A version in the original single-file format runs as this module.
  for (auto at = text.find("__coworkerd_tenant__.js"); at != std::string::npos; at = text.find("__coworkerd_tenant__.js", at))
    text.replace(at, 23, "script");
  for (auto& c : text) if (static_cast<unsigned char>(c) < 0x20) c = ' ';
  if (text.size() > 400) {
    size_t cut = 400;
    while (cut > 0 && (static_cast<unsigned char>(text[cut]) & 0xC0) == 0x80) --cut;
    text = text.substr(0, cut) + "…";
  }
  return text;
}

std::string trialClean(std::string text, const std::vector<std::string>& secrets, const std::vector<std::string>& variables) {
  return finishClean(trialWithoutJsonInput(trialWithoutPaths(trialRedact(std::move(text), secrets, variables))));
}

std::string cleanReason(const std::string& text) {
  return finishClean(trialWithoutJsonInput(trialWithoutPaths(redactNeedles(withoutStackFrames(text), secretRedactions().needles()))));
}
}  // namespace coworkerd
