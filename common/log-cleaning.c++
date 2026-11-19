#include "log-cleaning.h"
#include "support.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string_view>
#include <sys/mman.h>
#include <unordered_set>

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
constexpr size_t LOG_REDACTION_BYTES = 16 * 1024 * 1024;
constexpr size_t LOG_REDACTION_PATTERNS = 65536;
constexpr size_t LOG_PENDING_BYTES = 64 * 1024;
constexpr size_t LOG_MATCH_WORK = 8 * 1024 * 1024;
struct SharedLogRedactions {
  std::atomic<size_t> published{0};
  std::atomic<bool> overflow{false};
  char bytes[LOG_REDACTION_BYTES];
};
static_assert(std::atomic<size_t>::is_always_lock_free);
static_assert(std::atomic<bool>::is_always_lock_free);
struct LogRedactions {
  SharedLogRedactions* shared;
  std::unordered_set<std::string> retained;
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
}

void preparePlatformLogCleaning() { (void)logRedactions(); }

void retainPlatformLogRedactions(const std::vector<std::string>& needles) {
  auto& owner = logRedactions();
  if (owner.shared->overflow.load(std::memory_order_acquire)) return;
  try { for (const auto& needle : needles) {
    if (needle.empty() || owner.retained.contains(needle)) continue;
    const auto used = owner.shared->published.load(std::memory_order_relaxed);
    const auto remaining = LOG_REDACTION_BYTES - used;
    if (owner.retained.size() == LOG_REDACTION_PATTERNS || remaining < sizeof(uint32_t) ||
        needle.size() > remaining - sizeof(uint32_t)) {
      owner.shared->overflow.store(true, std::memory_order_release);
      return;
    }
    const auto size = static_cast<uint32_t>(needle.size());
    std::memcpy(owner.shared->bytes + used, &size, sizeof(size));
    std::memcpy(owner.shared->bytes + used + sizeof(size), needle.data(), size);
    owner.retained.insert(needle);
    owner.shared->published.store(used + sizeof(size) + size, std::memory_order_release);
  }} catch (...) {
    owner.shared->overflow.store(true, std::memory_order_release);
    throw;
  }
}

std::string PlatformLogCleaner::append(std::string_view input, bool final) {
  auto* shared = logRedactions().shared;
  if (blocked || shared->overflow.load(std::memory_order_acquire)) {
    blocked = true;
    pending.clear();
    return {};
  }
  const auto published = shared->published.load(std::memory_order_acquire);
  bool changed = false;
  while (consumed < published) {
    uint32_t size;
    std::memcpy(&size, shared->bytes + consumed, sizeof(size));
    needles.emplace_back(shared->bytes + consumed + sizeof(size), size);
    consumed += sizeof(size) + size;
    changed = true;
  }
  if (changed) sortRedactions(needles);
  if (needles.empty()) return std::string(input);
  std::string output;
  do {
    const auto take = std::min(input.size(), LOG_PENDING_BYTES);
    pending.append(input.substr(0, take));
    input.remove_prefix(take);
    size_t hold = 0, work = 0;
    auto charge = [&](size_t bytes) {
      if (bytes > LOG_MATCH_WORK - std::min(work, LOG_MATCH_WORK)) return false;
      work += bytes;
      return true;
    };
    const bool finishing = final && input.empty();
    if (!finishing) for (const auto& needle : needles) {
      const auto possible = std::min(pending.size(), needle.size() - 1);
      if (!charge(possible)) { blocked = true; break; }
      for (auto at = pending.find(needle.front(), pending.size() - possible);
           at != std::string::npos; at = pending.find(needle.front(), at + 1)) {
        const auto length = pending.size() - at;
        if (length <= hold) break;
        if (!charge(length)) { blocked = true; break; }
        if (std::string_view(needle).starts_with(std::string_view(pending).substr(at))) {
          hold = length;
          break;
        }
      }
      if (blocked) break;
    }
    auto cut = pending.size() - hold;
    // A complete match must not straddle the prefix retained for another
    // pattern. Keep it whole until that prefix is resolved by the next chunk.
    bool moved;
    do {
      moved = false;
      for (const auto& needle : needles) {
        if (!charge(pending.size())) { blocked = true; break; }
        for (auto at = pending.find(needle); at != std::string::npos && at < cut;
             at = pending.find(needle, at + 1)) {
          if (at + needle.size() > cut) { cut = at; moved = true; break; }
        }
      }
    } while (moved && !blocked);
    if (blocked || pending.size() - cut > LOG_PENDING_BYTES) {
      blocked = true;
      pending.clear();
      return {};
    }
    output += redactNeedles(pending.substr(0, cut), needles);
    pending.erase(0, cut);
  } while (!input.empty());
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
