#include "async-log.h"
#include "json.h"
#include "log-cleaning.h"
#include "log-sink.h"
#include "secret-dictionary.h"
#include "support.h"

#include <capnp/message.h>
#include <kj/debug.h>
#include <openssl/rand.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <csignal>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <poll.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace coworkerd {
namespace {
struct TempDir {
  std::string path = (std::filesystem::temp_directory_path() / "coworkerd-log-test-XXXXXX").string();
  TempDir() { KJ_REQUIRE(mkdtemp(path.data()) != nullptr); }
  ~TempDir() { std::error_code ignored; std::filesystem::remove_all(path, ignored); }
  std::string file(const char* name) const { return path + '/' + name; }
};
void writeFile(const std::string& path, std::string_view text) {
  const auto fd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  KJ_REQUIRE(fd >= 0);
  size_t at = 0;
  while (at < text.size()) {
    const auto n = write(fd, text.data() + at, text.size() - at);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) { close(fd); KJ_FAIL_REQUIRE("could not write the component fixture"); }
    at += n;
  }
  close(fd);
}
std::string syntheticValue() {
  std::array<unsigned char, 24> bytes;
  KJ_REQUIRE(RAND_bytes(bytes.data(), bytes.size()) == 1);
  std::string value;
  constexpr char hex[] = "0123456789abcdef";
  for (auto byte : bytes) { value += hex[byte >> 4]; value += hex[byte & 15]; }
  return value;
}
// Even a failing test reports only a digest, never a value or the log holding it.
bool containsDigest(std::string_view text, size_t size, const std::string& digest) {
  for (size_t at = 0; at + size <= text.size(); ++at)
    if (sha256Hex(text.substr(at, size)) == digest) return true;
  return false;
}
void requireClean(const std::string& text, const std::string& value) {
  const auto digest = sha256Hex(value);
  const bool leaked = containsDigest(text, value.size(), digest);
  KJ_REQUIRE(!leaked, "diagnostic redaction failed; synthetic value SHA-256", digest.c_str());
}
std::string longValue(size_t size) {
  std::string value;
  while (value.size() < size) value += syntheticValue();
  value.resize(size);
  return value;
}
void streams() {
  const auto value = syntheticValue() + "\n\"\\" + syntheticValue();
  std::vector<std::string> needles;
  addRedaction(needles, value);
  sortRedactions(needles);
  KJ_REQUIRE(appendPlatformLogRedactions(needles));
  const auto escaped = jsonEscape(value);
  for (const auto& pattern : {value, escaped}) {
    for (size_t split = 0; split <= pattern.size(); ++split) {
      PlatformLogCleaner cleaner;
      auto text = cleaner.append("before:" + pattern.substr(0, split));
      text += cleaner.append(pattern.substr(split) + ":after\n", true);
      requireClean(text, pattern);
      const bool exact = text == "before:[redacted]:after\n";
      KJ_REQUIRE(exact, "stream split must preserve surrounding diagnostics", split);
    }
  }
  // A value a rebuilt dictionary keeps still cleans queued output.
  KJ_REQUIRE(replacePlatformLogRedactions(needles));
  PlatformLogCleaner rebuilt;
  const auto old = rebuilt.append(value, true);
  requireClean(old, value);
  const bool replaced = old == "[redacted]";
  KJ_REQUIRE(replaced);
  // A needle the region cannot take is refused (its caller never sends the
  // value), and no writer is silenced by it.
  const bool refused = !appendPlatformLogRedactions({std::string(64 * 1024 * 1024, 'x')});
  PlatformLogCleaner after;
  const auto text = after.append("bounded diagnostic:" + value + "\n", true);
  requireClean(text, value);
  const bool written = text == "bounded diagnostic:[redacted]\n" && !after.suppressed();
  KJ_REQUIRE(refused && written, "a full region must refuse the addition, never silence a writer");
}
// A writer that keeps running across rebuilds: a removed value is written
// again, a kept one stays hidden, and a value split across a rebuild is still
// found in the bytes held back.
void generations() {
  const auto kept = syntheticValue(), removed = syntheticValue(), added = syntheticValue();
  std::vector<std::string> first, second;
  addRedaction(first, kept); addRedaction(first, removed);
  addRedaction(second, kept); addRedaction(second, added);
  KJ_REQUIRE(appendPlatformLogRedactions(first));
  PlatformLogCleaner cleaner;
  const auto before = cleaner.append("a:" + kept + " b:" + removed + "\n");
  requireClean(before, kept); requireClean(before, removed);
  KJ_REQUIRE(before == "a:[redacted] b:[redacted]\n");
  const auto generation = platformLogRedactionUsage().generation;
  const auto split = cleaner.append("split:" + kept.substr(0, 20));
  KJ_REQUIRE(replacePlatformLogRedactions(second));
  const auto joined = split + cleaner.append(kept.substr(20) + ":end\n");
  requireClean(joined, kept);
  KJ_REQUIRE(joined == "split:[redacted]:end\n", "a value split across a rebuild must stay hidden");
  const auto after = cleaner.append("a:" + kept + " b:" + removed + " c:" + added + "\n");
  requireClean(after, kept); requireClean(after, added);
  const bool released = after == "a:[redacted] b:" + removed + " c:[redacted]\n";
  KJ_REQUIRE(released && platformLogRedactionUsage().generation == generation + 1 && !cleaner.suppressed(),
      "a rebuilt dictionary must drop removed values and keep the rest");
  // Many rebuilds in a row, and appends between them, reach the same writer.
  for (int round = 0; round < 64; ++round) {
    const auto value = syntheticValue();
    std::vector<std::string> needles;
    addRedaction(needles, kept); addRedaction(needles, value);
    KJ_REQUIRE(round % 2 ? replacePlatformLogRedactions(needles) : appendPlatformLogRedactions(needles));
    const auto text = cleaner.append("r:" + value + ":" + kept + "\n");
    requireClean(text, value); requireClean(text, kept);
    KJ_REQUIRE(text == "r:[redacted]:[redacted]\n", round);
  }
}
SecretDictionary::Sink sharedSink() { return {appendPlatformLogRedactions, replacePlatformLogRedactions}; }
SecretDictionary::Residents residents(const std::string& process, uint64_t generation, std::set<std::string> revisions,
    bool known = true) {
  return {process, generation, known, std::move(revisions)};
}
// One tenant changes the same secret 70,000 times, 256 bytes each (about
// 18 MB of distinct values, more than the old append-only 16 MiB region held),
// with a call after nearly every change, while its native process keeps its
// last eight isolates, and its first eight all along. The dictionary stays
// bounded, the other tenant's and the platform's lines are written as before, an
// earlier value goes once nothing holds it, and the values the still-resident
// isolates hold stay hidden, long past their calls' deadlines and the grace.
void secretRotations() {
  SecretDictionary dictionary({}, sharedSink());
  PlatformLogCleaner cleaner;
  const auto quiet = syntheticValue();
  const std::string platform = "[server] an ordinary platform line\n";
  constexpr uint32_t ROTATIONS = 70000, RESIDENT = 8;
  std::deque<std::pair<std::string, std::string>> resident;  // (revision, value), newest last
  std::vector<std::pair<std::string, std::string>> first;    // the first eight, resident all along
  std::set<std::string> pinned;
  std::deque<std::string> history;
  double now = 0;
  size_t largest = 0, sharedLargest = 0;
  uint64_t admitted = 0;
  for (uint32_t rotation = 0; rotation < ROTATIONS; ++rotation) {
    now += 100;
    const auto value = longValue(256);
    dictionary.setCurrent({{"busy", {value}}, {"quiet", {quiet}}});
    const bool called = rotation % 7 != 3;  // every seventh value is never sent anywhere
    if (called) {
      KJ_REQUIRE(!dictionary.admit("busy", {value}, now, now + 30000), rotation);
      const auto revision = "busy-" + std::to_string(rotation);
      KJ_REQUIRE(dictionary.sent("busy", {value}, "p0", 1, revision, true, now, now + 30000));
      resident.emplace_back(revision, value);
      if (resident.size() > RESIDENT) resident.pop_front();
      if (first.size() < RESIDENT) {
        first.emplace_back(revision, value);
        pinned.insert(value);
      }
      ++admitted;
    } else {
      KJ_REQUIRE(!dictionary.holds(value), "a value no call carried is not in the dictionary", rotation);
    }
    if (rotation % 50 == 0) {
      KJ_REQUIRE(!dictionary.admit("quiet", {quiet}, now, now + 30000));
      KJ_REQUIRE(dictionary.sent("quiet", {quiet}, "p0", 1, "quiet", true, now, now + 30000));
    }
    // The node collects once a second (every ten changes here).
    if (rotation % 10 == 9) {
      std::set<std::string> revisions{"quiet"};
      for (const auto& [revision, kept] : resident) revisions.insert(revision);
      for (const auto& [revision, kept] : first) revisions.insert(revision);
      dictionary.collect(now, {residents("p0", 1, revisions)});
    }
    largest = std::max(largest, dictionary.bytes());
    sharedLargest = std::max(sharedLargest, platformLogRedactionUsage().bytes);
    history.push_back(value);
    if (history.size() > 2000) history.pop_front();
    if (rotation % 1000 == 999) {
      const auto line = cleaner.append(platform);
      KJ_REQUIRE(line == platform && !cleaner.suppressed(), "a platform line must pass unchanged", rotation);
      const auto other = cleaner.append("quiet:" + quiet + "\n");
      requireClean(other, quiet);
      KJ_REQUIRE(other == "quiet:[redacted]\n", "another tenant's value must stay hidden", rotation);
      for (const auto& [revision, kept] : resident) {
        const auto text = cleaner.append("resident:" + kept + "\n");
        requireClean(text, kept);
        KJ_REQUIRE(text == "resident:[redacted]\n", "a value a resident isolate holds must stay hidden", rotation);
      }
      for (const auto& [revision, kept] : first) {
        const auto text = cleaner.append("first:" + kept + "\n");
        requireClean(text, kept);
        KJ_REQUIRE(dictionary.holds(kept) && text == "first:[redacted]\n",
            "a value an isolate resident all along holds must stay hidden past its deadline and the grace", rotation);
      }
      // 2,000 changes (200 s) ago: not resident, past its deadline and the grace.
      const auto& gone = history.front();
      if (!pinned.contains(gone)) {
        const auto text = cleaner.append("gone:" + gone + "\n");
        KJ_REQUIRE(!dictionary.holds(gone) && text == "gone:" + gone + "\n", "an unused earlier value must leave", rotation);
      }
    }
  }
  // Holders: at most 16 resident isolates, and the calls of the last 30 s plus
  // the 60 s grace (about 900 changes), each 2 x 260 bytes at most.
  const bool bounded = largest <= 1000 * 2 * 260 + 1024 && sharedLargest <= 2 * (1000 * 2 * 260 + 1024);
  const auto state = dictionary.json(now);
  KJ_REQUIRE(bounded && admitted > 59000 && state.find("\"refused\":{\"full\":0,\"share\":0,\"earlier\":0,\"cooled\":0}") != std::string::npos,
      "the dictionary must stay bounded and never refuse", largest, sharedLargest);
}
// The two limits, with small values: a tenant past its earlier-value limit is
// refused (and recovers once its isolates are gone), a call that does not fit
// in the node's total is refused while the tenants already in keep running,
// what a process that names no isolates got stays until the generation is
// gone, and a descriptor the region cannot hold is never sent.
void secretLimits() {
  SecretDictionary::Limits limits;
  limits.nodeBytes = 64 * 1024;
  limits.nodePatterns = 1000;
  limits.tenantEarlierBytes = 10 * 1024;
  SecretDictionary dictionary(limits, sharedSink());
  PlatformLogCleaner cleaner;
  double now = 1000;
  std::map<std::string, std::set<std::string>> current;
  // 1. Earlier values of one tenant.
  const auto steady = longValue(512);
  current["steady"] = {steady};
  std::vector<std::string> rotated;
  std::set<std::string> revisions;
  for (int rotation = 0; rotation < 3; ++rotation) {
    rotated.push_back(longValue(4096));
    current["rotating"] = {rotated.back()};
    dictionary.setCurrent(current);
    KJ_REQUIRE(!dictionary.admit("rotating", {rotated.back()}, now, now + 1000), rotation);
    const auto revision = "r" + std::to_string(rotation);
    KJ_REQUIRE(dictionary.sent("rotating", {rotated.back()}, "p0", 1, revision, true, now, now + 1000));
    revisions.insert(revision);
    now += 2000;
    dictionary.collect(now, {residents("p0", 1, revisions)});
  }
  rotated.push_back(longValue(4096));
  current["rotating"] = {rotated.back()};
  dictionary.setCurrent(current);
  // Three earlier values of 4 KiB, all in resident isolates: past 10 KiB.
  const auto refused = dictionary.admit("rotating", {rotated.back()}, now, now + 1000);
  KJ_REQUIRE(refused.has_value() && refused->find("earlier secret values") != std::string::npos &&
      refused->find("within about ") != std::string::npos && refused->find(rotated[0].substr(0, 16)) == std::string::npos,
      "a tenant past its earlier-value limit must be refused, with the reason and when it recovers");
  KJ_REQUIRE(!dictionary.admit("steady", {steady}, now, now + 1000), "another tenant must not be refused");
  for (size_t i = 0; i < 3; ++i) {
    const auto text = cleaner.append("held:" + rotated[i] + "\n");
    requireClean(text, rotated[i]);
    KJ_REQUIRE(text == "held:[redacted]\n", "values resident isolates hold stay hidden while refused", i);
  }
  // The in-process cleaning (reasons, the node's own lines) hides them too.
  auto& redactions = secretRedactions();
  redactions.stamp = [] { return std::pair<uint64_t, uint64_t>{1, 1}; };
  redactions.values = [&] { return std::vector<std::string>{rotated.back()}; };
  redactions.earlierRevision = [&] { return dictionary.earlierRevision(); };
  redactions.earlierValues = [&] { return dictionary.earlierValues(); };
  const auto reason = cleanReason("failed with " + rotated[1]);
  requireClean(reason, rotated[1]);
  KJ_REQUIRE(reason == "failed with [redacted]", "an earlier value a resident isolate holds is hidden in reasons");
  // The isolates are evicted: after the grace the tenant may call again.
  dictionary.collect(now, {residents("p0", 1, {})});
  now += limits.graceMs;
  dictionary.collect(now, {residents("p0", 1, {})});
  KJ_REQUIRE(dictionary.earlierBytes("rotating") == 0 && !dictionary.admit("rotating", {rotated.back()}, now, now + 1000),
      "a tenant must recover once nothing holds its earlier values");
  const auto earlierReason = cleanReason("failed with " + rotated[1]);
  KJ_REQUIRE(earlierReason.find(rotated[1].substr(0, 300)) != std::string::npos, "a released value is no longer hidden");
  redactions.stamp = nullptr; redactions.values = nullptr; redactions.earlierRevision = nullptr; redactions.earlierValues = nullptr;
  now += 2000;
  dictionary.collect(now, {});
  // 2. The node's total: 15 KiB values of new tenants (each within its 16 KiB share) until one does not fit.
  std::vector<std::string> big;
  std::optional<std::string> full;
  size_t fitted = 0;
  for (int tenant = 0; tenant < 8 && !full; ++tenant) {
    big.push_back(longValue(15 * 1024));
    current["t" + std::to_string(tenant)] = {big.back()};
    dictionary.setCurrent(current);
    full = dictionary.admit("t" + std::to_string(tenant), {big.back()}, now, now + 1000);
    if (!full) ++fitted;
  }
  KJ_REQUIRE(full.has_value() && full->find("secret dictionary is full") != std::string::npos && fitted >= 2 &&
      dictionary.bytes() <= limits.nodeBytes, "a call that does not fit must be refused", fitted);
  KJ_REQUIRE(!dictionary.admit("t0", {big[0]}, now, now + 1000), "a tenant already in keeps running");
  const auto state = dictionary.json(now);
  KJ_REQUIRE(state.find("\"refused\":{\"full\":1,\"share\":0,\"earlier\":1,\"cooled\":0}") != std::string::npos &&
      state.find("\"limitBytes\":65536") != std::string::npos, "the node state shows usage and refusals");
  // The others' reservations end: after the grace the refused tenant fits.
  now += 1000;
  dictionary.collect(now, {});
  now += limits.graceMs;
  dictionary.collect(now, {});
  const auto last = "t" + std::to_string(fitted);
  KJ_REQUIRE(!dictionary.admit(last, {big.back()}, now, now + 1000), "a refused tenant fits once others are gone");
  // 3. A process that names no isolates keeps what it got until it is gone: `durable` long past its call's
  // deadline and the grace, `lasting` (its call's deadline an hour away) no longer once the generation is gone.
  const auto durable = syntheticValue(), lasting = syntheticValue();
  current["durable"] = {durable, lasting};
  dictionary.setCurrent(current);
  KJ_REQUIRE(dictionary.sent("durable", {durable}, "d0", 4, "a", false, now, now + 1000));
  KJ_REQUIRE(dictionary.sent("durable", {lasting}, "d0", 4, "b", false, now, now + 3600000));
  current["durable"] = {syntheticValue()};
  dictionary.setCurrent(current);
  for (int pass = 0; pass < 2; ++pass) {
    now += 10 * limits.graceMs;
    dictionary.collect(now, {residents("d0", 4, {}, false)});
    KJ_REQUIRE(dictionary.holds(durable) && dictionary.holds(lasting) && dictionary.earlierBytes("durable") > 0,
        "an unknown process keeps its values past their calls' deadlines and the grace", pass);
  }
  dictionary.collect(now, {residents("d0", 5, {}, false)});
  now += limits.graceMs + 1000;
  dictionary.collect(now, {residents("d0", 5, {}, false)});
  KJ_REQUIRE(!dictionary.holds(durable) && !dictionary.holds(lasting),
      "a generation that is gone releases its values after the grace, before its calls' deadlines");
  // 4. A rebuild keeps the values it was rebuilt for (they count as published).
  {
    SecretDictionary::Limits small;
    small.nodeBytes = 64 * 1024;
    size_t appended = 0;
    SecretDictionary compacting(small, {[&](const auto& needles) { appended += needles.size(); return false; },
        replacePlatformLogRedactions});
    const auto fresh = syntheticValue();
    compacting.setCurrent({{"c", {fresh}}});
    KJ_REQUIRE(!compacting.admit("c", {fresh}, now, now + 1000) && compacting.holds(fresh),
        "a value added through a rebuild is in the dictionary");
    const auto before = appended;
    KJ_REQUIRE(compacting.sent("c", {fresh}, "p0", 1, "r", true, now, now + 1000) && appended == before,
        "a value added through a rebuild is not added again");
  }
  // 5. A region that cannot take a descriptor's values: it is not sent.
  SecretDictionary refusing({}, {[](const auto&) { return false; }, [](const auto&) { return false; }});
  const auto lost = syntheticValue();
  KJ_REQUIRE(!refusing.sent("x", {lost}, "p0", 1, "r", true, now, now + 1000) && !refusing.holds(lost) &&
      refusing.json(now).find("\"sinkFailures\":1") != std::string::npos);
}
// No dictionary size may silence a writer: 130 values with one 64 KiB block of
// ordinary output exhausted the old per-value work quota, and that writer then
// stayed blocked until the node restarted. Only an oversized block is dropped
// (and counted as suppressed), and only that block.
void manyValues() {
  std::vector<std::string> values, needles;
  auto publish = [&](size_t count) {
    needles.clear();
    while (values.size() < count) {
      values.push_back(syntheticValue());
      addRedaction(needles, values.back());
    }
    sortRedactions(needles);
    KJ_REQUIRE(appendPlatformLogRedactions(needles));
  };
  publish(2);
  {
    PlatformLogCleaner cleaner;
    std::string huge(8 * 1024 * 1024 + 1, 'y');
    huge.replace(1024, values[0].size(), values[0]);
    const auto dropped = cleaner.append(huge);
    const bool discarded = dropped.empty() && cleaner.suppressed();
    KJ_REQUIRE(discarded, "an oversized block must be dropped");
    const auto after = cleaner.append("after:" + values[0] + "\n");
    requireClean(after, values[0]);
    const bool recovered = after == "after:[redacted]\n" && !cleaner.suppressed();
    KJ_REQUIRE(recovered, "the writer must recover after a dropped block");
    // The dropped block's tail never reappears: a value that began inside it
    // and ends in the next append is still hidden as a whole.
    std::string straddle(8 * 1024 * 1024 + 1, 'z');
    straddle.replace(straddle.size() - 10, 10, values[1].substr(0, 10));
    const auto first = cleaner.append(straddle);
    const auto rest = cleaner.append(values[1].substr(10) + ":rest\n", true);
    requireClean(first + rest, values[1]);
    const bool hidden = first.empty() && rest == "[redacted]:rest\n";
    KJ_REQUIRE(hidden, "a value straddling a dropped block must stay hidden");
  }
  publish(130);
  std::string block;
  while (block.size() < 64 * 1024) block += "an ordinary console line from any tenant, with no value in it\n";
  // Both writer kinds: the node's own streams (final at stream end) and the
  // child-output writer (never final).
  for (const bool sink : {false, true}) {
    PlatformLogCleaner cleaner;
    for (int round = 0; round < 4; ++round) {
      const auto text = cleaner.append(block, sink && round == 3);
      const bool unchanged = text == block && !cleaner.suppressed();
      KJ_REQUIRE(unchanged, "a 64 KiB block must pass a 130-value dictionary unchanged", sink, round);
    }
    for (const auto& value : values) {
      const auto text = cleaner.append("later:" + value + ":tail\n", sink);
      requireClean(text, value);
      const bool cleaned = text == "later:[redacted]:tail\n" && !cleaner.suppressed();
      KJ_REQUIRE(cleaned, "a value after the large blocks must still be cleaned", sink);
    }
    // A value split between two appends of the same stream is still found.
    const auto& value = values[77];
    auto split = cleaner.append("split:" + value.substr(0, 20));
    split += cleaner.append(value.substr(20) + ":end\n", sink);
    requireClean(split, value);
    const bool joined = split == "split:[redacted]:end\n";
    KJ_REQUIRE(joined, "a value split across appends must be cleaned", sink);
  }
}
// The multi-pattern cleaner against a direct reference: about 2,500 needles (values, their escaped forms, values
// that are prefixes, suffixes and repetitions of others, a periodic one), random text with them inside, cut into
// random chunks. Every byte any needle covers is hidden, adjacent hidden bytes give one marker, nothing else changes.
std::string referenceClean(const std::string& text, const std::vector<std::string>& needles) {
  std::vector<char> hidden(text.size());
  for (const auto& needle : needles)
    for (auto at = text.find(needle); at != std::string::npos; at = text.find(needle, at + 1))
      std::fill(hidden.begin() + at, hidden.begin() + at + needle.size(), 1);
  std::string out;
  for (size_t at = 0; at < text.size(); ++at) {
    if (!hidden[at]) out += text[at];
    else if (at == 0 || !hidden[at - 1]) out += "[redacted]";
  }
  return out;
}
void manyValuesEquivalence() {
  uint64_t seed = 0x9e3779b97f4a7c15ull;
  auto next = [&]() { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; return seed; };
  auto word = [&](size_t size, const char* alphabet) {
    std::string value;
    const auto letters = strlen(alphabet);
    for (size_t at = 0; at < size; ++at) value += alphabet[next() % letters];
    return value;
  };
  std::vector<std::string> values;
  for (int at = 0; at < 1000; ++at) values.push_back(word(8 + next() % 56, "abcdefghijklmnop0123456789"));
  for (int at = 0; at < 100; ++at) values.push_back(values[next() % values.size()].substr(0, 5 + next() % 4));  // prefixes
  for (int at = 0; at < 100; ++at) { auto& v = values[next() % 1000]; values.push_back(v.substr(v.size() / 2)); }  // suffixes
  for (int at = 0; at < 30; ++at) values.push_back(values[next() % 1000] + values[next() % 1000]);  // concatenations
  values.push_back(std::string(300, 'q'));  // periodic
  values.push_back("quote\"and\nnewline");
  // A value inside a longer one, and the text leaving the longer one right after it ('#' is in no value): only the
  // failure links' matches find it (each node reports the longest value its failure chain ends with).
  std::vector<std::string> embedded;
  for (int at = 0; at < 50; ++at) {
    const auto inner = values[next() % 1000], before = word(4 + next() % 8, "abcdefghijklmnop");
    values.push_back(before + inner + word(4, "abcdefghijklmnop"));
    embedded.push_back(before + inner + "#");
  }
  std::vector<std::string> needles;
  for (const auto& value : values) addRedaction(needles, value);
  sortRedactions(needles);
  KJ_REQUIRE(replacePlatformLogRedactions(needles));
  std::string text;
  while (text.size() < 512 * 1024) {
    text += word(1 + next() % 80, "abcdefghijklmnopqrstuvwxyz0123456789 \"\\\n");
    if (next() % 3 == 0) text += values[next() % values.size()];
    if (next() % 50 == 0) text += std::string(100 + next() % 900, 'q');
    if (next() % 20 == 0) text += embedded[next() % embedded.size()];
  }
  const auto expected = referenceClean(text, needles);
  for (int round = 0; round < 3; ++round) {
    PlatformLogCleaner cleaner;
    std::string out;
    for (size_t at = 0; at < text.size();) {
      const auto size = std::min<size_t>(text.size() - at, round == 0 ? 65536 : 1 + next() % (round == 1 ? 7 : 4096));
      out += cleaner.append(std::string_view(text).substr(at, size), at + size == text.size());
      at += size;
    }
    KJ_REQUIRE(out == expected, "the cleaner must hide exactly what the needles cover", round, out.size(), expected.size());
  }
}

// A value over several lines (a PEM key) printed by one process while another process's lines come in between:
// the drainer keeps one cleaner state per process stream, so the value is still found whole and no line of it is
// written (v137 section 6.23; with one state for all streams the other process's line broke the match).
void multiLineValues() {
  TempDir directory;
  const auto body1 = syntheticValue(), body2 = syntheticValue();
  const auto value = "-----BEGIN TEST KEY-----\n" + body1 + "\n" + body2 + "\n-----END TEST KEY-----";
  std::vector<std::string> needles;
  addRedaction(needles, value);
  KJ_REQUIRE(appendPlatformLogRedactions(needles));
  const auto sinkPath = directory.file("sink");
  std::string text;
  {
    WorkerdOutput output(sinkPath, WorkerLogSettings{});
    WorkerdOutput::Pipes first, second;
    KJ_REQUIRE(WorkerdOutput::open(first) && WorkerdOutput::open(second));
    const int a = dup(first.stdoutWrite), b = dup(second.stdoutWrite);
    KJ_REQUIRE(a >= 0 && b >= 0);
    WorkerdOutput::attach(first, 0, "p0", 0, 1, getpid());
    WorkerdOutput::attach(second, 1, "p1", 0, 1, getpid());
    auto line = [](int fd, const std::string& bytes) {
      KJ_REQUIRE(::write(fd, bytes.data(), bytes.size()) == static_cast<ssize_t>(bytes.size()));
      usleep(200000);  // read and queued before the next one
    };
    line(a, "key: -----BEGIN TEST KEY-----\n" + body1 + "\n");
    line(b, "other: an unrelated line\n");
    line(a, body2 + "\n-----END TEST KEY----- done\n");
    line(b, "other: another line\n");
    close(a); close(b);
  }
  const auto fd = open(sinkPath.c_str(), O_RDONLY | O_CLOEXEC);
  KJ_REQUIRE(fd >= 0);
  char buffer[4096];
  for (ssize_t n; (n = ::read(fd, buffer, sizeof buffer)) > 0;) text.append(buffer, n);
  close(fd);
  requireClean(text, body1);
  requireClean(text, body2);
  const bool whole = text.find("key: [redacted] done\n") != std::string::npos &&
      text.find("other: an unrelated line\n") != std::string::npos && text.find("other: another line\n") != std::string::npos;
  KJ_REQUIRE(whole, "a value over several lines must be hidden whole when other processes' lines come between", text.size());
}

// Readings, not a test (not registered with CTest): seconds per MiB of ordinary output and the memory the cleaner
// takes, with N values (48-byte tokens, one in ten with characters that JSON escapes) in the dictionary.
void benchmark(size_t count, size_t mib) {
  auto resident = []() {
    std::ifstream statm("/proc/self/statm"); size_t pages = 0, rss = 0; statm >> pages >> rss;
    return rss * size_t(sysconf(_SC_PAGESIZE));
  };
  uint64_t seed = 42;
  auto next = [&]() { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; return seed; };
  std::vector<std::string> values, needles;
  for (size_t at = 0; at < count; ++at) {
    std::string value;
    for (int c = 0; c < 48; ++c) value += "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[next() % 62];
    if (at % 10 == 0) value[at % 40] = '"';
    values.push_back(value);
    addRedaction(needles, value);
  }
  sortRedactions(needles);
  KJ_REQUIRE(replacePlatformLogRedactions(needles));
  std::string line = "[server] {\"event\":\"request\",\"worker\":\"acme/primary\",\"status\":200,\"elapsedMs\":3.41}\n";
  std::string text;
  for (size_t at = 0; text.size() < mib * 1024 * 1024; ++at) {
    text += line;
    if (at % 100 == 0) text += "token=" + values[next() % values.size()] + "\n";
  }
  const auto before = resident();
  PlatformLogCleaner cleaner;
  (void)cleaner.append("warm\n");
  const auto built = resident();
  const auto started = std::chrono::steady_clock::now();
  size_t written = 0;
  for (size_t at = 0; at < text.size(); at += 65536) written += cleaner.append(std::string_view(text).substr(at, 65536)).size();
  const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
  size_t automaton = 0;
#ifdef COWORKERD_CLEANER_AUTOMATON
  automaton = cleaner.automatonBytes();
#endif
  std::cout << "{\"values\":" << count << ",\"needles\":" << needles.size() << ",\"mib\":" << mib
            << ",\"secondsPerMiB\":" << seconds / double(mib) << ",\"residentDeltaBytes\":" << (built > before ? built - before : 0)
            << ",\"automatonBytes\":" << automaton << ",\"writtenBytes\":" << written << "}" << std::endl;
}

// One tenant's share: at most a quarter of the node's dictionary, its current
// values included, at their escaped size. A tenant whose secrets are mostly
// control characters (each escapes to six bytes) fills its share and is
// refused with the amount; other tenants' new values and calls go on.
void secretShare() {
  SecretDictionary::Limits limits;
  limits.nodeBytes = 256 * 1024;  // a share of 64 KiB
  SecretDictionary dictionary(limits, sharedSink());
  PlatformLogCleaner cleaner;
  double now = 1000;
  std::map<std::string, std::set<std::string>> current;
  std::vector<std::string> heavy;
  std::optional<std::string> refused;
  for (int i = 0; i < 64 && !refused; ++i) {
    // 512 bytes: a random prefix, then control characters.
    heavy.push_back(syntheticValue() + std::string(464, '\x01'));
    current["heavy"].insert(heavy.back());
    dictionary.setCurrent(current);
    refused = dictionary.admit("heavy", {heavy.back()}, now, now + 60000);
  }
  KJ_REQUIRE(refused.has_value() && refused->find("more than its share of 64 KiB") != std::string::npos &&
      refused->find("fewer or smaller secrets") != std::string::npos && refused->find(heavy[0].substr(0, 16)) == std::string::npos,
      "a tenant past its share is refused with the amount over it");
  KJ_REQUIRE(dictionary.tenantBytes("heavy") <= dictionary.shareBytes() && dictionary.bytes() <= limits.nodeBytes);
  // The others: new values enter and their calls go on, also once the heavy tenant's current values count in full.
  for (int tenant = 0; tenant < 3; ++tenant) {
    const auto name = "light" + std::to_string(tenant);
    const auto value = syntheticValue() + std::string(400, '\n');
    current[name] = {value};
    dictionary.setCurrent(current);
    KJ_REQUIRE(!dictionary.admit(name, {value}, now, now + 60000), "another tenant's new value must enter", tenant);
    const auto text = cleaner.append(name + ":" + value + "\n");
    requireClean(text, value);
    KJ_REQUIRE(text == name + ":[redacted]\n");
  }
  const auto state = dictionary.json(now);
  KJ_REQUIRE(state.find("\"refused\":{\"full\":0,\"share\":1,\"earlier\":0,\"cooled\":0}") != std::string::npos &&
      state.find("\"tenantShareBytes\":65536") != std::string::npos, "the node state shows the share refusal");
  // Values that changed after the call was admitted are checked when its descriptor is built: past the share,
  // nothing is sent and nothing is added.
  const auto first = syntheticValue();
  current["switcher"] = {first};
  dictionary.setCurrent(current);
  KJ_REQUIRE(!dictionary.admit("switcher", {first}, now, now + 60000));
  std::set<std::string> changed;
  for (int i = 0; i < 24; ++i) changed.insert(syntheticValue() + std::string(464, '\x01'));
  current["switcher"] = changed;
  dictionary.setCurrent(current);
  const bool notSent = !dictionary.sent("switcher", changed, "p0", 1, "r", true, now, now + 60000) &&
      std::none_of(changed.begin(), changed.end(), [&](const auto& value) { return dictionary.holds(value); }) &&
      dictionary.tenantBytes("switcher") <= dictionary.shareBytes();
  KJ_REQUIRE(notSent, "a descriptor whose new values pass the tenant's share is not sent");
}

// A Durable Object tenant whose 64 KiB secret changes every minute: past
// 512 KiB the first time the node rotates the process; the second time within
// the hour it rotates nothing and refuses that tenant's Durable Object calls
// with secrets instead (its stateless calls, and another tenant's Durable
// Object calls, go on); after the hour the next trigger rotates again and the
// tenant recovers.
void secretRotationCooldown() {
  SecretDictionary::Limits limits;
  std::vector<std::pair<std::string, uint64_t>> requests;
  SecretDictionary::Sink sink = sharedSink();
  sink.rotate = [&](const std::string& process, uint64_t generation, const std::string&) { requests.emplace_back(process, generation); };
  SecretDictionary dictionary(limits, sink);
  const auto calm = longValue(1024);
  double now = 0;
  uint64_t generation = 1, fastRefused = 0, calmRefused = 0, statelessRefused = 0;
  std::optional<double> retireAt;
  std::string reason;
  int lastAdmitted = -1, firstRefused = -1;
  for (int minute = 1; minute <= 150; ++minute) {
    now += 60000;
    // 60 KiB: the writers' cleaner takes needles of at most 65,535 bytes (a secret is at most 5 KiB, its escaped
    // form six times that).
    const auto token = longValue(60 * 1024);
    dictionary.setCurrent({{"fast", {token}}, {"calm", {calm}}});
    if (auto refused = dictionary.admit("fast", {token}, now, now + 30000, true)) {
      ++fastRefused;
      reason = *refused;
      if (firstRefused < 0) firstRefused = minute;
    } else {
      KJ_REQUIRE(dictionary.sent("fast", {token}, "d0", generation, "", false, now, now + 30000));
      lastAdmitted = minute;
    }
    if (dictionary.admit("fast", {token}, now, now + 30000, false)) ++statelessRefused;
    if (dictionary.admit("calm", {calm}, now, now + 30000, true)) ++calmRefused;
    else KJ_REQUIRE(dictionary.sent("calm", {calm}, "d0", generation, "", false, now, now + 30000));
    // The node's rotation: the requested generation goes a minute later.
    if (!retireAt && !requests.empty() && requests.back().second == generation) retireAt = now + 60000;
    std::vector<SecretDictionary::Residents> live{{"d0", generation, false, {}, true}};
    if (retireAt && now >= *retireAt) {
      ++generation;
      retireAt.reset();
      live = {{"d0", generation, false, {}, true}};
    } else if (retireAt) {
      live.push_back({"d0", generation + 1, false, {}, false});
    }
    dictionary.collect(now, live);
  }
  const auto state = dictionary.json(now);
  const bool cooled = requests.size() == 3 && fastRefused > 0 && reason.find("grow too fast") != std::string::npos &&
      calmRefused == 0 && statelessRefused == 0 && lastAdmitted > firstRefused &&
      state.find("\"rotationsSuppressed\":") != std::string::npos;
  KJ_REQUIRE(cooled, "one tenant must not rotate a durable process more than once an hour", requests.size(), fastRefused,
      calmRefused, statelessRefused, firstRefused, lastAdmitted);
}

// A Durable Object tenant that changes a 1 KiB token every hour, on a process
// that names no isolates: its earlier values pile up in that generation. Past
// rotateEarlierBytes the node rotates the generation, its values go after the
// grace, and the tenant is never refused (without the rotation it would be,
// for good, after about 1,000 hours).
void secretPinnedRotation() {
  SecretDictionary::Limits limits;  // the defaults: 1 MiB of earlier values per tenant, rotation past 512 KiB
  std::vector<std::pair<std::string, uint64_t>> requests;
  SecretDictionary::Sink sink = sharedSink();
  sink.rotate = [&](const std::string& process, uint64_t generation, const std::string&) { requests.emplace_back(process, generation); };
  SecretDictionary dictionary(limits, sink);
  double now = 0;
  uint64_t generation = 1, refused = 0;
  std::optional<double> retireAt;
  size_t largest = 0;
  for (int hour = 0; hour < 2400; ++hour) {
    const auto token = longValue(1024);
    dictionary.setCurrent({{"durable", {token}}});
    if (dictionary.admit("durable", {token}, now, now + 30000)) ++refused;
    else KJ_REQUIRE(dictionary.sent("durable", {token}, "d0", generation, "", false, now, now + 30000));
    // Ten minutes of collections each hour: the node's rotation lets the old generation go a minute after it starts.
    for (int minute = 0; minute < 10; ++minute) {
      now += 60000;
      if (!retireAt && !requests.empty() && requests.back().second == generation) retireAt = now + 60000;
      std::vector<SecretDictionary::Residents> live{{"d0", retireAt && now >= *retireAt ? generation + 1 : generation, false, {}, true}};
      if (retireAt && now >= *retireAt) { ++generation; retireAt.reset(); }
      else if (retireAt) live.push_back({"d0", generation + 1, false, {}, false});
      dictionary.collect(now, live);
      largest = std::max(largest, dictionary.earlierBytes("durable"));
    }
    now += 50 * 60000;
  }
  const auto state = dictionary.json(now);
  KJ_REQUIRE(refused == 0 && !requests.empty() && requests.size() <= 6 && largest <= limits.tenantEarlierBytes,
      "a tenant pinned in a process that names no isolates must never be refused for good", refused, requests.size(), largest);
  KJ_REQUIRE(state.find("\"rotationsRequested\":" + std::to_string(requests.size())) != std::string::npos);
}

// A stuck sink: the lines queued meanwhile wait longer than LOG_LINE_STALE_MS
// (shortened here) while a value they hold leaves the dictionary. When the
// sink recovers they are dropped and counted, never written with the value;
// both writers (the workerd output and the node's own output).
void staleLines() {
  TempDir directory;
  {
    const auto value = syntheticValue();
    std::vector<std::string> needles;
    addRedaction(needles, value);
    KJ_REQUIRE(appendPlatformLogRedactions(needles));
    const auto sinkPath = directory.file("sink");
    KJ_REQUIRE(mkfifo(sinkPath.c_str(), 0600) == 0);
    WorkerLogSettings settings;
    settings.staleLineMs = 1500;
    std::string text, state;
    int reader = -1;
    {
      WorkerdOutput output(sinkPath, settings);
      WorkerdOutput::Pipes pipes;
      KJ_REQUIRE(WorkerdOutput::open(pipes));
      const int writer = dup(pipes.stdoutWrite);
      KJ_REQUIRE(writer >= 0);
      WorkerdOutput::attach(pipes, 0, "p0", 0, 1, getpid());
      auto line = [&](const std::string& bytes) {
        KJ_REQUIRE(::write(writer, bytes.data(), bytes.size()) == static_cast<ssize_t>(bytes.size()));
      };
      line("first: nothing secret\n");  // cleaned, then the writer waits for the sink (a FIFO without a reader)
      usleep(300000);
      line("old:" + value + "\n");      // queued while the sink is stuck
      usleep(300000);
      KJ_REQUIRE(replacePlatformLogRedactions({}));  // the value leaves the dictionary
      usleep(2000000);
      reader = open(sinkPath.c_str(), O_RDONLY | O_NONBLOCK);
      KJ_REQUIRE(reader >= 0);
      line("after: written\n");
      const auto deadline = monotonicTimeMs() + 10000;
      while (text.find("after: written") == std::string::npos && monotonicTimeMs() < deadline) {
        char buffer[4096];
        const auto n = ::read(reader, buffer, sizeof buffer);
        if (n > 0) text.append(buffer, n);
        else usleep(10000);
      }
      // A line whose first half arrived while its value was held and whose end came after the value left:
      // it is as old as its first byte, so it is dropped too.
      const auto split = syntheticValue();
      std::vector<std::string> splitNeedles;
      addRedaction(splitNeedles, split);
      KJ_REQUIRE(appendPlatformLogRedactions(splitNeedles));
      line("half:" + split.substr(0, 20));
      usleep(300000);
      KJ_REQUIRE(replacePlatformLogRedactions({}));
      usleep(2000000);
      line(split.substr(20) + "\n");
      line("tail: written\n");
      const auto again = monotonicTimeMs() + 10000;
      while (text.find("tail: written") == std::string::npos && monotonicTimeMs() < again) {
        char buffer[4096];
        const auto n = ::read(reader, buffer, sizeof buffer);
        if (n > 0) text.append(buffer, n);
        else usleep(10000);
      }
      requireClean(text, split);
      state = WorkerdOutput::processJson(0);
      close(writer);
    }
    close(reader);
    requireClean(text, value);
    const bool dropped = text.find("first: nothing secret") != std::string::npos && text.find("after: written") != std::string::npos &&
        text.find("old:") == std::string::npos && text.find("half:") == std::string::npos &&
        text.find("tail: written") != std::string::npos && state.find("\"stale\":2") != std::string::npos;
    KJ_REQUIRE(dropped, "a workerd line queued past the stale limit, or begun before it, must be dropped and counted");
  }
  {
    const auto value = syntheticValue();
    std::vector<std::string> needles;
    addRedaction(needles, value);
    KJ_REQUIRE(appendPlatformLogRedactions(needles));
    int stream[2];
    KJ_REQUIRE(pipe(stream) == 0);
    const auto savedOut = dup(STDOUT_FILENO), savedErr = dup(STDERR_FILENO);
    KJ_REQUIRE(savedOut >= 0 && savedErr >= 0);
    struct Restore {
      int out, err;
      ~Restore() { dup2(out, STDOUT_FILENO); dup2(err, STDERR_FILENO); close(out); close(err); }
    } restore{savedOut, savedErr};
    dup2(stream[1], STDOUT_FILENO); dup2(stream[1], STDERR_FILENO); close(stream[1]);
    fcntl(stream[0], F_SETFL, fcntl(stream[0], F_GETFL) | O_NONBLOCK);
    std::string text, state;
    {
      AsyncLog log(1500);
      // The event loop keeps the log's clock current (runLogClock); here the test does.
      AsyncLog::updateClock();
      // Fill the pipe so that the node's writer blocks on it.
      const std::string filler(1000, 'f');
      for (int i = 0; i < 256; ++i) std::cout << "filler:" << filler << '\n';
      usleep(300000);
      AsyncLog::updateClock();
      std::cout << "old:" << value << '\n';
      usleep(300000);
      KJ_REQUIRE(replacePlatformLogRedactions({}));
      usleep(2000000);
      AsyncLog::updateClock();
      std::cout << "after: written" << '\n';
      const auto deadline = monotonicTimeMs() + 10000;
      while (text.find("after: written") == std::string::npos && monotonicTimeMs() < deadline) {
        char buffer[65536];
        const auto n = ::read(stream[0], buffer, sizeof buffer);
        if (n > 0) text.append(buffer, n);
        else usleep(10000);
      }
      state = AsyncLog::stateJson();
    }
    dup2(savedOut, STDOUT_FILENO); dup2(savedErr, STDERR_FILENO);
    for (char buffer[65536]; ::read(stream[0], buffer, sizeof buffer) > 0;) {}
    close(stream[0]);
    requireClean(text, value);
    const auto at = state.find("\"staleLines\":");
    const bool counted = at != std::string::npos && std::stoull(state.substr(at + 13)) >= 1;
    KJ_REQUIRE(text.find("after: written") != std::string::npos && text.find("old:") == std::string::npos && counted,
        "a node line queued past the stale limit must be dropped and counted");
  }
}
struct Child {
  pid_t pid = -1;
  int control = -1;
  Child(const char* workerd, const std::string& config, uint32_t slot, uint32_t processClass) {
    WorkerdOutput::Pipes pipes;
    KJ_REQUIRE(WorkerdOutput::open(pipes));
    int channel[2]; KJ_REQUIRE(pipe(channel) == 0);
    // Keep the control descriptor away from the stdout/stderr dup2 targets.
    const auto childControl = fcntl(channel[1], F_DUPFD_CLOEXEC, 64);
    close(channel[1]); KJ_REQUIRE(childControl >= 0);
    pid = fork(); KJ_REQUIRE(pid >= 0);
    if (pid == 0) {
      if (dup2(pipes.stdoutWrite, STDOUT_FILENO) < 0 || dup2(pipes.stderrWrite, STDERR_FILENO) < 0 ||
          dup2(childControl, 3) < 0) _exit(126);
      for (int fd : {pipes.stdoutRead, pipes.stdoutWrite, pipes.stderrRead, pipes.stderrWrite, channel[0], childControl})
        if (fd > 3) close(fd);
      execl(workerd, workerd, "serve", config.c_str(), "--control-fd=3", static_cast<char*>(nullptr));
      _exit(127);
    }
    close(childControl); control = channel[0];
    fcntl(control, F_SETFD, FD_CLOEXEC);
    WorkerdOutput::attach(pipes, slot, processClass == 2 ? "d0" : "p0", processClass, slot + 1, pid);
  }
  ~Child() {
    if (control >= 0) close(control);
    if (pid > 0) { kill(pid, SIGKILL); while (waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {} }
  }
  int wait() {
    int status = 0;
    for (unsigned i = 0; i < 500; ++i) {
      const auto result = waitpid(pid, &status, WNOHANG);
      if (result == pid) { pid = -1; return status; }
      KJ_REQUIRE(result == 0 || errno == EINTR);
      usleep(10000);
    }
    KJ_FAIL_REQUIRE("component Worker did not exit");
  }
  uint16_t listenPort() {
    std::string partial;
    const auto deadline = monotonicTimeMs() + 5000;
    while (monotonicTimeMs() < deadline) {
      pollfd descriptor{control, POLLIN, 0};
      KJ_REQUIRE(poll(&descriptor, 1, 100) >= 0 || errno == EINTR);
      if (!(descriptor.revents & (POLLIN | POLLHUP))) continue;
      char buffer[4096]; const auto n = read(control, buffer, sizeof(buffer));
      KJ_REQUIRE(n > 0, "Worker exited before listening");
      partial.append(buffer, n);
      for (auto end = partial.find('\n'); end != std::string::npos; end = partial.find('\n')) {
        capnp::MallocMessageBuilder message;
        capnp::JsonCodec codec;
        const auto line = partial.substr(0, end);
        codec.decodeRaw(kj::StringPtr(line.c_str(), line.size()), message.initRoot<capnp::JsonValue>());
        const auto record = message.getRoot<capnp::JsonValue>().asReader();
        if (json::text(record, "event") == "listen" && json::text(record, "socket") == "http")
          return static_cast<uint16_t>(json::field(record, "port").getNumber());
        partial.erase(0, end + 1);
      }
    }
    KJ_FAIL_REQUIRE("component Worker did not listen");
  }
};
void request(uint16_t port) {
  const int fd = socket(AF_INET, SOCK_STREAM, 0); KJ_REQUIRE(fd >= 0);
  timeval timeout{5, 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  sockaddr_in address{}; address.sin_family = AF_INET; address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    close(fd); KJ_FAIL_REQUIRE("could not call the component Worker");
  }
  constexpr std::string_view call = "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
  const auto sent = send(fd, call.data(), call.size(), 0);
  std::string answer; char buffer[4096];
  if (sent == static_cast<ssize_t>(call.size())) for (;;) {
    const auto n = read(fd, buffer, sizeof(buffer));
    if (n > 0) { answer.append(buffer, n); continue; }
    if (n < 0 && errno == EINTR) continue;
    break;
  }
  close(fd);
  KJ_REQUIRE(answer.starts_with("HTTP/1.1 500"), "the real request must throw");
}
void workerDiagnostics(const char* workerd) {
  KJ_REQUIRE(workerd && access(workerd, X_OK) == 0, "set COWORKERD_TEST_WORKERD to the built workerd");
  TempDir directory;
  const auto value = syntheticValue(), later = syntheticValue();
  const auto nodePath = directory.file("node.log"), workerdPath = directory.file("workerd.log");
  const auto savedOut = dup(STDOUT_FILENO), savedErr = dup(STDERR_FILENO);
  KJ_REQUIRE(savedOut >= 0 && savedErr >= 0);
  // Restore before an assertion can print. Raw values never reach the test log,
  // even when qualification deliberately removes the production cleaner.
  struct Restore {
    int out, err;
    ~Restore() { dup2(out, STDOUT_FILENO); dup2(err, STDERR_FILENO); close(out); close(err); }
  } restore{savedOut, savedErr};
  const auto nodeFd = open(nodePath.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  KJ_REQUIRE(nodeFd >= 0); dup2(nodeFd, STDOUT_FILENO); dup2(nodeFd, STDERR_FILENO); close(nodeFd);
  {
    AsyncLog nodeLog;
    WorkerdOutput output(workerdPath);
    // Both consumers already forked; production publication must reach them.
    std::vector<std::string> needles, rebuilt;
    addRedaction(needles, value);
    KJ_REQUIRE(appendPlatformLogRedactions(needles));
    std::cout << "node-out:" << value << '\n';
    std::cerr << "node-error:" << value << '\n';
    uint32_t slot = 0;
    for (const auto processClass : {0u, 2u}) for (const bool top : {true, false}) {
      if (processClass == 2 && top) {
        // A rebuild after both writers forked (it drops nothing they need and
        // adds a value) reaches both of them as well.
        addRedaction(rebuilt, value);
        addRedaction(rebuilt, later);
        KJ_REQUIRE(replacePlatformLogRedactions(rebuilt));
        std::cout << "node-later:" << later << '\n';
      }
      const auto stem = std::to_string(slot);
      const auto script = directory.path + '/' + stem + ".js";
      const auto config = directory.path + '/' + stem + ".capnp";
      writeFile(script, top ? "throw new Error('top-diagnostic:' + SECRET);" :
          "addEventListener('fetch', () => { throw new Error('request-diagnostic:' + SECRET); });");
      writeFile(config, "using Workerd = import \"/workerd/workerd.capnp\";\nconst test :Workerd.Config = ("
          "services = [(name = \"main\", worker = (serviceWorkerScript = embed \"" + stem +
          ".js\", compatibilityDate = \"2024-01-01\", bindings = [(name = \"SECRET\", text = \"" + value +
          "\")]))], sockets = [(name = \"http\", address = \"127.0.0.1:0\", http = (), service = \"main\")]);\n");
      Child child(workerd, config, slot++, processClass);
      if (top) { const auto status = child.wait(); KJ_REQUIRE(WIFEXITED(status) && WEXITSTATUS(status) != 0); }
      else { request(child.listenPort()); kill(child.pid, SIGTERM); child.wait(); }
    }
  }
  // Stop capturing before hash assertions, including deliberately failing ones.
  dup2(savedOut, STDOUT_FILENO); dup2(savedErr, STDERR_FILENO);
  const auto node = readFile(nodePath), diagnostics = readFile(workerdPath);
  requireClean(node, value); requireClean(diagnostics, value); requireClean(node, later);
  KJ_REQUIRE(node.find("node-out:[redacted]") != std::string::npos &&
      node.find("node-error:[redacted]") != std::string::npos, "both node streams must be cleaned");
  KJ_REQUIRE(node.find("node-later:[redacted]") != std::string::npos, "a rebuild must reach the forked node writer");
  for (const auto marker : {"top-diagnostic:[redacted]", "request-diagnostic:[redacted]"}) {
    const auto first = diagnostics.find(marker);
    KJ_REQUIRE(first != std::string::npos && diagnostics.find(marker, first + 1) != std::string::npos,
        "ordinary and durable diagnostic sources must both be exercised", marker);
  }
}
}  // namespace
}  // namespace coworkerd

int main(int argc, char** argv) {
  try {
    if (argc == 2 && std::string_view(argv[1]) == "streams") coworkerd::streams();
    else if (argc == 2 && std::string_view(argv[1]) == "many-values") coworkerd::manyValues();
    else if (argc == 2 && std::string_view(argv[1]) == "many-values-equivalence") coworkerd::manyValuesEquivalence();
    else if (argc == 2 && std::string_view(argv[1]) == "multi-line-values") coworkerd::multiLineValues();
    else if (argc == 4 && std::string_view(argv[1]) == "bench") coworkerd::benchmark(std::stoul(argv[2]), std::stoul(argv[3]));
    else if (argc == 2 && std::string_view(argv[1]) == "generations") coworkerd::generations();
    else if (argc == 2 && std::string_view(argv[1]) == "secret-rotations") coworkerd::secretRotations();
    else if (argc == 2 && std::string_view(argv[1]) == "secret-limits") coworkerd::secretLimits();
    else if (argc == 2 && std::string_view(argv[1]) == "secret-share") coworkerd::secretShare();
    else if (argc == 2 && std::string_view(argv[1]) == "secret-pinned-rotation") coworkerd::secretPinnedRotation();
    else if (argc == 2 && std::string_view(argv[1]) == "secret-rotation-cooldown") coworkerd::secretRotationCooldown();
    else if (argc == 2 && std::string_view(argv[1]) == "stale-lines") coworkerd::staleLines();
    else { KJ_REQUIRE(argc == 2); coworkerd::workerDiagnostics(argv[1]); }
    return 0;
  } catch (const kj::Exception& error) {
    // KJ_REQUIRE expressions above carry booleans/digests, never raw logs.
    std::cerr << error.getDescription().cStr() << '\n'; return 1;
  }
}
