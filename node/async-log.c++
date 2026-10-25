#include "async-log.h"

#include <kj/debug.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <dirent.h>
#include <iostream>
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <poll.h>
#include <sstream>
#include <stdexcept>
#include <streambuf>
#include <string_view>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <system_error>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif

namespace coworkerd {

// WorkerdOutput: workerd stdout/stderr -> pipes -> drainer process -> sink file.
namespace {
uint64_t wallClock() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}
constexpr uint32_t OUTPUT_SLOTS = 512;
constexpr size_t OUTPUT_DEFAULT_BUDGET = 4 * 1024 * 1024;
constexpr size_t OUTPUT_READ_BYTES = 64 * 1024;
constexpr size_t OUTPUT_FINAL_DRAIN_LIMIT = 16 * 1024 * 1024;
constexpr uint64_t OUTPUT_SINK_GIVE_UP_MS = 5000;
constexpr uint32_t OUTPUT_MAGIC = 0x434f5754;
// A log query: the record carries one end of a socket pair; the request and the answer travel on it.
constexpr uint32_t OUTPUT_QUERY_MAGIC = 0x434f5751;
constexpr size_t OUTPUT_QUERY_REQUEST_BYTES = 512;
// An append request's lines (one invocation's trace is at most 256 KiB, escaped as JSON at most about four times that).
constexpr size_t OUTPUT_APPEND_MAX_BYTES = 2 * 1024 * 1024;
constexpr size_t OUTPUT_QUERY_READ_BYTES = 64 * 1024;
constexpr uint32_t OUTPUT_QUERY_MAX_LINES = 10000;
constexpr int OUTPUT_QUERY_IO_MS = 2000;
#ifdef __linux__
constexpr int OUTPUT_PIPE_BYTES = 256 * 1024;
constexpr int OUTPUT_SEND_FLAGS = MSG_DONTWAIT | MSG_NOSIGNAL;
#else
constexpr int OUTPUT_SEND_FLAGS = MSG_DONTWAIT;
#endif
enum DropReason { DROP_BACKLOG, DROP_OVERSIZED, DROP_PARTIAL, DROP_SINK, DROP_REASONS };
constexpr std::array DROP_NAMES = {"backlog", "oversized", "partial", "sink"};
static_assert(DROP_NAMES.size() == DROP_REASONS);

// Per process slot. The drainer's reader thread accepts and drops lines, its
// writer thread writes them; the server only reads these counters.
struct alignas(64) OutputSlot {
  std::atomic<uint64_t> generation{0}, pid{0}, processClass{0}, attachedAtUnixMs{0}, openStreams{0};
  std::atomic<uint64_t> acceptedLines{0}, acceptedBytes{0}, writtenLines{0}, writtenBytes{0};
  std::atomic<uint64_t> backlogLines{0}, backlogBytes{0}, highWaterBytes{0};
  std::atomic<uint64_t> droppedLines{0}, droppedBytes{0};
  std::array<std::atomic<uint64_t>, DROP_REASONS> droppedBy{};
  std::atomic<uint64_t> firstDropUnixMs{0}, lastDropUnixMs{0}, lastDropGeneration{0}, lastDropPid{0};
  std::atomic<uint64_t> reportedLines{0}, markerLines{0};
  // Tenant lines: those the node appended for this slot's process (from its runtime's tail channel); lines of its own
  // output that looked like a tenant's line (never attributed).
  std::atomic<uint64_t> tenantLines{0}, tenantUnverified{0};
};
struct OutputShared {
  std::atomic<uint32_t> ready{0}, serverGone{0}, sinkOpen{0}, finished{0};
  std::atomic<uint64_t> attachments{0}, highestSlot{0}, serverGoneAtUnixMs{0}, finalDrainBytes{0};
  std::atomic<uint64_t> sinkOpenFailures{0}, sinkWriteFailures{0}, lastSinkErrno{0};
  // The sink's rotations and current size; the Workers' kept logs; queries answered and failed.
  std::atomic<uint64_t> sinkRotations{0}, sinkBytes{0}, sinkRotationFailures{0};
  std::atomic<uint64_t> tenantWorkers{0}, tenantKeptLines{0}, tenantDroppedLines{0}, tenantTruncatedLines{0};
  std::atomic<uint64_t> tenantEvictedLines{0}, tenantBufferBytes{0}, queries{0}, queryFailures{0};
  std::array<OutputSlot, OUTPUT_SLOTS> slots;
};
struct AttachMessage {
  uint32_t magic, slot, processClass, reserved;
  uint64_t generation;
  int64_t pid;
  char processId[16];
};
size_t outputBudget() {
  const char* value = std::getenv("COWORKERD_WORKERD_LOG_BUFFER_BYTES");
  if (!value) return OUTPUT_DEFAULT_BUDGET;
  char* end;
  errno = 0;
  auto result = std::strtoull(value, &end, 10);
  if (errno || *value == '-' || *end || result < 128 * 1024 || result > 64 * 1024 * 1024)
    throw std::invalid_argument("COWORKERD_WORKERD_LOG_BUFFER_BYTES must be 131072..67108864");
  return result;
}
uint64_t monotonicMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now().time_since_epoch()).count();
}
void closeDescriptor(int& fd) {
  if (fd >= 0) ::close(fd);
  fd = -1;
}
size_t writeSome(int fd, const char* data, size_t size) {
  size_t done = 0;
  while (done < size) {
    auto n = ::write(fd, data + done, size - done);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) break;
    done += n;
  }
  return done;
}
std::string jsonText(std::string_view value) {
  std::string result;
  for (char ch: value) {
    if (ch == '"' || ch == '\\') result += '\\';
    if (static_cast<unsigned char>(ch) < 0x20) result += ' '; else result += ch;
  }
  return result;
}
// The drainer must not keep the server's other descriptors: holding the AsyncLog
// writer's liveness pipe would hide the server's death from that writer.
void closeOtherDescriptors(int keep) {
  std::vector<int> open;
  DIR* directory = opendir("/proc/self/fd");
  if (!directory) directory = opendir("/dev/fd");
  if (!directory) throw std::runtime_error("cannot list descriptors");
  const int own = dirfd(directory);
  while (auto* entry = readdir(directory)) {
    char* end;
    const long fd = std::strtol(entry->d_name, &end, 10);
    if (*entry->d_name && !*end && fd > STDERR_FILENO && fd != keep && fd != own) open.push_back(int(fd));
  }
  closedir(directory);
  for (int fd: open) ::close(fd);
}

// ---- The Workers' logs (by tenant) ----
// Tenant lines as the node appends them: {"workerId":"..","versionId":"..","level":"..","message":"<JSON string>"}
// (fields in this order). The workerId is a Worker's full name, <tenant>/<worker>, the versionId a deployment's safe
// name; the message stays as its JSON text. Output of a process that starts with TENANT_PREFIX is only counted.
constexpr std::string_view TENANT_PREFIX = "[tenant-log] ";
bool nameCharacter(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.';
}
struct TenantLine { std::string_view workerId, versionId, level, message; };
std::optional<TenantLine> parseTenantLine(std::string_view json) {
  TenantLine line;
  auto literal = [&](std::string_view expected) {
    if (!json.starts_with(expected)) return false;
    json.remove_prefix(expected.size());
    return true;
  };
  auto name = [&](std::string_view& out, size_t limit) {
    size_t n = 0;
    while (n < json.size() && n <= limit && nameCharacter(json[n])) ++n;
    if (n == 0 || n > limit) return false;
    out = json.substr(0, n);
    json.remove_prefix(n);
    return true;
  };
  // A full name: the tenant, one '/', the Worker's name.
  auto fullName = [&](std::string_view& out) {
    std::string_view tenant, worker;
    if (!name(tenant, 63) || !literal("/") || !name(worker, 255)) return false;
    out = std::string_view(tenant.data(), tenant.size() + 1 + worker.size());
    return true;
  };
  if (!literal("{\"workerId\":\"") || !fullName(line.workerId) || !literal("\",\"versionId\":\"") ||
      !name(line.versionId, 255) || !literal("\",\"level\":\"") || !name(line.level, 16) || !literal("\",\"message\":\"")) {
    return std::nullopt;
  }
  bool escaped = false;
  size_t end = 0;
  for (; end < json.size(); ++end) {
    const char c = json[end];
    if (escaped) { escaped = false; continue; }
    if (c == '\\') { escaped = true; continue; }
    if (c == '"') break;
    if (static_cast<unsigned char>(c) < 0x20) return std::nullopt;
  }
  if (end >= json.size()) return std::nullopt;
  line.message = json.substr(0, end);
  json.remove_prefix(end + 1);
  if (json != "}") return std::nullopt;
  return line;
}
// The longest prefix of a JSON string's text within `limit` bytes that ends between two characters:
// never inside an escape sequence or a UTF-8 sequence.
size_t jsonTextCut(std::string_view text, size_t limit) {
  size_t at = 0;
  while (at < text.size()) {
    const auto c = static_cast<unsigned char>(text[at]);
    size_t unit = 1;
    if (c == '\\') unit = at + 1 < text.size() && text[at + 1] == 'u' ? 6 : 2;
    else if (c >= 0xf0) unit = 4;
    else if (c >= 0xe0) unit = 3;
    else if (c >= 0xc0) unit = 2;
    if (at + unit > limit) break;
    at += unit;
  }
  return at;
}
std::string jsonQuoted(std::string_view value) { return "\"" + jsonText(value) + "\""; }

// What the drainer keeps (reader thread writes, query thread reads; one mutex, held briefly). The quota
// is a tenant's: each tenant's lines (of all its Workers) in arrival order up to its buffer (the oldest go first),
// all of them up to totalBufferBytes (the oldest line of any tenant goes first); per tenant and second at most its
// lines per second, the others dropped and counted in one entry once that second is over. A tenant's own quota (its
// record's logLinesPerSecond and logBufferBytes, set through the query channel: "!quota ...") replaces the defaults.
// A query names a Worker: that Worker's lines and its tenant's drop entries.
class TenantLogs {
public:
  TenantLogs(OutputShared& shared, const WorkerLogSettings& settings): shared(shared), settings(settings),
      epoch(std::to_string(wallClock()) + "-" + std::to_string(getpid())) {}

  void add(const std::string& processId, const TenantLine& line) {
    const auto now = wallClock();
    const auto worker = std::string(line.workerId);
    const auto tenant = worker.substr(0, worker.find('/'));
    std::lock_guard lock(mutex);
    auto [found, created] = tenants.try_emplace(tenant);
    if (created) shared.tenantWorkers.store(tenants.size(), std::memory_order_relaxed);
    auto& log = found->second;
    closeWindow(found->first, log, now);
    const auto rate = linesPerSecond(tenant);
    if (log.windowLines >= rate) {
      if (!log.windowDropped) log.windowFirstDropUnixMs = now;
      ++log.windowDropped;
      ++log.droppedLines;
      shared.tenantDroppedLines.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    ++log.windowLines;
    const auto cut = line.message.size() > settings.maxLineBytes ? jsonTextCut(line.message, settings.maxLineBytes) : line.message.size();
    std::string json;
    json.reserve(160 + cut);
    json += "{\"seq\":" + std::to_string(++sequence) + ",\"atUnixMs\":" + std::to_string(now) + ",\"processId\":" +
        jsonQuoted(processId) + ",\"workerId\":\"";
    json += line.workerId;
    json += "\",\"versionId\":\"";
    json += line.versionId;
    json += "\",\"level\":\"";
    json += line.level;
    json += "\",\"message\":\"";
    json.append(line.message.data(), cut);
    json += '"';
    if (cut < line.message.size()) {
      json += ",\"truncated\":true,\"messageBytes\":" + std::to_string(line.message.size());
      ++log.truncatedLines;
      shared.tenantTruncatedLines.fetch_add(1, std::memory_order_relaxed);
    }
    json += '}';
    ++log.keptLines;
    shared.tenantKeptLines.fetch_add(1, std::memory_order_relaxed);
    push(tenant, log, sequence, now, worker, std::move(json));
  }

  // "<workerId> <after or -> <sinceUnixMs> <limit>": the answer, one JSON object. Two requests of the node itself:
  // "!quota <tenant> <linesPerSecond> <bufferBytes>" (0 0: the defaults again) and "!usage" (each tenant's
  // kept and dropped lines since the drainer started).
  std::string query(std::string_view request) {
    if (request.starts_with("!")) return command(request);
    std::string worker, cursor, since, limit;
    {
      std::istringstream in{std::string(request)};
      in >> worker >> cursor >> since >> limit;
    }
    uint64_t after = 0, sinceUnixMs = 0, count = 0;
    const bool byCursor = cursor != "-";
    const auto slash = worker.find('/');
    if (worker.empty() || worker.size() > 320 || slash == std::string::npos || slash == 0 || slash + 1 == worker.size() ||
        worker.find('/', slash + 1) != std::string::npos ||
        !std::all_of(worker.begin(), worker.end(), [](char c) { return nameCharacter(c) || c == '/'; }) ||
        (byCursor && !number(cursor, after)) || !number(since, sinceUnixMs) || !number(limit, count) || count == 0) {
      return "{\"error\":\"invalid query\"}";
    }
    const auto tenant = worker.substr(0, slash);
    count = std::min<uint64_t>(count, OUTPUT_QUERY_MAX_LINES);
    const auto now = wallClock();
    std::lock_guard lock(mutex);
    std::string out = "{\"epoch\":\"" + epoch + "\",\"workerId\":" + jsonQuoted(worker) + ",\"lines\":[";
    auto found = tenants.find(tenant);
    uint64_t next = byCursor ? after : 0, oldest = 0;
    bool more = false;
    if (found != tenants.end()) {
      auto& log = found->second;
      closeWindow(found->first, log, now);
      const auto& entries = log.entries;
      // This Worker's lines and its tenant's drop entries (they belong to no Worker).
      auto mine = [&](const Entry& e) { return e.worker.empty() || e.worker == worker; };
      std::vector<const Entry*> chosen;
      if (byCursor) {
        auto first = std::upper_bound(entries.begin(), entries.end(), after, [](uint64_t value, const Entry& e) { return value < e.seq; });
        for (auto it = first; it != entries.end(); ++it) {
          if (it->atUnixMs < sinceUnixMs || !mine(*it)) continue;
          if (chosen.size() == count) { more = true; break; }
          chosen.push_back(&*it);
        }
      } else {
        for (auto it = entries.rbegin(); it != entries.rend() && chosen.size() < count && it->atUnixMs >= sinceUnixMs; ++it)
          if (mine(*it)) chosen.push_back(&*it);
        std::reverse(chosen.begin(), chosen.end());
      }
      for (size_t i = 0; i < chosen.size(); ++i) { if (i) out += ','; out += chosen[i]->json; }
      if (!chosen.empty()) next = chosen.back()->seq;
      if (!byCursor && !entries.empty()) next = std::max(next, entries.back().seq);
      if (!entries.empty()) oldest = entries.front().seq;
      out += "],\"tenant\":" + jsonQuoted(tenant) + ",\"keptLines\":" + std::to_string(log.keptLines) + ",\"droppedLines\":" +
          std::to_string(log.droppedLines) + ",\"truncatedLines\":" + std::to_string(log.truncatedLines) + ",\"evictedLines\":" +
          std::to_string(log.evictedLines) + ",\"bufferBytes\":" + std::to_string(log.bytes);
    } else {
      out += "],\"tenant\":" + jsonQuoted(tenant) + ",\"keptLines\":0,\"droppedLines\":0,\"truncatedLines\":0,\"evictedLines\":0,\"bufferBytes\":0";
    }
    out += ",\"next\":" + std::to_string(next) + ",\"oldestSeq\":" + std::to_string(oldest) + ",\"more\":" +
        (more ? "true" : "false") + ",\"limits\":{\"tenantBufferBytes\":" + std::to_string(bufferBytes(tenant)) +
        ",\"totalBufferBytes\":" + std::to_string(settings.totalBufferBytes) + ",\"linesPerSecond\":" +
        std::to_string(linesPerSecond(tenant)) + ",\"maxLineBytes\":" + std::to_string(settings.maxLineBytes) + "}}";
    return out;
  }

private:
  struct Entry { uint64_t seq, atUnixMs; std::string worker, json; };
  struct Log {
    std::deque<Entry> entries;
    uint64_t bytes = 0;
    uint64_t windowSecond = 0, windowLines = 0, windowDropped = 0, windowFirstDropUnixMs = 0;
    uint64_t keptLines = 0, droppedLines = 0, truncatedLines = 0, evictedLines = 0;
  };
  struct Quota { uint64_t linesPerSecond = 0, bufferBytes = 0; };
  static constexpr uint64_t ENTRY_OVERHEAD = 64;

  static bool number(const std::string& text, uint64_t& out) {
    if (text.empty() || text.size() > 19 || !std::all_of(text.begin(), text.end(), [](char c) { return c >= '0' && c <= '9'; }))
      return false;
    out = std::stoull(text);
    return true;
  }
  uint64_t linesPerSecond(const std::string& tenant) const {
    auto found = quotas.find(tenant);
    return found != quotas.end() && found->second.linesPerSecond ? found->second.linesPerSecond : settings.linesPerSecond;
  }
  uint64_t bufferBytes(const std::string& tenant) const {
    auto found = quotas.find(tenant);
    return found != quotas.end() && found->second.bufferBytes ? found->second.bufferBytes : settings.tenantBufferBytes;
  }
  std::string command(std::string_view request) {
    std::istringstream in{std::string(request.substr(1))};
    std::string verb;
    in >> verb;
    std::lock_guard lock(mutex);
    if (verb == "quota") {
      std::string tenant, lines, bytes;
      in >> tenant >> lines >> bytes;
      Quota quota;
      if (tenant.empty() || tenant.size() > 63 || !std::all_of(tenant.begin(), tenant.end(), nameCharacter) ||
          !number(lines, quota.linesPerSecond) || !number(bytes, quota.bufferBytes) ||
          (quota.bufferBytes && (quota.bufferBytes < 4096 || quota.bufferBytes > settings.totalBufferBytes)))
        return "{\"error\":\"invalid quota\"}";
      if (!quota.linesPerSecond && !quota.bufferBytes) quotas.erase(tenant); else quotas[tenant] = quota;
      return "{\"ok\":true,\"tenant\":" + jsonQuoted(tenant) + ",\"linesPerSecond\":" + std::to_string(linesPerSecond(tenant)) +
          ",\"bufferBytes\":" + std::to_string(bufferBytes(tenant)) + "}";
    }
    if (verb == "usage") {
      std::string out = "{\"epoch\":\"" + epoch + "\",\"tenants\":{";
      size_t index = 0;
      for (const auto& [tenant, log] : tenants)
        out += std::string(index++ ? "," : "") + jsonQuoted(tenant) + ":{\"keptLines\":" + std::to_string(log.keptLines) +
            ",\"droppedLines\":" + std::to_string(log.droppedLines) + ",\"bufferBytes\":" + std::to_string(log.bytes) + "}";
      return out + "}}";
    }
    return "{\"error\":\"invalid query\"}";
  }

  // A second that is over and dropped lines: their one entry.
  void closeWindow(const std::string& tenant, Log& log, uint64_t now) {
    if (log.windowSecond == now / 1000) return;
    if (log.windowDropped) {
      const auto at = std::min(now, log.windowSecond * 1000 + 999);
      std::string json = "{\"seq\":" + std::to_string(++sequence) + ",\"atUnixMs\":" + std::to_string(at) + ",\"tenant\":" +
          jsonQuoted(tenant) + ",\"dropped\":" + std::to_string(log.windowDropped) + ",\"reason\":\"rate\",\"linesPerSecond\":" +
          std::to_string(linesPerSecond(tenant)) + ",\"firstDroppedAtUnixMs\":" + std::to_string(log.windowFirstDropUnixMs) +
          ",\"windowStartUnixMs\":" + std::to_string(log.windowSecond * 1000) + '}';
      push(tenant, log, sequence, at, {}, std::move(json));
    }
    log.windowSecond = now / 1000;
    log.windowLines = log.windowDropped = log.windowFirstDropUnixMs = 0;
  }
  void push(const std::string& tenant, Log& log, uint64_t seq, uint64_t at, std::string worker, std::string json) {
    const auto bytes = json.size() + ENTRY_OVERHEAD;
    log.entries.push_back({seq, at, std::move(worker), std::move(json)});
    log.bytes += bytes;
    total += bytes;
    ++live;
    order.emplace_back(seq, &log);
    const auto limit = bufferBytes(tenant);
    while (log.bytes > limit && log.entries.size() > 1) evictFront(log);
    while (total > settings.totalBufferBytes && !order.empty()) {
      auto [oldest, owner] = order.front();
      order.pop_front();
      if (!owner->entries.empty() && owner->entries.front().seq == oldest) evictFront(*owner);
    }
    // A tenant's own limit leaves its evicted entries in `order`: dropped once they outnumber the rest
    // (a tenant's kept entries are the ones from its oldest on).
    if (order.size() > 2 * live + 4096) {
      std::erase_if(order, [](const auto& entry) {
        return entry.second->entries.empty() || entry.first < entry.second->entries.front().seq;
      });
    }
    shared.tenantBufferBytes.store(total, std::memory_order_relaxed);
  }
  void evictFront(Log& log) {
    const auto& front = log.entries.front();
    const auto bytes = front.json.size() + ENTRY_OVERHEAD;
    log.entries.pop_front();
    log.bytes -= bytes;
    total -= bytes;
    --live;
    ++log.evictedLines;
    shared.tenantEvictedLines.fetch_add(1, std::memory_order_relaxed);
  }

  OutputShared& shared;
  const WorkerLogSettings settings;
  const std::string epoch;
  std::mutex mutex;
  std::map<std::string, Log> tenants;
  std::map<std::string, Quota> quotas;
  // Every kept entry once, oldest first, with its tenant's log (a std::map node does not move); an
  // entry its tenant already evicted is skipped.
  std::deque<std::pair<uint64_t, Log*>> order;
  uint64_t sequence = 0, total = 0, live = 0;
};

class OutputDrainer {
public:
  OutputDrainer(OutputShared& shared, int control, std::string sinkPath, size_t budget, const WorkerLogSettings& settings)
      : shared(shared), control(control), sinkPath(std::move(sinkPath)), budget(budget), settings(settings),
        readBuffer(OUTPUT_READ_BYTES), tenants(shared, settings) {}

  void run() {
    std::thread writer([this] { writeLoop(); });
    std::thread queries([this] { queryLoop(); });
    shared.ready.store(1, std::memory_order_release);
    readLoop();
    {
      std::lock_guard lock(mutex);
      readerDone = true;
    }
    wake.notify_all();
    {
      std::lock_guard lock(queryMutex);
      queriesDone = true;
    }
    queryWake.notify_all();
    writer.join();
    queries.join();
    shared.finished.store(1, std::memory_order_release);
  }

private:
  struct Stream { int fd = -1; std::string partial; uint64_t discarded = 0; bool discarding = false; };
  struct Source { uint32_t slot; uint64_t generation; int64_t pid; std::array<Stream, 2> streams; std::string processId; };
  struct Batch { uint32_t slot; uint64_t lines; std::string bytes; };

  // Reader thread: keeps every pipe empty whatever the sink does.
  void readLoop() {
    std::vector<pollfd> descriptors;
    std::vector<std::pair<size_t, size_t>> owners;
    for (;;) {
      descriptors.assign(1, pollfd{control, POLLIN, 0});
      owners.clear();
      for (size_t i = 0; i < sources.size(); ++i) for (size_t s = 0; s < 2; ++s) {
        if (sources[i].streams[s].fd < 0) continue;
        descriptors.push_back(pollfd{sources[i].streams[s].fd, POLLIN, 0});
        owners.emplace_back(i, s);
      }
      if (poll(descriptors.data(), descriptors.size(), -1) < 0) {
        if (errno == EINTR) continue;
        finalDrain();
        return;
      }
      for (size_t k = 1; k < descriptors.size(); ++k)
        if (descriptors[k].revents) drain(sources[owners[k - 1].first], owners[k - 1].second, 16, SIZE_MAX);
      std::erase_if(sources, [](const Source& source) {
        return source.streams[0].fd < 0 && source.streams[1].fd < 0;
      });
      if (descriptors[0].revents && !receive()) {
        finalDrain();
        return;
      }
    }
  }
  // The server's end of the control socket closes only when the server exits.
  // Every line written before that is already in a pipe: one bounded pass reads
  // it, and orphaned writers that keep the pipes open cannot hold the drainer.
  void finalDrain() {
    shared.serverGoneAtUnixMs.store(wallClock(), std::memory_order_relaxed);
    shared.serverGone.store(1, std::memory_order_release);
    uint64_t total = 0;
    for (auto& source: sources) for (size_t s = 0; s < 2; ++s) {
      total += drain(source, s, SIZE_MAX, OUTPUT_FINAL_DRAIN_LIMIT);
      if (source.streams[s].fd >= 0) finish(source, source.streams[s]);
    }
    sources.clear();
    shared.finalDrainBytes.store(total, std::memory_order_relaxed);
  }
  bool receive() {
    for (;;) {
      iovec io{pending.data() + pendingBytes, pending.size() - pendingBytes};
      union { cmsghdr align; char bytes[CMSG_SPACE(4 * sizeof(int))]; } ancillary;
      msghdr message{};
      message.msg_iov = &io;
      message.msg_iovlen = 1;
      message.msg_control = ancillary.bytes;
      message.msg_controllen = sizeof ancillary.bytes;
      const auto n = recvmsg(control, &message, 0);
      if (n < 0 && errno == EINTR) continue;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
      if (n <= 0) return false;
      for (auto* header = CMSG_FIRSTHDR(&message); header; header = CMSG_NXTHDR(&message, header)) {
        if (header->cmsg_level != SOL_SOCKET || header->cmsg_type != SCM_RIGHTS) continue;
        const auto count = (header->cmsg_len - CMSG_LEN(0)) / sizeof(int);
        for (size_t i = 0; i < count; ++i) {
          int fd;
          std::memcpy(&fd, CMSG_DATA(header) + i * sizeof(int), sizeof fd);
          pendingDescriptors.push_back(fd);
        }
      }
      pendingBytes += n;
      if (pendingBytes < pending.size()) continue;
      AttachMessage record;
      std::memcpy(&record, pending.data(), sizeof record);
      pendingBytes = 0;
      auto received = std::move(pendingDescriptors);
      pendingDescriptors.clear();
      attach(record, received);
    }
  }
  void attach(const AttachMessage& record, std::vector<int>& received) {
    if (record.magic == OUTPUT_QUERY_MAGIC && received.size() == 1) {
      // A log query: the query thread reads its request and answers on this descriptor.
      {
        std::lock_guard lock(queryMutex);
        queryDescriptors.push_back(received[0]);
      }
      queryWake.notify_one();
      return;
    }
    if (record.magic != OUTPUT_MAGIC || record.slot >= OUTPUT_SLOTS || received.size() != 2) {
      for (int fd: received) ::close(fd);
      return;
    }
    for (int fd: received) fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
    auto& slot = shared.slots[record.slot];
    slot.generation.store(record.generation, std::memory_order_relaxed);
    slot.pid.store(record.pid, std::memory_order_relaxed);
    slot.processClass.store(record.processClass, std::memory_order_relaxed);
    slot.attachedAtUnixMs.store(wallClock(), std::memory_order_relaxed);
    slot.openStreams.fetch_add(2, std::memory_order_relaxed);
    if (record.slot > shared.highestSlot.load(std::memory_order_relaxed))
      shared.highestSlot.store(record.slot, std::memory_order_release);
    Source source{record.slot, record.generation, record.pid, {}, {}};
    source.streams[0].fd = received[0];
    source.streams[1].fd = received[1];
    source.processId = std::string(record.processId, strnlen(record.processId, sizeof record.processId));
    sources.push_back(std::move(source));
    {
      std::lock_guard lock(mutex);
      names[record.slot] = std::string(record.processId, strnlen(record.processId, sizeof record.processId));
      classes[record.slot] = record.processClass;
      ++events;
    }
    wake.notify_one();
    shared.attachments.fetch_add(1, std::memory_order_relaxed);
  }
  uint64_t drain(Source& source, size_t index, size_t rounds, size_t limit) {
    auto& stream = source.streams[index];
    uint64_t total = 0;
    for (size_t round = 0; stream.fd >= 0 && round < rounds && total < limit;) {
      const auto n = ::read(stream.fd, readBuffer.data(), readBuffer.size());
      if (n > 0) {
        consume(source, stream, readBuffer.data(), n);
        total += n;
        ++round;
        continue;
      }
      if (n < 0 && errno == EINTR) continue;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
      finish(source, stream);
    }
    return total;
  }
  // Splits into whole lines. A line is queued whole or dropped whole; a line
  // larger than the whole backlog budget can never be queued.
  void consume(Source& source, Stream& stream, const char* data, size_t size) {
    auto& slot = shared.slots[source.slot];
    Batch batch{source.slot, 0, {}};
    batch.bytes.reserve(size + stream.partial.size());
    bool dropped = false;
    for (size_t at = 0; at < size;) {
      const auto* newline = static_cast<const char*>(std::memchr(data + at, '\n', size - at));
      const size_t end = newline ? size_t(newline - data) + 1 : size;
      const size_t piece = end - at;
      if (stream.discarding) {
        stream.discarded += piece;
        if (newline) {
          drop(source, 1, stream.discarded, DROP_OVERSIZED);
          dropped = true;
          stream.discarding = false;
          stream.discarded = 0;
        }
      } else if (stream.partial.size() + piece > budget) {
        const uint64_t length = stream.partial.size() + piece;
        std::string().swap(stream.partial);
        if (newline) {
          drop(source, 1, length, DROP_OVERSIZED);
          dropped = true;
        } else {
          stream.discarding = true;
          stream.discarded = length;
        }
      } else if (!newline) {
        stream.partial.append(data + at, piece);
      } else {
        const uint64_t length = stream.partial.size() + piece;
        // Tenant lines go to their Worker's log whatever happens to them on the way to the sink.
        if (!stream.partial.empty() ? stream.partial.front() == '[' : data[at] == '[') {
          if (stream.partial.empty()) observe(source, &stream == &source.streams[0], std::string_view(data + at, piece));
          else observe(source, &stream == &source.streams[0], stream.partial + std::string(data + at, piece));
        }
        if (slot.backlogBytes.load(std::memory_order_acquire) + batch.bytes.size() + length > budget) {
          drop(source, 1, length, DROP_BACKLOG);
          dropped = true;
        } else {
          batch.bytes.append(stream.partial);
          batch.bytes.append(data + at, piece);
          ++batch.lines;
        }
        if (stream.partial.capacity() > OUTPUT_READ_BYTES) std::string().swap(stream.partial);
        else stream.partial.clear();
      }
      at = end;
    }
    if (batch.lines) enqueue(std::move(batch));
    if (dropped) notify();
  }
  void enqueue(Batch&& batch) {
    auto& slot = shared.slots[batch.slot];
    const uint64_t bytes = batch.bytes.size();
    slot.acceptedLines.fetch_add(batch.lines, std::memory_order_relaxed);
    slot.acceptedBytes.fetch_add(bytes, std::memory_order_relaxed);
    slot.backlogLines.fetch_add(batch.lines, std::memory_order_acq_rel);
    const auto backlog = slot.backlogBytes.fetch_add(bytes, std::memory_order_acq_rel) + bytes;
    if (backlog > slot.highWaterBytes.load(std::memory_order_relaxed))
      slot.highWaterBytes.store(backlog, std::memory_order_relaxed);
    {
      std::lock_guard lock(mutex);
      queue.push_back(std::move(batch));
    }
    wake.notify_one();
  }
  // An unterminated tail is never written: no half line reaches the sink.
  void finish(Source& source, Stream& stream) {
    if (!stream.partial.empty() || stream.discarding) {
      drop(source, 1, stream.partial.size() + stream.discarded, DROP_PARTIAL);
      notify();
    }
    std::string().swap(stream.partial);
    stream.discarding = false;
    stream.discarded = 0;
    closeDescriptor(stream.fd);
    shared.slots[source.slot].openStreams.fetch_sub(1, std::memory_order_relaxed);
  }
  void drop(uint32_t index, uint64_t generation, uint64_t pid, uint64_t lines, uint64_t bytes, DropReason reason) {
    auto& slot = shared.slots[index];
    const auto now = wallClock();
    if (!slot.droppedLines.load(std::memory_order_relaxed)) slot.firstDropUnixMs.store(now, std::memory_order_relaxed);
    slot.lastDropUnixMs.store(now, std::memory_order_relaxed);
    slot.lastDropGeneration.store(generation, std::memory_order_relaxed);
    slot.lastDropPid.store(pid, std::memory_order_relaxed);
    slot.droppedBy[reason].fetch_add(lines, std::memory_order_relaxed);
    slot.droppedBytes.fetch_add(bytes, std::memory_order_relaxed);
    slot.droppedLines.fetch_add(lines, std::memory_order_release);
  }
  void drop(const Source& source, uint64_t lines, uint64_t bytes, DropReason reason) {
    drop(source.slot, source.generation, source.pid, lines, bytes, reason);
  }
  void notify() {
    {
      std::lock_guard lock(mutex);
      ++events;
    }
    wake.notify_one();
  }

  // Reader thread: one whole line of a process that starts with '['. Nothing a process writes on its standard output
  // or error is a Worker's line -- those come from the node (an append request) -- so a line that looks like one is
  // only counted.
  void observe(Source& source, bool, std::string_view line) {
    if (line.starts_with(TENANT_PREFIX)) shared.slots[source.slot].tenantUnverified.fetch_add(1, std::memory_order_relaxed);
  }

  // Query thread: an append request's size, its first line and the lines after it ("!append <processId> <bytes>");
  // 0 for every other request (one line).
  static size_t appendBytes(std::string_view header) {
    if (!header.starts_with("!append ")) return 0;
    const auto space = header.rfind(' ');
    uint64_t bytes = 0;
    const auto digits = header.substr(space + 1);
    if (digits.empty() || digits.size() > 9 || !std::all_of(digits.begin(), digits.end(), [](char c) { return c >= '0' && c <= '9'; }))
      return 0;
    for (char c : digits) bytes = bytes * 10 + static_cast<uint64_t>(c - '0');
    return bytes <= OUTPUT_APPEND_MAX_BYTES ? static_cast<size_t>(bytes) : 0;
  }
  // "!append <processId> <bytes>\n" and that many bytes of tenant lines, one per line as parseTenantLine reads them:
  // what the node received from that process's runtime over its source socket (node/main.c++). Counted for the
  // process's slot.
  std::string append(std::string_view header, std::string_view body) {
    std::istringstream in{std::string(header.substr(1))};
    std::string verb, processId, size;
    in >> verb >> processId >> size;
    if (processId.empty() || processId.size() > 15 || !std::all_of(processId.begin(), processId.end(), nameCharacter) ||
        size != std::to_string(body.size()))
      return "{\"error\":\"invalid append\"}";
    uint64_t accepted = 0, refused = 0;
    for (size_t at = 0; at < body.size();) {
      auto end = body.find('\n', at);
      if (end == std::string_view::npos) end = body.size();
      const auto line = body.substr(at, end - at);
      at = end + 1;
      if (line.empty()) continue;
      if (auto parsed = parseTenantLine(line)) { tenants.add(processId, *parsed); ++accepted; }
      else ++refused;
    }
    {
      std::lock_guard lock(mutex);
      const auto highest = std::min<uint64_t>(shared.highestSlot.load(std::memory_order_acquire), OUTPUT_SLOTS - 1);
      for (uint64_t i = 0; i <= highest; ++i)
        if (names[i] == processId) { shared.slots[i].tenantLines.fetch_add(accepted, std::memory_order_relaxed); break; }
    }
    return "{\"ok\":true,\"accepted\":" + std::to_string(accepted) + ",\"refused\":" + std::to_string(refused) + "}";
  }

  // Query thread: one request per descriptor, "<workerId> <after|-> <sinceUnixMs> <limit>\n" (or a request of the
  // node itself: "!quota", "!usage", "!append" with its lines); the answer is one JSON line, then the descriptor
  // closes. Neither pipe draining nor the sink waits.
  void queryLoop() {
    for (;;) {
      int fd = -1;
      {
        std::unique_lock lock(queryMutex);
        queryWake.wait(lock, [&] { return !queryDescriptors.empty() || queriesDone; });
        if (queryDescriptors.empty()) return;
        fd = queryDescriptors.front();
        queryDescriptors.pop_front();
      }
      answer(fd);
      ::close(fd);
    }
  }
  void answer(int fd) {
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
    const auto deadline = monotonicMs() + OUTPUT_QUERY_IO_MS;
    auto wait = [&](short events) {
      for (;;) {
        const auto now = monotonicMs();
        if (now >= deadline) return false;
        pollfd descriptor{fd, events, 0};
        const int ready = poll(&descriptor, 1, static_cast<int>(deadline - now));
        if (ready < 0 && errno == EINTR) continue;
        return ready > 0;
      }
    };
    std::string request;
    std::vector<char> buffer(OUTPUT_QUERY_READ_BYTES);
    // The whole request's size, once its first line is known (an append's lines follow that line).
    size_t whole = 0;
    for (;;) {
      if (!whole) {
        if (const auto end = request.find('\n'); end != std::string::npos)
          whole = end + 1 + appendBytes(std::string_view(request).substr(0, end));
        else if (request.size() > OUTPUT_QUERY_REQUEST_BYTES) break;
      }
      if (whole && request.size() >= whole) break;
      const auto n = ::read(fd, buffer.data(), buffer.size());
      if (n > 0) {
        request.append(buffer.data(), n);
        continue;
      }
      if (n < 0 && errno == EINTR) continue;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) && wait(POLLIN)) continue;
      break;
    }
    const auto end = request.find('\n');
    if (end == std::string::npos || (whole && request.size() < whole)) {
      shared.queryFailures.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    const auto header = std::string_view(request).substr(0, end);
    const auto text = (header.starts_with("!append ") ? append(header, std::string_view(request).substr(end + 1, whole - end - 1))
        : tenants.query(header)) + "\n";
    size_t done = 0;
    while (done < text.size()) {
      const auto n = ::write(fd, text.data() + done, text.size() - done);
      if (n > 0) { done += n; continue; }
      if (n < 0 && errno == EINTR) continue;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) && wait(POLLOUT)) continue;
      break;
    }
    (done == text.size() ? shared.queries : shared.queryFailures).fetch_add(1, std::memory_order_relaxed);
  }

  // Writer thread: the only writer of the sink, so whole lines never interleave.
  void writeLoop() {
    std::array<uint64_t, OUTPUT_SLOTS> reported{};
    uint64_t seen = 0;
    for (;;) {
      std::optional<Batch> batch;
      bool done = false;
      {
        std::unique_lock lock(mutex);
        wake.wait(lock, [&] { return !queue.empty() || readerDone || events != seen; });
        seen = events;
        if (!queue.empty()) {
          batch = std::move(queue.front());
          queue.pop_front();
        } else {
          done = readerDone;
        }
      }
      if (batch) write(*batch);
      else ensureSink(false);
      // Markers only after a process's backlog drained, as in AsyncLog.
      writeMarkers(reported);
      if (done) break;
    }
    closeDescriptor(sink);
    shared.sinkOpen.store(0, std::memory_order_relaxed);
  }
  bool ensureSink(bool wait) {
    for (uint32_t delayMs = 50; sink < 0;) {
      // A FIFO sink without a reader must not block the open.
      sink = ::open(sinkPath.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC | O_NONBLOCK, 0600);
      if (sink >= 0) {
        fcntl(sink, F_SETFL, fcntl(sink, F_GETFL) & ~O_NONBLOCK);
        shared.sinkOpen.store(1, std::memory_order_relaxed);
        failingSinceMs = 0;
        // Only a regular file rotates; its size so far counts.
        struct stat status;
        sinkRegular = fstat(sink, &status) == 0 && S_ISREG(status.st_mode);
        sinkBytes = sinkRegular ? static_cast<uint64_t>(status.st_size) : 0;
        shared.sinkBytes.store(sinkBytes, std::memory_order_relaxed);
        return true;
      }
      shared.lastSinkErrno.store(errno, std::memory_order_relaxed);
      shared.sinkOpenFailures.fetch_add(1, std::memory_order_relaxed);
      const auto now = monotonicMs();
      if (!failingSinceMs) failingSinceMs = now;
      if (!wait || (shared.serverGone.load(std::memory_order_acquire) && now - failingSinceMs > OUTPUT_SINK_GIVE_UP_MS))
        return false;
      usleep(delayMs * 1000);
      delayMs = std::min(delayMs * 2, 1000u);
    }
    return true;
  }
  void sinkFailed(int error) {
    shared.lastSinkErrno.store(error, std::memory_order_relaxed);
    shared.sinkWriteFailures.fetch_add(1, std::memory_order_relaxed);
    closeDescriptor(sink);
    shared.sinkOpen.store(0, std::memory_order_relaxed);
  }
  // Bytes that reached the sink; at the size limit it moves to <sink>.1, between whole lines.
  void wrote(size_t bytes) {
    sinkBytes += bytes;
    shared.sinkBytes.store(sinkBytes, std::memory_order_relaxed);
    if (!settings.sinkMaxBytes || !sinkRegular || sinkBytes < settings.sinkMaxBytes || sink < 0) return;
    closeDescriptor(sink);
    shared.sinkOpen.store(0, std::memory_order_relaxed);
    auto name = [&](uint32_t index) { return index ? sinkPath + "." + std::to_string(index) : sinkPath; };
    bool failed = false;
    if (settings.sinkFiles <= 1) failed = ::unlink(sinkPath.c_str()) != 0 && errno != ENOENT;
    else for (uint32_t index = settings.sinkFiles - 1; index > 0; --index)
      failed = (::rename(name(index - 1).c_str(), name(index).c_str()) != 0 && errno != ENOENT) || failed;
    (failed ? shared.sinkRotationFailures : shared.sinkRotations).fetch_add(1, std::memory_order_relaxed);
    sinkBytes = 0;
    shared.sinkBytes.store(0, std::memory_order_relaxed);
  }
  void write(Batch& batch) {
    auto& slot = shared.slots[batch.slot];
    const auto* data = batch.bytes.data();
    const size_t size = batch.bytes.size();
    const size_t written = ensureSink(true) ? writeSome(sink, data, size) : 0;
    const int writeError = errno;
    uint64_t lines = batch.lines, bytes = size;
    if (written < size) {
      const int error = writeError;
      if (sink >= 0) sinkFailed(error);
      // Lines completed before the failure count as written; the rest as dropped.
      bytes = written;
      while (bytes && data[bytes - 1] != '\n') --bytes;
      lines = std::count(data, data + bytes, '\n');
      drop(batch.slot, slot.generation.load(std::memory_order_relaxed), slot.pid.load(std::memory_order_relaxed),
          batch.lines - lines, size - bytes, DROP_SINK);
    }
    slot.writtenLines.fetch_add(lines, std::memory_order_relaxed);
    slot.writtenBytes.fetch_add(bytes, std::memory_order_relaxed);
    slot.backlogLines.fetch_sub(batch.lines, std::memory_order_acq_rel);
    slot.backlogBytes.fetch_sub(size, std::memory_order_acq_rel);
    if (written == size) wrote(size);
  }
  void writeMarkers(std::array<uint64_t, OUTPUT_SLOTS>& reported) {
    const auto highest = std::min<uint64_t>(shared.highestSlot.load(std::memory_order_acquire), OUTPUT_SLOTS - 1);
    for (uint32_t i = 0; i <= highest; ++i) {
      auto& slot = shared.slots[i];
      const auto dropped = slot.droppedLines.load(std::memory_order_acquire);
      if (dropped == reported[i] || slot.backlogLines.load(std::memory_order_acquire)) continue;
      if (!ensureSink(false)) return;
      std::string name;
      uint32_t processClass;
      {
        std::lock_guard lock(mutex);
        name = names[i];
        processClass = classes[i];
      }
      std::ostringstream line;
      line << "[log-dropped] {\"source\":\"workerd\",\"processId\":\"" << jsonText(name)
           << "\",\"processClass\":\"" << WorkerdOutput::className(processClass)
           << "\",\"generation\":" << slot.lastDropGeneration.load() << ",\"pid\":" << slot.lastDropPid.load()
           << ",\"count\":" << dropped - reported[i] << ",\"total\":" << dropped << ",\"reasons\":{";
      for (size_t r = 0; r < DROP_REASONS; ++r)
        line << (r ? "," : "") << '"' << DROP_NAMES[r] << "\":" << slot.droppedBy[r].load();
      line << "},\"firstDroppedAtUnixMs\":" << slot.firstDropUnixMs.load()
           << ",\"lastDroppedAtUnixMs\":" << slot.lastDropUnixMs.load() << "}\n";
      const auto text = line.str();
      if (writeSome(sink, text.data(), text.size()) != text.size()) {
        sinkFailed(errno);
        return;
      }
      reported[i] = dropped;
      slot.reportedLines.store(dropped, std::memory_order_relaxed);
      slot.markerLines.fetch_add(1, std::memory_order_relaxed);
      wrote(text.size());
    }
  }

  OutputShared& shared;
  const int control;
  const std::string sinkPath;
  const size_t budget;
  const WorkerLogSettings settings;
  std::vector<char> readBuffer;
  std::vector<Source> sources;
  std::array<char, sizeof(AttachMessage)> pending{};
  size_t pendingBytes = 0;
  std::vector<int> pendingDescriptors;
  // Shared by the two threads.
  std::mutex mutex;
  std::condition_variable wake;
  std::deque<Batch> queue;
  uint64_t events = 0;
  bool readerDone = false;
  std::array<std::string, OUTPUT_SLOTS> names;
  std::array<uint32_t, OUTPUT_SLOTS> classes{};
  // Writer thread only.
  int sink = -1;
  uint64_t failingSinceMs = 0;
  bool sinkRegular = false;
  uint64_t sinkBytes = 0;
  // The Workers' logs: written by the reader thread, read by the query thread.
  TenantLogs tenants;
  std::mutex queryMutex;
  std::condition_variable queryWake;
  std::deque<int> queryDescriptors;
  bool queriesDone = false;
};

[[noreturn]] void runDrainer(OutputShared& shared, int control, const std::string& sinkPath, size_t budget,
    const WorkerLogSettings& settings) {
  signal(SIGTERM, SIG_IGN); signal(SIGINT, SIG_IGN); signal(SIGPIPE, SIG_IGN);
  // Inherited streams belong to the server's log ring; this process never uses them.
  std::cout.rdbuf(nullptr); std::cerr.rdbuf(nullptr); std::clog.rdbuf(nullptr);
  const int null = ::open("/dev/null", O_RDWR);
  if (null < 0) _exit(1);
  for (int fd: {STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO}) dup2(null, fd);
  if (null > STDERR_FILENO) ::close(null);
#ifdef __linux__
  prctl(PR_SET_NAME, "coworkerd-wlog", 0, 0, 0);
#endif
  try {
    closeOtherDescriptors(control);
    fcntl(control, F_SETFL, fcntl(control, F_GETFL) | O_NONBLOCK);
    OutputDrainer(shared, control, sinkPath, budget, settings).run();
  } catch (...) {
    _exit(1);
  }
  _exit(0);
}
}  // namespace

WorkerdOutput::Pipes::~Pipes() {
  closeDescriptor(stdoutRead); closeDescriptor(stdoutWrite);
  closeDescriptor(stderrRead); closeDescriptor(stderrWrite);
}

class WorkerdOutput::Impl {
public:
  Impl(std::string sinkPath, WorkerLogSettings settings): sinkPath(std::move(sinkPath)), budget(outputBudget()),
      settings(settings) {
    if (settings.linesPerSecond == 0 || settings.maxLineBytes < 64 || settings.tenantBufferBytes < 4096 ||
        settings.totalBufferBytes < settings.tenantBufferBytes || settings.sinkFiles == 0 || settings.sinkFiles > 64 ||
        (settings.sinkMaxBytes && settings.sinkMaxBytes < 64 * 1024))
      throw std::invalid_argument("logs: linesPerSecond >= 1, maxLineBytes >= 64, tenantBufferBytes >= 4096, totalBufferBytes >= "
          "tenantBufferBytes, workerdLogFiles 1..64, workerdLogMaxBytes 0 or >= 65536");
    mapping = mmap(nullptr, sizeof(OutputShared), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
    if (mapping == MAP_FAILED) throw std::runtime_error("workerd output mmap failed");
    std::memset(mapping, 0, sizeof(OutputShared));
    shared = new (mapping) OutputShared;
    int pair[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, pair) != 0) {
      munmap(mapping, sizeof(OutputShared));
      throw std::runtime_error("workerd output control socket failed");
    }
    // workerd must never inherit the control socket: its EOF is the server's death.
    for (int fd: pair) fcntl(fd, F_SETFD, FD_CLOEXEC);
    drainerPid = fork();
    if (drainerPid < 0) {
      ::close(pair[0]); ::close(pair[1]); munmap(mapping, sizeof(OutputShared));
      throw std::runtime_error("workerd output fork failed");
    }
    if (drainerPid == 0) {
      ::close(pair[0]);
      runDrainer(*shared, pair[1], this->sinkPath, budget, this->settings);
    }
    ::close(pair[1]);
    control = pair[0];
    while (!shared->ready.load(std::memory_order_acquire)) {
      int status;
      if (waitpid(drainerPid, &status, WNOHANG) == drainerPid) {
        closeDescriptor(control); munmap(mapping, sizeof(OutputShared));
        throw std::runtime_error("workerd output drainer failed to initialize");
      }
      usleep(1000);
    }
  }
  ~Impl() {
    // Runs after every workerd was killed: the drainer reads each pipe to its
    // end, writes the backlog and exits. Outside the event loop, like AsyncLog.
    closeDescriptor(control);
    int status;
    while (!reaped && waitpid(drainerPid, &status, 0) < 0 && errno == EINTR) {}
    munmap(mapping, sizeof(OutputShared));
  }
  void open(Pipes& pipes) {
    int out[2], err[2];
    if (pipe(out) != 0) throw std::system_error(errno, std::generic_category(), "workerd output pipe");
    pipes.stdoutRead = out[0]; pipes.stdoutWrite = out[1];
    if (pipe(err) != 0) throw std::system_error(errno, std::generic_category(), "workerd output pipe");
    pipes.stderrRead = err[0]; pipes.stderrWrite = err[1];
    for (int fd: {out[0], out[1], err[0], err[1]}) fcntl(fd, F_SETFD, FD_CLOEXEC);
#ifdef __linux__
    // Room for scheduling hiccups of the drainer; best effort.
    fcntl(out[1], F_SETPIPE_SZ, OUTPUT_PIPE_BYTES);
    fcntl(err[1], F_SETPIPE_SZ, OUTPUT_PIPE_BYTES);
#endif
  }
  void attach(Pipes& pipes, uint32_t slot, const std::string& processId, uint32_t processClass,
      uint64_t generation, int64_t pid) {
    closeDescriptor(pipes.stdoutWrite);
    closeDescriptor(pipes.stderrWrite);
    AttachMessage record{};
    record.magic = OUTPUT_MAGIC;
    record.slot = slot;
    record.processClass = processClass;
    record.generation = generation;
    record.pid = pid;
    std::memcpy(record.processId, processId.data(), std::min(processId.size(), sizeof record.processId - 1));
    iovec io{&record, sizeof record};
    union { cmsghdr align; char bytes[CMSG_SPACE(2 * sizeof(int))]; } ancillary;
    std::memset(&ancillary, 0, sizeof ancillary);
    msghdr message{};
    message.msg_iov = &io;
    message.msg_iovlen = 1;
    message.msg_control = ancillary.bytes;
    message.msg_controllen = sizeof ancillary.bytes;
    auto* header = CMSG_FIRSTHDR(&message);
    header->cmsg_level = SOL_SOCKET;
    header->cmsg_type = SCM_RIGHTS;
    header->cmsg_len = CMSG_LEN(2 * sizeof(int));
    const int descriptors[2] = {pipes.stdoutRead, pipes.stderrRead};
    std::memcpy(CMSG_DATA(header), descriptors, sizeof descriptors);
    ssize_t sent = -1;
    if (slot < OUTPUT_SLOTS && control >= 0) {
      do sent = sendmsg(control, &message, OUTPUT_SEND_FLAGS); while (sent < 0 && errno == EINTR);
    } else {
      errno = slot < OUTPUT_SLOTS ? EBADF : ERANGE;
    }
    if (sent != ssize_t(sizeof record)) {
      // Without a reader the child's writes fail with EPIPE instead of blocking.
      ++attachFailures;
      lastAttachErrno = sent < 0 ? errno : EPROTO;
      // A partial record would desynchronize the channel: retire the drainer.
      if (sent > 0) closeDescriptor(control);
    }
    closeDescriptor(pipes.stdoutRead);
    closeDescriptor(pipes.stderrRead);
  }
  std::string processJson(uint32_t index) {
    if (index >= OUTPUT_SLOTS) return "null";
    auto& slot = shared->slots[index];
    std::ostringstream out;
    out << "{\"attachedGeneration\":" << slot.generation.load() << ",\"attachedPid\":" << slot.pid.load()
        << ",\"openStreams\":" << slot.openStreams.load() << ",\"backlogBytes\":" << slot.backlogBytes.load()
        << ",\"backlogLines\":" << slot.backlogLines.load() << ",\"highWaterBytes\":" << slot.highWaterBytes.load()
        << ",\"acceptedLines\":" << slot.acceptedLines.load() << ",\"acceptedBytes\":" << slot.acceptedBytes.load()
        << ",\"writtenLines\":" << slot.writtenLines.load() << ",\"writtenBytes\":" << slot.writtenBytes.load()
        << ",\"droppedLines\":" << slot.droppedLines.load() << ",\"droppedBytes\":" << slot.droppedBytes.load()
        << ",\"droppedByReason\":{";
    for (size_t r = 0; r < DROP_REASONS; ++r) out << (r ? "," : "") << '"' << DROP_NAMES[r] << "\":" << slot.droppedBy[r].load();
    out << "},\"firstDroppedAtUnixMs\":" << slot.firstDropUnixMs.load() << ",\"lastDroppedAtUnixMs\":" << slot.lastDropUnixMs.load()
        << ",\"reportedDroppedLines\":" << slot.reportedLines.load() << ",\"markerLines\":" << slot.markerLines.load()
        << ",\"tenantLines\":" << slot.tenantLines.load()
        << ",\"tenantUnverifiedLines\":" << slot.tenantUnverified.load() << "}";
    return out.str();
  }
  std::string json() {
    int status;
    if (!reaped && waitpid(drainerPid, &status, WNOHANG) == drainerPid) reaped = true;
    uint64_t backlogBytes = 0, backlogLines = 0, dropped = 0, accepted = 0, written = 0;
    for (const auto& slot: shared->slots) {
      backlogBytes += slot.backlogBytes.load(); backlogLines += slot.backlogLines.load();
      dropped += slot.droppedLines.load(); accepted += slot.acceptedLines.load(); written += slot.writtenLines.load();
    }
    std::ostringstream out;
    out << "{\"mode\":\"pipe-drainer-process\",\"drainerPid\":" << drainerPid
        << ",\"drainerAlive\":" << (reaped ? "false" : "true") << ",\"sinkPath\":\"" << jsonText(sinkPath)
        << "\",\"sinkOpen\":" << (shared->sinkOpen.load() ? "true" : "false")
        << ",\"bufferBytesPerProcess\":" << budget << ",\"maxLineBytes\":" << budget
        << ",\"attachments\":" << shared->attachments.load() << ",\"attachFailures\":" << attachFailures
        << ",\"lastAttachErrno\":" << lastAttachErrno << ",\"sinkOpenFailures\":" << shared->sinkOpenFailures.load()
        << ",\"sinkWriteFailures\":" << shared->sinkWriteFailures.load() << ",\"lastSinkErrno\":" << shared->lastSinkErrno.load()
        << ",\"backlogBytes\":" << backlogBytes << ",\"backlogLines\":" << backlogLines
        << ",\"acceptedLines\":" << accepted << ",\"writtenLines\":" << written << ",\"droppedLines\":" << dropped
        << ",\"rotation\":{\"maxBytes\":" << settings.sinkMaxBytes << ",\"files\":" << settings.sinkFiles
        << ",\"sinkBytes\":" << shared->sinkBytes.load() << ",\"rotations\":" << shared->sinkRotations.load()
        << ",\"failures\":" << shared->sinkRotationFailures.load()
        << "},\"workerLogs\":{\"tenants\":" << shared->tenantWorkers.load() << ",\"keptLines\":" << shared->tenantKeptLines.load()
        << ",\"droppedLines\":" << shared->tenantDroppedLines.load() << ",\"truncatedLines\":" << shared->tenantTruncatedLines.load()
        << ",\"evictedLines\":" << shared->tenantEvictedLines.load() << ",\"bufferBytes\":" << shared->tenantBufferBytes.load()
        << ",\"queries\":" << shared->queries.load() << ",\"queryFailures\":" << shared->queryFailures.load() + queryFailures
        << ",\"tenantBufferBytes\":" << settings.tenantBufferBytes << ",\"totalBufferBytes\":" << settings.totalBufferBytes
        << ",\"linesPerSecond\":" << settings.linesPerSecond << ",\"maxLineBytes\":" << settings.maxLineBytes << "}}";
    return out.str();
  }

  // A log query: a socket pair; the other end to the drainer with a query record (as an attach sends its pipes),
  // then the request on this end and the answer read to its end, both without blocking (an append's lines may not
  // fit an empty socket buffer, and the drainer reads them only once it holds its end).
  kj::Promise<std::string> workerLogs(kj::LowLevelAsyncIoProvider& provider, std::string request) {
    int pair[2];
    if (control < 0 || socketpair(AF_UNIX, SOCK_STREAM, 0, pair) != 0) {
      ++queryFailures;
      co_return co_await kj::Promise<std::string>(KJ_EXCEPTION(DISCONNECTED, "the workerd output reader is not available"));
    }
    for (int fd: pair) fcntl(fd, F_SETFD, FD_CLOEXEC);
    AttachMessage record{};
    record.magic = OUTPUT_QUERY_MAGIC;
    iovec io{&record, sizeof record};
    union { cmsghdr align; char bytes[CMSG_SPACE(sizeof(int))]; } ancillary;
    std::memset(&ancillary, 0, sizeof ancillary);
    msghdr message{};
    message.msg_iov = &io;
    message.msg_iovlen = 1;
    message.msg_control = ancillary.bytes;
    message.msg_controllen = sizeof ancillary.bytes;
    auto* header = CMSG_FIRSTHDR(&message);
    header->cmsg_level = SOL_SOCKET;
    header->cmsg_type = SCM_RIGHTS;
    header->cmsg_len = CMSG_LEN(sizeof(int));
    std::memcpy(CMSG_DATA(header), &pair[1], sizeof(int));
    ssize_t sent = -1;
    do sent = sendmsg(control, &message, OUTPUT_SEND_FLAGS); while (sent < 0 && errno == EINTR);
    ::close(pair[1]);
    if (sent != ssize_t(sizeof record)) {
      ::close(pair[0]);
      ++queryFailures;
      // A partial record would desynchronize the channel: retire the drainer, as attach does.
      if (sent > 0) closeDescriptor(control);
      co_return co_await kj::Promise<std::string>(KJ_EXCEPTION(DISCONNECTED, "the workerd output reader did not take the query"));
    }
    fcntl(pair[0], F_SETFL, fcntl(pair[0], F_GETFL) | O_NONBLOCK);
    auto stream = provider.wrapSocketFd(pair[0], kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);
    co_await stream->write(kj::arrayPtr(request.data(), request.size()).asBytes());
    auto text = co_await stream->readAllText(64 << 20);
    co_return std::string(text.cStr(), text.size());
  }

private:
  const std::string sinkPath;
  const size_t budget;
  const WorkerLogSettings settings;
  uint64_t queryFailures = 0;
  void* mapping;
  OutputShared* shared;
  pid_t drainerPid;
  int control = -1;
  bool reaped = false;
  uint64_t attachFailures = 0;
  int lastAttachErrno = 0;
};

WorkerdOutput::Impl* WorkerdOutput::active = nullptr;
WorkerdOutput::WorkerdOutput(std::string sinkPath, WorkerLogSettings settings)
    : impl(std::make_unique<Impl>(std::move(sinkPath), settings)) {
  active = impl.get();
}
WorkerdOutput::~WorkerdOutput() { active = nullptr; }
bool WorkerdOutput::open(Pipes& pipes) {
  if (!active) return false;
  active->open(pipes);
  return true;
}
const char* WorkerdOutput::className(uint32_t processClass) {
  return processClass == 2 ? "durable" : processClass == 1 ? "quarantine" : "ordinary";
}
void WorkerdOutput::attach(Pipes& pipes, uint32_t slot, const std::string& processId, uint32_t processClass,
    uint64_t generation, int64_t pid) {
  if (active) active->attach(pipes, slot, processId, processClass, generation, pid);
}
std::string WorkerdOutput::processJson(uint32_t slot) { return active ? active->processJson(slot) : "null"; }
std::string WorkerdOutput::stateJson() { return active ? active->json() : "null"; }
kj::Promise<std::string> WorkerdOutput::workerLogs(kj::LowLevelAsyncIoProvider& provider, const std::string& workerId,
    uint64_t after, uint64_t sinceUnixMs, uint32_t limit) {
  if (!active) return KJ_EXCEPTION(DISCONNECTED, "no workerd output reader in this mode");
  KJ_REQUIRE(!workerId.empty() && workerId.size() <= 320 && std::count(workerId.begin(), workerId.end(), '/') == 1 &&
      std::all_of(workerId.begin(), workerId.end(), [](char c) { return nameCharacter(c) || c == '/'; }), "invalid workerId");
  auto request = workerId + ' ' + (after == UINT64_MAX ? std::string("-") : std::to_string(after)) + ' ' +
      std::to_string(sinceUnixMs) + ' ' + std::to_string(limit) + '\n';
  return active->workerLogs(provider, std::move(request));
}
kj::Promise<std::string> WorkerdOutput::tenantQuota(kj::LowLevelAsyncIoProvider& provider, const std::string& tenant,
    uint64_t linesPerSecond, uint64_t bufferBytes) {
  if (!active) return std::string("{\"ok\":false}");
  KJ_REQUIRE(!tenant.empty() && tenant.size() <= 63 && std::all_of(tenant.begin(), tenant.end(), nameCharacter), "invalid tenant");
  return active->workerLogs(provider, "!quota " + tenant + ' ' + std::to_string(linesPerSecond) + ' ' + std::to_string(bufferBytes) + '\n');
}
kj::Promise<std::string> WorkerdOutput::tenantLines(kj::LowLevelAsyncIoProvider& provider) {
  if (!active) return std::string("{\"tenants\":{}}");
  return active->workerLogs(provider, "!usage\n");
}
kj::Promise<std::string> WorkerdOutput::appendTenantLines(kj::LowLevelAsyncIoProvider& provider, const std::string& processId,
    const std::string& lines) {
  if (!active) return KJ_EXCEPTION(DISCONNECTED, "no workerd output reader in this mode");
  KJ_REQUIRE(!processId.empty() && processId.size() <= 15 && std::all_of(processId.begin(), processId.end(), nameCharacter),
      "invalid processId");
  KJ_REQUIRE(lines.size() <= OUTPUT_APPEND_MAX_BYTES, "too many tenant log bytes in one append");
  return active->workerLogs(provider, "!append " + processId + ' ' + std::to_string(lines.size()) + '\n' + lines);
}
}  // namespace coworkerd
