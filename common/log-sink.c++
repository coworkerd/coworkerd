#include "log-sink.h"

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
namespace {
constexpr size_t DEFAULT_CAPACITY = 4 * 1024 * 1024;
constexpr size_t MAX_LINE = 64 * 1024;
constexpr std::array SOURCES = {"server", "execution", "decision", "decision-time",
  "static-attempt", "root", "refusal-count", "cold-start", "phase-time", "peer-send",
  "allocation", "refusal", "attempt", "peer-open", "process", "process-kill",
  "process-pool", "suspect", "membership", "deployment", "kj", "stdout", "stderr"};
static_assert(std::atomic<uint64_t>::is_always_lock_free);
static_assert(std::atomic<uint32_t>::is_always_lock_free);

struct Drop {
  std::atomic<uint64_t> count{0}, firstUnixMs{0}, lastUnixMs{0};
};
struct alignas(64) Shared {
  alignas(64) std::atomic<uint64_t> tail{0};
  alignas(64) std::atomic<uint64_t> head{0};
  std::atomic<uint64_t> accepted{0}, written{0}, highWaterBytes{0};
  std::atomic<uint64_t> clockUnixMs{0};
  std::atomic<uint32_t> closed{0}, writerError{0}, ready{0};
  std::array<Drop, SOURCES.size()> drops;
};
struct Header { uint32_t size, descriptor; };
uint64_t wallClock() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}
size_t capacity() {
  const char* value = std::getenv("COWORKERD_LOG_BUFFER_BYTES");
  if (!value) return DEFAULT_CAPACITY;
  char* end;
  errno = 0;
  auto result = std::strtoull(value, &end, 10);
  if (errno || *value == '-' || *end || result < 2 * MAX_LINE || result > 64 * 1024 * 1024)
    throw std::invalid_argument("COWORKERD_LOG_BUFFER_BYTES must be 131072..67108864");
  return result;
}
size_t source(std::string_view line, int descriptor) {
  if (line.starts_with('[')) {
    const auto end = line.find(']');
    if (end != std::string_view::npos) {
      auto name = line.substr(1, end - 1);
      for (size_t i = 0; i < SOURCES.size() - 2; ++i) if (name == SOURCES[i]) return i;
    }
  }
  return SOURCES.size() - (descriptor == STDOUT_FILENO ? 2 : 1);
}
void copyInto(char* ring, size_t cap, uint64_t offset, const void* data, size_t size) {
  auto at = offset % cap;
  auto first = std::min<size_t>(size, cap - at);
  std::memcpy(ring + at, data, first);
  std::memcpy(ring, static_cast<const char*>(data) + first, size - first);
}
void copyOut(const char* ring, size_t cap, uint64_t offset, void* data, size_t size) {
  auto at = offset % cap;
  auto first = std::min<size_t>(size, cap - at);
  std::memcpy(data, ring + at, first);
  std::memcpy(static_cast<char*>(data) + first, ring, size - first);
}
bool writeAll(int fd, const char* data, size_t size) {
  while (size) {
    auto n = ::write(fd, data, size);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) return false;
    data += n; size -= n;
  }
  return true;
}
}  // namespace

class AsyncLog::Impl {
public:
  class LineBuffer final: public std::streambuf {
  public:
    LineBuffer(Impl& owner, int descriptor, char* buffer): owner(owner), descriptor(descriptor), buffer(buffer) {}
    int sync() override { return 0; }  // unitbuf/flush never writes a partial line.
    std::streamsize xsputn(const char* data, std::streamsize size) override {
      for (std::streamsize i = 0; i < size; ++i) {
        if (used < MAX_LINE) buffer[used++] = data[i]; else oversized = true;
        if (data[i] == '\n') {
          owner.enqueue(buffer, used, descriptor, oversized);
          used = 0; oversized = false;
        }
      }
      return size;
    }
    int_type overflow(int_type c) override {
      if (!traits_type::eq_int_type(c, traits_type::eof())) {
        char ch = traits_type::to_char_type(c); xsputn(&ch, 1);
      }
      return traits_type::not_eof(c);
    }
    void finish() { if (used || oversized) { char ch = '\n'; xsputn(&ch, 1); } }
  private:
    Impl& owner;
    int descriptor;
    char* buffer;
    size_t used = 0;
    bool oversized = false;
  };

  Impl(): cap(capacity()), mappingBytes(sizeof(Shared) + cap + 2 * MAX_LINE) {
    mapping = mmap(nullptr, mappingBytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
    if (mapping == MAP_FAILED) throw std::runtime_error("async log mmap failed");
    std::memset(mapping, 0, mappingBytes);
    // Prefault all producer memory. Small deployment memlock limits must not prevent startup.
    locked = mlock(mapping, mappingBytes) == 0;
    if (!locked) std::cerr << "[log-warning] async log mlock failed; continuing with prefaulted memory: "
                          << std::strerror(errno) << std::endl;
    shared = new (mapping) Shared;
    shared->clockUnixMs.store(wallClock(), std::memory_order_relaxed);
    ring = static_cast<char*>(mapping) + sizeof(Shared);
    int pipeFd[2];
    if (pipe(pipeFd) != 0) { release(); throw std::runtime_error("async log liveness pipe failed"); }
    for (int fd: pipeFd) fcntl(fd, F_SETFD, FD_CLOEXEC);
    writerPid = fork();
    if (writerPid < 0) {
      close(pipeFd[0]); close(pipeFd[1]); release(); throw std::runtime_error("async log fork failed");
    }
    if (writerPid == 0) {
      close(pipeFd[1]);
      signal(SIGTERM, SIG_IGN); signal(SIGINT, SIG_IGN); signal(SIGPIPE, SIG_IGN);
#ifdef __linux__
      prctl(PR_SET_NAME, "coworkerd-log", 0, 0, 0);
#endif
      // The child is the sole writer and owns its mapping until the last record.
      // It is intentionally not coupled to the parent's death signal.
      consume(pipeFd[0]);
      _exit(shared->writerError.load(std::memory_order_relaxed) ? 1 : 0);
    }
    close(pipeFd[0]); livenessFd = pipeFd[1];
    while (!shared->ready.load(std::memory_order_acquire)) {
      int status;
      if (waitpid(writerPid, &status, WNOHANG) == writerPid) {
        close(livenessFd); release(); throw std::runtime_error("async log writer failed to initialize");
      }
      usleep(1000);
    }
    out = std::make_unique<LineBuffer>(*this, STDOUT_FILENO, ring + cap);
    err = std::make_unique<LineBuffer>(*this, STDERR_FILENO, ring + cap + MAX_LINE);
    oldOut = std::cout.rdbuf(out.get()); oldErr = std::cerr.rdbuf(err.get());
  }

  ~Impl() {
    out->finish(); err->finish();
    std::cout.rdbuf(oldOut); std::cerr.rdbuf(oldErr);
    shared->closed.store(1, std::memory_order_release);
    close(livenessFd);
    // Shutdown is outside the event loop: wait for all accepted lines to drain.
    int status;
    while (waitpid(writerPid, &status, 0) < 0 && errno == EINTR) {}
    release();
  }

  // Single producer: the server's event-loop thread. No allocation, lock, clock
  // read, descriptor I/O, notification syscall, or retry is allowed here.
  void enqueue(const char* data, size_t size, int descriptor, bool oversized) noexcept {
    const auto tail = shared->tail.load(std::memory_order_relaxed);
    const auto head = shared->head.load(std::memory_order_acquire);
    const auto bytes = sizeof(Header) + size;
    if (oversized || tail - head + bytes > cap || shared->writerError.load(std::memory_order_relaxed)) {
      auto& drop = shared->drops[source({data, size}, descriptor)];
      auto now = shared->clockUnixMs.load(std::memory_order_relaxed);
      if (!drop.count.load(std::memory_order_relaxed)) drop.firstUnixMs.store(now, std::memory_order_relaxed);
      drop.lastUnixMs.store(now, std::memory_order_relaxed);
      drop.count.fetch_add(1, std::memory_order_release);
      return;
    }
    Header header{static_cast<uint32_t>(size), static_cast<uint32_t>(descriptor)};
    copyInto(ring, cap, tail, &header, sizeof(header));
    copyInto(ring, cap, tail + sizeof(header), data, size);
    shared->accepted.fetch_add(1, std::memory_order_relaxed);
    auto high = shared->highWaterBytes.load(std::memory_order_relaxed);
    if (tail - head + bytes > high) shared->highWaterBytes.store(tail - head + bytes, std::memory_order_relaxed);
    // This is the ownership/acceptance boundary. SIGKILL cannot unpublish it.
    shared->tail.store(tail + bytes, std::memory_order_release);
  }

  std::string json() {
    int status;
    if (!reaped && waitpid(writerPid, &status, WNOHANG) == writerPid) {
      reaped = true;
      shared->writerError.store(1, std::memory_order_relaxed);
    }
    const auto head = shared->head.load(std::memory_order_acquire);
    const auto tail = shared->tail.load(std::memory_order_acquire);
    uint64_t dropped = 0;
    std::ostringstream output;
    output << "{\"mode\":\"shared-memory-process\",\"writerPid\":" << writerPid
      << ",\"locked\":" << (locked ? "true" : "false")
      << ",\"writerAlive\":" << (reaped ? "false" : "true")
      << ",\"writerError\":" << shared->writerError.load() << ",\"bufferBytes\":" << cap
      << ",\"maxLineBytes\":" << MAX_LINE << ",\"backlogBytes\":" << tail - head
      << ",\"backlogLines\":" << shared->accepted.load() - shared->written.load()
      << ",\"acceptedLines\":" << shared->accepted.load() << ",\"writtenLines\":" << shared->written.load()
      << ",\"highWaterBytes\":" << shared->highWaterBytes.load() << ",\"droppedBySource\":{";
    for (size_t i = 0; i < SOURCES.size(); ++i) {
      auto count = shared->drops[i].count.load(std::memory_order_acquire); dropped += count;
      if (i) output << ',';
      output << '"' << SOURCES[i] << "\":" << count;
    }
    output << "},\"droppedLines\":" << dropped << "}";
    return output.str();
  }
  Shared* shared;
private:
  void release() { if (locked) munlock(mapping, mappingBytes); munmap(mapping, mappingBytes); }
  void consume(int liveness) {
    shared->ready.store(1, std::memory_order_release);
    std::array<uint64_t, SOURCES.size()> reported{};
    bool parentGone = false;
    for (;;) {
      pollfd descriptor{liveness, POLLIN | POLLHUP, 0};
      if (poll(&descriptor, 1, 0) > 0 && (descriptor.revents & POLLHUP)) parentGone = true;
      // Observe completion before the queue snapshot: the final published line
      // and drop counters must be visible before deciding that draining is done.
      if (shared->closed.load(std::memory_order_acquire)) parentGone = true;
      auto head = shared->head.load(std::memory_order_relaxed);
      auto tail = shared->tail.load(std::memory_order_acquire);
      if (head != tail) {
        Header header; copyOut(ring, cap, head, &header, sizeof(header));
        auto at = (head + sizeof(header)) % cap;
        auto first = std::min<size_t>(header.size, cap - at);
        if (!writeAll(header.descriptor, ring + at, first) ||
            !writeAll(header.descriptor, ring, header.size - first)) {
          shared->writerError.store(1, std::memory_order_release); return;
        }
        shared->written.fetch_add(1, std::memory_order_relaxed);
        shared->head.store(head + sizeof(header) + header.size, std::memory_order_release);
      }
      // Only the writer emits recovery markers; they never consume ring space.
      // Wait for the backlog to drain so markers do not amplify an overload.
      if (shared->head.load(std::memory_order_relaxed) != shared->tail.load(std::memory_order_acquire)) continue;
      for (size_t i = 0; i < SOURCES.size(); ++i) {
        auto& drop = shared->drops[i];
        auto count = drop.count.load(std::memory_order_acquire);
        if (count == reported[i]) continue;
        std::ostringstream line;
        line << "[log-dropped] {\"source\":\"" << SOURCES[i] << "\",\"count\":" << count - reported[i]
          << ",\"total\":" << count << ",\"firstDroppedAtUnixMs\":" << drop.firstUnixMs.load()
          << ",\"lastDroppedAtUnixMs\":" << drop.lastUnixMs.load() << "}\n";
        auto text = line.str();
        if (!writeAll(STDOUT_FILENO, text.data(), text.size())) {
          shared->writerError.store(1, std::memory_order_release); return;
        }
        reported[i] = count;
      }
      if (head == tail) {
        if (parentGone) return;
        // Idle writer polling is entirely outside the server event loop.
        poll(&descriptor, 1, 1);
      }
    }
  }
  bool locked = false;
  size_t cap, mappingBytes;
  void* mapping;
  char* ring;
  pid_t writerPid;
  int livenessFd;
  bool reaped = false;
  std::unique_ptr<LineBuffer> out, err;
  std::streambuf *oldOut, *oldErr;
};

AsyncLog::Impl* AsyncLog::active = nullptr;
AsyncLog::AsyncLog(): impl(std::make_unique<Impl>()) { active = impl.get(); }
AsyncLog::~AsyncLog() { active = nullptr; }
std::string AsyncLog::stateJson() { return active ? active->json() : "{}"; }
void AsyncLog::updateClock() {
  if (active) active->shared->clockUnixMs.store(wallClock(), std::memory_order_relaxed);
}

kj::Promise<void> runLogClock(kj::Timer& timer) {
  for (;;) {
    coworkerd::AsyncLog::updateClock();
    co_await timer.afterDelay(10 * kj::MILLISECONDS);
  }
}
}  // namespace coworkerd
