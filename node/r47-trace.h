#pragma once

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include <kj/async-io.h>

namespace coworkerd::r47 {

enum Kind : uint64_t {
  TIMER = 1,
  CHECKPOINT_SCAN,
  CLOSED_EPOCH_CLEANUP,
  COLD_FILES,
  COLD_LOG_FILES,
  COLD_OBJECT_FILES,
  COLD_REMOVE,
  STATE_ROUTE,
  STATE_JSON,
  STATE_EXPIRY,
  STATE_CORE_FIELDS,
  STATE_DEBITS,
  STATE_EXECUTIONS,
  STATE_FLATTEN,
  STATE_ASYNC,
  STATE_SNAPSHOT,
  STATE_POOL,
  STATE_MEMORY,
  STATE_SLICE,
  SEAL_BUILD,
  SEAL_COMMIT,
  SEAL_POSITION,
  USAGE_BUILD,
  STATE_SECTION_BASE = 100,
};

struct Record {
  uint64_t sequence, kind, beganNs, endedNs, cpuNs, value;
};

struct Trace {
  static constexpr uint64_t CAPACITY = 1 << 17;
  uint64_t version, count, capacity, intervalNs;
  Record data[CAPACITY];
};

inline Trace* trace = nullptr;

inline uint64_t clockNs(clockid_t clock) {
  timespec now{};
  clock_gettime(clock, &now);
  return uint64_t(now.tv_sec) * 1000000000 + now.tv_nsec;
}

inline void openTrace() {
  const char* path = std::getenv("COWORKERD_R47_TRACE");
  if (!path || !*path) return;
  const int fd = open(path, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) throw std::runtime_error("could not create r47 trace");
  auto buffer = ftruncate(fd, sizeof(Trace)) == 0
      ? mmap(nullptr, sizeof(Trace), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) : MAP_FAILED;
  close(fd);
  if (buffer == MAP_FAILED) throw std::runtime_error("could not map r47 trace");
  std::memset(buffer, 0, sizeof(Trace));
  trace = static_cast<Trace*>(buffer);
  trace->version = 1;
  trace->capacity = Trace::CAPACITY;
  trace->intervalNs = 1000000;
}

inline void record(uint64_t kind, uint64_t began, uint64_t ended, uint64_t cpu, uint64_t value) {
  const uint64_t sequence = trace->count + 1;
  trace->data[(sequence - 1) % Trace::CAPACITY] = {sequence, kind, began, ended, cpu, value};
  std::atomic_thread_fence(std::memory_order_release);
  trace->count = sequence;
}

class Span {
public:
  explicit Span(uint64_t kind): kind(kind) {
    if (!trace) return;
    began = clockNs(CLOCK_MONOTONIC);
    cpu = clockNs(CLOCK_THREAD_CPUTIME_ID);
  }

  Span(const Span&) = delete;
  Span& operator=(const Span&) = delete;

  ~Span() { finish(); }

  void finish(uint64_t value = 0) {
    if (!began) return;
    const auto ended = clockNs(CLOCK_MONOTONIC);
    record(kind, began, ended, clockNs(CLOCK_THREAD_CPUTIME_ID) - cpu, value);
    began = 0;
  }

private:
  uint64_t kind, began = 0, cpu = 0;
};

inline kj::Promise<void> sampleTimer(kj::Timer& timer) {
  for (;;) {
    const auto cpu = clockNs(CLOCK_THREAD_CPUTIME_ID);
    const auto due = clockNs(CLOCK_MONOTONIC) + trace->intervalNs;
    co_await timer.atTime(kj::systemPreciseMonotonicClock().now() + 1 * kj::MILLISECONDS);
    const auto fired = clockNs(CLOCK_MONOTONIC);
    record(TIMER, due, fired, clockNs(CLOCK_THREAD_CPUTIME_ID) - cpu, 0);
  }
}

inline kj::Promise<void> runTimer(kj::Timer& timer) {
  if (!trace) return kj::NEVER_DONE;
  return sampleTimer(timer);
}

}  // namespace coworkerd::r47
