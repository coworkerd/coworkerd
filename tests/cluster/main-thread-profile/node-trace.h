#pragma once

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <stdexcept>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

namespace coworkerd::benchmarkTrace {

// Single writer on the node main thread. The benchmark reads after requests drain.
struct Record {
  uint64_t timestampNs, kind, first, second;
};
struct Ring {
  static constexpr uint64_t CAPACITY = 1 << 17;
  uint64_t version, count, capacity;
  Record events[CAPACITY];
};
inline Ring* ring = nullptr;

inline uint64_t now(clockid_t clock) {
  timespec ts{};
  clock_gettime(clock, &ts);
  return uint64_t(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}

inline void record(Record event) {
  ring->events[ring->count % Ring::CAPACITY] = event;
  std::atomic_thread_fence(std::memory_order_release);
  ++ring->count;
}

inline void admission(uint32_t roots, uint32_t nested) {
  if (ring != nullptr) record({now(CLOCK_MONOTONIC), 0, roots, nested});
}

inline void open() {
  const char* path = std::getenv("COWORKERD_BENCHMARK_TRACE");
  if (!path || !*path) return;
  int fd = ::open(path, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) throw std::runtime_error("could not create benchmark trace");
  void* memory = ftruncate(fd, sizeof(Ring)) == 0
      ? mmap(nullptr, sizeof(Ring), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0) : MAP_FAILED;
  close(fd);
  if (memory == MAP_FAILED) throw std::runtime_error("could not map benchmark trace");
  ring = static_cast<Ring*>(memory);
  ring->version = 1;
  ring->capacity = Ring::CAPACITY;
  admission(0, 0);
}

class LocalFlush {
public:
  LocalFlush() {
    if (ring == nullptr) return;
    began = now(CLOCK_MONOTONIC);
    cpu = now(CLOCK_THREAD_CPUTIME_ID);
  }

  void finish() {
    if (ring == nullptr) return;
    auto cpuNs = now(CLOCK_THREAD_CPUTIME_ID) - cpu;
    auto ended = now(CLOCK_MONOTONIC);
    record({ended, 1, ended - began, cpuNs});
  }

private:
  uint64_t began = 0, cpu = 0;
};
}  // namespace coworkerd::benchmarkTrace
