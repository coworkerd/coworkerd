#pragma once

#include <cstdint>
#include <fcntl.h>
#include <sstream>
#include <string>
#include <string_view>
#include <unistd.h>

namespace coworkerd {

// The oom_kill counter of a memory.events text ("oom_kill N" among its "key value" lines; 0 without one).
inline uint64_t oomKillsIn(std::string_view text) {
  std::istringstream input{std::string(text)};
  std::string key; uint64_t value;
  while (input >> key >> value) if (key == "oom_kill") return value;
  return 0;
}

// A cgroup's memory.events, which the process pool reads every 10 ms: one descriptor kept open and read from its
// start (the kernel regenerates the text at offset 0), not an open, read and close each time.
class MemoryEventsReader {
public:
  explicit MemoryEventsReader(std::string path): path(std::move(path)), fd(::open(this->path.c_str(), O_RDONLY | O_CLOEXEC)) {}
  ~MemoryEventsReader() { if (fd >= 0) ::close(fd); }
  MemoryEventsReader(const MemoryEventsReader&) = delete;
  MemoryEventsReader& operator=(const MemoryEventsReader&) = delete;
  uint64_t oomKills() const {
    if (fd < 0) return 0;
    char text[512];
    const auto n = pread(fd, text, sizeof(text), 0);
    return n > 0 ? oomKillsIn(std::string_view(text, size_t(n))) : 0;
  }
  const std::string path;

private:
  const int fd;
};

}  // namespace coworkerd
