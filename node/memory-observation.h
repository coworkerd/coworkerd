#pragma once

#include <cstdint>
#include <sstream>
#include <string>

namespace coworkerd {

// Counts belong to the event-loop thread. Bytes are retained payload/accounted buffer bytes, not an estimate of
// allocator overhead; mallinfo2 accounts for the latter. Never expose container keys (which may be tenant data).
class MemoryContainers {
public:
  void count(const char* name, uint64_t entries) {
    separator(name);
    out << "{\"entries\":" << entries << '}';
  }
  void count(const char* name, uint64_t entries, uint64_t bytes) {
    separator(name);
    out << "{\"entries\":" << entries << ",\"bytes\":" << bytes << '}';
  }
  std::string json() const { return "{" + out.str() + "}"; }
private:
  void separator(const char* name) { out << (first ? "" : ",") << '"' << name << "\":"; first = false; }
  std::ostringstream out;
  bool first = true;
};

}  // namespace coworkerd
