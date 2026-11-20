#pragma once

#include <list>
#include <iterator>
#include <map>
#include <string>
#include <unordered_map>

namespace coworkerd {

// One entry per hot version, ordered by its last touch. Admission only looks
// up its own version; maintenance removes the expired prefix without a scan.
class ProcessHeat {
public:
  ProcessHeat() = default;
  ProcessHeat(const ProcessHeat&) = delete;
  ProcessHeat& operator=(const ProcessHeat&) = delete;
  void touch(const std::string& hash, double now) {
    const auto found = positions.find(hash);
    if (found == positions.end()) {
      order.push_back(hash); positions.emplace(hash, std::prev(order.end()));
    } else order.splice(order.end(), order, found->second);
    values[hash] = now;
  }
  void expire(double now, double idleMs) {
    while (!order.empty()) {
      const auto found = values.find(order.front());
      if (now - found->second <= idleMs) break;
      positions.erase(found->first); values.erase(found); order.pop_front();
    }
  }
  auto find(const std::string& hash) const { return values.find(hash); }
  auto begin() const { return values.begin(); }
  auto end() const { return values.end(); }
  size_t size() const { return values.size(); }
private:
  std::map<std::string, double> values;
  std::list<std::string> order;
  std::unordered_map<std::string, std::list<std::string>::iterator> positions;
};

}  // namespace coworkerd
