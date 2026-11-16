#pragma once

#include "durable-log.h"
#include <functional>

namespace coworkerd {

// The history reader has only GET capability. Its identity always names the
// source; the RootStorageRef passed to the writer always names the destination.
using HistoryGet = std::function<kj::Promise<ObjectResponse>(std::string)>;
using HistoryYield = std::function<kj::Promise<void>()>;
using HistoryCheck = std::function<void()>;
using HistoryLog = std::function<void(const std::string&, const std::string&)>;
struct HistoryBudget {
  uint64_t payloadLimit = 128ull << 20, recordLimit = 100000, memoryLimit = 512ull << 20;
  uint64_t transientLimit = 384ull << 20, transientHighWater = 0;
  void transient(uint64_t required);
  uint64_t payloadBytes = 0, records = 0, memoryHighWater = 0;
  std::function<void(uint64_t, uint64_t, const std::string&)> refuse;
  void memory(uint64_t required);
  void add(const LogRecord& record);
};

struct FileDigest { uint64_t bytes = 0; std::string sha256; };
FileDigest recoveryFileDigest(const std::filesystem::path& path);
kj::Promise<std::vector<LogRecord>> readRecoverySegments(HistoryGet get, const ObjectManifest& manifest,
    const std::string& objectName, uint32_t parallelism, HistoryCheck check = {}, HistoryLog log = {},
    HistoryBudget* budget = nullptr, HistoryYield yield = {});
kj::Promise<void> downloadRecoveryPart(HistoryGet get, const ObjectManifest& manifest,
    uint32_t facetId, uint64_t part, int fd, off_t offset, HistoryCheck check = {});
kj::Promise<void> downloadRecoverySnapshot(HistoryGet get, const ObjectManifest& manifest,
    const RootStorageRef& destination, const std::filesystem::path& stack, const std::string& staging,
    uint64_t partBytes, uint32_t parallelism, HistoryCheck check = {});

}  // namespace coworkerd
