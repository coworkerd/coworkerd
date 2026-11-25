#include "durable-log.h"
#include "r47-trace.h"
#include "background-value.h"
#include "recovery-history.h"
#include "durable-shards.h"

#include "memory-observation.h"
#include "memory-watermark.h"
#include "json.h"
#include "object-state.h"
#include "support.h"

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <fcntl.h>
#include <kj/debug.h>
#include <openssl/evp.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <unistd.h>
#include <time.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <array>
#include <cstring>
#include <iostream>
#include <iterator>
#include <random>
#include <sstream>

namespace coworkerd {
namespace {
using json::quote;

uint64_t decimal(const std::string& text) {
  uint64_t value = 0;
  for (char c : text) {
    KJ_REQUIRE(c >= '0' && c <= '9' && value < (1ull << 60), "a number of decimal digits");
    value = value * 10 + uint64_t(c - '0');
  }
  return value;
}

std::string wide(uint64_t value, size_t digits = REVISION_DIGITS) {
  auto text = std::to_string(value);
  return text.size() >= digits ? text : std::string(digits - text.size(), '0') + text;
}

// Little-endian reading and writing of the record and segment formats.
void putU8(std::string& out, uint8_t value) { out.push_back(char(value)); }
void putU32(std::string& out, uint32_t value) {
  for (int i = 0; i < 4; i++) out.push_back(char((value >> (8 * i)) & 0xff));
}
void putU64(std::string& out, uint64_t value) {
  for (int i = 0; i < 8; i++) out.push_back(char((value >> (8 * i)) & 0xff));
}
void putText(std::string& out, const std::string& value) {
  KJ_REQUIRE(value.size() <= 0xffff, "a text of a record is at most 65535 bytes");
  putU8(out, uint8_t(value.size() & 0xff));
  putU8(out, uint8_t(value.size() >> 8));
  out += value;
}
void putBytes(std::string& out, const std::string& value) {
  putU32(out, uint32_t(value.size()));
  out += value;
}

bool syncFile(int fd) {
#ifdef __APPLE__
  return ::fsync(fd) == 0;
#else
  return ::fdatasync(fd) == 0;
#endif
}
// A complete predecessor file can prove absence. A partial OS read ending on
// a record boundary must not be mistaken for a complete log. Startup is the
// only writer here, so the descriptor must name a stable regular file.
// Yield only while the initial startup gate is closed to all listeners (see
// main). A retry after serving started keeps its original synchronous slices.
class StartupSlice {
public:
  explicit StartupSlice(bool enabled): enabled(enabled) {}
  bool elapsed() {
    if (!enabled) return false;
    if (++items < 64 && monotonicTimeMs() < deadline) return false;
    return true;
  }
  void reset() { items = 0; deadline = monotonicTimeMs() + 0.75; }
private:
  bool enabled;
  unsigned items = 0;
  double deadline = monotonicTimeMs() + 0.75;
};

std::optional<std::string> readLocalHistory(const std::filesystem::path& path) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK);
  if (fd < 0) return std::nullopt;
  const auto close = kj::defer([fd]() { ::close(fd); });
  struct stat before, after;
  if (::fstat(fd, &before) != 0 || !S_ISREG(before.st_mode) || before.st_size < 0) return std::nullopt;
  std::string bytes(size_t(before.st_size), '\0');
  size_t offset = 0;
  while (offset < bytes.size()) {
    const auto n = ::read(fd, bytes.data() + offset, std::min<size_t>(bytes.size() - offset, 1u << 20));
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) return std::nullopt;
    offset += size_t(n);
  }
  if (::fstat(fd, &after) != 0 || after.st_size != before.st_size) return std::nullopt;
  return bytes;
}
// The same snapshot checks as readLocalHistory, without one large zero-fill
// followed by the read. Checkpoints are between reads; a single syscall still
// runs to completion, and a prefix from a changed file is never accepted.
kj::Promise<std::optional<std::string>> readStartupHistory(const std::filesystem::path& path,
    kj::Timer& timer, bool slice) {
  if (!slice) co_return readLocalHistory(path);
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK);
  if (fd < 0) co_return std::nullopt;
  const auto close = kj::defer([fd]() { ::close(fd); });
  struct stat before, after;
  if (::fstat(fd, &before) != 0 || !S_ISREG(before.st_mode) || before.st_size < 0) co_return std::nullopt;
  std::string bytes;
  bytes.reserve(size_t(before.st_size));
  auto chunk = kj::heapArray<char>(std::min<size_t>(size_t(before.st_size), 256u << 10));
  StartupSlice budget(true);
  while (bytes.size() < size_t(before.st_size)) {
    const auto n = ::read(fd, chunk.begin(), std::min(chunk.size(), size_t(before.st_size) - bytes.size()));
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) co_return std::nullopt;
    bytes.append(chunk.begin(), size_t(n));
    if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
  }
  if (::fstat(fd, &after) != 0 || after.st_size != before.st_size) co_return std::nullopt;
  co_return bytes;
}

uint64_t peakResidentBytes() {
  struct rusage usage;
  if (::getrusage(RUSAGE_SELF, &usage) != 0) return 0;
#ifdef __APPLE__
  return uint64_t(usage.ru_maxrss);
#else
  return uint64_t(usage.ru_maxrss) * 1024;
#endif
}
bool syncDirectory(const std::filesystem::path& directory) {
  const int fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) return false;
  const bool ok = ::fsync(fd) == 0;
  ::close(fd);
  return ok;
}
bool atomicLocalFile(const std::filesystem::path& path, const std::string& bytes, bool sync) {
  const auto temporary = std::filesystem::path(path.string() + ".new");
  const int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
  if (fd < 0) return false;
  size_t done = 0;
  while (done < bytes.size()) {
    const auto written = ::write(fd, bytes.data() + done, bytes.size() - done);
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) break;
    done += size_t(written);
  }
  bool ok = done == bytes.size() && (!sync || syncFile(fd));
  if (::close(fd) != 0) ok = false;
  if (!ok) return false;
  std::error_code code;
  std::filesystem::rename(temporary, path, code);
  return !code && (!sync || syncDirectory(path.parent_path()));
}

struct Reader {
  std::string_view bytes;
  size_t at = 0;
  bool ok = true;
  bool need(size_t count) {
    if (!ok || bytes.size() - at < count) { ok = false; return false; }
    return true;
  }
  uint8_t u8() { return need(1) ? uint8_t(bytes[at++]) : 0; }
  uint32_t u32() {
    if (!need(4)) return 0;
    uint32_t value = 0;
    for (int i = 0; i < 4; i++) value |= uint32_t(uint8_t(bytes[at + i])) << (8 * i);
    at += 4;
    return value;
  }
  uint64_t u64() {
    if (!need(8)) return 0;
    uint64_t value = 0;
    for (int i = 0; i < 8; i++) value |= uint64_t(uint8_t(bytes[at + i])) << (8 * i);
    at += 8;
    return value;
  }
  std::string text() {
    if (!need(2)) return {};
    size_t length = size_t(uint8_t(bytes[at])) | (size_t(uint8_t(bytes[at + 1])) << 8);
    at += 2;
    if (!need(length)) return {};
    std::string value(bytes.substr(at, length));
    at += length;
    return value;
  }
  std::string block(size_t length) {
    if (!need(length)) return {};
    std::string value(bytes.substr(at, length));
    at += length;
    return value;
  }
  std::string sized() {
    uint32_t length = u32();
    return block(length);
  }
};

uint32_t beU32(const unsigned char* p) {
  return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | uint32_t(p[3]);
}

// SQLite's walChecksumBytes(): pairs of 32-bit words, native byte order unless the WAL magic's low bit asks for
// big-endian words (the same rule the shipper applies, so a replay checks what the owner read).
void walChecksum(bool bigEndianWords, const unsigned char* data, size_t size, uint32_t& s1, uint32_t& s2) {
  uint32_t a = s1, b = s2;
  if (bigEndianWords) {
    for (size_t i = 0; i + 8 <= size; i += 8) {
      a += beU32(data + i) + b;
      b += beU32(data + i + 4) + a;
    }
  } else {
    for (size_t i = 0; i + 8 <= size; i += 8) {
      uint32_t w[2];
      std::memcpy(w, data + i, 8);
      a += w[0] + b;
      b += w[1] + a;
    }
  }
  s1 = a;
  s2 = b;
}

const std::array<uint32_t, 256>& crcTable() {
  static const auto table = []() {
    std::array<uint32_t, 256> values{};
    for (uint32_t i = 0; i < 256; i++) {
      uint32_t crc = i;
      for (int bit = 0; bit < 8; bit++) crc = (crc >> 1) ^ (0x82f63b78u & (~(crc & 1) + 1));
      values[i] = crc;
    }
    return values;
  }();
  return table;
}

std::string hmacSha256(const std::string& key, const std::string& message) {
  unsigned char out[EVP_MAX_MD_SIZE];
  size_t length = 0;
  KJ_REQUIRE(EVP_Q_mac(nullptr, "HMAC", nullptr, "SHA256", nullptr, key.data(), key.size(),
                 reinterpret_cast<const unsigned char*>(message.data()), message.size(), out, sizeof(out), &length) !=
          nullptr,
      "HMAC-SHA256 failed");
  return std::string(reinterpret_cast<char*>(out), length);
}

std::string sha256Bytes(std::string_view value) {
  unsigned char out[EVP_MAX_MD_SIZE];
  unsigned int length = 0;
  auto* context = EVP_MD_CTX_new();
  KJ_REQUIRE(context != nullptr, "no digest context");
  EVP_DigestInit_ex(context, EVP_sha256(), nullptr);
  EVP_DigestUpdate(context, value.data(), value.size());
  EVP_DigestFinal_ex(context, out, &length);
  EVP_MD_CTX_free(context);
  return std::string(reinterpret_cast<char*>(out), length);
}

std::string hexOf(std::string_view bytes) {
  static const char* digits = "0123456789abcdef";
  std::string out;
  out.reserve(bytes.size() * 2);
  for (unsigned char c : bytes) {
    out.push_back(digits[c >> 4]);
    out.push_back(digits[c & 0xf]);
  }
  return out;
}

constexpr const char* SEGMENT_MAGIC = "CWSEG001";
constexpr size_t SEGMENT_DIGEST = 32;

}  // namespace

uint32_t crc32c(std::string_view bytes) {
  const auto& table = crcTable();
  uint32_t crc = 0xffffffffu;
  for (unsigned char c : bytes) crc = table[(crc ^ c) & 0xff] ^ (crc >> 8);
  return crc ^ 0xffffffffu;
}

std::string nodeLogPrefix(const std::string& nodeId) { return "nodes/" + nodeId + "/wal/"; }
std::string nodeLogEpochPrefix(const std::string& nodeId, uint64_t nodeEpoch) {
  return nodeLogPrefix(nodeId) + wide(nodeEpoch) + "/";
}
std::string nodeSegmentKey(const std::string& nodeId, uint64_t nodeEpoch, uint64_t segment) {
  return nodeLogEpochPrefix(nodeId, nodeEpoch) + wide(segment);
}
std::string objectPrefix(const std::string& namespaceId, const std::string& objectId) {
  return "do/" + namespaceId + "/objects/" + objectId + "/";
}
std::string objectManifestPrefix(const std::string& namespaceId, const std::string& objectId) {
  return objectPrefix(namespaceId, objectId) + "manifest";
}
std::string objectManifestKey(const std::string& namespaceId, const std::string& objectId, uint64_t revision) {
  return revisionKey(objectManifestPrefix(namespaceId, objectId), revision);
}
std::string objectManifestHintKey(const std::string& namespaceId, const std::string& objectId) {
  return objectManifestPrefix(namespaceId, objectId) + "/current";
}
std::string objectSnapshotPrefix(const std::string& namespaceId, const std::string& objectId, uint64_t sequence) {
  return objectPrefix(namespaceId, objectId) + "snapshot/" + wide(sequence) + "/";
}
std::string objectSnapshotIndexKey(const std::string& namespaceId, const std::string& objectId, uint64_t sequence) {
  return objectSnapshotPrefix(namespaceId, objectId, sequence) + "index";
}
std::string objectSnapshotPartKey(const std::string& namespaceId, const std::string& objectId, uint64_t sequence,
    uint32_t facetId, uint64_t part) {
  return objectSnapshotPrefix(namespaceId, objectId, sequence) + wide(facetId) + "-" + wide(part);
}
std::string shardAlarmPrefix(const std::string& namespaceId, uint32_t shard) {
  return "do/" + namespaceId + "/shards/" + std::to_string(shard) + "/alarms/";
}
std::string shardAlarmKey(const std::string& namespaceId, uint32_t shard, const std::string& objectId,
    uint32_t facetId) {
  return shardAlarmPrefix(namespaceId, shard) + objectId + "/" + std::to_string(facetId);
}
std::string nodeDurableUsageKey(const std::string& nodeId) { return "nodes/" + nodeId + "/durable-usage"; }

std::string durableHostId(const std::string& uniqueKey, const std::string& name) {
  const auto key = sha256Bytes(uniqueKey);
  auto base = hmacSha256(key, name).substr(0, 16);
  auto mac = hmacSha256(key, base).substr(0, 16);
  return hexOf(base + mac);
}

std::string durableHostName(const std::string& namespaceId, const std::string& objectId, uint64_t dataEpoch) {
  return namespaceId + "/" + objectId + "/" + std::to_string(dataEpoch);
}

std::optional<DurableObjectRef> parseDurableHostName(const std::string& name) {
  auto first = name.find('/');
  if (first == std::string::npos) return std::nullopt;
  auto second = name.find('/', first + 1);
  if (second == std::string::npos) return std::nullopt;
  DurableObjectRef ref;
  ref.namespaceId = name.substr(0, first);
  ref.objectId = name.substr(first + 1, second - first - 1);
  const auto epoch = name.substr(second + 1);
  if (ref.namespaceId.empty() || ref.objectId.empty() || epoch.empty()) return std::nullopt;
  if (!safeName(ref.namespaceId) || !safeName(ref.objectId)) return std::nullopt;
  for (char c : epoch) {
    if (c < '0' || c > '9') return std::nullopt;
  }
  ref.dataEpoch = decimal(epoch);
  return ref;
}

std::string encodeLogRecord(const LogRecord& record) {
  std::string out;
  putU8(out, uint8_t(record.kind));
  putU64(out, record.nodeSeq);
  putU64(out, record.nodeEpoch);
  putU64(out, record.shardEpoch);
  putU32(out, record.shard);
  putText(out, record.objectName);
  putText(out, record.storageId);
  putU32(out, record.facetId);
  putU32(out, record.parentFacetId);
  putText(out, record.facetName);
  putU64(out, record.txn);
  putU64(out, record.dataEpoch);
  putU64(out, record.sentAtMonotonicNs);
  putU64(out, record.generation);
  putU32(out, record.salt1);
  putU32(out, record.salt2);
  putU32(out, record.pageSize);
  putU32(out, record.firstFrame);
  putU32(out, record.frames);
  putU32(out, record.commits);
  putU64(out, record.segment);
  putU8(out, record.header.empty() ? 0 : 1);
  if (!record.header.empty()) {
    KJ_REQUIRE(record.header.size() == 32, "a WAL header is 32 bytes");
    out += record.header;
  }
  putBytes(out, record.frameBytes);
  putU32(out, crc32c(out));
  return out;
}

std::optional<LogRecord> decodeLogRecord(std::string_view bytes, std::string* error) {
  const auto fail = [&](const char* reason) -> std::optional<LogRecord> {
    if (error != nullptr) *error = reason;
    return std::nullopt;
  };
  if (bytes.size() < 5) return fail("too short");
  const auto body = bytes.substr(0, bytes.size() - 4);
  Reader tail{bytes.substr(bytes.size() - 4)};
  if (tail.u32() != crc32c(body)) return fail("checksum");
  Reader in{body};
  LogRecord record;
  const auto kind = in.u8();
  if (kind < 1 || kind > 4) return fail("kind");
  record.kind = LogRecordKind(kind);
  record.nodeSeq = in.u64();
  record.nodeEpoch = in.u64();
  record.shardEpoch = in.u64();
  record.shard = in.u32();
  record.objectName = in.text();
  record.storageId = in.text();
  record.facetId = in.u32();
  record.parentFacetId = in.u32();
  record.facetName = in.text();
  record.txn = in.u64();
  record.dataEpoch = in.u64();
  record.sentAtMonotonicNs = in.u64();
  record.generation = in.u64();
  record.salt1 = in.u32();
  record.salt2 = in.u32();
  record.pageSize = in.u32();
  record.firstFrame = in.u32();
  record.frames = in.u32();
  record.commits = in.u32();
  record.segment = in.u64();
  if (in.u8() != 0) record.header = in.block(32);
  record.frameBytes = in.sized();
  if (!in.ok || in.at != body.size()) return fail("length");
  if (record.kind == LogRecordKind::FRAMES) {
    if (record.pageSize < 512 || record.pageSize > 65536 || !record.frames) return fail("frames");
    if (record.frameBytes.size() != uint64_t(record.frames) * (24 + record.pageSize)) return fail("frame bytes");
  }
  return record;
}

std::string frameRecords(const std::vector<std::string>& encoded) {
  std::string out;
  size_t total = 0;
  for (const auto& record : encoded) total += record.size() + 4;
  out.reserve(total);
  for (const auto& record : encoded) putBytes(out, record);
  return out;
}

std::vector<std::string> splitRecords(std::string_view bytes, std::string* error) {
  std::vector<std::string> out;
  size_t at = 0;
  while (at + 4 <= bytes.size()) {
    uint32_t length = 0;
    for (int i = 0; i < 4; i++) length |= uint32_t(uint8_t(bytes[at + i])) << (8 * i);
    at += 4;
    if (bytes.size() - at < length) {
      if (error != nullptr) *error = "a record is cut short";
      return out;
    }
    out.emplace_back(bytes.substr(at, length));
    at += length;
  }
  if (at != bytes.size() && error != nullptr) *error = "trailing bytes";
  return out;
}

namespace {
// Read the same framed prefix as splitRecords. The backing startup snapshot
// stays alive, so a second copy of each payload is unnecessary.
kj::Promise<std::vector<std::string_view>> startupFrames(std::string_view bytes, std::string& error,
    kj::Timer& timer, bool slice) {
  std::vector<std::string_view> out;
  Reader reader{bytes};
  StartupSlice budget(slice);
  while (bytes.size() - reader.at >= 4) {
    const size_t length = reader.u32();
    if (!reader.need(length)) {
      error = "a record is cut short";
      co_return out;
    }
    out.emplace_back(bytes.substr(reader.at, length));
    reader.at += length;
    if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
  }
  if (reader.at != bytes.size()) error = "trailing bytes";
  co_return out;
}
}  // namespace

std::string encodeSegment(const SegmentHeader& header, const std::vector<std::string>& records) {
  std::ostringstream index;
  index << "{\"nodeId\":" << quote(header.nodeId) << ",\"nodeEpoch\":\"" << header.nodeEpoch << "\",\"segment\":\""
        << header.segment << "\",\"firstSeq\":\"" << header.firstSeq << "\",\"lastSeq\":\"" << header.lastSeq
        << "\",\"records\":\"" << header.records << "\",\"bytes\":\"" << header.bytes << "\",\"sealedAtUnixMs\":"
        << header.sealedAtUnixMs << ",\"reason\":" << quote(header.reason) << ",\"objects\":[";
  bool first = true;
  for (const auto& entry : header.objects) {
    if (!first) index << ",";
    first = false;
    index << "{\"object\":" << quote(entry.objectName) << ",\"facet\":" << entry.facetId << ",\"records\":\""
          << entry.records << "\",\"firstTxn\":\"" << entry.firstTxn << "\",\"lastTxn\":\"" << entry.lastTxn << "\"}";
  }
  index << "]}";
  const auto text = index.str();
  std::string out = SEGMENT_MAGIC;
  putU32(out, uint32_t(text.size()));
  out += text;
  for (const auto& record : records) putBytes(out, record);
  out += sha256Bytes(out);
  return out;
}

std::optional<SegmentContents> decodeSegment(const std::string& bytes, std::string* error) {
  const auto fail = [&](const char* reason) -> std::optional<SegmentContents> {
    if (error != nullptr) *error = reason;
    return std::nullopt;
  };
  const size_t magic = std::strlen(SEGMENT_MAGIC);
  if (bytes.size() < magic + 4 + SEGMENT_DIGEST) return fail("too short");
  if (bytes.compare(0, magic, SEGMENT_MAGIC) != 0) return fail("magic");
  const auto body = bytes.substr(0, bytes.size() - SEGMENT_DIGEST);
  if (sha256Bytes(body) != bytes.substr(bytes.size() - SEGMENT_DIGEST)) return fail("digest");
  Reader in{std::string_view(body).substr(magic)};
  const auto headerText = in.sized();
  if (!in.ok) return fail("header length");
  SegmentContents contents;
  try {
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(headerText.data(), headerText.size()), value);
    auto reader = value.asReader();
    contents.header.nodeId = json::text(reader, "nodeId");
    contents.header.nodeEpoch = decimal(json::text(reader, "nodeEpoch"));
    contents.header.segment = decimal(json::text(reader, "segment"));
    contents.header.firstSeq = decimal(json::text(reader, "firstSeq"));
    contents.header.lastSeq = decimal(json::text(reader, "lastSeq"));
    contents.header.records = decimal(json::text(reader, "records"));
    contents.header.reason = json::text(reader, "reason");
  } catch (const kj::Exception&) {
    return fail("header");
  }
  while (in.at < in.bytes.size()) {
    const auto encoded = in.sized();
    if (!in.ok) return fail("a record is cut short");
    std::string reason;
    auto record = decodeLogRecord(encoded, &reason);
    if (!record) return fail("a record does not check out");
    contents.records.push_back(std::move(*record));
  }
  if (contents.records.size() != contents.header.records) return fail("record count");
  return contents;
}

std::string objectManifestJson(const ObjectManifest& manifest) {
  std::ostringstream out;
  out << "{\"namespaceId\":" << quote(manifest.namespaceId) << ",\"objectId\":" << quote(manifest.objectId)
      << ",\"revision\":\"" << manifest.revision << "\",\"shard\":" << manifest.shard << ",\"shardEpoch\":\""
      << manifest.shardEpoch << "\",\"writer\":{\"nodeId\":" << quote(manifest.writerNodeId) << ",\"nodeEpoch\":\""
      << manifest.writerNodeEpoch << "\",\"instance\":" << quote(manifest.instance) << ",\"nonce\":"
      << quote(manifest.nonce) << "},\"writtenAtUnixMs\":" << manifest.writtenAtUnixMs << ",\"databaseBytes\":\""
      << manifest.databaseBytes << "\",\"snapshot\":";
  if (manifest.hasSnapshot) {
    out << "{\"sequence\":\"" << manifest.snapshotSequence << "\",\"parts\":\"" << manifest.snapshotParts
        << "\",\"bytes\":\"" << manifest.snapshotBytes << "\",\"atUnixMs\":" << manifest.snapshotAtUnixMs
        << ",\"nodeId\":" << quote(manifest.snapshotNodeId) << ",\"nodeEpoch\":\"" << manifest.snapshotNodeEpoch
        << "\",\"shardEpoch\":\"" << manifest.snapshotShardEpoch << "\",\"facets\":[";
    bool first = true;
    for (const auto& facet : manifest.facets) {
      if (!first) out << ",";
      first = false;
      out << "{\"facet\":" << facet.facetId << ",\"parent\":" << facet.parentFacetId << ",\"name\":"
          << quote(facet.name) << ",\"bytes\":\"" << facet.bytes << "\",\"parts\":\"" << facet.parts
          << "\",\"upToSeq\":\"" << facet.upToSeq << "\",\"sha256\":" << quote(facet.sha256) << "}";
    }
    out << "]}";
  } else {
    out << "null";
  }
  out << ",\"segments\":[";
  bool first = true;
  for (const auto& segment : manifest.segments) {
    if (!first) out << ",";
    first = false;
    out << "{\"nodeId\":" << quote(segment.nodeId) << ",\"nodeEpoch\":\"" << segment.nodeEpoch << "\",\"segment\":\""
        << segment.segment << "\",\"firstSeq\":\"" << segment.firstSeq << "\",\"lastSeq\":\"" << segment.lastSeq
        << "\",\"shardEpoch\":\"" << segment.shardEpoch << "\"}";
  }
  out << "],\"isolated\":[";
  first = true;
  for (const auto& [shardEpoch, sequence] : manifest.isolated) {
    if (!first) out << ",";
    first = false;
    out << "{\"shardEpoch\":\"" << shardEpoch << "\",\"nodeSeq\":\"" << sequence << "\"}";
  }
  out << "]}";
  return out.str();
}

std::optional<ObjectManifest> parseObjectManifest(const std::string& bytes) {
  try {
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), value);
    auto reader = value.asReader();
    ObjectManifest manifest;
    manifest.namespaceId = json::text(reader, "namespaceId");
    manifest.objectId = json::text(reader, "objectId");
    manifest.revision = decimal(json::text(reader, "revision"));
    if (auto shard = json::field(reader, "shard"); shard.isNumber()) manifest.shard = uint32_t(shard.getNumber());
    manifest.shardEpoch = decimal(json::text(reader, "shardEpoch"));
    if (auto writer = json::field(reader, "writer"); writer.isObject()) {
      manifest.writerNodeId = json::text(writer, "nodeId");
      manifest.writerNodeEpoch = decimal(json::text(writer, "nodeEpoch"));
      manifest.instance = json::text(writer, "instance");
      manifest.nonce = json::text(writer, "nonce");
    }
    if (auto at = json::field(reader, "writtenAtUnixMs"); at.isNumber()) manifest.writtenAtUnixMs = uint64_t(at.getNumber());
    manifest.databaseBytes = json::integer(reader, "databaseBytes");
    if (auto snapshot = json::field(reader, "snapshot"); snapshot.isObject()) {
      manifest.hasSnapshot = true;
      manifest.snapshotSequence = decimal(json::text(snapshot, "sequence"));
      manifest.snapshotParts = decimal(json::text(snapshot, "parts"));
      manifest.snapshotBytes = decimal(json::text(snapshot, "bytes"));
      if (auto at = json::field(snapshot, "atUnixMs"); at.isNumber()) manifest.snapshotAtUnixMs = uint64_t(at.getNumber());
      manifest.snapshotNodeId = json::text(snapshot, "nodeId");
      manifest.snapshotNodeEpoch = decimal(json::text(snapshot, "nodeEpoch"));
      manifest.snapshotShardEpoch = decimal(json::text(snapshot, "shardEpoch"));
      if (auto facets = json::field(snapshot, "facets"); facets.isArray()) {
        for (auto facet : facets.getArray()) {
          ManifestFacet entry;
          auto number = json::field(facet, "facet");
          entry.facetId = number.isNumber() ? uint32_t(number.getNumber()) : 0;
          auto parent = json::field(facet, "parent");
          entry.parentFacetId = parent.isNumber() ? uint32_t(parent.getNumber()) : 0;
          entry.name = json::text(facet, "name");
          entry.bytes = decimal(json::text(facet, "bytes"));
          entry.parts = decimal(json::text(facet, "parts"));
          entry.upToSeq = decimal(json::text(facet, "upToSeq"));
          entry.sha256 = json::text(facet, "sha256");
          manifest.facets.push_back(std::move(entry));
        }
      }
    }
    if (auto segments = json::field(reader, "segments"); segments.isArray()) {
      for (auto segment : segments.getArray()) {
        ManifestSegment entry;
        entry.nodeId = json::text(segment, "nodeId");
        entry.nodeEpoch = decimal(json::text(segment, "nodeEpoch"));
        entry.segment = decimal(json::text(segment, "segment"));
        entry.firstSeq = decimal(json::text(segment, "firstSeq"));
        entry.lastSeq = decimal(json::text(segment, "lastSeq"));
        entry.shardEpoch = decimal(json::text(segment, "shardEpoch"));
        manifest.segments.push_back(std::move(entry));
      }
    }
    if (auto isolated = json::field(reader, "isolated"); isolated.isArray()) {
      for (auto entry : isolated.getArray()) {
        manifest.isolated.emplace_back(decimal(json::text(entry, "shardEpoch")), decimal(json::text(entry, "nodeSeq")));
      }
    }
    if (manifest.namespaceId.empty() || manifest.objectId.empty() || !manifest.revision) return std::nullopt;
    return manifest;
  } catch (const kj::Exception&) {
    return std::nullopt;
  }
}

// ---- Replaying records into a database file ----

DatabaseReplay::DatabaseReplay(std::filesystem::path pathParam, bool fromEmpty, bool validateOnlyParam)
    : path(std::move(pathParam)), validateOnly(validateOnlyParam) {
  if (validateOnly) return;
  std::filesystem::create_directories(path.parent_path());
  if (fromEmpty) std::filesystem::remove(path);
  fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0600);
  KJ_REQUIRE(fd >= 0, "cannot open a Durable Object database to rebuild", path.string().c_str());
}

DatabaseReplay::~DatabaseReplay() {
  if (fd >= 0) ::close(fd);
}

bool DatabaseReplay::apply(const LogRecord& record, std::string* error) {
  const auto refuse = [&](const char* reason) {
    if (error != nullptr) *error = reason;
    return false;
  };
  if (record.kind == LogRecordKind::RESET || record.kind == LogRecordKind::DELETED) {
    KJ_REQUIRE(validateOnly || ::ftruncate(fd, 0) == 0, "cannot empty a Durable Object database", path.string().c_str());
    haveHeader = false;
    walHeader.clear();
    ++records;
    return true;
  }
  if (record.kind != LogRecordKind::FRAMES) return true;
  if (!record.header.empty()) {
    if (record.header.size() != 32) return refuse("WAL header length");
    const auto* header = reinterpret_cast<const unsigned char*>(record.header.data());
    uint32_t m = beU32(header);
    if ((m & 0xFFFFFFFE) != 0x377f0682) return refuse("not a WAL header");
    if (beU32(header + 4) != 3007000) return refuse("WAL header version");
    const uint32_t size = beU32(header + 8);
    if (size < 512 || size > 65536 || (size & (size - 1)) != 0) return refuse("WAL header page size");
    uint32_t s1 = 0, s2 = 0;
    walChecksum(m & 1, header, 24, s1, s2);
    if (s1 != beU32(header + 24) || s2 != beU32(header + 28)) return refuse("WAL header checksum");
    // Reopening an object resends the existing WAL header with its next frames. The shipper's generation counter
    // is local to that instance; the complete validated header identifies the WAL generation across reopens.
    // A repeated header must preserve both the frame position and the checksum chain already applied.
    if (!haveHeader || walHeader != record.header) {
      magic = m;
      pageSize = size;
      salt1 = beU32(header + 16);
      salt2 = beU32(header + 20);
      cksum1 = s1;
      cksum2 = s2;
      nextFrame = 1;
      walHeader = record.header;
      haveHeader = true;
    }
  }
  if (!haveHeader) return refuse("frames without their generation's header");
  if (record.pageSize != pageSize || record.salt1 != salt1 || record.salt2 != salt2)
    return refuse("frames of another generation");
  if (record.firstFrame != nextFrame) return refuse("a gap in the frames");
  const size_t frameSize = 24 + size_t(pageSize);
  const auto* frames = reinterpret_cast<const unsigned char*>(record.frameBytes.data());
  uint32_t s1 = cksum1, s2 = cksum2;
  for (uint32_t i = 0; i < record.frames; i++) {
    const unsigned char* frame = frames + size_t(i) * frameSize;
    const uint32_t page = beU32(frame);
    if (!page) return refuse("a frame without a page number");
    if (beU32(frame + 8) != salt1 || beU32(frame + 12) != salt2) return refuse("a frame of another generation");
    walChecksum(magic & 1, frame, 8, s1, s2);
    walChecksum(magic & 1, frame + 24, pageSize, s1, s2);
    if (s1 != beU32(frame + 16) || s2 != beU32(frame + 20)) return refuse("a frame's checksum");
    const off_t offset = off_t(page - 1) * off_t(pageSize);
    size_t done = 0;
    while (!validateOnly && done < pageSize) {
      auto written = ::pwrite(fd, frame + 24 + done, pageSize - done, offset + off_t(done));
      if (written < 0 && errno == EINTR) continue;
      KJ_REQUIRE(written > 0, "cannot write a page of a Durable Object database", path.string().c_str());
      done += size_t(written);
    }
    const uint32_t committed = beU32(frame + 4);
    if (committed != 0 && !validateOnly) {
      // A commit frame says how many pages the database has: everything beyond it belongs to a transaction that was
      // not committed.
      KJ_REQUIRE(::ftruncate(fd, off_t(committed) * off_t(pageSize)) == 0,
          "cannot truncate a Durable Object database", path.string().c_str());
    }
    ++framesApplied;
  }
  cksum1 = s1;
  cksum2 = s2;
  nextFrame = record.firstFrame + record.frames;
  ++records;
  return true;
}

kj::Promise<bool> DatabaseReplay::finishAsync() {
  if (fd < 0) co_return true;
  const bool synced = co_await synchronizeRecoveryFile(fd);
  ::close(fd); fd = -1;
  co_return synced;
}

// ---- The replicator ----

namespace {
// Messages from a durable process (patches/workerd 0013, coworkerd-wal-shipper.h).
constexpr uint8_t FROM_HELLO = 1, FROM_FRAMES = 2, FROM_OPEN = 3, FROM_CHECKPOINT = 4, FROM_RESET = 5,
                  FROM_DELETED = 6, FROM_CLOSED = 7, FROM_GAVE_UP = 8, FROM_ALARM = 9, FROM_RESIDENCY = 10,
                  FROM_ACTOR_RESET = 11;
// Messages to one.
constexpr uint8_t TO_ACK = 1, TO_SEALED = 2, TO_BREAK = 3, TO_CHECKPOINT = 4, TO_READY = 5, TO_CLOSE = 6,
                  TO_OPENED = 7, TO_CLOSE_FOR_CODE_UPDATE = 8;
constexpr uint32_t THREADED_WAL_REVISION = 1;
// An answer's status.
constexpr uint8_t ACK_OK = 0, ACK_UNAVAILABLE = 1, ACK_TOO_LARGE = 3, ACK_QUOTA = 4;

// Cloudflare's alarm retries: the first two seconds later, doubling, a quarter of jitter; six counted tries, and
// past them the delay doubles to about seventeen minutes (src/workerd/server/alarm-scheduler.h).
constexpr uint32_t ALARM_RETRY_START_SECONDS = 2, ALARM_RETRY_MAX_TRIES = 6, ALARM_RETRY_BACKOFF_MAX = 9;
// The facet tree index workerd keeps beside an object's databases (src/workerd/server/facet-tree-index.h): an
// 8 byte magic number, then one entry per facet -- its parent (2 bytes), the length of its name (2) and the name.
constexpr uint64_t FACET_INDEX_MAGIC = 0xc4cdce5bc5b0ef57ull;
// How many of this node's unsealed records one answer to another node's rebuild carries.
constexpr size_t TAIL_ANSWER_BYTES = 32u << 20;
// The decoded segments a node keeps for its rebuilds, and how long one stays without a reader.
constexpr uint64_t SEGMENT_CACHE_BYTES = 384ull << 20;
constexpr double SEGMENT_CACHE_IDLE_MS = 30000;
constexpr uint32_t SEGMENT_CACHE_SWEEP_MS = 5000;
// The largest record this node takes from another's replicator: a durable process cuts its messages at 8 MiB of
// frames (workerd's COWORKERD_DO_RECORD_BYTES), so anything far above that is not one of ours.
constexpr size_t RECORD_BYTES_LIMIT = 256u << 20;
// The production peer clients/servers use default ReaderOptions. Reserve space
// for the RPC/result structs, the record length and word alignment.
constexpr uint64_t TAIL_RPC_HEADROOM_BYTES = 4096;
// common/object-store.c++ reads at most 64 MiB. Count the whole envelope,
// including each record's length, the JSON index and the final digest.
constexpr uint64_t STORE_SEGMENT_BYTES = 64u << 20;
uint64_t segmentEnvelopeBudget(const std::string& nodeId, const std::string& reason) {
  return 1024 + quote(nodeId).size() + quote(reason).size();
}
uint64_t segmentEntryBudget(const LogRecord& record) {
  return 256 + quote(record.objectName).size();
}
uint64_t tailRecordBytes() {
  const auto rpcBytes = capnp::ReaderOptions().traversalLimitInWords * sizeof(capnp::word);
  return std::min<uint64_t>(rpcBytes, RECORD_BYTES_LIMIT) - TAIL_RPC_HEADROOM_BYTES;
}
// One message of a log stream carries at most this much: a larger record goes in pieces (Cap'n Proto reads at most
// 64 MiB in one message).
constexpr size_t APPEND_CHUNK_BYTES = 4u << 20;
// How much of one peer's stream may be in flight: without this the RPC system writes everything queued in one call,
// which an operating system refuses past a few tens of megabytes.
constexpr uint64_t APPEND_WINDOW_BYTES = 8u << 20;
// A manifest is written again at most this many times when another writer got its revision first.
constexpr uint32_t MANIFEST_ATTEMPTS = 8;
// The confirmation latencies kept for the state endpoint's percentiles.
constexpr size_t CONFIRM_SAMPLES = 4096;
// How often a stream to a peer that cannot be reached is tried again.
constexpr uint32_t PEER_RETRY_MS = 1000;

// The object a record belongs to, as this node keeps it: the name its Host was made with.
std::string hostNameOf(const LogRecord& record) {
  return record.objectName + "/" + std::to_string(record.dataEpoch);
}

// A non-negative integer field written as a JSON number (the hints and the position file write small numbers that
// way; the large ones are strings of decimal digits, read with decimal(json::text(...))).
uint64_t numberField(const json::Json::Reader& object, const char* name) {
  auto value = json::field(object, name);
  if (!value.isNumber()) return 0;
  const auto number = value.getNumber();
  return number > 0 ? uint64_t(number) : 0;
}

// A value below `bound`, for the jitter of an alarm retry.
uint64_t randomBelow(uint64_t bound) {
  if (bound == 0) return 0;
  static std::random_device device;
  static std::mt19937_64 engine(device());
  return engine() % bound;
}

std::string randomHex(size_t bytes) {
  static std::random_device device;
  static std::mt19937_64 engine(device());
  std::string out;
  out.reserve(bytes * 2);
  static const char* digits = "0123456789abcdef";
  for (size_t i = 0; i < bytes; i++) {
    const auto value = uint8_t(engine() & 0xff);
    out.push_back(digits[value >> 4]);
    out.push_back(digits[value & 0xf]);
  }
  return out;
}

double percentile(std::vector<double> values, double fraction) {
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  auto at = size_t(fraction * double(values.size() - 1) + 0.5);
  return values[std::min(at, values.size() - 1)];
}
}  // namespace

// Field by field what encodeLogRecord writes: 116 bytes of fixed fields, length prefixes and checksum, then the texts,
// the WAL header when there is one and the frames.
uint64_t encodedLogRecordSize(const LogRecord& record) {
  return 116 + record.objectName.size() + record.storageId.size() + record.facetName.size() +
      (record.header.empty() ? 0 : 32) + record.frameBytes.size();
}

EarlierSegmentBuild buildEarlierSegment(SegmentHeader header, EarlierSegmentInput input) {
  EarlierSegmentBuild out;
  std::vector<std::string> encoded;
  encoded.reserve(input.records.size());
  std::map<std::pair<std::string, uint32_t>, SegmentIndexEntry> index;
  header.records = 0;
  header.bytes = 0;
  header.firstSeq = 0;
  header.lastSeq = 0;
  for (size_t i = 0; i < input.records.size(); ++i) {
    const auto& record = input.records[i];
    const auto sequence = input.sequences[i];
    auto bytes = encodeLogRecord(record);
    KJ_REQUIRE(bytes.size() == input.sizes[i], "a record's encoded size changed between selection and encoding");
    header.bytes += bytes.size();
    encoded.push_back(std::move(bytes));
    ++header.records;
    if (!header.firstSeq) header.firstSeq = sequence;
    header.lastSeq = sequence;
    auto& entry = index[{record.objectName, record.facetId}];
    entry.objectName = record.objectName;
    entry.facetId = record.facetId;
    if (!entry.records) entry.firstTxn = record.txn;
    entry.lastTxn = record.txn;
    ++entry.records;
    const auto host = hostNameOf(record);
    out.touched.insert(host);
    auto& source = out.sourceEpochs[host];
    source = std::max(source, record.shardEpoch);
  }
  for (auto& [key, entry] : index) header.objects.push_back(entry);
  out.body = encodeSegment(header, encoded);
  out.sha256 = objectSha256(out.body);
  out.header = std::move(header);
  return out;
}

DurableLog::DurableLog(Settings settingsParam, const ObjectStoreConfig& storeConfig, kj::Network& networkParam,
    kj::Timer& timerParam, kj::LowLevelAsyncIoProvider& lowLevelParam, const MembershipView& membershipParam,
    Hooks hooksParam)
    : settings(std::move(settingsParam)),
      network(networkParam),
      timer(timerParam),
      lowLevel(lowLevelParam),
      membership(membershipParam),
      hooks(std::move(hooksParam)) {
  settings.config.validate();
  if (const auto* v = std::getenv("COWORKERD_ALARM_THREAD_CPU_FOR_TEST")) measureAlarmCpu = std::string_view(v) == "1";
  if (const auto* v = std::getenv("COWORKERD_SLOW_FLUSH_FOR_TEST"); v != nullptr && *v != '\0') slowFlushForTest = v;
  if (const auto* v = std::getenv("COWORKERD_HOLD_PEER_ACK_FOR_TEST"); v != nullptr) peerAckGateForTest = v;
  if (const auto* v = std::getenv("COWORKERD_CUT_DURABLE_CHANNEL_FOR_TEST"); v != nullptr) cutChannelForTest = v;
  if (settings.objectStore && !storeConfig.endpoint.empty()) {
    store.emplace(storeConfig, network, timer);
    workerStoreConfig = storeConfig;
    segmentCache = std::make_unique<SegmentDecodeCache>([this](std::string key) { return store->get(std::move(key)); },
        SEGMENT_CACHE_BYTES, SEGMENT_CACHE_IDLE_MS, [this](const std::string& key, const DecodedSegment& segment) {
          log("segment-decoded", "\"key\":" + quote(key) + ",\"records\":" + std::to_string(segment.records) +
              ",\"objects\":" + std::to_string(segment.byObject.size()) + ",\"encodedBytes\":" +
              std::to_string(segment.encodedBytes) + ",\"heldBytes\":" + std::to_string(segment.bytes) +
              ",\"downloadMs\":" + std::to_string(segment.downloadMs) + ",\"decodeMs\":" +
              std::to_string(segment.decodeMs) + ",\"groupMs\":" + std::to_string(segment.groupMs));
        },
        // The download runs on the decoder's worker thread, with an event loop and a connection of its own: a
        // segment can be close to 64 MiB, and the event loop's reader copies every chunk of it.
        [storeConfig](const std::string& key) {
          auto io = kj::setupAsyncIo();
          ObjectStore threadStore(storeConfig, io.provider->getNetwork(), io.provider->getTimer());
          return threadStore.get(key).wait(io.waitScope);
        });
    alarmPublication = std::make_unique<AlarmPublication>(*store, timer, this->settings.nodeId,
        [this](const std::string& ns, uint32_t shard, uint64_t epoch, uint64_t memberEpoch) {
          if (stopped || memberEpoch != nodeEpoch || membership.selfExpired || membership.identityTaken) return false;
          auto fence = ownerFenceThrough.find({ns, shard});
          if (epoch && fence != ownerFenceThrough.end() && fence->second >= epoch) return false;
          if (epoch && alarmHeld && alarmHeld(ns, shard) != epoch) return false;
          const auto known = hooks.knownShardEpoch ? hooks.knownShardEpoch(ns, shard) : 0;
          return !epoch || !known || known == epoch;
        },
        [this](const std::string& ns, const std::string& object, uint32_t shard, uint64_t epoch,
            const std::string& tenant, uint64_t at) {
          const auto key = ns + "/" + object + "/0";
          auto found = alarms.find(key);
          if (found != alarms.end() && !found->second.fromHint) return;
          if (found == alarms.end()) ++alarmsAdopted;
          else unindexAlarm(key, found->second);
          auto& state = alarms[key];state.namespaceId = ns;state.objectId = object;state.tenant = tenant;
          state.shard = shard;state.shardEpoch = epoch;state.atUnixMs = at;state.fromHint = true;
          state.dueObserved = false;state.objectName.clear();state.dataEpoch = 0;
          indexAlarm(key, state);
        }, [this](const std::string& event, const std::string& fields) { log(event, fields); });
    const auto pendingDirectory = this->settings.stackDirectory / "alarm-pending";
    alarmPublication->setPendingDirectory(pendingDirectory);
    alarmPublication->setReadBudget(alarmReads);
    alarmDiscovery = std::make_unique<AlarmDiscovery>(AlarmDiscovery::Settings{this->settings.nodeId,
        pendingDirectory, this->settings.config.alarmDiscoveryIntervalMs,
        this->settings.config.alarmTakeoverConcurrency},
        *store, timer, membership, alarmReads,
        [this]() { return startupReady && !stopped && bool(alarmTake) && nodeEpoch && membership.epoch == std::to_string(nodeEpoch) && membership.durableProcesses; },
        [this](std::string ns, uint32_t shard, uint32_t waited) { return alarmTake(std::move(ns), shard, waited); },
        [this](const std::string& ns) { return alarmTenant ? alarmTenant(ns) : ns; },
        [this](const std::string& ns, uint32_t shard, const std::string& exclude) { return alarmRank ? alarmRank(ns, shard, exclude) : 0; },
        [this](const std::string& ns, uint32_t shard) { return alarmValid ? alarmValid(ns, shard) : !hooks.namespaceActive || hooks.namespaceActive(ns); },
        [this](const std::string& ns, uint32_t shard, uint64_t epoch) {
          alarmPublication->ensureTake(ns, shard, epoch, alarmTenant ? alarmTenant(ns) : ns);
        }, [this](const std::string& ns, uint32_t shard) { return alarmPublication->forgetPending(ns, shard); },
        [this](const std::string& ns, uint32_t shard) { return alarmHeld ? alarmHeld(ns, shard) : uint64_t(0); },
        [this](const std::string& ns, uint32_t shard, uint64_t epoch) { return alarmPublication->publicationState(ns, shard, epoch); },
        [this](const std::string& ns, uint32_t shard, uint64_t epoch, bool adoptionOnly) { return alarmPublication->waitPublished(ns, shard, epoch, adoptionOnly); },
        [this](const std::string& ns, uint32_t shard, uint64_t epoch) { return alarmPublication->hintsReady(ns, shard, epoch); },
        [this](const std::string& ns, uint32_t shard, uint64_t epoch, const std::string& marker) { return alarmPublication->claimRetirement(ns, shard, epoch, marker); });
  }
}

DurableLog::~DurableLog() noexcept(false) {
  stopped = true;
  segmentCache.reset();
  alarmDiscovery.reset();
  alarmPublication.reset();
  if (logFd >= 0) ::close(logFd);
  if (recoveryFd >= 0) ::close(recoveryFd);
}

void DurableLog::taskFailed(kj::Exception&& error) {
  log("task-failed", "\"error\":" + quote(std::string(error.getDescription().cStr())));
}

void DurableLog::log(const std::string& event, const std::string& fields) const {
  std::cout << "[do-log] {\"event\":" << quote(event) << ",\"nodeId\":" << quote(settings.nodeId) << ",\"nodeEpoch\":\""
            << nodeEpoch << "\"" << (fields.empty() ? "" : "," + fields) << "}" << std::endl;
}

kj::Promise<std::vector<std::filesystem::path>> DurableLog::objectDirectories(std::error_code& code, bool slice) {
  StartupSlice budget(slice);
  std::vector<std::filesystem::path> directories;
  for (std::filesystem::directory_iterator it(settings.stackDirectory / "do", code), end;
      !code && it != end; it.increment(code)) {
    if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
    const auto status = it->symlink_status(code);
    if (code) break;
    if (std::filesystem::is_directory(status) &&
        isObjectStorageDirectory(it->path().filename().string()))
      directories.push_back(it->path());
  }
  std::sort(directories.begin(), directories.end());
  co_return directories;
}

void DurableLog::locate(Object& object, const std::string& namespaceId, const std::string& objectId,
    uint64_t dataEpoch) {
  if (!object.storageId.empty()) {
    KJ_REQUIRE(object.namespaceId == namespaceId && object.objectId == objectId && object.dataEpoch == dataEpoch,
        "an object's storage identity cannot change");
    return;
  }
  static_cast<RootStorageRef&>(object) = makeRootStorageRef(namespaceId, objectId, dataEpoch);
}
std::filesystem::path DurableLog::logDirectory() const {
  return settings.stackDirectory / "do" / "log";
}
std::filesystem::path DurableLog::databasePath(const RootStorageRef& ref, uint32_t facetId,
    const char* suffix, const std::string& staging) const {
  return ref.databasePath(settings.stackDirectory, facetId, suffix, staging);
}

void DurableLog::setNodeEpoch(uint64_t epoch) {
  if (epoch == nodeEpoch) return;
  // Sequence numbers will be reused. Fail pending requests under their old identity, but keep confirmed bytes
  // for the existing earlier-epoch uploader until its segment and manifests are durable.
  std::set<std::string> pending;
  for (const auto& [sequence, flight] : inFlight) pending.insert(flight.shipperKey);
  for (const auto& key : pending) {
    isolate(key, 0, "the node's log epoch changed before confirmation");
    auto* object = objectOf(key.substr(0, key.rfind('/')));
    if (object != nullptr) markRepair(*object, "the node's log epoch changed before confirmation");
  }
  for (const auto& [sequence, encoded] : records) {
    auto record = decodeLogRecord(encoded);
    KJ_REQUIRE(record.has_value(), "a local record cannot be decoded");
    if (isolated.contains(sequence)) earlierExcluded[nodeEpoch][sequence] = std::move(*record);
    else {
      auto& pending = earlier[nodeEpoch];
      if (auto prior = pending.find(sequence); prior != pending.end())
        unindexEarlierRecord({nodeEpoch, sequence}, prior->second);
      indexEarlierRecord({nodeEpoch, sequence}, *record);
      pending[sequence] = std::move(*record);
    }
  }
  if (nodeEpoch) {
    auto& info = predecessorLogs[nodeEpoch];
    info.complete = !logAppendFailed && logFd >= 0;
    info.sealedTo = sealedSeq;
    info.maxSeq = nextSeq;
    info.unpublishedShards.clear();
    info.unpublishedObjects.clear();
    info.unpublishedTracked.clear();
    for (const auto* held : {&earlier[nodeEpoch], &earlierExcluded[nodeEpoch]}) {
      for (const auto& [sequence, record] : *held) {
        const auto shard = std::make_pair(record.objectName.substr(0, record.objectName.find('/')), record.shard);
        noteEarlierUnpublished(info, record);info.shards.insert(shard);info.objects.insert(hostNameOf(record));
        info.objectEpochs[hostNameOf(record)] = std::max(info.objectEpochs[hostNameOf(record)], record.shardEpoch);
        info.startupUnsealed.emplace(record.objectName, record.shardEpoch);
      }
    }
    if (earlier[nodeEpoch].empty()) earlier.erase(nodeEpoch);
    if (earlierExcluded[nodeEpoch].empty()) earlierExcluded.erase(nodeEpoch);
  }
  for (auto& [id, peer] : peers) retirePeer(peer);
  inFlight.clear();
  confirmed.clear();
  if (logFd >= 0) {
    ::close(logFd);
    logFd = -1;
  }
  if (recoveryFd >= 0) { ::close(recoveryFd); recoveryFd = -1; }
  nodeEpoch = epoch;
  if (alarmPublication) alarmPublication->setNodeEpoch(epoch);
  recoverySerial = 0;
  carriedRecoveryObjects.clear();
  logAppendFailed = false;
  // Finish any in-flight store request under its original identity, but do not
  // let an obsolete upload's retry delay block publication at the new epoch.
  uploadRetryCanceler.cancel("the node log epoch changed");
  nextSeq = 0;
  sealedSeq = 0;
  segmentNumber = 0;
  records.clear();
  parsed.clear();
  unsealedByObject.clear();
  recordBytes = 0;
  isolated.clear();
  if (!enabled() || !epoch) return;
  std::filesystem::create_directories(logDirectory());
  logPath = logDirectory() / (wide(epoch) + ".log");
  logFd = ::open(logPath.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0600);
  if (logFd < 0) {
    logAppendFailed = true;
    log("log-file-failed", "\"path\":" + quote(logPath.string()) + ",\"errno\":" + std::to_string(errno));
  }
  if (logFd >= 0 && (!syncDirectory(logDirectory()) || !initializeRecovery()))
    failRecovery("the log epoch files and directory could not be made durable");
  if (peerCount() == 0) {
    log("no-peers", "\"warning\":\"this node has no peer to confirm its Durable Object writes: every write waits for "
                    "this machine's own disk, which costs tens of milliseconds\"");
  }
}

std::string DurableLog::bootId() const {
  // Linux names the boot of the running kernel; without a stable boot id,
  // cached databases are never reused. Tests can name their own boot.
  if (const char* given = std::getenv("COWORKERD_BOOT_ID_FOR_TEST"); given != nullptr && *given != '\0') return given;
  try {
    auto value = readFile("/proc/sys/kernel/random/boot_id");
    while (!value.empty() && (value.back() == '\n' || value.back() == '\r')) value.pop_back();
    return value;
  } catch (const kj::Exception&) {
    return {};
  }
}

bool DurableLog::readCacheMarker(std::string& reason) {
  sameBootCache = false;
  previousCacheBootId.clear();
  const auto path = settings.stackDirectory / "do" / "cache.json";
  std::string bytes;
  try {
    bytes = readFile(path);
  } catch (const kj::Exception&) {
    reason = "no marker: this node has not kept Durable Object files here";
    return false;
  }
  try {
    capnp::MallocMessageBuilder message;
    auto value = message.initRoot<capnp::JsonValue>();
    capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), value);
    auto reader = value.asReader();
    const auto marker = json::text(reader, "bootId");
    previousCacheBootId = marker;
    const auto current = bootId();
    sameBootCache = !current.empty() && current == marker;
    if (!current.empty() && current == marker) {
      reason = "the same boot of this machine: only the daemon or a durable process restarted";
      return true;
    }
    reason = current.empty() ? "no clean exit and this machine's boot cannot be read"
                             : "this machine restarted without a clean exit";
    return false;
  } catch (const kj::Exception&) {
    reason = "the marker does not read as one";
    return false;
  }
}

bool DurableLog::writeCacheMarker(bool clean) {
  const auto directory = settings.stackDirectory / "do";
  std::error_code code;
  std::filesystem::create_directories(directory, code);
  const auto path = directory / "cache.json";
  if (code) return false;
  std::string bytes = "{\"bootId\":" + quote(cacheMarkerBootOverride.value_or(bootId())) + ",\"clean\":" + (clean ? "true" : "false") +
      ",\"nodeId\":" + quote(settings.nodeId) + ",\"atUnixMs\":" + std::to_string(realClockUnixMs()) + "}\n";
  return atomicLocalFile(path, bytes, true);
}

void DurableLog::failRecovery(const std::string& reason) {
  recoveryWriteFailed = true;
  logAppendFailed = true;
  ++recoveryWriteFailures;
  if (logFd >= 0) { ::close(logFd); logFd = -1; }
  if (recoveryFd >= 0) { ::close(recoveryFd); recoveryFd = -1; }
  log("recovery-write-failed", "\"reason\":" + quote(reason) + ",\"errno\":" + std::to_string(errno) +
      ",\"action\":\"all Durable Objects remain unavailable until restart\"");
}

DurableLog::RecoveryEvent DurableLog::recoveryEvent(RecoveryKind kind, const Object& object,
    RecoveryGeneration generation) const {
  return {kind, object.objectName, generation, object.shardEpoch, 0, object.dataEpoch, object.shard};
}

void DurableLog::applyRecovery(const RecoveryEvent& event) {
  if (event.kind == RecoveryKind::HEADER) return;
  auto& state = recoveryStates[event.objectName];
  switch (event.kind) {
    case RecoveryKind::REPAIR_BEGIN: state.repair = event.generation; state.repairPending = true; break;
    case RecoveryKind::ISOLATED:
      state.exclusions[{event.shardEpoch, event.nodeSeq}] = event.generation; break;
    case RecoveryKind::REPAIR_DONE:
      if (state.repair == event.generation) {
        state.repairPending = false; state.repaired = event.generation;
      }
      break;
    case RecoveryKind::REBUILD_BEGIN: state.rebuild = event.generation; state.rebuildPending = true; break;
    case RecoveryKind::REBUILD_DONE:
      if (state.rebuild == event.generation) state.rebuildPending = false;
      break;
    case RecoveryKind::PUBLISHED: state.published = true; break;
    case RecoveryKind::HEAL_BEGIN: state.healing = true; break;
    case RecoveryKind::HEAL_DONE: state.healing = false; state.published = true; break;
    case RecoveryKind::HEADER: break;
  }
}

bool DurableLog::appendRecovery(const std::vector<RecoveryEvent>& events, bool sync) {
  if (recoveryWriteFailed || recoveryFd < 0) {
    if (!recoveryWriteFailed) failRecovery("the recovery sidecar is not open");
    return false;
  }
  std::string bytes;
  for (const auto& event : events) {
    std::string body = "DOR1";
    putU8(body, uint8_t(event.kind));putText(body, event.objectName);
    putU64(body, event.generation.first);putU64(body, event.generation.second);
    putU64(body, event.shardEpoch);putU64(body, event.nodeSeq);putU64(body, event.dataEpoch);putU32(body, event.shard);
    putU32(body, crc32c(body));putU32(bytes, uint32_t(body.size()));bytes += body;
  }
  ssize_t written;
  do { written = ::write(recoveryFd, bytes.data(), bytes.size()); } while (written < 0 && errno == EINTR);
  if (written != ssize_t(bytes.size()) || (sync && !syncFile(recoveryFd))) {
    failRecovery("a recovery event could not be appended and synchronized");
    return false;
  }
  for (const auto& event : events) applyRecovery(event);
  return true;
}

bool DurableLog::initializeRecovery() {
  if (recoveryWriteFailed) return false;
  const auto path = logDirectory() / (wide(nodeEpoch) + ".recovery");
  recoveryFd = ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
  if (recoveryFd < 0) return false;
  struct stat status;
  if (::fstat(recoveryFd, &status) != 0) return false;
  if (!status.st_size && !appendRecovery({RecoveryEvent{RecoveryKind::HEADER, settings.nodeId, {nodeEpoch, 1}}}, true))
    return false;
  if (!syncFile(recoveryFd) || !syncDirectory(logDirectory())) return false;
  recoveryEpochs[nodeEpoch] = {true, true, false, true};
  return true;
}

bool DurableLog::beginRepair(Object& object) {
  auto& state = recoveryStates[object.objectName];
  if (state.repairPending) { object.repairGeneration = state.repair; return !recoveryWriteFailed; }
  const RecoveryGeneration generation{nodeEpoch, ++recoverySerial};
  if (!appendRecovery({recoveryEvent(RecoveryKind::REPAIR_BEGIN, object, generation)}, true)) return false;
  object.repairGeneration = generation;
  return true;
}

bool DurableLog::markCrossedBoot(uint64_t epoch) {
  const auto path = logDirectory() / (wide(epoch) + ".crossed");
  if (!atomicLocalFile(path, "crossed-power-loss\n", true)) {
    failRecovery("the crossed-power-loss flag could not be made durable");
    return false;
  }
  recoveryEpochs[epoch].crossedBoot = true;
  if (auto found = predecessorLogs.find(epoch); found != predecessorLogs.end()) found->second.complete = false;
  return true;
}

kj::Promise<void> DurableLog::readRecovery(bool slice) {
  StartupSlice budget(slice);
  recoveryStates.clear();recoveryEpochs.clear();recoveryReadIncomplete = false;
  std::error_code code;
  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(logDirectory(), code))
    if (entry.path().extension() == ".recovery") files.push_back(entry.path());
  if (code) { recoveryReadIncomplete = true;failRecovery("the recovery directory could not be listed");co_return; }
  std::sort(files.begin(), files.end());
  for (const auto& file : files) {
    const auto epoch = decimal(file.stem().string());
    auto& info = recoveryEpochs[epoch];
    info.present = true;
    auto fileBytes = co_await readStartupHistory(file, timer, slice);
    if (!fileBytes) {
      recoveryReadIncomplete = true;
      if (epoch == nodeEpoch) failRecovery("the active recovery sidecar could not be read");
      continue;
    }
    const auto& bytes = *fileBytes;
    std::string error;const auto pieces = co_await startupFrames(bytes, error, timer, slice);
    info.complete = error.empty();
    bool header = false;
    for (const auto piece : pieces) {
      if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
      if (piece.size() < 8) { info.complete = false; break; }
      Reader checksum{std::string_view(piece).substr(piece.size() - 4)};
      const auto body = std::string_view(piece).substr(0, piece.size() - 4);
      if (checksum.u32() != crc32c(body)) { info.complete = false; break; }
      Reader in{body};
      if (in.block(4) != "DOR1") { info.complete = false; break; }
      RecoveryEvent event;event.kind = RecoveryKind(in.u8());event.objectName = in.text();
      event.generation = {in.u64(), in.u64()};event.shardEpoch = in.u64();event.nodeSeq = in.u64();
      event.dataEpoch = in.u64();event.shard = in.u32();
      if (!in.ok || in.at != body.size() || uint8_t(event.kind) > uint8_t(RecoveryKind::HEAL_DONE)) {
        info.complete = false;break;
      }
      if (!header) {
        if (event.kind != RecoveryKind::HEADER || event.objectName != settings.nodeId ||
            event.generation != RecoveryGeneration{epoch, 1}) { info.complete = false;break; }
        header = info.modern = true;continue;
      }
      if (event.kind == RecoveryKind::HEADER) { info.complete = false;break; }
      const auto identity = parseDurableHostName(event.objectName);
      if (!identity || identity->dataEpoch != event.dataEpoch) { info.complete = false;break; }
      applyRecovery(event);
      auto& object = objects[event.objectName];
      object.objectName = event.objectName;object.namespaceId = identity->namespaceId;object.objectId = identity->objectId;
      object.dataEpoch = identity->dataEpoch;object.shard = event.shard;
      object.shardEpoch = std::max(object.shardEpoch, event.shardEpoch);
      locate(object, identity->namespaceId, identity->objectId, identity->dataEpoch);
      if (event.generation.first == nodeEpoch) recoverySerial = std::max(recoverySerial, event.generation.second);
    }
    info.complete = info.complete && header;
    info.crossedBoot = std::filesystem::exists(logDirectory() / (wide(epoch) + ".crossed"), code);
    if (!info.complete) {
      recoveryReadIncomplete = true;
      log("recovery-prefix-incomplete", "\"epoch\":\"" + std::to_string(epoch) + "\"");
      if (epoch == nodeEpoch) failRecovery("the active recovery sidecar has an incomplete prefix");
    }
  }
}

// ---- Startup: this machine's files, and the records no segment covers ----

kj::Promise<void> DurableLog::start() {
  if (!enabled()) co_return;
  if (startupReady || startupInitializing) co_return;
  const bool slice = !startupAttempted;
  startupAttempted = true;startupInitializing = true;
  const auto initializing = kj::defer([this]() { startupInitializing = false; });
  std::error_code code;
  std::string reason;
  if (!startupMarkerRead) {
    diskTrusted = readCacheMarker(reason);startupNote = reason;startupMarkerRead = true;
  } else reason = startupNote;
  cacheMarkerBootOverride = diskTrusted ? std::nullopt : std::optional<std::string>(previousCacheBootId);
  // An old clean:true marker must not survive any change to object files. A
  // failed write gates all DO traffic and leaves those files untouched.
  if (!writeCacheMarker(false)) {
    ++startupMarkerFailures;startupRetryAfterMs = monotonicTimeMs() + 1000;
    log("startup-marker-failed", "\"reason\":\"clean:false could not be made durable; no object files changed\"");
    co_return;
  }
  if (recoveryWriteFailed) co_return;
  std::filesystem::create_directories(settings.stackDirectory / "do", code);
  KJ_REQUIRE(!code, "the Durable Object cache directory could not be created");
  const auto readingStarted = monotonicTimeMs();
  co_await readOwnLog(slice);
  startupLogReadMs = monotonicTimeMs() - readingStarted;startupLogPeakRssBytes = peakResidentBytes();
  if (!diskTrusted) {
    // The machine restarted and nothing says it was clean: with synchronous=OFF the files may hold pages the kernel
    // never wrote, so every object is rebuilt from its replicated history instead.
    for (auto& [epoch, info] : predecessorLogs) {
      if (epoch == nodeEpoch) continue;
      if (!markCrossedBoot(epoch)) co_return;
      info.complete = false;
    }
    uint64_t removed = 0;
    const auto directories = co_await objectDirectories(code, slice);
    StartupSlice cleanup(slice);
    for (const auto& directory : directories) {
      if (code) break;
      for (std::filesystem::directory_iterator entry(directory, code), end;
          !code && entry != end; entry.increment(code)) {
        if (cleanup.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); cleanup.reset(); }
        std::filesystem::remove_all(entry->path(), code);
        if (code) break;
        ++removed;
      }
    }
    if (code) {
      ++startupCleanupFailures;startupRetryAfterMs = monotonicTimeMs() + 1000;
      log("cache-cleanup-failed", "\"reason\":\"untrusted files remain; the previous boot marker is retained\"");
      co_return;
    }
    // Raw log and exclusion prefixes survive power loss. They may contain the
    // only copy of an ACK and can never prove absence after crossing a boot.
    filesDropped += removed;
    log("cache-dropped", "\"reason\":" + quote(reason) + ",\"entries\":" + std::to_string(removed));
  } else {
    co_await readPositions(slice);
  }
  if (recoveryWriteFailed) co_return;
  co_await classifyEarlier(slice);
  startupPeakRssBytes = peakResidentBytes();
  if (recoveryWriteFailed) co_return;
  if (!diskTrusted) {
    // Do not turn an old boot into a same-boot cache while its invalid files
    // still exist. A crash before this point must repeat untrusted cleanup.
    cacheMarkerBootOverride.reset();
    if (!writeCacheMarker(false)) {
      ++startupMarkerFailures;startupRetryAfterMs = monotonicTimeMs() + 1000;
      log("startup-marker-failed", "\"reason\":\"the cleaned cache boot could not be activated\"");
      co_return;
    }
  }
  startupReady = true;
  // Startup alarm work comes only from owner markers and the small pending-H index. Empty local shards require no
  // object-store reads. The ordinary ownership chain reclaims same-id epochs before the publisher adopts hints.
  diskLimitBytes = settings.config.diskBytes;
  if (!diskLimitBytes) {
    auto space = std::filesystem::space(settings.stackDirectory, code);
    diskLimitBytes = code ? (8ull << 30) : space.capacity / 2;
  }
  const auto logBytes = std::filesystem::file_size(logPath, code);
  log("started", "\"trusted\":" + std::string(diskTrusted ? "true" : "false") + ",\"reason\":" + quote(reason) +
          ",\"logPath\":" + quote(logPath.string()) + ",\"logBytes\":" + (code ? "null" : std::to_string(logBytes)) +
          ",\"objects\":" + std::to_string(objects.size()) + ",\"diskBytes\":\"" + std::to_string(diskLimitBytes) +
          "\",\"logReadMs\":" + std::to_string(startupLogReadMs) + ",\"logReadPeakRssBytes\":\"" + std::to_string(startupLogPeakRssBytes) +
          "\",\"startupPeakRssBytes\":\"" + std::to_string(startupPeakRssBytes) +
          "\",\"peers\":" + std::to_string(settings.config.peers));
}

// Reads the log files this node left and retains the possible writes after the last
// segment marker. Cache admission runs before prepareOpen() may truncate a WAL:
// unlogged frames may already have reached the database through a checkpoint.
kj::Promise<void> DurableLog::readOwnLog(bool slice) {
  predecessorLogs.clear();
  co_await readRecovery(slice);
  StartupSlice budget(slice);
  std::error_code code;
  if (!std::filesystem::exists(logDirectory(), code)) {
    if (code) failRecovery("the append-log directory could not be inspected");
    co_return;
  }
  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(logDirectory(), code)) {
    if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
    if (entry.path().extension() == ".log") files.push_back(entry.path());
    if (entry.path().extension() != ".closed") continue;
    const auto summaryEpoch = decimal(entry.path().stem().string());
    try {
      auto fileBytes = co_await readStartupHistory(entry.path(), timer, slice);
      KJ_REQUIRE(fileBytes.has_value(), "unreadable closed summary");
      const auto& bytes = *fileBytes;capnp::MallocMessageBuilder message;
      auto value = message.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), value);
      const auto reader = value.asReader();const auto epoch = summaryEpoch;
      if (json::text(reader, "nodeId") != settings.nodeId || decimal(json::text(reader, "epoch")) != epoch ||
          !json::boolean(reader, "completeEOF")) continue;
      auto& info = predecessorLogs[epoch];info.closed = true;
      info.sealedTo = info.maxSeq = decimal(json::text(reader, "through"));
      const auto markerBoot = json::text(reader, "bootId");
      const bool crossed = json::boolean(reader, "crossedBoot") ||
          std::filesystem::exists(logDirectory() / (wide(epoch) + ".crossed"), code);
      info.complete = !code && !crossed && !markerBoot.empty() && markerBoot == bootId();
      auto& recovery = recoveryEpochs[epoch];recovery.modern = json::boolean(reader, "modern");
      recovery.complete = true;recovery.crossedBoot = crossed;
      const auto shards = json::field(reader, "shards");
      if (shards.isArray()) for (const auto& shard : shards.getArray())
        info.shards.emplace(json::text(shard, "namespaceId"), uint32_t(numberField(shard, "shard")));
      const auto savedObjects = json::field(reader, "objects");
      if (savedObjects.isArray()) for (const auto& saved : savedObjects.getArray()) {
        const auto name = json::text(saved, "objectName");const auto identity = parseDurableHostName(name);
        if (!identity) continue;
        const auto sourceEpoch = decimal(json::text(saved, "shardEpoch"));
        info.objects.insert(name);info.objectEpochs[name] = std::max(info.objectEpochs[name], sourceEpoch);
        auto& object = objects[name];object.objectName = name;object.namespaceId = identity->namespaceId;
        object.positionNodeEpoch = std::max(object.positionNodeEpoch, epoch);
        object.objectId = identity->objectId;object.dataEpoch = identity->dataEpoch;
        object.shard = uint32_t(numberField(saved, "shard"));object.shardEpoch = std::max(object.shardEpoch, sourceEpoch);
        locate(object, identity->namespaceId, identity->objectId, identity->dataEpoch);
      }
    } catch (const kj::Exception&) {
      predecessorLogs[summaryEpoch].complete = false;
      recoveryReadIncomplete = true;
      log("closed-summary-unreadable", "\"file\":" + quote(entry.path().filename().string()));
    }
  }
  if (code) { recoveryReadIncomplete = true;failRecovery("the append-log directory could not be listed completely");co_return; }
  std::sort(files.begin(), files.end());
  // The last frame of each database any record names, and the generation it belongs to.
  struct Position {
    uint64_t nodeEpoch = 0, nodeSeq = 0, generation = 0;
    uint32_t salt1 = 0, salt2 = 0, pageSize = 0, lastFrame = 0;
    std::string objectName;
    uint32_t facetId = 0, parentFacetId = 0;
    std::string facetName;
    uint64_t txn = 0, shardEpoch = 0, dataEpoch = 0;
    uint32_t shard = 0;
    bool reset = false, deleted = false;
  };
  std::map<std::string, Position> positions;
  uint64_t read = 0, kept = 0, broken = 0;
  std::string fileNames = "[";
  for (const auto& file : files) {
    const auto epoch = decimal(file.stem().string());
    auto fileBytes = co_await readStartupHistory(file, timer, slice);
    if (!fileBytes) {
      recoveryReadIncomplete = true;
      predecessorLogs[epoch].complete = false;
      if (epoch == nodeEpoch) failRecovery("the active append log could not be read completely");
      log("log-read-incomplete", "\"epoch\":\"" + std::to_string(epoch) + "\"");
      continue;
    }
    const auto& bytes = *fileBytes;
    fileNames += (fileNames.size() > 1 ? "," : "") + quote(file.filename().string());
    uint64_t sealedTo = 0;
    std::map<uint64_t, LogRecord> unsealed;
    std::string splitError;
    const auto pieces = co_await startupFrames(bytes, splitError, timer, slice);
    bool complete = splitError.empty();
    if (!complete) ++broken;
    for (const auto encoded : pieces) {
      if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
      std::string error;
      auto record = decodeLogRecord(encoded, &error);
      if (!record || record->nodeEpoch != epoch) {
        // Only the tail of the file can be cut short (one write per record, no flush): the rest is as it was.
        ++broken;
        complete = false;
        break;
      }
      ++read;
      auto& metadata = predecessorLogs[epoch];metadata.maxSeq = std::max(metadata.maxSeq, record->nodeSeq);
      if (record->kind == LogRecordKind::SEAL) {
        sealedTo = std::max(sealedTo, record->nodeSeq);
        continue;
      }
      metadata.objects.insert(hostNameOf(*record));
      metadata.objectEpochs[hostNameOf(*record)] = std::max(metadata.objectEpochs[hostNameOf(*record)], record->shardEpoch);
      metadata.shards.emplace(record->objectName.substr(0, record->objectName.find('/')), record->shard);
      auto& position = positions[record->shipperKey()];
      position.nodeEpoch = record->nodeEpoch;
      position.nodeSeq = record->nodeSeq;
      position.objectName = record->objectName;
      position.dataEpoch = record->dataEpoch;
      position.facetId = record->facetId;
      position.parentFacetId = record->parentFacetId;
      position.facetName = record->facetName;
      position.shard = record->shard;
      position.shardEpoch = std::max(position.shardEpoch, record->shardEpoch);
      position.deleted = record->kind == LogRecordKind::DELETED;
      if (record->kind == LogRecordKind::FRAMES) {
        position.generation = record->generation;
        position.salt1 = record->salt1;
        position.salt2 = record->salt2;
        position.pageSize = record->pageSize;
        position.lastFrame = record->firstFrame + record->frames - 1;
        position.txn = record->txn + record->commits - 1;
        position.reset = false;
      } else {
        position.reset = record->kind == LogRecordKind::RESET;
        position.generation = 0;
        position.lastFrame = 0;
      }
      const auto sequence = record->nodeSeq;
      unsealed[sequence] = std::move(*record);
    }
    while (!unsealed.empty() && unsealed.begin()->first <= sealedTo) {
      if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
      unsealed.erase(unsealed.begin());
    }
    auto& info = predecessorLogs[epoch];
    auto& recovery = recoveryEpochs[epoch];
    recovery.crossedBoot = recovery.crossedBoot || std::filesystem::exists(logDirectory() / (wide(epoch) + ".crossed"), code);
    info.complete = complete && !code && (!recovery.present || recovery.complete) && !recovery.crossedBoot;
    info.closed = false;
    if (!complete) recoveryReadIncomplete = true;
    if (epoch == nodeEpoch && !complete) failRecovery("the active append log has an incomplete prefix");
    info.sealedTo = sealedTo;
    for (const auto& [sequence, record] : unsealed) {
      if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
      info.startupUnsealed.emplace(record.objectName, record.shardEpoch);
      noteEarlierUnpublished(info, record);
    }
    for (auto entry = unsealed.begin(); entry != unsealed.end();) {
      if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
      const auto& record = entry->second;
      auto state = recoveryStates.find(hostNameOf(record));
      if (state == recoveryStates.end() || !state->second.exclusions.contains({record.shardEpoch, record.nodeSeq})) {
        ++entry;
      } else {
        earlierExcluded[epoch][entry->first] = std::move(entry->second);
        entry = unsealed.erase(entry);
      }
    }
    kept += unsealed.size();
    if (!unsealed.empty()) {
      restoredEpochs.insert(epoch);
      if (auto prior = earlier.find(epoch); prior != earlier.end()) {
        for (const auto& [sequence, record] : prior->second) unindexEarlierRecord({epoch, sequence}, record);
      }
      // At every yield, any indexed position must already name a stored record.
      auto& held = earlier[epoch];
      held = std::move(unsealed);
      for (const auto& [sequence, record] : held) {
        if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
        indexEarlierRecord({epoch, sequence}, record);
      }
    }
  }
  // Keep ownership identity for classifying and resealing predecessor records.
  // Local database images are discarded at startup and rebuilt on first open.
  for (auto& [key, position] : positions) {
    if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
    const auto name = position.objectName + "/" + std::to_string(position.dataEpoch);
    auto reference = parseDurableHostName(name);
    if (!reference) continue;
    auto& object = objects[name];
    object.namespaceId = reference->namespaceId;
    object.objectId = reference->objectId;
    object.dataEpoch = reference->dataEpoch;
    object.objectName = name;
    locate(object, reference->namespaceId, reference->objectId, reference->dataEpoch);
    object.shard = position.shard;
    object.shardEpoch = std::max(object.shardEpoch, position.shardEpoch);
    object.positionNodeEpoch = std::max(object.positionNodeEpoch, position.nodeEpoch);
  }
  for (auto& [name, object] : objects)
    if (auto state = recoveryStates.find(name); state != recoveryStates.end() && state->second.repairPending)
      object.repairGeneration = state->second.repair;
  log("log-read", "\"files\":" + std::to_string(files.size()) + ",\"fileNames\":" + fileNames +
          "],\"records\":" + std::to_string(read) +
          ",\"unsealed\":" + std::to_string(kept) + ",\"objects\":" + std::to_string(objects.size()) +
          ",\"cutShort\":" + std::to_string(broken));
  co_return;
}

kj::Promise<void> DurableLog::classifyEarlier(bool slice) {
  StartupSlice budget(slice);
  const auto started = monotonicTimeMs();
  if (!store || earlier.empty()) co_return;
  using Position = std::pair<uint64_t, uint64_t>;
  using SegmentKey = std::tuple<std::string, uint64_t, uint64_t, uint64_t>;
  struct Candidate { std::string name, digest; uint64_t epoch, sequence, shardEpoch, bytes; uint32_t facet; };
  struct Receipt { std::string object, digest; Position position; uint32_t facet; };
  struct Cached { std::vector<Receipt> receipts; bool incomplete = false; uint64_t shardEpoch = 0; };
  std::map<std::string, std::vector<Candidate>> groups;
  std::set<std::tuple<std::string, uint64_t, uint64_t>> targets;
  for (const auto& [epoch, pending] : earlier) for (const auto& [sequence, record] : pending) {
    if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
    const auto bytes = encodeLogRecord(record);
    groups[record.objectName].push_back({hostNameOf(record), sha256Hex(bytes), epoch, sequence, record.shardEpoch, bytes.size(), record.facetId});
    targets.emplace(record.objectName, record.shardEpoch, record.nodeSeq);
  }
  constexpr uint64_t MAX_SEGMENT_READS = 256, MAX_OLD_MANIFEST_READS = 256, RECEIPT_BYTES = 64ull << 20;
  uint64_t segmentReads = 0, oldReads = 0, receiptBytes = 0;
  std::map<SegmentKey, Cached> cache;
  auto segmentKey = [](const ManifestSegment& ref) { return SegmentKey{ref.nodeId, ref.nodeEpoch, ref.firstSeq, ref.lastSeq}; };
  auto load = [&](const ManifestSegment& ref) -> kj::Promise<void> {
    const auto key = segmentKey(ref);
    if (cache.contains(key)) co_return;
    const uint64_t keyBytes = sizeof(Cached) + sizeof(SegmentKey) + ref.nodeId.size() * 2 + 128;
    if (cache.size() >= MAX_SEGMENT_READS || receiptBytes + keyBytes > RECEIPT_BYTES) co_return;
    receiptBytes += keyBytes;
    auto& saved = cache[key];
    saved.shardEpoch = ref.shardEpoch;
    if (!ref.firstSeq || ref.lastSeq < ref.firstSeq) { saved.incomplete = true;co_return; }
    if (segmentReads >= MAX_SEGMENT_READS) { saved.incomplete = true;co_return; }
    ++segmentReads;++startupSegmentReads;
    try {
      auto response = co_await timer.timeoutAfter(std::max(settings.peerTimeoutMs, settings.config.confirmTimeoutMs) * kj::MILLISECONDS,
          store->get(nodeSegmentKey(ref.nodeId, ref.nodeEpoch, ref.segment)));
      requireStorage(response.status == 200, "a classification segment is unreadable");
      auto contents = decodeSegment(response.bytes);
      requireStorage(contents && contents->header.nodeId == ref.nodeId && contents->header.nodeEpoch == ref.nodeEpoch &&
          contents->header.segment == ref.segment && contents->header.firstSeq == ref.firstSeq && contents->header.lastSeq == ref.lastSeq,
          "a classification segment does not match its reference");
      for (const auto& record : contents->records) {
        if (!targets.contains({record.objectName, record.shardEpoch, record.nodeSeq})) continue;
        const uint64_t charge = 2 * (sizeof(Receipt) + record.objectName.size() + 64 + 128);
        if (receiptBytes + charge > RECEIPT_BYTES) { saved.incomplete = true;continue; }
        receiptBytes += charge;
        saved.receipts.push_back({record.objectName, sha256Hex(encodeLogRecord(record)), {record.shardEpoch, record.nodeSeq}, record.facetId});
      }
    } catch (const kj::Exception&) { saved.incomplete = true; }
  };
  struct Summary { uint64_t first = 0, last = 0, records = 0, bytes = 0;std::string digests; };
  for (const auto& [wanted, candidates] : groups) {
    std::optional<ObjectManifest> manifest;
    bool fallback = false;
    try { manifest = co_await readManifest(wanted.substr(0,64), wanted.substr(65)); }
    catch (const kj::Exception&) { fallback = true; }
    std::set<std::tuple<uint64_t, uint64_t, std::string>> found;
    std::map<SegmentKey, std::set<uint64_t>> currentRefs;
    if (manifest) for (const auto& ref : manifest->segments) currentRefs[segmentKey(ref)].insert(ref.shardEpoch);
    auto covered = [&](const Receipt& record) {
      if (!manifest || !manifest->hasSnapshot) return false;
      if (record.position.first < manifest->snapshotShardEpoch) return true;
      if (record.position.first != manifest->snapshotShardEpoch) return false;
      auto facet = std::find_if(manifest->facets.begin(), manifest->facets.end(), [&](const ManifestFacet& f) { return f.facetId == record.facet; });
      return facet != manifest->facets.end() && record.position.second <= facet->upToSeq;
    };
    auto scan = [&](const ObjectManifest& version, bool historical) -> kj::Promise<void> {
      std::map<SegmentKey, uint64_t> visited;
      for (const auto& ref : version.segments) {
        const auto key = segmentKey(ref);
        auto [seen, added] = visited.emplace(key, ref.shardEpoch);
        if (!added) { if (seen->second != ref.shardEpoch) fallback = true;continue; }
        auto current = currentRefs.find(key);
        if (historical && current != currentRefs.end() && current->second.contains(ref.shardEpoch)) continue;
        co_await load(ref);
        auto cached = cache.find(key);
        if (cached == cache.end()) { fallback = true;continue; }
        const auto& saved = cached->second;fallback = fallback || saved.incomplete;
        // Recovery groups can reuse a publisher's sequence range for another
        // source shard epoch. That is ambiguous evidence, not a byte receipt
        // for the different reference; keep it for default adoption/resealing.
        if (saved.shardEpoch != ref.shardEpoch) { fallback = true;continue; }
        for (const auto& receipt : saved.receipts)
          if (receipt.object == wanted && (!historical || covered(receipt)))
            found.emplace(receipt.position.first, receipt.position.second, receipt.digest);
      }
    };
    if (manifest) {
      co_await scan(*manifest, false);
      auto missing = [&]() {
        return std::any_of(candidates.begin(), candidates.end(), [&](const Candidate& c) {
          return covered(Receipt{wanted, c.digest, {c.shardEpoch, c.sequence}, c.facet}) &&
              !found.contains({c.shardEpoch, c.sequence, c.digest});
        });
      };
      for (uint64_t revision = manifest->revision; revision > 1 && manifest->hasSnapshot && missing();) {
        if (oldReads >= MAX_OLD_MANIFEST_READS) { fallback = true;break; }
        --revision;++oldReads;++startupOldManifestReads;
        try {
          auto response = co_await timer.timeoutAfter(std::max(settings.peerTimeoutMs, settings.config.confirmTimeoutMs) * kj::MILLISECONDS,
              store->get(objectManifestKey(wanted.substr(0,64), wanted.substr(65), revision)));
          auto old = response.status == 200 ? parseObjectManifest(response.bytes) : std::nullopt;
          requireStorage(old && old->revision == revision && old->namespaceId + "/" + old->objectId == wanted,
              "an old classification manifest is unreadable");
          co_await scan(*old, true);
        } catch (const kj::Exception&) { fallback = true; }
      }
    }
    std::map<std::tuple<uint32_t, uint64_t, std::string, uint64_t>, Summary> summaries;
    for (const auto& candidate : candidates) {
      const auto object = objects.find(candidate.name);
      const bool history = found.contains({candidate.shardEpoch, candidate.sequence, candidate.digest});
      const bool fenced = !history && !fallback && manifest && object != objects.end() && manifest->shardEpoch > object->second.shardEpoch;
      const uint32_t kind = history ? 0 : fenced ? 1 : 2;
      auto& summary = summaries[{kind, candidate.epoch, candidate.name, candidate.shardEpoch}];
      if (!summary.first) summary.first = candidate.sequence;
      summary.last = candidate.sequence;++summary.records;summary.bytes += candidate.bytes;summary.digests += candidate.digest;
      if (history) ++startupHistoryRecords;
      else if (fenced) { ++startupFencedRecords;continue; }
      else { ++startupAdoptRecords;if (fallback) ++startupReadFallbackRecords;continue; }
      auto epoch = earlier.find(candidate.epoch);
      if (epoch == earlier.end()) continue;
      auto record = epoch->second.find(candidate.sequence);
      if (record == epoch->second.end()) continue;
      noteEarlierRetired(candidate.epoch, record->second);
      unindexEarlierRecord({candidate.epoch, candidate.sequence}, record->second);
      epoch->second.erase(record);
      if (epoch->second.empty()) earlier.erase(epoch);
    }
    for (const auto& [key, summary] : summaries) {
      const auto kind = std::get<0>(key);
      const auto fields = "\"classification\":" + quote(kind == 0 ? "already-history" : kind == 1 ? "higher-shard-fenced" : "adopt-reseal") +
          ",\"object\":" + quote(std::get<2>(key)) + ",\"sourceNodeEpoch\":\"" + std::to_string(std::get<1>(key)) +
          "\",\"sourceEpoch\":\"" + std::to_string(std::get<3>(key)) + "\",\"firstSeq\":\"" + std::to_string(summary.first) +
          "\",\"lastSeq\":\"" + std::to_string(summary.last) + "\",\"records\":" + std::to_string(summary.records) +
          ",\"bytes\":\"" + std::to_string(summary.bytes) + "\",\"sha256\":" + quote(sha256Hex(summary.digests)) +
          ",\"readFallback\":" + (fallback ? "true" : "false");
      log("restored-startup-classified", fields);
    }
  }
  persistEarlierSeals();
  startupClassificationMs = monotonicTimeMs() - started;
  log("startup-classification-complete", "\"history\":" + std::to_string(startupHistoryRecords) +
      ",\"fenced\":" + std::to_string(startupFencedRecords) + ",\"adopt\":" + std::to_string(startupAdoptRecords) +
      ",\"readFallback\":" + std::to_string(startupReadFallbackRecords) + ",\"segmentReads\":" + std::to_string(segmentReads) +
      ",\"oldManifestReads\":" + std::to_string(oldReads) + ",\"receiptBytes\":" + std::to_string(receiptBytes) +
      ",\"ms\":" + std::to_string(startupClassificationMs));
}

// Cuts a database's WAL back to `lastFrame` of the generation the log names and removes its shared memory, so that
// the next process to open it sees exactly what this node has: with synchronous=OFF the kernel keeps what was
// written, and what a process committed without handing it on was never answered to anybody.
std::optional<std::string> DurableLog::cutBackWal(const Object& object, uint32_t facetId, uint32_t salt1, uint32_t salt2,
    uint32_t pageSize, uint32_t lastFrame) {
  const auto& storageId = object.storageId;
  const auto wal = databasePath(object, facetId, "-wal");
  std::error_code code;
  if (!std::filesystem::exists(wal, code)) return std::nullopt;
  const auto size = std::filesystem::file_size(wal, code);
  if (code) return std::nullopt;
  auto failed = [&](const char* step, const char* operation, int error, ssize_t bytes = -1) -> std::optional<std::string> {
    ++walCutBackFailures;
    auto fields = "\"object\":" + quote(object.objectName) + ",\"storageId\":" + quote(storageId) +
        ",\"facet\":" + std::to_string(facetId) + ",\"step\":" + quote(step) + ",\"operation\":" + quote(operation) +
        ",\"errno\":" + std::to_string(error);
    if (bytes >= 0) fields += ",\"readBytes\":" + std::to_string(bytes);
    log("wal-cut-back-failed", fields);
    return std::string("WAL ") + (std::string_view(step) == "read" ? "read" : "write") +
        " failed at " + operation + " (errno " + std::to_string(error) + ")";
  };
  uint64_t want = 0;
  if (size >= 32 && pageSize >= 512) {
    const int fd = ::open(wal.c_str(), O_RDONLY);
    if (fd < 0) return failed("read", "open", errno);
    unsigned char header[32];
    ssize_t got;
    do { got = ::pread(fd, header, sizeof(header), 0); } while (got < 0 && errno == EINTR);
    const auto error = got < 0 ? errno : 0;
    ::close(fd);
    if (got < 0 || got != ssize_t(sizeof(header))) return failed("read", "pread", error, got);
    if (beU32(header + 16) == salt1 && beU32(header + 20) == salt2) {
      want = 32 + uint64_t(lastFrame) * (24 + uint64_t(pageSize));
    }
  }
  if (want && want >= size) {
    std::filesystem::remove(databasePath(object, facetId, "-shm"), code);
    return std::nullopt;
  }
  // Either a generation this node has no record of (its frames were never handed on) or a tail past the last record.
  if (want) {
    const int fd = ::open(wal.c_str(), O_WRONLY);
    if (fd < 0) return failed("truncate", "open", errno);
    if (::ftruncate(fd, off_t(want)) < 0) {
      const auto error = errno;
      ::close(fd);
      return failed("truncate", "ftruncate", error);
    }
    if (::fsync(fd) < 0) {
      const auto error = errno;
      ::close(fd);
      return failed("sync", "fsync", error);
    }
    ::close(fd);
  } else {
    std::filesystem::remove(wal, code);
    if (code) return failed("remove", "remove", code.value());
  }
  std::filesystem::remove(databasePath(object, facetId, "-shm"), code);
  ++walCutBacks;
  log("wal-cut-back", "\"storageId\":" + quote(storageId) + ",\"facet\":" + std::to_string(facetId) + ",\"was\":\"" +
          std::to_string(size) + "\",\"now\":\"" + std::to_string(want) + "\"");
  return std::nullopt;
}

kj::Promise<void> DurableLog::stop(double deadlineMonotonicMs) {
  stopped = true;
  sealDrainDeadlineMs = deadlineMonotonicMs;
  for (auto& [id, peer] : peers) retirePeer(peer);
  if (!enabled()) co_return;
  try {
    // A steady-state seal may be selecting, encoding or publishing when the node stops.
    // Wait for it to finish before deciding which confirmed records are still left to seal.
    while (sealing) co_await timer.afterDelay(1 * kj::MILLISECONDS);
    while (!records.empty() && !sealDrainExpired()) {
      const auto before = sealedSeq;
      co_await sealNow("drain");
      if (sealedSeq == before) break;  // Nothing sealable; retain the tail on disk and peers.
    }
  } catch (const kj::Exception& error) {
    log("drain-seal-failed", "\"error\":" + quote(std::string(error.getDescription().cStr())));
  }
  // A clean marker may authorize reuse on a later boot. Sync predecessor logs
  // as well as this epoch before writing it.
  bool clean = !logAppendFailed;
  std::error_code code;
  for (const auto& entry : std::filesystem::directory_iterator(logDirectory(), code)) {
    if (entry.path().extension() != ".log" && entry.path().extension() != ".recovery") continue;
    const int fd = ::open(entry.path().c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0) { clean = false; continue; }
#ifdef __APPLE__
    const auto result = ::fsync(fd);
#else
    const auto result = ::fdatasync(fd);
#endif
    if (result != 0) {
      clean = false;
      log("log-flush-failed", "\"path\":" + quote(entry.path().string()) +
          ",\"errno\":" + std::to_string(errno));
    }
    ::close(fd);
  }
  if (code) clean = false;
  // Cache reuse is deliberately limited to the same kernel boot. The SQLite
  // files use synchronous=OFF; a drain is not a promise that their pages survived power loss.
  if (clean && !recoveryWriteFailed && startupReady) writeCacheMarker(false);
  log("stopped", "\"records\":" + std::to_string(records.size()) + ",\"sealedSeq\":\"" + std::to_string(sealedSeq) + "\"");
}

// ---- The channel to a durable process ----

int DurableLog::openChannel(const std::string& processId, uint64_t threadedGeneration) {
  if (!enabled()) return -1;
  int fds[2];
  if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
    log("channel-failed", "\"processId\":" + quote(processId) + ",\"errno\":" + std::to_string(errno));
    return -1;
  }
  closeChannel(processId);
  auto& channel = channels[processId];
  channel.processId = processId;
  channel.threadedGeneration = threadedGeneration;
  ::fcntl(fds[0], F_SETFD, FD_CLOEXEC);
  channel.stream = lowLevel.wrapSocketFd(fds[0], kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
          kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC);
  channel.open = true;
  channel.writeChain = kj::READY_NOW;
  channel.reader = readChannel(channel).eagerlyEvaluate([this, processId](kj::Exception&& error) {
    auto found = channels.find(processId);
    if (found != channels.end()) {
      // A failing frame handler is not retried on the close path.
      found->second.inbox.clear();
      channelEnded(found->second, "the durable channel failed");
    }
    log("channel-ended", "\"processId\":" + quote(processId) + ",\"reason\":" +
            quote(std::string(error.getDescription().cStr())));
  });
  return fds[1];
}

void DurableLog::closeChannel(const std::string& processId) {
  auto found = channels.find(processId);
  if (found == channels.end()) return;
  // The coroutine borrows this Channel and its stream. Cancel it before the
  // map entry is destroyed or the same process id gets a replacement channel.
  found->second.reader = nullptr;
  channelEnded(found->second, "the durable process was closed");
  channels.erase(found);
}

bool DurableLog::drainChannel(const std::string& processId) {
  const auto found = channels.find(processId);
  if (found == channels.end() || found->second.threadedGeneration == 0) return true;
  auto& channel = found->second;
  if (!channel.open) return false;
  channel.draining = true;
  for (const auto& [key, registration] : channel.openFacets) {
    if (key.ends_with("/0")) askClose(channel, registration.id);
  }
  return channel.openFacets.empty();
}

kj::Promise<void> DurableLog::readChannel(Channel& channel) {
  auto buffer = kj::heapArray<kj::byte>(65536);
  for (;;) {
    const auto read = co_await channel.stream->tryRead(buffer.begin(), 1, buffer.size());
    if (read == 0) {
      channelEnded(channel, "the durable channel reached EOF");
      log("channel-closed", "\"processId\":" + quote(channel.processId));
      co_return;
    }
    channel.inbox.append(reinterpret_cast<const char*>(buffer.begin()), read);
    while (consumeChannelInbox(channel, true)) co_await timer.afterDelay(1 * kj::MICROSECONDS);
  }
}

bool DurableLog::consumeChannelInbox(Channel& channel, bool sliced) {
  constexpr size_t FRAMES_PER_SLICE = 16;
  constexpr double SLICE_MS = 0.5;
  const auto began = sliced ? monotonicTimeMs() : 0;
  r47::Span slice(r47::DURABLE_CHANNEL_SLICE);
  size_t at = 0, frames = 0;
  // At every suspension the inbox starts at the first unprocessed frame. Close can cancel
  // the reader and drain this same inbox without replaying its already processed prefix.
  KJ_DEFER(if (at != 0) channel.inbox.erase(0, at));
  while (channel.inbox.size() - at >= 4) {
    uint32_t length = 0;
    for (int i = 0; i < 4; i++) length |= uint32_t(uint8_t(channel.inbox[at + i])) << (8 * i);
    if (channel.inbox.size() - at - 4 < length) break;
    const auto message = std::string_view(channel.inbox).substr(at + 4, length);
    at += 4 + length;
    ++channel.messages;
    ++frames;
    onMessage(channel, message);
    if (sliced && (frames >= FRAMES_PER_SLICE || monotonicTimeMs() - began >= SLICE_MS)) {
      slice.finish(frames);
      return true;
    }
  }
  slice.finish(frames);
  return false;
}

void DurableLog::sendToChannel(Channel& channel, std::string message) {
  if (!channel.open) return;
  std::string framed;
  putU32(framed, uint32_t(message.size()));
  framed += message;
  auto bytes = kj::heapArray<kj::byte>(framed.size());
  std::memcpy(bytes.begin(), framed.data(), framed.size());
  auto& stream = *channel.stream;
  channel.writeChain = channel.writeChain
                           .then([&stream, bytes = kj::mv(bytes)]() mutable -> kj::Promise<void> {
    auto promise = stream.write(bytes.asPtr());
    return promise.attach(kj::mv(bytes));
  }).eagerlyEvaluate([this, processId = channel.processId](kj::Exception&& error) {
    auto found = channels.find(processId);
    if (found != channels.end()) channelEnded(found->second, "the durable channel could not be written");
    log("channel-write-failed", "\"processId\":" + quote(processId) + ",\"error\":" +
            quote(std::string(error.getDescription().cStr())));
  });
}

void DurableLog::answer(Channel& channel, uint64_t request, uint8_t status) {
  std::string message;
  putU8(message, TO_ACK);
  putU64(message, request);
  putU8(message, status);
  sendToChannel(channel, std::move(message));
}

void DurableLog::tellSealed(const std::string& key, uint32_t salt1, uint32_t salt2, uint32_t upToFrame) {
  std::string payload;
  putU32(payload, salt1);
  putU32(payload, salt2);
  putU32(payload, upToFrame);
  sendToOpenChannels(TO_SEALED, key, payload);
}

void DurableLog::tellBreak(const std::string& key, const std::string& reason) {
  std::string payload;
  putText(payload, reason);
  sendToOpenChannels(TO_BREAK, key, payload);
}

void DurableLog::askCheckpoint(const std::string& key) {
  ++checkpointAsks;
  sendToOpenChannels(TO_CHECKPOINT, key, {});
}

void DurableLog::sendToOpenChannels(uint8_t type, const std::string& key, const std::string& payload) {
  for (auto& [id, channel] : channels) {
    std::string message;
    putU8(message, type);
    if (channel.threadedGeneration != 0) {
      auto open = channel.openFacets.find(key);
      if (open == channel.openFacets.end()) continue;
      putU64(message, open->second.id);
    } else {
      putText(message, key);
    }
    message += payload;
    sendToChannel(channel, std::move(message));
  }
}

void DurableLog::onMessage(Channel& channel, std::string_view message) {
  Reader in{message};
  const auto type = in.u8();
  uint64_t openId = 0;
  if (channel.threadedGeneration != 0 && type != FROM_HELLO) {
    KJ_REQUIRE(channel.pid != 0, "threaded WAL message before readiness");
    openId = in.u64();
    KJ_REQUIRE(in.ok && openId != 0, "missing threaded WAL open ID");
  }
  auto isCurrentOpen = [&](std::string_view key) {
    if (openId == 0) return true;
    const auto registration = channel.openFacets.find(key);
    return registration != channel.openFacets.end() && registration->second.id == openId;
  };
  switch (type) {
    case FROM_HELLO: {
      const auto pid = in.u32();
      if (channel.threadedGeneration != 0) {
        in.u64();  // child's monotonic time
        const auto revision = in.u32();
        const auto generation = in.u64();
        KJ_REQUIRE(channel.pid == 0 && pid != 0 && in.ok && in.at == message.size() &&
            revision == THREADED_WAL_REVISION && generation == channel.threadedGeneration,
            "threaded WAL readiness mismatch", revision, generation);
        std::string ready;
        putU8(ready, TO_READY);
        putU32(ready, THREADED_WAL_REVISION);
        putU64(ready, generation);
        sendToChannel(channel, std::move(ready));
      }
      channel.pid = pid;
      log("process-attached", "\"processId\":" + quote(channel.processId) + ",\"pid\":" + std::to_string(channel.pid));
      return;
    }
    case FROM_FRAMES: {
      LogRecord record;
      const auto request = in.u64();
      const auto key = in.text();
      if (!isCurrentOpen(key)) {
        answer(channel, request, ACK_UNAVAILABLE);
        return;
      }
      record.objectName = in.text();
      record.storageId = in.text();
      record.facetId = in.u32();
      record.parentFacetId = in.u32();
      record.facetName = in.text();
      record.generation = in.u64();
      record.salt1 = in.u32();
      record.salt2 = in.u32();
      record.pageSize = in.u32();
      record.firstFrame = in.u32();
      record.frames = in.u32();
      record.commits = in.u32();
      in.u64();  // when the commit was read, by the process's monotonic clock
      in.u64();  // how long reading the frames took
      record.sentAtMonotonicNs = in.u64();
      if (in.u8() != 0) record.header = in.block(32);
      record.frameBytes = in.block(message.size() - in.at);
      if (!in.ok || record.key() != key) {
        log("message-refused", "\"processId\":" + quote(channel.processId) + ",\"reason\":\"frames do not read as a message\"");
        answer(channel, request, ACK_UNAVAILABLE);
        return;
      }
      record.kind = LogRecordKind::FRAMES;
      channel.frames += record.frames;
      channel.bytes += record.frameBytes.size();
      takeRecord(channel, std::move(record), request);
      return;
    }
    case FROM_OPEN: {
      const auto key = in.text();
      const auto objectName = in.text();
      const auto storageId = in.text();
      const auto facetId = in.u32();
      const auto parentFacetId = in.u32();
      const auto facetName = in.text();
      const auto salt1 = in.u32();
      const auto salt2 = in.u32();
      const auto frames = in.u32();
      const auto walBytes = in.u64();
      const auto databaseBytes = in.u64();
      if (!in.ok) return;
      if (openId != 0 && message.size() - in.at >= 16) {
        in.u64();  // child's monotonic time
        const auto localOpenNs = in.u64();
        ++threadedHomeOpenSamples;
        threadedHomeOpenMaxNs = std::max(threadedHomeOpenMaxNs, localOpenNs);
      }
      auto* object = objectOf(objectName);
      if (openId != 0) {
        KJ_REQUIRE(object != nullptr && object->dispatchedChannel == channel.processId &&
            storageId == object->storageId && key == objectName + "/" + std::to_string(facetId) &&
            !channel.openFacets.contains(key), "threaded WAL open does not match dispatch");
      }
      if (object == nullptr) {
        log("open-unknown", "\"object\":" + quote(objectName) + ",\"facet\":" + std::to_string(facetId));
        return;
      }
      if (storageId != object->storageId) {
        tellBreak(key, "Durable Object storage is unavailable: storage identity mismatch (STORAGE_UNAVAILABLE)");
        log("open-refused", "\"object\":" + quote(objectName) + ",\"reason\":\"storage identity mismatch\"");
        return;
      }
      log("root-storage-verified", "\"namespaceId\":" + quote(object->namespaceId) +
            ",\"objectId\":" + quote(object->objectId) + ",\"dataEpoch\":\"" + std::to_string(object->dataEpoch) +
            "\",\"uniqueKey\":" + quote(object->uniqueKey) + ",\"storageId\":" + quote(object->storageId) +
            ",\"reportedStorageId\":" + quote(storageId) + ",\"facetId\":" + std::to_string(facetId));
      object->dispatchPending = false;
      auto& facet = object->facets[facetId];
      facet.open = true;facet.processId = channel.processId;
      scheduleIdleCheckpoint(*object);
      facet.openEpoch = object->openEpoch;facet.openGeneration = object->openGeneration;
      channel.openFacets[key] = {{object->openEpoch, object->openGeneration}, openId};
      facet.facetId = facetId;
      facet.parentFacetId = parentFacetId;
      facet.facetName = facetName;
      facet.databaseBytes = databaseBytes;
      sized(*object);
      // The position this node confirmed is the one the files must be at: a process that committed a transaction and
      // died before its frames arrived leaves frames nobody knows, and nothing may read them (experiment 25, B.6).
      const bool sameGeneration = facet.salt1 == salt1 && facet.salt2 == salt2;
      const bool past = frames > (sameGeneration ? facet.lastFrame : 0);
      if (past && frames > 0) {
        ++positionMismatches;
        const auto reason = "this machine's copy of the object holds frames the node never confirmed";
        tellBreak(key, std::string("Durable Object storage is unavailable: ") + reason + " (STORAGE_UNAVAILABLE)");
        log("position-mismatch", "\"object\":" + quote(objectName) + ",\"facet\":" + std::to_string(facetId) +
                ",\"frames\":" + std::to_string(frames) + ",\"known\":" + std::to_string(sameGeneration ? facet.lastFrame : 0) +
                ",\"walBytes\":\"" + std::to_string(walBytes) + "\"");
        markRepair(*object, reason);
      } else {
        facet.salt1 = salt1;
        facet.salt2 = salt2;
        facet.lastFrame = frames;
      }
      if (object->state == ObjectState::LOCAL && !object->storageId.empty() && !object->cacheOpen) {
        object->cacheOpen = true;
        if (!writePosition(*object)) {
          object->cacheOpen = false;
          log("open-marker-failed", "\"object\":" + quote(objectName) + ",\"errno\":" + std::to_string(errno));
          const auto reason = "the object open marker could not be written";
          tellBreak(key, std::string("Durable Object storage is unavailable: ") + reason + " (STORAGE_UNAVAILABLE)");
          markRepair(*object, reason);
        }
      }
      if (openId != 0) {
        const bool fenced = (hooks.namespaceActive && !hooks.namespaceActive(object->namespaceId)) ||
            (facetId == 0 && object->definitionDrainDeadline != kj::none);
        const bool authorized = object->state == ObjectState::LOCAL && !fenced;
        std::string opened;
        putU8(opened, TO_OPENED);
        putU64(opened, openId);
        putU8(opened, authorized ? ACK_OK : ACK_UNAVAILABLE);
        sendToChannel(channel, std::move(opened));
        if (fenced && facetId == 0) {
          if (const auto deadline = object->definitionDrainDeadline;
              deadline != kj::none && timer.now() >= KJ_ASSERT_NONNULL(deadline)) {
            askCodeUpdateClose(channel, channel.openFacets.at(key), *object);
          } else {
            askClose(channel, openId);
          }
        }
      }
      return;
    }
    case FROM_CHECKPOINT: {
      const auto key = in.text();
      if (!isCurrentOpen(key)) return;
      const auto generation = in.u64();
      const auto salt1 = in.u32();
      const auto salt2 = in.u32();
      const auto frames = in.u32();
      in.u32();  // of those, confirmed
      const auto databaseBytes = in.u64();
      if (!in.ok) return;
      if (openId != 0 && message.size() - in.at >= 18) {
        in.u64();  // checkpoint start
        const auto tookNs = in.u64();
        in.text();  // checkpoint reason
        const auto bytes = in.u64();
        if (in.ok) {
          ++threadedHomeCheckpointSamples;
          threadedHomeCheckpointMaxNs = std::max(threadedHomeCheckpointMaxNs, tookNs);
          threadedHomeCheckpointMaxWalBytes = std::max(threadedHomeCheckpointMaxWalBytes, bytes);
        }
      }
      ++checkpoints;
      const auto split = key.rfind('/');
      if (split == std::string::npos) return;
      const auto objectName = key.substr(0, split);
      const auto facetId = uint32_t(decimal(key.substr(split + 1)));
      auto* object = objectOf(objectName);
      if (object == nullptr) return;
      ++object->snapshotVersion;
      auto& facet = object->facets[facetId];
      const auto known = facet.loggedGeneration == generation && facet.loggedSalt1 == salt1 && facet.loggedSalt2 == salt2
          ? facet.loggedFrame : 0;
      facet.checkpointGeneration = generation;facet.checkpointFrames = frames;
      facet.checkpointSalt1 = salt1;facet.checkpointSalt2 = salt2;
      facet.checkpointSafe = frames <= known;
      if (!facet.checkpointSafe) {
        ++positionMismatches;
        log("checkpoint-unlogged-frames", "\"object\":" + quote(objectName) + ",\"facet\":" + std::to_string(facetId) +
            ",\"frames\":" + std::to_string(frames) + ",\"logged\":" + std::to_string(known));
        markRepair(*object, "the database checkpoint contains frames absent from the daemon log");
        return;
      }
      facet.databaseBytes = databaseBytes;
      // The database file now holds every transaction of the generation that was checkpointed, so a snapshot taken
      // from here needs only the records after that one.
      facet.checkpointedSeq = facet.lastSeq;
      facet.lastFrame = 0;
      facet.salt1 = 0;
      facet.salt2 = 0;
      facet.generation = generation;
      sized(*object);
      const auto bytes = object->bytes;
      // The files are now at a position the node has confirmed: recorded beside them, so a restart of the daemon or
      // of a durable process within the same boot reuses them instead of rebuilding from the whole history.
      writePosition(*object);
      log("checkpoint", "\"object\":" + quote(objectName) + ",\"facet\":" + std::to_string(facetId) + ",\"frames\":" +
              std::to_string(frames) + ",\"databaseBytes\":\"" + std::to_string(databaseBytes) + "\"");
      tasks.add(snapshotIfDue(objectName));
      return;
    }
    case FROM_RESET: {
      LogRecord record;
      if (!isCurrentOpen(in.text())) return;  // the key, which the fields below give again
      record.objectName = in.text();
      record.storageId = in.text();
      record.facetId = in.u32();
      record.parentFacetId = in.u32();
      record.facetName = in.text();
      if (!in.ok) return;
      record.kind = LogRecordKind::RESET;
      auto* object = objectOf(record.objectName);
      if (object != nullptr && record.storageId != object->storageId) {
        log("record-refused", "\"object\":" + quote(record.objectName) + ",\"reason\":\"storage identity mismatch\"");
        return;
      }
      if (object != nullptr) {
        auto& facet = object->facets[record.facetId];
        facet.lastFrame = 0;
        facet.salt1 = 0;
        facet.salt2 = 0;
        facet.checkpointedSeq = 0;
        facet.txn = 0;
      }
      takeRecord(channel, std::move(record), 0);
      return;
    }
    case FROM_DELETED:
    case FROM_CLOSED: {
      const auto key = in.text();
      if (!in.ok) return;
      const auto split = key.rfind('/');
      if (split == std::string::npos) return;
      const auto objectName = key.substr(0, split);
      const auto facetId = uint32_t(decimal(key.substr(split + 1)));
      if (type == FROM_CLOSED) {
        in.u64();  // when the process closed it, by its monotonic clock
        const auto databaseBytes = in.u64();
        if (openId != 0 && (!in.ok || in.at != message.size())) return;
        auto* object = objectOf(objectName);
        if (object == nullptr) return;
        const auto association = channel.openFacets.find(key);
        if (association == channel.openFacets.end()) return;
        if (association->second.id != openId) return;
        const auto generation = association->second.position;
        channel.openFacets.erase(association);
        channel.closeAsked.erase(openId);
        auto& facet = object->facets[facetId];
        if (generation != std::make_pair(object->openEpoch, object->openGeneration) ||
            facet.processId != channel.processId || facet.openEpoch != generation.first ||
            facet.openGeneration != generation.second) return;
        facet.open = false;
        dirtyCacheSizes.insert(object->objectName);
        // The size as the instance left it: a small object may never checkpoint, and the tenant's storage and the
        // object's own limit are judged on this.
        if (in.ok && databaseBytes) {
          facet.databaseBytes = databaseBytes;
          sized(*object);
        }
        maybeCloseMarker(*object);
        return;
      }
      auto* object = objectOf(objectName);
      if (object == nullptr) return;
      if (openId != 0) {
        const auto prefix = objectName + "/";
        auto sender = channel.openFacets.lower_bound(prefix);
        for (; sender != channel.openFacets.end() && sender->first.starts_with(prefix); ++sender) {
          if (sender->second.id == openId) break;
        }
        if (sender == channel.openFacets.end() || !sender->first.starts_with(prefix)) return;
      }
      LogRecord record;
      record.kind = LogRecordKind::DELETED;
      record.objectName = objectName;
      record.storageId = object->storageId;
      record.facetId = facetId;
      takeRecord(channel, std::move(record), 0);
      return;
    }
    case FROM_ALARM: {
      if (!isCurrentOpen(in.text())) return;  // the key, which the fields below give again
      const auto objectName = in.text();
      const auto facetId = in.u32();
      const auto set = in.u8();
      const auto atUnixMs = in.u64();
      if (!in.ok) return;
      alarmReported(objectName, facetId, set ? atUnixMs : 0);
      return;
    }
    case FROM_ACTOR_RESET: {
      const auto objectName = in.text();
      const auto code = in.text();
      if (!in.ok || in.at != message.size() || openId == 0 ||
          !isCurrentOpen(objectName + "/0")) return;
      const auto* object = objectOf(objectName);
      if (object == nullptr || object->dispatchedChannel != channel.processId || !actorResetHook) return;
      actorResetHook({object->namespaceId, object->objectId, object->tenant, object->dataEpoch,
          object->shardEpoch, object->shard}, code);
      return;
    }
    case FROM_RESIDENCY: {
      auto& entry = residency[channel.processId];
      entry.resident = in.u64();
      entry.limit = in.u64();
      entry.evicted = in.u64();
      entry.woken = in.u64();
      entry.wakes = in.u64();
      entry.wakeUsTotal = in.u64();
      entry.wakeUsMax = in.u64();
      if (!in.ok) return;
      entry.atMs = monotonicTimeMs();
      return;
    }
    case FROM_GAVE_UP: {
      in.u64();  // the request the process gave up on
      const auto key = in.text();
      if (!in.ok || !isCurrentOpen(key)) return;
      const auto split = key.rfind('/');
      if (split == std::string::npos) return;
      const auto objectName = key.substr(0, split);
      auto* object = objectOf(objectName);
      ++confirmFailures;
      log("confirm-timeout", "\"object\":" + quote(objectName) + ",\"processId\":" + quote(channel.processId));
      if (object != nullptr) {
        isolate(key, 0, "the write was not confirmed within durableObjects.confirmTimeoutMs");
        markRepair(*object, "a write of this object was not confirmed");
      }
      return;
    }
    default:
      log("message-unknown", "\"processId\":" + quote(channel.processId) + ",\"type\":" + std::to_string(type));
      return;
  }
}

DurableLog::Object* DurableLog::objectOf(const std::string& objectName) {
  auto found = objects.find(objectName);
  return found == objects.end() ? nullptr : &found->second;
}

bool DurableLog::appendToLogFile(const std::string& encoded) {
  if (logFd < 0 || logAppendFailed) {
    logAppendFailed = true;
    log("log-write-failed", "\"reason\":\"this epoch's append log is unavailable\"");
    return false;
  }
  std::string framed;
  putU32(framed, uint32_t(encoded.size()));
  framed += encoded;
  size_t done = 0;
  while (done < framed.size()) {
    auto written = ::write(logFd, framed.data() + done, framed.size() - done);
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) {
      log("log-write-failed", "\"errno\":" + std::to_string(errno) +
          ",\"partialBytes\":\"" + std::to_string(done) + "\"");
      // A truncated record must remain the last one. Keep the acknowledged
      // prefix readable and refuse further writes until a new node epoch.
      logAppendFailed = true;
      ::close(logFd);
      logFd = -1;
      return false;
    }
    done += size_t(written);
  }
  return true;
}

uint64_t DurableLog::recordLimitBytes() const {
  // The local confirmation queue has no separate byte limit. The local FD uses
  // UInt32 lengths; the fragment cap and the actual peer RPC reader are tighter.
  const auto rawLimit = std::min({uint64_t(capnp::ReaderOptions().traversalLimitInWords * sizeof(capnp::word)),
      uint64_t(RECORD_BYTES_LIMIT), settings.config.peerBufferBytes, uint64_t(UINT32_MAX)});
  return rawLimit > TAIL_RPC_HEADROOM_BYTES ? rawLimit - TAIL_RPC_HEADROOM_BYTES : 0;
}

uint64_t DurableLog::sqlWalFrameLimitBytes(const RootStorageRef& storage) const {
  // takeRecord() encodes the canonical object identity, with an empty root facet
  // name. Reserve the WAL generation header that can accompany the first commit.
  LogRecord root;
  root.objectName = storage.namespaceId + "/" + storage.objectId;
  root.storageId = storage.storageId;
  root.header.resize(32);
  const auto overhead = encodedLogRecordSize(root);
  const auto limit = recordLimitBytes();
  KJ_REQUIRE(limit > overhead, "the recoverable record limit cannot fit a SQL WAL record");
  return limit - overhead;
}

// One record of this node's log: it goes to the peers of its object's shard, and the answer to the request that
// carried it waits for them. Nothing here touches the disk or the object store (experiment 25, D.2).
void DurableLog::takeRecord(Channel& channel, LogRecord&& record, uint64_t request) {
  if (stopped || recoveryWriteFailed || (startupAttempted && !startupReady)) {
    if (request) answer(channel, request, ACK_UNAVAILABLE);
    return;
  }
  // The durable process names the root by its namespace/object/data-epoch identity; the history is the
  // object's, whichever copy of its files the frames came from.
  auto* object = objectOf(record.objectName);
  if (object != nullptr && record.storageId != object->storageId) {
    log("record-refused", "\"object\":" + quote(record.objectName) + ",\"reason\":\"storage identity mismatch\"");
    if (request) answer(channel, request, ACK_UNAVAILABLE);
    return;
  }
  if (object != nullptr) {
    record.dataEpoch = object->dataEpoch;
    record.objectName = object->namespaceId + "/" + object->objectId;
  }
  if (object == nullptr) {
    log("record-refused", "\"object\":" + quote(record.objectName) + ",\"reason\":\"this node does not serve it\"");
    if (request) answer(channel, request, ACK_UNAVAILABLE);
    return;
  }
  if (ownerFenced(object->namespaceId, object->shard, object->shardEpoch)) {
    if (request) answer(channel, request, ACK_UNAVAILABLE);
    return;
  }
  // The final WAL commit frame names the size after the transaction. An over-limit database may still delete
  // data or reuse its existing pages; RESET and DROP also reduce storage. Never infer this from the SQL text.
  const auto heldFacet = object->facets.find(record.facetId);
  bool nonGrowing = record.kind == LogRecordKind::RESET || record.kind == LogRecordKind::DELETED;
  if (record.kind == LogRecordKind::FRAMES && record.frames && record.pageSize &&
      record.frameBytes.size() == uint64_t(record.frames) * (24 + uint64_t(record.pageSize)) &&
      heldFacet != object->facets.end()) {
    const auto* last = reinterpret_cast<const unsigned char*>(record.frameBytes.data()) +
        uint64_t(record.frames - 1) * (24 + uint64_t(record.pageSize));
    const auto pages = beU32(last + 4);
    nonGrowing = pages != 0 && uint64_t(pages) * record.pageSize <= heldFacet->second.databaseBytes;
  }
  if (object->overLimit && !nonGrowing) {
    ++overLimitAnswers;
    if (request) answer(channel, request, ACK_TOO_LARGE);
    markRepair(*object, "a growing write exceeded the object size limit");
    return;
  }
  // The tenant's durableObjectBytes quota: only the write is refused, and only while the tenant is over it. What the
  // tenant holds is what the nodes published of their own objects (every hour), so this is approximate by design --
  // it bounds the storage, it does not account for it.
  if (hooks.tenantQuotaBytes && !object->tenant.empty()) {
    const auto quota = hooks.tenantQuotaBytes(object->tenant);
    if (quota && tenantBytes(object->tenant) > quota && !nonGrowing) {
      ++quotaRefusals;
      log("quota-exceeded", "\"tenant\":" + quote(object->tenant) + ",\"bytes\":\"" +
              std::to_string(tenantBytes(object->tenant)) + "\",\"quota\":\"" + std::to_string(quota) + "\"");
      if (request) answer(channel, request, ACK_QUOTA);
      markRepair(*object, "a growing write exceeded the tenant storage quota");
      return;
    }
  }
  if (object->state != ObjectState::LOCAL) {
    log("record-refused", "\"object\":" + quote(record.objectName) + ",\"reason\":" + quote(object->repairReason.empty()
            ? std::string("the object is being rebuilt") : object->repairReason));
    if (request) answer(channel, request, ACK_UNAVAILABLE);
    return;
  }
  // A record of an epoch whose lease is gone is refused before it enters the log (one that waits for a heartbeat
  // in flight is taken; its confirmation waits). The same reading times the confirmation.
  const auto takenAtMs = leaseClockMs();
  if (const auto lease = leaseOf(takenAtMs);
      lease.verdict == LeaseTracker::Verdict::EXPIRED || lease.verdict == LeaseTracker::Verdict::NOT_HELD) {
    ++leaseRefusals;
    if (request) answer(channel, request, ACK_UNAVAILABLE);
    loseLease(lease);
    return;
  }
  record.nodeSeq = nextSeq + 1;
  record.nodeEpoch = nodeEpoch;
  record.shard = object->shard;
  record.shardEpoch = object->shardEpoch;
  record.txn = heldFacet == object->facets.end() ? 1 : heldFacet->second.txn + 1;
  const auto encoded = encodeLogRecord(record);
  const auto limit = recordLimitBytes();
  if (encoded.size() > limit) {
    ++recordLimitRefusals;
    const std::string reason = "the encoded WAL record exceeds the recoverable record limit";
    log("record-limit-refused", "\"object\":" + quote(object->objectName) + ",\"bytes\":\"" +
        std::to_string(encoded.size()) + "\",\"limitBytes\":\"" + std::to_string(limit) +
        "\",\"code\":\"STORAGE_UNAVAILABLE\"");
    if (request) answer(channel, request, ACK_UNAVAILABLE);
    // It never entered the log or any peer. Isolate any other pending frames of
    // this facet, then discard the refused local commit via the existing repair.
    isolate(record.shipperKey(), 0, reason);
    markRepair(*object, reason);
    return;
  }
  if (!appendToLogFile(encoded)) {
    ++confirmFailures;
    if (request) answer(channel, request, ACK_UNAVAILABLE);
    markRepair(*object, "the node's append log could not accept this write");
    return;
  }
  ++object->snapshotVersion;
  auto& facet = object->facets[record.facetId];
  facet.facetId = record.facetId;
  if (!record.facetName.empty()) {
    facet.parentFacetId = record.parentFacetId;
    facet.facetName = record.facetName;
  }
  facet.open = true;
  dirtyCacheSizes.insert(object->objectName);
  scheduleIdleCheckpoint(*object);
  MemoryWatermark::noteDaemonAppend();
  ++nextSeq;
  auto& localEpoch = predecessorLogs[nodeEpoch];
  localEpoch.maxSeq = std::max(localEpoch.maxSeq, record.nodeSeq);
  localEpoch.objects.insert(object->objectName);
  localEpoch.objectEpochs[object->objectName] = std::max(localEpoch.objectEpochs[object->objectName], record.shardEpoch);
  localEpoch.shards.emplace(object->namespaceId, object->shard);
  if (record.kind == LogRecordKind::FRAMES) {
    facet.txn += record.commits;
    facet.lastFrame = record.firstFrame + record.frames - 1;
    facet.salt1 = record.salt1;
    facet.salt2 = record.salt2;
    facet.generation = record.generation;
    facet.loggedNodeEpoch = nodeEpoch;facet.loggedSeq = record.nodeSeq;
    facet.loggedGeneration = record.generation;facet.loggedSalt1 = record.salt1;facet.loggedSalt2 = record.salt2;
    facet.loggedFrame = facet.lastFrame;facet.loggedPageSize = record.pageSize;
    framesTaken += record.frames;
  }
  facet.lastSeq = record.nodeSeq;
  facet.deleted = record.kind == LogRecordKind::DELETED;
  if (facet.deleted && facetDeletedHook) {
    facetDeletedHook({object->namespaceId, object->objectId, object->tenant, object->dataEpoch,
        object->shardEpoch, object->shard}, record.facetId);
  }
  ++recordsTaken;
  bytesTaken += encoded.size();
  recordBytes += encoded.size();
  object->sinceSnapshotBytes += encoded.size();
  records[record.nodeSeq] = encoded;
  // The frames live in `records` alone: what is kept beside them is the record without its bytes, for the segments'
  // index and for the answers (a rebuild decodes the ones it needs).
  {
    // Drop payload capacity as well as size before retaining metadata.
    std::string().swap(record.frameBytes);
    std::string().swap(record.header);
    rememberParsedRecord(record);
  }
  InFlight flight;
  flight.nodeSeq = record.nodeSeq;
  flight.request = request;
  flight.processId = channel.processId;
  flight.objectKey = record.key();
  flight.shipperKey = record.shipperKey();
  flight.sentAtMs = takenAtMs;
  flight.bytes = encoded.size();
  flight.endsAtFrame = record.kind == LogRecordKind::FRAMES ? record.firstFrame + record.frames - 1 : 0;
  flight.generation = record.generation;
  flight.peers = peersOf(object->namespaceId, object->shard);
  inFlight[record.nodeSeq] = flight;
  drivePeers();
  for (const auto& peer : flight.peers) {
    auto found = peers.find(peer);
    // A newly opened session takes the entire backlog. An already usable one takes only this record.
    if (found != peers.end() && !found->second.connecting && found->second.sink != kj::none &&
        currentPeer(peer, found->second.session) != nullptr && found->second.sentSeq < record.nodeSeq) {
      sendOne(found->second, record.nodeSeq, encoded);
    }
  }
  // checkConfirmations() flushes and checks the local log when this record has no selected peers.
  checkConfirmations();
  if (recordBytes >= settings.config.segmentBytes) tasks.add(sealNow("bytes"));
}

uint32_t DurableLog::peerCount() const {
  uint32_t live = 0;
  for (const auto& [id, member] : membership.members) {
    if (id == settings.nodeId || member.state == "draining" || member.durableProcesses == 0) continue;
    ++live;
  }
  return std::min(settings.config.peers, live);
}

// The peers of one shard: the first entries after it on the member ring -- the same order on every node, so a new
// owner knows whom to ask for the tail of the previous owner's log.
std::vector<std::string> DurableLog::peersOf(const std::string& namespaceId, uint32_t shard) const {
  std::vector<std::string> ids;
  for (const auto& [id, member] : membership.members) {
    if (id == settings.nodeId || member.state == "draining" || member.durableProcesses == 0) continue;
    ids.push_back(id);
  }
  if (ids.empty()) return {};
  auto order = rendezvousOrder(namespaceId + "/" + std::to_string(shard), ids);
  const auto want = std::min(size_t(settings.config.peers), order.size());
  order.resize(want);
  return order;
}

void DurableLog::sendOne(PeerStream& peer, uint64_t sequence, const std::string& encoded) {
  // One record goes in pieces of at most APPEND_CHUNK_BYTES, in order on the same stream; the peer takes it whole at
  // the piece that is not partial.
  const auto framed = frameRecords({encoded});
  size_t at = 0;
  while (framed.size() - at > APPEND_CHUNK_BYTES) {
    peer.queue.push_back(Queued{sequence, framed.substr(at, APPEND_CHUNK_BYTES), true});
    peer.queuedBytes += APPEND_CHUNK_BYTES;
    at += APPEND_CHUNK_BYTES;
  }
  peer.queue.push_back(Queued{sequence, framed.substr(at), false});
  peer.queuedBytes += framed.size() - at;
  pumpPeer(peer);
}

// A session is tied to both ends' identities. Membership can change between a send and its continuation,
// before the background loop observes it, so every continuation checks the live view as well as its token.
std::string DurableLog::peerIdentity(const std::string& nodeId) const {
  auto found = membership.members.find(nodeId);
  return found == membership.members.end() ? std::string() : found->second.address + "#" + found->second.epoch;
}

DurableLog::PeerStream* DurableLog::currentPeer(const std::string& nodeId, uint64_t session) {
  auto found = peers.find(nodeId);
  if (stopped || found == peers.end()) return nullptr;
  auto& peer = found->second;
  return peer.session == session && peer.ownerEpoch == nodeEpoch && peer.identity == peerIdentity(nodeId)
      ? &peer : nullptr;
}

void DurableLog::retirePeer(PeerStream& peer) {
  peer.session = ++nextPeerSession;
  peer.sink = kj::none;
  peer.connecting = false;
  peer.broken = true;
  peer.queue.clear();
  peer.queuedBytes = peer.inFlightBytes = 0;
  peer.firstSeq = peer.ackedSeq = peer.sentSeq = 0;
  peer.refusedSeqs.clear();
  peer.fullRetries.clear();
}

// One scheduler per peer, independent of business writes. A confirmed but unsealed tail also needs replication
// after a peer restarts. Neither isolated records nor a fully sealed prefix keep retrying an idle connection.
void DurableLog::drivePeers() {
  if (stopped || !nodeEpoch) return;
  for (const auto& [sequence, flight] : inFlight) {
    for (const auto& id : flight.peers) peers[id].nodeId = id;
  }
  const bool backlog = std::any_of(records.begin(), records.end(), [this](const auto& record) {
    return !isolated.contains(record.first);
  });
  const auto now = monotonicTimeMs();
  for (auto& [id, peer] : peers) {
    const auto identity = peerIdentity(id);
    if (peer.ownerEpoch != nodeEpoch || peer.identity != identity) {
      retirePeer(peer);
      peer.identity = identity;
      peer.ownerEpoch = nodeEpoch;
    }
    if (!backlog || identity.empty() || peer.connecting || (!peer.broken && peer.sink != kj::none)) continue;
    if (peer.attemptedAtMs != 0 && now - peer.attemptedAtMs < PEER_RETRY_MS) continue;
    tasks.add(openPeer(id));
  }
}

kj::Promise<void> DurableLog::peerLoop() {
  while (!stopped) {
    if (!cutChannelForTest.empty()) {
      std::error_code error;
      const auto size = std::filesystem::file_size(cutChannelForTest, error);
      if (!error && size > 0 && size <= 128) {
        std::string processId;
        KJ_IF_SOME(readError, kj::runCatchingExceptions([&] { processId = readFile(cutChannelForTest); })) {
          log("channel-test-trigger-read-failed", "\"reason\":" + quote(readError.getDescription().cStr()));
        }
        auto found = channels.find(processId);
        if (found != channels.end() && found->second.threadedGeneration != 0 && found->second.open) {
          auto& channel = found->second;
          channel.reader = nullptr;
          channelEnded(channel, "fixture cut the durable channel");
          channel.writeChain = nullptr;
          channel.stream = nullptr;
          log("channel-cut-for-test", "\"processId\":" + quote(processId) + ",\"pid\":" +
              std::to_string(channel.pid));
        }
      }
    }
    drivePeers();
    co_await timer.afterDelay(100 * kj::MILLISECONDS);
  }
}

// Sends what the stream's window allows, and again as the answers come back.
void DurableLog::pumpPeer(PeerStream& peer) {
  while (!peer.queue.empty() && peer.inFlightBytes < APPEND_WINDOW_BYTES) {
    if (peer.broken || peer.connecting || peer.sink == kj::none ||
        currentPeer(peer.nodeId, peer.session) == nullptr) return;
    auto item = std::move(peer.queue.front());
    peer.queue.pop_front();
    const auto size = item.bytes.size();
    const auto sequence = item.sequence;
    const bool partial = item.partial;
    peer.queuedBytes -= std::min(peer.queuedBytes, uint64_t(size));
    if (isolated.contains(item.sequence)) continue;
    peer.inFlightBytes += size;
    ++peer.appends;
    peer.sentSeq = std::max(peer.sentSeq, item.sequence);
    const auto nodeId = peer.nodeId;
    const auto session = peer.session;
    auto& sink = KJ_ASSERT_NONNULL(peer.sink);
    tasks.add(sink->value->append(std::move(item.bytes), item.partial)
                  .then([this, nodeId, session, size, sequence, partial](PeerSink::Accepted accepted) {
      auto* current = currentPeer(nodeId, session);
      if (current == nullptr) return;
      current->inFlightBytes -= std::min(current->inFlightBytes, uint64_t(size));
      if (!accepted.refusal.empty()) {
        ++refusals;
        ++current->refusals;
        current->lastRefusal = accepted.refusal;
        if (accepted.refusal.find("fenced") != std::string::npos) ++fencedRecords;
        if (accepted.refusal.starts_with("fenced:")) checkFencedShard(sequence);
        // A record the peer refused, for any reason, never counts as held there (checkConfirmations) until the
        // peer takes it; a full buffer is tried again on this session after a growing wait.
        current->refusedSeqs.insert(sequence);
        if (accepted.refusal.starts_with("full") && inFlight.contains(sequence) && !isolated.contains(sequence)) {
          auto& retry = current->fullRetries[sequence];
          if (!retry.pending && retry.attempts < FULL_RETRIES) {
            retry.pending = true;
            // Spread out: the records refused together are not all sent again at the same moment.
            const auto step = uint64_t(FULL_RETRY_FIRST_MS) << retry.attempts++;
            const auto waitMs = step + randomBelow(step / 2 + 1);
            tasks.add(timer.afterDelay(waitMs * kj::MILLISECONDS).then([this, nodeId, session, sequence]() {
              retryRefused(nodeId, session, sequence);
            }));
          }
        }
        log("peer-refused", "\"peer\":" + quote(nodeId) + ",\"reason\":" + quote(accepted.refusal) +
            ",\"seq\":\"" + std::to_string(sequence) + "\"");
        pumpPeer(*current);
        return;
      }
      current->lastRefusal.clear();
      current->lastAckMs = monotonicTimeMs();
      // An answer accounts for its own record and those before it -- not for one sent after it whose own answer,
      // possibly a refusal, has not come back yet (the peer's highest sequence can come from an earlier stream). A
      // piece that is not a record's last accounts only for the records before it: the record is held once its last
      // piece is taken.
      onPeerAck(nodeId, std::min(accepted.sequence, partial ? sequence - 1 : sequence));
      if (!partial && !current->refusedSeqs.empty() && current->refusedSeqs.erase(sequence)) {
        current->fullRetries.erase(sequence);
        checkConfirmations();
      }
      pumpPeer(*current);
    }).catch_([this, nodeId, session](kj::Exception&& error) {
      auto* current = currentPeer(nodeId, session);
      if (current == nullptr) return;
      retirePeer(*current);
      ++peerSwitches;
      log("peer-lost", "\"peer\":" + quote(nodeId) + ",\"session\":\"" + std::to_string(session) +
              "\",\"error\":" + quote(std::string(error.getDescription().cStr())));
    }).attach(sink.addRef()));
  }
}

// A peer refused a record as fenced: it knows a newer epoch of the record's shard. That is believed once the object
// store shows the next epoch after the record's (an epoch object is created only by the node that takes the shard),
// and then this node has lost the shard: raising its own floor to that epoch (fenceOwner) refuses the shard's
// pending and later writes at once, closes its objects here and drops it from the shards this node holds, so that
// requests go to the newer owner -- instead of each write waiting for confirmTimeoutMs. Only the fence does this; a
// full buffer or any other refusal does not. The read is shared with the peer floor's (raiseFloor).
void DurableLog::checkFencedShard(uint64_t sequence) {
  const auto record = parsed.find(sequence);
  if (record == parsed.end() || !record->second.shardEpoch) return;
  const auto& name = record->second.objectName;
  const auto namespaceId = name.substr(0, name.find('/'));
  const auto shard = record->second.shard;
  const auto epoch = record->second.shardEpoch;
  if (ownerFenced(namespaceId, shard, epoch)) return;
  ++fencedShardChecks;
  // Counted and logged when the fence comes, whichever read raises it (fenceOwner).
  auto& watched = lostShardChecks[{namespaceId, shard}];
  watched = std::max(watched, epoch);
  // This node already knows a newer epoch (its shards' view, read from the store, can learn it first): no read. The
  // fence is raised on the next turn, not inside the answer's own callback (it replaces the peer sessions).
  if (knownEpoch(namespaceId, shard) > epoch) {
    tasks.add(kj::evalLater([this, namespaceId, shard, epoch]() {
      if (!ownerFenced(namespaceId, shard, epoch))
        fenceOwner(namespaceId, shard, epoch, "a peer refused this node's append as fenced", true);
    }));
    return;
  }
  tasks.add(raiseFloor(namespaceId, shard, epoch + 1, "a peer refused this node's append as fenced"));
}

// A record the peer refused because its buffer was full, sent again on the same session while it still waits.
void DurableLog::retryRefused(const std::string& nodeId, uint64_t session, uint64_t sequence) {
  auto* current = currentPeer(nodeId, session);
  if (current == nullptr) return;
  if (auto retry = current->fullRetries.find(sequence); retry != current->fullRetries.end()) retry->second.pending = false;
  const auto record = records.find(sequence);
  if (!current->refusedSeqs.contains(sequence) || isolated.contains(sequence) || !inFlight.contains(sequence) ||
      record == records.end()) return;
  ++fullRetriesSent;
  sendOne(*current, sequence, record->second);
}

// The open floor is captured before awaiting the connection. If sealing moves it meanwhile, discard this
// attempt and reopen at the new floor; assigning a newer floor to the old request would misdescribe its backlog.
kj::Promise<void> DurableLog::openPeer(std::string nodeId) {
  uint64_t session;
  const auto firstSeq = sealedSeq + 1;
  const auto ownerEpoch = nodeEpoch;
  {
    auto& peer = peers[nodeId];
    if (stopped || peer.connecting) co_return;
    retirePeer(peer);
    peer.nodeId = nodeId;
    peer.ownerEpoch = ownerEpoch;
    peer.identity = peerIdentity(nodeId);
    peer.connecting = true;
    peer.broken = false;
    peer.attemptedAtMs = peer.openedAtMs = monotonicTimeMs();
    session = peer.session;
  }
  try {
    auto sink = co_await timer.timeoutAfter(PEER_RETRY_MS * kj::MILLISECONDS,
        hooks.openStream(nodeId, ownerEpoch, firstSeq));
    if (currentPeer(nodeId, session) == nullptr) co_return;
    // Isolation can advance during DROP as well. No append is dispatched until the current exclusions are held.
    std::set<uint64_t> dropped;
    while (dropped != isolated) {
      dropped = isolated;
      co_await timer.timeoutAfter(PEER_RETRY_MS * kj::MILLISECONDS,
          sink->drop(std::vector<uint64_t>(dropped.begin(), dropped.end())));
      if (currentPeer(nodeId, session) == nullptr) co_return;
    }
    auto* entry = currentPeer(nodeId, session);
    if (entry == nullptr) co_return;
    if (firstSeq != sealedSeq + 1) {
      retirePeer(*entry);
      co_return;
    }
    entry->sink = kj::rc<PeerStream::Sink>(kj::mv(sink));
    entry->firstSeq = firstSeq;
    entry->connecting = false;
    log("peer-opened", "\"peer\":" + quote(nodeId) + ",\"session\":\"" + std::to_string(session) +
            "\",\"receiver\":" + quote(entry->identity) + ",\"firstSeq\":\"" + std::to_string(firstSeq) + "\"");
    for (const auto& [sequence, encoded] : records) {
      if (sequence < firstSeq || isolated.contains(sequence)) continue;
      // Nor the records of a shard this node has given up (fenced): its newer owner has their tail already.
      if (!ownerFenceThrough.empty()) {
        if (const auto record = parsed.find(sequence); record != parsed.end() &&
            ownerFenced(record->second.objectName.substr(0, record->second.objectName.find('/')), record->second.shard,
                record->second.shardEpoch)) continue;
      }
      sendOne(*entry, sequence, encoded);
    }
  } catch (const kj::Exception& error) {
    auto* entry = currentPeer(nodeId, session);
    if (entry == nullptr) co_return;
    retirePeer(*entry);
    entry->lastRefusal = std::string(error.getDescription().cStr());
    log("peer-open-failed", "\"peer\":" + quote(nodeId) + ",\"session\":\"" + std::to_string(session) +
            "\",\"error\":" + quote(entry->lastRefusal));
  }
}

void DurableLog::onPeerAck(const std::string& nodeId, uint64_t accepted) {
  auto found = peers.find(nodeId);
  if (found == peers.end()) return;
  if (accepted <= found->second.ackedSeq) return;
  found->second.ackedSeq = accepted;
  checkConfirmations();
}

// A record is confirmed once K peers of its shard hold it and every earlier record of the same database is
// confirmed: the answers of one object are released in the order its transactions were committed.
void DurableLog::checkConfirmations() {
  if (stopped || recoveryWriteFailed || logAppendFailed || (startupAttempted && !startupReady)) return;
  std::set<std::string> fencedObjects;
  for (const auto& [sequence, flight] : inFlight) {
    auto record = parsed.find(sequence);
    if (record != parsed.end() && ownerFenced(
        record->second.objectName.substr(0, record->second.objectName.find('/')),
        record->second.shard, record->second.shardEpoch))
      fencedObjects.insert(hostNameOf(record->second));
  }
  for (const auto& name : fencedObjects) {
    if (auto* object = objectOf(name)) isolateObjectPending(*object, "the owner epoch was fenced");
  }
  if (recoveryWriteFailed) return;
  std::set<std::string> blocked;
  const auto configured = peerCount();
  std::vector<uint64_t> done;
  std::vector<std::pair<std::string, uint64_t>> failed;
  bool flushed = false;
  bool flushOk = false;
  for (auto& [sequence, flight] : inFlight) {
    if (isolated.contains(sequence)) continue;
    if (blocked.contains(flight.objectKey)) continue;
    uint32_t held = 0, reachable = 0;
    for (const auto& peer : flight.peers) {
      auto found = peers.find(peer);
      if (found == peers.end()) continue;
      if (currentPeer(peer, found->second.session) == nullptr) continue;
      if (found->second.sink != kj::none && !found->second.broken) ++reachable;
      if (found->second.ackedSeq >= sequence &&
          (found->second.refusedSeqs.empty() || !found->second.refusedSeqs.contains(sequence))) ++held;
    }
    // A nonempty selected peer list waits when none is reachable. Only an empty list uses the local disk.
    const auto want = std::min(configured, reachable);
    if (held < want || (reachable == 0 && !flight.peers.empty())) {
      blocked.insert(flight.objectKey);
      continue;
    }
    if (want == 0 && !flushed) {
      flushed = true;
#ifdef __APPLE__
      const auto result = ::fsync(logFd);
#else
      const auto result = ::fdatasync(logFd);
#endif
      if (!slowFlushForTest.empty()) slowFlushOnceForTest();
      flushOk = result == 0;
      if (!flushOk) {
        logAppendFailed = true;
        if (logFd >= 0) { ::close(logFd); logFd = -1; }
        log("log-flush-failed", "\"errno\":" + std::to_string(errno) +
            ",\"reason\":\"this epoch can never append or confirm again\"");
      }
      if (configured > 0 && !noPeerWarned) {
        noPeerWarned = true;
        log("no-peer-reachable", "\"warning\":\"no peer of a shard can be reached: its writes are answered once this "
            "machine's own disk holds them, and the peers take over again as soon as one answers\"");
      }
    }
    if (want == 0 && !flushOk) {
      failed.emplace_back(flight.shipperKey, sequence);
      blocked.insert(flight.objectKey);
      continue;
    }
    done.push_back(sequence);
  }
  if (logAppendFailed) {
    std::set<std::string> names;
    for (const auto& [sequence, flight] : inFlight)
      if (auto record = parsed.find(sequence); record != parsed.end()) names.insert(hostNameOf(record->second));
    for (const auto& name : names)
      if (auto* object = objectOf(name)) markRepair(*object, "the local log could not be flushed");
    return;
  }
  for (const auto& [key, sequence] : failed) {
    ++confirmFailures;
    isolate(key, sequence, "the local log could not be flushed");
    auto* object = objectOf(key.substr(0, key.rfind('/')));
    if (object != nullptr) markRepair(*object, "the local log could not be flushed");
  }
  // The lease, judged by a reading of its clock taken just before each answer -- after the local flush above, with
  // nothing that waits between it and the answer. A peer's acknowledgement never overrides it. Waiting for a heartbeat
  // in flight leaves the records unconfirmed until it settles or its end passes; a lost lease refuses them all.
  LeaseTracker::Judgment lease{LeaseTracker::Verdict::HELD, 0, ""};
  for (auto sequence : done) {
    auto found = inFlight.find(sequence);
    if (found == inFlight.end()) continue;
    const auto now = leaseClockMs();
    lease = leaseOf(now);
    if (lease.verdict != LeaseTracker::Verdict::HELD) break;
    const auto& flight = found->second;
    const auto took = now - flight.sentAtMs;
    ++confirmations;
    confirmMsTotal += took;
    if (confirmSamples.size() < CONFIRM_SAMPLES) confirmSamples.push_back(took);
    if (flight.request) {
      auto channel = channels.find(flight.processId);
      if (channel != channels.end()) answer(channel->second, flight.request, ACK_OK);
    }
    confirmed.insert(sequence);
    inFlight.erase(found);
  }
  if (lease.verdict == LeaseTracker::Verdict::WAIT) {
    ++leaseWaits;
    wakeForLease(lease.untilMs);
  } else if (lease.verdict != LeaseTracker::Verdict::HELD) {
    loseLease(lease);
  }
}

LeaseTracker::Judgment DurableLog::leaseOf(double nowMs) {
  if (!leaseCheck) return {LeaseTracker::Verdict::HELD, 0, ""};
  return leaseCheck(nodeEpoch, nowMs);
}

// Every record still waiting is refused (ACK_UNAVAILABLE, the write's STORAGE_UNAVAILABLE) and excluded the way any
// unconfirmed write is; confirmed records and the peers' valid tails stay. Then, once, the node stops holding its
// shards -- only when the expired epoch is the membership's current one: an older epoch's shards were given up when it
// expired, and the shards a newer epoch took before this log followed it (setNodeEpoch) are that epoch's. A current
// epoch not held yet (NOT_HELD) gives up nothing here; the membership's expiry check decides it.
void DurableLog::loseLease(const LeaseTracker::Judgment& judgment) {
  const bool first = leaseLostEpoch != nodeEpoch;
  if (judgment.verdict == LeaseTracker::Verdict::EXPIRED) leaseLostEpoch = nodeEpoch;
  std::set<std::string> names;
  for (const auto& [sequence, flight] : inFlight) {
    if (isolated.contains(sequence)) continue;
    ++leaseRefusals;
    if (auto record = parsed.find(sequence); record != parsed.end()) names.insert(hostNameOf(record->second));
  }
  if (first || !names.empty()) {
    if (first && judgment.verdict == LeaseTracker::Verdict::EXPIRED) ++leaseLosses;
    log("lease-refused", "\"objects\":" + std::to_string(names.size()) + ",\"verdict\":\"" +
        (judgment.verdict == LeaseTracker::Verdict::EXPIRED ? "expired" : "not-held") + "\",\"reason\":" +
        quote(judgment.reason));
  }
  for (const auto& name : names)
    if (auto* object = objectOf(name)) isolateObjectPending(*object, "the node's membership lease ended before confirmation");
  if (first && judgment.verdict == LeaseTracker::Verdict::EXPIRED && std::to_string(nodeEpoch) == membership.epoch &&
      leaseLostHook)
    leaseLostHook(judgment.reason);
}

// A WAIT is judged again when a heartbeat settles (leaseSettled) and at the earliest end it named.
void DurableLog::wakeForLease(double untilMs) {
  if (leaseWakeAtMs != 0 && leaseWakeAtMs <= untilMs) return;
  leaseWakeAtMs = untilMs;
  const auto generation = ++leaseWakeGeneration;
  const auto delayUs = int64_t(std::ceil(std::max(0.0, untilMs - leaseClockMs()) * 1000)) + 1000;
  tasks.add(timer.afterDelay(delayUs * kj::MICROSECONDS).then([this, generation]() {
    if (generation != leaseWakeGeneration) return;
    leaseWakeAtMs = 0;
    checkConfirmations();
  }));
}

void DurableLog::leaseSettled() {
  if (!inFlight.empty()) checkConfirmations();
}

void DurableLog::slowFlushOnceForTest() {
  const std::filesystem::path trigger(slowFlushForTest);
  std::error_code code;
  if (!std::filesystem::exists(trigger, code)) return;
  std::string text;
  try { text = readFile(trigger); } catch (const kj::Exception&) { return; }
  std::filesystem::remove(trigger, code);
  const auto ms = std::strtoull(text.c_str(), nullptr, 10);
  log("slow-flush-for-test", "\"ms\":" + std::to_string(ms));
  ::usleep(useconds_t(std::min<uint64_t>(ms, 60000) * 1000));
}

bool DurableLog::peerAckGateClosedForTest() const {
  if (peerAckGateForTest.empty()) return false;
  std::error_code error;
  return std::filesystem::exists(peerAckGateForTest, error);
}

kj::Promise<void> DurableLog::waitForPeerAckGateForTest() {
  if (!peerAckGateClosedForTest()) co_return;
  ++heldPeerAcksForTest;
  KJ_DEFER(--heldPeerAcksForTest);
  do {
    co_await timer.afterDelay(10 * kj::MILLISECONDS);
  } while (peerAckGateClosedForTest());
}

// Records this node will never confirm and never put in a segment: the write was answered with a failure, so a
// rebuild of the object must not bring it back. The peers are told to drop them; the manifest names them as well,
// for a peer that was out of reach when they were isolated.
void DurableLog::isolate(const std::string& objectKey, uint64_t fromSeq, const std::string& reason) {
  std::vector<uint64_t> dropped;
  for (const auto& [sequence, record] : parsed) {
    if (record.shipperKey() != objectKey || sequence < fromSeq) continue;
    if (confirmed.contains(sequence) || isolated.contains(sequence)) continue;
    dropped.push_back(sequence);
  }
  isolateRecords(dropped, objectKey, reason);
}

void DurableLog::isolateRecords(const std::vector<uint64_t>& dropped,
    const std::string& objectKey, const std::string& reason) {
  if (dropped.empty()) return;
  std::vector<RecoveryEvent> exclusions;
  for (auto sequence : dropped) {
    auto record = parsed.find(sequence);
    if (record == parsed.end()) continue;
    auto* object = objectOf(hostNameOf(record->second));
    if (object == nullptr || !beginRepair(*object)) continue;
    auto event = recoveryEvent(RecoveryKind::ISOLATED, *object, object->repairGeneration);
    event.shardEpoch = record->second.shardEpoch;event.nodeSeq = sequence;
    exclusions.push_back(std::move(event));
  }
  // Even on failure remove in-flight ACKs below. The sticky epoch gate then
  // prevents file replacement, publication and every subsequent confirmation.
  if (!exclusions.empty()) appendRecovery(exclusions, true);
  for (auto sequence : dropped) {
    if (isolated.insert(sequence).second) ++sealSelectionRevision;
    auto record = parsed.find(sequence);
    if (record != parsed.end()) {
      auto* object = objectOf(hostNameOf(record->second));
      if (object != nullptr) {
        object->isolatedRecords.emplace_back(record->second.shardEpoch, sequence);
        ++object->snapshotVersion;
      }
    }
    auto flight = inFlight.find(sequence);
    if (flight != inFlight.end()) {
      if (flight->second.request) {
        auto channel = channels.find(flight->second.processId);
        if (channel != channels.end()) answer(channel->second, flight->second.request, ACK_UNAVAILABLE);
      }
      inFlight.erase(flight);
    }
  }
  ++isolations;
  for (auto& [id, peer] : peers) {
    if (peer.sink != kj::none) {
      auto& sink = KJ_ASSERT_NONNULL(peer.sink);
      tasks.add(sink->value->drop(dropped).catch_([](kj::Exception&&) {}).attach(sink.addRef()));
    }
    // Invalidate pending ACKs and queued fragments as well. The replacement sends all exclusions before its tail.
    retirePeer(peer);
  }
  log("isolated", "\"object\":" + quote(objectKey) + ",\"records\":" + std::to_string(dropped.size()) + ",\"from\":\"" +
          std::to_string(dropped.front()) + "\",\"reason\":" + quote(reason));
}

void DurableLog::isolateObjectPending(Object& object, const std::string& reason) {
  std::vector<uint64_t> dropped;
  for (const auto& [sequence, flight] : inFlight) {
    auto record = parsed.find(sequence);
    if (record != parsed.end() && hostNameOf(record->second) == object.objectName &&
        !confirmed.contains(sequence) && !isolated.contains(sequence)) dropped.push_back(sequence);
  }
  isolateRecords(dropped, object.objectName, reason);
}

// The object is not to be served until its files have been thrown away and rebuilt from the replicated history
// (experiment 25, D.5): every object of this node's durable processes is closed, which is what shuts the files.
void DurableLog::markRepair(Object& object, const std::string& reason) {
  beginRepair(object);
  if (object.state == ObjectState::REPAIR) return;
  ++object.snapshotVersion;
  object.state = ObjectState::REPAIR;
  isolateObjectPending(object, reason);
  object.repairReason = reason;
  ++repairs;
  ++object.repairs;
  log("repair-needed", "\"object\":" + quote(object.objectName) + ",\"reason\":" + quote(reason));
  // The objects are closed by the rebuild (the next request waits for it), so the write that was not confirmed gets
  // its own answer -- STORAGE_UNAVAILABLE -- before the process that holds the object goes.
}

// ---- Segments, manifests and snapshots: all of it off the path a write waits on ----

kj::Promise<void> DurableLog::run() {
  if (!enabled()) co_await kj::Promise<void>(kj::NEVER_DONE);
  tasks.add(peerLoop());
  tasks.add(sealLoop());
  tasks.add(housekeeping());
  tasks.add(healLoop());
  tasks.add(alarmLoop());
  tasks.add(usageLoop());
  co_await kj::Promise<void>(kj::NEVER_DONE);
}

kj::Promise<void> DurableLog::sealLoop() {
  for (;;) {
    co_await timer.afterDelay(settings.config.segmentIntervalMs * kj::MILLISECONDS);
    try {
      co_await sealNow("interval");
    } catch (const kj::Exception& error) {
      log("seal-failed", "\"error\":" + quote(std::string(error.getDescription().cStr())));
    }
  }
}

// Seals the records this node's peers hold, in sequence order up to the first one still waiting for them, and puts
// them on the object store as one segment of the node's log. Nothing of this is on the path a write waits on.
kj::Promise<void> DurableLog::sealNow(std::string reason) {
  if (!enabled() || sealing || (stopped && reason != "drain") || sealDrainExpired() ||
      recoveryPublishWaiters || !nodeEpoch || recoveryWriteFailed || (startupAttempted && !startupReady)) co_return;
  sealing = true;
  const auto release = kj::defer([this]() { sealing = false; });
  const auto selectionEpoch = nodeEpoch;
  const auto selectionRevision = sealSelectionRevision;
  const auto selectionEnd = records.empty() ? 0 : records.rbegin()->first;
  const auto selectionOk = [&] { return nodeEpoch == selectionEpoch && sealSelectionRevision == selectionRevision &&
      !recoveryPublishWaiters && !recoveryWriteFailed && !sealDrainExpired(); };
  constexpr double SEAL_SLICE_MS = 0.5;
  constexpr size_t SEAL_COPY_SLICE_BYTES = 256 * 1024;
  size_t copied = 0;
  auto selecting = monotonicTimeMs();
  auto selectionSliceFull = [&] { return copied >= SEAL_COPY_SLICE_BYTES ||
      monotonicTimeMs() - selecting >= SEAL_SLICE_MS; };
  std::vector<std::string> encoded;
  std::map<std::pair<std::string, uint32_t>, SegmentIndexEntry> index;
  std::map<std::string, std::pair<uint32_t, uint32_t>> lastFrameOf;  // object key -> (salts' frame, frame)
  std::map<std::string, std::pair<uint32_t, uint32_t>> saltsOf;
  uint64_t first = 0, last = 0, bytes = 0;
  uint64_t wireBytes = segmentEnvelopeBudget(settings.nodeId, reason);
  std::set<std::string> touched;
  // Persist exclusions even when there are no confirmed records to upload, or an earlier record is still waiting.
  // Keep the isolated records until this succeeds, so a failed manifest write is retried on the next seal.
  for (const auto sequence : isolated) {
    {
      r47::Span build(r47::SEAL_BUILD);
      auto found = parsed.find(sequence);
      if (found != parsed.end()) touched.insert(hostNameOf(found->second));
    }
    if (selectionSliceFull()) {
      co_await timer.afterDelay(1 * kj::MICROSECONDS);
      if (!selectionOk()) co_return;
      copied = 0;
      selecting = monotonicTimeMs();
    }
  }
  for (const auto& [sequence, record] : records) {
    if (sequence > selectionEnd) break;
    {
      r47::Span build(r47::SEAL_BUILD);
      if (isolated.contains(sequence)) {
        if (!first) first = sequence;
        last = sequence;
      } else {
        if (!confirmed.contains(sequence)) break;
        auto found = parsed.find(sequence);
        if (found == parsed.end()) break;
        const auto& entry = found->second;
        const auto extra = record.size() + 4 +
            (index.contains({entry.objectName, entry.facetId}) ? 0 : segmentEntryBudget(entry));
        if (!encoded.empty() && (bytes + record.size() > settings.config.segmentBytes ||
            wireBytes + extra > STORE_SEGMENT_BYTES)) break;
        KJ_REQUIRE(wireBytes + extra <= STORE_SEGMENT_BYTES, "a record cannot fit a readable store segment");
        wireBytes += extra;
        if (!first) first = sequence;
        last = sequence;
        bytes += record.size();
        encoded.push_back(record);
        copied += record.size();
        auto& object = index[{entry.objectName, entry.facetId}];
        object.objectName = entry.objectName;
        object.facetId = entry.facetId;
        if (!object.records) object.firstTxn = entry.txn;
        object.lastTxn = entry.txn + (entry.commits ? entry.commits - 1 : 0);
        ++object.records;
        touched.insert(hostNameOf(entry));
        if (entry.kind == LogRecordKind::FRAMES) {
          lastFrameOf[entry.shipperKey()] = {entry.firstFrame + entry.frames - 1, entry.frames};
          saltsOf[entry.shipperKey()] = {entry.salt1, entry.salt2};
        }
        build.finish(record.size());
        if (bytes >= settings.config.segmentBytes) break;
      }
    }
    if (selectionSliceFull()) {
      co_await timer.afterDelay(1 * kj::MICROSECONDS);
      if (!selectionOk()) co_return;
      copied = 0;
      selecting = monotonicTimeMs();
    }
  }
  if (encoded.empty() && touched.empty()) co_return;
  SegmentHeader header;
  header.nodeId = settings.nodeId;
  header.nodeEpoch = nodeEpoch;
  header.segment = segmentNumber + 1;
  header.firstSeq = first;
  header.lastSeq = last;
  header.records = encoded.size();
  header.bytes = bytes;
  header.sealedAtUnixMs = realClockUnixMs();
  header.reason = reason;
  const bool haveRecords = !encoded.empty();
  size_t bodySize = 0;
  if (haveRecords) {
    auto encodeInBackground = backgroundValue([header, index = std::move(index), encoded = std::move(encoded)]() mutable {
      header.objects.reserve(index.size());
      for (auto& [key, entry] : index) header.objects.push_back(std::move(entry));
      auto body = encodeSegment(header, encoded);
      auto digest = objectSha256(body);
      return std::make_pair(std::move(body), std::move(digest));
    });
    auto serialized = co_await kj::mv(encodeInBackground);
    if (!selectionOk()) co_return;
    bodySize = serialized.first.size();
    if (!co_await uploadSegment(header, serialized.first, std::move(serialized.second), true)) co_return;
    if (nodeEpoch != header.nodeEpoch) co_return;
    segmentNumber = header.segment;
    ++segments;
    segmentBytes += bodySize;
    lastSegmentAtMs = monotonicTimeMs();
    if (recoveryPublishWaiters) co_return;
  }
  // Each object's manifest names the segment before anything drops the records: a manifest nobody has written yet
  // leaves the segment pointing at nothing, and a node taking the shard over would not replay it.
  uint32_t written = 0, failed = 0;
  for (const auto& objectName : touched) {
    if (sealDrainExpired()) co_return;
    auto* object = objectOf(objectName);
    if (object == nullptr) continue;
    if (healingManifest(*object)) { ++failed; continue; }
    if (haveRecords) object->segments.push_back(ManifestSegment{settings.nodeId, nodeEpoch, header.segment, first, last,
        object->shardEpoch});
    const bool ok = co_await writeManifest(*object, false).catch_([this](kj::Exception&& error) {
      ++manifestFailures;
      lastManifestError = std::string(error.getDescription().cStr());
      log("manifest-failed", "\"error\":" + quote(lastManifestError));
      return false;
    });
    if (nodeEpoch != header.nodeEpoch) co_return;
    if (ok) ++written;
    else ++failed;
  }
  log("segment", "\"segment\":\"" + std::to_string(header.segment) + "\",\"firstSeq\":\"" + std::to_string(first) +
          "\",\"lastSeq\":\"" + std::to_string(last) + "\",\"records\":" + std::to_string(header.records) +
          ",\"bytes\":\"" + std::to_string(bodySize) + "\",\"objects\":" + std::to_string(touched.size()) +
          ",\"manifests\":" + std::to_string(written) + ",\"manifestsFailed\":" + std::to_string(failed) +
          ",\"reason\":" + quote(reason));
  if (failed || !last || recoveryWriteFailed) {
    // The records stay where they are and the next segment carries them again (a record that ends up in two
    // segments is applied once: a rebuild takes each by its shard epoch and sequence).
    co_return;
  }
  // The records are on the object store and every manifest names them: this node drops them, the peers are told to,
  // and each database may now checkpoint the frames the segment covers.
  r47::Span commit(r47::SEAL_COMMIT);
  for (uint64_t sequence = (first ? first : last); sequence <= last; ++sequence) {
    auto found = records.find(sequence);
    if (found != records.end()) {
      recordBytes -= found->second.size();
      records.erase(found);
    }
    forgetParsedRecord(sequence);
    confirmed.erase(sequence);
    // Keep isolated sequence numbers until nodeEpoch changes, so a reopened stream receives the drops again.
  }
  sealedSeq = last;
  LogRecord marker;
  marker.kind = LogRecordKind::SEAL;
  marker.nodeSeq = last;
  marker.nodeEpoch = nodeEpoch;
  marker.segment = header.segment;
  appendToLogFile(encodeLogRecord(marker));
  for (auto& [id, peer] : peers) {
    if (peer.sink == kj::none) continue;
    auto& sink = KJ_ASSERT_NONNULL(peer.sink);
    tasks.add(sink->value->trim(last).catch_([](kj::Exception&&) {}).attach(sink.addRef()));
  }
  for (const auto& [key, frame] : lastFrameOf) {
    auto salts = saltsOf.find(key);
    if (salts == saltsOf.end()) continue;
    tellSealed(key, salts->second.first, salts->second.second, frame.first);
  }
  commit.finish(lastFrameOf.size());
  auto slice = monotonicTimeMs();
  for (const auto& name : touched) {
    {
      r47::Span position(r47::SEAL_POSITION);
      auto* object = objectOf(name);
      if (object != nullptr) writePosition(*object);
    }
    if (monotonicTimeMs() - slice >= SEAL_SLICE_MS) {
      co_await timer.afterDelay(1 * kj::MICROSECONDS);
      if (nodeEpoch != header.nodeEpoch || recoveryWriteFailed) co_return;
      slice = monotonicTimeMs();
    }
  }
}

// A segment is immutable and created only if absent; a failed upload is tried again with a growing wait, because
// the peers hold the records until it is there (requirement 7).
bool DurableLog::sealDrainExpired() const {
  return sealDrainDeadlineMs && monotonicTimeMs() >= *sealDrainDeadlineMs;
}

kj::Promise<bool> DurableLog::uploadSegment(const SegmentHeader& header, const std::string& body,
    std::string payloadSha256, bool observeDrainDeadline, std::shared_ptr<const std::string> offLoop) {
  KJ_REQUIRE(body.size() <= STORE_SEGMENT_BYTES, "a segment exceeds the store client's read limit");
  const auto key = nodeSegmentKey(settings.nodeId, header.nodeEpoch, header.segment);
  auto defer = [&]() {
    return (header.reason != "earlier-epoch" && nodeEpoch != header.nodeEpoch) ||
        (header.reason != "recovery-tail" && recoveryPublishWaiters > 0);
  };
  auto wait = settings.config.uploadRetryMs;
  for (uint32_t attempt = 1;; ++attempt) {
    if (observeDrainDeadline && sealDrainExpired()) co_return false;
    if (defer()) {
      log("segment-deferred", "\"key\":" + quote(key) + ",\"reason\":\"recovery or a new log epoch has priority\"");
      co_return false;
    }
    std::pair<bool, std::string> outcome;
    if (offLoop) {
      outcome = co_await backgroundValue([config = workerStoreConfig, key, offLoop, payloadSha256]() {
        try {
          auto io = kj::setupAsyncIo();
          ObjectStore threadStore(config, io.provider->getNetwork(), io.provider->getTimer());
          return std::make_pair(threadStore.create(key, *offLoop, payloadSha256).wait(io.waitScope).created, std::string());
        } catch (const kj::Exception& error) {
          return std::make_pair(false, std::string(error.getDescription().cStr()));
        }
      });
    } else {
      outcome = co_await store->create(key, body, payloadSha256)
                    .then([](CreateResult result) { return std::make_pair(result.created, std::string()); })
                    .catch_([](kj::Exception&& error) {
        return std::make_pair(false, std::string(error.getDescription().cStr()));
      });
    }
    if (outcome.second.empty() && !outcome.first) {
      // Existence alone does not prove this upload succeeded: another attempt may have used the same number.
      // Read errors retry the same immutable bytes; a different body must never be named by this manifest.
      auto check = [&body](ObjectResponse response) {
        return std::make_pair(response.status == 200 && response.bytes == body,
            response.status == 200 ? std::string() : std::string("the existing segment could not be read"));
      };
      std::pair<bool, std::string> held;
      if (offLoop) {
        held = co_await backgroundValue([config = workerStoreConfig, key, offLoop]() {
          try {
            auto io = kj::setupAsyncIo();
            ObjectStore threadStore(config, io.provider->getNetwork(), io.provider->getTimer());
            auto response = threadStore.get(key).wait(io.waitScope);
            return std::make_pair(response.status == 200 && response.bytes == *offLoop,
                response.status == 200 ? std::string() : std::string("the existing segment could not be read"));
          } catch (const kj::Exception& error) {
            return std::make_pair(false, std::string(error.getDescription().cStr()));
          }
        });
      } else {
        held = co_await store->get(key).then(check).catch_([](kj::Exception&& error) {
          return std::make_pair(false, std::string(error.getDescription().cStr()));
        });
      }
      outcome.second = std::move(held.second);
      if (outcome.second.empty() && !held.first) {
        ++uploadFailures;
        lastUploadError = "the existing segment has different bytes";
        log("segment-conflict", "\"key\":" + quote(key) + ",\"expectedSha256\":" +
            quote(payloadSha256.empty() ? sha256Hex(body) : payloadSha256));
        failConflict("a different immutable Durable Object log segment already exists at " + key);
      }
    }
    if (outcome.second.empty()) {
      ++uploads;
      lastUploadAtMs = monotonicTimeMs();
      lastUploadError.clear();
      if (!outcome.first) log("segment-exists", "\"key\":" + quote(key) + ",\"attempt\":" + std::to_string(attempt));
      co_return true;
    }
    ++uploadFailures;
    lastUploadError = outcome.second;
    log("upload-failed", "\"key\":" + quote(key) + ",\"attempt\":" + std::to_string(attempt) + ",\"error\":" +
            quote(lastUploadError) + ",\"retryMs\":" + std::to_string(wait));
    if (defer() || (observeDrainDeadline && sealDrainExpired())) co_return false;
    auto retryDelay = double(wait);
    if (observeDrainDeadline && sealDrainDeadlineMs)
      retryDelay = std::min(retryDelay, std::max(0., *sealDrainDeadlineMs - monotonicTimeMs()));
    const auto waited = co_await uploadRetryCanceler.wrap(timer.afterDelay(uint64_t(retryDelay) * kj::MILLISECONDS))
        .then([]() { return true; }).catch_([](kj::Exception&&) { return false; });
    if (!waited) co_return false;
    wait = std::min(wait * 2, settings.config.uploadRetryMaxMs);
  }
}

// The object's next manifest revision: the latest snapshot and, after it, the segments to replay in order. Created
// only if absent, so two writers cannot both be the n-th; a revision whose shard epoch is higher than this node's
// says another node owns the shard now, and this node gives it up rather than writing over its history.
bool DurableLog::healingManifest(const Object& object) const {
  const auto gate = manifestGates.find({object.namespaceId, object.objectId});
  return gate != manifestGates.end() && gate->second.healers;
}

kj::Promise<bool> DurableLog::writeManifest(Object& object, bool withSnapshot,
    std::optional<uint64_t> expectedRevision, const SnapshotImage* image) {
  if (!enabled() || recoveryWriteFailed) co_return false;
  const auto key = std::make_pair(object.namespaceId, object.objectId);
  if (!expectedRevision) {
    auto& gate = manifestGates[key];
    if (gate.healers) co_return false;
    ++gate.writers;
  }
  const auto endWrite = kj::defer([&]() {
    if (expectedRevision) return;
    auto gate = manifestGates.find(key);
    if (--gate->second.writers) return;
    for (auto& waiter : gate->second.drained) if (waiter->isWaiting()) waiter->fulfill();
    gate->second.drained.clear();
    if (!gate->second.healers) manifestGates.erase(gate);
  });
  if (auto state = recoveryStates.find(object.objectName); state != recoveryStates.end()) {
    for (const auto& [position, generation] : state->second.exclusions)
      if (std::find(object.isolatedRecords.begin(), object.isolatedRecords.end(), position) == object.isolatedRecords.end())
        object.isolatedRecords.push_back(position);
  }
  auto validImage = [&]() {
    if (recoveryWriteFailed) return false;
    if (image == nullptr) return true;
    if (object.state != image->state || object.shardEpoch != image->shardEpoch || object.dataEpoch != image->dataEpoch ||
        nodeEpoch != image->nodeEpoch || object.snapshotVersion != image->version) return false;
    for (const auto& [sequence, flight] : inFlight) {
      auto record = parsed.find(sequence);
      if (record != parsed.end() && hostNameOf(record->second) == object.objectName) return false;
    }
    return true;
  };
  for (uint32_t attempt = 1; attempt <= MANIFEST_ATTEMPTS; ++attempt) {
    auto previous = co_await readManifest(object.namespaceId, object.objectId);
    if (!validImage()) co_return false;
    const auto pendingSegments = object.segments;
    ObjectManifest manifest;
    manifest.namespaceId = object.namespaceId;
    manifest.objectId = object.objectId;
    manifest.shard = object.shard;
    manifest.shardEpoch = object.shardEpoch;
    manifest.revision = previous ? previous->revision + 1 : 1;
    if (previous && previous->shardEpoch > object.shardEpoch) {
      ++manifestFences;
      log("manifest-fenced", "\"object\":" + quote(object.objectName) + ",\"ours\":\"" +
              std::to_string(object.shardEpoch) + "\",\"theirs\":\"" + std::to_string(previous->shardEpoch) + "\"");
      object.state = ObjectState::GONE;
      co_await retireFencedTails(object, *previous);
      co_return false;
    }
    // A recovery image may only replace the exact history it replayed. A CAS
    // conflict must restart recovery rather than erase an unseen old-owner seal.
    if (expectedRevision && (previous ? previous->revision : 0) != *expectedRevision) {
      object.recoveryRevisionChanged = true;
      log("recovery-manifest-changed", "\"object\":" + quote(object.objectName) +
          ",\"expected\":\"" + std::to_string(*expectedRevision) + "\",\"actual\":\"" +
          std::to_string(previous ? previous->revision : 0) + "\"");
      co_return false;
    }
    if (previous) {
      manifest.hasSnapshot = previous->hasSnapshot;
      manifest.snapshotSequence = previous->snapshotSequence;
      manifest.snapshotParts = previous->snapshotParts;
      manifest.snapshotBytes = previous->snapshotBytes;
      manifest.snapshotAtUnixMs = previous->snapshotAtUnixMs;
      manifest.snapshotNodeId = previous->snapshotNodeId;
      manifest.snapshotNodeEpoch = previous->snapshotNodeEpoch;
      manifest.snapshotShardEpoch = previous->snapshotShardEpoch;
      manifest.facets = previous->facets;
      manifest.segments = previous->segments;
      manifest.isolated = previous->isolated;
    }
    uint64_t snapshotCoveredTo = 0;
    if (withSnapshot) {
      manifest.hasSnapshot = true;
      manifest.snapshotSequence = image ? image->sequence : object.snapshotSequence;
      manifest.snapshotParts = image ? image->parts : object.snapshotParts;
      manifest.snapshotBytes = image ? image->bytes : object.snapshotBytes;
      manifest.snapshotAtUnixMs = image ? image->atUnixMs : object.snapshotAtUnixMs;
      manifest.snapshotNodeId = settings.nodeId;
      manifest.snapshotNodeEpoch = nodeEpoch;
      manifest.snapshotShardEpoch = object.shardEpoch;
      manifest.facets = image ? image->facets : object.snapshotFacets;
      // Every segment the snapshot covers may go: the file holds those transactions.
      uint64_t covered = 0;
      bool firstFacet = true;
      for (const auto& facet : manifest.facets) {
        covered = firstFacet ? facet.upToSeq : std::min(covered, facet.upToSeq);
        firstFacet = false;
      }
      snapshotCoveredTo = covered;
      // Only the segments of the epoch this snapshot was taken in, and only those it holds every record of.
      std::erase_if(manifest.segments, [&](const ManifestSegment& segment) {
        return segment.shardEpoch < object.shardEpoch ||
            (segment.shardEpoch == object.shardEpoch && segment.lastSeq <= covered);
      });
    }
    for (const auto& segment : object.segments) manifest.segments.push_back(segment);
    if (withSnapshot) std::erase_if(manifest.segments, [&](const ManifestSegment& segment) {
      return segment.shardEpoch < object.shardEpoch ||
          (segment.shardEpoch == object.shardEpoch && segment.lastSeq <= snapshotCoveredTo);
    });
    // Retrying publication must not grow one identical segment reference per
    // second. Coalesce older manifests that already contain duplicates too.
    std::map<std::tuple<std::string, uint64_t, uint64_t>, ManifestSegment> unique;
    for (const auto& segment : manifest.segments) {
      auto [entry, added] = unique.emplace(std::make_tuple(segment.nodeId, segment.nodeEpoch, segment.segment), segment);
      if (!added) {
        entry->second.firstSeq = std::min(entry->second.firstSeq, segment.firstSeq);
        entry->second.lastSeq = std::max(entry->second.lastSeq, segment.lastSeq);
        entry->second.shardEpoch = std::max(entry->second.shardEpoch, segment.shardEpoch);
      }
    }
    manifest.segments.clear();
    for (const auto& [key, segment] : unique) manifest.segments.push_back(segment);
    for (const auto& entry : object.isolatedRecords) {
      if (std::find(manifest.isolated.begin(), manifest.isolated.end(), entry) == manifest.isolated.end())
        manifest.isolated.push_back(entry);
    }
    manifest.writerNodeId = settings.nodeId;
    manifest.writerNodeEpoch = nodeEpoch;
    manifest.instance = settings.instance;
    manifest.nonce = randomHex(8);
    manifest.writtenAtUnixMs = realClockUnixMs();
    manifest.databaseBytes = object.bytes;
    const auto key = objectManifestKey(object.namespaceId, object.objectId, manifest.revision);
    const auto bytes = objectManifestJson(manifest);
    auto result = co_await store->create(key, bytes);
    if (result.created) {
      ++manifestWrites;
      object.manifestRevision = std::max(object.manifestRevision, manifest.revision);
      if (expectedRevision) object.recoveryManifestRevision = manifest.revision;
      std::erase_if(object.segments, [&](const ManifestSegment& segment) {
        return std::any_of(pendingSegments.begin(), pendingSegments.end(), [&](const ManifestSegment& included) {
          return segment.nodeId == included.nodeId && segment.nodeEpoch == included.nodeEpoch && segment.segment == included.segment;
        });
      });
      if (withSnapshot && validImage()) {
        if (image) {
          object.snapshotSequence = image->sequence;object.snapshotParts = image->parts;object.snapshotBytes = image->bytes;
          object.snapshotFacets = image->facets;
        }
        object.sinceSnapshotBytes = 0;
        object.snapshotAtUnixMs = manifest.snapshotAtUnixMs;
      }
      co_return validImage();
    }
    ++manifestConflicts;
    // Another writer created this revision first (a node taking the shard over, or an old owner): read it again.
    co_await timer.afterDelay((20 + attempt * 30) * kj::MILLISECONDS);
  }
  ++manifestFailures;
  lastManifestError = "another writer kept taking the object's next manifest revision";
  log("manifest-conflict", "\"object\":" + quote(object.objectName) + ",\"attempts\":" +
          std::to_string(MANIFEST_ATTEMPTS));
  co_return false;
}

// The object's latest manifest: the revisions are listed and the largest read (the design leaves the `current` hint
// out of this path -- a reader lists, and only when it rebuilds, which is the cheaper of the two).
kj::Promise<std::optional<ObjectManifest>> DurableLog::readManifest(std::string namespaceId,
    std::string objectId) {
  const auto prefix = objectManifestPrefix(namespaceId, objectId);
  auto listing = co_await store->list(prefix + "/");
  const auto revision = latestRevision(listing, prefix);
  if (!revision) co_return std::nullopt;
  auto object = co_await store->get(objectManifestKey(namespaceId, objectId, revision));
  requireStorage(object.status == 200, "the listed Durable Object manifest could not be read");
  auto manifest = parseObjectManifest(object.bytes);
  if (!manifest) {
    log("manifest-unreadable", "\"object\":" + quote(namespaceId + "/" + objectId) + ",\"revision\":\"" +
            std::to_string(revision) + "\"");
    failStorage("the latest Durable Object manifest is invalid");
  }
  co_return manifest;
}

void DurableLog::rememberParsedRecord(const LogRecord& record) {
  parsed[record.nodeSeq] = record;
  unsealedByObject[record.objectName].insert(record.nodeSeq);
}

void DurableLog::forgetParsedRecord(uint64_t sequence) {
  auto metadata = parsed.find(sequence);
  if (metadata == parsed.end()) return;
  auto local = unsealedByObject.find(metadata->second.objectName);
  KJ_DASSERT(local != unsealedByObject.end() && local->second.contains(sequence));
  if (local != unsealedByObject.end()) {
    local->second.erase(sequence);
    if (local->second.empty()) unsealedByObject.erase(local);
  }
  parsed.erase(metadata);
}

void DurableLog::indexEarlierRecord(EarlierPosition position, const LogRecord& record) {
  earlierByObject[record.objectName].insert(position);
}

void DurableLog::unindexEarlierRecord(EarlierPosition position, const LogRecord& record) {
  auto indexed = earlierByObject.find(record.objectName);
  KJ_DASSERT(indexed != earlierByObject.end() && indexed->second.contains(position));
  if (indexed == earlierByObject.end()) return;
  indexed->second.erase(position);
  if (indexed->second.empty()) earlierByObject.erase(indexed);
}

// A newer snapshot is a cut, not a per-record receipt. Report unaccounted
// confirmed bytes distinctly from a proven transfer and keep the old segment.
kj::Promise<void> DurableLog::retireFencedTails(Object& object, const ObjectManifest& manifest) {
  auto& attempts = fencedTailReadAttempts[object.objectName];
  if (attempts.first != manifest.revision) attempts = {manifest.revision, 0};
  const auto wanted = object.namespaceId + "/" + object.objectId;
  using Position = std::pair<uint64_t, uint64_t>;
  struct Held { uint64_t epoch, bytes; std::string digest; bool restored; };
  std::map<Position, Held> held;
  auto take = [&](const LogRecord& record) {
    if (record.objectName != wanted || record.shardEpoch >= manifest.shardEpoch) return;
    auto encoded = encodeLogRecord(record);
    held.emplace(Position{record.shardEpoch, record.nodeSeq},
        Held{record.nodeEpoch, encoded.size(), sha256Hex(encoded), restoredEpochs.contains(record.nodeEpoch)});
  };
  if (auto local = unsealedByObject.find(wanted); local != unsealedByObject.end()) {
    for (auto sequence : local->second) {
      if (!confirmed.contains(sequence) || isolated.contains(sequence)) continue;
      auto entry = records.find(sequence);
      if (entry == records.end()) continue;
      if (auto record = decodeLogRecord(entry->second)) take(*record);
    }
  }
  if (auto old = earlierByObject.find(wanted); old != earlierByObject.end()) {
    for (auto [epoch, sequence] : old->second) take(earlier.at(epoch).at(sequence));
  }
  if (held.empty()) co_return;
  std::set<Position> transferred, excluded;
  for (const auto& position : manifest.isolated) excluded.insert(position);
  if (attempts.second < 5) {
    ++attempts.second;
    try {
      for (const auto& record : co_await readSegments(manifest, wanted)) {
        const Position position{record.shardEpoch, record.nodeSeq};
        auto candidate = held.find(position);
        if (candidate != held.end() && (record.ownerSealed || !excluded.contains(position)) &&
            candidate->second.digest == sha256Hex(encodeLogRecord(record))) transferred.insert(position);
      }
    } catch (const kj::Exception& error) {
      log("fenced-tail-pending", "\"object\":" + quote(object.objectName) + ",\"revision\":\"" +
          std::to_string(manifest.revision) + "\",\"attempt\":" + std::to_string(attempts.second) +
          ",\"error\":" + quote(std::string(error.getDescription().cStr())));
      if (attempts.second < 5) co_return;
    }
  }
  std::set<Position> released;
  struct Summary { uint64_t first = 0, last = 0, records = 0, bytes = 0; std::string digests; };
  std::map<std::tuple<uint32_t, uint64_t, uint64_t>, Summary> summaries;
  for (const auto& [position, record] : held) {
    const bool moved = transferred.contains(position);
    if (!moved && !record.restored && !(manifest.hasSnapshot && manifest.snapshotShardEpoch > position.first)) continue;
    released.insert(position);
    // These records are removed below; a later accounting pass cannot count them twice.
    const uint32_t disposition = moved ? 0 : record.restored ? 2 : 1;
    auto& summary = summaries[{disposition, position.first, record.epoch}];
    if (!summary.first) summary.first = position.second;
    summary.last = position.second;
    ++summary.records;
    summary.bytes += record.bytes;
    summary.digests += record.digest;
    if (moved) ++transferredTailRecords;
    else if (record.restored) { ++restoredUnaccountedRecords; restoredUnaccountedBytes += record.bytes; }
    else { ++unaccountedTailRecords; unaccountedTailBytes += record.bytes; }
  }
  for (const auto& [source, summary] : summaries) {
    log(std::get<0>(source) == 0 ? "confirmed-tail-transferred" : std::get<0>(source) == 2
            ? "restored-tail-unaccounted" : "confirmed-tail-unaccounted",
        "\"object\":" + quote(object.objectName) + ",\"shard\":" + std::to_string(object.shard) +
        ",\"sourceEpoch\":\"" + std::to_string(std::get<1>(source)) + "\",\"sourceNodeEpoch\":\"" +
        std::to_string(std::get<2>(source)) + "\",\"firstSeq\":\"" + std::to_string(summary.first) +
        "\",\"lastSeq\":\"" + std::to_string(summary.last) + "\",\"records\":" +
        std::to_string(summary.records) + ",\"bytes\":\"" + std::to_string(summary.bytes) +
        "\",\"sha256\":" + quote(sha256Hex(summary.digests)) + ",\"revision\":\"" +
        std::to_string(manifest.revision) + "\",\"newOwner\":" + quote(manifest.writerNodeId));
  }
  // readSegments can suspend: select against the live index again, just as
  // the old erase scan selected against the live maps. Copy the keys before
  // erasing the last entry of an object's index. No yield during retirement.
  std::vector<uint64_t> localSequences;
  if (auto local = unsealedByObject.find(wanted); local != unsealedByObject.end())
    localSequences.assign(local->second.begin(), local->second.end());
  for (auto sequence : localSequences) {
    auto entry = records.find(sequence);
    if (entry == records.end()) continue;
    auto record = decodeLogRecord(entry->second);
    // Selection requires confirmed and non-isolated, but erasure is by
    // released position and object alone, including duplicate positions.
    if (!record || record->objectName != wanted || !released.contains({record->shardEpoch, record->nodeSeq})) continue;
    recordBytes -= std::min<uint64_t>(recordBytes, entry->second.size());
    noteEarlierRetired(record->nodeEpoch, *record);
    forgetParsedRecord(sequence);
    confirmed.erase(sequence);
    ++sealSelectionRevision;
    records.erase(entry);
  }
  std::vector<EarlierPosition> precedingSequences;
  if (auto old = earlierByObject.find(wanted); old != earlierByObject.end())
    precedingSequences.assign(old->second.begin(), old->second.end());
  for (auto position : precedingSequences) {
    auto epoch = earlier.find(position.first);
    auto entry = epoch->second.find(position.second);
    const auto& record = entry->second;
    if (record.objectName != wanted || !released.contains({record.shardEpoch, record.nodeSeq})) continue;
    noteEarlierRetired(epoch->first, record);
    unindexEarlierRecord(position, record);
    epoch->second.erase(entry);
    if (epoch->second.empty()) {
      earlierUploads.erase(epoch->first);
      earlier.erase(epoch);
    }
  }
  if (released.size() < held.size())
    log("fenced-tail-pending", "\"object\":" + quote(object.objectName) + ",\"revision\":\"" +
        std::to_string(manifest.revision) + "\",\"attempt\":" + std::to_string(attempts.second) +
        ",\"records\":" + std::to_string(held.size() - released.size()));
}

// A snapshot: after a checkpoint the database file holds every transaction of the generations before it, so the
// file is uploaded in parts and the manifest then names it -- the segments it covers are no longer needed.
kj::Promise<void> DurableLog::snapshotIfDue(std::string objectName) {
  auto* object = objectOf(objectName);
  if (object == nullptr || !enabled() || object->state != ObjectState::LOCAL || object->snapshotting ||
      object->facets.empty()) co_return;
  const auto now = realClockUnixMs();
  const bool bySize = object->sinceSnapshotBytes > settings.config.snapshotBytes;
  const bool byTime = object->snapshotAtUnixMs != 0 && now > object->snapshotAtUnixMs + settings.config.snapshotIntervalMs;
  if (!bySize && !byTime) co_return;
  for (const auto& [sequence, flight] : inFlight) {
    auto record = parsed.find(sequence);
    if (record != parsed.end() && hostNameOf(record->second) == objectName) co_return;
  }
  try {
    co_await forceRecoverySnapshot(objectName, false, true);
  } catch (const kj::Exception& error) {
    ++snapshotFailures;
    log("snapshot-abandoned", "\"object\":" + quote(objectName) + ",\"error\":" +
        quote(std::string(error.getDescription().cStr())));
  }
}

kj::Promise<void> DurableLog::uploadSnapshotPart(std::string namespaceId, std::string objectId, uint64_t sequence,
    uint32_t facetId, uint64_t part, std::string bytes) {
  const auto key = objectSnapshotPartKey(namespaceId, objectId, sequence, facetId, part);
  auto wait = settings.config.uploadRetryMs;
  for (uint32_t attempt = 1;; ++attempt) {
    auto failure = co_await store->create(key, bytes).then([](CreateResult) { return std::string(); })
                       .catch_([](kj::Exception&& error) { return std::string(error.getDescription().cStr()); });
    if (failure.empty()) {
      ++uploads;
      co_return;
    }
    ++uploadFailures;
    ++snapshotFailures;
    lastUploadError = failure;
    KJ_REQUIRE(attempt < 8, "a part of a Durable Object's snapshot could not be uploaded", key.c_str(),
        failure.c_str());
    co_await timer.afterDelay(wait * kj::MILLISECONDS);
    wait = std::min(wait * 2, settings.config.uploadRetryMaxMs);
  }
}

// Once a second: the objects nothing has asked for in a while are checkpointed (so their WAL does not stay large),
// the files of cold objects go when this machine's share is full, and the records an earlier epoch of this node
// left are sealed under that epoch, so a node that takes the shards over finds them.
void DurableLog::scheduleIdleCheckpoint(Object& object) {
  if (object.lastAccessMs == 0) return;
  auto due = std::max(object.lastAccessMs, object.lastCheckpointAskMs) + settings.config.idleCheckpointMs;
  // An interval of zero still means once per housekeeping pass, never twice in the same one.
  if (settings.config.idleCheckpointMs == 0 && object.lastCheckpointAskMs > 0)
    due = std::nextafter(due, std::numeric_limits<double>::infinity());
  if (due == object.checkpointDueMs) return;
  if (object.checkpointDueMs != 0) idleCheckpoints.erase({object.checkpointDueMs, object.objectName});
  idleCheckpoints.emplace(due, object.objectName);
  object.checkpointDueMs = due;
}

kj::Promise<void> DurableLog::checkpointIdleObjects(double now) {
  size_t inspected = 0;
  auto began = monotonicTimeMs();
  while (!idleCheckpoints.empty() && idleCheckpoints.begin()->first <= now) {
    {
      r47::Span scan(r47::CHECKPOINT_SCAN);
      auto due = idleCheckpoints.extract(idleCheckpoints.begin());
      if (auto* object = objectOf(due.value().second)) {
        object->checkpointDueMs = 0;
        if (object->state == ObjectState::LOCAL) {
          object->lastCheckpointAskMs = now;
          bool open = false;
          for (const auto& [facetId, facet] : object->facets) {
            if (!facet.open) continue;
            askCheckpoint(object->objectName + "/" + std::to_string(facetId));
            open = true;
          }
          if (open) scheduleIdleCheckpoint(*object);
        }
      }
      scan.finish(1);
    }
    if (++inspected == 16 || monotonicTimeMs() - began >= 0.5) {
      co_await timer.afterDelay(1 * kj::MICROSECONDS);
      inspected = 0;
      began = monotonicTimeMs();
    }
  }
}

kj::Promise<void> DurableLog::housekeeping() {
  for (;;) {
    co_await timer.afterDelay(1 * kj::SECONDS);
    co_await checkpointIdleObjects(monotonicTimeMs());
    try {
      co_await resealEarlier();
      co_await cleanupClosedEpoch();
      co_await dropColdFiles();
    } catch (const kj::Exception& error) {
      log("housekeeping-failed", "\"error\":" + quote(std::string(error.getDescription().cStr())));
    }
  }
}

// The records of an earlier epoch of this node that no segment covers: this node is the only place they are (its
// peers kept them for an epoch that is gone), so they are sealed under that epoch before anything else may need
// them.
void DurableLog::noteEarlierRetired(uint64_t epoch, const LogRecord& record) {
  auto& info = predecessorLogs[epoch];
  info.retiredTo = std::max(info.retiredTo, record.nodeSeq);
  const auto key = std::make_pair(record.objectName.substr(0, record.objectName.find('/')), record.shard);
  auto held = info.unpublishedShards.find(key);
  if (held != info.unpublishedShards.end() && held->second && --held->second == 0)
    info.unpublishedShards.erase(held);
  auto object = info.unpublishedObjects.find(record.objectName);
  if (object != info.unpublishedObjects.end() && object->second && --object->second == 0)
    info.unpublishedObjects.erase(object);
  auto tracked = info.unpublishedTracked.find(key);
  if (tracked != info.unpublishedTracked.end() && tracked->second && --tracked->second == 0)
    info.unpublishedTracked.erase(tracked);
}

void DurableLog::noteEarlierUnpublished(PredecessorLog& info, const LogRecord& record) {
  const auto shard = std::make_pair(record.objectName.substr(0, record.objectName.find('/')), record.shard);
  ++info.unpublishedShards[shard];
  ++info.unpublishedObjects[record.objectName];
  ++info.unpublishedTracked[shard];
}

bool DurableLog::restoredObjectPending(const PredecessorLog& info, uint64_t epoch, const std::string& objectName) const {
  if (restoredTailControlForTest == RestoredTailControl::IGNORE_PUBLISHING && earlierPublishing.contains(objectName))
    return false;  // deliberately wrong: a manifest being written does not name the records yet
  if (auto held = info.unpublishedObjects.find(objectName); held != info.unpublishedObjects.end() && held->second)
    return true;
  if (auto indexed = earlierByObject.find(objectName); indexed != earlierByObject.end()) {
    const auto position = indexed->second.lower_bound({epoch, 0});
    if (position != indexed->second.end() && position->first == epoch) return true;
  }
  if (auto excluded = earlierExcluded.find(epoch); excluded != earlierExcluded.end())
    for (const auto& [sequence, record] : excluded->second)
      if (record.objectName == objectName) return true;
  return false;
}

void DurableLog::retireEarlierObject(uint64_t epoch, const std::string& hostName, uint64_t firstSeq, uint64_t lastSeq) {
  auto held = earlier.find(epoch);
  if (held == earlier.end()) return;
  const auto slash = hostName.rfind('/');
  auto indexed = earlierByObject.find(std::string_view(hostName).substr(0, slash == std::string::npos ? 0 : slash));
  if (indexed == earlierByObject.end()) return;
  std::vector<uint64_t> sequences;
  for (auto position = indexed->second.lower_bound({epoch, firstSeq});
       position != indexed->second.end() && position->first == epoch && position->second <= lastSeq; ++position) {
    auto record = held->second.find(position->second);
    if (record != held->second.end() && hostNameOf(record->second) == hostName) sequences.push_back(position->second);
  }
  for (const auto sequence : sequences) {
    auto record = held->second.find(sequence);
    noteEarlierRetired(epoch, record->second);
    unindexEarlierRecord({epoch, sequence}, record->second);
    held->second.erase(record);
  }
  if (held->second.empty()) earlier.erase(held);
}

kj::Promise<void> DurableLog::cleanupClosedEpoch() {
  if (recoveryWriteFailed || recoveryReadIncomplete || !nodeEpoch || (startupAttempted && !startupReady) ||
      persistingSeals || cleaningEpoch) co_return;
  cleaningEpoch = true;
  const auto cleaning = kj::defer([this]() { cleaningEpoch = false; });
  for (auto& [epoch, info] : predecessorLogs) {
    if (epoch == nodeEpoch || info.closed || !info.complete || info.sealedTo < info.maxSeq ||
        earlier.contains(epoch) || earlierExcluded.contains(epoch)) continue;
    if (std::any_of(info.unpublishedShards.begin(), info.unpublishedShards.end(), [](const auto& entry) { return entry.second != 0; })) continue;
    const auto original = logDirectory() / (wide(epoch) + ".log");
    std::error_code code;
    if (!std::filesystem::is_regular_file(original, code) || code) continue;
    std::optional<r47::Span> cleanup(std::in_place, r47::CLOSED_EPOCH_CLEANUP);  // the loop's part only
    std::vector<RecoveryEvent> carry;
    std::vector<std::string> carried;
    // Snapshot aggregate object state once per destination epoch. Later live
    // changes append to that same sidecar. The original files remain until the
    // whole snapshot and the replacement summary are durable.
    for (const auto& [name, state] : recoveryStates) {
      if (carriedRecoveryObjects.contains(name)) continue;
      auto found = objects.find(name);
      if (found == objects.end()) co_return;
      const auto& object = found->second;
      auto add = [&](RecoveryKind kind, RecoveryGeneration generation) {
        carry.push_back(recoveryEvent(kind, object, generation));
      };
      if (state.repaired.first) {
        add(RecoveryKind::REPAIR_BEGIN, state.repaired);add(RecoveryKind::REPAIR_DONE, state.repaired);
      }
      for (const auto& [position, generation] : state.exclusions) {
        auto event = recoveryEvent(RecoveryKind::ISOLATED, object, generation);
        event.shardEpoch = position.first;event.nodeSeq = position.second;carry.push_back(std::move(event));
      }
      if (state.repairPending) add(RecoveryKind::REPAIR_BEGIN, state.repair);
      if (state.rebuild.first) {
        add(RecoveryKind::REBUILD_BEGIN, state.rebuild);
        if (!state.rebuildPending) add(RecoveryKind::REBUILD_DONE, state.rebuild);
      }
      if (state.published) add(RecoveryKind::PUBLISHED, state.rebuild);
      if (state.healing) add(RecoveryKind::HEAL_BEGIN, state.rebuild);
      carried.push_back(name);
    }
    // Appended here, synchronized on a worker thread before anything below relies on it.
    if (!appendRecovery(carry, false)) co_return;
    for (const auto& name : carried) carriedRecoveryObjects.insert(name);
    cleanup.reset();
    if (!carry.empty() && !co_await synchronizeRecoveryFile(recoveryFd) && !syncFile(recoveryFd)) {
      failRecovery("a recovery event could not be appended and synchronized");
      co_return;
    }
    std::ostringstream summary;
    summary << "{\"nodeId\":" << quote(settings.nodeId) << ",\"epoch\":\"" << epoch
        << "\",\"completeEOF\":true,\"crossedBoot\":false,\"bootId\":" << quote(bootId())
        << ",\"modern\":" << (recoveryEpochs[epoch].modern ? "true" : "false")
        << ",\"through\":\"" << info.maxSeq << "\",\"shards\":[";
    bool first = true;
    for (const auto& [namespaceId, shard] : info.shards) {
      summary << (first ? "" : ",") << "{\"namespaceId\":" << quote(namespaceId) << ",\"shard\":" << shard << "}";
      first = false;
    }
    summary << "],\"objects\":[";first = true;
    for (const auto& [name, sourceEpoch] : info.objectEpochs) {
      const auto object = objects.find(name);if (object == objects.end()) continue;
      summary << (first ? "" : ",") << "{\"objectName\":" << quote(name) << ",\"shardEpoch\":\"" << sourceEpoch
          << "\",\"shard\":" << object->second.shard << "}";first = false;
    }
    summary << "]}\n";
    const auto replacement = logDirectory() / (wide(epoch) + ".closed");
    const auto closedEpoch = epoch;
    const auto through = info.maxSeq;
    const auto summaryBytes = summary.str().size();
    // The summary, the removal of the old log and of its sidecar, and the directory's synchronization: on a worker
    // thread (the old log can be hundreds of MiB).
    const auto outcome = co_await backgroundValue([replacement, text = summary.str(), original,
        sidecar = logDirectory() / (wide(epoch) + ".recovery"), directory = logDirectory()]() {
      if (!atomicLocalFile(replacement, text, true)) return 1;
      if (::unlink(original.c_str()) != 0 || (::unlink(sidecar.c_str()) != 0 && errno != ENOENT) ||
          !syncDirectory(directory)) return 2;
      return 0;
    });
    if (outcome == 1) { failRecovery("the released-epoch summary could not be made durable"); co_return; }
    if (outcome == 2) { failRecovery("the released log epoch could not be replaced by its durable summary"); co_return; }
    if (auto closed = predecessorLogs.find(closedEpoch); closed != predecessorLogs.end()) closed->second.closed = true;
    log("log-epoch-closed", "\"epoch\":\"" + std::to_string(closedEpoch) + "\",\"through\":\"" + std::to_string(through) +
        "\",\"summaryBytes\":" + std::to_string(summaryBytes));
    co_return;  // At most one cleanup per housekeeping pass.
  }
}

void DurableLog::persistEarlierSeals() {
  if (recoveryWriteFailed || persistingSeals) return;  // the asynchronous one is writing; it is redone later
  for (auto& [epoch, info] : predecessorLogs) {
    if (epoch == nodeEpoch || !info.complete || info.sealFailures >= 5) continue;
    auto through = info.retiredTo;
    if (auto pending = earlier.find(epoch); pending != earlier.end() && !pending->second.empty())
      through = std::min(through, pending->second.begin()->first - 1);
    if (auto pending = earlierExcluded.find(epoch); pending != earlierExcluded.end() && !pending->second.empty())
      through = std::min(through, pending->second.begin()->first - 1);
    if (through <= info.sealedTo) continue;
    const auto path = logDirectory() / (wide(epoch) + ".log");
    const int fd = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC);
    if (fd < 0) {
      ++info.sealFailures;
      log("earlier-log-seal-failed", "\"epoch\":\"" + std::to_string(epoch) +
          "\",\"errno\":" + std::to_string(errno));
      continue;
    }
    LogRecord marker;marker.kind = LogRecordKind::SEAL;marker.nodeEpoch = epoch;marker.nodeSeq = through;
    const auto framed = frameRecords({encodeLogRecord(marker)});
    size_t done = 0;
    while (done < framed.size()) {
      const auto n = ::write(fd, framed.data() + done, framed.size() - done);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) break;
      done += size_t(n);
    }
#ifdef __APPLE__
    const bool synced = done == framed.size() && ::fsync(fd) == 0;
#else
    const bool synced = done == framed.size() && ::fdatasync(fd) == 0;
#endif
    ::close(fd);
    if (!synced) {
      ++info.sealFailures;
      info.complete = false;
      log("earlier-log-seal-failed", "\"epoch\":\"" + std::to_string(epoch) +
          "\",\"partialBytes\":\"" + std::to_string(done) + "\"");
      continue;
    }
    info.sealedTo = through;
    info.sealFailures = 0;
    log("earlier-log-sealed", "\"epoch\":\"" + std::to_string(epoch) +
        "\",\"through\":\"" + std::to_string(through) + "\"");
  }
}

// What persistEarlierSeals does, with the appends and their fdatasync on a worker thread.
kj::Promise<void> DurableLog::persistEarlierSealsAsync() {
  if (recoveryWriteFailed || persistingSeals) co_return;
  struct Seal { uint64_t epoch, through; std::string path, framed; };
  std::vector<Seal> seals;
  for (auto& [epoch, info] : predecessorLogs) {
    if (epoch == nodeEpoch || !info.complete || info.sealFailures >= 5) continue;
    auto through = info.retiredTo;
    if (auto pending = earlier.find(epoch); pending != earlier.end() && !pending->second.empty())
      through = std::min(through, pending->second.begin()->first - 1);
    if (auto pending = earlierExcluded.find(epoch); pending != earlierExcluded.end() && !pending->second.empty())
      through = std::min(through, pending->second.begin()->first - 1);
    if (through <= info.sealedTo) continue;
    LogRecord marker;marker.kind = LogRecordKind::SEAL;marker.nodeEpoch = epoch;marker.nodeSeq = through;
    seals.push_back({epoch, through, (logDirectory() / (wide(epoch) + ".log")).string(), frameRecords({encodeLogRecord(marker)})});
  }
  if (seals.empty()) co_return;
  persistingSeals = true;
  const auto done = kj::defer([this]() { persistingSeals = false; });
  struct Outcome { int openErrno = 0; size_t written = 0; bool synced = false; };
  auto outcomes = co_await backgroundValue([seals]() {
    std::vector<Outcome> out;
    for (const auto& seal : seals) {
      Outcome outcome;
      const int fd = ::open(seal.path.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC);
      if (fd < 0) { outcome.openErrno = errno; out.push_back(outcome); continue; }
      while (outcome.written < seal.framed.size()) {
        const auto n = ::write(fd, seal.framed.data() + outcome.written, seal.framed.size() - outcome.written);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) break;
        outcome.written += size_t(n);
      }
#ifdef __APPLE__
      outcome.synced = outcome.written == seal.framed.size() && ::fsync(fd) == 0;
#else
      outcome.synced = outcome.written == seal.framed.size() && ::fdatasync(fd) == 0;
#endif
      ::close(fd);
      out.push_back(outcome);
    }
    return out;
  });
  for (size_t i = 0; i < seals.size(); ++i) {
    auto found = predecessorLogs.find(seals[i].epoch);
    if (found == predecessorLogs.end()) continue;
    auto& info = found->second;
    const auto& outcome = outcomes[i];
    if (outcome.openErrno) {
      ++info.sealFailures;
      log("earlier-log-seal-failed", "\"epoch\":\"" + std::to_string(seals[i].epoch) +
          "\",\"errno\":" + std::to_string(outcome.openErrno));
      continue;
    }
    if (!outcome.synced) {
      ++info.sealFailures;
      info.complete = false;
      log("earlier-log-seal-failed", "\"epoch\":\"" + std::to_string(seals[i].epoch) +
          "\",\"partialBytes\":\"" + std::to_string(outcome.written) + "\"");
      continue;
    }
    info.sealedTo = std::max(info.sealedTo, seals[i].through);
    info.sealFailures = 0;
    log("earlier-log-sealed", "\"epoch\":\"" + std::to_string(seals[i].epoch) +
        "\",\"through\":\"" + std::to_string(seals[i].through) + "\"");
  }
}

kj::Promise<void> DurableLog::resealEarlier() {
  co_await persistEarlierSealsAsync();
  if ((earlier.empty() && earlierExcluded.empty()) || !enabled() || !nodeEpoch || sealing || recoveryPublishWaiters || recoveryWriteFailed ||
      (startupAttempted && !startupReady)) co_return;
  // Normal seals and earlier-epoch seals share one writer, including numbering and manifest publication.
  // In particular, an epoch change must not start a list/PUT while the previous epoch's seal is still uploading.
  sealing = true;
  const auto release = kj::defer([this]() { sealing = false; });
  // An object containing only excluded records still needs a manifest. Do not
  // let its absent segment make a restored shard look fully published early.
  if (!earlierExcluded.empty()) {
    const auto epoch = earlierExcluded.begin()->first;
    if (earlierRetryAfter[epoch] <= monotonicTimeMs()) {
      std::set<std::string> names;
      for (const auto& [sequence, record] : earlierExcluded.begin()->second) names.insert(hostNameOf(record));
      for (const auto& name : names) {
        auto* object = objectOf(name);if (object == nullptr) continue;
        if (healingManifest(*object)) continue;
        auto& attempts = earlierPublishAttempts[epoch][name];
        if (++attempts > 5) { earlierRetryAfter[epoch] = monotonicTimeMs() + 30000;attempts = 0;continue; }
        auto current = co_await readManifest(object->namespaceId, object->objectId);
        const bool fenced = current && current->shardEpoch > object->shardEpoch;
        const bool published = fenced || co_await writeManifest(*object, false);
        if (!published || recoveryWriteFailed) continue;
        if (auto pending = earlierExcluded.find(epoch); pending != earlierExcluded.end()) {
          std::erase_if(pending->second, [&](const auto& entry) {
            if (hostNameOf(entry.second) != name) return false;
            noteEarlierRetired(epoch, entry.second);return true;
          });
          if (pending->second.empty()) earlierExcluded.erase(pending);
        }
        log("restored-exclusions-published", "\"object\":" + quote(name) + ",\"epoch\":\"" + std::to_string(epoch) +
            "\",\"fenced\":" + (fenced ? "true" : "false"));
      }
      co_await persistEarlierSealsAsync();
    }
  }
  if (earlier.empty() || recoveryWriteFailed) co_return;
  bool prioritized = false;
  auto chosen = earlier.end();
  while (!earlierPriority.empty()) {
    const auto epoch = earlierPriority.front();earlierPriority.pop_front();
    auto requested = earlier.find(epoch);
    if (requested != earlier.end() && earlierRetryAfter[epoch] <= monotonicTimeMs()) {
      chosen = requested;prioritized = true;break;
    }
  }
  if (chosen == earlier.end()) chosen = earlier.upper_bound(earlierCursor);
  for (size_t i = 0; i < earlier.size(); ++i) {
    if (chosen == earlier.end()) chosen = earlier.begin();
    if (earlierRetryAfter[chosen->first] <= monotonicTimeMs()) break;
    ++chosen;
  }
  if (chosen == earlier.end()) chosen = earlier.begin();
  if (earlierRetryAfter[chosen->first] > monotonicTimeMs()) co_return;
  const auto epoch = chosen->first;
  earlierCursor = epoch;
  // Retain this epoch while any upload or manifest can still fail. A new epoch may arrive during the awaits.
  const auto saved = earlierUploads.find(epoch);
  const auto cached = saved == earlierUploads.end() ? std::optional<ManifestSegment>() : saved->second;
  const bool reuse = cached.has_value();
  // Before, this selection encoded every record (some 15,000 per segment here) and the segment was encoded and
  // hashed on the event loop, up to 0.86 s at a time. Now the loop only picks the records, in slices, with the same
  // boundaries (each record's encoded size is computed, not encoded); the background thread encodes, indexes,
  // assembles and hashes (buildEarlierSegment), and the loop uploads with that digest and publishes.
  const auto selectStarted = monotonicTimeMs();
  auto selection = co_await selectEarlier(epoch, cached);
  const auto selectMs = monotonicTimeMs() - selectStarted;
  if (selection.aborted) co_return;
  if (reuse ? !selection.records : selection.input.records.empty()) {
    earlierUploads.erase(epoch);
    if (auto held = earlier.find(epoch); held != earlier.end() && held->second.empty()) earlier.erase(held);
    co_return;
  }
  SegmentHeader header;
  header.nodeId = settings.nodeId;
  header.nodeEpoch = epoch;
  header.segment = 1;
  header.sealedAtUnixMs = realClockUnixMs();
  header.reason = "earlier-epoch";
  std::set<std::string> touched;
  std::map<std::string, uint64_t> sourceEpochs;
  // The segment number of an earlier epoch may be taken already: the next free one is used.
  if (!reuse) {
    auto listing = co_await store->list(nodeLogEpochPrefix(settings.nodeId, epoch));
    header.segment = latestRevision(listing, nodeLogPrefix(settings.nodeId) + wide(epoch)) + 1;
    const auto buildStarted = monotonicTimeMs();
    auto built = co_await backgroundValue([header, input = std::move(selection.input)]() mutable {
      return buildEarlierSegment(std::move(header), std::move(input));
    });
    const auto buildMs = monotonicTimeMs() - buildStarted;
    ++earlierSegmentBuilds;
    header = std::move(built.header);
    touched = std::move(built.touched);
    sourceEpochs = std::move(built.sourceEpochs);
    const auto uploadStarted = monotonicTimeMs();
    {
      // The store client copies the body it is given and the request writes all of it: on a worker thread, as is
      // the last reference to the body (freeing 64 MiB is not free either).
      auto body = std::make_shared<const std::string>(std::move(built.body));
      const bool uploaded = co_await uploadSegment(header, *body, std::move(built.sha256), false, body);
      tasks.add(backgroundValue([body = std::move(body)]() mutable { body.reset(); return true; }).ignoreResult());
      if (!uploaded) co_return;
    }
    earlierUploads[epoch] = {settings.nodeId, epoch, header.segment, header.firstSeq, header.lastSeq, 0};
    ++segments;
    log("segment-earlier", "\"epoch\":\"" + std::to_string(epoch) + "\",\"segment\":\"" +
            std::to_string(header.segment) + "\",\"records\":" + std::to_string(header.records) + ",\"objects\":" +
            std::to_string(touched.size()) + ",\"selectMs\":" + std::to_string(selectMs) + ",\"buildMs\":" +
            std::to_string(buildMs) + ",\"uploadMs\":" + std::to_string(monotonicTimeMs() - uploadStarted));
  } else {
    header.segment = cached->segment;
    header.firstSeq = cached->firstSeq;
    header.lastSeq = cached->lastSeq;
    header.records = selection.records;
    touched = std::move(selection.touched);
    sourceEpochs = std::move(selection.sourceEpochs);
  }
  if (recoveryPublishWaiters) co_return;
  const auto publishStarted = monotonicTimeMs();
  const auto written = earlierManifests;
  co_await publishEarlierObjects(epoch, header, std::vector<std::string>(touched.begin(), touched.end()), sourceEpochs);
  log("segment-earlier-published", "\"epoch\":\"" + std::to_string(epoch) + "\",\"segment\":\"" +
          std::to_string(header.segment) + "\",\"objects\":" + std::to_string(touched.size()) + ",\"manifests\":" +
          std::to_string(earlierManifests - written) + ",\"publishMs\":" + std::to_string(monotonicTimeMs() - publishStarted));
  // A failed or fenced object does not republish the other objects every second.
  auto held = earlier.find(epoch);
  if (held == earlier.end() || held->second.empty() || held->second.begin()->first > header.lastSeq) {
    earlierUploads.erase(epoch);
    earlierPublishAttempts.erase(epoch);
    earlierRetryAfter.erase(epoch);
  } else if (std::all_of(touched.begin(), touched.end(), [&](const auto& name) {
      return earlierPublishAttempts[epoch][name] >= 5;
    })) {
    earlierRetryAfter[epoch] = monotonicTimeMs() + 30000;
    earlierPublishAttempts.erase(epoch);
  }
  co_await persistEarlierSealsAsync();
  if (prioritized && earlier.contains(epoch)) {
    earlierPriority.push_back(epoch);
    tasks.add(timer.afterDelay(0 * kj::MILLISECONDS).then([this]() { return resealEarlier(); }));
  }
}

kj::Promise<DurableLog::EarlierSelection> DurableLog::selectEarlier(uint64_t epoch, std::optional<ManifestSegment> cached) {
  // The boundaries are the ones the reseal always had: a segment ends before the record that would pass
  // settings.config.segmentBytes of records or the store's read limit, and always holds at least one record. The
  // records are copied for the background thread; every slice of about earlierSliceBytes yields, and the next slice
  // goes on from the next sequence (records published or retired meanwhile are no longer there to take).
  EarlierSelection out;
  const bool reuse = cached.has_value();
  uint64_t wireBytes = segmentEnvelopeBudget(settings.nodeId, "earlier-epoch"), bytes = 0;
  std::map<std::string, std::set<uint32_t>, std::less<>> keys;  // the (object, facet) pairs indexed so far
  uint64_t next = reuse ? cached->firstSeq : 0;
  uint32_t counted = 0;
  for (;;) {
    auto held = earlier.find(epoch);
    if (held == earlier.end()) {
      out.aborted = !out.input.records.empty() || out.records;
      co_return out;
    }
    uint64_t sliced = 0;
    const auto sliceStarted = monotonicTimeMs();
    bool more = false;
    for (auto it = held->second.lower_bound(next); it != held->second.end(); ++it) {
      const auto sequence = it->first;
      const auto& record = it->second;
      if (reuse) {
        if (sequence > cached->lastSeq) co_return out;
        ++out.records;
        if (!out.firstSeq) out.firstSeq = sequence;
        out.lastSeq = sequence;
        const auto host = hostNameOf(record);
        out.touched.insert(host);
        auto& source = out.sourceEpochs[host];
        source = std::max(source, record.shardEpoch);
        sliced += 64;
      } else {
        const auto size = encodedLogRecordSize(record);
        auto known = keys.find(std::string_view(record.objectName));
        const bool indexed = known != keys.end() && known->second.contains(record.facetId);
        const auto extra = size + 4 + (indexed ? 0 : segmentEntryBudget(record));
        if (!out.input.records.empty() && (bytes + size > settings.config.segmentBytes ||
            wireBytes + extra > STORE_SEGMENT_BYTES)) co_return out;
        KJ_REQUIRE(wireBytes + extra <= STORE_SEGMENT_BYTES, "an earlier record cannot fit a readable store segment");
        wireBytes += extra;
        bytes += size;
        if (!indexed) keys[record.objectName].insert(record.facetId);
        out.input.records.push_back(record);
        out.input.sequences.push_back(sequence);
        out.input.sizes.push_back(size);
        sliced += size;
      }
      next = sequence + 1;
      // A clock read costs about 1.4 us with an hpet clock source: look at it every 16 records only.
      if (sliced >= earlierSliceBytes || ((++counted & 15) == 0 && monotonicTimeMs() - sliceStarted >= 0.5)) {
        more = true;
        break;
      }
    }
    if (!more) co_return out;
    co_await timer.afterDelay(1 * kj::MICROSECONDS);
    if (recoveryWriteFailed) {
      out.aborted = true;
      co_return out;
    }
  }
}

// Before, the manifests were written one object after another (with a yield every 8): each write a listing, a
// read and a create on the store (measured at 13 ms per write), then a scan of the whole epoch's records to retire
// the object's (5 ms on the event loop); 4,638 writes over 5 segments took 94 s. Now up to
// EARLIER_PUBLISH_CONCURRENCY objects of the segment are written at once, and retiring uses the per-object index.
// Each object still gets one manifest write per segment, in segment order, and its records are retired only once
// that write succeeded.
constexpr uint32_t EARLIER_PUBLISH_CONCURRENCY = 16;
kj::Promise<void> DurableLog::publishEarlierObjects(uint64_t epoch, const SegmentHeader& header,
    std::vector<std::string> names, const std::map<std::string, uint64_t>& sourceEpochs) {
  size_t next = 0;
  auto worker = [this, epoch, &header, &names, &sourceEpochs, &next]() -> kj::Promise<void> {
    while (next < names.size()) {
      auto name = names[next++];
      const auto source = sourceEpochs.find(name);
      co_await publishEarlierObject(epoch, header, std::move(name), source == sourceEpochs.end() ? 0 : source->second);
    }
  };
  kj::Vector<kj::Promise<void>> workers;
  for (uint32_t i = 0; i < EARLIER_PUBLISH_CONCURRENCY && i < names.size(); ++i) workers.add(worker());
  co_await kj::joinPromises(workers.releaseAsArray());
}

kj::Promise<void> DurableLog::publishEarlierObject(uint64_t epoch, const SegmentHeader& header, std::string objectName,
    uint64_t sourceEpoch) {
  auto* object = objectOf(objectName);
  if (object == nullptr) {
    const auto identity = parseDurableHostName(objectName);
    KJ_REQUIRE(identity.has_value(), "an earlier record has no object identity");
    auto& held = objects[objectName];
    held.objectName = objectName;held.namespaceId = identity->namespaceId;held.objectId = identity->objectId;
    locate(held, identity->namespaceId, identity->objectId, identity->dataEpoch);
    held.shardEpoch = sourceEpoch;
    if (auto pending = earlier.find(epoch); pending != earlier.end())
      for (const auto& [sequence, record] : pending->second)
        if (hostNameOf(record) == objectName) { held.shard = record.shard; break; }
    object = &held;
  }
  if (healingManifest(*object)) co_return;
  auto& attempts = earlierPublishAttempts[epoch][objectName];
  if (attempts >= 5) co_return;
  ++attempts;
  const auto already = std::any_of(object->segments.begin(), object->segments.end(), [&](const ManifestSegment& segment) {
    return segment.nodeId == settings.nodeId && segment.nodeEpoch == epoch && segment.segment == header.segment;
  });
  if (!already) object->segments.push_back(ManifestSegment{settings.nodeId, epoch, header.segment, header.firstSeq,
      header.lastSeq, sourceEpoch});
  const auto key = object->namespaceId + "/" + object->objectId;
  earlierPublishing.insert(key);
  const auto publishing = kj::defer([this, key]() { earlierPublishing.erase(earlierPublishing.find(key)); });
  bool published = false;
  bool fenced = false;
  if (object->state == ObjectState::GONE) {
    auto current = co_await readManifest(object->namespaceId, object->objectId);
    // Lease expiry also marks an object GONE. Its acknowledged old tail still
    // needs publishing when no newer manifest fences it; this metadata write
    // does not reopen the object or grant ownership of its files.
    if (current && current->shardEpoch > object->shardEpoch) {
      fenced = true;
      co_await retireFencedTails(*object, *current);
    }
  }
  if (!fenced) published = co_await writeManifest(*object, false).catch_([](kj::Exception&&) { return false; });
  if (published) {
    ++earlierManifests;
    retireEarlierObject(epoch, objectName, header.firstSeq, header.lastSeq);
  }
}

// This machine's files are a cache: when they take more than their share, the objects nothing has asked for in the
// longest time and whose changes are all in a segment lose theirs. The object is rebuilt from its history when it
// is asked for again.
void DurableLog::accountCacheSize(Object& object) {
  std::error_code code;
  uint64_t bytes = 0;
  r47::Span disk(r47::COLD_OBJECT_FILES);
  for (const auto& [facetId, facet] : object.facets) {
    const auto database = std::filesystem::file_size(databasePath(object, facetId), code);
    if (!code) bytes += database;
    const auto wal = std::filesystem::file_size(databasePath(object, facetId, "-wal"), code);
    if (!code) bytes += wal;
  }
  cachedObjectDiskBytes -= object.cachedDiskBytes;
  cachedObjectDiskBytes += bytes;
  object.cachedDiskBytes = bytes;
  dirtyCacheSizes.erase(object.objectName);
  disk.finish(1);
}

kj::Promise<void> DurableLog::dropColdFiles(uint64_t reservedBytes) {
  if (scanningColdFiles && reservedBytes == 0) co_return;
  while (scanningColdFiles) co_await timer.afterDelay(1 * kj::MILLISECONDS);
  scanningColdFiles = true;
  const auto release = kj::defer([this]() { scanningColdFiles = false; });
  const auto target = diskLimitBytes - std::min(diskLimitBytes, std::max(reservedBytes, seedReservedBytes));
  const auto scanEpoch = nodeEpoch;
  coldFilesSkippedLive = 0;
  if (recoveryWriteFailed || (startupAttempted && !startupReady)) co_return;
  std::error_code code;
  uint64_t logBytes = 0;
  auto began = monotonicTimeMs();
  size_t items = 0;
  auto sliceFull = [&] { return ++items >= 16 || monotonicTimeMs() - began >= 0.5; };
  auto resetSlice = [&] { items = 0; began = monotonicTimeMs(); };
  for (const auto& entry : std::filesystem::directory_iterator(logDirectory(), code)) {
    {
      r47::Span logFiles(r47::COLD_LOG_FILES);
      if (entry.is_regular_file(code)) {
        const auto bytes = entry.file_size(code);
        if (!code) logBytes += bytes;
      }
    }
    if (sliceFull()) {
      co_await timer.afterDelay(1 * kj::MICROSECONDS);
      resetSlice();
    }
  }
  auto dirty = std::move(dirtyCacheSizes);
  while (!dirty.empty()) {
    auto entry = dirty.extract(dirty.begin());
    if (auto* object = objectOf(entry.value())) accountCacheSize(*object);
    if (sliceFull()) {
      co_await timer.afterDelay(1 * kj::MICROSECONDS);
      resetSlice();
    }
  }
  uint64_t total = logBytes + cachedObjectDiskBytes;
  if (reservedBytes != 0 || (total > target && diskUsedBytes <= target) ||
      monotonicTimeMs() >= nextDiskReconciliationMs) {
    for (auto& [name, object] : objects) {
      accountCacheSize(object);
      if (sliceFull()) {
        co_await timer.afterDelay(1 * kj::MICROSECONDS);
        resetSlice();
      }
    }
    nextDiskReconciliationMs = monotonicTimeMs() + 60000;
    total = logBytes + cachedObjectDiskBytes;
  }
  diskUsedBytes = total;
  if (total <= target || nodeEpoch != scanEpoch || recoveryWriteFailed) co_return;
  std::set<std::string> pendingEarlier;
  for (const auto& [epoch, pending] : earlier)
    for (const auto& [sequence, record] : pending) pendingEarlier.insert(hostNameOf(record));
  auto eligible = [&](const Object& object) {
    return (object.lastAccessMs == 0 || monotonicTimeMs() - object.lastAccessMs > 60000) &&
        !pendingEarlier.contains(object.objectName) && !object.snapshotting && object.state == ObjectState::LOCAL &&
        object.cachedDiskBytes != 0 && object.manifestRevision > 0 &&
        std::none_of(object.facets.begin(), object.facets.end(),
                     [&](const auto& entry) { return entry.second.lastSeq > sealedSeq; });
  };
  std::set<std::pair<double, std::string>> cold;
  for (const auto& [name, object] : objects) {
    if (eligible(object)) cold.emplace(object.lastAccessMs, name);
    if (sliceFull()) {
      co_await timer.afterDelay(1 * kj::MICROSECONDS);
      resetSlice();
    }
  }
  while (!cold.empty()) {
    auto entry = cold.extract(cold.begin());
    const auto& [access, name] = entry.value();
    if (sliceFull()) {
      co_await timer.afterDelay(1 * kj::MICROSECONDS);
      resetSlice();
    }
    if (total <= target || nodeEpoch != scanEpoch || recoveryWriteFailed) continue;
    auto* object = objectOf(name);
    if (object == nullptr || object->lastAccessMs != access || !eligible(*object)) continue;
    if (hasLiveOpenFacets(*object) || hasLiveHolders(*object)) {
      ++coldFilesSkippedLive;
      continue;
    }
    r47::Span removal(r47::COLD_REMOVE);
    uint64_t freed = 0;
    for (const auto& [facetId, facet] : object->facets) {
      for (const char* suffix : {"", "-wal", "-shm"}) {
        const auto path = databasePath(*object, facetId, suffix);
        const auto size = std::filesystem::file_size(path, code);
        if (!code) freed += size;
        std::filesystem::remove(path, code);
      }
    }
    std::filesystem::remove(object->facetIndexPath(settings.stackDirectory), code);
    ++object->snapshotVersion;
    object->state = ObjectState::UNKNOWN;
    object->facets.clear();
    accountCacheSize(*object);
    total -= std::min<uint64_t>(total, freed);
    ++filesDropped;
    log("files-dropped", "\"object\":" + quote(name) + ",\"bytes\":\"" + std::to_string(freed) +
            "\",\"reason\":\"this machine's share of Durable Object files is full\"");
  }
  diskUsedBytes = total;
  if (coldFilesSkippedLive != 0) {
    const auto now = monotonicTimeMs();
    if (lastColdFilesSkippedLogMs == 0 || now - lastColdFilesSkippedLogMs >= 60000) {
      lastColdFilesSkippedLogMs = now;
      log("files-drop-live-skipped", "\"objects\":" + std::to_string(coldFilesSkippedLive));
    }
  }
  co_return;
}

// ---- Opening an object: its files are brought up to date with its replicated history first ----

DurableLog::ObjectReady DurableLog::checkObjectDispatch(
    const ObjectIdentity& identity, const std::string& processId, uint64_t generation,
    std::string_view definition) {
  if (!enabled()) return {};
  if ((startupAttempted && !startupReady) || recoveryWriteFailed)
    return {false, 503, "STORAGE_UNAVAILABLE", "the node's recovery log is not ready"};
  auto* object = objectOf(durableHostName(identity.namespaceId, identity.objectId, identity.dataEpoch));
  if (ownerFenced(identity.namespaceId, identity.shard, identity.shardEpoch) ||
      (object != nullptr && (object->shardEpoch != identity.shardEpoch || object->state == ObjectState::GONE)))
    return {false, 409, "DO_SHARD_EPOCH_MISMATCH", "the object's shard ownership changed before dispatch"};
  if (object == nullptr || object->state != ObjectState::LOCAL)
    return {false, 503, "DO_RECOVERING", "the object needs recovery before dispatch"};
  const auto target = processId + "-" + std::to_string(generation);
  const auto selectedChannel = channels.find(target);
  if (selectedChannel == channels.end() || !selectedChannel->second.open ||
      (selectedChannel->second.threadedGeneration != 0 && selectedChannel->second.pid == 0)) {
    ObjectReady result{false, 503, "STORAGE_UNAVAILABLE", "the selected durable generation has no replication channel"};
    result.channelMissing = true;
    return result;
  }
  if (selectedChannel->second.draining)
    return {false, 503, "DO_UNAVAILABLE", "the selected durable generation is draining"};
  for (const auto& [id, channel] : channels) {
    if (id == target || (!channel.open && channel.threadedGeneration == 0)) continue;
    if (object->dispatchedChannel == id)
      return {false, 503, "DO_UNAVAILABLE", "another durable generation still holds the object"};
    if (channel.open && channelHasOpenObject(channel, *object))
      return {false, 503, "DO_UNAVAILABLE", "another durable generation still holds the object"};
  }
  if (!definition.empty() && !object->threadedDefinition.empty() &&
      (definition != object->threadedDefinition || object->definitionDrainDeadline != kj::none)) {
    auto old = channels.find(object->dispatchedChannel);
    if (old != channels.end()) {
      if (auto open = old->second.openFacets.find(object->objectName + "/0"); open != old->second.openFacets.end())
        askClose(old->second, open->second.id);
      const bool closeUnacknowledged =
          !old->second.open || hasLiveOpenFacets(*object) || object->dispatchPending;
      if (closeUnacknowledged || hasLiveHolders(*object)) {
        if (object->definitionDrainDeadline == kj::none) {
          auto deadline = timer.now() + settings.config.versionDrainTimeoutMs * kj::MILLISECONDS;
          object->definitionDrainDeadline = deadline;
          tasks.add(endDefinitionDrain(object->objectName, old->first, deadline));
        }
        ObjectReady result{false, 503, "DO_VERSION_DRAINING", "the previous Durable Object definition is draining"};
        result.generationMustClose = closeUnacknowledged &&
            timer.now() >= KJ_ASSERT_NONNULL(object->definitionDrainDeadline) +
                settings.config.confirmTimeoutMs * kj::MILLISECONDS;
        return result;
      }
    }
    object->definitionDrainDeadline = kj::none;
  }
  if (auto error = prepareOpen(*object, &target))
    return {false, 503, "STORAGE_UNAVAILABLE", *error};
  if (!definition.empty()) object->threadedDefinition.assign(definition);
  return {};
}

std::vector<std::string> DurableLog::drainThreadedNamespaces(const std::vector<std::string>& namespaces) {
  std::vector<std::string> drained;
  for (const auto& ns : namespaces) {
    auto prefix = ns + "/";
    bool open = false;
    for (auto& [id, channel] : channels) {
      if (channel.threadedGeneration == 0) continue;
      if (!channel.open) {
        open = true;
        continue;
      }
      for (auto it = channel.openFacets.lower_bound(prefix);
          it != channel.openFacets.end() && it->first.starts_with(prefix); ++it) {
        open = true;
        if (it->first.ends_with("/0")) askClose(channel, it->second.id);
      }
    }
    if (!open) drained.push_back(ns);
  }
  return drained;
}

void DurableLog::objectDispatched(const ObjectIdentity& identity, const std::string& processId, uint64_t generation) {
  if (auto* object = objectOf(durableHostName(identity.namespaceId, identity.objectId, identity.dataEpoch))) {
    object->lastAccessMs = monotonicTimeMs();
    scheduleIdleCheckpoint(*object);
    ++object->holders[processId + "-" + std::to_string(generation)].requests;
    object->dispatchedChannel = processId + "-" + std::to_string(generation);
    if (!hasLiveOpenFacets(*object)) object->dispatchPending = true;
  }
}

void DurableLog::objectRequestFinished(const ObjectIdentity& identity, const std::string& processId, uint64_t generation) {
  finishObjectHolder(identity, processId, generation, false);
}

void DurableLog::objectConnectionOpened(const ObjectIdentity& identity, const std::string& processId, uint64_t generation) {
  if (auto* object = objectOf(durableHostName(identity.namespaceId, identity.objectId, identity.dataEpoch)))
    ++object->holders[processId + "-" + std::to_string(generation)].connections;
}

void DurableLog::objectConnectionClosed(const ObjectIdentity& identity, const std::string& processId, uint64_t generation) {
  finishObjectHolder(identity, processId, generation, true);
}

void DurableLog::finishObjectHolder(
    const ObjectIdentity& identity, const std::string& processId, uint64_t generation, bool connection) {
  auto* object = objectOf(durableHostName(identity.namespaceId, identity.objectId, identity.dataEpoch));
  if (object == nullptr) return;
  auto holder = object->holders.find(processId + "-" + std::to_string(generation));
  if (holder == object->holders.end()) return;
  auto& count = connection ? holder->second.connections : holder->second.requests;
  if (count != 0) --count;
  if (holder->second.requests == 0 && holder->second.connections == 0) object->holders.erase(holder);
}

kj::Promise<DurableLog::ObjectReady> DurableLog::openObject(ObjectIdentity identity, bool prepareDispatch,
    bool background, bool waitForRebuild) {
  if (!enabled()) co_return ObjectReady{};
  if (startupAttempted && !startupReady) {
    if (!startupInitializing && monotonicTimeMs() >= startupRetryAfterMs) co_await start();
    if (!startupReady) co_return ObjectReady{false, 503, "STORAGE_UNAVAILABLE", "the startup marker is not durable"};
  }
  if (recoveryWriteFailed)
    co_return ObjectReady{false, 503, "STORAGE_UNAVAILABLE", "the recovery log failed; this process must restart"};
  if (ownerFenced(identity.namespaceId, identity.shard, identity.shardEpoch))
    co_return ObjectReady{false, 409, "DO_SHARD_EPOCH_MISMATCH", "the owner epoch was fenced"};
  if (!membership.epoch.empty()) setNodeEpoch(decimal(membership.epoch));
  const auto objectName = durableHostName(identity.namespaceId, identity.objectId, identity.dataEpoch);
  if (background) {
    auto alarm = alarms.find(identity.namespaceId + "/" + identity.objectId + "/0");
    if (alarm != alarms.end()) { alarm->second.objectName = objectName;alarm->second.dataEpoch = identity.dataEpoch; }
  }
  auto settled = rebuilding.find(objectName);
  if (settled != rebuilding.end() && settledRebuilds.contains(objectName)) {
    rebuilding.erase(settled);
    rebuildCancelers.erase(objectName);
    settledRebuilds.erase(objectName);
  }
  if (!objects.contains(objectName) && !rebuildMetadataFits(REBUILD_ENTRY_BYTES, background)) {
    ++rebuildMetadataRefusals;
    co_return ObjectReady{false, 503, "DO_RECOVERING", "the node's recovery metadata budget is full"};
  }
  auto& object = objects[objectName];
  if (object.objectName.empty()) {
    object.objectName = objectName;
    object.namespaceId = identity.namespaceId;
    object.objectId = identity.objectId;
    object.dataEpoch = identity.dataEpoch;
    locate(object, identity.namespaceId, identity.objectId, identity.dataEpoch);
  }
  object.shard = identity.shard;
  if (!identity.tenant.empty()) object.tenant = identity.tenant;
  if (identity.shardEpoch > object.shardEpoch && object.state == ObjectState::REBUILDING) {
    object.state = ObjectState::UNKNOWN;
    if (auto canceler = rebuildCancelers.find(objectName); canceler != rebuildCancelers.end())
      canceler->second->cancel("the object's recovery was superseded by a new shard epoch");
    rebuilding.erase(objectName);
    rebuildCancelers.erase(objectName);
    settledRebuilds.erase(objectName);
  }
  if (identity.shardEpoch >= object.shardEpoch) {
    if (identity.shardEpoch > object.shardEpoch) {
      ++object.snapshotVersion;
      // Snapshot watermarks belong to a shard epoch. Sequence numbers from a
      // predecessor's log must not cover future records of this epoch.
      for (auto& [id, facet] : object.facets) facet.lastSeq = facet.checkpointedSeq = 0;
    }
    object.shardEpoch = identity.shardEpoch;
    if (object.state == ObjectState::GONE) object.state = ObjectState::UNKNOWN;
  }
  object.lastAccessMs = monotonicTimeMs();
  scheduleIdleCheckpoint(object);
  // An object past durableObjects.objectBytes is not turned away here: only its writes are refused, at the moment
  // their frames would be confirmed (413 DO_OBJECT_TOO_LARGE), and everything that only reads is served.
  if (object.state == ObjectState::LOCAL) {
    if (prepareDispatch) {
      if (auto error = prepareOpen(object)) co_return ObjectReady{false, 503, "STORAGE_UNAVAILABLE", *error};
    }
    co_return ObjectReady{};
  }
  if (object.state == ObjectState::GONE) {
    co_return ObjectReady{false, 409, "DO_SHARD_EPOCH_MISMATCH",
        "another node owns this object's shard now"};
  }
  const auto reason = object.state == ObjectState::REPAIR
      ? object.repairReason
      : std::string("this node has not served the object at this data epoch");
  if (rebuilding.find(objectName) == rebuilding.end()) {
    if (!rebuildMetadataFits(REBUILD_ENTRY_BYTES, background)) {
      ++rebuildMetadataRefusals;
      co_return ObjectReady{false, 503, "DO_RECOVERING", "the node's recovery metadata budget is full"};
    }
    auto canceler = std::make_unique<kj::Canceler>();
    auto operation = rebuild(objectName, reason, background);
    auto counted = kj::mv(operation).catch_([this, identity, objectName](kj::Exception&& error) {
      auto* current = objectOf(objectName);
      if (current != nullptr && current->state != ObjectState::GONE &&
          current->shardEpoch == identity.shardEpoch) {
        lastRebuildError = error.getDescription().cStr();
        ++rebuildFailures;
        objectFailed(identity.namespaceId, identity.objectId, identity.shardEpoch, "rebuild", lastRebuildError);
      }
      throw kj::mv(error);
    });
    rebuilding.insert({objectName, canceler->wrap(kj::mv(counted)).fork()});
    rebuildCancelers[objectName] = std::move(canceler);
  }
  if (!background) promoteRebuild(objectName);
  if (!waitForRebuild) co_return ObjectReady{false, 503, "DO_ALARM_REBUILDING", "the alarm awaits its queued background rebuild"};
  const auto start = monotonicTimeMs();
  auto branch = rebuilding.find(objectName)->second.addBranch();
  bool done = false;
  try {
    done = co_await kj::mv(branch).then([]() { return true; }).exclusiveJoin(
        timer.afterDelay(settings.config.recoverTimeoutMs * kj::MILLISECONDS).then([]() { return false; }));
  } catch (const kj::Exception& error) {
    if (auto* current = objectOf(objectName); current == nullptr || current->state == ObjectState::GONE ||
        current->shardEpoch != identity.shardEpoch)
      co_return ObjectReady{false, 409, "DO_SHARD_EPOCH_MISMATCH", "ownership changed during recovery",
          monotonicTimeMs() - start};
    if (error.getDetail(REBUILD_METADATA_FULL) != kj::none)
      co_return ObjectReady{false, 503, "DO_RECOVERING", "the node's recovery metadata budget is full",
          monotonicTimeMs() - start};
    co_return ObjectReady{false, 503, "STORAGE_UNAVAILABLE",
        "the object could not be rebuilt from its replicated history", monotonicTimeMs() - start};
  }
  if (!done) {
    ++recoverTimeouts;
    co_return ObjectReady{false, 503, "DO_RECOVERING", "the object is being rebuilt from its replicated history",
        monotonicTimeMs() - start};
  }
  auto* entry = objectOf(objectName);
  if (entry == nullptr || entry->state != ObjectState::LOCAL) {
    co_return ObjectReady{false, 503, "STORAGE_UNAVAILABLE", "the object is not ready", monotonicTimeMs() - start};
  }
  if (ownerFenced(identity.namespaceId, identity.shard, identity.shardEpoch) || entry->shardEpoch > identity.shardEpoch)
    co_return ObjectReady{false, 409, "DO_SHARD_EPOCH_MISMATCH", "ownership changed during recovery",
        monotonicTimeMs() - start};
  if (entry->shardEpoch < identity.shardEpoch) {
    ++entry->snapshotVersion;
    for (auto& [id, facet] : entry->facets) facet.lastSeq = facet.checkpointedSeq = 0;
    entry->shardEpoch = identity.shardEpoch;
  }
  if (prepareDispatch) {
    if (auto error = prepareOpen(*entry))
      co_return ObjectReady{false, 503, "STORAGE_UNAVAILABLE", *error};
  }
  ObjectReady ready;
  ready.elapsedMs = monotonicTimeMs() - start;
  ready.rebuilt = true;
  co_return ready;
}

// The whole of a rebuild: the object's files on this machine go, its manifest says which snapshot and which
// segments make it up, and what no segment covers comes from the peers of its shard (and from this node's own log
// when it is repairing an object of its own). The previous owner's files are never read.
kj::Promise<void> DurableLog::waitForAlarmRebuild(const ObjectIdentity& identity) {
  const auto name = durableHostName(identity.namespaceId, identity.objectId, identity.dataEpoch);
  auto it = rebuilding.find(name);
  if (it == rebuilding.end()) return kj::READY_NOW;
  return it->second.addBranch();
}
kj::Promise<void> DurableLog::waitForAlarmRebuild(const std::string& ns, const std::string& object) {
  auto current = alarms.find(ns + "/" + object + "/0");if (current == alarms.end()) return kj::READY_NOW;
  auto it = rebuilding.find(current->second.objectName);if (it == rebuilding.end()) return kj::READY_NOW;
  return it->second.addBranch();
}
void DurableLog::promoteRebuild(const std::string& name) {
  auto run = rebuildRuns.find(name);
  if (run == rebuildRuns.end() || !run->second->background) return;
  backgroundRebuildMetadataBytes -= run->second->metadataBytes;
  run->second->background = false;
  ++rebuildPromotions;
  auto it = queuedRebuildIndex.find(name);
  if (it == queuedRebuildIndex.end() || !it->second.background) return;
  auto row = it->second.row;
  if (!row->ready->isWaiting()) { backgroundRebuildWaiters.erase(row);queuedRebuildIndex.erase(it);return; }
  rebuildWaiters.splice(rebuildWaiters.end(), backgroundRebuildWaiters, row);
  it->second.background = false;
  pumpRebuilds();
}
void DurableLog::pumpRebuilds() {
  const auto width = std::max<uint32_t>(1, settings.config.rebuildConcurrency);
  auto forget = [this](const RebuildWaiter& row) {
    auto it = queuedRebuildIndex.find(row.objectName);
    if (it != queuedRebuildIndex.end() && it->second.ticket == row.ticket) queuedRebuildIndex.erase(it);
  };
  auto discard = [&forget](auto& queue) { while (!queue.empty() && !queue.front().ready->isWaiting()) { forget(queue.front());queue.pop_front(); } };
  while (activeRebuilds < width) {
    discard(rebuildWaiters);discard(backgroundRebuildWaiters);
    const bool foreground = !rebuildWaiters.empty(), background = !backgroundRebuildWaiters.empty();
    if (!foreground && !background) break;
    bool pickBackground = background;
    if (foreground && background) {
      const auto reserved = std::min(2u, width);
      if (width <= 2) pickBackground = nextBackgroundRebuild;
      else if (activeBackgroundRebuilds < reserved) pickBackground = true;
      else if (activeRebuilds - activeBackgroundRebuilds < width - reserved) pickBackground = false;
      else pickBackground = nextBackgroundRebuild;
    }
    auto& queue = pickBackground ? backgroundRebuildWaiters : rebuildWaiters;
    forget(queue.front());
    auto next = kj::mv(queue.front());queue.pop_front();
    ++activeRebuilds;if (pickBackground) ++activeBackgroundRebuilds;
    nextBackgroundRebuild = !pickBackground;
    // Reserve before waking so cancellation before resumption returns this class's slot too.
    next.run->reserved = true;
    next.run->reservedBackground = pickBackground;
    next.ready->fulfill();
  }
}
void DurableLog::releaseRebuildSlot(RebuildRun& run) {
  if (!run.reserved) return;
  run.reserved = false;
  --activeRebuilds;
  if (run.reservedBackground) --activeBackgroundRebuilds;
  if (!stopped) pumpRebuilds();
}

bool DurableLog::rebuildMetadataFits(size_t bytes, bool background) const {
  // Keep a quarter of the budget available to requests during alarm takeover.
  return bytes <= rebuildMetadataLimit - rebuildMetadataBytes &&
      (!background || bytes <= rebuildMetadataLimit * 3 / 4 - backgroundRebuildMetadataBytes);
}

void DurableLog::chargeRebuildMetadata(RebuildRun& run, size_t bytes) {
  if (!rebuildMetadataFits(bytes, run.background)) {
    ++rebuildMetadataRefusals;
    auto error = KJ_EXCEPTION(OVERLOADED, "the node's recovery metadata budget is full");
    error.setDetail(REBUILD_METADATA_FULL, kj::heapArray<kj::byte>(0));
    throw kj::mv(error);
  }
  run.metadataBytes += bytes;
  rebuildMetadataBytes += bytes;
  if (run.background) backgroundRebuildMetadataBytes += bytes;
  peakRebuildMetadataBytes = std::max(peakRebuildMetadataBytes, rebuildMetadataBytes);
}

void DurableLog::setRebuildPhase(RebuildRun& run, RebuildPhase phase) {
  if (run.phase == phase) return;
  run.phase = phase;
  run.phaseAtMs = monotonicTimeMs();
}

const char* DurableLog::rebuildPhaseName(RebuildPhase phase) {
  switch (phase) {
    case RebuildPhase::QUEUED: return "queued";
    case RebuildPhase::PREPARING: return "preparing";
    case RebuildPhase::TAILS: return "tail-collection";
    case RebuildPhase::DEADLINE: return "deadline";
    case RebuildPhase::OWNER: return "owner";
    case RebuildPhase::FINAL_TAILS: return "final-tails";
    case RebuildPhase::RESUMING: return "resuming";
    case RebuildPhase::REPLAY: return "replay";
  }
  KJ_UNREACHABLE;
}

kj::Promise<void> DurableLog::acquireRebuildSlot(const std::string& name, RebuildRun& run) {
  auto available = kj::newPromiseAndFulfiller<void>();
  auto& queue = run.background ? backgroundRebuildWaiters : rebuildWaiters;
  const auto ticket = ++rebuildWaitSerial;
  queue.push_back({kj::mv(available.fulfiller), &run, name, ticket});
  queuedRebuildIndex.emplace(name, QueuedRebuild{run.background, ticket, std::prev(queue.end())});
  const auto removeWaiter = kj::defer([&]() {
    auto found = queuedRebuildIndex.find(name);
    if (found == queuedRebuildIndex.end() || found->second.ticket != ticket) return;
    auto& pending = found->second.background ? backgroundRebuildWaiters : rebuildWaiters;
    pending.erase(found->second.row);
    queuedRebuildIndex.erase(found);
  });
  const auto queuedAt = monotonicTimeMs();
  pumpRebuilds();
  co_await kj::mv(available.promise);
  run.queuedMs += monotonicTimeMs() - queuedAt;
}

kj::Promise<void> DurableLog::rebuild(std::string objectName, std::string reason, bool background) {
  RebuildRun run;
  run.background = background;
  run.phaseAtMs = monotonicTimeMs();
  const auto done = kj::defer([&]() {
    releaseRebuildSlot(run);
    rebuildMetadataBytes -= run.metadataBytes;
    if (run.background) backgroundRebuildMetadataBytes -= run.metadataBytes;
    rebuildRuns.erase(objectName);
    settledRebuilds.insert(objectName);
  });
  chargeRebuildMetadata(run, REBUILD_ENTRY_BYTES);
  rebuildRuns.emplace(objectName, &run);
  co_await acquireRebuildSlot(objectName, run);
  if (run.reservedBackground) ++backgroundRebuilds;
  else ++foregroundRebuilds;
  auto* object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "the recovery object disappeared");
  const auto targetEpoch = object->shardEpoch, targetDataEpoch = object->dataEpoch;
  for (uint32_t attempt = 1; attempt <= MANIFEST_ATTEMPTS; ++attempt) {
    if (!run.reserved) {
      setRebuildPhase(run, RebuildPhase::RESUMING);
      co_await acquireRebuildSlot(objectName, run);
      object = objectOf(objectName);
      KJ_REQUIRE(object && object->state == ObjectState::REBUILDING &&
          object->shardEpoch == targetEpoch && object->dataEpoch == targetDataEpoch,
          "ownership changed before retrying recovery");
      run.parkedAtMs = 0;
    }
    object->recoveryRevisionChanged = false;
    try {
      co_await rebuildAttempt(objectName, reason, run);
      co_return;
    } catch (const kj::Exception& error) {
      object = objectOf(objectName);
      if (object == nullptr || object->state != ObjectState::REBUILDING ||
          object->shardEpoch != targetEpoch || object->dataEpoch != targetDataEpoch ||
          !object->recoveryRevisionChanged) throw;
      // A known publication race is retried inside the same open. Storage
      // failures still fail that attempt, and ownership cancellation still wins.
      log("rebuild-retry-manifest", "\"object\":" + quote(objectName) + ",\"attempt\":" + std::to_string(attempt));
    }
  }
  failConflict("the recovery manifest kept changing during replay");
}

kj::Promise<uint64_t> DurableLog::recoveryLowerBound(const std::string& objectName,
    const std::optional<ObjectManifest>& manifest, bool preparingLocally) {
  if (!manifest) co_return 0;
  auto* object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "the recovery object disappeared before its lower-bound check");
  const auto namespaceId = object->namespaceId;
  const auto shard = object->shard;
  const auto response = co_await readRecoveryEpoch(shardEpochKey(namespaceId, shard, manifest->shardEpoch), preparingLocally);
  const auto epoch = parseShardEpoch(response->bytes);
  requireStorage(epoch && epoch->epoch == manifest->shardEpoch && epoch->namespaceId == namespaceId && epoch->shard == shard,
      "the latest manifest's ownership origin is unavailable");
  co_return std::max(manifest->snapshotShardEpoch, epoch->dataEpoch);
}

std::optional<DurableLog::SocketOpenAuthority> DurableLog::authorizeSocketOpen(
    const ObjectIdentity& identity, uint64_t generation, uint64_t openId) {
  const auto key = durableHostName(identity.namespaceId, identity.objectId, identity.dataEpoch);
  auto found = objects.find(key);
  const auto* object = found == objects.end() ? nullptr : &found->second;
  if (!object || object->state != ObjectState::LOCAL || object->shardEpoch != identity.shardEpoch ||
      object->definitionDrainDeadline != kj::none) return std::nullopt;
  auto channel = channels.find(object->dispatchedChannel);
  if (channel == channels.end() || !channel->second.open || channel->second.draining ||
      generation != channel->second.threadedGeneration || openId == 0 ||
      channel->second.closeAsked.contains(openId)) return std::nullopt;
  auto registration = channel->second.openFacets.find(key + "/0");
  if (registration == channel->second.openFacets.end() || registration->second.id != openId)
    return std::nullopt;
  return SocketOpenAuthority{channel->first.substr(0, channel->first.rfind('-')),
      registration->second.socketPark, registration->second.socketAdopt};
}

void DurableLog::askClose(Channel& channel, uint64_t openId) {
  if (!channel.closeAsked.insert(openId).second) return;
  std::string message;
  putU8(message, TO_CLOSE);
  putU64(message, openId);
  sendToChannel(channel, std::move(message));
}

void DurableLog::askCodeUpdateClose(Channel& channel, Channel::Open& root, const Object& object) {
  if (root.codeUpdateAsked) return;
  root.codeUpdateAsked = true;
  std::string message;
  putU8(message, TO_CLOSE_FOR_CODE_UPDATE);
  putU64(message, root.id);
  sendToChannel(channel, std::move(message));
  log("version-drain-expired", "\"object\":" + quote(object.objectName));
}

kj::Promise<void> DurableLog::endDefinitionDrain(std::string objectName, std::string channelId,
    kj::TimePoint deadline) {
  co_await timer.atTime(deadline);
  const auto* object = objectOf(objectName);
  if (object == nullptr || object->state != ObjectState::LOCAL || object->definitionDrainDeadline != deadline ||
      object->dispatchedChannel != channelId) co_return;
  auto channel = channels.find(channelId);
  if (channel == channels.end() || !channel->second.open) co_return;
  auto root = channel->second.openFacets.find(objectName + "/0");
  // An open still preparing its WAL registers separately. The registration
  // path checks this same deadline before accepting that open.
  if (root != channel->second.openFacets.end()) askCodeUpdateClose(channel->second, root->second, *object);
}

kj::Promise<bool> DurableLog::closeThreadedObject(const std::string& objectName) {
  const auto* object = objectOf(objectName);
  if (object == nullptr || object->state != ObjectState::REBUILDING) co_return false;
  const auto processId = object->dispatchedChannel;
  auto channel = channels.find(processId);
  if (channel == channels.end() || !channel->second.open || channel->second.threadedGeneration == 0) co_return false;
  const auto root = channel->second.openFacets.find(objectName + "/0");
  if (root == channel->second.openFacets.end()) co_return false;
  const auto expected = object->rebuildGeneration;
  const auto openId = root->second.id;
  askClose(channel->second, openId);
  const auto end = timer.now() + settings.config.confirmTimeoutMs * kj::MILLISECONDS;
  for (;;) {
    const auto* current = objectOf(objectName);
    KJ_REQUIRE(current != nullptr && current->state == ObjectState::REBUILDING &&
        current->rebuildGeneration == expected, "recovery changed while closing the durable home");
    auto owner = channels.find(processId);
    if (owner == channels.end()) co_return true;  // the pool reaped the generation
    if (!owner->second.open) co_return false;  // EOF alone cannot prove the child closed SQLite
    if (!hasLiveOpenFacets(*current) && !hasLiveHolders(*current) && !current->dispatchPending) {
      log("object-closed", "\"object\":" + quote(objectName) + ",\"processId\":" + quote(processId) +
          ",\"openId\":\"" + std::to_string(openId) + "\"");
      co_return true;
    }
    if (timer.now() >= end) co_return false;
    co_await timer.afterDelay(10 * kj::MILLISECONDS);
  }
}

kj::Promise<void> DurableLog::rebuildAttempt(std::string objectName, std::string reason, RebuildRun& run) {
  KJ_REQUIRE(run.reserved);
  setRebuildPhase(run, RebuildPhase::PREPARING);
  const auto start = monotonicTimeMs();
  const auto metadataBefore = run.metadataBytes;
  const auto releaseAttemptMetadata = kj::defer([&]() {
    rebuildMetadataBytes -= run.metadataBytes - metadataBefore;
    if (run.background) backgroundRebuildMetadataBytes -= run.metadataBytes - metadataBefore;
    run.metadataBytes = metadataBefore;
  });
  // Where this attempt's time goes, consecutive laps of the same clock (logged with `rebuilt`).
  struct Laps { double setup = 0, manifest = 0, snapshot = 0, segments = 0, local = 0, tailWait = 0, latest = 0,
      prepare = 0, replay = 0, files = 0, publish = 0, markers = 0; };
  Laps laps;
  SegmentReadStats segmentStats;
  auto lapMark = start;
  auto lap = [&lapMark](double& slot) { const auto now = monotonicTimeMs(); slot += now - lapMark; lapMark = now; };
  auto loopDelay = std::make_shared<double>(0);
  recoveryLoopObservers.insert(loopDelay);
  auto stopMeasuring = kj::defer([this, loopDelay]() { recoveryLoopObservers.erase(loopDelay); });
  if (!measuringRecovery) { measuringRecovery = true; tasks.add(measureRecoveryLoop()); }
  double tailStart = start;
  {
    auto* object = objectOf(objectName);
    KJ_REQUIRE(object != nullptr, "an object being rebuilt is gone");
    KJ_REQUIRE(!ownerFenced(object->namespaceId, object->shard, object->shardEpoch),
        "the recovery owner epoch was fenced");
    if (!object->recoveryStartedMs || object->recoveryShardEpoch != object->shardEpoch) {
      object->recoveryStartedMs = start;
      object->recoveryShardEpoch = object->shardEpoch;
      object->lateTails.clear();
    }
    tailStart = object->recoveryStartedMs;
    const bool repair = object->state == ObjectState::REPAIR;
    KJ_REQUIRE(!recoveryWriteFailed, "recovery cannot replace a held or unfenced image");
    if (repair) KJ_REQUIRE(beginRepair(*object), "the repair exclusion could not be made durable");
    isolateObjectPending(*object, "pending records cannot outlive the image used by recovery");
    KJ_REQUIRE(!recoveryWriteFailed, "pending exclusions could not be made durable");
    const auto dispatched = channels.find(object->dispatchedChannel);
    const bool threadedChannelLost = dispatched != channels.end() && dispatched->second.threadedGeneration != 0 &&
        !dispatched->second.open;
    const bool closeLive = threadedChannelLost || hasLiveOpenFacets(*object) ||
        (hasLiveDispatch(*object) && object->dispatchPending) || hasLiveHolders(*object);
    object->rebuildGeneration = {nodeEpoch, ++recoverySerial};
    KJ_REQUIRE(appendRecovery({recoveryEvent(RecoveryKind::REBUILD_BEGIN, *object, object->rebuildGeneration)}, false),
        "the rebuild begin marker could not be written");
    ++object->snapshotVersion;
    object->state = ObjectState::REBUILDING;
    log("rebuild-started", "\"object\":" + quote(objectName) + ",\"reason\":" + quote(reason) + ",\"repair\":" +
            (repair ? "true" : "false"));
    if (closeLive && !(co_await closeThreadedObject(objectName)) && hooks.closeObjects) {
      // Nothing may hold the files while they are replaced, and the object must stop serving what it read from them.
      co_await hooks.closeObjects("a Durable Object write was not confirmed: " + reason);
    }
  }
  auto* object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "an object being rebuilt is gone");
  const auto targetEpoch = object->shardEpoch;
  const auto targetDataEpoch = object->dataEpoch;
  const auto generation = object->rebuildGeneration;
  auto check = [&]() {
    object = objectOf(objectName);
    KJ_REQUIRE(object != nullptr && object->state == ObjectState::REBUILDING &&
        object->shardEpoch == targetEpoch && object->dataEpoch == targetDataEpoch && object->rebuildGeneration == generation &&
        !ownerFenced(object->namespaceId, object->shard, targetEpoch) && !recoveryWriteFailed,
        "ownership changed during object recovery");
  };
  std::error_code directoryError;
  std::filesystem::create_directories(object->directory(settings.stackDirectory), directoryError);
  KJ_REQUIRE(!directoryError, "the object's recovery directory could not be created");
  const RootStorageRef storage = *object;
  const auto staging = ".rebuild-" + std::to_string(nodeEpoch) + "-" + std::to_string(targetEpoch);
  std::set<uint32_t> stagingFacets;
  const auto cleanupStaging = kj::defer([this, storage, staging, &stagingFacets]() noexcept {
    for (auto facetId : stagingFacets) {
      try {
        std::error_code ignored;
        std::filesystem::remove(databasePath(storage, facetId, "", staging), ignored);
      } catch (...) {}
    }
  });
  lap(laps.setup);
  auto manifest = co_await readManifest(object->namespaceId, object->objectId);
  check();
  if (manifest) {
    size_t bytes = manifest->isolated.capacity() * sizeof(manifest->isolated.front()) +
        manifest->facets.capacity() * sizeof(ManifestFacet) + manifest->segments.capacity() * sizeof(ManifestSegment);
    for (const auto& facet : manifest->facets) bytes += facet.name.size() + facet.sha256.size();
    for (const auto& segment : manifest->segments) bytes += segment.nodeId.size();
    chargeRebuildMetadata(run, bytes);
  }
  lap(laps.manifest);
  std::vector<ManifestFacet> facets;
  uint64_t fromSeq = 0;
  object->recoveryManifestRevision = manifest ? manifest->revision : 0;
  // The first shard epoch has no predecessor. Read it and collect the local
  // tail under the rebuild slot, as for a trusted restart on the original disk.
  // Remote, missing or incomplete proof still releases the slot for polling.
  kj::Maybe<kj::Promise<bool>> preparedLocally;
  kj::Own<kj::PromiseFulfiller<bool>> localPreparation;
  if (targetEpoch == 1 || (diskTrusted && sameBootCache && targetDataEpoch < targetEpoch)) {
    auto prepared = kj::newPromiseAndFulfiller<bool>();
    preparedLocally = kj::mv(prepared.promise);
    localPreparation = kj::mv(prepared.fulfiller);
  }
  auto collecting = fetchTails(ObjectIdentity{object->namespaceId, object->objectId, object->tenant,
      object->dataEpoch, object->shardEpoch, object->shard},
      manifest, tailStart, run, kj::mv(localPreparation)).eagerlyEvaluate(nullptr);
  if (manifest) {
    if (manifest->shardEpoch > object->shardEpoch) {
      object->state = ObjectState::GONE;
      ++manifestFences;
      log("rebuild-fenced", "\"object\":" + quote(objectName) + ",\"ours\":\"" + std::to_string(object->shardEpoch) +
              "\",\"theirs\":\"" + std::to_string(manifest->shardEpoch) + "\"");
      KJ_FAIL_REQUIRE("another node owns this object's shard", objectName.c_str());
    }
    object->manifestRevision = manifest->revision;
    object->recoveryManifestRevision = manifest->revision;
    object->snapshotSequence = manifest->snapshotSequence;
    object->snapshotAtUnixMs = manifest->snapshotAtUnixMs;
    if (manifest->hasSnapshot) {
      for (const auto& facet : manifest->facets) stagingFacets.insert(facet.facetId);
      co_await downloadSnapshot(*manifest, storage, staging, check, [this, objectName](double ms) {
        log("recovery-fsync", "\"object\":" + quote(objectName) + ",\"stage\":\"download\",\"ms\":" + std::to_string(ms));
      });
      check();
      facets = manifest->facets;
      for (const auto& facet : facets) fromSeq = std::max(fromSeq, facet.upToSeq);
    }
  }
  lap(laps.snapshot);
  bool localProof = false;
  KJ_IF_SOME(prepared, preparedLocally) {
    localProof = co_await kj::mv(prepared);
    check();
  }
  if (!localProof) {
    // The staging image stays private while the collector waits for evidence.
    setRebuildPhase(run, RebuildPhase::TAILS);
    run.parkedAtMs = monotonicTimeMs();
    releaseRebuildSlot(run);
  }
  auto tails = co_await kj::mv(collecting);
  if (!localProof) {
    setRebuildPhase(run, RebuildPhase::RESUMING);
    co_await acquireRebuildSlot(objectName, run);
  }
  check();
  run.parkedAtMs = 0;
  lap(laps.tailWait);
  setRebuildPhase(run, RebuildPhase::REPLAY);
  std::vector<LogRecord> history;
  if (manifest) history = co_await readSegments(*manifest, object->namespaceId + "/" + object->objectId, &segmentStats);
  check();
  lap(laps.segments);
  std::set<std::pair<uint64_t, uint64_t>> durableRecords;
  for (const auto& record : history) durableRecords.emplace(record.shardEpoch, record.nodeSeq);
  object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "an object being rebuilt is gone");
  // What no segment covers: this node's own unsealed records for the object, and what the peers of its shard hold
  // of whichever owner wrote last.
  const auto historyName = object->namespaceId + "/" + object->objectId;
  std::set<std::pair<uint64_t, uint64_t>> localUnsealed;
  {
    r47::Span local(r47::RECOVERY_LOCAL_HISTORY);
    size_t decodedCount = 0;
    // The encoded unsealed log is node-wide. Decode only this object's sequences, in the
    // same order as the log, from the same event-loop turn as before. Do not hold a view
    // across an await: selection and its exclusion checks are one read of the local tail.
    if (auto localRecords = unsealedByObject.find(historyName); localRecords != unsealedByObject.end()) {
      for (auto sequence : localRecords->second) {
        if (isolated.contains(sequence)) continue;
        auto encoded = records.find(sequence);
        KJ_DASSERT(encoded != records.end());
        if (encoded == records.end()) continue;
        ++decodedCount;
        auto decoded = decodeLogRecord(encoded->second);
        if (!decoded || decoded->objectName != historyName) continue;
        auto state = recoveryStates.find(hostNameOf(*decoded));
        if (state != recoveryStates.end() && state->second.exclusions.contains({decoded->shardEpoch, decoded->nodeSeq})) continue;
        localUnsealed.emplace(decoded->shardEpoch, decoded->nodeSeq);
        history.push_back(std::move(*decoded));
      }
    }
    local.finish(decodedCount);
  }
  for (const auto& [epoch, taken] : earlier) {
    for (const auto& [sequence, record] : taken) {
      if (record.objectName == historyName) {
        localUnsealed.emplace(record.shardEpoch, record.nodeSeq);
        history.push_back(record);
      }
    }
  }
  lap(laps.local);
  auto latest = co_await readManifest(object->namespaceId, object->objectId);
  check();
  if (latest && (!manifest || latest->revision > manifest->revision)) {
    KJ_REQUIRE(latest->shardEpoch <= object->shardEpoch, "the object was fenced during collection");
    manifest = std::move(latest);
    object = objectOf(objectName);
    KJ_REQUIRE(object != nullptr && object->state == ObjectState::REBUILDING, "recovery lost the object");
    object->manifestRevision = manifest->revision;
    object->recoveryManifestRevision = manifest->revision;
    object->snapshotSequence = manifest->snapshotSequence;
    object->snapshotAtUnixMs = manifest->snapshotAtUnixMs;
    if (manifest->hasSnapshot) {
      for (const auto& facet : manifest->facets) stagingFacets.insert(facet.facetId);
      co_await downloadSnapshot(*manifest, storage, staging, check);
      check();
      facets = manifest->facets;
    }
    auto published = co_await readSegments(*manifest, historyName, &segmentStats);
    check();
    for (auto& record : published) {
      durableRecords.emplace(record.shardEpoch, record.nodeSeq);
      history.push_back(std::move(record));
    }
  }
  lap(laps.latest);
  object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "an object being rebuilt is gone");
  const auto lowerBound = co_await recoveryLowerBound(objectName, manifest, true);
  check();
  std::set<std::pair<uint64_t, uint64_t>> drop;
  uint64_t snapshotShardEpoch = 0;
  if (manifest) {
    for (const auto& entry : manifest->isolated) drop.insert(entry);
    snapshotShardEpoch = manifest->snapshotShardEpoch;
  }
  if (auto state = recoveryStates.find(objectName); state != recoveryStates.end())
    for (const auto& [position, generation] : state->second.exclusions) drop.insert(position);
  std::erase_if(history, [&](const LogRecord& record) {
    const auto position = std::make_pair(record.shardEpoch, record.nodeSeq);
    if (record.shardEpoch >= lowerBound || !localUnsealed.contains(position) || durableRecords.contains(position)) return false;
    noteLateTail(objectName, record, lowerBound);return true;
  });
  std::vector<LogRecord> adopted;
  auto coveredBySnapshot = [&](const LogRecord& record) {
    if (record.shardEpoch < snapshotShardEpoch) return true;
    if (record.shardEpoch != snapshotShardEpoch) return false;
    auto facet = std::find_if(facets.begin(), facets.end(), [&](const ManifestFacet& f) { return f.facetId == record.facetId; });
    return facet != facets.end() && record.nodeSeq <= facet->upToSeq;
  };
  for (const auto& record : history) {
    const auto position = std::make_pair(record.shardEpoch, record.nodeSeq);
    if (localUnsealed.contains(position) && !durableRecords.contains(position) && !drop.contains(position) && !coveredBySnapshot(record))
      adopted.push_back(record);
  }
  auto appendTail = [&](const LogRecord& record) {
    if (record.shardEpoch < lowerBound) { noteLateTail(objectName, record, lowerBound);return; }
    const auto position = std::make_pair(record.shardEpoch, record.nodeSeq);
    if (drop.contains(position)) return;
    bool covered = record.shardEpoch < snapshotShardEpoch;
    if (!covered && record.shardEpoch == snapshotShardEpoch) {
      auto facet = std::find_if(facets.begin(), facets.end(), [&](const ManifestFacet& f) {
        return f.facetId == record.facetId;
      });
      covered = facet != facets.end() && record.nodeSeq <= facet->upToSeq;
    }
    if (covered) {
      if (record.shardEpoch < snapshotShardEpoch) {
        noteLateTail(objectName, record, snapshotShardEpoch);
      }
      return;
    }
    if (!durableRecords.contains(position)) adopted.push_back(record);
    history.push_back(record);
  };
  for (const auto& source : tails.sources) {
    auto entry = object->recoveryTails.lower_bound({source.shardEpoch, 0});
    for (; entry != object->recoveryTails.end() && entry->first.first == source.shardEpoch; ++entry) {
      const auto& tail = entry->second;
      if (tail.record.nodeEpoch != source.nodeEpoch || (source.ownerComplete && tail.ownerProof != generation.second)) continue;
      appendTail(tail.record);
    }
  }
  // Finish and synchronize the new image before touching any original file or facet entry.
  for (const auto& record : history) stagingFacets.insert(record.facetId);
  lap(laps.prepare);
  auto rebuilt = co_await replayToAsync(storage, objectName, targetEpoch, staging, std::move(history), facets,
      snapshotShardEpoch, drop, {}, check, [this, objectName](double ms) {
        log("recovery-fsync", "\"object\":" + quote(objectName) + ",\"stage\":\"replay\",\"ms\":" + std::to_string(ms));
      });
  check();
  lap(laps.replay);
  KJ_REQUIRE(!recoveryWriteFailed, "a held or unfenced local image cannot be deleted");
  object->facets.clear();
  auto removeFile = [&](const std::filesystem::path& path) {
    std::error_code code;
    std::filesystem::remove(path, code);
    KJ_REQUIRE(!code, "cannot remove an old Durable Object file during replacement",
        path.string().c_str(), code.message().c_str());
  };
  std::set<uint32_t> sidecars;
  for (uint32_t id = 0; id < 64; ++id) sidecars.insert(id);
  for (const auto& [id, facet] : rebuilt) sidecars.insert(id);
  // No new database moves until every old WAL and shared-memory file is gone.
  for (auto id : sidecars)
    for (const char* suffix : {"-wal", "-shm"}) removeFile(databasePath(*object, id, suffix));
  for (const auto& [id, facet] : rebuilt) {
    std::error_code code;
    std::filesystem::rename(databasePath(storage, id, "", staging), databasePath(*object, id), code);
    KJ_REQUIRE(!code, "a completed recovery image could not replace the cache", id, code.message().c_str());
  }
  for (uint32_t id = 0; id < 64; ++id)
    if (!rebuilt.contains(id)) removeFile(databasePath(*object, id));
  std::vector<ManifestFacet> index;
  for (const auto& [id, facet] : rebuilt) {
    ManifestFacet entry;
    entry.facetId = id; entry.parentFacetId = facet.parentFacetId; entry.name = facet.facetName;
    index.push_back(std::move(entry));
  }
  writeFacetIndex(*object, index);
  object->facets = std::move(rebuilt);
  dirtyCacheSizes.insert(objectName);
  lap(laps.files);
  const auto publishingStarted = monotonicTimeMs();
  if (tails.cut) co_await forceRecoverySnapshot(objectName);
  else co_await publishRecovery(objectName, adopted);
  check();
  lap(laps.publish);
  std::vector<RecoveryEvent> completed;
  completed.push_back(recoveryEvent(RecoveryKind::PUBLISHED, *object, object->rebuildGeneration));
  completed.push_back(recoveryEvent(RecoveryKind::REBUILD_DONE, *object, object->rebuildGeneration));
  if (auto state = recoveryStates.find(objectName); state != recoveryStates.end() && state->second.healing)
    completed.push_back(recoveryEvent(RecoveryKind::HEAL_DONE, *object, object->rebuildGeneration));
  if (object->repairGeneration.first) {
    completed.push_back(recoveryEvent(RecoveryKind::REPAIR_DONE, *object, object->repairGeneration));
    object->imageRepairGeneration = object->repairGeneration;
  }
  KJ_REQUIRE(appendRecovery(completed, false), "the recovery completion marker could not be written");
  object->recoveryPublished = true;
  object->cacheOpen = false;object->repairGeneration = {};
  std::set<std::pair<uint64_t, uint64_t>> publishedPositions = durableRecords;
  for (const auto& record : adopted) publishedPositions.emplace(record.shardEpoch, record.nodeSeq);
  auto retire = [&](auto& epochs, bool indexed) {
    for (auto epoch = epochs.begin(); epoch != epochs.end();) {
      std::erase_if(epoch->second, [&](const auto& entry) {
        const auto& record = entry.second;
        if (record.objectName != historyName || (!publishedPositions.contains({record.shardEpoch, record.nodeSeq}) &&
            !drop.contains({record.shardEpoch, record.nodeSeq}) && record.shardEpoch >= lowerBound && !coveredBySnapshot(record))) return false;
        noteEarlierRetired(epoch->first, record);
        if (indexed) unindexEarlierRecord({epoch->first, entry.first}, record);
        return true;
      });
      if (epoch->second.empty()) epoch = epochs.erase(epoch);else ++epoch;
    }
  };
  retire(earlier, true);retire(earlierExcluded, false);persistEarlierSeals();
  log("recovery-published", "\"object\":" + quote(objectName) + ",\"collectionMs\":" +
      std::to_string(tails.collectionMs) + ",\"writeMs\":" +
      std::to_string(monotonicTimeMs() - publishingStarted) + ",\"cut\":" + (tails.cut ? "true" : "false") +
      ",\"adoptedRecords\":" + std::to_string(adopted.size()));
  object->state = ObjectState::LOCAL;
  object->recoveryStartedMs = 0;
  object->recoveryTails.clear();
  object->recoveryManifestRevision.reset();
  object->lateTails.clear();
  object->repairReason.clear();
  ++rebuilds;
  ++object->rebuilds;
  lastRebuildMs = monotonicTimeMs() - start;
  lastRebuildLoopDelayMs = *loopDelay;
  object->lastRebuildMs = lastRebuildMs;
  writePosition(*object);
  lap(laps.markers);
  log("rebuilt", "\"object\":" + quote(objectName) + ",\"ms\":" + std::to_string(lastRebuildMs) + ",\"manifest\":\"" +
          std::to_string(object->manifestRevision) + "\",\"snapshot\":\"" + std::to_string(object->snapshotSequence) +
          "\",\"fromSeq\":\"" + std::to_string(fromSeq) + "\",\"queueMs\":" + std::to_string(run.queuedMs) +
          ",\"loopDelayMs\":" + std::to_string(lastRebuildLoopDelayMs) +
          ",\"laps\":{\"setupMs\":" + std::to_string(laps.setup) + ",\"manifestMs\":" + std::to_string(laps.manifest) +
          ",\"snapshotMs\":" + std::to_string(laps.snapshot) + ",\"segmentsMs\":" + std::to_string(laps.segments) +
          ",\"localMs\":" + std::to_string(laps.local) + ",\"tailWaitMs\":" + std::to_string(laps.tailWait) +
          ",\"latestMs\":" + std::to_string(laps.latest) + ",\"prepareMs\":" + std::to_string(laps.prepare) +
          ",\"replayMs\":" + std::to_string(laps.replay) + ",\"filesMs\":" + std::to_string(laps.files) +
          ",\"publishMs\":" + std::to_string(laps.publish) + ",\"markersMs\":" + std::to_string(laps.markers) +
          "},\"tailCollectionMs\":" + std::to_string(tails.collectionMs) +
          ",\"segments\":{\"read\":" + std::to_string(segmentStats.segments) + ",\"bytes\":" + std::to_string(segmentStats.bytes) +
          ",\"records\":" + std::to_string(segmentStats.records) + ",\"hits\":" + std::to_string(segmentStats.cacheHits) +
          ",\"misses\":" + std::to_string(segmentStats.cacheMisses) + ",\"joins\":" + std::to_string(segmentStats.cacheJoins) +
          ",\"downloadMs\":" + std::to_string(segmentStats.downloadMs) + ",\"decodeMs\":" + std::to_string(segmentStats.decodeMs) +
          ",\"groupMs\":" + std::to_string(segmentStats.groupMs) + ",\"waitMs\":" + std::to_string(segmentStats.waitMs) +
          ",\"selectMs\":" + std::to_string(segmentStats.selectMs) + "}");
}

// A snapshot's parts, at most durableObjects.recoverParallelism at a time (a volume server of SeaweedFS 3.97
// answered 7 of 1000 reads with an error at 32 at once), each written where it belongs in its facet's file.
kj::Promise<void> DurableLog::downloadSnapshot(const ObjectManifest& manifest, const RootStorageRef& ref,
    const std::string& staging, std::function<void()> check, std::function<void(double)> observeFsync) {
  return downloadRecoverySnapshot([this](std::string key) { return store->get(std::move(key)); }, manifest,
      ref, settings.stackDirectory, staging, settings.config.snapshotPartBytes, settings.config.recoverParallelism,
      std::move(check), [this]() { return timer.afterDelay(0 * kj::MILLISECONDS); }, std::move(observeFsync));
}

kj::Promise<void> DurableLog::downloadSnapshotPart(const ObjectManifest& manifest, uint32_t facetId, uint64_t part,
    int fd, off_t offset) {
  return downloadRecoveryPart([this](std::string key) { return store->get(std::move(key)); }, manifest, facetId,
      part, fd, offset);
}

// The segments a manifest names, in order, at most recoverParallelism downloads at a time; only the records of this
// object are kept.
// Each segment is downloaded and decoded once per node, off the event loop, and shared by the rebuilds that name it
// (previously, one segment of a restarted owner's earlier epoch held every object of the shard, and each object's
// rebuild decoded all of it on the loop again).
kj::Promise<std::vector<LogRecord>> DurableLog::readSegments(const ObjectManifest& manifest,
    const std::string& objectName, SegmentReadStats* stats) {
  if (!sweepingSegments) { sweepingSegments = true; tasks.add(sweepSegmentCache()); }
  return readRecoverySegmentsDecoded(*segmentCache, [this](std::string key) { return store->get(std::move(key)); },
      manifest, objectName, settings.config.recoverParallelism,
      [this](const std::string& event, const std::string& fields) { log(event, fields); }, stats);
}

kj::Promise<void> DurableLog::sweepSegmentCache() {
  do co_await timer.afterDelay(SEGMENT_CACHE_SWEEP_MS * kj::MILLISECONDS);
  while (segmentCache->sweep());
  sweepingSegments = false;
}

void DurableLog::noteLateTail(const std::string& objectName, const LogRecord& record, uint64_t cutEpoch) {
  auto* object = objectOf(objectName);
  if (object == nullptr || !object->lateTails.emplace(record.nodeEpoch, record.nodeSeq).second) return;
  ++lateTailRecords;
  log("late-tail-before-snapshot", "\"object\":" + quote(objectName) + ",\"sourceEpoch\":\"" +
      std::to_string(record.shardEpoch) + "\",\"nodeEpoch\":\"" + std::to_string(record.nodeEpoch) +
      "\",\"nodeSeq\":\"" + std::to_string(record.nodeSeq) + "\",\"facet\":" +
      std::to_string(record.facetId) + ",\"cutEpoch\":\"" + std::to_string(cutEpoch) +
      "\",\"sha256\":" + quote(sha256Hex(encodeLogRecord(record))));
}

// Collect each source epoch from its original owner and all known holder addresses.
// Only the original process can prove completeness; otherwise publish a cut at the deadline.
kj::Promise<ObjectResponse> DurableLog::readRecoveryMetadata(std::string key, uint32_t timeoutMs) {
  auto permit = co_await recoveryMetadata.acquire();
  co_return co_await timer.timeoutAfter(timeoutMs * kj::MILLISECONDS, store->get(std::move(key)));
}

kj::Promise<std::shared_ptr<const ObjectResponse>> DurableLog::readRecoveryEpoch(std::string key, bool preparingLocally) {
  constexpr size_t MAX_CACHED_EPOCHS = 1024;
  const auto now = monotonicTimeMs();
  if (auto found = recoveryEpochReads.find(key); found != recoveryEpochReads.end()) {
    if (*found->second.freshUntilMs == 0 || now < *found->second.freshUntilMs)
      return found->second.answer.addBranch();
    recoveryEpochReads.erase(found);
  }
  if (recoveryEpochReads.size() >= MAX_CACHED_EPOCHS) {
    auto settled = std::find_if(recoveryEpochReads.begin(), recoveryEpochReads.end(), [](const auto& item) {
      return *item.second.freshUntilMs != 0;
    });
    if (settled != recoveryEpochReads.end()) recoveryEpochReads.erase(settled);
  }
  auto read = [](DurableLog& self, std::string key, std::shared_ptr<double> freshUntil, bool preparingLocally)
      -> kj::Promise<std::shared_ptr<const ObjectResponse>> {
    const auto markFailure = kj::defer([&]() {
      if (*freshUntil == 0) *freshUntil = monotonicTimeMs() + RECOVERY_WAKE_INTERVAL / kj::MILLISECONDS;
    });
    const auto limit = std::max(self.settings.peerTimeoutMs, self.settings.config.confirmTimeoutMs);
    ObjectResponse result;
    if (preparingLocally) {
      result = co_await self.timer.timeoutAfter(limit * kj::MILLISECONDS, self.store->get(std::move(key)));
    } else {
      result = co_await self.readRecoveryMetadata(std::move(key), limit);
    }
    requireStorage(result.status == 200 && result.bytes.size() <= 4096, "a source shard epoch is unavailable");
    *freshUntil = monotonicTimeMs() + 60000;
    co_return std::make_shared<const ObjectResponse>(std::move(result));
  };
  auto freshUntil = std::make_shared<double>(0);
  if (recoveryEpochReads.size() >= MAX_CACHED_EPOCHS)
    return read(*this, std::move(key), std::move(freshUntil), preparingLocally);
  auto fork = read(*this, key, freshUntil, preparingLocally).fork();
  auto branch = fork.addBranch();
  recoveryEpochReads.emplace(std::move(key), EpochRead{kj::mv(fork), std::move(freshUntil)});
  return branch;
}

kj::Promise<void> DurableLog::recoveryWake() {
  if (!recoveryWakeTick || timer.now() >= recoveryWakeAt) {
    recoveryWakeAt = timer.now() + RECOVERY_WAKE_INTERVAL;
    ++recoveryRound;
    recoveryWakeTick.emplace(timer.atTime(recoveryWakeAt).fork());
  }
  return recoveryWakeTick->addBranch();
}

kj::Promise<std::shared_ptr<const DurableLog::TailDiscovery>> DurableLog::tailTargets() {
  const auto now = monotonicTimeMs();
  if (!recoveryDiscovery || (!recoveryDiscovering && now - recoveryDiscoveryAtMs >= OWNER_REREAD_MS)) {
    recoveryDiscovering = true;
    recoveryDiscovery.emplace(discoverTailTargets().fork());
  }
  return recoveryDiscovery->addBranch();
}

kj::Promise<std::shared_ptr<const DurableLog::TailDiscovery>> DurableLog::discoverTailTargets() {
  auto state = lastTailDiscovery ? std::make_shared<TailDiscovery>(*lastTailDiscovery) : std::make_shared<TailDiscovery>();
  state->complete = false;
  for (auto& [key, target] : state->addresses) target.timeVerified = false;
  const auto settled = kj::defer([this]() {
    recoveryDiscoveryAtMs = monotonicTimeMs();
    recoveryDiscovering = false;
  });
  const auto budget = std::max<uint32_t>(1, settings.peerTimeoutMs);
  try {
    ObjectListing listing;
    std::string token;
    do {
      auto permit = co_await recoveryMetadata.acquire();
      auto page = co_await timer.timeoutAfter(budget * kj::MILLISECONDS, store->listPage(MEMBERS_PREFIX, 1000, token));
      if (++listing.pages == 1) listing.dateUnixMs = page.dateUnixMs;
      for (auto& entry : page.objects) listing.objects.push_back(std::move(entry));
      token = std::move(page.nextToken);
    } while (!token.empty());
    const auto listed = readMemberListing(listing);
    std::map<std::pair<std::string, uint64_t>, MemberRecord> values;
    std::map<std::pair<std::string, uint64_t>, MemberRecord> registrations;
    for (const auto& [id, node] : listed) {
      if (id == settings.nodeId) continue;
      for (const auto& [number, value] : node.epochs) {
        if (!value.registered && !value.heartbeat) continue;
        const auto key = value.heartbeat ? memberHeartbeatKey(id, number) : memberRegistrationKey(id, number);
        auto response = co_await readRecoveryMetadata(key, budget);
        auto record = response.status == 200 ? parseMemberRecord(response.bytes) : std::nullopt;
        if (!record || record->nodeId != id || record->epoch != number) {
          state->unresolved.insert(key);continue;
        }
        if (!value.heartbeat && value.registered) registrations.emplace(std::make_pair(id, number), *record);
        values.emplace(std::make_pair(id, number), std::move(*record));
      }
    }
    // All addresses found in this bounded round get a turn, even when the
    // wait deadline passed while reading metadata. Failed rounds retain them.
    state->complete = true;
    state->unresolved.erase("member-discovery");
    for (const auto& [id, node] : listed) {
      if (id == settings.nodeId) continue;
      for (const auto& [number, observed] : node.epochs) {
        auto found = values.find({id, number});
        if (found == values.end()) continue;
        const auto& record = found->second;
        const auto key = observed.heartbeat ? memberHeartbeatKey(id, number) : memberRegistrationKey(id, number);
        DurableTailTarget target{id, record.address, std::to_string(number), record.instance};
        target.heartbeatUnixMs = observed.heartbeat ? observed.heartbeatUnixMs : 0;
        target.registeredUnixMs = observed.registered ? observed.registeredUnixMs : 0;
        target.timeVerified = true;target.durableProcesses = record.durableProcesses;
        if (!observed.heartbeat && !record.instance.empty()) {
          for (auto successor = node.epochs.upper_bound(number); successor != node.epochs.end(); ++successor) {
            if (!successor->second.registered || !successor->second.registeredUnixMs) continue;
            const auto identity = std::make_pair(id, successor->first);
            auto newer = registrations.find(identity);
            if (newer == registrations.end()) {
              const auto registrationKey = memberRegistrationKey(id, successor->first);
              auto response = co_await readRecoveryMetadata(registrationKey, budget);
              auto value = response.status == 200 ? parseMemberRecord(response.bytes) : std::nullopt;
              if (!value || value->nodeId != id || value->epoch != successor->first) {
                state->unresolved.insert(registrationKey);continue;
              }
              state->unresolved.erase(registrationKey);
              newer = registrations.emplace(identity, std::move(*value)).first;
            }
            if (!newer->second.instance.empty() &&
                newer->second.instance != record.instance && successor->second.registeredUnixMs) {
              target.successorRegisteredUnixMs = successor->second.registeredUnixMs;break;
            }
          }
        }
        if (record.address.empty()) { state->unresolved.insert(key);continue; }
        state->addresses[target.key()] = std::move(target);
        state->unresolved.erase(key);
      }
    }
  } catch (const kj::Exception& error) {
    state->complete = false;state->unresolved.insert("member-discovery");
    log("tail-discovery-failed", "\"error\":" + quote(error.getDescription().cStr()));
  }
  lastTailDiscovery = state;
  co_return state;
}

kj::Promise<DurableLog::TailRecovery> DurableLog::fetchTails(
    ObjectIdentity identity, const std::optional<ObjectManifest>& manifest, double startedMs, RebuildRun& run,
    kj::Own<kj::PromiseFulfiller<bool>> localPreparation) {
  auto finishLocalPreparation = [&](bool complete) {
    if (!localPreparation) return;
    localPreparation->fulfill(kj::mv(complete));
    localPreparation = nullptr;
  };
  const auto preparationDone = kj::defer([&]() { finishLocalPreparation(false); });
  const auto name = durableHostName(identity.namespaceId, identity.objectId, identity.dataEpoch);
  const auto generation = objectOf(name)->rebuildGeneration;
  auto checkIdentity = [&]() {
    auto* current = objectOf(name);
    KJ_REQUIRE(current != nullptr && current->state == ObjectState::REBUILDING &&
        current->rebuildGeneration == generation && current->shardEpoch == identity.shardEpoch &&
        current->dataEpoch == identity.dataEpoch && !ownerFenced(identity.namespaceId, identity.shard, identity.shardEpoch),
        "the object changed ownership during tail recovery");
  };
  if (!run.reserved) setRebuildPhase(run, RebuildPhase::TAILS);
  const auto snapshotShardEpoch = co_await recoveryLowerBound(name, manifest, localPreparation.get() != nullptr);
  checkIdentity();
  const auto deadline = startedMs + settings.tailWaitMs;
  const auto wanted = identity.namespaceId + "/" + identity.objectId;
  using Position = std::pair<uint64_t, uint64_t>;
  struct Probe {
    DurableTailTarget target;
    uint64_t after = 0;
    bool complete = false, gone = false, irrelevant = false;
    std::string error;
  };
  struct Source {
    ShardEpoch epoch;
    uint64_t acquiredAtUnixMs = 0;
    std::map<std::string, Probe> probes;
    bool ownerComplete = false;
  };
  struct State {
    std::vector<Source> sources;
    std::map<std::string, DurableTailTarget> addresses;
    std::shared_ptr<const TailDiscovery> discovery;
  } state;
  // Epoch objects are authority, not hints. A failed read is a storage failure,
  // never permission to treat the history as empty and publish a cut.
  auto epoch = identity.shardEpoch;
  std::set<uint64_t> visited;
  while (epoch && (!snapshotShardEpoch || epoch >= snapshotShardEpoch)) {
    KJ_REQUIRE(visited.insert(epoch).second, "a shard's previous-epoch chain cycles");
    const auto response = co_await readRecoveryEpoch(shardEpochKey(identity.namespaceId, identity.shard, epoch),
        localPreparation.get() != nullptr);
    checkIdentity();
    auto record = parseShardEpoch(response->bytes);
    requireStorage(record && record->epoch == epoch && record->namespaceId == identity.namespaceId &&
        record->shard == identity.shard, "a source shard epoch is invalid during recovery");
    chargeRebuildMetadata(run, 512 + response->bytes.size());
    Source source;
    source.epoch = *record;
    source.acquiredAtUnixMs = response->lastModifiedUnixMs;
    if (record->nodeId == settings.nodeId && record->nodeEpoch != nodeEpoch &&
        !membership.heldEpochs.contains(std::to_string(record->nodeEpoch))) {
      auto prior = predecessorLogs.find(record->nodeEpoch);
      if (diskTrusted && sameBootCache && prior != predecessorLogs.end() && prior->second.complete) {
        source.ownerComplete = true;
        size_t retainedCount = 0;
        const bool has = prior->second.startupUnsealed.contains({wanted, record->epoch});
        // This immutable raw-log observation is scoped to namespace+objectId,
        // independent of dataEpoch. Every unexcluded byte is a possible write.
        if (has) {
          if (auto held = earlier.find(record->nodeEpoch); held != earlier.end())
            for (const auto& [sequence, entry] : held->second) {
              if (entry.objectName != wanted || entry.shardEpoch != record->epoch) continue;
              const Position position{entry.shardEpoch, entry.nodeSeq};
              objectOf(name)->recoveryTails.insert_or_assign(position, Object::RetainedTail{entry, generation.second});
              ++retainedCount;
            }
        } else ++localLogEmptyClosures;
        log(has ? "local-owner-log-complete" : "local-owner-log-empty", "\"object\":" + quote(name) +
            ",\"sourceEpoch\":\"" + std::to_string(record->epoch) + "\",\"sourceNodeEpoch\":\"" +
            std::to_string(record->nodeEpoch) + "\",\"records\":" + std::to_string(retainedCount));
      }
    }
    if (record->nodeId != settings.nodeId || (!source.ownerComplete && record->nodeEpoch != nodeEpoch &&
        !membership.heldEpochs.contains(std::to_string(record->nodeEpoch)))) finishLocalPreparation(false);
    state.sources.push_back(std::move(source));
    epoch = record->previousEpoch;
  }
  auto remember = [&](DurableTailTarget target, bool replace = false) {
    auto key = target.key();
    auto found = state.addresses.find(key);
    if (found == state.addresses.end()) {
      chargeRebuildMetadata(run, 256 + key.size() + target.nodeId.size() + target.address.size() +
          target.destinationEpoch.size() + target.instance.size());
      state.addresses.emplace(std::move(key), std::move(target));
    } else if (replace) {
      found->second = std::move(target);
    }
  };
  auto refreshMemory = [&]() {
    auto member = [&](const Member& entry) {
      if (entry.id != settings.nodeId && entry.durableProcesses && !entry.address.empty() && !entry.epoch.empty())
        remember({entry.id, entry.address, entry.epoch, entry.instance});
    };
    for (const auto& [key, entry] : membership.recoveryMembers) member(entry);
    for (const auto& [id, entry] : membership.members) member(entry);
    remember({settings.nodeId, "local", std::to_string(nodeEpoch), settings.instance});
  };
  auto addDiscovery = [&]() -> kj::Promise<void> {
    auto found = co_await tailTargets();
    checkIdentity();
    if (found == state.discovery) co_return;
    for (auto& [key, target] : state.addresses) target.timeVerified = false;
    for (const auto& [key, target] : found->addresses) {
      for (auto& [priorKey, prior] : state.addresses) {
        if (prior.nodeId != target.nodeId || prior.destinationEpoch != target.destinationEpoch) continue;
        prior.heartbeatUnixMs = target.heartbeatUnixMs;
        prior.registeredUnixMs = target.registeredUnixMs;
        prior.successorRegisteredUnixMs = target.successorRegisteredUnixMs;
        prior.durableProcesses = target.durableProcesses;
        prior.timeVerified = true;
      }
      remember(target, true);
    }
    state.discovery = std::move(found);
  };
  auto allOwners = [&]() {
    return std::all_of(state.sources.begin(), state.sources.end(),
        [](const Source& source) { return source.ownerComplete; });
  };
  // A page has no size preface in this protocol. Budget the largest legal page
  // at an aggregate 8 MiB/s shared by the transfer slots.
  // It is never shortened by the 25-second missing-holder wait.
  const auto slots = std::max<uint32_t>(1, settings.config.recoverParallelism);
  const auto pageBudget = uint32_t(std::max<double>(settings.config.confirmTimeoutMs,
      settings.peerTimeoutMs + std::ceil(double(tailRecordBytes() + TAIL_RPC_HEADROOM_BYTES) * slots * 1000 / (8u << 20))));
  const auto drainMs = std::ceil(double(std::max<uint64_t>(settings.config.peerBufferBytes,
      tailRecordBytes())) * slots * 1000 / (8u << 20));
  // Connect before taking a transfer slot. Requeue each page so one busy source
  // cannot delay other healthy targets or monopolize its own target queue.
  auto query = [&](size_t index, std::string key, bool owner, DurableTailRound round) -> kj::Promise<void> {
    auto& source = state.sources[index];
    auto& probe = source.probes.at(key);
    if (probe.complete || probe.gone) co_return;
    try {
      std::shared_ptr<DurableTailConnection> connection;
      // Queueing and reconnecting use their own budgets. A probe may drain a
      // full configured peer buffer, and its last page gets a full page budget.
      double transferringMs = 0;
      for (;;) {
        if (transferringMs >= drainMs) co_return;
        checkIdentity();
        DurableTailAsk ask;
        ask.namespaceId = identity.namespaceId;
        ask.shard = identity.shard;
        ask.shardEpoch = identity.shardEpoch;
        ask.ownerNodeId = source.epoch.nodeId;
        ask.ownerNodeEpoch = source.epoch.nodeEpoch;
        ask.afterSeq = probe.after;
        ask.objectId = identity.objectId;
        DurableTailAnswer answer;
        if (probe.target.nodeId == settings.nodeId) {
          std::string localRefusal;
          if (!membership.heldEpochs.contains(probe.target.destinationEpoch) &&
              probe.target.destinationEpoch != std::to_string(nodeEpoch) &&
              !canAnswerRestoredTail(ask, probe.target.destinationEpoch, &localRefusal)) {
            probe.gone = true;
            probe.error = localRefusal.empty() ? "this process does not hold the destination epoch" : localRefusal;
            co_return;
          }
          if (owner && source.epoch.nodeId == settings.nodeId &&
              !membership.heldEpochs.contains(std::to_string(source.epoch.nodeEpoch))) {
            auto prior = predecessorLogs.find(source.epoch.nodeEpoch);
            if (prior != predecessorLogs.end() && prior->second.startupUnsealed.contains({wanted, source.epoch.epoch})) {
              probe.error = "the local restored object requires a published recovery cut";
              co_return;
            }
          }
          answer = tailFor(ask, false);
        } else {
          if (!connection && hooks.connectTail) {
            connection = co_await hooks.connectTail(probe.target, round);
            KJ_REQUIRE(bool(connection) && connection->target.key() == probe.target.key(), "a tail connection changed identity");
          }
          auto permit = co_await tailPages.acquire(probe.target.key());
          checkIdentity();
          if (connection && connection->usable && !connection->usable()) {
            if (!round.final) KJ_FAIL_REQUIRE("the tail connection failed earlier in this round");
            // A different object's failure says nothing about this object's
            // history. Return the page slot and join a replacement connection.
            connection.reset();
            continue;
          }
          KJ_REQUIRE(bool(connection) || bool(hooks.fetchTail), "tail transport is not installed");
          const auto transferAt = monotonicTimeMs();
          if (owner && run.owner == probe.target.nodeId) run.probeAtMs = transferAt;
          ++tailFetches;
          auto request = connection ? connection->page(ask) : hooks.fetchTail(probe.target.nodeId, ask);
          auto timeout = timer.afterDelay(pageBudget * kj::MILLISECONDS).then([connection]() -> DurableTailAnswer {
            if (connection && connection->failed) connection->failed();
            KJ_FAIL_REQUIRE("a tail page did not arrive within its transfer budget");
          });
          try {
            answer = co_await kj::mv(request).exclusiveJoin(kj::mv(timeout));
          } catch (const kj::Exception& error) {
            if (connection && connection->failed && error.getType() == kj::Exception::Type::DISCONNECTED)
              connection->failed();
            throw;
          }
          transferringMs += monotonicTimeMs() - transferAt;
        }
        checkIdentity();
        if (!answer.refusal.empty()) {
          probe.error = answer.refusal;
          if (answer.refusal.find("does not hold the epoch") != std::string::npos) probe.gone = true;
          log("tail-refused", "\"object\":" + quote(name) + ",\"peer\":" +
              quote(probe.target.nodeId) + ",\"source\":" + quote(source.epoch.nodeId) +
              ",\"sourceEpoch\":\"" + std::to_string(source.epoch.epoch) +
              "\",\"reason\":" + quote(answer.refusal));
          co_return;
        }
        KJ_REQUIRE(answer.count == answer.records.size(), "a tail page's count is inconsistent");
        const bool ownerProof = owner && answer.ownerConfirmed;
        uint64_t last = probe.after;
        uint32_t mine = 0;
        std::map<Position, std::string> pageBytes;
        for (const auto& record : answer.records) {
          KJ_REQUIRE(record.nodeEpoch == ask.ownerNodeEpoch && record.nodeSeq > ask.afterSeq &&
              record.shard == identity.shard && record.objectName.starts_with(identity.namespaceId + "/"),
              "a tail page contains records outside its source");
          last = std::max(last, record.nodeSeq);
          if (record.objectName != wanted || record.shardEpoch != source.epoch.epoch) continue;
          ++mine;
          const Position position{record.shardEpoch, record.nodeSeq};
          const auto encoded = encodeLogRecord(record);
          auto [page, added] = pageBytes.emplace(position, encoded);
          KJ_REQUIRE(added || page->second == encoded, "one tail page repeats an identity with different bytes");
          const auto& retained = objectOf(name)->recoveryTails;
          auto held = retained.find(position);
          KJ_REQUIRE(held == retained.end() || encodeLogRecord(held->second.record) == encoded,
              "different bytes claim one tail record identity");
        }
        KJ_REQUIRE(!answer.more || last > probe.after, "a tail page makes no cursor progress");
        KJ_REQUIRE(answer.records.empty() || answer.lastSeq == last, "a tail page's last sequence is inconsistent");
        // Commit the page only after every record and the cursor are validated.
        for (auto& record : answer.records) {
          if (record.objectName == wanted && record.shardEpoch < snapshotShardEpoch) {
            noteLateTail(name, record, snapshotShardEpoch);
            continue;
          }
          if (record.objectName != wanted || record.shardEpoch != source.epoch.epoch) continue;
          const Position position{record.shardEpoch, record.nodeSeq};
          auto* live = objectOf(name);
          KJ_REQUIRE(live != nullptr, "recovery lost its retained tail storage");
          auto [retained, added] = live->recoveryTails.try_emplace(position, Object::RetainedTail{std::move(record)});
          if (ownerProof) retained->second.ownerProof = generation.second;
        }
        log(probe.target.nodeId == settings.nodeId ? "tail-local" : "tail-taken",
            "\"object\":" + quote(name) + ",\"peer\":" + quote(probe.target.nodeId) +
            ",\"source\":" + quote(source.epoch.nodeId) + ",\"sourceEpoch\":\"" +
            std::to_string(source.epoch.epoch) + "\",\"records\":" + std::to_string(answer.count) +
            ",\"ours\":" + std::to_string(mine) + ",\"lastSeq\":\"" + std::to_string(last) +
            "\",\"more\":" + (answer.more ? "true" : "false") + ",\"ownerProof\":" + (ownerProof ? "true" : "false"));
        probe.after = last;
        probe.error.clear();
        if (!answer.more) {
          probe.complete = true;
          if (ownerProof) source.ownerComplete = true;
          co_return;
        }
      }
    } catch (const kj::Exception& error) {
      checkIdentity();  // Ownership cancellation is not an individual peer failure.
      if (error.getDetail(REBUILD_METADATA_FULL) != kj::none) throw;
      const std::string description = error.getDescription().cStr();
      if (probe.error == description) co_return;
      probe.error = description.substr(0, 512);
      log("tail-failed", "\"object\":" + quote(name) + ",\"peer\":" +
          quote(probe.target.nodeId) + ",\"source\":" + quote(source.epoch.nodeId) +
          ",\"error\":" + quote(probe.error));
    }
  };
  const auto marginMs = uint64_t(membership.grantedTtlSeconds) * 1000 +
      membership.livenessGraceMs + membership.listIntervalMs;
  auto isCandidate = [&](const DurableTailTarget& target, const Source& source) {
    if (target.durableProcesses == 0) return false;
    if (!state.discovery || !state.discovery->complete || !target.timeVerified || !source.acquiredAtUnixMs ||
        !membership.grantedTtlSeconds) return true;
    auto before = [&](uint64_t at) {
      return at && at < source.acquiredAtUnixMs && source.acquiredAtUnixMs - at > marginMs;
    };
    if (target.heartbeatUnixMs) return !before(target.heartbeatUnixMs);
    // A registration alone is not a death proof. A different process must have
    // registered before the entire margin preceding this source's acquisition.
    return !(before(target.registeredUnixMs) && before(target.successorRegisteredUnixMs));
  };
  CutHold cutHold;
  // A held cut that ends without this decision: the old owner answered in full, or the ownership changed.
  const char* cutHoldEnd = "ownership changed";
  const auto cutHoldEnded = kj::defer([&]() {
    if (cutHold.holding)
      log("cut-hold-ended", "\"object\":" + quote(name) + ",\"reason\":" + quote(cutHoldEnd) +
          ",\"waitedMs\":" + std::to_string(uint64_t(monotonicTimeMs() - cutHold.sinceMs)));
  });
  auto mayCut = [&]() -> kj::Promise<bool> {
    std::vector<std::pair<std::string, uint64_t>> silent;
    for (const auto& source : state.sources)
      if (!source.ownerComplete) silent.emplace_back(source.epoch.nodeId, source.epoch.nodeEpoch);
    const auto cut = co_await previousOwnersMayBeCut(name, std::move(silent), cutHold, run);
    checkIdentity();
    co_return cut;
  };
  auto addProbe = [&](Source& source, const std::string& key, const DurableTailTarget& target) -> Probe& {
    auto found = source.probes.find(key);
    if (found != source.probes.end()) return found->second;
    chargeRebuildMetadata(run, 512 + key.size() + target.nodeId.size() + target.address.size() +
        target.destinationEpoch.size() + target.instance.size());
    return source.probes.emplace(key, Probe{target}).first->second;
  };
  for (;;) {
    checkIdentity();
    if (allOwners()) break;
    refreshMemory();
    const bool atDeadline = monotonicTimeMs() >= deadline;
    if (atDeadline && !state.discovery) {
      finishLocalPreparation(false);
      co_await addDiscovery();
    }
    const bool finalRound = atDeadline && (co_await mayCut());
    if (!run.reserved) setRebuildPhase(run, finalRound ? RebuildPhase::FINAL_TAILS :
        atDeadline ? RebuildPhase::OWNER : RebuildPhase::TAILS);
    const DurableTailRound round{recoveryRound, finalRound};
    kj::Vector<kj::Promise<void>> ownerCalls;
    kj::Vector<kj::Promise<void>> peerCalls;
    for (size_t i = 0; i < state.sources.size(); ++i) {
      auto& source = state.sources[i];
      if (source.ownerComplete) continue;
      if (run.owner.empty() && (source.epoch.nodeId != settings.nodeId || source.epoch.nodeEpoch != nodeEpoch)) {
        run.owner = source.epoch.nodeId;
        run.ownerEpoch = source.epoch.nodeEpoch;
      }
      std::optional<DurableTailTarget> ownerTarget;
      int best = -1;
      for (const auto& [key, target] : state.addresses) {
        if (target.nodeId != source.epoch.nodeId) continue;
        int score = target.instance == source.epoch.instance ? 2 : 0;
        if (target.destinationEpoch == std::to_string(source.epoch.nodeEpoch)) score += 4;
        if (target.nodeId == settings.nodeId && source.epoch.instance == settings.instance) score += 8;
        if (score > best) { ownerTarget = target;best = score; }
      }
      if (ownerTarget) {
        ownerTarget->destinationEpoch = std::to_string(source.epoch.nodeEpoch);
        const auto key = "owner/" + ownerTarget->address;
        auto& probe = addProbe(source, key, *ownerTarget);
        if (finalRound) probe.complete = false;
        ownerCalls.add(query(i, key, true, round));
      }
      if (source.ownerComplete || !state.discovery || (atDeadline && !finalRound)) continue;
      for (const auto& [addressKey, target] : state.addresses) {
        if (target.nodeId == source.epoch.nodeId) continue;
        const auto key = "peer/" + addressKey;
        auto& probe = addProbe(source, key, target);
        const bool irrelevant = !isCandidate(target, source);
        if (irrelevant && !probe.irrelevant) log("tail-candidate-before-window",
            "\"object\":" + quote(name) + ",\"peer\":" + quote(target.nodeId) +
            ",\"nodeEpoch\":\"" + target.destinationEpoch + "\",\"sourceEpoch\":\"" +
            std::to_string(source.epoch.epoch) + "\"");
        probe.irrelevant = irrelevant;
        if (finalRound) probe.complete = false;
        if (!irrelevant) peerCalls.add(query(i, key, false, round));
      }
    }
    // An original owner's full proof takes the fast exit without discovery.
    auto owners = kj::joinPromises(ownerCalls.releaseAsArray()).then([&]() { return allOwners(); }).fork();
    auto peersDone = kj::joinPromises(peerCalls.releaseAsArray()).fork();
    const auto ready = co_await owners.addBranch().exclusiveJoin(peersDone.addBranch().then([]() { return false; }));
    if (ready || allOwners()) break;
    co_await owners.addBranch();if (allOwners()) break;
    co_await peersDone.addBranch();
    finishLocalPreparation(false);
    if (finalRound) {
      if (co_await mayCut()) break;
      // A heartbeat became live again while reading the final pages. Keep the
      // verified prefix, but give each peer another turn before a later cut.
      for (auto& source : state.sources)
        for (auto& [key, probe] : source.probes)
          if (key.starts_with("peer/")) probe.complete = false;
    }
    co_await addDiscovery();
    if (!run.reserved) setRebuildPhase(run, atDeadline ? RebuildPhase::OWNER : RebuildPhase::DEADLINE);
    co_await recoveryWake();
  }
  cutHoldEnd = "the previous owner answered in full";
  checkIdentity();
  TailRecovery out;
  size_t recordCount = 0;
  out.collectionMs = monotonicTimeMs() - startedMs;
  out.cut = !allOwners();
  bool othersMissing = membership.recoveryHistoryIncomplete || (state.discovery && !state.discovery->unresolved.empty());
  std::string missing = "[";
  auto nameMissing = [&](const std::string& text) {
    if (missing.size() > 1) missing += ",";
    missing += quote(text);
  };
  for (auto& source : state.sources) {
    out.sources.push_back({source.epoch.epoch, source.epoch.nodeEpoch, source.ownerComplete});
    if (!source.ownerComplete) {
      for (const auto& [key, probe] : source.probes)
        if (key.starts_with("owner/") && probe.error.find("complete predecessor log is unavailable") != std::string::npos)
          out.sourceLogUnavailable = true;
      nameMissing("owner:" + source.epoch.nodeId + "/" + std::to_string(source.epoch.nodeEpoch) +
          "/shard-epoch-" + std::to_string(source.epoch.epoch));
      for (const auto& [key, probe] : source.probes) {
        if (!key.starts_with("peer/") || probe.complete || probe.gone || probe.irrelevant) continue;
        othersMissing = true;
        nameMissing(probe.target.nodeId + "/" + probe.target.destinationEpoch + "@" + probe.target.address);
      }
    }
    const auto& retained = objectOf(name)->recoveryTails;
    for (auto entry = retained.lower_bound({source.epoch.epoch, 0});
        entry != retained.end() && entry->first.first == source.epoch.epoch; ++entry) {
      const auto& tail = entry->second;
      if (tail.record.nodeEpoch != source.epoch.nodeEpoch ||
          (source.ownerComplete && tail.ownerProof != generation.second)) continue;
      ++recordCount;
    }
  }
  if (state.discovery) for (const auto& key : state.discovery->unresolved) nameMissing(key);
  if (membership.recoveryHistoryIncomplete) nameMissing("evicted-member-address-history");
  missing += "]";
  if (out.cut) {
    if (out.sourceLogUnavailable) ++ownerLogUnavailableRecoveries;
    out.ownerSilent = !othersMissing;
    if (out.ownerSilent) ++ownerSilentRecoveries;
    else ++tailRecoveryBounded;
    log(out.ownerSilent ? "owner-silent" : "tail-recovery-bounded",
        "\"object\":" + quote(name) + ",\"namespaceId\":" + quote(identity.namespaceId) +
        ",\"shard\":" + std::to_string(identity.shard) + ",\"shardEpoch\":\"" +
        std::to_string(identity.shardEpoch) + "\",\"missing\":" + missing +
        ",\"ownerLogUnavailable\":" + (out.sourceLogUnavailable ? "true" : "false") +
        ",\"waitedMs\":" + std::to_string(out.collectionMs) + ",\"records\":" +
        std::to_string(recordCount));
  }
  finishLocalPreparation(true);
  co_return out;
}

// One read of each silent previous owner's heartbeat of its node epoch before a cut.
// Its epoch still live by the membership's rule (the answer's Date minus its Last-Modified within TTL + grace) means a
// heartbeat reached the store late: no cut yet, the recovery keeps asking. A failed read, an answer without both times,
// or a record that names another node or epoch cannot allow a cut either. A heartbeat that is gone allows it, as
// before (that alone never proves the tail empty: the cut is what it was). The epochs this process held are its own
// to judge: never read. This is not a barrier against a heartbeat that lands after the read; the writer's own lease
// check is.
// Counted once per recovery that has to wait (by why it first waited), logged when the wait starts and when it ends.
kj::Promise<bool> DurableLog::previousOwnersMayBeCut(std::string objectName,
    std::vector<std::pair<std::string, uint64_t>> owners, CutHold& hold, RebuildRun& run) {
  for (const auto& [nodeId, epoch] : owners) {
    if (nodeId == settings.nodeId && membership.heldEpochs.contains(std::to_string(epoch))) continue;
    run.owner = nodeId;
    run.ownerEpoch = epoch;
    const auto answer = co_await readOwnerHeartbeat(nodeId, epoch, run.probeAtMs);
    if (answer == OwnerHeartbeat::GONE) continue;
    if (!hold.holding) {
      hold.holding = true;
      hold.sinceMs = monotonicTimeMs();
      ++(answer == OwnerHeartbeat::LIVE ? cutsHeldOwnerLive : cutsHeldOwnerUnknown);
      log(answer == OwnerHeartbeat::LIVE ? "cut-held-owner-live" : "cut-held-owner-unknown",
          "\"object\":" + quote(objectName) + ",\"owner\":" + quote(nodeId) + ",\"nodeEpoch\":\"" +
          std::to_string(epoch) + "\",\"phase\":\"owner\",\"queueMs\":" + std::to_string(run.queuedMs) +
          ",\"probeAgeMs\":" + std::to_string(monotonicTimeMs() - run.probeAtMs));
    }
    co_return false;
  }
  if (hold.holding) {
    hold.holding = false;
    log("cut-hold-ended", "\"object\":" + quote(objectName) + ",\"reason\":\"the previous owner's heartbeat is old "
        "enough\",\"waitedMs\":" + std::to_string(uint64_t(monotonicTimeMs() - hold.sinceMs)));
  }
  co_return true;
}

kj::Promise<DurableLog::OwnerHeartbeat> DurableLog::readOwnerHeartbeat(std::string nodeId, uint64_t epoch,
    double& probeAtMs) {
  const auto key = std::make_pair(nodeId, epoch);
  const auto now = monotonicTimeMs();
  const auto fresh = [now](const OwnerRead& read) {
    return *read.settledAtMs == 0 || now - *read.settledAtMs < RECOVERY_WAKE_INTERVAL / kj::MILLISECONDS;
  };
  if (auto found = ownerReads.find(key); found != ownerReads.end() && fresh(found->second)) {
    probeAtMs = found->second.startedMs;
    return found->second.answer.addBranch();
  }
  for (auto it = ownerReads.begin(); it != ownerReads.end();)
    it = !fresh(it->second) ? ownerReads.erase(it) : std::next(it);
  ++ownerHeartbeatReads;
  probeAtMs = now;
  const auto limitMs = int64_t(membership.grantedTtlSeconds) * 1000 + membership.livenessGraceMs;
  auto read = [](DurableLog& self, std::string nodeId, uint64_t epoch, int64_t limitMs, std::shared_ptr<double> settled)
      -> kj::Promise<OwnerHeartbeat> {
    const auto mark = kj::defer([&]() { *settled = monotonicTimeMs(); });
    if (!self.store) co_return OwnerHeartbeat::UNKNOWN;
    ObjectResponse response;
    try {
      response = co_await self.readRecoveryMetadata(memberHeartbeatKey(nodeId, epoch),
          std::max<uint32_t>(1, self.settings.peerTimeoutMs));
    } catch (const kj::Exception&) {
      co_return OwnerHeartbeat::UNKNOWN;
    }
    if (response.status == 404) co_return OwnerHeartbeat::GONE;
    if (response.status != 200 || !response.dateUnixMs || !response.lastModifiedUnixMs) co_return OwnerHeartbeat::UNKNOWN;
    const auto record = parseMemberRecord(response.bytes);
    if (!record || record->nodeId != nodeId || record->epoch != epoch) co_return OwnerHeartbeat::UNKNOWN;
    const auto age = int64_t(response.dateUnixMs) - int64_t(response.lastModifiedUnixMs);
    co_return age > limitMs ? OwnerHeartbeat::GONE : OwnerHeartbeat::LIVE;
  };
  auto settled = std::make_shared<double>(0);
  auto fork = read(*this, nodeId, epoch, limitMs, settled).fork();
  auto branch = fork.addBranch();
  ownerReads.insert_or_assign(key, OwnerRead{kj::mv(fork), std::move(settled), now});
  return branch;
}

// Recovered bytes are not part of this node's live append sequence. Publish
// them unchanged under this node's own segment namespace, then reference them.
kj::Promise<void> DurableLog::publishRecovery(std::string objectName, const std::vector<LogRecord>& recovered) {
  if (recovered.empty()) {
    auto* object = objectOf(objectName);
    KJ_REQUIRE(object != nullptr && object->state == ObjectState::REBUILDING, "recovery no longer owns the object");
    // Even a store-only replay must not open on a revision it never replayed.
    // A first object at the initial shard epoch has no earlier owner to race.
    const auto state = recoveryStates.find(objectName);
    if (object->manifestRevision || object->shardEpoch > 1 || !object->isolatedRecords.empty() ||
        (state != recoveryStates.end() && !state->second.exclusions.empty()))
      requireStorage(co_await writeManifest(*object, false,
          object->recoveryManifestRevision.value_or(object->manifestRevision)),
          "the replayed manifest changed before recovery completed");
    co_return;
  }
  ++recoveryPublishWaiters;
  const auto waiting = kj::defer([this]() { --recoveryPublishWaiters; });
  if (sealing && !sealingForRecovery)
    uploadRetryCanceler.cancel("recovered history must be published before background resealing");
  while (sealing) co_await timer.afterDelay(5 * kj::MILLISECONDS);
  sealing = true;
  sealingForRecovery = true;
  const auto release = kj::defer([this]() { sealing = false; sealingForRecovery = false; });
  auto* object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr && object->state == ObjectState::REBUILDING, "recovery no longer owns the object");
  const auto targetEpoch = object->shardEpoch;
  const auto publishingEpoch = nodeEpoch;
  std::map<uint64_t, std::map<uint64_t, const LogRecord*>> groups;
  for (const auto& record : recovered) groups[record.shardEpoch].emplace(record.nodeSeq, &record);
  for (const auto& [sourceEpoch, held] : groups) {
    auto next = held.begin();
    while (next != held.end()) {
      std::vector<std::string> encoded;
      SegmentHeader header;
      header.nodeId = settings.nodeId;
      header.nodeEpoch = publishingEpoch;
      header.segment = ++segmentNumber;
      header.sealedAtUnixMs = realClockUnixMs();
      header.reason = "recovery-tail";
      std::map<uint32_t, SegmentIndexEntry> facets;
      uint64_t wireBytes = segmentEnvelopeBudget(header.nodeId, header.reason);
      while (next != held.end()) {
        const auto& [sequence, pointer] = *next;
        const auto& record = *pointer;
        auto bytes = encodeLogRecord(record);
        const auto extra = bytes.size() + 4 +
            (facets.contains(record.facetId) ? 0 : segmentEntryBudget(record));
        if (!encoded.empty() && (header.bytes + bytes.size() > settings.config.segmentBytes ||
            wireBytes + extra > STORE_SEGMENT_BYTES)) break;
        KJ_REQUIRE(wireBytes + extra <= STORE_SEGMENT_BYTES, "a recovered record cannot fit a readable store segment");
        wireBytes += extra;
        if (!header.firstSeq) header.firstSeq = sequence;
        header.lastSeq = sequence;
        header.bytes += bytes.size();
        ++header.records;
        encoded.push_back(std::move(bytes));
        auto& entry = facets[record.facetId];
        entry.objectName = record.objectName;
        entry.facetId = record.facetId;
        if (!entry.records) entry.firstTxn = record.txn;
        entry.lastTxn = record.txn + (record.commits ? record.commits - 1 : 0);
        ++entry.records;
        ++next;
      }
      for (const auto& [facet, entry] : facets) header.objects.push_back(entry);
      const auto body = encodeSegment(header, encoded);
      requireStorage(co_await uploadSegment(header, body), "recovery publication was interrupted by an epoch change");
      object = objectOf(objectName);
      KJ_REQUIRE(nodeEpoch == publishingEpoch && object != nullptr &&
          object->state == ObjectState::REBUILDING && object->shardEpoch == targetEpoch,
          "ownership changed while publishing a recovered tail");
      object->segments.push_back({settings.nodeId, publishingEpoch, header.segment,
          header.firstSeq, header.lastSeq, sourceEpoch});
      ++segments;
      segmentBytes += body.size();
      log("recovery-segment", "\"object\":" + quote(objectName) + ",\"sourceEpoch\":\"" +
          std::to_string(sourceEpoch) + "\",\"segment\":\"" + std::to_string(header.segment) +
          "\",\"records\":" + std::to_string(header.records));
    }
  }
  requireStorage(co_await writeManifest(*object, false,
      object->recoveryManifestRevision.value_or(object->manifestRevision)),
      "recovered tails were not published in the replayed manifest revision");
}

// A bounded recovery fixes the exact history the object will serve. The
// existing snapshot shard epoch is its durable cut; no recovery field is added.
kj::Promise<void> DurableLog::forceRecoverySnapshot(std::string objectName, bool healing, bool local, std::string staging, std::function<void()> validate) {
  auto* object = objectOf(objectName);
  const auto state = local ? ObjectState::LOCAL : ObjectState::REBUILDING;
  KJ_REQUIRE(object != nullptr && object->state == state && !object->snapshotting,
      "a snapshot requires an object at the expected state");
  const auto expectedRevision = local ? std::optional<uint64_t>() :
      std::optional<uint64_t>(object->recoveryManifestRevision.value_or(object->manifestRevision));
  const auto targetEpoch = object->shardEpoch;
  const auto dataEpoch = object->dataEpoch, version = object->snapshotVersion;
  const auto publishingEpoch = nodeEpoch;
  const auto namespaceId = object->namespaceId;
  const auto objectId = object->objectId;
  const RootStorageRef storage = *object;
  const auto selected = object->facets;
  object->snapshotting = true;
  const auto release = kj::defer([this, objectName]() {
    auto* entry = objectOf(objectName);
    if (entry != nullptr) entry->snapshotting = false;
  });
  auto check = [&]() {
    if (validate) validate();
    auto* entry = objectOf(objectName);
    KJ_REQUIRE(entry != nullptr && entry->state == state && entry->dataEpoch == dataEpoch &&
        entry->shardEpoch == targetEpoch && nodeEpoch == publishingEpoch && entry->snapshotVersion == version,
        "the object changed while taking its snapshot");
    for (const auto& [sequence, flight] : inFlight) {
      auto record = parsed.find(sequence);
      KJ_REQUIRE(record == parsed.end() || hostNameOf(record->second) != objectName,
          "a snapshot cannot contain an unconfirmed write");
    }
  };
  auto exact = [&](std::string key, std::string bytes) -> kj::Promise<bool> {
    check();
    auto result = co_await store->create(key, bytes);
    check();
    if (result.created) co_return true;
    auto back = co_await store->get(key);
    check();
    requireStorage(back.status == 200, "an existing recovery snapshot object could not be verified");
    co_return back.bytes == bytes;
  };
  // Orphaned parts from interrupted attempts also reserve their sequence.
  const auto prefix = objectPrefix(namespaceId, objectId) + "snapshot/";
  auto listing = co_await store->list(prefix);
  check();
  uint64_t sequence = object->snapshotSequence;
  for (const auto& listed : listing.objects) {
    const auto rest = listed.key.substr(prefix.size());
    const auto slash = rest.find('/');
    if (slash == std::string::npos) continue;
    const auto number = rest.substr(0, slash);
    if (!number.empty() && std::all_of(number.begin(), number.end(), [](char c) { return c >= '0' && c <= '9'; }))
      sequence = std::max(sequence, decimal(number));
  }
  for (uint32_t attempt = 0; attempt < MANIFEST_ATTEMPTS; ++attempt) {
    check();
    ++sequence;
    std::vector<ManifestFacet> facets;
    uint64_t total = 0, parts = 0;
    bool conflict = false;
    for (const auto& [facetId, facet] : selected) {
      if (facet.deleted) continue;
      const auto path = databasePath(storage, facetId, "", staging);
      const int fd = ::open(path.c_str(), O_RDONLY);
      KJ_REQUIRE(fd >= 0, "a facet required by the recovery snapshot cannot be opened", path.string().c_str());
      const auto close = kj::defer([fd]() { ::close(fd); });
      struct stat stat;
      KJ_REQUIRE(::fstat(fd, &stat) == 0 && stat.st_size >= 0, "a recovery facet cannot be inspected");
      ManifestFacet entry;
      entry.facetId = facetId;
      entry.parentFacetId = facet.parentFacetId;
      entry.name = facet.facetName;
      entry.bytes = uint64_t(stat.st_size);
      entry.upToSeq = facet.checkpointedSeq;
      auto* digest = EVP_MD_CTX_new();
      KJ_REQUIRE(digest != nullptr && EVP_DigestInit_ex(digest, EVP_sha256(), nullptr) == 1,
          "cannot hash a recovery snapshot");
      const auto free = kj::defer([digest]() { EVP_MD_CTX_free(digest); });
      uint64_t offset = 0;
      while (offset < entry.bytes) {
        std::string bytes(size_t(std::min<uint64_t>(settings.config.snapshotPartBytes, entry.bytes - offset)), '\0');
        size_t read = 0;
        while (read < bytes.size()) {
          const auto got = ::pread(fd, bytes.data() + read, bytes.size() - read, off_t(offset + read));
          if (got < 0 && errno == EINTR) continue;
          KJ_REQUIRE(got > 0, "a recovery facet could not be read completely", path.string().c_str());
          read += size_t(got);
        }
        KJ_REQUIRE(EVP_DigestUpdate(digest, bytes.data(), bytes.size()) == 1, "cannot hash recovery bytes");
        if (!co_await exact(objectSnapshotPartKey(namespaceId, objectId, sequence, facetId, entry.parts), bytes)) {
          conflict = true;
          break;
        }
        ++entry.parts;
        offset += bytes.size();
      }
      if (conflict) break;
      unsigned char result[EVP_MAX_MD_SIZE];
      unsigned int length = 0;
      KJ_REQUIRE(EVP_DigestFinal_ex(digest, result, &length) == 1 && length == 32, "cannot finish a recovery digest");
      static constexpr char HEX[] = "0123456789abcdef";
      for (unsigned int i = 0; i < length; ++i) {
        entry.sha256.push_back(HEX[result[i] >> 4]);
        entry.sha256.push_back(HEX[result[i] & 15]);
      }
      total += entry.bytes;
      parts += entry.parts;
      facets.push_back(std::move(entry));
    }
    if (conflict) continue;
    const auto at = realClockUnixMs();
    std::ostringstream index;
    index << "{\"sequence\":\"" << sequence << "\",\"nodeId\":" << quote(settings.nodeId)
          << ",\"nodeEpoch\":\"" << publishingEpoch << "\",\"atUnixMs\":" << at
          << ",\"partBytes\":\"" << settings.config.snapshotPartBytes << "\",\"bytes\":\"" << total
          << "\",\"facets\":[";
    bool first = true;
    for (const auto& facet : facets) {
      if (!first) index << ",";
      first = false;
      index << "{\"facet\":" << facet.facetId << ",\"parent\":" << facet.parentFacetId << ",\"name\":"
            << quote(facet.name) << ",\"bytes\":\"" << facet.bytes << "\",\"parts\":\"" << facet.parts
            << "\",\"upToSeq\":\"" << facet.upToSeq << "\",\"sha256\":" << quote(facet.sha256) << "}";
    }
    index << "]}";
    if (!co_await exact(objectSnapshotIndexKey(namespaceId, objectId, sequence), index.str())) continue;
    check();
    object = objectOf(objectName);
    SnapshotImage image;
    image.sequence = sequence;image.parts = parts;image.bytes = total;image.atUnixMs = at;
    image.nodeEpoch = publishingEpoch;image.shardEpoch = targetEpoch;image.dataEpoch = dataEpoch;
    image.version = version;image.state = state;image.facets = std::move(facets);
    // Publishing removes older segment references only after the complete image
    // is available, including a valid empty image when no history survived.
    requireStorage(co_await writeManifest(*object, true, expectedRevision, &image), "a snapshot manifest was not published");
    check();
    ++snapshots;
    snapshotBytesWritten += total;
    log(local ? "snapshot" : "recovery-snapshot", "\"object\":" + quote(objectName) + ",\"sequence\":\"" +
        std::to_string(sequence) + "\",\"shardEpoch\":\"" + std::to_string(targetEpoch) +
        "\",\"facets\":" + std::to_string(selected.size()) + ",\"bytes\":\"" + std::to_string(total) + "\"");
    co_return;
  }
  failConflict("recovery snapshot sequences kept conflicting");
}

// Every source read is from the cache startup already judged trustworthy. No
// workerd may open these files until the whole WAL is validated and published.


void DurableLog::objectFailed(const std::string& namespaceId, const std::string& objectId, uint64_t shardEpoch,
    const std::string& step, const std::string& reason) {
  ++objectFailures;
  log("object-operation-failed", "\"namespaceId\":" + quote(namespaceId) + ",\"objectId\":" + quote(objectId) +
      ",\"shardEpoch\":\"" + std::to_string(shardEpoch) + "\",\"step\":" + quote(step) + ",\"reason\":" + quote(reason));
}

kj::Promise<void> DurableLog::healLoop() {
  for (;;) {
    co_await timer.afterDelay(100 * kj::MILLISECONDS);
    // A settled fork is removed by a later event, never by its own continuation.
    for (const auto& name : settledRebuilds) {
      rebuilding.erase(name);
      rebuildCancelers.erase(name);
    }
    settledRebuilds.clear();
  }
}



// The records of an object, in the order they were written -- by the shard's epoch, then the owner's epoch, then its
// sequence -- replayed into each of its databases. A gap in one owner's sequence ends that owner's part there: a
// peer holds a prefix, so what follows a gap was never answered to anybody.
// Both drivers keep one replay state across batches: WAL headers, salts and running
// checksums never reset when the seed yields to membership and tenant requests.
class DurableLog::ReplayState {
public:
  ReplayState(DurableLog& log, RootStorageRef destination, std::string historyName,
      uint64_t bookkeepingEpoch, std::string staging, std::vector<ManifestFacet> facets,
      uint64_t snapshotShardEpoch, std::set<std::pair<uint64_t, uint64_t>> drop,
      std::function<void(const LogRecord&)> beforeWrite)
      : log(log), destination(std::move(destination)), historyName(std::move(historyName)),
        bookkeepingEpoch(bookkeepingEpoch), staging(std::move(staging)), facets(std::move(facets)),
        snapshotShardEpoch(snapshotShardEpoch), drop(std::move(drop)), beforeWrite(std::move(beforeWrite)) {
    for (const auto& facet : this->facets) covered[facet.facetId] = facet.upToSeq;
    if (snapshotShardEpoch == bookkeepingEpoch) rebuiltUpTo = covered;
  }
  void add(LogRecord record) {
    if (!record.ownerSealed && drop.contains({record.shardEpoch, record.nodeSeq})) return;
    ordered.insert({{record.shardEpoch, record.nodeSeq}, std::move(record)});
  }
  void apply(const LogRecord& record) {
    auto found = covered.find(record.facetId);
    const bool inSnapshot = record.shardEpoch < snapshotShardEpoch ||
        (found != covered.end() && record.shardEpoch == snapshotShardEpoch && record.nodeSeq <= found->second);
    if (inSnapshot) {
      ++skipped;
      return;
    }
    auto& replay = replays[record.facetId];
    if (!replay) {
      // A facet the snapshot brought goes on from its file; one it did not starts from nothing.
      replay = std::make_unique<DatabaseReplay>(log.databasePath(destination, record.facetId, "", staging),
          covered.find(record.facetId) == covered.end());
    }
    if (beforeWrite) beforeWrite(record);
    std::string error;
    if (!replay->apply(record, &error)) {
      // The records of an object are a chain: one that does not follow the last means the history is not whole, and
      // an object is never served with less of it than it had. The rebuild fails and the request gets an answer it
      // can act on.
      ++log.replayStops;
      log.log("replay-stopped", "\"object\":" + quote(historyName) + ",\"facet\":" +
              std::to_string(record.facetId) + ",\"shardEpoch\":\"" + std::to_string(record.shardEpoch) +
              "\",\"nodeSeq\":\"" + std::to_string(record.nodeSeq) + "\",\"applied\":" + std::to_string(applied) +
              ",\"records\":" + std::to_string(ordered.size()) + ",\"reason\":" + quote(error));
      // Invalid staging is never used. Destruction closes it and the caller
      // removes it; synchronizing an image that is about to be discarded is unnecessary.
      KJ_FAIL_REQUIRE("the replicated history of a Durable Object is not whole", historyName.c_str(),
          record.nodeSeq, error.c_str());
    }
    ++applied;
    if (record.shardEpoch == bookkeepingEpoch)
      rebuiltUpTo[record.facetId] = std::max(rebuiltUpTo[record.facetId], record.nodeSeq);
    auto& facet = rebuilt[record.facetId];
    facet.facetId = record.facetId;
    if (!record.facetName.empty()) {
      facet.facetName = record.facetName;
      facet.parentFacetId = record.parentFacetId;
    }
    facet.txn = record.txn + (record.commits ? record.commits - 1 : 0);
  }
  kj::Promise<std::map<uint32_t, Facet>> finishAsync(std::function<void()> check,
      std::function<void(double)> observeFsync) {
    for (auto& [facetId, replay] : replays) {
      if (!replay) continue;
      const auto began = monotonicTimeMs();
      const bool synced = co_await replay->finishAsync();
      if (observeFsync) observeFsync(monotonicTimeMs() - began);
      check();
      KJ_REQUIRE(synced, "cannot synchronize a rebuilt Durable Object database",
          log.databasePath(destination, facetId, "", staging).string().c_str());
    }
    co_return finishMetadata();
  }
  std::map<uint32_t, Facet> finishMetadata() {
    for (const auto& facet : facets) {
      auto& entry = rebuilt[facet.facetId];
      entry.facetId = facet.facetId;
      entry.parentFacetId = facet.parentFacetId;
      if (!facet.name.empty()) entry.facetName = facet.name;
    }
    for (auto& [facetId, facet] : rebuilt) {
      facet.lastFrame = 0;
      facet.salt1 = 0;
      facet.salt2 = 0;
      facet.checkpointedSeq = rebuiltUpTo[facetId];
      facet.lastSeq = 0;
      facet.open = false;
    }
    log.log("replayed", "\"object\":" + quote(historyName) + ",\"records\":" + std::to_string(applied) +
            ",\"skipped\":" + std::to_string(skipped) + ",\"facets\":" + std::to_string(rebuilt.size()));
    return std::move(rebuilt);
  }
  std::map<std::pair<uint64_t, uint64_t>, LogRecord> ordered;
private:
  DurableLog& log;
  RootStorageRef destination;
  std::string historyName;
  uint64_t bookkeepingEpoch;
  std::string staging;
  std::vector<ManifestFacet> facets;
  uint64_t snapshotShardEpoch;
  std::set<std::pair<uint64_t, uint64_t>> drop;
  std::function<void(const LogRecord&)> beforeWrite;
  std::map<uint32_t, uint64_t> covered, rebuiltUpTo;
  std::map<uint32_t, std::unique_ptr<DatabaseReplay>> replays;
  std::map<uint32_t, Facet> rebuilt;
  uint64_t applied = 0, skipped = 0;
};
kj::Promise<std::map<uint32_t, DurableLog::Facet>> DurableLog::replayToAsync(RootStorageRef destination,
    std::string historyName, uint64_t bookkeepingEpoch, std::string staging, std::vector<LogRecord> history,
    std::vector<ManifestFacet> facets, uint64_t snapshotShardEpoch,
    std::set<std::pair<uint64_t, uint64_t>> drop, std::function<void(const LogRecord&)> beforeWrite,
    std::function<void()> check, std::function<void(double)> observeFsync) {
  ReplayState state(*this, std::move(destination), std::move(historyName), bookkeepingEpoch,
      std::move(staging), std::move(facets), snapshotShardEpoch, std::move(drop), std::move(beforeWrite));
  auto began = monotonicTimeMs();
  for (auto& record : history) {
    state.add(std::move(record));
    if (monotonicTimeMs() - began >= 2) {
      co_await timer.afterDelay(0 * kj::MILLISECONDS); check(); began = monotonicTimeMs();
    }
  }
  history.clear();
  for (const auto& [position, record] : state.ordered) {
    state.apply(record);
    if (monotonicTimeMs() - began >= 2) {
      co_await timer.afterDelay(0 * kj::MILLISECONDS); check(); began = monotonicTimeMs();
    }
  }
  check(); co_return co_await state.finishAsync(std::move(check), std::move(observeFsync));
}

kj::Promise<void> DurableLog::measureRecoveryLoop() {
  while (!recoveryLoopObservers.empty()) {
    const auto began = monotonicTimeMs();
    co_await timer.afterDelay(5 * kj::MILLISECONDS);
    const auto delay = monotonicTimeMs() - began - 5;
    for (const auto& maximum : recoveryLoopObservers) *maximum = std::max(*maximum, delay);
  }
  measuringRecovery = false;
}

// The facet tree index beside an object's databases: workerd writes it itself and no transaction covers it, so a
// rebuild writes the names and parents the records carry (experiment 25, D.6).
void DurableLog::writeFacetIndex(const RootStorageRef& ref, const std::vector<ManifestFacet>& facets) {
  const auto path = ref.facetIndexPath(settings.stackDirectory);
  auto removeIndex = [&]() {
    std::error_code code;
    std::filesystem::remove(path, code);
    KJ_REQUIRE(!code, "cannot remove an obsolete Durable Object facet index", path.string().c_str(), code.message().c_str());
  };
  std::vector<ManifestFacet> entries;
  for (const auto& facet : facets) {
    if (facet.facetId == 0 || facet.name.empty()) continue;
    entries.push_back(facet);
  }
  if (entries.empty()) { removeIndex(); return; }
  std::sort(entries.begin(), entries.end(), [](const ManifestFacet& a, const ManifestFacet& b) {
    return a.facetId < b.facetId;
  });
  std::string bytes;
  for (int i = 0; i < 8; i++) bytes.push_back(char((FACET_INDEX_MAGIC >> (8 * i)) & 0xff));
  uint32_t expected = 1;
  for (const auto& facet : entries) {
    // The ids are the positions of the entries: a gap would give the next facet another id than it had.
    if (facet.facetId != expected) {
      log("facet-index-gap", "\"storageId\":" + quote(ref.storageId) + ",\"facet\":" + std::to_string(facet.facetId));
      removeIndex();
      return;
    }
    ++expected;
    bytes.push_back(char(facet.parentFacetId & 0xff));
    bytes.push_back(char((facet.parentFacetId >> 8) & 0xff));
    bytes.push_back(char(facet.name.size() & 0xff));
    bytes.push_back(char((facet.name.size() >> 8) & 0xff));
    bytes += facet.name;
  }
  const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
  KJ_REQUIRE(fd >= 0, "cannot open a Durable Object facet index", path.string().c_str());
  const auto close = kj::defer([fd]() { ::close(fd); });
  size_t done = 0;
  while (done < bytes.size()) {
    auto written = ::write(fd, bytes.data() + done, bytes.size() - done);
    if (written < 0 && errno == EINTR) continue;
    KJ_REQUIRE(written > 0, "cannot write a Durable Object facet index", path.string().c_str());
    done += size_t(written);
  }
  KJ_REQUIRE(::fsync(fd) == 0, "cannot synchronize a Durable Object facet index", path.string().c_str());
}

// ---- Alarms ----
// An object's alarm lives in its own database (SQLite's _cf_METADATA), so it is replicated and recovered with the
// object; the durable process reports every change and every open, and this node keeps the schedule. Firing is a
// request to the object like any other -- only the node can make one, which is why the schedule is here and not in
// the process: the object's class comes from the Worker version the node names, the CPU is topped up for the event,
// the lease is renewed and the lines are kept. A hint beside the shard's epochs names each object's next alarm, so
// the node that takes the shard over knows which objects to wake; the object's own database then decides.
// The object's size as its databases last reported it, and the single-object limit judged on it: past the limit only
// its writes are refused (413 DO_OBJECT_TOO_LARGE); everything that reads is served.
void DurableLog::sized(Object& object) {
  dirtyCacheSizes.insert(object.objectName);
  uint64_t bytes = 0;
  for (const auto& [id, entry] : object.facets) bytes += entry.databaseBytes;
  object.bytes = bytes;
  if (bytes > settings.config.objectBytes && !object.overLimit) {
    object.overLimit = true;
    log("object-too-large", "\"object\":" + quote(object.objectName) + ",\"bytes\":\"" + std::to_string(bytes) +
            "\",\"limit\":\"" + std::to_string(settings.config.objectBytes) + "\"");
  } else if (bytes <= settings.config.objectBytes && object.overLimit) {
    object.overLimit = false;
    log("object-within-limit", "\"object\":" + quote(object.objectName) + ",\"bytes\":\"" +
            std::to_string(bytes) + "\"");
  }
}

void DurableLog::alarmReported(const std::string& objectName, uint32_t facetId, uint64_t atUnixMs) {
  if (facetId != 0) {
    log("alarm-invalid-facet", "\"facet\":" + std::to_string(facetId));
    return;
  }
  auto* object = objectOf(objectName);
  if (object == nullptr) {
    log("alarm-unknown", "\"object\":" + quote(objectName) + ",\"facet\":" + std::to_string(facetId));
    return;
  }
  const auto key = object->namespaceId + "/" + object->objectId + "/" + std::to_string(facetId);
  const auto found = object->alarms.find(facetId);
  const uint64_t previous = found == object->alarms.end() ? 0 : found->second;
  object->alarms[facetId] = atUnixMs;
  if (atUnixMs == 0) {
    if (previous) ++alarmsCleared;
    const bool had = eraseAlarm(key);
    if (previous || had) {
      log("alarm-cleared", "\"object\":" + quote(object->objectName) + ",\"facet\":" + std::to_string(facetId) +
              ",\"was\":" + std::to_string(previous));
      writeAlarmHint(object->namespaceId, object->objectId, object->shard, 0, facetId, object->shardEpoch, object->tenant);
    }
    return;
  }
  auto& state = alarms[key];
  unindexAlarm(key, state);
  if (state.atUnixMs != atUnixMs) state.dueObserved = false;
  const bool fresh = state.atUnixMs != atUnixMs || state.fromHint;
  state.objectName = object->objectName;
  state.namespaceId = object->namespaceId;
  state.objectId = object->objectId;
  state.tenant = object->tenant;
  state.shard = object->shard;
  state.facetId = facetId;
  state.dataEpoch = object->dataEpoch;
  state.shardEpoch = object->shardEpoch;
  state.atUnixMs = atUnixMs;
  state.fromHint = false;
  if (fresh) {
    ++alarmsSet;
    if (settings.threadedAlarmEvents && previous && previous != atUnixMs) {
      state.fireToken = 0;
      state.firing = false;
      state.rebuildWaiting = false;
    }
    state.retries = 0;
    state.countedRetries = 0;
    state.backoff = 0;
    state.clearing = false;
    state.notBeforeMs = 0;
    log("alarm-set", "\"object\":" + quote(object->objectName) + ",\"facet\":" + std::to_string(facetId) +
            ",\"atUnixMs\":" + std::to_string(atUnixMs));
    writeAlarmHint(object->namespaceId, object->objectId, object->shard, atUnixMs, facetId, object->shardEpoch, object->tenant);
  }
  indexAlarm(key, state);
}

// Publication is auxiliary and never joins a storage confirmation. The latest desired state is coalesced.
void DurableLog::writeAlarmHint(std::string namespaceId, std::string objectId, uint32_t shard,
    uint64_t atUnixMs, uint32_t facetId, uint64_t shardEpoch, std::string tenant) {
  if (!enabled() || facetId != 0 || !alarmPublication) return;
  alarmPublication->report(std::move(namespaceId), std::move(objectId), shard, shardEpoch, std::move(tenant), atUnixMs);
}

kj::Promise<void> DurableLog::readAlarmHints(std::string namespaceId, uint32_t shard, uint64_t shardEpoch,
    std::string tenant) {
  if (enabled() && alarmPublication) alarmPublication->take(std::move(namespaceId), shard, shardEpoch, std::move(tenant));
  co_return;
}

namespace {
uint64_t alarmThreadTime() { timespec t{};clock_gettime(CLOCK_THREAD_CPUTIME_ID, &t);return uint64_t(t.tv_sec) * 1000000000ull + t.tv_nsec; }
}
DurableLog::AlarmCpuScope::AlarmCpuScope(DurableLog& owner): owner(owner) {
  if (owner.measureAlarmCpu && !owner.alarmCpuDepth++) started = alarmThreadTime();
}
DurableLog::AlarmCpuScope::~AlarmCpuScope() {
  if (owner.measureAlarmCpu && !--owner.alarmCpuDepth) owner.alarmPumpCpuNs += alarmThreadTime() - started;
}
DurableLog::AlarmDispatchLease::AlarmDispatchLease(DurableLog& owner, std::string key,
    std::string process, uint64_t token, bool cold, std::vector<std::string> probes)
    : owner(owner), key(std::move(key)), process(std::move(process)), token(token), cold(cold), probes(std::move(probes)) {
  ++owner.activeAlarmFires;owner.peakAlarmFires = std::max(owner.peakAlarmFires, owner.activeAlarmFires);
  if (cold) ++owner.activeAlarmPreparations;
  if (owner.settings.threadedAlarmEvents) {
    KJ_ASSERT(owner.activeThreadedAlarms.insert(this->key).second);
  }
  auto& count = owner.alarmProcessFires[this->process];++count.first;count.second = std::max(count.second, count.first);
}
double DurableLog::alarmMonotonicMs() const {
  return alarmMonotonicForTest ? alarmMonotonicForTest() : monotonicTimeMs();
}

uint64_t DurableLog::alarmWallMs() const {
  return alarmWallForTest ? alarmWallForTest() : realClockUnixMs();
}

DurableLog::AlarmDispatchLease::~AlarmDispatchLease() noexcept(false) {
  AlarmCpuScope measured(owner);--owner.activeAlarmFires;
  if (cold) --owner.activeAlarmPreparations;
  --owner.alarmProcessFires.at(process).first;
  if (owner.settings.threadedAlarmEvents) owner.activeThreadedAlarms.erase(key);
  const auto now = owner.alarmMonotonicMs();
  // This lease exists before evalLater: even a dispatch that never starts or loses its entry releases its probe.
  for (const auto& probe: probes) {
    auto gate = owner.alarmCapacityGates.find(probe);
    if (gate != owner.alarmCapacityGates.end() && !gate->second.open && gate->second.probeToken == token) {
      gate->second.probing = false;gate->second.probeToken = 0;gate->second.next = now + 200;
    }
  }
  if (owner.stopped) return;
  auto alarm = owner.alarms.find(key);
  if (alarm != owner.alarms.end() && alarm->second.fireToken == token && alarm->second.firing) {
    alarm->second.firing = false;alarm->second.notBeforeMs = std::max(alarm->second.notBeforeMs, now + 1000);
    owner.indexAlarm(key, alarm->second);
  } else if (alarm != owner.alarms.end() && owner.settings.threadedAlarmEvents) {
    owner.indexAlarm(key, alarm->second);
  }
  owner.wakeAlarmPreparations();owner.scheduleAlarmPump();
}
void DurableLog::suspendAlarmBucket(const AlarmBucketKey& key) {
  auto it = alarmBuckets.find(key);if (it == alarmBuckets.end()) return;auto& b = it->second;
  if (b.ready) {
    auto t = alarmReadyTenants.find(key.first);
    if (t != alarmReadyTenants.end()) { if (t->second.erase({std::get<1>(b.head), std::get<2>(b.head), key.second})) --alarmReadyBucketCount;if (t->second.empty()) alarmReadyTenants.erase(t); }
    b.ready = false;
  }
  if (b.timed) { alarmDeadlines.erase({b.futureWake, key.first, key.second});b.timed = false; }
  if (!b.gate.empty()) { alarmCapacityGates[b.gate].blocked.erase(key);b.gate.clear(); }
}
void DurableLog::refreshAlarmBucket(const AlarmBucketKey& key) {
  auto it = alarmBuckets.find(key);if (it == alarmBuckets.end()) return;suspendAlarmBucket(key);auto& b = it->second;
  if (b.queue.empty() && b.retryReady.empty()) { alarmBuckets.erase(it);return; }
  const auto now = alarmMonotonicMs();const auto wall = alarmWallMs();
  const bool ordinaryDue = !b.queue.empty() && std::get<0>(*b.queue.begin()) <= wall;
  if (!b.queue.empty() && !ordinaryDue) {
    b.futureWake = std::get<0>(*b.queue.begin());b.timed = true;alarmDeadlines.emplace(b.futureWake, key.first, key.second);
  }
  if (!ordinaryDue && b.retryReady.empty()) return;
  b.head = !ordinaryDue ? *b.retryReady.begin() : b.retryReady.empty() ? *b.queue.begin() : std::min(*b.queue.begin(), *b.retryReady.begin());
  for (const auto& gateKey: {std::string("node"), std::string("process:") + key.second}) {
    auto gate = alarmCapacityGates.find(gateKey);
    if (gate != alarmCapacityGates.end() && !gate->second.open && (gate->second.probing || gate->second.next > now)) {
      b.gate = gateKey;gate->second.blocked.insert(key);return;
    }
  }
  b.ready = true;if (alarmReadyTenants[key.first].emplace(std::get<1>(b.head), std::get<2>(b.head), key.second).second) ++alarmReadyBucketCount;
}
void DurableLog::unindexAlarm(const std::string& key, AlarmState& state) {
  AlarmCpuScope measured(*this);
  if (state.retryIndexed) { alarmRetryDeadlines.erase({state.notBeforeMs, key});state.retryIndexed = false; }
  if (state.namespaceWaiting) { alarmPausedNamespaces[state.namespaceId].keys.erase(key);state.namespaceWaiting = false; }
  if (state.preparationWaiting) {
    auto tenant = alarmPreparationQueue.find(state.tenant);
    if (tenant != alarmPreparationQueue.end()) { if (tenant->second.erase({state.atUnixMs, key})) --alarmPreparationCount;if (tenant->second.empty()) alarmPreparationQueue.erase(tenant); }
    state.preparationWaiting = false;
  }
  if (!state.indexed) return;state.indexed = false;
  const AlarmBucketKey bucket{state.tenant, state.destination};auto it = alarmBuckets.find(bucket);KJ_REQUIRE(it != alarmBuckets.end());
  (state.dueObserved ? it->second.retryReady : it->second.queue).erase({state.wakeUnixMs, state.atUnixMs, key});refreshAlarmBucket(bucket);
}
void DurableLog::indexAlarm(const std::string& key, AlarmState& state) {
  AlarmCpuScope measured(*this);unindexAlarm(key, state);
  if (state.firing || state.rebuildWaiting || !state.atUnixMs || activeThreadedAlarms.contains(key)) return;
  if (alarmNamespace) {
    auto space = alarmNamespace(state.namespaceId);
    if (space.status == AlarmNamespace::Status::DELETED) { ++alarmInactive;alarms.erase(key);return; }
    if (space.status != AlarmNamespace::Status::ACTIVE) {
      state.namespaceWaiting = true;auto [it, inserted] = alarmPausedNamespaces.try_emplace(state.namespaceId);
      it->second.keys.insert(key);
      if (inserted) { it->second.next = alarmMonotonicMs() + 500;alarmNamespaceDeadlines.emplace(it->second.next, state.namespaceId); }
      return;
    }
    state.tenant = std::move(space.tenant);
  }
  if (state.notBeforeMs > alarmMonotonicMs()) {
    state.retryIndexed = true;alarmRetryDeadlines.emplace(state.notBeforeMs, key);return;
  }
  DurableAlarm ask{state.namespaceId, state.objectId, state.tenant, state.shard, state.facetId, state.dataEpoch,
      state.shardEpoch, state.atUnixMs, state.retries};
  state.destination = alarmDestination ? alarmDestination(ask) : std::string();
  state.wakeUnixMs = state.atUnixMs;state.indexed = true;
  const AlarmBucketKey bucket{state.tenant, state.destination};auto& b = alarmBuckets[bucket];
  (state.dueObserved ? b.retryReady : b.queue).emplace(state.wakeUnixMs, state.atUnixMs, key);
  refreshAlarmBucket(bucket);
}
bool DurableLog::eraseAlarm(const std::string& key) {
  auto it = alarms.find(key);if (it == alarms.end()) return false;
  unindexAlarm(key, it->second);alarms.erase(it);return true;
}
bool DurableLog::alarmLocallyReady(const AlarmState& state) const {
  if (state.objectName.empty()) return false;
  auto object = objects.find(state.objectName);
  return object != objects.end() && object->second.state == ObjectState::LOCAL && object->second.shardEpoch == state.shardEpoch;
}
void DurableLog::probeAlarmNamespace(const std::string& ns) {
  auto it = alarmPausedNamespaces.find(ns);if (it == alarmPausedNamespaces.end()) return;
  const auto status = alarmNamespace ? alarmNamespace(ns).status : AlarmNamespace::Status::ACTIVE;
  if (!it->second.draining) alarmNamespaceDeadlines.erase({it->second.next, ns});
  if (status == AlarmNamespace::Status::UNKNOWN || status == AlarmNamespace::Status::PAUSED) {
    if (it->second.keys.empty()) { alarmPausedNamespaces.erase(it);return; }
    it->second.draining = false;
    it->second.misses = std::min(7u, it->second.misses + 1);
    it->second.next = alarmMonotonicMs() + std::min<uint64_t>(60000, uint64_t(500) << it->second.misses);
    alarmNamespaceDeadlines.emplace(it->second.next, ns);return;
  }
  it->second.misses = 0;it->second.draining = true;
  for (unsigned n = 0;n < 64 && !it->second.keys.empty();++n) {
    const auto key = *it->second.keys.begin();it->second.keys.erase(it->second.keys.begin());auto alarm = alarms.find(key);
    if (alarm != alarms.end()) { alarm->second.namespaceWaiting = false;indexAlarm(key, alarm->second); }
  }
  if (it->second.keys.empty()) alarmPausedNamespaces.erase(it);
  else alarmActiveNamespaces.push_back(ns);
}
void DurableLog::closeAlarmCapacity(const std::string& key, double observedMs) {
  auto& gate = alarmCapacityGates[key];const auto now = alarmMonotonicMs();
  if (observedMs && observedMs < gate.opened) return;
  gate.open = false;gate.probing = false;gate.probeToken = 0;gate.next = now + 200;
}
void DurableLog::openAlarmCapacity(const std::string& key) {
  auto it = alarmCapacityGates.find(key);if (it == alarmCapacityGates.end()) return;
  it->second.open = true;it->second.probing = false;it->second.probeToken = 0;it->second.next = 0;it->second.opened = alarmMonotonicMs();
}
void DurableLog::alarmCapacityAvailable(const std::string& destination) {
  AlarmCpuScope measured(*this);openAlarmCapacity("node");openAlarmCapacity("process:" + destination);scheduleAlarmPump();
}
void DurableLog::wakeAlarmPreparations() {
  if (!alarmPreparationQueue.empty() && activeAlarmPreparations < ALARM_REBUILD_WAIT_LIMIT) scheduleAlarmPump();
}
kj::Promise<void> DurableLog::alarmRebuilt(std::string key, uint64_t token, kj::Promise<void> promise) {
  bool failed = false;
  try { co_await kj::mv(promise); } catch (const kj::Exception&) { failed = true; }
  AlarmCpuScope measured(*this);--alarmRebuildWaiting;
  if (stopped) co_return;
  auto it = alarms.find(key);
  if (it != alarms.end() && it->second.fireToken == token) {
    it->second.rebuildWaiting = false;if (failed) it->second.notBeforeMs = std::max(it->second.notBeforeMs, alarmMonotonicMs() + 1000);
    indexAlarm(key, it->second);
  }
  wakeAlarmPreparations();scheduleAlarmPump();
}
void DurableLog::armAlarmTimer(double at) {
  if (stopped || (alarmTimerAt && alarmTimerAt <= at)) return;
  alarmTimerAt = at;
  alarmTimer = timer.afterDelay(int64_t(std::max(0.0, at - alarmMonotonicMs()) * 1000) * kj::MICROSECONDS).then([this, at]() {
    if (alarmTimerAt == at) { alarmTimerAt = 0;scheduleAlarmPump(); }
  }).eagerlyEvaluate([](kj::Exception&&) {});
}
void DurableLog::scheduleAlarmPump() {
  if (stopped || alarmPumpScheduled) return;alarmPumpScheduled = true;
  tasks.add(kj::evalLater([this]() { alarmPumpScheduled = false;pumpAlarmFires(); }));
}
void DurableLog::pumpAlarmFires() {
  AlarmCpuScope measured(*this);if (stopped || !hooks.fireAlarm) return;
  const auto wall = alarmWallMs();const auto now = alarmMonotonicMs();unsigned promoted = 0;
  while (promoted++ < 64 && !alarmDeadlines.empty() && std::get<0>(*alarmDeadlines.begin()) <= wall) {
    const auto row = *alarmDeadlines.begin();alarmDeadlines.erase(alarmDeadlines.begin());refreshAlarmBucket({std::get<1>(row), std::get<2>(row)});
  }
  promoted = 0;
  while (promoted++ < 64 && !alarmRetryDeadlines.empty() && alarmRetryDeadlines.begin()->first <= now) {
    const auto key = alarmRetryDeadlines.begin()->second;alarmRetryDeadlines.erase(alarmRetryDeadlines.begin());auto it = alarms.find(key);
    if (it != alarms.end()) { it->second.retryIndexed = false;indexAlarm(key, it->second); }
  }
  if (!alarmActiveNamespaces.empty()) {
    const auto ns = alarmActiveNamespaces.front();alarmActiveNamespaces.pop_front();probeAlarmNamespace(ns);
  }
  if (now >= alarmNamespaceBatchAt && !alarmNamespaceDeadlines.empty() && alarmNamespaceDeadlines.begin()->first <= now) {
    alarmNamespaceBatchAt = now + 500;
    for (unsigned n = 0;n < 8 && !alarmNamespaceDeadlines.empty() && alarmNamespaceDeadlines.begin()->first <= now;++n) {
      const auto ns = alarmNamespaceDeadlines.begin()->second;probeAlarmNamespace(ns);
    }
  }
  unsigned gateWork = 0;
  for (auto& [key, gate]: alarmCapacityGates) {
    if (gate.probing || (!gate.open && gate.next > now)) continue;
    while (gateWork++ < 8 && !gate.blocked.empty()) {
      const auto bucket = *gate.blocked.begin();gate.blocked.erase(gate.blocked.begin());refreshAlarmBucket(bucket);
      if (!gate.open) break; // Only one bucket can offer the next probe.
    }
  }
  const auto fireRoom = [this] {
    return settings.threadedAlarmEvents || activeAlarmFires < settings.config.alarmFireConcurrency;
  };
  for (unsigned n = 0;n < 8 && fireRoom();++n) {
    const bool coldRoom = activeAlarmPreparations < ALARM_REBUILD_WAIT_LIMIT;
    auto preparation = alarmPreparationQueue.end();
    if (coldRoom && !alarmPreparationQueue.empty()) {
      preparation = alarmPreparationQueue.upper_bound(lastAlarmPreparationTenant);
      if (preparation == alarmPreparationQueue.end()) preparation = alarmPreparationQueue.begin();
    }
    bool takePreparation = preparation != alarmPreparationQueue.end();AlarmBucketKey bucket;
    if (takePreparation) {
      const auto tenantName = preparation->first;
      lastAlarmPreparationTenant = tenantName;
      const auto key = preparation->second.begin()->second;auto entry = alarms.find(key);
      KJ_REQUIRE(entry != alarms.end());indexAlarm(key, entry->second);
      entry = alarms.find(key);if (entry == alarms.end() || !entry->second.indexed) continue;
      bucket = {entry->second.tenant, entry->second.destination};
      // Earlier hot work in this tenant stays in the ordinary ready rotation. Keep this cold entry queued.
      auto b = alarmBuckets.find(bucket);
      if (b != alarmBuckets.end() && b->second.ready && std::get<2>(b->second.head) != key &&
          alarmLocallyReady(alarms.at(std::get<2>(b->second.head)))) {
        unindexAlarm(key, entry->second);entry->second.preparationWaiting = true;
        if (alarmPreparationQueue[entry->second.tenant].emplace(entry->second.atUnixMs, key).second) ++alarmPreparationCount;
        takePreparation = false;
      }
    }
    if (!takePreparation) {
      if (alarmReadyTenants.empty()) break;
      auto tenant = alarmReadyTenants.upper_bound(lastAlarmTenant);if (tenant == alarmReadyTenants.end()) tenant = alarmReadyTenants.begin();
      lastAlarmTenant = tenant->first;bucket = {tenant->first, std::get<2>(*tenant->second.begin())};
    }
    refreshAlarmBucket(bucket);auto b = alarmBuckets.find(bucket);if (b == alarmBuckets.end() || !b->second.ready) continue;
    const auto key = std::get<2>(b->second.head);auto& state = alarms.at(key);
    const bool locallyReady = alarmLocallyReady(state);
    if (!locallyReady && (!coldRoom || (!takePreparation && !alarmPreparationQueue.empty()))) {
      unindexAlarm(key, state);state.dueObserved = true;state.preparationWaiting = true;
      if (alarmPreparationQueue[state.tenant].emplace(state.atUnixMs, key).second) ++alarmPreparationCount;continue;
    }
    const auto token = state.fireToken = ++alarmFireSerial;std::vector<std::string> probes;
    for (const auto& gateKey: {std::string("node"), std::string("process:") + state.destination}) {
      auto gate = alarmCapacityGates.find(gateKey);
      if (gate != alarmCapacityGates.end() && !gate->second.open) {
        gate->second.probing = true;gate->second.probeToken = token;probes.push_back(gateKey);
      }
    }
    unindexAlarm(key, state);
    state.dueObserved = true;state.firing = true;
    auto lease = kj::heap<AlarmDispatchLease>(*this, key, state.destination, token, !locallyReady, std::move(probes));
    tasks.add(kj::evalLater([this, key, token, lease = kj::mv(lease)]() mutable {
      return fireAlarm(key, token).attach(kj::mv(lease));
    }));
  }
  if (!alarmActiveNamespaces.empty() || (fireRoom() && (!alarmReadyTenants.empty() ||
      (!alarmPreparationQueue.empty() && activeAlarmPreparations < ALARM_REBUILD_WAIT_LIMIT)))) scheduleAlarmPump();
  double next = 0;auto deadline = [&next](double at) { if (!next || at < next) next = at; };
  if (!alarmRetryDeadlines.empty()) deadline(alarmRetryDeadlines.begin()->first);
  if (!alarmNamespaceDeadlines.empty()) deadline(std::max(alarmNamespaceDeadlines.begin()->first, alarmNamespaceBatchAt));
  for (const auto& [key, gate]: alarmCapacityGates) if (!gate.open && !gate.probing && !gate.blocked.empty()) deadline(gate.next);
  if (next) armAlarmTimer(!fireRoom() && next <= now ? now + 200 : next);
}

bool DurableLog::alarmFireIsCurrent(const DurableAlarm& alarm) const {
  const auto key = alarm.namespaceId + "/" + alarm.objectId + "/" + std::to_string(alarm.facetId);
  const auto found = alarms.find(key);
  if (found == alarms.end()) return false;
  const auto& state = found->second;
  return state.firing && state.fireToken == alarm.fireToken && state.atUnixMs == alarm.atUnixMs &&
      state.shardEpoch == alarm.shardEpoch && (!state.dataEpoch || !alarm.dataEpoch || state.dataEpoch == alarm.dataEpoch);
}

kj::Promise<void> DurableLog::alarmLoop() {
  const bool measureCpu = std::getenv("COWORKERD_ALARM_THREAD_CPU_FOR_TEST") != nullptr;
  auto cpu = []() { timespec t{};clock_gettime(CLOCK_THREAD_CPUTIME_ID, &t);return uint64_t(t.tv_sec) * 1000000000ull + t.tv_nsec; };
  for (;;) {
    const auto expected = monotonicTimeMs() + 200;
    co_await timer.afterDelay(200 * kj::MILLISECONDS);
    const auto started = monotonicTimeMs();const auto thread = measureCpu ? cpu() : 0;
    alarmLoopMaxLagMs = std::max(alarmLoopMaxLagMs, started - expected);
    if (alarmPublication) alarmPublication->tick();
    pumpAlarmFires();
    const auto ns = uint64_t((monotonicTimeMs() - started) * 1000000);
    ++alarmTickSamples;++alarmTickUs[std::min<uint64_t>(1023, ns / 1000)];alarmTickMaxNs = std::max(alarmTickMaxNs, ns);
    if (measureCpu) alarmTickCpuNs += cpu() - thread;
  }
}

kj::Promise<void> DurableLog::fireAlarm(std::string key, uint64_t token) {
  auto found = alarms.find(key);
  if (found == alarms.end() || found->second.fireToken != token) co_return;
  DurableAlarm ask{found->second.namespaceId, found->second.objectId, found->second.tenant, found->second.shard,
      found->second.facetId, found->second.dataEpoch, found->second.shardEpoch, found->second.atUnixMs,
      found->second.retries, token,
      settings.threadedAlarmEvents && found->second.clearing};
  const auto started = alarmMonotonicMs();
  AlarmResult result;
  try {
    kj::Promise<AlarmResult> operation = nullptr;
    { AlarmCpuScope measured(*this);++alarmAttempts;operation = hooks.fireAlarm(ask); }
    result = co_await kj::mv(operation);
  } catch (const kj::Exception& error) {
    result = AlarmResult{AlarmOutcome::UNAVAILABLE, 0, std::string(error.getDescription().cStr())};
  }
  AlarmCpuScope measured(*this);
  if (result.outcome == AlarmOutcome::DEFERRED) closeAlarmCapacity(result.capacityKey.empty() ? "node" : result.capacityKey, result.capacityObservedMs);
  // Counted from the outcome, before the schedule is looked at again: the object's own clearing of its alarm is
  // reported as soon as the handler's transaction is confirmed, which is usually before this answer arrives.
  switch (result.outcome) {
    case AlarmOutcome::FIRED: ++alarmsFired; break;
    case AlarmOutcome::NOT_DUE: ++alarmsNotDue; break;
    case AlarmOutcome::FAILED: ++alarmFailures; break;
    case AlarmOutcome::UNAVAILABLE: break;
    case AlarmOutcome::DEFERRED: ++alarmDeferred;break;
    case AlarmOutcome::DELETED: ++alarmInactive;break;
    case AlarmOutcome::NAMESPACE_WAIT: break;
    case AlarmOutcome::REBUILDING: break;
    case AlarmOutcome::CLEARED: break;
    case AlarmOutcome::STOP: break;
  }
  auto again = alarms.find(key);
  const auto gone = again == alarms.end();
  const auto fields = "\"namespaceId\":" + quote(ask.namespaceId) + ",\"objectId\":" + quote(ask.objectId.substr(0, 12)) +
      ",\"facet\":" + std::to_string(ask.facetId) + ",\"atUnixMs\":" + std::to_string(ask.atUnixMs) +
      ",\"elapsedMs\":" + std::to_string(alarmMonotonicMs() - started);
  if (gone) {
    // Its schedule is gone (the object reported the clearing, or the shard left this node): the hint goes with it.
    log(result.outcome == AlarmOutcome::FIRED ? "alarm-fired" : "alarm-done", fields + ",\"scheduled\":false");
    if (result.outcome == AlarmOutcome::FIRED && !settings.threadedAlarmEvents)
      writeAlarmHint(ask.namespaceId, ask.objectId, ask.shard, 0, ask.facetId, ask.shardEpoch, ask.tenant);
    co_return;
  }
  auto& state = again->second;
  if (state.fireToken != token) co_return;
  state.firing = false;
  auto requeue = kj::defer([this, &key, token]() {
    auto it = alarms.find(key);if (it != alarms.end() && it->second.fireToken == token) indexAlarm(key, it->second);
  });
  if (state.atUnixMs != ask.atUnixMs || state.shardEpoch != ask.shardEpoch) {
    log("alarm-completion-superseded", fields);
    co_return;
  }
  switch (result.outcome) {
    case AlarmOutcome::FIRED: {
      log("alarm-fired", fields + ",\"retries\":" + std::to_string(state.retries));
      // The handler cleared the object's own alarm, and the report of that clears the schedule. A report that did
      // not come (the object went away with the answer in flight) leaves this entry behind, so it goes now; the
      // object's database is still the truth and reports again at its next open.
      if (settings.threadedAlarmEvents) {
        // The WAL channel reports the committed schedule independently of the reply.
        state.retries = 0;
        state.countedRetries = 0;
        state.backoff = 0;
        state.clearing = false;
        state.notBeforeMs = alarmMonotonicMs() + 1000;
      } else if (state.atUnixMs == ask.atUnixMs) {
        eraseAlarm(key);
        writeAlarmHint(ask.namespaceId, ask.objectId, ask.shard, 0, ask.facetId, ask.shardEpoch, ask.tenant);
      }
      co_return;
    }
    case AlarmOutcome::NOT_DUE: {
      log("alarm-not-due", fields + ",\"holds\":" + std::to_string(result.atUnixMs));
      if (settings.threadedAlarmEvents) {
        if (auto* object = objectOf(durableHostName(ask.namespaceId, ask.objectId, ask.dataEpoch))) {
          alarmReported(object->objectName, ask.facetId, result.atUnixMs);
          co_return;
        }
      }
      state.fromHint = false;
      if (result.atUnixMs == 0) {
        eraseAlarm(key);
        writeAlarmHint(ask.namespaceId, ask.objectId, ask.shard, 0, ask.facetId, ask.shardEpoch, ask.tenant);
      } else {
        state.atUnixMs = result.atUnixMs;
        state.dueObserved = false;
        state.retries = 0;
        state.countedRetries = 0;
        state.backoff = 0;
        state.clearing = false;
        state.notBeforeMs = 0;
        writeAlarmHint(ask.namespaceId, ask.objectId, ask.shard, result.atUnixMs, ask.facetId, ask.shardEpoch, ask.tenant);
      }
      co_return;
    }
    case AlarmOutcome::FAILED: {
      ++state.retries;
      if (settings.threadedAlarmEvents && state.retries > ALARM_RETRY_MAX_TRIES) {
        state.clearing = true;
        log("alarm-retry-exhausted", fields);
        co_return;  // The next validated dispatch conditionally clears the stored time.
      }
      ++alarmRetries;
      if (state.countedRetries < ALARM_RETRY_MAX_TRIES) {
        ++state.countedRetries;
      } else {
        state.backoff = std::min<uint32_t>(state.backoff + 1, ALARM_RETRY_BACKOFF_MAX);
      }
      const uint32_t steps = state.countedRetries >= ALARM_RETRY_MAX_TRIES
          ? ALARM_RETRY_MAX_TRIES - 1 + state.backoff
          : state.retries - 1;
      const uint64_t seconds = uint64_t(ALARM_RETRY_START_SECONDS) << std::min<uint32_t>(steps, ALARM_RETRY_BACKOFF_MAX);
      // A quarter of jitter, so the retries of many objects do not land together.
      const auto jitter = seconds * 1000 / 4;
      const uint64_t delay = seconds * 1000 - jitter / 2 + (jitter ? (randomBelow(jitter)) : 0);
      state.notBeforeMs = alarmMonotonicMs() + double(delay);
      log("alarm-failed", fields + ",\"retries\":" + std::to_string(state.retries) + ",\"afterMs\":" +
              std::to_string(delay) + ",\"reason\":" + quote(result.reason.substr(0, 200)));
      co_return;
    }
    case AlarmOutcome::CLEARED:
      if (auto* object = objectOf(durableHostName(ask.namespaceId, ask.objectId, ask.dataEpoch))) {
        alarmReported(object->objectName, ask.facetId, 0);
      } else {
        eraseAlarm(key);
        writeAlarmHint(ask.namespaceId, ask.objectId, ask.shard, 0, ask.facetId, ask.shardEpoch, ask.tenant);
      }
      co_return;
    case AlarmOutcome::STOP:
      state.clearing = true;
      state.notBeforeMs = 0;
      log("alarm-no-retry", fields);
      co_return;
    case AlarmOutcome::DEFERRED:
      // The destination gate holds the whole bucket; only one alarm probes it after 200ms.
      co_return;
    case AlarmOutcome::DELETED:
      eraseAlarm(key);
      co_return;
    case AlarmOutcome::NAMESPACE_WAIT:
      state.notBeforeMs = alarmMonotonicMs() + 500;
      co_return;
    case AlarmOutcome::REBUILDING:
      KJ_IF_SOME(resume, result.resume) {
        state.rebuildWaiting = true;++alarmRebuildWaiting;
        peakAlarmRebuildWaiting = std::max(peakAlarmRebuildWaiting, alarmRebuildWaiting);
        tasks.add(alarmRebuilt(key, token, kj::mv(resume)));
      } else state.notBeforeMs = alarmMonotonicMs() + 1000;
      co_return;
    case AlarmOutcome::UNAVAILABLE: {
      // Nothing ran: this node may not own the shard any more, or the object could not be rebuilt. Not counted
      // against the retries; tried again shortly, and the shard's new owner has the hint either way.
      state.notBeforeMs = alarmMonotonicMs() + 1000;
      log("alarm-unavailable", fields + ",\"reason\":" + quote(result.reason.substr(0, 200)));
      co_return;
    }
  }
}

void DurableLog::shardTaken(const std::string& namespaceId, uint32_t shard, uint64_t shardEpoch,
    const std::string& tenant) {
  if (!enabled()) return;
  tasks.add(readAlarmHints(namespaceId, shard, shardEpoch, tenant));
}

// ---- What this machine's files are at, so the same boot reuses them ----
// This machine's files are a cache. Keeping "I have served this data epoch" in memory alone used to discard the
// cache at the first request after a daemon or durable-process restart, so an object was rebuilt
// from its whole history (a gibibyte took 19.5 s). The position beside the files says how far they are confirmed, so
// a same-boot start can reuse a closed, admitted cache. A clean exit alone never
// authorizes reuse after a machine reboot.
bool DurableLog::writePosition(const Object& object) {
  if (object.state != ObjectState::LOCAL || object.storageId.empty()) return false;
  std::ostringstream out;
  out << "{\"objectName\":" << quote(object.objectName) << ",\"namespaceId\":" << quote(object.namespaceId)
      << ",\"objectId\":" << quote(object.objectId) << ",\"tenant\":" << quote(object.tenant)
      << ",\"dataEpoch\":\"" << object.dataEpoch << "\",\"shardEpoch\":\"" << object.shardEpoch
      << "\",\"shard\":" << object.shard << ",\"nodeId\":" << quote(settings.nodeId)
      << ",\"positionNodeEpoch\":\"" << nodeEpoch << "\",\"cacheOpen\":" << (object.cacheOpen ? "true" : "false")
      << ",\"openEpoch\":\"" << object.openEpoch << "\",\"openGeneration\":\"" << object.openGeneration
      << "\",\"imageRepairEpoch\":\"" << object.imageRepairGeneration.first
      << "\",\"imageRepairSerial\":\"" << object.imageRepairGeneration.second
      << "\",\"recoveryPublished\":" << (object.recoveryPublished ? "true" : "false")
      << ",\"snapshotSequence\":\"" << object.snapshotSequence << "\",\"snapshotAtUnixMs\":"
      << object.snapshotAtUnixMs << ",\"sinceSnapshotBytes\":\"" << object.sinceSnapshotBytes
      << "\",\"bytes\":\"" << object.bytes << "\",\"atUnixMs\":" << realClockUnixMs() << ",\"facets\":[";
  bool first = true;
  for (const auto& [facetId, facet] : object.facets) {
    out << (first ? "" : ",") << "{\"facetId\":" << facetId << ",\"parentFacetId\":" << facet.parentFacetId
        << ",\"facetName\":" << quote(facet.facetName) << ",\"txn\":\"" << facet.txn << "\",\"generation\":\""
        << facet.generation << "\",\"lastSeq\":\"" << facet.lastSeq << "\",\"checkpointedSeq\":\""
        << facet.checkpointedSeq << "\",\"lastFrame\":" << facet.lastFrame << ",\"salt1\":" << facet.salt1
        << ",\"salt2\":" << facet.salt2 << ",\"databaseBytes\":\"" << facet.databaseBytes
        << "\",\"deleted\":" << (facet.deleted ? "true" : "false")
        << ",\"checkpointGeneration\":\"" << facet.checkpointGeneration << "\",\"checkpointFrames\":" << facet.checkpointFrames
        << ",\"checkpointSalt1\":" << facet.checkpointSalt1 << ",\"checkpointSalt2\":" << facet.checkpointSalt2
        << ",\"checkpointSafe\":" << (facet.checkpointSafe ? "true" : "false") << "}";
    first = false;
  }
  out << "]}\n";
  const auto path = object.positionPath(settings.stackDirectory);
  const auto bytes = out.str();
  if (!atomicLocalFile(path, bytes, false)) return false;
  ++positionWrites;
  return true;
}

bool DurableLog::channelHasOpenObject(const Channel& channel, const Object& object) const {
  const auto prefix = object.objectName + "/";
  const auto generation = std::make_pair(object.openEpoch, object.openGeneration);
  for (auto it = channel.openFacets.lower_bound(prefix);
      it != channel.openFacets.end() && it->first.starts_with(prefix); ++it)
    if (it->second.position == generation) return true;
  return false;
}

bool DurableLog::hasLiveOpenFacets(const Object& object) const {
  for (const auto& [id, channel] : channels)
    if (channel.open && channelHasOpenObject(channel, object)) return true;
  return false;
}

bool DurableLog::hasLiveHolders(const Object& object) const {
  for (const auto& [id, holder] : object.holders) {
    const auto channel = channels.find(id);
    if ((holder.requests != 0 || holder.connections != 0) && channel != channels.end() && channel->second.open)
      return true;
  }
  return false;
}

bool DurableLog::hasLiveDispatch(const Object& object) const {
  const auto channel = channels.find(object.dispatchedChannel);
  return channel != channels.end() && channel->second.open;
}

std::optional<std::string> DurableLog::prepareOpen(Object& object, const std::string* targetChannel) {
  if (recoveryWriteFailed || object.state != ObjectState::LOCAL) return "the object open marker could not be written";
  const bool live = targetChannel == nullptr ? hasLiveOpenFacets(object) : [&]() {
    const auto channel = channels.find(*targetChannel);
    if (channel == channels.end() || !channel->second.open) return false;
    return channelHasOpenObject(channel->second, object);
  }();
  if (live) {
    if (targetChannel != nullptr && !object.cacheOpen) {
      object.cacheOpen = true;
      if (!writePosition(object)) {
        object.cacheOpen = false;
        log("open-marker-failed", "\"object\":" + quote(object.objectName) + ",\"errno\":" + std::to_string(errno));
        return "the object open marker could not be written";
      }
    }
    return std::nullopt;
  }
  const bool dispatched = hasLiveDispatch(object) &&
      (targetChannel == nullptr || object.dispatchedChannel == *targetChannel);
  if (dispatched && object.cacheOpen) return std::nullopt;
  if (!dispatched) {
    object.dispatchedChannel.clear();
    object.dispatchPending = false;
    for (const auto& [facetId, facet] : object.facets)
      if (!facet.deleted && !facet.loggedCheckpointCovered) {
        // Even a failed cut can have changed the files before reporting its error.
        dirtyCacheSizes.insert(object.objectName);
        if (auto error = cutBackWal(object, facetId, facet.loggedSalt1, facet.loggedSalt2, facet.loggedPageSize, facet.loggedFrame))
          return error;
      }
  }
  const bool wasOpen = object.cacheOpen;
  object.openGeneration = object.openEpoch == nodeEpoch ? object.openGeneration + 1 : 1;
  object.openEpoch = nodeEpoch;object.cacheOpen = true;
  if (writePosition(object)) return std::nullopt;
  object.cacheOpen = wasOpen;
  log("open-marker-failed", "\"object\":" + quote(object.objectName) + ",\"errno\":" + std::to_string(errno));
  return "the object open marker could not be written";
}



void DurableLog::maybeCloseMarker(Object& object) {
  if (object.state != ObjectState::LOCAL || recoveryWriteFailed) return;
  if (auto state = recoveryStates.find(object.objectName); state != recoveryStates.end() &&
      (state->second.repairPending || state->second.rebuildPending)) return;
  for (const auto& [facetId, facet] : object.facets)
    if (facet.open || !facet.checkpointSafe) return;
  const bool wasOpen = object.cacheOpen;
  object.cacheOpen = false;
  if (!writePosition(object)) object.cacheOpen = wasOpen;
}

void DurableLog::channelEnded(Channel& channel, const std::string& reason) {
  channel.open = false;
  // Bytes already read from the process remain authoritative. In particular a buffered close
  // may remove a facet from openFacets before we decide which objects need repair.
  KJ_IF_SOME(error, kj::runCatchingExceptions([&]() { consumeChannelInbox(channel, false); })) {
    log("channel-drain-failed", "\"processId\":" + quote(channel.processId) + ",\"reason\":" +
        quote(std::string(error.getDescription().cStr())));
  }
  // A write-side failure can precede EOF. Retain an incomplete tail for the reader;
  // explicit close destroys the inbox after this drain.
  std::set<std::string> affected;
  for (const auto& [key, registration] : channel.openFacets) {
    const auto split = key.rfind('/');
    if (split == std::string::npos) continue;
    auto* object = objectOf(key.substr(0, split));
    if (object == nullptr || registration.position != std::make_pair(object->openEpoch, object->openGeneration)) continue;
    auto facet = object->facets.find(uint32_t(decimal(key.substr(split + 1))));
    if (facet == object->facets.end() || facet->second.processId != channel.processId || !facet->second.open) continue;
    facet->second.open = false;affected.insert(object->objectName);
  }
  channel.openFacets.clear();
  for (const auto& name : affected) {
    auto* object = objectOf(name);
    // An ongoing rebuild already has its begin marker. Killing its live
    // process must not restart that rebuild or reset other processes again.
    if (object != nullptr && object->state == ObjectState::LOCAL) markRepair(*object, reason);
    dirtyCacheSizes.insert(name);
  }
}

kj::Promise<void> DurableLog::readPositions(bool slice) {
  std::error_code code;
  const auto directories = co_await objectDirectories(code, slice);
  StartupSlice budget(slice);
  uint64_t restored = 0, removed = 0;
  for (const auto& directory : directories) {
    if (code) break;
    for (std::filesystem::directory_iterator it(directory, code), end;
        !code && it != end; it.increment(code)) {
      if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
      if (it->path().extension() != ".position") continue;
      if (!std::filesystem::is_regular_file(it->symlink_status(code))) continue;
      try {
        const auto bytes = readFile(it->path());
        capnp::MallocMessageBuilder message;
        auto value = message.initRoot<capnp::JsonValue>();
        capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), value);
        auto reader = value.asReader();
        const auto objectName = json::text(reader, "objectName");
        const auto namespaceId = json::text(reader, "namespaceId");
        const auto objectId = json::text(reader, "objectId");
        const auto dataEpoch = decimal(json::text(reader, "dataEpoch"));
        if (namespaceId.size() != 64 || objectId.size() != 64 ||
            objectName != durableHostName(namespaceId, objectId, dataEpoch)) continue;
        auto ref = makeRootStorageRef(namespaceId, objectId, dataEpoch);
        if (it->path() != ref.positionPath(settings.stackDirectory)) continue;
        auto& object = objects[objectName];
        // Positions retain identity and pre-open usage, never a reusable image.
        static_cast<RootStorageRef&>(object) = std::move(ref);
        object.objectName = objectName;
        object.tenant = json::text(reader, "tenant");
        object.shardEpoch = std::max(object.shardEpoch, decimal(json::text(reader, "shardEpoch")));
        object.shard = uint32_t(numberField(reader, "shard"));
        object.bytes = decimal(json::text(reader, "bytes"));
        if (auto state = recoveryStates.find(objectName); state != recoveryStates.end() && state->second.repairPending)
          object.repairGeneration = state->second.repair;
        ++restored;
      } catch (const kj::Exception&) {
        continue;
      }
    }
  }
  if (restored) log("positions-read", "\"objects\":" + std::to_string(restored));
  for (const auto& directory : directories) {
    if (code) break;
    for (std::filesystem::directory_iterator it(directory, code), end;
        !code && it != end; it.increment(code)) {
      if (budget.elapsed()) { co_await timer.afterDelay(0 * kj::MILLISECONDS); budget.reset(); }
      if (it->path().filename().string().ends_with(".position")) continue;
      std::error_code removal;
      std::filesystem::remove_all(it->path(), removal);
      if (removal) {
        ++startupCleanupFailures;
        log("cache-cleanup-failed", "\"file\":" + quote(it->path().filename().string()) +
            ",\"reason\":" + quote(removal.message()));
      } else ++removed;
    }
  }
  if (code) {
    ++startupCleanupFailures;
    log("cache-cleanup-failed", "\"reason\":" + quote(code.message()));
  }
  filesDropped += removed;
  if (removed) log("cache-dropped", "\"reason\":\"rebuild on first open after restart\",\"entries\":" + std::to_string(removed));
}



// ---- Durable Object storage per tenant (the durableObjectBytes quota) ----
// Whether a namespace still counts: a deleted class's or a deleted database's objects are not served any more.
bool DurableLog::namespaceCounts(const std::string& namespaceId) const {
  return !hooks.namespaceActive || hooks.namespaceActive(namespaceId);
}

uint64_t DurableLog::tenantBytesHere(const std::string& tenant) const {
  uint64_t bytes = 0;
  for (const auto& [name, object] : objects) {
    if (object.tenant != tenant || object.state == ObjectState::GONE) continue;
    if (!namespaceCounts(object.namespaceId)) continue;
    bytes += object.bytes;
  }
  return bytes;
}

uint64_t DurableLog::tenantBytes(const std::string& tenant) const {
  uint64_t bytes = tenantBytesHere(tenant);
  for (const auto& [nodeId, tenants] : publishedBytes) {
    if (nodeId == settings.nodeId) continue;
    const auto found = tenants.find(tenant);
    if (found != tenants.end()) bytes += found->second;
  }
  return bytes;
}

std::string DurableLog::namespacesJson(const std::string& tenant) const {
  std::map<std::string, std::pair<uint64_t, uint64_t>> byNamespace;  // objects, bytes
  std::map<std::string, std::set<uint32_t>> shardsOf;
  for (const auto& [name, object] : objects) {
    if (!tenant.empty() && object.tenant != tenant) continue;
    if (object.state == ObjectState::GONE) continue;
    auto& entry = byNamespace[object.namespaceId];
    ++entry.first;
    entry.second += object.bytes;
    shardsOf[object.namespaceId].insert(object.shard);
  }
  std::ostringstream out;
  out << "{\"atUnixMs\":" << realClockUnixMs() << ",\"namespaces\":[";
  bool first = true;
  for (const auto& [namespaceId, entry] : byNamespace) {
    out << (first ? "" : ",") << "{\"namespaceId\":" << quote(namespaceId) << ",\"objects\":" << entry.first
        << ",\"bytes\":\"" << entry.second << "\",\"shards\":[";
    bool firstShard = true;
    for (const auto shard : shardsOf[namespaceId]) {
      out << (firstShard ? "" : ",") << shard;
      firstShard = false;
    }
    out << "]}";
    first = false;
  }
  out << "]}";
  return out.str();
}

kj::Promise<void> DurableLog::usageLoop() {
  for (;;) {
    try {
      co_await publishUsage();
      co_await readUsage();
    } catch (const kj::Exception& error) {
      log("usage-failed", "\"error\":" + quote(std::string(error.getDescription().cStr())));
    }
    co_await timer.afterDelay(std::max<uint32_t>(settings.config.usagePublishMs, 1000) * kj::MILLISECONDS);
  }
}

kj::Promise<void> DurableLog::publishUsage() {
  if (!enabled()) co_return;
  r47::Span build(r47::USAGE_BUILD);
  std::map<std::string, uint64_t> mine;
  for (const auto& [name, object] : objects) {
    if (object.tenant.empty() || object.state == ObjectState::GONE) continue;
    if (!namespaceCounts(object.namespaceId)) continue;
    mine[object.tenant] += object.bytes;
  }
  build.finish(objects.size());
  std::ostringstream out;
  out << "{\"nodeId\":" << quote(settings.nodeId) << ",\"atUnixMs\":" << realClockUnixMs() << ",\"tenants\":{";
  bool first = true;
  for (const auto& [tenant, bytes] : mine) {
    out << (first ? "" : ",") << quote(tenant) << ":\"" << bytes << "\"";
    first = false;
  }
  out << "}}\n";
  try {
    co_await store->put(nodeDurableUsageKey(settings.nodeId), out.str());
    ++usagePublishes;
    lastUsagePublishMs = monotonicTimeMs();
    publishedBytes[settings.nodeId] = std::move(mine);
  } catch (const kj::Exception& error) {
    ++usagePublishFailures;
    log("usage-publish-failed", "\"error\":" + quote(std::string(error.getDescription().cStr())));
  }
}

kj::Promise<void> DurableLog::readUsage() {
  if (!enabled()) co_return;
  // A membership refresh swaps its map while a store read is suspended. Keep this pass's node ids alive.
  std::vector<std::string> nodeIds;
  nodeIds.reserve(membership.members.size());
  for (const auto& [nodeId, member] : membership.members) {
    if (nodeId != settings.nodeId) nodeIds.push_back(nodeId);
  }
  for (const auto& nodeId : nodeIds) {
    try {
      auto answer = co_await store->get(nodeDurableUsageKey(nodeId));
      if (answer.status != 200) continue;
      capnp::MallocMessageBuilder message;
      auto value = message.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(answer.bytes.data(), answer.bytes.size()), value);
      auto reader = value.asReader();
      std::map<std::string, uint64_t> theirs;
      if (auto tenants = json::field(reader, "tenants"); tenants.isObject()) {
        for (auto field : tenants.getObject()) {
          if (!field.getValue().isString()) continue;
          theirs[std::string(field.getName().cStr())] = decimal(std::string(field.getValue().getString().cStr()));
        }
      }
      publishedBytes[nodeId] = std::move(theirs);
      ++usageReads;
    } catch (const kj::Exception&) {
      continue;
    }
  }
  lastUsageReadMs = monotonicTimeMs();
}

bool DurableLog::ownerFenced(const std::string& namespaceId, uint32_t shard, uint64_t epoch) const {
  const auto found = ownerFenceThrough.find({namespaceId, shard});
  return epoch && found != ownerFenceThrough.end() && epoch <= found->second;
}

uint64_t DurableLog::knownEpoch(const std::string& namespaceId, uint32_t shard) const {
  uint64_t known = hooks.knownShardEpoch ? hooks.knownShardEpoch(namespaceId, shard) : 0;
  if (!ownerFenceThrough.empty()) {
    if (const auto found = ownerFenceThrough.find({namespaceId, shard}); found != ownerFenceThrough.end())
      known = std::max(known, found->second + 1);
  }
  return known;
}

void DurableLog::fenceOwner(const std::string& namespaceId, uint32_t shard, uint64_t through,
    const std::string& reason, bool notifyShard) {
  if (!through) return;
  auto& boundary = ownerFenceThrough[{namespaceId, shard}];
  if (through > boundary) {
    boundary = through;
    ++ownerFenceEvents;
    log("owner-fenced", "\"namespaceId\":" + quote(namespaceId) + ",\"shard\":" + std::to_string(shard) +
        ",\"throughShardEpoch\":\"" + std::to_string(through) + "\",\"reason\":" + quote(reason));
  }
  // A shard a peer's fenced refusal made this node check is lost once the fence covers the refused record.
  if (!lostShardChecks.empty()) {
    if (const auto watched = lostShardChecks.find({namespaceId, shard});
        watched != lostShardChecks.end() && watched->second <= boundary) {
      ++fencedShardsLost;
      log("shard-lost-fenced", "\"namespaceId\":" + quote(namespaceId) + ",\"shard\":" + std::to_string(shard) +
          ",\"shardEpoch\":\"" + std::to_string(watched->second) + "\"");
      lostShardChecks.erase(watched);
    }
  }
  for (auto& [name, object] : objects) {
    if (object.namespaceId != namespaceId || object.shard != shard || object.shardEpoch > boundary) continue;
    isolateObjectPending(object, reason);
    object.state = ObjectState::GONE;
    ++object.snapshotVersion;
    if (auto canceler = rebuildCancelers.find(name); canceler != rebuildCancelers.end())
      canceler->second->cancel("the recovery owner epoch was fenced");
  }
  if (notifyShard && ownerFenceHook) ownerFenceHook(namespaceId, shard, boundary);
}

void DurableLog::shardGone(const std::string& namespaceId, uint32_t shard, const std::string& reason) {
  if (alarmPublication) alarmPublication->gone(namespaceId, shard);
  uint64_t through = hooks.knownShardEpoch ? hooks.knownShardEpoch(namespaceId, shard) : 0;
  for (const auto& [name, object] : objects)
    if (object.namespaceId == namespaceId && object.shard == shard) through = std::max(through, object.shardEpoch);
  // DurableShards is already dropping its holdings. Do not mutate that map
  // through the proof callback while it is iterating over the same holdings.
  fenceOwner(namespaceId, shard, through, reason, false);
  uint32_t count = 0;
  for (auto& [name, object] : objects) {
    if (object.namespaceId != namespaceId || object.shard != shard) continue;
    object.state = ObjectState::GONE;
    if (auto canceler = rebuildCancelers.find(name); canceler != rebuildCancelers.end())
      canceler->second->cancel("the shard was lost during object recovery");
    ++count;
  }
  uint32_t droppedAlarms = 0;
  for (auto it = alarms.begin(); it != alarms.end();) {
    if (it->second.namespaceId == namespaceId && it->second.shard == shard &&
        (settings.threadedAlarmEvents || !it->second.firing)) {
      unindexAlarm(it->first, it->second);
      it = alarms.erase(it);
      ++droppedAlarms;
    } else {
      ++it;
    }
  }
  alarmsDropped += droppedAlarms;
  if (count || droppedAlarms)
    log("shard-gone", "\"namespaceId\":" + quote(namespaceId) + ",\"shard\":" + std::to_string(shard) +
            ",\"objects\":" + std::to_string(count) + ",\"alarms\":" + std::to_string(droppedAlarms) +
            ",\"reason\":" + quote(reason));
}

// ---- As a peer of other nodes ----

std::string DurableLog::openIncoming(const std::string& nodeId, uint64_t ownerEpoch, const std::string& instance,
    const std::string& destinationEpoch, uint64_t firstSeq, uint64_t& streamId) {
  if (!enabled()) return "this node keeps no Durable Object log";
  if (!nodeIdName(nodeId) || !ownerEpoch) return "the session does not name a node and an epoch";
  if (!destinationEpoch.empty() && !membership.heldEpochs.contains(destinationEpoch))
    return "this node does not hold the epoch the stream is for";
  // One stream per owner epoch: an owner that opens a new one leaves the old one's records where they are until a
  // segment covers them or the owner says they are isolated.
  for (auto& [id, buffer] : incoming) {
    if (buffer.nodeId == nodeId && buffer.nodeEpoch == ownerEpoch && buffer.instance == instance) {
      streamId = id;
      buffer.openedAtMs = monotonicTimeMs();
      // The pieces of a record the previous stream was carrying are not part of what this one sends.
      incomingBytes -= std::min<uint64_t>(incomingBytes, buffer.fragment.size());
      buffer.bytes -= std::min<uint64_t>(buffer.bytes, buffer.fragment.size());
      buffer.fragment.clear();
      return {};
    }
  }
  streamId = ++nextStreamId;
  auto& buffer = incoming[streamId];
  buffer.nodeId = nodeId;
  buffer.nodeEpoch = ownerEpoch;
  buffer.instance = instance;
  buffer.streamId = streamId;
  buffer.droppedTo = firstSeq ? firstSeq - 1 : 0;
  buffer.openedAtMs = monotonicTimeMs();
  log("peer-stream", "\"owner\":" + quote(nodeId) + ",\"ownerEpoch\":\"" + std::to_string(ownerEpoch) +
          "\",\"firstSeq\":\"" + std::to_string(firstSeq) + "\",\"stream\":\"" + std::to_string(streamId) + "\"");
  return {};
}

// The records of one append: each is checked against its own checksum and against the shard's epoch this node knows
// -- an owner that lost its shard is confirmed by nobody -- and buffered until the owner says a segment covers them.
DurableLog::AppendResult DurableLog::appendIncoming(uint64_t streamId, std::string_view bytes, bool partial) {
  AppendResult result;
  auto found = incoming.find(streamId);
  if (found == incoming.end()) {
    result.refusal = "this stream is not open";
    return result;
  }
  auto& buffer = found->second;
  if (!bytes.empty()) MemoryWatermark::noteDaemonAppend();
  if (partial) {
    // A piece of a record the next calls complete: held, counted, and taken only when the record is whole.
    const bool tooLarge = buffer.fragment.size() + bytes.size() > tailRecordBytes() + 4;
    if (incomingBytes + bytes.size() > settings.config.peerBufferBytes || tooLarge) {
      if (tooLarge) ++recordLimitRefusals;
      ++bufferRefusals;
      incomingBytes -= std::min<uint64_t>(incomingBytes, buffer.fragment.size());
      buffer.bytes -= std::min<uint64_t>(buffer.bytes, buffer.fragment.size());
      buffer.fragment.clear();
      result.accepted = buffer.answeredSeq();
      result.refusal = tooLarge ? "a fragmented record exceeds the recoverable RPC limit"
          : "full: this node buffers durableObjects.peerBufferBytes for the owners it is a peer of";
      return result;
    }
    buffer.fragment += std::string(bytes);
    buffer.bytes += bytes.size();
    incomingBytes += bytes.size();
    result.accepted = buffer.answeredSeq();
    return result;
  }
  std::string whole;
  if (!buffer.fragment.empty()) {
    whole = std::move(buffer.fragment);
    buffer.fragment.clear();
    incomingBytes -= std::min<uint64_t>(incomingBytes, whole.size());
    buffer.bytes -= std::min<uint64_t>(buffer.bytes, whole.size());
    whole += std::string(bytes);
    bytes = whole;
  }
  // A record this node already holds, sent again after a reconnect, is taken again (as a duplicate) rather than
  // refused for room it does not need.
  if (incomingBytes + bytes.size() > settings.config.peerBufferBytes && !buffer.holdsFramed(bytes)) {
    ++bufferRefusals;
    ++buffer.refusedRecords;
    result.accepted = buffer.answeredSeq();
    result.refusal = "full: this node buffers durableObjects.peerBufferBytes for the owners it is a peer of";
    return result;
  }
  std::string error;
  auto records = splitRecords(bytes, &error);
  if (!error.empty()) {
    result.accepted = buffer.answeredSeq();
    result.refusal = "the records are cut short: " + error;
    return result;
  }
  for (const auto& encoded : records) {
    if (encoded.size() > tailRecordBytes()) {
      ++recordLimitRefusals;
      result.accepted = buffer.answeredSeq();
      result.refusal = "a record exceeds the recoverable RPC limit";
      return result;
    }
    std::string reason;
    auto record = decodeLogRecord(encoded, &reason);
    if (!record) {
      // A record that does not add up is refused here rather than at a rebuild (experiment 25, B.7).
      result.accepted = buffer.answeredSeq();
      result.refusal = "a record does not add up: " + reason;
      log("record-refused", "\"owner\":" + quote(buffer.nodeId) + ",\"reason\":" + quote(reason));
      return result;
    }
    if (record->nodeEpoch != buffer.nodeEpoch) {
      result.accepted = buffer.answeredSeq();
      result.refusal = "a record of another epoch of the owner";
      return result;
    }
    // A record this node already holds, sent again after the owner reconnected, was taken before any floor that
    // would refuse it now and is in a newer owner's tail: it is taken again as a duplicate, not refused -- refusing it
    // would hold back the owner's answers for its other shards until a trim. Only the same bytes are that record; a
    // different record under a held sequence is checked like any new one.
    if (record->nodeSeq <= buffer.droppedTo || buffer.isolated.contains(record->nodeSeq)) continue;
    const auto heldAt = buffer.held.find(record->nodeSeq);
    const bool duplicate = heldAt != buffer.held.end() && heldAt->second == encoded;
    if (record->kind != LogRecordKind::SEAL && !duplicate) {
      const auto known = knownEpoch(record->objectName.substr(0, record->objectName.find('/')), record->shard);
      if (known != 0 && record->shardEpoch < known) {
        ++fencedRecords;
        // The refused record is not held, so no answer of this stream may reach it until the owner drops it.
        if (record->nodeSeq > buffer.droppedTo && !buffer.isolated.contains(record->nodeSeq))
          buffer.fencedSeqs.insert(record->nodeSeq);
        result.accepted = buffer.answeredSeq();
        result.refusal = "fenced: this node knows a newer epoch of the shard";
        log("record-fenced", "\"owner\":" + quote(buffer.nodeId) + ",\"shard\":" + std::to_string(record->shard) +
                ",\"theirs\":\"" + std::to_string(record->shardEpoch) + "\",\"known\":\"" + std::to_string(known) + "\"");
        return result;
      }
    }
    auto [entry, added] = buffer.held.insert({record->nodeSeq, encoded});
    if (added) {
      buffer.bytes += encoded.size();
      incomingBytes += encoded.size();
      ++buffer.records;
    }
    const auto sequence = record->nodeSeq;
    if (added) {
      std::string().swap(record->frameBytes);
      std::string().swap(record->header);
      buffer.byObject[record->objectName].insert(sequence);
      buffer.positions.emplace(sequence, std::move(*record));
    }
    buffer.acceptedSeq = std::max(buffer.acceptedSeq, sequence);
  }
  buffer.lastAppendMs = monotonicTimeMs();
  result.accepted = buffer.answeredSeq();
  return result;
}

// Whether framed bytes are exactly one record this buffer already holds (or has dropped): read from the record's
// encoding (the kind byte, then the sequence, little endian); anything else is not.
bool DurableLog::PeerBuffer::holdsFramed(std::string_view framed) const {
  if (framed.size() < 13) return false;
  uint32_t length = 0;
  for (int i = 0; i < 4; i++) length |= uint32_t(uint8_t(framed[i])) << (8 * i);
  if (framed.size() != size_t(length) + 4) return false;
  uint64_t sequence = 0;
  for (int i = 0; i < 8; ++i) sequence |= uint64_t(uint8_t(framed[5 + i])) << (8 * i);
  return held.contains(sequence) || sequence <= droppedTo || isolated.contains(sequence);
}

void DurableLog::PeerBuffer::forgetPosition(uint64_t sequence) {
  auto position = positions.find(sequence);
  if (position == positions.end()) return;
  auto indexed = byObject.find(position->second.objectName);
  KJ_DASSERT(indexed != byObject.end() && indexed->second.contains(sequence));
  indexed->second.erase(sequence);
  if (indexed->second.empty()) byObject.erase(indexed);
  positions.erase(position);
}

void DurableLog::trimIncoming(uint64_t streamId, uint64_t upTo) {
  auto found = incoming.find(streamId);
  if (found == incoming.end()) return;
  auto& buffer = found->second;
  for (auto entry = buffer.held.begin(); entry != buffer.held.end();) {
    if (entry->first > upTo) break;
    incomingBytes -= std::min<uint64_t>(incomingBytes, entry->second.size());
    buffer.bytes -= std::min<uint64_t>(buffer.bytes, entry->second.size());
    buffer.forgetPosition(entry->first);
    entry = buffer.held.erase(entry);
  }
  buffer.droppedTo = std::max(buffer.droppedTo, upTo);
  buffer.isolated.erase(buffer.isolated.begin(), buffer.isolated.upper_bound(buffer.droppedTo));
  buffer.fencedSeqs.erase(buffer.fencedSeqs.begin(), buffer.fencedSeqs.upper_bound(buffer.droppedTo));
}

void DurableLog::dropIncoming(uint64_t streamId, const std::vector<uint64_t>& sequences) {
  auto found = incoming.find(streamId);
  if (found == incoming.end()) return;
  auto& buffer = found->second;
  for (auto sequence : sequences) {
    buffer.isolated.insert(sequence);
    buffer.fencedSeqs.erase(sequence);
    buffer.forgetPosition(sequence);
    auto entry = buffer.held.find(sequence);
    if (entry == buffer.held.end()) continue;
    incomingBytes -= std::min<uint64_t>(incomingBytes, entry->second.size());
    buffer.bytes -= std::min<uint64_t>(buffer.bytes, entry->second.size());
    buffer.held.erase(entry);
  }
  log("peer-dropped", "\"owner\":" + quote(buffer.nodeId) + ",\"records\":" + std::to_string(sequences.size()));
}

void DurableLog::closeIncoming(uint64_t streamId) {
  auto found = incoming.find(streamId);
  if (found == incoming.end()) return;
  incomingBytes -= std::min<uint64_t>(incomingBytes, found->second.bytes);
  incoming.erase(found);
}

bool DurableLog::canAnswerRestoredTail(const DurableTailAsk& ask, const std::string& destinationEpoch,
    std::string* refusal) {
  if (refusal != nullptr) refusal->clear();
  if (ask.ownerNodeId != settings.nodeId || !ask.ownerNodeEpoch || ask.ownerNodeEpoch == nodeEpoch ||
      destinationEpoch != std::to_string(ask.ownerNodeEpoch) || membership.heldEpochs.contains(destinationEpoch)) return false;
  auto found = predecessorLogs.find(ask.ownerNodeEpoch);
  if (diskTrusted && sameBootCache && found != predecessorLogs.end() && found->second.complete) return true;
  ++restoredLogUnavailableAnswers;
  const auto reason = !diskTrusted ? "untrusted-start" : !sameBootCache ? "different-boot" :
      found == predecessorLogs.end() ? "missing" : "incomplete";
  if (refusal != nullptr) *refusal = "this node does not hold the epoch; the complete predecessor log is unavailable (" +
      std::string(reason) + ")";
  log("restored-owner-log-unavailable", "\"namespaceId\":" + quote(ask.namespaceId) +
      ",\"shard\":" + std::to_string(ask.shard) + ",\"sourceNodeEpoch\":\"" +
      std::to_string(ask.ownerNodeEpoch) + "\",\"reason\":" + quote(reason));
  return false;
}

// What this node holds of another node's log for one shard: the records above `afterSeq`, without a gap, so that the
// new owner can tell how far the history reaches.
DurableTailAnswer DurableLog::tailFor(const DurableTailAsk& ask, bool remote) {
  return tailForPage(ask, remote, TAIL_ANSWER_BYTES);
}

// A peer answering a newer owner's tail request first raises its floor for the shard,
// so that an older owner -- one whose clock stood still while its machine was frozen, or an older version without the
// lease check -- cannot have an append confirmed here after the newer owner has cut its tail. The number in the
// request is believed only once the object store shows that epoch: an epoch object is created by the node that takes
// the shard, only if absent, so every owner of a lower epoch has been replaced, and the newest owner, whose epoch is
// at least this one, is never fenced by it. The answer is computed after the raise: an append taken before it is in
// the answer; one after it is refused. A request this node cannot check (store error or slowness, no such object, an
// object that is another epoch) is answered as before and raises nothing; the floor only grows. The requests of one
// epoch of a shard share one read (checkPeerFence), and a failed one is not read again for OWNER_REREAD_MS. A request
// for this node's own older tail (an owner proof) is not checked here: tailForPage fences for it as before, except
// that a restored epoch answers a request naming an object for that object without fencing (the shard is
// fenced when a proof is given for all of it). Checking an owner proof needs a wire signal or a new rule;
// that check is in the backlog.
kj::Promise<void> DurableLog::raisePeerFence(DurableTailAsk ask) {
  if (!enabled() || ask.shardEpoch < 2 || ask.namespaceId.empty() || ask.ownerNodeId == settings.nodeId) co_return;
  co_await raiseFloor(ask.namespaceId, ask.shard, ask.shardEpoch, "a newer epoch of the shard asked for a tail");
}

kj::Promise<void> DurableLog::raiseFloor(std::string namespaceId, uint32_t shard, uint64_t epoch, std::string reason) {
  if (!enabled() || epoch < 2 || epoch <= knownEpoch(namespaceId, shard)) co_return;
  const auto now = timer.now();
  for (auto it = fenceChecks.begin(); it != fenceChecks.end();)
    it = it->second.settled && now - it->second.at >= int64_t(OWNER_REREAD_MS) * kj::MILLISECONDS ? fenceChecks.erase(it)
                                                                                                    : std::next(it);
  const auto key = std::make_tuple(namespaceId, shard, epoch);
  auto found = fenceChecks.find(key);
  // A check of this epoch that failed within OWNER_REREAD_MS: answered at once, as before (a background read follows).
  if (found != fenceChecks.end() && found->second.settled) co_return;
  if (found == fenceChecks.end()) {
    ++peerFenceChecks;
    auto check = checkPeerFence(namespaceId, shard, epoch, 0, std::move(reason)).fork();
    found = fenceChecks.emplace(key, FenceCheck{kj::mv(check), now, false}).first;
  }
  co_await found->second.done.addBranch();
  // Settled: kept for OWNER_REREAD_MS (a failure is not read again meanwhile; a success is past the early return).
  if (auto settled = fenceChecks.find(key); settled != fenceChecks.end() && !settled->second.settled) {
    settled->second.settled = true;
    settled->second.at = timer.now();
  }
}

// The read behind raisePeerFence, shared by every request of the same epoch of the shard meanwhile. A success raises
// the floor here, before any of them is answered; a failure is kept for OWNER_REREAD_MS, and the epoch is read again
// in the background -- after OWNER_REREAD_MS, doubled each time, at most FENCE_REREADS times, while it is still not
// known -- so that the floor goes up soon after the store answers again.
kj::Promise<void> DurableLog::checkPeerFence(std::string namespaceId, uint32_t shard, uint64_t epoch, uint32_t reread,
    std::string reason) {
  std::string failure;
  try {
    const auto waitMs = std::max<uint32_t>(1, settings.peerTimeoutMs / 2);
    auto object = co_await timer.timeoutAfter(waitMs * kj::MILLISECONDS,
        store->get(shardEpochKey(namespaceId, shard, epoch)));
    if (object.status == 200) {
      const auto record = parseShardEpoch(object.bytes);
      if (record && record->epoch == epoch && record->namespaceId == namespaceId && record->shard == shard) {
        if (epoch > knownEpoch(namespaceId, shard)) {
          ++peerFenceRaises;
          fenceOwner(namespaceId, shard, epoch - 1, reason, true);
        }
        if (reread) fenceRereading.erase({namespaceId, shard, epoch});
        co_return;
      }
      failure = "the object at that epoch's key is not that epoch";
    } else {
      failure = "the store answered HTTP " + std::to_string(object.status);
    }
  } catch (const kj::Exception& error) {
    failure = error.getDescription().cStr();
  }
  ++peerFenceUnverified;
  log("peer-fence-unverified", "\"namespaceId\":" + quote(namespaceId) + ",\"shard\":" + std::to_string(shard) +
      ",\"shardEpoch\":\"" + std::to_string(epoch) + "\",\"reread\":" + std::to_string(reread) +
      ",\"reason\":" + quote(failure));
  const auto key = std::make_tuple(namespaceId, shard, epoch);
  if (reread == 0 && !fenceRereading.insert(key).second) co_return;  // a chain of rereads is already under way
  if (reread >= FENCE_REREADS) {
    fenceRereading.erase(key);
    co_return;
  }
  const auto waitMs = int64_t(OWNER_REREAD_MS) << reread;
  tasks.add(timer.afterDelay(waitMs * kj::MILLISECONDS).then([this, namespaceId, shard, epoch, reread, reason]() {
    if (epoch > knownEpoch(namespaceId, shard)) return checkPeerFence(namespaceId, shard, epoch, reread + 1, reason);
    fenceRereading.erase({namespaceId, shard, epoch});
    return kj::Promise<void>(kj::READY_NOW);
  }));
}

DurableTailAnswer DurableLog::tailForPage(const DurableTailAsk& ask, bool remote, size_t pageBytes) {
  DurableTailAnswer answer;
  if (!enabled()) {
    answer.refusal = "this node keeps no Durable Object log";
    return answer;
  }
  if (ask.shardEpoch && knownEpoch(ask.namespaceId, ask.shard) > ask.shardEpoch) {
    answer.refusal = "the requesting shard epoch has been fenced";
    return answer;
  }
  const auto sourceEpoch = std::to_string(ask.ownerNodeEpoch);
  if (ask.ownerNodeId == settings.nodeId && ask.ownerNodeEpoch && ask.ownerNodeEpoch != nodeEpoch &&
      !membership.heldEpochs.contains(sourceEpoch)) {
    if (!canAnswerRestoredTail(ask, sourceEpoch)) {
      answer.refusal = "this node does not hold the epoch; the complete predecessor log is unavailable";
      return answer;
    }
    const auto& info = predecessorLogs.at(ask.ownerNodeEpoch);
    const auto pending = info.unpublishedShards.find({ask.namespaceId, ask.shard});
    const bool shardPending = pending != info.unpublishedShards.end() && pending->second;
    // A request naming its object is answered for that object: once no record of it in this epoch is left
    // unnamed by a manifest, the rest of the shard does not make the new owner wait. Never "nothing left" while any
    // record of the object is unpublished (restoredObjectPending). A request without an object is answered for the
    // shard, as before.
    const auto named = ask.objectId.empty() ? std::string() : ask.namespaceId + "/" + ask.objectId;
    const auto tracked = info.unpublishedTracked.find({ask.namespaceId, ask.shard});
    const bool accounted = !shardPending ||
        (tracked != info.unpublishedTracked.end() && tracked->second == pending->second);
    const bool stillPending = (named.empty() || !accounted ||
        restoredTailControlForTest == RestoredTailControl::SHARD_ONLY)
        ? shardPending : restoredObjectPending(info, ask.ownerNodeEpoch, named);
    if (stillPending) {
      if (std::find(earlierPriority.begin(), earlierPriority.end(), ask.ownerNodeEpoch) == earlierPriority.end())
        earlierPriority.push_back(ask.ownerNodeEpoch);
      tasks.add(timer.afterDelay(0 * kj::MILLISECONDS).then([this]() { return resealEarlier(); }));
      answer.refusal = "the restored owner shard still has unpublished records";
      log("restored-owner-tail-pending", "\"namespaceId\":" + quote(ask.namespaceId) +
          ",\"shard\":" + std::to_string(ask.shard) + ",\"sourceNodeEpoch\":\"" +
          sourceEpoch + "\",\"records\":" + std::to_string(shardPending ? pending->second : 0) +
          (named.empty() ? std::string() : ",\"objectId\":" + quote(ask.objectId.substr(0, 12))));
      return answer;
    }
    // The shard's owner proof (and its fence) stays what it was: only once the whole shard is published.
    if (remote && ask.shardEpoch > 1 && !shardPending)
      fenceOwner(ask.namespaceId, ask.shard, ask.shardEpoch - 1, "a remote complete owner proof", true);
    ++tailAnswers;++restoredEmptyAnswers;
    if (shardPending) ++restoredObjectAnswers;
    answer.ownerConfirmed = true;
    log(shardPending ? "restored-owner-object-empty" : "restored-owner-tail-empty", "\"namespaceId\":" +
        quote(ask.namespaceId) + ",\"shard\":" + std::to_string(ask.shard) + ",\"sourceNodeEpoch\":\"" + sourceEpoch +
        "\"" + (named.empty() ? std::string() : ",\"objectId\":" + quote(ask.objectId.substr(0, 12))));
    return answer;
  }
  if (remote && ask.ownerNodeId == settings.nodeId && ask.shardEpoch > 1)
    fenceOwner(ask.namespaceId, ask.shard, ask.shardEpoch - 1, "a remote complete owner proof", true);
  ++tailAnswers;
  answer.ownerConfirmed = ask.ownerNodeId == settings.nodeId;
  std::map<std::tuple<uint64_t, uint64_t, uint64_t>, const std::string*> ordered;
  std::deque<std::string> temporary;
  auto matches = [&](const LogRecord& record) {
    return record.kind != LogRecordKind::SEAL && record.shard == ask.shard && record.nodeSeq > ask.afterSeq &&
        (!ask.ownerNodeEpoch || record.nodeEpoch == ask.ownerNodeEpoch) &&
        (!ask.shardEpoch || record.shardEpoch <= ask.shardEpoch) &&
        (ask.namespaceId.empty() || record.objectName.starts_with(ask.namespaceId + "/"));
  };
  auto take = [&](const std::string& encoded, const LogRecord* metadata) {
    std::optional<LogRecord> decoded;
    if (metadata == nullptr) {
      decoded = decodeLogRecord(encoded);
      if (!decoded) { answer.refusal = "a buffered tail record failed validation"; return; }
      metadata = &*decoded;
    }
    if (!matches(*metadata)) return;
    const auto position = std::make_tuple(metadata->nodeEpoch, metadata->nodeSeq, metadata->shardEpoch);
    auto [entry, added] = ordered.emplace(position, &encoded);
    if (!added && *entry->second != encoded) answer.refusal = "conflicting bytes for one tail record";
  };
  const auto wanted = ask.objectId.empty() ? std::string() : ask.namespaceId + "/" + ask.objectId;
  auto sourceMatches = [&](const PeerBuffer& buffer) {
    return (ask.ownerNodeId.empty() || buffer.nodeId == ask.ownerNodeId) &&
        (!ask.ownerNodeEpoch || buffer.nodeEpoch == ask.ownerNodeEpoch);
  };
  auto earlierMatches = [&](uint64_t epoch) {
    return (!ask.ownerNodeEpoch || ask.ownerNodeEpoch == epoch) && !restoredEpochs.contains(epoch) &&
        membership.heldEpochs.contains(std::to_string(epoch));
  };
  const bool own = ask.ownerNodeId.empty() || ask.ownerNodeId == settings.nodeId;
  const bool current = own && (!ask.ownerNodeEpoch || ask.ownerNodeEpoch == nodeEpoch);
  auto takePeer = [&](const PeerBuffer& buffer, uint64_t sequence, const std::string& bytes) {
    if (buffer.isolated.contains(sequence)) return;
    auto metadata = buffer.positions.find(sequence);
    take(bytes, metadata == buffer.positions.end() ? nullptr : &metadata->second);
  };
  auto takeCurrent = [&](uint64_t sequence, const std::string& bytes) {
    if (!confirmed.contains(sequence) || isolated.contains(sequence)) return;
    auto metadata = parsed.find(sequence);
    take(bytes, metadata == parsed.end() ? nullptr : &metadata->second);
  };
  auto takeEarlier = [&](const LogRecord& record) {
    if (!matches(record)) return;
    temporary.push_back(encodeLogRecord(record));
    take(temporary.back(), &record);
  };
  for (const auto& [streamId, buffer] : incoming) {
    if (!sourceMatches(buffer)) continue;
    // The drop watermark belongs to the source, even if it has no record of this object.
    answer.droppedTo = std::max(answer.droppedTo, buffer.droppedTo);
    if (wanted.empty()) {
      for (auto it = buffer.held.upper_bound(ask.afterSeq); it != buffer.held.end(); ++it)
        takePeer(buffer, it->first, it->second);
    } else if (auto index = buffer.byObject.find(wanted); index != buffer.byObject.end()) {
      for (auto seq = index->second.upper_bound(ask.afterSeq); seq != index->second.end(); ++seq)
        takePeer(buffer, *seq, buffer.held.at(*seq));
    }
  }
  if (current) {
    if (wanted.empty()) {
      for (auto it = records.upper_bound(ask.afterSeq); it != records.end(); ++it)
        takeCurrent(it->first, it->second);
    } else if (auto index = unsealedByObject.find(wanted); index != unsealedByObject.end()) {
      for (auto seq = index->second.upper_bound(ask.afterSeq); seq != index->second.end(); ++seq)
        takeCurrent(*seq, records.at(*seq));
    }
  }
  if (own) {
    if (wanted.empty()) {
      for (const auto& [epoch, held] : earlier) {
        if (!earlierMatches(epoch)) continue;
        for (auto it = held.upper_bound(ask.afterSeq); it != held.end(); ++it) takeEarlier(it->second);
      }
    } else if (auto index = earlierByObject.find(wanted); index != earlierByObject.end()) {
      const auto& positions = index->second;
      auto position = ask.ownerNodeEpoch ? positions.upper_bound({ask.ownerNodeEpoch, ask.afterSeq}) : positions.begin();
      for (; position != positions.end(); ++position) {
        const auto [epoch, sequence] = *position;
        if (ask.ownerNodeEpoch && epoch != ask.ownerNodeEpoch) break;
        if (sequence > ask.afterSeq && earlierMatches(epoch)) takeEarlier(earlier.at(epoch).at(sequence));
      }
    }
  }
  // Keep the old duplicate-identity check at the positions we give out. A second
  // stream can claim the same position under a different object name; look up
  // that position without scanning or adding the other object's tail to the page.
  if (!wanted.empty() && answer.refusal.empty()) {
    for (const auto& [position, bytes] : ordered) {
      const auto [epoch, sequence, shardEpoch] = position;
      auto conflict = [&](const LogRecord& record) {
        return record.objectName != wanted && record.nodeEpoch == epoch && record.shardEpoch == shardEpoch && matches(record);
      };
      for (const auto& [streamId, buffer] : incoming) {
        if (!sourceMatches(buffer) || buffer.nodeEpoch != epoch || buffer.isolated.contains(sequence)) continue;
        auto at = buffer.positions.find(sequence);
        if (at != buffer.positions.end() && conflict(at->second))
          answer.refusal = "conflicting bytes for one tail record";
      }
      if (current && epoch == nodeEpoch && confirmed.contains(sequence) && !isolated.contains(sequence)) {
        auto at = parsed.find(sequence);
        if (at != parsed.end() && conflict(at->second)) answer.refusal = "conflicting bytes for one tail record";
      } else if (own && earlierMatches(epoch)) {
        if (auto held = earlier.find(epoch); held != earlier.end()) {
          auto at = held->second.find(sequence);
          if (at != held->second.end() && conflict(at->second)) answer.refusal = "conflicting bytes for one tail record";
        }
      }
      if (!answer.refusal.empty()) break;
    }
  }
  if (!answer.refusal.empty()) return answer;
  size_t bytes = 0;
  for (auto it = ordered.begin(); it != ordered.end(); ++it) {
    const auto& encoded = *it->second;
    if (encoded.size() > tailRecordBytes()) {
      answer.refusal = "a historical record exceeds the recoverable RPC limit";
      return answer;
    }
    if (bytes && bytes + encoded.size() > pageBytes) {
      answer.more = true;
      break;
    }
    auto record = decodeLogRecord(encoded);
    if (!record) { answer.refusal = "a buffered tail record failed validation"; return answer; }
    if (!answer.firstSeq) answer.firstSeq = record->nodeSeq;
    answer.lastSeq = std::max(answer.lastSeq, record->nodeSeq);
    bytes += encoded.size();
    answer.records.push_back(std::move(*record));
    ++answer.count;
    if (encoded.size() > pageBytes) {
      answer.more = std::next(it) != ordered.end();
      break;  // One over-budget record occupies the page alone.
    }
  }
  return answer;
}

// ---- The state endpoint ----

std::string DurableLog::recoverySchedulerJson(double now) const {
  std::ostringstream out;
  out << "{\"metadataBytes\":" << rebuildMetadataBytes << ",\"peakMetadataBytes\":" << peakRebuildMetadataBytes
      << ",\"metadataLimitBytes\":" << rebuildMetadataLimit << ",\"backgroundMetadataBytes\":" << backgroundRebuildMetadataBytes
      << ",\"foregroundReservedBytes\":" << rebuildMetadataLimit / 4 << ",\"metadataRefusals\":" << rebuildMetadataRefusals
      << ",\"metadataRequests\":" << recoveryMetadata.started() << ",\"activeMetadataRequests\":" << recoveryMetadata.active()
      << ",\"peakMetadataRequests\":" << recoveryMetadata.peakActive()
      << ",\"activeTailTransfers\":" << tailPages.active() << ",\"peakTailTransfers\":" << tailPages.peakActive()
      << ",\"entries\":[";
  bool first = true;
  for (const auto& [name, run] : rebuildRuns) {
    if (!first) out << ",";
    first = false;
    const bool queued = run->phase == RebuildPhase::QUEUED || run->phase == RebuildPhase::RESUMING;
    out << "{\"object\":" << quote(name) << ",\"phase\":" << quote(rebuildPhaseName(run->phase))
        << ",\"phaseMs\":" << now - run->phaseAtMs << ",\"waitingMs\":" << (run->parkedAtMs ? now - run->parkedAtMs : 0)
        << ",\"queueMs\":" << run->queuedMs + (queued ? now - run->phaseAtMs : 0)
        << ",\"class\":" << quote(run->background ? "background" : "foreground")
        << ",\"owner\":" << quote(run->owner) << ",\"ownerEpoch\":\"" << run->ownerEpoch
        << "\",\"probeAgeMs\":" << (run->probeAtMs ? now - run->probeAtMs : 0) << "}";
  }
  out << "]}";
  return out.str();
}

std::string DurableLog::json() const {
  std::ostringstream out;
  uint64_t earlierRecords = 0;
  for (const auto& [epoch, pending] : earlier) earlierRecords += pending.size();
  for (const auto& [epoch, pending] : earlierExcluded) earlierRecords += pending.size();
  const auto now = monotonicTimeMs();
  uint64_t unconfirmed = 0, unconfirmedBytes = 0;
  for (const auto& [sequence, flight] : inFlight) {
    ++unconfirmed;
    unconfirmedBytes += flight.bytes;
  }
  uint32_t queuedRebuilds = 0;
  for (auto& waiter : rebuildWaiters) if (waiter.ready->isWaiting()) ++queuedRebuilds;
  uint32_t queuedBackgroundRebuilds = 0;
  for (auto& waiter : backgroundRebuildWaiters) if (waiter.ready->isWaiting()) ++queuedBackgroundRebuilds;
  const auto waitingRebuilds = std::count_if(rebuildRuns.begin(), rebuildRuns.end(), [](const auto& entry) {
    return entry.second->parkedAtMs != 0 && entry.second->phase != RebuildPhase::RESUMING;
  });
  double alarmTickP99Us = 0;uint64_t tickSeen = 0;
  for (size_t i = 0;i < alarmTickUs.size();++i) {
    tickSeen += alarmTickUs[i];
    if (tickSeen && tickSeen >= (alarmTickSamples * 99 + 99) / 100) {
      alarmTickP99Us = i == 1023 ? alarmTickMaxNs / 1000.0 : i + 1;break;
    }
  }
  uint32_t local = 0, rebuildingNow = 0, repairing = 0, openCaches = 0;
  uint64_t activeRequests = 0, activeConnections = 0;
  for (const auto& [name, object] : objects) {
    for (const auto& [id, holder] : object.holders) {
      const auto channel = channels.find(id);
      if (channel != channels.end() && channel->second.open) {
        activeRequests += holder.requests;
        activeConnections += holder.connections;
      }
    }
    if (object.cacheOpen) ++openCaches;
    if (object.state == ObjectState::LOCAL) ++local;
    else if (object.state == ObjectState::REBUILDING) ++rebuildingNow;
    else if (object.state == ObjectState::REPAIR) ++repairing;
  }
  out << "{\"enabled\":" << (enabled() ? "true" : "false") << ",\"nodeEpoch\":\"" << nodeEpoch
      << "\",\"earlierRecords\":" << earlierRecords << ",\"ownerFences\":" << ownerFenceEvents
      << ",\"storeClient\":" << (store ? store->stats().json() : std::string("null"))
      << ",\"storeProcess\":" << objectStoreProcessConnections().json()
      << ",\"localRecovery\":{\"writeFailed\":" << (recoveryWriteFailed ? "true" : "false")
      << ",\"writeFailures\":" << recoveryWriteFailures << ",\"startupReady\":" << ((!startupAttempted || startupReady) ? "true" : "false")
      << ",\"startupMarkerFailures\":" << startupMarkerFailures << ",\"startupCleanupFailures\":" << startupCleanupFailures << "}"
      << ",\"startupClassification\":{\"history\":" << startupHistoryRecords << ",\"fenced\":" << startupFencedRecords
      << ",\"adopt\":" << startupAdoptRecords << ",\"readFallback\":" << startupReadFallbackRecords
      << ",\"segmentReads\":" << startupSegmentReads << ",\"oldManifestReads\":" << startupOldManifestReads
      << ",\"ms\":" << startupClassificationMs << ",\"logReadMs\":" << startupLogReadMs
      << ",\"logReadPeakRssBytes\":\"" << startupLogPeakRssBytes << "\",\"peakRssBytes\":\"" << startupPeakRssBytes << "\"}"
      << ",\"peers\":{\"want\":" << peerCount() << ",\"configured\":" << settings.config.peers << ",\"streams\":[";
  bool first = true;
  for (const auto& [id, peer] : peers) {
    if (!first) out << ",";
    first = false;
    const bool current = !stopped && peer.ownerEpoch == nodeEpoch && peer.identity == peerIdentity(id);
    out << "{\"nodeId\":" << quote(id) << ",\"open\":"
        << ((current && peer.sink != kj::none && !peer.connecting && !peer.broken) ? "true" : "false")
        << ",\"session\":\"" << peer.session << "\",\"ownerEpoch\":\"" << peer.ownerEpoch
        << "\",\"receiver\":" << quote(peer.identity) << ",\"connecting\":" << (peer.connecting ? "true" : "false")
        << ",\"firstSeq\":\"" << peer.firstSeq << "\""
        << ",\"ackedSeq\":\"" << peer.ackedSeq << "\",\"sentSeq\":\"" << peer.sentSeq << "\",\"appends\":"
        << peer.appends << ",\"inFlightBytes\":\"" << peer.inFlightBytes << "\",\"queuedBytes\":\""
        << peer.queuedBytes << "\",\"lastAckAgoMs\":" << (peer.lastAckMs ? uint64_t(now - peer.lastAckMs) : 0)
        << ",\"refusal\":" << quote(peer.lastRefusal) << "}";
  }
  out << "],\"switches\":" << peerSwitches;
  if (!peerAckGateForTest.empty()) out << ",\"heldAcksForTest\":" << heldPeerAcksForTest;
  out << "},\"confirm\":{\"confirmations\":" << confirmations
      << ",\"failures\":" << confirmFailures << ",\"refusals\":" << refusals << ",\"isolations\":" << isolations
      << ",\"unconfirmed\":" << unconfirmed << ",\"unconfirmedBytes\":\"" << unconfirmedBytes
      << "\",\"meanMs\":" << (confirmations ? confirmMsTotal / double(confirmations) : 0) << ",\"p50Ms\":"
      << percentile(confirmSamples, 0.5) << ",\"p99Ms\":" << percentile(confirmSamples, 0.99)
      << ",\"leaseRefusals\":" << leaseRefusals << ",\"leaseWaits\":" << leaseWaits << ",\"leaseLosses\":" << leaseLosses
      << "},\"log\":{\"records\":" << recordsTaken << ",\"frames\":" << framesTaken << ",\"bytes\":\"" << bytesTaken
      << "\",\"unsealed\":" << records.size() << ",\"unsealedBytes\":\"" << recordBytes << "\",\"nextSeq\":\""
      << nextSeq << "\",\"sealedSeq\":\"" << sealedSeq << "\",\"walCutBacks\":" << walCutBacks
      << ",\"walCutBackFailures\":" << walCutBackFailures
      << ",\"positionMismatches\":" << positionMismatches << "},\"segments\":{\"sealed\":" << segments
      << ",\"bytes\":\"" << segmentBytes << "\",\"number\":\"" << segmentNumber << "\",\"uploads\":" << uploads
      << ",\"uploadFailures\":" << uploadFailures << ",\"lastSegmentAgoMs\":"
      << (lastSegmentAtMs ? uint64_t(now - lastSegmentAtMs) : 0) << ",\"lastUploadAgoMs\":"
      << (lastUploadAtMs ? uint64_t(now - lastUploadAtMs) : 0) << ",\"lastError\":" << quote(lastUploadError)
      << "},\"manifests\":{\"written\":" << manifestWrites << ",\"conflicts\":" << manifestConflicts
      << ",\"fenced\":" << manifestFences << ",\"failures\":" << manifestFailures << ",\"lastError\":"
      << quote(lastManifestError) << "},\"snapshots\":{\"taken\":" << snapshots << ",\"bytes\":\""
      << snapshotBytesWritten << "\",\"failures\":" << snapshotFailures << "},\"checkpoints\":{\"done\":"
      << checkpoints << ",\"asked\":" << checkpointAsks << "},\"threadedHomeIo\":{\"openSamples\":"
      << threadedHomeOpenSamples << ",\"maxOpenNs\":" << threadedHomeOpenMaxNs
      << ",\"checkpointSamples\":" << threadedHomeCheckpointSamples
      << ",\"maxCheckpointNs\":" << threadedHomeCheckpointMaxNs
      << ",\"maxCheckpointWalBytes\":" << threadedHomeCheckpointMaxWalBytes
      << "},\"objects\":{\"known\":" << objects.size()
      << ",\"local\":" << local << ",\"rebuilding\":" << rebuildingNow << ",\"repairing\":" << repairing
      << ",\"activeRebuilds\":" << activeRebuilds << ",\"queuedRebuilds\":" << queuedRebuilds + queuedBackgroundRebuilds
      << ",\"waitingRebuilds\":" << waitingRebuilds
      << ",\"activeBackgroundRebuilds\":" << activeBackgroundRebuilds << ",\"queuedBackgroundRebuilds\":" << queuedBackgroundRebuilds
      << ",\"foregroundRebuilds\":" << foregroundRebuilds << ",\"backgroundRebuilds\":" << backgroundRebuilds
      << ",\"rebuildPromotions\":" << rebuildPromotions
      << ",\"openCache\":" << openCaches
      << ",\"activeRequests\":" << activeRequests << ",\"activeConnections\":" << activeConnections
      << ",\"rebuilds\":" << rebuilds << ",\"rebuildFailures\":" << rebuildFailures << ",\"repairs\":" << repairs
      << ",\"recoverTimeouts\":" << recoverTimeouts << ",\"lastRebuildMs\":" << lastRebuildMs
      << ",\"lastRebuildLoopDelayMs\":" << lastRebuildLoopDelayMs;
  if (segmentCache) {
    out << ",\"segmentCache\":{\"entries\":" << segmentCache->entries() << ",\"heldBytes\":" << segmentCache->heldBytes()
        << ",\"limitBytes\":" << segmentCache->limitBytes() << ",\"decodes\":" << segmentCache->decodes
        << ",\"hits\":" << segmentCache->hits << ",\"misses\":" << segmentCache->misses << ",\"joins\":"
        << segmentCache->joins << ",\"evictions\":" << segmentCache->evictions << ",\"failures\":"
        << segmentCache->failures << ",\"loadLimit\":" << segmentCache->loadLimit() << ",\"loadsRunning\":"
        << segmentCache->loadsRunning() << ",\"loadsWaiting\":" << segmentCache->loadsWaiting() << ",\"loadWaits\":"
        << segmentCache->loadWaits << ",\"releasedOffLoop\":" << segmentsReleasedOffLoop() << "}";
  }
  out << ",\"lastError\":"
      << quote(lastRebuildError) << ",\"tooLargeAnswers\":" << overLimitAnswers << "},\"disk\":{\"trusted\":"
      << (diskTrusted ? "true" : "false") << ",\"note\":" << quote(startupNote) << ",\"limitBytes\":\""
      << diskLimitBytes << "\",\"usedBytes\":\"" << diskUsedBytes << "\",\"filesDropped\":" << filesDropped
      << ",\"coldFilesSkippedLive\":" << coldFilesSkippedLive
      << "},\"buffer\":{\"bytes\":\"" << incomingBytes << "\",\"limitBytes\":\"" << settings.config.peerBufferBytes
      << "\",\"streams\":" << incoming.size() << ",\"refusals\":" << bufferRefusals << ",\"fencedRecords\":"
      << fencedRecords << ",\"peerFenceChecks\":" << peerFenceChecks << ",\"peerFenceRaises\":" << peerFenceRaises
      << ",\"peerFenceUnverified\":" << peerFenceUnverified << ",\"ownerFences\":" << ownerFenceThrough.size()
      << ",\"fullRetriesSent\":" << fullRetriesSent << ",\"fencedShardChecks\":" << fencedShardChecks << ",\"fencedShardsLost\":" << fencedShardsLost
      << ",\"tailAnswers\":" << tailAnswers << ",\"tailFetches\":" << tailFetches << ",\"owners\":[";
  first = true;
  for (const auto& [streamId, buffer] : incoming) {
    if (!first) out << ",";
    first = false;
    out << "{\"nodeId\":" << quote(buffer.nodeId) << ",\"nodeEpoch\":\"" << buffer.nodeEpoch << "\",\"records\":"
        << buffer.held.size() << ",\"bytes\":\"" << buffer.bytes << "\",\"acceptedSeq\":\"" << buffer.acceptedSeq
        << "\",\"answeredSeq\":\"" << buffer.answeredSeq() << "\",\"fencedHeldBack\":" << buffer.fencedSeqs.size()
        << ",\"droppedTo\":\"" << buffer.droppedTo << "\"}";
  }
  out << "]},\"channels\":[";
  first = true;
  for (const auto& [id, channel] : channels) {
    if (!first) out << ",";
    first = false;
    out << "{\"processId\":" << quote(id) << ",\"pid\":" << channel.pid << ",\"open\":"
        << (channel.open ? "true" : "false") << ",\"messages\":" << channel.messages << ",\"frames\":"
        << channel.frames << ",\"bytes\":\"" << channel.bytes << "\"}";
  }
  // The resident objects of each durable process, as it reports them, and the alarms this node has to fire.
  out << "],\"resident\":[";
  first = true;
  for (const auto& [processId, entry] : residency) {
    if (!first) out << ",";
    first = false;
    out << "{\"processId\":" << quote(processId) << ",\"objects\":" << entry.resident << ",\"limit\":"
        << entry.limit << ",\"evicted\":" << entry.evicted << ",\"woken\":" << entry.woken
        << ",\"wakeMsTotal\":" << double(entry.wakeUsTotal) / 1000.0 << ",\"wakeMsMax\":"
        << double(entry.wakeUsMax) / 1000.0 << ",\"wakeMsMean\":"
        << (entry.wakes ? double(entry.wakeUsTotal) / double(entry.wakes) / 1000.0 : 0.0) << "}";
  }
  out << "],\"recovery\":{\"bounded\":" << tailRecoveryBounded << ",\"ownerSilent\":" << ownerSilentRecoveries
      << ",\"scheduler\":" << recoverySchedulerJson(now)
      << ",\"cutsHeldOwnerLive\":" << cutsHeldOwnerLive << ",\"cutsHeldOwnerUnknown\":" << cutsHeldOwnerUnknown
      << ",\"ownerHeartbeatReads\":" << ownerHeartbeatReads
      << ",\"lateRecords\":" << lateTailRecords << ",\"transferredRecords\":" << transferredTailRecords
      << ",\"unaccountedRecords\":" << unaccountedTailRecords << ",\"unaccountedBytes\":\""
      << unaccountedTailBytes << "\",\"restoredUnaccountedRecords\":" << restoredUnaccountedRecords
      << ",\"restoredUnaccountedBytes\":\"" << restoredUnaccountedBytes
      << "\",\"restoredEmptyAnswers\":" << restoredEmptyAnswers
      << ",\"restoredObjectAnswers\":" << restoredObjectAnswers << ",\"earlierSegmentBuilds\":" << earlierSegmentBuilds
      << ",\"earlierManifests\":" << earlierManifests << ",\"earlierPublishing\":" << earlierPublishing.size()
      << ",\"restoredLogUnavailableAnswers\":" << restoredLogUnavailableAnswers
      << ",\"localLogEmptyClosures\":" << localLogEmptyClosures
      << ",\"ownerLogUnavailable\":" << ownerLogUnavailableRecoveries
      << ",\"recordLimitRefusals\":" << recordLimitRefusals
      << ",\"objectFailures\":" << objectFailures << "},\"alarms\":{\"scheduled\":" << alarms.size() << ",\"set\":" << alarmsSet << ",\"cleared\":"
      << alarmsCleared << ",\"fired\":" << alarmsFired << ",\"failed\":" << alarmFailures << ",\"retries\":"
      << alarmRetries << ",\"fireConcurrency\":" << (settings.threadedAlarmEvents ? 0 : settings.config.alarmFireConcurrency)
      << ",\"inFlight\":" << activeAlarmFires << ",\"peakInFlight\":" << peakAlarmFires
      << ",\"capacityDeferred\":" << alarmDeferred << ",\"inactiveDropped\":" << alarmInactive
      << ",\"dispatchAttempts\":" << alarmAttempts << ",\"schedulingThreadCpuNs\":" << alarmPumpCpuNs
      << ",\"waitingRebuilds\":" << alarmRebuildWaiting << ",\"peakWaitingRebuilds\":" << peakAlarmRebuildWaiting
      << ",\"rebuildWaitLimit\":" << ALARM_REBUILD_WAIT_LIMIT << ",\"preparationPending\":" << alarmPreparationCount
      << ",\"activePreparations\":" << activeAlarmPreparations
      << ",\"pausedNamespaces\":" << alarmPausedNamespaces.size() << ",\"readyBuckets\":" << alarmReadyBucketCount
      << ",\"tickSamples\":" << alarmTickSamples << ",\"tickP99Us\":" << alarmTickP99Us
      << ",\"tickMaxUs\":" << alarmTickMaxNs / 1000.0 << ",\"tickThreadCpuNs\":" << alarmTickCpuNs
      << ",\"loopMaxLagMs\":" << alarmLoopMaxLagMs
      << ",\"notDue\":" << alarmsNotDue << ",\"adopted\":" << alarmsAdopted << ",\"dropped\":"
      << alarmsDropped << ",\"hintWrites\":" << (alarmHintWrites + (alarmPublication ? alarmPublication->hintWrites : 0)) << ",\"hintFailures\":" << (alarmHintFailures + (alarmPublication ? alarmPublication->hintFailures : 0))
      << (alarmPublication ? "," + alarmPublication->json() : "")
      << (alarmDiscovery ? ",\"discovery\":" + alarmDiscovery->json() : "")
      << ",\"processInFlight\":[";
  first = true;
  for (const auto& [process, counts]: alarmProcessFires) {
    if (!first) out << ",";first = false;
    out << "{\"process\":" << quote(process) << ",\"inFlight\":" << counts.first << ",\"peakInFlight\":" << counts.second << "}";
  }
  out << "],\"next\":[";
  first = true;
  {
    std::vector<const AlarmState*> soonest;
    for (const auto& [key, state] : alarms) soonest.push_back(&state);
    std::sort(soonest.begin(), soonest.end(),
        [](const AlarmState* a, const AlarmState* b) { return a->atUnixMs < b->atUnixMs; });
    if (soonest.size() > 16) soonest.resize(16);
    for (const auto* state : soonest) {
      if (!first) out << ",";
      first = false;
      out << "{\"namespaceId\":" << quote(state->namespaceId.substr(0, 12)) << ",\"objectId\":"
          << quote(state->objectId.substr(0, 12)) << ",\"facet\":" << state->facetId << ",\"shard\":"
          << state->shard << ",\"atUnixMs\":" << state->atUnixMs << ",\"retries\":" << state->retries
          << ",\"fromHint\":" << (state->fromHint ? "true" : "false") << "}";
    }
  }
  // The namespaces this node owns shards of: how many objects it holds of each, their bytes and the shards.
  out << "]},\"namespaces\":[";
  first = true;
  {
    std::map<std::string, std::tuple<std::string, uint64_t, uint64_t>> byNamespace;  // tenant, objects, bytes
    std::map<std::string, std::set<uint32_t>> shardsOf;
    for (const auto& [name, object] : objects) {
      if (object.state == ObjectState::GONE || !namespaceCounts(object.namespaceId)) continue;
      auto& entry = byNamespace[object.namespaceId];
      if (std::get<0>(entry).empty()) std::get<0>(entry) = object.tenant;
      ++std::get<1>(entry);
      std::get<2>(entry) += object.bytes;
      shardsOf[object.namespaceId].insert(object.shard);
    }
    for (const auto& [namespaceId, entry] : byNamespace) {
      if (!first) out << ",";
      first = false;
      out << "{\"namespaceId\":" << quote(namespaceId) << ",\"tenant\":" << quote(std::get<0>(entry))
          << ",\"objects\":" << std::get<1>(entry) << ",\"bytes\":\"" << std::get<2>(entry) << "\",\"shards\":[";
      bool firstShard = true;
      for (const auto shard : shardsOf[namespaceId]) {
        out << (firstShard ? "" : ",") << shard;
        firstShard = false;
      }
      out << "]}";
    }
  }
  // What the tenants' objects hold: this node's own, and every node's as they last published it.
  out << "],\"storage\":{\"quotaRefusals\":" << quotaRefusals << ",\"publishes\":" << usagePublishes
      << ",\"publishFailures\":" << usagePublishFailures << ",\"reads\":" << usageReads
      << ",\"positionWrites\":" << positionWrites
      << ",\"tenants\":[";
  first = true;
  {
    std::set<std::string> names;
    for (const auto& [name, object] : objects) {
      if (!object.tenant.empty()) names.insert(object.tenant);
    }
    for (const auto& [nodeId, tenants] : publishedBytes) {
      for (const auto& [tenant, bytes] : tenants) names.insert(tenant);
    }
    for (const auto& tenant : names) {
      if (!first) out << ",";
      first = false;
      out << "{\"tenant\":" << quote(tenant) << ",\"bytesHere\":\"" << tenantBytesHere(tenant)
          << "\",\"bytes\":\"" << tenantBytes(tenant) << "\",\"quotaBytes\":\""
          << (hooks.tenantQuotaBytes ? hooks.tenantQuotaBytes(tenant) : 0) << "\"}";
    }
  }
  out << "]},\"settings\":{\"confirmTimeoutMs\":" << settings.config.confirmTimeoutMs << ",\"recoverTimeoutMs\":"
      << settings.config.recoverTimeoutMs << ",\"recordLimitBytes\":\"" << recordLimitBytes()
      << "\",\"segmentIntervalMs\":" << settings.config.segmentIntervalMs
      << ",\"segmentBytes\":\"" << settings.config.segmentBytes << "\",\"snapshotBytes\":\""
      << settings.config.snapshotBytes << "\",\"objectBytes\":\"" << settings.config.objectBytes
      << "\",\"checkpointBytes\":\"" << settings.config.checkpointBytes << "\",\"recoverParallelism\":"
      << settings.config.recoverParallelism << ",\"rebuildConcurrency\":" << settings.config.rebuildConcurrency
      << ",\"alarmFireConcurrency\":" << settings.config.alarmFireConcurrency
      << ",\"residentObjects\":" << settings.config.residentObjects
      << ",\"usagePublishMs\":" << settings.config.usagePublishMs << "}}";
  return out.str();
}


std::string DurableLog::memoryJson() const {
  MemoryContainers out;
  out.count("records", records.size(), recordBytes);  // Erased after successful seal and manifests; unsealed backlog.
  uint64_t parsedBytes = 0, earlierEntries = 0, earlierBytes = 0;
  const auto payload = [](const LogRecord& r) { return r.header.size() + r.frameBytes.size() +
      r.objectName.size() + r.storageId.size() + r.facetName.size(); };
  for (const auto& [seq, r] : parsed) parsedBytes += payload(r);
  out.count("parsed", parsed.size(), parsedBytes);  // Same seal floor as records; decoded payload only.
  out.count("confirmed", confirmed.size());  // Erased at the successfully published seal floor.
  out.count("isolated", isolated.size());  // Retained until nodeEpoch changes, for repeated peer DROP.
  uint64_t exclusions = 0, exclusionBytes = 0;
  for (const auto& [name, object] : objects) {
    exclusions += object.isolatedRecords.size();
    exclusionBytes += object.isolatedRecords.capacity() * sizeof(std::pair<uint64_t, uint64_t>);
  }
  out.count("isolatedReported", exclusions, exclusionBytes);  // Object-scoped manifest exclusions.
  uint64_t persistentExclusions = 0;
  for (const auto& [name, state] : recoveryStates) persistentExclusions += state.exclusions.size();
  out.count("recoveryStates", recoveryStates.size());
  out.count("recoveryExclusions", persistentExclusions);
  out.count("recoveryEpochs", recoveryEpochs.size());
  out.count("predecessorLogs", predecessorLogs.size());
  out.count("ownerFences", ownerFenceThrough.size());
  out.count("carriedRecoveryObjects", carriedRecoveryObjects.size());
  uint64_t pendingBytes = 0;
  for (const auto& [seq, r] : inFlight) pendingBytes += r.bytes;
  out.count("inFlight", inFlight.size(), pendingBytes);  // Removed on confirmation or isolation/timeout.
  out.count("incoming", incoming.size(), incomingBytes);  // Streams retire with owner identity; peerBufferBytes caps payload.
  uint64_t held = 0, heldBytes = 0, dropped = 0, fragmentBytes = 0;
  std::ostringstream streams; streams << '['; bool first = true;
  for (const auto& [id, b] : incoming) {
    uint64_t bytes = 0;
    for (const auto& [seq, encoded] : b.held) bytes += encoded.size();
    held += b.held.size(); heldBytes += bytes; dropped += b.isolated.size(); fragmentBytes += b.fragment.size();
    if (!first) streams << ','; first = false;
    streams << "{\"streamId\":" << id << ",\"held\":{\"entries\":" << b.held.size()
        << ",\"bytes\":" << bytes << "},\"isolated\":{\"entries\":" << b.isolated.size()
        << "},\"fragmentBytes\":" << b.fragment.size() << '}';
  }
  streams << ']';
  out.count("incomingHeld", held, heldBytes);  // Erased on owner's TRIM/DROP; included in incomingBytes.
  out.count("incomingIsolated", dropped);  // DROP exclusions live with the stream; do not count toward held bytes.
  out.count("incomingFragments", incoming.size(), fragmentBytes);  // Cleared when the last fragment arrives or stream retires.
  out.count("objects", objects.size());  // Known object metadata retained for this daemon lifetime; disk cache eviction does not erase it.
  uint64_t facets = 0, objectSegments = 0, snapshotFacets = 0, objectAlarms = 0;
  for (const auto& [name, o] : objects) {
    facets += o.facets.size(); objectSegments += o.segments.size();
    snapshotFacets += o.snapshotFacets.size(); objectAlarms += o.alarms.size();
  }
  out.count("objectFacets", facets);  // Per known object's databases; removed with the object/facet.
  out.count("objectSegments", objectSegments);  // Pending segment references, cleared when manifest is written.
  out.count("snapshotFacets", snapshotFacets);  // Latest snapshot's facets, replaced by the next snapshot.
  out.count("objectAlarms", objectAlarms);  // Per facet's latest reported alarm; removed with facet/object.
  out.count("alarms", alarms.size());  // Removed when cleared/fired or ownership is lost; one per scheduled facet.
  out.count("residency", residency.size());  // One latest report per durable process channel.
  uint64_t inboxBytes = 0, queued = 0, queuedBytes = 0;
  for (const auto& [id, c] : channels) inboxBytes += c.inbox.size();
  for (const auto& [id, peer] : peers) { queued += peer.queue.size(); queuedBytes += peer.queuedBytes; }
  out.count("channels", channels.size(), inboxBytes);  // One per durable process; closed with the generation.
  uint64_t channelFacets = 0;
  for (const auto& [id, channel] : channels) channelFacets += channel.openFacets.size();
  out.count("channelOpenFacets", channelFacets);
  out.count("peers", peers.size());  // Streams are reconciled to current peer identities; node-id metadata is retained.
  out.count("peerQueue", queued, queuedBytes);  // Drained by append window; discarded on peer retirement.
  out.count("rebuilding", rebuilding.size());  // Settled promise released by next openObject(), never its continuation.
  out.count("manifestGates", manifestGates.size());  // Only objects with local manifest writers or healers.
  out.count("settledRebuilds", settledRebuilds.size());  // Same next-open cleanup as rebuilding.
  uint64_t tenants = 0;
  for (const auto& [node, values] : publishedBytes) tenants += values.size();
  out.count("publishedBytes", publishedBytes.size());  // One latest usage table per node seen; refreshed at usageReadMs, without a fixed node-id cap.
  out.count("publishedTenants", tenants);  // Latest tenant entries of publishedBytes (values are stored disk usage).
  for (const auto& [epoch, values] : earlier) for (const auto& [seq, r] : values) {
    ++earlierEntries; earlierBytes += payload(r);
  }
  uint64_t excludedEarlier = 0;
  for (const auto& [epoch, values] : earlierExcluded) for (const auto& [seq, r] : values) {
    ++excludedEarlier;++earlierEntries;earlierBytes += payload(r);
  }
  out.count("earlierExcludedRecords", excludedEarlier);
  out.count("earlier", earlier.size());  // Startup-only epoch backlog, removed after resealEarlier().
  out.count("earlierRecords", earlierEntries, earlierBytes);  // Same reseal lifecycle, decoded payload bytes.
  out.count("confirmSamples", confirmSamples.size(), confirmSamples.capacity() * sizeof(double));  // Capped at 4096.
  auto result = out.json(); result.pop_back();
  return result + ",\"incomingStreams\":" + streams.str() + '}';
}

}  // namespace coworkerd
