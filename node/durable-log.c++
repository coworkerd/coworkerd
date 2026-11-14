#include "durable-log.h"
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

#include <algorithm>
#include <cmath>
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
std::string shardAlarmKey(const std::string& namespaceId, uint32_t shard, const std::string& objectId) {
  return shardAlarmPrefix(namespaceId, shard) + objectId;
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

void DatabaseReplay::finish() {
  if (fd >= 0) {
    // The rebuilt file is the object's only copy until its next segment: this one waits for the disk.
    ::fsync(fd);
    ::close(fd);
    fd = -1;
  }
}

// ---- The replicator ----

namespace {
// Messages from a durable process (patches/workerd 0013, coworkerd-wal-shipper.h).
constexpr uint8_t FROM_HELLO = 1, FROM_FRAMES = 2, FROM_OPEN = 3, FROM_CHECKPOINT = 4, FROM_RESET = 5,
                  FROM_DELETED = 6, FROM_CLOSED = 7, FROM_GAVE_UP = 8, FROM_ALARM = 9, FROM_RESIDENCY = 10;
// Messages to one.
constexpr uint8_t TO_ACK = 1, TO_SEALED = 2, TO_BREAK = 3, TO_CHECKPOINT = 4;
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
  if (settings.objectStore && !storeConfig.endpoint.empty()) store.emplace(storeConfig, network, timer);
}

DurableLog::~DurableLog() noexcept(false) {
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

std::filesystem::path DurableLog::durableDirectory() const {
  return settings.stackDirectory / "do" / settings.hostUniqueKey;
}
std::filesystem::path DurableLog::logDirectory() const {
  return settings.stackDirectory / "do" / "log";
}
std::filesystem::path DurableLog::databasePath(const std::string& storageId, uint32_t facetId,
    const char* suffix) const {
  const auto name = facetId == 0 ? storageId + ".sqlite" + suffix
                                 : storageId + "." + std::to_string(facetId) + ".sqlite" + suffix;
  return durableDirectory() / name;
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
    else earlier[nodeEpoch][sequence] = std::move(*record);
  }
  if (nodeEpoch) {
    auto& info = predecessorLogs[nodeEpoch];
    info.complete = !logAppendFailed && logFd >= 0;
    info.sealedTo = sealedSeq;
    info.maxSeq = nextSeq;
    info.unpublishedShards.clear();
    for (const auto* held : {&earlier[nodeEpoch], &earlierExcluded[nodeEpoch]}) {
      for (const auto& [sequence, record] : *held) {
        const auto shard = std::make_pair(record.objectName.substr(0, record.objectName.find('/')), record.shard);
        ++info.unpublishedShards[shard];info.shards.insert(shard);info.objects.insert(hostNameOf(record));
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

void DurableLog::readRecovery() {
  recoveryStates.clear();recoveryEpochs.clear();recoveryReadIncomplete = false;
  std::error_code code;
  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(logDirectory(), code))
    if (entry.path().extension() == ".recovery") files.push_back(entry.path());
  if (code) { recoveryReadIncomplete = true;failRecovery("the recovery directory could not be listed");return; }
  std::sort(files.begin(), files.end());
  for (const auto& file : files) {
    const auto epoch = decimal(file.stem().string());
    auto& info = recoveryEpochs[epoch];
    info.present = true;
    auto fileBytes = readLocalHistory(file);
    if (!fileBytes) {
      recoveryReadIncomplete = true;
      if (epoch == nodeEpoch) failRecovery("the active recovery sidecar could not be read");
      continue;
    }
    const auto& bytes = *fileBytes;
    std::string error;const auto pieces = splitRecords(bytes, &error);
    info.complete = error.empty();
    bool header = false;
    for (const auto& piece : pieces) {
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
      if (object.storageId.empty()) object.storageId = durableHostId(settings.hostUniqueKey, event.objectName);
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
  std::filesystem::create_directories(durableDirectory(), code);
  KJ_REQUIRE(!code, "the Durable Object cache directory could not be created");
  const auto readingStarted = monotonicTimeMs();
  co_await readOwnLog();
  startupLogReadMs = monotonicTimeMs() - readingStarted;startupLogPeakRssBytes = peakResidentBytes();
  if (!diskTrusted) {
    // The machine restarted and nothing says it was clean: with synchronous=OFF the files may hold pages the kernel
    // never wrote, so every object is rebuilt from its replicated history instead.
    for (auto& [epoch, info] : predecessorLogs) {
      if (epoch == nodeEpoch) continue;
      if (!markCrossedBoot(epoch)) co_return;
      info.complete = false;
    }
    for (auto& [name, object] : objects) {
      markRepair(object, "the object cache crossed a machine boot");
      if (recoveryWriteFailed) co_return;
    }
    uint64_t removed = 0;
    for (const auto& entry : std::filesystem::directory_iterator(durableDirectory(), code)) {
      std::filesystem::remove_all(entry.path(), code);
      if (code) {
        ++startupCleanupFailures;startupRetryAfterMs = monotonicTimeMs() + 1000;
        log("cache-cleanup-failed", "\"reason\":\"untrusted files remain; the previous boot marker is retained\"");
        co_return;
      }
      ++removed;
    }
    if (code) { ++startupCleanupFailures;startupRetryAfterMs = monotonicTimeMs() + 1000;co_return; }
    // Raw log and exclusion prefixes survive power loss. They may contain the
    // only copy of an ACK and can never prove absence after crossing a boot.
    filesDropped += removed;
    log("cache-dropped", "\"reason\":" + quote(reason) + ",\"entries\":" + std::to_string(removed));
  } else {
    readPositions();
  }
  if (recoveryWriteFailed) co_return;
  co_await classifyEarlier();
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
kj::Promise<void> DurableLog::readOwnLog() {
  predecessorLogs.clear();
  readRecovery();
  std::error_code code;
  if (!std::filesystem::exists(logDirectory(), code)) {
    if (code) failRecovery("the append-log directory could not be inspected");
    co_return;
  }
  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(logDirectory(), code)) {
    if (entry.path().extension() == ".log") files.push_back(entry.path());
    if (entry.path().extension() != ".closed") continue;
    const auto summaryEpoch = decimal(entry.path().stem().string());
    try {
      auto fileBytes = readLocalHistory(entry.path());KJ_REQUIRE(fileBytes.has_value(), "unreadable closed summary");
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
        if (object.storageId.empty()) object.storageId = durableHostId(settings.hostUniqueKey, name);
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
    std::string storageId, objectName;
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
    auto fileBytes = readLocalHistory(file);
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
    const auto pieces = splitRecords(bytes, &splitError);
    bool complete = splitError.empty();
    if (!complete) ++broken;
    for (const auto& encoded : pieces) {
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
      unsealed[record->nodeSeq] = *record;
      metadata.objects.insert(hostNameOf(*record));
      metadata.objectEpochs[hostNameOf(*record)] = std::max(metadata.objectEpochs[hostNameOf(*record)], record->shardEpoch);
      metadata.shards.emplace(record->objectName.substr(0, record->objectName.find('/')), record->shard);
      auto& position = positions[record->shipperKey()];
      position.nodeEpoch = record->nodeEpoch;
      position.nodeSeq = record->nodeSeq;
      position.objectName = record->objectName;
      position.dataEpoch = record->dataEpoch;
      position.storageId = record->storageId;
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
    }
    std::erase_if(unsealed, [&](const auto& entry) { return entry.first <= sealedTo; });
    auto& info = predecessorLogs[epoch];
    auto& recovery = recoveryEpochs[epoch];
    recovery.crossedBoot = recovery.crossedBoot || std::filesystem::exists(logDirectory() / (wide(epoch) + ".crossed"), code);
    info.complete = complete && !code && (!recovery.present || recovery.complete) && !recovery.crossedBoot;
    info.closed = false;
    if (!complete) recoveryReadIncomplete = true;
    if (epoch == nodeEpoch && !complete) failRecovery("the active append log has an incomplete prefix");
    info.sealedTo = sealedTo;
    for (const auto& [sequence, record] : unsealed) {
      info.startupUnsealed.emplace(record.objectName, record.shardEpoch);
      ++info.unpublishedShards[{record.objectName.substr(0, record.objectName.find('/')), record.shard}];
    }
    std::erase_if(unsealed, [&](const auto& entry) {
      const auto& record = entry.second;
      auto state = recoveryStates.find(hostNameOf(record));
      if (state == recoveryStates.end() || !state->second.exclusions.contains({record.shardEpoch, record.nodeSeq}))
        return false;
      earlierExcluded[epoch][entry.first] = record;
      return true;
    });
    kept += unsealed.size();
    if (!unsealed.empty()) {
      restoredEpochs.insert(epoch);
      earlier[epoch] = std::move(unsealed);
    }
  }
  // Recover logged positions independently of the cached positions. Admission
  // must check both before any files are handed to a durable process.
  for (auto& [key, position] : positions) {
    const auto name = position.objectName + "/" + std::to_string(position.dataEpoch);
    auto reference = parseDurableHostName(name);
    if (!reference) continue;
    auto& object = objects[name];
    object.namespaceId = reference->namespaceId;
    object.objectId = reference->objectId;
    object.dataEpoch = reference->dataEpoch;
    object.objectName = name;
    object.storageId = position.storageId;
    object.shard = position.shard;
    object.shardEpoch = std::max(object.shardEpoch, position.shardEpoch);
    object.state = ObjectState::LOCAL;
    object.healPending = true;
    auto& facet = object.facets[position.facetId];
    facet.facetId = position.facetId;
    facet.parentFacetId = position.parentFacetId;
    facet.facetName = position.facetName;
    facet.generation = position.generation;
    facet.salt1 = position.salt1;
    facet.salt2 = position.salt2;
    facet.txn = position.txn;
    facet.deleted = position.deleted;
    facet.lastFrame = position.lastFrame;
    // The live seal cursor belongs to this node epoch. Preserve the old
    // sequence separately for checkpoint/cache admission; it must not pin a
    // fully published predecessor's cold files against the new epoch's zero.
    facet.lastSeq = position.nodeEpoch == nodeEpoch ? position.nodeSeq : 0;
    facet.loggedNodeEpoch = position.nodeEpoch;facet.loggedSeq = position.nodeSeq;
    facet.loggedGeneration = position.generation;facet.loggedSalt1 = position.salt1;facet.loggedSalt2 = position.salt2;
    facet.loggedFrame = position.lastFrame;facet.loggedPageSize = position.pageSize;
    object.positionNodeEpoch = std::max(object.positionNodeEpoch, position.nodeEpoch);
  }
  log("log-read", "\"files\":" + std::to_string(files.size()) + ",\"fileNames\":" + fileNames +
          "],\"records\":" + std::to_string(read) +
          ",\"unsealed\":" + std::to_string(kept) + ",\"objects\":" + std::to_string(objects.size()) +
          ",\"cutShort\":" + std::to_string(broken));
  co_return;
}

kj::Promise<void> DurableLog::classifyEarlier() {
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
      noteEarlierRetired(candidate.epoch, record->second);epoch->second.erase(record);
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
  const auto wal = databasePath(storageId, facetId, "-wal");
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
    std::filesystem::remove(databasePath(storageId, facetId, "-shm"), code);
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
  std::filesystem::remove(databasePath(storageId, facetId, "-shm"), code);
  ++walCutBacks;
  log("wal-cut-back", "\"storageId\":" + quote(storageId) + ",\"facet\":" + std::to_string(facetId) + ",\"was\":\"" +
          std::to_string(size) + "\",\"now\":\"" + std::to_string(want) + "\"");
  return std::nullopt;
}

kj::Promise<void> DurableLog::stop(double deadlineMonotonicMs) {
  stopped = true;
  for (auto& [id, peer] : peers) retirePeer(peer);
  if (!enabled()) co_return;
  try {
    if (!records.empty()) co_await sealNow("drain");
  } catch (const kj::Exception& error) {
    log("drain-seal-failed", "\"error\":" + quote(std::string(error.getDescription().cStr())));
  }
  (void)deadlineMonotonicMs;
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

int DurableLog::openChannel(const std::string& processId) {
  if (!enabled()) return -1;
  int fds[2];
  if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
    log("channel-failed", "\"processId\":" + quote(processId) + ",\"errno\":" + std::to_string(errno));
    return -1;
  }
  closeChannel(processId);
  auto& channel = channels[processId];
  channel.processId = processId;
  ::fcntl(fds[0], F_SETFD, FD_CLOEXEC);
  channel.stream = lowLevel.wrapSocketFd(fds[0], kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
          kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC);
  channel.open = true;
  channel.writeChain = kj::READY_NOW;
  channel.reader = readChannel(channel).eagerlyEvaluate([this, processId](kj::Exception&& error) {
    auto found = channels.find(processId);
    if (found != channels.end()) channelEnded(found->second, "the durable channel failed");
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
    size_t at = 0;
    while (channel.inbox.size() - at >= 4) {
      uint32_t length = 0;
      for (int i = 0; i < 4; i++) length |= uint32_t(uint8_t(channel.inbox[at + i])) << (8 * i);
      if (channel.inbox.size() - at - 4 < length) break;
      ++channel.messages;
      onMessage(channel, std::string_view(channel.inbox).substr(at + 4, length));
      at += 4 + length;
    }
    if (at > 0) channel.inbox.erase(0, at);
  }
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
  std::string message;
  putU8(message, TO_SEALED);
  putText(message, key);
  putU32(message, salt1);
  putU32(message, salt2);
  putU32(message, upToFrame);
  for (auto& [id, channel] : channels) sendToChannel(channel, message);
}

void DurableLog::tellBreak(const std::string& key, const std::string& reason) {
  std::string message;
  putU8(message, TO_BREAK);
  putText(message, key);
  putText(message, reason);
  for (auto& [id, channel] : channels) sendToChannel(channel, message);
}

void DurableLog::askCheckpoint(const std::string& key) {
  std::string message;
  putU8(message, TO_CHECKPOINT);
  putText(message, key);
  ++checkpointAsks;
  for (auto& [id, channel] : channels) sendToChannel(channel, message);
}

void DurableLog::onMessage(Channel& channel, std::string_view message) {
  Reader in{message};
  const auto type = in.u8();
  switch (type) {
    case FROM_HELLO: {
      channel.pid = in.u32();
      log("process-attached", "\"processId\":" + quote(channel.processId) + ",\"pid\":" + std::to_string(channel.pid));
      return;
    }
    case FROM_FRAMES: {
      LogRecord record;
      const auto request = in.u64();
      const auto key = in.text();
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
      auto* object = objectOf(objectName);
      if (object == nullptr) {
        log("open-unknown", "\"object\":" + quote(objectName) + ",\"facet\":" + std::to_string(facetId));
        return;
      }
      object->dispatchPending = false;
      auto& facet = object->facets[facetId];
      facet.open = true;facet.processId = channel.processId;
      facet.openEpoch = object->openEpoch;facet.openGeneration = object->openGeneration;
      channel.openFacets[key] = {object->openEpoch, object->openGeneration};
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
      return;
    }
    case FROM_CHECKPOINT: {
      const auto key = in.text();
      const auto generation = in.u64();
      const auto salt1 = in.u32();
      const auto salt2 = in.u32();
      const auto frames = in.u32();
      in.u32();  // of those, confirmed
      const auto databaseBytes = in.u64();
      if (!in.ok) return;
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
      in.text();  // the key, which the fields below give again
      record.objectName = in.text();
      record.storageId = in.text();
      record.facetId = in.u32();
      record.parentFacetId = in.u32();
      record.facetName = in.text();
      if (!in.ok) return;
      record.kind = LogRecordKind::RESET;
      auto* object = objectOf(record.objectName);
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
        auto* object = objectOf(objectName);
        if (object == nullptr) return;
        const auto association = channel.openFacets.find(key);
        if (association == channel.openFacets.end()) return;
        const auto generation = association->second;
        channel.openFacets.erase(association);
        auto& facet = object->facets[facetId];
        if (generation != std::make_pair(object->openEpoch, object->openGeneration) ||
            facet.processId != channel.processId || facet.openEpoch != generation.first ||
            facet.openGeneration != generation.second) return;
        facet.open = false;
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
      LogRecord record;
      record.kind = LogRecordKind::DELETED;
      record.objectName = objectName;
      record.storageId = object->storageId;
      record.facetId = facetId;
      takeRecord(channel, std::move(record), 0);
      return;
    }
    case FROM_ALARM: {
      in.text();  // the key, which the fields below give again
      const auto objectName = in.text();
      const auto facetId = in.u32();
      const auto set = in.u8();
      const auto atUnixMs = in.u64();
      if (!in.ok) return;
      alarmReported(objectName, facetId, set ? atUnixMs : 0);
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
      if (!in.ok) return;
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

// One record of this node's log: it goes to the peers of its object's shard, and the answer to the request that
// carried it waits for them. Nothing here touches the disk or the object store (experiment 25, D.2).
void DurableLog::takeRecord(Channel& channel, LogRecord&& record, uint64_t request) {
  if (stopped || recoveryWriteFailed || (startupAttempted && !startupReady)) {
    if (request) answer(channel, request, ACK_UNAVAILABLE);
    return;
  }
  // The durable process names the object by its Host's name, which carries the data epoch; the history is the
  // object's, whichever copy of its files the frames came from.
  auto* object = objectOf(record.objectName);
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
    parsed[record.nodeSeq] = record;
  }
  InFlight flight;
  flight.nodeSeq = record.nodeSeq;
  flight.request = request;
  flight.processId = channel.processId;
  flight.objectKey = record.key();
  flight.shipperKey = record.shipperKey();
  flight.sentAtMs = monotonicTimeMs();
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
    peer.queuedBytes -= std::min(peer.queuedBytes, uint64_t(size));
    if (isolated.contains(item.sequence)) continue;
    peer.inFlightBytes += size;
    ++peer.appends;
    peer.sentSeq = std::max(peer.sentSeq, item.sequence);
    const auto nodeId = peer.nodeId;
    const auto session = peer.session;
    auto& sink = KJ_ASSERT_NONNULL(peer.sink);
    tasks.add(sink->value->append(std::move(item.bytes), item.partial)
                  .then([this, nodeId, session, size](PeerSink::Accepted accepted) {
      auto* current = currentPeer(nodeId, session);
      if (current == nullptr) return;
      current->inFlightBytes -= std::min(current->inFlightBytes, uint64_t(size));
      if (!accepted.refusal.empty()) {
        ++refusals;
        ++current->refusals;
        current->lastRefusal = accepted.refusal;
        if (accepted.refusal.find("fenced") != std::string::npos) ++fencedRecords;
        log("peer-refused", "\"peer\":" + quote(nodeId) + ",\"reason\":" + quote(accepted.refusal));
        pumpPeer(*current);
        return;
      }
      current->lastRefusal.clear();
      current->lastAckMs = monotonicTimeMs();
      onPeerAck(nodeId, std::min(accepted.sequence, current->sentSeq));
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
      if (found->second.ackedSeq >= sequence) ++held;
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
  for (auto sequence : done) {
    auto found = inFlight.find(sequence);
    if (found == inFlight.end()) continue;
    const auto& flight = found->second;
    const auto took = monotonicTimeMs() - flight.sentAtMs;
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
    isolated.insert(sequence);
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
  if (!enabled() || sealing || recoveryPublishWaiters || !nodeEpoch || recoveryWriteFailed ||
      (startupAttempted && !startupReady)) co_return;
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
    auto found = parsed.find(sequence);
    if (found != parsed.end()) touched.insert(hostNameOf(found->second));
  }
  for (const auto& [sequence, record] : records) {
    if (isolated.contains(sequence)) {
      if (!first) first = sequence;
      last = sequence;
      continue;
    }
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
    if (bytes >= settings.config.segmentBytes) break;
  }
  if (encoded.empty() && touched.empty()) co_return;
  sealing = true;
  const auto release = kj::defer([this]() { sealing = false; });
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
  for (auto& [key, entry] : index) header.objects.push_back(entry);
  const auto body = encoded.empty() ? std::string() : encodeSegment(header, encoded);
  if (!encoded.empty()) {
    if (!co_await uploadSegment(header, body)) co_return;
    if (nodeEpoch != header.nodeEpoch) co_return;
    segmentNumber = header.segment;
    ++segments;
    segmentBytes += body.size();
    lastSegmentAtMs = monotonicTimeMs();
    if (recoveryPublishWaiters) co_return;
  }
  // Each object's manifest names the segment before anything drops the records: a manifest nobody has written yet
  // leaves the segment pointing at nothing, and a node taking the shard over would not replay it.
  uint32_t written = 0, failed = 0;
  for (const auto& objectName : touched) {
    auto* object = objectOf(objectName);
    if (object == nullptr) continue;
    if (healingManifest(*object)) { ++failed; continue; }
    if (!encoded.empty()) object->segments.push_back(ManifestSegment{settings.nodeId, nodeEpoch, header.segment, first, last,
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
          ",\"bytes\":\"" + std::to_string(body.size()) + "\",\"objects\":" + std::to_string(touched.size()) +
          ",\"manifests\":" + std::to_string(written) + ",\"manifestsFailed\":" + std::to_string(failed) +
          ",\"reason\":" + quote(reason));
  if (failed || !last || recoveryWriteFailed) {
    // The records stay where they are and the next segment carries them again (a record that ends up in two
    // segments is applied once: a rebuild takes each by its shard epoch and sequence).
    co_return;
  }
  // The records are on the object store and every manifest names them: this node drops them, the peers are told to,
  // and each database may now checkpoint the frames the segment covers.
  for (uint64_t sequence = (first ? first : last); sequence <= last; ++sequence) {
    auto found = records.find(sequence);
    if (found != records.end()) {
      recordBytes -= found->second.size();
      records.erase(found);
    }
    parsed.erase(sequence);
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
  for (const auto& name : touched) {
    auto* object = objectOf(name);
    if (object != nullptr) writePosition(*object);
  }
}

// A segment is immutable and created only if absent; a failed upload is tried again with a growing wait, because
// the peers hold the records until it is there (requirement 7).
kj::Promise<bool> DurableLog::uploadSegment(const SegmentHeader& header, const std::string& body) {
  KJ_REQUIRE(body.size() <= STORE_SEGMENT_BYTES, "a segment exceeds the store client's read limit");
  const auto key = nodeSegmentKey(settings.nodeId, header.nodeEpoch, header.segment);
  auto defer = [&]() {
    return (header.reason != "earlier-epoch" && nodeEpoch != header.nodeEpoch) ||
        (header.reason != "recovery-tail" && recoveryPublishWaiters > 0);
  };
  auto wait = settings.config.uploadRetryMs;
  for (uint32_t attempt = 1;; ++attempt) {
    if (defer()) {
      log("segment-deferred", "\"key\":" + quote(key) + ",\"reason\":\"recovery or a new log epoch has priority\"");
      co_return false;
    }
    auto outcome = co_await store->create(key, body)
                       .then([](CreateResult result) { return std::make_pair(result.created, std::string()); })
                       .catch_([](kj::Exception&& error) {
      return std::make_pair(false, std::string(error.getDescription().cStr()));
    });
    if (outcome.second.empty() && !outcome.first) {
      // Existence alone does not prove this upload succeeded: another attempt may have used the same number.
      // Read errors retry the same immutable bytes; a different body must never be named by this manifest.
      auto held = co_await store->get(key).then([&body](ObjectResponse response) {
        return std::make_pair(response.status == 200 && response.bytes == body,
            response.status == 200 ? std::string() : std::string("the existing segment could not be read"));
      }).catch_([](kj::Exception&& error) {
        return std::make_pair(false, std::string(error.getDescription().cStr()));
      });
      outcome.second = std::move(held.second);
      if (outcome.second.empty() && !held.first) {
        ++uploadFailures;
        lastUploadError = "the existing segment has different bytes";
        log("segment-conflict", "\"key\":" + quote(key) + ",\"expectedSha256\":" + quote(sha256Hex(body)));
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
    if (defer()) co_return false;
    const auto waited = co_await uploadRetryCanceler.wrap(timer.afterDelay(wait * kj::MILLISECONDS))
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
  for (const auto& [sequence, encoded] : records) {
    if (!confirmed.contains(sequence) || isolated.contains(sequence)) continue;
    auto record = decodeLogRecord(encoded);
    if (record) take(*record);
  }
  for (const auto& [epoch, pending] : earlier) {
    for (const auto& [sequence, record] : pending) take(record);
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
  for (auto entry = records.begin(); entry != records.end();) {
    auto record = decodeLogRecord(entry->second);
    if (!record || record->objectName != wanted || !released.contains({record->shardEpoch, record->nodeSeq})) {
      ++entry;
      continue;
    }
    recordBytes -= std::min<uint64_t>(recordBytes, entry->second.size());
    noteEarlierRetired(record->nodeEpoch, *record);
    parsed.erase(entry->first);
    confirmed.erase(entry->first);
    entry = records.erase(entry);
  }
  for (auto epoch = earlier.begin(); epoch != earlier.end();) {
    std::erase_if(epoch->second, [&](const auto& entry) {
      const auto& record = entry.second;
      if (record.objectName != wanted || !released.contains({record.shardEpoch, record.nodeSeq})) return false;
      noteEarlierRetired(epoch->first, record);
      return true;
    });
    if (epoch->second.empty()) {
      earlierUploads.erase(epoch->first);
      epoch = earlier.erase(epoch);
    } else ++epoch;
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
      object->cacheInvalid || object->cacheAdmissionPending || object->healPending ||
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
kj::Promise<void> DurableLog::housekeeping() {
  for (;;) {
    co_await timer.afterDelay(1 * kj::SECONDS);
    const auto now = monotonicTimeMs();
    for (auto& [name, object] : objects) {
      if (object.state != ObjectState::LOCAL) continue;
      if (object.lastAccessMs == 0 || now - object.lastAccessMs < settings.config.idleCheckpointMs) continue;
      if (now - object.lastCheckpointAskMs < settings.config.idleCheckpointMs) continue;
      object.lastCheckpointAskMs = now;
      for (const auto& [facetId, facet] : object.facets) {
        if (facet.open) askCheckpoint(name + "/" + std::to_string(facetId));
      }
    }
    try {
      co_await resealEarlier();
      cleanupClosedEpoch();
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
}

void DurableLog::cleanupClosedEpoch() {
  if (recoveryWriteFailed || recoveryReadIncomplete || !nodeEpoch || (startupAttempted && !startupReady)) return;
  for (auto& [epoch, info] : predecessorLogs) {
    if (epoch == nodeEpoch || info.closed || !info.complete || info.sealedTo < info.maxSeq ||
        earlier.contains(epoch) || earlierExcluded.contains(epoch)) continue;
    if (std::any_of(info.unpublishedShards.begin(), info.unpublishedShards.end(), [](const auto& entry) { return entry.second != 0; })) continue;
    const auto original = logDirectory() / (wide(epoch) + ".log");
    std::error_code code;
    if (!std::filesystem::is_regular_file(original, code) || code) continue;
    std::vector<RecoveryEvent> carry;
    std::vector<std::string> carried;
    // Snapshot aggregate object state once per destination epoch. Later live
    // changes append to that same sidecar. The original files remain until the
    // whole snapshot and the replacement summary are durable.
    for (const auto& [name, state] : recoveryStates) {
      if (carriedRecoveryObjects.contains(name)) continue;
      auto found = objects.find(name);
      if (found == objects.end()) return;
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
    if (!appendRecovery(carry, true)) return;
    for (const auto& name : carried) carriedRecoveryObjects.insert(name);
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
    if (!atomicLocalFile(replacement, summary.str(), true)) {
      failRecovery("the released-epoch summary could not be made durable");return;
    }
    if (::unlink(original.c_str()) != 0 ||
        (::unlink((logDirectory() / (wide(epoch) + ".recovery")).c_str()) != 0 && errno != ENOENT) ||
        !syncDirectory(logDirectory())) {
      failRecovery("the released log epoch could not be replaced by its durable summary");return;
    }
    info.closed = true;
    log("log-epoch-closed", "\"epoch\":\"" + std::to_string(epoch) + "\",\"through\":\"" + std::to_string(info.maxSeq) +
        "\",\"summaryBytes\":" + std::to_string(summary.str().size()));
    return;  // At most one synchronous cleanup per housekeeping pass.
  }
}

void DurableLog::persistEarlierSeals() {
  if (recoveryWriteFailed) return;
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

kj::Promise<void> DurableLog::resealEarlier() {
  persistEarlierSeals();
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
      persistEarlierSeals();
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
  std::vector<std::string> encoded;
  SegmentHeader header;
  std::map<std::pair<std::string, uint32_t>, SegmentIndexEntry> index;
  std::set<std::string> touched;
  std::map<std::string, uint64_t> sourceEpochs;
  uint64_t wireBytes = segmentEnvelopeBudget(settings.nodeId, "earlier-epoch");
  for (const auto& [sequence, record] : chosen->second) {
    if (reuse && (sequence < cached->firstSeq || sequence > cached->lastSeq)) continue;
    if (!reuse) {
      auto bytes = encodeLogRecord(record);
      const auto extra = bytes.size() + 4 +
          (index.contains({record.objectName, record.facetId}) ? 0 : segmentEntryBudget(record));
      if (!encoded.empty() && (header.bytes + bytes.size() > settings.config.segmentBytes ||
          wireBytes + extra > STORE_SEGMENT_BYTES)) break;
      KJ_REQUIRE(wireBytes + extra <= STORE_SEGMENT_BYTES, "an earlier record cannot fit a readable store segment");
      wireBytes += extra;
      header.bytes += bytes.size();
      encoded.push_back(std::move(bytes));
    }
    ++header.records;
    if (!header.firstSeq) header.firstSeq = sequence;
    header.lastSeq = sequence;
    auto& entry = index[{record.objectName, record.facetId}];
    entry.objectName = record.objectName;
    entry.facetId = record.facetId;
    if (!entry.records) entry.firstTxn = record.txn;
    entry.lastTxn = record.txn;
    ++entry.records;
    touched.insert(hostNameOf(record));
    sourceEpochs[hostNameOf(record)] = std::max(sourceEpochs[hostNameOf(record)], record.shardEpoch);
  }
  if (!header.records) {
    earlierUploads.erase(epoch);
    if (auto held = earlier.find(epoch); held != earlier.end() && held->second.empty()) earlier.erase(held);
    co_return;
  }
  header.nodeId = settings.nodeId;
  header.nodeEpoch = epoch;
  header.segment = 1;
  header.sealedAtUnixMs = realClockUnixMs();
  header.reason = "earlier-epoch";
  for (auto& [key, entry] : index) header.objects.push_back(entry);
  // The segment number of an earlier epoch may be taken already: the next free one is used.
  if (!reuse) {
    auto listing = co_await store->list(nodeLogEpochPrefix(settings.nodeId, epoch));
    header.segment = latestRevision(listing, nodeLogPrefix(settings.nodeId) + wide(epoch)) + 1;
    const auto body = encodeSegment(header, encoded);
    if (!co_await uploadSegment(header, body)) co_return;
    earlierUploads[epoch] = {settings.nodeId, epoch, header.segment, header.firstSeq, header.lastSeq, 0};
    ++segments;
    log("segment-earlier", "\"epoch\":\"" + std::to_string(epoch) + "\",\"segment\":\"" +
            std::to_string(header.segment) + "\",\"records\":" + std::to_string(header.records) + ",\"objects\":" +
            std::to_string(touched.size()));
  } else {
    header.segment = cached->segment;
    header.firstSeq = cached->firstSeq;
    header.lastSeq = cached->lastSeq;
  }
  if (recoveryPublishWaiters) co_return;
  for (const auto& objectName : touched) {
    auto* object = objectOf(objectName);
    if (object == nullptr) {
      const auto identity = parseDurableHostName(objectName);
      KJ_REQUIRE(identity.has_value(), "an earlier record has no object identity");
      auto& held = objects[objectName];
      held.objectName = objectName;held.namespaceId = identity->namespaceId;held.objectId = identity->objectId;
      held.dataEpoch = identity->dataEpoch;held.storageId = durableHostId(settings.hostUniqueKey, objectName);
      held.shardEpoch = sourceEpochs[objectName];
      if (auto pending = earlier.find(epoch); pending != earlier.end())
        for (const auto& [sequence, record] : pending->second)
          if (hostNameOf(record) == objectName) { held.shard = record.shard; break; }
      object = &held;
    }
    if (healingManifest(*object)) continue;
    auto& attempts = earlierPublishAttempts[epoch][objectName];
    if (attempts >= 5) continue;
    ++attempts;
    const auto already = std::any_of(object->segments.begin(), object->segments.end(), [&](const ManifestSegment& segment) {
      return segment.nodeId == settings.nodeId && segment.nodeEpoch == epoch && segment.segment == header.segment;
    });
    if (!already) object->segments.push_back(ManifestSegment{settings.nodeId, epoch, header.segment, header.firstSeq,
        header.lastSeq, sourceEpochs[objectName]});
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
      if (auto held = earlier.find(epoch); held != earlier.end()) {
        std::erase_if(held->second, [&](const auto& entry) {
          const auto& record = entry.second;
          if (entry.first < header.firstSeq || entry.first > header.lastSeq || hostNameOf(record) != objectName) return false;
          noteEarlierRetired(epoch, record);
          return true;
        });
        if (held->second.empty()) earlier.erase(held);
      }
    }
  }
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
  persistEarlierSeals();
  if (prioritized && earlier.contains(epoch)) {
    earlierPriority.push_back(epoch);
    tasks.add(timer.afterDelay(0 * kj::MILLISECONDS).then([this]() { return resealEarlier(); }));
  }
}

// This machine's files are a cache: when they take more than their share, the objects nothing has asked for in the
// longest time and whose changes are all in a segment lose theirs. The object is rebuilt from its history when it
// is asked for again.
kj::Promise<void> DurableLog::dropColdFiles() {
  coldFilesSkippedLive = 0;
  if (recoveryWriteFailed || (startupAttempted && !startupReady)) co_return;
  std::error_code code;
  uint64_t total = 0;
  std::set<std::string> pendingEarlier;
  for (const auto& [epoch, pending] : earlier)
    for (const auto& [sequence, record] : pending) pendingEarlier.insert(hostNameOf(record));
  for (const auto& entry : std::filesystem::directory_iterator(logDirectory(), code)) {
    if (!entry.is_regular_file(code)) continue;
    const auto bytes = entry.file_size(code);
    if (!code) total += bytes;
  }
  std::vector<std::pair<double, std::string>> cold;
  for (const auto& [name, object] : objects) {
    uint64_t bytes = 0;
    for (const auto& [facetId, facet] : object.facets) {
      const auto size = std::filesystem::file_size(databasePath(object.storageId, facetId), code);
      if (!code) bytes += size;
      const auto wal = std::filesystem::file_size(databasePath(object.storageId, facetId, "-wal"), code);
      if (!code) bytes += wal;
    }
    total += bytes;
    const bool quiet = object.lastAccessMs == 0 || monotonicTimeMs() - object.lastAccessMs > 60000;
    bool sealedAll = true;
    for (const auto& [facetId, facet] : object.facets) {
      if (facet.lastSeq > sealedSeq) sealedAll = false;
    }
    if (quiet && sealedAll && !pendingEarlier.contains(name) && !object.snapshotting && !object.healPending &&
        object.state == ObjectState::LOCAL && bytes > 0 && object.manifestRevision > 0)
      cold.emplace_back(object.lastAccessMs, name);
  }
  diskUsedBytes = total;
  if (total <= diskLimitBytes || cold.empty()) co_return;
  std::sort(cold.begin(), cold.end());
  for (const auto& [access, name] : cold) {
    if (total <= diskLimitBytes) break;
    auto* object = objectOf(name);
    if (object == nullptr) continue;
    if (hasLiveOpenFacets(*object) || hasLiveHolders(*object)) {
      ++coldFilesSkippedLive;
      continue;
    }
    uint64_t freed = 0;
    for (const auto& [facetId, facet] : object->facets) {
      for (const char* suffix : {"", "-wal", "-shm"}) {
        const auto path = databasePath(object->storageId, facetId, suffix);
        const auto size = std::filesystem::file_size(path, code);
        if (!code) freed += size;
        std::filesystem::remove(path, code);
      }
    }
    std::filesystem::remove(durableDirectory() / (object->storageId + ".facets"), code);
    ++object->snapshotVersion;
    object->state = ObjectState::UNKNOWN;
    object->facets.clear();
    total -= std::min<uint64_t>(total, freed);
    ++filesDropped;
    log("files-dropped", "\"object\":" + quote(name) + ",\"bytes\":\"" + std::to_string(freed) +
            "\",\"reason\":\"this machine's share of Durable Object files is full\"");
  }
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
    const ObjectIdentity& identity, const std::string& processId, uint64_t generation) {
  if (!enabled()) return {};
  if ((startupAttempted && !startupReady) || recoveryWriteFailed)
    return {false, 503, "STORAGE_UNAVAILABLE", "the node's recovery log is not ready"};
  auto* object = objectOf(durableHostName(identity.namespaceId, identity.objectId, identity.dataEpoch));
  if (ownerFenced(identity.namespaceId, identity.shard, identity.shardEpoch) ||
      (object != nullptr && (object->shardEpoch != identity.shardEpoch || object->state == ObjectState::GONE)))
    return {false, 409, "DO_SHARD_EPOCH_MISMATCH", "the object's shard ownership changed before dispatch"};
  if (object == nullptr || object->state != ObjectState::LOCAL || object->cacheAdmissionPending || object->healPending)
    return {false, 503, "DO_RECOVERING", "the object needs recovery before dispatch"};
  const auto target = processId + "-" + std::to_string(generation);
  for (const auto& [id, channel] : channels) {
    if (!channel.open || id == target) continue;
    if (object->dispatchedChannel == id)
      return {false, 503, "DO_UNAVAILABLE", "another durable generation still holds the object"};
    if (channelHasOpenObject(channel, *object))
      return {false, 503, "DO_UNAVAILABLE", "another durable generation still holds the object"};
  }
  if (auto error = prepareOpen(*object, &target))
    return {false, 503, "STORAGE_UNAVAILABLE", *error};
  return {};
}

void DurableLog::objectDispatched(const ObjectIdentity& identity, const std::string& processId, uint64_t generation) {
  if (auto* object = objectOf(durableHostName(identity.namespaceId, identity.objectId, identity.dataEpoch))) {
    object->lastAccessMs = monotonicTimeMs();
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

kj::Promise<DurableLog::ObjectReady> DurableLog::openObject(ObjectIdentity identity) {
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
  auto settled = rebuilding.find(objectName);
  if (settled != rebuilding.end() && settledRebuilds.contains(objectName)) {
    rebuilding.erase(settled);
    rebuildCancelers.erase(objectName);
    settledRebuilds.erase(objectName);
  }
  auto& object = objects[objectName];
  if (object.objectName.empty()) {
    object.objectName = objectName;
    object.namespaceId = identity.namespaceId;
    object.objectId = identity.objectId;
    object.dataEpoch = identity.dataEpoch;
    object.storageId = durableHostId(settings.hostUniqueKey, objectName);
  }
  object.shard = identity.shard;
  if (!identity.tenant.empty()) object.tenant = identity.tenant;
  // A background healer may be awaiting the same local restart acquisition
  // that just produced this request's route. Its cache is the surviving legacy
  // copy: join it without canceling it or changing its admission identity.
  const bool joiningHeal = object.healPending && rebuilding.contains(objectName) &&
      (object.state == ObjectState::LOCAL || object.state == ObjectState::REBUILDING);
  if (identity.shardEpoch > object.shardEpoch && object.state == ObjectState::REBUILDING && !joiningHeal) {
    object.state = ObjectState::UNKNOWN;
    if (auto canceler = rebuildCancelers.find(objectName); canceler != rebuildCancelers.end())
      canceler->second->cancel("the object's recovery was superseded by a new shard epoch");
    rebuilding.erase(objectName);
    rebuildCancelers.erase(objectName);
    settledRebuilds.erase(objectName);
  }
  if (identity.shardEpoch >= object.shardEpoch && !joiningHeal) {
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
  // Risky legacy caches are admitted inside the shared healing operation.
  // A request must join a background healer rather than race its state change
  // or hand its result to workerd with admission still pending.
  if (object.cacheAdmissionPending && object.state == ObjectState::LOCAL &&
      (!object.healPending || object.dataEpoch == 1)) {
    const auto admitted = co_await admitLegacyCache(objectName).then([]() { return true; })
        .catch_([](kj::Exception&&) { return false; });
    if (!admitted) co_return ObjectReady{false, 503, "STORAGE_UNAVAILABLE", "the legacy cache admission history could not be read"};
    if (ownerFenced(identity.namespaceId, identity.shard, identity.shardEpoch) || object.shardEpoch != identity.shardEpoch)
      co_return ObjectReady{false, 409, "DO_SHARD_EPOCH_MISMATCH", "ownership changed during cache admission"};
  }
  // An object past durableObjects.objectBytes is not turned away here: only its writes are refused, at the moment
  // their frames would be confirmed (413 DO_OBJECT_TOO_LARGE), and everything that only reads is served.
  // Data epoch 1 has never crossed an owner boundary. A restart or an own
  // seal cannot turn this trusted cache into a legacy takeover risk.
  if (object.healPending && object.dataEpoch == 1) {
    object.healPending = false;
    object.healError = false;
  }
  const bool healing = object.healPending &&
      (object.state == ObjectState::LOCAL || object.state == ObjectState::REBUILDING);
  if (healing) object.healRequested = true;
  if (object.cacheHold) {
    auto current = co_await readManifest(object.namespaceId, object.objectId)
        .catch_([](kj::Exception&&) { return std::optional<ObjectManifest>{}; });
    if (ownerFenced(identity.namespaceId, identity.shard, identity.shardEpoch))
      co_return ObjectReady{false, 409, "DO_SHARD_EPOCH_MISMATCH", "the owner epoch was fenced"};
    if (current && current->hasSnapshot && current->snapshotShardEpoch >= object.dataEpoch) object.cacheHold = false;
    else co_return ObjectReady{false, 503, "STORAGE_UNAVAILABLE", "HOLD: the local cache may be the only complete copy"};
  }
  if (object.state == ObjectState::LOCAL && !healing) {
    if (auto error = prepareOpen(object)) co_return ObjectReady{false, 503, "STORAGE_UNAVAILABLE", *error};
    co_return ObjectReady{};
  }
  if (healing && monotonicTimeMs() < object.healRetryAfterMs)
    co_return ObjectReady{false, 503, "STORAGE_UNAVAILABLE", "the trusted local object is waiting for its healing snapshot"};
  if (object.state == ObjectState::GONE) {
    co_return ObjectReady{false, 409, "DO_SHARD_EPOCH_MISMATCH",
        "another node owns this object's shard now"};
  }
  const auto reason = object.state == ObjectState::REPAIR
      ? object.repairReason
      : std::string("this node has not served the object at this data epoch");
  if (rebuilding.find(objectName) == rebuilding.end()) {
    auto canceler = std::make_unique<kj::Canceler>();
    auto operation = healing ? healObject(objectName) : rebuild(objectName, reason);
    auto counted = kj::mv(operation).catch_([this, identity, objectName, healing](kj::Exception&& error) {
      auto* current = objectOf(objectName);
      if (!healing && current != nullptr && current->state != ObjectState::GONE &&
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
  const auto start = monotonicTimeMs();
  auto branch = rebuilding.find(objectName)->second.addBranch();
  bool done = false;
  try {
    done = co_await kj::mv(branch).then([]() { return true; }).exclusiveJoin(
        timer.afterDelay(settings.config.recoverTimeoutMs * kj::MILLISECONDS).then([]() { return false; }));
  } catch (const kj::Exception& error) {
    if (auto* current = objectOf(objectName); current == nullptr || current->state == ObjectState::GONE ||
        (!healing && current->shardEpoch != identity.shardEpoch))
      co_return ObjectReady{false, 409, "DO_SHARD_EPOCH_MISMATCH", "ownership changed during recovery",
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
  if (auto error = prepareOpen(*entry))
    co_return ObjectReady{false, 503, "STORAGE_UNAVAILABLE", *error};
  ObjectReady ready;
  ready.elapsedMs = monotonicTimeMs() - start;
  ready.rebuilt = true;
  co_return ready;
}

// The whole of a rebuild: the object's files on this machine go, its manifest says which snapshot and which
// segments make it up, and what no segment covers comes from the peers of its shard (and from this node's own log
// when it is repairing an object of its own). The previous owner's files are never read.
kj::Promise<void> DurableLog::rebuild(std::string objectName, std::string reason) {
  const auto done = kj::defer([this, objectName]() { settledRebuilds.insert(objectName); });
  auto* object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "the recovery object disappeared");
  const auto targetEpoch = object->shardEpoch, targetDataEpoch = object->dataEpoch;
  for (uint32_t attempt = 1; attempt <= MANIFEST_ATTEMPTS; ++attempt) {
    object->recoveryRevisionChanged = false;
    try {
      co_await rebuildAttempt(objectName, reason);
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
    const std::optional<ObjectManifest>& manifest) {
  if (!manifest) co_return 0;
  auto* object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "the recovery object disappeared before its lower-bound check");
  const auto namespaceId = object->namespaceId;const auto shard = object->shard;
  ObjectResponse response{200, {}};
  if (auto cached = object->recoveryEpochReads.find(manifest->shardEpoch); cached != object->recoveryEpochReads.end()) {
    response.bytes = cached->second.first;response.lastModifiedUnixMs = cached->second.second;
  } else {
    response = co_await timer.timeoutAfter(std::max(settings.peerTimeoutMs, settings.config.confirmTimeoutMs) * kj::MILLISECONDS,
        store->get(shardEpochKey(namespaceId, shard, manifest->shardEpoch)));
  }
  const auto epoch = response.status == 200 ? parseShardEpoch(response.bytes) : std::nullopt;
  requireStorage(epoch && epoch->epoch == manifest->shardEpoch && epoch->namespaceId == namespaceId && epoch->shard == shard,
      "the latest manifest's ownership origin is unavailable");
  object = objectOf(objectName);KJ_REQUIRE(object != nullptr, "the lower-bound object disappeared");
  object->recoveryEpochReads.try_emplace(epoch->epoch, response.bytes, response.lastModifiedUnixMs);
  co_return std::max(manifest->snapshotShardEpoch, epoch->dataEpoch);
}

kj::Promise<void> DurableLog::rebuildAttempt(std::string objectName, std::string reason) {
  const auto start = monotonicTimeMs();
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
      object->recoveryAddresses.clear();object->recoveryUnknownKeys.clear();object->recoveryEpochReads.clear();
      object->recoveryDiscoveryCompleted = false;
    }
    tailStart = object->recoveryStartedMs;
    const bool repair = object->state == ObjectState::REPAIR;
    KJ_REQUIRE(!recoveryWriteFailed && !object->cacheHold, "recovery cannot replace a held or unfenced image");
    if (repair) KJ_REQUIRE(beginRepair(*object), "the repair exclusion could not be made durable");
    isolateObjectPending(*object, "pending records cannot outlive the image used by recovery");
    KJ_REQUIRE(!recoveryWriteFailed, "pending exclusions could not be made durable");
    const bool closeLive = hasLiveOpenFacets(*object) || (hasLiveDispatch(*object) && object->dispatchPending) ||
        hasLiveHolders(*object);
    object->rebuildGeneration = {nodeEpoch, ++recoverySerial};
    KJ_REQUIRE(appendRecovery({recoveryEvent(RecoveryKind::REBUILD_BEGIN, *object, object->rebuildGeneration)}, false),
        "the rebuild begin marker could not be written");
    ++object->snapshotVersion;
    object->state = ObjectState::REBUILDING;
    log("rebuild-started", "\"object\":" + quote(objectName) + ",\"reason\":" + quote(reason) + ",\"repair\":" +
            (repair ? "true" : "false"));
    if (closeLive && hooks.closeObjects) {
      // Nothing may hold the files while they are replaced, and the object must stop serving what it read from them.
      co_await hooks.closeObjects("a Durable Object write was not confirmed: " + reason);
    }
  }
  auto* object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "an object being rebuilt is gone");
  const auto targetEpoch = object->shardEpoch;
  const auto targetDataEpoch = object->dataEpoch;
  auto check = [&]() {
    object = objectOf(objectName);
    KJ_REQUIRE(object != nullptr && object->state == ObjectState::REBUILDING &&
        object->shardEpoch == targetEpoch && object->dataEpoch == targetDataEpoch &&
        !ownerFenced(object->namespaceId, object->shard, targetEpoch) && !recoveryWriteFailed,
        "ownership changed during object recovery");
  };
  const auto stagingId = object->storageId + ".rebuild-" + std::to_string(nodeEpoch) + "-" + std::to_string(targetEpoch);
  auto manifest = co_await readManifest(object->namespaceId, object->objectId);
  check();
  const auto initialLowerBound = co_await recoveryLowerBound(objectName, manifest);
  check();
  std::vector<ManifestFacet> facets;
  uint64_t fromSeq = 0;
  object->recoveryManifestRevision = manifest ? manifest->revision : 0;
  auto collecting = fetchTails(ObjectIdentity{object->namespaceId, object->objectId, object->tenant,
      object->dataEpoch, object->shardEpoch, object->shard},
      initialLowerBound, tailStart).eagerlyEvaluate(nullptr);
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
      co_await downloadSnapshot(*manifest, stagingId);
      check();
      facets = manifest->facets;
      for (const auto& facet : facets) fromSeq = std::max(fromSeq, facet.upToSeq);
    }
  }
  std::vector<LogRecord> history;
  if (manifest) history = co_await readSegments(*manifest, object->namespaceId + "/" + object->objectId);
  check();
  std::set<std::pair<uint64_t, uint64_t>> durableRecords;
  for (const auto& record : history) durableRecords.emplace(record.shardEpoch, record.nodeSeq);
  object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "an object being rebuilt is gone");
  // What no segment covers: this node's own unsealed records for the object, and what the peers of its shard hold
  // of whichever owner wrote last.
  const auto historyName = object->namespaceId + "/" + object->objectId;
  std::set<std::pair<uint64_t, uint64_t>> localUnsealed;
  for (const auto& [sequence, encoded] : records) {
    if (isolated.contains(sequence)) continue;
    auto decoded = decodeLogRecord(encoded);
    if (decoded && decoded->objectName == historyName) {
      auto state = recoveryStates.find(hostNameOf(*decoded));
      if (state != recoveryStates.end() && state->second.exclusions.contains({decoded->shardEpoch, decoded->nodeSeq})) continue;
      localUnsealed.emplace(decoded->shardEpoch, decoded->nodeSeq);
      history.push_back(std::move(*decoded));
    }
  }
  for (const auto& [epoch, taken] : earlier) {
    for (const auto& [sequence, record] : taken) {
      if (record.objectName == historyName) {
        localUnsealed.emplace(record.shardEpoch, record.nodeSeq);
        history.push_back(record);
      }
    }
  }
  auto tails = co_await kj::mv(collecting);
  check();
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
      co_await downloadSnapshot(*manifest, stagingId);
      check();
      facets = manifest->facets;
    }
    auto published = co_await readSegments(*manifest, historyName);
    check();
    for (auto& record : published) {
      durableRecords.emplace(record.shardEpoch, record.nodeSeq);
      history.push_back(std::move(record));
    }
  }
  object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "an object being rebuilt is gone");
  const auto lowerBound = co_await recoveryLowerBound(objectName, manifest);
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
  for (auto& record : tails.records) {
    if (record.shardEpoch < lowerBound) { noteLateTail(objectName, record, lowerBound);continue; }
    const auto position = std::make_pair(record.shardEpoch, record.nodeSeq);
    if (drop.contains(position)) continue;
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
      continue;
    }
    if (!durableRecords.contains(position)) adopted.push_back(record);
    history.push_back(std::move(record));
  }
  // Only now is every required store source readable. Keep the original files
  // intact on a failed manifest/segment/snapshot read, including unique legacy caches.
  check();removeObjectFiles(*object);object->facets.clear();
  if (manifest && manifest->hasSnapshot) {
    for (const auto& facet : facets) {
      std::error_code code;
      std::filesystem::rename(databasePath(stagingId, facet.facetId), databasePath(object->storageId, facet.facetId), code);
      KJ_REQUIRE(!code, "a verified recovery snapshot could not replace the cache");
    }
  }
  applyRecords(*object, std::move(history), facets, snapshotShardEpoch, drop);
  std::vector<ManifestFacet> index;
  for (const auto& [id, facet] : object->facets) {
    ManifestFacet entry;
    entry.facetId = id; entry.parentFacetId = facet.parentFacetId; entry.name = facet.facetName;
    index.push_back(std::move(entry));
  }
  writeFacetIndex(object->storageId, index);
  const auto publishingStarted = monotonicTimeMs();
  if (tails.cut) co_await forceRecoverySnapshot(objectName);
  else co_await publishRecovery(objectName, adopted);
  check();
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
  object->recoveryPublished = true;object->cacheInvalid = object->cacheHold = object->cacheAdmissionPending = false;
  object->cacheOpen = false;object->healPending = false;object->healingInProgress = false;object->repairGeneration = {};
  std::set<std::pair<uint64_t, uint64_t>> publishedPositions = durableRecords;
  for (const auto& record : adopted) publishedPositions.emplace(record.shardEpoch, record.nodeSeq);
  auto retire = [&](auto& epochs) {
    for (auto epoch = epochs.begin(); epoch != epochs.end();) {
      std::erase_if(epoch->second, [&](const auto& entry) {
        const auto& record = entry.second;
        if (record.objectName != historyName || (!publishedPositions.contains({record.shardEpoch, record.nodeSeq}) &&
            !drop.contains({record.shardEpoch, record.nodeSeq}) && record.shardEpoch >= lowerBound && !coveredBySnapshot(record))) return false;
        noteEarlierRetired(epoch->first, record);return true;
      });
      if (epoch->second.empty()) epoch = epochs.erase(epoch);else ++epoch;
    }
  };
  retire(earlier);retire(earlierExcluded);persistEarlierSeals();
  log("recovery-published", "\"object\":" + quote(objectName) + ",\"collectionMs\":" +
      std::to_string(tails.collectionMs) + ",\"writeMs\":" +
      std::to_string(monotonicTimeMs() - publishingStarted) + ",\"cut\":" + (tails.cut ? "true" : "false") +
      ",\"adoptedRecords\":" + std::to_string(adopted.size()));
  object->state = ObjectState::LOCAL;
  object->recoveryStartedMs = 0;
  object->recoveryTails.clear();
  object->recoveryAddresses.clear();object->recoveryUnknownKeys.clear();object->recoveryEpochReads.clear();
  object->recoveryDiscoveryCompleted = false;
  object->recoveryManifestRevision.reset();
  object->lateTails.clear();
  object->repairReason.clear();
  ++rebuilds;
  ++object->rebuilds;
  lastRebuildMs = monotonicTimeMs() - start;
  object->lastRebuildMs = lastRebuildMs;
  writePosition(*object);
  log("rebuilt", "\"object\":" + quote(objectName) + ",\"ms\":" + std::to_string(lastRebuildMs) + ",\"manifest\":\"" +
          std::to_string(object->manifestRevision) + "\",\"snapshot\":\"" + std::to_string(object->snapshotSequence) +
          "\",\"fromSeq\":\"" + std::to_string(fromSeq) + "\"");
}

void DurableLog::removeObjectFiles(const Object& object) {
  KJ_REQUIRE(!recoveryWriteFailed && !object.cacheHold, "a held or unfenced local image cannot be deleted");
  std::error_code code;
  for (uint32_t facetId = 0; facetId < 64; ++facetId) {
    for (const char* suffix : {"", "-wal", "-shm"}) {
      std::filesystem::remove(databasePath(object.storageId, facetId, suffix), code);
    }
  }
  std::filesystem::remove(durableDirectory() / (object.storageId + ".facets"), code);
}

// A snapshot's parts, at most durableObjects.recoverParallelism at a time (R5: a volume server of SeaweedFS 3.97
// answered 7 of 1000 reads with an error at 32 at once), each written where it belongs in its facet's file.
kj::Promise<void> DurableLog::downloadSnapshot(const ObjectManifest& manifest, const std::string& storageId) {
  const auto partBytes = settings.config.snapshotPartBytes;
  for (const auto& facet : manifest.facets) {
    const auto path = databasePath(storageId, facet.facetId);
    std::error_code code;
    std::filesystem::create_directories(path.parent_path(), code);
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    KJ_REQUIRE(fd >= 0, "cannot write a Durable Object database", path.string().c_str());
    const auto close = kj::defer([fd]() { ::close(fd); });
    kj::Vector<kj::Promise<void>> running;
    for (uint64_t part = 0; part < facet.parts; ++part) {
      running.add(downloadSnapshotPart(manifest, facet.facetId, part, fd, off_t(part * partBytes)));
      if (running.size() >= settings.config.recoverParallelism) {
        co_await kj::joinPromises(running.releaseAsArray());
        running.clear();
      }
    }
    if (running.size() > 0) co_await kj::joinPromises(running.releaseAsArray());
    ::fsync(fd);
    // The file must be the one the snapshot names: a wrong digest would be read as the object's state.
    const auto bytes = readFile(path);
    KJ_REQUIRE(bytes.size() == facet.bytes && sha256Hex(bytes) == facet.sha256,
        "a snapshot of a Durable Object does not add up", path.string().c_str(), facet.bytes, bytes.size());
  }
  co_return;
}

kj::Promise<void> DurableLog::downloadSnapshotPart(const ObjectManifest& manifest, uint32_t facetId, uint64_t part,
    int fd, off_t offset) {
  const auto key = objectSnapshotPartKey(manifest.namespaceId, manifest.objectId, manifest.snapshotSequence, facetId,
      part);
  auto object = co_await store->get(key);
  KJ_REQUIRE(object.status == 200, "a part of a Durable Object's snapshot is not on the store", key.c_str(),
      object.status);
  size_t written = 0;
  while (written < object.bytes.size()) {
    auto wrote = ::pwrite(fd, object.bytes.data() + written, object.bytes.size() - written, offset + off_t(written));
    if (wrote < 0 && errno == EINTR) continue;
    KJ_REQUIRE(wrote > 0, "cannot write a part of a Durable Object's snapshot");
    written += size_t(wrote);
  }
  co_return;
}

// The segments a manifest names, in order, at most recoverParallelism downloads at a time; only the records of this
// object are kept.
kj::Promise<std::vector<LogRecord>> DurableLog::readSegments(const ObjectManifest& manifest,
    const std::string& objectName) {  // the object in the history: "<namespace>/<object>" 
  std::vector<LogRecord> out;
  std::map<uint64_t, std::optional<ShardEpoch>> owners;
  const std::set<std::pair<uint64_t, uint64_t>> exclusions(manifest.isolated.begin(), manifest.isolated.end());
  std::vector<ManifestSegment> wanted = manifest.segments;
  std::sort(wanted.begin(), wanted.end(), [](const ManifestSegment& a, const ManifestSegment& b) {
    return std::tie(a.shardEpoch, a.nodeEpoch, a.segment) < std::tie(b.shardEpoch, b.nodeEpoch, b.segment);
  });
  for (size_t at = 0; at < wanted.size();) {
    kj::Vector<kj::Promise<std::string>> running;
    const size_t until = std::min<size_t>(wanted.size(), at + settings.config.recoverParallelism);
    for (size_t i = at; i < until; ++i) {
      const auto key = nodeSegmentKey(wanted[i].nodeId, wanted[i].nodeEpoch, wanted[i].segment);
      running.add(store->get(key).then([key](ObjectResponse response) {
        KJ_REQUIRE(response.status == 200, "a segment of a Durable Object's log is not on the store", key.c_str(),
            response.status);
        return std::move(response.bytes);
      }));
    }
    auto bodies = co_await kj::joinPromises(running.releaseAsArray());
    for (size_t i = 0; i < bodies.size(); ++i) {
      std::string error;
      auto contents = decodeSegment(bodies[i], &error);
      KJ_REQUIRE(contents.has_value(), "a segment of a Durable Object's log does not add up", error.c_str());
      for (auto& record : contents->records) {
        if (record.objectName != objectName) continue;
        const auto& header = contents->header;
        const auto& ref = wanted[at + i];
        if (exclusions.contains({record.shardEpoch, record.nodeSeq}) && header.reason != "earlier-epoch" &&
            header.reason != "recovery-tail" && header.nodeId == ref.nodeId &&
            header.nodeEpoch == ref.nodeEpoch && header.segment == ref.segment) {
          auto [owner, fresh] = owners.try_emplace(record.shardEpoch);
          if (fresh) {
            auto response = co_await store->get(shardEpochKey(manifest.namespaceId, record.shard, record.shardEpoch));
            requireStorage(response.status == 200, "an excluded record's original shard owner is unavailable");
            owner->second = parseShardEpoch(response.bytes);
            requireStorage(owner->second && owner->second->namespaceId == manifest.namespaceId &&
                owner->second->shard == record.shard && owner->second->epoch == record.shardEpoch,
                "an excluded record's original shard owner is invalid");
          }
          record.ownerSealed = owner->second && owner->second->nodeId == header.nodeId &&
              owner->second->nodeEpoch == header.nodeEpoch && record.nodeEpoch == header.nodeEpoch;
          if (record.ownerSealed) log("foreign-isolation-ignored", "\"object\":" + quote(objectName) +
              ",\"shardEpoch\":\"" + std::to_string(record.shardEpoch) + "\",\"nodeSeq\":\"" +
              std::to_string(record.nodeSeq) + "\"");
        }
        out.push_back(std::move(record));
      }
    }
    at = until;
  }
  co_return out;
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
kj::Promise<DurableLog::TailRecovery> DurableLog::fetchTails(
    ObjectIdentity identity, uint64_t snapshotShardEpoch, double startedMs) {
  Object object;
  object.namespaceId = identity.namespaceId; object.objectId = identity.objectId;
  object.dataEpoch = identity.dataEpoch; object.shardEpoch = identity.shardEpoch; object.shard = identity.shard;
  object.objectName = durableHostName(identity.namespaceId, identity.objectId, identity.dataEpoch);
  const auto deadline = startedMs + settings.tailWaitMs;
  const auto wanted = object.namespaceId + "/" + object.objectId;
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
    std::map<Position, LogRecord> records;
    std::set<Position> ownerRecords;
    bool ownerComplete = false;
  };
  struct State {
    std::vector<Source> sources;
    std::map<std::string, DurableTailTarget> addresses;
    std::set<std::string> unresolved;
    bool discoveryAttempted = false, discoverySucceeded = false;
  };
  auto state = std::make_shared<State>();
  if (auto* live = objectOf(object.objectName); live != nullptr) {
    state->addresses = live->recoveryAddresses;state->unresolved = live->recoveryUnknownKeys;
  }
  const auto preserveDiscovery = kj::defer([this, state, identity, name = object.objectName]() {
    auto* live = objectOf(name);
    if (live == nullptr || live->dataEpoch != identity.dataEpoch || live->shardEpoch != identity.shardEpoch) return;
    live->recoveryAddresses = state->addresses;live->recoveryUnknownKeys = state->unresolved;
    live->recoveryDiscoveryCompleted |= state->discoverySucceeded;
  });
  // Epoch objects are authority, not hints. A failed read is a storage failure,
  // never permission to treat the history as empty and publish a cut.
  auto epoch = object.shardEpoch;
  std::set<uint64_t> visited;
  while (epoch && (!snapshotShardEpoch || epoch >= snapshotShardEpoch)) {
    KJ_REQUIRE(visited.insert(epoch).second, "a shard's previous-epoch chain cycles");
    ObjectResponse response{200, {}};
    auto* live = objectOf(object.objectName);
    KJ_REQUIRE(live != nullptr, "the recovery object disappeared while reading its source chain");
    auto cached = live->recoveryEpochReads.find(epoch);
    if (cached != live->recoveryEpochReads.end()) {
      response.bytes = cached->second.first;response.lastModifiedUnixMs = cached->second.second;
    } else {
      response = co_await timer.timeoutAfter(std::max(settings.peerTimeoutMs, settings.config.confirmTimeoutMs) *
          kj::MILLISECONDS, store->get(shardEpochKey(object.namespaceId, object.shard, epoch)))
          .catch_([](kj::Exception&& error) -> ObjectResponse {
            failStorage("a source shard epoch could not be read: " + std::string(error.getDescription().cStr()));
          });
    }
    requireStorage(response.status == 200, "a source shard epoch is unavailable during recovery");
    auto record = parseShardEpoch(response.bytes);
    requireStorage(record && record->epoch == epoch && record->namespaceId == object.namespaceId &&
        record->shard == object.shard, "a source shard epoch is invalid during recovery");
    Source source;
    source.epoch = *record;
    source.acquiredAtUnixMs = response.lastModifiedUnixMs;
    if (auto* live = objectOf(object.objectName); live != nullptr)
      live->recoveryEpochReads.try_emplace(epoch, response.bytes, response.lastModifiedUnixMs);
    if (auto* live = objectOf(object.objectName); live != nullptr) {
      for (const auto& [position, held] : live->recoveryTails) {
        if (held.shardEpoch == record->epoch && held.nodeEpoch == record->nodeEpoch)
          source.records.emplace(position, held);
      }
    }
    if (record->nodeId == settings.nodeId && record->nodeEpoch != nodeEpoch &&
        !membership.heldEpochs.contains(std::to_string(record->nodeEpoch))) {
      auto prior = predecessorLogs.find(record->nodeEpoch);
      if (diskTrusted && sameBootCache && prior != predecessorLogs.end() && prior->second.complete) {
        source.ownerComplete = true;
        source.records.clear();
        const bool has = prior->second.startupUnsealed.contains({wanted, record->epoch});
        // This immutable raw-log observation is scoped to namespace+objectId,
        // independent of dataEpoch. Every unexcluded byte is a possible write.
        if (has) {
          if (auto held = earlier.find(record->nodeEpoch); held != earlier.end())
            for (const auto& [sequence, entry] : held->second) {
              if (entry.objectName != wanted || entry.shardEpoch != record->epoch) continue;
              const Position position{entry.shardEpoch, entry.nodeSeq};
              source.records.emplace(position, entry);source.ownerRecords.insert(position);
            }
        } else ++localLogEmptyClosures;
        log(has ? "local-owner-log-complete" : "local-owner-log-empty", "\"object\":" + quote(object.objectName) +
            ",\"sourceEpoch\":\"" + std::to_string(record->epoch) + "\",\"sourceNodeEpoch\":\"" +
            std::to_string(record->nodeEpoch) + "\",\"records\":" + std::to_string(source.records.size()));
      }
    }
    state->sources.push_back(std::move(source));
    epoch = record->previousEpoch;
  }
  auto remember = [&](const Member& member) {
    if (member.id == settings.nodeId || member.durableProcesses == 0 || member.address.empty() || member.epoch.empty()) return;
    DurableTailTarget target{member.id, member.address, member.epoch, member.instance};
    state->addresses.try_emplace(member.id + "/" + member.epoch + "/" + member.address, std::move(target));
  };
  auto refreshMemory = [&]() {
    for (const auto& [key, member] : membership.recoveryMembers) remember(member);
    for (const auto& [id, member] : membership.members) {
      // The explicit local entry below already covers this process's held epochs.
      if (id != settings.nodeId) remember(member);
    }
    state->addresses[settings.nodeId + "/local"] =
        {settings.nodeId, "local", std::to_string(nodeEpoch), settings.instance};
  };
  refreshMemory();
  auto allOwners = [&]() {
    return std::all_of(state->sources.begin(), state->sources.end(),
        [](const Source& source) { return source.ownerComplete; });
  };
  auto checkIdentity = [&]() {
    auto* current = objectOf(object.objectName);
    KJ_REQUIRE(current != nullptr && current->state == ObjectState::REBUILDING &&
        current->shardEpoch == object.shardEpoch && current->dataEpoch == object.dataEpoch,
        "the object changed ownership during tail recovery");
  };
  // A page has no size preface in this protocol. Budget the largest legal page
  // at an aggregate 8 MiB/s shared by all recovery slots, plus connection time.
  // It is never shortened by the 25-second missing-holder wait.
  const auto slots = std::max<uint32_t>(1, settings.config.recoverParallelism);
  const auto pageBudget = uint32_t(std::max<double>(settings.config.confirmTimeoutMs,
      settings.peerTimeoutMs + std::ceil(double(tailRecordBytes() + TAIL_RPC_HEADROOM_BYTES) * slots * 1000 / (8u << 20))));
  const auto drainMs = std::ceil(double(std::max<uint64_t>(settings.config.peerBufferBytes,
      tailRecordBytes())) * slots * 1000 / (8u << 20));
  // A source query exhausts all pages while holding one bounded network slot.
  // Local reads do not need a slot. No task may outlive the forked batch below.
  auto query = [&](size_t index, std::string key, bool owner) -> kj::Promise<void> {
    auto& source = state->sources[index];
    auto& probe = source.probes.at(key);
    if (probe.complete || probe.gone) co_return;
    bool active = false;
    const auto release = kj::defer([&]() {
      if (!active) return;
      --activeTailFetches;
      while (!tailFetchWaiters.empty()) {
        auto next = kj::mv(tailFetchWaiters.front());
        tailFetchWaiters.pop_front();
        if (!next.ready->isWaiting()) continue;
        ++activeTailFetches;
        // Cancellation may destroy the waiter after fulfillment but before it
        // resumes. Its defer must already own the reserved slot in that window.
        *next.acquired = true;
        next.ready->fulfill();
        break;
      }
    });
    try {
      if (probe.target.nodeId != settings.nodeId) {
        if (activeTailFetches >= slots) {
          auto available = kj::newPromiseAndFulfiller<void>();
          tailFetchWaiters.push_back({kj::mv(available.fulfiller), &active});
          co_await kj::mv(available.promise);
        } else {
          ++activeTailFetches;
        }
        active = true;
      }
      // Every known holder gets an attempt, including a retry after the wait
      // deadline. One probe may drain a full configured peer buffer, with the
      // last page retaining its whole page budget. No expired-deadline shortcut.
      const auto drainUntil = std::max(deadline, monotonicTimeMs()) + drainMs;
      bool firstPage = true;
      for (;;) {
        if (!firstPage && monotonicTimeMs() >= drainUntil) co_return;
        firstPage = false;
        checkIdentity();
        DurableTailAsk ask;
        ask.namespaceId = object.namespaceId;
        ask.shard = object.shard;
        ask.shardEpoch = object.shardEpoch;
        ask.ownerNodeId = source.epoch.nodeId;
        ask.ownerNodeEpoch = source.epoch.nodeEpoch;
        ask.afterSeq = probe.after;
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
          KJ_REQUIRE(bool(hooks.fetchTailAt) || bool(hooks.fetchTail), "tail transport is not installed");
          ++tailFetches;
          auto request = hooks.fetchTailAt ? hooks.fetchTailAt(probe.target, ask)
                                          : hooks.fetchTail(probe.target.nodeId, ask);
          answer = co_await timer.timeoutAfter(pageBudget * kj::MILLISECONDS, kj::mv(request));
        }
        checkIdentity();
        if (!answer.refusal.empty()) {
          probe.error = answer.refusal;
          if (answer.refusal.find("does not hold the epoch") != std::string::npos) probe.gone = true;
          log("tail-refused", "\"object\":" + quote(object.objectName) + ",\"peer\":" +
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
              record.shard == object.shard && record.objectName.starts_with(object.namespaceId + "/"),
              "a tail page contains records outside its source");
          last = std::max(last, record.nodeSeq);
          if (record.objectName != wanted || record.shardEpoch != source.epoch.epoch) continue;
          ++mine;
          const Position position{record.shardEpoch, record.nodeSeq};
          const auto encoded = encodeLogRecord(record);
          auto [page, added] = pageBytes.emplace(position, encoded);
          KJ_REQUIRE(added || page->second == encoded, "one tail page repeats an identity with different bytes");
          auto held = source.records.find(position);
          KJ_REQUIRE(held == source.records.end() || encodeLogRecord(held->second) == encoded,
              "different bytes claim one tail record identity");
        }
        KJ_REQUIRE(!answer.more || last > probe.after, "a tail page makes no cursor progress");
        KJ_REQUIRE(answer.records.empty() || answer.lastSeq == last, "a tail page's last sequence is inconsistent");
        // Commit the page only after every record and the cursor are validated.
        for (auto& record : answer.records) {
          if (record.objectName == wanted && record.shardEpoch < snapshotShardEpoch) {
            noteLateTail(object.objectName, record, snapshotShardEpoch);
            continue;
          }
          if (record.objectName != wanted || record.shardEpoch != source.epoch.epoch) continue;
          const Position position{record.shardEpoch, record.nodeSeq};
          auto* live = objectOf(object.objectName);
          KJ_REQUIRE(live != nullptr, "recovery lost its retained tail storage");
          auto [retained, added] = live->recoveryTails.try_emplace(position, record);
          KJ_REQUIRE(added || encodeLogRecord(retained->second) == encodeLogRecord(record),
              "a retained recovery tail changed its bytes");
          source.records.try_emplace(position, std::move(record));
          if (ownerProof) source.ownerRecords.insert(position);
        }
        log(probe.target.nodeId == settings.nodeId ? "tail-local" : "tail-taken",
            "\"object\":" + quote(object.objectName) + ",\"peer\":" + quote(probe.target.nodeId) +
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
      probe.error = error.getDescription().cStr();
      log("tail-failed", "\"object\":" + quote(object.objectName) + ",\"peer\":" +
          quote(probe.target.nodeId) + ",\"source\":" + quote(source.epoch.nodeId) +
          ",\"error\":" + quote(probe.error));
    }
  };
  const auto marginMs = uint64_t(membership.grantedTtlSeconds) * 1000 +
      membership.livenessGraceMs + membership.listIntervalMs;
  auto isCandidate = [&](const DurableTailTarget& target, const Source& source) {
    if (target.durableProcesses == 0) return false;
    if (!state->discoverySucceeded || !target.timeVerified || !source.acquiredAtUnixMs ||
        !membership.grantedTtlSeconds) return true;
    auto before = [&](uint64_t at) {
      return at && at < source.acquiredAtUnixMs && source.acquiredAtUnixMs - at > marginMs;
    };
    if (target.heartbeatUnixMs) return !before(target.heartbeatUnixMs);
    // A registration alone is not a death proof. A different process must have
    // registered before the entire margin preceding this source's acquisition.
    return !(before(target.registeredUnixMs) && before(target.successorRegisteredUnixMs));
  };
  double nextDiscovery = monotonicTimeMs();
  auto discover = [&]() -> kj::Promise<void> {
    state->discoveryAttempted = true;state->discoverySucceeded = false;
    for (auto& [key, target] : state->addresses) target.timeVerified = false;
    const auto budget = std::max<uint32_t>(1, settings.peerTimeoutMs);
    try {
      auto listing = co_await timer.timeoutAfter(budget * kj::MILLISECONDS, store->list(MEMBERS_PREFIX));
      const auto listed = readMemberListing(listing);
      std::map<std::pair<std::string, uint64_t>, MemberRecord> values;
      std::map<std::pair<std::string, uint64_t>, MemberRecord> registrations;
      for (const auto& [id, node] : listed) {
        if (id == settings.nodeId) continue;
        for (const auto& [number, value] : node.epochs) {
          if (!value.registered && !value.heartbeat) continue;
          const auto key = value.heartbeat ? memberHeartbeatKey(id, number) : memberRegistrationKey(id, number);
          auto response = co_await timer.timeoutAfter(budget * kj::MILLISECONDS, store->get(key));
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
      state->discoverySucceeded = true;
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
                auto response = co_await timer.timeoutAfter(budget * kj::MILLISECONDS, store->get(registrationKey));
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
          for (auto& [addressKey, prior] : state->addresses) {
            if (prior.nodeId != id || prior.destinationEpoch != target.destinationEpoch) continue;
            prior.heartbeatUnixMs = target.heartbeatUnixMs;prior.registeredUnixMs = target.registeredUnixMs;
            prior.successorRegisteredUnixMs = target.successorRegisteredUnixMs;
            prior.durableProcesses = target.durableProcesses;prior.timeVerified = true;
          }
          const bool relevant = std::any_of(state->sources.begin(), state->sources.end(),
              [&](const Source& source) { return isCandidate(target, source); });
          if (!relevant) { state->unresolved.erase(key);continue; }
          if (record.address.empty()) { state->unresolved.insert(key);continue; }
          state->addresses[id + "/" + target.destinationEpoch + "/" + target.address] = std::move(target);
          state->unresolved.erase(key);
        }
      }
    } catch (const kj::Exception& error) {
      state->discoverySucceeded = false;state->unresolved.insert("member-discovery");
      log("tail-discovery-failed", "\"object\":" + quote(object.objectName) +
          ",\"error\":" + quote(std::string(error.getDescription().cStr())));
    }
    nextDiscovery = monotonicTimeMs() + 1000;
  };
  for (;;) {
    checkIdentity();refreshMemory();
    kj::Vector<kj::Promise<void>> ownerCalls;
    kj::Vector<kj::Promise<void>> peerCalls;
    for (size_t i = 0; i < state->sources.size(); ++i) {
      auto& source = state->sources[i];
      if (source.ownerComplete) continue;
      std::optional<DurableTailTarget> ownerTarget;
      int best = -1;
      for (const auto& [key, target] : state->addresses) {
        if (target.nodeId != source.epoch.nodeId) continue;
        int score = target.instance == source.epoch.instance ? 2 : 0;
        if (target.destinationEpoch == std::to_string(source.epoch.nodeEpoch)) score += 4;
        if (target.nodeId == settings.nodeId && source.epoch.instance == settings.instance) score += 8;
        if (score > best) { ownerTarget = target;best = score; }
      }
      if (ownerTarget) {
        ownerTarget->destinationEpoch = std::to_string(source.epoch.nodeEpoch);
        const auto key = "owner/" + ownerTarget->address;
        auto [entry, added] = source.probes.try_emplace(key);
        if (added) entry->second.target = *ownerTarget;
        ownerCalls.add(query(i, key, true));
      }
      if (source.ownerComplete || !state->discoveryAttempted) continue;
      for (const auto& [addressKey, target] : state->addresses) {
        if (target.nodeId == source.epoch.nodeId) continue;
        const auto key = "peer/" + addressKey;
        auto [entry, added] = source.probes.try_emplace(key);
        if (added) entry->second.target = target;
        const bool irrelevant = !isCandidate(target, source);
        if (irrelevant && !entry->second.irrelevant) log("tail-candidate-before-window",
            "\"object\":" + quote(object.objectName) + ",\"peer\":" + quote(target.nodeId) +
            ",\"nodeEpoch\":\"" + target.destinationEpoch + "\",\"sourceEpoch\":\"" +
            std::to_string(source.epoch.epoch) + "\"");
        entry->second.irrelevant = irrelevant;
        if (!irrelevant) peerCalls.add(query(i, key, false));
      }
    }
    // An original owner's full proof takes the fast exit without discovery.
    auto owners = kj::joinPromises(ownerCalls.releaseAsArray()).then([&]() { return allOwners(); }).fork();
    auto peersDone = kj::joinPromises(peerCalls.releaseAsArray()).fork();
    const auto ready = co_await owners.addBranch().exclusiveJoin(peersDone.addBranch().then([]() { return false; }));
    if (ready || allOwners()) break;
    co_await owners.addBranch();if (allOwners()) break;
    co_await peersDone.addBranch();
    if (state->discoveryAttempted && monotonicTimeMs() >= deadline) break;
    if (!state->discoveryAttempted || monotonicTimeMs() >= nextDiscovery) {
      co_await discover();checkIdentity();continue;
    }
    co_await timer.afterDelay(uint32_t(std::max(1.0, std::min(200.0, deadline - monotonicTimeMs()))) * kj::MILLISECONDS);
  }
  checkIdentity();
  TailRecovery out;
  out.collectionMs = monotonicTimeMs() - startedMs;
  out.cut = !allOwners();
  bool othersMissing = membership.recoveryHistoryIncomplete || !state->unresolved.empty();
  std::string missing = "[";
  auto nameMissing = [&](const std::string& text) {
    if (missing.size() > 1) missing += ",";
    missing += quote(text);
  };
  for (auto& source : state->sources) {
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
    for (auto& [position, record] : source.records) {
      if (source.ownerComplete && !source.ownerRecords.contains(position)) continue;
      out.records.push_back(std::move(record));
    }
  }
  for (const auto& key : state->unresolved) nameMissing(key);
  if (membership.recoveryHistoryIncomplete) nameMissing("evicted-member-address-history");
  missing += "]";
  if (out.cut) {
    if (out.sourceLogUnavailable) ++ownerLogUnavailableRecoveries;
    out.ownerSilent = !othersMissing;
    if (out.ownerSilent) ++ownerSilentRecoveries;
    else ++tailRecoveryBounded;
    log(out.ownerSilent ? "owner-silent" : "tail-recovery-bounded",
        "\"object\":" + quote(object.objectName) + ",\"namespaceId\":" + quote(object.namespaceId) +
        ",\"shard\":" + std::to_string(object.shard) + ",\"shardEpoch\":\"" +
        std::to_string(object.shardEpoch) + "\",\"missing\":" + missing +
        ",\"ownerLogUnavailable\":" + (out.sourceLogUnavailable ? "true" : "false") +
        ",\"waitedMs\":" + std::to_string(out.collectionMs) + ",\"records\":" +
        std::to_string(out.records.size()));
  }
  co_return out;
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
kj::Promise<void> DurableLog::forceRecoverySnapshot(std::string objectName, bool healing, bool local) {
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
  const auto storageId = object->storageId;
  const auto selected = object->facets;
  object->snapshotting = true;
  const auto release = kj::defer([this, objectName]() {
    auto* entry = objectOf(objectName);
    if (entry != nullptr) entry->snapshotting = false;
  });
  auto check = [&]() {
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
    if (result.created) co_return true;
    auto back = co_await store->get(key);
    requireStorage(back.status == 200, "an existing recovery snapshot object could not be verified");
    co_return back.bytes == bytes;
  };
  // Orphaned parts from interrupted attempts also reserve their sequence.
  const auto prefix = objectPrefix(namespaceId, objectId) + "snapshot/";
  auto listing = co_await store->list(prefix);
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
      const auto path = databasePath(storageId, facetId);
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
        if (healing) co_await timer.afterDelay(
            uint32_t((bytes.size() * 1000 + (64u << 20) - 1) / (64u << 20)) * kj::MILLISECONDS);
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
kj::Promise<void> DurableLog::materializeTrustedWal(std::string objectName, uint64_t shardEpoch) {
  const auto epoch = nodeEpoch;
  auto check = [&]() -> Object& {
    auto* object = objectOf(objectName);
    KJ_REQUIRE(object != nullptr && object->state == ObjectState::REBUILDING &&
        object->shardEpoch == shardEpoch && nodeEpoch == epoch, "ownership changed while materializing trusted files");
    return *object;
  };
  const auto storageId = check().storageId;
  const auto facets = check().facets;
  for (const auto& [facetId, facet] : facets) {
    if (facet.deleted) continue;
    KJ_REQUIRE(!facet.open, "healing requires a local facet not yet opened by workerd");
    const auto database = databasePath(storageId, facetId);
    const int databaseFd = ::open(database.c_str(), O_RDONLY);
    KJ_REQUIRE(databaseFd >= 0, "a trusted healing facet is unavailable", database.string().c_str());
    ::close(databaseFd);
    const auto wal = databasePath(storageId, facetId, "-wal");
    const int fd = ::open(wal.c_str(), O_RDONLY);
    if (fd < 0) {
      KJ_REQUIRE(errno == ENOENT, "a trusted healing WAL could not be opened", wal.string().c_str());
      KJ_REQUIRE(!facet.lastFrame || facet.loggedCheckpointCovered || check().healingInProgress,
          "the missing healing WAL has no checkpoint covering its logged generation");
    } else {
      const auto close = kj::defer([fd]() { ::close(fd); });
      struct stat status;
      KJ_REQUIRE(::fstat(fd, &status) == 0 && status.st_size >= 0, "a healing WAL could not be inspected");
      KJ_REQUIRE(status.st_size > 32 || !facet.lastFrame || facet.loggedCheckpointCovered || check().healingInProgress,
          "the empty healing WAL has no checkpoint covering its logged generation");
      KJ_REQUIRE(!facet.lastFrame || status.st_size == 0 || status.st_size >= 32,
          "a trusted healing WAL header is truncated");
      if (facet.lastFrame && status.st_size > 32) {
        auto readAt = [&](void* target, size_t bytes, uint64_t offset) {
          size_t done = 0;
          while (done < bytes) {
            const auto got = ::pread(fd, static_cast<char*>(target) + done, bytes - done, off_t(offset + done));
            if (got < 0 && errno == EINTR) continue;
            KJ_REQUIRE(got > 0, "a trusted healing WAL is truncated", wal.string().c_str());
            done += size_t(got);
          }
        };
        std::string header(32, '\0');
        readAt(header.data(), header.size(), 0);
        const auto* raw = reinterpret_cast<const unsigned char*>(header.data());
        const auto pageSize = beU32(raw + 8), salt1 = beU32(raw + 16), salt2 = beU32(raw + 20);
        KJ_REQUIRE(pageSize >= 512 && pageSize <= 65536 && (pageSize & (pageSize - 1)) == 0 &&
            salt1 == facet.salt1 && salt2 == facet.salt2, "the trusted WAL generation does not match its position");
        const uint64_t frameSize = 24 + uint64_t(pageSize);
        KJ_REQUIRE(uint64_t(status.st_size) >= 32 + frameSize * facet.lastFrame,
            "the trusted WAL is shorter than its known position");
        // Validate every checksum before the first database page is changed.
        // Both passes stream at most 8 MiB, using the production replay checker.
        for (const bool verifyOnly : {true, false}) {
          DatabaseReplay replay(database, false, verifyOnly);
          uint32_t first = 1;
          while (first <= facet.lastFrame) {
            const auto count = uint32_t(std::min<uint64_t>(facet.lastFrame - first + 1,
                std::max<uint64_t>(1, (8u << 20) / frameSize)));
            LogRecord record;
            record.kind = LogRecordKind::FRAMES;
            record.header = first == 1 ? header : std::string();
            record.pageSize = pageSize;
            record.salt1 = salt1; record.salt2 = salt2;
            record.firstFrame = first; record.frames = count;
            record.frameBytes.resize(size_t(count * frameSize));
            readAt(record.frameBytes.data(), record.frameBytes.size(), 32 + uint64_t(first - 1) * frameSize);
            if (first + count - 1 == facet.lastFrame) {
              const auto* last = reinterpret_cast<const unsigned char*>(record.frameBytes.data()) + (count - 1) * frameSize;
              KJ_REQUIRE(beU32(last + 4) != 0, "the trusted WAL position is not a committed transaction");
            }
            std::string error;
            KJ_REQUIRE(replay.apply(record, &error), "the trusted WAL failed healing validation", error.c_str());
            first += count;
            co_await timer.afterDelay(1 * kj::MILLISECONDS);
            check();
          }
          replay.finish();
        }
        healedWalFrames += facet.lastFrame;
        log("heal-wal-materialized", "\"object\":" + quote(objectName) + ",\"facet\":" +
            std::to_string(facetId) + ",\"frames\":" + std::to_string(facet.lastFrame));
      }
    }
    // The healing marker covers the entire object. Synchronize every live
    // facet while every WAL still exists before making that marker durable.
    const int synchronizedDb = ::open(database.c_str(), O_RDWR | O_CLOEXEC);
    KJ_REQUIRE(synchronizedDb >= 0, "the materialized database could not be synchronized");
    const bool synchronized = ::fsync(synchronizedDb) == 0;
    ::close(synchronizedDb);
    KJ_REQUIRE(synchronized, "the materialized database synchronization failed");
  }
  if (std::any_of(facets.begin(), facets.end(), [](const auto& entry) { return !entry.second.deleted; })) {
    auto& healing = check();
    if (!healing.rebuildGeneration.first) healing.rebuildGeneration = {nodeEpoch, ++recoverySerial};
    KJ_REQUIRE(appendRecovery({recoveryEvent(RecoveryKind::HEAL_BEGIN, healing, healing.rebuildGeneration)}, true),
        "the healing-in-progress marker could not be made durable");
    healing.healingInProgress = true;
  }
  for (const auto& [facetId, facet] : facets) {
    if (facet.deleted) continue;
    const auto database = databasePath(storageId, facetId);
    const auto wal = databasePath(storageId, facetId, "-wal");
    std::error_code code;
    std::filesystem::remove(wal, code);
    KJ_REQUIRE(!code, "the materialized healing WAL could not be removed", wal.string().c_str());
    std::filesystem::remove(databasePath(storageId, facetId, "-shm"), code);
    KJ_REQUIRE(!code, "the healing facet's shared memory could not be removed");
    // Healing first reclaims the shard at this new process's epoch. This object
    // has served no event there, so its snapshot watermark at that epoch is zero.
    auto& updated = check().facets.at(facetId);
    updated.lastFrame = 0; updated.salt1 = 0; updated.salt2 = 0;
    updated.lastSeq = 0; updated.checkpointedSeq = 0; updated.generation = 0;
    const auto bytes = std::filesystem::file_size(database, code);
    KJ_REQUIRE(!code, "the materialized healing database is unavailable");
    updated.databaseBytes = bytes;
  }
  sized(check());
  co_return;
}

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
    // At most one background check may reach the snapshot gate. Requested
    // objects check their risk independently and have priority at that gate.
    if (healBusy || requestedHealWaiters) continue;
    for (auto& [name, object] : objects) {
      if (!object.healPending || object.state != ObjectState::LOCAL ||
          monotonicTimeMs() < object.healRetryAfterMs || rebuilding.contains(name)) continue;
      if (object.dataEpoch == 1) { object.healPending = false; object.healError = false; continue; }
      auto canceler = std::make_unique<kj::Canceler>();
      auto [entry, added] = rebuilding.emplace(name, canceler->wrap(healObject(name)).fork());
      rebuildCancelers[name] = std::move(canceler);
      tasks.add(entry->second.addBranch().catch_([](kj::Exception&&) {}));
      break;
    }
  }
}

kj::Promise<void> DurableLog::healObject(std::string objectName) {
  const auto settled = kj::defer([this, objectName]() { settledRebuilds.insert(objectName); });
  auto* object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr && object->healPending && object->state == ObjectState::LOCAL,
      "healing requires a trusted local cache");
  std::string step = "manifest";
  bool acquired = false, requestedWaiting = false;
  const auto release = kj::defer([&]() {
    if (requestedWaiting) --requestedHealWaiters;
    if (acquired) healBusy = false;
  });
  const auto namespaceId = object->namespaceId, objectId = object->objectId;
  const auto shard = object->shard;
  const auto dataEpoch = object->dataEpoch;
  const auto manifestKey = std::make_pair(namespaceId, objectId);
  auto& gate = manifestGates[manifestKey];
  ++gate.healers;
  const auto unpin = kj::defer([&]() {
    if (!--gate.healers && !gate.writers) manifestGates.erase(manifestKey);
  });
  try {
    // An older epoch can finish an ordinary manifest write while this cache
    // reclaims its shard. Wait before choosing the revision; retain the usual
    // CAS checks against all other nodes throughout the snapshot upload.
    if (gate.writers) {
      auto drained = kj::newPromiseAndFulfiller<void>();
      std::erase_if(gate.drained, [](auto& waiter) { return !waiter->isWaiting(); });
      gate.drained.push_back(kj::mv(drained.fulfiller));
      co_await drained.promise;
    }
    step = "cache-admission";
    if (object->cacheAdmissionPending) co_await admitLegacyCache(objectName);
    object = objectOf(objectName);
    KJ_REQUIRE(object != nullptr && object->state == ObjectState::LOCAL && object->healPending &&
        !object->cacheInvalid && !object->cacheHold, "the legacy cache was not admitted for healing");
    step = "manifest";
    // Risk checks never hold the snapshot gate, so a large risky object does
    // not serialize first opens of unrelated trusted files.
    auto manifest = co_await readManifest(namespaceId, objectId);
    object = objectOf(objectName);
    KJ_REQUIRE(object != nullptr && object->state == ObjectState::LOCAL && object->healPending,
        "healing lost the trusted local object");
    object->recoveryManifestRevision = manifest ? manifest->revision : 0;
    const auto cut = manifest && manifest->hasSnapshot ? manifest->snapshotShardEpoch : 0;
    // dataEpoch is the first acquisition in this uninterrupted run of the
    // same node's ownership, including a takeover that never reached a manifest.
    // An own restart/seal increases shardEpoch but never dataEpoch.
    const bool risk = !object->recoveryPublished && object->dataEpoch > 1 && object->dataEpoch > cut;
    if (!risk) {
      if (object->healingInProgress) {
        KJ_REQUIRE(appendRecovery({recoveryEvent(RecoveryKind::HEAL_DONE, *object, object->rebuildGeneration)}, false),
            "the completed healing marker could not be recorded");
        object->healingInProgress = false;object->recoveryPublished = true;
      }
      object->healPending = false; object->healError = false;
      object->recoveryManifestRevision.reset();
      writePosition(*object);
      co_return;
    }
    object->state = ObjectState::REBUILDING;
    step = "wait";
    for (;;) {
      object = objectOf(objectName);
      KJ_REQUIRE(object != nullptr && object->state == ObjectState::REBUILDING && object->healPending,
          "the healing object changed while waiting");
      if (object->healRequested && !requestedWaiting) { ++requestedHealWaiters; requestedWaiting = true; }
      if (!healBusy && (requestedWaiting || requestedHealWaiters == 0)) break;
      co_await timer.afterDelay(10 * kj::MILLISECONDS);
    }
    if (requestedWaiting) { --requestedHealWaiters; requestedWaiting = false; }
    healBusy = true; acquired = true;
    step = "ownership";
    const auto prefix = shardEpochsPrefix(namespaceId, shard);
    auto listing = co_await store->list(prefix + "/");
    const auto newest = latestRevision(listing, prefix);
    requireStorage(newest > 0, "the trusted object's shard authority is unavailable");
    auto response = co_await store->get(shardEpochKey(namespaceId, shard, newest));
    auto owner = response.status == 200 ? parseShardEpoch(response.bytes) : std::nullopt;
    requireStorage(owner && owner->epoch == newest && owner->namespaceId == namespaceId && owner->shard == shard,
        "the trusted object's shard authority is invalid");
    object = objectOf(objectName);
    KJ_REQUIRE(object != nullptr && object->state == ObjectState::REBUILDING, "healing lost its ownership check");
    if (owner->nodeId != settings.nodeId || owner->dataEpoch != dataEpoch) {
      object->healPending = false; object->state = ObjectState::GONE;
      log("heal-snapshot-skipped", "\"object\":" + quote(objectName) + ",\"reason\":\"the trusted cache no longer owns this history\"");
      co_return;
    }
    KJ_REQUIRE(bool(healOwnership), "healing ownership hook is unavailable");
    if (!membership.epoch.empty()) setNodeEpoch(decimal(membership.epoch));
    auto epoch = co_await healOwnership(ObjectIdentity{namespaceId, objectId, object->tenant,
        dataEpoch, owner->epoch, object->shard});
    object = objectOf(objectName);
    KJ_REQUIRE(epoch.has_value() && object != nullptr && object->state == ObjectState::REBUILDING,
        "the trusted cache could not reclaim its own shard for healing");
    object->shardEpoch = *epoch;
    const auto targetEpoch = *epoch;
    step = "local-wal";
    co_await materializeTrustedWal(objectName, targetEpoch);
    step = "snapshot";
    co_await forceRecoverySnapshot(objectName, true);
    object = objectOf(objectName);
    KJ_REQUIRE(object != nullptr && object->state == ObjectState::REBUILDING && object->shardEpoch == targetEpoch,
        "ownership changed while publishing a healing snapshot");
    KJ_REQUIRE(appendRecovery({recoveryEvent(RecoveryKind::HEAL_DONE, *object, object->rebuildGeneration)}, false),
        "the completed healing marker could not be recorded");
    object->healingInProgress = false;object->recoveryPublished = true;
    object->healPending = false; object->healError = false;
    object->healRetryAfterMs = 0; object->state = ObjectState::LOCAL;
    object->recoveryManifestRevision.reset();
    writePosition(*object);
    ++healSnapshots;
    log("heal-snapshot", "\"object\":" + quote(objectName) + ",\"shardEpoch\":\"" + std::to_string(targetEpoch) +
        "\",\"sequence\":\"" + std::to_string(object->snapshotSequence) + "\",\"source\":\"trusted-local-files\"");
  } catch (const kj::Exception& error) {
    ++healFailures;
    object = objectOf(objectName);
    if (object != nullptr && (object->state == ObjectState::REBUILDING || object->state == ObjectState::LOCAL)) {
      object->state = ObjectState::LOCAL;
      object->healError = true;
      object->healRetryAfterMs = monotonicTimeMs() + 30000;
    }
    if (object != nullptr && object->state == ObjectState::GONE) object->healPending = false;
    objectFailed(namespaceId, objectId, object ? object->shardEpoch : 0, "heal-" + step,
        std::string(error.getDescription().cStr()));
    throw;
  }
}

// The records of an object, in the order they were written -- by the shard's epoch, then the owner's epoch, then its
// sequence -- replayed into each of its databases. A gap in one owner's sequence ends that owner's part there: a
// peer holds a prefix, so what follows a gap was never answered to anybody.
void DurableLog::applyRecords(Object& object, std::vector<LogRecord> history,
    const std::vector<ManifestFacet>& facets, uint64_t snapshotShardEpoch,
    const std::set<std::pair<uint64_t, uint64_t>>& drop) {
  // A shard's epoch belongs to one owner and grows with every takeover, so (shard epoch, node sequence) is both the
  // order the transactions were written in and the identity of a record: a record that came from a segment and from
  // a peer is applied once.
  std::map<std::pair<uint64_t, uint64_t>, LogRecord> ordered;
  for (auto& record : history) {
    if (!record.ownerSealed && drop.contains({record.shardEpoch, record.nodeSeq})) continue;
    ordered.insert({{record.shardEpoch, record.nodeSeq}, std::move(record)});
  }
  std::map<uint32_t, uint64_t> covered;  // facet -> the sequence of the snapshot's shard epoch it already holds
  for (const auto& facet : facets) covered[facet.facetId] = facet.upToSeq;
  std::map<uint32_t, uint64_t> rebuiltUpTo;
  if (snapshotShardEpoch == object.shardEpoch) rebuiltUpTo = covered;
  std::map<uint32_t, std::unique_ptr<DatabaseReplay>> replays;
  uint64_t applied = 0, skipped = 0;
  for (const auto& [position, record] : ordered) {
    auto found = covered.find(record.facetId);
    const bool inSnapshot = record.shardEpoch < snapshotShardEpoch ||
        (found != covered.end() && record.shardEpoch == snapshotShardEpoch && record.nodeSeq <= found->second);
    if (inSnapshot) {
      ++skipped;
      continue;
    }
    auto& replay = replays[record.facetId];
    if (!replay) {
      // A facet the snapshot brought goes on from its file; one it did not starts from nothing.
      replay = std::make_unique<DatabaseReplay>(databasePath(object.storageId, record.facetId),
          covered.find(record.facetId) == covered.end());
    }
    std::string error;
    if (!replay->apply(record, &error)) {
      // The records of an object are a chain: one that does not follow the last means the history is not whole, and
      // an object is never served with less of it than it had. The rebuild fails and the request gets an answer it
      // can act on.
      ++replayStops;
      log("replay-stopped", "\"object\":" + quote(object.objectName) + ",\"facet\":" +
              std::to_string(record.facetId) + ",\"shardEpoch\":\"" + std::to_string(record.shardEpoch) +
              "\",\"nodeSeq\":\"" + std::to_string(record.nodeSeq) + "\",\"applied\":" + std::to_string(applied) +
              ",\"records\":" + std::to_string(ordered.size()) + ",\"reason\":" + quote(error));
      for (auto& [facetId, entry] : replays) {
        if (entry) entry->finish();
      }
      KJ_FAIL_REQUIRE("the replicated history of a Durable Object is not whole", object.objectName.c_str(),
          record.nodeSeq, error.c_str());
    }
    ++applied;
    if (record.shardEpoch == object.shardEpoch)
      rebuiltUpTo[record.facetId] = std::max(rebuiltUpTo[record.facetId], record.nodeSeq);
    auto& facet = object.facets[record.facetId];
    facet.facetId = record.facetId;
    if (!record.facetName.empty()) {
      facet.facetName = record.facetName;
      facet.parentFacetId = record.parentFacetId;
    }
    facet.txn = record.txn + (record.commits ? record.commits - 1 : 0);
  }
  for (auto& [facetId, replay] : replays) {
    if (replay) replay->finish();
  }
  for (const auto& facet : facets) {
    auto& entry = object.facets[facet.facetId];
    entry.facetId = facet.facetId;
    entry.parentFacetId = facet.parentFacetId;
    if (!facet.name.empty()) entry.facetName = facet.name;
  }
  // Every database of the object starts a fresh WAL under the next process that opens it.
  std::error_code code;
  for (auto& [facetId, facet] : object.facets) {
    std::filesystem::remove(databasePath(object.storageId, facetId, "-wal"), code);
    std::filesystem::remove(databasePath(object.storageId, facetId, "-shm"), code);
    facet.lastFrame = 0;
    facet.salt1 = 0;
    facet.salt2 = 0;
    facet.checkpointedSeq = rebuiltUpTo[facetId];
    facet.lastSeq = 0;
    facet.open = false;
  }
  log("replayed", "\"object\":" + quote(object.objectName) + ",\"records\":" + std::to_string(applied) +
          ",\"skipped\":" + std::to_string(skipped) + ",\"facets\":" + std::to_string(object.facets.size()));
}

// The facet tree index beside an object's databases: workerd writes it itself and no transaction covers it, so a
// rebuild writes the names and parents the records carry (experiment 25, D.6).
void DurableLog::writeFacetIndex(const std::string& storageId, const std::vector<ManifestFacet>& facets) {
  std::vector<ManifestFacet> entries;
  for (const auto& facet : facets) {
    if (facet.facetId == 0 || facet.name.empty()) continue;
    entries.push_back(facet);
  }
  if (entries.empty()) return;
  std::sort(entries.begin(), entries.end(), [](const ManifestFacet& a, const ManifestFacet& b) {
    return a.facetId < b.facetId;
  });
  std::string bytes;
  for (int i = 0; i < 8; i++) bytes.push_back(char((FACET_INDEX_MAGIC >> (8 * i)) & 0xff));
  uint32_t expected = 1;
  for (const auto& facet : entries) {
    // The ids are the positions of the entries: a gap would give the next facet another id than it had.
    if (facet.facetId != expected) {
      log("facet-index-gap", "\"storageId\":" + quote(storageId) + ",\"facet\":" + std::to_string(facet.facetId));
      return;
    }
    ++expected;
    bytes.push_back(char(facet.parentFacetId & 0xff));
    bytes.push_back(char((facet.parentFacetId >> 8) & 0xff));
    bytes.push_back(char(facet.name.size() & 0xff));
    bytes.push_back(char((facet.name.size() >> 8) & 0xff));
    bytes += facet.name;
  }
  const auto path = durableDirectory() / (storageId + ".facets");
  const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (fd < 0) return;
  size_t done = 0;
  while (done < bytes.size()) {
    auto written = ::write(fd, bytes.data() + done, bytes.size() - done);
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) break;
    done += size_t(written);
  }
  ::fsync(fd);
  ::close(fd);
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
    const bool had = alarms.erase(key) > 0;
    if (previous || had) {
      log("alarm-cleared", "\"object\":" + quote(object->objectName) + ",\"facet\":" + std::to_string(facetId) +
              ",\"was\":" + std::to_string(previous));
      tasks.add(writeAlarmHint(object->namespaceId, object->objectId, object->shard, 0, facetId));
    }
    return;
  }
  auto& state = alarms[key];
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
    state.retries = 0;
    state.countedRetries = 0;
    state.backoff = 0;
    state.notBeforeMs = 0;
    log("alarm-set", "\"object\":" + quote(object->objectName) + ",\"facet\":" + std::to_string(facetId) +
            ",\"atUnixMs\":" + std::to_string(atUnixMs));
    tasks.add(writeAlarmHint(object->namespaceId, object->objectId, object->shard, atUnixMs, facetId));
  }
}

// The hint: written when an object's alarm changes, removed when it is cleared. It is overwritable and carries no
// correctness -- a stale one costs one wake, a missing one costs a delay until the object is next opened.
kj::Promise<void> DurableLog::writeAlarmHint(std::string namespaceId, std::string objectId, uint32_t shard,
    uint64_t atUnixMs, uint32_t facetId) {
  if (!enabled()) co_return;
  const auto key = shardAlarmKey(namespaceId, shard, objectId);
  try {
    if (atUnixMs == 0) {
      co_await store->remove(key);
    } else {
      co_await store->put(key, "{\"atUnixMs\":" + std::to_string(atUnixMs) + ",\"facetId\":" +
              std::to_string(facetId) + ",\"nodeId\":" + quote(settings.nodeId) + ",\"writtenAtUnixMs\":" +
              std::to_string(realClockUnixMs()) + "}\n");
    }
    ++alarmHintWrites;
  } catch (const kj::Exception& error) {
    ++alarmHintFailures;
    log("alarm-hint-failed", "\"key\":" + quote(key) + ",\"error\":" +
            quote(std::string(error.getDescription().cStr())));
  }
}

// The hints of a shard this node has just taken: each becomes an alarm to fire, marked as coming from a hint. The
// first firing opens the object, which reports what its own database holds -- a hint for an alarm that already ran
// is dropped then, so an alarm is never fired twice for one scheduled time.
kj::Promise<void> DurableLog::readAlarmHints(std::string namespaceId, uint32_t shard, uint64_t shardEpoch,
    std::string tenant) {
  if (!enabled()) co_return;
  ObjectListing listing;
  try {
    listing = co_await store->list(shardAlarmPrefix(namespaceId, shard), 1000);
  } catch (const kj::Exception& error) {
    log("alarm-hints-failed", "\"namespaceId\":" + quote(namespaceId) + ",\"shard\":" + std::to_string(shard) +
            ",\"error\":" + quote(std::string(error.getDescription().cStr())));
    co_return;
  }
  uint32_t adopted = 0;
  for (const auto& listed : listing.objects) {
    const auto slash = listed.key.rfind('/');
    if (slash == std::string::npos) continue;
    const auto objectId = listed.key.substr(slash + 1);
    if (objectId.size() != 64) continue;
    uint64_t atUnixMs = 0;
    uint32_t facetId = 1;
    try {
      auto answer = co_await store->get(listed.key);
      if (answer.status != 200) continue;
      capnp::MallocMessageBuilder message;
      auto value = message.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(answer.bytes.data(), answer.bytes.size()), value);
      auto reader = value.asReader();
      atUnixMs = uint64_t(numberField(reader, "atUnixMs"));
      const auto facet = numberField(reader, "facetId");
      if (facet > 0) facetId = uint32_t(facet);
    } catch (const kj::Exception&) {
      continue;
    }
    if (!atUnixMs) continue;
    const auto key = namespaceId + "/" + objectId + "/" + std::to_string(facetId);
    auto found = alarms.find(key);
    // An alarm the objects here already reported is the truth: a hint never replaces it.
    if (found != alarms.end() && !found->second.fromHint) continue;
    auto& state = alarms[key];
    state.namespaceId = namespaceId;
    state.objectId = objectId;
    state.tenant = tenant;
    state.shard = shard;
    state.facetId = facetId;
    state.shardEpoch = shardEpoch;
    state.atUnixMs = atUnixMs;
    state.fromHint = true;
    state.notBeforeMs = 0;
    ++adopted;
  }
  alarmsAdopted += adopted;
  if (adopted) log("alarms-adopted", "\"namespaceId\":" + quote(namespaceId) + ",\"shard\":" +
          std::to_string(shard) + ",\"shardEpoch\":\"" + std::to_string(shardEpoch) + "\",\"alarms\":" +
          std::to_string(adopted));
}

kj::Promise<void> DurableLog::alarmLoop() {
  for (;;) {
    co_await timer.afterDelay(200 * kj::MILLISECONDS);
    if (!hooks.fireAlarm) continue;
    const auto now = realClockUnixMs();
    const auto mono = monotonicTimeMs();
    std::vector<std::string> due;
    for (const auto& [key, state] : alarms) {
      if (state.firing || !state.atUnixMs || state.atUnixMs > now || state.notBeforeMs > mono) continue;
      due.push_back(key);
    }
    for (auto& key : due) tasks.add(fireAlarm(key));
  }
}

kj::Promise<void> DurableLog::fireAlarm(std::string key) {
  auto found = alarms.find(key);
  if (found == alarms.end() || found->second.firing) co_return;
  found->second.firing = true;
  DurableAlarm ask{found->second.namespaceId, found->second.objectId, found->second.tenant, found->second.shard,
      found->second.facetId, found->second.dataEpoch, found->second.shardEpoch, found->second.atUnixMs,
      found->second.retries};
  const auto started = monotonicTimeMs();
  AlarmResult result;
  try {
    result = co_await hooks.fireAlarm(ask);
  } catch (const kj::Exception& error) {
    result = AlarmResult{AlarmOutcome::UNAVAILABLE, 0, std::string(error.getDescription().cStr())};
  }
  // Counted from the outcome, before the schedule is looked at again: the object's own clearing of its alarm is
  // reported as soon as the handler's transaction is confirmed, which is usually before this answer arrives.
  switch (result.outcome) {
    case AlarmOutcome::FIRED: ++alarmsFired; break;
    case AlarmOutcome::NOT_DUE: ++alarmsNotDue; break;
    case AlarmOutcome::FAILED: ++alarmFailures; break;
    case AlarmOutcome::UNAVAILABLE: break;
  }
  auto again = alarms.find(key);
  const auto gone = again == alarms.end();
  const auto fields = "\"namespaceId\":" + quote(ask.namespaceId) + ",\"objectId\":" + quote(ask.objectId.substr(0, 12)) +
      ",\"facet\":" + std::to_string(ask.facetId) + ",\"atUnixMs\":" + std::to_string(ask.atUnixMs) +
      ",\"elapsedMs\":" + std::to_string(monotonicTimeMs() - started);
  if (gone) {
    // Its schedule is gone (the object reported the clearing, or the shard left this node): the hint goes with it.
    log(result.outcome == AlarmOutcome::FIRED ? "alarm-fired" : "alarm-done", fields + ",\"scheduled\":false");
    if (result.outcome == AlarmOutcome::FIRED)
      tasks.add(writeAlarmHint(ask.namespaceId, ask.objectId, ask.shard, 0, ask.facetId));
    co_return;
  }
  auto& state = again->second;
  state.firing = false;
  switch (result.outcome) {
    case AlarmOutcome::FIRED: {
      log("alarm-fired", fields + ",\"retries\":" + std::to_string(state.retries));
      // The handler cleared the object's own alarm, and the report of that clears the schedule. A report that did
      // not come (the object went away with the answer in flight) leaves this entry behind, so it goes now; the
      // object's database is still the truth and reports again at its next open.
      if (state.atUnixMs == ask.atUnixMs) {
        alarms.erase(key);
        tasks.add(writeAlarmHint(ask.namespaceId, ask.objectId, ask.shard, 0, ask.facetId));
      }
      co_return;
    }
    case AlarmOutcome::NOT_DUE: {
      log("alarm-not-due", fields + ",\"holds\":" + std::to_string(result.atUnixMs));
      state.fromHint = false;
      if (result.atUnixMs == 0) {
        alarms.erase(key);
        tasks.add(writeAlarmHint(ask.namespaceId, ask.objectId, ask.shard, 0, ask.facetId));
      } else {
        state.atUnixMs = result.atUnixMs;
        state.retries = 0;
        state.countedRetries = 0;
        state.backoff = 0;
        state.notBeforeMs = 0;
      }
      co_return;
    }
    case AlarmOutcome::FAILED: {
      ++alarmRetries;
      ++state.retries;
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
      state.notBeforeMs = monotonicTimeMs() + double(delay);
      log("alarm-failed", fields + ",\"retries\":" + std::to_string(state.retries) + ",\"afterMs\":" +
              std::to_string(delay) + ",\"reason\":" + quote(result.reason.substr(0, 200)));
      co_return;
    }
    case AlarmOutcome::UNAVAILABLE: {
      // Nothing ran: this node may not own the shard any more, or the object could not be rebuilt. Not counted
      // against the retries; tried again shortly, and the shard's new owner has the hint either way.
      state.notBeforeMs = monotonicTimeMs() + 1000;
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
// This machine's files are a cache, and v61 threw the cache away at the first request after a restart of the daemon
// or of a durable process: the node kept "I have served this data epoch" in memory alone, so an object was rebuilt
// from its whole history (a gibibyte took 19.5 s). The position beside the files says how far they are confirmed, so
// a same-boot start can reuse a closed, admitted cache. A clean exit alone never
// authorizes reuse after a machine reboot.
std::filesystem::path DurableLog::positionPath(const std::string& storageId) const {
  return durableDirectory() / (storageId + ".position");
}

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
  const auto path = positionPath(object.storageId);
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
    if (it->second == generation) return true;
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
  KJ_REQUIRE(!object.cacheAdmissionPending, "legacy cache admission must precede handing files to workerd");
  if (!dispatched) {
    object.dispatchedChannel.clear();
    object.dispatchPending = false;
    for (const auto& [facetId, facet] : object.facets)
      if (!facet.deleted && !facet.loggedCheckpointCovered) {
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

kj::Promise<void> DurableLog::admitLegacyCache(std::string objectName) {
  auto* object = objectOf(objectName);KJ_REQUIRE(object != nullptr, "the legacy cache disappeared");
  const auto epoch = object->shardEpoch;
  auto manifest = co_await readManifest(object->namespaceId, object->objectId);
  object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr && object->state == ObjectState::LOCAL && object->shardEpoch == epoch,
      "the legacy cache changed during admission");
  if (manifest && manifest->shardEpoch > epoch) {
    object->state = ObjectState::GONE;KJ_FAIL_REQUIRE("the legacy cache was fenced by a newer manifest");
  }
  if (manifest && !manifest->isolated.empty()) {
    // readSegments ignores an old foreign isolated pair only after verifying
    // both the original-owner normal segment and that shard epoch's authority.
    auto history = co_await readSegments(*manifest, object->namespaceId + "/" + object->objectId);
    object = objectOf(objectName);
    KJ_REQUIRE(object != nullptr && object->state == ObjectState::LOCAL && object->shardEpoch == epoch,
        "the legacy cache changed while checking exclusions");
    std::set<std::pair<uint64_t, uint64_t>> verified;
    for (const auto& record : history) verified.emplace(record.shardEpoch, record.nodeSeq);
    uint64_t through = 0;
    for (const auto& [id, facet] : object->facets) through = std::max({through, facet.lastSeq, facet.checkpointedSeq, facet.loggedSeq});
    for (const auto& position : manifest->isolated) {
      const bool inImage = position.first < object->cacheShardEpoch ||
          (position.first == object->cacheShardEpoch && position.second <= through);
      if (!inImage || verified.contains(position)) continue;
      object->cacheInvalid = true;
      object->cacheHold = object->dataEpoch > 1 && !object->recoveryPublished &&
          (!manifest->hasSnapshot || manifest->snapshotShardEpoch < object->dataEpoch);
      object->healPending = false;
      markRepair(*object, "the legacy cache contains a manifest-isolated record");
      break;
    }
  }
  object->cacheAdmissionPending = false;
}

void DurableLog::maybeCloseMarker(Object& object) {
  if (object.state != ObjectState::LOCAL || recoveryWriteFailed) return;
  if (auto state = recoveryStates.find(object.objectName); state != recoveryStates.end() &&
      (state->second.repairPending || state->second.rebuildPending)) return;
  for (const auto& [facetId, facet] : object.facets)
    if (facet.open || !facet.checkpointSafe) return;
  object.cacheOpen = false;
  if (!writePosition(object)) object.cacheOpen = true;
}

void DurableLog::channelEnded(Channel& channel, const std::string& reason) {
  channel.open = false;
  std::set<std::string> affected;
  for (const auto& [key, generation] : channel.openFacets) {
    const auto split = key.rfind('/');
    if (split == std::string::npos) continue;
    auto* object = objectOf(key.substr(0, split));
    if (object == nullptr || generation != std::make_pair(object->openEpoch, object->openGeneration)) continue;
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
  }
}

void DurableLog::readPositions() {
  std::error_code code;
  uint64_t restored = 0;
  for (const auto& entry : std::filesystem::directory_iterator(durableDirectory(), code)) {
    if (entry.path().extension() != ".position") continue;
    std::string bytes;
    Object* reading = nullptr;
    try {
      bytes = readFile(entry.path());
    } catch (const kj::Exception&) {
      continue;
    }
    try {
      capnp::MallocMessageBuilder message;
      auto value = message.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(kj::arrayPtr(bytes.data(), bytes.size()), value);
      auto reader = value.asReader();
      const auto objectName = json::text(reader, "objectName");
      const auto namespaceId = json::text(reader, "namespaceId");
      const auto objectId = json::text(reader, "objectId");
      if (objectName.empty() || namespaceId.size() != 64 || objectId.size() != 64) continue;
      auto& object = objects[objectName];
      reading = &object;
      object.positionRead = false;
      // Logged positions take precedence over runtime WAL positions. Keep both
      // until checkpoint coverage and the open marker have been checked.
      object.objectName = objectName;
      object.namespaceId = namespaceId;
      object.objectId = objectId;
      object.tenant = json::text(reader, "tenant");
      object.dataEpoch = decimal(json::text(reader, "dataEpoch"));
      object.shardEpoch = std::max(object.shardEpoch, decimal(json::text(reader, "shardEpoch")));
      object.cacheShardEpoch = decimal(json::text(reader, "shardEpoch"));
      object.shard = uint32_t(numberField(reader, "shard"));
      object.storageId = durableHostId(settings.hostUniqueKey, objectName);
      object.snapshotSequence = decimal(json::text(reader, "snapshotSequence"));
      object.snapshotAtUnixMs = uint64_t(numberField(reader, "snapshotAtUnixMs"));
      object.sinceSnapshotBytes = decimal(json::text(reader, "sinceSnapshotBytes"));
      object.bytes = decimal(json::text(reader, "bytes"));
      const auto positionEpoch = json::integer(reader, "positionNodeEpoch");
      if (positionEpoch) object.positionNodeEpoch = positionEpoch;
      const auto source = recoveryEpochs.find(object.positionNodeEpoch);
      const bool modern = source != recoveryEpochs.end() && source->second.modern;
      if (modern) {
        KJ_REQUIRE(json::field(reader, "cacheOpen").isBoolean() &&
            json::field(reader, "openEpoch").isString() && json::field(reader, "openGeneration").isString(),
            "a modern cache position is missing its open-generation fields");
      }
      object.cacheOpen = json::boolean(reader, "cacheOpen");
      object.openEpoch = json::integer(reader, "openEpoch");
      object.openGeneration = json::integer(reader, "openGeneration");
      object.imageRepairGeneration = {json::integer(reader, "imageRepairEpoch"), json::integer(reader, "imageRepairSerial")};
      object.recoveryPublished = json::boolean(reader, "recoveryPublished");
      object.state = ObjectState::LOCAL;
      object.healPending = true;
      auto facetList = json::field(reader, "facets");
      KJ_REQUIRE(facetList.isArray(), "the cache position has no facet list");
      for (auto facet : facetList.getArray()) {
        const auto facetId = uint32_t(numberField(facet, "facetId"));
        auto& held = object.facets[facetId];
        const bool seen = held.loggedNodeEpoch != 0;
        held.facetId = facetId;
        held.parentFacetId = uint32_t(numberField(facet, "parentFacetId"));
        held.facetName = json::text(facet, "facetName");
        held.databaseBytes = decimal(json::text(facet, "databaseBytes"));
        held.deleted = held.deleted || json::boolean(facet, "deleted");
        held.checkpointedSeq = decimal(json::text(facet, "checkpointedSeq"));
        held.checkpointGeneration = json::integer(facet, "checkpointGeneration");
        held.checkpointFrames = uint32_t(numberField(facet, "checkpointFrames"));
        held.checkpointSalt1 = uint32_t(numberField(facet, "checkpointSalt1"));
        held.checkpointSalt2 = uint32_t(numberField(facet, "checkpointSalt2"));
        const auto positionGeneration = decimal(json::text(facet, "generation"));
        const auto positionFrame = uint32_t(numberField(facet, "lastFrame"));
        held.loggedCheckpointCovered = positionFrame == 0 && (!seen || (modern
              ? held.checkpointGeneration == held.loggedGeneration && held.checkpointSalt1 == held.loggedSalt1 &&
                  held.checkpointSalt2 == held.loggedSalt2 && held.checkpointFrames >= held.loggedFrame
              : positionGeneration == held.loggedGeneration && held.checkpointedSeq >= held.loggedSeq));
        if (seen) continue;
        held.txn = decimal(json::text(facet, "txn"));
        held.generation = decimal(json::text(facet, "generation"));
        held.lastSeq = positionEpoch == nodeEpoch ? decimal(json::text(facet, "lastSeq")) : 0;
        held.checkpointedSeq = decimal(json::text(facet, "checkpointedSeq"));
        held.lastFrame = uint32_t(numberField(facet, "lastFrame"));
        held.salt1 = uint32_t(numberField(facet, "salt1"));
        held.salt2 = uint32_t(numberField(facet, "salt2"));
        // A closed-epoch summary deliberately omits individual WAL positions.
        // Preserve the cache's position for checking a surviving WAL before
        // truncation; never interpret missing logged metadata as zero frames.
        if (positionFrame && !held.deleted) {
          const int fd = ::open(databasePath(object.storageId, facetId, "-wal").c_str(), O_RDONLY | O_CLOEXEC);
          unsigned char header[32];ssize_t n = -1;
          if (fd >= 0) {
            do { n = ::pread(fd, header, sizeof(header), 0); } while (n < 0 && errno == EINTR);
            ::close(fd);
          }
          const uint32_t page = n == sizeof(header) ? beU32(header + 8) : 0;
          if (page < 512 || page > 65536 || (page & (page - 1)) || beU32(header + 16) != held.salt1 ||
              beU32(header + 20) != held.salt2) { object.cacheInvalid = true;continue; }
          held.loggedGeneration = held.generation;held.loggedSalt1 = held.salt1;held.loggedSalt2 = held.salt2;
          held.loggedFrame = positionFrame;held.loggedPageSize = page;
        }
      }
      object.positionRead = true;
      ++restored;
    } catch (const kj::Exception&) {
      if (reading != nullptr) {
        reading->positionRead = false;
        reading->cacheInvalid = true;
      }
      continue;
    }
  }
  admitPositions();
  positionsReused += restored;
  if (restored) log("positions-reused", "\"objects\":" + std::to_string(restored));
}

void DurableLog::admitPositions() {
  std::error_code code;
  auto mayExist = [](const std::filesystem::path& path) {
    std::error_code error;const bool exists = std::filesystem::exists(path, error);
    return exists || bool(error);
  };
  auto readable = [&](const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return false;
    struct stat status;bool ok = ::fstat(fd, &status) == 0 && S_ISREG(status.st_mode);
    if (ok) {
      char byte;ssize_t read;
      do { read = ::pread(fd, &byte, 1, 0); } while (read < 0 && errno == EINTR);
      ok = read >= 0;
    }
    ::close(fd);return ok;
  };
  for (auto& [name, object] : objects) {
    std::string invalid;
    if (object.cacheInvalid) invalid = "the cache position could not be read completely";
    if (recoveryReadIncomplete) invalid = "a predecessor log or recovery sidecar has an incomplete prefix";
    auto state = recoveryStates.find(name);
    if (state != recoveryStates.end()) {
      object.recoveryPublished = object.recoveryPublished || state->second.published;
      object.healingInProgress = state->second.healing;
      if (state->second.repairPending) { invalid = "a persisted repair has not completed";object.repairGeneration = state->second.repair; }
      if (state->second.rebuildPending) invalid = "a persisted rebuild has not completed";
      if (!state->second.exclusions.empty() && object.imageRepairGeneration != state->second.repaired)
        invalid = "the cache position predates its persisted exclusions";
      for (const auto& [position, generation] : state->second.exclusions)
        if (std::find(object.isolatedRecords.begin(), object.isolatedRecords.end(), position) == object.isolatedRecords.end())
          object.isolatedRecords.push_back(position);
    }
    if (object.cacheOpen) invalid = "the predecessor left this object open in a durable process";
    if (auto epoch = recoveryEpochs.find(object.positionNodeEpoch); epoch != recoveryEpochs.end() &&
        epoch->second.modern && (!object.positionRead || !epoch->second.complete))
      invalid = "a modern log has no readable complete cache position or sidecar";
    bool missing = false, named = false, localCopyPresent = false;
    for (const auto& [facetId, facet] : object.facets) {
      if (facet.deleted) continue;
      localCopyPresent = localCopyPresent || mayExist(databasePath(object.storageId, facetId));
      named = named || !facet.facetName.empty();
      if (!readable(databasePath(object.storageId, facetId))) missing = true;
      if (facet.loggedFrame && !facet.loggedCheckpointCovered && !object.healingInProgress) {
        const auto wal = databasePath(object.storageId, facetId, "-wal");
        const auto bytes = std::filesystem::file_size(wal, code);
        if (code || bytes < 32 + uint64_t(facet.loggedFrame) * (24 + uint64_t(facet.loggedPageSize)))
          invalid = "the WAL does not cover the logged position or a recorded checkpoint";
      }
    }
    if (named && !readable(durableDirectory() / (object.storageId + ".facets"))) missing = true;
    if (object.facets.empty())
      for (uint32_t facetId = 0; facetId < 64 && !localCopyPresent; ++facetId)
        localCopyPresent = mayExist(databasePath(object.storageId, facetId));
    if (missing) invalid = "a required database or named-facet index is missing or unreadable";
    // A durable healing marker is written only after the database is fsynced,
    // before WAL removal; it intentionally survives the old open position.
    const bool resumableHealing = object.healingInProgress && !missing && !recoveryReadIncomplete &&
        state != recoveryStates.end() && !state->second.repairPending && !state->second.rebuildPending;
    if (resumableHealing) { invalid.clear();object.cacheInvalid = false; }
    object.healPending = !object.recoveryPublished && object.dataEpoch > 1;
    const auto source = recoveryEpochs.find(object.positionNodeEpoch);
    object.cacheAdmissionPending = source == recoveryEpochs.end() || !source->second.modern;
    if (object.healingInProgress) object.healPending = true;
    if (!invalid.empty()) {
      object.cacheInvalid = true;
      object.cacheHold = localCopyPresent && object.dataEpoch > 1 && !object.recoveryPublished && !resumableHealing;
      object.healPending = false;
      markRepair(object, invalid);
      log(object.cacheHold ? "cache-hold" : "cache-rejected", "\"object\":" + quote(name) + ",\"reason\":" + quote(invalid));
    }
  }
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
  std::map<std::string, uint64_t> mine;
  for (const auto& [name, object] : objects) {
    if (object.tenant.empty() || object.state == ObjectState::GONE) continue;
    if (!namespaceCounts(object.namespaceId)) continue;
    mine[object.tenant] += object.bytes;
  }
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
    if (it->second.namespaceId == namespaceId && it->second.shard == shard && !it->second.firing) {
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
      result.accepted = buffer.acceptedSeq;
      result.refusal = tooLarge ? "a fragmented record exceeds the recoverable RPC limit"
          : "full: this node buffers durableObjects.peerBufferBytes for the owners it is a peer of";
      return result;
    }
    buffer.fragment += std::string(bytes);
    buffer.bytes += bytes.size();
    incomingBytes += bytes.size();
    result.accepted = buffer.acceptedSeq;
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
  if (incomingBytes + bytes.size() > settings.config.peerBufferBytes) {
    ++bufferRefusals;
    ++buffer.refusedRecords;
    result.accepted = buffer.acceptedSeq;
    result.refusal = "full: this node buffers durableObjects.peerBufferBytes for the owners it is a peer of";
    return result;
  }
  std::string error;
  auto records = splitRecords(bytes, &error);
  if (!error.empty()) {
    result.accepted = buffer.acceptedSeq;
    result.refusal = "the records are cut short: " + error;
    return result;
  }
  for (const auto& encoded : records) {
    if (encoded.size() > tailRecordBytes()) {
      ++recordLimitRefusals;
      result.accepted = buffer.acceptedSeq;
      result.refusal = "a record exceeds the recoverable RPC limit";
      return result;
    }
    std::string reason;
    auto record = decodeLogRecord(encoded, &reason);
    if (!record) {
      // A record that does not add up is refused here rather than at a rebuild (experiment 25, B.7).
      result.accepted = buffer.acceptedSeq;
      result.refusal = "a record does not add up: " + reason;
      log("record-refused", "\"owner\":" + quote(buffer.nodeId) + ",\"reason\":" + quote(reason));
      return result;
    }
    if (record->nodeEpoch != buffer.nodeEpoch) {
      result.accepted = buffer.acceptedSeq;
      result.refusal = "a record of another epoch of the owner";
      return result;
    }
    if (record->kind != LogRecordKind::SEAL && hooks.knownShardEpoch) {
      const auto known = hooks.knownShardEpoch(record->objectName.substr(0, record->objectName.find('/')),
          record->shard);
      if (known != 0 && record->shardEpoch < known) {
        ++fencedRecords;
        result.accepted = buffer.acceptedSeq;
        result.refusal = "fenced: this node knows a newer epoch of the shard";
        log("record-fenced", "\"owner\":" + quote(buffer.nodeId) + ",\"shard\":" + std::to_string(record->shard) +
                ",\"theirs\":\"" + std::to_string(record->shardEpoch) + "\",\"known\":\"" + std::to_string(known) + "\"");
        return result;
      }
    }
    if (record->nodeSeq <= buffer.droppedTo || buffer.isolated.contains(record->nodeSeq)) continue;
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
      buffer.positions.emplace(sequence, std::move(*record));
    }
    buffer.acceptedSeq = std::max(buffer.acceptedSeq, sequence);
  }
  buffer.lastAppendMs = monotonicTimeMs();
  result.accepted = buffer.acceptedSeq;
  return result;
}

void DurableLog::trimIncoming(uint64_t streamId, uint64_t upTo) {
  auto found = incoming.find(streamId);
  if (found == incoming.end()) return;
  auto& buffer = found->second;
  for (auto entry = buffer.held.begin(); entry != buffer.held.end();) {
    if (entry->first > upTo) break;
    incomingBytes -= std::min<uint64_t>(incomingBytes, entry->second.size());
    buffer.bytes -= std::min<uint64_t>(buffer.bytes, entry->second.size());
    entry = buffer.held.erase(entry);
  }
  buffer.positions.erase(buffer.positions.begin(), buffer.positions.upper_bound(upTo));
  buffer.droppedTo = std::max(buffer.droppedTo, upTo);
  buffer.isolated.erase(buffer.isolated.begin(), buffer.isolated.upper_bound(buffer.droppedTo));
}

void DurableLog::dropIncoming(uint64_t streamId, const std::vector<uint64_t>& sequences) {
  auto found = incoming.find(streamId);
  if (found == incoming.end()) return;
  auto& buffer = found->second;
  for (auto sequence : sequences) {
    buffer.isolated.insert(sequence);
    buffer.positions.erase(sequence);
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
  DurableTailAnswer answer;
  if (!enabled()) {
    answer.refusal = "this node keeps no Durable Object log";
    return answer;
  }
  if (hooks.knownShardEpoch && ask.shardEpoch &&
      hooks.knownShardEpoch(ask.namespaceId, ask.shard) > ask.shardEpoch) {
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
    if (pending != info.unpublishedShards.end() && pending->second) {
      if (std::find(earlierPriority.begin(), earlierPriority.end(), ask.ownerNodeEpoch) == earlierPriority.end())
        earlierPriority.push_back(ask.ownerNodeEpoch);
      tasks.add(timer.afterDelay(0 * kj::MILLISECONDS).then([this]() { return resealEarlier(); }));
      answer.refusal = "the restored owner shard still has unpublished records";
      log("restored-owner-tail-pending", "\"namespaceId\":" + quote(ask.namespaceId) +
          ",\"shard\":" + std::to_string(ask.shard) + ",\"sourceNodeEpoch\":\"" +
          sourceEpoch + "\",\"records\":" + std::to_string(pending->second));
      return answer;
    }
    if (remote && ask.shardEpoch > 1)
      fenceOwner(ask.namespaceId, ask.shard, ask.shardEpoch - 1, "a remote complete owner proof", true);
    ++tailAnswers;++restoredEmptyAnswers;
    answer.ownerConfirmed = true;
    log("restored-owner-tail-empty", "\"namespaceId\":" + quote(ask.namespaceId) +
        ",\"shard\":" + std::to_string(ask.shard) + ",\"sourceNodeEpoch\":\"" + sourceEpoch + "\"");
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
  for (const auto& [streamId, buffer] : incoming) {
    if (!ask.ownerNodeId.empty() && buffer.nodeId != ask.ownerNodeId) continue;
    if (ask.ownerNodeEpoch && buffer.nodeEpoch != ask.ownerNodeEpoch) continue;
    answer.droppedTo = std::max(answer.droppedTo, buffer.droppedTo);
    for (auto it = buffer.held.upper_bound(ask.afterSeq); it != buffer.held.end(); ++it) {
      if (buffer.isolated.contains(it->first)) continue;
      auto metadata = buffer.positions.find(it->first);
      take(it->second, metadata == buffer.positions.end() ? nullptr : &metadata->second);
    }
  }
  if (ask.ownerNodeId.empty() || ask.ownerNodeId == settings.nodeId) {
    if (!ask.ownerNodeEpoch || ask.ownerNodeEpoch == nodeEpoch) {
      for (auto it = records.upper_bound(ask.afterSeq); it != records.end(); ++it) {
        if (!confirmed.contains(it->first) || isolated.contains(it->first)) continue;
        auto metadata = parsed.find(it->first);
        take(it->second, metadata == parsed.end() ? nullptr : &metadata->second);
      }
    }
    for (const auto& [epoch, held] : earlier) {
      if ((ask.ownerNodeEpoch && ask.ownerNodeEpoch != epoch) || restoredEpochs.contains(epoch)) continue;
      if (!membership.heldEpochs.contains(std::to_string(epoch))) continue;
      for (auto it = held.upper_bound(ask.afterSeq); it != held.end(); ++it) {
        if (!matches(it->second)) continue;
        temporary.push_back(encodeLogRecord(it->second));
        take(temporary.back(), &it->second);
      }
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
    if (bytes && bytes + encoded.size() > TAIL_ANSWER_BYTES) {
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
    if (encoded.size() > TAIL_ANSWER_BYTES) {
      answer.more = std::next(it) != ordered.end();
      break;  // One over-budget record occupies the page alone.
    }
  }
  return answer;
}

// ---- The state endpoint ----

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
  uint32_t local = 0, rebuildingNow = 0, repairing = 0, pendingHeals = 0, currentHealErrors = 0, heldCaches = 0, openCaches = 0;
  uint64_t activeRequests = 0, activeConnections = 0;
  for (const auto& [name, object] : objects) {
    for (const auto& [id, holder] : object.holders) {
      const auto channel = channels.find(id);
      if (channel != channels.end() && channel->second.open) {
        activeRequests += holder.requests;
        activeConnections += holder.connections;
      }
    }
    if (object.cacheHold) ++heldCaches;
    if (object.cacheOpen) ++openCaches;
    if (object.healPending) ++pendingHeals;
    if (object.healError && object.healPending && object.state != ObjectState::GONE) ++currentHealErrors;
    if (object.state == ObjectState::LOCAL) ++local;
    else if (object.state == ObjectState::REBUILDING) ++rebuildingNow;
    else if (object.state == ObjectState::REPAIR) ++repairing;
  }
  out << "{\"enabled\":" << (enabled() ? "true" : "false") << ",\"nodeEpoch\":\"" << nodeEpoch
      << "\",\"earlierRecords\":" << earlierRecords << ",\"ownerFences\":" << ownerFenceEvents
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
  out << "],\"switches\":" << peerSwitches << "},\"confirm\":{\"confirmations\":" << confirmations
      << ",\"failures\":" << confirmFailures << ",\"refusals\":" << refusals << ",\"isolations\":" << isolations
      << ",\"unconfirmed\":" << unconfirmed << ",\"unconfirmedBytes\":\"" << unconfirmedBytes
      << "\",\"meanMs\":" << (confirmations ? confirmMsTotal / double(confirmations) : 0) << ",\"p50Ms\":"
      << percentile(confirmSamples, 0.5) << ",\"p99Ms\":" << percentile(confirmSamples, 0.99)
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
      << checkpoints << ",\"asked\":" << checkpointAsks << "},\"objects\":{\"known\":" << objects.size()
      << ",\"local\":" << local << ",\"rebuilding\":" << rebuildingNow << ",\"repairing\":" << repairing
      << ",\"hold\":" << heldCaches << ",\"openCache\":" << openCaches
      << ",\"activeRequests\":" << activeRequests << ",\"activeConnections\":" << activeConnections
      << ",\"rebuilds\":" << rebuilds << ",\"rebuildFailures\":" << rebuildFailures << ",\"repairs\":" << repairs
      << ",\"recoverTimeouts\":" << recoverTimeouts << ",\"lastRebuildMs\":" << lastRebuildMs << ",\"lastError\":"
      << quote(lastRebuildError) << ",\"tooLargeAnswers\":" << overLimitAnswers << "},\"disk\":{\"trusted\":"
      << (diskTrusted ? "true" : "false") << ",\"note\":" << quote(startupNote) << ",\"limitBytes\":\""
      << diskLimitBytes << "\",\"usedBytes\":\"" << diskUsedBytes << "\",\"filesDropped\":" << filesDropped
      << ",\"coldFilesSkippedLive\":" << coldFilesSkippedLive
      << "},\"buffer\":{\"bytes\":\"" << incomingBytes << "\",\"limitBytes\":\"" << settings.config.peerBufferBytes
      << "\",\"streams\":" << incoming.size() << ",\"refusals\":" << bufferRefusals << ",\"fencedRecords\":"
      << fencedRecords << ",\"tailAnswers\":" << tailAnswers << ",\"tailFetches\":" << tailFetches << ",\"owners\":[";
  first = true;
  for (const auto& [streamId, buffer] : incoming) {
    if (!first) out << ",";
    first = false;
    out << "{\"nodeId\":" << quote(buffer.nodeId) << ",\"nodeEpoch\":\"" << buffer.nodeEpoch << "\",\"records\":"
        << buffer.held.size() << ",\"bytes\":\"" << buffer.bytes << "\",\"acceptedSeq\":\"" << buffer.acceptedSeq
        << "\",\"droppedTo\":\"" << buffer.droppedTo << "\"}";
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
      << ",\"lateRecords\":" << lateTailRecords << ",\"transferredRecords\":" << transferredTailRecords
      << ",\"unaccountedRecords\":" << unaccountedTailRecords << ",\"unaccountedBytes\":\""
      << unaccountedTailBytes << "\",\"restoredUnaccountedRecords\":" << restoredUnaccountedRecords
      << ",\"restoredUnaccountedBytes\":\"" << restoredUnaccountedBytes
      << "\",\"restoredEmptyAnswers\":" << restoredEmptyAnswers
      << ",\"restoredLogUnavailableAnswers\":" << restoredLogUnavailableAnswers
      << ",\"localLogEmptyClosures\":" << localLogEmptyClosures
      << ",\"ownerLogUnavailable\":" << ownerLogUnavailableRecoveries
      << ",\"healSnapshots\":" << healSnapshots << ",\"healFailures\":" << healFailures
      << ",\"healedWalFrames\":" << healedWalFrames
      << ",\"healErrors\":" << currentHealErrors << ",\"healPending\":" << pendingHeals << ",\"healBusy\":" << (healBusy ? "true" : "false")
      << ",\"recordLimitRefusals\":" << recordLimitRefusals
      << ",\"objectFailures\":" << objectFailures << "},\"alarms\":{\"scheduled\":" << alarms.size() << ",\"set\":" << alarmsSet << ",\"cleared\":"
      << alarmsCleared << ",\"fired\":" << alarmsFired << ",\"failed\":" << alarmFailures << ",\"retries\":"
      << alarmRetries << ",\"notDue\":" << alarmsNotDue << ",\"adopted\":" << alarmsAdopted << ",\"dropped\":"
      << alarmsDropped << ",\"hintWrites\":" << alarmHintWrites << ",\"hintFailures\":" << alarmHintFailures
      << ",\"next\":[";
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
      << ",\"positionsReused\":" << positionsReused << ",\"positionWrites\":" << positionWrites
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
      << settings.config.recoverParallelism << ",\"residentObjects\":" << settings.config.residentObjects
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
