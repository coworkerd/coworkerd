#include "durable-log.h"

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
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <iostream>
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

DatabaseReplay::DatabaseReplay(std::filesystem::path pathParam, bool fromEmpty): path(std::move(pathParam)) {
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
    KJ_REQUIRE(::ftruncate(fd, 0) == 0, "cannot empty a Durable Object database", path.string().c_str());
    haveHeader = false;
    ++records;
    return true;
  }
  if (record.kind != LogRecordKind::FRAMES) return true;
  if (!record.header.empty()) {
    const auto* header = reinterpret_cast<const unsigned char*>(record.header.data());
    uint32_t m = beU32(header);
    if ((m & 0xFFFFFFFE) != 0x377f0682) return refuse("not a WAL header");
    uint32_t s1 = 0, s2 = 0;
    walChecksum(m & 1, header, 24, s1, s2);
    if (s1 != beU32(header + 24) || s2 != beU32(header + 28)) return refuse("WAL header checksum");
    magic = m;
    pageSize = beU32(header + 8);
    salt1 = beU32(header + 16);
    salt2 = beU32(header + 20);
    cksum1 = s1;
    cksum2 = s2;
    nextFrame = 1;
    haveHeader = true;
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
    while (done < pageSize) {
      auto written = ::pwrite(fd, frame + 24 + done, pageSize - done, offset + off_t(done));
      if (written < 0 && errno == EINTR) continue;
      KJ_REQUIRE(written > 0, "cannot write a page of a Durable Object database", path.string().c_str());
      done += size_t(written);
    }
    const uint32_t committed = beU32(frame + 4);
    if (committed != 0) {
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
                  FROM_DELETED = 6, FROM_CLOSED = 7, FROM_GAVE_UP = 8;
// Messages to one.
constexpr uint8_t TO_ACK = 1, TO_SEALED = 2, TO_BREAK = 3, TO_CHECKPOINT = 4;
// An answer's status.
constexpr uint8_t ACK_OK = 0, ACK_UNAVAILABLE = 1, ACK_TOO_LARGE = 3;
// The facet tree index workerd keeps beside an object's databases (src/workerd/server/facet-tree-index.h): an
// 8 byte magic number, then one entry per facet -- its parent (2 bytes), the length of its name (2) and the name.
constexpr uint64_t FACET_INDEX_MAGIC = 0xc4cdce5bc5b0ef57ull;
// How many of this node's unsealed records one answer to another node's rebuild carries.
constexpr size_t TAIL_ANSWER_BYTES = 32u << 20;
// The largest record this node takes from another's replicator: a durable process cuts its messages at 8 MiB of
// frames (workerd's COWORKERD_DO_RECORD_BYTES), so anything far above that is not one of ours.
constexpr size_t RECORD_BYTES_LIMIT = 256u << 20;
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
constexpr double PEER_RETRY_MS = 1000;

// The object a record belongs to, as this node keeps it: the name its Host was made with.
std::string hostNameOf(const LogRecord& record) {
  return record.objectName + "/" + std::to_string(record.dataEpoch);
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
  if (logFd >= 0) {
    ::close(logFd);
    logFd = -1;
  }
  nodeEpoch = epoch;
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
    log("log-file-failed", "\"path\":" + quote(logPath.string()) + ",\"errno\":" + std::to_string(errno));
  }
  if (peerCount() == 0) {
    log("no-peers", "\"warning\":\"this node has no peer to confirm its Durable Object writes: every write waits for "
                    "this machine's own disk, which costs tens of milliseconds\"");
  }
}

std::string DurableLog::bootId() const {
  // Linux names the boot of the running kernel; elsewhere (macOS) there is none to read, so only a clean exit lets
  // the next start reuse this machine's files. The regressions name a boot of their own to drive both.
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
    const auto clean = json::boolean(reader, "clean");
    const auto current = bootId();
    if (clean) {
      reason = "the marker says this node exited cleanly";
      return true;
    }
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

void DurableLog::writeCacheMarker(bool clean) {
  const auto directory = settings.stackDirectory / "do";
  std::error_code code;
  std::filesystem::create_directories(directory, code);
  const auto path = directory / "cache.json";
  const auto temporary = directory / "cache.json.new";
  std::string bytes = "{\"bootId\":" + quote(bootId()) + ",\"clean\":" + (clean ? "true" : "false") +
      ",\"nodeId\":" + quote(settings.nodeId) + ",\"atUnixMs\":" + std::to_string(realClockUnixMs()) + "}\n";
  const int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
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
  std::filesystem::rename(temporary, path, code);
}

// ---- Startup: this machine's files, and the records no segment covers ----

kj::Promise<void> DurableLog::start() {
  if (!enabled()) co_return;
  std::error_code code;
  std::filesystem::create_directories(durableDirectory(), code);
  std::string reason;
  diskTrusted = readCacheMarker(reason);
  startupNote = reason;
  if (!diskTrusted) {
    // The machine restarted and nothing says it was clean: with synchronous=OFF the files may hold pages the kernel
    // never wrote, so every object is rebuilt from its replicated history instead.
    uint64_t removed = 0;
    for (const auto& entry : std::filesystem::directory_iterator(durableDirectory(), code)) {
      std::filesystem::remove_all(entry.path(), code);
      ++removed;
    }
    std::filesystem::remove_all(logDirectory(), code);
    filesDropped += removed;
    log("cache-dropped", "\"reason\":" + quote(reason) + ",\"entries\":" + std::to_string(removed));
  } else {
    co_await readOwnLog();
  }
  // From here on this node's files are its own: the marker says the next start may reuse them only after a clean
  // exit or within the same boot.
  writeCacheMarker(false);
  diskLimitBytes = settings.config.diskBytes;
  if (!diskLimitBytes) {
    auto space = std::filesystem::space(settings.stackDirectory, code);
    diskLimitBytes = code ? (8ull << 30) : space.capacity / 2;
  }
  log("started", "\"trusted\":" + std::string(diskTrusted ? "true" : "false") + ",\"reason\":" + quote(reason) +
          ",\"objects\":" + std::to_string(objects.size()) + ",\"diskBytes\":\"" + std::to_string(diskLimitBytes) +
          "\",\"peers\":" + std::to_string(settings.config.peers));
}

// Reads the log files this node left: every record gives the position of its database, and the records after the
// last segment marker are the ones no segment covers. Each object's WAL is then cut back to the last frame a record
// names -- a durable process may have committed a transaction and died before the frames reached this node, and
// nobody was told that write succeeded.
kj::Promise<void> DurableLog::readOwnLog() {
  std::error_code code;
  if (!std::filesystem::exists(logDirectory(), code)) co_return;
  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(logDirectory(), code)) {
    if (entry.path().extension() == ".log") files.push_back(entry.path());
  }
  std::sort(files.begin(), files.end());
  // The last frame of each database any record names, and the generation it belongs to.
  struct Position {
    uint64_t nodeEpoch = 0, generation = 0;
    uint32_t salt1 = 0, salt2 = 0, pageSize = 0, lastFrame = 0;
    std::string storageId, objectName;
    uint32_t facetId = 0, parentFacetId = 0;
    std::string facetName;
    uint64_t txn = 0, shardEpoch = 0, dataEpoch = 0;
    uint32_t shard = 0;
    bool reset = false;
  };
  std::map<std::string, Position> positions;
  uint64_t read = 0, kept = 0, broken = 0;
  for (const auto& file : files) {
    const auto epoch = decimal(file.stem().string());
    std::string bytes;
    try {
      bytes = readFile(file);
    } catch (const kj::Exception&) {
      continue;
    }
    uint64_t sealedTo = 0;
    std::map<uint64_t, LogRecord> unsealed;
    for (const auto& encoded : splitRecords(bytes)) {
      std::string error;
      auto record = decodeLogRecord(encoded, &error);
      if (!record) {
        // Only the tail of the file can be cut short (one write per record, no flush): the rest is as it was.
        ++broken;
        break;
      }
      ++read;
      if (record->kind == LogRecordKind::SEAL) {
        sealedTo = std::max(sealedTo, record->nodeSeq);
        continue;
      }
      unsealed[record->nodeSeq] = *record;
      auto& position = positions[record->shipperKey()];
      position.nodeEpoch = record->nodeEpoch;
      position.objectName = record->objectName;
      position.dataEpoch = record->dataEpoch;
      position.storageId = record->storageId;
      position.facetId = record->facetId;
      position.parentFacetId = record->parentFacetId;
      position.facetName = record->facetName;
      position.shard = record->shard;
      position.shardEpoch = std::max(position.shardEpoch, record->shardEpoch);
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
    kept += unsealed.size();
    if (!unsealed.empty()) earlier[epoch] = std::move(unsealed);
  }
  // Every database this node's log knows: its files are cut back to the position the log names, and the object is
  // this node's again at that data epoch.
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
    auto& facet = object.facets[position.facetId];
    facet.facetId = position.facetId;
    facet.parentFacetId = position.parentFacetId;
    facet.facetName = position.facetName;
    facet.generation = position.generation;
    facet.salt1 = position.salt1;
    facet.salt2 = position.salt2;
    facet.txn = position.txn;
    cutBackWal(position.storageId, position.facetId, position.salt1, position.salt2, position.pageSize,
        position.lastFrame);
  }
  log("log-read", "\"files\":" + std::to_string(files.size()) + ",\"records\":" + std::to_string(read) +
          ",\"unsealed\":" + std::to_string(kept) + ",\"objects\":" + std::to_string(objects.size()) +
          ",\"cutShort\":" + std::to_string(broken));
  co_return;
}

// Cuts a database's WAL back to `lastFrame` of the generation the log names and removes its shared memory, so that
// the next process to open it sees exactly what this node has: with synchronous=OFF the kernel keeps what was
// written, and what a process committed without handing it on was never answered to anybody.
void DurableLog::cutBackWal(const std::string& storageId, uint32_t facetId, uint32_t salt1, uint32_t salt2,
    uint32_t pageSize, uint32_t lastFrame) {
  const auto wal = databasePath(storageId, facetId, "-wal");
  std::error_code code;
  if (!std::filesystem::exists(wal, code)) return;
  const auto size = std::filesystem::file_size(wal, code);
  if (code) return;
  uint64_t want = 0;
  if (size >= 32 && pageSize >= 512) {
    const int fd = ::open(wal.c_str(), O_RDONLY);
    if (fd >= 0) {
      unsigned char header[32];
      const auto got = ::pread(fd, header, sizeof(header), 0);
      ::close(fd);
      if (got == ssize_t(sizeof(header)) && beU32(header + 16) == salt1 && beU32(header + 20) == salt2) {
        want = 32 + uint64_t(lastFrame) * (24 + uint64_t(pageSize));
      }
    }
  }
  if (want && want >= size) {
    std::filesystem::remove(databasePath(storageId, facetId, "-shm"), code);
    return;
  }
  // Either a generation this node has no record of (its frames were never handed on) or a tail past the last record.
  if (want) {
    const int fd = ::open(wal.c_str(), O_WRONLY);
    if (fd >= 0) {
      ::ftruncate(fd, off_t(want));
      ::fsync(fd);
      ::close(fd);
    }
  } else {
    std::filesystem::remove(wal, code);
  }
  std::filesystem::remove(databasePath(storageId, facetId, "-shm"), code);
  ++walCutBacks;
  log("wal-cut-back", "\"storageId\":" + quote(storageId) + ",\"facet\":" + std::to_string(facetId) + ",\"was\":\"" +
          std::to_string(size) + "\",\"now\":\"" + std::to_string(want) + "\"");
}

kj::Promise<void> DurableLog::stop(double deadlineMonotonicMs) {
  if (!enabled()) co_return;
  try {
    if (!records.empty()) co_await sealNow("drain");
  } catch (const kj::Exception& error) {
    log("drain-seal-failed", "\"error\":" + quote(std::string(error.getDescription().cStr())));
  }
  (void)deadlineMonotonicMs;
  // The files stay usable for the next start of this node.
  writeCacheMarker(true);
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
  tasks.add(readChannel(channel).catch_([this, processId](kj::Exception&& error) {
    auto found = channels.find(processId);
    if (found != channels.end()) found->second.open = false;
    log("channel-ended", "\"processId\":" + quote(processId) + ",\"reason\":" +
            quote(std::string(error.getDescription().cStr())));
  }));
  return fds[1];
}

void DurableLog::closeChannel(const std::string& processId) {
  auto found = channels.find(processId);
  if (found == channels.end()) return;
  found->second.open = false;
  channels.erase(found);
}

kj::Promise<void> DurableLog::readChannel(Channel& channel) {
  auto buffer = kj::heapArray<kj::byte>(65536);
  for (;;) {
    const auto read = co_await channel.stream->tryRead(buffer.begin(), 1, buffer.size());
    if (read == 0) {
      channel.open = false;
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
    if (found != channels.end()) found->second.open = false;
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
      auto& facet = object->facets[facetId];
      facet.facetId = facetId;
      facet.parentFacetId = parentFacetId;
      facet.facetName = facetName;
      facet.databaseBytes = databaseBytes;
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
      in.u32();  // salt1 of the generation that was checkpointed
      in.u32();  // salt2
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
      auto& facet = object->facets[facetId];
      facet.databaseBytes = databaseBytes;
      // The database file now holds every transaction of the generation that was checkpointed, so a snapshot taken
      // from here needs only the records after that one.
      facet.checkpointedSeq = facet.lastSeq;
      facet.lastFrame = 0;
      facet.salt1 = 0;
      facet.salt2 = 0;
      facet.generation = generation;
      uint64_t bytes = 0;
      for (const auto& [id, entry] : object->facets) bytes += entry.databaseBytes;
      object->bytes = bytes;
      log("checkpoint", "\"object\":" + quote(objectName) + ",\"facet\":" + std::to_string(facetId) + ",\"frames\":" +
              std::to_string(frames) + ",\"databaseBytes\":\"" + std::to_string(databaseBytes) + "\"");
      if (bytes > settings.config.objectBytes && !object->overLimit) {
        object->overLimit = true;
        log("object-too-large", "\"object\":" + quote(objectName) + ",\"bytes\":\"" + std::to_string(bytes) +
                "\",\"limit\":\"" + std::to_string(settings.config.objectBytes) + "\"");
      }
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
        auto* object = objectOf(objectName);
        if (object != nullptr) object->facets[facetId].open = false;
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

void DurableLog::appendToLogFile(const std::string& encoded) {
  if (logFd < 0) return;
  std::string framed;
  putU32(framed, uint32_t(encoded.size()));
  framed += encoded;
  size_t done = 0;
  while (done < framed.size()) {
    auto written = ::write(logFd, framed.data() + done, framed.size() - done);
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) {
      log("log-write-failed", "\"errno\":" + std::to_string(errno));
      return;
    }
    done += size_t(written);
  }
}

// One record of this node's log: it goes to the peers of its object's shard, and the answer to the request that
// carried it waits for them. Nothing here touches the disk or the object store (experiment 25, D.2).
void DurableLog::takeRecord(Channel& channel, LogRecord&& record, uint64_t request) {
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
  if (object->overLimit) {
    ++overLimitAnswers;
    if (request) answer(channel, request, ACK_TOO_LARGE);
    return;
  }
  if (object->state != ObjectState::LOCAL) {
    log("record-refused", "\"object\":" + quote(record.objectName) + ",\"reason\":" + quote(object->repairReason.empty()
            ? std::string("the object is being rebuilt") : object->repairReason));
    if (request) answer(channel, request, ACK_UNAVAILABLE);
    return;
  }
  auto& facet = object->facets[record.facetId];
  facet.facetId = record.facetId;
  if (!record.facetName.empty()) {
    facet.parentFacetId = record.parentFacetId;
    facet.facetName = record.facetName;
  }
  facet.open = true;
  record.nodeSeq = ++nextSeq;
  record.nodeEpoch = nodeEpoch;
  record.shard = object->shard;
  record.shardEpoch = object->shardEpoch;
  record.txn = facet.txn + 1;
  if (record.kind == LogRecordKind::FRAMES) {
    facet.txn += record.commits;
    facet.lastFrame = record.firstFrame + record.frames - 1;
    facet.salt1 = record.salt1;
    facet.salt2 = record.salt2;
    facet.generation = record.generation;
    framesTaken += record.frames;
  }
  facet.lastSeq = record.nodeSeq;
  const auto encoded = encodeLogRecord(record);
  ++recordsTaken;
  bytesTaken += encoded.size();
  recordBytes += encoded.size();
  object->sinceSnapshotBytes += encoded.size();
  appendToLogFile(encoded);
  records[record.nodeSeq] = encoded;
  // The frames live in `records` alone: what is kept beside them is the record without its bytes, for the segments'
  // index and for the answers (a rebuild decodes the ones it needs).
  {
    auto light = record;
    light.frameBytes.clear();
    light.header.clear();
    parsed[record.nodeSeq] = std::move(light);
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
  const auto now = monotonicTimeMs();
  for (const auto& peer : flight.peers) {
    auto found = peers.find(peer);
    if (found == peers.end() || found->second.broken || found->second.sink == kj::none) {
      // One attempt a second at a peer that cannot be reached, not one per write.
      if (found == peers.end() || now - found->second.attemptedAtMs >= PEER_RETRY_MS) {
        auto& entry = peers[peer];
        entry.nodeId = peer;
        entry.attemptedAtMs = now;
        tasks.add(openPeer(peer));
      }
      continue;
    }
    sendOne(found->second, record.nodeSeq, encoded);
  }
  if (flight.peers.empty()) {
    // A single node cluster: there is nobody to hold the frames, so this machine's own disk decides and the write
    // waits for it (slow but correct, and a warning says so at startup).
    if (logFd >= 0) {
#ifdef __APPLE__
      ::fsync(logFd);
#else
      ::fdatasync(logFd);
#endif
    }
  }
  checkConfirmations();
  if (recordBytes >= settings.config.segmentBytes) tasks.add(sealNow("bytes"));
}

uint32_t DurableLog::peerCount() const {
  uint32_t live = 0;
  for (const auto& [id, member] : membership.members) {
    if (id == settings.nodeId || member.state == "draining") continue;
    ++live;
  }
  return std::min(settings.config.peers, live);
}

// The peers of one shard: the first entries after it on the member ring -- the same order on every node, so a new
// owner knows whom to ask for the tail of the previous owner's log.
std::vector<std::string> DurableLog::peersOf(const std::string& namespaceId, uint32_t shard) const {
  std::vector<std::string> ids;
  for (const auto& [id, member] : membership.members) {
    if (id == settings.nodeId || member.state == "draining") continue;
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

// Sends what the stream's window allows, and again as the answers come back.
void DurableLog::pumpPeer(PeerStream& peer) {
  while (!peer.queue.empty() && peer.inFlightBytes < APPEND_WINDOW_BYTES) {
    if (peer.broken || peer.sink == kj::none) return;
    auto item = std::move(peer.queue.front());
    peer.queue.pop_front();
    const auto size = item.bytes.size();
    peer.queuedBytes -= std::min(peer.queuedBytes, uint64_t(size));
    peer.inFlightBytes += size;
    ++peer.appends;
    peer.sentSeq = std::max(peer.sentSeq, item.sequence);
    const auto nodeId = peer.nodeId;
    auto& sink = *KJ_ASSERT_NONNULL(peer.sink);
    tasks.add(sink.append(std::move(item.bytes), item.partial)
                  .then([this, nodeId, size](PeerSink::Accepted accepted) {
      auto found = peers.find(nodeId);
      if (found == peers.end()) return;
      found->second.inFlightBytes -= std::min(found->second.inFlightBytes, uint64_t(size));
      if (!accepted.refusal.empty()) {
        ++refusals;
        ++found->second.refusals;
        found->second.lastRefusal = accepted.refusal;
        if (accepted.refusal.find("fenced") != std::string::npos) ++fencedRecords;
        log("peer-refused", "\"peer\":" + quote(nodeId) + ",\"reason\":" + quote(accepted.refusal));
        pumpPeer(found->second);
        return;
      }
      found->second.lastRefusal.clear();
      found->second.lastAckMs = monotonicTimeMs();
      onPeerAck(nodeId, accepted.sequence);
      auto again = peers.find(nodeId);
      if (again != peers.end()) pumpPeer(again->second);
    }).catch_([this, nodeId, size](kj::Exception&& error) {
      auto found = peers.find(nodeId);
      if (found != peers.end()) {
        found->second.inFlightBytes -= std::min(found->second.inFlightBytes, uint64_t(size));
        if (!found->second.broken) {
          found->second.broken = true;
          found->second.sink = kj::none;
          // What was queued for the stream goes with it: the stream that takes its place is given every record no
          // segment covers.
          found->second.queue.clear();
          found->second.queuedBytes = 0;
          ++peerSwitches;
          log("peer-lost", "\"peer\":" + quote(nodeId) + ",\"error\":" +
                  quote(std::string(error.getDescription().cStr())));
        }
      }
    }));
  }
}

// Opens a stream to one peer and hands it every record no segment covers yet, so a peer that takes another's place
// holds the same tail (requirement 2: a peer that died is replaced by the next node on the ring).
kj::Promise<void> DurableLog::openPeer(std::string nodeId) {
  auto& peer = peers[nodeId];
  if (peer.connecting) co_return;
  peer.nodeId = nodeId;
  peer.connecting = true;
  peer.broken = false;
  peer.attemptedAtMs = monotonicTimeMs();
  peer.openedAtMs = monotonicTimeMs();
  try {
    auto sink = co_await hooks.openStream(nodeId, sealedSeq + 1);
    auto& entry = peers[nodeId];
    entry.nodeId = nodeId;
    entry.sink = kj::mv(sink);
    entry.connecting = false;
    entry.firstSeq = sealedSeq + 1;
    entry.ackedSeq = 0;
    log("peer-opened", "\"peer\":" + quote(nodeId) + ",\"firstSeq\":\"" + std::to_string(entry.firstSeq) + "\"");
    // Everything this node holds goes out at once: the backlog first, then the records as they come.
    for (const auto& [sequence, encoded] : records) {
      if (isolated.contains(sequence)) continue;
      sendOne(entry, sequence, encoded);
    }
  } catch (const kj::Exception& error) {
    auto& entry = peers[nodeId];
    entry.connecting = false;
    entry.broken = true;
    entry.sink = kj::none;
    entry.lastRefusal = std::string(error.getDescription().cStr());
    log("peer-open-failed", "\"peer\":" + quote(nodeId) + ",\"error\":" + quote(entry.lastRefusal));
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
  std::set<std::string> blocked;
  const auto configured = peerCount();
  std::vector<uint64_t> done;
  bool flushed = false;
  for (auto& [sequence, flight] : inFlight) {
    if (isolated.contains(sequence)) continue;
    if (blocked.contains(flight.objectKey)) continue;
    uint32_t held = 0, reachable = 0;
    for (const auto& peer : flight.peers) {
      auto found = peers.find(peer);
      if (found == peers.end()) continue;
      if (found->second.sink != kj::none && !found->second.broken) ++reachable;
      if (found->second.ackedSeq >= sequence) ++held;
    }
    // The peers of the record that can be reached decide how many must hold it: a member nothing can reach is not one
    // of them, and with none left this machine's own disk answers for the write (the warning at startup says so).
    const auto want = std::min(configured, reachable);
    if (held < want || (reachable == 0 && !flight.peers.empty())) {
      blocked.insert(flight.objectKey);
      continue;
    }
    if (want == 0 && logFd >= 0 && !flushed) {
      flushed = true;
#ifdef __APPLE__
      ::fsync(logFd);
#else
      ::fdatasync(logFd);
#endif
      if (configured > 0 && !noPeerWarned) {
        noPeerWarned = true;
        log("no-peer-reachable", "\"warning\":\"no peer of a shard can be reached: its writes are answered once this "
            "machine's own disk holds them, and the peers take over again as soon as one answers\"");
      }
    }
    done.push_back(sequence);
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
  if (dropped.empty()) return;
  for (auto sequence : dropped) {
    isolated.insert(sequence);
    auto record = parsed.find(sequence);
    isolatedReported.emplace_back(record == parsed.end() ? 0 : record->second.shardEpoch, sequence);
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
    if (peer.sink == kj::none) continue;
    auto& sink = *KJ_ASSERT_NONNULL(peer.sink);
    tasks.add(sink.drop(dropped).catch_([](kj::Exception&&) {}));
  }
  log("isolated", "\"object\":" + quote(objectKey) + ",\"records\":" + std::to_string(dropped.size()) + ",\"from\":\"" +
          std::to_string(dropped.front()) + "\",\"reason\":" + quote(reason));
}

// The object is not to be served until its files have been thrown away and rebuilt from the replicated history
// (experiment 25, D.5): every object of this node's durable processes is closed, which is what shuts the files.
void DurableLog::markRepair(Object& object, const std::string& reason) {
  if (object.state == ObjectState::REPAIR) return;
  object.state = ObjectState::REPAIR;
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
  tasks.add(sealLoop());
  tasks.add(housekeeping());
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
  if (!enabled() || sealing || !nodeEpoch) co_return;
  std::vector<std::string> encoded;
  std::map<std::pair<std::string, uint32_t>, SegmentIndexEntry> index;
  std::map<std::string, std::pair<uint32_t, uint32_t>> lastFrameOf;  // object key -> (salts' frame, frame)
  std::map<std::string, std::pair<uint32_t, uint32_t>> saltsOf;
  uint64_t first = 0, last = 0, bytes = 0;
  std::set<std::string> touched;
  for (const auto& [sequence, record] : records) {
    if (isolated.contains(sequence)) {
      last = sequence;
      continue;
    }
    if (!confirmed.contains(sequence)) break;
    auto found = parsed.find(sequence);
    if (found == parsed.end()) break;
    if (!first) first = sequence;
    last = sequence;
    bytes += record.size();
    encoded.push_back(record);
    const auto& entry = found->second;
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
  if (encoded.empty()) {
    if (last > sealedSeq) sealedSeq = last;  // only isolated records: nothing to put anywhere
    co_return;
  }
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
  const auto body = encodeSegment(header, encoded);
  co_await uploadSegment(header, body);
  segmentNumber = header.segment;
  ++segments;
  segmentBytes += body.size();
  lastSegmentAtMs = monotonicTimeMs();
  // Each object's manifest names the segment before anything drops the records: a manifest nobody has written yet
  // leaves the segment pointing at nothing, and a node taking the shard over would not replay it.
  uint32_t written = 0, failed = 0;
  for (const auto& objectName : touched) {
    auto* object = objectOf(objectName);
    if (object == nullptr) continue;
    object->segments.push_back(ManifestSegment{settings.nodeId, nodeEpoch, header.segment, first, last,
        object->shardEpoch});
    const bool ok = co_await writeManifest(*object, false).catch_([this](kj::Exception&& error) {
      ++manifestFailures;
      lastManifestError = std::string(error.getDescription().cStr());
      log("manifest-failed", "\"error\":" + quote(lastManifestError));
      return false;
    });
    if (ok) ++written;
    else ++failed;
  }
  log("segment", "\"segment\":\"" + std::to_string(header.segment) + "\",\"firstSeq\":\"" + std::to_string(first) +
          "\",\"lastSeq\":\"" + std::to_string(last) + "\",\"records\":" + std::to_string(header.records) +
          ",\"bytes\":\"" + std::to_string(body.size()) + "\",\"objects\":" + std::to_string(touched.size()) +
          ",\"manifests\":" + std::to_string(written) + ",\"manifestsFailed\":" + std::to_string(failed) +
          ",\"reason\":" + quote(reason));
  if (failed) {
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
    isolated.erase(sequence);
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
    auto& sink = *KJ_ASSERT_NONNULL(peer.sink);
    tasks.add(sink.trim(last).catch_([](kj::Exception&&) {}));
  }
  for (const auto& [key, frame] : lastFrameOf) {
    auto salts = saltsOf.find(key);
    if (salts == saltsOf.end()) continue;
    tellSealed(key, salts->second.first, salts->second.second, frame.first);
  }
}

// A segment is immutable and created only if absent; a failed upload is tried again with a growing wait, because
// the peers hold the records until it is there (requirement 7).
kj::Promise<void> DurableLog::uploadSegment(const SegmentHeader& header, const std::string& body) {
  const auto key = nodeSegmentKey(settings.nodeId, header.nodeEpoch, header.segment);
  auto wait = settings.config.uploadRetryMs;
  for (uint32_t attempt = 1;; ++attempt) {
    auto outcome = co_await store->create(key, body)
                       .then([](CreateResult result) { return std::make_pair(result.created, std::string()); })
                       .catch_([](kj::Exception&& error) {
      return std::make_pair(false, std::string(error.getDescription().cStr()));
    });
    if (outcome.second.empty()) {
      ++uploads;
      lastUploadAtMs = monotonicTimeMs();
      lastUploadError.clear();
      if (!outcome.first) log("segment-exists", "\"key\":" + quote(key) + ",\"attempt\":" + std::to_string(attempt));
      co_return;
    }
    ++uploadFailures;
    lastUploadError = outcome.second;
    log("upload-failed", "\"key\":" + quote(key) + ",\"attempt\":" + std::to_string(attempt) + ",\"error\":" +
            quote(lastUploadError) + ",\"retryMs\":" + std::to_string(wait));
    co_await timer.afterDelay(wait * kj::MILLISECONDS);
    wait = std::min(wait * 2, settings.config.uploadRetryMaxMs);
  }
}

// The object's next manifest revision: the latest snapshot and, after it, the segments to replay in order. Created
// only if absent, so two writers cannot both be the n-th; a revision whose shard epoch is higher than this node's
// says another node owns the shard now, and this node gives it up rather than writing over its history.
kj::Promise<bool> DurableLog::writeManifest(Object& object, bool withSnapshot) {
  if (!enabled()) co_return false;
  for (uint32_t attempt = 1; attempt <= MANIFEST_ATTEMPTS; ++attempt) {
    auto previous = co_await readManifest(object.namespaceId, object.objectId);
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
      manifest.facets = previous->facets;
      manifest.segments = previous->segments;
      manifest.isolated = previous->isolated;
    }
    if (withSnapshot) {
      manifest.hasSnapshot = true;
      manifest.snapshotSequence = object.snapshotSequence;
      manifest.snapshotParts = object.snapshotParts;
      manifest.snapshotBytes = object.snapshotBytes;
      manifest.snapshotAtUnixMs = object.snapshotAtUnixMs;
      manifest.snapshotNodeId = settings.nodeId;
      manifest.snapshotNodeEpoch = nodeEpoch;
      manifest.snapshotShardEpoch = object.shardEpoch;
      manifest.facets = object.snapshotFacets;
      // Every segment the snapshot covers may go: the file holds those transactions.
      uint64_t covered = 0;
      bool firstFacet = true;
      for (const auto& facet : object.snapshotFacets) {
        covered = firstFacet ? facet.upToSeq : std::min(covered, facet.upToSeq);
        firstFacet = false;
      }
      // Only the segments of the epoch this snapshot was taken in, and only those it holds every record of.
      std::erase_if(manifest.segments, [&](const ManifestSegment& segment) {
        return segment.shardEpoch < object.shardEpoch ||
            (segment.shardEpoch == object.shardEpoch && segment.lastSeq <= covered);
      });
    }
    for (const auto& segment : object.segments) manifest.segments.push_back(segment);
    for (const auto& entry : isolatedReported) {
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
      object.manifestRevision = manifest.revision;
      object.segments.clear();
      if (withSnapshot) {
        object.sinceSnapshotBytes = 0;
        object.snapshotAtUnixMs = manifest.snapshotAtUnixMs;
      }
      co_return true;
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
  if (object.status != 200) co_return std::nullopt;
  auto manifest = parseObjectManifest(object.bytes);
  if (!manifest) {
    log("manifest-unreadable", "\"object\":" + quote(namespaceId + "/" + objectId) + ",\"revision\":\"" +
            std::to_string(revision) + "\"");
    co_return std::nullopt;
  }
  co_return manifest;
}

// A snapshot: after a checkpoint the database file holds every transaction of the generations before it, so the
// file is uploaded in parts and the manifest then names it -- the segments it covers are no longer needed.
kj::Promise<void> DurableLog::snapshotIfDue(std::string objectName) {
  auto* object = objectOf(objectName);
  if (object == nullptr || !enabled() || object->state != ObjectState::LOCAL || object->snapshotting) co_return;
  const auto now = realClockUnixMs();
  const bool bySize = object->sinceSnapshotBytes > settings.config.snapshotBytes;
  const bool byTime = object->snapshotAtUnixMs != 0 &&
      now > object->snapshotAtUnixMs + settings.config.snapshotIntervalMs;
  const bool firstOne = object->snapshotAtUnixMs == 0 && bySize;
  if (!bySize && !byTime && !firstOne) co_return;
  object->snapshotting = true;
  const auto release = kj::defer([this, objectName]() {
    auto* entry = objectOf(objectName);
    if (entry != nullptr) entry->snapshotting = false;
  });
  const auto sequence = object->snapshotSequence + 1;
  std::vector<ManifestFacet> facets;
  uint64_t parts = 0, total = 0;
  for (const auto& [facetId, facet] : object->facets) {
    const auto path = databasePath(object->storageId, facetId);
    std::string bytes;
    try {
      bytes = readFile(path);
    } catch (const kj::Exception&) {
      continue;  // the facet's file is not here (deleted, or never written): nothing to snapshot
    }
    ManifestFacet entry;
    entry.facetId = facetId;
    entry.parentFacetId = facet.parentFacetId;
    entry.name = facet.facetName;
    entry.bytes = bytes.size();
    entry.sha256 = sha256Hex(bytes);
    entry.upToSeq = facet.checkpointedSeq;
    const auto partBytes = settings.config.snapshotPartBytes;
    const uint64_t count = (bytes.size() + partBytes - 1) / partBytes;
    kj::Vector<kj::Promise<void>> running;
    for (uint64_t part = 0; part < count; ++part) {
      const auto piece = bytes.substr(part * partBytes, std::min<uint64_t>(partBytes, bytes.size() - part * partBytes));
      running.add(uploadSnapshotPart(object->namespaceId, object->objectId, sequence, facetId, part, piece));
      if (running.size() >= settings.config.recoverParallelism) {
        co_await kj::joinPromises(running.releaseAsArray());
        running.clear();
      }
    }
    if (running.size() > 0) co_await kj::joinPromises(running.releaseAsArray());
    entry.parts = count;
    parts += count;
    total += bytes.size();
    facets.push_back(std::move(entry));
  }
  if (facets.empty()) co_return;
  object->snapshotSequence = sequence;
  object->snapshotParts = parts;
  object->snapshotBytes = total;
  object->snapshotAtUnixMs = now;
  object->snapshotFacets = facets;
  // The index is written after every part, so a manifest never names a snapshot whose parts are not all there.
  std::ostringstream index;
  index << "{\"sequence\":\"" << sequence << "\",\"nodeId\":" << quote(settings.nodeId) << ",\"nodeEpoch\":\""
        << nodeEpoch << "\",\"atUnixMs\":" << now << ",\"partBytes\":\"" << settings.config.snapshotPartBytes
        << "\",\"bytes\":\"" << total << "\",\"facets\":[";
  bool first = true;
  for (const auto& facet : facets) {
    if (!first) index << ",";
    first = false;
    index << "{\"facet\":" << facet.facetId << ",\"parent\":" << facet.parentFacetId << ",\"name\":"
          << quote(facet.name) << ",\"bytes\":\"" << facet.bytes << "\",\"parts\":\"" << facet.parts
          << "\",\"upToSeq\":\"" << facet.upToSeq << "\",\"sha256\":" << quote(facet.sha256) << "}";
  }
  index << "]}";
  co_await store->create(objectSnapshotIndexKey(object->namespaceId, object->objectId, sequence), index.str());
  ++snapshots;
  snapshotBytesWritten += total;
  log("snapshot", "\"object\":" + quote(objectName) + ",\"sequence\":\"" + std::to_string(sequence) +
          "\",\"facets\":" + std::to_string(facets.size()) + ",\"parts\":\"" + std::to_string(parts) +
          "\",\"bytes\":\"" + std::to_string(total) + "\"");
  co_await writeManifest(*object, true);
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
      co_await dropColdFiles();
    } catch (const kj::Exception& error) {
      log("housekeeping-failed", "\"error\":" + quote(std::string(error.getDescription().cStr())));
    }
  }
}

// The records of an earlier epoch of this node that no segment covers: this node is the only place they are (its
// peers kept them for an epoch that is gone), so they are sealed under that epoch before anything else may need
// them.
kj::Promise<void> DurableLog::resealEarlier() {
  if (earlier.empty() || !enabled() || !nodeEpoch) co_return;
  auto epoch = earlier.begin()->first;
  auto taken = std::move(earlier.begin()->second);
  earlier.erase(earlier.begin());
  std::vector<std::string> encoded;
  SegmentHeader header;
  std::map<std::pair<std::string, uint32_t>, SegmentIndexEntry> index;
  std::set<std::string> touched;
  for (const auto& [sequence, record] : taken) {
    encoded.push_back(encodeLogRecord(record));
    if (!header.firstSeq) header.firstSeq = sequence;
    header.lastSeq = sequence;
    auto& entry = index[{record.objectName, record.facetId}];
    entry.objectName = record.objectName;
    entry.facetId = record.facetId;
    if (!entry.records) entry.firstTxn = record.txn;
    entry.lastTxn = record.txn;
    ++entry.records;
    touched.insert(hostNameOf(record));
  }
  if (encoded.empty()) co_return;
  header.nodeId = settings.nodeId;
  header.nodeEpoch = epoch;
  header.segment = 1;
  header.records = encoded.size();
  header.sealedAtUnixMs = realClockUnixMs();
  header.reason = "earlier-epoch";
  for (auto& [key, entry] : index) header.objects.push_back(entry);
  // The segment number of an earlier epoch may be taken already: the next free one is used.
  auto listing = co_await store->list(nodeLogEpochPrefix(settings.nodeId, epoch));
  header.segment = latestRevision(listing, nodeLogPrefix(settings.nodeId) + wide(epoch)) + 1;
  const auto body = encodeSegment(header, encoded);
  co_await uploadSegment(header, body);
  ++segments;
  log("segment-earlier", "\"epoch\":\"" + std::to_string(epoch) + "\",\"segment\":\"" +
          std::to_string(header.segment) + "\",\"records\":" + std::to_string(header.records) + ",\"objects\":" +
          std::to_string(touched.size()));
  for (const auto& objectName : touched) {
    auto* object = objectOf(objectName);
    if (object == nullptr) continue;
    object->segments.push_back(ManifestSegment{settings.nodeId, epoch, header.segment, header.firstSeq,
        header.lastSeq, object->shardEpoch});
    co_await writeManifest(*object, false).catch_([](kj::Exception&&) { return false; });
  }
}

// This machine's files are a cache: when they take more than their share, the objects nothing has asked for in the
// longest time and whose changes are all in a segment lose theirs. The object is rebuilt from its history when it
// is asked for again.
kj::Promise<void> DurableLog::dropColdFiles() {
  std::error_code code;
  uint64_t total = 0;
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
    if (quiet && sealedAll && object.state == ObjectState::LOCAL && bytes > 0 && object.manifestRevision > 0)
      cold.emplace_back(object.lastAccessMs, name);
  }
  diskUsedBytes = total;
  if (total <= diskLimitBytes || cold.empty()) co_return;
  std::sort(cold.begin(), cold.end());
  for (const auto& [access, name] : cold) {
    if (total <= diskLimitBytes) break;
    auto* object = objectOf(name);
    if (object == nullptr) continue;
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
    object->state = ObjectState::UNKNOWN;
    object->facets.clear();
    total -= std::min<uint64_t>(total, freed);
    ++filesDropped;
    log("files-dropped", "\"object\":" + quote(name) + ",\"bytes\":\"" + std::to_string(freed) +
            "\",\"reason\":\"this machine's share of Durable Object files is full\"");
  }
  co_return;
}

// ---- Opening an object: its files are brought up to date with its replicated history first ----

kj::Promise<DurableLog::ObjectReady> DurableLog::openObject(ObjectIdentity identity) {
  if (!enabled()) co_return ObjectReady{};
  const auto objectName = durableHostName(identity.namespaceId, identity.objectId, identity.dataEpoch);
  auto settled = rebuilding.find(objectName);
  if (settled != rebuilding.end() && settledRebuilds.contains(objectName)) {
    rebuilding.erase(settled);
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
  if (identity.shardEpoch >= object.shardEpoch) {
    object.shardEpoch = identity.shardEpoch;
    if (object.state == ObjectState::GONE) object.state = ObjectState::UNKNOWN;
  }
  object.lastAccessMs = monotonicTimeMs();
  if (object.overLimit) {
    ++overLimitAnswers;
    co_return ObjectReady{false, 413, "DO_OBJECT_TOO_LARGE",
        "the object is larger than durableObjects.objectBytes"};
  }
  if (object.state == ObjectState::LOCAL) co_return ObjectReady{};
  if (object.state == ObjectState::GONE) {
    co_return ObjectReady{false, 409, "DO_SHARD_EPOCH_MISMATCH",
        "another node owns this object's shard now"};
  }
  const auto reason = object.state == ObjectState::REPAIR
      ? object.repairReason
      : std::string("this node has not served the object at this data epoch");
  if (rebuilding.find(objectName) == rebuilding.end()) {
    rebuilding.insert({objectName, rebuild(objectName, reason).fork()});
  }
  const auto start = monotonicTimeMs();
  auto branch = rebuilding.find(objectName)->second.addBranch();
  bool done = false;
  try {
    done = co_await kj::mv(branch).then([]() { return true; }).exclusiveJoin(
        timer.afterDelay(settings.config.recoverTimeoutMs * kj::MILLISECONDS).then([]() { return false; }));
  } catch (const kj::Exception& error) {
    lastRebuildError = std::string(error.getDescription().cStr());
    ++rebuildFailures;
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
  ObjectReady ready;
  ready.elapsedMs = monotonicTimeMs() - start;
  ready.rebuilt = true;
  co_return ready;
}

// The whole of a rebuild: the object's files on this machine go, its manifest says which snapshot and which
// segments make it up, and what no segment covers comes from the peers of its shard (and from this node's own log
// when it is repairing an object of its own). The previous owner's files are never read.
kj::Promise<void> DurableLog::rebuild(std::string objectName, std::string reason) {
  const auto start = monotonicTimeMs();
  {
    auto* object = objectOf(objectName);
    KJ_REQUIRE(object != nullptr, "an object being rebuilt is gone");
    const bool repair = object->state == ObjectState::REPAIR;
    object->state = ObjectState::REBUILDING;
    log("rebuild-started", "\"object\":" + quote(objectName) + ",\"reason\":" + quote(reason) + ",\"repair\":" +
            (repair ? "true" : "false"));
    if (repair && hooks.closeObjects) {
      // Nothing may hold the files while they are replaced, and the object must stop serving what it read from them.
      co_await hooks.closeObjects("a Durable Object write was not confirmed: " + reason);
    }
  }
  const auto done = kj::defer([this, objectName]() { settledRebuilds.insert(objectName); });
  auto* object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "an object being rebuilt is gone");
  removeObjectFiles(*object);
  auto manifest = co_await readManifest(object->namespaceId, object->objectId);
  object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "an object being rebuilt is gone");
  std::vector<ManifestFacet> facets;
  uint64_t fromSeq = 0;
  if (manifest) {
    if (manifest->shardEpoch > object->shardEpoch) {
      object->state = ObjectState::GONE;
      ++manifestFences;
      log("rebuild-fenced", "\"object\":" + quote(objectName) + ",\"ours\":\"" + std::to_string(object->shardEpoch) +
              "\",\"theirs\":\"" + std::to_string(manifest->shardEpoch) + "\"");
      KJ_FAIL_REQUIRE("another node owns this object's shard", objectName.c_str());
    }
    object->manifestRevision = manifest->revision;
    object->snapshotSequence = manifest->snapshotSequence;
    object->snapshotAtUnixMs = manifest->snapshotAtUnixMs;
    if (manifest->hasSnapshot) {
      co_await downloadSnapshot(*manifest, object->storageId);
      facets = manifest->facets;
      for (const auto& facet : facets) fromSeq = std::max(fromSeq, facet.upToSeq);
    }
  }
  std::vector<LogRecord> history;
  if (manifest) history = co_await readSegments(*manifest, object->namespaceId + "/" + object->objectId);
  object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "an object being rebuilt is gone");
  // What no segment covers: this node's own unsealed records for the object, and what the peers of its shard hold
  // of whichever owner wrote last.
  const auto historyName = object->namespaceId + "/" + object->objectId;
  for (const auto& [sequence, encoded] : records) {
    if (isolated.contains(sequence)) continue;
    auto decoded = decodeLogRecord(encoded);
    if (decoded && decoded->objectName == historyName) history.push_back(std::move(*decoded));
  }
  for (const auto& [epoch, taken] : earlier) {
    for (const auto& [sequence, record] : taken) {
      if (record.objectName == historyName) history.push_back(record);
    }
  }
  auto tails = co_await fetchTails(*object);
  for (auto& record : tails) history.push_back(std::move(record));
  object = objectOf(objectName);
  KJ_REQUIRE(object != nullptr, "an object being rebuilt is gone");
  std::set<std::pair<uint64_t, uint64_t>> drop;
  uint64_t snapshotShardEpoch = 0;
  if (manifest) {
    for (const auto& entry : manifest->isolated) drop.insert(entry);
    snapshotShardEpoch = manifest->snapshotShardEpoch;
  }
  applyRecords(*object, std::move(history), facets, snapshotShardEpoch, drop);
  if (!facets.empty()) writeFacetIndex(object->storageId, facets);
  object->state = ObjectState::LOCAL;
  object->repairReason.clear();
  ++rebuilds;
  ++object->rebuilds;
  lastRebuildMs = monotonicTimeMs() - start;
  object->lastRebuildMs = lastRebuildMs;
  log("rebuilt", "\"object\":" + quote(objectName) + ",\"ms\":" + std::to_string(lastRebuildMs) + ",\"manifest\":\"" +
          std::to_string(object->manifestRevision) + "\",\"snapshot\":\"" + std::to_string(object->snapshotSequence) +
          "\",\"fromSeq\":\"" + std::to_string(fromSeq) + "\"");
}

void DurableLog::removeObjectFiles(const Object& object) {
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
        out.push_back(std::move(record));
      }
    }
    at = until;
  }
  co_return out;
}

// What the peers of the shard still hold: every live member is asked (the peers of a shard are the first entries
// after it on the ring, but the ring has moved if the previous owner is gone), and the longest run without a gap is
// taken -- a peer holds a prefix of its owner's log, so the longest answer is the furthest the history reaches.
kj::Promise<std::vector<LogRecord>> DurableLog::fetchTails(Object& object) {
  std::vector<std::string> asked = peersOf(object.namespaceId, object.shard);
  for (const auto& [id, member] : membership.members) {
    if (id == settings.nodeId) continue;
    if (std::find(asked.begin(), asked.end(), id) == asked.end()) asked.push_back(id);
  }
  // Every peer holds a prefix of its owner's log, so the union of their answers is the longest prefix and has no
  // hole in it; a record missing from all of them was never confirmed to anybody.
  std::map<std::pair<uint64_t, uint64_t>, LogRecord> merged;
  for (const auto& peer : asked) {
    if (!hooks.fetchTail) break;
    DurableTailAsk ask;
    ask.namespaceId = object.namespaceId;
    ask.shard = object.shard;
    ask.shardEpoch = object.shardEpoch;
    try {
      ++tailFetches;
      auto answer = co_await hooks.fetchTail(peer, ask);
      if (!answer.refusal.empty()) {
        log("tail-refused", "\"peer\":" + quote(peer) + ",\"reason\":" + quote(answer.refusal));
        continue;
      }
      const auto wanted = object.namespaceId + "/" + object.objectId;
      uint32_t mine = 0;
      for (auto& record : answer.records) {
        if (record.objectName != wanted) continue;
        ++mine;
        merged[{record.shardEpoch, record.nodeSeq}] = std::move(record);
      }
      log("tail-taken", "\"peer\":" + quote(peer) + ",\"records\":" + std::to_string(answer.count) +
              ",\"ours\":" + std::to_string(mine) + ",\"lastSeq\":\"" + std::to_string(answer.lastSeq) +
              "\",\"more\":" + (answer.more ? "true" : "false"));
    } catch (const kj::Exception& error) {
      log("tail-failed", "\"peer\":" + quote(peer) + ",\"error\":" + quote(std::string(error.getDescription().cStr())));
    }
  }
  std::vector<LogRecord> out;
  out.reserve(merged.size());
  for (auto& [position, record] : merged) out.push_back(std::move(record));
  co_return out;
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
    if (drop.contains({record.shardEpoch, record.nodeSeq})) continue;
    ordered.insert({{record.shardEpoch, record.nodeSeq}, std::move(record)});
  }
  std::map<uint32_t, uint64_t> covered;  // facet -> the sequence of the snapshot's shard epoch it already holds
  for (const auto& facet : facets) covered[facet.facetId] = facet.upToSeq;
  std::map<uint32_t, std::unique_ptr<DatabaseReplay>> replays;
  uint64_t applied = 0, skipped = 0;
  for (const auto& [position, record] : ordered) {
    auto found = covered.find(record.facetId);
    const bool inSnapshot = found != covered.end() &&
        (record.shardEpoch < snapshotShardEpoch ||
            (record.shardEpoch == snapshotShardEpoch && record.nodeSeq <= found->second));
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
    facet.checkpointedSeq = 0;
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

void DurableLog::shardGone(const std::string& namespaceId, uint32_t shard, const std::string& reason) {
  uint32_t count = 0;
  for (auto& [name, object] : objects) {
    if (object.namespaceId != namespaceId || object.shard != shard) continue;
    if (object.state == ObjectState::LOCAL || object.state == ObjectState::REPAIR) object.state = ObjectState::GONE;
    ++count;
  }
  if (count) log("shard-gone", "\"namespaceId\":" + quote(namespaceId) + ",\"shard\":" + std::to_string(shard) +
          ",\"objects\":" + std::to_string(count) + ",\"reason\":" + quote(reason));
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
  if (partial) {
    // A piece of a record the next calls complete: held, counted, and taken only when the record is whole.
    if (incomingBytes + bytes.size() > settings.config.peerBufferBytes ||
        buffer.fragment.size() + bytes.size() > RECORD_BYTES_LIMIT) {
      ++bufferRefusals;
      incomingBytes -= std::min<uint64_t>(incomingBytes, buffer.fragment.size());
      buffer.bytes -= std::min<uint64_t>(buffer.bytes, buffer.fragment.size());
      buffer.fragment.clear();
      result.accepted = buffer.acceptedSeq;
      result.refusal = "full: this node buffers durableObjects.peerBufferBytes for the owners it is a peer of";
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
    if (encoded.size() > RECORD_BYTES_LIMIT) {
      result.accepted = buffer.acceptedSeq;
      result.refusal = "a record is larger than this node takes";
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
    if (record->nodeSeq <= buffer.droppedTo) continue;
    auto [entry, added] = buffer.held.insert({record->nodeSeq, encoded});
    if (added) {
      buffer.bytes += encoded.size();
      incomingBytes += encoded.size();
      ++buffer.records;
    }
    buffer.acceptedSeq = std::max(buffer.acceptedSeq, record->nodeSeq);
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
  buffer.droppedTo = std::max(buffer.droppedTo, upTo);
}

void DurableLog::dropIncoming(uint64_t streamId, const std::vector<uint64_t>& sequences) {
  auto found = incoming.find(streamId);
  if (found == incoming.end()) return;
  auto& buffer = found->second;
  for (auto sequence : sequences) {
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

// What this node holds of another node's log for one shard: the records above `afterSeq`, without a gap, so that the
// new owner can tell how far the history reaches.
DurableTailAnswer DurableLog::tailFor(const DurableTailAsk& ask) {
  DurableTailAnswer answer;
  if (!enabled()) {
    answer.refusal = "this node keeps no Durable Object log";
    return answer;
  }
  ++tailAnswers;
  std::vector<std::string> out;
  size_t bytes = 0;
  for (const auto& [streamId, buffer] : incoming) {
    if (!ask.ownerNodeId.empty() && buffer.nodeId != ask.ownerNodeId) continue;
    if (ask.ownerNodeEpoch && buffer.nodeEpoch != ask.ownerNodeEpoch) continue;
    uint64_t want = ask.afterSeq;
    answer.droppedTo = std::max(answer.droppedTo, buffer.droppedTo);
    for (const auto& [sequence, encoded] : buffer.held) {
      if (sequence <= want) continue;
      std::string reason;
      auto record = decodeLogRecord(encoded, &reason);
      if (!record) break;
      if (record->shard != ask.shard) continue;
      if (!ask.namespaceId.empty() && record->objectName.compare(0, ask.namespaceId.size(), ask.namespaceId) != 0)
        continue;
      if (bytes + encoded.size() > TAIL_ANSWER_BYTES) {
        answer.more = true;
        break;
      }
      if (!answer.firstSeq) answer.firstSeq = sequence;
      answer.lastSeq = sequence;
      bytes += encoded.size();
      answer.records.push_back(std::move(*record));
      ++answer.count;
    }
  }
  std::sort(answer.records.begin(), answer.records.end(), [](const LogRecord& a, const LogRecord& b) {
    return std::tie(a.nodeEpoch, a.nodeSeq) < std::tie(b.nodeEpoch, b.nodeSeq);
  });
  return answer;
}

// ---- The state endpoint ----

std::string DurableLog::json() const {
  std::ostringstream out;
  const auto now = monotonicTimeMs();
  uint64_t unconfirmed = 0, unconfirmedBytes = 0;
  for (const auto& [sequence, flight] : inFlight) {
    ++unconfirmed;
    unconfirmedBytes += flight.bytes;
  }
  uint32_t local = 0, rebuildingNow = 0, repairing = 0;
  for (const auto& [name, object] : objects) {
    if (object.state == ObjectState::LOCAL) ++local;
    else if (object.state == ObjectState::REBUILDING) ++rebuildingNow;
    else if (object.state == ObjectState::REPAIR) ++repairing;
  }
  out << "{\"enabled\":" << (enabled() ? "true" : "false") << ",\"nodeEpoch\":\"" << nodeEpoch
      << "\",\"peers\":{\"want\":" << peerCount() << ",\"configured\":" << settings.config.peers << ",\"streams\":[";
  bool first = true;
  for (const auto& [id, peer] : peers) {
    if (!first) out << ",";
    first = false;
    out << "{\"nodeId\":" << quote(id) << ",\"open\":" << ((peer.sink != kj::none) ? "true" : "false")
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
      << ",\"rebuilds\":" << rebuilds << ",\"rebuildFailures\":" << rebuildFailures << ",\"repairs\":" << repairs
      << ",\"recoverTimeouts\":" << recoverTimeouts << ",\"lastRebuildMs\":" << lastRebuildMs << ",\"lastError\":"
      << quote(lastRebuildError) << ",\"tooLargeAnswers\":" << overLimitAnswers << "},\"disk\":{\"trusted\":"
      << (diskTrusted ? "true" : "false") << ",\"note\":" << quote(startupNote) << ",\"limitBytes\":\""
      << diskLimitBytes << "\",\"usedBytes\":\"" << diskUsedBytes << "\",\"filesDropped\":" << filesDropped
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
  out << "],\"settings\":{\"confirmTimeoutMs\":" << settings.config.confirmTimeoutMs << ",\"recoverTimeoutMs\":"
      << settings.config.recoverTimeoutMs << ",\"segmentIntervalMs\":" << settings.config.segmentIntervalMs
      << ",\"segmentBytes\":\"" << settings.config.segmentBytes << "\",\"snapshotBytes\":\""
      << settings.config.snapshotBytes << "\",\"objectBytes\":\"" << settings.config.objectBytes
      << "\",\"checkpointBytes\":\"" << settings.config.checkpointBytes << "\",\"recoverParallelism\":"
      << settings.config.recoverParallelism << "}}";
  return out.str();
}

}  // namespace coworkerd
