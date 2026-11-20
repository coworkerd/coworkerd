#pragma once

// A Worker's log line as the node hands it to its log drainer, and how the drainer cuts it. Tenant lines:
// {"workerId":"..","versionId":"..","level":"..","message":"<JSON string>"} (fields in this order), with
// ,"messageBytes":N before the end when the message was already cut by its server (its full size). The workerId is
// a Worker's full name, <tenant>/<worker>, the versionId a deployment's safe name; the message stays as its JSON text.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace coworkerd {

inline bool nameCharacter(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.';
}
struct TenantLine { std::string_view workerId, versionId, level, message; uint64_t messageBytes = 0; };
inline std::optional<TenantLine> parseTenantLine(std::string_view json) {
  TenantLine line;
  auto literal = [&](std::string_view expected) {
    if (!json.starts_with(expected)) return false;
    json.remove_prefix(expected.size());
    return true;
  };
  auto name = [&](std::string_view& out, size_t limit) {
    size_t n = 0;
    while (n < json.size() && n <= limit && nameCharacter(json[n])) ++n;
    if (n == 0 || n > limit) return false;
    out = json.substr(0, n);
    json.remove_prefix(n);
    return true;
  };
  // A full name: the tenant, one '/', the Worker's name.
  auto fullName = [&](std::string_view& out) {
    std::string_view tenant, worker;
    if (!name(tenant, 63) || !literal("/") || !name(worker, 255)) return false;
    out = std::string_view(tenant.data(), tenant.size() + 1 + worker.size());
    return true;
  };
  if (!literal("{\"workerId\":\"") || !fullName(line.workerId) || !literal("\",\"versionId\":\"") ||
      !name(line.versionId, 255) || !literal("\",\"level\":\"") || !name(line.level, 16) || !literal("\",\"message\":\"")) {
    return std::nullopt;
  }
  bool escaped = false;
  size_t end = 0;
  for (; end < json.size(); ++end) {
    const char c = json[end];
    if (escaped) { escaped = false; continue; }
    if (c == '\\') { escaped = true; continue; }
    if (c == '"') break;
    if (static_cast<unsigned char>(c) < 0x20) return std::nullopt;
  }
  if (end >= json.size()) return std::nullopt;
  line.message = json.substr(0, end);
  json.remove_prefix(end + 1);
  // A message its server already cut carries its full size (the threaded server forwards at most 4,096 bytes).
  if (literal(",\"messageBytes\":")) {
    if (json.empty() || json.size() > 21) return std::nullopt;
    uint64_t bytes = 0;
    size_t digits = 0;
    while (digits < json.size() && json[digits] >= '0' && json[digits] <= '9') {
      if (bytes > (UINT64_MAX - 9) / 10) return std::nullopt;
      bytes = bytes * 10 + uint64_t(json[digits++] - '0');
    }
    if (digits == 0) return std::nullopt;
    json.remove_prefix(digits);
    line.messageBytes = bytes;
  }
  if (json != "}") return std::nullopt;
  return line;
}
// The longest prefix of a JSON string's text within `limit` bytes that ends between two characters:
// never inside an escape sequence or a UTF-8 sequence.
inline size_t jsonTextCut(std::string_view text, size_t limit) {
  size_t at = 0;
  while (at < text.size()) {
    const auto c = static_cast<unsigned char>(text[at]);
    size_t unit = 1;
    if (c == '\\') unit = at + 1 < text.size() && text[at + 1] == 'u' ? 6 : 2;
    else if (c >= 0xf0) unit = 4;
    else if (c >= 0xe0) unit = 3;
    else if (c >= 0xc0) unit = 2;
    if (at + unit > limit) break;
    at += unit;
  }
  return at;
}
// What the drainer keeps of a line's message: at most maxLineBytes of its JSON text, cut between two characters; the
// line is marked truncated when that cut something or its server had already (then with its full size). A server
// sends messageBytes only for a message it cut, so its presence is the mark: its value counts raw bytes, which can be
// fewer than the escaped text kept (v138 §8.5).
struct TenantLineKeep { size_t bytes; bool truncated; uint64_t messageBytes; };
inline TenantLineKeep tenantLineKeep(const TenantLine& line, size_t maxLineBytes) {
  const auto size = line.message.size();
  const auto bytes = size > maxLineBytes ? jsonTextCut(line.message, maxLineBytes) : size;
  const bool earlier = line.messageBytes > 0;
  return {bytes, bytes < size || earlier, earlier ? line.messageBytes : uint64_t(size)};
}

// A tenant line for the drainer (the message already JSON-escaped); messageBytes when its server cut the message.
inline std::string tenantLineText(std::string_view workerId, std::string_view versionId, std::string_view level,
    std::string_view escapedMessage, uint64_t messageBytes = 0) {
  std::string line = "{\"workerId\":\"";
  line.append(workerId).append("\",\"versionId\":\"").append(versionId).append("\",\"level\":\"").append(level)
      .append("\",\"message\":\"").append(escapedMessage).append("\"");
  if (messageBytes) line.append(",\"messageBytes\":").append(std::to_string(messageBytes));
  return line.append("}\n");
}

}  // namespace coworkerd
