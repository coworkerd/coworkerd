#include "json.h"
#include "support.h"

#include <capnp/message.h>
#include <kj/encoding.h>

#include <algorithm>
#include <cctype>

namespace coworkerd {
namespace json {
namespace {
void appendQuoted(std::string& output, kj::StringPtr value) {
  output += '"';
  appendJsonEscaped(output, {value.begin(), value.size()});
  output += '"';
}

void appendCompact(std::string& output, Json::Reader value) {
  switch (value.which()) {
    case Json::NULL_:
      output += "null";
      break;
    case Json::BOOLEAN:
      output += value.getBoolean() ? "true" : "false";
      break;
    case Json::NUMBER: {
      const auto number = kj::toCharSequence(value.getNumber());
      output.append(number.begin(), number.size());
      break;
    }
    case Json::STRING:
      appendQuoted(output, value.getString());
      break;
    case Json::ARRAY: {
      output += '[';
      const auto elements = value.getArray();
      for (auto i : kj::indices(elements)) {
        if (i > 0) output += ',';
        appendCompact(output, elements[i]);
      }
      output += ']';
      break;
    }
    case Json::OBJECT: {
      output += '{';
      const auto fields = value.getObject();
      for (auto i : kj::indices(fields)) {
        if (i > 0) output += ',';
        appendQuoted(output, fields[i].getName());
        output += ':';
        appendCompact(output, fields[i].getValue());
      }
      output += '}';
      break;
    }
    case Json::CALL:
    case Json::RAW: {
      const auto encoded = capnp::JsonCodec().encodeRaw(value);
      output.append(encoded.begin(), encoded.size());
      break;
    }
  }
}
}  // namespace

std::string encodeCompact(Json::Reader value) {
  // The invocation descriptor is written on every dispatch. Avoid allocating a
  // string tree for its individual fields, then flattening the tree.
  std::string output;
  output.reserve(1024);
  appendCompact(output, value);
  return output;
}

std::string quote(const std::string& value) {
  capnp::MallocMessageBuilder message;
  auto json = message.initRoot<Json>();
  json.setString(value.c_str());
  return capnp::JsonCodec().encodeRaw(json).cStr();
}
Json::Reader field(Json::Reader object, const char* name) {
  KJ_REQUIRE(object.isObject(), "expected a JSON object");
  for (auto entry : object.getObject()) if (entry.getName() == name) return entry.getValue();
  return Json::Reader();
}
std::string text(Json::Reader object, const char* name) {
  auto value = field(object, name);
  KJ_REQUIRE(value.isString(), "expected a JSON string", name);
  return value.getString().cStr();
}
uint64_t integer(Json::Reader object, const char* name) {
  auto value = field(object, name);
  if (value.isNull()) return 0;
  KJ_REQUIRE(value.isString(), "expected an integer string", name);
  std::string string = value.getString().cStr();
  KJ_REQUIRE(!string.empty() && std::all_of(string.begin(), string.end(),
      [](unsigned char c) { return std::isdigit(c); }), "invalid integer string", name);
  size_t end = 0;
  auto result = std::stoull(string, &end);
  KJ_REQUIRE(end == string.size(), "invalid integer string", name);
  return result;
}
bool boolean(Json::Reader object, const char* name) {
  auto value = field(object, name);
  return value.isBoolean() && value.getBoolean();
}
std::string base64(const std::string& value) {
  return kj::encodeBase64(kj::arrayPtr(reinterpret_cast<const kj::byte*>(value.data()), value.size())).cStr();
}
}  // namespace json
}  // namespace coworkerd
