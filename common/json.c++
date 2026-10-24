#include "json.h"

#include <capnp/message.h>
#include <kj/encoding.h>

#include <algorithm>
#include <cctype>

namespace coworkerd {
namespace json {
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
