#pragma once

#include <capnp/compat/json.h>

#include <cstdint>
#include <string>

// Small JSON helpers both daemons use on Cap'n Proto's JSON values (capnp::JsonValue), for the records they read and
// write by hand: a quoted string, a field of an object, a string field, an integer written as a string of decimal digits,
// a boolean field, base64.
namespace coworkerd {
namespace json {
using Json = capnp::JsonValue;
std::string quote(const std::string& value);
// The field's value, or a null value when the object has no such field. Throws when `object` is not an object.
Json::Reader field(Json::Reader object, const char* name);
// A string field; throws (naming the field) when it is missing or not a string.
std::string text(Json::Reader object, const char* name);
// A field holding a non-negative integer as a string of decimal digits ("12"); 0 when it is missing or null. Throws
// (naming the field) on anything else.
uint64_t integer(Json::Reader object, const char* name);
// True only for a field that is the boolean true.
bool boolean(Json::Reader object, const char* name);
std::string base64(const std::string& value);
}  // namespace json
}  // namespace coworkerd
