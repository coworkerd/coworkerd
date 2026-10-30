#pragma once

#include <string>

namespace coworkerd::reasons {

inline const std::string DEFINITION_UNKNOWN = "definition unknown";
inline const std::string DEFINITION_INCONSISTENT = "definition inconsistent";
inline const std::string CODE_UNAVAILABLE = "code unavailable";
inline const std::string ASSETS_UNAVAILABLE = "assets unavailable";
inline const std::string NOT_IN_MEMBERSHIP = "not in the membership table";
inline const std::string ANOTHER_DEPLOYMENT = "another deployment intervened";

inline std::string detail(const std::string& prefix, const std::string& text) {
  return prefix + ": " + text;
}

}  // namespace coworkerd::reasons
