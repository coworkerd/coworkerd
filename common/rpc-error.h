#pragma once

#include <kj/exception.h>
#include <kj/string.h>
#include <string>

namespace coworkerd {
// Assigned by the v76 RPC error-detail contract. Keep in sync with workerd's
// jsg/coworkerd-rpc-code.h in patches/workerd/0016-rpc-code-parent-death.patch.
inline constexpr kj::Exception::DetailTypeId COWORKERD_CODE_DETAIL_ID = 0xc6f4a09d8b3e2175ull;

inline kj::Exception tagRpcError(kj::Exception error, kj::StringPtr code) {
  error.setDetail(COWORKERD_CODE_DETAIL_ID, kj::heapArray(code.asBytes()));
  return error;
}

inline std::string rpcFailureCode(const kj::Exception& error) {
  KJ_IF_SOME(code, error.getDetail(COWORKERD_CODE_DETAIL_ID)) {
    return std::string(reinterpret_cast<const char*>(code.begin()), code.size());
  }
  return "EXECUTION_FAILED";
}
}  // namespace coworkerd
