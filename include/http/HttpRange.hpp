#pragma once

#include <cstdint>
#include <string>

namespace miniKV::http {

enum class RangeParseStatus { kNone, kSatisfiable, kInvalid, kUnsatisfiable };

struct ByteRange {
    uint64_t start = 0;
    uint64_t length = 0;
};

RangeParseStatus parseSingleByteRange(const std::string& headerValue,
                                      uint64_t resourceSize,
                                      ByteRange& out);

}  // namespace miniKV::http
