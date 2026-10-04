#include "http/HttpRange.hpp"

#include <algorithm>
#include <limits>

namespace miniKV::http {
namespace {

bool parseUnsigned(const std::string& value, uint64_t& out)
{
    if(value.empty()) return false;
    uint64_t result = 0;
    for(const char ch : value) {
        if(ch < '0' || ch > '9') return false;
        const uint64_t digit = static_cast<uint64_t>(ch - '0');
        if(result > (std::numeric_limits<uint64_t>::max() - digit) / 10) return false;
        result = result * 10 + digit;
    }
    out = result;
    return true;
}

}  // namespace

RangeParseStatus parseSingleByteRange(const std::string& headerValue,
                                      uint64_t resourceSize,
                                      ByteRange& out)
{
    out = {};
    if(headerValue.empty()) return RangeParseStatus::kNone;
    constexpr const char* prefix = "bytes=";
    if(headerValue.rfind(prefix, 0) != 0 ||
       headerValue.find(',') != std::string::npos) {
        return RangeParseStatus::kInvalid;
    }
    const std::string value = headerValue.substr(6);
    const size_t dash = value.find('-');
    if(dash == std::string::npos || value.find('-', dash + 1) != std::string::npos) {
        return RangeParseStatus::kInvalid;
    }
    // RFC 9110 suffix byte-range-spec: "bytes=-N" means the final N bytes.
    // MOV/MP4 demuxers commonly use it to find an index stored near EOF.
    if(dash == 0) {
        uint64_t suffixLength = 0;
        if(!parseUnsigned(value.substr(1), suffixLength) || suffixLength == 0) {
            return RangeParseStatus::kInvalid;
        }
        if(resourceSize == 0) return RangeParseStatus::kUnsatisfiable;
        out.length = std::min(suffixLength, resourceSize);
        out.start = resourceSize - out.length;
        return RangeParseStatus::kSatisfiable;
    }

    uint64_t start = 0;
    if(!parseUnsigned(value.substr(0, dash), start)) return RangeParseStatus::kInvalid;
    if(resourceSize == 0 || start >= resourceSize) return RangeParseStatus::kUnsatisfiable;

    uint64_t end = resourceSize - 1;
    const std::string endText = value.substr(dash + 1);
    if(!endText.empty()) {
        if(!parseUnsigned(endText, end)) return RangeParseStatus::kInvalid;
        if(end < start) return RangeParseStatus::kUnsatisfiable;
        end = std::min(end, resourceSize - 1);
    }
    out.start = start;
    out.length = end - start + 1;
    return RangeParseStatus::kSatisfiable;
}

}  // namespace miniKV::http
