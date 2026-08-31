#include "http/HttpRange.hpp"
#include "TestCheck.hpp"

int main()
{
    using miniKV::http::ByteRange;
    using miniKV::http::RangeParseStatus;
    ByteRange range;
    MINIKV_CHECK(parseSingleByteRange("", 100, range) == RangeParseStatus::kNone);
    MINIKV_CHECK(parseSingleByteRange("bytes=0-0", 100, range) == RangeParseStatus::kSatisfiable);
    MINIKV_CHECK(range.start == 0 && range.length == 1);
    MINIKV_CHECK(parseSingleByteRange("bytes=99-", 100, range) == RangeParseStatus::kSatisfiable);
    MINIKV_CHECK(range.start == 99 && range.length == 1);
    MINIKV_CHECK(parseSingleByteRange("bytes=10-999", 100, range) == RangeParseStatus::kSatisfiable);
    MINIKV_CHECK(range.start == 10 && range.length == 90);
    MINIKV_CHECK(parseSingleByteRange("bytes=100-", 100, range) == RangeParseStatus::kUnsatisfiable);
    MINIKV_CHECK(parseSingleByteRange("bytes=9-8", 100, range) == RangeParseStatus::kUnsatisfiable);
    MINIKV_CHECK(parseSingleByteRange("bytes=-10", 100, range) == RangeParseStatus::kInvalid);
    MINIKV_CHECK(parseSingleByteRange("bytes=0-1,3-4", 100, range) == RangeParseStatus::kInvalid);
    MINIKV_CHECK(parseSingleByteRange("items=0-1", 100, range) == RangeParseStatus::kInvalid);
    MINIKV_CHECK(parseSingleByteRange("bytes= 0-1", 100, range) == RangeParseStatus::kInvalid);
    return 0;
}
