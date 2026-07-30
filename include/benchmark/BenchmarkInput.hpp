#pragma once

#include <cstddef>
#include <cstdint>

namespace miniKV::benchmark {

// Fills a deterministic but position-dependent test pattern. Different file
// offsets produce different chunk content, so content-addressed DataNodes do
// not turn a large benchmark into a single deduplicated physical write.
void fillBenchmarkBytes(char* output, size_t size, uint64_t absoluteOffset,
                        uint32_t runIndex);

}  // namespace miniKV::benchmark
