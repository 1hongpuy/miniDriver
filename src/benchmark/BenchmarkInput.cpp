#include "benchmark/BenchmarkInput.hpp"

namespace miniKV::benchmark {

void fillBenchmarkBytes(char* output, size_t size, uint64_t absoluteOffset,
                        uint32_t runIndex)
{
    uint64_t state = absoluteOffset ^ (static_cast<uint64_t>(runIndex) << 32) ^
                     0x9e3779b97f4a7c15ULL;
    for (size_t index = 0; index < size; ++index) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        output[index] = static_cast<char>(state >> 56);
    }
}

}  // namespace miniKV::benchmark
