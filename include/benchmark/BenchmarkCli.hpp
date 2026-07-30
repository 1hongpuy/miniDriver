#pragma once

#include "benchmark/BenchmarkTypes.hpp"

#include <string>
#include <vector>

namespace miniKV::benchmark {

bool parseBenchmarkOptions(const std::vector<std::string>& args,
                           BenchmarkOptions& options, std::string& error);
std::string benchmarkUsage();

}  // namespace miniKV::benchmark
