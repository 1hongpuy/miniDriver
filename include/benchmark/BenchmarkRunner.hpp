#pragma once

#include "benchmark/BenchmarkTypes.hpp"

#include <iosfwd>

namespace miniKV::benchmark {

class BenchmarkRunner {
public:
    explicit BenchmarkRunner(BenchmarkOptions options);

    int run(std::ostream& console);

private:
    BenchmarkOptions options_;
};

}  // namespace miniKV::benchmark
