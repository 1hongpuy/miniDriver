#include "benchmark/BenchmarkInput.hpp"
#include "TestCheck.hpp"

#include <array>
#include <iostream>

int main()
{
    std::array<char, 64 * 1024> first{};
    std::array<char, 64 * 1024> firstAgain{};
    std::array<char, 64 * 1024> later{};

    miniKV::benchmark::fillBenchmarkBytes(first.data(), first.size(), 0, 0);
    miniKV::benchmark::fillBenchmarkBytes(firstAgain.data(), firstAgain.size(), 0, 0);
    miniKV::benchmark::fillBenchmarkBytes(later.data(), later.size(), 4ULL * 1024ULL * 1024ULL, 0);

    MINIKV_CHECK(first == firstAgain);
    MINIKV_CHECK(first != later);
    std::cout << "PASS: benchmark input is deterministic and offset-dependent\n";
    return 0;
}
