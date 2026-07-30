#include "benchmark/BenchmarkCli.hpp"
#include "benchmark/BenchmarkRunner.hpp"

#include <iostream>

int main(int argc, char** argv) {
    std::vector<std::string> args;
    for (int index = 1; index < argc; ++index) args.emplace_back(argv[index]);
    miniKV::benchmark::BenchmarkOptions options;
    std::string error;
    if (!miniKV::benchmark::parseBenchmarkOptions(args, options, error)) {
        std::cerr << error << '\n' << miniKV::benchmark::benchmarkUsage() << '\n';
        return 2;
    }
    return miniKV::benchmark::BenchmarkRunner(std::move(options)).run(std::cout);
}
