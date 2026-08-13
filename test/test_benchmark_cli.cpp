#include "benchmark/BenchmarkCli.hpp"
#include "TestCheck.hpp"
#include <iostream>
#include <string>
#include <vector>

int main() {
    miniKV::benchmark::BenchmarkOptions options;
    std::string error;
    const std::vector<std::string> valid{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/minikv-bench",
        "--sizes", "4MiB,1GiB", "--runs", "3", "--concurrency", "4", "--chunk-window", "2", "--global-chunk-budget", "2", "--remote-dir", "/benchmark"};
    MINIKV_CHECK(miniKV::benchmark::parseBenchmarkOptions(valid, options, error));
    MINIKV_CHECK(options.gateway.host == "127.0.0.1");
    MINIKV_CHECK(options.gateway.port == 18081);
    MINIKV_CHECK(options.sizes.size() == 2);
    MINIKV_CHECK(options.sizes[0] == 4ULL * 1024ULL * 1024ULL);
    MINIKV_CHECK(options.sizes[1] == 1024ULL * 1024ULL * 1024ULL);
    MINIKV_CHECK(options.runs == 3);
    MINIKV_CHECK(options.concurrency == 4);
    MINIKV_CHECK(options.chunkWindow == 2);
    MINIKV_CHECK(options.globalChunkBudget == 2);
    MINIKV_CHECK(options.remoteDir == "/benchmark");

    const std::vector<std::string> mixed{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/minikv-bench",
        "--mode", "mixed", "--concurrency", "2"};
    MINIKV_CHECK(miniKV::benchmark::parseBenchmarkOptions(mixed, options, error));
    MINIKV_CHECK(options.mode == miniKV::benchmark::BenchmarkMode::kMixed);

    const std::vector<std::string> invalidMixed{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/bench",
        "--mode", "mixed", "--concurrency", "1"};
    MINIKV_CHECK(!miniKV::benchmark::parseBenchmarkOptions(invalidMixed, options, error));
    const std::vector<std::string> invalidMode{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/minikv-bench",
        "--mode", "invalid"};
    MINIKV_CHECK(!miniKV::benchmark::parseBenchmarkOptions(invalidMode, options, error));

    const std::vector<std::string> missingWorkDir{"local", "--gateway", "127.0.0.1:18081"};
    MINIKV_CHECK(!miniKV::benchmark::parseBenchmarkOptions(missingWorkDir, options, error));
    MINIKV_CHECK(!error.empty());
    const std::vector<std::string> invalidGateway{
        "local", "--gateway", "127.0.0.1", "--work-dir", "/tmp/minikv-bench"};
    MINIKV_CHECK(!miniKV::benchmark::parseBenchmarkOptions(invalidGateway, options, error));
    MINIKV_CHECK(!error.empty());
    const std::vector<std::string> zeroConcurrency{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/minikv-bench",
        "--concurrency", "0"};
    MINIKV_CHECK(!miniKV::benchmark::parseBenchmarkOptions(zeroConcurrency, options, error));
    MINIKV_CHECK(!error.empty());
    const std::vector<std::string> invalidWindow{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/minikv-bench",
        "--chunk-window", "3"};
    MINIKV_CHECK(!miniKV::benchmark::parseBenchmarkOptions(invalidWindow, options, error));
    std::cout << "PASS: benchmark CLI parsing\n";
    return 0;
}
