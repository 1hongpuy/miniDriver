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
        "--sizes", "4MiB,1GiB", "--runs", "3", "--concurrency", "4", "--chunk-window", "2", "--global-chunk-budget", "2", "--fixture-settle-ms", "9000", "--remote-dir", "/benchmark"};
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
    MINIKV_CHECK(options.fixtureSettleMs == 9000);
    MINIKV_CHECK(options.remoteDir == "/benchmark");

    const std::vector<std::string> checksum{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/minikv-bench",
        "--upload-checksum", "sha256"};
    MINIKV_CHECK(miniKV::benchmark::parseBenchmarkOptions(checksum, options, error));
    MINIKV_CHECK(options.uploadChecksumType == "sha256");

    const std::vector<std::string> invalidChecksum{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/minikv-bench",
        "--upload-checksum", "md5"};
    MINIKV_CHECK(!miniKV::benchmark::parseBenchmarkOptions(invalidChecksum, options, error));

    const std::vector<std::string> readProfile{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/minikv-bench",
        "--mode", "download", "--read-profile", "hot-object",
        "--connection-mode", "keep-alive", "--requests-per-worker", "20"};
    MINIKV_CHECK(miniKV::benchmark::parseBenchmarkOptions(readProfile, options, error));
    MINIKV_CHECK(options.readProfile == miniKV::benchmark::BenchmarkReadProfile::kHotObject);
    MINIKV_CHECK(options.keepAlive);
    MINIKV_CHECK(options.requestsPerWorker == 20);

    const std::vector<std::string> transportOnly{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/minikv-bench",
        "--mode", "download", "--download-verification", "transport-only"};
    MINIKV_CHECK(miniKV::benchmark::parseBenchmarkOptions(transportOnly, options, error));
    MINIKV_CHECK(options.transportOnlyDownload);

    const std::vector<std::string> slowReaders{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/minikv-bench",
        "--mode", "download", "--read-profile", "hot-object", "--concurrency", "8",
        "--slow-reader-workers", "4", "--slow-reader-bytes-per-sec", "2MiB"};
    MINIKV_CHECK(miniKV::benchmark::parseBenchmarkOptions(slowReaders, options, error));
    MINIKV_CHECK(options.slowReaderWorkers == 4);
    MINIKV_CHECK(options.slowReaderBytesPerSecond == 2ULL * 1024ULL * 1024ULL);

    const std::vector<std::string> invalidSlowReaders{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/minikv-bench",
        "--mode", "download", "--concurrency", "4", "--slow-reader-workers", "4",
        "--slow-reader-bytes-per-sec", "2MiB"};
    MINIKV_CHECK(!miniKV::benchmark::parseBenchmarkOptions(invalidSlowReaders, options, error));

    const std::vector<std::string> invalidReadProfileMode{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/minikv-bench",
        "--mode", "upload", "--read-profile", "mixed-size"};
    MINIKV_CHECK(!miniKV::benchmark::parseBenchmarkOptions(invalidReadProfileMode, options, error));

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
    const std::vector<std::string> invalidFixtureSettle{
        "local", "--gateway", "127.0.0.1:18081", "--work-dir", "/tmp/minikv-bench",
        "--fixture-settle-ms", "60001"};
    MINIKV_CHECK(!miniKV::benchmark::parseBenchmarkOptions(invalidFixtureSettle, options, error));
    std::cout << "PASS: benchmark CLI parsing\n";
    return 0;
}
