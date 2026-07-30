#include "benchmark/BenchmarkTypes.hpp"
#include "TestCheck.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

using miniKV::benchmark::BenchmarkOptions;
using miniKV::benchmark::RunRecord;
using miniKV::benchmark::SizeSummary;

int main() {
    uint64_t bytes = 0;
    MINIKV_CHECK(miniKV::benchmark::parseSize("4MiB", bytes));
    MINIKV_CHECK(bytes == 4ULL * 1024ULL * 1024ULL);
    MINIKV_CHECK(miniKV::benchmark::parseSize("1GiB", bytes));
    MINIKV_CHECK(bytes == 1024ULL * 1024ULL * 1024ULL);
    MINIKV_CHECK(!miniKV::benchmark::parseSize("4MB", bytes));

    std::vector<RunRecord> records{
        {"run-a", 4, 1, 10.0, 20.0, true, true, ""},
        {"run-b", 4, 1, 30.0, 40.0, true, true, ""},
        {"run-c", 4, 1, 999.0, 999.0, false, false, "HTTP 500"},
    };
    const SizeSummary summary = miniKV::benchmark::summarize(4, 3, records);
    MINIKV_CHECK(summary.successCount == 2);
    MINIKV_CHECK(summary.uploadMinMs == 10.0);
    MINIKV_CHECK(summary.uploadMedianMs == 20.0);
    MINIKV_CHECK(summary.uploadP95Ms == 30.0);
    MINIKV_CHECK(summary.uploadP99Ms == 30.0);
    MINIKV_CHECK(summary.uploadMeanMs == 20.0);
    MINIKV_CHECK(summary.downloadMedianMs == 30.0);
    MINIKV_CHECK(summary.downloadP95Ms == 40.0);
    MINIKV_CHECK(summary.downloadP99Ms == 40.0);
    MINIKV_CHECK(miniKV::benchmark::mibPerSecond(4ULL * 1024ULL * 1024ULL, 2000.0) == 2.0);

    BenchmarkOptions options;
    options.gateway = {"127.0.0.1", 18081};
    options.workDir = std::filesystem::temp_directory_path() / "minikv-benchmark-types-test";
    options.sizes = {4};
    options.runs = 3;
    std::error_code error;
    std::filesystem::remove_all(options.workDir, error);
    std::filesystem::create_directories(options.workDir, error);
    MINIKV_CHECK(!error);

    MINIKV_CHECK(miniKV::benchmark::writeReports(options, records, {summary}, error));
    MINIKV_CHECK(!error);
    std::ifstream csv(options.workDir / "runs.csv");
    std::string line;
    std::getline(csv, line);
    MINIKV_CHECK(line == "run_id,size_bytes,upload_ms,upload_mib_per_s,download_ms,download_mib_per_s,chunk_count,upload_ok,download_ok,error");
    std::getline(csv, line);
    MINIKV_CHECK(!line.empty());
    std::getline(csv, line);
    MINIKV_CHECK(!line.empty());
    std::ifstream json(options.workDir / "summary.json");
    const std::string report((std::istreambuf_iterator<char>(json)), {});
    MINIKV_CHECK(report.find("\"gateway\":\"127.0.0.1:18081\"") != std::string::npos);
    MINIKV_CHECK(report.find("\"sizes\":[4]") != std::string::npos);
    MINIKV_CHECK(report.find("\"successCount\":2") != std::string::npos);
    std::filesystem::remove_all(options.workDir, error);

    std::cout << "PASS: benchmark size parsing, statistics, and reports\n";
    return 0;
}
