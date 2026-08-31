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
    MINIKV_CHECK(miniKV::benchmark::parseSize("64KiB", bytes));
    MINIKV_CHECK(bytes == 64ULL * 1024ULL);
    MINIKV_CHECK(miniKV::benchmark::parseSize("4MiB", bytes));
    MINIKV_CHECK(bytes == 4ULL * 1024ULL * 1024ULL);
    MINIKV_CHECK(miniKV::benchmark::parseSize("1GiB", bytes));
    MINIKV_CHECK(bytes == 1024ULL * 1024ULL * 1024ULL);
    MINIKV_CHECK(!miniKV::benchmark::parseSize("4MB", bytes));

    auto makeRecord = [](const std::string& runId, double uploadMs, double downloadMs,
                         bool uploadOk, bool downloadOk, const std::string& error) {
        RunRecord record;
        record.runId = runId;
        record.sizeBytes = 4;
        record.chunkCount = 1;
        record.uploadMs = uploadMs;
        record.downloadMs = downloadMs;
        record.uploadOk = uploadOk;
        record.downloadOk = downloadOk;
        record.error = error;
        return record;
    };
    std::vector<RunRecord> records{
        makeRecord("run-a", 10.0, 20.0, true, true, ""),
        makeRecord("run-b", 30.0, 40.0, true, true, ""),
        makeRecord("run-c", 999.0, 999.0, false, false, "HTTP 500"),
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
    options.chunkWindow = 2;
    options.globalChunkBudget = 2;
    std::error_code error;
    std::filesystem::remove_all(options.workDir, error);
    std::filesystem::create_directories(options.workDir, error);
    MINIKV_CHECK(!error);

    MINIKV_CHECK(miniKV::benchmark::writeReports(options, records, {summary}, error));
    MINIKV_CHECK(!error);
    std::ifstream csv(options.workDir / "runs.csv");
    std::string line;
    std::getline(csv, line);
    MINIKV_CHECK(line == "run_id,operation,reader_class,object_id,object_version,file_hash,input_hash,size_bytes,upload_ms,upload_mib_per_s,download_ms,download_mib_per_s,chunk_count,data_connection_opens,data_requests,data_connection_reuses,upload_ok,download_ok,error");
    std::getline(csv, line);
    MINIKV_CHECK(!line.empty());
    std::getline(csv, line);
    MINIKV_CHECK(!line.empty());
    std::ifstream summaryCsv(options.workDir / "summary.csv");
    std::getline(summaryCsv, line);
    MINIKV_CHECK(line.find("upload_workers_per_round") != std::string::npos);
    std::getline(summaryCsv, line);
    MINIKV_CHECK(line.find("end-to-end,independent,close,crc32c,1,0,0,3,1,1,0,3,0,2,2,0,4,3") == 0);
    std::ifstream json(options.workDir / "summary.json");
    const std::string report((std::istreambuf_iterator<char>(json)), {});
    MINIKV_CHECK(report.find("\"gateway\":\"127.0.0.1:18081\"") != std::string::npos);
    MINIKV_CHECK(report.find("\"sizes\":[4]") != std::string::npos);
    MINIKV_CHECK(report.find("\"chunkWindow\":2") != std::string::npos);
    MINIKV_CHECK(report.find("\"globalChunkBudget\":2") != std::string::npos);
    MINIKV_CHECK(report.find("\"uploadChecksum\":\"crc32c\"") != std::string::npos);
    MINIKV_CHECK(report.find("\"fixtureSettleMs\":0") != std::string::npos);
    MINIKV_CHECK(report.find("\"readProfile\":\"independent\"") != std::string::npos);
    MINIKV_CHECK(report.find("\"connectionMode\":\"close\"") != std::string::npos);
    MINIKV_CHECK(report.find("\"requestsPerWorker\":1") != std::string::npos);
    MINIKV_CHECK(report.find("\"slowReaderWorkers\":0") != std::string::npos);
    MINIKV_CHECK(report.find("\"slowReaderBytesPerSecond\":0") != std::string::npos);
    MINIKV_CHECK(report.find("\"uploadWorkersPerRound\":1") != std::string::npos);
    MINIKV_CHECK(report.find("\"downloadWorkersPerRound\":0") != std::string::npos);
    MINIKV_CHECK(report.find("\"successCount\":2") != std::string::npos);

    options.mode = miniKV::benchmark::BenchmarkMode::kMixed;
    options.concurrency = 5;
    options.runs = 2;
    MINIKV_CHECK(miniKV::benchmark::writeReports(options, records, {summary}, error));
    std::ifstream mixedJson(options.workDir / "summary.json");
    const std::string mixedReport((std::istreambuf_iterator<char>(mixedJson)), {});
    MINIKV_CHECK(mixedReport.find("\"uploadWorkersPerRound\":3") != std::string::npos);
    MINIKV_CHECK(mixedReport.find("\"downloadWorkersPerRound\":2") != std::string::npos);
    MINIKV_CHECK(mixedReport.find("\"uploadSamplesRequested\":6") != std::string::npos);
    MINIKV_CHECK(mixedReport.find("\"downloadSamplesRequested\":4") != std::string::npos);
    std::filesystem::remove_all(options.workDir, error);

    std::cout << "PASS: benchmark size parsing, statistics, and reports\n";
    return 0;
}
