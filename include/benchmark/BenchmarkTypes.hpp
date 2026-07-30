#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace miniKV::benchmark {

struct Endpoint {
    std::string host;
    uint16_t port = 0;
};

struct BenchmarkOptions {
    Endpoint gateway;
    std::filesystem::path workDir;
    std::vector<uint64_t> sizes;
    uint32_t runs = 3;
    uint32_t concurrency = 1;
    std::string remoteDir = "/benchmark";
};

struct RunRecord {
    std::string runId;
    uint64_t sizeBytes = 0;
    uint32_t chunkCount = 0;
    double uploadMs = 0;
    double downloadMs = 0;
    bool uploadOk = false;
    bool downloadOk = false;
    std::string error;
};

struct SizeSummary {
    uint64_t sizeBytes = 0;
    uint32_t requestedRuns = 0;
    uint32_t successCount = 0;
    double uploadMinMs = 0;
    double uploadMedianMs = 0;
    double uploadP95Ms = 0;
    double uploadP99Ms = 0;
    double uploadMeanMs = 0;
    double downloadMinMs = 0;
    double downloadMedianMs = 0;
    double downloadP95Ms = 0;
    double downloadP99Ms = 0;
    double downloadMeanMs = 0;
};

bool parseSize(std::string_view text, uint64_t& bytes);
bool parseEndpoint(std::string_view text, Endpoint& endpoint);
double mibPerSecond(uint64_t bytes, double elapsedMs);
SizeSummary summarize(uint64_t sizeBytes, uint32_t requestedRuns,
                      const std::vector<RunRecord>& records);
bool writeReports(const BenchmarkOptions& options, const std::vector<RunRecord>& records,
                  const std::vector<SizeSummary>& summaries, std::error_code& error);

}  // namespace miniKV::benchmark
