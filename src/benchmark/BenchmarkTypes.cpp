#include "benchmark/BenchmarkTypes.hpp"

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <limits>
#include <numeric>
#include <cmath>

namespace miniKV::benchmark {
namespace {

std::string csvEscape(const std::string& value) {
    std::string out = "\"";
    for (const char c : value) {
        if (c == '\"') out += "\"\"";
        else out += c;
    }
    return out + "\"";
}

double mean(const std::vector<double>& values) {
    return values.empty() ? 0.0
        : std::accumulate(values.begin(), values.end(), 0.0) / values.size();
}

double median(std::vector<double> values) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    const size_t middle = values.size() / 2;
    return values.size() % 2 == 0 ? (values[middle - 1] + values[middle]) / 2.0
                                  : values[middle];
}

double percentile(std::vector<double> values, double fraction) {
    if(values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    const size_t index = static_cast<size_t>(std::ceil(fraction * values.size())) - 1;
    return values[std::min(index, values.size() - 1)];
}

}

bool parseSize(std::string_view text, uint64_t& bytes) {
    const size_t suffixStart = text.find_first_not_of("0123456789");
    if (suffixStart == 0 || suffixStart == std::string_view::npos) return false;
    const std::string_view suffix = text.substr(suffixStart);
    uint64_t multiplier = 0;
    if (suffix == "MiB") multiplier = 1024ULL * 1024ULL;
    else if (suffix == "GiB") multiplier = 1024ULL * 1024ULL * 1024ULL;
    else return false;

    uint64_t value = 0;
    for (size_t i = 0; i < suffixStart; ++i) {
        const uint64_t digit = static_cast<uint64_t>(text[i] - '0');
        if (value > (std::numeric_limits<uint64_t>::max() - digit) / 10) return false;
        value = value * 10 + digit;
    }
    if (value == 0 || value > std::numeric_limits<uint64_t>::max() / multiplier) return false;
    bytes = value * multiplier;
    return true;
}

bool parseEndpoint(std::string_view text, Endpoint& endpoint) {
    const size_t colon = text.rfind(':');
    if (colon == std::string_view::npos || colon == 0 || colon + 1 == text.size()) return false;
    uint64_t port = 0;
    for (size_t i = colon + 1; i < text.size(); ++i) {
        if (text[i] < '0' || text[i] > '9') return false;
        port = port * 10 + static_cast<uint64_t>(text[i] - '0');
        if (port > UINT16_MAX) return false;
    }
    if (port == 0) return false;
    endpoint.host = std::string(text.substr(0, colon));
    endpoint.port = static_cast<uint16_t>(port);
    return true;
}

double mibPerSecond(uint64_t bytes, double elapsedMs) {
    return elapsedMs > 0.0 ? static_cast<double>(bytes) * 1000.0 / (1024.0 * 1024.0 * elapsedMs) : 0.0;
}

SizeSummary summarize(uint64_t sizeBytes, uint32_t requestedRuns,
                      const std::vector<RunRecord>& records) {
    SizeSummary summary;
    summary.sizeBytes = sizeBytes;
    summary.requestedRuns = requestedRuns;
    std::vector<double> uploads;
    std::vector<double> downloads;
    for (const RunRecord& record : records) {
        if (record.sizeBytes != sizeBytes || !record.uploadOk || !record.downloadOk || !record.error.empty()) continue;
        uploads.push_back(record.uploadMs);
        downloads.push_back(record.downloadMs);
    }
    summary.successCount = static_cast<uint32_t>(uploads.size());
    if (uploads.empty()) return summary;
    summary.uploadMinMs = *std::min_element(uploads.begin(), uploads.end());
    summary.uploadMedianMs = median(uploads);
    summary.uploadP95Ms = percentile(uploads, 0.95);
    summary.uploadP99Ms = percentile(uploads, 0.99);
    summary.uploadMeanMs = mean(uploads);
    summary.downloadMinMs = *std::min_element(downloads.begin(), downloads.end());
    summary.downloadMedianMs = median(downloads);
    summary.downloadP95Ms = percentile(downloads, 0.95);
    summary.downloadP99Ms = percentile(downloads, 0.99);
    summary.downloadMeanMs = mean(downloads);
    return summary;
}

bool writeReports(const BenchmarkOptions& options, const std::vector<RunRecord>& records,
                  const std::vector<SizeSummary>& summaries, std::error_code& error) {
    error.clear();
    std::filesystem::create_directories(options.workDir, error);
    if (error) return false;
    std::ofstream runs(options.workDir / "runs.csv", std::ios::trunc);
    std::ofstream summary(options.workDir / "summary.csv", std::ios::trunc);
    std::ofstream json(options.workDir / "summary.json", std::ios::trunc);
    if (!runs || !summary || !json) {
        error = std::make_error_code(std::errc::io_error);
        return false;
    }
    runs << "run_id,size_bytes,upload_ms,upload_mib_per_s,download_ms,download_mib_per_s,chunk_count,upload_ok,download_ok,error\n";
    runs << std::fixed << std::setprecision(3);
    for (const RunRecord& record : records) {
        runs << csvEscape(record.runId) << ',' << record.sizeBytes << ',' << record.uploadMs << ','
             << mibPerSecond(record.sizeBytes, record.uploadMs) << ',' << record.downloadMs << ','
             << mibPerSecond(record.sizeBytes, record.downloadMs) << ',' << record.chunkCount << ','
             << (record.uploadOk ? "true" : "false") << ',' << (record.downloadOk ? "true" : "false")
             << ',' << csvEscape(record.error) << '\n';
    }
    summary << "size_bytes,runs,upload_min_ms,upload_median_ms,upload_p95_ms,upload_p99_ms,upload_mean_ms,download_min_ms,download_median_ms,download_p95_ms,download_p99_ms,download_mean_ms,success_count\n";
    summary << std::fixed << std::setprecision(3);
    for (const SizeSummary& item : summaries) {
        summary << item.sizeBytes << ',' << item.requestedRuns << ',' << item.uploadMinMs << ','
                << item.uploadMedianMs << ',' << item.uploadP95Ms << ',' << item.uploadP99Ms << ','
                << item.uploadMeanMs << ',' << item.downloadMinMs << ',' << item.downloadMedianMs << ','
                << item.downloadP95Ms << ',' << item.downloadP99Ms << ',' << item.downloadMeanMs << ','
                << item.successCount << '\n';
    }
    json << "{\"gateway\":\"" << options.gateway.host << ':' << options.gateway.port
         << "\",\"requestedRuns\":" << options.runs
         << ",\"concurrency\":" << options.concurrency << ",\"sizes\":[";
    for (size_t i = 0; i < options.sizes.size(); ++i) {
        if (i) json << ',';
        json << options.sizes[i];
    }
    json << "],\"summaries\":[";
    for (size_t i = 0; i < summaries.size(); ++i) {
        if (i) json << ',';
        const SizeSummary& item = summaries[i];
        json << "{\"sizeBytes\":" << item.sizeBytes << ",\"successCount\":" << item.successCount
             << ",\"uploadMedianMs\":" << item.uploadMedianMs
             << ",\"uploadP95Ms\":" << item.uploadP95Ms
             << ",\"uploadP99Ms\":" << item.uploadP99Ms
             << ",\"downloadMedianMs\":" << item.downloadMedianMs
             << ",\"downloadP95Ms\":" << item.downloadP95Ms
             << ",\"downloadP99Ms\":" << item.downloadP99Ms << '}';
    }
    json << "]}";
    if (!runs.good() || !summary.good() || !json.good()) {
        error = std::make_error_code(std::errc::io_error);
        return false;
    }
    return true;
}

}  // namespace miniKV::benchmark
