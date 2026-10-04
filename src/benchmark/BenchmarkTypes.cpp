#include "benchmark/BenchmarkTypes.hpp"

#include <algorithm>
#include <array>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
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

double standardDeviation(const std::vector<double>& values) {
    if (values.empty()) return 0.0;
    const double average = mean(values);
    double squared = 0.0;
    for (const double value : values) {
        const double delta = value - average;
        squared += delta * delta;
    }
    return std::sqrt(squared / values.size());
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

uint32_t downloadWorkersPerRound(const BenchmarkOptions& options) {
    if (options.mode == BenchmarkMode::kDownloadOnly) return options.concurrency;
    if (options.mode == BenchmarkMode::kMixed) {
        if (options.readSharePercent == 0) return 0;
        const uint32_t workers = (options.concurrency * options.readSharePercent) / 100;
        return std::max<uint32_t>(1, workers);
    }
    return 0;
}

uint32_t uploadWorkersPerRound(const BenchmarkOptions& options) {
    if (options.mode == BenchmarkMode::kDownloadOnly) return 0;
    if (options.mode == BenchmarkMode::kMixed) {
        return options.concurrency - downloadWorkersPerRound(options);
    }
    return options.concurrency;
}

const char* modeName(BenchmarkMode mode) {
    switch (mode) {
    case BenchmarkMode::kEndToEnd: return "end-to-end";
    case BenchmarkMode::kUploadOnly: return "upload";
    case BenchmarkMode::kDownloadOnly: return "download";
    case BenchmarkMode::kMixed: return "mixed";
    }
    return "unknown";
}

const char* readProfileName(BenchmarkReadProfile profile) {
    switch (profile) {
    case BenchmarkReadProfile::kIndependent: return "independent";
    case BenchmarkReadProfile::kHotObject: return "hot-object";
    case BenchmarkReadProfile::kMixedSize: return "mixed-size";
    }
    return "unknown";
}

}

bool parseSize(std::string_view text, uint64_t& bytes) {
    const size_t suffixStart = text.find_first_not_of("0123456789");
    if (suffixStart == 0 || suffixStart == std::string_view::npos) return false;
    const std::string_view suffix = text.substr(suffixStart);
    uint64_t multiplier = 0;
    if (suffix == "KiB") multiplier = 1024ULL;
    else if (suffix == "MiB") multiplier = 1024ULL * 1024ULL;
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
                      const std::vector<RunRecord>& records, BenchmarkMode mode) {
    SizeSummary summary;
    summary.sizeBytes = sizeBytes;
    summary.requestedRuns = requestedRuns;
    std::vector<double> uploads;
    std::vector<double> downloads;
    double uploadStart = std::numeric_limits<double>::max();
    double uploadEnd = 0.0;
    double downloadStart = std::numeric_limits<double>::max();
    double downloadEnd = 0.0;
    for (const RunRecord& record : records) {
        if (record.sizeBytes != sizeBytes) continue;
        const bool uploadSample = mode != BenchmarkMode::kDownloadOnly && record.operation != "download";
        const bool downloadSample = mode != BenchmarkMode::kUploadOnly && record.operation != "upload";
        if (uploadSample) ++summary.uploadAttemptCount;
        if (downloadSample) ++summary.downloadAttemptCount;
        if (!record.uploadOk || !record.downloadOk || !record.error.empty()) continue;
        if (uploadSample) {
            uploads.push_back(record.uploadMs);
            if (record.endOffsetMs > record.startOffsetMs) {
                uploadStart = std::min(uploadStart, record.startOffsetMs);
                uploadEnd = std::max(uploadEnd, record.endOffsetMs);
            }
        }
        if (downloadSample) {
            downloads.push_back(record.downloadMs);
            if (record.endOffsetMs > record.startOffsetMs) {
                downloadStart = std::min(downloadStart, record.startOffsetMs);
                downloadEnd = std::max(downloadEnd, record.endOffsetMs);
            }
        }
    }
    summary.successCount = static_cast<uint32_t>(std::count_if(records.begin(), records.end(),
        [sizeBytes](const RunRecord& record) {
            return record.sizeBytes == sizeBytes && record.uploadOk && record.downloadOk &&
                   record.error.empty();
        }));
    if (!uploads.empty()) {
        summary.uploadSuccessCount = static_cast<uint32_t>(uploads.size());
        summary.uploadMinMs = *std::min_element(uploads.begin(), uploads.end());
        summary.uploadMedianMs = median(uploads);
        summary.uploadP95Ms = percentile(uploads, 0.95);
        summary.uploadP99Ms = percentile(uploads, 0.99);
        summary.uploadP999Ms = percentile(uploads, 0.999);
        summary.uploadMeanMs = mean(uploads);
        summary.uploadStddevMs = standardDeviation(uploads);
        if (uploadEnd > uploadStart) {
            const double elapsedSeconds = (uploadEnd - uploadStart) / 1000.0;
            summary.uploadOpsPerSecond = summary.uploadSuccessCount / elapsedSeconds;
            summary.uploadMiBPerSecond =
                (static_cast<double>(summary.uploadSuccessCount) * static_cast<double>(sizeBytes)) /
                (1024.0 * 1024.0 * elapsedSeconds);
        }
    }
    if (!downloads.empty()) {
        summary.downloadSuccessCount = static_cast<uint32_t>(downloads.size());
        summary.downloadMinMs = *std::min_element(downloads.begin(), downloads.end());
        summary.downloadMedianMs = median(downloads);
        summary.downloadP95Ms = percentile(downloads, 0.95);
        summary.downloadP99Ms = percentile(downloads, 0.99);
        summary.downloadP999Ms = percentile(downloads, 0.999);
        summary.downloadMeanMs = mean(downloads);
        summary.downloadStddevMs = standardDeviation(downloads);
        if (downloadEnd > downloadStart) {
            const double elapsedSeconds = (downloadEnd - downloadStart) / 1000.0;
            summary.downloadOpsPerSecond = summary.downloadSuccessCount / elapsedSeconds;
            summary.downloadMiBPerSecond =
                (static_cast<double>(summary.downloadSuccessCount) * static_cast<double>(sizeBytes)) /
                (1024.0 * 1024.0 * elapsedSeconds);
        }
    }
    return summary;
}

bool writeReports(const BenchmarkOptions& options, const std::vector<RunRecord>& records,
                  const std::vector<SizeSummary>& summaries, std::error_code& error) {
    error.clear();
    std::filesystem::create_directories(options.workDir, error);
    if (error) return false;
    std::ofstream runs(options.workDir / "runs.csv", std::ios::trunc);
    std::ofstream summary(options.workDir / "summary.csv", std::ios::trunc);
    std::ofstream uploadStages(options.workDir / "upload-stages-summary.csv", std::ios::trunc);
    std::ofstream json(options.workDir / "summary.json", std::ios::trunc);
    std::ofstream perSecond(options.workDir / "per-second.csv", std::ios::trunc);
    if (!runs || !summary || !uploadStages || !json || !perSecond) {
        error = std::make_error_code(std::errc::io_error);
        return false;
    }
    runs << "run_id,operation,reader_class,object_id,object_version,file_hash,upload_session_id,input_hash,size_bytes,upload_ms,upload_mib_per_s,upload_create_session_ms,upload_get_session_ms,upload_chunk_budget_wait_ms,upload_checksum_preparation_ms,upload_route_capability_ms,upload_data_node_ms,upload_chunk_total_ms,upload_object_commit_ms,download_ms,download_mib_per_s,chunk_count,data_connection_opens,data_requests,data_connection_reuses,upload_ok,download_ok,error,start_offset_ms,end_offset_ms\n";
    runs << std::fixed << std::setprecision(3);
    for (const RunRecord& record : records) {
        runs << csvEscape(record.runId) << ',' << csvEscape(record.operation) << ','
             << csvEscape(record.readerClass) << ','
             << csvEscape(record.objectId) << ',' << record.objectVersion << ','
             << csvEscape(record.fileHash) << ',' << csvEscape(record.uploadSessionId) << ','
             << csvEscape(record.inputHash) << ','
             << record.sizeBytes << ',' << record.uploadMs << ','
             << mibPerSecond(record.sizeBytes, record.uploadMs) << ','
             << record.uploadTimings.createSessionMs << ',' << record.uploadTimings.getSessionMs << ','
             << record.uploadTimings.chunkBudgetWaitMs << ','
             << record.uploadTimings.checksumPreparationMs << ','
             << record.uploadTimings.routeCapabilityMs << ','
             << record.uploadTimings.dataNodeUploadMs << ','
             << record.uploadTimings.chunkUploadTotalMs << ','
             << record.uploadTimings.objectCommitMs << ',' << record.downloadMs << ','
             << mibPerSecond(record.sizeBytes, record.downloadMs) << ',' << record.chunkCount << ','
             << record.dataConnectionOpens << ',' << record.dataRequests << ','
             << record.dataConnectionReuses << ','
             << (record.uploadOk ? "true" : "false") << ',' << (record.downloadOk ? "true" : "false")
             << ',' << csvEscape(record.error) << ',' << record.startOffsetMs << ',' << record.endOffsetMs << '\n';
    }
    uploadStages << "stage,samples,p50_ms,p95_ms,p99_ms\n";
    const std::array<std::pair<const char*, double miniKV::client::UploadPhaseTimings::*>, 8> stageFields{{
        {"create_session", &miniKV::client::UploadPhaseTimings::createSessionMs},
        {"get_session", &miniKV::client::UploadPhaseTimings::getSessionMs},
        {"chunk_budget_wait", &miniKV::client::UploadPhaseTimings::chunkBudgetWaitMs},
        {"checksum_preparation", &miniKV::client::UploadPhaseTimings::checksumPreparationMs},
        {"route_capability", &miniKV::client::UploadPhaseTimings::routeCapabilityMs},
        {"data_node_upload", &miniKV::client::UploadPhaseTimings::dataNodeUploadMs},
        {"chunk_upload_total", &miniKV::client::UploadPhaseTimings::chunkUploadTotalMs},
        {"object_commit", &miniKV::client::UploadPhaseTimings::objectCommitMs},
    }};
    uploadStages << std::fixed << std::setprecision(3);
    for(const auto& [name, field] : stageFields) {
        std::vector<double> values;
        for(const RunRecord& record : records) {
            if(record.uploadOk && record.downloadOk && record.error.empty() &&
               record.operation != "download") values.push_back(record.uploadTimings.*field);
        }
        uploadStages << name << ',' << values.size() << ',' << median(values) << ','
                     << percentile(values, 0.95) << ',' << percentile(values, 0.99) << '\n';
    }
    const uint32_t uploadWorkers = uploadWorkersPerRound(options);
    const uint32_t downloadWorkers = downloadWorkersPerRound(options);
    const uint32_t reportRounds = options.durationSeconds > 0 ? 1 : options.runs;
    summary << "mode,read_profile,connection_mode,download_verification,upload_checksum,requests_per_worker,slow_reader_workers,slow_reader_bytes_per_second,rounds,total_workers_per_round,upload_workers_per_round,download_workers_per_round,upload_samples_requested,download_samples_requested,chunk_window,global_chunk_budget,fixture_settle_ms,size_bytes,samples_total,upload_min_ms,upload_median_ms,upload_p95_ms,upload_p99_ms,upload_mean_ms,download_min_ms,download_median_ms,download_p95_ms,download_p99_ms,download_mean_ms,success_count,read_share_percent,ratio_policy,duration_seconds,warmup_seconds,download_fixture_count,upload_success_count,download_success_count,upload_attempt_count,download_attempt_count,upload_ops_per_sec,download_ops_per_sec,upload_mib_per_sec,download_mib_per_sec,upload_p999_ms,download_p999_ms,upload_stddev_ms,download_stddev_ms\n";
    summary << std::fixed << std::setprecision(3);
    for (const SizeSummary& item : summaries) {
        const uint32_t uploadSamples = options.durationSeconds > 0
            ? item.uploadAttemptCount : options.runs * uploadWorkers;
        const uint32_t downloadSamples = options.durationSeconds > 0
            ? item.downloadAttemptCount : options.runs * downloadWorkers;
        summary << modeName(options.mode) << ',' << readProfileName(options.readProfile) << ','
                << (options.keepAlive ? "keep-alive" : "close") << ','
                << (options.transportOnlyDownload ? "transport-only-crc32c" : "strict-final-sha256") << ','
                << options.uploadChecksumType << ','
                << options.requestsPerWorker << ','
                << options.slowReaderWorkers << ',' << options.slowReaderBytesPerSecond << ','
                << reportRounds << ',' << options.concurrency << ','
                << uploadWorkers << ',' << downloadWorkers << ',' << uploadSamples << ','
                << downloadSamples << ',' << options.chunkWindow << ',' << options.globalChunkBudget << ','
                << options.fixtureSettleMs << ','
                << item.sizeBytes << ',' << item.requestedRuns << ',' << item.uploadMinMs << ','
                << item.uploadMedianMs << ',' << item.uploadP95Ms << ',' << item.uploadP99Ms << ','
                << item.uploadMeanMs << ',' << item.downloadMinMs << ',' << item.downloadMedianMs << ','
                << item.downloadP95Ms << ',' << item.downloadP99Ms << ',' << item.downloadMeanMs << ','
                << item.successCount << ',' << options.readSharePercent << ','
                << (options.mode == BenchmarkMode::kMixed ? "request-generation-by-workers" : "not-applicable") << ','
                << options.durationSeconds << ','
                << options.warmupSeconds << ',' << options.downloadFixtureCount << ','
                << item.uploadSuccessCount << ',' << item.downloadSuccessCount << ','
                << item.uploadAttemptCount << ',' << item.downloadAttemptCount << ','
                << item.uploadOpsPerSecond << ',' << item.downloadOpsPerSecond << ','
                << item.uploadMiBPerSecond << ',' << item.downloadMiBPerSecond << ','
                << item.uploadP999Ms << ',' << item.downloadP999Ms << ','
                << item.uploadStddevMs << ',' << item.downloadStddevMs << '\n';
    }
    perSecond << "second,size_bytes,upload_ops,download_ops,upload_bytes,download_bytes,upload_mib,download_mib\n";
    struct Bucket { uint64_t uploadOps = 0; uint64_t downloadOps = 0; uint64_t uploadBytes = 0; uint64_t downloadBytes = 0; };
    std::map<std::pair<uint64_t, uint64_t>, Bucket> buckets;
    for (const RunRecord& record : records) {
        if (!record.uploadOk || !record.downloadOk || !record.error.empty() ||
            record.endOffsetMs < record.startOffsetMs) continue;
        const uint64_t second = static_cast<uint64_t>(record.endOffsetMs / 1000.0);
        auto& bucket = buckets[{second, record.sizeBytes}];
        if (record.operation != "download" && options.mode != BenchmarkMode::kDownloadOnly) {
            ++bucket.uploadOps;
            bucket.uploadBytes += record.sizeBytes;
        }
        if (record.operation != "upload" && options.mode != BenchmarkMode::kUploadOnly) {
            ++bucket.downloadOps;
            bucket.downloadBytes += record.sizeBytes;
        }
    }
    for (const auto& [key, bucket] : buckets) {
        perSecond << key.first << ',' << key.second << ',' << bucket.uploadOps << ',' << bucket.downloadOps << ','
                  << bucket.uploadBytes << ',' << bucket.downloadBytes << ','
                  << static_cast<double>(bucket.uploadBytes) / (1024.0 * 1024.0) << ','
                  << static_cast<double>(bucket.downloadBytes) / (1024.0 * 1024.0) << '\n';
    }
    uint64_t jsonUploadSamples = 0;
    uint64_t jsonDownloadSamples = 0;
    for (const SizeSummary& item : summaries) {
        jsonUploadSamples += options.durationSeconds > 0
            ? item.uploadAttemptCount : options.runs * uploadWorkers;
        jsonDownloadSamples += options.durationSeconds > 0
            ? item.downloadAttemptCount : options.runs * downloadWorkers;
    }
    json << "{\"gateway\":\"" << options.gateway.host << ':' << options.gateway.port
         << "\",\"rounds\":" << reportRounds
         << ",\"requestedRuns\":" << reportRounds
         << ",\"concurrency\":" << options.concurrency
         << ",\"uploadWorkersPerRound\":" << uploadWorkers
         << ",\"downloadWorkersPerRound\":" << downloadWorkers
         << ",\"uploadSamplesRequested\":" << jsonUploadSamples
         << ",\"downloadSamplesRequested\":" << jsonDownloadSamples
         << ",\"chunkWindow\":" << options.chunkWindow
         << ",\"globalChunkBudget\":" << options.globalChunkBudget
         << ",\"uploadChecksum\":\"" << options.uploadChecksumType << "\""
         << ",\"downloadVerification\":\""
         << (options.transportOnlyDownload ? "transport-only-crc32c" : "strict-final-sha256") << "\""
         << ",\"fixtureSettleMs\":" << options.fixtureSettleMs
         << ",\"readProfile\":\"" << readProfileName(options.readProfile) << "\""
         << ",\"connectionMode\":\"" << (options.keepAlive ? "keep-alive" : "close")
         << "\""
         << ",\"requestsPerWorker\":" << options.requestsPerWorker
         << ",\"slowReaderWorkers\":" << options.slowReaderWorkers
         << ",\"slowReaderBytesPerSecond\":" << options.slowReaderBytesPerSecond
         << ",\"readSharePercent\":" << options.readSharePercent
         << ",\"ratioPolicy\":\""
         << (options.mode == BenchmarkMode::kMixed ? "request-generation-by-workers" : "not-applicable")
         << "\""
         << ",\"durationSeconds\":" << options.durationSeconds
         << ",\"warmupSeconds\":" << options.warmupSeconds
         << ",\"downloadFixtureCount\":" << options.downloadFixtureCount
         << ",\"mode\":\""
         << modeName(options.mode) << "\",\"sizes\":[";
    for (size_t i = 0; i < options.sizes.size(); ++i) {
        if (i) json << ',';
        json << options.sizes[i];
    }
    json << "],\"summaries\":[";
    for (size_t i = 0; i < summaries.size(); ++i) {
        if (i) json << ',';
        const SizeSummary& item = summaries[i];
        json << "{\"sizeBytes\":" << item.sizeBytes << ",\"successCount\":" << item.successCount
             << ",\"uploadSuccessCount\":" << item.uploadSuccessCount
             << ",\"downloadSuccessCount\":" << item.downloadSuccessCount
             << ",\"uploadAttemptCount\":" << item.uploadAttemptCount
             << ",\"downloadAttemptCount\":" << item.downloadAttemptCount
             << ",\"uploadOpsPerSecond\":" << item.uploadOpsPerSecond
             << ",\"downloadOpsPerSecond\":" << item.downloadOpsPerSecond
             << ",\"uploadMiBPerSecond\":" << item.uploadMiBPerSecond
             << ",\"downloadMiBPerSecond\":" << item.downloadMiBPerSecond
             << ",\"uploadMedianMs\":" << item.uploadMedianMs
             << ",\"uploadP95Ms\":" << item.uploadP95Ms
             << ",\"uploadP99Ms\":" << item.uploadP99Ms
             << ",\"uploadP999Ms\":" << item.uploadP999Ms
             << ",\"uploadStddevMs\":" << item.uploadStddevMs
             << ",\"downloadMedianMs\":" << item.downloadMedianMs
             << ",\"downloadP95Ms\":" << item.downloadP95Ms
             << ",\"downloadP99Ms\":" << item.downloadP99Ms
             << ",\"downloadP999Ms\":" << item.downloadP999Ms
             << ",\"downloadStddevMs\":" << item.downloadStddevMs << '}';
    }
    json << "]}";
    if (!runs.good() || !summary.good() || !uploadStages.good() || !json.good() || !perSecond.good()) {
        error = std::make_error_code(std::errc::io_error);
        return false;
    }
    return true;
}

}  // namespace miniKV::benchmark
