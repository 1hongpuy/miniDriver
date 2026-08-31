#pragma once

#include "client/HttpTransport.hpp"

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace miniKV::benchmark {

enum class BenchmarkMode { kEndToEnd, kUploadOnly, kDownloadOnly, kMixed };
// Read profiles only change fixture selection.  They deliberately do not
// imply any replica-aware scheduler: every read still follows the current
// manifest candidate order.
enum class BenchmarkReadProfile { kIndependent, kHotObject, kMixedSize };

using Endpoint = miniKV::client::Endpoint;

struct BenchmarkOptions {
    Endpoint gateway;
    std::filesystem::path workDir;
    std::vector<uint64_t> sizes;
    uint32_t runs = 3;
    uint32_t concurrency = 1;
    uint32_t chunkWindow = 1;
    // Process-wide ceiling across concurrent benchmark files.  It is the
    // client-side counterpart to the DataNode admission budget.
    uint32_t globalChunkBudget = 2;
    // Must match the Gateway session's negotiated protocol. CRC32C is the
    // V3 new-object default; explicit sha256 keeps CAS compatibility tests
    // from silently using the wrong integrity contract.
    std::string uploadChecksumType = "crc32c";
    // Mixed mode creates download fixtures before the timed round.  A caller
    // can wait for DataNode/Gateway load reporting to settle before starting
    // the concurrent upload/download workers.
    uint32_t fixtureSettleMs = 0;
    BenchmarkMode mode = BenchmarkMode::kEndToEnd;
    BenchmarkReadProfile readProfile = BenchmarkReadProfile::kIndependent;
    // Close remains the historical baseline. Keep-alive reuses a sequential
    // HTTP/1.1 connection inside one object read where the server permits it.
    bool keepAlive = false;
    // strict writes the assembled object and validates a final SHA-256 in the
    // benchmark. transport-only still verifies each whole Chunk checksum, but
    // discards the bytes after receipt to isolate the SDK/DataNode path.
    bool transportOnlyDownload = false;
    // R6 repeats immutable-object reads sequentially inside each worker. It
    // is one for all historical benchmarks; values above one expose actual
    // HTTP/1.1 connection reuse for single-Chunk small objects.
    uint32_t requestsPerWorker = 1;
    // R5 reserves the final N download workers for intentionally throttled
    // socket consumption.  Zero means every reader is normal speed.
    uint32_t slowReaderWorkers = 0;
    uint64_t slowReaderBytesPerSecond = 0;
    // Optional V3 Worker-style read path. It still obtains legacy fixture
    // identity for old benchmark data, then asks Gateway for an authorized
    // objectId + objectVersion ReadPlan before touching DataNodes.
    bool sdkReadPlan = false;
    std::string clusterInternalToken;
    std::string servicePrincipal;
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
    // DataNode Chunk GET transport counters. Gateway manifest/control calls
    // are intentionally not included, so R6 directly measures the data path.
    uint64_t dataConnectionOpens = 0;
    uint64_t dataRequests = 0;
    uint64_t dataConnectionReuses = 0;
    // "normal" or "slow".  This stays per sample so R5 reports can compare
    // normal-reader P99 against deliberately throttled peers.
    std::string readerClass = "normal";
    std::string error;
    std::string fileHash;
    // Logical identity returned by the upload Commit response.  A fileHash is
    // a content/manifest identity and can be shared by multiple catalog
    // entries, so it is deliberately insufficient for the V3 SDK ReadPlan.
    std::string objectId;
    uint64_t objectVersion = 0;
    std::string inputHash;
    std::string operation;
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
                      const std::vector<RunRecord>& records,
                      BenchmarkMode mode = BenchmarkMode::kEndToEnd);
bool writeReports(const BenchmarkOptions& options, const std::vector<RunRecord>& records,
                  const std::vector<SizeSummary>& summaries, std::error_code& error);

}  // namespace miniKV::benchmark
