#include "benchmark/BenchmarkRunner.hpp"

#include "benchmark/BenchmarkHttpClient.hpp"
#include "benchmark/BenchmarkInput.hpp"
#include "client/MiniDriverClient.hpp"
#include "utils/Util.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <mutex>
#include <openssl/evp.h>
#include <set>
#include <sstream>
#include <thread>
#include <unordered_set>
#include <utility>

namespace miniKV::benchmark {
namespace {

constexpr size_t kBufferBytes = 64 * 1024;
constexpr int kGatewayTimeoutMs = 30000;
constexpr int kDataNodeTimeoutMs = 60000;

struct DownloadTransportStats {
    uint64_t connectionOpens = 0;
    uint64_t requests = 0;
    uint64_t connectionReuses = 0;
};

class ChunkBudget {
public:
    explicit ChunkBudget(uint32_t limit) : limit_(limit) {}
    double acquireTimed() {
        const auto started = std::chrono::steady_clock::now();
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return active_ < limit_; });
        ++active_;
        return std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - started).count();
    }
    void acquire() {
        (void)acquireTimed();
    }
    void release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            --active_;
        }
        cv_.notify_one();
    }
private:
    const uint32_t limit_;
    uint32_t active_ = 0;
    std::mutex mutex_;
    std::condition_variable cv_;
};

class ChunkBudgetLease {
public:
    explicit ChunkBudgetLease(ChunkBudget* budget) : budget_(budget) { if(budget_) budget_->acquire(); }
    ~ChunkBudgetLease() { if(budget_) budget_->release(); }
    ChunkBudgetLease(const ChunkBudgetLease&) = delete;
    ChunkBudgetLease& operator=(const ChunkBudgetLease&) = delete;
private:
    ChunkBudget* budget_;
};

std::string benchmarkInvocationId() {
    static const std::string value = std::to_string(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    return value;
}

std::string endpointString(const Endpoint& endpoint) {
    return endpoint.host + ':' + std::to_string(endpoint.port);
}

bool sha256File(const std::filesystem::path& path, std::string& hash, std::string& error) {
    std::ifstream input(path, std::ios::binary);
    if (!input) { error = "cannot open " + path.string(); return false; }
    EVP_MD_CTX* context = EVP_MD_CTX_new();
    if (context == nullptr || EVP_DigestInit_ex(context, EVP_sha256(), nullptr) != 1) {
        if (context != nullptr) EVP_MD_CTX_free(context);
        error = "cannot initialize SHA-256";
        return false;
    }
    std::array<char, kBufferBytes> buffer{};
    while (input) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const std::streamsize count = input.gcount();
        if (count > 0 && EVP_DigestUpdate(context, buffer.data(), static_cast<size_t>(count)) != 1) {
            EVP_MD_CTX_free(context); error = "cannot update SHA-256"; return false;
        }
    }
    if (!input.eof()) { EVP_MD_CTX_free(context); error = "cannot read " + path.string(); return false; }
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digestSize = 0;
    if (EVP_DigestFinal_ex(context, digest, &digestSize) != 1) {
        EVP_MD_CTX_free(context); error = "cannot finish SHA-256"; return false;
    }
    EVP_MD_CTX_free(context);
    static constexpr char digits[] = "0123456789abcdef";
    hash.clear(); hash.reserve(digestSize * 2);
    for (unsigned int i = 0; i < digestSize; ++i) {
        hash += digits[(digest[i] >> 4) & 0x0f];
        hash += digits[digest[i] & 0x0f];
    }
    return true;
}

uint32_t updateCrc32c(uint32_t crc, const char* bytes, size_t size)
{
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> values{};
        for(uint32_t index = 0; index < values.size(); ++index) {
            uint32_t value = index;
            for(unsigned int bit = 0; bit < 8; ++bit) {
                value = (value >> 1) ^ ((value & 1U) ? 0x82f63b78U : 0U);
            }
            values[index] = value;
        }
        return values;
    }();
    for(size_t index = 0; index < size; ++index) {
        crc = table[(crc ^ static_cast<uint8_t>(bytes[index])) & 0xffU] ^ (crc >> 8);
    }
    return crc;
}

std::string uint32Hex(uint32_t value)
{
    static constexpr char digits[] = "0123456789abcdef";
    std::string result(8, '0');
    for(size_t index = 0; index < result.size(); ++index) {
        result[index] = digits[(value >> ((7 - index) * 4)) & 0x0fU];
    }
    return result;
}

bool chunkDigestsRange(std::ifstream& input, uint64_t offset, uint64_t length,
                       std::string& hash, std::string& crc32c, std::string& error) {
    input.clear();
    input.seekg(static_cast<std::streamoff>(offset));
    if (!input) { error = "cannot seek benchmark input"; return false; }
    EVP_MD_CTX* context = EVP_MD_CTX_new();
    if (context == nullptr || EVP_DigestInit_ex(context, EVP_sha256(), nullptr) != 1) {
        if (context != nullptr) EVP_MD_CTX_free(context);
        error = "cannot initialize chunk SHA-256";
        return false;
    }
    std::array<char, kBufferBytes> buffer{};
    uint64_t remaining = length;
    uint32_t crc = 0xffffffffU;
    while (remaining > 0) {
        const size_t wanted = static_cast<size_t>(std::min<uint64_t>(remaining, buffer.size()));
        input.read(buffer.data(), static_cast<std::streamsize>(wanted));
        if (static_cast<size_t>(input.gcount()) != wanted ||
            EVP_DigestUpdate(context, buffer.data(), wanted) != 1) {
            EVP_MD_CTX_free(context); error = "cannot hash benchmark input range"; return false;
        }
        crc = updateCrc32c(crc, buffer.data(), wanted);
        remaining -= wanted;
    }
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digestSize = 0;
    if (EVP_DigestFinal_ex(context, digest, &digestSize) != 1) {
        EVP_MD_CTX_free(context); error = "cannot finish chunk SHA-256"; return false;
    }
    EVP_MD_CTX_free(context);
    static constexpr char digits[] = "0123456789abcdef";
    hash.clear(); hash.reserve(digestSize * 2);
    for (unsigned int i = 0; i < digestSize; ++i) {
        hash += digits[(digest[i] >> 4) & 0x0f];
        hash += digits[digest[i] & 0x0f];
    }
    crc32c = uint32Hex(crc ^ 0xffffffffU);
    return true;
}

bool makeInputFile(const std::filesystem::path& path, uint64_t size, uint32_t runIndex,
                   std::string& error) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output) { error = "cannot create " + path.string(); return false; }
    std::array<char, kBufferBytes> buffer{};
    uint64_t remaining = size;
    uint64_t offset = 0;
    while (remaining > 0) {
        const size_t bytes = static_cast<size_t>(std::min<uint64_t>(remaining, buffer.size()));
        fillBenchmarkBytes(buffer.data(), bytes, offset, runIndex);
        output.write(buffer.data(), static_cast<std::streamsize>(bytes));
        if (!output) { error = "cannot write " + path.string(); return false; }
        remaining -= bytes;
        offset += bytes;
    }
    return true;
}

bool gatewayRequest(const Endpoint& gateway, const std::string& method, const std::string& path,
                    const std::string& body, HttpResponse& response, std::string& error) {
    if (!httpRequest(gateway, method, path, {{"Content-Type", "application/json"}}, body,
                     kGatewayTimeoutMs, response, error)) {
        error = "Gateway " + method + " " + path + ": " + error;
        return false;
    }
    if (response.status < 200 || response.status >= 300) {
        error = "Gateway " + method + " " + path + " returned HTTP " +
            std::to_string(response.status) + ": " + response.body;
        return false;
    }
    return true;
}

bool makeRouteChain(const std::string& route, std::string& chain, std::string& error) {
    std::vector<std::string> targets;
    for (const std::string& node : miniKV::util::jsonObjectArray(route, "chain")) {
        const std::string nodeId = miniKV::util::jsonString(node, "nodeId");
        const std::string address = miniKV::util::jsonString(node, "address");
        const uint64_t port = miniKV::util::jsonUint(node, "httpPort");
        if (nodeId.empty() || address.empty() || port == 0 || port > UINT16_MAX) {
            error = "Gateway returned invalid replica chain";
            return false;
        }
        targets.push_back(nodeId + '@' + address + ':' + std::to_string(port));
    }
    if (targets.empty()) { error = "Gateway returned empty replica chain"; return false; }
    chain = miniKV::util::join(targets, ';');
    return true;
}

bool uploadRange(std::ifstream& input, uint64_t offset, uint64_t length,
                 const Endpoint& primary, const std::string& path,
                 const std::map<std::string, std::string>& headers,
                 std::string& error) {
    StreamingRequest request;
    if (!request.open(primary, "PUT", path, headers, length, kDataNodeTimeoutMs, error)) return false;
    input.clear();
    input.seekg(static_cast<std::streamoff>(offset));
    if (!input) { error = "cannot seek upload range"; return false; }
    std::array<char, kBufferBytes> buffer{};
    uint64_t remaining = length;
    while (remaining > 0) {
        const size_t wanted = static_cast<size_t>(std::min<uint64_t>(remaining, buffer.size()));
        input.read(buffer.data(), static_cast<std::streamsize>(wanted));
        if (static_cast<size_t>(input.gcount()) != wanted) { error = "cannot read upload range"; return false; }
        if (!request.write(buffer.data(), wanted, error)) return false;
        remaining -= wanted;
    }
    HttpResponse response;
    if (!request.finish(response, error)) return false;
    if (response.status != 200) {
        error = "DataNode PUT returned HTTP " + std::to_string(response.status) + ": " + response.body;
        return false;
    }
    return true;
}

bool uploadOneChunk(const BenchmarkOptions& options, const std::filesystem::path& inputPath,
                    const std::string& sessionId, uint64_t fileSize, uint64_t chunkSize,
                    uint32_t index, std::string& error) {
    const uint64_t offset = static_cast<uint64_t>(index) * chunkSize;
    const uint64_t bytes = std::min<uint64_t>(chunkSize, fileSize - offset);
    std::ifstream input(inputPath, std::ios::binary);
    if (!input) { error = "cannot reopen benchmark input for chunk"; return false; }
    std::string chunkHash;
    std::string chunkCrc32c;
    if (!chunkDigestsRange(input, offset, bytes, chunkHash, chunkCrc32c, error)) return false;
    HttpResponse routeResponse;
    const std::string routeBody = "{\"chunks\":[{\"index\":" + std::to_string(index) +
        ",\"hash\":\"" + chunkHash + "\",\"size\":" + std::to_string(bytes) +
        ",\"checksumType\":\"crc32c\",\"checksumDigest\":\"" + chunkCrc32c + "\"}]}";
    if (!gatewayRequest(options.gateway, "POST", "/api/v2/upload/sessions/" + sessionId + "/routes",
                        routeBody, routeResponse, error)) return false;
    const std::vector<std::string> routes = miniKV::util::jsonObjectArray(routeResponse.body, "routes");
    if (routes.size() != 1) { error = "Gateway returned invalid routes response"; return false; }
    const std::string primaryNodeId = miniKV::util::jsonString(routes[0], "primaryNodeId");
    Endpoint primary{miniKV::util::jsonString(routes[0], "primaryAddress"),
                     static_cast<uint16_t>(miniKV::util::jsonUint(routes[0], "primaryPort"))};
    const std::string uploadToken = miniKV::util::jsonString(routes[0], "uploadToken");
    std::string storageIdentity = miniKV::util::jsonString(routes[0], "storageIdentity");
    if(storageIdentity.empty()) storageIdentity = chunkHash;
    std::string chain;
    if (primaryNodeId.empty() || primary.host.empty() || primary.port == 0 || uploadToken.empty() ||
        !makeRouteChain(routes[0], chain, error)) {
        if (error.empty()) error = "Gateway returned invalid route";
        return false;
    }
    const std::map<std::string, std::string> headers{
        {"Content-Type", "application/octet-stream"}, {"X-Session-Id", sessionId},
        {"X-Chunk-Index", std::to_string(index)}, {"X-Commit-Owner", primaryNodeId},
        {"X-Gateway-Address", options.gateway.host}, {"X-Gateway-Port", std::to_string(options.gateway.port)},
        {"X-Replica-Chain", chain}, {"X-Replica-Position", "0"}, {"X-Upload-Token", uploadToken},
    };
    return uploadRange(input, offset, bytes, primary,
                       "/v2/chunks/" + storageIdentity, headers, error);
}

bool downloadFile(const miniKV::client::ClientConfig& config, bool sdkReadPlan,
                  const std::string& fileHash, const miniKV::client::ObjectRef& object,
                  const std::filesystem::path& outputPath, uint32_t& chunkCount,
                  bool keepAlive, bool transportOnly, DownloadTransportStats& transportStats,
                  std::string& error, uint64_t maxReadBytesPerSecond = 0,
                  miniKV::client::MiniDriverClient* persistentClient = nullptr) {
    miniKV::client::MiniDriverClient localClient(config);
    miniKV::client::MiniDriverClient& client = persistentClient != nullptr
        ? *persistentClient : localClient;
    miniKV::client::ObjectReadPlan plan;
    if (sdkReadPlan) {
        if (!client.getReadPlan(object, plan, error)) return false;
    } else {
        if (!client.getLegacyManifest(fileHash, plan, error)) return false;
    }
    chunkCount = static_cast<uint32_t>(plan.chunks.size());
    miniKV::client::TransferStats stats;
    miniKV::client::ReadOptions readOptions;
    readOptions.keepAlive = keepAlive;
    readOptions.maxReadBytesPerSecond = maxReadBytesPerSecond;
    if (transportOnly) {
        if (!client.downloadToSink(plan, readOptions, stats, error)) return false;
    } else if (!client.downloadToFile(plan, outputPath, readOptions, stats, error)) return false;
    transportStats.connectionOpens = stats.dataConnectionOpens;
    transportStats.requests = stats.dataRequests;
    transportStats.connectionReuses = stats.dataConnectionReuses;
    return true;
}

RunRecord runDownloadOne(const BenchmarkOptions& options, const RunRecord& fixture,
                         std::ostream& console,
                         miniKV::client::MiniDriverClient* persistentClient = nullptr,
                         uint64_t maxReadBytesPerSecond = 0,
                         std::string readerClass = "normal") {
    RunRecord record;
    record.runId = fixture.runId;
    record.operation = "download";
    record.sizeBytes = fixture.sizeBytes;
    record.chunkCount = fixture.chunkCount;
    record.fileHash = fixture.fileHash;
    record.objectId = fixture.objectId;
    record.objectVersion = fixture.objectVersion;
    record.inputHash = fixture.inputHash;
    record.readerClass = std::move(readerClass);
    // The fixture is deliberately prepared before the timed download interval.
    record.uploadOk = true;
    if (record.fileHash.empty() || record.inputHash.empty() ||
        (options.sdkReadPlan && (record.objectId.empty() || record.objectVersion == 0))) {
        record.error = "download fixture is incomplete";
        return record;
    }
    const std::filesystem::path outputPath = options.workDir / (record.runId + ".download");
    std::string error;
    const auto start = std::chrono::steady_clock::now();
    uint32_t downloadedChunks = 0;
    DownloadTransportStats transportStats;
    miniKV::client::ClientConfig config;
    config.gateway = options.gateway;
    config.clusterInternalToken = options.clusterInternalToken;
    config.servicePrincipal = options.servicePrincipal;
    if (!downloadFile(config, options.sdkReadPlan, record.fileHash,
                      {record.objectId, record.objectVersion}, outputPath, downloadedChunks,
                      options.keepAlive, options.transportOnlyDownload, transportStats, error, maxReadBytesPerSecond,
                      persistentClient)) {
        record.error = error;
        return record;
    }
    if (!options.transportOnlyDownload) {
        std::string outputHash;
        if (!sha256File(outputPath, outputHash, error)) { record.error = error; return record; }
        if (outputHash != record.inputHash) { record.error = "downloaded file SHA-256 mismatch"; return record; }
    }
    record.downloadMs = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - start).count();
    record.downloadOk = downloadedChunks == record.chunkCount;
    record.dataConnectionOpens = transportStats.connectionOpens;
    record.dataRequests = transportStats.requests;
    record.dataConnectionReuses = transportStats.connectionReuses;
    if (!record.downloadOk) record.error = "manifest chunk count changed during benchmark";
    console << record.runId << " download=" << std::fixed << std::setprecision(2)
            << mibPerSecond(record.sizeBytes, record.downloadMs) << " MiB/s\n";
    return record;
}

RunRecord runOne(const BenchmarkOptions& options, uint64_t size, uint32_t runIndex,
                 std::ostream& console, ChunkBudget* chunkBudget = nullptr) {
    RunRecord record;
    record.runId = "size-" + std::to_string(size) + "-run-" + std::to_string(runIndex + 1) +
        "-" + benchmarkInvocationId();
    record.operation = options.mode == BenchmarkMode::kUploadOnly ? "upload" : "end-to-end";
    record.sizeBytes = size;
    const std::filesystem::path inputPath = options.workDir / (record.runId + ".input");
    const std::filesystem::path outputPath = options.workDir / (record.runId + ".download");
    std::string error;
    if (!makeInputFile(inputPath, size, runIndex, error)) { record.error = error; return record; }
    std::string inputHash;
    if (!sha256File(inputPath, inputHash, error)) { record.error = error; return record; }
    record.inputHash = inputHash;

    const auto uploadStart = std::chrono::steady_clock::now();
    miniKV::client::MiniDriverClient client({options.gateway});
    miniKV::client::UploadOptions uploadOptions;
    uploadOptions.chunkWindow = options.chunkWindow;
    uploadOptions.checksumType = options.uploadChecksumType;
    if (chunkBudget != nullptr) {
        uploadOptions.acquireChunkTimed = [chunkBudget] { return chunkBudget->acquireTimed(); };
        uploadOptions.releaseChunk = [chunkBudget] { chunkBudget->release(); };
    }
    miniKV::client::UploadResult uploaded;
    if (!client.uploadFile(inputPath, record.runId + ".bin", options.remoteDir, uploadOptions, uploaded, error)) {
        record.uploadTimings = uploaded.timings;
        record.error = error; return record;
    }
    record.chunkCount = uploaded.chunkCount;
    record.uploadTimings = uploaded.timings;
    record.uploadSessionId = uploaded.sessionId;
    record.fileHash = uploaded.fileHash;
    record.objectId = uploaded.object.objectId;
    record.objectVersion = uploaded.object.objectVersion;
    record.uploadMs = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - uploadStart).count();
    record.uploadOk = true;

    if(options.mode == BenchmarkMode::kUploadOnly) {
        // The upload-capacity mode intentionally stops after the Gateway's
        // application-level commit. This is not a durable media commit until
        // the DataNode and metadata paths add an explicit sync boundary. Mark
        // the unused download half successful so the existing
        // per-run reporting pipeline can represent a valid upload-only run.
        record.downloadOk = true;
        console << record.runId << " upload=" << std::fixed << std::setprecision(2)
                << mibPerSecond(size, record.uploadMs) << " MiB/s\n";
        return record;
    }

    const auto downloadStart = std::chrono::steady_clock::now();
    uint32_t downloadedChunks = 0;
    DownloadTransportStats transportStats;
    miniKV::client::ClientConfig readConfig;
    readConfig.gateway = options.gateway;
    readConfig.clusterInternalToken = options.clusterInternalToken;
    readConfig.servicePrincipal = options.servicePrincipal;
    if (!downloadFile(readConfig, options.sdkReadPlan, record.fileHash,
                      {record.objectId, record.objectVersion}, outputPath, downloadedChunks,
                      options.keepAlive, options.transportOnlyDownload, transportStats, error)) {
        record.error = error; return record;
    }
    if (!options.transportOnlyDownload) {
        std::string outputHash;
        if (!sha256File(outputPath, outputHash, error)) { record.error = error; return record; }
        if (outputHash != inputHash) { record.error = "downloaded file SHA-256 mismatch"; return record; }
    }
    record.downloadMs = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - downloadStart).count();
    record.downloadOk = downloadedChunks == record.chunkCount;
    record.dataConnectionOpens = transportStats.connectionOpens;
    record.dataRequests = transportStats.requests;
    record.dataConnectionReuses = transportStats.connectionReuses;
    if (!record.downloadOk) record.error = "manifest chunk count changed during benchmark";
    console << record.runId << " upload=" << std::fixed << std::setprecision(2)
            << mibPerSecond(size, record.uploadMs) << " MiB/s download="
            << mibPerSecond(size, record.downloadMs) << " MiB/s\n";
    return record;
}

uint32_t durationDownloadWorkers(const BenchmarkOptions& options)
{
    if (options.mode == BenchmarkMode::kDownloadOnly) return options.concurrency;
    if (options.mode != BenchmarkMode::kMixed || options.readSharePercent == 0) return 0;
    const uint32_t workers = (options.concurrency * options.readSharePercent) / 100;
    return std::max<uint32_t>(1, workers);
}

void stampRecord(RunRecord& record, std::chrono::steady_clock::time_point steadyStart,
                 std::chrono::steady_clock::time_point operationStart,
                 std::chrono::steady_clock::time_point operationEnd)
{
    record.startOffsetMs = std::chrono::duration<double, std::milli>(operationStart - steadyStart).count();
    record.endOffsetMs = std::chrono::duration<double, std::milli>(operationEnd - steadyStart).count();
}

std::vector<std::string> splitCsvRow(const std::string& row)
{
    std::vector<std::string> cells;
    std::string cell;
    bool quoted = false;
    for (size_t index = 0; index < row.size(); ++index) {
        const char character = row[index];
        if (character == '"') {
            if (quoted && index + 1 < row.size() && row[index + 1] == '"') {
                cell += character;
                ++index;
            } else {
                quoted = !quoted;
            }
        } else if (character == ',' && !quoted) {
            cells.push_back(std::move(cell));
            cell.clear();
        } else {
            cell += character;
        }
    }
    cells.push_back(std::move(cell));
    return cells;
}

bool loadDurationFixtures(const BenchmarkOptions& options, uint32_t wanted,
                          std::vector<RunRecord>& fixtures, std::string& error)
{
    std::ifstream input(options.downloadFixtureManifest);
    if (!input) {
        error = "cannot open --download-fixture-manifest: " + options.downloadFixtureManifest.string();
        return false;
    }
    std::string line;
    if (!std::getline(input, line)) {
        error = "download fixture manifest is empty";
        return false;
    }
    const std::vector<std::string> header = splitCsvRow(line);
    const auto column = [&header](const std::string& name) -> size_t {
        const auto found = std::find(header.begin(), header.end(), name);
        return found == header.end() ? header.size() : static_cast<size_t>(found - header.begin());
    };
    const size_t operation = column("operation");
    const size_t objectId = column("object_id");
    const size_t objectVersion = column("object_version");
    const size_t fileHash = column("file_hash");
    const size_t inputHash = column("input_hash");
    const size_t sizeBytes = column("size_bytes");
    const size_t chunkCount = column("chunk_count");
    if (operation == header.size() || objectId == header.size() || objectVersion == header.size() ||
        fileHash == header.size() || inputHash == header.size() || sizeBytes == header.size() ||
        chunkCount == header.size()) {
        error = "download fixture manifest lacks required runs.csv columns";
        return false;
    }
    std::unordered_set<std::string> seen;
    while (std::getline(input, line) && (wanted == 0 || fixtures.size() < wanted)) {
        const std::vector<std::string> row = splitCsvRow(line);
        const size_t required = std::max({operation, objectId, objectVersion, fileHash, inputHash, sizeBytes, chunkCount});
        if (row.size() <= required || row[operation] != "download" || row[objectId].empty()) continue;
        try {
            RunRecord fixture;
            fixture.objectId = row[objectId];
            fixture.objectVersion = std::stoull(row[objectVersion]);
            fixture.fileHash = row[fileHash];
            fixture.inputHash = row[inputHash];
            fixture.sizeBytes = std::stoull(row[sizeBytes]);
            fixture.chunkCount = static_cast<uint32_t>(std::stoul(row[chunkCount]));
            if (fixture.sizeBytes != options.sizes.front() || fixture.objectVersion == 0 ||
                fixture.chunkCount == 0 || fixture.fileHash.empty() || fixture.inputHash.empty()) continue;
            const std::string key = fixture.objectId + ":" + std::to_string(fixture.objectVersion);
            if (seen.insert(key).second) {
                fixture.runId = "external-fixture-" + std::to_string(fixtures.size());
                fixture.uploadOk = true;
                fixture.downloadOk = true;
                fixture.operation = "download";
                fixtures.push_back(std::move(fixture));
            }
        } catch (const std::exception&) {
            continue;
        }
    }
    if (fixtures.empty()) {
        error = "download fixture manifest contains no valid matching ObjectRef entries";
        return false;
    }
    if (wanted > 0 && fixtures.size() < wanted) {
        error = "download fixture manifest has fewer distinct ObjectRefs than --download-fixtures";
        return false;
    }
    return true;
}

int runDurationBenchmark(const BenchmarkOptions& options, std::ostream& console)
{
    std::error_code error;
    const uint32_t downloadWorkers = durationDownloadWorkers(options);
    const uint32_t uploadWorkers = options.concurrency - downloadWorkers;
    const uint32_t fixtureCount = downloadWorkers == 0 ? 0 :
        (options.downloadFixtureCount == 0 ? downloadWorkers : options.downloadFixtureCount);
    std::vector<RunRecord> fixtures;
    if (downloadWorkers > 0) {
        if (!options.downloadFixtureManifest.empty()) {
            std::string fixtureError;
            if (!loadDurationFixtures(options, fixtureCount, fixtures, fixtureError)) {
                console << "duration fixture manifest FAILED: " << fixtureError << '\n';
                return 1;
            }
            console << "reusing " << fixtures.size() << " duration fixtures from "
                    << options.downloadFixtureManifest << '\n';
        } else {
            fixtures.resize(fixtureCount);
            BenchmarkOptions fixtureOptions = options;
            fixtureOptions.mode = BenchmarkMode::kUploadOnly;
            fixtureOptions.durationSeconds = 0;
            fixtureOptions.warmupSeconds = 0;
            for (uint32_t index = 0; index < fixtureCount; ++index) {
                std::ostringstream fixtureLog;
                fixtures[index] = runOne(fixtureOptions, options.sizes.front(),
                    4000000U + index, fixtureLog);
                fixtures[index].operation = "download";
                std::filesystem::remove(options.workDir / (fixtures[index].runId + ".input"), error);
                if (!fixtures[index].error.empty()) {
                    console << "duration fixture FAILED: " << fixtures[index].error << '\n';
                    return 1;
                }
            }
        }
        if (options.fixtureSettleMs > 0) {
            console << "waiting " << options.fixtureSettleMs
                    << " ms for duration fixture load reporting to settle\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(options.fixtureSettleMs));
        }
    }

    const auto runPhase = [&](uint32_t seconds, bool collect,
                              std::chrono::steady_clock::time_point steadyStart,
                              std::vector<RunRecord>& output) {
        if (seconds == 0) return;
        const auto phaseStart = std::chrono::steady_clock::now();
        const auto deadline = phaseStart + std::chrono::seconds(seconds);
        ChunkBudget chunkBudget(options.globalChunkBudget);
        std::vector<std::vector<RunRecord>> workerRecords(options.concurrency);
        std::vector<std::thread> workers;
        std::atomic<uint64_t> fixtureCursor{0};
        // Warmup uploads are real committed objects.  Their catalog names
        // must never be reused by steady-state uploads, and a per-worker
        // counter can also overlap at high IOPS.  Allocate every upload
        // index atomically from a phase-specific range instead.
        std::atomic<uint32_t> uploadRunIndex{collect ? 1000000000U : 500000000U};
        workers.reserve(options.concurrency);
        for (uint32_t worker = 0; worker < options.concurrency; ++worker) {
            workers.emplace_back([&, worker] {
                miniKV::client::ClientConfig persistentConfig;
                persistentConfig.gateway = options.gateway;
                persistentConfig.clusterInternalToken = options.clusterInternalToken;
                persistentConfig.servicePrincipal = options.servicePrincipal;
                miniKV::client::MiniDriverClient persistentClient(std::move(persistentConfig));
                uint64_t sequence = 0;
                while (std::chrono::steady_clock::now() < deadline) {
                    const auto operationStart = std::chrono::steady_clock::now();
                    RunRecord record;
                    if (worker < downloadWorkers) {
                        const RunRecord& fixture = fixtures[
                            static_cast<size_t>(fixtureCursor.fetch_add(1)) % fixtures.size()];
                        record = fixture;
                        record.runId += "-duration-reader-" + std::to_string(worker + 1) +
                            "-op-" + std::to_string(++sequence);
                        std::ostringstream operationLog;
                        record = runDownloadOne(options, record, operationLog,
                            options.keepAlive ? &persistentClient : nullptr);
                        std::error_code removeError;
                        std::filesystem::remove(options.workDir / (record.runId + ".download"), removeError);
                    } else {
                        BenchmarkOptions uploadOptions = options;
                        uploadOptions.mode = BenchmarkMode::kUploadOnly;
                        std::ostringstream operationLog;
                        record = runOne(uploadOptions, options.sizes.front(),
                            uploadRunIndex.fetch_add(1), operationLog, &chunkBudget);
                        std::error_code removeError;
                        std::filesystem::remove(options.workDir / (record.runId + ".input"), removeError);
                    }
                    const auto operationEnd = std::chrono::steady_clock::now();
                    const bool failed = !record.error.empty();
                    if (collect) {
                        stampRecord(record, steadyStart, operationStart, operationEnd);
                        workerRecords[worker].push_back(std::move(record));
                    }
                    // A duration run must not turn an unavailable Gateway or
                    // a saturated admission queue into a tight error loop.
                    // Keep failures observable, but yield briefly before the
                    // next attempt so the test does not become its own DoS.
                    if (failed) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    }
                }
            });
        }
        for (std::thread& worker : workers) worker.join();
        if (collect) {
            for (auto& records : workerRecords) {
                output.insert(output.end(),
                    std::make_move_iterator(records.begin()),
                    std::make_move_iterator(records.end()));
            }
        }
    };

    console << "duration workload warmup=" << options.warmupSeconds
            << "s steady=" << options.durationSeconds << "s concurrency="
            << options.concurrency << " upload_workers=" << uploadWorkers
            << " download_workers=" << downloadWorkers << '\n';
    runPhase(options.warmupSeconds, false, {}, fixtures /* unused output */);
    const auto steadyStart = std::chrono::steady_clock::now();
    std::vector<RunRecord> records;
    runPhase(options.durationSeconds, true, steadyStart, records);

    std::vector<SizeSummary> summaries;
    for (const uint64_t size : options.sizes) {
        const uint32_t requested = static_cast<uint32_t>(std::count_if(
            records.begin(), records.end(), [size](const RunRecord& record) {
                return record.sizeBytes == size;
            }));
        summaries.push_back(summarize(size, requested, records, options.mode));
    }
    if (!writeReports(options, records, summaries, error)) {
        console << "cannot write benchmark reports: " << error.message() << '\n';
        return 2;
    }
    const bool allSucceeded = !records.empty() && std::all_of(records.begin(), records.end(),
        [](const RunRecord& record) {
            return record.uploadOk && record.downloadOk && record.error.empty();
        });
    console << "duration records=" << records.size() << " reports: " << options.workDir << "\n";
    return allSucceeded ? 0 : 1;
}

}

BenchmarkRunner::BenchmarkRunner(BenchmarkOptions options) : options_(std::move(options)) {}

int BenchmarkRunner::run(std::ostream& console) {
    std::error_code filesystemError;
    std::filesystem::create_directories(options_.workDir, filesystemError);
    if (filesystemError) {
        console << "cannot create work directory: " << filesystemError.message() << '\n';
        return 2;
    }
    if (options_.durationSeconds > 0) {
        return runDurationBenchmark(options_, console);
    }
    if (options_.readProfile == BenchmarkReadProfile::kMixedSize) {
        // Prepare one immutable fixture per size, then run readers for every
        // size in the same timed round.  This is intentionally different
        // from the historical `--sizes` loop, which runs sizes serially and
        // cannot expose small-object tail latency behind original-file reads.
        ChunkBudget fixtureBudget(options_.globalChunkBudget);
        BenchmarkOptions fixtureOptions = options_;
        fixtureOptions.mode = BenchmarkMode::kUploadOnly;
        std::vector<RunRecord> fixtures;
        fixtures.reserve(options_.sizes.size());
        for (size_t index = 0; index < options_.sizes.size(); ++index) {
            std::ostringstream fixtureLog;
            RunRecord fixture = runOne(fixtureOptions, options_.sizes[index],
                                       2000000U + static_cast<uint32_t>(index), fixtureLog,
                                       &fixtureBudget);
            fixture.operation = "download";
            console << fixtureLog.str();
            std::error_code removeError;
            std::filesystem::remove(options_.workDir / (fixture.runId + ".input"), removeError);
            if (!fixture.error.empty()) {
                console << "mixed-size fixture FAILED: " << fixture.error << '\n';
                return 1;
            }
            fixtures.push_back(std::move(fixture));
        }
        std::vector<RunRecord> records;
        for (uint32_t round = 0; round < options_.runs; ++round) {
            std::vector<RunRecord> roundRecords(options_.concurrency);
            std::vector<std::string> logs(options_.concurrency);
            std::vector<std::thread> workers;
            workers.reserve(options_.concurrency);
            const auto started = std::chrono::steady_clock::now();
            for (uint32_t worker = 0; worker < options_.concurrency; ++worker) {
                workers.emplace_back([&, worker] {
                    // Rotate the starting fixture each round. With c4 and
                    // five asset sizes, a fixed worker modulo would otherwise
                    // never sample the fifth (50 MiB) original at all.
                    const RunRecord& source = fixtures[
                        (static_cast<size_t>(round) * options_.concurrency + worker) % fixtures.size()];
                    RunRecord fixture = source;
                    fixture.runId += "-mixed-r" + std::to_string(round + 1) +
                        "-reader-" + std::to_string(worker + 1);
                    std::ostringstream log;
                    const bool slow = options_.slowReaderWorkers > 0 &&
                        worker >= options_.concurrency - options_.slowReaderWorkers;
                    roundRecords[worker] = runDownloadOne(options_, fixture, log, nullptr,
                        slow ? options_.slowReaderBytesPerSecond : 0,
                        slow ? "slow" : "normal");
                    logs[worker] = log.str();
                });
            }
            for (std::thread& worker : workers) worker.join();
            uint64_t completedBytes = 0;
            for (uint32_t worker = 0; worker < options_.concurrency; ++worker) {
                console << logs[worker];
                RunRecord& record = roundRecords[worker];
                if (!record.error.empty()) console << "FAILED: " << record.error << '\n';
                if (record.uploadOk && record.downloadOk) completedBytes += record.sizeBytes;
                std::error_code removeError;
                std::filesystem::remove(options_.workDir / (record.runId + ".download"), removeError);
                records.push_back(std::move(record));
            }
            const double elapsed = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - started).count();
            console << "mixed-size round aggregate=" << std::fixed << std::setprecision(2)
                    << mibPerSecond(completedBytes, elapsed) << " MiB/s\n";
        }
        std::vector<SizeSummary> summaries;
        for (const uint64_t size : options_.sizes) {
            const uint32_t requested = static_cast<uint32_t>(std::count_if(
                records.begin(), records.end(), [size](const RunRecord& record) {
                    return record.sizeBytes == size;
                }));
            summaries.push_back(summarize(size, requested, records, options_.mode));
        }
        if (!writeReports(options_, records, summaries, filesystemError)) {
            console << "cannot write benchmark reports: " << filesystemError.message() << '\n';
            return 2;
        }
        const bool allSucceeded = std::all_of(records.begin(), records.end(), [](const RunRecord& record) {
            return record.uploadOk && record.downloadOk && record.error.empty();
        });
        console << "reports: " << options_.workDir << "\n";
        return allSucceeded ? 0 : 1;
    }
    if (options_.mode == BenchmarkMode::kDownloadOnly && options_.requestsPerWorker > 1) {
        // R6 path: retain a StreamRequest per worker while making distinct,
        // complete object reads. The regular path measures one object read per
        // worker and therefore cannot reveal Keep-Alive gains for a 64 KiB
        // single-Chunk object.
        std::vector<RunRecord> records;
        for (const uint64_t size : options_.sizes) {
            for (uint32_t round = 0; round < options_.runs; ++round) {
                const uint32_t fixtureCount = options_.readProfile == BenchmarkReadProfile::kHotObject
                    ? 1 : options_.concurrency;
                std::vector<RunRecord> fixtures(fixtureCount);
                BenchmarkOptions fixtureOptions = options_;
                fixtureOptions.mode = BenchmarkMode::kUploadOnly;
                for (uint32_t index = 0; index < fixtureCount; ++index) {
                    std::ostringstream fixtureLog;
                    fixtures[index] = runOne(fixtureOptions, size,
                        3000000U + round * options_.concurrency + index, fixtureLog);
                    fixtures[index].operation = "download";
                    console << fixtureLog.str();
                    std::error_code removeError;
                    std::filesystem::remove(options_.workDir / (fixtures[index].runId + ".input"), removeError);
                    if (!fixtures[index].error.empty()) {
                        console << "repeated-read fixture FAILED: " << fixtures[index].error << '\n';
                        return 1;
                    }
                }
                std::vector<std::vector<RunRecord>> workerRecords(options_.concurrency);
                std::vector<std::string> logs(options_.concurrency);
                std::vector<std::thread> workers;
                workers.reserve(options_.concurrency);
                const auto started = std::chrono::steady_clock::now();
                for (uint32_t worker = 0; worker < options_.concurrency; ++worker) {
                    workers.emplace_back([&, worker] {
                        miniKV::client::ClientConfig persistentConfig;
                        persistentConfig.gateway = options_.gateway;
                        persistentConfig.clusterInternalToken = options_.clusterInternalToken;
                        persistentConfig.servicePrincipal = options_.servicePrincipal;
                        miniKV::client::MiniDriverClient persistentClient(std::move(persistentConfig));
                        const RunRecord& source = fixtures[options_.readProfile == BenchmarkReadProfile::kHotObject
                            ? 0 : worker];
                        std::ostringstream log;
                        const bool slow = options_.slowReaderWorkers > 0 &&
                            worker >= options_.concurrency - options_.slowReaderWorkers;
                        workerRecords[worker].reserve(options_.requestsPerWorker);
                        for (uint32_t request = 0; request < options_.requestsPerWorker; ++request) {
                            RunRecord fixture = source;
                            fixture.runId += "-round-" + std::to_string(round + 1) +
                                "-reader-" + std::to_string(worker + 1) +
                                "-request-" + std::to_string(request + 1);
                            workerRecords[worker].push_back(runDownloadOne(
                                options_, fixture, log,
                                options_.keepAlive ? &persistentClient : nullptr,
                                slow ? options_.slowReaderBytesPerSecond : 0,
                                slow ? "slow" : "normal"));
                        }
                        logs[worker] = log.str();
                    });
                }
                for (std::thread& worker : workers) worker.join();
                uint64_t completedBytes = 0;
                for (uint32_t worker = 0; worker < options_.concurrency; ++worker) {
                    console << logs[worker];
                    for (RunRecord& record : workerRecords[worker]) {
                        if (!record.error.empty()) console << "FAILED: " << record.error << '\n';
                        if (record.uploadOk && record.downloadOk) completedBytes += record.sizeBytes;
                        std::error_code removeError;
                        std::filesystem::remove(options_.workDir / (record.runId + ".download"), removeError);
                        records.push_back(std::move(record));
                    }
                }
                const double elapsed = std::chrono::duration<double, std::milli>(
                    std::chrono::steady_clock::now() - started).count();
                console << "repeated-read round aggregate=" << std::fixed << std::setprecision(2)
                        << mibPerSecond(completedBytes, elapsed) << " MiB/s\n";
            }
        }
        std::vector<SizeSummary> summaries;
        for (const uint64_t size : options_.sizes) {
            summaries.push_back(summarize(size,
                options_.runs * options_.concurrency * options_.requestsPerWorker,
                records, options_.mode));
        }
        if (!writeReports(options_, records, summaries, filesystemError)) {
            console << "cannot write benchmark reports: " << filesystemError.message() << '\n';
            return 2;
        }
        const bool allSucceeded = std::all_of(records.begin(), records.end(), [](const RunRecord& record) {
            return record.uploadOk && record.downloadOk && record.error.empty();
        });
        console << "reports: " << options_.workDir << "\n";
        return allSucceeded ? 0 : 1;
    }
    std::vector<RunRecord> records;
    for (const uint64_t size : options_.sizes) {
        for (uint32_t runIndex = 0; runIndex < options_.runs; ++runIndex) {
            console << "running size=" << size << " round=" << runIndex + 1 << '/' << options_.runs
                    << " concurrency=" << options_.concurrency << '\n';
            std::vector<RunRecord> roundRecords(options_.concurrency);
            std::vector<std::string> workerLogs(options_.concurrency);
            const uint32_t downloadWorkers = durationDownloadWorkers(options_);
            ChunkBudget chunkBudget(options_.globalChunkBudget);
            std::vector<RunRecord> downloadFixtures(downloadWorkers);
            if (downloadWorkers > 0) {
                BenchmarkOptions fixtureOptions = options_;
                fixtureOptions.mode = BenchmarkMode::kUploadOnly;
                const uint32_t fixtureCount = options_.readProfile == BenchmarkReadProfile::kHotObject
                    ? 1 : downloadWorkers;
                for (uint32_t index = 0; index < fixtureCount; ++index) {
                    std::ostringstream fixtureLog;
                    const uint32_t fixtureRunIndex = 1000000U + runIndex * options_.concurrency + index;
                    downloadFixtures[index] = runOne(fixtureOptions, size, fixtureRunIndex, fixtureLog);
                    downloadFixtures[index].operation = "download";
                    std::error_code removeError;
                    std::filesystem::remove(options_.workDir /
                        (downloadFixtures[index].runId + ".input"), removeError);
                    if (!downloadFixtures[index].error.empty()) {
                        console << "download fixture FAILED: " << downloadFixtures[index].error << '\n';
                    }
                }
                if (options_.readProfile == BenchmarkReadProfile::kHotObject && fixtureCount == 1) {
                    for (uint32_t index = 1; index < downloadWorkers; ++index) {
                        downloadFixtures[index] = downloadFixtures[0];
                    }
                }
                if (options_.fixtureSettleMs > 0) {
                    console << "waiting " << options_.fixtureSettleMs
                            << " ms for fixture load reporting to settle\n";
                    std::this_thread::sleep_for(std::chrono::milliseconds(options_.fixtureSettleMs));
                }
            }
            std::vector<std::thread> workers;
            workers.reserve(options_.concurrency);
            const auto roundStart = std::chrono::steady_clock::now();
            for (uint32_t workerIndex = 0; workerIndex < options_.concurrency; ++workerIndex) {
                workers.emplace_back([&, workerIndex] {
                    std::ostringstream workerConsole;
                    const uint32_t uniqueRunIndex = runIndex * options_.concurrency + workerIndex;
                    if (workerIndex < downloadWorkers) {
                        if (downloadFixtures[workerIndex].uploadOk &&
                            downloadFixtures[workerIndex].error.empty()) {
                            RunRecord fixture = downloadFixtures[workerIndex];
                            // A hot-object read has one source object but every reader
                            // needs a distinct local output path and report identity.
                            if (options_.readProfile == BenchmarkReadProfile::kHotObject) {
                                fixture.runId += "-reader-" + std::to_string(workerIndex + 1);
                            }
                            const bool slow = options_.slowReaderWorkers > 0 &&
                                workerIndex >= options_.concurrency - options_.slowReaderWorkers;
                            roundRecords[workerIndex] = runDownloadOne(options_, fixture, workerConsole,
                                nullptr, slow ? options_.slowReaderBytesPerSecond : 0,
                                slow ? "slow" : "normal");
                        } else {
                            roundRecords[workerIndex] = downloadFixtures[workerIndex];
                        }
                    } else {
                        BenchmarkOptions workerOptions = options_;
                        if (workerOptions.mode == BenchmarkMode::kMixed) {
                            workerOptions.mode = BenchmarkMode::kUploadOnly;
                        }
                        roundRecords[workerIndex] = runOne(workerOptions, size, uniqueRunIndex, workerConsole,
                                                          &chunkBudget);
                    }
                    workerLogs[workerIndex] = workerConsole.str();
                });
            }
            for (std::thread& worker : workers) worker.join();
            const double roundMs = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - roundStart).count();
            uint32_t completedWorkers = 0;
            for (uint32_t workerIndex = 0; workerIndex < options_.concurrency; ++workerIndex) {
                console << workerLogs[workerIndex];
                RunRecord& record = roundRecords[workerIndex];
                if (!record.error.empty()) console << "FAILED: " << record.error << '\n';
                if (record.uploadOk && record.downloadOk) ++completedWorkers;
                if (record.uploadOk && record.downloadOk) {
                    std::error_code removeError;
                    std::filesystem::remove(options_.workDir / (record.runId + ".input"), removeError);
                    removeError.clear();
                    std::filesystem::remove(options_.workDir / (record.runId + ".download"), removeError);
                }
                records.push_back(std::move(record));
            }
            console << "round aggregate=" << std::fixed << std::setprecision(2)
                    << mibPerSecond(size * completedWorkers, roundMs) << " MiB/s"
                    << " completed=" << completedWorkers << '/' << options_.concurrency << '\n';
        }
    }
    std::vector<SizeSummary> summaries;
    for (const uint64_t size : options_.sizes) {
        summaries.push_back(summarize(size, options_.runs * options_.concurrency, records, options_.mode));
    }
    if (!writeReports(options_, records, summaries, filesystemError)) {
        console << "cannot write benchmark reports: " << filesystemError.message() << '\n';
        return 2;
    }
    const bool allSucceeded = std::all_of(records.begin(), records.end(), [](const RunRecord& record) {
        return record.uploadOk && record.downloadOk && record.error.empty();
    });
    console << "reports: " << options_.workDir << "\n";
    return allSucceeded ? 0 : 1;
}

}  // namespace miniKV::benchmark
