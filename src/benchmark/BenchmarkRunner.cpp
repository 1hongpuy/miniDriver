#include "benchmark/BenchmarkRunner.hpp"

#include "benchmark/BenchmarkHttpClient.hpp"
#include "benchmark/BenchmarkInput.hpp"
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
#include <mutex>
#include <openssl/evp.h>
#include <set>
#include <sstream>
#include <thread>

namespace miniKV::benchmark {
namespace {

constexpr size_t kBufferBytes = 64 * 1024;
constexpr int kGatewayTimeoutMs = 30000;
constexpr int kDataNodeTimeoutMs = 60000;

class ChunkBudget {
public:
    explicit ChunkBudget(uint32_t limit) : limit_(limit) {}
    void acquire() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return active_ < limit_; });
        ++active_;
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

bool sha256Range(std::ifstream& input, uint64_t offset, uint64_t length,
                 std::string& hash, std::string& error) {
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
    while (remaining > 0) {
        const size_t wanted = static_cast<size_t>(std::min<uint64_t>(remaining, buffer.size()));
        input.read(buffer.data(), static_cast<std::streamsize>(wanted));
        if (static_cast<size_t>(input.gcount()) != wanted ||
            EVP_DigestUpdate(context, buffer.data(), wanted) != 1) {
            EVP_MD_CTX_free(context); error = "cannot hash benchmark input range"; return false;
        }
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
    if (!sha256Range(input, offset, bytes, chunkHash, error)) return false;
    HttpResponse routeResponse;
    const std::string routeBody = "{\"chunks\":[{\"index\":" + std::to_string(index) +
        ",\"hash\":\"" + chunkHash + "\",\"size\":" + std::to_string(bytes) + "}]}";
    if (!gatewayRequest(options.gateway, "POST", "/api/v2/upload/sessions/" + sessionId + "/routes",
                        routeBody, routeResponse, error)) return false;
    const std::vector<std::string> routes = miniKV::util::jsonObjectArray(routeResponse.body, "routes");
    if (routes.size() != 1) { error = "Gateway returned invalid routes response"; return false; }
    const std::string primaryNodeId = miniKV::util::jsonString(routes[0], "primaryNodeId");
    Endpoint primary{miniKV::util::jsonString(routes[0], "primaryAddress"),
                     static_cast<uint16_t>(miniKV::util::jsonUint(routes[0], "primaryPort"))};
    const std::string uploadToken = miniKV::util::jsonString(routes[0], "uploadToken");
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
    return uploadRange(input, offset, bytes, primary, "/v2/chunks/" + chunkHash, headers, error);
}

bool downloadFile(const Endpoint& gateway, const std::string& fileHash,
                  const std::filesystem::path& outputPath, uint32_t& chunkCount,
                  std::string& error) {
    HttpResponse manifest;
    if (!gatewayRequest(gateway, "GET", "/api/v2/files/" + fileHash + "/manifest", "", manifest, error)) return false;
    const std::vector<std::string> chunks = miniKV::util::jsonObjectArray(manifest.body, "chunks");
    if (chunks.empty()) { error = "manifest has no chunks"; return false; }
    std::ofstream output(outputPath, std::ios::binary | std::ios::trunc);
    if (!output) { error = "cannot create " + outputPath.string(); return false; }
    chunkCount = static_cast<uint32_t>(chunks.size());
    for (size_t index = 0; index < chunks.size(); ++index) {
        const std::string chunkHash = miniKV::util::jsonString(chunks[index], "hash");
        bool read = false;
        std::string lastError;
        for (const std::string& replica : miniKV::util::jsonObjectArray(chunks[index], "replicas")) {
            Endpoint endpoint{miniKV::util::jsonString(replica, "address"),
                              static_cast<uint16_t>(miniKV::util::jsonUint(replica, "httpPort"))};
            HttpResponse response;
            std::string requestError;
            if (endpoint.host.empty() || endpoint.port == 0 ||
                !httpRequest(endpoint, "GET", "/v2/chunks/" + chunkHash, {}, "", kDataNodeTimeoutMs,
                             response, requestError)) {
                lastError = requestError;
                continue;
            }
            if (response.status != 200) {
                lastError = "HTTP " + std::to_string(response.status);
                continue;
            }
            if (miniKV::util::sha256Hex(response.body.data(), response.body.size()) != chunkHash) {
                lastError = "SHA-256 mismatch";
                continue;
            }
            output.write(response.body.data(), static_cast<std::streamsize>(response.body.size()));
            if (!output) { error = "cannot write downloaded chunk"; return false; }
            read = true;
            break;
        }
        if (!read) {
            error = "cannot download chunk " + std::to_string(index) + ": " + lastError;
            return false;
        }
    }
    return true;
}

RunRecord runDownloadOne(const BenchmarkOptions& options, const RunRecord& fixture,
                         std::ostream& console) {
    RunRecord record;
    record.runId = fixture.runId;
    record.operation = "download";
    record.sizeBytes = fixture.sizeBytes;
    record.chunkCount = fixture.chunkCount;
    record.fileHash = fixture.fileHash;
    record.inputHash = fixture.inputHash;
    // The fixture is deliberately prepared before the timed download interval.
    record.uploadOk = true;
    if (record.fileHash.empty() || record.inputHash.empty()) {
        record.error = "download fixture is incomplete";
        return record;
    }
    const std::filesystem::path outputPath = options.workDir / (record.runId + ".download");
    std::string error;
    const auto start = std::chrono::steady_clock::now();
    uint32_t downloadedChunks = 0;
    if (!downloadFile(options.gateway, record.fileHash, outputPath, downloadedChunks, error)) {
        record.error = error;
        return record;
    }
    std::string outputHash;
    if (!sha256File(outputPath, outputHash, error)) { record.error = error; return record; }
    if (outputHash != record.inputHash) { record.error = "downloaded file SHA-256 mismatch"; return record; }
    record.downloadMs = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - start).count();
    record.downloadOk = downloadedChunks == record.chunkCount;
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
    HttpResponse created;
    const std::string fileName = record.runId + ".bin";
    const std::string createBody = "{\"fileName\":\"" + miniKV::util::jsonEscape(fileName) +
        "\",\"dirPath\":\"" + miniKV::util::jsonEscape(options.remoteDir) +
        "\",\"fileSize\":" + std::to_string(size) + "}";
    if (!gatewayRequest(options.gateway, "POST", "/api/v2/upload/sessions", createBody, created, error)) {
        record.error = error; return record;
    }
    const std::string sessionId = miniKV::util::jsonString(created.body, "sessionId");
    if (sessionId.empty()) { record.error = "Gateway returned no sessionId"; return record; }
    HttpResponse session;
    if (!gatewayRequest(options.gateway, "GET", "/api/v2/upload/sessions/" + sessionId, "", session, error)) {
        record.error = error; return record;
    }
    const uint64_t chunkSize = miniKV::util::jsonUint(session.body, "chunkSize");
    const uint32_t totalChunks = static_cast<uint32_t>(miniKV::util::jsonUint(session.body, "totalChunks"));
    if (chunkSize == 0 || totalChunks == 0) { record.error = "Gateway returned invalid session chunking"; return record; }
    record.chunkCount = totalChunks;
    const auto completedValues = miniKV::util::jsonUIntArray(session.body, "completed");
    const std::set<uint32_t> completed(completedValues.begin(), completedValues.end());

    std::vector<uint32_t> pendingChunks;
    for (uint32_t index = 0; index < totalChunks; ++index) {
        if (!completed.count(index)) pendingChunks.push_back(index);
    }
    std::atomic<size_t> nextChunk{0};
    std::atomic<bool> failed{false};
    std::mutex errorMutex;
    std::mutex consoleMutex;
    std::string uploadError;
    const uint32_t workerCount = std::min<uint32_t>(options.chunkWindow,
        static_cast<uint32_t>(pendingChunks.size()));
    std::vector<std::thread> chunkWorkers;
    chunkWorkers.reserve(workerCount);
    for (uint32_t worker = 0; worker < workerCount; ++worker) {
        chunkWorkers.emplace_back([&] {
            for (;;) {
                if (failed.load()) return;
                const size_t task = nextChunk.fetch_add(1);
                if (task >= pendingChunks.size()) return;
                const uint32_t index = pendingChunks[task];
                std::string chunkError;
                ChunkBudgetLease budgetLease(chunkBudget);
                if (failed.load()) return;
                if (!uploadOneChunk(options, inputPath, sessionId, size, chunkSize, index, chunkError)) {
                    std::lock_guard<std::mutex> lock(errorMutex);
                    if (!failed.exchange(true)) uploadError = "chunk " + std::to_string(index) + ": " + chunkError;
                    return;
                }
                std::lock_guard<std::mutex> lock(consoleMutex);
                console << record.runId << " uploaded chunk " << index + 1 << '/' << totalChunks << '\n';
            }
        });
    }
    for (auto& worker : chunkWorkers) worker.join();
    if (failed.load()) { record.error = uploadError; return record; }
    HttpResponse committed;
    if (!gatewayRequest(options.gateway, "POST", "/api/v2/upload/sessions/" + sessionId + "/commit", "{}", committed, error)) {
        record.error = error; return record;
    }
    const std::string fileHash = miniKV::util::jsonString(committed.body, "fileHash");
    if (fileHash.empty()) { record.error = "Gateway returned no fileHash"; return record; }
    record.fileHash = fileHash;
    record.uploadMs = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - uploadStart).count();
    record.uploadOk = true;

    if(options.mode == BenchmarkMode::kUploadOnly) {
        // The upload-capacity mode intentionally stops after durable Gateway
        // commit. Mark the unused download half successful so the existing
        // per-run reporting pipeline can represent a valid upload-only run.
        record.downloadOk = true;
        console << record.runId << " upload=" << std::fixed << std::setprecision(2)
                << mibPerSecond(size, record.uploadMs) << " MiB/s\n";
        return record;
    }

    const auto downloadStart = std::chrono::steady_clock::now();
    uint32_t downloadedChunks = 0;
    if (!downloadFile(options.gateway, fileHash, outputPath, downloadedChunks, error)) {
        record.error = error; return record;
    }
    std::string outputHash;
    if (!sha256File(outputPath, outputHash, error)) { record.error = error; return record; }
    if (outputHash != inputHash) { record.error = "downloaded file SHA-256 mismatch"; return record; }
    record.downloadMs = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - downloadStart).count();
    record.downloadOk = downloadedChunks == totalChunks;
    if (!record.downloadOk) record.error = "manifest chunk count changed during benchmark";
    console << record.runId << " upload=" << std::fixed << std::setprecision(2)
            << mibPerSecond(size, record.uploadMs) << " MiB/s download="
            << mibPerSecond(size, record.downloadMs) << " MiB/s\n";
    return record;
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
    std::vector<RunRecord> records;
    for (const uint64_t size : options_.sizes) {
        for (uint32_t runIndex = 0; runIndex < options_.runs; ++runIndex) {
            console << "running size=" << size << " round=" << runIndex + 1 << '/' << options_.runs
                    << " concurrency=" << options_.concurrency << '\n';
            std::vector<RunRecord> roundRecords(options_.concurrency);
            std::vector<std::string> workerLogs(options_.concurrency);
            const uint32_t downloadWorkers = options_.mode == BenchmarkMode::kDownloadOnly
                ? options_.concurrency
                : options_.mode == BenchmarkMode::kMixed ? options_.concurrency / 2 : 0;
            ChunkBudget chunkBudget(options_.globalChunkBudget);
            std::vector<RunRecord> downloadFixtures(downloadWorkers);
            if (downloadWorkers > 0) {
                BenchmarkOptions fixtureOptions = options_;
                fixtureOptions.mode = BenchmarkMode::kUploadOnly;
                for (uint32_t index = 0; index < downloadWorkers; ++index) {
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
                            roundRecords[workerIndex] = runDownloadOne(options_, downloadFixtures[workerIndex], workerConsole);
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
