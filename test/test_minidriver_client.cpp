#include "client/MiniDriverClient.hpp"
#include "TestCheck.hpp"
#include "utils/Util.hpp"

#include <arpa/inet.h>
#include <array>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <unistd.h>

namespace {

class OneShotHttpServer {
public:
    explicit OneShotHttpServer(std::string response) : response_(std::move(response)) {
        fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        MINIKV_CHECK(fd_ >= 0);
        int reuse = 1;
        MINIKV_CHECK(::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        MINIKV_CHECK(::bind(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        MINIKV_CHECK(::listen(fd_, 1) == 0);
        socklen_t length = sizeof(address);
        MINIKV_CHECK(::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &length) == 0);
        port_ = ntohs(address.sin_port);
        thread_ = std::thread([this] { serve(); });
    }

    ~OneShotHttpServer() {
        if (fd_ >= 0) ::close(fd_);
        if (thread_.joinable()) thread_.join();
    }

    uint16_t port() const { return port_; }
    const std::string& request() const { return request_; }

private:
    void serve() {
        const int peer = ::accept(fd_, nullptr, nullptr);
        if (peer < 0) return;
        std::array<char, 512> buffer{};
        while (request_.find("\r\n\r\n") == std::string::npos) {
            const ssize_t received = ::recv(peer, buffer.data(), buffer.size(), 0);
            if (received <= 0) { ::close(peer); return; }
            request_.append(buffer.data(), static_cast<size_t>(received));
        }
        size_t sent = 0;
        while (sent < response_.size()) {
            const ssize_t written = ::send(peer, response_.data() + sent, response_.size() - sent, MSG_NOSIGNAL);
            if (written <= 0) break;
            sent += static_cast<size_t>(written);
        }
        ::close(peer);
    }

    int fd_ = -1;
    uint16_t port_ = 0;
    std::string response_;
    std::string request_;
    std::thread thread_;
};

std::string response(const std::string& body) {
    return "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(body.size()) +
        "\r\nConnection: close\r\n\r\n" + body;
}

std::string rangeResponse(const std::string& body, uint64_t start, uint64_t fullLength) {
    return "HTTP/1.1 206 Partial Content\r\nContent-Length: " + std::to_string(body.size()) +
        "\r\nContent-Range: bytes " + std::to_string(start) + "-" +
        std::to_string(start + body.size() - 1) + "/" + std::to_string(fullLength) +
        "\r\nConnection: close\r\n\r\n" + body;
}

std::string readFile(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

}  // namespace

int main() {
    // The first replica returns data of the expected length but wrong content.
    // The client must reject it by checksum and use the second static candidate.
    OneShotHttpServer badReplica(response("bad"));
    OneShotHttpServer goodReplica(response("good"));
    const std::string digest = miniKV::util::sha256Hex("good", 4);
    const std::string planBody =
        "{\"objectId\":\"object-a\",\"objectVersion\":7,\"fileSize\":4,\"chunkSize\":4,\"chunks\":[{"
        "\"index\":0,\"chunkId\":\"chunk-a\",\"storageIdentity\":\"chunk-a\",\"size\":4,"
        "\"checksumType\":\"sha256\",\"checksumDigest\":\"" + digest + "\","
        "\"readCapability\":\"capability-a\",\"replicas\":[{\"nodeId\":\"bad\",\"address\":\"127.0.0.1\",\"port\":" +
        std::to_string(badReplica.port()) + "},{\"nodeId\":\"good\",\"address\":\"127.0.0.1\",\"port\":" +
        std::to_string(goodReplica.port()) + "}]}]}";
    OneShotHttpServer gateway(response(planBody));

    miniKV::client::ClientConfig config;
    config.gateway = {"127.0.0.1", gateway.port()};
    config.clusterInternalToken = "cluster-token";
    config.servicePrincipal = "worker-a";
    miniKV::client::MiniDriverClient client(config);
    miniKV::client::ObjectReadPlan plan;
    std::string error;
    MINIKV_CHECK(client.getReadPlan({"object-a", 7}, plan, error));
    MINIKV_CHECK(plan.capabilityBound);
    MINIKV_CHECK(plan.chunks.size() == 1);

    const std::filesystem::path output = std::filesystem::temp_directory_path() /
        "minikv_client_readplan_fallback_test.bin";
    std::error_code filesystemError;
    std::filesystem::remove(output, filesystemError);
    miniKV::client::TransferStats stats;
    miniKV::client::ReadOptions options;
    options.keepAlive = false;
    options.maxReplicaAttempts = 2;
    MINIKV_CHECK(client.downloadToFile(plan, output, options, stats, error));
    MINIKV_CHECK(stats.replicaFallbacks == 1);
    MINIKV_CHECK(stats.bytesVerified == 4);
    MINIKV_CHECK(readFile(output) == "good");
    MINIKV_CHECK(gateway.request().find("POST /internal/v3/objects/object-a/versions/7/read-plan") != std::string::npos);
    MINIKV_CHECK(gateway.request().find("X-Cluster-Internal-Token: cluster-token") != std::string::npos);
    MINIKV_CHECK(badReplica.request().find("X-Read-Token: capability-a") != std::string::npos);
    MINIKV_CHECK(goodReplica.request().find("X-Read-Token: capability-a") != std::string::npos);
    std::filesystem::remove(output, filesystemError);

    // Object-level ranges are mapped to the relevant Chunk range. Without a
    // segment checksum sidecar a partial Chunk stays explicitly unverified,
    // even though the SDK validates HTTP status/length and may retry replicas.
    OneShotHttpServer rangeReplica(rangeResponse("cdef", 2, 6));
    miniKV::client::ObjectReadPlan rangePlan;
    rangePlan.object = {"object-range", 1};
    rangePlan.fileSize = 6;
    rangePlan.chunkSize = 6;
    rangePlan.capabilityBound = false;
    miniKV::client::ChunkReadPlan rangeChunk;
    rangeChunk.index = 0;
    rangeChunk.chunkId = "chunk-range";
    rangeChunk.storageIdentity = "chunk-range";
    rangeChunk.size = 6;
    rangeChunk.checksumType = "sha256";
    rangeChunk.checksumDigest = miniKV::util::sha256Hex("abcdef", 6);
    rangeChunk.replicas.push_back({"range", {"127.0.0.1", rangeReplica.port()}});
    rangePlan.chunks.push_back(std::move(rangeChunk));
    const std::filesystem::path rangeOutput = std::filesystem::temp_directory_path() /
        "minikv_client_object_range_test.bin";
    std::filesystem::remove(rangeOutput, filesystemError);
    miniKV::client::RangeReadResult rangeResult;
    miniKV::client::TransferStats rangeStats;
    MINIKV_CHECK(client.downloadRangeToFile(rangePlan, 2, 4, rangeOutput, options,
                                            rangeStats, rangeResult, error));
    MINIKV_CHECK(rangeResult.bytesRead == 4);
    MINIKV_CHECK(rangeResult.integrity == miniKV::client::IntegrityStatus::kUnverifiedPartialRange);
    MINIKV_CHECK(rangeStats.bytesVerified == 0);
    MINIKV_CHECK(readFile(rangeOutput) == "cdef");
    MINIKV_CHECK(rangeReplica.request().find("Range: bytes=2-5") != std::string::npos);
    std::filesystem::remove(rangeOutput, filesystemError);

    // A logical object range may cross storage Chunk boundaries. The SDK
    // issues independent byte ranges and reassembles the exact object order;
    // both pieces remain unverified because neither covers a whole Chunk.
    OneShotHttpServer firstCrossReplica(rangeResponse("bc", 1, 3));
    OneShotHttpServer secondCrossReplica(rangeResponse("de", 0, 3));
    miniKV::client::ObjectReadPlan crossPlan;
    crossPlan.object = {"object-cross-range", 1};
    crossPlan.fileSize = 6;
    crossPlan.chunkSize = 3;
    for (uint32_t index = 0; index < 2; ++index) {
        miniKV::client::ChunkReadPlan chunk;
        chunk.index = index;
        chunk.chunkId = "chunk-cross-" + std::to_string(index);
        chunk.storageIdentity = chunk.chunkId;
        chunk.size = 3;
        chunk.checksumType = "sha256";
        chunk.checksumDigest = miniKV::util::sha256Hex(index == 0 ? "abc" : "def", 3);
        const uint16_t port = index == 0 ? firstCrossReplica.port() : secondCrossReplica.port();
        chunk.replicas.push_back({"cross", {"127.0.0.1", port}});
        crossPlan.chunks.push_back(std::move(chunk));
    }
    const std::filesystem::path crossOutput = std::filesystem::temp_directory_path() /
        "minikv_client_cross_chunk_range_test.bin";
    std::filesystem::remove(crossOutput, filesystemError);
    miniKV::client::RangeReadResult crossResult;
    miniKV::client::TransferStats crossStats;
    MINIKV_CHECK(client.downloadRangeToFile(crossPlan, 1, 4, crossOutput, options,
                                            crossStats, crossResult, error));
    MINIKV_CHECK(crossResult.bytesRead == 4);
    MINIKV_CHECK(crossResult.integrity == miniKV::client::IntegrityStatus::kUnverifiedPartialRange);
    MINIKV_CHECK(readFile(crossOutput) == "bcde");
    MINIKV_CHECK(firstCrossReplica.request().find("Range: bytes=1-2") != std::string::npos);
    MINIKV_CHECK(secondCrossReplica.request().find("Range: bytes=0-1") != std::string::npos);
    std::filesystem::remove(crossOutput, filesystemError);
    // Replaying a completed Raft command must be success, not a malformed
    // "no sessionId" error. No DataNode call is necessary for CONTENT_EXISTS.
    const std::filesystem::path uploadInput = std::filesystem::temp_directory_path() /
        "minikv_client_content_exists_test.bin";
    { std::ofstream input(uploadInput, std::ios::binary); input << "same-content"; }
    OneShotHttpServer existingGateway(response("{\"status\":\"CONTENT_EXISTS\",\"object\":{\"objectId\":\"object-existing\",\"objectVersion\":3,\"fileHash\":\"hash-existing\",\"fileSize\":12,\"state\":\"AVAILABLE\"}}"));
    miniKV::client::ClientConfig uploadConfig;
    uploadConfig.gateway = {"127.0.0.1", existingGateway.port()};
    miniKV::client::MiniDriverClient uploadClient(uploadConfig);
    miniKV::client::UploadOptions uploadOptions;
    uploadOptions.commandId = "retry-command-1";
    miniKV::client::UploadResult uploadResult;
    ::setenv("MINIKV_METADATA_MODE", "raft", 1);
    MINIKV_CHECK(uploadClient.uploadFile(uploadInput, "same-content.bin", "/", uploadOptions, uploadResult, error));
    ::unsetenv("MINIKV_METADATA_MODE");
    MINIKV_CHECK(uploadResult.object.objectId == "object-existing");
    MINIKV_CHECK(uploadResult.object.objectVersion == 3);
    MINIKV_CHECK(uploadResult.fileHash == "hash-existing");
    MINIKV_CHECK(existingGateway.request().find("POST /api/v2/upload/preflight") != std::string::npos);
    std::filesystem::remove(uploadInput, filesystemError);
    return 0;
}
