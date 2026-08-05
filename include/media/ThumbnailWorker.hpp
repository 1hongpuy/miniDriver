#pragma once

#include "media/RedisTaskConsumer.hpp"
#include "media/JpegThumbnailGenerator.hpp"

#include <cstdint>
#include <deque>
#include <functional>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace miniKV {
namespace network { class EventLoop; }
namespace media {

struct ThumbnailWorkerConfig {
    std::string nodeId;
    std::string gatewayAddress;
    uint16_t gatewayPort = 0;
    std::string clusterSecret;
    std::string tempDir;
    uint32_t maxConcurrentJobs = 1;
    RedisTaskConsumerConfig redis;
};

class ThumbnailWorker : public std::enable_shared_from_this<ThumbnailWorker> {
public:
    static std::shared_ptr<ThumbnailWorker> create(network::EventLoop* loop,
                                                    ThumbnailWorkerConfig config);
    ~ThumbnailWorker();
    ThumbnailWorker(const ThumbnailWorker&) = delete;
    ThumbnailWorker& operator=(const ThumbnailWorker&) = delete;

    bool start();
    void stop();
    void submit(RedisTaskMessage message);

private:
    struct Endpoint { std::string address; uint16_t port = 0; };
    struct SourceChunk { std::string hash; uint64_t size = 0; std::vector<Endpoint> replicas; };
    struct UploadChunk { uint32_t index = 0; std::string hash; uint64_t size = 0; };
    struct CurrentJob {
        RedisTaskMessage message;
        std::string jobId;
        std::string leaseToken;
        std::string sourceFileHash;
        std::string profile;
        std::string derivedFileName;
        JpegThumbnailOptions jpegOptions;
        std::string sourcePath;
        std::string thumbnailPath;
        std::ofstream sourceOutput;
        std::vector<SourceChunk> sourceChunks;
        size_t sourceIndex = 0;
        size_t replicaIndex = 0;
        std::string thumbnailBytes;
        std::vector<UploadChunk> uploadChunks;
        std::vector<UploadChunk> missingChunks;
        size_t uploadIndex = 0;
        std::string derivedSessionId;
    };

    explicit ThumbnailWorker(network::EventLoop* loop, ThumbnailWorkerConfig config);
    void maybeStartNextInLoop();
    void claimCurrentInLoop();
    void fetchSourceManifestInLoop();
    void downloadNextSourceChunkInLoop();
    void handleSourceChunkResponse(std::string bytes, int status, std::string error);
    void generateThumbnailInExecutor();
    void beginDerivedUploadInLoop();
    void planNextUploadChunkInLoop();
    void uploadCurrentChunkInLoop(const std::string& route);
    void commitDerivedUploadInLoop();
    void failCurrentInLoop(bool unsupported, std::string error);
    void finishCurrentInLoop(bool acknowledge);
    void request(std::string method, std::string address, uint16_t port, std::string path,
                 std::string body, std::map<std::string, std::string> headers,
                 size_t maxResponseBytes,
                 std::function<void(int, std::string, std::string)> callback);
    static bool buildSourceManifest(const std::string& json, std::vector<SourceChunk>& chunks,
                                    std::string& error);
    static bool buildUploadManifest(const std::string& path, std::string& bytes,
                                    std::vector<UploadChunk>& chunks, std::string& manifestHash,
                                    std::string& error);
    static std::string chunksJson(const std::vector<UploadChunk>& chunks);

    network::EventLoop* loop_;
    ThumbnailWorkerConfig config_;
    std::unique_ptr<RedisTaskConsumer> consumer_;
    std::deque<RedisTaskMessage> pending_;
    std::unique_ptr<CurrentJob> current_;
    std::thread conversionThread_;
    bool started_ = false;
    bool stopping_ = false;
};

}  // namespace media
}  // namespace miniKV
