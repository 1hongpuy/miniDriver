#include "media/ThumbnailWorker.hpp"

#include "http/AsyncHttpClient.hpp"
#include "media/JpegThumbnailGenerator.hpp"
#include "network/EventLoop.hpp"
#include "utils/AsyncLogger.hpp"
#include "utils/Util.hpp"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <sstream>
#include <utility>

namespace miniKV {
namespace media {
namespace {

constexpr uint32_t kDerivedChunkSize = 4U * 1024U * 1024U;
constexpr size_t kMaxQueuedJobs = 64;
constexpr size_t kMaxDerivedImageBytes = 16U * 1024U * 1024U;

bool isSuccess(int status) { return status >= 200 && status < 300; }

}  // namespace

std::shared_ptr<ThumbnailWorker> ThumbnailWorker::create(network::EventLoop* loop,
                                                          ThumbnailWorkerConfig config)
{
    if(loop == nullptr) return {};
    return std::shared_ptr<ThumbnailWorker>(new ThumbnailWorker(loop, std::move(config)));
}

ThumbnailWorker::ThumbnailWorker(network::EventLoop* loop, ThumbnailWorkerConfig config)
    : loop_(loop), config_(std::move(config))
{
}

ThumbnailWorker::~ThumbnailWorker()
{
    stop();
}

bool ThumbnailWorker::start()
{
    if(started_ || stopping_ || config_.gatewayAddress.empty() || config_.gatewayPort == 0 ||
       config_.clusterSecret.empty() || config_.tempDir.empty() || config_.maxConcurrentJobs != 1 ||
       config_.redis.consumer.empty()) return false;
    std::error_code error;
    std::filesystem::create_directories(config_.tempDir, error);
    if(error) return false;
    consumer_ = std::make_unique<RedisTaskConsumer>(config_.redis);
    const std::weak_ptr<ThumbnailWorker> weakSelf(shared_from_this());
    if(!consumer_->start([weakSelf](RedisTaskMessage message) {
        if(auto self = weakSelf.lock()) {
            self->loop_->queueInLoop([weakSelf, message = std::move(message)]() mutable {
                if(auto worker = weakSelf.lock()) worker->submit(std::move(message));
            });
        }
    })) {
        consumer_.reset();
        return false;
    }
    started_ = true;
    miniKV::utils::logInfo("event=thumbnail_worker_ready node=" + config_.nodeId +
                           " stream=" + config_.redis.thumbnailStream +
                           " consumer=" + config_.redis.consumer);
    return true;
}

void ThumbnailWorker::stop()
{
    if(stopping_) return;
    stopping_ = true;
    if(consumer_) consumer_->stop();
    if(conversionThread_.joinable()) conversionThread_.join();
}

void ThumbnailWorker::submit(RedisTaskMessage message)
{
    if(stopping_ || message.id.empty() || message.jobId.empty()) return;
    if(pending_.size() >= kMaxQueuedJobs) {
        miniKV::utils::logWarn("event=thumbnail_message_deferred job=" + message.jobId +
                               " reason=local_queue_full");
        return;
    }
    pending_.push_back(std::move(message));
    maybeStartNextInLoop();
}

void ThumbnailWorker::maybeStartNextInLoop()
{
    if(stopping_ || current_ || pending_.empty()) return;
    current_ = std::make_unique<CurrentJob>();
    current_->message = std::move(pending_.front());
    pending_.pop_front();
    claimCurrentInLoop();
}

void ThumbnailWorker::request(std::string method, std::string address, uint16_t port, std::string path,
                              std::string body, std::map<std::string, std::string> headers,
                              size_t maxResponseBytes,
                              std::function<void(int, std::string, std::string)> callback)
{
    auto request = http::AsyncHttpRequest::create(loop_);
    auto payload = std::make_shared<std::string>(std::move(body));
    http::AsyncHttpRequestOptions options;
    options.address = std::move(address);
    options.port = port;
    options.method = std::move(method);
    options.path = std::move(path);
    options.headers = std::move(headers);
    options.contentLength = payload->size();
    options.timeoutMs = 30000;
    options.maxResponseBytes = maxResponseBytes;
    request->open(std::move(options), [request, payload] {
        if(request->write(payload->data(), payload->size()) != http::AsyncWriteResult::kAccepted) {
            request->cancel();
            return;
        }
        request->finishBody();
    }, [callback = std::move(callback)](http::HttpClientResponse response, std::string error) mutable {
        callback(response.status, std::move(response.body), std::move(error));
    });
}

void ThumbnailWorker::claimCurrentInLoop()
{
    if(!current_) return;
    const std::string jobId = current_->message.jobId;
    request("POST", config_.gatewayAddress, config_.gatewayPort,
            "/internal/v2/media/jobs/" + jobId + "/claim", "{\"leaseSeconds\":300}",
            {{"Content-Type", "application/json"}, {"X-Cluster-Internal-Token", config_.clusterSecret}},
            64 * 1024,
            [weakSelf = std::weak_ptr<ThumbnailWorker>(shared_from_this())](int status, std::string body,
                                                                              std::string error) {
        auto self = weakSelf.lock();
        if(!self || !self->current_) return;
        if(status == 409) { self->finishCurrentInLoop(true); return; }
        if(!isSuccess(status) || !error.empty()) {
            self->finishCurrentInLoop(false);
            return;
        }
        self->current_->jobId = miniKV::util::jsonString(body, "jobId");
        self->current_->leaseToken = miniKV::util::jsonString(body, "leaseToken");
        self->current_->sourceFileHash = miniKV::util::jsonString(body, "sourceFileHash");
        const std::string profile = miniKV::util::jsonString(body, "profile");
        if(self->current_->jobId.empty() || self->current_->leaseToken.empty() ||
           self->current_->sourceFileHash.empty() ||
           !jpegDerivedProfile(profile, self->current_->jpegOptions,
                               self->current_->derivedFileName)) {
            self->failCurrentInLoop(false, "invalid thumbnail claim response");
            return;
        }
        self->current_->profile = profile;
        self->fetchSourceManifestInLoop();
    });
}

bool ThumbnailWorker::buildSourceManifest(const std::string& json, std::vector<SourceChunk>& chunks,
                                          std::string& error)
{
    const uint64_t fileSize = miniKV::util::jsonUint(json, "fileSize");
    const uint64_t chunkSize = miniKV::util::jsonUint(json, "chunkSize");
    const std::vector<std::string> items = miniKV::util::jsonObjectArray(json, "chunks");
    if(fileSize == 0 || chunkSize == 0 || items.empty()) { error = "invalid source manifest"; return false; }
    chunks.clear();
    for(size_t index = 0; index < items.size(); ++index) {
        SourceChunk chunk;
        chunk.hash = miniKV::util::jsonString(items[index], "hash");
        chunk.size = index + 1 == items.size() ? fileSize - index * chunkSize : chunkSize;
        for(const std::string& node : miniKV::util::jsonObjectArray(items[index], "replicas")) {
            const uint64_t port = miniKV::util::jsonUint(node, "httpPort");
            Endpoint endpoint{miniKV::util::jsonString(node, "address"), static_cast<uint16_t>(port)};
            if(!endpoint.address.empty() && port > 0 && port <= UINT16_MAX) chunk.replicas.push_back(std::move(endpoint));
        }
        if(chunk.hash.empty() || chunk.size == 0 || chunk.replicas.empty()) {
            error = "source manifest has unavailable chunk";
            return false;
        }
        chunks.push_back(std::move(chunk));
    }
    return true;
}

void ThumbnailWorker::fetchSourceManifestInLoop()
{
    if(!current_) return;
    request("GET", config_.gatewayAddress, config_.gatewayPort,
            "/api/v2/files/" + current_->sourceFileHash + "/manifest", "", {}, 2 * 1024 * 1024,
            [weakSelf = std::weak_ptr<ThumbnailWorker>(shared_from_this())](int status, std::string body,
                                                                              std::string error) {
        auto self = weakSelf.lock();
        if(!self || !self->current_) return;
        if(!isSuccess(status) || !error.empty() ||
           !buildSourceManifest(body, self->current_->sourceChunks, error)) {
            self->failCurrentInLoop(false, error.empty() ? "cannot fetch source manifest" : error);
            return;
        }
        self->current_->sourcePath = (std::filesystem::path(self->config_.tempDir) /
                                      (self->current_->jobId + ".source.jpg")).string();
        self->current_->thumbnailPath = (std::filesystem::path(self->config_.tempDir) /
                                         (self->current_->jobId + "." + self->current_->profile + ".jpg")).string();
        self->current_->sourceOutput.open(self->current_->sourcePath, std::ios::binary | std::ios::trunc);
        if(!self->current_->sourceOutput) {
            self->failCurrentInLoop(false, "cannot create source temporary file");
            return;
        }
        self->downloadNextSourceChunkInLoop();
    });
}

void ThumbnailWorker::downloadNextSourceChunkInLoop()
{
    if(!current_) return;
    if(current_->sourceIndex == current_->sourceChunks.size()) {
        current_->sourceOutput.close();
        generateThumbnailInExecutor();
        return;
    }
    SourceChunk& chunk = current_->sourceChunks[current_->sourceIndex];
    if(current_->replicaIndex >= chunk.replicas.size()) {
        failCurrentInLoop(false, "all source replicas failed");
        return;
    }
    const Endpoint endpoint = chunk.replicas[current_->replicaIndex];
    request("GET", endpoint.address, endpoint.port, "/v2/chunks/" + chunk.hash, "", {},
            static_cast<size_t>(chunk.size + 1024),
            [weakSelf = std::weak_ptr<ThumbnailWorker>(shared_from_this())](int status, std::string body,
                                                                              std::string error) {
        if(auto self = weakSelf.lock()) self->handleSourceChunkResponse(std::move(body), status, std::move(error));
    });
}

void ThumbnailWorker::handleSourceChunkResponse(std::string bytes, int status, std::string error)
{
    if(!current_) return;
    const SourceChunk& chunk = current_->sourceChunks[current_->sourceIndex];
    if(!isSuccess(status) || !error.empty() || bytes.size() != chunk.size ||
       miniKV::util::sha256Hex(bytes.data(), bytes.size()) != chunk.hash) {
        ++current_->replicaIndex;
        downloadNextSourceChunkInLoop();
        return;
    }
    current_->sourceOutput.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    if(!current_->sourceOutput) {
        failCurrentInLoop(false, "cannot write source temporary file");
        return;
    }
    ++current_->sourceIndex;
    current_->replicaIndex = 0;
    downloadNextSourceChunkInLoop();
}

void ThumbnailWorker::generateThumbnailInExecutor()
{
    if(!current_) return;
    if(conversionThread_.joinable()) conversionThread_.join();
    const std::string source = current_->sourcePath;
    const std::string output = current_->thumbnailPath;
    const JpegThumbnailOptions options = current_->jpegOptions;
    const std::weak_ptr<ThumbnailWorker> weakSelf(shared_from_this());
    conversionThread_ = std::thread([weakSelf, source, output, options] {
        JpegThumbnailResult result;
        const bool success = generateJpegThumbnail(source, output, result, options);
        if(auto self = weakSelf.lock()) {
            self->loop_->queueInLoop([weakSelf, success, unsupported = result.unsupported,
                                      error = std::move(result.error)]() mutable {
                auto worker = weakSelf.lock();
                if(!worker || !worker->current_) return;
                if(!success) {
                    worker->failCurrentInLoop(unsupported,
                                              error.empty() ? "JPEG thumbnail generation failed" : error);
                    return;
                }
                worker->beginDerivedUploadInLoop();
            });
        }
    });
}

bool ThumbnailWorker::buildUploadManifest(const std::string& path, std::string& bytes,
                                          std::vector<UploadChunk>& chunks, std::string& manifestHash,
                                          std::string& error)
{
    std::ifstream input(path, std::ios::binary);
    if(!input) { error = "cannot open generated thumbnail"; return false; }
    std::ostringstream content;
    content << input.rdbuf();
    bytes = content.str();
    if(bytes.empty() || bytes.size() > kMaxDerivedImageBytes) { error = "derived image output size is invalid"; return false; }
    chunks.clear();
    for(size_t offset = 0, index = 0; offset < bytes.size(); offset += kDerivedChunkSize, ++index) {
        const size_t count = std::min<size_t>(kDerivedChunkSize, bytes.size() - offset);
        chunks.push_back({static_cast<uint32_t>(index), miniKV::util::sha256Hex(bytes.data() + offset, count), count});
    }
    std::string canonical = "minikv-manifest-v1\n" + std::to_string(bytes.size()) + "\n" +
                            std::to_string(kDerivedChunkSize) + "\n";
    for(const UploadChunk& chunk : chunks) {
        canonical += std::to_string(chunk.index) + ":" + chunk.hash + ":" +
                     std::to_string(chunk.size) + "\n";
    }
    manifestHash = miniKV::util::sha256Hex(canonical.data(), canonical.size());
    return true;
}

std::string ThumbnailWorker::chunksJson(const std::vector<UploadChunk>& chunks)
{
    std::ostringstream out;
    out << "[";
    for(size_t index = 0; index < chunks.size(); ++index) {
        if(index != 0) out << ',';
        out << "{\"index\":" << chunks[index].index << ",\"hash\":\"" << chunks[index].hash
            << "\",\"size\":" << chunks[index].size << "}";
    }
    out << "]";
    return out.str();
}

void ThumbnailWorker::beginDerivedUploadInLoop()
{
    if(!current_) return;
    std::string manifestHash;
    std::string error;
    if(!buildUploadManifest(current_->thumbnailPath, current_->thumbnailBytes,
                            current_->uploadChunks, manifestHash, error)) {
        failCurrentInLoop(false, error);
        return;
    }
    const std::string body = "{\"leaseToken\":\"" + miniKV::util::jsonEscape(current_->leaseToken) +
        "\",\"fileName\":\"" + miniKV::util::jsonEscape(current_->derivedFileName) +
        "\",\"fileSize\":" +
        std::to_string(current_->thumbnailBytes.size()) + ",\"chunkSize\":" +
        std::to_string(kDerivedChunkSize) + ",\"manifestHash\":\"" + manifestHash +
        "\",\"chunks\":" + chunksJson(current_->uploadChunks) + "}";
    request("POST", config_.gatewayAddress, config_.gatewayPort,
            "/internal/v2/media/jobs/" + current_->jobId + "/derived-uploads", body,
            {{"Content-Type", "application/json"}, {"X-Cluster-Internal-Token", config_.clusterSecret}},
            2 * 1024 * 1024,
            [weakSelf = std::weak_ptr<ThumbnailWorker>(shared_from_this())](int status, std::string response,
                                                                              std::string requestError) {
        auto self = weakSelf.lock();
        if(!self || !self->current_) return;
        if(!isSuccess(status) || !requestError.empty()) {
            self->failCurrentInLoop(false, requestError.empty() ? "cannot create derived upload" : requestError);
            return;
        }
        self->current_->derivedSessionId = miniKV::util::jsonString(response, "sessionId");
        const std::vector<std::string> missing = miniKV::util::jsonObjectArray(response, "missingChunks");
        for(const std::string& item : missing) {
            const uint64_t index = miniKV::util::jsonUint(item, "index", UINT32_MAX);
            if(index >= self->current_->uploadChunks.size()) {
                self->failCurrentInLoop(false, "invalid derived upload response");
                return;
            }
            self->current_->missingChunks.push_back(self->current_->uploadChunks[index]);
        }
        if(self->current_->derivedSessionId.empty()) {
            self->failCurrentInLoop(false, "invalid derived session");
            return;
        }
        self->planNextUploadChunkInLoop();
    });
}

void ThumbnailWorker::planNextUploadChunkInLoop()
{
    if(!current_) return;
    if(current_->uploadIndex == current_->missingChunks.size()) {
        commitDerivedUploadInLoop();
        return;
    }
    const UploadChunk& chunk = current_->missingChunks[current_->uploadIndex];
    const std::string body = "{\"chunks\":" + chunksJson({chunk}) + "}";
    request("POST", config_.gatewayAddress, config_.gatewayPort,
            "/api/v2/upload/sessions/" + current_->derivedSessionId + "/routes", body,
            {{"Content-Type", "application/json"}}, 2 * 1024 * 1024,
            [weakSelf = std::weak_ptr<ThumbnailWorker>(shared_from_this())](int status, std::string response,
                                                                              std::string error) {
        auto self = weakSelf.lock();
        if(!self || !self->current_) return;
        if(!isSuccess(status) || !error.empty()) {
            self->failCurrentInLoop(false, error.empty() ? "cannot plan derived route" : error);
            return;
        }
        const auto routes = miniKV::util::jsonObjectArray(response, "routes");
        if(routes.size() != 1) {
            self->failCurrentInLoop(false, "invalid derived route response");
            return;
        }
        self->uploadCurrentChunkInLoop(routes.front());
    });
}

void ThumbnailWorker::uploadCurrentChunkInLoop(const std::string& route)
{
    if(!current_ || current_->uploadIndex >= current_->missingChunks.size()) return;
    const UploadChunk& chunk = current_->missingChunks[current_->uploadIndex];
    const std::string primaryAddress = miniKV::util::jsonString(route, "primaryAddress");
    const uint64_t primaryPort = miniKV::util::jsonUint(route, "primaryPort");
    const std::string primaryNodeId = miniKV::util::jsonString(route, "primaryNodeId");
    const std::string uploadToken = miniKV::util::jsonString(route, "uploadToken");
    std::vector<std::string> chain;
    for(const std::string& node : miniKV::util::jsonObjectArray(route, "chain")) {
        const std::string id = miniKV::util::jsonString(node, "nodeId");
        const std::string address = miniKV::util::jsonString(node, "address");
        const uint64_t port = miniKV::util::jsonUint(node, "httpPort");
        if(id.empty() || address.empty() || port == 0 || port > UINT16_MAX) {
            failCurrentInLoop(false, "invalid derived replica chain");
            return;
        }
        chain.push_back(id + "@" + address + ":" + std::to_string(port));
    }
    if(primaryAddress.empty() || primaryPort == 0 || primaryPort > UINT16_MAX || primaryNodeId.empty() ||
       uploadToken.empty() || chain.empty()) {
        failCurrentInLoop(false, "invalid derived upload route");
        return;
    }
    const size_t offset = static_cast<size_t>(chunk.index) * kDerivedChunkSize;
    const std::string body = current_->thumbnailBytes.substr(offset, static_cast<size_t>(chunk.size));
    request("PUT", primaryAddress, static_cast<uint16_t>(primaryPort), "/v2/chunks/" + chunk.hash, body,
            {{"Content-Type", "application/octet-stream"}, {"X-Session-Id", current_->derivedSessionId},
             {"X-Chunk-Index", std::to_string(chunk.index)}, {"X-Commit-Owner", primaryNodeId},
             {"X-Gateway-Address", config_.gatewayAddress}, {"X-Gateway-Port", std::to_string(config_.gatewayPort)},
             {"X-Replica-Chain", miniKV::util::join(chain, ';')}, {"X-Replica-Position", "0"},
             {"X-Upload-Token", uploadToken}},
            64 * 1024,
            [weakSelf = std::weak_ptr<ThumbnailWorker>(shared_from_this())](int status, std::string,
                                                                              std::string error) {
        auto self = weakSelf.lock();
        if(!self || !self->current_) return;
        if(!isSuccess(status) || !error.empty()) {
            self->failCurrentInLoop(false, error.empty() ? "derived chunk upload failed" : error);
            return;
        }
        ++self->current_->uploadIndex;
        self->planNextUploadChunkInLoop();
    });
}

void ThumbnailWorker::commitDerivedUploadInLoop()
{
    if(!current_) return;
    const std::string body = "{\"leaseToken\":\"" + miniKV::util::jsonEscape(current_->leaseToken) + "\"}";
    request("POST", config_.gatewayAddress, config_.gatewayPort,
            "/internal/v2/media/jobs/" + current_->jobId + "/derived-uploads/" +
            current_->derivedSessionId + "/commit", body,
            {{"Content-Type", "application/json"}, {"X-Cluster-Internal-Token", config_.clusterSecret}},
            64 * 1024,
            [weakSelf = std::weak_ptr<ThumbnailWorker>(shared_from_this())](int status, std::string,
                                                                              std::string error) {
        auto self = weakSelf.lock();
        if(!self || !self->current_) return;
        if(!isSuccess(status) || !error.empty()) {
            self->failCurrentInLoop(false, error.empty() ? "derived upload commit failed" : error);
            return;
        }
        miniKV::utils::logInfo("event=derived_image_ready job=" + self->current_->jobId +
                               " profile=" + self->current_->profile);
        self->finishCurrentInLoop(true);
    });
}

void ThumbnailWorker::failCurrentInLoop(bool unsupported, std::string error)
{
    if(!current_) return;
    if(current_->leaseToken.empty()) { finishCurrentInLoop(false); return; }
    const std::string body = "{\"leaseToken\":\"" + miniKV::util::jsonEscape(current_->leaseToken) +
        "\",\"unsupported\":" + (unsupported ? "1" : "0") + ",\"error\":\"" +
        miniKV::util::jsonEscape(error.empty() ? "thumbnail worker failure" : error) +
        "\",\"retryAfterSeconds\":60}";
    request("POST", config_.gatewayAddress, config_.gatewayPort,
            "/internal/v2/media/jobs/" + current_->jobId + "/fail", body,
            {{"Content-Type", "application/json"}, {"X-Cluster-Internal-Token", config_.clusterSecret}},
            64 * 1024,
            [weakSelf = std::weak_ptr<ThumbnailWorker>(shared_from_this())](int status, std::string,
                                                                              std::string requestError) {
        if(auto self = weakSelf.lock()) {
            miniKV::utils::logWarn("event=thumbnail_failed status=" + std::to_string(status) +
                                   " error=" + (requestError.empty() ? "-" : requestError));
            self->finishCurrentInLoop(isSuccess(status) && requestError.empty());
        }
    });
}

void ThumbnailWorker::finishCurrentInLoop(bool acknowledge)
{
    if(!current_) return;
    if(current_->sourceOutput.is_open()) current_->sourceOutput.close();
    std::error_code error;
    std::filesystem::remove(current_->sourcePath, error);
    std::filesystem::remove(current_->thumbnailPath, error);
    const std::string messageId = current_->message.id;
    current_.reset();
    if(acknowledge && consumer_) consumer_->acknowledge(messageId);
    maybeStartNextInLoop();
}

}  // namespace media
}  // namespace miniKV
