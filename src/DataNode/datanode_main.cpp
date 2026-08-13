#include "http/DeferredResponse.hpp"
#include "http/HttpContext.hpp"
#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "http/HttpServer.hpp"
#include "network/EventLoop.hpp"
#include "network/TcpConnection.hpp"
#include "http/AsyncHttpClient.hpp"
#include "http/CorsPolicy.hpp"
#include "DataNode/FastDataStore.hpp"
#include "DataNode/ChunkDiskWritePipeline.hpp"
#include "DataNode/DiskWriteExecutor.hpp"
#include "DataNode/HttpGatewayControlClient.hpp"
#include "DataNode/NodeResourceGovernor.hpp"
#include "DataNode/ReplicaUploadPipe.hpp"
#include "utils/AsyncLogger.hpp"
#include "utils/Util.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <sys/statvfs.h>
#include <vector>

using miniKV::http::DeferredResponse;
using miniKV::http::HttpContext;
using miniKV::http::HttpRequest;
using miniKV::http::HttpResponse;
using miniKV::network::EventLoop;
using miniKV::network::TcpConnectionPtr;
using namespace miniKV::util;
using namespace miniKV::datanode;

namespace {

constexpr uint32_t kDefaultMaxConcurrentWrites = 2;
constexpr uint32_t kDefaultMaxConcurrentDownloads = 8;
constexpr uint32_t kDefaultMaxUploadsPerClient = 2;
constexpr uint32_t kDefaultMaxDownloadsPerClient = 4;
constexpr uint32_t kDefaultIoThreads = 2;
constexpr size_t kDefaultSendFileQuantumBytes = 256 * 1024;

struct ReplicaTarget {
    std::string nodeId;
    std::string address;
    uint16_t port = 0;
};

bool beginsWith(const std::string& value, const std::string& prefix)
{
    return value.rfind(prefix, 0) == 0;
}

void json(HttpResponse* response, int status, const std::string& body)
{
    response->setStatusCode(static_cast<HttpResponse::HttpStatusCode>(status));
    response->setContentType("application/json");
    response->setBody(body);
}

std::string configuredSecret(int argc, char** argv)
{
    if(argc > 7) return argv[7];
    if(const char* value = std::getenv("MINIKV_V2_CLUSTER_SECRET")) return value;
    return {};
}

std::string configuredAllowedOrigin()
{
    if(const char* value = std::getenv("MINIKV_V2_ALLOWED_ORIGIN")) return value;
    return {};
}

std::string configuredNodeId(const std::string& fallback)
{
    if(const char* value = std::getenv("MINIKV_V2_NODE_ID")) return value;
    return fallback;
}

uint32_t configuredLimit(const char* name, uint32_t fallback)
{
    const char* value = std::getenv(name);
    if(value == nullptr || *value == '\0') return fallback;
    try {
        const unsigned long parsed = std::stoul(value);
        if(parsed == 0 || parsed > UINT32_MAX) return fallback;
        return static_cast<uint32_t>(parsed);
    } catch(...) {
        return fallback;
    }
}

size_t configuredByteLimit(const char* name, size_t fallback)
{
    const char* value = std::getenv(name);
    if(value == nullptr || *value == '\0') return fallback;
    try {
        const unsigned long long parsed = std::stoull(value);
        if(parsed == 0 || parsed > static_cast<unsigned long long>(SIZE_MAX)) return fallback;
        return static_cast<size_t>(parsed);
    } catch(...) {
        return fallback;
    }
}

uint32_t configuredIoThreads()
{
    const char* value = std::getenv("MINIKV_V4_IO_THREADS");
    if(value == nullptr || *value == '\0') return kDefaultIoThreads;
    try {
        const unsigned long parsed = std::stoul(value);
        if(parsed <= 32) return static_cast<uint32_t>(parsed);
    } catch(...) {
    }
    miniKV::utils::logWarn("event=invalid_io_thread_config fallback=2");
    return kDefaultIoThreads;
}

std::vector<std::string> configuredCapabilities()
{
    if(const char* value = std::getenv("MINIKV_V2_NODE_CAPABILITIES")) {
        std::vector<std::string> capabilities;
        for(std::string capability : split(value, ',')) {
            if(!capability.empty()) capabilities.push_back(std::move(capability));
        }
        if(!capabilities.empty()) return capabilities;
    }
    return {"storage"};
}


bool parseReplicaTarget(const std::string& value, ReplicaTarget& out)
{
    const size_t at = value.find('@');
    const size_t colon = value.rfind(':');
    if(at == std::string::npos || colon == std::string::npos || colon <= at + 1) return false;
    try {
        out.nodeId = value.substr(0, at);
        out.address = value.substr(at + 1, colon - at - 1);
        const auto parsedPort = std::stoul(value.substr(colon + 1));
        if(out.nodeId.empty() || out.address.empty() || parsedPort == 0 || parsedPort > UINT16_MAX)
            return false;
        out.port = static_cast<uint16_t>(parsedPort);
        return true;
    } catch(...) {
        return false;
    }
}

std::vector<ReplicaTarget> parseReplicaChain(const std::string& value)
{
    std::vector<ReplicaTarget> out;
    for(const auto& item : split(value, ';')) {
        ReplicaTarget target;
        if(!parseReplicaTarget(item, target)) return {};
        out.push_back(std::move(target));
    }
    return out;
}

std::string joinReplicaChain(const std::vector<ReplicaTarget>& chain)
{
    std::vector<std::string> values;
    for(const auto& node : chain) {
        values.push_back(node.nodeId + "@" + node.address + ":" + std::to_string(node.port));
    }
    return join(values, ';');
}

bool sameChain(const std::vector<ReplicaTarget>& chain, const std::vector<std::string>& expected)
{
    return joinReplicaChain(chain) == join(expected, ';');
}

uint64_t availableBytes(const std::string& path)
{
    struct statvfs fs {};
    return ::statvfs(path.c_str(), &fs) == 0
        ? static_cast<uint64_t>(fs.f_bavail) * fs.f_frsize : 0;
}

uint64_t effectiveFreeBytes(const FastDataStore& store, const std::string& path)
{
    const uint64_t filesystemFree = availableBytes(path);
    const uint64_t reusable = store.reusableBytes();
    return UINT64_MAX - filesystemFree < reusable ? UINT64_MAX : filesystemFree + reusable;
}

uint64_t logicalUsedBytes(const FastDataStore& store)
{
    const uint64_t physicalHighWater = store.usedBytes();
    const uint64_t reusable = store.reusableBytes();
    return reusable >= physicalHighWater ? 0 : physicalHighWater - reusable;
}

class ChunkUploadStream : public std::enable_shared_from_this<ChunkUploadStream> {
public:
    using Clock = std::chrono::steady_clock;

    ChunkUploadStream(EventLoop* loop, FastDataStore& store,
                      NodeResourceGovernor& resourceGovernor,
                      DiskWriteExecutor& diskExecutor,
                      ReplicaConnectionPool::Ptr replicaConnectionPool,
                      std::string nodeId,
                      std::string gatewayAddress, uint16_t gatewayPort,
                      std::string clusterSecret, CorsPolicy corsPolicy,
                      std::string requestOrigin, const HttpRequest& request, std::string chunkHash)
        : loop_(loop), store_(store), resourceGovernor_(resourceGovernor),
            diskExecutor_(diskExecutor),
            replicaConnectionPool_(std::move(replicaConnectionPool)),
            nodeId_(std::move(nodeId)),
            gatewayAddress_(std::move(gatewayAddress)), gatewayPort_(gatewayPort),
            clusterSecret_(std::move(clusterSecret)),
            gatewayControl_(std::make_unique<HttpGatewayControlClient>(
                loop_, gatewayAddress_, gatewayPort_, clusterSecret_)),
            corsPolicy_(std::move(corsPolicy)),
            requestOrigin_(std::move(requestOrigin)), chunkHash_(std::move(chunkHash)),
            acceptedAt_(Clock::now())
    {
        clientId_ = request.getHeader("X-Client-Instance-Id");
        setup(request);
    }

    ~ChunkUploadStream()
    {
        if(diskPipeline_ != nullptr) diskPipeline_->cancel();
        releaseActiveWrite();
    }

    void startReplica(const TcpConnectionPtr& upstream)
    {
        requireLoopThread();
        if(!error_.empty() || writer_ == nullptr) return;

        std::weak_ptr<miniKV::network::TcpConnection> weakUpstream(upstream);
        diskPipeline_ = ChunkDiskWritePipeline::create(
            loop_, diskExecutor_, writer_, [weakUpstream] {
                if(auto connection = weakUpstream.lock()) connection->resumeRead();
            });
        if(diskPipeline_ == nullptr) {
            error_ = "cannot create local disk write pipeline";
            return;
        }
        if(position_ + 1 >= chain_.size()) return;

        const ReplicaTarget& target = chain_[position_ + 1];
        ReplicaUploadPipeOptions options;
        options.request.address = target.address;
        options.request.port = target.port;
        options.request.method = "PUT";
        options.request.path = "/v2/chunks/" + chunkHash_;
        options.request.contentLength = capability_.chunkSize;
        options.request.timeoutMs = 30000;
        options.request.headers = {
            {"Content-Type", "application/octet-stream"},
            {"X-Session-Id", capability_.sessionId},
            {"X-Chunk-Index", std::to_string(capability_.chunkIndex)},
            {"X-Commit-Owner", chain_.front().nodeId},
            {"X-Gateway-Address", gatewayAddress_},
            {"X-Gateway-Port", std::to_string(gatewayPort_)},
            {"X-Replica-Chain", joinReplicaChain(chain_)},
            {"X-Replica-Position", std::to_string(position_ + 1)},
            {"X-Upload-Token", uploadToken_},
            {"X-Client-Instance-Id", clientId_}
        };
        options.connectionPool = replicaConnectionPool_;
        options.connectionKey = {target.nodeId, target.address, target.port};
        options.resumeUpstream = [weakUpstream] {
            if(auto connection = weakUpstream.lock()) connection->resumeRead();
        };

        replicaPipe_ = ReplicaUploadPipe::create(loop_);
        std::weak_ptr<ChunkUploadStream> weakSelf(shared_from_this());
        replicaPipe_->start(std::move(options), [weakSelf](HttpClientResponse response, std::string error) {
            if(auto self = weakSelf.lock()) self->onReplicaComplete(std::move(response), std::move(error));
        });
    }

    HttpContext::BodyConsumeResult consume(const char* bytes, size_t size)
    {
        requireLoopThread();
        if(firstBodyAt_ == Clock::time_point{}) firstBodyAt_ = Clock::now();
        if(admissionRejected_) return HttpContext::BodyConsumeResult::kContinue;
        if(!error_.empty() || writer_ == nullptr || diskPipeline_ == nullptr) {
            logBodyRejected(error_.empty() ? "stream is not writable" : error_, size);
            return HttpContext::BodyConsumeResult::kAbort;
        }

        DiskWriteExecutor::SharedBlockPtr sharedBlock;
        const auto diskResult = diskPipeline_->push(bytes, size,
                                                     replicaPipe_ == nullptr ? nullptr : &sharedBlock);
        if(diskResult == HttpContext::BodyConsumeResult::kAbort) {
            error_ = "local disk write queue rejected body bytes";
            logBodyRejected(error_, size);
            return HttpContext::BodyConsumeResult::kAbort;
        }
        if(diskResult == HttpContext::BodyConsumeResult::kPauseBeforeConsume) {
            return diskResult;
        }
        if(replicaPipe_ == nullptr) return diskResult;

        const auto replicaResult = replicaPipe_->pushShared(std::move(sharedBlock), size);
        if(replicaResult == HttpContext::BodyConsumeResult::kAbort) {
            error_ = "replica stream rejected body bytes";
            diskPipeline_->cancel();
            logBodyRejected(error_, size);
            return HttpContext::BodyConsumeResult::kAbort;
        }
        if(replicaResult == HttpContext::BodyConsumeResult::kPause) {
            miniKV::utils::logDebug("event=chunk_replica_backpressure chunk=" + chunkHash_);
        }
        return diskResult == HttpContext::BodyConsumeResult::kPause ||
               replicaResult == HttpContext::BodyConsumeResult::kPause
            ? HttpContext::BodyConsumeResult::kPause
            : HttpContext::BodyConsumeResult::kContinue;
    }

    void logBodyRejected(const std::string& reason, size_t bodyBytes)
    {
        if(bodyRejectedLogged_) return;
        bodyRejectedLogged_ = true;
        miniKV::utils::logError("event=chunk_body_rejected chunk=" + chunkHash_ +
                                 " body_bytes=" + std::to_string(bodyBytes) +
                                 " reason=" + reason);
    }

    void finish(const DeferredResponse::Ptr& deferred)
    {
        requireLoopThread();
        if(response_ != nullptr) return;
        response_ = deferred;
        response_->defer();
        selfHold_ = shared_from_this();

        if(admissionRejected_) {
            completeClient(503, "DataNode write capacity reached");
            return;
        }
        if(!error_.empty() || writer_ == nullptr || diskPipeline_ == nullptr) {
            completeClient(400, error_.empty() ? "invalid stream" : error_);
            return;
        }

        std::weak_ptr<ChunkUploadStream> weakSelf(shared_from_this());
        diskPipeline_->finishInput([weakSelf](bool success, bool alreadyExists) {
            if(auto self = weakSelf.lock()) self->onLocalFinish(success, alreadyExists);
        });
        if(response_ == nullptr) return;
        if(replicaPipe_ == nullptr) replicaCompleted_ = true;
        else replicaPipe_->finish();
    }

private:
    void setup(const HttpRequest& request)
    {
        uploadToken_ = request.getHeader("X-Upload-Token");
        if(!verifyUploadCapability(uploadToken_, clusterSecret_, capability_)) {
            error_ = "invalid or expired upload token";
            return;
        }
        try {
            position_ = static_cast<size_t>(std::stoull(request.getHeader("X-Replica-Position")));
        } catch(...) {
            error_ = "invalid replica position";
            return;
        }
        chain_ = parseReplicaChain(request.getHeader("X-Replica-Chain"));
        if(request.getHeader("X-Session-Id") != capability_.sessionId ||
           request.getHeader("X-Chunk-Index") != std::to_string(capability_.chunkIndex) ||
           request.contentLength() != capability_.chunkSize || chunkHash_ != capability_.chunkHash ||
           chain_.empty() || !sameChain(chain_, capability_.chainTargets) ||
           position_ >= chain_.size() || chain_[position_].nodeId != nodeId_) {
            error_ = "request does not match upload token";
            return;
        }
        if(clientId_.empty()) clientId_ = "session:" + capability_.sessionId;
        uploadLease_ = resourceGovernor_.tryAcquireUpload(clientId_);
        if(!uploadLease_.has_value()) {
            admissionRejected_ = true;
            return;
        }
        auto writer = store_.beginPut(chunkHash_, capability_.chunkSize);
        if(writer == nullptr) {
            releaseActiveWrite();
            error_ = "cannot allocate local chunk extent";
            return;
        }
        writer_ = std::shared_ptr<FastDataStore::WriteSession>(std::move(writer));
    }

    void onLocalFinish(bool success, bool alreadyExists)
    {
        requireLoopThread();
        if(response_ == nullptr) return;
        if(!success) {
            miniKV::utils::logError("event=chunk_local_finish_failed chunk=" + chunkHash_ +
                                    " bytes=" + std::to_string(writer_->writtenBytes()));
            completeClient(400, "chunk length or SHA-256 verification failed");
            return;
        }
        localFinished_ = true;
        localFinishedAt_ = Clock::now();
        alreadyExists_ = alreadyExists;
        successfulNodes_.push_back(nodeId_);
        miniKV::utils::logDebug("event=chunk_local_finish chunk=" + chunkHash_ +
                                " bytes=" + std::to_string(writer_->writtenBytes()) +
                                " replica=" + (replicaPipe_ == nullptr ? "false" : "true"));
        maybeAfterLocalAndReplica();
    }

    void onReplicaComplete(HttpClientResponse response, std::string error)
    {
        requireLoopThread();
        if(response_ == nullptr) return;
        replicaFinishedAt_ = Clock::now();
        if(replicaPipe_ != nullptr) replicaMetrics_ = replicaPipe_->metrics();
        replicaPipe_.reset();
        miniKV::utils::logInfo("event=chunk_replica_complete chunk=" + chunkHash_ +
                               " http_status=" + std::to_string(response.status) +
                               " error=" + (error.empty() ? "-" : error));
        if(!error.empty() || response.status != 200) {
            replicaError_ = error.empty() ? "replica returned HTTP " + std::to_string(response.status)
                                          : std::move(error);
        } else {
            for(const auto& nodeId : split(jsonString(response.body, "successfulNodes"), ',')) {
                if(!nodeId.empty() && std::find(successfulNodes_.begin(), successfulNodes_.end(), nodeId) == successfulNodes_.end()) {
                    successfulNodes_.push_back(nodeId);
                }
            }
        }
        replicaCompleted_ = true;
        maybeAfterLocalAndReplica();
    }

    void maybeAfterLocalAndReplica()
    {
        if(!localFinished_ || !replicaCompleted_) return;
        afterReplica();
    }

    void afterReplica()
    {
        if(position_ != 0) {
            miniKV::utils::logDebug("event=replica_chunk_reply chunk=" + chunkHash_);
            completeClient(200, "");
            return;
        }
        miniKV::utils::logDebug("event=chunk_gateway_commit_start chunk=" + chunkHash_ +
                                " successful_nodes=" + std::to_string(successfulNodes_.size()));
        gatewayCommitStartedAt_ = Clock::now();
        std::weak_ptr<ChunkUploadStream> weakSelf(shared_from_this());
        gatewayControl_->commitChunk({capability_.sessionId, capability_.chunkIndex, chunkHash_,
                                    capability_.chunkSize, successfulNodes_, uploadToken_},
            [weakSelf](RpcResult result) {
            if(auto self = weakSelf.lock()) {
                self->requireLoopThread();
                self->gatewayCommitFinishedAt_ = Clock::now();
                miniKV::utils::logInfo("event=chunk_gateway_commit_result chunk=" + self->chunkHash_ +
                                       " http_status=" + std::to_string(result.httpStatus) +
                                       " error=" + (result.error.empty() ? "-" : result.error));
                if(!result.ok) {
                    self->completeClient(500, result.error.empty() ? "Gateway commit failed" : std::move(result.error));
                    return;
                }
                self->completeClient(200, "");
            }
        });
    }

    void completeClient(int status, const std::string& error)
    {
        requireLoopThread();
        if(response_ == nullptr) return;
        const auto completedAt = Clock::now();
        if(replicaPipe_ != nullptr) replicaMetrics_ = replicaPipe_->metrics();
        const FastDataStore::WriteMetrics writeMetrics = writer_ == nullptr
            ? FastDataStore::WriteMetrics{} : writer_->metrics();
        const ChunkDiskWritePipeline::Metrics diskMetrics = diskPipeline_ == nullptr
            ? ChunkDiskWritePipeline::Metrics{} : diskPipeline_->metrics();
        const std::string line = std::string(status == 200 ? "event=chunk_complete" : "event=chunk_failed") +
            " chunk=" + chunkHash_ + " session=" + capability_.sessionId +
            " index=" + std::to_string(capability_.chunkIndex) + " http_status=" +
            std::to_string(status) + " bytes=" + std::to_string(capability_.chunkSize) +
            " role=" + std::string(position_ == 0 ? "primary" : "replica") +
            " replicas=" + std::to_string(successfulNodes_.size()) +
            " total_ms=" + std::to_string(elapsedMilliseconds(acceptedAt_, completedAt)) +
            " body_receive_ms=" + std::to_string(elapsedMilliseconds(firstBodyAt_, localFinishedAt_)) +
            " sha_update_us=" + std::to_string(writeMetrics.shaUpdateNanoseconds / 1000ULL) +
            " pwrite_us=" + std::to_string(writeMetrics.pwriteNanoseconds / 1000ULL) +
            " sha_finalize_us=" + std::to_string(writeMetrics.shaFinalizeNanoseconds / 1000ULL) +
            " index_us=" + std::to_string(writeMetrics.indexNanoseconds / 1000ULL) +
            " replica_ms=" + std::to_string(elapsedMilliseconds(localFinishedAt_, replicaFinishedAt_)) +
            " gateway_commit_ms=" + std::to_string(elapsedMilliseconds(
                gatewayCommitStartedAt_, gatewayCommitFinishedAt_)) +
            " pauses=" + std::to_string(replicaMetrics_.pauseCount) +
            " pause_ms=" + std::to_string(replicaMetrics_.pauseNanoseconds / 1000000ULL) +
            " max_pending_bytes=" + std::to_string(replicaMetrics_.maxPendingBytes) +
            " disk_queue_peak_bytes=" + std::to_string(diskMetrics.peakQueuedBytes) +
            " disk_pause_count=" + std::to_string(diskMetrics.pauseCount) +
            " disk_pause_ms=" + std::to_string(diskMetrics.pauseNanoseconds / 1000000ULL) +
            " error=" + (error.empty() ? "-" : error);
        if(status == 200) miniKV::utils::logInfo(line);
        else miniKV::utils::logError(line);
        HttpResponse response;
        if(status != 200) {
            json(&response, status, jsonError(error));
            if(status == 503) response.addHeader("Retry-After", "1");
        } else {
            std::ostringstream body;
            body << "{\"chunkHash\":\"" << chunkHash_ << "\",\"alreadyExists\":"
                 << (alreadyExists_ ? "true" : "false") << ",\"successfulNodes\":\""
                 << join(successfulNodes_, ',') << "\"";
            if(!replicaError_.empty()) body << ",\"replicaWarning\":\"" << jsonEscape(replicaError_) << "\"";
            body << "}";
            json(&response, 200, body.str());
        }
        // Streaming PUT completes through DeferredResponse after the original
        // HttpServer callback has returned. Add CORS here, not only in the
        // immediate handler path, otherwise browsers treat a successful 200
        // as an opaque XHR network error.
        corsPolicy_.appendHeaders(response, requestOrigin_);
        if(status != 200 && position_ == 0 && !uploadToken_.empty()) {
            gatewayControl_->releaseLease({uploadToken_}, [](RpcResult) {});
        }
        releaseActiveWrite();
        auto deferred = std::move(response_);
        replicaPipe_.reset();
        diskPipeline_.reset();
        selfHold_.reset();
        deferred->complete(std::move(response));
    }

    void releaseActiveWrite()
    {
        uploadLease_.reset();
    }

    void requireLoopThread() const
    {
        if(!loop_->isInLoopThread()) std::abort();
    }

    static uint64_t elapsedMilliseconds(Clock::time_point started, Clock::time_point finished)
    {
        if(started == Clock::time_point{} || finished == Clock::time_point{} || finished < started) return 0;
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            finished - started).count());
    }

    EventLoop* loop_;
    FastDataStore& store_;
    NodeResourceGovernor& resourceGovernor_;
    DiskWriteExecutor& diskExecutor_;
    ReplicaConnectionPool::Ptr replicaConnectionPool_;
    std::string nodeId_;
    std::string gatewayAddress_;
    uint16_t gatewayPort_ = 0;
    std::string clusterSecret_;
    std::unique_ptr<GatewayControlClient> gatewayControl_;
    CorsPolicy corsPolicy_;
    std::string requestOrigin_;
    std::string chunkHash_;
    std::string clientId_;
    std::string uploadToken_;
    UploadCapability capability_;
    std::vector<ReplicaTarget> chain_;
    size_t position_ = 0;
    std::shared_ptr<FastDataStore::WriteSession> writer_;
    ChunkDiskWritePipeline::Ptr diskPipeline_;
    ReplicaUploadPipe::Ptr replicaPipe_;
    DeferredResponse::Ptr response_;
    std::vector<std::string> successfulNodes_;
    std::string error_;
    std::string replicaError_;
    bool alreadyExists_ = false;
    std::optional<NodeResourceGovernor::UploadLease> uploadLease_;
    bool admissionRejected_ = false;
    bool bodyRejectedLogged_ = false;
    bool localFinished_ = false;
    bool replicaCompleted_ = false;
    Clock::time_point acceptedAt_{};
    Clock::time_point firstBodyAt_{};
    Clock::time_point localFinishedAt_{};
    Clock::time_point replicaFinishedAt_{};
    Clock::time_point gatewayCommitStartedAt_{};
    Clock::time_point gatewayCommitFinishedAt_{};
    ReplicaUploadMetrics replicaMetrics_;
    std::shared_ptr<ChunkUploadStream> selfHold_;
};

}  // namespace

int main(int argc, char** argv)
{
    /*
    nodeId
    advertiseAddress
    本节点 HTTP 端口
    dataDir
    Gateway 地址和端口
    clusterSecret
    */
    if(argc < 7) {
        std::cerr << "usage: minikv_v2_datanode <nodeId> <advertiseAddress> <port> <dataDir> <gatewayAddress> <gatewayPort> [clusterSecret]\n";
        return 2;
    }
    const std::string nodeId = argv[1];
    const std::string advertiseAddress = argv[2];
    const uint16_t port = static_cast<uint16_t>(std::stoul(argv[3]));
    const std::string dataDir = argv[4];
    const std::string gatewayAddress = argv[5];
    const uint16_t gatewayPort = static_cast<uint16_t>(std::stoul(argv[6]));
    const std::string clusterSecret = configuredSecret(argc, argv);
    const std::vector<std::string> capabilities = configuredCapabilities();
    const CorsPolicy corsPolicy(configuredAllowedOrigin());
    if(clusterSecret.empty()) {
        std::cerr << "MINIKV_V2_CLUSTER_SECRET or a command-line clusterSecret is required\n";
        return 2;
    }

    if(!miniKV::utils::initAsyncLogger(miniKV::utils::asyncLoggerConfigFromEnvironment(
           "datanode", configuredNodeId(nodeId), dataDir + "/logs/datanode-" + nodeId + ".log"))) {
        std::cerr << "cannot initialize DataNode async logger\n";
    }

    FastDataStore store(dataDir);
    if(!store.open()) {
        miniKV::utils::logError("event=datanode_store_open_failed data_dir=" + dataDir);
        std::cerr << "cannot open DataNode store\n";
        return 1;
    }
    const uint32_t maxConcurrentWrites = configuredLimit(
        "MINIKV_V4_MAX_ACTIVE_UPLOADS", kDefaultMaxConcurrentWrites);
    const uint32_t maxConcurrentDownloads = configuredLimit(
        "MINIKV_V4_MAX_ACTIVE_DOWNLOADS", kDefaultMaxConcurrentDownloads);
    const uint32_t maxUploadsPerClient = configuredLimit(
        "MINIKV_V4_MAX_UPLOADS_PER_CLIENT", kDefaultMaxUploadsPerClient);
    const uint32_t maxDownloadsPerClient = configuredLimit(
        "MINIKV_V4_MAX_DOWNLOADS_PER_CLIENT", kDefaultMaxDownloadsPerClient);
    const size_t sendFileQuantumBytes = configuredByteLimit(
        "MINIKV_V4_SENDFILE_QUANTUM_BYTES", kDefaultSendFileQuantumBytes);
    const uint32_t ioThreads = configuredIoThreads();
    NodeResourceGovernor::Config resourceConfig;
    resourceConfig.maxActiveUploads = maxConcurrentWrites;
    resourceConfig.maxActiveDownloads = maxConcurrentDownloads;
    resourceConfig.maxUploadsPerClient = maxUploadsPerClient;
    resourceConfig.maxDownloadsPerClient = maxDownloadsPerClient;
    NodeResourceGovernor resourceGovernor(resourceConfig);
    EventLoop loop;
    DiskWriteExecutor::Config diskConfig;
    diskConfig.workerCount = 2;
    diskConfig.blockCount = 128;
    DiskWriteExecutor diskExecutor(diskConfig);
    HttpGatewayControlClient gatewayControl(&loop, gatewayAddress, gatewayPort, clusterSecret);

    std::function<void()> registerNode;
    std::function<void()> heartbeat;
    bool registered = false;
    bool registrationInFlight = false;
    registerNode = [&] {
        if(registered || registrationInFlight) return;
        registrationInFlight = true;
        gatewayControl.registerStorageNode({nodeId, advertiseAddress, port, availableBytes(dataDir), 0,
                                            maxConcurrentWrites, capabilities},
            [&](RpcResult result) {
                registrationInFlight = false;
                if(result.ok) {
                    registered = true;
                    miniKV::utils::logInfo("event=datanode_registered node=" + nodeId +
                                           " gateway=" + gatewayAddress + ":" +
                                           std::to_string(gatewayPort));
                    return;
                }
                miniKV::utils::logWarn("event=datanode_registration_failed node=" + nodeId +
                                       " error=" + result.error);
                loop.runAfter(1, registerNode);
            });
    };
    heartbeat = [&] {
        gatewayControl.sendHeartbeat({nodeId, logicalUsedBytes(store), effectiveFreeBytes(store, dataDir),
                                      0, 0, 0, 0,
                                      resourceGovernor.snapshot().activeUploads},
            [&](RpcResult result) {
                if(!result.ok) {
                    miniKV::utils::logDebug("event=datanode_heartbeat_failed node=" + nodeId +
                                            " error=" + result.error);
                    registered = false;
                    registerNode();
                }
            });
    };

    miniKV::http::HttpServer server(&loop, nullptr, port);
    server.setThreadNum(ioThreads);
    std::vector<std::pair<EventLoop*, ReplicaConnectionPool::Ptr>> replicaPools;
    server.setErrorResponseDecorator([&](const HttpRequest& request, HttpResponse* response) {
        if(beginsWith(request.path(), "/v2/chunks/")) {
            corsPolicy.appendHeaders(*response, request.getHeader("Origin"));
        }
    });
    server.setStreamCheck([&](const HttpRequest& request) {
        return request.method() == HttpRequest::kPut &&
               beginsWith(request.path(), "/v2/chunks/") && request.contentLength() > 0 &&
               corsPolicy.allows(request.getHeader("Origin"));
    });

    server.setBodyStreamSetup([&](HttpContext* context, const HttpRequest& request,
        const TcpConnectionPtr& upstream) {
        const std::string hash = request.path().substr(std::string("/v2/chunks/").size());
        ReplicaConnectionPool::Ptr replicaPool;
        for(const auto& item : replicaPools) {
            if(item.first == upstream->ownerLoop()) {
                replicaPool = item.second;
                break;
            }
        }
        auto stream = std::make_shared<ChunkUploadStream>(upstream->ownerLoop(), store,
        resourceGovernor, diskExecutor, std::move(replicaPool), nodeId,
        gatewayAddress, gatewayPort, clusterSecret, corsPolicy, request.getHeader("Origin"), request, hash);
        stream->startReplica(upstream);
        context->setUserData(stream);
        context->setBodyCallback(request.contentLength(), [stream](const char* bytes, size_t size) {
        return stream->consume(bytes, size);
        });
    });
    server.setHttpCallback([&](const HttpRequest& request, HttpResponse* response,
        const TcpConnectionPtr& connection, const DeferredResponse::Ptr& deferred) {
        const std::string& path = request.path();
        const std::string origin = request.getHeader("Origin");
        const bool chunkRequest = beginsWith(path, "/v2/chunks/");
        const bool internalDelete = request.method() == HttpRequest::kDelete &&
            beginsWith(path, "/internal/v2/chunks/");
        if(internalDelete) {
            if(!constantTimeEquals(request.getHeader("X-Cluster-Internal-Token"), clusterSecret)) {
                json(response, 403, jsonError("invalid cluster token"));
                return;
            }
            const std::string hash = path.substr(std::string("/internal/v2/chunks/").size());
            bool removed = false;
            if(hash.empty() || !store.remove(hash, removed)) {
                json(response, 500, jsonError("cannot delete local chunk"));
                return;
            }
            json(response, 200, std::string("{\"status\":\"deleted\",\"removed\":") +
                (removed ? "true}" : "false}"));
            return;
        }
        if(chunkRequest && !corsPolicy.allows(origin)) {
            json(response, 403, jsonError("origin is not allowed"));
            return;
        }
        if(chunkRequest && request.method() == HttpRequest::kOptions) {
            response->setStatusCode(HttpResponse::k200Ok);
            corsPolicy.appendHeaders(*response, origin);
            return;
        }
        if(request.method() == HttpRequest::kPut && beginsWith(path, "/v2/chunks/")) {
            auto stream = std::static_pointer_cast<ChunkUploadStream>(request.userData());
            if(stream == nullptr) {
                json(response, 400, jsonError("streaming body is required"));
                corsPolicy.appendHeaders(*response, origin);
                return;
            }
            stream->finish(deferred);
            return;
        }
        if((request.method() == HttpRequest::kGet || request.method() == HttpRequest::kHead) &&
        beginsWith(path, "/v2/chunks/")) {
            const std::string hash = path.substr(std::string("/v2/chunks/").size());
            FileRegion region;
            if(!store.getRegion(hash, region)) {
                json(response, 404, jsonError("chunk not found"));
                corsPolicy.appendHeaders(*response, origin);
                return;
            }
            response->setStatusCode(HttpResponse::k200Ok);
            response->addHeader("Content-Length", std::to_string(region.length));
            response->addHeader("X-Chunk-Hash", hash);
            if(request.method() == HttpRequest::kGet) {
                std::string clientId = request.getHeader("X-Client-Instance-Id");
                if(clientId.empty()) {
                    clientId = "connection:" + std::to_string(connection->fd());
                }
                auto lease = resourceGovernor.tryAcquireDownload(clientId);
                if(!lease.has_value()) {
                    json(response, 503, jsonError("DataNode download capacity reached"));
                    response->addHeader("Retry-After", "1");
                    corsPolicy.appendHeaders(*response, origin);
                    return;
                }
                auto sharedLease = std::make_shared<NodeResourceGovernor::DownloadLease>(
                    std::move(*lease));
                const auto startedAt = std::chrono::steady_clock::now();
                connection->setSendFileQuantum(sendFileQuantumBytes);
                response->setCloseConnection(true);
                response->addHeader("Connection", "close");
                response->setFileBody(store.dataFilePath(), region.offset, region.length,
                    [sharedLease, hash, startedAt](
                        const miniKV::network::SendFileResult& result) {
                        (void)sharedLease;
                        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - startedAt).count();
                        const std::string line = "event=chunk_download_complete chunk=" + hash +
                            " success=" + (result.success ? "true" : "false") + " bytes=" +
                            std::to_string(result.bytesSent) + " total_ms=" +
                            std::to_string(elapsed);
                        if(result.success) miniKV::utils::logInfo(line);
                        else miniKV::utils::logWarn(line);
                    });
            }
            corsPolicy.appendHeaders(*response, origin);
            return;
        }
        json(response, 404, jsonError("route not found"));
    });
    loop.runAfter(0, [&] { registerNode(); heartbeat(); });
    loop.runEvery(8000, heartbeat);
    loop.runEvery(1000, [&] {
        const NodeResourceGovernor::Snapshot resources = resourceGovernor.snapshot();
        const DiskWriteExecutor::Metrics disk = diskExecutor.metrics();
        const miniKV::network::TcpConnection::OutputMetrics output = server.outputBufferMetrics();
        miniKV::utils::logInfo("event=resource_snapshot"
            " active_uploads=" + std::to_string(resources.activeUploads) +
            " active_downloads=" + std::to_string(resources.activeDownloads) +
            " disk_queued_tasks=" + std::to_string(disk.queuedTasks) +
            " disk_queue_peak_tasks=" + std::to_string(disk.peakQueuedTasks) +
            " block_available=" + std::to_string(disk.availableBlocks) +
            " block_total=" + std::to_string(disk.totalBlocks) +
            " block_leased_bytes=" + std::to_string(disk.leasedBytes) +
            " block_peak_leased_bytes=" + std::to_string(disk.peakLeasedBytes) +
            " disk_active_workers=" + std::to_string(disk.activeWorkers) +
            " disk_total_workers=" + std::to_string(disk.totalWorkers) +
            " disk_completed_tasks=" + std::to_string(disk.completedTasks) +
            " output_buffer_current_bytes=" + std::to_string(output.currentBytes) +
            " output_buffer_peak_bytes=" + std::to_string(output.peakBytes) +
            " output_buffer_high_water_events=" + std::to_string(output.highWaterEvents));
        const auto eventLoops = server.eventLoops();
        for(size_t index = 0; index < eventLoops.size(); ++index) {
            const miniKV::network::EventLoop::Metrics metrics = eventLoops[index]->metrics();
            miniKV::utils::logInfo("event=event_loop_snapshot"
                " loop_index=" + std::to_string(index) +
                " loop_role=" + std::string(index == 0 ? "acceptor" : "io") +
                " loop_iterations=" + std::to_string(metrics.loopIterations) +
                " pending_queued=" + std::to_string(metrics.pendingFunctorsQueued) +
                " cross_thread_queued=" + std::to_string(metrics.crossThreadQueued) +
                " pending_executed=" + std::to_string(metrics.pendingFunctorsExecuted) +
                " pending_depth=" + std::to_string(metrics.pendingFunctorDepth) +
                " pending_peak_depth=" + std::to_string(metrics.pendingFunctorPeakDepth) +
                " timer_callbacks=" + std::to_string(metrics.timerCallbacks) +
                " timer_late_callbacks=" + std::to_string(metrics.timerLateCallbacks) +
                " timer_lag_total_ms=" + std::to_string(metrics.timerLagTotalMs) +
                " timer_lag_max_ms=" + std::to_string(metrics.timerLagMaxMs));
        }
    });
    server.start();
    for(miniKV::network::EventLoop* eventLoop : server.eventLoops()) {
        // The base loop never owns accepted data connections when I/O worker
        // loops are enabled, but giving it a shard also keeps the zero-worker
        // configuration correct.
        replicaPools.emplace_back(eventLoop, ReplicaConnectionPool::create(eventLoop));
    }
    // Every I/O loop needs its own timer probe.  A probe on only the accept
    // loop cannot describe worker-loop scheduling under Multi-Reactor load.
    for(miniKV::network::EventLoop* eventLoop : server.eventLoops()) {
        eventLoop->runEvery(1000, [] {});
    }
    miniKV::utils::logInfo("event=datanode_started node=" + nodeId + " port=" +
                           std::to_string(port) + " data_dir=" + dataDir +
                           " max_uploads=" + std::to_string(maxConcurrentWrites) +
                           " max_downloads=" + std::to_string(maxConcurrentDownloads) +
                           " io_threads=" + std::to_string(ioThreads) +
                           " sendfile_quantum_bytes=" + std::to_string(sendFileQuantumBytes));
    loop.loop();
    for(const auto& item : replicaPools) item.second->shutdown();
}
