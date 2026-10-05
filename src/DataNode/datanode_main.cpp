#include "http/DeferredResponse.hpp"
#include "http/HttpContext.hpp"
#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "http/HttpRange.hpp"
#include "http/HttpServer.hpp"
#include "network/EventLoop.hpp"
#include "network/TcpConnection.hpp"
#include "http/AsyncHttpClient.hpp"
#include "http/CorsPolicy.hpp"
#include "DataNode/FastDataStore.hpp"
#include "DataNode/ChunkDiskWritePipeline.hpp"
#include "DataNode/ChunkCommitReporter.hpp"
#include "DataNode/ChunkStore.hpp"
#include "DataNode/ChunkWriteTypes.hpp"
#include "DataNode/DiskWriteExecutor.hpp"
#include "DataNode/HttpChunkUploadAdapter.hpp"
#include "DataNode/HttpGatewayControlClient.hpp"
#include "DataNode/MetadataControlClient.hpp"
#include "metadata/MetadataClient.hpp"
#include "DataNode/NodeResourceGovernor.hpp"
#include "DataNode/ReplicaUploadPipe.hpp"
#include "DataNode/ReplicaTransport.hpp"
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
#include <stdexcept>
#include <sys/stat.h>
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
constexpr uint32_t kDefaultDiskWriteWorkers = 2;
constexpr uint32_t kDefaultDiskWriteBlocks = 128;
constexpr uint32_t kMaxDiskWriteWorkers = 32;
constexpr uint32_t kMaxDiskWriteBlocks = 4096;
constexpr size_t kDefaultSendFileQuantumBytes = 256 * 1024;
constexpr size_t kDefaultWriteBatchBytes = 256 * 1024;
constexpr uint64_t kDefaultWriteBatchDelayUs = 1000;
constexpr size_t kDefaultGroupCommitBytes = 8 * 1024 * 1024;
constexpr size_t kDefaultGroupCommitItems = 8;
constexpr uint64_t kDefaultGroupCommitDelayUs = 2000;
constexpr size_t kDefaultGroupCommitMaxPendingBytes = 64 * 1024 * 1024;
constexpr size_t kDefaultGroupCommitMaxPendingItems = 64;

bool beginsWith(const std::string& value, const std::string& prefix)
{
    return value.rfind(prefix, 0) == 0;
}

HttpContext::BodyConsumeResult toHttpBodyConsumeResult(StreamConsumeResult result)
{
    switch(result) {
    case StreamConsumeResult::kContinue:
        return HttpContext::BodyConsumeResult::kContinue;
    case StreamConsumeResult::kPause:
        return HttpContext::BodyConsumeResult::kPause;
    case StreamConsumeResult::kPauseBeforeConsume:
        return HttpContext::BodyConsumeResult::kPauseBeforeConsume;
    case StreamConsumeResult::kAbort:
        return HttpContext::BodyConsumeResult::kAbort;
    }
    return HttpContext::BodyConsumeResult::kAbort;
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

std::string configuredPhysicalIndexDirectory(const std::string& nodeId)
{
    const char* value = std::getenv("MINIKV_V3_PHYSICAL_INDEX_DIR");
    if(value == nullptr || *value == '\0') return {};
    std::string directory = value;
    const std::string marker = "%NODE_ID%";
    size_t offset = 0;
    while((offset = directory.find(marker, offset)) != std::string::npos) {
        directory.replace(offset, marker.size(), nodeId);
        offset += nodeId.size();
    }
    return directory;
}

std::string deviceNumberForPath(const std::string& path)
{
    struct stat status{};
    if(::stat(path.c_str(), &status) != 0) return "-";
    return std::to_string(static_cast<unsigned long long>(status.st_dev));
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

bool configuredDurabilityPolicy(DurabilityPolicy& policy, std::string& name)
{
    const char* value = std::getenv("MINIKV_V3_DURABILITY_MODE");
    name = value == nullptr || *value == '\0' ? "buffered" : value;
    if(name == "buffered") {
        policy = DurabilityPolicy::kBuffered;
        return true;
    }
    if(name == "chunk_sync") {
        policy = DurabilityPolicy::kChunkSync;
        return true;
    }
    if(name == "group_commit") {
        policy = DurabilityPolicy::kGroupCommit;
        return true;
    }
    return false;
}

bool configuredGroupCommit(FastDataStore::GroupCommitConfig& config)
{
    config.maxBatchBytes = configuredByteLimit(
        "MINIKV_V3_GROUP_COMMIT_BYTES", kDefaultGroupCommitBytes);
    config.maxBatchItems = configuredLimit(
        "MINIKV_V3_GROUP_COMMIT_ITEMS", kDefaultGroupCommitItems);
    config.maxPendingBytes = configuredByteLimit(
        "MINIKV_V3_GROUP_COMMIT_MAX_PENDING_BYTES",
        kDefaultGroupCommitMaxPendingBytes);
    config.maxPendingItems = configuredLimit(
        "MINIKV_V3_GROUP_COMMIT_MAX_PENDING_ITEMS",
        kDefaultGroupCommitMaxPendingItems);
    config.maxBatchDelayUs = kDefaultGroupCommitDelayUs;
    if(const char* value = std::getenv("MINIKV_V3_GROUP_COMMIT_DELAY_US")) {
        try {
            const unsigned long long parsed = std::stoull(value);
            if(parsed <= 1000000ULL) config.maxBatchDelayUs = parsed;
            else return false;
        } catch(...) {
            return false;
        }
    }
    return config.maxBatchBytes >= DiskWriteExecutor::kBlockBytes &&
           config.maxBatchBytes <= 64ULL * 1024ULL * 1024ULL &&
           config.maxBatchItems > 0 && config.maxBatchItems <= 1024 &&
           config.maxPendingBytes >= config.maxBatchBytes &&
           config.maxPendingBytes <= 1024ULL * 1024ULL * 1024ULL &&
           config.maxPendingItems >= config.maxBatchItems &&
           config.maxPendingItems <= 65536;
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

uint32_t configuredDiskLimit(const char* name, uint32_t fallback, uint32_t maximum)
{
    const uint32_t value = configuredLimit(name, fallback);
    if(value > maximum) {
        miniKV::utils::logWarn("event=invalid_disk_executor_config name=" + std::string(name) +
                               " fallback=" + std::to_string(fallback));
        return fallback;
    }
    return value;
}

ChunkDiskWritePipeline::Config configuredDiskPipeline(std::string& modeName)
{
    ChunkDiskWritePipeline::Config config;
    const char* configuredMode = std::getenv("MINIKV_V3_WRITE_BATCH_MODE");
    modeName = configuredMode == nullptr || *configuredMode == '\0' ? "pwritev" : configuredMode;
    if(modeName == "single") {
        config.writeMode = ChunkDiskWritePipeline::WriteMode::kSingleBlock;
    } else if(modeName == "pwritev") {
        config.writeMode = ChunkDiskWritePipeline::WriteMode::kPwritev;
    } else {
        miniKV::utils::logWarn("event=invalid_write_batch_mode value=" + modeName +
                               " fallback=pwritev");
        modeName = "pwritev";
    }

    config.targetBatchBytes = configuredByteLimit(
        "MINIKV_V3_WRITE_BATCH_BYTES", kDefaultWriteBatchBytes);
    if(config.targetBatchBytes < DiskWriteExecutor::kBlockBytes ||
       config.targetBatchBytes > ChunkDiskWritePipeline::kHighWatermarkBytes) {
        miniKV::utils::logWarn("event=invalid_write_batch_bytes value=" +
                               std::to_string(config.targetBatchBytes) +
                               " fallback=" + std::to_string(kDefaultWriteBatchBytes));
        config.targetBatchBytes = kDefaultWriteBatchBytes;
    }

    config.maxBatchDelayUs = kDefaultWriteBatchDelayUs;
    if(const char* value = std::getenv("MINIKV_V3_WRITE_BATCH_DELAY_US")) {
        try {
            const unsigned long long parsed = std::stoull(value);
            if(parsed <= 1000000ULL) config.maxBatchDelayUs = parsed;
            else throw std::out_of_range("write batch delay");
        } catch(...) {
            miniKV::utils::logWarn("event=invalid_write_batch_delay_us value=" +
                                   std::string(value) + " fallback=" +
                                   std::to_string(kDefaultWriteBatchDelayUs));
        }
    }
    return config;
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

struct ChunkWriteResult {
    int status = 500;
    std::string error;
    std::string contentHash;
    std::string chunkId;
    bool alreadyExists = false;
    std::vector<std::string> successfulNodes;
    std::string replicaWarning;
};

class ChunkWriteCoordinator : public std::enable_shared_from_this<ChunkWriteCoordinator> {
public:
    using Clock = std::chrono::steady_clock;
    using CompletionCallback = std::function<void(ChunkWriteResult)>;

    ChunkWriteCoordinator(EventLoop* loop, ChunkStore& store,
                      NodeResourceGovernor& resourceGovernor,
                      DiskWriteExecutor& diskExecutor,
                      ChunkDiskWritePipeline::Config diskPipelineConfig,
                      std::shared_ptr<ReplicaTransport> replicaTransport,
                      std::string nodeId,
                      std::string gatewayAddress, uint16_t gatewayPort,
                      std::string clusterSecret, ChunkWriteDescriptor descriptor,
                      std::shared_ptr<ChunkCommitReporter> controlClient = {})
        : loop_(loop), store_(store), resourceGovernor_(resourceGovernor),
            diskExecutor_(diskExecutor),
            diskPipelineConfig_(diskPipelineConfig),
            replicaTransport_(std::move(replicaTransport)),
            nodeId_(std::move(nodeId)),
            gatewayAddress_(std::move(gatewayAddress)), gatewayPort_(gatewayPort),
            clusterSecret_(std::move(clusterSecret)),
            gatewayControl_(controlClient ? std::move(controlClient) :
                std::make_shared<HttpGatewayControlClient>(
                    loop_, gatewayAddress_, gatewayPort_, clusterSecret_)),
            chunkHash_(descriptor.storageKey()), descriptor_(std::move(descriptor)),
            acceptedAt_(Clock::now())
    {
        setup();
    }

    ~ChunkWriteCoordinator()
    {
        if(diskPipeline_ != nullptr) diskPipeline_->cancel();
        releaseActiveWrite();
    }

    void startReplica(std::function<void()> resumeUpstream)
    {
        requireLoopThread();
        if(!error_.empty() || writer_ == nullptr) return;

        std::weak_ptr<ChunkWriteCoordinator> weakSelf(shared_from_this());
        diskPipelineConfig_.stageCallback = [weakSelf](const char* stage, Clock::time_point at) {
            if(auto self = weakSelf.lock()) self->onDiskStage(stage, at);
        };
        diskPipeline_ = ChunkDiskWritePipeline::create(
            loop_, diskExecutor_, writer_, resumeUpstream, diskPipelineConfig_);
        if(diskPipeline_ == nullptr) {
            error_ = "cannot create local disk write pipeline";
            return;
        }
        if(position_ + 1 >= chain_.size()) return;

        const ReplicaTarget& target = chain_[position_ + 1];
        replicaStartedAt_ = Clock::now();
        ReplicaWriteRequest request;
        request.descriptor = descriptor_;
        request.target = target;
        request.commitOwnerNodeId = chain_.front().nodeId;
        request.gatewayAddress = gatewayAddress_;
        request.gatewayPort = gatewayPort_;
        std::weak_ptr<ChunkWriteCoordinator> weakReplicaSelf(shared_from_this());
        replicaPipe_ = replicaTransport_->open(
            std::move(request), std::move(resumeUpstream),
            [weakReplicaSelf](ReplicaTransportResult result) {
                if(auto self = weakReplicaSelf.lock()) self->onReplicaComplete(std::move(result));
            });
    }

    StreamConsumeResult consume(const char* bytes, size_t size)
    {
        requireLoopThread();
        if(firstBodyAt_ == Clock::time_point{}) firstBodyAt_ = Clock::now();
        stage_ = "body_receive";
        if(admissionRejected_) return StreamConsumeResult::kContinue;
        if(!error_.empty() || writer_ == nullptr || diskPipeline_ == nullptr) {
            logBodyRejected(error_.empty() ? "stream is not writable" : error_, size);
            return StreamConsumeResult::kAbort;
        }

        DiskWriteExecutor::SharedBlockPtr sharedBlock;
        const auto diskResult = diskPipeline_->push(bytes, size,
                                                     replicaPipe_ == nullptr ? nullptr : &sharedBlock);
        if(diskResult == StreamConsumeResult::kAbort) {
            error_ = "local disk write queue rejected body bytes";
            stage_ = "disk_queue";
            logBodyRejected(error_, size);
            return StreamConsumeResult::kAbort;
        }
        if(diskResult == StreamConsumeResult::kPauseBeforeConsume) {
            return diskResult;
        }
        if(replicaPipe_ == nullptr) return diskResult;

        const auto replicaResult = replicaPipe_->pushShared(std::move(sharedBlock), size);
        if(replicaResult == StreamConsumeResult::kAbort) {
            error_ = "replica stream rejected body bytes";
            stage_ = "replica_stream";
            diskPipeline_->cancel();
            logBodyRejected(error_, size);
            return StreamConsumeResult::kAbort;
        }
        if(replicaResult == StreamConsumeResult::kPause) {
            miniKV::utils::logDebug("event=chunk_replica_backpressure chunk=" + chunkHash_);
        }
        return diskResult == StreamConsumeResult::kPause ||
               replicaResult == StreamConsumeResult::kPause
            ? StreamConsumeResult::kPause
            : StreamConsumeResult::kContinue;
    }

    void logBodyRejected(const std::string& reason, size_t bodyBytes)
    {
        if(bodyRejectedLogged_) return;
        bodyRejectedLogged_ = true;
        miniKV::utils::logError("event=chunk_body_rejected chunk=" + chunkHash_ +
                                 " body_bytes=" + std::to_string(bodyBytes) +
                                 " reason=" + reason);
    }

    void finish(CompletionCallback completion)
    {
        requireLoopThread();
        if(completion_) return;
        completion_ = std::move(completion);
        selfHold_ = shared_from_this();
        bodyFinishedAt_ = Clock::now();

        if(admissionRejected_) {
            stage_ = "admission";
            completeClient(503, "DataNode write capacity reached");
            return;
        }
        if(!error_.empty() || writer_ == nullptr || diskPipeline_ == nullptr) {
            if(stage_ == "accepted" || stage_ == "body_receive") stage_ = "local_allocate";
            completeClient(400, error_.empty() ? "invalid stream" : error_);
            return;
        }

        std::weak_ptr<ChunkWriteCoordinator> weakSelf(shared_from_this());
        diskPipeline_->finishInput([weakSelf](bool success, bool alreadyExists) {
            if(auto self = weakSelf.lock()) self->onLocalFinish(success, alreadyExists);
        });
        if(!completion_) return;
        if(replicaPipe_ == nullptr) replicaCompleted_ = true;
        else replicaPipe_->finish();
    }

private:
    void setup()
    {
        uploadToken_ = descriptor_.capabilityId;
        clientId_ = descriptor_.clientId;
        position_ = descriptor_.replicaPosition;
        chain_ = descriptor_.replicaChain;
        admissionRequestedAt_ = Clock::now();
        uploadLease_ = resourceGovernor_.tryAcquireUpload(clientId_, &admissionRejectReason_);
        if(!uploadLease_.has_value()) {
            admissionRejected_ = true;
            return;
        }
        admissionAcquiredAt_ = Clock::now();
        if(!store_.canAccept(descriptor_.contentLength)) {
            releaseActiveWrite();
            admissionRejected_ = true;
            stage_ = "local_admission";
            return;
        }
        FastDataStore::PutOptions putOptions;
        putOptions.storageKey = descriptor_.storageKey();
        putOptions.identityScheme = descriptor_.identityScheme;
        putOptions.checksum = descriptor_.checksum;
        auto writer = store_.begin(putOptions, descriptor_.contentLength);
        if(writer == nullptr) {
            releaseActiveWrite();
            error_ = "cannot allocate local chunk extent";
            stage_ = "local_allocate";
            return;
        }
        writer_ = std::move(writer);
        stage_ = "body_receive";
    }

    void onLocalFinish(bool success, bool alreadyExists)
    {
        requireLoopThread();
        if(!completion_) return;
        if(!success) {
            stage_ = "local_write";
            miniKV::utils::logError("event=chunk_local_finish_failed chunk=" + chunkHash_ +
                                    " bytes=" + std::to_string(writer_->writtenBytes()));
            completeClient(400, "chunk length or checksum verification failed");
            return;
        }
        localFinished_ = true;
        localFinishedAt_ = Clock::now();
        stage_ = replicaPipe_ == nullptr ? "metadata_commit" : "replica_wait";
        alreadyExists_ = alreadyExists;
        successfulNodes_.push_back(nodeId_);
        miniKV::utils::logDebug("event=chunk_local_finish chunk=" + chunkHash_ +
                                " bytes=" + std::to_string(writer_->writtenBytes()) +
                                " replica=" + (replicaPipe_ == nullptr ? "false" : "true"));
        maybeAfterLocalAndReplica();
    }

    void onReplicaComplete(ReplicaTransportResult result)
    {
        requireLoopThread();
        if(!completion_) return;
        replicaFinishedAt_ = Clock::now();
        if(replicaPipe_ != nullptr) replicaMetrics_ = replicaPipe_->metrics();
        replicaPipe_.reset();
        miniKV::utils::logInfo("event=chunk_replica_complete chunk=" + chunkHash_ +
                               " transport_status=" + std::to_string(result.status) +
                               " error=" + (result.error.empty() ? "-" : result.error));
        if(!result.error.empty() || result.status != 200) {
            replicaError_ = result.error.empty() ?
                "replica transport returned status " + std::to_string(result.status) :
                std::move(result.error);
        } else {
            for(const auto& nodeId : split(jsonString(result.body, "successfulNodes"), ',')) {
                if(!nodeId.empty() && std::find(successfulNodes_.begin(), successfulNodes_.end(), nodeId) == successfulNodes_.end()) {
                    successfulNodes_.push_back(nodeId);
                }
            }
        }
        replicaCompleted_ = true;
        stage_ = "metadata_commit";
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
            stage_ = "client_response";
            miniKV::utils::logDebug("event=replica_chunk_reply chunk=" + chunkHash_);
            completeClient(200, "");
            return;
        }
        miniKV::utils::logDebug("event=chunk_gateway_commit_start chunk=" + chunkHash_ +
                                " successful_nodes=" + std::to_string(successfulNodes_.size()));
        gatewayCommitStartedAt_ = Clock::now();
        stage_ = "metadata_commit";
        std::weak_ptr<ChunkWriteCoordinator> weakSelf(shared_from_this());
        // The control client is process-scoped and may deliver its callback on
        // the acceptor/metadata I/O loop rather than the DataNode connection's
        // owning loop.  All coordinator state is owned by the upload loop, so
        // hop back before touching it.  Without this hop the old HTTP client
        // happened to work only when the connection was assigned to the base
        // loop; Multi-Reactor uploads could abort at commit completion.
        gatewayControl_->commitChunk({descriptor_.sessionId, descriptor_.chunkIndex,
                                    descriptor_.contentHash,
                                    descriptor_.contentLength, successfulNodes_,
                                    descriptor_.leaseId, uploadToken_, descriptor_.requestId},
            [weakSelf, ownerLoop = loop_](RpcResult result) mutable {
            ownerLoop->queueInLoop([weakSelf, result = std::move(result)]() mutable {
                if(auto self = weakSelf.lock()) {
                    self->requireLoopThread();
                    self->gatewayCommitFinishedAt_ = Clock::now();
                    miniKV::utils::logInfo("event=chunk_gateway_commit_result chunk=" + self->chunkHash_ +
                                           " http_status=" + std::to_string(result.httpStatus) +
                                           " error=" + (result.error.empty() ? "-" : result.error));
                    if(!result.ok) {
                        self->stage_ = "metadata_commit";
                        self->completeClient(500, result.error.empty() ? "Gateway commit failed" : std::move(result.error));
                        return;
                    }
                    self->stage_ = "client_response";
                    self->completeClient(200, "");
                }
            });
        });
    }

    void completeClient(int status, const std::string& error)
    {
        requireLoopThread();
        if(!completion_) return;
        const auto completedAt = Clock::now();
        if(replicaPipe_ != nullptr) replicaMetrics_ = replicaPipe_->metrics();
        const FastDataStore::WriteMetrics writeMetrics = writer_ == nullptr
            ? FastDataStore::WriteMetrics{} : writer_->metrics();
        const FastDataStore::DurabilityMetrics durabilityMetrics = store_.durabilityMetrics();
        const ChunkDiskWritePipeline::Metrics diskMetrics = diskPipeline_ == nullptr
            ? ChunkDiskWritePipeline::Metrics{} : diskPipeline_->metrics();
        const std::string line = std::string(status == 200 ? "event=chunk_complete" : "event=chunk_failed") +
            " request_id=" + descriptor_.requestId + " chunk=" + chunkHash_ +
            " chunk_id=" + descriptor_.chunkId +
            " identity_scheme=" + chunkIdentitySchemeName(descriptor_.identityScheme) +
            " checksum_type=" + chunkChecksumTypeName(descriptor_.checksum.type) +
            " object_version=" + std::to_string(descriptor_.objectVersion) +
            " generation=" + std::to_string(descriptor_.generation) +
            " session=" + descriptor_.sessionId +
            " index=" + std::to_string(descriptor_.chunkIndex) + " http_status=" +
            std::to_string(status) + " bytes=" + std::to_string(descriptor_.contentLength) +
            " role=" + std::string(position_ == 0 ? "primary" : "replica") +
            " replicas=" + std::to_string(successfulNodes_.size()) +
            " total_ms=" + std::to_string(elapsedMilliseconds(acceptedAt_, completedAt)) +
            " slot_lifetime_us=" + std::to_string(elapsedMicroseconds(acceptedAt_, completedAt)) +
            " body_to_completion_us=" + std::to_string(elapsedMicroseconds(bodyFinishedAt_, completedAt)) +
            " gateway_commit_us=" + std::to_string(elapsedMicroseconds(gatewayCommitStartedAt_, gatewayCommitFinishedAt_)) +
            " slot_release_count=" + std::to_string(uploadLease_.has_value() ? 1U : slotReleaseCount_) +
            " admission_wait_us=" + std::to_string(elapsedMicroseconds(admissionRequestedAt_, admissionAcquiredAt_)) +
            " body_receive_us=" + std::to_string(elapsedMicroseconds(firstBodyAt_, bodyFinishedAt_)) +
            " disk_queue_wait_us=" + std::to_string(elapsedMicroseconds(diskEnqueuedAt_, diskStartedAt_)) +
            " disk_write_wall_us=" + std::to_string(elapsedMicroseconds(diskStartedAt_, diskFinishedAt_)) +
            " replica_wait_us=" + std::to_string(elapsedMicroseconds(replicaStartedAt_, replicaFinishedAt_)) +
            " post_commit_us=" + std::to_string(elapsedMicroseconds(gatewayCommitFinishedAt_, completedAt)) +
            " slot_stage_model=sequential:admission_wait,body_receive,disk_queue;overlapped:disk_write,replica_wait;gateway_commit_after_local_replica" +
            " admission_reject_reason=" + (admissionRejectReason_.empty() ? "-" : admissionRejectReason_) +
            " body_receive_ms=" + std::to_string(elapsedMilliseconds(firstBodyAt_, localFinishedAt_)) +
            " checksum_update_us=" + std::to_string(
                writeMetrics.checksumUpdateNanoseconds / 1000ULL) +
            " sha_update_us=" + std::to_string(writeMetrics.shaUpdateNanoseconds / 1000ULL) +
            " pwrite_us=" + std::to_string(writeMetrics.pwriteNanoseconds / 1000ULL) +
            " pwrite_ops=" + std::to_string(writeMetrics.pwriteOperations) +
            " pwritev_ops=" + std::to_string(writeMetrics.pwritevOperations) +
            " write_ready_wait_us=" + std::to_string(
                writeMetrics.writeReadyWaitNanoseconds / 1000ULL) +
            " durability_batch_formation_us=" + std::to_string(
                writeMetrics.durabilityBatchFormationNanoseconds / 1000ULL) +
            " durability_worker_busy_wait_us=" + std::to_string(
                writeMetrics.durabilityWorkerBusyWaitNanoseconds / 1000ULL) +
            " durability_callback_dispatch_us=" + std::to_string(
                writeMetrics.durabilityCallbackDispatchNanoseconds / 1000ULL) +
            " completion_wakeup_us=" + std::to_string(
                writeMetrics.completionWakeupNanoseconds / 1000ULL) +
            " durability_pending_items_enqueue=" + std::to_string(
                writeMetrics.durabilityPendingItemsAtEnqueue) +
            " durability_pending_bytes_enqueue=" + std::to_string(
                writeMetrics.durabilityPendingBytesAtEnqueue) +
            " durability_pending_items_batch_start=" + std::to_string(
                writeMetrics.durabilityPendingItemsAtBatchStart) +
            " durability_pending_bytes_batch_start=" + std::to_string(
                writeMetrics.durabilityPendingBytesAtBatchStart) +
            " checksum_finalize_us=" + std::to_string(
                writeMetrics.checksumFinalizeNanoseconds / 1000ULL) +
            " sha_finalize_us=" + std::to_string(writeMetrics.shaFinalizeNanoseconds / 1000ULL) +
            " data_sync_us=" + std::to_string(writeMetrics.dataSyncNanoseconds / 1000ULL) +
            " data_sync_ops=" + std::to_string(writeMetrics.dataSyncOperations) +
            " index_us=" + std::to_string(writeMetrics.indexNanoseconds / 1000ULL) +
            " index_mutex_wait_us=" + std::to_string(
                writeMetrics.indexMutexWaitNanoseconds / 1000ULL) +
            " index_batch_build_us=" + std::to_string(
                writeMetrics.indexBatchBuildNanoseconds / 1000ULL) +
            " index_write_us=" + std::to_string(
                writeMetrics.indexWriteNanoseconds / 1000ULL) +
            " index_sync_ops=" + std::to_string(writeMetrics.indexSyncOperations) +
            " sync_timing_owner=" + std::string(
                writeMetrics.durabilitySyncTimingOwner ? "true" : "false") +
            " durable=" + std::string(writeMetrics.durable ? "true" : "false") +
            " durable_sequence=" + std::to_string(writeMetrics.durableSequence) +
            " group_wait_us=" + std::to_string(writeMetrics.groupWaitNanoseconds / 1000ULL) +
            " durability_queue_wait_us=" + std::to_string(
                writeMetrics.durabilityQueueWaitNanoseconds / 1000ULL) +
            " group_commit_us=" + std::to_string(writeMetrics.groupCommitNanoseconds / 1000ULL) +
            " group_batch_bytes=" + std::to_string(writeMetrics.groupBatchBytes) +
            " group_batch_items=" + std::to_string(writeMetrics.groupBatchItems) +
            " durable_pending_bytes=" + std::to_string(durabilityMetrics.pendingBytes) +
            " durable_pending_items=" + std::to_string(durabilityMetrics.pendingItems) +
            " durable_worker_count=" + std::to_string(durabilityMetrics.workerCount) +
            " durable_batches=" + std::to_string(durabilityMetrics.committedBatches) +
            " replica_ms=" + std::to_string(elapsedMilliseconds(localFinishedAt_, replicaFinishedAt_)) +
            " gateway_commit_ms=" + std::to_string(elapsedMilliseconds(
                gatewayCommitStartedAt_, gatewayCommitFinishedAt_)) +
            " failure_stage=" + (status == 200 ? "none" : stage_) +
            " pauses=" + std::to_string(replicaMetrics_.pauseCount) +
            " pause_ms=" + std::to_string(replicaMetrics_.pauseNanoseconds / 1000000ULL) +
            " max_pending_bytes=" + std::to_string(replicaMetrics_.maxPendingBytes) +
            " disk_queue_peak_bytes=" + std::to_string(diskMetrics.peakQueuedBytes) +
            " disk_batches=" + std::to_string(diskMetrics.submittedBatches) +
            " disk_batch_bytes=" + std::to_string(diskMetrics.submittedBatchBytes) +
            " disk_batch_peak_bytes=" + std::to_string(diskMetrics.peakBatchBytes) +
            " disk_pause_count=" + std::to_string(diskMetrics.pauseCount) +
            " disk_pause_ms=" + std::to_string(diskMetrics.pauseNanoseconds / 1000000ULL) +
            " error=" + (error.empty() ? "-" : error);
        if(status == 200) miniKV::utils::logInfo(line);
        else miniKV::utils::logError(line);
        if(status != 200 && position_ == 0 && !uploadToken_.empty()) {
            gatewayControl_->releaseLease({descriptor_.leaseId, uploadToken_, descriptor_.requestId}, [](RpcResult) {});
        }
        releaseActiveWrite();
        ChunkWriteResult result;
        result.status = status;
        result.error = error;
        result.contentHash = descriptor_.contentHash;
        result.chunkId = descriptor_.chunkId;
        result.alreadyExists = alreadyExists_;
        result.successfulNodes = successfulNodes_;
        result.replicaWarning = replicaError_;
        auto completion = std::move(completion_);
        replicaPipe_.reset();
        diskPipeline_.reset();
        selfHold_.reset();
        completion(std::move(result));
    }

    void releaseActiveWrite()
    {
        if(uploadLease_.has_value()) {
            slotReleasedAt_ = Clock::now();
            ++slotReleaseCount_;
        }
        uploadLease_.reset();
    }

    void onDiskStage(const char* stage, Clock::time_point at)
    {
        if(stage == nullptr) return;
        const auto now = at == Clock::time_point{} ? Clock::now() : at;
        const std::string name(stage);
        if(name == "disk_enqueued" && diskEnqueuedAt_ == Clock::time_point{}) {
            diskEnqueuedAt_ = now;
        } else if(name == "disk_started" && diskStartedAt_ == Clock::time_point{}) {
            diskStartedAt_ = now;
        } else if(name == "disk_finished") {
            diskFinishedAt_ = now;
        }
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

    static uint64_t elapsedMicroseconds(Clock::time_point started, Clock::time_point finished)
    {
        if(started == Clock::time_point{} || finished == Clock::time_point{} || finished < started) return 0;
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            finished - started).count());
    }

    EventLoop* loop_;
    ChunkStore& store_;
    NodeResourceGovernor& resourceGovernor_;
    DiskWriteExecutor& diskExecutor_;
    ChunkDiskWritePipeline::Config diskPipelineConfig_;
    std::shared_ptr<ReplicaTransport> replicaTransport_;
    std::string nodeId_;
    std::string gatewayAddress_;
    uint16_t gatewayPort_ = 0;
    std::string clusterSecret_;
    std::shared_ptr<ChunkCommitReporter> gatewayControl_;
    std::string chunkHash_;
    std::string clientId_;
    std::string uploadToken_;
    ChunkWriteDescriptor descriptor_;
    std::vector<ReplicaTarget> chain_;
    size_t position_ = 0;
    std::shared_ptr<FastDataStore::WriteSession> writer_;
    ChunkDiskWritePipeline::Ptr diskPipeline_;
    std::shared_ptr<ReplicaWriteStream> replicaPipe_;
    CompletionCallback completion_;
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
    Clock::time_point admissionRequestedAt_{};
    Clock::time_point admissionAcquiredAt_{};
    Clock::time_point firstBodyAt_{};
    Clock::time_point bodyFinishedAt_{};
    Clock::time_point diskEnqueuedAt_{};
    Clock::time_point diskStartedAt_{};
    Clock::time_point diskFinishedAt_{};
    Clock::time_point replicaStartedAt_{};
    Clock::time_point localFinishedAt_{};
    Clock::time_point replicaFinishedAt_{};
    Clock::time_point gatewayCommitStartedAt_{};
    Clock::time_point gatewayCommitFinishedAt_{};
    Clock::time_point slotReleasedAt_{};
    std::string admissionRejectReason_;
    // Last externally meaningful stage; emitted only on failure so a short
    // benchmark can separate admission, body, disk, replica, and metadata
    // failures without inferring them from a free-form error string.
    std::string stage_ = "accepted";
    uint32_t slotReleaseCount_ = 0;
    ReplicaTransportMetrics replicaMetrics_;
    std::shared_ptr<ChunkWriteCoordinator> selfHold_;
};

class HttpChunkUploadStream : public std::enable_shared_from_this<HttpChunkUploadStream> {
public:
    HttpChunkUploadStream(EventLoop* loop, ChunkStore& store,
                          NodeResourceGovernor& resourceGovernor,
                          DiskWriteExecutor& diskExecutor,
                          ChunkDiskWritePipeline::Config diskPipelineConfig,
                          ReplicaConnectionPool::Ptr replicaConnectionPool,
                          std::string nodeId, std::string gatewayAddress,
                          uint16_t gatewayPort, std::string clusterSecret,
                          std::shared_ptr<ChunkCommitReporter> controlClient,
                          CorsPolicy corsPolicy, std::string requestOrigin,
                          const HttpRequest& request, std::string storageIdentity)
        : corsPolicy_(std::move(corsPolicy)), requestOrigin_(std::move(requestOrigin))
    {
        miniKV::utils::logDebug("event=chunk_stream_setup node=" + nodeId +
                                " route=" + storageIdentity +
                                " session=" + request.getHeader("X-Session-Id") +
                                " index=" + request.getHeader("X-Chunk-Index"));
        auto decoded = HttpChunkUploadAdapter::decode(
            request, storageIdentity, nodeId, clusterSecret);
        if(!decoded.ok) {
            decodeError_ = std::move(decoded.error);
            miniKV::utils::logWarn("event=chunk_upload_decode_failed node=" + nodeId +
                                   " route=" + storageIdentity +
                                   " reason=" + decodeError_ +
                                   " session=" + request.getHeader("X-Session-Id") +
                                   " chunk_index=" + request.getHeader("X-Chunk-Index") +
                                   " content_length=" + std::to_string(request.contentLength()));
            return;
        }
        requestId_ = decoded.descriptor.requestId;
        auto replicaTransport = std::make_shared<HttpReplicaTransport>(
            loop, std::move(replicaConnectionPool));
        coordinator_ = std::make_shared<ChunkWriteCoordinator>(
            loop, store, resourceGovernor, diskExecutor, diskPipelineConfig,
            std::move(replicaTransport), std::move(nodeId),
            std::move(gatewayAddress), gatewayPort, std::move(clusterSecret),
            std::move(decoded.descriptor), std::move(controlClient));
    }

    void start(const TcpConnectionPtr& upstream)
    {
        if(!coordinator_) return;
        std::weak_ptr<miniKV::network::TcpConnection> weakUpstream(upstream);
        coordinator_->startReplica([weakUpstream] {
            if(auto connection = weakUpstream.lock()) connection->resumeRead();
        });
    }

    StreamConsumeResult consume(const char* bytes, size_t size)
    {
        if(!coordinator_) return StreamConsumeResult::kAbort;
        return coordinator_->consume(bytes, size);
    }

    void finish(const DeferredResponse::Ptr& deferred)
    {
        deferred->defer();
        selfHold_ = shared_from_this();
        if(!coordinator_) {
            complete(deferred, {400, decodeError_});
            return;
        }
        std::weak_ptr<HttpChunkUploadStream> weakSelf(shared_from_this());
        coordinator_->finish([weakSelf, deferred](ChunkWriteResult result) {
            if(auto self = weakSelf.lock()) self->complete(deferred, std::move(result));
        });
    }

private:
    void complete(const DeferredResponse::Ptr& deferred, ChunkWriteResult result)
    {
        HttpResponse response;
        if(result.status != 200) {
            json(&response, result.status, jsonError(result.error));
            if(result.status == 503) response.addHeader("Retry-After", "1");
        } else {
            std::ostringstream body;
            body << "{\"chunkHash\":\"" << result.contentHash
                 << "\",\"chunkId\":\"" << result.chunkId
                 << "\",\"alreadyExists\":"
                 << (result.alreadyExists ? "true" : "false")
                 << ",\"successfulNodes\":\""
                 << join(result.successfulNodes, ',') << "\"";
            if(!result.replicaWarning.empty()) {
                body << ",\"replicaWarning\":\""
                     << jsonEscape(result.replicaWarning) << "\"";
            }
            body << "}";
            json(&response, 200, body.str());
        }
        corsPolicy_.appendHeaders(response, requestOrigin_);
        if(!requestId_.empty()) response.addHeader("X-Request-Id", requestId_);
        deferred->complete(std::move(response));
        selfHold_.reset();
    }

    CorsPolicy corsPolicy_;
    std::string requestOrigin_;
    std::string decodeError_;
    std::string requestId_;
    std::shared_ptr<ChunkWriteCoordinator> coordinator_;
    std::shared_ptr<HttpChunkUploadStream> selfHold_;
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
    const std::string bootId = [] {
        if(const char* value = std::getenv("MINIKV_V2_BOOT_ID")) return std::string(value);
        return miniKV::util::randomId();
    }();
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

    FastDataStore::Config storeConfig;
    std::string durabilityMode;
    if(!configuredDurabilityPolicy(storeConfig.durabilityPolicy, durabilityMode)) {
        miniKV::utils::logError("event=invalid_durability_mode value=" + durabilityMode);
        std::cerr << "MINIKV_V3_DURABILITY_MODE must be buffered, chunk_sync, or group_commit\n";
        return 2;
    }
    if(!configuredGroupCommit(storeConfig.groupCommit)) {
        miniKV::utils::logError("event=invalid_group_commit_config");
        std::cerr << "invalid MINIKV_V3_GROUP_COMMIT_* configuration\n";
        return 2;
    }
    storeConfig.physicalIndexDirectory = configuredPhysicalIndexDirectory(nodeId);
    const std::string physicalIndexDirectory = storeConfig.physicalIndexDirectory.empty()
        ? dataDir + "/physical_index" : storeConfig.physicalIndexDirectory;
    FastDataStore store(dataDir, storeConfig);
    if(!store.open()) {
        miniKV::utils::logError("event=datanode_store_open_failed data_dir=" + dataDir);
        std::cerr << "cannot open DataNode store\n";
        return 1;
    }
    miniKV::utils::logInfo(
        "event=datanode_durability_config mode=" + durabilityMode +
        " data_dir=" + dataDir +
        " physical_index_dir=" +
        physicalIndexDirectory +
        " data_device=" + deviceNumberForPath(dataDir + "/disk0.data") +
        " index_device=" + deviceNumberForPath(physicalIndexDirectory));
    FastDataStoreChunkStore chunkStore(store);
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
    const uint32_t diskWriteWorkers = configuredDiskLimit(
        "MINIKV_V4_DISK_WRITE_WORKERS", kDefaultDiskWriteWorkers, kMaxDiskWriteWorkers);
    const uint32_t diskWriteBlocks = configuredDiskLimit(
        "MINIKV_V4_DISK_WRITE_BLOCKS", kDefaultDiskWriteBlocks, kMaxDiskWriteBlocks);
    std::string writeBatchMode;
    const ChunkDiskWritePipeline::Config diskPipelineConfig =
        configuredDiskPipeline(writeBatchMode);
    NodeResourceGovernor::Config resourceConfig;
    resourceConfig.maxActiveUploads = maxConcurrentWrites;
    resourceConfig.maxActiveDownloads = maxConcurrentDownloads;
    resourceConfig.maxUploadsPerClient = maxUploadsPerClient;
    resourceConfig.maxDownloadsPerClient = maxDownloadsPerClient;
    NodeResourceGovernor resourceGovernor(resourceConfig);
    EventLoop loop;
    DiskWriteExecutor::Config diskConfig;
    diskConfig.workerCount = diskWriteWorkers;
    diskConfig.blockCount = diskWriteBlocks;
    DiskWriteExecutor diskExecutor(diskConfig);
    miniKV::utils::logInfo("event=datanode_runtime_config"
        " io_threads=" + std::to_string(ioThreads) +
        " max_active_uploads=" + std::to_string(maxConcurrentWrites) +
        " max_active_downloads=" + std::to_string(maxConcurrentDownloads) +
        " max_uploads_per_client=" + std::to_string(maxUploadsPerClient) +
        " max_downloads_per_client=" + std::to_string(maxDownloadsPerClient) +
        " disk_write_workers=" + std::to_string(diskWriteWorkers) +
        " disk_write_blocks=" + std::to_string(diskWriteBlocks) +
        " write_batch_mode=" + writeBatchMode +
        " write_batch_bytes=" + std::to_string(diskPipelineConfig.targetBatchBytes) +
        " write_batch_delay_us=" + std::to_string(diskPipelineConfig.maxBatchDelayUs) +
        " durability_mode=" + durabilityMode +
        " group_commit_bytes=" + std::to_string(storeConfig.groupCommit.maxBatchBytes) +
        " group_commit_items=" + std::to_string(storeConfig.groupCommit.maxBatchItems) +
        " group_commit_delay_us=" + std::to_string(storeConfig.groupCommit.maxBatchDelayUs) +
        " group_commit_max_pending_bytes=" +
            std::to_string(storeConfig.groupCommit.maxPendingBytes) +
        " group_commit_max_pending_items=" +
            std::to_string(storeConfig.groupCommit.maxPendingItems) +
        " durability_workers=" + std::to_string(
            storeConfig.durabilityPolicy == DurabilityPolicy::kGroupCommit ? 1U : 0U) +
        " sendfile_quantum_bytes=" + std::to_string(sendFileQuantumBytes));
    std::shared_ptr<ChunkCommitReporter> controlClient;
    if(const char* endpoints = std::getenv("MINIKV_DATANODE_METADATA_ENDPOINTS");
       endpoints != nullptr && *endpoints != '\0') {
        auto metadataClient = std::make_shared<miniKV::metadata::MetadataClient>(
            miniKV::metadata::MetadataClient::parseEndpoints(endpoints), clusterSecret, 1000);
        controlClient = std::make_shared<MetadataControlClient>(
            &loop, std::move(metadataClient), nodeId, bootId);
        miniKV::utils::logInfo("event=datanode_control_transport mode=metadata-direct endpoints=" +
                               std::string(endpoints));
    } else {
        controlClient = std::make_shared<HttpGatewayControlClient>(
            &loop, gatewayAddress, gatewayPort, clusterSecret);
        miniKV::utils::logInfo("event=datanode_control_transport mode=gateway-compat endpoint=" +
                               gatewayAddress + ":" + std::to_string(gatewayPort));
    }

    std::function<void()> registerNode;
    std::function<void()> heartbeat;
    bool registered = false;
    bool registrationInFlight = false;
    // Metadata deduplicates registration by nodeId + bootId.  Keep the
    // matching request payload immutable for this process lifetime: free
    // bytes change whenever this node writes a log or chunk, and changing it
    // on a retry turns an idempotent replay into COMMAND_ID_REUSE_MISMATCH.
    const uint64_t registeredCapacityBytes = availableBytes(dataDir);
    registerNode = [&] {
        if(registered || registrationInFlight) return;
        registrationInFlight = true;
        controlClient->registerStorageNode({nodeId, bootId, advertiseAddress, port,
                                            registeredCapacityBytes, 0,
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
                loop.runAfter(1000, registerNode);
            });
    };
    heartbeat = [&] {
        controlClient->sendHeartbeat({nodeId, logicalUsedBytes(store), effectiveFreeBytes(store, dataDir),
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
        auto stream = std::make_shared<HttpChunkUploadStream>(upstream->ownerLoop(), chunkStore,
        resourceGovernor, diskExecutor, diskPipelineConfig, std::move(replicaPool), nodeId,
        gatewayAddress, gatewayPort, clusterSecret, controlClient, corsPolicy,
        request.getHeader("Origin"), request, hash);
        stream->start(upstream);
        context->setUserData(stream);
        context->setBodyCallback(request.contentLength(), [stream](const char* bytes, size_t size) {
            return toHttpBodyConsumeResult(stream->consume(bytes, size));
        });
    });
    server.setHttpCallback([&](const HttpRequest& request, HttpResponse* response,
        const TcpConnectionPtr& connection, const DeferredResponse::Ptr& deferred) {
        const std::string& path = request.path();
        std::string requestId = request.getHeader("X-Request-Id");
        if(requestId.empty()) requestId = randomId();
        response->addHeader("X-Request-Id", requestId);
        if(request.method() == HttpRequest::kGet && path == "/healthz") {
            json(response, 200, "{\"status\":\"ok\",\"component\":\"datanode\"}");
            return;
        }
        if(request.method() == HttpRequest::kGet && path == "/readyz") {
            // Readiness is a process-level admission signal, not a probe that
            // consumes an upload lease.  Group-commit is the only durability
            // mode with an additional bounded pending queue.
            if(!registered || !store.canAcceptDurability(1)) {
                json(response, 503, jsonError("datanode is not ready"));
            } else {
                json(response, 200, "{\"status\":\"ready\",\"component\":\"datanode\"}");
            }
            return;
        }
        const std::string origin = request.getHeader("Origin");
        const bool chunkRequest = beginsWith(path, "/v2/chunks/");
        const bool internalChunkRead =
            (request.method() == HttpRequest::kGet || request.method() == HttpRequest::kHead) &&
            beginsWith(path, "/internal/v3/chunks/");
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
        miniKV::util::ReadCapability readCapability;
        if(internalChunkRead) {
            const std::string storageIdentity =
                path.substr(std::string("/internal/v3/chunks/").size());
            if(!verifyReadCapability(request.getHeader("X-Read-Token"),
                                     clusterSecret, readCapability) ||
               storageIdentity.empty() ||
               !constantTimeEquals(readCapability.storageIdentity, storageIdentity)) {
                json(response, 403, jsonError("invalid read capability"));
                return;
            }
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
            auto stream = std::static_pointer_cast<HttpChunkUploadStream>(request.userData());
            if(stream == nullptr) {
                json(response, 400, jsonError("streaming body is required"));
                corsPolicy.appendHeaders(*response, origin);
                return;
            }
            stream->finish(deferred);
            return;
        }
        if((request.method() == HttpRequest::kGet || request.method() == HttpRequest::kHead) &&
           (chunkRequest || internalChunkRead)) {
            const auto handlerStartedAt = std::chrono::steady_clock::now();
            const std::string hash = internalChunkRead
                ? path.substr(std::string("/internal/v3/chunks/").size())
                : path.substr(std::string("/v2/chunks/").size());
            FileRegion region;
            if(!store.getRegion(hash, region)) {
                json(response, 404, jsonError("chunk not found"));
                corsPolicy.appendHeaders(*response, origin);
                return;
            }
            const auto metadataReadyAt = std::chrono::steady_clock::now();
            const uint64_t fullLength = region.length;
            miniKV::http::ByteRange selectedRange;
            const miniKV::http::RangeParseStatus rangeStatus =
                miniKV::http::parseSingleByteRange(
                    request.getHeader("Range"), fullLength, selectedRange);
            if(rangeStatus == miniKV::http::RangeParseStatus::kInvalid ||
               rangeStatus == miniKV::http::RangeParseStatus::kUnsatisfiable) {
                response->setStatusCode(HttpResponse::k416RangeNotSatisfiable);
                response->addHeader("Content-Range", "bytes */" + std::to_string(fullLength));
                response->addHeader("Accept-Ranges", "bytes");
                response->setBody("");
                if(chunkRequest) corsPolicy.appendHeaders(*response, origin);
                return;
            }
            const bool partial = rangeStatus == miniKV::http::RangeParseStatus::kSatisfiable;
            const uint64_t selectedStart = partial ? selectedRange.start : 0;
            const uint64_t selectedLength = partial ? selectedRange.length : fullLength;
            response->setStatusCode(partial ? HttpResponse::k206PartialContent
                                            : HttpResponse::k200Ok);
            response->addHeader("Accept-Ranges", "bytes");
            response->addHeader("Content-Length", std::to_string(selectedLength));
            if(partial) {
                response->addHeader("Content-Range", "bytes " +
                    std::to_string(selectedStart) + "-" +
                    std::to_string(selectedStart + selectedLength - 1) + "/" +
                    std::to_string(fullLength));
            }
            response->addHeader("X-Chunk-Hash", hash);
            if(internalChunkRead) {
                response->addHeader("X-Object-Id", readCapability.objectId);
                response->addHeader("X-Object-Version",
                                    std::to_string(readCapability.objectVersion));
            }
            if(request.method() == HttpRequest::kGet) {
                std::string clientId = request.getHeader("X-Client-Instance-Id");
                if(clientId.empty()) {
                    clientId = "connection:" + std::to_string(connection->fd());
                }
                const auto admissionStartedAt = std::chrono::steady_clock::now();
                auto lease = resourceGovernor.tryAcquireDownload(clientId);
                const auto admissionReadyAt = std::chrono::steady_clock::now();
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
                response->setFileBody(store.dataFilePath(),
                    region.offset + static_cast<off_t>(selectedStart),
                    static_cast<size_t>(selectedLength),
                    [sharedLease, hash, requestId, selectedStart, selectedLength, startedAt,
                     handlerStartedAt, metadataReadyAt, admissionStartedAt, admissionReadyAt](
                        const miniKV::network::SendFileResult& result) {
                        (void)sharedLease;
                        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - startedAt).count();
                        const std::string line = "event=chunk_download_complete request_id=" + requestId +
                            " chunk=" + hash +
                            " success=" + (result.success ? "true" : "false") + " bytes=" +
                            std::to_string(result.bytesSent) + " range_start=" +
                            std::to_string(selectedStart) + " range_length=" +
                            std::to_string(selectedLength) + " total_ms=" +
                            std::to_string(elapsed) +
                            " handler_to_metadata_us=" + std::to_string(
                                std::chrono::duration_cast<std::chrono::microseconds>(
                                    metadataReadyAt - handlerStartedAt).count()) +
                            " admission_us=" + std::to_string(
                                std::chrono::duration_cast<std::chrono::microseconds>(
                                    admissionReadyAt - admissionStartedAt).count()) +
                            " response_prepare_us=" + std::to_string(
                                std::chrono::duration_cast<std::chrono::microseconds>(
                                    startedAt - admissionReadyAt).count()) +
                            " sendfile_calls=" + std::to_string(result.writeCalls) +
                            " sendfile_syscall_us=" + std::to_string(result.syscallNanoseconds / 1000ULL) +
                            " sendfile_eagain=" + std::to_string(result.wouldBlockCount) +
                            " sendfile_blocked_us=" + std::to_string(result.wouldBlockNanoseconds / 1000ULL) +
                            " sendfile_first_attempt_us=" + std::to_string(result.firstAttemptNanoseconds / 1000ULL);
                        if(result.success) miniKV::utils::logInfo(line);
                        else miniKV::utils::logWarn(line);
                    });
            }
            if(chunkRequest) corsPolicy.appendHeaders(*response, origin);
            return;
        }
        json(response, 404, jsonError("route not found"));
    });
    loop.runAfter(0, [&] { registerNode(); heartbeat(); });
    // Metadata Raft placement treats a heartbeat as fresh for 6 seconds.
    // Keep the data-node heartbeat comfortably below that window so a
    // healthy node is not temporarily filtered out between two heartbeats.
    loop.runEvery(2000, heartbeat);
    loop.runEvery(1000, [&] {
        const NodeResourceGovernor::Snapshot resources = resourceGovernor.snapshot();
        const DiskWriteExecutor::Metrics disk = diskExecutor.metrics();
        const FastDataStore::DurabilityMetrics durability = store.durabilityMetrics();
        const miniKV::network::TcpConnection::OutputMetrics output = server.outputBufferMetrics();
        miniKV::utils::logInfo("event=resource_snapshot"
            " active_uploads=" + std::to_string(resources.activeUploads) +
            " active_downloads=" + std::to_string(resources.activeDownloads) +
            " upload_acquire_attempts=" + std::to_string(resources.uploadAcquireAttempts) +
            " upload_acquire_successes=" + std::to_string(resources.uploadAcquireSuccesses) +
            " upload_rejects=" + std::to_string(resources.uploadRejects) +
            " upload_rejects_global=" + std::to_string(resources.uploadRejectsGlobal) +
            " upload_rejects_per_client=" + std::to_string(resources.uploadRejectsPerClient) +
            " upload_releases=" + std::to_string(resources.uploadReleases) +
            " disk_queued_tasks=" + std::to_string(disk.queuedTasks) +
            " disk_queue_peak_tasks=" + std::to_string(disk.peakQueuedTasks) +
            " block_available=" + std::to_string(disk.availableBlocks) +
            " block_total=" + std::to_string(disk.totalBlocks) +
            " block_leased_bytes=" + std::to_string(disk.leasedBytes) +
            " block_peak_leased_bytes=" + std::to_string(disk.peakLeasedBytes) +
            " disk_active_workers=" + std::to_string(disk.activeWorkers) +
            " disk_total_workers=" + std::to_string(disk.totalWorkers) +
            " disk_completed_tasks=" + std::to_string(disk.completedTasks) +
            " disk_started_tasks=" + std::to_string(disk.startedTasks) +
            " disk_queue_wait_total_us=" + std::to_string(disk.totalQueueWaitUs) +
            " disk_queue_wait_max_us=" + std::to_string(disk.maxQueueWaitUs) +
            " disk_work_total_us=" + std::to_string(disk.totalWorkUs) +
            " disk_work_max_us=" + std::to_string(disk.maxWorkUs) +
            " durable_pending_bytes=" + std::to_string(durability.pendingBytes) +
            " durable_peak_pending_bytes=" + std::to_string(durability.peakPendingBytes) +
            " durable_pending_items=" + std::to_string(durability.pendingItems) +
            " durable_peak_pending_items=" + std::to_string(durability.peakPendingItems) +
            " durable_active_sync_operations=" + std::to_string(durability.activeSyncOperations) +
            " durable_submitted_items=" + std::to_string(durability.submittedItems) +
            " durable_committed_items=" + std::to_string(durability.committedItems) +
            " durable_committed_batches=" + std::to_string(durability.committedBatches) +
            " durable_failed_batches=" + std::to_string(durability.failedBatches) +
            " durable_data_sync_operations=" + std::to_string(durability.dataSyncOperations) +
            " durable_index_sync_operations=" + std::to_string(durability.indexSyncOperations) +
            " durable_pwrite_ops_while_sync=" + std::to_string(
                durability.pwriteOperationsWhileSync) +
            " durable_pwrite_bytes_while_sync=" + std::to_string(
                durability.pwriteBytesWhileSync) +
            " durable_last_batch_bytes=" + std::to_string(durability.lastBatchBytes) +
            " durable_last_batch_items=" + std::to_string(durability.lastBatchItems) +
            " durable_last_batch_pending_bytes=" + std::to_string(
                durability.lastBatchPendingBytes) +
            " durable_last_batch_pending_items=" + std::to_string(
                durability.lastBatchPendingItems) +
            " durable_last_batch_formation_us=" + std::to_string(
                durability.lastBatchFormationNanoseconds / 1000ULL) +
            " durable_last_data_sync_us=" + std::to_string(
                durability.lastDataSyncNanoseconds / 1000ULL) +
            " durable_last_index_sync_us=" + std::to_string(
                durability.lastIndexSyncNanoseconds / 1000ULL) +
            " durable_last_index_mutex_wait_us=" + std::to_string(
                durability.lastIndexMutexWaitNanoseconds / 1000ULL) +
            " durable_last_index_batch_build_us=" + std::to_string(
                durability.lastIndexBatchBuildNanoseconds / 1000ULL) +
            " durable_last_index_write_us=" + std::to_string(
                durability.lastIndexWriteNanoseconds / 1000ULL) +
            " durable_last_batch_commit_us=" + std::to_string(
                durability.lastBatchCommitNanoseconds / 1000ULL) +
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
