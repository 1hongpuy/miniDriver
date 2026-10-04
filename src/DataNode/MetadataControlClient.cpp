#include "DataNode/MetadataControlClient.hpp"

#include "network/EventLoop.hpp"
#include "utils/Util.hpp"

#include <thread>
#include <ctime>

namespace miniKV::datanode {
namespace {

RpcResult resultFor(const metadata::ApplyResult& result, std::string error = {})
{
    RpcResult out;
    out.nodeEpoch = result.nodeEpoch;
    out.httpStatus = result.status == metadata::ApplyStatus::kUnavailable ? 503 :
                     (result.status == metadata::ApplyStatus::kInvalid ||
                      result.status == metadata::ApplyStatus::kNotFound ||
                      result.status == metadata::ApplyStatus::kConflict ||
                      result.status == metadata::ApplyStatus::kFenced ? 400 : 200);
    out.ok = result.status == metadata::ApplyStatus::kOk ||
             result.status == metadata::ApplyStatus::kAlreadyApplied;
    out.error = error.empty() ? result.message : std::move(error);
    if(!out.ok && out.error.empty()) out.error = "metadata command rejected";
    return out;
}

}  // namespace

MetadataControlClient::MetadataControlClient(network::EventLoop* loop,
                                             std::shared_ptr<metadata::MetadataClient> client,
                                             std::string nodeId,
                                             std::string bootId)
    : loop_(loop), client_(std::move(client)), nodeId_(std::move(nodeId)), bootId_(std::move(bootId)) {}

void MetadataControlClient::post(RpcResult result, RpcCallback callback) const
{
    if(!callback) return;
    loop_->queueInLoop([result = std::move(result), callback = std::move(callback)]() mutable {
        callback(std::move(result));
    });
}

void MetadataControlClient::registerStorageNode(const NodeRegistration& request,
                                                 RpcCallback callback)
{
    auto client = client_;
    const std::string nodeId = request.nodeId.empty() ? nodeId_ : request.nodeId;
    const std::string bootId = request.bootId.empty() ? bootId_ : request.bootId;
    std::thread([this, client, request, nodeId, bootId, callback = std::move(callback)]() mutable {
        metadata::MetadataCommand command;
        command.commandId = "datanode-register-" + nodeId + "-" + bootId;
        command.type = metadata::MetadataCommandType::kRegisterNode;
        command.actorType = "datanode";
        command.actorId = nodeId;
        command.payload = metadata::RegisterNodePayload{
            nodeId, bootId, request.address, request.httpPort,
            request.maxStorageBytes, request.capabilities};
        std::string error;
        const auto result = client->propose(command, &error);
        if(result.nodeEpoch != 0) nodeEpoch_.store(result.nodeEpoch, std::memory_order_release);
        post(resultFor(result, std::move(error)), std::move(callback));
    }).detach();
}

void MetadataControlClient::sendHeartbeat(const NodeHeartbeat& request,
                                           RpcCallback callback)
{
    auto client = client_;
    metadata::NodeHeartbeat heartbeat;
    heartbeat.nodeId = request.nodeId.empty() ? nodeId_ : request.nodeId;
    heartbeat.nodeEpoch = nodeEpoch_.load(std::memory_order_acquire);
    heartbeat.freeBytes = request.freeBytes;
    heartbeat.activeUploads = request.activeUploads;
    heartbeat.queueDepth = request.activeUploads;
    heartbeat.observedAtMs = static_cast<int64_t>(std::time(nullptr)) * 1000;
    std::thread([this, client, heartbeat, callback = std::move(callback)]() mutable {
        std::string error;
        const bool ok = client->heartbeat(heartbeat, &error);
        RpcResult result; result.ok = ok; result.httpStatus = ok ? 200 : 503;
        result.error = std::move(error);
        post(std::move(result), std::move(callback));
    }).detach();
}

void MetadataControlClient::commitChunk(const ChunkCommit& request,
                                         RpcCallback callback)
{
    auto client = client_;
    std::thread([this, client, request, callback = std::move(callback)]() mutable {
        std::string error;
        const auto result = client->commitChunk(request.sessionId, request.chunkIndex,
                                                request.chunkHash, request.size,
                                                request.successfulNodes,
                                                request.leaseId, &error);
        post(resultFor(result, std::move(error)), std::move(callback));
    }).detach();
}

void MetadataControlClient::releaseLease(const LeaseRelease& request,
                                          RpcCallback callback)
{
    auto client = client_;
    std::thread([this, client, request, callback = std::move(callback)]() mutable {
        metadata::ApplyResult result;
        std::string error;
        const auto leaseId = request.leaseId;
        const auto lease = client->lease(leaseId, &error);
        if(!lease) {
            result.status = metadata::ApplyStatus::kNotFound;
            result.message = error.empty() ? "lease not found" : error;
        } else {
            metadata::MetadataCommand command;
            command.commandId = "datanode-release-" + leaseId + "-" + std::to_string(lease->generation);
            command.type = metadata::MetadataCommandType::kReleaseLease;
            command.actorType = "datanode"; command.actorId = nodeId_;
            command.generation = lease->generation;
            command.payload = metadata::ReleaseLeasePayload{leaseId, lease->generation};
            result = client->propose(command, &error);
        }
        post(resultFor(result, std::move(error)), std::move(callback));
    }).detach();
}

}  // namespace miniKV::datanode
