#include "metadata/MetadataService.hpp"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <tuple>

namespace miniKV::metadata {

MetadataService::MetadataService(std::string snapshotDirectory)
{
    if(!snapshotDirectory.empty()) {
        snapshotStore_ = std::make_unique<SnapshotStore>(std::move(snapshotDirectory));
        std::string ignored;
        if(const auto snapshot = snapshotStore_->load(&ignored)) {
            if(state_.restore(*snapshot)) nextIndex_ = state_.metadataVersion() + 1;
        }
    }
    softState_.beginLeaderTerm(status_.term, 0);
}

ApplyResult MetadataService::unavailable(const MetadataCommand& command, const std::string& message) const
{
    ApplyResult result; result.commandId = command.commandId; result.status = ApplyStatus::kUnavailable;
    result.metadataVersion = state_.metadataVersion(); result.appliedIndex = state_.metadataVersion();
    result.appliedTerm = state_.lastAppliedTerm(); result.message = message; return result;
}

bool MetadataService::persistLocked(std::string* error)
{ return !snapshotStore_ || snapshotStore_->publish(state_.snapshot(), error); }

ApplyResult MetadataService::propose(const MetadataCommand& command)
{
    const auto started = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    auto record = [&](ApplyResult result) {
        const auto elapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - started).count());
        std::lock_guard<std::mutex> metricsLock(metricsMutex_);
        ++metrics_.proposalCount; metrics_.proposalTotalUs += elapsed;
        if(result.status != ApplyStatus::kOk && result.status != ApplyStatus::kAlreadyApplied)
            ++metrics_.proposalFailureCount;
        return result;
    };
    if(status_.role != "leader" || !status_.leaderResolved || !status_.quorumWritable)
        return record(unavailable(command, "metadata leader or quorum is unavailable"));
    ApplyResult result = state_.apply(command, nextIndex_++, status_.term);
    std::string error;
    if(!persistLocked(&error)) {
        status_.quorumWritable = false;
        return record(unavailable(command, "metadata snapshot persistence failed: " + error));
    }
    return record(std::move(result));
}

ApplyResult MetadataService::proposeBatch(const std::vector<MetadataCommand>& commands)
{
    if(commands.empty()) {
        ApplyResult result; result.status = ApplyStatus::kInvalid; result.message = "empty metadata command batch"; return result;
    }
    // The standalone service has no Raft append path.  Apply the same
    // independently-idempotent commands under one state lock and persist a
    // single snapshot publication, preserving the batch endpoint semantics.
    const auto started = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    ApplyResult last;
    for(const auto& command : commands) {
        if(status_.role != "leader" || !status_.leaderResolved || !status_.quorumWritable)
            last = unavailable(command, "metadata leader or quorum is unavailable");
        else
            last = state_.apply(command, nextIndex_++, status_.term);
        if(last.status != ApplyStatus::kOk && last.status != ApplyStatus::kAlreadyApplied) break;
    }
    if(last.status == ApplyStatus::kOk || last.status == ApplyStatus::kAlreadyApplied) {
        std::string error;
        if(!persistLocked(&error)) {
            status_.quorumWritable = false;
            last = unavailable(commands.back(), "metadata snapshot persistence failed: " + error);
        }
    }
    const auto elapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - started).count());
    {
        std::lock_guard<std::mutex> metricsLock(metricsMutex_);
        metrics_.proposalCount += commands.size();
        metrics_.proposalTotalUs += elapsed;
        if(last.status != ApplyStatus::kOk && last.status != ApplyStatus::kAlreadyApplied)
            ++metrics_.proposalFailureCount;
    }
    return last;
}

ApplyResult MetadataService::createSessionAndReserve(const MetadataCommand& create,
                                                      const std::vector<ReserveLeaseRequest>& requests)
{
    // The in-process compatibility service has no replicated append batch.
    // Preserve the same command ordering and idempotency semantics; NuRaft
    // provides the actual single-fsync implementation.
    auto result = propose(create);
    if(result.status != ApplyStatus::kOk && result.status != ApplyStatus::kAlreadyApplied) return result;
    return reserveLeaseBatch(requests);
}

bool MetadataService::heartbeat(const NodeHeartbeat& heartbeat, std::string* error)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if(status_.role != "leader" || !status_.leaderResolved) {
        if(error) *error = "not metadata leader";
        return false;
    }
    const auto durable = state_.node(heartbeat.nodeId);
    if(!durable) { if(error) *error = "node is not registered"; return false; }
    if(!softState_.update(*durable, heartbeat)) { if(error) *error = "stale node epoch or heartbeat timestamp"; return false; }
    // Heartbeats are soft state, but the transition into ONLINE is durable.
    // Do it only once per incarnation; steady-state heartbeats never append
    // Raft/metadata records.
    if(durable->health != NodeHealth::kOnline) {
        MetadataCommand command;
        command.commandId = "node-online-" + durable->nodeId + "-" + std::to_string(durable->nodeEpoch);
        command.type = MetadataCommandType::kMarkNodeHealth;
        command.actorType = "metadata-heartbeat";
        command.actorId = durable->nodeId;
        command.nodeEpoch = durable->nodeEpoch;
        command.issuedAt = heartbeat.observedAtMs / 1000;
        command.payload = MarkNodeHealthPayload{durable->nodeId, NodeHealth::kOnline};
        const auto result = state_.apply(command, nextIndex_++, status_.term);
        std::string persistError;
        if((result.status != ApplyStatus::kOk && result.status != ApplyStatus::kAlreadyApplied) ||
           !persistLocked(&persistError)) {
            status_.quorumWritable = false;
            if(error) *error = persistError.empty() ? result.message : persistError;
            return false;
        }
    }
    return true;
}

ApplyResult MetadataService::reserveLease(const ReserveLeaseRequest& request)
{
    const auto placementStarted = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    MetadataCommand command; command.commandId = request.commandId; command.type = MetadataCommandType::kReserveLease;
    command.actorType = request.actorType; command.actorId = request.actorId; command.generation = request.generation;
    if(status_.role != "leader" || !status_.leaderResolved || !status_.quorumWritable)
        return unavailable(command, "metadata leader or quorum is unavailable");

    struct Candidate { NodeRecord durable; NodeHeartbeat soft; uint64_t available = 0; };
    std::vector<Candidate> candidates;
    for(const auto& node : state_.nodes()) {
        const auto hb = softState_.heartbeat(node.nodeId);
        if(node.health != NodeHealth::kOnline || node.draining || !hb
           || !softState_.isFresh(node.nodeId, request.nowMs, heartbeatFreshnessMs_) || hb->nodeEpoch != node.nodeEpoch) continue;
        const uint64_t reported = std::min(node.registeredCapacityBytes, hb->freeBytes);
        const uint64_t available = reported > node.reservedBytes ? reported - node.reservedBytes : 0;
        if(available >= request.chunkSize) candidates.push_back({node, *hb, available});
    }
    const auto placementElapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - placementStarted).count());
    {
        std::lock_guard<std::mutex> metricsLock(metricsMutex_);
        ++metrics_.placementCount; metrics_.placementTotalUs += placementElapsed;
    }
    std::sort(candidates.begin(), candidates.end(), [](const Candidate& lhs, const Candidate& rhs) {
        if(lhs.soft.activeUploads != rhs.soft.activeUploads) return lhs.soft.activeUploads < rhs.soft.activeUploads;
        if(lhs.soft.queueDepth != rhs.soft.queueDepth) return lhs.soft.queueDepth < rhs.soft.queueDepth;
        if(lhs.available != rhs.available) return lhs.available > rhs.available;
        return lhs.durable.nodeId < rhs.durable.nodeId;
    });
    if(request.desiredRf == 0 || candidates.size() < request.desiredRf)
        return unavailable(command, "insufficient fresh eligible DataNodes for requested RF");

    command.placementEpoch = state_.placementEpoch();
    ReserveLeasePayload payload; payload.leaseId = request.leaseId; payload.requestKey = request.requestKey;
    payload.sessionId = request.sessionId; payload.chunkIndex = request.chunkIndex; payload.routeKey = request.routeKey;
    payload.chunkSize = request.chunkSize; payload.expiresAt = request.expiresAt;
    for(uint32_t i = 0; i < request.desiredRf; ++i) payload.targets.push_back({candidates[i].durable.nodeId, candidates[i].durable.nodeEpoch});
    command.payload = std::move(payload);
    ApplyResult result = state_.apply(command, nextIndex_++, status_.term);
    std::string error;
    if(!persistLocked(&error)) { status_.quorumWritable = false; return unavailable(command, "metadata persistence failed: " + error); }
    return result;
}

ApplyResult MetadataService::reserveLeaseBatch(const std::vector<ReserveLeaseRequest>& requests)
{
    if(requests.empty()) {
        ApplyResult result; result.status = ApplyStatus::kInvalid; result.message = "empty lease batch"; return result;
    }
    ApplyResult result;
    for(const auto& request : requests) {
        result = reserveLease(request);
        if(result.status != ApplyStatus::kOk && result.status != ApplyStatus::kAlreadyApplied) break;
    }
    return result;
}

ApplyResult MetadataService::commitChunk(const std::string& sessionId,
                                         uint32_t index,
                                         uint64_t size,
                                         const std::vector<std::string>& successfulNodes,
                                         const std::string& leaseId)
{
    MetadataCommand command;
    command.commandId = "commit-chunk-" + sessionId + "-" + std::to_string(index);
    command.type = MetadataCommandType::kCommitChunk;
    command.actorType = "datanode";
    command.actorId = successfulNodes.empty() ? std::string{} : successfulNodes.front();
    command.payload = CommitChunkPayload{};

    std::lock_guard<std::mutex> lock(mutex_);
    if(sessionId.empty() || leaseId.empty() || successfulNodes.empty() || size == 0) {
        return unavailable(command, "invalid chunk commit request");
    }
    const auto sessionRecord = state_.session(sessionId);
    const auto leaseRecord = state_.lease(leaseId);
    if(!sessionRecord || !leaseRecord || leaseRecord->state != LeaseState::kActive) {
        return unavailable(command, "session or active lease not found");
    }
    const auto chunkRecord = state_.chunk(sessionRecord->objectId, index);
    if(!chunkRecord) return unavailable(command, "chunk route not found");
    command.commandId += "-" + std::to_string(chunkRecord->generation);
    command.generation = chunkRecord->generation;
    auto& payload = std::get<CommitChunkPayload>(command.payload);
    payload.sessionId = sessionId;
    payload.leaseId = leaseId;
    payload.chunkIndex = index;
    payload.routeKey = chunkRecord->routeKey;
    payload.chunkSize = size;
    payload.checksumType = chunkRecord->checksumType;
    payload.checksumDigest = chunkRecord->checksumDigest;
    const int64_t verifiedAt = static_cast<int64_t>(std::time(nullptr));
    for(const auto& nodeId : successfulNodes) {
        const auto target = std::find_if(leaseRecord->targets.begin(), leaseRecord->targets.end(),
            [&](const LeaseTarget& item) { return item.nodeId == nodeId; });
        if(target == leaseRecord->targets.end()) return unavailable(command, "successful node is not in lease target set");
        payload.replicas.push_back({nodeId, target->nodeEpoch, chunkRecord->checksumDigest, verifiedAt});
    }

    std::vector<MetadataCommand> commands;
    commands.push_back(command);
    if(sessionRecord->totalChunks == 1) {
        const auto objectRecord = state_.object(sessionRecord->objectId);
        if(!objectRecord) return unavailable(command, "object record not found");
        MetadataCommand fileCommand;
        fileCommand.commandId = "commit-file-" + sessionId;
        fileCommand.type = MetadataCommandType::kCommitFile;
        fileCommand.actorType = "datanode";
        fileCommand.actorId = command.actorId;
        fileCommand.payload = CommitFilePayload{sessionId, sessionRecord->objectId,
                                                sessionRecord->objectVersion,
                                                objectRecord->contentHash};
        commands.push_back(std::move(fileCommand));
    }

    if(status_.role != "leader" || !status_.leaderResolved || !status_.quorumWritable)
        return unavailable(command, "metadata leader or quorum is unavailable");
    ApplyResult result;
    for(const auto& item : commands) {
        result = state_.apply(item, nextIndex_++, status_.term);
        if(result.status != ApplyStatus::kOk && result.status != ApplyStatus::kAlreadyApplied) return result;
    }
    std::string error;
    if(!persistLocked(&error)) {
        status_.quorumWritable = false;
        return unavailable(command, "metadata snapshot persistence failed: " + error);
    }
    return result;
}

std::optional<ReadFence> MetadataService::linearizableReadBarrier(const std::string& commandId)
{
    MetadataCommand command; command.commandId = commandId; command.type = MetadataCommandType::kReadBarrier;
    command.actorType = "metadata-service"; command.actorId = status().memberId; command.payload = ReadBarrierPayload{};
    const auto result = propose(command);
    if(result.status != ApplyStatus::kOk && result.status != ApplyStatus::kAlreadyApplied) return std::nullopt;
    return ReadFence{result.appliedTerm, result.appliedIndex, result.metadataVersion};
}

std::optional<UploadSessionRecord> MetadataService::session(const std::string& id) const
{ std::lock_guard<std::mutex> lock(mutex_); return state_.session(id); }
std::vector<UploadSessionRecord> MetadataService::sessions() const
{ std::lock_guard<std::mutex> lock(mutex_); return state_.sessions(); }
std::optional<LeaseRecord> MetadataService::lease(const std::string& id) const
{ std::lock_guard<std::mutex> lock(mutex_); return state_.lease(id); }
std::vector<NodeRecord> MetadataService::nodes() const
{ std::lock_guard<std::mutex> lock(mutex_); return state_.nodes(); }
std::optional<ObjectRecord> MetadataService::object(const std::string& id) const
{ std::lock_guard<std::mutex> lock(mutex_); return state_.object(id); }
std::optional<ChunkRouteRecord> MetadataService::chunk(const std::string& objectId, uint32_t index) const
{ std::lock_guard<std::mutex> lock(mutex_); return state_.chunk(objectId, index); }
std::optional<ReadDescriptor> MetadataService::readDescriptor(const std::string& id, uint64_t version) const
{ std::lock_guard<std::mutex> lock(mutex_); return state_.readDescriptor(id, version); }
std::vector<DirectoryRecord> MetadataService::directories(const std::string& ownerId, const std::string& parentPath) const
{ std::lock_guard<std::mutex> lock(mutex_); return state_.directories(ownerId, parentPath); }
std::vector<ObjectRecord> MetadataService::objects(const std::string& ownerId, const std::string& parentPath) const
{ std::lock_guard<std::mutex> lock(mutex_); return state_.objects(ownerId, parentPath); }
std::vector<DeleteTaskRecord> MetadataService::deleteTasks(const std::string& nodeId) const
{ std::lock_guard<std::mutex> lock(mutex_); return state_.deleteTasks(nodeId); }
ConsensusStatus MetadataService::status() const
{ std::lock_guard<std::mutex> lock(mutex_); return status_; }
MetadataMetricsSnapshot MetadataService::metrics() const
{ std::lock_guard<std::mutex> lock(metricsMutex_); return metrics_; }
std::string MetadataService::stateDigest() const
{ std::lock_guard<std::mutex> lock(mutex_); return state_.stateDigest(); }

void MetadataService::setConsensusStatus(ConsensusStatus status, int64_t nowMs)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const bool newTerm = status.role == "leader" && (status_.role != "leader" || status.term != status_.term);
    status_ = std::move(status);
    if(newTerm) softState_.beginLeaderTerm(status_.term, nowMs);
}

} // namespace miniKV::metadata
