#pragma once

#include "metadata/LeaderSoftState.hpp"
#include "metadata/MetadataStateMachine.hpp"
#include "metadata/SnapshotStore.hpp"

#include <memory>
#include <mutex>
#include <optional>
#include <string>

namespace miniKV::metadata {

struct ConsensusStatus {
    std::string role = "leader";
    uint64_t term = 1;
    std::string memberId = "meta-1";
    std::string leaderId = "meta-1";
    bool leaderResolved = true;
    bool quorumWritable = true;
    uint64_t commitIndex = 0;
    uint64_t lastApplied = 0;
};

struct ReadFence {
    uint64_t term = 0;
    uint64_t readIndex = 0;
    uint64_t metadataVersion = 0;
};

// Process-local cumulative control-plane timings.  These are observability
// counters only; they are never consulted by the replicated state machine and
// therefore cannot affect a command's deterministic result.
struct MetadataMetricsSnapshot {
    uint64_t proposalCount = 0;
    uint64_t proposalFailureCount = 0;
    uint64_t proposalTotalUs = 0;
    uint64_t raftAppendTotalUs = 0;
    uint64_t raftAppendCount = 0;
    uint64_t raftLogSyncCount = 0;
    uint64_t raftLogSyncFailureCount = 0;
    uint64_t raftLogSyncTotalUs = 0;
    uint64_t raftLogSyncMinUs = 0;
    uint64_t raftLogSyncMaxUs = 0;
    uint64_t raftLogWritevCount = 0;
    uint64_t raftLogWritevRecords = 0;
    uint64_t raftLogWritevBytes = 0;
    uint64_t raftLogWritevTotalUs = 0;
    uint64_t raftLogWritevMinUs = 0;
    uint64_t raftLogWritevMaxUs = 0;
    uint64_t quorumWaitCount = 0;
    uint64_t quorumWaitTotalUs = 0;
    uint64_t stateMachineDecodeTotalUs = 0;
    uint64_t placementCount = 0;
    uint64_t placementTotalUs = 0;
    uint64_t readIndexCount = 0;
    uint64_t readIndexFailureCount = 0;
    uint64_t readIndexTotalUs = 0;
};

struct ReserveLeaseRequest {
    std::string commandId;
    std::string actorType;
    std::string actorId;
    std::string leaseId;
    std::string requestKey;
    std::string sessionId;
    uint32_t chunkIndex = 0;
    std::string routeKey;
    uint64_t chunkSize = 0;
    uint64_t generation = 0;
    uint32_t desiredRf = 2;
    int64_t expiresAt = 0;
    int64_t nowMs = 0;
};

class MetadataService {
public:
    explicit MetadataService(std::string snapshotDirectory = {});

    ApplyResult propose(const MetadataCommand& command);
    ApplyResult proposeBatch(const std::vector<MetadataCommand>& commands);
    ApplyResult createSessionAndReserve(const MetadataCommand& create,
                                        const std::vector<ReserveLeaseRequest>& requests);
    ApplyResult reserveLease(const ReserveLeaseRequest& request);
    ApplyResult reserveLeaseBatch(const std::vector<ReserveLeaseRequest>& requests);
    ApplyResult commitChunk(const std::string& sessionId,
                            uint32_t index,
                            uint64_t size,
                            const std::vector<std::string>& successfulNodes,
                            const std::string& leaseId);
    bool heartbeat(const NodeHeartbeat& heartbeat, std::string* error = nullptr);
    std::optional<ReadFence> linearizableReadBarrier(const std::string& commandId);

    std::optional<UploadSessionRecord> session(const std::string& sessionId) const;
    std::vector<UploadSessionRecord> sessions() const;
    std::optional<LeaseRecord> lease(const std::string& leaseId) const;
    std::vector<NodeRecord> nodes() const;
    std::optional<ObjectRecord> object(const std::string& objectId) const;
    std::optional<ChunkRouteRecord> chunk(const std::string& objectId, uint32_t index) const;
    std::optional<ReadDescriptor> readDescriptor(const std::string& objectId,
                                                 uint64_t objectVersion) const;
    std::vector<DirectoryRecord> directories(const std::string& ownerId,
                                             const std::string& parentPath) const;
    std::vector<ObjectRecord> objects(const std::string& ownerId,
                                      const std::string& parentPath) const;
    std::vector<DeleteTaskRecord> deleteTasks(const std::string& nodeId) const;
    ConsensusStatus status() const;
    MetadataMetricsSnapshot metrics() const;
    void setConsensusStatus(ConsensusStatus status, int64_t nowMs);
    std::string stateDigest() const;

private:
    ApplyResult unavailable(const MetadataCommand& command, const std::string& message) const;
    bool persistLocked(std::string* error);

    mutable std::mutex mutex_;
    MetadataStateMachine state_;
    LeaderSoftState softState_;
    ConsensusStatus status_;
    std::unique_ptr<SnapshotStore> snapshotStore_;
    uint64_t nextIndex_ = 1;
    int64_t heartbeatFreshnessMs_ = 6000;
    mutable std::mutex metricsMutex_;
    MetadataMetricsSnapshot metrics_;
};

} // namespace miniKV::metadata
