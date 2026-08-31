#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace miniKV::metadata {

enum class ObjectState : uint8_t {
    kUploading,
    kProtecting,
    kCommitted,
    kDegraded,
    kFailed,
};

enum class ChunkState : uint8_t {
    kAllocated,
    kWriting,
    kCommitted,
    kRepairing,
    kFailed,
};

enum class ReplicaState : uint8_t {
    kWriting,
    kHealthy,
    kSuspect,
    kOffline,
    kRepairing,
};

enum class NodeHealth : uint8_t {
    kJoining,
    kOnline,
    kSuspect,
    kOffline,
    kRecovering,
};

enum class RepairState : uint8_t {
    kPending,
    kRunning,
    kSucceeded,
    kRetryWait,
    kBlocked,
    kFailed,
};

enum class ApplyStatus : uint8_t {
    kOk,
    kAlreadyApplied,
    kInvalid,
    kNotFound,
    kConflict,
    kFenced,
    kUnsupported,
};

struct ObjectRecord {
    std::string objectId;
    std::string ownerId;
    std::string parentPath;
    std::string name;
    std::string fileHash;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    uint32_t desiredRf = 2;
    ObjectState state = ObjectState::kUploading;
    uint64_t metadataVersion = 0;
};

struct UploadSessionRecord {
    std::string sessionId;
    std::string objectId;
    std::string ownerId;
    std::string manifestHash;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    uint32_t totalChunks = 0;
    int64_t expiresAt = 0;
};

struct ReplicaRecord {
    std::string nodeId;
    uint64_t nodeEpoch = 0;
    uint64_t generation = 0;
    ReplicaState state = ReplicaState::kWriting;
    std::string verifiedHash;
    int64_t verifiedAt = 0;
};

struct ChunkRecord {
    std::string objectId;
    uint32_t index = 0;
    std::string chunkHash;
    uint64_t size = 0;
    uint32_t desiredRf = 2;
    ChunkState state = ChunkState::kAllocated;
    uint64_t generation = 0;
    std::vector<ReplicaRecord> replicas;
};

struct LeaseRecord {
    std::string leaseId;
    std::string requestKey;
    std::string sessionId;
    uint32_t chunkIndex = 0;
    std::string chunkHash;
    uint64_t chunkSize = 0;
    uint64_t placementEpoch = 0;
    std::vector<std::string> targetNodeIds;
    int64_t expiresAt = 0;
};

struct NodeRecord {
    std::string nodeId;
    std::string bootId;
    uint64_t nodeEpoch = 0;
    std::string address;
    uint16_t dataPort = 0;
    NodeHealth health = NodeHealth::kJoining;
    uint64_t freeBytes = 0;
    uint32_t activeUploads = 0;
    uint32_t activeDownloads = 0;
    uint32_t diskQueueDepth = 0;
    uint64_t diskPauseMs = 0;
    uint64_t eventLoopLagUs = 0;
    uint64_t placementEpoch = 0;
    int64_t lastHeartbeatAt = 0;
};

struct RepairTask {
    std::string taskKey;
    std::string objectId;
    uint32_t chunkIndex = 0;
    std::string chunkHash;
    std::string sourceNodeId;
    std::string targetNodeId;
    uint64_t expectedGeneration = 0;
    RepairState state = RepairState::kPending;
    uint32_t attempts = 0;
    int64_t nextRetryAt = 0;
    std::string lastError;
};

struct ApplyResult {
    std::string commandId;
    ApplyStatus status = ApplyStatus::kInvalid;
    uint64_t metadataVersion = 0;
    uint64_t appliedIndex = 0;
    uint64_t nodeEpoch = 0;
    uint64_t placementEpoch = 0;
    std::string objectId;
    std::string sessionId;
    std::string leaseId;
    std::string message;
};

struct MetadataSnapshot {
    uint32_t schemaVersion = 1;
    uint64_t lastAppliedIndex = 0;
    std::string bytes;
};

const char* toString(ApplyStatus status);

} // namespace miniKV::metadata
