#pragma once

#include "metadata/MetadataTypes.hpp"

#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace miniKV::metadata {

enum class MetadataCommandType : uint8_t {
    kCreateSession,
    kReserveLease,
    kReleaseLease,
    kCommitChunk,
    kCommitFile,
    kRegisterNode,
    kHeartbeatNode,
    kMarkNodeHealth,
    kCreateRepairTask,
    kStartRepair,
    kFinishRepair,
    kFailRepair,
};

struct InitialChunk {
    uint32_t index = 0;
    std::string chunkHash;
    uint64_t size = 0;
};

struct CreateSessionPayload {
    std::string sessionId;
    std::string objectId;
    std::string ownerId;
    std::string parentPath;
    std::string name;
    std::string fileHash;
    std::string manifestHash;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    uint32_t desiredRf = 2;
    int64_t expiresAt = 0;
    std::vector<InitialChunk> chunks;
};

struct RegisterNodePayload {
    std::string nodeId;
    std::string bootId;
    std::string address;
    uint16_t dataPort = 0;
    int64_t observedAt = 0;
};

struct HeartbeatNodePayload {
    std::string nodeId;
    uint64_t freeBytes = 0;
    uint32_t activeUploads = 0;
    uint32_t activeDownloads = 0;
    uint32_t diskQueueDepth = 0;
    uint64_t diskPauseMs = 0;
    uint64_t eventLoopLagUs = 0;
    int64_t observedAt = 0;
};

struct MarkNodeHealthPayload {
    std::string nodeId;
    NodeHealth health = NodeHealth::kJoining;
    int64_t observedAt = 0;
};

struct ReserveLeasePayload {
    std::string leaseId;
    std::string requestKey;
    std::string sessionId;
    uint32_t chunkIndex = 0;
    std::string chunkHash;
    uint64_t chunkSize = 0;
    std::vector<std::string> targetNodeIds;
    int64_t expiresAt = 0;
};

struct ReplicaCommit {
    std::string nodeId;
    uint64_t nodeEpoch = 0;
    std::string verifiedHash;
    int64_t verifiedAt = 0;
};

struct CommitChunkPayload {
    std::string sessionId;
    std::string leaseId;
    uint32_t chunkIndex = 0;
    std::string chunkHash;
    uint64_t chunkSize = 0;
    std::vector<ReplicaCommit> replicas;
};

struct CommitFilePayload {
    std::string sessionId;
    std::string objectId;
    std::string fileHash;
};

using MetadataPayload = std::variant<CreateSessionPayload,
                                     RegisterNodePayload,
                                     HeartbeatNodePayload,
                                     MarkNodeHealthPayload,
                                     ReserveLeasePayload,
                                     CommitChunkPayload,
                                     CommitFilePayload>;

struct MetadataCommand {
    uint32_t schemaVersion = 1;
    std::string commandId;
    MetadataCommandType type = MetadataCommandType::kCreateSession;
    std::string actorType;
    std::string actorId;
    uint64_t expectedMetadataVersion = 0;
    uint64_t placementEpoch = 0;
    uint64_t nodeEpoch = 0;
    uint64_t generation = 0;
    int64_t issuedAt = 0;
    MetadataPayload payload;
};

std::optional<std::string> encodeMetadataCommand(const MetadataCommand& command);
std::optional<MetadataCommand> decodeMetadataCommand(const std::string& bytes);

} // namespace miniKV::metadata
