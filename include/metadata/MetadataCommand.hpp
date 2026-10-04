#pragma once

#include "metadata/MetadataTypes.hpp"

#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace miniKV::metadata {

enum class MetadataCommandType : uint8_t {
    kCreateSession = 1,
    kReserveLease = 2,
    kReleaseLease = 3,
    kExpireLease = 4,
    kCommitChunk = 5,
    kCommitFile = 6,
    kRegisterNode = 7,
    kMarkNodeHealth = 8,
    kSetNodeDraining = 9,
    kReadBarrier = 10,
    kCreateDirectory = 11,
    kDeleteObject = 12,
    kDeleteDirectory = 13,
    kExpireSession = 14,
    kAcknowledgeDelete = 15,
};

struct InitialChunk {
    uint32_t index = 0;
    std::string routeKey;
    std::string storageIdentity;
    IdentityScheme identityScheme = IdentityScheme::kOpaque;
    ChecksumType checksumType = ChecksumType::kCrc32c;
    std::string checksumDigest;
    uint64_t size = 0;
    uint64_t generation = 1;
};

struct CreateSessionPayload {
    std::string sessionId;
    std::string objectId;
    uint64_t objectVersion = 1;
    std::string ownerId = "admin";
    std::string parentPath;
    std::string name;
    std::string contentHash;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 4U * 1024U * 1024U;
    uint32_t desiredRf = 2;
    int64_t expiresAt = 0;
    std::vector<InitialChunk> chunks;
};

struct RegisterNodePayload {
    std::string nodeId;
    std::string bootId;
    std::string address;
    uint16_t dataPort = 0;
    uint64_t registeredCapacityBytes = 0;
    std::vector<std::string> capabilities;
};

struct MarkNodeHealthPayload {
    std::string nodeId;
    NodeHealth health = NodeHealth::kJoining;
    int64_t observedAt = 0;
};

struct SetNodeDrainingPayload {
    std::string nodeId;
    bool draining = false;
};

struct ReserveLeasePayload {
    std::string leaseId;
    std::string requestKey;
    std::string sessionId;
    uint32_t chunkIndex = 0;
    std::string routeKey;
    uint64_t chunkSize = 0;
    std::vector<LeaseTarget> targets;
    int64_t expiresAt = 0;
};

struct ReleaseLeasePayload {
    std::string leaseId;
    uint64_t expectedGeneration = 0;
};

struct ExpireLeasePayload {
    std::string leaseId;
    uint64_t expectedGeneration = 0;
    int64_t expectedExpiresAt = 0;
    int64_t observedAt = 0;
};

struct ReplicaCommit {
    std::string nodeId;
    uint64_t nodeEpoch = 0;
    std::string checksumDigest;
    int64_t verifiedAt = 0;
};

struct CommitChunkPayload {
    std::string sessionId;
    std::string leaseId;
    uint32_t chunkIndex = 0;
    std::string routeKey;
    uint64_t chunkSize = 0;
    ChecksumType checksumType = ChecksumType::kCrc32c;
    std::string checksumDigest;
    std::vector<ReplicaCommit> replicas;
};

struct CommitFilePayload {
    std::string sessionId;
    std::string objectId;
    uint64_t objectVersion = 1;
    std::string contentHash;
};

struct CreateDirectoryPayload {
    std::string ownerId;
    std::string path;
    int64_t createdAt = 0;
};

struct DeleteObjectPayload {
    std::string objectId;
    uint64_t objectVersion = 0;
};

struct DeleteDirectoryPayload {
    std::string ownerId;
    std::string path;
};

struct ExpireSessionPayload {
    std::string sessionId;
    int64_t expectedExpiresAt = 0;
    int64_t observedAt = 0;
};

struct AcknowledgeDeletePayload {
    std::string objectId;
    uint64_t objectVersion = 0;
    uint32_t chunkIndex = 0;
    std::string nodeId;
    uint64_t nodeEpoch = 0;
};

struct ReadBarrierPayload { uint64_t nonce = 0; };

using MetadataPayload = std::variant<CreateSessionPayload,
                                     RegisterNodePayload,
                                     MarkNodeHealthPayload,
                                     SetNodeDrainingPayload,
                                     ReserveLeasePayload,
                                     ReleaseLeasePayload,
                                     ExpireLeasePayload,
                                     CommitChunkPayload,
                                     CommitFilePayload,
                                     CreateDirectoryPayload,
                                     DeleteObjectPayload,
                                     DeleteDirectoryPayload,
                                     ExpireSessionPayload,
                                     AcknowledgeDeletePayload,
                                     ReadBarrierPayload>;

struct MetadataCommand {
    uint32_t schemaVersion = kMetadataSchemaVersion;
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
// Binary envelope used by the metadata HTTP service to append several
// independently-idempotent commands in one Raft append batch.  Each command
// is still applied separately by the replicated state machine; batching only
// changes the durable-log/fsync boundary.
std::optional<std::string> encodeMetadataCommandBatch(const std::vector<MetadataCommand>& commands);
std::optional<std::vector<MetadataCommand>> decodeMetadataCommandBatch(const std::string& bytes);
std::optional<MetadataCommand> decodeMetadataCommand(const std::string& bytes);
std::optional<std::string> canonicalCommandBytes(const MetadataCommand& command);
std::optional<std::string> commandFingerprint(const MetadataCommand& command);

} // namespace miniKV::metadata
