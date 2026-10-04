#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace miniKV::metadata {

constexpr uint32_t kMetadataSchemaVersion = 2;

enum class ObjectState : uint8_t { kUploading, kCommitted, kFailed, kDeleting };
enum class ChunkState : uint8_t { kAllocated, kCommitted, kFailed };
enum class ReplicaState : uint8_t { kWriting, kHealthy, kSuspect, kOffline };
enum class NodeHealth : uint8_t { kJoining, kRecovering, kOnline, kSuspect, kOffline };
enum class LeaseState : uint8_t { kActive, kCommitted, kReleased, kExpired };
enum class IdentityScheme : uint8_t { kOpaque, kContentHash };
enum class ChecksumType : uint8_t { kNone, kCrc32c, kSha256 };

enum class ApplyStatus : uint8_t {
    kOk,
    kAlreadyApplied,
    kInvalid,
    kNotFound,
    kConflict,
    kFenced,
    kUnsupported,
    kCommandIdReuseMismatch,
    kUnavailable,
};

struct ObjectRecord {
    std::string objectId;
    uint64_t objectVersion = 1;
    uint64_t metadataVersion = 0;
    std::string ownerId;
    std::string parentPath;
    std::string name;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    uint32_t desiredRf = 2;
    std::string contentHash;
    ObjectState state = ObjectState::kUploading;
};

struct UploadSessionRecord {
    std::string sessionId;
    std::string objectId;
    uint64_t objectVersion = 1;
    std::string ownerId;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    uint32_t totalChunks = 0;
    uint32_t completedChunks = 0;
    int64_t expiresAt = 0;
    bool expired = false;
};

// Namespace entries are metadata state, not a Gateway-local cache.  Keeping
// them in the replicated state machine makes directory creation/deletion
// deterministic across Gateway frontends.
struct DirectoryRecord {
    std::string ownerId;
    std::string path;
    int64_t createdAt = 0;
};

struct ReplicaRecord {
    std::string nodeId;
    uint64_t nodeEpoch = 0;
    uint64_t generation = 0;
    ReplicaState state = ReplicaState::kWriting;
    std::string checksumDigest;
    int64_t verifiedAt = 0;
};

struct ChunkRouteRecord {
    std::string routeKey;
    std::string objectId;
    uint64_t objectVersion = 1;
    uint32_t index = 0;
    std::string storageIdentity;
    IdentityScheme identityScheme = IdentityScheme::kOpaque;
    ChecksumType checksumType = ChecksumType::kCrc32c;
    std::string checksumDigest;
    uint64_t size = 0;
    uint32_t desiredRf = 2;
    uint64_t generation = 1;
    ChunkState state = ChunkState::kAllocated;
    std::vector<ReplicaRecord> replicas;
};

using ChunkRecord = ChunkRouteRecord;

struct LeaseTarget {
    std::string nodeId;
    uint64_t nodeEpoch = 0;
};

struct DeleteTaskRecord {
    std::string objectId;
    uint64_t objectVersion = 1;
    uint32_t chunkIndex = 0;
    std::string storageIdentity;
    std::vector<LeaseTarget> pendingReplicas;
};

struct LeaseRecord {
    std::string leaseId;
    std::string requestKey;
    std::string sessionId;
    uint32_t chunkIndex = 0;
    std::string routeKey;
    uint64_t chunkSize = 0;
    uint64_t placementEpoch = 0;
    uint64_t generation = 1;
    std::vector<LeaseTarget> targets;
    int64_t expiresAt = 0;
    LeaseState state = LeaseState::kActive;
};

struct NodeRecord {
    std::string nodeId;
    std::string bootId;
    uint64_t nodeEpoch = 0;
    std::string address;
    uint16_t dataPort = 0;
    uint64_t registeredCapacityBytes = 0;
    std::vector<std::string> capabilities;
    NodeHealth health = NodeHealth::kJoining;
    bool draining = false;
    uint64_t placementEpoch = 0;
    uint64_t reservedBytes = 0;
    uint32_t reservedWrites = 0;
};

struct ApplyResult {
    std::string commandId;
    ApplyStatus status = ApplyStatus::kInvalid;
    uint64_t metadataVersion = 0;
    uint64_t appliedIndex = 0;
    uint64_t appliedTerm = 0;
    uint64_t nodeEpoch = 0;
    uint64_t placementEpoch = 0;
    std::string objectId;
    uint64_t objectVersion = 0;
    std::string sessionId;
    std::string leaseId;
    std::string message;
};

struct DedupEntry {
    std::string commandId;
    uint8_t commandType = 0;
    std::string requestFingerprint;
    ApplyResult result;
    uint64_t appliedIndex = 0;
};

struct ReadDescriptor {
    ObjectRecord object;
    std::vector<ChunkRouteRecord> chunks;
};

struct MetadataSnapshot {
    uint32_t schemaVersion = kMetadataSchemaVersion;
    uint64_t lastAppliedIndex = 0;
    uint64_t lastAppliedTerm = 0;
    std::string bytes;
};

const char* toString(ApplyStatus status);
const char* toString(ChecksumType type);

} // namespace miniKV::metadata
