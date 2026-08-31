#pragma once

#include "client/HttpTransport.hpp"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <vector>

namespace miniKV::client {

struct ObjectRef {
    std::string objectId;
    uint64_t objectVersion = 0;
};

struct ClientConfig {
    Endpoint gateway;
    std::string clusterInternalToken;
    std::string servicePrincipal;
    int gatewayTimeoutMs = 30000;
    int dataNodeTimeoutMs = 60000;
};

struct ReadOptions {
    bool keepAlive = true;
    bool verifyChecksum = true;
    bool allowReplicaRetry = true;
    uint32_t maxReplicaAttempts = 2;
    uint64_t maxReadBytesPerSecond = 0;
};

struct UploadOptions {
    // Number of logical storage Chunks that may be in flight from this SDK
    // instance. It is deliberately independent from TCP packet boundaries.
    uint32_t chunkWindow = 2;
    // The v3 default path is CRC32C transport/storage verification. In this
    // mode the V2 compatibility field named `hash` carries a stable upload
    // route key, not a content hash. SHA-256 remains available for legacy CAS
    // compatibility and explicit strong-content mode.
    std::string checksumType = "crc32c";
    // Optional process-wide admission hook. Benchmark supplies this so its
    // global chunk budget remains meaningful after moving upload logic into
    // the SDK; production callers can leave both empty.
    std::function<void()> acquireChunk;
    std::function<void()> releaseChunk;
};

struct UploadResult {
    ObjectRef object;
    std::string sessionId;
    std::string fileHash;  // Legacy lookup key while V2 compatibility remains.
    uint32_t chunkSize = 0;
    uint32_t chunkCount = 0;
};

struct ReplicaTarget {
    std::string nodeId;
    Endpoint endpoint;
};

struct ChunkReadPlan {
    uint32_t index = 0;
    std::string chunkId;
    std::string storageIdentity;
    uint64_t size = 0;
    std::string checksumType;
    std::string checksumDigest;
    std::string readCapability;
    std::vector<ReplicaTarget> replicas;
};

struct ObjectReadPlan {
    ObjectRef object;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    // True when this came from a v3 ReadPlan and every Chunk has a capability.
    bool capabilityBound = false;
    std::vector<ChunkReadPlan> chunks;
};

struct TransferStats {
    uint64_t dataConnectionOpens = 0;
    uint64_t dataRequests = 0;
    uint64_t dataConnectionReuses = 0;
    uint64_t replicaFallbacks = 0;
    uint64_t bytesVerified = 0;
};

enum class IntegrityStatus {
    kVerifiedWholeChunk,
    kUnverifiedPartialRange,
    kUnverifiedChecksumDisabled,
};

// Result returned by an object-level range read.  A range covering only
// complete chunks can be verified with the existing whole-chunk digest.  A
// partial chunk is intentionally reported as unverified until the storage
// protocol supplies segment checksum sidecars.
struct RangeReadResult {
    uint64_t bytesRead = 0;
    IntegrityStatus integrity = IntegrityStatus::kUnverifiedPartialRange;
};

// First SDK core: it gives all native callers one ReadPlan/manifest parser,
// bounded candidate retry policy, connection reuse and per-chunk verification.
// It never accesses DataNode extents and it never makes node-health decisions.
class MiniDriverClient {
public:
    explicit MiniDriverClient(ClientConfig config);

    bool getReadPlan(const ObjectRef& object, ObjectReadPlan& out, std::string& error) const;
    // Compatibility adapter for existing V2 callers until they migrate to
    // objectId + objectVersion ReadPlan.
    bool getLegacyManifest(const std::string& fileHash, ObjectReadPlan& out,
                           std::string& error) const;

    // Compatibility upload adapter. The sessionId is created once and each
    // retry reuses the same Chunk index/session; it never creates a second
    // logical Chunk merely because a transport request was retried.
    bool uploadFile(const std::filesystem::path& input, const std::string& fileName,
                    const std::string& dirPath, const UploadOptions& options,
                    UploadResult& out, std::string& error) const;

    bool downloadToFile(const ObjectReadPlan& plan, const std::filesystem::path& output,
                        const ReadOptions& options, TransferStats& stats,
                        std::string& error);

    // Reads and (by default) verifies every whole Chunk, then discards it.
    // This is intentionally a diagnostic API for separating client-side
    // object-file output/final hashing from the authorized DataNode path.
    // It is not a replacement for downloadToFile() when callers need a file.
    bool downloadToSink(const ObjectReadPlan& plan, const ReadOptions& options,
                        TransferStats& stats, std::string& error);

    // Reads [offset, offset + length) from the logical immutable object. It
    // maps the range to one or more DataNode Chunk GET/Range requests without
    // exposing extents to the caller. Partial chunks remain explicitly
    // unverified; callers must not use this result as a whole-object proof.
    bool downloadRangeToFile(const ObjectReadPlan& plan, uint64_t offset, uint64_t length,
                             const std::filesystem::path& output,
                             const ReadOptions& options, TransferStats& stats,
                             RangeReadResult& result, std::string& error);

    bool readWholeChunk(const ChunkReadPlan& chunk, std::string& out,
                        const ReadOptions& options, TransferStats& stats,
                        std::string& error);

private:
    bool readChunkRange(const ChunkReadPlan& chunk, uint64_t offset, uint64_t length,
                        std::string& out, const ReadOptions& options,
                        TransferStats& stats, IntegrityStatus& integrity,
                        std::string& error);

    ClientConfig config_;
    // One client instance is normally owned by one worker. It keeps one
    // sequential HTTP/1.1 connection and reconnects only when the selected
    // DataNode changes or the peer closes it.
    StreamingRequest dataRequest_;
};

}  // namespace miniKV::client
