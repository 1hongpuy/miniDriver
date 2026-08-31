#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace miniKV::datanode {

enum class ChunkIdentityScheme {
    kCasSha256,
    kOpaqueChunkId
};

enum class ChunkChecksumType {
    kSha256,
    kCrc32c,
    kBlake3
};

// Transport-neutral backpressure result. Protocol adapters translate this to
// their own parser/stream control result (HTTP/1.1 currently maps it to
// HttpContext::BodyConsumeResult).
enum class StreamConsumeResult {
    kContinue,
    kPause,
    kPauseBeforeConsume,
    kAbort
};

struct ChunkChecksum {
    ChunkChecksumType type = ChunkChecksumType::kSha256;
    uint32_t segmentBytes = 0;
    std::string wholeDigest;
};

struct ReplicaTarget {
    std::string nodeId;
    std::string address;
    uint16_t port = 0;
};

// Protocol-independent description of one logical Chunk write. HTTP headers,
// URL layouts and response codes are decoded by an adapter before this value
// reaches the write coordinator.
struct ChunkWriteDescriptor {
    uint32_t schemaVersion = 1;
    ChunkIdentityScheme identityScheme = ChunkIdentityScheme::kCasSha256;
    std::string chunkId;
    std::string contentHash;          // Logical strong hash used by Gateway/manifest.
    std::string objectId;
    uint64_t objectVersion = 1;
    uint64_t generation = 0;
    std::string sessionId;
    uint32_t chunkIndex = 0;
    uint64_t contentLength = 0;
    ChunkChecksum checksum;
    std::vector<ReplicaTarget> replicaChain;
    size_t replicaPosition = 0;
    std::string capabilityId;
    std::string clientId;
    std::string requestId;

    // During the compatible CAS phase this remains the SHA-256 digest. The
    // opaque scheme will use chunkId after PhysicalStore dual-key support lands.
    std::string storageKey() const
    {
        return identityScheme == ChunkIdentityScheme::kOpaqueChunkId
            ? chunkId : checksum.wholeDigest;
    }
};

const char* chunkIdentitySchemeName(ChunkIdentityScheme scheme);
bool parseChunkIdentityScheme(const std::string& value, ChunkIdentityScheme& out);
const char* chunkChecksumTypeName(ChunkChecksumType type);
bool parseChunkChecksumType(const std::string& value, ChunkChecksumType& out);

bool parseReplicaTarget(const std::string& value, ReplicaTarget& out);
std::vector<ReplicaTarget> parseReplicaChain(const std::string& value);
std::string joinReplicaChain(const std::vector<ReplicaTarget>& chain);
bool sameReplicaChain(const std::vector<ReplicaTarget>& chain,
                      const std::vector<std::string>& expected);

}  // namespace miniKV::datanode
