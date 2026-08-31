#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace miniKV::control {

// Protocol-neutral read contract. Gateway adapters may encode this as JSON,
// while DataNode transports only consume the per-Chunk identity and token.
struct ReplicaReadTarget {
    std::string nodeId;
    std::string address;
    uint16_t port = 0;
};

struct ChunkReadDescriptor {
    uint32_t index = 0;
    std::string chunkId;
    std::string storageIdentity;
    uint64_t size = 0;
    std::string checksumType;
    std::string checksumDigest;
    uint64_t generation = 0;
    std::string readCapability;
    std::vector<ReplicaReadTarget> replicas;
};

struct ObjectReadDescriptor {
    std::string objectId;
    uint64_t objectVersion = 0;
    uint64_t metadataVersion = 0;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    std::vector<ChunkReadDescriptor> chunks;
};

}  // namespace miniKV::control
