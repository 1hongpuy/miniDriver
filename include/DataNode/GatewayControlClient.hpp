#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace miniKV::datanode {

struct RpcResult {
    bool ok = false;
    int httpStatus = 0;
    std::string error;
};

using RpcCallback = std::function<void(RpcResult)>;

struct NodeRegistration {
    std::string nodeId;
    std::string address;
    uint16_t httpPort = 0;
    uint64_t maxStorageBytes = 0;
    uint64_t reservedBytes = 0;
    uint32_t maxConcurrentWrites = 2;
};

struct NodeHeartbeat {
    std::string nodeId;
    uint64_t usedBytes = 0;
    uint64_t freeBytes = 0;
    uint32_t cpuPermille = 0;
    uint32_t memoryPermille = 0;
    uint32_t diskIoPermille = 0;
    uint32_t netOutMbps = 0;
    uint32_t activeUploads = 0;
};

struct ChunkCommit {
    std::string sessionId;
    uint32_t chunkIndex = 0;
    std::string chunkHash;
    uint64_t size = 0;
    std::vector<std::string> successfulNodes;
    std::string uploadToken;
};

struct LeaseRelease {
    std::string uploadToken;
};

class GatewayControlClient {
public:
    virtual ~GatewayControlClient() = default;

    virtual void registerStorageNode(const NodeRegistration& request,
                                     RpcCallback callback) = 0;
    virtual void sendHeartbeat(const NodeHeartbeat& request,
                               RpcCallback callback) = 0;
    virtual void commitChunk(const ChunkCommit& request,
                             RpcCallback callback) = 0;
    virtual void releaseLease(const LeaseRelease& request,
                              RpcCallback callback) = 0;
};

}  // namespace miniKV::v2
