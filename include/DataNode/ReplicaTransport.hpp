#pragma once

#include "DataNode/ChunkWriteTypes.hpp"
#include "DataNode/DiskWriteExecutor.hpp"

#include <functional>
#include <memory>
#include <string>

namespace miniKV::network {
class EventLoop;
}

namespace miniKV::datanode {

class ReplicaConnectionPool;

struct ReplicaWriteRequest {
    ChunkWriteDescriptor descriptor;
    ReplicaTarget target;
    std::string commitOwnerNodeId;
    std::string gatewayAddress;
    uint16_t gatewayPort = 0;
};

struct ReplicaTransportResult {
    int status = 0;
    std::string body;
    std::string error;
};

struct ReplicaTransportMetrics {
    uint64_t pauseCount = 0;
    uint64_t pauseNanoseconds = 0;
    size_t maxPendingBytes = 0;
};

class ReplicaWriteStream {
public:
    virtual ~ReplicaWriteStream() = default;
    virtual StreamConsumeResult pushShared(
        DiskWriteExecutor::SharedBlockPtr block, size_t size) = 0;
    virtual void finish() = 0;
    virtual void abort() = 0;
    virtual ReplicaTransportMetrics metrics() const = 0;
};

class ReplicaTransport {
public:
    using CompletionCallback = std::function<void(ReplicaTransportResult)>;
    virtual ~ReplicaTransport() = default;
    virtual std::shared_ptr<ReplicaWriteStream> open(
        ReplicaWriteRequest request, std::function<void()> resumeUpstream,
        CompletionCallback completion) = 0;
};

// The HTTP/1.1 chain remains the current transport implementation.  Its URL,
// headers and connection-pool details are intentionally confined here.
class HttpReplicaTransport final : public ReplicaTransport {
public:
    HttpReplicaTransport(network::EventLoop* loop,
                         std::shared_ptr<ReplicaConnectionPool> connectionPool);

    std::shared_ptr<ReplicaWriteStream> open(
        ReplicaWriteRequest request, std::function<void()> resumeUpstream,
        CompletionCallback completion) override;

private:
    network::EventLoop* loop_;
    std::shared_ptr<ReplicaConnectionPool> connectionPool_;
};

}  // namespace miniKV::datanode
