#pragma once

#include "DataNode/GatewayControlClient.hpp"
#include "metadata/MetadataClient.hpp"

#include <atomic>
#include <memory>
#include <string>

namespace miniKV::network { class EventLoop; }

namespace miniKV::datanode {

// Direct metadata control transport used by the 4.0-HA deployment.  The
// metadata client is synchronous by design, so calls are executed off the
// DataNode event loop and their small result is posted back to that loop.
// Object bytes never use this path; only registration, soft heartbeats and
// durable chunk commit/release control messages do.
class MetadataControlClient final : public GatewayControlClient {
public:
    MetadataControlClient(network::EventLoop* loop,
                          std::shared_ptr<metadata::MetadataClient> client,
                          std::string nodeId,
                          std::string bootId);

    void registerStorageNode(const NodeRegistration& request,
                             RpcCallback callback) override;
    void sendHeartbeat(const NodeHeartbeat& request,
                       RpcCallback callback) override;
    void commitChunk(const ChunkCommit& request,
                     RpcCallback callback) override;
    void releaseLease(const LeaseRelease& request,
                      RpcCallback callback) override;

private:
    void post(RpcResult result, RpcCallback callback) const;

    network::EventLoop* loop_ = nullptr;
    std::shared_ptr<metadata::MetadataClient> client_;
    std::string nodeId_;
    std::string bootId_;
    std::atomic<uint64_t> nodeEpoch_{0};
};

}  // namespace miniKV::datanode
