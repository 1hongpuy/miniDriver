#pragma once

#include "DataNode/ControlRequestCodec.hpp"

#include <memory>
#include <string>

namespace miniKV::network {
class EventLoop;
}

namespace miniKV::datanode {

class IControlRequestSender {
public:
    virtual ~IControlRequestSender() = default;
    virtual void send(const EncodedControlRequest& request,
                      const std::string& internalToken,
                      RpcCallback callback) = 0;
};

class HttpGatewayControlClient final : public GatewayControlClient {
public:
    HttpGatewayControlClient(network::EventLoop* loop, std::string gatewayAddress,
                             uint16_t gatewayPort, std::string internalToken);
    HttpGatewayControlClient(std::shared_ptr<IControlRequestSender> sender,
                             std::string internalToken);

    void registerStorageNode(const NodeRegistration& request,
                             RpcCallback callback) override;
    void sendHeartbeat(const NodeHeartbeat& request,
                       RpcCallback callback) override;
    void commitChunk(const ChunkCommit& request,
                     RpcCallback callback) override;

private:
    std::shared_ptr<IControlRequestSender> sender_;
    std::string internalToken_;
};

}  // namespace miniKV::v2
