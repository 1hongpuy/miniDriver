#include "DataNode/HttpGatewayControlClient.hpp"

#include "network/EventLoop.hpp"
#include "http/AsyncHttpClient.hpp"

#include <memory>
#include <utility>

namespace miniKV {
namespace datanode {

using namespace http;

class AsyncHttpControlRequestSender final : public IControlRequestSender {
public:
    AsyncHttpControlRequestSender(network::EventLoop* loop, std::string address, uint16_t port)
        : loop_(loop), address_(std::move(address)), port_(port) {}

    void send(const EncodedControlRequest& request, const std::string& internalToken,
              RpcCallback callback) override
    {
        auto httpRequest = AsyncHttpRequest::create(loop_);
        auto body = std::make_shared<std::string>(request.body);
        AsyncHttpRequestOptions options;
        options.address = address_;
        options.port = port_;
        options.method = request.method;
        options.path = request.path;
        options.contentLength = body->size();
        options.timeoutMs = 10000;
        options.headers = {{"Content-Type", "application/json"},
                           {"X-Cluster-Internal-Token", internalToken}};
        httpRequest->open(std::move(options), [httpRequest, body] {
            if(httpRequest->write(body->data(), body->size()) != AsyncWriteResult::kAccepted) {
                httpRequest->cancel();
                return;
            }
            httpRequest->finishBody();
        }, [callback = std::move(callback)](HttpClientResponse response, std::string error) mutable {
            RpcResult result;
            result.httpStatus = response.status;
            result.error = std::move(error);
            result.ok = result.error.empty() && result.httpStatus >= 200 && result.httpStatus < 300;
            if(!result.ok && result.error.empty()) result.error = "Gateway returned HTTP " + std::to_string(result.httpStatus);
            if(callback) callback(std::move(result));
        });
    }

private:
    network::EventLoop* loop_;
    std::string address_;
    uint16_t port_;
};



HttpGatewayControlClient::HttpGatewayControlClient(network::EventLoop* loop,
                                                   std::string gatewayAddress,
                                                   uint16_t gatewayPort,
                                                   std::string internalToken)
    : sender_(std::make_shared<AsyncHttpControlRequestSender>(loop, std::move(gatewayAddress), gatewayPort)),
      internalToken_(std::move(internalToken)) {}

HttpGatewayControlClient::HttpGatewayControlClient(std::shared_ptr<IControlRequestSender> sender,
                                                   std::string internalToken)
    : sender_(std::move(sender)), internalToken_(std::move(internalToken)) {}

void HttpGatewayControlClient::registerStorageNode(const NodeRegistration& request,
                                                   RpcCallback callback)
{
    sender_->send(makeNodeRegistrationRequest(request), internalToken_, std::move(callback));
}

void HttpGatewayControlClient::sendHeartbeat(const NodeHeartbeat& request,
                                             RpcCallback callback)
{
    sender_->send(makeHeartbeatRequest(request), internalToken_, std::move(callback));
}

void HttpGatewayControlClient::commitChunk(const ChunkCommit& request,
                                           RpcCallback callback)
{
    sender_->send(makeChunkCommitRequest(request), internalToken_, std::move(callback));
}

void HttpGatewayControlClient::releaseLease(const LeaseRelease& request,
                                            RpcCallback callback)
{
    sender_->send(makeLeaseReleaseRequest(request), internalToken_, std::move(callback));
}

}  // namespace miniKV::v2
}  // namespace
