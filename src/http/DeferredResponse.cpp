#include "http/DeferredResponse.hpp"

#include "network/Buffer.hpp"
#include "network/EventLoop.hpp"
#include "network/TcpConnection.hpp"

#include <utility>

namespace miniKV::http {

DeferredResponse::Ptr DeferredResponse::create(network::EventLoop* loop,
                                                const network::TcpConnectionPtr& connection,
                                                bool closeAfterResponse)
{
    return Ptr(new DeferredResponse(loop, connection, closeAfterResponse));
}

DeferredResponse::DeferredResponse(network::EventLoop* loop,
                                   std::weak_ptr<network::TcpConnection> connection,
                                   bool closeAfterResponse)
    : loop_(loop), connection_(std::move(connection)), closeAfterResponse_(closeAfterResponse) {}

void DeferredResponse::defer()
{
    deferred_.store(true);
}

void DeferredResponse::complete(HttpResponse response)
{
    if(completionScheduled_.exchange(true)) return;
    Ptr self(shared_from_this());
    // Always queue so a handler can mark defer() and finish synchronously
    // without HttpServer sending a second immediate response on its return.
    loop_->queueInLoop([self, response = std::move(response)]() mutable {
        self->completeInLoop(std::move(response));
    });
}

void DeferredResponse::completeInLoop(HttpResponse response)
{
    //延迟发送响应
    auto connection = connection_.lock();
    if(!connection || !connection->connected()) return;

    // HttpResponse defaults to close=true, so use the request-derived policy
    // supplied by HttpServer instead of accidentally closing every deferred
    // HTTP/1.1 keep-alive request.
    response.setCloseConnection(closeAfterResponse_);
    network::Buffer output;
    response.appendToBuffer(&output);
    connection->send(output.peek(), output.readableBytes());
    if(response.isSendFile())
    {
        connection->startSendFile(response.bodyFilePath(), response.bodyFileOffset(),
                                  response.bodyFileSize());
    }
    if(response.closeConnection()) connection->shutdown();
}

}  // namespace miniKV::http
