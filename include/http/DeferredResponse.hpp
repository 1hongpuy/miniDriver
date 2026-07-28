#pragma once

#include "http/HttpResponse.hpp"

#include <atomic>
#include <memory>

namespace miniKV::network {
class EventLoop;
class TcpConnection;
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
}

namespace miniKV::http {

// A handler calls defer() before returning when its response depends on an
// asynchronous operation. complete() may be called from any thread; actual
// socket writes are always queued back to the connection's EventLoop.
class DeferredResponse : public std::enable_shared_from_this<DeferredResponse> {
public:
    using Ptr = std::shared_ptr<DeferredResponse>;

    static Ptr create(network::EventLoop* loop,
                      const network::TcpConnectionPtr& connection,
                      bool closeAfterResponse);

    void defer();
    bool deferred() const { return deferred_.load(); }
    bool completed() const { return completionScheduled_.load(); }
    void complete(HttpResponse response);

private:
    DeferredResponse(network::EventLoop* loop,
                     std::weak_ptr<network::TcpConnection> connection,
                     bool closeAfterResponse);
    void completeInLoop(HttpResponse response);

    network::EventLoop* loop_;
    std::weak_ptr<network::TcpConnection> connection_;
    bool closeAfterResponse_;
    std::atomic<bool> deferred_{false};
    std::atomic<bool> completionScheduled_{false};
};

}  // namespace miniKV::http
