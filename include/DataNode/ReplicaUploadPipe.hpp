#pragma once

#include "http/HttpContext.hpp"
#include "http/AsyncHttpClient.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>



namespace miniKV::network {
class EventLoop;
}

namespace miniKV::datanode {

using namespace http;
// Bridges one Primary -> Replica HTTP request with the upstream streaming
// HTTP body callback. It owns only a bounded amount of data that could not
// yet be accepted by AsyncHttpRequest; TcpConnection owns normal socket
// output buffering.
struct ReplicaUploadPipeOptions {
    AsyncHttpRequestOptions request;
    size_t maxPendingBytes = 256 * 1024;

    // Called on the owning EventLoop after the downstream request becomes
    // writable again. The owner normally calls upstreamConnection->resumeRead().
    std::function<void()> resumeUpstream;
};

class ReplicaUploadPipe : public std::enable_shared_from_this<ReplicaUploadPipe> {
public:
    using Ptr = std::shared_ptr<ReplicaUploadPipe>;
    using CompletionCallback = std::function<void(HttpClientResponse response,
                                                  std::string error)>;

    static Ptr create(network::EventLoop* loop);
    ~ReplicaUploadPipe();

    ReplicaUploadPipe(const ReplicaUploadPipe&) = delete;
    ReplicaUploadPipe& operator=(const ReplicaUploadPipe&) = delete;

    // All stateful methods run on the owning EventLoop. start() creates the
    // downstream HTTP request; it does not wait for TCP connect.
    void start(ReplicaUploadPipeOptions options, CompletionCallback completion);

    // Called from HttpContext::BodyDataCallback. kPause means this segment
    // was accepted by the pipe but more upstream bytes must wait. kAbort means
    // this segment was not accepted and the HTTP stream must fail.
    miniKV::http::HttpContext::BodyConsumeResult push(const char* data, size_t size);

    // Signals that the upstream HTTP body is complete. Final success still
    // arrives later through CompletionCallback after the replica HTTP ACK.
    void finish();
    void abort();

    bool paused() const { return upstreamPaused_; }
    bool finished() const { return finished_; }
    size_t pendingBytes() const { return pendingBytes_; }

private:
    enum class State { kIdle, kOpening, kStreaming, kWaitingResponse, kFinished };

    explicit ReplicaUploadPipe(network::EventLoop* loop);

    void startInLoop(ReplicaUploadPipeOptions options, CompletionCallback completion);
    miniKV::http::HttpContext::BodyConsumeResult pushInLoop(const char* data, size_t size);
    void finishInLoop();
    void abortInLoop();
    bool enqueuePendingInLoop(const char* data, size_t size);
    bool flushPendingInLoop();
    void tryResumeUpstreamInLoop();
    void failInLoop(std::string error);
    void completeInLoop(HttpClientResponse response, std::string error);
    void holdLifetimeInLoop();
    void releaseLifetimeInLoop();

    network::EventLoop* loop_;
    AsyncHttpRequest::Ptr request_;
    ReplicaUploadPipeOptions options_;
    CompletionCallback completionCallback_;
    std::vector<std::string> pendingBlocks_;
    size_t pendingBytes_ = 0;
    bool requestReady_ = false;
    bool downstreamBlocked_ = false;
    bool upstreamPaused_ = false;
    bool inputFinished_ = false;
    bool requestFinishIssued_ = false;
    bool finished_ = false;
    State state_ = State::kIdle;
    Ptr lifetimeGuard_;
};

}  // namespace miniKV::v2
