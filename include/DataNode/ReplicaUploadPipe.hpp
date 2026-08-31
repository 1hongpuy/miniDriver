#pragma once

#include "http/HttpContext.hpp"
#include "http/AsyncHttpClient.hpp"
#include "http/PersistentHttpSession.hpp"
#include "DataNode/ReplicaConnectionPool.hpp"
#include "DataNode/DiskWriteExecutor.hpp"
#include "DataNode/ChunkWriteTypes.hpp"

#include <cstddef>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <string>



namespace miniKV::network {
class EventLoop;
}

namespace miniKV::datanode {

using namespace http;
struct ReplicaUploadMetrics {
    using Clock = std::chrono::steady_clock;

    uint64_t pauseCount = 0;
    uint64_t pauseNanoseconds = 0;
    size_t maxPendingBytes = 0;

    void recordPaused(size_t pendingBytes, Clock::time_point now)
    {
        if(pendingBytes > maxPendingBytes) maxPendingBytes = pendingBytes;
        if(paused_) return;
        paused_ = true;
        pausedAt_ = now;
        ++pauseCount;
    }

    void recordResumed(Clock::time_point now)
    {
        if(!paused_) return;
        const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(now - pausedAt_).count();
        if(elapsed > 0) pauseNanoseconds += static_cast<uint64_t>(elapsed);
        paused_ = false;
    }

private:
    bool paused_ = false;
    Clock::time_point pausedAt_{};
};
// Bridges one Primary -> Replica HTTP request with the upstream streaming
// HTTP body callback. It owns only a bounded amount of data that could not
// yet be accepted by AsyncHttpRequest; TcpConnection owns normal socket
// output buffering.
struct ReplicaUploadPipeOptions {
    AsyncHttpRequestOptions request;
    size_t maxPendingBytes = 256 * 1024;
    // A pool is optional so existing callers retain the one-shot client as a
    // safe fallback.  The supplied key is valid only for this owner loop.
    ReplicaConnectionPool::Ptr connectionPool;
    ReplicaPoolKey connectionKey;

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
    StreamConsumeResult push(const char* data, size_t size);
    StreamConsumeResult pushShared(
        DiskWriteExecutor::SharedBlockPtr block, size_t size);

    // Signals that the upstream HTTP body is complete. Final success still
    // arrives later through CompletionCallback after the replica HTTP ACK.
    void finish();
    void abort();

    bool paused() const { return upstreamPaused_; }
    bool finished() const { return finished_; }
    size_t pendingBytes() const { return pendingBytes_; }
    const ReplicaUploadMetrics& metrics() const { return metrics_; }

private:
    enum class State { kIdle, kOpening, kStreaming, kWaitingResponse, kFinished };

    explicit ReplicaUploadPipe(network::EventLoop* loop);

    void startInLoop(ReplicaUploadPipeOptions options, CompletionCallback completion);
    void startShortRequestInLoop();
    void startPersistentRequestInLoop(http::PersistentHttpSession::Ptr session);
    AsyncWriteResult writeDownstreamInLoop(const char* data, size_t size);
    void finishDownstreamInLoop();
    StreamConsumeResult pushInLoop(const char* data, size_t size);
    void finishInLoop();
    void abortInLoop();
    struct PendingBlock {
        std::string copied;
        DiskWriteExecutor::SharedBlockPtr shared;
        size_t size = 0;
        const char* data() const { return shared ? shared->data() : copied.data(); }
    };
    bool enqueuePendingInLoop(const char* data, size_t size);
    bool enqueueSharedPendingInLoop(DiskWriteExecutor::SharedBlockPtr block, size_t size);
    StreamConsumeResult pushSharedInLoop(
        DiskWriteExecutor::SharedBlockPtr block, size_t size);
    bool flushPendingInLoop();
    void tryResumeUpstreamInLoop();
    void failInLoop(std::string error);
    void completeInLoop(HttpClientResponse response, std::string error);
    void holdLifetimeInLoop();
    void releaseLifetimeInLoop();
    void markPausedInLoop();

    network::EventLoop* loop_;
    AsyncHttpRequest::Ptr request_;
    http::PersistentHttpSession::Ptr persistentRequest_;
    ReplicaUploadPipeOptions options_;
    CompletionCallback completionCallback_;
    std::deque<PendingBlock> pendingBlocks_;
    size_t pendingBytes_ = 0;
    ReplicaUploadMetrics metrics_;
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
