#include "DataNode/ReplicaUploadPipe.hpp"

#include "network/EventLoop.hpp"
#include "utils/AsyncLogger.hpp"

#include <cassert>
#include <utility>

namespace miniKV::datanode {

ReplicaUploadPipe::Ptr ReplicaUploadPipe::create(network::EventLoop* loop)
{
    return Ptr(new ReplicaUploadPipe(loop));
}

ReplicaUploadPipe::ReplicaUploadPipe(network::EventLoop* loop) : loop_(loop) {}

ReplicaUploadPipe::~ReplicaUploadPipe()
{
    assert(finished_ || state_ == State::kIdle);
}

void ReplicaUploadPipe::start(ReplicaUploadPipeOptions options, CompletionCallback completion)
{
    assert(loop_->isInLoopThread());
    startInLoop(std::move(options), std::move(completion));
}

void ReplicaUploadPipe::startInLoop(ReplicaUploadPipeOptions options, CompletionCallback completion)
{
    if(state_ != State::kIdle) return;
    if(options.maxPendingBytes == 0)
    {
        HttpClientResponse empty;
        if(completion) completion(std::move(empty), "replica pending buffer must be non-zero");
        return;
    }

    options_ = std::move(options);
    completionCallback_ = std::move(completion);
    state_ = State::kOpening;
    holdLifetimeInLoop();

    if(options_.connectionPool) {
        std::weak_ptr<ReplicaUploadPipe> weakSelf(shared_from_this());
        options_.connectionPool->borrow(options_.connectionKey,
            [weakSelf](http::PersistentHttpSession::Ptr session, std::string error) {
                if(auto self = weakSelf.lock()) {
                    if(self->finished_) return;
                    if(session) self->startPersistentRequestInLoop(std::move(session));
                    else {
                        miniKV::utils::logDebug("event=replica_pool_fallback reason=" + error);
                        self->startShortRequestInLoop();
                    }
                }
            });
        return;
    }
    startShortRequestInLoop();
}

void ReplicaUploadPipe::startShortRequestInLoop()
{

    request_ = AsyncHttpRequest::create(loop_);
    std::weak_ptr<ReplicaUploadPipe> weakSelf(shared_from_this());
    request_->setHighWaterMarkCallback([weakSelf](size_t) {
        if(Ptr self = weakSelf.lock())
        {
            self->downstreamBlocked_ = true;
            self->markPausedInLoop();
            self->upstreamPaused_ = true;
        }
    });
    request_->setLowWaterMarkCallback([weakSelf](size_t) {
        if(Ptr self = weakSelf.lock())
        {
            self->downstreamBlocked_ = false;
            self->flushPendingInLoop();
            self->tryResumeUpstreamInLoop();
        }
    });
    request_->open(
        options_.request,
        [weakSelf] {
            if(Ptr self = weakSelf.lock())
            {
                if(self->finished_) return;
                self->requestReady_ = true;
                self->state_ = State::kStreaming;
                miniKV::utils::logDebug("event=replica_connected pending_bytes=" +
                                        std::to_string(self->pendingBytes_));
                self->flushPendingInLoop();
                self->tryResumeUpstreamInLoop();
            }
        },
        [weakSelf](HttpClientResponse response, std::string error) {
            if(Ptr self = weakSelf.lock()) self->completeInLoop(std::move(response), std::move(error));
        });
}

void ReplicaUploadPipe::startPersistentRequestInLoop(http::PersistentHttpSession::Ptr session)
{
    persistentRequest_ = std::move(session);
    std::weak_ptr<ReplicaUploadPipe> weakSelf(shared_from_this());
    persistentRequest_->setHighWaterMarkCallback([weakSelf](size_t) {
        if(Ptr self = weakSelf.lock()) {
            self->downstreamBlocked_ = true;
            self->markPausedInLoop();
            self->upstreamPaused_ = true;
        }
    });
    persistentRequest_->setLowWaterMarkCallback([weakSelf](size_t) {
        if(Ptr self = weakSelf.lock()) {
            self->downstreamBlocked_ = false;
            self->flushPendingInLoop();
            self->tryResumeUpstreamInLoop();
        }
    });
    if(!persistentRequest_->start(options_.request,
        [weakSelf] {
            if(Ptr self = weakSelf.lock()) {
                if(self->finished_) return;
                self->requestReady_ = true;
                self->state_ = State::kStreaming;
                self->flushPendingInLoop();
                self->tryResumeUpstreamInLoop();
            }
        },
        [weakSelf](HttpClientResponse response, std::string error) {
            if(Ptr self = weakSelf.lock()) self->completeInLoop(std::move(response), std::move(error));
        })) {
        auto failed = std::move(persistentRequest_);
        if(options_.connectionPool) options_.connectionPool->discard(failed);
        startShortRequestInLoop();
    }
}

AsyncWriteResult ReplicaUploadPipe::writeDownstreamInLoop(const char* data, size_t size)
{
    if(persistentRequest_) return persistentRequest_->write(data, size);
    return request_ ? request_->write(data, size) : AsyncWriteResult::kClosed;
}

void ReplicaUploadPipe::finishDownstreamInLoop()
{
    if(persistentRequest_) persistentRequest_->finishBody();
    else if(request_) request_->finishBody();
}

StreamConsumeResult ReplicaUploadPipe::push(const char* data, size_t size)
{
    assert(loop_->isInLoopThread());
    return pushInLoop(data, size);
}

StreamConsumeResult ReplicaUploadPipe::pushShared(
    DiskWriteExecutor::SharedBlockPtr block, size_t size)
{
    assert(loop_->isInLoopThread());
    return pushSharedInLoop(std::move(block), size);
}

StreamConsumeResult ReplicaUploadPipe::pushSharedInLoop(
    DiskWriteExecutor::SharedBlockPtr block, size_t size)
{
    using Result = StreamConsumeResult;
    if(!block || finished_ || inputFinished_ || size == 0) return Result::kAbort;
    if(!pendingBlocks_.empty()) {
        if(!enqueueSharedPendingInLoop(std::move(block), size)) return Result::kAbort;
        flushPendingInLoop();
        markPausedInLoop();
        upstreamPaused_ = true;
        return Result::kPause;
    }
    const AsyncWriteResult result = writeDownstreamInLoop(block->data(), size);
    if(result == AsyncWriteResult::kAccepted) {
        if(downstreamBlocked_) { upstreamPaused_ = true; return Result::kPause; }
        return Result::kContinue;
    }
    if(result == AsyncWriteResult::kWouldBlock) {
        if(!enqueueSharedPendingInLoop(std::move(block), size)) return Result::kAbort;
        markPausedInLoop();
        upstreamPaused_ = true;
        return Result::kPause;
    }
    failInLoop("replica request stopped accepting shared body bytes");
    return Result::kAbort;
}

StreamConsumeResult ReplicaUploadPipe::pushInLoop(const char* data, size_t size)
{
    using Result = StreamConsumeResult;
    if(finished_ || state_ == State::kIdle || inputFinished_ || data == nullptr) return Result::kAbort;
    if(size == 0) return downstreamBlocked_ ? Result::kPause : Result::kContinue;

    // Preserve input order: a segment never overtakes one already waiting for
    // the connection to establish or its output queue to drain.
    if(!pendingBlocks_.empty())
    {
        if(!enqueuePendingInLoop(data, size)) return Result::kAbort;
        flushPendingInLoop();
        markPausedInLoop();
        upstreamPaused_ = true;
        return Result::kPause;
    }

    const AsyncWriteResult writeResult = writeDownstreamInLoop(data, size);
    if(writeResult == AsyncWriteResult::kAccepted)
    {
        if(downstreamBlocked_)
        {
            upstreamPaused_ = true;
            return Result::kPause;
        }
        return Result::kContinue;
    }
    if(writeResult == AsyncWriteResult::kWouldBlock)
    {
        if(!enqueuePendingInLoop(data, size)) return Result::kAbort;
        markPausedInLoop();
        upstreamPaused_ = true;
        miniKV::utils::logDebug("event=replica_pause pending_bytes=" +
                                std::to_string(pendingBytes_));
        return Result::kPause;
    }

    failInLoop("replica request stopped accepting body bytes");
    return Result::kAbort;
}

void ReplicaUploadPipe::finish()
{
    assert(loop_->isInLoopThread());
    finishInLoop();
}

void ReplicaUploadPipe::finishInLoop()
{
    if(finished_ || inputFinished_) return;
    inputFinished_ = true;
    if(!pendingBlocks_.empty())
    {
        upstreamPaused_ = true;
        return;
    }
    if(!requestReady_)
    {
        // AsyncHttpRequest records finishRequested_ and switches to waiting
        // response only after its non-blocking connect completes.
        finishDownstreamInLoop();
        requestFinishIssued_ = true;
        return;
    }
    finishDownstreamInLoop();
    requestFinishIssued_ = true;
    state_ = State::kWaitingResponse;
}

void ReplicaUploadPipe::abort()
{
    assert(loop_->isInLoopThread());
    abortInLoop();
}

void ReplicaUploadPipe::abortInLoop()
{
    if(finished_) return;
    failInLoop("replica upload aborted");
}

bool ReplicaUploadPipe::enqueuePendingInLoop(const char* data, size_t size)
{
    if(size > options_.maxPendingBytes - pendingBytes_)
    {
        failInLoop("replica pending buffer limit reached");
        return false;
    }
    PendingBlock pending;
    pending.copied.assign(data, size);
    pending.size = size;
    pendingBlocks_.push_back(std::move(pending));
    pendingBytes_ += size;
    if(pendingBytes_ > metrics_.maxPendingBytes) metrics_.maxPendingBytes = pendingBytes_;
    return true;
}

bool ReplicaUploadPipe::enqueueSharedPendingInLoop(DiskWriteExecutor::SharedBlockPtr block, size_t size)
{
    if(size > options_.maxPendingBytes - pendingBytes_) {
        failInLoop("replica pending buffer limit reached");
        return false;
    }
    pendingBlocks_.push_back({{}, std::move(block), size});
    pendingBytes_ += size;
    if(pendingBytes_ > metrics_.maxPendingBytes) metrics_.maxPendingBytes = pendingBytes_;
    return true;
}

bool ReplicaUploadPipe::flushPendingInLoop()
{
    if(finished_ || !requestReady_) return !finished_;

    while(!pendingBlocks_.empty())
    {
        const PendingBlock& block = pendingBlocks_.front();
        const AsyncWriteResult writeResult = writeDownstreamInLoop(block.data(), block.size);
        if(writeResult == AsyncWriteResult::kWouldBlock)
        {
            upstreamPaused_ = true;
            return true;
        }
        if(writeResult == AsyncWriteResult::kClosed)
        {
            failInLoop("replica request closed while flushing pending body bytes");
            return false;
        }
        pendingBytes_ -= block.size;
        pendingBlocks_.pop_front();
    }

    if(inputFinished_ && !requestFinishIssued_)
    {
        finishDownstreamInLoop();
        requestFinishIssued_ = true;
        state_ = State::kWaitingResponse;
    }
    return true;
}

void ReplicaUploadPipe::tryResumeUpstreamInLoop()
{
    if(finished_ || downstreamBlocked_ || !pendingBlocks_.empty()) return;
    if(!upstreamPaused_) return;
    upstreamPaused_ = false;
    metrics_.recordResumed(ReplicaUploadMetrics::Clock::now());
    miniKV::utils::logDebug("event=replica_resume");
    if(options_.resumeUpstream) options_.resumeUpstream();
}

void ReplicaUploadPipe::failInLoop(std::string error)
{
    if(finished_) return;
    // Complete this pipe with the original cause before cancelling the
    // request. AsyncHttpRequest::cancel() may invoke its response callback
    // synchronously when we already are on the EventLoop.
    auto request = std::move(request_);
    auto persistentRequest = std::move(persistentRequest_);
    HttpClientResponse empty;
    completeInLoop(std::move(empty), std::move(error));
    if(request) request->cancel();
    if(persistentRequest) {
        if(options_.connectionPool) options_.connectionPool->discard(persistentRequest);
        persistentRequest->cancel();
    }
}

void ReplicaUploadPipe::completeInLoop(HttpClientResponse response, std::string error)
{
    if(finished_) return;
    miniKV::utils::logInfo("event=replica_complete http_status=" +
                           std::to_string(response.status) + " error=" +
                           (error.empty() ? "-" : error));
    finished_ = true;
    state_ = State::kFinished;
    metrics_.recordResumed(ReplicaUploadMetrics::Clock::now());
    pendingBlocks_.clear();
    pendingBytes_ = 0;
    request_.reset();
    auto persistentRequest = std::move(persistentRequest_);
    if(persistentRequest && options_.connectionPool) {
        if(error.empty() && response.status == 200 && persistentRequest->idle() && persistentRequest->usable()) {
            options_.connectionPool->release(persistentRequest);
        } else {
            options_.connectionPool->discard(persistentRequest);
        }
    }

    auto completion = std::move(completionCallback_);
    releaseLifetimeInLoop();
    if(completion) completion(std::move(response), std::move(error));
}

void ReplicaUploadPipe::holdLifetimeInLoop()
{
    if(!lifetimeGuard_) lifetimeGuard_ = shared_from_this();
}

void ReplicaUploadPipe::releaseLifetimeInLoop()
{
    lifetimeGuard_.reset();
}

void ReplicaUploadPipe::markPausedInLoop()
{
    metrics_.recordPaused(pendingBytes_, ReplicaUploadMetrics::Clock::now());
}

}  // namespace miniKV::v2
