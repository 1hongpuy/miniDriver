#include "DataNode/ReplicaUploadPipe.hpp"

#include "network/EventLoop.hpp"

#include <cassert>
#include <iostream>
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

    request_ = AsyncHttpRequest::create(loop_);
    std::weak_ptr<ReplicaUploadPipe> weakSelf(shared_from_this());
    request_->setHighWaterMarkCallback([weakSelf](size_t) {
        if(Ptr self = weakSelf.lock())
        {
            self->downstreamBlocked_ = true;
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
                std::cerr << "replica pipe connected; flushing " << self->pendingBytes_
                          << " pending bytes\n";
                self->flushPendingInLoop();
                self->tryResumeUpstreamInLoop();
            }
        },
        [weakSelf](HttpClientResponse response, std::string error) {
            if(Ptr self = weakSelf.lock()) self->completeInLoop(std::move(response), std::move(error));
        });
}

miniKV::http::HttpContext::BodyConsumeResult ReplicaUploadPipe::push(const char* data,
                                                                       size_t size)
{
    assert(loop_->isInLoopThread());
    return pushInLoop(data, size);
}

miniKV::http::HttpContext::BodyConsumeResult ReplicaUploadPipe::pushInLoop(const char* data,
                                                                             size_t size)
{
    using Result = miniKV::http::HttpContext::BodyConsumeResult;
    if(finished_ || state_ == State::kIdle || inputFinished_ || data == nullptr) return Result::kAbort;
    if(size == 0) return downstreamBlocked_ ? Result::kPause : Result::kContinue;

    // Preserve input order: a segment never overtakes one already waiting for
    // the connection to establish or its output queue to drain.
    if(!pendingBlocks_.empty())
    {
        if(!enqueuePendingInLoop(data, size)) return Result::kAbort;
        flushPendingInLoop();
        upstreamPaused_ = true;
        return Result::kPause;
    }

    const AsyncWriteResult writeResult = request_->write(data, size);
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
        upstreamPaused_ = true;
        std::cerr << "replica pipe paused upstream with " << pendingBytes_ << " pending bytes\n";
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
        request_->finishBody();
        requestFinishIssued_ = true;
        return;
    }
    request_->finishBody();
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
    pendingBlocks_.emplace_back(data, size);
    pendingBytes_ += size;
    return true;
}

bool ReplicaUploadPipe::flushPendingInLoop()
{
    if(finished_ || !requestReady_) return !finished_;

    while(!pendingBlocks_.empty())
    {
        const std::string& block = pendingBlocks_.front();
        const AsyncWriteResult writeResult = request_->write(block.data(), block.size());
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
        pendingBytes_ -= block.size();
        pendingBlocks_.erase(pendingBlocks_.begin());
    }

    if(inputFinished_ && !requestFinishIssued_)
    {
        request_->finishBody();
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
    std::cerr << "replica pipe resuming upstream\n";
    if(options_.resumeUpstream) options_.resumeUpstream();
}

void ReplicaUploadPipe::failInLoop(std::string error)
{
    if(finished_) return;
    // Complete this pipe with the original cause before cancelling the
    // request. AsyncHttpRequest::cancel() may invoke its response callback
    // synchronously when we already are on the EventLoop.
    auto request = std::move(request_);
    HttpClientResponse empty;
    completeInLoop(std::move(empty), std::move(error));
    if(request) request->cancel();
}

void ReplicaUploadPipe::completeInLoop(HttpClientResponse response, std::string error)
{
    if(finished_) return;
    std::cerr << "replica pipe completed: HTTP " << response.status
              << ", error=" << (error.empty() ? "<none>" : error) << '\n';
    finished_ = true;
    state_ = State::kFinished;
    pendingBlocks_.clear();
    pendingBytes_ = 0;
    request_.reset();

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

}  // namespace miniKV::v2
