#include "DataNode/ChunkDiskWritePipeline.hpp"

#include "network/EventLoop.hpp"

#include <chrono>
#include <cstring>
#include <utility>

namespace miniKV::datanode {
namespace {

int64_t nowNanoseconds()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

}  // namespace

ChunkDiskWritePipeline::Ptr ChunkDiskWritePipeline::create(
    network::EventLoop* loop,
    DiskWriteExecutor& executor,
    std::shared_ptr<FastDataStore::WriteSession> writer,
    ReadyCallback readyCallback)
{
    if(loop == nullptr || writer == nullptr) return nullptr;
    return Ptr(new ChunkDiskWritePipeline(loop, executor, std::move(writer),
                                          std::move(readyCallback)));
}

ChunkDiskWritePipeline::ChunkDiskWritePipeline(
    network::EventLoop* loop,
    DiskWriteExecutor& executor,
    std::shared_ptr<FastDataStore::WriteSession> writer,
    ReadyCallback readyCallback)
    : loop_(loop), executor_(executor), writer_(std::move(writer)),
      readyCallback_(std::move(readyCallback))
{
}

http::HttpContext::BodyConsumeResult ChunkDiskWritePipeline::push(const char* bytes, size_t size)
{
    if(failed_ || cancelled_ || bytes == nullptr || size == 0 ||
       size > DiskWriteExecutor::kBlockBytes) {
        failed_ = true;
        return http::HttpContext::BodyConsumeResult::kAbort;
    }

    if(!selfHold_) selfHold_ = shared_from_this();

    auto block = executor_.tryAcquireBlock();
    if(!block.has_value()) {
        // A shared pool may briefly be exhausted by another admitted stream.
        // Keep this body segment in HttpContext's input Buffer; treating this
        // as a malformed request used to turn temporary contention into 400.
        markPaused();
        scheduleBlockAvailabilityCheck();
        return http::HttpContext::BodyConsumeResult::kPauseBeforeConsume;
    }

    std::memcpy(block->data(), bytes, size);
    metrics_.queuedBytes += size;
    if(metrics_.queuedBytes > metrics_.peakQueuedBytes) {
        metrics_.peakQueuedBytes = metrics_.queuedBytes;
    }
    pendingBlocks_.push_back({std::move(*block), size});
    scheduleNextAppend();
    if(failed_) return http::HttpContext::BodyConsumeResult::kAbort;

    if(metrics_.queuedBytes >= kHighWatermarkBytes) {
        markPaused();
        return http::HttpContext::BodyConsumeResult::kPause;
    }
    return http::HttpContext::BodyConsumeResult::kContinue;
}

void ChunkDiskWritePipeline::finishInput(FinishCallback callback)
{
    if(inputFinished_ || cancelled_) return;
    inputFinished_ = true;
    finishCallback_ = std::move(callback);
    maybeFinish();
}

void ChunkDiskWritePipeline::cancel()
{
    cancelled_ = true;
    finishCallback_ = nullptr;
    pendingBlocks_.clear();
    selfHold_.reset();
}

void ChunkDiskWritePipeline::scheduleNextAppend()
{
    if(appendInFlight_ || pendingBlocks_.empty() || cancelled_) return;

    PendingBlock pending = std::move(pendingBlocks_.front());
    pendingBlocks_.pop_front();
    appendInFlight_ = true;
    const auto self = shared_from_this();
    if(!executor_.submit(std::move(pending.block), [self, size = pending.size](DiskWriteExecutor::BlockLease workBlock) {
        const bool success = self->writer_->append(workBlock.data(), size);
        const std::weak_ptr<ChunkDiskWritePipeline> weakSelf = self;
        self->loop_->queueInLoop([weakSelf, success, size] {
            if(const auto pipeline = weakSelf.lock()) {
                pipeline->onAppendComplete(success, size);
            }
        });
    })) {
        appendInFlight_ = false;
        failed_ = true;
    }
}

void ChunkDiskWritePipeline::onAppendComplete(bool success, size_t size)
{
    appendInFlight_ = false;
    if(metrics_.queuedBytes >= size) metrics_.queuedBytes -= size;
    else metrics_.queuedBytes = 0;
    if(!success) {
        failed_ = true;
        pendingBlocks_.clear();
    }

    resumeIfDrained();
    if(!failed_) scheduleNextAppend();
    maybeFinish();
}

void ChunkDiskWritePipeline::maybeFinish()
{
    if(!inputFinished_ || appendInFlight_ || !pendingBlocks_.empty() || finishInFlight_ || cancelled_) return;

    if(failed_) {
        onFinishComplete(false, false);
        return;
    }

    finishInFlight_ = true;
    const std::weak_ptr<ChunkDiskWritePipeline> weakSelf = shared_from_this();
    if(!executor_.submitTask([weakSelf] {
        const auto self = weakSelf.lock();
        if(!self) return;
        bool alreadyExists = false;
        const bool success = self->writer_->finish(alreadyExists);
        self->loop_->queueInLoop([weakSelf, success, alreadyExists] {
            if(const auto pipeline = weakSelf.lock()) {
                pipeline->onFinishComplete(success, alreadyExists);
            }
        });
    })) {
        finishInFlight_ = false;
        onFinishComplete(false, false);
    }
}

void ChunkDiskWritePipeline::onFinishComplete(bool success, bool alreadyExists)
{
    finishInFlight_ = false;
    if(cancelled_) return;
    const auto keepAlive = std::move(selfHold_);
    const auto callback = std::move(finishCallback_);
    if(callback) callback(success, alreadyExists);
}

void ChunkDiskWritePipeline::markPaused()
{
    if(paused_) return;
    paused_ = true;
    ++metrics_.pauseCount;
    pauseStartedAtNanoseconds_ = nowNanoseconds();
}

void ChunkDiskWritePipeline::resumeIfDrained()
{
    if(!paused_ || metrics_.queuedBytes > kLowWatermarkBytes) return;
    paused_ = false;
    metrics_.pauseNanoseconds += nowNanoseconds() - pauseStartedAtNanoseconds_;
    pauseStartedAtNanoseconds_ = 0;
    if(readyCallback_) readyCallback_();
}

void ChunkDiskWritePipeline::scheduleBlockAvailabilityCheck()
{
    if(blockCheckScheduled_ || cancelled_ || failed_) return;
    blockCheckScheduled_ = true;
    const std::weak_ptr<ChunkDiskWritePipeline> weakSelf = shared_from_this();
    loop_->runAfter(1, [weakSelf] {
        const auto self = weakSelf.lock();
        if(!self || self->cancelled_ || self->failed_) return;

        self->blockCheckScheduled_ = false;
        if(!self->executor_.hasAvailableBlock()) {
            self->scheduleBlockAvailabilityCheck();
            return;
        }

        if(self->paused_) {
            self->paused_ = false;
            self->metrics_.pauseNanoseconds += nowNanoseconds() - self->pauseStartedAtNanoseconds_;
            self->pauseStartedAtNanoseconds_ = 0;
        }
        if(self->readyCallback_) self->readyCallback_();
    });
}

}  // namespace miniKV::datanode
