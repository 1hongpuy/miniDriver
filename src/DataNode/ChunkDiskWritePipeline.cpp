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
    return create(loop, executor, std::move(writer), std::move(readyCallback), Config{});
}

ChunkDiskWritePipeline::Ptr ChunkDiskWritePipeline::create(
    network::EventLoop* loop,
    DiskWriteExecutor& executor,
    std::shared_ptr<FastDataStore::WriteSession> writer,
    ReadyCallback readyCallback,
    Config config)
{
    if(loop == nullptr || writer == nullptr || config.targetBatchBytes == 0) return nullptr;
    return Ptr(new ChunkDiskWritePipeline(loop, executor, std::move(writer),
                                          std::move(readyCallback), config));
}

ChunkDiskWritePipeline::ChunkDiskWritePipeline(
    network::EventLoop* loop,
    DiskWriteExecutor& executor,
    std::shared_ptr<FastDataStore::WriteSession> writer,
    ReadyCallback readyCallback,
    Config config)
    : loop_(loop), executor_(executor), writer_(std::move(writer)),
      config_(config), readyCallback_(std::move(readyCallback))
{
}

StreamConsumeResult ChunkDiskWritePipeline::push(
    const char* bytes, size_t size, DiskWriteExecutor::SharedBlockPtr* sharedBlock)
{
    if(failed_ || cancelled_ || bytes == nullptr || size == 0 ||
       size > DiskWriteExecutor::kBlockBytes) {
        failed_ = true;
        return StreamConsumeResult::kAbort;
    }

    if(!selfHold_) selfHold_ = shared_from_this();

    auto block = executor_.tryAcquireSharedBlock();
    if(!block) {
        // A shared pool may briefly be exhausted by another admitted stream.
        // Keep this body segment in HttpContext's input Buffer; treating this
        // as a malformed request used to turn temporary contention into 400.
        markPaused();
        scheduleBlockAvailabilityCheck();
        return StreamConsumeResult::kPauseBeforeConsume;
    }

    std::memcpy(block->data(), bytes, size);
    metrics_.queuedBytes += size;
    if(metrics_.queuedBytes > metrics_.peakQueuedBytes) {
        metrics_.peakQueuedBytes = metrics_.queuedBytes;
    }
    pendingBlocks_.push_back({block, size});
    if(metrics_.queuedBytes == size && config_.stageCallback) {
        config_.stageCallback("disk_enqueued", std::chrono::steady_clock::now());
    }
    if(sharedBlock != nullptr) *sharedBlock = std::move(block);
    if(config_.writeMode == WriteMode::kSingleBlock ||
       metrics_.queuedBytes >= config_.targetBatchBytes) {
        scheduleNextAppend();
    } else {
        scheduleBatchDelay();
    }
    if(failed_) return StreamConsumeResult::kAbort;

    if(metrics_.queuedBytes >= kHighWatermarkBytes) {
        markPaused();
        return StreamConsumeResult::kPause;
    }
    return StreamConsumeResult::kContinue;
}

void ChunkDiskWritePipeline::finishInput(FinishCallback callback)
{
    if(inputFinished_ || cancelled_) return;
    inputFinished_ = true;
    finishCallback_ = std::move(callback);
    cancelBatchDelay();
    scheduleNextAppend(true);
    maybeFinish();
}

void ChunkDiskWritePipeline::cancel()
{
    cancelled_ = true;
    finishCallback_ = nullptr;
    cancelBatchDelay();
    pendingBlocks_.clear();
    selfHold_.reset();
}

void ChunkDiskWritePipeline::scheduleNextAppend(bool force)
{
    if(appendInFlight_ || pendingBlocks_.empty() || cancelled_) return;
    if(!force && config_.writeMode == WriteMode::kPwritev &&
       metrics_.queuedBytes < config_.targetBatchBytes) {
        scheduleBatchDelay();
        return;
    }

    cancelBatchDelay();

    std::vector<PendingBlock> batch;
    size_t batchBytes = 0;
    do {
        PendingBlock pending = std::move(pendingBlocks_.front());
        pendingBlocks_.pop_front();
        batchBytes += pending.size;
        batch.push_back(std::move(pending));
    } while(!pendingBlocks_.empty() &&
            config_.writeMode == WriteMode::kPwritev &&
            batchBytes < config_.targetBatchBytes);

    appendInFlight_ = true;
    ++metrics_.submittedBatches;
    metrics_.submittedBatchBytes += batchBytes;
    if(batchBytes > metrics_.peakBatchBytes) metrics_.peakBatchBytes = batchBytes;
    const auto self = shared_from_this();
    if(!executor_.submitTask([self, batch = std::move(batch), batchBytes]() mutable {
        bool success = false;
        const auto diskStartedAt = std::chrono::steady_clock::now();
        if(self->config_.writeMode == WriteMode::kSingleBlock) {
            success = self->writer_->append(batch.front().block->data(), batch.front().size);
        } else {
            std::vector<FastDataStore::WriteSlice> slices;
            slices.reserve(batch.size());
            for(const PendingBlock& pending : batch) {
                slices.push_back({pending.block->data(), pending.size});
            }
            success = self->writer_->appendBatch(slices);
        }
        const std::weak_ptr<ChunkDiskWritePipeline> weakSelf = self;
        self->loop_->queueInLoop([weakSelf, success, batchBytes, diskStartedAt] {
            if(const auto pipeline = weakSelf.lock()) {
                if(pipeline->config_.stageCallback) {
                    pipeline->config_.stageCallback("disk_started", diskStartedAt);
                }
                pipeline->onAppendComplete(success, batchBytes);
            }
        });
    })) {
        appendInFlight_ = false;
        if(metrics_.queuedBytes >= batchBytes) metrics_.queuedBytes -= batchBytes;
        else metrics_.queuedBytes = 0;
        failed_ = true;
    }
}

void ChunkDiskWritePipeline::scheduleBatchDelay()
{
    if(batchTimerId_ >= 0 || pendingBlocks_.empty() || cancelled_ || failed_ ||
       config_.writeMode != WriteMode::kPwritev) return;
    if(config_.maxBatchDelayUs == 0) {
        scheduleNextAppend(true);
        return;
    }
    const int64_t delayMs = static_cast<int64_t>((config_.maxBatchDelayUs + 999) / 1000);
    const std::weak_ptr<ChunkDiskWritePipeline> weakSelf = shared_from_this();
    batchTimerId_ = loop_->runAfter(delayMs, [weakSelf] {
        if(const auto self = weakSelf.lock()) self->onBatchDelay();
    });
}

void ChunkDiskWritePipeline::cancelBatchDelay()
{
    if(batchTimerId_ < 0) return;
    loop_->cancel(batchTimerId_);
    batchTimerId_ = -1;
}

void ChunkDiskWritePipeline::onBatchDelay()
{
    batchTimerId_ = -1;
    scheduleNextAppend(true);
}

void ChunkDiskWritePipeline::onAppendComplete(bool success, size_t size)
{
    appendInFlight_ = false;
    if(config_.stageCallback) config_.stageCallback("disk_finished", std::chrono::steady_clock::now());
    if(metrics_.queuedBytes >= size) metrics_.queuedBytes -= size;
    else metrics_.queuedBytes = 0;
    if(!success) {
        failed_ = true;
        pendingBlocks_.clear();
    }

    resumeIfDrained();
    if(!failed_) scheduleNextAppend(inputFinished_);
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
        self->writer_->finishAsync([weakSelf](bool success, bool alreadyExists) {
            if(const auto pipeline = weakSelf.lock()) {
                // finishAsync's group-commit callback runs on the serialized
                // durability worker.  Record that handoff separately from
                // the later EventLoop callback so queue/wakeup delay is not
                // folded into fdatasync time.
                pipeline->writer_->markDurabilityCallbackDispatched();
                pipeline->loop_->queueInLoop([weakSelf, success, alreadyExists] {
                    if(const auto queuedPipeline = weakSelf.lock()) {
                        queuedPipeline->writer_->markCompletionCallbackObserved();
                        queuedPipeline->onFinishComplete(success, alreadyExists);
                    }
                });
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
