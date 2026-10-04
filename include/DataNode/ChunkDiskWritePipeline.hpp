#pragma once

#include "DataNode/DiskWriteExecutor.hpp"
#include "DataNode/FastDataStore.hpp"
#include "DataNode/ChunkWriteTypes.hpp"

#include <cstddef>
#include <cstdint>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>

namespace miniKV::network {
class EventLoop;
}

namespace miniKV::datanode {

class ChunkDiskWritePipeline : public std::enable_shared_from_this<ChunkDiskWritePipeline> {
public:
    using Ptr = std::shared_ptr<ChunkDiskWritePipeline>;
    using ReadyCallback = std::function<void()>;
    using FinishCallback = std::function<void(bool success, bool alreadyExists)>;
    // Optional, opt-in lifecycle observer used by bounded diagnostics. The
    // callback is invoked on the event-loop thread and must not alter flow
    // control or storage semantics.
    using StageCallback = std::function<void(const char* stage,
                                             std::chrono::steady_clock::time_point at)>;

    struct Metrics {
        uint64_t queuedBytes = 0;
        uint64_t peakQueuedBytes = 0;
        uint64_t pauseCount = 0;
        uint64_t pauseNanoseconds = 0;
        uint64_t submittedBatches = 0;
        uint64_t submittedBatchBytes = 0;
        uint64_t peakBatchBytes = 0;
    };

    enum class WriteMode {
        kSingleBlock,
        kPwritev
    };

    struct Config {
        WriteMode writeMode = WriteMode::kPwritev;
        size_t targetBatchBytes = 256 * 1024;
        uint64_t maxBatchDelayUs = 1000;
        StageCallback stageCallback;
    };

    static constexpr size_t kHighWatermarkBytes = 1024 * 1024;
    static constexpr size_t kLowWatermarkBytes = 512 * 1024;

    static Ptr create(network::EventLoop* loop,
                      DiskWriteExecutor& executor,
                      std::shared_ptr<FastDataStore::WriteSession> writer,
                      ReadyCallback readyCallback);
    static Ptr create(network::EventLoop* loop,
                      DiskWriteExecutor& executor,
                      std::shared_ptr<FastDataStore::WriteSession> writer,
                      ReadyCallback readyCallback,
                      Config config);

    StreamConsumeResult push(const char* bytes, size_t size,
                             DiskWriteExecutor::SharedBlockPtr* sharedBlock = nullptr);
    void finishInput(FinishCallback callback);
    void cancel();

    bool failed() const { return failed_; }
    const Metrics& metrics() const { return metrics_; }
    const FastDataStore::WriteMetrics& writeMetrics() const { return writer_->metrics(); }

private:
    ChunkDiskWritePipeline(network::EventLoop* loop,
                           DiskWriteExecutor& executor,
                           std::shared_ptr<FastDataStore::WriteSession> writer,
                           ReadyCallback readyCallback,
                           Config config);

    struct PendingBlock {
        DiskWriteExecutor::SharedBlockPtr block;
        size_t size = 0;
    };

    void scheduleNextAppend(bool force = false);
    void scheduleBatchDelay();
    void cancelBatchDelay();
    void onBatchDelay();
    void onAppendComplete(bool success, size_t size);
    void maybeFinish();
    void onFinishComplete(bool success, bool alreadyExists);
    void markPaused();
    void resumeIfDrained();
    void scheduleBlockAvailabilityCheck();

    network::EventLoop* loop_;
    DiskWriteExecutor& executor_;
    std::shared_ptr<FastDataStore::WriteSession> writer_;
    Config config_;
    ReadyCallback readyCallback_;
    FinishCallback finishCallback_;
    std::deque<PendingBlock> pendingBlocks_;
    Ptr selfHold_;
    Metrics metrics_;
    bool appendInFlight_ = false;
    bool inputFinished_ = false;
    bool finishInFlight_ = false;
    bool failed_ = false;
    bool cancelled_ = false;
    bool paused_ = false;
    bool blockCheckScheduled_ = false;
    int batchTimerId_ = -1;
    int64_t pauseStartedAtNanoseconds_ = 0;
};

}  // namespace miniKV::datanode
