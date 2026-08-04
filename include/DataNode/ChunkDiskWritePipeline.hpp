#pragma once

#include "DataNode/DiskWriteExecutor.hpp"
#include "DataNode/FastDataStore.hpp"
#include "http/HttpContext.hpp"

#include <cstddef>
#include <cstdint>
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

    struct Metrics {
        uint64_t queuedBytes = 0;
        uint64_t peakQueuedBytes = 0;
        uint64_t pauseCount = 0;
        uint64_t pauseNanoseconds = 0;
    };

    static constexpr size_t kHighWatermarkBytes = 1024 * 1024;
    static constexpr size_t kLowWatermarkBytes = 512 * 1024;

    static Ptr create(network::EventLoop* loop,
                      DiskWriteExecutor& executor,
                      std::shared_ptr<FastDataStore::WriteSession> writer,
                      ReadyCallback readyCallback);

    http::HttpContext::BodyConsumeResult push(const char* bytes, size_t size);
    void finishInput(FinishCallback callback);
    void cancel();

    bool failed() const { return failed_; }
    const Metrics& metrics() const { return metrics_; }
    const FastDataStore::WriteMetrics& writeMetrics() const { return writer_->metrics(); }

private:
    ChunkDiskWritePipeline(network::EventLoop* loop,
                           DiskWriteExecutor& executor,
                           std::shared_ptr<FastDataStore::WriteSession> writer,
                           ReadyCallback readyCallback);

    struct PendingBlock {
        DiskWriteExecutor::BlockLease block;
        size_t size = 0;
    };

    void scheduleNextAppend();
    void onAppendComplete(bool success, size_t size);
    void maybeFinish();
    void onFinishComplete(bool success, bool alreadyExists);
    void markPaused();
    void resumeIfDrained();

    network::EventLoop* loop_;
    DiskWriteExecutor& executor_;
    std::shared_ptr<FastDataStore::WriteSession> writer_;
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
    int64_t pauseStartedAtNanoseconds_ = 0;
};

}  // namespace miniKV::datanode
