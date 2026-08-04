#pragma once

#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

namespace miniKV::datanode {

class DiskWriteExecutor {
public:
    static constexpr size_t kBlockBytes = 64 * 1024;

    struct Config {
        size_t workerCount = 2;
        size_t blockCount = 128;
    };

    struct Metrics {
        uint64_t leasedBytes = 0;
        uint64_t peakLeasedBytes = 0;
        uint64_t queuedTasks = 0;
    };

    class BlockLease {
    public:
        BlockLease() = default;
        BlockLease(const BlockLease&) = delete;
        BlockLease& operator=(const BlockLease&) = delete;
        BlockLease(BlockLease&& other) noexcept;
        BlockLease& operator=(BlockLease&& other) noexcept;
        ~BlockLease();

        char* data();
        explicit operator bool() const { return owner_ != nullptr; }

    private:
        friend class DiskWriteExecutor;

        BlockLease(DiskWriteExecutor* owner, uint16_t index)
            : owner_(owner), index_(index) {}

        void reset();

        DiskWriteExecutor* owner_ = nullptr;
        uint16_t index_ = UINT16_MAX;
    };

    using Work = std::function<void(BlockLease)>;
    using Task = std::function<void()>;

    DiskWriteExecutor();
    explicit DiskWriteExecutor(Config config);
    ~DiskWriteExecutor();

    DiskWriteExecutor(const DiskWriteExecutor&) = delete;
    DiskWriteExecutor& operator=(const DiskWriteExecutor&) = delete;

    std::optional<BlockLease> tryAcquireBlock();
    bool submit(BlockLease block, Work work);
    bool submitTask(Task task);
    Metrics metrics() const;
    void stop();

private:
    struct Block {
        char data[kBlockBytes];
    };

    struct WorkItem {
        BlockLease block;
        Work work;
        Task task;
    };

    void release(uint16_t index);
    void workerMain();

    std::unique_ptr<Block[]> blocks_;
    std::vector<uint16_t> freeIds_;
    std::deque<WorkItem> readyQueue_;
    std::vector<std::thread> workers_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_ = false;
    uint64_t leasedBytes_ = 0;
    uint64_t peakLeasedBytes_ = 0;
};

}  // namespace miniKV::datanode
