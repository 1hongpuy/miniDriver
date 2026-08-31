#include "DataNode/DiskWriteExecutor.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace miniKV::datanode {

DiskWriteExecutor::BlockLease::BlockLease(BlockLease&& other) noexcept
    : owner_(other.owner_), index_(other.index_)
{
    other.owner_ = nullptr;
    other.index_ = UINT16_MAX;
}

DiskWriteExecutor::BlockLease& DiskWriteExecutor::BlockLease::operator=(BlockLease&& other) noexcept
{
    if(this == &other) return *this;
    reset();
    owner_ = other.owner_;
    index_ = other.index_;
    other.owner_ = nullptr;
    other.index_ = UINT16_MAX;
    return *this;
}

DiskWriteExecutor::BlockLease::~BlockLease()
{
    reset();
}

char* DiskWriteExecutor::BlockLease::data()
{
    return owner_ == nullptr ? nullptr : owner_->blocks_[index_].data;
}

void DiskWriteExecutor::BlockLease::reset()
{
    if(owner_ == nullptr) return;
    owner_->release(index_);
    owner_ = nullptr;
    index_ = UINT16_MAX;
}

DiskWriteExecutor::DiskWriteExecutor()
    : DiskWriteExecutor(Config{})
{
}

DiskWriteExecutor::DiskWriteExecutor(Config config)
{
    if(config.workerCount == 0 || config.blockCount == 0 || config.blockCount > UINT16_MAX) {
        throw std::invalid_argument("invalid disk write executor configuration");
    }

    blocks_ = std::make_unique<Block[]>(config.blockCount);
    freeIds_.reserve(config.blockCount);
    for(uint16_t i = 0; i < config.blockCount; ++i) {
        freeIds_.push_back(i);
    }
    workers_.reserve(config.workerCount);
    for(size_t i = 0; i < config.workerCount; ++i) {
        workers_.emplace_back([this] { workerMain(); });
    }
}

DiskWriteExecutor::~DiskWriteExecutor()
{
    stop();
}

std::optional<DiskWriteExecutor::BlockLease> DiskWriteExecutor::tryAcquireBlock()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if(stopping_ || freeIds_.empty()) return std::nullopt;

    const uint16_t index = freeIds_.back();
    freeIds_.pop_back();
    leasedBytes_ += kBlockBytes;
    if(leasedBytes_ > peakLeasedBytes_) peakLeasedBytes_ = leasedBytes_;
    return BlockLease(this, index);
}

DiskWriteExecutor::SharedBlockPtr DiskWriteExecutor::tryAcquireSharedBlock()
{
    auto lease = tryAcquireBlock();
    if(!lease.has_value()) return nullptr;
    return SharedBlockPtr(new SharedBlock(std::move(*lease)));
}

bool DiskWriteExecutor::hasAvailableBlock() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return !stopping_ && !freeIds_.empty();
}

bool DiskWriteExecutor::submit(BlockLease block, Work work)
{
    if(!block || !work) return false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(stopping_) return false;
        WorkItem item;
        item.block = std::move(block);
        item.work = std::move(work);
        item.queuedAt = std::chrono::steady_clock::now();
        readyQueue_.push_back(std::move(item));
        peakQueuedTasks_ = std::max<uint64_t>(peakQueuedTasks_, readyQueue_.size());
    }
    cv_.notify_one();
    return true;
}

bool DiskWriteExecutor::submit(SharedBlockPtr block, SharedWork work)
{
    if(!block || !work) return false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(stopping_) return false;
        WorkItem item;
        item.sharedBlock = std::move(block);
        item.sharedWork = std::move(work);
        item.queuedAt = std::chrono::steady_clock::now();
        readyQueue_.push_back(std::move(item));
        peakQueuedTasks_ = std::max<uint64_t>(peakQueuedTasks_, readyQueue_.size());
    }
    cv_.notify_one();
    return true;
}

bool DiskWriteExecutor::submitTask(Task task)
{
    if(!task) return false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(stopping_) return false;
        WorkItem item;
        item.task = std::move(task);
        item.queuedAt = std::chrono::steady_clock::now();
        readyQueue_.push_back(std::move(item));
        peakQueuedTasks_ = std::max<uint64_t>(peakQueuedTasks_, readyQueue_.size());
    }
    cv_.notify_one();
    return true;
}

DiskWriteExecutor::Metrics DiskWriteExecutor::metrics() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return {leasedBytes_, peakLeasedBytes_, static_cast<uint64_t>(readyQueue_.size()),
            peakQueuedTasks_, static_cast<uint64_t>(freeIds_.size()),
            static_cast<uint64_t>(freeIds_.size() + leasedBytes_ / kBlockBytes),
            activeWorkers_, static_cast<uint64_t>(workers_.size()), completedTasks_,
            startedTasks_, totalQueueWaitUs_, maxQueueWaitUs_, totalWorkUs_, maxWorkUs_};
}

void DiskWriteExecutor::stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(stopping_) return;
        stopping_ = true;
    }
    cv_.notify_all();
    for(auto& worker : workers_) {
        if(worker.joinable()) worker.join();
    }
}

void DiskWriteExecutor::release(uint16_t index)
{
    std::lock_guard<std::mutex> lock(mutex_);
    freeIds_.push_back(index);
    leasedBytes_ -= kBlockBytes;
}

void DiskWriteExecutor::workerMain()
{
    for(;;) {
        WorkItem item;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return stopping_ || !readyQueue_.empty(); });
            if(stopping_ && readyQueue_.empty()) return;
            item = std::move(readyQueue_.front());
            readyQueue_.pop_front();
            ++activeWorkers_;
            ++startedTasks_;
            const uint64_t queueWaitUs = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - item.queuedAt).count());
            totalQueueWaitUs_ += queueWaitUs;
            maxQueueWaitUs_ = std::max(maxQueueWaitUs_, queueWaitUs);
        }

        const auto workStartedAt = std::chrono::steady_clock::now();
        try {
            if(item.work) item.work(std::move(item.block));
            else if(item.sharedWork) item.sharedWork(std::move(item.sharedBlock));
            else item.task();
        } catch(...) {
            // The owning upload pipeline reports its own write failure.
        }
        const uint64_t workUs = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - workStartedAt).count());
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(activeWorkers_ > 0) --activeWorkers_;
            ++completedTasks_;
            totalWorkUs_ += workUs;
            maxWorkUs_ = std::max(maxWorkUs_, workUs);
        }
    }
}

}  // namespace miniKV::datanode
