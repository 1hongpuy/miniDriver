#include "DataNode/DiskWriteExecutor.hpp"
#include "TestCheck.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <iostream>

int main()
{
    using miniKV::datanode::DiskWriteExecutor;

    DiskWriteExecutor::Config config;
    config.workerCount = 1;
    config.blockCount = 2;
    DiskWriteExecutor executor(config);

    auto first = executor.tryAcquireBlock();
    auto second = executor.tryAcquireBlock();
    MINIKV_CHECK(first.has_value());
    MINIKV_CHECK(second.has_value());
    MINIKV_CHECK(!executor.tryAcquireBlock().has_value());
    const auto exhausted = executor.metrics();
    MINIKV_CHECK(exhausted.availableBlocks == 0);
    MINIKV_CHECK(exhausted.totalBlocks == 2);
    MINIKV_CHECK(exhausted.peakLeasedBytes == 2 * DiskWriteExecutor::kBlockBytes);

    std::promise<void> ran;
    std::atomic<bool> wrote{false};
    MINIKV_CHECK(executor.submit(std::move(*first), [&ran, &wrote](DiskWriteExecutor::BlockLease block) {
        block.data()[0] = 'x';
        wrote.store(true);
        ran.set_value();
    }));

    MINIKV_CHECK(ran.get_future().wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    MINIKV_CHECK(wrote.load());
    MINIKV_CHECK(executor.tryAcquireBlock().has_value());

    std::promise<void> finalized;
    MINIKV_CHECK(executor.submitTask([&finalized] { finalized.set_value(); }));
    MINIKV_CHECK(finalized.get_future().wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    second.reset();
    const auto metrics = executor.metrics();
    MINIKV_CHECK(metrics.availableBlocks == 2);
    MINIKV_CHECK(metrics.totalWorkers == 1);
    MINIKV_CHECK(metrics.completedTasks >= 2);

    auto shared = executor.tryAcquireSharedBlock();
    MINIKV_CHECK(shared != nullptr);
    shared->data()[0] = 's';
    auto replicaReference = shared;
    std::promise<void> sharedWritten;
    MINIKV_CHECK(executor.submit(std::move(shared), [&sharedWritten](DiskWriteExecutor::SharedBlockPtr block) {
        MINIKV_CHECK(block != nullptr);
        MINIKV_CHECK(block->data()[0] == 's');
        sharedWritten.set_value();
    }));
    MINIKV_CHECK(sharedWritten.get_future().wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    // The disk worker has completed, but a replica/backpressure owner still
    // retains the same bounded pool slot.
    MINIKV_CHECK(executor.metrics().availableBlocks == 1);
    replicaReference.reset();
    MINIKV_CHECK(executor.metrics().availableBlocks == 2);

    std::cout << "PASS: bounded disk executor reuses fixed blocks\n";
    return 0;
}
