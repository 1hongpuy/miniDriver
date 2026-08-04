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

    std::cout << "PASS: bounded disk executor reuses fixed blocks\n";
    return 0;
}
