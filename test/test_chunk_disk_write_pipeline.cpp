#include "DataNode/ChunkDiskWritePipeline.hpp"
#include "DataNode/FastDataStore.hpp"
#include "network/EventLoop.hpp"
#include "utils/Util.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <thread>

int main()
{
    using miniKV::datanode::ChunkDiskWritePipeline;
    using miniKV::datanode::DiskWriteExecutor;

    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_chunk_disk_pipeline_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    const std::string bytes(2 * DiskWriteExecutor::kBlockBytes, 'p');
    const std::string hash = miniKV::util::sha256Hex(bytes.data(), bytes.size());

    miniKV::datanode::FastDataStore store(directory.string());
    if(!store.open()) {
        std::cerr << "FAIL: cannot open test data store\n";
        return 1;
    }
    auto session = store.beginPut(hash, bytes.size());
    if(session == nullptr) {
        std::cerr << "FAIL: cannot create write session\n";
        return 1;
    }

    DiskWriteExecutor::Config config;
    config.workerCount = 1;
    config.blockCount = 4;
    DiskWriteExecutor executor(config);

    miniKV::network::EventLoop loop;
    std::thread loopThread([&loop] { loop.loop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    std::promise<bool> finished;
    std::atomic<bool> pushed{false};
    loop.queueInLoop([&] {
        auto pipeline = ChunkDiskWritePipeline::create(
            &loop, executor,
            std::shared_ptr<miniKV::datanode::FastDataStore::WriteSession>(std::move(session)),
            [] {});
        if(!pipeline ||
           pipeline->push(bytes.data(), DiskWriteExecutor::kBlockBytes) !=
               miniKV::http::HttpContext::BodyConsumeResult::kContinue ||
           pipeline->push(bytes.data() + DiskWriteExecutor::kBlockBytes,
                          DiskWriteExecutor::kBlockBytes) !=
               miniKV::http::HttpContext::BodyConsumeResult::kContinue) {
            finished.set_value(false);
            loop.quit();
            return;
        }
        pushed.store(true);
        pipeline->finishInput([&finished, &loop](bool success, bool alreadyExists) {
            finished.set_value(success && !alreadyExists);
            loop.quit();
        });
    });

    auto result = finished.get_future();
    const auto waitStatus = result.wait_for(std::chrono::seconds(3));
    const bool succeeded = waitStatus == std::future_status::ready && result.get();
    if(!succeeded || !pushed.load()) {
        std::cerr << "FAIL: disk pipeline did not serialize and finish the chunk"
                  << " ready=" << (waitStatus == std::future_status::ready)
                  << " success=" << succeeded
                  << " pushed=" << pushed.load() << '\n';
        loop.quit();
        loopThread.join();
        return 1;
    }
    loopThread.join();

    std::string read;
    if(!store.get(hash, read) || read != bytes) {
        std::cerr << "FAIL: completed pipeline chunk is not readable\n";
        return 1;
    }

    DiskWriteExecutor::Config pausedConfig;
    pausedConfig.workerCount = 1;
    pausedConfig.blockCount = 32;
    DiskWriteExecutor pausedExecutor(pausedConfig);
    std::promise<void> blockerStarted;
    std::promise<void> unblockWorker;
    const auto unblock = unblockWorker.get_future().share();
    if(!pausedExecutor.submitTask([&blockerStarted, unblock] {
        blockerStarted.set_value();
        unblock.wait();
    }) || blockerStarted.get_future().wait_for(std::chrono::seconds(1)) !=
            std::future_status::ready) {
        std::cerr << "FAIL: cannot gate disk worker for backpressure test\n";
        return 1;
    }

    auto pausedSession = store.beginPut(hash, 16 * DiskWriteExecutor::kBlockBytes);
    if(pausedSession == nullptr) {
        std::cerr << "FAIL: cannot create paused pipeline session\n";
        return 1;
    }
    miniKV::network::EventLoop pausedLoop;
    std::thread pausedLoopThread([&pausedLoop] { pausedLoop.loop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    std::promise<bool> paused;
    pausedLoop.queueInLoop([&] {
        auto pipeline = ChunkDiskWritePipeline::create(
            &pausedLoop, pausedExecutor,
            std::shared_ptr<miniKV::datanode::FastDataStore::WriteSession>(std::move(pausedSession)),
            [] {});
        auto result = miniKV::http::HttpContext::BodyConsumeResult::kContinue;
        for(size_t i = 0; i < 16; ++i) {
            result = pipeline->push(bytes.data(), DiskWriteExecutor::kBlockBytes);
        }
        const bool hitHighWatermark =
            result == miniKV::http::HttpContext::BodyConsumeResult::kPause &&
            pipeline->metrics().queuedBytes == 16 * DiskWriteExecutor::kBlockBytes &&
            pipeline->metrics().pauseCount == 1;
        pipeline->cancel();
        paused.set_value(hitHighWatermark);
        pausedLoop.quit();
    });
    auto pausedResult = paused.get_future();
    if(pausedResult.wait_for(std::chrono::seconds(3)) != std::future_status::ready ||
       !pausedResult.get()) {
        std::cerr << "FAIL: disk queue did not apply its high-watermark pause\n";
        unblockWorker.set_value();
        pausedLoop.quit();
        pausedLoopThread.join();
        return 1;
    }
    unblockWorker.set_value();
    pausedLoopThread.join();

    DiskWriteExecutor::Config exhaustedConfig;
    exhaustedConfig.workerCount = 1;
    exhaustedConfig.blockCount = 1;
    DiskWriteExecutor exhaustedExecutor(exhaustedConfig);
    std::promise<void> exhaustedWorkerStarted;
    std::promise<void> unblockExhaustedWorker;
    const auto unblockExhausted = unblockExhaustedWorker.get_future().share();
    if(!exhaustedExecutor.submitTask([&exhaustedWorkerStarted, unblockExhausted] {
        exhaustedWorkerStarted.set_value();
        unblockExhausted.wait();
    }) || exhaustedWorkerStarted.get_future().wait_for(std::chrono::seconds(1)) !=
            std::future_status::ready) {
        std::cerr << "FAIL: cannot gate disk worker for pool exhaustion test\n";
        return 1;
    }

    auto exhaustedSession = store.beginPut(hash, 2 * DiskWriteExecutor::kBlockBytes);
    if(exhaustedSession == nullptr) {
        std::cerr << "FAIL: cannot create pool exhaustion session\n";
        unblockExhaustedWorker.set_value();
        return 1;
    }
    miniKV::network::EventLoop exhaustedLoop;
    std::thread exhaustedLoopThread([&exhaustedLoop] { exhaustedLoop.loop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    std::promise<bool> pausedBeforeConsume;
    exhaustedLoop.queueInLoop([&] {
        auto pipeline = ChunkDiskWritePipeline::create(
            &exhaustedLoop, exhaustedExecutor,
            std::shared_ptr<miniKV::datanode::FastDataStore::WriteSession>(std::move(exhaustedSession)),
            [] {});
        const auto first = pipeline->push(bytes.data(), DiskWriteExecutor::kBlockBytes);
        const auto second = pipeline->push(bytes.data() + DiskWriteExecutor::kBlockBytes,
                                           DiskWriteExecutor::kBlockBytes);
        pipeline->cancel();
        pausedBeforeConsume.set_value(
            first == miniKV::http::HttpContext::BodyConsumeResult::kContinue &&
            second == miniKV::http::HttpContext::BodyConsumeResult::kPauseBeforeConsume);
        exhaustedLoop.quit();
    });
    auto exhaustedResult = pausedBeforeConsume.get_future();
    if(exhaustedResult.wait_for(std::chrono::seconds(3)) != std::future_status::ready ||
       !exhaustedResult.get()) {
        std::cerr << "FAIL: exhausted block pool aborted instead of preserving body bytes\n";
        unblockExhaustedWorker.set_value();
        exhaustedLoop.quit();
        exhaustedLoopThread.join();
        return 1;
    }
    unblockExhaustedWorker.set_value();
    exhaustedLoopThread.join();

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: chunk disk write pipeline serializes blocks and finishes off-loop\n";
    return 0;
}
