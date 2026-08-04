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

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: chunk disk write pipeline serializes blocks and finishes off-loop\n";
    return 0;
}
