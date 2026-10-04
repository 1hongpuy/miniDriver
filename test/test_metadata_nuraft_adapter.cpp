#include "metadata/raft/NuRaftAdapters.hpp"
#include "TestCheck.hpp"

#include <atomic>
#include <cstring>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>
#include <unistd.h>

using namespace miniKV::metadata;
using namespace miniKV::metadata::raft;

namespace {

std::string testDirectory()
{
    return "/tmp/minidriver-nuraft-adapter-" + std::to_string(::getpid());
}

nuraft::ptr<nuraft::buffer> asBuffer(const std::string& bytes)
{
    auto buffer = nuraft::buffer::alloc(bytes.size());
    if(!bytes.empty()) std::memcpy(buffer->data_begin(), bytes.data(), bytes.size());
    buffer->pos(0); return buffer;
}

MetadataCommand registerNode()
{
    MetadataCommand command; command.commandId = "register-node-1";
    command.type = MetadataCommandType::kRegisterNode;
    command.actorType = "admin"; command.actorId = "admin";
    RegisterNodePayload payload; payload.nodeId = "dn-1"; payload.bootId = "boot-1";
    payload.address = "127.0.0.1"; payload.dataPort = 19201;
    payload.registeredCapacityBytes = 1024 * 1024;
    payload.capabilities = {"crc32c", "rf2"}; command.payload = payload; return command;
}

} // namespace

int main()
{
    const auto directory = testDirectory(); std::filesystem::remove_all(directory);
    std::filesystem::create_directories(directory);
    {
        auto storage = std::make_shared<DurableRaftStorage>(directory + "/raft");
        auto logs = nuraft::cs_new<DurableLogStore>(storage);
        auto manager = nuraft::cs_new<DurableStateManager>(1,
            std::vector<StaticMember>{{1, "127.0.0.1:18001"}}, storage, logs);
        auto machine = nuraft::cs_new<NuRaftStateMachine>(directory + "/snapshot", logs, manager);

        const auto commandBytes = encodeMetadataCommand(registerNode());
        MINIKV_CHECK(commandBytes.has_value()); auto payload = asBuffer(*commandBytes);
        auto entry = nuraft::cs_new<nuraft::log_entry>(7, payload);
        MINIKV_CHECK(logs->append(entry) == 1);
        logs->end_of_append_batch(1, 1);
        auto resultBuffer = machine->commit(1, *payload);
        const auto result = NuRaftStateMachine::decodeResult(*resultBuffer);
        MINIKV_CHECK(result && result->status == ApplyStatus::kOk && result->nodeEpoch == 1);
        MINIKV_CHECK(logs->last_durable_index() == 1 && logs->term_at(1) == 7);

        auto config = manager->load_config(); nuraft::snapshot snapshot(1, 7, config);
        bool callbackCalled = false; bool callbackResult = false;
        nuraft::async_result<bool>::handler_type done = [&](bool& ok, nuraft::ptr<std::exception>& error) {
            callbackCalled = true; callbackResult = ok; MINIKV_CHECK(!error);
        };
        machine->create_snapshot(snapshot, done);
        MINIKV_CHECK(callbackCalled && callbackResult && machine->last_snapshot());
    }
    {
        auto storage = std::make_shared<DurableRaftStorage>(directory + "/raft");
        auto logs = nuraft::cs_new<DurableLogStore>(storage);
        auto manager = nuraft::cs_new<DurableStateManager>(1,
            std::vector<StaticMember>{{1, "127.0.0.1:18001"}}, storage, logs);
        auto machine = nuraft::cs_new<NuRaftStateMachine>(directory + "/snapshot", logs, manager);
        MINIKV_CHECK(logs->next_slot() == 2 && logs->term_at(1) == 7);
        MINIKV_CHECK(machine->last_commit_index() == 1);
        const auto nodeState = machine->stateDigest();
        MINIKV_CHECK(!nodeState.empty() && machine->last_snapshot());
    }
    {
        auto storage = std::make_shared<DurableRaftStorage>(directory + "/async/raft");
        auto logs = nuraft::cs_new<DurableLogStore>(storage);
        std::atomic<bool> callbackCalled{false};
        std::atomic<bool> callbackOk{false};
        std::atomic<uint64_t> writevBatches{0};
        std::atomic<uint64_t> writevRecords{0};
        std::atomic<uint64_t> writevBytes{0};
        logs->enableAsyncAppend(true);
        logs->setAppendCompletionCallback([&](bool ok, uint64_t) {
            callbackOk.store(ok, std::memory_order_release);
            callbackCalled.store(true, std::memory_order_release);
        });
        logs->setAppendBatchMetricsCallback([&](uint64_t records, uint64_t bytes, uint64_t) {
            writevRecords.fetch_add(records, std::memory_order_relaxed);
            writevBytes.fetch_add(bytes, std::memory_order_relaxed);
            writevBatches.fetch_add(1, std::memory_order_release);
        });
        const auto commandBytes = encodeMetadataCommand(registerNode());
        MINIKV_CHECK(commandBytes.has_value()); auto payload = asBuffer(*commandBytes);
        auto entry = nuraft::cs_new<nuraft::log_entry>(9, payload);
        auto secondPayload = asBuffer(*commandBytes);
        auto secondEntry = nuraft::cs_new<nuraft::log_entry>(9, secondPayload);
        MINIKV_CHECK(logs->append(entry) == 1);
        MINIKV_CHECK(logs->append(secondEntry) == 2);
        logs->end_of_append_batch(1, 2);
        for(int i = 0; i < 100 && logs->last_durable_index() != 2; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        MINIKV_CHECK(logs->last_durable_index() == 2);
        MINIKV_CHECK(callbackCalled.load(std::memory_order_acquire));
        MINIKV_CHECK(callbackOk.load(std::memory_order_acquire));
        MINIKV_CHECK(writevBatches.load(std::memory_order_acquire) == 1);
        MINIKV_CHECK(writevRecords.load(std::memory_order_relaxed) == 2);
        MINIKV_CHECK(writevBytes.load(std::memory_order_relaxed) > 0);
    }
    {
        const auto customStorageDirectory = directory + "/custom/raft";
        const auto customWalDirectory = directory + "/custom/wal";
        {
            auto storage = std::make_shared<DurableRaftStorage>(customStorageDirectory);
            auto logs = nuraft::cs_new<DurableLogStore>(storage, customWalDirectory);
            auto payload = asBuffer(*encodeMetadataCommand(registerNode()));
            auto entry = nuraft::cs_new<nuraft::log_entry>(11, payload);
            MINIKV_CHECK(logs->append(entry) == 1);
            logs->end_of_append_batch(1, 1);
            MINIKV_CHECK(logs->last_durable_index() == 1);
        }
        MINIKV_CHECK(std::filesystem::exists(customWalDirectory + "/raft-log.bin"));
        MINIKV_CHECK(!std::filesystem::exists(customStorageDirectory + "/raft-log.bin"));
        auto storage = std::make_shared<DurableRaftStorage>(customStorageDirectory);
        auto logs = nuraft::cs_new<DurableLogStore>(storage, customWalDirectory);
        MINIKV_CHECK(logs->next_slot() == 2 && logs->term_at(1) == 11);
    }
    std::filesystem::remove_all(directory);
    std::cout << "metadata NuRaft durable adapter tests passed\n";
    return 0;
}
