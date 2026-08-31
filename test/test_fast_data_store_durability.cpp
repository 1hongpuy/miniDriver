#include "DataNode/FastDataStore.hpp"
#include "utils/Util.hpp"

#include <filesystem>
#include <atomic>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <cerrno>
#include <unistd.h>
#include <vector>

namespace {

bool writeChunk(miniKV::datanode::FastDataStore& store,
                const std::string& bytes,
                bool expectedAlreadyExists,
                bool expectedDurable)
{
    const std::string hash = miniKV::util::sha256Hex(bytes.data(), bytes.size());
    auto session = store.beginPut(hash, bytes.size());
    if(session == nullptr || !session->append(bytes.data(), bytes.size())) return false;

    bool alreadyExists = false;
    if(!session->finish(alreadyExists) || alreadyExists != expectedAlreadyExists) return false;

    const auto metrics = session->metrics();
    if(metrics.durable != expectedDurable) return false;
    if(expectedDurable) {
        if(metrics.dataSyncOperations != 1 || metrics.indexSyncOperations != 1) return false;
    } else if(metrics.dataSyncOperations != 0 || metrics.indexSyncOperations != 0) {
        return false;
    }

    std::string read;
    return store.get(hash, read) && read == bytes;
}

}  // namespace

int main()
{
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_fast_store_durability_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    const std::string legacyBytes(256 * 1024, 'b');
    {
        miniKV::datanode::FastDataStore buffered(directory.string());
        if(!buffered.open() || !writeChunk(buffered, legacyBytes, false, false)) {
            std::cerr << "FAIL: buffered durability baseline failed\n";
            return 1;
        }

        // More slices than the platform IOV_MAX forces appendBatch() to
        // advance across multiple pwritev calls while preserving one stream.
        const long systemIovMax = ::sysconf(_SC_IOV_MAX);
        const size_t fragmentedSliceCount = systemIovMax > 0 && systemIovMax < 65536
            ? static_cast<size_t>(systemIovMax) + 1 : 1025;
        const std::string fragmentedBytes(fragmentedSliceCount, 'f');
        const std::string fragmentedHash = miniKV::util::sha256Hex(
            fragmentedBytes.data(), fragmentedBytes.size());
        auto fragmentedSession = buffered.beginPut(fragmentedHash, fragmentedBytes.size());
        std::vector<miniKV::datanode::FastDataStore::WriteSlice> slices;
        slices.reserve(fragmentedBytes.size());
        for(size_t i = 0; i < fragmentedBytes.size(); ++i) {
            slices.push_back({fragmentedBytes.data() + i, 1});
        }
        bool fragmentedAlreadyExists = false;
        if(fragmentedSession == nullptr || !fragmentedSession->appendBatch(slices) ||
           !fragmentedSession->finish(fragmentedAlreadyExists) || fragmentedAlreadyExists ||
           fragmentedSession->metrics().pwritevOperations < 2) {
            std::cerr << "FAIL: pwritev batch did not advance across its iovec limit\n";
            return 1;
        }
        std::string fragmentedRead;
        if(!buffered.get(fragmentedHash, fragmentedRead) || fragmentedRead != fragmentedBytes) {
            std::cerr << "FAIL: fragmented pwritev batch changed byte order\n";
            return 1;
        }
    }

    miniKV::datanode::FastDataStore::Config config;
    config.durabilityPolicy = miniKV::datanode::DurabilityPolicy::kChunkSync;
    {
        miniKV::datanode::FastDataStore durable(directory.string(), config);
        if(!durable.open()) {
            std::cerr << "FAIL: cannot reopen store in chunk_sync mode\n";
            return 1;
        }

        // Re-uploading a legacy buffered object validates its referenced bytes,
        // synchronizes the data file, and republishes its index synchronously.
        if(!writeChunk(durable, legacyBytes, true, true)) {
            std::cerr << "FAIL: legacy duplicate was not durably acknowledged\n";
            return 1;
        }

        const std::string durableBytes(512 * 1024, 'd');
        if(!writeChunk(durable, durableBytes, false, true)) {
            std::cerr << "FAIL: new chunk_sync object failed\n";
            return 1;
        }
    }

    const std::filesystem::path groupDirectory =
        std::filesystem::temp_directory_path() / "minikv_fast_store_group_commit_test";
    std::filesystem::remove_all(groupDirectory, error);
    miniKV::datanode::FastDataStore::Config groupConfig;
    groupConfig.durabilityPolicy = miniKV::datanode::DurabilityPolicy::kGroupCommit;
    groupConfig.groupCommit.maxBatchBytes = 1024 * 1024;
    groupConfig.groupCommit.maxBatchItems = 4;
    groupConfig.groupCommit.maxBatchDelayUs = 100000;
    groupConfig.groupCommit.maxPendingBytes = 4 * 1024 * 1024;
    groupConfig.groupCommit.maxPendingItems = 16;
    {
        miniKV::datanode::FastDataStore grouped(groupDirectory.string(), groupConfig);
        if(!grouped.open()) {
            std::cerr << "FAIL: cannot open group_commit store\n";
            return 1;
        }

        std::vector<std::string> values;
        std::vector<std::string> hashes;
        std::vector<std::shared_ptr<miniKV::datanode::FastDataStore::WriteSession>> sessions;
        std::vector<std::future<bool>> completions;
        for(size_t i = 0; i < 4; ++i) {
            values.emplace_back(256 * 1024, static_cast<char>('g' + i));
            hashes.push_back(miniKV::util::sha256Hex(values.back().data(), values.back().size()));
            auto session = grouped.beginPut(hashes.back(), values.back().size());
            if(session == nullptr || !session->append(values.back().data(), values.back().size())) {
                std::cerr << "FAIL: cannot prepare grouped write\n";
                return 1;
            }
            auto completion = std::make_shared<std::promise<bool>>();
            completions.push_back(completion->get_future());
            if(!session->finishAsync([completion](bool success, bool alreadyExists) {
                completion->set_value(success && !alreadyExists);
            })) {
                std::cerr << "FAIL: cannot enqueue grouped write\n";
                return 1;
            }
            sessions.push_back(std::move(session));
        }
        for(auto& completion : completions) {
            if(completion.wait_for(std::chrono::seconds(2)) != std::future_status::ready ||
               !completion.get()) {
                std::cerr << "FAIL: grouped write did not complete\n";
                return 1;
            }
        }

        const auto groupMetrics = grouped.durabilityMetrics();
        if(groupMetrics.committedBatches != 1 || groupMetrics.committedItems != 4 ||
           groupMetrics.dataSyncOperations != 1 || groupMetrics.indexSyncOperations != 1 ||
           groupMetrics.lastDurableSequence == 0) {
            std::cerr << "FAIL: four chunks were not committed by one durability batch\n";
            return 1;
        }
        uint64_t operationOwners = 0;
        uint64_t timingOwners = 0;
        const uint64_t durableSequence = sessions.front()->metrics().durableSequence;
        for(size_t i = 0; i < sessions.size(); ++i) {
            const auto metrics = sessions[i]->metrics();
            operationOwners += metrics.dataSyncOperations;
            timingOwners += metrics.durabilitySyncTimingOwner ? 1 : 0;
            if(!metrics.durable || metrics.durableSequence != durableSequence ||
               metrics.groupBatchItems != 4 || metrics.groupBatchBytes != 1024 * 1024) {
                std::cerr << "FAIL: per-chunk group fencing metrics are inconsistent\n";
                return 1;
            }
            std::string read;
            if(!grouped.get(hashes[i], read) || read != values[i]) {
                std::cerr << "FAIL: grouped object is not readable\n";
                return 1;
            }
        }
        if(operationOwners != 1 || timingOwners != 1) {
            std::cerr << "FAIL: per-chunk metrics double counted grouped fdatasync\n";
            return 1;
        }
        if(groupMetrics.lastBatchBytes != 1024 * 1024 ||
           groupMetrics.lastBatchItems != 4 ||
           groupMetrics.lastBatchCommitNanoseconds == 0 ||
           groupMetrics.lastDataSyncNanoseconds == 0 ||
           groupMetrics.lastIndexSyncNanoseconds == 0) {
            std::cerr << "FAIL: group batch observability is incomplete\n";
            return 1;
        }
        for(const auto& session : sessions) {
            const auto metrics = session->metrics();
            if(metrics.groupWaitNanoseconds < metrics.durabilityQueueWaitNanoseconds) {
                std::cerr << "FAIL: per-write durability timing is inconsistent\n";
                return 1;
            }
        }
    }

    const std::filesystem::path overlapDirectory =
        std::filesystem::temp_directory_path() / "minikv_group_commit_overlap_metrics_test";
    std::filesystem::remove_all(overlapDirectory, error);
    auto overlapConfig = groupConfig;
    overlapConfig.groupCommit.maxBatchItems = 1;
    overlapConfig.groupCommit.maxBatchDelayUs = 0;
    std::atomic<bool> syncEntered{false};
    overlapConfig.dataSyncOverride = [&syncEntered](int) {
        syncEntered.store(true, std::memory_order_release);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        return 0;
    };
    {
        miniKV::datanode::FastDataStore overlapStore(overlapDirectory.string(), overlapConfig);
        const std::string firstBytes(64 * 1024, 'o');
        const std::string secondBytes(64 * 1024, 'p');
        const std::string firstHash = miniKV::util::sha256Hex(firstBytes.data(), firstBytes.size());
        const std::string secondHash = miniKV::util::sha256Hex(secondBytes.data(), secondBytes.size());
        auto first = overlapStore.open() ? overlapStore.beginPut(firstHash, firstBytes.size()) : nullptr;
        if(first == nullptr || !first->append(firstBytes.data(), firstBytes.size())) {
            std::cerr << "FAIL: cannot prepare overlap metrics write\n";
            return 1;
        }
        auto completion = std::make_shared<std::promise<bool>>();
        auto future = completion->get_future();
        if(!first->finishAsync([completion](bool success, bool) { completion->set_value(success); })) {
            std::cerr << "FAIL: cannot start overlap metrics sync\n";
            return 1;
        }
        for(size_t attempt = 0; attempt < 100 && !syncEntered.load(std::memory_order_acquire); ++attempt) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        auto second = overlapStore.beginPut(secondHash, secondBytes.size());
        if(!syncEntered.load(std::memory_order_acquire) || second == nullptr ||
           !second->append(secondBytes.data(), secondBytes.size()) ||
           future.wait_for(std::chrono::seconds(2)) != std::future_status::ready || !future.get()) {
            std::cerr << "FAIL: cannot observe pwrite during group sync\n";
            return 1;
        }
        const auto overlapMetrics = overlapStore.durabilityMetrics();
        if(overlapMetrics.pwriteOperationsWhileSync == 0 ||
           overlapMetrics.pwriteBytesWhileSync < secondBytes.size()) {
            std::cerr << "FAIL: pwrite/sync overlap was not recorded\n";
            return 1;
        }
    }

    const std::filesystem::path dataFailureDirectory =
        std::filesystem::temp_directory_path() / "minikv_group_commit_data_failure_test";
    std::filesystem::remove_all(dataFailureDirectory, error);
    auto dataFailureConfig = groupConfig;
    dataFailureConfig.groupCommit.maxBatchItems = 1;
    dataFailureConfig.groupCommit.maxBatchDelayUs = 0;
    dataFailureConfig.dataSyncOverride = [](int) {
        errno = EIO;
        return -1;
    };
    {
        miniKV::datanode::FastDataStore failedStore(
            dataFailureDirectory.string(), dataFailureConfig);
        const std::string bytes(64 * 1024, 'x');
        const std::string hash = miniKV::util::sha256Hex(bytes.data(), bytes.size());
        bool alreadyExists = false;
        auto session = failedStore.open() ? failedStore.beginPut(hash, bytes.size()) : nullptr;
        if(session == nullptr || !session->append(bytes.data(), bytes.size()) ||
           session->finish(alreadyExists) || failedStore.exists(hash) ||
           failedStore.durabilityMetrics().failedBatches != 1) {
            std::cerr << "FAIL: data sync failure published a grouped object\n";
            return 1;
        }
    }
    {
        miniKV::datanode::FastDataStore recovered(dataFailureDirectory.string());
        if(!recovered.open() || recovered.reusableBytes() < 64 * 1024) {
            std::cerr << "FAIL: restart did not reclaim an unindexed failed extent\n";
            return 1;
        }
    }

    const std::filesystem::path indexFailureDirectory =
        std::filesystem::temp_directory_path() / "minikv_group_commit_index_failure_test";
    std::filesystem::remove_all(indexFailureDirectory, error);
    auto indexFailureConfig = groupConfig;
    indexFailureConfig.groupCommit.maxBatchItems = 1;
    indexFailureConfig.groupCommit.maxBatchDelayUs = 0;
    indexFailureConfig.dataSyncOverride = [](int) { return 0; };
    indexFailureConfig.failIndexSync = [] { return true; };
    {
        miniKV::datanode::FastDataStore failedStore(
            indexFailureDirectory.string(), indexFailureConfig);
        const std::string bytes(64 * 1024, 'y');
        const std::string hash = miniKV::util::sha256Hex(bytes.data(), bytes.size());
        bool alreadyExists = false;
        auto session = failedStore.open() ? failedStore.beginPut(hash, bytes.size()) : nullptr;
        if(session == nullptr || !session->append(bytes.data(), bytes.size()) ||
           session->finish(alreadyExists) || failedStore.exists(hash) ||
           failedStore.durabilityMetrics().failedBatches != 1 ||
           failedStore.durabilityMetrics().dataSyncOperations != 1 ||
           failedStore.durabilityMetrics().indexSyncOperations != 1) {
            std::cerr << "FAIL: index sync failure published a grouped object\n";
            return 1;
        }
    }

    const std::filesystem::path drainDirectory =
        std::filesystem::temp_directory_path() / "minikv_group_commit_shutdown_drain_test";
    std::filesystem::remove_all(drainDirectory, error);
    auto drainConfig = groupConfig;
    drainConfig.groupCommit.maxBatchBytes = 8 * 1024 * 1024;
    drainConfig.groupCommit.maxBatchItems = 8;
    drainConfig.groupCommit.maxBatchDelayUs = 1000000;
    drainConfig.groupCommit.maxPendingBytes = 16 * 1024 * 1024;
    std::promise<bool> drained;
    std::future<bool> drainedResult = drained.get_future();
    std::shared_ptr<miniKV::datanode::FastDataStore::WriteSession> drainSession;
    {
        miniKV::datanode::FastDataStore draining(drainDirectory.string(), drainConfig);
        const std::string bytes(64 * 1024, 'z');
        const std::string hash = miniKV::util::sha256Hex(bytes.data(), bytes.size());
        drainSession = draining.open() ? draining.beginPut(hash, bytes.size()) : nullptr;
        if(drainSession == nullptr || !drainSession->append(bytes.data(), bytes.size()) ||
           !drainSession->finishAsync([&drained](bool success, bool alreadyExists) {
               drained.set_value(success && !alreadyExists);
           })) {
            std::cerr << "FAIL: cannot enqueue shutdown drain write\n";
            return 1;
        }
        // Store destruction must wake the coordinator and durably drain this
        // sub-threshold batch instead of waiting for the one-second deadline.
    }
    if(drainedResult.wait_for(std::chrono::seconds(2)) != std::future_status::ready ||
       !drainedResult.get() || !drainSession->metrics().durable) {
        std::cerr << "FAIL: shutdown did not drain pending durability waiters\n";
        return 1;
    }

    const std::filesystem::path capacityDirectory =
        std::filesystem::temp_directory_path() / "minikv_group_commit_capacity_test";
    std::filesystem::remove_all(capacityDirectory, error);
    auto capacityConfig = groupConfig;
    capacityConfig.groupCommit.maxBatchBytes = 64 * 1024;
    capacityConfig.groupCommit.maxBatchItems = 1;
    capacityConfig.groupCommit.maxBatchDelayUs = 0;
    capacityConfig.groupCommit.maxPendingBytes = 64 * 1024;
    capacityConfig.groupCommit.maxPendingItems = 1;
    std::promise<void> syncStarted;
    std::promise<void> releaseSync;
    const auto releaseSyncFuture = releaseSync.get_future().share();
    capacityConfig.dataSyncOverride = [&syncStarted, releaseSyncFuture](int) {
        syncStarted.set_value();
        releaseSyncFuture.wait();
        return 0;
    };
    {
        miniKV::datanode::FastDataStore bounded(capacityDirectory.string(), capacityConfig);
        const std::string bytes(64 * 1024, 'q');
        const std::string hash = miniKV::util::sha256Hex(bytes.data(), bytes.size());
        auto session = bounded.open() ? bounded.beginPut(hash, bytes.size()) : nullptr;
        std::promise<bool> completion;
        auto completionResult = completion.get_future();
        if(session == nullptr || !session->append(bytes.data(), bytes.size()) ||
           !session->finishAsync([&completion](bool success, bool) {
               completion.set_value(success);
           }) || syncStarted.get_future().wait_for(std::chrono::seconds(1)) !=
               std::future_status::ready || bounded.canAcceptDurability(bytes.size()) ||
           bounded.beginPut(miniKV::util::sha256Hex("second", 6), bytes.size()) != nullptr) {
            std::cerr << "FAIL: durability high watermark did not reject new admission\n";
            releaseSync.set_value();
            return 1;
        }
        releaseSync.set_value();
        if(completionResult.wait_for(std::chrono::seconds(2)) != std::future_status::ready ||
           !completionResult.get() || bounded.durabilityMetrics().pendingItems != 0 ||
           bounded.durabilityMetrics().pendingBytes != 0) {
            std::cerr << "FAIL: bounded durability request did not resume\n";
            return 1;
        }
    }

    std::filesystem::remove_all(directory, error);
    std::filesystem::remove_all(groupDirectory, error);
    std::filesystem::remove_all(dataFailureDirectory, error);
    std::filesystem::remove_all(indexFailureDirectory, error);
    std::filesystem::remove_all(drainDirectory, error);
    std::filesystem::remove_all(capacityDirectory, error);
    std::cout << "PASS: FastDataStore separates buffered, chunk_sync, and group_commit acknowledgements\n";
    return 0;
}
