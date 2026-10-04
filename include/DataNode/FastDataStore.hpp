#pragma once

#include "DataNode/ChunkWriteTypes.hpp"

#include <cstdint>
#include <chrono>
#include <functional>
#include <memory>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <sys/types.h>
#include <vector>

namespace leveldb {
class DB;
}

namespace miniKV::datanode {

class ChecksumContext;

enum class DurabilityPolicy {
    kBuffered,
    kChunkSync,
    kGroupCommit
};

struct PhysicalExtent { uint64_t offset = 0; uint64_t length = 0; };
struct FileRegion { off_t offset = 0; size_t length = 0; };

class FastDataStore {
public:
    struct PutOptions {
        std::string storageKey;
        ChunkIdentityScheme identityScheme = ChunkIdentityScheme::kCasSha256;
        ChunkChecksum checksum;
    };

    struct GroupCommitConfig {
        size_t maxBatchBytes = 8 * 1024 * 1024;
        size_t maxBatchItems = 8;
        uint64_t maxBatchDelayUs = 2000;
        size_t maxPendingBytes = 64 * 1024 * 1024;
        size_t maxPendingItems = 64;
    };

    struct Config {
        DurabilityPolicy durabilityPolicy = DurabilityPolicy::kBuffered;
        GroupCommitConfig groupCommit;
        // Deterministic failure injection for durability tests. Production
        // leaves both callbacks empty and uses fdatasync/LevelDB directly.
        std::function<int(int)> dataSyncOverride;
        std::function<bool()> failIndexSync;
        // Optional location for the local physical index. Empty preserves the
        // historical dataDirectory/physical_index layout. Deployments can put
        // the LevelDB index on a separate low-latency device while retaining
        // both durability barriers for durable policies.
        std::string physicalIndexDirectory;
    };

    struct WriteMetrics {
        uint64_t shaUpdateNanoseconds = 0;
        uint64_t pwriteNanoseconds = 0;
        uint64_t shaFinalizeNanoseconds = 0;
        uint64_t dataSyncNanoseconds = 0;
        uint64_t indexNanoseconds = 0;
        // indexNanoseconds is the complete index publication path.  These
        // fields split it into lock acquisition, WriteBatch construction, and
        // the LevelDB Write(sync=true) call for root-cause analysis.
        uint64_t indexMutexWaitNanoseconds = 0;
        uint64_t indexBatchBuildNanoseconds = 0;
        uint64_t indexWriteNanoseconds = 0;
        uint64_t dataSyncOperations = 0;
        uint64_t indexSyncOperations = 0;
        uint64_t pwriteOperations = 0;
        uint64_t pwritevOperations = 0;
        // Time from the final successful body write until the session enters
        // the durability queue.  This is intentionally separate from queue
        // and sync time so a slow completion handoff is observable.
        uint64_t writeReadyWaitNanoseconds = 0;
        // Time spent waiting for the sequencer to start this request's batch.
        // This excludes fdatasync and LevelDB publication.
        uint64_t durabilityQueueWaitNanoseconds = 0;
        // Time from the first queued request entering the coordinator until
        // the worker starts the selected batch.  This includes the configured
        // batch deadline and any preceding occupancy of the single worker.
        uint64_t durabilityBatchFormationNanoseconds = 0;
        // Lower-bound estimate of the part attributable to the worker being
        // busy with an earlier batch (batch formation minus maxBatchDelay).
        uint64_t durabilityWorkerBusyWaitNanoseconds = 0;
        uint64_t groupWaitNanoseconds = 0;
        uint64_t groupCommitNanoseconds = 0;
        // Queue depth samples captured at enqueue and immediately before the
        // worker removes this batch.  A completion snapshot alone is often
        // zero because the worker has already drained the queue by then.
        uint64_t durabilityPendingItemsAtEnqueue = 0;
        uint64_t durabilityPendingBytesAtEnqueue = 0;
        uint64_t durabilityPendingItemsAtBatchStart = 0;
        uint64_t durabilityPendingBytesAtBatchStart = 0;
        // The durability worker invokes the callback and the pipeline then
        // posts back to the owning EventLoop.  Keep both pieces visible so a
        // slow EventLoop wakeup is not mislabeled as a slow fdatasync.
        uint64_t durabilityCallbackDispatchNanoseconds = 0;
        uint64_t completionWakeupNanoseconds = 0;
        uint64_t durableSequence = 0;
        uint64_t groupBatchBytes = 0;
        uint64_t groupBatchItems = 0;
        bool durable = false;
        // In group_commit exactly one request is the accounting owner of the
        // batch-wide fdatasync/index timings.  The batch event is canonical.
        bool durabilitySyncTimingOwner = true;
        ChunkChecksumType checksumType = ChunkChecksumType::kSha256;
        uint64_t checksumUpdateNanoseconds = 0;
        uint64_t checksumFinalizeNanoseconds = 0;
    };

    struct DurabilityMetrics {
        uint64_t pendingBytes = 0;
        uint64_t peakPendingBytes = 0;
        uint64_t pendingItems = 0;
        uint64_t peakPendingItems = 0;
        uint64_t submittedItems = 0;
        uint64_t committedItems = 0;
        uint64_t committedBatches = 0;
        uint64_t failedBatches = 0;
        uint64_t dataSyncOperations = 0;
        uint64_t indexSyncOperations = 0;
        uint64_t lastDurableSequence = 0;
        uint64_t activeSyncOperations = 0;
        uint64_t pwriteOperationsWhileSync = 0;
        uint64_t pwriteBytesWhileSync = 0;
        uint64_t lastBatchBytes = 0;
        uint64_t lastBatchItems = 0;
        uint64_t lastBatchFormationNanoseconds = 0;
        uint64_t lastBatchPendingBytes = 0;
        uint64_t lastBatchPendingItems = 0;
        uint64_t lastDataSyncNanoseconds = 0;
        uint64_t lastIndexSyncNanoseconds = 0;
        uint64_t lastIndexMutexWaitNanoseconds = 0;
        uint64_t lastIndexBatchBuildNanoseconds = 0;
        uint64_t lastIndexWriteNanoseconds = 0;
        uint64_t lastBatchCommitNanoseconds = 0;
        // Zero when group_commit is disabled; group_commit currently creates
        // exactly one ordered durability worker.
        uint64_t workerCount = 0;
    };

    struct WriteSlice {
        const char* data = nullptr;
        size_t size = 0;
    };

    class WriteSession : public std::enable_shared_from_this<WriteSession> {
    public:
        using FinishCallback = std::function<void(bool success, bool alreadyExists)>;

        ~WriteSession();

        bool append(const char* bytes, size_t size);
        bool appendBatch(const std::vector<WriteSlice>& slices);
        bool finish(bool& alreadyExists);
        bool finishAsync(FinishCallback callback);
        // Called by the durability callback wrapper and by the owning
        // EventLoop callback respectively.  These are diagnostics only and
        // do not change completion semantics.
        void markDurabilityCallbackDispatched();
        void markCompletionCallbackObserved();
        void abort();
        uint64_t writtenBytes() const { return writtenBytes_; }
        uint64_t expectedSize() const { return expectedSize_; }
        const WriteMetrics& metrics() const { return metrics_; }

    private:
        friend class FastDataStore;
        WriteSession(FastDataStore* store, PutOptions options,
                     uint64_t expectedSize, uint64_t offset, bool discard);

        bool validateAndFinalize();
        bool finishImmediate(bool& alreadyExists);
        void completeGroupCommit(bool success, bool alreadyExists,
                                 uint64_t durableSequence, uint64_t batchBytes,
                                 uint64_t batchItems, uint64_t dataSyncNanoseconds,
                                 uint64_t indexNanoseconds,
                                 uint64_t indexMutexWaitNanoseconds,
                                 uint64_t indexBatchBuildNanoseconds,
                                 uint64_t indexWriteNanoseconds,
                                 uint64_t waitNanoseconds,
                                 uint64_t queueWaitNanoseconds,
                                 uint64_t commitNanoseconds,
                                 uint64_t batchFormationNanoseconds,
                                 uint64_t pendingItemsAtEnqueue,
                                 uint64_t pendingBytesAtEnqueue,
                                 uint64_t pendingItemsAtBatchStart,
                                 uint64_t pendingBytesAtBatchStart,
                                 bool ownsSyncOperations,
                                 bool dataSyncAttempted, bool indexSyncAttempted);

        FastDataStore* store_;
        PutOptions options_;
        uint64_t expectedSize_ = 0;
        uint64_t offset_ = 0;
        uint64_t writtenBytes_ = 0;
        std::unique_ptr<ChecksumContext> checksumContext_;
        bool discard_ = false;
        bool finished_ = false;
        bool finishInFlight_ = false;
        bool digestFinalized_ = false;
        bool failed_ = false;
        std::chrono::steady_clock::time_point writeReadyAt_{};
        std::chrono::steady_clock::time_point durabilityCompletedAt_{};
        std::chrono::steady_clock::time_point durabilityCallbackDispatchedAt_{};
        WriteMetrics metrics_;
    };

    explicit FastDataStore(std::string dataDirectory);
    FastDataStore(std::string dataDirectory, Config config);
    ~FastDataStore();

    bool open();
    std::shared_ptr<WriteSession> beginPut(const PutOptions& options,
                                           uint64_t expectedSize);
    std::shared_ptr<WriteSession> beginPut(const std::string& expectedHash,
                                           uint64_t expectedSize);
    bool put(const PutOptions& options, const std::string& bytes, bool& alreadyExists);
    bool put(const std::string& expectedHash, const std::string& bytes, bool& alreadyExists);
    bool get(const PutOptions& options, std::string& out) const;
    bool get(const std::string& chunkHash, std::string& out) const;
    bool getRegion(const std::string& storageKey, ChunkIdentityScheme scheme,
                   FileRegion& out) const;
    bool getRegion(const std::string& chunkHash, FileRegion& out) const;
    std::string dataFilePath() const { return dataDirectory_ + "/disk0.data"; }
    bool exists(const std::string& chunkHash) const;
    bool remove(const std::string& chunkHash, bool& removed);
    uint64_t usedBytes() const;
    uint64_t reusableBytes() const;
    DurabilityPolicy durabilityPolicy() const { return config_.durabilityPolicy; }
    DurabilityMetrics durabilityMetrics() const;
    bool canAcceptDurability(uint64_t bytes) const;

private:
    struct PendingDurability;
    class DurabilityCoordinator;

    bool enqueueGroupCommit(const std::shared_ptr<WriteSession>& session,
                            WriteSession::FinishCallback callback);
    void recordPwriteCompletion(uint64_t bytes);
    bool commitDurabilityBatch(std::vector<PendingDurability>& batch,
                               uint64_t durableSequence,
                               uint64_t& dataSyncNanoseconds,
                               uint64_t& indexNanoseconds,
                               uint64_t& indexMutexWaitNanoseconds,
                               uint64_t& indexBatchBuildNanoseconds,
                               uint64_t& indexWriteNanoseconds,
                               bool& dataSyncAttempted,
                               bool& indexSyncAttempted);
    bool findExtentLocked(const std::string& storageKey, ChunkIdentityScheme scheme,
                          PhysicalExtent& extent) const;
    bool putExtentLocked(const std::string& storageKey, ChunkIdentityScheme scheme,
                         const PhysicalExtent& extent,
                         bool sync = false);
    static std::string physicalIndexKey(const std::string& storageKey,
                                        ChunkIdentityScheme scheme);
    bool loadFreeExtentsLocked();
    bool rebuildFreeExtentsLocked();
    bool allocateExtentLocked(uint64_t length, PhysicalExtent& extent);
    bool addFreeExtentLocked(const PhysicalExtent& extent);

    std::string dataDirectory_;
    Config config_;
    int dataFd_ = -1;
    uint64_t nextOffset_ = 0;
    // This is a local physical index only. Gateway metadata remains the source
    // of truth for files and replica routes.
    std::unique_ptr<leveldb::DB> indexDb_;
    std::unique_ptr<DurabilityCoordinator> durabilityCoordinator_;
    mutable std::mutex mutex_;
    std::map<uint64_t, uint64_t> freeExtents_;
};

}  // namespace miniKV::v2
