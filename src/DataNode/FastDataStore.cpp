#include "DataNode/FastDataStore.hpp"
#include "DataNode/ChecksumProvider.hpp"
#include "utils/AsyncLogger.hpp"
#include "utils/Util.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <fcntl.h>
#include <future>
#include <leveldb/db.h>
#include <leveldb/iterator.h>
#include <leveldb/write_batch.h>
#include <limits>
#include <sys/stat.h>
#include <sys/uio.h>
#include <thread>
#include <unordered_set>
#include <unistd.h>

namespace miniKV::datanode {

using namespace util;

namespace {

using Clock = std::chrono::steady_clock;

uint64_t elapsedNanoseconds(Clock::time_point started, Clock::time_point finished)
{
    if(finished <= started) return 0;
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        finished - started).count());
}

}  // namespace

struct FastDataStore::PendingDurability {
    std::shared_ptr<WriteSession> session;
    WriteSession::FinishCallback callback;
    Clock::time_point queuedAt;
    uint64_t writeSequence = 0;
    bool alreadyExists = false;
};

class FastDataStore::DurabilityCoordinator {
public:
    DurabilityCoordinator(FastDataStore& store, GroupCommitConfig config)
        : store_(store), config_(config), worker_([this] { workerMain(); }) {}

    ~DurabilityCoordinator() { stop(); }

    bool enqueue(PendingDurability request)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const uint64_t bytes = request.session->expectedSize();
        if(stopping_ || metrics_.pendingItems >= config_.maxPendingItems ||
           bytes > config_.maxPendingBytes - std::min<uint64_t>(
               metrics_.pendingBytes, config_.maxPendingBytes)) {
            return false;
        }
        request.writeSequence = ++nextWriteSequence_;
        request.queuedAt = Clock::now();
        pending_.push_back(std::move(request));
        metrics_.pendingBytes += bytes;
        ++metrics_.pendingItems;
        metrics_.peakPendingBytes = std::max(metrics_.peakPendingBytes,
                                             metrics_.pendingBytes);
        metrics_.peakPendingItems = std::max(metrics_.peakPendingItems,
                                             metrics_.pendingItems);
        ++metrics_.submittedItems;
        cv_.notify_one();
        return true;
    }

    DurabilityMetrics metrics() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return metrics_;
    }

    bool canAccept(uint64_t bytes) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return !stopping_ && metrics_.pendingItems < config_.maxPendingItems &&
               metrics_.pendingBytes <= config_.maxPendingBytes &&
               bytes <= config_.maxPendingBytes - metrics_.pendingBytes;
    }

    void recordPwriteCompletion(uint64_t bytes)
    {
        if(activeSyncOperations_.load(std::memory_order_acquire) == 0) return;
        std::lock_guard<std::mutex> lock(mutex_);
        if(activeSyncOperations_.load(std::memory_order_relaxed) == 0) return;
        ++metrics_.pwriteOperationsWhileSync;
        metrics_.pwriteBytesWhileSync += bytes;
    }

    void stop()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(stopping_) return;
            stopping_ = true;
        }
        cv_.notify_all();
        if(worker_.joinable()) worker_.join();
    }

private:
    bool thresholdReachedLocked() const
    {
        return metrics_.pendingBytes >= config_.maxBatchBytes ||
               pending_.size() >= config_.maxBatchItems;
    }

    void workerMain()
    {
        for(;;) {
            std::vector<PendingDurability> batch;
            uint64_t batchBytes = 0;
            uint64_t durableSequence = 0;
            std::string flushReason = "deadline";
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return stopping_ || !pending_.empty(); });
                if(stopping_ && pending_.empty()) return;

                while(!stopping_ && !thresholdReachedLocked()) {
                    const auto deadline = pending_.front().queuedAt +
                        std::chrono::microseconds(config_.maxBatchDelayUs);
                    if(cv_.wait_until(lock, deadline) == std::cv_status::timeout) break;
                    if(thresholdReachedLocked()) break;
                }

                if(stopping_) {
                    flushReason = "shutdown";
                } else if(metrics_.pendingBytes >= config_.maxBatchBytes) {
                    flushReason = "bytes";
                } else if(pending_.size() >= config_.maxBatchItems) {
                    flushReason = "items";
                }

                while(!pending_.empty() && batch.size() < config_.maxBatchItems) {
                    const uint64_t itemBytes = pending_.front().session->expectedSize();
                    if(!batch.empty() && batchBytes + itemBytes > config_.maxBatchBytes) break;
                    batchBytes += itemBytes;
                    durableSequence = pending_.front().writeSequence;
                    batch.push_back(std::move(pending_.front()));
                    pending_.pop_front();
                }
                if(batch.empty()) continue;
            }

            const auto commitStarted = Clock::now();
            const uint64_t batchFormationNanoseconds = elapsedNanoseconds(
                batch.front().queuedAt, commitStarted);
            uint64_t dataSyncNanoseconds = 0;
            uint64_t indexNanoseconds = 0;
            bool dataSyncAttempted = false;
            bool indexSyncAttempted = false;
            activeSyncOperations_.fetch_add(1, std::memory_order_release);
            {
                std::lock_guard<std::mutex> lock(mutex_);
                metrics_.activeSyncOperations = activeSyncOperations_.load(
                    std::memory_order_relaxed);
            }
            const bool success = store_.commitDurabilityBatch(
                batch, durableSequence, dataSyncNanoseconds, indexNanoseconds,
                dataSyncAttempted, indexSyncAttempted);
            activeSyncOperations_.fetch_sub(1, std::memory_order_release);
            const auto committedAt = Clock::now();
            const uint64_t commitNanoseconds = elapsedNanoseconds(commitStarted, committedAt);

            {
                std::lock_guard<std::mutex> lock(mutex_);
                metrics_.pendingBytes -= batchBytes;
                metrics_.pendingItems -= batch.size();
                if(dataSyncAttempted) ++metrics_.dataSyncOperations;
                if(indexSyncAttempted) ++metrics_.indexSyncOperations;
                metrics_.activeSyncOperations = activeSyncOperations_.load(
                    std::memory_order_relaxed);
                metrics_.lastBatchBytes = batchBytes;
                metrics_.lastBatchItems = batch.size();
                metrics_.lastBatchFormationNanoseconds = batchFormationNanoseconds;
                metrics_.lastDataSyncNanoseconds = dataSyncNanoseconds;
                metrics_.lastIndexSyncNanoseconds = indexNanoseconds;
                metrics_.lastBatchCommitNanoseconds = commitNanoseconds;
                if(success) {
                    ++metrics_.committedBatches;
                    metrics_.committedItems += batch.size();
                    metrics_.lastDurableSequence = durableSequence;
                } else {
                    ++metrics_.failedBatches;
                }
            }

            miniKV::utils::logInfo(
                "event=durability_batch_complete"
                " durable_sequence=" + std::to_string(durableSequence) +
                " flush_reason=" + flushReason +
                " success=" + std::string(success ? "true" : "false") +
                " batch_bytes=" + std::to_string(batchBytes) +
                " batch_items=" + std::to_string(batch.size()) +
                " batch_formation_us=" + std::to_string(batchFormationNanoseconds / 1000ULL) +
                " data_sync_us=" + std::to_string(dataSyncNanoseconds / 1000ULL) +
                " index_sync_us=" + std::to_string(indexNanoseconds / 1000ULL) +
                " batch_commit_us=" + std::to_string(commitNanoseconds / 1000ULL));

            for(size_t i = 0; i < batch.size(); ++i) {
                PendingDurability& request = batch[i];
                const uint64_t queueWaitNanoseconds = elapsedNanoseconds(
                    request.queuedAt, commitStarted);
                const uint64_t waitNanoseconds = elapsedNanoseconds(
                    request.queuedAt, committedAt);
                request.session->completeGroupCommit(
                    success, request.alreadyExists, durableSequence, batchBytes,
                    batch.size(), dataSyncNanoseconds, indexNanoseconds,
                    waitNanoseconds, queueWaitNanoseconds, commitNanoseconds, i == 0,
                    dataSyncAttempted, indexSyncAttempted);
                if(request.callback) request.callback(success, request.alreadyExists);
            }
        }
    }

    FastDataStore& store_;
    GroupCommitConfig config_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<PendingDurability> pending_;
    DurabilityMetrics metrics_;
    uint64_t nextWriteSequence_ = 0;
    std::atomic<uint64_t> activeSyncOperations_{0};
    bool stopping_ = false;
    std::thread worker_;
};

FastDataStore::FastDataStore(std::string dataDirectory)
    : FastDataStore(std::move(dataDirectory), Config{}) {}

FastDataStore::FastDataStore(std::string dataDirectory, Config config)
    : dataDirectory_(std::move(dataDirectory)), config_(config) {}

FastDataStore::~FastDataStore() {
    durabilityCoordinator_.reset();
    indexDb_.reset();
    if (dataFd_ >= 0) ::close(dataFd_);
}

FastDataStore::WriteSession::WriteSession(FastDataStore* store,
                                          PutOptions options,
                                          uint64_t expectedSize,
                                          uint64_t offset,
                                          bool discard)
    : store_(store), options_(std::move(options)), expectedSize_(expectedSize),
      offset_(offset), discard_(discard) {
    metrics_.checksumType = options_.checksum.type;
    checksumContext_ = checksumProvider(options_.checksum.type).create();
    if(checksumContext_ == nullptr) failed_ = true;
}

FastDataStore::WriteSession::~WriteSession() {
    abort();
}

bool FastDataStore::WriteSession::append(const char* bytes, size_t size) {
    if (failed_ || finished_ || bytes == nullptr || size == 0 ||
        writtenBytes_ + size > expectedSize_) {
        failed_ = true;
        return false;
    }
    const auto checksumStarted = Clock::now();
    const bool checksumResult = checksumContext_->update(bytes, size);
    const uint64_t checksumElapsed = elapsedNanoseconds(checksumStarted, Clock::now());
    metrics_.checksumUpdateNanoseconds += checksumElapsed;
    if(options_.checksum.type == ChunkChecksumType::kSha256) {
        metrics_.shaUpdateNanoseconds += checksumElapsed;
    }
    if (!checksumResult) {
        failed_ = true;
        return false;
    }
    if (!discard_) {
        const auto writeStarted = Clock::now();
        size_t written = 0;
        while (written < size) {
            const ssize_t n = ::pwrite(store_->dataFd_, bytes + written, size - written,
                                       static_cast<off_t>(offset_ + writtenBytes_ + written));
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) {
                failed_ = true;
                return false;
            }
            ++metrics_.pwriteOperations;
            written += static_cast<size_t>(n);
        }
        metrics_.pwriteNanoseconds += elapsedNanoseconds(writeStarted, Clock::now());
        store_->recordPwriteCompletion(written);
    }
    writtenBytes_ += size;
    writeReadyAt_ = Clock::now();
    return true;
}

bool FastDataStore::WriteSession::appendBatch(const std::vector<WriteSlice>& slices) {
    if (failed_ || finished_ || slices.empty()) {
        failed_ = true;
        return false;
    }

    size_t totalSize = 0;
    for (const WriteSlice& slice : slices) {
        if (slice.data == nullptr || slice.size == 0 ||
            slice.size > std::numeric_limits<size_t>::max() - totalSize) {
            failed_ = true;
            return false;
        }
        totalSize += slice.size;
    }
    if (writtenBytes_ > expectedSize_ || totalSize > expectedSize_ - writtenBytes_) {
        failed_ = true;
        return false;
    }

    const auto checksumStarted = Clock::now();
    for (const WriteSlice& slice : slices) {
        if (!checksumContext_->update(slice.data, slice.size)) {
            const uint64_t elapsed = elapsedNanoseconds(checksumStarted, Clock::now());
            metrics_.checksumUpdateNanoseconds += elapsed;
            if(options_.checksum.type == ChunkChecksumType::kSha256) {
                metrics_.shaUpdateNanoseconds += elapsed;
            }
            failed_ = true;
            return false;
        }
    }
    const uint64_t checksumElapsed = elapsedNanoseconds(checksumStarted, Clock::now());
    metrics_.checksumUpdateNanoseconds += checksumElapsed;
    if(options_.checksum.type == ChunkChecksumType::kSha256) {
        metrics_.shaUpdateNanoseconds += checksumElapsed;
    }

    if (!discard_) {
        std::vector<iovec> vectors;
        vectors.reserve(slices.size());
        for (const WriteSlice& slice : slices) {
            iovec vector{};
            vector.iov_base = const_cast<char*>(slice.data);
            vector.iov_len = slice.size;
            vectors.push_back(vector);
        }

        const long configuredMaxIov = ::sysconf(_SC_IOV_MAX);
        const size_t maxIov = configuredMaxIov > 0
            ? static_cast<size_t>(configuredMaxIov) : static_cast<size_t>(1024);
        const auto writeStarted = Clock::now();
        size_t vectorIndex = 0;
        uint64_t batchWritten = 0;
        while (vectorIndex < vectors.size()) {
            const size_t vectorCount = std::min(maxIov, vectors.size() - vectorIndex);
            const ssize_t n = ::pwritev(store_->dataFd_, vectors.data() + vectorIndex,
                                        static_cast<int>(vectorCount),
                                        static_cast<off_t>(offset_ + writtenBytes_ + batchWritten));
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) {
                metrics_.pwriteNanoseconds += elapsedNanoseconds(writeStarted, Clock::now());
                failed_ = true;
                return false;
            }
            ++metrics_.pwritevOperations;
            size_t consumed = static_cast<size_t>(n);
            batchWritten += consumed;
            while (consumed > 0 && vectorIndex < vectors.size()) {
                iovec& current = vectors[vectorIndex];
                if (consumed < current.iov_len) {
                    current.iov_base = static_cast<char*>(current.iov_base) + consumed;
                    current.iov_len -= consumed;
                    consumed = 0;
                } else {
                    consumed -= current.iov_len;
                    ++vectorIndex;
                }
            }
        }
        metrics_.pwriteNanoseconds += elapsedNanoseconds(writeStarted, Clock::now());
        if (batchWritten != totalSize) {
            failed_ = true;
            return false;
        }
        store_->recordPwriteCompletion(batchWritten);
    }

    writtenBytes_ += totalSize;
    writeReadyAt_ = Clock::now();
    return true;
}

bool FastDataStore::WriteSession::validateAndFinalize() {
    if (finished_ || finishInFlight_ || failed_ || digestFinalized_ ||
        writtenBytes_ != expectedSize_) return false;
    const auto finalizeStarted = Clock::now();
    std::string actualChecksum;
    if (!checksumContext_->finalizeHex(actualChecksum)) {
        failed_ = true;
        return false;
    }
    const uint64_t finalizeElapsed = elapsedNanoseconds(finalizeStarted, Clock::now());
    metrics_.checksumFinalizeNanoseconds += finalizeElapsed;
    if(options_.checksum.type == ChunkChecksumType::kSha256) {
        metrics_.shaFinalizeNanoseconds += finalizeElapsed;
    }
    if (actualChecksum != options_.checksum.wholeDigest) {
        failed_ = true;
        return false;
    }
    digestFinalized_ = true;
    return true;
}

bool FastDataStore::WriteSession::finishImmediate(bool& alreadyExists) {
    alreadyExists = false;
    if (!validateAndFinalize()) return false;

    const bool chunkSync = store_->config_.durabilityPolicy == DurabilityPolicy::kChunkSync;
    if (chunkSync && discard_) {
        // A buffered object from an earlier run must not be promoted to a durable
        // acknowledgement without validating the bytes that its index references.
        std::string existingBytes;
        if (!store_->get(options_, existingBytes)) {
            failed_ = true;
            return false;
        }
    }
    if (chunkSync) {
        const auto syncStarted = Clock::now();
        ++metrics_.dataSyncOperations;
        if (::fdatasync(store_->dataFd_) != 0) {
            metrics_.dataSyncNanoseconds += elapsedNanoseconds(syncStarted, Clock::now());
            failed_ = true;
            return false;
        }
        metrics_.dataSyncNanoseconds += elapsedNanoseconds(syncStarted, Clock::now());
    }

    const auto indexStarted = Clock::now();
    std::lock_guard<std::mutex> lock(store_->mutex_);
    PhysicalExtent existing;
    const bool existedAtFinish = store_->findExtentLocked(
        options_.storageKey, options_.identityScheme, existing);
    if (chunkSync && !discard_) {
        // Publish the extent whose bytes were covered by the fdatasync above. A
        // concurrent identical upload may leave its tentative extent orphaned;
        // startup orphan reclamation is a separate 3.0 task.
        if (!store_->putExtentLocked(options_.storageKey, options_.identityScheme,
                                     {offset_, expectedSize_}, true)) {
            failed_ = true;
            return false;
        }
        alreadyExists = existedAtFinish;
        ++metrics_.indexSyncOperations;
    } else if (existedAtFinish) {
        alreadyExists = true;
        if (chunkSync) {
            if (!store_->putExtentLocked(options_.storageKey, options_.identityScheme,
                                         existing, true)) {
                failed_ = true;
                return false;
            }
            ++metrics_.indexSyncOperations;
        }
    } else if (!discard_) {
        if (!store_->putExtentLocked(options_.storageKey, options_.identityScheme,
                                     {offset_, expectedSize_})) {
            failed_ = true;
            return false;
        }
    } else {
        // The request body was intentionally discarded because the extent
        // existed at beginPut(). If a concurrent delete removed the index,
        // acknowledging here would publish a chunk that is no longer readable.
        failed_ = true;
        return false;
    }
    metrics_.indexNanoseconds += elapsedNanoseconds(indexStarted, Clock::now());
    metrics_.durable = chunkSync;
    finished_ = true;
    return true;
}

bool FastDataStore::WriteSession::finish(bool& alreadyExists) {
    if(store_->config_.durabilityPolicy != DurabilityPolicy::kGroupCommit) {
        return finishImmediate(alreadyExists);
    }

    std::mutex waitMutex;
    std::condition_variable waitCv;
    bool completed = false;
    bool success = false;
    bool existed = false;
    if(!finishAsync([&](bool callbackSuccess, bool callbackAlreadyExists) {
        {
            std::lock_guard<std::mutex> lock(waitMutex);
            success = callbackSuccess;
            existed = callbackAlreadyExists;
            completed = true;
        }
        waitCv.notify_one();
    })) return false;

    std::unique_lock<std::mutex> lock(waitMutex);
    waitCv.wait(lock, [&] { return completed; });
    alreadyExists = existed;
    return success;
}

bool FastDataStore::WriteSession::finishAsync(FinishCallback callback) {
    if(!callback) return false;
    if(store_->config_.durabilityPolicy != DurabilityPolicy::kGroupCommit) {
        bool alreadyExists = false;
        const bool success = finishImmediate(alreadyExists);
        callback(success, alreadyExists);
        return success;
    }
    if(!validateAndFinalize()) {
        callback(false, false);
        return false;
    }
    if(discard_) {
        // An object published by an earlier buffered run must be read and
        // verified before it can join a durable group acknowledgement.
        std::string existingBytes;
        if(!store_->get(options_, existingBytes)) {
            failed_ = true;
            callback(false, false);
            return false;
        }
    }
    finishInFlight_ = true;
    const auto enqueueStarted = Clock::now();
    if(!store_->enqueueGroupCommit(shared_from_this(), std::move(callback))) {
        finishInFlight_ = false;
        failed_ = true;
        return false;
    }
    const auto enqueuedAt = Clock::now();
    if(writeReadyAt_ != Clock::time_point{}) {
        metrics_.writeReadyWaitNanoseconds = elapsedNanoseconds(writeReadyAt_, enqueuedAt);
    } else {
        metrics_.writeReadyWaitNanoseconds = elapsedNanoseconds(enqueueStarted, enqueuedAt);
    }
    return true;
}

void FastDataStore::WriteSession::completeGroupCommit(
    bool success, bool alreadyExists, uint64_t durableSequence,
    uint64_t batchBytes, uint64_t batchItems, uint64_t dataSyncNanoseconds,
    uint64_t indexNanoseconds, uint64_t waitNanoseconds,
    uint64_t queueWaitNanoseconds, uint64_t commitNanoseconds,
    bool ownsSyncOperations,
    bool dataSyncAttempted, bool indexSyncAttempted) {
    (void)alreadyExists;
    finishInFlight_ = false;
    metrics_.durabilityQueueWaitNanoseconds = queueWaitNanoseconds;
    metrics_.groupWaitNanoseconds = waitNanoseconds;
    metrics_.groupCommitNanoseconds = commitNanoseconds;
    metrics_.durableSequence = durableSequence;
    metrics_.groupBatchBytes = batchBytes;
    metrics_.groupBatchItems = batchItems;
    metrics_.durabilitySyncTimingOwner = ownsSyncOperations;
    if(ownsSyncOperations) {
        metrics_.dataSyncNanoseconds = dataSyncNanoseconds;
        metrics_.indexNanoseconds = indexNanoseconds;
        metrics_.dataSyncOperations = dataSyncAttempted ? 1 : 0;
        metrics_.indexSyncOperations = indexSyncAttempted ? 1 : 0;
    }
    if(success) {
        metrics_.durable = true;
        finished_ = true;
    } else {
        failed_ = true;
    }
}

void FastDataStore::WriteSession::abort() {
    if (!finished_ && !finishInFlight_) failed_ = true;
}

bool FastDataStore::open() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::error_code ec;
    std::filesystem::create_directories(dataDirectory_, ec);
    if (ec) return false;
    dataFd_ = ::open((dataDirectory_ + "/disk0.data").c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
    if (dataFd_ < 0) return false;
    struct stat st{};
    if (::fstat(dataFd_, &st) != 0) return false;
    nextOffset_ = static_cast<uint64_t>(st.st_size);

    leveldb::Options options;
    options.create_if_missing = true;
    options.paranoid_checks = true;
    leveldb::DB* rawDb = nullptr;
    const leveldb::Status status = leveldb::DB::Open(
        options, dataDirectory_ + "/physical_index", &rawDb);
    if (!status.ok()) {
        ::close(dataFd_);
        dataFd_ = -1;
        return false;
    }
    indexDb_.reset(rawDb);
    if(!rebuildFreeExtentsLocked()) return false;
    if(config_.durabilityPolicy == DurabilityPolicy::kGroupCommit) {
        const GroupCommitConfig& group = config_.groupCommit;
        if(group.maxBatchBytes == 0 || group.maxBatchItems == 0 ||
           group.maxPendingBytes < group.maxBatchBytes ||
           group.maxPendingItems < group.maxBatchItems) {
            return false;
        }
        durabilityCoordinator_ = std::make_unique<DurabilityCoordinator>(*this, group);
    }
    return true;
}

std::shared_ptr<FastDataStore::WriteSession> FastDataStore::beginPut(
    const PutOptions& options, uint64_t expectedSize) {
    if (options.storageKey.empty() || expectedSize == 0 || dataFd_ < 0 ||
        !isSupportedChecksumType(options.checksum.type) ||
        !validateChecksumDigest(options.checksum.type, options.checksum.wholeDigest) ||
        (options.identityScheme == ChunkIdentityScheme::kCasSha256 &&
         (options.checksum.type != ChunkChecksumType::kSha256 ||
          options.storageKey != options.checksum.wholeDigest)) ||
        !canAcceptDurability(expectedSize)) return nullptr;
    std::lock_guard<std::mutex> lock(mutex_);
    PhysicalExtent existing;
    const bool alreadyPresent = findExtentLocked(
        options.storageKey, options.identityScheme, existing);
    PhysicalExtent extent;
    if (!alreadyPresent) {
        if (!allocateExtentLocked(expectedSize, extent)) {
            if (nextOffset_ > std::numeric_limits<uint64_t>::max() - expectedSize) return nullptr;
            extent = {nextOffset_, expectedSize};
            nextOffset_ += expectedSize;
        }
    }
    return std::shared_ptr<WriteSession>(
        new WriteSession(this, options, expectedSize, extent.offset, alreadyPresent));
}

std::shared_ptr<FastDataStore::WriteSession> FastDataStore::beginPut(
    const std::string& expectedHash, uint64_t expectedSize) {
    PutOptions options;
    options.storageKey = expectedHash;
    options.identityScheme = ChunkIdentityScheme::kCasSha256;
    options.checksum.type = ChunkChecksumType::kSha256;
    options.checksum.wholeDigest = expectedHash;
    return beginPut(options, expectedSize);
}

bool FastDataStore::put(const PutOptions& options, const std::string& bytes,
                        bool& alreadyExists) {
    auto session = beginPut(options, bytes.size());
    return session != nullptr && session->append(bytes.data(), bytes.size()) &&
           session->finish(alreadyExists);
}

bool FastDataStore::put(const std::string& expectedHash, const std::string& bytes, bool& alreadyExists) {
    auto session = beginPut(expectedHash, bytes.size());
    return session != nullptr && session->append(bytes.data(), bytes.size()) &&
           session->finish(alreadyExists);
}

bool FastDataStore::enqueueGroupCommit(
    const std::shared_ptr<WriteSession>& session,
    WriteSession::FinishCallback callback) {
    if(durabilityCoordinator_ == nullptr || session == nullptr || !callback) return false;
    PendingDurability request;
    request.session = session;
    request.callback = std::move(callback);
    if(durabilityCoordinator_->enqueue(request)) return true;
    if(request.callback) request.callback(false, false);
    return false;
}

void FastDataStore::recordPwriteCompletion(uint64_t bytes)
{
    if(durabilityCoordinator_ != nullptr) {
        durabilityCoordinator_->recordPwriteCompletion(bytes);
    }
}

bool FastDataStore::commitDurabilityBatch(
    std::vector<PendingDurability>& batch, uint64_t durableSequence,
    uint64_t& dataSyncNanoseconds, uint64_t& indexNanoseconds,
    bool& dataSyncAttempted, bool& indexSyncAttempted) {
    (void)durableSequence;
    if(batch.empty() || dataFd_ < 0 || indexDb_ == nullptr) return false;

    const auto syncStarted = Clock::now();
    dataSyncAttempted = true;
    const int syncResult = config_.dataSyncOverride
        ? config_.dataSyncOverride(dataFd_) : ::fdatasync(dataFd_);
    dataSyncNanoseconds = elapsedNanoseconds(syncStarted, Clock::now());
    if(syncResult != 0) return false;

    const auto indexStarted = Clock::now();
    bool indexSuccess = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        leveldb::WriteBatch writeBatch;
        std::unordered_set<std::string> publishedInBatch;
        for(PendingDurability& request : batch) {
            WriteSession& session = *request.session;
            PhysicalExtent existing;
            const std::string indexKey = physicalIndexKey(
                session.options_.storageKey, session.options_.identityScheme);
            const bool existed = findExtentLocked(
                session.options_.storageKey, session.options_.identityScheme, existing);
            request.alreadyExists = existed ||
                publishedInBatch.find(indexKey) != publishedInBatch.end();
            PhysicalExtent extent;
            if(session.discard_) {
                if(!existed) {
                    indexNanoseconds = elapsedNanoseconds(indexStarted, Clock::now());
                    return false;
                }
                extent = existing;
            } else {
                extent = {session.offset_, session.expectedSize_};
            }
            if(extent.length == 0) {
                indexNanoseconds = elapsedNanoseconds(indexStarted, Clock::now());
                return false;
            }
            writeBatch.Put(indexKey,
                           std::to_string(extent.offset) + ":" +
                           std::to_string(extent.length));
            publishedInBatch.insert(indexKey);
        }

        indexSyncAttempted = true;
        if(!(config_.failIndexSync && config_.failIndexSync())) {
            leveldb::WriteOptions options;
            options.sync = true;
            indexSuccess = indexDb_->Write(options, &writeBatch).ok();
        }
    }
    indexNanoseconds = elapsedNanoseconds(indexStarted, Clock::now());
    return indexSuccess;
}

FastDataStore::DurabilityMetrics FastDataStore::durabilityMetrics() const {
    if(durabilityCoordinator_ == nullptr) return {};
    return durabilityCoordinator_->metrics();
}

bool FastDataStore::canAcceptDurability(uint64_t bytes) const {
    if(config_.durabilityPolicy != DurabilityPolicy::kGroupCommit) return true;
    return durabilityCoordinator_ != nullptr && durabilityCoordinator_->canAccept(bytes);
}

bool FastDataStore::get(const PutOptions& options, std::string& out) const {
    if(options.storageKey.empty() ||
       !validateChecksumDigest(options.checksum.type, options.checksum.wholeDigest)) return false;
    PhysicalExtent extent;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!findExtentLocked(options.storageKey, options.identityScheme, extent)) return false;
    }
    out.resize(extent.length);
    size_t received = 0;
    while (received < out.size()) {
        const ssize_t n = ::pread(dataFd_, out.data() + received, out.size() - received,
                                  static_cast<off_t>(extent.offset + received));
        if (n <= 0) return false;
        received += static_cast<size_t>(n);
    }
    auto context = checksumProvider(options.checksum.type).create();
    std::string actual;
    return context != nullptr && context->update(out.data(), out.size()) &&
           context->finalizeHex(actual) && actual == options.checksum.wholeDigest;
}

bool FastDataStore::get(const std::string& chunkHash, std::string& out) const {
    PutOptions options;
    options.storageKey = chunkHash;
    options.identityScheme = ChunkIdentityScheme::kCasSha256;
    options.checksum.type = ChunkChecksumType::kSha256;
    options.checksum.wholeDigest = chunkHash;
    return get(options, out);
}

bool FastDataStore::getRegion(const std::string& storageKey, ChunkIdentityScheme scheme,
                              FileRegion& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    PhysicalExtent extent;
    if(!findExtentLocked(storageKey, scheme, extent)) return false;
    out.offset = static_cast<off_t>(extent.offset);
    out.length = static_cast<size_t>(extent.length);
    return true;
}

bool FastDataStore::getRegion(const std::string& chunkHash, FileRegion& out) const {
    if(getRegion(chunkHash, ChunkIdentityScheme::kCasSha256, out)) return true;
    return getRegion(chunkHash, ChunkIdentityScheme::kOpaqueChunkId, out);
}

bool FastDataStore::exists(const std::string& chunkHash) const {
    std::lock_guard<std::mutex> lock(mutex_);
    PhysicalExtent extent;
    return findExtentLocked(chunkHash, ChunkIdentityScheme::kCasSha256, extent) ||
           findExtentLocked(chunkHash, ChunkIdentityScheme::kOpaqueChunkId, extent);
}
uint64_t FastDataStore::usedBytes() const { std::lock_guard<std::mutex> lock(mutex_); return nextOffset_; }

uint64_t FastDataStore::reusableBytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t total = 0;
    for (const auto& [offset, length] : freeExtents_) {
        (void)offset;
        if (total > std::numeric_limits<uint64_t>::max() - length) {
            return std::numeric_limits<uint64_t>::max();
        }
        total += length;
    }
    return total;
}

bool FastDataStore::remove(const std::string& chunkHash, bool& removed) {
    removed = false;
    if (chunkHash.empty() || indexDb_ == nullptr) return false;

    std::lock_guard<std::mutex> lock(mutex_);
    PhysicalExtent extent;
    ChunkIdentityScheme scheme = ChunkIdentityScheme::kCasSha256;
    if (!findExtentLocked(chunkHash, scheme, extent)) {
        scheme = ChunkIdentityScheme::kOpaqueChunkId;
        if(!findExtentLocked(chunkHash, scheme, extent)) return true;
    }

    std::map<uint64_t, uint64_t> nextFree = freeExtents_;
    uint64_t mergedOffset = extent.offset;
    uint64_t mergedLength = extent.length;
    auto next = nextFree.lower_bound(mergedOffset);
    if (next != nextFree.begin()) {
        const auto previous = std::prev(next);
        if (previous->first <= std::numeric_limits<uint64_t>::max() - previous->second &&
            previous->first + previous->second == mergedOffset) {
            mergedOffset = previous->first;
            mergedLength += previous->second;
            nextFree.erase(previous);
        }
    }
    next = nextFree.lower_bound(mergedOffset);
    if (next != nextFree.end() &&
        mergedOffset <= std::numeric_limits<uint64_t>::max() - mergedLength &&
        mergedOffset + mergedLength == next->first) {
        mergedLength += next->second;
        nextFree.erase(next);
    }
    nextFree[mergedOffset] = mergedLength;

    leveldb::WriteBatch batch;
    batch.Delete(physicalIndexKey(chunkHash, scheme));
    for (const auto& [offset, length] : freeExtents_) {
        if (nextFree.find(offset) == nextFree.end()) {
            batch.Delete("free:" + std::to_string(offset));
        }
    }
    for (const auto& [offset, length] : nextFree) {
        const auto current = freeExtents_.find(offset);
        if (current == freeExtents_.end() || current->second != length) {
            batch.Put("free:" + std::to_string(offset), std::to_string(length));
        }
    }
    if (!indexDb_->Write(leveldb::WriteOptions(), &batch).ok()) return false;
    freeExtents_ = std::move(nextFree);
    removed = true;
    return true;
}

std::string FastDataStore::physicalIndexKey(const std::string& storageKey,
                                            ChunkIdentityScheme scheme) {
    return std::string(scheme == ChunkIdentityScheme::kOpaqueChunkId ? "o:" : "e:") +
           storageKey;
}

bool FastDataStore::findExtentLocked(const std::string& storageKey,
                                     ChunkIdentityScheme scheme,
                                     PhysicalExtent& extent) const {
    if (indexDb_ == nullptr) return false;
    std::string value;
    const leveldb::Status status = indexDb_->Get(
        leveldb::ReadOptions(), physicalIndexKey(storageKey, scheme), &value);
    if (!status.ok()) return false;

    const size_t separator = value.find(':');
    if (separator == std::string::npos || value.find(':', separator + 1) != std::string::npos) {
        return false;
    }
    try {
        const uint64_t offset = std::stoull(value.substr(0, separator));
        const uint64_t length = std::stoull(value.substr(separator + 1));
        if (length == 0) return false;
        extent = {offset, length};
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool FastDataStore::putExtentLocked(const std::string& storageKey,
                                    ChunkIdentityScheme scheme,
                                    const PhysicalExtent& extent, bool sync) {
    if (indexDb_ == nullptr || extent.length == 0) return false;
    const std::string value = std::to_string(extent.offset) + ":" + std::to_string(extent.length);
    leveldb::WriteOptions options;
    options.sync = sync;
    return indexDb_->Put(options, physicalIndexKey(storageKey, scheme), value).ok();
}

bool FastDataStore::loadFreeExtentsLocked() {
    if (indexDb_ == nullptr) return false;
    freeExtents_.clear();
    std::unique_ptr<leveldb::Iterator> it(indexDb_->NewIterator(leveldb::ReadOptions()));
    for (it->Seek("free:"); it->Valid() && it->key().ToString().rfind("free:", 0) == 0; it->Next()) {
        try {
            const std::string key = it->key().ToString();
            const uint64_t offset = std::stoull(key.substr(5));
            const uint64_t length = std::stoull(it->value().ToString());
            if (length == 0 || offset > std::numeric_limits<uint64_t>::max() - length ||
                !freeExtents_.emplace(offset, length).second) {
                return false;
            }
        } catch (const std::exception&) {
            return false;
        }
    }
    if (!it->status().ok()) return false;
    for (auto it = freeExtents_.begin(); it != freeExtents_.end();) {
        const auto next = std::next(it);
        if (next != freeExtents_.end() && it->first + it->second >= next->first) return false;
        it = next;
    }
    return true;
}

bool FastDataStore::rebuildFreeExtentsLocked() {
    if(indexDb_ == nullptr) return false;
    std::vector<PhysicalExtent> liveExtents;
    std::unique_ptr<leveldb::Iterator> live(indexDb_->NewIterator(leveldb::ReadOptions()));
    for(live->SeekToFirst(); live->Valid(); live->Next()) {
        const std::string key = live->key().ToString();
        if(key.rfind("e:", 0) != 0 && key.rfind("o:", 0) != 0) continue;
        const std::string value = live->value().ToString();
        const size_t separator = value.find(':');
        if(separator == std::string::npos ||
           value.find(':', separator + 1) != std::string::npos) return false;
        try {
            const uint64_t offset = std::stoull(value.substr(0, separator));
            const uint64_t length = std::stoull(value.substr(separator + 1));
            if(length == 0 || offset > std::numeric_limits<uint64_t>::max() - length ||
               offset + length > nextOffset_) return false;
            liveExtents.push_back({offset, length});
        } catch(const std::exception&) {
            return false;
        }
    }
    if(!live->status().ok()) return false;
    std::sort(liveExtents.begin(), liveExtents.end(),
              [](const PhysicalExtent& left, const PhysicalExtent& right) {
                  return left.offset < right.offset;
              });

    std::map<uint64_t, uint64_t> rebuilt;
    uint64_t cursor = 0;
    for(const PhysicalExtent& extent : liveExtents) {
        if(extent.offset < cursor) return false;
        if(extent.offset > cursor) rebuilt[cursor] = extent.offset - cursor;
        cursor = extent.offset + extent.length;
    }
    if(cursor < nextOffset_) rebuilt[cursor] = nextOffset_ - cursor;

    leveldb::WriteBatch batch;
    std::unique_ptr<leveldb::Iterator> free(indexDb_->NewIterator(leveldb::ReadOptions()));
    for(free->Seek("free:"); free->Valid() && free->key().ToString().rfind("free:", 0) == 0;
        free->Next()) {
        batch.Delete(free->key());
    }
    if(!free->status().ok()) return false;
    for(const auto& [offset, length] : rebuilt) {
        batch.Put("free:" + std::to_string(offset), std::to_string(length));
    }
    leveldb::WriteOptions options;
    options.sync = config_.durabilityPolicy != DurabilityPolicy::kBuffered;
    if(!indexDb_->Write(options, &batch).ok()) return false;
    freeExtents_ = std::move(rebuilt);
    return true;
}

bool FastDataStore::allocateExtentLocked(uint64_t length, PhysicalExtent& extent) {
    if (indexDb_ == nullptr || length == 0) return false;
    auto selected = freeExtents_.end();
    for (auto it = freeExtents_.begin(); it != freeExtents_.end(); ++it) {
        if (it->second >= length &&
            (selected == freeExtents_.end() || it->second < selected->second)) {
            selected = it;
        }
    }
    if (selected == freeExtents_.end()) return false;

    const uint64_t offset = selected->first;
    const uint64_t available = selected->second;
    leveldb::WriteBatch batch;
    batch.Delete("free:" + std::to_string(offset));
    if (available > length) {
        batch.Put("free:" + std::to_string(offset + length), std::to_string(available - length));
    }
    if (!indexDb_->Write(leveldb::WriteOptions(), &batch).ok()) return false;
    freeExtents_.erase(selected);
    if (available > length) freeExtents_[offset + length] = available - length;
    extent = {offset, length};
    return true;
}

bool FastDataStore::addFreeExtentLocked(const PhysicalExtent& extent) {
    if (extent.length == 0 || extent.offset > std::numeric_limits<uint64_t>::max() - extent.length) return false;
    std::map<uint64_t, uint64_t> nextFree = freeExtents_;
    uint64_t mergedOffset = extent.offset;
    uint64_t mergedLength = extent.length;
    auto next = nextFree.lower_bound(mergedOffset);
    if (next != nextFree.begin()) {
        const auto previous = std::prev(next);
        if (previous->first + previous->second == mergedOffset) {
            mergedOffset = previous->first;
            mergedLength += previous->second;
            nextFree.erase(previous);
        }
    }
    next = nextFree.lower_bound(mergedOffset);
    if (next != nextFree.end() && mergedOffset + mergedLength == next->first) {
        mergedLength += next->second;
        nextFree.erase(next);
    }
    nextFree[mergedOffset] = mergedLength;

    leveldb::WriteBatch batch;
    for (const auto& [offset, length] : freeExtents_) {
        if (nextFree.find(offset) == nextFree.end()) batch.Delete("free:" + std::to_string(offset));
    }
    for (const auto& [offset, length] : nextFree) {
        const auto current = freeExtents_.find(offset);
        if (current == freeExtents_.end() || current->second != length) {
            batch.Put("free:" + std::to_string(offset), std::to_string(length));
        }
    }
    if (!indexDb_->Write(leveldb::WriteOptions(), &batch).ok()) return false;
    freeExtents_ = std::move(nextFree);
    return true;
}

}  // namespace miniKV::v2
