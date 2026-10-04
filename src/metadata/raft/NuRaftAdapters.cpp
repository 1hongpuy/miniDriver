#include "metadata/raft/NuRaftAdapters.hpp"
#include "utils/AsyncLogger.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <cerrno>
#include <cstdlib>
#include <ctime>
#include <fcntl.h>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include <sys/uio.h>

namespace miniKV::metadata::raft {
namespace {

bool commandDiagnosticsEnabled()
{
    const char* value = std::getenv("MINIKV_METADATA_COMMAND_DIAGNOSTICS");
    return value != nullptr && std::string(value) == "1";
}

void logCommandSpan(const MetadataCommand& command, const ApplyResult& result,
                    uint64_t appendUs, uint64_t quorumUs, uint64_t decodeUs,
                    uint64_t totalUs, size_t batchItems)
{
    if(!commandDiagnosticsEnabled()) return;
    miniKV::utils::logInfo(
        "event=metadata_command_span command_id=" + command.commandId +
        " command_type=" + std::to_string(static_cast<unsigned>(command.type)) +
        " command_generation=" + std::to_string(command.generation) +
        " apply_status=" + std::to_string(static_cast<unsigned>(result.status)) +
        " applied_index=" + std::to_string(result.appliedIndex) +
        " raft_append_us=" + std::to_string(appendUs) +
        " quorum_result_wait_us=" + std::to_string(quorumUs) +
        " result_decode_us=" + std::to_string(decodeUs) +
        " proposal_total_us=" + std::to_string(totalUs) +
        " batch_items=" + std::to_string(batchItems));
}

constexpr const char* kStartKey = "raft/log-start";
constexpr const char* kNextKey = "raft/log-next";
constexpr const char* kConfigKey = "raft/config";
constexpr const char* kServerStateKey = "raft/server-state";
constexpr const char* kSnapshotInfoKey = "raft/snapshot-info";

std::string bufferBytes(const nuraft::buffer& buffer)
{
    return std::string(reinterpret_cast<const char*>(buffer.data_begin()), buffer.size());
}

nuraft::ptr<nuraft::buffer> makeBuffer(const std::string& bytes)
{
    auto output = nuraft::buffer::alloc(bytes.size());
    if(!bytes.empty()) std::memcpy(output->data_begin(), bytes.data(), bytes.size());
    output->pos(0);
    return output;
}

void appendU32(std::string& out, uint32_t value)
{
    for(int shift = 24; shift >= 0; shift -= 8) out.push_back(static_cast<char>((value >> shift) & 0xff));
}

void appendU64(std::string& out, uint64_t value)
{
    for(int shift = 56; shift >= 0; shift -= 8) out.push_back(static_cast<char>((value >> shift) & 0xff));
}

void appendString(std::string& out, const std::string& value)
{
    appendU32(out, static_cast<uint32_t>(value.size())); out.append(value);
}

bool readU32(const std::string& in, size_t& offset, uint32_t& value)
{
    if(offset + 4 > in.size()) return false; value = 0;
    for(int i = 0; i < 4; ++i) value = (value << 8) | static_cast<unsigned char>(in[offset++]);
    return true;
}

bool readU64(const std::string& in, size_t& offset, uint64_t& value)
{
    if(offset + 8 > in.size()) return false; value = 0;
    for(int i = 0; i < 8; ++i) value = (value << 8) | static_cast<unsigned char>(in[offset++]);
    return true;
}

bool readString(const std::string& in, size_t& offset, std::string& value)
{
    uint32_t size = 0; if(!readU32(in, offset, size) || offset + size > in.size()) return false;
    value.assign(in.data() + offset, size); offset += size; return true;
}

void requireOk(const leveldb::Status& status, const char* operation)
{
    if(!status.ok()) throw std::runtime_error(std::string(operation) + ": " + status.ToString());
}

uint64_t environmentUint64(const char* name, uint64_t fallback, uint64_t maximum)
{
    const char* raw = std::getenv(name);
    if(!raw || !*raw) return fallback;
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(raw, &end, 10);
    if(errno != 0 || end == raw || *end != '\0' || parsed > maximum)
        return fallback;
    return static_cast<uint64_t>(parsed);
}

} // namespace

DurableRaftStorage::DurableRaftStorage(const std::string& directory)
    : directory_(directory)
{
    std::error_code filesystemError;
    std::filesystem::create_directories(directory, filesystemError);
    if(filesystemError) throw std::runtime_error("create raft store: " + filesystemError.message());
    leveldb::Options options; options.create_if_missing = true;
    leveldb::DB* raw = nullptr; requireOk(leveldb::DB::Open(options, directory, &raw), "open raft store");
    db_.reset(raw);
}

DurableRaftStorage::~DurableRaftStorage() = default;

bool DurableRaftStorage::get(const std::string& key, std::string& value) const
{
    const auto status = db_->Get(leveldb::ReadOptions(), key, &value);
    if(status.IsNotFound()) return false; requireOk(status, "read raft store"); return true;
}

void DurableRaftStorage::putSync(const std::string& key, const std::string& value)
{
    leveldb::WriteOptions options; options.sync = true;
    requireOk(db_->Put(options, key, value), "sync raft store write");
}

void DurableRaftStorage::writeSync(leveldb::WriteBatch& batch)
{
    leveldb::WriteOptions options; options.sync = true;
    requireOk(db_->Write(options, &batch), "sync raft store batch");
}

std::string DurableLogStore::encodeU64(uint64_t value)
{
    std::string out; appendU64(out, value); return out;
}

uint64_t DurableLogStore::decodeU64(const std::string& value, uint64_t fallback)
{
    size_t offset = 0; uint64_t result = 0;
    return readU64(value, offset, result) && offset == value.size() ? result : fallback;
}

std::string DurableLogStore::logKey(nuraft::ulong index)
{
    std::ostringstream out; out << "raft/log/" << std::setw(20) << std::setfill('0') << index; return out.str();
}

std::string DurableLogStore::serializeEntry(nuraft::ptr<nuraft::log_entry>& entry)
{ return bufferBytes(*entry->serialize()); }

nuraft::ptr<nuraft::log_entry> DurableLogStore::deserializeEntry(const std::string& bytes)
{ auto buffer = makeBuffer(bytes); return nuraft::log_entry::deserialize(*buffer); }

namespace {

constexpr uint32_t kLogRecordMagic = 0x4d4b524c; // "MKRL"
constexpr uint32_t kLogRecordVersion = 1;
constexpr size_t kLogRecordHeaderSize = sizeof(uint32_t) * 2 + sizeof(uint64_t) + sizeof(uint32_t);
constexpr size_t kLogRecordTrailerSize = sizeof(uint64_t);

uint32_t readU32At(const char* data)
{
    uint32_t value = 0; std::memcpy(&value, data, sizeof(value)); return value;
}

uint64_t readU64At(const char* data)
{
    uint64_t value = 0; std::memcpy(&value, data, sizeof(value)); return value;
}

void appendNative(std::string& output, const void* data, size_t size)
{ output.append(static_cast<const char*>(data), size); }

bool writeAll(int fd, const char* data, size_t size)
{
    while(size > 0) {
        const ssize_t written = ::write(fd, data, size);
        if(written < 0 && errno == EINTR) continue;
        if(written <= 0) return false;
        data += written; size -= static_cast<size_t>(written);
    }
    return true;
}

bool writevAll(int fd, const std::vector<std::string>& records)
{
    if(records.empty()) return true;
    std::vector<iovec> vectors;
    vectors.reserve(records.size());
    for(const auto& record : records) {
        if(record.empty()) continue;
        vectors.push_back(iovec{const_cast<char*>(record.data()), record.size()});
    }
    size_t first = 0;
    const long configuredMax = ::sysconf(_SC_IOV_MAX);
    const size_t maxVectors = configuredMax > 0 ? static_cast<size_t>(configuredMax) : 16U;
    while(first < vectors.size()) {
        const int count = static_cast<int>(std::min<size_t>(vectors.size() - first, maxVectors));
        const ssize_t written = ::writev(fd, vectors.data() + first, count);
        if(written < 0 && errno == EINTR) continue;
        if(written <= 0) return false;
        size_t remaining = static_cast<size_t>(written);
        while(first < vectors.size() && remaining >= vectors[first].iov_len) {
            remaining -= vectors[first].iov_len;
            ++first;
        }
        if(first < vectors.size() && remaining > 0) {
            vectors[first].iov_base = static_cast<char*>(vectors[first].iov_base) + remaining;
            vectors[first].iov_len -= remaining;
        }
    }
    return true;
}

bool readAllAt(int fd, char* data, size_t size, off_t offset)
{
    while(size > 0) {
        const ssize_t read = ::pread(fd, data, size, offset);
        if(read < 0 && errno == EINTR) continue;
        if(read <= 0) return false;
        data += read; size -= static_cast<size_t>(read); offset += read;
    }
    return true;
}

} // namespace

DurableLogStore::DurableLogStore(std::shared_ptr<DurableRaftStorage> storage,
                                 std::string walDirectory)
    : storage_(std::move(storage))
{
    const std::string walRoot = walDirectory.empty() ? storage_->directory() : std::move(walDirectory);
    std::error_code filesystemError;
    std::filesystem::create_directories(walRoot, filesystemError);
    if(filesystemError) throw std::runtime_error("create raft WAL directory: " + filesystemError.message());
    logPath_ = walRoot + "/raft-log.bin";
    // Keep the hot log append-only.  Records are accumulated per NuRaft
    // append batch and emitted with one writev call at the batch boundary;
    // the following durable barrier still uses fdatasync.
    logFd_ = ::open(logPath_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if(logFd_ < 0) throw std::runtime_error("open raft log file: " + std::string(std::strerror(errno)));
    if(!loadLogFile()) {
        ::close(logFd_); logFd_ = -1;
        throw std::runtime_error("recover raft log file: " + logPath_);
    }
    // The LevelDB keys are retained as a compatibility marker for older
    // stores, but the hot Raft log itself is now an append-only WAL.  This
    // avoids paying a LevelDB WAL + manifest sync for every metadata command.
    std::string value;
    if(entries_.empty()) {
        if(storage_->get(kStartKey, value)) startIndex_ = decodeU64(value, 1);
        if(storage_->get(kNextKey, value)) nextIndex_ = decodeU64(value, startIndex_);
        if(nextIndex_ < startIndex_) nextIndex_ = startIndex_;
    }
    durableIndex_ = nextIndex_ ? nextIndex_ - 1 : 0;
    pendingDurableIndex_ = durableIndex_;
}

DurableLogStore::~DurableLogStore()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
        if(asyncAppendEnabled_ && logFd_ >= 0 && pendingDurableIndex_ > durableIndex_)
            syncRequested_ = true;
    }
    syncCondition_.notify_all();
    if(syncWorker_.joinable()) syncWorker_.join();
    std::lock_guard<std::mutex> lock(mutex_);
    if(logFd_ >= 0) {
        // NuRaft calls end_of_append_batch for normal appends.  Flush again at
        // shutdown so a clean process stop never leaves a valid but unsynced
        // tail behind.
        flushPendingRecordsLocked();
        syncLogLocked();
        ::close(logFd_); logFd_ = -1;
    }
}

void DurableLogStore::enableAsyncAppend(bool enabled) noexcept
{
    std::lock_guard<std::mutex> lock(mutex_);
    asyncAppendEnabled_ = enabled;
    if(enabled && !syncWorker_.joinable()) {
        stopping_ = false;
        syncWorker_ = std::thread(&DurableLogStore::syncWorkerLoop, this);
    }
}

void DurableLogStore::setSyncCoalesceUs(uint64_t microseconds) noexcept
{
    std::lock_guard<std::mutex> lock(mutex_);
    syncCoalesceUs_ = microseconds;
}

void DurableLogStore::setAppendCompletionCallback(std::function<void(bool, uint64_t)> callback)
{
    std::lock_guard<std::mutex> lock(mutex_);
    appendCompletionCallback_ = std::move(callback);
}

void DurableLogStore::setAppendBatchMetricsCallback(
    std::function<void(uint64_t, uint64_t, uint64_t)> callback)
{
    std::lock_guard<std::mutex> lock(mutex_);
    appendBatchMetricsCallback_ = std::move(callback);
}

void DurableLogStore::syncWorkerLoop()
{
    for(;;) {
        int fd = -1;
        nuraft::ulong target = 0;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            syncCondition_.wait(lock, [&] { return stopping_ || syncRequested_; });
            if(stopping_ && !syncRequested_) return;
            const auto coalesceUs = syncCoalesceUs_;
            lock.unlock();
            if(coalesceUs > 0) std::this_thread::sleep_for(std::chrono::microseconds(coalesceUs));
            lock.lock();
            // New append batches may have arrived while the coalescing window
            // was open. Consume all of them with this same durable barrier.
            fd = logFd_;
            target = pendingDurableIndex_;
            syncRequested_ = false;
        }

        const auto syncStarted = std::chrono::steady_clock::now();
        const bool ok = fd >= 0 && ::fdatasync(fd) == 0;
        const auto syncElapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - syncStarted).count());
        std::function<void(bool, uint64_t)> callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(ok) durableIndex_ = std::max(durableIndex_, target);
            else syncFailure_ = true;
            // A writer may have appended another batch while fdatasync was in
            // progress.  Keep the worker armed for that newer tail.
            if(pendingDurableIndex_ > durableIndex_ && !stopping_) syncRequested_ = true;
            callback = appendCompletionCallback_;
        }
        durableCondition_.notify_all();
        if(callback) callback(ok, syncElapsed);
        syncCondition_.notify_all();
    }
}

bool DurableLogStore::waitForDurable(nuraft::ulong target)
{
    if(target == 0) return true;
    if(!asyncAppendEnabled_) {
        std::lock_guard<std::mutex> lock(mutex_);
        return pendingDurableIndex_ <= durableIndex_ || syncLogLocked();
    }
    std::unique_lock<std::mutex> lock(mutex_);
    if(pendingDurableIndex_ > durableIndex_) syncRequested_ = true;
    syncCondition_.notify_all();
    durableCondition_.wait(lock, [&] {
        return durableIndex_ >= target || syncFailure_ || logFd_ < 0;
    });
    return durableIndex_ >= target && !syncFailure_;
}

uint64_t DurableLogStore::checksumRecord(const std::string& bytes)
{
    // FNV-1a is only a torn-record detector here; Raft still provides the
    // replicated correctness boundary and metadata commands carry their own
    // checksums.  It is deliberately cheap on the synchronous append path.
    uint64_t hash = 1469598103934665603ULL;
    for(const unsigned char byte : bytes) { hash ^= byte; hash *= 1099511628211ULL; }
    return hash;
}

std::string DurableLogStore::makeRecord(nuraft::ulong index, const std::string& bytes)
{
    if(bytes.size() > std::numeric_limits<uint32_t>::max())
        throw std::length_error("Raft log record is too large");
    std::string record;
    record.reserve(kLogRecordHeaderSize + bytes.size() + kLogRecordTrailerSize);
    const uint32_t magic = kLogRecordMagic, version = kLogRecordVersion;
    const uint64_t logIndex = index; const uint32_t size = static_cast<uint32_t>(bytes.size());
    appendNative(record, &magic, sizeof(magic)); appendNative(record, &version, sizeof(version));
    appendNative(record, &logIndex, sizeof(logIndex)); appendNative(record, &size, sizeof(size));
    record.append(bytes); const uint64_t checksum = checksumRecord(record);
    appendNative(record, &checksum, sizeof(checksum)); return record;
}

bool DurableLogStore::loadLogFile()
{
    if(logFd_ < 0) return false;
    const off_t fileSize = ::lseek(logFd_, 0, SEEK_END);
    if(fileSize < 0) return false;
    off_t offset = 0; nuraft::ulong expected = 0; bool first = true;
    while(offset + static_cast<off_t>(kLogRecordHeaderSize + kLogRecordTrailerSize) <= fileSize) {
        std::string header(kLogRecordHeaderSize, '\0');
        if(!readAllAt(logFd_, header.data(), header.size(), offset)) return false;
        const uint32_t magic = readU32At(header.data());
        const uint32_t version = readU32At(header.data() + sizeof(uint32_t));
        const nuraft::ulong index = readU64At(header.data() + sizeof(uint32_t) * 2);
        const uint32_t size = readU32At(header.data() + sizeof(uint32_t) * 2 + sizeof(uint64_t));
        if(magic != kLogRecordMagic || version != kLogRecordVersion
           || size > (64U * 1024U * 1024U)
           || offset + static_cast<off_t>(kLogRecordHeaderSize + size + kLogRecordTrailerSize) > fileSize)
            break;
        std::string payload(size, '\0');
        if(size && !readAllAt(logFd_, payload.data(), size, offset + static_cast<off_t>(kLogRecordHeaderSize))) return false;
        uint64_t storedChecksum = 0;
        if(!readAllAt(logFd_, reinterpret_cast<char*>(&storedChecksum), sizeof(storedChecksum),
                      offset + static_cast<off_t>(kLogRecordHeaderSize + size))) return false;
        std::string forChecksum = header; forChecksum.append(payload);
        if(checksumRecord(forChecksum) != storedChecksum) break;
        if(!first && index != expected) break;
        if(first) { startIndex_ = index; first = false; }
        entries_[index] = std::move(payload); expected = index + 1;
        offset += static_cast<off_t>(kLogRecordHeaderSize + size + kLogRecordTrailerSize);
    }
    // A crash can leave a partial final record.  Discard only the invalid
    // tail; all complete records before it are still valid Raft log entries.
    if(::ftruncate(logFd_, offset) != 0) return false;
    if(!entries_.empty()) { nextIndex_ = expected; startIndex_ = entries_.begin()->first; }
    else { startIndex_ = 1; nextIndex_ = 1; }
    return ::lseek(logFd_, 0, SEEK_END) >= 0;
}

bool DurableLogStore::flushPendingRecordsLocked()
{
    if(logFd_ < 0) return false;
    if(pendingRecordBytes_.empty()) return true;
    uint64_t bytes = 0;
    for(const auto& record : pendingRecordBytes_) bytes += record.size();
    const uint64_t records = pendingRecordBytes_.size();
    const auto started = std::chrono::steady_clock::now();
    if(!writevAll(logFd_, pendingRecordBytes_)) return false;
    const uint64_t elapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - started).count());
    if(appendBatchMetricsCallback_) appendBatchMetricsCallback_(records, bytes, elapsed);
    pendingRecordBytes_.clear();
    return true;
}

bool DurableLogStore::syncLogLocked()
{
    if(logFd_ < 0) return false;
    if(pendingDurableIndex_ <= durableIndex_) return true;
    if(::fdatasync(logFd_) != 0) return false;
    durableIndex_ = pendingDurableIndex_; return true;
}

bool DurableLogStore::rewriteLogLocked()
{
    if(logFd_ < 0) return false;
    const std::string temporary = logPath_ + ".tmp";
    const int tempFd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if(tempFd < 0) return false;
    bool ok = true;
    for(const auto& [index, bytes] : entries_) {
        const std::string record = makeRecord(index, bytes);
        if(!writeAll(tempFd, record.data(), record.size())) { ok = false; break; }
    }
    if(ok && ::fdatasync(tempFd) != 0) ok = false;
    if(::close(tempFd) != 0) ok = false;
    if(!ok || ::rename(temporary.c_str(), logPath_.c_str()) != 0) {
        ::unlink(temporary.c_str()); return false;
    }
    const std::string parent = std::filesystem::path(logPath_).parent_path().string();
    const int parentFd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY);
    if(parentFd < 0 || ::fsync(parentFd) != 0) {
        if(parentFd >= 0) ::close(parentFd);
        return false;
    }
    ::close(parentFd);
    ::close(logFd_); logFd_ = ::open(logPath_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if(logFd_ < 0) return false;
    leveldb::WriteBatch marker;
    marker.Put(kStartKey, encodeU64(startIndex_)); marker.Put(kNextKey, encodeU64(nextIndex_));
    storage_->writeSync(marker);
    durableIndex_ = nextIndex_ ? nextIndex_ - 1 : 0; pendingDurableIndex_ = durableIndex_; return true;
}

nuraft::ulong DurableLogStore::next_slot() const { std::lock_guard<std::mutex> lock(mutex_); return nextIndex_; }
nuraft::ulong DurableLogStore::start_index() const { std::lock_guard<std::mutex> lock(mutex_); return startIndex_; }

nuraft::ptr<nuraft::log_entry> DurableLogStore::dummy() const
{ return nuraft::cs_new<nuraft::log_entry>(0, nuraft::buffer::alloc(0)); }

nuraft::ptr<nuraft::log_entry> DurableLogStore::entry_at(nuraft::ulong index)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if(index < startIndex_ || index >= nextIndex_) return nullptr;
    const auto it = entries_.find(index); if(it == entries_.end()) return nullptr;
    return deserializeEntry(it->second);
}

nuraft::ptr<nuraft::log_entry> DurableLogStore::last_entry() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    if(nextIndex_ <= startIndex_) return dummy();
    const auto it = entries_.find(nextIndex_ - 1); if(it == entries_.end()) return dummy();
    return deserializeEntry(it->second);
}

nuraft::ulong DurableLogStore::append(nuraft::ptr<nuraft::log_entry>& entry)
{
    std::lock_guard<std::mutex> lock(mutex_); const auto index = nextIndex_;
    const std::string bytes = serializeEntry(entry);
    pendingRecordBytes_.push_back(makeRecord(index, bytes));
    entries_[index] = bytes; nextIndex_ = index + 1; pendingDurableIndex_ = index; return index;
}

void DurableLogStore::end_of_append_batch(nuraft::ulong, nuraft::ulong cnt)
{
    if(cnt == 0) return;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(!flushPendingRecordsLocked())
            throw std::runtime_error("write raft log batch failed");
    }
    if(asyncAppendEnabled_) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(pendingDurableIndex_ > durableIndex_) syncRequested_ = true;
        }
        syncCondition_.notify_all();
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if(pendingDurableIndex_ > durableIndex_ && !syncLogLocked())
        throw std::runtime_error("sync raft log batch failed");
}

void DurableLogStore::write_at(nuraft::ulong index, nuraft::ptr<nuraft::log_entry>& entry)
{
    nuraft::ulong target = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(!flushPendingRecordsLocked()) throw std::runtime_error("write pending raft log failed");
        target = pendingDurableIndex_;
    }
    if(!waitForDurable(target)) throw std::runtime_error("sync raft log before overwrite failed");
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.erase(entries_.lower_bound(index), entries_.end());
    entries_[index] = serializeEntry(entry); nextIndex_ = index + 1;
    if(startIndex_ > index) startIndex_ = index;
    if(!rewriteLogLocked()) throw std::runtime_error("rewrite raft log failed");
    durableIndex_ = nextIndex_ ? nextIndex_ - 1 : 0; pendingDurableIndex_ = durableIndex_;
}

nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
DurableLogStore::log_entries(nuraft::ulong start, nuraft::ulong end)
{ return log_entries_ext(start, end, 0); }

nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
DurableLogStore::log_entries_ext(nuraft::ulong start, nuraft::ulong end, nuraft::int64 hint)
{
    auto output = nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();
    if(hint < 0) return output; size_t bytes = 0;
    for(auto index = start; index < end; ++index) {
        auto entry = entry_at(index); if(!entry) return nullptr;
        bytes += entry->get_buf().size(); output->push_back(entry);
        if(hint > 0 && bytes >= static_cast<size_t>(hint)) break;
    }
    return output;
}

nuraft::ulong DurableLogStore::term_at(nuraft::ulong index)
{ auto entry = entry_at(index); return entry ? entry->get_term() : 0; }

nuraft::ptr<nuraft::buffer> DurableLogStore::pack(nuraft::ulong index, nuraft::int32 count)
{
    std::vector<std::string> entries; size_t size = sizeof(int32_t);
    for(int32_t i = 0; i < count; ++i) {
        auto entry = entry_at(index + i); if(!entry) return nullptr;
        entries.push_back(bufferBytes(*entry->serialize())); size += sizeof(int32_t) + entries.back().size();
    }
    auto output = nuraft::buffer::alloc(size); output->put(count);
    for(const auto& bytes : entries) {
        output->put(static_cast<int32_t>(bytes.size())); auto item = makeBuffer(bytes); output->put(*item);
    }
    output->pos(0); return output;
}

void DurableLogStore::apply_pack(nuraft::ulong index, nuraft::buffer& pack)
{
    pack.pos(0); const int32_t count = pack.get_int(); std::vector<std::string> entries;
    for(int32_t i = 0; i < count; ++i) {
        const int32_t size = pack.get_int(); auto buffer = nuraft::buffer::alloc(size); pack.get(buffer);
        entries.push_back(bufferBytes(*buffer));
    }
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.erase(entries_.lower_bound(index), entries_.end());
    for(int32_t i = 0; i < count; ++i) entries_[index + i] = entries[i];
    const auto next = index + count; nextIndex_ = next; if(startIndex_ > index) startIndex_ = index;
    if(!rewriteLogLocked()) throw std::runtime_error("rewrite packed raft log failed");
    durableIndex_ = next ? next - 1 : 0; pendingDurableIndex_ = durableIndex_;
}

bool DurableLogStore::compact(nuraft::ulong lastLogIndex)
{
    nuraft::ulong target = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(!flushPendingRecordsLocked()) return false;
        target = pendingDurableIndex_;
    }
    if(!waitForDurable(target)) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    const auto newStart = std::max(startIndex_, lastLogIndex + 1);
    entries_.erase(entries_.begin(), entries_.lower_bound(newStart));
    startIndex_ = newStart; if(nextIndex_ < startIndex_) nextIndex_ = startIndex_;
    try { return rewriteLogLocked(); } catch(...) { return false; }
}

bool DurableLogStore::flush()
{
    nuraft::ulong target = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(!flushPendingRecordsLocked()) return false;
        target = pendingDurableIndex_;
    }
    return waitForDurable(target);
}
nuraft::ulong DurableLogStore::last_durable_index() { std::lock_guard<std::mutex> lock(mutex_); return durableIndex_; }

DurableStateManager::DurableStateManager(int serverId, std::vector<StaticMember> members,
                                         std::shared_ptr<DurableRaftStorage> storage,
                                         nuraft::ptr<DurableLogStore> logStore)
    : serverId_(serverId), members_(std::move(members)), storage_(std::move(storage)), logStore_(std::move(logStore)) {}

nuraft::ptr<nuraft::cluster_config> DurableStateManager::load_config()
{
    std::string bytes;
    if(storage_->get(kConfigKey, bytes)) { auto buffer = makeBuffer(bytes); return nuraft::cluster_config::deserialize(*buffer); }
    auto config = nuraft::cs_new<nuraft::cluster_config>();
    for(const auto& member : members_) config->get_servers().push_back(nuraft::cs_new<nuraft::srv_config>(member.id, member.endpoint));
    save_config(*config); return config;
}

void DurableStateManager::save_config(const nuraft::cluster_config& config)
{ storage_->putSync(kConfigKey, bufferBytes(*config.serialize())); }

void DurableStateManager::save_state(const nuraft::srv_state& state)
{ storage_->putSync(kServerStateKey, bufferBytes(*state.serialize())); }

nuraft::ptr<nuraft::srv_state> DurableStateManager::read_state()
{
    std::string bytes; if(!storage_->get(kServerStateKey, bytes)) return nullptr;
    auto buffer = makeBuffer(bytes); return nuraft::srv_state::deserialize(*buffer);
}

nuraft::ptr<nuraft::log_store> DurableStateManager::load_log_store() { return logStore_; }
nuraft::int32 DurableStateManager::server_id() { return serverId_; }
void DurableStateManager::system_exit(int) {}

void DurableStateManager::saveSnapshotInfo(nuraft::snapshot& snapshot)
{ storage_->putSync(kSnapshotInfoKey, bufferBytes(*snapshot.serialize())); }

nuraft::ptr<nuraft::snapshot> DurableStateManager::loadSnapshotInfo() const
{
    std::string bytes; if(!storage_->get(kSnapshotInfoKey, bytes)) return nullptr;
    auto buffer = makeBuffer(bytes); return nuraft::snapshot::deserialize(*buffer);
}

NuRaftStateMachine::NuRaftStateMachine(std::string directory,
                                       nuraft::ptr<DurableLogStore> logStore,
                                       nuraft::ptr<DurableStateManager> stateManager)
    : snapshotStore_(std::move(directory)), logStore_(std::move(logStore)), stateManager_(std::move(stateManager))
{
    std::string error;
    if(const auto snapshot = snapshotStore_.load(&error)) {
        if(!state_.restore(*snapshot)) throw std::runtime_error("invalid metadata snapshot: " + error);
        lastCommitIndex_.store(snapshot->lastAppliedIndex);
    }
    lastSnapshot_ = stateManager_->loadSnapshotInfo();
}

nuraft::ptr<nuraft::buffer> NuRaftStateMachine::encodeResult(const ApplyResult& result)
{
    std::string bytes("MKR1", 4); appendU32(bytes, static_cast<uint32_t>(result.status));
    appendString(bytes, result.commandId); appendString(bytes, result.message); appendString(bytes, result.objectId);
    appendString(bytes, result.sessionId); appendString(bytes, result.leaseId);
    appendU64(bytes, result.objectVersion); appendU64(bytes, result.metadataVersion);
    appendU64(bytes, result.appliedIndex); appendU64(bytes, result.appliedTerm);
    appendU64(bytes, result.placementEpoch); appendU64(bytes, result.nodeEpoch); return makeBuffer(bytes);
}

std::optional<ApplyResult> NuRaftStateMachine::decodeResult(nuraft::buffer& buffer)
{
    const auto bytes = bufferBytes(buffer); if(bytes.size() < 4 || bytes.compare(0, 4, "MKR1") != 0) return std::nullopt;
    size_t offset = 4; uint32_t status = 0; ApplyResult result;
    if(!readU32(bytes, offset, status) || !readString(bytes, offset, result.commandId)
       || !readString(bytes, offset, result.message) || !readString(bytes, offset, result.objectId)
       || !readString(bytes, offset, result.sessionId) || !readString(bytes, offset, result.leaseId)
       || !readU64(bytes, offset, result.objectVersion) || !readU64(bytes, offset, result.metadataVersion)
       || !readU64(bytes, offset, result.appliedIndex) || !readU64(bytes, offset, result.appliedTerm)
       || !readU64(bytes, offset, result.placementEpoch) || !readU64(bytes, offset, result.nodeEpoch)
       || offset != bytes.size()) return std::nullopt;
    result.status = static_cast<ApplyStatus>(status); return result;
}

nuraft::ptr<nuraft::buffer> NuRaftStateMachine::commit(nuraft::ulong index, nuraft::buffer& data)
{
    const std::string bytes = bufferBytes(data); const auto command = decodeMetadataCommand(bytes);
    ApplyResult result;
    std::lock_guard<std::mutex> lock(mutex_);
    if(command) result = state_.apply(*command, index, logStore_->term_at(index));
    else { result.status = ApplyStatus::kInvalid; result.message = "invalid metadata command in committed Raft log";
           result.appliedIndex = index; result.appliedTerm = logStore_->term_at(index); }
    lastCommitIndex_.store(index); return encodeResult(result);
}

void NuRaftStateMachine::commit_config(nuraft::ulong index, nuraft::ptr<nuraft::cluster_config>&)
{ lastCommitIndex_.store(index); }

bool NuRaftStateMachine::publishSnapshot(nuraft::snapshot& snapshot, std::string* error)
{
    auto value = state_.snapshot(); value.lastAppliedIndex = snapshot.get_last_log_idx();
    value.lastAppliedTerm = snapshot.get_last_log_term();
    if(!snapshotStore_.publish(value, error)) return false;
    stateManager_->saveSnapshotInfo(snapshot); auto serialized = snapshot.serialize();
    lastSnapshot_ = nuraft::snapshot::deserialize(*serialized); return true;
}

void NuRaftStateMachine::create_snapshot(nuraft::snapshot& snapshot,
                                         nuraft::async_result<bool>::handler_type& done)
{
    bool success = false; nuraft::ptr<std::exception> error;
    try { std::lock_guard<std::mutex> lock(mutex_); std::string message; success = publishSnapshot(snapshot, &message);
          if(!success) error = nuraft::cs_new<std::runtime_error>(message); }
    catch(const std::exception& exception) { error = nuraft::cs_new<std::runtime_error>(exception.what()); }
    done(success, error);
}

int NuRaftStateMachine::read_logical_snp_obj(nuraft::snapshot&, void*&, nuraft::ulong objectId,
                                              nuraft::ptr<nuraft::buffer>& output, bool& last)
{
    if(objectId != 0) return -1; std::lock_guard<std::mutex> lock(mutex_);
    output = makeBuffer(state_.snapshot().bytes); last = true; return 0;
}

void NuRaftStateMachine::save_logical_snp_obj(nuraft::snapshot& snapshot, nuraft::ulong& objectId,
                                               nuraft::buffer& data, bool first, bool last)
{
    std::lock_guard<std::mutex> lock(mutex_); if(first) incomingSnapshot_.clear(); incomingSnapshot_ += bufferBytes(data);
    if(last) {
        MetadataSnapshot value; value.schemaVersion = kMetadataSchemaVersion;
        value.lastAppliedIndex = snapshot.get_last_log_idx(); value.lastAppliedTerm = snapshot.get_last_log_term();
        value.bytes = incomingSnapshot_; std::string error;
        if(!snapshotStore_.publish(value, &error)) throw std::runtime_error(error);
        stateManager_->saveSnapshotInfo(snapshot); incomingSnapshot_.clear();
    }
    ++objectId;
}

bool NuRaftStateMachine::apply_snapshot(nuraft::snapshot& snapshot)
{
    std::lock_guard<std::mutex> lock(mutex_); std::string error; const auto value = snapshotStore_.load(&error);
    if(!value || value->lastAppliedIndex != snapshot.get_last_log_idx() || !state_.restore(*value)) return false;
    auto serialized = snapshot.serialize(); lastSnapshot_ = nuraft::snapshot::deserialize(*serialized);
    lastCommitIndex_.store(snapshot.get_last_log_idx()); return true;
}

void NuRaftStateMachine::free_user_snp_ctx(void*& context) { context = nullptr; }
nuraft::ptr<nuraft::snapshot> NuRaftStateMachine::last_snapshot() { std::lock_guard<std::mutex> lock(mutex_); return lastSnapshot_; }
nuraft::ulong NuRaftStateMachine::last_commit_index() { return lastCommitIndex_.load(); }
std::optional<UploadSessionRecord> NuRaftStateMachine::session(const std::string& id) const { std::lock_guard<std::mutex> lock(mutex_); return state_.session(id); }
std::vector<UploadSessionRecord> NuRaftStateMachine::sessions() const { std::lock_guard<std::mutex> lock(mutex_); return state_.sessions(); }
std::optional<LeaseRecord> NuRaftStateMachine::lease(const std::string& id) const { std::lock_guard<std::mutex> lock(mutex_); return state_.lease(id); }
std::optional<ObjectRecord> NuRaftStateMachine::object(const std::string& id) const { std::lock_guard<std::mutex> lock(mutex_); return state_.object(id); }
std::optional<ChunkRouteRecord> NuRaftStateMachine::chunk(const std::string& objectId, uint32_t index) const { std::lock_guard<std::mutex> lock(mutex_); return state_.chunk(objectId, index); }
std::optional<ReadDescriptor> NuRaftStateMachine::readDescriptor(const std::string& id, uint64_t version) const { std::lock_guard<std::mutex> lock(mutex_); return state_.readDescriptor(id, version); }
std::vector<DirectoryRecord> NuRaftStateMachine::directories(const std::string& ownerId, const std::string& parentPath) const { std::lock_guard<std::mutex> lock(mutex_); return state_.directories(ownerId, parentPath); }
std::vector<ObjectRecord> NuRaftStateMachine::objects(const std::string& ownerId, const std::string& parentPath) const { std::lock_guard<std::mutex> lock(mutex_); return state_.objects(ownerId, parentPath); }
std::vector<DeleteTaskRecord> NuRaftStateMachine::deleteTasks(const std::string& nodeId) const { std::lock_guard<std::mutex> lock(mutex_); return state_.deleteTasks(nodeId); }
std::string NuRaftStateMachine::stateDigest() const { std::lock_guard<std::mutex> lock(mutex_); return state_.stateDigest(); }
std::optional<NodeRecord> NuRaftStateMachine::node(const std::string& id) const { std::lock_guard<std::mutex> lock(mutex_); return state_.node(id); }
std::vector<NodeRecord> NuRaftStateMachine::nodes() const { std::lock_guard<std::mutex> lock(mutex_); return state_.nodes(); }

NuRaftMetadataService::NuRaftMetadataService(int serverId, int raftPort,
                                             std::vector<StaticMember> members,
                                             std::string directory,
                                             std::string walDirectory)
    : serverId_(serverId), raftPort_(raftPort), members_(std::move(members)),
      directory_(std::move(directory)), walDirectory_(std::move(walDirectory))
{}

NuRaftMetadataService::~NuRaftMetadataService() { shutdown(); }

bool NuRaftMetadataService::start(std::string* error)
{
    try {
        storage_ = std::make_shared<DurableRaftStorage>(directory_ + "/raft");
        logStore_ = nuraft::cs_new<DurableLogStore>(storage_, walDirectory_);
        // Replication can overlap the leader's local WAL fdatasync.  NuRaft
        // still gates its commit index on last_durable_index(), and follower
        // append handlers wait for their own durable index, so this preserves
        // the majority-durable-log contract while removing needless serial
        // disk/replication idle time.
        logStore_->setSyncCoalesceUs(environmentUint64(
            "MINIKV_RAFT_FSYNC_COALESCE_US", 0, 1000000));
        logStore_->enableAsyncAppend(true);
        stateManager_ = nuraft::cs_new<DurableStateManager>(serverId_, members_, storage_, logStore_);
        machine_ = nuraft::cs_new<NuRaftStateMachine>(directory_ + "/snapshot", logStore_, stateManager_);
        nuraft::asio_service::options asio;
        nuraft::raft_params params;
        params.with_hb_interval(150).with_election_timeout_lower(600)
              .with_election_timeout_upper(1200).with_client_req_timeout(1000)
              .with_snapshot_enabled(10000).with_reserved_log_items(1000)
              .with_auto_forwarding(false);
        params.parallel_log_appending_ = true;
        server_ = launcher_.init(machine_, stateManager_, nullptr, raftPort_, asio, params);
        if(!server_) { if(error) *error = "NuRaft launcher initialization failed"; return false; }
        logStore_->setAppendCompletionCallback([this](bool ok, uint64_t elapsedUs) {
            {
                std::lock_guard<std::mutex> metricsLock(metricsMutex_);
                ++metrics_.raftLogSyncCount;
                metrics_.raftLogSyncTotalUs += elapsedUs;
                if(metrics_.raftLogSyncMinUs == 0 || elapsedUs < metrics_.raftLogSyncMinUs)
                    metrics_.raftLogSyncMinUs = elapsedUs;
                metrics_.raftLogSyncMaxUs = std::max(metrics_.raftLogSyncMaxUs, elapsedUs);
                if(!ok) ++metrics_.raftLogSyncFailureCount;
            }
            if(server_) server_->notify_log_append_completion(ok);
        });
        logStore_->setAppendBatchMetricsCallback([this](uint64_t records, uint64_t bytes, uint64_t elapsedUs) {
            std::lock_guard<std::mutex> metricsLock(metricsMutex_);
            ++metrics_.raftLogWritevCount;
            metrics_.raftLogWritevRecords += records;
            metrics_.raftLogWritevBytes += bytes;
            metrics_.raftLogWritevTotalUs += elapsedUs;
            if(metrics_.raftLogWritevMinUs == 0 || elapsedUs < metrics_.raftLogWritevMinUs)
                metrics_.raftLogWritevMinUs = elapsedUs;
            metrics_.raftLogWritevMaxUs = std::max(metrics_.raftLogWritevMaxUs, elapsedUs);
        });
        return true;
    } catch(const std::exception& exception) {
        if(error) *error = exception.what(); return false;
    }
}

void NuRaftMetadataService::shutdown()
{
    if(server_) { launcher_.shutdown(); server_.reset(); }
}

ApplyResult NuRaftMetadataService::unavailable(const MetadataCommand& command, const std::string& message) const
{
    ApplyResult result; result.commandId = command.commandId; result.status = ApplyStatus::kUnavailable;
    result.message = message; if(server_) { result.appliedIndex = server_->get_committed_log_idx(); result.appliedTerm = server_->get_term(); }
    return result;
}

ApplyResult NuRaftMetadataService::propose(const MetadataCommand& command)
{
    const auto started = std::chrono::steady_clock::now();
    uint64_t appendElapsed = 0;
    uint64_t quorumElapsed = 0;
    uint64_t decodeElapsed = 0;
    auto record = [&](ApplyResult result) {
        const auto elapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - started).count());
        {
            std::lock_guard<std::mutex> metricsLock(metricsMutex_);
            ++metrics_.proposalCount; metrics_.proposalTotalUs += elapsed;
            if(result.status != ApplyStatus::kOk && result.status != ApplyStatus::kAlreadyApplied)
                ++metrics_.proposalFailureCount;
        }
        logCommandSpan(command, result, appendElapsed, quorumElapsed, decodeElapsed, elapsed, 1);
        return result;
    };
    if(!server_ || !server_->is_initialized() || !server_->is_leader())
        return record(unavailable(command, "not metadata leader"));
    const auto encoded = encodeMetadataCommand(command);
    if(!encoded) { ApplyResult result = unavailable(command, "invalid metadata command"); result.status = ApplyStatus::kInvalid; return record(std::move(result)); }
    auto buffer = makeBuffer(*encoded);
    const auto appendStarted = std::chrono::steady_clock::now();
    const auto result = server_->append_entries({buffer});
    appendElapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - appendStarted).count());
    {
        std::lock_guard<std::mutex> metricsLock(metricsMutex_);
        metrics_.raftAppendTotalUs += appendElapsed;
        ++metrics_.raftAppendCount;
    }
    if(!result || !result->get_accepted() || result->get_result_code() != nuraft::cmd_result_code::OK)
        return record(unavailable(command, result ? result->get_result_str() : "Raft append failed"));
    const auto quorumStarted = std::chrono::steady_clock::now();
    auto committed = result->get();
    quorumElapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - quorumStarted).count());
    {
        std::lock_guard<std::mutex> metricsLock(metricsMutex_);
        ++metrics_.quorumWaitCount;
        metrics_.quorumWaitTotalUs += quorumElapsed;
    }
    if(!committed) return record(unavailable(command, "Raft commit returned no state-machine result"));
    const auto decodeStarted = std::chrono::steady_clock::now();
    const auto decoded = NuRaftStateMachine::decodeResult(*committed);
    decodeElapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - decodeStarted).count());
    {
        std::lock_guard<std::mutex> metricsLock(metricsMutex_);
        metrics_.stateMachineDecodeTotalUs += decodeElapsed;
    }
    return record(decoded ? *decoded : unavailable(command, "invalid state-machine result"));
}

ApplyResult NuRaftMetadataService::proposeBatch(const std::vector<MetadataCommand>& commands)
{
    if(commands.empty()) {
        ApplyResult result; result.status = ApplyStatus::kInvalid; result.message = "empty metadata command batch"; return result;
    }
    const auto started = std::chrono::steady_clock::now();
    const MetadataCommand& lastCommand = commands.back();
    uint64_t appendElapsed = 0;
    uint64_t quorumElapsed = 0;
    uint64_t decodeElapsed = 0;
    auto record = [&](ApplyResult result) {
        const auto elapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - started).count());
        {
            std::lock_guard<std::mutex> metricsLock(metricsMutex_);
            metrics_.proposalCount += commands.size();
            metrics_.proposalTotalUs += elapsed;
            if(result.status != ApplyStatus::kOk && result.status != ApplyStatus::kAlreadyApplied)
                ++metrics_.proposalFailureCount;
        }
        logCommandSpan(lastCommand, result, appendElapsed, quorumElapsed, decodeElapsed, elapsed, commands.size());
        return result;
    };
    if(!server_ || !server_->is_initialized() || !server_->is_leader())
        return record(unavailable(lastCommand, "not metadata leader"));
    std::vector<nuraft::ptr<nuraft::buffer>> buffers;
    buffers.reserve(commands.size());
    for(const auto& command : commands) {
        const auto encoded = encodeMetadataCommand(command);
        if(!encoded) {
            ApplyResult result = unavailable(lastCommand, "invalid metadata command batch");
            result.status = ApplyStatus::kInvalid; return record(std::move(result));
        }
        buffers.push_back(makeBuffer(*encoded));
    }
    const auto appendStarted = std::chrono::steady_clock::now();
    const auto appendResult = server_->append_entries(buffers);
    appendElapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - appendStarted).count());
    {
        std::lock_guard<std::mutex> metricsLock(metricsMutex_);
        metrics_.raftAppendTotalUs += appendElapsed;
        ++metrics_.raftAppendCount;
    }
    if(!appendResult || !appendResult->get_accepted() || appendResult->get_result_code() != nuraft::cmd_result_code::OK)
        return record(unavailable(lastCommand, appendResult ? appendResult->get_result_str() : "Raft append failed"));
    const auto quorumStarted = std::chrono::steady_clock::now();
    auto committed = appendResult->get();
    quorumElapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - quorumStarted).count());
    {
        std::lock_guard<std::mutex> metricsLock(metricsMutex_);
        ++metrics_.quorumWaitCount;
        metrics_.quorumWaitTotalUs += quorumElapsed;
    }
    if(!committed) return record(unavailable(lastCommand, "Raft batch commit returned no state-machine result"));
    const auto decodeStarted = std::chrono::steady_clock::now();
    const auto decoded = NuRaftStateMachine::decodeResult(*committed);
    decodeElapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - decodeStarted).count());
    return record(decoded ? *decoded : unavailable(lastCommand, "invalid state-machine batch result"));
}

bool NuRaftMetadataService::heartbeat(const NodeHeartbeat& heartbeat, std::string* error)
{
    if(!server_ || !server_->is_leader()) { if(error) *error = "not metadata leader"; return false; }
    const auto durable = machine_->node(heartbeat.nodeId);
    if(!durable) { if(error) *error = "node is not registered"; return false; }
    bool needsOnlineTransition = false;
    {
        std::lock_guard<std::mutex> lock(softMutex_);
        const auto term = server_->get_term();
        if(term != observedTerm_) { softState_.beginLeaderTerm(term, heartbeat.observedAtMs); observedTerm_ = term; }
        if(!softState_.update(*durable, heartbeat)) { if(error) *error = "stale node epoch or heartbeat"; return false; }
        needsOnlineTransition = durable->health != NodeHealth::kOnline;
    }
    if(needsOnlineTransition) {
        MetadataCommand command;
        command.commandId = "node-online-" + durable->nodeId + "-" + std::to_string(durable->nodeEpoch);
        command.type = MetadataCommandType::kMarkNodeHealth;
        command.actorType = "metadata-heartbeat";
        command.actorId = durable->nodeId;
        command.nodeEpoch = durable->nodeEpoch;
        command.issuedAt = heartbeat.observedAtMs / 1000;
        command.payload = MarkNodeHealthPayload{durable->nodeId, NodeHealth::kOnline};
        const auto result = propose(command);
        if(result.status != ApplyStatus::kOk && result.status != ApplyStatus::kAlreadyApplied) {
            if(error) *error = result.message.empty() ? "failed to persist node online transition" : result.message;
            return false;
        }
    }
    return true;
}

ApplyResult NuRaftMetadataService::reserveLease(const ReserveLeaseRequest& request)
{
    MetadataCommand command; command.commandId = request.commandId; command.type = MetadataCommandType::kReserveLease;
    command.actorType = request.actorType; command.actorId = request.actorId; command.generation = request.generation;
    if(!server_ || !server_->is_leader()) return unavailable(command, "not metadata leader");
    struct Candidate { NodeRecord node; NodeHeartbeat heartbeat; uint64_t available; };
    const auto placementStarted = std::chrono::steady_clock::now();
    std::vector<Candidate> candidates;
    {
        std::lock_guard<std::mutex> lock(softMutex_);
        const auto term = server_->get_term();
        if(term != observedTerm_) { softState_.beginLeaderTerm(term, request.nowMs); observedTerm_ = term; }
        for(const auto& node : machine_->nodes()) {
            const auto heartbeat = softState_.heartbeat(node.nodeId);
            if(node.health != NodeHealth::kOnline || node.draining || !heartbeat || heartbeat->nodeEpoch != node.nodeEpoch
               || !softState_.isFresh(node.nodeId, request.nowMs, heartbeatFreshnessMs_)) continue;
            const uint64_t reported = std::min(node.registeredCapacityBytes, heartbeat->freeBytes);
            const uint64_t available = reported > node.reservedBytes ? reported - node.reservedBytes : 0;
            if(available >= request.chunkSize) candidates.push_back({node, *heartbeat, available});
        }
    }
    std::sort(candidates.begin(), candidates.end(), [](const Candidate& a, const Candidate& b) {
        if(a.heartbeat.activeUploads != b.heartbeat.activeUploads) return a.heartbeat.activeUploads < b.heartbeat.activeUploads;
        if(a.heartbeat.queueDepth != b.heartbeat.queueDepth) return a.heartbeat.queueDepth < b.heartbeat.queueDepth;
        if(a.available != b.available) return a.available > b.available; return a.node.nodeId < b.node.nodeId;
    });
    const auto placementElapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - placementStarted).count());
    {
        std::lock_guard<std::mutex> metricsLock(metricsMutex_);
        ++metrics_.placementCount; metrics_.placementTotalUs += placementElapsed;
    }
    if(request.desiredRf == 0 || candidates.size() < request.desiredRf)
        return unavailable(command, "insufficient fresh eligible DataNodes");
    const auto snapshotNodes = machine_->nodes(); uint64_t placementEpoch = 0;
    for(const auto& node : snapshotNodes) placementEpoch = std::max(placementEpoch, node.placementEpoch);
    command.placementEpoch = placementEpoch;
    ReserveLeasePayload payload; payload.leaseId = request.leaseId; payload.requestKey = request.requestKey;
    payload.sessionId = request.sessionId; payload.chunkIndex = request.chunkIndex; payload.routeKey = request.routeKey;
    payload.chunkSize = request.chunkSize; payload.expiresAt = request.expiresAt;
    for(uint32_t i = 0; i < request.desiredRf; ++i) payload.targets.push_back({candidates[i].node.nodeId, candidates[i].node.nodeEpoch});
    command.payload = std::move(payload); return propose(command);
}

std::optional<std::vector<MetadataCommand>> NuRaftMetadataService::buildReserveCommands(
    const std::vector<ReserveLeaseRequest>& requests, ApplyResult& failure)
{
    if(requests.empty()) {
        failure.status = ApplyStatus::kInvalid; failure.message = "empty lease batch"; return std::nullopt;
    }
    if(!server_ || !server_->is_leader()) {
        MetadataCommand command;
        command.commandId = requests.back().commandId;
        command.type = MetadataCommandType::kReserveLease;
        failure = unavailable(command, "not metadata leader");
        return std::nullopt;
    }
    struct Candidate { NodeRecord node; NodeHeartbeat heartbeat; uint64_t available = 0; };
    std::map<std::string, uint64_t> plannedReservations;
    std::vector<MetadataCommand> commands;
    commands.reserve(requests.size());
    uint64_t placementEpoch = 0;
    {
        std::lock_guard<std::mutex> lock(softMutex_);
        const auto term = server_->get_term();
        int64_t observation = requests.front().nowMs;
        if(term != observedTerm_) { softState_.beginLeaderTerm(term, observation); observedTerm_ = term; }
        for(const auto& node : machine_->nodes()) placementEpoch = std::max(placementEpoch, node.placementEpoch);
        for(const auto& request : requests) {
            std::vector<Candidate> candidates;
            for(const auto& node : machine_->nodes()) {
                const auto heartbeat = softState_.heartbeat(node.nodeId);
                if(node.health != NodeHealth::kOnline || node.draining || !heartbeat || heartbeat->nodeEpoch != node.nodeEpoch
                   || !softState_.isFresh(node.nodeId, request.nowMs, heartbeatFreshnessMs_)) continue;
                const uint64_t reported = std::min(node.registeredCapacityBytes, heartbeat->freeBytes);
                const uint64_t reserved = node.reservedBytes + plannedReservations[node.nodeId];
                const uint64_t available = reported > reserved ? reported - reserved : 0;
                if(available >= request.chunkSize) candidates.push_back({node, *heartbeat, available});
            }
            std::sort(candidates.begin(), candidates.end(), [](const Candidate& a, const Candidate& b) {
                if(a.heartbeat.activeUploads != b.heartbeat.activeUploads) return a.heartbeat.activeUploads < b.heartbeat.activeUploads;
                if(a.heartbeat.queueDepth != b.heartbeat.queueDepth) return a.heartbeat.queueDepth < b.heartbeat.queueDepth;
                if(a.available != b.available) return a.available > b.available;
                return a.node.nodeId < b.node.nodeId;
            });
            MetadataCommand command; command.commandId = request.commandId; command.type = MetadataCommandType::kReserveLease;
            command.actorType = request.actorType; command.actorId = request.actorId; command.generation = request.generation;
            command.placementEpoch = placementEpoch;
            if(request.desiredRf == 0 || candidates.size() < request.desiredRf) {
                failure = unavailable(command, "insufficient fresh eligible DataNodes for requested RF");
                return std::nullopt;
            }
            ReserveLeasePayload payload; payload.leaseId = request.leaseId; payload.requestKey = request.requestKey;
            payload.sessionId = request.sessionId; payload.chunkIndex = request.chunkIndex; payload.routeKey = request.routeKey;
            payload.chunkSize = request.chunkSize; payload.expiresAt = request.expiresAt;
            for(uint32_t i = 0; i < request.desiredRf; ++i) {
                payload.targets.push_back({candidates[i].node.nodeId, candidates[i].node.nodeEpoch});
                plannedReservations[candidates[i].node.nodeId] += request.chunkSize;
            }
            command.payload = std::move(payload);
            commands.push_back(std::move(command));
        }
    }
    return commands;
}

ApplyResult NuRaftMetadataService::reserveLeaseBatch(const std::vector<ReserveLeaseRequest>& requests)
{
    ApplyResult failure;
    const auto commands = buildReserveCommands(requests, failure);
    if(!commands) return failure;
    // All commands share one append_entries call and one WAL fdatasync.  The
    // state machine still applies each lease independently and validates the
    // placement epoch/node epoch/capacity on every entry.
    return proposeBatch(*commands);
}

ApplyResult NuRaftMetadataService::commitChunk(const std::string& sessionId,
                                               uint32_t index,
                                               uint64_t size,
                                               const std::vector<std::string>& successfulNodes,
                                               const std::string& leaseId)
{
    MetadataCommand command;
    command.commandId = "commit-chunk-" + sessionId + "-" + std::to_string(index);
    command.type = MetadataCommandType::kCommitChunk;
    command.actorType = "datanode";
    command.actorId = successfulNodes.empty() ? std::string{} : successfulNodes.front();
    command.payload = CommitChunkPayload{};
    if(sessionId.empty() || leaseId.empty() || successfulNodes.empty() || size == 0)
        return unavailable(command, "invalid chunk commit request");
    if(!server_ || !server_->is_initialized() || !server_->is_leader())
        return unavailable(command, "not metadata leader");

    const auto sessionRecord = machine_->session(sessionId);
    const auto leaseRecord = machine_->lease(leaseId);
    if(!sessionRecord || !leaseRecord || leaseRecord->state != LeaseState::kActive)
        return unavailable(command, "session or active lease not found");
    const auto chunkRecord = machine_->chunk(sessionRecord->objectId, index);
    if(!chunkRecord) return unavailable(command, "chunk route not found");
    command.commandId += "-" + std::to_string(chunkRecord->generation);
    command.generation = chunkRecord->generation;
    auto& payload = std::get<CommitChunkPayload>(command.payload);
    payload.sessionId = sessionId;
    payload.leaseId = leaseId;
    payload.chunkIndex = index;
    payload.routeKey = chunkRecord->routeKey;
    payload.chunkSize = size;
    payload.checksumType = chunkRecord->checksumType;
    payload.checksumDigest = chunkRecord->checksumDigest;
    const int64_t verifiedAt = static_cast<int64_t>(std::time(nullptr));
    for(const auto& nodeId : successfulNodes) {
        const auto target = std::find_if(leaseRecord->targets.begin(), leaseRecord->targets.end(),
            [&](const LeaseTarget& item) { return item.nodeId == nodeId; });
        if(target == leaseRecord->targets.end()) return unavailable(command, "successful node is not in lease target set");
        payload.replicas.push_back({nodeId, target->nodeEpoch, chunkRecord->checksumDigest, verifiedAt});
    }

    std::vector<MetadataCommand> commands;
    commands.push_back(command);
    // The final chunk is the only point at which CommitFile can be safely
    // coalesced for a multi-chunk object: all preceding chunks are already
    // committed in the applied state observed above.  Keep the two commands
    // independently idempotent, but share their Raft WAL durability barrier.
    // If concurrent chunk commits make this snapshot stale, the normal public
    // CommitFile request remains the correctness fallback.
    if(sessionRecord->completedChunks + 1 == sessionRecord->totalChunks) {
        const auto objectRecord = machine_->object(sessionRecord->objectId);
        if(!objectRecord) return unavailable(command, "object record not found");
        MetadataCommand fileCommand;
        fileCommand.commandId = "commit-file-" + sessionId;
        fileCommand.type = MetadataCommandType::kCommitFile;
        fileCommand.actorType = "datanode";
        fileCommand.actorId = command.actorId;
        fileCommand.payload = CommitFilePayload{sessionId, sessionRecord->objectId,
                                                sessionRecord->objectVersion,
                                                objectRecord->contentHash};
        commands.push_back(std::move(fileCommand));
    }
    return proposeBatch(commands);
}

ApplyResult NuRaftMetadataService::createSessionAndReserve(
    const MetadataCommand& create, const std::vector<ReserveLeaseRequest>& requests)
{
    ApplyResult failure;
    const auto reservations = buildReserveCommands(requests, failure);
    if(!reservations) return failure;
    std::vector<MetadataCommand> commands;
    commands.reserve(1 + reservations->size());
    commands.push_back(create);
    commands.insert(commands.end(), reservations->begin(), reservations->end());
    // CreateSession is first in the same replicated batch, so every reserve
    // sees the session/chunk records while the WAL performs one durability
    // boundary for the complete preflight.
    return proposeBatch(commands);
}

std::optional<ReadFence> NuRaftMetadataService::linearizableReadBarrier(const std::string& id)
{
    // NuRaft's leader-alive check performs the quorum confirmation for the
    // current term.  Once the leader has observed a committed index, wait for
    // the local state machine to apply that index and serve the read without
    // appending a permanent no-op entry.  This is the ReadIndex-style path;
    // the standalone MetadataService intentionally keeps the no-op barrier as
    // its correctness baseline.
    (void)id;
    const auto started = std::chrono::steady_clock::now();
    auto record = [&](std::optional<ReadFence> fence) {
        const auto elapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - started).count());
        std::lock_guard<std::mutex> metricsLock(metricsMutex_);
        ++metrics_.readIndexCount; metrics_.readIndexTotalUs += elapsed;
        if(!fence) ++metrics_.readIndexFailureCount;
        return fence;
    };
    if(!server_ || !server_->is_initialized() || !server_->is_leader() || !server_->is_leader_alive())
        return record(std::nullopt);
    const uint64_t term = server_->get_term();
    const uint64_t readIndex = server_->get_committed_log_idx();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while(machine_ && machine_->last_commit_index() < readIndex &&
          std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if(!machine_ || machine_->last_commit_index() < readIndex) return record(std::nullopt);
    return record(ReadFence{term, readIndex, readIndex});
}

std::optional<UploadSessionRecord> NuRaftMetadataService::session(const std::string& id) const { return machine_->session(id); }
std::vector<UploadSessionRecord> NuRaftMetadataService::sessions() const { return machine_ ? machine_->sessions() : std::vector<UploadSessionRecord>{}; }
std::optional<LeaseRecord> NuRaftMetadataService::lease(const std::string& id) const { return machine_->lease(id); }
std::vector<NodeRecord> NuRaftMetadataService::nodes() const { return machine_ ? machine_->nodes() : std::vector<NodeRecord>{}; }
std::optional<ObjectRecord> NuRaftMetadataService::object(const std::string& id) const { return machine_->object(id); }
std::optional<ChunkRouteRecord> NuRaftMetadataService::chunk(const std::string& objectId, uint32_t index) const { return machine_->chunk(objectId, index); }
std::optional<ReadDescriptor> NuRaftMetadataService::readDescriptor(const std::string& id, uint64_t version) const { return machine_->readDescriptor(id, version); }
std::vector<DirectoryRecord> NuRaftMetadataService::directories(const std::string& ownerId, const std::string& parentPath) const { return machine_ ? machine_->directories(ownerId, parentPath) : std::vector<DirectoryRecord>{}; }
std::vector<ObjectRecord> NuRaftMetadataService::objects(const std::string& ownerId, const std::string& parentPath) const { return machine_ ? machine_->objects(ownerId, parentPath) : std::vector<ObjectRecord>{}; }
std::vector<DeleteTaskRecord> NuRaftMetadataService::deleteTasks(const std::string& nodeId) const { return machine_ ? machine_->deleteTasks(nodeId) : std::vector<DeleteTaskRecord>{}; }
std::string NuRaftMetadataService::stateDigest() const { return machine_ ? machine_->stateDigest() : std::string(); }

ConsensusStatus NuRaftMetadataService::status() const
{
    ConsensusStatus status; status.memberId = std::to_string(serverId_);
    if(!server_) { status.role = "stopped"; status.leaderResolved = false; status.quorumWritable = false; return status; }
    status.term = server_->get_term(); const int leader = server_->get_leader();
    status.leaderId = leader > 0 ? std::to_string(leader) : ""; status.leaderResolved = leader > 0;
    status.role = server_->is_leader() ? "leader" : "follower";
    status.quorumWritable = server_->is_leader() && server_->is_leader_alive();
    status.commitIndex = server_->get_committed_log_idx();
    status.lastApplied = machine_ ? machine_->last_commit_index() : 0;
    return status;
}

MetadataMetricsSnapshot NuRaftMetadataService::metrics() const
{
    std::lock_guard<std::mutex> lock(metricsMutex_);
    return metrics_;
}

} // namespace miniKV::metadata::raft
