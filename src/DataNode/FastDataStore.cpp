#include "DataNode/FastDataStore.hpp"
#include "utils/Util.hpp"

#include <chrono>
#include <cerrno>
#include <filesystem>
#include <fcntl.h>
#include <leveldb/db.h>
#include <leveldb/iterator.h>
#include <leveldb/write_batch.h>
#include <limits>
#include <openssl/evp.h>
#include <sys/stat.h>
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

FastDataStore::FastDataStore(std::string dataDirectory)
    : dataDirectory_(std::move(dataDirectory)) {}

FastDataStore::~FastDataStore() {
    indexDb_.reset();
    if (dataFd_ >= 0) ::close(dataFd_);
}

FastDataStore::WriteSession::WriteSession(FastDataStore* store,
                                          std::string expectedHash,
                                          uint64_t expectedSize,
                                          uint64_t offset,
                                          bool discard)
    : store_(store), expectedHash_(std::move(expectedHash)),
      expectedSize_(expectedSize), offset_(offset), discard_(discard) {
    digestContext_ = EVP_MD_CTX_new();
    if (digestContext_ == nullptr ||
        EVP_DigestInit_ex(static_cast<EVP_MD_CTX*>(digestContext_), EVP_sha256(), nullptr) != 1) {
        failed_ = true;
    }
}

FastDataStore::WriteSession::~WriteSession() {
    abort();
    if (digestContext_ != nullptr) EVP_MD_CTX_free(static_cast<EVP_MD_CTX*>(digestContext_));
}

bool FastDataStore::WriteSession::append(const char* bytes, size_t size) {
    if (failed_ || finished_ || bytes == nullptr || size == 0 ||
        writtenBytes_ + size > expectedSize_) {
        failed_ = true;
        return false;
    }
    const auto hashStarted = Clock::now();
    const int hashResult = EVP_DigestUpdate(static_cast<EVP_MD_CTX*>(digestContext_), bytes, size);
    metrics_.shaUpdateNanoseconds += elapsedNanoseconds(hashStarted, Clock::now());
    if (hashResult != 1) {
        failed_ = true;
        return false;
    }
    if (!discard_) {
        const auto writeStarted = Clock::now();
        size_t written = 0;
        while (written < size) {
            const ssize_t n = ::pwrite(store_->dataFd_, bytes + written, size - written,
                                       static_cast<off_t>(offset_ + writtenBytes_ + written));
            if (n <= 0) {
                failed_ = true;
                return false;
            }
            written += static_cast<size_t>(n);
        }
        metrics_.pwriteNanoseconds += elapsedNanoseconds(writeStarted, Clock::now());
    }
    writtenBytes_ += size;
    return true;
}

bool FastDataStore::WriteSession::finish(bool& alreadyExists) {
    alreadyExists = false;
    if (finished_ || failed_ || writtenBytes_ != expectedSize_) return false;
    const auto finalizeStarted = Clock::now();
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digestSize = 0;
    if (EVP_DigestFinal_ex(static_cast<EVP_MD_CTX*>(digestContext_), digest, &digestSize) != 1) {
        failed_ = true;
        return false;
    }
    std::string actualHash;
    static constexpr char kHex[] = "0123456789abcdef";
    actualHash.reserve(digestSize * 2);
    for (unsigned int i = 0; i < digestSize; ++i) {
        actualHash += kHex[(digest[i] >> 4) & 0x0f];
        actualHash += kHex[digest[i] & 0x0f];
    }
    if (actualHash != expectedHash_) {
        metrics_.shaFinalizeNanoseconds += elapsedNanoseconds(finalizeStarted, Clock::now());
        failed_ = true;
        return false;
    }
    metrics_.shaFinalizeNanoseconds += elapsedNanoseconds(finalizeStarted, Clock::now());

    const auto indexStarted = Clock::now();
    std::lock_guard<std::mutex> lock(store_->mutex_);
    PhysicalExtent existing;
    if (store_->findExtentLocked(expectedHash_, existing)) {
        alreadyExists = true;
    } else if (!discard_) {
        if (!store_->putExtentLocked(expectedHash_, {offset_, expectedSize_})) {
            failed_ = true;
            return false;
        }
    } else {
        // The matching extent was already present when the request started.
        alreadyExists = true;
    }
    metrics_.indexNanoseconds += elapsedNanoseconds(indexStarted, Clock::now());
    finished_ = true;
    return true;
}

void FastDataStore::WriteSession::abort() {
    if (!finished_) failed_ = true;
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
    return loadFreeExtentsLocked();
}

std::unique_ptr<FastDataStore::WriteSession> FastDataStore::beginPut(
    const std::string& expectedHash, uint64_t expectedSize) {
    if (expectedHash.empty() || expectedSize == 0 || dataFd_ < 0) return nullptr;
    std::lock_guard<std::mutex> lock(mutex_);
    PhysicalExtent existing;
    const bool alreadyPresent = findExtentLocked(expectedHash, existing);
    PhysicalExtent extent;
    if (!alreadyPresent) {
        if (!allocateExtentLocked(expectedSize, extent)) {
            if (nextOffset_ > std::numeric_limits<uint64_t>::max() - expectedSize) return nullptr;
            extent = {nextOffset_, expectedSize};
            nextOffset_ += expectedSize;
        }
    }
    return std::unique_ptr<WriteSession>(
        new WriteSession(this, expectedHash, expectedSize, extent.offset, alreadyPresent));
}

bool FastDataStore::put(const std::string& expectedHash, const std::string& bytes, bool& alreadyExists) {
    auto session = beginPut(expectedHash, bytes.size());
    return session != nullptr && session->append(bytes.data(), bytes.size()) &&
           session->finish(alreadyExists);
}

bool FastDataStore::get(const std::string& chunkHash, std::string& out) const {
    PhysicalExtent extent;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!findExtentLocked(chunkHash, extent)) return false;
    }
    out.resize(extent.length);
    size_t received = 0;
    while (received < out.size()) {
        const ssize_t n = ::pread(dataFd_, out.data() + received, out.size() - received,
                                  static_cast<off_t>(extent.offset + received));
        if (n <= 0) return false;
        received += static_cast<size_t>(n);
    }
    return sha256Hex(out.data(), out.size()) == chunkHash;
}

bool FastDataStore::getRegion(const std::string& chunkHash, FileRegion& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    PhysicalExtent extent;
    if(!findExtentLocked(chunkHash, extent)) return false;
    out.offset = static_cast<off_t>(extent.offset);
    out.length = static_cast<size_t>(extent.length);
    return true;
}

bool FastDataStore::exists(const std::string& chunkHash) const {
    std::lock_guard<std::mutex> lock(mutex_);
    PhysicalExtent extent;
    return findExtentLocked(chunkHash, extent);
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
    if (!findExtentLocked(chunkHash, extent)) return true;

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
    batch.Delete("e:" + chunkHash);
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

bool FastDataStore::findExtentLocked(const std::string& chunkHash, PhysicalExtent& extent) const {
    if (indexDb_ == nullptr) return false;
    std::string value;
    const leveldb::Status status = indexDb_->Get(
        leveldb::ReadOptions(), "e:" + chunkHash, &value);
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

bool FastDataStore::putExtentLocked(const std::string& chunkHash, const PhysicalExtent& extent) {
    if (indexDb_ == nullptr || extent.length == 0) return false;
    const std::string value = std::to_string(extent.offset) + ":" + std::to_string(extent.length);
    return indexDb_->Put(leveldb::WriteOptions(), "e:" + chunkHash, value).ok();
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
