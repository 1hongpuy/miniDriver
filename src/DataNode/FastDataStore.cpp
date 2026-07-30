#include "DataNode/FastDataStore.hpp"
#include "utils/Util.hpp"

#include <cerrno>
#include <filesystem>
#include <fcntl.h>
#include <leveldb/db.h>
#include <openssl/evp.h>
#include <sys/stat.h>
#include <unistd.h>

namespace miniKV::datanode {

using namespace util;

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
    if (EVP_DigestUpdate(static_cast<EVP_MD_CTX*>(digestContext_), bytes, size) != 1) {
        failed_ = true;
        return false;
    }
    if (!discard_) {
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
    }
    writtenBytes_ += size;
    return true;
}

bool FastDataStore::WriteSession::finish(bool& alreadyExists) {
    alreadyExists = false;
    if (finished_ || failed_ || writtenBytes_ != expectedSize_) return false;
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
        failed_ = true;
        return false;
    }

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
    return true;
}

std::unique_ptr<FastDataStore::WriteSession> FastDataStore::beginPut(
    const std::string& expectedHash, uint64_t expectedSize) {
    if (expectedHash.empty() || expectedSize == 0 || dataFd_ < 0) return nullptr;
    std::lock_guard<std::mutex> lock(mutex_);
    PhysicalExtent existing;
    const bool alreadyPresent = findExtentLocked(expectedHash, existing);
    const uint64_t offset = nextOffset_;
    if (!alreadyPresent) nextOffset_ += expectedSize;
    return std::unique_ptr<WriteSession>(
        new WriteSession(this, expectedHash, expectedSize, offset, alreadyPresent));
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

}  // namespace miniKV::v2
