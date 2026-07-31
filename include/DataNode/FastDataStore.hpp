#pragma once

#include <cstdint>
#include <memory>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <sys/types.h>

namespace leveldb {
class DB;
}

namespace miniKV::datanode {

struct PhysicalExtent { uint64_t offset = 0; uint64_t length = 0; };
struct FileRegion { off_t offset = 0; size_t length = 0; };

class FastDataStore {
public:
    class WriteSession {
    public:
        ~WriteSession();

        bool append(const char* bytes, size_t size);
        bool finish(bool& alreadyExists);
        void abort();
        uint64_t writtenBytes() const { return writtenBytes_; }

    private:
        friend class FastDataStore;
        WriteSession(FastDataStore* store, std::string expectedHash,
                     uint64_t expectedSize, uint64_t offset, bool discard);

        FastDataStore* store_;
        std::string expectedHash_;
        uint64_t expectedSize_ = 0;
        uint64_t offset_ = 0;
        uint64_t writtenBytes_ = 0;
        void* digestContext_ = nullptr;
        bool discard_ = false;
        bool finished_ = false;
        bool failed_ = false;
    };

    explicit FastDataStore(std::string dataDirectory);
    ~FastDataStore();

    bool open();
    std::unique_ptr<WriteSession> beginPut(const std::string& expectedHash,
                                           uint64_t expectedSize);
    bool put(const std::string& expectedHash, const std::string& bytes, bool& alreadyExists);
    bool get(const std::string& chunkHash, std::string& out) const;
    bool getRegion(const std::string& chunkHash, FileRegion& out) const;
    std::string dataFilePath() const { return dataDirectory_ + "/disk0.data"; }
    bool exists(const std::string& chunkHash) const;
    bool remove(const std::string& chunkHash, bool& removed);
    uint64_t usedBytes() const;
    uint64_t reusableBytes() const;

private:
    bool findExtentLocked(const std::string& chunkHash, PhysicalExtent& extent) const;
    bool putExtentLocked(const std::string& chunkHash, const PhysicalExtent& extent);
    bool loadFreeExtentsLocked();
    bool allocateExtentLocked(uint64_t length, PhysicalExtent& extent);
    bool addFreeExtentLocked(const PhysicalExtent& extent);

    std::string dataDirectory_;
    int dataFd_ = -1;
    uint64_t nextOffset_ = 0;
    // This is a local physical index only. Gateway metadata remains the source
    // of truth for files and replica routes.
    std::unique_ptr<leveldb::DB> indexDb_;
    mutable std::mutex mutex_;
    std::map<uint64_t, uint64_t> freeExtents_;
};

}  // namespace miniKV::v2
