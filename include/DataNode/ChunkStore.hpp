#pragma once

#include "DataNode/FastDataStore.hpp"

#include <cstdint>
#include <memory>

namespace miniKV::datanode {

// Protocol-independent storage boundary used by ChunkWriteCoordinator.  The
// current adapter preserves FastDataStore's extent, checksum and durability
// implementation without exposing HTTP concerns to that implementation.
class ChunkStore {
public:
    using WriteHandle = std::shared_ptr<FastDataStore::WriteSession>;

    virtual ~ChunkStore() = default;
    virtual bool canAccept(uint64_t bytes) const = 0;
    virtual WriteHandle begin(const FastDataStore::PutOptions& options,
                              uint64_t bytes) = 0;
    virtual FastDataStore::DurabilityMetrics durabilityMetrics() const = 0;
};

class FastDataStoreChunkStore final : public ChunkStore {
public:
    explicit FastDataStoreChunkStore(FastDataStore& store) : store_(store) {}

    bool canAccept(uint64_t bytes) const override
    {
        return store_.canAcceptDurability(bytes);
    }

    WriteHandle begin(const FastDataStore::PutOptions& options,
                      uint64_t bytes) override
    {
        return store_.beginPut(options, bytes);
    }

    FastDataStore::DurabilityMetrics durabilityMetrics() const override
    {
        return store_.durabilityMetrics();
    }

private:
    FastDataStore& store_;
};

}  // namespace miniKV::datanode
