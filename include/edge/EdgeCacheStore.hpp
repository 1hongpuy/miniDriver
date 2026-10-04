#pragma once

#include "client/MiniDriverClient.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

namespace miniKV::edge {

struct EdgeCacheKey {
    std::string id;
    uint64_t size = 0;
    std::string checksumType;
    std::string checksumDigest;

    static EdgeCacheKey fromChunk(const client::ObjectRef& object,
                                  const client::ChunkReadPlan& chunk);
};

class EdgeCacheStore;

class CacheLease {
public:
    struct State;
    CacheLease() = default;
    CacheLease(const CacheLease&) = delete;
    CacheLease& operator=(const CacheLease&) = delete;
    CacheLease(CacheLease&& other) noexcept;
    CacheLease& operator=(CacheLease&& other) noexcept;
    ~CacheLease();

    bool valid() const noexcept { return state_ != nullptr; }
    const std::filesystem::path& path() const noexcept { return path_; }
    uint64_t size() const noexcept { return size_; }
    void reset() noexcept;

private:
    friend class EdgeCacheStore;
    CacheLease(std::shared_ptr<State> state, std::string id,
               std::filesystem::path path, uint64_t size);

    std::shared_ptr<State> state_;
    std::string id_;
    std::filesystem::path path_;
    uint64_t size_ = 0;
};

class CacheReservation {
public:
    CacheReservation() = default;
    CacheReservation(const CacheReservation&) = delete;
    CacheReservation& operator=(const CacheReservation&) = delete;
    CacheReservation(CacheReservation&& other) noexcept;
    CacheReservation& operator=(CacheReservation&& other) noexcept;
    ~CacheReservation();

    bool valid() const noexcept { return state_ != nullptr; }
    uint64_t bytes() const noexcept { return bytes_; }
    void reset() noexcept;

private:
    friend class EdgeCacheStore;
    CacheReservation(std::shared_ptr<CacheLease::State> state, std::string id, uint64_t bytes);

    std::shared_ptr<CacheLease::State> state_;
    std::string id_;
    uint64_t bytes_ = 0;
};

class EdgeCacheStore {
public:
    struct Config {
        std::filesystem::path root;
        uint64_t capacityBytes = 0;  // 0 means cache disabled.
        bool verifyHitChecksum = true;
    };

    struct Stats {
        uint64_t readyBytes = 0;
        uint64_t reservedBytes = 0;
        uint64_t entries = 0;
        uint64_t pinnedEntries = 0;
        uint64_t hits = 0;
        uint64_t misses = 0;
        uint64_t rejectedReservations = 0;
        uint64_t recoveredEntries = 0;
    };

    explicit EdgeCacheStore(Config config);
    bool initialize(std::string& error);

    bool acquire(const EdgeCacheKey& key, CacheLease& out, std::string& error);
    // Wait for a concurrent immutable fill of this exact key to publish. This
    // coalesces independent HTTP ranges that require the same cold Chunk.
    bool waitForReady(const EdgeCacheKey& key, CacheLease& out,
                      std::chrono::milliseconds timeout, std::string& error);
    bool reserve(const EdgeCacheKey& key, CacheReservation& out, std::string& error);
    bool publish(const EdgeCacheKey& key, const std::string& bytes,
                 CacheReservation& reservation, CacheLease& out, std::string& error);
    Stats stats() const;

private:
    std::shared_ptr<CacheLease::State> state_;
};

}  // namespace miniKV::edge
