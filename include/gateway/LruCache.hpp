#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace miniKV::gateway {

template <typename Key, typename Value, typename Hash = std::hash<Key>>
class LruCache {
public:
    using ValuePtr = std::shared_ptr<const Value>;
    using Clock = std::function<int64_t()>;

    struct Config {
        size_t maxEntries = 0;
        size_t maxBytes = 0;
    };

    struct Stats {
        uint64_t hits = 0;
        uint64_t misses = 0;
        uint64_t evictions = 0;
        uint64_t expired = 0;
        uint64_t rejections = 0;
    };

    explicit LruCache(Config config, Clock clock = defaultClock)
        : config_(config), clock_(std::move(clock)) {}

    ValuePtr get(const Key& key)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto found = index_.find(key);
        if (found == index_.end()) {
            ++stats_.misses;
            return nullptr;
        }
        if (isExpired(found->second->expiresAt)) {
            eraseLocked(found->second);
            ++stats_.expired;
            ++stats_.misses;
            return nullptr;
        }
        recency_.splice(recency_.begin(), recency_, found->second);
        ++stats_.hits;
        return recency_.front().value;
    }

    bool put(Key key, ValuePtr value, size_t bytes, int64_t ttlSeconds)
    {
        if (value == nullptr || config_.maxEntries == 0 || config_.maxBytes == 0 ||
            bytes > config_.maxBytes) {
            std::lock_guard<std::mutex> lock(mutex_);
            ++stats_.rejections;
            return false;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto existing = index_.find(key);
        if (existing != index_.end()) eraseLocked(existing->second);

        Entry entry;
        entry.key = std::move(key);
        entry.value = std::move(value);
        entry.bytes = bytes;
        entry.expiresAt = expiryFor(ttlSeconds);
        recency_.push_front(std::move(entry));
        index_[recency_.front().key] = recency_.begin();
        currentBytes_ += bytes;
        trimLocked();
        return true;
    }

    bool erase(const Key& key)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto found = index_.find(key);
        if (found == index_.end()) return false;
        eraseLocked(found->second);
        return true;
    }

    void clear()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        recency_.clear();
        index_.clear();
        currentBytes_ = 0;
    }

    size_t size() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return index_.size();
    }

    size_t bytes() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return currentBytes_;
    }

    Stats stats() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return stats_;
    }

private:
    struct Entry {
        Key key;
        ValuePtr value;
        size_t bytes = 0;
        int64_t expiresAt = std::numeric_limits<int64_t>::max();
    };

    using EntryList = std::list<Entry>;
    using Iterator = typename EntryList::iterator;

    static int64_t defaultClock()
    {
        return std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    int64_t expiryFor(int64_t ttlSeconds) const
    {
        if (ttlSeconds <= 0) return std::numeric_limits<int64_t>::max();
        const int64_t now = clock_();
        if (now > std::numeric_limits<int64_t>::max() - ttlSeconds) {
            return std::numeric_limits<int64_t>::max();
        }
        return now + ttlSeconds;
    }

    bool isExpired(int64_t expiresAt) const
    {
        return expiresAt != std::numeric_limits<int64_t>::max() && clock_() >= expiresAt;
    }

    void eraseLocked(Iterator entry)
    {
        currentBytes_ -= entry->bytes;
        index_.erase(entry->key);
        recency_.erase(entry);
    }

    void trimLocked()
    {
        while (!recency_.empty() &&
               (index_.size() > config_.maxEntries || currentBytes_ > config_.maxBytes)) {
            const auto oldest = std::prev(recency_.end());
            if (isExpired(oldest->expiresAt)) ++stats_.expired;
            else ++stats_.evictions;
            eraseLocked(oldest);
        }
    }

    Config config_;
    Clock clock_;
    mutable std::mutex mutex_;
    EntryList recency_;
    std::unordered_map<Key, Iterator, Hash> index_;
    size_t currentBytes_ = 0;
    Stats stats_;
};

}  // namespace miniKV::gateway
