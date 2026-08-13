#pragma once

#include "http/PersistentHttpSession.hpp"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

namespace miniKV::network { class EventLoop; }

namespace miniKV::datanode {

struct ReplicaPoolKey {
    std::string nodeId;
    std::string address;
    uint16_t port = 0;

    bool operator==(const ReplicaPoolKey& other) const {
        return nodeId == other.nodeId && address == other.address && port == other.port;
    }
};

struct ReplicaPoolKeyHash {
    size_t operator()(const ReplicaPoolKey& key) const;
};

struct ReplicaConnectionPoolConfig {
    size_t maxSessionsPerTarget = 2;
    size_t maxSessions = 4;
};

struct ReplicaConnectionPoolMetrics {
    uint64_t created = 0;
    uint64_t reused = 0;
    uint64_t rejected = 0;
    uint64_t discarded = 0;
    size_t idle = 0;
    size_t borrowed = 0;
    size_t peakBorrowed = 0;
};

// Thread-affine pool. A pool instance belongs to exactly one EventLoop;
// sessions never cross EventLoop boundaries.
class ReplicaConnectionPool : public std::enable_shared_from_this<ReplicaConnectionPool> {
public:
    using Ptr = std::shared_ptr<ReplicaConnectionPool>;
    using BorrowCallback = std::function<void(http::PersistentHttpSession::Ptr, std::string)>;

    static Ptr create(network::EventLoop* loop, ReplicaConnectionPoolConfig config = {});
    ~ReplicaConnectionPool();

    void borrow(const ReplicaPoolKey& key, BorrowCallback callback);
    void release(const http::PersistentHttpSession::Ptr& session);
    void discard(const http::PersistentHttpSession::Ptr& session);
    void shutdown();
    ReplicaConnectionPoolMetrics metrics() const;
    network::EventLoop* ownerLoop() const { return loop_; }

private:
    struct Entry { ReplicaPoolKey key; http::PersistentHttpSession::Ptr session; };
    explicit ReplicaConnectionPool(network::EventLoop* loop, ReplicaConnectionPoolConfig config);
    void borrowInLoop(ReplicaPoolKey key, BorrowCallback callback);
    void releaseInLoop(const http::PersistentHttpSession::Ptr& session);
    void discardInLoop(const http::PersistentHttpSession::Ptr& session);
    void pruneClosedInLoop();
    size_t targetCountInLoop(const ReplicaPoolKey& key) const;
    Entry* findBorrowedInLoop(const http::PersistentHttpSession::Ptr& session);
    void refreshMetricsInLoop() const;

    network::EventLoop* loop_;
    ReplicaConnectionPoolConfig config_;
    std::deque<Entry> idle_;
    std::deque<Entry> borrowed_;
    mutable ReplicaConnectionPoolMetrics metrics_;
    bool stopping_ = false;
};

}  // namespace miniKV::datanode
