#include "DataNode/ReplicaConnectionPool.hpp"

#include "network/EventLoop.hpp"
#include "utils/AsyncLogger.hpp"

#include <algorithm>
#include <cassert>
#include <utility>

namespace miniKV::datanode {

size_t ReplicaPoolKeyHash::operator()(const ReplicaPoolKey& key) const
{
    const auto h1 = std::hash<std::string>{}(key.nodeId);
    const auto h2 = std::hash<std::string>{}(key.address);
    const auto h3 = std::hash<uint16_t>{}(key.port);
    return h1 ^ (h2 << 1U) ^ (h3 << 2U);
}

ReplicaConnectionPool::Ptr ReplicaConnectionPool::create(network::EventLoop* loop,
                                                          ReplicaConnectionPoolConfig config)
{
    return Ptr(new ReplicaConnectionPool(loop, config));
}

ReplicaConnectionPool::ReplicaConnectionPool(network::EventLoop* loop, ReplicaConnectionPoolConfig config)
    : loop_(loop), config_(config)
{
    assert(loop_ != nullptr);
    if(config_.maxSessionsPerTarget == 0) config_.maxSessionsPerTarget = 1;
    if(config_.maxSessions == 0) config_.maxSessions = 1;
}

ReplicaConnectionPool::~ReplicaConnectionPool()
{
    assert(stopping_ || (idle_.empty() && borrowed_.empty()));
}

void ReplicaConnectionPool::borrow(const ReplicaPoolKey& key, BorrowCallback callback)
{
    auto self = shared_from_this();
    loop_->runInLoop([self, key, callback = std::move(callback)]() mutable {
        self->borrowInLoop(key, std::move(callback));
    });
}

void ReplicaConnectionPool::borrowInLoop(ReplicaPoolKey key, BorrowCallback callback)
{
    assert(loop_->isInLoopThread());
    pruneClosedInLoop();
    if(stopping_ || key.nodeId.empty() || key.address.empty() || key.port == 0) {
        ++metrics_.rejected;
        if(callback) callback(nullptr, "replica connection pool is unavailable");
        return;
    }
    auto idle = std::find_if(idle_.begin(), idle_.end(), [&](const Entry& entry) { return entry.key == key; });
    if(idle != idle_.end()) {
        auto entry = std::move(*idle);
        idle_.erase(idle);
        borrowed_.push_back(std::move(entry));
        ++metrics_.reused;
        refreshMetricsInLoop();
        miniKV::utils::logInfo("event=replica_connection_reused node=" + key.nodeId +
                                " address=" + key.address + " port=" + std::to_string(key.port));
        if(callback) callback(borrowed_.back().session, "");
        return;
    }
    if(borrowed_.size() + idle_.size() >= config_.maxSessions ||
       targetCountInLoop(key) >= config_.maxSessionsPerTarget) {
        ++metrics_.rejected;
        refreshMetricsInLoop();
        miniKV::utils::logInfo("event=replica_pool_borrow_rejected node=" + key.nodeId);
        if(callback) callback(nullptr, "replica connection pool capacity reached");
        return;
    }
    borrowed_.push_back({std::move(key), http::PersistentHttpSession::create(loop_)});
    ++metrics_.created;
    refreshMetricsInLoop();
    miniKV::utils::logInfo("event=replica_connection_created node=" + borrowed_.back().key.nodeId +
                            " address=" + borrowed_.back().key.address +
                            " port=" + std::to_string(borrowed_.back().key.port));
    if(callback) callback(borrowed_.back().session, "");
}

void ReplicaConnectionPool::release(const http::PersistentHttpSession::Ptr& session)
{
    auto self = shared_from_this();
    loop_->runInLoop([self, session] { self->releaseInLoop(session); });
}

void ReplicaConnectionPool::releaseInLoop(const http::PersistentHttpSession::Ptr& session)
{
    assert(loop_->isInLoopThread());
    auto* entry = findBorrowedInLoop(session);
    if(!entry) return;
    auto it = std::find_if(borrowed_.begin(), borrowed_.end(), [&](const Entry& item) { return item.session == session; });
    Entry moved = std::move(*it);
    borrowed_.erase(it);
    if(!stopping_ && session->idle() && session->usable()) idle_.push_back(std::move(moved));
    else { ++metrics_.discarded; session->close(); }
    refreshMetricsInLoop();
}

void ReplicaConnectionPool::discard(const http::PersistentHttpSession::Ptr& session)
{
    auto self = shared_from_this();
    loop_->runInLoop([self, session] { self->discardInLoop(session); });
}

void ReplicaConnectionPool::discardInLoop(const http::PersistentHttpSession::Ptr& session)
{
    assert(loop_->isInLoopThread());
    auto erase = [&](std::deque<Entry>& entries) {
        const auto before = entries.size();
        entries.erase(std::remove_if(entries.begin(), entries.end(), [&](const Entry& entry) {
            if(entry.session != session) return false;
            entry.session->close();
            return true;
        }), entries.end());
        return entries.size() != before;
    };
    if(erase(borrowed_) || erase(idle_)) {
        ++metrics_.discarded;
        miniKV::utils::logInfo("event=replica_connection_discarded");
    }
    refreshMetricsInLoop();
}

void ReplicaConnectionPool::shutdown()
{
    auto self = shared_from_this();
    loop_->runInLoop([self] {
        self->stopping_ = true;
        for(auto& entry : self->idle_) entry.session->close();
        for(auto& entry : self->borrowed_) entry.session->close();
        self->idle_.clear();
        self->borrowed_.clear();
        self->refreshMetricsInLoop();
    });
}

ReplicaConnectionPoolMetrics ReplicaConnectionPool::metrics() const { return metrics_; }

void ReplicaConnectionPool::pruneClosedInLoop()
{
    idle_.erase(std::remove_if(idle_.begin(), idle_.end(), [&](const Entry& entry) {
        if(entry.session->usable()) return false;
        ++metrics_.discarded;
        return true;
    }), idle_.end());
    refreshMetricsInLoop();
}

size_t ReplicaConnectionPool::targetCountInLoop(const ReplicaPoolKey& key) const
{
    const auto count = [&](const std::deque<Entry>& entries) {
        return static_cast<size_t>(std::count_if(entries.begin(), entries.end(), [&](const Entry& entry) {
            return entry.key == key;
        }));
    };
    return count(idle_) + count(borrowed_);
}

ReplicaConnectionPool::Entry* ReplicaConnectionPool::findBorrowedInLoop(
    const http::PersistentHttpSession::Ptr& session)
{
    const auto it = std::find_if(borrowed_.begin(), borrowed_.end(), [&](const Entry& entry) {
        return entry.session == session;
    });
    return it == borrowed_.end() ? nullptr : &*it;
}

void ReplicaConnectionPool::refreshMetricsInLoop() const
{
    metrics_.idle = idle_.size();
    metrics_.borrowed = borrowed_.size();
    metrics_.peakBorrowed = std::max(metrics_.peakBorrowed, metrics_.borrowed);
}

}  // namespace miniKV::datanode
