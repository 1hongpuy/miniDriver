#include "gateway/GatewayState.hpp"

#include <cstdlib>
#include <atomic>
#include <chrono>
#include <iostream>
#include <future>
#include <mutex>
#include <string>

int main() {
    using miniKV::gateway::GatewayLockManager;
    using miniKV::gateway::GatewayLockMode;
    using miniKV::gateway::GatewayShardMultiLock;

    GatewayLockManager manager(GatewayLockMode::kSharded);
    if (manager.mode() != GatewayLockMode::kSharded ||
        manager.sessionShard("session-a") != manager.sessionShard("session-a") ||
        manager.objectShard("object-a") != manager.objectShard("object-a") ||
        GatewayLockManager::kShardCount != 64) {
        std::cerr << "FAIL: lock manager invariants\n";
        return 1;
    }
    if (std::string(GatewayLockManager::modeName(GatewayLockMode::kGlobal)) != "global" ||
        std::string(GatewayLockManager::modeName(GatewayLockMode::kSharded)) != "sharded") {
        std::cerr << "FAIL: lock mode names\n";
        return 1;
    }

    // ABBA regression test: callers present the same two domains in opposite
    // order, but GatewayShardMultiLock must always acquire by shard number.
    // Use the actual Session/Object shard mutexes, rather than stand-alone
    // test locks, so the regression covers the lock domains used by Gateway
    // business paths.
    std::mutex& first = manager.session(7);
    std::mutex& second = manager.object(41);
    std::atomic<int> inCritical{0};
    auto worker = [&](bool reverse) {
        for (int i = 0; i < 2000; ++i) {
            if (reverse) {
                GatewayShardMultiLock lock(second, 41, first, 7);
                if (inCritical.fetch_add(1) != 0) std::abort();
                inCritical.fetch_sub(1);
            } else {
                GatewayShardMultiLock lock(first, 7, second, 41);
                if (inCritical.fetch_add(1) != 0) std::abort();
                inCritical.fetch_sub(1);
            }
        }
    };
    auto left = std::async(std::launch::async, worker, false);
    auto right = std::async(std::launch::async, worker, true);
    if (left.wait_for(std::chrono::seconds(5)) != std::future_status::ready ||
        right.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
        std::cerr << "FAIL: ABBA lock ordering deadlocked\n";
        return 1;
    }
    left.get();
    right.get();
    std::cout << "PASS: Gateway lock manager uses stable 64-shard domains\n";
    std::cout << "PASS: GatewayShardMultiLock prevents ABBA deadlock\n";
    return 0;
}
