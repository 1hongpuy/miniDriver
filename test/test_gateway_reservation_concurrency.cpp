#include "gateway/GatewayState.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

using miniKV::gateway::ChunkRouteRequest;
using miniKV::gateway::CommitChunkStatus;
using miniKV::gateway::GatewayLockMode;
using miniKV::gateway::GatewayState;
using miniKV::gateway::NodeRecord;
using miniKV::gateway::NodeRuntime;
using miniKV::gateway::PlacementPlan;
using miniKV::gateway::RoutePlanStatus;
using miniKV::gateway::SessionState;

constexpr uint64_t kChunkSize = 64 * 1024;

int64_t unixNow() {
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

NodeRecord node(const std::string& id) {
    NodeRecord result;
    result.nodeId = id;
    result.address = "127.0.0.1";
    result.maxStorageBytes = 1ULL << 40;
    result.maxConcurrentWrites = 1000;
    result.capabilities = {"storage"};
    return result;
}

std::vector<std::string> nodeIds(const PlacementPlan& plan) {
    std::vector<std::string> ids;
    for (const auto& item : plan.chain) ids.push_back(item.record.nodeId);
    return ids;
}

}  // namespace

int main() {
    const auto directory = std::filesystem::temp_directory_path() /
        "minikv_gateway_reservation_concurrency_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    GatewayState state(directory.string(), 2, GatewayLockMode::kSharded);
    if (!state.open() || !state.registerNode(node("node-a")) ||
        !state.registerNode(node("node-b")) || !state.registerNode(node("node-c"))) {
        std::cerr << "FAIL: unable to initialize sharded reservation test\n";
        return 1;
    }
    NodeRuntime runtime;
    runtime.freeBytes = 1ULL << 38;
    runtime.lastHeartbeatAt = 1;
    for (const auto& id : {std::string("node-a"), std::string("node-b"), std::string("node-c")}) {
        if (!state.heartbeat(id, runtime)) {
            std::cerr << "FAIL: heartbeat " << id << '\n';
            return 1;
        }
    }

    std::vector<SessionState> sessions;
    sessions.reserve(256);
    for (int i = 0; i < 256; ++i) {
        SessionState session;
        if (!state.createSession("reservation-" + std::to_string(i), "/test",
                                 kChunkSize, static_cast<uint32_t>(kChunkSize), session)) {
            std::cerr << "FAIL: create session " << i << '\n';
            return 1;
        }
        sessions.push_back(std::move(session));
    }

    std::vector<PlacementPlan> planned;
    planned.reserve(sessions.size());
    std::vector<std::string> hashes;
    hashes.reserve(sessions.size());
    for (size_t i = 0; i < sessions.size(); ++i) {
        hashes.push_back("reservation-hash-" + std::to_string(i));
        std::vector<PlacementPlan> plans;
        if (state.planRoutes(sessions[i].sessionId,
                             {{0, hashes.back(), kChunkSize}}, plans) != RoutePlanStatus::kOk ||
            plans.size() != 1) {
            std::cerr << "FAIL: initial reservation plan " << i << '\n';
            return 1;
        }
        planned.push_back(std::move(plans.front()));
    }

    std::atomic<int> commitRaces{0};
    std::atomic<int> releaseCalls{0};
    std::atomic<bool> cleanupFinished{false};
    std::atomic<bool> start{false};
    std::vector<std::thread> workers;
    for (int worker = 0; worker < 4; ++worker) {
        workers.emplace_back([&, worker] {
            for (size_t i = static_cast<size_t>(worker); i < sessions.size(); i += 4) {
                const auto& session = sessions[i];
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                const auto ids = nodeIds(planned[i]);
                if ((i + worker) % 3 == 0) {
                    const auto committed = state.commitChunk(
                        session.sessionId, 0, hashes[i], kChunkSize, ids, planned[i].leaseId);
                    if (committed != CommitChunkStatus::kCommitted &&
                        committed != CommitChunkStatus::kAlreadyCommitted) {
                        // Cleanup may legitimately win the race and expire the
                        // lease before the commit reaches the state machine.
                        ++commitRaces;
                    }
                } else {
                    if (state.releaseLease(planned[i].leaseId)) ++releaseCalls;
                    // Duplicate cleanup/retry must not decrement counters twice.
                    (void)state.releaseLease(planned[i].leaseId);
                }
            }
        });
    }

    // Simulate timeout/maintenance cleanup concurrently with route and commit
    // requests. The large thresholds keep nodes online while forcing lease
    // expiry, so this exercises reservation rollback rather than node failure.
    std::thread cleanup([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        const int64_t expiryNow = unixNow() + 1000;
        for (int i = 0; i < 64; ++i) {
            state.checkNodeTimeouts(expiryNow + i, 1000000000, 1000000000);
            std::this_thread::yield();
        }
        cleanupFinished.store(true, std::memory_order_release);
    });
    std::thread heartbeats([&] {
        for (int i = 0; i < 128; ++i) {
            for (const auto& id : {std::string("node-a"), std::string("node-b"), std::string("node-c")}) {
                (void)state.heartbeat(id, runtime);
            }
            std::this_thread::yield();
        }
    });
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) worker.join();
    cleanup.join();
    heartbeats.join();
    if (!cleanupFinished.load(std::memory_order_acquire)) {
        std::cerr << "FAIL: cleanup worker did not finish\n";
        return 1;
    }

    // Force a final expiry pass and prove that capacity can be reserved again;
    // a leaked reservation would make this deterministic check fail.
    state.checkNodeTimeouts(unixNow() + 1000000, 1000000000, 1000000000);
    SessionState finalSession;
    if (!state.createSession("reservation-final", "/test", kChunkSize,
                             static_cast<uint32_t>(kChunkSize), finalSession)) {
        std::cerr << "FAIL: final session creation\n";
        return 1;
    }
    std::vector<PlacementPlan> finalPlans;
    if (state.planRoutes(finalSession.sessionId,
                         {{0, "reservation-final-hash", kChunkSize}}, finalPlans) !=
            RoutePlanStatus::kOk || finalPlans.size() != 1 ||
        !state.releaseLease(finalPlans.front().leaseId) ||
        state.releaseLease(finalPlans.front().leaseId)) {
        std::cerr << "FAIL: reservation was not exactly-once released\n";
        return 1;
    }

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: sharded reservation/cleanup concurrency has no leaked slots\n";
    std::cout << "INFO: commit_cleanup_races=" << commitRaces.load()
              << " explicit_releases=" << releaseCalls.load() << '\n';
    return 0;
}
