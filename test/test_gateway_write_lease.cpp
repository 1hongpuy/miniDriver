#include "gateway/GatewayState.hpp"

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace {

using miniKV::gateway::ChunkRouteRequest;
using miniKV::gateway::CommitChunkStatus;
using miniKV::gateway::GatewayState;
using miniKV::gateway::NodeRecord;
using miniKV::gateway::NodeRuntime;
using miniKV::gateway::PlacementPlan;
using miniKV::gateway::RoutePlanStatus;
using miniKV::gateway::SessionState;

constexpr uint32_t kChunkSize = 4 * 1024 * 1024;

bool check(bool condition, const char* expression, int line) {
    if (condition) return true;
    std::cerr << "FAIL:" << line << ": " << expression << '\n';
    return false;
}

#define CHECK(expression) \
    do { if (!check((expression), #expression, __LINE__)) return 1; } while (false)

NodeRecord storageNode(const std::string& nodeId) {
    NodeRecord node;
    node.nodeId = nodeId;
    node.address = "127.0.0.1";
    node.maxStorageBytes = 1024ULL * 1024ULL * 1024ULL;
    node.maxConcurrentWrites = 2;
    node.capabilities = {"storage"};
    return node;
}

std::vector<std::string> nodeIds(const PlacementPlan& plan) {
    std::vector<std::string> result;
    for (const auto& node : plan.chain) result.push_back(node.record.nodeId);
    return result;
}

RoutePlanStatus planOne(GatewayState& state, const SessionState& session,
                        const std::string& hash, PlacementPlan& plan) {
    std::vector<PlacementPlan> plans;
    const RoutePlanStatus status =
        state.planRoutes(session.sessionId, {{0, hash, kChunkSize}}, plans);
    if (status == RoutePlanStatus::kOk && plans.size() == 1) plan = plans.front();
    return status;
}

}  // namespace

int main() {
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_gateway_write_lease_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    std::string dbPath = directory.string();
    GatewayState state(dbPath);
    CHECK(state.open());
    CHECK(state.registerNode(storageNode("node-a")));
    CHECK(state.registerNode(storageNode("node-b")));

    // A heartbeat can be stale. It is telemetry, not Gateway's authoritative
    // write-slot counter.
    NodeRuntime staleRuntime;
    staleRuntime.freeBytes = 900ULL * 1024ULL * 1024ULL;
    staleRuntime.activeUploads = 2;
    CHECK(state.heartbeat("node-a", staleRuntime));
    CHECK(state.heartbeat("node-b", staleRuntime));

    SessionState first;
    SessionState second;
    SessionState third;
    CHECK(state.createSession("first.raw", "/benchmark", kChunkSize, kChunkSize, first));
    CHECK(state.createSession("second.raw", "/benchmark", kChunkSize, kChunkSize, second));
    CHECK(state.createSession("third.raw", "/benchmark", kChunkSize, kChunkSize, third));

    PlacementPlan firstPlan;
    PlacementPlan secondPlan;
    PlacementPlan duplicatePlan;
    PlacementPlan thirdPlan;
    CHECK(planOne(state, first, "hash-first", firstPlan) == RoutePlanStatus::kOk);
    CHECK(firstPlan.chain.size() == 2);
    CHECK(planOne(state, second, "hash-second", secondPlan) == RoutePlanStatus::kOk);
    CHECK(secondPlan.chain.size() == 2);

    // Retrying the same route request must not consume another write slot.
    CHECK(planOne(state, second, "hash-second", duplicatePlan) == RoutePlanStatus::kOk);
    CHECK(duplicatePlan.leaseId == secondPlan.leaseId);
    CHECK(nodeIds(duplicatePlan) == nodeIds(secondPlan));

    // Both nodes have their two slots reserved by firstPlan and secondPlan.
    CHECK(planOne(state, third, "hash-third", thirdPlan) == RoutePlanStatus::kNoCapacity);

    // Completing firstPlan releases its reservation, allowing thirdPlan.
    CHECK(state.commitChunk(first.sessionId, 0, "hash-first", kChunkSize,
                            nodeIds(firstPlan), firstPlan.leaseId) ==
          CommitChunkStatus::kCommitted);
    CHECK(state.commitChunk(first.sessionId, 0, "hash-first", kChunkSize,
                            nodeIds(firstPlan), firstPlan.leaseId) ==
          CommitChunkStatus::kAlreadyCommitted);
    CHECK(planOne(state, third, "hash-third", thirdPlan) == RoutePlanStatus::kOk);
    CHECK(thirdPlan.chain.size() == 2);

    // An explicit client/DataNode failure must free the route immediately;
    // waiting for the 120-second lease timeout would make the node look full.
    CHECK(state.releaseLease(thirdPlan.leaseId));
    SessionState fourth;
    CHECK(state.createSession("fourth.raw", "/benchmark", kChunkSize, kChunkSize, fourth));
    PlacementPlan fourthPlan;
    CHECK(planOne(state, fourth, "hash-fourth", fourthPlan) == RoutePlanStatus::kOk);

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: Gateway reserves, reuses, and releases write leases\n";
    return 0;
}
