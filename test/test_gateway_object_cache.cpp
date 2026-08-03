#include "gateway/GatewayState.hpp"

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace {

using miniKV::gateway::ChunkRouteRequest;
using miniKV::gateway::CommitChunkStatus;
using miniKV::gateway::DeleteStatus;
using miniKV::gateway::FileCommitStatus;
using miniKV::gateway::FileMeta;
using miniKV::gateway::GatewayState;
using miniKV::gateway::NodeRecord;
using miniKV::gateway::NodeRuntime;
using miniKV::gateway::ObjectMeta;
using miniKV::gateway::PlacementPlan;
using miniKV::gateway::RoutePlanStatus;
using miniKV::gateway::SessionState;

constexpr uint32_t kChunkSize = 1024;

bool check(bool condition, const char* expression, int line)
{
    if (condition) return true;
    std::cerr << "FAIL:" << line << ": " << expression << '\n';
    return false;
}

#define CHECK(expression) \
    do { if (!check((expression), #expression, __LINE__)) return 1; } while (false)

NodeRecord storageNode(const std::string& nodeId)
{
    NodeRecord node;
    node.nodeId = nodeId;
    node.address = "127.0.0.1";
    node.maxStorageBytes = 1024ULL * 1024ULL * 1024ULL;
    node.maxConcurrentWrites = 2;
    node.capabilities = {"storage"};
    return node;
}

std::vector<std::string> nodeIds(const PlacementPlan& plan)
{
    std::vector<std::string> result;
    for (const auto& node : plan.chain) result.push_back(node.record.nodeId);
    return result;
}

bool createObject(GatewayState& state, FileMeta& file)
{
    SessionState session;
    if (!state.createSession("cached.NEF", "/cache", kChunkSize, kChunkSize, session)) return false;
    const ChunkRouteRequest request{0, "cached-chunk", kChunkSize};
    std::vector<PlacementPlan> plans;
    if (state.planRoutes(session.sessionId, {request}, plans) != RoutePlanStatus::kOk ||
        plans.size() != 1) {
        return false;
    }
    if (state.commitChunk(session.sessionId, 0, request.chunkHash, request.chunkSize,
                          nodeIds(plans.front()), plans.front().leaseId) !=
        CommitChunkStatus::kCommitted) {
        return false;
    }
    return state.commitFile(session.sessionId, file) == FileCommitStatus::kCommitted;
}

}  // namespace

int main()
{
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_gateway_object_cache_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    GatewayState state(directory.string());
    NodeRuntime runtime;
    runtime.freeBytes = 900ULL * 1024ULL * 1024ULL;
    CHECK(state.open());
    CHECK(state.registerNode(storageNode("node-a")));
    CHECK(state.registerNode(storageNode("node-b")));
    CHECK(state.heartbeat("node-a", runtime));
    CHECK(state.heartbeat("node-b", runtime));

    FileMeta file;
    CHECK(createObject(state, file));

    const auto before = state.objectCacheStats();
    ObjectMeta object;
    CHECK(state.getObject(file.objectId, object));
    CHECK(object.objectId == file.objectId);
    const auto afterMiss = state.objectCacheStats();
    CHECK(afterMiss.misses == before.misses + 1);
    CHECK(afterMiss.hits == before.hits);

    CHECK(state.getObject(file.objectId, object));
    const auto afterHit = state.objectCacheStats();
    CHECK(afterHit.hits == afterMiss.hits + 1);
    CHECK(afterHit.misses == afterMiss.misses);

    CHECK(state.deleteObject(file.objectId) == DeleteStatus::kDeleted);
    CHECK(!state.getObject(file.objectId, object));

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: Gateway object cache uses LevelDB on miss and invalidates deletes\n";
    return 0;
}
