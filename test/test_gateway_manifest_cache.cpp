#include "gateway/GatewayState.hpp"

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace {
using miniKV::gateway::ChunkRouteRequest;
using miniKV::gateway::CommitChunkStatus;
using miniKV::gateway::FileCommitStatus;
using miniKV::gateway::FileMeta;
using miniKV::gateway::GatewayState;
using miniKV::gateway::ManifestSnapshot;
using miniKV::gateway::NodeRecord;
using miniKV::gateway::NodeRuntime;
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

NodeRecord storageNode(const std::string& nodeId, const std::string& address)
{
    NodeRecord node;
    node.nodeId = nodeId;
    node.address = address;
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
    if (!state.createSession("manifest-cache.NEF", "/cache", kChunkSize, kChunkSize, session)) return false;
    const ChunkRouteRequest request{0, "manifest-cache-chunk", kChunkSize};
    std::vector<PlacementPlan> plans;
    if (state.planRoutes(session.sessionId, {request}, plans) != RoutePlanStatus::kOk || plans.size() != 1) return false;
    if (state.commitChunk(session.sessionId, 0, request.chunkHash, request.chunkSize,
                          nodeIds(plans.front()), plans.front().leaseId) != CommitChunkStatus::kCommitted) return false;
    return state.commitFile(session.sessionId, file) == FileCommitStatus::kCommitted;
}
}  // namespace

int main()
{
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_gateway_manifest_cache_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    GatewayState state(directory.string());
    NodeRuntime runtime;
    runtime.freeBytes = 900ULL * 1024ULL * 1024ULL;
    CHECK(state.open());
    CHECK(state.registerNode(storageNode("node-a", "10.0.0.1")));
    CHECK(state.registerNode(storageNode("node-b", "10.0.0.2")));
    CHECK(state.heartbeat("node-a", runtime));
    CHECK(state.heartbeat("node-b", runtime));
    FileMeta file;
    CHECK(createObject(state, file));

    ManifestSnapshot manifest;
    const auto before = state.manifestCacheStats();
    CHECK(state.buildManifestSnapshot(file.fileHash, manifest));
    const auto afterMiss = state.manifestCacheStats();
    CHECK(afterMiss.misses == before.misses + 1);
    CHECK(state.buildManifestSnapshot(file.fileHash, manifest));
    const auto afterHit = state.manifestCacheStats();
    CHECK(afterHit.hits == afterMiss.hits + 1);

    CHECK(state.registerNode(storageNode("node-a", "10.0.0.9")));
    CHECK(state.buildManifestSnapshot(file.fileHash, manifest));
    CHECK(manifest.nodes.at("node-a").address == "10.0.0.9");
    const auto afterNodeChange = state.manifestCacheStats();
    CHECK(afterNodeChange.misses == afterHit.misses + 1);

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: Gateway manifest cache invalidates static node changes\n";
    return 0;
}
