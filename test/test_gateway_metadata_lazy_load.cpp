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
    if (!state.createSession("lazy.NEF", "/lazy", kChunkSize, kChunkSize, session)) return false;
    const ChunkRouteRequest request{0, "lazy-chunk", kChunkSize};
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
        std::filesystem::temp_directory_path() / "minikv_gateway_metadata_lazy_load_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);
    FileMeta file;
    {
        GatewayState writer(directory.string());
        NodeRuntime runtime;
        runtime.freeBytes = 900ULL * 1024ULL * 1024ULL;
        CHECK(writer.open());
        CHECK(writer.registerNode(storageNode("node-a")));
        CHECK(writer.registerNode(storageNode("node-b")));
        CHECK(writer.heartbeat("node-a", runtime));
        CHECK(writer.heartbeat("node-b", runtime));
        CHECK(createObject(writer, file));
    }

    GatewayState reader(directory.string());
    CHECK(reader.open());
    const auto cold = reader.metadataCacheUsage();
    CHECK(cold.objectEntries == 0);
    CHECK(cold.catalogEntries == 0);
    CHECK(cold.manifestEntries == 0);
    CHECK(cold.fileEntries == 0);
    CHECK(cold.routeEntries == 0);
    ObjectMeta object;
    FileMeta loaded;
    CHECK(reader.getObject(file.objectId, object));
    CHECK(reader.getFile(file.fileHash, loaded));
    const auto warm = reader.metadataCacheUsage();
    CHECK(warm.objectEntries == 1);
    CHECK(warm.fileEntries == 1);
    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: Gateway starts without eagerly loading business metadata\n";
    return 0;
}
