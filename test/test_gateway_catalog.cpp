#include "gateway/GatewayState.hpp"

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace {

using miniKV::gateway::CatalogSnapshot;
using miniKV::gateway::ChunkRouteRequest;
using miniKV::gateway::CommitChunkStatus;
using miniKV::gateway::FileCommitStatus;
using miniKV::gateway::FileMeta;
using miniKV::gateway::GatewayState;
using miniKV::gateway::NodeRecord;
using miniKV::gateway::NodeRuntime;
using miniKV::gateway::PlacementPlan;
using miniKV::gateway::RoutePlanStatus;
using miniKV::gateway::SessionState;

constexpr uint32_t kChunkSize = 4 * 1024 * 1024;

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

bool makeCompletedSession(GatewayState& state, const std::string& name,
                          const std::string& directory, SessionState& session)
{
    if (!state.createSession(name, directory, kChunkSize, kChunkSize, session)) return false;
    std::vector<PlacementPlan> plans;
    const std::vector<ChunkRouteRequest> requests = {{0, "chunk-" + name, kChunkSize}};
    if (state.planRoutes(session.sessionId, requests, plans) != RoutePlanStatus::kOk ||
        plans.size() != 1) return false;
    return state.commitChunk(session.sessionId, 0, requests[0].chunkHash, kChunkSize,
                             nodeIds(plans[0]), plans[0].leaseId) ==
           CommitChunkStatus::kCommitted;
}

}  // namespace

int main()
{
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_gateway_catalog_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    std::string dbPath = directory.string();
    std::string firstObjectId;
    {
        GatewayState state(dbPath);
        CHECK(state.open());
        CHECK(state.registerNode(storageNode("node-a")));
        CHECK(state.registerNode(storageNode("node-b")));
        NodeRuntime runtime;
        runtime.freeBytes = 900ULL * 1024ULL * 1024ULL;
        CHECK(state.heartbeat("node-a", runtime));
        CHECK(state.heartbeat("node-b", runtime));

    CHECK(state.createDirectory("/", "2026"));
    CHECK(state.createDirectory("/2026", "Xian"));
    CHECK(!state.createDirectory("/2026", "Xian"));
    CHECK(!state.createDirectory("/missing", "child"));
    CHECK(!state.createDirectory("/2026", ".."));

    CatalogSnapshot root;
    CHECK(state.listCatalog("/", root));
    CHECK(root.directories.size() == 1);
    CHECK(root.directories[0].path == "/2026");

    CatalogSnapshot year;
    CHECK(state.listCatalog("/2026", year));
    CHECK(year.directories.size() == 1);
    CHECK(year.directories[0].path == "/2026/Xian");
    CHECK(year.files.empty());

    SessionState first;
    CHECK(makeCompletedSession(state, "DSC_0001.NEF", "/2026/Xian", first));
    FileMeta firstFile;
    CHECK(state.commitFile(first.sessionId, firstFile) == FileCommitStatus::kCommitted);
        CHECK(!firstFile.objectId.empty());
        firstObjectId = firstFile.objectId;

    CatalogSnapshot xian;
    CHECK(state.listCatalog("/2026/Xian", xian));
    CHECK(xian.directories.empty());
    CHECK(xian.files.size() == 1);
        CHECK(xian.files[0].objectId == firstFile.objectId);
    CHECK(xian.files[0].name == "DSC_0001.NEF");
    CHECK(xian.files[0].fileHash == firstFile.fileHash);

    miniKV::gateway::ObjectMeta object;
    CHECK(state.getObject(firstFile.objectId, object));
        CHECK(object.fileHash == firstFile.fileHash);
    CHECK(object.parentPath == "/2026/Xian");

    SessionState duplicate;
    CHECK(makeCompletedSession(state, "DSC_0001.NEF", "/2026/Xian", duplicate));
    FileMeta duplicateFile;
        CHECK(state.commitFile(duplicate.sessionId, duplicateFile) == FileCommitStatus::kPathConflict);
    }

    GatewayState reopened(dbPath);
    CHECK(reopened.open());
    CatalogSnapshot persisted;
    CHECK(reopened.listCatalog("/2026/Xian", persisted));
    CHECK(persisted.files.size() == 1);
    CHECK(persisted.files[0].objectId == firstObjectId);

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: Gateway catalog persists directories and objects\n";
    return 0;
}
