#include "gateway/GatewayState.hpp"

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace {

using miniKV::gateway::CatalogSnapshot;
using miniKV::gateway::ChunkRouteRequest;
using miniKV::gateway::CommitChunkStatus;
using miniKV::gateway::DeleteStatus;
using miniKV::gateway::FileCommitStatus;
using miniKV::gateway::FileMeta;
using miniKV::gateway::GatewayState;
using miniKV::gateway::ManifestSnapshot;
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

bool createObject(GatewayState& state, const std::string& name,
                  const std::string& directory, FileMeta& file,
                  bool opaque = false, std::string* storageIdentity = nullptr)
{
    SessionState session;
    if (!state.createSession(name, directory, kChunkSize, kChunkSize, session)) return false;
    ChunkRouteRequest request;
    request.chunkIndex = 0;
    request.chunkHash = opaque ? std::string(64, 'a') : "chunk:" + name;
    request.chunkSize = kChunkSize;
    if (opaque) {
        request.identityScheme = "opaque-chunk-id";
        request.checksumType = "crc32c";
        request.checksumDigest = "e3069283";
    }
    std::vector<PlacementPlan> plans;
    if (state.planRoutes(session.sessionId, {request}, plans) != RoutePlanStatus::kOk ||
        plans.size() != 1) {
        return false;
    }
    if (storageIdentity != nullptr) {
        *storageIdentity = plans.front().identityScheme == "opaque-chunk-id" ?
            plans.front().chunkId : request.chunkHash;
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
        std::filesystem::temp_directory_path() / "minikv_gateway_delete_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    std::string pendingHash;
    std::string persistedOpaqueStorageIdentity;
    {
        GatewayState state(directory.string());
        NodeRuntime runtime;
        runtime.freeBytes = 900ULL * 1024ULL * 1024ULL;
        CHECK(state.open());
        CHECK(state.registerNode(storageNode("node-a")));
        CHECK(state.registerNode(storageNode("node-b")));
        CHECK(state.heartbeat("node-a", runtime));
        CHECK(state.heartbeat("node-b", runtime));

        FileMeta sharedFirst;
        FileMeta sharedSecond;
        CHECK(createObject(state, "shared.NEF", "/keep/a", sharedFirst));
        CHECK(createObject(state, "shared.NEF", "/keep/b", sharedSecond));
        CHECK(sharedFirst.fileHash == sharedSecond.fileHash);

        CHECK(state.deleteObject(sharedFirst.objectId) == DeleteStatus::kDeleted);
        ObjectMeta object;
        ManifestSnapshot manifest;
        CHECK(!state.getObject(sharedFirst.objectId, object));
        CHECK(state.getObject(sharedSecond.objectId, object));
        CHECK(state.buildManifestSnapshot(sharedSecond.fileHash, manifest));
        CHECK(state.pendingDeletesForNode("node-a").empty());

        FileMeta unique;
        CHECK(createObject(state, "unique.NEF", "/delete-me", unique));
        CHECK(state.deleteObject(unique.objectId, unique.objectVersion + 1) == DeleteStatus::kNotFound);
        ObjectMeta versionProtected;
        CHECK(state.getObject(unique.objectId, versionProtected));
        CHECK(state.deleteObject(unique.objectId) == DeleteStatus::kDeleted);
        CHECK(!state.getObject(unique.objectId, object));
        CHECK(!state.buildManifestSnapshot(unique.fileHash, manifest));

        const auto pendingA = state.pendingDeletesForNode("node-a");
        const auto pendingB = state.pendingDeletesForNode("node-b");
        CHECK(pendingA.size() == 1);
        CHECK(pendingB.size() == 1);
        CHECK(pendingA.front().chunkHash == pendingB.front().chunkHash);
        pendingHash = pendingA.front().chunkHash;
        CHECK(state.acknowledgeDelete(pendingHash, "node-a"));
        CHECK(state.pendingDeletesForNode("node-a").empty());
        CHECK(state.pendingDeletesForNode("node-b").size() == 1);

    }

    {
        GatewayState reopened(directory.string());
        NodeRuntime runtime;
        runtime.freeBytes = 900ULL * 1024ULL * 1024ULL;
        CHECK(reopened.open());
        CHECK(reopened.pendingDeletesForNode("node-b").size() == 1);
        CHECK(reopened.acknowledgeDelete(pendingHash, "node-b"));
        CHECK(reopened.pendingDeletesForNode("node-b").empty());
        CHECK(reopened.heartbeat("node-a", runtime));
        CHECK(reopened.heartbeat("node-b", runtime));

        FileMeta nestedOne;
        FileMeta nestedTwo;
        CHECK(createObject(reopened, "one.NEF", "/trip/day1", nestedOne));
        CHECK(createObject(reopened, "two.NEF", "/trip/day2", nestedTwo));
        CHECK(reopened.deleteDirectory("/trip") == DeleteStatus::kDeleted);
        CatalogSnapshot root;
        ObjectMeta object;
        CHECK(reopened.listCatalog("/", root));
        for (const auto& child : root.directories) CHECK(child.path != "/trip");
        CHECK(!reopened.getObject(nestedOne.objectId, object));
        CHECK(!reopened.getObject(nestedTwo.objectId, object));
        CHECK(reopened.deleteDirectory("/") == DeleteStatus::kInvalidRequest);

        FileMeta opaque;
        CHECK(createObject(reopened, "opaque.NEF", "/opaque", opaque, true,
                           &persistedOpaqueStorageIdentity));
        CHECK(persistedOpaqueStorageIdentity.rfind("chk-", 0) == 0);
        CHECK(reopened.deleteObject(opaque.objectId) == DeleteStatus::kDeleted);
        bool foundOpaqueDelete = false;
        for (const auto& task : reopened.pendingDeletesForNode("node-a")) {
            if (task.storageIdentity == persistedOpaqueStorageIdentity) {
                CHECK(task.storageIdentity != task.chunkHash);
                foundOpaqueDelete = true;
            }
        }
        CHECK(foundOpaqueDelete);
    }

    {
        GatewayState reopened(directory.string());
        CHECK(reopened.open());
        bool foundOpaqueDelete = false;
        for (const auto& task : reopened.pendingDeletesForNode("node-a")) {
            if (task.storageIdentity == persistedOpaqueStorageIdentity) {
                CHECK(task.storageIdentity != task.chunkHash);
                foundOpaqueDelete = true;
            }
        }
        CHECK(foundOpaqueDelete);
    }

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: Gateway deletes catalog entries and persists pending physical deletes\n";
    return 0;
}
