#include "gateway/GatewayState.hpp"

#include <filesystem>
#include <iostream>

namespace {

using miniKV::gateway::ChunkRouteRequest;
using miniKV::gateway::CommitChunkStatus;
using miniKV::gateway::FileCommitStatus;
using miniKV::gateway::GatewayState;
using miniKV::gateway::ManifestSnapshot;
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

NodeRecord storageNode(const std::string& id) {
    NodeRecord node;
    node.nodeId = id;
    node.address = id == "node-a" ? "10.0.0.1" : "10.0.0.2";
    node.httpPort = 9002;
    node.maxStorageBytes = 1024ULL * 1024ULL * 1024ULL;
    node.maxConcurrentWrites = 2;
    node.capabilities = {"storage"};
    return node;
}

std::vector<std::string> ids(const PlacementPlan& plan) {
    std::vector<std::string> result;
    for (const auto& node : plan.chain) result.push_back(node.record.nodeId);
    return result;
}

}  // namespace

int main() {
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_gateway_manifest_snapshot_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    std::string dbPath = directory.string();
    GatewayState state(dbPath);
    CHECK(state.open());
    CHECK(state.registerNode(storageNode("node-a")));
    CHECK(state.registerNode(storageNode("node-b")));
    NodeRuntime runtime;
    runtime.freeBytes = 900ULL * 1024ULL * 1024ULL;
    CHECK(state.heartbeat("node-a", runtime));
    CHECK(state.heartbeat("node-b", runtime));

    SessionState session;
    CHECK(state.createSession("two-chunks.raw", "/shoots", 2ULL * kChunkSize,
                              kChunkSize, session));
    std::vector<PlacementPlan> plans;
    const std::vector<ChunkRouteRequest> requests = {
        {0, "chunk-zero", kChunkSize},
        {1, "chunk-one", kChunkSize},
    };
    CHECK(state.planRoutes(session.sessionId, requests, plans) == RoutePlanStatus::kOk);
    CHECK(plans.size() == 2);
    for (size_t index = 0; index < plans.size(); ++index) {
        CHECK(state.commitChunk(session.sessionId, static_cast<uint32_t>(index),
                                requests[index].chunkHash, kChunkSize, ids(plans[index]),
                                plans[index].leaseId) == CommitChunkStatus::kCommitted);
    }
    miniKV::gateway::FileMeta file;
    CHECK(state.commitFile(session.sessionId, file) == FileCommitStatus::kCommitted);

    ManifestSnapshot snapshot;
    CHECK(state.buildManifestSnapshot(file.fileHash, snapshot));
    CHECK(snapshot.file.fileHash == file.fileHash);
    CHECK(snapshot.routes.size() == 2);
    CHECK(snapshot.routes[0].chunkHash == "chunk-zero");
    CHECK(snapshot.routes[1].chunkHash == "chunk-one");
    CHECK(snapshot.nodes.size() == 2);
    CHECK(snapshot.nodes.at("node-a").address == "10.0.0.1");
    CHECK(snapshot.nodes.at("node-b").address == "10.0.0.2");

    miniKV::control::ObjectReadDescriptor readDescriptor;
    CHECK(state.buildObjectReadDescriptor(file.objectId, readDescriptor));
    CHECK(readDescriptor.objectId == file.objectId);
    CHECK(readDescriptor.objectVersion == file.objectVersion);
    CHECK(readDescriptor.metadataVersion == file.metadataVersion);
    CHECK(readDescriptor.chunks.size() == 2);
    CHECK(readDescriptor.chunks[0].storageIdentity == "chunk-zero");
    CHECK(readDescriptor.chunks[0].replicas.size() == 2);

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: Gateway builds a complete manifest snapshot under one lock\n";
    return 0;
}
