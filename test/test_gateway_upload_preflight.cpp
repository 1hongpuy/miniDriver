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
using miniKV::gateway::PlacementPlan;
using miniKV::gateway::PreflightStatus;
using miniKV::gateway::RoutePlanStatus;
using miniKV::gateway::UploadPreflightRequest;
using miniKV::gateway::UploadPreflightResult;

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

bool uploadMissingChunks(GatewayState& state, const UploadPreflightResult& preflight)
{
    std::vector<PlacementPlan> plans;
    if (state.planRoutes(preflight.session.sessionId, preflight.missingChunks, plans) != RoutePlanStatus::kOk ||
        plans.size() != preflight.missingChunks.size()) {
        return false;
    }
    for (size_t i = 0; i < plans.size(); ++i) {
        const auto& chunk = preflight.missingChunks[i];
        if (state.commitChunk(preflight.session.sessionId, chunk.chunkIndex, chunk.chunkHash,
                              chunk.chunkSize, nodeIds(plans[i]), plans[i].leaseId) !=
            CommitChunkStatus::kCommitted) {
            return false;
        }
    }
    return true;
}

}  // namespace

int main()
{
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_gateway_upload_preflight_test";
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

    UploadPreflightRequest first;
    first.fileName = "source.NEF";
    first.dirPath = "/shoots";
    first.fileSize = 2 * kChunkSize;
    first.chunkSize = kChunkSize;
    first.chunks = {{0, "preflight-chunk-a", kChunkSize}, {1, "preflight-chunk-b", kChunkSize}};
    first.manifestHash = GatewayState::manifestHash(first.fileSize, first.chunkSize, first.chunks);

    UploadPreflightResult firstResult;
    CHECK(state.preflightUpload(first, firstResult) == PreflightStatus::kUploadRequired);
    CHECK(firstResult.missingChunks.size() == 2);
    CHECK(firstResult.presentChunks.empty());
    CHECK(uploadMissingChunks(state, firstResult));
    FileMeta firstFile;
    CHECK(state.commitFile(firstResult.session.sessionId, firstFile) == FileCommitStatus::kCommitted);
    CHECK(firstFile.fileHash == first.manifestHash);

    UploadPreflightRequest duplicate = first;
    duplicate.fileName = "copy.NEF";
    duplicate.dirPath = "/archive";
    UploadPreflightResult duplicateResult;
    CHECK(state.preflightUpload(duplicate, duplicateResult) == PreflightStatus::kContentExists);
    CHECK(!duplicateResult.object.objectId.empty());
    CHECK(duplicateResult.object.fileHash == first.manifestHash);
    CHECK(duplicateResult.missingChunks.empty());

    UploadPreflightRequest partial = first;
    partial.fileName = "partial.NEF";
    partial.dirPath = "/shoots";
    partial.chunks[1].chunkHash = "preflight-chunk-new";
    partial.manifestHash = GatewayState::manifestHash(
        partial.fileSize, partial.chunkSize, partial.chunks);
    UploadPreflightResult partialResult;
    CHECK(state.preflightUpload(partial, partialResult) == PreflightStatus::kUploadRequired);
    CHECK(partialResult.presentChunks.size() == 1);
    CHECK(partialResult.presentChunks[0].chunkIndex == 0);
    CHECK(partialResult.missingChunks.size() == 1);
    CHECK(partialResult.missingChunks[0].chunkIndex == 1);
    CHECK(partialResult.session.completed.count(0) == 1);

    UploadPreflightResult conflictResult;
    CHECK(state.preflightUpload(first, conflictResult) == PreflightStatus::kPathConflict);
    CHECK(conflictResult.session.sessionId.empty());

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: upload preflight rejects path conflicts and reuses existing content\n";
    return 0;
}
