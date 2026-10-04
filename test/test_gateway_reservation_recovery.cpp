#include "gateway/GatewayState.hpp"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace {

using miniKV::gateway::ChunkRouteRequest;
using miniKV::gateway::CommitChunkStatus;
using miniKV::gateway::FileCommitStatus;
using miniKV::gateway::GatewayLockMode;
using miniKV::gateway::GatewayState;
using miniKV::gateway::NodeRecord;
using miniKV::gateway::NodeRuntime;
using miniKV::gateway::PlacementPlan;
using miniKV::gateway::RoutePlanStatus;
using miniKV::gateway::SessionState;

constexpr uint64_t kChunkSize = 64ULL * 1024ULL;

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
    node.maxStorageBytes = 1ULL << 40;
    node.maxConcurrentWrites = 64;
    node.capabilities = {"storage"};
    return node;
}

void heartbeatOnline(GatewayState& state)
{
    NodeRuntime runtime;
    runtime.freeBytes = 1ULL << 38;
    runtime.lastHeartbeatAt = 1;
    (void)state.heartbeat("node-a", runtime);
    (void)state.heartbeat("node-b", runtime);
    (void)state.heartbeat("node-c", runtime);
}

std::vector<std::string> nodeIds(const PlacementPlan& plan)
{
    std::vector<std::string> result;
    for (const auto& item : plan.chain) result.push_back(item.record.nodeId);
    return result;
}

ChunkRouteRequest request(const std::string& hash)
{
    ChunkRouteRequest result;
    result.chunkIndex = 0;
    result.chunkHash = hash;
    result.chunkSize = kChunkSize;
    result.identityScheme = "opaque-chunk-id";
    result.checksumType = "sha256";
    result.checksumDigest = hash;
    return result;
}

}  // namespace

int main()
{
    const auto directory = std::filesystem::temp_directory_path() /
        "minikv_gateway_reservation_recovery_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    std::string committedSessionId;
    std::string committedObjectId;
    std::string committedFileHash;
    {
        GatewayState state(directory.string(), 2, GatewayLockMode::kSharded);
        CHECK(state.open());
        CHECK(state.registerNode(storageNode("node-a")));
        CHECK(state.registerNode(storageNode("node-b")));
        CHECK(state.registerNode(storageNode("node-c")));
        heartbeatOnline(state);

        SessionState committedSession;
        CHECK(state.createSession("recovery-committed", "/recovery", kChunkSize,
                                  static_cast<uint32_t>(kChunkSize), committedSession));
        committedSessionId = committedSession.sessionId;
        committedObjectId = committedSession.objectId;
        const std::string committedHash(64, 'c');
        std::vector<PlacementPlan> committedPlans;
        CHECK(state.planRoutes(committedSessionId, {request(committedHash)}, committedPlans) ==
              RoutePlanStatus::kOk);
        CHECK(committedPlans.size() == 1);
        CHECK(state.commitChunk(committedSessionId, 0, committedHash, kChunkSize,
                                nodeIds(committedPlans.front()), committedPlans.front().leaseId) ==
              CommitChunkStatus::kCommitted);
        miniKV::gateway::FileMeta committedFile;
        CHECK(state.commitFile(committedSessionId, committedFile) == FileCommitStatus::kCommitted);
        committedFileHash = committedFile.fileHash;
        CHECK(!committedFileHash.empty());

        // Leave a second lease outstanding. Leases are runtime reservations,
        // not durable metadata; restart must not resurrect this token.
        SessionState abandonedSession;
        CHECK(state.createSession("recovery-abandoned", "/recovery", kChunkSize,
                                  static_cast<uint32_t>(kChunkSize), abandonedSession));
        std::vector<PlacementPlan> abandonedPlans;
        CHECK(state.planRoutes(abandonedSession.sessionId,
                               {request(std::string(64, 'a'))}, abandonedPlans) ==
              RoutePlanStatus::kOk);
        CHECK(abandonedPlans.size() == 1);
    }

    {
        GatewayState restarted(directory.string(), 2, GatewayLockMode::kSharded);
        CHECK(restarted.open());
        heartbeatOnline(restarted);

        SessionState restoredSession;
        CHECK(restarted.getSession(committedSessionId, restoredSession));
        CHECK(restoredSession.objectId == committedObjectId);
        CHECK(restoredSession.completed.size() == 1);

        miniKV::gateway::FileMeta restoredFile;
        CHECK(restarted.getFile(committedFileHash, restoredFile));
        CHECK(restoredFile.fileHash == committedFileHash);
        CHECK(restoredFile.chunkHashes.size() == 1);

        miniKV::gateway::ObjectMeta restoredObject;
        CHECK(restarted.getObject(committedObjectId, restoredObject));
        CHECK(restoredObject.objectId == committedObjectId);
        CHECK(restoredObject.fileHash == committedFileHash);

        miniKV::gateway::ManifestSnapshot manifest;
        CHECK(restarted.buildManifestSnapshot(committedFileHash, manifest));
        CHECK(manifest.routes.size() == 1);

        miniKV::control::ObjectReadDescriptor descriptor;
        CHECK(restarted.buildObjectReadDescriptor(committedObjectId, descriptor));
        CHECK(descriptor.objectId == committedObjectId);
        CHECK(descriptor.objectVersion == restoredObject.objectVersion);
        CHECK(descriptor.chunks.size() == 1);

        // A stale pre-restart reservation must not consume node capacity.
        SessionState freshSession;
        CHECK(restarted.createSession("recovery-fresh", "/recovery", kChunkSize,
                                     static_cast<uint32_t>(kChunkSize), freshSession));
        std::vector<PlacementPlan> freshPlans;
        CHECK(restarted.planRoutes(freshSession.sessionId,
                                   {request(std::string(64, 'f'))}, freshPlans) ==
              RoutePlanStatus::kOk);
        CHECK(freshPlans.size() == 1);
        CHECK(restarted.releaseLease(freshPlans.front().leaseId));
        CHECK(!restarted.releaseLease(freshPlans.front().leaseId));
    }

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: sharded reservation recovery preserves committed metadata\n";
    std::cout << "PASS: runtime reservations do not resurrect across restart\n";
    return 0;
}
