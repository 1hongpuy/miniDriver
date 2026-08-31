#include "gateway/GatewayState.hpp"

#include <filesystem>
#include <iostream>
#include <leveldb/db.h>
#include <memory>
#include <string>
#include <vector>

namespace {

using miniKV::gateway::ChunkRouteRequest;
using miniKV::gateway::CommitChunkStatus;
using miniKV::gateway::FileCommitStatus;
using miniKV::gateway::GatewayState;
using miniKV::gateway::NodeRecord;
using miniKV::gateway::NodeRuntime;
using miniKV::gateway::PlacementPlan;
using miniKV::gateway::RoutePlanStatus;
using miniKV::gateway::SessionState;

constexpr uint64_t kChunkSize = 4ULL * 1024ULL * 1024ULL;
const std::string kChunkHash(64, 'a');

bool check(bool condition, const char* expression, int line)
{
    if(condition) return true;
    std::cerr << "FAIL:" << line << ": " << expression << '\n';
    return false;
}

#define CHECK(expression) \
    do { if(!check((expression), #expression, __LINE__)) return 1; } while(false)

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

void heartbeatOnline(GatewayState& state)
{
    NodeRuntime runtime;
    runtime.freeBytes = 900ULL * 1024ULL * 1024ULL;
    (void)state.heartbeat("node-a", runtime);
    (void)state.heartbeat("node-b", runtime);
}

std::vector<std::string> nodeIds(const PlacementPlan& plan)
{
    std::vector<std::string> result;
    for(const auto& node : plan.chain) result.push_back(node.record.nodeId);
    return result;
}

ChunkRouteRequest opaqueRequest()
{
    ChunkRouteRequest request;
    request.chunkIndex = 0;
    request.chunkHash = kChunkHash;
    request.chunkSize = kChunkSize;
    request.identityScheme = "opaque-chunk-id";
    request.checksumType = "sha256";
    request.checksumDigest = kChunkHash;
    return request;
}

std::string hex(const std::string& value)
{
    static constexpr char digits[] = "0123456789abcdef";
    std::string encoded;
    encoded.reserve(value.size() * 2);
    for(const unsigned char byte : value) {
        encoded.push_back(digits[byte >> 4]);
        encoded.push_back(digits[byte & 0x0f]);
    }
    return encoded;
}

}  // namespace

int main()
{
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() /
        "minikv_gateway_route_retry_identity_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    std::string sessionId;
    std::string objectId;
    std::string firstChunkId;
    {
        GatewayState state(directory.string());
        CHECK(state.open());
        CHECK(state.registerNode(storageNode("node-a")));
        CHECK(state.registerNode(storageNode("node-b")));
        heartbeatOnline(state);

        SessionState session;
        CHECK(state.createSession("retry.raw", "/benchmark", kChunkSize,
                                  static_cast<uint32_t>(kChunkSize), session));
        CHECK(!session.objectId.empty());
        CHECK(session.objectVersion == 1);
        CHECK(session.metadataVersion == 1);
        sessionId = session.sessionId;
        objectId = session.objectId;

        std::vector<PlacementPlan> plans;
        CHECK(state.planRoutes(sessionId, {opaqueRequest()}, plans) ==
              RoutePlanStatus::kOk);
        CHECK(plans.size() == 1);
        CHECK(plans[0].objectVersion == session.objectVersion);
        CHECK(!plans[0].chunkId.empty());
        firstChunkId = plans[0].chunkId;
    }

    {
        GatewayState restarted(directory.string());
        CHECK(restarted.open());
        SessionState restored;
        CHECK(restarted.getSession(sessionId, restored));
        CHECK(restored.objectId == objectId);
        CHECK(restored.objectVersion == 1);
        CHECK(restored.metadataVersion == 1);
        heartbeatOnline(restarted);

        std::vector<PlacementPlan> retryPlans;
        CHECK(restarted.planRoutes(sessionId, {opaqueRequest()}, retryPlans) ==
              RoutePlanStatus::kOk);
        CHECK(retryPlans.size() == 1);
        CHECK(retryPlans[0].chunkId == firstChunkId);
        CHECK(retryPlans[0].objectVersion == restored.objectVersion);
        CHECK(restarted.commitChunk(sessionId, 0, kChunkHash, kChunkSize,
                                    nodeIds(retryPlans[0]), retryPlans[0].leaseId) ==
              CommitChunkStatus::kCommitted);

        miniKV::gateway::FileMeta file;
        CHECK(restarted.commitFile(sessionId, file) == FileCommitStatus::kCommitted);
        CHECK(file.objectId == objectId);
        CHECK(file.objectVersion == restored.objectVersion);
        CHECK(file.metadataVersion == restored.metadataVersion);
    }

    std::filesystem::remove_all(directory, error);

    const std::filesystem::path legacyDirectory =
        std::filesystem::temp_directory_path() /
        "minikv_gateway_legacy_session_replay_test";
    std::filesystem::remove_all(legacyDirectory, error);
    const std::string legacySessionId = "legacy-session";
    {
        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::DB* raw = nullptr;
        CHECK(leveldb::DB::Open(options, legacyDirectory.string(), &raw).ok());
        std::unique_ptr<leveldb::DB> db(raw);
        // Original ten-field V2 Session encoding: no manifest, derived-job,
        // objectId or version fields.
        const std::string value = hex(legacySessionId) + "|" + hex("admin") +
            "|" + hex("legacy.raw") + "|" + hex("/legacy") + "|" +
            std::to_string(kChunkSize) + "|" + std::to_string(kChunkSize) +
            "|1|1|1|";
        CHECK(db->Put(leveldb::WriteOptions(), "s:" + legacySessionId, value).ok());
    }
    {
        GatewayState legacy(legacyDirectory.string());
        CHECK(legacy.open());
        SessionState restored;
        CHECK(legacy.getSession(legacySessionId, restored));
        CHECK(restored.objectId.empty());
        CHECK(restored.objectVersion == 1);
        CHECK(restored.metadataVersion == 1);
        CHECK(legacy.registerNode(storageNode("node-a")));
        CHECK(legacy.registerNode(storageNode("node-b")));
        heartbeatOnline(legacy);
        std::vector<PlacementPlan> plans;
        CHECK(legacy.planRoutes(legacySessionId, {opaqueRequest()}, plans) ==
              RoutePlanStatus::kOk);
        CHECK(plans.size() == 1);
        CHECK(legacy.commitChunk(legacySessionId, 0, kChunkHash, kChunkSize,
                                 nodeIds(plans[0]), plans[0].leaseId) ==
              CommitChunkStatus::kCommitted);
        miniKV::gateway::FileMeta file;
        CHECK(legacy.commitFile(legacySessionId, file) == FileCommitStatus::kCommitted);
        CHECK(!file.objectId.empty());
        CHECK(file.objectVersion == 1);
        CHECK(file.metadataVersion == 1);
    }
    std::filesystem::remove_all(legacyDirectory, error);
    std::cout << "PASS: Gateway restart preserves object and Chunk retry identity\n";
    return 0;
}
