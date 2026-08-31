#include "TestCheck.hpp"
#include "gateway/GatewayState.hpp"

#include <filesystem>
#include <vector>

namespace {

miniKV::gateway::NodeRecord node(const std::string& id)
{
    miniKV::gateway::NodeRecord value;
    value.nodeId = id;
    value.address = "127.0.0.1";
    value.maxStorageBytes = 1024ULL * 1024ULL * 1024ULL;
    value.capabilities = {"storage"};
    return value;
}

std::vector<std::string> nodeIds(const miniKV::gateway::PlacementPlan& plan)
{
    std::vector<std::string> result;
    for(const auto& member : plan.chain) result.push_back(member.record.nodeId);
    return result;
}

}  // namespace

int main()
{
    const auto directory = std::filesystem::temp_directory_path() / "minikv_gateway_ai_index_outbox_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    {
        miniKV::gateway::GatewayState state(directory.string());
        miniKV::gateway::NodeRuntime runtime;
        runtime.freeBytes = 1024ULL * 1024ULL * 1024ULL;
        MINIKV_CHECK(state.open());
        MINIKV_CHECK(state.registerNode(node("node-a")));
        MINIKV_CHECK(state.registerNode(node("node-b")));
        MINIKV_CHECK(state.heartbeat("node-a", runtime));
        MINIKV_CHECK(state.heartbeat("node-b", runtime));

        miniKV::gateway::SessionState session;
        MINIKV_CHECK(state.createSession("temple-sunset.jpg", "/travel", 1024, 1024, session));
        std::vector<miniKV::gateway::ChunkRouteRequest> requests = {{0, "chunk-hash", 1024}};
        std::vector<miniKV::gateway::PlacementPlan> plans;
        MINIKV_CHECK(state.planRoutes(session.sessionId, requests, plans) ==
                     miniKV::gateway::RoutePlanStatus::kOk);
        MINIKV_CHECK(plans.size() == 1);
        MINIKV_CHECK(state.commitChunk(session.sessionId, 0, "chunk-hash", 1024, nodeIds(plans[0]),
                                       plans[0].leaseId) == miniKV::gateway::CommitChunkStatus::kCommitted);

        miniKV::gateway::FileMeta file;
        MINIKV_CHECK(state.commitFile(session.sessionId, file) == miniKV::gateway::FileCommitStatus::kCommitted);
        const auto events = state.dueAiIndexEvents(file.createdAt, 10);
        MINIKV_CHECK(events.size() == 1);
        MINIKV_CHECK(events.front().eventId == file.objectId);
        MINIKV_CHECK(events.front().objectId == file.objectId);
        MINIKV_CHECK(events.front().objectKey == "/travel/temple-sunset.jpg");
        MINIKV_CHECK(events.front().fileHash == file.fileHash);
        MINIKV_CHECK(state.markAiIndexEventPublished(events.front().eventId, file.createdAt + 1));
        MINIKV_CHECK(state.dueAiIndexEvents(file.createdAt + 2, 10).empty());
    }

    {
        miniKV::gateway::GatewayState recovered(directory.string());
        MINIKV_CHECK(recovered.open());
        MINIKV_CHECK(recovered.dueAiIndexEvents(9999999999, 10).empty());
    }
    std::filesystem::remove_all(directory, error);
    return 0;
}
