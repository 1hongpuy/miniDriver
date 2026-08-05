#include "TestCheck.hpp"
#include "gateway/GatewayState.hpp"

#include <filesystem>
#include <chrono>
#include <vector>

namespace {

using miniKV::gateway::ChunkRouteRequest;
using miniKV::gateway::CommitChunkStatus;
using miniKV::gateway::DerivedUploadRequest;
using miniKV::gateway::DerivedUploadResult;
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

NodeRecord node(const std::string& id)
{
    NodeRecord result;
    result.nodeId = id;
    result.address = "127.0.0.1";
    result.maxStorageBytes = 1024ULL * 1024ULL * 1024ULL;
    result.maxConcurrentWrites = 2;
    result.capabilities = {"storage"};
    return result;
}

bool commitAll(GatewayState& state, const std::string& sessionId,
               const std::vector<ChunkRouteRequest>& chunks)
{
    std::vector<PlacementPlan> plans;
    if(state.planRoutes(sessionId, chunks, plans) != RoutePlanStatus::kOk ||
       plans.size() != chunks.size()) return false;
    for(size_t index = 0; index < chunks.size(); ++index) {
        std::vector<std::string> replicas;
        for(const auto& target : plans[index].chain) replicas.push_back(target.record.nodeId);
        if(state.commitChunk(sessionId, chunks[index].chunkIndex, chunks[index].chunkHash,
                             chunks[index].chunkSize, replicas, plans[index].leaseId) !=
           CommitChunkStatus::kCommitted) return false;
    }
    return true;
}

}  // namespace

int main()
{
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_gateway_derived_upload_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    GatewayState state(directory.string());
    NodeRuntime runtime;
    runtime.freeBytes = 900ULL * 1024ULL * 1024ULL;
    MINIKV_CHECK(state.open());
    MINIKV_CHECK(state.registerNode(node("node-a")));
    MINIKV_CHECK(state.registerNode(node("node-b")));
    MINIKV_CHECK(state.heartbeat("node-a", runtime));
    MINIKV_CHECK(state.heartbeat("node-b", runtime));

    UploadPreflightRequest source;
    source.fileName = "source.jpg";
    source.dirPath = "/shoots";
    source.fileSize = kChunkSize;
    source.chunkSize = kChunkSize;
    source.chunks = {{0, "source-chunk", kChunkSize}};
    source.manifestHash = GatewayState::manifestHash(source.fileSize, source.chunkSize, source.chunks);
    UploadPreflightResult sourceResult;
    MINIKV_CHECK(state.preflightUpload(source, sourceResult) == PreflightStatus::kUploadRequired);
    MINIKV_CHECK(commitAll(state, sourceResult.session.sessionId, sourceResult.missingChunks));
    FileMeta sourceFile;
    MINIKV_CHECK(state.commitFile(sourceResult.session.sessionId, sourceFile) == FileCommitStatus::kCommitted);

    const int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const auto enqueued = state.enqueueThumbnail(sourceFile.fileHash, "thumb-512-jpeg-v1", now);
    miniKV::media::MediaJob job;
    MINIKV_CHECK(state.claimMediaJob(enqueued.job.jobId, now, 120, job));

    DerivedUploadRequest request;
    request.fileName = "thumb-512-jpeg-v1.jpg";
    request.fileSize = kChunkSize;
    request.chunkSize = kChunkSize;
    request.chunks = {{0, "derived-chunk", kChunkSize}};
    request.manifestHash = GatewayState::manifestHash(request.fileSize, request.chunkSize, request.chunks);
    DerivedUploadResult derived;
    MINIKV_CHECK(state.createDerivedUpload(job.jobId, job.leaseToken, request, derived));
    MINIKV_CHECK(derived.missingChunks.size() == 1);
    MINIKV_CHECK(commitAll(state, derived.session.sessionId, derived.missingChunks));

    FileMeta derivedFile;
    MINIKV_CHECK(state.commitDerivedUpload(job.jobId, job.leaseToken,
                                           derived.session.sessionId, derivedFile) ==
                 FileCommitStatus::kCommitted);
    MINIKV_CHECK(!derivedFile.objectId.empty());
    miniKV::gateway::ObjectMeta derivedObject;
    MINIKV_CHECK(state.getObject(derivedFile.objectId, derivedObject));
    MINIKV_CHECK(derivedObject.parentPath.empty());
    miniKV::gateway::CatalogSnapshot catalog;
    MINIKV_CHECK(state.listCatalog("/shoots", catalog));
    MINIKV_CHECK(catalog.files.size() == 1);
    MINIKV_CHECK(catalog.thumbnailsByFileHash.count(sourceFile.fileHash) == 1);
    MINIKV_CHECK(catalog.thumbnailsByFileHash.at(sourceFile.fileHash).state ==
                 miniKV::media::JobState::kReady);
    miniKV::media::ThumbnailMeta thumbnail;
    MINIKV_CHECK(state.getThumbnail(sourceFile.fileHash, "thumb-512-jpeg-v1", thumbnail));
    MINIKV_CHECK(thumbnail.state == miniKV::media::JobState::kReady);
    MINIKV_CHECK(thumbnail.derivedObjectId == derivedFile.objectId);
    MINIKV_CHECK(thumbnail.derivedFileHash == derivedFile.fileHash);

    std::filesystem::remove_all(directory, error);
    return 0;
}
