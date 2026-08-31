#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "http/HttpServer.hpp"
#include "network/EventLoop.hpp"
#include "utils/ThreadPool.hpp"
#include "gateway/GatewayState.hpp"
#include "utils/Util.hpp"
#include "http/DeferredResponse.hpp"
#include "http/AsyncHttpClient.hpp"
#include "media/RawEmbeddedPreview.hpp"
#include "media/RedisAiEventPublisher.hpp"
#include "media/RedisTaskPublisher.hpp"
#include "utils/AsyncLogger.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>
#include <mutex>
#include <set>
#include <sstream>

using miniKV::http::HttpRequest;
using miniKV::http::HttpResponse;
using miniKV::network::EventLoop;
using miniKV::utils::ThreadPool;
using namespace miniKV::util;
using namespace miniKV::gateway;
namespace {

using Clock = std::chrono::steady_clock;

uint64_t elapsedMicroseconds(Clock::time_point started, Clock::time_point finished)
{
    if(finished <= started) return 0;
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
        finished - started).count());
}

bool beginsWith(const std::string& value, const std::string& prefix) { return value.rfind(prefix, 0) == 0; }
std::string pathTail(const std::string& path, const std::string& prefix, const std::string& suffix = "") {
    if (!beginsWith(path, prefix)) return {}; std::string value = path.substr(prefix.size());
    if (!suffix.empty() && value.size() >= suffix.size() && value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0) value.resize(value.size() - suffix.size());
    return value;
}
void json(HttpResponse* response, int status, const std::string& body) {
    response->setStatusCode(static_cast<HttpResponse::HttpStatusCode>(status));
    response->setContentType("application/json"); response->setBody(body);
}

std::string configuredSecret(int argc, char** argv) {
    if (argc > 3) return argv[3];
    if (const char* value = std::getenv("MINIKV_V2_CLUSTER_SECRET")) return value;
    return {};
}

std::string configuredNodeId()
{
    if(const char* value = std::getenv("MINIKV_V2_NODE_ID")) return value;
    return "-";
}

struct ChunkProtocolConfig {
    // V3's new-object default: physical identity is opaque and CRC32C is the
    // streaming transport/storage checksum. Existing cas-sha256 objects keep
    // their recorded scheme on read; operators can explicitly select the
    // legacy write protocol through MINIKV_V3_* environment variables.
    std::string identityScheme = "opaque-chunk-id";
    std::string checksumType = "crc32c";
};

bool configuredChunkProtocol(ChunkProtocolConfig& config)
{
    if(const char* value = std::getenv("MINIKV_V3_IDENTITY_SCHEME")) {
        config.identityScheme = value;
    }
    if(const char* value = std::getenv("MINIKV_V3_CHECKSUM_TYPE")) {
        config.checksumType = value;
    }
    const bool identityValid = config.identityScheme == "cas-sha256" ||
        config.identityScheme == "opaque-chunk-id";
    const bool checksumValid = config.checksumType == "sha256" ||
        config.checksumType == "crc32c";
    return identityValid && checksumValid &&
        !(config.identityScheme == "cas-sha256" && config.checksumType != "sha256");
}

uint64_t configuredUint(const char* name, uint64_t fallback)
{
    const char* value = std::getenv(name);
    if(value == nullptr || *value == '\0') return fallback;
    try {
        const unsigned long long parsed = std::stoull(value);
        return parsed == 0 ? fallback : static_cast<uint64_t>(parsed);
    } catch(...) {
        return fallback;
    }
}

miniKV::media::RedisTaskPublisherConfig configuredRedisPublisher()
{
    miniKV::media::RedisTaskPublisherConfig config;
    if(const char* value = std::getenv("MINIKV_V2_REDIS_ADDRESS")) config.address = value;
    const uint64_t port = configuredUint("MINIKV_V2_REDIS_PORT", config.port);
    if(port <= UINT16_MAX) config.port = static_cast<uint16_t>(port);
    if(const char* value = std::getenv("MINIKV_V2_REDIS_THUMBNAIL_STREAM")) {
        config.thumbnailStream = value;
    }
    config.streamMaxLen = configuredUint("MINIKV_V2_REDIS_STREAM_MAXLEN", config.streamMaxLen);
    return config;
}

miniKV::media::RedisAiEventPublisherConfig configuredAiEventPublisher()
{
    miniKV::media::RedisAiEventPublisherConfig config;
    if(const char* value = std::getenv("MINIKV_V2_REDIS_ADDRESS")) config.address = value;
    const uint64_t port = configuredUint("MINIKV_V2_REDIS_PORT", config.port);
    if(port <= UINT16_MAX) config.port = static_cast<uint16_t>(port);
    if(const char* value = std::getenv("MINIKV_V2_REDIS_AI_STREAM")) config.stream = value;
    config.streamMaxLen = configuredUint("MINIKV_V2_REDIS_STREAM_MAXLEN", config.streamMaxLen);
    return config;
}

bool isDerivedImageSourceFileName(const std::string& fileName)
{
    const size_t dot = fileName.rfind('.');
    if(dot == std::string::npos) return false;
    std::string suffix = fileName.substr(dot);
    std::transform(suffix.begin(), suffix.end(), suffix.begin(),
                   [](unsigned char value) { return static_cast<char>(std::tolower(value)); });
    return suffix == ".jpg" || suffix == ".jpeg" ||
           miniKV::media::isRawEmbeddedPreviewFileName(fileName);
}

constexpr std::array<const char*, 2> kJpegDerivedProfiles = {
    "thumb-512-jpeg-v1",
    "preview-2048-jpeg-v1",
};

const char* mediaJobStateName(miniKV::media::JobState state)
{
    switch(state) {
    case miniKV::media::JobState::kPending: return "PENDING";
    case miniKV::media::JobState::kRunning: return "RUNNING";
    case miniKV::media::JobState::kReady: return "READY";
    case miniKV::media::JobState::kFailed: return "FAILED";
    case miniKV::media::JobState::kUnsupported: return "UNSUPPORTED";
    }
    return "FAILED";
}

std::string mediaJobJson(const miniKV::media::MediaJob& job)
{
    return "{\"jobId\":\"" + jsonEscape(job.jobId) + "\",\"type\":\"thumbnail\",\"sourceFileHash\":\"" +
           jsonEscape(job.sourceFileHash) + "\",\"profile\":\"" + jsonEscape(job.profile) +
           "\",\"state\":\"" + mediaJobStateName(job.state) +
           "\",\"attempts\":" + std::to_string(job.attempts) + ",\"leaseUntil\":" +
           std::to_string(job.leaseUntil) + ",\"leaseToken\":\"" + jsonEscape(job.leaseToken) + "\"}";
}

std::vector<ChunkRouteRequest> parseRouteRequests(const std::string& body) {
    std::vector<ChunkRouteRequest> out;
    for (const auto& object : jsonObjectArray(body, "chunks")) {
        ChunkRouteRequest request;
        request.chunkIndex = static_cast<uint32_t>(jsonUint(object, "index", UINT32_MAX));
        request.chunkHash = jsonString(object, "hash");
        request.chunkSize = jsonUint(object, "size");
        request.checksumType = jsonString(object, "checksumType");
        request.checksumDigest = jsonString(object, "checksumDigest");
        if (request.chunkIndex == UINT32_MAX || request.chunkHash.empty() || request.chunkSize == 0) {
            return {};
        }
        out.push_back(std::move(request));
    }
    return out;
}

std::string nodeTarget(const NodeRecord& node) {
    return node.nodeId + "@" + node.address + ":" + std::to_string(node.httpPort);
}

const char* fileStateName(FileState state) {
    switch (state) {
    case FileState::kAvailable: return "AVAILABLE";
    case FileState::kDegraded: return "DEGRADED";
    case FileState::kProtecting: return "PROTECTING";
    }
    return "PROTECTING";
}

std::string manifestJson(const ManifestSnapshot& snapshot) {
    std::ostringstream out;
    out << "{\"objectId\":\"" << jsonEscape(snapshot.file.objectId)
        << "\",\"objectVersion\":" << snapshot.file.objectVersion
        << ",\"metadataVersion\":" << snapshot.file.metadataVersion
        << ",\"fileHash\":\"" << jsonEscape(snapshot.file.fileHash)
        << "\",\"fileSize\":" << snapshot.file.fileSize
        << ",\"chunkSize\":" << snapshot.file.chunkSize << ",\"chunks\":[";
    for (size_t i = 0; i < snapshot.routes.size(); ++i) {
        if (i) out << ',';
        const ChunkRoute& route = snapshot.routes[i];
        out << "{\"index\":" << i << ",\"hash\":\"" << jsonEscape(route.chunkHash)
            << "\",\"chunkId\":\"" << jsonEscape(route.chunkId)
            << "\",\"storageIdentity\":\"" << jsonEscape(
                route.identityScheme == "opaque-chunk-id" ? route.chunkId : route.chunkHash)
            << "\",\"identityScheme\":\"" << jsonEscape(route.identityScheme)
            << "\",\"checksumType\":\"" << jsonEscape(route.checksumType)
            << "\",\"checksumDigest\":\"" << jsonEscape(route.checksumDigest)
            << "\",\"objectVersion\":" << route.objectVersion
            << ",\"generation\":" << route.generation
            << ",\"replicas\":[";
        bool first = true;
        for (const auto& nodeId : route.replicas) {
            const auto node = snapshot.nodes.find(nodeId);
            if (node == snapshot.nodes.end()) continue;
            if (!first) out << ',';
            first = false;
            out << "{\"nodeId\":\"" << jsonEscape(node->second.nodeId)
                << "\",\"address\":\"" << jsonEscape(node->second.address)
                << "\",\"httpPort\":" << node->second.httpPort << "}";
        }
        out << "]}";
    }
    out << "]}";
    return out.str();
}

std::string catalogJson(const CatalogSnapshot& snapshot) {
    std::ostringstream out;
    out << "{\"path\":\"" << jsonEscape(snapshot.path) << "\",\"breadcrumbs\":[";
    for (size_t i = 0; i < snapshot.breadcrumbs.size(); ++i) {
        if (i) out << ',';
        const Breadcrumb& breadcrumb = snapshot.breadcrumbs[i];
        out << "{\"name\":\"" << jsonEscape(breadcrumb.name) << "\",\"path\":\""
            << jsonEscape(breadcrumb.path) << "\"}";
    }
    out << "],\"directories\":[";
    for (size_t i = 0; i < snapshot.directories.size(); ++i) {
        if (i) out << ',';
        const DirectoryMeta& directory = snapshot.directories[i];
        out << "{\"path\":\"" << jsonEscape(directory.path) << "\",\"createdAt\":"
            << directory.createdAt << "}";
    }
    out << "],\"files\":[";
    for (size_t i = 0; i < snapshot.files.size(); ++i) {
        if (i) out << ',';
        const ObjectMeta& file = snapshot.files[i];
        out << "{\"objectId\":\"" << jsonEscape(file.objectId) << "\",\"name\":\""
            << jsonEscape(file.name) << "\",\"fileHash\":\"" << jsonEscape(file.fileHash)
            << "\",\"objectVersion\":" << file.objectVersion
            << ",\"metadataVersion\":" << file.metadataVersion
            << ",\"fileSize\":" << file.fileSize << ",\"state\":\""
            << fileStateName(file.state) << "\",\"createdAt\":" << file.createdAt;
        const auto appendDerived = [&out](const char* field, const miniKV::media::ThumbnailMeta& derived) {
            out << ",\"" << field << "\":{\"profile\":\"" << jsonEscape(derived.profile)
                << "\",\"state\":\"" << mediaJobStateName(derived.state) << "\"";
            if(derived.state == miniKV::media::JobState::kReady) {
                out << ",\"objectId\":\"" << jsonEscape(derived.derivedObjectId) << "\"";
            }
            out << "}";
        };
        if(const auto thumbnail = snapshot.thumbnailsByFileHash.find(file.fileHash);
           thumbnail != snapshot.thumbnailsByFileHash.end()) {
            appendDerived("thumbnail", thumbnail->second);
        }
        if(const auto preview = snapshot.previewsByFileHash.find(file.fileHash);
           preview != snapshot.previewsByFileHash.end()) {
            appendDerived("preview", preview->second);
        }
        out << "}";
    }
    out << "]}";
    return out.str();
}

std::string objectJson(const ObjectMeta& object) {
    return "{\"objectId\":\"" + jsonEscape(object.objectId) + "\",\"parentPath\":\"" +
           jsonEscape(object.parentPath) + "\",\"name\":\"" + jsonEscape(object.name) +
           "\",\"objectVersion\":" + std::to_string(object.objectVersion) +
           ",\"metadataVersion\":" + std::to_string(object.metadataVersion) +
           ",\"fileHash\":\"" + jsonEscape(object.fileHash) + "\",\"fileSize\":" +
           std::to_string(object.fileSize) + ",\"state\":\"" + fileStateName(object.state) + "\"}";
}

std::string uploadPreflightJson(const UploadPreflightResult& result,
                                const ChunkProtocolConfig& protocol) {
    std::ostringstream out;
    out << "{\"status\":\"UPLOAD_REQUIRED\",\"sessionId\":\"" << jsonEscape(result.session.sessionId)
        << "\",\"objectId\":\"" << jsonEscape(result.session.objectId)
        << "\",\"objectVersion\":" << result.session.objectVersion
        << ",\"metadataVersion\":" << result.session.metadataVersion
        << ",\"manifestHash\":\"" << jsonEscape(result.session.manifestHash)
        << "\",\"identityScheme\":\"" << jsonEscape(protocol.identityScheme)
        << "\",\"checksumType\":\"" << jsonEscape(protocol.checksumType)
        << "\",\"chunkSize\":" << result.session.chunkSize
        << ",\"totalChunks\":" << result.session.totalChunks << ",\"completed\":[";
    bool first = true;
    for (const auto& [index, chunk] : result.session.completed) {
        if (!first) out << ',';
        first = false;
        out << index;
    }
    out << "],\"missingIndices\":[";
    for (size_t i = 0; i < result.missingChunks.size(); ++i) {
        if (i) out << ',';
        out << result.missingChunks[i].chunkIndex;
    }
    out << "]}";
    return out.str();
}

std::string derivedUploadJson(const DerivedUploadResult& result)
{
    std::ostringstream out;
    out << "{\"sessionId\":\"" << jsonEscape(result.session.sessionId)
        << "\",\"objectId\":\"" << jsonEscape(result.session.objectId)
        << "\",\"objectVersion\":" << result.session.objectVersion
        << ",\"metadataVersion\":" << result.session.metadataVersion
        << ",\"manifestHash\":\"" << jsonEscape(result.session.manifestHash)
        << "\",\"chunkSize\":" << result.session.chunkSize
        << ",\"totalChunks\":" << result.session.totalChunks << ",\"completed\":[";
    bool first = true;
    for(const auto& [index, chunk] : result.session.completed) {
        if(!first) out << ',';
        first = false;
        out << index;
    }
    out << "],\"missingChunks\":[";
    for(size_t index = 0; index < result.missingChunks.size(); ++index) {
        if(index != 0) out << ',';
        const auto& chunk = result.missingChunks[index];
        out << "{\"index\":" << chunk.chunkIndex << ",\"hash\":\""
            << jsonEscape(chunk.chunkHash) << "\",\"size\":" << chunk.chunkSize << "}";
    }
    out << "]}";
    return out.str();
}

bool parseObjectReadPlanPath(const std::string& path, std::string& objectId,
                             uint64_t& objectVersion)
{
    constexpr const char* prefix = "/internal/v3/objects/";
    constexpr const char* marker = "/versions/";
    constexpr const char* suffix = "/read-plan";
    if(!beginsWith(path, prefix) || path.size() <= std::strlen(prefix) + std::strlen(suffix) ||
       path.compare(path.size() - std::strlen(suffix), std::strlen(suffix), suffix) != 0) {
        return false;
    }
    const std::string value = path.substr(
        std::strlen(prefix), path.size() - std::strlen(prefix) - std::strlen(suffix));
    const size_t markerAt = value.find(marker);
    if(markerAt == std::string::npos || markerAt == 0) return false;
    objectId = value.substr(0, markerAt);
    const std::string version = value.substr(markerAt + std::strlen(marker));
    try {
        size_t parsed = 0;
        objectVersion = std::stoull(version, &parsed);
        return parsed == version.size() && objectVersion > 0;
    } catch(...) {
        return false;
    }
}

std::string objectReadPlanJson(miniKV::control::ObjectReadDescriptor descriptor,
                               const std::string& principalId,
                               const std::string& clusterSecret)
{
    std::ostringstream out;
    out << "{\"objectId\":\"" << jsonEscape(descriptor.objectId)
        << "\",\"objectVersion\":" << descriptor.objectVersion
        << ",\"metadataVersion\":" << descriptor.metadataVersion
        << ",\"fileSize\":" << descriptor.fileSize
        << ",\"chunkSize\":" << descriptor.chunkSize << ",\"chunks\":[";
    for(size_t index = 0; index < descriptor.chunks.size(); ++index) {
        auto& chunk = descriptor.chunks[index];
        miniKV::util::ReadCapability capability;
        capability.capabilityId = randomId();
        capability.principalId = principalId;
        capability.objectId = descriptor.objectId;
        capability.objectVersion = descriptor.objectVersion;
        capability.storageIdentity = chunk.storageIdentity;
        capability.expiresAt = unixSeconds() + 300;
        chunk.readCapability = issueReadCapability(capability, clusterSecret);
        if(chunk.readCapability.empty()) return {};
        if(index != 0) out << ',';
        out << "{\"index\":" << chunk.index << ",\"chunkId\":\""
            << jsonEscape(chunk.chunkId) << "\",\"storageIdentity\":\""
            << jsonEscape(chunk.storageIdentity) << "\",\"size\":" << chunk.size
            << ",\"checksumType\":\"" << jsonEscape(chunk.checksumType)
            << "\",\"checksumDigest\":\"" << jsonEscape(chunk.checksumDigest)
            << "\",\"generation\":" << chunk.generation
            << ",\"readCapability\":\"" << jsonEscape(chunk.readCapability)
            << "\",\"replicas\":[";
        for(size_t replicaIndex = 0; replicaIndex < chunk.replicas.size(); ++replicaIndex) {
            if(replicaIndex != 0) out << ',';
            const auto& replica = chunk.replicas[replicaIndex];
            out << "{\"nodeId\":\"" << jsonEscape(replica.nodeId)
                << "\",\"address\":\"" << jsonEscape(replica.address)
                << "\",\"port\":" << replica.port << '}';
        }
        out << "]}";
    }
    out << "]}";
    return out.str();
}

}  // namespace

int main(int argc, char** argv) {
    const int port = argc > 1 ? std::stoi(argv[1]) : 8081;
    const std::string dataDir = argc > 2 ? argv[2] : "./v2_gateway_data";
    const std::string clusterSecret = configuredSecret(argc, argv);
    if (clusterSecret.empty()) {
        std::cerr << "MINIKV_V2_CLUSTER_SECRET or a command-line clusterSecret is required\n";
        return 2;
    }
    ChunkProtocolConfig chunkProtocol;
    if(!configuredChunkProtocol(chunkProtocol)) {
        std::cerr << "invalid MINIKV_V3_IDENTITY_SCHEME/MINIKV_V3_CHECKSUM_TYPE combination\n";
        return 2;
    }
    std::filesystem::create_directories(dataDir);
    if(!miniKV::utils::initAsyncLogger(miniKV::utils::asyncLoggerConfigFromEnvironment(
           "gateway", configuredNodeId(), dataDir + "/logs/gateway.log"))) {
        std::cerr << "cannot initialize Gateway async logger\n";
    }
    std::string stateDir = dataDir + "/metadata";
    GatewayState state(stateDir);
    if (!state.open()) {
        miniKV::utils::logError("event=gateway_metadata_open_failed path=" + stateDir);
        std::cerr << "cannot open Gateway metadata\n";
        return 1;
    }
    miniKV::utils::logInfo("event=gateway_chunk_protocol identity_scheme=" +
                           chunkProtocol.identityScheme + " checksum_type=" +
                           chunkProtocol.checksumType);

    ThreadPool workers(4);
    EventLoop loop;
    miniKV::media::RedisTaskPublisher mediaPublisher(configuredRedisPublisher());
    if(!mediaPublisher.start()) {
        miniKV::utils::logError("event=media_publisher_start_failed");
        std::cerr << "cannot start media task publisher\n";
        return 1;
    }
    std::mutex aiEventDispatchMutex;
    std::set<std::string> aiEventsInFlight;
    miniKV::media::RedisAiEventPublisher aiEventPublisher(
        configuredAiEventPublisher(),
        [&](const miniKV::media::AiIndexEvent& event) {
            const bool marked = state.markAiIndexEventPublished(event.eventId, unixSeconds());
            if(!marked) {
                miniKV::utils::logWarn("event=ai_index_outbox_mark_failed event_id=" + event.eventId);
            } else {
                miniKV::utils::logInfo("event=ai_index_event_published event_id=" + event.eventId +
                                       " object=" + event.objectId);
            }
            std::lock_guard<std::mutex> lock(aiEventDispatchMutex);
            aiEventsInFlight.erase(event.eventId);
        });
    if(!aiEventPublisher.start()) {
        miniKV::utils::logError("event=ai_index_publisher_start_failed");
        std::cerr << "cannot start AI index event publisher\n";
        return 1;
    }
    miniKV::http::HttpServer server(&loop, &workers, port);
    std::set<std::string> deleteRequestsInFlight;
    std::function<void()> dispatchPendingDeletes;
    dispatchPendingDeletes = [&] {
        for(const NodeSnapshot& node : state.nodes()) {
            if(node.runtime.state != NodeLiveState::kOnline) continue;
            for(const DeleteTaskSnapshot& task : state.pendingDeletesForNode(node.record.nodeId)) {
                const std::string requestKey = node.record.nodeId + "\n" + task.storageIdentity;
                if(!deleteRequestsInFlight.insert(requestKey).second) continue;

                auto request = miniKV::http::AsyncHttpRequest::create(&loop);
                miniKV::http::AsyncHttpRequestOptions options;
                options.address = node.record.address;
                options.port = node.record.httpPort;
                options.method = "DELETE";
                options.path = "/internal/v2/chunks/" + task.storageIdentity;
                options.timeoutMs = 10000;
                options.headers = {{"X-Cluster-Internal-Token", clusterSecret}};
                request->open(std::move(options), [request] {
                    request->finishBody();
                }, [&, requestKey, nodeId = node.record.nodeId, chunkHash = task.chunkHash]
                    (miniKV::http::HttpClientResponse response, std::string error) {
                    deleteRequestsInFlight.erase(requestKey);
                    if(!error.empty() || response.status < 200 || response.status >= 300) {
                        miniKV::utils::logWarn("event=delete_dispatch_failed chunk=" + chunkHash +
                                               " node=" + nodeId + " error=" +
                                               (error.empty() ? "http_" + std::to_string(response.status) : error));
                        return;
                    }
                    if(!state.acknowledgeDelete(chunkHash, nodeId)) {
                        miniKV::utils::logError("event=delete_ack_persist_failed chunk=" + chunkHash +
                                                " node=" + nodeId);
                    }
                });
            }
        }
    };
    const auto enqueueMediaJob = [&](const miniKV::media::MediaJob& job) {
        if(!mediaPublisher.enqueue(job)) {
            miniKV::utils::logWarn("event=media_dispatch_deferred job=" + job.jobId +
                                   " reason=publisher_queue_full");
            return;
        }
        constexpr int64_t kRedisRepublishSeconds = 30;
        if(!state.deferMediaJobDispatch(job.jobId, unixSeconds() + kRedisRepublishSeconds)) {
            miniKV::media::MediaJob current;
            if(!state.getMediaJob(job.jobId, current) ||
               current.state == miniKV::media::JobState::kPending) {
                miniKV::utils::logWarn("event=media_dispatch_persist_failed job=" + job.jobId);
            }
        }
    };
    const auto dispatchPendingMediaJobs = [&] {
        constexpr size_t kDispatchBatchSize = 128;
        for(const miniKV::media::MediaJob& job : state.dueMediaJobs(unixSeconds(), kDispatchBatchSize)) {
            enqueueMediaJob(job);
        }
    };
    const auto dispatchAiIndexEvent = [&](const miniKV::media::AiIndexEvent& event) {
        {
            std::lock_guard<std::mutex> lock(aiEventDispatchMutex);
            if(!aiEventsInFlight.insert(event.eventId).second) return;
        }
        if(aiEventPublisher.enqueue(event)) return;
        {
            std::lock_guard<std::mutex> lock(aiEventDispatchMutex);
            aiEventsInFlight.erase(event.eventId);
        }
        miniKV::utils::logWarn("event=ai_index_dispatch_deferred event_id=" + event.eventId +
                               " reason=publisher_queue_full");
    };
    const auto dispatchPendingAiIndexEvents = [&] {
        constexpr size_t kDispatchBatchSize = 128;
        for(const miniKV::media::AiIndexEvent& event :
            state.dueAiIndexEvents(unixSeconds(), kDispatchBatchSize)) {
            dispatchAiIndexEvent(event);
        }
    };
    server.setHttpCallback([&](const HttpRequest& request, HttpResponse* response,
                               const miniKV::network::TcpConnectionPtr&,
                               const miniKV::http::DeferredResponse::Ptr&) {
        const std::string& path = request.path();
        const std::string body = request.body();
        std::string requestId = request.getHeader("X-Request-Id");
        if(requestId.empty()) requestId = randomId();
        response->addHeader("X-Request-Id", requestId);
        if(request.method() == HttpRequest::kGet && path == "/healthz") {
            json(response, 200, "{\"status\":\"ok\",\"component\":\"gateway\"}");
            return;
        }
        if(request.method() == HttpRequest::kGet && path == "/readyz") {
            json(response, 200, "{\"status\":\"ready\",\"component\":\"gateway\"}");
            return;
        }
        std::string readObjectId;
        uint64_t readObjectVersion = 0;
        if(request.method() == HttpRequest::kPost &&
           parseObjectReadPlanPath(path, readObjectId, readObjectVersion)) {
            if(!constantTimeEquals(request.getHeader("X-Cluster-Internal-Token"), clusterSecret)) {
                json(response, 403, jsonError("invalid cluster token"));
                return;
            }
            const std::string principalId = request.getHeader("X-Service-Principal");
            if(principalId.empty()) {
                json(response, 400, jsonError("service principal is required"));
                return;
            }
            miniKV::control::ObjectReadDescriptor descriptor;
            if(!state.buildObjectReadDescriptor(readObjectId, descriptor) ||
               descriptor.objectVersion != readObjectVersion) {
                json(response, 404, jsonError("object version not found"));
                return;
            }
            const std::string plan = objectReadPlanJson(
                std::move(descriptor), principalId, clusterSecret);
            if(plan.empty()) {
                json(response, 500, jsonError("cannot issue read capability"));
                return;
            }
            miniKV::utils::logInfo("event=object_read_plan request_id=" + requestId +
                                   " object=" + readObjectId + " version=" +
                                   std::to_string(readObjectVersion));
            json(response, 200, plan);
            return;
        }
        if (request.method() == HttpRequest::kPost && path == "/api/v2/nodes/register") {
            if (!constantTimeEquals(request.getHeader("X-Cluster-Internal-Token"), clusterSecret)) { json(response, 403, jsonError("invalid cluster token")); return; }
            NodeRecord node; node.nodeId = jsonString(body, "nodeId"); node.address = jsonString(body, "address"); node.httpPort = static_cast<uint16_t>(jsonUint(body, "httpPort", 9002)); node.maxStorageBytes = jsonUint(body, "maxStorageBytes"); node.reservedBytes = jsonUint(body, "reservedBytes"); node.maxConcurrentWrites = static_cast<uint32_t>(jsonUint(body, "maxConcurrentWrites", 2));
            for(const std::string& capability : split(jsonString(body, "capabilities"), ',')) {
                if(!capability.empty()) node.capabilities.push_back(capability);
            }
            if(node.capabilities.empty()) node.capabilities = {"storage"};
            if (!state.registerNode(node)) {
                json(response, 400, jsonError("invalid node registration"));
            } else {
                miniKV::utils::logInfo("event=node_registered node=" + node.nodeId +
                                       " address=" + node.address + " port=" +
                                       std::to_string(node.httpPort));
                json(response, 200, "{\"status\":\"registered\"}");
            }
            return;
        }
        if (request.method() == HttpRequest::kPost && beginsWith(path, "/api/v2/nodes/") && path.find("/heartbeat") != std::string::npos) {
            if (!constantTimeEquals(request.getHeader("X-Cluster-Internal-Token"), clusterSecret)) { json(response, 403, jsonError("invalid cluster token")); return; }
            const std::string nodeId = pathTail(path, "/api/v2/nodes/", "/heartbeat"); NodeRuntime runtime; runtime.usedBytes = jsonUint(body, "usedBytes"); runtime.freeBytes = jsonUint(body, "freeBytes"); runtime.cpuUsage = static_cast<double>(jsonUint(body, "cpuPermille")) / 1000.0; runtime.memoryUsage = static_cast<double>(jsonUint(body, "memoryPermille")) / 1000.0; runtime.diskIoUsage = static_cast<double>(jsonUint(body, "diskIoPermille")) / 1000.0; runtime.netOutMbps = static_cast<double>(jsonUint(body, "netOutMbps")); runtime.activeUploads = static_cast<uint32_t>(jsonUint(body, "activeUploads"));
            if (!state.heartbeat(nodeId, runtime)) {
                json(response, 404, jsonError("unknown node"));
            } else {
                dispatchPendingDeletes();
                json(response, 200, "{\"status\":\"ok\"}");
            }
            return;
        }
        if (request.method() == HttpRequest::kGet && path == "/api/v2/catalog") {
            CatalogSnapshot snapshot;
            const std::string catalogPath = getQueryValue(request.query(), "path");
            if (!state.listCatalog(catalogPath.empty() ? "/" : catalogPath, snapshot)) {
                json(response, 404, jsonError("directory not found"));
            } else {
                json(response, 200, catalogJson(snapshot));
            }
            return;
        }
        if (request.method() == HttpRequest::kPost && path == "/api/v2/directories") {
            DirectoryMeta directory;
            if (!state.createDirectory(jsonString(body, "parentPath"), jsonString(body, "name"), &directory)) {
                json(response, 400, jsonError("invalid parent directory, name, or duplicate path"));
            } else {
                json(response, 201, "{\"path\":\"" + jsonEscape(directory.path) + "\",\"createdAt\":" +
                    std::to_string(directory.createdAt) + "}");
            }
            return;
        }
        if (request.method() == HttpRequest::kPost && path == "/api/v2/upload/preflight") {
            UploadPreflightRequest preflight;
            preflight.fileName = jsonString(body, "fileName");
            preflight.dirPath = jsonString(body, "dirPath");
            preflight.fileSize = jsonUint(body, "fileSize");
            preflight.chunkSize = static_cast<uint32_t>(jsonUint(body, "chunkSize"));
            preflight.manifestHash = jsonString(body, "manifestHash");
            preflight.chunks = parseRouteRequests(body);

            UploadPreflightResult result;
            const auto preflightStarted = Clock::now();
            const PreflightStatus status = state.preflightUpload(preflight, result);
            miniKV::utils::logInfo("event=upload_preflight status=" + std::to_string(static_cast<int>(status)) +
                                   " file=" + preflight.fileName + " chunks=" +
                                   std::to_string(preflight.chunks.size()) + " elapsed_us=" +
                                   std::to_string(elapsedMicroseconds(preflightStarted, Clock::now())));
            if (status == PreflightStatus::kPathConflict) {
                json(response, 409, jsonError("a file already exists at this path"));
            } else if (status == PreflightStatus::kContentExists) {
                json(response, 200, "{\"status\":\"CONTENT_EXISTS\",\"object\":" +
                    objectJson(result.object) + "}");
            } else if (status == PreflightStatus::kUploadRequired) {
                json(response, 200, uploadPreflightJson(result, chunkProtocol));
            } else {
                json(response, 400, jsonError("invalid upload manifest"));
            }
            return;
        }
        if (request.method() == HttpRequest::kPost && path == "/api/v2/upload/sessions") {
            SessionState session;
            const auto sessionStarted = Clock::now();
            const bool created = state.createSession(jsonString(body, "fileName"), jsonString(body, "dirPath"), jsonUint(body, "fileSize"), static_cast<uint32_t>(jsonUint(body, "chunkSize")), session);
            miniKV::utils::logInfo("event=upload_session_create created=" +
                                   std::string(created ? "true" : "false") + " elapsed_us=" +
                                   std::to_string(elapsedMicroseconds(sessionStarted, Clock::now())));
            if (!created) { json(response, 400, jsonError("fileName and positive fileSize are required")); return; }
            json(response, 200, "{\"sessionId\":\"" + session.sessionId +
                "\",\"objectId\":\"" + jsonEscape(session.objectId) +
                "\",\"objectVersion\":" + std::to_string(session.objectVersion) +
                ",\"metadataVersion\":" + std::to_string(session.metadataVersion) +
                ",\"chunkSize\":" + std::to_string(session.chunkSize) +
                ",\"totalChunks\":" + std::to_string(session.totalChunks) + "}"); return;
        }
        if (request.method() == HttpRequest::kGet && beginsWith(path, "/api/v2/upload/sessions/")) {
            SessionState session; const std::string id = pathTail(path, "/api/v2/upload/sessions/");
            const auto sessionLookupStarted = Clock::now();
            const bool found = state.getSession(id, session);
            miniKV::utils::logInfo("event=upload_session_get found=" +
                                   std::string(found ? "true" : "false") + " elapsed_us=" +
                                   std::to_string(elapsedMicroseconds(sessionLookupStarted, Clock::now())));
            if (!found) { json(response, 404, jsonError("session not found")); return; }
            std::ostringstream out; out << "{\"sessionId\":\"" << session.sessionId
                << "\",\"objectId\":\"" << jsonEscape(session.objectId)
                << "\",\"objectVersion\":" << session.objectVersion
                << ",\"metadataVersion\":" << session.metadataVersion
                << ",\"fileSize\":" << session.fileSize << ",\"chunkSize\":" << session.chunkSize
                << ",\"totalChunks\":" << session.totalChunks << ",\"manifestHash\":\""
                << jsonEscape(session.manifestHash) << "\",\"identityScheme\":\""
                << jsonEscape(chunkProtocol.identityScheme) << "\",\"checksumType\":\""
                << jsonEscape(chunkProtocol.checksumType) << "\",\"completed\":["; bool first = true; for (const auto& [index, chunk] : session.completed) { if (!first) out << ','; first = false; out << index; } out << "]}"; json(response, 200, out.str()); return;
        }
        if (request.method() == HttpRequest::kPost && beginsWith(path, "/api/v2/upload/sessions/") && path.size() > 7 && path.rfind("/routes") == path.size() - 7) {
            const std::string id = pathTail(path, "/api/v2/upload/sessions/", "/routes");
            auto requests = parseRouteRequests(body);
            for(auto& routeRequest : requests) {
                routeRequest.identityScheme = chunkProtocol.identityScheme;
                routeRequest.checksumType = chunkProtocol.checksumType;
                if(chunkProtocol.checksumType == "sha256") {
                    routeRequest.checksumDigest = routeRequest.chunkHash;
                }
            }
            std::vector<PlacementPlan> plans;
            const auto routeStarted = Clock::now();
            const RoutePlanStatus routeStatus = state.planRoutes(id, requests, plans);
            miniKV::utils::logInfo("event=route_plan session=" + id + " chunks=" +
                                   std::to_string(requests.size()) + " status=" +
                                   std::to_string(static_cast<int>(routeStatus)) + " elapsed_us=" +
                                   std::to_string(elapsedMicroseconds(routeStarted, Clock::now())));
            if (requests.empty() || plans.size() != requests.size() ||
                routeStatus != RoutePlanStatus::kOk) {
                if (routeStatus == RoutePlanStatus::kNoCapacity) {
                    response->addHeader("Retry-After", "1");
                    json(response, 503, jsonError("no DataNode write capacity available"));
                } else {
                    json(response, 400, jsonError("invalid route request"));
                }
                return;
            }
            SessionState routeSession;
            if (!state.getSession(id, routeSession)) {
                json(response, 409, jsonError("upload session disappeared"));
                return;
            }
            std::ostringstream out;
            out << "{\"routes\":[";
            for (size_t i = 0; i < plans.size(); ++i) {
                if (i) out << ',';
                const auto& plan = plans[i];
                UploadCapability capability;
                capability.schemaVersion = 2;
                capability.sessionId = id;
                capability.chunkIndex = plan.chunkIndex;
                capability.chunkHash = requests[i].chunkHash;
                capability.chunkSize = requests[i].chunkSize;
                capability.identityScheme = plan.identityScheme;
                capability.chunkId = plan.chunkId;
                capability.objectId = routeSession.objectId.empty()
                    ? "upload:" + id : routeSession.objectId;
                capability.objectVersion = plan.objectVersion;
                capability.generation = plan.routeVersion;
                capability.checksumType = plan.checksumType;
                capability.checksumDigest = plan.checksumDigest;
                capability.leaseId = plan.leaseId;
                capability.expiresAt = plan.expiresAt;
                for (const auto& node : plan.chain) capability.chainTargets.push_back(nodeTarget(node.record));
                const std::string uploadToken = issueUploadCapability(capability, clusterSecret);
                if (uploadToken.empty()) { json(response, 500, jsonError("cannot issue upload token")); return; }
                out << "{\"chunkIndex\":" << plan.chunkIndex << ",\"chunkHash\":\"" << requests[i].chunkHash
                    << "\",\"chunkId\":\"" << jsonEscape(plan.chunkId)
                    << "\",\"objectId\":\"" << jsonEscape(routeSession.objectId)
                    << "\",\"objectVersion\":" << plan.objectVersion
                    << ",\"storageIdentity\":\"" << jsonEscape(
                        plan.identityScheme == "opaque-chunk-id" ? plan.chunkId : requests[i].chunkHash)
                    << "\",\"identityScheme\":\"" << plan.identityScheme
                    << "\",\"checksumType\":\"" << plan.checksumType
                    << "\",\"checksumDigest\":\"" << plan.checksumDigest
                    << "\",\"chunkSize\":" << requests[i].chunkSize << ",\"leaseId\":\"" << plan.leaseId
                    << "\",\"routeVersion\":" << plan.routeVersion << ",\"expiresAt\":" << plan.expiresAt
                    << ",\"uploadToken\":\"" << uploadToken << "\"";
                if (!plan.chain.empty()) {
                    const auto& primary = plan.chain.front().record;
                    out << ",\"primaryNodeId\":\"" << jsonEscape(primary.nodeId) << "\",\"primaryAddress\":\""
                        << jsonEscape(primary.address) << "\",\"primaryPort\":" << primary.httpPort;
                }
                out << ",\"chain\":[";
                for (size_t j = 0; j < plan.chain.size(); ++j) {
                    if (j) out << ',';
                    const auto& node = plan.chain[j].record;
                    out << "{\"nodeId\":\"" << jsonEscape(node.nodeId) << "\",\"address\":\""
                        << jsonEscape(node.address) << "\",\"httpPort\":" << node.httpPort << "}";
                }
                out << "]}";
            }
            out << "]}";
            json(response, 200, out.str()); return;
        }
        if (request.method() == HttpRequest::kPost && path == "/internal/v2/chunk-commits") {
            if (!constantTimeEquals(request.getHeader("X-Cluster-Internal-Token"), clusterSecret)) { json(response, 403, jsonError("invalid cluster token")); return; }
            UploadCapability capability;
            if (!verifyUploadCapability(jsonString(body, "uploadToken"), clusterSecret, capability) ||
                capability.sessionId != jsonString(body, "sessionId") ||
                capability.chunkIndex != jsonUint(body, "chunkIndex") ||
                capability.chunkHash != jsonString(body, "chunkHash") ||
                capability.chunkSize != jsonUint(body, "size")) { json(response, 400, jsonError("invalid upload capability")); return; }
            const std::vector<std::string> nodes = split(jsonString(body, "successfulNodes"), ',');
            const auto chunkCommitStarted = Clock::now();
            const CommitChunkStatus status = state.commitChunk(
                jsonString(body, "sessionId"), static_cast<uint32_t>(jsonUint(body, "chunkIndex")),
                jsonString(body, "chunkHash"), jsonUint(body, "size"), nodes, capability.leaseId);
            miniKV::utils::logInfo("event=chunk_commit request_id=" + requestId +
                                   " session=" + capability.sessionId + " index=" +
                                   std::to_string(capability.chunkIndex) + " replicas=" +
                                   std::to_string(nodes.size()) + " status=" +
                                   std::to_string(static_cast<int>(status)) + " elapsed_us=" +
                                   std::to_string(elapsedMicroseconds(chunkCommitStarted, Clock::now())));
            if (status == CommitChunkStatus::kInvalidRequest) {
                json(response, 400, jsonError("invalid chunk commit"));
            } else {
                json(response, 200, status == CommitChunkStatus::kCommitted
                    ? "{\"status\":\"committed\"}"
                    : "{\"status\":\"already_committed\"}");
            }
            return;
        }
        if (request.method() == HttpRequest::kPost && path == "/internal/v2/lease-releases") {
            if (!constantTimeEquals(request.getHeader("X-Cluster-Internal-Token"), clusterSecret)) {
                json(response, 403, jsonError("invalid cluster token"));
                return;
            }
            UploadCapability capability;
            if (!verifyUploadCapability(jsonString(body, "uploadToken"), clusterSecret, capability)) {
                json(response, 400, jsonError("invalid upload capability"));
                return;
            }
            // Releasing an already-committed or expired lease is a successful
            // idempotent cleanup operation.
            state.releaseLease(capability.leaseId);
            json(response, 200, "{\"status\":\"released\"}");
            return;
        }
        if (request.method() == HttpRequest::kPost && beginsWith(path, "/internal/v2/media/jobs/")) {
            if (!constantTimeEquals(request.getHeader("X-Cluster-Internal-Token"), clusterSecret)) {
                json(response, 403, jsonError("invalid cluster token"));
                return;
            }
            const std::string prefix = "/internal/v2/media/jobs/";
            const std::string derivedCreateSuffix = "/derived-uploads";
            const std::string derivedCommitSuffix = "/commit";
            const std::string claimSuffix = "/claim";
            const std::string completeSuffix = "/complete";
            const std::string failSuffix = "/fail";
            if(path.size() > prefix.size() + derivedCreateSuffix.size() &&
               path.rfind(derivedCreateSuffix) == path.size() - derivedCreateSuffix.size()) {
                const std::string jobId = pathTail(path, prefix, derivedCreateSuffix);
                DerivedUploadRequest derived;
                derived.fileName = jsonString(body, "fileName");
                derived.fileSize = jsonUint(body, "fileSize");
                derived.chunkSize = static_cast<uint32_t>(jsonUint(body, "chunkSize"));
                derived.manifestHash = jsonString(body, "manifestHash");
                derived.chunks = parseRouteRequests(body);
                DerivedUploadResult result;
                if(!state.createDerivedUpload(jobId, jsonString(body, "leaseToken"), derived, result)) {
                    json(response, 409, jsonError("derived upload rejected"));
                } else {
                    miniKV::utils::logInfo("event=derived_upload_created job=" + jobId +
                                           " session=" + result.session.sessionId +
                                           " missing=" + std::to_string(result.missingChunks.size()));
                    json(response, 200, derivedUploadJson(result));
                }
                return;
            }
            if(path.size() > prefix.size() + derivedCreateSuffix.size() + 1 +
                derivedCommitSuffix.size() &&
               path.rfind(derivedCommitSuffix) == path.size() - derivedCommitSuffix.size()) {
                const std::string rest = path.substr(prefix.size(),
                    path.size() - prefix.size() - derivedCommitSuffix.size());
                const size_t separator = rest.find(derivedCreateSuffix + "/");
                if(separator != std::string::npos) {
                    const std::string jobId = rest.substr(0, separator);
                    const std::string sessionId = rest.substr(separator + derivedCreateSuffix.size() + 1);
                    FileMeta file;
                    const FileCommitStatus status = state.commitDerivedUpload(
                        jobId, jsonString(body, "leaseToken"), sessionId, file);
                    if(status == FileCommitStatus::kCommitted) {
                        miniKV::utils::logInfo("event=derived_upload_committed job=" + jobId +
                                               " object=" + file.objectId + " file=" + file.fileHash);
                        json(response, 200, "{\"objectId\":\"" + jsonEscape(file.objectId) +
                                            "\",\"objectVersion\":" + std::to_string(file.objectVersion) +
                                            ",\"metadataVersion\":" + std::to_string(file.metadataVersion) +
                                            ",\"fileHash\":\"" + jsonEscape(file.fileHash) + "\"}");
                    } else {
                        json(response, 409, jsonError("derived upload commit rejected"));
                    }
                    return;
                }
            }
            if(path.size() > prefix.size() + claimSuffix.size() &&
               path.rfind(claimSuffix) == path.size() - claimSuffix.size()) {
                miniKV::media::MediaJob job;
                const int64_t leaseSeconds = static_cast<int64_t>(
                    jsonUint(body, "leaseSeconds", 60));
                const std::string jobId = pathTail(path, prefix, claimSuffix);
                if(!state.claimMediaJob(jobId, unixSeconds(), leaseSeconds, job)) {
                    json(response, 409, jsonError("media job is not claimable"));
                } else {
                    miniKV::utils::logInfo("event=media_job_claimed job=" + job.jobId +
                                           " attempt=" + std::to_string(job.attempts));
                    json(response, 200, mediaJobJson(job));
                }
                return;
            }
            if(path.size() > prefix.size() + completeSuffix.size() &&
               path.rfind(completeSuffix) == path.size() - completeSuffix.size()) {
                const std::string jobId = pathTail(path, prefix, completeSuffix);
                if(!state.completeMediaJob(jobId, jsonString(body, "leaseToken"),
                                           jsonString(body, "derivedObjectId"),
                                           jsonString(body, "derivedFileHash"), unixSeconds())) {
                    json(response, 409, jsonError("media completion rejected"));
                } else {
                    miniKV::utils::logInfo("event=media_job_completed job=" + jobId);
                    json(response, 200, "{\"status\":\"completed\"}");
                }
                return;
            }
            if(path.size() > prefix.size() + failSuffix.size() &&
               path.rfind(failSuffix) == path.size() - failSuffix.size()) {
                const std::string jobId = pathTail(path, prefix, failSuffix);
                const bool unsupported = jsonUint(body, "unsupported") != 0;
                const uint64_t retryAfterSeconds = jsonUint(body, "retryAfterSeconds", 60);
                const int64_t nextRetryAt = unsupported ? 0 : unixSeconds() +
                    static_cast<int64_t>(std::min<uint64_t>(retryAfterSeconds, 3600));
                if(!state.failMediaJob(jobId, jsonString(body, "leaseToken"), unsupported,
                                       jsonString(body, "error"), nextRetryAt, unixSeconds())) {
                    json(response, 409, jsonError("media failure rejected"));
                } else {
                    miniKV::utils::logWarn("event=media_job_failed job=" + jobId);
                    json(response, 200, "{\"status\":\"recorded\"}");
                }
                return;
            }
            json(response, 404, jsonError("media job action not found"));
            return;
        }
        if (request.method() == HttpRequest::kPost && beginsWith(path, "/api/v2/upload/sessions/") && path.size() > 7 && path.rfind("/commit") == path.size() - 7) {
            FileMeta file;
            const auto fileCommitStarted = Clock::now();
            const FileCommitStatus status = state.commitFile(pathTail(path, "/api/v2/upload/sessions/", "/commit"), file);
            miniKV::utils::logInfo("event=file_commit status=" + std::to_string(static_cast<int>(status)) +
                                   " object=" + file.objectId + " file=" + file.fileHash + " elapsed_us=" +
                                   std::to_string(elapsedMicroseconds(fileCommitStarted, Clock::now())));
            if (status == FileCommitStatus::kPathConflict) {
                json(response, 409, jsonError("a file already exists at this path"));
                return;
            }
            if (status != FileCommitStatus::kCommitted) {
                json(response, 400, jsonError("all chunks with at least one replica are required"));
                return;
            }
            dispatchPendingAiIndexEvents();
            if(isDerivedImageSourceFileName(file.fileName)) {
                for(const char* profile : kJpegDerivedProfiles) {
                    const auto derived = state.enqueueThumbnail(file.fileHash, profile, unixSeconds());
                    if(derived.publishRequired) enqueueMediaJob(derived.job);
                }
            }
            json(response, 200, "{\"objectId\":\"" + jsonEscape(file.objectId) +
                "\",\"objectVersion\":" + std::to_string(file.objectVersion) +
                ",\"metadataVersion\":" + std::to_string(file.metadataVersion) +
                ",\"fileHash\":\"" + jsonEscape(file.fileHash) + "\",\"state\":\"" +
                fileStateName(file.state) + "\"}");
            return;
        }
        if (request.method() == HttpRequest::kGet && beginsWith(path, "/api/v2/objects/") &&
            path.size() > std::string("/api/v2/objects/").size() + std::string("/manifest").size() &&
            path.rfind("/manifest") == path.size() - std::string("/manifest").size()) {
            const std::string objectId = pathTail(path, "/api/v2/objects/", "/manifest");
            ObjectMeta object;
            ManifestSnapshot snapshot;
            const auto manifestStarted = Clock::now();
            const bool found = state.getObject(objectId, object) && state.buildManifestSnapshot(object.fileHash, snapshot);
            miniKV::utils::logInfo("event=object_manifest object=" + objectId +
                                   " found=" + std::string(found ? "true" : "false") + " elapsed_us=" +
                                   std::to_string(elapsedMicroseconds(manifestStarted, Clock::now())));
            if (!found) {
                json(response, 404, jsonError("object not found or manifest is incomplete"));
            } else {
                // A content record can be linked by multiple logical objects.
                // The object endpoint must report the requested logical identity,
                // not whichever object first created the shared file record.
                snapshot.file.objectId = object.objectId;
                snapshot.file.objectVersion = object.objectVersion;
                snapshot.file.metadataVersion = object.metadataVersion;
                json(response, 200, manifestJson(snapshot));
            }
            return;
        }
        if (request.method() == HttpRequest::kGet && beginsWith(path, "/api/v2/objects/")) {
            ObjectMeta object;
            if (!state.getObject(pathTail(path, "/api/v2/objects/"), object)) {
                json(response, 404, jsonError("object not found"));
            } else {
                json(response, 200, "{\"objectId\":\"" + jsonEscape(object.objectId) + "\",\"parentPath\":\"" +
                    jsonEscape(object.parentPath) + "\",\"name\":\"" + jsonEscape(object.name) +
                    "\",\"objectVersion\":" + std::to_string(object.objectVersion) +
                    ",\"metadataVersion\":" + std::to_string(object.metadataVersion) +
                    ",\"fileHash\":\"" + jsonEscape(object.fileHash) + "\",\"fileSize\":" +
                    std::to_string(object.fileSize) + ",\"state\":\"" + fileStateName(object.state) + "\"}");
            }
            return;
        }
        if (request.method() == HttpRequest::kDelete && beginsWith(path, "/api/v2/objects/")) {
            const DeleteStatus status = state.deleteObject(pathTail(path, "/api/v2/objects/"));
            if(status == DeleteStatus::kDeleted) {
                dispatchPendingDeletes();
                json(response, 200, "{\"status\":\"deleted\"}");
            } else if(status == DeleteStatus::kNotFound) {
                json(response, 404, jsonError("object not found"));
            } else {
                json(response, 400, jsonError("invalid object delete request"));
            }
            return;
        }
        if (request.method() == HttpRequest::kDelete && path == "/api/v2/directories") {
            const DeleteStatus status = state.deleteDirectory(getQueryValue(request.query(), "path"));
            if(status == DeleteStatus::kDeleted) {
                dispatchPendingDeletes();
                json(response, 200, "{\"status\":\"deleted\"}");
            } else if(status == DeleteStatus::kNotFound) {
                json(response, 404, jsonError("directory not found"));
            } else {
                json(response, 400, jsonError("invalid directory delete request"));
            }
            return;
        }
        if (request.method() == HttpRequest::kGet && beginsWith(path, "/api/v2/files/") && path.size() > 9 && path.rfind("/manifest") == path.size() - 9) {
            const auto manifestStarted = Clock::now();
            ManifestSnapshot snapshot;
            const bool found = state.buildManifestSnapshot(pathTail(path, "/api/v2/files/", "/manifest"), snapshot);
            miniKV::utils::logInfo("event=file_manifest found=" +
                                   std::string(found ? "true" : "false") + " elapsed_us=" +
                                   std::to_string(elapsedMicroseconds(manifestStarted, Clock::now())));
            if (!found) {
                json(response, 404, jsonError("file not found or manifest is incomplete"));
                return;
            }
            json(response, 200, manifestJson(snapshot));
            return;
        }
        if (request.method() == HttpRequest::kGet && path == "/api/v2/admin/nodes") {
            std::ostringstream out; out << "{\"nodes\":["; const auto nodes = state.nodes(); for (size_t i = 0; i < nodes.size(); ++i) { if (i) out << ','; out << "{\"nodeId\":\"" << jsonEscape(nodes[i].record.nodeId) << "\",\"address\":\"" << jsonEscape(nodes[i].record.address) << "\",\"httpPort\":" << nodes[i].record.httpPort << ",\"state\":" << static_cast<int>(nodes[i].runtime.state) << ",\"usedBytes\":" << nodes[i].runtime.usedBytes << ",\"freeBytes\":" << nodes[i].runtime.freeBytes << "}"; } out << "]}"; json(response, 200, out.str()); return;
        }
        json(response, 404, jsonError("route not found"));
    });
    loop.runEvery(5000, [&] {
        state.checkNodeTimeouts(unixSeconds());
        dispatchPendingDeletes();
        dispatchPendingMediaJobs();
        dispatchPendingAiIndexEvents();
    });
    loop.runAfter(0, dispatchPendingDeletes);
    loop.runAfter(0, dispatchPendingMediaJobs);
    loop.runAfter(0, dispatchPendingAiIndexEvents);
    server.start();
    miniKV::utils::logInfo("event=gateway_started port=" + std::to_string(port) +
                           " metadata_dir=" + stateDir);
    loop.loop();
}
