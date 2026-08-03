#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "http/HttpServer.hpp"
#include "network/EventLoop.hpp"
#include "utils/ThreadPool.hpp"
#include "gateway/GatewayState.hpp"
#include "utils/Util.hpp"
#include "http/DeferredResponse.hpp"
#include "http/AsyncHttpClient.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <set>
#include <sstream>

using miniKV::http::HttpRequest;
using miniKV::http::HttpResponse;
using miniKV::network::EventLoop;
using miniKV::utils::ThreadPool;
using namespace miniKV::util;
using namespace miniKV::gateway;
namespace {

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

std::vector<ChunkRouteRequest> parseRouteRequests(const std::string& body) {
    std::vector<ChunkRouteRequest> out;
    for (const auto& object : jsonObjectArray(body, "chunks")) {
        ChunkRouteRequest request;
        request.chunkIndex = static_cast<uint32_t>(jsonUint(object, "index", UINT32_MAX));
        request.chunkHash = jsonString(object, "hash");
        request.chunkSize = jsonUint(object, "size");
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
    out << "{\"fileHash\":\"" << jsonEscape(snapshot.file.fileHash)
        << "\",\"fileSize\":" << snapshot.file.fileSize
        << ",\"chunkSize\":" << snapshot.file.chunkSize << ",\"chunks\":[";
    for (size_t i = 0; i < snapshot.routes.size(); ++i) {
        if (i) out << ',';
        const ChunkRoute& route = snapshot.routes[i];
        out << "{\"index\":" << i << ",\"hash\":\"" << jsonEscape(route.chunkHash)
            << "\",\"replicas\":[";
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
            << "\",\"fileSize\":" << file.fileSize << ",\"state\":\""
            << fileStateName(file.state) << "\",\"createdAt\":" << file.createdAt << "}";
    }
    out << "]}";
    return out.str();
}

std::string objectJson(const ObjectMeta& object) {
    return "{\"objectId\":\"" + jsonEscape(object.objectId) + "\",\"parentPath\":\"" +
           jsonEscape(object.parentPath) + "\",\"name\":\"" + jsonEscape(object.name) +
           "\",\"fileHash\":\"" + jsonEscape(object.fileHash) + "\",\"fileSize\":" +
           std::to_string(object.fileSize) + ",\"state\":\"" + fileStateName(object.state) + "\"}";
}

std::string uploadPreflightJson(const UploadPreflightResult& result) {
    std::ostringstream out;
    out << "{\"status\":\"UPLOAD_REQUIRED\",\"sessionId\":\"" << jsonEscape(result.session.sessionId)
        << "\",\"manifestHash\":\"" << jsonEscape(result.session.manifestHash)
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

}  // namespace

int main(int argc, char** argv) {
    const int port = argc > 1 ? std::stoi(argv[1]) : 8081;
    const std::string dataDir = argc > 2 ? argv[2] : "./v2_gateway_data";
    const std::string clusterSecret = configuredSecret(argc, argv);
    if (clusterSecret.empty()) {
        std::cerr << "MINIKV_V2_CLUSTER_SECRET or a command-line clusterSecret is required\n";
        return 2;
    }
    std::filesystem::create_directories(dataDir);
    std::string stateDir = dataDir + "/metadata";
    GatewayState state(stateDir);
    if (!state.open()) { std::cerr << "cannot open Gateway metadata\n"; return 1; }

    ThreadPool workers(4);
    EventLoop loop;
    miniKV::http::HttpServer server(&loop, &workers, port);
    std::set<std::string> deleteRequestsInFlight;
    std::function<void()> dispatchPendingDeletes;
    dispatchPendingDeletes = [&] {
        for(const NodeSnapshot& node : state.nodes()) {
            if(node.runtime.state != NodeLiveState::kOnline) continue;
            for(const DeleteTaskSnapshot& task : state.pendingDeletesForNode(node.record.nodeId)) {
                const std::string requestKey = node.record.nodeId + "\n" + task.chunkHash;
                if(!deleteRequestsInFlight.insert(requestKey).second) continue;

                auto request = miniKV::http::AsyncHttpRequest::create(&loop);
                miniKV::http::AsyncHttpRequestOptions options;
                options.address = node.record.address;
                options.port = node.record.httpPort;
                options.method = "DELETE";
                options.path = "/internal/v2/chunks/" + task.chunkHash;
                options.timeoutMs = 10000;
                options.headers = {{"X-Cluster-Internal-Token", clusterSecret}};
                request->open(std::move(options), [request] {
                    request->finishBody();
                }, [&, requestKey, nodeId = node.record.nodeId, chunkHash = task.chunkHash]
                    (miniKV::http::HttpClientResponse response, std::string error) {
                    deleteRequestsInFlight.erase(requestKey);
                    if(!error.empty() || response.status < 200 || response.status >= 300) {
                        std::cerr << "delete dispatch " << chunkHash << " to " << nodeId
                                  << " failed: "
                                  << (error.empty() ? "HTTP " + std::to_string(response.status) : error)
                                  << '\n';
                        return;
                    }
                    if(!state.acknowledgeDelete(chunkHash, nodeId)) {
                        std::cerr << "delete dispatch " << chunkHash << " to " << nodeId
                                  << " could not persist ACK\n";
                    }
                });
            }
        }
    };
    server.setHttpCallback([&](const HttpRequest& request, HttpResponse* response,
                               const miniKV::network::TcpConnectionPtr&,
                               const miniKV::http::DeferredResponse::Ptr&) {
        const std::string& path = request.path();
        const std::string body = request.body();
        if (request.method() == HttpRequest::kPost && path == "/api/v2/nodes/register") {
            if (!constantTimeEquals(request.getHeader("X-Cluster-Internal-Token"), clusterSecret)) { json(response, 403, jsonError("invalid cluster token")); return; }
            NodeRecord node; node.nodeId = jsonString(body, "nodeId"); node.address = jsonString(body, "address"); node.httpPort = static_cast<uint16_t>(jsonUint(body, "httpPort", 9002)); node.maxStorageBytes = jsonUint(body, "maxStorageBytes"); node.reservedBytes = jsonUint(body, "reservedBytes"); node.maxConcurrentWrites = static_cast<uint32_t>(jsonUint(body, "maxConcurrentWrites", 2)); node.capabilities = {"storage"};
            if (!state.registerNode(node)) json(response, 400, jsonError("invalid node registration")); else json(response, 200, "{\"status\":\"registered\"}");
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
            const PreflightStatus status = state.preflightUpload(preflight, result);
            if (status == PreflightStatus::kPathConflict) {
                json(response, 409, jsonError("a file already exists at this path"));
            } else if (status == PreflightStatus::kContentExists) {
                json(response, 200, "{\"status\":\"CONTENT_EXISTS\",\"object\":" +
                    objectJson(result.object) + "}");
            } else if (status == PreflightStatus::kUploadRequired) {
                json(response, 200, uploadPreflightJson(result));
            } else {
                json(response, 400, jsonError("invalid upload manifest"));
            }
            return;
        }
        if (request.method() == HttpRequest::kPost && path == "/api/v2/upload/sessions") {
            SessionState session;
            if (!state.createSession(jsonString(body, "fileName"), jsonString(body, "dirPath"), jsonUint(body, "fileSize"), static_cast<uint32_t>(jsonUint(body, "chunkSize")), session)) { json(response, 400, jsonError("fileName and positive fileSize are required")); return; }
            json(response, 200, "{\"sessionId\":\"" + session.sessionId + "\",\"chunkSize\":" + std::to_string(session.chunkSize) + ",\"totalChunks\":" + std::to_string(session.totalChunks) + "}"); return;
        }
        if (request.method() == HttpRequest::kGet && beginsWith(path, "/api/v2/upload/sessions/")) {
            SessionState session; const std::string id = pathTail(path, "/api/v2/upload/sessions/");
            if (!state.getSession(id, session)) { json(response, 404, jsonError("session not found")); return; }
            std::ostringstream out; out << "{\"sessionId\":\"" << session.sessionId << "\",\"fileSize\":" << session.fileSize << ",\"chunkSize\":" << session.chunkSize << ",\"totalChunks\":" << session.totalChunks << ",\"manifestHash\":\"" << jsonEscape(session.manifestHash) << "\",\"completed\":["; bool first = true; for (const auto& [index, chunk] : session.completed) { if (!first) out << ','; first = false; out << index; } out << "]}"; json(response, 200, out.str()); return;
        }
        if (request.method() == HttpRequest::kPost && beginsWith(path, "/api/v2/upload/sessions/") && path.size() > 7 && path.rfind("/routes") == path.size() - 7) {
            const std::string id = pathTail(path, "/api/v2/upload/sessions/", "/routes");
            const auto requests = parseRouteRequests(body);
            std::vector<PlacementPlan> plans;
            const RoutePlanStatus routeStatus = state.planRoutes(id, requests, plans);
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
            std::ostringstream out;
            out << "{\"routes\":[";
            for (size_t i = 0; i < plans.size(); ++i) {
                if (i) out << ',';
                const auto& plan = plans[i];
                UploadCapability capability;
                capability.sessionId = id;
                capability.chunkIndex = plan.chunkIndex;
                capability.chunkHash = requests[i].chunkHash;
                capability.chunkSize = requests[i].chunkSize;
                capability.leaseId = plan.leaseId;
                capability.expiresAt = plan.expiresAt;
                for (const auto& node : plan.chain) capability.chainTargets.push_back(nodeTarget(node.record));
                const std::string uploadToken = issueUploadCapability(capability, clusterSecret);
                if (uploadToken.empty()) { json(response, 500, jsonError("cannot issue upload token")); return; }
                out << "{\"chunkIndex\":" << plan.chunkIndex << ",\"chunkHash\":\"" << requests[i].chunkHash
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
            const CommitChunkStatus status = state.commitChunk(
                jsonString(body, "sessionId"), static_cast<uint32_t>(jsonUint(body, "chunkIndex")),
                jsonString(body, "chunkHash"), jsonUint(body, "size"), nodes, capability.leaseId);
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
        if (request.method() == HttpRequest::kPost && beginsWith(path, "/api/v2/upload/sessions/") && path.size() > 7 && path.rfind("/commit") == path.size() - 7) {
            FileMeta file;
            const FileCommitStatus status = state.commitFile(pathTail(path, "/api/v2/upload/sessions/", "/commit"), file);
            if (status == FileCommitStatus::kPathConflict) {
                json(response, 409, jsonError("a file already exists at this path"));
                return;
            }
            if (status != FileCommitStatus::kCommitted) {
                json(response, 400, jsonError("all chunks with at least one replica are required"));
                return;
            }
            json(response, 200, "{\"objectId\":\"" + jsonEscape(file.objectId) + "\",\"fileHash\":\"" +
                jsonEscape(file.fileHash) + "\",\"state\":\"" + fileStateName(file.state) + "\"}");
            return;
        }
        if (request.method() == HttpRequest::kGet && beginsWith(path, "/api/v2/objects/") &&
            path.size() > std::string("/api/v2/objects/").size() + std::string("/manifest").size() &&
            path.rfind("/manifest") == path.size() - std::string("/manifest").size()) {
            const std::string objectId = pathTail(path, "/api/v2/objects/", "/manifest");
            ObjectMeta object;
            ManifestSnapshot snapshot;
            if (!state.getObject(objectId, object) || !state.buildManifestSnapshot(object.fileHash, snapshot)) {
                json(response, 404, jsonError("object not found or manifest is incomplete"));
            } else {
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
                    "\",\"fileHash\":\"" + jsonEscape(object.fileHash) + "\",\"fileSize\":" +
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
            ManifestSnapshot snapshot;
            if (!state.buildManifestSnapshot(pathTail(path, "/api/v2/files/", "/manifest"), snapshot)) {
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
    });
    loop.runAfter(0, dispatchPendingDeletes);
    server.start();
    std::cout << "V2 Gateway listening on :" << port << "\n";
    loop.loop();
}
