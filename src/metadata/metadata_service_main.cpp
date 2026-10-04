#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "http/HttpServer.hpp"
#include "metadata/MetadataService.hpp"
#ifdef MINIKV_METADATA_RAFT_BINARY
#include "metadata/raft/NuRaftAdapters.hpp"
#endif
#include "network/EventLoop.hpp"
#include "utils/ThreadPool.hpp"
#include "utils/Util.hpp"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <vector>

namespace {

using miniKV::http::HttpRequest;
using miniKV::http::HttpResponse;
using namespace miniKV::metadata;
using boost::property_tree::ptree;

std::atomic<bool> gStop{false};
std::atomic<uint64_t> gReadSequence{1};
void signalHandler(int) { gStop.store(true, std::memory_order_relaxed); }

std::string envString(const char* name, std::string fallback)
{ const char* value = std::getenv(name); return value && *value ? value : std::move(fallback); }
uint64_t envUint(const char* name, uint64_t fallback)
{ try { const auto value = envString(name, ""); return value.empty() ? fallback : std::stoull(value); } catch(...) { return fallback; } }
int64_t nowMs()
{ return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count(); }
std::string q(const std::string& value) { return "\"" + miniKV::util::jsonEscape(value) + "\""; }
bool commandDiagnosticsEnabled()
{ const char* value = std::getenv("MINIKV_METADATA_COMMAND_DIAGNOSTICS"); return value && std::string(value) == "1"; }

std::string metricsJson(const MetadataMetricsSnapshot& metrics)
{
    std::ostringstream out;
    out << "{\"proposalCount\":" << metrics.proposalCount
        << ",\"proposalFailureCount\":" << metrics.proposalFailureCount
        << ",\"proposalTotalUs\":" << metrics.proposalTotalUs
        << ",\"raftAppendTotalUs\":" << metrics.raftAppendTotalUs
        << ",\"raftAppendCount\":" << metrics.raftAppendCount
        << ",\"raftLogSyncCount\":" << metrics.raftLogSyncCount
        << ",\"raftLogSyncFailureCount\":" << metrics.raftLogSyncFailureCount
        << ",\"raftLogSyncTotalUs\":" << metrics.raftLogSyncTotalUs
        << ",\"raftLogSyncMinUs\":" << metrics.raftLogSyncMinUs
        << ",\"raftLogSyncMaxUs\":" << metrics.raftLogSyncMaxUs
        << ",\"raftLogWritevCount\":" << metrics.raftLogWritevCount
        << ",\"raftLogWritevRecords\":" << metrics.raftLogWritevRecords
        << ",\"raftLogWritevBytes\":" << metrics.raftLogWritevBytes
        << ",\"raftLogWritevTotalUs\":" << metrics.raftLogWritevTotalUs
        << ",\"raftLogWritevMinUs\":" << metrics.raftLogWritevMinUs
        << ",\"raftLogWritevMaxUs\":" << metrics.raftLogWritevMaxUs
        << ",\"quorumWaitCount\":" << metrics.quorumWaitCount
        << ",\"quorumWaitTotalUs\":" << metrics.quorumWaitTotalUs
        << ",\"stateMachineDecodeTotalUs\":" << metrics.stateMachineDecodeTotalUs
        << ",\"placementCount\":" << metrics.placementCount
        << ",\"placementTotalUs\":" << metrics.placementTotalUs
        << ",\"readIndexCount\":" << metrics.readIndexCount
        << ",\"readIndexFailureCount\":" << metrics.readIndexFailureCount
        << ",\"readIndexTotalUs\":" << metrics.readIndexTotalUs << '}';
    return out.str();
}

void json(HttpResponse* response, int status, const std::string& body)
{
    response->setStatusCode(static_cast<HttpResponse::HttpStatusCode>(status));
    response->setContentType("application/json"); response->setBody(body); response->setCloseConnection(false);
    if(status == 503) response->addHeader("Retry-After", "1");
}

bool parseJson(const std::string& body, ptree& tree, std::string& error)
{
    try { std::istringstream input(body); boost::property_tree::read_json(input, tree); return true; }
    catch(const std::exception& exception) { error = exception.what(); return false; }
}

std::optional<std::string> hexDecode(const std::string& value)
{
    if(value.size() % 2 != 0) return std::nullopt;
    auto nibble = [](char c) -> int {
        if(c >= '0' && c <= '9') return c - '0';
        if(c >= 'a' && c <= 'f') return c - 'a' + 10;
        if(c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    std::string output; output.reserve(value.size() / 2);
    for(size_t i = 0; i < value.size(); i += 2) {
        const int hi = nibble(value[i]); const int lo = nibble(value[i + 1]);
        if(hi < 0 || lo < 0) return std::nullopt;
        output.push_back(static_cast<char>((hi << 4) | lo));
    }
    return output;
}

int statusCode(ApplyStatus status)
{
    switch(status) {
    case ApplyStatus::kOk: case ApplyStatus::kAlreadyApplied: return 200;
    case ApplyStatus::kInvalid: return 400;
    case ApplyStatus::kNotFound: return 404;
    case ApplyStatus::kConflict: case ApplyStatus::kFenced: case ApplyStatus::kCommandIdReuseMismatch: return 409;
    case ApplyStatus::kUnavailable: return 503;
    case ApplyStatus::kUnsupported: return 501;
    }
    return 500;
}

std::string resultJson(const ApplyResult& result)
{
    std::ostringstream out;
    out << "{\"commandId\":" << q(result.commandId) << ",\"status\":" << q(toString(result.status))
        << ",\"metadataVersion\":" << result.metadataVersion << ",\"appliedIndex\":" << result.appliedIndex
        << ",\"appliedTerm\":" << result.appliedTerm << ",\"placementEpoch\":" << result.placementEpoch
        << ",\"nodeEpoch\":" << result.nodeEpoch << ",\"objectId\":" << q(result.objectId)
        << ",\"objectVersion\":" << result.objectVersion << ",\"sessionId\":" << q(result.sessionId)
        << ",\"leaseId\":" << q(result.leaseId) << ",\"message\":" << q(result.message) << '}';
    return out.str();
}

std::string sessionJson(const UploadSessionRecord& session)
{
    std::ostringstream out;
    out << "{\"sessionId\":" << q(session.sessionId) << ",\"objectId\":" << q(session.objectId)
        << ",\"objectVersion\":" << session.objectVersion << ",\"owner\":" << q(session.ownerId)
        << ",\"fileSize\":" << session.fileSize << ",\"chunkSize\":" << session.chunkSize
        << ",\"totalChunks\":" << session.totalChunks << ",\"completedChunks\":" << session.completedChunks
        << ",\"expiresAt\":" << session.expiresAt << ",\"expired\":" << (session.expired ? "true" : "false") << '}';
    return out.str();
}

template <typename Service>
std::string sessionsJson(Service& service)
{
    std::ostringstream out; out << "{\"sessions\":[";
    const auto sessions = service.sessions();
    for(size_t i = 0; i < sessions.size(); ++i) { if(i) out << ','; out << sessionJson(sessions[i]); }
    out << "]}"; return out.str();
}

std::string leaseJson(const LeaseRecord& lease)
{
    std::ostringstream out;
    out << "{\"leaseId\": " << q(lease.leaseId) << ",\"requestKey\": " << q(lease.requestKey)
        << ",\"sessionId\": " << q(lease.sessionId) << ",\"chunkIndex\": " << lease.chunkIndex
        << ",\"routeKey\": " << q(lease.routeKey) << ",\"chunkSize\": " << lease.chunkSize
        << ",\"placementEpoch\": " << lease.placementEpoch << ",\"generation\": " << lease.generation
        << ",\"expiresAt\": " << lease.expiresAt << ",\"state\": " << static_cast<unsigned>(lease.state)
        << ",\"targets\":[";
    for(size_t i = 0; i < lease.targets.size(); ++i) {
        if(i) out << ',';
        out << "{\"nodeId\":" << q(lease.targets[i].nodeId)
            << ",\"nodeEpoch\":" << lease.targets[i].nodeEpoch << '}';
    }
    out << "]}";
    return out.str();
}

std::string chunkJson(const ChunkRouteRecord& chunk)
{
    std::ostringstream out; out << "{\"routeKey\":" << q(chunk.routeKey)
        << ",\"storageIdentity\":" << q(chunk.storageIdentity)
        << ",\"identityScheme\":" << static_cast<unsigned>(chunk.identityScheme)
        << ",\"checksumType\":" << q(toString(chunk.checksumType))
        << ",\"checksumDigest\":" << q(chunk.checksumDigest)
        << ",\"size\":" << chunk.size << ",\"desiredRf\":" << chunk.desiredRf
        << ",\"generation\":" << chunk.generation << ",\"state\":" << static_cast<unsigned>(chunk.state) << '}';
    return out.str();
}

std::string objectJson(const ObjectRecord& object)
{
    std::ostringstream out;
    out << "{\"objectId\":" << q(object.objectId) << ",\"objectVersion\":" << object.objectVersion
        << ",\"metadataVersion\":" << object.metadataVersion << ",\"owner\":" << q(object.ownerId)
        << ",\"parentPath\":" << q(object.parentPath) << ",\"name\":" << q(object.name)
        << ",\"fileSize\":" << object.fileSize << ",\"chunkSize\":" << object.chunkSize
        << ",\"desiredRf\":" << object.desiredRf << ",\"contentHash\":" << q(object.contentHash)
        << ",\"state\":" << static_cast<unsigned>(object.state) << '}';
    return out.str();
}

std::string nodeJson(const NodeRecord& node)
{
    std::ostringstream out;
    out << "{\"nodeId\":" << q(node.nodeId) << ",\"bootId\":" << q(node.bootId)
        << ",\"nodeEpoch\":" << node.nodeEpoch << ",\"address\":" << q(node.address)
        << ",\"dataPort\":" << node.dataPort << ",\"capacityBytes\":" << node.registeredCapacityBytes
        << ",\"health\":" << static_cast<unsigned>(node.health) << ",\"draining\":"
        << (node.draining ? "true" : "false") << ",\"placementEpoch\":" << node.placementEpoch
        << ",\"reservedBytes\":" << node.reservedBytes << ",\"reservedWrites\":" << node.reservedWrites
        << ",\"capabilities\":[";
    for(size_t i = 0; i < node.capabilities.size(); ++i) {
        if(i) out << ',';
        out << q(node.capabilities[i]);
    }
    out << "]}";
    return out.str();
}

std::string nodesJson(const std::vector<NodeRecord>& nodes)
{
    std::ostringstream out; out << "{\"nodes\":[";
    for(size_t i = 0; i < nodes.size(); ++i) {
        if(i) out << ',';
        out << nodeJson(nodes[i]);
    }
    out << "]}"; return out.str();
}

std::string descriptorJson(const ReadDescriptor& descriptor)
{
    std::ostringstream out; out << "{\"object\":" << objectJson(descriptor.object) << ",\"chunks\":[";
    for(size_t i = 0; i < descriptor.chunks.size(); ++i) {
        const auto& chunk = descriptor.chunks[i]; if(i) out << ',';
        out << "{\"index\":" << chunk.index << ",\"routeKey\":" << q(chunk.routeKey)
            << ",\"storageIdentity\":" << q(chunk.storageIdentity) << ",\"checksumType\":" << q(toString(chunk.checksumType))
            << ",\"checksumDigest\":" << q(chunk.checksumDigest) << ",\"size\":" << chunk.size
            << ",\"generation\":" << chunk.generation << ",\"replicas\":[";
        for(size_t j = 0; j < chunk.replicas.size(); ++j) {
            if(j) out << ','; out << "{\"nodeId\":" << q(chunk.replicas[j].nodeId)
                << ",\"nodeEpoch\":" << chunk.replicas[j].nodeEpoch;
            // The descriptor remains a metadata response, but including the
            // registered endpoint lets a Gateway mint a DataNode capability
            // without maintaining a second authoritative node registry.
            out << ",\"address\":\"\",\"dataPort\":0}";
        }
        out << "]}";
    }
    out << "]}"; return out.str();
}

template <typename Service>
std::string catalogJson(Service& service, const std::string& owner, const std::string& path)
{
    std::ostringstream out; out << "{\"directories\":[";
    const auto directories = service.directories(owner, path);
    for(size_t i = 0; i < directories.size(); ++i) {
        if(i) out << ',';
        out << "{\"owner\":" << q(directories[i].ownerId) << ",\"path\":" << q(directories[i].path)
            << ",\"createdAt\":" << directories[i].createdAt << '}';
    }
    out << "],\"objects\":[";
    const auto objects = service.objects(owner, path);
    for(size_t i = 0; i < objects.size(); ++i) { if(i) out << ','; out << objectJson(objects[i]); }
    out << "]}"; return out.str();
}

template <typename Service>
std::string deleteTasksJson(Service& service, const std::string& nodeId)
{
    std::ostringstream out; out << "{\"tasks\":[";
    const auto tasks = service.deleteTasks(nodeId);
    for(size_t i = 0; i < tasks.size(); ++i) {
        if(i) out << ',';
        const auto& task = tasks[i];
        out << "{\"objectId\":" << q(task.objectId) << ",\"objectVersion\":" << task.objectVersion
            << ",\"chunkIndex\":" << task.chunkIndex << ",\"storageIdentity\":" << q(task.storageIdentity)
            << ",\"replicas\":[";
        for(size_t j = 0; j < task.pendingReplicas.size(); ++j) {
            if(j) out << ',';
            out << "{\"nodeId\":" << q(task.pendingReplicas[j].nodeId)
                << ",\"nodeEpoch\":" << task.pendingReplicas[j].nodeEpoch << '}';
        }
        out << "]}";
    }
    out << "]}"; return out.str();
}

bool authorized(const HttpRequest& request, const std::string& secret)
{ return secret.empty() || request.getHeader("X-Cluster-Internal-Token") == secret; }

std::string tail(const std::string& path, const std::string& prefix)
{ return path.rfind(prefix, 0) == 0 ? miniKV::util::urlDecode(path.substr(prefix.size())) : std::string(); }

template <typename Service>
bool readFence(Service& service)
{ return service.linearizableReadBarrier("http-read-" + std::to_string(gReadSequence.fetch_add(1))) .has_value(); }

template <typename Service>
void handle(const HttpRequest& request, HttpResponse* response, Service& service, const std::string& secret)
{
    const std::string& path = request.path();
    if(request.method() == HttpRequest::kGet && path == "/healthz") {
        json(response, 200, "{\"status\":\"ok\",\"component\":\"metadata\"}"); return;
    }
    if(request.method() == HttpRequest::kGet && (path == "/readyz" || path == "/storage-readyz")) {
        const auto status = service.status();
        const bool strict = path == "/storage-readyz";
        const bool ready = status.leaderResolved && (!strict || status.quorumWritable);
        std::ostringstream out; out << "{\"memberId\":" << q(status.memberId) << ",\"role\":" << q(status.role)
            << ",\"leaderId\":" << q(status.leaderId) << ",\"term\":" << status.term
            << ",\"leaderResolved\":" << (status.leaderResolved ? "true" : "false")
            << ",\"quorumWritable\":" << (status.quorumWritable ? "true" : "false") << '}';
        json(response, ready ? 200 : 503, out.str()); return;
    }
    if(request.method() == HttpRequest::kGet && path == "/internal/v4/metadata/status") {
        const auto status = service.status();
        std::ostringstream out; out << "{\"role\":" << q(status.role)
            << ",\"memberId\":" << q(status.memberId) << ",\"leaderId\":" << q(status.leaderId)
            << ",\"term\":" << status.term << ",\"leaderResolved\":"
            << (status.leaderResolved ? "true" : "false") << ",\"quorumWritable\":"
            << (status.quorumWritable ? "true" : "false")
            << ",\"commitIndex\":" << status.commitIndex
            << ",\"lastApplied\":" << status.lastApplied
            << ",\"metrics\":" << metricsJson(service.metrics()) << '}';
        json(response, 200, out.str()); return;
    }
    if(request.method() == HttpRequest::kGet && path == "/internal/v4/metadata/nodes") {
        if(!readFence(service)) { json(response, 503, miniKV::util::jsonError("linearizable read unavailable")); return; }
        json(response, 200, nodesJson(service.nodes())); return;
    }
    if(request.method() == HttpRequest::kGet && path == "/internal/v4/metadata/catalog") {
        if(!readFence(service)) { json(response, 503, miniKV::util::jsonError("linearizable read unavailable")); return; }
        const auto ownerParam = miniKV::util::getQueryValue(request.query(), "owner");
        const auto pathParam = miniKV::util::getQueryValue(request.query(), "path");
        const std::string owner = ownerParam.empty() ? "admin" : miniKV::util::urlDecode(ownerParam);
        const std::string catalogPath = pathParam.empty() ? "/" : miniKV::util::urlDecode(pathParam);
        json(response, 200, catalogJson(service, owner, catalogPath)); return;
    }
    if(request.method() == HttpRequest::kGet && path == "/internal/v4/metadata/sessions") {
        if(!readFence(service)) { json(response, 503, miniKV::util::jsonError("linearizable read unavailable")); return; }
        json(response, 200, sessionsJson(service)); return;
    }
    if(request.method() == HttpRequest::kGet && path == "/internal/v4/metadata/delete-tasks") {
        if(!readFence(service)) { json(response, 503, miniKV::util::jsonError("linearizable read unavailable")); return; }
        json(response, 200, deleteTasksJson(service, miniKV::util::urlDecode(miniKV::util::getQueryValue(request.query(), "nodeId")))); return;
    }
    if(!authorized(request, secret)) { json(response, 403, miniKV::util::jsonError("invalid cluster token")); return; }

    // Route planning used to fetch the session, node registry, each chunk and
    // each lease through separate HTTP requests.  This read-only endpoint
    // performs one linearizable read fence and returns the same authoritative
    // records in one response.  It never chooses placement or mutates state;
    // a missing/inactive lease deliberately returns a non-200 response so the
    // Gateway can fall back to its reservation path.
    if(request.method() == HttpRequest::kPost && path == "/internal/v4/metadata/upload-routes") {
        ptree tree; std::string error;
        if(!parseJson(request.body(), tree, error)) { json(response, 400, miniKV::util::jsonError(error)); return; }
        const std::string sessionId = tree.get<std::string>("sessionId", "");
        if(sessionId.empty()) { json(response, 400, miniKV::util::jsonError("missing sessionId")); return; }
        if(!readFence(service)) { json(response, 503, miniKV::util::jsonError("linearizable read unavailable")); return; }
        const auto session = service.session(sessionId);
        if(!session) { json(response, 404, miniKV::util::jsonError("session not found")); return; }
        std::vector<std::pair<uint32_t, std::string>> requested;
        for(const auto& item : tree.get_child("chunks", ptree{})) {
            const auto& value = item.second;
            requested.emplace_back(value.get<uint32_t>("index", 0), value.get<std::string>("leaseId", ""));
        }
        if(requested.empty()) { json(response, 400, miniKV::util::jsonError("empty route request")); return; }
        std::ostringstream out;
        out << "{\"session\":" << sessionJson(*session) << ",\"nodes\":[";
        const auto nodes = service.nodes();
        for(size_t i = 0; i < nodes.size(); ++i) {
            if(i) out << ',';
            out << nodeJson(nodes[i]);
        }
        out << "],\"routes\":[";
        for(size_t i = 0; i < requested.size(); ++i) {
            const auto& [index, leaseId] = requested[i];
            const auto chunk = service.chunk(session->objectId, index);
            const auto lease = service.lease(leaseId);
            if(!chunk || !lease || lease->state != LeaseState::kActive ||
               lease->sessionId != sessionId || lease->chunkIndex != index) {
                json(response, 409, miniKV::util::jsonError("route lease is missing or inactive")); return;
            }
            if(i) out << ',';
            out << "{\"index\":" << index << ",\"chunk\":" << chunkJson(*chunk)
                << ",\"lease\":" << leaseJson(*lease) << '}';
        }
        out << "]}";
        json(response, 200, out.str()); return;
    }

    if(request.method() == HttpRequest::kGet && path == "/internal/v4/metadata/state-digest") {
        if(!readFence(service)) { json(response, 503, miniKV::util::jsonError("linearizable read unavailable")); return; }
        json(response, 200, "{\"digest\":" + q(service.stateDigest()) + "}"); return;
    }

    if(request.method() == HttpRequest::kPost && path == "/internal/v4/metadata/commands") {
        const auto command = decodeMetadataCommand(request.body());
        if(!command) { json(response, 400, miniKV::util::jsonError("invalid schema-v2 metadata command")); return; }
        const auto result = service.propose(*command); json(response, statusCode(result.status), resultJson(result)); return;
    }
    if(request.method() == HttpRequest::kPost && path == "/internal/v4/metadata/sessions/create-reserve") {
        ptree tree; std::string error;
        if(!parseJson(request.body(), tree, error)) { json(response, 400, miniKV::util::jsonError(error)); return; }
        const auto encoded = hexDecode(tree.get<std::string>("createCommandHex", ""));
        const auto create = encoded ? decodeMetadataCommand(*encoded) : std::nullopt;
        if(!create || create->type != MetadataCommandType::kCreateSession) {
            json(response, 400, miniKV::util::jsonError("invalid create-session command")); return;
        }
        std::vector<ReserveLeaseRequest> requests;
        for(const auto& item : tree.get_child("requests", ptree{})) {
            const auto& value = item.second; ReserveLeaseRequest reserve;
            reserve.commandId = value.get<std::string>("commandId", ""); reserve.actorType = value.get<std::string>("actorType", "gateway");
            reserve.actorId = value.get<std::string>("actorId", ""); reserve.leaseId = value.get<std::string>("leaseId", "");
            reserve.requestKey = value.get<std::string>("requestKey", ""); reserve.sessionId = value.get<std::string>("sessionId", "");
            reserve.chunkIndex = value.get<uint32_t>("chunkIndex", 0); reserve.routeKey = value.get<std::string>("routeKey", "");
            reserve.chunkSize = value.get<uint64_t>("chunkSize", 0); reserve.generation = value.get<uint64_t>("generation", 0);
            reserve.desiredRf = value.get<uint32_t>("desiredRf", 2); reserve.expiresAt = value.get<int64_t>("expiresAt", 0);
            reserve.nowMs = value.get<int64_t>("observedAt", nowMs()); requests.push_back(std::move(reserve));
        }
        if(requests.empty()) { json(response, 400, miniKV::util::jsonError("empty lease batch")); return; }
        const auto result = service.createSessionAndReserve(*create, requests);
        std::string responseBody = resultJson(result);
        // The mutation has already been majority-committed and applied before
        // this handler returns.  Include the just-created records in the same
        // response so a Gateway does not immediately perform two more
        // linearizable point reads on the Raft hot path.
        if(result.status == ApplyStatus::kOk || result.status == ApplyStatus::kAlreadyApplied) {
            const auto* payload = std::get_if<CreateSessionPayload>(&create->payload);
            if(payload) {
                const auto session = service.session(payload->sessionId);
                const auto object = service.object(payload->objectId);
                if(session && object) {
                    responseBody.pop_back();
                    responseBody += ",\"session\":" + sessionJson(*session) +
                                    ",\"object\":" + objectJson(*object) + '}';
                }
            }
        }
        json(response, statusCode(result.status), responseBody); return;
    }
    if(request.method() == HttpRequest::kPost && path == "/internal/v4/metadata/commands/batch") {
        const auto commands = decodeMetadataCommandBatch(request.body());
        if(!commands) { json(response, 400, miniKV::util::jsonError("invalid metadata command batch")); return; }
        const auto result = service.proposeBatch(*commands); json(response, statusCode(result.status), resultJson(result)); return;
    }
    if(request.method() == HttpRequest::kPost && path == "/internal/v4/metadata/chunks/commit") {
        ptree tree; std::string error;
        if(!parseJson(request.body(), tree, error)) { json(response, 400, miniKV::util::jsonError(error)); return; }
        const std::string sessionId = tree.get<std::string>("sessionId", "");
        const std::string leaseId = tree.get<std::string>("leaseId", "");
        const uint32_t index = tree.get<uint32_t>("chunkIndex", 0);
        const uint64_t size = tree.get<uint64_t>("size", 0);
        std::vector<std::string> successfulNodes;
        for(const auto& item : tree.get_child("successfulNodes", ptree{}))
            successfulNodes.push_back(item.second.get_value<std::string>());
        if(sessionId.empty() || leaseId.empty() || size == 0 || successfulNodes.empty()) {
            json(response, 400, miniKV::util::jsonError("invalid chunk commit request")); return;
        }
        const auto started = std::chrono::steady_clock::now();
        const auto result = service.commitChunk(sessionId, index, size, successfulNodes, leaseId);
        const auto elapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - started).count());
        if(commandDiagnosticsEnabled()) {
            miniKV::utils::logInfo("event=metadata_chunk_commit session=" + sessionId +
                " chunk_index=" + std::to_string(index) + " lease_id=" + leaseId +
                " command_id=" + result.commandId + " successful_nodes=" +
                std::to_string(successfulNodes.size()) + " http_handler_us=" +
                std::to_string(elapsed) + " apply_status=" +
                std::to_string(static_cast<unsigned>(result.status)));
        }
        json(response, statusCode(result.status), resultJson(result)); return;
    }
    if(request.method() == HttpRequest::kPost && path == "/internal/v4/metadata/heartbeats") {
        ptree tree; std::string error;
        if(!parseJson(request.body(), tree, error)) { json(response, 400, miniKV::util::jsonError(error)); return; }
        NodeHeartbeat heartbeat; heartbeat.nodeId = tree.get<std::string>("nodeId", "");
        heartbeat.nodeEpoch = tree.get<uint64_t>("nodeEpoch", 0); heartbeat.freeBytes = tree.get<uint64_t>("freeBytes", 0);
        heartbeat.activeUploads = tree.get<uint32_t>("activeUploads", 0); heartbeat.activeDownloads = tree.get<uint32_t>("activeDownloads", 0);
        heartbeat.queueDepth = tree.get<uint32_t>("queueDepth", 0); heartbeat.diskPauseMs = tree.get<uint64_t>("diskPauseMs", 0);
        heartbeat.eventLoopLagUs = tree.get<uint64_t>("eventLoopLagUs", 0); heartbeat.observedAtMs = tree.get<int64_t>("observedAt", nowMs());
        if(!service.heartbeat(heartbeat, &error)) {
            // A frontend may have selected a follower.  Surface that as a
            // retryable control-plane unavailability, while preserving 409
            // for a genuinely stale epoch/timestamp heartbeat.
            const bool retryable = error.find("not metadata leader") != std::string::npos;
            json(response, retryable ? 503 : 409, miniKV::util::jsonError(error)); return;
        }
        json(response, 200, "{\"status\":\"accepted\",\"durableMutation\":false}"); return;
    }
    if(request.method() == HttpRequest::kPost && path == "/internal/v4/metadata/leases/reserve") {
        ptree tree; std::string error;
        if(!parseJson(request.body(), tree, error)) { json(response, 400, miniKV::util::jsonError(error)); return; }
        ReserveLeaseRequest reserve; reserve.commandId = tree.get<std::string>("commandId", "");
        reserve.actorType = tree.get<std::string>("actorType", "gateway"); reserve.actorId = tree.get<std::string>("actorId", "");
        reserve.leaseId = tree.get<std::string>("leaseId", ""); reserve.requestKey = tree.get<std::string>("requestKey", "");
        reserve.sessionId = tree.get<std::string>("sessionId", ""); reserve.chunkIndex = tree.get<uint32_t>("chunkIndex", 0);
        reserve.routeKey = tree.get<std::string>("routeKey", ""); reserve.chunkSize = tree.get<uint64_t>("chunkSize", 0);
        reserve.generation = tree.get<uint64_t>("generation", 0); reserve.desiredRf = tree.get<uint32_t>("desiredRf", 2);
        reserve.expiresAt = tree.get<int64_t>("expiresAt", 0); reserve.nowMs = tree.get<int64_t>("observedAt", nowMs());
        const auto result = service.reserveLease(reserve); json(response, statusCode(result.status), resultJson(result)); return;
    }
    if(request.method() == HttpRequest::kPost && path == "/internal/v4/metadata/leases/reserve-batch") {
        ptree tree; std::string error;
        if(!parseJson(request.body(), tree, error)) { json(response, 400, miniKV::util::jsonError(error)); return; }
        std::vector<ReserveLeaseRequest> requests;
        for(const auto& item : tree.get_child("requests", ptree{})) {
            const auto& value = item.second;
            ReserveLeaseRequest reserve;
            reserve.commandId = value.get<std::string>("commandId", "");
            reserve.actorType = value.get<std::string>("actorType", "gateway");
            reserve.actorId = value.get<std::string>("actorId", "");
            reserve.leaseId = value.get<std::string>("leaseId", "");
            reserve.requestKey = value.get<std::string>("requestKey", "");
            reserve.sessionId = value.get<std::string>("sessionId", "");
            reserve.chunkIndex = value.get<uint32_t>("chunkIndex", 0);
            reserve.routeKey = value.get<std::string>("routeKey", "");
            reserve.chunkSize = value.get<uint64_t>("chunkSize", 0);
            reserve.generation = value.get<uint64_t>("generation", 0);
            reserve.desiredRf = value.get<uint32_t>("desiredRf", 2);
            reserve.expiresAt = value.get<int64_t>("expiresAt", 0);
            reserve.nowMs = value.get<int64_t>("observedAt", nowMs());
            requests.push_back(std::move(reserve));
        }
        if(requests.empty()) { json(response, 400, miniKV::util::jsonError("empty lease batch")); return; }
        const auto result = service.reserveLeaseBatch(requests); json(response, statusCode(result.status), resultJson(result)); return;
    }

    const std::string sessionId = tail(path, "/internal/v4/metadata/sessions/");
    if(request.method() == HttpRequest::kGet && !sessionId.empty()) {
        if(!readFence(service)) { json(response, 503, miniKV::util::jsonError("linearizable read unavailable")); return; }
        const auto value = service.session(sessionId); if(!value) { json(response, 404, miniKV::util::jsonError("session not found")); return; }
        json(response, 200, sessionJson(*value)); return;
    }
    const std::string leaseId = tail(path, "/internal/v4/metadata/leases/");
    if(request.method() == HttpRequest::kGet && !leaseId.empty()) {
        if(!readFence(service)) { json(response, 503, miniKV::util::jsonError("linearizable read unavailable")); return; }
        const auto value = service.lease(leaseId); if(!value) { json(response, 404, miniKV::util::jsonError("lease not found")); return; }
        json(response, 200, leaseJson(*value)); return;
    }
    const std::string objectId = tail(path, "/internal/v4/metadata/objects/");
    const std::string chunkPrefix = "/internal/v4/metadata/objects/";
    if(request.method() == HttpRequest::kGet && path.rfind(chunkPrefix, 0) == 0) {
        const auto marker = path.find("/chunks/", chunkPrefix.size());
        if(marker != std::string::npos) {
            const std::string id = miniKV::util::urlDecode(path.substr(chunkPrefix.size(), marker - chunkPrefix.size()));
            try {
                const uint32_t index = static_cast<uint32_t>(std::stoul(path.substr(marker + 8)));
                if(!readFence(service)) { json(response, 503, miniKV::util::jsonError("linearizable read unavailable")); return; }
                const auto value = service.chunk(id, index); if(!value) { json(response, 404, miniKV::util::jsonError("chunk not found")); return; }
                json(response, 200, chunkJson(*value)); return;
            } catch(...) { json(response, 400, miniKV::util::jsonError("invalid chunk index")); return; }
        }
    }
    if(request.method() == HttpRequest::kGet && !objectId.empty()) {
        if(!readFence(service)) { json(response, 503, miniKV::util::jsonError("linearizable read unavailable")); return; }
        const auto value = service.object(objectId); if(!value) { json(response, 404, miniKV::util::jsonError("object not found")); return; }
        json(response, 200, objectJson(*value)); return;
    }
    const std::string descriptorId = tail(path, "/internal/v4/metadata/read-descriptor/");
    if(request.method() == HttpRequest::kGet && !descriptorId.empty()) {
        uint64_t version = 0; try { version = std::stoull(miniKV::util::getQueryValue(request.query(), "version")); } catch(...) {}
        if(!readFence(service)) { json(response, 503, miniKV::util::jsonError("linearizable read unavailable")); return; }
        const auto value = service.readDescriptor(descriptorId, version);
        if(!value) { json(response, 404, miniKV::util::jsonError("committed object version not found")); return; }
        json(response, 200, descriptorJson(*value)); return;
    }
    json(response, 404, miniKV::util::jsonError("route not found"));
}

template <typename Service>
int runHttpService(Service& service, int port, size_t threads,
                   const std::string& secret, const std::string& mode,
                   const std::string& directory)
{
    std::signal(SIGINT, signalHandler); std::signal(SIGTERM, signalHandler);
    miniKV::network::EventLoop loop; miniKV::utils::ThreadPool workers(threads);
    miniKV::http::HttpServer server(&loop, &workers, port); server.setThreadNum(threads);
    server.setHttpCallback([&](const HttpRequest& request, HttpResponse* response,
                               const miniKV::network::TcpConnectionPtr&, const miniKV::http::DeferredResponse::Ptr&) {
        handle(request, response, service, secret);
    });
    server.start(); loop.runEvery(100, [&] { if(gStop.load(std::memory_order_relaxed)) loop.quit(); });
    std::cout << "metadata_service_started mode=" << mode << " port=" << port << " directory=" << directory << '\n';
    loop.loop(); return 0;
}

#ifdef MINIKV_METADATA_RAFT_BINARY
std::vector<miniKV::metadata::raft::StaticMember> parseMembers(const std::string& text)
{
    std::vector<miniKV::metadata::raft::StaticMember> members; std::stringstream input(text); std::string item;
    while(std::getline(input, item, ',')) {
        const auto separator = item.find('@'); if(separator == std::string::npos) continue;
        try { members.push_back({std::stoi(item.substr(0, separator)), item.substr(separator + 1)}); } catch(...) {}
    }
    return members;
}
#endif

} // namespace

int main(int argc, char** argv)
{
    const int port = argc > 1 ? std::stoi(argv[1]) : static_cast<int>(envUint("MINIKV_METADATA_PORT", 18101));
    const std::string directory = argc > 2 ? argv[2] : envString("MINIKV_METADATA_DIR", "data/metadata-service");
    const std::string secret = envString("MINIKV_V2_CLUSTER_SECRET", "");
    const size_t threads = static_cast<size_t>(envUint("MINIKV_METADATA_IO_THREADS", 2));
    if(port <= 0 || port > 65535 || threads == 0) { std::cerr << "invalid metadata service configuration\n"; return 2; }
#ifdef MINIKV_METADATA_RAFT_BINARY
    const int memberId = static_cast<int>(envUint("MINIKV_METADATA_MEMBER_ID", 1));
    const int raftPort = static_cast<int>(envUint("MINIKV_METADATA_RAFT_PORT", 18001));
    const std::string walDirectory = envString("MINIKV_METADATA_WAL_DIR", "");
    auto members = parseMembers(envString("MINIKV_METADATA_RAFT_MEMBERS",
        "1@127.0.0.1:18001,2@127.0.0.1:18002,3@127.0.0.1:18003"));
    if(members.size() != 3) { std::cerr << "MINIKV_METADATA_RAFT_MEMBERS must contain exactly three static members\n"; return 2; }
    miniKV::metadata::raft::NuRaftMetadataService service(memberId, raftPort, std::move(members), directory, walDirectory);
    std::string error; if(!service.start(&error)) { std::cerr << "cannot start metadata Raft: " << error << '\n'; return 1; }
    return runHttpService(service, port, threads, secret, "raft", directory);
#else
    MetadataService service(directory); ConsensusStatus status;
    status.memberId = envString("MINIKV_METADATA_MEMBER_ID", "meta-1"); status.leaderId = status.memberId;
    service.setConsensusStatus(status, nowMs());
    return runHttpService(service, port, threads, secret, "standalone", directory);
#endif
}
