#include "metadata/MetadataClient.hpp"

#include "utils/Util.hpp"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <random>
#include <sstream>

namespace miniKV::metadata {
namespace {
using boost::property_tree::ptree;

std::string hexEncode(const std::string& bytes)
{
    static constexpr char digits[] = "0123456789abcdef";
    std::string out; out.reserve(bytes.size() * 2);
    for(unsigned char byte : bytes) { out.push_back(digits[byte >> 4]); out.push_back(digits[byte & 0xf]); }
    return out;
}

bool parseJson(const std::string& body, ptree& tree, std::string* error)
{
    try { std::istringstream input(body); boost::property_tree::read_json(input, tree); return true; }
    catch(const std::exception& e) { if(error) *error = e.what(); return false; }
}

ApplyStatus parseStatus(const std::string& value)
{
    if(value == "OK") return ApplyStatus::kOk;
    if(value == "ALREADY_APPLIED") return ApplyStatus::kAlreadyApplied;
    if(value == "NOT_FOUND") return ApplyStatus::kNotFound;
    if(value == "CONFLICT") return ApplyStatus::kConflict;
    if(value == "FENCED") return ApplyStatus::kFenced;
    if(value == "UNSUPPORTED") return ApplyStatus::kUnsupported;
    if(value == "COMMAND_ID_REUSE_MISMATCH") return ApplyStatus::kCommandIdReuseMismatch;
    if(value == "UNAVAILABLE") return ApplyStatus::kUnavailable;
    return ApplyStatus::kInvalid;
}

ChecksumType checksumType(const std::string& value)
{
    if(value == "crc32c") return ChecksumType::kCrc32c;
    if(value == "sha256") return ChecksumType::kSha256;
    return ChecksumType::kNone;
}

NodeRecord parseNodeTree(const boost::property_tree::ptree& item)
{
    NodeRecord node;
    node.nodeId = item.get<std::string>("nodeId", "");
    node.bootId = item.get<std::string>("bootId", "");
    node.nodeEpoch = item.get<uint64_t>("nodeEpoch", 0);
    node.address = item.get<std::string>("address", "");
    node.dataPort = static_cast<uint16_t>(item.get<uint32_t>("dataPort", 0));
    node.registeredCapacityBytes = item.get<uint64_t>("capacityBytes", 0);
    node.health = static_cast<NodeHealth>(item.get<unsigned>("health", 0));
    node.draining = item.get<bool>("draining", false);
    node.placementEpoch = item.get<uint64_t>("placementEpoch", 0);
    node.reservedBytes = item.get<uint64_t>("reservedBytes", 0);
    node.reservedWrites = item.get<uint32_t>("reservedWrites", 0);
    for(const auto& capability : item.get_child("capabilities", boost::property_tree::ptree{}))
        node.capabilities.push_back(capability.second.get_value<std::string>());
    return node;
}

std::string urlPath(const std::string& prefix, const std::string& value)
{ return prefix + value; }

} // namespace

MetadataClient::MetadataClient(std::vector<client::Endpoint> endpoints,
                               std::string clusterSecret,
                               int timeoutMs)
    : endpoints_(std::move(endpoints)), clusterSecret_(std::move(clusterSecret)), timeoutMs_(timeoutMs) {}

std::vector<client::Endpoint> MetadataClient::parseEndpoints(const std::string& value)
{
    std::vector<client::Endpoint> result; std::stringstream input(value); std::string item;
    while(std::getline(input, item, ',')) {
        if(item.rfind("http://", 0) == 0) item.erase(0, 7);
        if(item.rfind("https://", 0) == 0) item.erase(0, 8);
        const auto colon = item.rfind(':'); if(colon == std::string::npos) continue;
        try { result.push_back({item.substr(0, colon), static_cast<uint16_t>(std::stoul(item.substr(colon + 1)))}); }
        catch(...) {}
    }
    return result;
}

std::string MetadataClient::newCommandId(const std::string& prefix)
{
    static std::atomic<uint64_t> sequence{1};
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    std::ostringstream out; out << prefix << '-' << now << '-' << sequence.fetch_add(1);
    return out.str();
}

bool MetadataClient::request(std::string_view method, std::string_view path, std::string_view body,
                             client::HttpResponse& response, std::string& error, bool retryable) const
{
    if(endpoints_.empty()) { error = "metadata client has no endpoints"; return false; }
    std::vector<client::Endpoint> candidates;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const size_t start = nextEndpoint_++ % endpoints_.size();
        for(size_t i = 0; i < endpoints_.size(); ++i) candidates.push_back(endpoints_[(start + i) % endpoints_.size()]);
    }
    std::string lastError = "metadata request failed";
    for(const auto& endpoint : candidates) {
        std::map<std::string, std::string> headers{{"Content-Type", "application/octet-stream"},
                                                    {"X-Cluster-Internal-Token", clusterSecret_}};
        if(client::httpRequest(endpoint, method, path, headers, body, timeoutMs_, response, lastError)) {
            if(response.status == 503 && retryable) continue;
            error = lastError; return true;
        }
    }
    error = lastError; return false;
}

std::optional<ApplyResult> MetadataClient::parseApplyResult(const std::string& body, std::string* error)
{
    ptree tree; if(!parseJson(body, tree, error)) return std::nullopt;
    ApplyResult result; result.commandId = tree.get<std::string>("commandId", "");
    result.status = parseStatus(tree.get<std::string>("status", "INVALID"));
    result.metadataVersion = tree.get<uint64_t>("metadataVersion", 0); result.appliedIndex = tree.get<uint64_t>("appliedIndex", 0);
    result.appliedTerm = tree.get<uint64_t>("appliedTerm", 0); result.placementEpoch = tree.get<uint64_t>("placementEpoch", 0);
    result.nodeEpoch = tree.get<uint64_t>("nodeEpoch", 0); result.objectId = tree.get<std::string>("objectId", "");
    result.objectVersion = tree.get<uint64_t>("objectVersion", 0); result.sessionId = tree.get<std::string>("sessionId", "");
    result.leaseId = tree.get<std::string>("leaseId", ""); result.message = tree.get<std::string>("message", "");
    return result;
}

ApplyResult MetadataClient::propose(const MetadataCommand& command, std::string* error)
{
    ApplyResult unavailable; unavailable.commandId = command.commandId; unavailable.status = ApplyStatus::kUnavailable;
    const auto encoded = encodeMetadataCommand(command);
    if(!encoded) { unavailable.status = ApplyStatus::kInvalid; unavailable.message = "cannot encode metadata command"; return unavailable; }
    client::HttpResponse response; std::string localError;
    if(!request("POST", "/internal/v4/metadata/commands", *encoded, response, localError, true)) {
        if(error) *error = localError; unavailable.message = localError; return unavailable;
    }
    auto result = parseApplyResult(response.body, &localError);
    if(!result) { if(error) *error = localError; unavailable.message = localError; return unavailable; }
    if(error && result->status == ApplyStatus::kUnavailable) *error = result->message;
    return *result;
}

ApplyResult MetadataClient::proposeBatch(const std::vector<MetadataCommand>& commands, std::string* error)
{
    ApplyResult unavailable;
    if(!commands.empty()) unavailable.commandId = commands.back().commandId;
    unavailable.status = ApplyStatus::kUnavailable;
    const auto encoded = encodeMetadataCommandBatch(commands);
    if(!encoded) { unavailable.status = ApplyStatus::kInvalid; unavailable.message = "cannot encode metadata command batch"; return unavailable; }
    client::HttpResponse response; std::string localError;
    if(!request("POST", "/internal/v4/metadata/commands/batch", *encoded, response, localError, true)) {
        if(error) *error = localError; unavailable.message = localError; return unavailable;
    }
    auto result = parseApplyResult(response.body, &localError);
    if(!result) { if(error) *error = localError; unavailable.message = localError; return unavailable; }
    if(error && result->status == ApplyStatus::kUnavailable) *error = result->message;
    return *result;
}

CreateSessionReservationResult MetadataClient::createSessionAndReserveDetailed(
    const MetadataCommand& create, const std::vector<ReserveLeaseRequest>& requests,
    std::string* error)
{
    CreateSessionReservationResult output;
    output.result.commandId = create.commandId;
    output.result.status = ApplyStatus::kUnavailable;
    const auto encoded = encodeMetadataCommand(create);
    if(!encoded || requests.empty()) {
        output.result.status = ApplyStatus::kInvalid;
        output.result.message = "invalid create/reserve batch";
        return output;
    }
    std::ostringstream body;
    body << "{\"createCommandHex\":\"" << hexEncode(*encoded) << "\",\"requests\":[";
    for(size_t i = 0; i < requests.size(); ++i) {
        if(i) body << ',';
        const auto& r = requests[i];
        body << "{\"commandId\":\"" << miniKV::util::jsonEscape(r.commandId)
             << "\",\"actorType\":\"" << miniKV::util::jsonEscape(r.actorType)
             << "\",\"actorId\":\"" << miniKV::util::jsonEscape(r.actorId)
             << "\",\"leaseId\":\"" << miniKV::util::jsonEscape(r.leaseId)
             << "\",\"requestKey\":\"" << miniKV::util::jsonEscape(r.requestKey)
             << "\",\"sessionId\":\"" << miniKV::util::jsonEscape(r.sessionId)
             << "\",\"chunkIndex\":" << r.chunkIndex << ",\"routeKey\":\""
             << miniKV::util::jsonEscape(r.routeKey) << "\",\"chunkSize\":" << r.chunkSize
             << ",\"generation\":" << r.generation << ",\"desiredRf\":" << r.desiredRf
             << ",\"expiresAt\":" << r.expiresAt << ",\"observedAt\":" << r.nowMs << '}';
    }
    body << "]}";
    client::HttpResponse response; std::string localError;
    if(!request("POST", "/internal/v4/metadata/sessions/create-reserve", body.str(), response, localError, true)) {
        if(error) *error = localError; output.result.message = localError; return output;
    }
    auto result = parseApplyResult(response.body, &localError);
    if(!result) { if(error) *error = localError; output.result.message = localError; return output; }
    output.result = *result;
    if(error && output.result.status == ApplyStatus::kUnavailable) *error = output.result.message;

    // The service serializes the records that were created by this same
    // committed batch.  They are only a latency optimization; if an older
    // metadata service does not provide them, callers can use the existing
    // point-read fallback without changing correctness.
    ptree tree;
    if(parseJson(response.body, tree, &localError)) {
        if(const auto sessionTree = tree.get_child_optional("session"))
            output.session = parseSessionTree(*sessionTree, &localError);
        if(const auto objectTree = tree.get_child_optional("object"))
            output.object = parseObjectTree(*objectTree, &localError);
    }
    return output;
}

ApplyResult MetadataClient::createSessionAndReserve(const MetadataCommand& create,
                                                     const std::vector<ReserveLeaseRequest>& requests,
                                                     std::string* error)
{
    return createSessionAndReserveDetailed(create, requests, error).result;
}

ApplyResult MetadataClient::reserveLease(const ReserveLeaseRequest& requestData, std::string* error)
{
    ApplyResult unavailable; unavailable.commandId = requestData.commandId; unavailable.status = ApplyStatus::kUnavailable;
    std::ostringstream body;
    body << "{\"commandId\":\"" << miniKV::util::jsonEscape(requestData.commandId)
         << "\",\"actorType\":\"" << miniKV::util::jsonEscape(requestData.actorType)
         << "\",\"actorId\":\"" << miniKV::util::jsonEscape(requestData.actorId)
         << "\",\"leaseId\":\"" << miniKV::util::jsonEscape(requestData.leaseId)
         << "\",\"requestKey\":\"" << miniKV::util::jsonEscape(requestData.requestKey)
         << "\",\"sessionId\":\"" << miniKV::util::jsonEscape(requestData.sessionId)
         << "\",\"chunkIndex\":" << requestData.chunkIndex << ",\"routeKey\":\""
         << miniKV::util::jsonEscape(requestData.routeKey) << "\",\"chunkSize\":" << requestData.chunkSize
         << ",\"generation\":" << requestData.generation << ",\"desiredRf\":" << requestData.desiredRf
         << ",\"expiresAt\":" << requestData.expiresAt << ",\"observedAt\":" << requestData.nowMs << '}';
    client::HttpResponse response; std::string localError;
    if(!request("POST", "/internal/v4/metadata/leases/reserve", body.str(), response, localError, true)) {
        if(error) *error = localError; unavailable.message = localError; return unavailable;
    }
    auto result = parseApplyResult(response.body, &localError);
    if(!result) { if(error) *error = localError; unavailable.message = localError; return unavailable; }
    return *result;
}

ApplyResult MetadataClient::reserveLeaseBatch(const std::vector<ReserveLeaseRequest>& requests,
                                               std::string* error)
{
    ApplyResult unavailable;
    if(!requests.empty()) unavailable.commandId = requests.back().commandId;
    unavailable.status = ApplyStatus::kUnavailable;
    if(requests.empty()) { unavailable.status = ApplyStatus::kInvalid; unavailable.message = "empty lease batch"; return unavailable; }
    std::ostringstream body;
    body << "{\"requests\":[";
    for(size_t i = 0; i < requests.size(); ++i) {
        if(i) body << ',';
        const auto& r = requests[i];
        body << "{\"commandId\":\"" << miniKV::util::jsonEscape(r.commandId)
             << "\",\"actorType\":\"" << miniKV::util::jsonEscape(r.actorType)
             << "\",\"actorId\":\"" << miniKV::util::jsonEscape(r.actorId)
             << "\",\"leaseId\":\"" << miniKV::util::jsonEscape(r.leaseId)
             << "\",\"requestKey\":\"" << miniKV::util::jsonEscape(r.requestKey)
             << "\",\"sessionId\":\"" << miniKV::util::jsonEscape(r.sessionId)
             << "\",\"chunkIndex\":" << r.chunkIndex
             << ",\"routeKey\":\"" << miniKV::util::jsonEscape(r.routeKey)
             << "\",\"chunkSize\":" << r.chunkSize << ",\"generation\":" << r.generation
             << ",\"desiredRf\":" << r.desiredRf << ",\"expiresAt\":" << r.expiresAt
             << ",\"observedAt\":" << r.nowMs << '}';
    }
    body << "]}";
    client::HttpResponse response; std::string localError;
    if(!request("POST", "/internal/v4/metadata/leases/reserve-batch", body.str(), response, localError, true)) {
        if(error) *error = localError; unavailable.message = localError; return unavailable;
    }
    auto result = parseApplyResult(response.body, &localError);
    if(!result) { if(error) *error = localError; unavailable.message = localError; return unavailable; }
    if(error && result->status == ApplyStatus::kUnavailable) *error = result->message;
    return *result;
}

ApplyResult MetadataClient::commitChunk(const std::string& sessionId, uint32_t index,
                                        const std::string& chunkHash, uint64_t size,
                                        const std::vector<std::string>& successfulNodes,
                                        const std::string& leaseId,
                                        std::string* error)
{
    (void)chunkHash;
    ApplyResult unavailable; unavailable.status = ApplyStatus::kUnavailable;
    unavailable.sessionId = sessionId; unavailable.leaseId = leaseId;
    if(sessionId.empty() || leaseId.empty() || successfulNodes.empty()) {
        unavailable.status = ApplyStatus::kInvalid;
        unavailable.message = "invalid chunk commit request";
        return unavailable;
    }
    std::ostringstream body;
    body << "{\"sessionId\":\"" << miniKV::util::jsonEscape(sessionId)
         << "\",\"leaseId\":\"" << miniKV::util::jsonEscape(leaseId)
         << "\",\"chunkIndex\":" << index << ",\"size\":" << size
         << ",\"successfulNodes\":[";
    for(size_t i = 0; i < successfulNodes.size(); ++i) {
        if(i) body << ',';
        body << '\"' << miniKV::util::jsonEscape(successfulNodes[i]) << '\"';
    }
    body << "]}";
    client::HttpResponse response; std::string localError;
    if(!request("POST", "/internal/v4/metadata/chunks/commit", body.str(), response, localError, true)) {
        if(error) *error = localError;
        unavailable.message = localError;
        return unavailable;
    }
    auto result = parseApplyResult(response.body, &localError);
    if(!result) {
        if(error) *error = localError.empty() ? response.body : localError;
        unavailable.message = localError.empty() ? response.body : localError;
        return unavailable;
    }
    if(error && result->status == ApplyStatus::kUnavailable) *error = result->message;
    return *result;
}

bool MetadataClient::heartbeat(const NodeHeartbeat& heartbeatData, std::string* error)
{
    std::ostringstream body; body << "{\"nodeId\":\"" << miniKV::util::jsonEscape(heartbeatData.nodeId)
        << "\",\"nodeEpoch\":" << heartbeatData.nodeEpoch << ",\"freeBytes\":" << heartbeatData.freeBytes
        << ",\"activeUploads\":" << heartbeatData.activeUploads << ",\"activeDownloads\":" << heartbeatData.activeDownloads
        << ",\"queueDepth\":" << heartbeatData.queueDepth << ",\"diskPauseMs\":" << heartbeatData.diskPauseMs
        << ",\"eventLoopLagUs\":" << heartbeatData.eventLoopLagUs << ",\"observedAt\":" << heartbeatData.observedAtMs << '}';
    client::HttpResponse response; std::string localError;
    if(!request("POST", "/internal/v4/metadata/heartbeats", body.str(), response, localError, true) || response.status != 200) {
        if(error) *error = localError.empty() ? response.body : localError; return false;
    }
    return true;
}

std::optional<UploadSessionRecord> MetadataClient::parseSessionTree(const ptree& t, std::string* /*error*/)
{
    UploadSessionRecord r;
    r.sessionId=t.get<std::string>("sessionId",""); r.objectId=t.get<std::string>("objectId",""); r.objectVersion=t.get<uint64_t>("objectVersion",0);
    r.ownerId=t.get<std::string>("owner","admin"); r.fileSize=t.get<uint64_t>("fileSize",0); r.chunkSize=t.get<uint32_t>("chunkSize",0);
    r.totalChunks=t.get<uint32_t>("totalChunks",0); r.completedChunks=t.get<uint32_t>("completedChunks",0); r.expiresAt=t.get<int64_t>("expiresAt",0);
    r.expired=t.get<bool>("expired",false);
    return r;
}

std::optional<UploadSessionRecord> MetadataClient::parseSession(const std::string& body, std::string* error)
{
    ptree t; if(!parseJson(body, t, error)) return std::nullopt;
    return parseSessionTree(t, error);
}

std::optional<LeaseRecord> MetadataClient::parseLease(const std::string& body, std::string* error)
{
    ptree t; if(!parseJson(body, t, error)) return std::nullopt; LeaseRecord r;
    r.leaseId=t.get<std::string>("leaseId",""); r.requestKey=t.get<std::string>("requestKey",""); r.sessionId=t.get<std::string>("sessionId","");
    r.chunkIndex=t.get<uint32_t>("chunkIndex",0); r.routeKey=t.get<std::string>("routeKey",""); r.chunkSize=t.get<uint64_t>("chunkSize",0);
    r.placementEpoch=t.get<uint64_t>("placementEpoch",0); r.generation=t.get<uint64_t>("generation",0); r.expiresAt=t.get<int64_t>("expiresAt",0);
    r.state=static_cast<LeaseState>(t.get<unsigned>("state",0));
    for(const auto& item : t.get_child("targets", ptree{})) { LeaseTarget target; target.nodeId=item.second.get<std::string>("nodeId",""); target.nodeEpoch=item.second.get<uint64_t>("nodeEpoch",0); r.targets.push_back(std::move(target)); }
    return r;
}

std::optional<ObjectRecord> MetadataClient::parseObjectTree(const ptree& t, std::string* /*error*/)
{
    ObjectRecord r;
    r.objectId=t.get<std::string>("objectId",""); r.objectVersion=t.get<uint64_t>("objectVersion",0); r.metadataVersion=t.get<uint64_t>("metadataVersion",0);
    r.ownerId=t.get<std::string>("owner","admin"); r.parentPath=t.get<std::string>("parentPath",""); r.name=t.get<std::string>("name","");
    r.fileSize=t.get<uint64_t>("fileSize",0); r.chunkSize=t.get<uint32_t>("chunkSize",0); r.desiredRf=t.get<uint32_t>("desiredRf",2);
    r.contentHash=t.get<std::string>("contentHash",""); r.state=static_cast<ObjectState>(t.get<unsigned>("state",0)); return r;
}

std::optional<ObjectRecord> MetadataClient::parseObject(const std::string& body, std::string* error)
{
    ptree t; if(!parseJson(body, t, error)) return std::nullopt;
    return parseObjectTree(t, error);
}

std::optional<ChunkRouteRecord> parseChunkRecord(const std::string& body, std::string* error)
{
    ptree t; if(!parseJson(body, t, error)) return std::nullopt; ChunkRouteRecord r;
    r.routeKey=t.get<std::string>("routeKey",""); r.storageIdentity=t.get<std::string>("storageIdentity","");
    r.identityScheme=static_cast<IdentityScheme>(t.get<unsigned>("identityScheme",0));
    r.checksumType=checksumType(t.get<std::string>("checksumType","none")); r.checksumDigest=t.get<std::string>("checksumDigest","");
    r.size=t.get<uint64_t>("size",0); r.desiredRf=t.get<uint32_t>("desiredRf",0); r.generation=t.get<uint64_t>("generation",0);
    r.state=static_cast<ChunkState>(t.get<unsigned>("state",0)); return r;
}

std::optional<ReadDescriptor> MetadataClient::parseDescriptor(const std::string& body, std::string* error)
{
    ptree t; if(!parseJson(body, t, error)) return std::nullopt; ReadDescriptor d;
    auto object = t.get_child_optional("object"); if(!object) { if(error) *error="descriptor has no object"; return std::nullopt; }
    d.object.objectId=object->get<std::string>("objectId",""); d.object.objectVersion=object->get<uint64_t>("objectVersion",0);
    d.object.metadataVersion=object->get<uint64_t>("metadataVersion",0); d.object.ownerId=object->get<std::string>("owner","admin");
    d.object.parentPath=object->get<std::string>("parentPath",""); d.object.name=object->get<std::string>("name","");
    d.object.fileSize=object->get<uint64_t>("fileSize",0); d.object.chunkSize=object->get<uint32_t>("chunkSize",0);
    d.object.desiredRf=object->get<uint32_t>("desiredRf",2); d.object.contentHash=object->get<std::string>("contentHash","");
    d.object.state=static_cast<ObjectState>(object->get<unsigned>("state",0));
    for(const auto& item : t.get_child("chunks", ptree{})) {
        ChunkRouteRecord c; c.index=item.second.get<uint32_t>("index",0); c.routeKey=item.second.get<std::string>("routeKey","");
        c.storageIdentity=item.second.get<std::string>("storageIdentity",""); c.size=item.second.get<uint64_t>("size",0);
        c.checksumType=checksumType(item.second.get<std::string>("checksumType","none")); c.checksumDigest=item.second.get<std::string>("checksumDigest","");
        c.generation=item.second.get<uint64_t>("generation",0);
        for(const auto& replica : item.second.get_child("replicas", ptree{})) {
            ReplicaRecord r; r.nodeId=replica.second.get<std::string>("nodeId",""); r.nodeEpoch=replica.second.get<uint64_t>("nodeEpoch",0); c.replicas.push_back(std::move(r));
        }
        d.chunks.push_back(std::move(c));
    }
    return d;
}

std::optional<UploadSessionRecord> MetadataClient::session(const std::string& id, std::string* error) const
{ client::HttpResponse r; std::string e; if(!request("GET", urlPath("/internal/v4/metadata/sessions/", id), {}, r, e, true) || r.status != 200) { if(error) *error=e; return std::nullopt; } return parseSession(r.body,error); }
std::vector<UploadSessionRecord> MetadataClient::sessions(std::string* error) const
{
    client::HttpResponse r; std::string e; std::vector<UploadSessionRecord> result;
    if(!request("GET", "/internal/v4/metadata/sessions", {}, r, e, true) || r.status != 200) { if(error) *error=e; return result; }
    ptree tree; if(!parseJson(r.body, tree, error)) return result;
    for(const auto& item : tree.get_child("sessions", ptree{})) {
        std::ostringstream body; boost::property_tree::write_json(body, item.second, false);
        auto value = parseSession(body.str(), error); if(value) result.push_back(*value);
    }
    return result;
}
std::optional<LeaseRecord> MetadataClient::lease(const std::string& id, std::string* error) const
{ client::HttpResponse r; std::string e; if(!request("GET", urlPath("/internal/v4/metadata/leases/", id), {}, r, e, true) || r.status != 200) { if(error) *error=e; return std::nullopt; } return parseLease(r.body,error); }
std::optional<ObjectRecord> MetadataClient::object(const std::string& id, std::string* error) const
{ client::HttpResponse r; std::string e; if(!request("GET", urlPath("/internal/v4/metadata/objects/", id), {}, r, e, true) || r.status != 200) { if(error) *error=e; return std::nullopt; } return parseObject(r.body,error); }
std::optional<ChunkRouteRecord> MetadataClient::chunk(const std::string& objectId, uint32_t index, std::string* error) const
{ client::HttpResponse r; std::string e; const auto path=urlPath("/internal/v4/metadata/objects/", objectId)+"/chunks/"+std::to_string(index); if(!request("GET", path, {}, r, e, true) || r.status != 200) { if(error) *error=e; return std::nullopt; } return parseChunkRecord(r.body,error); }

std::optional<UploadRouteSnapshot> MetadataClient::uploadRoutes(
    const std::string& sessionId, const std::vector<uint32_t>& chunkIndexes,
    std::string* error) const
{
    if(sessionId.empty() || chunkIndexes.empty()) {
        if(error) *error = "invalid upload route snapshot request";
        return std::nullopt;
    }
    std::ostringstream body;
    body << "{\"sessionId\":\"" << miniKV::util::jsonEscape(sessionId)
         << "\",\"chunks\":[";
    for(size_t i = 0; i < chunkIndexes.size(); ++i) {
        if(i) body << ',';
        body << "{\"index\":" << chunkIndexes[i]
             << ",\"leaseId\":\""
             << miniKV::util::jsonEscape("lease-" + sessionId + "-" + std::to_string(chunkIndexes[i]))
             << "\"}";
    }
    body << "]}";

    client::HttpResponse response; std::string localError;
    if(!request("POST", "/internal/v4/metadata/upload-routes", body.str(), response,
                localError, true) || response.status != 200) {
        if(error) *error = localError.empty() ? response.body : localError;
        return std::nullopt;
    }
    ptree tree;
    if(!parseJson(response.body, tree, &localError)) {
        if(error) *error = localError;
        return std::nullopt;
    }
    UploadRouteSnapshot snapshot;
    const auto sessionTree = tree.get_child_optional("session");
    if(!sessionTree) {
        if(error) *error = "upload route response has no session";
        return std::nullopt;
    }
    std::ostringstream sessionBody;
    boost::property_tree::write_json(sessionBody, *sessionTree, false);
    auto parsedSession = parseSession(sessionBody.str(), &localError);
    if(!parsedSession) {
        if(error) *error = localError;
        return std::nullopt;
    }
    snapshot.session = *parsedSession;

    for(const auto& item : tree.get_child("nodes", ptree{}))
        snapshot.nodes.push_back(parseNodeTree(item.second));
    for(const auto& item : tree.get_child("routes", ptree{})) {
        UploadRouteView view;
        view.index = item.second.get<uint32_t>("index", 0);
        const auto chunkTree = item.second.get_child_optional("chunk");
        const auto leaseTree = item.second.get_child_optional("lease");
        if(!chunkTree || !leaseTree) {
            if(error) *error = "upload route response has incomplete route";
            return std::nullopt;
        }
        std::ostringstream chunkBody;
        boost::property_tree::write_json(chunkBody, *chunkTree, false);
        auto parsedChunk = parseChunkRecord(chunkBody.str(), &localError);
        std::ostringstream leaseBody;
        boost::property_tree::write_json(leaseBody, *leaseTree, false);
        auto parsedLease = parseLease(leaseBody.str(), &localError);
        if(!parsedChunk || !parsedLease) {
            if(error) *error = localError;
            return std::nullopt;
        }
        view.chunk = *parsedChunk;
        view.lease = *parsedLease;
        snapshot.routes.push_back(std::move(view));
    }
    if(snapshot.routes.size() != chunkIndexes.size()) {
        if(error) *error = "upload route response is incomplete";
        return std::nullopt;
    }
    return snapshot;
}

std::vector<NodeRecord> MetadataClient::nodes(std::string* error) const
{
    client::HttpResponse r; std::string e; std::vector<NodeRecord> result;
    if(!request("GET", "/internal/v4/metadata/nodes", {}, r, e, true) || r.status != 200) { if(error) *error=e; return result; }
    ptree tree; if(!parseJson(r.body, tree, error)) return result;
    for(const auto& item : tree.get_child("nodes", ptree{})) {
        result.push_back(parseNodeTree(item.second));
    }
    return result;
}
std::optional<ReadDescriptor> MetadataClient::readDescriptor(const std::string& id, uint64_t version, std::string* error) const
{ client::HttpResponse r; std::string e; const auto path=urlPath("/internal/v4/metadata/read-descriptor/", id)+"?version="+std::to_string(version); if(!request("GET", path, {}, r, e, true) || r.status != 200) { if(error) *error=e; return std::nullopt; } return parseDescriptor(r.body,error); }
std::vector<DirectoryRecord> MetadataClient::directories(const std::string& ownerId,
                                                          const std::string& parentPath,
                                                          std::string* error) const
{
    client::HttpResponse r; std::string e; std::vector<DirectoryRecord> result;
    const auto path = std::string("/internal/v4/metadata/catalog?owner=") + ownerId + "&path=" + parentPath;
    if(!request("GET", path, {}, r, e, true) || r.status != 200) { if(error) *error=e; return result; }
    ptree tree; if(!parseJson(r.body, tree, error)) return result;
    for(const auto& item : tree.get_child("directories", ptree{})) {
        DirectoryRecord directory; directory.ownerId=item.second.get<std::string>("owner", "admin");
        directory.path=item.second.get<std::string>("path", ""); directory.createdAt=item.second.get<int64_t>("createdAt", 0);
        result.push_back(std::move(directory));
    }
    return result;
}

std::vector<ObjectRecord> MetadataClient::objects(const std::string& ownerId,
                                                  const std::string& parentPath,
                                                  std::string* error) const
{
    client::HttpResponse r; std::string e; std::vector<ObjectRecord> result;
    const auto path = std::string("/internal/v4/metadata/catalog?owner=") + ownerId + "&path=" + parentPath;
    if(!request("GET", path, {}, r, e, true) || r.status != 200) { if(error) *error=e; return result; }
    ptree tree; if(!parseJson(r.body, tree, error)) return result;
    for(const auto& item : tree.get_child("objects", ptree{})) {
        std::ostringstream body; boost::property_tree::write_json(body, item.second, false);
        auto objectValue = parseObject(body.str(), error); if(objectValue) result.push_back(*objectValue);
    }
    return result;
}

std::vector<DeleteTaskRecord> MetadataClient::deleteTasks(const std::string& nodeId,
                                                          std::string* error) const
{
    client::HttpResponse r; std::string e; std::vector<DeleteTaskRecord> result;
    const auto path = std::string("/internal/v4/metadata/delete-tasks?nodeId=") + nodeId;
    if(!request("GET", path, {}, r, e, true) || r.status != 200) { if(error) *error=e; return result; }
    ptree tree; if(!parseJson(r.body, tree, error)) return result;
    for(const auto& item : tree.get_child("tasks", ptree{})) {
        DeleteTaskRecord task; task.objectId=item.second.get<std::string>("objectId", "");
        task.objectVersion=item.second.get<uint64_t>("objectVersion", 0); task.chunkIndex=item.second.get<uint32_t>("chunkIndex", 0);
        task.storageIdentity=item.second.get<std::string>("storageIdentity", "");
        for(const auto& replica : item.second.get_child("replicas", ptree{}))
            task.pendingReplicas.push_back({replica.second.get<std::string>("nodeId", ""), replica.second.get<uint64_t>("nodeEpoch", 0)});
        result.push_back(std::move(task));
    }
    return result;
}
bool MetadataClient::readIndex(std::string* error) const
{ client::HttpResponse r; std::string e; if(!request("GET", "/internal/v4/metadata/state-digest", {}, r, e, true) || r.status != 200) { if(error) *error=e; return false; } return true; }
bool MetadataClient::available(std::string* error) const
{ client::HttpResponse r; std::string e; if(!request("GET", "/storage-readyz", {}, r, e, true) || r.status != 200) { if(error) *error=e; return false; } return true; }

} // namespace miniKV::metadata
