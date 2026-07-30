#include "gateway/GatewayState.hpp"
#include "utils/Util.hpp"


#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <leveldb/db.h>
#include <leveldb/iterator.h>
#include <leveldb/options.h>
#include <memory>
#include <mutex>
#include <ratio>
#include <sstream>
#include <streambuf>
#include <string>
#include <utility>
#include <vector>


namespace miniKV {
namespace gateway {

using namespace util;

//通用工具
std::string join(const std::vector<std::string>& values, char separator) {
    std::ostringstream oss;
    for(size_t i = 0; i < values.size(); i++)
    {
        if(i != 0) oss << separator;
        oss << values[i];
    }
    return oss.str();
}

bool hasCapability(const NodeRecord& node, const std::string& wanted)
{   
    //节点是否有运行存储的能力
    return std::find(node.capabilities.begin(), node.capabilities.end(), wanted) != node.capabilities.end();
}

std::string routeRequestKey(const std::string& sessionId,
                            const ChunkRouteRequest& request)
{
    return sessionId + '\n' + std::to_string(request.chunkIndex) + '\n' +
           request.chunkHash + '\n' + std::to_string(request.chunkSize);
}


/*
struct NodeRecord { //节点静态数据
    std::string nodeId;
    std::string address;
    uint16_t httpPort = 9002;
    uint64_t maxStorageBytes = 0;
    uint64_t reservedBytes = 0;
    uint32_t maxConcurrentWrites = 2;
    std::vector<std::string> capabilities;
};

*/
std::string nodeValue(const NodeRecord& node) {
    std::vector<std::string> capabilities;
    for (const auto& capability : node.capabilities) capabilities.push_back(hexEncode(capability));
    return hexEncode(node.nodeId) + "|" + hexEncode(node.address) + "|" + std::to_string(node.httpPort) + "|" +
           std::to_string(node.maxStorageBytes) + "|" + std::to_string(node.reservedBytes) + "|" +
           std::to_string(node.maxConcurrentWrites) + "|" + join(capabilities, ',');
}


bool parseNode(const std::string& value, NodeRecord& node)
{
    const auto f = split(value, '|');
    
    if(f.size() != 7) return false;
    try{
        if(!hexDecode(f[0], node.nodeId) || !hexDecode(f[1], node.address)) return false;
        node.httpPort = static_cast<uint16_t>(std::stoul(f[2]));
        node.maxStorageBytes = std::stoull(f[3]);
        node.reservedBytes = std::stoull(f[4]);
        node.maxConcurrentWrites = static_cast<uint32_t>(std::stoul(f[5]));
        for(const auto& item : split(f[6], ','))
        {
            if(!item.empty())
            {
                std::string decoded;
                if(!hexDecode(item, decoded)) return false;
                node.capabilities.push_back(decoded);
            }
        }
        return true;
    } catch(...) { return false; }
}

/*
struct ChunkRoute { //每个chunk的真实副本管理，写入真实的数据
    std::string chunkHash;
    uint64_t size = 0;
    uint32_t desiredReplicas = 2; //期望副本数
    std::vector<std::string> replicas;
    int64_t  updateAt = 0;
};
对于sha256之后容易是有这些字符与|冲突的，所以都要转换成这个16进制
*/
std::string routeValue(const ChunkRoute& route)
{
    std::vector<std::string> replicas;
    
    for(const auto& node : route.replicas)
    {
        replicas.push_back(hexEncode(node));
    }

    return hexEncode(route.chunkHash) + '|' + std::to_string(route.size) 
            + '|' + std::to_string(route.desiredReplicas) + '|' +  std::to_string(route.updateAt) + '|'
            + join(replicas, ',');
}

bool parseRoute(const std::string& value, ChunkRoute& route)
{
    const auto f = split(value, '|');
    if(f.size() != 5) return false;
    try{
        if(!hexDecode(f[0], route.chunkHash)) return false;
        route.size = std::stoull(f[1]);
        route.desiredReplicas = static_cast<uint32_t>(std::stoul(f[2]));
        route.updateAt = std::stoll(f[3]);
        for(const auto& item : split(f[4], ','))
        {
            if(!item.empty())
            {
                std::string node;
                if(!hexDecode(item, node)) return false;
                route.replicas.push_back(node);
            }
        }
        return true;
    }catch(...) {
        return false;
    }
}

/*
struct FileMeta {
    std::string fileHash;
    std::string ownerId = "admin";
    std::string fileName;
    std::string dirPath;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    std::vector<std::string> chunkHashes;
    FileState state = FileState::kProtecting;
    int64_t createAt = 0;
};
*/
std::string fileValue(const FileMeta& file)
{
    std::vector<std::string> hashes;
    for(const auto & hash : file.chunkHashes)
    {
        hashes.push_back(hexEncode(hash));
    }
    return   hexEncode(file.fileHash) + "|" + hexEncode(file.ownerId) + "|" + hexEncode(file.fileName) + "|" + hexEncode(file.dirPath) + "|" +
           std::to_string(file.fileSize) + "|" + std::to_string(file.chunkSize) + "|" + std::to_string(static_cast<int>(file.state)) + "|" + std::to_string(file.createdAt) + "|" + join(hashes, ',');

}


bool parseFile(const std::string& value, FileMeta& file) {
    const auto f = split(value, '|'); if (f.size() != 9) return false;
    try {
        if (!hexDecode(f[0], file.fileHash) || !hexDecode(f[1], file.ownerId) || !hexDecode(f[2], file.fileName) || !hexDecode(f[3], file.dirPath)) return false;
        file.fileSize = std::stoull(f[4]); file.chunkSize = static_cast<uint32_t>(std::stoul(f[5])); file.state = static_cast<FileState>(std::stoi(f[6])); file.createdAt = std::stoll(f[7]);
        for (const auto& item : split(f[8], ',')) { if (!item.empty()) { std::string hash; if (!hexDecode(item, hash)) return false; file.chunkHashes.push_back(hash); } }
        return true;
    } catch (...) { return false; }
}


/*
struct SessionState {
    std::string sessionId;
    std::string ownerId = "admin";
    std::string fileName;
    std::string dirPath;
    uint64_t fileSize  = 0;
    uint32_t chunkSize = 4 * 1024 * 1024;
    uint32_t totalChunks = 0;
    std::map<uint32_t, CompletedChunk>   completed;
    int64_t createdAt = 0;
    int64_t lastActivityAt = 0;
};
*/

std::string sessionValue(const SessionState& session) {
    std::vector<std::string> chunks;
    for (const auto& [index, chunk] : session.completed) chunks.push_back(std::to_string(index) + "," + hexEncode(chunk.chunkHash) + "," + std::to_string(chunk.size));
    return hexEncode(session.sessionId) + "|" + hexEncode(session.ownerId) + "|" + hexEncode(session.fileName) + "|" +
           hexEncode(session.dirPath) + "|" + std::to_string(session.fileSize) + "|" + std::to_string(session.chunkSize) + "|" +
           std::to_string(session.totalChunks) + "|" + std::to_string(session.createdAt) + "|" + std::to_string(session.lastActivityAt) + "|" + join(chunks, ';');
}
bool parseSession(const std::string& value, SessionState& session) {
    const auto f = split(value, '|'); 
    if (f.size() != 10) return false;
    try {
        if (!hexDecode(f[0], session.sessionId) || !hexDecode(f[1], session.ownerId) || !hexDecode(f[2], session.fileName) || !hexDecode(f[3], session.dirPath)) return false;
        session.fileSize = std::stoull(f[4]); session.chunkSize = static_cast<uint32_t>(std::stoul(f[5])); session.totalChunks = static_cast<uint32_t>(std::stoul(f[6]));
        session.createdAt = std::stoll(f[7]); session.lastActivityAt = std::stoll(f[8]);
        for (const auto& item : split(f[9], ';')) { if (item.empty()) continue; const auto chunk = split(item, ','); if (chunk.size() != 3) return false; CompletedChunk completed; completed.index = static_cast<uint32_t>(std::stoul(chunk[0])); if (!hexDecode(chunk[1], completed.chunkHash)) return false; completed.size = std::stoull(chunk[2]); session.completed[completed.index] = completed; }
        return true;
    } catch (...) { return false; }
}

bool GatewayState::persistSessionLocked(const SessionState& session)
{
    return db_->Put(leveldb::WriteOptions(), "s:" + session.sessionId, sessionValue(session)).ok();
}
bool GatewayState::persistFileLocked(const FileMeta& file)
{
    return db_->Put(leveldb::WriteOptions(), "f:" + file.fileHash, fileValue(file)).ok();
}
bool GatewayState::persistRouteLocked(const ChunkRoute& route)
{
    return db_->Put(leveldb::WriteOptions(), "c:" + route.chunkHash, routeValue(route)).ok();
}
bool GatewayState::loadSessionsLocked()
{
    auto it =  std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));

    for(it->Seek("s:"); it->Valid() && it->value().ToString().rfind("s:", 0) == 0; it->Next())
    {
        SessionState session;
        if(parseSession(it->value().ToString(), session))
        {
            sessions_[session.sessionId] = session;
        }
    }
    return it->status().ok();
}
bool GatewayState::loadFilesLocked()
{
    auto it =  std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));

    for(it->Seek("f:"); it->Valid() && it->value().ToString().rfind("f:", 0) == 0; it->Next())
    {
        FileMeta file;
        if(parseFile(it->value().ToString(), file))
        {
            files_[file.fileHash] = file;
        }
    }
    return it->status().ok();
}
bool GatewayState::loadRoutesLocked()
{
    auto it =  std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));

    for(it->Seek("c:"); it->Valid() && it->value().ToString().rfind("c:", 0) == 0; it->Next())
    {
        ChunkRoute route;
        if(parseRoute(it->value().ToString(), route))
        {
            routes_[route.chunkHash] = route;
        }
    }
    return it->status().ok();
}
bool GatewayState::loadNodesLocked()
{
    auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));

    for(it->Seek("n:"); it->Valid() && it->key().ToString().rfind("n:", 0) == 0; it->Next())
    {
        NodeRecord node;
        if(parseNode(it->value().ToString(), node))
        {
            nodeRecords_[node.nodeId] = node;
            nodeRuntime_[node.nodeId].state = NodeLiveState::kOffline;
        }
    }
    return it->status().ok();
}

//调度算法
/*
struct PlacementPlan { //临时写入计划
    uint32_t chunkIndex   = 0;
    std::string leaseId   = 0;
    uint64_t routeVersion = 0; //防止冲突版本号
    int64_t  expiresAt    = 0; //过期时间
    std::vector<NodeSnapshot> chain; //节点备份
};
*/
PlacementPlan GatewayState::selectPlacementLocked(const SessionState& session, uint32_t index)
{   
    struct Candidate { NodeSnapshot node; double score; };
    const uint64_t bytes = index + 1 == session.totalChunks ? session.fileSize - static_cast<uint64_t>(index) * session.chunkSize : session.chunkSize;
    std::vector<Candidate> candidates;

    for(const auto& [id, record] : nodeRecords_)
    {
        const NodeRuntime runtime = nodeRuntime_[id];
        const uint32_t reservedWrites = reservedWritesByNode_[id];
        const uint64_t reservedBytes = reservedBytesByNode_[id];
        if(!hasCapability(record, "storage") || runtime.state != NodeLiveState::kOnline ||
           reservedWrites >= record.maxConcurrentWrites) continue;
        if (runtime.freeBytes <= reservedBytes + bytes + record.reservedBytes) continue;
        const double capacity = static_cast<double>(record.maxStorageBytes ? record.maxStorageBytes : runtime.usedBytes + runtime.freeBytes);
        const double freeRatio = capacity > 0
            ? static_cast<double>(runtime.freeBytes - reservedBytes) / capacity : 0.0;
        const double idle = (1.0 - runtime.cpuUsage) * .10 + (1.0 - runtime.memoryUsage) * .10 + (1.0 - runtime.diskIoUsage) * .15;
        const double network = 1.0 / (1.0 + runtime.netOutMbps / 100.0) * .15;
        const double connections = 1.0 / (1.0 + runtime.activeUploads + reservedWrites) * .10;
        candidates.push_back({{record, runtime}, freeRatio * .35 + idle + network + connections});
    }
    std::sort(candidates.begin(), candidates.end(), [](const Candidate& left, const Candidate& right){
        return left.score > right.score;
    });
    PlacementPlan plan;
    plan.chunkIndex = index;
    for(size_t i = 0; i < candidates.size() && i < 2; i++)
    {
        plan.chain.push_back(candidates[i].node);
    }
    return plan;
}   

bool GatewayState::reserveLeaseLocked(const SessionState& session,
                                      const ChunkRouteRequest& request,
                                      PlacementPlan& plan,
                                      int64_t now)
{
    if(plan.chain.empty()) return false;
    plan.leaseId = randomId();
    if(plan.leaseId.empty()) return false;
    plan.routeVersion = std::hash<std::string>{}(plan.leaseId);
    plan.expiresAt = now + 120;

    WriteLease lease;
    lease.leaseId = plan.leaseId;
    lease.requestKey = routeRequestKey(session.sessionId, request);
    lease.sessionId = session.sessionId;
    lease.chunkIndex = request.chunkIndex;
    lease.chunkHash = request.chunkHash;
    lease.chunkSize = request.chunkSize;
    lease.expiresAt = plan.expiresAt;
    lease.plan = plan;

    for(const auto& node : plan.chain) {
        ++reservedWritesByNode_[node.record.nodeId];
        reservedBytesByNode_[node.record.nodeId] += request.chunkSize;
    }
    leaseByRequestKey_[lease.requestKey] = lease.leaseId;
    leases_[lease.leaseId] = std::move(lease);
    return true;
}

void GatewayState::releaseLeaseLocked(const std::string& leaseId)
{
    const auto it = leases_.find(leaseId);
    if(it == leases_.end()) return;

    for(const auto& node : it->second.plan.chain) {
        const std::string& nodeId = node.record.nodeId;
        auto writes = reservedWritesByNode_.find(nodeId);
        if(writes != reservedWritesByNode_.end()) {
            if(writes->second <= 1) reservedWritesByNode_.erase(writes);
            else --writes->second;
        }
        auto bytes = reservedBytesByNode_.find(nodeId);
        if(bytes != reservedBytesByNode_.end()) {
            if(bytes->second <= it->second.chunkSize) reservedBytesByNode_.erase(bytes);
            else bytes->second -= it->second.chunkSize;
        }
    }
    const auto request = leaseByRequestKey_.find(it->second.requestKey);
    if(request != leaseByRequestKey_.end() && request->second == leaseId) {
        leaseByRequestKey_.erase(request);
    }
    leases_.erase(it);
}

void GatewayState::releaseExpiredLeasesLocked(int64_t now)
{
    std::vector<std::string> expired;
    for(const auto& [leaseId, lease] : leases_) {
        if(lease.expiresAt <= now) expired.push_back(leaseId);
    }
    for(const auto& leaseId : expired) releaseLeaseLocked(leaseId);
}

void GatewayState::releaseLeasesForNodeLocked(const std::string& nodeId)
{
    std::vector<std::string> affected;
    for(const auto& [leaseId, lease] : leases_) {
        const bool containsNode = std::any_of(lease.plan.chain.begin(), lease.plan.chain.end(),
            [&nodeId](const NodeSnapshot& node) { return node.record.nodeId == nodeId; });
        if(containsNode) affected.push_back(leaseId);
    }
    for(const auto& leaseId : affected) releaseLeaseLocked(leaseId);
}

//
GatewayState::GatewayState(std::string& dbPath) : dbPath_(std::move(dbPath)) { }
GatewayState::~GatewayState() = default;

bool GatewayState::open()
{
    std::lock_guard<std::mutex> lock(mutex_);
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::DB* raw = nullptr;
    if(!leveldb::DB::Open(options, dbPath_, &raw).ok()) return false;
    db_.reset(raw);
    return loadNodesLocked() && loadSessionsLocked() && loadFilesLocked() && loadRoutesLocked();
}

//节点管理
bool GatewayState::registerNode(const NodeRecord& node)
{
    if(node.nodeId.empty() || node.address.empty() || !hasCapability(node, "storage")) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    const bool firstRegistration = nodeRecords_.find(node.nodeId) == nodeRecords_.end();
    nodeRecords_[node.nodeId] = node;
    if(firstRegistration) nodeRuntime_[node.nodeId].state = NodeLiveState::kRecovering;
    return db_->Put(leveldb::WriteOptions(), "n:"+node.nodeId, nodeValue(node)).ok();
}
bool GatewayState::heartbeat(const std::string& nodeId, const NodeRuntime& runtime)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if(!nodeRecords_.count(nodeId)) return false;
    releaseExpiredLeasesLocked(unixSeconds());
    NodeRuntime next = runtime;
    next.state = NodeLiveState::kOnline;
    next.lastHeartbeatAt = unixSeconds();
    nodeRuntime_[nodeId] = next;
    return true;
}
void GatewayState::checkNodeTimeouts(int64_t now, int64_t suspectAfterSeconds, int64_t offlineAfterSeconds)
{
    std::lock_guard<std::mutex> lock(mutex_);
    releaseExpiredLeasesLocked(now);
    for(auto& [id, runtime] : nodeRuntime_)
    {
        if(runtime.lastHeartbeatAt == 0 || now - runtime.lastHeartbeatAt >= offlineAfterSeconds) 
        {
            runtime.state = NodeLiveState::kOffline;
            releaseLeasesForNodeLocked(id);
        }
        else if(now - runtime.lastHeartbeatAt >= suspectAfterSeconds)
        {
            runtime.state = NodeLiveState::kSuspect;
        }
    }
}
std::vector<NodeSnapshot> GatewayState::nodes() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<NodeSnapshot> out;
    for(const auto& [id, recoed] : nodeRecords_)
    {
        const auto it = nodeRuntime_.find(id);
        out.push_back({recoed, it == nodeRuntime_.end() ? NodeRuntime{} : it->second});
    }
    return out;
}

//会话管理  
bool GatewayState::createSession(const std::string& fileName, const std::string& dirPath,
        uint64_t fileSize, uint32_t chunkSize, SessionState& out)
{
    if(fileName.empty() || fileSize == 0) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    out = {};
    out.sessionId = randomId();
    if(out.sessionId.empty()) return false;    
    out.fileName = fileName;
    out.dirPath = dirPath;
    out.fileSize = fileSize;
    out.chunkSize = chunkSize == 0 ? 4 * 1024 * 1024 : chunkSize;
    //整数除法的“向上取整（Ceiling）”公式
    out.totalChunks = static_cast<uint32_t>((fileSize + out.chunkSize - 1) / out.chunkSize);
    out.createdAt = out.lastActivityAt = unixSeconds();
    sessions_[out.sessionId] = out;
    return persistSessionLocked(out);
}
bool GatewayState::getSession(const std::string& sessionId, SessionState& out) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = sessions_.find(sessionId);
    if(it == sessions_.end()) return false;
    out = it->second;
    return true;
}
RoutePlanStatus GatewayState::planRoutes(const std::string& sessionId,
                                         const std::vector<ChunkRouteRequest>& requests,
                                         std::vector<PlacementPlan>& out)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = sessions_.find(sessionId);
    if(it == sessions_.end() || requests.empty()) return RoutePlanStatus::kInvalidRequest;
    out.clear();
    const int64_t now = unixSeconds();
    releaseExpiredLeasesLocked(now);
    std::vector<std::string> createdLeaseIds;
    for(const auto& request : requests)
    {
        const uint32_t index = request.chunkIndex;
        if(index >= it->second.totalChunks) {
            for(const auto& leaseId : createdLeaseIds) releaseLeaseLocked(leaseId);
            out.clear();
            return RoutePlanStatus::kInvalidRequest;
        }
        const uint64_t expected = index + 1 == it->second.totalChunks
            ? it->second.fileSize - static_cast<uint64_t>(index) * it->second.chunkSize
            : it->second.chunkSize;
        if(request.chunkHash.empty() || request.chunkSize != expected ||
           it->second.completed.count(index))
        {
            for(const auto& leaseId : createdLeaseIds) releaseLeaseLocked(leaseId);
            out.clear();
            return RoutePlanStatus::kInvalidRequest;
        }

        const std::string requestKey = routeRequestKey(sessionId, request);
        const auto existingId = leaseByRequestKey_.find(requestKey);
        if(existingId != leaseByRequestKey_.end()) {
            const auto existingLease = leases_.find(existingId->second);
            if(existingLease != leases_.end()) {
                out.push_back(existingLease->second.plan);
                continue;
            }
            leaseByRequestKey_.erase(existingId);
        }

        PlacementPlan plan = selectPlacementLocked(it->second, index);
        if(plan.chain.empty() || !reserveLeaseLocked(it->second, request, plan, now)) {
            for(const auto& leaseId : createdLeaseIds) releaseLeaseLocked(leaseId);
            out.clear();
            return RoutePlanStatus::kNoCapacity;
        }
        createdLeaseIds.push_back(plan.leaseId);
        out.push_back(std::move(plan));
    }
    return RoutePlanStatus::kOk;
}
CommitChunkStatus GatewayState::commitChunk(const std::string& sessionId, uint32_t index,
                                            const std::string& chunkHash, uint64_t size,
                                            const std::vector<std::string>& successfulNodes,
                                            const std::string& leaseId)
{
    //确认单个分片写入成功
    if(chunkHash.empty() || successfulNodes.empty()) return CommitChunkStatus::kInvalidRequest;
    std::lock_guard<std::mutex> lock(mutex_);
    releaseExpiredLeasesLocked(unixSeconds());
    auto sessionIt = sessions_.find(sessionId);
    if(sessionIt == sessions_.end() || index >= sessionIt->second.totalChunks) return CommitChunkStatus::kInvalidRequest;
    const uint64_t expected = index + 1 == sessionIt->second.totalChunks ? sessionIt->second.fileSize - static_cast<uint64_t>(index) * sessionIt->second.chunkSize : sessionIt->second.chunkSize;
    if(size != expected) return CommitChunkStatus::kInvalidRequest;

    const auto completed = sessionIt->second.completed.find(index);
    if(completed != sessionIt->second.completed.end()) {
        return completed->second.chunkHash == chunkHash && completed->second.size == size
            ? CommitChunkStatus::kAlreadyCommitted
            : CommitChunkStatus::kInvalidRequest;
    }

    const auto lease = leases_.find(leaseId);
    if(lease == leases_.end() || lease->second.sessionId != sessionId ||
       lease->second.chunkIndex != index || lease->second.chunkHash != chunkHash ||
       lease->second.chunkSize != size) return CommitChunkStatus::kInvalidRequest;
    for(const auto& nodeId : successfulNodes) {
        const bool allowed = std::any_of(lease->second.plan.chain.begin(), lease->second.plan.chain.end(),
            [&nodeId](const NodeSnapshot& node) { return node.record.nodeId == nodeId; });
        if(!allowed) return CommitChunkStatus::kInvalidRequest;
    }
    sessionIt->second.completed[index] = {index, chunkHash, size};
    sessionIt->second.lastActivityAt = unixSeconds();
    //真实chunk元数据落盘
    ChunkRoute& route = routes_[chunkHash];
    route.chunkHash = chunkHash;
    route.size = size;
    route.updateAt = unixSeconds();
    for(const auto& node : successfulNodes)
    {
        if(std::find(route.replicas.begin(), route.replicas.end(), node) == route.replicas.end())
        {
            route.replicas.push_back(node);
        }
    }
    const bool persisted = persistRouteLocked(route) && persistSessionLocked(sessionIt->second);
    if(persisted) releaseLeaseLocked(leaseId);
    return persisted ? CommitChunkStatus::kCommitted : CommitChunkStatus::kInvalidRequest;
}

bool GatewayState::releaseLease(const std::string& leaseId)
{
    if(leaseId.empty()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    if(leases_.find(leaseId) == leases_.end()) return false;
    releaseLeaseLocked(leaseId);
    return true;
}
bool GatewayState::commitFile(const std::string& sessionId, FileMeta& out)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto sessionIt = sessions_.find(sessionId);
    if(sessionIt == sessions_.end() || sessionIt->second.completed.size() != sessionIt->second.totalChunks)
    {
        return false;
    }
    std::string manifest;
    out = {};
    out.ownerId = sessionIt->second.ownerId; 
    out.fileName = sessionIt->second.fileName; 
    out.dirPath = sessionIt->second.dirPath; 
    out.fileSize = sessionIt->second.fileSize; 
    out.chunkSize = sessionIt->second.chunkSize; 
    out.createdAt = unixSeconds(); 
    out.state = FileState::kAvailable;
    for(int i = 0; i < sessionIt->second.totalChunks; i++)
    {
        auto chunk = sessionIt->second.completed.find(i);
        if(chunk == sessionIt->second.completed.end()) return false;
        out.chunkHashes.push_back(chunk->second.chunkHash);
        manifest += chunk->second.chunkHash + ":" + std::to_string(chunk->second.size) + ";";
        const auto route = routes_.find(chunk->second.chunkHash);
        if(route == routes_.end() || route->second.replicas.size() < 2)
        out.state = FileState::kProtecting;
    }
    out.fileHash = sha256Hex(manifest.data(), manifest.size());
    files_[out.fileHash] = out;
    return persistFileLocked(out);
}
bool GatewayState::getFile(const std::string& fileHash, FileMeta& out) const
{
    //false -- 没找到， true 找到
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = files_.find(fileHash);
    if(it == files_.end()) return false;
    out = it->second;
    return true;
}
bool GatewayState::getRoute(const std::string& chunkHash, ChunkRoute& out) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = routes_.find(chunkHash);
    if(it == routes_.end()) return false;
    out = it->second;
    return true;
}

bool GatewayState::buildManifestSnapshot(const std::string& fileHash,
                                         ManifestSnapshot& out) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto file = files_.find(fileHash);
    if(file == files_.end()) return false;

    ManifestSnapshot snapshot;
    snapshot.file = file->second;
    snapshot.routes.reserve(snapshot.file.chunkHashes.size());
    for(const auto& chunkHash : snapshot.file.chunkHashes) {
        const auto route = routes_.find(chunkHash);
        if(route == routes_.end()) return false;
        snapshot.routes.push_back(route->second);
        for(const auto& nodeId : route->second.replicas) {
            const auto node = nodeRecords_.find(nodeId);
            if(node == nodeRecords_.end()) return false;
            snapshot.nodes.emplace(nodeId, node->second);
        }
    }
    out = std::move(snapshot);
    return true;
}



}
}






















