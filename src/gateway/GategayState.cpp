#include "gateway/GatewayState.hpp"
#include "utils/Util.hpp"


#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <leveldb/db.h>
#include <leveldb/iterator.h>
#include <leveldb/options.h>
#include <leveldb/write_batch.h>
#include <memory>
#include <mutex>
#include <ratio>
#include <set>
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

std::string catalogPathKey(const std::string& ownerId, const std::string& path)
{
    return ownerId + '\n' + path;
}

std::string thumbnailKey(const std::string& sourceFileHash, const std::string& profile)
{
    return "d:" + sourceFileHash + ":thumbnail:" + profile;
}

bool isClaimableMediaJob(const media::MediaJob& job, int64_t now)
{
    if (job.state == media::JobState::kPending || job.state == media::JobState::kFailed) {
        return job.nextRetryAt <= now;
    }
    return job.state == media::JobState::kRunning && job.leaseUntil <= now;
}

bool normalizeDirectoryPath(const std::string& input, std::string& out)
{
    if (input.empty() || input.front() != '/' || input.find('\0') != std::string::npos) return false;
    std::vector<std::string> components;
    for (const auto& component : split(input, '/')) {
        if (component.empty()) continue;
        if (component == "." || component == "..") return false;
        components.push_back(component);
    }
    out = "/";
    for (size_t i = 0; i < components.size(); ++i) {
        if (i != 0) out += '/';
        out += components[i];
    }
    return true;
}

bool normalizeEntryName(const std::string& input, std::string& out)
{
    if (input.empty() || input == "." || input == ".." ||
        input.find('/') != std::string::npos || input.find('\0') != std::string::npos) return false;
    out = input;
    return true;
}

std::string childPath(const std::string& parentPath, const std::string& name)
{
    return parentPath == "/" ? "/" + name : parentPath + "/" + name;
}

std::vector<Breadcrumb> breadcrumbsFor(const std::string& path)
{
    std::vector<Breadcrumb> result;
    result.push_back({"/", "/"});
    if (path == "/") return result;
    std::string current;
    for (const auto& component : split(path.substr(1), '/')) {
        if (component.empty()) continue;
        current += "/" + component;
        result.push_back({component, current});
    }
    return result;
}

std::string directoryValue(const DirectoryMeta& directory)
{
    return hexEncode(directory.ownerId) + "|" + hexEncode(directory.path) + "|" +
           std::to_string(directory.createdAt);
}

bool parseDirectory(const std::string& value, DirectoryMeta& directory)
{
    const auto fields = split(value, '|');
    if (fields.size() != 3) return false;
    try {
        return hexDecode(fields[0], directory.ownerId) && hexDecode(fields[1], directory.path) &&
               ((directory.createdAt = std::stoll(fields[2])), true);
    } catch (...) {
        return false;
    }
}

std::string objectValue(const ObjectMeta& object)
{
    return hexEncode(object.objectId) + "|" + hexEncode(object.ownerId) + "|" +
           hexEncode(object.parentPath) + "|" + hexEncode(object.name) + "|" +
           hexEncode(object.fileHash) + "|" + std::to_string(object.fileSize) + "|" +
           hexEncode(object.contentType) + "|" + std::to_string(static_cast<int>(object.state)) +
           "|" + std::to_string(object.createdAt);
}

bool parseObject(const std::string& value, ObjectMeta& object)
{
    const auto fields = split(value, '|');
    if (fields.size() != 9) return false;
    try {
        if (!hexDecode(fields[0], object.objectId) || !hexDecode(fields[1], object.ownerId) ||
            !hexDecode(fields[2], object.parentPath) || !hexDecode(fields[3], object.name) ||
            !hexDecode(fields[4], object.fileHash) || !hexDecode(fields[6], object.contentType)) {
            return false;
        }
        object.fileSize = std::stoull(fields[5]);
        object.state = static_cast<FileState>(std::stoi(fields[7]));
        object.createdAt = std::stoll(fields[8]);
        return true;
    } catch (...) {
        return false;
    }
}

size_t estimatedObjectBytes(const ObjectMeta& object)
{
    return sizeof(ObjectMeta) + object.objectId.size() + object.ownerId.size() +
           object.parentPath.size() + object.name.size() + object.fileHash.size() +
           object.contentType.size();
}

constexpr size_t kObjectCacheMaxEntries = 4096;
constexpr size_t kObjectCacheMaxBytes = 4 * 1024 * 1024;
constexpr int64_t kObjectCacheTtlSeconds = 10 * 60;
constexpr size_t kFileCacheMaxEntries = 2048;
constexpr size_t kFileCacheMaxBytes = 8 * 1024 * 1024;
constexpr int64_t kFileCacheTtlSeconds = 10 * 60;
constexpr size_t kRouteCacheMaxEntries = 8192;
constexpr size_t kRouteCacheMaxBytes = 8 * 1024 * 1024;
constexpr int64_t kRouteCacheTtlSeconds = 10 * 60;
constexpr size_t kCatalogCacheMaxEntries = 512;
constexpr size_t kCatalogCacheMaxBytes = 8 * 1024 * 1024;
constexpr int64_t kCatalogCacheTtlSeconds = 30;
constexpr size_t kManifestCacheMaxEntries = 256;
constexpr size_t kManifestCacheMaxBytes = 16 * 1024 * 1024;
constexpr int64_t kManifestCacheTtlSeconds = 60;

size_t estimatedFileBytes(const FileMeta& file)
{
    size_t bytes = sizeof(FileMeta) + file.objectId.size() + file.fileHash.size() +
                   file.ownerId.size() + file.fileName.size() + file.dirPath.size();
    for (const auto& hash : file.chunkHashes) bytes += hash.size();
    return bytes;
}

size_t estimatedRouteBytes(const ChunkRoute& route)
{
    size_t bytes = sizeof(ChunkRoute) + route.chunkHash.size();
    for (const auto& replica : route.replicas) bytes += replica.size();
    return bytes;
}

size_t estimatedCatalogBytes(const CatalogSnapshot& catalog)
{
    size_t bytes = sizeof(CatalogSnapshot) + catalog.path.size();
    for (const auto& breadcrumb : catalog.breadcrumbs) bytes += sizeof(Breadcrumb) + breadcrumb.name.size() + breadcrumb.path.size();
    for (const auto& directory : catalog.directories) bytes += sizeof(DirectoryMeta) + directory.ownerId.size() + directory.path.size();
    for (const auto& object : catalog.files) bytes += estimatedObjectBytes(object);
    return bytes;
}

size_t estimatedManifestBytes(const ManifestSnapshot& manifest)
{
    size_t bytes = sizeof(ManifestSnapshot) + estimatedFileBytes(manifest.file);
    for (const auto& route : manifest.routes) bytes += estimatedRouteBytes(route);
    for (const auto& [nodeId, node] : manifest.nodes) {
        bytes += nodeId.size() + sizeof(NodeRecord) + node.nodeId.size() + node.address.size();
        for (const auto& capability : node.capabilities) bytes += capability.size();
    }
    return bytes;
}

std::string deleteTaskValue(const DeleteTask& task)
{
    std::vector<std::string> nodes;
    for (const auto& nodeId : task.pendingNodeIds) nodes.push_back(hexEncode(nodeId));
    return hexEncode(task.chunkHash) + "|" + std::to_string(task.createdAt) + "|" +
           std::to_string(task.updatedAt) + "|" + join(nodes, ',');
}

bool parseDeleteTask(const std::string& value, DeleteTask& task)
{
    const auto fields = split(value, '|');
    if (fields.size() != 4) return false;
    try {
        if (!hexDecode(fields[0], task.chunkHash)) return false;
        task.createdAt = std::stoll(fields[1]);
        task.updatedAt = std::stoll(fields[2]);
        for (const auto& node : split(fields[3], ',')) {
            if (node.empty()) continue;
            std::string nodeId;
            if (!hexDecode(node, nodeId) || nodeId.empty()) return false;
            task.pendingNodeIds.push_back(std::move(nodeId));
        }
        std::sort(task.pendingNodeIds.begin(), task.pendingNodeIds.end());
        task.pendingNodeIds.erase(std::unique(task.pendingNodeIds.begin(), task.pendingNodeIds.end()),
                                  task.pendingNodeIds.end());
        return !task.chunkHash.empty() && !task.pendingNodeIds.empty();
    } catch (...) {
        return false;
    }
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
           std::to_string(session.totalChunks) + "|" + std::to_string(session.createdAt) + "|" + std::to_string(session.lastActivityAt) + "|" + join(chunks, ';') + "|" +
           hexEncode(session.manifestHash);
}
bool parseSession(const std::string& value, SessionState& session) {
    const auto f = split(value, '|'); 
    if (f.size() != 10 && f.size() != 11) return false;
    try {
        if (!hexDecode(f[0], session.sessionId) || !hexDecode(f[1], session.ownerId) || !hexDecode(f[2], session.fileName) || !hexDecode(f[3], session.dirPath)) return false;
        session.fileSize = std::stoull(f[4]); session.chunkSize = static_cast<uint32_t>(std::stoul(f[5])); session.totalChunks = static_cast<uint32_t>(std::stoul(f[6]));
        session.createdAt = std::stoll(f[7]); session.lastActivityAt = std::stoll(f[8]);
        for (const auto& item : split(f[9], ';')) { if (item.empty()) continue; const auto chunk = split(item, ','); if (chunk.size() != 3) return false; CompletedChunk completed; completed.index = static_cast<uint32_t>(std::stoul(chunk[0])); if (!hexDecode(chunk[1], completed.chunkHash)) return false; completed.size = std::stoull(chunk[2]); session.completed[completed.index] = completed; }
        if (f.size() == 11 && !hexDecode(f[10], session.manifestHash)) return false;
        return true;
    } catch (...) { return false; }
}

bool GatewayState::persistSessionLocked(const SessionState& session)
{
    return db_->Put(leveldb::WriteOptions(), "s:" + session.sessionId, sessionValue(session)).ok();
}
bool GatewayState::persistRouteLocked(const ChunkRoute& route)
{
    return db_->Put(leveldb::WriteOptions(), "c:" + route.chunkHash, routeValue(route)).ok();
}
bool GatewayState::persistDirectoryLocked(const DirectoryMeta& directory)
{
    return db_->Put(leveldb::WriteOptions(),
                    "dir:" + catalogPathKey(directory.ownerId, directory.path),
                    directoryValue(directory)).ok();
}
bool GatewayState::getFileLocked(const std::string& fileHash, FileMeta& out) const
{
    if (const auto cached = fileCache_.get(fileHash)) {
        out = *cached;
        return true;
    }
    if (!db_) return false;
    std::string value;
    if (!db_->Get(leveldb::ReadOptions(), "f:" + fileHash, &value).ok()) return false;
    FileMeta file;
    if (!parseFile(value, file) || file.fileHash != fileHash) return false;
    fileCache_.put(fileHash, std::make_shared<const FileMeta>(file),
                   estimatedFileBytes(file), kFileCacheTtlSeconds);
    out = std::move(file);
    return true;
}

bool GatewayState::getRouteLocked(const std::string& chunkHash, ChunkRoute& out) const
{
    if (const auto cached = routeCache_.get(chunkHash)) {
        out = *cached;
        return true;
    }
    if (!db_) return false;
    std::string value;
    if (!db_->Get(leveldb::ReadOptions(), "c:" + chunkHash, &value).ok()) return false;
    ChunkRoute route;
    if (!parseRoute(value, route) || route.chunkHash != chunkHash) return false;
    routeCache_.put(chunkHash, std::make_shared<const ChunkRoute>(route),
                    estimatedRouteBytes(route), kRouteCacheTtlSeconds);
    out = std::move(route);
    return true;
}

bool GatewayState::getDirectoryLocked(const std::string& ownerId, const std::string& path,
                                      DirectoryMeta& out) const
{
    if (!db_) return false;
    std::string value;
    if (!db_->Get(leveldb::ReadOptions(), "dir:" + catalogPathKey(ownerId, path), &value).ok()) return false;
    return parseDirectory(value, out) && out.ownerId == ownerId && out.path == path;
}

bool GatewayState::getObjectLocked(const std::string& objectId, ObjectMeta& out) const
{
    if (const auto cached = objectCache_.get(objectId)) {
        out = *cached;
        return true;
    }
    if (!db_) return false;
    std::string value;
    if (!db_->Get(leveldb::ReadOptions(), "obj:" + objectId, &value).ok()) return false;

    ObjectMeta object;
    if (!parseObject(value, object) || object.objectId != objectId) return false;
    objectCache_.put(objectId, std::make_shared<const ObjectMeta>(object),
                     estimatedObjectBytes(object), kObjectCacheTtlSeconds);
    out = std::move(object);
    return true;
}

bool GatewayState::getMediaJobLocked(const std::string& jobId, media::MediaJob& out) const
{
    if (!db_ || jobId.empty()) return false;
    std::string value;
    if (!db_->Get(leveldb::ReadOptions(), "j:" + jobId, &value).ok()) return false;
    media::MediaJob job;
    if (!media::parseMediaJob(value, job) || job.jobId != jobId) return false;
    out = std::move(job);
    return true;
}

bool GatewayState::getThumbnailLocked(const std::string& sourceFileHash, const std::string& profile,
                                      media::ThumbnailMeta& out) const
{
    if (!db_ || sourceFileHash.empty() || profile.empty()) return false;
    std::string value;
    if (!db_->Get(leveldb::ReadOptions(), thumbnailKey(sourceFileHash, profile), &value).ok()) return false;
    media::ThumbnailMeta thumbnail;
    if (!media::parseThumbnailMeta(value, thumbnail) ||
        thumbnail.sourceFileHash != sourceFileHash || thumbnail.profile != profile) {
        return false;
    }
    out = std::move(thumbnail);
    return true;
}

bool GatewayState::objectIdAtPathLocked(const std::string& pathKey, std::string& objectId) const
{
    if (!db_) return false;
    return db_->Get(leveldb::ReadOptions(), "path:" + pathKey, &objectId).ok() && !objectId.empty();
}

bool GatewayState::loadDeleteTasksLocked()
{
    auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (it->Seek("del:"); it->Valid() && it->key().ToString().rfind("del:", 0) == 0; it->Next()) {
        DeleteTask task;
        if (!parseDeleteTask(it->value().ToString(), task)) return false;
        if (it->key().ToString() != "del:" + task.chunkHash) return false;
        deleteTasks_[task.chunkHash] = std::move(task);
    }
    return it->status().ok();
}
bool GatewayState::loadSessionsLocked()
{
    auto it =  std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));

    for(it->Seek("s:"); it->Valid() && it->key().ToString().rfind("s:", 0) == 0; it->Next())
    {
        SessionState session;
        if(parseSession(it->value().ToString(), session))
        {
            sessions_[session.sessionId] = session;
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
bool GatewayState::backfillLegacyCatalogLocked()
{
    leveldb::WriteBatch batch;
    bool changed = false;
    const int64_t now = unixSeconds();
    auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (it->Seek("f:"); it->Valid() && it->key().ToString().rfind("f:", 0) == 0; it->Next()) {
        FileMeta file;
        if (!parseFile(it->value().ToString(), file)) return false;
        const std::string fileHash = file.fileHash;
        std::string parent;
        std::string name;
        if (!normalizeDirectoryPath(file.dirPath, parent) || !normalizeEntryName(file.fileName, name)) continue;

        std::string current = "/";
        for (const auto& component : split(parent.substr(1), '/')) {
            if (component.empty()) continue;
            current = childPath(current, component);
            const std::string key = catalogPathKey(file.ownerId, current);
            std::string existing;
            const leveldb::Status status = db_->Get(leveldb::ReadOptions(), "dir:" + key, &existing);
            if (!status.ok() && !status.IsNotFound()) return false;
            if (status.IsNotFound()) {
                DirectoryMeta directory{file.ownerId, current, now};
                batch.Put("dir:" + key, directoryValue(directory));
                changed = true;
            }
        }

        const std::string pathKey = catalogPathKey(file.ownerId, childPath(parent, name));
        std::string existing;
        const leveldb::Status status = db_->Get(leveldb::ReadOptions(), "path:" + pathKey, &existing);
        if (!status.ok() && !status.IsNotFound()) return false;
        if (status.ok()) continue;
        ObjectMeta object;
        object.objectId = "legacy-" + fileHash;
        object.ownerId = file.ownerId;
        object.parentPath = parent;
        object.name = name;
        object.fileHash = fileHash;
        object.fileSize = file.fileSize;
        object.state = file.state;
        object.createdAt = file.createdAt;
        batch.Put("obj:" + object.objectId, objectValue(object));
        batch.Put("path:" + pathKey, object.objectId);
        changed = true;
    }
    return it->status().ok() && (!changed || db_->Write(leveldb::WriteOptions(), &batch).ok());
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
GatewayState::GatewayState(const std::string& dbPath)
    : dbPath_(dbPath),
      objectCache_(ObjectMetaCache::Config{kObjectCacheMaxEntries, kObjectCacheMaxBytes}),
      fileCache_(FileMetaCache::Config{kFileCacheMaxEntries, kFileCacheMaxBytes}),
      routeCache_(ChunkRouteCache::Config{kRouteCacheMaxEntries, kRouteCacheMaxBytes}),
      catalogCache_(CatalogCache::Config{kCatalogCacheMaxEntries, kCatalogCacheMaxBytes}),
      manifestCache_(ManifestCache::Config{kManifestCacheMaxEntries, kManifestCacheMaxBytes})
{
}
GatewayState::~GatewayState() = default;

bool GatewayState::open()
{
    std::lock_guard<std::mutex> lock(mutex_);
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::DB* raw = nullptr;
    if(!leveldb::DB::Open(options, dbPath_, &raw).ok()) return false;
    db_.reset(raw);
    return loadNodesLocked() && loadSessionsLocked() && loadDeleteTasksLocked() &&
           backfillLegacyCatalogLocked();
}

//节点管理
bool GatewayState::registerNode(const NodeRecord& node)
{
    if(node.nodeId.empty() || node.address.empty() || !hasCapability(node, "storage")) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    const bool firstRegistration = nodeRecords_.find(node.nodeId) == nodeRecords_.end();
    if (!db_->Put(leveldb::WriteOptions(), "n:"+node.nodeId, nodeValue(node)).ok()) return false;
    nodeRecords_[node.nodeId] = node;
    if(firstRegistration) nodeRuntime_[node.nodeId].state = NodeLiveState::kRecovering;
    manifestCache_.clear();
    return true;
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
std::string GatewayState::manifestHash(uint64_t fileSize, uint32_t chunkSize,
                                       const std::vector<ChunkRouteRequest>& chunks)
{
    if (fileSize == 0 || chunkSize == 0 || chunks.empty()) return {};
    std::string canonical = "minikv-manifest-v1\n" + std::to_string(fileSize) + "\n" +
                            std::to_string(chunkSize) + "\n";
    for (const auto& chunk : chunks) {
        canonical += std::to_string(chunk.chunkIndex) + ":" + chunk.chunkHash + ":" +
                     std::to_string(chunk.chunkSize) + "\n";
    }
    return sha256Hex(canonical.data(), canonical.size());
}

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

FileCommitStatus GatewayState::createObjectLinkLocked(const std::string& fileName,
                                                      const std::string& dirPath,
                                                      const FileMeta& file,
                                                      ObjectMeta& out)
{
    std::string parentPath;
    std::string name;
    if (!normalizeDirectoryPath(dirPath, parentPath) || !normalizeEntryName(fileName, name)) {
        return FileCommitStatus::kInvalidRequest;
    }

    const std::string pathKey = catalogPathKey(file.ownerId, childPath(parentPath, name));
    std::string existingObjectId;
    const leveldb::Status pathStatus = db_->Get(leveldb::ReadOptions(), "path:" + pathKey, &existingObjectId);
    if (pathStatus.ok()) return FileCommitStatus::kPathConflict;
    if (!pathStatus.IsNotFound()) return FileCommitStatus::kInvalidRequest;

    ObjectMeta object;
    object.objectId = randomId();
    if (object.objectId.empty()) return FileCommitStatus::kInvalidRequest;
    object.ownerId = file.ownerId;
    object.parentPath = parentPath;
    object.name = name;
    object.fileHash = file.fileHash;
    object.fileSize = file.fileSize;
    object.state = file.state;
    object.createdAt = unixSeconds();

    leveldb::WriteBatch batch;
    std::string current = "/";
    for (const auto& component : split(parentPath.substr(1), '/')) {
        if (component.empty()) continue;
        current = childPath(current, component);
        const std::string directoryKey = catalogPathKey(object.ownerId, current);
        std::string existingDirectory;
        const leveldb::Status directoryStatus = db_->Get(
            leveldb::ReadOptions(), "dir:" + directoryKey, &existingDirectory);
        if (!directoryStatus.ok() && !directoryStatus.IsNotFound()) {
            return FileCommitStatus::kInvalidRequest;
        }
        if (directoryStatus.IsNotFound()) {
            batch.Put("dir:" + directoryKey,
                      directoryValue({object.ownerId, current, object.createdAt}));
        }
    }
    batch.Put("obj:" + object.objectId, objectValue(object));
    batch.Put("path:" + pathKey, object.objectId);
    if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return FileCommitStatus::kInvalidRequest;

    objectCache_.erase(object.objectId);
    catalogCache_.clear();
    out = std::move(object);
    return FileCommitStatus::kCommitted;
}

PreflightStatus GatewayState::preflightUpload(const UploadPreflightRequest& request,
                                              UploadPreflightResult& out)
{
    out = {};
    std::string parentPath;
    std::string name;
    if (request.fileSize == 0 || request.chunkSize == 0 || request.manifestHash.empty() ||
        !normalizeDirectoryPath(request.dirPath, parentPath) ||
        !normalizeEntryName(request.fileName, name)) {
        return PreflightStatus::kInvalidRequest;
    }

    const uint64_t calculatedChunks =
        1 + (request.fileSize - 1) / request.chunkSize;
    if (calculatedChunks == 0 || calculatedChunks > UINT32_MAX ||
        request.chunks.size() != calculatedChunks) {
        return PreflightStatus::kInvalidRequest;
    }
    for (size_t index = 0; index < request.chunks.size(); ++index) {
        const auto& chunk = request.chunks[index];
        const uint64_t expected = index + 1 == request.chunks.size()
            ? request.fileSize - static_cast<uint64_t>(index) * request.chunkSize
            : request.chunkSize;
        if (chunk.chunkIndex != index || chunk.chunkHash.empty() || chunk.chunkSize != expected) {
            return PreflightStatus::kInvalidRequest;
        }
    }
    if (manifestHash(request.fileSize, request.chunkSize, request.chunks) != request.manifestHash) {
        return PreflightStatus::kInvalidRequest;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const std::string pathKey = catalogPathKey("admin", childPath(parentPath, name));
    std::string existingObjectId;
    const leveldb::Status pathStatus = db_->Get(leveldb::ReadOptions(), "path:" + pathKey, &existingObjectId);
    if (pathStatus.ok()) return PreflightStatus::kPathConflict;
    if (!pathStatus.IsNotFound()) return PreflightStatus::kInvalidRequest;

    FileMeta existingFile;
    if (getFileLocked(request.manifestHash, existingFile)) {
        const FileCommitStatus linkStatus =
            createObjectLinkLocked(name, parentPath, existingFile, out.object);
        if (linkStatus == FileCommitStatus::kCommitted) return PreflightStatus::kContentExists;
        return linkStatus == FileCommitStatus::kPathConflict
            ? PreflightStatus::kPathConflict
            : PreflightStatus::kInvalidRequest;
    }

    SessionState session;
    session.sessionId = randomId();
    if (session.sessionId.empty()) return PreflightStatus::kInvalidRequest;
    session.fileName = name;
    session.dirPath = parentPath;
    session.fileSize = request.fileSize;
    session.chunkSize = request.chunkSize;
    session.totalChunks = static_cast<uint32_t>(calculatedChunks);
    session.manifestHash = request.manifestHash;
    session.createdAt = session.lastActivityAt = unixSeconds();

    for (const auto& chunk : request.chunks) {
        ChunkRoute route;
        if (getRouteLocked(chunk.chunkHash, route) && route.size == chunk.chunkSize &&
            !route.replicas.empty()) {
            session.completed[chunk.chunkIndex] =
                {chunk.chunkIndex, chunk.chunkHash, chunk.chunkSize};
            out.presentChunks.push_back(chunk);
        } else {
            out.missingChunks.push_back(chunk);
        }
    }
    sessions_[session.sessionId] = session;
    if (!persistSessionLocked(session)) {
        sessions_.erase(session.sessionId);
        return PreflightStatus::kInvalidRequest;
    }
    out.session = std::move(session);
    return PreflightStatus::kUploadRequired;
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
    ChunkRoute route;
    if (!getRouteLocked(chunkHash, route)) route = {};
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
    if (persisted) {
        routeCache_.put(chunkHash, std::make_shared<const ChunkRoute>(route),
                        estimatedRouteBytes(route), kRouteCacheTtlSeconds);
        manifestCache_.clear();
    }
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
FileCommitStatus GatewayState::commitFile(const std::string& sessionId, FileMeta& out)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto sessionIt = sessions_.find(sessionId);
    if(sessionIt == sessions_.end() || sessionIt->second.completed.size() != sessionIt->second.totalChunks)
    {
        return FileCommitStatus::kInvalidRequest;
    }
    std::string legacyManifest;
    std::vector<ChunkRouteRequest> manifestChunks;
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
        if(chunk == sessionIt->second.completed.end()) return FileCommitStatus::kInvalidRequest;
        out.chunkHashes.push_back(chunk->second.chunkHash);
        legacyManifest += chunk->second.chunkHash + ":" + std::to_string(chunk->second.size) + ";";
        manifestChunks.push_back({static_cast<uint32_t>(i), chunk->second.chunkHash, chunk->second.size});
        ChunkRoute route;
        if (!getRouteLocked(chunk->second.chunkHash, route) || route.replicas.size() < 2) {
            out.state = FileState::kProtecting;
        }
    }
    // Sessions created by the preflight API have a browser-provided canonical
    // manifest. Older saved sessions keep their original V2 identity so they
    // remain resumable after this upgrade.
    if (!sessionIt->second.manifestHash.empty()) {
        const std::string calculated = manifestHash(out.fileSize, out.chunkSize, manifestChunks);
        if (calculated.empty() || calculated != sessionIt->second.manifestHash) {
            return FileCommitStatus::kInvalidRequest;
        }
        out.fileHash = calculated;
    } else {
        out.fileHash = sha256Hex(legacyManifest.data(), legacyManifest.size());
    }
    std::string parentPath;
    std::string name;
    if (!normalizeDirectoryPath(out.dirPath, parentPath) || !normalizeEntryName(out.fileName, name)) {
        return FileCommitStatus::kInvalidRequest;
    }
    out.dirPath = parentPath;
    out.fileName = name;

    const std::string pathKey = catalogPathKey(out.ownerId, childPath(parentPath, name));
    std::string existingObjectId;
    const leveldb::Status pathStatus = db_->Get(leveldb::ReadOptions(), "path:" + pathKey, &existingObjectId);
    if (pathStatus.ok()) return FileCommitStatus::kPathConflict;
    if (!pathStatus.IsNotFound()) return FileCommitStatus::kInvalidRequest;

    ObjectMeta object;
    object.objectId = randomId();
    if (object.objectId.empty()) return FileCommitStatus::kInvalidRequest;
    object.ownerId = out.ownerId;
    object.parentPath = parentPath;
    object.name = name;
    object.fileHash = out.fileHash;
    object.fileSize = out.fileSize;
    object.state = out.state;
    object.createdAt = out.createdAt;
    out.objectId = object.objectId;

    leveldb::WriteBatch batch;
    std::string current = "/";
    for (const auto& component : split(parentPath.substr(1), '/')) {
        if (component.empty()) continue;
        current = childPath(current, component);
        const std::string directoryKey = catalogPathKey(out.ownerId, current);
        std::string existingDirectory;
        const leveldb::Status directoryStatus = db_->Get(
            leveldb::ReadOptions(), "dir:" + directoryKey, &existingDirectory);
        if (!directoryStatus.ok() && !directoryStatus.IsNotFound()) return FileCommitStatus::kInvalidRequest;
        if (directoryStatus.IsNotFound()) {
            DirectoryMeta directory{out.ownerId, current, out.createdAt};
            batch.Put("dir:" + directoryKey, directoryValue(directory));
        }
    }
    std::string existingFile;
    const leveldb::Status fileStatus = db_->Get(leveldb::ReadOptions(), "f:" + out.fileHash, &existingFile);
    if (!fileStatus.ok() && !fileStatus.IsNotFound()) return FileCommitStatus::kInvalidRequest;
    if (fileStatus.IsNotFound()) {
        batch.Put("f:" + out.fileHash, fileValue(out));
    }
    batch.Put("obj:" + object.objectId, objectValue(object));
    batch.Put("path:" + pathKey, object.objectId);
    if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return FileCommitStatus::kInvalidRequest;

    objectCache_.erase(object.objectId);
    fileCache_.erase(out.fileHash);
    catalogCache_.clear();
    manifestCache_.erase(out.fileHash);
    return FileCommitStatus::kCommitted;
}
bool GatewayState::getFile(const std::string& fileHash, FileMeta& out) const
{
    //false -- 没找到， true 找到
    std::lock_guard<std::mutex> lock(mutex_);
    return getFileLocked(fileHash, out);
}
bool GatewayState::getRoute(const std::string& chunkHash, ChunkRoute& out) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return getRouteLocked(chunkHash, out);
}

bool GatewayState::buildManifestSnapshot(const std::string& fileHash,
                                         ManifestSnapshot& out) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto cached = manifestCache_.get(fileHash)) {
        out = *cached;
        return true;
    }
    FileMeta file;
    if (!getFileLocked(fileHash, file)) return false;

    ManifestSnapshot snapshot;
    snapshot.file = file;
    snapshot.routes.reserve(snapshot.file.chunkHashes.size());
    for(const auto& chunkHash : snapshot.file.chunkHashes) {
        ChunkRoute route;
        if (!getRouteLocked(chunkHash, route)) return false;
        snapshot.routes.push_back(route);
        for(const auto& nodeId : route.replicas) {
            const auto node = nodeRecords_.find(nodeId);
            if(node == nodeRecords_.end()) return false;
            snapshot.nodes.emplace(nodeId, node->second);
        }
    }
    manifestCache_.put(fileHash, std::make_shared<const ManifestSnapshot>(snapshot),
                       estimatedManifestBytes(snapshot), kManifestCacheTtlSeconds);
    out = std::move(snapshot);
    return true;
}

bool GatewayState::createDirectory(const std::string& parentPath, const std::string& name,
                                   DirectoryMeta* out)
{
    std::string parent;
    std::string entryName;
    if (!normalizeDirectoryPath(parentPath, parent) || !normalizeEntryName(name, entryName)) return false;

    std::lock_guard<std::mutex> lock(mutex_);
    DirectoryMeta parentDirectory;
    if (parent != "/" && !getDirectoryLocked("admin", parent, parentDirectory)) return false;
    const std::string path = childPath(parent, entryName);
    const std::string key = catalogPathKey("admin", path);
    std::string existingDirectory;
    const leveldb::Status directoryStatus = db_->Get(leveldb::ReadOptions(), "dir:" + key, &existingDirectory);
    if (directoryStatus.ok() || !directoryStatus.IsNotFound()) return false;
    std::string existingObjectId;
    const leveldb::Status objectStatus = db_->Get(leveldb::ReadOptions(), "path:" + key, &existingObjectId);
    if (objectStatus.ok() || !objectStatus.IsNotFound()) return false;

    DirectoryMeta directory;
    directory.path = path;
    directory.createdAt = unixSeconds();
    if (!persistDirectoryLocked(directory)) return false;
    catalogCache_.erase(parent);
    if (out != nullptr) *out = directory;
    return true;
}

bool GatewayState::buildCatalogSnapshotLocked(const std::string& normalized,
                                              CatalogSnapshot& out) const
{
    CatalogSnapshot snapshot;
    snapshot.path = normalized;
    snapshot.breadcrumbs = breadcrumbsFor(normalized);

    auto directoryIt = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (directoryIt->Seek("dir:"); directoryIt->Valid() &&
         directoryIt->key().ToString().rfind("dir:", 0) == 0; directoryIt->Next()) {
        DirectoryMeta directory;
        if (!parseDirectory(directoryIt->value().ToString(), directory)) return false;
        if (directory.ownerId == "admin" && directory.path != normalized &&
            directory.path.rfind(normalized == "/" ? "/" : normalized + "/", 0) == 0) {
            const std::string suffix = directory.path.substr(normalized == "/" ? 1 : normalized.size() + 1);
            if (suffix.find('/') == std::string::npos) snapshot.directories.push_back(std::move(directory));
        }
    }
    if (!directoryIt->status().ok()) return false;

    auto objectIt = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (objectIt->Seek("obj:"); objectIt->Valid() &&
         objectIt->key().ToString().rfind("obj:", 0) == 0; objectIt->Next()) {
        ObjectMeta object;
        if (!parseObject(objectIt->value().ToString(), object)) return false;
        if (object.ownerId == "admin" && object.parentPath == normalized) {
            snapshot.files.push_back(std::move(object));
        }
    }
    if (!objectIt->status().ok()) return false;

    std::sort(snapshot.directories.begin(), snapshot.directories.end(),
              [](const DirectoryMeta& left, const DirectoryMeta& right) { return left.path < right.path; });
    std::sort(snapshot.files.begin(), snapshot.files.end(),
              [](const ObjectMeta& left, const ObjectMeta& right) { return left.name < right.name; });
    out = std::move(snapshot);
    return true;
}

bool GatewayState::listCatalog(const std::string& path, CatalogSnapshot& out) const
{
    std::string normalized;
    if (!normalizeDirectoryPath(path, normalized)) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    if (const auto cached = catalogCache_.get(normalized)) {
        out = *cached;
        return true;
    }
    DirectoryMeta directory;
    if (normalized != "/" && !getDirectoryLocked("admin", normalized, directory)) return false;
    CatalogSnapshot snapshot;
    if (!buildCatalogSnapshotLocked(normalized, snapshot)) return false;
    catalogCache_.put(normalized, std::make_shared<const CatalogSnapshot>(snapshot),
                      estimatedCatalogBytes(snapshot), kCatalogCacheTtlSeconds);
    out = std::move(snapshot);
    return true;
}

bool GatewayState::getObject(const std::string& objectId, ObjectMeta& out) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return getObjectLocked(objectId, out);
}

ThumbnailEnqueueResult GatewayState::enqueueThumbnail(const std::string& sourceFileHash,
                                                       const std::string& profile, int64_t now)
{
    ThumbnailEnqueueResult result;
    if (sourceFileHash.empty() || profile.empty()) return result;

    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_) return result;

    media::ThumbnailMeta thumbnail;
    if (getThumbnailLocked(sourceFileHash, profile, thumbnail)) {
        media::MediaJob existing;
        if (!getMediaJobLocked(thumbnail.jobId, existing)) return result;
        result.job = existing;

        if (existing.state == media::JobState::kReady ||
            existing.state == media::JobState::kUnsupported ||
            (existing.state == media::JobState::kPending) ||
            (existing.state == media::JobState::kRunning && existing.leaseUntil > now) ||
            (existing.state == media::JobState::kFailed && existing.nextRetryAt > now)) {
            return result;
        }

        existing.state = media::JobState::kPending;
        existing.leaseUntil = 0;
        existing.leaseToken.clear();
        existing.nextRetryAt = now;
        existing.updatedAt = now;
        thumbnail.state = media::JobState::kPending;
        thumbnail.lastError.clear();
        thumbnail.updatedAt = now;

        leveldb::WriteBatch batch;
        batch.Put("j:" + existing.jobId, media::serializeMediaJob(existing));
        batch.Put(thumbnailKey(sourceFileHash, profile), media::serializeThumbnailMeta(thumbnail));
        if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return ThumbnailEnqueueResult{};
        result.job = std::move(existing);
        result.publishRequired = true;
        return result;
    }

    media::MediaJob job;
    job.jobId = randomId();
    if (job.jobId.empty()) return result;
    job.type = media::JobType::kThumbnail;
    job.sourceFileHash = sourceFileHash;
    job.profile = profile;
    job.state = media::JobState::kPending;
    job.nextRetryAt = now;
    job.createdAt = now;
    job.updatedAt = now;

    thumbnail.sourceFileHash = sourceFileHash;
    thumbnail.profile = profile;
    thumbnail.state = media::JobState::kPending;
    thumbnail.jobId = job.jobId;
    thumbnail.updatedAt = now;

    leveldb::WriteBatch batch;
    batch.Put("j:" + job.jobId, media::serializeMediaJob(job));
    batch.Put(thumbnailKey(sourceFileHash, profile), media::serializeThumbnailMeta(thumbnail));
    if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return result;
    result.job = std::move(job);
    result.publishRequired = true;
    return result;
}

bool GatewayState::getMediaJob(const std::string& jobId, media::MediaJob& out) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return getMediaJobLocked(jobId, out);
}

bool GatewayState::getThumbnail(const std::string& sourceFileHash, const std::string& profile,
                                media::ThumbnailMeta& out) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return getThumbnailLocked(sourceFileHash, profile, out);
}

bool GatewayState::claimMediaJob(const std::string& jobId, int64_t now, int64_t leaseSeconds,
                                 media::MediaJob& out)
{
    if (jobId.empty() || leaseSeconds <= 0 || leaseSeconds > 3600) return false;
    std::lock_guard<std::mutex> lock(mutex_);

    media::MediaJob job;
    if (!getMediaJobLocked(jobId, job) || !isClaimableMediaJob(job, now)) return false;
    media::ThumbnailMeta thumbnail;
    if (job.type != media::JobType::kThumbnail ||
        !getThumbnailLocked(job.sourceFileHash, job.profile, thumbnail) || thumbnail.jobId != job.jobId) {
        return false;
    }

    job.state = media::JobState::kRunning;
    ++job.attempts;
    job.leaseUntil = now + leaseSeconds;
    job.leaseToken = randomId();
    if (job.leaseToken.empty()) return false;
    job.nextRetryAt = 0;
    job.updatedAt = now;
    thumbnail.state = media::JobState::kRunning;
    thumbnail.lastError.clear();
    thumbnail.updatedAt = now;

    leveldb::WriteBatch batch;
    batch.Put("j:" + job.jobId, media::serializeMediaJob(job));
    batch.Put(thumbnailKey(job.sourceFileHash, job.profile), media::serializeThumbnailMeta(thumbnail));
    if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return false;
    out = std::move(job);
    return true;
}

bool GatewayState::completeMediaJob(const std::string& jobId, const std::string& leaseToken,
                                    const std::string& derivedObjectId,
                                    const std::string& derivedFileHash, int64_t now)
{
    if (jobId.empty() || leaseToken.empty() || derivedObjectId.empty() || derivedFileHash.empty()) return false;
    std::lock_guard<std::mutex> lock(mutex_);

    media::MediaJob job;
    if (!getMediaJobLocked(jobId, job) || job.state != media::JobState::kRunning ||
        job.leaseToken != leaseToken) {
        return false;
    }
    media::ThumbnailMeta thumbnail;
    if (job.type != media::JobType::kThumbnail ||
        !getThumbnailLocked(job.sourceFileHash, job.profile, thumbnail) || thumbnail.jobId != job.jobId) {
        return false;
    }

    job.state = media::JobState::kReady;
    job.leaseUntil = 0;
    job.leaseToken.clear();
    job.nextRetryAt = 0;
    job.lastError.clear();
    job.updatedAt = now;
    thumbnail.state = media::JobState::kReady;
    thumbnail.derivedObjectId = derivedObjectId;
    thumbnail.derivedFileHash = derivedFileHash;
    thumbnail.lastError.clear();
    thumbnail.updatedAt = now;

    leveldb::WriteBatch batch;
    batch.Put("j:" + job.jobId, media::serializeMediaJob(job));
    batch.Put(thumbnailKey(job.sourceFileHash, job.profile), media::serializeThumbnailMeta(thumbnail));
    return db_->Write(leveldb::WriteOptions(), &batch).ok();
}

bool GatewayState::failMediaJob(const std::string& jobId, const std::string& leaseToken,
                                bool unsupported, const std::string& error, int64_t nextRetryAt,
                                int64_t now)
{
    if (jobId.empty() || leaseToken.empty() || error.empty()) return false;
    std::lock_guard<std::mutex> lock(mutex_);

    media::MediaJob job;
    if (!getMediaJobLocked(jobId, job) || job.state != media::JobState::kRunning ||
        job.leaseToken != leaseToken) {
        return false;
    }
    media::ThumbnailMeta thumbnail;
    if (job.type != media::JobType::kThumbnail ||
        !getThumbnailLocked(job.sourceFileHash, job.profile, thumbnail) || thumbnail.jobId != job.jobId) {
        return false;
    }

    job.state = unsupported ? media::JobState::kUnsupported : media::JobState::kFailed;
    job.leaseUntil = 0;
    job.leaseToken.clear();
    job.nextRetryAt = unsupported ? 0 : nextRetryAt;
    job.lastError = error;
    job.updatedAt = now;
    thumbnail.state = job.state;
    thumbnail.lastError = error;
    thumbnail.updatedAt = now;

    leveldb::WriteBatch batch;
    batch.Put("j:" + job.jobId, media::serializeMediaJob(job));
    batch.Put(thumbnailKey(job.sourceFileHash, job.profile), media::serializeThumbnailMeta(thumbnail));
    return db_->Write(leveldb::WriteOptions(), &batch).ok();
}

std::vector<media::MediaJob> GatewayState::dueMediaJobs(int64_t now, size_t maxJobs) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<media::MediaJob> result;
    if (!db_ || maxJobs == 0) return result;

    auto iterator = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (iterator->Seek("j:"); iterator->Valid() &&
         iterator->key().ToString().rfind("j:", 0) == 0 && result.size() < maxJobs;
         iterator->Next()) {
        media::MediaJob job;
        if (!media::parseMediaJob(iterator->value().ToString(), job)) return {};
        if (isClaimableMediaJob(job, now)) result.push_back(std::move(job));
    }
    if (!iterator->status().ok()) return {};
    return result;
}

ObjectMetaCache::Stats GatewayState::objectCacheStats() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return objectCache_.stats();
}

CatalogCache::Stats GatewayState::catalogCacheStats() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return catalogCache_.stats();
}

ManifestCache::Stats GatewayState::manifestCacheStats() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return manifestCache_.stats();
}

MetadataCacheUsage GatewayState::metadataCacheUsage() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return {objectCache_.size(), fileCache_.size(), routeCache_.size(),
            catalogCache_.size(), manifestCache_.size()};
}

DeleteStatus GatewayState::deleteObject(const std::string& objectId)
{
    if (objectId.empty()) return DeleteStatus::kInvalidRequest;
    std::lock_guard<std::mutex> lock(mutex_);
    ObjectMeta object;
    if (!getObjectLocked(objectId, object)) return DeleteStatus::kNotFound;
    return deleteCatalogEntriesLocked({objectId}, {});
}

DeleteStatus GatewayState::deleteDirectory(const std::string& path)
{
    std::string normalized;
    if (!normalizeDirectoryPath(path, normalized) || normalized == "/") {
        return DeleteStatus::kInvalidRequest;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const std::string rootKey = catalogPathKey("admin", normalized);
    DirectoryMeta rootDirectory;
    if (!getDirectoryLocked("admin", normalized, rootDirectory)) return DeleteStatus::kNotFound;

    std::vector<std::string> objectIds;
    std::vector<std::string> directoryKeys;
    const std::string prefix = normalized + "/";
    auto objectIt = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (objectIt->Seek("obj:"); objectIt->Valid() && objectIt->key().ToString().rfind("obj:", 0) == 0; objectIt->Next()) {
        ObjectMeta object;
        if (!parseObject(objectIt->value().ToString(), object)) return DeleteStatus::kInvalidRequest;
        const std::string objectId = object.objectId;
        if (object.ownerId == "admin" &&
            (object.parentPath == normalized || object.parentPath.rfind(prefix, 0) == 0)) {
            objectIds.push_back(objectId);
        }
    }
    if (!objectIt->status().ok()) return DeleteStatus::kInvalidRequest;
    auto directoryIt = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (directoryIt->Seek("dir:"); directoryIt->Valid() && directoryIt->key().ToString().rfind("dir:", 0) == 0; directoryIt->Next()) {
        DirectoryMeta directory;
        if (!parseDirectory(directoryIt->value().ToString(), directory)) return DeleteStatus::kInvalidRequest;
        const std::string key = catalogPathKey(directory.ownerId, directory.path);
        if (directory.ownerId == "admin" &&
            (directory.path == normalized || directory.path.rfind(prefix, 0) == 0)) {
            directoryKeys.push_back(key);
        }
    }
    if (!directoryIt->status().ok()) return DeleteStatus::kInvalidRequest;
    return deleteCatalogEntriesLocked(objectIds, directoryKeys);
}

std::vector<DeleteTaskSnapshot> GatewayState::pendingDeletesForNode(const std::string& nodeId) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<DeleteTaskSnapshot> pending;
    for (const auto& [chunkHash, task] : deleteTasks_) {
        if (std::find(task.pendingNodeIds.begin(), task.pendingNodeIds.end(), nodeId) !=
            task.pendingNodeIds.end()) {
            pending.push_back({chunkHash, task.pendingNodeIds});
        }
    }
    return pending;
}

bool GatewayState::acknowledgeDelete(const std::string& chunkHash, const std::string& nodeId)
{
    if (chunkHash.empty() || nodeId.empty()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    const auto taskIt = deleteTasks_.find(chunkHash);
    if (taskIt == deleteTasks_.end()) return true;

    DeleteTask next = taskIt->second;
    const auto node = std::find(next.pendingNodeIds.begin(), next.pendingNodeIds.end(), nodeId);
    if (node == next.pendingNodeIds.end()) return true;
    next.pendingNodeIds.erase(node);
    next.updatedAt = unixSeconds();

    leveldb::WriteBatch batch;
    if (next.pendingNodeIds.empty()) {
        batch.Delete("del:" + chunkHash);
        batch.Delete("c:" + chunkHash);
    } else {
        batch.Put("del:" + chunkHash, deleteTaskValue(next));
    }
    if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return false;
    if (next.pendingNodeIds.empty()) {
        deleteTasks_.erase(taskIt);
        routeCache_.erase(chunkHash);
        manifestCache_.clear();
    } else {
        taskIt->second = std::move(next);
    }
    return true;
}

DeleteStatus GatewayState::deleteCatalogEntriesLocked(const std::vector<std::string>& objectIds,
                                                      const std::vector<std::string>& directoryKeys)
{
    std::map<std::string, ObjectMeta> targets;
    for (const auto& objectId : objectIds) {
        ObjectMeta object;
        if (!getObjectLocked(objectId, object)) return DeleteStatus::kNotFound;
        targets.emplace(objectId, std::move(object));
    }

    std::set<std::string> targetFileHashes;
    for (const auto& [objectId, object] : targets) {
        (void)objectId;
        targetFileHashes.insert(object.fileHash);
    }

    std::set<std::string> survivingFileHashes;
    auto objectIt = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (objectIt->Seek("obj:"); objectIt->Valid() && objectIt->key().ToString().rfind("obj:", 0) == 0; objectIt->Next()) {
        ObjectMeta object;
        if (!parseObject(objectIt->value().ToString(), object)) return DeleteStatus::kInvalidRequest;
        if (targets.count(object.objectId) == 0) survivingFileHashes.insert(object.fileHash);
    }
    if (!objectIt->status().ok()) return DeleteStatus::kInvalidRequest;

    std::set<std::string> survivingChunks;
    for (const auto& fileHash : survivingFileHashes) {
        FileMeta file;
        if (!getFileLocked(fileHash, file)) return DeleteStatus::kInvalidRequest;
        survivingChunks.insert(file.chunkHashes.begin(), file.chunkHashes.end());
    }

    std::set<std::string> removableFiles;
    std::set<std::string> removableChunks;
    for (const auto& fileHash : targetFileHashes) {
        if (survivingFileHashes.count(fileHash) != 0) continue;
        FileMeta file;
        if (!getFileLocked(fileHash, file)) return DeleteStatus::kInvalidRequest;
        removableFiles.insert(fileHash);
        for (const auto& chunkHash : file.chunkHashes) {
            if (survivingChunks.count(chunkHash) == 0) removableChunks.insert(chunkHash);
        }
    }

    std::map<std::string, DeleteTask> newTasks;
    const int64_t now = unixSeconds();
    for (const auto& chunkHash : removableChunks) {
        if (deleteTasks_.count(chunkHash) != 0) continue;
        ChunkRoute route;
        if (!getRouteLocked(chunkHash, route) || route.replicas.empty()) {
            return DeleteStatus::kInvalidRequest;
        }
        DeleteTask task;
        task.chunkHash = chunkHash;
        task.pendingNodeIds = route.replicas;
        std::sort(task.pendingNodeIds.begin(), task.pendingNodeIds.end());
        task.pendingNodeIds.erase(std::unique(task.pendingNodeIds.begin(), task.pendingNodeIds.end()),
                                  task.pendingNodeIds.end());
        if (task.pendingNodeIds.empty()) return DeleteStatus::kInvalidRequest;
        task.createdAt = now;
        task.updatedAt = now;
        newTasks.emplace(chunkHash, std::move(task));
    }

    leveldb::WriteBatch batch;
    for (const auto& [objectId, object] : targets) {
        batch.Delete("obj:" + objectId);
        batch.Delete("path:" + catalogPathKey(object.ownerId, childPath(object.parentPath, object.name)));
    }
    for (const auto& directoryKey : directoryKeys) batch.Delete("dir:" + directoryKey);
    for (const auto& fileHash : removableFiles) batch.Delete("f:" + fileHash);
    for (const auto& [chunkHash, task] : newTasks) {
        batch.Put("del:" + chunkHash, deleteTaskValue(task));
    }
    if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return DeleteStatus::kInvalidRequest;

    for (const auto& [objectId, object] : targets) {
        objectCache_.erase(objectId);
    }
    for (const auto& fileHash : removableFiles) {
        fileCache_.erase(fileHash);
        manifestCache_.erase(fileHash);
    }
    catalogCache_.clear();
    for (const auto& [chunkHash, task] : newTasks) deleteTasks_[chunkHash] = task;
    return DeleteStatus::kDeleted;
}



}
}
