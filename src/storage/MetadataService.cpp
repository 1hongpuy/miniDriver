#include "storage/MetadataService.hpp"
#include "storage/IMetadata.hpp"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <leveldb/iterator.h>
#include <leveldb/options.h>
#include <memory>
#include <mutex>
#include <sstream>
#include <iomanip> // C++ 标准库中的输入/输出流操纵符头文件
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

/*
Key 模式                       Value              用途
──────────────────────────────────────────────────────────
f:{fileHash}                → FileMetaV2 JSON    文件元数据
c:{fileHash}:{chunkIndex}   → ChunkMeta JSON     每个chunk的物理位置
s:{sessionId}               → SessionState JSON  上传会话状态，每次上传创建一个会话记录
n:{nodeId}                  → NodeInfo JSON      DataNode信息

V1 兼容:
p:{path}{name}              → MetaInfoV1 JSON    虚拟目录→文件映射
h:{hash}                    → PhysInfo JSON      ref_count + 物理信息
*/

namespace miniKV {
namespace storage {

MetadataService::MetadataService(const std::string& dbPath) :dbPath_(dbPath) {}

bool MetadataService::init() {
    leveldb::Options opts;
    opts.create_if_missing = true;
    leveldb::DB* ptr = nullptr;
    leveldb::Status s = leveldb::DB::Open(opts, dbPath_, &ptr);
    if(!s.ok()) return false;
    db_.reset(ptr); 
    return true;
}

//文件元数据

bool MetadataService::putFileMeta(const FileMetaV2& meta) 
{
    std::string key = makeFileKey(meta.fileHash);
    std::string val = fileMetaToJson(meta);
    leveldb::Status s = db_->Put(leveldb::WriteOptions(), key, val);
    return s.ok();
}
bool MetadataService::getFileMeta(const std::string& fileHash, FileMetaV2& out) 
{
    std::string key = makeFileKey(fileHash);
    std::string json;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &json);
    if(!s.ok()) return false;
    out = fileMetaFromJson(json);
    return true;
}
bool MetadataService::deleteFileMeta(const std::string& fileHash)
{
    std::string key = makeFileKey(fileHash);
    leveldb::Status s = db_->Delete(leveldb::WriteOptions(), key);
    return s.ok();
}

bool MetadataService::fileExists(const std::string& fileHash)
{
    std::string key = makeFileKey(fileHash);
    std::string dummy;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &dummy);
    return s.ok();
}

// ============================================================
// Chunk 元数据（c: keys）
// ============================================================

// bool MetadataService::putChunkMeta(const std::string& fileHash, uint32_t chunkIndex,
//     const ChunkMeta& meta) 
// {
//     std::string key = makeChunkKey(fileHash, chunkIndex);
//     std::string val = chunkMetaToJson(meta);
//     leveldb::Status s = db_->Put(leveldb::WriteOptions(), key, val);
//     return s.ok();
// }
// bool MetadataService::getChunkMeta(const std::string& fileHash, uint32_t chunkIndex,
//     ChunkMeta& out)
// {
//     std::string key = makeChunkKey(fileHash, chunkIndex);
//     std::string json;
//     leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &json);
//     if(!s.ok()) return false;
//     out = chunkMetaFromJson(json);
//     return true;
// }
// std::vector<ChunkMeta> MetadataService::getFileChunks(
// const std::string& fileHash)
// {
//     std::vector<ChunkMeta> result;
//     std::string prefix = makeChunkPrefix(fileHash);

//     auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
//     for(it->Seek(prefix); it->Valid() && it->key().starts_with(prefix) ; it->Next())
//     {
//         ChunkMeta cm = chunkMetaFromJson(it->value().ToString());
//         result.push_back(cm);
//     }

//     //seek找到第一个大于这个查找目标的字符，valid就是确定查找没有超出索引
//     return result;
// }
// bool MetadataService::markChunkDeleted(const std::string& fileHash,
//         uint32_t chunkIndex)
// {
//     //软删除
//     ChunkMeta meta; 
//     if(!getChunkMeta(fileHash, chunkIndex, meta)) return false;
//     meta.status = ChunkMeta::DELETED;
//     return putChunkMeta(fileHash,chunkIndex, meta);
// }
// bool MetadataService::deleteChunkMeta(const std::string& fileHash,
//        uint32_t chunkIndex)
// {
//     std::string key = makeChunkKey(fileHash, chunkIndex);
//     leveldb::Status s = db_->Delete(leveldb::WriteOptions(), key);
//     return s.ok();
// }
// std::vector<std::pair<std::string, uint32_t>> MetadataService::getDeletedChunks(
// int limit) //limiit 进行分批处理
// {
//     std::vector<std::pair<std::string, uint32_t>> result;
//     std::string prefix = "c:";
//     auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));

//     for(it->Seek(prefix) ; it->Valid() && it->value().starts_with(prefix) && (int)result.size() < limit; it->Next())
//     {
//         std::string key = it->key().ToString();
//         size_t p1 = key.find(':') + 1;
//         size_t p2 = key.find_last_of(':');

//         std::string hash = key.substr(p1, p2 - p1);
//         uint32_t idx = std::stoul(key.substr(p2+1));
//         result.push_back({hash, idx});
//     }
//     return result;
// }

//chunk 路由，每个切片，存储的这个分片所在服务器
//Key:   "c:h0"
//Value: "[\"vm-local-8c16g\",\"cloud-2c2g\"]"
bool MetadataService::putChunkRoute(const std::string& chunkHash,
        const std::vector<std::string>& nodeIds)
{
    std::string key = makeChunkKey(chunkHash);
    std::string val = nodesToJson(nodeIds);
    return db_->Put(leveldb::WriteOptions(), key,val).ok();
}
std::vector<std::string> MetadataService::getChunkRoute(const std::string& chunkHash)
{
    std::string key = makeChunkKey(chunkHash);
    std::string json;
    if(!db_->Get(leveldb::ReadOptions(), key, &json).ok()) return {};
    return nodesFromJson(json);
}
bool MetadataService::deleteChunkRoute(const std::string& chunkHash)
{
    return db_->Delete(leveldb::WriteOptions(), chunkHash).ok();
}

std::vector<std::pair<std::string, std::vector<std::string>>>
MetadataService::listAllChunkRoutes()
{
    std::vector<std::pair<std::string, std::vector<std::string>>> result;
    auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for(it->Seek("c:"); it->Valid() && it->key().starts_with("c:");it->Next())
    {
        std::string chunk = it->key().ToString().substr(2);
        result.push_back({chunk, nodesFromJson(it->value().ToString())});
    }
    return result;
}

//副本修复任务
std::string MetadataService::makeRepairTaskKey(const std::string& chunkHash) {
    return "r:" + std::to_string(time(nullptr)) + ":" + chunkHash;
}

std::string MetadataService::makeRepairPrefix()
{
    return "r:";
}

bool MetadataService::putRepairTask(const std::string& chunkHash,
    const std::string& assignedNode,
    const std::string& sourceNode,
    int64_t deadline)
{
    std::string key = makeRepairTaskKey(chunkHash);
    std::ostringstream oss;
    oss << "{\"assignedNode\":\"" << jsonEscape(assignedNode) << "\","
        << "\"sourceNode\":\"" << jsonEscape(sourceNode) << "\","
        << "\"deadline\":" << deadline << "}";
    return db_->Put(leveldb::WriteOptions(), key, oss.str()).ok();
}
// [用途] 查询某个 chunk 是否有进行中的修复任务
// [输出] {assignedNode, sourceNode, deadline} 或空
IMetadata::RepairTask MetadataService::getRepairTask(const std::string& chunkHash)
{
    RepairTask rt;
    auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (it->Seek("r:"); it->Valid() && it->key().starts_with("r:"); it->Next()) {
        // key: r:{timestamp}:{chunkHash}
        std::string key = it->key().ToString();
        if (key.find(chunkHash) != std::string::npos) {
            std::string json = it->value().ToString();
            rt.assignedNode = jsonGetStr(json, "assignedNode");
            rt.sourceNode   = jsonGetStr(json, "sourceNode");
            rt.deadline     = static_cast<int64_t>(jsonGetNum(json, "deadline"));
            return rt;
        }
    }
    return rt;  // 空 = 没有修复任务
}
bool MetadataService::deleteRepairTask(const std::string& chunkHash)
{
    auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (it->Seek("r:"); it->Valid() && it->key().starts_with("r:"); it->Next()) {
        if (it->key().ToString().find(chunkHash) != std::string::npos)
            return db_->Delete(leveldb::WriteOptions(), it->key().ToString()).ok();
    }
    return false;
}


// ============================================================
// 上传会话（s: keys）
// ============================================================
bool MetadataService::putSession(const SessionState &session)
{
    std::string key = makeSessionKey(session.sessionId);
    std::string val = sessionToJson(session);
    leveldb::Status s = db_->Put(leveldb::WriteOptions(), key, val);
    return s.ok();
}

bool MetadataService::getSession(const std::string& sessionId,
    SessionState& out) 
{
    std::string key = makeSessionKey(sessionId);
    std::string json;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &json);
    if(!s.ok()) return false;
    out = sessionFromJson(json);
    return true;
}
bool MetadataService::deleteSession(const std::string& sessionId)
{
    std::string key = makeSessionKey(sessionId);
    leveldb::Status s = db_->Delete(leveldb::WriteOptions(), key);
    return s.ok();
}
std::vector<std::string> MetadataService::listExpiredSessions(
int64_t ttlSeconds)
{
    std::vector<std::string> result;
    int64_t now = time(nullptr);
    std::string prefix = "s:";
    auto it = std::unique_ptr<leveldb::Iterator>(
        db_->NewIterator(leveldb::ReadOptions())
    );

    for(it->Seek(prefix) ; it->Valid() && it->value().starts_with(prefix) ; it->Next())
    {
        SessionState ss = sessionFromJson(it->value().ToString());
        if(now - ss.lastActivity > ttlSeconds)
        {
            result.push_back(ss.sessionId);
        }
    }
    return result;
}

// ============================================================
// 节点管理（n: keys）
// ============================================================
bool MetadataService::putNodeInfo(const NodeInfo& node)
{
    std::string key = makeNodeKey(node.nodeId);
    std::string val = nodeInfoToJson(node);
    leveldb::Status s = db_->Put(leveldb::WriteOptions(), key, val);
    return s.ok();
}
bool MetadataService::getNodeInfo(const std::string& nodeId, NodeInfo& out)
{
    std::string key = makeNodeKey(nodeId);
    std::string json;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &json);
    if(!s.ok()) return false;
    out = nodeInfoFromJson(json);
    return true;
}
std::vector<NodeInfo> MetadataService::listOnlineNodes()
{
    
    auto all = listAllNodes();
    std::vector<NodeInfo> result;
    result.resize(all.size());
    for(auto& n : all)
    {
        if(n.online)
        {
            result.push_back(std::move(n));
        }
    }
    return result;
}
std::vector<NodeInfo> MetadataService::listAllNodes()
{
    std::vector<NodeInfo> result;
    std::string prefix = "n:";

    auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));

    for(it->Seek(prefix) ; it->Valid() && it->value().starts_with(prefix); it->Next())
    {
        NodeInfo ni = nodeInfoFromJson(it->value().ToString());
        result.push_back(ni);
    }
    return result;
}

// ============================================================
// 引用计数
// ============================================================
bool MetadataService::incRefCount(const std::string& fileHash)
{
    FileMetaV2 meta;
    if(!getFileMeta(fileHash, meta)) return false;
    meta.refCount++;
    return putFileMeta(meta);
}
bool MetadataService::decRefCount(const std::string& fileHash)
{
    FileMetaV2 meta;
    if(!getFileMeta(fileHash, meta)) return false;
    if(meta.refCount > 0) meta.refCount--;
    return putFileMeta(meta);
}
int  MetadataService::getRefCount(const std::string& fileHash)
{
    FileMetaV2 meta;
    if(!getFileMeta(fileHash, meta)) return 0;
    return meta.refCount;
}

//key 构造
/*
f:{fileHash}                → FileMetaV2 JSON    文件元数据
c:{chunkhash}               → 存储的节点，备份的节点
s:{sessionId}               → SessionState JSON  上传会话状态，每次上传创建一个会话记录
n:{nodeId}                  → NodeInfo JSON      DataNode信息

*/
std::string MetadataService::makeFileKey(const std::string& hash){
    return  "f:" + hash; 
}

std::string MetadataService::makeChunkKey(const std::string& chunkhash){
    return  "c:" + chunkhash; 
}

std::string MetadataService::makeChunkPrefix(const std::string& hash)
{
    return  "c:" + hash + ":"; 
}

// s:{sessionId}
std::string MetadataService::makeSessionKey(const std::string& sid)
{
    return  "s:" + sid; 
}
// n:{nodeId}
std::string MetadataService::makeNodeKey(const std::string& nid)
{
    return  "n:" + nid; 
}
std::string MetadataService::makeGcTaskKey(const std::string& containerId)
{
    return  "g:" + std::to_string(time(nullptr)) + ":" + containerId;
}

std::string MetadataService::makeGcPrefix()
{
    return  "g:";
}

//GC任务
bool MetadataService::putGcTask(const std::string& containerId, uint32_t totalChunks)
{
    std::string key = makeGcTaskKey(containerId);
    std::string val = "{\"totalChunks\":" + std::to_string(totalChunks) + "}";
    return db_->Put(leveldb::WriteOptions(), key, val).ok();
}
std::vector<std::pair<std::string, uint32_t>> MetadataService::getGcTasks(
    int limit)
{
    std::vector<std::pair<std::string, uint32_t>> result;
    std::string prefix = makeGcPrefix();
    auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));

    for(it->Seek(prefix); it->Valid() && it->key().starts_with(prefix) && (int)result.size() < limit;
    it->Next())
    {
        std::string key = it->key().ToString();
        size_t p1 = key.find(':') + 1;
        size_t p2 = key.find(':', p1);
        std::string containerId = key.substr(p2 + 1);

        std::string json = it->value().ToString();
        uint32_t totalChunks = static_cast<uint32_t>(jsonGetNum(json, "totalChunks"));
        result.push_back({containerId, totalChunks});
    }
    return result;
}
bool MetadataService::deleteGcTask(const std::string& containerId)
{
    std::string prefix = makeGcPrefix();
    auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));

    for(it->Seek(prefix); 
    it->Valid() && it->key().starts_with(prefix);
    it->Next())
    {
        std::string key = it->key().ToString();
        size_t p1 = key.find(':') + 1;
        size_t p2 = key.find(':', p1);
        if(p2 != std::string::npos && key.substr(p2 + 1) == containerId)
        {
            return db_->Delete(leveldb::WriteOptions(), key).ok();
        }
    }
    return false;
}


// ============================================================
// JSON: chunkHashes 数组序列化
// ============================================================

std::string MetadataService::nodesToJson(const std::vector<std::string>& nodeIds) {
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < nodeIds.size(); ++i) {
        if (i > 0) oss << ",";
        oss << "\"" << jsonEscape(nodeIds[i]) << "\"";
    }
    oss << "]";
    return oss.str();
}

std::vector<std::string> MetadataService::nodesFromJson(const std::string& json) {
    std::vector<std::string> result;
    size_t pos = 0;
    while (pos < json.size()) {
        size_t start = json.find('"', pos);
        if (start == std::string::npos) break;
        size_t end = json.find('"', start + 1);
        if (end == std::string::npos) break;
        result.push_back(json.substr(start + 1, end - start - 1));
        pos = end + 1;
    }
    return result;
}


// ============================================================
// JSON: FileMetaV2 (含 chunkHashes 数组)
// ============================================================

std::string MetadataService::fileMetaToJson(const FileMetaV2& meta) {
    std::ostringstream oss;
    oss << "{"
        << "\"fileHash\":\"" << jsonEscape(meta.fileHash) << "\","
        << "\"fileName\":\"" << jsonEscape(meta.fileName) << "\","
        << "\"filePath\":\"" << jsonEscape(meta.filePath) << "\","
        << "\"fileSize\":" << meta.fileSize << ","
        << "\"chunkSize\":" << meta.chunkSize << ","
        << "\"totalChunks\":" << meta.totalChunks << ","
        << "\"chunkHashes\":[";
    for (size_t i = 0; i < meta.chunkHashes.size(); ++i) {
        if (i > 0) oss << ",";
        oss << "\"" << jsonEscape(meta.chunkHashes[i]) << "\"";
    }
    oss << "],"
        << "\"replicationFactor\":" << meta.replicationFactor << ","
        << "\"contentType\":\"" << jsonEscape(meta.contentType) << "\","
        << "\"thumbHash\":\"" << jsonEscape(meta.thumbHash) << "\","
        << "\"tags\":\"" << jsonEscape(meta.tags) << "\","
        << "\"aiSuggestions\":\"" << jsonEscape(meta.aiSuggestions) << "\","
        << "\"refCount\":" << meta.refCount << ","
        << "\"createdAt\":" << meta.createdAt << ","
        << "\"updatedAt\":" << meta.updatedAt
        << "}";
    return oss.str();
}

FileMetaV2 MetadataService::fileMetaFromJson(const std::string& json) {
    FileMetaV2 m;
    m.fileHash    = jsonGetStr(json, "fileHash");
    m.fileName    = jsonGetStr(json, "fileName");
    m.filePath    = jsonGetStr(json, "filePath");
    m.fileSize    = jsonGetNum(json, "fileSize");
    m.chunkSize   = static_cast<uint32_t>(jsonGetNum(json, "chunkSize"));
    m.totalChunks = static_cast<uint32_t>(jsonGetNum(json, "totalChunks"));
    m.replicationFactor = static_cast<uint32_t>(jsonGetNum(json, "replicationFactor"));
    m.contentType = jsonGetStr(json, "contentType");
    m.thumbHash   = jsonGetStr(json, "thumbHash");
    m.tags        = jsonGetStr(json, "tags");
    m.aiSuggestions = jsonGetStr(json, "aiSuggestions");
    m.refCount    = static_cast<int32_t>(jsonGetNum(json, "refCount"));
    m.createdAt   = static_cast<int64_t>(jsonGetNum(json, "createdAt"));
    m.updatedAt   = static_cast<int64_t>(jsonGetNum(json, "updatedAt"));

    // 解析 chunkHashes 数组
    size_t arr = json.find("\"chunkHashes\":[");
    if (arr != std::string::npos) {
        size_t pos = arr + 15;
        while (pos < json.size() && json[pos] != ']') {
            if (json[pos] == '"') {
                size_t end = json.find('"', pos + 1);
                if (end != std::string::npos) {
                    m.chunkHashes.push_back(json.substr(pos + 1, end - pos - 1));
                    pos = end + 1;
                    continue;
                }
            }
            pos++;
        }
    }
    return m;
}

// ============================================================
// JSON: SessionState (含 chunkHashes map 序列化)
// ============================================================

std::string MetadataService::sessionToJson(const SessionState& s) {
    std::ostringstream oss;
    oss << "{"
        << "\"sessionId\":\"" << jsonEscape(s.sessionId) << "\","
        << "\"fileName\":\"" << jsonEscape(s.fileName) << "\","
        << "\"dirPath\":\"" << jsonEscape(s.dirPath) << "\","
        << "\"fileSize\":" << s.fileSize << ","
        << "\"chunkSize\":" << s.chunkSize << ","
        << "\"totalChunks\":" << s.totalChunks << ","
        << "\"state\":" << (int)s.state << ","
        << "\"createdAt\":" << s.createdAt << ","
        << "\"lastActivity\":" << s.lastActivity << ","
        << "\"chunkHashes\":{";
    bool first = true;
    for (auto& [idx, hash] : s.chunkHashes) {
        if (!first) oss << ",";
        oss << "\"" << idx << "\":\"" << jsonEscape(hash) << "\"";
        first = false;
    }
    oss << "},"
        << "\"completedIndices\":[";
    first = true;
    for (auto idx : s.completedIndices) {
        if (!first) oss << ",";
        oss << idx;
        first = false;
    }
    oss << "]}";
    return oss.str();
}

SessionState MetadataService::sessionFromJson(const std::string& json) {
    SessionState s;
    s.sessionId    = jsonGetStr(json, "sessionId");
    s.fileName     = jsonGetStr(json, "fileName");
    s.dirPath      = jsonGetStr(json, "dirPath");
    s.fileSize     = jsonGetNum(json, "fileSize");
    s.chunkSize    = static_cast<uint32_t>(jsonGetNum(json, "chunkSize"));
    s.totalChunks  = static_cast<uint32_t>(jsonGetNum(json, "totalChunks"));
    s.state        = static_cast<SessionState::State>(jsonGetNum(json, "state"));
    s.createdAt    = static_cast<int64_t>(jsonGetNum(json, "createdAt"));
    s.lastActivity = static_cast<int64_t>(jsonGetNum(json, "lastActivity"));

    size_t hs = json.find("\"chunkHashes\":{");
    if (hs != std::string::npos) {
        size_t pos = hs + 14;
        while (pos < json.size() && json[pos] != '}') {
            if (json[pos] == '"') {
                size_t ke = json.find('"', pos + 1);
                std::string is = json.substr(pos + 1, ke - pos - 1);
                size_t vs = json.find('"', ke + 1);
                size_t ve = json.find('"', vs + 1);
                s.chunkHashes[(uint32_t)std::stoul(is)] = json.substr(vs + 1, ve - vs - 1);
                pos = ve + 1;
            } else pos++;
        }
    }
    size_t cs = json.find("\"completedIndices\":[");
    if (cs != std::string::npos) {
        size_t pos = cs + 20;
        while (pos < json.size() && json[pos] != ']') {
            if (json[pos] >= '0' && json[pos] <= '9') {
                uint32_t n = 0;
                while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9')
                    n = n * 10 + (json[pos++] - '0');
                s.completedIndices.insert(n);
            } else pos++;
        }
    }
    return s;
}

// ============================================================
// JSON: NodeInfo (含 capabilities 数组)
// ============================================================

std::string MetadataService::nodeInfoToJson(const NodeInfo& n) {
    std::ostringstream oss;
    oss << "{"
        << "\"nodeId\":\"" << jsonEscape(n.nodeId) << "\","
        << "\"address\":\"" << jsonEscape(n.address) << "\","
        << "\"totalBytes\":" << n.totalBytes << ","
        << "\"usedBytes\":" << n.usedBytes << ","
        << "\"online\":" << (n.online ? "true" : "false") << ","
        << "\"lastHeartbeat\":" << n.lastHeartbeat << ","
        << "\"httpPort\":" << n.httpPort << ","
        << "\"mkvPort\":" << n.mkvPort << ","
        << "\"capabilities\":[";
    for (size_t i = 0; i < n.capabilities.size(); ++i) {
        if (i > 0) oss << ",";
        oss << "\"" << jsonEscape(n.capabilities[i]) << "\"";
    }
    oss << "]}";
    return oss.str();
}

NodeInfo MetadataService::nodeInfoFromJson(const std::string& json) {
    NodeInfo n;
    n.nodeId    = jsonGetStr(json, "nodeId");
    n.address   = jsonGetStr(json, "address");
    n.totalBytes = jsonGetNum(json, "totalBytes");
    n.usedBytes  = jsonGetNum(json, "usedBytes");
    n.online     = (jsonGetStr(json, "online") == "true");
    n.lastHeartbeat = static_cast<int64_t>(jsonGetNum(json, "lastHeartbeat"));
    n.httpPort   = static_cast<int>(jsonGetNum(json, "httpPort"));
    n.mkvPort    = static_cast<int>(jsonGetNum(json, "mkvPort"));
    if (n.httpPort == 0) n.httpPort = 9002;
    if (n.mkvPort  == 0) n.mkvPort  = 9001;
    // capabilities
    size_t cs = json.find("\"capabilities\":[");
    if (cs != std::string::npos) {
        size_t pos = cs + 15;
        while (pos < json.size() && json[pos] != ']') {
            if (json[pos] == '"') {
                size_t end = json.find('"', pos + 1);
                if (end != std::string::npos) {
                    n.capabilities.push_back(json.substr(pos + 1, end - pos - 1));
                    pos = end + 1;
                    continue;
                }
            }
            pos++;
        }
    }
    return n;
}

// ============================================================
// JSON 辅助函数
// ============================================================

std::string MetadataService::jsonGetStr(const std::string& j, const std::string& f) {
    size_t p = j.find('"' + f + '"');
    if (p == std::string::npos) return "";
    p = j.find('"', p + f.size() + 3);
    if (p == std::string::npos) return "";
    size_t e = j.find('"', p + 1);
    if (e == std::string::npos) return "";
    return jsonUnescape(j.substr(p + 1, e - p - 1));
}

uint64_t MetadataService::jsonGetNum(const std::string& j, const std::string& f) {
    size_t p = j.find('"' + f + '"');
    if (p == std::string::npos) return 0;
    p = j.find(':', p + f.size() + 2);
    if (p == std::string::npos) return 0;
    uint64_t n = 0;
    while (++p < j.size() && j[p] >= '0' && j[p] <= '9') n = n * 10 + (j[p] - '0');
    return n;
}

std::string MetadataService::jsonEscape(const std::string& s) {
    std::string o;
    for (char c : s) {
        switch (c) {
            case '"': o += "\\\""; break;
            case '\\': o += "\\\\"; break;
            case '\n': o += "\\n"; break;
            case '\r': o += "\\r"; break;
            case '\t': o += "\\t"; break;
            default: o += c;
        }
    }
    return o;
}

std::string MetadataService::jsonUnescape(const std::string& s) {
    std::string o;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '\\' && i + 1 < s.size()) {
            switch (s[i + 1]) {
                case '"': o += '"'; break;
                case '\\': o += '\\'; break;
                case 'n': o += '\n'; break;
                case 'r': o += '\r'; break;
                case 't': o += '\t'; break;
                default: o += s[i + 1];
            }
            ++i;
        } else o += s[i];
    }
    return o;
}



}
}


































