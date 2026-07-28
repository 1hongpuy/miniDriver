#pragma once


#include "IMetadata.hpp"
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <leveldb/iterator.h>
#include <memory>
#include <mutex>



namespace miniKV {
namespace storage {

class MetadataService : public IMetadata {
public:
        // ---- 构造/析构/初始化 ----

    // [用途] 构造函数，只记录路径，不打开数据库
    // [输入] dbPath — LevelDB 数据目录路径
    explicit MetadataService(const std::string& dbPath);

    // [用途] 打开 LevelDB，创建必要目录
    // [输出] true=成功
    bool init();

    // ============================================================
    // 文件元数据（f: keys）
    // ============================================================

    bool putFileMeta(const FileMetaV2& meta) override;
    bool getFileMeta(const std::string& fileHash, FileMetaV2& out) override;
    bool deleteFileMeta(const std::string& fileHash) override;
    bool fileExists(const std::string& fileHash) override;

    // ============================================================
    // Chunk 元数据（c: keys）
    // ============================================================

    bool putChunkMeta(const std::string& fileHash, uint32_t chunkIndex,
                      const ChunkMeta& meta) override;
    bool getChunkMeta(const std::string& fileHash, uint32_t chunkIndex,
                      ChunkMeta& out) override;
    std::vector<ChunkMeta> getFileChunks(
        const std::string& fileHash) override;
    bool markChunkDeleted(const std::string& fileHash,
                          uint32_t chunkIndex) override;
    bool deleteChunkMeta(const std::string& fileHash,
                         uint32_t chunkIndex) override;
    std::vector<std::pair<std::string, uint32_t>> getDeletedChunks(
        int limit = 100) override;

    // Chunk 路由 (c:{chunkHash})
    bool putChunkRoute(const std::string& chunkHash,
            const std::vector<std::string>& nodeIds) override;
    std::vector<std::string> getChunkRoute(const std::string& chunkHash) override;
    bool deleteChunkRoute(const std::string& chunkHash) override;


    std::vector<std::pair<std::string, std::vector<std::string>>>
    listAllChunkRoutes() override;

    // 副本修复任务 (r:{timestamp}:{chunkHash})
    bool putRepairTask(const std::string& chunkHash,
                    const std::string& assignedNode,
                    const std::string& sourceNode,
                    int64_t deadline) override;
    RepairTask getRepairTask(const std::string& chunkHash) override;
    bool deleteRepairTask(const std::string& chunkHash) override;

    // ============================================================
    // 上传会话（s: keys）
    // ============================================================

    bool putSession(const SessionState& session) override;
    bool getSession(const std::string& sessionId,
                    SessionState& out) override;
    bool deleteSession(const std::string& sessionId) override;
    std::vector<std::string> listExpiredSessions(
        int64_t ttlSeconds) override;

    // ============================================================
    // 节点管理（n: keys）
    // ============================================================

    bool putNodeInfo(const NodeInfo& node) override;
    bool getNodeInfo(const std::string& nodeId, NodeInfo& out) override;
    std::vector<NodeInfo> listOnlineNodes() override;
    std::vector<NodeInfo> listAllNodes() override;

    // ============================================================
    // 引用计数
    // ============================================================

    bool incRefCount(const std::string& fileHash) override;
    bool decRefCount(const std::string& fileHash) override;
    int  getRefCount(const std::string& fileHash) override;


    
    // ============================================================
    // GC 任务队列（g: keys）— 统一容器 GC
    // ============================================================

    bool putGcTask(const std::string& containerId, uint32_t totalChunks) override;
    std::vector<std::pair<std::string, uint32_t>> getGcTasks(
        int limit = 100) override;
    bool deleteGcTask(const std::string& containerId) override;

private:
    // [用途] 对 FileMetaV2 做 JSON 序列化
    static std::string fileMetaToJson(const FileMetaV2& meta);

    // [用途] 从 JSON 反序列化 FileMetaV2
    static FileMetaV2 fileMetaFromJson(const std::string& json);

    // [用途] 对 ChunkMeta 做 JSON 序列化
    static std::string chunkMetaToJson(const ChunkMeta& meta);

    // [用途] 从 JSON 反序列化 ChunkMeta
    static ChunkMeta chunkMetaFromJson(const std::string& json);

    // [用途] 对 SessionState 做 JSON 序列化
    static std::string sessionToJson(const SessionState& s);

    // [用途] 从 JSON 反序列化 SessionState
    static SessionState sessionFromJson(const std::string& json);

    // [用途] 对 NodeInfo 做 JSON 序列化
    static std::string nodeInfoToJson(const NodeInfo& n);

    // [用途] 从 JSON 反序列化 NodeInfo
    static NodeInfo nodeInfoFromJson(const std::string& json);

    static std::string nodesToJson(const std::vector<std::string>& nodeIds);
    static std::vector<std::string> nodesFromJson(const std::string& json);


    // ---- Key 构造 ----
    // f:{hash}
    static std::string makeFileKey(const std::string& hash);
    // c:{hash}:{index}
    static std::string makeChunkKey(const std::string& chunkhash);
    // c:{hash}: 前缀（用于扫描一个文件的所有 chunk）
    static std::string makeChunkPrefix(const std::string& hash);
    static std::string makeRepairTaskKey(const std::string& chunkHash);
    static std::string makeRepairPrefix();
    // s:{sessionId}
    static std::string makeSessionKey(const std::string& sid);
    // n:{nodeId}
    static std::string makeNodeKey(const std::string& nid);
    
    // g:{nodeId}
    static std::string makeGcTaskKey(const std::string& containerId);
    static std::string makeGcPrefix();

    // ---- JSON 辅助（和 V1 FileMetaStorage 相同实现） ----
    // 从 JSON 字符串中提取 field 对应的字符串值
    static std::string jsonGetStr(const std::string& json,
                                    const std::string& field);
    // 从 JSON 字符串中提取 field 对应的数值
    static uint64_t jsonGetNum(const std::string& json,
                                const std::string& field);
    // JSON 字符串转义
    static std::string jsonEscape(const std::string& s);
    static std::string jsonUnescape(const std::string& s);

    // ---- 成员 ----
    std::string dbPath_;
    std::unique_ptr<leveldb::DB> db_;

};

}

}
























