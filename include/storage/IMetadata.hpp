#pragma once


#include "TypesV2.hpp"
#include <cstdint>
#include <utility>
#include <vector>
#include <string>



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

class IMetadata{
public:
    virtual ~IMetadata() = default;

    virtual bool putFileMeta(const FileMetaV2& meta) = 0;

    virtual bool getFileMeta(const std::string& fileHash,
                             FileMetaV2& out) = 0;

    virtual bool deleteFileMeta(const std::string& fileHash) = 0;

    virtual bool fileExists(const std::string& fileHash) = 0;
    //旧V2接口
    virtual bool putChunkMeta(const std::string& fileHash,
                              uint32_t chunkIndex,
                              const ChunkMeta& meta) = 0;

    virtual bool getChunkMeta(const std::string& fileHash,
                              uint32_t chunkIndex,
                               ChunkMeta& out) = 0;
    virtual std::vector<ChunkMeta> getFileChunks(
                const std::string& fileHash) = 0;    
                              
    virtual bool markChunkDeleted(const std::string& fileHash,
                                  uint32_t chunkIndex) = 0;

    virtual bool deleteChunkMeta(const std::string& fileHash,
                                 uint32_t chunkIndex) = 0;
    
    virtual std::vector<std::pair<std::string, uint32_t>>
            getDeletedChunks(int limit = 100) = 0;
    //Chunk 路由
    virtual bool putChunkRoute(const std::string& chunkHash,
        const std::vector<std::string>& nodeIds) = 0;

    // [用途] 读取 chunk 路由（下载时查出 chunk 在哪些节点上）
    virtual std::vector<std::string> getChunkRoute(
    const std::string& chunkHash) = 0;

    // [用途] 删除 chunk 路由（GC 清理完成后调用）
    virtual bool deleteChunkRoute(const std::string& chunkHash) = 0;

      // [用途] 全量扫描所有 chunk 路由（ReplicationMonitor 巡检用）
    // [输出] [{chunkHash, [nodeIds]}, ...]
    virtual std::vector<std::pair<std::string, std::vector<std::string>>>
        listAllChunkRoutes() = 0;

    // ============================================================
    // 副本修复任务（r: key space）— 巡检员防抖账本
    // ============================================================
    //
    // r:{timestamp}:{chunkHash} → {"assignedNode":"N3","sourceNode":"N1","deadline":1700000000}

    // [用途] 写修复任务（巡检员派单前记账防抖）
    virtual bool putRepairTask(const std::string& chunkHash,
                                const std::string& assignedNode,
                                const std::string& sourceNode,
                                int64_t deadline) = 0;

    // [用途] 查询某个 chunk 是否有进行中的修复任务
    // [输出] {assignedNode, sourceNode, deadline} 或空
    struct RepairTask {
        std::string assignedNode;
        std::string sourceNode;
        int64_t deadline = 0;
    };
    virtual RepairTask getRepairTask(const std::string& chunkHash) = 0;

    // [用途] 删除修复任务（修复完成或超时作废）
    virtual bool deleteRepairTask(const std::string& chunkHash) = 0;

    
    //上传会话
    virtual bool putSession(const SessionState& session) = 0;

    virtual bool getSession(const std::string& sessionId,
                            SessionState& out) = 0;
    
    virtual bool deleteSession(const std::string& sessionId) = 0;
    
    //获取过期的会话
    virtual std::vector<std::string> listExpiredSessions(int64_t ttlSeconds) = 0;
    
    virtual bool putNodeInfo(const NodeInfo& node) = 0;

    virtual bool getNodeInfo(const std::string& nodeId,
                             NodeInfo& out) = 0;
    
    virtual std::vector<NodeInfo> listOnlineNodes() = 0;
    
    virtual std::vector<NodeInfo> listAllNodes() = 0;
    //引用计数
    virtual bool incRefCount(const std::string& fileHash) = 0;

    virtual bool decRefCount(const std::string& fileHash) = 0;

    virtual int getRefCount(const std::string& fileHash) = 0;

    //GC 任务队列
    //containerId 容器的ID
    virtual bool putGcTask(const std::string& containerId, uint32_t totalChunks) = 0;

    virtual std::vector<std::pair<std::string, uint32_t>> getGcTasks(int limit = 100) = 0;

    virtual bool deleteGcTask(const std::string& containerId) = 0;
};

}



}








































