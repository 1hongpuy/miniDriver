#pragma once

#include <sstream>
#include <string>
#include <sys/types.h>
#include <vector>
#include <map>
#include <set>
#include <cstdint>

namespace miniKV {
namespace storage {

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



//ChunkLocation — chunk 的物理位置
struct ChunkLocation{
    std::string nodeId;
};


//ChunkMeta -- 单个 chunk 的元数据
//存储： c:{fileHash}:{chunkIndex}   → ChunkMeta JSON     每个chunk的物理位置
//
struct ChunkMeta{
   // uint32_t chunkIndex = 0;
    uint64_t size       = 0;
    std::string chunkHash  ;

    std::vector<ChunkLocation> replicas;
    enum Status{ACTIVE = 0, DELETED = 1};
    Status status = ACTIVE;
    int64_t createdAt = 0;
};

//FileMetaV2 v2版本的文件元数据
//存储：LevelDB key = f:{fileHash}
struct FileMetaV2{
    std::string fileHash;
    std::string fileName;
    std::string filePath;
    uint64_t    fileSize  = 0; //32位最大只有4GB
    uint32_t    chunkSize = 0; 
    uint32_t    totalChunks = 0;

    std::vector<std::string> chunkHashes;
    uint32_t    replicationFactor = 0;
    std::string contentType;
    std::string thumbHash; //缩略图 fileHash
    std::string tags;
    std::string aiSuggestions;
    int32_t     refCount = 1;//有符号可以方便进行这个负数判断
    int64_t     createdAt = 0;
    int64_t     updatedAt  = 0;   //4,294,967,295字节在2038年
};
    
//SessionState 上传会话
//存储：LevelDB key = s:{sessionId}
// 状态机：
//   CREATED → UPLOADING → COMMITTING → COMPLETED
//       │         │            │
//       └─────────┴────────────┴──→ CANCELLED（用户取消）
//       │         │
//       └─────────┴──────────────→ EXPIRED（超时，默认 24h）
struct SessionState{
    std::string sessionId;           //UUID,唯一ID
    std::string fileName;            //原始文件名
    std::string dirPath;

    uint64_t    fileSize = 0;
    uint32_t    chunkSize = 0;
    uint32_t    totalChunks = 0;

    enum State{ 
        CREATED,
        UPLOADING,
        COMMITTING,
        COMPLETED,
        CANCELLED,
        EXPIRED
    };
    State state = CREATED;

    std::map<uint32_t, std::string> chunkHashes;
    std::set<uint32_t>              completedIndices;

    int64_t createdAt = 0;            // unix秒
    int64_t lastActivity = 0;        // 最后活动时间   
};

// 5. NodeInfo — DataNode 节点信息
// 存储：LevelDB key = n:{nodeId}
struct NodeInfo {
    std::string nodeId;                 // 唯一标识
    std::string address;                // grpc地址
    uint64_t    totalBytes = 0;         // 磁盘总容量
    uint64_t    usedBytes  = 0;         // 已用容量
    bool        online     = false;     // 是否在线
    int64_t     lastHeartbeat  = 0;     // 最后心跳时间（unix）

    //节点订阅模式，单个节点都可以使用多种业务
    // "storage"     — 收 chunk 数据
    // "thumbnail"   — 生成缩略图
    // "ai_tag"      — AI 标签
    // "transcode"   — 视频转码
    std::vector<std::string> capabilities;
    int httpPort = 9002;
    int mkvPort  = 9001;
};

namespace V2Constants {

    constexpr uint32_t DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024;

    constexpr uint32_t SMALL_FILE_THRESHOLD = 4 * 1024 * 1024;

    constexpr int DEFAULT_REPLICATION_FACTOR = 2;

    constexpr int64_t SESSION_TTL_SECONDS = 86400;

    constexpr int64_t GC_INTERVAL_SECONDS = 300;

    constexpr int GC_BATCH_SIZE = 100; //最多清理的chunk数量

    constexpr int64_t HEARTBEAT_INTERVAL_SECONDS = 10; //S

    constexpr int64_t HEARTBEAT_TIMEOUT_SECONDS = 30; //S
}

}

}































