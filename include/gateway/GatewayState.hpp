#pragma once



#include <cstdint>
#include <string>
#include <mutex>
#include <map>
#include <memory>
#include <vector>

namespace leveldb {
    class DB;
}

namespace miniKV {
namespace gateway {

enum class NodeLiveState { kRecovering, kOnline, kSuspect, kOffline, kDraining };
//恢复或者注册， 上线， 亚健康，下线， 手动关闭
enum class FileState { kProtecting, kAvailable, kDegraded };
//文件切片完整正常，备份都有，  上传备份失败， 数据异常失去

struct NodeRecord { //节点静态数据
    std::string nodeId;
    std::string address;
    uint16_t httpPort = 9002;
    uint64_t maxStorageBytes = 0;
    uint64_t reservedBytes = 0; //安全红线存储空间
    uint32_t maxConcurrentWrites = 2;
    std::vector<std::string> capabilities;
};

struct NodeRuntime { //节点动态数据
    int64_t lastHeartbeatAt = 0; //时间搓
    uint64_t usedBytes      = 0;
    uint64_t freeBytes      = 0;
    double   cpuUsage       = 0; //利用率
    double   memoryUsage    = 0;
    double   diskIoUsage    = 0;
    double   netInMbps      = 0;
    double   netOutMbps     = 0;
    uint32_t activeUploads  = 0; //正在上传客户的个数
    uint32_t activeDownloads = 0;//正在下载客户的个数
    uint32_t activeReplicaWrites = 0; //正在拷贝的任务
    NodeLiveState state = NodeLiveState::kRecovering;
};

struct NodeSnapshot { NodeRecord record; NodeRuntime runtime; };

struct PlacementPlan { //临时写入计划
    uint32_t chunkIndex   = 0;
    std::string leaseId;
    uint64_t routeVersion = 0; //防止冲突版本号
    int64_t  expiresAt    = 0; //过期时间
    std::vector<NodeSnapshot> chain; //节点备份
};

struct ChunkRouteRequest { //客户端申请的清单
    uint32_t chunkIndex = 0;
    std::string chunkHash;
    uint64_t chunkSize = 0;
};

// Route planning failures have different client behavior. Invalid input must be
// corrected; temporary capacity exhaustion should be retried after a delay.
enum class RoutePlanStatus { kOk, kInvalidRequest, kNoCapacity };
enum class CommitChunkStatus { kCommitted, kAlreadyCommitted, kInvalidRequest };
enum class FileCommitStatus { kCommitted, kPathConflict, kInvalidRequest };

struct CompletedChunk { //最后写入的清单,会话层
    uint32_t index;
    std::string chunkHash;
    uint64_t size = 0;
};

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

struct ChunkRoute { //每个chunk的真实副本管理，写入真实的数据
    std::string chunkHash;
    uint64_t size = 0;
    uint32_t desiredReplicas = 2; //期望副本数
    std::vector<std::string> replicas;
    int64_t  updateAt = 0;
};

struct WriteLease {
    std::string leaseId;
    std::string requestKey;
    std::string sessionId;
    uint32_t chunkIndex = 0;
    std::string chunkHash;
    uint64_t chunkSize = 0;
    int64_t expiresAt = 0;
    PlacementPlan plan;
};

struct FileMeta {
    // This is populated when a session is committed.  It is deliberately not
    // stored in f:{fileHash}: a file hash describes content, while objectId
    // describes one logical entry in the user's directory tree.
    std::string objectId;
    std::string fileHash;
    std::string ownerId = "admin";
    std::string fileName;
    std::string dirPath;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    std::vector<std::string> chunkHashes;
    FileState state = FileState::kProtecting;
    int64_t createdAt = 0;
};

struct DirectoryMeta {
    std::string ownerId = "admin";
    std::string path;
    int64_t createdAt = 0;
};

struct ObjectMeta {
    std::string objectId;
    std::string ownerId = "admin";
    std::string parentPath;
    std::string name;
    std::string fileHash;
    uint64_t fileSize = 0;
    std::string contentType;
    FileState state = FileState::kProtecting;
    int64_t createdAt = 0;
};

struct Breadcrumb {
    std::string name;
    std::string path;
};

struct CatalogSnapshot {
    std::string path;
    std::vector<Breadcrumb> breadcrumbs;
    std::vector<DirectoryMeta> directories;
    std::vector<ObjectMeta> files;
};

struct ManifestSnapshot {
    FileMeta file;
    std::vector<ChunkRoute> routes;
    std::map<std::string, NodeRecord> nodes;
};

class GatewayState {
public:
    explicit GatewayState(const std::string& dbPath);
    ~GatewayState();
    bool open();

    bool registerNode(const NodeRecord& node);
    bool heartbeat(const std::string& nodeId, const NodeRuntime& runtime);
    void checkNodeTimeouts(int64_t now, int64_t suspectAfterSeconds = 20,
        int64_t offlineAfterSeconds = 30);
    std::vector<NodeSnapshot> nodes() const;

    bool createSession(const std::string& fileName, const std::string& dirPath,
        uint64_t fileSize, uint32_t chunkSize, SessionState& out);
    bool getSession(const std::string& sessionId, SessionState& out) const;
    RoutePlanStatus planRoutes(const std::string& sessionId,
                               const std::vector<ChunkRouteRequest>& requests,
                               std::vector<PlacementPlan>& out);
    CommitChunkStatus commitChunk(const std::string& sessionId, uint32_t index,
                                  const std::string& chunkHash, uint64_t size,
                                  const std::vector<std::string>& successfulNodes,
                                  const std::string& leaseId);
    bool releaseLease(const std::string& leaseId);
    FileCommitStatus commitFile(const std::string& sessionId, FileMeta& out);
    bool getFile(const std::string& fileHash, FileMeta& out) const;
    bool getRoute(const std::string& chunkHash, ChunkRoute& out) const;
    bool buildManifestSnapshot(const std::string& fileHash, ManifestSnapshot& out) const;
    bool createDirectory(const std::string& parentPath, const std::string& name,
                         DirectoryMeta* out = nullptr);
    bool listCatalog(const std::string& path, CatalogSnapshot& out) const;
    bool getObject(const std::string& objectId, ObjectMeta& out) const;


private:
    PlacementPlan selectPlacementLocked(const SessionState& session, uint32_t index);
    bool reserveLeaseLocked(const SessionState& session,
                            const ChunkRouteRequest& request,
                            PlacementPlan& plan,
                            int64_t now);
    void releaseLeaseLocked(const std::string& leaseId);
    void releaseExpiredLeasesLocked(int64_t now);
    void releaseLeasesForNodeLocked(const std::string& nodeId);
    bool persistSessionLocked(const SessionState& session);
    bool persistFileLocked(const FileMeta& file);
    bool persistRouteLocked(const ChunkRoute& route);
    bool persistDirectoryLocked(const DirectoryMeta& directory);
    bool persistObjectLocked(const ObjectMeta& object);
    bool loadSessionsLocked();
    bool loadFilesLocked();
    bool loadRoutesLocked();
    bool loadNodesLocked();
    bool loadDirectoriesLocked();
    bool loadObjectsLocked();
    bool backfillLegacyCatalogLocked();

    std::string dbPath_;
    std::unique_ptr<leveldb::DB> db_;
    mutable std::mutex mutex_; //可以再静态函数里面修改
    //内存索引，加速，不用一直查询leveldb，leveldb备份
    std::map<std::string, NodeRecord>   nodeRecords_;
    std::map<std::string, NodeRuntime>  nodeRuntime_;
    std::map<std::string, SessionState> sessions_;
    std::map<std::string, FileMeta>     files_;
    std::map<std::string, ChunkRoute>   routes_;
    std::map<std::string, DirectoryMeta> directories_;
    std::map<std::string, ObjectMeta> objects_;
    std::map<std::string, std::string> objectByPath_;
    std::map<std::string, WriteLease> leases_;
    std::map<std::string, std::string> leaseByRequestKey_;
    std::map<std::string, uint32_t> reservedWritesByNode_;
    std::map<std::string, uint64_t> reservedBytesByNode_;
};



}
}































