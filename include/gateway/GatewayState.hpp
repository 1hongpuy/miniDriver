#pragma once

#include "gateway/LruCache.hpp"
#include "control/ObjectReadTypes.hpp"
#include "media/AiIndexEvent.hpp"
#include "media/MediaJob.hpp"
#include "metadata/MetadataClient.hpp"

#include <cstdint>
#include <cstddef>
#include <chrono>
#include <string>
#include <mutex>
#include <shared_mutex>
#include <array>
#include <map>
#include <memory>
#include <vector>

namespace leveldb {
    class DB;
}

namespace miniKV {
namespace gateway {

void recordGatewayMutexSample(const char* label, uint64_t waitUs, uint64_t holdUs);
void recordGatewayMutexInterval(std::chrono::steady_clock::time_point start,
                                std::chrono::steady_clock::time_point end);
void recordGatewayLevelDbSample(const char* label, uint64_t durationUs);
// Reset/flush process-wide diagnostics so an external benchmark can delimit
// one exact steady-state window without restarting the Gateway.
void resetGatewayMutexDiagnostics();
// Emits a cumulative snapshot when mutex diagnostics are enabled.  This is
// intentionally process-wide so every GatewayState::mutex_ acquire site is
// accounted for, including maintenance and read-plan paths.
void flushGatewayMutexDiagnostics();

enum class GatewayLockMode { kGlobal, kSharded };

// The first sharding generation deliberately has only two business domains.
// Route/lease/chunk state follows the owning upload session and catalog/cache
// state follows the owning object/namespace shard.  Keeping this primitive in
// the public header makes lock ownership auditable without exposing internals.
class GatewayLockManager {
public:
    static constexpr size_t kShardCount = 64;

    explicit GatewayLockManager(GatewayLockMode mode = GatewayLockMode::kGlobal)
        : mode_(mode) {}

    GatewayLockMode mode() const noexcept { return mode_; }
    static GatewayLockMode modeFromEnvironment();
    static const char* modeName(GatewayLockMode mode) noexcept;
    size_t sessionShard(const std::string& sessionId) const noexcept;
    size_t objectShard(const std::string& canonicalKey) const noexcept;
    std::mutex& global() noexcept { return globalMutex_; }
    std::mutex& lifecycle() noexcept { return lifecycleMutex_; }
    std::mutex& session(size_t shard) noexcept { return sessionMutexes_[shard % kShardCount]; }
    std::mutex& object(size_t shard) noexcept { return objectMutexes_[shard % kShardCount]; }
    std::shared_mutex& nodes() noexcept { return nodeRegistryMutex_; }

private:
    GatewayLockMode mode_;
    std::mutex globalMutex_;
    std::mutex lifecycleMutex_;
    std::array<std::mutex, kShardCount> sessionMutexes_{};
    std::array<std::mutex, kShardCount> objectMutexes_{};
    std::shared_mutex nodeRegistryMutex_;
};

class GatewayMutexGuard {
public:
    GatewayMutexGuard(std::mutex& mutex, const char* label)
        : mutex_(mutex), label_(label), requestedAt_(std::chrono::steady_clock::now()) {
        mutex_.lock();
        lockedAt_ = std::chrono::steady_clock::now();
    }
    ~GatewayMutexGuard() {
        const auto releasedAt = std::chrono::steady_clock::now();
        recordGatewayMutexSample(label_,
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(lockedAt_ - requestedAt_).count()),
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(releasedAt - lockedAt_).count()));
        recordGatewayMutexInterval(lockedAt_, releasedAt);
        mutex_.unlock();
    }
    GatewayMutexGuard(const GatewayMutexGuard&) = delete;
    GatewayMutexGuard& operator=(const GatewayMutexGuard&) = delete;
private:
    std::mutex& mutex_;
    const char* label_;
    std::chrono::steady_clock::time_point requestedAt_;
    std::chrono::steady_clock::time_point lockedAt_;
};

// Acquires two shard mutexes in a deterministic shard-number order.  Cross
// domain operations must use this helper instead of spelling out lock order in
// business code; accepting the arguments in either order is the ABBA safety
// property exercised by test_gateway_lock_manager.
class GatewayShardMultiLock {
public:
    GatewayShardMultiLock(std::mutex& first, size_t firstShard,
                          std::mutex& second, size_t secondShard)
    {
        if (&first == &second || firstShard == secondShard) {
            firstLock_ = std::unique_lock<std::mutex>(first);
            return;
        }
        if (firstShard < secondShard) {
            firstLock_ = std::unique_lock<std::mutex>(first);
            secondLock_ = std::unique_lock<std::mutex>(second);
        } else {
            firstLock_ = std::unique_lock<std::mutex>(second);
            secondLock_ = std::unique_lock<std::mutex>(first);
        }
    }

    GatewayShardMultiLock(const GatewayShardMultiLock&) = delete;
    GatewayShardMultiLock& operator=(const GatewayShardMultiLock&) = delete;

private:
    std::unique_lock<std::mutex> firstLock_;
    std::unique_lock<std::mutex> secondLock_;
};

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
    std::string identityScheme = "cas-sha256";
    std::string chunkId;
    std::string checksumType = "sha256";
    std::string checksumDigest;
    uint64_t objectVersion = 1;
    std::vector<NodeSnapshot> chain; //节点备份
};

struct ChunkRouteRequest { //客户端申请的清单
    uint32_t chunkIndex = 0;
    std::string chunkHash;
    uint64_t chunkSize = 0;
    std::string identityScheme = "cas-sha256";
    std::string checksumType = "sha256";
    std::string checksumDigest;
};

// Route planning failures have different client behavior. Invalid input must be
// corrected; temporary capacity exhaustion should be retried after a delay.
enum class RoutePlanStatus { kOk, kInvalidRequest, kNoCapacity };
enum class CommitChunkStatus { kCommitted, kAlreadyCommitted, kInvalidRequest };
enum class FileCommitStatus { kCommitted, kPathConflict, kInvalidRequest };
enum class DeleteStatus { kDeleted, kNotFound, kInvalidRequest };
enum class PreflightStatus { kUploadRequired, kContentExists, kPathConflict, kInvalidRequest };

struct CompletedChunk { //最后写入的清单,会话层
    uint32_t index;
    std::string chunkHash;
    uint64_t size = 0;
};

struct SessionState {
    std::string sessionId;
    // Allocate logical identity before issuing any Chunk route. Retries after
    // a Gateway restart must keep both object and physical Chunk identities.
    std::string objectId;
    uint64_t objectVersion = 1;
    uint64_t metadataVersion = 1;
    std::string ownerId = "admin";
    std::string fileName;
    std::string dirPath;
    uint64_t fileSize  = 0;
    uint32_t chunkSize = 4 * 1024 * 1024;
    uint32_t totalChunks = 0;
    // Remote MetadataService sessions expose the aggregate completion count;
    // the Gateway compatibility view does not materialize every completed
    // chunk, so keep that count separately from the local completed map.
    uint32_t completedChunks = 0;
    // New sessions bind this canonical content identity before any bytes are sent.
    // Empty means a legacy session created by the original V2 API.
    std::string manifestHash;
    // Non-empty only for a private object produced by an internal media job.
    // Browser sessions leave these fields empty and retain the existing catalog semantics.
    std::string derivedJobId;
    std::string derivedProfile;
    std::map<uint32_t, CompletedChunk>   completed;
    int64_t createdAt = 0;
    int64_t lastActivityAt = 0;
};

struct ChunkRoute { //每个chunk的真实副本管理，写入真实的数据
    std::string chunkHash;
    std::string chunkId;
    std::string identityScheme = "cas-sha256";
    std::string checksumType = "sha256";
    std::string checksumDigest;
    uint64_t objectVersion = 1;
    uint64_t generation = 0;
    uint64_t size = 0;
    uint32_t desiredReplicas = 2; //期望副本数
    std::vector<std::string> replicas;
    int64_t  updateAt = 0;
};

// A lease is also the in-memory reservation token for the node write slots.
// The token is deliberately runtime-only (leases are not persisted): its
// unique id gives retries/cleanup an idempotent handle, while the terminal
// state makes the exactly-once release rule explicit to ownership audits.
struct ReservationToken {
    enum class State { kReserved, kCommitted, kRolledBack };
    std::string tokenId;
    std::vector<std::string> nodeIds;
    uint64_t bytes = 0;
    State state = State::kReserved;
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
    ReservationToken reservation;
};

struct FileMeta {
    // This is populated when a session is committed.  It is deliberately not
    // stored in f:{fileHash}: a file hash describes content, while objectId
    // describes one logical entry in the user's directory tree.
    std::string objectId;
    uint64_t objectVersion = 1;
    uint64_t metadataVersion = 1;
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
    uint64_t objectVersion = 1;
    uint64_t metadataVersion = 1;
    std::string ownerId = "admin";
    std::string parentPath;
    std::string name;
    std::string fileHash;
    uint64_t fileSize = 0;
    std::string contentType;
    FileState state = FileState::kProtecting;
    int64_t createdAt = 0;
};

struct UploadPreflightRequest {
    // Optional stable client idempotency key.  In raft mode it is used to
    // derive the session/object ids and the CreateSession command id.
    std::string commandId;
    std::string fileName;
    std::string dirPath;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    std::string manifestHash;
    std::vector<ChunkRouteRequest> chunks;
};

struct UploadPreflightResult {
    SessionState session;
    ObjectMeta object;
    std::vector<ChunkRouteRequest> presentChunks;
    std::vector<ChunkRouteRequest> missingChunks;
};

struct DerivedUploadRequest {
    std::string fileName;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    std::string manifestHash;
    std::vector<ChunkRouteRequest> chunks;
};

struct DerivedUploadResult {
    SessionState session;
    std::vector<ChunkRouteRequest> presentChunks;
    std::vector<ChunkRouteRequest> missingChunks;
};

using ObjectMetaCache = LruCache<std::string, ObjectMeta>;

struct Breadcrumb {
    std::string name;
    std::string path;
};

struct CatalogSnapshot {
    std::string path;
    std::vector<Breadcrumb> breadcrumbs;
    std::vector<DirectoryMeta> directories;
    std::vector<ObjectMeta> files;
    std::map<std::string, media::ThumbnailMeta> thumbnailsByFileHash;
    std::map<std::string, media::ThumbnailMeta> previewsByFileHash;
};

struct ManifestSnapshot {
    FileMeta file;
    std::vector<ChunkRoute> routes;
    std::map<std::string, NodeRecord> nodes;
};

using FileMetaCache = LruCache<std::string, FileMeta>;
using ChunkRouteCache = LruCache<std::string, ChunkRoute>;
using CatalogCache = LruCache<std::string, CatalogSnapshot>;
using ManifestCache = LruCache<std::string, ManifestSnapshot>;

struct MetadataCacheUsage {
    size_t objectEntries = 0;
    size_t fileEntries = 0;
    size_t routeEntries = 0;
    size_t catalogEntries = 0;
    size_t manifestEntries = 0;
};

struct DeleteTaskSnapshot {
    std::string chunkHash;
    std::string storageIdentity;
    std::vector<std::string> pendingNodeIds;
};

struct DeleteTask {
    std::string chunkHash;
    std::string storageIdentity;
    std::vector<std::string> pendingNodeIds;
    int64_t createdAt = 0;
    int64_t updatedAt = 0;
};

struct ThumbnailEnqueueResult {
    media::MediaJob job;
    bool publishRequired = false;
};

// Per-request diagnostic decomposition for Gateway mutations.  It is passed
// only by observability callers and has no effect on metadata semantics.
struct GatewayMutationTiming {
    uint64_t mutexWaitUs = 0;
    uint64_t criticalSectionUs = 0;
    uint64_t levelDbWriteUs = 0;
};

class GatewayState {
public:
    explicit GatewayState(const std::string& dbPath, uint32_t replicationFactor = 2,
                          GatewayLockMode lockMode = GatewayLockManager::modeFromEnvironment());
    ~GatewayState();
    bool open();
    // In raft mode Gateway does not open a local metadata LevelDB.  The
    // adapter is deliberately opt-in so legacy deployments keep their exact
    // behavior until the core upload/read path is migrated.
    void configureRemoteMetadata(std::shared_ptr<metadata::MetadataClient> client);
    bool remoteMetadataEnabled() const noexcept { return static_cast<bool>(remoteMetadata_); }
    GatewayLockMode lockMode() const noexcept { return locks_.mode(); }
    static constexpr size_t kLockShardCount = GatewayLockManager::kShardCount;

    bool registerNode(const NodeRecord& node);
    bool heartbeat(const std::string& nodeId, const NodeRuntime& runtime);
    void checkNodeTimeouts(int64_t now, int64_t suspectAfterSeconds = 20,
        int64_t offlineAfterSeconds = 30);
    std::vector<NodeSnapshot> nodes() const;

    bool createSession(const std::string& fileName, const std::string& dirPath,
        uint64_t fileSize, uint32_t chunkSize, SessionState& out,
        GatewayMutationTiming* timing = nullptr);
    // The browser calls this after hashing its fixed-size chunks. It either
    // rejects the logical path, links an existing content object, or creates a
    // resumable session containing only the chunks that still need transfer.
    PreflightStatus preflightUpload(const UploadPreflightRequest& request,
                                    UploadPreflightResult& out);
    bool createDerivedUpload(const std::string& jobId, const std::string& leaseToken,
                             const DerivedUploadRequest& request, DerivedUploadResult& out);
    static std::string manifestHash(uint64_t fileSize, uint32_t chunkSize,
                                    const std::vector<ChunkRouteRequest>& chunks);
    bool getSession(const std::string& sessionId, SessionState& out) const;
    RoutePlanStatus planRoutes(const std::string& sessionId,
                               const std::vector<ChunkRouteRequest>& requests,
                               std::vector<PlacementPlan>& out,
                               GatewayMutationTiming* timing = nullptr);
    CommitChunkStatus commitChunk(const std::string& sessionId, uint32_t index,
                                  const std::string& chunkHash, uint64_t size,
                                  const std::vector<std::string>& successfulNodes,
                                  const std::string& leaseId,
                                  GatewayMutationTiming* timing = nullptr);
    bool releaseLease(const std::string& leaseId);
    FileCommitStatus commitFile(const std::string& sessionId, FileMeta& out,
                                GatewayMutationTiming* timing = nullptr);
    FileCommitStatus commitDerivedUpload(const std::string& jobId,
                                         const std::string& leaseToken,
                                         const std::string& sessionId, FileMeta& out);
    bool getFile(const std::string& fileHash, FileMeta& out) const;
    bool getRoute(const std::string& chunkHash, ChunkRoute& out) const;
    bool buildManifestSnapshot(const std::string& fileHash, ManifestSnapshot& out) const;
    bool buildObjectReadDescriptor(const std::string& objectId,
                                   control::ObjectReadDescriptor& out) const;
    bool createDirectory(const std::string& parentPath, const std::string& name,
                         DirectoryMeta* out = nullptr);
    bool listCatalog(const std::string& path, CatalogSnapshot& out) const;
    bool getObject(const std::string& objectId, ObjectMeta& out) const;
    ObjectMetaCache::Stats objectCacheStats() const;
    CatalogCache::Stats catalogCacheStats() const;
    ManifestCache::Stats manifestCacheStats() const;
    MetadataCacheUsage metadataCacheUsage() const;
    // expectedObjectVersion==0 preserves legacy callers. V3 control APIs
    // pass a concrete version so a stale lifecycle request cannot delete a
    // newer logical object incarnation.
    DeleteStatus deleteObject(const std::string& objectId, uint64_t expectedObjectVersion = 0);
    DeleteStatus deleteDirectory(const std::string& path);
    std::vector<DeleteTaskSnapshot> pendingDeletesForNode(const std::string& nodeId) const;
    bool acknowledgeDelete(const std::string& chunkHash, const std::string& nodeId);

    ThumbnailEnqueueResult enqueueThumbnail(const std::string& sourceFileHash,
                                            const std::string& profile, int64_t now);
    bool getMediaJob(const std::string& jobId, media::MediaJob& out) const;
    bool getThumbnail(const std::string& sourceFileHash, const std::string& profile,
                      media::ThumbnailMeta& out) const;
    bool claimMediaJob(const std::string& jobId, int64_t now, int64_t leaseSeconds,
                       media::MediaJob& out);
    bool completeMediaJob(const std::string& jobId, const std::string& leaseToken,
                          const std::string& derivedObjectId,
                          const std::string& derivedFileHash, int64_t now);
    bool failMediaJob(const std::string& jobId, const std::string& leaseToken,
                      bool unsupported, const std::string& error, int64_t nextRetryAt,
                      int64_t now);
    // Records that a PENDING job has been handed to the bounded Redis publisher.
    // A worker may still claim it immediately; this only rate-limits recovery scans.
    bool deferMediaJobDispatch(const std::string& jobId, int64_t nextDispatchAt);
    std::vector<media::MediaJob> dueMediaJobs(int64_t now, size_t maxJobs) const;

    // AI index events are written in the same LevelDB batch as File Commit.
    // Redis publication is at-least-once; consumers must de-duplicate by
    // eventId and objectId/objectVersion.
    std::vector<media::AiIndexEvent> dueAiIndexEvents(int64_t now, size_t maxEvents) const;
    bool markAiIndexEventPublished(const std::string& eventId, int64_t now);


private:
    uint32_t replicationFactor_ = 2;
    PlacementPlan selectPlacementLocked(const SessionState& session, uint32_t index);
    bool reserveLeaseLocked(const SessionState& session,
                            const ChunkRouteRequest& request,
                            PlacementPlan& plan,
                            int64_t now);
    bool releaseLeaseLocked(const std::string& leaseId,
                            ReservationToken::State terminalState = ReservationToken::State::kRolledBack);
    void releaseExpiredLeasesLocked(int64_t now);
    void releaseLeasesForNodeLocked(const std::string& nodeId);
    bool persistSessionLocked(const SessionState& session);
    bool persistRouteLocked(const ChunkRoute& route);
    bool persistDirectoryLocked(const DirectoryMeta& directory);
    bool getFileLocked(const std::string& fileHash, FileMeta& out) const;
    bool getRouteLocked(const std::string& chunkHash, ChunkRoute& out) const;
    bool getDirectoryLocked(const std::string& ownerId, const std::string& path,
                            DirectoryMeta& out) const;
    bool getObjectLocked(const std::string& objectId, ObjectMeta& out) const;
    bool getMediaJobLocked(const std::string& jobId, media::MediaJob& out) const;
    bool getThumbnailLocked(const std::string& sourceFileHash, const std::string& profile,
                            media::ThumbnailMeta& out) const;
    bool objectIdAtPathLocked(const std::string& pathKey, std::string& objectId) const;
    bool buildCatalogSnapshotLocked(const std::string& normalized, CatalogSnapshot& out) const;
    bool loadDeleteTasksLocked();
    DeleteStatus deleteCatalogEntriesLocked(const std::vector<std::string>& objectIds,
                                            const std::vector<std::string>& directoryKeys);
    bool loadSessionsLocked();
    bool loadNodesLocked();
    bool backfillLegacyCatalogLocked();
    FileCommitStatus createObjectLinkLocked(const std::string& fileName,
                                            const std::string& dirPath,
                                            const FileMeta& file,
                                            ObjectMeta& out);

    std::mutex& sessionMutex(const std::string& sessionId) const noexcept;
    std::mutex& objectMutex(const std::string& canonicalKey) const noexcept;
    size_t leaseShard(const std::string& leaseId) const noexcept;
    bool useShardedLocks() const noexcept { return locks_.mode() == GatewayLockMode::kSharded; }

    std::string dbPath_;
    std::unique_ptr<leveldb::DB> db_;
    std::shared_ptr<metadata::MetadataClient> remoteMetadata_;
    mutable std::map<std::string, uint64_t> remoteNodeEpochs_;
    mutable std::map<std::string, std::string> remoteBootIds_;
    mutable GatewayLockManager locks_;
    // Compatibility alias for the global mode and for the not-yet-migrated
    // maintenance/media APIs. Core upload paths select Session/Object shards.
    std::mutex& mutex_ = locks_.global();
    mutable ObjectMetaCache objectCache_;
    mutable FileMetaCache fileCache_;
    mutable ChunkRouteCache routeCache_;
    mutable CatalogCache catalogCache_;
    mutable ManifestCache manifestCache_;
    //内存索引，加速，不用一直查询leveldb，leveldb备份
    std::map<std::string, NodeRecord>   nodeRecords_;
    std::map<std::string, NodeRuntime>  nodeRuntime_;
    std::map<std::string, SessionState> sessions_;
    std::map<std::string, DeleteTask> deleteTasks_;
    // In sharded mode lease state follows the owning session shard. Lease IDs
    // carry the shard prefix so release/timeout paths can find the owner
    // without a process-wide lookup mutex. Global mode keeps the legacy maps.
    std::map<std::string, WriteLease> leases_;
    std::map<std::string, std::string> leaseByRequestKey_;
    std::array<std::map<std::string, WriteLease>, GatewayLockManager::kShardCount>
        shardedLeases_;
    std::array<std::map<std::string, std::string>, GatewayLockManager::kShardCount>
        shardedLeaseByRequestKey_;
    std::map<std::string, uint32_t> reservedWritesByNode_;
    std::map<std::string, uint64_t> reservedBytesByNode_;
};



}
}
