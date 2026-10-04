#pragma once

#include "metadata/MetadataStateMachine.hpp"
#include "metadata/SnapshotStore.hpp"
#include "metadata/MetadataService.hpp"

#include <libnuraft/nuraft.hxx>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace miniKV::metadata::raft {

struct StaticMember {
    int id = 0;
    std::string endpoint;
};

class DurableRaftStorage {
public:
    explicit DurableRaftStorage(const std::string& directory);
    ~DurableRaftStorage();
    DurableRaftStorage(const DurableRaftStorage&) = delete;
    DurableRaftStorage& operator=(const DurableRaftStorage&) = delete;

    bool get(const std::string& key, std::string& value) const;
    void putSync(const std::string& key, const std::string& value);
    void writeSync(leveldb::WriteBatch& batch);
    leveldb::DB* db() const { return db_.get(); }
    const std::string& directory() const { return directory_; }

private:
    std::string directory_;
    std::unique_ptr<leveldb::DB> db_;
};

class DurableLogStore final : public nuraft::log_store {
public:
    explicit DurableLogStore(std::shared_ptr<DurableRaftStorage> storage,
                             std::string walDirectory = {});
    ~DurableLogStore() override;

    nuraft::ulong next_slot() const override;
    nuraft::ulong start_index() const override;
    nuraft::ptr<nuraft::log_entry> last_entry() const override;
    nuraft::ulong append(nuraft::ptr<nuraft::log_entry>& entry) override;
    void end_of_append_batch(nuraft::ulong start, nuraft::ulong cnt) override;
    void write_at(nuraft::ulong index,
                  nuraft::ptr<nuraft::log_entry>& entry) override;
    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
        log_entries(nuraft::ulong start, nuraft::ulong end) override;
    nuraft::ptr<std::vector<nuraft::ptr<nuraft::log_entry>>>
        log_entries_ext(nuraft::ulong start, nuraft::ulong end,
                        nuraft::int64 byteHint = 0) override;
    nuraft::ptr<nuraft::log_entry> entry_at(nuraft::ulong index) override;
    nuraft::ulong term_at(nuraft::ulong index) override;
    nuraft::ptr<nuraft::buffer> pack(nuraft::ulong index,
                                     nuraft::int32 count) override;
    void apply_pack(nuraft::ulong index, nuraft::buffer& pack) override;
    bool compact(nuraft::ulong lastLogIndex) override;
    bool flush() override;
    nuraft::ulong last_durable_index() override;

    // NuRaft's parallel-log-appending mode lets replication overlap the
    // leader's local fdatasync.  The commit index is still gated by
    // last_durable_index(), and followers wait for the same boundary before
    // acknowledging the append, so this only changes scheduling, not the
    // durability contract.
    void enableAsyncAppend(bool enabled) noexcept;
    // Optional short coalescing window for independent append batches. A
    // value of zero preserves immediate sync scheduling. The window only
    // delays fdatasync; durableIndex_ advances only after the sync succeeds.
    void setSyncCoalesceUs(uint64_t microseconds) noexcept;
    void setAppendCompletionCallback(std::function<void(bool, uint64_t)> callback);
    void setAppendBatchMetricsCallback(std::function<void(uint64_t, uint64_t, uint64_t)> callback);

private:
    static std::string logKey(nuraft::ulong index);
    static std::string encodeU64(uint64_t value);
    static uint64_t decodeU64(const std::string& value, uint64_t fallback);
    static std::string serializeEntry(nuraft::ptr<nuraft::log_entry>& entry);
    static nuraft::ptr<nuraft::log_entry> deserializeEntry(const std::string& bytes);
    nuraft::ptr<nuraft::log_entry> dummy() const;
    static uint64_t checksumRecord(const std::string& bytes);
    static std::string makeRecord(nuraft::ulong index, const std::string& bytes);
    bool loadLogFile();
    bool flushPendingRecordsLocked();
    bool rewriteLogLocked();
    bool syncLogLocked();
    void syncWorkerLoop();
    bool waitForDurable(nuraft::ulong target);

    std::shared_ptr<DurableRaftStorage> storage_;
    mutable std::mutex mutex_;
    int logFd_ = -1;
    std::string logPath_;
    std::map<nuraft::ulong, std::string> entries_;
    nuraft::ulong startIndex_ = 1;
    nuraft::ulong nextIndex_ = 1;
    nuraft::ulong durableIndex_ = 0;
    nuraft::ulong pendingDurableIndex_ = 0;
    // NuRaft calls append() once per log entry and closes the group with
    // end_of_append_batch(). Keep the encoded records until that boundary so
    // the whole group can be emitted with one writev syscall. This changes no
    // durability boundary: fdatasync is still issued only after the group is
    // fully written and the durable index is advanced only after sync.
    std::vector<std::string> pendingRecordBytes_;
    bool asyncAppendEnabled_ = false;
    uint64_t syncCoalesceUs_ = 0;
    bool syncRequested_ = false;
    bool syncFailure_ = false;
    bool stopping_ = false;
    std::condition_variable syncCondition_;
    std::condition_variable durableCondition_;
    std::thread syncWorker_;
    std::function<void(bool, uint64_t)> appendCompletionCallback_;
    std::function<void(uint64_t, uint64_t, uint64_t)> appendBatchMetricsCallback_;
};

class DurableStateManager final : public nuraft::state_mgr {
public:
    DurableStateManager(int serverId,
                        std::vector<StaticMember> members,
                        std::shared_ptr<DurableRaftStorage> storage,
                        nuraft::ptr<DurableLogStore> logStore);

    nuraft::ptr<nuraft::cluster_config> load_config() override;
    void save_config(const nuraft::cluster_config& config) override;
    void save_state(const nuraft::srv_state& state) override;
    nuraft::ptr<nuraft::srv_state> read_state() override;
    nuraft::ptr<nuraft::log_store> load_log_store() override;
    nuraft::int32 server_id() override;
    void system_exit(int exitCode) override;

    void saveSnapshotInfo(nuraft::snapshot& snapshot);
    nuraft::ptr<nuraft::snapshot> loadSnapshotInfo() const;

private:
    int serverId_;
    std::vector<StaticMember> members_;
    std::shared_ptr<DurableRaftStorage> storage_;
    nuraft::ptr<DurableLogStore> logStore_;
};

class NuRaftStateMachine final : public nuraft::state_machine {
public:
    NuRaftStateMachine(std::string snapshotDirectory,
                       nuraft::ptr<DurableLogStore> logStore,
                       nuraft::ptr<DurableStateManager> stateManager);

    nuraft::ptr<nuraft::buffer> commit(nuraft::ulong logIndex,
                                       nuraft::buffer& data) override;
    void commit_config(nuraft::ulong logIndex,
                       nuraft::ptr<nuraft::cluster_config>& config) override;
    bool apply_snapshot(nuraft::snapshot& snapshot) override;
    int read_logical_snp_obj(nuraft::snapshot& snapshot, void*& context,
                             nuraft::ulong objectId,
                             nuraft::ptr<nuraft::buffer>& output,
                             bool& lastObject) override;
    void save_logical_snp_obj(nuraft::snapshot& snapshot,
                              nuraft::ulong& objectId,
                              nuraft::buffer& data,
                              bool firstObject, bool lastObject) override;
    void free_user_snp_ctx(void*& context) override;
    nuraft::ptr<nuraft::snapshot> last_snapshot() override;
    nuraft::ulong last_commit_index() override;
    void create_snapshot(nuraft::snapshot& snapshot,
                         nuraft::async_result<bool>::handler_type& done) override;

    std::optional<UploadSessionRecord> session(const std::string& id) const;
    std::vector<UploadSessionRecord> sessions() const;
    std::optional<LeaseRecord> lease(const std::string& id) const;
    std::optional<ObjectRecord> object(const std::string& id) const;
    std::optional<ChunkRouteRecord> chunk(const std::string& objectId, uint32_t index) const;
    std::optional<ReadDescriptor> readDescriptor(const std::string& id,
                                                 uint64_t version) const;
    std::vector<DirectoryRecord> directories(const std::string& ownerId,
                                             const std::string& parentPath) const;
    std::vector<ObjectRecord> objects(const std::string& ownerId,
                                      const std::string& parentPath) const;
    std::vector<DeleteTaskRecord> deleteTasks(const std::string& nodeId) const;
    std::string stateDigest() const;
    std::optional<NodeRecord> node(const std::string& id) const;
    std::vector<NodeRecord> nodes() const;

    static nuraft::ptr<nuraft::buffer> encodeResult(const ApplyResult& result);
    static std::optional<ApplyResult> decodeResult(nuraft::buffer& buffer);

private:
    bool publishSnapshot(nuraft::snapshot& snapshot, std::string* error);

    mutable std::mutex mutex_;
    MetadataStateMachine state_;
    SnapshotStore snapshotStore_;
    nuraft::ptr<DurableLogStore> logStore_;
    nuraft::ptr<DurableStateManager> stateManager_;
    nuraft::ptr<nuraft::snapshot> lastSnapshot_;
    std::string incomingSnapshot_;
    std::atomic<uint64_t> lastCommitIndex_{0};
};

class NuRaftMetadataService {
public:
    NuRaftMetadataService(int serverId, int raftPort,
                          std::vector<StaticMember> members,
                          std::string directory,
                          std::string walDirectory = {});
    ~NuRaftMetadataService();
    NuRaftMetadataService(const NuRaftMetadataService&) = delete;
    NuRaftMetadataService& operator=(const NuRaftMetadataService&) = delete;

    bool start(std::string* error = nullptr);
    void shutdown();
    ApplyResult propose(const MetadataCommand& command);
    ApplyResult proposeBatch(const std::vector<MetadataCommand>& commands);
    ApplyResult createSessionAndReserve(const MetadataCommand& create,
                                        const std::vector<ReserveLeaseRequest>& requests);
    ApplyResult reserveLease(const ReserveLeaseRequest& request);
    ApplyResult reserveLeaseBatch(const std::vector<ReserveLeaseRequest>& requests);
    ApplyResult commitChunk(const std::string& sessionId,
                            uint32_t index,
                            uint64_t size,
                            const std::vector<std::string>& successfulNodes,
                            const std::string& leaseId);
    bool heartbeat(const NodeHeartbeat& heartbeat, std::string* error = nullptr);
    std::optional<ReadFence> linearizableReadBarrier(const std::string& commandId);
    std::optional<UploadSessionRecord> session(const std::string& id) const;
    std::vector<UploadSessionRecord> sessions() const;
    std::optional<LeaseRecord> lease(const std::string& id) const;
    std::optional<ObjectRecord> object(const std::string& id) const;
    std::optional<ChunkRouteRecord> chunk(const std::string& objectId, uint32_t index) const;
    std::optional<ReadDescriptor> readDescriptor(const std::string& id,
                                                 uint64_t version) const;
    std::vector<DirectoryRecord> directories(const std::string& ownerId,
                                             const std::string& parentPath) const;
    std::vector<ObjectRecord> objects(const std::string& ownerId,
                                      const std::string& parentPath) const;
    std::vector<DeleteTaskRecord> deleteTasks(const std::string& nodeId) const;
    std::vector<NodeRecord> nodes() const;
    ConsensusStatus status() const;
    MetadataMetricsSnapshot metrics() const;
    std::string stateDigest() const;

private:
    ApplyResult unavailable(const MetadataCommand& command,
                            const std::string& message) const;
    std::optional<std::vector<MetadataCommand>> buildReserveCommands(
        const std::vector<ReserveLeaseRequest>& requests, ApplyResult& failure);

    int serverId_;
    int raftPort_;
    std::vector<StaticMember> members_;
    std::string directory_;
    std::string walDirectory_;
    std::shared_ptr<DurableRaftStorage> storage_;
    nuraft::ptr<DurableLogStore> logStore_;
    nuraft::ptr<DurableStateManager> stateManager_;
    nuraft::ptr<NuRaftStateMachine> machine_;
    nuraft::raft_launcher launcher_;
    nuraft::ptr<nuraft::raft_server> server_;
    mutable std::mutex softMutex_;
    LeaderSoftState softState_;
    uint64_t observedTerm_ = 0;
    int64_t heartbeatFreshnessMs_ = 6000;
    mutable std::mutex metricsMutex_;
    MetadataMetricsSnapshot metrics_;
};

} // namespace miniKV::metadata::raft
