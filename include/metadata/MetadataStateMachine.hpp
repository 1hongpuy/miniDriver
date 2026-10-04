#pragma once

#include "metadata/MetadataCommand.hpp"

#include <map>
#include <optional>
#include <string>

namespace miniKV::metadata {

class MetadataStateMachine {
public:
    ApplyResult apply(const MetadataCommand& command, uint64_t raftIndex, uint64_t raftTerm);
    ApplyResult apply(const MetadataCommand& command, uint64_t raftIndex)
    {
        return apply(command, raftIndex, 0);
    }

    std::optional<ObjectRecord> object(const std::string& objectId) const;
    std::optional<UploadSessionRecord> session(const std::string& sessionId) const;
    std::vector<UploadSessionRecord> sessions() const;
    std::optional<ChunkRouteRecord> chunk(const std::string& objectId, uint32_t index) const;
    std::optional<LeaseRecord> lease(const std::string& leaseId) const;
    std::optional<NodeRecord> node(const std::string& nodeId) const;
    std::optional<ApplyResult> appliedResult(const std::string& commandId) const;
    std::optional<DedupEntry> dedupEntry(const std::string& commandId) const;
    std::vector<NodeRecord> nodes() const;
    std::optional<ReadDescriptor> readDescriptor(const std::string& objectId,
                                                 uint64_t objectVersion) const;
    std::vector<DirectoryRecord> directories(const std::string& ownerId,
                                             const std::string& parentPath) const;
    std::vector<ObjectRecord> objects(const std::string& ownerId,
                                      const std::string& parentPath) const;
    std::vector<DeleteTaskRecord> deleteTasks(const std::string& nodeId) const;

    uint64_t metadataVersion() const;
    uint64_t lastAppliedTerm() const;
    uint64_t placementEpoch() const;
    size_t dedupEntryCount() const;
    MetadataSnapshot snapshot() const;
    bool restore(const MetadataSnapshot& snapshot);
    std::string stateDigest() const;

private:
    static std::string chunkKey(const std::string& objectId, uint32_t index);
    static std::string catalogKey(const std::string& ownerId,
                                  const std::string& parentPath,
                                  const std::string& name);
    ApplyResult applyNew(const MetadataCommand& command, uint64_t raftIndex, uint64_t raftTerm);
    ApplyResult resultFor(const MetadataCommand& command,
                          uint64_t raftIndex,
                          uint64_t raftTerm,
                          ApplyStatus status,
                          std::string message) const;
    bool finishLease(LeaseRecord& lease, LeaseState terminalState);

    uint64_t lastAppliedIndex_ = 0;
    uint64_t lastAppliedTerm_ = 0;
    uint64_t placementEpoch_ = 0;
    std::map<std::string, ObjectRecord> objects_;
    std::map<std::string, UploadSessionRecord> sessions_;
    std::map<std::string, ChunkRouteRecord> chunks_;
    std::map<std::string, LeaseRecord> leases_;
    std::map<std::string, NodeRecord> nodes_;
    std::map<std::string, std::string> catalog_;
    std::map<std::string, DirectoryRecord> directories_;
    std::map<std::string, DeleteTaskRecord> deleteTasks_;
    std::map<std::string, DedupEntry> dedup_;
};

} // namespace miniKV::metadata
