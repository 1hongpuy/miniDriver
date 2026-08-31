#pragma once

#include "metadata/MetadataCommand.hpp"

#include <map>
#include <optional>
#include <string>

namespace miniKV::metadata {

class MetadataStateMachine {
public:
    ApplyResult apply(const MetadataCommand& command, uint64_t appliedIndex);

    std::optional<ObjectRecord> object(const std::string& objectId) const;
    std::optional<UploadSessionRecord> session(const std::string& sessionId) const;
    std::optional<ChunkRecord> chunk(const std::string& objectId, uint32_t index) const;
    std::optional<LeaseRecord> lease(const std::string& leaseId) const;
    std::optional<NodeRecord> node(const std::string& nodeId) const;
    std::optional<ApplyResult> appliedResult(const std::string& commandId) const;

    uint64_t metadataVersion() const;
    uint64_t placementEpoch() const;
    MetadataSnapshot snapshot() const;
    bool restore(const MetadataSnapshot& snapshot);
    std::string stateDigest() const;

private:
    static std::string chunkKey(const std::string& objectId, uint32_t index);
    ApplyResult applyNew(const MetadataCommand& command, uint64_t appliedIndex);
    ApplyResult resultFor(const MetadataCommand& command,
                          uint64_t appliedIndex,
                          ApplyStatus status,
                          std::string message) const;

    uint64_t lastAppliedIndex_ = 0;
    uint64_t placementEpoch_ = 0;
    std::map<std::string, ObjectRecord> objects_;
    std::map<std::string, UploadSessionRecord> sessions_;
    std::map<std::string, ChunkRecord> chunks_;
    std::map<std::string, LeaseRecord> leases_;
    std::map<std::string, NodeRecord> nodes_;
    std::map<std::string, RepairTask> repairTasks_;
    std::map<std::string, ApplyResult> appliedResults_;
};

} // namespace miniKV::metadata
