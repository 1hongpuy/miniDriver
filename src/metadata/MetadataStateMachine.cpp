#include "metadata/MetadataStateMachine.hpp"

#include <iomanip>
#include <set>
#include <sstream>
#include <utility>

namespace miniKV::metadata {
namespace {

constexpr uint32_t kSnapshotSchemaVersion = 1;
constexpr uint32_t kMaxCollectionEntries = 1U << 20;

class Writer {
public:
    void u8(uint8_t value) { bytes_.push_back(static_cast<char>(value)); }
    void u16(uint16_t value)
    {
        for(int shift = 0; shift < 16; shift += 8) u8(static_cast<uint8_t>(value >> shift));
    }
    void u32(uint32_t value)
    {
        for(int shift = 0; shift < 32; shift += 8) u8(static_cast<uint8_t>(value >> shift));
    }
    void u64(uint64_t value)
    {
        for(int shift = 0; shift < 64; shift += 8) u8(static_cast<uint8_t>(value >> shift));
    }
    void i64(int64_t value) { u64(static_cast<uint64_t>(value)); }
    void string(const std::string& value)
    {
        u32(static_cast<uint32_t>(value.size()));
        bytes_.append(value);
    }
    std::string take() { return std::move(bytes_); }

private:
    std::string bytes_;
};

class Reader {
public:
    explicit Reader(const std::string& bytes) : bytes_(bytes) {}

    bool u8(uint8_t& value)
    {
        if(pos_ >= bytes_.size()) return false;
        value = static_cast<uint8_t>(bytes_[pos_++]);
        return true;
    }
    bool u16(uint16_t& value)
    {
        uint8_t byte = 0;
        value = 0;
        for(int shift = 0; shift < 16; shift += 8) {
            if(!u8(byte)) return false;
            value |= static_cast<uint16_t>(byte) << shift;
        }
        return true;
    }
    bool u32(uint32_t& value)
    {
        uint8_t byte = 0;
        value = 0;
        for(int shift = 0; shift < 32; shift += 8) {
            if(!u8(byte)) return false;
            value |= static_cast<uint32_t>(byte) << shift;
        }
        return true;
    }
    bool u64(uint64_t& value)
    {
        uint8_t byte = 0;
        value = 0;
        for(int shift = 0; shift < 64; shift += 8) {
            if(!u8(byte)) return false;
            value |= static_cast<uint64_t>(byte) << shift;
        }
        return true;
    }
    bool i64(int64_t& value)
    {
        uint64_t raw = 0;
        if(!u64(raw)) return false;
        value = static_cast<int64_t>(raw);
        return true;
    }
    bool string(std::string& value)
    {
        uint32_t size = 0;
        if(!u32(size) || size > bytes_.size() - pos_) return false;
        value.assign(bytes_.data() + pos_, size);
        pos_ += size;
        return true;
    }
    bool done() const { return pos_ == bytes_.size(); }

private:
    const std::string& bytes_;
    size_t pos_ = 0;
};

bool validObjectState(uint8_t value) { return value <= static_cast<uint8_t>(ObjectState::kFailed); }
bool validChunkState(uint8_t value) { return value <= static_cast<uint8_t>(ChunkState::kFailed); }
bool validReplicaState(uint8_t value) { return value <= static_cast<uint8_t>(ReplicaState::kRepairing); }
bool validNodeHealth(uint8_t value) { return value <= static_cast<uint8_t>(NodeHealth::kRecovering); }
bool validRepairState(uint8_t value) { return value <= static_cast<uint8_t>(RepairState::kFailed); }
bool validApplyStatus(uint8_t value) { return value <= static_cast<uint8_t>(ApplyStatus::kUnsupported); }

void writeObject(Writer& writer, const ObjectRecord& record)
{
    writer.string(record.objectId);
    writer.string(record.ownerId);
    writer.string(record.parentPath);
    writer.string(record.name);
    writer.string(record.fileHash);
    writer.u64(record.fileSize);
    writer.u32(record.chunkSize);
    writer.u32(record.desiredRf);
    writer.u8(static_cast<uint8_t>(record.state));
    writer.u64(record.metadataVersion);
}

bool readObject(Reader& reader, ObjectRecord& record)
{
    uint8_t state = 0;
    if(!reader.string(record.objectId) || !reader.string(record.ownerId) || !reader.string(record.parentPath)
       || !reader.string(record.name) || !reader.string(record.fileHash) || !reader.u64(record.fileSize)
       || !reader.u32(record.chunkSize) || !reader.u32(record.desiredRf) || !reader.u8(state)
       || !validObjectState(state) || !reader.u64(record.metadataVersion)) {
        return false;
    }
    record.state = static_cast<ObjectState>(state);
    return true;
}

void writeSession(Writer& writer, const UploadSessionRecord& record)
{
    writer.string(record.sessionId);
    writer.string(record.objectId);
    writer.string(record.ownerId);
    writer.string(record.manifestHash);
    writer.u64(record.fileSize);
    writer.u32(record.chunkSize);
    writer.u32(record.totalChunks);
    writer.i64(record.expiresAt);
}

bool readSession(Reader& reader, UploadSessionRecord& record)
{
    return reader.string(record.sessionId) && reader.string(record.objectId) && reader.string(record.ownerId)
        && reader.string(record.manifestHash) && reader.u64(record.fileSize) && reader.u32(record.chunkSize)
        && reader.u32(record.totalChunks) && reader.i64(record.expiresAt);
}

void writeReplica(Writer& writer, const ReplicaRecord& record)
{
    writer.string(record.nodeId);
    writer.u64(record.nodeEpoch);
    writer.u64(record.generation);
    writer.u8(static_cast<uint8_t>(record.state));
    writer.string(record.verifiedHash);
    writer.i64(record.verifiedAt);
}

bool readReplica(Reader& reader, ReplicaRecord& record)
{
    uint8_t state = 0;
    if(!reader.string(record.nodeId) || !reader.u64(record.nodeEpoch) || !reader.u64(record.generation)
       || !reader.u8(state) || !validReplicaState(state) || !reader.string(record.verifiedHash)
       || !reader.i64(record.verifiedAt)) {
        return false;
    }
    record.state = static_cast<ReplicaState>(state);
    return true;
}

void writeChunk(Writer& writer, const ChunkRecord& record)
{
    writer.string(record.objectId);
    writer.u32(record.index);
    writer.string(record.chunkHash);
    writer.u64(record.size);
    writer.u32(record.desiredRf);
    writer.u8(static_cast<uint8_t>(record.state));
    writer.u64(record.generation);
    writer.u32(static_cast<uint32_t>(record.replicas.size()));
    for(const auto& replica : record.replicas) writeReplica(writer, replica);
}

bool readChunk(Reader& reader, ChunkRecord& record)
{
    uint8_t state = 0;
    uint32_t count = 0;
    if(!reader.string(record.objectId) || !reader.u32(record.index) || !reader.string(record.chunkHash)
       || !reader.u64(record.size) || !reader.u32(record.desiredRf) || !reader.u8(state)
       || !validChunkState(state) || !reader.u64(record.generation) || !reader.u32(count)
       || count > kMaxCollectionEntries) {
        return false;
    }
    record.state = static_cast<ChunkState>(state);
    record.replicas.clear();
    record.replicas.reserve(count);
    for(uint32_t i = 0; i < count; ++i) {
        ReplicaRecord replica;
        if(!readReplica(reader, replica)) return false;
        record.replicas.push_back(std::move(replica));
    }
    return true;
}

void writeLease(Writer& writer, const LeaseRecord& record)
{
    writer.string(record.leaseId);
    writer.string(record.requestKey);
    writer.string(record.sessionId);
    writer.u32(record.chunkIndex);
    writer.string(record.chunkHash);
    writer.u64(record.chunkSize);
    writer.u64(record.placementEpoch);
    writer.u32(static_cast<uint32_t>(record.targetNodeIds.size()));
    for(const auto& nodeId : record.targetNodeIds) writer.string(nodeId);
    writer.i64(record.expiresAt);
}

bool readLease(Reader& reader, LeaseRecord& record)
{
    uint32_t count = 0;
    if(!reader.string(record.leaseId) || !reader.string(record.requestKey) || !reader.string(record.sessionId)
       || !reader.u32(record.chunkIndex) || !reader.string(record.chunkHash) || !reader.u64(record.chunkSize)
       || !reader.u64(record.placementEpoch) || !reader.u32(count) || count > kMaxCollectionEntries) {
        return false;
    }
    record.targetNodeIds.clear();
    record.targetNodeIds.reserve(count);
    for(uint32_t i = 0; i < count; ++i) {
        std::string nodeId;
        if(!reader.string(nodeId)) return false;
        record.targetNodeIds.push_back(std::move(nodeId));
    }
    return reader.i64(record.expiresAt);
}

void writeNode(Writer& writer, const NodeRecord& record)
{
    writer.string(record.nodeId);
    writer.string(record.bootId);
    writer.u64(record.nodeEpoch);
    writer.string(record.address);
    writer.u16(record.dataPort);
    writer.u8(static_cast<uint8_t>(record.health));
    writer.u64(record.freeBytes);
    writer.u32(record.activeUploads);
    writer.u32(record.activeDownloads);
    writer.u32(record.diskQueueDepth);
    writer.u64(record.diskPauseMs);
    writer.u64(record.eventLoopLagUs);
    writer.u64(record.placementEpoch);
    writer.i64(record.lastHeartbeatAt);
}

bool readNode(Reader& reader, NodeRecord& record)
{
    uint8_t health = 0;
    if(!reader.string(record.nodeId) || !reader.string(record.bootId) || !reader.u64(record.nodeEpoch)
       || !reader.string(record.address) || !reader.u16(record.dataPort) || !reader.u8(health)
       || !validNodeHealth(health) || !reader.u64(record.freeBytes) || !reader.u32(record.activeUploads)
       || !reader.u32(record.activeDownloads) || !reader.u32(record.diskQueueDepth)
       || !reader.u64(record.diskPauseMs) || !reader.u64(record.eventLoopLagUs)
       || !reader.u64(record.placementEpoch) || !reader.i64(record.lastHeartbeatAt)) {
        return false;
    }
    record.health = static_cast<NodeHealth>(health);
    return true;
}

void writeRepairTask(Writer& writer, const RepairTask& record)
{
    writer.string(record.taskKey);
    writer.string(record.objectId);
    writer.u32(record.chunkIndex);
    writer.string(record.chunkHash);
    writer.string(record.sourceNodeId);
    writer.string(record.targetNodeId);
    writer.u64(record.expectedGeneration);
    writer.u8(static_cast<uint8_t>(record.state));
    writer.u32(record.attempts);
    writer.i64(record.nextRetryAt);
    writer.string(record.lastError);
}

bool readRepairTask(Reader& reader, RepairTask& record)
{
    uint8_t state = 0;
    if(!reader.string(record.taskKey) || !reader.string(record.objectId) || !reader.u32(record.chunkIndex)
       || !reader.string(record.chunkHash) || !reader.string(record.sourceNodeId)
       || !reader.string(record.targetNodeId) || !reader.u64(record.expectedGeneration) || !reader.u8(state)
       || !validRepairState(state) || !reader.u32(record.attempts) || !reader.i64(record.nextRetryAt)
       || !reader.string(record.lastError)) {
        return false;
    }
    record.state = static_cast<RepairState>(state);
    return true;
}

void writeApplyResult(Writer& writer, const ApplyResult& result)
{
    writer.string(result.commandId);
    writer.u8(static_cast<uint8_t>(result.status));
    writer.u64(result.metadataVersion);
    writer.u64(result.appliedIndex);
    writer.u64(result.nodeEpoch);
    writer.u64(result.placementEpoch);
    writer.string(result.objectId);
    writer.string(result.sessionId);
    writer.string(result.leaseId);
    writer.string(result.message);
}

bool readApplyResult(Reader& reader, ApplyResult& result)
{
    uint8_t status = 0;
    if(!reader.string(result.commandId) || !reader.u8(status) || !validApplyStatus(status)
       || !reader.u64(result.metadataVersion) || !reader.u64(result.appliedIndex)
       || !reader.u64(result.nodeEpoch) || !reader.u64(result.placementEpoch)
       || !reader.string(result.objectId) || !reader.string(result.sessionId)
       || !reader.string(result.leaseId) || !reader.string(result.message)) {
        return false;
    }
    result.status = static_cast<ApplyStatus>(status);
    return true;
}

template <typename T, typename WriteRecord>
void writeMap(Writer& writer, const std::map<std::string, T>& values, WriteRecord writeRecord)
{
    writer.u32(static_cast<uint32_t>(values.size()));
    for(const auto& [key, value] : values) {
        writer.string(key);
        writeRecord(writer, value);
    }
}

template <typename T, typename ReadRecord>
bool readMap(Reader& reader, std::map<std::string, T>& values, ReadRecord readRecord)
{
    uint32_t count = 0;
    if(!reader.u32(count) || count > kMaxCollectionEntries) return false;
    values.clear();
    for(uint32_t i = 0; i < count; ++i) {
        std::string key;
        T value;
        if(!reader.string(key) || key.empty() || !readRecord(reader, value)) return false;
        if(!values.emplace(std::move(key), std::move(value)).second) return false;
    }
    return true;
}

bool isOnline(const NodeRecord& node)
{
    return node.health == NodeHealth::kOnline;
}

} // namespace

const char* toString(ApplyStatus status)
{
    switch(status) {
    case ApplyStatus::kOk: return "OK";
    case ApplyStatus::kAlreadyApplied: return "ALREADY_APPLIED";
    case ApplyStatus::kInvalid: return "INVALID";
    case ApplyStatus::kNotFound: return "NOT_FOUND";
    case ApplyStatus::kConflict: return "CONFLICT";
    case ApplyStatus::kFenced: return "FENCED";
    case ApplyStatus::kUnsupported: return "UNSUPPORTED";
    }
    return "INVALID";
}

std::string MetadataStateMachine::chunkKey(const std::string& objectId, uint32_t index)
{
    return objectId + "\n" + std::to_string(index);
}

ApplyResult MetadataStateMachine::resultFor(const MetadataCommand& command,
                                            uint64_t appliedIndex,
                                            ApplyStatus status,
                                            std::string message) const
{
    ApplyResult result;
    result.commandId = command.commandId;
    result.status = status;
    result.metadataVersion = appliedIndex;
    result.appliedIndex = appliedIndex;
    result.placementEpoch = placementEpoch_;
    result.message = std::move(message);
    return result;
}

ApplyResult MetadataStateMachine::apply(const MetadataCommand& command, uint64_t appliedIndex)
{
    if(appliedIndex == 0 || appliedIndex <= lastAppliedIndex_) {
        return resultFor(command, lastAppliedIndex_, ApplyStatus::kInvalid, "applied index is not monotonic");
    }

    lastAppliedIndex_ = appliedIndex;
    if(command.commandId.empty()) {
        return resultFor(command, appliedIndex, ApplyStatus::kInvalid, "commandId is required");
    }

    const auto alreadyApplied = appliedResults_.find(command.commandId);
    if(alreadyApplied != appliedResults_.end()) return alreadyApplied->second;

    ApplyResult result = applyNew(command, appliedIndex);
    appliedResults_.emplace(command.commandId, result);
    return result;
}

ApplyResult MetadataStateMachine::applyNew(const MetadataCommand& command, uint64_t appliedIndex)
{
    if(command.schemaVersion != 1) {
        return resultFor(command, appliedIndex, ApplyStatus::kInvalid, "unsupported command schema version");
    }
    const uint64_t previousVersion = appliedIndex - 1;
    if(command.expectedMetadataVersion != 0 && command.expectedMetadataVersion != previousVersion) {
        return resultFor(command, appliedIndex, ApplyStatus::kConflict, "metadata version does not match");
    }

    switch(command.type) {
    case MetadataCommandType::kCreateSession: {
        const auto* payload = std::get_if<CreateSessionPayload>(&command.payload);
        if(payload == nullptr || payload->sessionId.empty() || payload->objectId.empty() || payload->ownerId.empty()
           || payload->fileHash.empty() || payload->chunkSize == 0 || payload->desiredRf == 0
           || payload->chunks.empty()) {
            return resultFor(command, appliedIndex, ApplyStatus::kInvalid, "invalid create session payload");
        }
        if(sessions_.count(payload->sessionId) != 0 || objects_.count(payload->objectId) != 0) {
            return resultFor(command, appliedIndex, ApplyStatus::kConflict, "session or object already exists");
        }
        std::set<uint32_t> indexes;
        uint64_t totalSize = 0;
        for(const auto& initialChunk : payload->chunks) {
            if(initialChunk.chunkHash.empty() || initialChunk.size == 0 || !indexes.insert(initialChunk.index).second) {
                return resultFor(command, appliedIndex, ApplyStatus::kInvalid, "invalid initial chunk list");
            }
            totalSize += initialChunk.size;
        }
        for(uint32_t index = 0; index < payload->chunks.size(); ++index) {
            if(indexes.count(index) == 0) {
                return resultFor(command, appliedIndex, ApplyStatus::kInvalid, "initial chunk indexes must be contiguous");
            }
        }
        if(totalSize != payload->fileSize) {
            return resultFor(command, appliedIndex, ApplyStatus::kInvalid, "chunk sizes do not match file size");
        }

        ObjectRecord object;
        object.objectId = payload->objectId;
        object.ownerId = payload->ownerId;
        object.parentPath = payload->parentPath;
        object.name = payload->name;
        object.fileHash = payload->fileHash;
        object.fileSize = payload->fileSize;
        object.chunkSize = payload->chunkSize;
        object.desiredRf = payload->desiredRf;
        object.state = ObjectState::kUploading;
        object.metadataVersion = appliedIndex;
        objects_.emplace(object.objectId, object);

        UploadSessionRecord session;
        session.sessionId = payload->sessionId;
        session.objectId = payload->objectId;
        session.ownerId = payload->ownerId;
        session.manifestHash = payload->manifestHash;
        session.fileSize = payload->fileSize;
        session.chunkSize = payload->chunkSize;
        session.totalChunks = static_cast<uint32_t>(payload->chunks.size());
        session.expiresAt = payload->expiresAt;
        sessions_.emplace(session.sessionId, session);

        for(const auto& initialChunk : payload->chunks) {
            ChunkRecord chunk;
            chunk.objectId = payload->objectId;
            chunk.index = initialChunk.index;
            chunk.chunkHash = initialChunk.chunkHash;
            chunk.size = initialChunk.size;
            chunk.desiredRf = payload->desiredRf;
            chunks_.emplace(chunkKey(chunk.objectId, chunk.index), std::move(chunk));
        }
        ApplyResult result = resultFor(command, appliedIndex, ApplyStatus::kOk, "session created");
        result.objectId = payload->objectId;
        result.sessionId = payload->sessionId;
        return result;
    }
    case MetadataCommandType::kRegisterNode: {
        const auto* payload = std::get_if<RegisterNodePayload>(&command.payload);
        if(payload == nullptr || payload->nodeId.empty() || payload->bootId.empty() || payload->address.empty()
           || payload->dataPort == 0) {
            return resultFor(command, appliedIndex, ApplyStatus::kInvalid, "invalid register node payload");
        }
        auto it = nodes_.find(payload->nodeId);
        if(it == nodes_.end()) {
            NodeRecord node;
            node.nodeId = payload->nodeId;
            node.bootId = payload->bootId;
            node.nodeEpoch = 1;
            node.address = payload->address;
            node.dataPort = payload->dataPort;
            node.health = NodeHealth::kRecovering;
            node.lastHeartbeatAt = payload->observedAt;
            node.placementEpoch = ++placementEpoch_;
            it = nodes_.emplace(node.nodeId, std::move(node)).first;
        } else if(it->second.bootId != payload->bootId) {
            NodeRecord& node = it->second;
            ++node.nodeEpoch;
            node.bootId = payload->bootId;
            node.address = payload->address;
            node.dataPort = payload->dataPort;
            node.health = NodeHealth::kRecovering;
            node.lastHeartbeatAt = payload->observedAt;
            node.placementEpoch = ++placementEpoch_;
        } else {
            it->second.address = payload->address;
            it->second.dataPort = payload->dataPort;
            it->second.lastHeartbeatAt = payload->observedAt;
        }
        ApplyResult result = resultFor(command, appliedIndex, ApplyStatus::kOk, "node registered");
        result.nodeEpoch = it->second.nodeEpoch;
        return result;
    }
    case MetadataCommandType::kHeartbeatNode: {
        const auto* payload = std::get_if<HeartbeatNodePayload>(&command.payload);
        if(payload == nullptr || payload->nodeId.empty()) {
            return resultFor(command, appliedIndex, ApplyStatus::kInvalid, "invalid heartbeat payload");
        }
        const auto it = nodes_.find(payload->nodeId);
        if(it == nodes_.end()) return resultFor(command, appliedIndex, ApplyStatus::kNotFound, "node not found");
        NodeRecord& node = it->second;
        if(command.nodeEpoch != node.nodeEpoch) {
            return resultFor(command, appliedIndex, ApplyStatus::kFenced, "node epoch is stale");
        }
        node.freeBytes = payload->freeBytes;
        node.activeUploads = payload->activeUploads;
        node.activeDownloads = payload->activeDownloads;
        node.diskQueueDepth = payload->diskQueueDepth;
        node.diskPauseMs = payload->diskPauseMs;
        node.eventLoopLagUs = payload->eventLoopLagUs;
        node.lastHeartbeatAt = payload->observedAt;
        ApplyResult result = resultFor(command, appliedIndex, ApplyStatus::kOk, "heartbeat applied");
        result.nodeEpoch = node.nodeEpoch;
        return result;
    }
    case MetadataCommandType::kMarkNodeHealth: {
        const auto* payload = std::get_if<MarkNodeHealthPayload>(&command.payload);
        if(payload == nullptr || payload->nodeId.empty()) {
            return resultFor(command, appliedIndex, ApplyStatus::kInvalid, "invalid node health payload");
        }
        const auto it = nodes_.find(payload->nodeId);
        if(it == nodes_.end()) return resultFor(command, appliedIndex, ApplyStatus::kNotFound, "node not found");
        NodeRecord& node = it->second;
        if(command.nodeEpoch != node.nodeEpoch) {
            return resultFor(command, appliedIndex, ApplyStatus::kFenced, "node epoch is stale");
        }
        if(node.health != payload->health) {
            node.health = payload->health;
            node.placementEpoch = ++placementEpoch_;
        }
        node.lastHeartbeatAt = payload->observedAt;
        ApplyResult result = resultFor(command, appliedIndex, ApplyStatus::kOk, "node health updated");
        result.nodeEpoch = node.nodeEpoch;
        return result;
    }
    case MetadataCommandType::kReserveLease: {
        const auto* payload = std::get_if<ReserveLeasePayload>(&command.payload);
        if(payload == nullptr || payload->leaseId.empty() || payload->requestKey.empty() || payload->sessionId.empty()
           || payload->chunkHash.empty() || payload->chunkSize == 0 || payload->targetNodeIds.empty()) {
            return resultFor(command, appliedIndex, ApplyStatus::kInvalid, "invalid reserve lease payload");
        }
        if(leases_.count(payload->leaseId) != 0) {
            return resultFor(command, appliedIndex, ApplyStatus::kConflict, "lease already exists");
        }
        const auto sessionIt = sessions_.find(payload->sessionId);
        if(sessionIt == sessions_.end()) return resultFor(command, appliedIndex, ApplyStatus::kNotFound, "session not found");
        const auto chunkIt = chunks_.find(chunkKey(sessionIt->second.objectId, payload->chunkIndex));
        if(chunkIt == chunks_.end()) return resultFor(command, appliedIndex, ApplyStatus::kNotFound, "chunk not found");
        const ChunkRecord& chunk = chunkIt->second;
        if(chunk.state != ChunkState::kAllocated || chunk.chunkHash != payload->chunkHash || chunk.size != payload->chunkSize) {
            return resultFor(command, appliedIndex, ApplyStatus::kConflict, "chunk is not leaseable");
        }
        if(command.placementEpoch != placementEpoch_) {
            return resultFor(command, appliedIndex, ApplyStatus::kFenced, "placement epoch is stale");
        }
        if(payload->targetNodeIds.size() != chunk.desiredRf) {
            return resultFor(command, appliedIndex, ApplyStatus::kInvalid, "target replica count does not match desired RF");
        }
        std::set<std::string> uniqueNodes;
        for(const auto& nodeId : payload->targetNodeIds) {
            const auto nodeIt = nodes_.find(nodeId);
            if(nodeIt == nodes_.end() || !isOnline(nodeIt->second) || !uniqueNodes.insert(nodeId).second) {
                return resultFor(command, appliedIndex, ApplyStatus::kConflict, "lease target is not an eligible node");
            }
        }
        LeaseRecord lease;
        lease.leaseId = payload->leaseId;
        lease.requestKey = payload->requestKey;
        lease.sessionId = payload->sessionId;
        lease.chunkIndex = payload->chunkIndex;
        lease.chunkHash = payload->chunkHash;
        lease.chunkSize = payload->chunkSize;
        lease.placementEpoch = placementEpoch_;
        lease.targetNodeIds = payload->targetNodeIds;
        lease.expiresAt = payload->expiresAt;
        leases_.emplace(lease.leaseId, lease);
        ApplyResult result = resultFor(command, appliedIndex, ApplyStatus::kOk, "lease reserved");
        result.sessionId = lease.sessionId;
        result.leaseId = lease.leaseId;
        result.objectId = sessionIt->second.objectId;
        return result;
    }
    case MetadataCommandType::kCommitChunk: {
        const auto* payload = std::get_if<CommitChunkPayload>(&command.payload);
        if(payload == nullptr || payload->sessionId.empty() || payload->leaseId.empty() || payload->chunkHash.empty()
           || payload->chunkSize == 0) {
            return resultFor(command, appliedIndex, ApplyStatus::kInvalid, "invalid commit chunk payload");
        }
        const auto sessionIt = sessions_.find(payload->sessionId);
        const auto leaseIt = leases_.find(payload->leaseId);
        if(sessionIt == sessions_.end() || leaseIt == leases_.end()) {
            return resultFor(command, appliedIndex, ApplyStatus::kNotFound, "session or lease not found");
        }
        const LeaseRecord& lease = leaseIt->second;
        if(lease.sessionId != payload->sessionId || lease.chunkIndex != payload->chunkIndex
           || lease.chunkHash != payload->chunkHash || lease.chunkSize != payload->chunkSize) {
            return resultFor(command, appliedIndex, ApplyStatus::kConflict, "commit does not match lease");
        }
        if(command.placementEpoch != lease.placementEpoch || command.placementEpoch != placementEpoch_) {
            return resultFor(command, appliedIndex, ApplyStatus::kFenced, "placement epoch is stale");
        }
        const auto chunkIt = chunks_.find(chunkKey(sessionIt->second.objectId, payload->chunkIndex));
        if(chunkIt == chunks_.end()) return resultFor(command, appliedIndex, ApplyStatus::kNotFound, "chunk not found");
        ChunkRecord& chunk = chunkIt->second;
        if(chunk.chunkHash != payload->chunkHash || chunk.size != payload->chunkSize) {
            return resultFor(command, appliedIndex, ApplyStatus::kConflict, "commit does not match chunk");
        }
        if(command.generation != chunk.generation) {
            return resultFor(command, appliedIndex, ApplyStatus::kFenced, "chunk generation is stale");
        }
        if(payload->replicas.size() != chunk.desiredRf) {
            return resultFor(command, appliedIndex, ApplyStatus::kConflict, "strict RF has not been reached");
        }
        std::set<std::string> targets(lease.targetNodeIds.begin(), lease.targetNodeIds.end());
        std::set<std::string> seen;
        std::vector<ReplicaRecord> replicas;
        replicas.reserve(payload->replicas.size());
        for(const auto& reported : payload->replicas) {
            const auto nodeIt = nodes_.find(reported.nodeId);
            if(targets.count(reported.nodeId) == 0 || !seen.insert(reported.nodeId).second || nodeIt == nodes_.end()
               || !isOnline(nodeIt->second) || nodeIt->second.nodeEpoch != reported.nodeEpoch
               || reported.verifiedHash != chunk.chunkHash) {
                return resultFor(command, appliedIndex, ApplyStatus::kFenced, "replica report is stale or invalid");
            }
            ReplicaRecord replica;
            replica.nodeId = reported.nodeId;
            replica.nodeEpoch = reported.nodeEpoch;
            replica.generation = chunk.generation;
            replica.state = ReplicaState::kHealthy;
            replica.verifiedHash = reported.verifiedHash;
            replica.verifiedAt = reported.verifiedAt;
            replicas.push_back(std::move(replica));
        }
        chunk.replicas = std::move(replicas);
        chunk.state = ChunkState::kCommitted;
        auto objectIt = objects_.find(sessionIt->second.objectId);
        if(objectIt != objects_.end()) objectIt->second.metadataVersion = appliedIndex;
        ApplyResult result = resultFor(command, appliedIndex, ApplyStatus::kOk, "chunk committed");
        result.objectId = sessionIt->second.objectId;
        result.sessionId = payload->sessionId;
        result.leaseId = payload->leaseId;
        return result;
    }
    case MetadataCommandType::kCommitFile: {
        const auto* payload = std::get_if<CommitFilePayload>(&command.payload);
        if(payload == nullptr || payload->sessionId.empty() || payload->objectId.empty() || payload->fileHash.empty()) {
            return resultFor(command, appliedIndex, ApplyStatus::kInvalid, "invalid commit file payload");
        }
        const auto sessionIt = sessions_.find(payload->sessionId);
        const auto objectIt = objects_.find(payload->objectId);
        if(sessionIt == sessions_.end() || objectIt == objects_.end()) {
            return resultFor(command, appliedIndex, ApplyStatus::kNotFound, "session or object not found");
        }
        if(sessionIt->second.objectId != payload->objectId || objectIt->second.fileHash != payload->fileHash) {
            return resultFor(command, appliedIndex, ApplyStatus::kConflict, "file commit does not match session or object");
        }
        for(uint32_t index = 0; index < sessionIt->second.totalChunks; ++index) {
            const auto chunkIt = chunks_.find(chunkKey(payload->objectId, index));
            if(chunkIt == chunks_.end() || chunkIt->second.state != ChunkState::kCommitted) {
                return resultFor(command, appliedIndex, ApplyStatus::kConflict, "not all chunks have reached desired RF");
            }
        }
        ObjectRecord& object = objectIt->second;
        object.state = ObjectState::kCommitted;
        object.metadataVersion = appliedIndex;
        ApplyResult result = resultFor(command, appliedIndex, ApplyStatus::kOk, "file committed");
        result.objectId = object.objectId;
        result.sessionId = payload->sessionId;
        return result;
    }
    default:
        return resultFor(command, appliedIndex, ApplyStatus::kUnsupported, "command is reserved for a later phase");
    }
}

std::optional<ObjectRecord> MetadataStateMachine::object(const std::string& objectId) const
{
    const auto it = objects_.find(objectId);
    return it == objects_.end() ? std::nullopt : std::optional<ObjectRecord>(it->second);
}

std::optional<UploadSessionRecord> MetadataStateMachine::session(const std::string& sessionId) const
{
    const auto it = sessions_.find(sessionId);
    return it == sessions_.end() ? std::nullopt : std::optional<UploadSessionRecord>(it->second);
}

std::optional<ChunkRecord> MetadataStateMachine::chunk(const std::string& objectId, uint32_t index) const
{
    const auto it = chunks_.find(chunkKey(objectId, index));
    return it == chunks_.end() ? std::nullopt : std::optional<ChunkRecord>(it->second);
}

std::optional<LeaseRecord> MetadataStateMachine::lease(const std::string& leaseId) const
{
    const auto it = leases_.find(leaseId);
    return it == leases_.end() ? std::nullopt : std::optional<LeaseRecord>(it->second);
}

std::optional<NodeRecord> MetadataStateMachine::node(const std::string& nodeId) const
{
    const auto it = nodes_.find(nodeId);
    return it == nodes_.end() ? std::nullopt : std::optional<NodeRecord>(it->second);
}

std::optional<ApplyResult> MetadataStateMachine::appliedResult(const std::string& commandId) const
{
    const auto it = appliedResults_.find(commandId);
    return it == appliedResults_.end() ? std::nullopt : std::optional<ApplyResult>(it->second);
}

uint64_t MetadataStateMachine::metadataVersion() const { return lastAppliedIndex_; }
uint64_t MetadataStateMachine::placementEpoch() const { return placementEpoch_; }

MetadataSnapshot MetadataStateMachine::snapshot() const
{
    Writer writer;
    writer.u32(kSnapshotSchemaVersion);
    writer.u64(lastAppliedIndex_);
    writer.u64(placementEpoch_);
    writeMap(writer, objects_, writeObject);
    writeMap(writer, sessions_, writeSession);
    writeMap(writer, chunks_, writeChunk);
    writeMap(writer, leases_, writeLease);
    writeMap(writer, nodes_, writeNode);
    writeMap(writer, repairTasks_, writeRepairTask);
    writeMap(writer, appliedResults_, writeApplyResult);

    MetadataSnapshot snapshot;
    snapshot.schemaVersion = kSnapshotSchemaVersion;
    snapshot.lastAppliedIndex = lastAppliedIndex_;
    snapshot.bytes = writer.take();
    return snapshot;
}

bool MetadataStateMachine::restore(const MetadataSnapshot& snapshot)
{
    if(snapshot.schemaVersion != kSnapshotSchemaVersion) return false;

    Reader reader(snapshot.bytes);
    uint32_t schemaVersion = 0;
    uint64_t lastAppliedIndex = 0;
    uint64_t placementEpoch = 0;
    std::map<std::string, ObjectRecord> objects;
    std::map<std::string, UploadSessionRecord> sessions;
    std::map<std::string, ChunkRecord> chunks;
    std::map<std::string, LeaseRecord> leases;
    std::map<std::string, NodeRecord> nodes;
    std::map<std::string, RepairTask> repairTasks;
    std::map<std::string, ApplyResult> appliedResults;

    if(!reader.u32(schemaVersion) || schemaVersion != kSnapshotSchemaVersion || !reader.u64(lastAppliedIndex)
       || !reader.u64(placementEpoch) || lastAppliedIndex != snapshot.lastAppliedIndex
       || !readMap(reader, objects, readObject) || !readMap(reader, sessions, readSession)
       || !readMap(reader, chunks, readChunk) || !readMap(reader, leases, readLease)
       || !readMap(reader, nodes, readNode) || !readMap(reader, repairTasks, readRepairTask)
       || !readMap(reader, appliedResults, readApplyResult) || !reader.done()) {
        return false;
    }

    lastAppliedIndex_ = lastAppliedIndex;
    placementEpoch_ = placementEpoch;
    objects_ = std::move(objects);
    sessions_ = std::move(sessions);
    chunks_ = std::move(chunks);
    leases_ = std::move(leases);
    nodes_ = std::move(nodes);
    repairTasks_ = std::move(repairTasks);
    appliedResults_ = std::move(appliedResults);
    return true;
}

std::string MetadataStateMachine::stateDigest() const
{
    const auto snapshotBytes = snapshot().bytes;
    uint64_t hash = 1469598103934665603ULL;
    for(const unsigned char byte : snapshotBytes) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    std::ostringstream stream;
    stream << std::hex << std::setfill('0') << std::setw(16) << hash;
    return stream.str();
}

} // namespace miniKV::metadata
