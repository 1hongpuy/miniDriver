#include "metadata/MetadataCommand.hpp"

#include <limits>

namespace miniKV::metadata {
namespace {

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

constexpr uint32_t kMaxCollectionEntries = 1U << 20;

void writeInitialChunks(Writer& writer, const std::vector<InitialChunk>& chunks)
{
    writer.u32(static_cast<uint32_t>(chunks.size()));
    for(const auto& chunk : chunks) {
        writer.u32(chunk.index);
        writer.string(chunk.chunkHash);
        writer.u64(chunk.size);
    }
}

bool readInitialChunks(Reader& reader, std::vector<InitialChunk>& chunks)
{
    uint32_t count = 0;
    if(!reader.u32(count) || count > kMaxCollectionEntries) return false;
    chunks.clear();
    chunks.reserve(count);
    for(uint32_t i = 0; i < count; ++i) {
        InitialChunk chunk;
        if(!reader.u32(chunk.index) || !reader.string(chunk.chunkHash) || !reader.u64(chunk.size)) return false;
        chunks.push_back(std::move(chunk));
    }
    return true;
}

void writeStrings(Writer& writer, const std::vector<std::string>& values)
{
    writer.u32(static_cast<uint32_t>(values.size()));
    for(const auto& value : values) writer.string(value);
}

bool readStrings(Reader& reader, std::vector<std::string>& values)
{
    uint32_t count = 0;
    if(!reader.u32(count) || count > kMaxCollectionEntries) return false;
    values.clear();
    values.reserve(count);
    for(uint32_t i = 0; i < count; ++i) {
        std::string value;
        if(!reader.string(value)) return false;
        values.push_back(std::move(value));
    }
    return true;
}

void writeReplicaCommits(Writer& writer, const std::vector<ReplicaCommit>& replicas)
{
    writer.u32(static_cast<uint32_t>(replicas.size()));
    for(const auto& replica : replicas) {
        writer.string(replica.nodeId);
        writer.u64(replica.nodeEpoch);
        writer.string(replica.verifiedHash);
        writer.i64(replica.verifiedAt);
    }
}

bool readReplicaCommits(Reader& reader, std::vector<ReplicaCommit>& replicas)
{
    uint32_t count = 0;
    if(!reader.u32(count) || count > kMaxCollectionEntries) return false;
    replicas.clear();
    replicas.reserve(count);
    for(uint32_t i = 0; i < count; ++i) {
        ReplicaCommit replica;
        if(!reader.string(replica.nodeId) || !reader.u64(replica.nodeEpoch)
           || !reader.string(replica.verifiedHash) || !reader.i64(replica.verifiedAt)) {
            return false;
        }
        replicas.push_back(std::move(replica));
    }
    return true;
}

bool isKnownCommandType(uint8_t value)
{
    return value <= static_cast<uint8_t>(MetadataCommandType::kFailRepair);
}

bool isKnownNodeHealth(uint8_t value)
{
    return value <= static_cast<uint8_t>(NodeHealth::kRecovering);
}

} // namespace

std::optional<std::string> encodeMetadataCommand(const MetadataCommand& command)
{
    if(command.schemaVersion != 1 || command.commandId.empty()) return std::nullopt;

    Writer writer;
    writer.u32(command.schemaVersion);
    writer.string(command.commandId);
    writer.u8(static_cast<uint8_t>(command.type));
    writer.string(command.actorType);
    writer.string(command.actorId);
    writer.u64(command.expectedMetadataVersion);
    writer.u64(command.placementEpoch);
    writer.u64(command.nodeEpoch);
    writer.u64(command.generation);
    writer.i64(command.issuedAt);

    switch(command.type) {
    case MetadataCommandType::kCreateSession: {
        const auto* payload = std::get_if<CreateSessionPayload>(&command.payload);
        if(payload == nullptr) return std::nullopt;
        writer.string(payload->sessionId);
        writer.string(payload->objectId);
        writer.string(payload->ownerId);
        writer.string(payload->parentPath);
        writer.string(payload->name);
        writer.string(payload->fileHash);
        writer.string(payload->manifestHash);
        writer.u64(payload->fileSize);
        writer.u32(payload->chunkSize);
        writer.u32(payload->desiredRf);
        writer.i64(payload->expiresAt);
        writeInitialChunks(writer, payload->chunks);
        break;
    }
    case MetadataCommandType::kRegisterNode: {
        const auto* payload = std::get_if<RegisterNodePayload>(&command.payload);
        if(payload == nullptr) return std::nullopt;
        writer.string(payload->nodeId);
        writer.string(payload->bootId);
        writer.string(payload->address);
        writer.u16(payload->dataPort);
        writer.i64(payload->observedAt);
        break;
    }
    case MetadataCommandType::kHeartbeatNode: {
        const auto* payload = std::get_if<HeartbeatNodePayload>(&command.payload);
        if(payload == nullptr) return std::nullopt;
        writer.string(payload->nodeId);
        writer.u64(payload->freeBytes);
        writer.u32(payload->activeUploads);
        writer.u32(payload->activeDownloads);
        writer.u32(payload->diskQueueDepth);
        writer.u64(payload->diskPauseMs);
        writer.u64(payload->eventLoopLagUs);
        writer.i64(payload->observedAt);
        break;
    }
    case MetadataCommandType::kMarkNodeHealth: {
        const auto* payload = std::get_if<MarkNodeHealthPayload>(&command.payload);
        if(payload == nullptr) return std::nullopt;
        writer.string(payload->nodeId);
        writer.u8(static_cast<uint8_t>(payload->health));
        writer.i64(payload->observedAt);
        break;
    }
    case MetadataCommandType::kReserveLease: {
        const auto* payload = std::get_if<ReserveLeasePayload>(&command.payload);
        if(payload == nullptr) return std::nullopt;
        writer.string(payload->leaseId);
        writer.string(payload->requestKey);
        writer.string(payload->sessionId);
        writer.u32(payload->chunkIndex);
        writer.string(payload->chunkHash);
        writer.u64(payload->chunkSize);
        writeStrings(writer, payload->targetNodeIds);
        writer.i64(payload->expiresAt);
        break;
    }
    case MetadataCommandType::kCommitChunk: {
        const auto* payload = std::get_if<CommitChunkPayload>(&command.payload);
        if(payload == nullptr) return std::nullopt;
        writer.string(payload->sessionId);
        writer.string(payload->leaseId);
        writer.u32(payload->chunkIndex);
        writer.string(payload->chunkHash);
        writer.u64(payload->chunkSize);
        writeReplicaCommits(writer, payload->replicas);
        break;
    }
    case MetadataCommandType::kCommitFile: {
        const auto* payload = std::get_if<CommitFilePayload>(&command.payload);
        if(payload == nullptr) return std::nullopt;
        writer.string(payload->sessionId);
        writer.string(payload->objectId);
        writer.string(payload->fileHash);
        break;
    }
    default:
        return std::nullopt;
    }

    return writer.take();
}

std::optional<MetadataCommand> decodeMetadataCommand(const std::string& bytes)
{
    Reader reader(bytes);
    MetadataCommand command;
    uint8_t rawType = 0;
    if(!reader.u32(command.schemaVersion) || command.schemaVersion != 1
       || !reader.string(command.commandId) || command.commandId.empty() || !reader.u8(rawType)
       || !isKnownCommandType(rawType) || !reader.string(command.actorType)
       || !reader.string(command.actorId) || !reader.u64(command.expectedMetadataVersion)
       || !reader.u64(command.placementEpoch) || !reader.u64(command.nodeEpoch)
       || !reader.u64(command.generation) || !reader.i64(command.issuedAt)) {
        return std::nullopt;
    }
    command.type = static_cast<MetadataCommandType>(rawType);

    switch(command.type) {
    case MetadataCommandType::kCreateSession: {
        CreateSessionPayload payload;
        if(!reader.string(payload.sessionId) || !reader.string(payload.objectId)
           || !reader.string(payload.ownerId) || !reader.string(payload.parentPath)
           || !reader.string(payload.name) || !reader.string(payload.fileHash)
           || !reader.string(payload.manifestHash) || !reader.u64(payload.fileSize)
           || !reader.u32(payload.chunkSize) || !reader.u32(payload.desiredRf)
           || !reader.i64(payload.expiresAt) || !readInitialChunks(reader, payload.chunks)) {
            return std::nullopt;
        }
        command.payload = std::move(payload);
        break;
    }
    case MetadataCommandType::kRegisterNode: {
        RegisterNodePayload payload;
        if(!reader.string(payload.nodeId) || !reader.string(payload.bootId) || !reader.string(payload.address)
           || !reader.u16(payload.dataPort) || !reader.i64(payload.observedAt)) {
            return std::nullopt;
        }
        command.payload = std::move(payload);
        break;
    }
    case MetadataCommandType::kHeartbeatNode: {
        HeartbeatNodePayload payload;
        if(!reader.string(payload.nodeId) || !reader.u64(payload.freeBytes)
           || !reader.u32(payload.activeUploads) || !reader.u32(payload.activeDownloads)
           || !reader.u32(payload.diskQueueDepth) || !reader.u64(payload.diskPauseMs)
           || !reader.u64(payload.eventLoopLagUs) || !reader.i64(payload.observedAt)) {
            return std::nullopt;
        }
        command.payload = std::move(payload);
        break;
    }
    case MetadataCommandType::kMarkNodeHealth: {
        MarkNodeHealthPayload payload;
        uint8_t rawHealth = 0;
        if(!reader.string(payload.nodeId) || !reader.u8(rawHealth) || !isKnownNodeHealth(rawHealth)
           || !reader.i64(payload.observedAt)) {
            return std::nullopt;
        }
        payload.health = static_cast<NodeHealth>(rawHealth);
        command.payload = std::move(payload);
        break;
    }
    case MetadataCommandType::kReserveLease: {
        ReserveLeasePayload payload;
        if(!reader.string(payload.leaseId) || !reader.string(payload.requestKey)
           || !reader.string(payload.sessionId) || !reader.u32(payload.chunkIndex)
           || !reader.string(payload.chunkHash) || !reader.u64(payload.chunkSize)
           || !readStrings(reader, payload.targetNodeIds) || !reader.i64(payload.expiresAt)) {
            return std::nullopt;
        }
        command.payload = std::move(payload);
        break;
    }
    case MetadataCommandType::kCommitChunk: {
        CommitChunkPayload payload;
        if(!reader.string(payload.sessionId) || !reader.string(payload.leaseId)
           || !reader.u32(payload.chunkIndex) || !reader.string(payload.chunkHash)
           || !reader.u64(payload.chunkSize) || !readReplicaCommits(reader, payload.replicas)) {
            return std::nullopt;
        }
        command.payload = std::move(payload);
        break;
    }
    case MetadataCommandType::kCommitFile: {
        CommitFilePayload payload;
        if(!reader.string(payload.sessionId) || !reader.string(payload.objectId)
           || !reader.string(payload.fileHash)) {
            return std::nullopt;
        }
        command.payload = std::move(payload);
        break;
    }
    default:
        return std::nullopt;
    }

    if(!reader.done()) return std::nullopt;
    return command;
}

} // namespace miniKV::metadata
