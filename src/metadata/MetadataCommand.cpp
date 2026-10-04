#include "metadata/MetadataCommand.hpp"

#include <openssl/sha.h>

#include <iomanip>
#include <limits>
#include <sstream>

namespace miniKV::metadata {
namespace {

constexpr uint32_t kMaxCollectionEntries = 1U << 20;

class Writer {
public:
    void u8(uint8_t value) { bytes_.push_back(static_cast<char>(value)); }
    void boolean(bool value) { u8(value ? 1 : 0); }
    void u16(uint16_t value) { for(int s = 0; s < 16; s += 8) u8(static_cast<uint8_t>(value >> s)); }
    void u32(uint32_t value) { for(int s = 0; s < 32; s += 8) u8(static_cast<uint8_t>(value >> s)); }
    void u64(uint64_t value) { for(int s = 0; s < 64; s += 8) u8(static_cast<uint8_t>(value >> s)); }
    void i64(int64_t value) { u64(static_cast<uint64_t>(value)); }
    void string(const std::string& value)
    {
        if(value.size() > std::numeric_limits<uint32_t>::max()) throw std::length_error("metadata string too large");
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
    bool u8(uint8_t& value) { if(pos_ == bytes_.size()) return false; value = static_cast<uint8_t>(bytes_[pos_++]); return true; }
    bool boolean(bool& value) { uint8_t raw = 0; if(!u8(raw) || raw > 1) return false; value = raw != 0; return true; }
    bool u16(uint16_t& value) { uint8_t b = 0; value = 0; for(int s = 0; s < 16; s += 8) { if(!u8(b)) return false; value |= static_cast<uint16_t>(b) << s; } return true; }
    bool u32(uint32_t& value) { uint8_t b = 0; value = 0; for(int s = 0; s < 32; s += 8) { if(!u8(b)) return false; value |= static_cast<uint32_t>(b) << s; } return true; }
    bool u64(uint64_t& value) { uint8_t b = 0; value = 0; for(int s = 0; s < 64; s += 8) { if(!u8(b)) return false; value |= static_cast<uint64_t>(b) << s; } return true; }
    bool i64(int64_t& value) { uint64_t raw = 0; if(!u64(raw)) return false; value = static_cast<int64_t>(raw); return true; }
    bool string(std::string& value) { uint32_t n = 0; if(!u32(n) || n > bytes_.size() - pos_) return false; value.assign(bytes_.data() + pos_, n); pos_ += n; return true; }
    bool done() const { return pos_ == bytes_.size(); }
private:
    const std::string& bytes_;
    size_t pos_ = 0;
};

template <typename T, typename Fn>
void writeVector(Writer& writer, const std::vector<T>& values, Fn fn)
{
    writer.u32(static_cast<uint32_t>(values.size()));
    for(const auto& value : values) fn(writer, value);
}

template <typename T, typename Fn>
bool readVector(Reader& reader, std::vector<T>& values, Fn fn)
{
    uint32_t count = 0;
    if(!reader.u32(count) || count > kMaxCollectionEntries) return false;
    values.clear();
    values.reserve(count);
    for(uint32_t i = 0; i < count; ++i) {
        T value;
        if(!fn(reader, value)) return false;
        values.push_back(std::move(value));
    }
    return true;
}

void writeStrings(Writer& writer, const std::vector<std::string>& values)
{
    writeVector(writer, values, [](Writer& out, const std::string& value) { out.string(value); });
}

bool readStrings(Reader& reader, std::vector<std::string>& values)
{
    return readVector(reader, values, [](Reader& in, std::string& value) { return in.string(value); });
}

bool knownType(uint8_t value)
{
    return value >= static_cast<uint8_t>(MetadataCommandType::kCreateSession)
        && value <= static_cast<uint8_t>(MetadataCommandType::kAcknowledgeDelete);
}

bool knownIdentity(uint8_t value) { return value <= static_cast<uint8_t>(IdentityScheme::kContentHash); }
bool knownChecksum(uint8_t value) { return value <= static_cast<uint8_t>(ChecksumType::kSha256); }
bool knownHealth(uint8_t value) { return value <= static_cast<uint8_t>(NodeHealth::kOffline); }

bool writePayload(Writer& writer, const MetadataCommand& command)
{
    switch(command.type) {
    case MetadataCommandType::kCreateSession: {
        const auto* p = std::get_if<CreateSessionPayload>(&command.payload); if(!p) return false;
        writer.string(p->sessionId); writer.string(p->objectId); writer.u64(p->objectVersion);
        writer.string(p->ownerId); writer.string(p->parentPath); writer.string(p->name);
        writer.string(p->contentHash); writer.u64(p->fileSize); writer.u32(p->chunkSize);
        writer.u32(p->desiredRf); writer.i64(p->expiresAt);
        writeVector(writer, p->chunks, [](Writer& out, const InitialChunk& c) {
            out.u32(c.index); out.string(c.routeKey); out.string(c.storageIdentity);
            out.u8(static_cast<uint8_t>(c.identityScheme)); out.u8(static_cast<uint8_t>(c.checksumType));
            out.string(c.checksumDigest); out.u64(c.size); out.u64(c.generation);
        });
        return true;
    }
    case MetadataCommandType::kRegisterNode: {
        const auto* p = std::get_if<RegisterNodePayload>(&command.payload); if(!p) return false;
        writer.string(p->nodeId); writer.string(p->bootId); writer.string(p->address); writer.u16(p->dataPort);
        writer.u64(p->registeredCapacityBytes); writeStrings(writer, p->capabilities); return true;
    }
    case MetadataCommandType::kMarkNodeHealth: {
        const auto* p = std::get_if<MarkNodeHealthPayload>(&command.payload); if(!p) return false;
        writer.string(p->nodeId); writer.u8(static_cast<uint8_t>(p->health)); writer.i64(p->observedAt); return true;
    }
    case MetadataCommandType::kSetNodeDraining: {
        const auto* p = std::get_if<SetNodeDrainingPayload>(&command.payload); if(!p) return false;
        writer.string(p->nodeId); writer.boolean(p->draining); return true;
    }
    case MetadataCommandType::kReserveLease: {
        const auto* p = std::get_if<ReserveLeasePayload>(&command.payload); if(!p) return false;
        writer.string(p->leaseId); writer.string(p->requestKey); writer.string(p->sessionId);
        writer.u32(p->chunkIndex); writer.string(p->routeKey); writer.u64(p->chunkSize);
        writeVector(writer, p->targets, [](Writer& out, const LeaseTarget& target) { out.string(target.nodeId); out.u64(target.nodeEpoch); });
        writer.i64(p->expiresAt); return true;
    }
    case MetadataCommandType::kReleaseLease: {
        const auto* p = std::get_if<ReleaseLeasePayload>(&command.payload); if(!p) return false;
        writer.string(p->leaseId); writer.u64(p->expectedGeneration); return true;
    }
    case MetadataCommandType::kExpireLease: {
        const auto* p = std::get_if<ExpireLeasePayload>(&command.payload); if(!p) return false;
        writer.string(p->leaseId); writer.u64(p->expectedGeneration); writer.i64(p->expectedExpiresAt); writer.i64(p->observedAt); return true;
    }
    case MetadataCommandType::kCommitChunk: {
        const auto* p = std::get_if<CommitChunkPayload>(&command.payload); if(!p) return false;
        writer.string(p->sessionId); writer.string(p->leaseId); writer.u32(p->chunkIndex);
        writer.string(p->routeKey); writer.u64(p->chunkSize); writer.u8(static_cast<uint8_t>(p->checksumType));
        writer.string(p->checksumDigest);
        writeVector(writer, p->replicas, [](Writer& out, const ReplicaCommit& replica) {
            out.string(replica.nodeId); out.u64(replica.nodeEpoch); out.string(replica.checksumDigest); out.i64(replica.verifiedAt);
        });
        return true;
    }
    case MetadataCommandType::kCommitFile: {
        const auto* p = std::get_if<CommitFilePayload>(&command.payload); if(!p) return false;
        writer.string(p->sessionId); writer.string(p->objectId); writer.u64(p->objectVersion); writer.string(p->contentHash); return true;
    }
    case MetadataCommandType::kCreateDirectory: {
        const auto* p = std::get_if<CreateDirectoryPayload>(&command.payload); if(!p) return false;
        writer.string(p->ownerId); writer.string(p->path); writer.i64(p->createdAt); return true;
    }
    case MetadataCommandType::kDeleteObject: {
        const auto* p = std::get_if<DeleteObjectPayload>(&command.payload); if(!p) return false;
        writer.string(p->objectId); writer.u64(p->objectVersion); return true;
    }
    case MetadataCommandType::kDeleteDirectory: {
        const auto* p = std::get_if<DeleteDirectoryPayload>(&command.payload); if(!p) return false;
        writer.string(p->ownerId); writer.string(p->path); return true;
    }
    case MetadataCommandType::kExpireSession: {
        const auto* p = std::get_if<ExpireSessionPayload>(&command.payload); if(!p) return false;
        writer.string(p->sessionId); writer.i64(p->expectedExpiresAt); writer.i64(p->observedAt); return true;
    }
    case MetadataCommandType::kAcknowledgeDelete: {
        const auto* p = std::get_if<AcknowledgeDeletePayload>(&command.payload); if(!p) return false;
        writer.string(p->objectId); writer.u64(p->objectVersion); writer.u32(p->chunkIndex);
        writer.string(p->nodeId); writer.u64(p->nodeEpoch); return true;
    }
    case MetadataCommandType::kReadBarrier: {
        const auto* p = std::get_if<ReadBarrierPayload>(&command.payload); if(!p) return false;
        writer.u64(p->nonce); return true;
    }
    }
    return false;
}

std::optional<std::string> encodeInternal(const MetadataCommand& command, bool includeCommandId)
{
    if(command.schemaVersion != kMetadataSchemaVersion || (includeCommandId && command.commandId.empty())) return std::nullopt;
    try {
        Writer writer;
        writer.u32(command.schemaVersion);
        if(includeCommandId) writer.string(command.commandId);
        writer.u8(static_cast<uint8_t>(command.type));
        writer.string(command.actorType); writer.string(command.actorId);
        writer.u64(command.expectedMetadataVersion); writer.u64(command.placementEpoch);
        writer.u64(command.nodeEpoch); writer.u64(command.generation); writer.i64(command.issuedAt);
        if(!writePayload(writer, command)) return std::nullopt;
        return writer.take();
    } catch(const std::length_error&) {
        return std::nullopt;
    }
}

} // namespace

std::optional<std::string> encodeMetadataCommand(const MetadataCommand& command)
{
    return encodeInternal(command, true);
}

std::optional<std::string> encodeMetadataCommandBatch(const std::vector<MetadataCommand>& commands)
{
    constexpr uint32_t kMaxBatchCommands = 4096;
    constexpr uint64_t kMaxBatchBytes = 64ULL * 1024ULL * 1024ULL;
    if(commands.empty() || commands.size() > kMaxBatchCommands) return std::nullopt;
    std::string output;
    output.reserve(4);
    auto appendU32 = [&output](uint32_t value) {
        for(int shift = 0; shift < 32; shift += 8)
            output.push_back(static_cast<char>((value >> shift) & 0xff));
    };
    appendU32(static_cast<uint32_t>(commands.size()));
    for(const auto& command : commands) {
        const auto encoded = encodeMetadataCommand(command);
        if(!encoded || encoded->size() > std::numeric_limits<uint32_t>::max() ||
           output.size() + 4ULL + encoded->size() > kMaxBatchBytes)
            return std::nullopt;
        appendU32(static_cast<uint32_t>(encoded->size()));
        output.append(*encoded);
    }
    return output;
}

std::optional<std::vector<MetadataCommand>> decodeMetadataCommandBatch(const std::string& bytes)
{
    constexpr uint32_t kMaxBatchCommands = 4096;
    constexpr uint64_t kMaxBatchBytes = 64ULL * 1024ULL * 1024ULL;
    if(bytes.size() < 4 || bytes.size() > kMaxBatchBytes) return std::nullopt;
    size_t position = 0;
    auto readU32 = [&bytes, &position](uint32_t& value) {
        if(position + 4 > bytes.size()) return false;
        value = 0;
        for(int shift = 0; shift < 32; shift += 8)
            value |= static_cast<uint32_t>(static_cast<unsigned char>(bytes[position++])) << shift;
        return true;
    };
    uint32_t count = 0;
    if(!readU32(count) || count == 0 || count > kMaxBatchCommands) return std::nullopt;
    std::vector<MetadataCommand> commands;
    commands.reserve(count);
    for(uint32_t i = 0; i < count; ++i) {
        uint32_t length = 0;
        if(!readU32(length) || length == 0 || position + length > bytes.size()) return std::nullopt;
        const auto command = decodeMetadataCommand(bytes.substr(position, length));
        if(!command) return std::nullopt;
        commands.push_back(*command);
        position += length;
    }
    if(position != bytes.size()) return std::nullopt;
    return commands;
}

std::optional<std::string> canonicalCommandBytes(const MetadataCommand& command)
{
    // Wall-clock issuance is audit metadata, not request identity.  Excluding
    // it from the canonical bytes makes a stable commandId replayable after a
    // client timeout even when the retry is reconstructed seconds later.
    MetadataCommand canonical = command;
    canonical.issuedAt = 0;
    return encodeInternal(canonical, false);
}

std::optional<std::string> commandFingerprint(const MetadataCommand& command)
{
    const auto bytes = canonicalCommandBytes(command);
    if(!bytes) return std::nullopt;
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(bytes->data()), bytes->size(), digest);
    std::ostringstream output;
    output << std::hex << std::setfill('0');
    for(unsigned char byte : digest) output << std::setw(2) << static_cast<unsigned>(byte);
    return output.str();
}

std::optional<MetadataCommand> decodeMetadataCommand(const std::string& bytes)
{
    Reader reader(bytes);
    MetadataCommand command;
    uint8_t rawType = 0;
    if(!reader.u32(command.schemaVersion) || command.schemaVersion != kMetadataSchemaVersion
       || !reader.string(command.commandId) || command.commandId.empty() || !reader.u8(rawType) || !knownType(rawType)
       || !reader.string(command.actorType) || !reader.string(command.actorId)
       || !reader.u64(command.expectedMetadataVersion) || !reader.u64(command.placementEpoch)
       || !reader.u64(command.nodeEpoch) || !reader.u64(command.generation) || !reader.i64(command.issuedAt)) return std::nullopt;
    command.type = static_cast<MetadataCommandType>(rawType);

    switch(command.type) {
    case MetadataCommandType::kCreateSession: {
        CreateSessionPayload p;
        if(!reader.string(p.sessionId) || !reader.string(p.objectId) || !reader.u64(p.objectVersion)
           || !reader.string(p.ownerId) || !reader.string(p.parentPath) || !reader.string(p.name)
           || !reader.string(p.contentHash) || !reader.u64(p.fileSize) || !reader.u32(p.chunkSize)
           || !reader.u32(p.desiredRf) || !reader.i64(p.expiresAt)
           || !readVector(reader, p.chunks, [](Reader& in, InitialChunk& c) {
               uint8_t identity = 0, checksum = 0;
               if(!in.u32(c.index) || !in.string(c.routeKey) || !in.string(c.storageIdentity)
                  || !in.u8(identity) || !knownIdentity(identity) || !in.u8(checksum) || !knownChecksum(checksum)
                  || !in.string(c.checksumDigest) || !in.u64(c.size) || !in.u64(c.generation)) return false;
               c.identityScheme = static_cast<IdentityScheme>(identity); c.checksumType = static_cast<ChecksumType>(checksum); return true;
           })) return std::nullopt;
        command.payload = std::move(p); break;
    }
    case MetadataCommandType::kRegisterNode: {
        RegisterNodePayload p;
        if(!reader.string(p.nodeId) || !reader.string(p.bootId) || !reader.string(p.address) || !reader.u16(p.dataPort)
           || !reader.u64(p.registeredCapacityBytes) || !readStrings(reader, p.capabilities)) return std::nullopt;
        command.payload = std::move(p); break;
    }
    case MetadataCommandType::kMarkNodeHealth: {
        MarkNodeHealthPayload p; uint8_t health = 0;
        if(!reader.string(p.nodeId) || !reader.u8(health) || !knownHealth(health) || !reader.i64(p.observedAt)) return std::nullopt;
        p.health = static_cast<NodeHealth>(health); command.payload = std::move(p); break;
    }
    case MetadataCommandType::kSetNodeDraining: {
        SetNodeDrainingPayload p; if(!reader.string(p.nodeId) || !reader.boolean(p.draining)) return std::nullopt;
        command.payload = std::move(p); break;
    }
    case MetadataCommandType::kReserveLease: {
        ReserveLeasePayload p;
        if(!reader.string(p.leaseId) || !reader.string(p.requestKey) || !reader.string(p.sessionId)
           || !reader.u32(p.chunkIndex) || !reader.string(p.routeKey) || !reader.u64(p.chunkSize)
           || !readVector(reader, p.targets, [](Reader& in, LeaseTarget& t) { return in.string(t.nodeId) && in.u64(t.nodeEpoch); })
           || !reader.i64(p.expiresAt)) return std::nullopt;
        command.payload = std::move(p); break;
    }
    case MetadataCommandType::kReleaseLease: {
        ReleaseLeasePayload p; if(!reader.string(p.leaseId) || !reader.u64(p.expectedGeneration)) return std::nullopt;
        command.payload = std::move(p); break;
    }
    case MetadataCommandType::kExpireLease: {
        ExpireLeasePayload p;
        if(!reader.string(p.leaseId) || !reader.u64(p.expectedGeneration) || !reader.i64(p.expectedExpiresAt) || !reader.i64(p.observedAt)) return std::nullopt;
        command.payload = std::move(p); break;
    }
    case MetadataCommandType::kCommitChunk: {
        CommitChunkPayload p; uint8_t checksum = 0;
        if(!reader.string(p.sessionId) || !reader.string(p.leaseId) || !reader.u32(p.chunkIndex)
           || !reader.string(p.routeKey) || !reader.u64(p.chunkSize) || !reader.u8(checksum) || !knownChecksum(checksum)
           || !reader.string(p.checksumDigest)
           || !readVector(reader, p.replicas, [](Reader& in, ReplicaCommit& replica) {
               return in.string(replica.nodeId) && in.u64(replica.nodeEpoch)
                   && in.string(replica.checksumDigest) && in.i64(replica.verifiedAt);
           })) return std::nullopt;
        p.checksumType = static_cast<ChecksumType>(checksum); command.payload = std::move(p); break;
    }
    case MetadataCommandType::kCommitFile: {
        CommitFilePayload p;
        if(!reader.string(p.sessionId) || !reader.string(p.objectId) || !reader.u64(p.objectVersion) || !reader.string(p.contentHash)) return std::nullopt;
        command.payload = std::move(p); break;
    }
    case MetadataCommandType::kCreateDirectory: {
        CreateDirectoryPayload p;
        if(!reader.string(p.ownerId) || !reader.string(p.path) || !reader.i64(p.createdAt)) return std::nullopt;
        command.payload = std::move(p); break;
    }
    case MetadataCommandType::kDeleteObject: {
        DeleteObjectPayload p;
        if(!reader.string(p.objectId) || !reader.u64(p.objectVersion)) return std::nullopt;
        command.payload = std::move(p); break;
    }
    case MetadataCommandType::kDeleteDirectory: {
        DeleteDirectoryPayload p;
        if(!reader.string(p.ownerId) || !reader.string(p.path)) return std::nullopt;
        command.payload = std::move(p); break;
    }
    case MetadataCommandType::kExpireSession: {
        ExpireSessionPayload p;
        if(!reader.string(p.sessionId) || !reader.i64(p.expectedExpiresAt) || !reader.i64(p.observedAt)) return std::nullopt;
        command.payload = std::move(p); break;
    }
    case MetadataCommandType::kAcknowledgeDelete: {
        AcknowledgeDeletePayload p;
        if(!reader.string(p.objectId) || !reader.u64(p.objectVersion) || !reader.u32(p.chunkIndex)
           || !reader.string(p.nodeId) || !reader.u64(p.nodeEpoch)) return std::nullopt;
        command.payload = std::move(p); break;
    }
    case MetadataCommandType::kReadBarrier: {
        ReadBarrierPayload p; if(!reader.u64(p.nonce)) return std::nullopt; command.payload = p; break;
    }
    }
    if(!reader.done()) return std::nullopt;
    return command;
}

} // namespace miniKV::metadata
