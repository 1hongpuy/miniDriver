#include "metadata/MetadataStateMachine.hpp"

#include <algorithm>
#include <iomanip>
#include <set>
#include <sstream>
#include <utility>

namespace miniKV::metadata {
namespace {

constexpr uint32_t kMaxEntries = 1U << 20;

class Writer {
public:
    void u8(uint8_t v) { bytes_.push_back(static_cast<char>(v)); }
    void boolean(bool v) { u8(v ? 1 : 0); }
    void u16(uint16_t v) { for(int s = 0; s < 16; s += 8) u8(static_cast<uint8_t>(v >> s)); }
    void u32(uint32_t v) { for(int s = 0; s < 32; s += 8) u8(static_cast<uint8_t>(v >> s)); }
    void u64(uint64_t v) { for(int s = 0; s < 64; s += 8) u8(static_cast<uint8_t>(v >> s)); }
    void i64(int64_t v) { u64(static_cast<uint64_t>(v)); }
    void string(const std::string& v) { u32(static_cast<uint32_t>(v.size())); bytes_.append(v); }
    std::string take() { return std::move(bytes_); }
private:
    std::string bytes_;
};

class Reader {
public:
    explicit Reader(const std::string& bytes) : bytes_(bytes) {}
    bool u8(uint8_t& v) { if(pos_ == bytes_.size()) return false; v = static_cast<uint8_t>(bytes_[pos_++]); return true; }
    bool boolean(bool& v) { uint8_t raw = 0; if(!u8(raw) || raw > 1) return false; v = raw != 0; return true; }
    bool u16(uint16_t& v) { uint8_t b = 0; v = 0; for(int s = 0; s < 16; s += 8) { if(!u8(b)) return false; v |= static_cast<uint16_t>(b) << s; } return true; }
    bool u32(uint32_t& v) { uint8_t b = 0; v = 0; for(int s = 0; s < 32; s += 8) { if(!u8(b)) return false; v |= static_cast<uint32_t>(b) << s; } return true; }
    bool u64(uint64_t& v) { uint8_t b = 0; v = 0; for(int s = 0; s < 64; s += 8) { if(!u8(b)) return false; v |= static_cast<uint64_t>(b) << s; } return true; }
    bool i64(int64_t& v) { uint64_t raw = 0; if(!u64(raw)) return false; v = static_cast<int64_t>(raw); return true; }
    bool string(std::string& v) { uint32_t n = 0; if(!u32(n) || n > bytes_.size() - pos_) return false; v.assign(bytes_.data() + pos_, n); pos_ += n; return true; }
    bool done() const { return pos_ == bytes_.size(); }
private:
    const std::string& bytes_;
    size_t pos_ = 0;
};

template <typename T, typename Fn>
void writeVector(Writer& out, const std::vector<T>& values, Fn fn)
{
    out.u32(static_cast<uint32_t>(values.size()));
    for(const auto& value : values) fn(out, value);
}

template <typename T, typename Fn>
bool readVector(Reader& in, std::vector<T>& values, Fn fn)
{
    uint32_t count = 0;
    if(!in.u32(count) || count > kMaxEntries) return false;
    values.clear(); values.reserve(count);
    for(uint32_t i = 0; i < count; ++i) { T value; if(!fn(in, value)) return false; values.push_back(std::move(value)); }
    return true;
}

template <typename T, typename Fn>
void writeMap(Writer& out, const std::map<std::string, T>& values, Fn fn)
{
    out.u32(static_cast<uint32_t>(values.size()));
    for(const auto& [key, value] : values) { out.string(key); fn(out, value); }
}

template <typename T, typename Fn>
bool readMap(Reader& in, std::map<std::string, T>& values, Fn fn)
{
    uint32_t count = 0;
    if(!in.u32(count) || count > kMaxEntries) return false;
    values.clear();
    for(uint32_t i = 0; i < count; ++i) {
        std::string key; T value;
        if(!in.string(key) || key.empty() || !fn(in, value) || !values.emplace(std::move(key), std::move(value)).second) return false;
    }
    return true;
}

void writeStrings(Writer& out, const std::vector<std::string>& values)
{ writeVector(out, values, [](Writer& writer, const std::string& value) { writer.string(value); }); }
bool readStrings(Reader& in, std::vector<std::string>& values)
{ return readVector(in, values, [](Reader& reader, std::string& value) { return reader.string(value); }); }

void writeObject(Writer& out, const ObjectRecord& r)
{
    out.string(r.objectId); out.u64(r.objectVersion); out.u64(r.metadataVersion); out.string(r.ownerId);
    out.string(r.parentPath); out.string(r.name); out.u64(r.fileSize); out.u32(r.chunkSize);
    out.u32(r.desiredRf); out.string(r.contentHash); out.u8(static_cast<uint8_t>(r.state));
}
bool readObject(Reader& in, ObjectRecord& r)
{
    uint8_t state = 0;
    if(!in.string(r.objectId) || !in.u64(r.objectVersion) || !in.u64(r.metadataVersion) || !in.string(r.ownerId)
       || !in.string(r.parentPath) || !in.string(r.name) || !in.u64(r.fileSize) || !in.u32(r.chunkSize)
       || !in.u32(r.desiredRf) || !in.string(r.contentHash) || !in.u8(state)
       || state > static_cast<uint8_t>(ObjectState::kDeleting)) return false;
    r.state = static_cast<ObjectState>(state); return true;
}

void writeSession(Writer& out, const UploadSessionRecord& r)
{
    out.string(r.sessionId); out.string(r.objectId); out.u64(r.objectVersion); out.string(r.ownerId);
    out.u64(r.fileSize); out.u32(r.chunkSize); out.u32(r.totalChunks); out.u32(r.completedChunks); out.i64(r.expiresAt); out.boolean(r.expired);
}
bool readSession(Reader& in, UploadSessionRecord& r)
{
    return in.string(r.sessionId) && in.string(r.objectId) && in.u64(r.objectVersion) && in.string(r.ownerId)
        && in.u64(r.fileSize) && in.u32(r.chunkSize) && in.u32(r.totalChunks) && in.u32(r.completedChunks) && in.i64(r.expiresAt) && in.boolean(r.expired);
}

void writeDirectory(Writer& out, const DirectoryRecord& r)
{ out.string(r.ownerId); out.string(r.path); out.i64(r.createdAt); }
bool readDirectory(Reader& in, DirectoryRecord& r)
{ return in.string(r.ownerId) && in.string(r.path) && in.i64(r.createdAt); }

void writeReplica(Writer& out, const ReplicaRecord& r)
{
    out.string(r.nodeId); out.u64(r.nodeEpoch); out.u64(r.generation); out.u8(static_cast<uint8_t>(r.state));
    out.string(r.checksumDigest); out.i64(r.verifiedAt);
}
bool readReplica(Reader& in, ReplicaRecord& r)
{
    uint8_t state = 0;
    if(!in.string(r.nodeId) || !in.u64(r.nodeEpoch) || !in.u64(r.generation) || !in.u8(state)
       || state > static_cast<uint8_t>(ReplicaState::kOffline) || !in.string(r.checksumDigest) || !in.i64(r.verifiedAt)) return false;
    r.state = static_cast<ReplicaState>(state); return true;
}

void writeChunk(Writer& out, const ChunkRouteRecord& r)
{
    out.string(r.routeKey); out.string(r.objectId); out.u64(r.objectVersion); out.u32(r.index);
    out.string(r.storageIdentity); out.u8(static_cast<uint8_t>(r.identityScheme)); out.u8(static_cast<uint8_t>(r.checksumType));
    out.string(r.checksumDigest); out.u64(r.size); out.u32(r.desiredRf); out.u64(r.generation);
    out.u8(static_cast<uint8_t>(r.state)); writeVector(out, r.replicas, writeReplica);
}
bool readChunk(Reader& in, ChunkRouteRecord& r)
{
    uint8_t identity = 0, checksum = 0, state = 0;
    if(!in.string(r.routeKey) || !in.string(r.objectId) || !in.u64(r.objectVersion) || !in.u32(r.index)
       || !in.string(r.storageIdentity) || !in.u8(identity) || identity > static_cast<uint8_t>(IdentityScheme::kContentHash)
       || !in.u8(checksum) || checksum > static_cast<uint8_t>(ChecksumType::kSha256) || !in.string(r.checksumDigest)
       || !in.u64(r.size) || !in.u32(r.desiredRf) || !in.u64(r.generation) || !in.u8(state)
       || state > static_cast<uint8_t>(ChunkState::kFailed) || !readVector(in, r.replicas, readReplica)) return false;
    r.identityScheme = static_cast<IdentityScheme>(identity); r.checksumType = static_cast<ChecksumType>(checksum);
    r.state = static_cast<ChunkState>(state); return true;
}

void writeTarget(Writer& out, const LeaseTarget& t) { out.string(t.nodeId); out.u64(t.nodeEpoch); }
bool readTarget(Reader& in, LeaseTarget& t) { return in.string(t.nodeId) && in.u64(t.nodeEpoch); }
void writeDeleteTask(Writer& out, const DeleteTaskRecord& r)
{
    out.string(r.objectId); out.u64(r.objectVersion); out.u32(r.chunkIndex); out.string(r.storageIdentity);
    writeVector(out, r.pendingReplicas, writeTarget);
}
bool readDeleteTask(Reader& in, DeleteTaskRecord& r)
{
    return in.string(r.objectId) && in.u64(r.objectVersion) && in.u32(r.chunkIndex)
        && in.string(r.storageIdentity) && readVector(in, r.pendingReplicas, readTarget);
}
void writeLease(Writer& out, const LeaseRecord& r)
{
    out.string(r.leaseId); out.string(r.requestKey); out.string(r.sessionId); out.u32(r.chunkIndex);
    out.string(r.routeKey); out.u64(r.chunkSize); out.u64(r.placementEpoch); out.u64(r.generation);
    writeVector(out, r.targets, writeTarget); out.i64(r.expiresAt); out.u8(static_cast<uint8_t>(r.state));
}
bool readLease(Reader& in, LeaseRecord& r)
{
    uint8_t state = 0;
    if(!in.string(r.leaseId) || !in.string(r.requestKey) || !in.string(r.sessionId) || !in.u32(r.chunkIndex)
       || !in.string(r.routeKey) || !in.u64(r.chunkSize) || !in.u64(r.placementEpoch) || !in.u64(r.generation)
       || !readVector(in, r.targets, readTarget) || !in.i64(r.expiresAt) || !in.u8(state)
       || state > static_cast<uint8_t>(LeaseState::kExpired)) return false;
    r.state = static_cast<LeaseState>(state); return true;
}

void writeNode(Writer& out, const NodeRecord& r)
{
    out.string(r.nodeId); out.string(r.bootId); out.u64(r.nodeEpoch); out.string(r.address); out.u16(r.dataPort);
    out.u64(r.registeredCapacityBytes); writeStrings(out, r.capabilities); out.u8(static_cast<uint8_t>(r.health));
    out.boolean(r.draining); out.u64(r.placementEpoch); out.u64(r.reservedBytes); out.u32(r.reservedWrites);
}
bool readNode(Reader& in, NodeRecord& r)
{
    uint8_t health = 0;
    if(!in.string(r.nodeId) || !in.string(r.bootId) || !in.u64(r.nodeEpoch) || !in.string(r.address) || !in.u16(r.dataPort)
       || !in.u64(r.registeredCapacityBytes) || !readStrings(in, r.capabilities) || !in.u8(health)
       || health > static_cast<uint8_t>(NodeHealth::kOffline) || !in.boolean(r.draining)
       || !in.u64(r.placementEpoch) || !in.u64(r.reservedBytes) || !in.u32(r.reservedWrites)) return false;
    r.health = static_cast<NodeHealth>(health); return true;
}

void writeResult(Writer& out, const ApplyResult& r)
{
    out.string(r.commandId); out.u8(static_cast<uint8_t>(r.status)); out.u64(r.metadataVersion);
    out.u64(r.appliedIndex); out.u64(r.appliedTerm); out.u64(r.nodeEpoch); out.u64(r.placementEpoch);
    out.string(r.objectId); out.u64(r.objectVersion); out.string(r.sessionId); out.string(r.leaseId); out.string(r.message);
}
bool readResult(Reader& in, ApplyResult& r)
{
    uint8_t status = 0;
    if(!in.string(r.commandId) || !in.u8(status) || status > static_cast<uint8_t>(ApplyStatus::kUnavailable)
       || !in.u64(r.metadataVersion) || !in.u64(r.appliedIndex) || !in.u64(r.appliedTerm)
       || !in.u64(r.nodeEpoch) || !in.u64(r.placementEpoch) || !in.string(r.objectId)
       || !in.u64(r.objectVersion) || !in.string(r.sessionId) || !in.string(r.leaseId) || !in.string(r.message)) return false;
    r.status = static_cast<ApplyStatus>(status); return true;
}

void writeDedup(Writer& out, const DedupEntry& r)
{ out.string(r.commandId); out.u8(r.commandType); out.string(r.requestFingerprint); writeResult(out, r.result); out.u64(r.appliedIndex); }
bool readDedup(Reader& in, DedupEntry& r)
{ return in.string(r.commandId) && in.u8(r.commandType) && in.string(r.requestFingerprint) && readResult(in, r.result) && in.u64(r.appliedIndex); }

bool eligible(const NodeRecord& node)
{ return node.health == NodeHealth::kOnline && !node.draining; }

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
    case ApplyStatus::kCommandIdReuseMismatch: return "COMMAND_ID_REUSE_MISMATCH";
    case ApplyStatus::kUnavailable: return "UNAVAILABLE";
    }
    return "INVALID";
}

const char* toString(ChecksumType type)
{
    switch(type) {
    case ChecksumType::kNone: return "none";
    case ChecksumType::kCrc32c: return "crc32c";
    case ChecksumType::kSha256: return "sha256";
    }
    return "unknown";
}

std::string MetadataStateMachine::chunkKey(const std::string& objectId, uint32_t index)
{ return objectId + "\n" + std::to_string(index); }

std::string MetadataStateMachine::catalogKey(const std::string& ownerId,
                                             const std::string& parentPath,
                                             const std::string& name)
{ return ownerId + "\n" + parentPath + "\n" + name; }

ApplyResult MetadataStateMachine::resultFor(const MetadataCommand& command,
                                            uint64_t index,
                                            uint64_t term,
                                            ApplyStatus status,
                                            std::string message) const
{
    ApplyResult result;
    result.commandId = command.commandId; result.status = status; result.metadataVersion = index;
    result.appliedIndex = index; result.appliedTerm = term; result.placementEpoch = placementEpoch_;
    result.message = std::move(message); return result;
}

ApplyResult MetadataStateMachine::apply(const MetadataCommand& command, uint64_t raftIndex, uint64_t raftTerm)
{
    if(raftIndex == 0 || raftIndex <= lastAppliedIndex_)
        return resultFor(command, lastAppliedIndex_, lastAppliedTerm_, ApplyStatus::kInvalid, "raft index is not monotonic");
    lastAppliedIndex_ = raftIndex;
    lastAppliedTerm_ = raftTerm;
    if(command.commandId.empty()) return resultFor(command, raftIndex, raftTerm, ApplyStatus::kInvalid, "commandId is required");
    const auto fingerprint = commandFingerprint(command);
    if(!fingerprint) return resultFor(command, raftIndex, raftTerm, ApplyStatus::kInvalid, "command cannot be canonically encoded");
    const auto existing = dedup_.find(command.commandId);
    if(existing != dedup_.end()) {
        if(existing->second.commandType != static_cast<uint8_t>(command.type)
           || existing->second.requestFingerprint != *fingerprint) {
            return resultFor(command, raftIndex, raftTerm, ApplyStatus::kCommandIdReuseMismatch,
                             "commandId was already used for a different request");
        }
        return existing->second.result;
    }
    ApplyResult result = applyNew(command, raftIndex, raftTerm);
    dedup_.emplace(command.commandId, DedupEntry{command.commandId, static_cast<uint8_t>(command.type), *fingerprint, result, raftIndex});
    return result;
}

bool MetadataStateMachine::finishLease(LeaseRecord& lease, LeaseState terminalState)
{
    if(lease.state != LeaseState::kActive) return false;
    for(const auto& target : lease.targets) {
        auto node = nodes_.find(target.nodeId);
        if(node == nodes_.end()) continue;
        node->second.reservedBytes = node->second.reservedBytes >= lease.chunkSize
            ? node->second.reservedBytes - lease.chunkSize : 0;
        if(node->second.reservedWrites > 0) --node->second.reservedWrites;
    }
    lease.state = terminalState;
    return true;
}

ApplyResult MetadataStateMachine::applyNew(const MetadataCommand& command, uint64_t index, uint64_t term)
{
    if(command.schemaVersion != kMetadataSchemaVersion)
        return resultFor(command, index, term, ApplyStatus::kInvalid, "unsupported metadata command schema");
    if(command.expectedMetadataVersion != 0 && command.expectedMetadataVersion != index - 1)
        return resultFor(command, index, term, ApplyStatus::kConflict, "metadata version does not match");

    switch(command.type) {
    case MetadataCommandType::kRegisterNode: {
        const auto* p = std::get_if<RegisterNodePayload>(&command.payload);
        if(!p || p->nodeId.empty() || p->bootId.empty() || p->address.empty() || p->dataPort == 0 || p->registeredCapacityBytes == 0)
            return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid register node payload");
        auto it = nodes_.find(p->nodeId);
        if(it == nodes_.end()) {
            NodeRecord node; node.nodeId = p->nodeId; node.bootId = p->bootId; node.nodeEpoch = 1;
            node.address = p->address; node.dataPort = p->dataPort; node.registeredCapacityBytes = p->registeredCapacityBytes;
            node.capabilities = p->capabilities; node.health = NodeHealth::kRecovering; node.placementEpoch = ++placementEpoch_;
            it = nodes_.emplace(node.nodeId, std::move(node)).first;
        } else {
            NodeRecord& node = it->second;
            if(node.bootId != p->bootId) { ++node.nodeEpoch; node.bootId = p->bootId; node.health = NodeHealth::kRecovering; }
            node.address = p->address; node.dataPort = p->dataPort; node.registeredCapacityBytes = p->registeredCapacityBytes;
            node.capabilities = p->capabilities; node.placementEpoch = ++placementEpoch_;
        }
        ApplyResult result = resultFor(command, index, term, ApplyStatus::kOk, "node registered");
        result.nodeEpoch = it->second.nodeEpoch; result.placementEpoch = placementEpoch_; return result;
    }
    case MetadataCommandType::kMarkNodeHealth: {
        const auto* p = std::get_if<MarkNodeHealthPayload>(&command.payload);
        if(!p || p->nodeId.empty()) return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid health payload");
        auto it = nodes_.find(p->nodeId); if(it == nodes_.end()) return resultFor(command, index, term, ApplyStatus::kNotFound, "node not found");
        if(command.nodeEpoch != it->second.nodeEpoch) return resultFor(command, index, term, ApplyStatus::kFenced, "node epoch is stale");
        if(it->second.health != p->health) { it->second.health = p->health; it->second.placementEpoch = ++placementEpoch_; }
        ApplyResult result = resultFor(command, index, term, ApplyStatus::kOk, "node health updated");
        result.nodeEpoch = it->second.nodeEpoch; result.placementEpoch = placementEpoch_; return result;
    }
    case MetadataCommandType::kSetNodeDraining: {
        const auto* p = std::get_if<SetNodeDrainingPayload>(&command.payload);
        if(!p || p->nodeId.empty()) return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid draining payload");
        auto it = nodes_.find(p->nodeId); if(it == nodes_.end()) return resultFor(command, index, term, ApplyStatus::kNotFound, "node not found");
        if(command.nodeEpoch != it->second.nodeEpoch) return resultFor(command, index, term, ApplyStatus::kFenced, "node epoch is stale");
        if(it->second.draining != p->draining) { it->second.draining = p->draining; it->second.placementEpoch = ++placementEpoch_; }
        return resultFor(command, index, term, ApplyStatus::kOk, "node draining state updated");
    }
    case MetadataCommandType::kCreateSession: {
        const auto* p = std::get_if<CreateSessionPayload>(&command.payload);
        if(!p || p->sessionId.empty() || p->objectId.empty() || p->objectVersion == 0 || p->ownerId.empty()
           || p->name.empty() || p->chunkSize == 0 || p->desiredRf == 0)
            return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid create session payload");
        if(sessions_.count(p->sessionId) || objects_.count(p->objectId))
            return resultFor(command, index, term, ApplyStatus::kConflict, "session or object already exists");
        std::set<uint32_t> indexes; std::set<std::string> routes; uint64_t total = 0;
        for(const auto& c : p->chunks) {
            if(c.routeKey.empty() || c.storageIdentity.empty() || c.generation == 0 || c.size == 0
               || c.checksumType == ChecksumType::kNone || c.checksumDigest.empty()
               || !indexes.insert(c.index).second || !routes.insert(c.routeKey).second)
                return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid initial chunk list");
            if(total > UINT64_MAX - c.size) return resultFor(command, index, term, ApplyStatus::kInvalid, "object size overflow");
            total += c.size;
        }
        for(uint32_t i = 0; i < p->chunks.size(); ++i) if(indexes.count(i) == 0)
            return resultFor(command, index, term, ApplyStatus::kInvalid, "chunk indexes must be contiguous");
        if(total != p->fileSize) return resultFor(command, index, term, ApplyStatus::kInvalid, "chunk sizes do not match object size");
        ObjectRecord object; object.objectId = p->objectId; object.objectVersion = p->objectVersion; object.metadataVersion = index;
        object.ownerId = p->ownerId; object.parentPath = p->parentPath; object.name = p->name; object.fileSize = p->fileSize;
        object.chunkSize = p->chunkSize; object.desiredRf = p->desiredRf; object.contentHash = p->contentHash;
        objects_.emplace(object.objectId, object);
        UploadSessionRecord session; session.sessionId = p->sessionId; session.objectId = p->objectId;
        session.objectVersion = p->objectVersion; session.ownerId = p->ownerId; session.fileSize = p->fileSize;
        session.chunkSize = p->chunkSize; session.totalChunks = static_cast<uint32_t>(p->chunks.size()); session.expiresAt = p->expiresAt;
        sessions_.emplace(session.sessionId, session);
        for(const auto& c : p->chunks) {
            ChunkRouteRecord chunk; chunk.routeKey = c.routeKey; chunk.objectId = p->objectId; chunk.objectVersion = p->objectVersion;
            chunk.index = c.index; chunk.storageIdentity = c.storageIdentity; chunk.identityScheme = c.identityScheme;
            chunk.checksumType = c.checksumType; chunk.checksumDigest = c.checksumDigest; chunk.size = c.size;
            chunk.desiredRf = p->desiredRf; chunk.generation = c.generation;
            chunks_.emplace(chunkKey(chunk.objectId, chunk.index), std::move(chunk));
        }
        ApplyResult result = resultFor(command, index, term, ApplyStatus::kOk, "session created");
        result.objectId = p->objectId; result.objectVersion = p->objectVersion; result.sessionId = p->sessionId; return result;
    }
    case MetadataCommandType::kReserveLease: {
        const auto* p = std::get_if<ReserveLeasePayload>(&command.payload);
        if(!p || p->leaseId.empty() || p->requestKey.empty() || p->sessionId.empty() || p->routeKey.empty()
           || p->chunkSize == 0 || p->targets.empty() || command.generation == 0)
            return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid reserve lease payload");
        if(leases_.count(p->leaseId)) return resultFor(command, index, term, ApplyStatus::kConflict, "lease already exists");
        auto session = sessions_.find(p->sessionId); if(session == sessions_.end()) return resultFor(command, index, term, ApplyStatus::kNotFound, "session not found");
        auto chunk = chunks_.find(chunkKey(session->second.objectId, p->chunkIndex));
        if(chunk == chunks_.end()) return resultFor(command, index, term, ApplyStatus::kNotFound, "chunk not found");
        if(chunk->second.state != ChunkState::kAllocated || chunk->second.routeKey != p->routeKey
           || chunk->second.size != p->chunkSize || chunk->second.generation != command.generation)
            return resultFor(command, index, term, ApplyStatus::kFenced, "chunk route or generation is stale");
        if(command.placementEpoch != placementEpoch_) return resultFor(command, index, term, ApplyStatus::kFenced, "placement epoch is stale");
        if(p->targets.size() != chunk->second.desiredRf) return resultFor(command, index, term, ApplyStatus::kConflict, "strict RF target count mismatch");
        for(const auto& [id, active] : leases_) if(active.state == LeaseState::kActive && active.sessionId == p->sessionId && active.chunkIndex == p->chunkIndex)
            return resultFor(command, index, term, ApplyStatus::kConflict, "chunk already has an active lease");
        std::set<std::string> unique;
        for(const auto& target : p->targets) {
            auto node = nodes_.find(target.nodeId);
            if(node == nodes_.end() || !unique.insert(target.nodeId).second || !eligible(node->second)
               || node->second.nodeEpoch != target.nodeEpoch || node->second.placementEpoch > command.placementEpoch
               || node->second.registeredCapacityBytes - std::min(node->second.registeredCapacityBytes, node->second.reservedBytes) < p->chunkSize)
                return resultFor(command, index, term, ApplyStatus::kFenced, "placement target is stale or lacks capacity");
        }
        LeaseRecord lease; lease.leaseId = p->leaseId; lease.requestKey = p->requestKey; lease.sessionId = p->sessionId;
        lease.chunkIndex = p->chunkIndex; lease.routeKey = p->routeKey; lease.chunkSize = p->chunkSize;
        lease.placementEpoch = command.placementEpoch; lease.generation = command.generation; lease.targets = p->targets; lease.expiresAt = p->expiresAt;
        leases_.emplace(lease.leaseId, lease);
        for(const auto& target : p->targets) { auto& node = nodes_.at(target.nodeId); node.reservedBytes += p->chunkSize; ++node.reservedWrites; }
        ApplyResult result = resultFor(command, index, term, ApplyStatus::kOk, "lease reserved");
        result.leaseId = p->leaseId; result.sessionId = p->sessionId; result.objectId = session->second.objectId;
        result.objectVersion = session->second.objectVersion; return result;
    }
    case MetadataCommandType::kReleaseLease: {
        const auto* p = std::get_if<ReleaseLeasePayload>(&command.payload);
        if(!p || p->leaseId.empty()) return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid release payload");
        auto lease = leases_.find(p->leaseId); if(lease == leases_.end()) return resultFor(command, index, term, ApplyStatus::kNotFound, "lease not found");
        if(lease->second.generation != p->expectedGeneration) return resultFor(command, index, term, ApplyStatus::kFenced, "lease generation is stale");
        const bool changed = finishLease(lease->second, LeaseState::kReleased);
        ApplyResult result = resultFor(command, index, term, changed ? ApplyStatus::kOk : ApplyStatus::kAlreadyApplied,
                                       changed ? "lease released" : "lease already terminal");
        result.leaseId = p->leaseId; return result;
    }
    case MetadataCommandType::kExpireLease: {
        const auto* p = std::get_if<ExpireLeasePayload>(&command.payload);
        if(!p || p->leaseId.empty()) return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid expire payload");
        auto lease = leases_.find(p->leaseId); if(lease == leases_.end()) return resultFor(command, index, term, ApplyStatus::kNotFound, "lease not found");
        if(lease->second.generation != p->expectedGeneration || lease->second.expiresAt != p->expectedExpiresAt)
            return resultFor(command, index, term, ApplyStatus::kFenced, "lease expiration precondition is stale");
        if(p->observedAt < p->expectedExpiresAt) return resultFor(command, index, term, ApplyStatus::kConflict, "lease has not reached explicit expiry time");
        const bool changed = finishLease(lease->second, LeaseState::kExpired);
        ApplyResult result = resultFor(command, index, term, changed ? ApplyStatus::kOk : ApplyStatus::kAlreadyApplied,
                                       changed ? "lease expired" : "lease already terminal");
        result.leaseId = p->leaseId; return result;
    }
    case MetadataCommandType::kCommitChunk: {
        const auto* p = std::get_if<CommitChunkPayload>(&command.payload);
        if(!p || p->sessionId.empty() || p->leaseId.empty() || p->routeKey.empty() || p->chunkSize == 0
           || p->checksumType == ChecksumType::kNone || p->checksumDigest.empty())
            return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid commit chunk payload");
        auto session = sessions_.find(p->sessionId); auto lease = leases_.find(p->leaseId);
        if(session == sessions_.end() || lease == leases_.end()) return resultFor(command, index, term, ApplyStatus::kNotFound, "session or lease not found");
        auto chunk = chunks_.find(chunkKey(session->second.objectId, p->chunkIndex));
        if(chunk == chunks_.end()) return resultFor(command, index, term, ApplyStatus::kNotFound, "chunk not found");
        if(chunk->second.state == ChunkState::kCommitted) return resultFor(command, index, term, ApplyStatus::kAlreadyApplied, "chunk already committed");
        if(lease->second.state != LeaseState::kActive || lease->second.sessionId != p->sessionId
           || lease->second.chunkIndex != p->chunkIndex || lease->second.routeKey != p->routeKey
           || lease->second.chunkSize != p->chunkSize || lease->second.generation != command.generation
           || chunk->second.generation != command.generation)
            return resultFor(command, index, term, ApplyStatus::kFenced, "commit does not match active lease generation");
        if(chunk->second.checksumType != p->checksumType || chunk->second.checksumDigest != p->checksumDigest)
            return resultFor(command, index, term, ApplyStatus::kConflict, "checksum does not match route");
        if(p->replicas.size() != chunk->second.desiredRf) return resultFor(command, index, term, ApplyStatus::kConflict, "strict RF has not been reached");
        std::map<std::string, uint64_t> targets; for(const auto& target : lease->second.targets) targets.emplace(target.nodeId, target.nodeEpoch);
        std::set<std::string> seen; std::vector<ReplicaRecord> replicas;
        for(const auto& reported : p->replicas) {
            auto target = targets.find(reported.nodeId); auto node = nodes_.find(reported.nodeId);
            if(target == targets.end() || !seen.insert(reported.nodeId).second || node == nodes_.end()
               || target->second != reported.nodeEpoch || node->second.nodeEpoch != reported.nodeEpoch
               || reported.checksumDigest != p->checksumDigest)
                return resultFor(command, index, term, ApplyStatus::kFenced, "replica report is stale or invalid");
            replicas.push_back({reported.nodeId, reported.nodeEpoch, command.generation,
                                ReplicaState::kHealthy, reported.checksumDigest, reported.verifiedAt});
        }
        chunk->second.replicas = std::move(replicas); chunk->second.state = ChunkState::kCommitted;
        ++session->second.completedChunks; finishLease(lease->second, LeaseState::kCommitted);
        auto object = objects_.find(session->second.objectId); if(object != objects_.end()) object->second.metadataVersion = index;
        ApplyResult result = resultFor(command, index, term, ApplyStatus::kOk, "chunk committed");
        result.objectId = session->second.objectId; result.objectVersion = session->second.objectVersion;
        result.sessionId = p->sessionId; result.leaseId = p->leaseId; return result;
    }
    case MetadataCommandType::kCommitFile: {
        const auto* p = std::get_if<CommitFilePayload>(&command.payload);
        if(!p || p->sessionId.empty() || p->objectId.empty() || p->objectVersion == 0)
            return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid commit file payload");
        auto session = sessions_.find(p->sessionId); auto object = objects_.find(p->objectId);
        if(session == sessions_.end() || object == objects_.end()) return resultFor(command, index, term, ApplyStatus::kNotFound, "session or object not found");
        if(session->second.objectId != p->objectId || session->second.objectVersion != p->objectVersion
           || object->second.objectVersion != p->objectVersion)
            return resultFor(command, index, term, ApplyStatus::kFenced, "object version does not match session");
        if(object->second.state == ObjectState::kCommitted) return resultFor(command, index, term, ApplyStatus::kAlreadyApplied, "object already committed");
        if(session->second.completedChunks != session->second.totalChunks)
            return resultFor(command, index, term, ApplyStatus::kConflict, "not all chunks reached desired RF");
        const std::string pathKey = catalogKey(object->second.ownerId, object->second.parentPath, object->second.name);
        const auto bound = catalog_.find(pathKey);
        if(bound != catalog_.end() && bound->second != p->objectId)
            return resultFor(command, index, term, ApplyStatus::kConflict, "catalog path already bound to another object");
        object->second.state = ObjectState::kCommitted; object->second.metadataVersion = index; object->second.contentHash = p->contentHash;
        catalog_[pathKey] = p->objectId;
        ApplyResult result = resultFor(command, index, term, ApplyStatus::kOk, "file committed");
        result.objectId = p->objectId; result.objectVersion = p->objectVersion; result.sessionId = p->sessionId; return result;
    }
    case MetadataCommandType::kCreateDirectory: {
        const auto* p = std::get_if<CreateDirectoryPayload>(&command.payload);
        if(!p || p->ownerId.empty() || p->path.empty() || p->path.front() != '/')
            return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid directory payload");
        const std::string key = "dir\n" + p->ownerId + "\n" + p->path;
        if(directories_.count(key)) return resultFor(command, index, term, ApplyStatus::kAlreadyApplied, "directory already exists");
        DirectoryRecord directory; directory.ownerId = p->ownerId; directory.path = p->path; directory.createdAt = p->createdAt;
        directories_.emplace(key, std::move(directory));
        return resultFor(command, index, term, ApplyStatus::kOk, "directory created");
    }
    case MetadataCommandType::kDeleteObject: {
        const auto* p = std::get_if<DeleteObjectPayload>(&command.payload);
        if(!p || p->objectId.empty() || p->objectVersion == 0)
            return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid delete object payload");
        auto object = objects_.find(p->objectId);
        if(object == objects_.end()) return resultFor(command, index, term, ApplyStatus::kNotFound, "object not found");
        if(object->second.objectVersion != p->objectVersion)
            return resultFor(command, index, term, ApplyStatus::kFenced, "object version does not match");
        if(object->second.state == ObjectState::kDeleting) {
            ApplyResult result = resultFor(command, index, term, ApplyStatus::kAlreadyApplied, "object deletion already started");
            result.objectId = p->objectId; result.objectVersion = p->objectVersion; return result;
        }
        catalog_.erase(catalogKey(object->second.ownerId, object->second.parentPath, object->second.name));
        object->second.state = ObjectState::kDeleting; object->second.metadataVersion = index;
        for(const auto& [key, chunk] : chunks_) {
            if(chunk.objectId != p->objectId || chunk.objectVersion != p->objectVersion) continue;
            DeleteTaskRecord task; task.objectId = p->objectId; task.objectVersion = p->objectVersion;
            task.chunkIndex = chunk.index; task.storageIdentity = chunk.storageIdentity;
            for(const auto& replica : chunk.replicas) task.pendingReplicas.push_back({replica.nodeId, replica.nodeEpoch});
            if(!task.pendingReplicas.empty()) deleteTasks_[key] = std::move(task);
        }
        ApplyResult result = resultFor(command, index, term, ApplyStatus::kOk, "object deletion started");
        result.objectId = p->objectId; result.objectVersion = p->objectVersion; return result;
    }
    case MetadataCommandType::kDeleteDirectory: {
        const auto* p = std::get_if<DeleteDirectoryPayload>(&command.payload);
        if(!p || p->ownerId.empty() || p->path.empty() || p->path == "/")
            return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid delete directory payload");
        const std::string root = p->path.back() == '/' ? p->path.substr(0, p->path.size() - 1) : p->path;
        const std::string prefix = root + "/";
        bool found = false;
        for(const auto& [key, directory] : directories_) {
            if(directory.ownerId == p->ownerId && (directory.path == root || directory.path.rfind(prefix, 0) == 0)) found = true;
        }
        for(auto& [key, object] : objects_) {
            if(object.ownerId != p->ownerId || (object.parentPath != root && object.parentPath.rfind(prefix, 0) != 0)) continue;
            found = true; object.state = ObjectState::kDeleting; object.metadataVersion = index;
            catalog_.erase(catalogKey(object.ownerId, object.parentPath, object.name));
            for(const auto& [chunkKeyValue, chunk] : chunks_) {
                if(chunk.objectId != object.objectId || chunk.objectVersion != object.objectVersion) continue;
                DeleteTaskRecord task; task.objectId = object.objectId; task.objectVersion = object.objectVersion;
                task.chunkIndex = chunk.index; task.storageIdentity = chunk.storageIdentity;
                for(const auto& replica : chunk.replicas) task.pendingReplicas.push_back({replica.nodeId, replica.nodeEpoch});
                if(!task.pendingReplicas.empty()) deleteTasks_[chunkKeyValue] = std::move(task);
            }
        }
        for(auto it = directories_.begin(); it != directories_.end();) {
            const auto& directory = it->second;
            if(directory.ownerId == p->ownerId && (directory.path == root || directory.path.rfind(prefix, 0) == 0)) it = directories_.erase(it);
            else ++it;
        }
        if(!found) return resultFor(command, index, term, ApplyStatus::kNotFound, "directory not found");
        return resultFor(command, index, term, ApplyStatus::kOk, "directory deletion started");
    }
    case MetadataCommandType::kExpireSession: {
        const auto* p = std::get_if<ExpireSessionPayload>(&command.payload);
        if(!p || p->sessionId.empty()) return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid expire session payload");
        auto session = sessions_.find(p->sessionId);
        if(session == sessions_.end()) return resultFor(command, index, term, ApplyStatus::kNotFound, "session not found");
        if(session->second.expiresAt != p->expectedExpiresAt)
            return resultFor(command, index, term, ApplyStatus::kFenced, "session expiration precondition is stale");
        if(p->observedAt < p->expectedExpiresAt)
            return resultFor(command, index, term, ApplyStatus::kConflict, "session has not reached explicit expiry time");
        if(session->second.expired) return resultFor(command, index, term, ApplyStatus::kAlreadyApplied, "session already expired");
        session->second.expired = true;
        for(auto& [leaseId, lease] : leases_) if(lease.sessionId == p->sessionId && lease.state == LeaseState::kActive) finishLease(lease, LeaseState::kExpired);
        auto object = objects_.find(session->second.objectId);
        if(object != objects_.end() && object->second.state == ObjectState::kUploading) { object->second.state = ObjectState::kFailed; object->second.metadataVersion = index; }
        ApplyResult result = resultFor(command, index, term, ApplyStatus::kOk, "session expired");
        result.sessionId = p->sessionId; result.objectId = session->second.objectId; result.objectVersion = session->second.objectVersion; return result;
    }
    case MetadataCommandType::kAcknowledgeDelete: {
        const auto* p = std::get_if<AcknowledgeDeletePayload>(&command.payload);
        if(!p || p->objectId.empty() || p->objectVersion == 0 || p->nodeId.empty())
            return resultFor(command, index, term, ApplyStatus::kInvalid, "invalid delete acknowledgement payload");
        const std::string key = chunkKey(p->objectId, p->chunkIndex);
        auto task = deleteTasks_.find(key);
        if(task == deleteTasks_.end()) return resultFor(command, index, term, ApplyStatus::kAlreadyApplied, "delete already acknowledged");
        auto replica = std::find_if(task->second.pendingReplicas.begin(), task->second.pendingReplicas.end(),
                                    [&](const LeaseTarget& target) { return target.nodeId == p->nodeId; });
        if(replica == task->second.pendingReplicas.end()) return resultFor(command, index, term, ApplyStatus::kAlreadyApplied, "delete node already acknowledged");
        if(replica->nodeEpoch != p->nodeEpoch) return resultFor(command, index, term, ApplyStatus::kFenced, "delete node epoch is stale");
        task->second.pendingReplicas.erase(replica);
        if(task->second.pendingReplicas.empty()) {
            chunks_.erase(key); deleteTasks_.erase(task);
            bool remaining = false;
            for(const auto& [otherKey, other] : deleteTasks_) if(other.objectId == p->objectId) { remaining = true; break; }
            if(!remaining) {
                objects_.erase(p->objectId);
                for(auto it = sessions_.begin(); it != sessions_.end();) {
                    if(it->second.objectId == p->objectId) it = sessions_.erase(it); else ++it;
                }
            }
        }
        ApplyResult result = resultFor(command, index, term, ApplyStatus::kOk, "delete acknowledged");
        result.objectId = p->objectId; result.objectVersion = p->objectVersion; return result;
    }
    case MetadataCommandType::kReadBarrier:
        return resultFor(command, index, term, ApplyStatus::kOk, "linearizable read barrier applied");
    }
    return resultFor(command, index, term, ApplyStatus::kUnsupported, "unsupported command");
}

std::optional<ObjectRecord> MetadataStateMachine::object(const std::string& id) const
{ auto it = objects_.find(id); return it == objects_.end() ? std::nullopt : std::optional<ObjectRecord>(it->second); }
std::optional<UploadSessionRecord> MetadataStateMachine::session(const std::string& id) const
{ auto it = sessions_.find(id); return it == sessions_.end() ? std::nullopt : std::optional<UploadSessionRecord>(it->second); }
std::vector<UploadSessionRecord> MetadataStateMachine::sessions() const
{
    std::vector<UploadSessionRecord> result; result.reserve(sessions_.size());
    for(const auto& [id, session] : sessions_) { (void)id; result.push_back(session); }
    return result;
}
std::optional<ChunkRouteRecord> MetadataStateMachine::chunk(const std::string& id, uint32_t index) const
{ auto it = chunks_.find(chunkKey(id, index)); return it == chunks_.end() ? std::nullopt : std::optional<ChunkRouteRecord>(it->second); }
std::optional<LeaseRecord> MetadataStateMachine::lease(const std::string& id) const
{ auto it = leases_.find(id); return it == leases_.end() ? std::nullopt : std::optional<LeaseRecord>(it->second); }
std::optional<NodeRecord> MetadataStateMachine::node(const std::string& id) const
{ auto it = nodes_.find(id); return it == nodes_.end() ? std::nullopt : std::optional<NodeRecord>(it->second); }
std::optional<DedupEntry> MetadataStateMachine::dedupEntry(const std::string& id) const
{ auto it = dedup_.find(id); return it == dedup_.end() ? std::nullopt : std::optional<DedupEntry>(it->second); }
std::optional<ApplyResult> MetadataStateMachine::appliedResult(const std::string& id) const
{ auto entry = dedupEntry(id); return entry ? std::optional<ApplyResult>(entry->result) : std::nullopt; }
std::vector<NodeRecord> MetadataStateMachine::nodes() const
{
    std::vector<NodeRecord> result; result.reserve(nodes_.size());
    for(const auto& [id, node] : nodes_) { (void)id; result.push_back(node); }
    return result;
}

std::optional<ReadDescriptor> MetadataStateMachine::readDescriptor(const std::string& id, uint64_t version) const
{
    auto objectIt = objects_.find(id);
    if(objectIt == objects_.end() || objectIt->second.objectVersion != version || objectIt->second.state != ObjectState::kCommitted) return std::nullopt;
    ReadDescriptor descriptor; descriptor.object = objectIt->second;
    for(uint32_t i = 0;; ++i) {
        auto chunkIt = chunks_.find(chunkKey(id, i)); if(chunkIt == chunks_.end()) break;
        if(chunkIt->second.state != ChunkState::kCommitted) return std::nullopt;
        descriptor.chunks.push_back(chunkIt->second);
    }
    return descriptor;
}

std::vector<DirectoryRecord> MetadataStateMachine::directories(const std::string& ownerId,
                                                               const std::string& parentPath) const
{
    std::vector<DirectoryRecord> result;
    const std::string prefix = parentPath == "/" ? "/" : parentPath + "/";
    for(const auto& [key, directory] : directories_) {
        (void)key;
        if(directory.ownerId != ownerId || directory.path == parentPath) continue;
        if(directory.path.rfind(prefix, 0) != 0) continue;
        const std::string suffix = directory.path.substr(prefix.size());
        if(suffix.find('/') == std::string::npos) result.push_back(directory);
    }
    return result;
}

std::vector<ObjectRecord> MetadataStateMachine::objects(const std::string& ownerId,
                                                        const std::string& parentPath) const
{
    std::vector<ObjectRecord> result;
    for(const auto& [key, object] : objects_) {
        (void)key;
        if(object.ownerId == ownerId && object.parentPath == parentPath && object.state != ObjectState::kDeleting)
            result.push_back(object);
    }
    std::sort(result.begin(), result.end(), [](const ObjectRecord& lhs, const ObjectRecord& rhs) {
        return lhs.name < rhs.name;
    });
    return result;
}

std::vector<DeleteTaskRecord> MetadataStateMachine::deleteTasks(const std::string& nodeId) const
{
    std::vector<DeleteTaskRecord> result;
    for(const auto& [key, task] : deleteTasks_) {
        (void)key;
        if(std::any_of(task.pendingReplicas.begin(), task.pendingReplicas.end(),
                       [&](const LeaseTarget& target) { return target.nodeId == nodeId; })) result.push_back(task);
    }
    return result;
}

uint64_t MetadataStateMachine::metadataVersion() const { return lastAppliedIndex_; }
uint64_t MetadataStateMachine::lastAppliedTerm() const { return lastAppliedTerm_; }
uint64_t MetadataStateMachine::placementEpoch() const { return placementEpoch_; }
size_t MetadataStateMachine::dedupEntryCount() const { return dedup_.size(); }

MetadataSnapshot MetadataStateMachine::snapshot() const
{
    Writer out; out.u32(kMetadataSchemaVersion); out.u64(lastAppliedIndex_); out.u64(lastAppliedTerm_); out.u64(placementEpoch_);
    writeMap(out, objects_, writeObject); writeMap(out, sessions_, writeSession); writeMap(out, chunks_, writeChunk);
    writeMap(out, leases_, writeLease); writeMap(out, nodes_, writeNode);
    writeMap(out, catalog_, [](Writer& writer, const std::string& value) { writer.string(value); });
    writeMap(out, directories_, writeDirectory); writeMap(out, deleteTasks_, writeDeleteTask);
    writeMap(out, dedup_, writeDedup);
    MetadataSnapshot snapshot; snapshot.lastAppliedIndex = lastAppliedIndex_; snapshot.lastAppliedTerm = lastAppliedTerm_; snapshot.bytes = out.take(); return snapshot;
}

bool MetadataStateMachine::restore(const MetadataSnapshot& snapshot)
{
    if(snapshot.schemaVersion != kMetadataSchemaVersion) return false;
    Reader in(snapshot.bytes); uint32_t schema = 0; uint64_t index = 0, term = 0, placement = 0;
    std::map<std::string, ObjectRecord> objects; std::map<std::string, UploadSessionRecord> sessions;
    std::map<std::string, ChunkRouteRecord> chunks; std::map<std::string, LeaseRecord> leases;
    std::map<std::string, NodeRecord> nodes; std::map<std::string, std::string> catalog;
    std::map<std::string, DirectoryRecord> directories; std::map<std::string, DeleteTaskRecord> deleteTasks;
    std::map<std::string, DedupEntry> dedup;
    if(!in.u32(schema) || schema != kMetadataSchemaVersion || !in.u64(index) || !in.u64(term) || !in.u64(placement)
       || index != snapshot.lastAppliedIndex || term != snapshot.lastAppliedTerm
       || !readMap(in, objects, readObject) || !readMap(in, sessions, readSession) || !readMap(in, chunks, readChunk)
       || !readMap(in, leases, readLease) || !readMap(in, nodes, readNode)
       || !readMap(in, catalog, [](Reader& reader, std::string& value) { return reader.string(value); })
       || !readMap(in, directories, readDirectory) || !readMap(in, deleteTasks, readDeleteTask)
       || !readMap(in, dedup, readDedup) || !in.done()) return false;
    lastAppliedIndex_ = index; lastAppliedTerm_ = term; placementEpoch_ = placement;
    objects_ = std::move(objects); sessions_ = std::move(sessions); chunks_ = std::move(chunks);
    leases_ = std::move(leases); nodes_ = std::move(nodes); catalog_ = std::move(catalog);
    directories_ = std::move(directories); deleteTasks_ = std::move(deleteTasks); dedup_ = std::move(dedup);
    return true;
}

std::string MetadataStateMachine::stateDigest() const
{
    const std::string bytes = snapshot().bytes; uint64_t hash = 1469598103934665603ULL;
    for(unsigned char byte : bytes) { hash ^= byte; hash *= 1099511628211ULL; }
    std::ostringstream out; out << std::hex << std::setfill('0') << std::setw(16) << hash; return out.str();
}

} // namespace miniKV::metadata
