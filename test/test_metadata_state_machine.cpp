#include "TestCheck.hpp"
#include "metadata/MetadataStateMachine.hpp"

using namespace miniKV::metadata;

namespace {

MetadataCommand registerNode(const std::string& id, const std::string& nodeId, const std::string& boot = "boot-1")
{
    MetadataCommand command; command.commandId = id; command.type = MetadataCommandType::kRegisterNode;
    command.actorType = "datanode"; command.actorId = nodeId;
    RegisterNodePayload p; p.nodeId = nodeId; p.bootId = boot; p.address = "127.0.0.1";
    p.dataPort = 19000; p.registeredCapacityBytes = 1024 * 1024; p.capabilities = {"crc32c", "sendfile"};
    command.payload = p; return command;
}

MetadataCommand markOnline(const std::string& id, const std::string& nodeId, uint64_t epoch)
{
    MetadataCommand command; command.commandId = id; command.type = MetadataCommandType::kMarkNodeHealth; command.nodeEpoch = epoch;
    MarkNodeHealthPayload p; p.nodeId = nodeId; p.health = NodeHealth::kOnline; p.observedAt = 100; command.payload = p; return command;
}

MetadataCommand createSession(const std::string& id = "create")
{
    MetadataCommand command; command.commandId = id; command.type = MetadataCommandType::kCreateSession;
    command.actorType = "gateway"; command.actorId = "gw-1";
    CreateSessionPayload p; p.sessionId = "session-1"; p.objectId = "object-1"; p.objectVersion = 3;
    p.ownerId = "admin"; p.parentPath = "/photos"; p.name = "cat.jpg"; p.fileSize = 4; p.chunkSize = 4;
    p.desiredRf = 2; p.expiresAt = 1000;
    p.chunks.push_back({0, "route-0", "opaque-chunk-0", IdentityScheme::kOpaque,
                        ChecksumType::kCrc32c, "a1b2c3d4", 4, 7});
    command.payload = p; return command;
}

MetadataCommand reserveLease(const std::string& id, uint64_t placementEpoch)
{
    MetadataCommand command; command.commandId = id; command.type = MetadataCommandType::kReserveLease;
    command.placementEpoch = placementEpoch; command.generation = 7;
    ReserveLeasePayload p; p.leaseId = "lease-1"; p.requestKey = "request-1"; p.sessionId = "session-1";
    p.chunkIndex = 0; p.routeKey = "route-0"; p.chunkSize = 4; p.targets = {{"dn-1", 1}, {"dn-2", 1}};
    p.expiresAt = 1100; command.payload = p; return command;
}

MetadataCommand commitChunk(const std::string& id, bool fullRf = true)
{
    MetadataCommand command; command.commandId = id; command.type = MetadataCommandType::kCommitChunk; command.generation = 7;
    CommitChunkPayload p; p.sessionId = "session-1"; p.leaseId = "lease-1"; p.chunkIndex = 0;
    p.routeKey = "route-0"; p.chunkSize = 4; p.checksumType = ChecksumType::kCrc32c; p.checksumDigest = "a1b2c3d4";
    p.replicas.push_back({"dn-1", 1, "a1b2c3d4", 120});
    if(fullRf) p.replicas.push_back({"dn-2", 1, "a1b2c3d4", 121});
    command.payload = p; return command;
}

MetadataCommand commitFile(const std::string& id = "commit-file")
{
    MetadataCommand command; command.commandId = id; command.type = MetadataCommandType::kCommitFile;
    CommitFilePayload p; p.sessionId = "session-1"; p.objectId = "object-1"; p.objectVersion = 3;
    p.contentHash = "final-sha256"; command.payload = p; return command;
}

void bootstrap(MetadataStateMachine& state)
{
    MINIKV_CHECK(state.apply(registerNode("register-1", "dn-1"), 1, 1).status == ApplyStatus::kOk);
    MINIKV_CHECK(state.apply(markOnline("online-1", "dn-1", 1), 2, 1).status == ApplyStatus::kOk);
    MINIKV_CHECK(state.apply(registerNode("register-2", "dn-2"), 3, 1).status == ApplyStatus::kOk);
    MINIKV_CHECK(state.apply(markOnline("online-2", "dn-2", 1), 4, 1).status == ApplyStatus::kOk);
    MINIKV_CHECK(state.apply(createSession(), 5, 1).status == ApplyStatus::kOk);
}

void runSequence(MetadataStateMachine& state)
{
    bootstrap(state);
    MINIKV_CHECK(state.placementEpoch() == 4);
    const auto lease = reserveLease("reserve", 4);
    const auto encoded = encodeMetadataCommand(lease); MINIKV_CHECK(encoded);
    const auto decoded = decodeMetadataCommand(*encoded); MINIKV_CHECK(decoded);
    MINIKV_CHECK(commandFingerprint(lease) == commandFingerprint(*decoded));
    const auto batchBytes = encodeMetadataCommandBatch({lease, commitChunk("batch-commit", false)});
    MINIKV_CHECK(batchBytes);
    const auto batch = decodeMetadataCommandBatch(*batchBytes);
    MINIKV_CHECK(batch && batch->size() == 2 && (*batch)[0].commandId == "reserve" &&
                 (*batch)[1].commandId == "batch-commit");
    MINIKV_CHECK(state.apply(*decoded, 6, 2).status == ApplyStatus::kOk);
    MINIKV_CHECK(state.node("dn-1")->reservedBytes == 4);

    auto reused = lease; std::get<ReserveLeasePayload>(reused.payload).chunkSize = 3;
    MINIKV_CHECK(state.apply(reused, 7, 2).status == ApplyStatus::kCommandIdReuseMismatch);
    MINIKV_CHECK(state.node("dn-1")->reservedBytes == 4);

    MINIKV_CHECK(state.apply(commitChunk("under-rf", false), 8, 2).status == ApplyStatus::kConflict);
    MINIKV_CHECK(state.apply(commitChunk("commit-chunk"), 9, 2).status == ApplyStatus::kOk);
    MINIKV_CHECK(state.lease("lease-1")->state == LeaseState::kCommitted);
    MINIKV_CHECK(state.node("dn-1")->reservedBytes == 0);
    MINIKV_CHECK(state.apply(commitFile(), 10, 2).status == ApplyStatus::kOk);
    MINIKV_CHECK(state.readDescriptor("object-1", 3)->chunks.size() == 1);

    const auto replay = state.apply(commitFile(), 11, 2);
    MINIKV_CHECK(replay.status == ApplyStatus::kOk);
    MINIKV_CHECK(replay.appliedIndex == 10);
    MINIKV_CHECK(state.metadataVersion() == 11);

    const auto restarted = state.apply(registerNode("restart", "dn-1", "boot-2"), 12, 3);
    MINIKV_CHECK(restarted.status == ApplyStatus::kOk && restarted.nodeEpoch == 2);
    MINIKV_CHECK(state.apply(markOnline("stale-health", "dn-1", 1), 13, 3).status == ApplyStatus::kFenced);

    MetadataCommand directory; directory.commandId = "mkdir-photos"; directory.type = MetadataCommandType::kCreateDirectory;
    directory.payload = CreateDirectoryPayload{"admin", "/photos", 200};
    MINIKV_CHECK(state.apply(directory, 14, 3).status == ApplyStatus::kOk);
    MINIKV_CHECK(state.directories("admin", "/").size() == 1);

    MetadataCommand remove; remove.commandId = "delete-object-1"; remove.type = MetadataCommandType::kDeleteObject;
    remove.payload = DeleteObjectPayload{"object-1", 3};
    MINIKV_CHECK(state.apply(remove, 15, 3).status == ApplyStatus::kOk);
    MINIKV_CHECK(state.deleteTasks("dn-1").size() == 1);
    MetadataCommand ack1; ack1.commandId = "ack-delete-1"; ack1.type = MetadataCommandType::kAcknowledgeDelete;
    ack1.payload = AcknowledgeDeletePayload{"object-1", 3, 0, "dn-1", 1};
    MINIKV_CHECK(state.apply(ack1, 16, 3).status == ApplyStatus::kOk);
    MetadataCommand ack2 = ack1; ack2.commandId = "ack-delete-2";
    std::get<AcknowledgeDeletePayload>(ack2.payload).nodeId = "dn-2";
    MINIKV_CHECK(state.apply(ack2, 17, 3).status == ApplyStatus::kOk);
    MINIKV_CHECK(!state.object("object-1"));

    auto expiring = createSession("create-expiring");
    auto& expiringPayload = std::get<CreateSessionPayload>(expiring.payload);
    expiringPayload.sessionId = "session-expiring";
    expiringPayload.objectId = "object-expiring";
    expiringPayload.expiresAt = 50;
    MINIKV_CHECK(state.apply(expiring, 18, 3).status == ApplyStatus::kOk);
    MetadataCommand expire; expire.commandId = "expire-session-expiring";
    expire.type = MetadataCommandType::kExpireSession;
    expire.payload = ExpireSessionPayload{"session-expiring", 50, 50};
    MINIKV_CHECK(state.apply(expire, 19, 3).status == ApplyStatus::kOk);
    MINIKV_CHECK(state.session("session-expiring")->expired);
    MINIKV_CHECK(state.apply(expire, 20, 3).status == ApplyStatus::kOk);
}

} // namespace

int main()
{
    MetadataStateMachine first; runSequence(first);
    MetadataStateMachine second; runSequence(second);
    MINIKV_CHECK(first.stateDigest() == second.stateDigest());
    MINIKV_CHECK(first.dedupEntryCount() == 17);

    auto badCodec = createSession("codec");
    const auto bytes = encodeMetadataCommand(badCodec); MINIKV_CHECK(bytes);
    std::string truncated = *bytes; truncated.pop_back();
    MINIKV_CHECK(!decodeMetadataCommand(truncated));
    return 0;
}
