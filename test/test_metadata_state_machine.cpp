#include "TestCheck.hpp"
#include "metadata/MetadataStateMachine.hpp"

#include <vector>

using namespace miniKV::metadata;

namespace {

MetadataCommand registerNode(const std::string& commandId,
                             const std::string& nodeId,
                             const std::string& bootId)
{
    MetadataCommand command;
    command.commandId = commandId;
    command.type = MetadataCommandType::kRegisterNode;
    command.actorType = "gateway";
    command.actorId = "gateway-1";
    RegisterNodePayload payload;
    payload.nodeId = nodeId;
    payload.bootId = bootId;
    payload.address = "127.0.0.1";
    payload.dataPort = 19000;
    payload.observedAt = 100;
    command.payload = payload;
    return command;
}

MetadataCommand markOnline(const std::string& commandId, const std::string& nodeId, uint64_t nodeEpoch)
{
    MetadataCommand command;
    command.commandId = commandId;
    command.type = MetadataCommandType::kMarkNodeHealth;
    command.nodeEpoch = nodeEpoch;
    MarkNodeHealthPayload payload;
    payload.nodeId = nodeId;
    payload.health = NodeHealth::kOnline;
    payload.observedAt = 101;
    command.payload = payload;
    return command;
}

MetadataCommand createSession(const std::string& commandId)
{
    MetadataCommand command;
    command.commandId = commandId;
    command.type = MetadataCommandType::kCreateSession;
    command.actorType = "gateway";
    command.actorId = "gateway-1";
    CreateSessionPayload payload;
    payload.sessionId = "session-1";
    payload.objectId = "object-1";
    payload.ownerId = "admin";
    payload.parentPath = "/photos";
    payload.name = "sunset.jpg";
    payload.fileHash = "file-hash";
    payload.manifestHash = "manifest-hash";
    payload.fileSize = 4;
    payload.chunkSize = 4;
    payload.desiredRf = 2;
    payload.expiresAt = 1000;
    payload.chunks.push_back({0, "chunk-hash", 4});
    command.payload = payload;
    return command;
}

MetadataCommand reserveLease(const std::string& commandId, uint64_t placementEpoch)
{
    MetadataCommand command;
    command.commandId = commandId;
    command.type = MetadataCommandType::kReserveLease;
    command.placementEpoch = placementEpoch;
    ReserveLeasePayload payload;
    payload.leaseId = "lease-1";
    payload.requestKey = "request-1";
    payload.sessionId = "session-1";
    payload.chunkIndex = 0;
    payload.chunkHash = "chunk-hash";
    payload.chunkSize = 4;
    payload.targetNodeIds = {"dn-1", "dn-2"};
    payload.expiresAt = 1100;
    command.payload = payload;
    return command;
}

MetadataCommand commitChunk(const std::string& commandId, uint64_t placementEpoch, bool fullRf)
{
    MetadataCommand command;
    command.commandId = commandId;
    command.type = MetadataCommandType::kCommitChunk;
    command.placementEpoch = placementEpoch;
    command.generation = 0;
    CommitChunkPayload payload;
    payload.sessionId = "session-1";
    payload.leaseId = "lease-1";
    payload.chunkIndex = 0;
    payload.chunkHash = "chunk-hash";
    payload.chunkSize = 4;
    payload.replicas.push_back({"dn-1", 1, "chunk-hash", 120});
    if(fullRf) payload.replicas.push_back({"dn-2", 1, "chunk-hash", 121});
    command.payload = payload;
    return command;
}

MetadataCommand commitFile(const std::string& commandId)
{
    MetadataCommand command;
    command.commandId = commandId;
    command.type = MetadataCommandType::kCommitFile;
    CommitFilePayload payload;
    payload.sessionId = "session-1";
    payload.objectId = "object-1";
    payload.fileHash = "file-hash";
    command.payload = payload;
    return command;
}

void applyBootstrap(MetadataStateMachine& stateMachine)
{
    const auto dn1 = stateMachine.apply(registerNode("register-dn-1", "dn-1", "boot-1"), 1);
    MINIKV_CHECK(dn1.status == ApplyStatus::kOk);
    MINIKV_CHECK(dn1.nodeEpoch == 1);
    MINIKV_CHECK(stateMachine.apply(markOnline("online-dn-1", "dn-1", 1), 2).status == ApplyStatus::kOk);
    const auto dn2 = stateMachine.apply(registerNode("register-dn-2", "dn-2", "boot-1"), 3);
    MINIKV_CHECK(dn2.status == ApplyStatus::kOk);
    MINIKV_CHECK(stateMachine.apply(markOnline("online-dn-2", "dn-2", 1), 4).status == ApplyStatus::kOk);
    MINIKV_CHECK(stateMachine.apply(createSession("create-session"), 5).status == ApplyStatus::kOk);
}

} // namespace

int main()
{
    MetadataStateMachine stateMachine;
    applyBootstrap(stateMachine);
    MINIKV_CHECK(stateMachine.placementEpoch() == 4);

    const auto encoded = encodeMetadataCommand(reserveLease("reserve-lease", stateMachine.placementEpoch()));
    MINIKV_CHECK(encoded.has_value());
    const auto decoded = decodeMetadataCommand(*encoded);
    MINIKV_CHECK(decoded.has_value());
    MINIKV_CHECK(decoded->commandId == "reserve-lease");
    MINIKV_CHECK(decoded->type == MetadataCommandType::kReserveLease);
    MINIKV_CHECK(stateMachine.apply(reserveLease("stale-placement", 3), 6).status == ApplyStatus::kFenced);
    MINIKV_CHECK(stateMachine.apply(*decoded, 7).status == ApplyStatus::kOk);

    auto staleGeneration = commitChunk("stale-generation", 4, true);
    staleGeneration.generation = 1;
    MINIKV_CHECK(stateMachine.apply(staleGeneration, 8).status == ApplyStatus::kFenced);

    const auto underReplicated = stateMachine.apply(commitChunk("commit-under-rf", 4, false), 9);
    MINIKV_CHECK(underReplicated.status == ApplyStatus::kConflict);
    MINIKV_CHECK(stateMachine.chunk("object-1", 0)->state == ChunkState::kAllocated);

    const auto committedChunk = stateMachine.apply(commitChunk("commit-chunk", 4, true), 10);
    MINIKV_CHECK(committedChunk.status == ApplyStatus::kOk);
    MINIKV_CHECK(stateMachine.chunk("object-1", 0)->state == ChunkState::kCommitted);
    MINIKV_CHECK(stateMachine.apply(commitFile("commit-file"), 11).status == ApplyStatus::kOk);
    MINIKV_CHECK(stateMachine.object("object-1")->state == ObjectState::kCommitted);

    const auto repeatedCommit = stateMachine.apply(commitFile("commit-file"), 12);
    MINIKV_CHECK(repeatedCommit.status == ApplyStatus::kOk);
    MINIKV_CHECK(repeatedCommit.appliedIndex == 11);
    MINIKV_CHECK(stateMachine.metadataVersion() == 12);

    const auto restarted = stateMachine.apply(registerNode("restart-dn-1", "dn-1", "boot-2"), 13);
    MINIKV_CHECK(restarted.status == ApplyStatus::kOk);
    MINIKV_CHECK(restarted.nodeEpoch == 2);
    MetadataCommand staleHeartbeat;
    staleHeartbeat.commandId = "stale-heartbeat";
    staleHeartbeat.type = MetadataCommandType::kHeartbeatNode;
    staleHeartbeat.nodeEpoch = 1;
    HeartbeatNodePayload heartbeat;
    heartbeat.nodeId = "dn-1";
    heartbeat.observedAt = 200;
    staleHeartbeat.payload = heartbeat;
    MINIKV_CHECK(stateMachine.apply(staleHeartbeat, 14).status == ApplyStatus::kFenced);

    auto staleVersion = createSession("bad-version");
    staleVersion.expectedMetadataVersion = 1;
    MINIKV_CHECK(stateMachine.apply(staleVersion, 15).status == ApplyStatus::kConflict);

    MetadataStateMachine sameSequence;
    applyBootstrap(sameSequence);
    MINIKV_CHECK(sameSequence.apply(reserveLease("stale-placement", 3), 6).status == ApplyStatus::kFenced);
    MINIKV_CHECK(sameSequence.apply(reserveLease("reserve-lease", 4), 7).status == ApplyStatus::kOk);
    MINIKV_CHECK(sameSequence.apply(staleGeneration, 8).status == ApplyStatus::kFenced);
    MINIKV_CHECK(sameSequence.apply(commitChunk("commit-under-rf", 4, false), 9).status == ApplyStatus::kConflict);
    MINIKV_CHECK(sameSequence.apply(commitChunk("commit-chunk", 4, true), 10).status == ApplyStatus::kOk);
    MINIKV_CHECK(sameSequence.apply(commitFile("commit-file"), 11).status == ApplyStatus::kOk);
    MINIKV_CHECK(sameSequence.apply(commitFile("commit-file"), 12).status == ApplyStatus::kOk);
    MINIKV_CHECK(sameSequence.apply(registerNode("restart-dn-1", "dn-1", "boot-2"), 13).status == ApplyStatus::kOk);
    MINIKV_CHECK(sameSequence.apply(staleHeartbeat, 14).status == ApplyStatus::kFenced);
    MINIKV_CHECK(sameSequence.apply(staleVersion, 15).status == ApplyStatus::kConflict);
    MINIKV_CHECK(stateMachine.stateDigest() == sameSequence.stateDigest());
    return 0;
}
