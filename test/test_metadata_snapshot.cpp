#include "TestCheck.hpp"
#include "metadata/MetadataStateMachine.hpp"

using namespace miniKV::metadata;

namespace {

MetadataCommand registerNode(const std::string& id, const std::string& nodeId)
{
    MetadataCommand command;
    command.commandId = id;
    command.type = MetadataCommandType::kRegisterNode;
    RegisterNodePayload payload;
    payload.nodeId = nodeId;
    payload.bootId = "boot-1";
    payload.address = "127.0.0.1";
    payload.dataPort = 19001;
    payload.observedAt = 10;
    command.payload = payload;
    return command;
}

MetadataCommand markOnline(const std::string& id, const std::string& nodeId)
{
    MetadataCommand command;
    command.commandId = id;
    command.type = MetadataCommandType::kMarkNodeHealth;
    command.nodeEpoch = 1;
    MarkNodeHealthPayload payload;
    payload.nodeId = nodeId;
    payload.health = NodeHealth::kOnline;
    payload.observedAt = 11;
    command.payload = payload;
    return command;
}

MetadataCommand createSession()
{
    MetadataCommand command;
    command.commandId = "create";
    command.type = MetadataCommandType::kCreateSession;
    CreateSessionPayload payload;
    payload.sessionId = "session-snapshot";
    payload.objectId = "object-snapshot";
    payload.ownerId = "admin";
    payload.fileHash = "file-hash";
    payload.manifestHash = "manifest-hash";
    payload.fileSize = 8;
    payload.chunkSize = 4;
    payload.desiredRf = 2;
    payload.chunks = {{0, "chunk-0", 4}, {1, "chunk-1", 4}};
    command.payload = payload;
    return command;
}

} // namespace

int main()
{
    MetadataStateMachine original;
    MINIKV_CHECK(original.apply(registerNode("node-1", "dn-1"), 1).status == ApplyStatus::kOk);
    MINIKV_CHECK(original.apply(markOnline("online-1", "dn-1"), 2).status == ApplyStatus::kOk);
    MINIKV_CHECK(original.apply(registerNode("node-2", "dn-2"), 3).status == ApplyStatus::kOk);
    MINIKV_CHECK(original.apply(markOnline("online-2", "dn-2"), 4).status == ApplyStatus::kOk);
    MINIKV_CHECK(original.apply(createSession(), 5).status == ApplyStatus::kOk);

    const MetadataSnapshot snapshot = original.snapshot();
    MINIKV_CHECK(snapshot.lastAppliedIndex == 5);
    MINIKV_CHECK(!snapshot.bytes.empty());

    MetadataStateMachine restored;
    MINIKV_CHECK(restored.restore(snapshot));
    MINIKV_CHECK(restored.metadataVersion() == original.metadataVersion());
    MINIKV_CHECK(restored.placementEpoch() == original.placementEpoch());
    MINIKV_CHECK(restored.stateDigest() == original.stateDigest());
    MINIKV_CHECK(restored.object("object-snapshot")->fileSize == 8);
    MINIKV_CHECK(restored.chunk("object-snapshot", 1)->chunkHash == "chunk-1");

    const auto repeated = restored.apply(createSession(), 6);
    MINIKV_CHECK(repeated.status == ApplyStatus::kOk);
    MINIKV_CHECK(repeated.appliedIndex == 5);
    MINIKV_CHECK(restored.object("object-snapshot")->state == ObjectState::kUploading);

    const auto digestBeforeBadRestore = restored.stateDigest();
    MetadataSnapshot corrupt = snapshot;
    corrupt.bytes.pop_back();
    MINIKV_CHECK(!restored.restore(corrupt));
    MINIKV_CHECK(restored.stateDigest() == digestBeforeBadRestore);
    return 0;
}
