#include "TestCheck.hpp"
#include "metadata/MetadataStateMachine.hpp"

using namespace miniKV::metadata;

namespace {

MetadataCommand registerNode(const std::string& id, const std::string& nodeId)
{
    MetadataCommand c; c.commandId = id; c.type = MetadataCommandType::kRegisterNode;
    RegisterNodePayload p; p.nodeId = nodeId; p.bootId = "boot"; p.address = "127.0.0.1";
    p.dataPort = 19001; p.registeredCapacityBytes = 1024; c.payload = p; return c;
}

MetadataCommand markOnline(const std::string& id, const std::string& nodeId)
{
    MetadataCommand c; c.commandId = id; c.type = MetadataCommandType::kMarkNodeHealth; c.nodeEpoch = 1;
    MarkNodeHealthPayload p; p.nodeId = nodeId; p.health = NodeHealth::kOnline; p.observedAt = 10; c.payload = p; return c;
}

MetadataCommand createSession()
{
    MetadataCommand c; c.commandId = "create"; c.type = MetadataCommandType::kCreateSession;
    CreateSessionPayload p; p.sessionId = "session"; p.objectId = "object"; p.objectVersion = 1;
    p.ownerId = "admin"; p.name = "snapshot.bin"; p.fileSize = 8; p.chunkSize = 4; p.desiredRf = 2;
    p.chunks = {
        {0, "route-0", "chunk-0", IdentityScheme::kOpaque, ChecksumType::kCrc32c, "crc-0", 4, 1},
        {1, "route-1", "chunk-1", IdentityScheme::kOpaque, ChecksumType::kCrc32c, "crc-1", 4, 1},
    };
    c.payload = p; return c;
}

} // namespace

int main()
{
    MetadataStateMachine original;
    MINIKV_CHECK(original.apply(registerNode("node-1", "dn-1"), 1, 7).status == ApplyStatus::kOk);
    MINIKV_CHECK(original.apply(markOnline("online-1", "dn-1"), 2, 7).status == ApplyStatus::kOk);
    MINIKV_CHECK(original.apply(registerNode("node-2", "dn-2"), 3, 7).status == ApplyStatus::kOk);
    MINIKV_CHECK(original.apply(markOnline("online-2", "dn-2"), 4, 7).status == ApplyStatus::kOk);
    MINIKV_CHECK(original.apply(createSession(), 5, 7).status == ApplyStatus::kOk);

    const auto snapshot = original.snapshot();
    MINIKV_CHECK(snapshot.schemaVersion == 2 && snapshot.lastAppliedIndex == 5 && snapshot.lastAppliedTerm == 7);
    MetadataStateMachine restored;
    MINIKV_CHECK(restored.restore(snapshot));
    MINIKV_CHECK(restored.stateDigest() == original.stateDigest());
    MINIKV_CHECK(restored.chunk("object", 1)->storageIdentity == "chunk-1");
    MINIKV_CHECK(restored.dedupEntry("create")->requestFingerprint == *commandFingerprint(createSession()));

    const auto replay = restored.apply(createSession(), 6, 8);
    MINIKV_CHECK(replay.status == ApplyStatus::kOk && replay.appliedIndex == 5);

    const auto before = restored.stateDigest();
    MetadataSnapshot corrupt = snapshot; corrupt.bytes.pop_back();
    MINIKV_CHECK(!restored.restore(corrupt));
    MINIKV_CHECK(restored.stateDigest() == before);
    return 0;
}
