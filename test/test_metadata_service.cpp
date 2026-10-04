#include "TestCheck.hpp"
#include "metadata/MetadataService.hpp"

#include <filesystem>
#include <unistd.h>

using namespace miniKV::metadata;

namespace {

MetadataCommand nodeCommand(const std::string& commandId, const std::string& nodeId)
{
    MetadataCommand command; command.commandId = commandId; command.type = MetadataCommandType::kRegisterNode;
    RegisterNodePayload p; p.nodeId = nodeId; p.bootId = "boot"; p.address = "127.0.0.1";
    p.dataPort = 19000; p.registeredCapacityBytes = 1024; command.payload = p; return command;
}

MetadataCommand onlineCommand(const std::string& commandId, const std::string& nodeId)
{
    MetadataCommand command; command.commandId = commandId; command.type = MetadataCommandType::kMarkNodeHealth; command.nodeEpoch = 1;
    MarkNodeHealthPayload p; p.nodeId = nodeId; p.health = NodeHealth::kOnline; p.observedAt = 1000; command.payload = p; return command;
}

MetadataCommand sessionCommand()
{
    MetadataCommand command; command.commandId = "session"; command.type = MetadataCommandType::kCreateSession;
    CreateSessionPayload p; p.sessionId = "s1"; p.objectId = "o1"; p.name = "a.jpg"; p.fileSize = 4; p.chunkSize = 4;
    p.chunks = {{0, "r1", "c1", IdentityScheme::kOpaque, ChecksumType::kCrc32c, "crc", 4, 1}};
    command.payload = p; return command;
}

} // namespace

int main()
{
    const std::string directory = "/tmp/minikv-metadata-service-" + std::to_string(::getpid());
    std::filesystem::remove_all(directory);
    {
        MetadataService service(directory);
        MINIKV_CHECK(service.propose(nodeCommand("n1", "dn-1")).status == ApplyStatus::kOk);
        MINIKV_CHECK(service.propose(onlineCommand("on1", "dn-1")).status == ApplyStatus::kOk);
        MINIKV_CHECK(service.propose(nodeCommand("n2", "dn-2")).status == ApplyStatus::kOk);
        MINIKV_CHECK(service.propose(onlineCommand("on2", "dn-2")).status == ApplyStatus::kOk);
        MINIKV_CHECK(service.propose(sessionCommand()).status == ApplyStatus::kOk);
        const auto beforeHeartbeat = service.status();
        MINIKV_CHECK(service.heartbeat({"dn-1", 1, 900, 2, 0, 1, 0, 0, 2000}));
        MINIKV_CHECK(service.heartbeat({"dn-2", 1, 900, 1, 0, 1, 0, 0, 2000}));
        MINIKV_CHECK(service.status().term == beforeHeartbeat.term);

        ReserveLeaseRequest request; request.commandId = "lease"; request.leaseId = "l1";
        request.requestKey = "req"; request.sessionId = "s1"; request.routeKey = "r1";
        request.chunkSize = 4; request.generation = 1; request.desiredRf = 2;
        request.expiresAt = 10000; request.nowMs = 2001;
        MINIKV_CHECK(service.reserveLease(request).status == ApplyStatus::kOk);
        const auto committed = service.commitChunk("s1", 0, 4, {"dn-1", "dn-2"}, "l1");
        MINIKV_CHECK(committed.status == ApplyStatus::kOk || committed.status == ApplyStatus::kAlreadyApplied);
        MINIKV_CHECK(service.object("o1") && service.object("o1")->state == ObjectState::kCommitted);
        MINIKV_CHECK(service.linearizableReadBarrier("read-1"));
    }
    {
        MetadataService recovered(directory);
        MINIKV_CHECK(recovered.session("s1"));
        MINIKV_CHECK(recovered.propose(sessionCommand()).status == ApplyStatus::kOk);
        ConsensusStatus follower; follower.role = "follower"; follower.term = 2;
        follower.memberId = "meta-2"; follower.leaderId = "meta-1";
        recovered.setConsensusStatus(follower, 3000);
        MetadataCommand barrier; barrier.commandId = "blocked"; barrier.type = MetadataCommandType::kReadBarrier; barrier.payload = ReadBarrierPayload{};
        MINIKV_CHECK(recovered.propose(barrier).status == ApplyStatus::kUnavailable);
    }
    std::filesystem::remove_all(directory);
    return 0;
}
