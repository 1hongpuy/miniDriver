#include "DataNode/ControlRequestCodec.hpp"

#include "utils/Util.hpp"

namespace miniKV::datanode {

using namespace util;

EncodedControlRequest makeNodeRegistrationRequest(const NodeRegistration& request)
{
    return {"POST", "/api/v2/nodes/register",
            "{\"nodeId\":\"" + jsonEscape(request.nodeId) +
            "\",\"address\":\"" + jsonEscape(request.address) +
            "\",\"httpPort\":" + std::to_string(request.httpPort) +
            ",\"maxStorageBytes\":" + std::to_string(request.maxStorageBytes) +
            ",\"reservedBytes\":" + std::to_string(request.reservedBytes) +
            ",\"maxConcurrentWrites\":" + std::to_string(request.maxConcurrentWrites) + "}"};
}

EncodedControlRequest makeHeartbeatRequest(const NodeHeartbeat& request)
{
    return {"POST", "/api/v2/nodes/" + request.nodeId + "/heartbeat",
            "{\"usedBytes\":" + std::to_string(request.usedBytes) +
            ",\"freeBytes\":" + std::to_string(request.freeBytes) +
            ",\"cpuPermille\":" + std::to_string(request.cpuPermille) +
            ",\"memoryPermille\":" + std::to_string(request.memoryPermille) +
            ",\"diskIoPermille\":" + std::to_string(request.diskIoPermille) +
            ",\"netOutMbps\":" + std::to_string(request.netOutMbps) +
            ",\"activeUploads\":" + std::to_string(request.activeUploads) + "}"};
}

EncodedControlRequest makeChunkCommitRequest(const ChunkCommit& request)
{
    return {"POST", "/internal/v2/chunk-commits",
            "{\"sessionId\":\"" + jsonEscape(request.sessionId) +
            "\",\"chunkIndex\":" + std::to_string(request.chunkIndex) +
            ",\"chunkHash\":\"" + jsonEscape(request.chunkHash) +
            "\",\"size\":" + std::to_string(request.size) +
            ",\"successfulNodes\":\"" + jsonEscape(join(request.successfulNodes, ',')) +
            "\",\"uploadToken\":\"" + jsonEscape(request.uploadToken) + "\"}"};
}

EncodedControlRequest makeLeaseReleaseRequest(const LeaseRelease& request)
{
    return {"POST", "/internal/v2/lease-releases",
            "{\"uploadToken\":\"" + jsonEscape(request.uploadToken) + "\"}"};
}

}  // namespace miniKV::v2
