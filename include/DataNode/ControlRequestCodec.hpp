#pragma once

#include "DataNode/GatewayControlClient.hpp"

#include <string>

namespace miniKV::datanode {

struct EncodedControlRequest {
    std::string method;
    std::string path;
    std::string body;
};

EncodedControlRequest makeNodeRegistrationRequest(const NodeRegistration& request);
EncodedControlRequest makeHeartbeatRequest(const NodeHeartbeat& request);
EncodedControlRequest makeChunkCommitRequest(const ChunkCommit& request);

}  // namespace miniKV::v2
