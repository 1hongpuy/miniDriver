#pragma once

#include "DataNode/GatewayControlClient.hpp"

namespace miniKV::datanode {

// The existing control client already exposes protocol-neutral request/result
// objects.  This role name makes the ChunkWriteCoordinator dependency explicit;
// HttpGatewayControlClient remains only one transport implementation.
using ChunkCommitReporter = GatewayControlClient;

}  // namespace miniKV::datanode
