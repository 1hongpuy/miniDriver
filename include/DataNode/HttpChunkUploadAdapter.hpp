#pragma once

#include "DataNode/ChunkWriteTypes.hpp"

#include <string>

namespace miniKV::http {
class HttpRequest;
}

namespace miniKV::datanode {

struct HttpChunkUploadDecodeResult {
    bool ok = false;
    ChunkWriteDescriptor descriptor;
    std::string error;
};

// HTTP/1.1 is one adapter for the Chunk write contract. This decoder owns all
// URL/header/Capability validation so the coordinator no longer needs to know
// header names or capability wire versions.
class HttpChunkUploadAdapter {
public:
    static HttpChunkUploadDecodeResult decode(
        const http::HttpRequest& request,
        const std::string& routeIdentity,
        const std::string& localNodeId,
        const std::string& clusterSecret);
};

}  // namespace miniKV::datanode
