#include "DataNode/ChunkWriteTypes.hpp"

#include "utils/Util.hpp"

#include <limits>
#include <utility>

namespace miniKV::datanode {

const char* chunkIdentitySchemeName(ChunkIdentityScheme scheme)
{
    switch(scheme) {
    case ChunkIdentityScheme::kCasSha256: return "cas-sha256";
    case ChunkIdentityScheme::kOpaqueChunkId: return "opaque-chunk-id";
    }
    return "unknown";
}

bool parseChunkIdentityScheme(const std::string& value, ChunkIdentityScheme& out)
{
    if(value.empty() || value == "cas-sha256") {
        out = ChunkIdentityScheme::kCasSha256;
        return true;
    }
    if(value == "opaque-chunk-id") {
        out = ChunkIdentityScheme::kOpaqueChunkId;
        return true;
    }
    return false;
}

const char* chunkChecksumTypeName(ChunkChecksumType type)
{
    switch(type) {
    case ChunkChecksumType::kSha256: return "sha256";
    case ChunkChecksumType::kCrc32c: return "crc32c";
    case ChunkChecksumType::kBlake3: return "blake3";
    }
    return "unknown";
}

bool parseChunkChecksumType(const std::string& value, ChunkChecksumType& out)
{
    if(value.empty() || value == "sha256") {
        out = ChunkChecksumType::kSha256;
        return true;
    }
    if(value == "crc32c") {
        out = ChunkChecksumType::kCrc32c;
        return true;
    }
    if(value == "blake3") {
        out = ChunkChecksumType::kBlake3;
        return true;
    }
    return false;
}

bool parseReplicaTarget(const std::string& value, ReplicaTarget& out)
{
    const size_t at = value.find('@');
    const size_t colon = value.rfind(':');
    if(at == std::string::npos || colon == std::string::npos || colon <= at + 1) return false;
    try {
        out.nodeId = value.substr(0, at);
        out.address = value.substr(at + 1, colon - at - 1);
        const unsigned long parsedPort = std::stoul(value.substr(colon + 1));
        if(out.nodeId.empty() || out.address.empty() || parsedPort == 0 ||
           parsedPort > std::numeric_limits<uint16_t>::max()) return false;
        out.port = static_cast<uint16_t>(parsedPort);
        return true;
    } catch(...) {
        return false;
    }
}

std::vector<ReplicaTarget> parseReplicaChain(const std::string& value)
{
    std::vector<ReplicaTarget> out;
    for(const std::string& item : miniKV::util::split(value, ';')) {
        ReplicaTarget target;
        if(!parseReplicaTarget(item, target)) return {};
        out.push_back(std::move(target));
    }
    return out;
}

std::string joinReplicaChain(const std::vector<ReplicaTarget>& chain)
{
    std::vector<std::string> values;
    values.reserve(chain.size());
    for(const ReplicaTarget& node : chain) {
        values.push_back(node.nodeId + "@" + node.address + ":" + std::to_string(node.port));
    }
    return miniKV::util::join(values, ';');
}

bool sameReplicaChain(const std::vector<ReplicaTarget>& chain,
                      const std::vector<std::string>& expected)
{
    return joinReplicaChain(chain) == miniKV::util::join(expected, ';');
}

}  // namespace miniKV::datanode
