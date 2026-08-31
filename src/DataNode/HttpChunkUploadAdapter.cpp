#include "DataNode/HttpChunkUploadAdapter.hpp"
#include "DataNode/ChecksumProvider.hpp"

#include "http/HttpRequest.hpp"
#include "utils/Util.hpp"

#include <limits>
#include <stdexcept>

namespace miniKV::datanode {

HttpChunkUploadDecodeResult HttpChunkUploadAdapter::decode(
    const http::HttpRequest& request,
    const std::string& routeIdentity,
    const std::string& localNodeId,
    const std::string& clusterSecret)
{
    HttpChunkUploadDecodeResult result;
    miniKV::util::UploadCapability capability;
    const std::string uploadToken = request.getHeader("X-Upload-Token");
    if(!miniKV::util::verifyUploadCapability(uploadToken, clusterSecret, capability)) {
        result.error = "invalid or expired upload token";
        return result;
    }

    size_t position = 0;
    try {
        const unsigned long long parsed = std::stoull(
            request.getHeader("X-Replica-Position"));
        if(parsed > std::numeric_limits<size_t>::max()) throw std::out_of_range("position");
        position = static_cast<size_t>(parsed);
    } catch(...) {
        result.error = "invalid replica position";
        return result;
    }

    ChunkWriteDescriptor descriptor;
    descriptor.schemaVersion = capability.schemaVersion;
    if(!parseChunkIdentityScheme(capability.identityScheme,
                                 descriptor.identityScheme) ||
       !parseChunkChecksumType(capability.checksumType,
                               descriptor.checksum.type)) {
        result.error = "unsupported chunk identity or checksum type";
        return result;
    }
    descriptor.chunkId = capability.chunkId.empty()
        ? capability.chunkHash : capability.chunkId;
    descriptor.contentHash = capability.chunkHash;
    descriptor.objectId = capability.objectId;
    descriptor.objectVersion = capability.objectVersion == 0
        ? 1 : capability.objectVersion;
    descriptor.generation = capability.generation;
    descriptor.sessionId = capability.sessionId;
    descriptor.chunkIndex = capability.chunkIndex;
    descriptor.contentLength = capability.chunkSize;
    descriptor.checksum.segmentBytes = capability.checksumSegmentBytes;
    descriptor.checksum.wholeDigest = capability.checksumDigest.empty()
        ? capability.chunkHash : capability.checksumDigest;
    descriptor.replicaChain = parseReplicaChain(request.getHeader("X-Replica-Chain"));
    descriptor.replicaPosition = position;
    descriptor.capabilityId = uploadToken;
    descriptor.clientId = request.getHeader("X-Client-Instance-Id");
    if(descriptor.clientId.empty()) descriptor.clientId = "session:" + descriptor.sessionId;
    descriptor.requestId = request.getHeader("X-Request-Id");
    if(descriptor.requestId.empty()) descriptor.requestId = miniKV::util::randomId();

    const std::string expectedRouteIdentity =
        descriptor.identityScheme == ChunkIdentityScheme::kOpaqueChunkId
            ? descriptor.chunkId : descriptor.checksum.wholeDigest;
    if(request.getHeader("X-Session-Id") != descriptor.sessionId ||
       request.getHeader("X-Chunk-Index") != std::to_string(descriptor.chunkIndex) ||
       request.contentLength() != descriptor.contentLength ||
       routeIdentity != expectedRouteIdentity || descriptor.chunkId.empty() ||
       descriptor.contentHash.empty() ||
       !isSupportedChecksumType(descriptor.checksum.type) ||
       !validateChecksumDigest(descriptor.checksum.type,
                               descriptor.checksum.wholeDigest) ||
       descriptor.replicaChain.empty() ||
       !sameReplicaChain(descriptor.replicaChain, capability.chainTargets) ||
       position >= descriptor.replicaChain.size() ||
       descriptor.replicaChain[position].nodeId != localNodeId) {
        result.error = "request does not match upload token";
        return result;
    }

    // A CAS key is only trustworthy when the stored bytes are verified using
    // the same SHA-256 digest. CRC32C is enabled with opaque identities.
    if(descriptor.identityScheme == ChunkIdentityScheme::kCasSha256 &&
       (descriptor.checksum.type != ChunkChecksumType::kSha256 ||
        descriptor.chunkId != descriptor.checksum.wholeDigest ||
        descriptor.contentHash != descriptor.checksum.wholeDigest)) {
        result.error = "cas-sha256 requires matching SHA-256 checksum";
        return result;
    }

    result.ok = true;
    result.descriptor = std::move(descriptor);
    return result;
}

}  // namespace miniKV::datanode
