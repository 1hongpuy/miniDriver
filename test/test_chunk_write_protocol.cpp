#include "DataNode/ChunkWriteTypes.hpp"
#include "DataNode/HttpChunkUploadAdapter.hpp"
#include "http/HttpRequest.hpp"
#include "utils/Util.hpp"
#include "TestCheck.hpp"

#include <iostream>
#include <string>

namespace {

void addHeader(miniKV::http::HttpRequest& request,
               const std::string& name, const std::string& value)
{
    const std::string line = name + ": " + value;
    request.addHeader(line.data(), line.data() + name.size(),
                      line.data() + line.size());
}

miniKV::http::HttpRequest uploadRequest(const std::string& token,
                                        const std::string& chain,
                                        uint64_t size)
{
    miniKV::http::HttpRequest request;
    const std::string method = "PUT";
    MINIKV_CHECK(request.setMethod(method.data(), method.data() + method.size()));
    addHeader(request, "Content-Length", std::to_string(size));
    addHeader(request, "X-Session-Id", "session-1");
    addHeader(request, "X-Chunk-Index", "3");
    addHeader(request, "X-Replica-Chain", chain);
    addHeader(request, "X-Replica-Position", "0");
    addHeader(request, "X-Upload-Token", token);
    addHeader(request, "X-Client-Instance-Id", "client-1");
    addHeader(request, "X-Request-Id", "request-1");
    return request;
}

miniKV::util::UploadCapability capability(uint32_t schemaVersion)
{
    miniKV::util::UploadCapability value;
    value.schemaVersion = schemaVersion;
    value.sessionId = "session-1";
    value.chunkIndex = 3;
    value.chunkHash = std::string(64, 'a');
    value.chunkSize = 4 * 1024 * 1024;
    value.chainTargets = {"node-a@127.0.0.1:19001", "node-b@127.0.0.1:19002"};
    value.leaseId = "lease-1";
    value.expiresAt = miniKV::util::unixSeconds() + 60;
    if(schemaVersion >= 2) {
        value.identityScheme = "cas-sha256";
        value.chunkId = value.chunkHash;
        value.objectId = "object-1";
        value.objectVersion = 7;
        value.generation = 11;
        value.checksumType = "sha256";
        value.checksumDigest = value.chunkHash;
    }
    return value;
}

}  // namespace

int main()
{
    const std::string secret = "p5-protocol-test-secret";

    const auto legacy = capability(1);
    const std::string legacyToken = miniKV::util::issueUploadCapability(legacy, secret);
    MINIKV_CHECK(!legacyToken.empty());
    miniKV::util::UploadCapability decodedLegacy;
    MINIKV_CHECK(miniKV::util::verifyUploadCapability(
        legacyToken, secret, decodedLegacy));
    MINIKV_CHECK(decodedLegacy.schemaVersion == 1);
    MINIKV_CHECK(decodedLegacy.identityScheme == "cas-sha256");
    MINIKV_CHECK(decodedLegacy.chunkId == legacy.chunkHash);
    MINIKV_CHECK(decodedLegacy.checksumDigest == legacy.chunkHash);

    const auto current = capability(2);
    const std::string currentToken = miniKV::util::issueUploadCapability(current, secret);
    MINIKV_CHECK(!currentToken.empty());
    miniKV::util::UploadCapability decodedCurrent;
    MINIKV_CHECK(miniKV::util::verifyUploadCapability(
        currentToken, secret, decodedCurrent));
    MINIKV_CHECK(decodedCurrent.schemaVersion == 2);
    MINIKV_CHECK(decodedCurrent.objectId == "object-1");
    MINIKV_CHECK(decodedCurrent.objectVersion == 7);
    MINIKV_CHECK(decodedCurrent.generation == 11);
    MINIKV_CHECK(decodedCurrent.checksumType == "sha256");

    auto request = uploadRequest(currentToken,
                                 miniKV::util::join(current.chainTargets, ';'),
                                 current.chunkSize);
    auto result = miniKV::datanode::HttpChunkUploadAdapter::decode(
        request, current.chunkHash, "node-a", secret);
    MINIKV_CHECK(result.ok);
    MINIKV_CHECK(result.descriptor.schemaVersion == 2);
    MINIKV_CHECK(result.descriptor.chunkId == current.chunkHash);
    MINIKV_CHECK(result.descriptor.contentHash == current.chunkHash);
    MINIKV_CHECK(result.descriptor.objectVersion == 7);
    MINIKV_CHECK(result.descriptor.generation == 11);
    MINIKV_CHECK(result.descriptor.storageKey() == current.chunkHash);
    MINIKV_CHECK(result.descriptor.replicaChain.size() == 2);
    MINIKV_CHECK(result.descriptor.clientId == "client-1");
    MINIKV_CHECK(result.descriptor.requestId == "request-1");

    MINIKV_CHECK(!miniKV::datanode::HttpChunkUploadAdapter::decode(
        request, std::string(64, 'b'), "node-a", secret).ok);
    MINIKV_CHECK(!miniKV::datanode::HttpChunkUploadAdapter::decode(
        request, current.chunkHash, "node-b", secret).ok);

    auto opaque = capability(2);
    opaque.identityScheme = "opaque-chunk-id";
    opaque.chunkId = "opaque-123";
    const std::string opaqueToken = miniKV::util::issueUploadCapability(opaque, secret);
    auto opaqueRequest = uploadRequest(opaqueToken,
                                       miniKV::util::join(opaque.chainTargets, ';'),
                                       opaque.chunkSize);
    const auto opaqueResult = miniKV::datanode::HttpChunkUploadAdapter::decode(
        opaqueRequest, opaque.chunkId, "node-a", secret);
    MINIKV_CHECK(opaqueResult.ok);
    MINIKV_CHECK(opaqueResult.descriptor.storageKey() == opaque.chunkId);

    auto crc = opaque;
    crc.chunkId = "opaque-crc32c-123";
    crc.checksumType = "crc32c";
    crc.checksumDigest = "e3069283";
    const std::string crcToken = miniKV::util::issueUploadCapability(crc, secret);
    auto crcRequest = uploadRequest(crcToken,
                                    miniKV::util::join(crc.chainTargets, ';'),
                                    crc.chunkSize);
    const auto crcResult = miniKV::datanode::HttpChunkUploadAdapter::decode(
        crcRequest, crc.chunkId, "node-a", secret);
    MINIKV_CHECK(crcResult.ok);
    MINIKV_CHECK(crcResult.descriptor.checksum.type ==
                 miniKV::datanode::ChunkChecksumType::kCrc32c);

    crc.identityScheme = "cas-sha256";
    crc.chunkId = crc.chunkHash;
    const std::string invalidCasToken = miniKV::util::issueUploadCapability(crc, secret);
    auto invalidCasRequest = uploadRequest(invalidCasToken,
        miniKV::util::join(crc.chainTargets, ';'), crc.chunkSize);
    MINIKV_CHECK(!miniKV::datanode::HttpChunkUploadAdapter::decode(
        invalidCasRequest, crc.chunkHash, "node-a", secret).ok);

    std::string tampered = currentToken;
    tampered.back() = tampered.back() == '0' ? '1' : '0';
    miniKV::util::UploadCapability rejected;
    MINIKV_CHECK(!miniKV::util::verifyUploadCapability(tampered, secret, rejected));

    std::cout << "PASS: v1/v2 Capability and HTTP Chunk descriptor adapter\n";
    return 0;
}
