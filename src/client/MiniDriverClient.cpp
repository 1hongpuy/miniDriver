#include "client/MiniDriverClient.hpp"

#include "utils/Util.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <fstream>
#include <map>
#include <mutex>
#include <openssl/evp.h>
#include <set>
#include <sstream>
#include <thread>
#include <tuple>
#include <utility>

namespace miniKV::client {
namespace {

uint32_t crc32c(const char* bytes, size_t size) {
    static const auto table = [] {
        std::array<uint32_t, 256> value{};
        for (uint32_t index = 0; index < value.size(); ++index) {
            uint32_t crc = index;
            for (unsigned bit = 0; bit < 8; ++bit) {
                crc = (crc >> 1) ^ ((crc & 1U) ? 0x82f63b78U : 0U);
            }
            value[index] = crc;
        }
        return value;
    }();
    uint32_t crc = 0xffffffffU;
    for (size_t index = 0; index < size; ++index) {
        crc = table[(crc ^ static_cast<uint8_t>(bytes[index])) & 0xffU] ^ (crc >> 8);
    }
    return crc ^ 0xffffffffU;
}

std::string hex32(uint32_t value) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string out(8, '0');
    for (int index = 7; index >= 0; --index) {
        out[static_cast<size_t>(index)] = digits[value & 0xfU];
        value >>= 4;
    }
    return out;
}

uint32_t updateCrc32c(uint32_t crc, const char* bytes, size_t size) {
    static const auto table = [] {
        std::array<uint32_t, 256> value{};
        for (uint32_t index = 0; index < value.size(); ++index) {
            uint32_t entry = index;
            for (unsigned bit = 0; bit < 8; ++bit) {
                entry = (entry >> 1) ^ ((entry & 1U) ? 0x82f63b78U : 0U);
            }
            value[index] = entry;
        }
        return value;
    }();
    for (size_t index = 0; index < size; ++index) {
        crc = table[(crc ^ static_cast<uint8_t>(bytes[index])) & 0xffU] ^ (crc >> 8);
    }
    return crc;
}

bool chunkDigests(const std::filesystem::path& inputPath, uint64_t offset, uint64_t length,
                  bool includeSha256, std::string& sha256, std::string& crc32c,
                  std::string& error) {
    std::ifstream input(inputPath, std::ios::binary);
    if (!input) { error = "cannot open " + inputPath.string(); return false; }
    input.seekg(static_cast<std::streamoff>(offset));
    if (!input) { error = "cannot seek input"; return false; }
    EVP_MD_CTX* context = nullptr;
    if (includeSha256) {
        context = EVP_MD_CTX_new();
        if (context == nullptr || EVP_DigestInit_ex(context, EVP_sha256(), nullptr) != 1) {
            if (context != nullptr) EVP_MD_CTX_free(context);
            error = "cannot initialize SHA-256";
            return false;
        }
    }
    std::array<char, 64 * 1024> buffer{};
    uint64_t remaining = length;
    uint32_t crc = 0xffffffffU;
    while (remaining > 0) {
        const size_t wanted = static_cast<size_t>(std::min<uint64_t>(remaining, buffer.size()));
        input.read(buffer.data(), static_cast<std::streamsize>(wanted));
        if (static_cast<size_t>(input.gcount()) != wanted ||
            (includeSha256 && EVP_DigestUpdate(context, buffer.data(), wanted) != 1)) {
            if (context != nullptr) EVP_MD_CTX_free(context);
            error = "cannot hash upload input";
            return false;
        }
        crc = updateCrc32c(crc, buffer.data(), wanted);
        remaining -= wanted;
    }
    sha256.clear();
    if (includeSha256) {
        unsigned char digest[EVP_MAX_MD_SIZE];
        unsigned int digestSize = 0;
        if (EVP_DigestFinal_ex(context, digest, &digestSize) != 1) {
            EVP_MD_CTX_free(context); error = "cannot finish SHA-256"; return false;
        }
        EVP_MD_CTX_free(context);
        static constexpr char digits[] = "0123456789abcdef";
        sha256.reserve(digestSize * 2);
        for (unsigned int index = 0; index < digestSize; ++index) {
            sha256 += digits[(digest[index] >> 4) & 0xfU];
            sha256 += digits[digest[index] & 0xfU];
        }
    }
    crc32c = hex32(crc ^ 0xffffffffU);
    return true;
}

bool verifyWhole(const ChunkReadPlan& chunk, const std::string& body, std::string& error) {
    if (body.size() != chunk.size) {
        error = "chunk length mismatch";
        return false;
    }
    if (chunk.checksumType == "sha256") {
        if (miniKV::util::sha256Hex(body.data(), body.size()) != chunk.checksumDigest) {
            error = "chunk SHA-256 mismatch";
            return false;
        }
        return true;
    }
    if (chunk.checksumType == "crc32c") {
        if (hex32(crc32c(body.data(), body.size())) != chunk.checksumDigest) {
            error = "chunk CRC32C mismatch";
            return false;
        }
        return true;
    }
    error = "unsupported checksum type " + chunk.checksumType;
    return false;
}

bool parseChunks(const std::string& json, bool capabilityBound, uint64_t fileSize,
                 uint32_t chunkSize, std::vector<ChunkReadPlan>& out, std::string& error) {
    out.clear();
    for (const std::string& item : miniKV::util::jsonObjectArray(json, "chunks")) {
        ChunkReadPlan chunk;
        chunk.index = static_cast<uint32_t>(miniKV::util::jsonUint(item, "index", UINT32_MAX));
        chunk.chunkId = miniKV::util::jsonString(item, "chunkId");
        chunk.storageIdentity = miniKV::util::jsonString(item, "storageIdentity");
        chunk.size = miniKV::util::jsonUint(item, "size");
        chunk.checksumType = miniKV::util::jsonString(item, "checksumType");
        chunk.checksumDigest = miniKV::util::jsonString(item, "checksumDigest");
        chunk.readCapability = miniKV::util::jsonString(item, "readCapability");
        if (chunk.storageIdentity.empty()) chunk.storageIdentity = miniKV::util::jsonString(item, "hash");
        if (chunk.checksumDigest.empty()) chunk.checksumDigest = miniKV::util::jsonString(item, "hash");
        if (chunk.checksumType.empty()) chunk.checksumType = "sha256";
        // V2 manifests preserve chunk order but did not serialize `index`.
        // The compatibility adapter reconstructs it; V3 ReadPlan must remain
        // explicit so a malformed plan cannot silently reorder an object.
        if (chunk.index == UINT32_MAX && !capabilityBound) {
            chunk.index = static_cast<uint32_t>(out.size());
        }
        // Historical V2 manifests omit each Chunk's byte length. Derive it
        // from the immutable file size and ordered storage chunking only in
        // that adapter; a V3 capability-bound plan must carry explicit size.
        if (chunk.size == 0 && !capabilityBound && chunkSize > 0) {
            const uint64_t offset = static_cast<uint64_t>(chunk.index) * chunkSize;
            if (offset < fileSize) chunk.size = std::min<uint64_t>(chunkSize, fileSize - offset);
        }
        if (chunk.index == UINT32_MAX || chunk.storageIdentity.empty() || chunk.size == 0 ||
            chunk.checksumDigest.empty() || (capabilityBound && chunk.readCapability.empty())) {
            error = "invalid chunk read plan";
            return false;
        }
        for (const std::string& replica : miniKV::util::jsonObjectArray(item, "replicas")) {
            ReplicaTarget target;
            target.nodeId = miniKV::util::jsonString(replica, "nodeId");
            target.endpoint.host = miniKV::util::jsonString(replica, "address");
            target.endpoint.port = static_cast<uint16_t>(miniKV::util::jsonUint(
                replica, capabilityBound ? "port" : "httpPort"));
            if (!target.endpoint.host.empty() && target.endpoint.port != 0) chunk.replicas.push_back(std::move(target));
        }
        if (chunk.replicas.empty()) {
            error = "chunk plan has no DataNode candidate";
            return false;
        }
        out.push_back(std::move(chunk));
    }
    if (out.empty()) { error = "read plan has no chunks"; return false; }
    std::sort(out.begin(), out.end(), [](const ChunkReadPlan& left, const ChunkReadPlan& right) {
        return left.index < right.index;
    });
    for (uint32_t index = 0; index < out.size(); ++index) {
        if (out[index].index != index) { error = "read plan chunk ordering is invalid"; return false; }
    }
    return true;
}

bool parseReadHints(const std::string& json, const ObjectRef& requested,
                    ObjectReadHints& out, std::string& error) {
    ObjectReadHints parsed;
    parsed.object.objectId = miniKV::util::jsonString(json, "objectId");
    parsed.object.objectVersion = miniKV::util::jsonUint(json, "objectVersion");
    parsed.size = miniKV::util::jsonUint(json, "fileSize");
    if(parsed.object.objectId != requested.objectId ||
       parsed.object.objectVersion != requested.objectVersion || parsed.size == 0) {
        error = "invalid object read hints response";
        return false;
    }
    for(const std::string& item : miniKV::util::jsonObjectArray(json, "candidates")) {
        NodeReadHint hint;
        hint.nodeId = miniKV::util::jsonString(item, "nodeId");
        hint.localBytes = miniKV::util::jsonUint(item, "localBytes");
        hint.health = miniKV::util::jsonString(item, "health");
        hint.coverageRatio = static_cast<double>(miniKV::util::jsonUint(item, "coveragePermille")) / 1000.0;
        if(hint.nodeId.empty() || hint.localBytes > parsed.size || hint.coverageRatio < 0.0 ||
           hint.coverageRatio > 1.0 || hint.health.empty()) {
            error = "invalid read hint candidate";
            return false;
        }
        parsed.candidates.push_back(std::move(hint));
    }
    if(parsed.candidates.empty()) { error = "read hints have no candidates"; return false; }
    out = std::move(parsed);
    return true;
}

std::string pathEscape(const std::string& value) {
    static constexpr char digits[] = "0123456789ABCDEF";
    std::string escaped;
    for (const unsigned char byte : value) {
        const bool safe = (byte >= 'a' && byte <= 'z') || (byte >= 'A' && byte <= 'Z') ||
            (byte >= '0' && byte <= '9') || byte == '-' || byte == '_' || byte == '.' || byte == '~';
        if (safe) escaped.push_back(static_cast<char>(byte));
        else {
            escaped.push_back('%');
            escaped.push_back(digits[byte >> 4]);
            escaped.push_back(digits[byte & 0xf]);
        }
    }
    return escaped;
}

void addDelta(const StreamingRequest::Stats& before, const StreamingRequest::Stats& after,
              TransferStats& stats) {
    stats.dataConnectionOpens += after.connectionOpens - before.connectionOpens;
    stats.dataRequests += after.requests - before.requests;
    stats.dataConnectionReuses += after.connectionReuses - before.connectionReuses;
}

bool requestSuccess(const Endpoint& endpoint, const std::string& method, const std::string& path,
                    const std::string& body, int timeoutMs, HttpResponse& response,
                    std::string& error) {
    if (!httpRequest(endpoint, method, path, {{"Content-Type", "application/json"}}, body,
                     timeoutMs, response, error)) return false;
    if (response.status < 200 || response.status >= 300) {
        error = method + " " + path + " HTTP " + std::to_string(response.status) + ": " + response.body;
        return false;
    }
    return true;
}

bool replicaChain(const std::string& route, std::string& out, std::string& error) {
    std::vector<std::string> nodes;
    for (const std::string& item : miniKV::util::jsonObjectArray(route, "chain")) {
        const std::string id = miniKV::util::jsonString(item, "nodeId");
        const std::string address = miniKV::util::jsonString(item, "address");
        const uint64_t port = miniKV::util::jsonUint(item, "httpPort");
        if (id.empty() || address.empty() || port == 0 || port > UINT16_MAX) {
            error = "invalid replica chain"; return false;
        }
        nodes.push_back(id + '@' + address + ':' + std::to_string(port));
    }
    if (nodes.empty()) { error = "empty replica chain"; return false; }
    out = miniKV::util::join(nodes, ';');
    return true;
}

bool putChunk(const ClientConfig& config, const std::filesystem::path& inputPath,
              const std::string& sessionId, uint64_t fileSize, uint64_t chunkSize,
              uint32_t chunkIndex, const std::string& checksumType,
              UploadPhaseTimings& timings, std::string& error) {
    const auto chunkStartedAt = std::chrono::steady_clock::now();
    const uint64_t offset = static_cast<uint64_t>(chunkIndex) * chunkSize;
    const uint64_t size = std::min<uint64_t>(chunkSize, fileSize - offset);
    std::string sha256;
    std::string crc32c;
    const bool strongContent = checksumType == "sha256";
    const auto checksumStartedAt = std::chrono::steady_clock::now();
    if (!chunkDigests(inputPath, offset, size, strongContent, sha256, crc32c, error)) return false;
    timings.checksumPreparationMs += std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - checksumStartedAt).count();
    const std::string checksum = checksumType == "sha256" ? sha256 : crc32c;
    // V2 calls this field `hash`, but target opaque-chunk-id objects must not
    // turn it into a synchronous content-addressing key. It is only a stable
    // route/commit key inside this upload session; Gateway assigns chunkId.
    const std::string routeKey = strongContent ? sha256 :
        "upload:" + sessionId + ":" + std::to_string(chunkIndex);
    const std::string routeBody = "{\"chunks\":[{\"index\":" + std::to_string(chunkIndex) +
        ",\"hash\":\"" + routeKey + "\",\"size\":" + std::to_string(size) +
        ",\"checksumType\":\"" + miniKV::util::jsonEscape(checksumType) +
        "\",\"checksumDigest\":\"" + checksum + "\"}]}";
    HttpResponse routes;
    const std::string routePath = "/api/v2/upload/sessions/" + pathEscape(sessionId) + "/routes";
    bool routed = false;
    const auto routeStartedAt = std::chrono::steady_clock::now();
    for (uint32_t attempt = 0; attempt < 6; ++attempt) {
        std::string routeError;
        if (!httpRequest(config.gateway, "POST", routePath, {{"Content-Type", "application/json"}}, routeBody,
                         config.gatewayTimeoutMs, routes, routeError)) {
            error = routeError;
        } else if (routes.status >= 200 && routes.status < 300) {
            routed = true;
            break;
        } else if (routes.status != 503) {
            error = "POST " + routePath + " HTTP " + std::to_string(routes.status) + ": " + routes.body;
            return false;
        } else {
            error = "route admission stayed full";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25U * (attempt + 1U)));
    }
    if (!routed) return false;
    timings.routeCapabilityMs += std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - routeStartedAt).count();
    const std::vector<std::string> values = miniKV::util::jsonObjectArray(routes.body, "routes");
    if (values.size() != 1) { error = "invalid route response"; return false; }
    const std::string& route = values.front();
    Endpoint primary{miniKV::util::jsonString(route, "primaryAddress"),
                     static_cast<uint16_t>(miniKV::util::jsonUint(route, "primaryPort"))};
    const std::string primaryId = miniKV::util::jsonString(route, "primaryNodeId");
    const std::string token = miniKV::util::jsonString(route, "uploadToken");
    std::string identity = miniKV::util::jsonString(route, "storageIdentity");
    if (identity.empty()) identity = routeKey;
    std::string chain;
    if (primary.host.empty() || primary.port == 0 || primaryId.empty() || token.empty() ||
        !replicaChain(route, chain, error)) {
        if (error.empty()) error = "invalid upload route";
        return false;
    }
    std::ifstream input(inputPath, std::ios::binary);
    if (!input) { error = "cannot reopen upload input"; return false; }
    input.seekg(static_cast<std::streamoff>(offset));
    if (!input) { error = "cannot seek upload input"; return false; }
    const std::map<std::string, std::string> headers{
        {"Content-Type", "application/octet-stream"}, {"X-Session-Id", sessionId},
        {"X-Chunk-Index", std::to_string(chunkIndex)}, {"X-Commit-Owner", primaryId},
        {"X-Gateway-Address", config.gateway.host}, {"X-Gateway-Port", std::to_string(config.gateway.port)},
        {"X-Replica-Chain", chain}, {"X-Replica-Position", "0"}, {"X-Upload-Token", token},
    };
    const auto dataNodeStartedAt = std::chrono::steady_clock::now();
    StreamingRequest request;
    if (!request.open(primary, "PUT", "/v2/chunks/" + pathEscape(identity), headers, size,
                      config.dataNodeTimeoutMs, error)) return false;
    std::array<char, 64 * 1024> buffer{};
    uint64_t remaining = size;
    while (remaining > 0) {
        const size_t wanted = static_cast<size_t>(std::min<uint64_t>(remaining, buffer.size()));
        input.read(buffer.data(), static_cast<std::streamsize>(wanted));
        if (static_cast<size_t>(input.gcount()) != wanted || !request.write(buffer.data(), wanted, error)) {
            if (error.empty()) error = "cannot read upload input";
            return false;
        }
        remaining -= wanted;
    }
    HttpResponse response;
    if (!request.finish(response, error)) return false;
    if (response.status != 200) {
        error = "DataNode PUT HTTP " + std::to_string(response.status) + ": " + response.body;
        return false;
    }
    timings.dataNodeUploadMs += std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - dataNodeStartedAt).count();
    timings.chunkUploadTotalMs += std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - chunkStartedAt).count();
    return true;
}

}  // namespace

MiniDriverClient::MiniDriverClient(ClientConfig config) : config_(std::move(config)) {}

bool MiniDriverClient::getReadPlan(const ObjectRef& object, ObjectReadPlan& out, std::string& error) const {
    if (object.objectId.empty() || object.objectVersion == 0 || config_.clusterInternalToken.empty() ||
        config_.servicePrincipal.empty()) {
        error = "object reference, cluster token, and service principal are required";
        return false;
    }
    HttpResponse response;
    const std::map<std::string, std::string> headers{
        {"X-Cluster-Internal-Token", config_.clusterInternalToken},
        {"X-Service-Principal", config_.servicePrincipal},
    };
    if (!httpRequest(config_.gateway, "POST", "/internal/v3/objects/" + pathEscape(object.objectId) +
                     "/versions/" + std::to_string(object.objectVersion) + "/read-plan",
                     headers, "", config_.gatewayTimeoutMs, response, error)) return false;
    if (response.status != 200) {
        error = "read-plan HTTP " + std::to_string(response.status) +
                (response.body.empty() ? std::string{} : ": " + response.body);
        return false;
    }
    ObjectReadPlan parsed;
    parsed.object.objectId = miniKV::util::jsonString(response.body, "objectId");
    parsed.object.objectVersion = miniKV::util::jsonUint(response.body, "objectVersion");
    parsed.fileSize = miniKV::util::jsonUint(response.body, "fileSize");
    parsed.chunkSize = static_cast<uint32_t>(miniKV::util::jsonUint(response.body, "chunkSize"));
    parsed.capabilityBound = true;
    if (parsed.object.objectId != object.objectId || parsed.object.objectVersion != object.objectVersion ||
        parsed.fileSize == 0 || parsed.chunkSize == 0 ||
        !parseChunks(response.body, true, parsed.fileSize, parsed.chunkSize, parsed.chunks, error)) return false;
    out = std::move(parsed);
    return true;
}

bool MiniDriverClient::getLegacyManifest(const std::string& fileHash, ObjectReadPlan& out,
                                         std::string& error) const {
    if (fileHash.empty()) { error = "file hash is required"; return false; }
    HttpResponse response;
    if (!httpRequest(config_.gateway, "GET", "/api/v2/files/" + pathEscape(fileHash) + "/manifest", {}, "",
                     config_.gatewayTimeoutMs, response, error)) return false;
    if (response.status != 200) { error = "manifest HTTP " + std::to_string(response.status); return false; }
    ObjectReadPlan parsed;
    parsed.object.objectId = miniKV::util::jsonString(response.body, "objectId");
    parsed.object.objectVersion = miniKV::util::jsonUint(response.body, "objectVersion", 1);
    parsed.fileSize = miniKV::util::jsonUint(response.body, "fileSize");
    parsed.chunkSize = static_cast<uint32_t>(miniKV::util::jsonUint(response.body, "chunkSize"));
    parsed.capabilityBound = false;
    if (parsed.fileSize == 0 || parsed.chunkSize == 0 ||
        !parseChunks(response.body, false, parsed.fileSize, parsed.chunkSize, parsed.chunks, error)) return false;
    out = std::move(parsed);
    return true;
}

bool MiniDriverClient::uploadFile(const std::filesystem::path& input, const std::string& fileName,
                                  const std::string& dirPath, const UploadOptions& options,
                                  UploadResult& out, std::string& error) const {
    out = {};
    if (fileName.empty() || options.chunkWindow == 0 ||
        (options.checksumType != "crc32c" && options.checksumType != "sha256")) {
        error = "file name, positive chunk window, and crc32c|sha256 checksum are required";
        return false;
    }
    std::error_code fileError;
    const uint64_t fileSize = std::filesystem::file_size(input, fileError);
    if (fileError || fileSize == 0) {
        error = "cannot determine non-empty input file size";
        return false;
    }
    HttpResponse created;
    const bool raftMetadata = [] {
        const char* value = std::getenv("MINIKV_METADATA_MODE");
        return value != nullptr && std::string(value) == "raft";
    }();
    std::string controlCommandId = options.commandId;
    if(controlCommandId.empty()) {
        // Uploads may be started concurrently by several benchmark or
        // application threads.  A steady-clock value alone can collide when
        // two calls enter this function within the same clock tick; that
        // would incorrectly reuse the Raft idempotency key for different
        // manifests.  Keep the clock component for log readability and add a
        // process-wide monotonic sequence for uniqueness.
        static std::atomic<uint64_t> uploadCommandSequence{1};
        controlCommandId = "sdk-upload-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
            std::to_string(uploadCommandSequence.fetch_add(1, std::memory_order_relaxed));
    }
    uint32_t preflightChunkSize = 4U * 1024U * 1024U;
    std::string createBody;
    if(raftMetadata) {
        std::ostringstream body;
        body << "{\"commandId\":\"" << miniKV::util::jsonEscape(controlCommandId)
             << "\",\"fileName\":\"" << miniKV::util::jsonEscape(fileName)
             << "\",\"dirPath\":\"" << miniKV::util::jsonEscape(dirPath)
             << "\",\"fileSize\":" << fileSize << ",\"chunkSize\":" << preflightChunkSize
             << ",\"manifestHash\":\"";
        std::vector<std::tuple<uint32_t, uint64_t, std::string, std::string>> chunks;
        const uint32_t total = static_cast<uint32_t>((fileSize + preflightChunkSize - 1) / preflightChunkSize);
        std::string canonical = "minikv-manifest-v1\n" + std::to_string(fileSize) + "\n" +
                                std::to_string(preflightChunkSize) + "\n";
        for(uint32_t index = 0; index < total; ++index) {
            const uint64_t offset = static_cast<uint64_t>(index) * preflightChunkSize;
            const uint64_t size = std::min<uint64_t>(preflightChunkSize, fileSize - offset);
            std::string sha, crc, digestError;
            if(!chunkDigests(input, offset, size, options.checksumType == "sha256", sha, crc, digestError)) {
                error = digestError; return false;
            }
            const std::string routeKey = "upload:" + controlCommandId + ":" + std::to_string(index);
            const std::string checksum = options.checksumType == "sha256" ? sha : crc;
            canonical += std::to_string(index) + ":" + routeKey + ":" + std::to_string(size) + "\n";
            chunks.emplace_back(index, size, routeKey, checksum);
        }
        const std::string manifestHash = miniKV::util::sha256Hex(canonical.data(), canonical.size());
        body << miniKV::util::jsonEscape(manifestHash) << "\",\"chunks\":[";
        for(size_t i = 0; i < chunks.size(); ++i) {
            if(i) body << ',';
            body << "{\"index\":" << std::get<0>(chunks[i]) << ",\"hash\":\""
                 << miniKV::util::jsonEscape(std::get<2>(chunks[i])) << "\",\"size\":"
                 << std::get<1>(chunks[i]) << ",\"checksumType\":\""
                 << miniKV::util::jsonEscape(options.checksumType) << "\",\"checksumDigest\":\""
                 << std::get<3>(chunks[i]) << "\"}";
        }
        body << "]}";
        createBody = body.str();
    } else {
        const std::string createBodyLegacy = "{\"fileName\":\"" + miniKV::util::jsonEscape(fileName) +
            "\",\"dirPath\":\"" + miniKV::util::jsonEscape(dirPath) + "\",\"fileSize\":" +
            std::to_string(fileSize) + "}";
        createBody = createBodyLegacy;
    }
    const auto createStartedAt = std::chrono::steady_clock::now();
    const std::string createPath = raftMetadata ? "/api/v2/upload/preflight" : "/api/v2/upload/sessions";
    const std::map<std::string, std::string> createHeaders = raftMetadata
        ? std::map<std::string, std::string>{{"Content-Type", "application/json"},
                                             {"X-Minidriver-Command-Id", controlCommandId}}
        : std::map<std::string, std::string>{};
    if (!httpRequest(config_.gateway, "POST", createPath, createHeaders, createBody,
                     config_.gatewayTimeoutMs, created, error) || created.status < 200 || created.status >= 300) {
        if(error.empty()) error = "POST " + createPath + " HTTP " + std::to_string(created.status) + ": " + created.body;
        return false;
    }
    out.timings.createSessionMs = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - createStartedAt).count();
    const std::string sessionId = miniKV::util::jsonString(created.body, "sessionId");
    if (sessionId.empty()) { error = "Gateway returned no sessionId"; return false; }
    HttpResponse session;
    // Raft preflight already returns the committed session contract.  Reading
    // it back immediately would add another ReadIndex/HTTP round trip to
    // every small-object upload.  Legacy session creation keeps the explicit
    // GET because its response does not carry the negotiated checksum and
    // completion state.
    const bool preflightHasSession = raftMetadata &&
        miniKV::util::jsonString(created.body, "status") == "UPLOAD_REQUIRED" &&
        miniKV::util::jsonUint(created.body, "chunkSize") != 0 &&
        miniKV::util::jsonUint(created.body, "totalChunks") != 0;
    if(preflightHasSession) {
        session.status = created.status;
        session.body = created.body;
    } else {
        const auto getSessionStartedAt = std::chrono::steady_clock::now();
        if (!requestSuccess(config_.gateway, "GET", "/api/v2/upload/sessions/" + pathEscape(sessionId), "",
                            config_.gatewayTimeoutMs, session, error)) return false;
        out.timings.getSessionMs = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - getSessionStartedAt).count();
    }
    const uint64_t chunkSize = miniKV::util::jsonUint(session.body, "chunkSize");
    const uint32_t totalChunks = static_cast<uint32_t>(miniKV::util::jsonUint(session.body, "totalChunks"));
    const std::string gatewayChecksum = miniKV::util::jsonString(session.body, "checksumType");
    if (chunkSize == 0 || totalChunks == 0 ||
        (gatewayChecksum != "crc32c" && gatewayChecksum != "sha256")) {
        error = "Gateway returned invalid session chunking/checksum";
        return false;
    }
    if (gatewayChecksum != options.checksumType) {
        error = "Gateway checksum policy " + gatewayChecksum +
            " differs from requested SDK checksum " + options.checksumType;
        return false;
    }
    const std::vector<uint32_t> completedValues = miniKV::util::jsonUIntArray(session.body, "completed");
    const std::set<uint32_t> completed(completedValues.begin(), completedValues.end());
    std::atomic<uint64_t> completedBytes{0};
    uint32_t validCompletedChunks = 0;
    for(const uint32_t index : completed) {
        if(index >= totalChunks) continue;
        ++validCompletedChunks;
        const uint64_t offset = static_cast<uint64_t>(index) * chunkSize;
        completedBytes.fetch_add(std::min<uint64_t>(chunkSize, fileSize - offset),
                                 std::memory_order_relaxed);
    }
    std::atomic<uint32_t> completedChunkCount{validCompletedChunks};
    if(options.onProgress) {
        options.onProgress({completedBytes.load(std::memory_order_relaxed), fileSize,
                            validCompletedChunks, totalChunks});
    }
    std::vector<uint32_t> pending;
    pending.reserve(totalChunks);
    for (uint32_t index = 0; index < totalChunks; ++index) {
        if (!completed.count(index)) pending.push_back(index);
    }
    std::atomic<size_t> next{0};
    std::atomic<bool> failed{false};
    std::mutex errorMutex;
    std::mutex timingMutex;
    std::string chunkError;
    const uint32_t workerCount = std::min<uint32_t>(options.chunkWindow,
        static_cast<uint32_t>(pending.size()));
    std::vector<std::thread> workers;
    workers.reserve(workerCount);
    for (uint32_t worker = 0; worker < workerCount; ++worker) {
        workers.emplace_back([&] {
            while (!failed.load()) {
                const size_t task = next.fetch_add(1);
                if (task >= pending.size()) return;
                std::string currentError;
                UploadPhaseTimings chunkTimings;
                if (options.acquireChunkTimed) {
                    chunkTimings.chunkBudgetWaitMs = options.acquireChunkTimed();
                } else if (options.acquireChunk) {
                    options.acquireChunk();
                }
                const bool uploaded = putChunk(config_, input, sessionId, fileSize, chunkSize, pending[task],
                                               gatewayChecksum, chunkTimings, currentError);
                if (options.releaseChunk) options.releaseChunk();
                {
                    std::lock_guard<std::mutex> lock(timingMutex);
                    // Keep the admission wait even when the subsequent PUT
                    // fails; otherwise failed c16 requests hide the very
                    // queueing delay this diagnostic is meant to expose.
                    out.timings.chunkBudgetWaitMs += chunkTimings.chunkBudgetWaitMs;
                    if (uploaded) {
                        out.timings.checksumPreparationMs += chunkTimings.checksumPreparationMs;
                        out.timings.routeCapabilityMs += chunkTimings.routeCapabilityMs;
                        out.timings.dataNodeUploadMs += chunkTimings.dataNodeUploadMs;
                        out.timings.chunkUploadTotalMs += chunkTimings.chunkUploadTotalMs;
                    }
                }
                if (!uploaded) {
                    std::lock_guard<std::mutex> lock(errorMutex);
                    if (!failed.exchange(true)) chunkError = "chunk " + std::to_string(pending[task]) +
                        ": " + currentError;
                    return;
                }
                if(options.onProgress) {
                    const uint32_t chunkIndex = pending[task];
                    const uint64_t offset = static_cast<uint64_t>(chunkIndex) * chunkSize;
                    const uint64_t chunkBytes = std::min<uint64_t>(chunkSize, fileSize - offset);
                    const uint64_t logical = completedBytes.fetch_add(
                        chunkBytes, std::memory_order_relaxed) + chunkBytes;
                    const uint32_t done = completedChunkCount.fetch_add(
                        1, std::memory_order_relaxed) + 1;
                    options.onProgress({logical, fileSize, done, totalChunks});
                }
            }
        });
    }
    for (std::thread& worker : workers) worker.join();
    if (failed.load()) { error = chunkError; return false; }
    HttpResponse committed;
    const auto commitStartedAt = std::chrono::steady_clock::now();
    if (!requestSuccess(config_.gateway, "POST", "/api/v2/upload/sessions/" + pathEscape(sessionId) +
                        "/commit", "{}", config_.gatewayTimeoutMs, committed, error)) return false;
    out.timings.objectCommitMs = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - commitStartedAt).count();
    out.sessionId = sessionId;
    out.object.objectId = miniKV::util::jsonString(committed.body, "objectId");
    out.object.objectVersion = miniKV::util::jsonUint(committed.body, "objectVersion", 1);
    out.fileHash = miniKV::util::jsonString(committed.body, "fileHash");
    out.chunkSize = static_cast<uint32_t>(chunkSize);
    out.chunkCount = totalChunks;
    if (out.fileHash.empty() || out.object.objectId.empty() || out.object.objectVersion == 0) {
        error = "Gateway returned incomplete commit identity";
        return false;
    }
    return true;
}

bool MiniDriverClient::putObject(const std::filesystem::path& input, const std::string& fileName,
                                 const std::string& dirPath, const UploadOptions& options,
                                 ObjectRef& out, std::string& error) const {
    UploadResult uploaded;
    if (!uploadFile(input, fileName, dirPath, options, uploaded, error)) return false;
    out = std::move(uploaded.object);
    return true;
}

bool MiniDriverClient::putObject(const ObjectSource& source, const PutObjectOptions& options,
                                 ObjectRef& out, std::string& error) const {
    if(source.file.empty()) { error = "replayable object source file is required"; return false; }
    // Gateway's legacy catalog still needs an entry name while the Object API
    // is being migrated.  Keep it opaque so business naming is not smuggled
    // into storage identity or the compute contract.
    const std::string internalName = "_object_" + miniKV::util::randomId();
    return putObject(source.file, internalName, "/", options.transfer, out, error);
}

bool MiniDriverClient::getObject(const ObjectRef& object, const ReadOptions& options,
                                 const ObjectSink& sink, TransferStats& stats,
                                 std::string& error) {
    if (!sink) { error = "object sink is required"; return false; }
    ObjectReadPlan plan;
    if (!getReadPlan(object, plan, error)) return false;
    uint64_t delivered = 0;
    for (const ChunkReadPlan& chunk : plan.chunks) {
        std::string body;
        if (!readWholeChunk(chunk, body, options, stats, error)) return false;
        if (!sink(body.data(), body.size(), error)) {
            if (error.empty()) error = "object sink rejected bytes";
            return false;
        }
        delivered += body.size();
    }
    if (delivered != plan.fileSize) { error = "object length mismatch"; return false; }
    return true;
}

bool MiniDriverClient::getRange(const ObjectRef& object, uint64_t offset, uint64_t length,
                                const ReadOptions& options, const ObjectSink& sink,
                                TransferStats& stats, RangeReadResult& result,
                                std::string& error) {
    result = {};
    if (!sink || length == 0) { error = "object sink and positive range length are required"; return false; }
    ObjectReadPlan plan;
    if (!getReadPlan(object, plan, error)) return false;
    if (offset >= plan.fileSize || length > plan.fileSize - offset) {
        error = "range exceeds object";
        return false;
    }
    const uint64_t end = offset + length;
    uint64_t chunkStart = 0;
    bool allWholeChunks = true;
    for (const ChunkReadPlan& chunk : plan.chunks) {
        const uint64_t chunkEnd = chunkStart + chunk.size;
        const uint64_t begin = std::max(offset, chunkStart);
        const uint64_t finish = std::min(end, chunkEnd);
        if (begin < finish) {
            const uint64_t localOffset = begin - chunkStart;
            const uint64_t wanted = finish - begin;
            IntegrityStatus integrity = IntegrityStatus::kUnverifiedPartialRange;
            std::string body;
            if (!readChunkRange(chunk, localOffset, wanted, body, options, stats, integrity, error)) return false;
            if (!sink(body.data(), body.size(), error)) {
                if (error.empty()) error = "object sink rejected bytes";
                return false;
            }
            result.bytesRead += body.size();
            allWholeChunks = allWholeChunks && integrity == IntegrityStatus::kVerifiedWholeChunk;
        }
        chunkStart = chunkEnd;
    }
    if (result.bytesRead != length) { error = "range length mismatch"; return false; }
    result.integrity = allWholeChunks ? IntegrityStatus::kVerifiedWholeChunk
                                      : IntegrityStatus::kUnverifiedPartialRange;
    return true;
}

bool MiniDriverClient::headObject(const ObjectRef& object, ObjectInfo& out, std::string& error) const {
    out = {};
    if (object.objectId.empty() || object.objectVersion == 0) {
        error = "object reference is required";
        return false;
    }
    HttpResponse response;
    const std::map<std::string, std::string> headers{
        {"X-Cluster-Internal-Token", config_.clusterInternalToken},
        {"X-Service-Principal", config_.servicePrincipal},
    };
    if (config_.clusterInternalToken.empty() || config_.servicePrincipal.empty()) {
        error = "cluster token and service principal are required";
        return false;
    }
    if (!httpRequest(config_.gateway, "GET", "/internal/v3/objects/" + pathEscape(object.objectId) +
                     "/versions/" + std::to_string(object.objectVersion) + "/head", headers, "",
                     config_.gatewayTimeoutMs, response, error)) return false;
    if (response.status != 200) { error = "object head HTTP " + std::to_string(response.status); return false; }
    ObjectInfo parsed;
    parsed.object.objectId = miniKV::util::jsonString(response.body, "objectId");
    parsed.object.objectVersion = miniKV::util::jsonUint(response.body, "objectVersion");
    parsed.metadataVersion = miniKV::util::jsonUint(response.body, "metadataVersion");
    parsed.size = miniKV::util::jsonUint(response.body, "fileSize");
    parsed.name = miniKV::util::jsonString(response.body, "name");
    parsed.parentPath = miniKV::util::jsonString(response.body, "parentPath");
    parsed.state = miniKV::util::jsonString(response.body, "state");
    if (parsed.object.objectId != object.objectId || parsed.object.objectVersion != object.objectVersion ||
        parsed.size == 0 || parsed.state.empty()) {
        error = "invalid object head response";
        return false;
    }
    out = std::move(parsed);
    return true;
}

bool MiniDriverClient::deleteObject(const ObjectRef& object, std::string& error) const {
    if (object.objectId.empty() || object.objectVersion == 0) {
        error = "object reference is required";
        return false;
    }
    HttpResponse response;
    const std::map<std::string, std::string> headers{
        {"X-Cluster-Internal-Token", config_.clusterInternalToken},
        {"X-Service-Principal", config_.servicePrincipal},
    };
    if (config_.clusterInternalToken.empty() || config_.servicePrincipal.empty()) {
        error = "cluster token and service principal are required";
        return false;
    }
    if (!httpRequest(config_.gateway, "DELETE", "/internal/v3/objects/" + pathEscape(object.objectId) +
                     "/versions/" + std::to_string(object.objectVersion) + "/delete", headers, "",
                     config_.gatewayTimeoutMs, response, error)) return false;
    if (response.status != 202) { error = "object delete HTTP " + std::to_string(response.status); return false; }
    return true;
}

bool MiniDriverClient::getObjectReadHints(const ObjectRef& object, ObjectReadHints& out,
                                          std::string& error) const {
    out = {};
    if(object.objectId.empty() || object.objectVersion == 0 || config_.clusterInternalToken.empty() ||
       config_.servicePrincipal.empty()) { error = "object reference and control credentials are required"; return false; }
    HttpResponse response;
    const std::map<std::string, std::string> headers{{"X-Cluster-Internal-Token", config_.clusterInternalToken},
                                                     {"X-Service-Principal", config_.servicePrincipal}};
    if(!httpRequest(config_.gateway, "POST", "/internal/v3/objects/" + pathEscape(object.objectId) +
                    "/versions/" + std::to_string(object.objectVersion) + "/read-hints", headers, "",
                    config_.gatewayTimeoutMs, response, error)) return false;
    if(response.status != 200) { error = "object read hints HTTP " + std::to_string(response.status); return false; }
    return parseReadHints(response.body, object, out, error);
}

bool MiniDriverClient::batchGetObjectReadHints(const std::vector<ObjectRef>& objects,
                                               std::vector<ObjectReadHints>& out,
                                               std::string& error) const {
    out.clear();
    if(objects.empty() || config_.clusterInternalToken.empty() || config_.servicePrincipal.empty()) {
        error = "objects and control credentials are required"; return false;
    }
    std::ostringstream body;
    body << "{\"objects\":[";
    for(size_t index = 0; index < objects.size(); ++index) {
        if(objects[index].objectId.empty() || objects[index].objectVersion == 0) {
            error = "object reference is required"; return false;
        }
        if(index != 0) body << ',';
        body << "{\"objectId\":\"" << miniKV::util::jsonEscape(objects[index].objectId)
             << "\",\"objectVersion\":" << objects[index].objectVersion << '}';
    }
    body << "]}";
    HttpResponse response;
    const std::map<std::string, std::string> headers{{"Content-Type", "application/json"},
                                                     {"X-Cluster-Internal-Token", config_.clusterInternalToken},
                                                     {"X-Service-Principal", config_.servicePrincipal}};
    if(!httpRequest(config_.gateway, "POST", "/internal/v3/objects/read-hints:batch", headers, body.str(),
                    config_.gatewayTimeoutMs, response, error)) return false;
    if(response.status != 200) { error = "batch object read hints HTTP " + std::to_string(response.status); return false; }
    const std::vector<std::string> values = miniKV::util::jsonObjectArray(response.body, "objects");
    if(values.size() != objects.size()) { error = "batch object read hints count mismatch"; return false; }
    out.reserve(values.size());
    for(size_t index = 0; index < values.size(); ++index) {
        ObjectReadHints hints;
        if(!parseReadHints(values[index], objects[index], hints, error)) { out.clear(); return false; }
        out.push_back(std::move(hints));
    }
    return true;
}

bool MiniDriverClient::readChunkRange(const ChunkReadPlan& chunk, uint64_t offset, uint64_t length,
                                      std::string& out, const ReadOptions& options,
                                      TransferStats& stats, IntegrityStatus& integrity,
                                      std::string& error) {
    if (length == 0 || offset >= chunk.size || length > chunk.size - offset) {
        error = "invalid chunk range";
        return false;
    }
    const bool wholeChunk = offset == 0 && length == chunk.size;
    const uint32_t attempts = std::min<uint32_t>(static_cast<uint32_t>(chunk.replicas.size()),
        options.allowReplicaRetry ? std::max<uint32_t>(1, options.maxReplicaAttempts) : 1);
    std::string lastError;
    for (uint32_t attempt = 0; attempt < attempts; ++attempt) {
        const ReplicaTarget& candidate = chunk.replicas[attempt];
        const StreamingRequest::Stats before = dataRequest_.stats();
        HttpResponse response;
        std::map<std::string, std::string> headers;
        const std::string path = chunk.readCapability.empty()
            ? "/v2/chunks/" + pathEscape(chunk.storageIdentity)
            : "/internal/v3/chunks/" + pathEscape(chunk.storageIdentity);
        if (!chunk.readCapability.empty()) headers.emplace("X-Read-Token", chunk.readCapability);
        if (!wholeChunk) {
            headers.emplace("Range", "bytes=" + std::to_string(offset) + "-" +
                std::to_string(offset + length - 1));
        }
        const bool ok = dataRequest_.open(candidate.endpoint, "GET", path, headers, 0,
                                     config_.dataNodeTimeoutMs, lastError, options.keepAlive) &&
            dataRequest_.write(nullptr, 0, lastError) &&
            dataRequest_.finish(response, lastError, options.maxReadBytesPerSecond);
        addDelta(before, dataRequest_.stats(), stats);
        if (!ok) continue;
        const int expectedStatus = wholeChunk ? 200 : 206;
        if (response.status != expectedStatus) {
            lastError = "DataNode HTTP " + std::to_string(response.status);
            continue;
        }
        if (response.body.size() != length) {
            lastError = "DataNode range length mismatch";
            continue;
        }
        if (wholeChunk && options.verifyChecksum && !verifyWhole(chunk, response.body, lastError)) continue;
        if (attempt > 0) ++stats.replicaFallbacks;
        if (wholeChunk && options.verifyChecksum) {
            stats.bytesVerified += response.body.size();
            integrity = IntegrityStatus::kVerifiedWholeChunk;
        } else if (wholeChunk) {
            integrity = IntegrityStatus::kUnverifiedChecksumDisabled;
        } else {
            integrity = IntegrityStatus::kUnverifiedPartialRange;
        }
        out = std::move(response.body);
        return true;
    }
    error = "cannot read chunk " + std::to_string(chunk.index) + ": " + lastError;
    return false;
}

bool MiniDriverClient::readWholeChunk(const ChunkReadPlan& chunk, std::string& out,
                                      const ReadOptions& options, TransferStats& stats,
                                      std::string& error) {
    IntegrityStatus ignored = IntegrityStatus::kUnverifiedPartialRange;
    return readChunkRange(chunk, 0, chunk.size, out, options, stats, ignored, error);
}

bool MiniDriverClient::downloadToFile(const ObjectReadPlan& plan, const std::filesystem::path& output,
                                      const ReadOptions& options, TransferStats& stats,
                                      std::string& error) {
    std::ofstream file(output, std::ios::binary | std::ios::trunc);
    if (!file) { error = "cannot create " + output.string(); return false; }
    for (const ChunkReadPlan& chunk : plan.chunks) {
        std::string body;
        if (!readWholeChunk(chunk, body, options, stats, error)) return false;
        file.write(body.data(), static_cast<std::streamsize>(body.size()));
        if (!file) { error = "cannot write downloaded object"; return false; }
    }
    return true;
}

bool MiniDriverClient::downloadToSink(const ObjectReadPlan& plan, const ReadOptions& options,
                                      TransferStats& stats, std::string& error) {
    uint64_t bytesRead = 0;
    for (const ChunkReadPlan& chunk : plan.chunks) {
        std::string body;
        if (!readWholeChunk(chunk, body, options, stats, error)) return false;
        if (body.size() != chunk.size) {
            error = "downloaded chunk length does not match read plan";
            return false;
        }
        bytesRead += body.size();
    }
    if (bytesRead != plan.fileSize) {
        error = "downloaded object length does not match read plan";
        return false;
    }
    return true;
}

bool MiniDriverClient::downloadRangeToFile(const ObjectReadPlan& plan, uint64_t offset, uint64_t length,
                                           const std::filesystem::path& output,
                                           const ReadOptions& options, TransferStats& stats,
                                           RangeReadResult& result, std::string& error) {
    result = {};
    if (plan.fileSize == 0 || length == 0 || offset >= plan.fileSize ||
        length > plan.fileSize - offset) {
        error = "invalid object range";
        return false;
    }
    std::ofstream file(output, std::ios::binary | std::ios::trunc);
    if (!file) { error = "cannot create " + output.string(); return false; }

    const uint64_t end = offset + length;
    uint64_t chunkObjectOffset = 0;
    uint64_t written = 0;
    bool allVerified = true;
    bool checksumDisabled = false;
    for (const ChunkReadPlan& chunk : plan.chunks) {
        if (chunk.size == 0 || chunk.size > plan.fileSize ||
            chunkObjectOffset > plan.fileSize - chunk.size) {
            error = "invalid object chunk layout";
            return false;
        }
        const uint64_t chunkEnd = chunkObjectOffset + chunk.size;
        if (chunkEnd > offset && chunkObjectOffset < end) {
            const uint64_t start = std::max(offset, chunkObjectOffset);
            const uint64_t stop = std::min(end, chunkEnd);
            const uint64_t localOffset = start - chunkObjectOffset;
            const uint64_t localLength = stop - start;
            std::string body;
            IntegrityStatus pieceIntegrity = IntegrityStatus::kUnverifiedPartialRange;
            if (!readChunkRange(chunk, localOffset, localLength, body, options, stats,
                                pieceIntegrity, error)) return false;
            file.write(body.data(), static_cast<std::streamsize>(body.size()));
            if (!file) { error = "cannot write downloaded range"; return false; }
            written += body.size();
            if (pieceIntegrity != IntegrityStatus::kVerifiedWholeChunk) allVerified = false;
            if (pieceIntegrity == IntegrityStatus::kUnverifiedChecksumDisabled) checksumDisabled = true;
        }
        chunkObjectOffset = chunkEnd;
    }
    if (chunkObjectOffset != plan.fileSize || written != length) {
        error = "object range does not match chunk layout";
        return false;
    }
    result.bytesRead = written;
    result.integrity = allVerified ? IntegrityStatus::kVerifiedWholeChunk :
        (checksumDisabled ? IntegrityStatus::kUnverifiedChecksumDisabled :
                            IntegrityStatus::kUnverifiedPartialRange);
    return true;
}

}  // namespace miniKV::client
