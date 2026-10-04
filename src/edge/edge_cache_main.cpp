#include "client/MiniDriverClient.hpp"
#include "edge/EdgeCacheStore.hpp"
#include "http/HttpRange.hpp"
#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "http/HttpServer.hpp"
#include "network/EventLoop.hpp"
#include "utils/AsyncLogger.hpp"
#include "utils/ThreadPool.hpp"

#include <atomic>
#include <cctype>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

using miniKV::client::ClientConfig;
using miniKV::client::MiniDriverClient;
using miniKV::client::ObjectInfo;
using miniKV::client::ObjectReadPlan;
using miniKV::client::ObjectRef;
using miniKV::client::ReadOptions;
using miniKV::client::TransferStats;
using miniKV::edge::CacheLease;
using miniKV::edge::CacheReservation;
using miniKV::edge::EdgeCacheKey;
using miniKV::edge::EdgeCacheStore;
using miniKV::http::HttpRequest;
using miniKV::http::HttpResponse;

std::atomic<bool> gStop{false};

void onSignal(int) {
    gStop.store(true, std::memory_order_relaxed);
}

std::string envString(const char* name, std::string fallback = {}) {
    const char* value = std::getenv(name);
    return value == nullptr || *value == '\0' ? fallback : value;
}

uint64_t envUint(const char* name, uint64_t fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') return fallback;
    try {
        size_t parsed = 0;
        const unsigned long long number = std::stoull(value, &parsed);
        if (parsed != std::strlen(value)) return fallback;
        return static_cast<uint64_t>(number);
    } catch (...) {
        return fallback;
    }
}

bool startsWith(std::string_view text, std::string_view prefix) {
    return text.size() >= prefix.size() && text.substr(0, prefix.size()) == prefix;
}

std::string jsonEscape(std::string_view text) {
    std::string out;
    out.reserve(text.size() + 8);
    for (const unsigned char c : text) {
        switch (c) {
        case '"': out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default:
            if (c < 0x20U) out += "?";
            else out.push_back(static_cast<char>(c));
        }
    }
    return out;
}

void error(HttpResponse* response, HttpResponse::HttpStatusCode status, std::string message) {
    response->setStatusCode(status);
    response->setContentType("application/json");
    response->setBody("{\"error\":\"" + jsonEscape(message) + "\"}");
}

std::string contentTypeForName(const std::string& name) {
    const auto dot = name.find_last_of('.');
    if (dot == std::string::npos) return "application/octet-stream";
    std::string extension = name.substr(dot + 1);
    for (char& c : extension) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (extension == "mp4" || extension == "m4v") return "video/mp4";
    if (extension == "webm") return "video/webm";
    if (extension == "mkv") return "video/x-matroska";
    if (extension == "mov") return "video/quicktime";
    if (extension == "avi") return "video/x-msvideo";
    return "application/octet-stream";
}

bool parseObjectPath(const std::string& path, ObjectRef& object, std::string& errorMessage) {
    constexpr std::string_view prefix = "/v1/play/";
    if (!startsWith(path, prefix)) {
        errorMessage = "route not found";
        return false;
    }
    const std::string tail = path.substr(prefix.size());
    const size_t slash = tail.rfind('/');
    if (slash == std::string::npos || slash == 0 || slash + 1 >= tail.size() ||
        tail.find('/', slash + 1) != std::string::npos) {
        errorMessage = "expected /v1/play/{objectId}/{objectVersion}";
        return false;
    }
    object.objectId = tail.substr(0, slash);
    try {
        size_t parsed = 0;
        object.objectVersion = std::stoull(tail.substr(slash + 1), &parsed);
        if (parsed != tail.size() - slash - 1 || object.objectVersion == 0) throw std::invalid_argument("version");
    } catch (...) {
        errorMessage = "objectVersion must be a positive integer";
        return false;
    }
    if (object.objectId.empty() || object.objectId.find("..") != std::string::npos ||
        object.objectId.find('/') != std::string::npos) {
        errorMessage = "invalid objectId";
        return false;
    }
    return true;
}

struct EdgeConfig {
    ClientConfig client;
    std::shared_ptr<EdgeCacheStore> cache;
};

void addObjectHeaders(HttpResponse* response, const ObjectInfo& info,
                      uint64_t totalSize, uint64_t start, uint64_t length, bool partial) {
    response->setStatusCode(partial ? HttpResponse::k206PartialContent : HttpResponse::k200Ok);
    response->setContentType(contentTypeForName(info.name));
    response->addHeader("Accept-Ranges", "bytes");
    response->addHeader("Content-Length", std::to_string(length));
    response->addHeader("X-MiniDriver-Object-Id", info.object.objectId);
    response->addHeader("X-MiniDriver-Object-Version", std::to_string(info.object.objectVersion));
    response->addHeader("X-MiniDriver-Edge-Mode", "whole-chunk-cache");
    if (partial) {
        response->addHeader("Content-Range", "bytes " + std::to_string(start) + "-" +
                            std::to_string(start + length - 1) + "/" + std::to_string(totalSize));
    }
}

void handlePlayback(const HttpRequest& request, HttpResponse* response, const EdgeConfig& config) {
    ObjectRef object;
    std::string failure;
    if (!parseObjectPath(request.path(), object, failure)) {
        error(response, HttpResponse::k404NotFound, failure);
        return;
    }
    if (request.method() != HttpRequest::kGet && request.method() != HttpRequest::kHead) {
        response->addHeader("Allow", "GET, HEAD");
        error(response, HttpResponse::k405MethodNotAllowed, "only GET and HEAD are supported");
        return;
    }

    // Step 3 runs in the existing trusted-cluster lab boundary. The Edge
    // process owns the control-plane credential; the external playback token
    // is deliberately deferred to Step 9.
    MiniDriverClient client(config.client);
    ObjectInfo info;
    if (!client.headObject(object, info, failure)) {
        error(response, HttpResponse::k404NotFound, "cannot resolve committed object: " + failure);
        return;
    }
    miniKV::http::ByteRange parsedRange;
    const miniKV::http::RangeParseStatus rangeStatus =
        miniKV::http::parseSingleByteRange(request.getHeader("Range"), info.size, parsedRange);
    if (rangeStatus == miniKV::http::RangeParseStatus::kInvalid ||
        rangeStatus == miniKV::http::RangeParseStatus::kUnsatisfiable) {
        response->setStatusCode(HttpResponse::k416RangeNotSatisfiable);
        response->addHeader("Accept-Ranges", "bytes");
        response->addHeader("Content-Range", "bytes */" + std::to_string(info.size));
        response->setBody("");
        return;
    }
    const bool partial = rangeStatus == miniKV::http::RangeParseStatus::kSatisfiable;
    const uint64_t start = partial ? parsedRange.start : 0;
    const uint64_t length = partial ? parsedRange.length : info.size;
    // HEAD must never be rejected because the object is large. It is metadata
    // only and does not require a ReadPlan or any DataNode body read.
    if (request.method() == HttpRequest::kHead) {
        addObjectHeaders(response, info, info.size, start, length, partial);
        return;
    }

    ObjectReadPlan plan;
    if (!client.getReadPlan(object, plan, failure) || plan.fileSize != info.size) {
        error(response, HttpResponse::k503ServiceUnavailable, "cannot obtain object read plan: " + failure);
        return;
    }
    if (length == 0) {
        error(response, HttpResponse::k416RangeNotSatisfiable, "empty object response is unsupported");
        return;
    }

    ReadOptions options;
    options.keepAlive = true;
    options.verifyChecksum = true;
    TransferStats stats;
    std::vector<CacheLease> leases;
    std::vector<HttpResponse::FileSegment> segments;
    uint64_t objectOffset = 0;
    uint64_t originBytes = 0;
    uint64_t cacheHits = 0;
    const uint64_t end = start + length;
    for (const auto& chunk : plan.chunks) {
        if (chunk.size == 0 || objectOffset > plan.fileSize - chunk.size) {
            error(response, HttpResponse::k500InternalServerError, "invalid object chunk layout");
            return;
        }
        const uint64_t chunkEnd = objectOffset + chunk.size;
        if (chunkEnd > start && objectOffset < end) {
            const EdgeCacheKey key = EdgeCacheKey::fromChunk(object, chunk);
            CacheLease lease;
            std::string cacheError;
            if (!config.cache->acquire(key, lease, cacheError)) {
                CacheReservation reservation;
                if (!config.cache->reserve(key, reservation, cacheError)) {
                    response->addHeader("Retry-After", "1");
                    error(response, HttpResponse::k503ServiceUnavailable,
                          "edge cache fill unavailable: " + cacheError);
                    return;
                }
                std::string body;
                if (!client.readWholeChunk(chunk, body, options, stats, failure)) {
                    error(response, HttpResponse::k503ServiceUnavailable,
                          "origin chunk read failed: " + failure);
                    return;
                }
                originBytes += body.size();
                if (!config.cache->publish(key, body, reservation, lease, cacheError)) {
                    error(response, HttpResponse::k500InternalServerError,
                          "edge cache publish failed: " + cacheError);
                    return;
                }
            } else {
                ++cacheHits;
            }
            const uint64_t segmentStart = std::max(start, objectOffset);
            const uint64_t segmentEnd = std::min(end, chunkEnd);
            segments.push_back({lease.path().string(),
                                static_cast<off_t>(segmentStart - objectOffset),
                                static_cast<size_t>(segmentEnd - segmentStart)});
            leases.push_back(std::move(lease));
        }
        objectOffset = chunkEnd;
    }
    if (objectOffset != plan.fileSize || segments.empty()) {
        error(response, HttpResponse::k500InternalServerError, "object range does not map to cache segments");
        return;
    }
    addObjectHeaders(response, info, plan.fileSize, start, length, partial);
    auto heldLeases = std::make_shared<std::vector<CacheLease>>(std::move(leases));
    response->setFileBodies(std::move(segments), static_cast<size_t>(length),
        [heldLeases, object, start, length, stats, originBytes, cacheHits](const miniKV::network::SendFileResult& sent) {
            miniKV::utils::logInfo(
                "event=edge_response_complete object_id=" + object.objectId +
                " object_version=" + std::to_string(object.objectVersion) +
                " range_start=" + std::to_string(start) +
                " range_length=" + std::to_string(length) +
                " response_bytes=" + std::to_string(sent.bytesSent) +
                " origin_bytes=" + std::to_string(originBytes) +
                " cache_hits=" + std::to_string(cacheHits) +
                " origin_requests=" + std::to_string(stats.dataRequests) +
                " replica_fallbacks=" + std::to_string(stats.replicaFallbacks) +
                " success=" + (sent.success ? "true" : "false"));
        });
}

}  // namespace

int main(int, char**) {
    const uint64_t configuredPort = envUint("MINIDRIVER_EDGE_LISTEN_PORT", 19100);
    const uint64_t handlerThreads = envUint("MINIDRIVER_EDGE_HANDLER_THREADS", 2);
    const uint64_t ioThreads = envUint("MINIDRIVER_EDGE_IO_THREADS", 2);
    if (configuredPort == 0 || configuredPort > 65535 || handlerThreads == 0 || ioThreads == 0) {
        std::cerr << "invalid Edge port or thread count\n";
        return 2;
    }

    EdgeConfig config;
    config.client.gateway.host = envString("MINIDRIVER_EDGE_GATEWAY_HOST", "127.0.0.1");
    config.client.gateway.port = static_cast<uint16_t>(envUint("MINIDRIVER_EDGE_GATEWAY_PORT", 18080));
    config.client.clusterInternalToken = envString("MINIDRIVER_EDGE_CLUSTER_TOKEN");
    config.client.servicePrincipal = envString("MINIDRIVER_EDGE_SERVICE_PRINCIPAL", "edge-cache");
    config.client.gatewayTimeoutMs = static_cast<int>(envUint("MINIDRIVER_EDGE_GATEWAY_TIMEOUT_MS", 30000));
    config.client.dataNodeTimeoutMs = static_cast<int>(envUint("MINIDRIVER_EDGE_DATANODE_TIMEOUT_MS", 60000));
    EdgeCacheStore::Config cacheConfig;
    const std::string cacheRoot = envString("MINIDRIVER_EDGE_CACHE_ROOT", "/data/minidriver-edge-cache");
    cacheConfig.root = cacheRoot;
    cacheConfig.capacityBytes = envUint("MINIDRIVER_EDGE_CACHE_CAPACITY_BYTES", 32ULL * 1024ULL * 1024ULL * 1024ULL);
    cacheConfig.verifyHitChecksum = envUint("MINIDRIVER_EDGE_VERIFY_HIT_CHECKSUM", 1) != 0;
    config.cache = std::make_shared<EdgeCacheStore>(std::move(cacheConfig));
    if (config.client.gateway.port == 0 || config.client.clusterInternalToken.empty() ||
        config.client.servicePrincipal.empty()) {
        std::cerr << "MINIDRIVER_EDGE_GATEWAY_PORT, MINIDRIVER_EDGE_CLUSTER_TOKEN, and "
                     "MINIDRIVER_EDGE_SERVICE_PRINCIPAL are required\n";
        return 2;
    }
    std::string cacheError;
    if (!config.cache->initialize(cacheError)) {
        std::cerr << "cannot initialize Edge cache: " << cacheError << '\n';
        return 2;
    }

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    miniKV::network::EventLoop loop;
    miniKV::utils::ThreadPool workers(static_cast<size_t>(handlerThreads));
    miniKV::http::HttpServer server(&loop, &workers, static_cast<int>(configuredPort));
    server.setThreadNum(static_cast<size_t>(ioThreads));
    server.setHttpCallback([&config](const HttpRequest& request, HttpResponse* response,
                                      const miniKV::network::TcpConnectionPtr&,
                                      const miniKV::http::DeferredResponse::Ptr&) {
        if (request.method() == HttpRequest::kGet && request.path() == "/healthz") {
            response->setStatusCode(HttpResponse::k200Ok);
            response->setContentType("application/json");
            const auto stats = config.cache->stats();
            response->setBody("{\"status\":\"ok\",\"component\":\"edge-cache\","
                              "\"mode\":\"whole-chunk-cache\",\"readyBytes\":" +
                              std::to_string(stats.readyBytes) + ",\"reservedBytes\":" +
                              std::to_string(stats.reservedBytes) + "}");
            return;
        }
        handlePlayback(request, response, config);
    });
    server.start();
    loop.runEvery(100, [&loop] {
        if (gStop.load(std::memory_order_relaxed)) loop.quit();
    });
    std::cout << "edge_cache_started port=" << configuredPort
              << " gateway=" << config.client.gateway.host << ':' << config.client.gateway.port
              << " cache_root=" << cacheRoot
              << " cache_capacity_bytes=" << envUint("MINIDRIVER_EDGE_CACHE_CAPACITY_BYTES", 32ULL * 1024ULL * 1024ULL * 1024ULL)
              << '\n';
    loop.loop();
    return 0;
}
