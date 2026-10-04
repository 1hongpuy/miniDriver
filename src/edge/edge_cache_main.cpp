#include "client/MiniDriverClient.hpp"
#include "client/HttpTransport.hpp"
#include "edge/EdgeCacheStore.hpp"
#include "http/HttpRange.hpp"
#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "http/HttpServer.hpp"
#include "network/Buffer.hpp"
#include "network/EventLoop.hpp"
#include "network/TcpConnection.hpp"
#include "utils/AsyncLogger.hpp"
#include "utils/ThreadPool.hpp"
#include "utils/Util.hpp"

#include <atomic>
#include <cctype>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

using miniKV::client::ClientConfig;
using miniKV::client::ChunkReadPlan;
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
using miniKV::network::TcpConnectionPtr;

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

std::string queryEscape(std::string_view value) {
    static constexpr char digits[] = "0123456789ABCDEF";
    std::string result;
    result.reserve(value.size() * 3U);
    for (const unsigned char byte : value) {
        const bool safe = (byte >= 'a' && byte <= 'z') ||
                          (byte >= 'A' && byte <= 'Z') ||
                          (byte >= '0' && byte <= '9') ||
                          byte == '-' || byte == '_' || byte == '.' || byte == '~';
        if (safe) result.push_back(static_cast<char>(byte));
        else {
            result.push_back('%');
            result.push_back(digits[(byte >> 4U) & 0x0fU]);
            result.push_back(digits[byte & 0x0fU]);
        }
    }
    return result;
}

bool parseVirtualPath(const std::string& path, std::string& virtualPath,
                      std::string& parentPath, std::string& leafName,
                      std::string& errorMessage) {
    constexpr std::string_view prefix = "/vod/";
    if (!startsWith(path, prefix)) {
        errorMessage = "route not found";
        return false;
    }
    const std::string tail = miniKV::util::urlDecode(path.substr(prefix.size()));
    if (tail.empty() || tail.front() == '/' || tail.find('\0') != std::string::npos) {
        errorMessage = "expected /vod/{virtualPath}";
        return false;
    }
    size_t begin = 0;
    while (begin < tail.size()) {
        const size_t end = tail.find('/', begin);
        const std::string_view part(tail.data() + begin,
                                    (end == std::string::npos ? tail.size() : end) - begin);
        if (part.empty() || part == "." || part == "..") {
            errorMessage = "invalid virtual path";
            return false;
        }
        if (end == std::string::npos) break;
        begin = end + 1;
    }
    virtualPath = "/" + tail;
    const size_t slash = virtualPath.find_last_of('/');
    parentPath = slash == 0 ? "/" : virtualPath.substr(0, slash);
    leafName = virtualPath.substr(slash + 1);
    return true;
}

struct EdgeConfig {
    ClientConfig client;
    std::shared_ptr<EdgeCacheStore> cache;
};

bool resolveVirtualObject(const HttpRequest& request, const EdgeConfig& config,
                          ObjectRef& object, std::string& virtualPath,
                          std::string& errorMessage) {
    std::string parentPath;
    std::string leafName;
    if (!parseVirtualPath(request.path(), virtualPath, parentPath, leafName, errorMessage)) return false;

    std::map<std::string, std::string> headers;
    headers.emplace("X-Cluster-Internal-Token", config.client.clusterInternalToken);
    headers.emplace("X-Service-Principal", config.client.servicePrincipal);
    miniKV::client::HttpResponse catalog;
    if (!miniKV::client::httpRequest(config.client.gateway, "GET",
                                     "/api/v2/catalog?path=" + queryEscape(parentPath), headers, "",
                                     config.client.gatewayTimeoutMs, catalog, errorMessage)) {
        errorMessage = "cannot resolve virtual path: " + errorMessage;
        return false;
    }
    if (catalog.status != 200) {
        const std::string message = miniKV::util::jsonString(catalog.body, "error");
        errorMessage = "catalog HTTP " + std::to_string(catalog.status) +
                       (message.empty() ? std::string{} : ": " + message);
        return false;
    }
    for (const std::string& file : miniKV::util::jsonObjectArray(catalog.body, "files")) {
        if (miniKV::util::jsonString(file, "name") != leafName) continue;
        if (miniKV::util::jsonString(file, "state") != "AVAILABLE") {
            errorMessage = "virtual path is not available";
            return false;
        }
        object.objectId = miniKV::util::jsonString(file, "objectId");
        object.objectVersion = miniKV::util::jsonUint(file, "objectVersion");
        if (!object.objectId.empty() && object.objectVersion > 0) return true;
    }
    errorMessage = "virtual path not found";
    return false;
}

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


bool requestClosesConnection(const HttpRequest& request) {
    std::string connection = request.getHeader("Connection");
    for (char& value : connection) {
        value = static_cast<char>(std::tolower(static_cast<unsigned char>(value)));
    }
    if (connection == "close") return true;
    if (connection == "keep-alive") return false;
    return request.version() != "HTTP/1.1";
}

struct PlaybackStreamPart {
    ChunkReadPlan chunk;
    uint64_t cacheOffset = 0;
    uint64_t length = 0;
};

bool buildPlaybackStreamParts(const ObjectReadPlan& plan, uint64_t start, uint64_t length,
                              std::vector<PlaybackStreamPart>& out, std::string& errorMessage) {
    out.clear();
    const uint64_t end = start + length;
    uint64_t objectOffset = 0;
    for (const ChunkReadPlan& chunk : plan.chunks) {
        if (chunk.size == 0 || objectOffset > plan.fileSize - chunk.size) {
            errorMessage = "invalid object chunk layout";
            return false;
        }
        const uint64_t chunkEnd = objectOffset + chunk.size;
        if (chunkEnd > start && objectOffset < end) {
            const uint64_t segmentStart = std::max(start, objectOffset);
            const uint64_t segmentEnd = std::min(end, chunkEnd);
            const uint64_t segmentLength = segmentEnd - segmentStart;
            if (segmentLength == 0 ||
                segmentLength > static_cast<uint64_t>(std::numeric_limits<size_t>::max()) ||
                segmentStart - objectOffset >
                    static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
                errorMessage = "object range exceeds local file API limits";
                return false;
            }
            out.push_back({chunk, segmentStart - objectOffset, segmentLength});
        }
        objectOffset = chunkEnd;
    }
    if (objectOffset != plan.fileSize || out.empty()) {
        errorMessage = "object range does not map to storage chunks";
        return false;
    }
    return true;
}

// Sends one immutable cached Chunk segment at a time.  This is deliberately
// different from HttpResponse::setFileBodies(): an open-ended media Range may
// span a whole multi-hundred-MiB object, and waiting for every Chunk to enter
// the cache before sending headers makes FFmpeg time out during open().
class EdgePlaybackStream final : public std::enable_shared_from_this<EdgePlaybackStream> {
public:
    EdgePlaybackStream(const EdgeConfig& config, miniKV::utils::ThreadPool* workers,
                       TcpConnectionPtr connection, ObjectInfo info,
                       std::vector<PlaybackStreamPart> parts, uint64_t start,
                       uint64_t length, bool closeAfterResponse, std::string virtualPath)
        : config_(config),
          workers_(workers),
          connection_(std::move(connection)),
          info_(std::move(info)),
          parts_(std::move(parts)),
          start_(start),
          length_(length),
          closeAfterResponse_(closeAfterResponse),
          virtualPath_(std::move(virtualPath)) {}

    void start() {
        std::cerr << "event=edge_stream_begin virtual_path=" << virtualPath_
                  << " range_start=" << start_
                  << " range_length=" << length_
                  << " parts=" << parts_.size() << '\n';
        connection_->pauseRead();
        enqueueNextChunk();
    }

private:
    void enqueueNextChunk() {
        try {
            auto self = shared_from_this();
            workers_->enqueue([self] { self->fetchAndSendNextChunk(); });
        } catch (const std::exception& exception) {
            finish(false, "cannot schedule Edge Chunk fetch: " + std::string(exception.what()));
        }
    }

    void fetchAndSendNextChunk() {
        const size_t index = nextPart_.fetch_add(1, std::memory_order_relaxed);
        if (index >= parts_.size()) {
            finish(true, {});
            return;
        }
        const PlaybackStreamPart& part = parts_[index];
        const EdgeCacheKey key = EdgeCacheKey::fromChunk(info_.object, part.chunk);
        CacheLease lease;
        std::string cacheError;
        bool cacheHit = config_.cache->acquire(key, lease, cacheError);
        if (!cacheHit) {
            CacheReservation reservation;
            if (!config_.cache->reserve(key, reservation, cacheError)) {
                if (cacheError == "edge cache fill already active") {
                    std::cerr << "event=edge_stream_wait_fill virtual_path=" << virtualPath_
                              << " part=" << index << '\n';
                    if (!config_.cache->waitForReady(key, lease, std::chrono::seconds(90), cacheError)) {
                        finish(false, "edge cache concurrent fill unavailable: " + cacheError);
                        return;
                    }
                    cacheHit = true;
                } else {
                    finish(false, "edge cache fill unavailable: " + cacheError);
                    return;
                }
            }
            if (!cacheHit) {
                MiniDriverClient client(config_.client);
                ReadOptions options;
                options.keepAlive = true;
                options.verifyChecksum = true;
                TransferStats stats;
                std::string body;
                std::string failure;
                if (!client.readWholeChunk(part.chunk, body, options, stats, failure)) {
                    finish(false, "origin chunk read failed: " + failure);
                    return;
                }
                originBytes_.fetch_add(body.size(), std::memory_order_relaxed);
                originRequests_.fetch_add(stats.dataRequests, std::memory_order_relaxed);
                replicaFallbacks_.fetch_add(stats.replicaFallbacks, std::memory_order_relaxed);
                if (!config_.cache->publish(key, body, reservation, lease, cacheError)) {
                    finish(false, "edge cache publish failed: " + cacheError);
                    return;
                }
            }
        }
        if (cacheHit) {
            cacheHits_.fetch_add(1, std::memory_order_relaxed);
        }

        std::cerr << "event=edge_stream_part_ready virtual_path=" << virtualPath_
                  << " part=" << index
                  << " source=" << (cacheHit ? "cache" : "origin")
                  << " segment_offset=" << part.cacheOffset
                  << " segment_length=" << part.length << '\n';

        const bool sendHeaders = !headersScheduled_.exchange(true, std::memory_order_relaxed);
        const std::string path = lease.path().string();
        const auto heldLease = std::make_shared<CacheLease>(std::move(lease));
        const off_t offset = static_cast<off_t>(part.cacheOffset);
        const size_t bytes = static_cast<size_t>(part.length);
        auto self = shared_from_this();
        connection_->ownerLoop()->queueInLoop(
            [self, heldLease, path, offset, bytes, sendHeaders] {
                if (!self->connection_->connected()) {
                    self->finishInLoop(false, "client disconnected before Chunk send");
                    return;
                }
                if (sendHeaders) {
                    HttpResponse response;
                    response.setCloseConnection(false);
                    addObjectHeaders(&response, self->info_, self->info_.size,
                                     self->start_, self->length_, true);
                    miniKV::network::Buffer output;
                    response.appendToBuffer(&output);
                    self->connection_->send(std::string(output.peek(), output.readableBytes()));
                }
                self->connection_->startSendFile(path, offset, bytes,
                    [self, heldLease, bytes](const miniKV::network::SendFileResult& sent) {
                        // Keep the immutable cache file pinned until the kernel has
                        // consumed this segment.  The next fetch is scheduled only
                        // after backpressure has drained this segment.
                        (void)heldLease;
                        if (!sent.success || sent.bytesSent != bytes) {
                            self->finish(false, "sendfile failed after " +
                                std::to_string(sent.bytesSent) + " bytes");
                            return;
                        }
                        std::cerr << "event=edge_stream_part_sent virtual_path=" << self->virtualPath_
                                  << " bytes=" << bytes << '\n';
                        self->enqueueNextChunk();
                    });
            });
    }

    void finish(bool success, std::string reason) {
        if (finished_.exchange(true, std::memory_order_relaxed)) return;
        auto self = shared_from_this();
        connection_->ownerLoop()->queueInLoop(
            [self, success, reason = std::move(reason)]() mutable {
                self->finishInLoop(success, std::move(reason));
            });
    }

    void finishInLoop(bool success, std::string reason) {
        if (!success && !headersScheduled_.load(std::memory_order_relaxed) &&
            connection_->connected()) {
            HttpResponse response;
            response.setCloseConnection(false);
            error(&response, HttpResponse::k503ServiceUnavailable, reason);
            miniKV::network::Buffer output;
            response.appendToBuffer(&output);
            connection_->send(std::string(output.peek(), output.readableBytes()));
        }
        if (connection_->connected()) {
            connection_->resumeRead();
            if (!success || closeAfterResponse_) connection_->shutdown();
        }
        std::cerr << "event=edge_stream_complete virtual_path=" << virtualPath_
                  << " range_start=" << start_
                  << " range_length=" << length_
                  << " origin_bytes=" << originBytes_.load(std::memory_order_relaxed)
                  << " cache_hits=" << cacheHits_.load(std::memory_order_relaxed)
                  << " origin_requests=" << originRequests_.load(std::memory_order_relaxed)
                  << " replica_fallbacks=" << replicaFallbacks_.load(std::memory_order_relaxed)
                  << " success=" << (success ? "true" : "false")
                  << (reason.empty() ? "" : " reason=" + reason) << '\n';
    }

    const EdgeConfig& config_;
    miniKV::utils::ThreadPool* workers_;
    TcpConnectionPtr connection_;
    ObjectInfo info_;
    std::vector<PlaybackStreamPart> parts_;
    uint64_t start_ = 0;
    uint64_t length_ = 0;
    bool closeAfterResponse_ = false;
    std::string virtualPath_;
    std::atomic<size_t> nextPart_{0};
    std::atomic<bool> headersScheduled_{false};
    std::atomic<bool> finished_{false};
    std::atomic<uint64_t> originBytes_{0};
    std::atomic<uint64_t> originRequests_{0};
    std::atomic<uint64_t> replicaFallbacks_{0};
    std::atomic<uint64_t> cacheHits_{0};
};

bool startStreamingPlayback(const HttpRequest& request, HttpResponse* response,
                            const EdgeConfig& config, miniKV::utils::ThreadPool* workers,
                            const TcpConnectionPtr& connection,
                            const miniKV::http::DeferredResponse::Ptr& deferred) {
    ObjectRef object;
    std::string virtualPath;
    std::string failure;
    if (!resolveVirtualObject(request, config, object, virtualPath, failure)) {
        error(response, HttpResponse::k404NotFound, failure);
        return false;
    }
    MiniDriverClient client(config.client);
    ObjectInfo info;
    if (!client.headObject(object, info, failure)) {
        error(response, HttpResponse::k404NotFound, "cannot resolve committed object: " + failure);
        return false;
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
        return false;
    }
    const bool partial = rangeStatus == miniKV::http::RangeParseStatus::kSatisfiable;
    const uint64_t start = partial ? parsedRange.start : 0;
    const uint64_t length = partial ? parsedRange.length : info.size;
    if (length == 0 || length > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        error(response, HttpResponse::k416RangeNotSatisfiable, "empty or oversized object response");
        return false;
    }

    ObjectReadPlan plan;
    if (!client.getReadPlan(object, plan, failure) || plan.fileSize != info.size) {
        error(response, HttpResponse::k503ServiceUnavailable,
              "cannot obtain object read plan: " + failure);
        return false;
    }
    std::vector<PlaybackStreamPart> parts;
    if (!buildPlaybackStreamParts(plan, start, length, parts, failure)) {
        error(response, HttpResponse::k500InternalServerError, failure);
        return false;
    }

    deferred->defer();
    auto stream = std::make_shared<EdgePlaybackStream>(
        config, workers, connection, std::move(info), std::move(parts), start, length,
        requestClosesConnection(request), std::move(virtualPath));
    stream->start();
    return true;
}

void handlePlayback(const HttpRequest& request, HttpResponse* response, const EdgeConfig& config) {
    ObjectRef object;
    std::string virtualPath;
    std::string failure;
    if (!resolveVirtualObject(request, config, object, virtualPath, failure)) {
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
        [heldLeases, object, virtualPath, start, length, stats, originBytes, cacheHits](const miniKV::network::SendFileResult& sent) {
            miniKV::utils::logInfo(
                "event=edge_response_complete object_id=" + object.objectId +
                " object_version=" + std::to_string(object.objectVersion) +
                " virtual_path=" + virtualPath +
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
    server.setHttpCallback([&config, &workers](const HttpRequest& request, HttpResponse* response,
                                                const TcpConnectionPtr& connection,
                                                const miniKV::http::DeferredResponse::Ptr& deferred) {
        if (request.path() != "/healthz") {
            // EdgeCache intentionally has no file logger initialization. Write the
            // bounded request trace to stderr so `kubectl logs` can observe the
            // client-side media backend without exposing cluster credentials.
            std::cerr <<
                "event=edge_request method=" + request.methodString() +
                " path=" + request.path() +
                " range=" + request.getHeader("Range") +
                " user_agent=" + request.getHeader("User-Agent") << '\n';
        }
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
        if (request.method() == HttpRequest::kGet) {
            startStreamingPlayback(request, response, config, &workers, connection, deferred);
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
