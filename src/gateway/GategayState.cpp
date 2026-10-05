#include "gateway/GatewayState.hpp"
#include "utils/Util.hpp"
#include "utils/AsyncLogger.hpp"


#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <leveldb/db.h>
#include <leveldb/iterator.h>
#include <leveldb/options.h>
#include <leveldb/write_batch.h>
#include <memory>
#include <mutex>
#include <ratio>
#include <set>
#include <sstream>
#include <streambuf>
#include <string>
#include <utility>
#include <vector>
#include <cstdlib>
#include <unordered_map>


namespace miniKV {
namespace gateway {

namespace {
int64_t uploadLeaseTtlSeconds()
{
    constexpr int64_t kDefaultSeconds = 24 * 60 * 60;
    constexpr int64_t kMaxSeconds = 7 * 24 * 60 * 60;
    const char* value = std::getenv("MINIKV_UPLOAD_LEASE_TTL_SECONDS");
    if(value == nullptr || *value == 0) return kDefaultSeconds;
    try {
        const int64_t parsed = std::stoll(value);
        return std::clamp(parsed, int64_t{60}, kMaxSeconds);
    } catch(...) {
        return kDefaultSeconds;
    }
}

bool legacyAiOutboxEnabled()
{
    const char* value = std::getenv("MINIKV_GATEWAY_AI_OUTBOX_ENABLED");
    return value != nullptr && (std::string(value) == "1" || std::string(value) == "true" ||
                                std::string(value) == "on");
}
}  // namespace

GatewayLockMode GatewayLockManager::modeFromEnvironment()
{
    const char* value = std::getenv("MINIKV_GATEWAY_LOCK_MODE");
    if (value != nullptr && std::string(value) == "sharded") return GatewayLockMode::kSharded;
    return GatewayLockMode::kGlobal;
}

const char* GatewayLockManager::modeName(GatewayLockMode mode) noexcept
{
    return mode == GatewayLockMode::kSharded ? "sharded" : "global";
}

size_t GatewayLockManager::sessionShard(const std::string& sessionId) const noexcept
{
    return std::hash<std::string>{}(sessionId) % kShardCount;
}

size_t GatewayLockManager::objectShard(const std::string& canonicalKey) const noexcept
{
    return std::hash<std::string>{}(canonicalKey) % kShardCount;
}

namespace {
struct GatewayMutexAggregate {
    uint64_t count = 0;
    uint64_t totalWaitUs = 0;
    uint64_t totalHoldUs = 0;
    uint64_t maxWaitUs = 0;
    uint64_t maxHoldUs = 0;
    uint64_t totalDbUs = 0;
    uint64_t maxDbUs = 0;
    std::vector<uint64_t> waits;
    std::vector<uint64_t> holds;
    std::vector<uint64_t> dbWrites;
};
std::mutex gatewayMutexMetricsMutex;
std::unordered_map<std::string, GatewayMutexAggregate> gatewayMutexMetrics;
std::chrono::steady_clock::time_point gatewayMutexFirstSample;
std::vector<std::pair<uint64_t, uint64_t>> gatewayMutexIntervals;

uint64_t percentile(std::vector<uint64_t> values, double p)
{
    if(values.empty()) return 0;
    const size_t index = static_cast<size_t>(p * static_cast<double>(values.size() - 1));
    std::nth_element(values.begin(), values.begin() + index, values.end());
    return values[index];
}
}

void recordGatewayMutexSample(const char* label, uint64_t waitUs, uint64_t holdUs) {
    if(std::getenv("MINIKV_GATEWAY_MUTEX_DIAGNOSTICS") == nullptr) return;
    std::lock_guard<std::mutex> lock(gatewayMutexMetricsMutex);
    auto& metric = gatewayMutexMetrics[label == nullptr ? "unknown" : label];
    if(gatewayMutexFirstSample == std::chrono::steady_clock::time_point{}) {
        gatewayMutexFirstSample = std::chrono::steady_clock::now();
    }
    ++metric.count;
    metric.totalWaitUs += waitUs;
    metric.totalHoldUs += holdUs;
    metric.maxWaitUs = std::max(metric.maxWaitUs, waitUs);
    metric.maxHoldUs = std::max(metric.maxHoldUs, holdUs);
    // Diagnostics are explicitly opt-in and only run during bounded benchmark
    // cases, so retaining samples gives useful P50/P95/P99 values without
    // affecting the normal storage path.
    metric.waits.push_back(waitUs);
    metric.holds.push_back(holdUs);
    if(metric.count % 1000 == 0) {
        miniKV::utils::logInfo("event=gateway_mutex_sample label=" +
            std::string(label == nullptr ? "unknown" : label) +
            " acquire_count=" + std::to_string(metric.count) +
            " total_hold_us=" + std::to_string(metric.totalHoldUs) +
            " max_hold_us=" + std::to_string(metric.maxHoldUs));
    }
}

void recordGatewayMutexInterval(std::chrono::steady_clock::time_point start,
                                std::chrono::steady_clock::time_point end) {
    if(std::getenv("MINIKV_GATEWAY_MUTEX_DIAGNOSTICS") == nullptr) return;
    const uint64_t startNs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        start.time_since_epoch()).count());
    const uint64_t endNs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        end.time_since_epoch()).count());
    if(endNs <= startNs) return;
    std::lock_guard<std::mutex> lock(gatewayMutexMetricsMutex);
    gatewayMutexIntervals.emplace_back(startNs, endNs);
}

void recordGatewayLevelDbSample(const char* label, uint64_t durationUs) {
    if(std::getenv("MINIKV_GATEWAY_MUTEX_DIAGNOSTICS") == nullptr) return;
    std::lock_guard<std::mutex> lock(gatewayMutexMetricsMutex);
    auto& metric = gatewayMutexMetrics[label == nullptr ? "unknown" : label];
    metric.totalDbUs += durationUs;
    metric.maxDbUs = std::max(metric.maxDbUs, durationUs);
    metric.dbWrites.push_back(durationUs);
}

void resetGatewayMutexDiagnostics() {
    if(std::getenv("MINIKV_GATEWAY_MUTEX_DIAGNOSTICS") == nullptr) return;
    std::lock_guard<std::mutex> lock(gatewayMutexMetricsMutex);
    gatewayMutexMetrics.clear();
    gatewayMutexIntervals.clear();
    gatewayMutexFirstSample = std::chrono::steady_clock::now();
    miniKV::utils::logInfo("event=gateway_mutex_diagnostics_reset");
}

void flushGatewayMutexDiagnostics() {
    if(std::getenv("MINIKV_GATEWAY_MUTEX_DIAGNOSTICS") == nullptr) return;
    std::lock_guard<std::mutex> lock(gatewayMutexMetricsMutex);
    const auto now = std::chrono::steady_clock::now();
    const uint64_t windowUs = gatewayMutexFirstSample == std::chrono::steady_clock::time_point{}
        ? 0 : static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            now - gatewayMutexFirstSample).count());
    uint64_t totalHoldUs = 0;
    uint64_t totalCount = 0;
    for(const auto& [label, metric] : gatewayMutexMetrics) {
        totalHoldUs += metric.totalHoldUs;
        totalCount += metric.count;
        miniKV::utils::logInfo("event=gateway_mutex_summary label=" + label +
            " acquire_count=" + std::to_string(metric.count) +
            " mutex_wait_p50_us=" + std::to_string(percentile(metric.waits, 0.50)) +
            " mutex_wait_p95_us=" + std::to_string(percentile(metric.waits, 0.95)) +
            " mutex_wait_p99_us=" + std::to_string(percentile(metric.waits, 0.99)) +
            " mutex_wait_p999_us=" + std::to_string(percentile(metric.waits, 0.999)) +
            " mutex_wait_max_us=" + std::to_string(metric.maxWaitUs) +
            " lock_hold_p50_us=" + std::to_string(percentile(metric.holds, 0.50)) +
            " lock_hold_p95_us=" + std::to_string(percentile(metric.holds, 0.95)) +
            " lock_hold_p99_us=" + std::to_string(percentile(metric.holds, 0.99)) +
            " lock_hold_p999_us=" + std::to_string(percentile(metric.holds, 0.999)) +
            " lock_hold_max_us=" + std::to_string(metric.maxHoldUs) +
            " sum_lock_hold_us=" + std::to_string(metric.totalHoldUs) +
            " leveldb_write_p50_us=" + std::to_string(percentile(metric.dbWrites, 0.50)) +
            " leveldb_write_p95_us=" + std::to_string(percentile(metric.dbWrites, 0.95)) +
            " leveldb_write_p99_us=" + std::to_string(percentile(metric.dbWrites, 0.99)) +
            " leveldb_write_p999_us=" + std::to_string(percentile(metric.dbWrites, 0.999)) +
            " leveldb_write_max_us=" + std::to_string(metric.maxDbUs) +
            " window_us=" + std::to_string(windowUs));
    }
    // Sum of holds is useful for per-shard utilization but may exceed wall
    // time when independent shards are held concurrently.  The union of
    // intervals is the process-wide occupancy and is therefore bounded by
    // 100%, matching the definition used for the global mutex baseline.
    uint64_t busyUs = 0;
    if (!gatewayMutexIntervals.empty()) {
        const uint64_t windowStartNs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
            gatewayMutexFirstSample.time_since_epoch()).count());
        const uint64_t windowEndNs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count());
        std::vector<std::pair<uint64_t, uint64_t>> clipped;
        clipped.reserve(gatewayMutexIntervals.size());
        for (const auto [rawStart, rawEnd] : gatewayMutexIntervals) {
            const uint64_t start = std::max(rawStart, windowStartNs);
            const uint64_t end = std::min(rawEnd, windowEndNs);
            if (end > start) clipped.emplace_back(start, end);
        }
        std::sort(clipped.begin(), clipped.end());
        if (clipped.empty()) {
            gatewayMutexIntervals.clear();
        } else {
        uint64_t unionStart = clipped.front().first;
        uint64_t unionEnd = clipped.front().second;
        for (size_t i = 1; i < clipped.size(); ++i) {
            const auto [start, end] = clipped[i];
            if (start <= unionEnd) {
                unionEnd = std::max(unionEnd, end);
            } else {
                busyUs += (unionEnd - unionStart) / 1000ULL;
                unionStart = start;
                unionEnd = end;
            }
        }
        busyUs += (unionEnd - unionStart) / 1000ULL;
        }
    }
    const uint64_t occupancy = windowUs == 0 ? 0 : (busyUs * 1000000ULL) / windowUs;
    miniKV::utils::logInfo("event=gateway_mutex_summary_total acquire_count=" +
        std::to_string(totalCount) + " sum_lock_hold_us=" + std::to_string(totalHoldUs) +
        " busy_union_us=" + std::to_string(busyUs) +
        " window_us=" + std::to_string(windowUs) +
        " mutex_occupancy_ppm=" + std::to_string(occupancy));
}

namespace {
struct GatewayMutexDiagnosticsAtExit {
    ~GatewayMutexDiagnosticsAtExit() { flushGatewayMutexDiagnostics(); }
} gatewayMutexDiagnosticsAtExit;
}

using namespace util;

//通用工具
std::string join(const std::vector<std::string>& values, char separator) {
    std::ostringstream oss;
    for(size_t i = 0; i < values.size(); i++)
    {
        if(i != 0) oss << separator;
        oss << values[i];
    }
    return oss.str();
}

bool hasCapability(const NodeRecord& node, const std::string& wanted)
{   
    //节点是否有运行存储的能力
    return std::find(node.capabilities.begin(), node.capabilities.end(), wanted) != node.capabilities.end();
}

bool lowerHexDigest(const std::string& value, size_t expectedLength)
{
    if(value.size() != expectedLength) return false;
    return std::all_of(value.begin(), value.end(), [](unsigned char byte) {
        return (byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'f');
    });
}

std::string routeRequestKey(const std::string& sessionId,
                            const ChunkRouteRequest& request)
{
    return sessionId + '\n' + std::to_string(request.chunkIndex) + '\n' +
           request.chunkHash + '\n' + std::to_string(request.chunkSize) + '\n' +
           request.identityScheme + '\n' + request.checksumType + '\n' +
           request.checksumDigest;
}

std::string catalogPathKey(const std::string& ownerId, const std::string& path)
{
    return ownerId + '\n' + path;
}

std::string thumbnailKey(const std::string& sourceFileHash, const std::string& profile)
{
    return "d:" + sourceFileHash + ":thumbnail:" + profile;
}

bool isClaimableMediaJob(const media::MediaJob& job, int64_t now)
{
    // Redis may already contain the job while its next dispatch scan is deferred.
    // Pending jobs are therefore claimable immediately; retry backoff applies only
    // after a worker has explicitly failed the job.
    if (job.state == media::JobState::kPending) return true;
    if (job.state == media::JobState::kFailed) {
        return job.nextRetryAt <= now;
    }
    return job.state == media::JobState::kRunning && job.leaseUntil <= now;
}

bool normalizeDirectoryPath(const std::string& input, std::string& out)
{
    if (input.empty() || input.front() != '/' || input.find('\0') != std::string::npos) return false;
    std::vector<std::string> components;
    for (const auto& component : split(input, '/')) {
        if (component.empty()) continue;
        if (component == "." || component == "..") return false;
        components.push_back(component);
    }
    out = "/";
    for (size_t i = 0; i < components.size(); ++i) {
        if (i != 0) out += '/';
        out += components[i];
    }
    return true;
}

bool normalizeEntryName(const std::string& input, std::string& out)
{
    if (input.empty() || input == "." || input == ".." ||
        input.find('/') != std::string::npos || input.find('\0') != std::string::npos) return false;
    out = input;
    return true;
}

std::string childPath(const std::string& parentPath, const std::string& name)
{
    return parentPath == "/" ? "/" + name : parentPath + "/" + name;
}

std::vector<Breadcrumb> breadcrumbsFor(const std::string& path)
{
    std::vector<Breadcrumb> result;
    result.push_back({"/", "/"});
    if (path == "/") return result;
    std::string current;
    for (const auto& component : split(path.substr(1), '/')) {
        if (component.empty()) continue;
        current += "/" + component;
        result.push_back({component, current});
    }
    return result;
}

std::string directoryValue(const DirectoryMeta& directory)
{
    return hexEncode(directory.ownerId) + "|" + hexEncode(directory.path) + "|" +
           std::to_string(directory.createdAt);
}

bool parseDirectory(const std::string& value, DirectoryMeta& directory)
{
    const auto fields = split(value, '|');
    if (fields.size() != 3) return false;
    try {
        return hexDecode(fields[0], directory.ownerId) && hexDecode(fields[1], directory.path) &&
               ((directory.createdAt = std::stoll(fields[2])), true);
    } catch (...) {
        return false;
    }
}

std::string objectValue(const ObjectMeta& object)
{
    return hexEncode(object.objectId) + "|" + hexEncode(object.ownerId) + "|" +
           hexEncode(object.parentPath) + "|" + hexEncode(object.name) + "|" +
           hexEncode(object.fileHash) + "|" + std::to_string(object.fileSize) + "|" +
           hexEncode(object.contentType) + "|" + std::to_string(static_cast<int>(object.state)) +
           "|" + std::to_string(object.createdAt) + "|" +
           std::to_string(object.objectVersion) + "|" + std::to_string(object.metadataVersion);
}

bool parseObject(const std::string& value, ObjectMeta& object)
{
    const auto fields = split(value, '|');
    if (fields.size() != 9 && fields.size() != 11) return false;
    try {
        if (!hexDecode(fields[0], object.objectId) || !hexDecode(fields[1], object.ownerId) ||
            !hexDecode(fields[2], object.parentPath) || !hexDecode(fields[3], object.name) ||
            !hexDecode(fields[4], object.fileHash) || !hexDecode(fields[6], object.contentType)) {
            return false;
        }
        object.fileSize = std::stoull(fields[5]);
        object.state = static_cast<FileState>(std::stoi(fields[7]));
        object.createdAt = std::stoll(fields[8]);
        object.objectVersion = fields.size() == 11 ? std::stoull(fields[9]) : 1;
        object.metadataVersion = fields.size() == 11 ? std::stoull(fields[10]) : 1;
        return true;
    } catch (...) {
        return false;
    }
}

size_t estimatedObjectBytes(const ObjectMeta& object)
{
    return sizeof(ObjectMeta) + object.objectId.size() + object.ownerId.size() +
           object.parentPath.size() + object.name.size() + object.fileHash.size() +
           object.contentType.size();
}

constexpr size_t kObjectCacheMaxEntries = 4096;
constexpr size_t kObjectCacheMaxBytes = 4 * 1024 * 1024;
constexpr int64_t kObjectCacheTtlSeconds = 10 * 60;
constexpr size_t kFileCacheMaxEntries = 2048;
constexpr size_t kFileCacheMaxBytes = 8 * 1024 * 1024;
constexpr int64_t kFileCacheTtlSeconds = 10 * 60;
constexpr size_t kRouteCacheMaxEntries = 8192;
constexpr size_t kRouteCacheMaxBytes = 8 * 1024 * 1024;
constexpr int64_t kRouteCacheTtlSeconds = 10 * 60;
constexpr size_t kCatalogCacheMaxEntries = 512;
constexpr size_t kCatalogCacheMaxBytes = 8 * 1024 * 1024;
constexpr int64_t kCatalogCacheTtlSeconds = 30;
constexpr size_t kManifestCacheMaxEntries = 256;
constexpr size_t kManifestCacheMaxBytes = 16 * 1024 * 1024;
constexpr int64_t kManifestCacheTtlSeconds = 60;

size_t estimatedFileBytes(const FileMeta& file)
{
    size_t bytes = sizeof(FileMeta) + file.objectId.size() + file.fileHash.size() +
                   file.ownerId.size() + file.fileName.size() + file.dirPath.size();
    for (const auto& hash : file.chunkHashes) bytes += hash.size();
    return bytes;
}

size_t estimatedRouteBytes(const ChunkRoute& route)
{
    size_t bytes = sizeof(ChunkRoute) + route.chunkHash.size() + route.chunkId.size() +
                   route.identityScheme.size() + route.checksumType.size() +
                   route.checksumDigest.size();
    for (const auto& replica : route.replicas) bytes += replica.size();
    return bytes;
}

size_t estimatedCatalogBytes(const CatalogSnapshot& catalog)
{
    size_t bytes = sizeof(CatalogSnapshot) + catalog.path.size();
    for (const auto& breadcrumb : catalog.breadcrumbs) bytes += sizeof(Breadcrumb) + breadcrumb.name.size() + breadcrumb.path.size();
    for (const auto& directory : catalog.directories) bytes += sizeof(DirectoryMeta) + directory.ownerId.size() + directory.path.size();
    for (const auto& object : catalog.files) bytes += estimatedObjectBytes(object);
    const auto addDerivedBytes = [&bytes](const auto& records) {
        for (const auto& [fileHash, record] : records) {
            bytes += fileHash.size() + sizeof(media::ThumbnailMeta) + record.profile.size() +
                     record.derivedObjectId.size() + record.derivedFileHash.size() +
                     record.jobId.size() + record.lastError.size();
        }
    };
    addDerivedBytes(catalog.thumbnailsByFileHash);
    addDerivedBytes(catalog.previewsByFileHash);
    return bytes;
}

size_t estimatedManifestBytes(const ManifestSnapshot& manifest)
{
    size_t bytes = sizeof(ManifestSnapshot) + estimatedFileBytes(manifest.file);
    for (const auto& route : manifest.routes) bytes += estimatedRouteBytes(route);
    for (const auto& [nodeId, node] : manifest.nodes) {
        bytes += nodeId.size() + sizeof(NodeRecord) + node.nodeId.size() + node.address.size();
        for (const auto& capability : node.capabilities) bytes += capability.size();
    }
    return bytes;
}

std::string deleteTaskValue(const DeleteTask& task)
{
    std::vector<std::string> nodes;
    for (const auto& nodeId : task.pendingNodeIds) nodes.push_back(hexEncode(nodeId));
    return hexEncode(task.chunkHash) + "|" + std::to_string(task.createdAt) + "|" +
           std::to_string(task.updatedAt) + "|" + join(nodes, ',') + "|" +
           hexEncode(task.storageIdentity);
}

bool parseDeleteTask(const std::string& value, DeleteTask& task)
{
    const auto fields = split(value, '|');
    if (fields.size() != 4 && fields.size() != 5) return false;
    try {
        if (!hexDecode(fields[0], task.chunkHash)) return false;
        if (fields.size() == 5) {
            if (!hexDecode(fields[4], task.storageIdentity)) return false;
        } else {
            task.storageIdentity = task.chunkHash;
        }
        task.createdAt = std::stoll(fields[1]);
        task.updatedAt = std::stoll(fields[2]);
        for (const auto& node : split(fields[3], ',')) {
            if (node.empty()) continue;
            std::string nodeId;
            if (!hexDecode(node, nodeId) || nodeId.empty()) return false;
            task.pendingNodeIds.push_back(std::move(nodeId));
        }
        std::sort(task.pendingNodeIds.begin(), task.pendingNodeIds.end());
        task.pendingNodeIds.erase(std::unique(task.pendingNodeIds.begin(), task.pendingNodeIds.end()),
                                  task.pendingNodeIds.end());
        return !task.chunkHash.empty() && !task.storageIdentity.empty() &&
               !task.pendingNodeIds.empty();
    } catch (...) {
        return false;
    }
}


/*
struct NodeRecord { //节点静态数据
    std::string nodeId;
    std::string address;
    uint16_t httpPort = 9002;
    uint64_t maxStorageBytes = 0;
    uint64_t reservedBytes = 0;
    uint32_t maxConcurrentWrites = 2;
    std::vector<std::string> capabilities;
};

*/
std::string nodeValue(const NodeRecord& node) {
    std::vector<std::string> capabilities;
    for (const auto& capability : node.capabilities) capabilities.push_back(hexEncode(capability));
    return hexEncode(node.nodeId) + "|" + hexEncode(node.address) + "|" + std::to_string(node.httpPort) + "|" +
           std::to_string(node.maxStorageBytes) + "|" + std::to_string(node.reservedBytes) + "|" +
           std::to_string(node.maxConcurrentWrites) + "|" + join(capabilities, ',');
}


bool parseNode(const std::string& value, NodeRecord& node)
{
    const auto f = split(value, '|');
    
    if(f.size() != 7) return false;
    try{
        if(!hexDecode(f[0], node.nodeId) || !hexDecode(f[1], node.address)) return false;
        node.httpPort = static_cast<uint16_t>(std::stoul(f[2]));
        node.maxStorageBytes = std::stoull(f[3]);
        node.reservedBytes = std::stoull(f[4]);
        node.maxConcurrentWrites = static_cast<uint32_t>(std::stoul(f[5]));
        for(const auto& item : split(f[6], ','))
        {
            if(!item.empty())
            {
                std::string decoded;
                if(!hexDecode(item, decoded)) return false;
                node.capabilities.push_back(decoded);
            }
        }
        return true;
    } catch(...) { return false; }
}

/*
struct ChunkRoute { //每个chunk的真实副本管理，写入真实的数据
    std::string chunkHash;
    uint64_t size = 0;
    uint32_t desiredReplicas = 2; //期望副本数
    std::vector<std::string> replicas;
    int64_t  updateAt = 0;
};
对于sha256之后容易是有这些字符与|冲突的，所以都要转换成这个16进制
*/
std::string routeValue(const ChunkRoute& route)
{
    std::vector<std::string> replicas;
    
    for(const auto& node : route.replicas)
    {
        replicas.push_back(hexEncode(node));
    }

    return hexEncode(route.chunkHash) + '|' + std::to_string(route.size)
            + '|' + std::to_string(route.desiredReplicas) + '|' +  std::to_string(route.updateAt) + '|'
            + join(replicas, ',') + '|' + hexEncode(route.identityScheme) + '|' +
            hexEncode(route.chunkId) + '|' + hexEncode(route.checksumType) + '|' +
            hexEncode(route.checksumDigest) + '|' + std::to_string(route.objectVersion) + '|' +
            std::to_string(route.generation);
}

bool parseRoute(const std::string& value, ChunkRoute& route)
{
    const auto f = split(value, '|');
    if(f.size() != 5 && f.size() != 11) return false;
    try{
        route = {};
        if(!hexDecode(f[0], route.chunkHash)) return false;
        route.size = std::stoull(f[1]);
        route.desiredReplicas = static_cast<uint32_t>(std::stoul(f[2]));
        route.updateAt = std::stoll(f[3]);
        for(const auto& item : split(f[4], ','))
        {
            if(!item.empty())
            {
                std::string node;
                if(!hexDecode(item, node)) return false;
                route.replicas.push_back(node);
            }
        }
        if(f.size() == 11) {
            if(!hexDecode(f[5], route.identityScheme) ||
               !hexDecode(f[6], route.chunkId) ||
               !hexDecode(f[7], route.checksumType) ||
               !hexDecode(f[8], route.checksumDigest)) return false;
            route.objectVersion = std::stoull(f[9]);
            route.generation = std::stoull(f[10]);
        } else {
            route.identityScheme = "cas-sha256";
            route.chunkId = route.chunkHash;
            route.checksumType = "sha256";
            route.checksumDigest = route.chunkHash;
            route.objectVersion = 1;
        }
        return true;
    }catch(...) {
        return false;
    }
}

/*
struct FileMeta {
    std::string fileHash;
    std::string ownerId = "admin";
    std::string fileName;
    std::string dirPath;
    uint64_t fileSize = 0;
    uint32_t chunkSize = 0;
    std::vector<std::string> chunkHashes;
    FileState state = FileState::kProtecting;
    int64_t createAt = 0;
};
*/
std::string fileValue(const FileMeta& file)
{
    std::vector<std::string> hashes;
    for(const auto & hash : file.chunkHashes)
    {
        hashes.push_back(hexEncode(hash));
    }
    return   hexEncode(file.fileHash) + "|" + hexEncode(file.ownerId) + "|" + hexEncode(file.fileName) + "|" + hexEncode(file.dirPath) + "|" +
           std::to_string(file.fileSize) + "|" + std::to_string(file.chunkSize) + "|" + std::to_string(static_cast<int>(file.state)) + "|" + std::to_string(file.createdAt) + "|" + join(hashes, ',') + "|" +
           std::to_string(file.objectVersion) + "|" + std::to_string(file.metadataVersion);

}


bool parseFile(const std::string& value, FileMeta& file) {
    const auto f = split(value, '|'); if (f.size() != 9 && f.size() != 11) return false;
    try {
        if (!hexDecode(f[0], file.fileHash) || !hexDecode(f[1], file.ownerId) || !hexDecode(f[2], file.fileName) || !hexDecode(f[3], file.dirPath)) return false;
        file.fileSize = std::stoull(f[4]); file.chunkSize = static_cast<uint32_t>(std::stoul(f[5])); file.state = static_cast<FileState>(std::stoi(f[6])); file.createdAt = std::stoll(f[7]);
        for (const auto& item : split(f[8], ',')) { if (!item.empty()) { std::string hash; if (!hexDecode(item, hash)) return false; file.chunkHashes.push_back(hash); } }
        file.objectVersion = f.size() == 11 ? std::stoull(f[9]) : 1;
        file.metadataVersion = f.size() == 11 ? std::stoull(f[10]) : 1;
        return true;
    } catch (...) { return false; }
}


/*
struct SessionState {
    std::string sessionId;
    std::string ownerId = "admin";
    std::string fileName;
    std::string dirPath;
    uint64_t fileSize  = 0;
    uint32_t chunkSize = 4 * 1024 * 1024;
    uint32_t totalChunks = 0;
    std::map<uint32_t, CompletedChunk>   completed;
    int64_t createdAt = 0;
    int64_t lastActivityAt = 0;
};
*/

std::string sessionValue(const SessionState& session) {
    std::vector<std::string> chunks;
    for (const auto& [index, chunk] : session.completed) chunks.push_back(std::to_string(index) + "," + hexEncode(chunk.chunkHash) + "," + std::to_string(chunk.size));
    return hexEncode(session.sessionId) + "|" + hexEncode(session.ownerId) + "|" + hexEncode(session.fileName) + "|" +
           hexEncode(session.dirPath) + "|" + std::to_string(session.fileSize) + "|" + std::to_string(session.chunkSize) + "|" +
           std::to_string(session.totalChunks) + "|" + std::to_string(session.createdAt) + "|" + std::to_string(session.lastActivityAt) + "|" + join(chunks, ';') + "|" +
           hexEncode(session.manifestHash) + "|" + hexEncode(session.derivedJobId) + "|" +
           hexEncode(session.derivedProfile) + "|" + hexEncode(session.objectId) + "|" +
           std::to_string(session.objectVersion) + "|" +
           std::to_string(session.metadataVersion);
}
bool parseSession(const std::string& value, SessionState& session) {
    const auto f = split(value, '|'); 
    if (f.size() != 10 && f.size() != 11 && f.size() != 13 && f.size() != 16) return false;
    try {
        if (!hexDecode(f[0], session.sessionId) || !hexDecode(f[1], session.ownerId) || !hexDecode(f[2], session.fileName) || !hexDecode(f[3], session.dirPath)) return false;
        session.fileSize = std::stoull(f[4]); session.chunkSize = static_cast<uint32_t>(std::stoul(f[5])); session.totalChunks = static_cast<uint32_t>(std::stoul(f[6]));
        session.createdAt = std::stoll(f[7]); session.lastActivityAt = std::stoll(f[8]);
        for (const auto& item : split(f[9], ';')) { if (item.empty()) continue; const auto chunk = split(item, ','); if (chunk.size() != 3) return false; CompletedChunk completed; completed.index = static_cast<uint32_t>(std::stoul(chunk[0])); if (!hexDecode(chunk[1], completed.chunkHash)) return false; completed.size = std::stoull(chunk[2]); session.completed[completed.index] = completed; }
        if (f.size() >= 11 && !hexDecode(f[10], session.manifestHash)) return false;
        if (f.size() == 13 && (!hexDecode(f[11], session.derivedJobId) ||
                               !hexDecode(f[12], session.derivedProfile))) return false;
        if (f.size() == 16) {
            if (!hexDecode(f[11], session.derivedJobId) ||
                !hexDecode(f[12], session.derivedProfile) ||
                !hexDecode(f[13], session.objectId)) return false;
            session.objectVersion = std::stoull(f[14]);
            session.metadataVersion = std::stoull(f[15]);
            if (session.objectId.empty() || session.objectVersion == 0 ||
                session.metadataVersion == 0) return false;
        }
        return true;
    } catch (...) { return false; }
}

bool GatewayState::persistSessionLocked(const SessionState& session)
{
    return db_->Put(leveldb::WriteOptions(), "s:" + session.sessionId, sessionValue(session)).ok();
}
bool GatewayState::persistRouteLocked(const ChunkRoute& route)
{
    return db_->Put(leveldb::WriteOptions(), "c:" + route.chunkHash, routeValue(route)).ok();
}
bool GatewayState::persistDirectoryLocked(const DirectoryMeta& directory)
{
    return db_->Put(leveldb::WriteOptions(),
                    "dir:" + catalogPathKey(directory.ownerId, directory.path),
                    directoryValue(directory)).ok();
}
bool GatewayState::getFileLocked(const std::string& fileHash, FileMeta& out) const
{
    if (const auto cached = fileCache_.get(fileHash)) {
        out = *cached;
        return true;
    }
    if (!db_) return false;
    std::string value;
    if (!db_->Get(leveldb::ReadOptions(), "f:" + fileHash, &value).ok()) return false;
    FileMeta file;
    if (!parseFile(value, file) || file.fileHash != fileHash) return false;
    fileCache_.put(fileHash, std::make_shared<const FileMeta>(file),
                   estimatedFileBytes(file), kFileCacheTtlSeconds);
    out = std::move(file);
    return true;
}

bool GatewayState::getRouteLocked(const std::string& chunkHash, ChunkRoute& out) const
{
    if (const auto cached = routeCache_.get(chunkHash)) {
        out = *cached;
        return true;
    }
    if (!db_) return false;
    std::string value;
    if (!db_->Get(leveldb::ReadOptions(), "c:" + chunkHash, &value).ok()) return false;
    ChunkRoute route;
    if (!parseRoute(value, route) || route.chunkHash != chunkHash) return false;
    routeCache_.put(chunkHash, std::make_shared<const ChunkRoute>(route),
                    estimatedRouteBytes(route), kRouteCacheTtlSeconds);
    out = std::move(route);
    return true;
}

bool GatewayState::getDirectoryLocked(const std::string& ownerId, const std::string& path,
                                      DirectoryMeta& out) const
{
    if (!db_) return false;
    std::string value;
    if (!db_->Get(leveldb::ReadOptions(), "dir:" + catalogPathKey(ownerId, path), &value).ok()) return false;
    return parseDirectory(value, out) && out.ownerId == ownerId && out.path == path;
}

bool GatewayState::getObjectLocked(const std::string& objectId, ObjectMeta& out) const
{
    if (const auto cached = objectCache_.get(objectId)) {
        out = *cached;
        return true;
    }
    if (!db_) return false;
    std::string value;
    if (!db_->Get(leveldb::ReadOptions(), "obj:" + objectId, &value).ok()) return false;

    ObjectMeta object;
    if (!parseObject(value, object) || object.objectId != objectId) return false;
    objectCache_.put(objectId, std::make_shared<const ObjectMeta>(object),
                     estimatedObjectBytes(object), kObjectCacheTtlSeconds);
    out = std::move(object);
    return true;
}

bool GatewayState::getMediaJobLocked(const std::string& jobId, media::MediaJob& out) const
{
    if (!db_ || jobId.empty()) return false;
    std::string value;
    if (!db_->Get(leveldb::ReadOptions(), "j:" + jobId, &value).ok()) return false;
    media::MediaJob job;
    if (!media::parseMediaJob(value, job) || job.jobId != jobId) return false;
    out = std::move(job);
    return true;
}

bool GatewayState::getThumbnailLocked(const std::string& sourceFileHash, const std::string& profile,
                                      media::ThumbnailMeta& out) const
{
    if (!db_ || sourceFileHash.empty() || profile.empty()) return false;
    std::string value;
    if (!db_->Get(leveldb::ReadOptions(), thumbnailKey(sourceFileHash, profile), &value).ok()) return false;
    media::ThumbnailMeta thumbnail;
    if (!media::parseThumbnailMeta(value, thumbnail) ||
        thumbnail.sourceFileHash != sourceFileHash || thumbnail.profile != profile) {
        return false;
    }
    out = std::move(thumbnail);
    return true;
}

bool GatewayState::objectIdAtPathLocked(const std::string& pathKey, std::string& objectId) const
{
    if (!db_) return false;
    return db_->Get(leveldb::ReadOptions(), "path:" + pathKey, &objectId).ok() && !objectId.empty();
}

bool GatewayState::loadDeleteTasksLocked()
{
    auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (it->Seek("del:"); it->Valid() && it->key().ToString().rfind("del:", 0) == 0; it->Next()) {
        DeleteTask task;
        if (!parseDeleteTask(it->value().ToString(), task)) return false;
        if (it->key().ToString() != "del:" + task.chunkHash) return false;
        deleteTasks_[task.chunkHash] = std::move(task);
    }
    return it->status().ok();
}
bool GatewayState::loadSessionsLocked()
{
    auto it =  std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));

    for(it->Seek("s:"); it->Valid() && it->key().ToString().rfind("s:", 0) == 0; it->Next())
    {
        SessionState session;
        if(parseSession(it->value().ToString(), session))
        {
            sessions_[session.sessionId] = session;
        }
    }
    return it->status().ok();
}
bool GatewayState::loadNodesLocked()
{
    auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));

    for(it->Seek("n:"); it->Valid() && it->key().ToString().rfind("n:", 0) == 0; it->Next())
    {
        NodeRecord node;
        if(parseNode(it->value().ToString(), node))
        {
            nodeRecords_[node.nodeId] = node;
            nodeRuntime_[node.nodeId].state = NodeLiveState::kOffline;
        }
    }
    return it->status().ok();
}
bool GatewayState::backfillLegacyCatalogLocked()
{
    leveldb::WriteBatch batch;
    bool changed = false;
    const int64_t now = unixSeconds();
    auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (it->Seek("f:"); it->Valid() && it->key().ToString().rfind("f:", 0) == 0; it->Next()) {
        FileMeta file;
        if (!parseFile(it->value().ToString(), file)) return false;
        const std::string fileHash = file.fileHash;
        std::string parent;
        std::string name;
        if (!normalizeDirectoryPath(file.dirPath, parent) || !normalizeEntryName(file.fileName, name)) continue;

        std::string current = "/";
        for (const auto& component : split(parent.substr(1), '/')) {
            if (component.empty()) continue;
            current = childPath(current, component);
            const std::string key = catalogPathKey(file.ownerId, current);
            std::string existing;
            const leveldb::Status status = db_->Get(leveldb::ReadOptions(), "dir:" + key, &existing);
            if (!status.ok() && !status.IsNotFound()) return false;
            if (status.IsNotFound()) {
                DirectoryMeta directory{file.ownerId, current, now};
                batch.Put("dir:" + key, directoryValue(directory));
                changed = true;
            }
        }

        const std::string pathKey = catalogPathKey(file.ownerId, childPath(parent, name));
        std::string existing;
        const leveldb::Status status = db_->Get(leveldb::ReadOptions(), "path:" + pathKey, &existing);
        if (!status.ok() && !status.IsNotFound()) return false;
        if (status.ok()) continue;
        ObjectMeta object;
        object.objectId = "legacy-" + fileHash;
        object.objectVersion = file.objectVersion;
        object.metadataVersion = file.metadataVersion;
        object.ownerId = file.ownerId;
        object.parentPath = parent;
        object.name = name;
        object.fileHash = fileHash;
        object.fileSize = file.fileSize;
        object.state = file.state;
        object.createdAt = file.createdAt;
        batch.Put("obj:" + object.objectId, objectValue(object));
        batch.Put("path:" + pathKey, object.objectId);
        changed = true;
    }
    return it->status().ok() && (!changed || db_->Write(leveldb::WriteOptions(), &batch).ok());
}

//调度算法
/*
struct PlacementPlan { //临时写入计划
    uint32_t chunkIndex   = 0;
    std::string leaseId   = 0;
    uint64_t routeVersion = 0; //防止冲突版本号
    int64_t  expiresAt    = 0; //过期时间
    std::vector<NodeSnapshot> chain; //节点备份
};
*/
PlacementPlan GatewayState::selectPlacementLocked(const SessionState& session, uint32_t index)
{   
    struct Candidate { NodeSnapshot node; double score; };
    const uint64_t bytes = index + 1 == session.totalChunks ? session.fileSize - static_cast<uint64_t>(index) * session.chunkSize : session.chunkSize;
    std::vector<Candidate> candidates;

    for(const auto& [id, record] : nodeRecords_)
    {
        const NodeRuntime runtime = nodeRuntime_[id];
        const uint32_t reservedWrites = reservedWritesByNode_[id];
        const uint64_t reservedBytes = reservedBytesByNode_[id];
        if(!hasCapability(record, "storage") || runtime.state != NodeLiveState::kOnline ||
           reservedWrites >= record.maxConcurrentWrites) continue;
        if (runtime.freeBytes <= reservedBytes + bytes + record.reservedBytes) continue;
        const double capacity = static_cast<double>(record.maxStorageBytes ? record.maxStorageBytes : runtime.usedBytes + runtime.freeBytes);
        const double freeRatio = capacity > 0
            ? static_cast<double>(runtime.freeBytes - reservedBytes) / capacity : 0.0;
        const double idle = (1.0 - runtime.cpuUsage) * .10 + (1.0 - runtime.memoryUsage) * .10 + (1.0 - runtime.diskIoUsage) * .15;
        const double network = 1.0 / (1.0 + runtime.netOutMbps / 100.0) * .15;
        const double connections = 1.0 / (1.0 + runtime.activeUploads + reservedWrites) * .10;
        candidates.push_back({{record, runtime}, freeRatio * .35 + idle + network + connections});
    }
    std::sort(candidates.begin(), candidates.end(), [](const Candidate& left, const Candidate& right){
        return left.score > right.score;
    });
    PlacementPlan plan;
    plan.chunkIndex = index;
    for(size_t i = 0; i < candidates.size() && i < replicationFactor_; i++)
    {
        plan.chain.push_back(candidates[i].node);
    }
    return plan;
}   

bool GatewayState::reserveLeaseLocked(const SessionState& session,
                                      const ChunkRouteRequest& request,
                                      PlacementPlan& plan,
                                      int64_t now)
{
    if(plan.chain.empty()) return false;
    const std::string randomLeaseId = randomId();
    if (randomLeaseId.empty()) return false;
    plan.leaseId = useShardedLocks()
        ? std::to_string(locks_.sessionShard(session.sessionId)) + ":" + randomLeaseId
        : randomLeaseId;
    if(plan.leaseId.empty()) return false;
    plan.routeVersion = std::hash<std::string>{}(plan.leaseId);
    plan.expiresAt = now + 120;
    plan.identityScheme = request.identityScheme;
    plan.checksumType = request.checksumType;
    plan.checksumDigest = request.checksumDigest;
    plan.objectVersion = session.objectVersion;
    if(request.identityScheme == "opaque-chunk-id") {
        const std::string seed = session.sessionId + "\n" +
            std::to_string(request.chunkIndex) + "\n" + request.chunkHash;
        plan.chunkId = "chk-" + sha256Hex(seed.data(), seed.size()).substr(0, 40);
    } else {
        plan.chunkId = request.chunkHash;
    }

    WriteLease lease;
    lease.leaseId = plan.leaseId;
    lease.requestKey = routeRequestKey(session.sessionId, request);
    lease.sessionId = session.sessionId;
    lease.chunkIndex = request.chunkIndex;
    lease.chunkHash = request.chunkHash;
    lease.chunkSize = request.chunkSize;
    lease.expiresAt = plan.expiresAt;
    lease.plan = plan;
    lease.reservation.tokenId = lease.leaseId;
    lease.reservation.bytes = request.chunkSize;

    for(const auto& node : plan.chain) {
        ++reservedWritesByNode_[node.record.nodeId];
        reservedBytesByNode_[node.record.nodeId] += request.chunkSize;
        lease.reservation.nodeIds.push_back(node.record.nodeId);
    }
    if (useShardedLocks()) {
        const size_t shard = locks_.sessionShard(session.sessionId);
        shardedLeaseByRequestKey_[shard][lease.requestKey] = lease.leaseId;
        shardedLeases_[shard][lease.leaseId] = std::move(lease);
    } else {
        leaseByRequestKey_[lease.requestKey] = lease.leaseId;
        leases_[lease.leaseId] = std::move(lease);
    }
    return true;
}

bool GatewayState::releaseLeaseLocked(const std::string& leaseId,
                                      ReservationToken::State terminalState)
{
    const size_t shard = useShardedLocks() ? leaseShard(leaseId) : kLockShardCount;
    auto* leaseMap = useShardedLocks() && shard < kLockShardCount
        ? &shardedLeases_[shard] : &leases_;
    auto* requestMap = useShardedLocks() && shard < kLockShardCount
        ? &shardedLeaseByRequestKey_[shard] : &leaseByRequestKey_;
    const auto it = leaseMap->find(leaseId);
    // Erasing the token is the idempotence boundary: a retry, timeout scan or
    // disconnect cleanup can call this again, but only the first caller may
    // decrement NodeRegistry reservation counters.
    if(it == leaseMap->end()) return false;

    auto& token = it->second.reservation;
    token.state = terminalState;
    for(const auto& nodeId : token.nodeIds) {
        auto writes = reservedWritesByNode_.find(nodeId);
        if(writes != reservedWritesByNode_.end()) {
            if(writes->second <= 1) reservedWritesByNode_.erase(writes);
            else --writes->second;
        }
        auto bytes = reservedBytesByNode_.find(nodeId);
        if(bytes != reservedBytesByNode_.end()) {
            if(bytes->second <= token.bytes) reservedBytesByNode_.erase(bytes);
            else bytes->second -= token.bytes;
        }
    }
    const auto request = requestMap->find(it->second.requestKey);
    if(request != requestMap->end() && request->second == leaseId) {
        requestMap->erase(request);
    }
    leaseMap->erase(it);
    return true;
}

void GatewayState::releaseExpiredLeasesLocked(int64_t now)
{
    std::vector<std::string> expired;
    if (useShardedLocks()) {
        for (auto& shardLeases : shardedLeases_) {
            expired.clear();
            for (const auto& [leaseId, lease] : shardLeases) {
                if (lease.expiresAt <= now) expired.push_back(leaseId);
            }
            for (const auto& leaseId : expired) releaseLeaseLocked(leaseId);
        }
        return;
    }
    for(const auto& [leaseId, lease] : leases_) {
        if(lease.expiresAt <= now) expired.push_back(leaseId);
    }
    for(const auto& leaseId : expired) releaseLeaseLocked(leaseId);
}

void GatewayState::releaseLeasesForNodeLocked(const std::string& nodeId)
{
    std::vector<std::string> affected;
    if (useShardedLocks()) {
        for (auto& shardLeases : shardedLeases_) {
            affected.clear();
            for (const auto& [leaseId, lease] : shardLeases) {
                const bool containsNode = std::any_of(lease.plan.chain.begin(), lease.plan.chain.end(),
                    [&nodeId](const NodeSnapshot& node) { return node.record.nodeId == nodeId; });
                if (containsNode) affected.push_back(leaseId);
            }
            for (const auto& leaseId : affected) releaseLeaseLocked(leaseId);
        }
        return;
    }
    for(const auto& [leaseId, lease] : leases_) {
        const bool containsNode = std::any_of(lease.plan.chain.begin(), lease.plan.chain.end(),
            [&nodeId](const NodeSnapshot& node) { return node.record.nodeId == nodeId; });
        if(containsNode) affected.push_back(leaseId);
    }
    for(const auto& leaseId : affected) releaseLeaseLocked(leaseId);
}

//
GatewayState::GatewayState(const std::string& dbPath, uint32_t replicationFactor,
                           GatewayLockMode lockMode)
    : replicationFactor_(std::max<uint32_t>(1, replicationFactor)),
      dbPath_(dbPath),
      locks_(lockMode),
      objectCache_(ObjectMetaCache::Config{kObjectCacheMaxEntries, kObjectCacheMaxBytes}),
      fileCache_(FileMetaCache::Config{kFileCacheMaxEntries, kFileCacheMaxBytes}),
      routeCache_(ChunkRouteCache::Config{kRouteCacheMaxEntries, kRouteCacheMaxBytes}),
      catalogCache_(CatalogCache::Config{kCatalogCacheMaxEntries, kCatalogCacheMaxBytes}),
      manifestCache_(ManifestCache::Config{kManifestCacheMaxEntries, kManifestCacheMaxBytes})
{
}
GatewayState::~GatewayState() = default;

void GatewayState::configureRemoteMetadata(std::shared_ptr<metadata::MetadataClient> client)
{
    std::lock_guard<std::mutex> lock(locks_.lifecycle());
    remoteMetadata_ = std::move(client);
}

std::mutex& GatewayState::sessionMutex(const std::string& sessionId) const noexcept
{
    return locks_.session(locks_.sessionShard(sessionId));
}

std::mutex& GatewayState::objectMutex(const std::string& canonicalKey) const noexcept
{
    return locks_.object(locks_.objectShard(canonicalKey));
}

size_t GatewayState::leaseShard(const std::string& leaseId) const noexcept
{
    if (leaseId.empty()) return kLockShardCount;
    const auto separator = leaseId.find(':');
    if (separator == std::string::npos || separator == 0) return kLockShardCount;
    size_t shard = 0;
    for (size_t index = 0; index < separator; ++index) {
        const char character = leaseId[index];
        if (character < '0' || character > '9') return kLockShardCount;
        shard = shard * 10 + static_cast<size_t>(character - '0');
        if (shard >= kLockShardCount) return kLockShardCount;
    }
    return shard;
}

bool GatewayState::open()
{
    if (remoteMetadata_) {
        // A raft-backed Gateway must never create or open a local metadata
        // database.  Keeping this branch before LevelDB::Open is the concrete
        // no-dual-write invariant for the frontend.
        // Metadata election may still be in progress at process startup. The
        // HTTP readiness endpoint probes it later; opening the frontend must
        // not create a local fallback database or fail permanently during the
        // election window.
        return true;
    }
    // Startup/recovery owns the lifecycle lock; business shard locks are not
    // touched until the recovered in-memory indexes are fully constructed.
    GatewayMutexGuard lock(locks_.lifecycle(), __func__);
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::DB* raw = nullptr;
    if(!leveldb::DB::Open(options, dbPath_, &raw).ok()) return false;
    db_.reset(raw);
    return loadNodesLocked() && loadSessionsLocked() && loadDeleteTasksLocked() &&
           backfillLegacyCatalogLocked();
}

//节点管理
bool GatewayState::registerNode(const NodeRecord& node)
{
    if(node.nodeId.empty() || node.address.empty() || !hasCapability(node, "storage")) return false;
    if (remoteMetadata_) {
        auto& bootId = remoteBootIds_[node.nodeId];
        if (bootId.empty()) bootId = randomId();
        metadata::MetadataCommand command;
        command.commandId = metadata::MetadataClient::newCommandId("register-" + node.nodeId);
        command.type = metadata::MetadataCommandType::kRegisterNode;
        command.actorType = "gateway"; command.actorId = node.nodeId; command.issuedAt = unixSeconds();
        metadata::RegisterNodePayload payload;
        payload.nodeId = node.nodeId; payload.bootId = bootId; payload.address = node.address;
        payload.dataPort = node.httpPort; payload.registeredCapacityBytes = node.maxStorageBytes;
        payload.capabilities = node.capabilities; command.payload = std::move(payload);
        const auto result = remoteMetadata_->propose(command);
        if (result.status != metadata::ApplyStatus::kOk && result.status != metadata::ApplyStatus::kAlreadyApplied) return false;
        remoteNodeEpochs_[node.nodeId] = result.nodeEpoch;
        return true;
    }
    if (useShardedLocks()) {
        // Persist first; the registry lock protects only the in-memory
        // ownership update and is never held across LevelDB I/O.
        if (!db_->Put(leveldb::WriteOptions(), "n:" + node.nodeId, nodeValue(node)).ok()) return false;
        std::unique_lock<std::shared_mutex> lock(locks_.nodes());
        const bool firstRegistration = nodeRecords_.find(node.nodeId) == nodeRecords_.end();
        nodeRecords_[node.nodeId] = node;
        if (firstRegistration) nodeRuntime_[node.nodeId].state = NodeLiveState::kRecovering;
        manifestCache_.clear();
        return true;
    }
    GatewayMutexGuard lock(mutex_, __func__);
    const bool firstRegistration = nodeRecords_.find(node.nodeId) == nodeRecords_.end();
    if (!db_->Put(leveldb::WriteOptions(), "n:"+node.nodeId, nodeValue(node)).ok()) return false;
    nodeRecords_[node.nodeId] = node;
    if(firstRegistration) nodeRuntime_[node.nodeId].state = NodeLiveState::kRecovering;
    manifestCache_.clear();
    return true;
}
bool GatewayState::heartbeat(const std::string& nodeId, const NodeRuntime& runtime)
{
    if (remoteMetadata_) {
        uint64_t nodeEpoch = 0;
        const auto cachedEpoch = remoteNodeEpochs_.find(nodeId);
        if (cachedEpoch != remoteNodeEpochs_.end()) {
            nodeEpoch = cachedEpoch->second;
        } else {
            // A Gateway may be restarted or may receive a heartbeat for a
            // node registered through another frontend.  Reconstruct the
            // durable epoch from Metadata instead of requiring sticky state.
            for (const auto& node : remoteMetadata_->nodes()) {
                if (node.nodeId == nodeId) { nodeEpoch = node.nodeEpoch; break; }
            }
            if (nodeEpoch == 0) {
                miniKV::utils::logWarn("event=remote_node_heartbeat_rejected reason=missing_epoch node=" + nodeId);
                return false;
            }
            remoteNodeEpochs_[nodeId] = nodeEpoch;
        }
        metadata::NodeHeartbeat heartbeat;
        heartbeat.nodeId = nodeId; heartbeat.nodeEpoch = nodeEpoch; heartbeat.freeBytes = runtime.freeBytes;
        heartbeat.activeUploads = runtime.activeUploads; heartbeat.activeDownloads = runtime.activeDownloads;
        heartbeat.queueDepth = runtime.activeUploads; heartbeat.observedAtMs = static_cast<int64_t>(unixSeconds()) * 1000;
        std::string error;
        const bool accepted = remoteMetadata_->heartbeat(heartbeat, &error);
        if(!accepted) {
            miniKV::utils::logWarn("event=remote_node_heartbeat_rejected reason=" +
                                   (error.empty() ? std::string("metadata_unavailable") : error) +
                                   " node=" + nodeId + " epoch=" + std::to_string(nodeEpoch));
        }
        return accepted;
    }
    if (useShardedLocks()) {
        std::unique_lock<std::shared_mutex> lock(locks_.nodes());
        if(!nodeRecords_.count(nodeId)) return false;
        NodeRuntime next = runtime;
        next.state = NodeLiveState::kOnline;
        next.lastHeartbeatAt = unixSeconds();
        nodeRuntime_[nodeId] = next;
        return true;
    }
    GatewayMutexGuard lock(mutex_, __func__);
    if(!nodeRecords_.count(nodeId)) return false;
    releaseExpiredLeasesLocked(unixSeconds());
    NodeRuntime next = runtime;
    next.state = NodeLiveState::kOnline;
    next.lastHeartbeatAt = unixSeconds();
    nodeRuntime_[nodeId] = next;
    return true;
}
void GatewayState::checkNodeTimeouts(int64_t now, int64_t suspectAfterSeconds, int64_t offlineAfterSeconds)
{
    if (remoteMetadata_) {
        // Session expiration is an explicit replicated command.  The
        // frontend only observes wall-clock time to decide when to propose;
        // MetadataStateMachine validates the expected expiry deterministically.
        for(const auto& session : remoteMetadata_->sessions()) {
            if(session.expired || session.expiresAt <= 0 || session.expiresAt > now) continue;
            metadata::MetadataCommand command;
            command.commandId = "expire-session-" + session.sessionId + "-" + std::to_string(session.expiresAt);
            command.type = metadata::MetadataCommandType::kExpireSession;
            command.actorType = "gateway-expiry"; command.actorId = "admin"; command.issuedAt = now;
            command.payload = metadata::ExpireSessionPayload{session.sessionId, session.expiresAt, now};
            const auto result = remoteMetadata_->propose(command);
            if(result.status != metadata::ApplyStatus::kOk && result.status != metadata::ApplyStatus::kAlreadyApplied &&
               result.status != metadata::ApplyStatus::kFenced && result.status != metadata::ApplyStatus::kNotFound) {
                miniKV::utils::logWarn("event=remote_session_expiry_failed session=" + session.sessionId +
                                       " status=" + std::string(metadata::toString(result.status)));
            }
        }
        return;
    }
    std::unique_ptr<GatewayMutexGuard> globalLock;
    // Timeout cleanup scans every session-owned lease map. Acquire all session
    // shards in ascending order before NodeRegistry, matching the hot-path
    // SessionShard -> NodeRegistry order without a process-wide lease mutex.
    std::array<std::unique_lock<std::mutex>, GatewayLockManager::kShardCount> sessionLocks;
    if (useShardedLocks()) {
        for (size_t shard = 0; shard < GatewayLockManager::kShardCount; ++shard) {
            sessionLocks[shard] = std::unique_lock<std::mutex>(locks_.session(shard));
        }
    } else {
        globalLock = std::make_unique<GatewayMutexGuard>(mutex_, __func__);
    }
    // In sharded mode nodeRuntime_ and reservation counters are owned by the
    // NodeRegistry domain. No LevelDB I/O occurs while this lock is held.
    std::unique_ptr<std::unique_lock<std::shared_mutex>> nodeLock;
    if (useShardedLocks()) {
        nodeLock = std::make_unique<std::unique_lock<std::shared_mutex>>(locks_.nodes());
    }
    releaseExpiredLeasesLocked(now);
    for(auto& [id, runtime] : nodeRuntime_)
    {
        if(runtime.lastHeartbeatAt == 0 || now - runtime.lastHeartbeatAt >= offlineAfterSeconds) 
        {
            runtime.state = NodeLiveState::kOffline;
            releaseLeasesForNodeLocked(id);
        }
        else if(now - runtime.lastHeartbeatAt >= suspectAfterSeconds)
        {
            runtime.state = NodeLiveState::kSuspect;
        }
    }
}
std::vector<NodeSnapshot> GatewayState::nodes() const
{
    if (remoteMetadata_) {
        std::vector<NodeSnapshot> out;
        for (const auto& node : remoteMetadata_->nodes()) {
            NodeRecord record; record.nodeId = node.nodeId; record.address = node.address;
            record.httpPort = node.dataPort; record.maxStorageBytes = node.registeredCapacityBytes;
            record.capabilities = node.capabilities;
            NodeRuntime runtime;
            runtime.state = node.draining ? NodeLiveState::kDraining :
                (node.health == metadata::NodeHealth::kOnline ? NodeLiveState::kOnline :
                 node.health == metadata::NodeHealth::kSuspect ? NodeLiveState::kSuspect :
                 node.health == metadata::NodeHealth::kOffline ? NodeLiveState::kOffline : NodeLiveState::kRecovering);
            out.push_back({std::move(record), runtime});
        }
        return out;
    }
    if (useShardedLocks()) {
        std::shared_lock<std::shared_mutex> lock(locks_.nodes());
        std::vector<NodeSnapshot> out;
        for(const auto& [id, record] : nodeRecords_)
        {
            const auto it = nodeRuntime_.find(id);
            out.push_back({record, it == nodeRuntime_.end() ? NodeRuntime{} : it->second});
        }
        return out;
    }
    GatewayMutexGuard lock(mutex_, __func__);
    std::vector<NodeSnapshot> out;
    for(const auto& [id, recoed] : nodeRecords_)
    {
        const auto it = nodeRuntime_.find(id);
        out.push_back({recoed, it == nodeRuntime_.end() ? NodeRuntime{} : it->second});
    }
    return out;
}

//会话管理  
std::string GatewayState::manifestHash(uint64_t fileSize, uint32_t chunkSize,
                                       const std::vector<ChunkRouteRequest>& chunks)
{
    if (fileSize == 0 || chunkSize == 0 || chunks.empty()) return {};
    std::string canonical = "minikv-manifest-v1\n" + std::to_string(fileSize) + "\n" +
                            std::to_string(chunkSize) + "\n";
    for (const auto& chunk : chunks) {
        canonical += std::to_string(chunk.chunkIndex) + ":" + chunk.chunkHash + ":" +
                     std::to_string(chunk.chunkSize) + "\n";
    }
    return sha256Hex(canonical.data(), canonical.size());
}

bool GatewayState::createSession(const std::string& fileName, const std::string& dirPath,
        uint64_t fileSize, uint32_t chunkSize, SessionState& out,
        GatewayMutationTiming* timing)
{
    if (remoteMetadata_) {
        // Schema-v2 requires chunk checksums in CreateSession.  The raft
        // frontend therefore accepts the preflight API as the canonical
        // session creation path; the legacy endpoint has no chunk manifest
        // and is rejected rather than creating an incomplete remote object.
        (void)fileName; (void)dirPath; (void)fileSize; (void)chunkSize; (void)out; (void)timing;
        return false;
    }
    if(fileName.empty() || fileSize == 0) return false;
    out = {};
    out.sessionId = randomId();
    out.objectId = randomId();
    if(out.sessionId.empty() || out.objectId.empty()) return false;
    const auto requestedAt = std::chrono::steady_clock::now();
    std::mutex& selectedMutex = useShardedLocks() ? sessionMutex(out.sessionId) : mutex_;
    const std::string lockLabel = useShardedLocks()
        ? std::string("createSession.session.") + std::to_string(locks_.sessionShard(out.sessionId))
        : __func__;
    GatewayMutexGuard lock(selectedMutex, lockLabel.c_str());
    const auto lockedAt = std::chrono::steady_clock::now();
    if(timing != nullptr) timing->mutexWaitUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(lockedAt - requestedAt).count());
    out.fileName = fileName;
    out.dirPath = dirPath;
    out.fileSize = fileSize;
    out.chunkSize = chunkSize == 0 ? 4 * 1024 * 1024 : chunkSize;
    //整数除法的“向上取整（Ceiling）”公式
    out.totalChunks = static_cast<uint32_t>((fileSize + out.chunkSize - 1) / out.chunkSize);
    out.createdAt = out.lastActivityAt = unixSeconds();
    sessions_[out.sessionId] = out;
    const auto writeStartedAt = std::chrono::steady_clock::now();
    const bool persisted = persistSessionLocked(out);
    const auto finishedAt = std::chrono::steady_clock::now();
    if(timing != nullptr) {
        timing->levelDbWriteUs = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(finishedAt - writeStartedAt).count());
        timing->criticalSectionUs = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(finishedAt - lockedAt).count());
    }
    return persisted;
}

FileCommitStatus GatewayState::createObjectLinkLocked(const std::string& fileName,
                                                      const std::string& dirPath,
                                                      const FileMeta& file,
                                                      ObjectMeta& out)
{
    std::string parentPath;
    std::string name;
    if (!normalizeDirectoryPath(dirPath, parentPath) || !normalizeEntryName(fileName, name)) {
        return FileCommitStatus::kInvalidRequest;
    }

    const std::string pathKey = catalogPathKey(file.ownerId, childPath(parentPath, name));
    std::string existingObjectId;
    const leveldb::Status pathStatus = db_->Get(leveldb::ReadOptions(), "path:" + pathKey, &existingObjectId);
    if (pathStatus.ok()) return FileCommitStatus::kPathConflict;
    if (!pathStatus.IsNotFound()) return FileCommitStatus::kInvalidRequest;

    ObjectMeta object;
    object.objectId = randomId();
    if (object.objectId.empty()) return FileCommitStatus::kInvalidRequest;
    object.ownerId = file.ownerId;
    object.objectVersion = file.objectVersion;
    object.metadataVersion = file.metadataVersion;
    object.parentPath = parentPath;
    object.name = name;
    object.fileHash = file.fileHash;
    object.fileSize = file.fileSize;
    object.state = file.state;
    object.createdAt = unixSeconds();

    leveldb::WriteBatch batch;
    std::string current = "/";
    for (const auto& component : split(parentPath.substr(1), '/')) {
        if (component.empty()) continue;
        current = childPath(current, component);
        const std::string directoryKey = catalogPathKey(object.ownerId, current);
        std::string existingDirectory;
        const leveldb::Status directoryStatus = db_->Get(
            leveldb::ReadOptions(), "dir:" + directoryKey, &existingDirectory);
        if (!directoryStatus.ok() && !directoryStatus.IsNotFound()) {
            return FileCommitStatus::kInvalidRequest;
        }
        if (directoryStatus.IsNotFound()) {
            batch.Put("dir:" + directoryKey,
                      directoryValue({object.ownerId, current, object.createdAt}));
        }
    }
    batch.Put("obj:" + object.objectId, objectValue(object));
    batch.Put("path:" + pathKey, object.objectId);
    if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return FileCommitStatus::kInvalidRequest;

    objectCache_.erase(object.objectId);
    catalogCache_.clear();
    out = std::move(object);
    return FileCommitStatus::kCommitted;
}

PreflightStatus GatewayState::preflightUpload(const UploadPreflightRequest& request,
                                              UploadPreflightResult& out)
{
    if (remoteMetadata_) {
        if(request.fileName.empty() || request.fileSize == 0 || request.chunkSize == 0
           || request.manifestHash.empty() || request.chunks.empty()) return PreflightStatus::kInvalidRequest;
        const std::string commandId = request.commandId.empty()
            ? metadata::MetadataClient::newCommandId("create-session") : request.commandId;
        const std::string sessionId = request.commandId.empty()
            ? metadata::MetadataClient::newCommandId("session") : "session-" + request.commandId;
        const std::string objectId = request.commandId.empty()
            ? metadata::MetadataClient::newCommandId("object") : "object-" + request.commandId;

        // Exact resume is deliberately a read before a mutation.  A retry
        // keeps its commandId, hence also its derived session/object ids.
        // Rebuilding CreateSession with a fresh expiresAt would change the
        // idempotency fingerprint and turn a recoverable timeout into a
        // COMMAND_ID_REUSE_MISMATCH.
        if(!request.commandId.empty()) {
            std::string lookupError;
            const auto existingSession = remoteMetadata_->session(sessionId, &lookupError);
            if(existingSession) {
                const auto existingObject = remoteMetadata_->object(existingSession->objectId, &lookupError);
                const uint32_t expectedChunks = static_cast<uint32_t>(
                    (request.fileSize + request.chunkSize - 1) / request.chunkSize);
                if(!existingObject || existingSession->objectId != objectId
                   || existingSession->fileSize != request.fileSize
                   || existingSession->chunkSize != request.chunkSize
                   || existingSession->totalChunks != expectedChunks
                   || existingObject->ownerId != "admin"
                   || existingObject->parentPath != request.dirPath
                   || existingObject->name != request.fileName
                   || existingObject->contentHash != request.manifestHash) {
                    miniKV::utils::logWarn("event=remote_preflight_resume_rejected command_id=" +
                                           request.commandId + " reason=immutable_manifest_mismatch");
                    return PreflightStatus::kInvalidRequest;
                }

                out = {};
                out.session.sessionId = existingSession->sessionId;
                out.session.objectId = existingSession->objectId;
                out.session.objectVersion = existingSession->objectVersion;
                out.session.ownerId = existingSession->ownerId;
                out.session.fileName = request.fileName;
                out.session.dirPath = request.dirPath;
                out.session.fileSize = existingSession->fileSize;
                out.session.chunkSize = existingSession->chunkSize;
                out.session.totalChunks = existingSession->totalChunks;
                out.session.manifestHash = request.manifestHash;
                out.session.createdAt = unixSeconds();
                out.session.lastActivityAt = out.session.createdAt;
                out.object.objectId = existingObject->objectId;
                out.object.objectVersion = existingObject->objectVersion;
                out.object.metadataVersion = existingObject->metadataVersion;
                out.object.ownerId = existingObject->ownerId;
                out.object.parentPath = existingObject->parentPath;
                out.object.name = existingObject->name;
                out.object.fileHash = existingObject->contentHash;
                out.object.fileSize = existingObject->fileSize;

                if(existingObject->state == metadata::ObjectState::kCommitted) {
                    out.object.state = FileState::kAvailable;
                    return PreflightStatus::kContentExists;
                }
                if(existingSession->expired) {
                    miniKV::utils::logWarn("event=remote_preflight_resume_rejected command_id=" +
                                           request.commandId + " reason=session_expired");
                    return PreflightStatus::kSessionExpired;
                }
                if(existingObject->state != metadata::ObjectState::kUploading) {
                    miniKV::utils::logWarn("event=remote_preflight_resume_rejected command_id=" +
                                           request.commandId + " reason=object_not_uploading");
                    return PreflightStatus::kInvalidRequest;
                }

                for(const auto& input : request.chunks) {
                    const auto chunk = remoteMetadata_->chunk(existingSession->objectId, input.chunkIndex, &lookupError);
                    const auto expectedDigest = input.checksumDigest.empty() ? input.chunkHash : input.checksumDigest;
                    const auto expectedType = input.checksumType == "sha256"
                        ? metadata::ChecksumType::kSha256 : metadata::ChecksumType::kCrc32c;
                    if(!chunk || chunk->size != input.chunkSize
                       || chunk->checksumType != expectedType
                       || chunk->checksumDigest != expectedDigest) {
                        miniKV::utils::logWarn("event=remote_preflight_resume_rejected command_id=" +
                                               request.commandId + " reason=chunk_manifest_mismatch");
                        return PreflightStatus::kInvalidRequest;
                    }
                    if(chunk->state == metadata::ChunkState::kCommitted) {
                        out.session.completed.emplace(input.chunkIndex,
                            CompletedChunk{input.chunkIndex, input.chunkHash, input.chunkSize});
                        out.presentChunks.push_back(input);
                    } else {
                        out.missingChunks.push_back(input);
                    }
                }
                out.session.completedChunks = static_cast<uint32_t>(out.session.completed.size());
                out.object.state = FileState::kProtecting;
                miniKV::utils::logInfo("event=remote_preflight_resume command_id=" + request.commandId +
                                      " completed=" + std::to_string(out.session.completedChunks) +
                                      " missing=" + std::to_string(out.missingChunks.size()));
                return PreflightStatus::kUploadRequired;
            }
            if(!lookupError.empty()) {
                miniKV::utils::logWarn("event=remote_preflight_resume_lookup_failed command_id=" +
                                       request.commandId + " error=" + lookupError);
                return PreflightStatus::kInvalidRequest;
            }
        }

        metadata::MetadataCommand command;
        command.commandId = commandId;
        command.type = metadata::MetadataCommandType::kCreateSession;
        command.actorType = "gateway"; command.actorId = "admin"; command.issuedAt = 0;
        metadata::CreateSessionPayload payload;
        payload.sessionId = sessionId; payload.objectId = objectId; payload.ownerId = "admin";
        payload.parentPath = request.dirPath; payload.name = request.fileName; payload.contentHash = request.manifestHash;
        payload.fileSize = request.fileSize; payload.chunkSize = request.chunkSize == 0 ? 4U * 1024U * 1024U : request.chunkSize;
        payload.desiredRf = replicationFactor_; payload.expiresAt = unixSeconds() + uploadLeaseTtlSeconds();
        std::vector<metadata::ReserveLeaseRequest> preflightReserves;
        for(const auto& input : request.chunks) {
            metadata::InitialChunk chunk; chunk.index = input.chunkIndex; chunk.routeKey = sessionId + "/route/" + std::to_string(input.chunkIndex);
            // Opaque chunk identities are used as DataNode URL paths. Keep
            // them URL-safe and deterministic across command retries; a
            // slash here would be escaped by the SDK and no longer match the
            // capability identity validated by the DataNode.
            const std::string identitySeed = objectId + "\n" +
                std::to_string(input.chunkIndex) + "\n" +
                (input.checksumDigest.empty() ? input.chunkHash : input.checksumDigest);
            chunk.storageIdentity = "chk-" +
                sha256Hex(identitySeed.data(), identitySeed.size()).substr(0, 40);
            chunk.identityScheme = input.identityScheme == "cas-sha256" ? metadata::IdentityScheme::kContentHash : metadata::IdentityScheme::kOpaque;
            chunk.checksumType = input.checksumType == "sha256" ? metadata::ChecksumType::kSha256 : metadata::ChecksumType::kCrc32c;
            chunk.checksumDigest = input.checksumDigest.empty() ? input.chunkHash : input.checksumDigest;
            chunk.size = input.chunkSize; chunk.generation = 1; payload.chunks.push_back(std::move(chunk));
            metadata::ReserveLeaseRequest reserve;
            reserve.commandId = "reserve-" + sessionId + "-" + std::to_string(input.chunkIndex);
            reserve.actorType = "gateway"; reserve.actorId = "admin";
            reserve.leaseId = "lease-" + sessionId + "-" + std::to_string(input.chunkIndex);
            reserve.requestKey = sessionId + "/" + std::to_string(input.chunkIndex) + "/" + input.chunkHash;
            reserve.sessionId = sessionId; reserve.chunkIndex = input.chunkIndex;
            reserve.routeKey = sessionId + "/route/" + std::to_string(input.chunkIndex);
            reserve.chunkSize = input.chunkSize; reserve.generation = 1; reserve.desiredRf = replicationFactor_;
            reserve.expiresAt = payload.expiresAt; reserve.nowMs = static_cast<int64_t>(unixSeconds()) * 1000;
            preflightReserves.push_back(std::move(reserve));
        }
        command.payload = std::move(payload);
        std::string metadataError;
        // Create the session and reserve all chunk leases in one append batch.
        // This is safe because placement is computed by the authoritative
        // Metadata leader and the state machine applies CreateSession first.
        const auto detailedResult = remoteMetadata_->createSessionAndReserveDetailed(
            command, preflightReserves, &metadataError);
        const auto& result = detailedResult.result;
        if(result.status == metadata::ApplyStatus::kConflict) {
            miniKV::utils::logWarn("event=remote_preflight_rejected status=CONFLICT command_id=" +
                                   command.commandId + " message=" + result.message);
            return PreflightStatus::kPathConflict;
        }
        if(result.status != metadata::ApplyStatus::kOk && result.status != metadata::ApplyStatus::kAlreadyApplied) {
            miniKV::utils::logWarn("event=remote_preflight_rejected status=" +
                                   std::string(metadata::toString(result.status)) +
                                   " command_id=" + command.commandId + " message=" +
                                   (result.message.empty() ? metadataError : result.message));
            return PreflightStatus::kInvalidRequest;
        }
        // The create+reserve response contains the records produced by the
        // same committed batch.  Keep the point-read fallback for rolling
        // upgrades or an older MetadataService, but the normal Raft path now
        // avoids two additional ReadIndex HTTP round trips.
        auto session = detailedResult.session;
        auto object = detailedResult.object;
        if(!session) session = remoteMetadata_->session(sessionId);
        if(!object) object = remoteMetadata_->object(objectId);
        if(!session || !object) {
            miniKV::utils::logWarn("event=remote_preflight_missing_state session_id=" + sessionId +
                                   " object_id=" + objectId);
            return PreflightStatus::kInvalidRequest;
        }
        out = {}; out.session.sessionId = session->sessionId; out.session.objectId = session->objectId;
        out.session.objectVersion = session->objectVersion; out.session.ownerId = session->ownerId; out.session.fileSize = session->fileSize;
        out.session.chunkSize = session->chunkSize; out.session.totalChunks = session->totalChunks; out.session.createdAt = unixSeconds();
        out.session.lastActivityAt = out.session.createdAt; out.session.fileName = request.fileName; out.session.dirPath = request.dirPath;
        out.session.manifestHash = request.manifestHash; out.object.objectId = object->objectId; out.object.objectVersion = object->objectVersion;
        out.object.metadataVersion = object->metadataVersion; out.object.ownerId = object->ownerId; out.object.parentPath = object->parentPath;
        out.object.name = object->name; out.object.fileHash = object->contentHash; out.object.fileSize = object->fileSize;
        out.object.state = FileState::kProtecting; out.missingChunks = request.chunks;
        return PreflightStatus::kUploadRequired;
    }
    out = {};
    std::string parentPath;
    std::string name;
    if (request.fileSize == 0 || request.chunkSize == 0 || request.manifestHash.empty() ||
        !normalizeDirectoryPath(request.dirPath, parentPath) ||
        !normalizeEntryName(request.fileName, name)) {
        return PreflightStatus::kInvalidRequest;
    }

    const uint64_t calculatedChunks =
        1 + (request.fileSize - 1) / request.chunkSize;
    if (calculatedChunks == 0 || calculatedChunks > UINT32_MAX ||
        request.chunks.size() != calculatedChunks) {
        return PreflightStatus::kInvalidRequest;
    }
    for (size_t index = 0; index < request.chunks.size(); ++index) {
        const auto& chunk = request.chunks[index];
        const uint64_t expected = index + 1 == request.chunks.size()
            ? request.fileSize - static_cast<uint64_t>(index) * request.chunkSize
            : request.chunkSize;
        if (chunk.chunkIndex != index || chunk.chunkHash.empty() || chunk.chunkSize != expected) {
            return PreflightStatus::kInvalidRequest;
        }
    }
    if (manifestHash(request.fileSize, request.chunkSize, request.chunks) != request.manifestHash) {
        return PreflightStatus::kInvalidRequest;
    }

    const std::string pathOwnerKey = catalogPathKey("admin", childPath(parentPath, name));
    std::mutex& selectedMutex = useShardedLocks() ? objectMutex(pathOwnerKey) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    const std::string pathKey = pathOwnerKey;
    std::string existingObjectId;
    const leveldb::Status pathStatus = db_->Get(leveldb::ReadOptions(), "path:" + pathKey, &existingObjectId);
    if (pathStatus.ok()) return PreflightStatus::kPathConflict;
    if (!pathStatus.IsNotFound()) return PreflightStatus::kInvalidRequest;

    FileMeta existingFile;
    if (getFileLocked(request.manifestHash, existingFile)) {
        const FileCommitStatus linkStatus =
            createObjectLinkLocked(name, parentPath, existingFile, out.object);
        if (linkStatus == FileCommitStatus::kCommitted) return PreflightStatus::kContentExists;
        return linkStatus == FileCommitStatus::kPathConflict
            ? PreflightStatus::kPathConflict
            : PreflightStatus::kInvalidRequest;
    }

    SessionState session;
    session.sessionId = randomId();
    session.objectId = randomId();
    if (session.sessionId.empty() || session.objectId.empty()) {
        return PreflightStatus::kInvalidRequest;
    }
    session.fileName = name;
    session.dirPath = parentPath;
    session.fileSize = request.fileSize;
    session.chunkSize = request.chunkSize;
    session.totalChunks = static_cast<uint32_t>(calculatedChunks);
    session.manifestHash = request.manifestHash;
    session.createdAt = session.lastActivityAt = unixSeconds();

    for (const auto& chunk : request.chunks) {
        ChunkRoute route;
        if (getRouteLocked(chunk.chunkHash, route) && route.size == chunk.chunkSize &&
            !route.replicas.empty()) {
            session.completed[chunk.chunkIndex] =
                {chunk.chunkIndex, chunk.chunkHash, chunk.chunkSize};
            out.presentChunks.push_back(chunk);
        } else {
            out.missingChunks.push_back(chunk);
        }
    }
    sessions_[session.sessionId] = session;
    if (!persistSessionLocked(session)) {
        sessions_.erase(session.sessionId);
        return PreflightStatus::kInvalidRequest;
    }
    out.session = std::move(session);
    return PreflightStatus::kUploadRequired;
}

bool GatewayState::createDerivedUpload(const std::string& jobId, const std::string& leaseToken,
                                       const DerivedUploadRequest& request,
                                       DerivedUploadResult& out)
{
    out = {};
    std::string fileName;
    if(jobId.empty() || leaseToken.empty() || request.fileSize == 0 || request.chunkSize == 0 ||
       request.manifestHash.empty() || !normalizeEntryName(request.fileName, fileName)) {
        return false;
    }
    const uint64_t expectedChunks = 1 + (request.fileSize - 1) / request.chunkSize;
    if(expectedChunks == 0 || expectedChunks > UINT32_MAX || request.chunks.size() != expectedChunks ||
       manifestHash(request.fileSize, request.chunkSize, request.chunks) != request.manifestHash) {
        return false;
    }
    for(size_t index = 0; index < request.chunks.size(); ++index) {
        const auto& chunk = request.chunks[index];
        const uint64_t expectedSize = index + 1 == request.chunks.size()
            ? request.fileSize - static_cast<uint64_t>(index) * request.chunkSize
            : request.chunkSize;
        if(chunk.chunkIndex != index || chunk.chunkHash.empty() || chunk.chunkSize != expectedSize) {
            return false;
        }
    }

    const int64_t now = unixSeconds();
    std::mutex& selectedMutex = useShardedLocks() ? sessionMutex(jobId) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    media::MediaJob job;
    if(!getMediaJobLocked(jobId, job) || job.type != media::JobType::kThumbnail ||
       job.state != media::JobState::kRunning || job.leaseToken != leaseToken ||
       job.leaseUntil < now) {
        return false;
    }

    SessionState session;
    session.sessionId = randomId();
    session.objectId = randomId();
    if(session.sessionId.empty() || session.objectId.empty()) return false;
    session.fileName = fileName;
    session.fileSize = request.fileSize;
    session.chunkSize = request.chunkSize;
    session.totalChunks = static_cast<uint32_t>(expectedChunks);
    session.manifestHash = request.manifestHash;
    session.derivedJobId = job.jobId;
    session.derivedProfile = job.profile;
    session.createdAt = session.lastActivityAt = now;
    for(const auto& chunk : request.chunks) {
        ChunkRoute route;
        if(getRouteLocked(chunk.chunkHash, route) && route.size == chunk.chunkSize &&
           !route.replicas.empty()) {
            session.completed.emplace(chunk.chunkIndex,
                                      CompletedChunk{chunk.chunkIndex, chunk.chunkHash, chunk.chunkSize});
            out.presentChunks.push_back(chunk);
        } else {
            out.missingChunks.push_back(chunk);
        }
    }
    sessions_[session.sessionId] = session;
    if(!persistSessionLocked(session)) {
        sessions_.erase(session.sessionId);
        return false;
    }
    out.session = std::move(session);
    return true;
}

bool GatewayState::getSession(const std::string& sessionId, SessionState& out) const
{
    if (remoteMetadata_) {
        const auto value = remoteMetadata_->session(sessionId);
        if(!value) return false;
        out = {}; out.sessionId = value->sessionId; out.objectId = value->objectId; out.objectVersion = value->objectVersion;
        const auto object = remoteMetadata_->object(value->objectId);
        if(!object) return false;
        out.metadataVersion = object->metadataVersion; out.ownerId = value->ownerId;
        out.fileSize = value->fileSize; out.chunkSize = value->chunkSize; out.totalChunks = value->totalChunks;
        out.completedChunks = value->completedChunks;
        out.createdAt = unixSeconds(); out.lastActivityAt = out.createdAt;
        return true;
    }
    std::mutex& selectedMutex = useShardedLocks() ? sessionMutex(sessionId) : mutex_;
    const std::string lockLabel = useShardedLocks()
        ? std::string("getSession.session.") + std::to_string(locks_.sessionShard(sessionId))
        : __func__;
    GatewayMutexGuard lock(selectedMutex, lockLabel.c_str());
    const auto it = sessions_.find(sessionId);
    if(it == sessions_.end()) return false;
    out = it->second;
    return true;
}
RoutePlanStatus GatewayState::planRoutes(const std::string& sessionId,
                                         const std::vector<ChunkRouteRequest>& requests,
                                         std::vector<PlacementPlan>& out,
                                         GatewayMutationTiming* timing)
{
    if (remoteMetadata_) {
        out.clear();
        if(requests.empty()) return RoutePlanStatus::kInvalidRequest;

        // The normal route path historically fetched session, nodes, chunks
        // and leases independently.  In Raft mode that meant one ReadIndex
        // confirmation and HTTP round trip per metadata object.  Try the
        // read-only aggregate first; if a lease is missing/expired, fall back
        // to the existing reservation path below so retries preserve its
        // capacity and fencing semantics.
        std::vector<uint32_t> requestedIndexes;
        requestedIndexes.reserve(requests.size());
        for(const auto& request : requests) requestedIndexes.push_back(request.chunkIndex);
        const auto aggregate = remoteMetadata_->uploadRoutes(sessionId, requestedIndexes);
        if(aggregate && aggregate->routes.size() == requests.size()) {
            std::unordered_map<std::string, NodeRecord> nodes;
            for(const auto& node : aggregate->nodes) {
                NodeRecord converted;
                converted.nodeId = node.nodeId;
                converted.address = node.address;
                converted.httpPort = node.dataPort;
                converted.maxStorageBytes = node.registeredCapacityBytes;
                converted.capabilities = node.capabilities;
                nodes.emplace(converted.nodeId, std::move(converted));
            }
            std::unordered_map<uint32_t, const metadata::UploadRouteView*> routes;
            for(const auto& route : aggregate->routes) routes.emplace(route.index, &route);
            bool valid = true;
            std::vector<PlacementPlan> fastPlans;
            fastPlans.reserve(requests.size());
            for(const auto& request : requests) {
                const auto routeIt = routes.find(request.chunkIndex);
                if(routeIt == routes.end()) { valid = false; break; }
                const auto& route = *routeIt->second;
                const auto& chunk = route.chunk;
                const auto& lease = route.lease;
                if(chunk.state != metadata::ChunkState::kAllocated ||
                   chunk.size != request.chunkSize || lease.state != metadata::LeaseState::kActive ||
                   lease.sessionId != sessionId || lease.chunkIndex != request.chunkIndex ||
                   (!request.checksumDigest.empty() && request.checksumDigest != chunk.checksumDigest) ||
                   (request.chunkHash != chunk.checksumDigest && request.identityScheme == "cas-sha256")) {
                    valid = false; break;
                }
                PlacementPlan plan;
                plan.chunkIndex = request.chunkIndex;
                plan.leaseId = lease.leaseId;
                plan.routeVersion = lease.generation;
                plan.expiresAt = lease.expiresAt;
                plan.identityScheme = chunk.identityScheme == metadata::IdentityScheme::kContentHash
                    ? "cas-sha256" : "opaque-chunk-id";
                plan.chunkId = chunk.storageIdentity;
                plan.checksumType = metadata::toString(chunk.checksumType);
                plan.checksumDigest = chunk.checksumDigest;
                plan.objectVersion = aggregate->session.objectVersion;
                for(const auto& target : lease.targets) {
                    const auto nodeIt = nodes.find(target.nodeId);
                    if(nodeIt == nodes.end()) { valid = false; break; }
                    NodeSnapshot snapshot;
                    snapshot.record = nodeIt->second;
                    snapshot.runtime.state = NodeLiveState::kOnline;
                    plan.chain.push_back(std::move(snapshot));
                }
                if(!valid || plan.chain.size() != replicationFactor_) { valid = false; break; }
                fastPlans.push_back(std::move(plan));
            }
            if(valid && fastPlans.size() == requests.size()) {
                out = std::move(fastPlans);
                return RoutePlanStatus::kOk;
            }
            out.clear();
        }

        SessionState session;
        if(!getSession(sessionId, session)) return RoutePlanStatus::kInvalidRequest;
        const auto nodeRecords = remoteMetadata_->nodes();
        std::map<std::string, NodeRecord> nodes;
        for(const auto& node : nodeRecords) { NodeRecord converted; converted.nodeId=node.nodeId; converted.address=node.address; converted.httpPort=node.dataPort; converted.maxStorageBytes=node.registeredCapacityBytes; converted.capabilities=node.capabilities; nodes.emplace(converted.nodeId, std::move(converted)); }
        std::vector<metadata::ReserveLeaseRequest> reserves;
        std::vector<metadata::ReserveLeaseRequest> allReserves;
        std::vector<metadata::ChunkRouteRecord> chunks;
        reserves.reserve(requests.size()); allReserves.reserve(requests.size()); chunks.reserve(requests.size());
        const int64_t leaseExpires = unixSeconds() + uploadLeaseTtlSeconds();
        const int64_t observedAt = static_cast<int64_t>(unixSeconds()) * 1000;
        for(const auto& request : requests) {
            const auto chunk = remoteMetadata_->chunk(session.objectId, request.chunkIndex);
            if(!chunk || chunk->state != metadata::ChunkState::kAllocated || chunk->size != request.chunkSize) { out.clear(); return RoutePlanStatus::kInvalidRequest; }
            metadata::ReserveLeaseRequest reserve; reserve.commandId = "reserve-" + sessionId + "-" + std::to_string(request.chunkIndex);
            reserve.actorType = "gateway"; reserve.actorId = "admin"; reserve.leaseId = "lease-" + sessionId + "-" + std::to_string(request.chunkIndex);
            reserve.requestKey = sessionId + "/" + std::to_string(request.chunkIndex) + "/" + request.chunkHash;
            reserve.sessionId = sessionId; reserve.chunkIndex = request.chunkIndex; reserve.routeKey = chunk->routeKey;
            reserve.chunkSize = request.chunkSize; reserve.generation = chunk->generation; reserve.desiredRf = replicationFactor_;
            reserve.expiresAt = leaseExpires; reserve.nowMs = observedAt;
            allReserves.push_back(reserve);
            // A retry after the preflight create+reserve batch already has a
            // durable lease.  Reuse it rather than appending another entry.
            if(!remoteMetadata_->lease(reserve.leaseId)) reserves.push_back(reserve);
            chunks.push_back(*chunk);
        }
        // Placement is still decided by the Metadata leader, but all lease
        // commands are appended as one Raft batch.  Each command retains its
        // own commandId and state-machine validation; only the WAL fsync
        // boundary is shared.
        if(!reserves.empty()) {
            const auto batchResult = remoteMetadata_->reserveLeaseBatch(reserves);
            if(batchResult.status == metadata::ApplyStatus::kUnavailable) { out.clear(); return RoutePlanStatus::kNoCapacity; }
            if(batchResult.status != metadata::ApplyStatus::kOk && batchResult.status != metadata::ApplyStatus::kAlreadyApplied) { out.clear(); return RoutePlanStatus::kNoCapacity; }
        }
        for(size_t i = 0; i < requests.size(); ++i) {
            const auto& request = requests[i]; const auto& chunk = chunks[i]; const auto& reserve = allReserves[i];
            const auto lease = remoteMetadata_->lease(reserve.leaseId); if(!lease) { out.clear(); return RoutePlanStatus::kNoCapacity; }
            PlacementPlan plan; plan.chunkIndex=request.chunkIndex; plan.leaseId=lease->leaseId; plan.routeVersion=lease->generation; plan.expiresAt=lease->expiresAt;
            plan.identityScheme = chunk.identityScheme == metadata::IdentityScheme::kContentHash ? "cas-sha256" : "opaque-chunk-id";
            plan.chunkId = chunk.storageIdentity; plan.checksumType = metadata::toString(chunk.checksumType); plan.checksumDigest = chunk.checksumDigest; plan.objectVersion=session.objectVersion;
            for(const auto& target : lease->targets) { const auto it=nodes.find(target.nodeId); if(it==nodes.end()) { out.clear(); return RoutePlanStatus::kNoCapacity; } NodeSnapshot snapshot; snapshot.record=it->second; snapshot.runtime.state=NodeLiveState::kOnline; snapshot.record.maxStorageBytes=it->second.maxStorageBytes; plan.chain.push_back(std::move(snapshot)); }
            if(plan.chain.size() != replicationFactor_) { out.clear(); return RoutePlanStatus::kNoCapacity; }
            out.push_back(std::move(plan));
        }
        return RoutePlanStatus::kOk;
    }
    const auto requestedAt = std::chrono::steady_clock::now();
    std::mutex& selectedMutex = useShardedLocks() ? sessionMutex(sessionId) : mutex_;
    const std::string lockLabel = useShardedLocks()
        ? std::string("planRoutes.session.") + std::to_string(locks_.sessionShard(sessionId))
        : __func__;
    GatewayMutexGuard lock(selectedMutex, lockLabel.c_str());
    const auto lockedAt = std::chrono::steady_clock::now();
    if(timing != nullptr) timing->mutexWaitUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(lockedAt - requestedAt).count());
    const auto it = sessions_.find(sessionId);
    if(it == sessions_.end() || requests.empty()) return RoutePlanStatus::kInvalidRequest;
    // Reservation counters are part of NodeRegistry ownership.  This is an
    // exclusive, short-lived lock (no LevelDB I/O occurs in this section).
    std::unique_lock<std::shared_mutex> nodeLock;
    if (useShardedLocks()) nodeLock = std::unique_lock<std::shared_mutex>(locks_.nodes());
    out.clear();
    const int64_t now = unixSeconds();
    if (!useShardedLocks()) releaseExpiredLeasesLocked(now);
    const size_t sessionShardIndex = locks_.sessionShard(sessionId);
    auto& leaseRequestIndex = useShardedLocks()
        ? shardedLeaseByRequestKey_[sessionShardIndex] : leaseByRequestKey_;
    auto& leaseIndex = useShardedLocks()
        ? shardedLeases_[sessionShardIndex] : leases_;
    std::vector<std::string> createdLeaseIds;
    for(const auto& originalRequest : requests)
    {
        ChunkRouteRequest request = originalRequest;
        if(request.identityScheme.empty()) request.identityScheme = "cas-sha256";
        if(request.checksumType.empty()) request.checksumType = "sha256";
        if(request.checksumDigest.empty() && request.checksumType == "sha256") {
            request.checksumDigest = request.chunkHash;
        }
        const uint32_t index = request.chunkIndex;
        if(index >= it->second.totalChunks) {
            for(const auto& leaseId : createdLeaseIds) releaseLeaseLocked(leaseId);
            out.clear();
            return RoutePlanStatus::kInvalidRequest;
        }
        const uint64_t expected = index + 1 == it->second.totalChunks
            ? it->second.fileSize - static_cast<uint64_t>(index) * it->second.chunkSize
            : it->second.chunkSize;
        const bool supportedIdentity = request.identityScheme == "cas-sha256" ||
            request.identityScheme == "opaque-chunk-id";
        const bool legacyCasChecksum = request.identityScheme == "cas-sha256" &&
            request.checksumType == "sha256" &&
            request.checksumDigest == request.chunkHash;
        const bool supportedChecksum = legacyCasChecksum ||
            (request.checksumType == "sha256" &&
             lowerHexDigest(request.checksumDigest, 64)) ||
            (request.checksumType == "crc32c" &&
             lowerHexDigest(request.checksumDigest, 8));
        const bool validCas = request.identityScheme != "cas-sha256" ||
            (request.checksumType == "sha256" &&
             request.checksumDigest == request.chunkHash);
        if(request.chunkHash.empty() || request.chunkSize != expected ||
           !supportedIdentity || !supportedChecksum || !validCas ||
           it->second.completed.count(index))
        {
            for(const auto& leaseId : createdLeaseIds) releaseLeaseLocked(leaseId);
            out.clear();
            return RoutePlanStatus::kInvalidRequest;
        }

        const std::string requestKey = routeRequestKey(sessionId, request);
        const auto existingId = leaseRequestIndex.find(requestKey);
        if(existingId != leaseRequestIndex.end()) {
            const auto existingLease = leaseIndex.find(existingId->second);
            if(existingLease != leaseIndex.end()) {
                out.push_back(existingLease->second.plan);
                continue;
            }
            leaseRequestIndex.erase(existingId);
        }

        PlacementPlan plan = selectPlacementLocked(it->second, index);
        if(plan.chain.empty() || !reserveLeaseLocked(it->second, request, plan, now)) {
            for(const auto& leaseId : createdLeaseIds) releaseLeaseLocked(leaseId);
            out.clear();
            return RoutePlanStatus::kNoCapacity;
        }
        createdLeaseIds.push_back(plan.leaseId);
        out.push_back(std::move(plan));
    }
    if(timing != nullptr) timing->criticalSectionUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - lockedAt).count());
    return RoutePlanStatus::kOk;
}
CommitChunkStatus GatewayState::commitChunk(const std::string& sessionId, uint32_t index,
                                            const std::string& chunkHash, uint64_t size,
                                            const std::vector<std::string>& successfulNodes,
                                            const std::string& leaseId,
                                            GatewayMutationTiming* timing)
{
    if (remoteMetadata_) {
        SessionState session; if(!getSession(sessionId, session)) return CommitChunkStatus::kInvalidRequest;
        const auto chunk = remoteMetadata_->chunk(session.objectId, index); const auto lease = remoteMetadata_->lease(leaseId);
        if(!chunk || !lease || lease->state != metadata::LeaseState::kActive) return CommitChunkStatus::kInvalidRequest;
        metadata::MetadataCommand command; command.commandId = "commit-chunk-" + sessionId + "-" + std::to_string(index) + "-" + std::to_string(chunk->generation);
        command.type = metadata::MetadataCommandType::kCommitChunk; command.actorType="gateway"; command.actorId="admin"; command.generation=chunk->generation; command.issuedAt=unixSeconds();
        metadata::CommitChunkPayload payload; payload.sessionId=sessionId; payload.leaseId=leaseId; payload.chunkIndex=index; payload.routeKey=chunk->routeKey; payload.chunkSize=size; payload.checksumType=chunk->checksumType; payload.checksumDigest=chunk->checksumDigest;
        for(const auto& nodeId : successfulNodes) { auto target=std::find_if(lease->targets.begin(), lease->targets.end(), [&](const auto& candidate){ return candidate.nodeId==nodeId; }); if(target==lease->targets.end()) return CommitChunkStatus::kInvalidRequest; payload.replicas.push_back({nodeId,target->nodeEpoch,chunk->checksumDigest,unixSeconds()}); }
        command.payload=std::move(payload);
        // Once this is the final not-yet-committed chunk, there is no reason
        // to publish CommitChunk and CommitFile as separate durable entries.
        // Append the independently-idempotent commands in one Raft batch;
        // the state machine still applies and validates them in order, and
        // the later public commitFile call observes the already-committed
        // object without appending another entry.  A stale remote session
        // snapshot merely falls back to the explicit public CommitFile.
        metadata::ApplyResult result;
        const auto remoteObject = remoteMetadata_->object(session.objectId);
        const bool finalChunk = session.completedChunks + 1 == session.totalChunks;
        if(finalChunk) {
            if(!remoteObject) return CommitChunkStatus::kInvalidRequest;
            metadata::MetadataCommand fileCommand;
            fileCommand.commandId = "commit-file-" + sessionId;
            fileCommand.type = metadata::MetadataCommandType::kCommitFile;
            fileCommand.actorType = "gateway"; fileCommand.actorId = "admin"; fileCommand.issuedAt = unixSeconds();
            fileCommand.payload = metadata::CommitFilePayload{sessionId, session.objectId, session.objectVersion, remoteObject->contentHash};
            result = remoteMetadata_->proposeBatch({command, fileCommand});
        } else {
            result = remoteMetadata_->propose(command);
        }
        if(result.status==metadata::ApplyStatus::kAlreadyApplied) return CommitChunkStatus::kAlreadyCommitted;
        return result.status==metadata::ApplyStatus::kOk ? CommitChunkStatus::kCommitted : CommitChunkStatus::kInvalidRequest;
    }
    //确认单个分片写入成功
    if(chunkHash.empty() || successfulNodes.empty()) return CommitChunkStatus::kInvalidRequest;
    const auto requestedAt = std::chrono::steady_clock::now();
    std::mutex& selectedMutex = useShardedLocks() ? sessionMutex(sessionId) : mutex_;
    const std::string lockLabel = useShardedLocks()
        ? std::string("commitChunk.session.") + std::to_string(locks_.sessionShard(sessionId))
        : __func__;
    GatewayMutexGuard lock(selectedMutex, lockLabel.c_str());
    const auto lockedAt = std::chrono::steady_clock::now();
    if(timing != nullptr) {
        timing->mutexWaitUs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            lockedAt - requestedAt).count());
    }
    // Reservation expiry is maintained by the compatibility cleanup path until
    // the reservation index is fully migrated into NodeRegistry.  Skipping it
    // here avoids mutating NodeRegistry-owned maps without its exclusive lock.
    if (!useShardedLocks()) releaseExpiredLeasesLocked(unixSeconds());
    // Keep invalid commit diagnostics close to the state validation.  In
    // sharded mode an HTTP 400 can otherwise be reported by the DataNode
    // without identifying which lease/session invariant rejected it.  This
    // is observation only; the validation and return statuses are unchanged.
    const auto invalid = [&](const char* reason, const std::string& detail = std::string()) {
        miniKV::utils::logWarn("event=chunk_commit_rejected reason=" + std::string(reason) +
            " session=" + sessionId + " index=" + std::to_string(index) +
            " chunk=" + chunkHash + " lease=" + leaseId +
            " successful_nodes=" + join(successfulNodes, ',') +
            " detail=" + (detail.empty() ? "-" : detail));
        return CommitChunkStatus::kInvalidRequest;
    };
    auto sessionIt = sessions_.find(sessionId);
    if(sessionIt == sessions_.end()) return invalid("session_missing");
    if(index >= sessionIt->second.totalChunks) {
        return invalid("chunk_index_out_of_range", "total_chunks=" + std::to_string(sessionIt->second.totalChunks));
    }
    const uint64_t expected = index + 1 == sessionIt->second.totalChunks ? sessionIt->second.fileSize - static_cast<uint64_t>(index) * sessionIt->second.chunkSize : sessionIt->second.chunkSize;
    if(size != expected) return invalid("chunk_size_mismatch", "expected=" + std::to_string(expected) +
        " actual=" + std::to_string(size));

    const auto completed = sessionIt->second.completed.find(index);
    if(completed != sessionIt->second.completed.end()) {
        return completed->second.chunkHash == chunkHash && completed->second.size == size
            ? CommitChunkStatus::kAlreadyCommitted
            : invalid("duplicate_chunk_mismatch");
    }

    const size_t leaseShardIndex = useShardedLocks() ? leaseShard(leaseId) : kLockShardCount;
    auto& leaseIndex = useShardedLocks() && leaseShardIndex < kLockShardCount
        ? shardedLeases_[leaseShardIndex] : leases_;
    const auto lease = leaseIndex.find(leaseId);
    if(lease == leaseIndex.end()) return invalid("lease_missing");
    if(lease->second.sessionId != sessionId) return invalid("lease_session_mismatch",
        "lease_session=" + lease->second.sessionId);
    if(lease->second.chunkIndex != index) return invalid("lease_index_mismatch",
        "lease_index=" + std::to_string(lease->second.chunkIndex));
    if(lease->second.chunkHash != chunkHash) return invalid("lease_hash_mismatch");
    if(lease->second.chunkSize != size) return invalid("lease_size_mismatch",
        "lease_size=" + std::to_string(lease->second.chunkSize));
    for(const auto& nodeId : successfulNodes) {
        const bool allowed = std::any_of(lease->second.plan.chain.begin(), lease->second.plan.chain.end(),
            [&nodeId](const NodeSnapshot& node) { return node.record.nodeId == nodeId; });
        if(!allowed) return invalid("node_not_in_lease", nodeId);
    }
    sessionIt->second.completed[index] = {index, chunkHash, size};
    sessionIt->second.lastActivityAt = unixSeconds();
    //真实chunk元数据落盘
    ChunkRoute route;
    if (!getRouteLocked(chunkHash, route)) route = {};
    route.chunkHash = chunkHash;
    route.chunkId = lease->second.plan.chunkId;
    route.identityScheme = lease->second.plan.identityScheme;
    route.checksumType = lease->second.plan.checksumType;
    route.checksumDigest = lease->second.plan.checksumDigest;
    route.objectVersion = lease->second.plan.objectVersion;
    route.generation = lease->second.plan.routeVersion;
    route.size = size;
    route.desiredReplicas = replicationFactor_;
    route.updateAt = unixSeconds();
    for(const auto& node : successfulNodes)
    {
        if(std::find(route.replicas.begin(), route.replicas.end(), node) == route.replicas.end())
        {
            route.replicas.push_back(node);
        }
    }
    const auto writeStartedAt = std::chrono::steady_clock::now();
    const bool persisted = persistRouteLocked(route) && persistSessionLocked(sessionIt->second);
    const auto writeFinishedAt = std::chrono::steady_clock::now();
    if (persisted) {
        routeCache_.put(chunkHash, std::make_shared<const ChunkRoute>(route),
                        estimatedRouteBytes(route), kRouteCacheTtlSeconds);
        manifestCache_.clear();
    }
    if (persisted) {
        if (useShardedLocks()) {
            std::unique_lock<std::shared_mutex> nodeLock(locks_.nodes());
            releaseLeaseLocked(leaseId, ReservationToken::State::kCommitted);
        } else {
            releaseLeaseLocked(leaseId, ReservationToken::State::kCommitted);
        }
    }
    if(timing != nullptr) {
        timing->levelDbWriteUs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            writeFinishedAt - writeStartedAt).count());
        timing->criticalSectionUs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - lockedAt).count());
    }
    return persisted ? CommitChunkStatus::kCommitted : CommitChunkStatus::kInvalidRequest;
}

bool GatewayState::releaseLease(const std::string& leaseId)
{
    if(leaseId.empty()) return false;
    if (remoteMetadata_) {
        const auto lease = remoteMetadata_->lease(leaseId); if(!lease) return false;
        metadata::MetadataCommand command; command.commandId = "release-lease-" + leaseId + "-" + std::to_string(lease->generation);
        command.type = metadata::MetadataCommandType::kReleaseLease; command.actorType="gateway"; command.actorId="admin"; command.issuedAt=unixSeconds();
        command.payload = metadata::ReleaseLeasePayload{leaseId, lease->generation};
        const auto result = remoteMetadata_->propose(command); return result.status==metadata::ApplyStatus::kOk || result.status==metadata::ApplyStatus::kAlreadyApplied;
    }
    if (useShardedLocks()) {
        const size_t shard = leaseShard(leaseId);
        if (shard >= kLockShardCount) return false;
        std::unique_lock<std::mutex> sessionLock(locks_.session(shard));
        if (shardedLeases_[shard].find(leaseId) == shardedLeases_[shard].end()) return false;
        std::unique_lock<std::shared_mutex> nodeLock(locks_.nodes());
        return releaseLeaseLocked(leaseId, ReservationToken::State::kRolledBack);
    }
    GatewayMutexGuard lock(mutex_, __func__);
    return releaseLeaseLocked(leaseId, ReservationToken::State::kRolledBack);
}
FileCommitStatus GatewayState::commitFile(const std::string& sessionId, FileMeta& out,
                                          GatewayMutationTiming* timing)
{
    if (remoteMetadata_) {
        SessionState session; if(!getSession(sessionId, session)) return FileCommitStatus::kInvalidRequest;
        const auto object = remoteMetadata_->object(session.objectId); if(!object) return FileCommitStatus::kInvalidRequest;
        if(object->state == metadata::ObjectState::kCommitted) {
            out={}; out.objectId=object->objectId; out.objectVersion=object->objectVersion; out.metadataVersion=object->metadataVersion;
            out.ownerId=object->ownerId; out.fileName=object->name; out.dirPath=object->parentPath; out.fileHash=object->contentHash;
            out.fileSize=object->fileSize; out.chunkSize=object->chunkSize; out.state=FileState::kAvailable; out.createdAt=unixSeconds();
            return FileCommitStatus::kCommitted;
        }
        metadata::MetadataCommand command; command.commandId = "commit-file-" + sessionId; command.type=metadata::MetadataCommandType::kCommitFile;
        command.actorType="gateway"; command.actorId="admin"; command.issuedAt=unixSeconds(); command.payload=metadata::CommitFilePayload{sessionId, session.objectId, session.objectVersion, object->contentHash};
        const auto result=remoteMetadata_->propose(command);
        if(result.status!=metadata::ApplyStatus::kOk && result.status!=metadata::ApplyStatus::kAlreadyApplied) return FileCommitStatus::kInvalidRequest;
        const auto committed=remoteMetadata_->object(session.objectId); if(!committed) return FileCommitStatus::kInvalidRequest;
        out={}; out.objectId=committed->objectId; out.objectVersion=committed->objectVersion; out.metadataVersion=committed->metadataVersion; out.ownerId=committed->ownerId; out.fileName=committed->name; out.dirPath=committed->parentPath; out.fileHash=committed->contentHash; out.fileSize=committed->fileSize; out.chunkSize=committed->chunkSize; out.state=FileState::kAvailable; out.createdAt=unixSeconds();
        return FileCommitStatus::kCommitted;
    }
    const auto requestedAt = std::chrono::steady_clock::now();
    std::mutex& selectedMutex = useShardedLocks() ? sessionMutex(sessionId) : mutex_;
    const std::string lockLabel = useShardedLocks()
        ? std::string("commitFile.session.") + std::to_string(locks_.sessionShard(sessionId))
        : __func__;
    GatewayMutexGuard lock(selectedMutex, lockLabel.c_str());
    const auto lockedAt = std::chrono::steady_clock::now();
    if(timing != nullptr) {
        timing->mutexWaitUs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            lockedAt - requestedAt).count());
    }
    const auto sessionIt = sessions_.find(sessionId);
    if(sessionIt == sessions_.end() || sessionIt->second.completed.size() != sessionIt->second.totalChunks)
    {
        return FileCommitStatus::kInvalidRequest;
    }
    std::string legacyManifest;
    std::vector<ChunkRouteRequest> manifestChunks;
    out = {};
    out.ownerId = sessionIt->second.ownerId; 
    out.fileName = sessionIt->second.fileName; 
    out.dirPath = sessionIt->second.dirPath; 
    out.fileSize = sessionIt->second.fileSize; 
    out.chunkSize = sessionIt->second.chunkSize; 
    out.createdAt = unixSeconds(); 
    out.state = FileState::kAvailable;
    for(int i = 0; i < sessionIt->second.totalChunks; i++)
    {
        auto chunk = sessionIt->second.completed.find(i);
        if(chunk == sessionIt->second.completed.end()) return FileCommitStatus::kInvalidRequest;
        out.chunkHashes.push_back(chunk->second.chunkHash);
        legacyManifest += chunk->second.chunkHash + ":" + std::to_string(chunk->second.size) + ";";
        manifestChunks.push_back({static_cast<uint32_t>(i), chunk->second.chunkHash, chunk->second.size});
        ChunkRoute route;
        if (!getRouteLocked(chunk->second.chunkHash, route) || route.replicas.size() < replicationFactor_) {
            out.state = FileState::kProtecting;
        }
    }
    // Sessions created by the preflight API have a browser-provided canonical
    // manifest. Older saved sessions keep their original V2 identity so they
    // remain resumable after this upgrade.
    if (!sessionIt->second.manifestHash.empty()) {
        const std::string calculated = manifestHash(out.fileSize, out.chunkSize, manifestChunks);
        if (calculated.empty() || calculated != sessionIt->second.manifestHash) {
            return FileCommitStatus::kInvalidRequest;
        }
        out.fileHash = calculated;
    } else {
        out.fileHash = sha256Hex(legacyManifest.data(), legacyManifest.size());
    }
    std::string parentPath;
    std::string name;
    if (!normalizeDirectoryPath(out.dirPath, parentPath) || !normalizeEntryName(out.fileName, name)) {
        return FileCommitStatus::kInvalidRequest;
    }
    out.dirPath = parentPath;
    out.fileName = name;

    // Object and namespace/path records are both owned by ObjectShard.  A
    // commit may touch two different shards, so acquire them in shard-number
    // order after the SessionShard.  In global mode the compatibility guard
    // above already serializes the operation and no extra lock is needed.
    std::unique_ptr<GatewayShardMultiLock> objectAndPathLock;
    if (useShardedLocks()) {
        const std::string objectKey = sessionIt->second.objectId.empty()
            ? (sessionId + "#object") : sessionIt->second.objectId;
        const std::string pathKeyForShard = catalogPathKey(out.ownerId, childPath(parentPath, name));
        objectAndPathLock = std::make_unique<GatewayShardMultiLock>(
            objectMutex(objectKey), locks_.objectShard(objectKey),
            objectMutex(pathKeyForShard), locks_.objectShard(pathKeyForShard));
    }

    const std::string pathKey = catalogPathKey(out.ownerId, childPath(parentPath, name));
    std::string existingObjectId;
    const leveldb::Status pathStatus = db_->Get(leveldb::ReadOptions(), "path:" + pathKey, &existingObjectId);
    if (pathStatus.ok()) {
        // A client may lose the first successful response and retry the same
        // Session commit.  Treat that as the same business operation, while
        // keeping a different Session/object at the same path as a conflict.
        ObjectMeta existingObject;
        if (sessionIt->second.objectId.empty() ||
            existingObjectId != sessionIt->second.objectId ||
            !getObjectLocked(existingObjectId, existingObject) ||
            existingObject.fileHash != out.fileHash) {
            return FileCommitStatus::kPathConflict;
        }
        out.objectId = existingObject.objectId;
        out.objectVersion = existingObject.objectVersion;
        out.metadataVersion = existingObject.metadataVersion;
        out.state = existingObject.state;
        out.createdAt = existingObject.createdAt;
        return FileCommitStatus::kCommitted;
    }
    if (!pathStatus.IsNotFound()) return FileCommitStatus::kInvalidRequest;

    ObjectMeta object;
    object.objectId = sessionIt->second.objectId.empty()
        ? randomId() : sessionIt->second.objectId;
    if (object.objectId.empty()) return FileCommitStatus::kInvalidRequest;
    object.ownerId = out.ownerId;
    object.objectVersion = sessionIt->second.objectVersion;
    object.metadataVersion = sessionIt->second.metadataVersion;
    object.parentPath = parentPath;
    object.name = name;
    object.fileHash = out.fileHash;
    object.fileSize = out.fileSize;
    object.state = out.state;
    object.createdAt = out.createdAt;
    out.objectId = object.objectId;
    out.objectVersion = object.objectVersion;
    out.metadataVersion = object.metadataVersion;

    leveldb::WriteBatch batch;
    std::string current = "/";
    for (const auto& component : split(parentPath.substr(1), '/')) {
        if (component.empty()) continue;
        current = childPath(current, component);
        const std::string directoryKey = catalogPathKey(out.ownerId, current);
        std::string existingDirectory;
        const leveldb::Status directoryStatus = db_->Get(
            leveldb::ReadOptions(), "dir:" + directoryKey, &existingDirectory);
        if (!directoryStatus.ok() && !directoryStatus.IsNotFound()) return FileCommitStatus::kInvalidRequest;
        if (directoryStatus.IsNotFound()) {
            DirectoryMeta directory{out.ownerId, current, out.createdAt};
            batch.Put("dir:" + directoryKey, directoryValue(directory));
        }
    }
    std::string existingFile;
    const leveldb::Status fileStatus = db_->Get(leveldb::ReadOptions(), "f:" + out.fileHash, &existingFile);
    if (!fileStatus.ok() && !fileStatus.IsNotFound()) return FileCommitStatus::kInvalidRequest;
    if (fileStatus.IsNotFound()) {
        batch.Put("f:" + out.fileHash, fileValue(out));
    }
    batch.Put("obj:" + object.objectId, objectValue(object));
    batch.Put("path:" + pathKey, object.objectId);
    if(legacyAiOutboxEnabled()) {
        media::AiIndexEvent aiEvent;
        aiEvent.eventId = object.objectId;
        aiEvent.objectId = object.objectId;
        aiEvent.objectVersion = object.objectVersion;
        aiEvent.metadataVersion = object.metadataVersion;
        aiEvent.objectKey = childPath(parentPath, name);
        aiEvent.fileHash = out.fileHash;
        aiEvent.fileSize = out.fileSize;
        aiEvent.occurredAt = out.createdAt;
        batch.Put("ai:" + aiEvent.eventId, media::serializeAiIndexEvent(aiEvent));
    }
    const auto writeStartedAt = std::chrono::steady_clock::now();
    if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return FileCommitStatus::kInvalidRequest;
    const auto writeFinishedAt = std::chrono::steady_clock::now();

    objectCache_.erase(object.objectId);
    fileCache_.erase(out.fileHash);
    catalogCache_.clear();
    manifestCache_.erase(out.fileHash);
    if(timing != nullptr) {
        timing->levelDbWriteUs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            writeFinishedAt - writeStartedAt).count());
        timing->criticalSectionUs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - lockedAt).count());
    }
    return FileCommitStatus::kCommitted;
}

FileCommitStatus GatewayState::commitDerivedUpload(const std::string& jobId,
                                                    const std::string& leaseToken,
                                                    const std::string& sessionId,
                                                    FileMeta& out)
{
    out = {};
    if(jobId.empty() || leaseToken.empty() || sessionId.empty()) return FileCommitStatus::kInvalidRequest;
    const int64_t now = unixSeconds();
    std::mutex& selectedMutex = useShardedLocks() ? sessionMutex(sessionId) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    const auto sessionIt = sessions_.find(sessionId);
    if(sessionIt == sessions_.end() || sessionIt->second.derivedJobId != jobId ||
       sessionIt->second.completed.size() != sessionIt->second.totalChunks) {
        return FileCommitStatus::kInvalidRequest;
    }
    media::MediaJob job;
    media::ThumbnailMeta thumbnail;
    if(!getMediaJobLocked(jobId, job) || job.type != media::JobType::kThumbnail ||
       job.state != media::JobState::kRunning || job.leaseToken != leaseToken ||
       job.leaseUntil < now || sessionIt->second.derivedProfile != job.profile ||
       !getThumbnailLocked(job.sourceFileHash, job.profile, thumbnail) || thumbnail.jobId != jobId) {
        return FileCommitStatus::kInvalidRequest;
    }

    std::vector<ChunkRouteRequest> manifestChunks;
    out.ownerId = sessionIt->second.ownerId;
    out.fileName = sessionIt->second.fileName;
    out.fileSize = sessionIt->second.fileSize;
    out.chunkSize = sessionIt->second.chunkSize;
    out.createdAt = now;
    out.state = FileState::kAvailable;
    for(uint32_t index = 0; index < sessionIt->second.totalChunks; ++index) {
        const auto completed = sessionIt->second.completed.find(index);
        if(completed == sessionIt->second.completed.end()) return FileCommitStatus::kInvalidRequest;
        out.chunkHashes.push_back(completed->second.chunkHash);
        manifestChunks.push_back({index, completed->second.chunkHash, completed->second.size});
        ChunkRoute route;
        if(!getRouteLocked(completed->second.chunkHash, route) || route.replicas.size() < replicationFactor_) {
            out.state = FileState::kProtecting;
        }
    }
    out.fileHash = manifestHash(out.fileSize, out.chunkSize, manifestChunks);
    if(out.fileHash.empty() || out.fileHash != sessionIt->second.manifestHash) {
        return FileCommitStatus::kInvalidRequest;
    }

    ObjectMeta object;
    object.objectId = sessionIt->second.objectId.empty()
        ? randomId() : sessionIt->second.objectId;
    if(object.objectId.empty()) return FileCommitStatus::kInvalidRequest;
    object.ownerId = out.ownerId;
    object.objectVersion = sessionIt->second.objectVersion;
    object.metadataVersion = sessionIt->second.metadataVersion;
    object.name = out.fileName;
    object.fileHash = out.fileHash;
    object.fileSize = out.fileSize;
    object.contentType = "image/jpeg";
    object.state = out.state;
    object.createdAt = out.createdAt;
    out.objectId = object.objectId;
    out.objectVersion = object.objectVersion;
    out.metadataVersion = object.metadataVersion;

    std::string existingFile;
    const leveldb::Status fileStatus = db_->Get(leveldb::ReadOptions(), "f:" + out.fileHash, &existingFile);
    if(!fileStatus.ok() && !fileStatus.IsNotFound()) return FileCommitStatus::kInvalidRequest;

    job.state = media::JobState::kReady;
    job.leaseUntil = 0;
    job.leaseToken.clear();
    job.nextRetryAt = 0;
    job.lastError.clear();
    job.updatedAt = now;
    thumbnail.state = media::JobState::kReady;
    thumbnail.derivedObjectId = object.objectId;
    thumbnail.derivedFileHash = out.fileHash;
    thumbnail.lastError.clear();
    thumbnail.updatedAt = now;

    leveldb::WriteBatch batch;
    if(fileStatus.IsNotFound()) batch.Put("f:" + out.fileHash, fileValue(out));
    batch.Put("obj:" + object.objectId, objectValue(object));
    batch.Put("j:" + job.jobId, media::serializeMediaJob(job));
    batch.Put(thumbnailKey(job.sourceFileHash, job.profile), media::serializeThumbnailMeta(thumbnail));
    if(!db_->Write(leveldb::WriteOptions(), &batch).ok()) return FileCommitStatus::kInvalidRequest;

    objectCache_.erase(object.objectId);
    fileCache_.erase(out.fileHash);
    catalogCache_.clear();
    manifestCache_.erase(out.fileHash);
    return FileCommitStatus::kCommitted;
}
bool GatewayState::getFile(const std::string& fileHash, FileMeta& out) const
{
    //false -- 没找到， true 找到
    std::mutex& selectedMutex = useShardedLocks() ? objectMutex(fileHash) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    return getFileLocked(fileHash, out);
}
bool GatewayState::getRoute(const std::string& chunkHash, ChunkRoute& out) const
{
    std::mutex& selectedMutex = useShardedLocks() ? objectMutex(chunkHash) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    return getRouteLocked(chunkHash, out);
}

bool GatewayState::buildManifestSnapshot(const std::string& fileHash,
                                         ManifestSnapshot& out) const
{
    // NodeRegistry is a separate ownership domain.  Copy the static node
    // records under its short-lived shared lock before taking ObjectShard;
    // never hold NodeRegistry while doing LevelDB/cache I/O.
    std::map<std::string, NodeRecord> nodeSnapshot;
    if (useShardedLocks()) {
        std::shared_lock<std::shared_mutex> nodeLock(locks_.nodes());
        nodeSnapshot = nodeRecords_;
    }
    std::mutex& selectedMutex = useShardedLocks() ? objectMutex(fileHash) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    if (const auto cached = manifestCache_.get(fileHash)) {
        out = *cached;
        return true;
    }
    FileMeta file;
    if (!getFileLocked(fileHash, file)) return false;

    ManifestSnapshot snapshot;
    snapshot.file = file;
    snapshot.routes.reserve(snapshot.file.chunkHashes.size());
    for(const auto& chunkHash : snapshot.file.chunkHashes) {
        ChunkRoute route;
        if (!getRouteLocked(chunkHash, route)) return false;
        snapshot.routes.push_back(route);
        for(const auto& nodeId : route.replicas) {
            if (useShardedLocks()) {
                const auto node = nodeSnapshot.find(nodeId);
                if (node == nodeSnapshot.end()) return false;
                snapshot.nodes.emplace(nodeId, node->second);
            } else {
                const auto node = nodeRecords_.find(nodeId);
                if (node == nodeRecords_.end()) return false;
                snapshot.nodes.emplace(nodeId, node->second);
            }
        }
    }
    manifestCache_.put(fileHash, std::make_shared<const ManifestSnapshot>(snapshot),
                       estimatedManifestBytes(snapshot), kManifestCacheTtlSeconds);
    out = std::move(snapshot);
    return true;
}

bool GatewayState::buildObjectReadDescriptor(
    const std::string& objectId, control::ObjectReadDescriptor& out) const
{
    if (remoteMetadata_) {
        const auto object = remoteMetadata_->object(objectId); if(!object || object->state != metadata::ObjectState::kCommitted) return false;
        const auto descriptor = remoteMetadata_->readDescriptor(objectId, object->objectVersion); if(!descriptor) return false;
        const auto metadataNodes = remoteMetadata_->nodes(); std::map<std::string, metadata::NodeRecord> nodes;
        for(const auto& node : metadataNodes) nodes.emplace(node.nodeId, node);
        out = {}; out.objectId=descriptor->object.objectId; out.objectVersion=descriptor->object.objectVersion; out.metadataVersion=descriptor->object.metadataVersion; out.fileSize=descriptor->object.fileSize; out.chunkSize=descriptor->object.chunkSize;
        for(const auto& source : descriptor->chunks) {
            control::ChunkReadDescriptor chunk; chunk.index=source.index; chunk.chunkId=source.storageIdentity; chunk.storageIdentity=source.storageIdentity; chunk.size=source.size; chunk.checksumType=metadata::toString(source.checksumType); chunk.checksumDigest=source.checksumDigest; chunk.generation=source.generation;
            for(const auto& replica : source.replicas) { const auto it=nodes.find(replica.nodeId); if(it==nodes.end()) return false; chunk.replicas.push_back({it->second.nodeId,it->second.address,it->second.dataPort}); }
            if(chunk.replicas.empty()) return false; out.chunks.push_back(std::move(chunk));
        }
        return !out.chunks.empty();
    }
    out = {};
    std::map<std::string, NodeRecord> nodeSnapshot;
    if (useShardedLocks()) {
        std::shared_lock<std::shared_mutex> nodeLock(locks_.nodes());
        nodeSnapshot = nodeRecords_;
    }
    std::mutex& selectedMutex = useShardedLocks() ? objectMutex(objectId) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    ObjectMeta object;
    if(!getObjectLocked(objectId, object)) return false;
    FileMeta file;
    if(!getFileLocked(object.fileHash, file)) return false;

    control::ObjectReadDescriptor descriptor;
    descriptor.objectId = object.objectId;
    descriptor.objectVersion = object.objectVersion;
    descriptor.metadataVersion = object.metadataVersion;
    descriptor.fileSize = object.fileSize;
    descriptor.chunkSize = file.chunkSize;
    descriptor.chunks.reserve(file.chunkHashes.size());
    for(size_t index = 0; index < file.chunkHashes.size(); ++index) {
        ChunkRoute route;
        if(!getRouteLocked(file.chunkHashes[index], route)) return false;
        control::ChunkReadDescriptor chunk;
        chunk.index = static_cast<uint32_t>(index);
        chunk.chunkId = route.chunkId.empty() ? route.chunkHash : route.chunkId;
        chunk.storageIdentity = route.identityScheme == "opaque-chunk-id"
            ? chunk.chunkId : route.chunkHash;
        chunk.size = route.size;
        chunk.checksumType = route.checksumType;
        chunk.checksumDigest = route.checksumDigest;
        chunk.generation = route.generation;
        for(const auto& nodeId : route.replicas) {
            const NodeRecord* node = nullptr;
            if (useShardedLocks()) {
                const auto found = nodeSnapshot.find(nodeId);
                if (found == nodeSnapshot.end()) return false;
                node = &found->second;
            } else {
                const auto found = nodeRecords_.find(nodeId);
                if (found == nodeRecords_.end()) return false;
                node = &found->second;
            }
            chunk.replicas.push_back({node->nodeId, node->address, node->httpPort});
        }
        if(chunk.storageIdentity.empty() || chunk.replicas.empty()) return false;
        descriptor.chunks.push_back(std::move(chunk));
    }
    out = std::move(descriptor);
    return true;
}

bool GatewayState::createDirectory(const std::string& parentPath, const std::string& name,
                                   DirectoryMeta* out)
{
    std::string parent;
    std::string entryName;
    if (!normalizeDirectoryPath(parentPath, parent) || !normalizeEntryName(name, entryName)) return false;

    const std::string lockKey = catalogPathKey("admin", parent);
    if (remoteMetadata_) {
        const std::string path = childPath(parent, entryName);
        if (parent != "/") {
            const auto slash = parent.find_last_of('/');
            const std::string grandparent = slash <= 0 ? "/" : parent.substr(0, slash);
            const auto children = remoteMetadata_->directories("admin", grandparent);
            const bool parentExists = std::any_of(children.begin(), children.end(),
                [&](const metadata::DirectoryRecord& directory) { return directory.path == parent; });
            if(!parentExists) return false;
        }
        metadata::MetadataCommand command;
        command.commandId = metadata::MetadataClient::newCommandId("create-directory");
        command.type = metadata::MetadataCommandType::kCreateDirectory;
        command.actorType = "gateway"; command.actorId = "admin"; command.issuedAt = unixSeconds();
        command.payload = metadata::CreateDirectoryPayload{"admin", path, unixSeconds()};
        const auto result = remoteMetadata_->propose(command);
        if(result.status != metadata::ApplyStatus::kOk && result.status != metadata::ApplyStatus::kAlreadyApplied) return false;
        if(out) *out = DirectoryMeta{"admin", path, unixSeconds()};
        return true;
    }
    std::mutex& selectedMutex = useShardedLocks() ? objectMutex(lockKey) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    DirectoryMeta parentDirectory;
    if (parent != "/" && !getDirectoryLocked("admin", parent, parentDirectory)) return false;
    const std::string path = childPath(parent, entryName);
    const std::string key = catalogPathKey("admin", path);
    std::string existingDirectory;
    const leveldb::Status directoryStatus = db_->Get(leveldb::ReadOptions(), "dir:" + key, &existingDirectory);
    if (directoryStatus.ok() || !directoryStatus.IsNotFound()) return false;
    std::string existingObjectId;
    const leveldb::Status objectStatus = db_->Get(leveldb::ReadOptions(), "path:" + key, &existingObjectId);
    if (objectStatus.ok() || !objectStatus.IsNotFound()) return false;

    DirectoryMeta directory;
    directory.path = path;
    directory.createdAt = unixSeconds();
    if (!persistDirectoryLocked(directory)) return false;
    catalogCache_.erase(parent);
    if (out != nullptr) *out = directory;
    return true;
}

bool GatewayState::buildCatalogSnapshotLocked(const std::string& normalized,
                                              CatalogSnapshot& out) const
{
    CatalogSnapshot snapshot;
    snapshot.path = normalized;
    snapshot.breadcrumbs = breadcrumbsFor(normalized);

    auto directoryIt = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (directoryIt->Seek("dir:"); directoryIt->Valid() &&
         directoryIt->key().ToString().rfind("dir:", 0) == 0; directoryIt->Next()) {
        DirectoryMeta directory;
        if (!parseDirectory(directoryIt->value().ToString(), directory)) return false;
        if (directory.ownerId == "admin" && directory.path != normalized &&
            directory.path.rfind(normalized == "/" ? "/" : normalized + "/", 0) == 0) {
            const std::string suffix = directory.path.substr(normalized == "/" ? 1 : normalized.size() + 1);
            if (suffix.find('/') == std::string::npos) snapshot.directories.push_back(std::move(directory));
        }
    }
    if (!directoryIt->status().ok()) return false;

    auto objectIt = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (objectIt->Seek("obj:"); objectIt->Valid() &&
         objectIt->key().ToString().rfind("obj:", 0) == 0; objectIt->Next()) {
        ObjectMeta object;
        if (!parseObject(objectIt->value().ToString(), object)) return false;
        if (object.ownerId == "admin" && object.parentPath == normalized) {
            media::ThumbnailMeta thumbnail;
            if(getThumbnailLocked(object.fileHash, "thumb-512-jpeg-v1", thumbnail)) {
                snapshot.thumbnailsByFileHash.emplace(object.fileHash, std::move(thumbnail));
            }
            media::ThumbnailMeta preview;
            if(getThumbnailLocked(object.fileHash, "preview-2048-jpeg-v1", preview)) {
                snapshot.previewsByFileHash.emplace(object.fileHash, std::move(preview));
            }
            snapshot.files.push_back(std::move(object));
        }
    }
    if (!objectIt->status().ok()) return false;

    std::sort(snapshot.directories.begin(), snapshot.directories.end(),
              [](const DirectoryMeta& left, const DirectoryMeta& right) { return left.path < right.path; });
    std::sort(snapshot.files.begin(), snapshot.files.end(),
              [](const ObjectMeta& left, const ObjectMeta& right) { return left.name < right.name; });
    out = std::move(snapshot);
    return true;
}

bool GatewayState::listCatalog(const std::string& path, CatalogSnapshot& out) const
{
    std::string normalized;
    if (!normalizeDirectoryPath(path, normalized)) return false;
    if (remoteMetadata_) {
        out = {}; out.path = normalized; out.breadcrumbs = breadcrumbsFor(normalized);
        if(normalized != "/" && remoteMetadata_->directories("admin", normalized).empty()) {
            // The endpoint returns children, so verify that the requested path
            // itself exists by checking its parent list (root is implicit).
            const auto slash = normalized.find_last_of('/');
            const std::string parent = slash == 0 ? "/" : normalized.substr(0, slash);
            const auto children = remoteMetadata_->directories("admin", parent);
            const bool exists = std::any_of(children.begin(), children.end(),
                [&](const metadata::DirectoryRecord& directory) { return directory.path == normalized; });
            if(!exists) return false;
        }
        for(const auto& directory : remoteMetadata_->directories("admin", normalized))
            out.directories.push_back({directory.ownerId, directory.path, directory.createdAt});
        for(const auto& object : remoteMetadata_->objects("admin", normalized)) {
            ObjectMeta converted; converted.objectId=object.objectId; converted.objectVersion=object.objectVersion;
            converted.metadataVersion=object.metadataVersion; converted.ownerId=object.ownerId; converted.parentPath=object.parentPath;
            converted.name=object.name; converted.fileHash=object.contentHash; converted.fileSize=object.fileSize;
            converted.contentType="application/octet-stream"; converted.state=object.state==metadata::ObjectState::kCommitted ? FileState::kAvailable : FileState::kProtecting;
            converted.createdAt=unixSeconds(); out.files.push_back(std::move(converted));
        }
        return true;
    }
    std::mutex& selectedMutex = useShardedLocks() ? objectMutex(catalogPathKey("admin", normalized)) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    if (const auto cached = catalogCache_.get(normalized)) {
        out = *cached;
        return true;
    }
    DirectoryMeta directory;
    if (normalized != "/" && !getDirectoryLocked("admin", normalized, directory)) return false;
    CatalogSnapshot snapshot;
    if (!buildCatalogSnapshotLocked(normalized, snapshot)) return false;
    catalogCache_.put(normalized, std::make_shared<const CatalogSnapshot>(snapshot),
                      estimatedCatalogBytes(snapshot), kCatalogCacheTtlSeconds);
    out = std::move(snapshot);
    return true;
}

bool GatewayState::getObject(const std::string& objectId, ObjectMeta& out) const
{
    if (remoteMetadata_) {
        const auto object = remoteMetadata_->object(objectId);
        // A replicated delete first moves the authoritative metadata record
        // to kDeleting while DataNode cleanup is still asynchronous.  Do not
        // expose that tombstone through the ordinary object lookup API: the
        // catalog/read contract is that only committed objects are readable.
        if(!object || object->state != metadata::ObjectState::kCommitted) return false;
        out={}; out.objectId=object->objectId; out.objectVersion=object->objectVersion; out.metadataVersion=object->metadataVersion; out.ownerId=object->ownerId; out.parentPath=object->parentPath; out.name=object->name; out.fileHash=object->contentHash; out.fileSize=object->fileSize; out.contentType="application/octet-stream"; out.state=object->state==metadata::ObjectState::kCommitted?FileState::kAvailable:FileState::kProtecting; out.createdAt=unixSeconds(); return true;
    }
    std::mutex& selectedMutex = useShardedLocks() ? objectMutex(objectId) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    return getObjectLocked(objectId, out);
}

ThumbnailEnqueueResult GatewayState::enqueueThumbnail(const std::string& sourceFileHash,
                                                       const std::string& profile, int64_t now)
{
    ThumbnailEnqueueResult result;
    if (sourceFileHash.empty() || profile.empty()) return result;

    std::mutex& selectedMutex = useShardedLocks() ? objectMutex(sourceFileHash + "\n" + profile) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    if (!db_) return result;

    media::ThumbnailMeta thumbnail;
    if (getThumbnailLocked(sourceFileHash, profile, thumbnail)) {
        media::MediaJob existing;
        if (!getMediaJobLocked(thumbnail.jobId, existing)) return result;
        result.job = existing;

        if (existing.state == media::JobState::kReady ||
            existing.state == media::JobState::kUnsupported ||
            (existing.state == media::JobState::kPending) ||
            (existing.state == media::JobState::kRunning && existing.leaseUntil > now) ||
            (existing.state == media::JobState::kFailed && existing.nextRetryAt > now)) {
            return result;
        }

        existing.state = media::JobState::kPending;
        existing.leaseUntil = 0;
        existing.leaseToken.clear();
        existing.nextRetryAt = now;
        existing.updatedAt = now;
        thumbnail.state = media::JobState::kPending;
        thumbnail.lastError.clear();
        thumbnail.updatedAt = now;

        leveldb::WriteBatch batch;
        batch.Put("j:" + existing.jobId, media::serializeMediaJob(existing));
        batch.Put(thumbnailKey(sourceFileHash, profile), media::serializeThumbnailMeta(thumbnail));
        if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return ThumbnailEnqueueResult{};
        result.job = std::move(existing);
        result.publishRequired = true;
        return result;
    }

    media::MediaJob job;
    job.jobId = randomId();
    if (job.jobId.empty()) return result;
    job.type = media::JobType::kThumbnail;
    job.sourceFileHash = sourceFileHash;
    job.profile = profile;
    job.state = media::JobState::kPending;
    job.nextRetryAt = now;
    job.createdAt = now;
    job.updatedAt = now;

    thumbnail.sourceFileHash = sourceFileHash;
    thumbnail.profile = profile;
    thumbnail.state = media::JobState::kPending;
    thumbnail.jobId = job.jobId;
    thumbnail.updatedAt = now;

    leveldb::WriteBatch batch;
    batch.Put("j:" + job.jobId, media::serializeMediaJob(job));
    batch.Put(thumbnailKey(sourceFileHash, profile), media::serializeThumbnailMeta(thumbnail));
    if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return result;
    result.job = std::move(job);
    result.publishRequired = true;
    return result;
}

bool GatewayState::getMediaJob(const std::string& jobId, media::MediaJob& out) const
{
    std::mutex& selectedMutex = useShardedLocks() ? sessionMutex(jobId) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    return getMediaJobLocked(jobId, out);
}

bool GatewayState::getThumbnail(const std::string& sourceFileHash, const std::string& profile,
                                media::ThumbnailMeta& out) const
{
    std::mutex& selectedMutex = useShardedLocks() ? objectMutex(sourceFileHash + "\n" + profile) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    return getThumbnailLocked(sourceFileHash, profile, out);
}

bool GatewayState::claimMediaJob(const std::string& jobId, int64_t now, int64_t leaseSeconds,
                                 media::MediaJob& out)
{
    if (jobId.empty() || leaseSeconds <= 0 || leaseSeconds > 3600) return false;
    std::mutex& selectedMutex = useShardedLocks() ? sessionMutex(jobId) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);

    media::MediaJob job;
    if (!getMediaJobLocked(jobId, job) || !isClaimableMediaJob(job, now)) return false;
    media::ThumbnailMeta thumbnail;
    if (job.type != media::JobType::kThumbnail ||
        !getThumbnailLocked(job.sourceFileHash, job.profile, thumbnail) || thumbnail.jobId != job.jobId) {
        return false;
    }

    job.state = media::JobState::kRunning;
    ++job.attempts;
    job.leaseUntil = now + leaseSeconds;
    job.leaseToken = randomId();
    if (job.leaseToken.empty()) return false;
    job.nextRetryAt = 0;
    job.updatedAt = now;
    thumbnail.state = media::JobState::kRunning;
    thumbnail.lastError.clear();
    thumbnail.updatedAt = now;

    leveldb::WriteBatch batch;
    batch.Put("j:" + job.jobId, media::serializeMediaJob(job));
    batch.Put(thumbnailKey(job.sourceFileHash, job.profile), media::serializeThumbnailMeta(thumbnail));
    if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return false;
    out = std::move(job);
    return true;
}

bool GatewayState::completeMediaJob(const std::string& jobId, const std::string& leaseToken,
                                    const std::string& derivedObjectId,
                                    const std::string& derivedFileHash, int64_t now)
{
    if (jobId.empty() || leaseToken.empty() || derivedObjectId.empty() || derivedFileHash.empty()) return false;
    std::mutex& selectedMutex = useShardedLocks() ? sessionMutex(jobId) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);

    media::MediaJob job;
    if (!getMediaJobLocked(jobId, job) || job.state != media::JobState::kRunning ||
        job.leaseToken != leaseToken || job.leaseUntil < now) {
        return false;
    }
    media::ThumbnailMeta thumbnail;
    if (job.type != media::JobType::kThumbnail ||
        !getThumbnailLocked(job.sourceFileHash, job.profile, thumbnail) || thumbnail.jobId != job.jobId) {
        return false;
    }

    job.state = media::JobState::kReady;
    job.leaseUntil = 0;
    job.leaseToken.clear();
    job.nextRetryAt = 0;
    job.lastError.clear();
    job.updatedAt = now;
    thumbnail.state = media::JobState::kReady;
    thumbnail.derivedObjectId = derivedObjectId;
    thumbnail.derivedFileHash = derivedFileHash;
    thumbnail.lastError.clear();
    thumbnail.updatedAt = now;

    leveldb::WriteBatch batch;
    batch.Put("j:" + job.jobId, media::serializeMediaJob(job));
    batch.Put(thumbnailKey(job.sourceFileHash, job.profile), media::serializeThumbnailMeta(thumbnail));
    return db_->Write(leveldb::WriteOptions(), &batch).ok();
}

bool GatewayState::failMediaJob(const std::string& jobId, const std::string& leaseToken,
                                bool unsupported, const std::string& error, int64_t nextRetryAt,
                                int64_t now)
{
    if (jobId.empty() || leaseToken.empty() || error.empty()) return false;
    std::mutex& selectedMutex = useShardedLocks() ? sessionMutex(jobId) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);

    media::MediaJob job;
    if (!getMediaJobLocked(jobId, job) || job.state != media::JobState::kRunning ||
        job.leaseToken != leaseToken || job.leaseUntil < now) {
        return false;
    }
    media::ThumbnailMeta thumbnail;
    if (job.type != media::JobType::kThumbnail ||
        !getThumbnailLocked(job.sourceFileHash, job.profile, thumbnail) || thumbnail.jobId != job.jobId) {
        return false;
    }

    job.state = unsupported ? media::JobState::kUnsupported : media::JobState::kFailed;
    job.leaseUntil = 0;
    job.leaseToken.clear();
    job.nextRetryAt = unsupported ? 0 : nextRetryAt;
    job.lastError = error;
    job.updatedAt = now;
    thumbnail.state = job.state;
    thumbnail.lastError = error;
    thumbnail.updatedAt = now;

    leveldb::WriteBatch batch;
    batch.Put("j:" + job.jobId, media::serializeMediaJob(job));
    batch.Put(thumbnailKey(job.sourceFileHash, job.profile), media::serializeThumbnailMeta(thumbnail));
    return db_->Write(leveldb::WriteOptions(), &batch).ok();
}

bool GatewayState::deferMediaJobDispatch(const std::string& jobId, int64_t nextDispatchAt)
{
    if (jobId.empty() || nextDispatchAt <= 0) return false;
    std::mutex& selectedMutex = useShardedLocks() ? sessionMutex(jobId) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);

    media::MediaJob job;
    if (!getMediaJobLocked(jobId, job) || job.state != media::JobState::kPending) return false;
    job.nextRetryAt = nextDispatchAt;
    if (!db_->Put(leveldb::WriteOptions(), "j:" + job.jobId,
                  media::serializeMediaJob(job)).ok()) {
        return false;
    }
    return true;
}

std::vector<media::MediaJob> GatewayState::dueMediaJobs(int64_t now, size_t maxJobs) const
{
    GatewayMutexGuard lock(mutex_, __func__);
    std::vector<media::MediaJob> result;
    if (!db_ || maxJobs == 0) return result;

    auto iterator = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (iterator->Seek("j:"); iterator->Valid() &&
         iterator->key().ToString().rfind("j:", 0) == 0 && result.size() < maxJobs;
         iterator->Next()) {
        media::MediaJob job;
        if (!media::parseMediaJob(iterator->value().ToString(), job)) return {};
        const bool needsDispatch =
            (job.state == media::JobState::kPending && job.nextRetryAt <= now) ||
            (job.state == media::JobState::kFailed && job.nextRetryAt <= now) ||
            (job.state == media::JobState::kRunning && job.leaseUntil <= now);
        if (needsDispatch) result.push_back(std::move(job));
    }
    if (!iterator->status().ok()) return {};
    return result;
}

std::vector<media::AiIndexEvent> GatewayState::dueAiIndexEvents(int64_t now,
                                                                 size_t maxEvents) const
{
    GatewayMutexGuard lock(mutex_, __func__);
    std::vector<media::AiIndexEvent> result;
    if(!db_ || maxEvents == 0) return result;

    auto iterator = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for(iterator->Seek("ai:"); iterator->Valid() &&
        iterator->key().ToString().rfind("ai:", 0) == 0 && result.size() < maxEvents;
        iterator->Next()) {
        media::AiIndexEvent event;
        if(!media::parseAiIndexEvent(iterator->value().ToString(), event)) return {};
        if(event.publishedAt == 0 && event.occurredAt <= now) result.push_back(std::move(event));
    }
    if(!iterator->status().ok()) return {};
    return result;
}

bool GatewayState::markAiIndexEventPublished(const std::string& eventId, int64_t now)
{
    if(eventId.empty() || now <= 0) return false;
    GatewayMutexGuard lock(mutex_, __func__);
    if(!db_) return false;
    std::string value;
    if(!db_->Get(leveldb::ReadOptions(), "ai:" + eventId, &value).ok()) return false;
    media::AiIndexEvent event;
    if(!media::parseAiIndexEvent(value, event)) return false;
    if(event.publishedAt != 0) return true;
    event.publishedAt = now;
    return db_->Put(leveldb::WriteOptions(), "ai:" + eventId,
                    media::serializeAiIndexEvent(event)).ok();
}

ObjectMetaCache::Stats GatewayState::objectCacheStats() const
{
    GatewayMutexGuard lock(mutex_, __func__);
    return objectCache_.stats();
}

CatalogCache::Stats GatewayState::catalogCacheStats() const
{
    GatewayMutexGuard lock(mutex_, __func__);
    return catalogCache_.stats();
}

ManifestCache::Stats GatewayState::manifestCacheStats() const
{
    GatewayMutexGuard lock(mutex_, __func__);
    return manifestCache_.stats();
}

MetadataCacheUsage GatewayState::metadataCacheUsage() const
{
    GatewayMutexGuard lock(mutex_, __func__);
    return {objectCache_.size(), fileCache_.size(), routeCache_.size(),
            catalogCache_.size(), manifestCache_.size()};
}

DeleteStatus GatewayState::deleteObject(const std::string& objectId, uint64_t expectedObjectVersion)
{
    if (objectId.empty()) return DeleteStatus::kInvalidRequest;
    if (remoteMetadata_) {
        const auto object = remoteMetadata_->object(objectId);
        if(!object || (expectedObjectVersion != 0 && object->objectVersion != expectedObjectVersion)) return DeleteStatus::kNotFound;
        metadata::MetadataCommand command; command.commandId = metadata::MetadataClient::newCommandId("delete-object");
        command.type = metadata::MetadataCommandType::kDeleteObject; command.actorType = "gateway"; command.actorId = "admin";
        command.issuedAt = unixSeconds(); command.payload = metadata::DeleteObjectPayload{objectId, object->objectVersion};
        const auto result = remoteMetadata_->propose(command);
        if(result.status == metadata::ApplyStatus::kNotFound) return DeleteStatus::kNotFound;
        if(result.status != metadata::ApplyStatus::kOk && result.status != metadata::ApplyStatus::kAlreadyApplied) return DeleteStatus::kInvalidRequest;
        return DeleteStatus::kDeleted;
    }
    std::mutex& selectedMutex = useShardedLocks() ? objectMutex(objectId) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    ObjectMeta object;
    if (!getObjectLocked(objectId, object)) return DeleteStatus::kNotFound;
    if (expectedObjectVersion != 0 && object.objectVersion != expectedObjectVersion) {
        return DeleteStatus::kNotFound;
    }
    return deleteCatalogEntriesLocked({objectId}, {});
}

DeleteStatus GatewayState::deleteDirectory(const std::string& path)
{
    std::string normalized;
    if (!normalizeDirectoryPath(path, normalized) || normalized == "/") {
        return DeleteStatus::kInvalidRequest;
    }

    if (remoteMetadata_) {
        const auto slash = normalized.find_last_of('/');
        const std::string parent = slash == 0 ? "/" : normalized.substr(0, slash);
        const auto children = remoteMetadata_->directories("admin", parent);
        const bool exists = std::any_of(children.begin(), children.end(),
            [&](const metadata::DirectoryRecord& directory) { return directory.path == normalized; });
        if(!exists) return DeleteStatus::kNotFound;
        metadata::MetadataCommand command; command.commandId = metadata::MetadataClient::newCommandId("delete-directory");
        command.type = metadata::MetadataCommandType::kDeleteDirectory; command.actorType = "gateway"; command.actorId = "admin";
        command.issuedAt = unixSeconds(); command.payload = metadata::DeleteDirectoryPayload{"admin", normalized};
        const auto result = remoteMetadata_->propose(command);
        if(result.status == metadata::ApplyStatus::kNotFound) return DeleteStatus::kNotFound;
        return (result.status == metadata::ApplyStatus::kOk || result.status == metadata::ApplyStatus::kAlreadyApplied)
            ? DeleteStatus::kDeleted : DeleteStatus::kInvalidRequest;
    }

    std::mutex& selectedMutex = useShardedLocks() ? objectMutex(catalogPathKey("admin", normalized)) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    const std::string rootKey = catalogPathKey("admin", normalized);
    DirectoryMeta rootDirectory;
    if (!getDirectoryLocked("admin", normalized, rootDirectory)) return DeleteStatus::kNotFound;

    std::vector<std::string> objectIds;
    std::vector<std::string> directoryKeys;
    const std::string prefix = normalized + "/";
    auto objectIt = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (objectIt->Seek("obj:"); objectIt->Valid() && objectIt->key().ToString().rfind("obj:", 0) == 0; objectIt->Next()) {
        ObjectMeta object;
        if (!parseObject(objectIt->value().ToString(), object)) return DeleteStatus::kInvalidRequest;
        const std::string objectId = object.objectId;
        if (object.ownerId == "admin" &&
            (object.parentPath == normalized || object.parentPath.rfind(prefix, 0) == 0)) {
            objectIds.push_back(objectId);
        }
    }
    if (!objectIt->status().ok()) return DeleteStatus::kInvalidRequest;
    auto directoryIt = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (directoryIt->Seek("dir:"); directoryIt->Valid() && directoryIt->key().ToString().rfind("dir:", 0) == 0; directoryIt->Next()) {
        DirectoryMeta directory;
        if (!parseDirectory(directoryIt->value().ToString(), directory)) return DeleteStatus::kInvalidRequest;
        const std::string key = catalogPathKey(directory.ownerId, directory.path);
        if (directory.ownerId == "admin" &&
            (directory.path == normalized || directory.path.rfind(prefix, 0) == 0)) {
            directoryKeys.push_back(key);
        }
    }
    if (!directoryIt->status().ok()) return DeleteStatus::kInvalidRequest;
    return deleteCatalogEntriesLocked(objectIds, directoryKeys);
}

std::vector<DeleteTaskSnapshot> GatewayState::pendingDeletesForNode(const std::string& nodeId) const
{
    if (remoteMetadata_) {
        std::vector<DeleteTaskSnapshot> pending;
        for(const auto& task : remoteMetadata_->deleteTasks(nodeId)) {
            std::vector<std::string> nodeIds;
            for(const auto& replica : task.pendingReplicas) nodeIds.push_back(replica.nodeId);
            pending.push_back({task.objectId + "\n" + std::to_string(task.chunkIndex), task.storageIdentity, std::move(nodeIds)});
        }
        return pending;
    }
    GatewayMutexGuard lock(mutex_, __func__);
    std::vector<DeleteTaskSnapshot> pending;
    for (const auto& [chunkHash, task] : deleteTasks_) {
        if (std::find(task.pendingNodeIds.begin(), task.pendingNodeIds.end(), nodeId) !=
            task.pendingNodeIds.end()) {
            pending.push_back({chunkHash, task.storageIdentity, task.pendingNodeIds});
        }
    }
    return pending;
}

bool GatewayState::acknowledgeDelete(const std::string& chunkHash, const std::string& nodeId)
{
    if (chunkHash.empty() || nodeId.empty()) return false;
    if (remoteMetadata_) {
        const auto separator = chunkHash.rfind('\n');
        if(separator == std::string::npos) return false;
        const std::string objectId = chunkHash.substr(0, separator);
        uint32_t index = 0; try { index = static_cast<uint32_t>(std::stoul(chunkHash.substr(separator + 1))); } catch(...) { return false; }
        const auto task = remoteMetadata_->deleteTasks(nodeId);
        auto found = std::find_if(task.begin(), task.end(), [&](const metadata::DeleteTaskRecord& candidate) {
            return candidate.objectId == objectId && candidate.chunkIndex == index;
        });
        if(found == task.end()) return true;
        auto replica = std::find_if(found->pendingReplicas.begin(), found->pendingReplicas.end(),
            [&](const metadata::LeaseTarget& target) { return target.nodeId == nodeId; });
        if(replica == found->pendingReplicas.end()) return true;
        metadata::MetadataCommand command; command.commandId = metadata::MetadataClient::newCommandId("ack-delete");
        command.type = metadata::MetadataCommandType::kAcknowledgeDelete; command.actorType = "gateway"; command.actorId = nodeId;
        command.nodeEpoch = replica->nodeEpoch; command.issuedAt = unixSeconds();
        command.payload = metadata::AcknowledgeDeletePayload{objectId, found->objectVersion, index, nodeId, replica->nodeEpoch};
        const auto result = remoteMetadata_->propose(command);
        return result.status == metadata::ApplyStatus::kOk || result.status == metadata::ApplyStatus::kAlreadyApplied;
    }
    std::mutex& selectedMutex = useShardedLocks() ? objectMutex(chunkHash) : mutex_;
    GatewayMutexGuard lock(selectedMutex, __func__);
    const auto taskIt = deleteTasks_.find(chunkHash);
    if (taskIt == deleteTasks_.end()) return true;

    DeleteTask next = taskIt->second;
    const auto node = std::find(next.pendingNodeIds.begin(), next.pendingNodeIds.end(), nodeId);
    if (node == next.pendingNodeIds.end()) return true;
    next.pendingNodeIds.erase(node);
    next.updatedAt = unixSeconds();

    leveldb::WriteBatch batch;
    if (next.pendingNodeIds.empty()) {
        batch.Delete("del:" + chunkHash);
        batch.Delete("c:" + chunkHash);
    } else {
        batch.Put("del:" + chunkHash, deleteTaskValue(next));
    }
    if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return false;
    if (next.pendingNodeIds.empty()) {
        deleteTasks_.erase(taskIt);
        routeCache_.erase(chunkHash);
        manifestCache_.clear();
    } else {
        taskIt->second = std::move(next);
    }
    return true;
}

DeleteStatus GatewayState::deleteCatalogEntriesLocked(const std::vector<std::string>& objectIds,
                                                      const std::vector<std::string>& directoryKeys)
{
    std::map<std::string, ObjectMeta> targets;
    for (const auto& objectId : objectIds) {
        ObjectMeta object;
        if (!getObjectLocked(objectId, object)) return DeleteStatus::kNotFound;
        targets.emplace(objectId, std::move(object));
    }

    std::set<std::string> targetFileHashes;
    for (const auto& [objectId, object] : targets) {
        (void)objectId;
        targetFileHashes.insert(object.fileHash);
    }

    std::set<std::string> survivingFileHashes;
    auto objectIt = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
    for (objectIt->Seek("obj:"); objectIt->Valid() && objectIt->key().ToString().rfind("obj:", 0) == 0; objectIt->Next()) {
        ObjectMeta object;
        if (!parseObject(objectIt->value().ToString(), object)) return DeleteStatus::kInvalidRequest;
        if (targets.count(object.objectId) == 0) survivingFileHashes.insert(object.fileHash);
    }
    if (!objectIt->status().ok()) return DeleteStatus::kInvalidRequest;

    std::set<std::string> survivingChunks;
    for (const auto& fileHash : survivingFileHashes) {
        FileMeta file;
        if (!getFileLocked(fileHash, file)) return DeleteStatus::kInvalidRequest;
        survivingChunks.insert(file.chunkHashes.begin(), file.chunkHashes.end());
    }

    std::set<std::string> removableFiles;
    std::set<std::string> removableChunks;
    for (const auto& fileHash : targetFileHashes) {
        if (survivingFileHashes.count(fileHash) != 0) continue;
        FileMeta file;
        if (!getFileLocked(fileHash, file)) return DeleteStatus::kInvalidRequest;
        removableFiles.insert(fileHash);
        for (const auto& chunkHash : file.chunkHashes) {
            if (survivingChunks.count(chunkHash) == 0) removableChunks.insert(chunkHash);
        }
    }

    std::map<std::string, DeleteTask> newTasks;
    const int64_t now = unixSeconds();
    for (const auto& chunkHash : removableChunks) {
        if (deleteTasks_.count(chunkHash) != 0) continue;
        ChunkRoute route;
        if (!getRouteLocked(chunkHash, route) || route.replicas.empty()) {
            return DeleteStatus::kInvalidRequest;
        }
        DeleteTask task;
        task.chunkHash = chunkHash;
        task.storageIdentity = route.identityScheme == "opaque-chunk-id" ?
            route.chunkId : route.chunkHash;
        task.pendingNodeIds = route.replicas;
        std::sort(task.pendingNodeIds.begin(), task.pendingNodeIds.end());
        task.pendingNodeIds.erase(std::unique(task.pendingNodeIds.begin(), task.pendingNodeIds.end()),
                                  task.pendingNodeIds.end());
        if (task.pendingNodeIds.empty()) return DeleteStatus::kInvalidRequest;
        task.createdAt = now;
        task.updatedAt = now;
        newTasks.emplace(chunkHash, std::move(task));
    }

    leveldb::WriteBatch batch;
    for (const auto& [objectId, object] : targets) {
        batch.Delete("obj:" + objectId);
        batch.Delete("path:" + catalogPathKey(object.ownerId, childPath(object.parentPath, object.name)));
    }
    for (const auto& directoryKey : directoryKeys) batch.Delete("dir:" + directoryKey);
    for (const auto& fileHash : removableFiles) batch.Delete("f:" + fileHash);
    for (const auto& [chunkHash, task] : newTasks) {
        batch.Put("del:" + chunkHash, deleteTaskValue(task));
    }
    if (!db_->Write(leveldb::WriteOptions(), &batch).ok()) return DeleteStatus::kInvalidRequest;

    for (const auto& [objectId, object] : targets) {
        objectCache_.erase(objectId);
    }
    for (const auto& fileHash : removableFiles) {
        fileCache_.erase(fileHash);
        manifestCache_.erase(fileHash);
    }
    catalogCache_.clear();
    for (const auto& [chunkHash, task] : newTasks) deleteTasks_[chunkHash] = task;
    return DeleteStatus::kDeleted;
}



}
}
