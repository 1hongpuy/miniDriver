#include "media/ThumbnailWorker.hpp"
#include "network/EventLoop.hpp"
#include "utils/AsyncLogger.hpp"

#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <unistd.h>

namespace {

const char* requiredEnvironment(const char* name)
{
    const char* value = std::getenv(name);
    if(value == nullptr || *value == '\0') {
        std::cerr << "thumbnail worker missing " << name << '\n';
        std::exit(2);
    }
    return value;
}

bool parsePort(const char* value, uint16_t& output)
{
    char* end = nullptr;
    const unsigned long parsed = std::strtoul(value, &end, 10);
    if(end == value || *end != '\0' || parsed == 0 || parsed > std::numeric_limits<uint16_t>::max()) {
        return false;
    }
    output = static_cast<uint16_t>(parsed);
    return true;
}

bool parseJobs(const char* value, uint32_t& output)
{
    char* end = nullptr;
    const unsigned long parsed = std::strtoul(value, &end, 10);
    if(end == value || *end != '\0' || parsed == 0 || parsed > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    output = static_cast<uint32_t>(parsed);
    return true;
}

}  // namespace

int main(int argc, char** argv)
{
    if(argc != 5) {
        std::cerr << "usage: minikv_v2_thumbnail_worker <node-id> <data-dir> <temp-dir> <max-jobs>\n";
        return 2;
    }

    const std::string nodeId = argv[1];
    const char* gatewayAddress = requiredEnvironment("MINIKV_V2_GATEWAY_ADDRESS");
    const char* gatewayPort = requiredEnvironment("MINIKV_V2_GATEWAY_PORT");
    const char* redisAddress = requiredEnvironment("MINIKV_V2_REDIS_ADDRESS");
    const char* redisPort = requiredEnvironment("MINIKV_V2_REDIS_PORT");
    const char* stream = requiredEnvironment("MINIKV_V2_REDIS_THUMBNAIL_STREAM");
    const char* clusterSecret = requiredEnvironment("MINIKV_V2_CLUSTER_SECRET");

    uint16_t parsedGatewayPort = 0;
    uint16_t parsedRedisPort = 0;
    uint32_t maxJobs = 0;
    if(!parsePort(gatewayPort, parsedGatewayPort) || !parsePort(redisPort, parsedRedisPort) ||
       !parseJobs(argv[4], maxJobs)) {
        std::cerr << "thumbnail worker has invalid numeric configuration\n";
        return 2;
    }

    miniKV::utils::initAsyncLogger(miniKV::utils::asyncLoggerConfigFromEnvironment(
        "thumbnail_worker", nodeId, std::string(argv[2]) + "/logs/thumbnail-worker.log"));
    miniKV::network::EventLoop loop;
    miniKV::media::ThumbnailWorkerConfig config;
    config.nodeId = nodeId;
    config.gatewayAddress = gatewayAddress;
    config.gatewayPort = parsedGatewayPort;
    config.clusterSecret = clusterSecret;
    config.tempDir = argv[3];
    config.maxConcurrentJobs = maxJobs;
    config.redis.address = redisAddress;
    config.redis.port = parsedRedisPort;
    config.redis.thumbnailStream = stream;
    config.redis.consumer = "thumbnail-" + nodeId + "-" + std::to_string(::getpid());

    const auto worker = miniKV::media::ThumbnailWorker::create(&loop, std::move(config));
    if(!worker || !worker->start()) {
        miniKV::utils::logError("event=thumbnail_worker_start_failed node=" + nodeId);
        miniKV::utils::shutdownAsyncLogger();
        return 1;
    }
    miniKV::utils::logInfo("event=thumbnail_worker_started node=" + nodeId +
                           " gateway=" + gatewayAddress + ":" + gatewayPort +
                           " redis=" + redisAddress + ":" + redisPort +
                           " stream=" + stream + " temp_dir=" + argv[3] +
                           " max_jobs=" + argv[4]);
    loop.loop();
    worker->stop();
    miniKV::utils::shutdownAsyncLogger();
    return 0;
}
