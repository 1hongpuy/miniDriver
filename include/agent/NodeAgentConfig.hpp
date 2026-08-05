#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace miniKV {
namespace agent {

enum class ServiceType {
    kGateway,
    kDataNode,
    kThumbnailWorker
};

struct RestartPolicy {
    bool onFailure = true;
    uint32_t initialBackoffSeconds = 2;
    uint32_t maxBackoffSeconds = 30;
};

struct ServiceLogs {
    std::string stdoutPath;
    std::string stderrPath;
};

struct ServiceLoggingConfig {
    std::string filePath;
    std::string level = "info";
    uint32_t queueSize = 8192;
    uint64_t rotateBytes = 20ULL * 1024ULL * 1024ULL;
    uint32_t rotateFiles = 5;
};

struct RedisTaskConfig {
    std::string address = "127.0.0.1";
    uint16_t port = 6379;
    std::string thumbnailStream = "media:thumbnail";
    uint64_t streamMaxLen = 100000;
};

struct ManagedServiceConfig {
    std::string id;
    ServiceType type = ServiceType::kDataNode;
    bool enabled = true;
    uint16_t listenPort = 0;
    std::string dataDir;
    std::string tempDir;
    uint32_t maxConcurrentJobs = 1;
    RestartPolicy restart;
    ServiceLogs logs;
    ServiceLoggingConfig logging;
};

//advertiseAddress：其他节点访问本机服务时使用的 Tailscale IP。
struct NodeAgentConfig {
    std::string nodeId;
    std::string advertiseAddress;
    std::string gatewayAddress;
    uint16_t gatewayPort = 0;
    std::string secretFile;
    std::string webAllowedOrigin;
    std::vector<std::string> capabilities;
    RedisTaskConfig redis;
    std::vector<ManagedServiceConfig> services;
};

NodeAgentConfig parseNodeAgentConfigText(const std::string& yamlText);
NodeAgentConfig loadNodeAgentConfigFile(const std::string& path);
std::string readClusterSecret(const std::string& path);

}


}  // namespace miniKV::v2























