#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace miniKV {
namespace agent {

enum class ServiceType {
    kGateway,
    kDataNode
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

struct ManagedServiceConfig {
    std::string id;
    ServiceType type = ServiceType::kDataNode;
    bool enabled = true;
    uint16_t listenPort = 0;
    std::string dataDir;
    RestartPolicy restart;
    ServiceLogs logs;
};

//advertiseAddress：其他节点访问本机服务时使用的 Tailscale IP。
struct NodeAgentConfig {
    std::string nodeId;
    std::string advertiseAddress;
    std::string gatewayAddress;
    uint16_t gatewayPort = 0;
    std::string secretFile;
    std::string webAllowedOrigin;
    std::vector<ManagedServiceConfig> services;
};

NodeAgentConfig parseNodeAgentConfigText(const std::string& yamlText);
NodeAgentConfig loadNodeAgentConfigFile(const std::string& path);
std::string readClusterSecret(const std::string& path);

}


}  // namespace miniKV::v2


























