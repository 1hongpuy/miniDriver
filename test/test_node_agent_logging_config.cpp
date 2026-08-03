#include "TestCheck.hpp"
#include "agent/NodeAgentConfig.hpp"

int main()
{
    const auto config = miniKV::agent::parseNodeAgentConfigText(R"(
node: { nodeId: node-c, advertiseAddress: 100.89.50.125 }
cluster: { secretFile: /tmp/secret, gatewayAddress: 100.75.93.124, gatewayPort: 18081 }
services:
  - id: datanode-0
    type: datanode
    listenPort: 9002
    dataDir: /tmp/data
    logging: { file: /tmp/logs/datanode.log, level: debug, queueSize: 4096,
               rotateBytes: 1048576, rotateFiles: 3 }
  - id: gateway-0
    type: gateway
    listenPort: 18081
    dataDir: /tmp/gateway
)");

    MINIKV_CHECK(config.services.size() == 2);
    MINIKV_CHECK(config.services[0].logging.filePath == "/tmp/logs/datanode.log");
    MINIKV_CHECK(config.services[0].logging.level == "debug");
    MINIKV_CHECK(config.services[0].logging.queueSize == 4096);
    MINIKV_CHECK(config.services[0].logging.rotateBytes == 1048576);
    MINIKV_CHECK(config.services[0].logging.rotateFiles == 3);
    MINIKV_CHECK(config.services[1].logging.filePath == "/tmp/gateway/logs/gateway-0.log");
    MINIKV_CHECK(config.services[1].logging.level == "info");
    MINIKV_CHECK(config.services[1].logging.queueSize == 8192);
    return 0;
}
