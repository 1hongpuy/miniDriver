#include "TestCheck.hpp"
#include "agent/NodeAgentConfig.hpp"

int main()
{
    const auto config = miniKV::agent::parseNodeAgentConfigText(R"(
node:
  nodeId: node-c
  advertiseAddress: 100.89.50.125
  capabilities: [storage, thumbnail]
cluster:
  secretFile: /tmp/secret
  gatewayAddress: 100.75.93.124
  gatewayPort: 18081
  redis: { address: 100.89.50.125, port: 6380, thumbnailStream: media:test, streamMaxLen: 200 }
services:
  - id: thumbnail-worker-0
    type: thumbnail_worker
    dataDir: /tmp/thumbnail-worker
    tempDir: /tmp/thumbnail-worker/tmp
    maxConcurrentJobs: 1
    logging: { file: /tmp/logs/thumbnail-worker.log, level: info }
)");

    MINIKV_CHECK(config.services.size() == 1);
    MINIKV_CHECK(config.services[0].id == "thumbnail-worker-0");
    MINIKV_CHECK(config.services[0].dataDir == "/tmp/thumbnail-worker");
    MINIKV_CHECK(config.services[0].tempDir == "/tmp/thumbnail-worker/tmp");
    MINIKV_CHECK(config.services[0].maxConcurrentJobs == 1);
    MINIKV_CHECK(config.redis.thumbnailStream == "media:test");
    return 0;
}
