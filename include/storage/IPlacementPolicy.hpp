#pragma once

#include "TypesV2.hpp"
#include <atomic>
#include <cstddef>
#include <map>
#include <mutex>
#include <vector>
#include <string>
#include <cstdint>



namespace miniKV {
namespace storage {

class IPlacementPolicy {
public:
    virtual ~IPlacementPolicy() = default;
    //从在线节点中选择 N 个节点来存放副本
    virtual std::vector<std::string> selectNodes(
        int replicaCount, 
        const std::vector<NodeInfo>& onlineNodes) = 0;

    static std::vector<NodeInfo> filterByCapability(
        const std::vector<NodeInfo>& nodes,
        const std::string& capability);
    
    static bool hasCapability(const NodeInfo& node, const std::string& cap);
};

//轮询策略
class RoundRobinPolicy : public IPlacementPolicy {
public:
    std::vector<std::string> selectNodes(int replicaCount, const std::vector<NodeInfo>& onlineNodes) override;
private:
    std::atomic<uint64_t> counter_{0};
};


class LeastUsedPolicy : public IPlacementPolicy {
public:
    std::vector<std::string> selectNodes(
        int replicaCount, 
        const std::vector<NodeInfo>& onlineNodes) override;
};


class DynamicScoringPolicy : public IPlacementPolicy {
public:
    std::vector<std::string> selectNodes(
        int replicaCount,
        const std::vector<NodeInfo>& onlineNodes) override;
private:
    static double score(const NodeInfo& node, int activeConns);

    std::map<std::string, int> simulatedConnections_;
    std::mutex simMutex_;
};


}

}
