#include "storage/IPlacementPolicy.hpp"
#include "storage/TypesV2.hpp"
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <sys/types.h>
#include <vector>



namespace miniKV {
namespace storage {

bool IPlacementPolicy::hasCapability(const NodeInfo& node, const std::string& cap)
{
    for(auto& c : node.capabilities)
    {
        if(c == cap) return true;
    }
    return false;
}

std::vector<NodeInfo> IPlacementPolicy::filterByCapability(const std::vector<NodeInfo> &nodes, const std::string &capability)
{
    std::vector<NodeInfo> result;
    for(auto& node : nodes)
    {
        if(hasCapability(node, capability))
        {
            result.push_back(node);
        }
    }
    return result;
}

// [输入] replicaCount   — 需要的副本数（通常 2）
// [输入] onlineNodes    — 当前在线的节点列表（含容量信息）
std::vector<std::string> RoundRobinPolicy::selectNodes(int replicaCount, const std::vector<NodeInfo>& onlineNodes)
{
    std::vector<std::string> result;

    if(onlineNodes.empty() || replicaCount <= 0) return result;

    auto sorted = onlineNodes;
    std::sort(sorted.begin(), sorted.end(), [](const NodeInfo& a, const NodeInfo& b){
        return (a.totalBytes - a.usedBytes) > (b.totalBytes - b.usedBytes);
    });
   
    size_t start = counter_.fetch_add(1, std::memory_order_relaxed) % sorted.size();
    for(size_t i = 0; i < sorted.size() && (int)result.size() < replicaCount; i++)
    {
        result.push_back(sorted[(start+i) % sorted.size()].nodeId);
    }
    return  result;
}


std::vector<std::string> LeastUsedPolicy::selectNodes(
    int replicaCount, 
    const std::vector<NodeInfo>& onlineNodes) 
{
    if(onlineNodes.empty() || replicaCount <= 0) return {};

    auto sorted = onlineNodes;
    std::sort(sorted.begin(), sorted.end(), [](const NodeInfo& a, const NodeInfo& b){
        return a.usedBytes < b.usedBytes;
    });

    std::vector<std::string> result;

    for(size_t i = 0; i < sorted.size() && (int)result.size() < replicaCount; i++)
    {
        result.push_back(sorted[i].nodeId);
    }
    return result;
}


double DynamicScoringPolicy::score(const NodeInfo& node, int activeConns)
{
    // Score = freeRatio × 100 - activeConns × penaltyFactor
    if(node.totalBytes == 0) return -1e9;
    double freeRatio = static_cast<double>(node.totalBytes - node.usedBytes) / static_cast<double>(node.totalBytes);
    constexpr double penaltyFactor = 10.0;

    return freeRatio * 100 - static_cast<double>(activeConns) * penaltyFactor;
    
}

std::vector<std::string> DynamicScoringPolicy::selectNodes(
    int replicaCount,
    const std::vector<NodeInfo>& onlineNodes)
{
    if(onlineNodes.empty() || replicaCount <= 0) return {};

    std::lock_guard<std::mutex> lock(simMutex_);
    auto sorted = onlineNodes;
    std::sort(sorted.begin(), sorted.end(), 
        [this](const NodeInfo& a, const NodeInfo& b){
            int connA = 0, connB = 0;
            auto it = simulatedConnections_.find(a.nodeId);
            if(it != simulatedConnections_.end()) connA = it->second;
            it = simulatedConnections_.find(b.nodeId);
            if(it != simulatedConnections_.end()) connB = it->second;
            return score(a, connA) > score(b, connB);
    });
    std::vector<std::string> result;
    for(size_t i = 0; i < sorted.size() && (int)result.size() < replicaCount; i++)
    {
        const auto& node = sorted[i];
        result.push_back(node.nodeId);
        simulatedConnections_[node.nodeId]++;
    }
    return result;
}




}
}