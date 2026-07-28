#include "storage/ChunkAllocator.hpp"



namespace miniKV {
namespace storage {
    
ChunkAllocator::ChunkAllocator(std::unique_ptr<IPlacementPolicy> policy)
:policy_(std::move(policy))
{
    if(!policy) { //默认轮询
        policy_ = std::make_unique<RoundRobinPolicy>();
    }
}



std::vector<std::string> ChunkAllocator::selectNodes(
        int replicationFactor,
        const std::vector<NodeInfo>& onlineNodes)
{
    return policy_->selectNodes(replicationFactor, onlineNodes);
}

// 注册/移除节点（保留，用于跟踪节点存在性）
//使用集合
void ChunkAllocator::registerNode(const std::string& nodeId)
{
    knownNodes_.insert(nodeId);
}
void ChunkAllocator::deregisterNode(const std::string& nodeId)
{
    knownNodes_.erase(nodeId);
}

}
}

