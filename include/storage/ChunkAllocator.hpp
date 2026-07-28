#pragma once


#include "TypesV2.hpp"
#include "IPlacementPolicy.hpp"
#include <map>
#include <string>
#include <memory>
#include <vector>


namespace miniKV {
namespace storage {


class ChunkAllocator {
public:
    explicit ChunkAllocator(std::unique_ptr<IPlacementPolicy> policy = nullptr);

    std::vector<std::string> selectNodes(
        int replicationFactor,
        const std::vector<NodeInfo>& onlineNodes
    );

    void registerNode(const std::string& nodeId);
    void deregisterNode(const std::string& nodeId);
private:
    std::unique_ptr<IPlacementPolicy> policy_;
    std::set<std::string> knownNodes_;
};

}



}






















