#pragma once


#include "TypesV2.hpp"
#include <vector>
#include <string>



namespace miniKV {
namespace storage {

class DataNodeClient;


class IReplicaScheduler {
public:
    virtual ~IReplicaScheduler() = default;

    struct WriteResult {
        std::string nodeId;
        bool        ok     = false;
        std::string error;
        uint32_t    bytesWritten = 0;
        std::string chunkHash;
    };

    // [用途] 将数据写入指定的副本节点列表
    // [输入] nodeIds — 目标节点 ID 列表
    // [输入] data    — 要写入的数据
    // [输入] len     — 数据长度
    // [输入] rpcClient — DataNode RPC 客户端
    virtual std::vector<WriteResult> schedule(
        const std::vector<std::string>& nodeIds,
        const char* data, size_t len,
        DataNodeClient* rpcClient) = 0;
};

// WriteAll — 全部成功才返回 ok
class WriteAllScheduler : public IReplicaScheduler {
public:
    std::vector<WriteResult> schedule(
        const std::vector<std::string>& nodeIds,
        const char* data, size_t len,
        DataNodeClient* rpcClient) override;
};

// WriteQuorum — 多数派成功即可
class WriteQuorumScheduler : public IReplicaScheduler {
public:
    explicit WriteQuorumScheduler(int quorumSize = 0);
    std::vector<WriteResult> schedule(
        const std::vector<std::string>& nodeIds,
        const char* data, size_t len,
        DataNodeClient* rpcClient) override;
private:
    int quorumSize_;
};

}

}

























