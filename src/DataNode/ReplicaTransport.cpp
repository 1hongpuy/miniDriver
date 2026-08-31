#include "DataNode/ReplicaTransport.hpp"

#include "DataNode/ReplicaUploadPipe.hpp"
#include "DataNode/ReplicaConnectionPool.hpp"

#include "utils/Util.hpp"

namespace miniKV::datanode {
namespace {

class HttpReplicaWriteStream final : public ReplicaWriteStream {
public:
    explicit HttpReplicaWriteStream(ReplicaUploadPipe::Ptr pipe)
        : pipe_(std::move(pipe)) {}

    StreamConsumeResult pushShared(
        DiskWriteExecutor::SharedBlockPtr block, size_t size) override
    {
        return pipe_->pushShared(std::move(block), size);
    }

    void finish() override { pipe_->finish(); }
    void abort() override { pipe_->abort(); }
    ReplicaTransportMetrics metrics() const override
    {
        const ReplicaUploadMetrics metrics = pipe_->metrics();
        return {metrics.pauseCount, metrics.pauseNanoseconds,
                metrics.maxPendingBytes};
    }

private:
    ReplicaUploadPipe::Ptr pipe_;
};

}  // namespace

HttpReplicaTransport::HttpReplicaTransport(
    network::EventLoop* loop, std::shared_ptr<ReplicaConnectionPool> connectionPool)
    : loop_(loop), connectionPool_(std::move(connectionPool))
{
}

std::shared_ptr<ReplicaWriteStream> HttpReplicaTransport::open(
    ReplicaWriteRequest request, std::function<void()> resumeUpstream,
    CompletionCallback completion)
{
    ReplicaUploadPipeOptions options;
    options.request.address = request.target.address;
    options.request.port = request.target.port;
    options.request.method = "PUT";
    options.request.path = "/v2/chunks/" + request.descriptor.storageKey();
    options.request.contentLength = request.descriptor.contentLength;
    options.request.timeoutMs = 30000;
    options.request.headers = {
        {"Content-Type", "application/octet-stream"},
        {"X-Session-Id", request.descriptor.sessionId},
        {"X-Chunk-Index", std::to_string(request.descriptor.chunkIndex)},
        {"X-Commit-Owner", request.commitOwnerNodeId},
        {"X-Gateway-Address", request.gatewayAddress},
        {"X-Gateway-Port", std::to_string(request.gatewayPort)},
        {"X-Replica-Chain", joinReplicaChain(request.descriptor.replicaChain)},
        {"X-Replica-Position", std::to_string(request.descriptor.replicaPosition + 1)},
        {"X-Upload-Token", request.descriptor.capabilityId},
        {"X-Client-Instance-Id", request.descriptor.clientId},
        {"X-Request-Id", request.descriptor.requestId}
    };
    options.connectionPool = connectionPool_;
    options.connectionKey = {
        request.target.nodeId, request.target.address, request.target.port};
    options.resumeUpstream = std::move(resumeUpstream);

    auto pipe = ReplicaUploadPipe::create(loop_);
    pipe->start(std::move(options),
        [completion = std::move(completion)](
            http::HttpClientResponse response, std::string error) mutable {
            completion({response.status, std::move(response.body), std::move(error)});
        });
    return std::make_shared<HttpReplicaWriteStream>(std::move(pipe));
}

}  // namespace miniKV::datanode
