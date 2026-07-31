#include "http/DeferredResponse.hpp"
#include "http/HttpContext.hpp"
#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "http/HttpServer.hpp"
#include "network/EventLoop.hpp"
#include "network/TcpConnection.hpp"
#include "http/AsyncHttpClient.hpp"
#include "http/CorsPolicy.hpp"
#include "DataNode/FastDataStore.hpp"
#include "DataNode/HttpGatewayControlClient.hpp"
#include "DataNode/ReplicaUploadPipe.hpp"
#include "DataNode/WriteAdmission.hpp"
#include "utils/Util.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <sys/statvfs.h>
#include <vector>

using miniKV::http::DeferredResponse;
using miniKV::http::HttpContext;
using miniKV::http::HttpRequest;
using miniKV::http::HttpResponse;
using miniKV::network::EventLoop;
using miniKV::network::TcpConnectionPtr;
using namespace miniKV::util;
using namespace miniKV::datanode;

namespace {

constexpr uint32_t kMaxConcurrentWrites = 2;

struct ReplicaTarget {
    std::string nodeId;
    std::string address;
    uint16_t port = 0;
};

bool beginsWith(const std::string& value, const std::string& prefix)
{
    return value.rfind(prefix, 0) == 0;
}

void json(HttpResponse* response, int status, const std::string& body)
{
    response->setStatusCode(static_cast<HttpResponse::HttpStatusCode>(status));
    response->setContentType("application/json");
    response->setBody(body);
}

std::string configuredSecret(int argc, char** argv)
{
    if(argc > 7) return argv[7];
    if(const char* value = std::getenv("MINIKV_V2_CLUSTER_SECRET")) return value;
    return {};
}

std::string configuredAllowedOrigin()
{
    if(const char* value = std::getenv("MINIKV_V2_ALLOWED_ORIGIN")) return value;
    return {};
}


bool parseReplicaTarget(const std::string& value, ReplicaTarget& out)
{
    const size_t at = value.find('@');
    const size_t colon = value.rfind(':');
    if(at == std::string::npos || colon == std::string::npos || colon <= at + 1) return false;
    try {
        out.nodeId = value.substr(0, at);
        out.address = value.substr(at + 1, colon - at - 1);
        const auto parsedPort = std::stoul(value.substr(colon + 1));
        if(out.nodeId.empty() || out.address.empty() || parsedPort == 0 || parsedPort > UINT16_MAX)
            return false;
        out.port = static_cast<uint16_t>(parsedPort);
        return true;
    } catch(...) {
        return false;
    }
}

std::vector<ReplicaTarget> parseReplicaChain(const std::string& value)
{
    std::vector<ReplicaTarget> out;
    for(const auto& item : split(value, ';')) {
        ReplicaTarget target;
        if(!parseReplicaTarget(item, target)) return {};
        out.push_back(std::move(target));
    }
    return out;
}

std::string joinReplicaChain(const std::vector<ReplicaTarget>& chain)
{
    std::vector<std::string> values;
    for(const auto& node : chain) {
        values.push_back(node.nodeId + "@" + node.address + ":" + std::to_string(node.port));
    }
    return join(values, ';');
}

bool sameChain(const std::vector<ReplicaTarget>& chain, const std::vector<std::string>& expected)
{
    return joinReplicaChain(chain) == join(expected, ';');
}

uint64_t availableBytes(const std::string& path)
{
    struct statvfs fs {};
    return ::statvfs(path.c_str(), &fs) == 0
        ? static_cast<uint64_t>(fs.f_bavail) * fs.f_frsize : 0;
}

uint64_t effectiveFreeBytes(const FastDataStore& store, const std::string& path)
{
    const uint64_t filesystemFree = availableBytes(path);
    const uint64_t reusable = store.reusableBytes();
    return UINT64_MAX - filesystemFree < reusable ? UINT64_MAX : filesystemFree + reusable;
}

uint64_t logicalUsedBytes(const FastDataStore& store)
{
    const uint64_t physicalHighWater = store.usedBytes();
    const uint64_t reusable = store.reusableBytes();
    return reusable >= physicalHighWater ? 0 : physicalHighWater - reusable;
}

class ChunkUploadStream : public std::enable_shared_from_this<ChunkUploadStream> {
public:
    ChunkUploadStream(EventLoop* loop, FastDataStore& store, WriteAdmission& writeAdmission,
                      std::string nodeId, GatewayControlClient& gatewayControl,
                      std::string gatewayAddress, uint16_t gatewayPort,
                      std::string clusterSecret, CorsPolicy corsPolicy,
                      std::string requestOrigin, const HttpRequest& request, std::string chunkHash)
        : loop_(loop), store_(store), writeAdmission_(writeAdmission), nodeId_(std::move(nodeId)),
            gatewayControl_(gatewayControl),
            gatewayAddress_(std::move(gatewayAddress)), gatewayPort_(gatewayPort),
            clusterSecret_(std::move(clusterSecret)), corsPolicy_(std::move(corsPolicy)),
            requestOrigin_(std::move(requestOrigin)), chunkHash_(std::move(chunkHash))
    {
        setup(request);
    }

    ~ChunkUploadStream() { releaseActiveWrite(); }

    void startReplica(const TcpConnectionPtr& upstream)
    {
        if(!error_.empty() || writer_ == nullptr || position_ + 1 >= chain_.size()) return;

        const ReplicaTarget& target = chain_[position_ + 1];
        ReplicaUploadPipeOptions options;
        options.request.address = target.address;
        options.request.port = target.port;
        options.request.method = "PUT";
        options.request.path = "/v2/chunks/" + chunkHash_;
        options.request.contentLength = capability_.chunkSize;
        options.request.timeoutMs = 30000;
        options.request.headers = {
            {"Content-Type", "application/octet-stream"},
            {"X-Session-Id", capability_.sessionId},
            {"X-Chunk-Index", std::to_string(capability_.chunkIndex)},
            {"X-Commit-Owner", chain_.front().nodeId},
            {"X-Gateway-Address", gatewayAddress_},
            {"X-Gateway-Port", std::to_string(gatewayPort_)},
            {"X-Replica-Chain", joinReplicaChain(chain_)},
            {"X-Replica-Position", std::to_string(position_ + 1)},
            {"X-Upload-Token", uploadToken_}
        };
        std::weak_ptr<miniKV::network::TcpConnection> weakUpstream(upstream);
        options.resumeUpstream = [weakUpstream] {
            if(auto connection = weakUpstream.lock()) connection->resumeRead();
        };

        replicaPipe_ = ReplicaUploadPipe::create(loop_);
        std::weak_ptr<ChunkUploadStream> weakSelf(shared_from_this());
        replicaPipe_->start(std::move(options), [weakSelf](HttpClientResponse response, std::string error) {
            if(auto self = weakSelf.lock()) self->onReplicaComplete(std::move(response), std::move(error));
        });
    }

    HttpContext::BodyConsumeResult consume(const char* bytes, size_t size)
    {
        if(admissionRejected_) return HttpContext::BodyConsumeResult::kContinue;
        if(!error_.empty() || writer_ == nullptr) return HttpContext::BodyConsumeResult::kAbort;
        if(!writer_->append(bytes, size)) {
            error_ = "local streaming write failed";
            return HttpContext::BodyConsumeResult::kAbort;
        }
        if(replicaPipe_ == nullptr) return HttpContext::BodyConsumeResult::kContinue;

        const auto result = replicaPipe_->push(bytes, size);
        if(result == HttpContext::BodyConsumeResult::kAbort) {
            error_ = "replica stream rejected body bytes";
            std::cerr << "chunk upload " << chunkHash_ << " replica body rejected\n";
        } else if(result == HttpContext::BodyConsumeResult::kPause) {
            std::cerr << "chunk upload " << chunkHash_ << " paused for replica backpressure\n";
        }
        return result;
    }

    void finish(const DeferredResponse::Ptr& deferred)
    {
        if(response_ != nullptr) return;
        response_ = deferred;
        response_->defer();
        selfHold_ = shared_from_this();

        if(admissionRejected_) {
            completeClient(503, "DataNode write capacity reached");
            return;
        }
        if(!error_.empty() || writer_ == nullptr) {
            completeClient(400, error_.empty() ? "invalid stream" : error_);
            return;
        }

        bool alreadyExists = false;
        if(!writer_->finish(alreadyExists)) {
            std::cerr << "chunk upload " << chunkHash_
                      << " local finish failed after " << writer_->writtenBytes() << " bytes\n";
            completeClient(400, "chunk length or SHA-256 verification failed");
            return;
        }
        std::cerr << "chunk upload " << chunkHash_ << " local finish succeeded"
                  << (replicaPipe_ == nullptr ? " without replica\n" : ", waiting for replica\n");
        alreadyExists_ = alreadyExists;
        successfulNodes_.push_back(nodeId_);

        if(replicaPipe_ == nullptr) {
            afterReplica();
            return;
        }
        replicaPipe_->finish();
    }

private:
    void setup(const HttpRequest& request)
    {
        uploadToken_ = request.getHeader("X-Upload-Token");
        if(!verifyUploadCapability(uploadToken_, clusterSecret_, capability_)) {
            error_ = "invalid or expired upload token";
            return;
        }
        try {
            position_ = static_cast<size_t>(std::stoull(request.getHeader("X-Replica-Position")));
        } catch(...) {
            error_ = "invalid replica position";
            return;
        }
        chain_ = parseReplicaChain(request.getHeader("X-Replica-Chain"));
        if(request.getHeader("X-Session-Id") != capability_.sessionId ||
           request.getHeader("X-Chunk-Index") != std::to_string(capability_.chunkIndex) ||
           request.contentLength() != capability_.chunkSize || chunkHash_ != capability_.chunkHash ||
           chain_.empty() || !sameChain(chain_, capability_.chainTargets) ||
           position_ >= chain_.size() || chain_[position_].nodeId != nodeId_) {
            error_ = "request does not match upload token";
            return;
        }
        if(!writeAdmission_.tryAcquire()) {
            admissionRejected_ = true;
            return;
        }
        activeWriteCounted_ = true;
        writer_ = store_.beginPut(chunkHash_, capability_.chunkSize);
        if(writer_ == nullptr) {
            releaseActiveWrite();
            error_ = "cannot allocate local chunk extent";
            return;
        }
    }

    void onReplicaComplete(HttpClientResponse response, std::string error)
    {
        if(response_ == nullptr) return;
        replicaPipe_.reset();
        std::cerr << "chunk upload " << chunkHash_ << " replica completed: HTTP "
                  << response.status << ", error=" << (error.empty() ? "<none>" : error) << '\n';
        if(!error.empty() || response.status != 200) {
            replicaError_ = error.empty() ? "replica returned HTTP " + std::to_string(response.status)
                                          : std::move(error);
        } else {
            for(const auto& nodeId : split(jsonString(response.body, "successfulNodes"), ',')) {
                if(!nodeId.empty() && std::find(successfulNodes_.begin(), successfulNodes_.end(), nodeId) == successfulNodes_.end()) {
                    successfulNodes_.push_back(nodeId);
                }
            }
        }
        afterReplica();
    }

    void afterReplica()
    {
        if(position_ != 0) {
            std::cerr << "chunk upload " << chunkHash_ << " replica node replying to primary\n";
            completeClient(200, "");
            return;
        }
        std::cerr << "chunk upload " << chunkHash_ << " sending Gateway commit with "
                  << successfulNodes_.size() << " successful node(s)\n";
        std::weak_ptr<ChunkUploadStream> weakSelf(shared_from_this());
        gatewayControl_.commitChunk({capability_.sessionId, capability_.chunkIndex, chunkHash_,
                                    capability_.chunkSize, successfulNodes_, uploadToken_},
            [weakSelf](RpcResult result) {
            if(auto self = weakSelf.lock()) {
                std::cerr << "chunk upload " << self->chunkHash_ << " Gateway commit result: HTTP "
                          << result.httpStatus << ", error="
                          << (result.error.empty() ? "<none>" : result.error) << '\n';
                if(!result.ok) {
                    self->completeClient(500, result.error.empty() ? "Gateway commit failed" : std::move(result.error));
                    return;
                }
                self->completeClient(200, "");
            }
        });
    }

    void completeClient(int status, const std::string& error)
    {
        if(response_ == nullptr) return;
        std::cerr << "chunk upload " << chunkHash_ << " responding to client: HTTP " << status;
        if(!error.empty()) std::cerr << ", error=" << error;
        std::cerr << '\n';
        HttpResponse response;
        if(status != 200) {
            json(&response, status, jsonError(error));
            if(status == 503) response.addHeader("Retry-After", "1");
        } else {
            std::ostringstream body;
            body << "{\"chunkHash\":\"" << chunkHash_ << "\",\"alreadyExists\":"
                 << (alreadyExists_ ? "true" : "false") << ",\"successfulNodes\":\""
                 << join(successfulNodes_, ',') << "\"";
            if(!replicaError_.empty()) body << ",\"replicaWarning\":\"" << jsonEscape(replicaError_) << "\"";
            body << "}";
            json(&response, 200, body.str());
        }
        // Streaming PUT completes through DeferredResponse after the original
        // HttpServer callback has returned. Add CORS here, not only in the
        // immediate handler path, otherwise browsers treat a successful 200
        // as an opaque XHR network error.
        corsPolicy_.appendHeaders(response, requestOrigin_);
        if(status != 200 && position_ == 0 && !uploadToken_.empty()) {
            gatewayControl_.releaseLease({uploadToken_}, [](RpcResult) {});
        }
        releaseActiveWrite();
        auto deferred = std::move(response_);
        replicaPipe_.reset();
        selfHold_.reset();
        deferred->complete(std::move(response));
    }

    void releaseActiveWrite()
    {
        if(activeWriteCounted_) {
            writeAdmission_.release();
            activeWriteCounted_ = false;
        }
    }

    EventLoop* loop_;
    FastDataStore& store_;
    WriteAdmission& writeAdmission_;
    std::string nodeId_;
    GatewayControlClient& gatewayControl_;
    std::string gatewayAddress_;
    uint16_t gatewayPort_ = 0;
    std::string clusterSecret_;
    CorsPolicy corsPolicy_;
    std::string requestOrigin_;
    std::string chunkHash_;
    std::string uploadToken_;
    UploadCapability capability_;
    std::vector<ReplicaTarget> chain_;
    size_t position_ = 0;
    std::unique_ptr<FastDataStore::WriteSession> writer_;
    ReplicaUploadPipe::Ptr replicaPipe_;
    DeferredResponse::Ptr response_;
    std::vector<std::string> successfulNodes_;
    std::string error_;
    std::string replicaError_;
    bool alreadyExists_ = false;
    bool activeWriteCounted_ = false;
    bool admissionRejected_ = false;
    std::shared_ptr<ChunkUploadStream> selfHold_;
};

}  // namespace

int main(int argc, char** argv)
{
    /*
    nodeId
    advertiseAddress
    本节点 HTTP 端口
    dataDir
    Gateway 地址和端口
    clusterSecret
    */
    if(argc < 7) {
        std::cerr << "usage: minikv_v2_datanode <nodeId> <advertiseAddress> <port> <dataDir> <gatewayAddress> <gatewayPort> [clusterSecret]\n";
        return 2;
    }
    const std::string nodeId = argv[1];
    const std::string advertiseAddress = argv[2];
    const uint16_t port = static_cast<uint16_t>(std::stoul(argv[3]));
    const std::string dataDir = argv[4];
    const std::string gatewayAddress = argv[5];
    const uint16_t gatewayPort = static_cast<uint16_t>(std::stoul(argv[6]));
    const std::string clusterSecret = configuredSecret(argc, argv);
    const CorsPolicy corsPolicy(configuredAllowedOrigin());
    if(clusterSecret.empty()) {
        std::cerr << "MINIKV_V2_CLUSTER_SECRET or a command-line clusterSecret is required\n";
        return 2;
    }

    FastDataStore store(dataDir);
    if(!store.open()) {
        std::cerr << "cannot open DataNode store\n";
        return 1;
    }
    WriteAdmission writeAdmission(kMaxConcurrentWrites);
    EventLoop loop;
    HttpGatewayControlClient gatewayControl(&loop, gatewayAddress, gatewayPort, clusterSecret);

    std::function<void()> registerNode;
    std::function<void()> heartbeat;
    bool registered = false;
    bool registrationInFlight = false;
    registerNode = [&] {
        if(registered || registrationInFlight) return;
        registrationInFlight = true;
        gatewayControl.registerStorageNode({nodeId, advertiseAddress, port, availableBytes(dataDir), 0, kMaxConcurrentWrites},
            [&](RpcResult result) {
                registrationInFlight = false;
                if(result.ok) {
                    registered = true;
                    return;
                }
                std::cerr << "DataNode registration failed: " << result.error << '\n';
                loop.runAfter(1, registerNode);
            });
    };
    heartbeat = [&] {
        gatewayControl.sendHeartbeat({nodeId, logicalUsedBytes(store), effectiveFreeBytes(store, dataDir),
                                      0, 0, 0, 0, writeAdmission.active()},
            [&](RpcResult result) {
                if(!result.ok) {
                    registered = false;
                    registerNode();
                }
            });
    };

    miniKV::http::HttpServer server(&loop, nullptr, port);
    server.setStreamCheck([&](const HttpRequest& request) {
        return request.method() == HttpRequest::kPut &&
               beginsWith(request.path(), "/v2/chunks/") && request.contentLength() > 0 &&
               corsPolicy.allows(request.getHeader("Origin"));
    });

    server.setBodyStreamSetup([&](HttpContext* context, const HttpRequest& request,
        const TcpConnectionPtr& upstream) {
        const std::string hash = request.path().substr(std::string("/v2/chunks/").size());
        auto stream = std::make_shared<ChunkUploadStream>(&loop, store, writeAdmission, nodeId, gatewayControl,
        gatewayAddress, gatewayPort, clusterSecret, corsPolicy, request.getHeader("Origin"), request, hash);
        stream->startReplica(upstream);
        context->setUserData(stream);
        context->setBodyCallback(request.contentLength(), [stream](const char* bytes, size_t size) {
        return stream->consume(bytes, size);
        });
    });
    server.setHttpCallback([&](const HttpRequest& request, HttpResponse* response,
        const TcpConnectionPtr&, const DeferredResponse::Ptr& deferred) {
        const std::string& path = request.path();
        const std::string origin = request.getHeader("Origin");
        const bool chunkRequest = beginsWith(path, "/v2/chunks/");
        const bool internalDelete = request.method() == HttpRequest::kDelete &&
            beginsWith(path, "/internal/v2/chunks/");
        if(internalDelete) {
            if(!constantTimeEquals(request.getHeader("X-Cluster-Internal-Token"), clusterSecret)) {
                json(response, 403, jsonError("invalid cluster token"));
                return;
            }
            const std::string hash = path.substr(std::string("/internal/v2/chunks/").size());
            bool removed = false;
            if(hash.empty() || !store.remove(hash, removed)) {
                json(response, 500, jsonError("cannot delete local chunk"));
                return;
            }
            json(response, 200, std::string("{\"status\":\"deleted\",\"removed\":") +
                (removed ? "true}" : "false}"));
            return;
        }
        if(chunkRequest && !corsPolicy.allows(origin)) {
            json(response, 403, jsonError("origin is not allowed"));
            return;
        }
        if(chunkRequest && request.method() == HttpRequest::kOptions) {
            response->setStatusCode(HttpResponse::k200Ok);
            corsPolicy.appendHeaders(*response, origin);
            return;
        }
        if(request.method() == HttpRequest::kPut && beginsWith(path, "/v2/chunks/")) {
            auto stream = std::static_pointer_cast<ChunkUploadStream>(request.userData());
            if(stream == nullptr) {
                json(response, 400, jsonError("streaming body is required"));
                corsPolicy.appendHeaders(*response, origin);
                return;
            }
            stream->finish(deferred);
            return;
        }
        if((request.method() == HttpRequest::kGet || request.method() == HttpRequest::kHead) &&
        beginsWith(path, "/v2/chunks/")) {
            const std::string hash = path.substr(std::string("/v2/chunks/").size());
            FileRegion region;
            if(!store.getRegion(hash, region)) {
                json(response, 404, jsonError("chunk not found"));
                corsPolicy.appendHeaders(*response, origin);
                return;
            }
            response->setStatusCode(HttpResponse::k200Ok);
            response->addHeader("Content-Length", std::to_string(region.length));
            response->addHeader("X-Chunk-Hash", hash);
            if(request.method() == HttpRequest::kGet) {
                response->setFileBody(store.dataFilePath(), region.offset, region.length);
            }
            corsPolicy.appendHeaders(*response, origin);
            return;
        }
        json(response, 404, jsonError("route not found"));
    });
    loop.runAfter(0, [&] { registerNode(); heartbeat(); });
    loop.runEvery(8000, heartbeat);
    server.start();
    std::cout << "V2 DataNode " << nodeId << " listening on :" << port << '\n';
    loop.loop();
}
