#pragma once

#include "client/HttpTransport.hpp"
#include "metadata/MetadataCommand.hpp"
#include "metadata/MetadataService.hpp"

#include <boost/property_tree/ptree.hpp>

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace miniKV::metadata {

// A read-only, linearizable snapshot used by Gateway route planning.  The
// older client API fetched session, nodes, chunks and leases independently;
// this shape lets the metadata service perform one ReadIndex confirmation and
// one HTTP round trip while retaining the same authoritative records.
struct UploadRouteView {
    uint32_t index = 0;
    ChunkRouteRecord chunk;
    LeaseRecord lease;
};

struct UploadRouteSnapshot {
    UploadSessionRecord session;
    std::vector<NodeRecord> nodes;
    std::vector<UploadRouteView> routes;
};

// The create+reserve mutation already has a committed state-machine result.
// Returning the two records produced by that same apply avoids immediately
// issuing two additional linearizable reads on the Raft hot path.
struct CreateSessionReservationResult {
    ApplyResult result;
    std::optional<UploadSessionRecord> session;
    std::optional<ObjectRecord> object;
};

// Synchronous control-plane client used by Gateway frontends.  It owns no
// metadata state: endpoint failover is only a transport concern and every
// mutation is replayed with the caller supplied commandId.
class MetadataClient {
public:
    MetadataClient(std::vector<client::Endpoint> endpoints,
                   std::string clusterSecret,
                   int timeoutMs = 1000);

    static std::vector<client::Endpoint> parseEndpoints(const std::string& value);
    static std::string newCommandId(const std::string& prefix = "gw");

    ApplyResult propose(const MetadataCommand& command, std::string* error = nullptr);
    ApplyResult proposeBatch(const std::vector<MetadataCommand>& commands, std::string* error = nullptr);
    ApplyResult createSessionAndReserve(const MetadataCommand& create,
                                        const std::vector<ReserveLeaseRequest>& requests,
                                        std::string* error = nullptr);
    CreateSessionReservationResult createSessionAndReserveDetailed(
        const MetadataCommand& create,
        const std::vector<ReserveLeaseRequest>& requests,
        std::string* error = nullptr);
    ApplyResult reserveLease(const ReserveLeaseRequest& request, std::string* error = nullptr);
    ApplyResult reserveLeaseBatch(const std::vector<ReserveLeaseRequest>& requests, std::string* error = nullptr);
    ApplyResult commitChunk(const std::string& sessionId, uint32_t index,
                            const std::string& chunkHash, uint64_t size,
                            const std::vector<std::string>& successfulNodes,
                            const std::string& leaseId,
                            std::string* error = nullptr);
    bool heartbeat(const NodeHeartbeat& heartbeat, std::string* error = nullptr);

    std::optional<UploadSessionRecord> session(const std::string& id, std::string* error = nullptr) const;
    std::vector<UploadSessionRecord> sessions(std::string* error = nullptr) const;
    std::optional<LeaseRecord> lease(const std::string& id, std::string* error = nullptr) const;
    std::optional<ObjectRecord> object(const std::string& id, std::string* error = nullptr) const;
    std::optional<ChunkRouteRecord> chunk(const std::string& objectId, uint32_t index,
                                          std::string* error = nullptr) const;
    std::optional<UploadRouteSnapshot> uploadRoutes(const std::string& sessionId,
                                                    const std::vector<uint32_t>& chunkIndexes,
                                                    std::string* error = nullptr) const;
    std::vector<NodeRecord> nodes(std::string* error = nullptr) const;
    std::optional<ReadDescriptor> readDescriptor(const std::string& id, uint64_t version,
                                                   std::string* error = nullptr) const;
    std::vector<DirectoryRecord> directories(const std::string& ownerId,
                                             const std::string& parentPath,
                                             std::string* error = nullptr) const;
    std::vector<ObjectRecord> objects(const std::string& ownerId,
                                      const std::string& parentPath,
                                      std::string* error = nullptr) const;
    std::vector<DeleteTaskRecord> deleteTasks(const std::string& nodeId,
                                               std::string* error = nullptr) const;
    bool readIndex(std::string* error = nullptr) const;
    bool available(std::string* error = nullptr) const;

private:
    bool request(std::string_view method, std::string_view path, std::string_view body,
                 client::HttpResponse& response, std::string& error,
                 bool retryable) const;
    static std::optional<ApplyResult> parseApplyResult(const std::string& body, std::string* error);
    static std::optional<UploadSessionRecord> parseSessionTree(
        const boost::property_tree::ptree& tree, std::string* error);
    static std::optional<ObjectRecord> parseObjectTree(
        const boost::property_tree::ptree& tree, std::string* error);
    static std::optional<UploadSessionRecord> parseSession(const std::string& body, std::string* error);
    static std::optional<LeaseRecord> parseLease(const std::string& body, std::string* error);
    static std::optional<ObjectRecord> parseObject(const std::string& body, std::string* error);
    static std::optional<ReadDescriptor> parseDescriptor(const std::string& body, std::string* error);
    static std::string statusName(ApplyStatus status);

    std::vector<client::Endpoint> endpoints_;
    std::string clusterSecret_;
    int timeoutMs_ = 1000;
    mutable std::mutex mutex_;
    mutable size_t nextEndpoint_ = 0;
};

} // namespace miniKV::metadata
