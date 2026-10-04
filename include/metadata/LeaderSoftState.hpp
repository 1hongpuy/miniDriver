#pragma once

#include "metadata/MetadataTypes.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace miniKV::metadata {

struct NodeHeartbeat {
    std::string nodeId;
    uint64_t nodeEpoch = 0;
    uint64_t freeBytes = 0;
    uint32_t activeUploads = 0;
    uint32_t activeDownloads = 0;
    uint32_t queueDepth = 0;
    uint64_t diskPauseMs = 0;
    uint64_t eventLoopLagUs = 0;
    int64_t observedAtMs = 0;
};

class LeaderSoftState {
public:
    void beginLeaderTerm(uint64_t term, int64_t startedAtMs);
    bool update(const NodeRecord& durableNode, const NodeHeartbeat& heartbeat);
    std::optional<NodeHeartbeat> heartbeat(const std::string& nodeId) const;
    std::vector<NodeHeartbeat> freshHeartbeats(int64_t nowMs, int64_t freshnessMs) const;
    bool isFresh(const std::string& nodeId, int64_t nowMs, int64_t freshnessMs) const;
    uint64_t term() const { return term_; }
    int64_t leaderStartedAtMs() const { return leaderStartedAtMs_; }

private:
    uint64_t term_ = 0;
    int64_t leaderStartedAtMs_ = 0;
    std::map<std::string, NodeHeartbeat> heartbeats_;
};

} // namespace miniKV::metadata
