#include "metadata/LeaderSoftState.hpp"

namespace miniKV::metadata {

void LeaderSoftState::beginLeaderTerm(uint64_t term, int64_t startedAtMs)
{
    term_ = term;
    leaderStartedAtMs_ = startedAtMs;
    heartbeats_.clear();
}

bool LeaderSoftState::update(const NodeRecord& durableNode, const NodeHeartbeat& heartbeat)
{
    if(heartbeat.nodeId.empty() || heartbeat.nodeId != durableNode.nodeId
       || heartbeat.nodeEpoch != durableNode.nodeEpoch || heartbeat.observedAtMs < leaderStartedAtMs_) return false;
    auto current = heartbeats_.find(heartbeat.nodeId);
    if(current != heartbeats_.end() && heartbeat.observedAtMs < current->second.observedAtMs) return false;
    heartbeats_[heartbeat.nodeId] = heartbeat;
    return true;
}

std::optional<NodeHeartbeat> LeaderSoftState::heartbeat(const std::string& nodeId) const
{
    const auto it = heartbeats_.find(nodeId);
    return it == heartbeats_.end() ? std::nullopt : std::optional<NodeHeartbeat>(it->second);
}

bool LeaderSoftState::isFresh(const std::string& nodeId, int64_t nowMs, int64_t freshnessMs) const
{
    const auto value = heartbeat(nodeId);
    return value && nowMs >= value->observedAtMs && nowMs - value->observedAtMs <= freshnessMs;
}

std::vector<NodeHeartbeat> LeaderSoftState::freshHeartbeats(int64_t nowMs, int64_t freshnessMs) const
{
    std::vector<NodeHeartbeat> result;
    for(const auto& [nodeId, value] : heartbeats_) {
        (void)nodeId;
        if(nowMs >= value.observedAtMs && nowMs - value.observedAtMs <= freshnessMs) result.push_back(value);
    }
    return result;
}

} // namespace miniKV::metadata
