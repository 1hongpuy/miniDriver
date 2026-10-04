#include "TestCheck.hpp"
#include "metadata/LeaderSoftState.hpp"

using namespace miniKV::metadata;

int main()
{
    NodeRecord node; node.nodeId = "dn-1"; node.nodeEpoch = 3;
    LeaderSoftState state; state.beginLeaderTerm(8, 1000);
    NodeHeartbeat stale{"dn-1", 2, 100, 0, 0, 0, 0, 0, 1001};
    MINIKV_CHECK(!state.update(node, stale));
    NodeHeartbeat fresh{"dn-1", 3, 100, 1, 2, 3, 4, 5, 1002};
    MINIKV_CHECK(state.update(node, fresh));
    MINIKV_CHECK(state.isFresh("dn-1", 6002, 6000));
    MINIKV_CHECK(!state.isFresh("dn-1", 7003, 6000));
    state.beginLeaderTerm(9, 8000);
    MINIKV_CHECK(!state.heartbeat("dn-1"));
    return 0;
}
