#include "TestCheck.hpp"
#include "DataNode/ReplicaUploadPipe.hpp"

#include <chrono>

int main()
{
    using Clock = std::chrono::steady_clock;
    miniKV::datanode::ReplicaUploadMetrics metrics;
    const auto started = Clock::now();

    metrics.recordPaused(524288, started);
    metrics.recordPaused(128, started + std::chrono::milliseconds(1));
    metrics.recordResumed(started + std::chrono::milliseconds(25));

    MINIKV_CHECK(metrics.pauseCount == 1);
    MINIKV_CHECK(metrics.pauseNanoseconds >= 25000000);
    MINIKV_CHECK(metrics.maxPendingBytes == 524288);
    return 0;
}
