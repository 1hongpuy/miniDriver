#include "TestCheck.hpp"
#include "media/RedisTaskPublisher.hpp"

#include <chrono>
#include <cstdlib>
#include <thread>

int main()
{
    miniKV::media::RedisTaskPublisherConfig config;
    config.maxPendingJobs = 1;
    miniKV::media::RedisTaskPublisher publisher(config);

    miniKV::media::MediaJob first;
    first.jobId = "job-one";
    first.profile = "thumb-512-jpeg-v1";
    miniKV::media::MediaJob second = first;
    second.jobId = "job-two";

    MINIKV_CHECK(publisher.enqueue(first));
    MINIKV_CHECK(!publisher.enqueue(second));
    MINIKV_CHECK(publisher.pendingCount() == 1);
    publisher.stop();

    const char* livePort = std::getenv("MINIKV_REDIS_TEST_PORT");
    if(livePort == nullptr) return 0;

    miniKV::media::RedisTaskPublisherConfig liveConfig;
    liveConfig.port = static_cast<uint16_t>(std::strtoul(livePort, nullptr, 10));
    liveConfig.thumbnailStream = "media:thumbnail:test";
    liveConfig.maxPendingJobs = 4;
    miniKV::media::RedisTaskPublisher livePublisher(liveConfig);
    MINIKV_CHECK(livePublisher.start());
    MINIKV_CHECK(livePublisher.enqueue(first));
    for(int attempt = 0; attempt < 50 && livePublisher.pendingCount() != 0; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    MINIKV_CHECK(livePublisher.pendingCount() == 0);
    livePublisher.stop();
    return 0;
}
