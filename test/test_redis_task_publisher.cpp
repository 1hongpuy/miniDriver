#include "TestCheck.hpp"
#include "media/RedisTaskPublisher.hpp"

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
    return 0;
}
