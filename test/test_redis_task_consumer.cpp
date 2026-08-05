#include "TestCheck.hpp"
#include "media/RedisTaskConsumer.hpp"

#include <chrono>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <vector>

int main()
{
    const char* livePort = std::getenv("MINIKV_REDIS_TEST_PORT");
    if(livePort == nullptr) return 0;
    miniKV::media::RedisTaskConsumerConfig config;
    config.port = static_cast<uint16_t>(std::strtoul(livePort, nullptr, 10));
    config.thumbnailStream = "media:thumbnail:consumer-test";
    config.group = "thumbnail-workers-test";
    config.consumer = "consumer-one";
    config.blockMs = 50;
    config.reclaimIdleMs = 1000;
    std::mutex mutex;
    std::vector<miniKV::media::RedisTaskMessage> received;
    miniKV::media::RedisTaskConsumer consumer(config);
    MINIKV_CHECK(consumer.start([&](miniKV::media::RedisTaskMessage message) {
        std::lock_guard<std::mutex> lock(mutex);
        received.push_back(std::move(message));
    }));
    for(int wait = 0; wait < 100; ++wait) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if(!received.empty()) break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    {
        std::lock_guard<std::mutex> lock(mutex);
        MINIKV_CHECK(received.size() == 1);
        MINIKV_CHECK(received.front().jobId == "job-one");
        MINIKV_CHECK(received.front().profile == "thumb-512-jpeg-v1");
        consumer.acknowledge(received.front().id);
    }
    consumer.stop();
    return 0;
}
