#include "TestCheck.hpp"
#include "media/RedisAiEventPublisher.hpp"

#include <chrono>
#include <cstdlib>
#include <thread>

int main()
{
    const char* livePort = std::getenv("MINIKV_REDIS_TEST_PORT");
    if(livePort == nullptr) return 0;

    miniKV::media::RedisAiEventPublisherConfig config;
    config.port = static_cast<uint16_t>(std::strtoul(livePort, nullptr, 10));
    config.stream = "minidrive:file-events:publisher-test";
    config.maxPendingEvents = 4;
    bool published = false;
    miniKV::media::RedisAiEventPublisher publisher(config,
        [&published](const miniKV::media::AiIndexEvent& event) {
            published = event.eventId == "event-one";
        });
    MINIKV_CHECK(publisher.start());

    miniKV::media::AiIndexEvent event;
    event.eventId = "event-one";
    event.objectId = "object-one";
    event.objectKey = "/travel/temple.jpg";
    event.fileHash = "manifest-hash";
    event.fileSize = 1024;
    event.occurredAt = 100;
    MINIKV_CHECK(publisher.enqueue(event));
    for(int attempt = 0; attempt < 50 && !published; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    MINIKV_CHECK(published);
    MINIKV_CHECK(publisher.pendingCount() == 0);
    publisher.stop();
    return 0;
}
