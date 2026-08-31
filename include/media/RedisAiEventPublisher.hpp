#pragma once

#include "media/AiIndexEvent.hpp"

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

namespace miniKV {
namespace media {

struct RedisAiEventPublisherConfig {
    std::string address = "127.0.0.1";
    uint16_t port = 6379;
    std::string stream = "minidrive:file-events";
    uint64_t streamMaxLen = 100000;
    size_t maxPendingEvents = 1024;
    uint32_t reconnectDelayMs = 1000;
};

// This publisher provides bounded in-process buffering only. Durable recovery
// comes from GatewayState's LevelDB outbox, which re-enqueues unpublished events
// after a process restart.
class RedisAiEventPublisher {
public:
    using PublishedCallback = std::function<void(const AiIndexEvent&)>;

    RedisAiEventPublisher(RedisAiEventPublisherConfig config, PublishedCallback onPublished);
    ~RedisAiEventPublisher();
    RedisAiEventPublisher(const RedisAiEventPublisher&) = delete;
    RedisAiEventPublisher& operator=(const RedisAiEventPublisher&) = delete;

    bool start();
    bool enqueue(AiIndexEvent event);
    void stop();
    size_t pendingCount() const;

private:
    void run();
    bool publish(const AiIndexEvent& event);

    const RedisAiEventPublisherConfig config_;
    const PublishedCallback onPublished_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<AiIndexEvent> pending_;
    std::thread thread_;
    bool started_ = false;
    bool stopping_ = false;
};

}  // namespace media
}  // namespace miniKV
