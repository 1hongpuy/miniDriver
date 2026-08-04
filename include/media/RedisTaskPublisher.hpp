#pragma once

#include "media/MediaJob.hpp"

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

namespace miniKV {
namespace media {

struct RedisTaskPublisherConfig {
    std::string address = "127.0.0.1";
    uint16_t port = 6379;
    std::string thumbnailStream = "media:thumbnail";
    uint64_t streamMaxLen = 100000;
    size_t maxPendingJobs = 1024;
    uint32_t reconnectDelayMs = 1000;
};

class RedisTaskPublisher {
public:
    explicit RedisTaskPublisher(RedisTaskPublisherConfig config);
    ~RedisTaskPublisher();

    RedisTaskPublisher(const RedisTaskPublisher&) = delete;
    RedisTaskPublisher& operator=(const RedisTaskPublisher&) = delete;

    bool start();
    bool enqueue(const MediaJob& job);
    void stop();
    size_t pendingCount() const;

private:
    void run();
    bool publish(const MediaJob& job);

    const RedisTaskPublisherConfig config_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<MediaJob> pending_;
    std::thread thread_;
    bool started_ = false;
    bool stopping_ = false;
};

}  // namespace media
}  // namespace miniKV
