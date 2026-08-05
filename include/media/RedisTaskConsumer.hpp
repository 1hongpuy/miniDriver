#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

namespace miniKV {
namespace media {

struct RedisTaskConsumerConfig {
    std::string address = "127.0.0.1";
    uint16_t port = 6379;
    std::string thumbnailStream = "media:thumbnail";
    std::string group = "thumbnail-workers";
    std::string consumer;
    uint32_t blockMs = 500;
    uint32_t reclaimIdleMs = 300000;
};

struct RedisTaskMessage {
    std::string id;
    std::string jobId;
    std::string profile;
};

class RedisTaskConsumer {
public:
    using MessageCallback = std::function<void(RedisTaskMessage)>;

    explicit RedisTaskConsumer(RedisTaskConsumerConfig config);
    ~RedisTaskConsumer();
    RedisTaskConsumer(const RedisTaskConsumer&) = delete;
    RedisTaskConsumer& operator=(const RedisTaskConsumer&) = delete;

    bool start(MessageCallback callback);
    void acknowledge(const std::string& messageId);
    void stop();

private:
    void run();

    const RedisTaskConsumerConfig config_;
    MessageCallback callback_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<std::string> acknowledgements_;
    std::thread thread_;
    bool started_ = false;
    bool stopping_ = false;
};

}  // namespace media
}  // namespace miniKV
