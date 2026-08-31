#include "media/RedisAiEventPublisher.hpp"

#include <hiredis/hiredis.h>

#include <chrono>
#include <utility>

namespace miniKV {
namespace media {

RedisAiEventPublisher::RedisAiEventPublisher(RedisAiEventPublisherConfig config,
                                             PublishedCallback onPublished)
    : config_(std::move(config)), onPublished_(std::move(onPublished))
{
}

RedisAiEventPublisher::~RedisAiEventPublisher()
{
    stop();
}

bool RedisAiEventPublisher::start()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if(started_ || stopping_ || config_.address.empty() || config_.port == 0 ||
       config_.stream.empty() || config_.streamMaxLen == 0 || config_.maxPendingEvents == 0) {
        return false;
    }
    started_ = true;
    thread_ = std::thread(&RedisAiEventPublisher::run, this);
    return true;
}

bool RedisAiEventPublisher::enqueue(AiIndexEvent event)
{
    if(event.eventId.empty() || event.objectId.empty() || event.fileHash.empty() || event.fileSize == 0) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if(stopping_ || pending_.size() >= config_.maxPendingEvents) return false;
    pending_.push_back(std::move(event));
    cv_.notify_one();
    return true;
}

void RedisAiEventPublisher::stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(stopping_) return;
        stopping_ = true;
        cv_.notify_all();
    }
    if(thread_.joinable()) thread_.join();
}

size_t RedisAiEventPublisher::pendingCount() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_.size();
}

void RedisAiEventPublisher::run()
{
    for(;;) {
        AiIndexEvent event;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return stopping_ || !pending_.empty(); });
            if(stopping_) return;
            event = std::move(pending_.front());
            pending_.pop_front();
        }
        if(publish(event)) {
            if(onPublished_) onPublished_(event);
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(!stopping_) pending_.push_front(std::move(event));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.reconnectDelayMs));
    }
}

bool RedisAiEventPublisher::publish(const AiIndexEvent& event)
{
    timeval timeout{};
    timeout.tv_sec = 1;
    redisContext* context = redisConnectWithTimeout(config_.address.c_str(), config_.port, timeout);
    if(context == nullptr) return false;
    if(context->err != 0) {
        redisFree(context);
        return false;
    }
    redisReply* reply = static_cast<redisReply*>(redisCommand(
        context,
        "XADD %s MAXLEN ~ %llu * eventId %s eventType FILE_UPLOAD_COMMITTED "
        "objectId %s objectKey %s fileHash %s fileSize %llu objectVersion %llu "
        "metadataVersion %llu occurredAt %lld",
        config_.stream.c_str(), static_cast<unsigned long long>(config_.streamMaxLen),
        event.eventId.c_str(), event.objectId.c_str(), event.objectKey.c_str(), event.fileHash.c_str(),
        static_cast<unsigned long long>(event.fileSize),
        static_cast<unsigned long long>(event.objectVersion),
        static_cast<unsigned long long>(event.metadataVersion),
        static_cast<long long>(event.occurredAt)));
    const bool success = reply != nullptr && reply->type != REDIS_REPLY_ERROR;
    if(reply != nullptr) freeReplyObject(reply);
    redisFree(context);
    return success;
}

}  // namespace media
}  // namespace miniKV
