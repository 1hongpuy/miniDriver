#include "media/RedisTaskConsumer.hpp"

#include <hiredis/hiredis.h>

#include <chrono>
#include <utility>
#include <vector>

namespace miniKV {
namespace media {
namespace {

bool isError(const redisReply* reply)
{
    return reply == nullptr || reply->type == REDIS_REPLY_ERROR;
}

void collectEntries(const redisReply* entries, std::vector<RedisTaskMessage>& messages)
{
    if(entries == nullptr || entries->type != REDIS_REPLY_ARRAY) return;
    for(size_t index = 0; index < entries->elements; ++index) {
        const redisReply* entry = entries->element[index];
        if(entry == nullptr || entry->type != REDIS_REPLY_ARRAY || entry->elements != 2) continue;
        const redisReply* id = entry->element[0];
        const redisReply* fields = entry->element[1];
        if(id == nullptr || id->type != REDIS_REPLY_STRING || fields == nullptr ||
           fields->type != REDIS_REPLY_ARRAY) continue;
        RedisTaskMessage message;
        message.id.assign(id->str, id->len);
        for(size_t field = 0; field + 1 < fields->elements; field += 2) {
            const redisReply* key = fields->element[field];
            const redisReply* value = fields->element[field + 1];
            if(key == nullptr || value == nullptr || key->str == nullptr || value->str == nullptr) continue;
            const std::string name(key->str, key->len);
            if(name == "jobId") message.jobId.assign(value->str, value->len);
            if(name == "profile") message.profile.assign(value->str, value->len);
        }
        if(!message.id.empty() && !message.jobId.empty()) messages.push_back(std::move(message));
    }
}

void drainAcknowledgements(redisContext* context, const RedisTaskConsumerConfig& config,
                           std::deque<std::string>& acknowledgements)
{
    while(!acknowledgements.empty()) {
        const std::string id = std::move(acknowledgements.front());
        acknowledgements.pop_front();
        redisReply* reply = static_cast<redisReply*>(redisCommand(
            context, "XACK %s %s %s", config.thumbnailStream.c_str(), config.group.c_str(), id.c_str()));
        if(reply != nullptr) freeReplyObject(reply);
    }
}

}  // namespace

RedisTaskConsumer::RedisTaskConsumer(RedisTaskConsumerConfig config)
    : config_(std::move(config))
{
}

RedisTaskConsumer::~RedisTaskConsumer()
{
    stop();
}

bool RedisTaskConsumer::start(MessageCallback callback)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if(started_ || stopping_ || !callback || config_.address.empty() || config_.port == 0 ||
       config_.thumbnailStream.empty() || config_.group.empty() || config_.consumer.empty()) return false;
    callback_ = std::move(callback);
    started_ = true;
    thread_ = std::thread(&RedisTaskConsumer::run, this);
    return true;
}

void RedisTaskConsumer::acknowledge(const std::string& messageId)
{
    if(messageId.empty()) return;
    std::lock_guard<std::mutex> lock(mutex_);
    if(stopping_) return;
    acknowledgements_.push_back(messageId);
    cv_.notify_one();
}

void RedisTaskConsumer::stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(stopping_) return;
        stopping_ = true;
        cv_.notify_all();
    }
    if(thread_.joinable()) thread_.join();
}

void RedisTaskConsumer::run()
{
    while(true) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(stopping_) return;
        }
        timeval timeout{};
        timeout.tv_sec = 1;
        redisContext* context = redisConnectWithTimeout(config_.address.c_str(), config_.port, timeout);
        if(context == nullptr || context->err != 0) {
            if(context != nullptr) redisFree(context);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }
        redisReply* group = static_cast<redisReply*>(redisCommand(
            context, "XGROUP CREATE %s %s 0 MKSTREAM", config_.thumbnailStream.c_str(),
            config_.group.c_str()));
        if(group != nullptr) freeReplyObject(group);  // BUSYGROUP is expected after first startup.

        for(;;) {
            std::deque<std::string> acknowledgements;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if(stopping_) { redisFree(context); return; }
                acknowledgements.swap(acknowledgements_);
            }
            drainAcknowledgements(context, config_, acknowledgements);

            std::vector<RedisTaskMessage> claimed;
            redisReply* stale = static_cast<redisReply*>(redisCommand(
                context, "XAUTOCLAIM %s %s %s %u 0-0 COUNT 8", config_.thumbnailStream.c_str(),
                config_.group.c_str(), config_.consumer.c_str(), config_.reclaimIdleMs));
            if(stale != nullptr && stale->type == REDIS_REPLY_ARRAY && stale->elements >= 2) {
                collectEntries(stale->element[1], claimed);
            }
            if(stale != nullptr) freeReplyObject(stale);

            if(claimed.empty()) {
                redisReply* reply = static_cast<redisReply*>(redisCommand(
                    context, "XREADGROUP GROUP %s %s COUNT 1 BLOCK %u STREAMS %s >",
                    config_.group.c_str(), config_.consumer.c_str(), config_.blockMs,
                    config_.thumbnailStream.c_str()));
                if(isError(reply)) {
                    if(reply != nullptr) freeReplyObject(reply);
                    break;
                }
                if(reply != nullptr && reply->type == REDIS_REPLY_ARRAY && reply->elements > 0) {
                    const redisReply* stream = reply->element[0];
                    if(stream != nullptr && stream->type == REDIS_REPLY_ARRAY && stream->elements == 2) {
                        collectEntries(stream->element[1], claimed);
                    }
                }
                if(reply != nullptr) freeReplyObject(reply);
            }
            for(RedisTaskMessage& message : claimed) callback_(std::move(message));
        }
        redisFree(context);
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
}

}  // namespace media
}  // namespace miniKV
