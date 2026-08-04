#include "media/RedisTaskPublisher.hpp"

#include <hiredis/hiredis.h>

#include <chrono>
#include <utility>

namespace miniKV {
namespace media {

RedisTaskPublisher::RedisTaskPublisher(RedisTaskPublisherConfig config)
    : config_(std::move(config))
{
}

RedisTaskPublisher::~RedisTaskPublisher()
{
    stop();
}

bool RedisTaskPublisher::start()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (started_ || stopping_ || config_.address.empty() || config_.port == 0 ||
        config_.thumbnailStream.empty() || config_.streamMaxLen == 0 ||
        config_.maxPendingJobs == 0) {
        return false;
    }
    started_ = true;
    thread_ = std::thread(&RedisTaskPublisher::run, this);
    return true;
}

bool RedisTaskPublisher::enqueue(const MediaJob& job)
{
    if (job.jobId.empty() || job.type != JobType::kThumbnail || job.profile.empty()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_ || pending_.size() >= config_.maxPendingJobs) return false;
    pending_.push_back(job);
    cv_.notify_one();
    return true;
}

void RedisTaskPublisher::stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) return;
        stopping_ = true;
        cv_.notify_all();
    }
    if (thread_.joinable()) thread_.join();
}

size_t RedisTaskPublisher::pendingCount() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_.size();
}

void RedisTaskPublisher::run()
{
    for (;;) {
        MediaJob job;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return stopping_ || !pending_.empty(); });
            if (stopping_) return;
            job = std::move(pending_.front());
            pending_.pop_front();
        }

        if (publish(job)) continue;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!stopping_) pending_.push_front(std::move(job));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.reconnectDelayMs));
    }
}

bool RedisTaskPublisher::publish(const MediaJob& job)
{
    timeval timeout{};
    timeout.tv_sec = 1;
    redisContext* context = redisConnectWithTimeout(config_.address.c_str(), config_.port, timeout);
    if (context == nullptr) return false;
    if (context->err != 0) {
        redisFree(context);
        return false;
    }

    redisReply* reply = static_cast<redisReply*>(redisCommand(
        context,
        "XADD %s MAXLEN ~ %llu * jobId %s type thumbnail profile %s",
        config_.thumbnailStream.c_str(),
        static_cast<unsigned long long>(config_.streamMaxLen),
        job.jobId.c_str(), job.profile.c_str()));
    const bool success = reply != nullptr && reply->type != REDIS_REPLY_ERROR;
    if (reply != nullptr) freeReplyObject(reply);
    redisFree(context);
    return success;
}

}  // namespace media
}  // namespace miniKV
