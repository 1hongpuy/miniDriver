#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>

namespace miniKV::network {

class EventLoop;

class EventLoopThread {
public:
    EventLoopThread() = default;
    ~EventLoopThread();

    EventLoopThread(const EventLoopThread&) = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;

    EventLoop* startLoop();
    void stop() noexcept;

private:
    void threadMain();

    std::mutex mutex_;
    std::condition_variable condition_;
    std::thread thread_;
    EventLoop* loop_ = nullptr;
    bool started_ = false;
};

}  // namespace miniKV::network
