#pragma once

#include <cstddef>
#include <memory>
#include <vector>

namespace miniKV::network {

class EventLoop;
class EventLoopThread;

class EventLoopThreadPool {
public:
    explicit EventLoopThreadPool(EventLoop* baseLoop);
    ~EventLoopThreadPool();

    EventLoopThreadPool(const EventLoopThreadPool&) = delete;
    EventLoopThreadPool& operator=(const EventLoopThreadPool&) = delete;

    void setThreadNum(size_t count);
    void start();
    void stop() noexcept;
    EventLoop* nextLoop();
    size_t size() const noexcept { return loops_.size(); }

private:
    EventLoop* baseLoop_;
    size_t threadCount_ = 0;
    size_t next_ = 0;
    bool started_ = false;
    std::vector<std::unique_ptr<EventLoopThread>> threads_;
    std::vector<EventLoop*> loops_;
};

}  // namespace miniKV::network
