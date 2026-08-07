#include "network/EventLoopThread.hpp"

#include "network/EventLoop.hpp"

#include <stdexcept>

namespace miniKV::network {

EventLoopThread::~EventLoopThread()
{
    stop();
}

EventLoop* EventLoopThread::startLoop()
{
    std::unique_lock<std::mutex> lock(mutex_);
    if(started_) {
        if(loop_ == nullptr) throw std::logic_error("EventLoopThread cannot be restarted");
        return loop_;
    }

    started_ = true;
    try {
        thread_ = std::thread([this] { threadMain(); });
    } catch(...) {
        started_ = false;
        throw;
    }
    condition_.wait(lock, [this] { return loop_ != nullptr; });
    return loop_;
}

void EventLoopThread::stop() noexcept
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(loop_ != nullptr) loop_->quit();
    }

    if(thread_.joinable()) {
        if(thread_.get_id() == std::this_thread::get_id()) {
            std::terminate();
        }
        thread_.join();
    }
}

void EventLoopThread::threadMain()
{
    EventLoop loop;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop_ = &loop;
        condition_.notify_all();
    }

    loop.loop();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop_ = nullptr;
        condition_.notify_all();
    }
}

}  // namespace miniKV::network
