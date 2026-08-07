#include "network/EventLoopThreadPool.hpp"

#include "network/EventLoop.hpp"
#include "network/EventLoopThread.hpp"

#include <stdexcept>

namespace miniKV::network {

EventLoopThreadPool::EventLoopThreadPool(EventLoop* baseLoop)
    : baseLoop_(baseLoop)
{
    if(baseLoop_ == nullptr) throw std::invalid_argument("base EventLoop is required");
}

EventLoopThreadPool::~EventLoopThreadPool()
{
    stop();
}

void EventLoopThreadPool::setThreadNum(size_t count)
{
    if(started_) throw std::logic_error("cannot resize a running EventLoopThreadPool");
    threadCount_ = count;
}

void EventLoopThreadPool::start()
{
    if(started_) return;
    if(!baseLoop_->isInLoopThread()) {
        throw std::logic_error("EventLoopThreadPool must start on its base loop");
    }

    started_ = true;
    next_ = 0;
    try {
        threads_.reserve(threadCount_);
        loops_.reserve(threadCount_);
        for(size_t index = 0; index < threadCount_; ++index) {
            auto thread = std::make_unique<EventLoopThread>();
            EventLoop* loop = thread->startLoop();
            loops_.push_back(loop);
            threads_.push_back(std::move(thread));
        }
    } catch(...) {
        for(auto& thread : threads_) thread->stop();
        loops_.clear();
        threads_.clear();
        started_ = false;
        throw;
    }
}

void EventLoopThreadPool::stop() noexcept
{
    if(!started_) return;
    for(auto& thread : threads_) thread->stop();
    loops_.clear();
    threads_.clear();
    next_ = 0;
    started_ = false;
}

EventLoop* EventLoopThreadPool::nextLoop()
{
    if(!started_) throw std::logic_error("EventLoopThreadPool is not started");
    if(!baseLoop_->isInLoopThread()) {
        throw std::logic_error("nextLoop must run on the base EventLoop");
    }
    if(loops_.empty()) return baseLoop_;

    EventLoop* selected = loops_[next_];
    next_ = (next_ + 1) % loops_.size();
    return selected;
}

}  // namespace miniKV::network
