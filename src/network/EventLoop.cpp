#include "network/EventLoop.hpp"
#include "network/Poller.hpp"
#include "network/channel.hpp"
#include <bits/types/struct_itimerspec.h>
#include <bits/types/struct_timespec.h>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>



namespace miniKV {

namespace network {

EventLoop::EventLoop() 
    : looping_(false), 
      quit_(false), 
      callingPendingFunctors_(false),
      poller_(new Poller()),
      threadId_(std::this_thread::get_id()),
      wakeupFd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)),
      wakeupChannel_(new Channel(this, wakeupFd_))
{
    initTimer(timerFd_, timerChannel_, this, timers_, [this](){handleTimerRead(timerFd_, timers_);});
    wakeupChannel_->setReadCallback([this](){handleRead();});
    wakeupChannel_->enableReading();//注册到EPOLL
}

EventLoop::~EventLoop(){
    if(timerChannel_)
    {
        timerChannel_->disableAll();
        timerChannel_->remove();
    }
    if(timerFd_ >= 0)
    {
        ::close(timerFd_);
    }
    if(wakeupChannel_)
    {
        wakeupChannel_->disableAll();
        wakeupChannel_->remove();
    }
    ::close(wakeupFd_);
}

void EventLoop::loop(){
    threadId_ = std::this_thread::get_id();

    looping_ = true;
    quit_ = false;

    //原子操作没有人quit的时候才进行
    while(!quit_)
    {
        loopIterations_.fetch_add(1, std::memory_order_relaxed);
        activeChannels_.clear();

        poller_->poll(activeChannels_);

        for(Channel* channel : activeChannels_)
        {
            channel->handleEvent();
        }
        doPendingFunctors();
    }

    looping_ = false;
}

void EventLoop::quit(){
    quit_ = true;

    if(!isInLoopThread()) {
        wakeup();
    }
}

void EventLoop::runInLoop(Functor cb)
{
    if(isInLoopThread()) //在epoll线程
    {
        cb();
    }else {
        queueInLoop(std::move(cb));
    }
}

void EventLoop::queueInLoop(Functor cb)
{
    pendingFunctorsQueued_.fetch_add(1, std::memory_order_relaxed);
    if(!isInLoopThread()) crossThreadQueued_.fetch_add(1, std::memory_order_relaxed);
    const uint64_t depth = pendingFunctorDepth_.fetch_add(1, std::memory_order_relaxed) + 1;
    uint64_t peak = pendingFunctorPeakDepth_.load(std::memory_order_relaxed);
    while(peak < depth && !pendingFunctorPeakDepth_.compare_exchange_weak(
        peak, depth, std::memory_order_relaxed)) {}
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pendingFunctors_.push_back(std::move(cb));
    }
    if (!isInLoopThread() || callingPendingFunctors_) {
        wakeup();
    }
}

void EventLoop::addTimerInLoop(TimerEntry timer)
{
    if(cancelledTimers_.erase(timer.id) > 0)
    {
        return ;
    }
    timers_[timer.id] = std::move(timer);
    resetTimerfd(timerFd_, timers_);
}

void EventLoop::cancelInLoop(int timerId)
{
    if(timers_.erase(timerId) == 0)
    {
        cancelledTimers_.insert(timerId);
    }
    resetTimerfd(timerFd_, timers_);
}

void EventLoop::wakeup()
{
    uint64_t one = 1;
    ::write(wakeupFd_, &one, sizeof(one));
}

void EventLoop::handleRead()
{
    uint64_t one = 1;
    ::read(wakeupFd_, &one, sizeof(one));
}

void EventLoop::doPendingFunctors()
{
    std::vector<Functor> functors;
    callingPendingFunctors_ = true;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        functors.swap(pendingFunctors_);
    }
    pendingFunctorDepth_.fetch_sub(functors.size(), std::memory_order_relaxed);

    for(auto f : functors)
    {
        f();//这里没有锁，所以这里可以让工作进程，接着运行，并且这里的函数必须轻量化
    }
    pendingFunctorsExecuted_.fetch_add(functors.size(), std::memory_order_relaxed);
    callingPendingFunctors_ = false;
}  

void EventLoop::abortNotInLoopThread()
{
    std::cerr << "EventLoop::abortNotInLoopThread - "
              << "current thread is not the IO thread!" << std::endl;

    abort();           
}


//2026-7-9 新增定时器部分
void EventLoop::initTimer(int& timerFd_, std::unique_ptr<Channel>& timerChannel_,
    EventLoop* loop,
    std::map<int, TimerEntry>& timers_,
    std::function<void()> handleTimerReadFn)
{
    timerFd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    timerChannel_ = std::make_unique<Channel>(loop, timerFd_);
    timerChannel_->setReadCallback(handleTimerReadFn);
    timerChannel_->enableReading();
}
void EventLoop::handleTimerRead(int timerFd_,  std::map<int, TimerEntry>& timers_)
{
    uint64_t exp;
    ::read(timerFd_, &exp, sizeof(exp));

    int64_t now = currentTimeMs();

    std::vector<int> toRemove;
    std::vector<std::function<void()>> toRun;

    for(auto& [id, timer] : timers_)
    {
        if(timer.expiration <= now)
        {
            const uint64_t lagMs = static_cast<uint64_t>(now - timer.expiration);
            timerCallbacks_.fetch_add(1, std::memory_order_relaxed);
            if(lagMs > 0) {
                timerLateCallbacks_.fetch_add(1, std::memory_order_relaxed);
                timerLagTotalMs_.fetch_add(lagMs, std::memory_order_relaxed);
                uint64_t previous = timerLagMaxMs_.load(std::memory_order_relaxed);
                while(previous < lagMs && !timerLagMaxMs_.compare_exchange_weak(
                    previous, lagMs, std::memory_order_relaxed)) {}
            }
            toRun.push_back(timer.callback);
            if(timer.interval > 0)
            {
                timer.expiration = now + timer.interval;
            }
            else {
                toRemove.push_back(id);
            }
        }
    }

    for(int id : toRemove)
    {
        timers_.erase(id);
    }

    for(auto& cb : toRun)
    {
        cb();
    }
    resetTimerfd(timerFd_, timers_);
}
void EventLoop::resetTimerfd(int timerFd_, const std::map<int, struct TimerEntry>& timers_)
{
    //it_value（Value） = “第一次响铃的时间”（初次闹钟）。
    //it_interval（Interval） = “后续每隔多久响一次”（重复间隔）。
    if(timerFd_ < 0)
    {
        return ;
    }

    int64_t earliest = INT64_MAX;
    for(auto& [id, timer] : timers_)
    {
        if(timer.expiration < earliest)
        {
            earliest = timer.expiration;
        }
    }

    struct itimerspec newValue = {};
    if(earliest == INT64_MAX)
    {
        newValue.it_value.tv_nsec = 0;
        newValue.it_value.tv_sec  = 0;
    }
    else {
        int64_t now = currentTimeMs();  //ms单位
        int64_t delay = earliest - now ;
        if(delay < 1) delay = 1;
    
        newValue.it_value.tv_sec    = delay / 1000; //s
        newValue.it_value.tv_nsec   = (delay % 1000) * 1000000;
    }
    newValue.it_interval.tv_nsec = 0;
    newValue.it_interval.tv_sec  = 0;
    ::timerfd_settime(timerFd_, 0, &newValue, nullptr);
}
int64_t EventLoop::currentTimeMs()
{
    struct timespec ts;
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1000 + static_cast<int64_t>(ts.tv_nsec) / 1000000; 
}


int EventLoop::runAfter(int64_t delayMs, Functor cb)
{
    int64_t now = currentTimeMs();
    TimerEntry t;
    t.id = nextTimerId_.fetch_add(1);
    t.expiration = now + std::max<int64_t>(delayMs, 0);
    t.interval = 0;
    t.callback = std::move(cb);
    // timers_[t.id] = t;
    // resetTimerfd(timerFd_, timers_);
    runInLoop([this, timer = std::move(t)]() mutable {
        addTimerInLoop(std::move(timer));
    });
    return t.id;
}

EventLoop::Metrics EventLoop::metrics() const noexcept
{
    return {loopIterations_.load(std::memory_order_relaxed),
            pendingFunctorsQueued_.load(std::memory_order_relaxed),
            crossThreadQueued_.load(std::memory_order_relaxed),
            pendingFunctorsExecuted_.load(std::memory_order_relaxed),
            pendingFunctorDepth_.load(std::memory_order_relaxed),
            pendingFunctorPeakDepth_.load(std::memory_order_relaxed),
            timerCallbacks_.load(std::memory_order_relaxed),
            timerLateCallbacks_.load(std::memory_order_relaxed),
            timerLagTotalMs_.load(std::memory_order_relaxed),
            timerLagMaxMs_.load(std::memory_order_relaxed)};
}
int EventLoop::runEvery(int64_t intervalMs, Functor cb)
{
    int64_t now = currentTimeMs();
    TimerEntry t;
    t.id = nextTimerId_.fetch_add(1);
    t.expiration = now + std::max<int64_t>(intervalMs, 1);
    t.interval = std::max<int64_t>(intervalMs, 1);;
    t.callback = std::move(cb);
    // timers_[t.id] = t;
    // resetTimerfd(timerFd_, timers_);
    runInLoop([this, timer = std::move(t)]() mutable {
        addTimerInLoop(std::move(timer));
    });
    return t.id;
}
void EventLoop::cancel(int timerId) //取消定时器
{
    //不能当作输入时因为这个runInLoop是无参数输入
    runInLoop([this, timerId]() {
        cancelInLoop(timerId);
    });
}


}

}
























