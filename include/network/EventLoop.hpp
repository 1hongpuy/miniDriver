#pragma once

#include <cstdint>
#include <future>
#include <map>
#include <vector>
#include <memory>
#include <atomic>
#include <functional>
#include <thread>
#include <mutex>
#include "Poller.hpp"
#include "channel.hpp"
#include <set>

namespace miniKV {
namespace network {




class EventLoop{
public:
    using Functor = std::function<void()>;

    EventLoop();
    ~EventLoop();

    void loop();
    void quit();

    void updateChannel(Channel* channel) {
        poller_->updateChannel(channel);
    }

    void removeChannel(Channel* channel) {
        poller_->removeChannel(channel);
    }

    void runInLoop(Functor cb);

    void queueInLoop(Functor cb);

    bool isInLoopThread() const {
        return threadId_ == std::this_thread::get_id();
    }

    //定时器部分
    int runAfter(int64_t delayMs, Functor cb);
    int runEvery(int64_t intervalMs, Functor cb);
    void cancel(int timerId); //取消定时器

private:
    struct TimerEntry{
        int id;
        int64_t expiration; 
        int64_t interval;
        std::function<void()> callback;
    };
    void abortNotInLoopThread();
    void wakeup();
    void handleRead();
    void doPendingFunctors();

    void initTimer(int& timerFd_, std::unique_ptr<Channel>& timerChannel_,
        EventLoop* loop,
        std::map<int, TimerEntry>& timers_,
        std::function<void()> handleTimerReadFn);

    void addTimerInLoop(TimerEntry timer);
    void cancelInLoop(int timerId);
    void handleTimerRead(int timerFd_,
        std::map<int, TimerEntry>& timers_);
    void resetTimerfd(int timerFd_, const std::map<int, struct TimerEntry>& timers_);
    static int64_t currentTimeMs();

    //timer
    int timerFd_ = -1;
    std::unique_ptr<Channel> timerChannel_;
    std::map<int, TimerEntry> timers_;
    std::set<int> cancelledTimers_; //被取消的定时器id
    std::atomic<int> nextTimerId_{1};//发放的定时器id


    std::atomic<bool> looping_;
    std::atomic<bool> quit_;
    bool callingPendingFunctors_ = false;
    std::unique_ptr<Poller> poller_;
    std::vector<Channel*> activeChannels_;

    std::thread::id threadId_;
    int wakeupFd_;
    std::unique_ptr<Channel> wakeupChannel_;
    std::mutex mutex_;
    std::vector<Functor> pendingFunctors_;
};

}
}




















