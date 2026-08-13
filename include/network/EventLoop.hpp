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

    struct Metrics {
        uint64_t loopIterations = 0;
        uint64_t pendingFunctorsQueued = 0;
        uint64_t crossThreadQueued = 0;
        uint64_t pendingFunctorsExecuted = 0;
        uint64_t pendingFunctorDepth = 0;
        uint64_t pendingFunctorPeakDepth = 0;
        uint64_t timerCallbacks = 0;
        uint64_t timerLateCallbacks = 0;
        uint64_t timerLagTotalMs = 0;
        uint64_t timerLagMaxMs = 0;
    };

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
    Metrics metrics() const noexcept;

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
    std::atomic<uint64_t> loopIterations_{0};
    std::atomic<uint64_t> pendingFunctorsQueued_{0};
    std::atomic<uint64_t> crossThreadQueued_{0};
    std::atomic<uint64_t> pendingFunctorsExecuted_{0};
    std::atomic<uint64_t> pendingFunctorDepth_{0};
    std::atomic<uint64_t> pendingFunctorPeakDepth_{0};
    std::atomic<uint64_t> timerCallbacks_{0};
    std::atomic<uint64_t> timerLateCallbacks_{0};
    std::atomic<uint64_t> timerLagTotalMs_{0};
    std::atomic<uint64_t> timerLagMaxMs_{0};
};

}
}


















