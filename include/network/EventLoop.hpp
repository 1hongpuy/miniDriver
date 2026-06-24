#pragma once

#include <future>
#include <vector>
#include <memory>
#include <atomic>
#include <functional>
#include <thread>
#include <mutex>
#include "Poller.hpp"
#include "channel.hpp"


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

private:
    void abortNotInLoopThread();
    void wakeup();
    void handleRead();
    void doPendingFunctors();

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




















