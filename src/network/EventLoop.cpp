#include "network/EventLoop.hpp"
#include "network/Poller.hpp"
#include "network/channel.hpp"
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <sys/eventfd.h>
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
    wakeupChannel_->setReadCallback([this](){handleRead();});
    wakeupChannel_->enableReading();//注册到EPOLL
}

EventLoop::~EventLoop(){
    ::close(wakeupFd_);
}

void EventLoop::loop(){
    threadId_ = std::this_thread::get_id();

    looping_ = true;
    quit_ = false;

    //原子操作没有人quit的时候才进行
    while(!quit_)
    {
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
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pendingFunctors_.push_back(std::move(cb));
    }
    if (!isInLoopThread() || callingPendingFunctors_) {
        wakeup();
    }
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

    for(auto f : functors)
    {
        f();//这里没有锁，所以这里可以让工作进程，接着运行，并且这里的函数必须轻量化
    }
    callingPendingFunctors_ = false;
}  

void EventLoop::abortNotInLoopThread()
{
    std::cerr << "EventLoop::abortNotInLoopThread - "
              << "current thread is not the IO thread!" << std::endl;

    abort();           
}

}

}


























