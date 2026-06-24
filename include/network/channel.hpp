#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <sys/epoll.h>
#include <sys/types.h>


namespace miniKV {
namespace network {

class EventLoop;

class Channel {
public:
    using EventCallback = std::function<void()>;

    Channel(EventLoop* loop, int fd) : loop_(loop), fd_(fd), events_(0), revents_(0) { }

    void handleEvent();

    void setReadCallback(EventCallback cb){
        readCallback_ = std::move(cb);
    }
    void setWriteCallback(EventCallback cb){
        writeCallback_ = std::move(cb);
    }
    void setCloseCallback(EventCallback cb){
        closeCallback_ = std::move(cb);
    }
    void setErrorCallback(EventCallback cb){
        errorCallback_ = std::move(cb);
    }

    int fd() const {return fd_;}
    uint32_t events() const {return events_;}
    void set_revents(uint32_t revent){
        revents_ = revent;
    }

    void enableReading() {
        events_ |= (EPOLLIN | EPOLLPRI);
        update();
    }
    void disableReading() {
        events_ &= ~(EPOLLIN | EPOLLPRI);
        update();
    }
    void enableWriteing() {
        events_ |= EPOLLOUT;
        update();
    }
    void disableWriting() {
        events_ &= ~EPOLLOUT;
        update();
    }
    void disableAll()
    {
        events_ = 0;
        update();
    }

    bool isWriting() const { //查询的就是是否监听
        return events_ & EPOLLOUT;
    }
    bool isReading() const {
        return events_ & EPOLLIN;
    }
    void remove();

    void tie(const std::shared_ptr<void>& obj);
private:
    void update();
    void handleEventWithGuard();

    EventLoop *loop_;
    const int fd_;
    uint32_t events_;
    uint32_t revents_;

    EventCallback readCallback_;
    EventCallback writeCallback_;
    EventCallback closeCallback_;
    EventCallback errorCallback_;

    std::weak_ptr<void> tie_;
    bool tied_ = false;
};


}
    
}



















