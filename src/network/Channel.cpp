#include "network/channel.hpp"
#include "network/EventLoop.hpp"
#include <memory>
#include <sys/epoll.h>


namespace miniKV {

namespace network {


void Channel::tie(const std::shared_ptr<void>& obj)
{
    tie_ = obj;
    tied_ = true;
}
void Channel::handleEvent()
{
    if(tied_) //weak_ptr
    {
        std::shared_ptr<void> guard = tie_.lock();//
        if(guard)
        {
            handleEventWithGuard();
        }else{

        }
    }
    else {
        handleEventWithGuard();
    }
}
void Channel::handleEventWithGuard()
{
    /*
    EPOLLHUP 挂起，并且EPOLLIN为0
    */
    if((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN))
    {
        if(closeCallback_) closeCallback_();
    }
    if(revents_ & EPOLLERR)
    {
        if(errorCallback_) errorCallback_();
    }
    if(revents_ & (EPOLLIN | EPOLLRDHUP | EPOLLPRI))
    {
        if(readCallback_) readCallback_();
    }
    if(revents_ & (EPOLLOUT))
    {
        if(writeCallback_) writeCallback_();
    }
}

void Channel::update()
{
    loop_->updateChannel(this);
}

void Channel::remove()
{
    loop_->removeChannel(this);
}

}

}










