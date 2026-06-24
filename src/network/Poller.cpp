#include "network/Poller.hpp"
#include "network/channel.hpp"
#include <cstddef>
#include <sys/epoll.h>
#include <unistd.h>
#include <cstring>
#include <iostream>

namespace miniKV {
namespace network {

Poller::Poller() : epollfd_(epoll_create1(EPOLL_CLOEXEC)), events_(16) {

}
Poller::~Poller() {
    close(epollfd_);
}


void Poller::poll(std::vector<Channel *>& activeChannels)
{
    int numEvents = epoll_wait(epollfd_, events_.data(), static_cast<int>(events_.size()), -1);

    if(numEvents > 0){
        for(int i = 0; i < numEvents; i++)
        {
            Channel* channel = static_cast<Channel *>(events_[i].data.ptr);
            channel->set_revents(events_[i].events);
            activeChannels.push_back(channel);
        }
        if(numEvents == events_.size())
        {
            events_.resize(events_.size() * 2);
        }
    }
}

void Poller::updateChannel(Channel *channel)
{
    struct epoll_event ev = {};
    ev.events = channel->events();
    ev.data.ptr = channel;

    int fd = channel->fd();

    if(channels_.find(fd) == channels_.end())
    {
        channels_[fd] = channel;
        epoll_ctl(epollfd_, EPOLL_CTL_ADD, fd, &ev);
    }
    else{
        epoll_ctl(epollfd_, EPOLL_CTL_MOD, fd, &ev);
    }
}

void Poller::removeChannel(Channel *channel)
{
    int fd = channel->fd();
    channels_.erase(fd);
    epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd,  nullptr);
}

}

}
























