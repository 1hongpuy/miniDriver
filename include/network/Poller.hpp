#pragma once


#include <vector>
#include <map>
#include <sys/epoll.h>
#include "channel.hpp"


namespace miniKV {

namespace network {

class Poller{
public:
    Poller();
    ~Poller();

    void poll(std::vector<Channel*>& activeChannels);

    void updateChannel(Channel* channel);
    void removeChannel(Channel* channel);

private:
    int epollfd_;
    std::vector<struct epoll_event> events_;
    std::map<int, Channel*> channels_;
};

}
}















