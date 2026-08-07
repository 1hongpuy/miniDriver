#pragma once

#include <functional>
#include <netinet/in.h>
#include "channel.hpp"



namespace miniKV {
namespace network {

class EventLoop;

class Acceptor{
public:
    using NewConnectionCallback = std::function<void(int sockfd, const struct sockaddr_in& addr)>;

    Acceptor(EventLoop* loop, int port);
    ~Acceptor();

    void setNewConnectionCallback(NewConnectionCallback cb){
        newConnectionCallback_ = std::move(cb);
    }

    void listen();
    void stop();

    bool listening() const{ return listening_; }
private:
    void handleRead();
    
    EventLoop* loop_;
    int acceptFd_;
    Channel acceptChannel_;
    bool listening_;
    NewConnectionCallback newConnectionCallback_;
};


}

}
























