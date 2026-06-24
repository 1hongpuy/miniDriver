#pragma once

#include "EventLoop.hpp"
#include "Acceptor.hpp"
#include "TcpConnection.hpp"
#include <map>
#include <memory>
#include <string>
#include <functional>

namespace miniKV {
namespace network {

class TcpServer{
public:
    TcpServer(EventLoop* loop, int port);
    ~TcpServer();

    void start();

    void setConnectionCallback(ConnectionCallback cb) {connectionCallback_ = std::move(cb);}
    void setMessageCallback(MessageCallback cb) {messageCallback_ = std::move(cb);}

private:
    void newConnection(int sockfd, const struct sockaddr_in& peerAddr);

    void removeConnection(const TcpConnectionPtr& conn);

    EventLoop* loop_;
    std::unique_ptr<Acceptor> acceptor_;

    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;

    bool started_;
    int nextConnId_;
    std::map<int, TcpConnectionPtr> connections_;
};    


}


}


















