#include "network/TcpServer.hpp"
#include "network/Acceptor.hpp"
#include "network/EventLoop.hpp"
#include "network/Poller.hpp"
#include "network/TcpConnection.hpp"
#include <iostream>
#include <memory>



namespace miniKV {
namespace network {

TcpServer::TcpServer(EventLoop* loop, int port)
:loop_(loop), started_(false), nextConnId_(1), acceptor_(new Acceptor(loop, port)) 
{
    acceptor_->setNewConnectionCallback(
        [this](int sockfd, const struct sockaddr_in& peerAddr){
            newConnection(sockfd, peerAddr);}
        );
}

TcpServer::~TcpServer()
{
    for(auto& item : connections_)
    {
        TcpConnectionPtr conn(item.second);
        item.second.reset();
        conn->connectDestroyed(); //为了后面多线程准备
    }
}

void TcpServer::start()
{
    if(!started_)
    {
        started_ = true;
        acceptor_->listen();
    }
}

void TcpServer::newConnection(int sockfd, const struct sockaddr_in& peerAddr)
{
    EventLoop* ioLoop = loop_;

    TcpConnectionPtr conn = std::make_shared<TcpConnection>(ioLoop, sockfd, nextConnId_++);

    connections_[sockfd] = conn;
    
    conn->setConnectionCallback(connectionCallback_);
    conn->setMessageCallback(messageCallback_);

    conn->setInternalCloseCallback([this](const TcpConnectionPtr& conn){
        removeConnection(conn);   
    });

    conn->connectEstablished();
}

void TcpServer::removeConnection(const TcpConnectionPtr& conn)
{
    connections_.erase(conn->fd());
    conn->connectDestroyed();
}

}
}













