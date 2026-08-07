#include "network/TcpServer.hpp"
#include "network/Acceptor.hpp"
#include "network/EventLoop.hpp"
#include "network/Poller.hpp"
#include "network/TcpConnection.hpp"
#include "network/EventLoopThreadPool.hpp"
#include <cassert>
#include <future>
#include <iostream>
#include <memory>
#include <set>
#include <stdexcept>
#include <vector>



namespace miniKV {
namespace network {

TcpServer::TcpServer(EventLoop* loop, int port)
:loop_(loop), acceptor_(new Acceptor(loop, port)),
 threadPool_(new EventLoopThreadPool(loop)), started_(false), nextConnId_(1)
{
    acceptor_->setNewConnectionCallback(
        [this](int sockfd, const struct sockaddr_in& peerAddr){
            newConnection(sockfd, peerAddr);}
        );
}

TcpServer::~TcpServer()
{
    stop();
}

void TcpServer::setThreadNum(size_t count)
{
    if(started_) throw std::logic_error("cannot resize a running TcpServer");
    threadPool_->setThreadNum(count);
}

void TcpServer::start()
{
    if(!started_)
    {
        assert(loop_->isInLoopThread());
        threadPool_->start();
        started_ = true;
        acceptor_->listen();
    }
}

void TcpServer::stop()
{
    if(!started_) return;
    assert(loop_->isInLoopThread());
    acceptor_->stop();
    started_ = false;

    std::vector<TcpConnectionPtr> connections;
    connections.reserve(connections_.size());
    for(auto& [fd, connection] : connections_) {
        (void)fd;
        connections.push_back(std::move(connection));
    }
    connections_.clear();

    std::set<EventLoop*> workerLoops;
    for(const auto& connection : connections) {
        EventLoop* owner = connection->ownerLoop();
        if(owner == loop_) connection->connectDestroyed();
        else {
            workerLoops.insert(owner);
            owner->queueInLoop([connection] { connection->connectDestroyed(); });
        }
    }

    std::vector<std::future<void>> barriers;
    barriers.reserve(workerLoops.size());
    for(EventLoop* worker : workerLoops) {
        auto barrier = std::make_shared<std::promise<void>>();
        barriers.push_back(barrier->get_future());
        worker->queueInLoop([barrier] { barrier->set_value(); });
    }
    for(auto& barrier : barriers) barrier.get();
    connections.clear();
    threadPool_->stop();
}

void TcpServer::newConnection(int sockfd, const struct sockaddr_in& peerAddr)
{
    assert(loop_->isInLoopThread());
    EventLoop* ioLoop = threadPool_->nextLoop();

    TcpConnectionPtr conn = std::make_shared<TcpConnection>(ioLoop, sockfd, nextConnId_++);

    connections_[sockfd] = conn;
    
    conn->setConnectionCallback(connectionCallback_);
    conn->setMessageCallback(messageCallback_);

    conn->setInternalCloseCallback([this](const TcpConnectionPtr& conn){
        removeConnection(conn);   
    });

    ioLoop->runInLoop([conn] { conn->connectEstablished(); });
}

void TcpServer::removeConnection(const TcpConnectionPtr& conn)
{
    loop_->runInLoop([this, conn] { removeConnectionInLoop(conn); });
}

void TcpServer::removeConnectionInLoop(const TcpConnectionPtr& conn)
{
    assert(loop_->isInLoopThread());
    auto found = connections_.find(conn->fd());
    if(found == connections_.end() || found->second != conn) return;
    connections_.erase(found);
    conn->ownerLoop()->queueInLoop([conn] { conn->connectDestroyed(); });
}

}
}











