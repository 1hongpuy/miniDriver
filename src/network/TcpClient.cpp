#include "network/TcpClient.hpp"
#include "network/Buffer.hpp"
#include "network/EventLoop.hpp"
#include "network/TcpConnection.hpp"
#include "network/channel.hpp"
#include <cassert>
#include <memory>
#include <unistd.h>
#include <utility>





namespace miniKV {
namespace network {


TcpClient::Ptr TcpClient::create(EventLoop *loop)
{
    return Ptr(new TcpClient(loop));
}

TcpClient::TcpClient(EventLoop* loop) : loop_(loop){}

TcpClient::~TcpClient() {
    assert(!channel_);
    assert(!connection_);
}
//系统约定：当连接建立成功（三次握手完成）时，
// 该 socket 变为可写（Writeable）。因此，epoll 会触发 EPOLLOUT 事件
void TcpClient::connect(const std::string &addr, int port)
{
    Ptr self(shared_from_this());
    loop_->runInLoop([self, addr, port](){
        self->connectInLoop(addr, port);
    });
}

void TcpClient::connectInLoop(std::string addr, int port)
{
    if(state_ != kDisconnected)
    {
        return ;
    }
    holdLifetimeInLoop();

    int sockfd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,  0);
    if(sockfd < 0)
    {
        if(connectionCallback_) connectionCallback_(nullptr);
        releaseLifetimeInLoop();
        return ;
    }

    struct sockaddr_in servaddr = {};
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    if(::inet_pton(AF_INET, addr.c_str(), &servaddr.sin_addr) <= 0)
    {
        ::close(sockfd);
        if(connectionCallback_) connectionCallback_(nullptr);
        releaseLifetimeInLoop();
        return ;
    }

    int ret = ::connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr));
    if(ret == 0)
    {
        state_ = kConnected;
        onNewConnection(sockfd);
    }
    else if(errno == EINPROGRESS) //操作进行中
    {
        state_ = kConnecting;
        channel_ = std::make_unique<Channel>(loop_, sockfd);
        std::weak_ptr<TcpClient> weakSelf(shared_from_this());
        channel_->setWriteCallback([weakSelf](){
            if(Ptr self = weakSelf.lock())
            {
                self->handleWrite();
            }
        });
        channel_->setErrorCallback([weakSelf](){
            if(Ptr self = weakSelf.lock()) self->failConnect();
        });
        //这个channel生命周期跟着tcpclient一起的
        channel_->enableWriteing();
        if(connectTimeoutMs_ > 0)
        {
            connectTimerId_ = loop_->runAfter(connectTimeoutMs_, [weakSelf]() {
                if(Ptr self = weakSelf.lock()) self->handleConnectTimeout();
            });
        }
    }
    else {
        ::close(sockfd);
        if(connectionCallback_) connectionCallback_(nullptr);
        releaseLifetimeInLoop();
    }
}

void TcpClient::finishStopInLoop(StopCallback callback)
{
    if(callback) callback();
    releaseLifetimeInLoop();
}

void TcpClient::holdLifetimeInLoop()
{
    if(!lifetimeGuard_)
    {
        lifetimeGuard_ = shared_from_this();
    }
}

void TcpClient::releaseLifetimeInLoop()
{
    lifetimeGuard_.reset(); 
}

//handleWrite 只在“连接尚未确定结果”的状态下被调用。 它必须立即确认结果，并决定下一步是“转入正式连接状态”还是“宣告失败”。handleWrite 只在“连接尚未确定结果”的状态下被调用。 它必须立即确认结果，并决定下一步是“转入正式连接状态”还是“宣告失败”。
/*
返回 -1 / EINPROGRESS：连接还在握手
                    |
                    v
             为 fd 创建临时 Channel
             监听 EPOLLOUT / EPOLLERR
                    |
                    v
             epoll 触发 Channel::handleEvent()
                    |
                    v
             TcpClient::handleWrite()
*/
inline void TcpClient::handleWrite() //??
{

    if(state_ != kConnecting || !channel_)
    {
        return;
    }

    int err = 0;
    socklen_t len = sizeof(err);
    if(::getsockopt(channel_->fd(), SOL_SOCKET, SO_ERROR, &err, &len) < 0)
    {
        //检查这个部分
        err = errno;
    }

    const int sockfd = channel_->fd();

    if(err == 0)
    {
        removeConnectingChannel(false);
        cancelConnectTimer();
        state_ = kConnected;
        onNewConnection(sockfd);
    }else {
       failConnect();
    }
}

void TcpClient::onNewConnection(int sockfd) //连接成功
{
    int opt = 1;
    //必须在 connect() / listen() 之前调
    ::setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    //目的：禁用 Nagle 算法，禁止将多个小数据包合并成一个大包发送。

    connection_ = std::make_shared<TcpConnection>(loop_, sockfd, nectConnId_++);

    if(messageCallback_)
    {
        connection_->setMessageCallback(messageCallback_);
    }
    connection_->setConnectionCallback(connectionCallback_);
    std::weak_ptr<TcpClient> weakSelf(shared_from_this());
    connection_->setInternalCloseCallback([weakSelf](const TcpConnectionPtr& conn){
        if(Ptr self = weakSelf.lock())
        {
            self->removeConnection(conn);
        }
    });
    connection_->connectEstablished();
}

void TcpClient::removeConnection(const TcpConnectionPtr& conn)
{
    if(connection_ != conn)
    {
        return ;
    }

    state_ = kDisconnected;
    conn->connectDestroyed();
    connection_.reset();
    releaseLifetimeInLoop();
}

void TcpClient::cancelConnectTimer()
{
    if(connectTimerId_ != 0)
    {
        loop_->cancel(connectTimerId_);
        connectTimerId_ = 0;
    }
}

void TcpClient::stop(StopCallback callback)
{
    Ptr self(shared_from_this());
    loop_->runInLoop([self, callback = std::move(callback)]() mutable  {
        self->stopInLoop(std::move(callback));
    });
}

void TcpClient::stopInLoop(StopCallback callback)
{
    if(state_ == kDisconnected && !channel_ && !connection_)
    {
        finishStopInLoop(std::move(callback));
        return ;
    }

    state_ = kClosing;
    cancelConnectTimer();

    const bool sockIsOwnedByConnection = connection_ && channel_ && connection_->fd() == channel_->fd();
    if(connection_)
    {
        connection_->setInternalCloseCallback(CloseCallback());
        connection_->connectDestroyed();
        connection_.reset();
    }
    state_ = kDisconnected;
    removeConnectingChannel(!sockIsOwnedByConnection, [self = shared_from_this(), callback = std::move(callback)]() mutable {
        self->finishStopInLoop(callback);
    });
}

void TcpClient::disconnect()
{
    Ptr self(shared_from_this());
    loop_->runInLoop([self](){
        self->disconnectInLoop();
    });
}


void TcpClient::disconnectInLoop()
{
    if(state_ == kConnecting)
    {
        failConnect();
        return;
    }
    if(state_ == kConnected && connection_)
    {
        state_ = kClosing;
        connection_->shutdown();
    }
}

void TcpClient::handleConnectTimeout()
{
    if(state_ == kConnecting)
    {
        failConnect();
    }
}

void TcpClient::failConnect()
{
    if(state_ != kConnecting)
    {
        return ;
    }

    cancelConnectTimer();
    state_  = kClosing;
    Ptr self(shared_from_this());
    removeConnectingChannel(true, [self](){
        self ->state_ = kDisconnected;
        if(self->connectionCallback_)
        {
            self->connectionCallback_(nullptr);
        }
        self->releaseLifetimeInLoop();
    });
}

void TcpClient::removeConnectingChannel(bool closeSocket, StopCallback callback)
{
    if(!channel_)
    {
        if(callback) callback();
        return;
    }

    const int sockfd = channel_->fd();
    channel_->disableAll();
    channel_->remove();
    Ptr self(shared_from_this());
    loop_->queueInLoop([self, sockfd, closeSocket, callback = std::move(callback)]() mutable {
        if(self->channel_ && self->channel_->fd() == sockfd)
        {
            self->channel_.reset();
        }
        if(closeSocket)
        {
            ::close(sockfd);
        }
        if(callback)
        {
            callback();
        }
    });
}



}

}












