#include "network/TcpConnection.hpp"
#include "network/EventLoop.hpp"
#include "network/channel.hpp"
#include <asm-generic/errno-base.h>
#include <asm-generic/errno.h>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <fcntl.h>
#include <memory>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>



namespace miniKV {
namespace network {

TcpConnection::TcpConnection(EventLoop* loop, int fd, int id)
: loop_(loop), fd_(fd), id_(id), state_(kConnectiong), channel_(new Channel(loop, fd))
{
    channel_->setReadCallback([this](){handleRead();});
    channel_->setCloseCallback([this](){handleClose();});
    channel_->setErrorCallback([this](){handleError();});
    channel_->setWriteCallback([this](){handleWrite();});
}

TcpConnection::~TcpConnection(){
    assert(state_ == kDisconnected);
    close(fd_); //这里式不是太简陋了？？
}

void TcpConnection::handleRead(){
    int savedErrno = 0;
    ssize_t n = inputBuffer_.readFD(fd(), &savedErrno);
    //channel_->fd();也可以，但是fd更快一点
    if(n < 0)
    {
        handleError();
    }
    else if(n == 0) {
        handleClose(); //对方发送FIN报文，
    }
    else{
        if(messageCallback_)
        {
            messageCallback_(shared_from_this(), &inputBuffer_);
        }
    }
}

void TcpConnection::handleWrite(){
    // 连接已断开则不再写
    if(state_ != kConnected && state_ != kDisconnecting) return;


    if(channel_->isWriting() && outputBuffer_.readableBytes() > 0) //设置了写监控
    {
        const size_t oldQueuedBytes = outputBuffer_.readableBytes();
        ssize_t n = write(fd(), outputBuffer_.peek(), outputBuffer_.readableBytes());
        if(n > 0)
        {
            outputBuffer_.retrieve(n);
            checkLowWaterMark(oldQueuedBytes);
            if(outputBuffer_.readableBytes() == 0)
            {
                channel_->disableWriting();
                if(writeCompleteCallback_)
                {
                    writeCompleteCallback_(shared_from_this());
                }
                if(state_ == kDisconnecting)
                {
                    shutdownInLoop();
                }
            }
        }
        else if(errno == EAGAIN || errno == EWOULDBLOCK)
        {
            channel_->enableWriteing();
            return ;
        }
        else {
            handleError();
            return ;
        }
    }
    if(sendFileCtx_ && sendFileCtx_->fd >= 0 && outputBuffer_.readableBytes() == 0)
    {
        while(sendFileCtx_->remaining > 0)
        {
            ssize_t sent = ::sendfile(fd_, sendFileCtx_->fd, 
                                      &sendFileCtx_->offset,
                                       sendFileCtx_->remaining);
            if(sent > 0)
            {
                sendFileCtx_->remaining -= sent;
            }                           
            else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                channel_->enableWriteing();
                return ;
            }
            else {
                break;
            }
        }
        ::close(sendFileCtx_->fd);
        sendFileCtx_.reset();
        channel_->disableWriting();
        if(state_ == kDisconnecting)
        {
            shutdownInLoop();
        }
        return ;
    }
}

void TcpConnection::pauseRead()
{
    TcpConnectionPtr self(shared_from_this());
    
    loop_->runInLoop([self](){
        self->pauseReadInLoop();
    });
}

void TcpConnection::pauseReadInLoop()
{
    if(state_ != kConnected || readPaused_)
    {
        return;
    }

    readPaused_ = true;
    channel_->disableReading();
}

void TcpConnection::resumeRead() {
    TcpConnectionPtr self(shared_from_this());
    
    loop_->runInLoop([self](){
        self->resumeReadInLoop();
    });
}

void TcpConnection::resumeReadInLoop()
{
    if(state_ != kConnected || readPaused_)
    {
        return;
    }

    readPaused_ = false;
    channel_->enableReading();

    //恢复后，要再次触发一次这个http，把buffer部分的数据进行读取
    if(inputBuffer_.readableBytes() > 0 && messageCallback_)
    {
        TcpConnectionPtr self(shared_from_this());
        loop_->queueInLoop([self](){
            if(self->state_ == kConnected && !self->readPaused_ && 
                self->inputBuffer_.readableBytes() > 0 && self->messageCallback_)
            {
                self->messageCallback_(self, &self->inputBuffer_);
            }
        });
    }
}

void TcpConnection::shutdown() {
    TcpConnectionPtr self(shared_from_this());
    loop_->runInLoop([self](){
        if(self->state_ != kConnected)
        {
            return;
        }
        self->setState(kDisconnecting);
        if(!self->channel_->isWriting())
        {
            self->shutdownInLoop();
        }
    });
}

void TcpConnection::shutdownInLoop() //半关闭，发送一个FIN
{
    if(!channel_->isWriting())
    {
        ::shutdown(fd_, SHUT_WR);
    }
}

//主动完全发送数据
void TcpConnection::send(const std::string &buf)
{ 
    //拷贝
    if(buf.empty())
    {
        return;
    }
    TcpConnectionPtr self(shared_from_this());
    loop_->runInLoop([self, buf](){
        self->sendInLoop(buf);
    });
}

void TcpConnection::send(const char* data, size_t size)
{ 
    //拷贝
    if(data == nullptr || size == 0)
    {
        return ;
    }
    send(std::string(data, size));
}

void TcpConnection::sendInLoop(const std::string& buf)
{
    if(state_ != kConnected)
    {
        return ;
    }
    ssize_t nwrote = 0;
    size_t remaining = buf.size();

    if(!channel_->isWriting() && outputBuffer_.readableBytes() == 0)
    {
        //这里没有监听epoll 写事件，并且buffer没有要发送的数据
        nwrote = write(fd(), buf.data(), buf.size());
        if(nwrote >= 0) //写入成功的字节数
        {
            remaining -= static_cast<size_t>(nwrote);
        }
        else if(errno == EAGAIN || errno == EWOULDBLOCK)
        {
            nwrote = 0;
        }
        else {
            handleError();
            return;
        }
    }
    if(remaining > 0)
    {
        const size_t oldQueuedBytes = outputBuffer_.readableBytes();
        outputBuffer_.append((buf.data()+nwrote), remaining);
        checkHighWaterMark(oldQueuedBytes);
        if(!channel_->isWriting())
        {
            channel_->enableWriteing();
        }
    }
}

void TcpConnection::checkHighWaterMark(size_t oldQueuedBytes)
{
    const size_t queuedBytes = outputBuffer_.readableBytes();
    if(highWaterMarkCallback_ && highWaterMark_ > 0 && oldQueuedBytes < highWaterMark_ && queuedBytes >= highWaterMark_)
    {
        highWaterMarkCallback_(shared_from_this(), queuedBytes);
    }
}

void TcpConnection::checkLowWaterMark(size_t oldQueuedBytes)
{
    const size_t queuedBytes = outputBuffer_.readableBytes();
    if(lowWaterMarkCallback_ && lowWaterMark_ > 0 && oldQueuedBytes >= lowWaterMark_ && queuedBytes < lowWaterMark_)
    {
        lowWaterMarkCallback_(shared_from_this(), queuedBytes);
    }
}

void TcpConnection::connectEstablished() {
    //改变状态，变成连接状态
    setState(kConnected);
    readPaused_ = false;
    channel_->tie(shared_from_this());
    channel_->enableReading();
    if(connectionCallback_) connectionCallback_(shared_from_this());
    //如果这个函数定义了，就调用
}

void TcpConnection::connectDestroyed() //整个TCP连接断开最后调用的函数
{
    if(state_ != kDisconnected)
    {
        setState(kDisconnected);
    }
    channel_->disableAll();
    if(connectionCallback_)
    {
        connectionCallback_(shared_from_this());
    }
    channel_->remove();
}

void TcpConnection::handleClose(){//对方断开连接的报警机制
    if(state_  == kDisconnected)
    {
        return;
    }
    setState(kDisconnected);
    channel_->disableAll();
    TcpConnectionPtr guardThis(shared_from_this());
    if(internalCloseCallback_)
    {
        internalCloseCallback_(guardThis);
    }
    //这样的guardThis的生命周期会到这个函数运行完成之后，才会结束，这样才会析构
}

void TcpConnection::handleError(){
    if(errorCallback_)
    {
        errorCallback_(shared_from_this());
    }
    handleClose();
}

void TcpConnection::startSendFile(const std::string &filePath, size_t fileSize)
{
    TcpConnectionPtr self(shared_from_this());
    loop_->runInLoop([self, filePath, fileSize](){
        self->startSendFileInLoop(filePath, fileSize);
    });
}

void TcpConnection::startSendFileInLoop(const std::string& filePath, size_t fileSize)
{
    if(state_ != kConnected)
    {
        return;
    }
    int fd = ::open(filePath.c_str(), O_RDONLY);
    if(fd < 0)
    {
        handleClose();
        return;
    }

    sendFileCtx_ = std::make_unique<SendFileCtx>();
    sendFileCtx_->fd = fd;
    sendFileCtx_->offset = 0;
    sendFileCtx_->remaining = fileSize;

    // 先注册 EPOLLOUT，再触发第一次发送
    channel_->enableWriteing();
    handleWrite();
}

}
}
















