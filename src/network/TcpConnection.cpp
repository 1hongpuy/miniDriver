#include "network/TcpConnection.hpp"
#include "network/EventLoop.hpp"
#include "network/channel.hpp"
#include <asm-generic/errno-base.h>
#include <asm-generic/errno.h>
#include <cassert>
#include <cerrno>
#include <csignal>
#include <cstddef>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>



namespace miniKV {
namespace network {

namespace {

void ignoreSigPipe()
{
    static std::once_flag once;
    std::call_once(once, [] {
        struct sigaction action {};
        action.sa_handler = SIG_IGN;
        ::sigemptyset(&action.sa_mask);
        ::sigaction(SIGPIPE, &action, nullptr);
    });
}

}  // namespace

TcpConnection::TcpConnection(EventLoop* loop, int fd, int id,
                             std::shared_ptr<OutputBufferStats> processOutputStats)
: loop_(loop), fd_(fd), id_(id), state_(kConnectiong), channel_(new Channel(loop, fd)),
  processOutputStats_(std::move(processOutputStats))
{
    ignoreSigPipe();
    channel_->setReadCallback([this](){handleRead();});
    channel_->setCloseCallback([this](){handleClose();});
    channel_->setErrorCallback([this](){handleError();});
    channel_->setWriteCallback([this](){handleWrite();});
}

TcpConnection::~TcpConnection(){
    assert(state_ == kDisconnected);
    resetOutputBytes();
    close(fd_); //这里式不是太简陋了？？
}

void TcpConnection::setContext(std::any context)
{
    assert(loop_->isInLoopThread());
    context_ = std::move(context);
}

const std::any& TcpConnection::context() const
{
    assert(loop_->isInLoopThread());
    return context_;
}

void TcpConnection::clearContext()
{
    assert(loop_->isInLoopThread());
    context_.reset();
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
        ssize_t n = ::send(fd(), outputBuffer_.peek(), outputBuffer_.readableBytes(),
                           MSG_NOSIGNAL);
        if(n > 0)
        {
            outputBuffer_.retrieve(n);
            removeOutputBytes(static_cast<size_t>(n));
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
        const auto attemptStartedAt = std::chrono::steady_clock::now();
        if(!sendFileCtx_->firstAttemptRecorded) {
            sendFileCtx_->firstAttemptRecorded = true;
            sendFileCtx_->firstAttemptNanoseconds = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    attemptStartedAt - sendFileCtx_->startedAt).count());
        }
        if(sendFileCtx_->blocked) {
            sendFileCtx_->wouldBlockNanoseconds += static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    attemptStartedAt - sendFileCtx_->blockedAt).count());
            sendFileCtx_->blocked = false;
        }
        const size_t requestedBytes = std::min(sendFileCtx_->remaining, sendFileQuantum_);
        const ssize_t sent = ::sendfile(fd_, sendFileCtx_->fd,
                                        &sendFileCtx_->offset,
                                        requestedBytes);
        sendFileCtx_->syscallNanoseconds += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - attemptStartedAt).count());
        if(sent > 0)
        {
            const size_t sentBytes = static_cast<size_t>(sent);
            sendFileCtx_->remaining -= sentBytes;
            ++sendFileCtx_->writeCalls;
            sendFileCtx_->maxBytesPerCall = std::max(sendFileCtx_->maxBytesPerCall,
                                                      sentBytes);
            if(sendFileCtx_->remaining > 0) {
                channel_->enableWriteing();
                return;
            }
            channel_->disableWriting();
            finishSendFile(true);
            if(state_ == kDisconnecting)
            {
                shutdownInLoop();
            }
            return;
        }
        else if(sent < 0 && errno == EINTR) {
            channel_->enableWriteing();
            return;
        }
        else if(sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            ++sendFileCtx_->wouldBlockCount;
            sendFileCtx_->blockedAt = std::chrono::steady_clock::now();
            sendFileCtx_->blocked = true;
            channel_->enableWriteing();
            return;
        }
        else {
            handleError();
            return;
        }
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
    // resumeRead() is only meaningful after pauseReadInLoop() disabled EPOLLIN.
    // The previous condition returned precisely when the connection was paused,
    // leaving an upload permanently back-pressured after its replica connected.
    if(state_ != kConnected || !readPaused_)
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
        nwrote = ::send(fd(), buf.data(), buf.size(), MSG_NOSIGNAL);
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
        addOutputBytes(remaining);
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
    if(highWaterMark_ > 0 && oldQueuedBytes < highWaterMark_ && queuedBytes >= highWaterMark_) {
        outputHighWaterEvents_.fetch_add(1, std::memory_order_relaxed);
        if(processOutputStats_) processOutputStats_->highWaterEvents.fetch_add(1, std::memory_order_relaxed);
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
    finishSendFile(false);
    if(connectionCallback_)
    {
        connectionCallback_(shared_from_this());
    }
    clearContext();
    resetOutputBytes();
    channel_->remove();
}

void TcpConnection::handleClose(){//对方断开连接的报警机制
    if(state_  == kDisconnected)
    {
        return;
    }
    setState(kDisconnected);
    channel_->disableAll();
    resetOutputBytes();
    finishSendFile(false);
    TcpConnectionPtr guardThis(shared_from_this());
    if(internalCloseCallback_)
    {
        internalCloseCallback_(guardThis);
    }
    //这样的guardThis的生命周期会到这个函数运行完成之后，才会结束，这样才会析构
}

TcpConnection::OutputMetrics TcpConnection::outputMetrics() const noexcept
{
    return {outputCurrentBytes_.load(std::memory_order_relaxed),
            outputPeakBytes_.load(std::memory_order_relaxed),
            outputHighWaterEvents_.load(std::memory_order_relaxed)};
}

void TcpConnection::addOutputBytes(size_t bytes)
{
    const uint64_t current = outputCurrentBytes_.fetch_add(bytes, std::memory_order_relaxed) + bytes;
    uint64_t peak = outputPeakBytes_.load(std::memory_order_relaxed);
    while(peak < current && !outputPeakBytes_.compare_exchange_weak(peak, current, std::memory_order_relaxed)) {}
    if(!processOutputStats_) return;
    const uint64_t processCurrent = processOutputStats_->currentBytes.fetch_add(bytes, std::memory_order_relaxed) + bytes;
    peak = processOutputStats_->peakBytes.load(std::memory_order_relaxed);
    while(peak < processCurrent && !processOutputStats_->peakBytes.compare_exchange_weak(peak, processCurrent, std::memory_order_relaxed)) {}
}

void TcpConnection::removeOutputBytes(size_t bytes)
{
    outputCurrentBytes_.fetch_sub(bytes, std::memory_order_relaxed);
    if(processOutputStats_) processOutputStats_->currentBytes.fetch_sub(bytes, std::memory_order_relaxed);
}

void TcpConnection::resetOutputBytes()
{
    const uint64_t bytes = outputCurrentBytes_.exchange(0, std::memory_order_relaxed);
    if(bytes > 0 && processOutputStats_) processOutputStats_->currentBytes.fetch_sub(bytes, std::memory_order_relaxed);
}

void TcpConnection::handleError(){
    if(errorCallback_)
    {
        errorCallback_(shared_from_this());
    }
    handleClose();
}

void TcpConnection::startSendFile(const std::string &filePath, size_t fileSize,
                                  SendFileCompleteCallback callback)
{
    startSendFile(filePath, 0, fileSize, std::move(callback));
}

void TcpConnection::startSendFile(const std::string& filePath, off_t offset, size_t fileSize,
                                  SendFileCompleteCallback callback)
{
    TcpConnectionPtr self(shared_from_this());
    loop_->runInLoop([self, filePath, offset, fileSize, callback = std::move(callback)]() mutable {
        self->startSendFileInLoop(filePath, offset, fileSize, std::move(callback));
    });
}

void TcpConnection::startSendFileInLoop(const std::string& filePath, off_t offset,
                                        size_t fileSize,
                                        SendFileCompleteCallback callback)
{
    if(state_ != kConnected)
    {
        if(callback) callback({false, 0, 0, 0});
        return;
    }
    if(sendFileCtx_) {
        if(callback) callback({false, 0, 0, 0});
        return;
    }
    int fd = ::open(filePath.c_str(), O_RDONLY);
    if(fd < 0)
    {
        if(callback) callback({false, 0, 0, 0});
        handleClose();
        return;
    }

    sendFileCtx_ = std::make_unique<SendFileCtx>();
    sendFileCtx_->fd = fd;
    sendFileCtx_->offset = offset;
    sendFileCtx_->remaining = fileSize;
    sendFileCtx_->totalBytes = fileSize;
    sendFileCtx_->startedAt = std::chrono::steady_clock::now();
    sendFileCtx_->callback = std::move(callback);

    if(fileSize == 0) {
        finishSendFile(true);
        return;
    }

    // 先注册 EPOLLOUT，再触发第一次发送
    channel_->enableWriteing();
    handleWrite();
}

void TcpConnection::finishSendFile(bool success)
{
    if(!sendFileCtx_) return;
    std::unique_ptr<SendFileCtx> context = std::move(sendFileCtx_);
    if(context->fd >= 0) ::close(context->fd);

    SendFileResult result;
    result.success = success && context->remaining == 0;
    result.bytesSent = context->totalBytes - context->remaining;
    result.writeCalls = context->writeCalls;
    result.maxBytesPerCall = context->maxBytesPerCall;
    result.syscallNanoseconds = context->syscallNanoseconds;
    result.wouldBlockCount = context->wouldBlockCount;
    result.wouldBlockNanoseconds = context->wouldBlockNanoseconds;
    result.firstAttemptNanoseconds = context->firstAttemptNanoseconds;
    auto callback = std::move(context->callback);
    if(callback) callback(result);
}

}
}








