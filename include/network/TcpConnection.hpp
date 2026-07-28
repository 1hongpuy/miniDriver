#pragma once


#include "Buffer.hpp"
#include "channel.hpp"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <functional> //回调
#include <sys/types.h>
#include <type_traits>


namespace miniKV{
namespace network {

//前向声明
//该类面向与业务，实际的对接网络操作还是使用channel
class EventLoop;
class TcpConnection;

using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
using ConnectionCallback = std::function<void(const TcpConnectionPtr&)>;
using MessageCallback = std::function<void(const TcpConnectionPtr&, Buffer*)>;
using CloseCallback = std::function<void(const TcpConnectionPtr&)>;
using WriteCompleteCallback = std::function<void(const TcpConnectionPtr&)>;
using ErrorCallback = std::function<void(const TcpConnectionPtr&)>;
using WaterMarkCallback = std::function<void(const TcpConnectionPtr&, size_t)>;



class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    TcpConnection(EventLoop* loop, int fd, int id);
    ~TcpConnection();

    void send(const std::string& buf);
    void send(const char* data, size_t size);
    void shutdown();
    bool connected() const{return state_ == kConnected;}
    int fd() const {return fd_;}


    void pauseRead();
    void resumeRead();
    bool readPaused() const {
        return readPaused_;
    }
    size_t queuedBytes() const {
        return outputBuffer_.readableBytes();
    }

    void setConnectionCallback(ConnectionCallback cb){
        connectionCallback_ = std::move(cb);
    }
    void setMessageCallback(MessageCallback cb)
    {
        messageCallback_ = std::move(cb);
    }
    void setInternalCloseCallback(CloseCallback cb)
    {
        internalCloseCallback_ = std::move(cb);
    }
    void setWriteCompleteCallback(WriteCompleteCallback cb)
    {
        writeCompleteCallback_ = std::move(cb);
    }
    void setErrorCallback(ErrorCallback cb)
    {
        errorCallback_ = std::move(cb);
    }
    void setHighWaterMark(size_t bytes, WaterMarkCallback cb)
    {
        highWaterMark_ = bytes;
        highWaterMarkCallback_ = std::move(cb);
    }
    void setLowWaterMark(size_t bytes, WaterMarkCallback cb)
    {
        lowWaterMark_ = bytes;
        lowWaterMarkCallback_ = std::move(cb);
    }

    void connectEstablished();
    void connectDestroyed();
    void startSendFile(const std::string& filePath, size_t fileSize);

private:

    void handleRead();
    void handleWrite();
    void handleClose();
    void handleError();
    void sendInLoop(const std::string& buf);//？
    void startSendFileInLoop(const std::string& filePath, size_t fileSize);
    void shutdownInLoop();
    void pauseReadInLoop();
    void resumeReadInLoop();
    void checkHighWaterMark(size_t oldQueuedBytes);
    void checkLowWaterMark(size_t oldQueuedBytes);

    enum StateE {
        kConnectiong, 
        kConnected,
        kDisconnecting,
        kDisconnected
    };

    struct SendFileCtx {
        int fd = -1;
        off_t  offset = 0;
        size_t remaining = 0;
    };
    std::unique_ptr<SendFileCtx> sendFileCtx_;

    void setState(StateE s) {state_ = s;}
    EventLoop* loop_;
    int fd_;
    int id_;
    StateE state_;

    std::unique_ptr<Channel> channel_;

    Buffer inputBuffer_;
    Buffer outputBuffer_;
    bool readPaused_ = false;
    size_t highWaterMark_ = 512 * 1024;
    size_t lowWaterMark_ = 256 * 1024;

    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;
    CloseCallback internalCloseCallback_;
    WriteCompleteCallback writeCompleteCallback_;
    ErrorCallback errorCallback_;
    WaterMarkCallback highWaterMarkCallback_;
    WaterMarkCallback lowWaterMarkCallback_;
};


}

}



























