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

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    TcpConnection(EventLoop* loop, int fd, int id);
    ~TcpConnection();

    void send(const std::string& buf);
    void shutdown();
    void shutdownInLoop();
    bool connected() const{return state_ == kConnected;}
    int fd() const {return fd_;}

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

    void connectEstablished();
    void connectDestroyed();
    void startSendFile(const std::string& filePath, size_t fileSize);

private:

    void handleRead();
    void handleWrite();
    void handleClose();
    void handleError();

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

    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;
    CloseCallback internalCloseCallback_;
};


}

}



























