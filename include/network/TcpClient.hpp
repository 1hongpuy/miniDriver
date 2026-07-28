#pragma once



#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>




namespace miniKV {
namespace network {

class EventLoop;
class Channel;
class TcpConnection;
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
using MessageCallback = std::function<void(const TcpConnectionPtr&, class Buffer*)>;


class TcpClient : public std::enable_shared_from_this<TcpClient>{
public:
    using ConnectionCallback = std::function<void(const TcpConnectionPtr&)>;
    using StopCallback = std::function<void()>;
    using Ptr = std::shared_ptr<TcpClient>;

    static Ptr create(EventLoop* loop);
 
    ~TcpClient();

    void connect(const std::string& addr, int port); //应用层调用主动连接使用
    void disconnect();
    void stop(StopCallback callback = StopCallback());
    void setConnectTimeout(int64_t timeoutMs)
    {
        connectTimeoutMs_ = timeoutMs;
    }

    void setConnectionCallback(ConnectionCallback cb) {
        connectionCallback_ = std::move(cb);
    }

    void setMessageCallback (MessageCallback cb) {
        messageCallback_ = std::move(cb);
    }

    TcpConnectionPtr connection() const { return connection_; }

private:
    TcpClient(EventLoop* loop);
    enum StateE{
        kDisconnected,
        kConnecting,
        kConnected,
        kClosing
    };

    void onNewConnection(int sockfd);  //创建tcp连接，调用成功后使用
    void handleWrite();
    void handleConnectTimeout();
    void failConnect();
    void connectInLoop(std::string addr, int port);
    void disconnectInLoop();
    void removeConnectingChannel(bool closeSocket, StopCallback callback = StopCallback());
    void removeConnection(const TcpConnectionPtr& conn);
    void cancelConnectTimer();
    void stopInLoop(StopCallback callback);
    void finishStopInLoop(StopCallback callback);
    void holdLifetimeInLoop();
    void releaseLifetimeInLoop();

    EventLoop* loop_;
    std::unique_ptr<Channel> channel_;
    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;
    TcpConnectionPtr connection_;
    int nectConnId_ = 1; //每个tcpclient的唯一标识
    int connectTimerId_ = 0;
    int64_t connectTimeoutMs_ = 10000;
    StateE state_ = kDisconnected;
    Ptr lifetimeGuard_;
};







}
}















