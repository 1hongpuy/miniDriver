#pragma once

#include "http/HttpClientTypes.hpp"



#include <cstddef>
#include <cstdint>
#include <functional>
#include <linux/limits.h>
#include <map>
#include <memory>
#include <string>
#include <sys/types.h>
#include <vector>



namespace miniKV {
namespace network {

class EventLoop;
class TcpClient;
class TcpConnection;
class Buffer;

using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
}
}

namespace miniKV{
namespace http {

struct AsyncHttpRequestOptions{
    std::string address;
    uint16_t    port;
    std::string method;    //http方法
    std::string path;
    std::map<std::string, std::string> headers; //http 头部键值对,就是头部相应有什么东西
    uint64_t contentLength = 0;
    int timeoutMs = 10000;
    size_t maxPreconnectBodyBytes = 256 * 1024;
    size_t maxResponseBytes = 1024 * 1024;
    size_t highWaterMark = 512 * 1024;
    size_t lowWaterMark = 256 * 1024;
};

enum class AsyncWriteResult {
    kAccepted,
    kWouldBlock,
    kClosed
}; //单次转发得状态

class AsyncHttpRequest : public std::enable_shared_from_this<AsyncHttpRequest> {
public:
    using Ptr = std::shared_ptr<AsyncHttpRequest>;
    using ReadyCallback = std::function<void()>;
    using ResponseCallback = std::function<void(HttpClientResponse response,
                                                std::string error)>;
    using QueueWaterMarkCallback = std::function<void(size_t queuedBytes)>;

    static Ptr create(network::EventLoop* loop);
    ~AsyncHttpRequest();

    AsyncHttpRequest(const AsyncHttpRequest&) = delete;
    //禁止使用这个拷贝构造函数
    AsyncHttpRequest& operator=(const AsyncHttpRequest&) = delete; 

    void open(AsyncHttpRequestOptions options,
              ReadyCallback ready,
              ResponseCallback response);
    
    AsyncWriteResult write(const char* data, size_t size);
    void finishBody();
    void cancel();

    bool ready() const { return ready_; }
    bool finished() const { return finished_; }
    uint64_t acceptedBodyBytes() const {
        return acceptedBodyBytes_;
    }
    uint64_t expertedBodyBytes() const {
        return options_.contentLength;
    }

    void setHighWaterMarkCallback(QueueWaterMarkCallback callback)
    {
        highWaterMarkCallback_ = std::move(callback);
    }
    void setLowWaterMarkCallback(QueueWaterMarkCallback callback)
    {
        lowWaterMarkCallback_ = std::move(callback);
    }
private:
    enum class State {
        kIdle,
        kConnecting,
        kWritingBody,
        kWaitingResponse,
        kFinished
    };

    explicit AsyncHttpRequest(network::EventLoop* loop);

    void openInLoop(AsyncHttpRequestOptions options,
                    ReadyCallback ready,
                    ResponseCallback response);
    AsyncWriteResult writeInLoop(const char* data, size_t size);
    void finishBodyInLoop();
    void cancelInLoop();

    void onConnection(const network::TcpConnectionPtr& connection);
    void onMessage(const network::TcpConnectionPtr& connection, network::Buffer* buffer);
    void sendHeadersInLoop();
    void flushPreconnectBodyInLoop();
    void parseResponseInLoop();
    void onHighWaterMark(size_t queuedBytes);
    void onLowWaterMark(size_t queuedBytes);
    void completeInLoop(HttpClientResponse response, std::string error);
    void startResponseTimeoutInLoop();
    void cancelTimeoutInLoop();
    void holdLifetimeInLoop();
    void releaseLifetimeInLoop();


    network::EventLoop* loop_;
    std::shared_ptr<network::TcpClient> client_;
    network::TcpConnectionPtr connection_;
    AsyncHttpRequestOptions options_;
    ReadyCallback readyCallback_;
    ResponseCallback responseCallback_;
    QueueWaterMarkCallback highWaterMarkCallback_;
    QueueWaterMarkCallback lowWaterMarkCallback_;
    std::vector<std::string> preconnectBody_;  //缓冲区 临时保存每次要转发得数据，就是此时我们的tcp连接备份机器还没有成功
    size_t preconnectBodyBytes_ = 0;    //累计存储缓冲区的数据计数
    uint64_t acceptedBodyBytes_ = 0;    //接收body累计数据计数

    //解析
    std::string responseBytes_; //接收到的这个响应
    HttpClientResponse parsedResponse_;
    size_t responseBodyOffset_ = 0;    //body起始位置
    uint64_t expectedResponseBodyBytes_ = 0;
    bool responseHeadersParsed_ = false;    //头解析完毕
    bool responseHasContentLength_ = false; //响应里带不带content-Length
    int  timeoutTimerId_ = 0; //定时器ID
    bool finishRequested_ = false; //装货完毕，锁上门
    bool ready_ = false; //三次握手结束
    bool finished_ = false; //卡车安全到达，任务彻底结束
    State state_ = State::kIdle;
    Ptr lifetimeGuard_;
};

}

}






























