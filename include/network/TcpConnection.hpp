#pragma once


#include "Buffer.hpp"
#include "channel.hpp"
#include <algorithm>
#include <any>
#include <cstddef>
#include <memory>
#include <string>
#include <functional> //回调
#include <atomic>
#include <chrono>
#include <sys/types.h>
#include <type_traits>
#include <vector>


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

struct SendFileResult {
    bool success = false;
    size_t bytesSent = 0;           //发送字节
    size_t writeCalls = 0;          //调用次数
    size_t maxBytesPerCall = 0;     //单次系统发送的最大字节数
    uint64_t syscallNanoseconds = 0;
    uint64_t wouldBlockCount = 0;
    uint64_t wouldBlockNanoseconds = 0;
    uint64_t firstAttemptNanoseconds = 0;
};

struct SendFileSegment {
    std::string path;
    off_t offset = 0;
    size_t size = 0;
};

struct OutputBufferStats {
    std::atomic<uint64_t> currentBytes{0};
    std::atomic<uint64_t> peakBytes{0};
    std::atomic<uint64_t> highWaterEvents{0};
};

using SendFileCompleteCallback = std::function<void(const SendFileResult&)>;



class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    TcpConnection(EventLoop* loop, int fd, int id,
                  std::shared_ptr<OutputBufferStats> processOutputStats = {});
    ~TcpConnection();

    void send(const std::string& buf);
    void send(const char* data, size_t size);
    void shutdown();
    bool connected() const{return state_ == kConnected;}
    int fd() const {return fd_;}
    EventLoop* ownerLoop() const noexcept { return loop_; }
    void setContext(std::any context);
    const std::any& context() const;
    void clearContext();


    void pauseRead();
    void resumeRead();
    bool readPaused() const {
        return readPaused_;
    }
    size_t queuedBytes() const {
        return outputBuffer_.readableBytes();
    }
    struct OutputMetrics { uint64_t currentBytes = 0; uint64_t peakBytes = 0; uint64_t highWaterEvents = 0; };
    OutputMetrics outputMetrics() const noexcept;

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
    void setSendFileQuantum(size_t bytes) { sendFileQuantum_ = std::max<size_t>(1, bytes); }
    void startSendFile(const std::string& filePath, size_t fileSize,
                       SendFileCompleteCallback callback = {});
    void startSendFile(const std::string& filePath, off_t offset, size_t fileSize,
                       SendFileCompleteCallback callback = {});
    void startSendFileSequence(std::vector<SendFileSegment> segments,
                               SendFileCompleteCallback callback = {});

private:

    void handleRead();
    void handleWrite();
    void handleClose();
    void handleError();
    void sendInLoop(const std::string& buf);//？
    void startSendFileInLoop(const std::string& filePath, off_t offset, size_t fileSize,
                             SendFileCompleteCallback callback);
    void startSendFileSequenceInLoop(std::vector<SendFileSegment> segments,
                                     SendFileCompleteCallback callback);
    bool openNextSendFileSegment();
    void finishSendFile(bool success);
    void shutdownInLoop();
    void pauseReadInLoop();
    void resumeReadInLoop();
    void checkHighWaterMark(size_t oldQueuedBytes);
    void checkLowWaterMark(size_t oldQueuedBytes);
    void addOutputBytes(size_t bytes);
    void removeOutputBytes(size_t bytes);
    void resetOutputBytes();

    enum StateE {
        kConnectiong, 
        kConnected,
        kDisconnecting,
        kDisconnected
    };

    struct SendFileCtx {
        int fd = -1;                        // 文件描述符
        off_t  offset = 0;                  // 读盘的物理偏移量
        size_t remaining = 0;               // 剩下多少字节没发完
        size_t totalRemaining = 0;          // sequence 中尚未发送的总字节
        size_t totalBytes = 0;              // 总共要发送多少字节
        size_t writeCalls = 0;              // 调用了多少次sendfile
        size_t maxBytesPerCall = 0;         // 单次调用sendfile发送的最大字节数
        uint64_t syscallNanoseconds = 0;
        uint64_t wouldBlockCount = 0;
        uint64_t wouldBlockNanoseconds = 0;
        uint64_t firstAttemptNanoseconds = 0;
        std::chrono::steady_clock::time_point startedAt;
        std::chrono::steady_clock::time_point blockedAt;
        bool firstAttemptRecorded = false;
        bool blocked = false;
        SendFileCompleteCallback callback;  // 发完之后通知谁（业务回调函数）
        std::vector<SendFileSegment> segments;
        size_t segmentIndex = 0;
    };
    std::unique_ptr<SendFileCtx> sendFileCtx_;
    size_t sendFileQuantum_ = 256 * 1024;

    void setState(StateE s) {state_ = s;}
    EventLoop* loop_;
    int fd_;
    int id_;
    StateE state_;

    std::unique_ptr<Channel> channel_;

    Buffer inputBuffer_;
    Buffer outputBuffer_;
    std::shared_ptr<OutputBufferStats> processOutputStats_;
    std::atomic<uint64_t> outputCurrentBytes_{0};
    std::atomic<uint64_t> outputPeakBytes_{0};
    std::atomic<uint64_t> outputHighWaterEvents_{0};
    std::any context_;
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



















