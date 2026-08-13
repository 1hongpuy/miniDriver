#pragma once

#include "http/AsyncHttpClient.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace miniKV::http {

// A single HTTP/1.1 keep-alive connection.  It deliberately supports one
// request at a time: the caller must wait for ResponseCallback before starting
// the next request.  This keeps response framing and ownership simple.
class PersistentHttpSession : public std::enable_shared_from_this<PersistentHttpSession> {
public:
    using Ptr = std::shared_ptr<PersistentHttpSession>;
    using ReadyCallback = std::function<void()>;
    using ResponseCallback = std::function<void(HttpClientResponse, std::string)>;
    using QueueWaterMarkCallback = std::function<void(size_t)>;

    static Ptr create(network::EventLoop* loop);
    ~PersistentHttpSession();

    PersistentHttpSession(const PersistentHttpSession&) = delete;
    PersistentHttpSession& operator=(const PersistentHttpSession&) = delete;

    // May be called only when idle.  The callback runs on ownerLoop().
    bool start(AsyncHttpRequestOptions options, ReadyCallback ready,
               ResponseCallback response);
    AsyncWriteResult write(const char* data, size_t size);
    void finishBody();
    void cancel();
    void close();

    bool idle() const { return state_ == State::kIdle; }
    bool usable() const { return state_ != State::kClosed; }
    network::EventLoop* ownerLoop() const { return loop_; }
    void setHighWaterMarkCallback(QueueWaterMarkCallback callback) {
        highWaterMarkCallback_ = std::move(callback);
    }
    void setLowWaterMarkCallback(QueueWaterMarkCallback callback) {
        lowWaterMarkCallback_ = std::move(callback);
    }

private:
    enum class State { kIdle, kConnecting, kWritingBody, kWaitingResponse, kClosed };
    explicit PersistentHttpSession(network::EventLoop* loop);

    bool startInLoop(AsyncHttpRequestOptions options, ReadyCallback ready,
                     ResponseCallback response);
    AsyncWriteResult writeInLoop(const char* data, size_t size);
    void finishBodyInLoop();
    void cancelInLoop();
    void closeInLoop();
    void connectInLoop();
    void onConnection(const network::TcpConnectionPtr& connection);
    void onMessage(const network::TcpConnectionPtr& connection, network::Buffer* buffer);
    void sendHeadersInLoop();
    void flushPreconnectBodyInLoop();
    void parseResponseInLoop();
    void startResponseTimeoutInLoop();
    void cancelResponseTimeoutInLoop();
    void completeRequestInLoop(HttpClientResponse response, std::string error,
                               bool keepConnection);
    void clearRequestStateInLoop();
    void failAndCloseInLoop(std::string error);

    network::EventLoop* loop_;
    std::shared_ptr<network::TcpClient> client_;
    network::TcpConnectionPtr connection_;
    AsyncHttpRequestOptions options_;
    ReadyCallback readyCallback_;
    ResponseCallback responseCallback_;
    QueueWaterMarkCallback highWaterMarkCallback_;
    QueueWaterMarkCallback lowWaterMarkCallback_;
    std::vector<std::string> preconnectBody_;
    size_t preconnectBodyBytes_ = 0;
    uint64_t acceptedBodyBytes_ = 0;
    bool finishRequested_ = false;
    bool ready_ = false;
    int timeoutTimerId_ = 0;

    std::string responseBytes_;
    HttpClientResponse parsedResponse_;
    size_t responseBodyOffset_ = 0;
    uint64_t expectedResponseBodyBytes_ = 0;
    bool responseHeadersParsed_ = false;
    bool responseConnectionClose_ = false;
    State state_ = State::kIdle;
};

}  // namespace miniKV::http
