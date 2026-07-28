#include "http/AsyncHttpClient.hpp"

#include "http/HttpClientTypes.hpp"
#include "network/Buffer.hpp"
#include "network/EventLoop.hpp"
#include "network/TcpClient.hpp"
#include "network/TcpConnection.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <utility>


namespace miniKV {
namespace http   {

//工具类函数
std::string lower(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c){
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

std::string trim(std::string value)
{
    const auto first = value.find_first_not_of(" \t\r\n");
    //它查找第一个“不属于” 指定字符集合的位置。
    if(first == std::string::npos) return ""; //全是空格
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

bool isManagedHeader(const std::string& name)
{
    const std::string key = lower(name);
    return key == "host" || key == "connection" || key == "content-length";
}

bool parseUnsigned(const std::string& value, uint64_t& out)
{
    //将字符串解析为无符号长整数（uint64_t）
    try{
        size_t consumed = 0;
        out = std::stoull(trim(value), &consumed);
        return consumed == trim(value).size();
    } catch(...)
    {
        return false;
    }
}

//
AsyncHttpRequest::Ptr AsyncHttpRequest::create(network::EventLoop *loop)
{
    return Ptr(new AsyncHttpRequest(loop));
}

AsyncHttpRequest::AsyncHttpRequest(network::EventLoop* loop) : loop_(loop) {}

AsyncHttpRequest::~AsyncHttpRequest()
{
    assert(finished_ || state_ == State::kIdle);
}

void AsyncHttpRequest::open(AsyncHttpRequestOptions options, ReadyCallback ready, ResponseCallback response)
{
    Ptr self(shared_from_this());
    loop_->runInLoop([self, options = std::move(options), ready = std::move(ready), 
                      response = std::move(response)]() mutable {
        self->openInLoop(std::move(options), std::move(ready), std::move(response));
    });
}

void AsyncHttpRequest::openInLoop(AsyncHttpRequestOptions options, 
                                  ReadyCallback ready, 
                                  ResponseCallback response) {
    if(state_ != State::kIdle)
    {
        return ;
    }
    if(options.address.empty() || options.port == 0 || options.method.empty() || options.path.empty())
    {
        HttpClientResponse empty;
        if(response)
        {
            //参数报错
            response(std::move(empty), "invalid async HTTP request options");
        }
        return ;
    }

    options_ = std::move(options);
    readyCallback_ = std::move(ready);
    responseCallback_ = std::move(response);
    state_ = State::kConnecting;
    holdLifetimeInLoop();

    client_ = network::TcpClient::create(loop_);
    client_->setConnectTimeout(options_.timeoutMs);
    std::weak_ptr<AsyncHttpRequest> weakSelf(shared_from_this());
    client_->setConnectionCallback([weakSelf](const network::TcpConnectionPtr& connection){
        if(Ptr self = weakSelf.lock()) self->onConnection(connection);
    });

    client_->setMessageCallback([weakSelf](const network::TcpConnectionPtr& connection,
                                network::Buffer* buffer){
        if(Ptr self = weakSelf.lock()) self->onMessage(connection, buffer);
    });

    if(options_.timeoutMs > 0)
    {
        timeoutTimerId_ = loop_->runAfter(options_.timeoutMs, [weakSelf] {
            if(Ptr self = weakSelf.lock())
            {
                HttpClientResponse empty;
                self->completeInLoop(std::move(empty), "async HTTP request timed out");
            }
        });
    }
    client_->connect(options_.address, options_.port);
}

AsyncWriteResult AsyncHttpRequest::write(const char *data, size_t size)
{
    //必须马上调用，这样才能告诉调用者结果，同步
    if(!loop_->isInLoopThread() )
    {
        return AsyncWriteResult::kWouldBlock;
    }
    return writeInLoop(data, size);
}

AsyncWriteResult AsyncHttpRequest::writeInLoop(const char* data, size_t size)
{
    if(finished_ || state_ == State::kIdle ||  state_ == State::kWaitingResponse || data == nullptr)
    {
        return AsyncWriteResult::kClosed;
    }
    if(size == 0)
    {
        return AsyncWriteResult::kAccepted;
    }

    if(acceptedBodyBytes_ + size > options_.contentLength)
    {
        HttpClientResponse empty;
        completeInLoop(empty, "request body exceeds Content-Length");
        return AsyncWriteResult::kClosed;
    }

    if(!ready_)
    {
        //tcp还没有连接得时候, 缓存数据
        if(preconnectBodyBytes_ + size > options_.maxPreconnectBodyBytes)
        {
            return AsyncWriteResult::kWouldBlock;
        }
        preconnectBody_.emplace_back(data, size); //直接构造放入
        preconnectBodyBytes_ += size;
        acceptedBodyBytes_ += size;
        return AsyncWriteResult::kAccepted;
    }
    connection_->send(data, size);
    acceptedBodyBytes_ += size;
    return AsyncWriteResult::kAccepted;
}   

void AsyncHttpRequest::finishBody()
{
    Ptr self(shared_from_this());
    loop_->runInLoop([self](){
        self->finishBodyInLoop();
    });
}

void AsyncHttpRequest::finishBodyInLoop()
{
    if(finished_ || finishRequested_)
    {
        //进入该函数得时候这两个都是false
        return ;
    }
    if(acceptedBodyBytes_ != options_.contentLength)
    {
        HttpClientResponse empty;
        completeInLoop(std::move(empty), "request body does not match Content-Length");
        return;
    }
    finishRequested_ = true;
    if(ready_) //一般只有短时间没有连接成功的时候，就是数据量很小
    {
        state_ = State::kWaitingResponse;
    }
}


void AsyncHttpRequest::cancel()
{
    Ptr self(shared_from_this());
    loop_->runInLoop([self](){
        self->cancelInLoop();});
}

void AsyncHttpRequest::cancelInLoop()
{
    if(finished_) return ;
    HttpClientResponse empty;
    completeInLoop(std::move(empty), "async HTTP request cancelled");
}

void AsyncHttpRequest::onConnection(const network::TcpConnectionPtr& connection)
{
    if(finished_) return;
    if(!connection)
    {
        HttpClientResponse empty;
        completeInLoop(std::move(empty), "TCP connection failed");
        return;
    }

    if(!connection->connected()) 
    {
        //连接被断开了
        if(responseHeadersParsed_ &&  !responseHasContentLength_)
        {
            //如果解析完这个Header，对方没有给这个length,再http1.1种，这表示我们关闭连接，表示数据发送完了
            parsedResponse_.body = responseBytes_.substr(responseBodyOffset_);
            completeInLoop(std::move(parsedResponse_), "");
            return ;
        }
        if(responseHeadersParsed_)
        {
            //断开补救一下，看看是不是还有数据再这个缓冲区
            parseResponseInLoop();
            if(finished_) return;
        }
        //异常提前断开
        HttpClientResponse empty;
        completeInLoop(std::move(empty), "TCP connection closed before a complete HTTP response");
        return ;
    }

    connection_ = connection;
    connection_->setHighWaterMark(options_.highWaterMark, [weakSelf = std::weak_ptr<AsyncHttpRequest>(shared_from_this())]
    (const network::TcpConnectionPtr&, size_t queuedBytes) {
        if(Ptr self = weakSelf.lock()) {self->onHighWaterMark(queuedBytes);}
    });
    connection_->setLowWaterMark(options_.lowWaterMark,
        [weakSelf = std::weak_ptr<AsyncHttpRequest>(shared_from_this())]
        (const network::TcpConnectionPtr&, size_t queuedBytes) {
            if(Ptr self = weakSelf.lock()) self->onLowWaterMark(queuedBytes);
        });
    //连接正常
    sendHeadersInLoop();
    ready_ = true;
    flushPreconnectBodyInLoop(); //倾斜缓冲区
    state_ = finishRequested_ ? State::kWaitingResponse : State::kWritingBody;
    if(readyCallback_) readyCallback_();
}

void AsyncHttpRequest::onMessage(const network::TcpConnectionPtr& connection,
    network::Buffer* buffer)
{
    //收到对方的响应
    if(finished_ || connection != connection_ || buffer == nullptr) return ;

    const size_t readable = buffer->readableBytes();
    if(responseBytes_.size() + readable > options_.maxResponseBytes)
    {
        HttpClientResponse empty;
        completeInLoop(std::move(empty), "HTTP response exceeds configured limit");
        return ;
    }
    responseBytes_.append(buffer->peek(), readable);
    buffer->retrieve(readable);
    parseResponseInLoop();
}


/*
HTTP/1.1 200 OK\r\n               <-- 起始行 (声明协议版本、状态码)
Content-Type: application/json\r\n <-- Header 1 (元数据)
Content-Length: 15\r\n             <-- Header 2 (告诉接收方 Body 有 15 字节)
Connection: close\r\n              <-- Header 3 (告诉接收方：发完我就关连接)
\r\n                               <-- 空行 (Double CRLF，Header 结束，Body 开始)
{"status":"ok"}                    <-- Body (真正的业务数据负载，共 15 字节)
*/
void AsyncHttpRequest::sendHeadersInLoop()
{
    std::ostringstream request;
    request << options_.method << " " << options_.path << " HTTP/1.1\r\n";
    request << "Host: " << options_.address << "\r\n";
    request << "Connection: close\r\n";
    for(const auto& [name, value] : options_.headers)
    {
        if(!isManagedHeader(name)) request << name << ": " << value << "\r\n";
    }
    request << "Content-Length: " << options_.contentLength << "\r\n\r\n";
    connection_->send(request.str());
}

void AsyncHttpRequest::flushPreconnectBodyInLoop()
{
    for(const std::string& block : preconnectBody_)
    {
        connection_->send(block);
    }
    preconnectBody_.clear();
    preconnectBodyBytes_ = 0;
}

void AsyncHttpRequest::parseResponseInLoop()
{
    if(!responseHeadersParsed_)
    {
        const size_t headerEnd = responseBytes_.find("\r\n\r\n");
        if(headerEnd == std::string::npos) return;
        //getline()这个函数是可以读取空格，遇到换行符或者EOF结束，但是不读取换行符的
        std::istringstream lines(responseBytes_.substr(0, headerEnd));
        std::string statusLine;
        if(!std::getline(lines, statusLine))
        {
            HttpClientResponse empty;
            completeInLoop(std::move(empty), "invalid HTTP response status line");
            return;
        }
        //HTTP/1.1 200 OK\r\n               <-- 起始行 (声明协议版本、状态码)
        std::istringstream status(statusLine);
        std::string version;
        status >> version >> parsedResponse_.status;
        if(version.rfind("HTTP/", 0) != 0 || parsedResponse_.status == 0)
        {
            HttpClientResponse empty;
            completeInLoop(std::move(empty), "invalid HTTP response status line");
            return;
        }

        std::string line;
        while(std::getline(lines, line))
        {
            const size_t colon = line.find(':');
            if(colon == std::string::npos) continue;
            parsedResponse_.headers[trim(line.substr(0, colon))] = trim(line.substr(colon + 1));
        }
        responseHeadersParsed_ = true;
        responseBodyOffset_ = headerEnd + 4;// 越过 "\r\n\r\n" 这 4 个字节

        bool hasLength = false;
        for(const auto& [name, value] : parsedResponse_.headers)
        {
            if(lower(name) == "content-length")
            {
                responseHasContentLength_ = true;
                hasLength = parseUnsigned(value, expectedResponseBodyBytes_);
                break;
            }
        }
        if(responseHasContentLength_ && !hasLength)
        {
            HttpClientResponse empty;
            completeInLoop(std::move(empty), "invalid HTTP response Content-Length");
            return;
        }
        if(!hasLength && (parsedResponse_.status == 204 || parsedResponse_.status == 304))
        {
            hasLength = true;
            expectedResponseBodyBytes_ = 0;
        }
        if(!hasLength)
        {
            // Connection: close is mandatory for V2 AsyncHttpRequest. The
            // remaining body is finalized when TcpConnection reports close.
            expectedResponseBodyBytes_ = 0;
            return;
        }
    }

    const size_t availableBody = responseBytes_.size() - responseBodyOffset_;
    if(expectedResponseBodyBytes_ > 0 && availableBody < expectedResponseBodyBytes_)
    {
        return;
    }
    if(expectedResponseBodyBytes_ == 0 && responseHeadersParsed_)
    {
        // A response without Content-Length is complete only on connection
        // close. A zero Content-Length response is complete immediately.
        bool explicitZeroLength = false;
        for(const auto& [name, value] : parsedResponse_.headers)
        {
            if(lower(name) == "content-length")
            {
                explicitZeroLength = trim(value) == "0";
                break;
            }
        }
        if(!explicitZeroLength && parsedResponse_.status != 204 && parsedResponse_.status != 304)
        {
            return;
        }
    }

    parsedResponse_.body = responseBytes_.substr(responseBodyOffset_,
                                                  static_cast<size_t>(expectedResponseBodyBytes_));
    completeInLoop(std::move(parsedResponse_), "");
}

void AsyncHttpRequest::onHighWaterMark(size_t queuedBytes)
{
    if(!finished_ && highWaterMarkCallback_) highWaterMarkCallback_(queuedBytes);
}

void AsyncHttpRequest::onLowWaterMark(size_t queuedBytes)
{
    if(!finished_ && lowWaterMarkCallback_) lowWaterMarkCallback_(queuedBytes);
}

void AsyncHttpRequest::completeInLoop(HttpClientResponse response, std::string error)
{
    //结束连接
    if(finished_) return;

    finished_ = true;
    state_ = State::kFinished;
    cancelTimeoutInLoop();

    auto callback = std::move(responseCallback_);
    readyCallback_  = nullptr;
    connection_.reset();
    if(client_)
    {
        client_->stop();
        client_.reset();
    }
    releaseLifetimeInLoop();
    if(callback) callback(std::move(response), std::move(error));
}

void AsyncHttpRequest::cancelTimeoutInLoop()
{
    if(timeoutTimerId_ != 0)
    {
        loop_->cancel(timeoutTimerId_);
        timeoutTimerId_ = 0;
    }
}

void AsyncHttpRequest::holdLifetimeInLoop() {
    if(!lifetimeGuard_)
    {
        lifetimeGuard_ = shared_from_this();
    }
}

void AsyncHttpRequest::releaseLifetimeInLoop() {
    lifetimeGuard_.reset();
}

}
}



