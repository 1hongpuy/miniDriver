#include "http/PersistentHttpSession.hpp"

#include "network/Buffer.hpp"
#include "network/EventLoop.hpp"
#include "network/TcpClient.hpp"
#include "network/TcpConnection.hpp"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <utility>

namespace miniKV::http {
namespace {

std::string lower(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

std::string trim(std::string value)
{
    const auto first = value.find_first_not_of(" \t\r\n");
    if(first == std::string::npos) return "";
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

bool parseUnsigned(const std::string& value, uint64_t& out)
{
    const auto stripped = trim(value);
    try {
        size_t consumed = 0;
        out = std::stoull(stripped, &consumed);
        return consumed == stripped.size();
    } catch(...) {
        return false;
    }
}

bool managedHeader(const std::string& name)
{
    const auto key = lower(name);
    return key == "host" || key == "connection" || key == "content-length";
}

}  // namespace

PersistentHttpSession::Ptr PersistentHttpSession::create(network::EventLoop* loop)
{
    return Ptr(new PersistentHttpSession(loop));
}

PersistentHttpSession::PersistentHttpSession(network::EventLoop* loop) : loop_(loop) {}

PersistentHttpSession::~PersistentHttpSession() = default;

bool PersistentHttpSession::start(AsyncHttpRequestOptions options, ReadyCallback ready,
                                  ResponseCallback response)
{
    if(!loop_->isInLoopThread()) return false;
    return startInLoop(std::move(options), std::move(ready), std::move(response));
}

bool PersistentHttpSession::startInLoop(AsyncHttpRequestOptions options, ReadyCallback ready,
                                        ResponseCallback response)
{
    if(state_ != State::kIdle || options.address.empty() || options.port == 0 ||
       options.method.empty() || options.path.empty()) {
        if(response) response({}, "persistent HTTP session is not idle or options are invalid");
        return false;
    }
    options_ = std::move(options);
    readyCallback_ = std::move(ready);
    responseCallback_ = std::move(response);
    if(connection_ && connection_->connected()) {
        ready_ = true;
        state_ = State::kWritingBody;
        sendHeadersInLoop();
        if(readyCallback_) readyCallback_();
        return true;
    }
    state_ = State::kConnecting;
    connectInLoop();
    return true;
}

void PersistentHttpSession::connectInLoop()
{
    client_ = network::TcpClient::create(loop_);
    client_->setConnectTimeout(options_.timeoutMs);
    const auto weakSelf = std::weak_ptr<PersistentHttpSession>(shared_from_this());
    client_->setConnectionCallback([weakSelf](const network::TcpConnectionPtr& connection) {
        if(auto self = weakSelf.lock()) self->onConnection(connection);
    });
    client_->setMessageCallback([weakSelf](const network::TcpConnectionPtr& connection,
                                           network::Buffer* buffer) {
        if(auto self = weakSelf.lock()) self->onMessage(connection, buffer);
    });
    client_->connect(options_.address, options_.port);
}

AsyncWriteResult PersistentHttpSession::write(const char* data, size_t size)
{
    if(!loop_->isInLoopThread()) return AsyncWriteResult::kWouldBlock;
    return writeInLoop(data, size);
}

AsyncWriteResult PersistentHttpSession::writeInLoop(const char* data, size_t size)
{
    if(state_ == State::kIdle || state_ == State::kClosed || state_ == State::kWaitingResponse ||
       data == nullptr) return AsyncWriteResult::kClosed;
    if(size == 0) return AsyncWriteResult::kAccepted;
    if(acceptedBodyBytes_ + size > options_.contentLength) {
        failAndCloseInLoop("request body exceeds Content-Length");
        return AsyncWriteResult::kClosed;
    }
    if(!ready_) {
        if(preconnectBodyBytes_ + size > options_.maxPreconnectBodyBytes) {
            return AsyncWriteResult::kWouldBlock;
        }
        preconnectBody_.emplace_back(data, size);
        preconnectBodyBytes_ += size;
    } else {
        connection_->send(data, size);
    }
    acceptedBodyBytes_ += size;
    return AsyncWriteResult::kAccepted;
}

void PersistentHttpSession::finishBody()
{
    auto self = shared_from_this();
    loop_->runInLoop([self] { self->finishBodyInLoop(); });
}

void PersistentHttpSession::finishBodyInLoop()
{
    if(state_ == State::kIdle || state_ == State::kClosed || finishRequested_) return;
    if(acceptedBodyBytes_ != options_.contentLength) {
        failAndCloseInLoop("request body does not match Content-Length");
        return;
    }
    finishRequested_ = true;
    if(ready_) {
        state_ = State::kWaitingResponse;
        startResponseTimeoutInLoop();
    }
}

void PersistentHttpSession::cancel()
{
    auto self = shared_from_this();
    loop_->runInLoop([self] { self->cancelInLoop(); });
}

void PersistentHttpSession::cancelInLoop()
{
    if(state_ == State::kIdle || state_ == State::kClosed) return;
    failAndCloseInLoop("persistent HTTP request cancelled");
}

void PersistentHttpSession::close()
{
    auto self = shared_from_this();
    loop_->runInLoop([self] { self->closeInLoop(); });
}

void PersistentHttpSession::closeInLoop()
{
    cancelResponseTimeoutInLoop();
    readyCallback_ = nullptr;
    responseCallback_ = nullptr;
    clearRequestStateInLoop();
    connection_.reset();
    if(client_) {
        auto client = std::move(client_);
        client->stop();
    }
    state_ = State::kClosed;
}

void PersistentHttpSession::onConnection(const network::TcpConnectionPtr& connection)
{
    if(state_ == State::kClosed) return;
    if(!connection) {
        failAndCloseInLoop("persistent HTTP TCP connection failed");
        return;
    }
    if(!connection->connected()) {
        if(state_ != State::kIdle) failAndCloseInLoop("persistent HTTP connection closed before response");
        else { connection_.reset(); client_.reset(); state_ = State::kClosed; }
        return;
    }
    connection_ = connection;
    const auto weakSelf = std::weak_ptr<PersistentHttpSession>(shared_from_this());
    connection_->setHighWaterMark(options_.highWaterMark, [weakSelf](const auto&, size_t bytes) {
        if(auto self = weakSelf.lock(); self && self->highWaterMarkCallback_) self->highWaterMarkCallback_(bytes);
    });
    connection_->setLowWaterMark(options_.lowWaterMark, [weakSelf](const auto&, size_t bytes) {
        if(auto self = weakSelf.lock(); self && self->lowWaterMarkCallback_) self->lowWaterMarkCallback_(bytes);
    });
    ready_ = true;
    state_ = State::kWritingBody;
    sendHeadersInLoop();
    flushPreconnectBodyInLoop();
    if(finishRequested_) {
        state_ = State::kWaitingResponse;
        startResponseTimeoutInLoop();
    }
    if(readyCallback_) readyCallback_();
}

void PersistentHttpSession::onMessage(const network::TcpConnectionPtr& connection, network::Buffer* buffer)
{
    if(connection != connection_ || !buffer || state_ != State::kWaitingResponse) return;
    const size_t readable = buffer->readableBytes();
    if(responseBytes_.size() + readable > options_.maxResponseBytes) {
        failAndCloseInLoop("HTTP response exceeds configured limit");
        return;
    }
    responseBytes_.append(buffer->peek(), readable);
    buffer->retrieve(readable);
    parseResponseInLoop();
}

void PersistentHttpSession::sendHeadersInLoop()
{
    std::ostringstream request;
    request << options_.method << " " << options_.path << " HTTP/1.1\r\n";
    request << "Host: " << options_.address << "\r\n";
    request << "Connection: keep-alive\r\n";
    for(const auto& [name, value] : options_.headers) {
        if(!managedHeader(name)) request << name << ": " << value << "\r\n";
    }
    request << "Content-Length: " << options_.contentLength << "\r\n\r\n";
    connection_->send(request.str());
}

void PersistentHttpSession::flushPreconnectBodyInLoop()
{
    for(const auto& part : preconnectBody_) connection_->send(part);
    preconnectBody_.clear();
    preconnectBodyBytes_ = 0;
}

void PersistentHttpSession::parseResponseInLoop()
{
    if(!responseHeadersParsed_) {
        const auto headerEnd = responseBytes_.find("\r\n\r\n");
        if(headerEnd == std::string::npos) return;
        std::istringstream lines(responseBytes_.substr(0, headerEnd));
        std::string statusLine;
        if(!std::getline(lines, statusLine)) { failAndCloseInLoop("invalid HTTP response status line"); return; }
        std::istringstream status(statusLine);
        std::string version;
        status >> version >> parsedResponse_.status;
        if(version.rfind("HTTP/", 0) != 0 || parsedResponse_.status == 0) {
            failAndCloseInLoop("invalid HTTP response status line"); return;
        }
        std::string line;
        bool hasLength = false;
        while(std::getline(lines, line)) {
            const auto colon = line.find(':');
            if(colon == std::string::npos) continue;
            const auto name = trim(line.substr(0, colon));
            const auto value = trim(line.substr(colon + 1));
            parsedResponse_.headers[name] = value;
            if(lower(name) == "content-length") hasLength = parseUnsigned(value, expectedResponseBodyBytes_);
            if(lower(name) == "connection" && lower(value) == "close") responseConnectionClose_ = true;
        }
        if(!hasLength) { failAndCloseInLoop("persistent HTTP response lacks valid Content-Length"); return; }
        responseHeadersParsed_ = true;
        responseBodyOffset_ = headerEnd + 4;
    }
    const auto bodyBytes = responseBytes_.size() - responseBodyOffset_;
    if(bodyBytes < expectedResponseBodyBytes_) return;
    if(bodyBytes > expectedResponseBodyBytes_) {
        failAndCloseInLoop("unexpected bytes after persistent HTTP response");
        return;
    }
    parsedResponse_.body = responseBytes_.substr(responseBodyOffset_, static_cast<size_t>(expectedResponseBodyBytes_));
    completeRequestInLoop(std::move(parsedResponse_), "", !responseConnectionClose_);
}

void PersistentHttpSession::startResponseTimeoutInLoop()
{
    if(timeoutTimerId_ != 0 || options_.timeoutMs <= 0) return;
    const auto weakSelf = std::weak_ptr<PersistentHttpSession>(shared_from_this());
    timeoutTimerId_ = loop_->runAfter(options_.timeoutMs, [weakSelf] {
        if(auto self = weakSelf.lock()) self->failAndCloseInLoop("persistent HTTP response timed out");
    });
}

void PersistentHttpSession::cancelResponseTimeoutInLoop()
{
    if(timeoutTimerId_ != 0) { loop_->cancel(timeoutTimerId_); timeoutTimerId_ = 0; }
}

void PersistentHttpSession::completeRequestInLoop(HttpClientResponse response, std::string error,
                                                  bool keepConnection)
{
    cancelResponseTimeoutInLoop();
    auto callback = std::move(responseCallback_);
    readyCallback_ = nullptr;
    clearRequestStateInLoop();
    if(keepConnection && connection_ && connection_->connected()) state_ = State::kIdle;
    else closeInLoop();
    if(callback) callback(std::move(response), std::move(error));
}

void PersistentHttpSession::clearRequestStateInLoop()
{
    options_ = {};
    preconnectBody_.clear();
    preconnectBodyBytes_ = 0;
    acceptedBodyBytes_ = 0;
    finishRequested_ = false;
    ready_ = connection_ && connection_->connected();
    responseBytes_.clear();
    parsedResponse_ = {};
    responseBodyOffset_ = 0;
    expectedResponseBodyBytes_ = 0;
    responseHeadersParsed_ = false;
    responseConnectionClose_ = false;
}

void PersistentHttpSession::failAndCloseInLoop(std::string error)
{
    if(state_ == State::kClosed) return;
    HttpClientResponse empty;
    completeRequestInLoop(std::move(empty), std::move(error), false);
}

}  // namespace miniKV::http
