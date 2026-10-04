#include "client/HttpTransport.hpp"

#include <algorithm>
#include <climits>
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <chrono>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <poll.h>
#include <sstream>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#endif

namespace miniKV::client {
namespace {

#ifdef _WIN32
using PollDescriptor = WSAPOLLFD;
constexpr short kPollIn = POLLRDNORM;
constexpr short kPollOut = POLLWRNORM;
bool ensureWinsock(std::string& error) {
    static const bool ready = [] {
        WSADATA data{};
        return ::WSAStartup(MAKEWORD(2, 2), &data) == 0;
    }();
    if (!ready) error = "WSAStartup failed";
    return ready;
}
int socketError() { return ::WSAGetLastError(); }
bool interruptedOrWouldBlock(int error) {
    return error == WSAEINTR || error == WSAEWOULDBLOCK;
}
std::string socketErrorText(int error) { return "WinSock error " + std::to_string(error); }
void closeSocket(TransportSocket fd) { if (fd != kInvalidTransportSocket) ::closesocket(fd); }
#else
using PollDescriptor = pollfd;
constexpr short kPollIn = POLLIN;
constexpr short kPollOut = POLLOUT;
bool ensureWinsock(std::string&) { return true; }
int socketError() { return errno; }
bool interruptedOrWouldBlock(int error) { return error == EINTR || error == EAGAIN || error == EWOULDBLOCK; }
std::string socketErrorText(int error) { return std::strerror(error); }
void closeSocket(TransportSocket fd) { if (fd != kInvalidTransportSocket) ::close(fd); }
#endif

bool waitFor(TransportSocket fd, short events, int timeoutMs, std::string& error) {
    PollDescriptor descriptor{fd, events, 0};
#ifdef _WIN32
    const int result = ::WSAPoll(&descriptor, 1, timeoutMs);
#else
    const int result = ::poll(&descriptor, 1, timeoutMs);
#endif
    if (result == 0) { error = "HTTP socket timed out"; return false; }
    if (result < 0) { error = std::string("poll failed: ") + socketErrorText(socketError()); return false; }
    if (descriptor.revents & (POLLERR | POLLNVAL)) { error = "HTTP socket error"; return false; }
    if (events == kPollIn && (descriptor.revents & POLLHUP)) return true;
    if (!(descriptor.revents & events)) { error = "unexpected HTTP socket event"; return false; }
    return true;
}

TransportSocket connectSocket(const Endpoint& endpoint, int timeoutMs, std::string& error) {
    if (!ensureWinsock(error)) return kInvalidTransportSocket;
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* results = nullptr;
    if (::getaddrinfo(endpoint.host.c_str(), std::to_string(endpoint.port).c_str(), &hints, &results) != 0) {
        error = "cannot resolve HTTP endpoint " + endpoint.host;
        return kInvalidTransportSocket;
    }
    TransportSocket fd = kInvalidTransportSocket;
    for (addrinfo* item = results; item != nullptr; item = item->ai_next) {
        int socketType = item->ai_socktype;
#ifndef _WIN32
        socketType |= SOCK_CLOEXEC | SOCK_NONBLOCK;
#endif
        fd = ::socket(item->ai_family, socketType, item->ai_protocol);
        if (fd == kInvalidTransportSocket) continue;
#ifdef _WIN32
        u_long nonBlocking = 1;
        if (::ioctlsocket(fd, FIONBIO, &nonBlocking) != 0) { closeSocket(fd); fd = kInvalidTransportSocket; continue; }
#endif
        if (::connect(fd, item->ai_addr, item->ai_addrlen) != 0) {
            const int connectError = socketError();
#ifdef _WIN32
            const bool inProgress = connectError == WSAEWOULDBLOCK || connectError == WSAEINPROGRESS;
#else
            const bool inProgress = connectError == EINPROGRESS;
#endif
            if (!inProgress || !waitFor(fd, kPollOut, timeoutMs, error)) {
                closeSocket(fd); fd = kInvalidTransportSocket; continue;
            }
            int socketError = 0;
#ifdef _WIN32
            int length = sizeof(socketError);
#else
            socklen_t length = sizeof(socketError);
#endif
            if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socketError, &length) != 0 || socketError != 0) {
                error = socketError == 0 ? "getsockopt(SO_ERROR) failed" : socketErrorText(socketError);
                closeSocket(fd); fd = kInvalidTransportSocket; continue;
            }
        }
        break;
    }
    ::freeaddrinfo(results);
    if (fd == kInvalidTransportSocket && error.empty()) error = "cannot connect to HTTP endpoint";
    return fd;
}

bool writeAll(TransportSocket fd, const char* data, size_t size, int timeoutMs, std::string& error) {
    size_t written = 0;
    while (written < size) {
        if (!waitFor(fd, kPollOut, timeoutMs, error)) return false;
#ifdef _WIN32
        const int result = ::send(fd, data + written, static_cast<int>(std::min<size_t>(size - written, INT_MAX)), 0);
#else
        const ssize_t result = ::send(fd, data + written, size - written, MSG_NOSIGNAL);
#endif
        if (result > 0) { written += static_cast<size_t>(result); continue; }
        if (result < 0 && interruptedOrWouldBlock(socketError())) continue;
        error = result == 0 ? "HTTP socket closed during write" : socketErrorText(socketError());
        return false;
    }
    return true;
}

std::string lowerHeaderName(std::string name) {
    for (char& c : name) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return name;
}

const std::string* findHeader(const std::map<std::string, std::string>& headers,
                              const char* wanted) {
    const std::string normalized = lowerHeaderName(wanted);
    for (const auto& [name, value] : headers) {
        if (lowerHeaderName(name) == normalized) return &value;
    }
    return nullptr;
}

bool parseContentLength(const std::map<std::string, std::string>& headers, uint64_t& length) {
    const std::string* value = findHeader(headers, "Content-Length");
    if (value == nullptr || value->empty()) return false;
    length = 0;
    for (const char c : *value) {
        if (c < '0' || c > '9') return false;
        if (length > (UINT64_MAX - static_cast<uint64_t>(c - '0')) / 10) return false;
        length = length * 10 + static_cast<uint64_t>(c - '0');
    }
    return true;
}

void throttle(uint64_t bytes, uint64_t maxReadBytesPerSecond) {
    if (bytes == 0 || maxReadBytesPerSecond == 0) return;
    const uint64_t nanoseconds = bytes * 1000000000ULL / maxReadBytesPerSecond;
    if (nanoseconds > 0) std::this_thread::sleep_for(std::chrono::nanoseconds(nanoseconds));
}

bool readResponse(TransportSocket fd, int timeoutMs, HttpResponse& response, std::string& error,
                  uint64_t maxReadBytesPerSecond) {
    response = {};
    std::string bytes;
    char buffer[64 * 1024];
    size_t headerEnd = std::string::npos;
    uint64_t expectedBody = 0;
    while (true) {
        if (headerEnd != std::string::npos && bytes.size() >= headerEnd + 4 + expectedBody) break;
        if (!waitFor(fd, kPollIn, timeoutMs, error)) return false;
#ifdef _WIN32
        const int result = ::recv(fd, buffer, static_cast<int>(sizeof(buffer)), 0);
#else
        const ssize_t result = ::recv(fd, buffer, sizeof(buffer), 0);
#endif
        if (result == 0) { error = "HTTP response closed before Content-Length body completed"; return false; }
        if (result < 0) {
            if (interruptedOrWouldBlock(socketError())) continue;
            error = socketErrorText(socketError()); return false;
        }
        const size_t oldSize = bytes.size();
        bytes.append(buffer, static_cast<size_t>(result));
        if (headerEnd != std::string::npos) {
            const size_t bodyStart = headerEnd + 4;
            const size_t fresh = bytes.size() > std::max(oldSize, bodyStart)
                ? bytes.size() - std::max(oldSize, bodyStart) : 0;
            throttle(fresh, maxReadBytesPerSecond);
            continue;
        }
        headerEnd = bytes.find("\r\n\r\n");
        if (headerEnd == std::string::npos) continue;
        std::istringstream headers(bytes.substr(0, headerEnd));
        std::string statusLine;
        if (!std::getline(headers, statusLine)) { error = "missing HTTP status line"; return false; }
        std::istringstream status(statusLine);
        std::string version;
        status >> version >> response.status;
        if (version.rfind("HTTP/", 0) != 0 || response.status == 0) { error = "invalid HTTP status line"; return false; }
        std::string line;
        while (std::getline(headers, line)) {
            if (!line.empty() && line.back() == '\r') line.pop_back();
            const size_t colon = line.find(':');
            if (colon == std::string::npos) continue;
            std::string name = line.substr(0, colon);
            std::string value = line.substr(colon + 1);
            while (!value.empty() && value.front() == ' ') value.erase(0, 1);
            response.headers[std::move(name)] = std::move(value);
        }
        if (!parseContentLength(response.headers, expectedBody)) { error = "HTTP response has no valid Content-Length"; return false; }
        if (bytes.size() > headerEnd + 4) throttle(bytes.size() - (headerEnd + 4), maxReadBytesPerSecond);
    }
    response.body.assign(bytes.data() + headerEnd + 4, static_cast<size_t>(expectedBody));
    return true;
}

}  // namespace

StreamingRequest::~StreamingRequest() { cancel(); }

bool StreamingRequest::open(const Endpoint& endpoint, std::string_view method, std::string_view path,
                            const std::map<std::string, std::string>& headers,
                            uint64_t contentLength, int timeoutMs, std::string& error,
                            bool keepAlive) {
    const std::string endpointKey = endpoint.host + ':' + std::to_string(endpoint.port);
    if (fd_ != kInvalidTransportSocket && (!keepAlive_ || !keepAlive || endpointKey_ != endpointKey)) cancel();
    error.clear();
    if (fd_ == kInvalidTransportSocket) {
        fd_ = connectSocket(endpoint, timeoutMs, error);
        if (fd_ == kInvalidTransportSocket) return false;
        ++stats_.connectionOpens;
    } else {
        ++stats_.connectionReuses;
    }
    std::ostringstream request;
    request << method << ' ' << path << " HTTP/1.1\r\nHost: " << endpoint.host
            << "\r\nConnection: " << (keepAlive ? "keep-alive" : "close") << "\r\n";
    for (const auto& [name, value] : headers) request << name << ": " << value << "\r\n";
    request << "Content-Length: " << contentLength << "\r\n\r\n";
    const std::string headerBytes = request.str();
    if (!writeAll(fd_, headerBytes.data(), headerBytes.size(), timeoutMs, error)) { cancel(); return false; }
    ++stats_.requests;
    expectedBytes_ = contentLength;
    sentBytes_ = 0;
    timeoutMs_ = timeoutMs;
    keepAlive_ = keepAlive;
    endpointKey_ = endpointKey;
    return true;
}

bool StreamingRequest::write(const char* bytes, size_t size, std::string& error) {
    if (fd_ == kInvalidTransportSocket || (size > 0 && bytes == nullptr) || sentBytes_ + size > expectedBytes_) {
        error = "invalid SDK streaming write"; return false;
    }
    if (size > 0 && !writeAll(fd_, bytes, size, timeoutMs_, error)) { cancel(); return false; }
    sentBytes_ += size;
    return true;
}

bool StreamingRequest::finish(HttpResponse& response, std::string& error,
                              uint64_t maxReadBytesPerSecond) {
    if (fd_ == kInvalidTransportSocket || sentBytes_ != expectedBytes_) { error = "SDK request body is incomplete"; cancel(); return false; }
    const bool ok = readResponse(fd_, timeoutMs_, response, error, maxReadBytesPerSecond);
    expectedBytes_ = 0;
    sentBytes_ = 0;
    const std::string* connection = findHeader(response.headers, "Connection");
    if (!ok || !keepAlive_ || (connection != nullptr && *connection == "close")) cancel();
    return ok;
}

void StreamingRequest::cancel() {
    closeSocket(fd_);
    fd_ = kInvalidTransportSocket;
    expectedBytes_ = 0;
    sentBytes_ = 0;
    timeoutMs_ = 0;
    keepAlive_ = false;
    endpointKey_.clear();
}

bool httpRequest(const Endpoint& endpoint, std::string_view method, std::string_view path,
                 const std::map<std::string, std::string>& headers,
                 std::string_view body, int timeoutMs,
                 HttpResponse& response, std::string& error) {
    StreamingRequest request;
    return request.open(endpoint, method, path, headers, body.size(), timeoutMs, error) &&
           request.write(body.data(), body.size(), error) && request.finish(response, error);
}

}  // namespace miniKV::client
