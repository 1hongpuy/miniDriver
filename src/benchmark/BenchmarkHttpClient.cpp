#include "benchmark/BenchmarkHttpClient.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <poll.h>
#include <sstream>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace miniKV::benchmark {
namespace {

bool waitFor(int fd, short events, int timeoutMs, std::string& error) {
    pollfd descriptor{fd, events, 0};
    const int result = ::poll(&descriptor, 1, timeoutMs);
    if (result == 0) { error = "HTTP socket timed out"; return false; }
    if (result < 0) { error = std::string("poll failed: ") + std::strerror(errno); return false; }
    if (descriptor.revents & (POLLERR | POLLNVAL)) { error = "HTTP socket error"; return false; }
    if (events == POLLIN && (descriptor.revents & POLLHUP)) return true;
    if (!(descriptor.revents & events)) { error = "unexpected HTTP socket event"; return false; }
    return true;
}

int connectSocket(const Endpoint& endpoint, int timeoutMs, std::string& error) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* results = nullptr;
    if (::getaddrinfo(endpoint.host.c_str(), std::to_string(endpoint.port).c_str(), &hints, &results) != 0) {
        error = "cannot resolve HTTP endpoint " + endpoint.host;
        return -1;
    }
    int fd = -1;
    for (addrinfo* item = results; item != nullptr; item = item->ai_next) {
        fd = ::socket(item->ai_family, item->ai_socktype | SOCK_CLOEXEC | SOCK_NONBLOCK, item->ai_protocol);
        if (fd < 0) continue;
        if (::connect(fd, item->ai_addr, item->ai_addrlen) != 0) {
            if (errno != EINPROGRESS || !waitFor(fd, POLLOUT, timeoutMs, error)) {
                ::close(fd); fd = -1; continue;
            }
            int socketError = 0;
            socklen_t length = sizeof(socketError);
            if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socketError, &length) != 0 || socketError != 0) {
                error = socketError == 0 ? "getsockopt(SO_ERROR) failed" : std::strerror(socketError);
                ::close(fd); fd = -1; continue;
            }
        }
        break;
    }
    ::freeaddrinfo(results);
    if (fd < 0 && error.empty()) error = "cannot connect to HTTP endpoint";
    return fd;
}

bool writeAll(int fd, const char* data, size_t size, int timeoutMs, std::string& error) {
    size_t written = 0;
    while (written < size) {
        if (!waitFor(fd, POLLOUT, timeoutMs, error)) return false;
        const ssize_t result = ::send(fd, data + written, size - written, MSG_NOSIGNAL);
        if (result > 0) { written += static_cast<size_t>(result); continue; }
        if (result < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
        error = result == 0 ? "HTTP socket closed during write" : std::strerror(errno);
        return false;
    }
    return true;
}

bool parseContentLength(const std::map<std::string, std::string>& headers, uint64_t& length) {
    const auto it = headers.find("Content-Length");
    if (it == headers.end() || it->second.empty()) return false;
    length = 0;
    for (const char value : it->second) {
        if (value < '0' || value > '9') return false;
        if (length > (UINT64_MAX - static_cast<uint64_t>(value - '0')) / 10) return false;
        length = length * 10 + static_cast<uint64_t>(value - '0');
    }
    return true;
}

bool readResponse(int fd, int timeoutMs, HttpResponse& response, std::string& error,
                  uint64_t maxReadBytesPerSecond) {
    response = {};
    std::string bytes;
    char buffer[64 * 1024];
    size_t headerEnd = std::string::npos;
    uint64_t expectedBody = 0;
    while (true) {
        if (headerEnd != std::string::npos && bytes.size() >= headerEnd + 4 + expectedBody) break;
        if (!waitFor(fd, POLLIN, timeoutMs, error)) return false;
        const ssize_t result = ::recv(fd, buffer, sizeof(buffer), 0);
        if (result == 0) {
            error = "HTTP response closed before Content-Length body completed";
            return false;
        }
        if (result < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
            error = std::strerror(errno);
            return false;
        }
        const size_t oldSize = bytes.size();
        bytes.append(buffer, static_cast<size_t>(result));
        if (headerEnd != std::string::npos) {
            if (maxReadBytesPerSecond > 0) {
                const size_t bodyStart = headerEnd + 4;
                const size_t newlyReadBody = bytes.size() > std::max(oldSize, bodyStart)
                    ? bytes.size() - std::max(oldSize, bodyStart) : 0;
                if (newlyReadBody > 0) {
                    const uint64_t nanoseconds = static_cast<uint64_t>(newlyReadBody) * 1000000000ULL /
                        maxReadBytesPerSecond;
                    if (nanoseconds > 0) std::this_thread::sleep_for(std::chrono::nanoseconds(nanoseconds));
                }
            }
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
        if (maxReadBytesPerSecond > 0 && bytes.size() > headerEnd + 4) {
            const uint64_t bodyAlreadyRead = bytes.size() - (headerEnd + 4);
            const uint64_t nanoseconds = bodyAlreadyRead * 1000000000ULL / maxReadBytesPerSecond;
            if (nanoseconds > 0) std::this_thread::sleep_for(std::chrono::nanoseconds(nanoseconds));
        }
    }
    response.body.assign(bytes.data() + headerEnd + 4, static_cast<size_t>(expectedBody));
    return true;
}

}

StreamingRequest::~StreamingRequest() { cancel(); }

bool StreamingRequest::open(const Endpoint& endpoint, std::string_view method, std::string_view path,
                            const std::map<std::string, std::string>& headers,
                            uint64_t contentLength, int timeoutMs, std::string& error,
                            bool keepAlive) {
    const std::string endpointKey = endpoint.host + ':' + std::to_string(endpoint.port);
    if(fd_ >= 0 && (!keepAlive_ || !keepAlive || endpointKey_ != endpointKey)) cancel();
    error.clear();
    if(fd_ < 0) {
        fd_ = connectSocket(endpoint, timeoutMs, error);
        if (fd_ < 0) return false;
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
    if (fd_ < 0 || (size > 0 && bytes == nullptr) || sentBytes_ + size > expectedBytes_) {
        error = "invalid benchmark streaming write";
        return false;
    }
    if (size > 0 && !writeAll(fd_, bytes, size, timeoutMs_, error)) { cancel(); return false; }
    sentBytes_ += size;
    return true;
}

bool StreamingRequest::finish(HttpResponse& response, std::string& error,
                              uint64_t maxReadBytesPerSecond) {
    if (fd_ < 0 || sentBytes_ != expectedBytes_) { error = "benchmark request body is incomplete"; cancel(); return false; }
    const bool ok = readResponse(fd_, timeoutMs_, response, error, maxReadBytesPerSecond);
    expectedBytes_ = 0;
    sentBytes_ = 0;
    if(!ok || !keepAlive_ || response.headers["Connection"] == "close") cancel();
    return ok;
}

void StreamingRequest::cancel() {
    if (fd_ >= 0) ::close(fd_);
    fd_ = -1;
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

}  // namespace miniKV::benchmark
