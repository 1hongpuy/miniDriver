#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#else
#include <sys/types.h>
#endif

namespace miniKV::client {

#ifdef _WIN32
using TransportSocket = SOCKET;
constexpr TransportSocket kInvalidTransportSocket = INVALID_SOCKET;
#else
using TransportSocket = int;
constexpr TransportSocket kInvalidTransportSocket = -1;
#endif

// Deliberately small synchronous transport used by the first SDK core.  The
// SDK owns object semantics; this type only owns a sequential HTTP/1.1 socket.
struct Endpoint {
    std::string host;
    uint16_t port = 0;
};

struct HttpResponse {
    int status = 0;
    std::map<std::string, std::string> headers;
    std::string body;
};

class StreamingRequest {
public:
    struct Stats {
        uint64_t connectionOpens = 0;
        uint64_t requests = 0;
        uint64_t connectionReuses = 0;
    };

    StreamingRequest() = default;
    ~StreamingRequest();
    StreamingRequest(const StreamingRequest&) = delete;
    StreamingRequest& operator=(const StreamingRequest&) = delete;

    bool open(const Endpoint& endpoint, std::string_view method, std::string_view path,
              const std::map<std::string, std::string>& headers,
              uint64_t contentLength, int timeoutMs, std::string& error,
              bool keepAlive = false);
    bool write(const char* bytes, size_t size, std::string& error);
    bool finish(HttpResponse& response, std::string& error,
                uint64_t maxReadBytesPerSecond = 0);
    void cancel();
    const Stats& stats() const { return stats_; }

private:
    TransportSocket fd_ = kInvalidTransportSocket;
    uint64_t expectedBytes_ = 0;
    uint64_t sentBytes_ = 0;
    int timeoutMs_ = 0;
    bool keepAlive_ = false;
    std::string endpointKey_;
    Stats stats_;
};

bool httpRequest(const Endpoint& endpoint, std::string_view method, std::string_view path,
                 const std::map<std::string, std::string>& headers,
                 std::string_view body, int timeoutMs,
                 HttpResponse& response, std::string& error);

}  // namespace miniKV::client
