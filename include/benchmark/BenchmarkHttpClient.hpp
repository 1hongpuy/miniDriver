#pragma once

#include "benchmark/BenchmarkTypes.hpp"

#include <cstdint>
#include <map>
#include <string>
#include <string_view>

namespace miniKV::benchmark {

struct HttpResponse {
    int status = 0;
    std::map<std::string, std::string> headers;
    std::string body;
};

class StreamingRequest {
public:
    StreamingRequest() = default;
    ~StreamingRequest();

    StreamingRequest(const StreamingRequest&) = delete;
    StreamingRequest& operator=(const StreamingRequest&) = delete;

    bool open(const Endpoint& endpoint, std::string_view method, std::string_view path,
              const std::map<std::string, std::string>& headers,
              uint64_t contentLength, int timeoutMs, std::string& error);
    bool write(const char* bytes, size_t size, std::string& error);
    bool finish(HttpResponse& response, std::string& error);
    void cancel();

private:
    int fd_ = -1;
    uint64_t expectedBytes_ = 0;
    uint64_t sentBytes_ = 0;
    int timeoutMs_ = 0;
};

bool httpRequest(const Endpoint& endpoint, std::string_view method, std::string_view path,
                 const std::map<std::string, std::string>& headers,
                 std::string_view body, int timeoutMs,
                 HttpResponse& response, std::string& error);

}  // namespace miniKV::benchmark
