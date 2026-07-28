#include "http/AsyncHttpClient.hpp"
#include "network/EventLoop.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {

class SilentServer {
public:
    SilentServer()
    {
        fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if(fd_ < 0) std::abort();

        int reuse = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if(::bind(fd_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
           ::listen(fd_, 1) != 0) {
            std::abort();
        }

        socklen_t length = sizeof(address);
        if(::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &length) != 0) std::abort();
        port_ = ntohs(address.sin_port);
        thread_ = std::thread([this] {
            const int peer = ::accept(fd_, nullptr, nullptr);
            if(peer >= 0) {
                char buffer[1024];
                ::recv(peer, buffer, sizeof(buffer), 0);
                std::this_thread::sleep_for(std::chrono::seconds(2));
                ::close(peer);
            }
        });
    }

    ~SilentServer()
    {
        if(fd_ >= 0) ::close(fd_);
        if(thread_.joinable()) thread_.join();
    }

    uint16_t port() const { return port_; }

private:
    int fd_ = -1;
    uint16_t port_ = 0;
    std::thread thread_;
};

}  // namespace

int main()
{
    SilentServer server;
    miniKV::network::EventLoop loop;
    std::mutex mutex;
    std::condition_variable condition;
    bool callbackCalled = false;

    std::thread loopThread([&] { loop.loop(); });
    auto request = miniKV::http::AsyncHttpRequest::create(&loop);
    miniKV::http::AsyncHttpRequestOptions options;
    options.address = "127.0.0.1";
    options.port = server.port();
    options.method = "PUT";
    options.path = "/timeout-test";
    options.contentLength = 1;
    options.timeoutMs = 300;

    request->open(options, [request] {
        request->write("x", 1);
        // Deliberately do not call finishBody(). The peer cannot reply until
        // the request body is formally complete.
    }, [&](miniKV::http::HttpClientResponse, std::string) {
        std::lock_guard<std::mutex> lock(mutex);
        callbackCalled = true;
        condition.notify_one();
    });

    {
        std::unique_lock<std::mutex> lock(mutex);
        condition.wait_for(lock, std::chrono::milliseconds(700), [&] { return callbackCalled; });
    }

    loop.quit();
    loopThread.join();

    if(callbackCalled) {
        std::fprintf(stderr,
                     "FAIL: request timed out before finishBody(); response timeout started at open()\n");
        return 1;
    }
    std::puts("PASS: request did not start response timeout before finishBody()");
    return 0;
}
