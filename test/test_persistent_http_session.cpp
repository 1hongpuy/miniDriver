#include "http/PersistentHttpSession.hpp"
#include "network/EventLoop.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {

class KeepAliveServer {
public:
    KeepAliveServer()
    {
        fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if(fd_ < 0) std::abort();
        int reuse = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if(::bind(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
           ::listen(fd_, 2) != 0) std::abort();
        socklen_t length = sizeof(address);
        if(::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &length) != 0) std::abort();
        port_ = ntohs(address.sin_port);
        thread_ = std::thread([this] { serve(); });
    }

    ~KeepAliveServer()
    {
        if(fd_ >= 0) ::close(fd_);
        if(thread_.joinable()) thread_.join();
    }

    uint16_t port() const { return port_; }
    int accepts() const { return accepts_; }

private:
    static bool readRequest(int peer, std::string& pending)
    {
        while(pending.find("\r\n\r\n") == std::string::npos) {
            char bytes[1024];
            const auto n = ::recv(peer, bytes, sizeof(bytes), 0);
            if(n <= 0) return false;
            pending.append(bytes, static_cast<size_t>(n));
        }
        const auto headerEnd = pending.find("\r\n\r\n");
        const auto content = pending.find("Content-Length:");
        size_t length = 0;
        if(content != std::string::npos) {
            const auto lineEnd = pending.find("\r\n", content);
            length = static_cast<size_t>(std::stoul(pending.substr(content + 15, lineEnd - content - 15)));
        }
        const size_t total = headerEnd + 4 + length;
        while(pending.size() < total) {
            char bytes[1024];
            const auto n = ::recv(peer, bytes, sizeof(bytes), 0);
            if(n <= 0) return false;
            pending.append(bytes, static_cast<size_t>(n));
        }
        pending.erase(0, total);
        return true;
    }

    void serve()
    {
        const int peer = ::accept(fd_, nullptr, nullptr);
        if(peer < 0) return;
        ++accepts_;
        std::string pending;
        for(int request = 0; request < 2; ++request) {
            if(!readRequest(peer, pending)) break;
            const std::string response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n"
                                         "Connection: keep-alive\r\n\r\nok";
            ::send(peer, response.data(), response.size(), MSG_NOSIGNAL);
        }
        ::close(peer);
    }

    int fd_ = -1;
    uint16_t port_ = 0;
    int accepts_ = 0;
    std::thread thread_;
};

}  // namespace

int main()
{
    KeepAliveServer server;
    std::mutex mutex;
    std::condition_variable condition;
    miniKV::network::EventLoop* loop = nullptr;
    bool loopReady = false;
    int completed = 0;
    std::string failure;
    std::thread loopThread([&] {
        miniKV::network::EventLoop localLoop;
        {
            std::lock_guard<std::mutex> lock(mutex);
            loop = &localLoop;
            loopReady = true;
            condition.notify_all();
        }
        localLoop.loop();
    });
    {
        std::unique_lock<std::mutex> lock(mutex);
        condition.wait(lock, [&] { return loopReady; });
    }

    auto session = miniKV::http::PersistentHttpSession::create(loop);
    auto begin = [&](auto&& self, int index) -> void {
        loop->queueInLoop([&, self, index] {
            miniKV::http::AsyncHttpRequestOptions options;
            options.address = "127.0.0.1";
            options.port = server.port();
            options.method = "PUT";
            options.path = "/replica/" + std::to_string(index);
            options.contentLength = 1;
            options.timeoutMs = 1000;
            const bool accepted = session->start(options, [session] {
                if(session->write("x", 1) != miniKV::http::AsyncWriteResult::kAccepted) return;
                session->finishBody();
            }, [&, self, index](miniKV::http::HttpClientResponse response, std::string error) {
                if(!error.empty() || response.status != 200 || response.body != "ok") failure = error.empty() ? "bad response" : error;
                ++completed;
                if(index == 0 && failure.empty()) self(self, 1);
                else condition.notify_all();
            });
            if(!accepted) { failure = "session refused request"; condition.notify_all(); }
        });
    };
    begin(begin, 0);

    {
        std::unique_lock<std::mutex> lock(mutex);
        condition.wait_for(lock, std::chrono::seconds(3), [&] { return completed == 2 || !failure.empty(); });
    }
    session->close();
    loop->quit();
    loopThread.join();

    if(!failure.empty() || completed != 2 || server.accepts() != 1) {
        std::fprintf(stderr, "FAIL: completed=%d accepts=%d error=%s\n", completed, server.accepts(), failure.c_str());
        return 1;
    }
    std::puts("PASS: persistent HTTP session reuses one TCP connection for sequential requests");
    return 0;
}
