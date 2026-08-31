#include "benchmark/BenchmarkHttpClient.hpp"
#include "TestCheck.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {

class FragmentedServer {
public:
    FragmentedServer() {
        fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        MINIKV_CHECK(fd_ >= 0);
        int reuse = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        MINIKV_CHECK(::bind(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        MINIKV_CHECK(::listen(fd_, 1) == 0);
        socklen_t length = sizeof(address);
        MINIKV_CHECK(::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &length) == 0);
        port_ = ntohs(address.sin_port);
        thread_ = std::thread([this] {
            const int peer = ::accept(fd_, nullptr, nullptr);
            MINIKV_CHECK(peer >= 0);
            for (int request = 0; request < 2; ++request) {
                std::string headers;
                char buffer[256];
                while (headers.find("\r\n\r\n") == std::string::npos) {
                    const ssize_t received = ::recv(peer, buffer, sizeof(buffer), 0);
                    MINIKV_CHECK(received > 0);
                    headers.append(buffer, static_cast<size_t>(received));
                }
                const char first[] = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n"
                    "X-Test: yes\r\nConnection: keep-alive\r\n\r\n";
                MINIKV_CHECK(::send(peer, first, sizeof(first) - 1, MSG_NOSIGNAL) > 0);
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                const char body = request == 0 ? 'a' : 'x';
                MINIKV_CHECK(::send(peer, &body, 1, MSG_NOSIGNAL) == 1);
                MINIKV_CHECK(::send(peer, request == 0 ? "bc" : "yz", 2, MSG_NOSIGNAL) == 2);
            }
            ::close(peer);
        });
    }

    ~FragmentedServer() {
        if (fd_ >= 0) ::close(fd_);
        if (thread_.joinable()) thread_.join();
    }

    uint16_t port() const { return port_; }

private:
    int fd_ = -1;
    uint16_t port_ = 0;
    std::thread thread_;
};

}

int main() {
    FragmentedServer server;
    miniKV::benchmark::StreamingRequest request;
    miniKV::benchmark::HttpResponse response;
    std::string error;
    const miniKV::benchmark::Endpoint endpoint{"127.0.0.1", server.port()};
    MINIKV_CHECK(request.open(endpoint, "GET", "/first", {}, 0, 1000, error, true));
    MINIKV_CHECK(request.write(nullptr, 0, error));
    MINIKV_CHECK(request.finish(response, error));
    MINIKV_CHECK(error.empty());
    MINIKV_CHECK(response.status == 200);
    MINIKV_CHECK(response.headers.at("Content-Length") == "3");
    MINIKV_CHECK(response.body == "abc");
    miniKV::benchmark::HttpResponse second;
    MINIKV_CHECK(request.open(endpoint, "GET", "/second", {}, 0, 1000, error, true));
    MINIKV_CHECK(request.write(nullptr, 0, error));
    MINIKV_CHECK(request.finish(second, error));
    MINIKV_CHECK(second.status == 200);
    MINIKV_CHECK(second.body == "xyz");
    std::cout << "PASS: benchmark HTTP client reads fragmented keep-alive responses\n";
    return 0;
}
