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
            char buffer[1024];
            ::recv(peer, buffer, sizeof(buffer), 0);
            const char first[] = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nX-Test: yes\r\n\r\n";
            MINIKV_CHECK(::send(peer, first, sizeof(first) - 1, MSG_NOSIGNAL) > 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            MINIKV_CHECK(::send(peer, "abc", 3, MSG_NOSIGNAL) == 3);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
    miniKV::benchmark::HttpResponse response;
    std::string error;
    const bool ok = miniKV::benchmark::httpRequest(
        {"127.0.0.1", server.port()}, "GET", "/test", {}, "", 1000, response, error);
    MINIKV_CHECK(ok);
    MINIKV_CHECK(error.empty());
    MINIKV_CHECK(response.status == 200);
    MINIKV_CHECK(response.headers.at("Content-Length") == "3");
    MINIKV_CHECK(response.body == "abc");
    std::cout << "PASS: benchmark HTTP client reads Content-Length response\n";
    return 0;
}
