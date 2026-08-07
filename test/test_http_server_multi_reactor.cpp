#include "http/DeferredResponse.hpp"
#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "http/HttpServer.hpp"
#include "network/EventLoop.hpp"
#include "TestCheck.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

uint16_t reserveLoopbackPort()
{
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    MINIKV_CHECK(fd >= 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    MINIKV_CHECK(::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
    socklen_t length = sizeof(address);
    MINIKV_CHECK(::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) == 0);
    const uint16_t port = ntohs(address.sin_port);
    ::close(fd);
    return port;
}

std::string request(uint16_t port, const std::string& first, const std::string& second)
{
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    MINIKV_CHECK(fd >= 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    MINIKV_CHECK(::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
    MINIKV_CHECK(::send(fd, first.data(), first.size(), 0) ==
                 static_cast<ssize_t>(first.size()));
    if(!second.empty()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        MINIKV_CHECK(::send(fd, second.data(), second.size(), 0) ==
                     static_cast<ssize_t>(second.size()));
    }

    std::string response;
    char buffer[1024];
    for(;;) {
        const ssize_t bytes = ::recv(fd, buffer, sizeof(buffer), 0);
        if(bytes > 0) response.append(buffer, static_cast<size_t>(bytes));
        else break;
    }
    ::close(fd);
    return response;
}

}  // namespace

int main()
{
    miniKV::network::EventLoop baseLoop;
    const uint16_t port = reserveLoopbackPort();
    miniKV::http::HttpServer server(&baseLoop, nullptr, port);
    server.setThreadNum(2);

    std::mutex mutex;
    std::map<std::string, std::thread::id> callbackThreads;
    std::vector<std::thread> completionThreads;
    server.setHttpCallback([&](const miniKV::http::HttpRequest& request,
                               miniKV::http::HttpResponse* response,
                               const miniKV::network::TcpConnectionPtr&,
                               const miniKV::http::DeferredResponse::Ptr& deferred) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            callbackThreads[request.path()] = std::this_thread::get_id();
        }
        if(request.path() == "/beta") {
            deferred->defer();
            std::lock_guard<std::mutex> lock(mutex);
            completionThreads.emplace_back([deferred] {
                miniKV::http::HttpResponse delayed;
                delayed.setStatusCode(miniKV::http::HttpResponse::k200Ok);
                delayed.setBody("/beta");
                deferred->complete(std::move(delayed));
            });
            return;
        }
        response->setStatusCode(miniKV::http::HttpResponse::k200Ok);
        response->setBody(request.path());
    });

    server.start();
    std::string alphaResponse;
    std::string betaResponse;
    std::thread alpha([&] {
        alphaResponse = request(port,
            "GET /alpha HTTP/1.1\r\nHost: local\r\n",
            "Connection: close\r\n\r\n");
    });
    std::thread beta([&] {
        betaResponse = request(port,
            "GET /beta HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n", "");
    });
    baseLoop.runAfter(3000, [&] { baseLoop.quit(); });
    std::thread waiter([&] {
        alpha.join();
        beta.join();
        baseLoop.quit();
    });
    baseLoop.loop();
    waiter.join();

    std::vector<std::thread> completions;
    {
        std::lock_guard<std::mutex> lock(mutex);
        completions.swap(completionThreads);
    }
    for(auto& thread : completions) thread.join();

    MINIKV_CHECK(alphaResponse.find("200 OK") != std::string::npos);
    MINIKV_CHECK(alphaResponse.find("/alpha") != std::string::npos);
    MINIKV_CHECK(betaResponse.find("200 OK") != std::string::npos);
    MINIKV_CHECK(betaResponse.find("/beta") != std::string::npos);
    {
        std::lock_guard<std::mutex> lock(mutex);
        MINIKV_CHECK(callbackThreads.size() == 2);
        MINIKV_CHECK(callbackThreads.at("/alpha") != callbackThreads.at("/beta"));
        MINIKV_CHECK(callbackThreads.at("/alpha") != std::this_thread::get_id());
        MINIKV_CHECK(callbackThreads.at("/beta") != std::this_thread::get_id());
    }
    return 0;
}
