#include "network/Buffer.hpp"
#include "network/EventLoop.hpp"
#include "network/TcpConnection.hpp"
#include "network/TcpServer.hpp"
#include "TestCheck.hpp"

#include <arpa/inet.h>
#include <chrono>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <set>
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

void runClients(uint16_t port)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::vector<int> clients;
    for(int index = 0; index < 4; ++index) {
        const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        MINIKV_CHECK(fd >= 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);
        MINIKV_CHECK(::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        clients.push_back(fd);
    }
    for(int fd : clients) MINIKV_CHECK(::send(fd, "x", 1, 0) == 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    for(int fd : clients) ::close(fd);
}

}  // namespace

int main()
{
    miniKV::network::EventLoop baseLoop;
    const uint16_t port = reserveLoopbackPort();
    miniKV::network::TcpServer server(&baseLoop, port);
    server.setThreadNum(2);

    std::mutex mutex;
    std::map<int, std::thread::id> connectionThreads;
    std::map<std::thread::id, size_t> connectedPerThread;
    size_t connected = 0;
    size_t messages = 0;
    size_t disconnected = 0;
    bool affinityOk = true;

    server.setConnectionCallback([&](const miniKV::network::TcpConnectionPtr& connection) {
        std::lock_guard<std::mutex> lock(mutex);
        const auto current = std::this_thread::get_id();
        if(connection->connected()) {
            connectionThreads[connection->fd()] = current;
            ++connectedPerThread[current];
            ++connected;
        } else {
            auto found = connectionThreads.find(connection->fd());
            if(found == connectionThreads.end() || found->second != current) affinityOk = false;
            ++disconnected;
            if(disconnected == 4) baseLoop.quit();
        }
    });
    server.setMessageCallback([&](const miniKV::network::TcpConnectionPtr& connection,
                                  miniKV::network::Buffer* buffer) {
        std::lock_guard<std::mutex> lock(mutex);
        auto found = connectionThreads.find(connection->fd());
        if(found == connectionThreads.end() || found->second != std::this_thread::get_id()) {
            affinityOk = false;
        }
        buffer->retrieve(buffer->readableBytes());
        ++messages;
    });

    server.start();
    baseLoop.runAfter(3000, [&] { baseLoop.quit(); });
    std::thread clients([&] { runClients(port); });
    baseLoop.loop();
    clients.join();
    server.stop();

    std::lock_guard<std::mutex> lock(mutex);
    MINIKV_CHECK(connected == 4);
    MINIKV_CHECK(messages == 4);
    MINIKV_CHECK(disconnected == 4);
    MINIKV_CHECK(affinityOk);
    MINIKV_CHECK(connectedPerThread.size() == 2);
    for(const auto& [threadId, count] : connectedPerThread) {
        (void)threadId;
        MINIKV_CHECK(count == 2);
    }
    return 0;
}
