#include "network/EventLoop.hpp"
#include "network/EventLoopThread.hpp"
#include "network/TcpConnection.hpp"
#include "TestCheck.hpp"

#include <any>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

int main()
{
    int sockets[2] = {-1, -1};
    MINIKV_CHECK(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                              0, sockets) == 0);

    miniKV::network::EventLoopThread loopThread;
    auto* loop = loopThread.startLoop();
    std::mutex mutex;
    std::condition_variable condition;
    bool completed = false;
    bool ownerMatched = false;
    bool contextMatched = false;
    bool contextCleared = false;
    std::thread::id callbackThread;

    loop->queueInLoop([&] {
        auto connection = std::make_shared<miniKV::network::TcpConnection>(
            loop, sockets[0], 1);
        connection->connectEstablished();
        connection->setContext(std::string("connection-local"));
        ownerMatched = connection->ownerLoop() == loop && loop->isInLoopThread();
        contextMatched =
            std::any_cast<std::string>(connection->context()) == "connection-local";
        callbackThread = std::this_thread::get_id();
        connection->connectDestroyed();
        contextCleared = !connection->context().has_value();
        connection.reset();

        std::lock_guard<std::mutex> lock(mutex);
        completed = true;
        condition.notify_one();
    });

    {
        std::unique_lock<std::mutex> lock(mutex);
        MINIKV_CHECK(condition.wait_for(lock, std::chrono::seconds(2), [&] {
            return completed;
        }));
    }

    MINIKV_CHECK(ownerMatched);
    MINIKV_CHECK(contextMatched);
    MINIKV_CHECK(contextCleared);
    MINIKV_CHECK(callbackThread != std::this_thread::get_id());

    ::close(sockets[1]);
    loopThread.stop();
    return 0;
}
