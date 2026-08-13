#include "network/EventLoop.hpp"
#include "network/EventLoopThread.hpp"
#include "TestCheck.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

int main()
{
    miniKV::network::EventLoopThread thread;
    auto* loop = thread.startLoop();
    MINIKV_CHECK(loop != nullptr);

    std::mutex mutex;
    std::condition_variable condition;
    bool called = false;
    std::thread::id callbackThread;
    loop->queueInLoop([&] {
        std::lock_guard<std::mutex> lock(mutex);
        called = true;
        callbackThread = std::this_thread::get_id();
        condition.notify_one();
    });

    {
        std::unique_lock<std::mutex> lock(mutex);
        MINIKV_CHECK(condition.wait_for(lock, std::chrono::seconds(2), [&] {
            return called;
        }));
    }

    MINIKV_CHECK(callbackThread != std::this_thread::get_id());
    bool timerCalled = false;
    loop->runAfter(1, [&] {
        std::lock_guard<std::mutex> lock(mutex);
        timerCalled = true;
        condition.notify_one();
    });
    {
        std::unique_lock<std::mutex> lock(mutex);
        MINIKV_CHECK(condition.wait_for(lock, std::chrono::seconds(2), [&] {
            return timerCalled;
        }));
    }
    const auto metrics = loop->metrics();
    MINIKV_CHECK(metrics.crossThreadQueued >= 2);
    MINIKV_CHECK(metrics.pendingFunctorsExecuted >= 2);
    MINIKV_CHECK(metrics.timerCallbacks >= 1);
    MINIKV_CHECK(thread.startLoop() == loop);
    thread.stop();
    thread.stop();
    return 0;
}
