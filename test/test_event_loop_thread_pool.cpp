#include "network/EventLoop.hpp"
#include "network/EventLoopThreadPool.hpp"
#include "TestCheck.hpp"

int main()
{
    miniKV::network::EventLoop baseLoop;

    miniKV::network::EventLoopThreadPool fallback(&baseLoop);
    fallback.setThreadNum(0);
    fallback.start();
    MINIKV_CHECK(fallback.size() == 0);
    MINIKV_CHECK(fallback.nextLoop() == &baseLoop);

    miniKV::network::EventLoopThreadPool pool(&baseLoop);
    pool.setThreadNum(2);
    pool.start();
    auto* first = pool.nextLoop();
    auto* second = pool.nextLoop();
    auto* third = pool.nextLoop();
    MINIKV_CHECK(pool.size() == 2);
    MINIKV_CHECK(first != &baseLoop);
    MINIKV_CHECK(second != &baseLoop);
    MINIKV_CHECK(first != second);
    MINIKV_CHECK(third == first);
    pool.stop();
    pool.stop();
    return 0;
}
