#include "DataNode/ReplicaConnectionPool.hpp"
#include "network/EventLoop.hpp"

#include <cstdio>

int main()
{
    miniKV::network::EventLoop loop;
    auto pool = miniKV::datanode::ReplicaConnectionPool::create(
        &loop, {.maxSessionsPerTarget = 1, .maxSessions = 1});
    const miniKV::datanode::ReplicaPoolKey key{"node-b", "127.0.0.1", 9002};
    miniKV::http::PersistentHttpSession::Ptr first;
    miniKV::http::PersistentHttpSession::Ptr second;
    bool rejected = false;
    pool->borrow(key, [&](auto session, std::string error) { first = std::move(session); rejected = !error.empty(); });
    pool->borrow(key, [&](auto, std::string error) { rejected = rejected || !error.empty(); });
    pool->release(first);
    pool->borrow(key, [&](auto session, std::string error) { if(error.empty()) second = std::move(session); });
    pool->discard(second);
    pool->shutdown();
    const auto metrics = pool->metrics();
    if(!first || first != second || !rejected || metrics.created != 1 || metrics.reused != 1 ||
       metrics.rejected != 1 || metrics.discarded < 1) {
        std::fprintf(stderr, "FAIL: pool limits/reuse metrics were incorrect\n");
        return 1;
    }
    std::puts("PASS: replica connection pool enforces capacity and reuses an idle session");
    return 0;
}
