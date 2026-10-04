#include "network/EventLoop.hpp"
#include "network/TcpConnection.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {

bool check(bool condition, const char* expression, int line)
{
    if(condition) return true;
    std::fprintf(stderr, "FAIL:%d: %s\n", line, expression);
    return false;
}

#define CHECK(expression) \
    do { if(!check((expression), #expression, __LINE__)) return 1; } while(false)

}  // namespace

int main()
{
    char path[] = "/tmp/minikv_sendfile_offset_XXXXXX";
    const int fileFd = ::mkstemp(path);
    CHECK(fileFd >= 0);

    const std::string fileBytes = "prefix|payload-from-nonzero-offset|suffix";
    const off_t offset = static_cast<off_t>(fileBytes.find("payload"));
    const std::string expected = "payload-from-nonzero-offset";
    CHECK(::write(fileFd, fileBytes.data(), fileBytes.size()) ==
          static_cast<ssize_t>(fileBytes.size()));
    ::close(fileFd);

    std::array<int, 2> sockets{};
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()) == 0);
    const int flags = ::fcntl(sockets[0], F_GETFL, 0);
    CHECK(flags >= 0);
    CHECK(::fcntl(sockets[0], F_SETFL, flags | O_NONBLOCK) == 0);

    miniKV::network::EventLoop loop;
    auto connection = std::make_shared<miniKV::network::TcpConnection>(&loop, sockets[0], 1);
    connection->connectEstablished();
    connection->setSendFileQuantum(5);

    std::atomic<bool> readerFailed{false};
    std::atomic<bool> sendCompleted{false};
    miniKV::network::SendFileResult sendResult;
    std::string received;
    std::thread reader([&] {
        while(received.size() < expected.size()) {
            pollfd descriptor{sockets[1], POLLIN, 0};
            if(::poll(&descriptor, 1, 2000) <= 0) {
                readerFailed = true;
                break;
            }
            char buffer[64];
            const ssize_t count = ::read(sockets[1], buffer, sizeof(buffer));
            if(count <= 0) {
                readerFailed = true;
                break;
            }
            received.append(buffer, static_cast<size_t>(count));
        }
    });

    loop.runAfter(5, [&] {
        connection->startSendFile(path, offset, expected.size(),
            [&](const miniKV::network::SendFileResult& result) {
                sendResult = result;
                sendCompleted = true;
                loop.quit();
            });
    });
    loop.runAfter(2000, [&] {
        readerFailed = true;
        loop.quit();
    });
    loop.loop();
    reader.join();

    CHECK(!readerFailed.load());
    CHECK(sendCompleted.load());
    CHECK(sendResult.success);
    CHECK(sendResult.bytesSent == expected.size());
    CHECK(sendResult.writeCalls >= (expected.size() + 4) / 5);
    CHECK(sendResult.maxBytesPerCall <= 5);
    CHECK(received == expected);

    std::atomic<bool> emptyCompleted{false};
    std::atomic<int> emptyCallbackCount{0};
    miniKV::network::SendFileResult emptyResult;
    loop.runAfter(1, [&] {
        connection->startSendFile(path, offset, 0,
            [&](const miniKV::network::SendFileResult& result) {
                emptyResult = result;
                emptyCompleted = true;
                ++emptyCallbackCount;
                loop.quit();
            });
    });
    loop.runAfter(2000, [&] { loop.quit(); });
    loop.loop();

    CHECK(emptyCompleted.load());
    CHECK(emptyCallbackCount.load() == 1);
    CHECK(emptyResult.success);
    CHECK(emptyResult.bytesSent == 0);
    CHECK(emptyResult.writeCalls == 0);

    const std::string firstSegment = "prefix|";
    const std::string sequenceExpected = firstSegment + expected;
    std::atomic<bool> sequenceFailed{false};
    std::atomic<bool> sequenceCompleted{false};
    miniKV::network::SendFileResult sequenceResult;
    std::string sequenceReceived;
    std::thread sequenceReader([&] {
        while(sequenceReceived.size() < sequenceExpected.size()) {
            pollfd descriptor{sockets[1], POLLIN, 0};
            if(::poll(&descriptor, 1, 2000) <= 0) { sequenceFailed = true; break; }
            char buffer[64];
            const ssize_t count = ::read(sockets[1], buffer, sizeof(buffer));
            if(count <= 0) { sequenceFailed = true; break; }
            sequenceReceived.append(buffer, static_cast<size_t>(count));
        }
    });
    loop.runAfter(1, [&] {
        std::vector<miniKV::network::SendFileSegment> segments;
        segments.push_back({path, 0, firstSegment.size()});
        segments.push_back({path, offset, expected.size()});
        connection->startSendFileSequence(std::move(segments),
            [&](const miniKV::network::SendFileResult& result) {
                sequenceResult = result;
                sequenceCompleted = true;
                loop.quit();
            });
    });
    loop.runAfter(2000, [&] { sequenceFailed = true; loop.quit(); });
    loop.loop();
    sequenceReader.join();
    CHECK(!sequenceFailed.load());
    CHECK(sequenceCompleted.load());
    CHECK(sequenceResult.success);
    CHECK(sequenceResult.bytesSent == sequenceExpected.size());
    CHECK(sequenceReceived == sequenceExpected);

    connection->connectDestroyed();
    connection.reset();
    ::close(sockets[1]);

    std::array<int, 2> disconnectedSockets{};
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0,
                       disconnectedSockets.data()) == 0);
    const int disconnectedFlags = ::fcntl(disconnectedSockets[0], F_GETFL, 0);
    CHECK(disconnectedFlags >= 0);
    CHECK(::fcntl(disconnectedSockets[0], F_SETFL,
                  disconnectedFlags | O_NONBLOCK) == 0);

    auto disconnected = std::make_shared<miniKV::network::TcpConnection>(
        &loop, disconnectedSockets[0], 2);
    disconnected->connectEstablished();
    ::close(disconnectedSockets[1]);

    int disconnectCallbackCount = 0;
    miniKV::network::SendFileResult disconnectResult;
    disconnected->startSendFile(path, offset, expected.size(),
        [&](const miniKV::network::SendFileResult& result) {
            ++disconnectCallbackCount;
            disconnectResult = result;
        });

    CHECK(disconnectCallbackCount == 1);
    CHECK(!disconnectResult.success);
    disconnected->connectDestroyed();
    disconnected.reset();
    ::unlink(path);

    std::puts("PASS: sendfile preserves non-zero file offsets");
    return 0;
}
