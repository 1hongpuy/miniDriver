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

    std::atomic<bool> readerFailed{false};
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
        loop.quit();
    });

    loop.runAfter(5, [&] { connection->startSendFile(path, offset, expected.size()); });
    loop.loop();
    reader.join();

    connection->connectDestroyed();
    connection.reset();
    ::close(sockets[1]);
    ::unlink(path);

    CHECK(!readerFailed.load());
    CHECK(received == expected);
    std::puts("PASS: sendfile preserves non-zero file offsets");
    return 0;
}
