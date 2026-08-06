#include "http/HttpContext.hpp"
#include "TestCheck.hpp"

#include <iostream>
#include <string>

int main()
{
    constexpr size_t kBodyBytes = 128 * 1024;
    miniKV::http::HttpContext context;
    miniKV::network::Buffer buffer;
    const std::string headers =
        "PUT /v2/chunks/test HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Content-Length: 131072\r\n"
        "\r\n";
    const std::string body(kBodyBytes, 'x');
    buffer.append(headers.data(), headers.size());
    buffer.append(body.data(), body.size());

    MINIKV_CHECK(!context.parseRequest(&buffer));
    MINIKV_CHECK(context.headersReady());

    size_t consumed = 0;
    context.setBodyCallback(kBodyBytes, [&consumed](const char*, size_t bytes) {
        consumed += bytes;
        return miniKV::http::HttpContext::BodyConsumeResult::kContinue;
    });

    MINIKV_CHECK(context.parseRequest(&buffer));
    MINIKV_CHECK(context.gotAll());
    MINIKV_CHECK(consumed == kBodyBytes);
    MINIKV_CHECK(buffer.readableBytes() == 0);

    miniKV::http::HttpContext pausedContext;
    miniKV::network::Buffer pausedBuffer;
    pausedBuffer.append(headers.data(), headers.size());
    pausedBuffer.append(body.data(), body.size());
    MINIKV_CHECK(!pausedContext.parseRequest(&pausedBuffer));
    MINIKV_CHECK(pausedContext.headersReady());

    size_t pausedConsumed = 0;
    bool waitForCapacity = true;
    pausedContext.setBodyCallback(kBodyBytes, [&pausedConsumed, &waitForCapacity](const char*, size_t bytes) {
        if(waitForCapacity) {
            waitForCapacity = false;
            return miniKV::http::HttpContext::BodyConsumeResult::kPauseBeforeConsume;
        }
        pausedConsumed += bytes;
        return miniKV::http::HttpContext::BodyConsumeResult::kContinue;
    });

    MINIKV_CHECK(!pausedContext.parseRequest(&pausedBuffer));
    MINIKV_CHECK(pausedContext.bodyPause());
    MINIKV_CHECK(pausedConsumed == 0);
    MINIKV_CHECK(pausedBuffer.readableBytes() == kBodyBytes);

    pausedContext.resumeBody();
    MINIKV_CHECK(pausedContext.parseRequest(&pausedBuffer));
    MINIKV_CHECK(pausedContext.gotAll());
    MINIKV_CHECK(pausedConsumed == kBodyBytes);
    MINIKV_CHECK(pausedBuffer.readableBytes() == 0);

    std::cout << "PASS: streaming HTTP context drains and preserves paused body bytes\n";
    return 0;
}
