#include "http/HttpResponse.hpp"
#include "network/TcpConnection.hpp"

#include <iostream>

namespace {

bool check(bool condition, const char* expression, int line)
{
    if(condition) return true;
    std::cerr << "FAIL:" << line << ": " << expression << '\n';
    return false;
}

#define CHECK(expression) \
    do { if(!check((expression), #expression, __LINE__)) return 1; } while(false)

}  // namespace

int main()
{
    miniKV::http::HttpResponse response;
    bool completed = false;
    miniKV::network::SendFileResult observed;
    response.setFileBody("/tmp/data", 17, 4096,
        [&](const miniKV::network::SendFileResult& result) {
        completed = true;
        observed = result;
    });

    CHECK(response.isSendFile());
    CHECK(response.bodyFileOffset() == 17);
    CHECK(response.bodyFileSize() == 4096);
    CHECK(static_cast<bool>(response.fileCompleteCallback()));
    miniKV::network::SendFileResult result;
    result.success = false;
    result.bytesSent = 1234;
    response.fileCompleteCallback()(result);
    CHECK(completed);
    CHECK(!observed.success);
    CHECK(observed.bytesSent == 1234);

    std::cout << "PASS: HTTP file response retains transfer completion callback\n";
    return 0;
}
