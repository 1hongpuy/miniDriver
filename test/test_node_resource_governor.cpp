#include "DataNode/NodeResourceGovernor.hpp"

#include <iostream>
#include <optional>
#include <utility>

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
    using miniKV::datanode::NodeResourceGovernor;

    NodeResourceGovernor::Config config;
    config.maxActiveUploads = 3;
    config.maxActiveDownloads = 2;
    config.maxUploadsPerClient = 2;
    config.maxDownloadsPerClient = 1;
    NodeResourceGovernor governor(config);

    auto uploadA1 = governor.tryAcquireUpload("client-a");
    auto uploadA2 = governor.tryAcquireUpload("client-a");
    CHECK(uploadA1.has_value());
    CHECK(uploadA2.has_value());
    CHECK(!governor.tryAcquireUpload("client-a").has_value());

    auto uploadB = governor.tryAcquireUpload("client-b");
    CHECK(uploadB.has_value());
    CHECK(!governor.tryAcquireUpload("client-c").has_value());
    CHECK(governor.snapshot().activeUploads == 3);

    uploadA1.reset();
    CHECK(governor.snapshot().activeUploads == 2);
    auto uploadC = governor.tryAcquireUpload("client-c");
    CHECK(uploadC.has_value());

    auto downloadA = governor.tryAcquireDownload("client-a");
    CHECK(downloadA.has_value());
    CHECK(!governor.tryAcquireDownload("client-a").has_value());
    auto downloadB = governor.tryAcquireDownload("client-b");
    CHECK(downloadB.has_value());
    CHECK(!governor.tryAcquireDownload("client-c").has_value());

    NodeResourceGovernor::DownloadLease moved = std::move(*downloadA);
    downloadA.reset();
    CHECK(governor.snapshot().activeDownloads == 2);
    moved.reset();
    CHECK(governor.snapshot().activeDownloads == 1);
    CHECK(governor.tryAcquireDownload("client-c").has_value());

    std::cout << "PASS: node resource governor enforces global and per-client limits\n";
    return 0;
}
