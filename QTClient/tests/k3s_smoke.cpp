#include "qtclient/MiniDriverStorageAdapter.hpp"

#include <QCoreApplication>
#include <QFile>
#include <QTemporaryDir>
#include <QThread>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

struct SmokeResult {
    bool ok = false;
    std::string error;
    miniKV::client::ObjectRef object;
    uint64_t uploadBytes = 0;
    uint64_t downloadBytes = 0;
};

std::string envOr(const char* name, const char* fallback = "")
{
    const char* value = std::getenv(name);
    return value == nullptr ? std::string(fallback) : std::string(value);
}

bool writeFixture(const std::string& path, size_t bytes, std::string& error)
{
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if(!output) {
        error = "cannot create fixture: " + path;
        return false;
    }

    std::vector<char> data(bytes);
    for(size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<char>((i * 131U + 17U) & 0xffU);
    }
    output.write(data.data(), static_cast<std::streamsize>(data.size()));
    if(!output) {
        error = "cannot write fixture: " + path;
        return false;
    }
    return true;
}

bool sameBytes(const std::string& left, const std::string& right, std::string& error)
{
    std::ifstream lhs(left, std::ios::binary);
    std::ifstream rhs(right, std::ios::binary);
    if(!lhs || !rhs) {
        error = "cannot open upload/download result for comparison";
        return false;
    }

    constexpr size_t kBufferBytes = 64 * 1024;
    std::vector<char> leftBuffer(kBufferBytes);
    std::vector<char> rightBuffer(kBufferBytes);
    for(;;) {
        lhs.read(leftBuffer.data(), static_cast<std::streamsize>(leftBuffer.size()));
        rhs.read(rightBuffer.data(), static_cast<std::streamsize>(rightBuffer.size()));
        const std::streamsize leftCount = lhs.gcount();
        const std::streamsize rightCount = rhs.gcount();
        if(leftCount != rightCount ||
           !std::equal(leftBuffer.begin(), leftBuffer.begin() + leftCount,
                       rightBuffer.begin())) {
            error = "downloaded bytes differ from uploaded fixture";
            return false;
        }
        if(leftCount == 0) return true;
    }
}

}  // namespace

int main(int argc, char** argv)
{
    QCoreApplication app(argc, argv);

    // The current SDK selects /api/v2/upload/preflight versus the legacy
    // session endpoint from this process environment.  This executable is
    // specifically a K3s/Raft smoke test, so make that selection explicit
    // instead of allowing a stale shell environment to silently use legacy.
    ::setenv("MINIKV_METADATA_MODE", "raft", 1);

    const std::string host = envOr("MINIDRIVER_QT_GATEWAY_HOST", "192.168.137.10");
    const std::string portText = envOr("MINIDRIVER_QT_GATEWAY_PORT", "30280");
    const std::string token = envOr("MINIDRIVER_QT_CLUSTER_TOKEN");
    const std::string principal = envOr("MINIDRIVER_QT_PRINCIPAL", "qt-k3s-smoke");
    if(token.empty()) {
        std::cerr << "MINIDRIVER_QT_CLUSTER_TOKEN is required\n";
        return 2;
    }

    uint16_t port = 0;
    try {
        const unsigned long parsed = std::stoul(portText);
        if(parsed == 0 || parsed > 65535) throw std::out_of_range("port");
        port = static_cast<uint16_t>(parsed);
    } catch(const std::exception&) {
        std::cerr << "invalid MINIDRIVER_QT_GATEWAY_PORT: " << portText << "\n";
        return 2;
    }

    QTemporaryDir tempDir;
    if(!tempDir.isValid()) {
        std::cerr << "cannot create temporary directory\n";
        return 2;
    }

    const std::string input = (tempDir.path() + "/upload-" +
                               QString::number(QCoreApplication::applicationPid()) +
                               ".bin").toStdString();
    const std::string output = (tempDir.path() + "/download.bin").toStdString();
    SmokeResult result;
    if(!writeFixture(input, 64 * 1024, result.error)) {
        std::cerr << result.error << "\n";
        return 2;
    }

    miniKV::client::ClientConfig config;
    config.gateway = {host, port};
    config.clusterInternalToken = token;
    config.servicePrincipal = principal;
    config.gatewayTimeoutMs = 30000;
    config.dataNodeTimeoutMs = 60000;

    QThread* worker = QThread::create([&result, &config, &input, &output] {
        miniKV::qtclient::MiniDriverStorageAdapter adapter(config);
        miniKV::client::ObjectRef object;

        const bool uploaded = adapter.upload(
            input,
            [&result](uint64_t completed, uint64_t) {
                result.uploadBytes = completed;
            },
            object,
            result.error);
        if(!uploaded) return;

        result.object = object;
        const bool downloaded = adapter.download(
            object,
            output,
            [&result](uint64_t completed, uint64_t) {
                result.downloadBytes = completed;
            },
            result.error);
        if(!downloaded) return;

        if(!sameBytes(input, output, result.error)) return;
        result.ok = true;
    });

    QObject::connect(worker, &QThread::finished, &app, &QCoreApplication::quit);
    worker->start();
    app.exec();
    worker->wait();
    delete worker;

    if(!result.ok) {
        std::cerr << "qt k3s smoke failed: "
                  << (result.error.empty() ? "unknown error" : result.error) << "\n";
        return 1;
    }

    std::cout << "qt k3s smoke passed\n"
              << "gateway=" << host << ":" << port << "\n"
              << "upload_bytes=" << result.uploadBytes << "\n"
              << "download_bytes=" << result.downloadBytes << "\n"
              << "object_id=" << result.object.objectId << "\n"
              << "object_version=" << result.object.objectVersion << "\n";
    return 0;
}
