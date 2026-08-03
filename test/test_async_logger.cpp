#include "TestCheck.hpp"
#include "utils/AsyncLogger.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

int main()
{
    const std::filesystem::path path =
        std::filesystem::temp_directory_path() / "minikv-v2-async-logger-test.log";
    std::error_code error;
    std::filesystem::remove(path, error);

    miniKV::utils::AsyncLoggerConfig config;
    config.filePath = path.string();
    config.processName = "test";
    config.nodeId = "node-test";
    config.level = "info";
    config.queueSize = 64;
    config.rotateBytes = 1024 * 1024;
    config.rotateFiles = 1;

    MINIKV_CHECK(miniKV::utils::initAsyncLogger(config));
    miniKV::utils::logDebug("event=logger_debug_hidden");
    miniKV::utils::logInfo("event=logger_test session=session-1 bytes=42");

    // Gateway control-plane traffic can be sparse. The record must become
    // observable without requiring process shutdown or a full stdio buffer.
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    std::ifstream visibleInput(path);
    const std::string visibleContent((std::istreambuf_iterator<char>(visibleInput)),
                                     std::istreambuf_iterator<char>());
    MINIKV_CHECK(visibleContent.find("event=logger_test") != std::string::npos);

    miniKV::utils::shutdownAsyncLogger();

    MINIKV_CHECK(miniKV::utils::initAsyncLogger(config));
    miniKV::utils::logWarn("event=logger_test_append");
    miniKV::utils::shutdownAsyncLogger();

    std::ifstream input(path);
    const std::string content((std::istreambuf_iterator<char>(input)),
                              std::istreambuf_iterator<char>());
    MINIKV_CHECK(content.find("level=info") != std::string::npos);
    MINIKV_CHECK(content.find("process=test") != std::string::npos);
    MINIKV_CHECK(content.find("node=node-test") != std::string::npos);
    MINIKV_CHECK(content.find("event=logger_test") != std::string::npos);
    MINIKV_CHECK(content.find("session=session-1") != std::string::npos);
    MINIKV_CHECK(content.find("event=logger_test_append") != std::string::npos);
    MINIKV_CHECK(content.find("event=logger_debug_hidden") == std::string::npos);

    std::filesystem::remove(path, error);
    return 0;
}
