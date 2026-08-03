#pragma once

#include <cstddef>
#include <string>

namespace miniKV::utils {

struct AsyncLoggerConfig {
    std::string filePath;
    std::string processName;
    std::string nodeId;
    std::string level = "info";
    size_t queueSize = 8192;
    size_t rotateBytes = 20 * 1024 * 1024;
    size_t rotateFiles = 5;
};

bool initAsyncLogger(const AsyncLoggerConfig& config);
void shutdownAsyncLogger();

AsyncLoggerConfig asyncLoggerConfigFromEnvironment(std::string processName,
                                                   std::string nodeId,
                                                   std::string defaultFilePath);

void logDebug(const std::string& fields);
void logInfo(const std::string& fields);
void logWarn(const std::string& fields);
void logError(const std::string& fields);

}  // namespace miniKV::utils
