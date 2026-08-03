#include "utils/AsyncLogger.hpp"

#include <chrono>
#include <filesystem>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>

#include <spdlog/async.h>
#include <spdlog/async_logger.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>

namespace miniKV::utils {
namespace {

std::mutex gLoggerMutex;
std::shared_ptr<spdlog::logger> gLogger;
std::string gProcessName;
std::string gNodeId;

bool parseLevel(const std::string& text, spdlog::level::level_enum& out)
{
    if(text == "trace") out = spdlog::level::trace;
    else if(text == "debug") out = spdlog::level::debug;
    else if(text == "info") out = spdlog::level::info;
    else if(text == "warn") out = spdlog::level::warn;
    else if(text == "error") out = spdlog::level::err;
    else if(text == "critical") out = spdlog::level::critical;
    else if(text == "off") out = spdlog::level::off;
    else return false;
    return true;
}

uint64_t environmentUnsigned(const char* name, uint64_t fallback)
{
    const char* value = std::getenv(name);
    if(value == nullptr || *value == '\0') return fallback;

    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(value, &end, 10);
    if(errno != 0 || end == value || *end != '\0' ||
       parsed > std::numeric_limits<uint64_t>::max()) return fallback;
    return static_cast<uint64_t>(parsed);
}

void write(spdlog::level::level_enum level, const std::string& fields)
{
    std::lock_guard<std::mutex> lock(gLoggerMutex);
    if(!gLogger) return;

    gLogger->log(level,
                 "process={} node={} {}",
                 gProcessName.empty() ? "-" : gProcessName,
                 gNodeId.empty() ? "-" : gNodeId,
                 fields);
}

}  // namespace

bool initAsyncLogger(const AsyncLoggerConfig& config)
{
    if(config.filePath.empty() || config.queueSize == 0 || config.rotateBytes == 0) return false;
    spdlog::level::level_enum level;
    if(!parseLevel(config.level, level)) return false;

    std::lock_guard<std::mutex> lock(gLoggerMutex);
    if(gLogger) return false;

    try {
        const std::filesystem::path path(config.filePath);
        if(path.has_parent_path()) std::filesystem::create_directories(path.parent_path());

        spdlog::init_thread_pool(config.queueSize, 1);
        const auto sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            config.filePath, config.rotateBytes, config.rotateFiles, false);
        gLogger = std::make_shared<spdlog::async_logger>(
            "minikv_v2_async",
            sink,
            spdlog::thread_pool(),
            spdlog::async_overflow_policy::overrun_oldest);
        gLogger->set_pattern("ts=%Y-%m-%dT%H:%M:%S.%e%z level=%l tid=%t %v");
        gLogger->set_level(level);
        spdlog::register_logger(gLogger);
        // Control-plane events can be sparse; make them observable without
        // waiting for process shutdown or an internal stdio buffer to fill.
        spdlog::flush_every(std::chrono::seconds(1));
        gProcessName = config.processName;
        gNodeId = config.nodeId;
        return true;
    } catch(const std::exception&) {
        gLogger.reset();
        spdlog::shutdown();
        return false;
    }
}

void shutdownAsyncLogger()
{
    std::lock_guard<std::mutex> lock(gLoggerMutex);
    if(!gLogger) return;

    gLogger->flush();
    gLogger.reset();
    gProcessName.clear();
    gNodeId.clear();
    spdlog::shutdown();
}

AsyncLoggerConfig asyncLoggerConfigFromEnvironment(std::string processName,
                                                   std::string nodeId,
                                                   std::string defaultFilePath)
{
    AsyncLoggerConfig config;
    config.processName = std::move(processName);
    config.nodeId = std::move(nodeId);
    config.filePath = defaultFilePath;
    if(const char* value = std::getenv("MINIKV_V2_LOG_FILE"); value != nullptr && *value != '\0') {
        config.filePath = value;
    }
    if(const char* value = std::getenv("MINIKV_V2_LOG_LEVEL"); value != nullptr && *value != '\0') {
        config.level = value;
    }
    config.queueSize = static_cast<size_t>(environmentUnsigned(
        "MINIKV_V2_LOG_QUEUE_SIZE", config.queueSize));
    config.rotateBytes = static_cast<size_t>(environmentUnsigned(
        "MINIKV_V2_LOG_ROTATE_BYTES", config.rotateBytes));
    config.rotateFiles = static_cast<size_t>(environmentUnsigned(
        "MINIKV_V2_LOG_ROTATE_FILES", config.rotateFiles));
    return config;
}

void logDebug(const std::string& fields)
{
    write(spdlog::level::debug, fields);
}

void logInfo(const std::string& fields)
{
    write(spdlog::level::info, fields);
}

void logWarn(const std::string& fields)
{
    write(spdlog::level::warn, fields);
}

void logError(const std::string& fields)
{
    write(spdlog::level::err, fields);
}

}  // namespace miniKV::utils
