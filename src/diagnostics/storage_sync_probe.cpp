#include <algorithm>
#include <chrono>
#include <cctype>
#include <cmath>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
    std::string directory;
    std::string label = "unnamed";
    std::string runId = "manual";
    size_t size = 0;
    size_t warmup = 30;
    size_t samples = 300;
    size_t fileBytes = 4U * 1024U * 1024U;
    int64_t startAtUnixMs = 0;
};

uint64_t elapsedUs(Clock::time_point start, Clock::time_point end)
{
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
}

int64_t unixMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

bool parseUnsigned(const char* value, size_t& out)
{
    if(value == nullptr || *value == '\0') return false;
    try {
        const auto parsed = std::stoull(value);
        if(parsed == 0 || parsed > static_cast<unsigned long long>(SIZE_MAX)) return false;
        out = static_cast<size_t>(parsed);
        return true;
    } catch(...) {
        return false;
    }
}

bool parseSigned(const char* value, int64_t& out)
{
    if(value == nullptr || *value == '\0') return false;
    try { out = std::stoll(value); return out > 0; }
    catch(...) { return false; }
}

void usage(const char* program)
{
    std::cerr << "Usage: " << program
              << " --directory DIR --size BYTES [--label NAME] [--run-id ID]"
                 " [--warmup N] [--samples N] [--file-bytes BYTES]"
                 " [--start-at-unix-ms EPOCH_MS]\\n";
}

bool parseArgs(int argc, char** argv, Options& options)
{
    for(int index = 1; index < argc; ++index) {
        const std::string argument(argv[index]);
        auto requireValue = [&](const char* name) -> const char* {
            if(index + 1 >= argc) {
                std::cerr << "missing value for " << name << '\n';
                return nullptr;
            }
            return argv[++index];
        };
        if(argument == "--directory") {
            const char* value = requireValue("--directory"); if(!value) return false;
            options.directory = value;
        } else if(argument == "--label") {
            const char* value = requireValue("--label"); if(!value) return false;
            options.label = value;
        } else if(argument == "--run-id") {
            const char* value = requireValue("--run-id"); if(!value) return false;
            options.runId = value;
        } else if(argument == "--size") {
            const char* value = requireValue("--size"); if(!value || !parseUnsigned(value, options.size)) return false;
        } else if(argument == "--warmup") {
            const char* value = requireValue("--warmup"); if(!value || !parseUnsigned(value, options.warmup)) return false;
        } else if(argument == "--samples") {
            const char* value = requireValue("--samples"); if(!value || !parseUnsigned(value, options.samples)) return false;
        } else if(argument == "--file-bytes") {
            const char* value = requireValue("--file-bytes"); if(!value || !parseUnsigned(value, options.fileBytes)) return false;
        } else if(argument == "--start-at-unix-ms") {
            const char* value = requireValue("--start-at-unix-ms"); if(!value || !parseSigned(value, options.startAtUnixMs)) return false;
        } else if(argument == "--help" || argument == "-h") {
            usage(argv[0]); std::exit(0);
        } else {
            std::cerr << "unknown argument: " << argument << '\n';
            return false;
        }
    }
    if(options.directory.empty() || options.size == 0 || options.fileBytes < options.size) return false;
    options.fileBytes = (options.fileBytes / options.size) * options.size;
    return options.fileBytes >= options.size;
}

uint64_t percentile(std::vector<uint64_t> values, double fraction)
{
    if(values.empty()) return 0;
    std::sort(values.begin(), values.end());
    const size_t index = std::max<size_t>(0, static_cast<size_t>(std::ceil(values.size() * fraction)) - 1U);
    return values[std::min(index, values.size() - 1U)];
}

std::string sanitize(std::string value)
{
    for(char& item : value) {
        if(!(std::isalnum(static_cast<unsigned char>(item)) || item == '-' || item == '_')) item = '_';
    }
    return value.empty() ? "run" : value;
}

bool writeFully(int fd, const std::vector<char>& bytes, off_t offset, int& error)
{
    size_t written = 0;
    while(written < bytes.size()) {
        const ssize_t result = ::pwrite(fd, bytes.data() + written, bytes.size() - written,
                                        offset + static_cast<off_t>(written));
        if(result < 0) {
            if(errno == EINTR) continue;
            error = errno;
            return false;
        }
        if(result == 0) { error = EIO; return false; }
        written += static_cast<size_t>(result);
    }
    return true;
}

}  // namespace

int main(int argc, char** argv)
{
    Options options;
    if(!parseArgs(argc, argv, options)) { usage(argv[0]); return 2; }

    const std::filesystem::path scratch = std::filesystem::path(options.directory) / ".minidriver-sync-probe";
    std::error_code filesystemError;
    std::filesystem::create_directories(scratch, filesystemError);
    if(filesystemError) {
        std::cerr << "cannot create scratch directory: " << filesystemError.message() << '\n';
        return 1;
    }
    const auto filePath = scratch / ("probe-" + sanitize(options.runId) + "-" + std::to_string(::getpid()) + ".bin");
    const int fd = ::open(filePath.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
    if(fd < 0) { std::cerr << "open failed: " << std::strerror(errno) << '\n'; return 1; }
    const auto cleanup = [&] { ::close(fd); std::filesystem::remove(filePath, filesystemError); };
    const int fallocateResult = ::posix_fallocate(fd, 0, static_cast<off_t>(options.fileBytes));
    if(fallocateResult != 0) {
        std::cerr << "posix_fallocate failed: " << std::strerror(fallocateResult) << '\n';
        cleanup(); return 1;
    }

    std::vector<char> bytes(options.size);
    for(size_t index = 0; index < bytes.size(); ++index) bytes[index] = static_cast<char>((index * 131U) & 0xffU);
    if(options.startAtUnixMs > 0) {
        while(unixMs() < options.startAtUnixMs) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const int64_t startedUnixMs = unixMs();
    std::vector<uint64_t> writeValues, syncValues, totalValues;
    writeValues.reserve(options.samples); syncValues.reserve(options.samples); totalValues.reserve(options.samples);
    const size_t slots = options.fileBytes / options.size;

    for(size_t iteration = 0; iteration < options.warmup + options.samples; ++iteration) {
        const off_t offset = static_cast<off_t>((iteration % slots) * options.size);
        int writeError = 0;
        const auto totalStarted = Clock::now();
        const auto writeStarted = totalStarted;
        const bool writeOk = writeFully(fd, bytes, offset, writeError);
        const auto writeFinished = Clock::now();
        int syncError = 0;
        bool syncOk = false;
        const auto syncStarted = Clock::now();
        if(writeOk) {
            syncOk = ::fdatasync(fd) == 0;
            if(!syncOk) syncError = errno;
        }
        const auto finished = Clock::now();
        if(iteration < options.warmup) {
            if(!writeOk || !syncOk) {
                std::cerr << "warmup failed write_errno=" << writeError << " sync_errno=" << syncError << '\n';
                cleanup(); return 1;
            }
            continue;
        }
        const size_t sample = iteration - options.warmup;
        const uint64_t writeUs = elapsedUs(writeStarted, writeFinished);
        const uint64_t syncUs = elapsedUs(syncStarted, finished);
        const uint64_t totalUs = elapsedUs(totalStarted, finished);
        std::cout << "sample,label=" << options.label << ",run_id=" << options.runId
                  << ",sample=" << sample << ",size_bytes=" << options.size
                  << ",offset=" << offset << ",pwrite_us=" << writeUs
                  << ",fdatasync_us=" << syncUs << ",total_us=" << totalUs
                  << ",write_errno=" << writeError << ",sync_errno=" << syncError
                  << ",start_unix_ms=" << startedUnixMs << ",end_unix_ms=" << unixMs() << '\n';
        if(!writeOk || !syncOk) {
            cleanup(); return 1;
        }
        writeValues.push_back(writeUs); syncValues.push_back(syncUs); totalValues.push_back(totalUs);
    }
    const auto mean = [](const std::vector<uint64_t>& values) {
        return values.empty() ? 0ULL : std::accumulate(values.begin(), values.end(), uint64_t{0}) / values.size();
    };
    std::cout << "summary,label=" << options.label << ",run_id=" << options.runId
              << ",samples=" << options.samples << ",size_bytes=" << options.size
              << ",pwrite_p50_us=" << percentile(writeValues, 0.50)
              << ",pwrite_p95_us=" << percentile(writeValues, 0.95)
              << ",pwrite_p99_us=" << percentile(writeValues, 0.99)
              << ",pwrite_mean_us=" << mean(writeValues)
              << ",fdatasync_p50_us=" << percentile(syncValues, 0.50)
              << ",fdatasync_p95_us=" << percentile(syncValues, 0.95)
              << ",fdatasync_p99_us=" << percentile(syncValues, 0.99)
              << ",fdatasync_mean_us=" << mean(syncValues)
              << ",total_p50_us=" << percentile(totalValues, 0.50)
              << ",total_p95_us=" << percentile(totalValues, 0.95)
              << ",total_p99_us=" << percentile(totalValues, 0.99)
              << ",total_mean_us=" << mean(totalValues)
              << ",start_unix_ms=" << startedUnixMs << ",end_unix_ms=" << unixMs() << '\n';
    cleanup();
    return 0;
}
