#include "benchmark/BenchmarkCli.hpp"

#include <cctype>
#include <limits>

namespace miniKV::benchmark {
namespace {

bool parseRuns(const std::string& text, uint32_t& runs) {
    if (text.empty()) return false;
    uint64_t value = 0;
    for (const char c : text) {
        if (!std::isdigit(static_cast<unsigned char>(c)) ||
            value > (std::numeric_limits<uint32_t>::max() - static_cast<uint64_t>(c - '0')) / 10) return false;
        value = value * 10 + static_cast<uint64_t>(c - '0');
    }
    if (value == 0) return false;
    runs = static_cast<uint32_t>(value);
    return true;
}

bool parseSizes(const std::string& text, std::vector<uint64_t>& sizes) {
    sizes.clear();
    size_t start = 0;
    while (start < text.size()) {
        const size_t comma = text.find(',', start);
        const std::string value = text.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        uint64_t bytes = 0;
        if (!parseSize(value, bytes)) return false;
        sizes.push_back(bytes);
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    return !sizes.empty();
}

}

std::string benchmarkUsage() {
    return "usage: minikv_v2_bench local --gateway HOST:PORT --work-dir ABSOLUTE_PATH "
           "[--sizes 64KiB,4MiB,32MiB,256MiB,1GiB] [--runs 3] [--concurrency 1] [--chunk-window 1|2] [--global-chunk-budget 2] [--upload-checksum crc32c|sha256] [--fixture-settle-ms 0] "
           "[--mode end-to-end|upload|download|mixed] [--read-profile independent|hot-object|mixed-size] "
           "[--connection-mode close|keep-alive] [--requests-per-worker 1] "
           "[--download-verification strict|transport-only] "
           "[--slow-reader-workers 0] [--slow-reader-bytes-per-sec 1MiB] "
           "[--sdk-read-plan true|false --cluster-internal-token TOKEN --service-principal ID] "
           "[--remote-dir /benchmark]";
}

bool parseBenchmarkOptions(const std::vector<std::string>& args,
                           BenchmarkOptions& options, std::string& error) {
    options = {};
    options.sizes = {4ULL * 1024ULL * 1024ULL, 32ULL * 1024ULL * 1024ULL,
                     256ULL * 1024ULL * 1024ULL, 1024ULL * 1024ULL * 1024ULL};
    options.runs = 3;
    options.concurrency = 1;
    options.chunkWindow = 1;
    options.globalChunkBudget = 2;
    options.uploadChecksumType = "crc32c";
    options.fixtureSettleMs = 0;
    options.keepAlive = false;
    options.requestsPerWorker = 1;
    options.slowReaderWorkers = 0;
    options.slowReaderBytesPerSecond = 0;
    options.readProfile = BenchmarkReadProfile::kIndependent;
    options.remoteDir = "/benchmark";
    if (args.empty() || args.front() != "local") { error = "only the local command is supported"; return false; }
    bool gatewaySet = false;
    bool workDirSet = false;
    for (size_t index = 1; index < args.size(); index += 2) {
        if (index + 1 >= args.size()) { error = "missing value for " + args[index]; return false; }
        const std::string& flag = args[index];
        const std::string& value = args[index + 1];
        if (flag == "--gateway") {
            if (gatewaySet || !parseEndpoint(value, options.gateway)) { error = "invalid --gateway HOST:PORT"; return false; }
            gatewaySet = true;
        } else if (flag == "--work-dir") {
            if (workDirSet || value.empty()) { error = "invalid --work-dir"; return false; }
            options.workDir = value;
            if (!options.workDir.is_absolute()) { error = "--work-dir must be an absolute path"; return false; }
            workDirSet = true;
        } else if (flag == "--sizes") {
            if (!parseSizes(value, options.sizes)) { error = "invalid --sizes list"; return false; }
        } else if (flag == "--runs") {
            if (!parseRuns(value, options.runs)) { error = "invalid --runs count"; return false; }
        } else if (flag == "--concurrency") {
            if (!parseRuns(value, options.concurrency)) { error = "invalid --concurrency count"; return false; }
        } else if (flag == "--chunk-window") {
            if (!parseRuns(value, options.chunkWindow) || options.chunkWindow > 2) {
                error = "--chunk-window must be 1 or 2"; return false;
            }
        } else if (flag == "--global-chunk-budget") {
            if (!parseRuns(value, options.globalChunkBudget)) {
                error = "invalid --global-chunk-budget"; return false;
            }
        } else if (flag == "--upload-checksum") {
            if (value != "crc32c" && value != "sha256") {
                error = "--upload-checksum must be crc32c or sha256";
                return false;
            }
            options.uploadChecksumType = value;
        } else if (flag == "--fixture-settle-ms") {
            if (!parseRuns(value, options.fixtureSettleMs) || options.fixtureSettleMs > 60000) {
                error = "--fixture-settle-ms must be 1..60000"; return false;
            }
        } else if (flag == "--mode") {
            if(value == "end-to-end") options.mode = BenchmarkMode::kEndToEnd;
            else if(value == "upload") options.mode = BenchmarkMode::kUploadOnly;
            else if(value == "download") options.mode = BenchmarkMode::kDownloadOnly;
            else if(value == "mixed") options.mode = BenchmarkMode::kMixed;
            else { error = "invalid --mode"; return false; }
        } else if (flag == "--read-profile") {
            if(value == "independent") options.readProfile = BenchmarkReadProfile::kIndependent;
            else if(value == "hot-object") options.readProfile = BenchmarkReadProfile::kHotObject;
            else if(value == "mixed-size") options.readProfile = BenchmarkReadProfile::kMixedSize;
            else { error = "--read-profile must be independent, hot-object, or mixed-size"; return false; }
        } else if(flag == "--connection-mode") {
            if(value == "close") options.keepAlive = false;
            else if(value == "keep-alive") options.keepAlive = true;
            else { error = "--connection-mode must be close or keep-alive"; return false; }
        } else if(flag == "--requests-per-worker") {
            if(!parseRuns(value, options.requestsPerWorker) || options.requestsPerWorker > 100000) {
                error = "--requests-per-worker must be 1..100000"; return false;
            }
        } else if(flag == "--download-verification") {
            if(value == "strict") options.transportOnlyDownload = false;
            else if(value == "transport-only") options.transportOnlyDownload = true;
            else { error = "--download-verification must be strict or transport-only"; return false; }
        } else if(flag == "--slow-reader-workers") {
            if(!parseRuns(value, options.slowReaderWorkers)) {
                error = "--slow-reader-workers must be positive"; return false;
            }
        } else if(flag == "--slow-reader-bytes-per-sec") {
            if(!parseSize(value, options.slowReaderBytesPerSecond)) {
                error = "--slow-reader-bytes-per-sec must use KiB, MiB, or GiB"; return false;
            }
        } else if (flag == "--sdk-read-plan") {
            if (value == "true") options.sdkReadPlan = true;
            else if (value == "false") options.sdkReadPlan = false;
            else { error = "--sdk-read-plan must be true or false"; return false; }
        } else if (flag == "--cluster-internal-token") {
            if (value.empty()) { error = "invalid --cluster-internal-token"; return false; }
            options.clusterInternalToken = value;
        } else if (flag == "--service-principal") {
            if (value.empty()) { error = "invalid --service-principal"; return false; }
            options.servicePrincipal = value;
        } else if (flag == "--remote-dir") {
            if (value.empty()) { error = "invalid --remote-dir"; return false; }
            options.remoteDir = value.front() == '/' ? value : "/" + value;
        } else {
            error = "unknown option " + flag;
            return false;
        }
    }
    if (!gatewaySet || !workDirSet) { error = "--gateway and --work-dir are required"; return false; }
    if (options.mode == BenchmarkMode::kMixed && options.concurrency < 2) {
        error = "--mode mixed requires --concurrency >= 2";
        return false;
    }
    if ((options.readProfile == BenchmarkReadProfile::kHotObject ||
         options.readProfile == BenchmarkReadProfile::kMixedSize) &&
        options.mode != BenchmarkMode::kDownloadOnly) {
        error = "this --read-profile requires --mode download";
        return false;
    }
    if (options.slowReaderWorkers > 0) {
        if (options.mode != BenchmarkMode::kDownloadOnly ||
            options.slowReaderWorkers >= options.concurrency ||
            options.slowReaderBytesPerSecond == 0) {
            error = "slow readers require download mode, 1..concurrency-1 workers, and a byte rate";
            return false;
        }
    } else if (options.slowReaderBytesPerSecond != 0) {
        error = "--slow-reader-bytes-per-sec requires --slow-reader-workers";
        return false;
    }
    if (options.sdkReadPlan && (options.clusterInternalToken.empty() || options.servicePrincipal.empty())) {
        error = "--sdk-read-plan requires --cluster-internal-token and --service-principal";
        return false;
    }
    return true;
}

}  // namespace miniKV::benchmark
