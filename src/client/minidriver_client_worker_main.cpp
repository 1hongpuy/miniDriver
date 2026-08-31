#include "client/MiniDriverClient.hpp"

#include <cctype>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <map>
#include <string>

namespace {

void usage() {
    std::cerr << "usage: minidriver_client_worker read"
              << " --gateway HOST:PORT --cluster-token TOKEN --service-principal ID"
              << " --object-id ID --object-version N --output ABSOLUTE_PATH"
              << " [--range OFFSET:LENGTH]\n";
}

bool parseUint64(const std::string& text, uint64_t& out) {
    if (text.empty()) return false;
    uint64_t value = 0;
    for (const char character : text) {
        if (!std::isdigit(static_cast<unsigned char>(character)) ||
            value > (std::numeric_limits<uint64_t>::max() - static_cast<uint64_t>(character - '0')) / 10) {
            return false;
        }
        value = value * 10 + static_cast<uint64_t>(character - '0');
    }
    out = value;
    return true;
}

bool parseEndpoint(const std::string& text, miniKV::client::Endpoint& endpoint) {
    const size_t colon = text.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 == text.size()) return false;
    uint64_t port = 0;
    if (!parseUint64(text.substr(colon + 1), port) || port == 0 || port > UINT16_MAX) return false;
    endpoint.host = text.substr(0, colon);
    endpoint.port = static_cast<uint16_t>(port);
    return true;
}

const char* integrityName(miniKV::client::IntegrityStatus value) {
    switch (value) {
    case miniKV::client::IntegrityStatus::kVerifiedWholeChunk: return "verified-whole-chunk";
    case miniKV::client::IntegrityStatus::kUnverifiedPartialRange: return "unverified-partial-range";
    case miniKV::client::IntegrityStatus::kUnverifiedChecksumDisabled: return "unverified-checksum-disabled";
    }
    return "unknown";
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2 || std::string(argv[1]) != "read") {
        usage();
        return 2;
    }
    std::map<std::string, std::string> options;
    for (int index = 2; index < argc; index += 2) {
        if (index + 1 >= argc || options.count(argv[index])) {
            usage();
            return 2;
        }
        options.emplace(argv[index], argv[index + 1]);
    }
    const auto get = [&options](const std::string& name) -> std::string {
        const auto it = options.find(name);
        return it == options.end() ? std::string{} : it->second;
    };
    miniKV::client::ClientConfig config;
    config.clusterInternalToken = get("--cluster-token");
    config.servicePrincipal = get("--service-principal");
    miniKV::client::ObjectRef object{get("--object-id"), 0};
    const std::string outputText = get("--output");
    if (!parseEndpoint(get("--gateway"), config.gateway) || config.clusterInternalToken.empty() ||
        config.servicePrincipal.empty() || object.objectId.empty() ||
        !parseUint64(get("--object-version"), object.objectVersion) || object.objectVersion == 0 ||
        outputText.empty() || !std::filesystem::path(outputText).is_absolute()) {
        usage();
        return 2;
    }

    miniKV::client::MiniDriverClient client(std::move(config));
    miniKV::client::ObjectReadPlan plan;
    std::string error;
    if (!client.getReadPlan(object, plan, error)) {
        std::cerr << "read-plan failed: " << error << '\n';
        return 1;
    }
    miniKV::client::ReadOptions readOptions;
    miniKV::client::TransferStats stats;
    const std::filesystem::path output(outputText);
    const std::string range = get("--range");
    if (range.empty()) {
        if (!client.downloadToFile(plan, output, readOptions, stats, error)) {
            std::cerr << "object read failed: " << error << '\n';
            return 1;
        }
        std::cout << "object_id=" << object.objectId << " object_version=" << object.objectVersion
                  << " bytes=" << plan.fileSize << " integrity=verified-whole-chunk"
                  << " replica_fallbacks=" << stats.replicaFallbacks << '\n';
        return 0;
    }
    const size_t colon = range.find(':');
    uint64_t offset = 0;
    uint64_t length = 0;
    if (colon == std::string::npos || !parseUint64(range.substr(0, colon), offset) ||
        !parseUint64(range.substr(colon + 1), length) || length == 0) {
        usage();
        return 2;
    }
    miniKV::client::RangeReadResult result;
    if (!client.downloadRangeToFile(plan, offset, length, output, readOptions, stats, result, error)) {
        std::cerr << "range read failed: " << error << '\n';
        return 1;
    }
    std::cout << "object_id=" << object.objectId << " object_version=" << object.objectVersion
              << " bytes=" << result.bytesRead << " integrity=" << integrityName(result.integrity)
              << " replica_fallbacks=" << stats.replicaFallbacks << '\n';
    return 0;
}
