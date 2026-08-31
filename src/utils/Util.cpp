#include "utils/Util.hpp"

#include <cstddef>
#include <openssl/crypto.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/rand.h>

#include <array>
#include <chrono>
#include <cctype>
#include <iomanip>
#include <sstream>
#include <string>
#include <limits>
#include <vector>

namespace miniKV {
namespace util {

constexpr char kHexDigits[] = "0123456789abcdef";
    
int hexNibble(char value) {
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    if (value >= 'A' && value <= 'F') return value - 'A' + 10;
    return -1;
}

//system tool
//getTime
int64_t unixSeconds() {
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
//计算hash
std::string sha256Hex(const char* data, size_t size)
{
    unsigned char digestp[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data), size, digestp);
    std::string out;
    out.reserve(SHA256_DIGEST_LENGTH * 2);
    for(unsigned char value : digestp)
    {
        out += kHexDigits[(value >> 4) & 0x0f];
        out += kHexDigits[value & 0x0f];
    }
    return out;
}

std::string randomId()
{
    //每个线程都有这个独立的版本
    std::array<unsigned char, 16> bytes{};
    if(RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) != 1) return {};
    std::string out;
    out.reserve(bytes.size() * 2);
    for(const unsigned char value : bytes)
    {
        out += kHexDigits[(value >> 4) & 0x0f];
        out += kHexDigits[value & 0x0f];
    }
    return out;
}

std::string jsonEscape(const std::string& value) {
    std::string out;
    out.reserve(value.size());
    for (const unsigned char c : value) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    out += "\\u00";
                    out += kHexDigits[(c >> 4) & 0x0f];
                    out += kHexDigits[c & 0x0f];
                } else {
                    out += static_cast<char>(c);
                }
        }
    }
    return out;
}

std::string jsonString(const std::string& json, const std::string& key) {
    const std::string marker = "\"" + key + "\"";
    size_t pos = json.find(marker);
    if (pos == std::string::npos || (pos = json.find(':', pos + marker.size())) == std::string::npos) return {};
    pos = json.find('"', pos + 1);
    if (pos == std::string::npos) return {};
    std::string out;
    for (++pos; pos < json.size(); ++pos) {
        if (json[pos] == '"') break;
        if (json[pos] == '\\' && pos + 1 < json.size()) {
            ++pos;
            out += json[pos] == 'n' ? '\n' : json[pos];
        } else out += json[pos];
    }
    return out;
}

uint64_t jsonUint(const std::string& json, const std::string& key, uint64_t fallback) {
    const std::string marker = "\"" + key + "\"";
    size_t pos = json.find(marker);
    if (pos == std::string::npos || (pos = json.find(':', pos + marker.size())) == std::string::npos) return fallback;
    while (++pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos]))) {}
    uint64_t value = 0;
    bool found = false;
    while (pos < json.size() && std::isdigit(static_cast<unsigned char>(json[pos]))) {
        const uint64_t digit = static_cast<uint64_t>(json[pos++] - '0');
        if (value > (std::numeric_limits<uint64_t>::max() - digit) / 10) return fallback;
        value = value * 10 + digit;
        found = true;
    }
    return found ? value : fallback;
}

std::vector<uint32_t> jsonUIntArray(const std::string& json, const std::string& key) {
    std::vector<uint32_t> values;
    const std::string marker = "\"" + key + "\"";
    size_t pos = json.find(marker);
    if (pos == std::string::npos || (pos = json.find('[', pos + marker.size())) == std::string::npos) return values;
    ++pos;
    while (pos < json.size() && json[pos] != ']') {
        while (pos < json.size() && !std::isdigit(static_cast<unsigned char>(json[pos])) && json[pos] != ']') ++pos;
        uint64_t value = 0; bool found = false;
        while (pos < json.size() && std::isdigit(static_cast<unsigned char>(json[pos]))) {
            const uint64_t digit = static_cast<uint64_t>(json[pos++] - '0');
            if (value > (std::numeric_limits<uint64_t>::max() - digit) / 10) return {};
            value = value * 10 + digit;
            found = true;
        }
        if (found && value <= UINT32_MAX) values.push_back(static_cast<uint32_t>(value));
    }
    return values;
}

std::vector<std::string> jsonObjectArray(const std::string& json, const std::string& key) {
    std::vector<std::string> objects;
    const std::string marker = "\"" + key + "\"";
    size_t pos = json.find(marker);
    if (pos == std::string::npos || (pos = json.find('[', pos + marker.size())) == std::string::npos) return objects;

    bool inString = false;
    bool escaped = false;
    int objectDepth = 0;
    size_t objectStart = std::string::npos;
    for (++pos; pos < json.size(); ++pos) {
        const char c = json[pos];
        if (inString) {
            if (escaped) escaped = false;
            else if (c == '\\') escaped = true;
            else if (c == '"') inString = false;
            continue;
        }
        if (c == '"') { inString = true; continue; }
        if (c == '{') {
            if (objectDepth++ == 0) objectStart = pos;
        } else if (c == '}' && objectDepth > 0) {
            if (--objectDepth == 0 && objectStart != std::string::npos) {
                objects.push_back(json.substr(objectStart, pos - objectStart + 1));
                objectStart = std::string::npos;
            }
        } else if (c == ']' && objectDepth == 0) {
            break;
        }
    }
    return objects;
}

std::string jsonError(const std::string& message) { return "{\"error\":\"" + jsonEscape(message) + "\"}"; }

std::string urlDecode(const std::string& value) {
    std::string out;
    out.reserve(value.size());
    for (size_t i = 0; i < value.size(); ++i) {
        if (value[i] == '+' ) out += ' ';
        else if (value[i] == '%' && i + 2 < value.size()) {
            const int high = hexNibble(value[i + 1]);
            const int low = hexNibble(value[i + 2]);
            if (high < 0 || low < 0) {
                out += value[i];
            } else {
                out += static_cast<char>((high << 4) | low);
                i += 2;
            }
        } else out += value[i];
    }
    return out;
}

std::map<std::string, std::string> parseForm(const std::string& body) {
    std::map<std::string, std::string> out;
    for (const auto& item : split(body, '&')) {
        const size_t equal = item.find('=');
        if (equal != std::string::npos) out[urlDecode(item.substr(0, equal))] = urlDecode(item.substr(equal + 1));
    }
    return out;
}

std::string getQueryValue(const std::string& query, const std::string& key) {
    const auto values = parseForm(query);
    const auto it = values.find(key);
    return it == values.end() ? "" : it->second;
}

std::string hexEncode(const std::string& value) {
    std::string out;
    out.reserve(value.size() * 2);
    for (const unsigned char c : value) {
        out += kHexDigits[(c >> 4) & 0x0f];
        out += kHexDigits[c & 0x0f];
    }
    return out;
}

bool hexDecode(const std::string& value, std::string& out) {
    if (value.size() % 2 != 0) return false;
    out.clear(); out.reserve(value.size() / 2);
    for (size_t i = 0; i < value.size(); i += 2) {
        const int high = hexNibble(value[i]);
        const int low = hexNibble(value[i + 1]);
        if (high < 0 || low < 0) {
            out.clear();
            return false;
        }
        out += static_cast<char>((high << 4) | low);
    }
    return true;
}

//根据符号，分割字符串
std::vector<std::string> split(const std::string& value, char separator) {
    std::vector<std::string> out;
    size_t begin = 0;

    while(begin <= value.size())
    {
        size_t end = value.find(separator, begin);
        out.push_back(value.substr(begin, end-begin));
        if(end == std::string::npos) break;
        begin = end + 1;
    }
    return out;
}

std::string join(const std::vector<std::string> &values, char separator)
{
    std::ostringstream out;
    for(size_t i = 0; i < values.size(); i++)
    {
        if(i != 0) out << separator;
        out << values[i];
    }
    return out.str();
}

//token 校验
bool constantTimeEquals(const std::string& left, const std::string& right)
{
    return left.size() == right.size() &&
        CRYPTO_memcmp(left.data(), right.data(), left.size()) == 0;
}

//clusterSecret共享密匙
std::string issueUploadCapability(const UploadCapability& capability,
                                  const std::string& clusterSecret) {
    if (clusterSecret.empty() || capability.sessionId.empty() ||
        capability.chunkHash.empty() || capability.chunkSize == 0 ||
        capability.chainTargets.empty() || capability.leaseId.empty()) {
        return {};
    }
    std::vector<std::string> targets;
    for (const auto& target : capability.chainTargets) targets.push_back(hexEncode(target));
    std::string payload;
    if(capability.schemaVersion >= 2) {
        const std::string chunkId = capability.chunkId.empty()
            ? capability.chunkHash : capability.chunkId;
        const std::string checksumDigest = capability.checksumDigest.empty()
            ? capability.chunkHash : capability.checksumDigest;
        if(chunkId.empty() || checksumDigest.empty() || capability.identityScheme.empty() ||
           capability.checksumType.empty() || capability.objectVersion == 0) return {};
        payload = "v2|" + hexEncode(capability.sessionId) + "|" +
            std::to_string(capability.chunkIndex) + "|" + hexEncode(capability.chunkHash) + "|" +
            std::to_string(capability.chunkSize) + "|" + join(targets, ',') + "|" +
            hexEncode(capability.leaseId) + "|" + std::to_string(capability.expiresAt) + "|" +
            hexEncode(capability.identityScheme) + "|" + hexEncode(chunkId) + "|" +
            hexEncode(capability.objectId) + "|" + std::to_string(capability.objectVersion) + "|" +
            std::to_string(capability.generation) + "|" + hexEncode(capability.checksumType) + "|" +
            std::to_string(capability.checksumSegmentBytes) + "|" + hexEncode(checksumDigest);
    } else {
        payload = "v1|" + hexEncode(capability.sessionId) + "|" +
            std::to_string(capability.chunkIndex) + "|" + hexEncode(capability.chunkHash) + "|" +
            std::to_string(capability.chunkSize) + "|" + join(targets, ',') + "|" +
            hexEncode(capability.leaseId) + "|" + std::to_string(capability.expiresAt);
    }
    unsigned char signature[EVP_MAX_MD_SIZE];
    unsigned int signatureSize = 0;
    if (HMAC(EVP_sha256(), clusterSecret.data(), static_cast<int>(clusterSecret.size()),
             reinterpret_cast<const unsigned char*>(payload.data()), payload.size(),
             signature, &signatureSize) == nullptr) {
        return {};
    }
    return hexEncode(payload) + "." +
        hexEncode(std::string(reinterpret_cast<const char*>(signature), signatureSize));
}

bool verifyUploadCapability(const std::string& token,
                            const std::string& clusterSecret,
                            UploadCapability& out) {
    out = {};
    const size_t splitAt = token.find('.');
    if (splitAt == std::string::npos || splitAt == 0 ||
        splitAt + 1 >= token.size() || token.size() > 8192 || clusterSecret.empty()) {
        return false;
    }
    std::string payload;
    if (!hexDecode(token.substr(0, splitAt), payload)) return false;
    std::string signature;
    if (!hexDecode(token.substr(splitAt + 1), signature)) return false;
    unsigned char expected[EVP_MAX_MD_SIZE];
    unsigned int expectedSize = 0;
    if (HMAC(EVP_sha256(), clusterSecret.data(), static_cast<int>(clusterSecret.size()),
             reinterpret_cast<const unsigned char*>(payload.data()), payload.size(),
             expected, &expectedSize) == nullptr ||
        signature.size() != expectedSize ||
        !constantTimeEquals(signature,
                            std::string(reinterpret_cast<const char*>(expected), expectedSize))) {
        return false;
    }
    //获取通信证
    const auto fields = split(payload, '|');
    const bool v1 = fields.size() == 8 && fields[0] == "v1";
    const bool v2 = fields.size() == 16 && fields[0] == "v2";
    if(!v1 && !v2) return false;
    try {
        if (!hexDecode(fields[1], out.sessionId) || !hexDecode(fields[3], out.chunkHash) ||
            !hexDecode(fields[6], out.leaseId)) {
            return false;
        }
        const unsigned long chunkIndex = std::stoul(fields[2]);
        if(chunkIndex > UINT32_MAX) return false;
        out.chunkIndex = static_cast<uint32_t>(chunkIndex);
        out.chunkSize = std::stoull(fields[4]);
        out.expiresAt = std::stoll(fields[7]);
        for (const auto& item : split(fields[5], ',')) {
            if (item.empty()) continue;
            std::string target;
            if (!hexDecode(item, target)) return false;
            out.chainTargets.push_back(std::move(target));
        }
        if(v2) {
            out.schemaVersion = 2;
            if(!hexDecode(fields[8], out.identityScheme) ||
               !hexDecode(fields[9], out.chunkId) ||
               !hexDecode(fields[10], out.objectId) ||
               !hexDecode(fields[13], out.checksumType) ||
               !hexDecode(fields[15], out.checksumDigest)) return false;
            out.objectVersion = std::stoull(fields[11]);
            out.generation = std::stoull(fields[12]);
            const unsigned long segmentBytes = std::stoul(fields[14]);
            if(segmentBytes > UINT32_MAX) return false;
            out.checksumSegmentBytes = static_cast<uint32_t>(segmentBytes);
        } else {
            out.schemaVersion = 1;
            out.identityScheme = "cas-sha256";
            out.chunkId = out.chunkHash;
            out.objectVersion = 1;
            out.checksumType = "sha256";
            out.checksumDigest = out.chunkHash;
        }
    } catch (...) {
        return false;
    }
    return !out.sessionId.empty() && !out.chunkHash.empty() && out.chunkSize > 0 &&
           !out.chainTargets.empty() && !out.leaseId.empty() &&
           !out.identityScheme.empty() && !out.chunkId.empty() &&
           out.objectVersion > 0 && !out.checksumType.empty() &&
           !out.checksumDigest.empty() &&
           out.expiresAt >= unixSeconds();
}

std::string issueReadCapability(const ReadCapability& capability,
                                const std::string& clusterSecret)
{
    if(clusterSecret.empty() || capability.schemaVersion != 1 ||
       capability.capabilityId.empty() || capability.principalId.empty() ||
       capability.scope != "object:read" || capability.objectId.empty() ||
       capability.objectVersion == 0 || capability.storageIdentity.empty() ||
       capability.expiresAt <= 0) {
        return {};
    }
    const std::string payload = "read-v1|" + hexEncode(capability.capabilityId) + "|" +
        hexEncode(capability.principalId) + "|" + hexEncode(capability.scope) + "|" +
        hexEncode(capability.objectId) + "|" + std::to_string(capability.objectVersion) + "|" +
        hexEncode(capability.storageIdentity) + "|" + std::to_string(capability.expiresAt);
    unsigned char signature[EVP_MAX_MD_SIZE];
    unsigned int signatureSize = 0;
    if(HMAC(EVP_sha256(), clusterSecret.data(), static_cast<int>(clusterSecret.size()),
            reinterpret_cast<const unsigned char*>(payload.data()), payload.size(),
            signature, &signatureSize) == nullptr) {
        return {};
    }
    return hexEncode(payload) + "." +
        hexEncode(std::string(reinterpret_cast<const char*>(signature), signatureSize));
}

bool verifyReadCapability(const std::string& token,
                          const std::string& clusterSecret,
                          ReadCapability& out)
{
    out = {};
    const size_t splitAt = token.find('.');
    if(splitAt == std::string::npos || splitAt == 0 || splitAt + 1 >= token.size() ||
       token.size() > 4096 || clusterSecret.empty()) {
        return false;
    }
    std::string payload;
    std::string signature;
    if(!hexDecode(token.substr(0, splitAt), payload) ||
       !hexDecode(token.substr(splitAt + 1), signature)) {
        return false;
    }
    unsigned char expected[EVP_MAX_MD_SIZE];
    unsigned int expectedSize = 0;
    if(HMAC(EVP_sha256(), clusterSecret.data(), static_cast<int>(clusterSecret.size()),
            reinterpret_cast<const unsigned char*>(payload.data()), payload.size(),
            expected, &expectedSize) == nullptr || signature.size() != expectedSize ||
       !constantTimeEquals(signature,
            std::string(reinterpret_cast<const char*>(expected), expectedSize))) {
        return false;
    }
    const auto fields = split(payload, '|');
    if(fields.size() != 8 || fields[0] != "read-v1") return false;
    try {
        out.schemaVersion = 1;
        if(!hexDecode(fields[1], out.capabilityId) ||
           !hexDecode(fields[2], out.principalId) ||
           !hexDecode(fields[3], out.scope) ||
           !hexDecode(fields[4], out.objectId) ||
           !hexDecode(fields[6], out.storageIdentity)) {
            return false;
        }
        out.objectVersion = std::stoull(fields[5]);
        out.expiresAt = std::stoll(fields[7]);
    } catch(...) {
        return false;
    }
    return !out.capabilityId.empty() && !out.principalId.empty() &&
           out.scope == "object:read" && !out.objectId.empty() &&
           out.objectVersion > 0 && !out.storageIdentity.empty() &&
           out.expiresAt >= unixSeconds();
}

}
}








