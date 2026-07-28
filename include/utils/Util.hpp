#pragma once


#include <cstdint>
#include <map>
#include <string>
#include <vector>


namespace miniKV {
namespace util {

//通行证
struct UploadCapability {
    std::string sessionId;
    uint32_t chunkIndex = 0;
    std::string chunkHash;
    uint64_t chunkSize = 0;
    std::vector<std::string> chainTargets;
    std::string leaseId;
    int64_t expiresAt = 0;
};


int64_t unixSeconds();
std::string sha256Hex(const char* data, size_t size);
std::string randomId();

std::string jsonString(const std::string& json, const std::string& key);
uint64_t jsonUint(const std::string& json, const std::string& key, uint64_t fallback = 0);
std::vector<uint32_t> jsonUIntArray(const std::string& json, const std::string& key);
std::vector<std::string> jsonObjectArray(const std::string& json, const std::string& key);
std::string jsonEscape(const std::string& value);
std::string jsonError(const std::string& message);

std::string urlDecode(const std::string& value);
std::map<std::string, std::string> parseForm(const std::string& body);
std::string getQueryValue(const std::string& query, const std::string& key);

std::string hexEncode(const std::string& value);
bool hexDecode(const std::string& value, std::string& out);
std::vector<std::string> split(const std::string& value, char separator);
std::string join(const std::vector<std::string>& values, char separator);

std::string issueUploadCapability(const UploadCapability& capability,
                                  const std::string& clusterSecret);
bool verifyUploadCapability(const std::string& token,
                            const std::string& clusterSecret,
                            UploadCapability& out);
bool constantTimeEquals(const std::string& left, const std::string& right);


}

}





























