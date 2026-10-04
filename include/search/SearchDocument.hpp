#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace miniKV::search {

struct ObjectRef {
    std::string objectId;
    std::int64_t objectVersion = 0;
};

struct SearchDocument {
    std::string docId;
    ObjectRef object;
    std::string tenantId;
    std::string mediaType;
    std::unordered_map<std::string, std::string> metadata;
    ObjectRef thumbnail;
    bool hasThumbnail = false;
    std::string embeddingModelId;
    std::vector<float> embedding;
    std::string processorVersion;
    std::uint64_t indexGeneration = 0;
    std::string state = "ready";
};

enum class ApplyResult {
    Applied,
    Idempotent,
    RejectedStale,
    RejectedInvalid,
};

bool validate(const SearchDocument& document, std::string* error = nullptr);

}  // namespace miniKV::search
