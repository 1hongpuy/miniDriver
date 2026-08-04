#pragma once

#include <cstdint>
#include <string>

namespace miniKV {
namespace media {

enum class JobType {
    kThumbnail = 0
};

enum class JobState {
    kPending = 0,
    kRunning,
    kReady,
    kFailed,
    kUnsupported
};

struct MediaJob {
    std::string jobId;
    JobType type = JobType::kThumbnail;
    std::string sourceFileHash;
    std::string profile;
    JobState state = JobState::kPending;
    uint32_t attempts = 0;
    int64_t leaseUntil = 0;
    int64_t nextRetryAt = 0;
    std::string lastError;
    int64_t createdAt = 0;
    int64_t updatedAt = 0;
};

struct ThumbnailMeta {
    std::string sourceFileHash;
    std::string profile;
    JobState state = JobState::kPending;
    std::string derivedObjectId;
    std::string derivedFileHash;
    std::string jobId;
    std::string lastError;
    int64_t updatedAt = 0;
};

std::string serializeMediaJob(const MediaJob& job);
bool parseMediaJob(const std::string& value, MediaJob& out);

std::string serializeThumbnailMeta(const ThumbnailMeta& thumbnail);
bool parseThumbnailMeta(const std::string& value, ThumbnailMeta& out);

}  // namespace media
}  // namespace miniKV
