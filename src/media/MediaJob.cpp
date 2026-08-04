#include "media/MediaJob.hpp"

#include "utils/Util.hpp"

#include <limits>
#include <string>
#include <vector>

namespace miniKV {
namespace media {
namespace {

using miniKV::util::hexDecode;
using miniKV::util::hexEncode;
using miniKV::util::split;

bool parseJobType(const std::string& value, JobType& out)
{
    try {
        const int parsed = std::stoi(value);
        if (parsed != static_cast<int>(JobType::kThumbnail)) return false;
        out = static_cast<JobType>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

bool parseJobState(const std::string& value, JobState& out)
{
    try {
        const int parsed = std::stoi(value);
        if (parsed < static_cast<int>(JobState::kPending) ||
            parsed > static_cast<int>(JobState::kUnsupported)) {
            return false;
        }
        out = static_cast<JobState>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

bool parseUint32(const std::string& value, uint32_t& out)
{
    try {
        const unsigned long parsed = std::stoul(value);
        if (parsed > std::numeric_limits<uint32_t>::max()) return false;
        out = static_cast<uint32_t>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

bool parseInt64(const std::string& value, int64_t& out)
{
    try {
        out = std::stoll(value);
        return true;
    } catch (...) {
        return false;
    }
}

}  // namespace

std::string serializeMediaJob(const MediaJob& job)
{
    return hexEncode(job.jobId) + "|" + std::to_string(static_cast<int>(job.type)) + "|" +
           hexEncode(job.sourceFileHash) + "|" + hexEncode(job.profile) + "|" +
           std::to_string(static_cast<int>(job.state)) + "|" + std::to_string(job.attempts) + "|" +
           std::to_string(job.leaseUntil) + "|" + hexEncode(job.leaseToken) + "|" +
           std::to_string(job.nextRetryAt) + "|" + hexEncode(job.lastError) + "|" +
           std::to_string(job.createdAt) + "|" + std::to_string(job.updatedAt);
}

bool parseMediaJob(const std::string& value, MediaJob& out)
{
    const std::vector<std::string> fields = split(value, '|');
    if (fields.size() != 12) return false;

    MediaJob parsed;
    if (!hexDecode(fields[0], parsed.jobId) || !parseJobType(fields[1], parsed.type) ||
        !hexDecode(fields[2], parsed.sourceFileHash) || !hexDecode(fields[3], parsed.profile) ||
        !parseJobState(fields[4], parsed.state) || !parseUint32(fields[5], parsed.attempts) ||
        !parseInt64(fields[6], parsed.leaseUntil) || !hexDecode(fields[7], parsed.leaseToken) ||
        !parseInt64(fields[8], parsed.nextRetryAt) || !hexDecode(fields[9], parsed.lastError) ||
        !parseInt64(fields[10], parsed.createdAt) || !parseInt64(fields[11], parsed.updatedAt) ||
        parsed.jobId.empty() ||
        parsed.sourceFileHash.empty() || parsed.profile.empty()) {
        return false;
    }

    out = std::move(parsed);
    return true;
}

std::string serializeThumbnailMeta(const ThumbnailMeta& thumbnail)
{
    return hexEncode(thumbnail.sourceFileHash) + "|" + hexEncode(thumbnail.profile) + "|" +
           std::to_string(static_cast<int>(thumbnail.state)) + "|" +
           hexEncode(thumbnail.derivedObjectId) + "|" + hexEncode(thumbnail.derivedFileHash) + "|" +
           hexEncode(thumbnail.jobId) + "|" + hexEncode(thumbnail.lastError) + "|" +
           std::to_string(thumbnail.updatedAt);
}

bool parseThumbnailMeta(const std::string& value, ThumbnailMeta& out)
{
    const std::vector<std::string> fields = split(value, '|');
    if (fields.size() != 8) return false;

    ThumbnailMeta parsed;
    if (!hexDecode(fields[0], parsed.sourceFileHash) || !hexDecode(fields[1], parsed.profile) ||
        !parseJobState(fields[2], parsed.state) || !hexDecode(fields[3], parsed.derivedObjectId) ||
        !hexDecode(fields[4], parsed.derivedFileHash) || !hexDecode(fields[5], parsed.jobId) ||
        !hexDecode(fields[6], parsed.lastError) || !parseInt64(fields[7], parsed.updatedAt) ||
        parsed.sourceFileHash.empty() || parsed.profile.empty()) {
        return false;
    }

    out = std::move(parsed);
    return true;
}

}  // namespace media
}  // namespace miniKV
