#include "TestCheck.hpp"
#include "media/MediaJob.hpp"

#include <string>

int main()
{
    miniKV::media::MediaJob expected;
    expected.jobId = "job-1";
    expected.type = miniKV::media::JobType::kThumbnail;
    expected.sourceFileHash = "source-file-hash";
    expected.profile = "thumb-512-jpeg-v1";
    expected.state = miniKV::media::JobState::kPending;
    expected.attempts = 2;
    expected.leaseUntil = 123;
    expected.nextRetryAt = 456;
    expected.lastError = "temporary redis failure";
    expected.createdAt = 10;
    expected.updatedAt = 11;

    miniKV::media::MediaJob actual;
    MINIKV_CHECK(miniKV::media::parseMediaJob(
        miniKV::media::serializeMediaJob(expected), actual));
    MINIKV_CHECK(actual.jobId == expected.jobId);
    MINIKV_CHECK(actual.type == expected.type);
    MINIKV_CHECK(actual.sourceFileHash == expected.sourceFileHash);
    MINIKV_CHECK(actual.profile == expected.profile);
    MINIKV_CHECK(actual.state == expected.state);
    MINIKV_CHECK(actual.attempts == expected.attempts);
    MINIKV_CHECK(actual.leaseUntil == expected.leaseUntil);
    MINIKV_CHECK(actual.nextRetryAt == expected.nextRetryAt);
    MINIKV_CHECK(actual.lastError == expected.lastError);
    MINIKV_CHECK(actual.createdAt == expected.createdAt);
    MINIKV_CHECK(actual.updatedAt == expected.updatedAt);

    miniKV::media::ThumbnailMeta thumbnail;
    thumbnail.sourceFileHash = expected.sourceFileHash;
    thumbnail.profile = expected.profile;
    thumbnail.state = miniKV::media::JobState::kReady;
    thumbnail.derivedObjectId = "thumbnail-object";
    thumbnail.derivedFileHash = "thumbnail-file-hash";
    thumbnail.jobId = expected.jobId;
    thumbnail.updatedAt = 12;

    miniKV::media::ThumbnailMeta parsedThumbnail;
    MINIKV_CHECK(miniKV::media::parseThumbnailMeta(
        miniKV::media::serializeThumbnailMeta(thumbnail), parsedThumbnail));
    MINIKV_CHECK(parsedThumbnail.sourceFileHash == thumbnail.sourceFileHash);
    MINIKV_CHECK(parsedThumbnail.profile == thumbnail.profile);
    MINIKV_CHECK(parsedThumbnail.state == thumbnail.state);
    MINIKV_CHECK(parsedThumbnail.derivedObjectId == thumbnail.derivedObjectId);
    MINIKV_CHECK(parsedThumbnail.derivedFileHash == thumbnail.derivedFileHash);
    MINIKV_CHECK(parsedThumbnail.jobId == thumbnail.jobId);
    MINIKV_CHECK(parsedThumbnail.updatedAt == thumbnail.updatedAt);

    const std::string invalid = "not-a-media-job";
    MINIKV_CHECK(!miniKV::media::parseMediaJob(invalid, actual));
    return 0;
}
