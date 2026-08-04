#include "TestCheck.hpp"
#include "gateway/GatewayState.hpp"

#include <filesystem>
#include <string>

int main()
{
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_gateway_media_jobs_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    miniKV::gateway::GatewayState state(directory.string());
    MINIKV_CHECK(state.open());

    constexpr int64_t now = 100;
    const auto first = state.enqueueThumbnail("source-file", "thumb-512-jpeg-v1", now);
    MINIKV_CHECK(first.publishRequired);
    MINIKV_CHECK(!first.job.jobId.empty());
    MINIKV_CHECK(first.job.state == miniKV::media::JobState::kPending);

    const auto duplicate = state.enqueueThumbnail("source-file", "thumb-512-jpeg-v1", now + 1);
    MINIKV_CHECK(!duplicate.publishRequired);
    MINIKV_CHECK(duplicate.job.jobId == first.job.jobId);

    miniKV::media::MediaJob claimed;
    MINIKV_CHECK(state.claimMediaJob(first.job.jobId, now, 10, claimed));
    MINIKV_CHECK(claimed.state == miniKV::media::JobState::kRunning);
    MINIKV_CHECK(claimed.attempts == 1);
    MINIKV_CHECK(claimed.leaseUntil == now + 10);
    const std::string firstLeaseToken = claimed.leaseToken;
    MINIKV_CHECK(!firstLeaseToken.empty());
    MINIKV_CHECK(!state.claimMediaJob(first.job.jobId, now + 1, 10, claimed));

    MINIKV_CHECK(state.claimMediaJob(first.job.jobId, now + 11, 10, claimed));
    MINIKV_CHECK(claimed.attempts == 2);
    MINIKV_CHECK(claimed.leaseToken != firstLeaseToken);

    MINIKV_CHECK(!state.completeMediaJob(first.job.jobId, firstLeaseToken,
                                         "derived-object", "derived-file", now + 12));
    MINIKV_CHECK(state.completeMediaJob(first.job.jobId, claimed.leaseToken,
                                        "derived-object", "derived-file", now + 12));
    miniKV::media::ThumbnailMeta thumbnail;
    MINIKV_CHECK(state.getThumbnail("source-file", "thumb-512-jpeg-v1", thumbnail));
    MINIKV_CHECK(thumbnail.state == miniKV::media::JobState::kReady);
    MINIKV_CHECK(thumbnail.derivedObjectId == "derived-object");
    MINIKV_CHECK(thumbnail.derivedFileHash == "derived-file");

    const auto readyDuplicate = state.enqueueThumbnail("source-file", "thumb-512-jpeg-v1", now + 13);
    MINIKV_CHECK(!readyDuplicate.publishRequired);
    MINIKV_CHECK(readyDuplicate.job.jobId == first.job.jobId);

    const auto failed = state.enqueueThumbnail("retry-source", "thumb-512-jpeg-v1", now);
    MINIKV_CHECK(state.claimMediaJob(failed.job.jobId, now, 10, claimed));
    MINIKV_CHECK(state.failMediaJob(failed.job.jobId, claimed.leaseToken, false,
                                    "temporary", now + 20, now + 1));
    const auto notDue = state.dueMediaJobs(now + 19, 10);
    MINIKV_CHECK(notDue.empty());
    const auto due = state.dueMediaJobs(now + 20, 10);
    MINIKV_CHECK(due.size() == 1);
    MINIKV_CHECK(due.front().jobId == failed.job.jobId);

    std::filesystem::remove_all(directory, error);
    return 0;
}
