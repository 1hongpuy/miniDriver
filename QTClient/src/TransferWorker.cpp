#include "qtclient/TransferWorker.hpp"

#include <QFileInfo>

#include <filesystem>
#include <algorithm>
#include <chrono>
#include <mutex>
#include <utility>

namespace miniKV::qtclient {

namespace {

struct RateValues {
    uint64_t instantBytesPerSecond = 0;
    uint64_t averageBytesPerSecond = 0;
    qint64 elapsedMs = 0;
    qint64 etaSeconds = -1;
};

class RateMeter {
public:
    RateValues sample(uint64_t completed, uint64_t total) {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto now = std::chrono::steady_clock::now();
        if (!started_) {
            started_ = true;
            startedAt_ = now;
            lastAt_ = now;
            lastCompleted_ = completed;
        }
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - startedAt_).count();
        const auto deltaMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastAt_).count();
        const uint64_t deltaBytes = completed >= lastCompleted_ ? completed - lastCompleted_ : 0;
        uint64_t instant = 0;
        if (deltaMs > 0) instant = deltaBytes * 1000ULL / static_cast<uint64_t>(deltaMs);
        if (instant > 0) {
            smoothBytesPerSecond_ = smoothBytesPerSecond_ == 0
                ? static_cast<double>(instant)
                : smoothBytesPerSecond_ * 0.75 + static_cast<double>(instant) * 0.25;
        }
        const uint64_t average = elapsed > 0
            ? completed * 1000ULL / static_cast<uint64_t>(elapsed) : 0;
        lastAt_ = now;
        lastCompleted_ = completed;
        RateValues result;
        result.instantBytesPerSecond = static_cast<uint64_t>(smoothBytesPerSecond_);
        result.averageBytesPerSecond = average;
        result.elapsedMs = elapsed;
        if (total > completed && smoothBytesPerSecond_ > 0) {
            result.etaSeconds = static_cast<qint64>((total - completed) /
                static_cast<uint64_t>(smoothBytesPerSecond_));
        } else if (total == completed && total != 0) {
            result.etaSeconds = 0;
        }
        return result;
    }

private:
    std::mutex mutex_;
    bool started_ = false;
    std::chrono::steady_clock::time_point startedAt_{};
    std::chrono::steady_clock::time_point lastAt_{};
    uint64_t lastCompleted_ = 0;
    double smoothBytesPerSecond_ = 0;
};

void applyRates(TransferSnapshot& snapshot, const RateValues& values) {
    snapshot.bytesPerSecond = values.instantBytesPerSecond;
    snapshot.averageBytesPerSecond = values.averageBytesPerSecond;
    snapshot.elapsedMs = values.elapsedMs;
    snapshot.etaSeconds = values.etaSeconds;
}

}  // namespace

TransferWorker::TransferWorker(TransferSpec spec, QObject* parent)
    : QObject(parent), spec_(std::move(spec)) {}

TransferSnapshot TransferWorker::baseSnapshot(TransferState state, const QString& stage) const {
    TransferSnapshot snapshot;
    snapshot.taskId = spec_.taskId;
    snapshot.direction = spec_.direction;
    snapshot.state = state;
    snapshot.displayName = spec_.direction == TransferDirection::Upload
        ? QFileInfo(spec_.localPath).fileName()
        : spec_.objectId + QStringLiteral("@v") + QString::number(spec_.objectVersion);
    snapshot.localPath = spec_.localPath;
    snapshot.stage = stage;
    snapshot.attempt = spec_.attempt;
    return snapshot;
}

void TransferWorker::start() {
    TransferSnapshot running = baseSnapshot(TransferState::Running, QStringLiteral("starting"));
    emit progress(running);
    if (spec_.direction == TransferDirection::Upload) runUpload();
    else runDownload();
}

void TransferWorker::runUpload() {
    emit log({spec_.taskId, QStringLiteral("upload"), QStringLiteral("upload started")});
    MiniDriverStorageAdapter adapter(spec_.clientConfig);
    miniKV::client::ObjectRef object;
    std::string error;
    const uint64_t total = static_cast<uint64_t>(QFileInfo(spec_.localPath).size());
    RateMeter meter;
    TransferSnapshot initial = baseSnapshot(TransferState::Running, QStringLiteral("upload"));
    initial.totalBytes = total;
    emit progress(initial);
    const bool ok = adapter.upload(
        std::filesystem::path(spec_.localPath.toStdString()),
        [this, total, &meter](uint64_t completed, uint64_t) {
            TransferSnapshot snapshot = baseSnapshot(TransferState::Running, QStringLiteral("upload"));
            snapshot.completedBytes = completed;
            snapshot.totalBytes = total;
            applyRates(snapshot, meter.sample(completed, total));
            emit progress(snapshot);
        }, object, error, spec_.commandId.toStdString());
    if (!ok) {
        emitFailure(QStringLiteral("upload"), error);
        return;
    }
    QString verificationResult;
    if (spec_.verifyRoundTrip) {
        TransferSnapshot verifying = baseSnapshot(TransferState::Verifying,
                                                   QStringLiteral("verifying download"));
        verifying.totalBytes = total;
        emit progress(verifying);
        const std::filesystem::path verifyPath = std::filesystem::temp_directory_path() /
            ("minidriver-qt-" + spec_.taskId.toStdString() + ".verify");
        RateMeter verifyMeter;
        const bool downloaded = adapter.download(
            object, verifyPath,
            [this, total, &verifyMeter](uint64_t completed, uint64_t reportedTotal) {
                TransferSnapshot snapshot = baseSnapshot(TransferState::Verifying,
                                                          QStringLiteral("verifying download"));
                snapshot.completedBytes = completed;
                snapshot.totalBytes = reportedTotal == 0 ? total : reportedTotal;
                applyRates(snapshot, verifyMeter.sample(snapshot.completedBytes, snapshot.totalBytes));
                emit progress(snapshot);
            }, error);
        if (!downloaded) {
            std::error_code ignored;
            std::filesystem::remove(verifyPath, ignored);
            emitFailure(QStringLiteral("verify download"), error);
            return;
        }
        std::string sourceDigest;
        std::string downloadedDigest;
        const bool sourceHashed = MiniDriverStorageAdapter::sha256File(
            std::filesystem::path(spec_.localPath.toStdString()), sourceDigest, error);
        const bool downloadedHashed = sourceHashed && MiniDriverStorageAdapter::sha256File(
            verifyPath, downloadedDigest, error);
        std::error_code ignored;
        std::filesystem::remove(verifyPath, ignored);
        if (!downloadedHashed) {
            emitFailure(QStringLiteral("verify hash"), error);
            return;
        }
        if (sourceDigest != downloadedDigest) {
            emitFailure(QStringLiteral("verify hash"), "source/download SHA-256 mismatch");
            return;
        }
        verificationResult = QStringLiteral("; SHA-256 verified");
    }
    TransferSnapshot completed = baseSnapshot(TransferState::Completed, QStringLiteral("completed"));
    completed.completedBytes = total;
    completed.totalBytes = total;
    completed.objectId = QString::fromStdString(object.objectId);
    completed.objectVersion = object.objectVersion;
    completed.result = QStringLiteral("ObjectRef(%1, v%2)%3").arg(
        completed.objectId).arg(completed.objectVersion).arg(verificationResult);
    emit log({spec_.taskId, QStringLiteral("upload"), completed.result});
    emit finished(completed);
}

void TransferWorker::runDownload() {
    emit log({spec_.taskId, QStringLiteral("download"), QStringLiteral("download started")});
    MiniDriverStorageAdapter adapter(spec_.clientConfig);
    miniKV::client::ObjectRef object{spec_.objectId.toStdString(), spec_.objectVersion};
    std::string error;
    uint64_t total = 0;
    uint64_t lastCompleted = 0;
    RateMeter meter;
    const bool ok = adapter.download(
        object, std::filesystem::path(spec_.localPath.toStdString()),
        [this, &total, &lastCompleted, &meter](uint64_t completed, uint64_t reportedTotal) {
            lastCompleted = completed;
            total = reportedTotal;
            TransferSnapshot snapshot = baseSnapshot(TransferState::Running, QStringLiteral("download"));
            snapshot.completedBytes = completed;
            snapshot.totalBytes = total;
            applyRates(snapshot, meter.sample(completed, total));
            emit progress(snapshot);
        }, error);
    if (!ok) {
        emitFailure(QStringLiteral("download"), error);
        return;
    }
    TransferSnapshot completed = baseSnapshot(TransferState::Completed, QStringLiteral("completed"));
    completed.completedBytes = total == 0 ? lastCompleted : total;
    completed.totalBytes = total == 0 ? lastCompleted : total;
    completed.objectId = spec_.objectId;
    completed.objectVersion = spec_.objectVersion;
    completed.result = QStringLiteral("downloaded to %1").arg(spec_.localPath);
    emit log({spec_.taskId, QStringLiteral("download"), completed.result});
    emit finished(completed);
}

void TransferWorker::emitFailure(const QString& stage, const std::string& error) {
    TransferSnapshot failed = baseSnapshot(TransferState::Failed, stage);
    failed.error = QString::fromStdString(error.empty() ? "transfer failed" : error);
    emit log({spec_.taskId, stage, failed.error});
    emit finished(failed);
}

}  // namespace miniKV::qtclient
