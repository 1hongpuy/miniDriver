#include "qtclient/TransferManager.hpp"

#include <QFileInfo>

namespace miniKV::qtclient {

TransferManager::TransferManager(QObject* parent) : QObject(parent) {}

bool TransferManager::persist(const TransferSpec& spec, const TransferSnapshot& snapshot) {
    if (store_.upsert(spec, snapshot)) return true;
    stopActiveUploadsForExit();
    if (journalFailureReported_) return false;
    journalFailureReported_ = true;
    emit log({spec.taskId, QStringLiteral("journal"),
              QStringLiteral("transfer recovery journal is unavailable: %1 (database: %2)")
                  .arg(store_.error(), store_.databasePath())});
    return false;
}

TransferManager::~TransferManager() {
    stopping_ = true;
    markActiveUploadsInterrupted();
    stopActiveUploadsForExit();
    for (auto it = active_.begin(); it != active_.end(); ++it) {
        if (it->thread) {
            it->thread->quit();
            it->thread->wait();
        }
    }
    active_.clear();
}

TransferSnapshot TransferManager::initialSnapshot(const TransferSpec& spec) const {
    TransferSnapshot snapshot;
    snapshot.taskId = spec.taskId;
    snapshot.direction = spec.direction;
    snapshot.state = TransferState::Queued;
    snapshot.displayName = spec.direction == TransferDirection::Upload
        ? QFileInfo(spec.localPath).fileName()
        : spec.objectId + QStringLiteral("@v") + QString::number(spec.objectVersion);
    snapshot.objectId = spec.objectId;
    snapshot.objectVersion = spec.objectVersion;
    snapshot.localPath = spec.localPath;
    snapshot.stage = QStringLiteral("queued");
    snapshot.attempt = spec.attempt;
    return snapshot;
}

bool TransferManager::captureUploadIdentity(TransferSpec& spec, QString& error) const {
    const QFileInfo source(spec.localPath);
    if (!source.exists() || !source.isFile() || source.size() <= 0) {
        error = QStringLiteral("The selected source file no longer exists or is empty.");
        return false;
    }
    spec.gatewayHost = QString::fromStdString(spec.clientConfig.gateway.host);
    spec.gatewayPort = spec.clientConfig.gateway.port;
    spec.servicePrincipal = QString::fromStdString(spec.clientConfig.servicePrincipal);
    spec.metadataMode = qEnvironmentVariable("MINIKV_METADATA_MODE", QStringLiteral("raft"));
    spec.targetPath = QStringLiteral("/");
    spec.sourceSize = static_cast<quint64>(source.size());
    spec.sourceModifiedMs = source.lastModified().toMSecsSinceEpoch();
    return true;
}

bool TransferManager::validateUploadResumeIdentity(const TransferSpec& spec,
                                                   const miniKV::client::ClientConfig& config,
                                                   QString& error) const {
    const QString mode = qEnvironmentVariable("MINIKV_METADATA_MODE", QStringLiteral("raft"));
    if (spec.gatewayHost.isEmpty() || spec.gatewayPort == 0 || spec.servicePrincipal.isEmpty() ||
        spec.metadataMode.isEmpty() || spec.targetPath.isEmpty() || spec.sourceSize == 0 ||
        spec.sourceModifiedMs <= 0) {
        error = QStringLiteral("This interrupted upload was created by an older client without recovery identity. Start a new upload instead.");
        return false;
    }
    if (spec.gatewayHost != QString::fromStdString(config.gateway.host) ||
        spec.gatewayPort != config.gateway.port ||
        spec.servicePrincipal != QString::fromStdString(config.servicePrincipal) || spec.metadataMode != mode) {
        error = QStringLiteral("The current connection does not match the cluster/account/mode that created this upload. Restore its original settings before retrying.");
        return false;
    }
    const QFileInfo source(spec.localPath);
    if (!source.exists() || !source.isFile() || static_cast<quint64>(source.size()) != spec.sourceSize ||
        source.lastModified().toMSecsSinceEpoch() != spec.sourceModifiedMs) {
        error = QStringLiteral("The source file changed, moved, or was deleted. Start a new upload; it cannot safely resume the old session.");
        return false;
    }
    return true;
}

QString TransferManager::enqueueUpload(const QString& localPath,
                                       const miniKV::client::ClientConfig& config,
                                       bool verifyRoundTrip) {
    TransferSpec spec;
    spec.taskId = QUuid::createUuid().toString(QUuid::WithoutBraces);
    spec.direction = TransferDirection::Upload;
    spec.localPath = localPath;
    spec.verifyRoundTrip = verifyRoundTrip;
    spec.commandId = QStringLiteral("qt-upload-") + spec.taskId;
    spec.clientConfig = config;
    QString identityError;
    captureUploadIdentity(spec, identityError);
    TransferSnapshot snapshot = initialSnapshot(spec);
    if (!identityError.isEmpty()) {
        snapshot.state = TransferState::Failed;
        snapshot.stage = QStringLiteral("source");
        snapshot.error = identityError;
        taskSpecs_.insert(spec.taskId, spec);
        failedTasks_.insert(spec.taskId);
        emit taskAdded(snapshot);
        return spec.taskId;
    }
    if (stopping_) {
        snapshot.state = TransferState::Failed;
        snapshot.stage = QStringLiteral("stopping");
        snapshot.error = QStringLiteral("Client is stopping; no new transfer was started.");
        taskSpecs_.insert(spec.taskId, spec);
        failedTasks_.insert(spec.taskId);
        emit taskAdded(snapshot);
        return spec.taskId;
    }
    if (!persist(spec, snapshot)) {
        snapshot.state = TransferState::Failed;
        snapshot.stage = QStringLiteral("journal");
        snapshot.error = QStringLiteral("Upload was not started because its recovery record could not be saved.");
        taskSpecs_.insert(spec.taskId, spec);
        failedTasks_.insert(spec.taskId);
        emit taskAdded(snapshot);
        return spec.taskId;
    }
    taskSpecs_.insert(spec.taskId, spec);
    pending_.enqueue(spec);
    emit taskAdded(snapshot);
    emit log({spec.taskId, QStringLiteral("manager"), QStringLiteral("upload queued")});
    startNext();
    return spec.taskId;
}

QString TransferManager::enqueueDownload(const QString& objectId, quint64 objectVersion,
                                         const QString& outputPath,
                                         const miniKV::client::ClientConfig& config) {
    TransferSpec spec;
    spec.taskId = QUuid::createUuid().toString(QUuid::WithoutBraces);
    spec.direction = TransferDirection::Download;
    spec.localPath = outputPath;
    spec.objectId = objectId;
    spec.objectVersion = objectVersion;
    spec.clientConfig = config;
    TransferSnapshot snapshot = initialSnapshot(spec);
    if (stopping_) {
        snapshot.state = TransferState::Failed;
        snapshot.stage = QStringLiteral("stopping");
        snapshot.error = QStringLiteral("Client is stopping; no new transfer was started.");
        taskSpecs_.insert(spec.taskId, spec);
        failedTasks_.insert(spec.taskId);
        emit taskAdded(snapshot);
        return spec.taskId;
    }
    if (!persist(spec, snapshot)) {
        snapshot.state = TransferState::Failed;
        snapshot.stage = QStringLiteral("journal");
        snapshot.error = QStringLiteral("Download was not started because its recovery record could not be saved.");
        taskSpecs_.insert(spec.taskId, spec);
        failedTasks_.insert(spec.taskId);
        emit taskAdded(snapshot);
        return spec.taskId;
    }
    taskSpecs_.insert(spec.taskId, spec);
    pending_.enqueue(spec);
    emit taskAdded(snapshot);
    emit log({spec.taskId, QStringLiteral("manager"), QStringLiteral("download queued")});
    startNext();
    return spec.taskId;
}

bool TransferManager::retry(const QString& taskId,
                            const miniKV::client::ClientConfig& config,
                            QString& error) {
    if (stopping_) {
        error = QStringLiteral("Client is stopping; retry is unavailable.");
        return false;
    }
    const auto found = taskSpecs_.constFind(taskId);
    if (found == taskSpecs_.cend()) {
        error = QStringLiteral("Unknown transfer task");
        return false;
    }
    if (!failedTasks_.contains(taskId)) {
        error = QStringLiteral("Only a failed transfer can be retried");
        return false;
    }
    if (retryPending_.contains(taskId)) {
        error = QStringLiteral("Retry is already queued");
        return false;
    }

    TransferSpec spec = found.value();
    spec.clientConfig = config;
    if (spec.direction == TransferDirection::Upload && !validateUploadResumeIdentity(spec, config, error)) {
        return false;
    }
    ++spec.attempt;
    const TransferSnapshot snapshot = initialSnapshot(spec);
    if (!persist(spec, snapshot)) {
        error = QStringLiteral("Recovery journal could not save the retry: %1").arg(store_.error());
        return false;
    }
    taskSpecs_.insert(taskId, spec);
    retryPending_.insert(taskId);
    failedTasks_.remove(taskId);
    finishedTasks_.remove(taskId);
    pending_.enqueue(spec);
    emit taskUpdated(snapshot);
    emit log({taskId, QStringLiteral("manager"),
              QStringLiteral("retry queued; attempt %1; upload keeps its original command ID")
                  .arg(spec.attempt)});
    startNext();
    return true;
}

void TransferManager::restoreInterruptedUploads(const miniKV::client::ClientConfig& config) {
    if (!store_.available()) {
        emit log({QString(), QStringLiteral("journal"),
                  QStringLiteral("transfer recovery is disabled: %1 (database: %2)")
                      .arg(store_.error(), store_.databasePath())});
        return;
    }

    QVector<TransferSpec> interrupted;
    if (!store_.interruptedUploads(interrupted)) {
        emit log({QString(), QStringLiteral("journal"),
                  QStringLiteral("could not read transfer recovery journal: %1 (database: %2)")
                      .arg(store_.error(), store_.databasePath())});
        return;
    }

    int restored = 0;
    for (TransferSpec spec : interrupted) {
        if (taskSpecs_.contains(spec.taskId)) continue;
        spec.clientConfig = config;
        taskSpecs_.insert(spec.taskId, spec);
        failedTasks_.insert(spec.taskId);
        TransferSnapshot snapshot = initialSnapshot(spec);
        snapshot.state = TransferState::Failed;
        snapshot.stage = QStringLiteral("interrupted");
        QString identityError;
        if (!validateUploadResumeIdentity(spec, config, identityError)) {
            snapshot.stage = QStringLiteral("needs attention");
            snapshot.error = identityError;
        } else {
            snapshot.error = QStringLiteral("Previous client instance stopped; select this row and retry to resume.");
        }
        persist(spec, snapshot);
        emit taskAdded(snapshot);
        emit log({spec.taskId, QStringLiteral("manager"),
                  QStringLiteral("interrupted upload restored; retry keeps original command ID")});
        ++restored;
    }
    emit log({QString(), QStringLiteral("journal"),
              QStringLiteral("transfer recovery journal ready: %1; restored %2 interrupted upload(s)")
                  .arg(store_.databasePath()).arg(restored)});
}

void TransferManager::markActiveUploadsInterrupted() {
    for (auto it = active_.cbegin(); it != active_.cend(); ++it) {
        const auto spec = taskSpecs_.constFind(it.key());
        if (spec == taskSpecs_.cend() || spec->direction != TransferDirection::Upload) continue;
        TransferSnapshot snapshot = initialSnapshot(*spec);
        snapshot.state = TransferState::Failed;
        snapshot.stage = QStringLiteral("interrupted");
        snapshot.error = QStringLiteral("Client exited while this upload was active; select this row and retry to resume.");
        persist(*spec, snapshot);
    }
}

void TransferManager::stopActiveUploadsForExit() {
    stopping_ = true;
    pending_.clear();
    for (auto it = active_.cbegin(); it != active_.cend(); ++it) {
        const auto spec = taskSpecs_.constFind(it.key());
        if (spec != taskSpecs_.cend() && spec->direction == TransferDirection::Upload && it->cancellation) {
            it->cancellation->store(true, std::memory_order_release);
        }
    }
}

void TransferManager::startNext() {
    if (stopping_) return;
    // A Retry can be queued while its failed worker is still unwinding. Never
    // overlap attempts of the same logical task/session; defer that entry
    // until QThread::finished removes the old ActiveTask.
    const int queuedAtEntry = pending_.size();
    for (int index = 0; index < queuedAtEntry && active_.size() < kMaxActiveTasks &&
                        !pending_.isEmpty(); ++index) {
        TransferSpec next = pending_.dequeue();
        if (active_.contains(next.taskId)) {
            pending_.enqueue(std::move(next));
            continue;
        }
        startTask(next);
    }
}

void TransferManager::startTask(const TransferSpec& spec) {
    retryPending_.remove(spec.taskId);
    TransferSnapshot running = initialSnapshot(spec);
    running.state = TransferState::Running;
    running.stage = QStringLiteral("starting");
    if (!persist(spec, running)) {
        running.state = TransferState::Failed;
        running.stage = QStringLiteral("journal");
        running.error = QStringLiteral("Transfer was not started because its recovery record could not be updated.");
        failedTasks_.insert(spec.taskId);
        emit taskUpdated(running);
        return;
    }
    auto* thread = new QThread(this);
    auto cancellation = std::make_shared<std::atomic_bool>(false);
    auto* worker = new TransferWorker(spec, cancellation);
    worker->moveToThread(thread);
    active_.insert(spec.taskId, ActiveTask{thread, std::move(cancellation)});

    connect(thread, &QThread::started, worker, &TransferWorker::start);
    connect(worker, &TransferWorker::progress, this, &TransferManager::onProgress,
            Qt::QueuedConnection);
    connect(worker, &TransferWorker::log, this, &TransferManager::log,
            Qt::QueuedConnection);
    connect(worker, &TransferWorker::finished, this, &TransferManager::onFinished,
            Qt::QueuedConnection);
    connect(worker, &TransferWorker::finished, worker, &QObject::deleteLater);
    connect(worker, &TransferWorker::finished, thread, &QThread::quit,
            Qt::DirectConnection);
    connect(thread, &QThread::finished, this, [this, taskId = spec.taskId, thread] {
        active_.remove(taskId);
        thread->deleteLater();
        startNext();
    });
    thread->start();
}

void TransferManager::onProgress(const TransferSnapshot& snapshot) {
    if (finishedTasks_.contains(snapshot.taskId)) return;
    emit taskUpdated(snapshot);
}

void TransferManager::onFinished(const TransferSnapshot& snapshot) {
    finishedTasks_.insert(snapshot.taskId);
    if (snapshot.state == TransferState::Failed) failedTasks_.insert(snapshot.taskId);
    else failedTasks_.remove(snapshot.taskId);
    const auto spec = taskSpecs_.constFind(snapshot.taskId);
    if (spec != taskSpecs_.cend()) persist(spec.value(), snapshot);
    emit taskUpdated(snapshot);
}

}  // namespace miniKV::qtclient
