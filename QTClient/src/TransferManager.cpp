#include "qtclient/TransferManager.hpp"

#include <QFileInfo>

namespace miniKV::qtclient {

TransferManager::TransferManager(QObject* parent) : QObject(parent) {}

TransferManager::~TransferManager() {
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
    taskSpecs_.insert(spec.taskId, spec);
    pending_.enqueue(spec);
    emit taskAdded(initialSnapshot(spec));
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
    taskSpecs_.insert(spec.taskId, spec);
    pending_.enqueue(spec);
    emit taskAdded(initialSnapshot(spec));
    emit log({spec.taskId, QStringLiteral("manager"), QStringLiteral("download queued")});
    startNext();
    return spec.taskId;
}

bool TransferManager::retry(const QString& taskId,
                            const miniKV::client::ClientConfig& config,
                            QString& error) {
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
    ++spec.attempt;
    taskSpecs_.insert(taskId, spec);
    retryPending_.insert(taskId);
    failedTasks_.remove(taskId);
    finishedTasks_.remove(taskId);
    pending_.enqueue(spec);
    emit taskUpdated(initialSnapshot(spec));
    emit log({taskId, QStringLiteral("manager"),
              QStringLiteral("retry queued; attempt %1; upload keeps its original command ID")
                  .arg(spec.attempt)});
    startNext();
    return true;
}

void TransferManager::startNext() {
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
    auto* thread = new QThread(this);
    auto* worker = new TransferWorker(spec);
    worker->moveToThread(thread);
    active_.insert(spec.taskId, ActiveTask{thread, worker});

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
    emit taskUpdated(snapshot);
}

}  // namespace miniKV::qtclient
