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
    pending_.enqueue(spec);
    emit taskAdded(initialSnapshot(spec));
    emit log({spec.taskId, QStringLiteral("manager"), QStringLiteral("download queued")});
    startNext();
    return spec.taskId;
}

void TransferManager::startNext() {
    while (active_.size() < kMaxActiveTasks && !pending_.isEmpty()) {
        startTask(pending_.dequeue());
    }
}

void TransferManager::startTask(const TransferSpec& spec) {
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
    emit taskUpdated(snapshot);
}

}  // namespace miniKV::qtclient
