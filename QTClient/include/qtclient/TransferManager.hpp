#pragma once

#include "qtclient/TransferWorker.hpp"

#include <QHash>
#include <QObject>
#include <QQueue>
#include <QSet>
#include <QThread>

namespace miniKV::qtclient {

class TransferManager final : public QObject {
    Q_OBJECT

public:
    explicit TransferManager(QObject* parent = nullptr);
    ~TransferManager() override;

    QString enqueueUpload(const QString& localPath,
                          const miniKV::client::ClientConfig& config,
                          bool verifyRoundTrip = false);
    QString enqueueDownload(const QString& objectId, quint64 objectVersion,
                            const QString& outputPath,
                            const miniKV::client::ClientConfig& config);
    // Retries the same logical transfer. Upload retries retain commandId, so
    // Raft preflight returns the original session and skips committed chunks.
    bool retry(const QString& taskId, const miniKV::client::ClientConfig& config,
               QString& error);
    bool hasActiveTasks() const noexcept { return !active_.isEmpty(); }

signals:
    void taskAdded(const miniKV::qtclient::TransferSnapshot& snapshot);
    void taskUpdated(const miniKV::qtclient::TransferSnapshot& snapshot);
    void log(const miniKV::qtclient::LogEvent& event);

private slots:
    void startNext();
    void onProgress(const miniKV::qtclient::TransferSnapshot& snapshot);
    void onFinished(const miniKV::qtclient::TransferSnapshot& snapshot);

private:
    struct ActiveTask {
        QThread* thread = nullptr;
        TransferWorker* worker = nullptr;
    };

    void startTask(const TransferSpec& spec);
    TransferSnapshot initialSnapshot(const TransferSpec& spec) const;

    static constexpr int kMaxActiveTasks = 2;
    QQueue<TransferSpec> pending_;
    QHash<QString, ActiveTask> active_;
    QHash<QString, TransferSpec> taskSpecs_;
    QSet<QString> finishedTasks_;
    QSet<QString> failedTasks_;
    QSet<QString> retryPending_;
};

}  // namespace miniKV::qtclient
