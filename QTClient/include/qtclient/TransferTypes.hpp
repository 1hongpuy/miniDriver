#pragma once

#include "client/MiniDriverClient.hpp"

#include <QMetaType>
#include <QString>
#include <QUuid>

namespace miniKV::qtclient {

enum class TransferDirection {
    Upload,
    Download,
};

enum class TransferState {
    Queued,
    Running,
    Verifying,
    Completed,
    Failed,
    RetryWaiting,
    Cancelling,
    Cancelled,
};

struct TransferSpec {
    QString taskId;
    TransferDirection direction = TransferDirection::Upload;
    QString localPath;
    QString objectId;
    quint64 objectVersion = 0;
    bool verifyRoundTrip = false;
    QString commandId;
    int attempt = 1;
    miniKV::client::ClientConfig clientConfig;
};

struct TransferSnapshot {
    QString taskId;
    TransferDirection direction = TransferDirection::Upload;
    TransferState state = TransferState::Queued;
    QString displayName;
    // Upload: source path. Download: published output path. This is UI-only
    // local state, never sent to Gateway/DataNode.
    QString localPath;
    quint64 completedBytes = 0;
    quint64 totalBytes = 0;
    QString stage;
    QString objectId;
    quint64 objectVersion = 0;
    quint64 bytesPerSecond = 0;
    quint64 averageBytesPerSecond = 0;
    qint64 elapsedMs = 0;
    qint64 etaSeconds = -1;
    int attempt = 1;
    QString result;
    QString error;
};

struct LogEvent {
    QString taskId;
    QString stage;
    QString message;
};

}  // namespace miniKV::qtclient

Q_DECLARE_METATYPE(miniKV::qtclient::TransferSnapshot)
Q_DECLARE_METATYPE(miniKV::qtclient::LogEvent)
