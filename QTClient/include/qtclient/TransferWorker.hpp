#pragma once

#include "qtclient/MiniDriverStorageAdapter.hpp"

#include <QObject>

#include <atomic>
#include <memory>

namespace miniKV::qtclient {

class TransferWorker final : public QObject {
    Q_OBJECT

public:
    using CancellationState = std::shared_ptr<std::atomic_bool>;

    explicit TransferWorker(TransferSpec spec, CancellationState cancellation,
                            QObject* parent = nullptr);

public slots:
    void start();

signals:
    void progress(const miniKV::qtclient::TransferSnapshot& snapshot);
    void log(const miniKV::qtclient::LogEvent& event);
    void finished(const miniKV::qtclient::TransferSnapshot& snapshot);

private:
    void runUpload();
    void runDownload();
    TransferSnapshot baseSnapshot(TransferState state, const QString& stage) const;
    void emitFailure(const QString& stage, const std::string& error);

    TransferSpec spec_;
    // Owned by TransferManager rather than accessed through a QObject living
    // on another thread. It remains valid until the worker has unwound.
    CancellationState cancelRequested_;
};

}  // namespace miniKV::qtclient
