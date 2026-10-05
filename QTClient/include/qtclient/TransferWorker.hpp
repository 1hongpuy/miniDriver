#pragma once

#include "qtclient/MiniDriverStorageAdapter.hpp"

#include <QObject>

#include <atomic>
#include <memory>

namespace miniKV::qtclient {

class TransferWorker final : public QObject {
    Q_OBJECT

public:
    explicit TransferWorker(TransferSpec spec, QObject* parent = nullptr);

    // Safe to call from TransferManager's GUI thread while start() is running
    // on this worker's QThread. The SDK observes this cooperatively.
    void requestCancel() noexcept { cancelRequested_->store(true, std::memory_order_release); }

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
    std::shared_ptr<std::atomic_bool> cancelRequested_ = std::make_shared<std::atomic_bool>(false);
};

}  // namespace miniKV::qtclient
