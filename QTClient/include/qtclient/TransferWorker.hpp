#pragma once

#include "qtclient/MiniDriverStorageAdapter.hpp"

#include <QObject>

namespace miniKV::qtclient {

class TransferWorker final : public QObject {
    Q_OBJECT

public:
    explicit TransferWorker(TransferSpec spec, QObject* parent = nullptr);

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
};

}  // namespace miniKV::qtclient
