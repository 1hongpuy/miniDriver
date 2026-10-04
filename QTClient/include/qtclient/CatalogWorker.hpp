#pragma once

#include "qtclient/CatalogTypes.hpp"

#include "client/MiniDriverClient.hpp"

#include <QObject>

namespace miniKV::qtclient {

// Catalog requests use synchronous HTTP underneath, so this object always
// runs in its own QThread. It never accesses QWidget instances.
class CatalogWorker final : public QObject {
    Q_OBJECT

public:
    CatalogWorker(QString path, miniKV::client::ClientConfig config,
                  QObject* parent = nullptr);

public slots:
    void start();

signals:
    void loaded(const miniKV::qtclient::CatalogSnapshot& snapshot);
    void failed(const QString& path, const QString& error);
    void finished();

private:
    QString path_;
    miniKV::client::ClientConfig config_;
};

}  // namespace miniKV::qtclient
