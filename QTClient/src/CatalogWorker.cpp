#include "qtclient/CatalogWorker.hpp"

#include "qtclient/MiniDriverStorageAdapter.hpp"

#include <utility>

namespace miniKV::qtclient {

CatalogWorker::CatalogWorker(QString path, miniKV::client::ClientConfig config, QObject* parent)
    : QObject(parent), path_(std::move(path)), config_(std::move(config)) {}

void CatalogWorker::start() {
    MiniDriverStorageAdapter adapter(config_);
    CatalogSnapshot snapshot;
    std::string error;
    if (adapter.listCatalog(path_, snapshot, error)) {
        emit loaded(snapshot);
    } else {
        emit failed(path_, QString::fromStdString(
            error.empty() ? "catalog request failed" : error));
    }
    emit finished();
}

}  // namespace miniKV::qtclient
