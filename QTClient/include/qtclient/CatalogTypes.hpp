#pragma once

#include <QMetaType>
#include <QString>
#include <QVector>

namespace miniKV::qtclient {

// These types mirror the existing Gateway /api/v2/catalog response. They are
// a Qt presentation adapter for the current trusted-cluster catalog API, not
// a new multi-user business directory model.
struct CatalogDirectory {
    QString path;
    qint64 createdAt = 0;
};

struct CatalogFile {
    QString objectId;
    quint64 objectVersion = 0;
    quint64 metadataVersion = 0;
    QString name;
    QString parentPath;
    QString state;
    quint64 fileSize = 0;
    qint64 createdAt = 0;
};

struct CatalogSnapshot {
    QString path;
    QVector<CatalogDirectory> directories;
    QVector<CatalogFile> files;
};

}  // namespace miniKV::qtclient

Q_DECLARE_METATYPE(miniKV::qtclient::CatalogSnapshot)
