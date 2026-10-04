#pragma once

#include "qtclient/TransferTypes.hpp"

#include <QAbstractListModel>
#include <QVector>

namespace miniKV::qtclient {

class LogModel final : public QAbstractListModel {
    Q_OBJECT

public:
    explicit LogModel(QObject* parent = nullptr);
    int rowCount(const QModelIndex& parent = {}) const override;
    QVariant data(const QModelIndex& index, int role = Qt::DisplayRole) const override;

public slots:
    void appendEvent(const miniKV::qtclient::LogEvent& event);

private:
    QVector<QString> rows_;
};

}  // namespace miniKV::qtclient
