#pragma once

#include "qtclient/TransferTypes.hpp"

#include <QAbstractTableModel>
#include <QVector>

namespace miniKV::qtclient {

class TransferModel final : public QAbstractTableModel {
    Q_OBJECT

public:
    enum Column {
        TaskColumn,
        DirectionColumn,
        NameColumn,
        ProgressColumn,
        SpeedColumn,
        ElapsedColumn,
        EtaColumn,
        StateColumn,
        ResultColumn,
        ColumnCount,
    };

    explicit TransferModel(QObject* parent = nullptr);

    int rowCount(const QModelIndex& parent = {}) const override;
    int columnCount(const QModelIndex& parent = {}) const override;
    QVariant data(const QModelIndex& index, int role = Qt::DisplayRole) const override;
    QVariant headerData(int section, Qt::Orientation orientation,
                        int role = Qt::DisplayRole) const override;

public slots:
    void addTask(const miniKV::qtclient::TransferSnapshot& snapshot);
    void updateTask(const miniKV::qtclient::TransferSnapshot& snapshot);

private:
    static QString stateText(TransferState state);
    static QString directionText(TransferDirection direction);
    QVector<TransferSnapshot> rows_;
};

}  // namespace miniKV::qtclient
