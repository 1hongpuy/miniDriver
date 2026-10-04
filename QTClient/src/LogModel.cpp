#include "qtclient/LogModel.hpp"

#include <QDateTime>

namespace miniKV::qtclient {

LogModel::LogModel(QObject* parent) : QAbstractListModel(parent) {}

int LogModel::rowCount(const QModelIndex& parent) const {
    return parent.isValid() ? 0 : rows_.size();
}

QVariant LogModel::data(const QModelIndex& index, int role) const {
    if (!index.isValid() || index.row() >= rows_.size() || role != Qt::DisplayRole) return {};
    return rows_[index.row()];
}

void LogModel::appendEvent(const LogEvent& event) {
    const QString line = QStringLiteral("%1 [%2] %3")
        .arg(QDateTime::currentDateTime().toString(Qt::ISODate))
        .arg(event.taskId.isEmpty() ? QStringLiteral("client") : event.taskId)
        .arg(event.stage + QStringLiteral(": ") + event.message);
    const int row = rows_.size();
    beginInsertRows({}, row, row);
    rows_.push_back(line);
    endInsertRows();
    constexpr int kMaxRows = 5000;
    if (rows_.size() > kMaxRows) {
        beginRemoveRows({}, 0, rows_.size() - kMaxRows - 1);
        rows_.erase(rows_.begin(), rows_.begin() + (rows_.size() - kMaxRows));
        endRemoveRows();
    }
}

}  // namespace miniKV::qtclient
