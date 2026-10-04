#include "qtclient/TransferModel.hpp"

#include <QStringList>

#include <algorithm>
#include <cmath>

namespace miniKV::qtclient {

TransferModel::TransferModel(QObject* parent) : QAbstractTableModel(parent) {}

int TransferModel::rowCount(const QModelIndex& parent) const {
    return parent.isValid() ? 0 : rows_.size();
}

int TransferModel::columnCount(const QModelIndex& parent) const {
    return parent.isValid() ? 0 : ColumnCount;
}

QString TransferModel::stateText(TransferState state) {
    switch (state) {
    case TransferState::Queued: return QStringLiteral("Queued");
    case TransferState::Running: return QStringLiteral("Running");
    case TransferState::Verifying: return QStringLiteral("Verifying");
    case TransferState::Completed: return QStringLiteral("Completed");
    case TransferState::Failed: return QStringLiteral("Failed");
    case TransferState::RetryWaiting: return QStringLiteral("Retry waiting");
    case TransferState::Cancelling: return QStringLiteral("Cancelling");
    case TransferState::Cancelled: return QStringLiteral("Cancelled");
    }
    return QStringLiteral("Unknown");
}

QString TransferModel::directionText(TransferDirection direction) {
    return direction == TransferDirection::Upload ? QStringLiteral("Upload")
                                                   : QStringLiteral("Download");
}

QVariant TransferModel::data(const QModelIndex& index, int role) const {
    if (!index.isValid() || index.row() >= rows_.size()) return {};
    const auto& row = rows_[index.row()];
    if (role == Qt::TextAlignmentRole) {
        const int alignment = static_cast<int>(Qt::AlignLeft | Qt::AlignVCenter);
        return QVariant(alignment);
    }
    if (role != Qt::DisplayRole) return {};
    switch (index.column()) {
    case TaskColumn: return row.taskId;
    case DirectionColumn: return directionText(row.direction);
    case NameColumn: return row.displayName;
    case ProgressColumn:
        if (row.totalBytes == 0) return row.stage;
        return QStringLiteral("%1 / %2 (%3%)")
            .arg(row.completedBytes).arg(row.totalBytes)
            .arg((100.0 * row.completedBytes) / row.totalBytes, 0, 'f', 1);
    case SpeedColumn: {
        if (row.bytesPerSecond == 0) return QStringLiteral("-");
        const double mib = static_cast<double>(row.bytesPerSecond) / (1024.0 * 1024.0);
        return QStringLiteral("%1 MiB/s").arg(mib, 0, 'f', 2);
    }
    case ElapsedColumn: {
        const qint64 seconds = std::max<qint64>(0, row.elapsedMs / 1000);
        return QStringLiteral("%1:%2").arg(seconds / 60, 2, 10, QLatin1Char('0'))
            .arg(seconds % 60, 2, 10, QLatin1Char('0'));
    }
    case EtaColumn:
        if (row.etaSeconds < 0 || row.state == TransferState::Completed) return QStringLiteral("-");
        return QStringLiteral("%1s").arg(row.etaSeconds);
    case AttemptColumn: return row.attempt;
    case StateColumn: return stateText(row.state);
    case ResultColumn: return row.state == TransferState::Failed ? row.error : row.result;
    default: return {};
    }
}

QVariant TransferModel::headerData(int section, Qt::Orientation orientation, int role) const {
    if (orientation != Qt::Horizontal || role != Qt::DisplayRole) return {};
    static const QStringList headers = {
        "Task", "Direction", "Name/Object", "Progress", "Speed", "Elapsed", "ETA", "Attempt", "State", "Result"
    };
    return section >= 0 && section < headers.size() ? headers[section] : QVariant{};
}

void TransferModel::addTask(const TransferSnapshot& snapshot) {
    const int row = rows_.size();
    beginInsertRows({}, row, row);
    rows_.push_back(snapshot);
    endInsertRows();
}

void TransferModel::updateTask(const TransferSnapshot& snapshot) {
    for (int row = 0; row < rows_.size(); ++row) {
        if (rows_[row].taskId == snapshot.taskId) {
            rows_[row] = snapshot;
            emit dataChanged(index(row, 0), index(row, ColumnCount - 1));
            return;
        }
    }
}

}  // namespace miniKV::qtclient
