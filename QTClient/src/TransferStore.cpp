#include "qtclient/TransferStore.hpp"

#include <QDateTime>
#include <QDir>
#include <QSqlDatabase>
#include <QSqlError>
#include <QSqlQuery>
#include <QStandardPaths>
#include <QUuid>

namespace miniKV::qtclient {

namespace {

int directionValue(TransferDirection direction) {
    return direction == TransferDirection::Upload ? 0 : 1;
}

}  // namespace

TransferStore::TransferStore()
    : connectionName_(QStringLiteral("minidriver-transfer-store-") +
                      QUuid::createUuid().toString(QUuid::WithoutBraces)) {
    const QString root = QStandardPaths::writableLocation(QStandardPaths::AppDataLocation);
    if (root.isEmpty() || !QDir().mkpath(root)) {
        error_ = QStringLiteral("cannot create application data directory for transfer journal");
        return;
    }
    databasePath_ = root + QStringLiteral("/transfers.sqlite3");
    QSqlDatabase db = QSqlDatabase::addDatabase(QStringLiteral("QSQLITE"), connectionName_);
    db.setDatabaseName(databasePath_);
    if (!db.open()) {
        error_ = db.lastError().text();
        return;
    }
    QSqlQuery query(db);
    // Each snapshot is an SQLite autocommit transaction. WAL plus FULL sync
    // makes a forced process termination or power loss recover the last
    // committed task record on the next start.
    if (!query.exec(QStringLiteral("PRAGMA journal_mode=WAL")) ||
        !query.exec(QStringLiteral("PRAGMA synchronous=FULL")) ||
        !query.exec(QStringLiteral("PRAGMA busy_timeout=5000"))) {
        error_ = query.lastError().text();
        db.close();
        return;
    }
    if (!query.exec(QStringLiteral(
            "CREATE TABLE IF NOT EXISTS transfer_tasks ("
            "task_id TEXT PRIMARY KEY, direction INTEGER NOT NULL, local_path TEXT NOT NULL, "
            "object_id TEXT NOT NULL, object_version INTEGER NOT NULL, verify_round_trip INTEGER NOT NULL, "
            "command_id TEXT NOT NULL, attempt INTEGER NOT NULL, state INTEGER NOT NULL, "
            "completed_bytes INTEGER NOT NULL, total_bytes INTEGER NOT NULL, result TEXT NOT NULL, "
            "error TEXT NOT NULL, updated_at_ms INTEGER NOT NULL)"))) {
        error_ = query.lastError().text();
        db.close();
        return;
    }
    available_ = true;
}

TransferStore::~TransferStore() {
    if (QSqlDatabase::contains(connectionName_)) {
        QSqlDatabase::database(connectionName_).close();
    }
}

bool TransferStore::upsert(const TransferSpec& spec, const TransferSnapshot& snapshot) {
    if (!available_) return false;
    QSqlQuery query(QSqlDatabase::database(connectionName_));
    query.prepare(QStringLiteral(
        "INSERT INTO transfer_tasks (task_id,direction,local_path,object_id,object_version,"
        "verify_round_trip,command_id,attempt,state,completed_bytes,total_bytes,result,error,updated_at_ms) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(task_id) DO UPDATE SET direction=excluded.direction,local_path=excluded.local_path,"
        "object_id=excluded.object_id,object_version=excluded.object_version,"
        "verify_round_trip=excluded.verify_round_trip,command_id=excluded.command_id,attempt=excluded.attempt,"
        "state=excluded.state,completed_bytes=excluded.completed_bytes,total_bytes=excluded.total_bytes,"
        "result=excluded.result,error=excluded.error,updated_at_ms=excluded.updated_at_ms"));
    query.addBindValue(spec.taskId);
    query.addBindValue(directionValue(spec.direction));
    query.addBindValue(spec.localPath);
    query.addBindValue(snapshot.objectId.isEmpty() ? spec.objectId : snapshot.objectId);
    query.addBindValue(snapshot.objectVersion == 0 ? spec.objectVersion : snapshot.objectVersion);
    query.addBindValue(spec.verifyRoundTrip ? 1 : 0);
    query.addBindValue(spec.commandId);
    query.addBindValue(spec.attempt);
    query.addBindValue(static_cast<int>(snapshot.state));
    query.addBindValue(static_cast<qulonglong>(snapshot.completedBytes));
    query.addBindValue(static_cast<qulonglong>(snapshot.totalBytes));
    query.addBindValue(snapshot.result);
    query.addBindValue(snapshot.error);
    query.addBindValue(QDateTime::currentMSecsSinceEpoch());
    if (query.exec()) return true;
    error_ = query.lastError().text();
    return false;
}

QVector<TransferSpec> TransferStore::interruptedUploads() {
    QVector<TransferSpec> result;
    if (!available_) return result;
    QSqlQuery query(QSqlDatabase::database(connectionName_));
    if (!query.exec(QStringLiteral(
            "SELECT task_id,local_path,verify_round_trip,command_id,attempt "
            "FROM transfer_tasks WHERE direction=0 AND state IN (0,1,2,4) "
            "ORDER BY updated_at_ms ASC"))) {
        error_ = query.lastError().text();
        return result;
    }
    while (query.next()) {
        TransferSpec spec;
        spec.taskId = query.value(0).toString();
        spec.direction = TransferDirection::Upload;
        spec.localPath = query.value(1).toString();
        spec.verifyRoundTrip = query.value(2).toInt() != 0;
        spec.commandId = query.value(3).toString();
        spec.attempt = query.value(4).toInt();
        if (!spec.taskId.isEmpty() && !spec.localPath.isEmpty() && !spec.commandId.isEmpty()) {
            result.push_back(std::move(spec));
        }
    }
    return result;
}

}  // namespace miniKV::qtclient
