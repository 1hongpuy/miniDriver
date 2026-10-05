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

// QSqlQuery binds a null QString as SQL NULL. Several journal columns are
// deliberately NOT NULL, so an unfinished upload's empty ObjectRef, result
// and error must be represented by empty TEXT instead.
QString nonNullText(const QString& value) {
    return value.isNull() ? QStringLiteral("") : value;
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
    // SQLite has no ADD COLUMN IF NOT EXISTS. Inspecting the table keeps an
    // interrupted journal written by an older client readable, even if a
    // prior migration was interrupted between ALTER statements.
    const auto ensureColumn = [&query, &db](const QString& name, const QString& declaration) {
        QSqlQuery columns(db);
        if (!columns.exec(QStringLiteral("PRAGMA table_info(transfer_tasks)"))) return false;
        while (columns.next()) {
            if (columns.value(1).toString() == name) return true;
        }
        return query.exec(QStringLiteral("ALTER TABLE transfer_tasks ADD COLUMN ") + declaration);
    };
    if (!ensureColumn(QStringLiteral("gateway_host"), QStringLiteral("gateway_host TEXT NOT NULL DEFAULT ''")) ||
        !ensureColumn(QStringLiteral("gateway_port"), QStringLiteral("gateway_port INTEGER NOT NULL DEFAULT 0")) ||
        !ensureColumn(QStringLiteral("service_principal"), QStringLiteral("service_principal TEXT NOT NULL DEFAULT ''")) ||
        !ensureColumn(QStringLiteral("metadata_mode"), QStringLiteral("metadata_mode TEXT NOT NULL DEFAULT ''")) ||
        !ensureColumn(QStringLiteral("target_path"), QStringLiteral("target_path TEXT NOT NULL DEFAULT '/'")) ||
        !ensureColumn(QStringLiteral("source_size"), QStringLiteral("source_size INTEGER NOT NULL DEFAULT 0")) ||
        !ensureColumn(QStringLiteral("source_modified_ms"), QStringLiteral("source_modified_ms INTEGER NOT NULL DEFAULT 0")) ||
        !query.exec(QStringLiteral("PRAGMA user_version=2"))) {
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
        "verify_round_trip,command_id,attempt,gateway_host,gateway_port,service_principal,metadata_mode,target_path,"
        "source_size,source_modified_ms,state,completed_bytes,total_bytes,result,error,updated_at_ms) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(task_id) DO UPDATE SET direction=excluded.direction,local_path=excluded.local_path,"
        "object_id=excluded.object_id,object_version=excluded.object_version,"
        "verify_round_trip=excluded.verify_round_trip,command_id=excluded.command_id,attempt=excluded.attempt,"
        "gateway_host=excluded.gateway_host,gateway_port=excluded.gateway_port,"
        "service_principal=excluded.service_principal,metadata_mode=excluded.metadata_mode,target_path=excluded.target_path,"
        "source_size=excluded.source_size,source_modified_ms=excluded.source_modified_ms,"
        "state=excluded.state,completed_bytes=excluded.completed_bytes,total_bytes=excluded.total_bytes,"
        "result=excluded.result,error=excluded.error,updated_at_ms=excluded.updated_at_ms"));
    const QString objectId = snapshot.objectId.isEmpty() ? spec.objectId : snapshot.objectId;
    query.addBindValue(nonNullText(spec.taskId));
    query.addBindValue(directionValue(spec.direction));
    query.addBindValue(nonNullText(spec.localPath));
    query.addBindValue(nonNullText(objectId));
    query.addBindValue(snapshot.objectVersion == 0 ? spec.objectVersion : snapshot.objectVersion);
    query.addBindValue(spec.verifyRoundTrip ? 1 : 0);
    query.addBindValue(nonNullText(spec.commandId));
    query.addBindValue(spec.attempt);
    query.addBindValue(nonNullText(spec.gatewayHost));
    query.addBindValue(spec.gatewayPort);
    query.addBindValue(nonNullText(spec.servicePrincipal));
    query.addBindValue(nonNullText(spec.metadataMode));
    query.addBindValue(nonNullText(spec.targetPath));
    query.addBindValue(static_cast<qulonglong>(spec.sourceSize));
    query.addBindValue(spec.sourceModifiedMs);
    query.addBindValue(static_cast<int>(snapshot.state));
    query.addBindValue(static_cast<qulonglong>(snapshot.completedBytes));
    query.addBindValue(static_cast<qulonglong>(snapshot.totalBytes));
    query.addBindValue(nonNullText(snapshot.result));
    query.addBindValue(nonNullText(snapshot.error));
    query.addBindValue(QDateTime::currentMSecsSinceEpoch());
    if (query.exec()) {
        error_.clear();
        return true;
    }
    error_ = query.lastError().text();
    return false;
}

bool TransferStore::interruptedUploads(QVector<TransferSpec>& result) {
    result.clear();
    if (!available_) return false;
    QSqlQuery query(QSqlDatabase::database(connectionName_));
    if (!query.exec(QStringLiteral(
            "SELECT task_id,local_path,verify_round_trip,command_id,attempt,gateway_host,gateway_port,"
            "service_principal,metadata_mode,target_path,source_size,source_modified_ms "
            "FROM transfer_tasks WHERE direction=0 AND state IN (0,1,2,4) "
            "ORDER BY updated_at_ms ASC"))) {
        error_ = query.lastError().text();
        return false;
    }
    while (query.next()) {
        TransferSpec spec;
        spec.taskId = query.value(0).toString();
        spec.direction = TransferDirection::Upload;
        spec.localPath = query.value(1).toString();
        spec.verifyRoundTrip = query.value(2).toInt() != 0;
        spec.commandId = query.value(3).toString();
        spec.attempt = query.value(4).toInt();
        spec.gatewayHost = query.value(5).toString();
        spec.gatewayPort = static_cast<quint16>(query.value(6).toUInt());
        spec.servicePrincipal = query.value(7).toString();
        spec.metadataMode = query.value(8).toString();
        spec.targetPath = query.value(9).toString();
        spec.sourceSize = query.value(10).toULongLong();
        spec.sourceModifiedMs = query.value(11).toLongLong();
        if (!spec.taskId.isEmpty() && !spec.localPath.isEmpty() && !spec.commandId.isEmpty()) {
            result.push_back(std::move(spec));
        }
    }
    error_.clear();
    return true;
}

}  // namespace miniKV::qtclient
