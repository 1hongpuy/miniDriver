#include "qtclient/TransferStore.hpp"

#include <QCoreApplication>
#include <QDebug>
#include <QTemporaryDir>

namespace {

bool check(bool condition, const char* message) {
    if (condition) return true;
    qCritical().noquote() << message;
    return false;
}

}  // namespace

int main(int argc, char* argv[]) {
    QTemporaryDir temporary;
    if (!temporary.isValid()) {
        qCritical() << "cannot create temporary Qt transfer-store test directory";
        return 1;
    }
    qputenv("XDG_DATA_HOME", (temporary.path() + QStringLiteral("/data")).toUtf8());
    QCoreApplication application(argc, argv);
    application.setOrganizationName(QStringLiteral("minidriver-test"));
    application.setApplicationName(QStringLiteral("transfer-store"));

    miniKV::qtclient::TransferSpec expected;
    expected.taskId = QStringLiteral("task-1");
    expected.direction = miniKV::qtclient::TransferDirection::Upload;
    expected.localPath = QStringLiteral("/tmp/source.mov");
    expected.verifyRoundTrip = true;
    expected.commandId = QStringLiteral("qt-upload-task-1");
    expected.attempt = 1;

    expected.gatewayHost = QStringLiteral("100.89.50.125");
    expected.gatewayPort = 30280;
    expected.servicePrincipal = QStringLiteral("qt-windows-smoke");
    expected.metadataMode = QStringLiteral("raft");
    expected.targetPath = QStringLiteral("/");
    expected.sourceSize = 844657979;
    expected.sourceModifiedMs = 1791200000123;
    {
        miniKV::qtclient::TransferStore store;
        if (!check(store.available(), "SQLite transfer store is unavailable")) return 1;
        miniKV::qtclient::TransferSnapshot queued;
        queued.taskId = expected.taskId;
        queued.direction = expected.direction;
        queued.state = miniKV::qtclient::TransferState::Queued;
        queued.localPath = expected.localPath;
        queued.stage = QStringLiteral("queued");

        if (!check(store.upsert(expected, queued), "empty queued snapshot was not persisted")) return 1;
        QVector<miniKV::qtclient::TransferSpec> restored;
        if (!check(store.interruptedUploads(restored), "cannot read persisted queued snapshot")) return 1;
        if (!check(restored.size() == 1, "queued upload was not returned for recovery")) return 1;
        if (!check(restored[0].taskId == expected.taskId &&
                   restored[0].commandId == expected.commandId &&
                   restored[0].localPath == expected.localPath,
                   "recovered upload identity differs from persisted identity")) return 1;
        if (!check(restored[0].gatewayHost == expected.gatewayHost &&
                   restored[0].gatewayPort == expected.gatewayPort &&
                   restored[0].servicePrincipal == expected.servicePrincipal &&
                   restored[0].metadataMode == expected.metadataMode &&
                   restored[0].targetPath == expected.targetPath &&
                   restored[0].sourceSize == expected.sourceSize &&
                   restored[0].sourceModifiedMs == expected.sourceModifiedMs,
                   "recovered upload recovery fields differ from persisted values")) return 1;
    }

    {
        miniKV::qtclient::TransferStore reopened;
        if (!check(reopened.available(), "reopened SQLite transfer store is unavailable")) return 1;
        QVector<miniKV::qtclient::TransferSpec> restored;
        if (!check(reopened.interruptedUploads(restored), "cannot read journal after reopening it")) return 1;
        if (!check(restored.size() == 1 && restored[0].commandId == expected.commandId,
                   "queued upload was not durable across transfer-store reopen")) return 1;
    }
    return 0;
}
