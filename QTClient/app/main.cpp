#include "qtclient/MainWindow.hpp"

#include <QApplication>
#include <QCoreApplication>
#include <QDir>
#include <QLockFile>
#include <QMessageBox>
#include <QStandardPaths>

int main(int argc, char* argv[]) {
    QApplication app(argc, argv);
    QCoreApplication::setOrganizationName(QStringLiteral("MiniDriver"));
    QCoreApplication::setApplicationName(QStringLiteral("minidriver_qt_client"));
    qRegisterMetaType<miniKV::qtclient::TransferSnapshot>();
    const QString appDataPath = QStandardPaths::writableLocation(QStandardPaths::AppDataLocation);
    if (appDataPath.isEmpty() || !QDir().mkpath(appDataPath)) {
        QMessageBox::critical(nullptr, QStringLiteral("MiniDriver Qt Client"),
                              QStringLiteral("Cannot create the application data directory. The client will not start because transfer recovery would be unsafe."));
        return 1;
    }
    QLockFile instanceLock(appDataPath + QStringLiteral("/client.lock"));
    if (!instanceLock.tryLock(100)) {
        QMessageBox::information(nullptr, QStringLiteral("MiniDriver Qt Client"),
                                 QStringLiteral("Another MiniDriver Qt Client instance is already using this task journal. Close that instance before starting another one."));
        return 0;
    }

    qRegisterMetaType<miniKV::qtclient::LogEvent>();
    qRegisterMetaType<miniKV::qtclient::CatalogSnapshot>();
    miniKV::qtclient::MainWindow window;
    window.show();
    return app.exec();
}
