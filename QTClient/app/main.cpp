#include "qtclient/MainWindow.hpp"

#include <QApplication>
#include <QCoreApplication>

int main(int argc, char* argv[]) {
    QApplication app(argc, argv);
    QCoreApplication::setOrganizationName(QStringLiteral("MiniDriver"));
    QCoreApplication::setApplicationName(QStringLiteral("minidriver_qt_client"));
    qRegisterMetaType<miniKV::qtclient::TransferSnapshot>();
    qRegisterMetaType<miniKV::qtclient::LogEvent>();
    qRegisterMetaType<miniKV::qtclient::CatalogSnapshot>();
    miniKV::qtclient::MainWindow window;
    window.show();
    return app.exec();
}
