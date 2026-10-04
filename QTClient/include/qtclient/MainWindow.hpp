#pragma once

#include "qtclient/CatalogTypes.hpp"
#include "qtclient/CatalogWorker.hpp"
#include "qtclient/LogModel.hpp"
#include "qtclient/TransferManager.hpp"
#include "qtclient/TransferModel.hpp"
#include "qtclient/VideoPlayerWidget.hpp"

#include <QLineEdit>
#include <QCloseEvent>
#include <QCheckBox>
#include <QFile>
#include <QMainWindow>
#include <QPointer>
#include <QElapsedTimer>
#include <QLabel>
#include <QTableView>
#include <QTabWidget>
#include <QTreeWidget>
#include <QTimer>
#include <QSet>
#include <QThread>
#include <QUrl>

class QDialog;

namespace miniKV::qtclient {

class MainWindow final : public QMainWindow {
    Q_OBJECT

public:
    explicit MainWindow(QWidget* parent = nullptr);

protected:
    void closeEvent(QCloseEvent* event) override;

private slots:
    void chooseUpload();
    void chooseDownload();
    void showTask(const miniKV::qtclient::TransferSnapshot& snapshot);
    void appendLog(const miniKV::qtclient::LogEvent& event);
    void updateHeartbeat();
    void refreshCatalog();
    void catalogUp();
    void activateCatalogItem(QTreeWidgetItem* item, int column);
    void downloadSelectedCatalogObject();
    void previewSelectedCatalogObject();
    void playSelectedCatalogObject();
    void catalogLoaded(const miniKV::qtclient::CatalogSnapshot& snapshot);
    void catalogFailed(const QString& path, const QString& error);
    void showVideoPlayerWindow();
    void saveConnectionSettings();
    void loadConnectionSettings();

private:
    miniKV::client::ClientConfig clientConfig() const;
    QString connectionSettingsPath() const;
    void buildUi();
    VideoPlayerWidget* ensureVideoPlayerWindow();
    QUrl edgePlaybackUrl(const QString& objectId, quint64 version, QString& error) const;
    bool selectedCatalogObject(QString& objectId, quint64& version, QString& name);
    QString previewOutputPath(const QString& objectId, quint64 version, const QString& name) const;

    QLineEdit* gatewayHost_ = nullptr;
    QLineEdit* gatewayPort_ = nullptr;
    QLineEdit* clusterToken_ = nullptr;
    QLineEdit* servicePrincipal_ = nullptr;
    QLineEdit* edgeHost_ = nullptr;
    QLineEdit* edgePort_ = nullptr;
    QLineEdit* metadataMode_ = nullptr;
    QLineEdit* objectId_ = nullptr;
    QLineEdit* objectVersion_ = nullptr;
    QLineEdit* catalogPath_ = nullptr;
    QCheckBox* verifyRoundTrip_ = nullptr;
    TransferModel* transferModel_ = nullptr;
    LogModel* logModel_ = nullptr;
    TransferManager* transferManager_ = nullptr;
    QTableView* transferView_ = nullptr;
    QTabWidget* contentTabs_ = nullptr;
    QTreeWidget* catalogView_ = nullptr;
    QPushButton* catalogRefreshButton_ = nullptr;
    QFile logFile_;
    QTimer* heartbeatTimer_ = nullptr;
    QElapsedTimer heartbeatClock_;
    qint64 lastHeartbeatMs_ = 0;
    qint64 maxHeartbeatGapMs_ = 0;
    QLabel* heartbeatLabel_ = nullptr;
    QPointer<QDialog> videoPlayerWindow_;
    QPointer<VideoPlayerWidget> videoPlayer_;
    QThread* catalogThread_ = nullptr;
    bool catalogLoading_ = false;
    QSet<QString> previewTaskIds_;
};

}  // namespace miniKV::qtclient
