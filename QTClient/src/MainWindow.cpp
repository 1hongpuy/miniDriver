#include "qtclient/MainWindow.hpp"

#include <QAbstractItemView>
#include <QTabWidget>
#include <QFileDialog>
#include <QFileInfo>
#include <QCloseEvent>
#include <QDateTime>
#include <QDialog>
#include <QFormLayout>
#include <QGridLayout>
#include <QGroupBox>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QJsonDocument>
#include <QJsonObject>
#include <QLabel>
#include <QListView>
#include <QMessageBox>
#include <QDir>
#include <QPushButton>
#include <QSaveFile>
#include <QStandardPaths>
#include <QSplitter>
#include <QStatusBar>
#include <QTextStream>
#include <QVBoxLayout>
#include <QUrl>

#include <algorithm>

namespace miniKV::qtclient {

namespace {

constexpr int kCatalogKindRole = Qt::UserRole;
constexpr int kCatalogPathRole = Qt::UserRole + 1;
constexpr int kCatalogObjectIdRole = Qt::UserRole + 2;
constexpr int kCatalogObjectVersionRole = Qt::UserRole + 3;

QString displaySize(quint64 bytes) {
    static constexpr const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
    double value = static_cast<double>(bytes);
    size_t unit = 0;
    while (value >= 1024.0 && unit + 1 < sizeof(units) / sizeof(units[0])) {
        value /= 1024.0;
        ++unit;
    }
    return unit == 0 ? QStringLiteral("%1 B").arg(bytes)
                     : QStringLiteral("%1 %2").arg(value, 0, 'f', 2).arg(units[unit]);
}

QString itemLeafName(const QString& path) {
    const QString trimmed = path.endsWith(QLatin1Char('/')) && path.size() > 1
        ? path.left(path.size() - 1) : path;
    const int slash = trimmed.lastIndexOf(QLatin1Char('/'));
    return slash >= 0 ? trimmed.mid(slash + 1) : trimmed;
}

}  // namespace

MainWindow::MainWindow(QWidget* parent) : QMainWindow(parent) {
    transferManager_ = new TransferManager(this);
    transferModel_ = new TransferModel(this);
    logModel_ = new LogModel(this);
    const QString logDirectory = QStandardPaths::writableLocation(
        QStandardPaths::AppDataLocation);
    if (!logDirectory.isEmpty() && QDir().mkpath(logDirectory)) {
        logFile_.setFileName(logDirectory + QStringLiteral("/minidriver-qt-client.log"));
        logFile_.open(QIODevice::WriteOnly | QIODevice::Append | QIODevice::Text);
    }
    buildUi();
    loadConnectionSettings();

    heartbeatLabel_ = new QLabel(QStringLiteral("GUI heartbeat: --"), this);
    heartbeatLabel_->setToolTip(QStringLiteral("诊断 Qt GUI EventLoop 是否被传输任务阻塞"));
    statusBar()->addPermanentWidget(heartbeatLabel_);
    heartbeatClock_.start();
    lastHeartbeatMs_ = heartbeatClock_.elapsed();
    heartbeatTimer_ = new QTimer(this);
    heartbeatTimer_->setInterval(100);
    connect(heartbeatTimer_, &QTimer::timeout, this, &MainWindow::updateHeartbeat);
    heartbeatTimer_->start();

    connect(transferManager_, &TransferManager::taskAdded,
            transferModel_, &TransferModel::addTask);
    connect(transferManager_, &TransferManager::taskUpdated,
            transferModel_, &TransferModel::updateTask);
    connect(transferManager_, &TransferManager::taskUpdated,
            this, &MainWindow::showTask);
    connect(transferManager_, &TransferManager::log,
            logModel_, &LogModel::appendEvent);
    connect(transferManager_, &TransferManager::log,
            this, &MainWindow::appendLog);
}

void MainWindow::buildUi() {
    auto* central = new QWidget(this);
    auto* root = new QVBoxLayout(central);

    auto* connectionBox = new QGroupBox(QStringLiteral("Connection"), central);
    auto* connectionForm = new QFormLayout(connectionBox);
    gatewayHost_ = new QLineEdit(QStringLiteral("127.0.0.1"), connectionBox);
    gatewayPort_ = new QLineEdit(QStringLiteral("18080"), connectionBox);
    clusterToken_ = new QLineEdit(connectionBox);
    clusterToken_->setEchoMode(QLineEdit::Password);
    servicePrincipal_ = new QLineEdit(QStringLiteral("qt-client"), connectionBox);
    edgeHost_ = new QLineEdit(QStringLiteral("127.0.0.1"), connectionBox);
    edgePort_ = new QLineEdit(QStringLiteral("19100"), connectionBox);
    metadataMode_ = new QLineEdit(qEnvironmentVariable("MINIKV_METADATA_MODE", QStringLiteral("raft")), connectionBox);
    connectionForm->addRow(QStringLiteral("Gateway host"), gatewayHost_);
    connectionForm->addRow(QStringLiteral("Gateway port"), gatewayPort_);
    connectionForm->addRow(QStringLiteral("Cluster token"), clusterToken_);
    connectionForm->addRow(QStringLiteral("Service principal"), servicePrincipal_);
    connectionForm->addRow(QStringLiteral("Edge host (Step 3 lab)"), edgeHost_);
    connectionForm->addRow(QStringLiteral("Edge port"), edgePort_);
    connectionForm->addRow(QStringLiteral("Metadata mode"), metadataMode_);
    auto* settingsButtons = new QHBoxLayout();
    auto* saveSettings = new QPushButton(QStringLiteral("Save connection settings"), connectionBox);
    auto* reloadSettings = new QPushButton(QStringLiteral("Reload settings"), connectionBox);
    settingsButtons->addWidget(saveSettings);
    settingsButtons->addWidget(reloadSettings);
    settingsButtons->addStretch(1);
    connectionForm->addRow(settingsButtons);
    connect(saveSettings, &QPushButton::clicked, this, &MainWindow::saveConnectionSettings);
    connect(reloadSettings, &QPushButton::clicked, this, &MainWindow::loadConnectionSettings);
    root->addWidget(connectionBox);

    auto* actions = new QGroupBox(QStringLiteral("Object transfer"), central);
    auto* actionLayout = new QGridLayout(actions);
    objectId_ = new QLineEdit(actions);
    objectVersion_ = new QLineEdit(QStringLiteral("1"), actions);
    downloadName_ = new QLineEdit(actions);
    downloadName_->setPlaceholderText(
        QStringLiteral("Select a catalog object to use its original name"));
    auto* upload = new QPushButton(QStringLiteral("Upload file..."), actions);
    auto* download = new QPushButton(QStringLiteral("Download ObjectRef..."), actions);
    auto* openPlayer = new QPushButton(QStringLiteral("Open video player..."), actions);
    verifyRoundTrip_ = new QCheckBox(QStringLiteral("Upload then download + SHA-256 verify"), actions);
    actionLayout->addWidget(new QLabel(QStringLiteral("Object ID"), actions), 0, 0);
    actionLayout->addWidget(objectId_, 0, 1);
    actionLayout->addWidget(new QLabel(QStringLiteral("Version"), actions), 0, 2);
    actionLayout->addWidget(objectVersion_, 0, 3);
    actionLayout->addWidget(new QLabel(QStringLiteral("Download file name"), actions), 1, 0);
    actionLayout->addWidget(downloadName_, 1, 1, 1, 3);
    actionLayout->addWidget(upload, 2, 0, 1, 2);
    actionLayout->addWidget(download, 2, 2, 1, 2);
    actionLayout->addWidget(openPlayer, 3, 0, 1, 4);
    actionLayout->addWidget(verifyRoundTrip_, 4, 0, 1, 4);
    root->addWidget(actions);
    connect(upload, &QPushButton::clicked, this, &MainWindow::chooseUpload);
    connect(download, &QPushButton::clicked, this, &MainWindow::chooseDownload);
    connect(openPlayer, &QPushButton::clicked, this, &MainWindow::showVideoPlayerWindow);

    transferView_ = new QTableView(central);
    transferView_->setModel(transferModel_);
    transferView_->setSelectionMode(QAbstractItemView::NoSelection);
    transferView_->horizontalHeader()->setStretchLastSection(true);
    transferView_->horizontalHeader()->setSectionResizeMode(QHeaderView::ResizeToContents);

    contentTabs_ = new QTabWidget(central);
    auto* tabs = contentTabs_;
    auto* transferTab = new QWidget(tabs);
    auto* transferLayout = new QVBoxLayout(transferTab);
    transferLayout->setContentsMargins(0, 0, 0, 0);
    transferLayout->addWidget(transferView_);
    tabs->addTab(transferTab, QStringLiteral("Transfers"));

    auto* catalogTab = new QWidget(tabs);
    auto* catalogLayout = new QVBoxLayout(catalogTab);
    catalogLayout->setContentsMargins(6, 6, 6, 6);
    auto* catalogControls = new QHBoxLayout();
    catalogPath_ = new QLineEdit(QStringLiteral("/"), catalogTab);
    catalogRefreshButton_ = new QPushButton(QStringLiteral("Refresh"), catalogTab);
    auto* catalogUpButton = new QPushButton(QStringLiteral("Up"), catalogTab);
    auto* catalogDownloadButton = new QPushButton(QStringLiteral("Download selected..."), catalogTab);
    auto* catalogPreviewButton = new QPushButton(QStringLiteral("Download + preview"), catalogTab);
    auto* catalogPlayButton = new QPushButton(QStringLiteral("Play via Edge (lab)"), catalogTab);
    catalogControls->addWidget(new QLabel(QStringLiteral("Path"), catalogTab));
    catalogControls->addWidget(catalogPath_, 1);
    catalogControls->addWidget(catalogRefreshButton_);
    catalogControls->addWidget(catalogUpButton);
    catalogControls->addWidget(catalogDownloadButton);
    catalogControls->addWidget(catalogPreviewButton);
    catalogControls->addWidget(catalogPlayButton);
    catalogLayout->addLayout(catalogControls);
    auto* catalogNotice = new QLabel(
        QStringLiteral("Trusted-cluster catalog: current Gateway catalog API; not a multi-user ACL browser."),
        catalogTab);
    catalogNotice->setWordWrap(true);
    catalogLayout->addWidget(catalogNotice);
    catalogView_ = new QTreeWidget(catalogTab);
    catalogView_->setHeaderLabels({QStringLiteral("Name"), QStringLiteral("Type"),
                                   QStringLiteral("Size"), QStringLiteral("Version"),
                                   QStringLiteral("State"), QStringLiteral("Path / ObjectRef")});
    catalogView_->setSelectionMode(QAbstractItemView::SingleSelection);
    catalogView_->setRootIsDecorated(false);
    catalogView_->setAlternatingRowColors(true);
    catalogView_->header()->setStretchLastSection(true);
    catalogLayout->addWidget(catalogView_, 1);
    tabs->addTab(catalogTab, QStringLiteral("Virtual directory"));

    connect(catalogRefreshButton_, &QPushButton::clicked, this, &MainWindow::refreshCatalog);
    connect(catalogPath_, &QLineEdit::returnPressed, this, &MainWindow::refreshCatalog);
    connect(catalogUpButton, &QPushButton::clicked, this, &MainWindow::catalogUp);
    connect(catalogDownloadButton, &QPushButton::clicked,
            this, &MainWindow::downloadSelectedCatalogObject);
    connect(catalogPreviewButton, &QPushButton::clicked,
            this, &MainWindow::previewSelectedCatalogObject);
    connect(catalogPlayButton, &QPushButton::clicked,
            this, &MainWindow::playSelectedCatalogObject);
    connect(catalogView_, &QTreeWidget::itemActivated, this, &MainWindow::activateCatalogItem);
    connect(catalogView_, &QTreeWidget::currentItemChanged, this,
            [this](QTreeWidgetItem* current, QTreeWidgetItem*) {
                if (current == nullptr ||
                    current->data(0, kCatalogKindRole).toString() != QStringLiteral("file")) {
                    return;
                }
                const QString objectId = current->data(0, kCatalogObjectIdRole).toString();
                const quint64 version = current->data(0, kCatalogObjectVersionRole).toULongLong();
                if (!objectId.isEmpty() && version > 0) {
                    objectId_->setText(objectId);
                    objectVersion_->setText(QString::number(version));
                }
                // Catalog names are virtual names. Keep only the leaf before
                // proposing one as a local output path.
                downloadName_->setText(QFileInfo(current->text(0)).fileName());
            });

    auto* logView = new QListView(central);
    logView->setModel(logModel_);
    logView->setMinimumHeight(130);
    auto* splitter = new QSplitter(Qt::Vertical, central);
    splitter->addWidget(tabs);
    splitter->addWidget(logView);
    splitter->setStretchFactor(0, 4);
    splitter->setStretchFactor(1, 1);
    splitter->setCollapsible(0, false);
    splitter->setCollapsible(1, false);
    splitter->setSizes({620, 170});
    root->addWidget(splitter, 1);

    setCentralWidget(central);
    setWindowTitle(QStringLiteral("MiniDriver Qt Client"));
    resize(1180, 720);
    statusBar()->showMessage(QStringLiteral("Ready"));
}

VideoPlayerWidget* MainWindow::ensureVideoPlayerWindow() {
    if (videoPlayerWindow_ != nullptr && videoPlayer_ != nullptr) return videoPlayer_;

    auto* dialog = new QDialog(this);
    dialog->setAttribute(Qt::WA_DeleteOnClose);
    dialog->setModal(false);
    dialog->setWindowTitle(QStringLiteral("MiniDriver Video Player"));
    dialog->resize(1080, 700);
    dialog->setMinimumSize(720, 480);

    auto* layout = new QVBoxLayout(dialog);
    layout->setContentsMargins(0, 0, 0, 0);
    auto* player = new VideoPlayerWidget(dialog);
    layout->addWidget(player);
    connect(player, &VideoPlayerWidget::log, this, [this](const QString& message) {
        appendLog({QString(), QStringLiteral("player"), message});
    });
    connect(dialog, &QObject::destroyed, this, [this] {
        videoPlayerWindow_ = nullptr;
        videoPlayer_ = nullptr;
        appendLog({QString(), QStringLiteral("player"), QStringLiteral("player window closed")});
    });
    videoPlayerWindow_ = dialog;
    videoPlayer_ = player;
    return player;
}

void MainWindow::showVideoPlayerWindow() {
    VideoPlayerWidget* player = ensureVideoPlayerWindow();
    videoPlayerWindow_->show();
    videoPlayerWindow_->raise();
    videoPlayerWindow_->activateWindow();
    if (player->currentLocalFile().isEmpty()) {
        statusBar()->showMessage(QStringLiteral("Video player opened; select a local media file in the player window."));
    }
}

QString MainWindow::connectionSettingsPath() const {
    const QString directory = QStandardPaths::writableLocation(QStandardPaths::AppConfigLocation);
    return directory.isEmpty() ? QString() : QDir(directory).filePath(QStringLiteral("connection.json"));
}

void MainWindow::saveConnectionSettings() {
    const QString path = connectionSettingsPath();
    if (path.isEmpty() || !QDir().mkpath(QFileInfo(path).absolutePath())) {
        QMessageBox::warning(this, QStringLiteral("Settings"), QStringLiteral("Cannot create the local settings directory."));
        return;
    }
    const QString mode = metadataMode_->text().trimmed();
    if (mode.isEmpty()) {
        QMessageBox::warning(this, QStringLiteral("Settings"), QStringLiteral("Metadata mode cannot be empty."));
        return;
    }
    QJsonObject settings;
    settings.insert(QStringLiteral("schemaVersion"), 1);
    settings.insert(QStringLiteral("gatewayHost"), gatewayHost_->text().trimmed());
    settings.insert(QStringLiteral("gatewayPort"), gatewayPort_->text().trimmed());
    settings.insert(QStringLiteral("servicePrincipal"), servicePrincipal_->text().trimmed());
    settings.insert(QStringLiteral("edgeHost"), edgeHost_->text().trimmed());
    settings.insert(QStringLiteral("edgePort"), edgePort_->text().trimmed());
    settings.insert(QStringLiteral("metadataMode"), mode);
    settings.insert(QStringLiteral("clusterTokenEnv"), QStringLiteral("MINIDRIVER_QT_CLUSTER_TOKEN"));
    QSaveFile output(path);
    if (!output.open(QIODevice::WriteOnly) || output.write(QJsonDocument(settings).toJson(QJsonDocument::Indented)) < 0 || !output.commit()) {
        QMessageBox::warning(this, QStringLiteral("Settings"), QStringLiteral("Cannot save connection settings: %1").arg(output.errorString()));
        return;
    }
    qputenv("MINIKV_METADATA_MODE", mode.toUtf8());
    statusBar()->showMessage(QStringLiteral("Connection settings saved: %1").arg(path), 5000);
}

void MainWindow::loadConnectionSettings() {
    const QString path = connectionSettingsPath();
    if (path.isEmpty() || !QFileInfo::exists(path)) return;
    QFile input(path);
    if (!input.open(QIODevice::ReadOnly)) {
        statusBar()->showMessage(QStringLiteral("Cannot read connection settings: %1").arg(input.errorString()), 5000);
        return;
    }
    QJsonParseError parseError;
    const QJsonDocument document = QJsonDocument::fromJson(input.readAll(), &parseError);
    if (parseError.error != QJsonParseError::NoError || !document.isObject()) {
        statusBar()->showMessage(QStringLiteral("Invalid connection settings JSON: %1").arg(parseError.errorString()), 5000);
        return;
    }
    const QJsonObject settings = document.object();
    const auto apply = [&settings](QLineEdit* field, const char* key) {
        const QString value = settings.value(QLatin1String(key)).toString();
        if (!value.isEmpty()) field->setText(value);
    };
    apply(gatewayHost_, "gatewayHost");
    apply(gatewayPort_, "gatewayPort");
    apply(servicePrincipal_, "servicePrincipal");
    apply(edgeHost_, "edgeHost");
    apply(edgePort_, "edgePort");
    apply(metadataMode_, "metadataMode");
    const QString token = qEnvironmentVariable("MINIDRIVER_QT_CLUSTER_TOKEN");
    if (!token.isEmpty()) clusterToken_->setText(token);
    const QString mode = metadataMode_->text().trimmed();
    if (!mode.isEmpty()) qputenv("MINIKV_METADATA_MODE", mode.toUtf8());
    statusBar()->showMessage(QStringLiteral("Connection settings loaded%1").arg(token.isEmpty() ? QStringLiteral("; token still required") : QString()), 5000);
}

miniKV::client::ClientConfig MainWindow::clientConfig() const {
    miniKV::client::ClientConfig config;
    config.gateway.host = gatewayHost_->text().trimmed().toStdString();
    bool validPort = false;
    const uint16_t port = gatewayPort_->text().toUShort(&validPort);
    config.gateway.port = validPort ? port : 0;
    config.clusterInternalToken = clusterToken_->text().toStdString();
    config.servicePrincipal = servicePrincipal_->text().trimmed().toStdString();
    const QString mode = metadataMode_->text().trimmed();
    if (!mode.isEmpty()) qputenv("MINIKV_METADATA_MODE", mode.toUtf8());
    return config;
}

void MainWindow::chooseUpload() {
    const QString path = QFileDialog::getOpenFileName(this, QStringLiteral("Select file to upload"));
    if (path.isEmpty()) return;
    downloadName_->setText(QFileInfo(path).fileName());
    const QString taskId = transferManager_->enqueueUpload(path, clientConfig(),
                                                            verifyRoundTrip_->isChecked());
    statusBar()->showMessage(QStringLiteral("Upload queued: %1").arg(taskId));
}

void MainWindow::chooseDownload() {
    bool validVersion = false;
    const quint64 version = objectVersion_->text().toULongLong(&validVersion);
    if (objectId_->text().trimmed().isEmpty() || !validVersion || version == 0) {
        QMessageBox::warning(this, QStringLiteral("Invalid ObjectRef"),
                             QStringLiteral("Enter a non-empty object ID and positive version."));
        return;
    }
    QString suggestedName = QFileInfo(downloadName_->text().trimmed()).fileName();
    if (suggestedName.isEmpty()) {
        suggestedName = QStringLiteral("%1-v%2.bin")
            .arg(objectId_->text().trimmed()).arg(version);
    }
    const QString output = QFileDialog::getSaveFileName(
        this, QStringLiteral("Download object to"), suggestedName);
    if (output.isEmpty()) return;
    const QString taskId = transferManager_->enqueueDownload(
        objectId_->text().trimmed(), version, output, clientConfig());
    statusBar()->showMessage(QStringLiteral("Download queued: %1").arg(taskId));
}

void MainWindow::showTask(const TransferSnapshot& snapshot) {
    if (snapshot.state == TransferState::Completed) {
        if (!snapshot.objectId.isEmpty()) {
            objectId_->setText(snapshot.objectId);
            objectVersion_->setText(QString::number(snapshot.objectVersion));
        }
        const bool previewCompleted = previewTaskIds_.remove(snapshot.taskId);
        if (previewCompleted) {
            VideoPlayerWidget* player = ensureVideoPlayerWindow();
            videoPlayerWindow_->show();
            videoPlayerWindow_->raise();
            videoPlayerWindow_->activateWindow();
            if (player->openLocalFile(snapshot.localPath)) {
                player->play();
                statusBar()->showMessage(QStringLiteral("Previewing downloaded object: %1")
                    .arg(snapshot.localPath));
            }
        }
        if (snapshot.direction == TransferDirection::Upload) {
            downloadName_->setText(QFileInfo(snapshot.localPath).fileName());
            if (contentTabs_ != nullptr) contentTabs_->setCurrentIndex(1);
            refreshCatalog();
        }
        if (!previewCompleted) statusBar()->showMessage(snapshot.result);
    } else if (snapshot.state == TransferState::Failed) {
        previewTaskIds_.remove(snapshot.taskId);
        statusBar()->showMessage(QStringLiteral("Task failed: %1").arg(snapshot.error));
    } else if (snapshot.state == TransferState::Verifying) {
        statusBar()->showMessage(QStringLiteral("Verifying %1").arg(snapshot.displayName));
    }
}

void MainWindow::refreshCatalog() {
    if (catalogLoading_) {
        statusBar()->showMessage(QStringLiteral("Catalog refresh already running"));
        return;
    }
    QString path = catalogPath_->text().trimmed();
    if (path.isEmpty()) path = QStringLiteral("/");
    if (!path.startsWith(QLatin1Char('/'))) {
        QMessageBox::warning(this, QStringLiteral("Invalid catalog path"),
                             QStringLiteral("Catalog path must start with '/'."));
        return;
    }
    catalogLoading_ = true;
    catalogRefreshButton_->setEnabled(false);
    auto* thread = new QThread(this);
    auto* worker = new CatalogWorker(path, clientConfig());
    worker->moveToThread(thread);
    catalogThread_ = thread;
    connect(thread, &QThread::started, worker, &CatalogWorker::start);
    connect(worker, &CatalogWorker::loaded, this, &MainWindow::catalogLoaded,
            Qt::QueuedConnection);
    connect(worker, &CatalogWorker::failed, this, &MainWindow::catalogFailed,
            Qt::QueuedConnection);
    connect(worker, &CatalogWorker::finished, worker, &QObject::deleteLater);
    connect(worker, &CatalogWorker::finished, thread, &QThread::quit,
            Qt::DirectConnection);
    connect(thread, &QThread::finished, this, [this, thread] {
        if (catalogThread_ == thread) {
            catalogThread_ = nullptr;
            catalogLoading_ = false;
            catalogRefreshButton_->setEnabled(true);
        }
        thread->deleteLater();
    });
    appendLog({QString(), QStringLiteral("catalog"), QStringLiteral("refreshing %1").arg(path)});
    thread->start();
}

void MainWindow::catalogUp() {
    QString path = catalogPath_->text().trimmed();
    if (path.isEmpty() || path == QStringLiteral("/")) {
        catalogPath_->setText(QStringLiteral("/"));
        return;
    }
    if (path.endsWith(QLatin1Char('/'))) path.chop(1);
    const int slash = path.lastIndexOf(QLatin1Char('/'));
    catalogPath_->setText(slash <= 0 ? QStringLiteral("/") : path.left(slash));
    refreshCatalog();
}

void MainWindow::activateCatalogItem(QTreeWidgetItem* item, int) {
    if (item == nullptr) return;
    const QString kind = item->data(0, kCatalogKindRole).toString();
    if (kind == QStringLiteral("directory")) {
        catalogPath_->setText(item->data(0, kCatalogPathRole).toString());
        refreshCatalog();
        return;
    }
    QString objectId;
    quint64 version = 0;
    QString name;
    if (!selectedCatalogObject(objectId, version, name)) return;
    objectId_->setText(objectId);
    objectVersion_->setText(QString::number(version));
    downloadName_->setText(QFileInfo(name).fileName());
    statusBar()->showMessage(QStringLiteral("Selected ObjectRef(%1, v%2)").arg(objectId).arg(version));
}

bool MainWindow::selectedCatalogObject(QString& objectId, quint64& version, QString& name) {
    auto* item = catalogView_->currentItem();
    if (item == nullptr || item->data(0, kCatalogKindRole).toString() != QStringLiteral("file")) {
        QMessageBox::information(this, QStringLiteral("Select an object"),
                                 QStringLiteral("Select a file entry in the virtual directory first."));
        return false;
    }
    objectId = item->data(0, kCatalogObjectIdRole).toString();
    version = item->data(0, kCatalogObjectVersionRole).toULongLong();
    name = item->text(0);
    return !objectId.isEmpty() && version > 0;
}

void MainWindow::downloadSelectedCatalogObject() {
    QString objectId;
    quint64 version = 0;
    QString name;
    if (!selectedCatalogObject(objectId, version, name)) return;
    const QString suggestedName = QFileInfo(name).fileName();
    downloadName_->setText(suggestedName);
    const QString output = QFileDialog::getSaveFileName(
        this, QStringLiteral("Download object to"), suggestedName);
    if (output.isEmpty()) return;
    const QString taskId = transferManager_->enqueueDownload(objectId, version, output, clientConfig());
    statusBar()->showMessage(QStringLiteral("Download queued: %1").arg(taskId));
}

QString MainWindow::previewOutputPath(const QString& objectId, quint64 version, const QString& name) const {
    QString root = QStandardPaths::writableLocation(QStandardPaths::MoviesLocation);
    if (root.isEmpty()) root = QStandardPaths::writableLocation(QStandardPaths::AppDataLocation);
    if (root.isEmpty()) return {};
    root += QStringLiteral("/MiniDriver/previews");
    if (!QDir().mkpath(root)) return {};
    QString safeId = objectId;
    for (QChar& character : safeId) {
        if (!character.isLetterOrNumber() && character != QLatin1Char('-') && character != QLatin1Char('_')) {
            character = QLatin1Char('_');
        }
    }
    QString fileName = QFileInfo(name).fileName();
    if (fileName.isEmpty()) fileName = QStringLiteral("object.bin");
    return root + QStringLiteral("/%1-v%2-%3").arg(safeId).arg(version).arg(fileName);
}

void MainWindow::previewSelectedCatalogObject() {
    QString objectId;
    quint64 version = 0;
    QString name;
    if (!selectedCatalogObject(objectId, version, name)) return;
    const QString output = previewOutputPath(objectId, version, name);
    if (output.isEmpty()) {
        QMessageBox::warning(this, QStringLiteral("Preview unavailable"),
                             QStringLiteral("Cannot create a local preview directory."));
        return;
    }
    const QString taskId = transferManager_->enqueueDownload(objectId, version, output, clientConfig());
    previewTaskIds_.insert(taskId);
    statusBar()->showMessage(QStringLiteral("Preview download queued: %1").arg(taskId));
}

QUrl MainWindow::edgePlaybackUrl(const QString& objectId, quint64 version, QString& error) const {
    const QString host = edgeHost_->text().trimmed();
    bool validPort = false;
    const quint16 port = edgePort_->text().toUShort(&validPort);
    if (host.isEmpty() || !validPort || port == 0 || version == 0) {
        error = QStringLiteral("Enter a valid Edge host, port, and ObjectRef.");
        return {};
    }
    const QByteArray escapedObjectId = QUrl::toPercentEncoding(objectId);
    QUrl url;
    url.setScheme(QStringLiteral("http"));
    url.setHost(host);
    url.setPort(port);
    url.setPath(QStringLiteral("/v1/play/") + QString::fromLatin1(escapedObjectId) +
                QStringLiteral("/") + QString::number(version));
    if (!url.isValid()) error = QStringLiteral("Cannot construct Edge playback URL.");
    return url;
}

void MainWindow::playSelectedCatalogObject() {
    QString objectId;
    quint64 version = 0;
    QString name;
    if (!selectedCatalogObject(objectId, version, name)) return;
    QString failure;
    const QUrl url = edgePlaybackUrl(objectId, version, failure);
    if (!url.isValid()) {
        QMessageBox::warning(this, QStringLiteral("Invalid Edge endpoint"), failure);
        return;
    }
    VideoPlayerWidget* player = ensureVideoPlayerWindow();
    videoPlayerWindow_->show();
    videoPlayerWindow_->raise();
    videoPlayerWindow_->activateWindow();
    if (player->openNetworkUrl(url)) {
        player->play();
        statusBar()->showMessage(QStringLiteral("Opening %1 through Edge HTTP Range").arg(name));
    }
}

void MainWindow::catalogLoaded(const CatalogSnapshot& snapshot) {
    catalogView_->clear();
    catalogPath_->setText(snapshot.path);
    for (const CatalogDirectory& directory : snapshot.directories) {
        auto* item = new QTreeWidgetItem(catalogView_);
        item->setText(0, itemLeafName(directory.path));
        item->setText(1, QStringLiteral("Directory"));
        item->setText(5, directory.path);
        item->setData(0, kCatalogKindRole, QStringLiteral("directory"));
        item->setData(0, kCatalogPathRole, directory.path);
    }
    for (const CatalogFile& file : snapshot.files) {
        auto* item = new QTreeWidgetItem(catalogView_);
        item->setText(0, file.name.isEmpty() ? file.objectId : file.name);
        item->setText(1, QStringLiteral("Object"));
        item->setText(2, displaySize(file.fileSize));
        item->setText(3, QString::number(file.objectVersion));
        item->setText(4, file.state);
        item->setText(5, QStringLiteral("ObjectRef(%1, v%2)")
            .arg(file.objectId).arg(file.objectVersion));
        item->setData(0, kCatalogKindRole, QStringLiteral("file"));
        item->setData(0, kCatalogObjectIdRole, file.objectId);
        item->setData(0, kCatalogObjectVersionRole, file.objectVersion);
    }
    catalogView_->resizeColumnToContents(0);
    catalogView_->resizeColumnToContents(1);
    catalogView_->resizeColumnToContents(2);
    catalogView_->resizeColumnToContents(3);
    catalogView_->resizeColumnToContents(4);
    appendLog({QString(), QStringLiteral("catalog"), QStringLiteral("loaded %1: %2 directories, %3 objects")
        .arg(snapshot.path).arg(snapshot.directories.size()).arg(snapshot.files.size())});
    statusBar()->showMessage(QStringLiteral("Catalog loaded: %1 objects").arg(snapshot.files.size()));
}

void MainWindow::catalogFailed(const QString& path, const QString& error) {
    appendLog({QString(), QStringLiteral("catalog"), QStringLiteral("%1: %2").arg(path, error)});
    statusBar()->showMessage(QStringLiteral("Catalog failed: %1").arg(error));
}

void MainWindow::updateHeartbeat() {
    const qint64 now = heartbeatClock_.elapsed();
    const qint64 gap = now - lastHeartbeatMs_;
    lastHeartbeatMs_ = now;
    maxHeartbeatGapMs_ = (std::max)(maxHeartbeatGapMs_, gap);
    if (heartbeatLabel_) {
        heartbeatLabel_->setText(QStringLiteral("GUI heartbeat: %1 ms (max %2 ms)")
            .arg(gap).arg(maxHeartbeatGapMs_));
    }
}

void MainWindow::appendLog(const LogEvent& event) {
    statusBar()->showMessage(event.message, 5000);
    if (!logFile_.isOpen()) return;
    QString message = event.message;
    // Do not persist the cluster secret if a lower layer ever includes it in
    // a diagnostic string. Upload/read capabilities are never supplied to
    // the Qt layer, so they cannot be logged here.
    const QString secret = clusterToken_->text();
    if (!secret.isEmpty()) message.replace(secret, QStringLiteral("<redacted>"));
    QTextStream stream(&logFile_);
    stream << QDateTime::currentDateTime().toString(Qt::ISODate) << " ["
           << (event.taskId.isEmpty() ? QStringLiteral("client") : event.taskId)
           << "] " << event.stage << ": " << message << Qt::endl;
    logFile_.flush();
}

void MainWindow::closeEvent(QCloseEvent* event) {
    if (catalogLoading_) {
        QMessageBox::information(this, QStringLiteral("Catalog request is active"),
                                 QStringLiteral("Wait for the current catalog refresh to finish."));
        event->ignore();
        return;
    }
    if (!transferManager_->hasActiveTasks()) {
        event->accept();
        return;
    }
    const auto answer = QMessageBox::question(
        this, QStringLiteral("Transfers are active"),
        QStringLiteral("A synchronous transfer is still running. Wait for it to finish and close?"),
        QMessageBox::Yes | QMessageBox::No, QMessageBox::No);
    if (answer == QMessageBox::Yes) event->accept();
    else event->ignore();
}

}  // namespace miniKV::qtclient
