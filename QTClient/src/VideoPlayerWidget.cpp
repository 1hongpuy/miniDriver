#include "qtclient/VideoPlayerWidget.hpp"

#include <QAudioOutput>
#include <QComboBox>
#include <QFileDialog>
#include <QFileInfo>
#include <QHBoxLayout>
#include <QLabel>
#include <QMediaMetaData>
#include <QMessageBox>
#include <QPushButton>
#include <QSlider>
#include <QStackedLayout>
#include <QTimer>
#include <QUrl>
#include <QVideoFrame>
#include <QVideoSink>
#include <QVBoxLayout>
#include <QVideoWidget>

#include <algorithm>
#include <limits>

namespace miniKV::qtclient {

namespace {

int sliderValue(qint64 milliseconds) {
    return static_cast<int>(std::clamp<qint64>(
        milliseconds, 0, std::numeric_limits<int>::max()));
}

QString reportedText(const QVariant& value, const QString& fallback = QStringLiteral("not reported")) {
    const QString text = value.toString().trimmed();
    return text.isEmpty() ? fallback : text;
}

QVariant firstReportedValue(const QMediaMetaData& primary,
                            const QList<QMediaMetaData>& tracks,
                            QMediaMetaData::Key key) {
    const QVariant primaryValue = primary.value(key);
    if (primaryValue.isValid() && !primaryValue.isNull()) return primaryValue;
    for (const QMediaMetaData& track : tracks) {
        const QVariant value = track.value(key);
        if (value.isValid() && !value.isNull()) return value;
    }
    return {};
}

QString bitRateText(const QVariant& value) {
    bool valid = false;
    const qlonglong bitsPerSecond = value.toLongLong(&valid);
    if (!valid || bitsPerSecond <= 0) return QStringLiteral("not reported");
    return QStringLiteral("%1 Mbps").arg(
        static_cast<double>(bitsPerSecond) / 1000.0 / 1000.0, 0, 'f', 2);
}

}  // namespace

VideoPlayerWidget::VideoPlayerWidget(QWidget* parent) : QWidget(parent) {
    player_ = new QMediaPlayer(this);
    audioOutput_ = new QAudioOutput(this);
    player_->setAudioOutput(audioOutput_);
    buildUi();
    player_->setVideoOutput(videoOutput_);
    diagnosticsClock_.start();
    diagnosticsTimer_ = new QTimer(this);
    diagnosticsTimer_->setInterval(250);

    connect(openButton_, &QPushButton::clicked, this, &VideoPlayerWidget::chooseLocalFile);
    connect(playPauseButton_, &QPushButton::clicked, this, &VideoPlayerWidget::togglePlayback);
    connect(stopButton_, &QPushButton::clicked, this, &VideoPlayerWidget::stopPlayback);
    connect(positionSlider_, &QSlider::sliderPressed, this, &VideoPlayerWidget::beginSeek);
    connect(positionSlider_, &QSlider::sliderReleased, this, &VideoPlayerWidget::endSeek);
    connect(positionSlider_, &QSlider::sliderMoved, this, &VideoPlayerWidget::seekTo);
    connect(volumeSlider_, &QSlider::valueChanged, this, [this](int value) {
        audioOutput_->setVolume(static_cast<qreal>(value) / 100.0);
    });
    connect(muteButton_, &QPushButton::toggled, this, &VideoPlayerWidget::toggleMuted);
    connect(playbackRateBox_, &QComboBox::currentTextChanged,
            this, &VideoPlayerWidget::setPlaybackRate);
    connect(player_, &QMediaPlayer::positionChanged, this, &VideoPlayerWidget::updatePosition);
    connect(player_, &QMediaPlayer::durationChanged, this, &VideoPlayerWidget::updateDuration);
    connect(player_, &QMediaPlayer::durationChanged, this,
            [this](qint64) { updateMediaInfo(); });
    connect(player_, &QMediaPlayer::playbackStateChanged,
            this, &VideoPlayerWidget::updatePlaybackState);
    connect(player_, &QMediaPlayer::mediaStatusChanged,
            this, &VideoPlayerWidget::updateMediaStatus);
    connect(player_, &QMediaPlayer::bufferProgressChanged,
            this, &VideoPlayerWidget::updateBufferProgress);
    connect(player_, &QMediaPlayer::errorOccurred, this, &VideoPlayerWidget::reportError);
    connect(player_, &QMediaPlayer::metaDataChanged, this, &VideoPlayerWidget::updateMediaInfo);
    connect(player_, &QMediaPlayer::tracksChanged, this, &VideoPlayerWidget::updateMediaInfo);
    connect(videoOutput_->videoSink(), &QVideoSink::videoFrameChanged,
            this, &VideoPlayerWidget::observeVideoFrame);
    connect(diagnosticsTimer_, &QTimer::timeout,
            this, &VideoPlayerWidget::updatePlaybackDiagnostics);
    diagnosticsTimer_->start();
    resetPlaybackDiagnostics();
}

void VideoPlayerWidget::buildUi() {
    auto* root = new QVBoxLayout(this);
    root->setContentsMargins(6, 6, 6, 6);

    auto* controls = new QHBoxLayout();
    openButton_ = new QPushButton(QStringLiteral("Play local video..."), this);
    playPauseButton_ = new QPushButton(QStringLiteral("Play"), this);
    stopButton_ = new QPushButton(QStringLiteral("Stop"), this);
    muteButton_ = new QPushButton(QStringLiteral("Mute"), this);
    muteButton_->setCheckable(true);
    positionLabel_ = new QLabel(QStringLiteral("00:00 / 00:00"), this);
    positionSlider_ = new QSlider(Qt::Horizontal, this);
    positionSlider_->setRange(0, 0);
    volumeSlider_ = new QSlider(Qt::Horizontal, this);
    volumeSlider_->setRange(0, 100);
    volumeSlider_->setValue(80);
    volumeSlider_->setMaximumWidth(120);
    playbackRateBox_ = new QComboBox(this);
    playbackRateBox_->addItems({QStringLiteral("0.5x"), QStringLiteral("0.75x"),
                                QStringLiteral("1.0x"), QStringLiteral("1.25x"),
                                QStringLiteral("1.5x"), QStringLiteral("2.0x")});
    playbackRateBox_->setCurrentText(QStringLiteral("1.0x"));
    playbackRateBox_->setMaximumWidth(80);

    controls->addWidget(openButton_);
    controls->addWidget(playPauseButton_);
    controls->addWidget(stopButton_);
    controls->addWidget(new QLabel(QStringLiteral("Speed"), this));
    controls->addWidget(playbackRateBox_);
    controls->addWidget(positionSlider_, 1);
    controls->addWidget(positionLabel_);
    controls->addWidget(new QLabel(QStringLiteral("Volume"), this));
    controls->addWidget(volumeSlider_);
    controls->addWidget(muteButton_);
    root->addLayout(controls);

    stateLabel_ = new QLabel(QStringLiteral("Player: no media selected"), this);
    stateLabel_->setWordWrap(true);
    root->addWidget(stateLabel_);

    mediaInfoLabel_ = new QLabel(QStringLiteral("Media: no metadata yet"), this);
    mediaInfoLabel_->setWordWrap(true);
    mediaInfoLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
    root->addWidget(mediaInfoLabel_);

    playbackDiagnosticsLabel_ = new QLabel(
        QStringLiteral("Diagnostics: waiting for decoded video frames"), this);
    playbackDiagnosticsLabel_->setWordWrap(true);
    playbackDiagnosticsLabel_->setToolTip(QStringLiteral(
        "Frame rate is counted when Qt delivers a decoded frame to QVideoWidget's video sink. "
        "It is not a monitor-presented FPS or a hardware-decoder counter."));
    root->addWidget(playbackDiagnosticsLabel_);

    auto* videoArea = new QWidget(this);
    videoArea->setMinimumSize(480, 270);
    videoStack_ = new QStackedLayout(videoArea);
    videoStack_->setContentsMargins(0, 0, 0, 0);
    emptyMediaLabel_ = new QLabel(
        QStringLiteral("No local video selected\nClick ‘Play local video...’ to open an MP4 file."),
        videoArea);
    emptyMediaLabel_->setAlignment(Qt::AlignCenter);
    emptyMediaLabel_->setStyleSheet(
        QStringLiteral("QLabel { background: #202124; color: #d9d9d9; font-size: 14px; }"));
    videoOutput_ = new QVideoWidget(videoArea);
    videoOutput_->setAspectRatioMode(Qt::KeepAspectRatio);
    videoStack_->addWidget(emptyMediaLabel_);
    videoStack_->addWidget(videoOutput_);
    videoStack_->setCurrentWidget(emptyMediaLabel_);
    root->addWidget(videoArea, 1);
}

bool VideoPlayerWidget::openLocalFile(const QString& path) {
    const QFileInfo info(path);
    if (!info.exists() || !info.isFile() || !info.isReadable()) {
        const QString message = QStringLiteral("Cannot open local media file: %1").arg(path);
        setStateText(message);
        emit log(message);
        return false;
    }

    player_->stop();
    currentLocalFile_ = info.absoluteFilePath();
    durationMs_ = 0;
    resetPlaybackDiagnostics();
    positionSlider_->setRange(0, 0);
    positionLabel_->setText(QStringLiteral("00:00 / 00:00"));
    videoStack_->setCurrentWidget(videoOutput_);
    player_->setSource(QUrl::fromLocalFile(currentLocalFile_));
    setStateText(QStringLiteral("Player: loaded %1").arg(info.fileName()));
    emit log(QStringLiteral("player loaded local media: %1").arg(currentLocalFile_));
    return true;
}

bool VideoPlayerWidget::openNetworkUrl(const QUrl& url) {
    if (!url.isValid() || (url.scheme() != QStringLiteral("http") &&
                           url.scheme() != QStringLiteral("https")) || url.host().isEmpty()) {
        const QString message = QStringLiteral("Invalid playback URL: %1").arg(url.toDisplayString());
        setStateText(message);
        emit log(message);
        return false;
    }
    player_->stop();
    currentLocalFile_.clear();
    durationMs_ = 0;
    positionSlider_->setRange(0, 0);
    positionLabel_->setText(QStringLiteral("00:00 / 00:00"));
    resetPlaybackDiagnostics();
    videoStack_->setCurrentWidget(videoOutput_);
    player_->setSource(url);
    setStateText(QStringLiteral("Player: opening Edge URL %1").arg(url.toDisplayString()));
    emit log(QStringLiteral("player opening Edge URL: %1").arg(url.toDisplayString()));
    return true;
}

void VideoPlayerWidget::chooseLocalFile() {
    const QString path = QFileDialog::getOpenFileName(
        this, QStringLiteral("Select local video"), currentLocalFile_,
        QStringLiteral("Video files (*.mp4 *.mkv *.mov *.webm *.avi);;All files (*)"));
    if (!path.isEmpty()) openLocalFile(path);
}

void VideoPlayerWidget::togglePlayback() {
    if (player_->source().isEmpty()) {
        chooseLocalFile();
        if (currentLocalFile_.isEmpty()) return;
    }
    if (player_->playbackState() == QMediaPlayer::PlayingState) {
        player_->pause();
    } else {
        player_->play();
    }
}

void VideoPlayerWidget::play() {
    if (!player_->source().isEmpty()) player_->play();
}

void VideoPlayerWidget::stopPlayback() {
    player_->stop();
    player_->setPosition(0);
}

void VideoPlayerWidget::beginSeek() {
    userSeeking_ = true;
}

void VideoPlayerWidget::endSeek() {
    player_->setPosition(positionSlider_->value());
    userSeeking_ = false;
}

void VideoPlayerWidget::seekTo(int value) {
    positionLabel_->setText(QStringLiteral("%1 / %2")
        .arg(formatTime(value)).arg(formatTime(durationMs_)));
}

void VideoPlayerWidget::updatePosition(qint64 positionMs) {
    if (!userSeeking_) positionSlider_->setValue(sliderValue(positionMs));
    if (!userSeeking_) {
        positionLabel_->setText(QStringLiteral("%1 / %2")
            .arg(formatTime(positionMs)).arg(formatTime(durationMs_)));
    }
}

void VideoPlayerWidget::updateDuration(qint64 durationMs) {
    durationMs_ = std::max<qint64>(0, durationMs);
    positionSlider_->setRange(0, sliderValue(durationMs_));
    positionLabel_->setText(QStringLiteral("%1 / %2")
        .arg(formatTime(player_->position())).arg(formatTime(durationMs_)));
}

void VideoPlayerWidget::updatePlaybackState(QMediaPlayer::PlaybackState state) {
    switch (state) {
    case QMediaPlayer::PlayingState:
        playPauseButton_->setText(QStringLiteral("Pause"));
        setStateText(QStringLiteral("Player: playing"));
        emit log(QStringLiteral("player state: playing"));
        break;
    case QMediaPlayer::PausedState:
        playPauseButton_->setText(QStringLiteral("Play"));
        setStateText(QStringLiteral("Player: paused"));
        emit log(QStringLiteral("player state: paused"));
        break;
    case QMediaPlayer::StoppedState:
        playPauseButton_->setText(QStringLiteral("Play"));
        setStateText(QStringLiteral("Player: stopped"));
        break;
    }
}

void VideoPlayerWidget::updateMediaStatus(QMediaPlayer::MediaStatus status) {
    setStateText(QStringLiteral("Player: %1").arg(mediaStatusText(status)));
}

void VideoPlayerWidget::updateBufferProgress(float progress) {
    const int percent = static_cast<int>(std::clamp(progress, 0.0F, 1.0F) * 100.0F);
    if (player_->mediaStatus() == QMediaPlayer::BufferingMedia ||
        player_->mediaStatus() == QMediaPlayer::StalledMedia) {
        setStateText(QStringLiteral("Player: %1 (%2%)")
            .arg(mediaStatusText(player_->mediaStatus())).arg(percent));
    }
}

void VideoPlayerWidget::reportError(QMediaPlayer::Error error, const QString& message) {
    if (error == QMediaPlayer::NoError) return;
    const QString detail = message.isEmpty() ? QStringLiteral("unknown media error") : message;
    emptyMediaLabel_->setText(QStringLiteral("Unable to play local media\n%1").arg(detail));
    videoStack_->setCurrentWidget(emptyMediaLabel_);
    setStateText(QStringLiteral("Player error: %1").arg(detail));
    emit log(QStringLiteral("player error code=%1: %2").arg(static_cast<int>(error)).arg(detail));
}

void VideoPlayerWidget::updateMediaInfo() {
    const QMediaMetaData metadata = player_->metaData();
    const QList<QMediaMetaData> videoTracks = player_->videoTracks();
    const QList<QMediaMetaData> audioTracks = player_->audioTracks();
    const QVariant resolutionValue = firstReportedValue(
        metadata, videoTracks, QMediaMetaData::Resolution);
    const QSize resolution = resolutionValue.toSize();
    const QString resolutionText = resolution.isValid()
        ? QStringLiteral("%1x%2").arg(resolution.width()).arg(resolution.height())
        : QStringLiteral("not reported");
    const QVariant fpsValue = firstReportedValue(
        metadata, videoTracks, QMediaMetaData::VideoFrameRate);
    bool fpsValid = false;
    const double fps = fpsValue.toDouble(&fpsValid);
    const QString fpsText = fpsValid && fps > 0.0
        ? QStringLiteral("%1 fps").arg(fps, 0, 'f', 2)
        : QStringLiteral("not reported");
    const QString videoCodec = reportedText(firstReportedValue(
        metadata, videoTracks, QMediaMetaData::VideoCodec));
    const QString audioCodec = reportedText(firstReportedValue(
        metadata, audioTracks, QMediaMetaData::AudioCodec));
    const QString videoBitRate = bitRateText(firstReportedValue(
        metadata, videoTracks, QMediaMetaData::VideoBitRate));
    const QString audioBitRate = bitRateText(firstReportedValue(
        metadata, audioTracks, QMediaMetaData::AudioBitRate));

    mediaInfoLabel_->setText(QStringLiteral(
        "Video: %1, %2, codec=%3, bitrate=%4, tracks=%5 | "
        "Audio: codec=%6, bitrate=%7, tracks=%8 | Duration: %9")
        .arg(resolutionText, fpsText, videoCodec, videoBitRate)
        .arg(videoTracks.size())
        .arg(audioCodec, audioBitRate)
        .arg(audioTracks.size())
        .arg(formatTime(player_->duration())));
}

void VideoPlayerWidget::toggleMuted(bool muted) {
    audioOutput_->setMuted(muted);
    muteButton_->setText(muted ? QStringLiteral("Unmute") : QStringLiteral("Mute"));
    emit log(QStringLiteral("player audio: %1").arg(muted ? QStringLiteral("muted")
                                                               : QStringLiteral("unmuted")));
}

void VideoPlayerWidget::setPlaybackRate(const QString& text) {
    QString numeric = text;
    numeric.remove(QLatin1Char('x'));
    bool valid = false;
    const qreal rate = numeric.toDouble(&valid);
    if (valid && rate > 0.0) {
        player_->setPlaybackRate(rate);
        emit log(QStringLiteral("player playback rate: %1x").arg(rate, 0, 'f', 2));
    }
}

void VideoPlayerWidget::observeVideoFrame(const QVideoFrame& frame) {
    if (!frame.isValid()) return;
    ++framesInWindow_;
}

void VideoPlayerWidget::updatePlaybackDiagnostics() {
    const qint64 now = diagnosticsClock_.elapsed();
    const qint64 timerGap = now - previousDiagnosticsTickMs_;
    previousDiagnosticsTickMs_ = now;
    maxDiagnosticsTimerGapMs_ = std::max(maxDiagnosticsTimerGapMs_, timerGap);

    const qint64 frameWindowElapsed = now - frameWindowStartedMs_;
    if (frameWindowElapsed >= 1000) {
        deliveredFramesPerSecond_ = static_cast<double>(framesInWindow_) * 1000.0 /
                                   static_cast<double>(frameWindowElapsed);
        framesInWindow_ = 0;
        frameWindowStartedMs_ = now;
    }
    playbackDiagnosticsLabel_->setText(QStringLiteral(
        "Diagnostics: decoded frames delivered to Qt video sink ≈ %1 fps | "
        "GUI timer gap %2 ms (max %3 ms). Not monitor-presented FPS.")
        .arg(deliveredFramesPerSecond_, 0, 'f', 1)
        .arg(timerGap)
        .arg(maxDiagnosticsTimerGapMs_));
}

void VideoPlayerWidget::resetPlaybackDiagnostics() {
    diagnosticsClock_.restart();
    frameWindowStartedMs_ = 0;
    previousDiagnosticsTickMs_ = 0;
    maxDiagnosticsTimerGapMs_ = 0;
    framesInWindow_ = 0;
    deliveredFramesPerSecond_ = 0.0;
    if (playbackDiagnosticsLabel_ != nullptr) {
        playbackDiagnosticsLabel_->setText(
            QStringLiteral("Diagnostics: waiting for decoded video frames"));
    }
}

void VideoPlayerWidget::setStateText(const QString& text) {
    stateLabel_->setText(text);
}

QString VideoPlayerWidget::formatTime(qint64 milliseconds) {
    const qint64 totalSeconds = std::max<qint64>(0, milliseconds / 1000);
    const qint64 hours = totalSeconds / 3600;
    const qint64 minutes = (totalSeconds / 60) % 60;
    const qint64 seconds = totalSeconds % 60;
    if (hours > 0) {
        return QStringLiteral("%1:%2:%3")
            .arg(hours, 2, 10, QLatin1Char('0'))
            .arg(minutes, 2, 10, QLatin1Char('0'))
            .arg(seconds, 2, 10, QLatin1Char('0'));
    }
    return QStringLiteral("%1:%2")
        .arg(minutes, 2, 10, QLatin1Char('0'))
        .arg(seconds, 2, 10, QLatin1Char('0'));
}

QString VideoPlayerWidget::mediaStatusText(QMediaPlayer::MediaStatus status) {
    switch (status) {
    case QMediaPlayer::NoMedia: return QStringLiteral("no media selected");
    case QMediaPlayer::LoadingMedia: return QStringLiteral("loading");
    case QMediaPlayer::LoadedMedia: return QStringLiteral("loaded");
    case QMediaPlayer::StalledMedia: return QStringLiteral("stalled");
    case QMediaPlayer::BufferingMedia: return QStringLiteral("buffering");
    case QMediaPlayer::BufferedMedia: return QStringLiteral("buffered");
    case QMediaPlayer::EndOfMedia: return QStringLiteral("end of media");
    case QMediaPlayer::InvalidMedia: return QStringLiteral("invalid media");
    }
    return QStringLiteral("unknown status");
}

}  // namespace miniKV::qtclient
