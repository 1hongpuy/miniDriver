#pragma once

#include <QMediaPlayer>
#include <QElapsedTimer>
#include <QUrl>
#include <QWidget>

class QAudioOutput;
class QComboBox;
class QLabel;
class QPushButton;
class QSlider;
class QStackedLayout;
class QTimer;
class QVideoFrame;
class QVideoWidget;

namespace miniKV::qtclient {

// Owns only Qt Multimedia UI state. It deliberately has no MiniDriver SDK or
// network dependency: Step 2 validates local-file decode/playback before the
// later Edge HTTP Range path is introduced.
class VideoPlayerWidget final : public QWidget {
    Q_OBJECT

public:
    explicit VideoPlayerWidget(QWidget* parent = nullptr);

    bool openLocalFile(const QString& path);
    bool openNetworkUrl(const QUrl& url);
    void play();
    QString currentLocalFile() const { return currentLocalFile_; }

signals:
    void log(const QString& message);

private slots:
    void chooseLocalFile();
    void togglePlayback();
    void stopPlayback();
    void beginSeek();
    void endSeek();
    void seekTo(int value);
    void updatePosition(qint64 positionMs);
    void updateDuration(qint64 durationMs);
    void updatePlaybackState(QMediaPlayer::PlaybackState state);
    void updateMediaStatus(QMediaPlayer::MediaStatus status);
    void updateBufferProgress(float progress);
    void reportError(QMediaPlayer::Error error, const QString& message);
    void updateMediaInfo();
    void toggleMuted(bool muted);
    void setPlaybackRate(const QString& text);
    void observeVideoFrame(const QVideoFrame& frame);
    void updatePlaybackDiagnostics();

private:
    void buildUi();
    void setStateText(const QString& text);
    void resetPlaybackDiagnostics();
    static QString formatTime(qint64 milliseconds);
    static QString mediaStatusText(QMediaPlayer::MediaStatus status);

    QMediaPlayer* player_ = nullptr;
    QAudioOutput* audioOutput_ = nullptr;
    QVideoWidget* videoOutput_ = nullptr;
    QPushButton* openButton_ = nullptr;
    QPushButton* playPauseButton_ = nullptr;
    QPushButton* stopButton_ = nullptr;
    QSlider* positionSlider_ = nullptr;
    QSlider* volumeSlider_ = nullptr;
    QLabel* positionLabel_ = nullptr;
    QLabel* stateLabel_ = nullptr;
    QLabel* mediaInfoLabel_ = nullptr;
    QLabel* playbackDiagnosticsLabel_ = nullptr;
    QLabel* emptyMediaLabel_ = nullptr;
    QPushButton* muteButton_ = nullptr;
    QComboBox* playbackRateBox_ = nullptr;
    QStackedLayout* videoStack_ = nullptr;
    QTimer* diagnosticsTimer_ = nullptr;
    QString currentLocalFile_;
    qint64 durationMs_ = 0;
    QElapsedTimer diagnosticsClock_;
    qint64 frameWindowStartedMs_ = 0;
    qint64 previousDiagnosticsTickMs_ = 0;
    qint64 maxDiagnosticsTimerGapMs_ = 0;
    int framesInWindow_ = 0;
    double deliveredFramesPerSecond_ = 0.0;
    bool userSeeking_ = false;
};

}  // namespace miniKV::qtclient
