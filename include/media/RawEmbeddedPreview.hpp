#pragma once

#include <cstdint>
#include <string>

namespace miniKV {
namespace media {

struct RawEmbeddedPreviewOptions {
    uint64_t maxInputBytes = 512ULL * 1024ULL * 1024ULL;
    uint64_t maxEmbeddedJpegBytes = 64ULL * 1024ULL * 1024ULL;
};

struct RawEmbeddedPreviewResult {
    bool unsupported = false;
    std::string error;
};

bool isRawEmbeddedPreviewFileName(const std::string& fileName);

// Extracts a camera-provided JPEG preview without demosaicing RAW sensor data.
bool extractRawEmbeddedJpeg(const std::string& rawPath, const std::string& jpegPath,
                            RawEmbeddedPreviewResult& result,
                            const RawEmbeddedPreviewOptions& options = {});

}  // namespace media
}  // namespace miniKV
