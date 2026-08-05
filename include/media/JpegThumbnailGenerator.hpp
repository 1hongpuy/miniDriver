#pragma once

#include <cstdint>
#include <string>

namespace miniKV {
namespace media {

struct JpegThumbnailOptions {
    uint64_t maxInputBytes = 100ULL * 1024ULL * 1024ULL;
    uint32_t maxSourceDimension = 16000;
    uint64_t maxSourcePixels = 80ULL * 1024ULL * 1024ULL;
    uint32_t maxEdge = 512;
    int jpegQuality = 82;
};

struct JpegThumbnailResult {
    uint32_t width = 0;
    uint32_t height = 0;
    // Retrying cannot help when the source is malformed or exceeds a hard limit.
    bool unsupported = false;
    std::string error;
};

// Maps a stable derived-object profile to its image-generation settings.
// Returns false for profiles this JPEG-only generator cannot produce.
bool jpegDerivedProfile(const std::string& profile, JpegThumbnailOptions& options,
                        std::string& derivedFileName);

bool generateJpegThumbnail(const std::string& sourcePath, const std::string& outputPath,
                           JpegThumbnailResult& result,
                           const JpegThumbnailOptions& options = {});

}  // namespace media
}  // namespace miniKV
