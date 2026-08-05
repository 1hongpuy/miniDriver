#include "media/RawEmbeddedPreview.hpp"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>

#include <libraw/libraw.h>

namespace miniKV {
namespace media {
namespace {

void fail(RawEmbeddedPreviewResult& result, std::string error, bool unsupported)
{
    result = {};
    result.unsupported = unsupported;
    result.error = std::move(error);
}

std::string lowerExtension(const std::string& fileName)
{
    std::string extension = std::filesystem::path(fileName).extension().string();
    std::transform(extension.begin(), extension.end(), extension.begin(),
                   [](unsigned char value) { return static_cast<char>(std::tolower(value)); });
    return extension;
}

}  // namespace

bool isRawEmbeddedPreviewFileName(const std::string& fileName)
{
    const std::string extension = lowerExtension(fileName);
    return extension == ".nef" || extension == ".cr2" || extension == ".arw" ||
           extension == ".dng";
}

bool extractRawEmbeddedJpeg(const std::string& rawPath, const std::string& jpegPath,
                            RawEmbeddedPreviewResult& result,
                            const RawEmbeddedPreviewOptions& options)
{
    result = {};
    if(rawPath.empty() || jpegPath.empty() || rawPath == jpegPath || options.maxInputBytes == 0 ||
       options.maxEmbeddedJpegBytes == 0) {
        fail(result, "invalid RAW preview request", false);
        return false;
    }

    std::error_code filesystemError;
    const uint64_t inputBytes = std::filesystem::file_size(rawPath, filesystemError);
    if(filesystemError || inputBytes == 0 || inputBytes > options.maxInputBytes) {
        fail(result, "RAW input size is invalid", true);
        return false;
    }

    LibRaw decoder;
    if(decoder.open_file(rawPath.c_str()) != LIBRAW_SUCCESS) {
        fail(result, "cannot open RAW source", true);
        return false;
    }
    if(decoder.unpack_thumb() != LIBRAW_SUCCESS) {
        fail(result, "RAW source has no embedded preview", true);
        return false;
    }

    const libraw_thumbnail_t& thumbnail = decoder.imgdata.thumbnail;
    if(thumbnail.tformat != LIBRAW_THUMBNAIL_JPEG || thumbnail.thumb == nullptr ||
       thumbnail.tlength == 0 || thumbnail.tlength > options.maxEmbeddedJpegBytes) {
        fail(result, "RAW embedded preview is not a supported JPEG", true);
        return false;
    }

    const std::filesystem::path output(jpegPath);
    if(!output.parent_path().empty()) std::filesystem::create_directories(output.parent_path(), filesystemError);
    if(filesystemError) {
        fail(result, "cannot create RAW preview directory", false);
        return false;
    }
    std::ofstream destination(jpegPath, std::ios::binary | std::ios::trunc);
    if(!destination) {
        fail(result, "cannot create RAW preview JPEG", false);
        return false;
    }
    destination.write(thumbnail.thumb, static_cast<std::streamsize>(thumbnail.tlength));
    destination.close();
    if(!destination) {
        std::filesystem::remove(output, filesystemError);
        fail(result, "cannot write RAW preview JPEG", false);
        return false;
    }
    return true;
}

}  // namespace media
}  // namespace miniKV
