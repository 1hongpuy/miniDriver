#include "media/JpegThumbnailGenerator.hpp"

#include <algorithm>
#include <array>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <vector>

#define STBI_MAX_DIMENSIONS 16000
#define STB_IMAGE_STATIC
#define STB_IMAGE_IMPLEMENTATION
#include "stb_image.h"

#define STB_IMAGE_RESIZE_STATIC
#define STB_IMAGE_RESIZE_IMPLEMENTATION
#include "stb_image_resize2.h"

#define STB_IMAGE_WRITE_STATIC
#define STB_IMAGE_WRITE_IMPLEMENTATION
#include "stb_image_write.h"

namespace miniKV {
namespace media {
namespace {

bool hasJpegSignature(const std::string& path)
{
    std::ifstream input(path, std::ios::binary);
    std::array<unsigned char, 3> bytes{};
    return input.read(reinterpret_cast<char*>(bytes.data()), bytes.size()).gcount() == 3 &&
           bytes[0] == 0xff && bytes[1] == 0xd8 && bytes[2] == 0xff;
}

void fail(JpegThumbnailResult& result, const std::string& error, bool unsupported = false)
{
    result = {};
    result.unsupported = unsupported;
    result.error = error;
}

}  // namespace

bool jpegDerivedProfile(const std::string& profile, JpegThumbnailOptions& options,
                        std::string& derivedFileName)
{
    JpegThumbnailOptions selected;
    std::string selectedFileName;
    if(profile == "thumb-512-jpeg-v1") {
        selected.maxEdge = 512;
        selected.jpegQuality = 82;
        selectedFileName = "thumb-512-jpeg-v1.jpg";
    } else if(profile == "preview-2048-jpeg-v1") {
        selected.maxEdge = 2048;
        selected.jpegQuality = 88;
        selectedFileName = "preview-2048-jpeg-v1.jpg";
    } else {
        return false;
    }
    options = selected;
    derivedFileName = std::move(selectedFileName);
    return true;
}

bool generateJpegThumbnail(const std::string& sourcePath, const std::string& outputPath,
                           JpegThumbnailResult& result, const JpegThumbnailOptions& options)
{
    result = {};
    if(sourcePath.empty() || outputPath.empty() || sourcePath == outputPath || options.maxInputBytes == 0 ||
       options.maxSourceDimension == 0 || options.maxSourcePixels == 0 || options.maxEdge == 0 ||
       options.jpegQuality < 1 || options.jpegQuality > 100) {
        fail(result, "invalid thumbnail request");
        return false;
    }
    std::error_code filesystemError;
    const uint64_t inputBytes = std::filesystem::file_size(sourcePath, filesystemError);
    if(filesystemError || inputBytes == 0 || inputBytes > options.maxInputBytes) {
        fail(result, "JPEG input size is invalid", true);
        return false;
    }
    if(!hasJpegSignature(sourcePath)) {
        fail(result, "source is not a JPEG", true);
        return false;
    }

    int sourceWidth = 0;
    int sourceHeight = 0;
    int sourceComponents = 0;
    if(stbi_info(sourcePath.c_str(), &sourceWidth, &sourceHeight, &sourceComponents) == 0 ||
       sourceWidth <= 0 || sourceHeight <= 0 ||
       sourceWidth > static_cast<int>(options.maxSourceDimension) ||
       sourceHeight > static_cast<int>(options.maxSourceDimension) ||
       static_cast<uint64_t>(sourceWidth) * static_cast<uint64_t>(sourceHeight) > options.maxSourcePixels) {
        fail(result, "JPEG dimensions exceed limits", true);
        return false;
    }

    using ImagePtr = std::unique_ptr<unsigned char, decltype(&stbi_image_free)>;
    ImagePtr source(stbi_load(sourcePath.c_str(), &sourceWidth, &sourceHeight, &sourceComponents, 3),
                    &stbi_image_free);
    if(!source) {
        fail(result, "cannot decode JPEG", true);
        return false;
    }

    uint32_t outputWidth = static_cast<uint32_t>(sourceWidth);
    uint32_t outputHeight = static_cast<uint32_t>(sourceHeight);
    const uint32_t largest = std::max(outputWidth, outputHeight);
    if(largest > options.maxEdge) {
        if(outputWidth >= outputHeight) {
            outputWidth = options.maxEdge;
            outputHeight = std::max<uint32_t>(1, static_cast<uint32_t>(
                (static_cast<uint64_t>(sourceHeight) * options.maxEdge) / sourceWidth));
        } else {
            outputHeight = options.maxEdge;
            outputWidth = std::max<uint32_t>(1, static_cast<uint32_t>(
                (static_cast<uint64_t>(sourceWidth) * options.maxEdge) / sourceHeight));
        }
    }
    if(static_cast<uint64_t>(outputWidth) * outputHeight >
       std::numeric_limits<size_t>::max() / 3) {
        fail(result, "thumbnail allocation overflow");
        return false;
    }
    std::vector<unsigned char> resized(static_cast<size_t>(outputWidth) * outputHeight * 3);
    if(stbir_resize_uint8_srgb(source.get(), sourceWidth, sourceHeight, sourceWidth * 3,
                               resized.data(), static_cast<int>(outputWidth),
                               static_cast<int>(outputHeight), static_cast<int>(outputWidth) * 3,
                               STBIR_RGB) == nullptr) {
        fail(result, "cannot resize JPEG");
        return false;
    }

    const std::filesystem::path output(outputPath);
    if(!output.parent_path().empty()) std::filesystem::create_directories(output.parent_path(), filesystemError);
    if(filesystemError || stbi_write_jpg(outputPath.c_str(), static_cast<int>(outputWidth),
                                         static_cast<int>(outputHeight), 3, resized.data(),
                                         options.jpegQuality) == 0) {
        fail(result, "cannot write thumbnail JPEG");
        return false;
    }
    result.width = outputWidth;
    result.height = outputHeight;
    return true;
}

}  // namespace media
}  // namespace miniKV
