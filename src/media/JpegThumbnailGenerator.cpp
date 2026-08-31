#include "media/JpegThumbnailGenerator.hpp"

#include <algorithm>
#include <array>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <string_view>
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

uint16_t readU16(const unsigned char* data, bool littleEndian)
{
    return littleEndian ? static_cast<uint16_t>(data[0] | (static_cast<uint16_t>(data[1]) << 8))
                        : static_cast<uint16_t>((static_cast<uint16_t>(data[0]) << 8) | data[1]);
}

uint32_t readU32(const unsigned char* data, bool littleEndian)
{
    if(littleEndian) {
        return static_cast<uint32_t>(data[0]) |
               (static_cast<uint32_t>(data[1]) << 8) |
               (static_cast<uint32_t>(data[2]) << 16) |
               (static_cast<uint32_t>(data[3]) << 24);
    }
    return (static_cast<uint32_t>(data[0]) << 24) |
           (static_cast<uint32_t>(data[1]) << 16) |
           (static_cast<uint32_t>(data[2]) << 8) |
           static_cast<uint32_t>(data[3]);
}

uint16_t exifOrientation(const std::string& path)
{
    std::ifstream input(path, std::ios::binary);
    if(!input) return 1;
    std::vector<unsigned char> bytes((std::istreambuf_iterator<char>(input)), {});
    if(bytes.size() < 4 || bytes[0] != 0xff || bytes[1] != 0xd8) return 1;

    for(size_t offset = 2; offset + 4 <= bytes.size();) {
        if(bytes[offset] != 0xff) { ++offset; continue; }
        while(offset < bytes.size() && bytes[offset] == 0xff) ++offset;
        if(offset >= bytes.size()) return 1;
        const unsigned char marker = bytes[offset++];
        if(marker == 0xd9 || marker == 0xda) return 1;
        if(marker == 0x01 || (marker >= 0xd0 && marker <= 0xd7)) continue;
        if(offset + 2 > bytes.size()) return 1;
        const size_t segmentLength = (static_cast<size_t>(bytes[offset]) << 8) | bytes[offset + 1];
        if(segmentLength < 2 || offset + segmentLength > bytes.size()) return 1;
        const size_t payload = offset + 2;
        const size_t payloadSize = segmentLength - 2;
        offset += segmentLength;
        if(marker != 0xe1 || payloadSize < 14 ||
           std::string_view(reinterpret_cast<const char*>(bytes.data() + payload), 6) !=
               std::string_view("Exif\0\0", 6)) {
            continue;
        }

        const size_t tiff = payload + 6;
        const size_t tiffSize = payloadSize - 6;
        if(tiffSize < 8) return 1;
        const bool littleEndian = bytes[tiff] == 'I' && bytes[tiff + 1] == 'I';
        if(!littleEndian && !(bytes[tiff] == 'M' && bytes[tiff + 1] == 'M')) return 1;
        if(readU16(bytes.data() + tiff + 2, littleEndian) != 42) return 1;
        const uint32_t ifdOffset = readU32(bytes.data() + tiff + 4, littleEndian);
        if(ifdOffset > tiffSize - 2) return 1;
        const size_t ifd = tiff + ifdOffset;
        const uint16_t entries = readU16(bytes.data() + ifd, littleEndian);
        if(entries > (tiff + tiffSize - (ifd + 2)) / 12) return 1;
        for(uint16_t index = 0; index < entries; ++index) {
            const size_t entry = ifd + 2 + static_cast<size_t>(index) * 12;
            if(readU16(bytes.data() + entry, littleEndian) != 0x0112 ||
               readU16(bytes.data() + entry + 2, littleEndian) != 3 ||
               readU32(bytes.data() + entry + 4, littleEndian) != 1) {
                continue;
            }
            const uint16_t orientation = readU16(bytes.data() + entry + 8, littleEndian);
            return orientation >= 1 && orientation <= 8 ? orientation : 1;
        }
        return 1;
    }
    return 1;
}

bool applyOrientation(const unsigned char* source, int sourceWidth, int sourceHeight,
                      uint16_t orientation, std::vector<unsigned char>& output,
                      int& outputWidth, int& outputHeight)
{
    if(orientation < 2 || orientation > 8) return false;
    const bool swapDimensions = orientation >= 5;
    outputWidth = swapDimensions ? sourceHeight : sourceWidth;
    outputHeight = swapDimensions ? sourceWidth : sourceHeight;
    output.resize(static_cast<size_t>(outputWidth) * outputHeight * 3);
    for(int y = 0; y < outputHeight; ++y) {
        for(int x = 0; x < outputWidth; ++x) {
            int sourceX = x;
            int sourceY = y;
            switch(orientation) {
            case 2: sourceX = sourceWidth - 1 - x; break;
            case 3: sourceX = sourceWidth - 1 - x; sourceY = sourceHeight - 1 - y; break;
            case 4: sourceY = sourceHeight - 1 - y; break;
            case 5: sourceX = y; sourceY = x; break;
            case 6: sourceX = y; sourceY = sourceHeight - 1 - x; break;
            case 7: sourceX = sourceWidth - 1 - y; sourceY = sourceHeight - 1 - x; break;
            case 8: sourceX = sourceWidth - 1 - y; sourceY = x; break;
            default: return false;
            }
            const size_t destination = (static_cast<size_t>(y) * outputWidth + x) * 3;
            const size_t input = (static_cast<size_t>(sourceY) * sourceWidth + sourceX) * 3;
            output[destination] = source[input];
            output[destination + 1] = source[input + 1];
            output[destination + 2] = source[input + 2];
        }
    }
    return true;
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
        selected.jpegQuality = 85;
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

    std::vector<unsigned char> oriented;
    const unsigned char* resizeSource = source.get();
    if(options.applyExifOrientation) {
        int orientedWidth = sourceWidth;
        int orientedHeight = sourceHeight;
        if(applyOrientation(source.get(), sourceWidth, sourceHeight, exifOrientation(sourcePath), oriented,
                            orientedWidth, orientedHeight)) {
            resizeSource = oriented.data();
            sourceWidth = orientedWidth;
            sourceHeight = orientedHeight;
        }
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
    if(stbir_resize_uint8_srgb(resizeSource, sourceWidth, sourceHeight, sourceWidth * 3,
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
