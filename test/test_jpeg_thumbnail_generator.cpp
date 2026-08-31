#include "TestCheck.hpp"
#include "media/JpegThumbnailGenerator.hpp"

#define STB_IMAGE_WRITE_STATIC
#define STB_IMAGE_WRITE_IMPLEMENTATION
#include "stb_image_write.h"

#define STB_IMAGE_STATIC
#define STB_IMAGE_IMPLEMENTATION
#include "stb_image.h"

#include <array>
#include <filesystem>
#include <fstream>
#include <vector>

namespace {

bool writeOrientationSixJpeg(const std::filesystem::path& source,
                             const std::filesystem::path& oriented)
{
    std::ifstream input(source, std::ios::binary);
    std::vector<unsigned char> bytes((std::istreambuf_iterator<char>(input)), {});
    if(bytes.size() < 2 || bytes[0] != 0xff || bytes[1] != 0xd8) return false;
    const std::array<unsigned char, 36> exif = {
        0xff, 0xe1, 0x00, 0x22,
        'E', 'x', 'i', 'f', 0x00, 0x00,
        'I', 'I', 0x2a, 0x00, 0x08, 0x00, 0x00, 0x00,
        0x01, 0x00,
        0x12, 0x01, 0x03, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x06, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00
    };
    std::ofstream output(oriented, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), 2);
    output.write(reinterpret_cast<const char*>(exif.data()), exif.size());
    output.write(reinterpret_cast<const char*>(bytes.data() + 2),
                 static_cast<std::streamsize>(bytes.size() - 2));
    return static_cast<bool>(output);
}

}  // namespace

int main()
{
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_jpeg_thumbnail_generator_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);
    std::filesystem::create_directories(directory, error);

    const std::filesystem::path source = directory / "source.jpg";
    const std::filesystem::path thumbnail = directory / "thumbnail.jpg";
    std::vector<unsigned char> pixels(1024 * 512 * 3, 0);
    for(size_t index = 0; index < pixels.size(); index += 3) {
        pixels[index] = 220;
        pixels[index + 1] = 120;
        pixels[index + 2] = 40;
    }
    MINIKV_CHECK(stbi_write_jpg(source.c_str(), 1024, 512, 3, pixels.data(), 90) != 0);

    miniKV::media::JpegThumbnailResult result;
    MINIKV_CHECK(miniKV::media::generateJpegThumbnail(source.string(), thumbnail.string(), result));
    MINIKV_CHECK(result.width == 512);
    MINIKV_CHECK(result.height == 256);
    int width = 0;
    int height = 0;
    int components = 0;
    unsigned char* decoded = stbi_load(thumbnail.c_str(), &width, &height, &components, 3);
    MINIKV_CHECK(decoded != nullptr);
    MINIKV_CHECK(width == 512);
    MINIKV_CHECK(height == 256);
    stbi_image_free(decoded);

    const std::filesystem::path orientedSource = directory / "oriented-source.jpg";
    const std::filesystem::path orientedThumbnail = directory / "oriented-thumbnail.jpg";
    MINIKV_CHECK(writeOrientationSixJpeg(source, orientedSource));
    MINIKV_CHECK(miniKV::media::generateJpegThumbnail(orientedSource.string(),
                                                       orientedThumbnail.string(), result));
    MINIKV_CHECK(result.width == 256);
    MINIKV_CHECK(result.height == 512);

    miniKV::media::JpegThumbnailOptions previewOptions;
    std::string previewFileName;
    MINIKV_CHECK(miniKV::media::jpegDerivedProfile("preview-2048-jpeg-v1",
                                                   previewOptions, previewFileName));
    MINIKV_CHECK(previewOptions.maxEdge == 2048);
    MINIKV_CHECK(previewOptions.jpegQuality == 88);
    MINIKV_CHECK(previewFileName == "preview-2048-jpeg-v1.jpg");
    miniKV::media::JpegThumbnailOptions thumbOptions;
    std::string thumbFileName;
    MINIKV_CHECK(miniKV::media::jpegDerivedProfile("thumb-512-jpeg-v1", thumbOptions,
                                                   thumbFileName));
    MINIKV_CHECK(thumbOptions.maxEdge == 512);
    MINIKV_CHECK(thumbOptions.jpegQuality == 85);
    MINIKV_CHECK(thumbOptions.applyExifOrientation);
    MINIKV_CHECK(!miniKV::media::jpegDerivedProfile("invalid-profile", previewOptions,
                                                    previewFileName));

    const std::filesystem::path largeSource = directory / "large-source.jpg";
    const std::filesystem::path preview = directory / "preview.jpg";
    std::vector<unsigned char> largePixels(4096 * 1024 * 3, 0);
    for(size_t index = 0; index < largePixels.size(); index += 3) {
        largePixels[index] = 40;
        largePixels[index + 1] = 140;
        largePixels[index + 2] = 220;
    }
    MINIKV_CHECK(stbi_write_jpg(largeSource.c_str(), 4096, 1024, 3, largePixels.data(), 90) != 0);
    MINIKV_CHECK(miniKV::media::generateJpegThumbnail(largeSource.string(), preview.string(), result,
                                                       previewOptions));
    MINIKV_CHECK(result.width == 2048);
    MINIKV_CHECK(result.height == 512);

    const std::filesystem::path nonJpeg = directory / "not-image.txt";
    std::FILE* invalid = std::fopen(nonJpeg.c_str(), "wb");
    MINIKV_CHECK(invalid != nullptr);
    std::fputs("not a jpeg", invalid);
    std::fclose(invalid);
    MINIKV_CHECK(!miniKV::media::generateJpegThumbnail(nonJpeg.string(),
                                                        (directory / "invalid.jpg").string(), result));
    MINIKV_CHECK(result.unsupported);

    std::filesystem::remove_all(directory, error);
    return 0;
}
