#include "TestCheck.hpp"
#include "media/JpegThumbnailGenerator.hpp"

#define STB_IMAGE_WRITE_STATIC
#define STB_IMAGE_WRITE_IMPLEMENTATION
#include "stb_image_write.h"

#define STB_IMAGE_STATIC
#define STB_IMAGE_IMPLEMENTATION
#include "stb_image.h"

#include <filesystem>
#include <vector>

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
