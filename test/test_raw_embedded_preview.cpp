#include "TestCheck.hpp"
#include "media/RawEmbeddedPreview.hpp"

#include <cstdio>
#include <filesystem>

int main()
{
    MINIKV_CHECK(miniKV::media::isRawEmbeddedPreviewFileName("DSC_0001.NEF"));
    MINIKV_CHECK(miniKV::media::isRawEmbeddedPreviewFileName("frame.dng"));
    MINIKV_CHECK(!miniKV::media::isRawEmbeddedPreviewFileName("photo.jpg"));

    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_raw_embedded_preview_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);
    std::filesystem::create_directories(directory, error);

    const std::filesystem::path invalidRaw = directory / "invalid.nef";
    std::FILE* file = std::fopen(invalidRaw.c_str(), "wb");
    MINIKV_CHECK(file != nullptr);
    std::fputs("not a RAW image", file);
    std::fclose(file);

    miniKV::media::RawEmbeddedPreviewResult result;
    const std::filesystem::path output = directory / "preview.jpg";
    MINIKV_CHECK(!miniKV::media::extractRawEmbeddedJpeg(invalidRaw.string(), output.string(), result));
    MINIKV_CHECK(result.unsupported);
    MINIKV_CHECK(!std::filesystem::exists(output));

    std::filesystem::remove_all(directory, error);
    return 0;
}
