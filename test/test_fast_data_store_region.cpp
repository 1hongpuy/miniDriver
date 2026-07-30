#include "DataNode/FastDataStore.hpp"
#include "utils/Util.hpp"

#include <filesystem>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>

namespace {

bool check(bool condition, const char* expression, int line) {
    if (condition) return true;
    std::cerr << "FAIL:" << line << ": " << expression << '\n';
    return false;
}

#define CHECK(expression) \
    do { if (!check((expression), #expression, __LINE__)) return 1; } while (false)

}  // namespace

int main() {
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_fast_data_store_region_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    miniKV::datanode::FastDataStore store(directory.string());
    CHECK(store.open());
    const std::string first = "first-chunk";
    const std::string second = "second-chunk-at-a-nonzero-offset";
    bool alreadyExists = false;
    CHECK(store.put(miniKV::util::sha256Hex(first.data(), first.size()), first, alreadyExists));
    CHECK(!alreadyExists);
    const std::string secondHash = miniKV::util::sha256Hex(second.data(), second.size());
    CHECK(store.put(secondHash, second, alreadyExists));
    CHECK(!alreadyExists);

    miniKV::datanode::FileRegion region;
    CHECK(store.getRegion(secondHash, region));
    CHECK(region.offset == static_cast<off_t>(first.size()));
    CHECK(region.length == second.size());
    const int fd = ::open((directory / "disk0.data").c_str(), O_RDONLY | O_CLOEXEC);
    CHECK(fd >= 0);
    std::string bytes(region.length, '\0');
    CHECK(::pread(fd, bytes.data(), bytes.size(), region.offset) ==
          static_cast<ssize_t>(bytes.size()));
    CHECK(bytes == second);
    ::close(fd);

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: FastDataStore exposes a non-zero extent offset\n";
    return 0;
}
