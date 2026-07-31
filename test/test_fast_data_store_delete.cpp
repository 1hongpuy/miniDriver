#include "DataNode/FastDataStore.hpp"

#include <filesystem>
#include <iostream>
#include <string>

namespace {

bool check(bool condition, const char* expression, int line)
{
    if (condition) return true;
    std::cerr << "FAIL:" << line << ": " << expression << '\n';
    return false;
}

#define CHECK(expression) \
    do { if (!check((expression), #expression, __LINE__)) return 1; } while (false)

constexpr const char* kHashAbc =
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
constexpr const char* kHashDef =
    "cb8379ac2098aa165029e3938a51da0bcecfc008fd6795f401178647f96c5b34";
constexpr const char* kHashGhi =
    "50ae61e841fac4e8f9e40baf2ad36ec868922ea48368c18f9535e47db56dd7fb";
constexpr const char* kHashAbcdef =
    "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721";

}  // namespace

int main()
{
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_fast_data_store_delete_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    {
        miniKV::datanode::FastDataStore store(directory.string());
        bool alreadyExists = false;
        bool removed = false;
        CHECK(store.open());
        CHECK(store.put(kHashAbc, "abc", alreadyExists));
        CHECK(!alreadyExists);
        CHECK(store.put(kHashDef, "def", alreadyExists));
        CHECK(!alreadyExists);
        CHECK(store.remove(kHashAbc, removed));
        CHECK(removed);
        CHECK(!store.exists(kHashAbc));
        CHECK(store.reusableBytes() == 3);
        CHECK(store.put(kHashGhi, "ghi", alreadyExists));
        CHECK(!alreadyExists);

        miniKV::datanode::FileRegion reused;
        CHECK(store.getRegion(kHashGhi, reused));
        CHECK(reused.offset == 0);

        CHECK(store.remove(kHashGhi, removed));
        CHECK(removed);
        CHECK(store.remove(kHashGhi, removed));
        CHECK(!removed);
        CHECK(store.remove(kHashDef, removed));
        CHECK(removed);
        CHECK(store.reusableBytes() == 6);
    }

    {
        miniKV::datanode::FastDataStore reopened(directory.string());
        bool alreadyExists = false;
        miniKV::datanode::FileRegion reused;
        CHECK(reopened.open());
        CHECK(reopened.reusableBytes() == 6);
        CHECK(reopened.put(kHashAbcdef, "abcdef", alreadyExists));
        CHECK(!alreadyExists);
        CHECK(reopened.getRegion(kHashAbcdef, reused));
        CHECK(reused.offset == 0);
        CHECK(reopened.reusableBytes() == 0);
    }

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: FastDataStore deletes chunks and reuses persistent free extents\n";
    return 0;
}
