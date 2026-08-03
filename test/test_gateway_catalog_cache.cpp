#include "gateway/GatewayState.hpp"

#include <filesystem>
#include <iostream>

namespace {
using miniKV::gateway::CatalogSnapshot;
using miniKV::gateway::DirectoryMeta;
using miniKV::gateway::GatewayState;

bool check(bool condition, const char* expression, int line)
{
    if (condition) return true;
    std::cerr << "FAIL:" << line << ": " << expression << '\n';
    return false;
}

#define CHECK(expression) \
    do { if (!check((expression), #expression, __LINE__)) return 1; } while (false)
}  // namespace

int main()
{
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_gateway_catalog_cache_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    GatewayState state(directory.string());
    CHECK(state.open());
    CHECK(state.createDirectory("/", "shoots"));

    CatalogSnapshot catalog;
    const auto before = state.catalogCacheStats();
    CHECK(state.listCatalog("/", catalog));
    CHECK(catalog.directories.size() == 1);
    const auto afterMiss = state.catalogCacheStats();
    CHECK(afterMiss.misses == before.misses + 1);

    CHECK(state.listCatalog("/", catalog));
    const auto afterHit = state.catalogCacheStats();
    CHECK(afterHit.hits == afterMiss.hits + 1);

    DirectoryMeta created;
    CHECK(state.createDirectory("/", "archive", &created));
    CHECK(state.listCatalog("/", catalog));
    CHECK(catalog.directories.size() == 2);
    const auto afterInvalidation = state.catalogCacheStats();
    CHECK(afterInvalidation.misses == afterHit.misses + 1);

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: Gateway catalog cache hits and invalidates parent listings\n";
    return 0;
}
