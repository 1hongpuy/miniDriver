#include "edge/EdgeCacheStore.hpp"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>

namespace {

bool check(bool value, const char* expression, int line) {
    if (value) return true;
    std::cerr << "check failed at line " << line << ": " << expression << '\n';
    return false;
}

#define MINIKV_CHECK(expression) do { if (!check((expression), #expression, __LINE__)) return 1; } while (false)

}  // namespace

int main() {
    char directoryTemplate[] = "/tmp/minidriver-edge-cache-test-XXXXXX";
    char* created = ::mkdtemp(directoryTemplate);
    MINIKV_CHECK(created != nullptr);
    const std::filesystem::path root(created);

    miniKV::edge::EdgeCacheStore::Config config;
    config.root = root;
    config.capacityBytes = 32;
    config.verifyHitChecksum = true;
    miniKV::edge::EdgeCacheStore store(config);
    std::string error;
    MINIKV_CHECK(store.initialize(error));

    miniKV::client::ObjectRef object{"object-test", 1};
    miniKV::client::ChunkReadPlan chunk;
    chunk.index = 0;
    chunk.storageIdentity = "storage-test";
    chunk.size = 3;
    chunk.checksumType = "crc32c";
    chunk.checksumDigest = "364b3fb7";  // CRC32C("abc")
    const auto key = miniKV::edge::EdgeCacheKey::fromChunk(object, chunk);

    miniKV::edge::CacheLease absent;
    MINIKV_CHECK(!store.acquire(key, absent, error));
    miniKV::edge::CacheReservation reservation;
    MINIKV_CHECK(store.reserve(key, reservation, error));
    miniKV::edge::CacheReservation duplicate;
    MINIKV_CHECK(!store.reserve(key, duplicate, error));

    miniKV::edge::CacheLease published;
    MINIKV_CHECK(store.publish(key, "abc", reservation, published, error));
    MINIKV_CHECK(published.valid());
    MINIKV_CHECK(std::filesystem::file_size(published.path()) == 3);

    miniKV::edge::CacheLease hit;
    MINIKV_CHECK(store.acquire(key, hit, error));
    MINIKV_CHECK(hit.valid());
    published.reset();
    hit.reset();

    const auto path = root / (key.id + ".ready");
    {
        std::ofstream corrupt(path, std::ios::binary | std::ios::trunc);
        corrupt << "abd";
    }
    miniKV::edge::CacheLease corrupt;
    MINIKV_CHECK(!store.acquire(key, corrupt, error));
    const auto stats = store.stats();
    MINIKV_CHECK(stats.entries == 0);
    MINIKV_CHECK(stats.readyBytes == 0);

    std::error_code ignored;
    std::filesystem::remove_all(root, ignored);
    return 0;
}
