#include "gateway/LruCache.hpp"

#include <cstdint>
#include <iostream>
#include <memory>
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

using Cache = miniKV::gateway::LruCache<std::string, std::string>;

std::shared_ptr<const std::string> value(const char* text)
{
    return std::make_shared<const std::string>(text);
}

}  // namespace

int main()
{
    int64_t now = 100;
    Cache::Config config;
    config.maxEntries = 2;
    config.maxBytes = 10;
    Cache cache(config, [&now] { return now; });

    CHECK(cache.put("a", value("aaaa"), 4, 60));
    CHECK(cache.put("b", value("bbbb"), 4, 60));
    CHECK(*cache.get("a") == "aaaa");
    CHECK(cache.put("c", value("cccc"), 4, 60));
    CHECK(cache.get("b") == nullptr);
    CHECK(*cache.get("a") == "aaaa");
    CHECK(*cache.get("c") == "cccc");
    CHECK(cache.stats().evictions == 1);

    Cache byteCache(config, [&now] { return now; });
    CHECK(byteCache.put("a", value("aaaaaa"), 6, 60));
    CHECK(byteCache.put("b", value("bbbbbb"), 6, 60));
    CHECK(byteCache.get("a") == nullptr);
    CHECK(*byteCache.get("b") == "bbbbbb");
    CHECK(byteCache.bytes() == 6);
    CHECK(byteCache.stats().evictions == 1);
    CHECK(!byteCache.put("oversized", value("eleven-byte"), 11, 60));
    CHECK(byteCache.stats().rejections == 1);

    Cache ttlCache(config, [&now] { return now; });
    CHECK(ttlCache.put("expires", value("soon"), 4, 5));
    now += 6;
    CHECK(ttlCache.get("expires") == nullptr);
    CHECK(ttlCache.stats().expired == 1);
    CHECK(ttlCache.bytes() == 0);

    Cache replacementCache(config, [&now] { return now; });
    CHECK(replacementCache.put("same", value("old"), 3, 60));
    CHECK(replacementCache.put("same", value("updated"), 7, 60));
    CHECK(replacementCache.size() == 1);
    CHECK(replacementCache.bytes() == 7);
    CHECK(*replacementCache.get("same") == "updated");
    CHECK(replacementCache.erase("same"));
    CHECK(replacementCache.get("same") == nullptr);

    std::cout << "PASS: LRU cache evicts by recency, bytes, and TTL\n";
    return 0;
}
