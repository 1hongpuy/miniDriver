#pragma once

#include "search/SearchDocument.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace miniKV::search {

struct SearchHit {
    SearchDocument document;
    float score = 0.0F;
};

struct SearchSegment {
    std::uint64_t generation = 0;
    std::vector<SearchDocument> documents;
};

class IndexBuilder {
public:
    ApplyResult apply(const SearchDocument& document, std::string* error = nullptr);
    SearchSegment flush(std::uint64_t generation);
    bool restore(const std::vector<SearchDocument>& documents, std::string* error = nullptr);
    std::size_t size() const;

private:
    static std::string versionKey(const SearchDocument& document);

    mutable std::mutex mutex_;
    std::vector<SearchDocument> pending_;
    // One current document per object + processor.  This is the same
    // version rule used by Compute Plane's durable SearchDocument table.
    std::unordered_map<std::string, SearchDocument> current_;
};

class SearchNode {
public:
    void install(SearchSegment segment);
    bool persist(const std::filesystem::path& path, std::string* error = nullptr) const;
    bool load(const std::filesystem::path& path, std::string* error = nullptr);
    std::vector<SearchDocument> snapshotDocuments() const;
    std::vector<SearchHit> queryEmbedding(const std::vector<float>& query,
                                           std::size_t limit = 10) const;
    std::size_t documentCount() const;
    std::uint64_t generation() const;

private:
    mutable std::mutex mutex_;
    std::vector<SearchSegment> segments_;
    std::uint64_t generation_ = 0;
};

class SearchShardRouter {
public:
    explicit SearchShardRouter(std::size_t shardCount = 1);

    ApplyResult apply(const SearchDocument& document, std::string* error = nullptr);
    bool flushAndPersist(const std::filesystem::path& directory,
                         std::uint64_t generation, std::string* error = nullptr);
    bool load(const std::filesystem::path& directory, std::string* error = nullptr);
    std::vector<SearchHit> queryEmbedding(const std::vector<float>& query,
                                           std::size_t limit = 10) const;
    std::size_t documentCount() const;
    std::size_t shardCount() const;
    std::uint64_t generation() const;

private:
    std::size_t shardFor(const SearchDocument& document) const;

    struct Shard {
        IndexBuilder builder;
        SearchNode node;
    };
    std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace miniKV::search
