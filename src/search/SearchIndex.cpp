#include "search/SearchIndex.hpp"

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <sstream>
#include <unordered_map>

namespace miniKV::search {

namespace {

void setError(std::string* error, const char* message) {
    if (error != nullptr) *error = message;
}

class BinaryWriter {
public:
    explicit BinaryWriter(const std::filesystem::path& path) : stream_(path, std::ios::binary | std::ios::trunc) {}

    template <typename T>
    void pod(T value) {
        stream_.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    void string(const std::string& value) {
        const std::uint64_t size = value.size();
        pod(size);
        stream_.write(value.data(), static_cast<std::streamsize>(size));
    }

    bool finish() {
        stream_.flush();
        const bool okay = stream_.good();
        stream_.close();
        return okay && !stream_.fail();
    }

    bool good() const { return stream_.good(); }

private:
    std::ofstream stream_;
};

class BinaryReader {
public:
    explicit BinaryReader(const std::filesystem::path& path) : stream_(path, std::ios::binary) {}

    template <typename T>
    bool pod(T& value) {
        return static_cast<bool>(stream_.read(reinterpret_cast<char*>(&value), sizeof(value)));
    }

    bool string(std::string& value) {
        std::uint64_t size = 0;
        if (!pod(size) || size > kMaxStringBytes) return false;
        value.resize(static_cast<std::size_t>(size));
        return size == 0 || static_cast<bool>(stream_.read(value.data(), static_cast<std::streamsize>(size)));
    }

    bool good() const { return stream_.good(); }
    bool eof() const { return stream_.eof(); }

    static constexpr std::uint64_t kMaxStringBytes = 64ULL * 1024ULL * 1024ULL;

private:
    std::ifstream stream_;
};

void writeDocument(BinaryWriter& writer, const SearchDocument& document) {
    writer.string(document.docId);
    writer.string(document.object.objectId);
    writer.pod(document.object.objectVersion);
    writer.string(document.tenantId);
    writer.string(document.mediaType);
    std::vector<std::pair<std::string, std::string>> metadata(document.metadata.begin(), document.metadata.end());
    std::sort(metadata.begin(), metadata.end());
    writer.pod(static_cast<std::uint64_t>(metadata.size()));
    for (const auto& entry : metadata) {
        writer.string(entry.first);
        writer.string(entry.second);
    }
    writer.pod(static_cast<std::uint8_t>(document.hasThumbnail ? 1 : 0));
    if (document.hasThumbnail) {
        writer.string(document.thumbnail.objectId);
        writer.pod(document.thumbnail.objectVersion);
    }
    writer.string(document.embeddingModelId);
    writer.pod(static_cast<std::uint64_t>(document.embedding.size()));
    for (float value : document.embedding) writer.pod(value);
    writer.string(document.processorVersion);
    writer.pod(document.indexGeneration);
    writer.string(document.state);
}

constexpr char kRouterMagic[] = "MKROUTER1";

struct RouterManifest {
    std::uint32_t format = 0;
    std::uint64_t generation = 0;
    std::size_t shardCount = 0;
};

std::filesystem::path generationShardPath(const std::filesystem::path& directory,
                                           std::size_t shard,
                                           std::uint64_t generation) {
    return directory / ("shard-" + std::to_string(shard) + ".g"
                        + std::to_string(generation) + ".index");
}

bool persistRouterManifest(const std::filesystem::path& path, std::uint64_t generation,
                           std::size_t shardCount, std::string* error) {
    std::filesystem::path temporary = path;
    temporary += ".tmp";
    BinaryWriter writer(temporary);
    writer.pod(static_cast<std::uint32_t>(sizeof(kRouterMagic) - 1));
    writer.string(kRouterMagic);
    // Format 2 names shard files by generation.  The manifest is the single
    // publication point, so a crash while writing a new generation leaves the
    // previous manifest and its files intact.
    writer.pod(static_cast<std::uint32_t>(2));
    writer.pod(generation);
    writer.pod(static_cast<std::uint64_t>(shardCount));
    if (!writer.finish()) {
        setError(error, "cannot write SearchShardRouter manifest");
        std::error_code ignored;
        std::filesystem::remove(temporary, ignored);
        return false;
    }
    std::error_code renameError;
    std::filesystem::rename(temporary, path, renameError);
    if (renameError) {
        setError(error, "cannot atomically publish SearchShardRouter manifest");
        std::error_code ignored;
        std::filesystem::remove(temporary, ignored);
        return false;
    }
    return true;
}

std::optional<RouterManifest> readRouterManifest(const std::filesystem::path& path,
                                                 std::string* error) {
    BinaryReader reader(path);
    std::uint32_t magicLength = 0;
    std::string magic;
    std::uint32_t format = 0;
    std::uint64_t generation = 0;
    std::uint64_t shardCount = 0;
    if (!reader.pod(magicLength) || magicLength != sizeof(kRouterMagic) - 1
        || !reader.string(magic) || magic != kRouterMagic
        || !reader.pod(format) || (format != 1 && format != 2)
        || !reader.pod(generation) || !reader.pod(shardCount)
        || shardCount == 0 || shardCount > 1'000'000
        || (!reader.good() && !reader.eof())) {
        setError(error, "invalid SearchShardRouter manifest");
        return std::nullopt;
    }
    return RouterManifest{format, generation, static_cast<std::size_t>(shardCount)};
}

bool readDocument(BinaryReader& reader, SearchDocument& document) {
    if (!reader.string(document.docId)
        || !reader.string(document.object.objectId)
        || !reader.pod(document.object.objectVersion)
        || !reader.string(document.tenantId)
        || !reader.string(document.mediaType)) return false;
    std::uint64_t metadataCount = 0;
    if (!reader.pod(metadataCount) || metadataCount > 1'000'000) return false;
    for (std::uint64_t i = 0; i < metadataCount; ++i) {
        std::string key;
        std::string value;
        if (!reader.string(key) || !reader.string(value)) return false;
        document.metadata.emplace(std::move(key), std::move(value));
    }
    std::uint8_t hasThumbnail = 0;
    if (!reader.pod(hasThumbnail) || hasThumbnail > 1) return false;
    document.hasThumbnail = hasThumbnail != 0;
    if (document.hasThumbnail
        && (!reader.string(document.thumbnail.objectId) || !reader.pod(document.thumbnail.objectVersion))) return false;
    if (!reader.string(document.embeddingModelId)) return false;
    std::uint64_t embeddingSize = 0;
    if (!reader.pod(embeddingSize) || embeddingSize > 10'000'000) return false;
    document.embedding.resize(static_cast<std::size_t>(embeddingSize));
    for (float& value : document.embedding) {
        if (!reader.pod(value)) return false;
    }
    return reader.string(document.processorVersion)
        && reader.pod(document.indexGeneration)
        && reader.string(document.state);
}

float cosine(const std::vector<float>& lhs, const std::vector<float>& rhs) {
    if (lhs.empty() || lhs.size() != rhs.size()) return -std::numeric_limits<float>::infinity();
    double dot = 0.0;
    double lhsNorm = 0.0;
    double rhsNorm = 0.0;
    for (std::size_t i = 0; i < lhs.size(); ++i) {
        dot += static_cast<double>(lhs[i]) * rhs[i];
        lhsNorm += static_cast<double>(lhs[i]) * lhs[i];
        rhsNorm += static_cast<double>(rhs[i]) * rhs[i];
    }
    if (lhsNorm <= 0.0 || rhsNorm <= 0.0) return -std::numeric_limits<float>::infinity();
    return static_cast<float>(dot / std::sqrt(lhsNorm * rhsNorm));
}

}  // namespace

bool validate(const SearchDocument& document, std::string* error) {
    if (document.docId.empty() || document.object.objectId.empty()
        || document.object.objectVersion <= 0) {
        setError(error, "SearchDocument requires a positive object reference");
        return false;
    }
    if (document.processorVersion.empty()) {
        setError(error, "SearchDocument requires processorVersion");
        return false;
    }
    if (document.state != "ready" && document.state != "indexed" && document.state != "stale") {
        setError(error, "SearchDocument has an invalid state");
        return false;
    }
    if (document.hasThumbnail
        && (document.thumbnail.objectId.empty() || document.thumbnail.objectVersion <= 0)) {
        setError(error, "SearchDocument thumbnail reference is invalid");
        return false;
    }
    if (!document.embedding.empty() && document.embeddingModelId.empty()) {
        setError(error, "embeddingModelId is required when an embedding is present");
        return false;
    }
    for (float value : document.embedding) {
        if (!std::isfinite(value)) {
            setError(error, "SearchDocument embedding contains a non-finite value");
            return false;
        }
    }
    return true;
}

std::string IndexBuilder::versionKey(const SearchDocument& document) {
    return document.object.objectId + "\x1f" + document.processorVersion;
}

ApplyResult IndexBuilder::apply(const SearchDocument& document, std::string* error) {
    if (!validate(document, error)) return ApplyResult::RejectedInvalid;
    if (document.state == "stale") {
        setError(error, "stale SearchDocument cannot be indexed");
        return ApplyResult::RejectedInvalid;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string key = versionKey(document);
    const auto existing = current_.find(key);
    if (existing != current_.end()) {
        if (existing->second.object.objectVersion > document.object.objectVersion) {
            setError(error, "older object version cannot replace a newer document");
            return ApplyResult::RejectedStale;
        }
        if (existing->second.object.objectVersion == document.object.objectVersion) {
            // Replayed READY events are safe.  Keep the first projection and
            // avoid producing duplicate pending entries.
            return ApplyResult::Idempotent;
        }
    }
    current_[key] = document;
    pending_.push_back(document);
    return ApplyResult::Applied;
}

SearchSegment IndexBuilder::flush(std::uint64_t generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    SearchSegment segment;
    segment.generation = generation;
    segment.documents.swap(pending_);
    return segment;
}

std::size_t IndexBuilder::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_.size();
}

bool IndexBuilder::restore(const std::vector<SearchDocument>& documents, std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    current_.clear();
    pending_.clear();
    for (const SearchDocument& document : documents) {
        if (!validate(document, error) || document.state == "stale") return false;
        const std::string key = versionKey(document);
        const auto existing = current_.find(key);
        if (existing == current_.end()
            || existing->second.object.objectVersion < document.object.objectVersion) {
            current_[key] = document;
        }
    }
    return true;
}

void SearchNode::install(SearchSegment segment) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (segment.generation < generation_) return;
    generation_ = segment.generation;
    segments_.push_back(std::move(segment));
}

bool SearchNode::persist(const std::filesystem::path& path, std::string* error) const {
    std::vector<SearchSegment> segments;
    std::uint64_t generation = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        segments = segments_;
        generation = generation_;
    }
    try {
        if (!path.parent_path().empty()) std::filesystem::create_directories(path.parent_path());
        std::filesystem::path temporary = path;
        temporary += ".tmp";
        BinaryWriter writer(temporary);
        const char magic[] = "MKSEARCH1";
        writer.pod(static_cast<std::uint32_t>(sizeof(magic) - 1));
        writer.string(magic);
        writer.pod(static_cast<std::uint32_t>(1));
        writer.pod(generation);
        writer.pod(static_cast<std::uint64_t>(segments.size()));
        for (const SearchSegment& segment : segments) {
            writer.pod(segment.generation);
            writer.pod(static_cast<std::uint64_t>(segment.documents.size()));
            for (const SearchDocument& document : segment.documents) writeDocument(writer, document);
        }
        // Close/flush before rename.  Besides making the publication safe on
        // platforms that reject renaming an open file, this prevents buffered
        // document bytes from being omitted from the atomically published
        // index.
        if (!writer.finish()) {
            setError(error, "cannot write SearchNode index");
            std::error_code ignored;
            std::filesystem::remove(temporary, ignored);
            return false;
        }
        std::error_code renameError;
        std::filesystem::rename(temporary, path, renameError);
        if (renameError) {
            setError(error, "cannot atomically publish SearchNode index");
            std::error_code ignored;
            std::filesystem::remove(temporary, ignored);
            return false;
        }
        return true;
    } catch (const std::exception&) {
        setError(error, "exception while persisting SearchNode index");
        return false;
    }
}

bool SearchNode::load(const std::filesystem::path& path, std::string* error) {
    try {
        BinaryReader reader(path);
        std::uint32_t magicLength = 0;
        std::string magic;
        std::uint32_t format = 0;
        std::uint64_t generation = 0;
        std::uint64_t segmentCount = 0;
        if (!reader.pod(magicLength) || magicLength != 9 || !reader.string(magic)
            || magic != "MKSEARCH1" || !reader.pod(format) || format != 1
            || !reader.pod(generation) || !reader.pod(segmentCount) || segmentCount > 1'000'000) {
            setError(error, "invalid SearchNode index header");
            return false;
        }
        std::vector<SearchSegment> segments;
        segments.reserve(static_cast<std::size_t>(segmentCount));
        std::uint64_t totalDocuments = 0;
        for (std::uint64_t i = 0; i < segmentCount; ++i) {
            SearchSegment segment;
            std::uint64_t documentCount = 0;
            if (!reader.pod(segment.generation) || !reader.pod(documentCount) || documentCount > 10'000'000
                || totalDocuments > 10'000'000 - documentCount) {
                setError(error, "invalid SearchNode segment");
                return false;
            }
            totalDocuments += documentCount;
            segment.documents.reserve(static_cast<std::size_t>(documentCount));
            for (std::uint64_t j = 0; j < documentCount; ++j) {
                SearchDocument document;
                if (!readDocument(reader, document) || !validate(document, error)) {
                    setError(error, "invalid SearchNode document");
                    return false;
                }
                segment.documents.push_back(std::move(document));
            }
            segments.push_back(std::move(segment));
        }
        if (!reader.good() && !reader.eof()) {
            setError(error, "truncated SearchNode index");
            return false;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (generation < generation_) {
            setError(error, "older SearchNode generation cannot replace newer state");
            return false;
        }
        generation_ = generation;
        segments_ = std::move(segments);
        return true;
    } catch (const std::exception&) {
        setError(error, "exception while loading SearchNode index");
        return false;
    }
}

std::vector<SearchDocument> SearchNode::snapshotDocuments() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_map<std::string, SearchDocument> latest;
    for (const SearchSegment& segment : segments_) {
        for (const SearchDocument& document : segment.documents) {
            const std::string key = document.object.objectId + "\x1f" + document.processorVersion;
            const auto it = latest.find(key);
            if (it == latest.end()
                || it->second.object.objectVersion < document.object.objectVersion) {
                latest[key] = document;
            }
        }
    }
    std::vector<SearchDocument> result;
    result.reserve(latest.size());
    for (auto& entry : latest) result.push_back(std::move(entry.second));
    return result;
}

std::vector<SearchHit> SearchNode::queryEmbedding(const std::vector<float>& query,
                                                  std::size_t limit) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_map<std::string, SearchHit> best;
    for (const SearchSegment& segment : segments_) {
        for (const SearchDocument& document : segment.documents) {
            const float score = cosine(query, document.embedding);
            if (!std::isfinite(score)) continue;
            const std::string key = document.object.objectId + "\x1f" + document.processorVersion;
            auto it = best.find(key);
            if (it == best.end()
                || document.object.objectVersion > it->second.document.object.objectVersion
                || (document.object.objectVersion == it->second.document.object.objectVersion
                    && score > it->second.score)) {
                best[key] = SearchHit{document, score};
            }
        }
    }
    std::vector<SearchHit> result;
    result.reserve(best.size());
    for (auto& entry : best) result.push_back(std::move(entry.second));
    std::sort(result.begin(), result.end(), [](const SearchHit& lhs, const SearchHit& rhs) {
        if (lhs.score != rhs.score) return lhs.score > rhs.score;
        return lhs.document.docId < rhs.document.docId;
    });
    if (result.size() > limit) result.resize(limit);
    return result;
}

std::size_t SearchNode::documentCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_map<std::string, bool> ids;
    for (const SearchSegment& segment : segments_) {
        for (const SearchDocument& document : segment.documents) {
            ids[document.object.objectId + "\x1f" + document.processorVersion] = true;
        }
    }
    return ids.size();
}

std::uint64_t SearchNode::generation() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return generation_;
}

SearchShardRouter::SearchShardRouter(std::size_t shardCount) {
    shardCount = std::max<std::size_t>(1, shardCount);
    shards_.reserve(shardCount);
    for (std::size_t i = 0; i < shardCount; ++i) shards_.push_back(std::make_unique<Shard>());
}

std::size_t SearchShardRouter::shardFor(const SearchDocument& document) const {
    // FNV-1a is stable across processes and standard-library implementations.
    std::uint64_t hash = 1469598103934665603ULL;
    const std::string key = document.object.objectId + "\x1f" + document.processorVersion;
    for (unsigned char value : key) {
        hash ^= value;
        hash *= 1099511628211ULL;
    }
    return static_cast<std::size_t>(hash % shards_.size());
}

ApplyResult SearchShardRouter::apply(const SearchDocument& document, std::string* error) {
    return shards_[shardFor(document)]->builder.apply(document, error);
}

bool SearchShardRouter::flushAndPersist(const std::filesystem::path& directory,
                                        std::uint64_t generation, std::string* error) {
    try {
        // Reject a backwards publication before draining any builder.  A
        // lower generation must never silently discard pending documents
        // merely because SearchNode::install() protects its current state.
        if (generation < this->generation()) {
            setError(error, "older SearchShardRouter generation cannot be published");
            return false;
        }
        std::filesystem::create_directories(directory);
        for (std::size_t index = 0; index < shards_.size(); ++index) {
            Shard& shard = *shards_[index];
            shard.node.install(shard.builder.flush(generation));
            const auto path = generationShardPath(directory, index, generation);
            if (!shard.node.persist(path, error)) return false;
        }
        // Publish one manifest only after every shard has been persisted. A
        // loader can therefore reject a directory from a partially completed
        // flush instead of mixing generations across shards.
        const std::uint64_t publishedGeneration = shards_.empty() ? 0 : shards_.front()->node.generation();
        for (const auto& shard : shards_) {
            if (shard->node.generation() != publishedGeneration) {
                setError(error, "SearchShardRouter shards have mixed generations");
                return false;
            }
        }
        if (!persistRouterManifest(directory / "manifest", publishedGeneration, shards_.size(), error)) {
            return false;
        }

        // Old generation files are no longer referenced by the manifest.  A
        // best-effort cleanup is safe after publication: if the process dies
        // during cleanup, the new manifest still points to complete files and
        // orphaned old files are ignored on recovery.
        std::error_code cleanupError;
        for (const auto& entry : std::filesystem::directory_iterator(directory, cleanupError)) {
            if (cleanupError || !entry.is_regular_file(cleanupError)) continue;
            const std::string name = entry.path().filename().string();
            if (name.find(".g") == std::string::npos || name.rfind(".index") != name.size() - 6) continue;
            if (name.find("shard-") != 0) continue;
            if (entry.path() == generationShardPath(directory, 0, publishedGeneration)) {
                // Keep the current file; the generic check below handles the
                // other shards without assuming that shard zero exists.
                continue;
            }
            bool current = false;
            for (std::size_t index = 0; index < shards_.size(); ++index) {
                if (entry.path() == generationShardPath(directory, index, publishedGeneration)) {
                    current = true;
                    break;
                }
            }
            if (!current) std::filesystem::remove(entry.path(), cleanupError);
        }
        return true;
    } catch (const std::exception&) {
        setError(error, "exception while persisting SearchNode shards");
        return false;
    }
}

bool SearchShardRouter::load(const std::filesystem::path& directory, std::string* error) {
    try {
        const auto manifestPath = directory / "manifest";
        if (!std::filesystem::exists(manifestPath)) {
            setError(error, "missing SearchShardRouter manifest");
            return false;
        }
        const auto manifest = readRouterManifest(manifestPath, error);
        if (!manifest.has_value() || manifest->shardCount != shards_.size()) {
            if (manifest.has_value()) setError(error, "SearchShardRouter shard count mismatch");
            return false;
        }
        for (std::size_t index = 0; index < shards_.size(); ++index) {
            Shard& shard = *shards_[index];
            const auto path = manifest->format == 2
                ? generationShardPath(directory, index, manifest->generation)
                : directory / ("shard-" + std::to_string(index) + ".index");
            // A shard directory is a single recovery unit.  Silently
            // accepting a missing shard would expose a partial index as if
            // it were complete and could hide data loss after a failed
            // publication.
            if (!std::filesystem::exists(path)) {
                setError(error, "missing SearchNode shard index");
                return false;
            }
            if (!shard.node.load(path, error)
                || !shard.builder.restore(shard.node.snapshotDocuments(), error)) return false;
            if (shard.node.generation() != manifest->generation) {
                setError(error, "SearchShardRouter shard generation mismatch");
                return false;
            }
        }
        return true;
    } catch (const std::exception&) {
        setError(error, "exception while loading SearchNode shards");
        return false;
    }
}

std::vector<SearchHit> SearchShardRouter::queryEmbedding(const std::vector<float>& query,
                                                         std::size_t limit) const {
    std::vector<SearchHit> result;
    for (const auto& shard : shards_) {
        auto hits = shard->node.queryEmbedding(query, std::numeric_limits<std::size_t>::max());
        result.insert(result.end(), hits.begin(), hits.end());
    }
    std::sort(result.begin(), result.end(), [](const SearchHit& lhs, const SearchHit& rhs) {
        if (lhs.score != rhs.score) return lhs.score > rhs.score;
        return lhs.document.docId < rhs.document.docId;
    });
    std::unordered_map<std::string, bool> seen;
    std::vector<SearchHit> unique;
    for (auto& hit : result) {
        const std::string key = hit.document.object.objectId + "\x1f" + hit.document.processorVersion;
        if (!seen.emplace(key, true).second) continue;
        unique.push_back(std::move(hit));
        if (unique.size() >= limit) break;
    }
    return unique;
}

std::size_t SearchShardRouter::documentCount() const {
    std::size_t count = 0;
    for (const auto& shard : shards_) count += shard->node.documentCount();
    return count;
}

std::size_t SearchShardRouter::shardCount() const { return shards_.size(); }

std::uint64_t SearchShardRouter::generation() const {
    std::uint64_t result = 0;
    for (const auto& shard : shards_) {
        result = std::max(result, shard->node.generation());
    }
    return result;
}

}  // namespace miniKV::search
