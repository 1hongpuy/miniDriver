#include "search/SearchIndex.hpp"

#include <cassert>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>

using miniKV::search::ApplyResult;
using miniKV::search::IndexBuilder;
using miniKV::search::ObjectRef;
using miniKV::search::SearchDocument;
using miniKV::search::SearchNode;
using miniKV::search::SearchShardRouter;

SearchDocument document(std::int64_t version, const char* id, std::vector<float> embedding) {
    SearchDocument value;
    value.docId = id;
    value.object = ObjectRef{"source", version};
    value.tenantId = "tenant-a";
    value.mediaType = "image";
    value.processorVersion = "image-index-v1";
    value.thumbnail = ObjectRef{"thumbnail", 1};
    value.hasThumbnail = true;
    value.embeddingModelId = "test-v1";
    value.embedding = std::move(embedding);
    return value;
}

int main() {
    const auto root = std::filesystem::temp_directory_path() / "minikv-search-index-test";
    std::error_code cleanupError;
    std::filesystem::remove_all(root, cleanupError);

    IndexBuilder builder;
    std::string error;
    assert(builder.apply(document(1, "doc-v1", {1.0F, 0.0F}), &error) == ApplyResult::Applied);
    assert(builder.apply(document(1, "doc-v1", {1.0F, 0.0F}), &error) == ApplyResult::Idempotent);
    assert(builder.apply(document(0, "invalid", {1.0F}), &error) == ApplyResult::RejectedInvalid);
    // A newer object version may reuse the business doc id.  The projection
    // must still expose only the newest version for this object/processor.
    assert(builder.apply(document(2, "doc-v1", {0.0F, 1.0F}), &error) == ApplyResult::Applied);
    assert(builder.apply(document(1, "old", {1.0F, 0.0F}), &error) == ApplyResult::RejectedStale);
    assert(builder.size() == 1);

    SearchNode node;
    node.install(builder.flush(7));
    assert(node.generation() == 7);
    assert(node.documentCount() == 1);
    const auto hits = node.queryEmbedding({0.0F, 1.0F}, 2);
    assert(hits.size() == 1);
    assert(hits[0].document.docId == "doc-v1");
    assert(std::fabs(hits[0].score - 1.0F) < 1e-5F);
    assert(hits[0].document.object.objectVersion == 2);

    // Older segments cannot roll the projection generation backwards.
    node.install(builder.flush(6));
    assert(node.generation() == 7);

    // Router-level generation checks must happen before draining pending
    // documents, otherwise a rejected backwards flush could lose them.
    SearchShardRouter guarded(1);
    assert(guarded.apply(document(1, "guarded", {1.0F, 0.0F}), &error) == ApplyResult::Applied);
    assert(guarded.flushAndPersist(root, 20, &error));
    assert(guarded.apply(document(2, "guarded-new", {0.0F, 1.0F}), &error) == ApplyResult::Applied);
    assert(!guarded.flushAndPersist(root, 19, &error));
    assert(guarded.flushAndPersist(root, 21, &error));
    assert(guarded.documentCount() == 2);

    SearchShardRouter router(3);
    assert(router.apply(document(1, "doc-a", {1.0F, 0.0F}), &error) == ApplyResult::Applied);
    assert(router.apply(document(1, "doc-b", {0.0F, 1.0F}), &error) == ApplyResult::Applied);
    assert(router.flushAndPersist(root, 11, &error));
    assert(router.documentCount() == 2);
    // A generation-scoped publication keeps the prior generation available
    // until the new manifest is atomically installed.
    assert(std::filesystem::exists(root / "shard-0.g11.index")
           || std::filesystem::exists(root / "shard-1.g11.index")
           || std::filesystem::exists(root / "shard-2.g11.index"));
    // A torn next-generation file is ignored while the manifest still points
    // at generation 11.  Recovery must therefore retain the last complete
    // publication instead of trying to infer state from directory contents.
    {
        std::ofstream torn(root / "shard-0.g12.index", std::ios::binary);
        torn << "torn";
    }
    SearchShardRouter tornRecovery(3);
    assert(tornRecovery.load(root, &error));
    assert(tornRecovery.generation() == 11);

    SearchShardRouter restored(3);
    assert(restored.load(root, &error));
    assert(restored.documentCount() == 2);
    assert(restored.apply(document(1, "doc-a-old", {1.0F, 0.0F}), &error) == ApplyResult::RejectedStale);
    assert(restored.apply(document(2, "doc-a-new", {0.8F, 0.2F}), &error) == ApplyResult::Applied);
    assert(restored.flushAndPersist(root, 12, &error));
    SearchShardRouter reloaded(3);
    assert(reloaded.load(root, &error));
    assert(reloaded.documentCount() == 2);
    const auto shardHits = reloaded.queryEmbedding({1.0F, 0.0F}, 10);
    assert(shardHits.size() == 2);
    assert(shardHits[0].document.object.objectVersion == 2);
    assert(reloaded.shardCount() == 3);
    std::filesystem::remove(root / "shard-1.g12.index", cleanupError);
    SearchShardRouter partial(3);
    assert(!partial.load(root, &error));
    std::filesystem::remove_all(root, cleanupError);

    std::cout << "search index tests passed\n";
    return 0;
}
