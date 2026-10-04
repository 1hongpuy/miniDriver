#include "http/HttpRequest.hpp"
#include "http/HttpResponse.hpp"
#include "http/HttpServer.hpp"
#include "network/EventLoop.hpp"
#include "search/SearchIndex.hpp"
#include "utils/ThreadPool.hpp"
#include "utils/Util.hpp"

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <algorithm>
#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace {

using miniKV::http::HttpRequest;
using miniKV::http::HttpResponse;
using miniKV::search::ApplyResult;
using miniKV::search::ObjectRef;
using miniKV::search::SearchDocument;
using miniKV::search::SearchHit;
using miniKV::search::SearchShardRouter;
using miniKV::utils::ThreadPool;

using boost::property_tree::ptree;

std::atomic<bool> g_stop{false};

void onSignal(int) { g_stop.store(true, std::memory_order_relaxed); }

std::uint64_t envUint(const char* name, std::uint64_t fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') return fallback;
    try {
        const auto parsed = std::stoull(value);
        return parsed == 0 ? fallback : parsed;
    } catch (...) {
        return fallback;
    }
}

std::string envString(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    return value == nullptr || *value == '\0' ? fallback : std::string(value);
}

void json(HttpResponse* response, HttpResponse::HttpStatusCode status,
          const std::string& body) {
    response->setStatusCode(status);
    response->setContentType("application/json");
    response->setBody(body);
    response->setCloseConnection(false);
}

std::string quoted(const std::string& value) {
    return "\"" + miniKV::util::jsonEscape(value) + "\"";
}

bool parseJson(const std::string& body, ptree& tree, std::string& error) {
    try {
        std::istringstream input(body);
        boost::property_tree::read_json(input, tree);
        return true;
    } catch (const std::exception& exception) {
        error = exception.what();
        return false;
    }
}

bool parseObjectRef(const ptree& tree, const std::string& prefix, ObjectRef& out,
                   std::string& error, bool required) {
    const auto objectId = tree.get_optional<std::string>(prefix + "object_id");
    const auto version = tree.get_optional<std::int64_t>(prefix + "object_version");
    if (!objectId && !version && !required) return true;
    if (!objectId || !version || objectId->empty() || *version <= 0) {
        error = "invalid " + prefix + "object reference";
        return false;
    }
    out.objectId = *objectId;
    out.objectVersion = *version;
    return true;
}

bool parseDocument(const std::string& body, SearchDocument& document,
                   std::string& error) {
    ptree tree;
    if (!parseJson(body, tree, error)) return false;

    document.docId = tree.get<std::string>("doc_id", "");
    document.object.objectId = tree.get<std::string>("object_id", "");
    document.object.objectVersion = tree.get<std::int64_t>("object_version", 0);
    document.tenantId = tree.get<std::string>("tenant_id", "");
    document.mediaType = tree.get<std::string>("media_type", "");
    document.processorVersion = tree.get<std::string>("processor_version", "");
    document.indexGeneration = tree.get<std::uint64_t>("index_generation", 0);
    document.state = tree.get<std::string>("state", "ready");

    if (const auto metadata = tree.get_child_optional("metadata")) {
        for (const auto& entry : *metadata) {
            document.metadata[entry.first] = entry.second.get_value<std::string>();
        }
    }

    const ptree* thumbnail = nullptr;
    if (const auto value = tree.get_child_optional("thumbnail")) thumbnail = &*value;
    else if (const auto value = tree.get_child_optional("thumbnail_ref")) thumbnail = &*value;
    if (thumbnail != nullptr) {
        // JSON null is represented by an empty property tree.  Treat it as
        // an absent optional reference; an actual object is validated below.
        const auto thumbnailId = thumbnail->get_optional<std::string>("object_id");
        const auto thumbnailVersion = thumbnail->get_optional<std::int64_t>("object_version");
        const std::string rawThumbnail = thumbnail->data();
        const bool explicitNull = rawThumbnail == "null";
        if (!explicitNull && (thumbnailId || thumbnailVersion || !thumbnail->empty() || !rawThumbnail.empty())) {
            document.hasThumbnail = true;
            document.thumbnail.objectId = thumbnail->get<std::string>("object_id", "");
            document.thumbnail.objectVersion = thumbnail->get<std::int64_t>("object_version", 0);
        }
    }

    if (const auto embedding = tree.get_child_optional("embedding")) {
        document.embeddingModelId = embedding->get<std::string>("model_id", "");
        if (const auto vector = embedding->get_child_optional("vector")) {
            for (const auto& value : *vector) {
                try {
                    document.embedding.push_back(value.second.get_value<float>());
                } catch (const std::exception&) {
                    error = "embedding vector contains a non-number";
                    return false;
                }
            }
        }
    }

    if (document.docId.empty() || document.object.objectId.empty()
        || document.object.objectVersion <= 0 || document.processorVersion.empty()) {
        error = "document requires doc_id, object_id, positive object_version and processor_version";
        return false;
    }
    if (document.hasThumbnail
        && (document.thumbnail.objectId.empty() || document.thumbnail.objectVersion <= 0)) {
        error = "thumbnail object reference is invalid";
        return false;
    }
    return true;
}

std::string documentJson(const SearchDocument& document) {
    std::ostringstream output;
    output << "{\"doc_id\":" << quoted(document.docId)
           << ",\"object_id\":" << quoted(document.object.objectId)
           << ",\"object_version\":" << document.object.objectVersion
           << ",\"tenant_id\":" << quoted(document.tenantId)
           << ",\"media_type\":" << quoted(document.mediaType)
           << ",\"metadata\":{";
    std::vector<std::pair<std::string, std::string>> metadata(document.metadata.begin(),
                                                               document.metadata.end());
    std::sort(metadata.begin(), metadata.end());
    for (std::size_t index = 0; index < metadata.size(); ++index) {
        if (index != 0) output << ',';
        output << quoted(metadata[index].first) << ':' << quoted(metadata[index].second);
    }
    output << "},\"thumbnail_ref\":";
    if (document.hasThumbnail) {
        output << "{\"object_id\":" << quoted(document.thumbnail.objectId)
               << ",\"object_version\":" << document.thumbnail.objectVersion << '}';
    } else {
        output << "null";
    }
    output << ",\"embedding\":";
    if (!document.embedding.empty()) {
        output << "{\"model_id\":" << quoted(document.embeddingModelId) << ",\"vector\":[";
        output << std::setprecision(9);
        for (std::size_t index = 0; index < document.embedding.size(); ++index) {
            if (index != 0) output << ',';
            output << document.embedding[index];
        }
        output << "]}";
    } else {
        output << "null";
    }
    output << ",\"processor_version\":" << quoted(document.processorVersion)
           << ",\"index_generation\":" << document.indexGeneration
           << ",\"state\":" << quoted(document.state) << '}';
    return output.str();
}

std::string hitJson(const SearchHit& hit) {
    std::string result = documentJson(hit.document);
    if (!result.empty() && result.back() == '}') result.pop_back();
    std::ostringstream output;
    output << result << ",\"score\":" << std::setprecision(9) << hit.score << '}';
    return output.str();
}

struct ServiceState {
    explicit ServiceState(std::size_t shards, std::filesystem::path directory)
        : router(shards), indexDirectory(std::move(directory)) {}

    SearchShardRouter router;
    std::filesystem::path indexDirectory;
    std::mutex mutex;
};

void handleRequest(const HttpRequest& request, HttpResponse* response,
                   ServiceState& state) {
    const std::string path = request.path();
    if (request.method() == HttpRequest::kGet && path == "/healthz") {
        std::lock_guard<std::mutex> lock(state.mutex);
        std::ostringstream output;
        output << "{\"status\":\"ok\",\"documents\":" << state.router.documentCount()
               << ",\"shards\":" << state.router.shardCount()
               << ",\"generation\":" << state.router.generation() << '}';
        json(response, HttpResponse::k200Ok, output.str());
        return;
    }

    if (request.method() == HttpRequest::kPost && path == "/v1/index/documents") {
        SearchDocument document;
        std::string error;
        if (!parseDocument(request.body(), document, error)) {
            json(response, HttpResponse::k400BadRequest, miniKV::util::jsonError(error));
            return;
        }
        std::lock_guard<std::mutex> lock(state.mutex);
        const ApplyResult result = state.router.apply(document, &error);
        const char* name = "rejected_invalid";
        HttpResponse::HttpStatusCode status = HttpResponse::k400BadRequest;
        if (result == ApplyResult::Applied) {
            name = "applied";
            status = HttpResponse::k201Created;
        } else if (result == ApplyResult::Idempotent) {
            name = "idempotent";
            status = HttpResponse::k200Ok;
        } else if (result == ApplyResult::RejectedStale) {
            name = "rejected_stale";
            status = HttpResponse::k409Conflict;
        }
        std::ostringstream output;
        output << "{\"result\":\"" << name << "\",\"documents\":"
               << state.router.documentCount();
        if (!error.empty()) output << ",\"error\":" << quoted(error);
        output << '}';
        json(response, status, output.str());
        return;
    }

    if (request.method() == HttpRequest::kPost && path == "/v1/index/query") {
        ptree tree;
        std::string error;
        if (!parseJson(request.body(), tree, error)) {
            json(response, HttpResponse::k400BadRequest, miniKV::util::jsonError(error));
            return;
        }
        std::vector<float> query;
        if (const auto vector = tree.get_child_optional("embedding")) {
            for (const auto& value : *vector) {
                try {
                    query.push_back(value.second.get_value<float>());
                } catch (const std::exception&) {
                    error = "query embedding contains a non-number";
                    break;
                }
            }
        }
        const std::size_t limit = static_cast<std::size_t>(
            tree.get<std::uint64_t>("limit", 10));
        if (query.empty() || query.size() > 10'000 || limit > 10'000 || !error.empty()) {
            json(response, HttpResponse::k400BadRequest,
                 miniKV::util::jsonError(error.empty() ? "embedding and valid limit are required" : error));
            return;
        }
        std::lock_guard<std::mutex> lock(state.mutex);
        const auto hits = state.router.queryEmbedding(query, limit);
        std::ostringstream output;
        output << "{\"hits\":[";
        for (std::size_t index = 0; index < hits.size(); ++index) {
            if (index != 0) output << ',';
            output << hitJson(hits[index]);
        }
        output << "]}";
        json(response, HttpResponse::k200Ok, output.str());
        return;
    }

    if (request.method() == HttpRequest::kPost && path == "/v1/index/flush") {
        ptree tree;
        std::string error;
        if (!parseJson(request.body().empty() ? "{}" : request.body(), tree, error)) {
            json(response, HttpResponse::k400BadRequest, miniKV::util::jsonError(error));
            return;
        }
        std::lock_guard<std::mutex> lock(state.mutex);
        const std::uint64_t requested = tree.get<std::uint64_t>("generation", 0);
        const std::uint64_t generation = requested == 0 ? state.router.generation() + 1 : requested;
        if (!state.router.flushAndPersist(state.indexDirectory, generation, &error)) {
            json(response, HttpResponse::k500InternalServerError, miniKV::util::jsonError(error));
            return;
        }
        std::ostringstream output;
        output << "{\"status\":\"flushed\",\"generation\":" << state.router.generation()
               << ",\"documents\":" << state.router.documentCount() << '}';
        json(response, HttpResponse::k200Ok, output.str());
        return;
    }

    if (request.method() == HttpRequest::kPost && path == "/v1/index/reload") {
        std::string error;
        std::lock_guard<std::mutex> lock(state.mutex);
        if (!state.router.load(state.indexDirectory, &error)) {
            json(response, HttpResponse::k500InternalServerError, miniKV::util::jsonError(error));
            return;
        }
        std::ostringstream output;
        output << "{\"status\":\"reloaded\",\"generation\":" << state.router.generation()
               << ",\"documents\":" << state.router.documentCount() << '}';
        json(response, HttpResponse::k200Ok, output.str());
        return;
    }

    json(response, HttpResponse::k404NotFound, miniKV::util::jsonError("route not found"));
}

}  // namespace

int main(int argc, char** argv) {
    const int port = argc > 1 ? std::stoi(argv[1])
                              : static_cast<int>(envUint("MINIKV_SEARCH_PORT", 19090));
    const std::filesystem::path directory = argc > 2
        ? argv[2] : envString("MINIKV_SEARCH_INDEX_DIR", "data/search-index");
    const std::size_t shards = argc > 3
        ? static_cast<std::size_t>(std::stoull(argv[3]))
        : static_cast<std::size_t>(envUint("MINIKV_SEARCH_SHARDS", 1));
    const std::size_t ioThreads = static_cast<std::size_t>(envUint("MINIKV_SEARCH_IO_THREADS", 2));
    if (port <= 0 || port > 65535 || shards == 0 || shards > 1024 || ioThreads == 0) {
        std::cerr << "invalid SearchNode server configuration\n";
        return 2;
    }

    ServiceState state(shards, directory);
    if (std::filesystem::exists(directory / "manifest")) {
        std::string error;
        std::lock_guard<std::mutex> lock(state.mutex);
        if (!state.router.load(directory, &error)) {
            std::cerr << "cannot load SearchNode index: " << error << '\n';
            return 3;
        }
    }

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    miniKV::network::EventLoop loop;
    ThreadPool workers(ioThreads);
    miniKV::http::HttpServer server(&loop, &workers, port);
    server.setThreadNum(ioThreads);
    server.setHttpCallback([&state](const HttpRequest& request, HttpResponse* response,
                                    const miniKV::network::TcpConnectionPtr&, const miniKV::http::DeferredResponse::Ptr&) {
        handleRequest(request, response, state);
    });
    server.start();
    loop.runEvery(100, [&loop] {
        if (g_stop.load(std::memory_order_relaxed)) loop.quit();
    });
    std::cout << "search_node_started port=" << port << " shards=" << shards
              << " index_dir=" << directory << '\n';
    loop.loop();
    return 0;
}
