#pragma once
#include "Handler.hpp"
#include "storage/IMetaStorage.hpp"

namespace miniKV {
namespace handler {

using namespace http;
using namespace network;

class MoveHandler : public Handler {
public:
    explicit MoveHandler( storage::IMetaStorage* metaStorage) : metaStorage(metaStorage) {}
    
    bool match(const HttpRequest& req) override {
        return req.path() == "/api/files/move"
            && req.method() == HttpRequest::kPost;
    }

    void handle(const HttpRequest& req, HttpResponse* resp,
                const TcpConnectionPtr&) override {
        auto extract = [&req](const std::string& key) -> std::string {
            const std::string& body = req.body();
            size_t p = body.find('"' + key + '"');
            if (p == std::string::npos) return "";
            p = body.find('"', p + key.size() + 3);
            if (p == std::string::npos) return "";
            size_t e = body.find('"', p + 1);
            if (e == std::string::npos) return "";
            return body.substr(p + 1, e - p - 1);
        };

        std::string oldPath = extract("old_path");
        std::string oldName = extract("old_name");
        std::string newPath = extract("new_path");
        std::string newName = extract("new_name");

        if (oldPath.empty() || oldName.empty() || newPath.empty()) {
            resp->setStatusCode(HttpResponse::k400BadRequest);
            resp->setBody("Missing old_path/old_name/new_path");
            return;
        }
        if (newName.empty()) newName = oldName;

        if (metaStorage->moveByPath(oldPath, oldName, newPath, newName)) {
            resp->setStatusCode(HttpResponse::k200Ok);
            resp->setContentType("application/json");
            resp->setBody("{\"status\":\"ok\"}");
        } else {
            resp->setStatusCode(HttpResponse::k404NotFound);
            resp->setBody("Move failed");
        }
    }

private:
    storage::IMetaStorage* metaStorage;
};


}

} // namespace miniKV
