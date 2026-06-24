#pragma once
#include "Handler.hpp"
#include "Common.hpp"
#include "storage/IMetaStorage.hpp"

namespace miniKV {
namespace handler {

using namespace http;
using namespace network;

class DeleteHandler : public Handler {
public:
    explicit DeleteHandler(storage::IMetaStorage* metaStorage) :metaStorage(metaStorage) {}
    bool match(const HttpRequest& req) override {
        return req.path() == "/api/files"
            && req.method() == HttpRequest::kDelete;
    }

    void handle(const HttpRequest& req, HttpResponse* resp,
                const TcpConnectionPtr&) override {
        std::string filePath = urlDecode(getQueryParam(req.query(), "path"));
        std::string fileName = urlDecode(getQueryParam(req.query(), "name"));

        if (filePath.empty() || fileName.empty()) {
            resp->setStatusCode(HttpResponse::k400BadRequest);
            resp->setBody("Missing path or name");
            return;
        }

        if (metaStorage->removeByPath(filePath, fileName)) {
            resp->setStatusCode(HttpResponse::k200Ok);
            resp->setContentType("application/json");
            resp->setBody("{\"status\":\"ok\"}");
        } else {
            resp->setStatusCode(HttpResponse::k404NotFound);
            resp->setBody("File not found");
        }
    }
private:
    storage::IMetaStorage* metaStorage;
};


}
} // namespace miniKV
