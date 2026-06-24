#pragma once
#include "Handler.hpp"
#include "Common.hpp"
#include "http/HttpResponse.hpp"
#include "storage/IDateStorage.hpp"
#include "storage/IMetaStorage.hpp"
#include <linux/limits.h>
#include <openssl/types.h>
#include <string>
#include <unistd.h>




namespace miniKV {
namespace handler {
    using namespace http;
    using namespace network;
class DownloadHandler : public Handler {
public:
    explicit DownloadHandler(storage::IMetaStorage* metaStorage, storage::IDateStorage* dataStorage) 
                            : metaStorage(metaStorage), dataStorage(dataStorage){}
    bool match(const HttpRequest& req) override {
        return req.path().find("/api/files/") == 0
            && req.path().find("/download") != std::string::npos;
    }
    
    void handle(const HttpRequest& req, HttpResponse* resp,
        const TcpConnectionPtr&) override {
        size_t start = 11;  // "/api/files/" 长度
        size_t end   = req.path().find("/download", start);
        if (end == std::string::npos) {
            resp->setStatusCode(HttpResponse::k400BadRequest);
            resp->setBody("Invalid URL");
            return;
        }
        std::string hash = req.path().substr(start, end - start);

        // 优先从 path+name 查（有 file_name），fallback 到 hash
        storage::MetaInfo meta;
        bool found = false;
        std::string fp = urlDecode(getQueryParam(req.query(), "path"));
        std::string fn = urlDecode(getQueryParam(req.query(), "name"));
        if (!fp.empty() && !fn.empty())
            found = metaStorage->getMetaByPath(fp, fn, meta);
        if (!found)
            found = metaStorage->getMeta(hash, meta);
        if (!found) {
            resp->setStatusCode(HttpResponse::k404NotFound);
            resp->setBody("File not found");
            return;
        }

        std::string fname = meta.file_name.empty() ? "download" : meta.file_name;
        bool forceDl = !getQueryParam(req.query(), "dl").empty();
        std::string dispos = forceDl ? "attachment" : "inline";

        std::string diskPath = dataStorage->filePath(hash);
        if(access(diskPath.c_str(), F_OK) != 0)
        {
            resp->setStatusCode(HttpResponse::k404NotFound);
            resp->setBody("File data not found on disk");
            return ;
        }
        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType(getMimeType(fname));
        resp->addHeader("Content-Disposition",
                        dispos + "; filename=\"" + fname + "\"");
        resp->addHeader("Cache-Control", "max-age=3600");
        struct stat st;
        if(stat(diskPath.c_str(), &st) != 0)
        {
            resp->setStatusCode(HttpResponse::k404NotFound);
            resp->setBody("File data not found on disk");
            return ;
        }
        resp->setFileBody(diskPath, st.st_size);  // sendfile 零拷贝
    }
private:
    storage::IMetaStorage* metaStorage;
    storage::IDateStorage* dataStorage;
};

}
}





















