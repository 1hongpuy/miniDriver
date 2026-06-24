#pragma once


#include "Handler.hpp"
#include "Common.hpp"
#include "http/HttpRequest.hpp"
#include "storage/IMetaStorage.hpp"
#include <memory>
#include <string>




namespace miniKV {

namespace handler {

class FileListHandler : public Handler {
public:
    explicit FileListHandler(storage::IMetaStorage* metaStorage) :metaStorage(metaStorage){

    }
    bool match(const http::HttpRequest& req) override {
        return req.path() == "/api/files" && req.method() == http::HttpRequest::kGet; 
    }
    void handle(const http::HttpRequest& req, 
        http::HttpResponse* resp, 
        const network::TcpConnectionPtr& conn) override {
            std::string dirPath = normalizePath(
                urlDecode(getQueryParam(req.query(), "path")));

            auto files   = metaStorage->listByPath(dirPath);
            auto subDirs = metaStorage->listSubDirs(dirPath);

            std::string json = "{\"path\":\"" + escapeJson(dirPath) + "\",";
            json += "\"folders\":[";
            for (size_t i = 0; i < subDirs.size(); ++i) {
                if (i > 0) json += ",";
                json += "\"" + escapeJson(subDirs[i]) + "\"";
            }
            json += "],\"files\":[";
            for (size_t i = 0; i < files.size(); ++i) {
                if (i > 0) json += ",";
                json += "{";
                json += "\"file_name\":\""    + escapeJson(files[i].file_name)    + "\",";
                json += "\"file_hash\":\""    + escapeJson(files[i].file_hash)    + "\",";
                json += "\"file_path\":\""    + escapeJson(files[i].file_path)    + "\",";
                json += "\"file_size\":"      + std::to_string(files[i].file_size) + ",";
                json += "\"file_time\":\""    + escapeJson(files[i].file_time)    + "\",";
                json += "\"content_type\":\"" + escapeJson(files[i].content_type)  + "\"";
                json += "}";
            }
            json += "]}";
    
            resp->setStatusCode(http::HttpResponse::k200Ok);
            resp->setContentType("application/json; charset=utf-8");
            resp->setBody(json);
            resp->setCloseConnection(false);
        }
private:
    storage::IMetaStorage* metaStorage;
};

}



}
























