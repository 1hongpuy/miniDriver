#pragma  once




#include "Handler.hpp"
#include "Common.hpp"
#include <string>

namespace miniKV {
namespace handler {

    using namespace http;
    using namespace network;
    class StaticFileHandler : public Handler {
    public:
        explicit StaticFileHandler(const char* wwwDir) :wwwDir(wwwDir){}
        bool match(const HttpRequest& req) override {
            std::string p = req.path();
            return p == "/" || p == "/index.html"
                || p == "/style.css" || p == "/app.js"
                || p == "/favicon.ico" || p == "/favicon.svg";
        }
    
        void handle(const HttpRequest& req, HttpResponse* resp,
                    const TcpConnectionPtr&) override {
            std::string file = req.path();
            if (file == "/") file = "/index.html";
    
            std::string content = readFile(std::string(wwwDir) + file.substr(1));
            if (content.empty()) {
                resp->setStatusCode(HttpResponse::k404NotFound);
                resp->setBody("Not Found");
                return;
            }
            resp->setStatusCode(HttpResponse::k200Ok);
            resp->setContentType(getMimeType(file));
            resp->setBody(content);
            resp->setCloseConnection(false);
        }
    private:
        const char* wwwDir;  // "../www/"
    };

}

} // namespace miniKV















